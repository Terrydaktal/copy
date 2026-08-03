//! Source scanning, destination indexing, and transfer-plan construction.

use super::*;

#[derive(Default, Clone, Copy)]
pub(super) struct TreeCounts {
    pub(super) files: u64,
    pub(super) bytes: u64,
    pub(super) dirs: u64,
}

pub(super) fn count_tree_any(path: &Path, include_root_dir: bool) -> TreeCounts {
    let root_meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(_) => return TreeCounts::default(),
    };
    if root_meta.is_file() {
        return TreeCounts {
            files: 1,
            bytes: root_meta.len(),
            dirs: 0,
        };
    }
    if !root_meta.is_dir() {
        return TreeCounts::default();
    }
    let mut counts = TreeCounts {
        dirs: u64::from(include_root_dir),
        ..TreeCounts::default()
    };
    for entry in WalkDir::new(path)
        .sort(false)
        .skip_hidden(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.depth() > 0)
    {
        let file_type = entry.file_type();
        if file_type.is_dir() {
            counts.dirs = counts.dirs.saturating_add(1);
        } else if file_type.is_file() {
            counts.files = counts.files.saturating_add(1);
            if let Ok(meta) = entry.metadata() {
                counts.bytes = counts.bytes.saturating_add(meta.len());
            }
        }
    }
    counts
}

pub(super) fn count_regular_files_any(path: &Path) -> u64 {
    count_tree_any(path, false).files
}

pub(super) fn count_directories_any(path: &Path, include_root: bool) -> u64 {
    count_tree_any(path, include_root).dirs
}

pub(super) fn top_level_rel_component(rel: &str) -> Option<&str> {
    let trimmed = rel.trim_start_matches("./").trim_start_matches('/');
    let first = trimmed.split('/').next().unwrap_or("");
    if first.is_empty() {
        None
    } else {
        Some(first)
    }
}

pub(super) fn rel_matches_prefix(rel: &str, prefix: &str) -> bool {
    rel == prefix
        || rel
            .strip_prefix(prefix)
            .map(|suffix| suffix.starts_with('/'))
            .unwrap_or(false)
}

#[derive(Default, Clone, Copy)]
pub(super) struct PreMergeFastRenameStats {
    pub(super) moved_entries: u64,
    pub(super) moved_files: u64,
    pub(super) moved_bytes: u64,
    pub(super) removed_copy_bytes: u64,
}

pub(super) fn premerge_fast_rename_noncolliding_children(
    src_root: &Path,
    dst_root: &Path,
    manifest: Option<&mut TransferManifest>,
    exclude_top_level_name: Option<&str>,
) -> PreMergeFastRenameStats {
    if !src_root.is_dir() || !dst_root.is_dir() {
        return PreMergeFastRenameStats::default();
    }

    let mut aggregate_by_top: FxHashMap<String, (u64, u64, u64)> = FxHashMap::default();
    if let Some(manifest) = manifest.as_deref() {
        for entry in &manifest.identical_files {
            if let Some(top) = top_level_rel_component(&entry.rel) {
                let aggregate = aggregate_by_top.entry(top.to_string()).or_default();
                aggregate.0 = aggregate.0.saturating_add(1);
                aggregate.1 = aggregate.1.saturating_add(entry.size);
            }
        }
        for entry in &manifest.copy_files {
            if let Some(top) = top_level_rel_component(&entry.rel) {
                let aggregate = aggregate_by_top.entry(top.to_string()).or_default();
                aggregate.0 = aggregate.0.saturating_add(1);
                aggregate.1 = aggregate.1.saturating_add(entry.size);
                aggregate.2 = aggregate.2.saturating_add(entry.size);
            }
        }
    }

    let mut children: Vec<PathBuf> = match fs::read_dir(src_root) {
        Ok(rd) => rd.filter_map(Result::ok).map(|e| e.path()).collect(),
        Err(_) => return PreMergeFastRenameStats::default(),
    };
    children.sort();

    let mut moved_names: HashSet<String> = HashSet::new();
    let mut stats = PreMergeFastRenameStats::default();

    for src_child in children {
        let name = match src_child.file_name() {
            Some(n) => n.to_string_lossy().to_string(),
            None => continue,
        };
        if exclude_top_level_name == Some(name.as_str()) {
            continue;
        }
        let dst_child = dst_root.join(&name);
        if dst_child.exists() || src_child == dst_child {
            continue;
        }
        if !can_fast_rename_same_fs(&src_child, &dst_child) {
            continue;
        }

        let (child_files, child_bytes, removed_copy_bytes) =
            aggregate_by_top.get(&name).copied().unwrap_or_default();
        if fs::rename(&src_child, &dst_child).is_ok() {
            moved_names.insert(name);
            stats.moved_entries = stats.moved_entries.saturating_add(1);
            stats.moved_files = stats.moved_files.saturating_add(child_files);
            stats.moved_bytes = stats.moved_bytes.saturating_add(child_bytes);
            stats.removed_copy_bytes = stats.removed_copy_bytes.saturating_add(removed_copy_bytes);
        }
    }

    if moved_names.is_empty() {
        return stats;
    }

    if let Some(m) = manifest {
        m.dirs.retain(|rel| {
            top_level_rel_component(rel)
                .map(|top| !moved_names.contains(top))
                .unwrap_or(true)
        });
        m.copy_files.retain(|entry| {
            let keep = top_level_rel_component(&entry.rel)
                .map(|top| !moved_names.contains(top))
                .unwrap_or(true);
            keep
        });
        m.identical_files.retain(|entry| {
            top_level_rel_component(&entry.rel)
                .map(|top| !moved_names.contains(top))
                .unwrap_or(true)
        });
    }

    stats
}

pub(super) fn destination_file_counts(
    destination_root: &Path,
    source_rel_files: &HashSet<String>,
) -> (u64, u64) {
    if !destination_root.is_dir() {
        return (0, 0);
    }
    let idx = build_destination_index(destination_root);
    let total = idx.file_sizes.len() as u64;
    let uncollided = idx
        .file_sizes
        .keys()
        .filter(|rel| !source_rel_files.contains(*rel))
        .count() as u64;
    (total, uncollided)
}

#[derive(Default)]
pub(super) struct DestinationIndex {
    pub(super) file_sizes: FxHashMap<String, u64>,
    pub(super) file_mtimes: FxHashMap<String, SystemTime>,
    pub(super) dirs: FxHashSet<String>,
    pub(super) symlinks: FxHashSet<String>,
}

impl DestinationIndex {
    pub(super) fn path_exists(&self, rel: &str) -> bool {
        self.file_sizes.contains_key(rel) || self.dirs.contains(rel) || self.symlinks.contains(rel)
    }
}

pub(super) fn build_destination_index(destination_root: &Path) -> DestinationIndex {
    if !destination_root.is_dir() {
        return DestinationIndex::default();
    }

    let mut file_sizes: FxHashMap<String, u64> = FxHashMap::default();
    let mut file_mtimes: FxHashMap<String, SystemTime> = FxHashMap::default();
    let mut dirs: FxHashSet<String> = FxHashSet::default();
    let mut symlinks: FxHashSet<String> = FxHashSet::default();

    for ent in WalkDir::new(destination_root)
        .sort(false)
        .skip_hidden(false)
        .into_iter()
        .filter_map(Result::ok)
    {
        let rel = ent
            .path()
            .strip_prefix(destination_root)
            .ok()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        if rel.is_empty() {
            continue;
        }

        let fty = ent.file_type();
        if fty.is_file() {
            let metadata = ent
                .metadata()
                .or_else(|_| fs::symlink_metadata(ent.path()))
                .ok();
            let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
            if let Some(mtime) = metadata.and_then(|m| m.modified().ok()) {
                file_mtimes.insert(rel.clone(), mtime);
            }
            file_sizes.insert(rel, size);
        } else if fty.is_dir() && ent.depth() > 0 {
            dirs.insert(rel);
        } else if fty.is_symlink() {
            symlinks.insert(rel);
        }
    }

    DestinationIndex {
        file_sizes,
        file_mtimes,
        dirs,
        symlinks,
    }
}

pub(super) fn add_parent_dir_chain(rel: &str, include_root: bool, out: &mut FxHashSet<String>) {
    if rel.is_empty() {
        return;
    }
    let mut cur = rel;
    loop {
        match cur.rfind('/') {
            Some(idx) => {
                let parent = &cur[..idx];
                if parent.is_empty() {
                    if include_root {
                        out.insert(String::new());
                    }
                    break;
                }
                out.insert(parent.to_string());
                cur = parent;
            }
            None => {
                if include_root {
                    out.insert(String::new());
                }
                break;
            }
        }
    }
}

pub(super) fn normalize_rel(path: &Path) -> String {
    let s = path.to_string_lossy();
    if s.contains('\\') {
        s.replace('\\', "/")
    } else {
        s.into_owned()
    }
}

pub(super) fn map_dir_dest_path(
    include_root: bool,
    src_base: &str,
    rel: &str,
    dst_base: &Path,
) -> PathBuf {
    if include_root {
        if rel.is_empty() {
            dst_base.join(src_base)
        } else {
            dst_base.join(src_base).join(rel)
        }
    } else if rel.is_empty() {
        dst_base.to_path_buf()
    } else {
        dst_base.join(rel)
    }
}

pub(super) fn map_display_rel(include_root: bool, src_base: &str, rel: &str) -> String {
    if include_root {
        if rel.is_empty() {
            format!("{src_base}/")
        } else {
            format!("{src_base}/{rel}")
        }
    } else if rel.is_empty() {
        String::new()
    } else {
        rel.to_string()
    }
}

pub(super) fn bounded_preview_change(
    rel: String,
    kind: ChangeKind,
    depth: Option<usize>,
) -> (String, ChangeKind) {
    let Some(depth) = depth.filter(|depth| *depth > 0) else {
        return (rel, kind);
    };
    let components: Vec<&str> = rel.trim_end_matches('/').split('/').collect();
    if components.len() <= depth {
        return (rel, kind);
    }
    let mut bounded = components[..depth].join("/");
    bounded.push('/');
    let bounded_kind = match kind {
        ChangeKind::NewFile => ChangeKind::NewDir,
        other => other,
    };
    (bounded, bounded_kind)
}

pub(super) fn insert_preview_change(
    changes: &mut FxHashMap<String, ChangeKind>,
    rel: String,
    kind: ChangeKind,
    depth: Option<usize>,
) {
    let (rel, kind) = bounded_preview_change(rel, kind, depth);
    changes
        .entry(rel)
        .and_modify(|existing| {
            if matches!(kind, ChangeKind::ModFile) {
                *existing = kind;
            }
        })
        .or_insert(kind);
}

pub(super) fn map_dir_dest(
    include_root: bool,
    src_base: &str,
    rel: &str,
    dst_base: &Path,
) -> (PathBuf, String) {
    (
        map_dir_dest_path(include_root, src_base, rel, dst_base),
        map_display_rel(include_root, src_base, rel),
    )
}

pub(super) fn ensure_dst_file_path<'a>(
    dst_file: &'a mut Option<PathBuf>,
    include_root: bool,
    src_base: &str,
    rel: &str,
    dst_base: &Path,
) -> &'a Path {
    if dst_file.is_none() {
        *dst_file = Some(map_dir_dest_path(include_root, src_base, rel, dst_base));
    }
    dst_file.as_deref().expect("destination path should be set")
}

pub(super) fn parent_rel_in_set(rel: &str, set: &FxHashSet<String>) -> bool {
    if set.is_empty() {
        return false;
    }
    match rel.rfind('/') {
        Some(idx) => set.contains(&rel[..idx]),
        None => false,
    }
}

pub(super) fn pre_scan_new_tree_lite(
    src_root: &Path,
    include_root: bool,
    src_base: &str,
    exclude_rel: Option<&str>,
    build_source_display_paths: bool,
    bounded_preview_depth: Option<usize>,
) -> PreScan {
    let mut out = PreScan {
        planned_bytes_exact: false,
        ..PreScan::default()
    };
    let mut preview: FxHashMap<String, ChangeKind> = FxHashMap::default();
    if include_root && !src_base.is_empty() {
        insert_preview_change(
            &mut preview,
            format!("{src_base}/"),
            ChangeKind::NewDir,
            bounded_preview_depth,
        );
    }

    let walker = WalkDir::new(src_root)
        .sort(false)
        .skip_hidden(false)
        .parallelism(jwalk::Parallelism::RayonDefaultPool {
            busy_timeout: Duration::from_secs(1),
        });
    let mut files = 0u64;
    let mut dirs = u64::from(include_root);
    for entry in walker.into_iter().filter_map(Result::ok) {
        if entry.depth() == 0 {
            continue;
        }
        let needs_rel = exclude_rel.is_some()
            || build_source_display_paths
            || bounded_preview_depth
                .map(|depth| entry.depth() <= depth + usize::from(include_root))
                .unwrap_or(true);
        let rel = needs_rel
            .then(|| entry.path().strip_prefix(src_root).ok().map(normalize_rel))
            .flatten();
        if rel
            .as_deref()
            .zip(exclude_rel)
            .map(|(rel, prefix)| rel_matches_prefix(rel, prefix))
            .unwrap_or(false)
        {
            continue;
        }
        let file_type = entry.file_type();
        let is_dir = file_type.is_dir();
        let is_symlink = file_type.is_symlink();
        if is_dir {
            dirs = dirs.saturating_add(1);
        } else if !is_symlink {
            files = files.saturating_add(1);
        }
        if let Some(rel) = rel {
            let display = map_display_rel(include_root, src_base, &rel);
            if build_source_display_paths {
                out.source_display_paths
                    .insert(display.trim_end_matches('/').to_string());
            }
            insert_preview_change(
                &mut preview,
                if is_dir {
                    format!("{}/", display.trim_end_matches('/'))
                } else {
                    display
                },
                if is_dir {
                    ChangeKind::NewDir
                } else {
                    ChangeKind::NewFile
                },
                bounded_preview_depth,
            );
        }
    }
    out.total_regular_files = Some(files);
    out.total_regular_bytes = None;
    out.total_dirs = Some(dirs);
    out.add_files = files;
    out.add_dirs = dirs;
    out.has_itemized_changes = !preview.is_empty();
    out.change_preview.extend(
        preview
            .into_iter()
            .map(|(rel, kind)| ChangeItem { kind, rel }),
    );
    out
}

pub(super) struct ScannedFileEntry {
    rel: Arc<str>,
    source_path: Option<PathBuf>,
    size: u64,
    is_symlink: bool,
    dev: u64,
    ino: u64,
    nlink: u64,
    mtime: Option<SystemTime>,
}

type SrcScanEntries = (
    Vec<String>,
    Vec<ScannedFileEntry>,
    Vec<ManifestDirTimeEntry>,
);

pub(super) fn scan_source_entries(
    src_root: &Path,
    exclude_rel: Option<&str>,
    parallel_directory_walk: bool,
) -> SrcScanEntries {
    if parallel_directory_walk {
        let mut dirs = Vec::new();
        let mut files = Vec::new();
        let mut dir_times = Vec::new();
        for entry in WalkDir::new(src_root)
            .sort(false)
            .skip_hidden(false)
            .parallelism(jwalk::Parallelism::RayonDefaultPool {
                busy_timeout: Duration::from_secs(1),
            })
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            let rel = match path.strip_prefix(src_root) {
                Ok(rel) => normalize_rel(rel),
                Err(_) => continue,
            };
            if exclude_rel
                .map(|prefix| rel_matches_prefix(&rel, prefix))
                .unwrap_or(false)
            {
                continue;
            }
            let meta = match fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(_) => continue,
            };
            if meta.is_dir() {
                dir_times.push(ManifestDirTimeEntry {
                    rel: rel.clone(),
                    atime: FileTime::from_last_access_time(&meta),
                    mtime: FileTime::from_last_modification_time(&meta),
                });
                if !rel.is_empty() {
                    dirs.push(rel);
                }
            } else if meta.is_file() {
                files.push(ScannedFileEntry {
                    rel: rel.into(),
                    source_path: None,
                    size: meta.len(),
                    is_symlink: false,
                    dev: meta.dev(),
                    ino: meta.ino(),
                    nlink: meta.nlink(),
                    mtime: meta.modified().ok(),
                });
            } else if meta.file_type().is_symlink() {
                files.push(ScannedFileEntry {
                    rel: rel.into(),
                    source_path: Some(path),
                    size: 0,
                    is_symlink: true,
                    dev: meta.dev(),
                    ino: meta.ino(),
                    nlink: meta.nlink(),
                    mtime: None,
                });
            }
        }
        return (dirs, files, dir_times);
    }
    let mut dirs: Vec<String> = Vec::new();
    let mut files: Vec<ScannedFileEntry> = Vec::new();
    let mut dir_times: Vec<ManifestDirTimeEntry> = Vec::new();

    fn walk_source_entries(
        current: &Path,
        current_meta: Option<fs::Metadata>,
        rel: &str,
        exclude_rel: Option<&str>,
        dirs: &mut Vec<String>,
        files: &mut Vec<ScannedFileEntry>,
        dir_times: &mut Vec<ManifestDirTimeEntry>,
    ) {
        let meta = match current_meta {
            Some(meta) => meta,
            None => match fs::symlink_metadata(current) {
                Ok(m) => m,
                Err(_) => return,
            },
        };
        if meta.is_dir() {
            dir_times.push(ManifestDirTimeEntry {
                rel: rel.to_string(),
                atime: FileTime::from_last_access_time(&meta),
                mtime: FileTime::from_last_modification_time(&meta),
            });
            if !rel.is_empty() {
                dirs.push(rel.to_string());
            }
            let rd = match fs::read_dir(current) {
                Ok(v) => v,
                Err(_) => return,
            };
            for entry in rd.filter_map(Result::ok) {
                let child_path = entry.path();
                let child_name = match child_path.file_name() {
                    Some(n) => n.to_string_lossy().into_owned(),
                    None => continue,
                };
                let child_rel = if rel.is_empty() {
                    child_name.clone()
                } else {
                    format!("{rel}/{child_name}")
                };
                if exclude_rel
                    .map(|prefix| rel_matches_prefix(&child_rel, prefix))
                    .unwrap_or(false)
                {
                    continue;
                }
                let child_meta = match fs::symlink_metadata(&child_path) {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                if child_meta.is_dir() {
                    walk_source_entries(
                        &child_path,
                        Some(child_meta),
                        &child_rel,
                        exclude_rel,
                        dirs,
                        files,
                        dir_times,
                    );
                } else if child_meta.is_file() {
                    files.push(ScannedFileEntry {
                        rel: child_rel.into(),
                        source_path: None,
                        size: child_meta.len(),
                        is_symlink: false,
                        dev: child_meta.dev(),
                        ino: child_meta.ino(),
                        nlink: child_meta.nlink(),
                        mtime: child_meta.modified().ok(),
                    });
                } else if child_meta.file_type().is_symlink() {
                    files.push(ScannedFileEntry {
                        rel: child_rel.into(),
                        source_path: Some(child_path),
                        size: 0,
                        is_symlink: true,
                        dev: child_meta.dev(),
                        ino: child_meta.ino(),
                        nlink: child_meta.nlink(),
                        mtime: None,
                    });
                }
            }
        }
    }

    walk_source_entries(
        src_root,
        None,
        "",
        exclude_rel,
        &mut dirs,
        &mut files,
        &mut dir_times,
    );

    (dirs, files, dir_times)
}

pub(super) fn pre_scan_directory(
    src_path: &str,
    dst_path: &str,
    src_mnt: &Path,
    build_manifest: bool,
    build_source_display_paths: bool,
    collect_file_relation_breakdown: bool,
    replace_dest_symlink: bool,
    merge_collision_policy: MergeCollisionPolicy,
    exclude_rel: Option<&str>,
    bounded_preview_depth: Option<usize>,
    preview_lite: bool,
) -> PreScan {
    let src_no_trailing = src_path.trim_end_matches('/');
    let include_root = !src_path.ends_with('/');
    let src_root = Path::new(src_no_trailing);
    let dst_base = Path::new(dst_path.trim_end_matches('/'));
    let src_base = src_mnt
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    let destination_root = if include_root {
        dst_base.join(&src_base)
    } else {
        dst_base.to_path_buf()
    };
    let destination_missing = !destination_root.exists();

    if preview_lite && destination_missing {
        return pre_scan_new_tree_lite(
            src_root,
            include_root,
            &src_base,
            exclude_rel,
            build_source_display_paths,
            bounded_preview_depth,
        );
    }

    // Strict fast-path for non-verbose preview when destination root is missing:
    // all source content is guaranteed "new", so skip destination path construction/stat checks.
    let src_dev = fs::metadata(src_root).ok().map(|m| m.dev());
    let dst_dev = fs::metadata(&destination_root).ok().map(|m| m.dev());
    let source_media = dev_media_kind(src_root);
    let destination_media = dev_media_kind(&destination_root);
    let parallel_source_scan = source_media != MediaKind::Hdd;
    let can_parallel_scans = !destination_missing
        && src_dev.is_some()
        && dst_dev.is_some()
        && (src_dev != dst_dev
            || (source_media == MediaKind::Nvme && destination_media == MediaKind::Nvme));
    let (mut dirs, files, mut dir_times, destination_index): (
        Vec<String>,
        Vec<ScannedFileEntry>,
        Vec<ManifestDirTimeEntry>,
        Option<DestinationIndex>,
    ) = if destination_missing {
        let (d, f, t) = scan_source_entries(src_root, exclude_rel, parallel_source_scan);
        (d, f, t, None)
    } else if can_parallel_scans {
        std::thread::scope(|scope| {
            let idx_handle = scope.spawn(|| build_destination_index(&destination_root));
            let (d, f, t) = scan_source_entries(src_root, exclude_rel, parallel_source_scan);
            let idx = idx_handle
                .join()
                .unwrap_or_else(|_| build_destination_index(&destination_root));
            (d, f, t, Some(idx))
        })
    } else {
        let (d, f, t) = scan_source_entries(src_root, exclude_rel, parallel_source_scan);
        let idx = build_destination_index(&destination_root);
        (d, f, t, Some(idx))
    };

    let mut out = PreScan::default();
    out.total_regular_files = Some(files.iter().filter(|entry| !entry.is_symlink).count() as u64);
    out.total_regular_bytes = Some(
        files
            .iter()
            .filter(|entry| !entry.is_symlink)
            .map(|entry| entry.size)
            .sum(),
    );
    if build_source_display_paths {
        let mut source_display_paths: FxHashSet<String> = FxHashSet::default();
        if include_root && !src_base.is_empty() {
            source_display_paths.insert(src_base.clone());
        }
        for rel in &dirs {
            let mapped = map_display_rel(include_root, &src_base, rel);
            let key = mapped.trim_end_matches('/').to_string();
            if !key.is_empty() {
                source_display_paths.insert(key);
            }
        }
        for entry in &files {
            let mapped = map_display_rel(include_root, &src_base, &entry.rel);
            let key = mapped.trim_end_matches('/').to_string();
            if !key.is_empty() {
                source_display_paths.insert(key);
            }
        }
        out.source_display_paths = source_display_paths;
    }
    let source_rel_dirs: FxHashSet<String> = dirs.iter().cloned().collect();
    let root_new_dir = include_root && !destination_root.is_dir();
    out.total_dirs = Some(dirs.len() as u64 + u64::from(include_root));

    let mut directory_preview_changes: FxHashMap<String, ChangeKind> = FxHashMap::default();
    if include_root && destination_missing {
        let (dst_root, display_rel) = map_dir_dest(true, &src_base, "", dst_base);
        if !dst_root.is_dir() {
            if !display_rel.is_empty() {
                insert_preview_change(
                    &mut directory_preview_changes,
                    display_rel,
                    ChangeKind::NewDir,
                    bounded_preview_depth,
                );
                out.has_itemized_changes = true;
            }
        }
    }

    let mut missing_dir_prefixes: FxHashSet<String> = FxHashSet::default();
    dirs.sort_by(|a, b| {
        let a_depth = a.bytes().filter(|byte| *byte == b'/').count();
        let b_depth = b.bytes().filter(|byte| *byte == b'/').count();
        a_depth.cmp(&b_depth).then_with(|| a.cmp(b))
    });

    for rel in &dirs {
        let parent_missing = match rel.rfind('/') {
            Some(idx) => missing_dir_prefixes.contains(&rel[..idx]),
            None => false,
        };
        let dir_missing = if destination_missing || parent_missing {
            true
        } else if let Some(idx) = destination_index.as_ref() {
            !idx.dirs.contains(rel.as_str())
        } else {
            !map_dir_dest_path(include_root, &src_base, rel, dst_base).is_dir()
        };
        if dir_missing {
            missing_dir_prefixes.insert(rel.clone());
            let display_rel = map_display_rel(include_root, &src_base, rel);
            let rel_dir = format!("{display_rel}/").replace("//", "/");
            insert_preview_change(
                &mut directory_preview_changes,
                rel_dir,
                ChangeKind::NewDir,
                bounded_preview_depth,
            );
            out.has_itemized_changes = true;
        }
    }

    let source_dir_count = dirs.len();
    let mut manifest_dirs = if build_manifest { Some(dirs) } else { None };

    type FileReduce = (
        u64,
        u64,
        u64,
        FxHashMap<String, ChangeKind>,
        Vec<ManifestFileEntry>,
        Vec<ManifestFileEntry>,
        FxHashSet<String>,
        u64,
        FileRelationBreakdown,
    );
    let has_missing_subtrees = !missing_dir_prefixes.is_empty();
    let (
        add_files,
        mod_files,
        planned_bytes,
        detailed_changes,
        mut manifest_copy_files,
        mut manifest_identical_files,
        mut changed_parent_dirs,
        _overlap_count,
        file_relation_breakdown,
    ): FileReduce = files
        .par_iter()
        .fold(
            || {
                (
                    0,
                    0,
                    0,
                    FxHashMap::default(),
                    Vec::new(),
                    Vec::new(),
                    FxHashSet::default(),
                    0,
                    FileRelationBreakdown::default(),
                )
            },
            |mut acc, entry| {
                let rel = &entry.rel;
                let src_file = &entry.source_path;
                let size = entry.size;
                let is_symlink = entry.is_symlink;
                let dst_idx = destination_index.as_ref();
                let mut dst_file: Option<PathBuf> = None;
                let change = if destination_missing {
                    Some(ChangeKind::NewFile)
                } else if has_missing_subtrees && parent_rel_in_set(rel, &missing_dir_prefixes) {
                    Some(ChangeKind::NewFile)
                } else if is_symlink {
                    if let Some(idx) = dst_idx {
                        if idx.symlinks.contains(rel.as_ref()) {
                            if symlink_targets_equal(
                                src_file.as_ref().unwrap(),
                                ensure_dst_file_path(
                                    &mut dst_file,
                                    include_root,
                                    &src_base,
                                    rel,
                                    dst_base,
                                ),
                            ) {
                                None
                            } else {
                                Some(ChangeKind::ModFile)
                            }
                        } else if idx.path_exists(rel.as_ref()) {
                            Some(ChangeKind::ModFile)
                        } else {
                            Some(ChangeKind::NewFile)
                        }
                    } else {
                        match fs::symlink_metadata(ensure_dst_file_path(
                            &mut dst_file,
                            include_root,
                            &src_base,
                            rel,
                            dst_base,
                        )) {
                            Ok(dm)
                                if dm.file_type().is_symlink()
                                    && symlink_targets_equal(
                                        src_file.as_ref().unwrap(),
                                        ensure_dst_file_path(
                                            &mut dst_file,
                                            include_root,
                                            &src_base,
                                            rel,
                                            dst_base,
                                        ),
                                    ) =>
                            {
                                None
                            }
                            Ok(_) => Some(ChangeKind::ModFile),
                            Err(_) => Some(ChangeKind::NewFile),
                        }
                    }
                } else {
                    let needs_mtime =
                        merge_collision_policy.requires_mtime() || collect_file_relation_breakdown;
                    let src_mtime = needs_mtime.then_some(entry.mtime).flatten();
                    let dst_exists = if let Some(idx) = dst_idx {
                        idx.path_exists(rel.as_ref())
                    } else {
                        fs::symlink_metadata(ensure_dst_file_path(
                            &mut dst_file,
                            include_root,
                            &src_base,
                            rel,
                            dst_base,
                        ))
                        .is_ok()
                    };
                    let dst_path =
                        ensure_dst_file_path(&mut dst_file, include_root, &src_base, rel, dst_base);
                    let dst_is_symlink = if let Some(idx) = dst_idx {
                        idx.symlinks.contains(rel.as_ref())
                    } else {
                        fs::symlink_metadata(dst_path)
                            .map(|dm| dm.file_type().is_symlink())
                            .unwrap_or(false)
                    };
                    let dst_size = if replace_dest_symlink && dst_is_symlink {
                        None
                    } else if let Some(idx) = dst_idx {
                        idx.file_sizes.get(rel.as_ref()).copied()
                    } else {
                        match fs::symlink_metadata(&dst_path) {
                            Ok(dm) if dm.is_file() => fs::metadata(&dst_path).ok().map(|m| m.len()),
                            Ok(_) => None,
                            Err(_) => None,
                        }
                    };
                    let dst_mtime = if replace_dest_symlink && dst_is_symlink {
                        None
                    } else if needs_mtime {
                        if let Some(idx) = dst_idx {
                            idx.file_mtimes.get(rel.as_ref()).copied()
                        } else {
                            fs::metadata(dst_path).ok().and_then(|m| m.modified().ok())
                        }
                    } else {
                        None
                    };
                    if collect_file_relation_breakdown && dst_exists && !dst_is_symlink {
                        if let Some(breakdown) =
                            classify_file_relation(size, src_mtime, dst_size, dst_mtime)
                        {
                            acc.8.add_assign(breakdown);
                        }
                    }
                    regular_file_collision_change(
                        merge_collision_policy,
                        size,
                        src_mtime,
                        dst_exists,
                        dst_size,
                        dst_mtime,
                    )
                };
                let is_overlap = !matches!(change, Some(ChangeKind::NewFile));
                if is_overlap {
                    acc.7 += 1;
                }
                if let Some(kind) = change {
                    if !is_symlink {
                        match kind {
                            ChangeKind::NewFile => acc.0 += 1,
                            _ => acc.1 += 1,
                        }
                        acc.2 += size;
                    }
                    if build_manifest {
                        acc.4.push(ManifestFileEntry {
                            rel: rel.clone(),
                            size,
                            dev: entry.dev,
                            ino: entry.ino,
                            nlink: entry.nlink,
                            is_symlink,
                        });
                    }
                    {
                        let display_rel = map_display_rel(include_root, &src_base, &rel);
                        insert_preview_change(&mut acc.3, display_rel, kind, bounded_preview_depth);
                    }
                    add_parent_dir_chain(&rel, include_root, &mut acc.6);
                } else if build_manifest {
                    acc.5.push(ManifestFileEntry {
                        rel: rel.clone(),
                        size,
                        dev: entry.dev,
                        ino: entry.ino,
                        nlink: entry.nlink,
                        is_symlink,
                    });
                }
                acc
            },
        )
        .reduce(
            || {
                (
                    0,
                    0,
                    0,
                    FxHashMap::default(),
                    Vec::new(),
                    Vec::new(),
                    FxHashSet::default(),
                    0,
                    FileRelationBreakdown::default(),
                )
            },
            |mut a, b| {
                a.0 += b.0;
                a.1 += b.1;
                a.2 += b.2;
                for (rel, kind) in b.3 {
                    insert_preview_change(&mut a.3, rel, kind, None);
                }
                a.4.extend(b.4);
                a.5.extend(b.5);
                a.6.extend(b.6);
                a.7 += b.7;
                a.8.add_assign(b.8);
                a
            },
        );

    out.add_files += add_files;
    out.mod_files += mod_files;
    out.planned_bytes += planned_bytes;

    if destination_missing {
        out.uncollided_files = 0;
    } else {
        let idx = destination_index
            .as_ref()
            .expect("destination index should exist when destination is present");
        let dest_total_files = idx.file_sizes.len() as u64;
        let source_regular_total = out.total_regular_files.unwrap_or(0);
        let overlap_files = source_regular_total.saturating_sub(add_files);
        let uncollided_by_overlap = dest_total_files.saturating_sub(overlap_files);
        out.uncollided_files = uncollided_by_overlap;
    }

    out.add_dirs = missing_dir_prefixes.len() as u64 + u64::from(root_new_dir);
    for rel in &missing_dir_prefixes {
        add_parent_dir_chain(rel, include_root, &mut changed_parent_dirs);
    }
    let mod_dirs_count = changed_parent_dirs
        .iter()
        .filter(|rel| {
            if rel.is_empty() {
                include_root && !root_new_dir
            } else {
                source_rel_dirs.contains(rel.as_str())
                    && !missing_dir_prefixes.contains(rel.as_str())
            }
        })
        .count() as u64;
    let total_dirs = out.total_dirs.unwrap_or(0);
    out.mod_dirs = mod_dirs_count.min(total_dirs.saturating_sub(out.add_dirs));
    if destination_missing {
        out.uncollided_dirs = 0;
    } else {
        let idx = destination_index
            .as_ref()
            .expect("destination index should exist when destination is present");
        let dest_total_dirs = idx.dirs.len() as u64;
        let uncollided_dirs_by_scan = idx
            .dirs
            .iter()
            .filter(|rel| !source_rel_dirs.contains(rel.as_str()))
            .count() as u64;
        let source_dir_total_no_root = source_dir_count as u64;
        let source_dirs_not_new =
            source_dir_total_no_root.saturating_sub(missing_dir_prefixes.len() as u64);
        let uncollided_dirs_by_overlap = dest_total_dirs.saturating_sub(source_dirs_not_new);
        out.uncollided_dirs = uncollided_dirs_by_scan.max(uncollided_dirs_by_overlap);
    }

    if out.add_files > 0 || out.mod_files > 0 {
        out.has_itemized_changes = true;
    }

    directory_preview_changes.extend(detailed_changes);
    out.change_preview.extend(
        directory_preview_changes
            .into_iter()
            .map(|(rel, kind)| ChangeItem { kind, rel }),
    );
    out.file_relation_breakdown = file_relation_breakdown;

    if build_manifest {
        if let Some(d) = manifest_dirs.take() {
            manifest_copy_files.sort_by(|a, b| a.rel.cmp(&b.rel));
            manifest_identical_files.sort_by(|a, b| a.rel.cmp(&b.rel));
            dir_times.sort_by(|a, b| {
                let a_depth = a.rel.bytes().filter(|byte| *byte == b'/').count();
                let b_depth = b.rel.bytes().filter(|byte| *byte == b'/').count();
                b_depth.cmp(&a_depth).then_with(|| a.rel.cmp(&b.rel))
            });
            out.transfer_manifest = Some(TransferManifest {
                dirs: d,
                dir_times,
                copy_files: manifest_copy_files,
                identical_files: manifest_identical_files,
            });
        }
    }

    out
}

pub(super) fn pre_scan_file(
    src_mnt: &Path,
    dst_path: &str,
    dst_obj_kind: DstObjKind,
    build_source_display_paths: bool,
    collect_file_relation_breakdown: bool,
    replace_dest_symlink: bool,
    merge_collision_policy: MergeCollisionPolicy,
    destination_index: Option<&DestinationIndex>,
) -> PreScan {
    let mut out = PreScan::default();
    let src_lmd = match fs::symlink_metadata(src_mnt) {
        Ok(m) => m,
        Err(_) => return out,
    };
    let src_is_symlink = src_lmd.file_type().is_symlink();
    let src_meta = if src_is_symlink {
        None
    } else {
        match fs::metadata(src_mnt) {
            Ok(m) => Some(m),
            Err(_) => return out,
        }
    };
    let size = src_meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let src_mtime = src_meta.as_ref().and_then(|m| m.modified().ok());
    out.total_regular_files = Some(if src_is_symlink { 0 } else { 1 });
    out.total_regular_bytes = Some(size);
    out.total_dirs = Some(0);

    let src_name = src_mnt
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "source".to_string());
    let src_name_key = src_name.clone();

    let (dst_file, display_rel) = match dst_obj_kind {
        DstObjKind::Dir | DstObjKind::DirExisting => {
            let base = Path::new(dst_path.trim_end_matches('/'));
            (base.join(&src_name), src_name.clone())
        }
        _ => {
            let p = Path::new(dst_path);
            let n = p
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or(src_name.clone());
            (p.to_path_buf(), n)
        }
    };

    let destination_root = match dst_obj_kind {
        DstObjKind::Dir | DstObjKind::DirExisting => {
            Some(PathBuf::from(dst_path.trim_end_matches('/')))
        }
        _ => dst_file.parent().map(|p| {
            if p.as_os_str().is_empty() {
                PathBuf::from(".")
            } else {
                p.to_path_buf()
            }
        }),
    };
    if let Some(root) = destination_root {
        if root.is_dir() {
            let mut source_rel_files = HashSet::new();
            let rel_key = display_rel.trim_end_matches('/').to_string();
            if !rel_key.is_empty() {
                source_rel_files.insert(rel_key);
            }
            // For file-to-file moves within the same destination directory (rename case),
            // exclude the source filename from uncollided totals. It is part of the move
            // operation and should not be treated as unrelated destination-only content.
            let src_parent = src_mnt.parent().unwrap_or_else(|| Path::new("."));
            if realpath_allow_missing(src_parent) == realpath_allow_missing(&root) {
                let src_rel = src_name_key.trim_end_matches('/').to_string();
                if !src_rel.is_empty() {
                    source_rel_files.insert(src_rel);
                }
            }
            let uncollided_by_scan = if let Some(index) = destination_index {
                index
                    .file_sizes
                    .keys()
                    .filter(|rel| !source_rel_files.contains(*rel))
                    .count() as u64
            } else {
                destination_file_counts(&root, &source_rel_files).1
            };
            out.uncollided_files = uncollided_by_scan;
        }
    }

    if build_source_display_paths && !display_rel.is_empty() {
        out.source_display_paths
            .insert(display_rel.trim_end_matches('/').to_string());
    }

    let change = if src_is_symlink {
        match fs::symlink_metadata(&dst_file) {
            Ok(dm) if dm.file_type().is_symlink() && symlink_targets_equal(src_mnt, &dst_file) => {
                None
            }
            Ok(_) => Some(ChangeKind::ModFile),
            Err(_) => Some(ChangeKind::NewFile),
        }
    } else {
        let dst_lmd = fs::symlink_metadata(&dst_file).ok();
        let dst_is_symlink = dst_lmd
            .as_ref()
            .map(|md| md.file_type().is_symlink())
            .unwrap_or(false);
        let dst_exists = dst_lmd.is_some();
        let dst_meta = if replace_dest_symlink && dst_is_symlink {
            None
        } else {
            fs::metadata(&dst_file).ok()
        };
        let dst_size = dst_meta.as_ref().map(|m| m.len());
        let dst_mtime = dst_meta.as_ref().and_then(|m| m.modified().ok());
        if collect_file_relation_breakdown && dst_meta.is_some() && !dst_is_symlink {
            if let Some(breakdown) = classify_file_relation(size, src_mtime, dst_size, dst_mtime) {
                out.file_relation_breakdown = breakdown;
            }
        }
        regular_file_collision_change(
            merge_collision_policy,
            size,
            src_mtime,
            dst_exists,
            dst_size,
            dst_mtime,
        )
    };

    if let Some(ch) = change {
        out.has_itemized_changes = true;
        out.planned_bytes = if src_is_symlink { 0 } else { size };
        match ch {
            ChangeKind::NewFile => {
                if !src_is_symlink {
                    out.add_files = 1;
                }
            }
            _ => {
                if !src_is_symlink {
                    out.mod_files = 1;
                }
            }
        }
        out.change_preview.push(ChangeItem {
            kind: ch,
            rel: display_rel,
        });
    }

    out
}
