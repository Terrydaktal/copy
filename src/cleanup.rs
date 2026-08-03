//! Source cleanup and recursive removal for move and sync operations.

use super::*;

pub(super) fn remove_path_recursive(path: &Path, use_sudo: bool, mode: TransferMode) -> bool {
    if !path.exists() {
        return true;
    }
    if use_sudo {
        let cmd = vec![
            "rm".to_string(),
            "-rf".to_string(),
            "--".to_string(),
            path.display().to_string(),
        ];
        match run_command_capture(&cmd, true) {
            Ok(out) if out.code == 0 => true,
            _ => {
                log(
                    mode,
                    &format!("Failed to remove existing path: {}", path.display()),
                    LogLevel::Error,
                );
                false
            }
        }
    } else {
        let res = if path.is_dir() {
            fs::remove_dir_all(path)
        } else {
            fs::remove_file(path)
        };
        if res.is_err() {
            log(
                mode,
                &format!("Failed to remove existing path: {}", path.display()),
                LogLevel::Error,
            );
            return false;
        }
        true
    }
}

pub(super) fn remove_empty_dirs(path: &Path, remove_root: bool) {
    if !path.is_dir() {
        return;
    }
    let root = path.to_path_buf();
    let mut stack: Vec<(PathBuf, bool)> = vec![(root.clone(), false)];
    while let Some((dir, visited)) = stack.pop() {
        if !visited {
            stack.push((dir.clone(), true));
            let entries = match fs::read_dir(&dir) {
                Ok(v) => v,
                Err(_) => continue,
            };
            for ent in entries.filter_map(Result::ok) {
                let p = ent.path();
                let md = match fs::symlink_metadata(&p) {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                if md.is_dir() {
                    stack.push((p, false));
                }
            }
            continue;
        }

        if !remove_root && dir == root {
            continue;
        }
        let is_empty = fs::read_dir(&dir)
            .map(|mut it| it.next().is_none())
            .unwrap_or(false);
        if is_empty {
            let _ = fs::remove_dir(&dir);
        }
    }
}

pub(super) fn cleanup_source_dirs(
    src_root: &Path,
    remove_root: bool,
    use_sudo: bool,
    mode: TransferMode,
) {
    if !src_root.is_dir() {
        return;
    }
    if use_sudo {
        let mut cmd = vec!["find".to_string(), src_root.display().to_string()];
        if !remove_root {
            cmd.push("-mindepth".to_string());
            cmd.push("1".to_string());
        }
        cmd.extend([
            "-depth".to_string(),
            "-type".to_string(),
            "d".to_string(),
            "-empty".to_string(),
            "-delete".to_string(),
        ]);
        if let Ok(out) = run_command_capture(&cmd, true) {
            if out.code != 0 {
                log(
                    mode,
                    &format!(
                        "Source cleanup failed: find exited with status {}.",
                        out.code
                    ),
                    LogLevel::Warn,
                );
            }
        }
    } else {
        remove_empty_dirs(src_root, remove_root);
    }
}

pub(super) fn cleanup_source_dirs_from_manifest(
    src_root: &Path,
    manifest: &TransferManifest,
    remove_root: bool,
    exclude_rel: Option<&str>,
) {
    for rel in manifest.dirs.iter().rev() {
        if exclude_rel
            .map(|prefix| rel_matches_prefix(rel, prefix))
            .unwrap_or(false)
        {
            continue;
        }
        let _ = fs::remove_dir(src_root.join(rel));
    }
    if remove_root {
        let _ = fs::remove_dir(src_root);
    }
}

pub(super) fn remove_single_file(path: &Path, use_sudo: bool, mode: TransferMode) -> bool {
    if !path.exists() {
        return true;
    }
    if use_sudo {
        let cmd = vec![
            "rm".to_string(),
            "-f".to_string(),
            "--".to_string(),
            path.display().to_string(),
        ];
        run_command_capture(&cmd, true)
            .map(|o| o.code == 0)
            .unwrap_or_else(|_| {
                log(
                    mode,
                    &format!("Failed to remove source file: {}", path.display()),
                    LogLevel::Warn,
                );
                false
            })
    } else {
        fs::remove_file(path).is_ok()
    }
}

pub(super) fn prune_move_source_duplicates(
    src_path: &str,
    dst_path: &str,
    src_obj_kind: SrcObjKind,
    contents_mode: bool,
    exclude_rel: Option<&str>,
    use_sudo: bool,
    mode: TransferMode,
    manifest: Option<&TransferManifest>,
    _expected_files: u64,
    _expected_bytes: u64,
) -> DeleteCleanupOutcome {
    let mut removed = DeleteCleanupOutcome::default();
    let report_progress = |_force: bool, _removed: &DeleteCleanupOutcome| {};

    match src_obj_kind {
        SrcObjKind::File => {
            let src = Path::new(src_path);
            let mut dst_buf = PathBuf::from(dst_path);
            if dst_buf.is_dir() {
                let src_name = match src.file_name() {
                    Some(v) => v,
                    None => return removed,
                };
                dst_buf = dst_buf.join(src_name);
            }
            let dst = dst_buf.as_path();
            let src_lmd = match fs::symlink_metadata(src) {
                Ok(v) => v,
                Err(_) => return removed,
            };
            let same = if src_lmd.file_type().is_symlink() {
                symlink_targets_equal(src, dst)
            } else {
                let src_md = match fs::metadata(src) {
                    Ok(v) if v.is_file() => v,
                    _ => return removed,
                };
                match fs::metadata(dst) {
                    Ok(v) => v.is_file() && v.len() == src_md.len(),
                    Err(_) => false,
                }
            };
            if same && remove_single_file(src, use_sudo, mode) {
                removed.files += 1;
                removed.bytes += if src_lmd.file_type().is_symlink() {
                    0
                } else {
                    src_lmd.len()
                };
            }
            report_progress(false, &removed);
        }
        SrcObjKind::Dir => {
            let src_no_trailing = src_path.trim_end_matches('/');
            let include_root = if contents_mode {
                false
            } else {
                !src_path.ends_with('/')
            };
            let src_root = Path::new(src_no_trailing);
            let dst_base = Path::new(dst_path.trim_end_matches('/'));
            let src_base = src_root
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();

            if let Some(m) = manifest {
                let mut privileged_deletes: Vec<(PathBuf, u64)> = Vec::new();
                for (entries, destination_was_written) in
                    [(&m.copy_files, true), (&m.identical_files, false)]
                {
                    for entry in entries {
                        if entry.rel.is_empty()
                            || exclude_rel
                                .map(|prefix| rel_matches_prefix(&entry.rel, prefix))
                                .unwrap_or(false)
                        {
                            continue;
                        }
                        let src_file = src_root.join(entry.rel.as_ref());
                        let dst_item =
                            map_dir_dest_path(include_root, &src_base, &entry.rel, dst_base);
                        if src_file == dst_item {
                            continue;
                        }
                        let src_md = match fs::symlink_metadata(&src_file) {
                            Ok(md) => md,
                            Err(_) => continue,
                        };
                        let source_unchanged = src_md.dev() == entry.dev
                            && src_md.ino() == entry.ino
                            && (entry.is_symlink || src_md.len() == entry.size);
                        if !source_unchanged {
                            continue;
                        }
                        let destination_matches = destination_was_written
                            || if entry.is_symlink {
                                symlink_targets_equal(&src_file, &dst_item)
                            } else {
                                fs::metadata(&dst_item)
                                    .map(|meta| meta.is_file() && meta.len() == entry.size)
                                    .unwrap_or(false)
                            };
                        if !destination_matches {
                            continue;
                        }
                        if use_sudo {
                            privileged_deletes.push((src_file, entry.size));
                        } else if remove_single_file(&src_file, false, mode) {
                            removed.files = removed.files.saturating_add(1);
                            removed.bytes = removed.bytes.saturating_add(entry.size);
                        }
                        report_progress(false, &removed);
                    }
                }
                const PRIVILEGED_DELETE_CHUNK: usize = 256;
                for chunk in privileged_deletes.chunks(PRIVILEGED_DELETE_CHUNK) {
                    let mut cmd = vec!["rm".to_string(), "-f".to_string(), "--".to_string()];
                    cmd.extend(chunk.iter().map(|(path, _)| path.display().to_string()));
                    let _ = run_command_capture(&cmd, true);
                    for (path, size) in chunk {
                        if !path.exists() {
                            removed.files = removed.files.saturating_add(1);
                            removed.bytes = removed.bytes.saturating_add(*size);
                        }
                    }
                }
                report_progress(true, &removed);
                return removed;
            }

            for ent in WalkDir::new(src_root)
                .sort(false)
                .skip_hidden(false)
                .parallelism(jwalk::Parallelism::RayonDefaultPool {
                    busy_timeout: Duration::from_secs(0),
                })
                .into_iter()
                .filter_map(Result::ok)
            {
                let p = ent.path();
                if p == src_root {
                    continue;
                }
                let md = match fs::symlink_metadata(&p) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if !md.is_file() && !md.file_type().is_symlink() {
                    continue;
                }
                let rel = match p.strip_prefix(src_root) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let rel_str = normalize_rel(rel);
                if exclude_rel
                    .map(|prefix| rel_matches_prefix(&rel_str, prefix))
                    .unwrap_or(false)
                {
                    continue;
                }
                let dst_item = if include_root {
                    dst_base.join(&src_base).join(rel)
                } else {
                    dst_base.join(rel)
                };

                if p == dst_item {
                    continue;
                }

                let same = if md.file_type().is_symlink() {
                    symlink_targets_equal(&p, &dst_item)
                } else {
                    match fs::metadata(&dst_item) {
                        Ok(dm) => dm.is_file() && dm.len() == md.len(),
                        Err(_) => false,
                    }
                };
                if same && remove_single_file(&p, use_sudo, mode) {
                    removed.files += 1;
                    removed.bytes += md.len();
                }
                report_progress(false, &removed);
            }
        }
    }

    report_progress(true, &removed);
    removed
}
