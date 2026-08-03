//! Local Rust-backend transfer execution and worker coordination.

use super::*;

pub(super) fn run_rust_transfer(
    src_path: &str,
    dst_path: &str,
    src_obj_kind: SrcObjKind,
    _is_move: bool,
    planned_bytes: u64,
    manifest: Option<&TransferManifest>,
    media: MediaKind,
    replace_dest_symlink: bool,
    merge_collision_policy: MergeCollisionPolicy,
    exclude_rel: Option<&str>,
) -> TransferOutcome {
    let done = Arc::new(AtomicU64::new(0));
    let copy_buf_bytes = copy_chunk_bytes_for_media(media);
    let inflight_limiter = inflight_max_bytes_for_media(media)
        .map(InflightWriteLimiter::new)
        .map(Arc::new);
    let eta_workload = manifest
        .map(|m| {
            EtaWorkload::from_manifest(
                m,
                src_obj_kind == SrcObjKind::Dir && !src_path.ends_with('/'),
            )
        })
        .or_else(|| {
            (src_obj_kind == SrcObjKind::File)
                .then(|| {
                    fs::symlink_metadata(src_path)
                        .ok()
                        .map(|meta| EtaWorkload::from_file(meta.len()))
                })
                .flatten()
        });
    let eta_progress = Arc::new(AtomicEtaProgress::default());
    let transfer_start = Instant::now();
    print_transfer_columns_header();
    let io_window_for_avg = ProcessIoWindow::from_pid(std::process::id());
    let io_start_counters = io_window_for_avg.current_totals();
    let device_window_for_avg = DeviceIoWindow::from_transfer_paths(src_path, dst_path);
    let device_start_totals = device_window_for_avg.current_totals();
    let transfer_start_for_ticker = transfer_start;
    let io_start_counters_for_ticker = io_start_counters;
    let device_start_totals_for_ticker = device_start_totals;
    let src_path_for_ticker = src_path.to_string();
    let dst_path_for_ticker = dst_path.to_string();
    let eta_workload_for_ticker = eta_workload;
    let eta_progress_for_ticker = Arc::clone(&eta_progress);

    let done_for_ticker = Arc::clone(&done);
    let io_rates_shared = Arc::new(Mutex::new(TransferProgressRates::default()));
    let io_rates_for_ticker = Arc::clone(&io_rates_shared);
    let (ticker_stop_tx, ticker_stop_rx) = mpsc::channel::<()>();
    let ticker = thread::spawn(move || {
        let mut io_window = ProcessIoWindow::from_pid(std::process::id());
        let _ = io_window.sample();
        let device_window =
            DeviceIoWindow::from_transfer_paths(&src_path_for_ticker, &dst_path_for_ticker);
        let mut last_device_totals = device_start_totals_for_ticker;
        let mut last_device_at = transfer_start_for_ticker;
        let mut last_done_bytes: u64 = 0;
        let mut last_done_at = transfer_start_for_ticker;
        let mut eta_estimator = TransferEtaEstimator::default();
        loop {
            match ticker_stop_rx.recv_timeout(Duration::from_millis(200)) {
                Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let now = Instant::now();
                    let done_bytes = done_for_ticker.load(Ordering::Relaxed);
                    let mut io_rates = io_window.sample();
                    let dt = now.duration_since(last_done_at).as_secs_f64().max(1e-6);
                    io_rates.write_all_bps =
                        Some(done_bytes.saturating_sub(last_done_bytes) as f64 / dt);
                    last_done_bytes = done_bytes;
                    last_done_at = now;
                    let io_delta =
                        proc_io_deltas(io_start_counters_for_ticker, io_window.last_counters);
                    let device_now_totals = device_window.current_totals();
                    let device_delta =
                        device_io_deltas(device_start_totals_for_ticker, device_now_totals);
                    let dt_dev = now.duration_since(last_device_at).as_secs_f64().max(1e-6);
                    io_rates.read_complete_bps =
                        counter_delta(last_device_totals.0, device_now_totals.0)
                            .map(|v| v as f64 / dt_dev);
                    io_rates.write_complete_bps =
                        counter_delta(last_device_totals.1, device_now_totals.1)
                            .map(|v| v as f64 / dt_dev);
                    last_device_totals = device_now_totals;
                    last_device_at = now;
                    if let Ok(mut g) = io_rates_for_ticker.lock() {
                        *g = io_rates;
                    }
                    print_transfer_progress_bars(
                        now.duration_since(transfer_start_for_ticker).as_secs_f64(),
                        planned_bytes,
                        Some(done_bytes),
                        "Transfer",
                        io_rates,
                        io_delta,
                        device_delta,
                        eta_workload_for_ticker,
                        Some(eta_progress_for_ticker.snapshot()),
                        Some(&mut eta_estimator),
                        false,
                        false,
                    );
                }
            }
        }
    });

    macro_rules! finish_transfer {
        ($rc:expr) => {{
            let final_done = done.load(Ordering::Relaxed);
            let elapsed = transfer_start.elapsed().as_secs_f64().max(1e-6);
            let io_end_totals = io_window_for_avg.current_totals();
            let io_delta = proc_io_deltas(io_start_counters, io_end_totals);
            let device_end_totals = device_window_for_avg.current_totals();
            let device_delta = device_io_deltas(device_start_totals, device_end_totals);
            let mut final_io_rates = io_rates_shared.lock().map(|g| *g).unwrap_or_default();
            if final_io_rates.write_all_bps.is_none() {
                final_io_rates.write_all_bps = Some(final_done as f64 / elapsed);
            }
            if final_io_rates.rchar_bps.is_none() {
                final_io_rates.rchar_bps = io_delta.rchar.map(|v| v as f64 / elapsed);
            }
            if final_io_rates.wchar_bps.is_none() {
                final_io_rates.wchar_bps = io_delta.wchar.map(|v| v as f64 / elapsed);
            }
            if final_io_rates.read_bytes_bps.is_none() {
                final_io_rates.read_bytes_bps = io_delta.read_bytes.map(|v| v as f64 / elapsed);
            }
            if final_io_rates.write_bytes_bps.is_none() {
                final_io_rates.write_bytes_bps = io_delta.write_bytes.map(|v| v as f64 / elapsed);
            }
            if final_io_rates.read_complete_bps.is_none() {
                final_io_rates.read_complete_bps =
                    device_delta.read_complete.map(|v| v as f64 / elapsed);
            }
            if final_io_rates.write_complete_bps.is_none() {
                final_io_rates.write_complete_bps =
                    device_delta.write_complete.map(|v| v as f64 / elapsed);
            }
            let _ = ticker_stop_tx.send(());
            let _ = ticker.join();
            print_transfer_progress_bars(
                elapsed,
                planned_bytes,
                Some(final_done),
                "Transfer",
                final_io_rates,
                io_delta,
                device_delta,
                eta_workload,
                Some(eta_progress.snapshot()),
                None,
                true,
                false,
            );
            return TransferOutcome {
                rc: $rc,
                bytes_done: final_done,
                elapsed_s: elapsed,
                progress_snapshot: Some(ProgressSnapshot {
                    elapsed_s: elapsed,
                    planned_bytes,
                    write_all_total: Some(final_done),
                    phase_label: "Transfer",
                    rates: final_io_rates,
                    proc_deltas: io_delta,
                    device_deltas: device_delta,
                    eta_workload,
                    eta_progress: Some(eta_progress.snapshot()),
                }),
            };
        }};
    }

    match src_obj_kind {
        SrcObjKind::File => {
            let src = Path::new(src_path);
            let mut dst_buf = PathBuf::from(dst_path);
            let dst_is_symlink = fs::symlink_metadata(&dst_buf)
                .map(|md| md.file_type().is_symlink())
                .unwrap_or(false);
            if dst_buf.is_dir() && !(replace_dest_symlink && dst_is_symlink) {
                let src_name = match src.file_name() {
                    Some(v) => v,
                    None => finish_transfer!(1),
                };
                dst_buf = dst_buf.join(src_name);
            }
            let dst = dst_buf.as_path();
            let src_lmd = match fs::symlink_metadata(src) {
                Ok(v) => v,
                Err(_) => finish_transfer!(1),
            };
            if src_lmd.file_type().is_symlink() {
                let needs_copy = !symlink_targets_equal(src, dst);
                if needs_copy && copy_symlink(src, dst).is_err() {
                    finish_transfer!(1);
                }
                eta_progress.mark_file(0);
                finish_transfer!(0);
            }
            let src_meta = match fs::metadata(src) {
                Ok(v) => v,
                Err(_) => finish_transfer!(1),
            };
            let src_mtime = src_meta.modified().ok();
            let dst_lmd = fs::symlink_metadata(dst).ok();
            let dst_is_symlink = dst_lmd
                .as_ref()
                .map(|md| md.file_type().is_symlink())
                .unwrap_or(false);
            let dst_exists = dst_lmd.is_some();
            let dst_meta = if replace_dest_symlink && dst_is_symlink {
                None
            } else {
                fs::metadata(dst).ok()
            };
            let dst_size = dst_meta.as_ref().map(|m| m.len());
            let dst_mtime = dst_meta.as_ref().and_then(|m| m.modified().ok());
            let needs_copy = regular_file_collision_change(
                merge_collision_policy,
                src_meta.len(),
                src_mtime,
                dst_exists,
                dst_size,
                dst_mtime,
            )
            .is_some();
            if needs_copy {
                let _permit =
                    acquire_file_write_permit(inflight_limiter.as_ref(), src_meta.len(), media);
                if replace_dest_symlink
                    && fs::symlink_metadata(dst)
                        .map(|md| md.file_type().is_symlink())
                        .unwrap_or(false)
                    && fs::remove_file(dst).is_err()
                {
                    finish_transfer!(1);
                }
                if copy_file_preserve_with_progress_buf(src, dst, copy_buf_bytes, |n| {
                    done.fetch_add(n, Ordering::Relaxed);
                })
                .is_err()
                {
                    finish_transfer!(1);
                }
            }
            eta_progress.mark_file(src_meta.len());
        }
        SrcObjKind::Dir => {
            let src_no_trailing = src_path.trim_end_matches('/');
            let include_root = !src_path.ends_with('/');
            let src_root = Path::new(src_no_trailing);
            let dst_base = Path::new(dst_path.trim_end_matches('/'));
            let src_base = src_root
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();

            if include_root {
                if fs::create_dir_all(dst_base.join(&src_base)).is_err() {
                    finish_transfer!(1);
                }
                eta_progress.mark_dir();
            } else {
                if fs::create_dir_all(dst_base).is_err() {
                    finish_transfer!(1);
                }
            }

            if let Some(m) = manifest {
                for rel in &m.dirs {
                    let (dst_dir, _) = map_dir_dest(include_root, &src_base, rel, dst_base);
                    if fs::create_dir_all(&dst_dir).is_err() {
                        finish_transfer!(1);
                    }
                    eta_progress.mark_dir();
                }
                let copy_ok = m
                    .copy_files
                    .par_iter()
                    .map(|entry| {
                        let src_file = src_root.join(&*entry.rel);
                        let (dst_item, _) =
                            map_dir_dest(include_root, &src_base, &entry.rel, dst_base);
                        let src_md = match fs::symlink_metadata(&src_file) {
                            Ok(md) => md,
                            Err(_) => return false,
                        };
                        if src_md.file_type().is_symlink() {
                            let ok = copy_symlink(&src_file, &dst_item).is_ok();
                            if ok {
                                eta_progress.mark_file(0);
                            }
                            ok
                        } else if src_md.is_file() {
                            let src_mtime = src_md.modified().ok();
                            let dst_lmd = fs::symlink_metadata(&dst_item).ok();
                            let dst_is_symlink = dst_lmd
                                .as_ref()
                                .map(|md| md.file_type().is_symlink())
                                .unwrap_or(false);
                            let dst_exists = dst_lmd.is_some();
                            let dst_meta = if replace_dest_symlink && dst_is_symlink {
                                None
                            } else {
                                fs::metadata(&dst_item).ok()
                            };
                            let dst_size = dst_meta.as_ref().map(|m| m.len());
                            let dst_mtime = dst_meta.as_ref().and_then(|m| m.modified().ok());
                            let needs_copy = regular_file_collision_change(
                                merge_collision_policy,
                                src_md.len(),
                                src_mtime,
                                dst_exists,
                                dst_size,
                                dst_mtime,
                            )
                            .is_some();
                            if !needs_copy {
                                eta_progress.mark_file(src_md.len());
                                return true;
                            }
                            let _permit = acquire_file_write_permit(
                                inflight_limiter.as_ref(),
                                src_md.len(),
                                media,
                            );
                            if replace_dest_symlink
                                && fs::symlink_metadata(&dst_item)
                                    .map(|md| md.file_type().is_symlink())
                                    .unwrap_or(false)
                                && fs::remove_file(&dst_item).is_err()
                            {
                                return false;
                            }
                            let ok = copy_file_preserve_with_progress_buf(
                                &src_file,
                                &dst_item,
                                copy_buf_bytes,
                                |n| {
                                    done.fetch_add(n, Ordering::Relaxed);
                                },
                            )
                            .is_ok();
                            if ok {
                                eta_progress.mark_file(src_md.len());
                            }
                            ok
                        } else {
                            true
                        }
                    })
                    .reduce(|| true, |a, b| a && b);
                if !copy_ok {
                    finish_transfer!(1);
                }
                preserve_directory_times_tree(
                    src_root,
                    dst_base,
                    include_root,
                    &src_base,
                    manifest.map(|m| m.dir_times.as_slice()),
                );
            } else {
                let mut entries: Vec<PathBuf> = WalkDir::new(src_root)
                    .sort(false)
                    .skip_hidden(false)
                    .parallelism(jwalk::Parallelism::RayonDefaultPool {
                        busy_timeout: Duration::from_secs(0),
                    })
                    .into_iter()
                    .filter_map(Result::ok)
                    .map(|e| e.path().to_path_buf())
                    .collect();
                entries.sort();

                for p in entries {
                    if p == src_root {
                        continue;
                    }
                    let rel = normalize_rel(p.strip_prefix(src_root).unwrap_or(Path::new("")));
                    if exclude_rel
                        .map(|prefix| rel_matches_prefix(&rel, prefix))
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    let (dst_item, _) = map_dir_dest(include_root, &src_base, &rel, dst_base);
                    let md = match fs::symlink_metadata(&p) {
                        Ok(v) => v,
                        Err(_) => finish_transfer!(1),
                    };
                    if md.is_dir() {
                        if fs::create_dir_all(&dst_item).is_err() {
                            finish_transfer!(1);
                        }
                        eta_progress.mark_dir();
                        continue;
                    }
                    if md.file_type().is_symlink() {
                        let needs_copy = !symlink_targets_equal(&p, &dst_item);
                        if needs_copy && copy_symlink(&p, &dst_item).is_err() {
                            finish_transfer!(1);
                        }
                        eta_progress.mark_file(0);
                        continue;
                    }
                    if !md.is_file() {
                        continue;
                    }
                    let src_mtime = md.modified().ok();
                    let dst_lmd = fs::symlink_metadata(&dst_item).ok();
                    let dst_is_symlink = dst_lmd
                        .as_ref()
                        .map(|meta| meta.file_type().is_symlink())
                        .unwrap_or(false);
                    let dst_exists = dst_lmd.is_some();
                    let dst_meta = if replace_dest_symlink && dst_is_symlink {
                        None
                    } else {
                        fs::metadata(&dst_item).ok()
                    };
                    let dst_size = dst_meta.as_ref().map(|m| m.len());
                    let dst_mtime = dst_meta.as_ref().and_then(|m| m.modified().ok());
                    let needs_copy = regular_file_collision_change(
                        merge_collision_policy,
                        md.len(),
                        src_mtime,
                        dst_exists,
                        dst_size,
                        dst_mtime,
                    )
                    .is_some();
                    if needs_copy {
                        let _permit =
                            acquire_file_write_permit(inflight_limiter.as_ref(), md.len(), media);
                        if replace_dest_symlink
                            && fs::symlink_metadata(&dst_item)
                                .map(|meta| meta.file_type().is_symlink())
                                .unwrap_or(false)
                            && fs::remove_file(&dst_item).is_err()
                        {
                            finish_transfer!(1);
                        }
                        if copy_file_preserve_with_progress_buf(
                            &p,
                            &dst_item,
                            copy_buf_bytes,
                            |n| {
                                done.fetch_add(n, Ordering::Relaxed);
                            },
                        )
                        .is_err()
                        {
                            finish_transfer!(1);
                        }
                    }
                    eta_progress.mark_file(md.len());
                }
                preserve_directory_times_tree(
                    src_root,
                    dst_base,
                    include_root,
                    &src_base,
                    manifest.map(|m| m.dir_times.as_slice()),
                );
            }
        }
    }

    finish_transfer!(0)
}
