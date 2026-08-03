//! Application runtime support shared by the feature modules.
//!
//! This module contains process-wide configuration, terminal logging, and
//! transfer-tuning helpers. The command flow itself lives in `entry`.

use filetime::{set_file_times, FileTime};
use jemallocator::Jemalloc;
use jwalk::WalkDir;
use nix::sys::stat::{major, minor};
use nix::sys::statvfs::statvfs;
use rayon::prelude::*;
use regex::Regex;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::env;
use std::ffi::CString;
use std::fmt::Write as FmtWrite;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{symlink, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
#[path = "model.rs"]
mod model;
use model::*;
#[path = "progress.rs"]
mod progress;
use progress::*;
#[path = "telemetry.rs"]
mod telemetry;
use telemetry::*;
#[path = "resolve.rs"]
mod resolve;
use resolve::*;
#[path = "scanner.rs"]
mod scanner;
use scanner::*;
#[path = "copy_engine.rs"]
mod copy_engine;
use copy_engine::*;
#[path = "backup.rs"]
mod backup;
use backup::*;
#[path = "cleanup.rs"]
mod cleanup;
use cleanup::*;
#[path = "rsync.rs"]
mod rsync;
use rsync::*;
#[path = "local.rs"]
mod local;
use local::*;
#[path = "orchestrator.rs"]
mod orchestrator;
use orchestrator::*;
#[path = "ui.rs"]
mod ui;
use ui::*;
#[path = "remote.rs"]
mod remote;
use remote::*;
#[path = "entry.rs"]
mod entry;
pub(crate) use entry::run;
use entry::{format_number, print_counts_table, print_preview_counts_table};
#[path = "policy.rs"]
mod policy;
use policy::*;
#[path = "cli.rs"]
mod cli;
use cli::*;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const OKBLUE: &str = "\x1b[94m";
const LIGHT_TEAL: &str = "\x1b[96m";
const OKGREEN: &str = "\x1b[92m";
const WARNING: &str = "\x1b[93m";
const FAIL: &str = "\x1b[91m";
const WHITE: &str = "\x1b[97m";
const DIM: &str = "\x1b[90m";
const ENDC: &str = "\x1b[0m";

fn log(mode: TransferMode, msg: &str, level: LogLevel) {
    finish_progress_render_state();
    match level {
        LogLevel::Error => eprintln!("{FAIL}ERROR: {msg}{ENDC}"),
        LogLevel::Warn => eprintln!("{WARNING}WARNING: {msg}{ENDC}"),
        LogLevel::Info => println!("{OKBLUE}{}: {msg}{ENDC}", mode.word()),
    }
}

fn log_transfer_complete(mode: TransferMode) {
    if matches!(mode, TransferMode::Copy) {
        println!();
    }
    log(
        mode,
        &format!("{} complete.", mode.word_cap()),
        LogLevel::Info,
    );
    if matches!(mode, TransferMode::Copy) {
        println!();
    }
}

fn option_u64_saturating_sub(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.saturating_sub(y)),
        _ => None,
    }
}

fn option_u64_saturating_add(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.saturating_add(y)),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}

fn parse_env_threads() -> Option<usize> {
    let raw = env::var("COPY_RS_THREADS").ok()?;
    let parsed = raw.trim().parse::<usize>().ok()?;
    if parsed == 0 {
        return None;
    }
    Some(parsed)
}

fn parse_env_u64(name: &str) -> Option<u64> {
    let raw = env::var(name).ok()?;
    let parsed = raw.trim().parse::<u64>().ok()?;
    if parsed == 0 {
        return None;
    }
    Some(parsed)
}

fn preferred_thread_count(media: MediaKind) -> usize {
    if let Some(n) = parse_env_threads() {
        return match media {
            MediaKind::Hdd => n.clamp(1, 2),
            MediaKind::Nvme => n.clamp(2, 32),
            MediaKind::Other => n.clamp(1, 8),
        };
    }
    let logical = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    match media {
        MediaKind::Hdd => logical.clamp(2, 2),
        MediaKind::Nvme => logical.clamp(2, 32),
        MediaKind::Other => logical.clamp(1, 8),
    }
}

fn transfer_media_kind(source: MediaKind, destination: MediaKind) -> MediaKind {
    if source == MediaKind::Hdd || destination == MediaKind::Hdd {
        MediaKind::Hdd
    } else if source == MediaKind::Nvme && destination == MediaKind::Nvme {
        MediaKind::Nvme
    } else {
        MediaKind::Other
    }
}

fn copy_chunk_bytes_for_media(media: MediaKind) -> usize {
    if let Some(kib) = parse_env_u64("COPY_RS_CHUNK_KIB") {
        return (kib.saturating_mul(1024)).max(64 * 1024) as usize;
    }
    match media {
        MediaKind::Hdd => 4 * 1024 * 1024,
        MediaKind::Nvme => 2 * 1024 * 1024,
        MediaKind::Other => 1024 * 1024,
    }
}

fn copy_chunk_bytes_for_file(media: MediaKind, file_size: u64) -> usize {
    const TINY: u64 = 64 * 1024;
    const MEDIUM: u64 = 4 * 1024 * 1024;
    if file_size <= TINY {
        64 * 1024
    } else if file_size <= MEDIUM {
        256 * 1024
    } else {
        copy_chunk_bytes_for_media(media)
    }
}

fn inflight_max_bytes_for_media(media: MediaKind) -> Option<u64> {
    if let Some(mib) = parse_env_u64("COPY_RS_MAX_INFLIGHT_MIB") {
        return Some(mib.saturating_mul(1024 * 1024));
    }
    match media {
        MediaKind::Hdd => Some(96 * 1024 * 1024),
        _ => None,
    }
}

fn inflight_reserve_bytes_for_file(file_size: u64, media: MediaKind) -> u64 {
    match media {
        MediaKind::Hdd => {
            let min_reserve = 4 * 1024 * 1024u64;
            let max_reserve = 32 * 1024 * 1024u64;
            file_size.max(min_reserve).min(max_reserve)
        }
        _ => file_size.max(1024 * 1024),
    }
}

fn acquire_file_write_permit(
    limiter: Option<&Arc<InflightWriteLimiter>>,
    file_size: u64,
    media: MediaKind,
) -> Option<InflightWritePermit> {
    let lim = limiter?;
    let reserve = inflight_reserve_bytes_for_file(file_size, media);
    Some(lim.acquire(reserve))
}

fn configure_rayon_threads_for_media(media: MediaKind) {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let threads = preferred_thread_count(media);
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global();
    });
}

fn fmt_mode_word(label: &str, active: bool) -> String {
    if active {
        format!("{OKGREEN}{label}{ENDC}")
    } else {
        format!("{DIM}{label}{ENDC}")
    }
}

fn print_preview_root_line(preview_root: &Path, highlight_new_leaf: bool, emphasize_non_new: bool) {
    let full = preview_root.display().to_string();
    if !highlight_new_leaf {
        if emphasize_non_new {
            println!("{WARNING}{}{ENDC}", full);
        } else {
            println!("{full}");
        }
        return;
    }

    let trimmed = full.trim_end_matches('/');
    let p = Path::new(trimmed);
    let leaf = match p.file_name() {
        Some(v) => v.to_string_lossy().to_string(),
        None => {
            println!("{WARNING}{}{ENDC}", full);
            return;
        }
    };
    if leaf.is_empty() {
        println!("{WARNING}{}{ENDC}", full);
        return;
    }

    let parent = p
        .parent()
        .map(|x| x.display().to_string())
        .unwrap_or_default();
    let parent_trimmed = parent.trim_end_matches('/');
    if parent_trimmed.is_empty() {
        if p.is_absolute() {
            println!("{WARNING}/{OKGREEN}{leaf}/{ENDC}");
        } else {
            println!("{OKGREEN}{leaf}/{ENDC}");
        }
    } else {
        println!("{WARNING}{parent_trimmed}/{OKGREEN}{leaf}/{ENDC}");
    }
}
