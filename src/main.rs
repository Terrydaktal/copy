use filetime::{set_file_times, FileTime};
use jemallocator::Jemalloc;
use jwalk::WalkDir;
use nix::sys::stat::{major, minor};
use nix::sys::statvfs::statvfs;
use rayon::prelude::*;
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{self, BufRead, Read, Write};
use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, Once};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const OKBLUE: &str = "\x1b[94m";
const OKGREEN: &str = "\x1b[92m";
const WARNING: &str = "\x1b[93m";
const FAIL: &str = "\x1b[91m";
const WHITE: &str = "\x1b[97m";
const DIM: &str = "\x1b[90m";
const ENDC: &str = "\x1b[0m";

enum LogLevel {
    Info,
    Warn,
    Error,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransferMode {
    Copy,
    Move,
}

impl TransferMode {
    fn word(self) -> &'static str {
        match self {
            TransferMode::Copy => "copy",
            TransferMode::Move => "move",
        }
    }

    fn word_cap(self) -> &'static str {
        match self {
            TransferMode::Copy => "Copy",
            TransferMode::Move => "Move",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SrcObjKind {
    File,
    Dir,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DstObjKind {
    Dir,
    DirExisting,
    DirNew,
    File,
    FileExistingForDir,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MediaKind {
    Nvme,
    Hdd,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransferBackend {
    Rust,
    Rsync,
}

#[derive(Clone)]
struct RemoteSpec {
    user: Option<String>,
    host: String,
    path: String,
}

#[derive(Clone)]
enum Endpoint {
    Local(PathBuf),
    Remote(RemoteSpec),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ChangeKind {
    NewFile,
    ModFile,
    NewDir,
    ModDir,
    RemovedDir,
}

#[derive(Clone)]
struct ChangeItem {
    kind: ChangeKind,
    rel: String,
}

#[derive(Default, Clone)]
struct ManifestFileEntry {
    rel: String,
    size: u64,
}

#[derive(Default, Clone)]
struct TransferManifest {
    dirs: Vec<String>,
    copy_files: Vec<ManifestFileEntry>,
    identical_files: Vec<ManifestFileEntry>,
}

struct PreScan {
    planned_bytes: u64,
    planned_bytes_exact: bool,
    total_regular_files: Option<u64>,
    total_regular_bytes: Option<u64>,
    total_dirs: Option<u64>,
    add_files: u64,
    mod_files: u64,
    unaffected_files: u64,
    add_dirs: u64,
    mod_dirs: u64,
    unaffected_dirs: u64,
    change_preview: Vec<ChangeItem>,
    has_itemized_changes: bool,
    transfer_manifest: Option<TransferManifest>,
}

impl Default for PreScan {
    fn default() -> Self {
        Self {
            planned_bytes: 0,
            planned_bytes_exact: true,
            total_regular_files: None,
            total_regular_bytes: None,
            total_dirs: None,
            add_files: 0,
            mod_files: 0,
            unaffected_files: 0,
            add_dirs: 0,
            mod_dirs: 0,
            unaffected_dirs: 0,
            change_preview: Vec::new(),
            has_itemized_changes: false,
            transfer_manifest: None,
        }
    }
}

#[derive(Default)]
struct CliArgs {
    source: String,
    destination: String,
    extra: Vec<String>,
    move_mode: bool,
    sudo: bool,
    overwrite: bool,
    contents_only: bool,
    backup: bool,
    showall: bool,
    preview_only: bool,
    preview_lite: bool,
}

struct CmdOutput {
    code: i32,
}

#[derive(Clone, Copy)]
struct TransferOutcome {
    rc: i32,
    bytes_done: u64,
    elapsed_s: f64,
    io_read_bytes: Option<u64>,
    io_write_bytes: Option<u64>,
}

#[derive(Clone, Copy, Default)]
struct DeviceIoRates {
    src_read_bps: Option<f64>,
    dst_write_bps: Option<f64>,
}

#[derive(Default)]
struct DeviceIoWindow {
    src_keys: Vec<(u64, u64)>,
    dst_keys: Vec<(u64, u64)>,
    last_at: Option<Instant>,
    last_src_read_bytes: Option<u64>,
    last_dst_write_bytes: Option<u64>,
}

struct InflightWriteLimiter {
    max_bytes: u64,
    used_bytes: Mutex<u64>,
    cv: Condvar,
}

struct InflightWritePermit {
    limiter: Arc<InflightWriteLimiter>,
    reserved: u64,
}

impl Drop for InflightWritePermit {
    fn drop(&mut self) {
        self.limiter.release(self.reserved);
    }
}

impl InflightWriteLimiter {
    fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes: max_bytes.max(1),
            used_bytes: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    fn acquire(self: &Arc<Self>, want_bytes: u64) -> InflightWritePermit {
        let reserve = want_bytes.max(1).min(self.max_bytes);
        let mut used = self.used_bytes.lock().unwrap_or_else(|e| e.into_inner());
        while (*used + reserve > self.max_bytes) && *used > 0 {
            used = self.cv.wait(used).unwrap_or_else(|e| e.into_inner());
        }
        *used = used.saturating_add(reserve);
        drop(used);
        InflightWritePermit {
            limiter: Arc::clone(self),
            reserved: reserve,
        }
    }

    fn release(&self, bytes: u64) {
        let mut used = self.used_bytes.lock().unwrap_or_else(|e| e.into_inner());
        *used = used.saturating_sub(bytes);
        self.cv.notify_all();
    }
}

enum RsyncStreamEvent {
    Progress(u64),
    Text(String),
}

#[derive(Clone, Copy, Default)]
struct DeleteCleanupOutcome {
    files: u64,
    bytes: u64,
}

fn log(mode: TransferMode, msg: &str, level: LogLevel) {
    match level {
        LogLevel::Error => eprintln!("{FAIL}ERROR: {msg}{ENDC}"),
        LogLevel::Warn => eprintln!("{WARNING}WARNING: {msg}{ENDC}"),
        LogLevel::Info => println!("{OKBLUE}{}: {msg}{ENDC}", mode.word()),
    }
}

fn fmt_hms_ms(total_seconds: f64) -> String {
    let ms_total = (total_seconds.max(0.0) * 1000.0).round() as i64;
    let h = ms_total / 3_600_000;
    let m = (ms_total % 3_600_000) / 60_000;
    let s = (ms_total % 60_000) / 1000;
    let ms = ms_total % 1000;
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}

fn fmt_hms_tenths(total_seconds: f64) -> String {
    let tenth_total = (total_seconds.max(0.0) * 10.0).round() as i64;
    let h = tenth_total / 36_000;
    let m = (tenth_total % 36_000) / 600;
    let s = (tenth_total % 600) / 10;
    let t = tenth_total % 10;
    format!("{h:02}:{m:02}:{s:02}.{t}")
}

fn format_bytes_binary(byte_value: u64, decimals: usize) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut idx = 0usize;
    let mut value = byte_value as f64;
    while idx < units.len() - 1 && value >= 1024.0 {
        value /= 1024.0;
        idx += 1;
    }
    while idx < units.len() - 1 && (value * 10f64.powi(decimals as i32)).round() / 10f64.powi(decimals as i32) >= 1024.0 {
        value /= 1024.0;
        idx += 1;
    }
    if units[idx] == "B" {
        format!("{} B", value as u64)
    } else {
        format!("{value:.decimals$} {}", units[idx])
    }
}

fn fmt_speed_bps(bps: f64) -> String {
    let b = if bps.is_finite() && bps > 0.0 { bps as u64 } else { 0 };
    format_bytes_binary(b, 2)
}

fn fmt_bytes_col_10(byte_value: u64) -> String {
    let s = format_bytes_binary(byte_value, 2);
    if s.len() >= 11 {
        s
    } else {
        format!("{:>11}", s)
    }
}

fn fmt_rate_col(bps: f64) -> String {
    let raw = format!("{}/s", fmt_speed_bps(bps));
    if raw.len() >= 14 {
        raw
    } else {
        format!("{:>14}", raw)
    }
}

fn fmt_rate_col_opt(bps: Option<f64>) -> String {
    match bps {
        Some(v) => fmt_rate_col(v),
        None => format!("{:>14}", "--/s"),
    }
}

fn print_transfer_columns_header() {
    println!(
        "{:<10} {:>7} {:>23} | {:>14} | {:>14} {:>14}",
        "Time",
        "%",
        "Transferred / Total",
        "Throughput",
        "Read",
        "Write"
    );
}

fn print_summary_rate_line(label: &str, bps: f64, duration_s: f64, total: bool) {
    let total_suffix = if total { " (total)" } else { "" };
    let prefix = format!("{label}:");
    println!(
        "{prefix:<24}{}/s | Duration: {}{}",
        fmt_speed_bps(bps),
        fmt_hms_ms(duration_s),
        total_suffix
    );
}

fn print_duration_only_line(duration_s: f64, total: bool) {
    let total_suffix = if total { " (total)" } else { "" };
    println!("{:<38}| Duration: {}{}", "", fmt_hms_ms(duration_s), total_suffix);
}

fn print_move_speed_summary(
    avg_transfer_bps: f64,
    avg_read_bps: f64,
    avg_write_bps: f64,
    transfer_duration_s: f64,
    avg_delete_bps: f64,
    delete_duration_s: f64,
    total_duration_s: f64,
) {
    println!("{:<24}{}/s", "Average transfer speed:", fmt_speed_bps(avg_transfer_bps));
    println!("{:<24}{}/s", "Average read speed:", fmt_speed_bps(avg_read_bps));
    println!(
        "{:<24}{}/s | Duration: {}",
        "Average write speed:",
        fmt_speed_bps(avg_write_bps),
        fmt_hms_ms(transfer_duration_s)
    );
    println!(
        "{:<24}{}/s | Duration: {}",
        "Average delete speed:",
        fmt_speed_bps(avg_delete_bps),
        fmt_hms_ms(delete_duration_s)
    );
    print_duration_only_line(total_duration_s, true);
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
        return n;
    }
    let logical = thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    match media {
        MediaKind::Hdd => logical.clamp(2, 2),
        MediaKind::Nvme => logical.clamp(2, 32),
        MediaKind::Other => logical.clamp(1, 8),
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
        let _ = rayon::ThreadPoolBuilder::new().num_threads(threads).build_global();
    });
}

fn count_regular_files_any(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    if path.is_file() {
        return 1;
    }
    if !path.is_dir() {
        return 0;
    }
    WalkDir::new(path)
        .skip_hidden(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().is_file())
        .count() as u64
}

fn read_diskstats_bytes() -> io::Result<HashMap<(u64, u64), (u64, u64)>> {
    let raw = fs::read_to_string("/proc/diskstats")?;
    let mut out: HashMap<(u64, u64), (u64, u64)> = HashMap::new();
    for line in raw.lines() {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 10 {
            continue;
        }
        let maj = match cols[0].parse::<u64>() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let min = match cols[1].parse::<u64>() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let sectors_read = match cols[5].parse::<u64>() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let sectors_written = match cols[9].parse::<u64>() {
            Ok(v) => v,
            Err(_) => continue,
        };
        out.insert(
            (maj, min),
            (sectors_read.saturating_mul(512), sectors_written.saturating_mul(512)),
        );
    }
    Ok(out)
}

fn unescape_mountinfo_field(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\\'
            && i + 3 < bytes.len()
            && bytes[i + 1].is_ascii_digit()
            && bytes[i + 2].is_ascii_digit()
            && bytes[i + 3].is_ascii_digit()
        {
            let oct = &raw[i + 1..i + 4];
            if let Ok(v) = u8::from_str_radix(oct, 8) {
                out.push(v);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn mount_source_device_for_path(path: &Path) -> Option<PathBuf> {
    let probe = existing_probe_path(path)?;
    let probe_real = realpath_allow_missing(&probe);
    let raw = fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mut best: Option<(usize, PathBuf)> = None;
    for line in raw.lines() {
        let (left, right) = line.split_once(" - ")?;
        let left_cols: Vec<&str> = left.split_whitespace().collect();
        if left_cols.len() < 5 {
            continue;
        }
        let right_cols: Vec<&str> = right.split_whitespace().collect();
        if right_cols.len() < 2 {
            continue;
        }
        let mount_point = PathBuf::from(unescape_mountinfo_field(left_cols[4]));
        if !probe_real.starts_with(&mount_point) {
            continue;
        }
        let src = unescape_mountinfo_field(right_cols[1]);
        if !src.starts_with("/dev/") {
            continue;
        }
        let depth = mount_point.components().count();
        match &best {
            Some((best_depth, _)) if *best_depth >= depth => {}
            _ => best = Some((depth, PathBuf::from(src))),
        }
    }
    best.map(|(_, p)| p)
}

fn device_key_for_block_device(devnode: &Path) -> Option<(u64, u64)> {
    let md = fs::metadata(devnode).ok()?;
    let rdev = md.rdev();
    if rdev == 0 {
        return None;
    }
    Some((major(rdev), minor(rdev)))
}

fn parse_major_minor(raw: &str) -> Option<(u64, u64)> {
    let mut parts = raw.trim().split(':');
    let maj = parts.next()?.trim().parse::<u64>().ok()?;
    let min = parts.next()?.trim().parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((maj, min))
}

fn device_key_for_sys_block_name(name: &str) -> Option<(u64, u64)> {
    let dev_path = Path::new("/sys/class/block").join(name).join("dev");
    let raw = fs::read_to_string(dev_path).ok()?;
    parse_major_minor(&raw)
}

fn collect_leaf_keys_for_block_name(
    name: &str,
    visited: &mut HashSet<String>,
    out: &mut Vec<(u64, u64)>,
) {
    if !visited.insert(name.to_string()) {
        return;
    }

    let slaves_dir = Path::new("/sys/class/block").join(name).join("slaves");
    let mut had_slave = false;
    if let Ok(rd) = fs::read_dir(&slaves_dir) {
        for ent in rd.flatten() {
            let child_name = ent.file_name().to_string_lossy().to_string();
            if child_name.is_empty() {
                continue;
            }
            had_slave = true;
            collect_leaf_keys_for_block_name(&child_name, visited, out);
        }
    }

    if !had_slave {
        if let Some(k) = device_key_for_sys_block_name(name) {
            out.push(k);
        }
    }
}

fn leaf_device_keys_for_block_device(devnode: &Path) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();

    let canonical = fs::canonicalize(devnode).unwrap_or_else(|_| devnode.to_path_buf());
    if let Some(name) = canonical.file_name().and_then(|s| s.to_str()) {
        collect_leaf_keys_for_block_name(name, &mut visited, &mut out);
    }

    if out.is_empty() {
        if let Some(k) = device_key_for_block_device(devnode) {
            out.push(k);
        }
    }

    out.sort_unstable();
    out.dedup();
    out
}

fn local_path_from_transfer_arg(arg: &str) -> Option<PathBuf> {
    let trimmed = arg.trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('/') || trimmed.starts_with("./") || trimmed.starts_with("../") {
        return Some(PathBuf::from(trimmed));
    }
    None
}

fn device_keys_for_path(path: &Path) -> Vec<(u64, u64)> {
    if let Some(devnode) = mount_source_device_for_path(path) {
        let keys = leaf_device_keys_for_block_device(&devnode);
        if !keys.is_empty() {
            return keys;
        }
    }
    let Some(probe) = existing_probe_path(path) else {
        return Vec::new();
    };
    let Some(md) = fs::metadata(probe).ok() else {
        return Vec::new();
    };
    let dev = md.dev();
    vec![(major(dev), minor(dev))]
}

fn sum_diskstats_counter(
    table: &HashMap<(u64, u64), (u64, u64)>,
    keys: &[(u64, u64)],
    read_counter: bool,
) -> Option<u64> {
    if keys.is_empty() {
        return None;
    }
    let mut found = false;
    let mut total: u64 = 0;
    for key in keys {
        if let Some((read_bytes, write_bytes)) = table.get(key) {
            found = true;
            let v = if read_counter { *read_bytes } else { *write_bytes };
            total = total.saturating_add(v);
        }
    }
    if found {
        Some(total)
    } else {
        None
    }
}

fn block_name_for_dev_key(key: (u64, u64)) -> Option<String> {
    let link = PathBuf::from(format!("/sys/dev/block/{}:{}", key.0, key.1));
    let canon = fs::canonicalize(link).ok()?;
    canon.file_name().map(|s| s.to_string_lossy().to_string())
}

fn block_leaf_names_for_path(path: &Path) -> Vec<String> {
    let mut out: Vec<String> = device_keys_for_path(path)
        .into_iter()
        .filter_map(block_name_for_dev_key)
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn block_is_rotational(name: &str) -> Option<bool> {
    let p = Path::new("/sys/class/block").join(name).join("queue/rotational");
    let raw = fs::read_to_string(p).ok()?;
    match raw.trim() {
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

fn parse_scheduler_info(raw: &str) -> (Option<String>, HashSet<String>) {
    let mut active: Option<String> = None;
    let mut available: HashSet<String> = HashSet::new();
    for tok in raw.split_whitespace() {
        let is_active = tok.starts_with('[') && tok.ends_with(']');
        let name = tok.trim_matches('[').trim_matches(']').to_string();
        if name.is_empty() {
            continue;
        }
        if is_active {
            active = Some(name.clone());
        }
        available.insert(name);
    }
    (active, available)
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn set_block_scheduler(name: &str, scheduler: &str, use_sudo: bool) -> bool {
    let p = Path::new("/sys/class/block").join(name).join("queue/scheduler");
    if fs::write(&p, scheduler).is_ok() {
        return true;
    }
    if !use_sudo {
        return false;
    }
    let script = format!(
        "printf %s {} > {}",
        shell_single_quote(scheduler),
        shell_single_quote(&p.display().to_string())
    );
    let cmd = vec!["sh".to_string(), "-c".to_string(), script];
    run_command_capture(&cmd, true).map(|o| o.code == 0).unwrap_or(false)
}

fn prefer_hdd_scheduler_for_paths(paths: &[&Path], use_sudo: bool, mode: TransferMode) {
    let mut block_names: BTreeSet<String> = BTreeSet::new();
    for p in paths {
        for name in block_leaf_names_for_path(p) {
            block_names.insert(name);
        }
    }
    if block_names.is_empty() {
        return;
    }

    let mut changed: Vec<String> = Vec::new();
    let mut failed: Vec<String> = Vec::new();
    for name in block_names {
        if !matches!(block_is_rotational(&name), Some(true)) {
            continue;
        }
        let sched_path = Path::new("/sys/class/block").join(&name).join("queue/scheduler");
        let raw = match fs::read_to_string(&sched_path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let (active, available) = parse_scheduler_info(&raw);
        let desired = if available.contains("mq-deadline") {
            Some("mq-deadline")
        } else if available.contains("deadline") {
            Some("deadline")
        } else {
            None
        };
        let Some(desired_scheduler) = desired else {
            continue;
        };
        if active.as_deref() == Some(desired_scheduler) {
            continue;
        }
        if set_block_scheduler(&name, desired_scheduler, use_sudo) {
            changed.push(format!("{name}:{desired_scheduler}"));
        } else {
            failed.push(name);
        }
    }

    if !changed.is_empty() {
        log(
            mode,
            &format!("Using preferred HDD scheduler on {}", changed.join(", ")),
            LogLevel::Info,
        );
    }
    if !failed.is_empty() {
        log(
            mode,
            &format!(
                "Could not set preferred HDD scheduler on {} (insufficient permissions or unsupported device).",
                failed.join(", ")
            ),
            LogLevel::Warn,
        );
    }
}

impl DeviceIoWindow {
    fn from_transfer_paths(src_path: &str, dst_path: &str) -> Self {
        let src_keys = local_path_from_transfer_arg(src_path)
            .as_deref()
            .map(device_keys_for_path)
            .unwrap_or_default();
        let dst_keys = local_path_from_transfer_arg(dst_path)
            .as_deref()
            .map(device_keys_for_path)
            .unwrap_or_default();
        Self {
            src_keys,
            dst_keys,
            ..Self::default()
        }
    }

    fn sample(&mut self) -> DeviceIoRates {
        let mut rates = DeviceIoRates::default();
        let now = Instant::now();
        let table = match read_diskstats_bytes() {
            Ok(v) => v,
            Err(_) => return rates,
        };

        let src_read_now = sum_diskstats_counter(&table, &self.src_keys, true);
        let dst_write_now = sum_diskstats_counter(&table, &self.dst_keys, false);

        if let Some(prev_at) = self.last_at {
            let dt = now.duration_since(prev_at).as_secs_f64().max(1e-6);
            if let (Some(cur), Some(prev)) = (src_read_now, self.last_src_read_bytes) {
                rates.src_read_bps = Some(cur.saturating_sub(prev) as f64 / dt);
            }
            if let (Some(cur), Some(prev)) = (dst_write_now, self.last_dst_write_bytes) {
                rates.dst_write_bps = Some(cur.saturating_sub(prev) as f64 / dt);
            }
        }

        self.last_at = Some(now);
        if src_read_now.is_some() {
            self.last_src_read_bytes = src_read_now;
        }
        if dst_write_now.is_some() {
            self.last_dst_write_bytes = dst_write_now;
        }
        rates
    }

    fn current_totals(&self) -> (Option<u64>, Option<u64>) {
        let table = match read_diskstats_bytes() {
            Ok(v) => v,
            Err(_) => return (None, None),
        };
        (
            sum_diskstats_counter(&table, &self.src_keys, true),
            sum_diskstats_counter(&table, &self.dst_keys, false),
        )
    }
}

fn counter_delta(start: Option<u64>, end: Option<u64>) -> Option<u64> {
    match (start, end) {
        (Some(a), Some(b)) => Some(b.saturating_sub(a)),
        _ => None,
    }
}

fn existing_probe_path(path: &Path) -> Option<PathBuf> {
    let mut probe = if path.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        path.to_path_buf()
    };
    loop {
        if probe.exists() {
            return Some(probe);
        }
        if !probe.pop() {
            return None;
        }
    }
}

fn destination_available_bytes(path: &Path) -> io::Result<(u64, PathBuf)> {
    let probe = existing_probe_path(path)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No existing destination ancestor path found"))?;
    let stats = statvfs(&probe)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("statvfs failed for {}: {e}", probe.display())))?;
    let avail = stats
        .blocks_available()
        .saturating_mul(stats.fragment_size() as u64);
    Ok((avail, probe))
}

fn can_fast_rename_same_fs(source: &Path, target: &Path) -> bool {
    source
        .parent()
        .and_then(|p| fs::metadata(p).ok())
        .map(|m| m.dev())
        .zip(target.parent().and_then(|p| fs::metadata(p).ok()).map(|m| m.dev()))
        .map(|(a, b)| a == b)
        .unwrap_or(false)
}

fn expand_user(value: &str) -> String {
    if let Some(rest) = value.strip_prefix("~/") {
        if let Ok(home) = env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    if value == "~" {
        if let Ok(home) = env::var("HOME") {
            return home;
        }
    }
    value.to_string()
}

fn realpath_allow_missing(input: &Path) -> PathBuf {
    let abs = if input.is_absolute() {
        input.to_path_buf()
    } else {
        env::current_dir().unwrap_or_else(|_| PathBuf::from("/")).join(input)
    };

    if abs.exists() {
        return fs::canonicalize(&abs).unwrap_or(abs);
    }

    let mut tail: Vec<PathBuf> = Vec::new();
    let mut cur = abs.clone();
    while !cur.exists() {
        if let Some(name) = cur.file_name() {
            tail.push(PathBuf::from(name));
        }
        if let Some(parent) = cur.parent() {
            cur = parent.to_path_buf();
        } else {
            break;
        }
    }

    let mut resolved = if cur.exists() {
        fs::canonicalize(&cur).unwrap_or(cur)
    } else {
        cur
    };
    for part in tail.iter().rev() {
        resolved.push(part);
    }
    resolved
}

fn to_real_path(value: &str) -> PathBuf {
    let expanded = expand_user(value);
    realpath_allow_missing(Path::new(&expanded))
}

fn parse_remote_spec(value: &str) -> Option<RemoteSpec> {
    if value.contains("://") {
        return None;
    }
    let idx = value.find(':')?;
    let lhs = &value[..idx];
    let rhs = &value[idx + 1..];
    if lhs.is_empty() || rhs.is_empty() {
        return None;
    }
    if lhs.contains('/') || lhs.contains('\\') || lhs.contains(char::is_whitespace) {
        return None;
    }
    let (user, host) = match lhs.rfind('@') {
        Some(at) => {
            let u = lhs[..at].trim();
            let h = lhs[at + 1..].trim();
            if u.is_empty() || h.is_empty() {
                return None;
            }
            (Some(u.to_string()), h.to_string())
        }
        None => (None, lhs.to_string()),
    };
    Some(RemoteSpec {
        user,
        host,
        path: rhs.to_string(),
    })
}

fn wildcard_match(pat: &str, text: &str) -> bool {
    let p: Vec<char> = pat.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let mut pi = 0usize;
    let mut ti = 0usize;
    let mut star: Option<usize> = None;
    let mut match_ti = 0usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            pi += 1;
            match_ti = ti;
        } else if let Some(s) = star {
            pi = s + 1;
            match_ti += 1;
            ti = match_ti;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn ssh_config_user_for_host_from_text(host: &str, txt: &str) -> Option<String> {
    let mut in_match = true;
    let mut found: Option<String> = None;

    for raw in txt.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let key = match parts.next() {
            Some(k) => k.to_ascii_lowercase(),
            None => continue,
        };
        let val = parts.collect::<Vec<_>>().join(" ");
        if val.is_empty() {
            continue;
        }

        if key == "host" {
            let mut has_positive = false;
            let mut matched_positive = false;
            let mut matched_negative = false;
            for pat in val.split_whitespace() {
                if let Some(neg) = pat.strip_prefix('!') {
                    if !neg.is_empty() && wildcard_match(neg, host) {
                        matched_negative = true;
                    }
                } else {
                    has_positive = true;
                    if wildcard_match(pat, host) {
                        matched_positive = true;
                    }
                }
            }
            in_match = has_positive && matched_positive && !matched_negative;
            continue;
        }

        if in_match && key == "user" && found.is_none() {
            found = Some(val);
        }
    }
    found
}

fn ssh_config_user_for_host(host: &str) -> Option<String> {
    let home = env::var("HOME").ok()?;
    let cfg_path = Path::new(&home).join(".ssh/config");
    let txt = fs::read_to_string(cfg_path).ok()?;
    ssh_config_user_for_host_from_text(host, &txt)
}

fn enrich_remote_spec(mut r: RemoteSpec) -> RemoteSpec {
    if r.user.is_none() {
        r.user = ssh_config_user_for_host(&r.host);
    }
    r
}

fn endpoint_to_rsync(
    endpoint: &Endpoint,
    as_source: bool,
    contents_mode: bool,
    local_src_kind: Option<SrcObjKind>,
) -> String {
    match endpoint {
        Endpoint::Local(p) => {
            let mut s = p.display().to_string();
            if as_source
                && contents_mode
                && matches!(local_src_kind, Some(SrcObjKind::Dir))
                && !s.ends_with('/')
            {
                s.push('/');
            }
            if !as_source && p.is_dir() && !s.ends_with('/') {
                s.push('/');
            }
            s
        }
        Endpoint::Remote(r) => {
            let mut path = r.path.clone();
            if as_source && contents_mode && !path.ends_with('/') {
                path.push('/');
            }
            let user_host = match &r.user {
                Some(u) => format!("{u}@{}", r.host),
                None => r.host.clone(),
            };
            format!("{user_host}:{path}")
        }
    }
}

fn resolve_source(value: &str, mode: TransferMode) -> Result<(PathBuf, SrcObjKind), i32> {
    let p = to_real_path(value);
    if !p.exists() {
        log(mode, &format!("Source path does not exist: {value}"), LogLevel::Error);
        return Err(1);
    }
    if p.is_dir() {
        return Ok((p, SrcObjKind::Dir));
    }
    if p.is_file() {
        return Ok((p, SrcObjKind::File));
    }
    log(mode, &format!("Source path must be a file or directory: {value}"), LogLevel::Error);
    Err(1)
}

fn resolve_destination_for_file(value: &str, mode: TransferMode) -> Result<(PathBuf, DstObjKind), i32> {
    let dst_real = to_real_path(value);
    if dst_real.exists() {
        if dst_real.is_dir() {
            return Ok((dst_real, DstObjKind::Dir));
        }
        return Ok((dst_real, DstObjKind::File));
    }
    let parent = dst_real.parent().unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        log(
            mode,
            &format!("Destination parent directory does not exist: {}", parent.display()),
            LogLevel::Error,
        );
        return Err(1);
    }
    Ok((dst_real, DstObjKind::File))
}

fn resolve_destination_for_dir(
    value: &str,
    mode: TransferMode,
    allow_existing_file: bool,
) -> Result<(PathBuf, DstObjKind), i32> {
    let p = to_real_path(value);
    if p.exists() {
        if !p.is_dir() {
            if allow_existing_file {
                return Ok((p, DstObjKind::FileExistingForDir));
            }
            log(
                mode,
                &format!("Destination path must be a directory (or a new directory path): {value}"),
                LogLevel::Error,
            );
            return Err(1);
        }
        return Ok((p, DstObjKind::DirExisting));
    }
    let parent = p.parent().unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        log(
            mode,
            &format!("Destination parent directory does not exist: {}", parent.display()),
            LogLevel::Error,
        );
        return Err(1);
    }
    Ok((p, DstObjKind::DirNew))
}

fn destination_file_counts(destination_root: &Path, source_rel_files: &HashSet<String>) -> (u64, u64) {
    if !destination_root.is_dir() {
        return (0, 0);
    }

    WalkDir::new(destination_root)
        .skip_hidden(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().is_file())
        .fold((0u64, 0u64), |(total, unaffected), e| {
            let rel = e
                .path()
                .strip_prefix(destination_root)
                .ok()
                .map(normalize_rel)
                .unwrap_or_default();
            let is_unaffected = !rel.is_empty() && !source_rel_files.contains(&rel);
            (total + 1, unaffected + u64::from(is_unaffected))
        })
}

fn count_directories_any(path: &Path, include_root: bool) -> u64 {
    if !path.exists() || !path.is_dir() {
        return 0;
    }
    let descendants = WalkDir::new(path)
        .skip_hidden(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_dir() && e.depth() > 0)
        .count() as u64;
    if include_root {
        descendants + 1
    } else {
        descendants
    }
}

fn destination_dir_counts(destination_root: &Path, source_rel_dirs: &HashSet<String>) -> (u64, u64) {
    if !destination_root.is_dir() {
        return (0, 0);
    }

    WalkDir::new(destination_root)
        .skip_hidden(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_dir() && e.depth() > 0)
        .fold((0u64, 0u64), |(total, unaffected), e| {
            let rel = e
                .path()
                .strip_prefix(destination_root)
                .ok()
                .map(normalize_rel)
                .unwrap_or_default();
            let is_unaffected = !rel.is_empty() && !source_rel_dirs.contains(&rel);
            (total + 1, unaffected + u64::from(is_unaffected))
        })
}

fn add_parent_dir_chain(rel: &str, include_root: bool, out: &mut HashSet<String>) {
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

fn usage() {
    eprintln!("usage: copy [-h] [-m] [-s] [-o] [-c] [-b] [-v] [--preview] [--preview-lite] source destination");
}

fn print_help() {
    println!("usage: copy [-h] [-m] [-s] [-o] [-c] [-b] [-v] [--preview] [--preview-lite] source destination");
    println!();
    println!("Standalone copy/move with preview/progress.");
    println!("Supports local paths and one-sided remote rsync endpoints like user@host:/path or host:/path.");
    println!("Remote mode reads ~/.ssh/config for Host/User matching and uses rsync over SSH.");
    println!("Local mode preflight checks destination free space against planned transfer bytes (no sudo required).");
    println!();
    println!("positional arguments:");
    println!("  source                Source path (file or directory)");
    println!("  destination           Destination path (directory, or file path when source is a file)");
    println!();
    println!("options:");
    println!("  -h, --help            show this help message and exit");
    println!("  -m, --move            Move mode: transfer then remove source data (equivalent to move behavior).");
    println!("  -s, --sudo            Run transfer commands with sudo");
    println!("  -o, --overwrite       Force overwrite (replace) instead of merge when destination target already exists.");
    println!("  -c, --contents-only   Transfer source directory children into destination (like source/*; do not nest source basename).");
    println!("                        In --move mode, source directories are removed if they become empty.");
    println!("  -b, --backup          Create a timestamped backup when destination data will be merged or overwritten.");
    println!("  -v, --verbose, --showall");
    println!("                        Show hierarchical preview: up to 5 changed entries per level (modified first), expand only modified folders, and abbreviate remaining new/modified/unchanged/removed counts.");
    println!("  --preview             Run preview only (no prompt, no transfer).");
    println!("  --preview-lite        Faster preview-only mode; skips exact byte scan on brand-new destination trees.");
}

fn parse_args() -> Result<CliArgs, i32> {
    let mut args = CliArgs::default();
    let mut positional: Vec<String> = Vec::new();

    for raw in env::args().skip(1) {
        match raw.as_str() {
            "-h" | "--help" => {
                print_help();
                return Err(0);
            }
            "-m" | "--move" => args.move_mode = true,
            "-s" | "--sudo" => args.sudo = true,
            "-o" | "--overwrite" => args.overwrite = true,
            "-c" | "--contents-only" => args.contents_only = true,
            "-b" | "--backup" => args.backup = true,
            "-v" | "--verbose" | "--showall" => args.showall = true,
            "--preview" => args.preview_only = true,
            "--preview-lite" => args.preview_lite = true,
            _ if raw.starts_with('-') => {
                usage();
                eprintln!("copy: error: unrecognized arguments: {raw}");
                return Err(1);
            }
            _ => positional.push(raw),
        }
    }

    if positional.len() < 2 {
        usage();
        eprintln!("copy: error: the following arguments are required: source, destination");
        return Err(1);
    }

    args.source = positional.remove(0);
    args.destination = positional.remove(0);
    args.extra = positional;
    Ok(args)
}

fn run_command_capture(cmd: &[String], sudo: bool) -> io::Result<CmdOutput> {
    let mut full: Vec<String> = Vec::new();
    if sudo {
        full.push("sudo".to_string());
    }
    full.extend(cmd.iter().cloned());

    let output = Command::new(&full[0]).args(&full[1..]).output()?;
    Ok(CmdOutput {
        code: output.status.code().unwrap_or(1),
    })
}

fn fmt_mode_word(label: &str, active: bool) -> String {
    if active {
        format!("{OKGREEN}{label}{ENDC}")
    } else {
        format!("{DIM}{label}{ENDC}")
    }
}

fn print_preview_root_line(preview_root: &Path, highlight_new_leaf: bool) {
    let full = preview_root.display().to_string();
    if !highlight_new_leaf {
        println!("{WARNING}{}{ENDC}", full);
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

    let parent = p.parent().map(|x| x.display().to_string()).unwrap_or_default();
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

fn backup_base_path(path: &Path) -> Option<PathBuf> {
    let src = if path == Path::new("/") {
        return None;
    } else {
        path
    };
    let parent = src.parent().unwrap_or_else(|| Path::new("."));
    let name = src.file_name()?.to_string_lossy().to_string();
    let now = chrono_like_stamp();
    Some(parent.join(format!("{name}.{now}")))
}

fn chrono_like_stamp() -> String {
    // YYYYMMDD-HHMMSS localtime without external crates.
    // Falls back to unix seconds formatting if conversion fails.
    let now = SystemTime::now();
    let secs = now.duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0)).as_secs() as i64;
    let t = libc_time::LocalTime::from_unix(secs).unwrap_or_else(|| libc_time::LocalTime::fallback(secs));
    format!(
        "{:04}{:02}{:02}-{:02}{:02}{:02}",
        t.year, t.month, t.day, t.hour, t.min, t.sec
    )
}

fn next_backup_candidate_from_base(base: &Path) -> Option<PathBuf> {
    for idx in 0..1000 {
        let candidate = if idx == 0 {
            base.to_path_buf()
        } else {
            PathBuf::from(format!("{}.{}", base.display(), idx))
        };
        if !candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

fn plan_backup_path(path: &Path) -> Option<PathBuf> {
    let base = backup_base_path(path)?;
    next_backup_candidate_from_base(&base)
}

fn copy_file_preserve_with_progress_buf<F>(
    src: &Path,
    dst: &Path,
    buf_bytes: usize,
    mut on_bytes: F,
) -> io::Result<u64>
where
    F: FnMut(u64),
{
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut in_file = fs::File::open(src)?;
    let mut out_file = fs::File::create(dst)?;
    let mut buf = vec![0u8; buf_bytes.max(64 * 1024)];
    let mut total: u64 = 0;
    loop {
        let n = in_file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        out_file.write_all(&buf[..n])?;
        let n64 = n as u64;
        total += n64;
        on_bytes(n64);
    }
    out_file.flush()?;
    let meta = fs::symlink_metadata(src)?;
    fs::set_permissions(dst, fs::Permissions::from_mode(meta.permissions().mode()))?;
    let atime = FileTime::from_last_access_time(&meta);
    let mtime = FileTime::from_last_modification_time(&meta);
    set_file_times(dst, atime, mtime)?;
    Ok(total)
}

fn copy_file_preserve_with_progress<F>(src: &Path, dst: &Path, on_bytes: F) -> io::Result<u64>
where
    F: FnMut(u64),
{
    copy_file_preserve_with_progress_buf(src, dst, 1024 * 1024, on_bytes)
}

fn copy_file_preserve(src: &Path, dst: &Path) -> io::Result<u64> {
    copy_file_preserve_with_progress(src, dst, |_| {})
}

fn remove_path_local_if_exists(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(md) => {
            if md.file_type().is_dir() {
                fs::remove_dir_all(path)
            } else {
                fs::remove_file(path)
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn copy_symlink(src: &Path, dst: &Path) -> io::Result<()> {
    let target = fs::read_link(src)?;
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_path_local_if_exists(dst)?;
    symlink(target, dst)
}

fn symlink_targets_equal(src: &Path, dst: &Path) -> bool {
    let src_md = match fs::symlink_metadata(src) {
        Ok(m) if m.file_type().is_symlink() => m,
        _ => return false,
    };
    let dst_md = match fs::symlink_metadata(dst) {
        Ok(m) if m.file_type().is_symlink() => m,
        _ => return false,
    };
    let _ = (src_md, dst_md);
    match (fs::read_link(src), fs::read_link(dst)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

fn copy_path_recursive(src: &Path, dst: &Path) -> io::Result<()> {
    let meta = fs::symlink_metadata(src)?;
    if meta.file_type().is_symlink() {
        copy_symlink(src, dst)?;
        return Ok(());
    }
    if meta.is_file() {
        let _ = copy_file_preserve(src, dst)?;
        return Ok(());
    }

    fs::create_dir_all(dst)?;
    fs::set_permissions(dst, fs::Permissions::from_mode(meta.permissions().mode()))?;
    for ent in fs::read_dir(src)? {
        let ent = ent?;
        let child_src = ent.path();
        let child_dst = dst.join(ent.file_name());
        copy_path_recursive(&child_src, &child_dst)?;
    }
    let atime = FileTime::from_last_access_time(&meta);
    let mtime = FileTime::from_last_modification_time(&meta);
    let _ = set_file_times(dst, atime, mtime);
    Ok(())
}

fn remove_path_recursive(path: &Path, use_sudo: bool, mode: TransferMode) -> bool {
    if !path.exists() {
        return true;
    }
    if use_sudo {
        let cmd = vec!["rm".to_string(), "-rf".to_string(), "--".to_string(), path.display().to_string()];
        match run_command_capture(&cmd, true) {
            Ok(out) if out.code == 0 => true,
            _ => {
                log(mode, &format!("Failed to remove existing path: {}", path.display()), LogLevel::Error);
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
            log(mode, &format!("Failed to remove existing path: {}", path.display()), LogLevel::Error);
            return false;
        }
        true
    }
}

fn backup_path_with_base(path: &Path, use_sudo: bool, base: &Path, mode: TransferMode) -> Option<PathBuf> {
    if path == Path::new("/") {
        log(mode, "Refusing to backup root path.", LogLevel::Error);
        return None;
    }

    for idx in 0..1000 {
        let candidate = if idx == 0 {
            base.to_path_buf()
        } else {
            PathBuf::from(format!("{}.{}", base.display(), idx))
        };
        if candidate.exists() {
            continue;
        }

        let ok = if use_sudo {
            let cmd = vec![
                "mv".to_string(),
                "--".to_string(),
                path.display().to_string(),
                candidate.display().to_string(),
            ];
            run_command_capture(&cmd, true).map(|o| o.code == 0).unwrap_or(false)
        } else {
            fs::rename(path, &candidate).is_ok()
        };

        if ok {
            return Some(candidate);
        }
    }

    log(mode, &format!("Failed to create unique backup name for: {}", path.display()), LogLevel::Error);
    None
}

fn copy_path_to_backup(path: &Path, backup_path: &Path, use_sudo: bool, mode: TransferMode) -> Option<PathBuf> {
    let ok = if use_sudo {
        let cmd = vec![
            "cp".to_string(),
            "-a".to_string(),
            "--".to_string(),
            path.display().to_string(),
            backup_path.display().to_string(),
        ];
        run_command_capture(&cmd, true).map(|o| o.code == 0).unwrap_or(false)
    } else {
        copy_path_recursive(path, backup_path).is_ok()
    };

    if !ok {
        log(mode, &format!("Failed to create backup copy: {}", path.display()), LogLevel::Error);
        return None;
    }
    Some(backup_path.to_path_buf())
}

fn remove_empty_dirs(path: &Path, remove_root: bool) {
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

fn cleanup_source_dirs(src_root: &Path, remove_root: bool, use_sudo: bool, mode: TransferMode) {
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
                log(mode, &format!("Source cleanup failed: find exited with status {}.", out.code), LogLevel::Warn);
            }
        }
    } else {
        remove_empty_dirs(src_root, remove_root);
    }
}

fn remove_single_file(path: &Path, use_sudo: bool, mode: TransferMode) -> bool {
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

fn prune_move_source_duplicates(
    src_path: &str,
    dst_path: &str,
    src_obj_kind: SrcObjKind,
    contents_mode: bool,
    use_sudo: bool,
    mode: TransferMode,
    manifest: Option<&TransferManifest>,
    expected_files: u64,
    expected_bytes: u64,
) -> DeleteCleanupOutcome {
    let mut removed = DeleteCleanupOutcome::default();
    let mut last_report = Instant::now();
    let mut last_report_bytes: u64 = 0;

    let mut report_progress = |force: bool, removed: &DeleteCleanupOutcome| {
        let now = Instant::now();
        if !force && now.duration_since(last_report) < Duration::from_millis(500) {
            return;
        }
        if force && removed.files == 0 {
            return;
        }
        let dt = now.duration_since(last_report).as_secs_f64().max(1e-6);
        let speed_bps = removed.bytes.saturating_sub(last_report_bytes) as f64 / dt;
        let cleanup_pct = if expected_bytes > 0 {
            (removed.bytes as f64 * 100.0 / expected_bytes as f64).min(100.0)
        } else if expected_files > 0 {
            (removed.files as f64 * 100.0 / expected_files as f64).min(100.0)
        } else {
            -1.0
        };
        if expected_bytes > 0 {
            if expected_files > 0 {
                println!(
                    "Cleanup: {cleanup_pct:6.2}% {} / {} | {} | Deleted files: {} / {}",
                    fmt_bytes_col_10(removed.bytes),
                    format_bytes_binary(expected_bytes, 2),
                    fmt_rate_col(speed_bps),
                    format_number(removed.files),
                    format_number(expected_files)
                );
            } else {
                println!(
                    "Cleanup: {cleanup_pct:6.2}% {} / {} | {} | Deleted files: {}",
                    fmt_bytes_col_10(removed.bytes),
                    format_bytes_binary(expected_bytes, 2),
                    fmt_rate_col(speed_bps),
                    format_number(removed.files)
                );
            }
        } else {
            if expected_files > 0 {
                println!(
                    "Cleanup: {cleanup_pct:6.2}% {} | {} | Deleted files: {} / {}",
                    fmt_bytes_col_10(removed.bytes),
                    fmt_rate_col(speed_bps),
                    format_number(removed.files),
                    format_number(expected_files)
                );
            } else {
                println!(
                    "Cleanup: ---% {} | {} | Deleted files: {}",
                    fmt_bytes_col_10(removed.bytes),
                    fmt_rate_col(speed_bps),
                    format_number(removed.files)
                );
            }
        }
        last_report = now;
        last_report_bytes = removed.bytes;
    };

    match src_obj_kind {
        SrcObjKind::File => {
            let src = Path::new(src_path);
            let dst = Path::new(dst_path);
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
                removed.bytes += if src_lmd.file_type().is_symlink() { 0 } else { src_lmd.len() };
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
                let mut by_parent: BTreeMap<PathBuf, Vec<(PathBuf, u64)>> = BTreeMap::new();
                let mut seen_rel: HashSet<String> = HashSet::new();

                for entry in m.identical_files.iter().chain(m.copy_files.iter()) {
                    if entry.rel.is_empty() || !seen_rel.insert(entry.rel.clone()) {
                        continue;
                    }
                    let src_file = src_root.join(&entry.rel);
                    let (dst_item, _) = map_dir_dest(include_root, &src_base, &entry.rel, dst_base);
                    if src_file == dst_item {
                        continue;
                    }
                    let src_md = match fs::symlink_metadata(&src_file) {
                        Ok(md) => md,
                        Err(_) => continue,
                    };
                    let dst_matches = if src_md.file_type().is_symlink() {
                        symlink_targets_equal(&src_file, &dst_item)
                    } else {
                        match fs::metadata(&dst_item) {
                            Ok(dm) if dm.is_file() => entry.size == 0 || dm.len() == entry.size,
                            _ => false,
                        }
                    };
                    if !dst_matches {
                        continue;
                    }
                    let bytes = if entry.size > 0 {
                        entry.size
                    } else {
                        fs::metadata(&src_file).map(|m| m.len()).unwrap_or(0)
                    };
                    let parent = src_file.parent().unwrap_or(src_root).to_path_buf();
                    by_parent.entry(parent).or_default().push((src_file, bytes));
                }

                for (_parent, mut files) in by_parent {
                    files.sort_by(|a, b| a.0.cmp(&b.0));
                    if use_sudo {
                        let mut idx = 0usize;
                        const SUDO_DELETE_CHUNK: usize = 256;
                        while idx < files.len() {
                            let end = (idx + SUDO_DELETE_CHUNK).min(files.len());
                            let chunk = &files[idx..end];
                            let mut cmd = vec![
                                "rm".to_string(),
                                "-f".to_string(),
                                "--".to_string(),
                            ];
                            for (src_file, _) in chunk {
                                cmd.push(src_file.display().to_string());
                            }
                            let _ = run_command_capture(&cmd, true);
                            for (src_file, size) in chunk {
                                if !src_file.exists() {
                                    removed.files += 1;
                                    removed.bytes += *size;
                                }
                                report_progress(false, &removed);
                            }
                            idx = end;
                        }
                    } else {
                        for (src_file, size) in files {
                            if remove_single_file(&src_file, use_sudo, mode) {
                                removed.files += 1;
                                removed.bytes += size;
                            }
                            report_progress(false, &removed);
                        }
                    }
                }
                report_progress(true, &removed);
                return removed;
            }

            for ent in WalkDir::new(src_root)
                .skip_hidden(false)
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

fn normalize_rel(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn map_dir_dest(include_root: bool, src_base: &str, rel: &str, dst_base: &Path) -> (PathBuf, String) {
    if include_root {
        if rel.is_empty() {
            (dst_base.join(src_base), format!("{src_base}/"))
        } else {
            (dst_base.join(src_base).join(rel), format!("{src_base}/{rel}"))
        }
    } else if rel.is_empty() {
        (dst_base.to_path_buf(), String::new())
    } else {
        (dst_base.join(rel), rel.to_string())
    }
}

fn top_name_from_display(display_rel: &str, fallback_is_dir: bool) -> Option<(String, bool)> {
    let trimmed = display_rel.trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    let mut parts = trimmed.splitn(2, '/');
    let top = parts.next()?.to_string();
    let has_rest = parts.next().is_some();
    Some((top, has_rest || fallback_is_dir))
}

fn merge_top_state(map: &mut HashMap<String, (bool, bool)>, name: String, is_added: bool, is_dir: bool) {
    map.entry(name)
        .and_modify(|(added, dir)| {
            *added = *added || is_added;
            *dir = *dir || is_dir;
        })
        .or_insert((is_added, is_dir));
}

fn rel_or_ancestor_in_set(rel: &str, set: &HashSet<String>) -> bool {
    if rel.is_empty() || set.is_empty() {
        return false;
    }
    if set.contains(rel) {
        return true;
    }
    let mut cur = rel;
    while let Some(idx) = cur.rfind('/') {
        cur = &cur[..idx];
        if set.contains(cur) {
            return true;
        }
    }
    false
}

fn parent_rel_in_set(rel: &str, set: &HashSet<String>) -> bool {
    if set.is_empty() {
        return false;
    }
    match rel.rfind('/') {
        Some(idx) => rel_or_ancestor_in_set(&rel[..idx], set),
        None => false,
    }
}

fn pre_scan_directory(
    src_path: &str,
    dst_path: &str,
    src_mnt: &Path,
    collect_detailed: bool,
    preview_lite: bool,
    build_manifest: bool,
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

    // Strict fast-path for non-verbose preview when destination root is missing:
    // all source content is guaranteed "new", so skip destination path construction/stat checks.
    if destination_missing && !collect_detailed {
        let mut out = PreScan::default();
        out.planned_bytes_exact = !preview_lite;
        let mut top_states: HashMap<String, (bool, bool)> = HashMap::new();

        if include_root && !src_base.is_empty() {
            merge_top_state(&mut top_states, src_base.clone(), true, true);
            out.has_itemized_changes = true;
        }

        type FastReduce = (
            u64,
            u64,
            u64,
            bool,
            HashMap<String, (bool, bool)>,
            Vec<String>,
            Vec<ManifestFileEntry>,
        );
        let (
            add_files,
            add_dirs_desc,
            planned_bytes,
            has_itemized,
            reduced_top_states,
            mut manifest_dirs,
            mut manifest_copy_files,
        ): FastReduce =
            WalkDir::new(src_root)
            .skip_hidden(false)
            .into_iter()
            .filter_map(Result::ok)
            .par_bridge()
            .fold(
                || (0, 0, 0, false, HashMap::new(), Vec::new(), Vec::new()),
                |mut acc, ent| {
                    if ent.depth() == 0 {
                        return acc;
                    }
                    let fty = ent.file_type();
                    if !fty.is_dir() && !fty.is_file() && !fty.is_symlink() {
                        return acc;
                    }

                    acc.3 = true;

                    if !include_root && ent.depth() == 1 {
                        let top = ent.file_name().to_string_lossy().to_string();
                        if !top.is_empty() {
                            merge_top_state(&mut acc.4, top, true, fty.is_dir());
                        }
                    }

                    if build_manifest {
                        if let Ok(rel_path) = ent.path().strip_prefix(src_root) {
                            let rel = normalize_rel(rel_path);
                            if !rel.is_empty() {
                                if fty.is_dir() {
                                    acc.5.push(rel);
                                } else if fty.is_file() || fty.is_symlink() {
                                    let mut sz = 0;
                                    if fty.is_file() {
                                        if let Ok(md) = ent.metadata() {
                                            sz = md.len();
                                        }
                                    } else if let Ok(md) = fs::symlink_metadata(ent.path()) {
                                        sz = md.len();
                                    }
                                    acc.6.push(ManifestFileEntry { rel, size: sz });
                                }
                            }
                        }
                    }

                    if fty.is_file() {
                        acc.0 += 1;
                        if !preview_lite {
                            if let Ok(md) = ent.metadata() {
                                acc.2 += md.len();
                            }
                        }
                    } else if fty.is_dir() {
                        acc.1 += 1;
                    }

                    acc
                },
            )
            .reduce(
                || (0, 0, 0, false, HashMap::new(), Vec::new(), Vec::new()),
                |mut a, b| {
                    a.0 += b.0;
                    a.1 += b.1;
                    a.2 += b.2;
                    a.3 = a.3 || b.3;
                    for (k, (is_added, is_dir)) in b.4 {
                        merge_top_state(&mut a.4, k, is_added, is_dir);
                    }
                    a.5.extend(b.5);
                    a.6.extend(b.6);
                    a
                },
            );

        out.add_files = add_files;
        let root_new_dir = u64::from(include_root && !src_base.is_empty());
        out.add_dirs = add_dirs_desc.saturating_add(root_new_dir);
        out.total_dirs = Some(out.add_dirs);
        out.mod_dirs = 0;
        out.unaffected_dirs = 0;
        out.total_regular_files = Some(add_files);
        out.total_regular_bytes = if preview_lite { None } else { Some(planned_bytes) };
        out.planned_bytes = planned_bytes;
        out.has_itemized_changes = out.has_itemized_changes || has_itemized;

        for (k, v) in reduced_top_states {
            merge_top_state(&mut top_states, k, v.0, v.1);
        }

        let mut tops: Vec<(String, (bool, bool))> = top_states.into_iter().collect();
        tops.sort_by(|a, b| a.0.cmp(&b.0));
        for (name, (_is_added, is_dir)) in tops {
            let kind = if is_dir {
                ChangeKind::NewDir
            } else {
                ChangeKind::NewFile
            };
            let rel = if is_dir { format!("{name}/") } else { name };
            out.change_preview.push(ChangeItem { kind, rel });
        }

        if build_manifest {
            manifest_dirs.sort_by(|a, b| {
                let da = a.bytes().filter(|c| *c == b'/').count();
                let db = b.bytes().filter(|c| *c == b'/').count();
                da.cmp(&db).then_with(|| a.cmp(b))
            });
            manifest_copy_files.sort_by(|a, b| a.rel.cmp(&b.rel));
            out.transfer_manifest = Some(TransferManifest {
                dirs: manifest_dirs,
                copy_files: manifest_copy_files,
                identical_files: Vec::new(),
            });
        }

        return out;
    }

    let mut dirs: Vec<String> = Vec::new();
    let mut files: Vec<(String, PathBuf, u64, bool)> = Vec::new();
    let mut stack: Vec<PathBuf> = vec![src_root.to_path_buf()];
    while let Some(cur_dir) = stack.pop() {
        let entries = match fs::read_dir(&cur_dir) {
            Ok(v) => v,
            Err(_) => continue,
        };
        for ent in entries.filter_map(Result::ok) {
            let p = ent.path();
            let rel = p.strip_prefix(src_root).map(normalize_rel).unwrap_or_else(|_| String::new());
            if rel.is_empty() {
                continue;
            }
            let md = match fs::symlink_metadata(&p) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if md.is_dir() {
                dirs.push(rel);
                stack.push(p);
            } else if md.is_file() {
                files.push((rel, p, md.len(), false));
            } else if md.file_type().is_symlink() {
                files.push((rel, p, 0, true));
            }
        }
    }

    let mut out = PreScan::default();
    out.planned_bytes_exact = true;
    out.total_regular_files = Some(files.iter().filter(|(_, _, _, is_symlink)| !*is_symlink).count() as u64);
    out.total_regular_bytes = Some(files.iter().filter(|(_, _, _, is_symlink)| !*is_symlink).map(|(_, _, size, _)| *size).sum());
    let source_rel_files: HashSet<String> = files.iter().map(|(rel, _, _, _)| rel.clone()).collect();
    let source_rel_dirs: HashSet<String> = dirs.iter().cloned().collect();
    let root_new_dir = include_root && !destination_root.is_dir();
    out.total_dirs = Some(dirs.len() as u64 + u64::from(include_root));
    let mut top_states: HashMap<String, (bool, bool)> = HashMap::new();
    let mut manifest_dirs = if build_manifest { Some(dirs.clone()) } else { None };

    if include_root && destination_missing {
        let (dst_root, display_rel) = map_dir_dest(true, &src_base, "", dst_base);
        if !dst_root.is_dir() {
            if !display_rel.is_empty() {
                if collect_detailed {
                    out.change_preview.push(ChangeItem {
                        kind: ChangeKind::NewDir,
                        rel: display_rel,
                    });
                } else if let Some((top, is_dir)) = top_name_from_display(&display_rel, true) {
                    merge_top_state(&mut top_states, top, true, is_dir);
                }
                out.has_itemized_changes = true;
            }
        }
    }

    let mut missing_dir_prefixes: HashSet<String> = HashSet::new();
    let mut dirs_depth_sorted = dirs.clone();
    dirs_depth_sorted.sort_by_key(|rel| rel.bytes().filter(|b| *b == b'/').count());

    for rel in &dirs_depth_sorted {
        let parent_missing = rel_or_ancestor_in_set(rel, &missing_dir_prefixes);
        let (dst_dir, display_rel) = map_dir_dest(include_root, &src_base, rel, dst_base);
        let dir_missing = destination_missing || parent_missing || !dst_dir.is_dir();
        if dir_missing {
            missing_dir_prefixes.insert(rel.clone());
            let rel_dir = format!("{display_rel}/").replace("//", "/");
            if collect_detailed {
                out.change_preview.push(ChangeItem {
                    kind: ChangeKind::NewDir,
                    rel: rel_dir.clone(),
                });
            } else if let Some((top, is_dir)) = top_name_from_display(&rel_dir, true) {
                merge_top_state(&mut top_states, top, true, is_dir);
            }
            out.has_itemized_changes = true;
        }
    }

    type TopMap = HashMap<String, (bool, bool)>;
    type FileReduce = (
        u64,
        u64,
        u64,
        Vec<ChangeItem>,
        TopMap,
        Vec<ManifestFileEntry>,
        Vec<ManifestFileEntry>,
        HashSet<String>,
    );
    let has_missing_subtrees = !missing_dir_prefixes.is_empty();
    let (
        add_files,
        mod_files,
        planned_bytes,
        detailed_changes,
        reduced_top_states,
        mut manifest_copy_files,
        mut manifest_identical_files,
        mut changed_parent_dirs,
    ): FileReduce = files
        .par_iter()
        .fold(
            || (0, 0, 0, Vec::new(), HashMap::new(), Vec::new(), Vec::new(), HashSet::new()),
            |mut acc, (rel, src_file, size, is_symlink)| {
                let (dst_file, display_rel) = map_dir_dest(include_root, &src_base, rel, dst_base);
                let change = if destination_missing {
                    Some(ChangeKind::NewFile)
                } else if has_missing_subtrees && parent_rel_in_set(rel, &missing_dir_prefixes) {
                    Some(ChangeKind::NewFile)
                } else if *is_symlink {
                    match fs::symlink_metadata(&dst_file) {
                        Ok(dm) if dm.file_type().is_symlink() && symlink_targets_equal(src_file, &dst_file) => None,
                        Ok(_) => Some(ChangeKind::ModFile),
                        Err(_) => Some(ChangeKind::NewFile),
                    }
                } else {
                    match fs::metadata(&dst_file) {
                        Ok(dm) if dm.is_file() && dm.len() == *size => None,
                        Ok(_) => Some(ChangeKind::ModFile),
                        Err(_) => Some(ChangeKind::NewFile),
                    }
                };
                if let Some(kind) = change {
                    if !*is_symlink {
                        match kind {
                            ChangeKind::NewFile => acc.0 += 1,
                            _ => acc.1 += 1,
                        }
                        acc.2 += *size;
                    }
                    if build_manifest {
                        acc.5.push(ManifestFileEntry {
                            rel: rel.clone(),
                            size: *size,
                        });
                    }
                    if collect_detailed {
                        acc.3.push(ChangeItem {
                            kind,
                            rel: display_rel.clone(),
                        });
                    } else if let Some((top, is_dir)) = top_name_from_display(&display_rel, false) {
                        merge_top_state(&mut acc.4, top, matches!(kind, ChangeKind::NewFile), is_dir);
                    }
                    add_parent_dir_chain(rel, include_root, &mut acc.7);
                } else if build_manifest {
                    acc.6.push(ManifestFileEntry {
                        rel: rel.clone(),
                        size: *size,
                    });
                }
                acc
            },
        )
        .reduce(
            || (0, 0, 0, Vec::new(), HashMap::new(), Vec::new(), Vec::new(), HashSet::new()),
            |mut a, b| {
                a.0 += b.0;
                a.1 += b.1;
                a.2 += b.2;
                a.3.extend(b.3);
                for (k, (is_added, is_dir)) in b.4 {
                    merge_top_state(&mut a.4, k, is_added, is_dir);
                }
                a.5.extend(b.5);
                a.6.extend(b.6);
                a.7.extend(b.7);
                a
            },
        );

    out.add_files += add_files;
    out.mod_files += mod_files;
    out.planned_bytes += planned_bytes;
    if destination_missing {
        out.unaffected_files = 0;
    } else {
        let (dest_total_files, unaffected_by_scan) = destination_file_counts(&destination_root, &source_rel_files);
        let source_regular_total = out.total_regular_files.unwrap_or(0);
        let source_not_new = source_regular_total.saturating_sub(add_files);
        let unaffected_by_overlap = dest_total_files.saturating_sub(source_not_new);
        out.unaffected_files = unaffected_by_scan.max(unaffected_by_overlap);
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
                source_rel_dirs.contains(rel.as_str()) && !missing_dir_prefixes.contains(rel.as_str())
            }
        })
        .count() as u64;
    let total_dirs = out.total_dirs.unwrap_or(0);
    out.mod_dirs = mod_dirs_count.min(total_dirs.saturating_sub(out.add_dirs));
    if destination_missing {
        out.unaffected_dirs = 0;
    } else {
        let (dest_total_dirs, unaffected_dirs_by_scan) =
            destination_dir_counts(&destination_root, &source_rel_dirs);
        let source_dir_total_no_root = dirs.len() as u64;
        let source_dirs_not_new = source_dir_total_no_root.saturating_sub(missing_dir_prefixes.len() as u64);
        let unaffected_dirs_by_overlap = dest_total_dirs.saturating_sub(source_dirs_not_new);
        out.unaffected_dirs = unaffected_dirs_by_scan.max(unaffected_dirs_by_overlap);
    }

    if out.add_files > 0 || out.mod_files > 0 {
        out.has_itemized_changes = true;
    }

    if collect_detailed {
        out.change_preview.extend(detailed_changes);
    } else {
        for (k, v) in reduced_top_states {
            merge_top_state(&mut top_states, k, v.0, v.1);
        }
        let mut tops: Vec<(String, (bool, bool))> = top_states.into_iter().collect();
        tops.sort_by(|a, b| a.0.cmp(&b.0));
        for (name, (is_added, is_dir)) in tops {
            let kind = if is_added {
                if is_dir {
                    ChangeKind::NewDir
                } else {
                    ChangeKind::NewFile
                }
            } else if is_dir {
                ChangeKind::ModDir
            } else {
                ChangeKind::ModFile
            };
            let rel = if is_dir { format!("{name}/") } else { name };
            out.change_preview.push(ChangeItem { kind, rel });
        }
    }

    if build_manifest {
        if let Some(mut d) = manifest_dirs.take() {
            d.sort_by(|a, b| {
                let da = a.bytes().filter(|c| *c == b'/').count();
                let db = b.bytes().filter(|c| *c == b'/').count();
                da.cmp(&db).then_with(|| a.cmp(b))
            });
            manifest_copy_files.sort_by(|a, b| a.rel.cmp(&b.rel));
            manifest_identical_files.sort_by(|a, b| a.rel.cmp(&b.rel));
            out.transfer_manifest = Some(TransferManifest {
                dirs: d,
                copy_files: manifest_copy_files,
                identical_files: manifest_identical_files,
            });
        }
    }

    out
}

fn pre_scan_file(src_mnt: &Path, dst_path: &str, dst_obj_kind: DstObjKind) -> PreScan {
    let mut out = PreScan::default();
    out.planned_bytes_exact = true;
    let src_lmd = match fs::symlink_metadata(src_mnt) {
        Ok(m) => m,
        Err(_) => return out,
    };
    let src_is_symlink = src_lmd.file_type().is_symlink();
    let size = if src_is_symlink {
        0
    } else {
        match fs::metadata(src_mnt) {
            Ok(m) => m.len(),
            Err(_) => return out,
        }
    };
    out.total_regular_files = Some(if src_is_symlink { 0 } else { 1 });
    out.total_regular_bytes = Some(size);
    out.total_dirs = Some(0);

    let src_name = src_mnt
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "source".to_string());

    if matches!(dst_obj_kind, DstObjKind::Dir | DstObjKind::DirExisting) {
        let base = Path::new(dst_path.trim_end_matches('/'));
        if base.is_dir() {
            let mut source_rel_files = HashSet::new();
            source_rel_files.insert(src_name.clone());
            let (_dest_total, unaffected_by_scan) = destination_file_counts(base, &source_rel_files);
            out.unaffected_files = unaffected_by_scan;
        }
    }

    let (dst_file, display_rel) = match dst_obj_kind {
        DstObjKind::Dir | DstObjKind::DirExisting => {
            let base = Path::new(dst_path.trim_end_matches('/'));
            (base.join(&src_name), src_name)
        }
        _ => {
            let p = Path::new(dst_path);
            let n = p
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or(src_name);
            (p.to_path_buf(), n)
        }
    };

    let change = if src_is_symlink {
        match fs::symlink_metadata(&dst_file) {
            Ok(dm) if dm.file_type().is_symlink() && symlink_targets_equal(src_mnt, &dst_file) => None,
            Ok(_) => Some(ChangeKind::ModFile),
            Err(_) => Some(ChangeKind::NewFile),
        }
    } else {
        match fs::metadata(&dst_file) {
            Ok(dm) if dm.is_file() && dm.len() == size => None,
            Ok(_) => Some(ChangeKind::ModFile),
            Err(_) => Some(ChangeKind::NewFile),
        }
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
        out.change_preview.push(ChangeItem { kind: ch, rel: display_rel });
    }

    out
}

fn parse_progress2_bytes(line: &str) -> Option<u64> {
    let re = Regex::new(r"^\s*([0-9][0-9,]*(?:\.[0-9]+)?)([kKmMgGtTpPeE]?)\s+[0-9]{1,3}%").ok()?;
    let caps = re.captures(line)?;
    let num_txt = caps.get(1)?.as_str().replace(',', "");
    let unit = caps.get(2).map(|m| m.as_str().to_ascii_uppercase()).unwrap_or_default();
    let mut val: f64 = num_txt.parse().ok()?;
    let mult = match unit.as_str() {
        "K" => 1024f64,
        "M" => 1024f64.powi(2),
        "G" => 1024f64.powi(3),
        "T" => 1024f64.powi(4),
        "P" => 1024f64.powi(5),
        "E" => 1024f64.powi(6),
        _ => 1.0,
    };
    val *= mult;
    Some(val as u64)
}

fn handle_rsync_stream_line(tx: &mpsc::Sender<RsyncStreamEvent>, line: &str) {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return;
    }
    if let Some(bytes) = parse_progress2_bytes(trimmed) {
        let _ = tx.send(RsyncStreamEvent::Progress(bytes));
    } else {
        let _ = tx.send(RsyncStreamEvent::Text(trimmed.to_string()));
    }
}

fn spawn_rsync_stdout_reader(stdout: impl Read + Send + 'static, tx: mpsc::Sender<RsyncStreamEvent>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let reader = io::BufReader::new(stdout);
        for line in reader.lines().flatten() {
            handle_rsync_stream_line(&tx, &line);
        }
    })
}

fn spawn_rsync_stderr_reader(stderr: impl Read + Send + 'static, tx: mpsc::Sender<RsyncStreamEvent>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut reader = io::BufReader::new(stderr);
        let mut buf = [0u8; 8192];
        let mut pending: Vec<u8> = Vec::new();

        loop {
            let n = match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => break,
            };
            pending.extend_from_slice(&buf[..n]);

            let mut consumed = 0usize;
            for i in 0..pending.len() {
                let b = pending[i];
                if b == b'\n' || b == b'\r' {
                    if i > consumed {
                        let chunk = &pending[consumed..i];
                        let line = String::from_utf8_lossy(chunk);
                        handle_rsync_stream_line(&tx, &line);
                    }
                    consumed = i + 1;
                }
            }
            if consumed > 0 {
                pending.drain(..consumed);
            }
        }

        if !pending.is_empty() {
            let line = String::from_utf8_lossy(&pending);
            handle_rsync_stream_line(&tx, &line);
        }
    })
}

fn run_rsync_transfer(
    src_path: &str,
    dst_path: &str,
    planned_bytes: u64,
    use_sudo: bool,
    remove_source_during: bool,
) -> TransferOutcome {
    let mut cmd: Vec<String> = vec![
        "rsync".to_string(),
        "-aH".to_string(),
        "--size-only".to_string(),
    ];
    if remove_source_during {
        cmd.push("--remove-source-files".to_string());
    }
    cmd.extend([
        "--info=progress2,stats2,name0".to_string(),
        src_path.to_string(),
        dst_path.to_string(),
    ]);

    let mut full_cmd = Vec::new();
    if use_sudo {
        full_cmd.push("sudo".to_string());
    }
    full_cmd.extend(cmd);

    let mut child = match Command::new(&full_cmd[0])
        .args(&full_cmd[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => {
            return TransferOutcome {
                rc: 1,
                bytes_done: 0,
                elapsed_s: 0.0,
                io_read_bytes: None,
                io_write_bytes: None,
            }
        }
    };

    let transfer_start = Instant::now();
    print_transfer_columns_header();
    let mut done_bytes: u64 = 0;
    let mut last_report_bytes: u64 = 0;
    let mut last_report = transfer_start;
    let mut io_window = DeviceIoWindow::from_transfer_paths(src_path, dst_path);
    let _ = io_window.sample();
    let (io_start_read, io_start_write) = io_window.current_totals();
    let mut last_io_rates = DeviceIoRates::default();

    let (event_tx, event_rx) = mpsc::channel::<RsyncStreamEvent>();
    let stdout_handle = child
        .stdout
        .take()
        .map(|stdout| spawn_rsync_stdout_reader(stdout, event_tx.clone()));
    let stderr_handle = child
        .stderr
        .take()
        .map(|stderr| spawn_rsync_stderr_reader(stderr, event_tx.clone()));
    drop(event_tx);

    let print_progress = |done: u64, speed_bps: f64, io_rates: DeviceIoRates, elapsed_s: f64| {
        let timer = fmt_hms_tenths(elapsed_s);
        if planned_bytes > 0 {
            let pct = (done as f64 * 100.0 / planned_bytes as f64).min(100.0);
            println!(
                "{timer} {pct:6.2}% {} / {} | {} | {} {}",
                fmt_bytes_col_10(done),
                format_bytes_binary(planned_bytes, 2),
                fmt_rate_col(speed_bps),
                fmt_rate_col_opt(io_rates.src_read_bps),
                fmt_rate_col_opt(io_rates.dst_write_bps),
            );
        } else {
            println!(
                "{timer} ---% {} | {} | {} {}",
                fmt_bytes_col_10(done),
                fmt_rate_col(speed_bps),
                fmt_rate_col_opt(io_rates.src_read_bps),
                fmt_rate_col_opt(io_rates.dst_write_bps),
            );
        }
    };

    let rc: i32 = loop {
        match event_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(RsyncStreamEvent::Progress(bytes)) => {
                if bytes > done_bytes {
                    done_bytes = bytes;
                }
            }
            Ok(RsyncStreamEvent::Text(line)) => println!("{line}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {}
        }

        let now = Instant::now();
        if now.duration_since(last_report) >= Duration::from_millis(500) {
            let dt = now.duration_since(last_report).as_secs_f64().max(1e-6);
            let speed = done_bytes.saturating_sub(last_report_bytes) as f64 / dt;
            last_io_rates = io_window.sample();
            print_progress(done_bytes, speed, last_io_rates, now.duration_since(transfer_start).as_secs_f64());
            last_report_bytes = done_bytes;
            last_report = now;
        }

        if let Ok(Some(status)) = child.try_wait() {
            while let Ok(event) = event_rx.recv_timeout(Duration::from_millis(20)) {
                match event {
                    RsyncStreamEvent::Progress(bytes) => {
                        if bytes > done_bytes {
                            done_bytes = bytes;
                        }
                    }
                    RsyncStreamEvent::Text(line) => println!("{line}"),
                }
            }
            break status.code().unwrap_or(1);
        }
    };

    if let Some(h) = stdout_handle {
        let _ = h.join();
    }
    if let Some(h) = stderr_handle {
        let _ = h.join();
    }

    let final_done = if planned_bytes > 0 && (rc == 0 || rc == 24) {
        planned_bytes.max(done_bytes)
    } else {
        done_bytes
    };

    if planned_bytes > 0 {
        let pct = (final_done as f64 * 100.0 / planned_bytes as f64).min(100.0);
        let now = Instant::now();
        let dt = now.duration_since(last_report).as_secs_f64().max(1e-6);
        let final_speed = if final_done > last_report_bytes {
            (final_done - last_report_bytes) as f64 / dt
        } else {
            final_done as f64 / now.duration_since(transfer_start).as_secs_f64().max(1e-6)
        };
        println!(
            "{} {pct:6.2}% {} / {} | {} | {} {}",
            fmt_hms_tenths(now.duration_since(transfer_start).as_secs_f64()),
            fmt_bytes_col_10(final_done),
            format_bytes_binary(planned_bytes, 2),
            fmt_rate_col(final_speed),
            fmt_rate_col_opt(last_io_rates.src_read_bps),
            fmt_rate_col_opt(last_io_rates.dst_write_bps),
        );
    } else {
        println!(
            "{} ---% {} | {} | {} {}",
            fmt_hms_tenths(transfer_start.elapsed().as_secs_f64()),
            fmt_bytes_col_10(final_done),
            fmt_rate_col(0.0),
            fmt_rate_col_opt(last_io_rates.src_read_bps),
            fmt_rate_col_opt(last_io_rates.dst_write_bps),
        );
    }

    TransferOutcome {
        rc,
        bytes_done: final_done,
        elapsed_s: transfer_start.elapsed().as_secs_f64(),
        io_read_bytes: counter_delta(io_start_read, io_window.current_totals().0),
        io_write_bytes: counter_delta(io_start_write, io_window.current_totals().1),
    }
}

fn run_rust_transfer(
    src_path: &str,
    dst_path: &str,
    src_obj_kind: SrcObjKind,
    _is_move: bool,
    planned_bytes: u64,
    manifest: Option<&TransferManifest>,
    media: MediaKind,
) -> TransferOutcome {
    let done = Arc::new(AtomicU64::new(0));
    let copy_buf_bytes = copy_chunk_bytes_for_media(media);
    let inflight_limiter = inflight_max_bytes_for_media(media).map(InflightWriteLimiter::new).map(Arc::new);
    let transfer_start = Instant::now();
    print_transfer_columns_header();
    let io_window_for_avg = DeviceIoWindow::from_transfer_paths(src_path, dst_path);
    let (io_start_read, io_start_write) = io_window_for_avg.current_totals();
    let transfer_start_for_ticker = transfer_start;
    let src_path_for_ticker = src_path.to_string();
    let dst_path_for_ticker = dst_path.to_string();

    let done_for_ticker = Arc::clone(&done);
    let io_rates_shared = Arc::new(Mutex::new(DeviceIoRates::default()));
    let io_rates_for_ticker = Arc::clone(&io_rates_shared);
    let (ticker_stop_tx, ticker_stop_rx) = mpsc::channel::<()>();
    let ticker = thread::spawn(move || {
        let mut last_report = Instant::now();
        let mut last_report_bytes: u64 = 0;
        let mut io_window = DeviceIoWindow::from_transfer_paths(&src_path_for_ticker, &dst_path_for_ticker);
        let _ = io_window.sample();
        loop {
            match ticker_stop_rx.recv_timeout(Duration::from_millis(500)) {
                Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let now = Instant::now();
                    let done_bytes = done_for_ticker.load(Ordering::Relaxed);
                    let dt = now.duration_since(last_report).as_secs_f64().max(1e-6);
                    let speed = done_bytes.saturating_sub(last_report_bytes) as f64 / dt;
                    let io_rates = io_window.sample();
                    if let Ok(mut g) = io_rates_for_ticker.lock() {
                        *g = io_rates;
                    }
                    if planned_bytes > 0 {
                        let pct = (done_bytes as f64 * 100.0 / planned_bytes as f64).min(100.0);
                        println!(
                            "{} {pct:6.2}% {} / {} | {} | {} {}",
                            fmt_hms_tenths(now.duration_since(transfer_start_for_ticker).as_secs_f64()),
                            fmt_bytes_col_10(done_bytes),
                            format_bytes_binary(planned_bytes, 2),
                            fmt_rate_col(speed),
                            fmt_rate_col_opt(io_rates.src_read_bps),
                            fmt_rate_col_opt(io_rates.dst_write_bps),
                        );
                    } else {
                        println!(
                            "{} ---% {} | {} | {} {}",
                            fmt_hms_tenths(now.duration_since(transfer_start_for_ticker).as_secs_f64()),
                            fmt_bytes_col_10(done_bytes),
                            fmt_rate_col(speed),
                            fmt_rate_col_opt(io_rates.src_read_bps),
                            fmt_rate_col_opt(io_rates.dst_write_bps),
                        );
                    }
                    last_report = now;
                    last_report_bytes = done_bytes;
                }
            }
        }
    });

    macro_rules! finish_transfer {
        ($rc:expr) => {{
            let final_done = done.load(Ordering::Relaxed);
            let elapsed = transfer_start.elapsed().as_secs_f64().max(1e-6);
            let final_speed = final_done as f64 / elapsed;
            let final_io_rates = io_rates_shared.lock().map(|g| *g).unwrap_or_default();
            if planned_bytes > 0 {
                let pct = (final_done as f64 * 100.0 / planned_bytes as f64).min(100.0);
                println!(
                    "{} {pct:6.2}% {} / {} | {} | {} {}",
                    fmt_hms_tenths(elapsed),
                    fmt_bytes_col_10(final_done),
                    format_bytes_binary(planned_bytes, 2),
                    fmt_rate_col(final_speed),
                    fmt_rate_col_opt(final_io_rates.src_read_bps),
                    fmt_rate_col_opt(final_io_rates.dst_write_bps),
                );
            } else {
                println!(
                    "{} ---% {} | {} | {} {}",
                    fmt_hms_tenths(elapsed),
                    fmt_bytes_col_10(final_done),
                    fmt_rate_col(final_speed),
                    fmt_rate_col_opt(final_io_rates.src_read_bps),
                    fmt_rate_col_opt(final_io_rates.dst_write_bps),
                );
            }
            let _ = ticker_stop_tx.send(());
            let _ = ticker.join();
            let (io_end_read, io_end_write) = io_window_for_avg.current_totals();
            return TransferOutcome {
                rc: $rc,
                bytes_done: final_done,
                elapsed_s: elapsed,
                io_read_bytes: counter_delta(io_start_read, io_end_read),
                io_write_bytes: counter_delta(io_start_write, io_end_write),
            };
        }};
    }

    match src_obj_kind {
        SrcObjKind::File => {
            let src = Path::new(src_path);
            let dst = Path::new(dst_path);
            let src_lmd = match fs::symlink_metadata(src) {
                Ok(v) => v,
                Err(_) => finish_transfer!(1),
            };
            if src_lmd.file_type().is_symlink() {
                let needs_copy = !symlink_targets_equal(src, dst);
                if needs_copy && copy_symlink(src, dst).is_err() {
                    finish_transfer!(1);
                }
                finish_transfer!(0);
            }
            let src_meta = match fs::metadata(src) {
                Ok(v) => v,
                Err(_) => finish_transfer!(1),
            };
            let needs_copy = match fs::metadata(dst) {
                Ok(dm) => !(dm.is_file() && dm.len() == src_meta.len()),
                Err(_) => true,
            };
            if needs_copy {
                let _permit = acquire_file_write_permit(inflight_limiter.as_ref(), src_meta.len(), media);
                if copy_file_preserve_with_progress_buf(src, dst, copy_buf_bytes, |n| {
                    done.fetch_add(n, Ordering::Relaxed);
                })
                .is_err()
                {
                    finish_transfer!(1);
                }
            }
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
                }
                let copy_ok = m
                    .copy_files
                    .par_iter()
                    .map(|entry| {
                        let src_file = src_root.join(&entry.rel);
                        let (dst_item, _) = map_dir_dest(include_root, &src_base, &entry.rel, dst_base);
                        let src_md = match fs::symlink_metadata(&src_file) {
                            Ok(md) => md,
                            Err(_) => return false,
                        };
                        if src_md.file_type().is_symlink() {
                            copy_symlink(&src_file, &dst_item).is_ok()
                        } else if src_md.is_file() {
                            let _permit =
                                acquire_file_write_permit(inflight_limiter.as_ref(), src_md.len(), media);
                            copy_file_preserve_with_progress_buf(&src_file, &dst_item, copy_buf_bytes, |n| {
                                done.fetch_add(n, Ordering::Relaxed);
                            })
                            .is_ok()
                        } else {
                            true
                        }
                    })
                    .reduce(|| true, |a, b| a && b);
                if !copy_ok {
                    finish_transfer!(1);
                }
            } else {
                let mut entries: Vec<PathBuf> = WalkDir::new(src_root)
                    .skip_hidden(false)
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
                    let (dst_item, _) = map_dir_dest(include_root, &src_base, &rel, dst_base);
                    let md = match fs::symlink_metadata(&p) {
                        Ok(v) => v,
                        Err(_) => finish_transfer!(1),
                    };
                    if md.is_dir() {
                        if fs::create_dir_all(&dst_item).is_err() {
                            finish_transfer!(1);
                        }
                        continue;
                    }
                    if md.file_type().is_symlink() {
                        let needs_copy = !symlink_targets_equal(&p, &dst_item);
                        if needs_copy && copy_symlink(&p, &dst_item).is_err() {
                            finish_transfer!(1);
                        }
                        continue;
                    }
                    if !md.is_file() {
                        continue;
                    }
                    let needs_copy = match fs::metadata(&dst_item) {
                        Ok(dm) => !(dm.is_file() && dm.len() == md.len()),
                        Err(_) => true,
                    };
                    if needs_copy {
                        let _permit = acquire_file_write_permit(inflight_limiter.as_ref(), md.len(), media);
                        if copy_file_preserve_with_progress_buf(&p, &dst_item, copy_buf_bytes, |n| {
                            done.fetch_add(n, Ordering::Relaxed);
                        })
                        .is_err()
                        {
                            finish_transfer!(1);
                        }
                    }
                }
            }
        }
    }

    finish_transfer!(0)
}

fn dev_media_kind(path: &Path) -> MediaKind {
    let md = match fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return MediaKind::Other,
    };
    let dev = md.dev();
    let maj = major(dev);
    let min = minor(dev);
    let sys_link = PathBuf::from(format!("/sys/dev/block/{maj}:{min}"));
    let canon = match fs::canonicalize(&sys_link) {
        Ok(c) => c,
        Err(_) => return MediaKind::Other,
    };

    let mut rotational: Option<bool> = None;
    let mut saw_nvme = false;

    for anc in canon.ancestors() {
        if let Some(name) = anc.file_name() {
            let n = name.to_string_lossy();
            if n.starts_with("nvme") {
                saw_nvme = true;
            }
            let q = Path::new("/sys/class/block").join(n.as_ref()).join("queue/rotational");
            if let Ok(s) = fs::read_to_string(&q) {
                let t = s.trim();
                if t == "0" {
                    rotational = Some(false);
                    if n.starts_with("nvme") {
                        saw_nvme = true;
                    }
                    break;
                }
                if t == "1" {
                    rotational = Some(true);
                    break;
                }
            }
        }
    }

    match rotational {
        Some(true) => MediaKind::Hdd,
        Some(false) if saw_nvme => MediaKind::Nvme,
        Some(false) => MediaKind::Other,
        None if canon.to_string_lossy().contains("/nvme") => MediaKind::Nvme,
        _ => MediaKind::Other,
    }
}

#[derive(Default, Clone)]
struct TreeNode {
    children: BTreeMap<String, TreeNode>,
    state: Option<String>,
    is_dir: bool,
}

fn build_change_tree(items: &[ChangeItem]) -> TreeNode {
    let mut root = TreeNode {
        children: BTreeMap::new(),
        state: None,
        is_dir: true,
    };

    for it in items {
        let mut p = it.rel.trim().trim_start_matches("./").to_string();
        if p.is_empty() {
            continue;
        }
        let leaf_is_dir = p.ends_with('/');
        p = p.trim_end_matches('/').to_string();
        if p.is_empty() {
            continue;
        }
        let parts: Vec<&str> = p.split('/').collect();

        let leaf_state = match it.kind {
            ChangeKind::NewFile | ChangeKind::NewDir => "added",
            ChangeKind::RemovedDir => "removed",
            _ => "modified",
        }
        .to_string();

        let mut node = &mut root;
        for (idx, part) in parts.iter().enumerate() {
            let is_leaf = idx == parts.len() - 1;
            node = node.children.entry((*part).to_string()).or_insert_with(|| TreeNode {
                children: BTreeMap::new(),
                state: None,
                is_dir: true,
            });

            if is_leaf {
                node.is_dir = leaf_is_dir;
                match leaf_state.as_str() {
                    "added" => {
                        if node.state.is_none() {
                            node.state = Some("added".to_string());
                        }
                    }
                    "removed" => node.state = Some("removed".to_string()),
                    _ => {
                        if node.state.as_deref() != Some("added") {
                            node.state = Some("modified".to_string());
                        }
                    }
                }
            } else {
                node.is_dir = true;
                if node.state.as_deref() != Some("added") {
                    node.state = Some("modified".to_string());
                }
            }
        }
    }

    root
}

#[derive(Clone)]
struct LevelEntry {
    name: String,
    state: String,
    is_dir: bool,
    node: Option<TreeNode>,
}

fn collect_level_entries(abs_dir: &Path, node: Option<&TreeNode>, extra: &HashMap<String, String>) -> Vec<LevelEntry> {
    let mut existing_entries: BTreeSet<String> = BTreeSet::new();
    if abs_dir.is_dir() {
        if let Ok(rd) = fs::read_dir(abs_dir) {
            for e in rd.flatten() {
                existing_entries.insert(e.file_name().to_string_lossy().to_string());
            }
        }
    }

    let mut changed: BTreeSet<String> = BTreeSet::new();
    if let Some(n) = node {
        for k in n.children.keys() {
            changed.insert(k.clone());
        }
    }

    let mut all: BTreeSet<String> = BTreeSet::new();
    all.extend(existing_entries.iter().cloned());
    all.extend(changed.iter().cloned());
    all.extend(extra.keys().cloned());

    let mut out = Vec::new();
    for name in all {
        let child = node.and_then(|n| n.children.get(&name)).cloned();
        let mut state = extra.get(&name).cloned();
        if state.is_none() {
            state = child.as_ref().and_then(|c| c.state.clone());
        }
        let state = state.unwrap_or_else(|| "unchanged".to_string());
        let full = abs_dir.join(&name);
        let is_dir = child.as_ref().map(|c| c.is_dir).unwrap_or_else(|| full.is_dir());
        out.push(LevelEntry { name, state, is_dir, node: child });
    }
    out
}

fn select_level_entries(entries: &[LevelEntry], max_entries: usize) -> (Vec<LevelEntry>, usize, usize, usize, usize) {
    let mut changed: Vec<LevelEntry> = entries
        .iter()
        .filter(|e| e.state != "unchanged")
        .cloned()
        .collect();

    changed.sort_by(|a, b| {
        let pa = match a.state.as_str() {
            "modified" | "replaced" => 0,
            "removed" => 1,
            "added" => 2,
            _ => 9,
        };
        let pb = match b.state.as_str() {
            "modified" | "replaced" => 0,
            "removed" => 1,
            "added" => 2,
            _ => 9,
        };
        pa.cmp(&pb).then(a.name.cmp(&b.name))
    });

    let selected: Vec<LevelEntry> = changed.into_iter().take(max_entries).collect();
    let selected_names: HashSet<String> = selected.iter().map(|e| e.name.clone()).collect();

    let mut hidden_new = 0usize;
    let mut hidden_mod = 0usize;
    let mut hidden_unch = 0usize;
    let mut hidden_rem = 0usize;

    for e in entries {
        if selected_names.contains(&e.name) {
            continue;
        }
        match e.state.as_str() {
            "added" => hidden_new += 1,
            "modified" | "replaced" => hidden_mod += 1,
            "removed" => hidden_rem += 1,
            _ => hidden_unch += 1,
        }
    }

    (selected, hidden_new, hidden_mod, hidden_unch, hidden_rem)
}

fn format_entry(entry: &LevelEntry, row_kind: Option<&str>) -> String {
    let suffix = if entry.is_dir { "/" } else { "" };
    if row_kind == Some("replaced_old") {
        return format!("{FAIL}{}{} (old){ENDC}", entry.name, suffix);
    }
    if row_kind == Some("replaced_new") {
        return format!("{OKGREEN}{}{} (new){ENDC}", entry.name, suffix);
    }
    match entry.state.as_str() {
        "removed" => format!("{FAIL}{}{} (removed){ENDC}", entry.name, suffix),
        "added" => format!("{OKGREEN}{}{}{ENDC}", entry.name, suffix),
        "modified" => format!("{WARNING}{}{}{ENDC}", entry.name, suffix),
        _ => format!("{WHITE}{}{}{ENDC}", entry.name, suffix),
    }
}

fn render_showall_level(abs_dir: &Path, node: Option<&TreeNode>, prefix: &str, extras: &HashMap<String, String>, depth: usize) {
    let entries = collect_level_entries(abs_dir, node, extras);
    let (selected, hn, hm, hu, hr) = select_level_entries(&entries, 5);

    enum Unit {
        Entry(LevelEntry, Option<&'static str>),
        Summary(usize, usize, usize, usize),
    }

    let mut units: Vec<Unit> = Vec::new();
    for entry in selected {
        if entry.state == "replaced" {
            units.push(Unit::Entry(entry.clone(), Some("replaced_old")));
            units.push(Unit::Entry(entry, Some("replaced_new")));
        } else {
            units.push(Unit::Entry(entry, None));
        }
    }

    if hn + hm + hu + hr > 0 {
        units.push(Unit::Summary(hn, hm, hu, hr));
    }

    for (idx, unit) in units.iter().enumerate() {
        let last = idx + 1 == units.len();
        let branch = if last { "└── " } else { "├── " };

        match unit {
            Unit::Summary(n_new, n_mod, n_unch, n_rem) => {
                let mut parts: Vec<String> = Vec::new();
                if *n_new > 0 {
                    parts.push(format!("{n_new} more new"));
                }
                if *n_mod > 0 {
                    parts.push(format!("{n_mod} more modified"));
                }
                if *n_unch > 0 {
                    parts.push(format!("{n_unch} more unchanged"));
                }
                if *n_rem > 0 {
                    parts.push(format!("{n_rem} more removed"));
                }
                println!("{prefix}{branch}... and {}", parts.join(" "));
            }
            Unit::Entry(entry, row_kind) => {
                println!("{prefix}{branch}{}", format_entry(entry, *row_kind));
                let should_expand = row_kind.is_none() && entry.is_dir && entry.state == "modified";
                if should_expand {
                    let child_prefix = format!("{prefix}{}", if last { "    " } else { "│   " });
                    let empty: HashMap<String, String> = HashMap::new();
                    render_showall_level(
                        &abs_dir.join(&entry.name),
                        entry.node.as_ref(),
                        &child_prefix,
                        &empty,
                        depth + 1,
                    );
                }
            }
        }
    }
}

fn print_showall_preview(
    preview_root: &Path,
    preview_items: &[ChangeItem],
    extra_added: &HashSet<String>,
    extra_modified: &HashSet<String>,
    extra_replaced: &HashSet<String>,
    extra_removed: &HashSet<String>,
) {
    let mut root_extra: HashMap<String, String> = HashMap::new();
    for n in extra_added {
        root_extra.insert(n.clone(), "added".to_string());
    }
    for n in extra_modified {
        if root_extra.get(n).map(|s| s.as_str()) != Some("added") {
            root_extra.insert(n.clone(), "modified".to_string());
        }
    }
    for n in extra_replaced {
        root_extra.insert(n.clone(), "replaced".to_string());
    }
    for n in extra_removed {
        root_extra.insert(n.clone(), "removed".to_string());
    }

    let tree = build_change_tree(preview_items);
    render_showall_level(preview_root, Some(&tree), "", &root_extra, 0);
}

struct TopPreviewData {
    root: PathBuf,
    top_states: HashMap<String, String>,
    top_is_dir: HashMap<String, bool>,
    unchanged_files: usize,
    unchanged_dirs: usize,
}

fn collect_top_level_preview(
    preview_root: &Path,
    preview_items: &[ChangeItem],
    extra_added: &HashSet<String>,
    extra_modified: &HashSet<String>,
    extra_replaced: &HashSet<String>,
    extra_removed: &HashSet<String>,
) -> TopPreviewData {
    let root = preview_root.to_path_buf();

    let mut existing_entries: BTreeSet<String> = BTreeSet::new();
    if root.is_dir() {
        if let Ok(rd) = fs::read_dir(&root) {
            for e in rd.flatten() {
                existing_entries.insert(e.file_name().to_string_lossy().to_string());
            }
        }
    }

    let mut top_states: HashMap<String, String> = HashMap::new();
    let mut top_is_dir: HashMap<String, bool> = HashMap::new();

    for it in preview_items {
        let mut item = it.rel.trim().trim_start_matches("./").to_string();
        if item.is_empty() {
            continue;
        }
        let is_dir = item.ends_with('/');
        item = item.trim_end_matches('/').to_string();
        if item.is_empty() {
            continue;
        }
        let top = item.split('/').next().unwrap_or("").to_string();
        if top.is_empty() {
            continue;
        }

        if is_dir || item.contains('/') {
            top_is_dir.insert(top.clone(), true);
        } else {
            top_is_dir.entry(top.clone()).or_insert(false);
        }

        let state = match it.kind {
            ChangeKind::NewFile | ChangeKind::NewDir => "added",
            ChangeKind::RemovedDir => "removed",
            _ => "modified",
        }
        .to_string();

        let prev = top_states.get(&top).cloned();
        if prev.as_deref() == Some("added") {
            continue;
        }
        if state == "added" || prev.is_none() {
            top_states.insert(top, state);
        } else {
            top_states.insert(top, "modified".to_string());
        }
    }

    for n in extra_added {
        top_states.insert(n.clone(), "added".to_string());
    }
    for n in extra_modified {
        if top_states.get(n).map(|s| s.as_str()) != Some("added") {
            top_states.insert(n.clone(), "modified".to_string());
        }
    }
    for n in extra_replaced {
        top_states.insert(n.clone(), "replaced".to_string());
    }
    for n in extra_removed {
        top_states.insert(n.clone(), "removed".to_string());
    }

    for name in top_states.clone().keys() {
        if top_states.get(name).map(|s| s.as_str()) == Some("added") && existing_entries.contains(name) {
            top_states.insert(name.clone(), "modified".to_string());
        }
    }

    let mut all_entries: BTreeSet<String> = BTreeSet::new();
    all_entries.extend(existing_entries.iter().cloned());
    all_entries.extend(top_states.keys().cloned());

    let mut unchanged_files = 0usize;
    let mut unchanged_dirs = 0usize;
    for name in &existing_entries {
        if top_states.contains_key(name) {
            continue;
        }
        let full = root.join(name);
        if full.is_dir() {
            unchanged_dirs += 1;
        } else {
            unchanged_files += 1;
        }
    }

    TopPreviewData {
        root,
        top_states,
        top_is_dir,
        unchanged_files,
        unchanged_dirs,
    }
}

fn print_changed_top_preview(
    preview_root: &Path,
    preview_items: &[ChangeItem],
    extra_added: &HashSet<String>,
    extra_modified: &HashSet<String>,
    extra_replaced: &HashSet<String>,
    extra_removed: &HashSet<String>,
) {
    let max_top_entries = 15usize;
    let d = collect_top_level_preview(
        preview_root,
        preview_items,
        extra_added,
        extra_modified,
        extra_replaced,
        extra_removed,
    );

    let mut changed_names: Vec<String> = d.top_states.keys().cloned().collect();
    changed_names.sort();

    let mut added_files = 0;
    let mut added_dirs = 0;
    let mut changed_files = 0;
    let mut changed_dirs = 0;

    for name in &changed_names {
        let full = d.root.join(name);
        let is_dir = *d.top_is_dir.get(name).unwrap_or(&full.is_dir());
        let state = d.top_states.get(name).map(|s| s.as_str()).unwrap_or("modified");
        if state == "added" {
            if is_dir {
                added_dirs += 1;
            } else {
                added_files += 1;
            }
        } else if is_dir {
            changed_dirs += 1;
        } else {
            changed_files += 1;
        }
    }

    let visible_names: Vec<String> = changed_names.iter().take(max_top_entries).cloned().collect();
    let hidden_names: Vec<String> = changed_names.iter().skip(max_top_entries).cloned().collect();

    let mut hidden_new = 0usize;
    let mut hidden_modified = 0usize;
    let mut hidden_removed = 0usize;
    let hidden_unchanged = d.unchanged_files + d.unchanged_dirs;

    for n in hidden_names {
        match d.top_states.get(&n).map(|s| s.as_str()).unwrap_or("modified") {
            "added" => hidden_new += 1,
            "removed" => hidden_removed += 1,
            _ => hidden_modified += 1,
        }
    }

    if visible_names.is_empty() {
        println!("(no new additions)");
    } else {
        let mut rows: Vec<(String, String, bool)> = Vec::new();
        for name in visible_names {
            let full = d.root.join(&name);
            let is_dir = *d.top_is_dir.get(&name).unwrap_or(&full.is_dir());
            let state = d.top_states.get(&name).cloned().unwrap_or_else(|| "modified".to_string());
            if state == "replaced" {
                rows.push(("replaced_old".to_string(), name.clone(), is_dir));
                rows.push(("replaced_new".to_string(), name.clone(), is_dir));
            } else {
                rows.push(("single".to_string(), name.clone(), is_dir));
            }
        }

        for (idx, (kind, name, is_dir)) in rows.iter().enumerate() {
            let last = idx + 1 == rows.len();
            let branch = if last { "└── " } else { "├── " };
            let suffix = if *is_dir { "/" } else { "" };
            let state = d.top_states.get(name).map(|s| s.as_str()).unwrap_or("modified");
            let (color, label) = if kind == "replaced_old" {
                (FAIL, " (old)")
            } else if kind == "replaced_new" {
                (OKGREEN, " (new)")
            } else if state == "removed" {
                (FAIL, " (removed)")
            } else if state == "added" {
                (OKGREEN, "")
            } else {
                (WARNING, "")
            };
            println!("{branch}{color}{name}{suffix}{label}{ENDC}");
        }
    }

    let hidden_total = hidden_new + hidden_modified + hidden_removed + hidden_unchanged;
    if hidden_total > 0 {
        println!(
            "... and {} more new {} more modified {} more unchanged and {} more removed",
            hidden_new, hidden_modified, hidden_unchanged, hidden_removed
        );
        println!();
    }

    println!(
        "Top level: Added: dirs={added_dirs} files={added_files} | Changed: dirs={changed_dirs} files={changed_files} | Unchanged: dirs={} files={}",
        d.unchanged_dirs,
        d.unchanged_files
    );
}

mod libc_time {
    use nix::libc::{localtime_r, time_t, tm};

    #[derive(Clone, Copy)]
    pub struct LocalTime {
        pub year: i32,
        pub month: i32,
        pub day: i32,
        pub hour: i32,
        pub min: i32,
        pub sec: i32,
    }

    impl LocalTime {
        pub fn from_unix(secs: i64) -> Option<Self> {
            let mut out = tm {
                tm_sec: 0,
                tm_min: 0,
                tm_hour: 0,
                tm_mday: 0,
                tm_mon: 0,
                tm_year: 0,
                tm_wday: 0,
                tm_yday: 0,
                tm_isdst: 0,
                #[cfg(any(target_env = "gnu", target_env = "musl"))]
                tm_gmtoff: 0,
                #[cfg(any(target_env = "gnu", target_env = "musl"))]
                tm_zone: std::ptr::null(),
            };
            let mut t: time_t = secs as time_t;
            let ptr = unsafe { localtime_r(&mut t, &mut out) };
            if ptr.is_null() {
                return None;
            }
            Some(Self {
                year: out.tm_year + 1900,
                month: out.tm_mon + 1,
                day: out.tm_mday,
                hour: out.tm_hour,
                min: out.tm_min,
                sec: out.tm_sec,
            })
        }

        pub fn fallback(secs: i64) -> Self {
            let sec = (secs % 60).abs() as i32;
            Self {
                year: 1970,
                month: 1,
                day: 1,
                hour: 0,
                min: 0,
                sec,
            }
        }
    }
}

fn run_remote_transfer_mode(
    requested_mode: TransferMode,
    source_input: &str,
    source: &str,
    destination: &str,
    source_remote: Option<RemoteSpec>,
    destination_remote: Option<RemoteSpec>,
    use_sudo: bool,
    contents_mode_requested: bool,
    overwrite: bool,
    backup_requested: bool,
    show_all: bool,
    preview_only: bool,
) -> i32 {
    if source_remote.is_some() && destination_remote.is_some() {
        log(
            requested_mode,
            "Remote-to-remote paths are not supported in this mode.",
            LogLevel::Error,
        );
        return 1;
    }

    let is_move = requested_mode == TransferMode::Move;
    let mut local_src_kind: Option<SrcObjKind> = None;
    let source_ep = match source_remote {
        Some(r) => Endpoint::Remote(enrich_remote_spec(r)),
        None => match resolve_source(source, requested_mode) {
            Ok((p, k)) => {
                local_src_kind = Some(k);
                Endpoint::Local(p)
            }
            Err(code) => return code,
        },
    };

    let destination_ep = match destination_remote {
        Some(r) => Endpoint::Remote(enrich_remote_spec(r)),
        None => {
            if let Some(kind) = local_src_kind {
                let resolved = match kind {
                    SrcObjKind::File => resolve_destination_for_file(destination, requested_mode),
                    SrcObjKind::Dir => resolve_destination_for_dir(destination, requested_mode, false),
                };
                match resolved {
                    Ok((p, _)) => Endpoint::Local(p),
                    Err(code) => return code,
                }
            } else {
                let p = to_real_path(destination);
                if !p.exists() {
                    let parent = p.parent().unwrap_or_else(|| Path::new("."));
                    if !parent.is_dir() {
                        log(
                            requested_mode,
                            &format!("Destination parent directory does not exist: {}", parent.display()),
                            LogLevel::Error,
                        );
                        return 1;
                    }
                }
                Endpoint::Local(p)
            }
        }
    };

    if overwrite {
        log(
            requested_mode,
            "--overwrite is not supported for remote endpoints; using rsync merge semantics.",
            LogLevel::Warn,
        );
    }
    if backup_requested {
        log(
            requested_mode,
            "--backup is not supported for remote endpoints; continuing without backup.",
            LogLevel::Warn,
        );
    }
    if show_all {
        log(
            requested_mode,
            "--showall preview tree is not available for remote endpoints.",
            LogLevel::Warn,
        );
    }

    let contents_active = contents_mode_requested && !matches!(local_src_kind, Some(SrcObjKind::File));
    let mode_dir_active = matches!(local_src_kind, Some(SrcObjKind::Dir)) && !contents_active;
    println!(
        "{}",
        [
            fmt_mode_word("Overwrite", false),
            fmt_mode_word("Move", is_move),
            fmt_mode_word("Copy", !is_move),
            fmt_mode_word("Merge", true),
            fmt_mode_word("Rename", false),
            fmt_mode_word("Backup", false),
            fmt_mode_word("File", matches!(local_src_kind, Some(SrcObjKind::File))),
            fmt_mode_word("Dir", mode_dir_active),
            fmt_mode_word("Contents", contents_active),
        ]
        .join(" ")
    );
    println!();

    let src_path = endpoint_to_rsync(&source_ep, true, contents_active, local_src_kind);
    let dst_path = endpoint_to_rsync(&destination_ep, false, false, local_src_kind);
    println!("{WARNING}Remote rsync mode: detailed pre-scan is skipped for remote endpoints.{ENDC}");
    println!("Source: {WHITE}{src_path}{ENDC}");
    println!("Destination: {WHITE}{dst_path}{ENDC}");
    println!();

    if preview_only {
        return 0;
    }

    print!("Proceed with {}? [y/N]: ", requested_mode.word());
    let _ = io::stdout().flush();
    let mut ans = String::new();
    let _ = io::stdin().read_line(&mut ans);
    let ans = ans.trim().to_ascii_lowercase();
    if ans != "y" && ans != "yes" {
        println!("{FAIL}Cancelled.{ENDC}");
        return 0;
    }

    if use_sudo {
        let _ = Command::new("sudo").arg("-v").status();
    }

    log(
        requested_mode,
        &format!(
            "Starting {} (rsync backend): {} -> {}...",
            requested_mode.word(),
            source_input,
            destination
        ),
        LogLevel::Info,
    );
    let start_ts = Instant::now();
    let transfer = run_rsync_transfer(&src_path, &dst_path, 0, use_sudo, is_move);

    if is_move && (transfer.rc == 0 || transfer.rc == 24) && matches!(source_ep, Endpoint::Local(_)) {
        if let (Endpoint::Local(src_local), Some(SrcObjKind::Dir)) = (&source_ep, local_src_kind) {
            cleanup_source_dirs(src_local, !contents_active, use_sudo, requested_mode);
        }
    }

    let result = if transfer.rc == 0 {
        log(
            requested_mode,
            &format!("{} complete.", requested_mode.word_cap()),
            LogLevel::Info,
        );
        0
    } else if transfer.rc == 24 {
        log(
            requested_mode,
            &format!(
                "{} failed: some source files vanished during transfer (rsync exit 24).",
                requested_mode.word_cap()
            ),
            LogLevel::Error,
        );
        1
    } else {
        log(
            requested_mode,
            &format!(
                "{} failed: transfer exited with status {}.",
                requested_mode.word_cap(),
                transfer.rc
            ),
            LogLevel::Error,
        );
        1
    };

    let total_elapsed_s = start_ts.elapsed().as_secs_f64();
    let avg_transfer_bps = if transfer.elapsed_s > 0.0 {
        transfer.bytes_done as f64 / transfer.elapsed_s
    } else {
        0.0
    };
    let avg_total_bps = if total_elapsed_s > 0.0 {
        transfer.bytes_done as f64 / total_elapsed_s
    } else {
        0.0
    };
    print_summary_rate_line("Average transfer speed", avg_transfer_bps, transfer.elapsed_s, false);
    print_summary_rate_line("Overall throughput", avg_total_bps, total_elapsed_s, true);
    result
}

fn main() {
    std::process::exit(real_main());
}

fn real_main() -> i32 {
    let args = match parse_args() {
        Ok(a) => a,
        Err(code) => return code,
    };

    let requested_mode = if args.move_mode { TransferMode::Move } else { TransferMode::Copy };
    let is_move = requested_mode == TransferMode::Move;

    if !args.extra.is_empty() {
        log(
            requested_mode,
            "Unexpected extra path arguments. If using '*', quote it (e.g. 'src/*').",
            LogLevel::Error,
        );
        return 1;
    }

    let source_input = args.source.clone();
    let mut source = source_input.clone();
    let destination = args.destination.clone();
    let use_sudo = args.sudo;
    let show_all = args.showall;
    let preview_lite = args.preview_lite;
    let preview_only = args.preview_only || preview_lite;
    let backup_requested = args.backup;
    let overwrite = args.overwrite;
    let force_requested = args.contents_only;
    let mut source_glob_contents = false;

    if source.ends_with("/*") {
        source.pop();
        source_glob_contents = true;
    }

    let force = force_requested || source_glob_contents;
    let contents_mode_requested = force_requested || source_glob_contents;
    let source_remote = parse_remote_spec(&source);
    let destination_remote = parse_remote_spec(&destination).map(enrich_remote_spec);

    if source_remote.is_some() || destination_remote.is_some() {
        return run_remote_transfer_mode(
            requested_mode,
            &source_input,
            &source,
            &destination,
            source_remote.map(enrich_remote_spec),
            destination_remote,
            use_sudo,
            contents_mode_requested,
            overwrite,
            backup_requested,
            show_all,
            preview_only,
        );
    }

    let (src_mnt, src_obj_kind) = match resolve_source(&source, requested_mode) {
        Ok(v) => v,
        Err(code) => return code,
    };

    let (dst_mnt, dst_obj_kind) = match src_obj_kind {
        SrcObjKind::File => match resolve_destination_for_file(&destination, requested_mode) {
            Ok(v) => v,
            Err(code) => return code,
        },
        SrcObjKind::Dir => match resolve_destination_for_dir(&destination, requested_mode, overwrite) {
            Ok(v) => v,
            Err(code) => return code,
        },
    };

    let source_contents_mode = source_glob_contents && !force;
    let dest_tail_raw = destination.trim_end_matches('/').split('/').last().unwrap_or("");
    let destination_is_dir_ref = destination.ends_with('/') || dest_tail_raw.is_empty() || dest_tail_raw == "." || dest_tail_raw == "..";

    let mut rename_dir_to_new_path = false;
    let mut merge_child_into_parent = false;
    let mut source_already_in_destination = false;
    let mut overwrite_parent_from_child = false;

    if src_obj_kind == SrcObjKind::Dir && matches!(dst_obj_kind, DstObjKind::Dir | DstObjKind::DirExisting) {
        let dst_slot_for_src = realpath_allow_missing(&dst_mnt.join(src_mnt.file_name().unwrap_or_default()));
        if dst_slot_for_src == src_mnt {
            let src_base = src_mnt.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
            let dst_base = dst_mnt.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
            if src_base == dst_base {
                source_already_in_destination = true;
                if force && !source_contents_mode {
                    merge_child_into_parent = true;
                    source_already_in_destination = false;
                }
            } else {
                source_already_in_destination = true;
                if overwrite && !source_contents_mode && !destination_is_dir_ref {
                    overwrite_parent_from_child = true;
                    source_already_in_destination = false;
                } else if force && !source_contents_mode {
                    source_already_in_destination = false;
                }
            }
        } else {
            let src_parent_real = realpath_allow_missing(src_mnt.parent().unwrap_or_else(|| Path::new(".")));
            if dst_slot_for_src == src_parent_real {
                source_already_in_destination = true;
                if force && !source_contents_mode {
                    merge_child_into_parent = true;
                    source_already_in_destination = false;
                }
            }
        }
    }

    let rename_style_existing_dir_target = src_obj_kind == SrcObjKind::Dir
        && dst_obj_kind == DstObjKind::DirExisting
        && !source_contents_mode
        && !destination_is_dir_ref
        && src_mnt.file_name() != dst_mnt.file_name();

    let mut target_dir_for_name: Option<PathBuf> = None;
    let mut target_name_for_conflict: Option<String> = None;
    let mut overwrite_rename_dir_target = false;
    let mut overwrite_replace_file_target = false;

    if overwrite_parent_from_child {
        overwrite_rename_dir_target = true;
    } else if overwrite && force && rename_style_existing_dir_target {
        overwrite_rename_dir_target = true;
    }

    if overwrite && src_obj_kind == SrcObjKind::Dir && dst_obj_kind == DstObjKind::FileExistingForDir && !source_contents_mode {
        overwrite_replace_file_target = true;
    }

    let force_merge_dir_target = force
        && src_obj_kind == SrcObjKind::Dir
        && matches!(dst_obj_kind, DstObjKind::Dir | DstObjKind::DirExisting)
        && !source_contents_mode
        && !source_already_in_destination
        && !overwrite_parent_from_child
        && !overwrite_rename_dir_target;

    if matches!(dst_obj_kind, DstObjKind::Dir | DstObjKind::DirExisting) {
        if overwrite_rename_dir_target || force_merge_dir_target || (merge_child_into_parent && src_obj_kind == SrcObjKind::Dir) {
            target_dir_for_name = dst_mnt.parent().map(|p| p.to_path_buf());
            target_name_for_conflict = dst_mnt.file_name().map(|s| s.to_string_lossy().to_string());
        } else {
            target_dir_for_name = Some(dst_mnt.clone());
            target_name_for_conflict = match src_obj_kind {
                SrcObjKind::Dir => src_mnt.file_name().map(|s| s.to_string_lossy().to_string()),
                SrcObjKind::File => src_mnt.file_name().map(|s| s.to_string_lossy().to_string()),
            };
        }
    } else if dst_obj_kind == DstObjKind::DirNew && src_obj_kind == SrcObjKind::Dir {
        target_dir_for_name = dst_mnt.parent().map(|p| p.to_path_buf());
        target_name_for_conflict = dst_mnt.file_name().map(|s| s.to_string_lossy().to_string());
    } else if matches!(dst_obj_kind, DstObjKind::File | DstObjKind::FileExistingForDir) {
        target_dir_for_name = dst_mnt.parent().map(|p| p.to_path_buf());
        target_name_for_conflict = dst_mnt.file_name().map(|s| s.to_string_lossy().to_string());
    }

    let mut target_conflict_path: Option<PathBuf> = None;
    let mut existing_same_name_target = false;
    if let (Some(dir), Some(name)) = (&target_dir_for_name, &target_name_for_conflict) {
        let p = dir.join(name);
        existing_same_name_target = p.exists();
        target_conflict_path = Some(p);
    }

    let mut overwrite_target_path: Option<PathBuf> = None;
    let mut overwrite_target_kind: Option<&str> = None;

    if overwrite_rename_dir_target {
        let candidate_real = realpath_allow_missing(&dst_mnt);
        if candidate_real == src_mnt {
            log(requested_mode, "Refusing to overwrite source directory itself.", LogLevel::Error);
            return 1;
        }
        overwrite_target_path = Some(candidate_real);
        overwrite_target_kind = Some("dir");
    } else if overwrite_replace_file_target {
        let candidate_real = realpath_allow_missing(&dst_mnt);
        if candidate_real == src_mnt {
            log(requested_mode, "Refusing to overwrite source directory itself.", LogLevel::Error);
            return 1;
        }
        overwrite_target_path = Some(candidate_real);
        overwrite_target_kind = Some("file");
    } else if overwrite
        && src_obj_kind == SrcObjKind::Dir
        && matches!(dst_obj_kind, DstObjKind::Dir | DstObjKind::DirExisting)
        && !source_contents_mode
        && !merge_child_into_parent
        && !source_already_in_destination
    {
        if let Some(src_name) = src_mnt.file_name() {
            let candidate = dst_mnt.join(src_name);
            if candidate.exists() && candidate.is_dir() {
                let candidate_real = realpath_allow_missing(&candidate);
                if candidate_real == src_mnt {
                    log(requested_mode, "Refusing to overwrite source directory itself.", LogLevel::Error);
                    return 1;
                }
                overwrite_target_path = Some(candidate_real);
                overwrite_target_kind = Some("dir");
            }
        }
    }

    let src_path = match src_obj_kind {
        SrcObjKind::File => src_mnt.display().to_string(),
        SrcObjKind::Dir => {
            let src_s = src_mnt.display().to_string();
            if (overwrite_rename_dir_target || overwrite_replace_file_target)
                && !source_contents_mode
            {
                rename_dir_to_new_path = true;
                format!("{}/", src_s.trim_end_matches('/'))
            } else if force_merge_dir_target && !source_contents_mode {
                format!("{}/", src_s.trim_end_matches('/'))
            } else if dst_obj_kind == DstObjKind::DirNew && !source_contents_mode {
                if !force {
                    rename_dir_to_new_path = true;
                }
                format!("{}/", src_s.trim_end_matches('/'))
            } else if merge_child_into_parent && !source_contents_mode {
                format!("{}/", src_s.trim_end_matches('/'))
            } else if source_contents_mode {
                format!("{}/", src_s.trim_end_matches('/'))
            } else {
                src_s.trim_end_matches('/').to_string()
            }
        }
    };

    let dst_path = if overwrite_rename_dir_target || overwrite_replace_file_target {
        dst_mnt.display().to_string().trim_end_matches('/').to_string()
    } else if matches!(dst_obj_kind, DstObjKind::Dir | DstObjKind::DirExisting) {
        format!("{}/", dst_mnt.display().to_string().trim_end_matches('/'))
    } else {
        dst_mnt.display().to_string().trim_end_matches('/').to_string()
    };

    let src_base = src_mnt.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
    let src_parent = src_mnt.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("."));

    let target_base = target_name_for_conflict.clone().unwrap_or_default();
    let target_parent = target_dir_for_name.clone().unwrap_or_default();

    let mut _mode_move = realpath_allow_missing(&src_parent) != realpath_allow_missing(&target_parent);
    if merge_child_into_parent || overwrite_parent_from_child {
        _mode_move = false;
    }

    let mut mode_rename = !src_base.is_empty() && !target_base.is_empty() && src_base != target_base;
    if force_merge_dir_target || merge_child_into_parent {
        mode_rename = false;
    }

    let mut mode_overwrite = false;
    let mut mode_merge = false;
    if src_obj_kind == SrcObjKind::Dir {
        if overwrite_target_path.is_some() || (existing_same_name_target && overwrite) {
            mode_overwrite = true;
        } else if force_merge_dir_target || existing_same_name_target {
            mode_merge = true;
        }
    } else if existing_same_name_target {
        mode_overwrite = true;
    }

    if source_already_in_destination {
        mode_overwrite = false;
        _mode_move = false;
        mode_merge = false;
        mode_rename = false;
    }

    let mut backup_source_path: Option<PathBuf> = None;
    let mut backup_source_kind: Option<&str> = None;
    if backup_requested && !source_already_in_destination {
        if let Some(otp) = &overwrite_target_path {
            backup_source_path = Some(otp.clone());
            backup_source_kind = overwrite_target_kind;
        } else if (mode_merge || mode_overwrite)
            && target_conflict_path.as_ref().map(|p| p.exists()).unwrap_or(false)
        {
            if let Some(tp) = &target_conflict_path {
                let p = realpath_allow_missing(tp);
                if p != src_mnt {
                    backup_source_kind = Some(if p.is_dir() { "dir" } else { "file" });
                    backup_source_path = Some(p);
                }
            }
        }
    }

    let mut planned_backup_path: Option<PathBuf> = None;
    if let Some(bsp) = &backup_source_path {
        planned_backup_path = plan_backup_path(bsp);
        if planned_backup_path.is_none() {
            log(
                requested_mode,
                &format!("Failed to plan backup path for: {}", bsp.display()),
                LogLevel::Error,
            );
            return 1;
        }
    }

    let mode_backup = planned_backup_path.is_some();
    let contents_mode_active = contents_mode_requested && src_obj_kind == SrcObjKind::Dir;
    let mode_dir_active = src_obj_kind == SrcObjKind::Dir && !contents_mode_active;

    println!(
        "{}",
        [
            fmt_mode_word("Overwrite", mode_overwrite),
            fmt_mode_word("Move", is_move),
            fmt_mode_word("Copy", !is_move),
            fmt_mode_word("Merge", mode_merge),
            fmt_mode_word("Rename", mode_rename),
            fmt_mode_word("Backup", mode_backup),
            fmt_mode_word("File", src_obj_kind == SrcObjKind::File),
            fmt_mode_word("Dir", mode_dir_active),
            fmt_mode_word("Contents", contents_mode_active),
        ]
        .join(" ")
    );
    println!();

    if use_sudo && !preview_only {
        let _ = Command::new("sudo").arg("-v").status();
    }

    let media = dev_media_kind(&src_mnt);
    let backend = if use_sudo {
        TransferBackend::Rsync
    } else {
        TransferBackend::Rust
    };
    configure_rayon_threads_for_media(media);
    let build_transfer_manifest =
        !preview_only && src_obj_kind == SrcObjKind::Dir && (matches!(backend, TransferBackend::Rust) || is_move);

    let prescan = if source_already_in_destination {
        PreScan::default()
    } else {
        let mut pre_dst_path = dst_path.clone();
        let mut preflight_tmpdir: Option<TempDir> = None;

        if src_obj_kind == SrcObjKind::Dir && overwrite_target_path.is_some() {
            let pre_parent = dst_mnt.parent().unwrap_or_else(|| Path::new("."));
            if let Ok(td) = tempfile::Builder::new().prefix(&format!(".{}-preflight-", requested_mode.word())).tempdir_in(pre_parent) {
                pre_dst_path = td.path().join("target").display().to_string();
                preflight_tmpdir = Some(td);
            }
        }

        let ps = match src_obj_kind {
            SrcObjKind::Dir => {
                pre_scan_directory(&src_path, &pre_dst_path, &src_mnt, show_all, preview_lite, build_transfer_manifest)
            }
            SrcObjKind::File => pre_scan_file(&src_mnt, &pre_dst_path, dst_obj_kind),
        };

        drop(preflight_tmpdir);
        ps
    };

    let planned_bytes = prescan.planned_bytes;
    let planned_bytes_exact = prescan.planned_bytes_exact;
    let total_regular_files = prescan.total_regular_files;
    let total_regular_bytes = prescan.total_regular_bytes;
    let total_dirs = prescan.total_dirs;
    let add_files = prescan.add_files;
    let mod_files = prescan.mod_files;
    let unaffected_files = prescan.unaffected_files;
    let add_dirs = prescan.add_dirs;
    let mod_dirs = prescan.mod_dirs;
    let unaffected_dirs = prescan.unaffected_dirs;
    let transfer_manifest = prescan.transfer_manifest;
    let mut display_change_preview = prescan.change_preview.clone();
    let has_itemized_changes = prescan.has_itemized_changes;

    let (manifest_cleanup_files, manifest_cleanup_bytes) = if is_move {
        if let Some(m) = transfer_manifest.as_ref() {
            let mut seen: HashSet<&str> = HashSet::new();
            let mut files: u64 = 0;
            let mut bytes: u64 = 0;
            for e in m.identical_files.iter().chain(m.copy_files.iter()) {
                if e.rel.is_empty() || !seen.insert(e.rel.as_str()) {
                    continue;
                }
                files += 1;
                bytes = bytes.saturating_add(e.size);
            }
            (files, bytes)
        } else {
            (0, 0)
        }
    } else {
        (0, 0)
    };

    let dst_preview_root = if matches!(dst_obj_kind, DstObjKind::Dir | DstObjKind::DirExisting | DstObjKind::DirNew) {
        PathBuf::from(format!("{}/", dst_path.trim_end_matches('/')))
    } else {
        PathBuf::from(
            Path::new(dst_path.trim_end_matches('/'))
                .parent()
                .unwrap_or_else(|| Path::new("/"))
                .display()
                .to_string()
                + "/",
        )
    };

    let mut simple_rename_src: Option<String> = None;
    let mut simple_rename_dst: Option<String> = None;
    let mut simple_rename_parent: Option<PathBuf> = None;
    let mut rename_target_only: Option<String> = None;
    let mut rename_target_is_dir = false;

    if src_obj_kind == SrcObjKind::Dir && rename_dir_to_new_path {
        let src_base = src_mnt.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let dst_base = dst_mnt.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let src_parent = src_mnt.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("/"));
        let dst_parent = dst_mnt.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("/"));
        if src_parent == dst_parent && !src_base.is_empty() && !dst_base.is_empty() && src_base != dst_base {
            simple_rename_src = Some(src_base);
            simple_rename_dst = Some(dst_base);
            simple_rename_parent = Some(src_parent);
        } else if !src_base.is_empty() && !dst_base.is_empty() {
            rename_target_only = Some(dst_base);
            rename_target_is_dir = true;
        }
    } else if src_obj_kind == SrcObjKind::File && dst_obj_kind == DstObjKind::File {
        let src_base = src_mnt.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let dst_base = dst_mnt.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let src_parent = src_mnt.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("/"));
        let dst_parent = dst_mnt.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("/"));
        if src_parent == dst_parent && !src_base.is_empty() && !dst_base.is_empty() && src_base != dst_base {
            simple_rename_src = Some(src_base);
            simple_rename_dst = Some(dst_base);
            simple_rename_parent = Some(src_parent);
        } else if !src_base.is_empty() && !dst_base.is_empty() && (src_parent != dst_parent || src_base != dst_base) {
            rename_target_only = Some(dst_base);
        }
    }

    let preview_inside_target_dir = rename_target_only.is_some() && rename_target_is_dir && overwrite_target_path.is_some();

    let mut preview_root = if let Some(parent) = &simple_rename_parent {
        PathBuf::from(format!("{}/", parent.display().to_string().trim_end_matches('/')))
    } else if rename_target_only.is_some() && !preview_inside_target_dir && !rename_target_is_dir {
        let rename_parent = if rename_target_is_dir {
            dst_mnt.parent().unwrap_or_else(|| Path::new("/"))
        } else {
            dst_mnt.parent().unwrap_or_else(|| Path::new("/"))
        };
        PathBuf::from(format!("{}/", rename_parent.display().to_string().trim_end_matches('/')))
    } else {
        dst_preview_root.clone()
    };

    if simple_rename_parent.is_some() && src_obj_kind == SrcObjKind::Dir && simple_rename_dst.is_some() && !display_change_preview.is_empty() {
        let dst_name = simple_rename_dst.clone().unwrap_or_default();
        let mut remapped = Vec::new();
        for ch in display_change_preview {
            let item = ch.rel.trim_start_matches("./").to_string();
            let rel = if item.starts_with(&format!("{dst_name}/")) || item == dst_name {
                item
            } else if item.is_empty() {
                format!("{dst_name}/")
            } else {
                format!("{dst_name}/{item}")
            };
            remapped.push(ChangeItem { kind: ch.kind, rel });
        }
        display_change_preview = remapped;
    }

    if let Some(pb) = &planned_backup_path {
        if overwrite_target_path.is_none() {
            let backup_parent = pb.parent().unwrap_or_else(|| Path::new("/"));
            let current_root = PathBuf::from(preview_root.to_string_lossy().trim_end_matches('/').to_string());
            if realpath_allow_missing(backup_parent) != realpath_allow_missing(&current_root) {
                let current_root_name = current_root.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
                if !current_root_name.is_empty() && !display_change_preview.is_empty() {
                    let mut remapped = Vec::new();
                    for ch in display_change_preview {
                        let item = ch.rel.trim_start_matches("./").to_string();
                        let rel = if item.starts_with(&format!("{current_root_name}/")) || item == current_root_name {
                            item
                        } else if item.is_empty() {
                            format!("{current_root_name}/")
                        } else {
                            format!("{current_root_name}/{item}")
                        };
                        remapped.push(ChangeItem { kind: ch.kind, rel });
                    }
                    display_change_preview = remapped;
                }
                preview_root = PathBuf::from(format!("{}/", backup_parent.display().to_string().trim_end_matches('/')));
            }
        }
    }

    if is_move && merge_child_into_parent && src_obj_kind == SrcObjKind::Dir {
        let preview_root_real = realpath_allow_missing(Path::new(preview_root.to_string_lossy().trim_end_matches('/')));
        let src_real = realpath_allow_missing(&src_mnt);
        if let Ok(removed_rel) = src_real.strip_prefix(&preview_root_real) {
            let rel = normalize_rel(removed_rel);
            if !rel.is_empty() && rel != "." && rel != ".." && !rel.starts_with("../") {
                display_change_preview.push(ChangeItem {
                    kind: ChangeKind::RemovedDir,
                    rel: format!("{}/", rel.trim_end_matches('/')),
                });
            }
        }
    }

    let preview_root_lossy = preview_root.to_string_lossy();
    let preview_root_trimmed = Path::new(preview_root_lossy.trim_end_matches('/'));
    let highlight_new_preview_leaf = src_obj_kind == SrcObjKind::Dir
        && matches!(dst_obj_kind, DstObjKind::DirNew)
        && !preview_root_trimmed.exists();
    print_preview_root_line(&preview_root, highlight_new_preview_leaf);

    let mut extra_added: HashSet<String> = HashSet::new();
    let mut extra_modified: HashSet<String> = HashSet::new();
    let mut extra_replaced: HashSet<String> = HashSet::new();
    let mut extra_removed: HashSet<String> = HashSet::new();

    if let Some(pb) = &planned_backup_path {
        if overwrite_target_path.is_none() {
            if let Some(n) = pb.file_name().map(|s| s.to_string_lossy().to_string()) {
                extra_added.insert(n);
            }
        }
    }
    if let Some(otp) = &overwrite_target_path {
        if !preview_inside_target_dir {
            if let Some(n) = otp.file_name().map(|s| s.to_string_lossy().to_string()) {
                extra_replaced.insert(n);
            }
        }
    }
    if let Some(rt) = &rename_target_only {
        if !preview_inside_target_dir && !rename_target_is_dir {
            if existing_same_name_target {
                extra_modified.insert(rt.trim_end_matches('/').to_string());
            } else {
                extra_added.insert(rt.trim_end_matches('/').to_string());
            }
        }
    }
    if let Some(sd) = &simple_rename_dst {
        if existing_same_name_target {
            extra_modified.insert(sd.trim_end_matches('/').to_string());
        } else {
            extra_added.insert(sd.trim_end_matches('/').to_string());
        }
    }
    if is_move {
        if let Some(ss) = &simple_rename_src {
            extra_removed.insert(ss.trim_end_matches('/').to_string());
        }
    }

    if show_all {
        print_showall_preview(
            Path::new(preview_root.to_string_lossy().trim_end_matches('/')),
            &display_change_preview,
            &extra_added,
            &extra_modified,
            &extra_replaced,
            &extra_removed,
        );
    } else {
        print_changed_top_preview(
            Path::new(preview_root.to_string_lossy().trim_end_matches('/')),
            &display_change_preview,
            &extra_added,
            &extra_modified,
            &extra_replaced,
            &extra_removed,
        );
    }

    let overwrite_requires_action = overwrite_target_path.as_ref().map(|p| p.exists()).unwrap_or(false);
    let move_cleanup_only = is_move
        && !source_already_in_destination
        && planned_bytes == 0
        && !has_itemized_changes
        && !overwrite_requires_action;

    let likely_cleanup_files = if is_move && !source_already_in_destination {
        if manifest_cleanup_files > 0 {
            manifest_cleanup_files
        } else {
            total_regular_files.unwrap_or(0)
        }
    } else {
        0
    };
    let likely_cleanup_bytes = if is_move && !source_already_in_destination {
        if manifest_cleanup_bytes > 0 {
            manifest_cleanup_bytes
        } else {
            total_regular_bytes.unwrap_or(0)
        }
    } else {
        0
    };
    let likely_cleanup_dirs = if is_move && !source_already_in_destination {
        total_dirs.unwrap_or(0)
    } else {
        0
    };

    let file_row = total_regular_files.map(|total_regular| {
        let identical_files = total_regular.saturating_sub(add_files + mod_files);
        let deleted_src_files = if is_move && !source_already_in_destination {
            if likely_cleanup_files > 0 {
                likely_cleanup_files
            } else {
                total_regular
            }
        } else {
            0
        };
        let deleted_dest_files = overwrite_target_path
            .as_ref()
            .map(|p| count_regular_files_any(p))
            .unwrap_or(0);
        (
            add_files,
            mod_files,
            identical_files,
            unaffected_files,
            deleted_src_files,
            deleted_dest_files,
        )
    });
    let dir_row = total_dirs.map(|total_source_dirs| {
        let identical_dirs = total_source_dirs.saturating_sub(add_dirs + mod_dirs);
        let deleted_src_dirs = if is_move && !source_already_in_destination {
            likely_cleanup_dirs
        } else {
            0
        };
        let deleted_dest_dirs = overwrite_target_path
            .as_ref()
            .map(|p| count_directories_any(p, true))
            .unwrap_or(0);
        (
            add_dirs,
            mod_dirs,
            identical_dirs,
            unaffected_dirs,
            deleted_src_dirs,
            deleted_dest_dirs,
        )
    });
    if file_row.is_some() || dir_row.is_some() {
        println!(
            "{:<5} | {:>10} | {:>10} | {:>10} | {:>10} | {:>13} | {:>14}",
            "Type",
            "New",
            "Modified",
            "Identical",
            "Unaffected",
            "Deleted (src)",
            "Deleted (dest)"
        );
        if let Some((new_v, mod_v, identical_v, unaffected_v, deleted_src_v, deleted_dest_v)) = file_row {
            println!(
                "{:<5} | {:>10} | {:>10} | {:>10} | {:>10} | {:>13} | {:>14}",
                "Files",
                format_number(new_v),
                format_number(mod_v),
                format_number(identical_v),
                format_number(unaffected_v),
                format_number(deleted_src_v),
                format_number(deleted_dest_v)
            );
        }
        if let Some((new_v, mod_v, identical_v, unaffected_v, deleted_src_v, deleted_dest_v)) = dir_row {
            println!(
                "{:<5} | {:>10} | {:>10} | {:>10} | {:>10} | {:>13} | {:>14}",
                "Dirs",
                format_number(new_v),
                format_number(mod_v),
                format_number(identical_v),
                format_number(unaffected_v),
                format_number(deleted_src_v),
                format_number(deleted_dest_v)
            );
        }
    }

    if planned_bytes_exact {
        println!(
            "Planned transfer bytes: {} ({})",
            format_number(planned_bytes),
            format_bytes_binary(planned_bytes, 2)
        );
    } else {
        println!(
            "Planned transfer bytes: {} ({}) [preview-lite: exact byte scan skipped]",
            format_number(planned_bytes),
            format_bytes_binary(planned_bytes, 2)
        );
    }
    println!();

    if preview_only {
        return 0;
    }

    let no_changes_planned = source_already_in_destination
        || ((planned_bytes == 0 && !has_itemized_changes && !overwrite_requires_action) && !move_cleanup_only);

    if overwrite_requires_action && planned_bytes == 0 && !has_itemized_changes {
        log(requested_mode, "Overwrite requested: destination conflict will be replaced.", LogLevel::Info);
    }
    if no_changes_planned {
        log(
            requested_mode,
            &format!("No changes detected; nothing to {}.", requested_mode.word()),
            LogLevel::Info,
        );
        return 0;
    }
    if move_cleanup_only {
        log(
            requested_mode,
            "Destination already has matching files; source files will be removed to complete move.",
            LogLevel::Info,
        );
    }

    print!("Proceed with {}? [y/N]: ", requested_mode.word());
    let _ = io::stdout().flush();
    let mut ans = String::new();
    let _ = io::stdin().read_line(&mut ans);
    let ans = ans.trim().to_ascii_lowercase();
    if ans != "y" && ans != "yes" {
        println!("{FAIL}Cancelled.{ENDC}");
        return 0;
    }

    if source_already_in_destination {
        log(requested_mode, "No changes: source is already in destination directory.", LogLevel::Info);
        return 0;
    }

    let maybe_fast_rename_target = if is_move
        && !use_sudo
        && !backup_requested
        && !overwrite_requires_action
        && !move_cleanup_only
        && !source_contents_mode
        && !contents_mode_requested
        && !merge_child_into_parent
        && !overwrite_parent_from_child
        && !overwrite_rename_dir_target
        && !overwrite_replace_file_target
    {
        match src_obj_kind {
            SrcObjKind::File => {
                if matches!(dst_obj_kind, DstObjKind::Dir | DstObjKind::DirExisting) {
                    src_mnt.file_name().map(|n| dst_mnt.join(n))
                } else {
                    Some(dst_mnt.clone())
                }
            }
            SrcObjKind::Dir => match dst_obj_kind {
                DstObjKind::Dir | DstObjKind::DirExisting => src_mnt.file_name().map(|n| dst_mnt.join(n)),
                DstObjKind::DirNew => Some(dst_mnt.clone()),
                _ => None,
            },
        }
    } else {
        None
    };

    let fast_rename_possible = maybe_fast_rename_target
        .as_ref()
        .map(|rename_target| {
            can_fast_rename_same_fs(&src_mnt, rename_target)
                && !rename_target.exists()
                && *rename_target != src_mnt
        })
        .unwrap_or(false);

    if planned_bytes > 0 && !fast_rename_possible {
        match destination_available_bytes(&dst_mnt) {
            Ok((available_bytes, probe_path)) => {
                if available_bytes < planned_bytes {
                    let msg = format!(
                        "Insufficient free space on destination filesystem (probe: {}). Need: {} ({}), available: {} ({}).",
                        probe_path.display(),
                        format_number(planned_bytes),
                        format_bytes_binary(planned_bytes, 2),
                        format_number(available_bytes),
                        format_bytes_binary(available_bytes, 2)
                    );
                    if overwrite_target_path.is_some() && !backup_requested && !overwrite_parent_from_child {
                        log(
                            requested_mode,
                            &format!(
                                "{} Continuing because overwrite will remove destination data before transfer.",
                                msg
                            ),
                            LogLevel::Warn,
                        );
                    } else {
                        log(requested_mode, &msg, LogLevel::Error);
                        return 1;
                    }
                }
            }
            Err(err) => {
                log(
                    requested_mode,
                    &format!("Could not determine destination free space: {err}"),
                    LogLevel::Warn,
                );
            }
        }
    }

    if let Some(rename_target) = maybe_fast_rename_target {
        if fast_rename_possible {
            if fs::rename(&src_mnt, &rename_target).is_ok() {
                log(
                    requested_mode,
                    &format!(
                        "Fast-path rename on same filesystem: {} -> {}",
                        src_mnt.display(),
                        rename_target.display()
                    ),
                    LogLevel::Info,
                );
                log(requested_mode, &format!("{} complete.", requested_mode.word_cap()), LogLevel::Info);
                return 0;
            }
        }
    }

    if use_sudo {
        let _ = Command::new("sudo").arg("-v").status();
    }

    prefer_hdd_scheduler_for_paths(&[&src_mnt, &dst_mnt], use_sudo, requested_mode);

    let backend_name = match backend {
        TransferBackend::Rust => "rust",
        TransferBackend::Rsync => "rsync",
    };

    if move_cleanup_only {
        log(
            requested_mode,
            &format!(
                "Starting {} cleanup: {} -> {}...",
                requested_mode.word(),
                source_input,
                destination
            ),
            LogLevel::Info,
        );
    } else {
        log(
            requested_mode,
            &format!(
                "Starting {} ({} backend): {} -> {}...",
                requested_mode.word(),
                backend_name,
                source_input,
                destination
            ),
            LogLevel::Info,
        );
    }

    let start_ts = Instant::now();
    let mut transferred_bytes_total: u64 = 0;
    let mut transferred_elapsed_total_s: f64 = 0.0;
    let mut transfer_read_bytes_total: u64 = 0;
    let mut transfer_write_bytes_total: u64 = 0;
    let mut transfer_read_elapsed_s: f64 = 0.0;
    let mut transfer_write_elapsed_s: f64 = 0.0;
    let mut deleted_cleanup_total = DeleteCleanupOutcome::default();
    let mut deleted_cleanup_elapsed_s: f64 = 0.0;
    let mut cleanup_notice_emitted = false;

    let result: i32 = (|| {
        if backup_requested {
            if let Some(bsp) = &backup_source_path {
                if overwrite_target_path.is_none() {
                    if backup_source_kind == Some("file") {
                        log(requested_mode, &format!("Backing up existing file: {}", bsp.display()), LogLevel::Info);
                    } else {
                        log(requested_mode, &format!("Backing up existing directory: {}", bsp.display()), LogLevel::Info);
                    }
                    if let Some(pbp) = &planned_backup_path {
                        if copy_path_to_backup(bsp, pbp, use_sudo, requested_mode).is_none() {
                            return 1;
                        }
                        log(requested_mode, &format!("Backup saved as: {}", pbp.display()), LogLevel::Info);
                    }
                }
            }
        }

        if let Some(otp) = &overwrite_target_path {
            if overwrite_parent_from_child {
                let stage_parent = dst_mnt.parent().unwrap_or_else(|| Path::new("."));
                let stage_path = match tempfile::Builder::new()
                    .prefix(&format!(".{}-stage-", requested_mode.word()))
                    .tempdir_in(stage_parent)
                {
                    Ok(td) => td.keep(),
                    Err(_) => {
                        log(requested_mode, "Failed to create staging directory.", LogLevel::Error);
                        return 1;
                    }
                };

                log(requested_mode, &format!("Staging source before overwrite: {}", stage_path.display()), LogLevel::Info);

                let transfer = match backend {
                    TransferBackend::Rsync => {
                        run_rsync_transfer(&src_path, &stage_path.display().to_string(), planned_bytes, use_sudo, false)
                    }
                    TransferBackend::Rust => run_rust_transfer(
                        &src_path,
                        &stage_path.display().to_string(),
                        src_obj_kind,
                        is_move,
                        planned_bytes,
                        transfer_manifest.as_ref(),
                        media,
                    ),
                };
                transferred_bytes_total += transfer.bytes_done;
                transferred_elapsed_total_s += transfer.elapsed_s;
                if let Some(rb) = transfer.io_read_bytes {
                    transfer_read_bytes_total = transfer_read_bytes_total.saturating_add(rb);
                    transfer_read_elapsed_s += transfer.elapsed_s;
                }
                if let Some(wb) = transfer.io_write_bytes {
                    transfer_write_bytes_total = transfer_write_bytes_total.saturating_add(wb);
                    transfer_write_elapsed_s += transfer.elapsed_s;
                }
                let rc_transfer = transfer.rc;

                if rc_transfer == 0 || rc_transfer == 24 {
                    if is_move {
                        cleanup_source_dirs(&src_mnt, true, use_sudo, requested_mode);
                    }
                    if backup_requested {
                        log(requested_mode, &format!("Backing up existing directory: {}", otp.display()), LogLevel::Info);
                        let backup_base = planned_backup_path.clone().or_else(|| backup_base_path(otp));
                        if let Some(bb) = backup_base {
                            if backup_path_with_base(otp, use_sudo, &bb, requested_mode).is_none() {
                                let _ = remove_path_recursive(&stage_path, use_sudo, requested_mode);
                                return 1;
                            }
                        } else {
                            return 1;
                        }
                    } else {
                        log(requested_mode, &format!("Overwriting existing directory: {}", otp.display()), LogLevel::Info);
                        if !remove_path_recursive(otp, use_sudo, requested_mode) {
                            let _ = remove_path_recursive(&stage_path, use_sudo, requested_mode);
                            return 1;
                        }
                    }

                    if use_sudo {
                        let cmd = vec![
                            "mv".to_string(),
                            "--".to_string(),
                            stage_path.display().to_string(),
                            otp.display().to_string(),
                        ];
                        let mv_ok = run_command_capture(&cmd, true).map(|o| o.code == 0).unwrap_or(false);
                        if !mv_ok {
                            log(requested_mode, "Failed to place staged directory into destination.", LogLevel::Error);
                            let _ = remove_path_recursive(&stage_path, use_sudo, requested_mode);
                            return 1;
                        }
                    } else if fs::rename(&stage_path, otp).is_err() {
                        log(requested_mode, "Failed to place staged directory into destination.", LogLevel::Error);
                        let _ = remove_path_recursive(&stage_path, use_sudo, requested_mode);
                        return 1;
                    }

                    if rc_transfer == 0 {
                        log(requested_mode, &format!("{} complete.", requested_mode.word_cap()), LogLevel::Info);
                        return 0;
                    }
                    log(
                        requested_mode,
                        &format!("{} failed: some source files vanished during transfer (rsync exit 24).", requested_mode.word_cap()),
                        LogLevel::Error,
                    );
                    log(
                        requested_mode,
                        &format!("Re-run {} to converge once the source tree is stable.", requested_mode.word()),
                        LogLevel::Error,
                    );
                    return 1;
                }

                log(
                    requested_mode,
                    &format!("{} failed: rsync exited with status {}.", requested_mode.word_cap(), rc_transfer),
                    LogLevel::Error,
                );
                let _ = remove_path_recursive(&stage_path, use_sudo, requested_mode);
                return 1;
            }

            if backup_requested {
                if overwrite_target_kind == Some("file") {
                    log(requested_mode, &format!("Backing up existing file: {}", otp.display()), LogLevel::Info);
                } else {
                    log(requested_mode, &format!("Backing up existing directory: {}", otp.display()), LogLevel::Info);
                }
                let backup_base = planned_backup_path.clone().or_else(|| backup_base_path(otp));
                if let Some(bb) = backup_base {
                    if let Some(bp) = backup_path_with_base(otp, use_sudo, &bb, requested_mode) {
                        log(requested_mode, &format!("Backup saved as: {}", bp.display()), LogLevel::Info);
                    } else {
                        return 1;
                    }
                } else {
                    return 1;
                }
            } else {
                if overwrite_target_kind == Some("file") {
                    log(requested_mode, &format!("Overwriting existing file: {}", otp.display()), LogLevel::Info);
                } else {
                    log(requested_mode, &format!("Overwriting existing directory: {}", otp.display()), LogLevel::Info);
                }
                if !remove_path_recursive(otp, use_sudo, requested_mode) {
                    return 1;
                }
            }
        }

        if move_cleanup_only {
            if !cleanup_notice_emitted {
                if likely_cleanup_files > 0 {
                    if likely_cleanup_bytes > 0 {
                        log(
                            requested_mode,
                            &format!(
                                "Finalizing move: deleting source files from prescan: {} files ({}).",
                                format_number(likely_cleanup_files),
                                format_bytes_binary(likely_cleanup_bytes, 2)
                            ),
                            LogLevel::Info,
                        );
                    } else {
                        log(
                            requested_mode,
                            &format!(
                                "Finalizing move: deleting source files from prescan: {} files.",
                                format_number(likely_cleanup_files)
                            ),
                            LogLevel::Info,
                        );
                    }
                }
                cleanup_notice_emitted = true;
            }
            let delete_start = Instant::now();
            let deleted_now = prune_move_source_duplicates(
                &src_path,
                &dst_path,
                src_obj_kind,
                contents_mode_requested && src_obj_kind == SrcObjKind::Dir,
                use_sudo,
                requested_mode,
                transfer_manifest.as_ref(),
                likely_cleanup_files,
                likely_cleanup_bytes,
            );
            if src_obj_kind == SrcObjKind::Dir {
                let remove_root = (!source_contents_mode) || rename_dir_to_new_path;
                cleanup_source_dirs(&src_mnt, remove_root, use_sudo, requested_mode);
            }
            if deleted_now.files > 0 {
                deleted_cleanup_total.files += deleted_now.files;
                deleted_cleanup_total.bytes += deleted_now.bytes;
                deleted_cleanup_elapsed_s += delete_start.elapsed().as_secs_f64();
            }
            log(requested_mode, &format!("{} complete.", requested_mode.word_cap()), LogLevel::Info);
            return 0;
        }

        let transfer = match backend {
            TransferBackend::Rsync => run_rsync_transfer(&src_path, &dst_path, planned_bytes, use_sudo, false),
            TransferBackend::Rust => run_rust_transfer(
                &src_path,
                &dst_path,
                src_obj_kind,
                is_move,
                planned_bytes,
                transfer_manifest.as_ref(),
                media,
            ),
        };
        transferred_bytes_total += transfer.bytes_done;
        transferred_elapsed_total_s += transfer.elapsed_s;
        if let Some(rb) = transfer.io_read_bytes {
            transfer_read_bytes_total = transfer_read_bytes_total.saturating_add(rb);
            transfer_read_elapsed_s += transfer.elapsed_s;
        }
        if let Some(wb) = transfer.io_write_bytes {
            transfer_write_bytes_total = transfer_write_bytes_total.saturating_add(wb);
            transfer_write_elapsed_s += transfer.elapsed_s;
        }
        let rc_transfer = transfer.rc;

        if is_move && (rc_transfer == 0 || rc_transfer == 24) {
            if !cleanup_notice_emitted {
                if likely_cleanup_files > 0 {
                    if likely_cleanup_bytes > 0 {
                        log(
                            requested_mode,
                            &format!(
                                "Finalizing move: deleting source files from prescan: {} files ({}).",
                                format_number(likely_cleanup_files),
                                format_bytes_binary(likely_cleanup_bytes, 2)
                            ),
                            LogLevel::Info,
                        );
                    } else {
                        log(
                            requested_mode,
                            &format!(
                                "Finalizing move: deleting source files from prescan: {} files.",
                                format_number(likely_cleanup_files)
                            ),
                            LogLevel::Info,
                        );
                    }
                }
                cleanup_notice_emitted = true;
            }
            let delete_start = Instant::now();
            let deleted_now = prune_move_source_duplicates(
                &src_path,
                &dst_path,
                src_obj_kind,
                contents_mode_requested && src_obj_kind == SrcObjKind::Dir,
                use_sudo,
                requested_mode,
                transfer_manifest.as_ref(),
                likely_cleanup_files,
                likely_cleanup_bytes,
            );
            if src_obj_kind == SrcObjKind::Dir {
                let remove_root = (!source_contents_mode) || rename_dir_to_new_path;
                cleanup_source_dirs(&src_mnt, remove_root, use_sudo, requested_mode);
            }
            if deleted_now.files > 0 {
                deleted_cleanup_total.files += deleted_now.files;
                deleted_cleanup_total.bytes += deleted_now.bytes;
                deleted_cleanup_elapsed_s += delete_start.elapsed().as_secs_f64();
            }
        }

        if rc_transfer == 0 {
            log(requested_mode, &format!("{} complete.", requested_mode.word_cap()), LogLevel::Info);
            return 0;
        }
        if rc_transfer == 24 {
            log(
                requested_mode,
                &format!("{} failed: some source files vanished during transfer (rsync exit 24).", requested_mode.word_cap()),
                LogLevel::Error,
            );
            log(
                requested_mode,
                &format!("Re-run {} to converge once the source tree is stable.", requested_mode.word()),
                LogLevel::Error,
            );
            return 1;
        }

        log(
            requested_mode,
            &format!("{} failed: transfer exited with status {}.", requested_mode.word_cap(), rc_transfer),
            LogLevel::Error,
        );
        1
    })();

    let total_elapsed_s = start_ts.elapsed().as_secs_f64();
    let avg_transfer_bps = if transferred_elapsed_total_s > 0.0 {
        transferred_bytes_total as f64 / transferred_elapsed_total_s
    } else {
        0.0
    };
    let avg_read_bps = if transfer_read_elapsed_s > 0.0 {
        transfer_read_bytes_total as f64 / transfer_read_elapsed_s
    } else {
        0.0
    };
    let avg_write_bps = if transfer_write_elapsed_s > 0.0 {
        transfer_write_bytes_total as f64 / transfer_write_elapsed_s
    } else {
        0.0
    };
    let total_work_bytes = transferred_bytes_total.saturating_add(deleted_cleanup_total.bytes);
    let avg_total_bps = if total_elapsed_s > 0.0 {
        total_work_bytes as f64 / total_elapsed_s
    } else {
        0.0
    };
    if is_move {
        let avg_delete_bps = if deleted_cleanup_elapsed_s > 0.0 {
            deleted_cleanup_total.bytes as f64 / deleted_cleanup_elapsed_s.max(1e-6)
        } else {
            0.0
        };
        print_move_speed_summary(
            avg_transfer_bps,
            avg_read_bps,
            avg_write_bps,
            transferred_elapsed_total_s,
            avg_delete_bps,
            deleted_cleanup_elapsed_s,
            total_elapsed_s,
        );
    } else {
        if transferred_elapsed_total_s > 0.0 {
            print_summary_rate_line(
                "Average transfer speed",
                avg_transfer_bps,
                transferred_elapsed_total_s,
                false,
            );
        }
        print_summary_rate_line("Overall throughput", avg_total_bps, total_elapsed_s, true);
    }
    result
}

fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    let chars: Vec<char> = s.chars().collect();
    for (i, ch) in chars.iter().enumerate() {
        out.push(*ch);
        let rem = chars.len() - i - 1;
        if rem > 0 && rem % 3 == 0 {
            out.push(',');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_remote_spec_accepts_user_host_path() {
        let spec = parse_remote_spec("alice@nas:/data/photos").expect("remote spec");
        assert_eq!(spec.user.as_deref(), Some("alice"));
        assert_eq!(spec.host, "nas");
        assert_eq!(spec.path, "/data/photos");
    }

    #[test]
    fn parse_remote_spec_accepts_host_path_without_user() {
        let spec = parse_remote_spec("backup:/srv/archive").expect("remote spec");
        assert_eq!(spec.user, None);
        assert_eq!(spec.host, "backup");
        assert_eq!(spec.path, "/srv/archive");
    }

    #[test]
    fn parse_remote_spec_rejects_local_path_with_colon() {
        assert!(parse_remote_spec("/tmp/a:b").is_none());
        assert!(parse_remote_spec("mtp://phone/path").is_none());
    }

    #[test]
    fn ssh_config_parser_uses_first_matching_user() {
        let cfg = r#"
Host box
  User first
Host box
  User second
"#;
        assert_eq!(
            ssh_config_user_for_host_from_text("box", cfg).as_deref(),
            Some("first")
        );
    }

    #[test]
    fn ssh_config_parser_supports_wildcards_and_negation() {
        let cfg = r#"
Host * !blocked
  User wildcard
Host blocked
  User denied
Host dev-*
  User devuser
"#;
        assert_eq!(
            ssh_config_user_for_host_from_text("prod-1", cfg).as_deref(),
            Some("wildcard")
        );
        assert_eq!(
            ssh_config_user_for_host_from_text("dev-a", cfg).as_deref(),
            Some("wildcard")
        );
        assert_eq!(
            ssh_config_user_for_host_from_text("blocked", cfg).as_deref(),
            Some("denied")
        );
    }

    #[test]
    fn handle_rsync_stream_line_emits_progress_event() {
        let (tx, rx) = mpsc::channel();
        handle_rsync_stream_line(&tx, "   1,024  10%   1.00MB/s    0:00:00");
        match rx.recv().expect("event") {
            RsyncStreamEvent::Progress(bytes) => assert_eq!(bytes, 1024),
            RsyncStreamEvent::Text(line) => panic!("expected progress, got text: {line}"),
        }
    }

    #[test]
    fn handle_rsync_stream_line_emits_text_event() {
        let (tx, rx) = mpsc::channel();
        handle_rsync_stream_line(&tx, "building file list ...");
        match rx.recv().expect("event") {
            RsyncStreamEvent::Text(line) => assert_eq!(line, "building file list ..."),
            RsyncStreamEvent::Progress(bytes) => panic!("expected text, got progress: {bytes}"),
        }
    }
}
