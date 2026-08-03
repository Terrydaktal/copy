//! Shared domain types passed between planning, transfer, telemetry, and UI.

use super::*;

pub(super) enum LogLevel {
    Info,
    Warn,
    Error,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TransferMode {
    Copy,
    Move,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CollisionWinner {
    Source,
    Dest,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CollisionCombineMode {
    Any,
    All,
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct CollisionPredicates {
    pub(super) always: bool,
    pub(super) newer: bool,
    pub(super) larger: bool,
    pub(super) size_differs: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct MergeCollisionPolicy {
    pub(super) winner: CollisionWinner,
    pub(super) combine: CollisionCombineMode,
    pub(super) predicates: CollisionPredicates,
}

impl Default for MergeCollisionPolicy {
    fn default() -> Self {
        Self {
            winner: CollisionWinner::Source,
            combine: CollisionCombineMode::Any,
            predicates: CollisionPredicates {
                size_differs: true,
                ..CollisionPredicates::default()
            },
        }
    }
}

impl MergeCollisionPolicy {
    pub(super) fn requires_mtime(self) -> bool {
        self.predicates.newer
    }
}

impl TransferMode {
    pub(super) fn word(self) -> &'static str {
        match self {
            TransferMode::Copy => "copy",
            TransferMode::Move => "move",
        }
    }

    pub(super) fn word_cap(self) -> &'static str {
        match self {
            TransferMode::Copy => "Copy",
            TransferMode::Move => "Move",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SrcObjKind {
    File,
    Dir,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum DstObjKind {
    Dir,
    DirExisting,
    DirNew,
    File,
    FileExistingForDir,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MediaKind {
    Nvme,
    Hdd,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TransferBackend {
    Rust,
    Rsync,
}

#[derive(Clone)]
pub(super) struct RemoteSpec {
    pub(super) user: Option<String>,
    pub(super) host: String,
    pub(super) path: String,
}

#[derive(Clone)]
pub(super) enum Endpoint {
    Local(PathBuf),
    Remote(RemoteSpec),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ChangeKind {
    NewFile,
    ModFile,
    NewDir,
    RemovedDir,
}

#[derive(Clone)]
pub(super) struct ChangeItem {
    pub(super) kind: ChangeKind,
    pub(super) rel: String,
}

#[derive(Default, Clone)]
pub(super) struct ManifestFileEntry {
    pub(super) rel: Arc<str>,
    pub(super) size: u64,
    pub(super) dev: u64,
    pub(super) ino: u64,
    #[allow(dead_code)]
    pub(super) nlink: u64,
    pub(super) is_symlink: bool,
}

#[derive(Clone)]
pub(super) struct ManifestDirTimeEntry {
    pub(super) rel: String,
    pub(super) atime: FileTime,
    pub(super) mtime: FileTime,
}

#[derive(Default, Clone)]
pub(super) struct TransferManifest {
    pub(super) dirs: Vec<String>,
    pub(super) dir_times: Vec<ManifestDirTimeEntry>,
    pub(super) copy_files: Vec<ManifestFileEntry>,
    pub(super) identical_files: Vec<ManifestFileEntry>,
}

pub(super) struct PreScan {
    pub(super) planned_bytes: u64,
    pub(super) planned_bytes_exact: bool,
    pub(super) total_regular_files: Option<u64>,
    pub(super) total_regular_bytes: Option<u64>,
    pub(super) total_dirs: Option<u64>,
    pub(super) add_files: u64,
    pub(super) mod_files: u64,
    pub(super) uncollided_files: u64,
    pub(super) add_dirs: u64,
    pub(super) mod_dirs: u64,
    pub(super) uncollided_dirs: u64,
    pub(super) change_preview: Vec<ChangeItem>,
    pub(super) source_display_paths: FxHashSet<String>,
    pub(super) has_itemized_changes: bool,
    pub(super) transfer_manifest: Option<TransferManifest>,
    pub(super) file_relation_breakdown: FileRelationBreakdown,
}

#[derive(Default, Clone, Copy)]
pub(super) struct FileRelationBreakdown {
    pub(super) same_time_same_size: u64,
    pub(super) same_time_source_larger: u64,
    pub(super) same_time_source_smaller: u64,
    pub(super) same_size_source_newer: u64,
    pub(super) same_size_source_older: u64,
    pub(super) source_older_smaller: u64,
    pub(super) source_older_larger: u64,
    pub(super) source_newer_smaller: u64,
    pub(super) source_newer_larger: u64,
}

impl FileRelationBreakdown {
    pub(super) fn add_assign(&mut self, other: Self) {
        self.same_time_same_size = self
            .same_time_same_size
            .saturating_add(other.same_time_same_size);
        self.same_time_source_larger = self
            .same_time_source_larger
            .saturating_add(other.same_time_source_larger);
        self.same_time_source_smaller = self
            .same_time_source_smaller
            .saturating_add(other.same_time_source_smaller);
        self.same_size_source_newer = self
            .same_size_source_newer
            .saturating_add(other.same_size_source_newer);
        self.same_size_source_older = self
            .same_size_source_older
            .saturating_add(other.same_size_source_older);
        self.source_older_smaller = self
            .source_older_smaller
            .saturating_add(other.source_older_smaller);
        self.source_older_larger = self
            .source_older_larger
            .saturating_add(other.source_older_larger);
        self.source_newer_smaller = self
            .source_newer_smaller
            .saturating_add(other.source_newer_smaller);
        self.source_newer_larger = self
            .source_newer_larger
            .saturating_add(other.source_newer_larger);
    }
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
            uncollided_files: 0,
            add_dirs: 0,
            mod_dirs: 0,
            uncollided_dirs: 0,
            change_preview: Vec::new(),
            source_display_paths: FxHashSet::default(),
            has_itemized_changes: false,
            transfer_manifest: None,
            file_relation_breakdown: FileRelationBreakdown::default(),
        }
    }
}

pub(super) struct CmdOutput {
    pub(super) code: i32,
}

#[derive(Clone, Copy)]
pub(super) struct TransferOutcome {
    pub(super) rc: i32,
    pub(super) bytes_done: u64,
    pub(super) elapsed_s: f64,
    pub(super) progress_snapshot: Option<ProgressSnapshot>,
}

#[derive(Clone, Copy, Default)]
pub(super) struct TransferProgressRates {
    pub(super) write_all_bps: Option<f64>,
    pub(super) rchar_bps: Option<f64>,
    pub(super) wchar_bps: Option<f64>,
    pub(super) read_bytes_bps: Option<f64>,
    pub(super) write_bytes_bps: Option<f64>,
    pub(super) read_complete_bps: Option<f64>,
    pub(super) write_complete_bps: Option<f64>,
}

#[derive(Default)]
pub(super) struct TransferRateSmoother {
    pub(super) rates: TransferProgressRates,
}

impl TransferRateSmoother {
    pub(super) fn update(
        &mut self,
        sample: TransferProgressRates,
        dt: f64,
    ) -> TransferProgressRates {
        let alpha = (1.0 - (-dt.max(0.0) / 1.25).exp()).clamp(0.05, 1.0);
        fn blend(previous: Option<f64>, current: Option<f64>, alpha: f64) -> Option<f64> {
            match (previous, current) {
                (Some(previous), Some(current)) => Some(previous + alpha * (current - previous)),
                (None, current) => current,
                (previous, None) => previous,
            }
        }
        self.rates.write_all_bps = blend(self.rates.write_all_bps, sample.write_all_bps, alpha);
        self.rates.rchar_bps = blend(self.rates.rchar_bps, sample.rchar_bps, alpha);
        self.rates.wchar_bps = blend(self.rates.wchar_bps, sample.wchar_bps, alpha);
        self.rates.read_bytes_bps = blend(self.rates.read_bytes_bps, sample.read_bytes_bps, alpha);
        self.rates.write_bytes_bps =
            blend(self.rates.write_bytes_bps, sample.write_bytes_bps, alpha);
        self.rates.read_complete_bps = blend(
            self.rates.read_complete_bps,
            sample.read_complete_bps,
            alpha,
        );
        self.rates.write_complete_bps = blend(
            self.rates.write_complete_bps,
            sample.write_complete_bps,
            alpha,
        );
        self.rates
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct ProcIoCounters {
    pub(super) rchar: u64,
    pub(super) wchar: u64,
    pub(super) read_bytes: u64,
    pub(super) write_bytes: u64,
}

#[derive(Clone, Copy, Default)]
pub(super) struct ProcIoDeltas {
    pub(super) rchar: Option<u64>,
    pub(super) wchar: Option<u64>,
    pub(super) read_bytes: Option<u64>,
    pub(super) write_bytes: Option<u64>,
}

#[derive(Clone, Copy, Default)]
pub(super) struct DeviceIoDeltas {
    pub(super) read_complete: Option<u64>,
    pub(super) write_complete: Option<u64>,
}

#[derive(Clone, Copy, Default)]
pub(super) struct ProgressSnapshot {
    pub(super) elapsed_s: f64,
    pub(super) planned_bytes: u64,
    pub(super) write_all_total: Option<u64>,
    pub(super) phase_label: &'static str,
    pub(super) rates: TransferProgressRates,
    pub(super) proc_deltas: ProcIoDeltas,
    pub(super) device_deltas: DeviceIoDeltas,
    pub(super) eta_workload: Option<EtaWorkload>,
    pub(super) eta_progress: Option<EtaProgressTotals>,
}

#[derive(Default)]
pub(super) struct DeviceIoWindow {
    pub(super) src_keys: Vec<(u64, u64)>,
    pub(super) dst_keys: Vec<(u64, u64)>,
}

#[derive(Default)]
pub(super) struct ProcessIoWindow {
    pub(super) pid: u32,
    pub(super) last_at: Option<Instant>,
    pub(super) last_counters: Option<ProcIoCounters>,
}

pub(super) struct InflightWriteLimiter {
    pub(super) max_bytes: u64,
    pub(super) used_bytes: Mutex<u64>,
    pub(super) cv: Condvar,
}

pub(super) struct InflightWritePermit {
    pub(super) limiter: Arc<InflightWriteLimiter>,
    pub(super) reserved: u64,
}

impl Drop for InflightWritePermit {
    fn drop(&mut self) {
        self.limiter.release(self.reserved);
    }
}

impl InflightWriteLimiter {
    pub(super) fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes: max_bytes.max(1),
            used_bytes: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    pub(super) fn acquire(self: &Arc<Self>, want_bytes: u64) -> InflightWritePermit {
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

    pub(super) fn release(&self, bytes: u64) {
        let mut used = self.used_bytes.lock().unwrap_or_else(|e| e.into_inner());
        *used = used.saturating_sub(bytes);
        self.cv.notify_all();
    }
}

pub(super) enum RsyncStreamEvent {
    Progress(u64),
    Text(String),
}

#[derive(Clone, Copy, Default)]
pub(super) struct DeleteCleanupOutcome {
    pub(super) files: u64,
    pub(super) bytes: u64,
}
pub(super) const ETA_FILE_BIN_COUNT: usize = 8;
pub(super) const ETA_MAX_REGIME_HYPOTHESES: usize = 64;
pub(super) const ETA_BUCKET_S: f64 = 1.0;
pub(super) const ETA_SAMPLE_RETENTION_S: f64 = 90.0;

pub(super) type EtaFileCounts = [u64; ETA_FILE_BIN_COUNT];
pub(super) type EtaFileBytes = [u64; ETA_FILE_BIN_COUNT];

pub(super) fn eta_file_bin(size: u64) -> usize {
    if size <= 4 * 1024 {
        0
    } else if size <= 16 * 1024 {
        1
    } else if size <= 64 * 1024 {
        2
    } else if size <= 256 * 1024 {
        3
    } else if size <= 1024 * 1024 {
        4
    } else if size <= 4 * 1024 * 1024 {
        5
    } else if size <= 64 * 1024 * 1024 {
        6
    } else {
        7
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct EtaWorkload {
    pub(super) file_bins: EtaFileCounts,
    pub(super) file_bytes: EtaFileBytes,
    pub(super) dirs: u64,
}

impl EtaWorkload {
    pub(super) fn from_manifest(manifest: &TransferManifest, include_root: bool) -> Self {
        let mut workload = Self {
            file_bins: [0; ETA_FILE_BIN_COUNT],
            file_bytes: [0; ETA_FILE_BIN_COUNT],
            dirs: manifest.dirs.len() as u64 + u64::from(include_root),
        };
        for entry in &manifest.copy_files {
            let bin = eta_file_bin(entry.size);
            workload.file_bins[bin] = workload.file_bins[bin].saturating_add(1);
            workload.file_bytes[bin] = workload.file_bytes[bin].saturating_add(entry.size);
        }
        workload
    }

    pub(super) fn from_file(size: u64) -> Self {
        let mut workload = Self {
            file_bins: [0; ETA_FILE_BIN_COUNT],
            file_bytes: [0; ETA_FILE_BIN_COUNT],
            dirs: 0,
        };
        let bin = eta_file_bin(size);
        workload.file_bins[bin] = 1;
        workload.file_bytes[bin] = size;
        workload
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct EtaProgressTotals {
    pub(super) file_bins: EtaFileCounts,
    pub(super) file_bytes: EtaFileBytes,
    pub(super) dirs: u64,
}

impl EtaProgressTotals {
    pub(super) fn file_count(self) -> u64 {
        self.file_bins.iter().copied().sum()
    }

    pub(super) fn delta(self, previous: Self) -> Self {
        Self {
            file_bins: std::array::from_fn(|idx| {
                self.file_bins[idx].saturating_sub(previous.file_bins[idx])
            }),
            file_bytes: std::array::from_fn(|idx| {
                self.file_bytes[idx].saturating_sub(previous.file_bytes[idx])
            }),
            dirs: self.dirs.saturating_sub(previous.dirs),
        }
    }
}

pub(super) struct AtomicEtaProgress {
    pub(super) file_bins: [AtomicU64; ETA_FILE_BIN_COUNT],
    pub(super) file_bytes: [AtomicU64; ETA_FILE_BIN_COUNT],
    pub(super) dirs: AtomicU64,
}

impl Default for AtomicEtaProgress {
    fn default() -> Self {
        Self {
            file_bins: std::array::from_fn(|_| AtomicU64::new(0)),
            file_bytes: std::array::from_fn(|_| AtomicU64::new(0)),
            dirs: AtomicU64::new(0),
        }
    }
}

impl AtomicEtaProgress {
    pub(super) fn mark_file(&self, size: u64) {
        let bin = eta_file_bin(size);
        self.file_bins[bin].fetch_add(1, Ordering::Relaxed);
        self.file_bytes[bin].fetch_add(size, Ordering::Relaxed);
    }

    pub(super) fn mark_dir(&self) {
        self.dirs.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> EtaProgressTotals {
        EtaProgressTotals {
            file_bins: std::array::from_fn(|idx| self.file_bins[idx].load(Ordering::Relaxed)),
            file_bytes: std::array::from_fn(|idx| self.file_bytes[idx].load(Ordering::Relaxed)),
            dirs: self.dirs.load(Ordering::Relaxed),
        }
    }
}
