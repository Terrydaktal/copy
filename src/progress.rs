//! Progress rendering, rate formatting, and regime-aware ETA estimation.
//!
//! This module owns the presentation state and the statistical model used by
//! local and rsync-backed transfers. It does not perform filesystem I/O.

use super::*;

pub(super) fn fmt_hms_ms(total_seconds: f64) -> String {
    let ms_total = (total_seconds.max(0.0) * 1000.0).round() as i64;
    let h = ms_total / 3_600_000;
    let m = (ms_total % 3_600_000) / 60_000;
    let s = (ms_total % 60_000) / 1000;
    let ms = ms_total % 1000;
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}

pub(super) fn fmt_hms_tenths(total_seconds: f64) -> String {
    let tenth_total = (total_seconds.max(0.0) * 10.0).round() as i64;
    let h = tenth_total / 36_000;
    let m = (tenth_total % 36_000) / 600;
    let s = (tenth_total % 600) / 10;
    let t = tenth_total % 10;
    format!("{h:02}:{m:02}:{s:02}.{t}")
}

pub(super) fn format_bytes_binary(byte_value: u64, decimals: usize) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut idx = 0usize;
    let mut value = byte_value as f64;
    while idx < units.len() - 1 && value >= 1024.0 {
        value /= 1024.0;
        idx += 1;
    }
    while idx < units.len() - 1
        && (value * 10f64.powi(decimals as i32)).round() / 10f64.powi(decimals as i32) >= 1024.0
    {
        value /= 1024.0;
        idx += 1;
    }
    if units[idx] == "B" {
        format!("{} B", value as u64)
    } else {
        format!("{value:.decimals$} {}", units[idx])
    }
}

pub(super) fn fmt_speed_bps(bps: f64) -> String {
    let b = if bps.is_finite() && bps > 0.0 {
        bps as u64
    } else {
        0
    };
    format_bytes_binary(b, 2)
}

pub(super) fn print_transfer_columns_header() {
    reset_progress_render_state();
    // Intentionally empty: live progress block prints its own structure.
}

pub(super) fn terminal_columns() -> usize {
    static COLUMNS: OnceLock<usize> = OnceLock::new();
    *COLUMNS.get_or_init(query_terminal_columns)
}

pub(super) fn query_terminal_columns() -> usize {
    fn tty_columns_from_fd(fd: i32) -> Option<usize> {
        let mut ws = nix::libc::winsize {
            ws_row: 0,
            ws_col: 0,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let rc = unsafe { nix::libc::ioctl(fd, nix::libc::TIOCGWINSZ, &mut ws) };
        if rc == 0 && ws.ws_col >= 40 {
            Some(ws.ws_col as usize)
        } else {
            None
        }
    }

    tty_columns_from_fd(io::stdout().as_raw_fd())
        .or_else(|| tty_columns_from_fd(io::stderr().as_raw_fd()))
        .or_else(|| {
            env::var("COLUMNS")
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
                .filter(|v| *v >= 40)
        })
        .unwrap_or(120)
}

pub(super) fn stdout_is_tty() -> bool {
    unsafe { nix::libc::isatty(io::stdout().as_raw_fd()) == 1 }
}

pub(super) fn fmt_bytes_block_opt(byte_value: Option<u64>, decimals: usize) -> String {
    match byte_value {
        Some(v) => format_bytes_binary(v, decimals),
        None => "--".to_string(),
    }
}

pub(super) fn fmt_rate_block_opt(bps: Option<f64>) -> String {
    match bps {
        Some(v) if v.is_finite() && v >= 0.0 => {
            let mut value = v;
            let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
            let mut idx = 0usize;
            while value >= 1024.0 && idx + 1 < units.len() {
                value /= 1024.0;
                idx += 1;
            }
            let number = if value >= 100.0 {
                format!("{value:.1}")
            } else if value >= 10.0 {
                format!("{value:.2}")
            } else {
                format!("{value:.3}")
            };
            format!("{number} {}/s", units[idx])
        }
        _ => "--/s".to_string(),
    }
}

pub(super) fn build_progress_bar(pct: Option<f64>, width: usize) -> String {
    let w = width.max(8);
    match pct {
        Some(v) => {
            let clamped = v.clamp(0.0, 100.0);
            if clamped >= 100.0 {
                return "=".repeat(w);
            }
            let filled = ((clamped / 100.0) * (w as f64)).floor() as usize;
            if filled == 0 {
                format!(">{}", " ".repeat(w.saturating_sub(1)))
            } else if filled >= w {
                "=".repeat(w)
            } else {
                format!("{}>{}", "=".repeat(filled), " ".repeat(w - filled - 1))
            }
        }
        None => " ".repeat(w),
    }
}

pub(super) fn clamp_line_for_columns(mut line: String, terminal_width: usize) -> String {
    let max_cols = terminal_width.saturating_sub(1).max(40);
    if line.chars().count() > max_cols {
        line = line.chars().take(max_cols).collect();
    }
    line
}

#[derive(Clone, Copy, Default)]
pub(super) struct EtaTelemetry {
    pub(super) write_bytes: Option<u64>,
    pub(super) write_complete: Option<u64>,
    pub(super) read_bytes: Option<u64>,
    pub(super) read_complete: Option<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum EtaActivity {
    WarmingUp,
    Active,
    WorkloadBound,
    PipelineBound,
    TransientStall,
    Stalled,
    Complete,
}

impl EtaActivity {
    fn label(self) -> &'static str {
        match self {
            Self::WarmingUp => "warming-up",
            Self::Active => "active",
            Self::WorkloadBound => "workload-bound",
            Self::PipelineBound => "pipeline-bound",
            Self::TransientStall => "stalled briefly",
            Self::Stalled => "stalled",
            Self::Complete => "complete",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct EtaEstimate {
    pub(super) p10_s: Option<f64>,
    pub(super) p50_s: Option<f64>,
    pub(super) p90_s: Option<f64>,
    pub(super) confidence: f64,
    pub(super) activity: EtaActivity,
}

#[derive(Clone, Copy)]
struct EtaWorkSample {
    elapsed_s: f64,
    bytes: u64,
    progress: EtaProgressTotals,
}

#[derive(Clone, Copy)]
struct EtaRegimeHypothesis {
    run_length_s: f64,
    mean_log_bps: f64,
    m2_log_bps: f64,
    observations: f64,
    log_probability: f64,
}

impl EtaRegimeHypothesis {
    fn variance(self) -> f64 {
        if self.observations > 1.0 {
            (self.m2_log_bps / (self.observations - 1.0)).max(0.04 * 0.04)
        } else {
            0.40 * 0.40
        }
    }

    fn observe(self, observation: f64, dt_s: f64, log_probability: f64) -> Self {
        let observations = self.observations + 1.0;
        let delta = observation - self.mean_log_bps;
        let mean_log_bps = self.mean_log_bps + delta / observations;
        let m2_log_bps = self.m2_log_bps + delta * (observation - mean_log_bps);
        Self {
            run_length_s: self.run_length_s + dt_s,
            mean_log_bps,
            m2_log_bps,
            observations,
            log_probability,
        }
    }
}

#[derive(Default)]
struct EtaRegimeModel {
    hypotheses: Vec<EtaRegimeHypothesis>,
    observations: u64,
    observed_s: f64,
    zero_progress_s: f64,
}

pub(super) fn log_sum_exp(values: impl IntoIterator<Item = f64>) -> f64 {
    let values: Vec<f64> = values.into_iter().filter(|v| v.is_finite()).collect();
    let max = values.iter().copied().max_by(f64::total_cmp);
    let Some(max) = max else {
        return f64::NEG_INFINITY;
    };
    max + values
        .iter()
        .map(|value| (*value - max).exp())
        .sum::<f64>()
        .ln()
}

pub(super) fn student_t_log_likelihood(observation: f64, mean: f64, variance: f64) -> f64 {
    const NU: f64 = 4.0;
    let variance = variance.max(0.04 * 0.04);
    let residual = observation - mean;
    -0.5 * variance.ln() - 0.5 * (NU + 1.0) * (1.0 + residual * residual / (NU * variance)).ln()
}

impl EtaRegimeModel {
    fn observe(&mut self, rate_bps: f64, dt_s: f64) {
        if !rate_bps.is_finite() || rate_bps <= 0.0 || dt_s <= 0.0 {
            return;
        }
        self.observed_s += dt_s;
        let observation = rate_bps.max(1.0).ln();
        self.observations = self.observations.saturating_add(1);
        if self.hypotheses.is_empty() {
            self.hypotheses.push(EtaRegimeHypothesis {
                run_length_s: dt_s,
                mean_log_bps: observation,
                m2_log_bps: 0.0,
                observations: 1.0,
                log_probability: 0.0,
            });
            return;
        }

        let hazard = (1.0 - (-dt_s / 240.0).exp()).clamp(0.0001, 0.5);
        let mut growth = Vec::with_capacity(self.hypotheses.len());
        let mut change_terms = Vec::with_capacity(self.hypotheses.len());
        for hypothesis in self.hypotheses.iter().copied() {
            let likelihood = student_t_log_likelihood(
                observation,
                hypothesis.mean_log_bps,
                hypothesis.variance(),
            );
            growth.push(hypothesis.observe(
                observation,
                dt_s,
                hypothesis.log_probability + (1.0 - hazard).ln() + likelihood,
            ));
            change_terms.push(hypothesis.log_probability + hazard.ln() + likelihood);
        }

        let change_log_probability = log_sum_exp(change_terms);
        let mut next = Vec::with_capacity(growth.len() + 1);
        next.push(EtaRegimeHypothesis {
            run_length_s: dt_s,
            mean_log_bps: observation,
            m2_log_bps: 0.0,
            observations: 1.0,
            log_probability: change_log_probability,
        });
        next.extend(growth);
        let normalizer = log_sum_exp(next.iter().map(|hypothesis| hypothesis.log_probability));
        for hypothesis in &mut next {
            hypothesis.log_probability -= normalizer;
        }
        next.sort_by(|a, b| b.log_probability.total_cmp(&a.log_probability));
        next.truncate(ETA_MAX_REGIME_HYPOTHESES);
        self.hypotheses = next;
    }

    fn observe_zero(&mut self, dt_s: f64) {
        if dt_s > 0.0 {
            self.observed_s += dt_s;
            self.zero_progress_s += dt_s;
        }
    }

    fn current_rate_bps(&self) -> Option<f64> {
        self.hypotheses
            .first()
            .map(|hypothesis| hypothesis.mean_log_bps.exp())
            .filter(|rate| rate.is_finite() && *rate > 0.0)
    }

    fn recent_change_probability(&self) -> f64 {
        self.hypotheses
            .iter()
            .filter(|hypothesis| hypothesis.run_length_s <= 2.5)
            .map(|hypothesis| hypothesis.log_probability.exp())
            .sum::<f64>()
            .clamp(0.0, 1.0)
    }

    fn log_rate_sigma(&self) -> f64 {
        self.hypotheses
            .first()
            .map(|hypothesis| hypothesis.variance().sqrt())
            .unwrap_or(1.0)
            .clamp(0.12, 1.5)
    }

    fn zero_probability(&self) -> f64 {
        if self.observed_s <= 0.0 {
            0.0
        } else {
            (self.zero_progress_s / self.observed_s).clamp(0.0, 1.0)
        }
    }
}

#[derive(Clone, Copy)]
struct EtaCostModel {
    file_overhead_s: [f64; ETA_FILE_BIN_COUNT],
    file_weight: [f64; ETA_FILE_BIN_COUNT],
    dir_overhead_s: f64,
    dir_weight: f64,
}

impl Default for EtaCostModel {
    fn default() -> Self {
        Self {
            file_overhead_s: [0.0; ETA_FILE_BIN_COUNT],
            file_weight: [0.0; ETA_FILE_BIN_COUNT],
            dir_overhead_s: 0.0,
            dir_weight: 0.0,
        }
    }
}

impl EtaCostModel {
    fn observe(
        &mut self,
        dt_s: f64,
        bytes: u64,
        progress: EtaProgressTotals,
        capacity_bps: Option<f64>,
    ) {
        let files = progress.file_count();
        let objects = files.saturating_add(progress.dirs);
        if dt_s <= 0.0 || objects == 0 {
            return;
        }
        let byte_time_s = capacity_bps
            .filter(|rate| rate.is_finite() && *rate > 0.0)
            .map(|rate| bytes as f64 / rate)
            .unwrap_or(0.0);
        let residual_s = (dt_s - byte_time_s).max(0.0);
        let per_object_s = residual_s / objects as f64;
        if files > 0 {
            for idx in 0..ETA_FILE_BIN_COUNT {
                let count = progress.file_bins[idx];
                if count == 0 {
                    continue;
                }
                let weight = self.file_weight[idx];
                let alpha = (count as f64 / (weight + count as f64)).clamp(0.05, 0.5);
                self.file_overhead_s[idx] =
                    self.file_overhead_s[idx] * (1.0 - alpha) + per_object_s * alpha;
                self.file_weight[idx] += count as f64;
            }
        }
        if progress.dirs > 0 {
            let alpha =
                (progress.dirs as f64 / (self.dir_weight + progress.dirs as f64)).clamp(0.05, 0.5);
            self.dir_overhead_s = self.dir_overhead_s * (1.0 - alpha) + per_object_s * alpha;
            self.dir_weight += progress.dirs as f64;
        }
    }

    fn remaining_eta_s(&self, workload: EtaWorkload, progress: EtaProgressTotals) -> f64 {
        let mut eta = 0.0;
        for idx in 0..ETA_FILE_BIN_COUNT {
            let remaining = workload.file_bins[idx].saturating_sub(progress.file_bins[idx]);
            if self.file_weight[idx] >= 2.0 {
                eta += remaining as f64 * self.file_overhead_s[idx].max(0.0);
            }
        }
        if self.dir_weight >= 2.0 {
            eta +=
                workload.dirs.saturating_sub(progress.dirs) as f64 * self.dir_overhead_s.max(0.0);
        }
        eta
    }

    fn confidence(&self) -> f64 {
        let weight: f64 = self.file_weight.iter().sum::<f64>() + self.dir_weight;
        (1.0 - (-weight / 500.0).exp()).clamp(0.0, 1.0)
    }
}

#[derive(Default)]
pub(super) struct TransferEtaEstimator {
    samples: VecDeque<(f64, u64)>,
    work_samples: VecDeque<EtaWorkSample>,
    display_finish_s: Option<f64>,
    last_elapsed_s: Option<f64>,
    last_done: Option<u64>,
    last_model_sample: Option<EtaWorkSample>,
    regime_start_s: f64,
    change_since_s: Option<(f64, f64)>,
    zero_progress_s: f64,
    regime_model: EtaRegimeModel,
    cost_model: EtaCostModel,
    sequential_bps: Option<f64>,
    sequential_weight: f64,
    last_estimate: Option<EtaEstimate>,
}

pub(super) fn rate_over_window(
    samples: &VecDeque<(f64, u64)>,
    start_s: f64,
    now_s: f64,
    window_s: f64,
) -> Option<f64> {
    let cutoff_s = (now_s - window_s).max(start_s);
    let mut first: Option<(f64, u64)> = None;
    for &(t, bytes) in samples.iter().filter(|(t, _)| *t >= start_s) {
        if t <= cutoff_s {
            first = Some((t, bytes));
        } else if first.is_none() {
            first = Some((t, bytes));
            break;
        } else {
            break;
        }
    }
    let last = samples.iter().rev().find(|(t, _)| *t >= start_s).copied()?;
    let first = first?;
    let dt = last.0 - first.0;
    if dt <= 1e-6 {
        return None;
    }
    Some(last.1.saturating_sub(first.1) as f64 / dt)
}

pub(super) fn median_rate(rates: impl IntoIterator<Item = Option<f64>>) -> Option<f64> {
    let mut values: Vec<f64> = rates
        .into_iter()
        .flatten()
        .filter(|rate| rate.is_finite() && *rate > 0.0)
        .collect();
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    Some(values[values.len() / 2])
}

impl TransferEtaEstimator {
    #[cfg(test)]
    pub(super) fn update(
        &mut self,
        done: Option<u64>,
        total: u64,
        instant_bps: Option<f64>,
        elapsed_s: f64,
        finalize_line: bool,
        workload: Option<EtaWorkload>,
        progress: Option<EtaProgressTotals>,
    ) -> Option<f64> {
        self.update_with_telemetry(
            done,
            total,
            instant_bps,
            elapsed_s,
            finalize_line,
            workload,
            progress,
            EtaTelemetry::default(),
            TransferProgressRates::default(),
        )
    }

    pub(super) fn update_with_telemetry(
        &mut self,
        done: Option<u64>,
        total: u64,
        instant_bps: Option<f64>,
        elapsed_s: f64,
        finalize_line: bool,
        workload: Option<EtaWorkload>,
        progress: Option<EtaProgressTotals>,
        telemetry: EtaTelemetry,
        rates: TransferProgressRates,
    ) -> Option<f64> {
        let done = done?;
        if total == 0 {
            return None;
        }
        let done_clamped = done.min(total);
        let remain = total.saturating_sub(done_clamped);
        if remain == 0 {
            self.last_estimate = Some(EtaEstimate {
                p10_s: Some(0.0),
                p50_s: Some(0.0),
                p90_s: Some(0.0),
                confidence: 1.0,
                activity: EtaActivity::Complete,
            });
            if finalize_line {
                *self = Self::default();
            }
            return Some(0.0);
        }

        if self
            .last_elapsed_s
            .map(|previous| elapsed_s < previous)
            .unwrap_or(false)
        {
            *self = Self::default();
        }
        let dt = self
            .last_elapsed_s
            .map(|previous| (elapsed_s - previous).max(0.0))
            .unwrap_or(0.0);
        if self.last_done == Some(done_clamped) {
            self.zero_progress_s += dt;
        } else {
            self.zero_progress_s = 0.0;
        }
        self.last_elapsed_s = Some(elapsed_s);
        self.last_done = Some(done_clamped);

        self.samples.push_back((elapsed_s, done_clamped));
        if let (Some(_), Some(progress)) = (workload, progress) {
            self.work_samples.push_back(EtaWorkSample {
                elapsed_s,
                bytes: done_clamped,
                progress,
            });
        }
        while self
            .samples
            .front()
            .map(|(t, _)| elapsed_s - *t > ETA_SAMPLE_RETENTION_S)
            .unwrap_or(false)
        {
            self.samples.pop_front();
        }
        while self
            .work_samples
            .front()
            .map(|sample| elapsed_s - sample.elapsed_s > ETA_SAMPLE_RETENTION_S)
            .unwrap_or(false)
        {
            self.work_samples.pop_front();
        }

        let current_work_sample = EtaWorkSample {
            elapsed_s,
            bytes: done_clamped,
            progress: progress.unwrap_or_default(),
        };
        if let Some(previous) = self.last_model_sample {
            let model_dt = current_work_sample.elapsed_s - previous.elapsed_s;
            if model_dt >= ETA_BUCKET_S {
                let delta_bytes = current_work_sample.bytes.saturating_sub(previous.bytes);
                let delta_progress = current_work_sample.progress.delta(previous.progress);
                if delta_bytes > 0 {
                    let rate = delta_bytes as f64 / model_dt;
                    self.regime_model.observe(rate, model_dt);
                    let files = delta_progress.file_count();
                    let average_file_size = if files > 0 { delta_bytes / files } else { 0 };
                    let sequential_sample = if workload.is_some() && progress.is_some() {
                        delta_bytes >= 4 * 1024 * 1024
                            && (files == 0 || average_file_size >= 1024 * 1024)
                    } else {
                        true
                    };
                    self.update_capacity(rate, sequential_sample, elapsed_s, model_dt);
                    if workload.is_some() && progress.is_some() {
                        self.cost_model.observe(
                            model_dt,
                            delta_bytes,
                            delta_progress,
                            self.sequential_bps.or(Some(rate)),
                        );
                    }
                } else {
                    self.regime_model.observe_zero(model_dt);
                    if workload.is_some()
                        && progress.is_some()
                        && (delta_progress.file_count() > 0 || delta_progress.dirs > 0)
                    {
                        self.cost_model
                            .observe(model_dt, 0, delta_progress, self.sequential_bps);
                    }
                }
                self.last_model_sample = Some(current_work_sample);
            }
        } else {
            self.last_model_sample = Some(current_work_sample);
        }

        if self.zero_progress_s > 0.0 {
            if let Some(finish) = &mut self.display_finish_s {
                *finish += dt;
                if self.zero_progress_s >= 5.0 {
                    self.last_estimate = Some(EtaEstimate {
                        p10_s: None,
                        p50_s: None,
                        p90_s: None,
                        confidence: 0.0,
                        activity: EtaActivity::Stalled,
                    });
                    return None;
                }
                let eta = (*finish - elapsed_s).max(0.0).round();
                self.last_estimate = Some(EtaEstimate {
                    p10_s: Some(eta),
                    p50_s: Some(eta),
                    p90_s: Some(eta),
                    confidence: 0.25,
                    activity: EtaActivity::TransientStall,
                });
                return Some(eta);
            }
        }

        const WARMUP_S: f64 = 5.0;
        let sample_span_s = match (self.samples.front(), self.samples.back()) {
            (Some((first_t, _)), Some((last_t, _))) => (last_t - first_t).max(0.0),
            _ => 0.0,
        };
        let recent_rate = median_rate([
            rate_over_window(&self.samples, self.regime_start_s, elapsed_s, 1.2),
            rate_over_window(&self.samples, self.regime_start_s, elapsed_s, 2.5),
            rate_over_window(&self.samples, self.regime_start_s, elapsed_s, 5.0),
        ])
        .or_else(|| instant_bps.filter(|rate| rate.is_finite() && *rate > 0.0));

        if let Some(rate) = recent_rate {
            if self.regime_model.hypotheses.is_empty() {
                self.regime_model
                    .observe(rate, sample_span_s.max(ETA_BUCKET_S));
            }
        }

        if self.zero_progress_s >= 5.0 {
            self.last_estimate = Some(EtaEstimate {
                p10_s: None,
                p50_s: None,
                p90_s: None,
                confidence: 0.0,
                activity: EtaActivity::Stalled,
            });
            return None;
        }
        if self.zero_progress_s >= 1.5 {
            if let Some(finish) = &mut self.display_finish_s {
                *finish += dt;
                let eta = (*finish - elapsed_s).max(0.0).round();
                self.last_estimate = Some(EtaEstimate {
                    p10_s: Some(eta),
                    p50_s: Some(eta),
                    p90_s: Some(eta),
                    confidence: 0.25,
                    activity: EtaActivity::TransientStall,
                });
                return Some(eta);
            }
            return None;
        }

        let current_rate = recent_rate
            .or_else(|| self.regime_model.current_rate_bps())
            .filter(|rate| rate.is_finite() && *rate > 0.0);
        let rate = match current_rate {
            Some(rate) => rate,
            _ if sample_span_s < WARMUP_S => {
                self.last_estimate = Some(EtaEstimate {
                    p10_s: None,
                    p50_s: None,
                    p90_s: None,
                    confidence: 0.0,
                    activity: EtaActivity::WarmingUp,
                });
                return None;
            }
            _ => return None,
        };

        let capacity_bps = self.sequential_bps.unwrap_or(rate).max(1.0);
        let byte_eta_s = remain as f64 / capacity_bps;
        let object_eta_s = workload
            .zip(progress)
            .map(|(workload, progress)| self.cost_model.remaining_eta_s(workload, progress))
            .unwrap_or(0.0);
        let workload_model_active = workload.is_some()
            && progress.is_some()
            && (self.cost_model.confidence() > 0.05 || object_eta_s > 0.0);
        let producer_eta_s = if workload_model_active {
            byte_eta_s + object_eta_s
        } else {
            (byte_eta_s + object_eta_s).max(remain as f64 / rate)
        };
        let pipeline_eta_s = self.pipeline_eta_s(telemetry, rates, elapsed_s);
        let model_eta_s = pipeline_eta_s
            .map(|pipeline| producer_eta_s.max(pipeline))
            .unwrap_or(producer_eta_s);
        let model_eta_s = if model_eta_s.is_finite() && model_eta_s > 0.0 {
            model_eta_s
        } else {
            return None;
        };
        let workload_confidence = workload
            .zip(progress)
            .map(|_| self.cost_model.confidence())
            .unwrap_or(0.0);
        let rate_uncertainty = self.regime_model.log_rate_sigma();
        let pipeline_confidence = if pipeline_eta_s.is_some() { 0.45 } else { 0.0 };
        let confidence = (((1.0 - (-(self.regime_model.observations as f64) / 20.0).exp()) * 0.65
            + workload_confidence * 0.25
            + pipeline_confidence * 0.10)
            * (1.0 - 0.5 * self.regime_model.zero_probability()))
        .clamp(0.0, 1.0);
        let uncertainty = (rate_uncertainty * (1.0 - 0.45 * confidence) + 0.08).clamp(0.12, 1.35);
        let p10_s = (model_eta_s / uncertainty.exp()).max(0.0);
        let p90_s = (model_eta_s * uncertainty.exp()).max(model_eta_s);
        let workload_bound = object_eta_s > byte_eta_s * 0.25;
        let pipeline_bound = pipeline_eta_s
            .map(|pipeline| pipeline > producer_eta_s * 1.25)
            .unwrap_or(false);
        let activity = if pipeline_bound {
            EtaActivity::PipelineBound
        } else if workload_bound {
            EtaActivity::WorkloadBound
        } else {
            EtaActivity::Active
        };
        let model_finish_s = elapsed_s + model_eta_s;
        let regime_changed = self.regime_model.recent_change_probability() > 0.65;
        if self.display_finish_s.is_none() || regime_changed {
            self.display_finish_s = Some(model_finish_s);
        } else if let Some(finish) = &mut self.display_finish_s {
            let smoothing_s = 2.0 + 4.0 * (1.0 - confidence);
            let alpha = if dt > 0.0 {
                1.0 - (-dt / smoothing_s).exp()
            } else {
                0.0
            };
            *finish += alpha * (model_finish_s - *finish);
        }

        let displayed_s = self
            .display_finish_s
            .map(|finish| (finish - elapsed_s).max(0.0).round());
        self.last_estimate = Some(EtaEstimate {
            p10_s: Some(p10_s),
            p50_s: displayed_s.map(|value| value as f64),
            p90_s: Some(p90_s),
            confidence,
            activity,
        });
        if finalize_line {
            let result = displayed_s;
            *self = Self::default();
            return result;
        }
        displayed_s
    }

    fn update_capacity(&mut self, rate: f64, sequential_sample: bool, elapsed_s: f64, dt_s: f64) {
        if !sequential_sample || !rate.is_finite() || rate <= 0.0 {
            return;
        }
        let previous = self.sequential_bps;
        let ratio = previous.map(|old| rate / old.max(1.0));
        let candidate_s = ratio.and_then(|ratio| {
            if ratio < 0.125 || ratio > 8.0 {
                Some(1.2)
            } else if ratio < 0.60 || ratio > 1.67 {
                Some(2.5)
            } else {
                None
            }
        });
        let mut regime_changed = false;
        if let Some(required_s) = candidate_s {
            if self.change_since_s.is_none() {
                self.change_since_s = Some(((elapsed_s - dt_s).max(0.0), required_s));
            }
            if self
                .change_since_s
                .map(|(started, required)| elapsed_s - started >= required)
                .unwrap_or(false)
            {
                self.regime_start_s = self
                    .change_since_s
                    .map(|(started, _)| started)
                    .unwrap_or((elapsed_s - dt_s).max(0.0));
                self.change_since_s = None;
                regime_changed = true;
            }
        } else {
            self.change_since_s = None;
        }

        let alpha = if regime_changed { 0.65 } else { 0.20 };
        self.sequential_bps = Some(match self.sequential_bps {
            Some(previous) => previous * (1.0 - alpha) + rate * alpha,
            None => rate,
        });
        self.sequential_weight += dt_s;
    }

    fn pipeline_eta_s(
        &self,
        telemetry: EtaTelemetry,
        rates: TransferProgressRates,
        elapsed_s: f64,
    ) -> Option<f64> {
        let write_backlog = match (telemetry.write_bytes, telemetry.write_complete) {
            (Some(submitted), Some(completed)) => submitted.saturating_sub(completed),
            _ => 0,
        };
        let read_backlog = match (telemetry.read_bytes, telemetry.read_complete) {
            (Some(submitted), Some(completed)) => submitted.saturating_sub(completed),
            _ => 0,
        };
        let write_rate = rates
            .write_complete_bps
            .filter(|rate| *rate > 0.0)
            .or_else(|| {
                if elapsed_s > 1.0 {
                    telemetry
                        .write_complete
                        .map(|completed| completed as f64 / elapsed_s)
                        .filter(|rate| *rate > 0.0)
                } else {
                    None
                }
            });
        let read_rate = rates
            .read_complete_bps
            .filter(|rate| *rate > 0.0)
            .or_else(|| {
                if elapsed_s > 1.0 {
                    telemetry
                        .read_complete
                        .map(|completed| completed as f64 / elapsed_s)
                        .filter(|rate| *rate > 0.0)
                } else {
                    None
                }
            });
        let write_eta = write_rate.map(|rate| write_backlog as f64 / rate);
        let read_eta = read_rate.map(|rate| read_backlog as f64 / rate);
        match (write_eta, read_eta) {
            (Some(write), Some(read)) => Some(write.max(read)),
            (Some(write), None) => Some(write),
            (None, Some(read)) => Some(read),
            (None, None) => None,
        }
    }

    pub(super) fn last_estimate(&self) -> Option<EtaEstimate> {
        self.last_estimate
    }
}

#[derive(Default)]

struct ProgressRenderState {
    active: bool,
    lines: usize,
    finalized_lines: usize,
    last_frame: String,
}

fn progress_render_state() -> &'static Mutex<ProgressRenderState> {
    static STATE: OnceLock<Mutex<ProgressRenderState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(ProgressRenderState::default()))
}

pub(super) fn eta_debug_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| env::var_os("COPY_RS_ETA_DEBUG").is_some())
}

pub(super) fn print_transfer_progress_bars(
    elapsed_s: f64,
    planned_bytes: u64,
    write_all_total: Option<u64>,
    phase_label: &str,
    rates: TransferProgressRates,
    proc_totals_delta: ProcIoDeltas,
    device_totals_delta: DeviceIoDeltas,
    eta_workload: Option<EtaWorkload>,
    eta_progress: Option<EtaProgressTotals>,
    mut eta_estimator: Option<&mut TransferEtaEstimator>,
    finalize_line: bool,
    flushing: bool,
) {
    const BAR_WIDTH: usize = 34;
    const LABEL_COL_WIDTH: usize = 13;
    const BYTES_COL_WIDTH: usize = 11;
    const RATE_COL_WIDTH: usize = 14;

    let done = write_all_total;
    let pct = if planned_bytes > 0 {
        done.map(|d| (d as f64 * 100.0 / planned_bytes as f64).clamp(0.0, 100.0))
    } else {
        None
    };
    let eta_telemetry = EtaTelemetry {
        write_bytes: proc_totals_delta.write_bytes,
        write_complete: device_totals_delta.write_complete,
        read_bytes: proc_totals_delta.read_bytes,
        read_complete: device_totals_delta.read_complete,
    };
    let mut eta_estimate = None;
    let eta = match eta_estimator.as_mut() {
        Some(estimator) => {
            let eta = estimator.update_with_telemetry(
                done,
                planned_bytes,
                rates.write_all_bps,
                elapsed_s,
                finalize_line,
                eta_workload,
                eta_progress,
                eta_telemetry,
                rates,
            );
            eta_estimate = estimator.last_estimate();
            eta
        }
        None if done.map(|value| value >= planned_bytes).unwrap_or(false) => Some(0.0),
        None => None,
    };

    let transfer_total = done;
    let transfer_rate = rates.write_all_bps;
    let write_disk_total = proc_totals_delta.write_bytes;
    let write_disk_rate = rates.write_bytes_bps;
    let write_complete_total = device_totals_delta.write_complete;
    let write_complete_rate = rates.write_complete_bps;
    let read_disk_total = proc_totals_delta.read_bytes;
    let read_disk_rate = rates.read_bytes_bps;
    let read_complete_total = device_totals_delta.read_complete;
    let read_complete_rate = rates.read_complete_bps;
    let read_cache_total =
        option_u64_saturating_sub(proc_totals_delta.rchar, proc_totals_delta.read_bytes);
    let read_cache_rate = read_cache_total.map(|total| {
        if elapsed_s <= 0.0 {
            0.0
        } else {
            total as f64 / elapsed_s.max(1e-6)
        }
    });

    let done_s = fmt_bytes_block_opt(transfer_total, 3);
    let planned_s = if planned_bytes > 0 {
        format_bytes_binary(planned_bytes, 3)
    } else {
        "--".to_string()
    };
    let eta_s = match eta {
        Some(v) => fmt_hms_tenths(v.max(0.0)),
        None => "--:--:--.-".to_string(),
    };
    let eta_debug = if eta_debug_enabled() {
        eta_estimate
            .map(|estimate| {
                let p10 = estimate
                    .p10_s
                    .map(fmt_hms_tenths)
                    .unwrap_or_else(|| "--:--:--.-".to_string());
                let p50 = estimate
                    .p50_s
                    .map(fmt_hms_tenths)
                    .unwrap_or_else(|| "--:--:--.-".to_string());
                let p90 = estimate
                    .p90_s
                    .map(fmt_hms_tenths)
                    .unwrap_or_else(|| "--:--:--.-".to_string());
                format!(
                    " [p10 {p10} p50 {p50} p90 {p90} {} {:.0}%]",
                    estimate.activity.label(),
                    estimate.confidence * 100.0
                )
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    let pct_s = match pct {
        Some(v) => format!("{v:>6.2}%"),
        None => "  ---%".to_string(),
    };
    let transfer_complete = pct.map(|p| p >= 100.0).unwrap_or(false);
    let terminal_width = terminal_columns();
    let clamp = |line| clamp_line_for_columns(line, terminal_width);
    let transfer_rate_display = if transfer_complete {
        format!("{:>width$}", "Complete", width = RATE_COL_WIDTH)
    } else {
        format!(
            "{:>width$}",
            fmt_rate_block_opt(transfer_rate),
            width = RATE_COL_WIDTH
        )
    };

    let object_progress_lines = eta_workload.zip(eta_progress).map(|(workload, progress)| {
        let total_files: u64 = workload.file_bins.iter().copied().sum();
        let completed_files = progress.file_count().min(total_files);
        let completed_dirs = progress.dirs.min(workload.dirs);
        vec![
            clamp(format!(
                "{:<label_width$}  {} / {}",
                "Files",
                format_number(completed_files),
                format_number(total_files),
                label_width = LABEL_COL_WIDTH
            )),
            clamp(format!(
                "{:<label_width$}  {} / {}",
                "Dirs",
                format_number(completed_dirs),
                format_number(workload.dirs),
                label_width = LABEL_COL_WIDTH
            )),
        ]
    });

    let mut lines = vec![
        clamp(format!(
            "{pct_s} [{}] {done_s} / {planned_s}  {eta_s} eta{eta_debug}",
            build_progress_bar(pct, BAR_WIDTH)
        )),
        String::new(),
    ];
    if let Some(object_lines) = object_progress_lines {
        lines.extend(object_lines);
    }
    lines.extend([
        clamp(format!(
            "{:<label_width$}  {:>bytes_width$}   {:>rate_width$}",
            phase_label,
            fmt_bytes_block_opt(transfer_total, 3),
            transfer_rate_display,
            label_width = LABEL_COL_WIDTH,
            bytes_width = BYTES_COL_WIDTH,
            rate_width = RATE_COL_WIDTH
        )),
        clamp(format!(
            "{:<label_width$}  {:>bytes_width$}   {:>rate_width$}",
            "WriteDisk",
            fmt_bytes_block_opt(write_disk_total, 3),
            fmt_rate_block_opt(write_disk_rate),
            label_width = LABEL_COL_WIDTH,
            bytes_width = BYTES_COL_WIDTH,
            rate_width = RATE_COL_WIDTH
        )),
        clamp(format!(
            "{:<label_width$}  {:>bytes_width$}   {:>rate_width$}{}",
            "WriteComplete",
            fmt_bytes_block_opt(write_complete_total, 3),
            fmt_rate_block_opt(write_complete_rate),
            if flushing { " flushing" } else { "" },
            label_width = LABEL_COL_WIDTH,
            bytes_width = BYTES_COL_WIDTH,
            rate_width = RATE_COL_WIDTH
        )),
        clamp(format!(
            "{:<label_width$}  {:>bytes_width$}   {:>rate_width$}",
            "ReadCache",
            fmt_bytes_block_opt(read_cache_total, 3),
            fmt_rate_block_opt(read_cache_rate),
            label_width = LABEL_COL_WIDTH,
            bytes_width = BYTES_COL_WIDTH,
            rate_width = RATE_COL_WIDTH
        )),
        clamp(format!(
            "{:<label_width$}  {:>bytes_width$}   {:>rate_width$}",
            "ReadDisk",
            fmt_bytes_block_opt(read_disk_total, 3),
            fmt_rate_block_opt(read_disk_rate),
            label_width = LABEL_COL_WIDTH,
            bytes_width = BYTES_COL_WIDTH,
            rate_width = RATE_COL_WIDTH
        )),
        clamp(format!(
            "{:<label_width$}  {:>bytes_width$}   {:>rate_width$}",
            "ReadComplete",
            fmt_bytes_block_opt(read_complete_total, 3),
            fmt_rate_block_opt(read_complete_rate),
            label_width = LABEL_COL_WIDTH,
            bytes_width = BYTES_COL_WIDTH,
            rate_width = RATE_COL_WIDTH
        )),
    ]);

    if !stdout_is_tty() {
        if finalize_line {
            for line in &lines {
                println!("{line}");
            }
        }
        return;
    }

    if let Ok(mut state) = progress_render_state().lock() {
        let frame = lines.join("\n");
        if state.active && !finalize_line && state.last_frame == frame {
            return;
        }
        let previous_row_count = if state.active {
            state.lines
        } else {
            state.finalized_lines
        };
        let previous_lines = if state.active {
            previous_row_count.saturating_sub(1)
        } else {
            previous_row_count
        };
        let mut output = String::with_capacity(frame.len() + lines.len() * 16 + 64);
        if previous_lines > 0 {
            let _ = write!(output, "\x1b[{}A\r", previous_lines);
        }

        // Keep each frame to one terminal row. Vertical cursor movement avoids
        // line-feed scrolling when the frame is rendered at the bottom edge.
        output.push_str("\x1b[?7l");
        for (idx, line) in lines.iter().enumerate() {
            let _ = write!(output, "\r\x1b[2K{line}");
            if idx + 1 < lines.len() {
                if previous_lines == 0 {
                    output.push('\n');
                } else {
                    output.push_str("\x1b[1B\r");
                }
            }
        }
        output.push_str("\x1b[?7h");
        let trailing_lines = previous_row_count.saturating_sub(lines.len());
        if trailing_lines > 0 {
            for _ in 0..trailing_lines {
                output.push_str("\x1b[1B\r\x1b[2K");
            }
            let _ = write!(output, "\x1b[{}A\r", trailing_lines);
        }
        if finalize_line {
            output.push('\n');
            state.active = false;
            state.lines = 0;
            state.finalized_lines = lines.len();
            state.last_frame.clear();
        } else {
            state.active = true;
            state.lines = lines.len();
            state.finalized_lines = 0;
            state.last_frame = frame;
        }
        let stdout = io::stdout();
        let mut locked = stdout.lock();
        let _ = locked.write_all(output.as_bytes());
        let _ = locked.flush();
    }
}

pub(super) fn reset_progress_render_state() {
    finish_progress_render_state();
}

pub(super) fn finish_progress_render_state() {
    if let Ok(mut state) = progress_render_state().lock() {
        if state.active {
            let mut output = String::new();
            let up = state.lines.saturating_sub(1);
            if up > 0 {
                let _ = write!(output, "\x1b[{up}A\r");
            }
            for idx in 0..state.lines {
                output.push_str("\r\x1b[2K");
                if idx + 1 < state.lines {
                    output.push_str("\x1b[1B\r");
                }
            }
            // Leave the cursor below the cleared frame so the next message
            // starts on a fresh line rather than overwriting its last row.
            output.push('\n');
            let stdout = io::stdout();
            let mut locked = stdout.lock();
            let _ = locked.write_all(output.as_bytes());
            let _ = locked.flush();
        }
        state.active = false;
        state.lines = 0;
        state.finalized_lines = 0;
    }
}

pub(super) fn print_summary_rate_line(label: &str, bps: f64, duration_s: f64, total: bool) {
    let total_suffix = if total { " (total)" } else { "" };
    let prefix = format!("{label}:");
    println!(
        "{prefix:<24}{}/s | Duration: {}{}",
        fmt_speed_bps(bps),
        fmt_hms_ms(duration_s),
        total_suffix
    );
}

pub(super) fn print_copy_duration_summary(
    transfer_duration_s: f64,
    transfer_bps: f64,
    flush_duration_s: f64,
    flush_bps: f64,
    cleanup: Option<(f64, f64, f64, f64)>,
    total_duration_s: f64,
) {
    println!(
        "{:<26}{}  ({}/s)",
        "Transfer Duration:",
        fmt_hms_ms(transfer_duration_s),
        fmt_speed_bps(transfer_bps)
    );
    println!(
        "{:<26}{}  ({}/s)",
        "Transfer Flush Duration:",
        fmt_hms_ms(flush_duration_s),
        fmt_speed_bps(flush_bps)
    );
    if let Some((cleanup_duration_s, cleanup_bps, cleanup_flush_duration_s, cleanup_flush_bps)) =
        cleanup
    {
        println!(
            "{:<26}{}  ({}/s)",
            "Cleanup Duration:",
            fmt_hms_ms(cleanup_duration_s),
            fmt_speed_bps(cleanup_bps)
        );
        println!(
            "{:<26}{}  ({}/s)",
            "Cleanup Flush Duration:",
            fmt_hms_ms(cleanup_flush_duration_s),
            fmt_speed_bps(cleanup_flush_bps)
        );
    }
    println!("{:<26}{}", "Total Duration:", fmt_hms_ms(total_duration_s));
}
