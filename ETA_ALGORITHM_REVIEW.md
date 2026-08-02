# Previous ETA Algorithm Review

This document contains the ETA implementation that was replaced, plus the
runtime context used to review it. It is retained as the before-state and
failure-case record for the new estimator.

## Inputs

The progress display calls `smoothed_transfer_eta_s` every 0.2 seconds with:

- `done`: cumulative logical bytes completed by the transfer code
- `total`: planned transfer bytes
- `instant_bps`: logical bytes completed since the previous 0.2-second update,
  divided by elapsed time
- `elapsed_s`: elapsed transfer time
- `phase_label`: currently `"Transfer"`
- `finalize_line`: whether the transfer progress line is final

The ETA does not use `write_bytes`, `read_bytes`, `/proc/diskstats`,
`WriteComplete`, or `ReadComplete`.

## Current implementation

```rust
fn transfer_eta_s(done: Option<u64>, total: u64, bps: Option<f64>) -> Option<f64> {
    if total == 0 {
        return None;
    }
    let done = done?;
    let bps = bps?;
    if bps <= 0.0 {
        return None;
    }
    let remain = total.saturating_sub(done.min(total));
    Some(remain as f64 / bps)
}

#[derive(Default)]
struct EtaSmootherState {
    key: String,
    samples: VecDeque<(f64, u64)>,
    display_eta_s: Option<f64>,
    last_elapsed_s: Option<f64>,
    slowdown_since_s: Option<f64>,
}

fn eta_smoother_state() -> &'static Mutex<EtaSmootherState> {
    static STATE: OnceLock<Mutex<EtaSmootherState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(EtaSmootherState::default()))
}

fn quantize_eta_s(eta_s: f64) -> f64 {
    eta_s.max(0.0).round()
}

fn ew_regression_speed_bps(
    samples: &VecDeque<(f64, u64)>,
    now_t: f64,
    tau_s: f64,
) -> Option<f64> {
    if samples.len() < 2 || !tau_s.is_finite() || tau_s <= 0.0 {
        return None;
    }

    let mut sw = 0.0;
    let mut swt = 0.0;
    let mut swx = 0.0;
    for (t, bytes) in samples {
        let age = (now_t - *t).max(0.0);
        let w = (-age / tau_s).exp();
        sw += w;
        swt += w * *t;
        swx += w * (*bytes as f64);
    }
    if sw <= 0.0 {
        return None;
    }
    let t_bar = swt / sw;
    let x_bar = swx / sw;

    let mut num = 0.0;
    let mut den = 0.0;
    for (t, bytes) in samples {
        let age = (now_t - *t).max(0.0);
        let w = (-age / tau_s).exp();
        let dt = *t - t_bar;
        let dx = (*bytes as f64) - x_bar;
        num += w * dt * dx;
        den += w * dt * dt;
    }
    if den <= 1e-12 {
        return None;
    }
    let v_hat = num / den;
    if v_hat.is_finite() && v_hat > 0.0 {
        Some(v_hat)
    } else {
        None
    }
}

fn smoothed_transfer_eta_s(
    phase_label: &str,
    done: Option<u64>,
    total: u64,
    instant_bps: Option<f64>,
    elapsed_s: f64,
    finalize_line: bool,
) -> Option<f64> {
    if total == 0 {
        return None;
    }
    let done = done?;
    let done_clamped = done.min(total);
    let remain = total.saturating_sub(done_clamped);
    if remain == 0 {
        return Some(0.0);
    }

    let key = format!("{phase_label}:{total}");
    if let Ok(mut st) = eta_smoother_state().lock() {
        if st.key != key || st.last_elapsed_s.map(|v| elapsed_s < v).unwrap_or(false) {
            st.key = key.clone();
            st.samples.clear();
            st.display_eta_s = None;
            st.last_elapsed_s = None;
            st.slowdown_since_s = None;
        }

        if st
            .samples
            .back()
            .map(|(t, b)| *t != elapsed_s || *b != done_clamped)
            .unwrap_or(true)
        {
            st.samples.push_back((elapsed_s, done_clamped));
        }
        const REGRESSION_WINDOW_S: f64 = 60.0;
        while st
            .samples
            .front()
            .map(|(t, _)| elapsed_s - *t > REGRESSION_WINDOW_S)
            .unwrap_or(false)
        {
            st.samples.pop_front();
        }

        const TAU_S: f64 = 20.0;
        let sample_span_s = match (st.samples.front(), st.samples.back()) {
            (Some((first_t, _)), Some((last_t, _))) => (last_t - first_t).max(0.0),
            _ => 0.0,
        };
        const ETA_WARMUP_S: f64 = 5.0;
        if st.display_eta_s.is_none() && sample_span_s < ETA_WARMUP_S {
            st.last_elapsed_s = Some(elapsed_s);
            return None;
        }

        let reg_bps = if sample_span_s >= 3.0 {
            ew_regression_speed_bps(&st.samples, elapsed_s, TAU_S)
        } else {
            None
        };
        let avg_bps = if elapsed_s > 0.25 && done_clamped > 0 {
            Some(done_clamped as f64 / elapsed_s.max(1e-6))
        } else {
            None
        };
        let speed_bps = match (reg_bps, avg_bps, instant_bps) {
            (Some(r), Some(a), _) => Some((r * 0.80) + (a * 0.20)),
            (Some(r), None, Some(i)) if i > 0.0 => Some((r * 0.85) + (i * 0.15)),
            (Some(r), _, _) => Some(r),
            (None, Some(a), Some(i)) if i > 0.0 => Some((a * 0.80) + (i * 0.20)),
            (None, Some(a), _) => Some(a),
            (None, None, Some(i)) if i > 0.0 => Some(i),
            _ => None,
        }?;
        let raw_eta = transfer_eta_s(Some(done_clamped), total, Some(speed_bps))?;

        let dt = st
            .last_elapsed_s
            .map(|prev| (elapsed_s - prev).max(0.0))
            .unwrap_or(0.0);
        st.last_elapsed_s = Some(elapsed_s);

        let next_eta = if let Some(prev_display) = st.display_eta_s {
            let countdown = (prev_display - dt).max(0.0);
            let margin = (0.05 * countdown).max(10.0);
            if raw_eta < countdown {
                let down_alpha = if dt > 0.0 {
                    1.0 - (-dt / 5.0).exp()
                } else {
                    0.0
                };
                st.slowdown_since_s = None;
                countdown + down_alpha * (raw_eta - countdown)
            } else if raw_eta <= countdown + margin {
                st.slowdown_since_s = None;
                countdown
            } else {
                if st.slowdown_since_s.is_none() {
                    st.slowdown_since_s = Some(elapsed_s);
                }
                let severe_slowdown = raw_eta >= countdown.max(10.0) * 2.0;
                let sustained = st
                    .slowdown_since_s
                    .map(|start| (elapsed_s - start) >= 2.0)
                    .unwrap_or(false);
                if severe_slowdown || sustained {
                    let up_alpha = if dt > 0.0 {
                        let tau = if severe_slowdown { 3.0 } else { 12.0 };
                        1.0 - (-dt / tau).exp()
                    } else {
                        0.0
                    };
                    countdown + up_alpha * (raw_eta - countdown)
                } else {
                    countdown
                }
            }
        } else {
            raw_eta
        };

        st.display_eta_s = Some(next_eta.max(0.0));
        let out = st.display_eta_s.map(quantize_eta_s);
        if finalize_line {
            st.key.clear();
            st.samples.clear();
            st.display_eta_s = None;
            st.last_elapsed_s = None;
            st.slowdown_since_s = None;
        }
        return out;
    }

    transfer_eta_s(Some(done_clamped), total, instant_bps).map(quantize_eta_s)
}
```

The call site is:

```rust
let eta = smoothed_transfer_eta_s(
    phase_label,
    write_all_total,
    planned_bytes,
    rates.write_all_bps,
    elapsed_s,
    finalize_line,
);
```

## Observed failure case

```text
57.92% 38.411 GiB / 66.317 GiB 00:00:48.0 eta

Transfer        38.411 GiB      9.726 MiB/s
WriteDisk       38.739 GiB      10.29 MiB/s
WriteComplete    6.926 GiB        0.000 B/s
ReadCache       27.539 GiB      24.52 MiB/s
ReadDisk        10.880 GiB      9.173 MiB/s
ReadComplete    10.877 GiB      9.817 MiB/s
```

Approximately 27.906 GiB remained. At 9.726 MiB/s, the instantaneous ETA was
about 49 minutes, while the display showed 48 seconds. The likely regime change
was an initially fast page-cache-backed transfer followed by physical USB HDD
throughput.

## Review goals

Please recommend an ETA algorithm that:

- remains stable during ordinary short throughput fluctuations;
- detects sustained cache-to-disk or fast-to-slow regime changes within a few
  seconds;
- does not retain a misleading whole-transfer average after such a change;
- handles temporary zero-progress stalls without immediately producing extreme
  ETAs;
- remains useful for both large files and trees containing many small files;
- updates every 0.2 seconds but may aggregate samples over longer windows;
- reports hours, minutes, and seconds with one-second display granularity.
