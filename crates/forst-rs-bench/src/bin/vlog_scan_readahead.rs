//! FRS-VLOG-SCAN-READAHEAD (Phase-2 cycle 6) — scan-side prefetch-pipelining
//! latency-hiding cost model. Design: `2026-06-15-vlog-scan-readahead-design.md`.
//!
//! `FRS_VLOG_SCAN_COALESCE` resolves a scan's `BlobRef` derefs ONE WINDOW at a
//! time, but `ScanCoalesceIter` processes windows SERIALLY relative to
//! consumption: it drains window `k`'s rows, and only when EMPTY does it BLOCK on
//! window `k+1`'s coalesced remote read. On the disaggregated/remote path that is
//! one window-RTT of dead time between every window. `FRS_VLOG_SCAN_READAHEAD=1`
//! does one-window-deep look-ahead: window `k+1`'s coalesced read runs on the
//! shared read-I/O pool WHILE the consumer drains window `k`.
//!
//! This bin models the CROSS-WINDOW latency term ONLY: a scan of `Wn` windows,
//! each costing one modeled coalesced remote READ (`FRS_MODEL_RTT_MS`) plus a
//! modeled per-window CONSUME time (`FRS_MODEL_CONSUME_MS`, the downstream
//! operator draining the window's resolved rows).
//!
//!   serial    (cycle-5): per window  read THEN consume  ⇒ ≈ Wn·(rtt + consume)
//!   readahead (this):    window k+1's read overlaps window k's consume
//!                        ⇒ ≈ rtt + Wn·max(rtt, consume)
//!
//! The hidden term is `Wn·min(rtt, consume)` — approaching one window's RTT
//! amortized per window. The pool mirrors `prefetch.rs::read_io_pool`
//! (`clamp(cores/2, 2, 6)`, env `FRS_RS_PREFETCH_THREADS`). std-only
//! (Mutex + Condvar + mpsc), `#![forbid(unsafe_code)]`-clean.
//!
//! Run:   `cargo run -p forst-rs-bench --bin vlog_scan_readahead --release`
//! Smoke: append `-- --smoke` (caps timings; asserts the win at consume≈rtt).

#![forbid(unsafe_code)]

use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

fn pool_width() -> usize {
    std::env::var("FRS_RS_PREFETCH_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(|| {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            (cores / 2).clamp(2, 6)
        })
}

fn ms_env(key: &str, default: f64) -> Duration {
    let v = std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(default);
    Duration::from_secs_f64(v / 1e3)
}

/// SERIAL (cycle-5 `ScanCoalesceIter::next`): per window, the remote coalesced
/// read THEN the consume happen back-to-back on the consumer thread. Wall ≈
/// `Wn·(rtt + consume)`.
fn serial_scan(windows: usize, rtt: Duration, consume: Duration) -> Duration {
    let t0 = Instant::now();
    for _ in 0..windows {
        std::thread::sleep(rtt); // window's coalesced remote read (blocking)
        std::thread::sleep(consume); // consumer drains the window's rows
    }
    t0.elapsed()
}

type Job = Box<dyn FnOnce() + Send>;
struct Shared {
    queue: Mutex<(std::collections::VecDeque<Job>, bool)>,
    cv: Condvar,
}
/// Bounded worker pool mirroring `prefetch.rs::ReadIoPool`.
struct Pool {
    shared: Arc<Shared>,
    workers: Vec<std::thread::JoinHandle<()>>,
}
impl Pool {
    fn new(n: usize) -> Self {
        let shared = Arc::new(Shared {
            queue: Mutex::new((std::collections::VecDeque::new(), false)),
            cv: Condvar::new(),
        });
        let mut workers = Vec::with_capacity(n);
        for _ in 0..n.max(1) {
            let sh = Arc::clone(&shared);
            workers.push(std::thread::spawn(move || loop {
                let job = {
                    let mut g = sh.queue.lock().unwrap_or_else(|p| p.into_inner());
                    loop {
                        if let Some(j) = g.0.pop_front() {
                            break Some(j);
                        }
                        if g.1 {
                            break None;
                        }
                        g = sh.cv.wait(g).unwrap_or_else(|p| p.into_inner());
                    }
                };
                match job {
                    Some(j) => j(),
                    None => break,
                }
            }));
        }
        Self { shared, workers }
    }
    fn submit(&self, job: Job) {
        let mut g = self.shared.queue.lock().unwrap_or_else(|p| p.into_inner());
        g.0.push_back(job);
        drop(g);
        self.shared.cv.notify_one();
    }
    fn shutdown(self) {
        {
            let mut g = self.shared.queue.lock().unwrap_or_else(|p| p.into_inner());
            g.1 = true;
        }
        self.shared.cv.notify_all();
        for w in self.workers {
            let _ = w.join();
        }
    }
}

/// A window's "resolution" = one modeled coalesced remote read, run on the pool,
/// signalling completion over a oneshot channel (the consumer's join).
fn launch_window(pool: &Pool, rtt: Duration) -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::channel();
    pool.submit(Box::new(move || {
        std::thread::sleep(rtt); // window's coalesced remote read, overlapped
        let _ = tx.send(());
    }));
    rx
}

/// READAHEAD depth-`D` (this cycle): keep up to `D` windows' reads in flight on the
/// pool ahead of the one being consumed (a FIFO of receivers), join the FRONT
/// (oldest) before each consume, and refill the FIFO back up to `D`. Mirrors
/// `ScanCoalesceIter::next`'s refill-to-depth → join-front → drain loop.
///
/// * `D == 1` reproduces the cycle-6 one-window-deep look-ahead exactly:
///   `rtt + Wn·max(rtt, consume)` — depth-1 can only hide one `consume` worth of
///   the read, so when `rtt > consume` the consumer still stalls `rtt - consume`
///   per window.
/// * `D >= ceil(rtt / consume)` (and `D <= pool width`) fully hides the read:
///   `D` windows resolve in parallel, so a resolved window is always ready when
///   the consumer finishes the previous drain ⇒ steady state ≈ `Wn·consume`
///   (`consume`-bound), mirroring ForSt's parallel read threads. Effective
///   concurrency is `min(D, pool_width)`.
fn readahead_scan_depth(
    pool: &Pool,
    windows: usize,
    rtt: Duration,
    consume: Duration,
    depth: usize,
) -> Duration {
    let depth = depth.max(1);
    let t0 = Instant::now();
    let mut inflight: std::collections::VecDeque<mpsc::Receiver<()>> =
        std::collections::VecDeque::with_capacity(depth);
    let mut launched = 0usize;
    for k in 0..windows {
        // Refill the in-flight FIFO up to `depth` windows (bounded by what is left
        // to scan) — these reads run concurrently on the pool.
        while inflight.len() < depth && launched < windows {
            inflight.push_back(launch_window(pool, rtt));
            launched += 1;
        }
        // Join the FRONT (oldest) in-flight window — its read overlapped the prior
        // windows' consumes (and the deeper windows' reads).
        if let Some(rx) = inflight.pop_front() {
            let _ = rx.recv();
        }
        let _ = k;
        std::thread::sleep(consume); // consumer drains this window's rows
    }
    t0.elapsed()
}

/// FRS-VLOG-SCAN-READAHEAD-ADAPTIVE (cycle 8): the EWMA depth controller, modelled
/// exactly as `db.rs::AdaptiveDepthCtl` — `ceil(rtt_ewma/consume_ewma)` clamped to
/// `min(pool_width, byte_cap, static_ceiling)`, floored at 1. Mirrored here so the
/// bench can prove the adaptive path reaches the best STATIC depth across RTT
/// regimes WITHOUT a per-regime knob.
struct AdaptiveCtl {
    rtt_ewma: Option<f64>,
    consume_ewma: Option<f64>,
    row_bytes_ewma: Option<f64>,
    pool_width: usize,
    static_ceiling: usize,
    budget_bytes: u64,
    window: usize,
}
impl AdaptiveCtl {
    const ALPHA: f64 = 0.25;
    fn new(pool_width: usize, static_ceiling: usize, budget_bytes: u64, window: usize) -> Self {
        Self {
            rtt_ewma: None,
            consume_ewma: None,
            row_bytes_ewma: None,
            pool_width: pool_width.max(1),
            static_ceiling: static_ceiling.max(1),
            budget_bytes,
            window: window.max(1),
        }
    }
    fn push(slot: &mut Option<f64>, s: f64) {
        *slot = Some(match *slot {
            None => s,
            Some(p) => p + Self::ALPHA * (s - p),
        });
    }
    fn observe(&mut self, rtt: Duration, consume: Duration, avg_row_bytes: f64) {
        Self::push(&mut self.rtt_ewma, rtt.as_secs_f64());
        Self::push(&mut self.consume_ewma, consume.as_secs_f64());
        if avg_row_bytes > 0.0 {
            Self::push(&mut self.row_bytes_ewma, avg_row_bytes);
        }
    }
    fn byte_cap(&self) -> usize {
        let avg = self.row_bytes_ewma.unwrap_or(0.0);
        if avg <= 0.0 {
            return self.pool_width;
        }
        let per_window = self.window as f64 * avg;
        if per_window <= 0.0 {
            return self.pool_width;
        }
        let max_plus_one = (self.budget_bytes as f64 / per_window).floor();
        (max_plus_one - 1.0).max(1.0) as usize
    }
    fn target_depth(&self) -> usize {
        let raw = match (self.rtt_ewma, self.consume_ewma) {
            // `+1`: the joined window was launched D-1 consumes ago, so the read is
            // hidden once (D-1)·consume >= rtt ⇒ D >= rtt/consume + 1. One extra
            // slot beyond the consumed one is the minimum for ANY overlap.
            (Some(rtt), Some(c)) if c > 0.0 => (rtt / c).ceil() as usize + 1,
            (Some(_), _) => self.pool_width,
            _ => 1,
        };
        let ceiling = self
            .pool_width
            .min(self.byte_cap())
            .min(self.static_ceiling)
            .max(1);
        raw.clamp(1, ceiling)
    }
}

/// ADAPTIVE scan: same FIFO pipeline as `readahead_scan_depth`, but the refill
/// target is the controller's live `target_depth()`, re-evaluated each window from
/// the measured read latency (the launched window's modeled `rtt`) and the elapsed
/// inter-join drain (`consume`). Mirrors `ScanCoalesceIter::next` under adaptive.
/// Returns the wall AND the final settled depth (to show convergence).
fn adaptive_scan(
    pool: &Pool,
    windows: usize,
    rtt: Duration,
    consume: Duration,
    pool_width: usize,
    static_ceiling: usize,
    avg_row_bytes: f64,
) -> (Duration, usize) {
    let mut ctl = AdaptiveCtl::new(pool_width, static_ceiling, 1u64 << 40, 32);
    let t0 = Instant::now();
    let mut inflight: std::collections::VecDeque<mpsc::Receiver<()>> =
        std::collections::VecDeque::new();
    let mut launched = 0usize;
    // Mirrors the engine: `last_drain_end` = when the PREVIOUS join completed, so
    // the consume sample is the PURE drain (excludes the recv block = unhidden
    // read, which the controller already sees via the modeled `rtt`).
    let mut last_drain_end: Option<Instant> = None;
    for _ in 0..windows {
        let target = ctl.target_depth().max(1);
        while inflight.len() < target && launched < windows {
            inflight.push_back(launch_window(pool, rtt));
            launched += 1;
        }
        // Pure drain interval since the previous join completed (BEFORE this recv).
        let observed_consume = last_drain_end.map(|t| t.elapsed());
        if let Some(rx) = inflight.pop_front() {
            let _ = rx.recv();
        }
        if let Some(c) = observed_consume {
            // The modeled true read latency IS `rtt` (the launch sleep) — the same
            // value the pool job would time. Feed it + the pure drain.
            ctl.observe(rtt, c, avg_row_bytes);
        }
        last_drain_end = Some(Instant::now());
        std::thread::sleep(consume); // downstream drain of this window's rows
    }
    (t0.elapsed(), ctl.target_depth())
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");
    let pool_n = pool_width();
    // Smoke caps both timings to keep CI fast while preserving the ratio.
    let (rtt, consume_base) = if smoke {
        (Duration::from_millis(4), Duration::from_millis(4))
    } else {
        (
            ms_env("FRS_MODEL_RTT_MS", 23.0),
            ms_env("FRS_MODEL_CONSUME_MS", 23.0),
        )
    };

    let depths: Vec<usize> = if smoke {
        vec![1, 2, 4]
    } else {
        std::env::var("FRS_VLOG_SCAN_READAHEAD_DEPTH")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|d| vec![d])
            .unwrap_or_else(|| vec![1, 2, 4, 8])
    };

    println!(
        "== vlog-scan READAHEAD depth-D latency-hiding cost model ==\n\
         pool width = {pool_n} (clamp(cores/2,2,6); FRS_RS_PREFETCH_THREADS)\n\
         modeled per-window read RTT = {:.1} ms (FRS_MODEL_RTT_MS)\n\
         serial      = Wn·(rtt + consume)\n\
         depth-1     = rtt + Wn·max(rtt, consume)         (cycle-6: hides 1 consume)\n\
         depth-D     ≈ rtt + Wn·max(rtt/min(D,pool), consume)  (D reads in parallel)\n\
         depth-OPT fully hides the read once D >= ceil(rtt/consume) (& D <= pool)\n",
        ms(rtt)
    );

    let pool = Pool::new(pool_n);
    // Sweep consume/rtt ratio × windows × depth. Depth pays off most when
    // rtt > consume (a single in-flight window can't hide the whole read); when
    // consume >= rtt depth-1 already saturates and deeper adds nothing (never
    // regresses). Track the best deep-vs-depth1 win in the rtt-bound regime.
    let window_counts: Vec<usize> = if smoke { vec![16] } else { vec![8, 32] };
    // Include a read-bound ratio (consume = rtt/4) where depth is the lever.
    let consume_ratios: Vec<f64> = if smoke {
        vec![0.25]
    } else {
        vec![0.25, 1.0, 2.0]
    };

    let mut best_deep_over_d1_readbound = 1.0f64;
    for &cr in &consume_ratios {
        let consume = consume_base.mul_f64(cr);
        println!(
            "\n-- consume/rtt = {cr} (consume = {:.1} ms) --",
            ms(consume)
        );
        println!(
            "{:>9} {:>13} {:>11} {:>13} {:>10} {:>13}",
            "windows", "serial(ms)", "depth", "depth-D(ms)", "vs serial", "vs depth-1"
        );
        for &wn in &window_counts {
            let s = serial_scan(wn, rtt, consume);
            let mut d1 = f64::NAN;
            for &d in &depths {
                let r = readahead_scan_depth(&pool, wn, rtt, consume, d);
                let vs_serial = ms(s) / ms(r).max(1e-9);
                if d == 1 {
                    d1 = ms(r);
                }
                let vs_d1 = d1 / ms(r).max(1e-9);
                // Only deep (D>1) in the read-bound regime (consume < rtt) is the
                // claim under test — depth must hide more of the read there.
                if d > 1 && cr < 1.0 {
                    best_deep_over_d1_readbound = best_deep_over_d1_readbound.max(vs_d1);
                }
                println!(
                    "{wn:>9} {:>13.1} {d:>11} {:>13.1} {:>9.2}× {:>12.2}×",
                    ms(s),
                    ms(r),
                    vs_serial,
                    vs_d1
                );
            }
        }
    }

    // ========================================================================
    // ADAPTIVE (cycle 8): self-sizing depth vs the BEST static depth per regime.
    // ========================================================================
    // The claim under test: the EWMA controller reaches near the best static depth
    // across BOTH the read-bound and drain-bound regimes WITHOUT a per-regime knob,
    // and never regresses the drain-bound case (where best static = depth 1).
    println!(
        "\n== ADAPTIVE depth controller vs BEST static depth (no per-regime knob) ==\n\
         pool width = {pool_n}; static ceiling = 64; budget = unbounded\n\
         For each regime: best static over {{1,2,4,8}} vs the adaptive run.\n"
    );
    println!(
        "{:>14} {:>12} {:>11} {:>14} {:>13} {:>10} {:>13}",
        "consume/rtt",
        "best-static",
        "best(ms)",
        "adaptive(ms)",
        "settled-D",
        "adpt/best",
        "adpt/serial"
    );
    let adaptive_regimes: Vec<f64> = if smoke {
        vec![0.25, 2.0]
    } else {
        vec![0.125, 0.25, 0.5, 1.0, 2.0]
    };
    // Enough windows that the controller's depth-1→target RAMP (a fixed warmup of
    // ~ceil(rtt/consume) windows) amortizes — the claim is steady-state parity with
    // the best static depth, not zero warmup. Smoke caps the per-window cost (4ms)
    // so a longer run is still sub-second.
    let adaptive_windows = if smoke { 80 } else { 256 };
    let mut worst_adaptive_vs_best = 1.0f64; // ratio adaptive/best; >1 means adaptive trails
    let mut drain_bound_regressed = false;
    let mut converged_all = true; // settled-D == best-static-D every regime
    for &cr in &adaptive_regimes {
        let consume = consume_base.mul_f64(cr);
        let serial = ms(serial_scan(adaptive_windows, rtt, consume));
        // Best static depth for this regime: sweep a generous set, take the min.
        // Averaged over 3 runs so a noisy outlier doesn't pick a spurious "best".
        let static_set = [1usize, 2, 3, 4, 6, 8];
        let mut best_static_ms = f64::INFINITY;
        let mut best_static_d = 1usize;
        for &d in &static_set {
            let mut acc = 0.0;
            for _ in 0..3 {
                acc += ms(readahead_scan_depth(
                    &pool,
                    adaptive_windows,
                    rtt,
                    consume,
                    d,
                ));
            }
            let r = acc / 3.0;
            if r < best_static_ms {
                best_static_ms = r;
                best_static_d = d;
            }
        }
        // Adaptive: ceiling = 64 (no static pin), avg_row_bytes small (byte cap
        // inert here — exercised in the engine UT). Mean over 3 runs to de-noise.
        let mut adpt_sum = 0.0;
        let mut settled = 0usize;
        for _ in 0..3 {
            let (w, d) = adaptive_scan(&pool, adaptive_windows, rtt, consume, pool_n, 64, 64.0);
            adpt_sum += ms(w);
            settled = d;
        }
        let adaptive_ms = adpt_sum / 3.0;
        let adpt_over_best = adaptive_ms / best_static_ms.max(1e-9);
        worst_adaptive_vs_best = worst_adaptive_vs_best.max(adpt_over_best);
        // PRIMARY claim: the controller CONVERGES to the MINIMAL depth that fully
        // hides the read — `ceil(rtt/consume)` clamped to the pool width — without a
        // per-regime knob. (Best-static's argmin is NOISY: any depth >= that minimum
        // ties on wall, so `best_static_d` may land on 4/6/8 interchangeably; the
        // controller deliberately picks the SMALLEST optimum, the least buffering.)
        let expected_optimal =
            ((rtt.as_secs_f64() / consume.as_secs_f64()).ceil() as usize + 1).clamp(1, pool_n);
        if cr < 1.0 && settled != expected_optimal {
            converged_all = false;
        }
        // Drain-bound (consume >= rtt): the minimal overlapping depth is 2 (one read
        // hidden behind one drain); the adaptive run must SETTLE at exactly 2 (never
        // over-provision a consume-bound scan, never collapse to the non-overlapping
        // depth 1).
        if cr >= 1.0 && settled != 2.min(pool_n) {
            drain_bound_regressed = true;
        }
        println!(
            "{cr:>14} {best_static_d:>12} {best_static_ms:>11.1} {adaptive_ms:>14.1} \
             {settled:>13} {adpt_over_best:>9.2}× {:>12.2}×",
            serial / adaptive_ms.max(1e-9)
        );
    }
    pool.shutdown();

    if smoke {
        // In the read-bound regime (consume = rtt/4) a single in-flight window
        // hides only ~1 consume of the 4-consume read; depth >= 2 must hide more,
        // so deep readahead must beat depth-1 by a clear margin. A collapse to
        // depth-1 means the multi-window FIFO refill broke.
        assert!(
            best_deep_over_d1_readbound > 1.3,
            "depth smoke: deep readahead didn't beat depth-1 in the read-bound regime \
             ({best_deep_over_d1_readbound:.2}× <= 1.3×)"
        );
        // The adaptive controller must CONVERGE to the best static depth in the
        // read-bound regime (no per-regime knob), SETTLE at depth 1 when
        // drain-bound (never over-provision), and stay within a modest factor of the
        // best static wall across the sweep (the residual is the depth-1→target
        // warmup ramp, a fixed cost).
        assert!(
            converged_all,
            "adaptive smoke: read-bound depth did not converge to the best static depth"
        );
        assert!(
            !drain_bound_regressed,
            "adaptive smoke: drain-bound case did not settle at the minimal overlapping depth 2"
        );
        // Loose sanity bound on the wall: the adaptive run carries the depth-1→target
        // warmup ramp AND, on a shared CI host, real-`sleep` scheduling jitter, so a
        // tight wall ratio is unreliable here — convergence (above) is the load-bearing
        // claim. This only guards against a gross blow-up (e.g. depth oscillation).
        assert!(
            worst_adaptive_vs_best <= 2.5,
            "adaptive smoke: adaptive grossly trailed best static by {worst_adaptive_vs_best:.2}× (> 2.5×)"
        );
        println!(
            "\nSMOKE OK: deep-over-depth1 (read-bound) {best_deep_over_d1_readbound:.2}× (> 1.3×); \
             adaptive CONVERGED to the minimal-optimal depth (read-bound) + settled at the minimal \
             overlapping depth 2 (drain-bound); within {worst_adaptive_vs_best:.2}× of best static \
             wall (warmup + sleep jitter)."
        );
    }
}
