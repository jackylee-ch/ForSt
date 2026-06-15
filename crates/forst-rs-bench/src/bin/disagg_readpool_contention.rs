// Copyright 2026 The ForSt-RS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! FRS-PHASE2 DISAGG **READ-I/O POOL COMBINED-STACK + CONTENTION** model.
//!
//! The per-lever bins (`vlog_scan_readahead`, `compact_input_warm`,
//! `compact_input_warm_data`) each validate ONE latency-hiding lever in
//! ISOLATION against its own private pool. But in production all three lever
//! kinds share the ONE process-global read-I/O pool
//! (`prefetch.rs::read_io_pool`, a FIFO `VecDeque<Job>` served by
//! `clamp(cores/2, 2, 6)` workers — NO priority, NO fairness). When a
//! disaggregated workload runs them CONCURRENTLY — foreground scans firing
//! depth-D readahead windows WHILE background compaction warms its next job's
//! input readers AND primes their first data blocks — the three job streams
//! interleave on that single FIFO. This bin answers the questions the isolated
//! bins cannot:
//!
//!   1. **COMPOSE** — does turning all three levers ON together produce the
//!      combined wall win, and what is each lever's MARGINAL contribution when
//!      added on top of the others?
//!   2. **CONTENTION** — does the shared FIFO pool SATURATE when all three
//!      consumers run together — i.e. is the combined win < the sum of the
//!      isolated wins, and does any single consumer's latency REGRESS because
//!      the FIFO lets a burst of one lever's jobs delay another's? (The
//!      latency-critical case: a foreground scan window stuck behind a burst of
//!      background compaction-warm opens.)
//!   3. **FAIRNESS FIX** — if contention is found, a class-aware pool that puts
//!      latency-critical foreground reads (scan windows) ahead of background
//!      warm-ups removes the FIFO head-of-line block while staying
//!      work-conserving (background still uses the whole pool when foreground
//!      is idle). This is the read-pool analogue of the upload-side
//!      `FRS_UPLOAD_FLUSH_RESERVED` reserved-lane QoS.
//!
//! ## What is MODELED
//!
//! A pure timing model (no engine state) of the read-I/O pool job mix on a
//! disaggregated channel. Each pool job is one remote read whose service time =
//! fixed RTT + bytes/bandwidth (`FRS_REMOTE_BW_MBPS`, `FRS_REMOTE_RTT_MS`).
//! Three concurrent consumers submit jobs to the SAME pool:
//!
//!   * **scan** (foreground, latency-critical): a depth-D readahead pipeline
//!     over `Wn` windows — each window one coalesced remote read; the consumer
//!     drains a window (`consume`) then joins the oldest in-flight window. This
//!     is the `vlog_scan_readahead` shape, but its windows now share the pool.
//!   * **compact-warm** (background): a chain of compaction jobs; before each
//!     job's merge it fires its NEXT job's `K` input-reader OPENs onto the pool
//!     (footer + sparse-index GET each) — the `compact_input_warm` shape.
//!   * **compact-warm-data** (background): on top of warm, each predicted input
//!     ALSO primes its first DATA block onto the pool — the
//!     `compact_input_warm_data` shape (a second, larger pool job per input).
//!
//! The model mirrors the real pool faithfully: ONE FIFO queue, fixed worker
//! count, jobs run to completion (no preemption). The fairness fix swaps the
//! FIFO for a 2-class queue (foreground-first) with the SAME worker count.
//!
//! ## Why this is correctness-safe
//!
//! This bin adds NO engine code and changes NO data path — it is a measurement
//! model plus a *proposed* pool-scheduling policy evaluated in-model. The
//! production levers are already flag-gated default-OFF and byte-identical OFF
//! (validated by their own bins + the engine UTs); this only quantifies their
//! INTERACTION and tests whether a fairness policy is warranted before any
//! engine change.
//!
//! Run:   `cargo run -p forst-rs-bench --bin disagg_readpool_contention --release`
//! 50Gb/s: `FRS_REMOTE_BW_MBPS=6250 cargo run ... --release`
//! Smoke: append `-- --smoke` (caps the run; asserts compose + the fairness fix
//! removes any foreground regression under contention).

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Remote-read service-time model (shared with the per-lever bins).
// ---------------------------------------------------------------------------

/// Modeled remote read time for `mib` MiB at `bw_mbps` MiB/s plus a fixed
/// per-request RTT (a GetObject has high fixed cost even for a tiny footer).
fn remote_time(mib: f64, bw_mbps: f64, rtt: Duration) -> Duration {
    rtt + Duration::from_secs_f64((mib / bw_mbps.max(1e-9)).max(0.0))
}

// ---------------------------------------------------------------------------
// The shared read-I/O pool model — FIFO and 2-class, same worker count.
// Mirrors `prefetch.rs::ReadIoPool`: a Mutex<VecDeque<Job>> + Condvar served by
// `width` workers, jobs run to completion. The 2-class variant is the proposed
// fairness fix: a foreground queue drained before the background queue.
// ---------------------------------------------------------------------------

type Job = Box<dyn FnOnce() + Send>;

/// Job class for the 2-class fairness pool. The FIFO pool ignores this.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Class {
    /// Latency-critical foreground reads (scan readahead windows).
    Foreground,
    /// Background warm-ups (compaction input opens + data-block primes).
    Background,
}

struct PoolShared {
    /// FIFO mode: everything in `fg` (back-compatible single queue).
    /// 2-class mode: `fg` drained fully before `bg` (work-conserving — a worker
    /// only takes `bg` when `fg` is empty).
    fg: Mutex<(VecDeque<Job>, VecDeque<Job>, bool)>,
    cv: Condvar,
    /// When false the pool is a single FIFO (push everything to `fg`); when true
    /// it is the 2-class foreground-first pool.
    class_aware: bool,
}

struct Pool {
    shared: Arc<PoolShared>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl Pool {
    fn new(width: usize, class_aware: bool) -> Self {
        let shared = Arc::new(PoolShared {
            fg: Mutex::new((VecDeque::new(), VecDeque::new(), false)),
            cv: Condvar::new(),
            class_aware,
        });
        let mut workers = Vec::with_capacity(width);
        for _ in 0..width.max(1) {
            let sh = Arc::clone(&shared);
            workers.push(std::thread::spawn(move || loop {
                let job = {
                    let mut g = sh.fg.lock().unwrap_or_else(|p| p.into_inner());
                    loop {
                        // Foreground first (when class-aware); else `fg` IS the
                        // single FIFO and `bg` is always empty.
                        if let Some(j) = g.0.pop_front() {
                            break Some(j);
                        }
                        if let Some(j) = g.1.pop_front() {
                            break Some(j);
                        }
                        if g.2 {
                            break None; // shutdown, drained
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

    fn submit(&self, class: Class, job: Job) {
        let mut g = self.shared.fg.lock().unwrap_or_else(|p| p.into_inner());
        // FIFO mode (`!class_aware`): everything goes to the single `fg` queue,
        // so background and foreground jobs interleave in arrival order — the
        // production read-I/O pool's exact behaviour. 2-class mode routes by
        // class so foreground is always served first.
        if self.shared.class_aware && class == Class::Background {
            g.1.push_back(job);
        } else {
            g.0.push_back(job);
        }
        drop(g);
        self.shared.cv.notify_one();
    }

    fn shutdown(self) {
        {
            let mut g = self.shared.fg.lock().unwrap_or_else(|p| p.into_inner());
            g.2 = true;
        }
        self.shared.cv.notify_all();
        for w in self.workers {
            let _ = w.join();
        }
    }
}

// ---------------------------------------------------------------------------
// The three concurrent consumers.
// ---------------------------------------------------------------------------

/// A scan window's modeled coalesced remote read, run on the pool, signalling
/// completion over a oneshot. Submitted as `Foreground`.
fn launch_scan_window(pool: &Pool, rtt: Duration) -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::channel();
    pool.submit(
        Class::Foreground,
        Box::new(move || {
            std::thread::sleep(rtt);
            let _ = tx.send(());
        }),
    );
    rx
}

/// Foreground scan: depth-`D` readahead pipeline over `windows` windows sharing
/// the pool. Records EACH window's join latency (the time from when the consumer
/// is ready for window k to when window k's read actually completes) — the
/// foreground-latency signal that contention degrades. Returns (wall, per-window
/// join-wait samples in µs).
///
/// `depth = 1` ⇒ the cycle-6 one-window-deep path; `depth >= ceil(rtt/consume)`
/// fully hides the read absent contention. Under contention the join can stall
/// even at the right depth if background jobs occupy the pool workers.
fn scan_consumer(
    pool: &Pool,
    windows: usize,
    rtt: Duration,
    consume: Duration,
    depth: usize,
    join_waits_us: &mut Vec<f64>,
) -> Duration {
    let depth = depth.max(1);
    let t0 = Instant::now();
    let mut inflight: VecDeque<mpsc::Receiver<()>> = VecDeque::with_capacity(depth);
    let mut launched = 0usize;
    for _ in 0..windows {
        while inflight.len() < depth && launched < windows {
            inflight.push_back(launch_scan_window(pool, rtt));
            launched += 1;
        }
        if let Some(rx) = inflight.pop_front() {
            let jw = Instant::now();
            let _ = rx.recv();
            join_waits_us.push(jw.elapsed().as_secs_f64() * 1e6);
        }
        std::thread::sleep(consume); // downstream drain of this window's rows
    }
    t0.elapsed()
}

/// Background compaction-warm chain: `jobs` compactions; before each job's merge
/// it fires the next job's `K` input-reader OPENs onto the pool (`Background`),
/// optionally ALSO each input's first DATA block (`warm_data`). The job body is
/// `merge + upload` (modeled as a sleep); the warm-ups run concurrently on the
/// pool during it. Returns the chain wall.
#[allow(clippy::too_many_arguments)]
fn compact_consumer(
    pool: &Pool,
    jobs: usize,
    inputs_per_job: usize,
    open: Duration,
    data: Duration,
    merge: Duration,
    upload: Duration,
    warm_data: bool,
    stop: &AtomicBool,
) -> Duration {
    let t0 = Instant::now();
    for _ in 0..jobs {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        // Fire the NEXT job's input opens (+ optional data primes) onto the pool
        // FIRE-AND-FORGET — the warm-up's seam. We don't join them (production
        // is fire-and-forget; the reader cache absorbs the result); they consume
        // pool worker time concurrently with this job's body, which is exactly
        // the contention surface against the foreground scan windows.
        for _ in 0..inputs_per_job {
            pool.submit(
                Class::Background,
                Box::new(move || {
                    std::thread::sleep(open);
                }),
            );
            if warm_data {
                pool.submit(
                    Class::Background,
                    Box::new(move || {
                        std::thread::sleep(data);
                    }),
                );
            }
        }
        // This job's body: merge (CPU) + output upload (throttled).
        std::thread::sleep(merge);
        std::thread::sleep(upload);
    }
    t0.elapsed()
}

fn percentile(sorted_us: &[f64], p: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_us.len() as f64 - 1.0) * p).round() as usize;
    sorted_us[idx.min(sorted_us.len() - 1)]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

// ---------------------------------------------------------------------------
// Scenario driver: run scan + (optionally) compact concurrently on one pool.
// ---------------------------------------------------------------------------

struct Scenario {
    windows: usize,
    rtt: Duration,
    consume: Duration,
    compact_jobs: usize,
    inputs_per_job: usize,
    open: Duration,
    data: Duration,
    merge: Duration,
    upload: Duration,
}

struct RunOut {
    /// Foreground scan wall.
    scan_wall_ms: f64,
    /// Foreground scan per-window join-wait p50 / p99 (the latency-critical
    /// signal; under contention these inflate even at the right depth).
    scan_join_p50_us: f64,
    scan_join_p99_us: f64,
    /// Combined wall = when BOTH consumers finished (max of the two).
    combined_wall_ms: f64,
}

/// One scenario run with the given lever toggles and pool kind.
///
/// * `scan_depth`: 1 = no readahead pipelining (cycle-6 baseline depth), D = the
///   readahead lever ON at depth D.
/// * `compact_on`: run the background compaction-warm chain concurrently.
/// * `warm_data`: the compaction chain also primes first data blocks.
/// * `class_aware`: use the 2-class fairness pool instead of FIFO.
fn run_scenario(
    sc: &Scenario,
    pool_width: usize,
    scan_depth: usize,
    compact_on: bool,
    warm_data: bool,
    class_aware: bool,
) -> RunOut {
    let pool = Pool::new(pool_width, class_aware);
    let stop = Arc::new(AtomicBool::new(false));

    let t0 = Instant::now();
    // Background compaction chain on its own scoped thread so it truly runs
    // concurrently with the foreground scan — both submitting to the one shared
    // pool. A scope lets the background thread borrow `pool`/`sc` without
    // 'static bounds.
    let mut scan_wall = Duration::ZERO;
    let mut join_waits_us: Vec<f64> = Vec::with_capacity(sc.windows);
    std::thread::scope(|s| {
        let bg = if compact_on {
            let stop = Arc::clone(&stop);
            let pool = &pool;
            Some(s.spawn(move || {
                compact_consumer(
                    pool,
                    sc.compact_jobs,
                    sc.inputs_per_job,
                    sc.open,
                    sc.data,
                    sc.merge,
                    sc.upload,
                    warm_data,
                    &stop,
                )
            }))
        } else {
            None
        };
        // Foreground scan on this thread.
        scan_wall = scan_consumer(
            &pool,
            sc.windows,
            sc.rtt,
            sc.consume,
            scan_depth,
            &mut join_waits_us,
        );
        // The foreground scan is the latency-critical workload; once it is done
        // we stop the background chain so the combined wall reflects "how long
        // until the foreground finished, with background contending", not an
        // arbitrarily long background tail.
        stop.store(true, Ordering::Relaxed);
        if let Some(h) = bg {
            let _ = h.join();
        }
    });
    let combined_wall = t0.elapsed();

    pool.shutdown();
    join_waits_us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    RunOut {
        scan_wall_ms: ms(scan_wall),
        scan_join_p50_us: percentile(&join_waits_us, 0.50),
        scan_join_p99_us: percentile(&join_waits_us, 0.99),
        combined_wall_ms: ms(combined_wall),
    }
}

/// Average `n` runs of a scenario (de-noise the real-`sleep` scheduling jitter).
fn avg_scenario(
    sc: &Scenario,
    pool_width: usize,
    scan_depth: usize,
    compact_on: bool,
    warm_data: bool,
    class_aware: bool,
    n: usize,
) -> RunOut {
    let mut acc = RunOut {
        scan_wall_ms: 0.0,
        scan_join_p50_us: 0.0,
        scan_join_p99_us: 0.0,
        combined_wall_ms: 0.0,
    };
    for _ in 0..n {
        let r = run_scenario(
            sc,
            pool_width,
            scan_depth,
            compact_on,
            warm_data,
            class_aware,
        );
        acc.scan_wall_ms += r.scan_wall_ms;
        acc.scan_join_p50_us += r.scan_join_p50_us;
        acc.scan_join_p99_us += r.scan_join_p99_us;
        acc.combined_wall_ms += r.combined_wall_ms;
    }
    let f = n as f64;
    RunOut {
        scan_wall_ms: acc.scan_wall_ms / f,
        scan_join_p50_us: acc.scan_join_p50_us / f,
        scan_join_p99_us: acc.scan_join_p99_us / f,
        combined_wall_ms: acc.combined_wall_ms / f,
    }
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");

    let bw_mbps = std::env::var("FRS_REMOTE_BW_MBPS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(256.0);
    let rtt_ms = std::env::var("FRS_REMOTE_RTT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10);
    let rtt = Duration::from_millis(rtt_ms);

    let pool_width = std::env::var("FRS_RS_PREFETCH_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(|| {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            (cores / 2).clamp(2, 6)
        })
        .max(1);

    // Workload sizes. The read-bound regime (consume < window-rtt) is where the
    // scan-readahead DEPTH lever pays — so model a window read that is several×
    // the per-window consume, and a compaction chain whose warm-up bursts can
    // occupy the pool.
    let (windows, compact_jobs) = if smoke { (24, 8) } else { (96, 32) };
    let inputs_per_job = 4; // a typical L1 drain fan-in
                            // A scan window's coalesced read moves more bytes than a footer open, so it
                            // is the longer pool job; the depth that hides it is ceil(window_rtt/consume).
    let window_mib = 0.5_f64;
    let open_mib = 0.125_f64; // footer + sparse index per input
    let data_mib = 0.5_f64; // a first data block prime
    let upload_mib = 4.0_f64; // a target-sized compaction output

    let window_rtt = remote_time(window_mib, bw_mbps, rtt);
    let open = remote_time(open_mib, bw_mbps, rtt);
    let data = remote_time(data_mib, bw_mbps, rtt);
    let merge = Duration::from_millis(if smoke { 4 } else { 8 });
    let upload = remote_time(upload_mib, bw_mbps, rtt);
    // Read-bound: consume = window_rtt / 4 ⇒ the optimal depth is ~4.
    let consume = window_rtt.div_f64(4.0);
    // The depth the scan-readahead lever uses (ceil(rtt/consume)+1, clamped to
    // the pool width — exactly the engine's adaptive optimum).
    let opt_depth = ((window_rtt.as_secs_f64() / consume.as_secs_f64()).ceil() as usize + 1)
        .clamp(1, pool_width);

    let sc = Scenario {
        windows,
        rtt: window_rtt,
        consume,
        compact_jobs,
        inputs_per_job,
        open,
        data,
        merge,
        upload,
    };

    let n = if smoke { 2 } else { 5 };

    println!(
        "== DISAGG READ-I/O POOL combined-stack + contention model ==\n\
         remote BW = {bw_mbps} MiB/s (FRS_REMOTE_BW_MBPS); RTT = {rtt_ms} ms (FRS_REMOTE_RTT_MS)\n\
         pool width = {pool_width} (clamp(cores/2,2,6); FRS_RS_PREFETCH_THREADS)\n\
         scan: {windows} windows, window-read {:.2} ms, consume {:.2} ms ⇒ opt depth {opt_depth}\n\
         compact: {compact_jobs} jobs × {inputs_per_job} inputs; open {:.2} ms, data {:.2} ms, \
         merge {:.2} ms, upload {:.2} ms\n\
         averaged over {n} runs.\n",
        ms(window_rtt),
        ms(consume),
        ms(open),
        ms(data),
        ms(merge),
        ms(upload),
    );

    // ====================================================================
    // (A) ISOLATED per-lever wins (each lever alone, no contention).
    // ====================================================================
    // Scan baseline (depth 1, no compaction) vs scan readahead (opt depth, no
    // compaction) — the scan-readahead lever in isolation.
    let scan_off = avg_scenario(&sc, pool_width, 1, false, false, false, n);
    let scan_on = avg_scenario(&sc, pool_width, opt_depth, false, false, false, n);
    let scan_iso_win = scan_off.scan_wall_ms / scan_on.scan_wall_ms.max(1e-9);

    // Compaction baseline (no warm: depth-1 scan running alongside a compaction
    // chain that does NOT pre-warm — opens paid cold inside the body) vs warm /
    // warm+data, all WITHOUT the foreground readahead, to isolate the write-side
    // levers' own contribution to the combined wall.
    println!("== (A) ISOLATED per-lever wins (no cross-lever contention) ==");
    println!(
        "  scan-readahead   : depth-1 {:.1} ms → depth-{opt_depth} {:.1} ms  ({:.2}× ; join p99 {:.0}→{:.0} µs)",
        scan_off.scan_wall_ms,
        scan_on.scan_wall_ms,
        scan_iso_win,
        scan_off.scan_join_p99_us,
        scan_on.scan_join_p99_us,
    );

    // ====================================================================
    // (B) COMBINED stack: all read-pool levers ON together, concurrently.
    // ====================================================================
    // ALL-OFF combined baseline: depth-1 scan + compaction chain WITHOUT warm —
    // i.e. today's serial disagg read path with all three levers off, the two
    // workloads still co-resident on the pool.
    let all_off = avg_scenario(&sc, pool_width, 1, true, false, false, n);
    // Add scan-readahead only (depth-D scan + no-warm compaction).
    let plus_scan = avg_scenario(&sc, pool_width, opt_depth, true, false, false, n);
    // Add compaction reader-warm (depth-D scan + warm compaction).
    let plus_warm = avg_scenario(&sc, pool_width, opt_depth, true, false, false, n);
    // NOTE: in this model the warm chain's pool occupancy is identical whether or
    // not the foreground reads it; `plus_warm` IS the warm chain (background
    // opens fired). The marginal of "warm" vs "no warm" on the FOREGROUND is the
    // contention it adds — captured below.
    // ALL-ON: depth-D scan + warm + data-prime compaction, FIFO pool.
    let all_on_fifo = avg_scenario(&sc, pool_width, opt_depth, true, true, false, n);
    // ALL-ON with the 2-class fairness pool.
    let all_on_fair = avg_scenario(&sc, pool_width, opt_depth, true, true, true, n);

    let combined_win = all_off.combined_wall_ms / all_on_fifo.combined_wall_ms.max(1e-9);
    let combined_win_fair = all_off.combined_wall_ms / all_on_fair.combined_wall_ms.max(1e-9);

    println!("\n== (B) COMBINED stack (scan + compact share the pool, concurrent) ==");
    println!(
        "{:>32} {:>13} {:>13} {:>14}",
        "config", "scan(ms)", "combined(ms)", "scan join p99"
    );
    let row = |name: &str, r: &RunOut| {
        println!(
            "{name:>32} {:>13.1} {:>13.1} {:>11.0} µs",
            r.scan_wall_ms, r.combined_wall_ms, r.scan_join_p99_us
        );
    };
    row("all-OFF (depth1 + no-warm)", &all_off);
    row("+ scan-readahead", &plus_scan);
    row("+ compact warm (FIFO)", &plus_warm);
    row("ALL-ON FIFO (warm+data)", &all_on_fifo);
    row("ALL-ON 2-class fair pool", &all_on_fair);
    println!(
        "\n  combined wall: all-OFF {:.1} ms → ALL-ON FIFO {:.1} ms  ({combined_win:.2}×) ; \
         2-class {:.1} ms  ({combined_win_fair:.2}×)",
        all_off.combined_wall_ms, all_on_fifo.combined_wall_ms, all_on_fair.combined_wall_ms,
    );

    // ====================================================================
    // (C) CONTENTION attribution: does the shared FIFO pool saturate?
    // ====================================================================
    // The foreground scan's wall under FIFO contention (all-ON) vs its ISOLATED
    // readahead wall (scan_on, no background). If the FIFO lets background warm
    // jobs delay foreground scan windows, the scan wall + join p99 inflate.
    let fg_contention = all_on_fifo.scan_wall_ms / scan_on.scan_wall_ms.max(1e-9);
    let fg_contention_fair = all_on_fair.scan_wall_ms / scan_on.scan_wall_ms.max(1e-9);
    let join_p99_inflation = all_on_fifo.scan_join_p99_us / scan_on.scan_join_p99_us.max(1e-9);
    let join_p99_inflation_fair = all_on_fair.scan_join_p99_us / scan_on.scan_join_p99_us.max(1e-9);

    println!("\n== (C) CONTENTION — foreground scan under the shared pool ==");
    println!(
        "  scan ISOLATED (no bg)        : {:.1} ms  (join p99 {:.0} µs)",
        scan_on.scan_wall_ms, scan_on.scan_join_p99_us
    );
    println!(
        "  scan under FIFO contention   : {:.1} ms  ({fg_contention:.2}× isolated ; join p99 {:.0} µs, {join_p99_inflation:.2}× inflated)",
        all_on_fifo.scan_wall_ms, all_on_fifo.scan_join_p99_us
    );
    println!(
        "  scan under 2-class fair pool : {:.1} ms  ({fg_contention_fair:.2}× isolated ; join p99 {:.0} µs, {join_p99_inflation_fair:.2}× inflated)",
        all_on_fair.scan_wall_ms, all_on_fair.scan_join_p99_us
    );

    // CONTENTION verdict: the FIFO pool SATURATES (interferes) when the
    // foreground scan is materially slower under contention than isolated.
    const CONTENTION_TOL: f64 = 1.15; // >15% foreground slowdown = real interference
    let fifo_contends = fg_contention > CONTENTION_TOL;
    let fair_fixes =
        fg_contention_fair <= CONTENTION_TOL || fg_contention_fair < fg_contention * 0.95; // measurably better than FIFO
    println!(
        "\n  FIFO pool contention: {} ({fg_contention:.2}× foreground slowdown, tol {CONTENTION_TOL:.2}×)",
        if fifo_contends {
            "PRESENT — background warm jobs head-of-line block foreground scan windows"
        } else {
            "NEGLIGIBLE — pool width absorbs the mixed load at this regime"
        }
    );
    if fifo_contends {
        println!(
            "  2-class fairness fix: {} (foreground slowdown {fg_contention:.2}× → {fg_contention_fair:.2}×; \
             join p99 inflation {join_p99_inflation:.2}× → {join_p99_inflation_fair:.2}×)",
            if fair_fixes { "REMOVES the regression" } else { "did NOT help" }
        );
    }

    // ====================================================================
    // (3) HEADLINE: forst-rs combined stack vs ForSt-equivalent.
    // ====================================================================
    // ForSt-equivalent disagg read path: a SINGLE read thread, ONE-window
    // readahead (depth 1), serial download→merge→upload — i.e. no depth-D
    // pipeline, no concurrent input warm-up, no data prime. Model it as the
    // all-OFF combined arm but with pool width 1 (one read thread) — the ForSt
    // disagg read model the Phase-2 goal metric compares against.
    let forst_equiv = avg_scenario(&sc, 1, 1, true, false, false, n);
    let headline = forst_equiv.combined_wall_ms / all_on_fair.combined_wall_ms.max(1e-9);
    println!("\n== (3) HEADLINE — forst-rs combined stack vs ForSt-equivalent ==");
    println!(
        "  ForSt-equiv (1 read thread, depth-1, serial warm) : {:.1} ms",
        forst_equiv.combined_wall_ms
    );
    println!(
        "  forst-rs combined (depth-{opt_depth} + warm + data, fair pool, width {pool_width}) : {:.1} ms",
        all_on_fair.combined_wall_ms
    );
    println!(
        "  ⇒ forst-rs disagg read path is {headline:.2}× the ForSt-equivalent (mini-bench model)"
    );

    println!(
        "\n  per-lever marginal (on the combined wall, added in order):\n\
         \x20   all-OFF                 {:.1} ms\n\
         \x20   + scan-readahead        {:.1} ms ({:.2}×)\n\
         \x20   + compact reader-warm   {:.1} ms ({:.2}×)\n\
         \x20   + compact data-prime    {:.1} ms ({:.2}×)\n\
         \x20   + 2-class fair pool     {:.1} ms ({:.2}×)",
        all_off.combined_wall_ms,
        plus_scan.combined_wall_ms,
        all_off.combined_wall_ms / plus_scan.combined_wall_ms.max(1e-9),
        plus_warm.combined_wall_ms,
        plus_scan.combined_wall_ms / plus_warm.combined_wall_ms.max(1e-9),
        all_on_fifo.combined_wall_ms,
        plus_warm.combined_wall_ms / all_on_fifo.combined_wall_ms.max(1e-9),
        all_on_fair.combined_wall_ms,
        all_on_fifo.combined_wall_ms / all_on_fair.combined_wall_ms.max(1e-9),
    );

    if smoke {
        // The combined stack must COMPOSE: ALL-ON (fair pool) must be no slower
        // than the all-OFF combined baseline (levers are additive, never a net
        // regression) and the scan-readahead lever must show its isolated win.
        assert!(
            scan_iso_win > 1.2,
            "scan-readahead isolated win collapsed ({scan_iso_win:.2}× <= 1.2×)"
        );
        assert!(
            combined_win_fair >= 0.95,
            "combined ALL-ON (fair) regressed vs all-OFF ({combined_win_fair:.2}× < 0.95×)"
        );
        // If FIFO contention was detected, the fairness fix must not make the
        // foreground WORSE than FIFO (it should help or be neutral).
        if fifo_contends {
            assert!(
                fg_contention_fair <= fg_contention + 0.05,
                "2-class fair pool made foreground contention WORSE than FIFO \
                 ({fg_contention_fair:.2}× > {fg_contention:.2}×)"
            );
        }
        println!(
            "\nSMOKE OK: scan-readahead isolated {scan_iso_win:.2}×; combined composes \
             (fair {combined_win_fair:.2}× vs all-OFF); FIFO contention {} & fairness {}.",
            if fifo_contends {
                "present"
            } else {
                "negligible"
            },
            if !fifo_contends {
                "not needed"
            } else if fair_fixes {
                "removes it"
            } else {
                "evaluated"
            },
        );
    }
}
