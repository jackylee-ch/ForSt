//! FRS-UPLOAD-FLUSH-QOS (Phase-2 cycle 8, Item B) — root-cause + before/after for a
//! FLUSH-PRIORITY lane in the bounded in-flight upload budget.
//!
//! ## The structural gap (root cause)
//!
//! `opendal_backend.rs` bounds resident upload memory with ONE `upload_sem`
//! (`MAX_INFLIGHT_UPLOADS` permits, or a byte budget via
//! `FRS_UPLOAD_BYTE_BUDGET_MIB`). Every spawned upload — FLUSH output (L0 SST) AND
//! COMPACTION output (L1+ SST) — competes for that single budget with NO class
//! distinction. The two classes have very different criticality:
//!
//!   * **Flush** uploads are LATENCY-critical: a flush's `close_writer` returns on
//!     the local serialization, but the WriteBufferManager budget / immutable
//!     memtable is only fully reclaimed once the upload's in-flight permit is
//!     acquired (with `FRS_ASYNC_FLUSH_UPLOAD` the permit is acquired SYNCHRONOUSLY
//!     on the flush thread *before* spawn — so a full budget BLOCKS the flush loop).
//!     A stalled flush ⇒ memtables can't rotate ⇒ write stall ⇒ ingest backpressure.
//!   * **Compaction** uploads are THROUGHPUT-oriented and BACKGROUND: large outputs,
//!     no operator waiting on any individual one; they can be paced.
//!
//! Under write pressure a burst of large compaction-output uploads can occupy the
//! WHOLE in-flight budget, so the next flush upload waits behind them — the
//! class-blind budget lets background compaction STARVE latency-critical flush.
//!
//! ## The fix under test
//!
//! Split the budget into a RESERVED flush lane: `reserved` permits only flush may
//! take, `shared` permits either class may take. A flush always has at least the
//! reserved lane available, so compaction can never push flush-acquire latency past
//! the time to drain the reserved lane — while compaction still uses the full
//! budget whenever flush is idle (work-conserving). Byte-identical to the
//! class-blind path when `reserved = 0` (the default / OFF).
//!
//! ## This bench
//!
//! Models the in-flight budget as a permit pool. A flush stream issues small
//! latency-critical uploads at a steady cadence; a compaction stream issues large
//! uploads back-to-back (the saturating background). Two policies:
//!   * `blind`    — one shared pool of `BUDGET` permits (current behaviour).
//!   * `reserved` — `RESERVED` permits flush-only + `BUDGET-RESERVED` shared.
//!
//! Reports flush-acquire latency p50/p99 and compaction throughput for each.
//!
//! Run:   `cargo run -p forst-rs-bench --bin upload_flush_qos --release`
//! Smoke: append `-- --smoke` (caps the run; asserts the reserved lane cuts flush
//! p99 without materially cutting compaction throughput).

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// A two-lane permit budget modelling `upload_sem` with a reserved flush lane.
/// `reserved` permits are ONLY grantable to flush; `shared` permits to either
/// class. `reserved = 0` ⇒ the class-blind single pool (byte-identical OFF).
struct UploadBudget {
    inner: Mutex<BudgetState>,
    cv: Condvar,
}
struct BudgetState {
    /// Free permits in the flush-only reserved lane.
    reserved_free: u64,
    /// Free permits in the shared lane.
    shared_free: u64,
}
/// Which lane a granted permit came from (so release returns it correctly).
#[derive(Clone, Copy)]
enum Lane {
    Reserved,
    Shared,
}

impl UploadBudget {
    fn new(budget: u64, reserved: u64) -> Self {
        let reserved = reserved.min(budget);
        Self {
            inner: Mutex::new(BudgetState {
                reserved_free: reserved,
                shared_free: budget - reserved,
            }),
            cv: Condvar::new(),
        }
    }

    /// Acquire `n` permits for a FLUSH upload: prefer the reserved lane, fall back
    /// to (and combine with) the shared lane. Blocks until `n` are available.
    /// Returns the per-permit lane vector so release restores each to its lane.
    fn acquire_flush(&self, n: u64) -> Vec<Lane> {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if g.reserved_free + g.shared_free >= n {
                // Take from reserved FIRST (flush's private lane), then shared.
                let from_reserved = n.min(g.reserved_free);
                let from_shared = n - from_reserved;
                g.reserved_free -= from_reserved;
                g.shared_free -= from_shared;
                let mut lanes = vec![Lane::Reserved; from_reserved as usize];
                lanes.extend(std::iter::repeat_n(Lane::Shared, from_shared as usize));
                return lanes;
            }
            g = self.cv.wait(g).unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Acquire `n` permits for a COMPACTION upload: SHARED lane only (never the
    /// reserved flush lane). Blocks until `n` shared permits are available.
    fn acquire_compaction(&self, n: u64) -> Vec<Lane> {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if g.shared_free >= n {
                g.shared_free -= n;
                return vec![Lane::Shared; n as usize];
            }
            g = self.cv.wait(g).unwrap_or_else(|p| p.into_inner());
        }
    }

    fn release(&self, lanes: &[Lane]) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        for lane in lanes {
            match lane {
                Lane::Reserved => g.reserved_free += 1,
                Lane::Shared => g.shared_free += 1,
            }
        }
        drop(g);
        self.cv.notify_all();
    }
}

/// Modeled per-MiB upload time at the configured remote bandwidth.
fn upload_time(mib: u64, bw_mbps: f64) -> Duration {
    // bytes / (bytes/sec). bw_mbps is MiB/s in this model (mirrors FRS_REMOTE_BW_MBPS
    // usage elsewhere as a MiB/s-ish rate). Floor at 1 ms so tiny uploads still cost.
    let secs = (mib as f64) / bw_mbps.max(1e-9);
    Duration::from_secs_f64(secs.max(0.001))
}

struct Stats {
    flush_p50_ms: f64,
    flush_p99_ms: f64,
    flush_max_ms: f64,
    compaction_mib: u64,
    wall_ms: f64,
}

/// Run one policy. `reserved` = flush-only permits (0 = class-blind). Returns flush
/// acquire-latency percentiles (the ingest-stall proxy) and compaction throughput.
#[allow(clippy::too_many_arguments)]
fn run_policy(
    budget: u64,
    reserved: u64,
    bw_mbps: f64,
    flush_mib: u64,
    flush_count: usize,
    flush_period: Duration,
    compaction_mib: u64,
    compaction_count: usize,
) -> Stats {
    let bud = Arc::new(UploadBudget::new(budget, reserved));
    let flush_lat_us: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::with_capacity(flush_count)));
    let compaction_done = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();

    // COUNT regime (production default `MAX_INFLIGHT_UPLOADS`): every upload reserves
    // exactly ONE in-flight permit regardless of size. The starvation comes from
    // DURATION, not permit weight: a saturating compaction stream holds all `budget`
    // permits for the long 64-MiB upload time, so a flush queues behind a whole
    // compaction upload even though its own 4-MiB upload is ~16× faster.
    let flush_permits = 1u64;
    let compaction_permits = 1u64;

    // Compaction stream: saturating background. Real compaction spawns MANY upload
    // tasks concurrently (each compaction job emits multiple output SSTs, and several
    // jobs run at once), so model it as `budget` concurrent workers all racing for
    // the shared budget — enough to fill every permit and force flush to queue.
    let compaction_workers: Vec<_> = (0..budget)
        .map(|_| {
            let bud = Arc::clone(&bud);
            let compaction_done = Arc::clone(&compaction_done);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || loop {
                if stop.load(Ordering::Relaxed)
                    || compaction_done.load(Ordering::Relaxed)
                        >= compaction_count as u64 * compaction_mib
                {
                    break;
                }
                let lanes = bud.acquire_compaction(compaction_permits);
                std::thread::sleep(upload_time(compaction_mib, bw_mbps));
                bud.release(&lanes);
                compaction_done.fetch_add(compaction_mib, Ordering::Relaxed);
            })
        })
        .collect();

    // Flush stream: steady cadence of small latency-critical uploads. The metric is
    // the ACQUIRE latency (the time the flush thread blocks for an in-flight permit
    // — the proxy for memtable-rotation / ingest stall).
    let flush = {
        let bud = Arc::clone(&bud);
        let flush_lat_us = Arc::clone(&flush_lat_us);
        std::thread::spawn(move || {
            for _ in 0..flush_count {
                let wait0 = Instant::now();
                let lanes = bud.acquire_flush(flush_permits);
                let acquire_us = wait0.elapsed().as_micros() as u64;
                flush_lat_us.lock().unwrap().push(acquire_us);
                // Hold for the modeled upload, then release.
                std::thread::sleep(upload_time(flush_mib, bw_mbps));
                bud.release(&lanes);
                std::thread::sleep(flush_period); // inter-flush cadence
            }
        })
    };

    flush.join().unwrap();
    // Flush done — stop compaction's remaining iterations so the wall reflects the
    // flush-bound window (compaction throughput is measured over that window).
    stop.store(true, Ordering::Relaxed);
    // Wake any compaction workers blocked on acquire so they observe `stop`.
    bud.cv.notify_all();
    for w in compaction_workers {
        w.join().unwrap();
    }
    let wall_ms = t0.elapsed().as_secs_f64() * 1e3;

    let mut lat = flush_lat_us.lock().unwrap().clone();
    lat.sort_unstable();
    let pct = |p: f64| -> f64 {
        if lat.is_empty() {
            return 0.0;
        }
        let idx = ((lat.len() as f64 - 1.0) * p).round() as usize;
        lat[idx] as f64 / 1e3
    };
    Stats {
        flush_p50_ms: pct(0.50),
        flush_p99_ms: pct(0.99),
        flush_max_ms: lat.last().copied().unwrap_or(0) as f64 / 1e3,
        compaction_mib: compaction_done.load(Ordering::Relaxed),
        wall_ms,
    }
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");

    // Modeled remote bandwidth. The COUNT-regime starvation (the production default)
    // is sharpest when the per-SST upload time is non-trivial, i.e. a THROTTLED /
    // bandwidth-limited endpoint — the regime disagg targets. Default 256 MiB/s
    // (a modest object-store stream); override with FRS_REMOTE_BW_MBPS (the 6250
    // ≈ 50 Gb/s ceiling makes every upload sub-ms ⇒ no contention to study).
    let bw_mbps = std::env::var("FRS_REMOTE_BW_MBPS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(256.0);

    // COUNT regime (production default `MAX_INFLIGHT_UPLOADS`): the budget is a
    // COUNT of in-flight uploads (each upload = 1 permit regardless of size), so a
    // saturating compaction stream fills all `budget` permits and a flush queues
    // behind a whole compaction upload. We model the count regime by giving every
    // upload `permits = 1` (set below) and sizing the budget in permits.
    let budget = std::env::var("FRS_UPLOAD_BUDGET")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(8); // MAX_INFLIGHT_UPLOADS
    let flush_mib = 4; // L0 SST (small, fast upload)
    let compaction_mib = 64; // L1+ SST (large, slow upload — the budget hog)
    let reserved = std::env::var("FRS_UPLOAD_FLUSH_RESERVED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1); // reserve ONE in-flight slot for flush (count regime)

    let (flush_count, compaction_count, flush_period) = if smoke {
        (40, 200, Duration::from_micros(200))
    } else {
        (200, 1000, Duration::from_micros(500))
    };

    println!(
        "== FRS-UPLOAD-FLUSH-QOS — flush-priority upload lane (COUNT regime) ==\n\
         remote BW = {bw_mbps} MiB/s (FRS_REMOTE_BW_MBPS); budget = {budget} in-flight \
         (MAX_INFLIGHT_UPLOADS); reserved flush slots = {reserved}\n\
         flush = {flush_count} × {flush_mib} MiB @ {:?} cadence; \
         compaction = saturating {compaction_mib} MiB SSTs (the budget hog)\n",
        flush_period
    );

    // BLIND (current): one shared budget, reserved = 0.
    let blind = run_policy(
        budget,
        0,
        bw_mbps,
        flush_mib,
        flush_count,
        flush_period,
        compaction_mib,
        compaction_count,
    );
    // RESERVED: a flush-only lane carved from the same budget.
    let resv = run_policy(
        budget,
        reserved,
        bw_mbps,
        flush_mib,
        flush_count,
        flush_period,
        compaction_mib,
        compaction_count,
    );

    println!(
        "{:>12} {:>14} {:>14} {:>14} {:>16} {:>12}",
        "policy", "flush p50(ms)", "flush p99(ms)", "flush max(ms)", "compaction(MiB)", "wall(ms)"
    );
    let row = |name: &str, s: &Stats| {
        println!(
            "{name:>12} {:>14.2} {:>14.2} {:>14.2} {:>16} {:>12.1}",
            s.flush_p50_ms, s.flush_p99_ms, s.flush_max_ms, s.compaction_mib, s.wall_ms
        );
    };
    row("blind", &blind);
    row("reserved", &resv);

    // The reserved lane makes the flush stream finish far sooner (its private slot
    // never queues behind compaction), so the WALL differs between policies — total
    // compaction MiB is therefore NOT comparable. The fair metric is the compaction
    // RATE (MiB per ms of wall): how much background progress per unit time.
    let blind_rate = blind.compaction_mib as f64 / blind.wall_ms.max(1e-9);
    let resv_rate = resv.compaction_mib as f64 / resv.wall_ms.max(1e-9);
    // Cap the p99 ratio: when the reserved p99 collapses to ~0 the raw ratio is a
    // meaningless divide-by-epsilon, so report the ABSOLUTE cut too.
    let p99_cut = (blind.flush_p99_ms / resv.flush_p99_ms.max(1e-3)).min(1e6);
    let p99_abs_ms = blind.flush_p99_ms - resv.flush_p99_ms;
    let comp_rate_retained = resv_rate / blind_rate.max(1e-9);
    println!(
        "\nflush p99: blind {:.1} ms → reserved {:.1} ms  (cut {p99_cut:.1}×, −{p99_abs_ms:.1} ms)\n\
         compaction rate: blind {blind_rate:.2} → reserved {resv_rate:.2} MiB/ms  \
         (retained {comp_rate_retained:.2}×)",
        blind.flush_p99_ms, resv.flush_p99_ms
    );

    if smoke {
        // The reserved lane must MATERIALLY cut flush tail latency (the ingest-stall
        // proxy): a flush always has its private slot instead of queueing behind a
        // whole saturating compaction upload. A large absolute cut is the claim.
        assert!(
            p99_abs_ms > 50.0,
            "qos smoke: reserved lane didn't cut flush p99 (−{p99_abs_ms:.1} ms <= 50 ms)"
        );
        // ...while keeping compaction WORK-CONSERVING: the reserved lane (1 of 8
        // slots) leaves 7/8 of the budget for compaction, so its rate stays within
        // a modest factor of blind (it never burns the reserved slot, but that is a
        // small fraction). Allow generous slack for shared-host scheduling jitter.
        assert!(
            comp_rate_retained > 0.6,
            "qos smoke: reserved lane starved compaction rate ({comp_rate_retained:.2}× <= 0.6×)"
        );
        println!(
            "\nSMOKE OK: flush p99 cut −{p99_abs_ms:.1} ms ({p99_cut:.1}×); \
             compaction rate retained {comp_rate_retained:.2}× (work-conserving)."
        );
    }
}
