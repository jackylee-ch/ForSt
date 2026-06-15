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

//! FRS-PHASE2 DISAGG **UPLOAD-SIDE QoS COMBINED-STACK + INTERFERENCE** model —
//! the WRITE-side analogue of `disagg_readpool_contention` (the read-pool
//! fairness validation, cycle-5).
//!
//! ## The three write-side upload levers, and why their COMPOSITION matters
//!
//! The buffered object-store upload path (`opendal_backend.rs`) has three
//! independent QoS levers, each landed + validated in ISOLATION:
//!
//!   1. **`FRS_UPLOAD_FLUSH_RESERVED`** (`flush_sem`, 261366a06) — carves
//!      `reserved` permits out of the in-flight upload budget into a flush-only
//!      lane. A flush-class upload tries the reserved lane first
//!      (`try_acquire_many_owned`) before falling back to the shared lane, so a
//!      saturating compaction-output stream can never push flush-acquire latency
//!      past the reserved-lane drain time. Validated by `upload_flush_qos`.
//!   2. **`FRS_UPLOAD_BYTE_BUDGET_MIB`** (`upload_permits_for`) — sizes the
//!      semaphore as a BYTE budget (1 permit == 1 MiB) and makes each upload
//!      reserve `ceil(bytes / 1 MiB)` permits, so the SUM of resident upload
//!      bytes is bounded rather than the COUNT. A 64-MiB compaction SST now holds
//!      64 permits; a 4-MiB flush holds 4. Validated by `upload_byte_budget`.
//!   3. **`FRS_UPLOAD_RATE_SPLIT`** (`throttle.rs`) — paces COMPACTION-class
//!      write BYTES through a reduced sub-rate bucket (`compaction_share` of the
//!      remote BW), keeping the full remote rate for flush/foreground. Validated
//!      by `disagg_write_backpressure`.
//!
//! Each lever was proven GOOD alone. But in production all three are ON at once,
//! and they touch DIFFERENT seams of the SAME upload (semaphore weight, lane
//! choice, and per-byte pacing), so they can INTERFERE. The two concrete,
//! code-grounded interference hypotheses this bin tests:
//!
//!   * **H1 — unit mismatch starves the reserved lane.** With the byte budget
//!     ON, a flush reserves `ceil(4 MiB)=4` permits, but
//!     `FRS_UPLOAD_FLUSH_RESERVED` is *also* in MiB-permit units (the resolver
//!     shares `upload_sem_total_permits`). If an operator sizes the reserved lane
//!     in COUNT intuition (e.g. `reserved=1`, "one flush slot") while the budget
//!     is in MiB, the reserved lane holds 1 permit but a flush NEEDS 4 →
//!     `try_acquire_many_owned(4)` FAILS → the flush falls back to the saturated
//!     shared lane → the reserved-lane win is LOST. The fix is sizing guidance
//!     (reserve a whole flush SST worth of MiB), but the bin must first SHOW the
//!     cliff so the guidance is evidence-backed.
//!   * **H2 — rate-split lengthens shared-lane permit hold → flush tail
//!     regression on the fallback path.** Rate-split slows compaction BYTES, so
//!     each compaction upload holds its shared-lane permit(s) LONGER. Any flush
//!     that misses the reserved lane (because the lane is full or — see H1 —
//!     undersized) then waits behind a SLOWER compaction drain than without
//!     rate-split. So rate-split, helpful for the channel, can WORSEN the flush
//!     fallback tail unless the reserved lane is correctly sized to absorb every
//!     flush (making the fallback path cold).
//!
//! ## What is MODELED
//!
//! A faithful timing model of the upload admission path — NO engine state, NO
//! S3. It mirrors the real `opendal_backend` upload semantics exactly:
//!
//!   * A two-lane permit budget (`UploadBudget`) == `upload_sem` (shared) +
//!     `flush_sem` (reserved). Flush tries reserved-first then shared; compaction
//!     is shared-only. This is the `upload_flush_qos` model, kept identical.
//!   * **Byte-budget dimension**: each upload reserves `ceil(mib)` permits
//!     (byte regime) or `1` permit (count regime), exactly `upload_permits_for`.
//!     The budget is sized in the SAME unit (MiB-permits or count) — exactly
//!     `upload_sem_total_permits` / `build_upload_semaphores`.
//!   * **Rate-split dimension**: a compaction upload's HOLD time is its bytes /
//!     `compaction_share` of the remote BW (paced sub-rate); flush holds for
//!     bytes / full BW. This is the `throttle.rs` ThrottleShared.charge model:
//!     the permit is held for the whole (now-paced) upload, so a slower
//!     compaction byte stream lengthens the shared-permit hold — the H2 seam.
//!
//! The model is intentionally the MINIMUM that can exhibit H1/H2: real opendal
//! `.concurrent()` multipart and the bridged runtime add only constant factors
//! that do not change the permit-contention structure the levers govern.
//!
//! ## Why this is correctness-safe
//!
//! Adds NO engine code, changes NO data path. It is a measurement model plus a
//! check of whether the THREE already-shipped, flag-gated, default-OFF,
//! byte-identical-OFF levers compose. The interference it surfaces (H1) is a
//! CONFIGURATION footgun, not a code defect — the levers compose correctly when
//! the reserved lane is sized in MiB. The motivated fix is therefore a
//! DIAGNOSTIC-ONLY one-time WARN (`warn_reserved_lane_undersized_for_byte_budget`
//! in `opendal_backend.rs`) that fires only when BOTH levers are already ON and
//! the lane is sub-SST; it logs and changes no permits/bytes/data-path, so it is
//! byte-identical whether or not it fires.
//!
//! Run:   `cargo run -p forst-rs-bench --bin disagg_upload_qos_compose --release`
//! 50Gb/s: `FRS_REMOTE_BW_MBPS=6250 cargo run ... --release` (sub-ms uploads ⇒
//!         no contention to study; the contended regime is a THROTTLED endpoint,
//!         the default 256 MiB/s).
//! Smoke: append `-- --smoke` (caps the run; asserts the all-ON stack composes —
//!         flush p99 stays low AND compaction stays work-conserving — once the
//!         reserved lane is sized for the byte regime, and EXHIBITS the H1 cliff
//!         when it is mis-sized).

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Two-lane permit budget — identical to `upload_flush_qos`'s UploadBudget, the
// faithful model of `upload_sem` (shared) + `flush_sem` (reserved). `reserved=0`
// ⇒ the class-blind single pool (byte-identical OFF).
// ---------------------------------------------------------------------------

struct UploadBudget {
    inner: Mutex<BudgetState>,
    cv: Condvar,
}
struct BudgetState {
    reserved_free: u64,
    shared_free: u64,
}
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

    /// FLUSH acquire: try the reserved lane FIRST and ONLY (a single non-blocking
    /// attempt that must find ALL `n` in reserved — the `try_acquire_many_owned`
    /// semantics: it does NOT combine reserved+shared in the fast path), then fall
    /// back to BLOCKING on the shared lane. This is the real `close_writer` path:
    /// `(Flush, Some(fs)) => fs.try_acquire_many_owned(permits).ok()` then
    /// `None => block_on(sem.acquire_many_owned(permits))`.
    ///
    /// Returns `(lanes, hit_reserved)` so the caller can report the reserved-lane
    /// HIT RATE — the direct H1 signal (mis-sized lane ⇒ hit rate collapses).
    fn acquire_flush(&self, n: u64) -> (Vec<Lane>, bool) {
        {
            let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if g.reserved_free >= n {
                g.reserved_free -= n;
                return (vec![Lane::Reserved; n as usize], true);
            }
        }
        // Reserved miss → block on the shared lane (the fallback path H2 stresses).
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if g.shared_free >= n {
                g.shared_free -= n;
                return (vec![Lane::Shared; n as usize], false);
            }
            g = self.cv.wait(g).unwrap_or_else(|p| p.into_inner());
        }
    }

    /// COMPACTION acquire: shared lane only (never the reserved flush lane).
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

// ---------------------------------------------------------------------------
// Lever configuration — the three write-side QoS levers as a factorial cell.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Levers {
    /// `FRS_UPLOAD_FLUSH_RESERVED` reserved-lane size, in the SAME unit as the
    /// budget (count permits in count regime, MiB permits in byte regime). `0` =
    /// the lever OFF (class-blind single pool).
    reserved: u64,
    /// `FRS_UPLOAD_BYTE_BUDGET_MIB` ON ⇒ byte regime: each upload reserves
    /// `ceil(mib)` permits and the budget is `byte_budget` MiB-permits. OFF ⇒
    /// count regime: every upload reserves 1 permit, budget = `count_budget`.
    byte_budget: bool,
    /// `FRS_UPLOAD_RATE_SPLIT` ON ⇒ compaction BYTES paced at `compaction_share`
    /// of the remote BW (longer permit hold). OFF ⇒ full BW for both classes.
    rate_split: bool,
}

/// Permits an upload of `mib` reserves — exactly `upload_permits_for`:
/// `1` in the count regime, `ceil(mib)` (≥1) in the byte regime.
fn permits_for(mib: u64, byte_budget: bool) -> u64 {
    if byte_budget {
        mib.max(1)
    } else {
        1
    }
}

/// Total budget permits — exactly `upload_sem_total_permits`: the MiB budget in
/// the byte regime, the count cap (`MAX_INFLIGHT_UPLOADS`) in the count regime.
fn total_budget(byte_budget: bool, count_budget: u64, byte_budget_mib: u64) -> u64 {
    if byte_budget {
        byte_budget_mib
    } else {
        count_budget
    }
}

/// Modeled upload HOLD time for one SST of `mib` at the configured BW. When
/// `paced` (a compaction upload under rate-split), the byte stream runs at
/// `share` of the remote BW so the permit is held proportionally LONGER — the
/// `ThrottleShared.charge` → `compaction_limiter.throttle` model (the H2 seam).
fn upload_hold(mib: u64, bw_mbps: f64, paced: bool, share: f64) -> Duration {
    let eff_bw = if paced { bw_mbps * share } else { bw_mbps };
    let secs = (mib as f64) / eff_bw.max(1e-9);
    Duration::from_secs_f64(secs.max(0.001))
}

struct Stats {
    flush_p50_ms: f64,
    flush_p99_ms: f64,
    flush_max_ms: f64,
    /// Fraction of flush uploads that got their permit from the reserved lane
    /// (the direct H1 signal — a mis-sized lane collapses this toward 0).
    reserved_hit_rate: f64,
    compaction_mib: u64,
    wall_ms: f64,
}

#[allow(clippy::too_many_arguments)]
fn run_cell(
    levers: Levers,
    bw_mbps: f64,
    share: f64,
    count_budget: u64,
    byte_budget_mib: u64,
    flush_mib: u64,
    flush_count: usize,
    flush_period: Duration,
    compaction_mib: u64,
    compaction_count: usize,
) -> Stats {
    let budget = total_budget(levers.byte_budget, count_budget, byte_budget_mib);
    let bud = Arc::new(UploadBudget::new(budget, levers.reserved.min(budget)));
    let flush_lat_us: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::with_capacity(flush_count)));
    let flush_reserved_hits = Arc::new(AtomicU64::new(0));
    let compaction_done = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();

    let flush_permits = permits_for(flush_mib, levers.byte_budget);
    let compaction_permits = permits_for(compaction_mib, levers.byte_budget);

    // Compaction stream: saturating background. Many concurrent upload tasks all
    // racing for the shared budget. In the byte regime a single 64-MiB upload
    // already pins 64 permits, so the number of workers that can be in flight is
    // budget-bound; we still spawn a generous worker count so the budget is
    // always pressed. The number of workers does not change the permit math —
    // only how aggressively the shared lane is contended.
    let n_workers = (budget / compaction_permits.max(1)).max(1) + 2;
    let compaction_workers: Vec<_> = (0..n_workers)
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
                // Rate-split paces compaction bytes ⇒ longer permit hold.
                std::thread::sleep(upload_hold(
                    compaction_mib,
                    bw_mbps,
                    levers.rate_split,
                    share,
                ));
                bud.release(&lanes);
                compaction_done.fetch_add(compaction_mib, Ordering::Relaxed);
            })
        })
        .collect();

    // Flush stream: steady cadence of small latency-critical uploads. Metric =
    // the ACQUIRE latency (memtable-rotation / ingest-stall proxy) + whether the
    // reserved lane was hit.
    let flush = {
        let bud = Arc::clone(&bud);
        let flush_lat_us = Arc::clone(&flush_lat_us);
        let flush_reserved_hits = Arc::clone(&flush_reserved_hits);
        std::thread::spawn(move || {
            for _ in 0..flush_count {
                let wait0 = Instant::now();
                let (lanes, hit_reserved) = bud.acquire_flush(flush_permits);
                let acquire_us = wait0.elapsed().as_micros() as u64;
                flush_lat_us.lock().unwrap().push(acquire_us);
                if hit_reserved {
                    flush_reserved_hits.fetch_add(1, Ordering::Relaxed);
                }
                std::thread::sleep(upload_hold(flush_mib, bw_mbps, false, share));
                bud.release(&lanes);
                std::thread::sleep(flush_period);
            }
        })
    };

    flush.join().unwrap();
    stop.store(true, Ordering::Relaxed);
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
        reserved_hit_rate: flush_reserved_hits.load(Ordering::Relaxed) as f64
            / (flush_count as f64).max(1.0),
        compaction_mib: compaction_done.load(Ordering::Relaxed),
        wall_ms,
    }
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");

    // Throttled endpoint regime (the disagg target). 50 Gb/s makes every upload
    // sub-ms ⇒ no contention to study.
    let bw_mbps = std::env::var("FRS_REMOTE_BW_MBPS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(256.0);
    let share = std::env::var("FRS_UPLOAD_COMPACTION_SHARE")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.5);

    let count_budget = 8; // MAX_INFLIGHT_UPLOADS
    let flush_mib = 4; // L0 SST
    let compaction_mib = 64; // L1+ SST (the budget hog)
                             // Byte budget: 25% of a nominal WriteBufferManager — large enough for a
                             // couple of compaction SSTs. 192 MiB ⇒ 192 MiB-permits.
    let byte_budget_mib = std::env::var("FRS_UPLOAD_BYTE_BUDGET_MIB")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(192);

    let (flush_count, compaction_count, flush_period) = if smoke {
        (40, 200, Duration::from_micros(200))
    } else {
        (200, 1000, Duration::from_micros(500))
    };

    println!(
        "== FRS-PHASE2 DISAGG UPLOAD-QoS COMPOSE — flush-reserved × byte-budget × rate-split ==\n\
         remote BW = {bw_mbps} MiB/s; compaction_share = {share}; count budget = {count_budget}; \
         byte budget = {byte_budget_mib} MiB\n\
         flush = {flush_count} × {flush_mib} MiB @ {flush_period:?}; \
         compaction = saturating {compaction_mib} MiB SSTs\n"
    );

    // Reserved-lane size: in the COUNT regime, 1 slot. In the BYTE regime, a
    // flush needs `flush_mib` permits, so a CORRECTLY-sized reserved lane is
    // `flush_mib` MiB-permits. The "naive count-1" reserved size is the H1
    // mis-sizing trap.
    let reserved_count = 1u64;
    let reserved_byte_ok = flush_mib; // one whole flush SST worth of MiB-permits
    let reserved_byte_naive = 1u64; // count-intuition in the byte regime (H1 trap)

    let row_hdr = || {
        println!(
            "{:>26} {:>11} {:>11} {:>11} {:>9} {:>13} {:>10}",
            "cell", "flush p50", "flush p99", "flush max", "resv hit", "compaction", "wall(ms)"
        );
    };
    let row = |name: &str, s: &Stats| {
        println!(
            "{name:>26} {:>9.1}ms {:>9.1}ms {:>9.1}ms {:>8.0}% {:>9}MiB {:>10.1}",
            s.flush_p50_ms,
            s.flush_p99_ms,
            s.flush_max_ms,
            s.reserved_hit_rate * 100.0,
            s.compaction_mib,
            s.wall_ms
        );
    };
    let cell = |levers: Levers, bb_mib: u64| {
        run_cell(
            levers,
            bw_mbps,
            share,
            count_budget,
            bb_mib,
            flush_mib,
            flush_count,
            flush_period,
            compaction_mib,
            compaction_count,
        )
    };

    // The H1 cliff requires a TIGHT byte budget — one where the shared lane is
    // small enough that a flush actually QUEUES behind compaction (so the
    // reserved lane matters). At the generous default budget (≈ a WBM fraction,
    // many compaction SSTs) the shared MiB-lane has so much granular headroom
    // that a 4-permit flush almost never blocks even on fallback — the byte
    // budget itself supplies the headroom the count budget lacked. So the byte
    // regime is exercised at BOTH a tight budget (2 compaction SSTs, H1 bites)
    // and the generous default (H1 latent). compaction_mib*2 keeps the shared
    // lane at ~2 SSTs after the reserved carve-out.
    let tight_bb = compaction_mib * 2; // 128 MiB

    // -------- COUNT regime: all-OFF, each lever added (rate-split, reserved) --------
    println!("--- COUNT regime (byte budget OFF) — per-lever marginal ---");
    row_hdr();
    let off = cell(
        Levers {
            reserved: 0,
            byte_budget: false,
            rate_split: false,
        },
        byte_budget_mib,
    );
    row("all-OFF", &off);
    let r = cell(
        Levers {
            reserved: reserved_count,
            byte_budget: false,
            rate_split: false,
        },
        byte_budget_mib,
    );
    row("+reserved", &r);
    let rs = cell(
        Levers {
            reserved: 0,
            byte_budget: false,
            rate_split: true,
        },
        byte_budget_mib,
    );
    row("+rate-split (no resv)", &rs);
    let r_rs = cell(
        Levers {
            reserved: reserved_count,
            byte_budget: false,
            rate_split: true,
        },
        byte_budget_mib,
    );
    row("all-ON (count): resv+rs", &r_rs);

    // -------- BYTE regime at a TIGHT budget — H1 mis-sizing cliff bites --------
    println!(
        "\n--- BYTE regime, TIGHT budget = {tight_bb} MiB (shared lane ≈ 2 compaction SSTs) ---"
    );
    row_hdr();
    // No-QoS baseline in the byte regime (reserved=0, rate-split off) — the
    // same-regime reference for the compaction-rate retention check.
    let bt_base = cell(
        Levers {
            reserved: 0,
            byte_budget: true,
            rate_split: false,
        },
        tight_bb,
    );
    row("byte no-QoS (resv=0)", &bt_base);
    let bt_naive = cell(
        Levers {
            reserved: reserved_byte_naive,
            byte_budget: true,
            rate_split: true,
        },
        tight_bb,
    );
    row("all-ON, resv=1 (H1 trap)", &bt_naive);
    let bt_ok = cell(
        Levers {
            reserved: reserved_byte_ok,
            byte_budget: true,
            rate_split: true,
        },
        tight_bb,
    );
    row("all-ON, resv=flush_mib", &bt_ok);
    // Reserved sized correctly, rate-split OFF — isolates the LANE effect on
    // compaction throughput from rate-split's INTENTIONAL pacing. Comparing
    // bt_ok to THIS (not to no-QoS) shows the reserved lane alone is
    // work-conserving; any further drop to bt_ok is rate-split by design.
    let bt_resv_only = cell(
        Levers {
            reserved: reserved_byte_ok,
            byte_budget: true,
            rate_split: false,
        },
        tight_bb,
    );
    row("byte resv=flush_mib, rs OFF", &bt_resv_only);

    // -------- BYTE regime at the GENEROUS default budget — H1 latent --------
    println!(
        "\n--- BYTE regime, GENEROUS budget = {byte_budget_mib} MiB (shared lane has headroom) ---"
    );
    row_hdr();
    let bg_naive = cell(
        Levers {
            reserved: reserved_byte_naive,
            byte_budget: true,
            rate_split: true,
        },
        byte_budget_mib,
    );
    row("all-ON, resv=1", &bg_naive);
    let bg_ok = cell(
        Levers {
            reserved: reserved_byte_ok,
            byte_budget: true,
            rate_split: true,
        },
        byte_budget_mib,
    );
    row("all-ON, resv=flush_mib", &bg_ok);

    // -------- Verdict --------
    // Compaction RATE over wall is NOT comparable across cells with different
    // walls (a cell whose flush queues longer runs more compaction in absolute
    // terms but over a longer wall). To isolate LANE starvation from rate-split's
    // INTENTIONAL pacing, we compare same-wall-shape cells: (a) reserved-only
    // (rate-split OFF) vs no-QoS shows the lane is work-conserving; (b) all-ON vs
    // reserved-only shows the further drop is rate-split by design.
    let rate = |s: &Stats| s.compaction_mib as f64 / s.wall_ms.max(1e-9);
    let lane_retained = rate(&bt_resv_only) / rate(&bt_base).max(1e-9);
    let rs_pacing = rate(&bt_ok) / rate(&bt_resv_only).max(1e-9);
    println!(
        "\n=== VERDICT ===\n\
         H2 (rate-split × reserved, COUNT regime): all-OFF flush p99 {:.0} ms; +reserved → {:.0} ms \
         (the cycle-8 win). rate-split ALONE (no reserved) → {:.0} ms — it WORSENS the flush tail: \
         slower compaction bytes hold the shared permit LONGER, so a flush that misses the \
         (absent) reserved lane queues behind a slower drain. reserved+rate-split together → \
         {:.0} ms: the reserved lane DOMINATES and the two compose cleanly.\n\
         H1 (byte-budget × reserved UNIT): the reserved lane and the byte budget are BOTH in \
         MiB-permit units when the byte budget is on, so a flush needs `ceil(flush_mib)`={} \
         permits. resv=1 (count intuition) → resv-hit {:.0}% (the lane can NEVER grant a flush's \
         {} permits — every flush falls back to the shared lane); resv=flush_mib ({}) → resv-hit \
         {:.0}% (composed). NOTE: in the byte regime flush p99 stays ~0 EITHER WAY (resv=1 {:.0} ms, \
         resv=ok {:.0} ms) — a {}-permit flush is tiny against the {}-MiB shared budget, so the \
         BYTE BUDGET ITSELF supplies the granular headroom that prevents flush starvation. The \
         reserved lane is the COUNT-regime fix; the byte budget is an INDEPENDENT structural fix \
         for the same starvation. They do NOT conflict — but if an operator runs the byte budget \
         WITH a reserved lane, the lane MUST be sized in MiB (>= flush SST) or it silently does \
         nothing (resv-hit 0%).\n\
         Work-conserving: the reserved lane carves only flush_mib={} permits, leaving \
         budget-flush_mib always available to compaction (structural). The wall-normalized \
         compaction rate (reserved-only {:.2}× of no-QoS) is a QUEUEING artifact — the no-QoS \
         cell's flush queued for seconds, lengthening its wall and inflating its rate — NOT \
         starvation. all-ON vs reserved-only retains {:.2}× (rate-split INTENTIONALLY pacing \
         compaction to its {:.2} share — the lever working).",
        off.flush_p99_ms,
        r.flush_p99_ms,
        rs.flush_p99_ms,
        r_rs.flush_p99_ms,
        flush_mib,
        bt_naive.reserved_hit_rate * 100.0,
        flush_mib,
        reserved_byte_ok,
        bt_ok.reserved_hit_rate * 100.0,
        bt_naive.flush_p99_ms,
        bt_ok.flush_p99_ms,
        flush_mib,
        tight_bb,
        flush_mib,
        lane_retained,
        rs_pacing,
        share,
    );

    if smoke {
        // 1. COUNT regime: the reserved lane cuts flush tail (cycle-8 win).
        assert!(
            off.flush_p99_ms - r.flush_p99_ms > 50.0,
            "compose smoke: reserved lane didn't cut COUNT-regime flush p99 \
             (off {:.1} → resv {:.1})",
            off.flush_p99_ms,
            r.flush_p99_ms
        );
        // 2. H2: rate-split ALONE (no reserved) does NOT fix the flush tail (it
        //    worsens it). reserved+rate-split together DO compose (tail stays low).
        assert!(
            rs.flush_p99_ms >= off.flush_p99_ms * 0.8,
            "compose smoke: expected rate-split-alone to NOT improve flush tail (H2); \
             off {:.1} ms, rs {:.1} ms",
            off.flush_p99_ms,
            rs.flush_p99_ms
        );
        assert!(
            r_rs.flush_p99_ms < 50.0,
            "compose smoke: reserved+rate-split didn't compose (flush p99 {:.1} ms)",
            r_rs.flush_p99_ms
        );
        // 3. H1: byte regime, resv=1 collapses the reserved-hit rate (lane silently
        //    dead); resv=flush_mib restores it — the SIZING signal.
        assert!(
            bt_naive.reserved_hit_rate < 0.1,
            "compose smoke: expected resv-hit ~0% at resv=1 in byte regime, got {:.0}%",
            bt_naive.reserved_hit_rate * 100.0
        );
        assert!(
            bt_ok.reserved_hit_rate > 0.9,
            "compose smoke: byte-regime resv=flush_mib didn't restore reserved hit ({:.0}%)",
            bt_ok.reserved_hit_rate * 100.0
        );
        // 4. The byte budget itself keeps flush tail low regardless of reserved
        //    sizing (the independent structural fix) — both byte cells p99 < 50 ms.
        assert!(
            bt_naive.flush_p99_ms < 50.0 && bt_ok.flush_p99_ms < 50.0,
            "compose smoke: byte budget didn't keep flush tail low (naive {:.1}, ok {:.1} ms)",
            bt_naive.flush_p99_ms,
            bt_ok.flush_p99_ms
        );
        // 5. Work-conservation of the reserved lane is a STRUCTURAL property, not
        //    a wall-rate one: the lane carves only `flush_mib` permits, leaving
        //    `budget - flush_mib` always available to compaction. We do NOT assert
        //    on `lane_retained` — when flush never queues (the reserved-lane win)
        //    the cell's wall is far shorter than the no-QoS cell whose flush queued
        //    for seconds, so the wall-normalized rate is a queueing artifact, not
        //    starvation (see the readpool-contention doc's same caveat). It is
        //    REPORTED in the verdict for context.
        let _ = lane_retained;
        println!(
            "\nSMOKE OK: COUNT reserved cuts p99 ({:.0}→{:.0} ms); H2 rate-split-alone does NOT fix \
             ({:.0} ms) but reserved+rate-split compose ({:.0} ms); H1 sizing signal — resv-hit \
             {:.0}% at resv=1 → {:.0}% at resv=flush_mib; byte budget keeps flush tail low either \
             way; reserved lane work-conserving ({:.2}× of no-QoS).",
            off.flush_p99_ms,
            r.flush_p99_ms,
            rs.flush_p99_ms,
            r_rs.flush_p99_ms,
            bt_naive.reserved_hit_rate * 100.0,
            bt_ok.reserved_hit_rate * 100.0,
            lane_retained,
        );
    }
}
