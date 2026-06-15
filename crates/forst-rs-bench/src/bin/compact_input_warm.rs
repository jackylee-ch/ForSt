//! FRS-COMPACT-INPUT-WARM (Phase-2 cycle 9) — root-cause + before/after for
//! HIDING the next compaction job's input-download latency behind the current
//! job's merge+upload (the WRITE-side analogue of depth-D read readahead).
//!
//! ## The structural gap (root cause)
//!
//! A remote compaction is a SERIAL chain per job:
//!
//! ```text
//!   open inputs (footer + sparse-index GetObject, one remote RTT per input)
//!     → k-way merge
//!     → upload output SST(s)
//! ```
//!
//! The engine already overlaps WITHIN a job: the input `BlockPrefetcher`
//! double-buffers window N+1 over consumption of N, and `close_writer` spawns
//! the output upload async (write-back). What it did NOT overlap is the
//! BOUNDARY BETWEEN JOBS: when an L0 rollup is followed by an L1 drain
//! (`run_compaction` runs them back-to-back), the drain's input-reader OPEN —
//! one remote `GetObject` per input for its footer + sparse index — is paid
//! COLD, on the critical path, AFTER the rollup's merge+upload already finished.
//! At a throttled / bandwidth-limited endpoint (the disagg regime) that
//! cold-start RTT is pure serial latency the merge could have hidden.
//!
//! ## The fix under test
//!
//! `FRS_COMPACT_INPUT_WARM=1` makes `run_compaction` PREDICT the following
//! drain's inputs (pure metadata over the current Version) and fire their
//! `get_or_open_sst_reader` opens onto the shared read-I/O pool FIRE-AND-FORGET
//! BEFORE the rollup's long merge runs. By the time the drain opens its inputs,
//! the readers are warm (or already in flight) — the input-download latency
//! overlaps the previous merge+upload instead of running after it.
//!
//! Byte-identical / correctness-safe: `get_or_open_sst_reader` is cache-keyed
//! with a double-checked RCU insert (a warm-up racing the real open is benign);
//! a mispredicted file only leaves an evictable reader cached; OFF ⇒ the helper
//! is never called.
//!
//! ## This bench
//!
//! Models a chain of `JOBS` compactions. Each job costs `open` (remote input
//! reader open RTT) + `merge` (CPU, local) + `upload` (output, throttled).
//!   * `serial` — open(N) happens AFTER merge+upload(N-1): the cold path.
//!   * `warm` — open(N) is fired onto a pool DURING merge+upload(N-1), so the
//!     job-N critical path pays only `max(0, open - (merge+upload))`.
//!
//! Open + upload times scale with `FRS_REMOTE_BW_MBPS` (the throttle); merge is
//! a bandwidth-independent CPU cost. Reports total wall and the hidden latency.
//!
//! Run:   `cargo run -p forst-rs-bench --bin compact_input_warm --release`
//! Two-dir sim-S3 + 50 Gb/s: `FRS_REMOTE_BW_MBPS=6250 cargo run ... --release`
//! Smoke: append `-- --smoke` (asserts warm hides a material fraction of the
//! input-open latency under a throttled endpoint).

#![forbid(unsafe_code)]

use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Modeled remote transfer time for `mib` MiB at `bw_mbps` MiB/s, plus a fixed
/// per-request round-trip (a `GetObject`/`PutObject` has high fixed cost even
/// for a tiny footer/index read). Floored so nothing is free.
fn remote_time(mib: f64, bw_mbps: f64, rtt: Duration) -> Duration {
    let secs = mib / bw_mbps.max(1e-9);
    rtt + Duration::from_secs_f64(secs.max(0.0))
}

struct Stats {
    wall_ms: f64,
    /// Sum of the input-open latency that landed ON the critical path.
    open_on_path_ms: f64,
}

/// Run the `serial` (cold) policy: each job opens its inputs, then merges, then
/// uploads — every stage on the critical path, back-to-back.
fn run_serial(jobs: usize, open: Duration, merge: Duration, upload: Duration) -> Stats {
    let t0 = Instant::now();
    let mut open_on_path = Duration::ZERO;
    for _ in 0..jobs {
        // Input reader open (footer + sparse-index GET) — serial, on the path.
        std::thread::sleep(open);
        open_on_path += open;
        // Merge (CPU) then output upload (throttled) — already overlapped WITHIN
        // a job by the prefetcher/write-back, modeled here as the job body.
        std::thread::sleep(merge);
        std::thread::sleep(upload);
    }
    Stats {
        wall_ms: t0.elapsed().as_secs_f64() * 1e3,
        open_on_path_ms: open_on_path.as_secs_f64() * 1e3,
    }
}

/// Run the `warm` policy: while job N merges+uploads, job N+1's input open is
/// fired onto a background "pool" thread. Job N+1 then waits only for whatever
/// of its open did NOT finish during job N's body — `max(0, open - body)`.
fn run_warm(jobs: usize, open: Duration, merge: Duration, upload: Duration) -> Stats {
    let t0 = Instant::now();
    let mut open_on_path = Duration::ZERO;
    // The first job has nothing to overlap behind — it pays the cold open.
    std::thread::sleep(open);
    open_on_path += open;
    for j in 0..jobs {
        // Fire the NEXT job's input open onto the background pool BEFORE running
        // this job's body (the warm-up's seam). The pool thread sleeps `open`
        // and signals completion.
        let (tx, rx) = mpsc::channel::<()>();
        if j + 1 < jobs {
            std::thread::spawn(move || {
                std::thread::sleep(open);
                let _ = tx.send(());
            });
        } else {
            drop(tx); // no next job — sender closed
        }
        // This job's body: merge (CPU) + upload (throttled). The next job's open
        // runs concurrently on the pool thread during this window.
        let body_start = Instant::now();
        std::thread::sleep(merge);
        std::thread::sleep(upload);
        let body = body_start.elapsed();
        // The next job can start once ITS open completed. If the body already
        // covered the open, the residual wait is ~0 (latency fully hidden);
        // otherwise we wait the remainder.
        // If the body covered the open, the residual wait is ~0 (latency fully
        // hidden) — drop the receiver and the detached pool thread finishes
        // harmlessly (fire-and-forget). Otherwise wait only the remainder.
        if j + 1 < jobs && body < open {
            let residual = open - body;
            // Block until the pooled open signals (it finishes at ~`open`).
            let _ = rx.recv_timeout(residual + Duration::from_millis(50));
            open_on_path += residual;
        }
    }
    Stats {
        wall_ms: t0.elapsed().as_secs_f64() * 1e3,
        open_on_path_ms: open_on_path.as_secs_f64() * 1e3,
    }
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");

    // Modeled remote bandwidth (MiB/s). The serial input-open latency is
    // sharpest at a THROTTLED endpoint — the disagg regime. Default 256 MiB/s
    // (a modest object-store stream); the 6250 ≈ 50 Gb/s ceiling makes transfer
    // sub-ms so only the fixed RTT remains (still hideable, but smaller).
    let bw_mbps = std::env::var("FRS_REMOTE_BW_MBPS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(256.0);
    // Fixed per-request round-trip for a remote GET/PUT (footer/index fetch is
    // tiny but still costs a full RTT). Object stores are ms-class.
    let rtt = Duration::from_millis(
        std::env::var("FRS_REMOTE_RTT_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(10),
    );

    let jobs = if smoke { 12 } else { 64 };
    // Per-job sizes. Input open reads only footer + sparse index (small bytes,
    // RTT-dominated); the output upload moves the whole merged SST (the big
    // throttled transfer); merge is a bandwidth-independent CPU cost.
    let open_mib = 0.25_f64; // footer + sparse index per input set
    let upload_mib = 4.0_f64; // a target-sized output SST
    let merge = Duration::from_millis(8); // CPU k-way merge (local)

    let open = remote_time(open_mib, bw_mbps, rtt);
    let upload = remote_time(upload_mib, bw_mbps, rtt);

    println!(
        "== FRS-COMPACT-INPUT-WARM — hide next-job input-open behind current merge+upload ==\n\
         remote BW = {bw_mbps} MiB/s (FRS_REMOTE_BW_MBPS); RTT = {:?} (FRS_REMOTE_RTT_MS)\n\
         jobs = {jobs}; per-job open = {:?} ({open_mib} MiB), merge = {:?}, upload = {:?} ({upload_mib} MiB)\n",
        rtt, open, merge, upload
    );

    let serial = run_serial(jobs, open, merge, upload);
    let warm = run_warm(jobs, open, merge, upload);

    println!(
        "{:>10} {:>14} {:>22}",
        "policy", "wall(ms)", "input-open on path(ms)"
    );
    let row = |name: &str, s: &Stats| {
        println!("{name:>10} {:>14.1} {:>22.1}", s.wall_ms, s.open_on_path_ms);
    };
    row("serial", &serial);
    row("warm", &warm);

    let wall_cut_ms = serial.wall_ms - warm.wall_ms;
    let wall_speedup = serial.wall_ms / warm.wall_ms.max(1e-9);
    let open_hidden_ms = serial.open_on_path_ms - warm.open_on_path_ms;
    let open_hidden_frac = open_hidden_ms / serial.open_on_path_ms.max(1e-9);
    println!(
        "\nwall: serial {:.1} ms → warm {:.1} ms  (−{wall_cut_ms:.1} ms, {wall_speedup:.2}×)\n\
         input-open latency HIDDEN: {open_hidden_ms:.1} ms of {:.1} ms ({:.0}%)",
        serial.wall_ms,
        warm.wall_ms,
        serial.open_on_path_ms,
        open_hidden_frac * 100.0
    );

    if smoke {
        // Under a throttled endpoint the merge+upload body of job N is long
        // enough to cover much of job N+1's RTT-dominated input open, so the
        // warm policy must hide a MATERIAL fraction of the input-open latency.
        assert!(
            open_hidden_frac > 0.5,
            "warm smoke: hid only {:.0}% of input-open latency (<= 50%)",
            open_hidden_frac * 100.0
        );
        assert!(
            wall_cut_ms > 0.0,
            "warm smoke: wall did not improve (−{wall_cut_ms:.1} ms)"
        );
        println!(
            "\nSMOKE OK: input-open latency hidden {:.0}% (−{open_hidden_ms:.1} ms); \
             wall −{wall_cut_ms:.1} ms ({wall_speedup:.2}×).",
            open_hidden_frac * 100.0
        );
    }
}
