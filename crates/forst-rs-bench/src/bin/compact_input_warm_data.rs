//! FRS-COMPACT-INPUT-WARM-DATA (Phase-2 cycle 10) — root-cause + before/after
//! for HIDING the next compaction job's FIRST-DATA-BLOCK read latency (on top of
//! the reader-open latency cycle 9 already hides) behind the current job's
//! merge+upload.
//!
//! ## The residual gap (root cause)
//!
//! Cycle 9 (`FRS_COMPACT_INPUT_WARM`) hides the next job's input-reader OPEN
//! (footer + sparse-index `GetObject`, one RTT/input) behind the current
//! merge+upload. But the drain's actual merge constructs a compaction
//! `BlockPrefetcher` whose first window reads with `CacheFillPolicy::Skip` — a
//! cache-FIRST read that NEVER inserts (each input block is read once, so
//! inserting would only evict the foreground hot set). `Skip` serves a resident
//! block for free, but after a bare reader warm-up the first DATA block is NOT
//! resident, so the merge's first `next_decoded` still pays a cold remote
//! `GetObject` for it — ON the critical path, AFTER the previous merge+upload.
//!
//! ```text
//!   cycle 9 warm:   [open hidden]  -> merge -> upload
//!                                       ^ first DATA-block GET still cold here
//! ```
//!
//! ## The fix under test
//!
//! `FRS_COMPACT_INPUT_WARM_DATA=1` (requires `FRS_COMPACT_INPUT_WARM=1`) extends
//! the same fire-and-forget pool job: after opening the predicted input's
//! reader, it ALSO primes that input's first data block into the decoded cache
//! via `SstReaderImpl::prime_first_data_block` (a cache-first read that inserts
//! at `Low`, identical to the cold demand read — only the GET's TIMING moves
//! earlier). The drain's `Skip` first window then HITS the primed block: no cold
//! GET on the critical path. Byte/cache-identical OFF; a primed block on a
//! mispredicted file is a single evictable `Low` entry.
//!
//! ## This bench
//!
//! Models a chain of `JOBS` compactions. Each job's REMOTE inputs cost an
//! `open` RTT (footer/index) PLUS a `first_data` RTT+transfer (block 0), then a
//! local `merge` (CPU), then a throttled `upload`.
//!   * `cold` (cycle 9 reader-warm only) — open(N) is hidden behind job N-1's
//!     body, but first_data(N) is paid COLD on job N's critical path.
//!   * `warm-data` (cycle 10) — BOTH open(N) and first_data(N) are fired onto a
//!     pool DURING job N-1's body, so job N pays only `max(0, (open+first_data) -
//!     body)`.
//!
//! All remote times scale with `FRS_REMOTE_BW_MBPS`; merge is bandwidth-
//! independent. Reports total wall and the first-data-block latency hidden.
//!
//! Run:   `cargo run -p forst-rs-bench --bin compact_input_warm_data --release`
//! Two-dir sim-S3 + 50 Gb/s: `FRS_REMOTE_BW_MBPS=6250 cargo run ... --release`
//! Smoke: append `-- --smoke` (asserts warm-data hides a material fraction of
//! the first-data-block latency the cycle-9 reader-warm still paid cold).

#![forbid(unsafe_code)]

use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Modeled remote transfer time for `mib` MiB at `bw_mbps` MiB/s, plus a fixed
/// per-request round-trip (a `GetObject` has high fixed cost even for a tiny
/// read). Floored so nothing is free.
fn remote_time(mib: f64, bw_mbps: f64, rtt: Duration) -> Duration {
    let secs = mib / bw_mbps.max(1e-9);
    rtt + Duration::from_secs_f64(secs.max(0.0))
}

struct Stats {
    wall_ms: f64,
    /// Sum of the FIRST-DATA-BLOCK latency that landed ON the critical path.
    first_data_on_path_ms: f64,
}

/// `cold` policy (cycle-9 reader-warm only): the reader OPEN is hidden behind the
/// previous body, but each job pays its first-data-block GET cold on its path,
/// then merges + uploads.
fn run_cold(jobs: usize, first_data: Duration, merge: Duration, upload: Duration) -> Stats {
    let t0 = Instant::now();
    let mut fd_on_path = Duration::ZERO;
    for _ in 0..jobs {
        // First DATA block GET — cold, on the path (the `Skip` window misses it).
        std::thread::sleep(first_data);
        fd_on_path += first_data;
        std::thread::sleep(merge);
        std::thread::sleep(upload);
    }
    Stats {
        wall_ms: t0.elapsed().as_secs_f64() * 1e3,
        first_data_on_path_ms: fd_on_path.as_secs_f64() * 1e3,
    }
}

/// `warm-data` policy (cycle 10): while job N merges+uploads, job N+1's
/// first-data-block read is fired onto the pool. Job N+1 then waits only for
/// whatever did NOT finish during job N's body — `max(0, first_data - body)`.
fn run_warm_data(jobs: usize, first_data: Duration, merge: Duration, upload: Duration) -> Stats {
    let t0 = Instant::now();
    let mut fd_on_path = Duration::ZERO;
    // The first job has nothing to overlap behind — it pays the cold first-data.
    std::thread::sleep(first_data);
    fd_on_path += first_data;
    for j in 0..jobs {
        let (tx, rx) = mpsc::channel::<()>();
        if j + 1 < jobs {
            std::thread::spawn(move || {
                std::thread::sleep(first_data);
                let _ = tx.send(());
            });
        } else {
            drop(tx);
        }
        let body_start = Instant::now();
        std::thread::sleep(merge);
        std::thread::sleep(upload);
        let body = body_start.elapsed();
        if j + 1 < jobs && body < first_data {
            let residual = first_data - body;
            let _ = rx.recv_timeout(residual + Duration::from_millis(50));
            fd_on_path += residual;
        }
    }
    Stats {
        wall_ms: t0.elapsed().as_secs_f64() * 1e3,
        first_data_on_path_ms: fd_on_path.as_secs_f64() * 1e3,
    }
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");

    let bw_mbps = std::env::var("FRS_REMOTE_BW_MBPS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(256.0);
    let rtt = Duration::from_millis(
        std::env::var("FRS_REMOTE_RTT_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(10),
    );

    let jobs = if smoke { 12 } else { 64 };
    // The first DATA block is a real (64 KiB-class) block read: RTT + a small
    // transfer. The output upload moves the whole merged SST; merge is CPU.
    let first_data_mib = 0.0625_f64; // one 64 KiB data block
    let upload_mib = 4.0_f64; // a target-sized output SST
    let merge = Duration::from_millis(8); // CPU k-way merge (local)

    let first_data = remote_time(first_data_mib, bw_mbps, rtt);
    let upload = remote_time(upload_mib, bw_mbps, rtt);

    println!(
        "== FRS-COMPACT-INPUT-WARM-DATA — hide next-job first-data-block GET behind current merge+upload ==\n\
         remote BW = {bw_mbps} MiB/s (FRS_REMOTE_BW_MBPS); RTT = {:?} (FRS_REMOTE_RTT_MS)\n\
         jobs = {jobs}; first-data block = {:?} ({first_data_mib} MiB), merge = {:?}, upload = {:?} ({upload_mib} MiB)\n",
        rtt, first_data, merge, upload
    );

    let cold = run_cold(jobs, first_data, merge, upload);
    let warm = run_warm_data(jobs, first_data, merge, upload);

    println!(
        "{:>12} {:>14} {:>26}",
        "policy", "wall(ms)", "first-data on path(ms)"
    );
    let row = |name: &str, s: &Stats| {
        println!(
            "{name:>12} {:>14.1} {:>26.1}",
            s.wall_ms, s.first_data_on_path_ms
        );
    };
    row("cold(cyc9)", &cold);
    row("warm-data", &warm);

    let wall_cut_ms = cold.wall_ms - warm.wall_ms;
    let wall_speedup = cold.wall_ms / warm.wall_ms.max(1e-9);
    let fd_hidden_ms = cold.first_data_on_path_ms - warm.first_data_on_path_ms;
    let fd_hidden_frac = fd_hidden_ms / cold.first_data_on_path_ms.max(1e-9);
    println!(
        "\nwall: cold {:.1} ms → warm-data {:.1} ms  (−{wall_cut_ms:.1} ms, {wall_speedup:.2}×)\n\
         first-data-block latency HIDDEN: {fd_hidden_ms:.1} ms of {:.1} ms ({:.0}%)",
        cold.wall_ms,
        warm.wall_ms,
        cold.first_data_on_path_ms,
        fd_hidden_frac * 100.0
    );

    if smoke {
        assert!(
            fd_hidden_frac > 0.5,
            "warm-data smoke: hid only {:.0}% of first-data-block latency (<= 50%)",
            fd_hidden_frac * 100.0
        );
        assert!(
            wall_cut_ms > 0.0,
            "warm-data smoke: wall did not improve (−{wall_cut_ms:.1} ms)"
        );
        println!(
            "\nSMOKE OK: first-data-block latency hidden {:.0}% (−{fd_hidden_ms:.1} ms); \
             wall −{wall_cut_ms:.1} ms ({wall_speedup:.2}×).",
            fd_hidden_frac * 100.0
        );
    }
}
