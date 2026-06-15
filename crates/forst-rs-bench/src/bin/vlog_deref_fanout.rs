//! FRS-VLOG-DEREF-FANOUT (Phase-2 cycle 4) — coalesced vlog-deref SEGMENT
//! fan-out cost model. Design: `2026-06-15-vlog-deref-segment-fanout-design.md`.
//!
//! `FRS_VLOG_COALESCE_DEREF` already collapses a batch's deferred `BlobRef`
//! derefs into one ranged read PER SEGMENT, but
//! `DbImpl::coalesced_vlog_deref_into` walks those `M` segments in a SERIAL
//! `for` loop — on the disaggregated/remote path a batch spanning `M` segments
//! pays `M × remote-RTT` back-to-back. `FRS_VLOG_DEREF_FANOUT=1` fans those
//! per-segment reads across the shared read-I/O pool.
//!
//! This bin models the CROSS-SEGMENT latency term ONLY: `M` segment derefs,
//! serial (current loop) vs concurrent through a `min(M, pool)`-wide pool, where
//! each segment costs one modeled round-trip (`FRS_MODEL_RTT_MS`; the cold open
//! and ranged read collapse to the segment's single overlap-able round-trip on
//! the hot contiguous-flush case). Pool width mirrors `prefetch.rs::read_io_pool`
//! using `clamp(cores/2, 2, 6)` (env `FRS_RS_PREFETCH_THREADS`). std-only
//! using Mutex + Condvar + VecDeque, `#![forbid(unsafe_code)]`-clean, matching
//! the `scan_cold_start` sibling.
//!
//! Run: `cargo run -p forst-rs-bench --bin vlog_deref_fanout --release`
//! Smoke (CI): append `-- --smoke` (caps RTT + asserts the win holds at M≥4).

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

fn rtt() -> Duration {
    let ms = std::env::var("FRS_MODEL_RTT_MS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(23.0);
    Duration::from_secs_f64(ms / 1e3)
}

/// CURRENT: M segment derefs issued back-to-back by the serial loop. Wall ≈ M × RTT.
fn serial_deref(m: usize, rtt: Duration) -> Duration {
    let t0 = Instant::now();
    for _ in 0..m {
        std::thread::sleep(rtt); // one segment's coalesced ranged read
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

/// FRS-VLOG-DEREF-LOCALITY: WARM-local segments. Each segment's bytes are
/// already cache-resident, so a deref is served inline (no remote RTT) and —
/// with the locality gate — on the DIRECT path with NO pool involvement. This
/// models the per-segment COORDINATION cost only (the byte work is identical on
/// both paths): the direct path is a plain function call per segment, ~0 µs.
/// Repeated `reps` times so the µs-scale signal is measurable above timer noise.
fn warm_local_direct_deref(m: usize, reps: usize) -> Duration {
    let t0 = Instant::now();
    for _ in 0..reps {
        for _ in 0..m {
            // Direct path: the serial loop calls deref_one_segment_into — no
            // channel, no pool submit, no barrier. Model the dispatch cost as a
            // trivial inline op (std::hint::black_box keeps it from being elided).
            std::hint::black_box(0u64);
        }
    }
    t0.elapsed()
}

/// FRS-VLOG-DEREF-LOCALITY: what the OLD (locality-BLIND) gate did to WARM
/// segments — it fanned them out anyway (FS-level `is_local()` was always
/// `false`), paying the read-I/O-pool COORDINATION per batch (mpsc channel
/// alloc + M `tx.clone()` + M boxed-job submits + M condvar wakeups + the
/// receiver barrier) for bytes that were already local. This models exactly that
/// coordination — each job does NO byte work (warm), so the wall is pure
/// dispatch overhead the direct path never pays. `reps` batches.
fn warm_local_fanned_deref(pool: &Pool, m: usize, reps: usize) -> Duration {
    let t0 = Instant::now();
    for _ in 0..reps {
        let done = Arc::new((Mutex::new(0usize), Condvar::new()));
        for _ in 0..m {
            let d = Arc::clone(&done);
            pool.submit(Box::new(move || {
                std::hint::black_box(0u64); // warm: byte work is inline, ~0
                let (mu, cv) = &*d;
                *mu.lock().unwrap_or_else(|p| p.into_inner()) += 1;
                cv.notify_all();
            }));
        }
        let (mu, cv) = &*done;
        let mut g = mu.lock().unwrap_or_else(|p| p.into_inner());
        while *g < m {
            g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
        }
    }
    t0.elapsed()
}

/// PROPOSED: submit all M segment derefs to the bounded pool and barrier. Wall ≈
/// ceil(M / pool) × RTT — exactly what `coalesced_vlog_deref_fanout` does via
/// `prime_opens_concurrent`.
fn concurrent_deref(pool: &Pool, m: usize, rtt: Duration) -> Duration {
    let t0 = Instant::now();
    let done = Arc::new((Mutex::new(0usize), Condvar::new()));
    for _ in 0..m {
        let d = Arc::clone(&done);
        pool.submit(Box::new(move || {
            std::thread::sleep(rtt); // one segment's coalesced ranged read, overlapped
            let (mu, cv) = &*d;
            *mu.lock().unwrap_or_else(|p| p.into_inner()) += 1;
            cv.notify_all();
        }));
    }
    let (mu, cv) = &*done;
    let mut g = mu.lock().unwrap_or_else(|p| p.into_inner());
    while *g < m {
        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
    }
    t0.elapsed()
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");
    let pool_n = pool_width();
    // Smoke caps RTT to keep CI fast while preserving the ratio.
    let r = if smoke {
        Duration::from_millis(
            std::env::var("FRS_MODEL_RTT_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(5)
                .min(5),
        )
    } else {
        rtt()
    };

    println!(
        "== vlog-deref SEGMENT fan-out cost model ==\n\
         pool width = {pool_n} (clamp(cores/2,2,6); FRS_RS_PREFETCH_THREADS)\n\
         modeled per-segment RTT = {:.1} ms (FRS_MODEL_RTT_MS)\n\
         serial = M × RTT  vs  concurrent = ceil(M / pool) × RTT\n",
        ms(r)
    );
    println!(
        "{:>10} {:>14} {:>16} {:>10}",
        "segments M", "serial (ms)", "concurrent (ms)", "speedup"
    );

    let pool = Pool::new(pool_n);
    let cases = if smoke {
        vec![4usize, 8]
    } else {
        vec![2, 4, 8, 16, 32]
    };
    let mut min_speedup_at_4 = f64::INFINITY;
    for &m in &cases {
        let s = serial_deref(m, r);
        let c = concurrent_deref(&pool, m, r);
        let sp = ms(s) / ms(c).max(1e-9);
        if m >= 4 {
            min_speedup_at_4 = min_speedup_at_4.min(sp);
        }
        println!("{m:>10} {:>14.1} {:>16.1} {:>9.2}×", ms(s), ms(c), sp);
    }

    // -------------------------------------------------------------------------
    // FRS-VLOG-DEREF-LOCALITY: warm-cache regime. For segments whose bytes are
    // already cache-resident-local, the locality-aware gate takes the DIRECT
    // path (no pool dispatch). The OLD locality-blind gate fanned them out
    // anyway, paying pool submit/wakeup/barrier overhead on top of a µs pread.
    // -------------------------------------------------------------------------
    println!(
        "\n== WARM-LOCAL regime (cache-resident segments — coordination cost) ==\n\
         old gate (locality-BLIND) fanned warm derefs to the pool;\n\
         new gate (locality-AWARE) takes the direct path (no pool dispatch).\n\
         Byte work is identical (already local); this isolates DISPATCH cost.\n"
    );
    let warm_reps = if smoke { 2_000 } else { 20_000 };
    println!(
        "{:>10} {:>20} {:>20} {:>12}",
        "segments M", "old: fanned (µs/b)", "new: direct (µs/b)", "overhead×"
    );
    let pool2 = Pool::new(pool_n);
    let mut max_warm_overhead = 0.0f64;
    for &m in &cases {
        let old = warm_local_fanned_deref(&pool2, m, warm_reps);
        let new = warm_local_direct_deref(m, warm_reps);
        let old_us = old.as_secs_f64() * 1e6 / warm_reps as f64;
        let new_us = new.as_secs_f64() * 1e6 / warm_reps as f64;
        let ovh = old_us / new_us.max(1e-9);
        if m >= 4 {
            max_warm_overhead = max_warm_overhead.max(ovh);
        }
        println!("{m:>10} {old_us:>20.3} {new_us:>20.3} {ovh:>11.1}×");
    }
    pool2.shutdown();
    println!(
        "\nWARM: the direct path removes the per-batch pool COORDINATION the old\n\
         gate paid on cache-resident segments (overhead {max_warm_overhead:.0}× at M>=4);\n\
         COLD/remote still fans out (speedup {min_speedup_at_4:.2}× above)."
    );

    if smoke {
        // The COLD/remote win must hold (> 1.5× at M ≥ 4) — a regression here
        // means the pool barrier collapsed to serial.
        assert!(
            min_speedup_at_4 > 1.5,
            "fan-out smoke: speedup at M>=4 collapsed ({min_speedup_at_4:.2}× <= 1.5×)"
        );
        // The WARM direct path must be CHEAPER than fanning out (the locality win
        // this cycle adds) — pool COORDINATION is genuine overhead when local.
        assert!(
            max_warm_overhead > 2.0,
            "locality smoke: warm direct path not meaningfully cheaper than fanout \
             ({max_warm_overhead:.1}× <= 2×) — locality gate gives no win"
        );
        println!(
            "\nSMOKE OK: cold speedup {min_speedup_at_4:.2}× (> 1.5×); \
             warm direct cheaper by {max_warm_overhead:.0}× (> 2×)."
        );
    }
}
