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
    pool.shutdown();

    if smoke {
        // The win must be > 1.5× at M ≥ 4 (bounded at min(M, pool)); a regression
        // here means the pool barrier collapsed to serial.
        assert!(
            min_speedup_at_4 > 1.5,
            "fan-out smoke: speedup at M>=4 collapsed ({min_speedup_at_4:.2}× <= 1.5×)"
        );
        println!("\nSMOKE OK: M>=4 speedup {min_speedup_at_4:.2}× (> 1.5×).");
    }
}
