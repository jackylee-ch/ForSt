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

/// READAHEAD (this cycle): launch window 0's read, then for each window join its
/// in-flight read (overlapped with the PRIOR window's consume) and immediately
/// launch the next window's read on the pool BEFORE consuming. Wall ≈
/// `rtt + Wn·max(rtt, consume)` (the first read is un-hidden; thereafter each
/// window costs the larger of its read or its consume). Mirrors
/// `ScanCoalesceIter::next`'s join-then-launch-then-drain loop.
fn readahead_scan(pool: &Pool, windows: usize, rtt: Duration, consume: Duration) -> Duration {
    let t0 = Instant::now();
    // A window's "resolution" = one modeled coalesced remote read, run on the
    // pool, signalling completion over a oneshot channel (the consumer's join).
    let launch = |pool: &Pool| -> mpsc::Receiver<()> {
        let (tx, rx) = mpsc::channel();
        pool.submit(Box::new(move || {
            std::thread::sleep(rtt); // window's coalesced remote read, overlapped
            let _ = tx.send(());
        }));
        rx
    };
    let mut prefetch = Some(launch(pool)); // window 0 in flight
    for k in 0..windows {
        // Join the in-flight window (its read overlapped window k-1's consume).
        if let Some(rx) = prefetch.take() {
            let _ = rx.recv();
        }
        // Look one window ahead: launch window k+1's read BEFORE consuming k, so
        // it overlaps k's consume.
        if k + 1 < windows {
            prefetch = Some(launch(pool));
        }
        std::thread::sleep(consume); // consumer drains window k's rows
    }
    t0.elapsed()
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

    println!(
        "== vlog-scan READAHEAD latency-hiding cost model ==\n\
         pool width = {pool_n} (clamp(cores/2,2,6); FRS_RS_PREFETCH_THREADS)\n\
         modeled per-window read RTT = {:.1} ms (FRS_MODEL_RTT_MS)\n\
         serial    = Wn·(rtt + consume)\n\
         readahead = rtt + Wn·max(rtt, consume)  (window k+1 read hides behind k's consume)\n",
        ms(rtt)
    );
    println!(
        "{:>9} {:>11} {:>13} {:>16} {:>9}",
        "windows", "consume(ms)", "serial(ms)", "readahead(ms)", "speedup"
    );

    let pool = Pool::new(pool_n);
    // Sweep windows × consume/rtt ratio. The win is largest when consume ≈ rtt
    // (each window fully hides one RTT); it shrinks as consume ≫ rtt (consume
    // dominates) or consume ≪ rtt (little to overlap), but never regresses.
    let window_counts: Vec<usize> = if smoke {
        vec![8, 16]
    } else {
        vec![4, 8, 16, 32]
    };
    let consume_ratios: Vec<f64> = if smoke {
        vec![1.0]
    } else {
        vec![0.5, 1.0, 2.0]
    };

    let mut min_speedup_at_parity = f64::INFINITY;
    for &cr in &consume_ratios {
        let consume = consume_base.mul_f64(cr);
        for &wn in &window_counts {
            let s = serial_scan(wn, rtt, consume);
            let r = readahead_scan(&pool, wn, rtt, consume);
            let sp = ms(s) / ms(r).max(1e-9);
            if (cr - 1.0).abs() < 1e-9 {
                min_speedup_at_parity = min_speedup_at_parity.min(sp);
            }
            println!(
                "{wn:>9} {:>11.1} {:>13.1} {:>16.1} {:>8.2}×",
                ms(consume),
                ms(s),
                ms(r),
                sp
            );
        }
    }
    pool.shutdown();

    if smoke {
        // At consume ≈ rtt the readahead should hide ≈ one RTT per window ⇒
        // approaching 2× as Wn grows; require a clear win (a collapse to serial
        // means the pipeline join/launch interleave broke).
        assert!(
            min_speedup_at_parity > 1.3,
            "readahead smoke: speedup at consume≈rtt collapsed ({min_speedup_at_parity:.2}× <= 1.3×)"
        );
        println!("\nSMOKE OK: consume≈rtt speedup {min_speedup_at_parity:.2}× (> 1.3×).");
    }
}
