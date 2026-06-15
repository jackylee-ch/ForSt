//! FRS-VLOG-SCAN-COALESCE (Phase-2 cycle 5) — SCAN-PATH inline vlog-deref vs
//! windowed-coalesce + segment fan-out cost model. Design:
//! `2026-06-15-vlog-scan-coalesce-design.md`.
//!
//! The single-iterator scan (`prefix_scan_iter_owned_arc`,
//! `ValueDecision::Blob`) resolves each separated row via `db.vlog_deref(ptr)`
//! INLINE, per-row, fully serial. With KV-separation ON, a scan over `N`
//! separated values spanning `M` vlog segments pays `N × remote-RTT` back to
//! back — the dominant remote-read cost for scan-heavy disaggregated state.
//!
//! `FRS_VLOG_SCAN_COALESCE=1` buffers a WINDOW of rows, defers their `BlobRef`
//! derefs, and resolves each window in ONE coalesced pass: group by `segment_id`
//! (one ranged read per segment touched in the window) and — with
//! `FRS_VLOG_DEREF_FANOUT=1` — fan those per-segment reads across the read-I/O
//! pool. The window emits in key order regardless.
//!
//! This bin models the SCAN latency term ONLY: for a scan of `N` separated rows
//! distributed across `M` segments, with window `W`, the three walls are
//! `serial = N × RTT` (one round-trip per row), `coalesce = Σ_windows
//! segs_touched × RTT` (serial per segment), and `+fanout = Σ_windows
//! ceil(segs_touched / pool) × RTT`. Here `segs_touched` per window is the number
//! of DISTINCT segments the window's rows hit (`min(W, M)` under the
//! contiguous-flush round-robin layout a scan walks). The model issues one
//! modeled round-trip (`FRS_MODEL_RTT_MS`) per segment read, exactly the unit
//! `coalesced_vlog_deref_into` / `_fanout` pay.
//!
//! Run:   `cargo run -p forst-rs-bench --bin vlog_scan_coalesce --release`
//! Smoke: append `-- --smoke` (caps RTT + asserts the win holds).

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

/// CURRENT: N separated rows, each resolved by one inline `vlog_deref` —
/// N back-to-back modeled round-trips.
fn serial_scan(n: usize, rtt: Duration) -> Duration {
    let t0 = Instant::now();
    for _ in 0..n {
        std::thread::sleep(rtt);
    }
    t0.elapsed()
}

/// Number of DISTINCT segments a window of `w` consecutive rows touches, given
/// rows are laid out round-robin across `m` segments (the contiguous-flush
/// layout). For a contiguous run of `w` rows that is `min(w, m)`.
fn segs_in_window(w: usize, m: usize) -> usize {
    w.min(m).max(1)
}

type Job = Box<dyn FnOnce() + Send>;
struct Shared {
    queue: Mutex<(std::collections::VecDeque<Job>, bool)>,
    cv: Condvar,
}
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

/// Barrier on `k` modeled segment reads submitted to the pool. Wall ≈
/// ceil(k / pool) × RTT.
fn fanout_reads(pool: &Pool, k: usize, rtt: Duration) -> Duration {
    let t0 = Instant::now();
    let done = Arc::new((Mutex::new(0usize), Condvar::new()));
    for _ in 0..k {
        let d = Arc::clone(&done);
        pool.submit(Box::new(move || {
            std::thread::sleep(rtt);
            let (mu, cv) = &*d;
            *mu.lock().unwrap_or_else(|p| p.into_inner()) += 1;
            cv.notify_all();
        }));
    }
    let (mu, cv) = &*done;
    let mut g = mu.lock().unwrap_or_else(|p| p.into_inner());
    while *g < k {
        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
    }
    t0.elapsed()
}

/// COALESCE: walk the scan in windows of `w`. Each window resolves its distinct
/// segments either serially (`pool=None`) or fanned across the pool.
fn coalesced_scan(pool: Option<&Pool>, n: usize, m: usize, w: usize, rtt: Duration) -> Duration {
    let t0 = Instant::now();
    let mut remaining = n;
    while remaining > 0 {
        let win = w.min(remaining);
        let k = segs_in_window(win, m);
        match pool {
            None => {
                for _ in 0..k {
                    std::thread::sleep(rtt); // serial per-segment ranged read
                }
            }
            Some(p) => {
                // Reuse the wall of the fan-out barrier (its own t0 is relative).
                let _ = fanout_reads(p, k, rtt);
            }
        }
        remaining -= win;
    }
    t0.elapsed()
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");
    let pool_n = pool_width();
    let r = if smoke {
        Duration::from_millis(
            std::env::var("FRS_MODEL_RTT_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(4)
                .min(4),
        )
    } else {
        rtt()
    };
    let window: usize = std::env::var("FRS_VLOG_SCAN_COALESCE_WINDOW")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(256);

    println!(
        "== SCAN-PATH vlog-deref: inline-serial vs windowed-coalesce + fan-out ==\n\
         pool width = {pool_n} (clamp(cores/2,2,6); FRS_RS_PREFETCH_THREADS)\n\
         modeled per-read RTT = {:.1} ms (FRS_MODEL_RTT_MS)\n\
         window W = {window} rows (FRS_VLOG_SCAN_COALESCE_WINDOW)\n\
         serial = N × RTT  vs  coalesce = Σ segs × RTT  vs  +fanout = Σ ceil(segs/pool) × RTT\n",
        ms(r)
    );
    println!(
        "{:>8} {:>6} {:>14} {:>16} {:>10} {:>18} {:>10}",
        "rows N",
        "segs M",
        "serial (ms)",
        "coalesce (ms)",
        "vs serial",
        "+fanout (ms)",
        "vs serial"
    );

    let pool = Pool::new(pool_n);
    // Smoke uses small N to stay fast; full sweep covers scan-heavy regimes.
    let cases: Vec<(usize, usize)> = if smoke {
        vec![(64, 8), (256, 16)]
    } else {
        vec![(256, 4), (1024, 8), (4096, 16), (4096, 64), (16384, 32)]
    };
    let mut min_fan_speedup = f64::INFINITY;
    for &(n, m) in &cases {
        let s = serial_scan(n, r);
        let co = coalesced_scan(None, n, m, window, r);
        let fo = coalesced_scan(Some(&pool), n, m, window, r);
        let sp_co = ms(s) / ms(co).max(1e-9);
        let sp_fo = ms(s) / ms(fo).max(1e-9);
        min_fan_speedup = min_fan_speedup.min(sp_fo);
        println!(
            "{n:>8} {m:>6} {:>14.1} {:>16.1} {:>9.2}× {:>18.1} {:>9.2}×",
            ms(s),
            ms(co),
            sp_co,
            ms(fo),
            sp_fo
        );
    }
    pool.shutdown();

    if smoke {
        // The coalesce alone collapses N→M-class reads; with fan-out the win is
        // larger. Require a clear win (a regression = the window/segment model
        // collapsed back toward per-row serial).
        assert!(
            min_fan_speedup > 2.0,
            "scan-coalesce smoke: speedup collapsed ({min_fan_speedup:.2}× <= 2.0×)"
        );
        println!("\nSMOKE OK: +fanout speedup {min_fan_speedup:.2}× (> 2.0×).");
    }
}
