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

//! FRS-PHASE2 cycle-1 evidence micro-bench — **k-way scan cold-start: serial vs
//! concurrent first-block prime**.
//!
//! ## The bottleneck this quantifies
//!
//! A cross-tier prefix/range scan (`DbImpl::build_lazy_prefix_key_stream`,
//! `db.rs:9588`) builds a `LazyPrefixIter` over K SST sources, each owning a
//! `BlockPrefetcher`. The merge's FIRST step seeds every source's head by
//! calling `ensure_head_pinned`/`peek` in a **sequential `for` loop**
//! (`db.rs:15970-15972` tree path, `:16111-16113` linear path). Each source's
//! first `next_decoded()` is the FIRST time that source submits a window and
//! **synchronously waits** for the block. On a cold REMOTE scan (disaggregated
//! state, the cache-miss tail the paper calls L1×L4, PVLDB 18(12):4856-4857)
//! those K first-block GETs are issued back-to-back ⇒ the scan cold-start pays
//! **K × remote-RTT serially** before the first row can be merged.
//!
//! ForSt hides exactly this with its depth-3 parallel read threads
//! (`ForStStateExecutor.java:59-66`): the K independent cold reads overlap.
//! forst-rs's read-I/O pool (`prefetch.rs:228-288`, `clamp(cores/2,2,6)`
//! workers) ALREADY exists — but the merge cold-start does not USE it to prime
//! the sources concurrently; it only parallelises *within* one source's
//! sequential readahead.
//!
//! ## What is MODELED
//!
//! S3 GET latency is modeled as a fixed per-first-block RTT
//! (`FRS_MODEL_RTT_MS`, default 23 ms = recorded dev-Mac→BOS; set 2 for the
//! ≥50 Gb/s intra-DC online box). The "serial" arm sleeps K×RTT (the current
//! merge cold-start); the "concurrent" arm fans the K sleeps across a bounded
//! worker pool of `min(K, POOL)` (the proposed prime), so wall ≈ ceil(K/POOL)
//! × RTT. This isolates the cold-start latency term — it does NOT model
//! steady-state throughput (where readahead already overlaps) or warm hits
//! (where there is no GET at all). It is the cold-scan-startup tail ONLY,
//! which is the term that dominates short/medium prefix probes on remote state
//! (q9/q19/q20 join + OVER-window: many small prefix scans, each a fresh merge
//! cold-start over the overlapping L0/L1 SSTs).
//!
//! Run: `cargo run -p forst-rs-bench --release --bin scan_cold_start`
//!      `cargo run -p forst-rs-bench --release --bin scan_cold_start -- --smoke`

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Pool size the proposed concurrent prime would use: the existing read-I/O
/// pool is `clamp(cores/2, 2, 6)` (`prefetch.rs:280-285`). Model the same.
fn pool_size() -> usize {
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

/// CURRENT cold-start: K first-block GETs issued back-to-back by the merge's
/// sequential head-seed loop. Wall ≈ K × RTT.
fn serial_cold_start(k: usize, rtt: Duration) -> Duration {
    let t0 = Instant::now();
    for _ in 0..k {
        std::thread::sleep(rtt); // one synchronous first-block GET
    }
    t0.elapsed()
}

/// A bounded worker pool that mirrors `prefetch.rs::ReadIoPool` (std-only:
/// Mutex + Condvar + VecDeque, `#![forbid(unsafe_code)]`-clean). Submitting K
/// jobs and joining models the proposed concurrent prime: the K first-block
/// GETs run `min(K, pool)`-wide.
struct Pool {
    shared: Arc<Shared>,
    workers: Vec<std::thread::JoinHandle<()>>,
}
type Job = Box<dyn FnOnce() + Send>;
struct Shared {
    queue: Mutex<(std::collections::VecDeque<Job>, bool)>,
    cv: Condvar,
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

/// PROPOSED cold-start: prime all K sources' first blocks concurrently through
/// the bounded pool, then the merge seeds from already-ready heads. Wall ≈
/// ceil(K / pool) × RTT.
fn concurrent_cold_start(pool: &Pool, k: usize, rtt: Duration) -> Duration {
    let t0 = Instant::now();
    let done = Arc::new((Mutex::new(0usize), Condvar::new()));
    for _ in 0..k {
        let d = Arc::clone(&done);
        pool.submit(Box::new(move || {
            std::thread::sleep(rtt); // one first-block GET, overlapped
            let (m, cv) = &*d;
            *m.lock().unwrap_or_else(|p| p.into_inner()) += 1;
            cv.notify_all();
        }));
    }
    let (m, cv) = &*done;
    let mut g = m.lock().unwrap_or_else(|p| p.into_inner());
    while *g < k {
        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
    }
    t0.elapsed()
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");
    let rtt = rtt();
    let pool_n = pool_size();
    // Source counts model the merge fan-out: a fresh prefix probe over the
    // active memtable + a few immutable memtables + the overlapping L0/L1 SSTs.
    // q7 FANOUT-DIAG recorded peaks of tens of SST sources; sweep the regime.
    let fanouts: &[usize] = if smoke { &[4, 16] } else { &[2, 4, 8, 16, 32] };
    let reps = if smoke { 1 } else { 3 };

    println!(
        "== scan cold-start: serial (current merge head-seed) vs concurrent prime \
         (proposed) ==\n\
         modeled per-first-block RTT = {:.1} ms (FRS_MODEL_RTT_MS); prime pool = {} workers \
         (FRS_RS_PREFETCH_THREADS / clamp(cores/2,2,6))\n",
        ms(rtt),
        pool_n
    );
    println!(
        "{:>10} | {:>14} | {:>16} | {:>9}",
        "fanout K", "serial (ms)", "concurrent (ms)", "speedup"
    );
    println!("{}", "-".repeat(58));

    let pool = Pool::new(pool_n);
    for &k in fanouts {
        let mut ser = Vec::new();
        let mut con = Vec::new();
        for _ in 0..reps {
            ser.push(ms(serial_cold_start(k, rtt)));
            con.push(ms(concurrent_cold_start(&pool, k, rtt)));
        }
        let median = |mut xs: Vec<f64>| {
            xs.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
            xs[xs.len() / 2]
        };
        let s = median(ser);
        let c = median(con);
        println!(
            "{:>10} | {:>14.1} | {:>16.1} | {:>8.2}x",
            k,
            s,
            c,
            s / c.max(f64::MIN_POSITIVE)
        );
    }
    pool.shutdown();

    println!(
        "\nReading: the speedup is the cold-start latency removed from every fresh \n\
         multi-source scan over remote (cache-miss) state. It bounds at ~min(K, pool); \n\
         the win is largest at high fan-out (many overlapping L0 SSTs = compaction-behind \n\
         join state) and high RTT (cold object store). It is ZERO on warm/cached scans \n\
         (no GET) and on single-source scans (K=1), so the proposed prime must be \n\
         flag-gated and a no-op when every source's first block is already cache-resident."
    );
}
