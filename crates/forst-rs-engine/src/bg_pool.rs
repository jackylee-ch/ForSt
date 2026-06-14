//! FRS-SLOT-SHARED-BG (2026-06-05): a bounded, shared background worker pool.
//!
//! Root cause it fixes: today every `DbImpl` spawns its own `forst-rs-flush` and
//! `forst-rs-compact` thread (db.rs), so background concurrency scales with
//! (operators × parallelism). On q4 that is ~12 DbImpls ⇒ up to ~12 compactions
//! running at once, spiking total CPU to ~9 cores and starving the CPU-bound
//! interval-join → the throughput sawtooth (proven on a clean 18-core machine,
//! swap=0). RocksDB stays flat because Flink shares ONE bounded background-jobs
//! pool across the whole slot. This module is the engine half of that fix: a
//! process-global pool with a FIXED worker count, so total background CPU is
//! capped no matter how many `DbImpl`s exist.
//!
//! The pool is intentionally generic over `Box<dyn FnOnce() + Send>` jobs so it
//! has no dependency on `DbImpl` and can be unit-tested in isolation. The DbImpl
//! integration (a follow-up) submits a closure that upgrades a `Weak<DbImpl>` and
//! dispatches the flush/compaction. std-only (`Mutex` + `Condvar` + `VecDeque`),
//! preserving `#![forbid(unsafe_code)]`.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

/// A unit of background work.
pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

struct Shared {
    queue: Mutex<VecDeque<Job>>,
    cv: Condvar,
}

/// A fixed-size pool of worker threads draining a shared MPMC queue. The number
/// of concurrently-executing jobs never exceeds `n_workers`.
pub(crate) struct WorkerPool {
    shared: Arc<Shared>,
    /// Join handles kept alive for the pool's lifetime (process-global pools
    /// live forever; held so the threads aren't detached and for future
    /// graceful-shutdown support). Not otherwise read.
    #[allow(dead_code)]
    workers: Vec<JoinHandle<()>>,
}

/// Requester class assigned to every worker in a pool (sticky thread-local).
#[derive(Clone, Copy, PartialEq, Eq)]
enum PoolClass {
    /// Foreground — promotes cache, full remote rate (the read pool).
    Foreground,
    /// Background-flush — exempt from cache promotion; flush-priority for the
    /// upload split (a checkpoint awaits these L0 SSTs).
    Flush,
    /// Compaction — exempt from cache promotion AND paced against the reduced
    /// sub-rate by the QoS remote throttle (FRS_UPLOAD_RATE_SPLIT).
    Compaction,
}

impl WorkerPool {
    /// Spawn a pool with `n_workers` threads (clamped to ≥1), each named with
    /// `name` for visibility in `sample`/profilers.
    pub(crate) fn new(n_workers: usize, name: &str) -> Self {
        Self::with_class(n_workers, name, PoolClass::Foreground)
    }

    /// [`new`](Self::new), but every worker thread is marked as a BACKGROUND
    /// cache requester (`forst_rs_storage::requester`). Used by the flush pool
    /// so its SST reads cannot evict/promote the operator hot set in
    /// requester-aware caches (FRS-CACHE-BG-EXEMPT, ForSt §2.1.4 — only
    /// state-executor threads affect LRU order). The mark is advisory and
    /// inert unless a cache policy flag opts in (default OFF).
    ///
    /// NOT used for `bg_read_pool`: its workers execute FOREGROUND operator
    /// reads (parallel batch iterator opens) that must keep promoting.
    pub(crate) fn new_background(n_workers: usize, name: &str) -> Self {
        Self::with_class(n_workers, name, PoolClass::Flush)
    }

    /// [`new_background`](Self::new_background), but every worker is also marked
    /// as a COMPACTION requester so the QoS remote throttle paces its large
    /// continuous SST-rewrite uploads against the reduced sub-rate — flush /
    /// checkpoint critical-path writes are never starved by compaction
    /// (FRS_UPLOAD_RATE_SPLIT, default OFF). Use for the compaction pool.
    pub(crate) fn new_compaction(n_workers: usize, name: &str) -> Self {
        Self::with_class(n_workers, name, PoolClass::Compaction)
    }

    fn with_class(n_workers: usize, name: &str, class: PoolClass) -> Self {
        let n = n_workers.max(1);
        let shared = Arc::new(Shared {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
        });
        let mut workers = Vec::with_capacity(n);
        for _ in 0..n {
            let sh = Arc::clone(&shared);
            let handle = std::thread::Builder::new()
                .name(name.to_string())
                .spawn(move || {
                    match class {
                        PoolClass::Foreground => {}
                        PoolClass::Flush => {
                            forst_rs_storage::requester::mark_thread_background();
                        }
                        PoolClass::Compaction => {
                            forst_rs_storage::requester::mark_thread_compaction();
                        }
                    }
                    worker_loop(sh)
                })
                .expect("failed to spawn bg worker");
            workers.push(handle);
        }
        Self { shared, workers }
    }

    /// Submit a job. Returns immediately; the job runs on some worker thread.
    pub(crate) fn submit(&self, job: Job) {
        let mut q = self.shared.queue.lock().expect("bg queue poisoned");
        q.push_back(job);
        drop(q);
        self.shared.cv.notify_one();
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

fn worker_loop(shared: Arc<Shared>) {
    loop {
        let job = {
            let mut q = shared.queue.lock().expect("bg queue poisoned");
            loop {
                if let Some(job) = q.pop_front() {
                    break job;
                }
                q = shared.cv.wait(q).expect("bg queue poisoned");
            }
        };
        // Run OUTSIDE the lock so other workers can pull concurrently.
        //
        // H1 (2026-06-11 PMC review): a panicking job must NOT kill the
        // worker — with a fixed-size pool every dead worker permanently
        // shrinks background capacity, and once all workers are dead queued
        // jobs never run, hanging any caller joining on a job's channel
        // (e.g. `batch_open_prefix_iters_parallel*` in db.rs). The panic is
        // contained here; the job's result channel signals the failure to
        // the submitter (its `Sender` is dropped during unwind, so the
        // joining `recv` observes a disconnect and converts it to an error).
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    /// The pool must never run more than `n_workers` jobs at once, must reach
    /// exactly that high-water mark when saturated, and must execute every
    /// submitted job. Deterministic: jobs park on a gate until the test opens it.
    #[test]
    fn bounds_concurrency_to_worker_count() {
        const CAP: usize = 3;
        const JOBS: usize = 12;
        let pool = WorkerPool::new(CAP, "test-bg");

        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let ran = Arc::new(AtomicUsize::new(0));
        // Gate the jobs hold until the test releases them, so they pile up and
        // we can observe the true concurrency ceiling.
        let gate = Arc::new((Mutex::new(false), Condvar::new()));

        for _ in 0..JOBS {
            let active = Arc::clone(&active);
            let max_seen = Arc::clone(&max_seen);
            let ran = Arc::clone(&ran);
            let gate = Arc::clone(&gate);
            pool.submit(Box::new(move || {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                // Wait until the test opens the gate.
                let (lock, cv) = &*gate;
                let mut open = lock.lock().unwrap();
                while !*open {
                    open = cv.wait(open).unwrap();
                }
                drop(open);
                active.fetch_sub(1, Ordering::SeqCst);
                ran.fetch_add(1, Ordering::SeqCst);
            }));
        }

        // Wait until the pool is saturated, observed via the HIGH-WATER mark
        // (`max_seen`), not `active`. The workers do `fetch_add(active)` then a
        // separate `fetch_max(max_seen)`; a thread that bumps `active` to CAP
        // has not necessarily run its `fetch_max` yet, so polling `active`
        // could read CAP while `max_seen` still lags at CAP-1 — the source of
        // the intermittent CI failure "high-water != cap". `max_seen` reaching
        // CAP is the true saturation signal (all jobs park on the gate, so once
        // CAP run concurrently they stay until released). We still assert the
        // ceiling is never exceeded every iteration.
        let start = Instant::now();
        while max_seen.load(Ordering::SeqCst) < CAP {
            assert!(
                active.load(Ordering::SeqCst) <= CAP,
                "concurrency exceeded cap"
            );
            if start.elapsed() > Duration::from_secs(5) {
                panic!(
                    "pool never saturated to {} (active={}, max_seen={})",
                    CAP,
                    active.load(Ordering::SeqCst),
                    max_seen.load(Ordering::SeqCst)
                );
            }
            std::thread::yield_now();
        }
        // At saturation, never more than CAP run at once.
        assert_eq!(max_seen.load(Ordering::SeqCst), CAP, "high-water != cap");
        assert!(active.load(Ordering::SeqCst) <= CAP);

        // Release the gate; all jobs must finish.
        {
            let (lock, cv) = &*gate;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        let start = Instant::now();
        while ran.load(Ordering::SeqCst) < JOBS {
            if start.elapsed() > Duration::from_secs(5) {
                panic!("only {} of {} jobs ran", ran.load(Ordering::SeqCst), JOBS);
            }
            std::thread::yield_now();
        }
        assert_eq!(ran.load(Ordering::SeqCst), JOBS);
        assert_eq!(max_seen.load(Ordering::SeqCst), CAP);
    }

    /// FRS-CACHE-BG-EXEMPT + FRS-PHASE2 UPLOAD-RATE-SPLIT: `new_background`
    /// (flush) workers carry the background mark but are NOT compaction-class;
    /// `new_compaction` workers carry BOTH (background + compaction); plain
    /// `new` workers stay foreground. The background mark exempts flush/
    /// compaction reads from LRU promotion; the compaction mark additionally
    /// routes their remote writes onto the QoS sub-rate bucket.
    #[test]
    fn background_pool_marks_workers_plain_pool_does_not() {
        use forst_rs_storage::requester::{is_background_thread, is_compaction_thread};
        let check = |pool: &WorkerPool, expect_bg: bool, expect_comp: bool, what: &'static str| {
            let (tx, rx) = std::sync::mpsc::channel::<(bool, bool)>();
            pool.submit(Box::new(move || {
                let _ = tx.send((is_background_thread(), is_compaction_thread()));
            }));
            let (bg, comp) = rx
                .recv_timeout(Duration::from_secs(10))
                .expect("job did not run");
            assert_eq!(bg, expect_bg, "{what}: background");
            assert_eq!(comp, expect_comp, "{what}: compaction");
        };
        let bg = WorkerPool::new_background(1, "test-bg-marked");
        check(
            &bg,
            true,
            false,
            "new_background workers must be background, NOT compaction",
        );
        let comp = WorkerPool::new_compaction(1, "test-comp-marked");
        check(
            &comp,
            true,
            true,
            "new_compaction workers must be background AND compaction",
        );
        let fg = WorkerPool::new(1, "test-fg-unmarked");
        check(&fg, false, false, "plain new workers must stay foreground");
    }

    /// H1: a panicking job (i) does not kill its worker — subsequent jobs on
    /// the SAME (single) worker still run; (ii) surfaces to the submitter as
    /// a channel disconnect (its `Sender` clone is dropped during unwind), so
    /// the batch-join pattern in db.rs converts it to an `Err` slot; (iii)
    /// nothing hangs — the whole sequence completes within the test timeout.
    #[test]
    fn panicking_job_does_not_kill_worker_and_signals_channel() {
        let pool = WorkerPool::new(1, "test-bg-panic");
        let (tx, rx) = std::sync::mpsc::channel::<(usize, &'static str)>();

        // Job 0 panics; its tx clone is dropped during unwind (never sends).
        let tx0 = tx.clone();
        pool.submit(Box::new(move || {
            let _keep = tx0; // moved into the job like the db.rs batch paths
            panic!("deliberate test panic");
        }));
        // Job 1 must still run on the same single worker.
        let tx1 = tx.clone();
        pool.submit(Box::new(move || {
            let _ = tx1.send((1, "ok"));
        }));
        drop(tx); // only job-owned clones remain

        // (i)+(iii): the post-panic job completes (worker survived, no hang).
        let got = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("worker died after a panicking job (or hang)");
        assert_eq!(got, (1, "ok"));
        // (ii): once every job-owned sender is gone the channel disconnects —
        // the panicked job's slot is observable as Err, never a silent hang.
        match rx.recv_timeout(Duration::from_secs(10)) {
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {}
            other => panic!("expected Disconnected after panic, got {other:?}"),
        }
    }
}
