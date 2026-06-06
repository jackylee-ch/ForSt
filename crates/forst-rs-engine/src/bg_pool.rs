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

impl WorkerPool {
    /// Spawn a pool with `n_workers` threads (clamped to ≥1), each named with
    /// `name` for visibility in `sample`/profilers.
    pub(crate) fn new(n_workers: usize, name: &str) -> Self {
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
                .spawn(move || worker_loop(sh))
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
        job();
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

        // Wait until the pool is saturated (active == CAP) or timeout.
        let start = Instant::now();
        while active.load(Ordering::SeqCst) < CAP {
            assert!(
                active.load(Ordering::SeqCst) <= CAP,
                "concurrency exceeded cap"
            );
            if start.elapsed() > Duration::from_secs(5) {
                panic!(
                    "pool never saturated to {} (active={})",
                    CAP,
                    active.load(Ordering::SeqCst)
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
}
