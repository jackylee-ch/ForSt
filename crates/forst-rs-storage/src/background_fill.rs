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

//! FRS-PHASE2-C3U3 (design §4.1.1): the post-restore **background-fill
//! scheduler** — turns the instant-link restore's lazy cache warm into a
//! PACED background fill.
//!
//! After an instant-link restore the working set lives at its remote
//! physicals and the cache warms only as foreground reads touch files —
//! every first touch pays a whole-file remote fetch on the operator's
//! critical path. This scheduler walks the restored file set on a small
//! **read pool** of background-class worker threads and pre-loads files
//! through [`CachedFileSystem::fill_file_cold`], which applies the merged
//! admission machinery's **Bottom/Skip** policy:
//!
//! - **Bottom**: fills enter at the COLD end of the LRU
//!   ([`crate::local_cache::LocalCache::put_cold`]) — a mis-predicted warm
//!   never displaces hot-set recency;
//! - **Skip**: `promote_limit`-blocked keys and fills that exceed the FREE
//!   budget headroom are skipped (a background fill never evicts live
//!   entries — budget-capped).
//!
//! **Pacing** bounds foreground impact: a global bytes-per-second budget is
//! enforced across the pool (token-bucket-by-schedule — workers sleep until
//! the cumulative fill is back on schedule), so the warm shares disk/network
//! bandwidth instead of bursting against foreground reads. Workers are
//! marked background-class ([`crate::requester`]) so any reads they issue
//! neither promote LRU order nor pollute foreground hit metrics
//! (FRS-CACHE-BG-EXEMPT).
//!
//! Inert by default: nothing constructs a scheduler unless the owner
//! (engine restore path, gated by `FRS_RESTORE_BG_FILL=1`) opts in.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::cached_fs::{BackgroundFillOutcome, CachedFileSystem};
use crate::requester;

/// Tuning for [`BackgroundFill::start`].
#[derive(Debug, Clone, Copy)]
pub struct BackgroundFillParams {
    /// Size of the read pool (worker threads). Clamped to ≥ 1.
    pub workers: usize,
    /// Global pacing budget in bytes/second across the pool; `0` = unpaced.
    pub pace_bytes_per_sec: u64,
}

impl Default for BackgroundFillParams {
    fn default() -> Self {
        Self {
            // Mirrors ForSt's small background load-back pool: enough to
            // overlap a few remote GETs, far below the foreground read
            // parallelism.
            workers: 2,
            // Default pace: 64 MiB/s — a fraction of NVMe/intra-DC S3
            // bandwidth, so the warm is feelable-fast but never saturating.
            pace_bytes_per_sec: 64 * 1024 * 1024,
        }
    }
}

/// Final tally of one background-fill run.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackgroundFillReport {
    /// Files loaded into the cache (cold end).
    pub filled: usize,
    /// Total bytes loaded.
    pub filled_bytes: u64,
    /// Files already cache-resident (no I/O issued).
    pub already_cached: usize,
    /// Files skipped by the anti-thrash block (`promote_limit`).
    pub skipped_blocked: usize,
    /// Files skipped because they exceeded the free budget headroom.
    pub skipped_budget: usize,
    /// Files whose remote read errored (best-effort: demand reads retry).
    pub errors: usize,
    /// Files left unprocessed by a cancel.
    pub cancelled: usize,
    /// Wall time from `start` to the last worker finishing.
    pub elapsed_ms: u128,
}

struct Shared {
    fs: Arc<CachedFileSystem>,
    queue: Mutex<Vec<PathBuf>>,
    stop: AtomicBool,
    filled: AtomicUsize,
    filled_bytes: AtomicU64,
    already_cached: AtomicUsize,
    skipped_blocked: AtomicUsize,
    skipped_budget: AtomicUsize,
    errors: AtomicUsize,
    started: Instant,
    pace_bytes_per_sec: u64,
}

impl Shared {
    /// Token-bucket-by-schedule pacing: with B bytes filled so far and a
    /// budget of R bytes/sec, the run "owes" B/R seconds of wall time;
    /// sleep the deficit before issuing the next fetch. Coarse (per-file)
    /// but allocation-free and exact in aggregate.
    fn pace(&self) {
        if self.pace_bytes_per_sec == 0 {
            return;
        }
        let owed = Duration::from_secs_f64(
            self.filled_bytes.load(Ordering::Relaxed) as f64 / self.pace_bytes_per_sec as f64,
        );
        let elapsed = self.started.elapsed();
        if owed > elapsed {
            // Bounded catnaps so a cancel is honored promptly mid-deficit.
            let mut remaining = owed - elapsed;
            while remaining > Duration::ZERO && !self.stop.load(Ordering::Relaxed) {
                let nap = remaining.min(Duration::from_millis(20));
                std::thread::sleep(nap);
                remaining = remaining.saturating_sub(nap);
            }
        }
    }
}

/// Handle for an in-flight background fill. [`Self::wait`] joins the pool
/// and returns the [`BackgroundFillReport`]; [`Self::cancel`] makes workers
/// stop after their current file. Dropping the handle cancels and joins
/// (bounded by one in-flight fetch per worker) so a closing engine never
/// leaks fill threads.
pub struct BackgroundFill {
    shared: Arc<Shared>,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl BackgroundFill {
    /// Spawns the read pool over `paths` (deduplicated by the caller if
    /// needed; duplicates degrade to `AlreadyCached`). Returns immediately;
    /// filling proceeds in the background.
    pub fn start(
        fs: Arc<CachedFileSystem>,
        paths: Vec<PathBuf>,
        params: BackgroundFillParams,
    ) -> Self {
        let shared = Arc::new(Shared {
            fs,
            queue: Mutex::new(paths),
            stop: AtomicBool::new(false),
            filled: AtomicUsize::new(0),
            filled_bytes: AtomicU64::new(0),
            already_cached: AtomicUsize::new(0),
            skipped_blocked: AtomicUsize::new(0),
            skipped_budget: AtomicUsize::new(0),
            errors: AtomicUsize::new(0),
            started: Instant::now(),
            pace_bytes_per_sec: params.pace_bytes_per_sec,
        });
        let workers = params.workers.max(1);
        let handles = (0..workers)
            .map(|i| {
                let sh = shared.clone();
                std::thread::Builder::new()
                    .name(format!("frs-bgfill-{i}"))
                    .spawn(move || {
                        // Background-class: reads from this thread neither
                        // promote LRU order nor count as foreground misses.
                        requester::mark_thread_background();
                        loop {
                            if sh.stop.load(Ordering::Relaxed) {
                                return;
                            }
                            let Some(path) =
                                sh.queue.lock().expect("bgfill queue poisoned").pop()
                            else {
                                return;
                            };
                            sh.pace();
                            if sh.stop.load(Ordering::Relaxed) {
                                // Put the un-filled path back so the report's
                                // `cancelled` count stays exact.
                                sh.queue
                                    .lock()
                                    .expect("bgfill queue poisoned")
                                    .push(path);
                                return;
                            }
                            match sh.fs.fill_file_cold(&path) {
                                Ok(BackgroundFillOutcome::Filled(n)) => {
                                    sh.filled.fetch_add(1, Ordering::Relaxed);
                                    sh.filled_bytes.fetch_add(n, Ordering::Relaxed);
                                }
                                Ok(BackgroundFillOutcome::AlreadyCached) => {
                                    sh.already_cached.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(BackgroundFillOutcome::SkippedBlocked) => {
                                    sh.skipped_blocked.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(BackgroundFillOutcome::SkippedBudget) => {
                                    sh.skipped_budget.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    // Best-effort: the demand read path
                                    // retries and surfaces real errors.
                                    sh.errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    })
                    .expect("spawn frs-bgfill worker")
            })
            .collect();
        Self { shared, handles }
    }

    /// Requests a prompt stop: workers exit after their current file (or
    /// mid-pacing-sleep). Idempotent.
    pub fn cancel(&self) {
        self.shared.stop.store(true, Ordering::Relaxed);
    }

    /// Joins the pool and returns the tally.
    pub fn wait(mut self) -> BackgroundFillReport {
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
        self.report()
    }

    /// Snapshot of the tally (exact once workers have exited).
    fn report(&self) -> BackgroundFillReport {
        let sh = &self.shared;
        BackgroundFillReport {
            filled: sh.filled.load(Ordering::Relaxed),
            filled_bytes: sh.filled_bytes.load(Ordering::Relaxed),
            already_cached: sh.already_cached.load(Ordering::Relaxed),
            skipped_blocked: sh.skipped_blocked.load(Ordering::Relaxed),
            skipped_budget: sh.skipped_budget.load(Ordering::Relaxed),
            errors: sh.errors.load(Ordering::Relaxed),
            cancelled: sh.queue.lock().expect("bgfill queue poisoned").len(),
            elapsed_ms: sh.started.elapsed().as_millis(),
        }
    }
}

impl Drop for BackgroundFill {
    fn drop(&mut self) {
        self.cancel();
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_cache::{AdmissionParams, CachePolicy, LocalCache};
    use forst_rs_io::{FileSystem, MemoryFileSystem, WriteMode};
    use std::path::Path;

    const FILE_KB: usize = 8;

    fn remote_with_files(n: usize) -> Arc<dyn FileSystem> {
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        fs.create_dir_all(Path::new("/db")).unwrap();
        let payload = vec![0xABu8; FILE_KB * 1024];
        for i in 0..n {
            let mut w = fs
                .open_writable_file(
                    Path::new(&format!("/db/{i:06}.sst")),
                    WriteMode::CreateNew,
                )
                .unwrap();
            w.append(&payload).unwrap();
            w.sync().unwrap();
        }
        fs
    }

    fn cached(
        remote: Arc<dyn FileSystem>,
        budget_files: u64,
        policy: CachePolicy,
    ) -> (tempfile::TempDir, Arc<CachedFileSystem>) {
        let tmp = tempfile::TempDir::new().unwrap();
        let cache =
            LocalCache::open_with_policy(tmp.path(), budget_files * (FILE_KB as u64) * 1024, policy)
                .unwrap();
        (tmp, Arc::new(CachedFileSystem::new(remote, Arc::new(cache))))
    }

    fn paths(n: usize) -> Vec<PathBuf> {
        (0..n).map(|i| PathBuf::from(format!("/db/{i:06}.sst"))).collect()
    }

    /// Whole restored set fits the budget: everything is filled, cache-hit
    /// reads afterwards, zero foreground misses charged.
    #[test]
    fn test_c3u3_fills_restored_set_within_budget() {
        let remote = remote_with_files(8);
        let (_g, fs) = cached(remote, 16, CachePolicy::default());
        let bf = BackgroundFill::start(
            fs.clone(),
            paths(8),
            BackgroundFillParams {
                workers: 3,
                pace_bytes_per_sec: 0,
            },
        );
        let r = bf.wait();
        assert_eq!(r.filled, 8, "{r:?}");
        assert_eq!(r.filled_bytes, 8 * (FILE_KB as u64) * 1024);
        assert_eq!(r.errors, 0);
        assert_eq!(r.cancelled, 0);
        for p in paths(8) {
            assert!(fs.cache().contains(p.to_str().unwrap()), "{p:?} warmed");
        }
    }

    /// Budget cap: with budget ≪ set, the fill stops at the headroom — it
    /// never evicts what it just loaded (no thrash), and the report says so.
    #[test]
    fn test_c3u3_budget_capped_never_evicts() {
        let remote = remote_with_files(12);
        let (_g, fs) = cached(remote, 4, CachePolicy::default());
        let r = BackgroundFill::start(
            fs.clone(),
            paths(12),
            BackgroundFillParams {
                workers: 1,
                pace_bytes_per_sec: 0,
            },
        )
        .wait();
        assert_eq!(r.filled, 4, "fills exactly the headroom: {r:?}");
        assert_eq!(r.skipped_budget, 8, "{r:?}");
        // Nothing was evicted to make room — the cache holds the 4 fills.
        assert_eq!(fs.cache().current_bytes(), 4 * (FILE_KB as u64) * 1024);
    }

    /// Bottom policy: a background prefill that is never touched is evicted
    /// BEFORE older foreground-warmed entries when a demand put needs room.
    #[test]
    fn test_c3u3_cold_fill_evicts_before_hot_set() {
        let remote = remote_with_files(2);
        let (_g, fs) = cached(remote, 2, CachePolicy::default());
        let payload = vec![0xCDu8; FILE_KB * 1024];
        // Foreground-warm a hot entry FIRST (older in insertion order).
        fs.cache().put("/db/hot.sst", &payload).unwrap();
        // Background-fill one restored file (cold insert, budget has room).
        let r = BackgroundFill::start(
            fs.clone(),
            paths(1),
            BackgroundFillParams {
                workers: 1,
                pace_bytes_per_sec: 0,
            },
        )
        .wait();
        assert_eq!(r.filled, 1, "{r:?}");
        // A new demand put needs room: the untouched COLD fill must be the
        // victim even though the hot entry is older.
        fs.cache().put("/db/new.sst", &payload).unwrap();
        assert!(fs.cache().contains("/db/hot.sst"), "hot survives");
        assert!(fs.cache().contains("/db/new.sst"));
        assert!(
            !fs.cache().contains("/db/000000.sst"),
            "untouched cold prefill is the first victim"
        );
    }

    /// Skip policy: a promote_limit-blocked key is never background-filled.
    #[test]
    fn test_c3u3_blocked_key_skipped() {
        let remote = remote_with_files(1);
        let policy = CachePolicy {
            admission: Some(AdmissionParams {
                access_before_promote: 1,
                promote_limit: 1,
                ..AdmissionParams::default()
            }),
            ..CachePolicy::default()
        };
        let (_g, fs) = cached(remote, 4, policy);
        let payload = vec![0xEFu8; FILE_KB * 1024];
        // Drive the key over the eviction cap: fill the cache so the key
        // gets evicted promote_limit times.
        fs.cache().put("/db/000000.sst", &payload).unwrap();
        for i in 0..4 {
            fs.cache().put(&format!("/db/filler-{i}.sst"), &payload).unwrap();
        }
        assert!(fs.cache().is_admission_blocked("/db/000000.sst"));
        let r = BackgroundFill::start(
            fs.clone(),
            paths(1),
            BackgroundFillParams {
                workers: 1,
                pace_bytes_per_sec: 0,
            },
        )
        .wait();
        assert_eq!(r.skipped_blocked, 1, "{r:?}");
        assert_eq!(r.filled, 0);
    }

    /// Pacing: a tight bytes/sec budget stretches the warm wall-time to at
    /// least filled_bytes / budget (the foreground-impact bound), and cancel
    /// stops a paced run promptly.
    #[test]
    fn test_c3u3_pacing_bounds_fill_rate_and_cancel_is_prompt() {
        let remote = remote_with_files(6);
        let (_g, fs) = cached(remote.clone(), 16, CachePolicy::default());
        // Single worker: the pace() before file k waits out the deficit of
        // the k-1 files already filled, so 6 files x 8 KiB at 256 KiB/s
        // bound the wall time below by (5 x 8 KiB) / 256 KiB/s ~= 156 ms.
        let r = BackgroundFill::start(
            fs,
            paths(6),
            BackgroundFillParams {
                workers: 1,
                pace_bytes_per_sec: 256 * 1024,
            },
        )
        .wait();
        assert_eq!(r.filled, 6, "{r:?}");
        let min_ms = (5u128 * FILE_KB as u128 * 1024 * 1000) / (256 * 1024);
        assert!(
            r.elapsed_ms >= min_ms,
            "paced run must respect the schedule: {} < {min_ms} ({r:?})",
            r.elapsed_ms
        );

        // Cancel: a fresh paced run with a huge deficit stops promptly.
        let (_g2, fs2) = cached(remote, 16, CachePolicy::default());
        let bf = BackgroundFill::start(
            fs2,
            paths(6),
            BackgroundFillParams {
                workers: 1,
                pace_bytes_per_sec: 1, // 1 B/s — pathological deficit
            },
        );
        std::thread::sleep(Duration::from_millis(50));
        bf.cancel();
        let started = Instant::now();
        let r2 = bf.wait();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "cancel must not wait out the pacing deficit"
        );
        assert!(r2.cancelled > 0, "{r2:?}");
    }
}
