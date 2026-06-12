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

//! FRS-PHASE2-C3U3 minibench (design §4.1.1): **warm-time vs
//! foreground-impact pair** for the post-restore background-fill scheduler.
//!
//! Model: an instant-link restore left N files remote-resident (cold local
//! cache). A foreground "operator" continuously probes the restored set in
//! a Zipf-ish hot/cold mix while the warm strategy runs. The remote is a
//! latency-modeled in-memory backend (per-read sleep, bounded concurrent
//! channel) so foreground misses and warm fetches CONTEND like they do on a
//! real S3/NVMe channel.
//!
//! Cells:
//!   lazy        — no fill: every first foreground touch pays the remote
//!                 fetch inline (warm completes when the set has been
//!                 demand-touched once).
//!   bgfill-fast — background fill, unpaced (max warm speed).
//!   bgfill-paced— background fill, paced (bounded foreground impact).
//!
//! Metrics: warm wall-time (cache holds the whole set), foreground read
//! p50/p95/avg during the warm window, foreground ops/sec.
//!
//! Run: `cargo run -p forst-rs-storage --release --example restore_bgfill_bench`

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use forst_rs_io::{FileSystem, MemoryFileSystem, WriteMode};
use forst_rs_storage::background_fill::{BackgroundFill, BackgroundFillParams};
use forst_rs_storage::cached_fs::CachedFileSystem;
use forst_rs_storage::local_cache::{CachePolicy, LocalCache};

const FILES: usize = 64;
const FILE_KB: usize = 256; // 64 × 256 KiB = 16 MiB restored set
const REMOTE_LATENCY_MS: u64 = 10; // per whole-file GET
const REMOTE_CHANNEL: usize = 4; // concurrent remote transfers
const FG_WINDOW_MS: u64 = 4000; // foreground probe window per cell

/// Latency-modeled remote: every sequential/random open pays
/// `REMOTE_LATENCY_MS` while holding one of `REMOTE_CHANNEL` slots — warm
/// fetches and foreground misses contend exactly like a bounded S3 channel.
struct SlowRemote {
    inner: Arc<dyn FileSystem>,
    slots: Arc<(Mutex<usize>, std::sync::Condvar)>,
}

impl SlowRemote {
    fn new(inner: Arc<dyn FileSystem>) -> Self {
        Self {
            inner,
            slots: Arc::new((Mutex::new(REMOTE_CHANNEL), std::sync::Condvar::new())),
        }
    }
    fn transfer_delay(&self) {
        let (lock, cv) = &*self.slots;
        let mut free = lock.lock().unwrap();
        while *free == 0 {
            free = cv.wait(free).unwrap();
        }
        *free -= 1;
        drop(free);
        std::thread::sleep(Duration::from_millis(REMOTE_LATENCY_MS));
        let (lock, cv) = &*self.slots;
        *lock.lock().unwrap() += 1;
        cv.notify_one();
    }
}

impl FileSystem for SlowRemote {
    fn open_sequential_file(
        &self,
        path: &Path,
    ) -> forst_rs_common::error::ForstResult<Box<dyn forst_rs_io::SequentialFile>> {
        self.transfer_delay();
        self.inner.open_sequential_file(path)
    }
    fn open_random_access_file(
        &self,
        path: &Path,
    ) -> forst_rs_common::error::ForstResult<Box<dyn forst_rs_io::RandomAccessFile>> {
        self.transfer_delay();
        self.inner.open_random_access_file(path)
    }
    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> forst_rs_common::error::ForstResult<Box<dyn forst_rs_io::WritableFile>> {
        self.inner.open_writable_file(path, mode)
    }
    fn file_exists(&self, path: &Path) -> forst_rs_common::error::ForstResult<bool> {
        self.inner.file_exists(path)
    }
    fn get_file_metadata(
        &self,
        path: &Path,
    ) -> forst_rs_common::error::ForstResult<forst_rs_io::FileMetadata> {
        self.inner.get_file_metadata(path)
    }
    fn list_dir(
        &self,
        dir: &Path,
    ) -> forst_rs_common::error::ForstResult<Vec<forst_rs_io::FileMetadata>> {
        self.inner.list_dir(dir)
    }
    fn create_dir_all(&self, dir: &Path) -> forst_rs_common::error::ForstResult<()> {
        self.inner.create_dir_all(dir)
    }
    fn delete_file(&self, path: &Path) -> forst_rs_common::error::ForstResult<()> {
        self.inner.delete_file(path)
    }
    fn delete_dir(&self, path: &Path, recursive: bool) -> forst_rs_common::error::ForstResult<()> {
        self.inner.delete_dir(path, recursive)
    }
    fn rename(&self, src: &Path, dst: &Path) -> forst_rs_common::error::ForstResult<()> {
        self.inner.rename(src, dst)
    }
    fn name(&self) -> &str {
        "slow-remote"
    }
}

fn build_remote() -> Arc<dyn FileSystem> {
    let mem: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    mem.create_dir_all(Path::new("/db")).unwrap();
    let payload = vec![0xA5u8; FILE_KB * 1024];
    for i in 0..FILES {
        let mut w = mem
            .open_writable_file(Path::new(&format!("/db/{i:06}.sst")), WriteMode::CreateNew)
            .unwrap();
        w.append(&payload).unwrap();
        w.sync().unwrap();
    }
    Arc::new(SlowRemote::new(mem))
}

fn paths() -> Vec<PathBuf> {
    (0..FILES)
        .map(|i| PathBuf::from(format!("/db/{i:06}.sst")))
        .collect()
}

struct CellOut {
    name: &'static str,
    warm_ms: u128,
    fg_ops: usize,
    fg_p50_us: u128,
    fg_p999_us: u128,
    fg_max_us: u128,
    fg_slow_ops: usize,
}

/// Foreground probe loop: reads over the restored set until `stop`, via the
/// whole-file demand path (`open_sequential_file` → fetch-through-cache):
/// a hit serves from the local cache; a miss pays the modeled remote fetch
/// INLINE and fills the cache — the §4.1 "whole-file fetch on a cold probe"
/// lazy-warm shape.
fn foreground_probe(
    fs: &CachedFileSystem,
    stop: &AtomicBool,
    warmed_at: &Mutex<Option<Instant>>,
    started: Instant,
) -> (usize, Vec<u128>) {
    let set = paths();
    let mut lat = Vec::with_capacity(1 << 16);
    let mut ops = 0usize;
    let mut seed = 0x9E3779B97F4A7C15u64;
    let mut buf = vec![0u8; 16 * 1024];
    let _ = started;
    while !stop.load(Ordering::Relaxed) {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        // 80% of probes hit a 25%-of-set hot subset, 20% roam the tail.
        let r = (seed >> 33) as usize;
        let idx = if r % 5 < 4 {
            r % (FILES / 4)
        } else {
            FILES / 4 + r % (FILES - FILES / 4)
        };
        let t = Instant::now();
        let mut f = fs.open_sequential_file(&set[idx]).unwrap();
        let _ = f.read(&mut buf).unwrap();
        lat.push(t.elapsed().as_micros());
        ops += 1;
        // Track warm completion (all files cache-resident).
        if warmed_at.lock().unwrap().is_none()
            && set.iter().all(|p| fs.cache().contains(p.to_str().unwrap()))
        {
            *warmed_at.lock().unwrap() = Some(Instant::now());
        }
    }
    (ops, lat)
}

fn pct(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn run_cell(name: &'static str, fill: Option<BackgroundFillParams>) -> CellOut {
    let remote = build_remote();
    let tmp = tempfile::TempDir::new().unwrap();
    let cache = LocalCache::open_with_policy(
        tmp.path(),
        (FILES as u64 + 8) * (FILE_KB as u64) * 1024,
        CachePolicy::default(),
    )
    .unwrap();
    let fs = Arc::new(CachedFileSystem::new(remote, Arc::new(cache)));

    let stop = AtomicBool::new(false);
    let warmed_at: Mutex<Option<Instant>> = Mutex::new(None);
    let started = Instant::now();

    let (ops, mut lat) = std::thread::scope(|s| {
        let bf = fill.map(|p| BackgroundFill::start(fs.clone(), paths(), p));
        let fg = s.spawn(|| foreground_probe(&fs, &stop, &warmed_at, started));
        // Run the foreground window, then stop.
        std::thread::sleep(Duration::from_millis(FG_WINDOW_MS));
        stop.store(true, Ordering::Relaxed);
        let out = fg.join().unwrap();
        if let Some(bf) = bf {
            let _ = bf.wait();
        }
        out
    });
    let warm_ms = warmed_at
        .lock()
        .unwrap()
        .map(|t| (t - started).as_millis())
        .unwrap_or(u128::MAX); // never warmed inside the window
    lat.sort_unstable();
    // Foreground IMPACT lives in the tail: the ops that stalled behind a
    // whole-file fetch (inline demand fill or channel contention).
    let slow = lat.iter().filter(|&&l| l > 1000).count();
    CellOut {
        name,
        warm_ms,
        fg_ops: ops,
        fg_p50_us: pct(&lat, 0.50),
        fg_p999_us: pct(&lat, 0.999),
        fg_max_us: lat.last().copied().unwrap_or(0),
        fg_slow_ops: slow,
    }
}

fn main() {
    println!(
        "restore_bgfill_bench: {FILES} files x {FILE_KB} KiB, remote {REMOTE_LATENCY_MS} ms/GET \
         x{REMOTE_CHANNEL} channel, fg window {FG_WINDOW_MS} ms"
    );
    let cells = [
        run_cell("lazy", None),
        run_cell(
            "bgfill-fast",
            Some(BackgroundFillParams {
                workers: 3,
                pace_bytes_per_sec: 0,
            }),
        ),
        run_cell(
            "bgfill-paced",
            Some(BackgroundFillParams {
                workers: 2,
                pace_bytes_per_sec: 8 * 1024 * 1024, // 8 MiB/s ⇒ ~2 s warm
            }),
        ),
    ];
    println!(
        "{:<14} {:>9} {:>9} {:>10} {:>11} {:>10} {:>12}",
        "cell", "warm_ms", "fg_ops", "fg_p50_us", "fg_p999_us", "fg_max_us", "fg_slow(>1ms)"
    );
    for c in cells {
        let warm = if c.warm_ms == u128::MAX {
            "never".to_string()
        } else {
            c.warm_ms.to_string()
        };
        println!(
            "{:<14} {:>9} {:>9} {:>10} {:>11} {:>10} {:>12}",
            c.name, warm, c.fg_ops, c.fg_p50_us, c.fg_p999_us, c.fg_max_us, c.fg_slow_ops
        );
    }
}
