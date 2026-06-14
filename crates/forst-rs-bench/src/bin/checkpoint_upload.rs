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

//! FRS-PHASE2 — **checkpoint multi-file upload: serial copy loop vs parallel
//! multi-file PUT** (catalog item #4, `2026-06-13-forst-optimization-catalog.md`
//! §3.2).
//!
//! ## The bottleneck this quantifies
//!
//! At a checkpoint barrier the engine copies every live SST into the checkpoint
//! directory via `copy_live_ssts` (`checkpoint.rs`), historically a **serial
//! `for` loop** calling `copy_file` once per SST. On disaggregated state the
//! checkpoint dir is a REMOTE object store, so each `copy_file` ends in a
//! `flush()`+`sync()` that completes an object PUT — a full remote round-trip.
//! The loop pays **N × PUT-RTT serially** at the barrier even though every PUT
//! is INDEPENDENT (distinct object keys) and the object-store backend can
//! absorb many in flight (the opendal backend's `MAX_INFLIGHT_UPLOADS=8`,
//! `opendal_backend.rs:196`).
//!
//! ForSt's UFS link-mode uploads ZERO at the barrier (paper §5.2, PVLDB
//! 18(12)), but for the flush-time upload that remains it streams files through
//! the file-system layer concurrently. forst-rs's serial loop under-fills the
//! 8-wide pipe → the barrier wall is the term this bench isolates.
//!
//! ## What is MODELED
//!
//! Per-object PUT latency is modeled as a fixed `flush()/sync()` sleep on the
//! writable file (`FRS_MODEL_RTT_MS`, default 23 ms = recorded dev-Mac→BOS; set
//! 2 for the ≥50 Gb/s intra-DC online box that `FRS_MODEL_BW_MBPS=6250`
//! targets). The SST bytes are tiny so the bandwidth term is negligible — this
//! isolates the **op-latency / round-trip** term, which is what a serial loop
//! over many small-to-medium objects pays. The "serial" arm copies the N files
//! back-to-back (the serial `copy_live_ssts_serial`); the "parallel" arm fans
//! the N copies across a bounded pool of `min(N, POOL)` workers (the
//! `FRS_CKPT_PARALLEL_UPLOAD` path), so the wall ≈ ceil(N/POOL) × PUT-RTT.
//!
//! This is the checkpoint-BARRIER latency term ONLY; it does not model
//! steady-state flush-time async upload (already decoupled) or link-mode
//! (zero-upload barrier).
//!
//! Run: `cargo run -p forst-rs-bench --release --bin checkpoint_upload`
//!      `cargo run -p forst-rs-bench --release --bin checkpoint_upload -- --smoke`

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use forst_rs_common::ForstResult;
use forst_rs_engine::checkpoint::copy_file;
use forst_rs_io::{
    FileMetadata, FileSystem, MemoryFileSystem, RandomAccessFile, SequentialFile, WritableFile,
    WriteMode,
};

/// Bounded PUT concurrency the parallel path uses — mirrors the opendal
/// backend's `MAX_INFLIGHT_UPLOADS=8` (`opendal_backend.rs:196`).
fn pool_size() -> usize {
    std::env::var("FRS_CKPT_UPLOAD_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8)
}

fn rtt() -> Duration {
    let ms = std::env::var("FRS_MODEL_RTT_MS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(23.0);
    Duration::from_secs_f64(ms / 1e3)
}

/// A writable file that sleeps `rtt` once on the terminating `sync()` —
/// modeling the object-store PUT round-trip that completes when `copy_file`
/// flushes+syncs the destination. The inner `MemoryFileSystem` file holds the
/// bytes (so byte-identity still holds); only the latency is injected.
struct PutLatencyFile {
    inner: Box<dyn WritableFile>,
    rtt: Duration,
    synced: bool,
}

impl WritableFile for PutLatencyFile {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        self.inner.append(data)
    }
    fn flush(&mut self) -> ForstResult<()> {
        self.inner.flush()
    }
    fn sync(&mut self) -> ForstResult<()> {
        // The PUT completes (object becomes durable) on the first sync — one
        // remote round-trip per object.
        if !self.synced {
            std::thread::sleep(self.rtt);
            self.synced = true;
        }
        self.inner.sync()
    }
    fn file_size(&self) -> ForstResult<u64> {
        self.inner.file_size()
    }
}

/// Wraps a `MemoryFileSystem`, injecting a per-object PUT round-trip on the
/// destination `sync()`. `is_local()==false` puts `copy_file` on the remote
/// path (straight-to-final, no rename). Source reads are local memory (no
/// latency) — only the DESTINATION PUT pays RTT, isolating the upload term.
struct PutLatencyFileSystem {
    inner: Arc<dyn FileSystem>,
    rtt: Duration,
}

impl FileSystem for PutLatencyFileSystem {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        self.inner.open_sequential_file(path)
    }
    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        self.inner.open_random_access_file(path)
    }
    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        let inner = self.inner.open_writable_file(path, mode)?;
        Ok(Box::new(PutLatencyFile {
            inner,
            rtt: self.rtt,
            synced: false,
        }))
    }
    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        self.inner.file_exists(path)
    }
    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata> {
        self.inner.get_file_metadata(path)
    }
    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<FileMetadata>> {
        self.inner.list_dir(dir)
    }
    fn create_dir_all(&self, dir: &Path) -> ForstResult<()> {
        self.inner.create_dir_all(dir)
    }
    fn delete_file(&self, path: &Path) -> ForstResult<()> {
        self.inner.delete_file(path)
    }
    fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()> {
        self.inner.delete_dir(path, recursive)
    }
    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        self.inner.rename(src, dst)
    }
    // Remote regime: no atomic rename ⇒ copy_file streams straight to final key.
    fn supports_atomic_rename(&self) -> bool {
        false
    }
    fn is_local(&self) -> bool {
        false
    }
    fn name(&self) -> &str {
        "PutLatencyFileSystem(checkpoint_upload bench)"
    }
}

/// Writes `n` source SST files of `bytes_each` into `src_dir` (no latency).
fn seed_sources(fs: &dyn FileSystem, src_dir: &Path, n: usize, bytes_each: usize) -> Vec<PathBuf> {
    fs.create_dir_all(src_dir).expect("mkdir src");
    let payload = vec![0xABu8; bytes_each];
    let mut paths = Vec::with_capacity(n);
    for i in 0..n {
        let p = src_dir.join(format!("{i:06}.sst"));
        let mut wf = fs
            .open_writable_file(&p, WriteMode::CreateOrTruncate)
            .expect("open src");
        wf.append(&payload).expect("append src");
        wf.flush().expect("flush src");
        wf.sync().expect("sync src");
        paths.push(p);
    }
    paths
}

/// CURRENT serial copy loop. Wall ≈ N × PUT-RTT.
fn copy_serial(fs: &dyn FileSystem, srcs: &[PathBuf], dst_dir: &Path) -> u64 {
    fs.create_dir_all(dst_dir).expect("mkdir dst");
    let mut total = 0u64;
    for src in srcs {
        let dst = dst_dir.join(src.file_name().unwrap());
        total += copy_file(fs, src, &dst).expect("copy");
    }
    total
}

/// Parallel multi-file PUT: fan the N independent copies across a bounded pool
/// of `min(N, POOL)` workers. Wall ≈ ceil(N/POOL) × PUT-RTT. Mirrors the
/// engine's `copy_live_ssts_parallel` dispatch (bench is a standalone harness).
fn copy_parallel(fs: Arc<dyn FileSystem>, srcs: &[PathBuf], dst_dir: &Path, pool: usize) -> u64 {
    fs.create_dir_all(dst_dir).expect("mkdir dst");
    let total = Arc::new(Mutex::new(0u64));
    let next = Arc::new(Mutex::new(0usize));
    let srcs: Arc<Vec<PathBuf>> = Arc::new(srcs.to_vec());
    let dst_dir: Arc<PathBuf> = Arc::new(dst_dir.to_path_buf());
    let workers = pool.min(srcs.len()).max(1);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let fs = Arc::clone(&fs);
            let total = Arc::clone(&total);
            let next = Arc::clone(&next);
            let srcs = Arc::clone(&srcs);
            let dst_dir = Arc::clone(&dst_dir);
            scope.spawn(move || loop {
                let i = {
                    let mut g = next.lock().unwrap();
                    let i = *g;
                    if i >= srcs.len() {
                        break;
                    }
                    *g = i + 1;
                    i
                };
                let dst = dst_dir.join(srcs[i].file_name().unwrap());
                let bytes = copy_file(fs.as_ref(), &srcs[i], &dst).expect("copy");
                *total.lock().unwrap() += bytes;
            });
        }
    });
    Arc::try_unwrap(total).unwrap().into_inner().unwrap()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");
    // Cap the modeled RTT in smoke/CI so the serial arm's N×RTT stays bounded.
    let rtt = if smoke {
        rtt()
            .min(Duration::from_millis(3))
            .max(Duration::from_millis(1))
    } else {
        rtt()
    };
    let pool = pool_size();
    let n_list: &[usize] = if smoke { &[16] } else { &[8, 16, 32, 64] };
    let bytes_each = if smoke { 4 * 1024 } else { 64 * 1024 };
    let reps = if smoke { 1 } else { 3 };

    println!(
        "checkpoint_upload bench — modeled PUT-RTT={:.1} ms (FRS_MODEL_RTT_MS), \
         pool={} (FRS_CKPT_UPLOAD_THREADS), bytes/sst={}, reps={}\n",
        rtt.as_secs_f64() * 1e3,
        pool,
        bytes_each,
        reps
    );
    println!(
        "{:>6} {:>14} {:>14} {:>10}",
        "N", "serial (ms)", "parallel (ms)", "speedup"
    );

    for &n in n_list {
        let mut ser = Vec::with_capacity(reps);
        let mut par = Vec::with_capacity(reps);
        for _ in 0..reps {
            // Fresh FS per rep so each arm copies into an empty dst.
            let inner: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
            let fs: Arc<dyn FileSystem> = Arc::new(PutLatencyFileSystem { inner, rtt });
            let srcs = seed_sources(fs.as_ref(), Path::new("/db"), n, bytes_each);

            let t = Instant::now();
            let bs = copy_serial(fs.as_ref(), &srcs, Path::new("/ckpt-serial"));
            ser.push(t.elapsed().as_secs_f64() * 1e3);

            let t = Instant::now();
            let bp = copy_parallel(Arc::clone(&fs), &srcs, Path::new("/ckpt-par"), pool);
            par.push(t.elapsed().as_secs_f64() * 1e3);

            assert_eq!(bs, bp, "serial/parallel byte counts must match");
            assert_eq!(bs, (n * bytes_each) as u64, "copied byte count wrong");
        }
        let s = median(ser);
        let p = median(par);
        println!("{n:>6} {s:>14.1} {p:>14.1} {:>9.2}x", s / p);
    }
    println!(
        "\nInterpretation: serial ≈ N×RTT (serial copy_live_ssts); parallel ≈ \
         ceil(N/{pool})×RTT (FRS_CKPT_PARALLEL_UPLOAD). The gap is the \
         checkpoint-barrier upload term on disaggregated/remote state."
    );
}
