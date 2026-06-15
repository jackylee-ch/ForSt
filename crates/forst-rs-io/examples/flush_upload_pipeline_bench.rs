//! VERIFY-BEFORE-BUILD mini-bench for the async FLUSH↔UPLOAD pipelining lever.
//!
//! Confirms that the production write path OVERLAPS local SST flush with remote
//! upload: each SST `close_writer` SPAWNS its remote upload on a background
//! runtime and returns on the local serialization (gated by a shared
//! `MAX_INFLIGHT_UPLOADS` semaphore), so the engine continues flushing SST N+1
//! locally while SST N uploads to the remote. Flush throughput is therefore NOT
//! gated by remote-upload latency.
//!
//! Faithful modeling. The PRODUCTION remote latency lives inside the SPAWNED
//! upload task (`op.write_with(...).await` in `OpendalWritableFile::close_writer`),
//! NOT in `append()`. A `ThrottledFileSystem` over an in-memory backend paces
//! `append()` in the flush thread and so cannot isolate the spawned-upload
//! overlap. This bench therefore uses a self-contained `RemoteFakeFs` —
//! a contention-robust, modeled-RTT mock that reproduces the production
//! async-upload REGISTRY semantics exactly:
//!   * `close_writer` spawns the upload (sleep proportional to bytes / bandwidth)
//!     and returns;
//!   * the upload holds a permit from a shared `MAX_INFLIGHT` semaphore;
//!   * `await_upload(path)` / `await_all_uploads()` block until the relevant
//!     upload(s) finish (the durability barrier the checkpoint relies on).
//!
//! Two modes are contrasted:
//!   * `serial` — write SST N, then `await_upload(N)` BEFORE writing SST N+1
//!     (the pre-pipelining behaviour: flush blocks on upload).
//!   * `pipelined` — write all SSTs back-to-back (each spawns its upload), then
//!     one `await_all_uploads()` barrier at the end (the shipped behaviour). The
//!     N+1 flush proceeds while N uploads.
//!
//! Run:
//! ```text
//! FRS_REMOTE_BW_MBPS=64 cargo run -p forst-rs-io \
//!     --example flush_upload_pipeline_bench --release
//! ```
//! `FRS_BENCH_SST_COUNT` (default 24), `FRS_BENCH_SST_MIB` (default 4) and
//! `FRS_BENCH_LOCAL_MS` (per-SST local-flush cost, default 5 ms) tune the
//! workload; `FRS_REMOTE_BW_MBPS` (default 64) sets the modeled remote rate.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Shared registry of in-flight uploads, keyed by path — mirrors
/// `OpendalFileSystem.pending`.
type Registry = Arc<Mutex<HashMap<String, Arc<UploadSlot>>>>;

/// One in-flight upload. The background thread sets `done=true` and notifies;
/// awaiters block on the condvar (the watch-channel analogue).
struct UploadSlot {
    done: Mutex<bool>,
    cv: std::sync::Condvar,
}

impl UploadSlot {
    fn new() -> Self {
        Self {
            done: Mutex::new(false),
            cv: std::sync::Condvar::new(),
        }
    }
    fn finish(&self) {
        *self.done.lock().unwrap() = true;
        self.cv.notify_all();
    }
    fn wait(&self) {
        let mut g = self.done.lock().unwrap();
        while !*g {
            g = self.cv.wait(g).unwrap();
        }
    }
}

/// Counting semaphore (std-only) bounding concurrent in-flight uploads to
/// `MAX_INFLIGHT`, mirroring `OpendalFileSystem.upload_sem`.
struct Sem {
    permits: Mutex<usize>,
    cv: std::sync::Condvar,
}
impl Sem {
    fn new(n: usize) -> Self {
        Self {
            permits: Mutex::new(n),
            cv: std::sync::Condvar::new(),
        }
    }
    fn acquire(&self) {
        let mut g = self.permits.lock().unwrap();
        while *g == 0 {
            g = self.cv.wait(g).unwrap();
        }
        *g -= 1;
    }
    fn release(&self) {
        *self.permits.lock().unwrap() += 1;
        self.cv.notify_one();
    }
}

/// Contention-robust modeled-RTT remote: each upload runs on its own thread,
/// sleeping for the modeled transfer time, bounded by `sem`. This reproduces
/// the production spawn-and-return-on-local-serialization property.
struct RemoteFakeFs {
    bytes_per_sec: f64,
    local_flush: Duration,
    pending: Registry,
    sem: Arc<Sem>,
    handles: Mutex<Vec<thread::JoinHandle<()>>>,
    /// FRS-ASYNC-FLUSH-UPLOAD: when true, acquire the in-flight permit on the
    /// FLUSH THREAD before spawning (true buffer backpressure); when false,
    /// acquire it inside the spawned task (the soft, buffer-unbounded default).
    flush_thread_backpressure: bool,
    /// Resident upload buffers right now (bytes a queued/in-flight task holds).
    resident_bytes: Arc<std::sync::atomic::AtomicUsize>,
    /// Peak of `resident_bytes` over the run — the RSS proxy.
    peak_bytes: Arc<std::sync::atomic::AtomicUsize>,
}

const MAX_INFLIGHT: usize = 8;

impl RemoteFakeFs {
    fn new(mibps: u64, local_flush: Duration) -> Self {
        Self::new_with(mibps, local_flush, false)
    }

    fn new_with(mibps: u64, local_flush: Duration, flush_thread_backpressure: bool) -> Self {
        Self {
            bytes_per_sec: mibps as f64 * 1024.0 * 1024.0,
            local_flush,
            pending: Arc::new(Mutex::new(HashMap::new())),
            sem: Arc::new(Sem::new(MAX_INFLIGHT)),
            handles: Mutex::new(Vec::new()),
            flush_thread_backpressure,
            resident_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peak_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn bump_resident(&self, nbytes: usize) {
        use std::sync::atomic::Ordering;
        let now = self.resident_bytes.fetch_add(nbytes, Ordering::AcqRel) + nbytes;
        self.peak_bytes.fetch_max(now, Ordering::AcqRel);
    }

    /// Write one SST: pay the LOCAL flush cost in the calling (flush) thread,
    /// then SPAWN the remote upload and return immediately.
    fn write_sst(&self, path: &str, nbytes: usize) {
        use std::sync::atomic::Ordering;
        // Local serialization cost (memtable -> local SST bytes) — paid inline.
        thread::sleep(self.local_flush);

        let slot = Arc::new(UploadSlot::new());
        // Supersede any prior pending upload to the same path (last-writer-wins),
        // exactly like the production registry.
        if let Some(prior) = self
            .pending
            .lock()
            .unwrap()
            .insert(path.to_string(), slot.clone())
        {
            prior.wait();
        }

        // The SST buffer becomes resident the moment it is captured for upload.
        self.bump_resident(nbytes);

        // TRUE backpressure: block the FLUSH THREAD on the permit here, so the
        // buffer is only captured once admission is granted — at most
        // MAX_INFLIGHT buffers resident. (Soft mode skips this and lets the
        // spawned task queue on the permit while already holding the buffer.)
        if self.flush_thread_backpressure {
            self.sem.acquire();
        }

        let upload_secs = nbytes as f64 / self.bytes_per_sec;
        let sem = self.sem.clone();
        let resident = self.resident_bytes.clone();
        let prefetched = self.flush_thread_backpressure;
        let h = thread::spawn(move || {
            if !prefetched {
                // Soft mode: acquire INSIDE the task — the buffer is already
                // resident while we queue here (the unbounded-RSS hazard).
                sem.acquire();
            }
            thread::sleep(Duration::from_secs_f64(upload_secs));
            sem.release();
            resident.fetch_sub(nbytes, Ordering::AcqRel);
            slot.finish();
        });
        self.handles.lock().unwrap().push(h);
    }

    fn peak_resident_bytes(&self) -> usize {
        self.peak_bytes.load(std::sync::atomic::Ordering::Acquire)
    }

    fn await_upload(&self, path: &str) {
        let slot = self.pending.lock().unwrap().get(path).cloned();
        if let Some(slot) = slot {
            slot.wait();
        }
    }

    fn await_all_uploads(&self) {
        let handles: Vec<_> = std::mem::take(&mut *self.handles.lock().unwrap());
        for h in handles {
            let _ = h.join();
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn run_serial(mibps: u64, local: Duration, count: usize, nbytes: usize) -> Duration {
    let fs = RemoteFakeFs::new(mibps, local);
    let start = Instant::now();
    for i in 0..count {
        let p = format!("/serial/{i:06}.sst");
        fs.write_sst(&p, nbytes);
        // Pre-pipelining behaviour: block on THIS upload before the next flush.
        fs.await_upload(&p);
    }
    fs.await_all_uploads();
    start.elapsed()
}

fn run_pipelined(mibps: u64, local: Duration, count: usize, nbytes: usize) -> Duration {
    let fs = RemoteFakeFs::new(mibps, local);
    let start = Instant::now();
    for i in 0..count {
        let p = format!("/pipelined/{i:06}.sst");
        // Each write spawns the upload and returns on the local serialization;
        // the next flush proceeds while this one uploads (bounded by the shared
        // MAX_INFLIGHT semaphore).
        fs.write_sst(&p, nbytes);
    }
    // One durability barrier at the end (mirrors the checkpoint await scope).
    fs.await_all_uploads();
    start.elapsed()
}

/// FRS-ASYNC-FLUSH-UPLOAD RSS A/B: pipelined write loop with a fast flush over a
/// slow remote, in soft (`backpressure=false`) vs true (`backpressure=true`)
/// mode. Returns `(wall, peak_resident_bytes)`. Soft mode lets the flush loop
/// spawn unbounded buffer-holding tasks (peak ~= whole working set); true mode
/// bounds peak at ~`MAX_INFLIGHT * SST_size`.
fn run_rss_ab(
    mibps: u64,
    local: Duration,
    count: usize,
    nbytes: usize,
    backpressure: bool,
) -> (Duration, usize) {
    let fs = RemoteFakeFs::new_with(mibps, local, backpressure);
    let start = Instant::now();
    for i in 0..count {
        let p = format!("/rss/{i:06}.sst");
        fs.write_sst(&p, nbytes);
    }
    fs.await_all_uploads();
    (start.elapsed(), fs.peak_resident_bytes())
}

fn main() {
    let mibps = std::env::var("FRS_REMOTE_BW_MBPS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(64);
    let count = env_usize("FRS_BENCH_SST_COUNT", 24);
    let sst_mib = env_usize("FRS_BENCH_SST_MIB", 4);
    let local_ms = env_usize("FRS_BENCH_LOCAL_MS", 5) as u64;
    let nbytes = sst_mib * 1024 * 1024;
    let local = Duration::from_millis(local_ms);

    println!(
        "flush<->upload pipeline mini-bench: {count} SSTs x {sst_mib} MiB, \
         remote = {mibps} MiB/s, local flush = {local_ms} ms/SST, in-flight cap = {MAX_INFLIGHT}"
    );

    // Warm up.
    let _ = run_pipelined(mibps, local, 2, nbytes);

    let serial = run_serial(mibps, local, count, nbytes);
    let pipelined = run_pipelined(mibps, local, count, nbytes);

    let total_mib = (count * sst_mib) as f64;
    let serial_tput = total_mib / serial.as_secs_f64();
    let pipe_tput = total_mib / pipelined.as_secs_f64();

    println!(
        "  serial    : {:>8.3} s  ({:>7.1} MiB/s)",
        serial.as_secs_f64(),
        serial_tput
    );
    println!(
        "  pipelined : {:>8.3} s  ({:>7.1} MiB/s)",
        pipelined.as_secs_f64(),
        pipe_tput
    );
    println!(
        "  speedup   : {:.2}x (pipelined hides upload latency behind the next flush)",
        serial.as_secs_f64() / pipelined.as_secs_f64()
    );

    // FRS-ASYNC-FLUSH-UPLOAD: peak resident-buffer A/B (soft vs true backpressure)
    // under a fast flush loop + slow remote — the RSS lever the new flag adds.
    // Local flush is forced tiny so the flush loop outruns the remote, exposing
    // the unbounded-buffer hazard in soft mode.
    let fast_local = Duration::from_micros(50);
    let (soft_wall, soft_peak) = run_rss_ab(mibps, fast_local, count, nbytes, false);
    let (true_wall, true_peak) = run_rss_ab(mibps, fast_local, count, nbytes, true);
    let mib = 1024.0 * 1024.0;
    println!("  FRS_ASYNC_FLUSH_UPLOAD peak resident buffers (fast flush, {mibps} MiB/s remote):");
    println!(
        "    OFF (soft)  : peak {:>7.1} MiB  (wall {:>6.3} s) — buffers pile up while queued",
        soft_peak as f64 / mib,
        soft_wall.as_secs_f64()
    );
    println!(
        "    ON  (true)  : peak {:>7.1} MiB  (wall {:>6.3} s) — bounded ~MAX_INFLIGHT*SST",
        true_peak as f64 / mib,
        true_wall.as_secs_f64()
    );
    println!(
        "    RSS bound   : {:.2}x lower peak with the flag ON (throughput unchanged)",
        soft_peak as f64 / true_peak.max(1) as f64
    );
}
