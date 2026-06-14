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

//! Bandwidth-throttling [`FileSystem`] decorator for LOCAL S3 simulation.
//!
//! [`ThrottledFileSystem`] wraps an arbitrary [`FileSystem`] and rate-limits
//! the BYTES that flow through its read and write handles using a token-bucket
//! [`RateLimiter`]. It is the seam used by the NEXMark "local S3 simulation":
//! the disaggregated-state remote leg is a *local* directory standing in for an
//! S3 bucket, and this decorator caps its effective bandwidth so a single box
//! can reproduce the latency/throughput regime of a real disaggregated store
//! (e.g. 50 Gb/s = 6250 MiB/s) without touching the network.
//!
//! # Where it sits
//!
//! It wraps the **remote** leg only (the OpenDAL "S3" directory) — the local
//! cache and the local store (`db_path`, WAL, MANIFEST, …) are untouched, so
//! cache hits and metadata chatter stay at native speed. This mirrors a real
//! disaggregated deployment, where only the object-store round-trips pay the
//! WAN bandwidth.
//!
//! # Flag gating — default OFF, byte-identical
//!
//! The rate is driven by the `FRS_REMOTE_BW_MBPS` env knob (MiB/s). A value of
//! `0` (the default / unset) means **unlimited**: [`RateLimiter::unlimited`] /
//! [`ThrottledFileSystem::from_env`] return a pass-through that never sleeps,
//! so the decorator is behaviorally identical to the bare backend when off.
//! Set `FRS_REMOTE_BW_MBPS=6250` for the 50 Gb/s S3-sim regime.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use forst_rs_common::error::ForstResult;

use crate::filesystem::{
    FileMetadata, FileSystem, RandomAccessFile, SequentialFile, WritableFile, WriteMode,
};

/// Environment knob (MiB/s) controlling the remote-store bandwidth cap.
///
/// `0` / unset = unlimited (decorator is a no-op pass-through). `6250` ≈
/// 50 Gb/s. See the module docs.
pub const REMOTE_BW_MBPS_ENV: &str = "FRS_REMOTE_BW_MBPS";

const MIB: f64 = 1024.0 * 1024.0;

/// A lock-free-ish token-bucket rate limiter measured in **bytes per second**.
///
/// The bucket refills continuously at `bytes_per_sec` and is capped at a
/// one-second burst. [`Self::throttle`] charges `n` bytes against the bucket
/// and, when the bucket would go negative, sleeps the calling thread for the
/// exact deficit / rate interval. When constructed [`unlimited`](Self::unlimited)
/// it short-circuits with zero overhead — no clock read, no lock, no sleep —
/// guaranteeing byte-identical behavior to the unwrapped backend.
///
/// The accounting uses a single `AtomicU64` holding "available token-nanos"
/// (bytes are converted to the time they represent at the configured rate),
/// updated with a CAS loop. Contention is low in practice: the SST read/write
/// path issues coarse-grained (KiB–MiB) charges, not per-byte ones.
#[derive(Debug)]
pub struct RateLimiter {
    /// `0` means unlimited (pass-through).
    bytes_per_sec: u64,
    /// Maximum burst the bucket may accumulate, in bytes (one second's worth).
    burst_bytes: u64,
    /// Shared mutable state, guarded by a CAS loop. Holds the timestamp (as
    /// nanos since `origin`) up to which the bucket's allowance has been
    /// "consumed". Reads/writes that arrive after this point get fresh tokens;
    /// reads/writes that arrive before it must wait until it passes.
    consumed_until_nanos: AtomicU64,
    /// Monotonic origin for the nanos timeline.
    origin: Instant,
}

impl RateLimiter {
    /// Creates a limiter that caps throughput at `bytes_per_sec`.
    ///
    /// `bytes_per_sec == 0` yields an [`unlimited`](Self::unlimited) limiter.
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            // One second of burst headroom so coarse bursts (a 4 MiB SST block
            // fetch) don't serialize sub-rate traffic.
            burst_bytes: bytes_per_sec,
            consumed_until_nanos: AtomicU64::new(0),
            origin: Instant::now(),
        }
    }

    /// Creates an unlimited (pass-through) limiter that never sleeps.
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// Builds a limiter from `mibps` MiB/s (`0` = unlimited).
    pub fn from_mibps(mibps: u64) -> Self {
        if mibps == 0 {
            return Self::unlimited();
        }
        Self::new((mibps as f64 * MIB) as u64)
    }

    /// Reads [`REMOTE_BW_MBPS_ENV`] and builds the corresponding limiter.
    ///
    /// Unset, empty, non-numeric, or `0` all map to [`unlimited`](Self::unlimited).
    pub fn from_env() -> Self {
        let mibps = std::env::var(REMOTE_BW_MBPS_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        Self::from_mibps(mibps)
    }

    /// Returns `true` when this limiter imposes no cap (pass-through).
    #[inline]
    pub fn is_unlimited(&self) -> bool {
        self.bytes_per_sec == 0
    }

    /// The configured cap in bytes/sec (`0` = unlimited).
    #[inline]
    pub fn bytes_per_sec(&self) -> u64 {
        self.bytes_per_sec
    }

    /// Charges `n` bytes against the bucket, sleeping the caller for the exact
    /// time needed to stay under the configured rate.
    ///
    /// No-op (and no clock read) when unlimited or when `n == 0`.
    pub fn throttle(&self, n: usize) {
        if self.bytes_per_sec == 0 || n == 0 {
            return;
        }
        let cost_nanos = (n as u128 * 1_000_000_000u128 / self.bytes_per_sec as u128) as u64;
        let burst_nanos =
            (self.burst_bytes as u128 * 1_000_000_000u128 / self.bytes_per_sec as u128) as u64;

        loop {
            let now = self.now_nanos();
            let prev = self.consumed_until_nanos.load(Ordering::Acquire);
            // The bucket's "consumed" frontier never trails `now` by more than
            // one burst (otherwise an idle period would bank unbounded tokens).
            let floor = now.saturating_sub(burst_nanos);
            let base = prev.max(floor);
            let new_until = base.saturating_add(cost_nanos);
            if self
                .consumed_until_nanos
                .compare_exchange_weak(prev, new_until, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                // We must wait until the frontier we just claimed is reached.
                if new_until > now {
                    let wait = new_until - now;
                    std::thread::sleep(Duration::from_nanos(wait));
                }
                return;
            }
            // CAS lost; retry with a fresh `now`.
        }
    }

    #[inline]
    fn now_nanos(&self) -> u64 {
        // Saturating cast: a 64-bit nanos counter overflows after ~584 years.
        self.origin.elapsed().as_nanos() as u64
    }
}

/// A [`FileSystem`] decorator that rate-limits the bytes flowing through the
/// files it opens, sharing one [`RateLimiter`] across every handle.
///
/// All directory/metadata operations delegate straight through (they move no
/// bulk bytes); only [`SequentialFile::read`], [`RandomAccessFile::read_at`] /
/// [`RandomAccessFile::read_ranges`], and [`WritableFile::append`] are charged.
pub struct ThrottledFileSystem {
    inner: Arc<dyn FileSystem>,
    limiter: Arc<RateLimiter>,
    /// Cumulative bytes charged through this decorator (diagnostics / smoke
    /// verification — proves the throttle is on the remote leg).
    bytes_charged: Arc<AtomicU64>,
}

impl std::fmt::Debug for ThrottledFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThrottledFileSystem")
            .field("inner", &self.inner.name())
            .field("limiter", &self.limiter)
            .field("bytes_charged", &self.bytes_charged())
            .finish()
    }
}

impl ThrottledFileSystem {
    /// Wraps `inner`, throttling its byte traffic with `limiter`.
    pub fn new(inner: Arc<dyn FileSystem>, limiter: Arc<RateLimiter>) -> Self {
        Self {
            inner,
            limiter,
            bytes_charged: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Wraps `inner` with a limiter built from [`REMOTE_BW_MBPS_ENV`].
    ///
    /// When the env knob is unset / `0`, the returned decorator is a
    /// byte-identical pass-through (the limiter never sleeps).
    pub fn from_env(inner: Arc<dyn FileSystem>) -> Self {
        Self::new(inner, Arc::new(RateLimiter::from_env()))
    }

    /// Returns `true` when the wrapped limiter imposes no cap.
    pub fn is_unlimited(&self) -> bool {
        self.limiter.is_unlimited()
    }

    /// Borrows the wrapped backend. Callers that detect an
    /// [`is_unlimited`](Self::is_unlimited) decorator can unwrap it to keep
    /// the FS stack byte-identical (no decorator indirection) when off.
    pub fn inner_arc(&self) -> &Arc<dyn FileSystem> {
        &self.inner
    }

    /// Total bytes charged through this decorator's read/write handles.
    pub fn bytes_charged(&self) -> u64 {
        self.bytes_charged.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// File handle wrappers
// ---------------------------------------------------------------------------

struct ThrottledSequential {
    inner: Box<dyn SequentialFile>,
    limiter: Arc<RateLimiter>,
    bytes_charged: Arc<AtomicU64>,
}

impl SequentialFile for ThrottledSequential {
    fn read(&mut self, buf: &mut [u8]) -> ForstResult<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.bytes_charged.fetch_add(n as u64, Ordering::Relaxed);
            self.limiter.throttle(n);
        }
        Ok(n)
    }

    fn skip(&mut self, n: u64) -> ForstResult<()> {
        // A skip transfers no bytes to the caller; do not charge it.
        self.inner.skip(n)
    }
}

struct ThrottledRandom {
    inner: Box<dyn RandomAccessFile>,
    limiter: Arc<RateLimiter>,
    bytes_charged: Arc<AtomicU64>,
}

impl RandomAccessFile for ThrottledRandom {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        let n = self.inner.read_at(offset, buf)?;
        if n > 0 {
            self.bytes_charged.fetch_add(n as u64, Ordering::Relaxed);
            self.limiter.throttle(n);
        }
        Ok(n)
    }

    fn read_ranges(&self, ranges: &[(u64, usize)]) -> ForstResult<Vec<Vec<u8>>> {
        let out = self.inner.read_ranges(ranges)?;
        let total: usize = out.iter().map(|v| v.len()).sum();
        if total > 0 {
            self.bytes_charged
                .fetch_add(total as u64, Ordering::Relaxed);
            // One charge for the whole concurrent batch: the backend issued the
            // ranges in parallel, so the wall cost is the aggregate bytes over
            // the link, not the serial sum of per-range sleeps.
            self.limiter.throttle(total);
        }
        Ok(out)
    }

    fn file_size(&self) -> ForstResult<u64> {
        self.inner.file_size()
    }

    fn is_local(&self) -> bool {
        // The wrapped leg is the remote/object store — preserve its locality
        // answer so the prefetcher keeps using the remote readahead regime.
        self.inner.is_local()
    }

    fn local_file_handle(&self) -> Option<std::sync::Arc<std::fs::File>> {
        // Intentionally None: handing back the raw local fd would let callers
        // bypass the throttle via io_uring direct reads.
        None
    }
}

struct ThrottledWritable {
    inner: Box<dyn WritableFile>,
    limiter: Arc<RateLimiter>,
    bytes_charged: Arc<AtomicU64>,
}

impl WritableFile for ThrottledWritable {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        self.inner.append(data)?;
        let n = data.len();
        if n > 0 {
            self.bytes_charged.fetch_add(n as u64, Ordering::Relaxed);
            self.limiter.throttle(n);
        }
        Ok(())
    }

    fn flush(&mut self) -> ForstResult<()> {
        self.inner.flush()
    }

    fn sync(&mut self) -> ForstResult<()> {
        self.inner.sync()
    }

    fn file_size(&self) -> ForstResult<u64> {
        self.inner.file_size()
    }
}

// ---------------------------------------------------------------------------
// FileSystem impl — bulk-byte ops are throttled, everything else delegates
// ---------------------------------------------------------------------------

impl FileSystem for ThrottledFileSystem {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        let inner = self.inner.open_sequential_file(path)?;
        if self.limiter.is_unlimited() {
            return Ok(inner);
        }
        Ok(Box::new(ThrottledSequential {
            inner,
            limiter: Arc::clone(&self.limiter),
            bytes_charged: Arc::clone(&self.bytes_charged),
        }))
    }

    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        let inner = self.inner.open_random_access_file(path)?;
        if self.limiter.is_unlimited() {
            return Ok(inner);
        }
        Ok(Box::new(ThrottledRandom {
            inner,
            limiter: Arc::clone(&self.limiter),
            bytes_charged: Arc::clone(&self.bytes_charged),
        }))
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        let inner = self.inner.open_writable_file(path, mode)?;
        if self.limiter.is_unlimited() {
            return Ok(inner);
        }
        Ok(Box::new(ThrottledWritable {
            inner,
            limiter: Arc::clone(&self.limiter),
            bytes_charged: Arc::clone(&self.bytes_charged),
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

    fn supports_atomic_rename(&self) -> bool {
        self.inner.supports_atomic_rename()
    }

    fn sync_dir(&self, dir: &Path) -> ForstResult<()> {
        self.inner.sync_dir(dir)
    }

    fn name(&self) -> &str {
        "ThrottledFileSystem"
    }

    fn is_local(&self) -> bool {
        self.inner.is_local()
    }

    fn ensure_cached(&self, path: &Path) -> ForstResult<()> {
        self.inner.ensure_cached(path)
    }

    fn prefetch_concurrent(&self, paths: &[&Path]) {
        self.inner.prefetch_concurrent(paths)
    }

    fn await_upload(&self, path: &Path) -> ForstResult<()> {
        self.inner.await_upload(path)
    }

    fn await_all_uploads(&self) -> ForstResult<()> {
        self.inner.await_all_uploads()
    }

    fn pre_seed_admission(&self, path: &Path) {
        self.inner.pre_seed_admission(path)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_fs::MemoryFileSystem;

    fn write_file(fs: &dyn FileSystem, path: &str, bytes: &[u8]) {
        let mut w = fs
            .open_writable_file(Path::new(path), WriteMode::CreateNew)
            .unwrap();
        w.append(bytes).unwrap();
        drop(w);
    }

    #[test]
    fn unlimited_is_passthrough_no_sleep() {
        let lim = RateLimiter::unlimited();
        assert!(lim.is_unlimited());
        let start = Instant::now();
        // Charging a gigabyte against an unlimited limiter must not sleep.
        lim.throttle(1024 * 1024 * 1024);
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn from_mibps_zero_is_unlimited() {
        assert!(RateLimiter::from_mibps(0).is_unlimited());
        assert!(!RateLimiter::from_mibps(10).is_unlimited());
        assert_eq!(RateLimiter::from_mibps(1).bytes_per_sec(), 1024 * 1024);
    }

    #[test]
    fn throttle_enforces_rate() {
        // 1 MiB/s; charging 256 KiB twice (after burst headroom is spent)
        // must take measurable time. Burst is 1 MiB so the first ~1 MiB is
        // free, then it paces. Charge 4 MiB total: ~3 MiB beyond burst → ≥~2.8s
        // at 1 MiB/s. Keep the assertion loose to avoid CI flakiness.
        let lim = RateLimiter::new(1024 * 1024);
        let start = Instant::now();
        for _ in 0..16 {
            lim.throttle(256 * 1024); // 16 * 256KiB = 4 MiB
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(1500),
            "expected pacing ≥1.5s for 4MiB at 1MiB/s, got {elapsed:?}"
        );
    }

    #[test]
    fn wrapped_fs_unlimited_returns_inner_handles() {
        let inner = Arc::new(MemoryFileSystem::new());
        inner.create_dir_all(Path::new("/d")).unwrap();
        write_file(inner.as_ref(), "/d/a.sst", b"hello-world");
        let fs = ThrottledFileSystem::new(
            Arc::clone(&inner) as Arc<dyn FileSystem>,
            Arc::new(RateLimiter::unlimited()),
        );
        assert!(fs.is_unlimited());
        // Read back through the decorator — content must be byte-identical.
        let mut r = fs.open_sequential_file(Path::new("/d/a.sst")).unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello-world");
    }

    #[test]
    fn wrapped_fs_charges_reads_and_writes() {
        let inner = Arc::new(MemoryFileSystem::new());
        inner.create_dir_all(Path::new("/d")).unwrap();
        let fs = ThrottledFileSystem::new(
            Arc::clone(&inner) as Arc<dyn FileSystem>,
            // Generous rate so the test does not actually sleep long, but the
            // byte accounting still runs.
            Arc::new(RateLimiter::from_mibps(100_000)),
        );
        // Write 11 bytes through the throttled writable.
        write_file(&fs, "/d/a.sst", b"hello-world");
        // Read them back through the throttled random reader.
        let r = fs.open_random_access_file(Path::new("/d/a.sst")).unwrap();
        let mut buf = [0u8; 11];
        let n = r.read_at(0, &mut buf).unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf, b"hello-world");
        // 11 written + 11 read = 22 charged.
        assert_eq!(fs.bytes_charged(), 22);
    }

    #[test]
    fn delegates_metadata_ops() {
        let inner = Arc::new(MemoryFileSystem::new());
        inner.create_dir_all(Path::new("/d")).unwrap();
        write_file(inner.as_ref(), "/d/a.sst", b"xyz");
        let fs = ThrottledFileSystem::new(
            Arc::clone(&inner) as Arc<dyn FileSystem>,
            Arc::new(RateLimiter::from_mibps(100_000)),
        );
        assert!(fs.file_exists(Path::new("/d/a.sst")).unwrap());
        assert_eq!(fs.get_file_metadata(Path::new("/d/a.sst")).unwrap().size, 3);
        assert_eq!(fs.list_dir(Path::new("/d")).unwrap().len(), 1);
        assert_eq!(fs.name(), "ThrottledFileSystem");
    }
}
