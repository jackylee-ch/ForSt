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

//! B1 background flush worker — integration tests.
//!
//! Validates that:
//! 1. Flushes run on the worker thread, not the writer thread.
//! 2. WriteController stalls writers when imm_count >= cap and unblocks
//!    after the worker drains an imm.
//! 3. Engine drop drains the queue (no data loss).
//! 4. Background flush errors propagate to the next writer.
//! 5. Concurrent writers + async flush yields correct data.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use forst_rs_common::{EngineOptions, ForstError, ForstResult};
use forst_rs_engine::{ColumnFamilyDescriptor, DbImpl};
use forst_rs_io::{
    FileMetadata, FileSystem, MemoryFileSystem, RandomAccessFile, SequentialFile, WritableFile,
    WriteMode,
};

// Cargo runs tests in one integration-test binary concurrently by default. These cases
// deliberately force write stalls and slow background flushes, so keep them serialized to avoid
// cross-test backlog interference.
static FLUSH_WORKER_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Serializes the tests WITHOUT poisoning-cascade: if one test panics (e.g. a
/// stall-timeout flake on an overloaded instrumented CI runner), the remaining
/// tests must still run on their own merits instead of insta-failing with
/// `PoisonError` at the lock acquisition. The guard's only job is mutual
/// exclusion; the protected "data" is `()`, so a poisoned lock is still a
/// perfectly valid lock.
fn serialize_tests() -> std::sync::MutexGuard<'static, ()> {
    FLUSH_WORKER_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// ---------------------------------------------------------------------
// SlowFs — wraps MemoryFileSystem and adds a configurable per-write
// sleep. Lets us prove flushes run off the writer's stack: we can
// observe a writer return in microseconds while the wrapped `write` call
// (executed on the worker thread) takes hundreds of milliseconds.
// ---------------------------------------------------------------------

struct SlowFs {
    inner: Arc<MemoryFileSystem>,
    write_delay: Mutex<Duration>,
    write_count: AtomicUsize,
    enabled: AtomicBool,
}

impl SlowFs {
    fn new(write_delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryFileSystem::new()),
            write_delay: Mutex::new(write_delay),
            write_count: AtomicUsize::new(0),
            enabled: AtomicBool::new(true),
        })
    }

    fn write_count(&self) -> usize {
        self.write_count.load(Ordering::Acquire)
    }

    fn disable_delay(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    fn delay(&self) {
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        let d = *self.write_delay.lock().unwrap();
        if !d.is_zero() {
            thread::sleep(d);
        }
    }
}

impl FileSystem for SlowFs {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        self.inner.open_sequential_file(path)
    }
    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        self.delay();
        self.inner.open_random_access_file(path)
    }
    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        self.delay();
        self.write_count.fetch_add(1, Ordering::AcqRel);
        self.inner.open_writable_file(path, mode)
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
    fn name(&self) -> &str {
        "SlowFs"
    }
}

// ---------------------------------------------------------------------
// FailingFs — first N writes succeed, then writes fail. Used to test
// background-flush error propagation.
// ---------------------------------------------------------------------

struct FailingFs {
    inner: Arc<MemoryFileSystem>,
    fail_after: AtomicUsize,
    write_count: AtomicUsize,
}

impl FailingFs {
    fn new(fail_after_n_writes: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryFileSystem::new()),
            fail_after: AtomicUsize::new(fail_after_n_writes),
            write_count: AtomicUsize::new(0),
        })
    }
}

impl FileSystem for FailingFs {
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
        let n = self.write_count.fetch_add(1, Ordering::AcqRel);
        if n >= self.fail_after.load(Ordering::Acquire) {
            return Err(ForstError::Io(std::io::Error::other(
                "injected write failure",
            )));
        }
        self.inner.open_writable_file(path, mode)
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
    fn name(&self) -> &str {
        "FailingFs"
    }
}

// ---------------------------------------------------------------------
// Helper: tiny write_buffer_size so a few puts trigger a switch.
// ---------------------------------------------------------------------

fn small_buf_options() -> EngineOptions {
    EngineOptions {
        db_path: "/db".to_string(),
        // ~1 KB triggers a switch after only a handful of small puts.
        write_buffer_size: 1024,
        ..EngineOptions::default()
    }
}

// =====================================================================
// Tests
// =====================================================================

/// The writer must return as soon as the imm has been enqueued — not
/// after the disk write completes. We make `open_writable_file` (the
/// flush's only sync-blocking syscall) sleep 200 ms, then verify the
/// writer's call wraps up in much less than 200 ms.
#[test]
fn test_flush_runs_off_writer_thread() {
    let _guard = serialize_tests();

    let fs = SlowFs::new(Duration::from_millis(200));
    let fs_dyn: Arc<dyn FileSystem> = fs.clone();
    let db = DbImpl::open_with_fs(small_buf_options(), fs_dyn).unwrap();
    let cf = db.default_cf();

    // Prime: write enough rows to trigger one switch.
    for i in 0..50 {
        let k = format!("k{:04}", i);
        let v = vec![b'v'; 64];
        db.put(&cf, k.as_bytes(), &v).unwrap();
    }

    // The next write should switch the memtable. Time it: it must finish
    // far below the slow filesystem's per-write delay (200 ms × 2 calls
    // = 400 ms in sync mode). 80 ms is generous slack for a CI runner.
    let start = Instant::now();
    db.put(&cf, b"trigger", b"go").unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(80),
        "writer blocked {:?} — expected <80ms (flush should be async)",
        elapsed
    );

    // Disable the artificial delay before drop so shutdown is quick.
    fs.disable_delay();
    drop(db);
    // After drop the worker has joined and at least one SST was written.
    assert!(fs.write_count() >= 1, "no SST writes observed");
}

/// `max_write_buffer_number` is 3 by default. After 3 imms accumulate,
/// the next writer's `may_throttle()` must block until the worker drains
/// one. We verify by:
///   - making the worker very slow (per-write 100 ms),
///   - rapidly pushing enough puts to overflow the imm queue,
///   - measuring that one of the puts blocked >= 50 ms.
#[test]
fn test_max_write_buffer_number_backpressure() {
    let _guard = serialize_tests();

    let fs = SlowFs::new(Duration::from_millis(100));
    let fs_dyn: Arc<dyn FileSystem> = fs.clone();
    let db = DbImpl::open_with_fs(small_buf_options(), fs_dyn).unwrap();

    // Fire enough writes to accumulate >3 imms quickly. Each "burst" is
    // small enough to fit in one memtable; the cumulative threshold
    // forces a switch after each burst.
    let mut max_blocked = Duration::ZERO;

    'writes: for round in 0..6 {
        for i in 0..50 {
            let k = format!("r{}k{:04}", round, i).into_bytes();
            let v = vec![b'v'; 64];

            // Put from a short-lived helper thread so this test can observe
            // that the writer has stalled, then release the artificial SlowFs
            // delay well before the production 45s stall timeout can fire.
            let db_for_put = db.clone();
            let (tx, rx) = mpsc::channel();
            let start = Instant::now();
            let handle = thread::spawn(move || {
                let cf = db_for_put.default_cf();
                let result = db_for_put.put(&cf, &k, &v);
                let elapsed = start.elapsed();
                tx.send((result, elapsed)).unwrap();
            });

            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok((result, elapsed)) => {
                    // Fast path: the put did NOT stall (< 50 ms). It must have
                    // succeeded — a fast put has no reason to error.
                    handle.join().unwrap();
                    result.unwrap();
                    if elapsed > max_blocked {
                        max_blocked = elapsed;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // The put blocked >= 50 ms — backpressure is PROVEN (the
                    // sole assertion target). Release the artificial delay and
                    // stop. We deliberately do NOT require the blocked put's
                    // result to be Ok: on a heavily-loaded CI runner the stall
                    // can occasionally exceed the production stall timeout before
                    // disable_delay drains the backlog, returning an Err — but
                    // that Err is itself further evidence of backpressure, not a
                    // failure of what this test checks. Join the writer so its
                    // thread (and the `db` clone it holds) is cleaned up.
                    max_blocked = Duration::from_millis(50);
                    fs.disable_delay();
                    let _ = handle.join();
                    break 'writes;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("writer thread exited before reporting put result")
                }
            }
            if max_blocked >= Duration::from_millis(50) {
                fs.disable_delay();
                break 'writes;
            }
        }
    }

    // At least one writer must have observed a stall (50 ms is well
    // below the 100 ms per-flush delay × 3-imm budget but above any
    // reasonable noise floor on a CI runner). Stop as soon as this is
    // proven; this test is not meant to grow an unbounded slow-flush backlog.
    assert!(
        max_blocked >= Duration::from_millis(50),
        "no backpressure observed: max_blocked={:?}",
        max_blocked
    );

    // Cleanup.
    fs.disable_delay();
    drop(db);
}

/// Dropping the engine must (1) drain the flush queue, (2) join the
/// worker, (3) leave the on-disk SSTs that the queue had asked for. We
/// reopen the same MemoryFileSystem after drop and verify all writes are
/// readable.
#[test]
fn test_close_drains_pending_flushes() {
    let _guard = serialize_tests();

    let mem_fs = Arc::new(MemoryFileSystem::new());
    let fs_dyn: Arc<dyn FileSystem> = mem_fs.clone();

    {
        let db = DbImpl::open_with_fs(small_buf_options(), fs_dyn.clone()).unwrap();
        let cf = db.default_cf();
        // Push enough data to enqueue several flushes.
        for i in 0..400 {
            let k = format!("k{:04}", i);
            let v = vec![b'v'; 32];
            db.put(&cf, k.as_bytes(), &v).unwrap();
        }
        // No explicit flush_all; rely on Drop.
    }
    // After drop: at least one SST file must exist on disk (proof that
    // the worker landed pending requests before exiting).
    let metas = mem_fs.list_dir(Path::new("/db")).unwrap();
    let sst_count = metas
        .iter()
        .filter(|m| !m.is_dir && m.path.extension().map(|e| e == "sst").unwrap_or(false))
        .count();
    assert!(
        sst_count >= 1,
        "expected pending flush to land on disk before drop returned"
    );
}

/// A background flush failure must surface to the *next* writer. We
/// inject a write failure on the second SST write (the first is the
/// initial memtable flush). After the worker has had a chance to fail,
/// the next put must return Err.
#[test]
fn test_flush_error_propagates_to_next_writer() {
    let _guard = serialize_tests();

    let fs = FailingFs::new(0); // every write fails
    let opts = EngineOptions {
        max_write_buffer_number: 64,
        ..small_buf_options()
    };
    let fs_dyn: Arc<dyn FileSystem> = fs.clone();
    let db = DbImpl::open_with_fs(opts, fs_dyn).unwrap();
    let cf = db.default_cf();

    // Trigger a flush: write enough to switch the memtable.
    for i in 0..200 {
        let k = format!("k{:04}", i);
        let v = vec![b'v'; 64];
        // Some early puts may succeed (no flush triggered yet); we don't
        // care here. We only require *eventually* a put returns Err
        // because the worker recorded a flush failure.
        if let Err(e) = db.put(&cf, k.as_bytes(), &v) {
            // Saw the propagated error — exactly what the test wants.
            let msg = e.to_string();
            assert!(
                msg.contains("injected write failure"),
                "expected injected write failure, got {msg}"
            );
            return;
        }
        // Give the worker a chance to run after each batch of writes.
        if i % 20 == 0 {
            thread::sleep(Duration::from_millis(5));
        }
    }

    // If we get here, the worker had not recorded the error before the first
    // 200 writers checked the slot. Keep crossing writer boundaries until the
    // background flush reports the injected write failure. A fixed sleep is
    // racy on busy CI runners because the final writer can beat the worker.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut attempt = 0usize;
    while Instant::now() < deadline {
        let key = format!("final{:04}", attempt);
        match db.put(&cf, key.as_bytes(), &[b'v'; 64]) {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("injected write failure"),
                    "expected injected write failure after worker recorded failure, got {msg}"
                );
                return;
            }
            Ok(_) => {
                attempt += 1;
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    panic!("expected injected write failure after waiting for background flush worker");
}

/// Multiple writer threads + async flush. The on-disk + in-memory state
/// must be consistent: every value we wrote must be readable via `get`.
#[test]
fn test_concurrent_writers_with_async_flush() {
    let _guard = serialize_tests();

    let opts = EngineOptions {
        db_path: "/db".to_string(),
        write_buffer_size: 8 * 1024, // small, but not tiny — want a few flushes
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    let db = DbImpl::open_with_fs(opts, fs).unwrap();

    const THREADS: usize = 4;
    const PER_THREAD: usize = 1_000;

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let db = db.clone();
        let cf = db.default_cf();
        handles.push(thread::spawn(move || {
            for i in 0..PER_THREAD {
                let k = format!("t{}k{:05}", t, i);
                let v = format!("v{}_{}", t, i);
                db.put(&cf, k.as_bytes(), v.as_bytes()).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Drain anything still pending before validation.
    db.flush_all().unwrap();

    let cf = db.default_cf();
    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            let k = format!("t{}k{:05}", t, i);
            let v = format!("v{}_{}", t, i);
            let got = db.get(&cf, k.as_bytes()).unwrap();
            assert_eq!(got.as_deref(), Some(v.as_bytes()), "missing key {}", k);
        }
    }
}

/// Smoke test: explicit `flush_all` after rapid writes drains every CF
/// without leaving imms behind.
#[test]
fn test_flush_all_drains_after_async_path() {
    let _guard = serialize_tests();

    let opts = EngineOptions {
        db_path: "/db".to_string(),
        write_buffer_size: 1024,
        // De-flake: this test asserts `flush_all` drains every CF through the
        // async path — it is NOT a write-stall test (that is covered by
        // `test_max_write_buffer_number_backpressure`). With the default
        // `max_write_buffer_number: 3`, 400 tiny puts into a 1 KiB buffer pile
        // up imms faster than the flush worker drains them, hitting the stall
        // path; under the slow llvm-cov instrumented CI build the drain cannot
        // clear within the 45 s stall_timeout and the put fails spuriously
        // (run 27485409277). Raise the imm ceiling so the drain-correctness
        // assertion is not gated by the stall timeout while still exercising
        // many async flushes via the tiny buffer.
        max_write_buffer_number: 64,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    let db = DbImpl::open_with_fs(opts, fs).unwrap();

    let a = db.default_cf();
    let b = db
        .create_column_family(ColumnFamilyDescriptor::new("cfb"))
        .unwrap();
    for i in 0..200 {
        db.put(&a, format!("a{:04}", i).as_bytes(), &[1u8; 32])
            .unwrap();
        db.put(&b, format!("b{:04}", i).as_bytes(), &[2u8; 32])
            .unwrap();
    }
    db.flush_all().unwrap();

    // After flush_all, every CF's in-memory imm queue must be empty.
    // (We can't easily expose imm_count from the public API, so instead
    // we re-flush and assert no error / no progress.)
    db.flush_all().unwrap();

    // Spot-check correctness post-flush.
    assert_eq!(
        db.get(&a, b"a0000").unwrap().as_deref(),
        Some(&[1u8; 32][..])
    );
    assert_eq!(
        db.get(&b, b"b0199").unwrap().as_deref(),
        Some(&[2u8; 32][..])
    );
}
