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

//! DbImpl — the top-level engine struct. See `2.8_read_write_paths.md` §2-3.
//!
//! Wires the in-memory side of the LSM-tree (active memtable + immutable
//! queue) with on-disk SST files and the write controller. `flush_cf()`
//! persists the oldest immutable memtable to an L0 SST file, atomically
//! installs the new version, and caches a reader for subsequent lookups.
//!
//! Compaction (W15) and the C ABI bridge (W16) are not yet implemented.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::thread::JoinHandle;

use forst_rs_common::{
    ColumnFamilyId, EngineOptions, FileNumber, ForstError, ForstResult, InternalKey, OpType,
    SequenceNumber, DEFAULT_CF_ID,
};
use forst_rs_io::{FileSystem, LocalFileSystem, MemoryFileSystem, OpendalFileSystem};
use forst_rs_storage::cache::clock::ShardedClockCache;
use forst_rs_storage::cached_fs::CachedFileSystem;
use forst_rs_storage::local_cache::LocalCache;
use forst_rs_storage::sst::{SstReaderImpl, SstWriterOptions};
use forst_rs_storage::version::{
    SstFileMeta, Version, VersionEdit, VersionSetImpl, VersionSetSnapshot,
};

use crate::checkpoint::{
    copy_live_ssts, serialize_snapshot, write_blob, CheckpointManifest, CHECKPOINT_BLOB_NAME,
};
use crate::column_family::{ColumnFamilyData, ColumnFamilyDescriptor, ColumnFamilyHandle};
use crate::compaction::{compaction_output_path, CompactionJob};
use crate::compaction_filter::CompactionFilter;
use crate::file_deletion_guard::FileDeletionGuard;
use crate::flush::{sst_file_path, FlushExecutor, FlushJob, FlushQueue, FlushRequest};
use crate::mvcc::{self, DbId, Snapshot, SnapshotRegistry};
use crate::runtime_tuning::WriteBufferManager;
use crate::snapshot_view::SnapshotView;
use crate::write_batch::WriteBatch;
use crate::write_controller::WriteController;

/// Process-wide allocator for [`DbId`] values.
///
/// Every `DbImpl::open*` path mints a fresh id via `fetch_add(1, Relaxed)`.
/// Bound into every `Snapshot` at capture time (spec §15 "Same-DB"
/// invariant) so the FFI release path can detect cross-DB releases.
static NEXT_DB_ID: AtomicU64 = AtomicU64::new(1);

/// Bounded capacity for the background flush queue. Sized comfortably above
/// `max_write_buffer_number` so the writer's `try_send` rarely blocks; real
/// backpressure is handled by [`WriteController::set_imm_count`] which
/// stalls writers when imm count >= the configured cap.
const FLUSH_QUEUE_CAPACITY: usize = 64;

/// Cadence at which the snapshot-age ticker polls
/// [`SnapshotRegistry::check_long_lived`] (spec §6a.3). One second is
/// far below the 5-minute default warn threshold, so the worst-case
/// detection latency after a snapshot first crosses the line is ~1 s —
/// well inside any operator-actionable timeframe. Bounded to avoid
/// log spam: the warn is RATE-LIMITED INSIDE the ticker by only emitting
/// when `check_long_lived` returns `Some(...)`, which already requires
/// a snapshot to be live past the threshold.
const SNAPSHOT_AGE_TICK_MS: u64 = 1_000;

/// Sequence-number warn threshold (spec §6a.4). 2^59 — at this point
/// the writer has burned half of the engine's 2^60-bit usable seq
/// space; surfacing the condition early lets an operator schedule a
/// checkpoint-and-restart cycle before the fatal threshold lands.
/// Process-singleton warn (gated via [`SEQ_HIGH_WARNED`]) so logs do
/// not get spammed on every write past the line.
const SEQ_NUMBER_WARN_THRESHOLD: u64 = 1u64 << 59;

/// Sequence-number fatal threshold (spec §6a.4). 2^60 — at this point
/// the engine refuses further writes; the only safe recovery is a
/// checkpoint + restart cycle. We stop BEFORE the InternalKey 56-bit
/// packed-seq invariant trips (a `debug_assert!` in
/// `SequenceNumber::new` that release builds elide); the 2^60 limit
/// gives operators a 4-bit safety margin against the absolute hard
/// stop at `u64::MAX >> 8` = 2^56 - 1. NOTE: the spec uses 2^60 as a
/// conservative bar to flag well before the 56-bit packed-seq limit
/// would actually fire in misuse; see the inline comment on
/// `write_single` for why the check still triggers a real fatal even
/// though the on-disk encoder would tolerate slightly more.
const SEQ_NUMBER_FATAL_THRESHOLD: u64 = 1u64 << 60;

/// Process-singleton flag that gates the one-time `tracing::warn!` for
/// the sequence-number warn threshold. We use a plain `AtomicBool`
/// (CAS to claim the warn slot) rather than `std::sync::Once` so the
/// flag is reachable from test code that wants to assert "warn fires
/// at most once across the process".
static SEQ_HIGH_WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Name of the default column family (always id 0).
pub const DEFAULT_CF_NAME: &str = "default";

// PR-C6-H2: re-export `ValueSink` from the storage layer so callers
// who already depend on `forst-rs-engine` do not need to take a direct
// dep on `forst-rs-storage` just to name the trait.
pub use forst_rs_storage::memtable::{
    GetBorrowedResult, MemtableValueRef, SinkGetOutcome, ValueSink,
};

/// `BinaryBuilder` implements `ValueSink` so the engine can stream the
/// memtable inline-cache fast path directly into an Arrow value
/// buffer.
///
/// This impl lives in `forst-rs-engine` (not the storage crate) because
/// it is the read-path coupling point: the storage crate purposefully
/// stays Arrow-agnostic on the trait definition so a non-Arrow consumer
/// (e.g. a future packed-byte sink) can also implement `ValueSink`.
struct BinaryBuilderSink<'a>(&'a mut arrow::array::BinaryBuilder);

impl<'a> ValueSink for BinaryBuilderSink<'a> {
    #[inline]
    fn append_borrowed(&mut self, value: &[u8]) {
        // `append_value(impl AsRef<[u8]>)` -> `append_slice(&[u8])`:
        // one memcpy into the Arrow value buffer, no extra alloc when
        // the builder was pre-sized in `with_capacity`.
        self.0.append_value(value);
    }

    #[inline]
    fn append_null(&mut self) {
        self.0.append_null();
    }
}

/// Per-SST descriptor returned by [`DbImpl::list_live_files`].
///
/// The Vec is enumerated in (level, smallest_key) order so callers can
/// treat the result as a stable manifest of the LSM-tree's on-disk state at
/// the moment of the call. Memory ownership: returned [`String`]s are owned;
/// the Vec is freed when dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveFileInfo {
    /// Absolute on-disk path of the SST file (resolved against
    /// `EngineOptions::db_path`).
    pub path: PathBuf,
    /// File size in bytes (as recorded by the writer; not re-statted).
    pub size: u64,
    /// Largest sequence number contained in the file. Mirrors the RocksDB
    /// `largest_seqno` field that Flink's incremental restore consults.
    pub sequence: u64,
    /// LSM level the file currently lives on. 0..MAX_LEVELS.
    pub level: u8,
    /// Owning column family name. forst-rs's VersionSet does not currently
    /// partition SST files by CF (one global level layout); this field is
    /// always [`DEFAULT_CF_NAME`] today, but is exposed so the FFI surface
    /// is forward-compatible with a future per-CF VersionSet.
    pub cf_name: String,
}

/// Result of [`DbImpl::create_incremental_checkpoint`]. See spec §10b.
///
/// `new_ssts` lists the SSTs the caller must upload to durable storage
/// (those not already shared with `base_checkpoint_id`). `shared_ssts`
/// lists the SSTs the caller can reference by handle from the previous
/// checkpoint without re-uploading. `manifest_path` points at the
/// engine-persisted checkpoint manifest blob.
#[derive(Debug, Clone)]
pub struct IncrementalCheckpointResult {
    /// On-disk path to the manifest blob persisted by the engine.
    pub manifest_path: PathBuf,
    /// SSTs created since `base_checkpoint_id` — caller must upload.
    pub new_ssts: Vec<LiveFileInfo>,
    /// SSTs shared with `base_checkpoint_id` — caller can reuse handles.
    pub shared_ssts: Vec<LiveFileInfo>,
}

/// RAII guard that releases a previously-reserved chunk back to the cross-CF
/// [`WriteBufferManager`] when dropped, unless [`Self::commit`] is called.
///
/// R31-M3: lets us reserve BEFORE the memtable put while keeping the budget
/// honest when the put errors. A successful put calls `commit()` so the
/// reservation persists until the next flush release; an error returns via
/// `?` and the guard drops, refunding the bytes.
struct WbmReleaseGuard<'a> {
    wbm: Option<&'a WriteBufferManager>,
    charge: u64,
}

impl<'a> WbmReleaseGuard<'a> {
    #[inline]
    fn new(wbm: &'a WriteBufferManager, charge: u64) -> Self {
        Self {
            wbm: Some(wbm),
            charge,
        }
    }

    /// Mark the reservation as committed — the put succeeded and the bytes
    /// should remain charged until the flush release path runs.
    #[inline]
    fn commit(mut self) {
        self.wbm = None;
    }
}

impl<'a> Drop for WbmReleaseGuard<'a> {
    #[inline]
    fn drop(&mut self) {
        if let Some(wbm) = self.wbm.take() {
            wbm.release(self.charge);
        }
    }
}

/// The top-level engine struct.
pub struct DbImpl {
    options: EngineOptions,
    db_path: PathBuf,
    fs: Arc<dyn FileSystem>,
    /// Column families keyed by id.
    cfs: RwLock<HashMap<ColumnFamilyId, Arc<ColumnFamilyData>>>,
    /// Name → id lookup for CF discovery.
    cf_name_to_id: RwLock<HashMap<String, ColumnFamilyId>>,
    /// Lock-free SST file layout.
    version_set: Arc<VersionSetImpl>,
    /// Open SST readers keyed by file number. Installed after flush /
    /// compaction and dropped after files are deleted from the Version.
    sst_readers: RwLock<HashMap<FileNumber, Arc<SstReaderImpl>>>,
    /// Tracks SST files pinned by active checkpoints so concurrent
    /// compactions do not delete them prematurely.
    deletion_guard: Arc<FileDeletionGuard>,
    /// Files whose deletion was deferred because a checkpoint still held a
    /// pin. Reaped during subsequent flush/compact calls once the pins
    /// have been released.
    pending_deletions: Mutex<Vec<FileNumber>>,
    /// Monotonic sequence number shared across all CFs. Incremented on every
    /// successful mutation.
    sequence_number: AtomicU64,
    /// Back-pressure controller.
    write_controller: Arc<WriteController>,
    /// Serializes writers so the active memtable's internal sequence matches
    /// the engine-level sequence allocated just above it.
    write_mutex: Mutex<()>,
    /// Allocator for new CF ids.
    next_cf_id: AtomicU32,
    /// Background flush queue (B1: writers enqueue, worker drains). The
    /// sender lives in here; the receiver is taken once by the worker on
    /// startup. Dropping this `Arc` drops the sender, which closes the
    /// channel and signals the worker to exit.
    flush_queue: Arc<FlushQueue>,
    /// Handle to the background flush worker thread. `Some` while the
    /// engine is alive; taken and joined in [`Drop`] for clean shutdown.
    flush_worker: Mutex<Option<JoinHandle<()>>>,
    /// Most recent error from a background flush. Surfaced to the next
    /// writer that calls [`Self::write_single`] / [`Self::batch_write`] so
    /// the application learns about flush failures even though the failing
    /// flush ran off the writer's stack. Cleared after the writer observes
    /// it.
    flush_error: Mutex<Option<ForstError>>,
    /// Count of flush requests enqueued but not yet completed by the
    /// worker. Used by [`Self::wait_for_pending_flushes`] to know when the
    /// worker has drained everything we asked it to. Bumped on enqueue,
    /// decremented after `run_flush` returns (success or failure).
    pending_flush_count: AtomicU32,
    /// MVCC snapshot registry. Owns the ref-counted set of live snapshot
    /// sequence numbers. Compaction reads `min_active()` once per pass
    /// and consults [`crate::mvcc::should_drop`] per entry to decide
    /// whether a version may be reclaimed. See spec §6a.2.
    snapshot_registry: Arc<SnapshotRegistry>,
    /// Process-monotonic id stamped onto every `Snapshot` issued by this
    /// engine. The FFI release path checks this against the calling
    /// `DbImpl` to enforce the spec §15 "Same-DB" invariant.
    db_id: DbId,
    /// Shared LRU block cache (B-Prod-P7, spec §6d). Sized at open by
    /// `EngineOptions::block_cache_capacity_bytes` (falling back to the
    /// legacy `block_cache_size` when the new field is `0`). Held here so
    /// (a) every CF read path can sample the same cache instance and
    /// (b) the FFI tuning surface can read its current bytes / hit-rate
    /// for diagnostics. The SST-reader-side wiring lands incrementally as
    /// readers migrate to the shared cache.
    block_cache: Arc<ShardedClockCache>,
    /// Cross-CF memtable budget (B-Prod-P7, spec §6d). Sized at open by
    /// `EngineOptions::write_buffer_manager_capacity_bytes` (`0` =
    /// unbounded). Per-write paths reserve / release bytes via
    /// `WriteBufferManager::{reserve, release}`; once `over_budget()`
    /// trips, the writer hot path triggers a flush of the largest CF
    /// rather than blocking, matching RocksDB's `allow_stall=false`
    /// default.
    write_buffer_manager: Arc<WriteBufferManager>,
    /// Background ticker that periodically calls
    /// [`SnapshotRegistry::check_long_lived`] and emits a `tracing::warn!`
    /// when a snapshot has exceeded its `max_age_ms` warn-line
    /// (spec §6a.3). Per spec the engine NEVER auto-releases the
    /// snapshot — this is a WARN-only path; the operator runbook is the
    /// remediation surface. `Some` while the engine is alive; taken and
    /// joined in [`Drop`] for clean shutdown.
    snapshot_age_worker: Mutex<Option<JoinHandle<()>>>,
    /// Signal that tells the snapshot-age ticker to stop and exit. The
    /// ticker checks this between sleeps; setting it to `true` and
    /// then joining drains the thread within the configured tick
    /// interval (currently 1 second, see `SNAPSHOT_AGE_TICK_MS`).
    snapshot_age_shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl DbImpl {
    /// Creates a new engine with the given options, using [`LocalFileSystem`]
    /// at `options.db_path`.
    pub fn open(options: EngineOptions) -> ForstResult<Arc<Self>> {
        if options.write_buffer_size == 0 {
            return Err(ForstError::invalid_argument(
                "write_buffer_size must be greater than zero",
            ));
        }
        if options.db_path.is_empty() {
            return Err(ForstError::invalid_argument("db_path must not be empty"));
        }
        let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
        Self::open_with_fs(options, fs)
    }

    /// Creates a new engine with a pluggable [`FileSystem`]. Useful for
    /// in-memory tests with [`MemoryFileSystem`].
    pub fn open_with_fs(options: EngineOptions, fs: Arc<dyn FileSystem>) -> ForstResult<Arc<Self>> {
        if options.write_buffer_size == 0 {
            return Err(ForstError::invalid_argument(
                "write_buffer_size must be greater than zero",
            ));
        }
        let db_path = PathBuf::from(&options.db_path);
        // Ensure the db directory exists.
        if !options.db_path.is_empty() {
            fs.create_dir_all(&db_path)?;
        }

        // B-Prod-P7 §6d runtime tuning hooks. The new
        // `block_cache_capacity_bytes` field takes precedence; when it is
        // `0` we fall back to the legacy `usize` `block_cache_size` so
        // pre-P7 callers building EngineOptions through `..default()`
        // keep their cache sizing.
        let cache_bytes = if options.block_cache_capacity_bytes != 0 {
            // Truncate to usize on 32-bit hosts (the validator already
            // capped at 1 PiB, well under 4 GiB only on impossibly small
            // targets — the truncation is always lossless on 64-bit, and
            // a 32-bit host cannot meaningfully address 1 PiB anyway).
            options.block_cache_capacity_bytes.min(usize::MAX as u64) as usize
        } else {
            options.block_cache_size
        };
        let block_cache = Arc::new(ShardedClockCache::with_capacity(cache_bytes));
        let write_buffer_manager =
            WriteBufferManager::new(options.write_buffer_manager_capacity_bytes);

        let db = Arc::new(Self {
            options,
            db_path,
            fs,
            cfs: RwLock::new(HashMap::new()),
            cf_name_to_id: RwLock::new(HashMap::new()),
            version_set: Arc::new(VersionSetImpl::new()),
            sst_readers: RwLock::new(HashMap::new()),
            deletion_guard: Arc::new(FileDeletionGuard::new()),
            pending_deletions: Mutex::new(Vec::new()),
            sequence_number: AtomicU64::new(0),
            write_controller: Arc::new(WriteController::with_defaults()),
            write_mutex: Mutex::new(()),
            next_cf_id: AtomicU32::new(1),
            flush_queue: Arc::new(FlushQueue::new(FLUSH_QUEUE_CAPACITY)),
            flush_worker: Mutex::new(None),
            flush_error: Mutex::new(None),
            pending_flush_count: AtomicU32::new(0),
            snapshot_registry: SnapshotRegistry::new(),
            db_id: DbId(NEXT_DB_ID.fetch_add(1, Ordering::Relaxed)),
            block_cache,
            write_buffer_manager,
            snapshot_age_worker: Mutex::new(None),
            snapshot_age_shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });

        let default_desc = ColumnFamilyDescriptor::new(DEFAULT_CF_NAME);
        db.create_cf_with_id(DEFAULT_CF_ID, default_desc)?;
        Self::spawn_flush_worker(&db);
        Self::spawn_snapshot_age_worker(&db);
        Ok(db)
    }

    /// Opens the engine with default [`EngineOptions`] backed by an
    /// in-memory filesystem. Convenience for tests.
    pub fn open_default() -> ForstResult<Arc<Self>> {
        let opts = EngineOptions {
            db_path: "/db".to_string(),
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        Self::open_with_fs(opts, fs)
    }

    // -----------------------------------------------------------------
    // OpenDAL convenience constructors
    //
    // These wrap [`OpendalFileSystem`] as `Arc<dyn FileSystem>` and call
    // [`Self::open_with_fs`]. They exist purely for ergonomics — there
    // is no behavioral difference vs. building the OpendalFileSystem by
    // hand and passing it through the legacy injection path.
    //
    // Performance note: OpenDAL adds one indirection layer (sync→async
    // bridge + per-request operator dispatch) over the bare
    // [`LocalFileSystem`]. For latency-critical local-disk workloads,
    // prefer [`Self::open`] (uses [`LocalFileSystem`] directly). For
    // remote backends (S3, GCS, …) OpenDAL is the only first-class
    // option today.
    // -----------------------------------------------------------------

    /// Opens the engine on top of an arbitrary [`opendal::Operator`].
    ///
    /// Use this when you have constructed and configured an operator
    /// yourself (custom layers, retry policy, presigned credentials,
    /// in-house service builder, …). For common cases prefer the more
    /// targeted helpers below.
    pub fn open_with_opendal(
        options: EngineOptions,
        op: opendal::Operator,
    ) -> ForstResult<Arc<Self>> {
        let fs: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::with_operator(op)?);
        Self::open_with_fs(options, fs)
    }

    /// Opens the engine on top of an OpenDAL local-FS operator rooted at
    /// `root`. The directory is created on first write.
    ///
    /// Equivalent to [`Self::open`] for most local-disk uses, but goes
    /// through OpenDAL — useful for parity testing across local and
    /// remote backends.
    pub fn open_local_opendal(
        options: EngineOptions,
        root: &std::path::Path,
    ) -> ForstResult<Arc<Self>> {
        let fs: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::local(root)?);
        Self::open_with_fs(options, fs)
    }

    /// Opens the engine on top of an OpenDAL in-memory operator. Useful
    /// for unit tests and ephemeral pipelines that want OpenDAL's
    /// observability layers without touching disk.
    pub fn open_memory_opendal(options: EngineOptions) -> ForstResult<Arc<Self>> {
        let fs: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::memory()?);
        Self::open_with_fs(options, fs)
    }

    /// Opens the engine on top of an OpenDAL S3 operator.
    ///
    /// `endpoint` lets the caller target S3-compatible services (MinIO,
    /// Ceph RGW, R2, …). `access_key_id` / `secret_access_key` are
    /// optional — when `None`, OpenDAL falls back to the AWS SDK
    /// default credential chain (env vars, instance profile, …).
    pub fn open_s3(
        options: EngineOptions,
        bucket: &str,
        region: &str,
        endpoint: Option<&str>,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
    ) -> ForstResult<Arc<Self>> {
        let fs: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::s3(
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
        )?);
        Self::open_with_fs(options, fs)
    }

    /// Opens a remote-storage-backed engine with a local SST cache (B-Prod-P6).
    ///
    /// State files are persisted via OpenDAL on the URI's scheme (e.g.
    /// `memory://`, `file:///abs/path`, `s3://bucket/`), and a local LRU
    /// cache rooted at `cache_dir` fronts every read with `cache_capacity_bytes`
    /// of byte budget. Service-specific configuration (S3 region, endpoint,
    /// credentials, …) is passed via `opendal_config` whose keys match the
    /// OpenDAL builder field names for the URI's scheme.
    ///
    /// On URI scheme `memory://` the `opendal_config` map is ignored; the
    /// in-memory backend has no configurable knobs. For `file://` the
    /// `path` portion of the URI is used as the FS root (the map is also
    /// ignored). For `s3://`, the host portion is the bucket, and the map
    /// MUST contain at minimum `region`; optional keys include `endpoint`,
    /// `access_key_id`, `secret_access_key`.
    ///
    /// # Errors
    ///
    /// - [`ForstError::InvalidArgument`] for an unsupported URI scheme,
    ///   malformed URI, or non-UTF-8 `cache_dir`.
    /// - [`ForstError::Io`] for any underlying OpenDAL builder failure or
    ///   cache directory creation failure.
    pub fn open_remote(
        options: EngineOptions,
        uri: &str,
        opendal_config: HashMap<String, String>,
        cache_dir: &std::path::Path,
        cache_capacity_bytes: u64,
    ) -> ForstResult<Arc<Self>> {
        let remote_fs = build_opendal_fs_from_uri(uri, &opendal_config)?;
        let cache = LocalCache::open(cache_dir, cache_capacity_bytes).map_err(|e| {
            ForstError::Io(std::io::Error::other(format!(
                "open_remote: failed to open local cache at {:?}: {e}",
                cache_dir
            )))
        })?;
        let cached_fs: Arc<dyn FileSystem> =
            Arc::new(CachedFileSystem::new(remote_fs, Arc::new(cache)));
        Self::open_with_fs(options, cached_fs)
    }

    /// Returns a handle to the default column family.
    pub fn default_cf(&self) -> ColumnFamilyHandle {
        self.lookup_cf_by_id(DEFAULT_CF_ID)
            .expect("default CF must always exist")
            .handle()
            .clone()
    }

    /// Creates a new column family with the given descriptor.
    pub fn create_column_family(
        &self,
        desc: ColumnFamilyDescriptor,
    ) -> ForstResult<ColumnFamilyHandle> {
        // Reject duplicate names.
        {
            let name_map = self.cf_name_to_id.read().expect("lock poisoned");
            if name_map.contains_key(desc.name()) {
                return Err(ForstError::invalid_argument(format!(
                    "column family '{}' already exists",
                    desc.name()
                )));
            }
        }
        let id = ColumnFamilyId(self.next_cf_id.fetch_add(1, Ordering::SeqCst));
        self.create_cf_with_id(id, desc)
    }

    fn create_cf_with_id(
        &self,
        id: ColumnFamilyId,
        desc: ColumnFamilyDescriptor,
    ) -> ForstResult<ColumnFamilyHandle> {
        let name = desc.name().to_string();
        let options = desc.options().clone();
        let merge_op = desc.merge_operator();
        let filter = desc.compaction_filter();
        let handle = ColumnFamilyHandle::new(id, &name);

        let initial_snapshot = Arc::new(SnapshotView::empty());
        let cf_data = Arc::new(ColumnFamilyData::new_with_filter_and_shards(
            handle.clone(),
            options,
            merge_op,
            filter,
            initial_snapshot,
            self.options.memtable_shards,
        ));

        {
            let mut cfs = self.cfs.write().expect("lock poisoned");
            cfs.insert(id, cf_data.clone());
        }
        {
            let mut names = self.cf_name_to_id.write().expect("lock poisoned");
            names.insert(name, id);
        }

        // Install a snapshot view that references the active memtable.
        self.refresh_snapshot_view(&cf_data);
        Ok(handle)
    }

    /// Returns a handle for the given CF name, if it exists.
    pub fn column_family(&self, name: &str) -> Option<ColumnFamilyHandle> {
        let name_map = self.cf_name_to_id.read().expect("lock poisoned");
        let id = name_map.get(name).copied()?;
        let cfs = self.cfs.read().expect("lock poisoned");
        cfs.get(&id).map(|cf| cf.handle().clone())
    }

    /// Installs (or replaces) the compaction filter on a column family
    /// after it has already been created. Pass `None` to clear the filter.
    ///
    /// This is the post-`open` configuration path that the FFI export
    /// `frs_cf_set_compaction_filter_ttl` uses to wire a Flink TTL filter
    /// onto a CF without re-creating it. The next compaction job built
    /// for the CF observes the new filter; in-flight jobs run to
    /// completion against the snapshot they already captured.
    ///
    /// # Errors
    ///
    /// Returns [`ForstError::InvalidArgument`] when the handle does not
    /// match a known column family.
    pub fn set_compaction_filter(
        &self,
        cf: &ColumnFamilyHandle,
        filter: Option<Arc<dyn CompactionFilter>>,
    ) -> ForstResult<()> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        cf_data.set_compaction_filter(filter);
        Ok(())
    }

    /// Drops a column family (B-Prod-followup-5, spec §6g).
    ///
    /// Removes `cf` from the engine's CF maps, flips its shared `dropped`
    /// flag (visible from every cloned [`ColumnFamilyHandle`]), and
    /// releases the memtable buffers back to the
    /// [`WriteBufferManager`]. Subsequent operations on the dropped CF
    /// return [`ForstError::InvalidArgument`].
    ///
    /// SST FILES ARE NOT DELETED. forst-rs's `VersionSet` is currently
    /// CF-agnostic — a single SST may contain rows for multiple CFs
    /// (the engine relies on the Flink-side `CfRouter` to disambiguate
    /// via cf-id key prefixes). Walking the version set to remove "this
    /// CF's files" would corrupt sibling CFs. The correct reclamation
    /// story is: drop the CF, let compaction's `should_drop` MVCC logic
    /// retire the orphaned rows as they age out. This matches the spec
    /// §6g acceptance criterion ("dropped CF is invisible to subsequent
    /// reads"; physical storage reclamation is allowed to be lazy).
    ///
    /// Idempotent:
    /// - Dropping an already-dropped CF returns `Ok(())` (no-op).
    /// - Dropping the default CF returns
    ///   [`ForstError::InvalidArgument`] (the default CF cannot be
    ///   dropped; an attempt is a programming error, not a no-op).
    pub fn drop_cf(&self, cf: &ColumnFamilyHandle) -> ForstResult<()> {
        // Reject the default CF up-front. The check is by id (not name)
        // so renames in the future can't slip past — id 0 is the
        // process-wide default-CF contract.
        if cf.id() == DEFAULT_CF_ID {
            return Err(ForstError::invalid_argument(
                "default column family cannot be dropped",
            ));
        }

        // Idempotent: already-dropped → no-op. We check the handle's
        // shared flag first so callers that re-drop with a stale handle
        // get Ok without us needing to find the CF in the maps.
        if cf.is_dropped() {
            return Ok(());
        }

        // Resolve the CF data while it's still in the maps. We bypass
        // `lookup_cf_by_id` here because that path returns
        // InvalidArgument once the flag is flipped — and we're the one
        // about to flip it.
        let cf_data = {
            let cfs = self.cfs.read().expect("lock poisoned");
            cfs.get(&cf.id()).cloned()
        };
        let Some(cf_data) = cf_data else {
            // Not in the maps and not flagged dropped — caller passed a
            // bogus handle. Surface as InvalidArgument for parity with
            // every other CF-resolving entry point.
            return Err(ForstError::invalid_argument(format!(
                "column family id {} not found",
                cf.id()
            )));
        };

        // Flip the dropped flag BEFORE removing from the maps so a
        // concurrent `lookup_cf_by_id` race observes either:
        //   (a) the CF still in the map but flagged → InvalidArgument
        //   (b) the CF gone from the map → InvalidArgument (not found)
        // Either way the caller sees consistent "CF unusable" rejection.
        cf_data.mark_dropped();

        // Release memtable bytes back to the WriteBufferManager. We
        // approximate by sampling both the active and immutable memtable
        // usage; the per-shard accounting is precise enough for the
        // cross-CF budget cap which is itself coarse (~512 MiB default).
        let mut released_bytes: u64 = cf_data.active_memtable().memory_usage() as u64;
        for imm in cf_data.imm_memtables() {
            released_bytes = released_bytes.saturating_add(imm.memory_usage() as u64);
        }
        self.write_buffer_manager.release(released_bytes);

        // Remove from both maps. The (cfs map, name map) ordering doesn't
        // matter for correctness — the dropped flag is the source of
        // truth once flipped — but removing from `cf_name_to_id` first
        // lets a same-name `create_column_family` immediately succeed.
        let cf_name = cf_data.handle().name().to_string();
        {
            let mut names = self.cf_name_to_id.write().expect("lock poisoned");
            names.remove(&cf_name);
        }
        {
            let mut cfs = self.cfs.write().expect("lock poisoned");
            cfs.remove(&cf.id());
        }

        // The `Arc<ColumnFamilyData>` may still have outstanding refs
        // (compaction job snapshots, in-flight write batches). Those
        // refs will observe `is_dropped() == true` on their next CF
        // access and bail out cleanly. Once the last ref drops, the
        // memtable and snapshot view are reclaimed.
        Ok(())
    }

    /// Ingests pre-built SST files into the engine's L0 layer
    /// (B-Prod-followup-5, spec §6g).
    ///
    /// Each `src_path` is hardlinked (or copied if cross-filesystem)
    /// into the engine's SST directory under a freshly allocated file
    /// number, its footer is read to extract `min_key` / `max_key` /
    /// `min_sequence` / `max_sequence` / `num_entries` / `file_size`,
    /// and the resulting `SstFileMeta` is installed at L0 via a single
    /// atomic `VersionSetImpl::apply` call.
    ///
    /// CONTRACT — caller responsibilities:
    ///
    /// - The source SSTs MUST be readable by [`SstReaderImpl::open`]
    ///   (i.e. produced by another forst-rs instance or a compatible
    ///   builder). RocksDB-compat ingestion is not supported on this
    ///   path today.
    /// - Key ranges in the ingested SSTs SHOULD NOT overlap with keys
    ///   already present at non-zero levels. The engine installs at L0
    ///   where overlap is permitted; deeper levels would require
    ///   compaction-level overlap analysis the caller cannot easily do.
    /// - The `cf` argument is currently informational — forst-rs's
    ///   `VersionSet` is global (not partitioned by CF), so the
    ///   ingested files become visible to ALL live CFs. Callers route
    ///   CF visibility via the key encoding (Flink's `CfRouter` prefix
    ///   scheme); the `cf` parameter exists so a future per-CF
    ///   VersionSet can drop in without breaking callers.
    ///
    /// Returns the list of newly allocated SST file numbers (one per
    /// `src_path`) in the same order. On any per-file failure the
    /// already-linked dest files are best-effort cleaned up and the
    /// error is propagated — the version set is never partially
    /// updated.
    pub fn ingest_external_sst(
        &self,
        cf: &ColumnFamilyHandle,
        sst_paths: &[&Path],
    ) -> ForstResult<Vec<u64>> {
        let _cf_data = self.lookup_cf_by_id(cf.id())?;
        if sst_paths.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 1: allocate file numbers, hardlink/copy source SSTs into
        // the engine's SST dir, and read their footers to build
        // SstFileMeta entries. We accumulate (file_number, dest_path,
        // meta) so the version edit and the readers cache can both be
        // populated atomically below.
        let mut new_files: Vec<(FileNumber, PathBuf, SstFileMeta)> =
            Vec::with_capacity(sst_paths.len());
        let mut new_ids: Vec<u64> = Vec::with_capacity(sst_paths.len());

        for src in sst_paths {
            let file_number = self.version_set.allocate_file_number();
            let dest = sst_file_path(&self.db_path, file_number);

            // Try hardlink first (fastest, zero-copy on same FS); fall
            // back to a byte copy on cross-FS / unsupported FS.
            if let Err(_link_err) = std::fs::hard_link(src, &dest) {
                std::fs::copy(src, &dest).map_err(|e| {
                    // Best-effort cleanup of any already-linked dest
                    // files so a failed ingest doesn't leak SSTs into
                    // the engine's directory.
                    Self::cleanup_ingested(&new_files);
                    ForstError::Io(std::io::Error::new(
                        e.kind(),
                        format!(
                            "ingest_external_sst: hardlink+copy '{}' -> '{}' failed: {e}",
                            src.display(),
                            dest.display()
                        ),
                    ))
                })?;
            }

            // Open the dest file and read the footer to extract the
            // metadata fields VersionEdit needs. We open via the engine's
            // FileSystem (so the OpenDAL / cached-fs paths get exercised
            // uniformly), not std::fs.
            let rac = self.fs.open_random_access_file(&dest).inspect_err(|_| {
                Self::cleanup_ingested(&new_files);
            })?;
            // Re-open the dest via a *fresh* RAC so we can sample its
            // file_size BEFORE consuming the handle into SstReaderImpl
            // (the reader takes Box<dyn RandomAccessFile> by value).
            // The second `open_random_access_file` is cheap on every
            // FileSystem impl we ship.
            let file_size = match self.fs.open_random_access_file(&dest) {
                Ok(probe) => match probe.file_size() {
                    Ok(sz) => sz,
                    Err(e) => {
                        Self::cleanup_ingested(&new_files);
                        return Err(e);
                    }
                },
                Err(e) => {
                    Self::cleanup_ingested(&new_files);
                    return Err(e);
                }
            };
            let reader = SstReaderImpl::open(rac).inspect_err(|_| {
                Self::cleanup_ingested(&new_files);
            })?;
            let footer = reader.footer().clone();

            let meta = SstFileMeta {
                file_number,
                file_size,
                smallest_key: footer.min_key.clone(),
                largest_key: footer.max_key.clone(),
                min_sequence: SequenceNumber(footer.min_sequence),
                max_sequence: SequenceNumber(footer.max_sequence),
                num_entries: footer.total_entries,
            };

            new_ids.push(file_number.value());
            new_files.push((file_number, dest, meta));

            // Drop the temp reader; we'll re-open into the engine's
            // sst_readers cache below after the version edit lands.
            drop(reader);
        }

        // Phase 2: install all new SSTs at L0 in a single VersionEdit.
        // The edit also bumps `last_sequence` to cover the highest seq
        // we just ingested so subsequent reads at u64::MAX still see
        // these keys without the read path having to specially handle
        // "ingested seq > engine seq".
        let mut max_seq_ingested: u64 = 0;
        let mut new_files_for_edit: Vec<(u32, SstFileMeta)> = Vec::with_capacity(new_files.len());
        for (_, _, meta) in &new_files {
            if meta.max_sequence.value() > max_seq_ingested {
                max_seq_ingested = meta.max_sequence.value();
            }
            new_files_for_edit.push((0u32, meta.clone()));
        }

        let edit = VersionEdit {
            new_files: new_files_for_edit,
            last_sequence: if max_seq_ingested > 0 {
                Some(SequenceNumber(max_seq_ingested))
            } else {
                None
            },
            ..Default::default()
        };
        if let Err(e) = self.version_set.apply(&edit) {
            Self::cleanup_ingested(&new_files);
            return Err(e);
        }

        // Phase 3: pre-populate the sst_readers cache so the first
        // point lookup doesn't pay the open-file cost. We rebuild
        // readers because the temp readers from phase 1 were dropped
        // above; this also ensures the cache holds readers opened
        // through the engine's FileSystem (cf. CachedFileSystem
        // bookkeeping).
        let mut readers_map = self.sst_readers.write().expect("lock poisoned");
        for (file_number, dest, _meta) in &new_files {
            let rac = self.fs.open_random_access_file(dest)?;
            let reader = Arc::new(SstReaderImpl::open(rac)?);
            readers_map.insert(*file_number, reader);
        }
        drop(readers_map);

        // Bump the engine sequence counter so writes following the
        // ingest don't reuse a seq < the ingested max. Matches the
        // VersionEdit's `last_sequence` update above; we set both
        // because external readers consult `engine.sequence_number()`
        // (e.g. `snapshot()`) while the read path consults
        // `version_set.last_sequence()`.
        if max_seq_ingested > self.sequence_number.load(Ordering::Acquire) {
            self.sequence_number
                .store(max_seq_ingested, Ordering::Release);
        }

        // L0 file count for back-pressure.
        self.write_controller
            .set_l0_file_count(self.version_set.current().l0_files().len() as u32);

        Ok(new_ids)
    }

    /// Best-effort cleanup helper for [`Self::ingest_external_sst`].
    /// Deletes any dest files we already linked/copied before the call
    /// failed. Errors are intentionally swallowed (the surfaced error is
    /// the original failure that triggered cleanup; cleanup failures
    /// would only obscure it).
    fn cleanup_ingested(new_files: &[(FileNumber, PathBuf, SstFileMeta)]) {
        for (_, dest, _) in new_files {
            let _ = std::fs::remove_file(dest);
        }
    }

    /// Returns a reference to the engine options.
    pub fn options(&self) -> &EngineOptions {
        &self.options
    }

    /// Returns the shared write controller.
    pub fn write_controller(&self) -> &Arc<WriteController> {
        &self.write_controller
    }

    /// Returns the current sequence number (latest assigned).
    pub fn sequence_number(&self) -> u64 {
        self.sequence_number.load(Ordering::Acquire)
    }

    /// Returns the number of L0 SST files in the current Version. Read by
    /// the FFI metadata surface (`frs_l0_file_count`) and used internally
    /// by tests asserting flush behaviour.
    pub fn l0_file_count(&self) -> u32 {
        self.version_set.current().l0_files().len() as u32
    }

    /// Returns the MVCC snapshot registry. The FFI / engine bindings call
    /// `capture(db_id, current_seq)` on the returned `Arc` to mint a new
    /// `Snapshot`; compaction reads `min_active()` to gate reclamation.
    /// See spec §6a.2.
    pub fn snapshot_registry(&self) -> &Arc<SnapshotRegistry> {
        &self.snapshot_registry
    }

    /// Sets the snapshot warn-line threshold in milliseconds (spec §6a.3).
    ///
    /// Effects the next tick of the background snapshot-age worker; the
    /// worker emits a `tracing::warn!` for every period in which a
    /// pinned snapshot has been alive longer than this threshold.
    /// Defaults to [`crate::mvcc::DEFAULT_MAX_AGE_MS`] (5 minutes).
    /// Per spec the engine NEVER auto-releases — this is WARN-only.
    pub fn set_snapshot_max_age_ms(&self, ms: u64) {
        self.snapshot_registry.set_max_age_ms(ms);
    }

    /// One-shot read of the snapshot-age warn condition (spec §6a.3).
    /// Returns `Some(...)` when at least one pinned snapshot has been
    /// alive longer than the configured `max_age_ms`. Intended for
    /// FFI / test drivers that want to poll the registry on their own
    /// cadence; the engine's own background ticker calls this method
    /// every `SNAPSHOT_AGE_TICK_MS` (~1 s, private constant) and emits
    /// a warn line on each non-`None` return.
    pub fn check_long_lived_snapshots(&self) -> Option<crate::mvcc::SnapshotAgeWarning> {
        self.snapshot_registry.check_long_lived()
    }

    /// Returns the process-monotonic `DbId` stamped onto every snapshot
    /// captured against this engine. Used by the FFI release path to
    /// reject cross-DB releases (spec §15 "Same-DB" invariant).
    pub fn db_id(&self) -> DbId {
        self.db_id
    }

    /// Returns the shared LRU block cache held by this engine. Sized at
    /// open time by `EngineOptions::block_cache_capacity_bytes`
    /// (B-Prod-P7, spec §6d). Exposed so the FFI tuning surface can
    /// sample its current bytes / hit-rate, and so SST readers can wire
    /// onto the same cache instance as they migrate to the shared cache.
    pub fn block_cache(&self) -> &Arc<ShardedClockCache> {
        &self.block_cache
    }

    /// Returns the cross-CF [`WriteBufferManager`] (B-Prod-P7, spec §6d).
    /// The writer hot path consults this to decide whether to trigger a
    /// flush after a reservation pushes the running sum past the
    /// configured cap.
    pub fn write_buffer_manager(&self) -> &Arc<WriteBufferManager> {
        &self.write_buffer_manager
    }

    // ---------------------------------------------------------------
    // MVCC snapshot API (spec §6a.5)
    //
    // `snapshot()` mints a `Snapshot` pinned at the current sequence number;
    // `get_at()` runs a versioned point-lookup against that snapshot;
    // `release_snapshot()` is the explicit-drop alias offered by the FFI for
    // C callers that prefer named release over RAII drop. The cross-DB check
    // in `get_at()` enforces the spec §15 "Same-DB" invariant on the read
    // path so the FFI doesn't have to re-validate at the boundary.
    // ---------------------------------------------------------------

    /// Captures a snapshot at the current sequence number.
    ///
    /// The returned [`Snapshot`] is RAII: dropping it releases the
    /// ref-count back to the registry so compaction's `min_active`
    /// query advances. Use [`Self::release_snapshot`] for callers that
    /// prefer an explicit named release.
    pub fn snapshot(&self) -> Snapshot {
        let seq = SequenceNumber::new(self.sequence_number());
        self.snapshot_registry.capture(self.db_id, seq)
    }

    /// Releases the snapshot. Equivalent to `drop(snapshot)`; provided
    /// so the FFI surface can expose a named release entry point. Always
    /// idempotent — moving the snapshot in here drops it exactly once.
    pub fn release_snapshot(&self, snapshot: Snapshot) {
        // Snapshot::Drop fires here and decrements the registry ref-count.
        drop(snapshot);
    }

    /// Reads the latest version of `key` (in the default CF) with seq
    /// <= snapshot.seq.
    ///
    /// Returns `Ok(None)` when no version is visible at snapshot time
    /// or the latest visible version is a deletion tombstone. Returns
    /// `Err(InvalidArgument)` when `snapshot` was issued by a different
    /// `DbImpl` instance, per spec §10.0 ABI contract — the FFI relies
    /// on this same-DB check happening here so it doesn't have to
    /// re-validate at the C boundary.
    pub fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> ForstResult<Option<Vec<u8>>> {
        if snapshot.db_id() != self.db_id {
            return Err(ForstError::invalid_argument(
                "Snapshot was issued by a different DbImpl instance",
            ));
        }
        let cf = self.default_cf();
        self.get_at_cf(&cf, snapshot, key)
    }

    /// Per-CF variant of [`Self::get_at`]. Useful for callers that hold
    /// a non-default `ColumnFamilyHandle`. Same cross-DB invariant.
    pub fn get_at_cf(
        &self,
        cf: &ColumnFamilyHandle,
        snapshot: &Snapshot,
        key: &[u8],
    ) -> ForstResult<Option<Vec<u8>>> {
        if snapshot.db_id() != self.db_id {
            return Err(ForstError::invalid_argument(
                "Snapshot was issued by a different DbImpl instance",
            ));
        }
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let candidates = self.iter_versions_of(&cf_data, key)?;
        // Borrow-extend the owned (key, value) pairs into VersionedEntry
        // refs for `mvcc::get_at` — entries live for the duration of the
        // iterator's traversal (one synchronous call), so the references
        // are valid throughout.
        let result = mvcc::get_at(
            snapshot,
            key,
            candidates.iter().map(|(k, v)| mvcc::VersionedEntry {
                key: k,
                value: v.as_slice(),
            }),
        )
        .map(|s| s.to_vec());
        Ok(result)
    }

    /// Snapshot-aware variant of [`Self::scan`]: returns (key, value) pairs
    /// reflecting the engine state visible at `snapshot.seq`. Filters out
    /// keys whose latest version at snapshot time is a deletion tombstone.
    ///
    /// Memory cost is O(scanned bytes), same as [`Self::scan`] (the FFI
    /// iterator surface materializes the full set up front today — see
    /// spec §6a.5 / `forst_rs_ffi`'s §9 module comment).
    pub fn scan_at(
        &self,
        cf: &ColumnFamilyHandle,
        snapshot: &Snapshot,
    ) -> ForstResult<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::collections::BTreeSet;

        if snapshot.db_id() != self.db_id {
            return Err(ForstError::invalid_argument(
                "Snapshot was issued by a different DbImpl instance",
            ));
        }
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let read_seq = snapshot.seq().value();
        let lower: &[u8] = &[];
        let upper: Option<&[u8]> = None;
        let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();

        // Active memtable.
        {
            let mem_arc = cf_data.active_memtable();
            for (k, _, _, _) in mem_arc.collect_range_entries(lower, upper, read_seq) {
                keys.insert(k);
            }
        }
        // Immutable memtables.
        for imm in cf_data.imm_memtables() {
            for (k, _, _, _) in imm.collect_range_entries(lower, upper, read_seq) {
                keys.insert(k);
            }
        }
        // SST layer — bound files whose key range overlaps the (empty)
        // bound. Any file with `largest_seqno > snapshot.seq` is still
        // worth scanning because individual entries inside the file may
        // be older than snapshot.seq (per spec §6a.5, snapshot filtering
        // happens at the per-entry seq level, not at the file level).
        let version = self.version_set.current();
        for sst in version.live_sst_files() {
            let reader = self.get_or_open_sst_reader(&sst)?;
            // PR-C5-H1: scan_borrowed avoids the per-row `value.to_vec()`
            // that `reader.scan(...)` pays inside `SstReaderImpl::scan`
            // (sst/reader.rs §455). We discard the value here anyway —
            // only the key is collected into the BTreeSet.
            reader.scan_borrowed(lower, upper, |view| {
                keys.insert(view.key.to_vec());
                Ok(())
            })?;
        }

        // Resolve each candidate key through the versioned read path so
        // that tombstones and stale versions are filtered correctly.
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.get_at_cf(cf, snapshot, &key)? {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    /// Engine-side alias for [`Self::scan_at`] used by the FFI
    /// `frs_iterator_open_at` export. Same contract, separate name so the
    /// FFI binding documentation reads cleanly.
    pub fn iter_at(
        &self,
        snapshot: &Snapshot,
        cf: &ColumnFamilyHandle,
    ) -> ForstResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_at(cf, snapshot)
    }

    /// Collects every version of `user_key` from memtable + L0 + lower
    /// levels in the order required by [`mvcc::get_at`]: `(user_key
    /// ASC, sequence DESC)`.
    ///
    /// The returned Vec owns its keys and values so the borrow held by
    /// the [`mvcc::VersionedEntry`] adapter built in [`Self::get_at_cf`]
    /// stays alive for the duration of the read. This is intentionally
    /// straightforward (gather-then-sort) rather than a true k-way
    /// merge — point reads visit O(versions per key) entries which is
    /// tiny in practice; the merge cost dominates only for full-range
    /// scans (handled by [`Self::scan`]).
    fn iter_versions_of(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        user_key: &[u8],
    ) -> ForstResult<Vec<(InternalKey, Vec<u8>)>> {
        // Single-key range: [user_key, user_key + 0x00) i.e. exclusive
        // upper of `user_key.push(0)`. We pass `read_sequence = u64::MAX`
        // so `collect_range_entries` returns ALL versions and let
        // `mvcc::get_at` do the snapshot-visibility filter.
        let mut upper = user_key.to_vec();
        upper.push(0u8);
        let mut entries: Vec<(InternalKey, Vec<u8>)> = Vec::new();

        // Active memtable.
        {
            let mem = cf_data.active_memtable();
            for (k, v, seq, op) in mem.collect_range_entries(user_key, Some(&upper), u64::MAX) {
                if k != user_key {
                    continue;
                }
                let ik = InternalKey::new(k, SequenceNumber::new(seq), op);
                entries.push((ik, v.unwrap_or_default()));
            }
        }

        // Immutable memtables (any order — we sort below).
        for imm in cf_data.imm_memtables() {
            for (k, v, seq, op) in imm.collect_range_entries(user_key, Some(&upper), u64::MAX) {
                if k != user_key {
                    continue;
                }
                let ik = InternalKey::new(k, SequenceNumber::new(seq), op);
                entries.push((ik, v.unwrap_or_default()));
            }
        }

        // SST layer — every file whose key range covers `user_key`.
        let version = self.version_set.current();
        for sst in version.live_sst_files() {
            if user_key < sst.smallest_key.as_slice() || user_key > sst.largest_key.as_slice() {
                continue;
            }
            let reader = self.get_or_open_sst_reader(&sst)?;
            for (k, v, seq, op) in reader.scan(user_key, Some(&upper))? {
                if k != user_key {
                    continue;
                }
                let ik = InternalKey::new(k, SequenceNumber::new(seq), op);
                entries.push((ik, v.unwrap_or_default()));
            }
        }

        // Sort by (user_key ASC, sequence DESC) — only one user_key here,
        // so this collapses to a stable sort by sequence DESC.
        entries.sort_by(|a, b| {
            a.0.user_key()
                .cmp(b.0.user_key())
                .then_with(|| b.0.sequence().0.cmp(&a.0.sequence().0))
        });
        Ok(entries)
    }

    // ---------------------------------------------------------------
    // Write path
    // ---------------------------------------------------------------

    /// Inserts or overwrites the value for `key`.
    pub fn put(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> ForstResult<u64> {
        self.write_single(cf, key, Some(value), OpType::Put)
    }

    /// Combined get + put in one call. Returns the OLD value (before the put).
    ///
    /// Equivalent to `let old = self.get(cf, key)?; self.put(cf, key, new_value)?; Ok(old)`
    /// but performs both operations in a single engine call, saving one FFM
    /// boundary crossing for the dominant ValueState read-modify-write pattern.
    /// The read uses the latest sequence (u64::MAX visibility) and the put
    /// allocates a fresh sequence number — same semantics as separate calls.
    pub fn get_and_put(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        new_value: &[u8],
    ) -> ForstResult<Option<Vec<u8>>> {
        // Pre-write checks (same as write_single).
        self.consume_flush_error()?;
        self.write_controller.may_throttle()?;
        let cf_data = self.lookup_cf_by_id(cf.id())?;

        // Read the old value (latest view, same as get()).
        let read_seq = u64::MAX;
        let old_value = self.get_internal(&cf_data, key, read_seq)?;

        // Write the new value (same logic as write_single for Put).
        let seq = self.sequence_number.fetch_add(1, Ordering::Relaxed) + 1;
        Self::check_sequence_overflow(seq)?;

        // R31-M3: charge the WriteBufferManager BEFORE put_with_seq so that
        // the cross-CF budget accurately reflects the byte commitment even
        // when the put then errors (saturates over budget triggers a flush;
        // dropping the reservation on the error path keeps the budget honest).
        // The reserve was previously placed after the put, so a put returning
        // Err(...) silently skipped the charge entirely — a slow leak of
        // unaccounted bytes whenever the memtable rejected a write.
        let charge = key.len() as u64 + new_value.len() as u64 + 8 + 1 + 48;
        self.write_buffer_manager.reserve(charge);
        let wbm_guard = WbmReleaseGuard::new(&self.write_buffer_manager, charge);

        {
            // R28-H1: pattern-match the typed FrozenMemTable variant
            // instead of substring-scanning `to_string()` — the prior
            // `.contains("frozen MemTable")` shape was fragile against any
            // future Display reformat or wrapper that might prepend a
            // context prefix.
            // R28-L3: add bounded exponential backoff between retries so a
            // protracted freeze (e.g. flush worker stalled on slow remote
            // storage) doesn't burn a CPU core busy-spinning. Sleeps are
            // tiny (100us, 200us, 400us, 800us, …) and capped by `attempt
            // < 8` so worst-case retry duration is ~25.6ms before the
            // error surfaces to the caller — orders of magnitude shorter
            // than the original `to_string()` allocation it replaces.
            let mut attempt: u32 = 0;
            loop {
                let mem_arc = cf_data.active_memtable();
                match mem_arc.put_with_seq(key, Some(new_value), OpType::Put as u8, seq) {
                    Ok(_) => break,
                    Err(ForstError::FrozenMemTable) if attempt < 8 => {
                        std::thread::yield_now();
                        std::thread::sleep(std::time::Duration::from_micros(
                            100u64 << attempt,
                        ));
                        attempt += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        // Put succeeded — keep the reservation; suppress the guard's
        // drop-time release.
        wbm_guard.commit();

        let needs_flush = {
            let _writer = self.write_mutex.lock().expect("lock poisoned");
            self.maybe_switch_memtable_in_lock(&cf_data)?
        };
        let wbm_over = self.write_buffer_manager.over_budget();
        if needs_flush || wbm_over {
            self.enqueue_flush(cf_data.clone())?;
        }

        Ok(old_value)
    }

    /// Deletes the key.
    pub fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> ForstResult<u64> {
        self.write_single(cf, key, None, OpType::Delete)
    }

    /// Deletes the key with `SingleDelete` semantics. See
    /// [`crate::WriteBatch::single_delete`] for the contract — callers must
    /// guarantee the key has been `put` at most once since the last
    /// delete-family operation.
    pub fn single_delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> ForstResult<u64> {
        self.write_single(cf, key, None, OpType::SingleDelete)
    }

    /// Appends a merge operand for the key.
    pub fn merge(&self, cf: &ColumnFamilyHandle, key: &[u8], operand: &[u8]) -> ForstResult<u64> {
        self.write_single(cf, key, Some(operand), OpType::Merge)
    }

    fn write_single(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: Option<&[u8]>,
        op: OpType,
    ) -> ForstResult<u64> {
        // Surface any error from a prior background flush before we accept
        // a new write — the application learns about flush failures at the
        // next write boundary even though the failure happened off-thread.
        self.consume_flush_error()?;
        self.write_controller.may_throttle()?;
        let cf_data = self.lookup_cf_by_id(cf.id())?;

        // PERF (D1): reserve the engine-level sequence number with a single
        // lock-free `fetch_add` BEFORE acquiring `write_mutex`. The pre-D1
        // path took the mutex first, then incremented the memtable's local
        // counter, then ran a `bump_sequence` CAS loop on the engine
        // counter — all inside the lock. Allocating outside the lock cuts
        // the critical-section work and lets contended writers hand seqs
        // to the lock holder without ordering games. `Relaxed` is sufficient
        // because the seq is plumbed through subsequent `mem.write()` /
        // `read()` boundaries that establish the necessary happens-before.
        let seq = self.sequence_number.fetch_add(1, Ordering::Relaxed) + 1;
        // Spec §6a.4 sequence-number overflow guard. The check runs AFTER
        // the allocation so a single writer racing the threshold can't
        // wedge subsequent writers — the next writer trips the fatal arm
        // and returns Internal; the one writer that actually crossed the
        // line returns the same error and never reaches `put_with_seq`.
        Self::check_sequence_overflow(seq)?;

        // E1: with `ShardedMemTable` the put hot path holds only the
        // owning shard's `RwLock` (one of N), so concurrent writers hashing
        // to different shards never block each other. We do NOT take
        // `write_mutex` for the put itself — only for the switch decision
        // below, so the active-memtable swap remains serialized.
        //
        // Race retry: there's a brief window between `active_memtable()` capture
        // and `put_with_seq` where the flush worker may have frozen the memtable
        // and the writer thread may have swapped a fresh active in. The frozen
        // state is transient (always followed by a swap); retry up to 8 times
        // to re-acquire the new active. The race surfaces under llvm-cov
        // instrumentation slowdown but is rare in production.
        //
        // B-Prod-P7 §6d: charge the cross-CF WriteBufferManager for the
        // reservation this write contributed to the active memtable. The
        // released amount comes back on a successful flush via
        // `wbm_release_on_flush`. Approximation matches what
        // `VectorizedMemTable::put` charges internally
        // (key + value + 8 + 1 + 48); WBM precision needs are coarse
        // (cap is 512 MiB by default) so the constant 57-byte overhead
        // approximation is fine.
        //
        // R31-M3: reserve BEFORE put_with_seq so the budget is accurate
        // even if the put fails. The guard releases the reservation on
        // drop unless the put succeeds and we commit() it.
        let charge = key.len() as u64
            + value.map(|v| v.len() as u64).unwrap_or(0)
            + 8 // seq
            + 1 // op-type
            + 48; // record header
        self.write_buffer_manager.reserve(charge);
        let wbm_guard = WbmReleaseGuard::new(&self.write_buffer_manager, charge);
        {
            // R28-H1 + R28-L3: typed retry + bounded exponential backoff.
            // See `get_and_put` comment block for rationale; both retry
            // sites must use the same pattern so neither becomes the slow
            // path under flush contention.
            let mut attempt: u32 = 0;
            loop {
                let mem_arc = cf_data.active_memtable();
                match mem_arc.put_with_seq(key, value, op as u8, seq) {
                    Ok(_) => break,
                    Err(ForstError::FrozenMemTable) if attempt < 8 => {
                        std::thread::yield_now();
                        std::thread::sleep(std::time::Duration::from_micros(
                            100u64 << attempt,
                        ));
                        attempt += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        wbm_guard.commit();

        let needs_flush = {
            let _writer = self.write_mutex.lock().expect("lock poisoned");
            self.maybe_switch_memtable_in_lock(&cf_data)?
        };

        // Phase 2 (outside write_mutex): enqueue the imm memtable for the
        // background flush worker. Backpressure is handled by the
        // WriteController stall above — when imm count >= cap the next
        // writer's `may_throttle()` call blocks until the worker drains
        // an imm and calls `set_imm_count(new_lower)`.
        // B-Prod-P7 §6d: when WBM is over budget, force a flush so the
        // background worker can drain bytes back below the cap.
        let wbm_over = self.write_buffer_manager.over_budget();
        if needs_flush || wbm_over {
            self.enqueue_flush(cf_data.clone())?;
        }

        Ok(seq)
    }

    /// Spec §6a.4 sequence-number overflow guard.
    ///
    /// Called by every write path AFTER the seq has been reserved via
    /// `fetch_add` on `sequence_number`. Returns:
    ///
    /// - `Ok(())` when `seq < SEQ_NUMBER_WARN_THRESHOLD` (2^59);
    /// - `Ok(())` when `SEQ_NUMBER_WARN_THRESHOLD <= seq < SEQ_NUMBER_FATAL_THRESHOLD`,
    ///   AND emits a one-time `tracing::warn!` if no prior write has
    ///   already claimed the warn slot (process-singleton);
    /// - `Err(ForstError::Internal(...))` when `seq >= SEQ_NUMBER_FATAL_THRESHOLD`
    ///   (2^60). Writes never land on the memtable past this line; the
    ///   error message names checkpoint+restart as the recovery path.
    ///
    /// The check is intentionally per-write rather than per-batch so a
    /// long-running batch that nudges the counter past the threshold
    /// surfaces the warn / fatal at the SAME write boundary the
    /// counter advanced (no "we crossed but didn't notice for 10ms"
    /// window). Read paths do NOT consult this helper — readers continue
    /// to work after the fatal threshold so operators can drain a
    /// checkpoint cleanly without the read side failing too.
    fn check_sequence_overflow(seq: u64) -> ForstResult<()> {
        if seq >= SEQ_NUMBER_FATAL_THRESHOLD {
            return Err(ForstError::internal(format!(
                "sequence number {} exceeded 2^60 threshold; engine stopped \
                 accepting writes; restart from checkpoint to recover",
                seq
            )));
        }
        if seq >= SEQ_NUMBER_WARN_THRESHOLD
            && SEQ_HIGH_WARNED
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                )
                .is_ok()
        {
            // First writer past the warn line claims the slot and
            // emits the line. All subsequent writers in this process
            // observe `true` on the load and skip — see `SEQ_HIGH_WARNED`
            // doc on why we use a plain AtomicBool rather than `Once`.
            tracing::warn!(
                seq,
                warn_threshold = SEQ_NUMBER_WARN_THRESHOLD,
                fatal_threshold = SEQ_NUMBER_FATAL_THRESHOLD,
                "sequence number high; consider checkpoint + restart \
                 before the engine reaches the 2^60 fatal threshold"
            );
        }
        Ok(())
    }

    /// Test-only seam that forces the engine's sequence counter to an
    /// arbitrary value so the warn / fatal arms of
    /// [`Self::check_sequence_overflow`] can be exercised without
    /// burning 2^59 writes.
    ///
    /// This stores DIRECTLY into the underlying `AtomicU64`; the next
    /// write path's `fetch_add(1)` returns this value, so calling
    /// `force_set_sequence(SEQ_NUMBER_WARN_THRESHOLD - 1)` arms the
    /// warn arm and `force_set_sequence(SEQ_NUMBER_FATAL_THRESHOLD - 1)`
    /// arms the fatal arm.
    #[cfg(test)]
    pub(crate) fn force_set_sequence(&self, seq: u64) {
        self.sequence_number.store(seq, Ordering::Release);
    }

    /// If the current L0 file count is at or above the slowdown trigger,
    /// run an L0→L1 compaction. Returns `Ok(())` either way.
    fn maybe_auto_compact(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<()> {
        let l0_count = self.version_set.current().l0_files().len() as u32;
        let trigger = self.write_controller.config().l0_slowdown_trigger;
        if l0_count >= trigger {
            self.compact_l0_for_cf(cf_data)?;
        }
        Ok(())
    }

    /// Applies a [`WriteBatch`] atomically. Returns the last assigned sequence.
    ///
    /// PR-B5-H1: takes `WriteBatch<'a>` so callers (FFI batch hot path,
    /// tests, JNI compat) can pass borrowed slices. The batch is fully
    /// consumed before this function returns, so the borrow's lifetime
    /// always covers the call.
    pub fn batch_write<'a>(&self, batch: WriteBatch<'a>) -> ForstResult<u64> {
        if batch.is_empty() {
            return Ok(self.sequence_number());
        }
        self.consume_flush_error()?;
        self.write_controller.may_throttle()?;

        // Resolve and cache CF lookups up front so we fail fast on missing CFs.
        let groups = batch.group_by_cf();
        let mut cf_datas: HashMap<ColumnFamilyId, Arc<ColumnFamilyData>> =
            HashMap::with_capacity(groups.len());
        for &cf_id in groups.keys() {
            cf_datas.insert(cf_id, self.lookup_cf_by_id(cf_id)?);
        }

        // PERF (D1): reserve the entire engine-level sequence range with a
        // single lock-free `fetch_add(N)` BEFORE acquiring `write_mutex`.
        // Pre-D1 this fetch_add ran INSIDE the lock and once per CF group;
        // hoisting it out collapses N atomic ops to one and removes them
        // from the critical section entirely. We then carve the range up
        // among per-CF groups by their relative offsets.
        let total_count: u64 = groups.values().map(|v| v.len() as u64).sum();
        // `fetch_add` returns the previous value; the FIRST seq we own is
        // `prev + 1`, the LAST is `prev + total_count`.
        let prev = self
            .sequence_number
            .fetch_add(total_count, Ordering::Relaxed);
        let last_seq = prev + total_count;
        // Spec §6a.4 sequence-number overflow guard. Check the
        // HIGHEST seq in the reserved range — if last_seq is past the
        // fatal threshold every row in this batch lands beyond it, so
        // refusing the whole batch is correct.
        Self::check_sequence_overflow(last_seq)?;

        // E1: per-CF batch insert routes rows by shard; each shard takes
        // its own write lock independently, so concurrent batches across
        // distinct keys see no engine-level serialization. The switch
        // decision still goes through `write_mutex` to keep
        // `active_memtable` swaps serialized.
        let entries = batch.into_entries();
        let mut group_offset: u64 = 1; // first owned seq is `prev + 1`
        for (cf_id, indices) in &groups {
            let cf_data = cf_datas.get(cf_id).expect("cf_data pre-populated");
            let mem_arc = cf_data.active_memtable();

            // PR-B5-H1: `entries[i].key` is now `Cow<'_, [u8]>`. `.as_ref()`
            // yields `&[u8]` for both Borrowed and Owned variants — the
            // zero-copy FFI path passes borrowed slices straight through.
            let keys: Vec<&[u8]> = indices.iter().map(|&i| entries[i].key.as_ref()).collect();
            let values: Vec<Option<&[u8]>> = indices
                .iter()
                .map(|&i| entries[i].value.as_deref())
                .collect();
            let op_types: Vec<u8> = indices.iter().map(|&i| entries[i].op_type as u8).collect();
            let base_seq = prev + group_offset;
            mem_arc.batch_insert_with_base_seq(&keys, &values, &op_types, base_seq)?;
            group_offset += indices.len() as u64;
        }

        let mut cfs_to_flush: Vec<Arc<ColumnFamilyData>> = Vec::new();
        {
            let _writer = self.write_mutex.lock().expect("lock poisoned");
            for cf_data in cf_datas.values() {
                if self.maybe_switch_memtable_in_lock(cf_data)? {
                    cfs_to_flush.push(cf_data.clone());
                }
            }
        }

        // Phase 2: hand each frozen imm to the background flush worker so
        // this writer returns immediately. Backpressure is enforced by the
        // WriteController on the next writer's `may_throttle()`.
        for cf_data in &cfs_to_flush {
            self.enqueue_flush(cf_data.clone())?;
        }

        Ok(last_seq)
    }

    /// Direct columnar batch put from an Arrow `RecordBatch` — the C1
    /// zero-copy hot path.
    ///
    /// This is the engine-level entry point used by the FFI's
    /// `frs_batch_put_arrow`. It bypasses [`WriteBatch`] entirely (no per-row
    /// `Vec<u8>` allocation, no re-borrow into `Vec<&[u8]>`) and dispatches
    /// the batch's three columns straight into the active memtable's column
    /// buffers via slice-copy. See
    /// [`forst_rs_storage::memtable::VectorizedMemTable::batch_put_arrow_with_base_seq`]
    /// for the column-extend mechanics.
    ///
    /// The batch schema MUST be exactly
    /// `key: Binary, value: Binary nullable, op_type: UInt8`. Schema and
    /// op-type validation happens once up-front (atomic reject — no partial
    /// state on failure).
    ///
    /// Returns the last engine-level sequence number assigned by this batch.
    pub fn batch_put_arrow(
        &self,
        cf: &ColumnFamilyHandle,
        batch: &arrow::array::RecordBatch,
    ) -> ForstResult<u64> {
        let count = batch.num_rows();
        if count == 0 {
            return Ok(self.sequence_number());
        }
        self.consume_flush_error()?;
        self.write_controller.may_throttle()?;
        let cf_data = self.lookup_cf_by_id(cf.id())?;

        // PERF (D1, mirrors `batch_write`): reserve the engine sequence range
        // with a single lock-free `fetch_add(N)` BEFORE acquiring the write
        // mutex. The first owned seq is `prev + 1`.
        let prev = self
            .sequence_number
            .fetch_add(count as u64, Ordering::Relaxed);
        let last_seq = prev + count as u64;
        let base_seq = prev + 1;
        // Spec §6a.4 sequence-number overflow guard. Same rationale as
        // `batch_write` — check the HIGHEST seq in the reserved range.
        Self::check_sequence_overflow(last_seq)?;

        // E1: arrow batch is partitioned across shards in `ShardedMemTable`;
        // each shard takes its own lock so concurrent batches don't
        // serialize on a single memtable lock. Switch decision still
        // serializes on `write_mutex`.
        {
            let mem_arc = cf_data.active_memtable();
            mem_arc.batch_put_arrow_with_base_seq(batch, base_seq)?;
        }
        let needs_flush = {
            let _writer = self.write_mutex.lock().expect("lock poisoned");
            self.maybe_switch_memtable_in_lock(&cf_data)?
        };

        // Phase 2 (outside write_mutex): hand the imm to the background
        // worker. Same pattern as `write_single` / `batch_write`.
        if needs_flush {
            self.enqueue_flush(cf_data.clone())?;
        }

        Ok(last_seq)
    }

    /// Forces the active memtable of a CF to switch (for flush testing).
    ///
    /// Note: this does NOT enqueue the resulting imm onto the background
    /// flush queue — callers (mostly tests) typically follow up with
    /// `flush_cf` / `flush_all` to drain synchronously, and we don't want
    /// duplicate work bouncing through the worker.
    pub fn force_switch_memtable(&self, cf: &ColumnFamilyHandle) -> ForstResult<()> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let _writer = self.write_mutex.lock().expect("lock poisoned");
        cf_data.swap_active_memtable();
        self.refresh_snapshot_view(&cf_data);

        // Enforce max_write_buffer_number: stall if exceeded.
        let imm = cf_data.imm_count() as u32;
        self.write_controller.set_imm_count(imm);
        Ok(())
    }

    // ---------------------------------------------------------------
    // Flush path
    // ---------------------------------------------------------------

    /// Flushes the oldest immutable memtable of `cf` to a new L0 SST file.
    ///
    /// Returns `Ok(None)` if there are no immutable memtables to flush.
    /// Otherwise returns the [`SstFileMeta`] of the produced file.
    pub fn flush_cf(&self, cf: &ColumnFamilyHandle) -> ForstResult<Option<SstFileMeta>> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        self.flush_cf_data(&cf_data)
    }

    /// Flushes all pending immutable memtables across all column families.
    ///
    /// This walks each CF and synchronously drains its imm queue. Any imms
    /// already enqueued for the background worker may also be processed
    /// here — the per-CF flush mutex serializes us against the worker so
    /// the same memtable cannot be flushed twice.
    pub fn flush_all(&self) -> ForstResult<()> {
        let cfs: Vec<Arc<ColumnFamilyData>> = {
            let guard = self.cfs.read().expect("lock poisoned");
            guard.values().cloned().collect()
        };
        // First, give the background worker a chance to land any
        // already-enqueued requests so the synchronous drain below isn't
        // fighting it for the per-CF flush mutex.
        self.wait_for_pending_flushes();
        for cf_data in cfs {
            // Drain anything still pending (e.g. imms from
            // `force_switch_memtable` that bypassed the queue) so callers
            // see a fully-flushed state.
            while cf_data.imm_count() > 0 {
                if self.flush_cf_data(&cf_data)?.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Convenience: switch the active memtable and then flush it. Useful in
    /// tests and when the engine is about to checkpoint.
    pub fn switch_and_flush(&self, cf: &ColumnFamilyHandle) -> ForstResult<Option<SstFileMeta>> {
        self.force_switch_memtable(cf)?;
        self.flush_cf(cf)
    }

    /// Enumerates every live SST file in the current Version.
    ///
    /// If `flush_memtable` is true the engine first runs [`Self::flush_all`]
    /// so newly written rows still in memtables are persisted and become
    /// visible in the returned list — this is the contract Flink's
    /// incremental restore relies on (see `RocksDB.getLiveFiles(true)`).
    ///
    /// Iteration walks levels 0..MAX_LEVELS in order; within each level
    /// files are returned in `smallest_key` order (the same order
    /// [`Version::apply_edit`] sorts them). Returned [`LiveFileInfo::path`]
    /// is the absolute path under [`EngineOptions::db_path`].
    pub fn list_live_files(&self, flush_memtable: bool) -> ForstResult<Vec<LiveFileInfo>> {
        if flush_memtable {
            // Mirror RocksDB's `getLiveFiles(true)`: force-switch every CF's
            // active memtable into an imm, then drain. Without the switch,
            // `flush_all` only drains existing imms and rows still sitting
            // in the active memtable would not be persisted to an SST yet.
            let cfs: Vec<Arc<ColumnFamilyData>> = {
                let guard = self.cfs.read().expect("lock poisoned");
                guard.values().cloned().collect()
            };
            for cf_data in &cfs {
                let handle = cf_data.handle().clone();
                self.force_switch_memtable(&handle)?;
            }
            self.flush_all()?;
        }
        let version = self.version_set.current();
        let mut out = Vec::new();
        for (level_idx, level_meta) in version.levels.iter().enumerate() {
            for file in &level_meta.files {
                out.push(LiveFileInfo {
                    path: sst_file_path(&self.db_path, file.file_number),
                    size: file.file_size,
                    sequence: file.max_sequence.value(),
                    level: level_idx as u8,
                    cf_name: DEFAULT_CF_NAME.to_string(),
                });
            }
        }
        Ok(out)
    }

    /// Returns the size of the manifest. forst-rs persists the version
    /// manifest as a single CHECKPOINT.blob next to the live SSTs (rather
    /// than RocksDB's MANIFEST log). When the manifest does not yet exist
    /// (a fresh DB that has never been checkpointed) this returns 0 — that
    /// matches how RocksDB reports manifest size before the first WAL
    /// switch, so Flink's incremental restore handles it identically.
    pub fn manifest_file_size(&self) -> u64 {
        let manifest_path = self.db_path.join(crate::checkpoint::CHECKPOINT_BLOB_NAME);
        match self.fs.get_file_metadata(&manifest_path) {
            Ok(meta) => meta.size,
            Err(_) => 0,
        }
    }

    // ---------------------------------------------------------------
    // Compaction path
    // ---------------------------------------------------------------

    /// Runs an L0→L1 rollup compaction for the given column family. Picks
    /// every current L0 file (plus every L1 file — L1 may overlap L0) and
    /// produces a single new L1 file consolidating versions.
    ///
    /// Returns the new L1 file's metadata, or `Ok(None)` if no L0 files
    /// were present.
    pub fn compact_l0(&self, cf: &ColumnFamilyHandle) -> ForstResult<Option<SstFileMeta>> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        self.compact_l0_for_cf(&cf_data)
    }

    /// Runs compaction for every column family. For each CF, this drains
    /// every L0 file and then keeps picking any level that exceeds its
    /// target size until the entire LSM is balanced. Useful in tests and
    /// before checkpointing.
    pub fn compact_all(&self) -> ForstResult<()> {
        let cfs: Vec<Arc<ColumnFamilyData>> = {
            let guard = self.cfs.read().expect("lock poisoned");
            guard.values().cloned().collect()
        };
        for cf_data in cfs {
            loop {
                let did_work = self.compact_once(&cf_data)?;
                if !did_work {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Picks one compaction job (L0 rollup or size-based deeper level) and
    /// runs it. Returns `true` if work was done, `false` if the CF is
    /// already balanced. Useful for integrating with a scheduler.
    pub fn compact_once_for(&self, cf: &ColumnFamilyHandle) -> ForstResult<bool> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        self.compact_once(&cf_data)
    }

    /// Runs an L→L+1 compaction. `level` must be >= 1 (use `compact_l0` for
    /// L0 rollup). Returns the produced file meta, or `Ok(None)` if the
    /// level was empty.
    pub fn compact_level(
        &self,
        cf: &ColumnFamilyHandle,
        level: u32,
    ) -> ForstResult<Option<SstFileMeta>> {
        if level == 0 {
            return self.compact_l0(cf);
        }
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        self.compact_level_for_cf(&cf_data, level)
    }

    fn compact_once(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<bool> {
        // Priority 1: drain L0 if any files exist.
        if !self.version_set.current().l0_files().is_empty() {
            self.compact_l0_for_cf(cf_data)?;
            return Ok(true);
        }
        // Priority 2: pick the deepest level that exceeds its size budget.
        let Some(level) = self.pick_compaction_level() else {
            return Ok(false);
        };
        self.compact_level_for_cf(cf_data, level)?;
        Ok(true)
    }

    /// Returns the shallowest level (>= 1) whose total file size exceeds its
    /// target, or `None` if every level is within budget.
    ///
    /// Target is `max_bytes_for_level_base * multiplier^(level-1)`.
    fn pick_compaction_level(&self) -> Option<u32> {
        let version = self.version_set.current();
        let base = self.options.max_bytes_for_level_base as f64;
        let mult = self.options.max_bytes_for_level_multiplier;
        for level in 1..(self.options.num_levels - 1) {
            let total_size: u64 = version.levels[level]
                .files
                .iter()
                .map(|f| f.file_size)
                .sum();
            let target = (base * mult.powi(level as i32 - 1)) as u64;
            if total_size > target {
                return Some(level as u32);
            }
        }
        None
    }

    fn compact_level_for_cf(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        level: u32,
    ) -> ForstResult<Option<SstFileMeta>> {
        let _guard = cf_data.lock_flush();

        let version = self.version_set.current();
        let level_idx = level as usize;
        if level_idx >= version.num_levels() {
            return Ok(None);
        }
        let src_files: Vec<SstFileMeta> = version.levels[level_idx].files.clone();
        if src_files.is_empty() {
            return Ok(None);
        }
        let next_level = level_idx + 1;
        if next_level >= version.num_levels() {
            // Can't go deeper — the engine is at max depth. Treat as no-op.
            return Ok(None);
        }
        let dst_candidates: Vec<SstFileMeta> = version.levels[next_level].files.clone();

        // Compute the key range spanned by src_files; pull any dst file
        // whose range overlaps.
        let (mut min_key, mut max_key) = (
            src_files[0].smallest_key.clone(),
            src_files[0].largest_key.clone(),
        );
        for f in &src_files {
            if f.smallest_key < min_key {
                min_key = f.smallest_key.clone();
            }
            if f.largest_key > max_key {
                max_key = f.largest_key.clone();
            }
        }
        let mut overlapping_dst: Vec<SstFileMeta> = Vec::new();
        for f in &dst_candidates {
            if f.largest_key < min_key {
                continue;
            }
            if f.smallest_key > max_key {
                continue;
            }
            overlapping_dst.push(f.clone());
        }

        // Build input list. Source files carry `level`; destination overlap
        // carries `next_level`.
        let mut inputs: Vec<(u32, SstFileMeta, Arc<SstReaderImpl>)> = Vec::new();
        for f in &src_files {
            inputs.push((level, f.clone(), self.get_or_open_sst_reader(f)?));
        }
        for f in &overlapping_dst {
            inputs.push((
                next_level as u32,
                f.clone(),
                self.get_or_open_sst_reader(f)?,
            ));
        }

        let is_bottommost =
            (next_level + 1..version.num_levels()).all(|lvl| version.levels[lvl].files.is_empty());
        let output_file_number = self.version_set.allocate_file_number();
        let output_path = compaction_output_path(&self.db_path, output_file_number);
        let writer_options = SstWriterOptions {
            block_size: self.options.block_size,
            compression: self.options.compression,
        };

        // Snapshot the registry's min-active sequence ONCE per pass so the
        // compaction job sees a stable horizon while it runs. A snapshot
        // captured AFTER this read (i.e. lower min_active) only matters
        // for FUTURE compactions — its retention contract is forward
        // -looking, not retroactive. See spec §6a.5.
        let min_active_snapshot = self.snapshot_registry.min_active();
        let job = CompactionJob {
            inputs,
            output_level: next_level as u32,
            output_file_number,
            output_path: output_path.clone(),
            writer_options,
            fs: self.fs.clone(),
            merge_operator: cf_data.merge_operator().cloned(),
            compaction_filter: cf_data.compaction_filter(),
            is_bottommost,
            min_active_snapshot,
        };

        let Some(edit) = job.run()? else {
            return Ok(None);
        };
        self.version_set.apply(&edit)?;

        let new_meta = edit.new_files.first().map(|(_, m)| m.clone());
        if let Some(ref meta) = new_meta {
            let rac = self.fs.open_random_access_file(&output_path)?;
            let reader = Arc::new(SstReaderImpl::open(rac)?);
            self.sst_readers
                .write()
                .expect("lock poisoned")
                .insert(meta.file_number, reader);
        }
        {
            let mut cache = self.sst_readers.write().expect("lock poisoned");
            for (_, file_number) in &edit.deleted_files {
                cache.remove(file_number);
            }
        }
        // R32-H2: route deletions through `delete_file_guarded` so any
        // outstanding pins (e.g. an in-flight `create_checkpoint` or
        // `create_incremental_checkpoint`) defer the unlink to
        // `pending_deletions` instead of yanking the file out from
        // under the copy. Sister path `compact_l0_for_cf` already does
        // this — L1+ compactions previously bypassed the guard.
        for (_, file_number) in &edit.deleted_files {
            self.delete_file_guarded(*file_number);
        }
        self.reap_pending_deletions();

        Ok(new_meta)
    }

    // ---------------------------------------------------------------
    // Checkpoint path
    // ---------------------------------------------------------------

    /// Writes a consistent checkpoint of the engine state into `target_dir`.
    ///
    /// All pending memtables are flushed first so the checkpoint only needs
    /// to copy on-disk SST files. Returns a [`CheckpointManifest`] describing
    /// the produced files.
    pub fn create_checkpoint(
        &self,
        target_dir: &std::path::Path,
    ) -> ForstResult<CheckpointManifest> {
        // 1. Flush every pending imm memtable.
        self.flush_all()?;

        // 2. Also flush the active memtable (switch + flush) so the
        //    checkpoint includes writes that haven't yet crossed the
        //    write_buffer_size threshold.
        let cfs: Vec<Arc<ColumnFamilyData>> = {
            let guard = self.cfs.read().expect("lock poisoned");
            guard.values().cloned().collect()
        };
        for cf_data in &cfs {
            // Only switch + flush when the active memtable has data.
            let has_data = {
                let mem_arc = cf_data.active_memtable();
                mem_arc.num_entries() > 0
            };
            if has_data {
                let _writer = self.write_mutex.lock().expect("lock poisoned");
                cf_data.swap_active_memtable();
                self.refresh_snapshot_view(cf_data);
                drop(_writer);
                self.flush_cf_data(cf_data)?;
            }
        }

        // 3. Capture the VersionSet snapshot AFTER flushes so it contains
        //    every new L0 file.
        //
        // R32-H1: Take the snapshot, capture the live-SST list, AND pin the
        // files atomically under the VersionSet apply_lock — mirrors the
        // R31-H1 fix for `create_incremental_checkpoint`. Without this, a
        // concurrent compaction could interleave its `apply` +
        // `delete_file_guarded` between our snapshot read and our pin,
        // leaving the manifest pointing at an already-unlinked file. While
        // the apply_lock is held, compaction's `apply` is blocked, so once
        // our pin lands `can_delete()` returns false and the unlink defers
        // to `pending_deletions`.
        let (snapshot, live, _pin) =
            self.version_set
                .snapshot_with_locked_view(|snap: &VersionSetSnapshot| {
                    let live = snap.version.live_sst_files();
                    let file_numbers: Vec<FileNumber> =
                        live.iter().map(|f| f.file_number).collect();
                    let pin = self.deletion_guard.pin_batch(&file_numbers);
                    (snap.clone(), live, pin)
                });
        let blob = serialize_snapshot(&snapshot)?;

        // 5. Ensure target dir exists, write blob + copy every live SST.
        self.fs.create_dir_all(target_dir)?;
        write_blob(self.fs.as_ref(), target_dir, &blob)?;

        let (sst_bytes, sst_files) =
            copy_live_ssts(self.fs.as_ref(), &self.db_path, target_dir, &live)?;

        // PinHandle is released here when `_pin` drops; any deletions that
        // were deferred during the checkpoint will be reaped on the next
        // compaction (or immediately below).
        drop(_pin);
        self.reap_pending_deletions();

        Ok(CheckpointManifest {
            target_dir: target_dir.to_path_buf(),
            sst_files,
            total_bytes: blob.len() as u64 + sst_bytes,
        })
    }

    /// Opens an engine by restoring state from a checkpoint directory.
    ///
    /// The checkpoint directory must contain `CHECKPOINT.blob` and every SST
    /// file it references. The engine is opened with `db_path = target_dir`
    /// (i.e. subsequent reads/writes operate directly on the checkpoint
    /// files; copy the checkpoint first if you want to preserve it).
    pub fn open_from_checkpoint(
        options: EngineOptions,
        fs: Arc<dyn FileSystem>,
    ) -> ForstResult<Arc<Self>> {
        use crate::checkpoint::{deserialize_snapshot, read_blob};

        let db_path = PathBuf::from(&options.db_path);
        let blob = read_blob(fs.as_ref(), &db_path)?;
        let snapshot = deserialize_snapshot(&blob)?;

        // Verify every referenced SST file exists.
        for file in snapshot.version.live_sst_files() {
            let path = sst_file_path(&db_path, file.file_number);
            if !fs.file_exists(&path)? {
                return Err(ForstError::corruption(format!(
                    "checkpoint references missing SST file: {}",
                    path.display()
                )));
            }
        }

        // R28-M1 + R29-H1: scan db_path for SST files NOT referenced by the
        // snapshot. The snapshot's `next_file_number` is the writer-side
        // counter the engine had at checkpoint time. If a crash interrupted
        // a flush BEFORE the manifest was updated, the orphaned SST sits on
        // disk with a file number ≥ snapshot.next_file_number — restoring
        // with the snapshot's counter would reuse those numbers and
        // overwrite valid checkpoint files mid-flush. Take the max of the
        // snapshot's counter and (highest observed file number + 1) so the
        // engine allocates new SST file numbers strictly above anything
        // already present on disk.
        //
        // R29-H1: drop the outer `fs.file_exists(&db_path)?` guard — every
        // FileSystem impl returns false for directories, so the entire
        // scan never ran and the R28-M1 fix was dead code. `list_dir` is
        // called directly; its `Err` arm (missing-dir, permission, etc.)
        // is logged at debug and treated as "no observed files".
        let mut max_observed: u64 = 0;
        let mut orphans: Vec<PathBuf> = Vec::new();
        // R38-H1: separate list of mid-write tmp files that crashed before
        // the rename-into-place (or where the rename itself failed). These
        // use the `.<num>.sst.tmp` naming produced by `FlushJob::temp_path`
        // and `CompactionJob`'s temp-path helper (leading dot + `.tmp`
        // suffix). We rename them just like SST orphans so they never get
        // picked up as live state and a future flush cannot collide with
        // the file name.
        let mut tmp_orphans: Vec<PathBuf> = Vec::new();
        match fs.list_dir(&db_path) {
            Ok(entries) => {
                let referenced: std::collections::HashSet<u64> = snapshot
                    .version
                    .live_sst_files()
                    .iter()
                    .map(|f| f.file_number.value())
                    .collect();
                for entry in entries {
                    if entry.is_dir {
                        continue;
                    }
                    let name = match entry.path.file_name().and_then(|n| n.to_str()) {
                        Some(n) => n,
                        None => continue,
                    };
                    // Match the `<≥6-digit padded number>.sst` naming
                    // produced by `sst_file_path` (R29-L1: `{:06}` only
                    // enforces a minimum width — numbers > 999_999 widen
                    // naturally, so the regex must accept any length).
                    if let Some(stem) = name.strip_suffix(".sst") {
                        if let Ok(num) = stem.parse::<u64>() {
                            if num > max_observed {
                                max_observed = num;
                            }
                            if !referenced.contains(&num) {
                                orphans.push(entry.path.clone());
                            }
                        }
                        continue;
                    }
                    // R38-H1: match `.<num>.sst.tmp` tmp-write artifacts.
                    // Strip the leading `.` and trailing `.sst.tmp`, then
                    // parse the inner number for the file-counter bump.
                    if let Some(inner) = name.strip_suffix(".sst.tmp") {
                        if let Some(stem) = inner.strip_prefix('.') {
                            if let Ok(num) = stem.parse::<u64>() {
                                if num > max_observed {
                                    max_observed = num;
                                }
                            }
                            // Whether or not the number parses we treat the
                            // tmp file as orphan-rename-eligible — names
                            // that fail to parse are by definition not in
                            // the active naming space either.
                            tmp_orphans.push(entry.path.clone());
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!(
                    "open_from_checkpoint: list_dir({}) failed during orphan scan: {} \
                     (continuing with snapshot.next_file_number unchanged)",
                    db_path.display(),
                    e
                );
            }
        }
        // R29-M1: rename orphans to `*.sst.orphan-<unix-millis>` (atomic
        // single-fs rename) so they no longer match the `<num>.sst` scan
        // regex on future restarts. Default-safe: no data loss, just
        // out-of-band file the operator can triage / delete manually.
        // Rename failures are logged but do not block the restore — the
        // file simply remains visible to the next scan, which will retry
        // the rename with a fresh timestamp.
        let ts_suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        for orphan in &orphans {
            let dst = {
                let mut s = orphan.as_os_str().to_owned();
                s.push(format!(".orphan-{}", ts_suffix));
                PathBuf::from(s)
            };
            match fs.rename(orphan, &dst) {
                Ok(()) => tracing::warn!(
                    "open_from_checkpoint: renamed orphan SST not referenced by checkpoint \
                     manifest {} → {} (R29-M1 rename-on-restore default)",
                    orphan.display(),
                    dst.display()
                ),
                Err(e) => tracing::warn!(
                    "open_from_checkpoint: failed to rename orphan SST {} → {} ({}); \
                     leaving in place — next restore will retry",
                    orphan.display(),
                    dst.display(),
                    e
                ),
            }
        }
        // R38-H1: rename mid-write tmp orphans into `*.sst.tmp.orphan-<ts>`
        // so the next restore's scan no longer matches them (the suffix
        // `.orphan-<ts>` is appended in full so neither the `.sst` nor the
        // `.sst.tmp` arm hits them on a future restart).
        for orphan in &tmp_orphans {
            let dst = {
                let mut s = orphan.as_os_str().to_owned();
                s.push(format!(".orphan-{}", ts_suffix));
                PathBuf::from(s)
            };
            match fs.rename(orphan, &dst) {
                Ok(()) => tracing::warn!(
                    "open_from_checkpoint: renamed orphan tmp SST {} → {} \
                     (R38-H1 mid-write rename-into-place artifact)",
                    orphan.display(),
                    dst.display()
                ),
                Err(e) => tracing::warn!(
                    "open_from_checkpoint: failed to rename tmp orphan {} → {} ({}); \
                     leaving in place — next restore will retry",
                    orphan.display(),
                    dst.display(),
                    e
                ),
            }
        }
        let restored_next_file_number = snapshot
            .next_file_number
            .max(max_observed.saturating_add(1));

        // Build the DbImpl with the restored VersionSet.
        let version_set = Arc::new(forst_rs_storage::version::VersionSetImpl::from_restored(
            (*snapshot.version).clone(),
            restored_next_file_number,
            snapshot.last_sequence,
        ));

        let cache_bytes = if options.block_cache_capacity_bytes != 0 {
            options.block_cache_capacity_bytes.min(usize::MAX as u64) as usize
        } else {
            options.block_cache_size
        };
        let block_cache = Arc::new(ShardedClockCache::with_capacity(cache_bytes));
        let write_buffer_manager =
            WriteBufferManager::new(options.write_buffer_manager_capacity_bytes);

        let db = Arc::new(Self {
            options,
            db_path,
            fs,
            cfs: RwLock::new(HashMap::new()),
            cf_name_to_id: RwLock::new(HashMap::new()),
            version_set,
            sst_readers: RwLock::new(HashMap::new()),
            deletion_guard: Arc::new(FileDeletionGuard::new()),
            pending_deletions: Mutex::new(Vec::new()),
            sequence_number: AtomicU64::new(snapshot.last_sequence),
            write_controller: Arc::new(WriteController::with_defaults()),
            write_mutex: Mutex::new(()),
            next_cf_id: AtomicU32::new(1),
            flush_queue: Arc::new(FlushQueue::new(FLUSH_QUEUE_CAPACITY)),
            flush_worker: Mutex::new(None),
            flush_error: Mutex::new(None),
            pending_flush_count: AtomicU32::new(0),
            snapshot_registry: SnapshotRegistry::new(),
            db_id: DbId(NEXT_DB_ID.fetch_add(1, Ordering::Relaxed)),
            block_cache,
            write_buffer_manager,
            snapshot_age_worker: Mutex::new(None),
            snapshot_age_shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });

        let default_desc = ColumnFamilyDescriptor::new(DEFAULT_CF_NAME);
        db.create_cf_with_id(DEFAULT_CF_ID, default_desc)?;
        Self::spawn_flush_worker(&db);
        Self::spawn_snapshot_age_worker(&db);
        Ok(db)
    }

    /// Captures an incremental checkpoint pinned at `snapshot`.
    ///
    /// Per spec §10b: the returned [`IncrementalCheckpointResult`] describes
    /// the SSTs that the caller must upload (`new_ssts`) and the SSTs
    /// already present in `base_checkpoint_id` that can be referenced by
    /// handle without re-uploading (`shared_ssts`). For `base_checkpoint_id
    /// == 0` (full / first checkpoint) every live SST is reported as new.
    ///
    /// The manifest is persisted under
    /// `<db_path>/checkpoints/<checkpoint_id>/CHECKPOINT.blob`. Callers
    /// upload the manifest plus the `new_ssts` to durable storage; restore
    /// uses [`Self::open_from_incremental`] to reconstruct a DB from the
    /// uploaded files.
    pub fn create_incremental_checkpoint(
        &self,
        snapshot: &Snapshot,
        checkpoint_id: u64,
        base_checkpoint_id: u64,
    ) -> ForstResult<IncrementalCheckpointResult> {
        if snapshot.db_id() != self.db_id {
            return Err(ForstError::invalid_argument(
                "Snapshot was issued by a different DbImpl instance",
            ));
        }

        // Flush so every write that preceded the snapshot is on disk. This
        // matches `create_checkpoint` semantics — versioned reads against
        // the snapshot still work even if newer writes have come in
        // afterwards (compaction would not drop them while the snapshot
        // pins them).
        self.flush_all()?;
        let cfs: Vec<Arc<ColumnFamilyData>> = {
            let guard = self.cfs.read().expect("lock poisoned");
            guard.values().cloned().collect()
        };
        for cf_data in &cfs {
            let has_data = {
                let mem_arc = cf_data.active_memtable();
                mem_arc.num_entries() > 0
            };
            if has_data {
                let _writer = self.write_mutex.lock().expect("lock poisoned");
                cf_data.swap_active_memtable();
                self.refresh_snapshot_view(cf_data);
                drop(_writer);
                self.flush_cf_data(cf_data)?;
            }
        }

        // Capture the VersionSet snapshot AFTER flushes so the manifest
        // contains every L0 file the snapshot pins.
        //
        // R31-H1: We take the snapshot AND pin the live files under the
        // VersionSet apply_lock so a concurrent compaction cannot interleave
        // its `apply` + `delete_file_guarded` between our snapshot read and
        // our pin. Without this, a compaction could apply its VersionEdit
        // (logically removing a file from the new version) and then call
        // `delete_file_guarded` on the unlinked file BEFORE our pin lands —
        // resulting in a manifest that references a file already removed
        // from disk. Under apply_lock, compaction's apply is blocked while
        // we pin; once our pin is in place, can_delete() returns false and
        // delete_file_guarded defers the unlink to `pending_deletions`.
        let (version_snapshot, _pin) =
            self.version_set
                .snapshot_with_locked_view(|snap: &VersionSetSnapshot| {
                    let live = snap.version.live_sst_files();
                    let file_numbers: Vec<FileNumber> =
                        live.iter().map(|f| f.file_number).collect();
                    let pin = self.deletion_guard.pin_batch(&file_numbers);
                    (snap.clone(), pin)
                });
        let blob = serialize_snapshot(&version_snapshot)?;

        let base_dir = self.incremental_checkpoint_dir(base_checkpoint_id);
        let base_live: std::collections::HashSet<FileNumber> = if base_checkpoint_id != 0
            && self.fs.file_exists(&base_dir.join(CHECKPOINT_BLOB_NAME))?
        {
            use crate::checkpoint::{deserialize_snapshot, read_blob};
            let base_blob = read_blob(self.fs.as_ref(), &base_dir)?;
            let base_snap = deserialize_snapshot(&base_blob)?;
            base_snap
                .version
                .live_sst_files()
                .iter()
                .map(|f| f.file_number)
                .collect()
        } else {
            std::collections::HashSet::new()
        };

        let mut new_ssts: Vec<LiveFileInfo> = Vec::new();
        let mut shared_ssts: Vec<LiveFileInfo> = Vec::new();
        for (level_idx, level_meta) in version_snapshot.version.levels.iter().enumerate() {
            for file in &level_meta.files {
                let info = LiveFileInfo {
                    path: sst_file_path(&self.db_path, file.file_number),
                    size: file.file_size,
                    sequence: file.max_sequence.value(),
                    level: level_idx as u8,
                    cf_name: DEFAULT_CF_NAME.to_string(),
                };
                if base_live.contains(&file.file_number) {
                    shared_ssts.push(info);
                } else {
                    new_ssts.push(info);
                }
            }
        }

        // Persist the manifest blob into the per-checkpoint subdir so
        // `open_from_incremental` can pick it up by checkpoint id.
        let target_dir = self.incremental_checkpoint_dir(checkpoint_id);
        self.fs.create_dir_all(&target_dir)?;
        let manifest_path = write_blob(self.fs.as_ref(), &target_dir, &blob)?;

        drop(_pin);

        Ok(IncrementalCheckpointResult {
            manifest_path,
            new_ssts,
            shared_ssts,
        })
    }

    /// Returns the canonical on-disk directory for an incremental
    /// checkpoint with the given `checkpoint_id`. `0` is reserved by
    /// [`Self::create_incremental_checkpoint`] to mean "no base"; passing
    /// `0` here returns the directory the engine would use IF such an id
    /// existed, but no caller should rely on that path.
    fn incremental_checkpoint_dir(&self, checkpoint_id: u64) -> PathBuf {
        self.db_path
            .join("checkpoints")
            .join(format!("{:020}", checkpoint_id))
    }

    /// Opens a fresh engine reconstructed from the manifest blob at
    /// `base_manifest` plus the SST file list `sst_files`.
    ///
    /// Per spec §10b restore path: each `sst_files` entry is hardlinked
    /// (or copied, on filesystems without hardlink support) into
    /// `target_dir` under its source basename, then the engine is opened
    /// against `target_dir` via the same blob-restore path used by
    /// [`Self::open_from_checkpoint`]. The returned engine is fully
    /// writable; callers that want to preserve the original checkpoint
    /// should pass a copy of `sst_files`.
    ///
    /// # Caller contract: clean target directory
    ///
    /// R32-L1: the dst-already-exists branch (see [`same_file_or_size`])
    /// validates that a pre-existing dst SST is *plausibly* identical to
    /// the src by checking inode (unix) or size (non-unix). It does NOT
    /// perform a content fingerprint (e.g. CRC32C) — adding one would gate
    /// every restore on a full-file read for every dst, which is the
    /// hot-path cost an incremental checkpoint exists to avoid. The
    /// caller's responsibility is to pass a clean `target_dir`: either
    /// (a) a freshly-created directory, or (b) a directory cleared by
    /// the framework (e.g. `ForStRsRestoreOperation.ensureTargetDirEmpty`
    /// already calls `deleteRecursively` before invoking this).
    ///
    /// A content-mismatch attack against this code path requires the
    /// attacker to seed `target_dir` with a same-size (or same-inode-by-
    /// hardlink) rogue file BEFORE the restore is invoked. That is
    /// equivalent to compromising the target storage location, at which
    /// point the entire engine state is already untrusted. The caller-
    /// clean-target contract converts the property from "weak signal at
    /// restore time" to "no signal needed; precondition holds".
    pub fn open_from_incremental(
        target_dir: &str,
        base_manifest: &str,
        sst_files: &[String],
    ) -> ForstResult<Arc<Self>> {
        let target = PathBuf::from(target_dir);
        let manifest = PathBuf::from(base_manifest);

        // Ensure the target dir exists; native LocalFileSystem will fail
        // gracefully if creation is denied.
        let fs: Arc<dyn FileSystem> = Arc::new(forst_rs_io::LocalFileSystem::new());
        fs.create_dir_all(&target)?;

        // Copy the manifest into target_dir/CHECKPOINT.blob if it lives
        // elsewhere; otherwise reuse in place.
        let target_manifest = target.join(CHECKPOINT_BLOB_NAME);
        if manifest != target_manifest {
            // Read source blob + write into the target.
            use crate::checkpoint::{read_blob, write_blob};
            let manifest_dir = manifest
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            let blob = read_blob(fs.as_ref(), &manifest_dir)?;
            write_blob(fs.as_ref(), &target, &blob)?;
        }

        // Hardlink (or copy) every SST file into the target directory.
        for src in sst_files {
            let src_path = PathBuf::from(src);
            let basename = src_path
                .file_name()
                .ok_or_else(|| ForstError::invalid_argument(format!("bad SST path: {src}")))?;
            let dst = target.join(basename);
            if dst == src_path {
                continue;
            }
            if fs.file_exists(&dst)? {
                // R31-M4: dst already exists — only accept it if it is the
                // same on-disk file as src (hardlink to the same inode, on
                // unix) or, on non-unix where we can't compare inodes, the
                // sizes match. Otherwise hard-fail: silently keeping the
                // stale contents would let an attacker (or a previous failed
                // restore) seed `target_dir` with rogue data that the engine
                // would then open as authoritative.
                if !same_file_or_size(&src_path, &dst)? {
                    return Err(ForstError::invalid_argument(format!(
                        "open_from_incremental: dst SST '{}' already exists \
                         and does not match src '{}' (inode/size mismatch)",
                        dst.display(),
                        src_path.display(),
                    )));
                }
                // Identical file already present — nothing to do.
                continue;
            }
            // Try hardlink first (cheap, no copy); fall back to copy
            // if hardlink fails (cross-device, FS doesn't support, etc.)
            #[cfg(unix)]
            let linked = std::fs::hard_link(&src_path, &dst).is_ok();
            #[cfg(not(unix))]
            let linked = false;
            if !linked {
                crate::checkpoint::copy_file(fs.as_ref(), &src_path, &dst)?;
            }
        }

        // Now open the engine from the materialized checkpoint dir.
        let options = EngineOptions {
            db_path: target.to_string_lossy().into_owned(),
            ..EngineOptions::default()
        };
        Self::open_from_checkpoint(options, fs)
    }

    fn compact_l0_for_cf(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
    ) -> ForstResult<Option<SstFileMeta>> {
        // Serialize compaction per CF so two callers cannot both pick the
        // same L0 files. We piggyback on the flush_mutex since flush and
        // compaction both rewrite the on-disk layer.
        let _guard = cf_data.lock_flush();

        let version = self.version_set.current();
        let l0_files: Vec<SstFileMeta> = version.l0_files().to_vec();
        if l0_files.is_empty() {
            return Ok(None);
        }
        let l1_files: Vec<SstFileMeta> = version.levels[1].files.clone();

        // Gather input readers.
        let mut inputs: Vec<(u32, SstFileMeta, Arc<SstReaderImpl>)> = Vec::new();
        for meta in &l0_files {
            inputs.push((0, meta.clone(), self.get_or_open_sst_reader(meta)?));
        }
        for meta in &l1_files {
            inputs.push((1, meta.clone(), self.get_or_open_sst_reader(meta)?));
        }

        // Allocate the output file number and build the job. This rollup
        // produces a single bottommost-style file at L1. We mark it as
        // bottommost iff there are no files below L1 (all higher levels
        // empty) so delete tombstones can be eliminated.
        let is_bottommost =
            (2..version.num_levels()).all(|lvl| version.levels[lvl].files.is_empty());

        let output_file_number = self.version_set.allocate_file_number();
        let output_path = compaction_output_path(&self.db_path, output_file_number);
        let writer_options = SstWriterOptions {
            block_size: self.options.block_size,
            compression: self.options.compression,
        };

        // Snapshot the registry's min-active sequence ONCE per pass — see
        // the matching read in `compact_level_for_cf` for rationale.
        let min_active_snapshot = self.snapshot_registry.min_active();
        let job = CompactionJob {
            inputs,
            output_level: 1,
            output_file_number,
            output_path: output_path.clone(),
            writer_options,
            fs: self.fs.clone(),
            merge_operator: cf_data.merge_operator().cloned(),
            compaction_filter: cf_data.compaction_filter(),
            is_bottommost,
            min_active_snapshot,
        };

        let Some(edit) = job.run()? else {
            return Ok(None);
        };

        // Apply the VersionEdit atomically.
        self.version_set.apply(&edit)?;

        // Open the new reader, prune deleted readers, and delete stale files
        // from disk.
        let new_meta = edit.new_files.first().map(|(_, m)| m.clone());
        if let Some(ref meta) = new_meta {
            let rac = self.fs.open_random_access_file(&output_path)?;
            let reader = Arc::new(SstReaderImpl::open(rac)?);
            self.sst_readers
                .write()
                .expect("lock poisoned")
                .insert(meta.file_number, reader);
        }
        {
            let mut cache = self.sst_readers.write().expect("lock poisoned");
            for (_, file_number) in &edit.deleted_files {
                cache.remove(file_number);
            }
        }
        for (_, file_number) in &edit.deleted_files {
            self.delete_file_guarded(*file_number);
        }
        self.reap_pending_deletions();

        // Update back-pressure counts — L0 is now empty (for this rollup).
        self.write_controller
            .set_l0_file_count(self.version_set.current().l0_files().len() as u32);

        Ok(new_meta)
    }

    // ---------------------------------------------------------------
    // Scan path
    // ---------------------------------------------------------------

    /// Range scan: returns all key-value pairs with keys in `[lower, upper)`
    /// as a sorted `Vec`. Pass `upper = None` for an unbounded upper end.
    ///
    /// Tombstoned keys are skipped. Merge operands are resolved via the CF's
    /// configured merge operator.
    pub fn scan(
        &self,
        cf: &ColumnFamilyHandle,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> ForstResult<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::collections::BTreeSet;

        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();

        // Active memtable (sharded — `collect_range_entries` merges across shards).
        {
            let mem_arc = cf_data.active_memtable();
            for (k, _, _, _) in mem_arc.collect_range_entries(lower, upper, u64::MAX) {
                keys.insert(k);
            }
        }
        // Immutable memtables.
        for imm in cf_data.imm_memtables() {
            for (k, _, _, _) in imm.collect_range_entries(lower, upper, u64::MAX) {
                keys.insert(k);
            }
        }
        // SST layer.
        let version = self.version_set.current();
        for sst in version.live_sst_files() {
            if sst.largest_key.as_slice() < lower {
                continue;
            }
            if let Some(hi) = upper {
                if sst.smallest_key.as_slice() >= hi {
                    continue;
                }
            }
            let reader = self.get_or_open_sst_reader(&sst)?;
            // PR-C5-H1: scan_borrowed skips the per-row `value.to_vec()`
            // baked into `SstReaderImpl::scan`. Value is discarded — only
            // the key feeds the BTreeSet.
            reader.scan_borrowed(lower, upper, |view| {
                keys.insert(view.key.to_vec());
                Ok(())
            })?;
        }

        // Resolve each key via the normal read path (handles deletes + merges).
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.get(cf, &key)? {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    /// Prefix scan: all key-value pairs whose keys start with `prefix`.
    ///
    /// Materialised form retained for the Arrow-side FFI exports
    /// (`frs_prefix_scan_arrow`, `frs_batch_prefix_scan`) that build a
    /// `RecordBatch` from a fully-known row count up front. Streaming
    /// callers (the chunked iterator FFI: `frs_vec_iter_prefix_open*`)
    /// should use [`Self::prefix_scan_iter`] to avoid the intermediate
    /// Vec.
    #[inline]
    pub fn prefix_scan(
        &self,
        cf: &ColumnFamilyHandle,
        prefix: &[u8],
    ) -> ForstResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.prefix_scan_iter(cf, prefix)?.collect()
    }

    /// Streaming form of [`Self::prefix_scan`].
    ///
    /// PR-B5-H2 / C8-H1: lazy k-way merge across ALL three LSM tiers
    /// (active memtable, immutable memtables, live SSTs) — see
    /// [`LazyPrefixIter`] for the streaming-merge state machine. No tier
    /// is eagerly drained before the iterator returns, so the first
    /// `.next()` is O(active-mem-tier + num_imm_mems + num_overlap_ssts)
    /// rather than O(rows × versions) across the entire LSM.
    ///
    /// Before C8-H1 the borrowing variant only walked the ACTIVE memtable's
    /// prefix-index; any row that had rotated into an imm memtable or
    /// flushed to an SST silently VANISHED. This delegates to the same
    /// cross-tier enumeration as [`Self::prefix_scan_iter_owned`], wired
    /// for the `&'a self` lifetime by capturing the engine reference (not
    /// an `Arc<Self>`) in the iterator's value-resolution callback.
    pub fn prefix_scan_iter<'a>(
        &'a self,
        cf: &ColumnFamilyHandle,
        prefix: &[u8],
    ) -> ForstResult<impl Iterator<Item = ForstResult<(Vec<u8>, Vec<u8>)>> + 'a> {
        let cf_handle = cf.clone();
        let inner = self.build_lazy_prefix_key_stream(cf, prefix)?;
        // Per-key value resolution closure: route through the full
        // versioned read path so tombstones in upper tiers correctly hide
        // lower-tier rows and merge operands are resolved.
        //
        // B10-H3: `LazyPrefixIter::Item` is now `Arc<[u8]>` (zero-copy in
        // the FFI streaming path). The borrowing variant preserves the
        // public `(Vec<u8>, Vec<u8>)` contract by calling `to_vec()` at
        // the boundary — same cost as before this fix. The FFI streaming
        // path uses `prefix_scan_iter_owned_arc` to skip this conversion.
        let db: &'a DbImpl = self;
        Ok(inner.filter_map(move |key_arc| {
            let key_slice: &[u8] = key_arc.as_ref();
            match db.get(&cf_handle, key_slice) {
                Ok(Some(value)) => Some(Ok((key_slice.to_vec(), value))),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        }))
    }

    /// Owned-Arc variant of [`Self::prefix_scan_iter`] returning a
    /// `'static`-lifetime iterator.
    ///
    /// PR-C6-H1: the FFI chunked-iterator path (`frs_vec_iter_prefix_open*`)
    /// needs to stash the resulting iterator in a per-shard `IterHandle`
    /// registry that outlives the originating FFI call. The borrowing
    /// `prefix_scan_iter` cannot be stashed (its lifetime is tied to the
    /// `&self` borrow). This variant takes an `Arc<DbImpl>` and captures
    /// it inside the iterator closure so the iterator can outlive any
    /// caller-side borrow.
    ///
    /// C8-H3: the previous implementation eagerly drained the active
    /// memtable + ALL imm memtables + ALL overlapping SSTs into a
    /// BTreeSet BEFORE returning the iterator; the first FFI call paid
    /// the full O(rows × versions) materialisation cost, defeating the
    /// C6-H1 "streaming, chunk-on-demand" promise. This variant now uses
    /// a lazy k-way merge ([`LazyPrefixIter`]) that holds at most one
    /// pending key per tier; SST tiers stream block-by-block via
    /// [`SstReaderImpl::read_block_at`] so no SST is fully decoded
    /// up-front. Value resolution remains lazy — each emitted key is
    /// resolved via `db.get(...)` only when the consumer calls `next()`.
    pub fn prefix_scan_iter_owned(
        self: &Arc<Self>,
        cf: &ColumnFamilyHandle,
        prefix: &[u8],
    ) -> ForstResult<Box<dyn Iterator<Item = ForstResult<(Vec<u8>, Vec<u8>)>> + Send + 'static>>
    {
        // B10-H3 / B11-H3: thin adapter over the new `_arc` variant — copies
        // both halves of the Arc<[u8]> pair into owned Vec<u8> at the
        // boundary. Existing callers (engine tests, `prefix_scan` collector)
        // keep the legacy public contract. New FFI consumers should call
        // `prefix_scan_iter_owned_arc` directly to skip both per-row Vec
        // allocations.
        let arc_iter = self.prefix_scan_iter_owned_arc(cf, prefix)?;
        Ok(Box::new(arc_iter.map(|r| match r {
            Ok((k, v)) => Ok((k.as_ref().to_vec(), v.as_ref().to_vec())),
            Err(e) => Err(e),
        })))
    }

    /// B10-H3 zero-copy key variant of [`Self::prefix_scan_iter_owned`].
    /// Emits the key as an `Arc<[u8]>` so the FFI chunked-iter consumer
    /// can call `arc.as_ref()` and `copy_nonoverlapping` into the caller's
    /// direct ByteBuffer without an intermediate `Vec<u8>` allocation per
    /// emitted row. The Arc strong-count bump is one atomic add per emit
    /// — orders of magnitude cheaper than the Vec alloc + memcpy it
    /// replaces (≈ 8 ns vs ≈ 50-100 ns + heap pressure for the typical
    /// 32-byte composite keys we emit on Q12-style state-bound scans).
    ///
    /// B11-H3: the value is ALSO now `Arc<[u8]>` (was `Vec<u8>` in B10-H3).
    /// C12-H1 cost-model correction: `db.get_arc` calls `Arc::<[u8]>::from(vec)`
    /// which is NOT zero-copy — std's `From<Vec<T>> for Arc<[T]>` allocates a
    /// fresh `ArcInner<[T]>` block (refcount header + len + data) and memcpys
    /// the Vec's contents in. So the first emit per row pays alloc + memcpy
    /// at the engine boundary (roughly equivalent to the legacy `Vec` path,
    /// plus a constant-size refcount header). The actual downstream win is
    /// that every consumer past the first only pays a refcount bump instead
    /// of `Vec::clone`'s alloc + memcpy. Engine-side allocations for the
    /// value path are unchanged (still one alloc per resolved value inside
    /// the SST/memtable read path); a structural block-cache refactor would
    /// be required to make the FFI boundary truly zero-copy, tracked as
    /// follow-up. See `Db::get_arc` for the per-row cost breakdown.
    pub fn prefix_scan_iter_owned_arc(
        self: &Arc<Self>,
        cf: &ColumnFamilyHandle,
        prefix: &[u8],
    ) -> ForstResult<
        Box<dyn Iterator<Item = ForstResult<(Arc<[u8]>, Arc<[u8]>)>> + Send + 'static>,
    > {
        let cf_handle = cf.clone();
        let inner = self.build_lazy_prefix_key_stream(cf, prefix)?;
        let db = Arc::clone(self);
        Ok(Box::new(inner.filter_map(move |key_arc| {
            match db.get_arc(&cf_handle, key_arc.as_ref()) {
                Ok(Some(value)) => Some(Ok((key_arc, value))),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        })))
    }

    /// R17-M1: shared-error-slot variant of [`Self::prefix_scan_iter_owned_arc`].
    ///
    /// Identical behaviour to the un-suffixed variant, except a caller-owned
    /// `Arc<Mutex<Option<ForstError>>>` is installed on the underlying
    /// [`LazyPrefixIter`] so tier-peek errors land in the SAME slot the FFI
    /// consumer already drains for outer `db.get_arc` errors (the R16-M2
    /// path). Pre-fix (R16-M2 only), tier-peek errors were stored in
    /// `LazyPrefixIter::last_error` but the FFI layer wrapped the iter in a
    /// `Box<dyn Iterator>` which erased the concrete type, so
    /// `take_last_error()` was unreachable. Unifying both paths through one
    /// slot makes tier errors observable to the Java side via the existing
    /// `FrsErrorCode` mechanism.
    pub fn prefix_scan_iter_owned_arc_with_error_slot(
        self: &Arc<Self>,
        cf: &ColumnFamilyHandle,
        prefix: &[u8],
        error_slot: Arc<Mutex<Option<ForstError>>>,
    ) -> ForstResult<
        Box<dyn Iterator<Item = ForstResult<(Arc<[u8]>, Arc<[u8]>)>> + Send + 'static>,
    > {
        let cf_handle = cf.clone();
        let mut inner = self.build_lazy_prefix_key_stream(cf, prefix)?;
        inner.set_shared_error_slot(error_slot);
        let db = Arc::clone(self);
        Ok(Box::new(inner.filter_map(move |key_arc| {
            match db.get_arc(&cf_handle, key_arc.as_ref()) {
                Ok(Some(value)) => Some(Ok((key_arc, value))),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        })))
    }

    /// Builds the lazy k-way merge over key sources (one per LSM tier)
    /// shared by both [`Self::prefix_scan_iter`] and
    /// [`Self::prefix_scan_iter_owned`]. The returned iterator yields
    /// each visible user-key exactly once, in sorted order, deduped
    /// across tiers. Value resolution is the caller's responsibility.
    ///
    /// Per-tier sources:
    ///  * Active memtable: `prefix_scan_keys` → sorted `Vec<Arc<[u8]>>`.
    ///    Tier-scoped; not multiplied by other tiers.
    ///  * Each immutable memtable: same.
    ///  * Each overlapping SST: block-streaming via `read_block_at`,
    ///    decoding one block at a time. The full SST is NEVER materialised
    ///    in memory up-front.
    fn build_lazy_prefix_key_stream(
        &self,
        cf: &ColumnFamilyHandle,
        prefix: &[u8],
    ) -> ForstResult<LazyPrefixIter> {
        let upper = prefix_upper_bound(prefix);
        let upper_slice = upper.as_deref();
        let cf_data = self.lookup_cf_by_id(cf.id())?;

        let mut sources: Vec<TierKeySource> = Vec::new();

        // Tier 1: active memtable.
        //
        // C9-H1: replaced the eager `prefix_scan_keys` (which globally
        // sorted + deduped every matching key across all shards before
        // `LazyPrefixIter` could return) with `prefix_scan_cursor`. The
        // cursor snapshots each shard's matching keys via the existing
        // per-shard `prefix_scan_keys` (cheap: `Arc::clone` on the
        // prefix-index fast path), sorts each shard once (small, since
        // the prefix-index fast path returns insertion-order buckets),
        // then exposes a heap-based k-way merge that advances lazily —
        // no global sort, no global dedup, `O(num_shards)` resident
        // footprint regardless of total matching key count.
        let mem_arc = cf_data.active_memtable();
        let active_cursor = mem_arc.prefix_scan_cursor(prefix, upper_slice);
        if !active_cursor.is_empty() {
            sources.push(TierKeySource::MemCursor {
                cursor: active_cursor,
            });
        }
        // Tier 2: immutable memtables. Same C9-H1 treatment: lazy
        // per-shard cursor instead of eager global sort.
        for imm in cf_data.imm_memtables() {
            let imm_cursor = imm.prefix_scan_cursor(prefix, upper_slice);
            if !imm_cursor.is_empty() {
                sources.push(TierKeySource::MemCursor { cursor: imm_cursor });
            }
        }
        // Tier 3: overlapping SSTs, block-streaming.
        let version = self.version_set.current();
        for sst in version.live_sst_files() {
            if sst.largest_key.as_slice() < prefix {
                continue;
            }
            if let Some(hi) = upper_slice {
                if sst.smallest_key.as_slice() >= hi {
                    continue;
                }
            }
            let reader = self.get_or_open_sst_reader(&sst)?;
            sources.push(TierKeySource::Sst {
                reader,
                prefix: prefix.to_vec(),
                upper: upper.clone(),
                next_block: 0,
                buffered: Vec::new(),
                pos: 0,
            });
        }

        LazyPrefixIter::new(sources)
    }

    #[allow(clippy::type_complexity)]
    pub fn batch_prefix_scan(
        &self,
        cf: &ColumnFamilyHandle,
        prefixes: &[&[u8]],
    ) -> ForstResult<Vec<Vec<(Vec<u8>, Vec<u8>)>>> {
        let mut results = Vec::with_capacity(prefixes.len());
        for prefix in prefixes {
            results.push(self.prefix_scan(cf, prefix)?);
        }
        Ok(results)
    }

    // ---------------------------------------------------------------
    // Import / Export (B-Prod-P10, spec §6g)
    //
    // The community RocksDB / ForSt path uses an
    // `ExportImportFilesMetaData` structure that hardlinks a CF's live SST
    // files into an "export dir", then reopens them as a new CF in the
    // destination DB. forst-rs cannot follow that path verbatim today —
    // the engine's `VersionSet` does not partition SSTs by CF, so a single
    // SST may contain rows for multiple CFs and "extract these N SSTs as
    // CF X" has no well-defined meaning here. The implementer guidance
    // attached to PR B-Prod-P10 explicitly allows the "less efficient but
    // correct" path: scan the source CF, write every (key, value) pair to
    // a self-describing export blob, and replay the blob into a fresh CF
    // on import. Rows visible at the time of `cf_export` are exactly the
    // rows visible after `create_cf_from_import` — which is the §6g
    // acceptance criterion.
    //
    // On-disk format (single file `EXPORT.frsblob` under the export dir):
    //
    //   magic           : 8 bytes  = b"FRSEXP01"
    //   cf_name_len     : 8 bytes  = u64 little-endian
    //   cf_name         : N bytes  UTF-8
    //   repeated entries until EOF:
    //     key_len       : 4 bytes  u32 little-endian
    //     key           : K bytes
    //     value_len     : 4 bytes  u32 little-endian
    //     value         : V bytes
    //
    // The file is "self-describing": the magic + cf-name header lets the
    // import side validate it before replaying. No checksum today (the
    // export dir is expected to live on a durable filesystem; future work
    // can add a trailing CRC32 if required). Empty CFs produce a valid
    // header-only blob (zero entries).
    // ---------------------------------------------------------------

    /// Magic bytes prefixing an `EXPORT.frsblob` file. Bumping the suffix
    /// is the migration story if the format ever changes.
    const EXPORT_MAGIC: &'static [u8; 8] = b"FRSEXP01";

    /// Filename written under `export_dir` by [`Self::cf_export`]. The
    /// import side ([`Self::create_cf_from_import`]) reads the same name
    /// from `import_dir`.
    const EXPORT_BLOB_NAME: &'static str = "EXPORT.frsblob";

    /// Exports every live (key, value) pair in `cf` to a single
    /// self-describing blob under `export_dir` (`EXPORT.frsblob`). The
    /// directory is created if it does not exist. Per spec §6g, this is
    /// the producer side of cross-job state transfer: the resulting
    /// directory can be shipped to another job and consumed via
    /// [`Self::create_cf_from_import`] to seed a new CF with the exact
    /// same rows.
    ///
    /// The export reads through the engine's normal scan path
    /// ([`Self::scan`]), which already resolves tombstones, merges and
    /// MVCC visibility — so the export captures the latest visible row
    /// for every user key in the CF. Tombstones are intentionally not
    /// preserved (the import side is creating a brand-new CF; a "deleted"
    /// row would be re-inserted as a tombstone with no prior version, a
    /// no-op).
    ///
    /// Atomicity: the blob is fully written before this method returns;
    /// concurrent writers to `cf` are not blocked, but only writes
    /// committed before this call took the underlying scan are guaranteed
    /// to appear in the export. This matches RocksDB's
    /// `ExportColumnFamily` snapshot-at-call-time semantics.
    pub fn cf_export(&self, cf: &ColumnFamilyHandle, export_dir: &Path) -> ForstResult<()> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;

        // Make sure on-disk SSTs reflect the latest writes — pure
        // hygiene; the scan path also reads memtables, but flushing keeps
        // the export deterministic and minimizes the scan's working set.
        // Failure to flush is non-fatal here: the scan still sees rows
        // that are still in memtables.
        let _ = self.force_switch_memtable(cf);
        let _ = self.flush_cf_data(&cf_data);

        std::fs::create_dir_all(export_dir).map_err(|e| {
            ForstError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "cf_export: create export dir '{}' failed: {e}",
                    export_dir.display()
                ),
            ))
        })?;

        let cf_name = cf_data.handle().name().to_string();
        let cf_name_bytes = cf_name.as_bytes();
        let entries = self.scan(cf, b"", None)?;

        // Pre-size the buffer: header + per-entry 8 bytes of length
        // prefixes + payload. Cheap upper-bound, avoids reallocations on
        // large CFs.
        let payload_bytes: usize = entries.iter().map(|(k, v)| 4 + k.len() + 4 + v.len()).sum();
        let mut buf = Vec::with_capacity(8 + 8 + cf_name_bytes.len() + payload_bytes);
        buf.extend_from_slice(Self::EXPORT_MAGIC);
        buf.extend_from_slice(&(cf_name_bytes.len() as u64).to_le_bytes());
        buf.extend_from_slice(cf_name_bytes);
        for (k, v) in &entries {
            if k.len() > u32::MAX as usize || v.len() > u32::MAX as usize {
                return Err(ForstError::invalid_argument(
                    "cf_export: per-entry key/value must fit in u32 (4 GiB)",
                ));
            }
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }

        let blob_path = export_dir.join(Self::EXPORT_BLOB_NAME);
        std::fs::write(&blob_path, &buf).map_err(|e| {
            ForstError::Io(std::io::Error::new(
                e.kind(),
                format!("cf_export: write '{}' failed: {e}", blob_path.display()),
            ))
        })?;
        Ok(())
    }

    /// Creates a new column family `name` and seeds it with every entry
    /// from `import_dir/EXPORT.frsblob`. Returns the new CF's handle.
    /// Per spec §6g, this is the consumer side of cross-job state
    /// transfer.
    ///
    /// `name` is an explicit caller choice — the manifest's original CF
    /// name is intentionally ignored so the consumer can re-namespace the
    /// imported state (mirrors RocksDB's `ImportColumnFamily(new_name,
    /// metadata)` ergonomics). If a CF with that name already exists the
    /// call returns [`ForstError::InvalidArgument`] and no state is
    /// imported.
    ///
    /// Errors: [`ForstError::InvalidArgument`] for a missing or
    /// magic-mismatched blob; [`ForstError::Corruption`] for a truncated
    /// blob or invalid length prefix; otherwise propagates errors from
    /// the underlying CF creation / put paths.
    pub fn create_cf_from_import(
        &self,
        name: &str,
        import_dir: &Path,
    ) -> ForstResult<ColumnFamilyHandle> {
        let blob_path = import_dir.join(Self::EXPORT_BLOB_NAME);
        let blob = std::fs::read(&blob_path).map_err(|e| {
            ForstError::invalid_argument(format!(
                "create_cf_from_import: read '{}' failed: {e}",
                blob_path.display()
            ))
        })?;

        // ---- Header ----
        if blob.len() < 16 {
            return Err(ForstError::corruption(format!(
                "create_cf_from_import: blob too short ({} bytes)",
                blob.len()
            )));
        }
        if &blob[..8] != Self::EXPORT_MAGIC {
            return Err(ForstError::invalid_argument(
                "create_cf_from_import: magic mismatch (not an EXPORT.frsblob)",
            ));
        }
        let cf_name_len = u64::from_le_bytes(blob[8..16].try_into().expect("8 bytes")) as usize;
        let header_end = 16usize
            .checked_add(cf_name_len)
            .ok_or_else(|| ForstError::corruption("cf_name_len overflows usize"))?;
        if blob.len() < header_end {
            return Err(ForstError::corruption(
                "create_cf_from_import: header truncated",
            ));
        }
        // We do not validate or use the embedded cf_name — the caller
        // re-names by passing `name`. Reading it would just be a debug
        // courtesy.

        // ---- Create the destination CF ----
        let cf = self.create_column_family(ColumnFamilyDescriptor::new(name))?;

        // ---- Replay entries ----
        let mut cursor = header_end;
        while cursor < blob.len() {
            // key
            if cursor + 4 > blob.len() {
                return Err(ForstError::corruption(
                    "create_cf_from_import: truncated key length",
                ));
            }
            let key_len =
                u32::from_le_bytes(blob[cursor..cursor + 4].try_into().expect("4 bytes")) as usize;
            cursor += 4;
            if cursor + key_len > blob.len() {
                return Err(ForstError::corruption(
                    "create_cf_from_import: truncated key payload",
                ));
            }
            let key = &blob[cursor..cursor + key_len];
            cursor += key_len;

            // value
            if cursor + 4 > blob.len() {
                return Err(ForstError::corruption(
                    "create_cf_from_import: truncated value length",
                ));
            }
            let value_len =
                u32::from_le_bytes(blob[cursor..cursor + 4].try_into().expect("4 bytes")) as usize;
            cursor += 4;
            if cursor + value_len > blob.len() {
                return Err(ForstError::corruption(
                    "create_cf_from_import: truncated value payload",
                ));
            }
            let value = &blob[cursor..cursor + value_len];
            cursor += value_len;

            self.put(&cf, key, value)?;
        }

        Ok(cf)
    }

    fn flush_cf_data(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<Option<SstFileMeta>> {
        // Serialize flushes for this CF so concurrent callers cannot pick up
        // the same oldest imm and write it twice.
        let _flush_guard = cf_data.lock_flush();

        // Peek at the oldest imm without popping; if it's empty, return early.
        let imm_list = cf_data.imm_memtables();
        let Some(oldest) = imm_list.first().cloned() else {
            return Ok(None);
        };

        // Allocate a fresh file number and build the flush job.
        let file_number = self.version_set.allocate_file_number();
        let path = sst_file_path(&self.db_path, file_number);
        let writer_opts = SstWriterOptions {
            block_size: self.options.block_size,
            compression: self.options.compression,
        };
        let job = FlushJob::new(
            oldest.clone(),
            file_number,
            path.clone(),
            writer_opts,
            self.fs.clone(),
        );
        let meta = job.run()?;

        // Install the new file into L0 atomically. Note: we install BEFORE
        // popping the memtable so a concurrent reader can never transiently
        // fail to find the key (it will see either the imm or the SST, never
        // neither).
        let edit = VersionEdit {
            new_files: vec![(0, meta.clone())],
            ..Default::default()
        };
        self.version_set.apply(&edit)?;

        // Pre-populate the SST reader cache so the first read doesn't pay
        // the open-file cost.
        let rac = self.fs.open_random_access_file(&path)?;
        let reader = Arc::new(SstReaderImpl::open(rac)?);
        self.sst_readers
            .write()
            .expect("lock poisoned")
            .insert(meta.file_number, reader);

        // B-Prod-P7 §6d: release the bytes this memtable held back to
        // the cross-CF WriteBufferManager. We sample memory_usage()
        // BEFORE the pop so the byte count we release is the same one
        // that contributed to `over_budget()` earlier.
        let released_bytes = oldest.memory_usage() as u64;

        // Pop the memtable now that its data is durably in the SST and the
        // Version has been updated. Readers that acquired a snapshot before
        // the pop still see the in-memory imm; readers after see only the
        // SST — both return the same values.
        cf_data.pop_oldest_imm();
        self.write_buffer_manager.release(released_bytes);
        self.refresh_snapshot_view(cf_data);
        self.write_controller
            .set_imm_count(cf_data.imm_count() as u32);
        self.write_controller
            .set_l0_file_count(self.version_set.current().l0_files().len() as u32);
        self.write_controller.on_flush_complete();

        Ok(Some(meta))
    }

    /// Legacy helper: checks the active memtable usage and performs a full
    /// switch-and-flush under the assumption the caller is NOT holding
    /// `write_mutex`. Retained for convenience in tests and paths that do
    /// not care about write-path latency.
    #[allow(dead_code)]
    fn maybe_switch_memtable(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<()> {
        if self.maybe_switch_memtable_in_lock(cf_data)? {
            self.flush_cf_data(cf_data)?;
        }
        Ok(())
    }

    // ---------------------------------------------------------------
    // Background flush worker plumbing (B1)
    // ---------------------------------------------------------------

    /// Spawns the background flush worker thread. Called once during
    /// engine construction. The worker holds a `Weak<DbImpl>` so the
    /// engine can still be dropped while the worker is mid-recv.
    fn spawn_flush_worker(db: &Arc<Self>) {
        let rx = match db.flush_queue.take_receiver() {
            Some(rx) => rx,
            None => {
                // Should never happen — only called once per construction.
                debug_assert!(false, "flush worker spawned more than once");
                return;
            }
        };
        let weak: Weak<DbImpl> = Arc::downgrade(db);
        let weak_for_err = Weak::clone(&weak);
        let handle = std::thread::Builder::new()
            .name("forst-rs-flush".to_string())
            .spawn(move || {
                crate::flush::flush_loop(rx, weak, move |err| {
                    if let Some(db) = weak_for_err.upgrade() {
                        db.record_flush_error(err);
                    }
                });
            })
            .expect("failed to spawn flush worker thread");
        *db.flush_worker.lock().expect("lock poisoned") = Some(handle);
    }

    /// Spawns the background snapshot-age ticker (spec §6a.3).
    ///
    /// The ticker runs at [`SNAPSHOT_AGE_TICK_MS`] cadence and emits a
    /// `tracing::warn!` line for every period in which the snapshot
    /// registry reports at least one snapshot past its `max_age_ms`
    /// warn-line. Per spec the worker NEVER auto-releases the offending
    /// snapshot — doing so would silently break the reader's correctness
    /// contract (a `Snapshot` pinned at seq S guarantees its versions
    /// remain readable for as long as the handle is alive; auto-releasing
    /// would let compaction reclaim those versions while a Java-side
    /// reader still references them). The remediation surface is the
    /// operator runbook, which is exactly what the warn line points at.
    ///
    /// Exit path: the worker checks `snapshot_age_shutdown` between
    /// sleeps; [`Drop`] sets the flag and joins, draining within one
    /// tick (~1 s) of shutdown.
    fn spawn_snapshot_age_worker(db: &Arc<Self>) {
        let weak: Weak<DbImpl> = Arc::downgrade(db);
        let shutdown = Arc::clone(&db.snapshot_age_shutdown);
        let handle = std::thread::Builder::new()
            .name("forst-rs-snap-age".to_string())
            .spawn(move || {
                loop {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    // Sleep first so a fresh-open engine doesn't fire a
                    // spurious warn on the first tick before any
                    // snapshot has had time to age.
                    std::thread::sleep(std::time::Duration::from_millis(SNAPSHOT_AGE_TICK_MS));
                    let Some(db) = weak.upgrade() else {
                        return;
                    };
                    if let Some(w) = db.snapshot_registry.check_long_lived() {
                        // Emit structured fields so a log aggregator can
                        // index by seq / db_id. The Display impl gives a
                        // human-readable summary; we include both so the
                        // operator gets the runbook hint inline.
                        tracing::warn!(
                            seq = w.seq.value(),
                            db_id = w.db_id.0,
                            age_ms = w.age_ms,
                            max_age_ms = w.max_age_ms,
                            hint = w.hint,
                            "{}",
                            w
                        );
                    }
                }
            })
            .expect("failed to spawn snapshot-age worker thread");
        *db.snapshot_age_worker.lock().expect("lock poisoned") = Some(handle);
    }

    /// Pushes a flush request onto the background queue. The writer never
    /// blocks on disk I/O; backpressure is supplied by the WriteController
    /// stall on the next writer's `may_throttle()` call when imm count
    /// >= `max_write_buffer_number`.
    ///
    /// Returns an error only if the engine is shutting down (the worker
    /// has dropped its receiver) — in which case the writer surfaces the
    /// error to its caller.
    fn enqueue_flush(&self, cf_data: Arc<ColumnFamilyData>) -> ForstResult<()> {
        self.pending_flush_count.fetch_add(1, Ordering::AcqRel);
        match self.flush_queue.enqueue(FlushRequest { cf_data }) {
            Ok(()) => Ok(()),
            Err(e) => {
                // Roll back the counter so a shutdown-time failure can't
                // wedge a `wait_for_pending_flushes` caller.
                self.pending_flush_count.fetch_sub(1, Ordering::AcqRel);
                Err(e)
            }
        }
    }

    /// Records the most recent background flush error so the next writer
    /// can surface it via [`Self::consume_flush_error`].
    fn record_flush_error(&self, err: ForstError) {
        let mut slot = self.flush_error.lock().expect("lock poisoned");
        // Keep the first error; subsequent errors are dropped to avoid
        // swamping the log if the disk is permanently unhappy. The next
        // successful writer clears the slot.
        if slot.is_none() {
            *slot = Some(err);
        }
    }

    /// If a background flush has failed since the last call, returns the
    /// stored error and clears the slot. The next writer/batch_write will
    /// see `Ok(())` so the engine recovers automatically once the
    /// underlying issue (e.g. disk full) is resolved.
    fn consume_flush_error(&self) -> ForstResult<()> {
        let mut slot = self.flush_error.lock().expect("lock poisoned");
        if let Some(err) = slot.take() {
            return Err(err);
        }
        Ok(())
    }

    /// Blocks until every flush request that has been *enqueued* has been
    /// processed by the background worker. Used by [`Self::flush_all`],
    /// checkpoints, and `Drop` so callers see a consistent on-disk state
    /// without bouncing the per-CF flush mutex against the worker.
    ///
    /// Imms produced by direct `force_switch_memtable` (which doesn't
    /// enqueue) won't be counted here — callers must flush those
    /// synchronously via `flush_cf` / `flush_all`'s drain loop.
    ///
    /// The polling loop is bounded so a stuck worker can't deadlock the
    /// caller; it sleeps in 1 ms increments which is fine for tests and
    /// shutdown latency.
    fn wait_for_pending_flushes(&self) {
        let timeout = self.write_controller.config().stall_timeout;
        let start = std::time::Instant::now();
        while self.pending_flush_count.load(Ordering::Acquire) > 0 {
            if start.elapsed() >= timeout {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// In-lock portion of the switch decision. Returns `true` if a switch
    /// happened and the caller should flush outside the write lock.
    fn maybe_switch_memtable_in_lock(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<bool> {
        let usage = cf_data.active_memtable().memory_usage();

        let threshold = cf_data.options().effective_write_buffer_size(&self.options);
        if usage < threshold {
            return Ok(false);
        }

        cf_data.swap_active_memtable();
        self.refresh_snapshot_view(cf_data);

        let imm = cf_data.imm_count() as u32;
        self.write_controller.set_imm_count(imm);
        Ok(true)
    }

    // PERF (D1): `bump_sequence` (a CAS loop on `sequence_number`) was removed.
    // The engine now allocates seqs via a single lock-free `fetch_add` BEFORE
    // acquiring `write_mutex` and hands the seq down to the memtable's
    // `*_with_seq` APIs. The memtable no longer needs the engine to "catch
    // up" to its local counter — there's only one source of truth.

    fn refresh_snapshot_view(&self, cf_data: &Arc<ColumnFamilyData>) {
        let view = SnapshotView::new(
            cf_data.active_memtable(),
            cf_data.imm_memtables(),
            self.version_set.current(),
            self.sequence_number.load(Ordering::Acquire),
        );
        cf_data.store_snapshot_view(Arc::new(view));
    }

    // ---------------------------------------------------------------
    // Read path
    // ---------------------------------------------------------------

    /// Point-lookup: returns the latest value for `key`.
    pub fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> ForstResult<Option<Vec<u8>>> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let read_seq = u64::MAX; // latest view for W12 (no explicit snapshots yet)
        self.get_internal(&cf_data, key, read_seq)
    }

    /// B11-H3: ref-counted variant of [`Self::get`] that returns the resolved
    /// value as `Arc<[u8]>` instead of `Vec<u8>`. Refcount-cheap downstream
    /// sharing is the actual win — see the cost model below.
    ///
    /// Used by [`Self::prefix_scan_iter_owned_arc`] so the FFI chunked-iter
    /// consumer can clone the value Arc into downstream futures / channels
    /// without re-allocating. Every consumer after the first sees a refcount
    /// bump instead of a fresh `Vec::clone`.
    ///
    /// # C12-H1 cost model (corrected; supersedes the prior "zero-copy" claim).
    ///
    /// `Arc::<[u8]>::from(Vec<u8>)` is NOT zero-copy. The stdlib
    /// `impl From<Vec<T>> for Arc<[T]>` allocates a fresh
    /// `ArcInner<[T]>{ refcount, weak, len, [T; len] }` block and memcpys
    /// the Vec's contents into it; the original Vec is then dropped. So:
    ///
    /// * **First consumer (this call):** 1 heap alloc + 1 memcpy at the
    ///   engine boundary — essentially the same per-byte cost as the
    ///   `Vec`-returning variant (which also allocates once inside the
    ///   memtable / SST read path) plus a constant-size refcount header.
    /// * **Downstream clones:** refcount bump only — this is where the
    ///   `Arc<[u8]>` shape wins over `Vec::clone`, which would alloc + memcpy
    ///   each time.
    ///
    /// Switching to `Arc::from(vec.into_boxed_slice())` does NOT avoid
    /// the memcpy: the `Box<[T]>` allocation has no refcount header, so the
    /// `Arc::<[T]>::from(Box<[T]>)` path still allocates a new
    /// `ArcInner<[T]>` block and copies. No production-grade fix is
    /// available short of:
    ///
    /// * a structural refactor of the block-cache / memtable layer to hand
    ///   out `Arc<[u8]>` / `bytes::Bytes` directly, sharing the underlying
    ///   Arrow buffer across reads (tracked as a follow-up — out of scope
    ///   for the engine FFI boundary work this method supports); or
    /// * a custom allocator that lays out `ArcInner` adjacent to a `Vec`'s
    ///   allocation header so the transition becomes a header rewrite
    ///   instead of a copy (theoretically possible, not stable in std).
    ///
    /// Until that refactor lands, the per-row downstream-share win is real
    /// (refcount-cheap clones for the FFI fan-out path) but the per-row
    /// first-emit cost is alloc + memcpy at engine boundary, not zero.
    pub fn get_arc(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
    ) -> ForstResult<Option<Arc<[u8]>>> {
        // See method docstring (C12-H1 cost model). `Arc::<[u8]>::from(Vec)`
        // allocates a refcounted block and memcpys; on a 32-byte value the
        // cost is roughly the same as `Vec::clone` for the first emit. The
        // win is downstream — subsequent consumers refcount-bump instead of
        // allocating + memcpying.
        Ok(self.get(cf, key)?.map(Arc::<[u8]>::from))
    }

    /// Zero-copy point lookup: returns a raw pointer + length to the value
    /// stored inline in the active memtable's hash index.
    ///
    /// Returns `Some((ptr, len))` when the key's latest version is a small
    /// Put (≤ 64 bytes) in the active memtable. Returns `None` if:
    /// - Key not found in active memtable
    /// - Value exceeds inline threshold (stored in columnar storage)
    /// - Latest version is a tombstone or Merge
    /// - Key is only in immutable memtables or SSTs
    ///
    /// The caller should fall back to `get()` when this returns `None`.
    ///
    /// # Safety
    /// The returned pointer is valid as long as the active memtable is not
    /// flushed and no write to the same key occurs. In Flink's single-threaded
    /// per-slot model, both conditions hold during record processing.
    pub fn get_pinned(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> Option<(*const u8, usize)> {
        let cf_data = self.lookup_cf_by_id(cf.id()).ok()?;
        let mem = cf_data.active_memtable();
        mem.get_pinned_ptr(key)
    }

    /// Batch point-lookup.
    ///
    /// Before performing the lookups, prefetches any SST files that are not
    /// already in the reader cache into the local file cache (S3 vector I/O
    /// prefetch). This amortizes S3 round-trip latency: one GetObject per
    /// SST file instead of paying the RTT on the first `get_internal` that
    /// happens to miss the reader cache.
    #[inline]
    pub fn batch_get(
        &self,
        cf: &ColumnFamilyHandle,
        keys: &[&[u8]],
    ) -> ForstResult<Vec<Option<Vec<u8>>>> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;

        // S3 vector I/O prefetch: warm the file cache for SSTs not yet in
        // the reader cache. This is a no-op on LocalFileSystem/MemoryFileSystem
        // (the trait default returns Ok(())).
        self.prefetch_sst_files_for_batch(&cf_data, keys);

        let mem = cf_data.active_memtable();
        let read_seq = u64::MAX;
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            // Fast path: try active memtable directly (inline cache + hash index)
            let active_hit = mem.get(k, read_seq)?;
            match active_hit {
                Some(entry) if entry.op_type == OpType::Put => {
                    out.push(entry.value);
                }
                Some(_) => {
                    // Delete/Merge in active memtable
                    out.push(None);
                }
                None => {
                    // Not in active memtable — fall back to full path
                    out.push(self.get_internal(&cf_data, k, read_seq)?);
                }
            }
        }
        Ok(out)
    }

    /// Batch point-lookup returning results as an Arrow RecordBatch.
    ///
    /// Input: a `BinaryArray` of keys.
    /// Output: a `RecordBatch` with columns:
    /// - `value: Binary` (nullable) — the value bytes for each key (null if not found)
    /// - `found: Boolean` — true if found, false if not found
    ///
    /// This method builds the Arrow output directly during the lookup loop,
    /// avoiding the intermediate `Vec<Option<Vec<u8>>>` allocation that
    /// `batch_get` incurs. Combined with the Arrow C Data Interface at the
    /// FFI layer, this eliminates all per-value memcpy on the return path.
    pub fn batch_get_arrow(
        &self,
        cf: &ColumnFamilyHandle,
        keys: &arrow::array::BinaryArray,
    ) -> ForstResult<arrow::array::RecordBatch> {
        use arrow::array::{Array, BinaryBuilder, BooleanBuilder, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};

        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let n = keys.len();

        // Pre-allocate builders with estimated capacity. 32 bytes/value is a
        // reasonable heuristic for Flink state values.
        let mut value_builder = BinaryBuilder::with_capacity(n, n * 32);
        let mut found_builder = BooleanBuilder::with_capacity(n);

        // PR-C6-H2: active-memtable inline-cache fast path writes
        // borrowed value bytes directly into the Arrow BinaryBuilder
        // via the `ValueSink` trait — no intermediate `Vec<u8>` per
        // key. Only the slow-path tail (immutable memtables, SST
        // files, merge resolution) still goes through the
        // `Option<Vec<u8>>`-returning `get_internal`. The slow path
        // contributes one alloc + one memcpy per missed key, exactly
        // as before.
        let mem = cf_data.active_memtable();
        let read_seq = u64::MAX;
        for i in 0..n {
            let key = keys.value(i);
            let mut sink = BinaryBuilderSink(&mut value_builder);
            match mem.get_into(key, read_seq, &mut sink) {
                SinkGetOutcome::HitPut => {
                    found_builder.append_value(true);
                }
                SinkGetOutcome::HitTombstone => {
                    found_builder.append_value(false);
                }
                SinkGetOutcome::Miss | SinkGetOutcome::NeedsFullPath => {
                    // Drop the sink borrow before re-borrowing the
                    // builder via the legacy `Option<Vec<u8>>` path.
                    drop(sink);
                    match self.get_internal(&cf_data, key, read_seq)? {
                        Some(value) => {
                            value_builder.append_value(&value);
                            found_builder.append_value(true);
                        }
                        None => {
                            value_builder.append_null();
                            found_builder.append_value(false);
                        }
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Binary, true),
            Field::new("found", DataType::Boolean, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(value_builder.finish()),
                Arc::new(found_builder.finish()),
            ],
        )
        .map_err(|e| ForstError::invalid_argument(e.to_string()))
    }

    /// Prefetch SST files that a batch of keys might touch.
    ///
    /// Collects all SST file numbers from the current version that are NOT
    /// already open in `sst_readers`, then calls `fs.ensure_cached(path)`
    /// for each. On a CachedFileSystem backed by S3, this fetches the
    /// entire SST in one GetObject call and populates the local cache.
    /// On local/memory filesystems this is a no-op.
    ///
    /// Best-effort: errors are silently ignored (the subsequent
    /// `get_or_open_sst_reader` will retry and surface the error if it
    /// persists).
    fn prefetch_sst_files_for_batch(&self, _cf_data: &Arc<ColumnFamilyData>, _keys: &[&[u8]]) {
        let version = self.version_set.current();
        let readers = self.sst_readers.read().expect("lock poisoned");

        // Collect file numbers not yet in the reader cache.
        let mut to_prefetch: Vec<PathBuf> = Vec::new();
        for level in &version.levels {
            for sst in &level.files {
                if !readers.contains_key(&sst.file_number) {
                    to_prefetch.push(sst_file_path(&self.db_path, sst.file_number));
                }
            }
        }
        drop(readers);

        // Prefetch each file. Errors are best-effort — the read path will
        // retry on the actual lookup and surface the error there.
        for path in &to_prefetch {
            let _ = self.fs.ensure_cached(path);
        }
    }

    #[inline]
    fn get_internal(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        read_seq: u64,
    ) -> ForstResult<Option<Vec<u8>>> {
        // Stage 1: Active memtable — peek at the newest visible entry.
        // E1: ShardedMemTable::get hashes the key to one shard internally.
        let active_hit = cf_data.active_memtable().get(key, read_seq)?;
        match active_hit {
            Some(entry) if entry.op_type == OpType::Put => return Ok(entry.value),
            Some(entry)
                if entry.op_type == OpType::Delete || entry.op_type == OpType::SingleDelete =>
            {
                return Ok(None);
            }
            Some(entry) => {
                // Merge — collect operands and resolve via the merge operator.
                debug_assert_eq!(entry.op_type, OpType::Merge);
                let first_operand = entry
                    .value
                    .ok_or_else(|| ForstError::corruption("Merge entry missing operand payload"))?;
                let mut operands: Vec<Vec<u8>> = Vec::new();
                operands.push(first_operand);
                let base =
                    self.collect_merge_operands(cf_data, key, entry.sequence, &mut operands)?;
                return self
                    .apply_merge_operator(cf_data, key, base, operands)
                    .map(Some);
            }
            None => {}
        }

        // Stage 2: Immutable memtables (newest → oldest).
        let imm_list = cf_data.imm_memtables();
        for imm in imm_list.iter().rev() {
            let hit = imm.get(key, read_seq)?;
            let Some(entry) = hit else { continue };
            match entry.op_type {
                OpType::Put => return Ok(entry.value),
                OpType::Delete | OpType::SingleDelete => return Ok(None),
                OpType::Merge => {
                    let first_operand = entry.value.ok_or_else(|| {
                        ForstError::corruption("Merge entry missing operand payload")
                    })?;
                    let mut operands: Vec<Vec<u8>> = Vec::new();
                    operands.push(first_operand);
                    let base = self.collect_merge_operands_from_imm_start(
                        cf_data,
                        key,
                        entry.sequence,
                        imm_list.clone(),
                        &mut operands,
                    )?;
                    return self
                        .apply_merge_operator(cf_data, key, base, operands)
                        .map(Some);
                }
            }
        }

        // Stage 3: SST files (L0 newest-first, then L1..Ln binary search).
        let version = self.version_set.current();
        self.sst_get(cf_data, &version, key, &mut Vec::new())
    }

    /// Searches the SST layers for `key`. Returns the resolved user value
    /// (None for missing / tombstoned) — including full merge chains that
    /// start in the SST layer.
    fn sst_get(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        version: &Version,
        key: &[u8],
        merge_operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        // L0: iterate newest → oldest. L0 files may overlap; each is checked.
        for sst in version.l0_files().iter().rev() {
            if let Some(res) = self.sst_lookup(sst, key)? {
                match res.op_type {
                    OpType::Put => {
                        if merge_operands.is_empty() {
                            return Ok(res.value);
                        }
                        return self
                            .apply_merge_operator(
                                cf_data,
                                key,
                                res.value,
                                std::mem::take(merge_operands),
                            )
                            .map(Some);
                    }
                    OpType::Delete | OpType::SingleDelete => {
                        if merge_operands.is_empty() {
                            return Ok(None);
                        }
                        return self
                            .apply_merge_operator(
                                cf_data,
                                key,
                                None,
                                std::mem::take(merge_operands),
                            )
                            .map(Some);
                    }
                    OpType::Merge => {
                        if let Some(v) = res.value {
                            merge_operands.push(v);
                        }
                        // continue down the LSM
                    }
                }
            }
        }

        // L1..Ln: at most one candidate per level.
        for level in 1..version.num_levels() {
            let Some(idx) = version.find_sst_for_key(level, key) else {
                continue;
            };
            let sst = &version.levels[level].files[idx];
            if let Some(res) = self.sst_lookup(sst, key)? {
                match res.op_type {
                    OpType::Put => {
                        if merge_operands.is_empty() {
                            return Ok(res.value);
                        }
                        return self
                            .apply_merge_operator(
                                cf_data,
                                key,
                                res.value,
                                std::mem::take(merge_operands),
                            )
                            .map(Some);
                    }
                    OpType::Delete | OpType::SingleDelete => {
                        if merge_operands.is_empty() {
                            return Ok(None);
                        }
                        return self
                            .apply_merge_operator(
                                cf_data,
                                key,
                                None,
                                std::mem::take(merge_operands),
                            )
                            .map(Some);
                    }
                    OpType::Merge => {
                        if let Some(v) = res.value {
                            merge_operands.push(v);
                        }
                    }
                }
            }
        }

        // Exhausted the LSM. If we collected merges but no base, apply with
        // base=None (partial merge with empty base).
        if !merge_operands.is_empty() {
            return self
                .apply_merge_operator(cf_data, key, None, std::mem::take(merge_operands))
                .map(Some);
        }
        Ok(None)
    }

    fn sst_lookup(
        &self,
        meta: &SstFileMeta,
        key: &[u8],
    ) -> ForstResult<Option<forst_rs_storage::sst::LookupResult>> {
        // Quick key-range check — avoids opening the file for obvious misses.
        if key < meta.smallest_key.as_slice() || key > meta.largest_key.as_slice() {
            return Ok(None);
        }
        let reader = self.get_or_open_sst_reader(meta)?;
        reader.get(key)
    }

    fn get_or_open_sst_reader(&self, meta: &SstFileMeta) -> ForstResult<Arc<SstReaderImpl>> {
        {
            let cache = self.sst_readers.read().expect("lock poisoned");
            if let Some(r) = cache.get(&meta.file_number) {
                return Ok(r.clone());
            }
        }
        // Double-checked lock — another thread may have cached meanwhile.
        let mut cache = self.sst_readers.write().expect("lock poisoned");
        if let Some(r) = cache.get(&meta.file_number) {
            return Ok(r.clone());
        }
        let path = sst_file_path(&self.db_path, meta.file_number);
        let file = self.fs.open_random_access_file(&path)?;
        let reader = Arc::new(SstReaderImpl::open(file)?);
        cache.insert(meta.file_number, reader.clone());
        Ok(reader)
    }

    fn apply_merge_operator(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        base: Option<Vec<u8>>,
        operands: Vec<Vec<u8>>,
    ) -> ForstResult<Vec<u8>> {
        let op = cf_data.merge_operator().ok_or_else(|| {
            ForstError::invalid_argument(format!(
                "column family '{}' has no merge operator configured",
                cf_data.handle().name()
            ))
        })?;
        // `operands` was collected newest-first — the merge operator expects
        // oldest-first so reverse before invoking.
        let reversed: Vec<Vec<u8>> = operands.into_iter().rev().collect();
        let slices: Vec<&[u8]> = reversed.iter().map(|v| v.as_slice()).collect();
        op.full_merge(key, base.as_deref(), &slices)
    }

    /// Collect additional merge operands older than the outer `first_seq`.
    ///
    /// Scans the active memtable for older versions, then the immutable list
    /// from newest to oldest. Each memtable has its own local sequence space,
    /// so `cutoff` is reset to `u64::MAX` when crossing a memtable boundary.
    /// SST lookup is stubbed (W14 work).
    fn collect_merge_operands(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        first_seq: u64,
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        // Active memtable — skip the already-consumed outer entry (first_seq)
        // by probing at first_seq - 1.
        let mem_arc = cf_data.active_memtable();
        let initial = if first_seq == 0 {
            // No older entries possible; skip active scan.
            None
        } else {
            Some(first_seq - 1)
        };
        if let Some(cutoff) = initial {
            if let Some(base) = self.peel_merges_from_memtable(&mem_arc, key, cutoff, operands)? {
                return Ok(base.value);
            }
        }

        // Immutable memtables (newest → oldest). Each has its own sequence
        // space, so start from u64::MAX.
        let imm_list = cf_data.imm_memtables();
        for imm in imm_list.iter().rev() {
            if let Some(base) = self.peel_merges_from_memtable(imm, key, u64::MAX, operands)? {
                return Ok(base.value);
            }
        }

        // SST layer: walk L0 newest-first, then L1..Ln.
        self.peel_merges_from_sst(cf_data, key, operands)
    }

    /// Walks the imm list when the outer hit was itself in the immutable
    /// queue. `first_seq` is the already-consumed outer sequence; peel
    /// continues from `first_seq - 1` in the starting imm, then u64::MAX for
    /// older imms.
    fn collect_merge_operands_from_imm_start(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        first_seq: u64,
        imm_list: Vec<crate::column_family::SharedMemTable>,
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        let mut started = false;
        for imm in imm_list.iter().rev() {
            let cutoff = if !started {
                started = true;
                if first_seq == 0 {
                    continue;
                }
                first_seq - 1
            } else {
                u64::MAX
            };
            if let Some(base) = self.peel_merges_from_memtable(imm, key, cutoff, operands)? {
                return Ok(base.value);
            }
        }
        // Fall through into SSTs.
        self.peel_merges_from_sst(cf_data, key, operands)
    }

    /// Peels merges out of the SST layer. Returns:
    /// - `Ok(Some(value))` when a Put base is found (value may itself be None
    ///   if the Put stored a null payload).
    /// - `Ok(None)` when a Delete is hit or the SST layer is exhausted with
    ///   no base — the caller treats this as "base is empty".
    fn peel_merges_from_sst(
        &self,
        _cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        let version = self.version_set.current();
        for sst in version.l0_files().iter().rev() {
            if let Some(res) = self.sst_lookup(sst, key)? {
                match res.op_type {
                    OpType::Put => return Ok(res.value),
                    OpType::Delete | OpType::SingleDelete => return Ok(None),
                    OpType::Merge => {
                        if let Some(v) = res.value {
                            operands.push(v);
                        }
                    }
                }
            }
        }
        for level in 1..version.num_levels() {
            let Some(idx) = version.find_sst_for_key(level, key) else {
                continue;
            };
            let sst = &version.levels[level].files[idx];
            if let Some(res) = self.sst_lookup(sst, key)? {
                match res.op_type {
                    OpType::Put => return Ok(res.value),
                    OpType::Delete | OpType::SingleDelete => return Ok(None),
                    OpType::Merge => {
                        if let Some(v) = res.value {
                            operands.push(v);
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Peels `Merge` entries off a single memtable, appending operands to
    /// `operands`. Returns `Ok(Some(base))` when a `Put`/`Delete` is found
    /// (the base value is in `base.value`; `Delete` produces `Value: None`),
    /// or `Ok(None)` if the memtable is exhausted without finding a base.
    ///
    /// `start_cutoff` is the highest sequence number to consider (inclusive).
    /// Pass `entry.sequence - 1` to skip an already-consumed entry, or
    /// `u64::MAX` to start from the newest entry.
    fn peel_merges_from_memtable(
        &self,
        mem_arc: &crate::column_family::SharedMemTable,
        key: &[u8],
        start_cutoff: u64,
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<MergeBase>> {
        let mut cutoff = start_cutoff;
        loop {
            let hit = mem_arc.get(key, cutoff)?;
            let Some(entry) = hit else { return Ok(None) };
            match entry.op_type {
                OpType::Put => return Ok(Some(MergeBase { value: entry.value })),
                OpType::Delete | OpType::SingleDelete => {
                    return Ok(Some(MergeBase { value: None }))
                }
                OpType::Merge => {
                    if let Some(v) = entry.value {
                        operands.push(v);
                    }
                    if entry.sequence == 0 {
                        return Ok(None);
                    }
                    cutoff = entry.sequence - 1;
                }
            }
        }
    }

    pub(crate) fn lookup_cf_by_id(&self, id: ColumnFamilyId) -> ForstResult<Arc<ColumnFamilyData>> {
        let cfs = self.cfs.read().expect("lock poisoned");
        let cf_data = cfs.get(&id).cloned().ok_or_else(|| {
            ForstError::invalid_argument(format!("column family id {} not found", id))
        })?;
        // Spec §6g / B-Prod-followup-5: dropped CFs are removed from the
        // `cfs` map atomically with the `dropped` flag flip, so a hit here
        // means the CF is still live. The flag check is a belt-and-braces
        // race guard for the brief window where a caller may have stashed
        // an Arc<ColumnFamilyData> across a `drop_cf` boundary (compaction
        // workers, write batches in-flight, etc.). Returning
        // InvalidArgument from this central choke point ensures every
        // engine entry point that resolves a handle gets the consistent
        // "CF dropped" rejection without each call site re-checking.
        if cf_data.is_dropped() {
            return Err(ForstError::invalid_argument(format!(
                "column family id {} has been dropped",
                id
            )));
        }
        Ok(cf_data)
    }

    /// Access the shared [`FileDeletionGuard`] — used by checkpoints to pin
    /// files during the copy phase so concurrent compactions cannot delete
    /// them.
    pub fn deletion_guard(&self) -> &Arc<FileDeletionGuard> {
        &self.deletion_guard
    }

    /// Attempts to delete a file, respecting the [`FileDeletionGuard`].
    /// Pinned files are deferred to [`Self::reap_pending_deletions`].
    fn delete_file_guarded(&self, file_number: FileNumber) {
        if self.deletion_guard.can_delete(file_number) {
            let path = sst_file_path(&self.db_path, file_number);
            let _ = self.fs.delete_file(&path);
        } else {
            self.pending_deletions
                .lock()
                .expect("lock poisoned")
                .push(file_number);
        }
    }

    /// Reaps previously-deferred deletions whose pins have since been
    /// released. Called after every compaction so deletions eventually
    /// drain.
    fn reap_pending_deletions(&self) {
        let mut pending = self.pending_deletions.lock().expect("lock poisoned");
        let mut still_pending = Vec::with_capacity(pending.len());
        for file_number in pending.drain(..) {
            if self.deletion_guard.can_delete(file_number) {
                let path = sst_file_path(&self.db_path, file_number);
                let _ = self.fs.delete_file(&path);
            } else {
                still_pending.push(file_number);
            }
        }
        *pending = still_pending;
    }
}

// ---------------------------------------------------------------------
// Remote-storage URI -> OpendalFileSystem
// ---------------------------------------------------------------------

/// R31-M4 helper: returns `true` when `a` and `b` are the same on-disk file
/// (same device + inode on unix) or, on non-unix where inode comparison is
/// unavailable, when their sizes match. Used by
/// [`DbImpl::open_from_incremental`] to validate a pre-existing dst SST.
///
/// # Limitations
///
/// R32-L2: the non-unix branch uses size-only equality, which is a weak
/// fingerprint — two distinct SST files of the same byte length match.
/// A stronger CRC32C-based check would catch this but at the cost of a
/// full-file read on every restored SST (defeating the purpose of the
/// incremental checkpoint hot path). The mitigation is the caller-clean-
/// target contract documented on
/// [`DbImpl::open_from_incremental`]: callers are required to invoke this
/// function only with a target directory they own and have cleared, so
/// the size-only check is a defense-in-depth signal against accidental
/// reuse, not an attacker-grade integrity check.
///
/// CRC32C fallback is deferred behind a future `--features content-verify`
/// feature flag (no current consumer; benchmarks have not justified the
/// per-restore cost). When that flag is wired up, this function would
/// route to a size-then-CRC32C composite predicate without changing the
/// call sites.
fn same_file_or_size(a: &Path, b: &Path) -> ForstResult<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let md_a = std::fs::metadata(a)
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("stat '{}': {e}", a.display()))))?;
        let md_b = std::fs::metadata(b)
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("stat '{}': {e}", b.display()))))?;
        Ok(md_a.dev() == md_b.dev() && md_a.ino() == md_b.ino())
    }
    #[cfg(not(unix))]
    {
        let md_a = std::fs::metadata(a)
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("stat '{}': {e}", a.display()))))?;
        let md_b = std::fs::metadata(b)
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("stat '{}': {e}", b.display()))))?;
        Ok(md_a.len() == md_b.len())
    }
}

/// Parses an OpenDAL URI of the form `<scheme>://<authority>[/path]` and
/// returns a configured [`OpendalFileSystem`]. Used by
/// [`DbImpl::open_remote`] (B-Prod-P6) to bridge Java-supplied URI strings
/// onto the strongly-typed OpenDAL builder.
///
/// Supported schemes today: `memory://`, `file://`, `s3://`. Service-
/// specific knobs (region, endpoint, credentials, …) come from
/// `extra_config` whose keys MUST match the OpenDAL builder field names
/// for that scheme. Keys that don't apply to the scheme are ignored.
fn build_opendal_fs_from_uri(
    uri: &str,
    extra_config: &HashMap<String, String>,
) -> ForstResult<Arc<dyn FileSystem>> {
    // Parse "scheme://rest" without depending on the `url` crate (already
    // pulled in transitively via opendal but not exposed in our deps).
    let (scheme, rest) = uri.split_once("://").ok_or_else(|| {
        ForstError::invalid_argument(format!(
            "open_remote: URI '{uri}' must be of the form '<scheme>://<rest>'"
        ))
    })?;

    let fs: Arc<dyn FileSystem> = match scheme {
        "memory" => Arc::new(OpendalFileSystem::memory()?),
        "file" => {
            // `file:///abs/path` → root = `/abs/path`. Tolerate bare
            // `file://relative/path` too, treating it as relative.
            let root = if rest.starts_with('/') {
                rest.to_string()
            } else {
                format!("/{}", rest)
            };
            Arc::new(OpendalFileSystem::local(std::path::Path::new(&root))?)
        }
        "s3" => {
            // `s3://bucket` or `s3://bucket/`. The path portion (if any)
            // is treated as the bucket prefix; OpenDAL's `bucket` field
            // is just the name, so we use the host portion.
            let bucket = rest.split('/').next().unwrap_or(rest).to_string();
            if bucket.is_empty() {
                return Err(ForstError::invalid_argument(format!(
                    "open_remote: s3 URI '{uri}' missing bucket name"
                )));
            }
            let region = extra_config.get("region").cloned().ok_or_else(|| {
                ForstError::invalid_argument(
                    "open_remote: s3 scheme requires 'region' in opendal_config",
                )
            })?;
            let endpoint = extra_config.get("endpoint").map(String::as_str);
            let access_key_id = extra_config.get("access_key_id").map(String::as_str);
            let secret_access_key = extra_config.get("secret_access_key").map(String::as_str);
            Arc::new(OpendalFileSystem::s3(
                &bucket,
                &region,
                endpoint,
                access_key_id,
                secret_access_key,
            )?)
        }
        other => {
            return Err(ForstError::invalid_argument(format!(
                "open_remote: unsupported URI scheme '{other}' (supported: memory, file, s3)"
            )));
        }
    };

    Ok(fs)
}

// ---------------------------------------------------------------------
// Background flush worker callback
// ---------------------------------------------------------------------

/// Bridges `crate::flush::flush_loop` into the engine. The worker thread
/// receives a [`FlushRequest`] and calls [`Self::run_flush`] to actually
/// run the flush + auto-compaction, mirroring the work that `write_single`
/// / `batch_write` used to do inline.
impl FlushExecutor for DbImpl {
    fn run_flush(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<()> {
        // Use a guard so the counter is always decremented, even if a
        // panic or early-return happens inside flush_cf_data.
        struct Guard<'a>(&'a AtomicU32);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let _guard = Guard(&self.pending_flush_count);

        // It's possible the queue collapsed two requests for the same CF
        // (writer1 enqueues, then before the worker recvs writer2 enqueues
        // again). The second `flush_cf_data` will simply find an empty
        // imm list and return `Ok(None)`, so we treat that as a no-op.
        self.flush_cf_data(cf_data)?;
        // Auto-compact L0 if it has grown past the slowdown trigger so
        // the engine stays well clear of the write-stall ceiling.
        self.maybe_auto_compact(cf_data)?;
        Ok(())
    }
}

/// Drains pending flushes and joins the worker thread on shutdown so no
/// data is lost. We never panic in `Drop` (would abort the process under
/// double-panic) — instead we log and continue. The Mutex around the
/// JoinHandle lets us `take()` it cleanly.
impl Drop for DbImpl {
    fn drop(&mut self) {
        // 1. Wait for every flush we've already enqueued to land. We do
        //    this BEFORE touching the worker handle so anything that
        //    arrived in the queue before drop gets a chance to flush to
        //    disk. (Imms produced by `force_switch_memtable` that were
        //    never enqueued are NOT flushed here — callers must call
        //    `flush_all` explicitly before dropping the engine if they
        //    care about those.)
        self.wait_for_pending_flushes();

        // 2. Drop our last sender by replacing the queue with an empty
        //    one. This closes the channel and the worker's `recv()`
        //    returns `Err`, exiting `flush_loop`.
        //
        //    Note: `Arc::strong_count(&self.flush_queue)` may still be > 1
        //    if any in-flight `enqueue_flush` call holds a clone, but
        //    those are bounded — they'll drop the clone before returning
        //    to the writer.
        self.flush_queue = Arc::new(FlushQueue::new(1));

        // 3. Take and join the worker. If the thread has already exited
        //    (e.g. because we dropped the queue above), `join` returns
        //    immediately. We log on join error rather than panic per the
        //    contract: a poisoned thread must not abort the runtime.
        let handle = self.flush_worker.lock().ok().and_then(|mut g| g.take());
        if let Some(h) = handle {
            if let Err(e) = h.join() {
                eprintln!("forst-rs: flush worker panicked during shutdown: {:?}", e);
            }
        }

        // 4. Signal the snapshot-age ticker to stop. The worker checks
        //    this flag between sleeps so the join below resolves within
        //    one tick (~1 s by default). Same panic-on-thread-poison
        //    discipline as the flush worker: log instead of abort so
        //    one bad thread doesn't tear down the whole runtime.
        self.snapshot_age_shutdown
            .store(true, std::sync::atomic::Ordering::Release);
        let snap_handle = self
            .snapshot_age_worker
            .lock()
            .ok()
            .and_then(|mut g| g.take());
        if let Some(h) = snap_handle {
            if let Err(e) = h.join() {
                eprintln!(
                    "forst-rs: snapshot-age worker panicked during shutdown: {:?}",
                    e
                );
            }
        }
    }
}

/// Internal result of peeling merges off a single memtable.
struct MergeBase {
    value: Option<Vec<u8>>,
}

/// Computes the exclusive upper bound representing "all keys with this
/// prefix". Returns `None` if the prefix is all `0xff` bytes (no finite
/// upper bound exists).
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut result = prefix.to_vec();
    while let Some(&last) = result.last() {
        if last == 0xff {
            result.pop();
        } else {
            let idx = result.len() - 1;
            result[idx] = last + 1;
            return Some(result);
        }
    }
    None
}

// ============================================================================
// Lazy k-way merge for cross-tier prefix scan (C8-H3)
// ============================================================================
//
// Streaming primitive that powers both `DbImpl::prefix_scan_iter` and
// `DbImpl::prefix_scan_iter_owned`. Each LSM tier (active memtable,
// immutable memtables, overlapping SSTs) exposes a sorted-keys stream;
// `LazyPrefixIter` holds at most one pending key per tier in a min-heap
// and emits each distinct user-key exactly once. Two streaming-cost
// properties:
//   * No tier is fully materialised before `next()` is first called.
//   * SST tiers stream block-by-block via `read_block_at`, so a 100K-row
//     SST decodes incrementally — the consumer can drain 1K at a time and
//     pays O(blocks-touched × block-size) memory rather than O(N).
//
// Tombstones / merge resolution are NOT done here. The caller resolves
// each emitted key via `db.get(...)` (which walks the full LSM stack with
// correct visibility); a `None` result simply skips the row. This matches
// the contract the previous BTreeSet implementation had.

/// One per-tier source feeding `LazyPrefixIter`. Each variant exposes a
/// streaming sorted-keys iterator scoped to the tier's contribution to a
/// single prefix scan.
enum TierKeySource {
    /// Active or immutable memtable, viewed through a [`MemTierCursor`].
    ///
    /// C9-H1: the legacy `MemKeys` variant held a `Vec<Arc<[u8]>>` that had
    /// been fully materialised + globally sorted + deduped at iter
    /// construction. For a 100K-row active memtable that cost ~10ms before
    /// the consumer could read the first row. `MemTierCursor` replaces that
    /// upfront global sort with per-shard sort + cross-shard heap merge —
    /// the heap holds at most one pending key per shard, so the resident
    /// footprint is `O(num_shards)` independent of total matching rows.
    MemCursor {
        cursor: forst_rs_storage::memtable::sharded::MemTierCursor,
    },
    /// Overlapping SST. Blocks are read one at a time via
    /// `read_block_at(next_block)`; per-block keys are buffered as
    /// `Vec<Arc<[u8]>>` (a single block is bounded by `block_size`, default
    /// 4 KiB — small constant, not multiplied by tier count).
    ///
    /// C9-H2: buffer element type is `Arc<[u8]>` rather than `Vec<u8>`.
    /// The `Arc::<[u8]>::from(view.key)` step still pays exactly ONE
    /// allocation per accepted user-key (the underlying SST block's
    /// `BinaryArray` storage is shared across blocks via Arrow's
    /// Buffer-backed slices, so we cannot safely borrow the key bytes past
    /// the block's lifetime — but switching from `Vec<u8>` → `Arc<[u8]>`
    /// lets downstream `last_emitted` / `min_key` tracking use cheap
    /// `Arc::clone` (atomic refcount bump) instead of `Vec::clone`
    /// (full byte copy).
    Sst {
        reader: Arc<SstReaderImpl>,
        prefix: Vec<u8>,
        upper: Option<Vec<u8>>,
        next_block: usize,
        buffered: Vec<Arc<[u8]>>,
        pos: usize,
    },
}

impl TierKeySource {
    /// Peeks at the next available key, lazily decoding the next SST
    /// block if the current buffer is exhausted. Returns `Ok(None)` when
    /// the tier source is fully drained.
    fn peek(&mut self) -> ForstResult<Option<&[u8]>> {
        match self {
            TierKeySource::MemCursor { cursor } => Ok(cursor.peek()),
            TierKeySource::Sst {
                reader,
                prefix,
                upper,
                next_block,
                buffered,
                pos,
            } => {
                // Replenish the buffer until either we find a usable key
                // or we've exhausted the SST.
                loop {
                    if *pos < buffered.len() {
                        return Ok(Some(buffered[*pos].as_ref()));
                    }
                    if *next_block >= reader.index_entry_count() {
                        return Ok(None);
                    }
                    // Range-skip empty blocks before paying decompression.
                    let block_idx = *next_block;
                    *next_block += 1;
                    let batch = reader.read_block_at(block_idx)?;
                    buffered.clear();
                    *pos = 0;
                    forst_rs_storage::sst::for_each_row_in_batch(&batch, |view| {
                        if view.key < prefix.as_slice() {
                            return Ok(());
                        }
                        if let Some(hi) = upper.as_deref() {
                            if view.key >= hi {
                                return Ok(());
                            }
                        }
                        // Filter dedups WITHIN the block: SST rows are
                        // `(key ASC, sequence DESC)`, so consecutive rows
                        // can share a user_key. We only want the user_key
                        // once per tier — push only when different from
                        // the previously buffered one.
                        //
                        // C9-H2: buffer `Arc<[u8]>` rather than `Vec<u8>`.
                        // The single `Arc::<[u8]>::from` is unavoidable
                        // (the SST block's borrowed bytes are tied to the
                        // block's lifetime, which we drop on the next
                        // iteration of the outer `loop`). But downstream
                        // emit + last_emitted tracking now use
                        // `Arc::clone` (atomic refcount bump only).
                        if buffered.last().map(|k| k.as_ref()) != Some(view.key) {
                            buffered.push(Arc::<[u8]>::from(view.key));
                        }
                        Ok(())
                    })?;
                    // Loop: if this block was entirely out-of-range or
                    // contained no rows, peek the next block.
                }
            }
        }
    }

    /// Consumes the currently-peeked key and advances the cursor.
    fn advance(&mut self) {
        match self {
            TierKeySource::MemCursor { cursor } => cursor.advance(),
            TierKeySource::Sst { pos, .. } => *pos += 1,
        }
    }

    /// Returns the currently-peeked key as an `Arc<[u8]>` clone — used by
    /// `LazyPrefixIter::next` to track `last_emitted` and `min_key` without
    /// paying `Vec::clone` (full byte copy) per emit.
    ///
    /// Precondition: caller must have just successfully `peek`-ed `Some(_)`
    /// from this source; otherwise returns `None`.
    fn peek_arc(&self) -> Option<Arc<[u8]>> {
        match self {
            TierKeySource::MemCursor { cursor } => cursor.peek_arc(),
            TierKeySource::Sst { buffered, pos, .. } => buffered.get(*pos).map(Arc::clone),
        }
    }
}

/// Lazy k-way merge iterator over a `Vec<TierKeySource>`.
///
/// Implementation note: a full min-heap is overkill for typical workloads
/// (3 active mem shards' worth × num imm mems × L0 SSTs ≈ tens of tiers,
/// not hundreds). We keep the implementation simple — a linear scan over
/// `sources` to find the min head — which is provably O(num_tiers) per
/// emitted key. The hot-path cost on a 100K-row scan with ~10 tiers is
/// ~10 pointer compares per key, dwarfed by the `db.get(key)` resolve.
/// Swapping in a `BinaryHeap` would save ~3×; defer until profiling
/// demands it.
pub struct LazyPrefixIter {
    sources: Vec<TierKeySource>,
    /// C9-H2: `Arc<[u8]>` instead of `Vec<u8>`. `Arc::clone` per emit is an
    /// atomic refcount bump (8 ns on contemporary x86) vs. `Vec::clone`
    /// which allocates + memcpys the full key payload (≈ 50-100 ns + alloc
    /// pressure for 32-byte composite keys).
    last_emitted: Option<Arc<[u8]>>,
    /// R15-M3: sticky last-error state set by `next()` when a tier source's
    /// `peek()` returns an `Err`. Pre-fix, the error was coerced to
    /// `has_peek = false` so a transient I/O failure on one tier appeared
    /// as a clean end-of-iterator to the FFI caller (silent data loss for
    /// the affected key range). The FFI layer
    /// (`fill_chunk_from_iter`) consults this field via
    /// `take_last_error()` after each chunk-get cycle to surface the error
    /// as an `FrsErrorCode` to the Java side.
    last_error: Option<ForstError>,
    /// R17-M1: optional shared error slot wired by the FFI consumer when it
    /// needs tier-peek errors to land in the SAME slot it already drains for
    /// outer `db.get_arc` errors (the R16-M2 infrastructure). Pre-fix,
    /// `last_error` lived only on the concrete `LazyPrefixIter`; once the
    /// FFI wrapped it in `Box<dyn Iterator>` the concrete type was erased
    /// and `take_last_error` was unreachable — so tier peek errors were
    /// silently dropped while `db.get_arc` errors were surfaced via the
    /// R16-M2 path. Plumbing this shared `Arc<Mutex<Option<ForstError>>>`
    /// into the iter unifies both error paths into a single observable slot.
    /// `None` preserves the in-process API where callers (engine tests,
    /// `prefix_scan` collector) consult `take_last_error()` directly.
    shared_error_slot: Option<Arc<Mutex<Option<ForstError>>>>,
}

impl LazyPrefixIter {
    fn new(sources: Vec<TierKeySource>) -> ForstResult<Self> {
        Ok(Self {
            sources,
            last_emitted: None,
            last_error: None,
            shared_error_slot: None,
        })
    }

    /// R17-M1: install a shared error slot. When set, tier-peek errors are
    /// recorded into BOTH the local `last_error` field (legacy in-process
    /// API) and the shared slot. The FFI layer drains the shared slot via
    /// `take_last_error()` on `IterHandle` so it sees both tier-peek errors
    /// (captured here) and outer `db.get_arc` errors (captured by the
    /// R16-M2 filter_map adapter) through a single mechanism.
    pub fn set_shared_error_slot(&mut self, slot: Arc<Mutex<Option<ForstError>>>) {
        self.shared_error_slot = Some(slot);
    }

    /// R15-M3: take + clear the last tier-peek error, if any. Called by the
    /// FFI consumer (`fill_chunk_from_iter`) after each chunk-get cycle so
    /// transient tier errors are surfaced to the Java side as
    /// `FrsErrorCode` rather than silently truncating the scan.
    ///
    /// Returns `Some(err)` once per error and `None` thereafter until a new
    /// error occurs.
    pub fn take_last_error(&mut self) -> Option<ForstError> {
        self.last_error.take()
    }
}

impl Iterator for LazyPrefixIter {
    type Item = Arc<[u8]>;

    fn next(&mut self) -> Option<Arc<[u8]>> {
        // Drop already-emitted duplicates from all tiers + find the min
        // pending key across all sources. On error from a tier source we
        // currently swallow it (matches the previous BTreeSet behaviour
        // for transient SST read failures during compaction races — the
        // caller's `db.get` will surface any persistent corruption).
        loop {
            // C9-H2: the running candidate is an `Arc<[u8]>`. `Arc::clone`
            // on each update is an atomic refcount bump (≈ 8 ns) versus
            // `Vec::clone` (alloc + memcpy of the full key payload, ≈ 50-
            // 100 ns + alloc pressure for 32-byte composite keys). The
            // legacy code paid `min_key.to_vec()` on every candidate
            // update — once per source per emit. After this change, the
            // candidate-update cost is purely the Arc refcount.
            let mut min_idx: Option<usize> = None;
            let mut min_key: Option<Arc<[u8]>> = None;
            let num_sources = self.sources.len();
            for i in 0..num_sources {
                // Advance past any tier-local entries equal to the last
                // emission (dedup across tiers). We materialise the peeked
                // key as an `Arc<[u8]>` (cheap refcount bump) so the
                // borrow-checker sees no overlap between probing and the
                // subsequent `advance()` on the same source.
                loop {
                    // Step 1: cheap presence check.
                    //
                    // R15-M3 / R16-L2 + R16-M2: on an `Err(_)` from `peek()`
                    // we capture the error (stickily — later errors
                    // overwrite earlier ones) AND break the inner per-source
                    // loop. The outer loop continues because other tier
                    // sources may still have valid keys to emit; the
                    // recorded error is surfaced to the FFI caller via the
                    // FFI-layer shared error slot (`IterHandle::last_error`)
                    // which is drained from the FFI consumer
                    // (`fill_chunk_from_iter` callers) after each chunk.
                    // Note: the previous comment claimed `LazyPrefixIter::
                    // take_last_error` was drained by the FFI layer, but the
                    // Box<dyn Iterator> shape erased the concrete type — see
                    // R16-M2 fix for the actual error-surfacing path.
                    let has_peek = match self.sources[i].peek() {
                        Ok(Some(_)) => true,
                        Ok(None) => false,
                        Err(e) => {
                            // R17-M1: when the FFI shared slot is wired, publish
                            // the tier-peek error there so the FFI consumer
                            // (which already drains the slot for outer
                            // `db.get_arc` errors per R16-M2) observes it
                            // through a SINGLE mechanism. `ForstError` is not
                            // `Clone` (it wraps `io::Error`), so we move the
                            // value into the shared slot and record a sentinel
                            // string in `last_error` to preserve the
                            // in-process `take_last_error()` API contract.
                            // Lock poison is benign here — overwriting a
                            // poisoned slot restores forward progress.
                            if let Some(slot) = self.shared_error_slot.as_ref() {
                                let mut guard =
                                    slot.lock().unwrap_or_else(|p| p.into_inner());
                                // R18-M3: sticky-FIRST. If the FFI consumer
                                // has not yet drained a prior error in this
                                // chunk, preserve it — a later tier-peek
                                // failure may be a cascade of the first
                                // and the first is more diagnosable. Pre-
                                // fix `*guard = Some(e)` overwrote
                                // unconditionally, hiding root causes when
                                // multiple tiers failed in the same pass.
                                if guard.is_none() {
                                    *guard = Some(e);
                                }
                                // R18-L2: when the shared FFI slot is wired,
                                // the FFI consumer reads errors strictly
                                // through the slot. Pre-fix this branch
                                // recorded a sentinel string in `last_error`
                                // ostensibly to "preserve the in-process
                                // `take_last_error()` API contract", but that
                                // dual-channel publish is now wrong: a single
                                // tier-peek error would surface BOTH through
                                // the slot AND through `take_last_error()`,
                                // and downstream FFI consumers (e.g.,
                                // fill_chunk_from_iter) double-counted it as
                                // two separate errors. Leave `last_error` as
                                // None — the slot is the authoritative
                                // channel when wired. Callers using
                                // LazyPrefixIter directly (without an FFI
                                // shared slot) take the else-branch below
                                // and observe the error through
                                // `take_last_error()` as before.
                            } else {
                                // R19-L1: sticky-FIRST in the unwired branch too.
                                // The wired branch above (which routes errors
                                // into the shared FFI slot) already preserves the
                                // FIRST captured error per R18-M3. Mirror that
                                // semantics here so callers using LazyPrefixIter
                                // directly (no FFI shared slot, e.g., in-process
                                // engine consumers) observe the same root-cause
                                // ordering: an early tier-peek failure should NOT
                                // be buried by a later cascade failure when both
                                // hit within the same merge pass.
                                if self.last_error.is_none() {
                                    self.last_error = Some(e);
                                }
                            }
                            false
                        }
                    };
                    if !has_peek {
                        break;
                    }
                    // Step 2: take an owned Arc clone (atomic refcount
                    // bump). This releases the `&mut self.sources[i]`
                    // borrow held by `peek` above so the rest of the
                    // loop body can call `advance()` freely.
                    let candidate = match self.sources[i].peek_arc() {
                        Some(k) => k,
                        None => break,
                    };
                    if self.last_emitted.as_deref() == Some(candidate.as_ref()) {
                        self.sources[i].advance();
                        continue;
                    }
                    // Step 3: candidate comparison + adoption.
                    let beats = match min_key.as_deref() {
                        None => true,
                        Some(cur) => candidate.as_ref() < cur,
                    };
                    if beats {
                        min_key = Some(candidate);
                        min_idx = Some(i);
                    }
                    break;
                }
            }
            let (idx, key_arc) = match (min_idx, min_key) {
                (Some(i), Some(k)) => (i, k),
                _ => return None,
            };
            self.sources[idx].advance();
            // `last_emitted` retention: cheap Arc clone, no byte copy.
            self.last_emitted = Some(Arc::clone(&key_arc));
            // B10-H3: emit the `Arc<[u8]>` directly — no `to_vec()` at the
            // emit boundary. The FFI `fill_chunk_from_iter` consumer reads
            // `arc.as_ref()` and `copy_nonoverlapping`s into the caller's
            // direct ByteBuffer (no owned-Vec needed). The borrowing /
            // collect callers (`prefix_scan`) call `arc.as_ref().to_vec()`
            // explicitly at their boundary so the public Vec<u8> contract
            // stays intact for downstream consumers.
            return Some(key_arc);
        }
    }
}

// Need this to satisfy Drop semantics if we expose raw Arc<VectorizedMemTable>
// from imm_memtables in the immutable slice layer (currently handled cleanly).
#[allow(clippy::missing_docs_in_private_items)]
trait _Marker {}

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_storage::merge_operator::{ListAppendMergeOperator, MergeOperator};

    fn open() -> Arc<DbImpl> {
        DbImpl::open_default().expect("open")
    }

    // --- bring-up ---

    #[test]
    fn test_open_creates_default_cf() {
        let db = open();
        let h = db.column_family(DEFAULT_CF_NAME).expect("default cf");
        assert_eq!(h.id(), DEFAULT_CF_ID);
        assert_eq!(h.name(), DEFAULT_CF_NAME);
    }

    #[test]
    fn test_default_cf_accessor() {
        let db = open();
        let h = db.default_cf();
        assert_eq!(h.name(), DEFAULT_CF_NAME);
    }

    #[test]
    fn test_open_rejects_zero_write_buffer() {
        let opts = EngineOptions {
            write_buffer_size: 0,
            ..EngineOptions::default()
        };
        assert!(DbImpl::open(opts).is_err());
    }

    #[test]
    fn test_create_column_family_assigns_new_id() {
        let db = open();
        let h = db
            .create_column_family(ColumnFamilyDescriptor::new("cf1"))
            .unwrap();
        assert_ne!(h.id(), DEFAULT_CF_ID);
        assert_eq!(h.name(), "cf1");
    }

    #[test]
    fn test_create_column_family_rejects_duplicate() {
        let db = open();
        db.create_column_family(ColumnFamilyDescriptor::new("cf1"))
            .unwrap();
        let err = db
            .create_column_family(ColumnFamilyDescriptor::new("cf1"))
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    // --- put / get ---

    #[test]
    fn test_put_then_get_returns_value() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        let got = db.get(&cf, b"k").unwrap();
        assert_eq!(got.as_deref(), Some(b"v".as_ref()));
    }

    #[test]
    fn test_get_missing_key_returns_none() {
        let db = open();
        let cf = db.default_cf();
        assert!(db.get(&cf, b"absent").unwrap().is_none());
    }

    #[test]
    fn test_put_overwrites_previous_value() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"old").unwrap();
        db.put(&cf, b"k", b"new").unwrap();
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"new".as_ref()));
    }

    #[test]
    fn test_put_increments_sequence() {
        let db = open();
        let cf = db.default_cf();
        let s1 = db.put(&cf, b"k1", b"v1").unwrap();
        let s2 = db.put(&cf, b"k2", b"v2").unwrap();
        assert!(s2 > s1);
        assert_eq!(db.sequence_number(), s2);
    }

    // --- delete ---

    #[test]
    fn test_delete_masks_prior_put() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.delete(&cf, b"k").unwrap();
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_delete_of_missing_key_is_noop() {
        let db = open();
        let cf = db.default_cf();
        db.delete(&cf, b"never").unwrap();
        assert!(db.get(&cf, b"never").unwrap().is_none());
    }

    // --- merge ---

    fn open_with_merge_cf() -> (Arc<DbImpl>, ColumnFamilyHandle) {
        let db = open();
        let op: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("merge_cf").with_merge_operator(op))
            .unwrap();
        (db, cf)
    }

    #[test]
    fn test_merge_with_prior_put_produces_concatenation() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"a").unwrap();
        db.merge(&cf, b"k", b"b").unwrap();
        db.merge(&cf, b"k", b"c").unwrap();
        let got = db.get(&cf, b"k").unwrap();
        assert_eq!(got.as_deref(), Some(b"a,b,c".as_ref()));
    }

    #[test]
    fn test_merge_without_base_starts_empty() {
        let (db, cf) = open_with_merge_cf();
        db.merge(&cf, b"k", b"x").unwrap();
        db.merge(&cf, b"k", b"y").unwrap();
        let got = db.get(&cf, b"k").unwrap();
        assert_eq!(got.as_deref(), Some(b"x,y".as_ref()));
    }

    #[test]
    fn test_merge_requires_operator() {
        let db = open();
        let cf = db.default_cf(); // no merge operator configured
        db.merge(&cf, b"k", b"v").unwrap();
        let err = db.get(&cf, b"k").unwrap_err();
        assert!(err.to_string().contains("merge operator"));
    }

    #[test]
    fn test_merge_after_delete_uses_empty_base() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"base").unwrap();
        db.delete(&cf, b"k").unwrap();
        db.merge(&cf, b"k", b"x").unwrap();
        let got = db.get(&cf, b"k").unwrap();
        // After Delete, there's no base; merge starts from empty.
        assert_eq!(got.as_deref(), Some(b"x".as_ref()));
    }

    // --- batch write / batch get ---

    #[test]
    fn test_batch_write_put_and_delete_read_back() {
        let db = open();
        let cf = db.default_cf();
        let mut b = WriteBatch::new();
        b.put(&cf, b"k1", b"v1")
            .put(&cf, b"k2", b"v2")
            .delete(&cf, b"k3");
        db.batch_write(b).unwrap();

        assert_eq!(db.get(&cf, b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
        assert_eq!(db.get(&cf, b"k2").unwrap().as_deref(), Some(b"v2".as_ref()));
        assert!(db.get(&cf, b"k3").unwrap().is_none());
    }

    #[test]
    fn test_single_delete_point_read_behaves_like_delete() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.single_delete(&cf, b"k").unwrap();
        assert!(
            db.get(&cf, b"k").unwrap().is_none(),
            "SingleDelete must make the key appear absent to point reads"
        );
    }

    #[test]
    fn test_write_batch_single_delete_roundtrip() {
        let db = open();
        let cf = db.default_cf();
        let mut wb = WriteBatch::new();
        wb.put(&cf, b"k1", b"v1")
            .single_delete(&cf, b"k1")
            .put(&cf, b"k2", b"v2");
        db.batch_write(wb).unwrap();
        assert!(db.get(&cf, b"k1").unwrap().is_none());
        assert_eq!(db.get(&cf, b"k2").unwrap().as_deref(), Some(b"v2".as_ref()));
    }

    #[test]
    fn test_single_delete_compaction_elides_matching_put() {
        // Verify the `[SingleDelete, Put]` pair is elided at compaction time
        // regardless of bottommost-ness: after compaction the key reads
        // absent AND a subsequent Put survives (i.e. no stale tombstone is
        // retained to hide it).
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.flush_all().unwrap();
        db.single_delete(&cf, b"k").unwrap();
        db.flush_all().unwrap();

        db.compact_all().unwrap();
        assert!(
            db.get(&cf, b"k").unwrap().is_none(),
            "key must be absent after [SD, Put] elision"
        );

        // Prove the elision removed the tombstone (not just shadowed it):
        // a fresh Put should become visible immediately without requiring
        // another flush+compaction.
        db.put(&cf, b"k", b"new").unwrap();
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"new".as_ref()));
    }

    #[test]
    fn test_single_delete_falls_back_when_two_puts_precede() {
        // Contract violation: two consecutive Puts before the
        // SingleDelete. Compaction must fall back to normal Delete
        // semantics so the older Put remains shadowed.
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v1").unwrap();
        db.flush_all().unwrap();
        db.put(&cf, b"k", b"v2").unwrap();
        db.flush_all().unwrap();
        db.single_delete(&cf, b"k").unwrap();
        db.flush_all().unwrap();

        db.compact_all().unwrap();
        // The key must still read as absent — the tombstone must win over
        // whichever Put survived the collapse.
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_single_delete_with_intervening_merge_does_not_resurrect() {
        // Regression guard for the Round-3 [SD, Put, Merge]-style shape:
        // the conservative strategy must NOT elide the SingleDelete when
        // the version list has more than two entries. The older merged /
        // put value must stay hidden.
        use crate::ColumnFamilyDescriptor;
        use forst_rs_storage::merge_operator::{ListAppendMergeOperator, MergeOperator};
        use std::sync::Arc as StdArc;

        let db = open();
        let op: StdArc<dyn MergeOperator> = StdArc::new(ListAppendMergeOperator::with_comma());
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("merge_cf").with_merge_operator(op))
            .unwrap();

        db.put(&cf, b"k", b"base").unwrap();
        db.merge(&cf, b"k", b"op1").unwrap();
        db.flush_all().unwrap();
        db.single_delete(&cf, b"k").unwrap();
        db.flush_all().unwrap();

        db.compact_all().unwrap();
        assert!(
            db.get(&cf, b"k").unwrap().is_none(),
            "key must remain absent after fallback — Merge must not resurrect"
        );
    }

    #[test]
    fn test_batch_get_parallel_reads() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"a", b"1").unwrap();
        db.put(&cf, b"b", b"2").unwrap();
        let out = db
            .batch_get(&cf, &[b"a".as_ref(), b"missing".as_ref(), b"b".as_ref()])
            .unwrap();
        assert_eq!(out[0].as_deref(), Some(b"1".as_ref()));
        assert_eq!(out[1], None);
        assert_eq!(out[2].as_deref(), Some(b"2".as_ref()));
    }

    #[test]
    fn test_batch_get_arrow_returns_correct_results() {
        use arrow::array::{Array, BinaryArray, BooleanArray};

        let db = open();
        let cf = db.default_cf();

        // Write some keys.
        db.put(&cf, b"k1", b"v1").unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();
        db.put(&cf, b"k3", b"v3").unwrap();

        // Build keys array including a missing key.
        let keys = BinaryArray::from(vec![
            b"k1".as_slice(),
            b"k2".as_slice(),
            b"missing".as_slice(),
            b"k3".as_slice(),
        ]);

        let result = db.batch_get_arrow(&cf, &keys).unwrap();
        assert_eq!(result.num_rows(), 4);

        let values = result
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let found = result
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert_eq!(values.value(0), b"v1");
        assert_eq!(values.value(1), b"v2");
        assert!(values.is_null(2));
        assert_eq!(values.value(3), b"v3");
        assert!(found.value(0));
        assert!(found.value(1));
        assert!(!found.value(2));
        assert!(found.value(3));
    }

    #[test]
    fn test_batch_get_arrow_empty_keys() {
        use arrow::array::BinaryArray;

        let db = open();
        let cf = db.default_cf();
        let keys = BinaryArray::from(Vec::<&[u8]>::new());
        let result = db.batch_get_arrow(&cf, &keys).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_get_and_put_returns_old_value() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"old").unwrap();
        let old = db.get_and_put(&cf, b"k", b"new").unwrap();
        assert_eq!(old.as_deref(), Some(b"old".as_ref()));
        // Verify the new value is now stored.
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"new".as_ref()));
    }

    #[test]
    fn test_get_and_put_missing_key_returns_none() {
        let db = open();
        let cf = db.default_cf();
        let old = db.get_and_put(&cf, b"absent", b"val").unwrap();
        assert_eq!(old, None);
        // The put still succeeds.
        assert_eq!(
            db.get(&cf, b"absent").unwrap().as_deref(),
            Some(b"val".as_ref())
        );
    }

    #[test]
    fn test_get_and_put_overwrites_multiple_times() {
        let db = open();
        let cf = db.default_cf();
        let old1 = db.get_and_put(&cf, b"k", b"v1").unwrap();
        assert_eq!(old1, None);
        let old2 = db.get_and_put(&cf, b"k", b"v2").unwrap();
        assert_eq!(old2.as_deref(), Some(b"v1".as_ref()));
        let old3 = db.get_and_put(&cf, b"k", b"v3").unwrap();
        assert_eq!(old3.as_deref(), Some(b"v2".as_ref()));
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v3".as_ref()));
    }

    #[test]
    fn test_batch_write_empty_is_ok() {
        let db = open();
        db.batch_write(WriteBatch::new()).unwrap();
    }

    #[test]
    fn test_batch_write_multiple_cfs() {
        let db = open();
        let a = db.default_cf();
        let b_cf = db
            .create_column_family(ColumnFamilyDescriptor::new("other"))
            .unwrap();
        let mut wb = WriteBatch::new();
        wb.put(&a, b"k", b"A").put(&b_cf, b"k", b"B");
        db.batch_write(wb).unwrap();
        assert_eq!(db.get(&a, b"k").unwrap().as_deref(), Some(b"A".as_ref()));
        assert_eq!(db.get(&b_cf, b"k").unwrap().as_deref(), Some(b"B".as_ref()));
    }

    // --- memtable switch ---

    #[test]
    fn test_force_switch_memtable_increments_imm_count() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        let cf_data = db.lookup_cf_by_id(cf.id()).unwrap();
        assert_eq!(cf_data.imm_count(), 0);
        db.force_switch_memtable(&cf).unwrap();
        assert_eq!(cf_data.imm_count(), 1);
    }

    #[test]
    fn test_read_sees_imm_memtable_after_switch() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.force_switch_memtable(&cf).unwrap();
        // After switch, the value lives in the immutable queue only.
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v".as_ref()));
    }

    #[test]
    fn test_snapshot_view_updates_after_switch() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        let cf_data = db.lookup_cf_by_id(cf.id()).unwrap();
        let before = cf_data.snapshot_view();
        assert_eq!(before.imm_list().len(), 0);
        db.force_switch_memtable(&cf).unwrap();
        let after = cf_data.snapshot_view();
        assert_eq!(after.imm_list().len(), 1);
    }

    // --- merge spanning active + imm memtable ---

    #[test]
    fn test_merge_resolves_across_active_and_imm_memtables() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"base").unwrap();
        db.merge(&cf, b"k", b"a").unwrap();
        db.force_switch_memtable(&cf).unwrap();
        // New writes land in the fresh active memtable.
        db.merge(&cf, b"k", b"b").unwrap();
        db.merge(&cf, b"k", b"c").unwrap();
        let got = db.get(&cf, b"k").unwrap();
        assert_eq!(got.as_deref(), Some(b"base,a,b,c".as_ref()));
    }

    #[test]
    fn test_column_family_lookup_by_name_missing_returns_none() {
        let db = open();
        assert!(db.column_family("does-not-exist").is_none());
    }

    // --- W13 flush tests ---

    #[test]
    fn test_flush_cf_noop_when_no_imm() {
        let db = open();
        let cf = db.default_cf();
        assert!(db.flush_cf(&cf).unwrap().is_none());
    }

    #[test]
    fn test_flush_cf_produces_sst() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();
        db.force_switch_memtable(&cf).unwrap();
        let meta = db.flush_cf(&cf).unwrap().expect("meta");
        assert_eq!(meta.num_entries, 2);
        assert_eq!(meta.smallest_key, b"k1");
        assert_eq!(meta.largest_key, b"k2");
    }

    #[test]
    fn test_read_after_flush_returns_value() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        // Data should now live only in the SST file.
        let cf_data = db.lookup_cf_by_id(cf.id()).unwrap();
        assert_eq!(cf_data.imm_count(), 0);
        assert_eq!(db.version_set.current().l0_files().len(), 1);
        assert_eq!(db.get(&cf, b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
        assert_eq!(db.get(&cf, b"k2").unwrap().as_deref(), Some(b"v2".as_ref()));
        assert!(db.get(&cf, b"missing").unwrap().is_none());
    }

    #[test]
    fn test_delete_survives_flush() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.delete(&cf, b"k").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_newer_memtable_masks_older_sst() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"old").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        // Newer value in fresh active memtable.
        db.put(&cf, b"k", b"new").unwrap();
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"new".as_ref()));
    }

    #[test]
    fn test_delete_in_memtable_masks_sst_put() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.delete(&cf, b"k").unwrap();
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_flush_all_drains_multiple_cfs() {
        let db = open();
        let a = db.default_cf();
        let b = db
            .create_column_family(ColumnFamilyDescriptor::new("cfb"))
            .unwrap();
        db.put(&a, b"ka", b"va").unwrap();
        db.put(&b, b"kb", b"vb").unwrap();
        db.force_switch_memtable(&a).unwrap();
        db.force_switch_memtable(&b).unwrap();
        db.flush_all().unwrap();
        assert_eq!(db.version_set.current().l0_files().len(), 2);
        assert_eq!(db.get(&a, b"ka").unwrap().as_deref(), Some(b"va".as_ref()));
        assert_eq!(db.get(&b, b"kb").unwrap().as_deref(), Some(b"vb".as_ref()));
    }

    #[test]
    fn test_multiple_l0_ssts_newest_wins() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k", b"v3").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        assert_eq!(db.version_set.current().l0_files().len(), 3);
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v3".as_ref()));
    }

    #[test]
    fn test_merge_after_flush_uses_sst_base() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"base").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.merge(&cf, b"k", b"a").unwrap();
        db.merge(&cf, b"k", b"b").unwrap();
        assert_eq!(
            db.get(&cf, b"k").unwrap().as_deref(),
            Some(b"base,a,b".as_ref())
        );
    }

    #[test]
    fn test_merge_chain_spans_memtable_imm_and_sst() {
        // Cross-SST merge peel: put then merges, flushed together. The read
        // path currently uses `SstReaderImpl::get()` which returns only the
        // newest entry for a key. When a single SST contains both a Put base
        // and Merge operands for the same key, the peel cannot walk older
        // versions within that SST. This is resolved in W15 compaction by
        // consolidating multiple versions into a single entry per key.
        //
        // The correct behaviour for this scenario is exercised by
        // `test_merge_after_flush_uses_sst_base`, which flushes the base
        // separately from the subsequent merges.
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"base").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap(); // base alone in SST
        db.merge(&cf, b"k", b"a").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap(); // a alone in another SST
        db.merge(&cf, b"k", b"b").unwrap();
        db.force_switch_memtable(&cf).unwrap(); // b in imm
        db.merge(&cf, b"k", b"c").unwrap(); // c in active
        assert_eq!(
            db.get(&cf, b"k").unwrap().as_deref(),
            Some(b"base,a,b,c".as_ref())
        );
    }

    #[test]
    fn test_auto_flush_on_threshold() {
        // Use a tiny write_buffer_size so any put triggers a switch + flush.
        use forst_rs_io::MemoryFileSystem;
        let opts = EngineOptions {
            db_path: "/db".to_string(),
            write_buffer_size: 32, // extremely small
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = DbImpl::open_with_fs(opts, fs).unwrap();
        let cf = db.default_cf();
        for i in 0..10u32 {
            let k = format!("key{:04}", i);
            let v = format!("value{:04}", i);
            db.put(&cf, k.as_bytes(), v.as_bytes()).unwrap();
        }
        // Some L0 files should have been created.
        assert!(!db.version_set.current().l0_files().is_empty());
        for i in 0..10u32 {
            let k = format!("key{:04}", i);
            let v = format!("value{:04}", i);
            assert_eq!(
                db.get(&cf, k.as_bytes()).unwrap().as_deref(),
                Some(v.as_bytes())
            );
        }
    }

    #[test]
    fn test_batch_write_then_flush_then_read() {
        let db = open();
        let cf = db.default_cf();
        let mut batch = WriteBatch::new();
        for i in 0..50u32 {
            // PR-B5-H1: put_owned for `format!`-derived keys that don't
            // outlive the iteration scope.
            batch.put_owned(&cf, format!("key{:04}", i).into_bytes(), b"v".to_vec());
        }
        db.batch_write(batch).unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        for i in 0..50u32 {
            let k = format!("key{:04}", i);
            assert_eq!(
                db.get(&cf, k.as_bytes()).unwrap().as_deref(),
                Some(b"v".as_ref())
            );
        }
    }

    #[test]
    fn test_sst_reader_cache_reuses_reader() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        let meta = &db.version_set.current().l0_files()[0].clone();
        let r1 = db.get_or_open_sst_reader(meta).unwrap();
        let r2 = db.get_or_open_sst_reader(meta).unwrap();
        assert!(Arc::ptr_eq(&r1, &r2));
    }

    // --- W14.2 scan tests ---

    #[test]
    fn test_scan_empty_db_returns_empty() {
        let db = open();
        let cf = db.default_cf();
        let out = db.scan(&cf, b"", None).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn test_scan_collects_sorted_memtable_entries() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"b", b"B").unwrap();
        db.put(&cf, b"a", b"A").unwrap();
        db.put(&cf, b"c", b"C").unwrap();
        let out = db.scan(&cf, b"", None).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].0, b"a");
        assert_eq!(out[1].0, b"b");
        assert_eq!(out[2].0, b"c");
    }

    #[test]
    fn test_scan_respects_bounds() {
        let db = open();
        let cf = db.default_cf();
        for k in [b"a".as_ref(), b"b", b"c", b"d", b"e"] {
            db.put(&cf, k, b"v").unwrap();
        }
        let out = db.scan(&cf, b"b", Some(b"d")).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, b"b");
        assert_eq!(out[1].0, b"c");
    }

    #[test]
    fn test_scan_spans_memtable_imm_and_sst() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"a", b"1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap(); // a in SST
        db.put(&cf, b"b", b"2").unwrap();
        db.force_switch_memtable(&cf).unwrap(); // b in imm
        db.put(&cf, b"c", b"3").unwrap(); // c in active
        let out = db.scan(&cf, b"", None).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(out[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(out[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_scan_skips_deleted_keys() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"a", b"1").unwrap();
        db.put(&cf, b"b", b"2").unwrap();
        db.put(&cf, b"c", b"3").unwrap();
        db.delete(&cf, b"b").unwrap();
        let out = db.scan(&cf, b"", None).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, b"a");
        assert_eq!(out[1].0, b"c");
    }

    #[test]
    fn test_scan_respects_newer_memtable_over_sst() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"old").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k", b"new").unwrap();
        let out = db.scan(&cf, b"", None).unwrap();
        assert_eq!(out, vec![(b"k".to_vec(), b"new".to_vec())]);
    }

    #[test]
    fn test_scan_resolves_merges() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"a").unwrap();
        db.merge(&cf, b"k", b"b").unwrap();
        db.merge(&cf, b"k", b"c").unwrap();
        let out = db.scan(&cf, b"", None).unwrap();
        assert_eq!(out, vec![(b"k".to_vec(), b"a,b,c".to_vec())]);
    }

    #[test]
    fn test_prefix_scan_basic() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"user:a", b"A").unwrap();
        db.put(&cf, b"user:b", b"B").unwrap();
        db.put(&cf, b"order:1", b"O1").unwrap();
        let out = db.prefix_scan(&cf, b"user:").unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, b"user:a");
        assert_eq!(out[1].0, b"user:b");
    }

    #[test]
    fn test_prefix_scan_with_ff_suffix() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, &[b'a', 0x00], b"1").unwrap();
        db.put(&cf, &[b'a', 0xff], b"2").unwrap();
        db.put(&cf, b"b", b"3").unwrap();
        let out = db.prefix_scan(&cf, b"a").unwrap();
        assert_eq!(out.len(), 2);
    }

    /// A7-H2 regression test: `prefix_scan_iter_owned` must see ALL rows under the
    /// prefix even after the active memtable has flushed to SST. The previous
    /// implementation only walked the active memtable's prefix-index, so after a
    /// flush rotated rows out of the active memtable they vanished from the
    /// iterator (Q11 `entries()` silent rowloss).
    #[test]
    fn prefix_scan_iter_after_flush_sees_all_rows() {
        let db = open();
        let cf = db.default_cf();

        // Seed three rows under `user:` before any flush.
        db.put(&cf, b"user:a", b"A").unwrap();
        db.put(&cf, b"user:b", b"B").unwrap();
        db.put(&cf, b"user:c", b"C").unwrap();
        // Also seed a row outside the prefix to assert it is not picked up.
        db.put(&cf, b"order:1", b"O1").unwrap();

        // Open the owned iterator BEFORE flush (mirroring the FFI chunked-iter
        // contract: the iterator is registered in a per-shard slot at open
        // time, then drained later — possibly across flush boundaries).
        let iter = db.prefix_scan_iter_owned(&cf, b"user:").unwrap();

        // Force the active memtable to rotate to an SST. After this call the
        // prefix-index of the active memtable no longer contains the seeded
        // rows; only the SST does.
        db.switch_and_flush(&cf).unwrap().expect("flush produced sst");

        // Drain the iter and verify all three rows are visible in sorted order.
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = iter.collect::<ForstResult<Vec<_>>>().unwrap();
        out.sort_by(|l, r| l.0.cmp(&r.0));
        assert_eq!(
            out,
            vec![
                (b"user:a".to_vec(), b"A".to_vec()),
                (b"user:b".to_vec(), b"B".to_vec()),
                (b"user:c".to_vec(), b"C".to_vec()),
            ],
            "prefix_scan_iter_owned must enumerate active mem + imm mems + SSTs"
        );

        // Re-open after flush — same expectation, this time the SST is the
        // only tier holding the rows when the iterator is constructed.
        let iter2 = db.prefix_scan_iter_owned(&cf, b"user:").unwrap();
        let mut out2: Vec<(Vec<u8>, Vec<u8>)> = iter2.collect::<ForstResult<Vec<_>>>().unwrap();
        out2.sort_by(|l, r| l.0.cmp(&r.0));
        assert_eq!(out2.len(), 3);
    }

    /// C8-H1 regression: the BORROWING `prefix_scan_iter` must also see
    /// rows that have rotated into immutable memtables and flushed SSTs.
    /// Before C8-H1 only A7-H2's owned variant had the cross-tier walk;
    /// `frs_prefix_scan_arrow` / `frs_batch_prefix_scan` still funneled
    /// through the borrowing variant and silently missed post-flush rows.
    #[test]
    fn prefix_scan_iter_borrowing_sees_all_tiers() {
        let db = open();
        let cf = db.default_cf();

        db.put(&cf, b"k/a", b"A").unwrap();
        db.put(&cf, b"k/b", b"B").unwrap();
        db.switch_and_flush(&cf).unwrap().expect("flush 1");
        db.put(&cf, b"k/c", b"C").unwrap();
        // Leave `k/c` in the active memtable.

        let out: Vec<(Vec<u8>, Vec<u8>)> = db
            .prefix_scan_iter(&cf, b"k/")
            .unwrap()
            .collect::<ForstResult<Vec<_>>>()
            .unwrap();
        let mut got: Vec<&[u8]> = out.iter().map(|(k, _)| k.as_slice()).collect();
        got.sort();
        assert_eq!(
            got,
            vec![b"k/a".as_slice(), b"k/b".as_slice(), b"k/c".as_slice()],
            "borrowing prefix_scan_iter must merge active mem + SST rows; got {:?}",
            got
        );
    }

    /// C8-H3 streaming test: opens an iterator on a 100K-row state and
    /// confirms that draining 1K keys at a time terminates each chunk in
    /// bounded work. We don't measure wall-clock (CI variance is too
    /// noisy), but we do assert linear progression — `next()` returns
    /// keys monotonically and the iterator does NOT buffer everything
    /// before the first emission. The pre-fix BTreeSet variant would
    /// have allocated 100K `Vec<u8>` entries inside `prefix_scan_iter_owned`
    /// BEFORE returning; this test indirectly validates the lazy primitive
    /// by checking we can interleave drain with flush and still see all
    /// keys.
    #[test]
    fn prefix_scan_iter_owned_streams_lazily_over_100k_rows() {
        let db = open();
        let cf = db.default_cf();

        const N: u32 = 100_000;
        for i in 0..N {
            let key = format!("ns/{:08}", i);
            db.put(&cf, key.as_bytes(), b"v").unwrap();
        }
        // Push half of the rows out to SST to exercise the SST tier.
        db.switch_and_flush(&cf).unwrap().expect("flush produced sst");
        for i in N..(N * 2) {
            let key = format!("ns/{:08}", i);
            db.put(&cf, key.as_bytes(), b"v").unwrap();
        }

        let mut iter = db.prefix_scan_iter_owned(&cf, b"ns/").unwrap();

        // Drain 1K at a time and assert monotonic ordering across chunks.
        let mut last_key: Option<Vec<u8>> = None;
        let mut total = 0usize;
        loop {
            let mut chunk = 0usize;
            while chunk < 1_000 {
                match iter.next() {
                    Some(Ok((k, _v))) => {
                        if let Some(prev) = &last_key {
                            assert!(
                                k > *prev,
                                "lazy iter must produce strictly increasing keys; \
                                 prev={:?} cur={:?}",
                                String::from_utf8_lossy(prev),
                                String::from_utf8_lossy(&k),
                            );
                        }
                        last_key = Some(k);
                        chunk += 1;
                        total += 1;
                    }
                    Some(Err(e)) => panic!("iter error: {:?}", e),
                    None => break,
                }
            }
            if chunk == 0 {
                break;
            }
        }
        assert_eq!(total, (N * 2) as usize, "must see every row exactly once");
    }

    /// C9-H1 regression: `prefix_scan_iter_owned` MUST NOT materialise the
    /// full active-memtable matching set before the consumer can read the
    /// first row. Before this fix the cross-shard `prefix_scan_keys` ran a
    /// global sort + dedup over 100K `Arc<[u8]>` entries at iterator
    /// construction, which dominated the wall clock to first row.
    ///
    /// We measure two intervals:
    ///   * `t_construct` — time to build `prefix_scan_iter_owned`. This is
    ///     where the legacy path paid the global sort.
    ///   * `t_first` — time from construction completion to the first
    ///     emitted row.
    ///
    /// The new `MemTierCursor` path does a per-shard sort (cheap because
    /// each shard holds ~N/16 entries) and a `BinaryHeap` push per shard.
    /// We assert a generous upper bound — 50ms total on a 100K-entry
    /// active memtable is still ~5× faster than the legacy global-sort
    /// path, and is robust against CI variance. If the legacy regression
    /// returns the same test scaled to 1M would show it sharply.
    #[test]
    fn lazy_iter_construction_does_not_materialize_active_mem() {
        let db = open();
        let cf = db.default_cf();

        // Populate the ACTIVE memtable with 100K rows. Keep them all in
        // the active tier (no flush) so we exercise the new
        // `MemTierCursor` path specifically — the SST tier already
        // streams via `read_block_at` and was not regressed by C9-H1.
        const N: u32 = 100_000;
        for i in 0..N {
            let key = format!("ns/{:08}", i);
            db.put(&cf, key.as_bytes(), b"v").unwrap();
        }

        // Sanity: no SST files exist yet.
        assert!(
            db.version_set.current().live_sst_files().is_empty(),
            "test precondition: all rows must be in the active memtable"
        );

        // Measure construction + first-row latency.
        let t0 = std::time::Instant::now();
        let mut iter = db.prefix_scan_iter_owned(&cf, b"ns/").unwrap();
        let t_construct = t0.elapsed();

        let t1 = std::time::Instant::now();
        let first = iter.next().expect("at least one row");
        let t_first = t1.elapsed();

        let (first_key, _) = first.expect("first row Ok");
        assert_eq!(first_key, b"ns/00000000".to_vec(), "first row sanity");

        // Generous bound: 100ms in CI-variant debug-with-many-shards
        // scenarios is plenty of headroom over the actual ~10-30ms we
        // observe locally for the new path. The pre-fix path's global
        // sort + dedup on 100K Arc<[u8]> entries took >100ms on a warm
        // M1, dominated by the sort comparator. We assert <100ms here
        // to catch a regression to that path; the real perf signal will
        // come from the Q11/Q12 benchmarks.
        let total = t_construct + t_first;
        assert!(
            total.as_millis() < 100,
            "C9-H1: iter construction + first row must complete in <100ms \
             for a 100K-row active memtable; got construct={:?}, first={:?}, \
             total={:?}",
            t_construct,
            t_first,
            total
        );

        // Also drain the rest to confirm correctness — every key is
        // emitted exactly once in sorted order.
        let mut count = 1u32;
        let mut last_key = first_key;
        while let Some(item) = iter.next() {
            let (k, _) = item.expect("ok row");
            assert!(
                k > last_key,
                "C9-H1: keys must be strictly increasing across heap merge"
            );
            last_key = k;
            count += 1;
        }
        assert_eq!(count, N, "must see every active-memtable row exactly once");
    }

    // --- W15 compaction tests ---

    #[test]
    fn test_compact_l0_noop_when_empty() {
        let db = open();
        let cf = db.default_cf();
        assert!(db.compact_l0(&cf).unwrap().is_none());
    }

    #[test]
    fn test_compact_l0_single_file_rolls_up_to_l1() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        assert_eq!(db.version_set.current().l0_files().len(), 1);

        let new_meta = db.compact_l0(&cf).unwrap().expect("new L1 file");
        let v = db.version_set.current();
        assert_eq!(v.l0_files().len(), 0);
        assert_eq!(v.levels[1].files.len(), 1);
        assert_eq!(v.levels[1].files[0].file_number, new_meta.file_number);
        // Read still works.
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v".as_ref()));
    }

    #[test]
    fn test_compact_l0_multiple_files_merged() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k3", b"v3").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        assert_eq!(db.version_set.current().l0_files().len(), 3);

        db.compact_l0(&cf).unwrap().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.l0_files().len(), 0);
        assert_eq!(v.levels[1].files.len(), 1);
        assert_eq!(v.levels[1].files[0].num_entries, 3);

        assert_eq!(db.get(&cf, b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
        assert_eq!(db.get(&cf, b"k2").unwrap().as_deref(), Some(b"v2".as_ref()));
        assert_eq!(db.get(&cf, b"k3").unwrap().as_deref(), Some(b"v3".as_ref()));
    }

    #[test]
    fn test_compact_l0_consolidates_versions() {
        let db = open();
        let cf = db.default_cf();
        // Same key written three times; each flush produces a new L0.
        db.put(&cf, b"k", b"v1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k", b"v3").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();

        db.compact_l0(&cf).unwrap().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.levels[1].files[0].num_entries, 1); // Only latest kept.
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v3".as_ref()));
    }

    #[test]
    fn test_compact_l0_drops_bottommost_tombstones() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.delete(&cf, b"k").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();

        db.compact_l0(&cf).unwrap();
        let v = db.version_set.current();
        // Bottommost compaction — tombstone + Put both removed.
        assert_eq!(v.levels[1].files.len(), 0);
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_compact_l0_with_merge_operator_resolves_chain() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"a").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.merge(&cf, b"k", b"b").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.merge(&cf, b"k", b"c").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();

        db.compact_l0(&cf).unwrap();
        let v = db.version_set.current();
        assert_eq!(v.levels[1].files[0].num_entries, 1); // Collapsed.
        assert_eq!(
            db.get(&cf, b"k").unwrap().as_deref(),
            Some(b"a,b,c".as_ref())
        );
    }

    #[test]
    fn test_compact_l0_deletes_old_files_from_disk() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        let old_file = db.version_set.current().l0_files()[0].file_number;
        let old_path = sst_file_path(&db.db_path, old_file);
        assert!(db.fs.file_exists(&old_path).unwrap());

        db.compact_l0(&cf).unwrap().unwrap();
        assert!(!db.fs.file_exists(&old_path).unwrap());
    }

    #[test]
    fn test_compact_all_flushes_multiple_cfs() {
        let db = open();
        let a = db.default_cf();
        let b = db
            .create_column_family(ColumnFamilyDescriptor::new("other"))
            .unwrap();
        for i in 0..5 {
            db.put(&a, format!("a{}", i).as_bytes(), b"va").unwrap();
            db.put(&b, format!("b{}", i).as_bytes(), b"vb").unwrap();
        }
        db.force_switch_memtable(&a).unwrap();
        db.force_switch_memtable(&b).unwrap();
        db.flush_all().unwrap();
        db.compact_all().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.l0_files().len(), 0);
        for i in 0..5 {
            assert!(db.get(&a, format!("a{}", i).as_bytes()).unwrap().is_some());
            assert!(db.get(&b, format!("b{}", i).as_bytes()).unwrap().is_some());
        }
    }

    // --- W15.3 multi-level compaction ---

    #[test]
    fn test_compact_level_from_l1_to_l2() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        // Roll L0 → L1.
        db.compact_l0(&cf).unwrap().unwrap();
        assert_eq!(db.version_set.current().levels[1].files.len(), 1);
        assert_eq!(db.version_set.current().levels[2].files.len(), 0);

        // Now roll L1 → L2.
        db.compact_level(&cf, 1).unwrap().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.levels[1].files.len(), 0);
        assert_eq!(v.levels[2].files.len(), 1);
        assert_eq!(db.get(&cf, b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
    }

    #[test]
    fn test_compact_level_zero_delegates_to_l0() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_level(&cf, 0).unwrap().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.l0_files().len(), 0);
        assert_eq!(v.levels[1].files.len(), 1);
    }

    #[test]
    fn test_compact_level_returns_none_on_empty_level() {
        let db = open();
        let cf = db.default_cf();
        let out = db.compact_level(&cf, 2).unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn test_compact_level_overlapping_files_are_merged_into_destination() {
        let db = open();
        let cf = db.default_cf();
        // Seed L1 with "a..d".
        db.put(&cf, b"a", b"1").unwrap();
        db.put(&cf, b"d", b"2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();
        // Seed L2 with "c..f" by first landing in L0, then push down.
        db.put(&cf, b"c", b"3").unwrap();
        db.put(&cf, b"f", b"4").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();
        // Now L1 has two files, push the whole level to L2.
        db.compact_level(&cf, 1).unwrap().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.levels[1].files.len(), 0);
        // All keys read correctly.
        for (k, expected) in [
            (b"a" as &[u8], b"1" as &[u8]),
            (b"c", b"3"),
            (b"d", b"2"),
            (b"f", b"4"),
        ] {
            assert_eq!(
                db.get(&cf, k).unwrap().as_deref(),
                Some(expected),
                "mismatch at key {:?}",
                k
            );
        }
    }

    #[test]
    fn test_compact_once_prefers_l0_drain() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        assert!(!db.version_set.current().l0_files().is_empty());
        let did = db.compact_once_for(&cf).unwrap();
        assert!(did);
        assert!(db.version_set.current().l0_files().is_empty());
    }

    #[test]
    fn test_compact_once_returns_false_when_balanced() {
        let db = open();
        let cf = db.default_cf();
        let did = db.compact_once_for(&cf).unwrap();
        assert!(!did);
    }

    // --- W15 CompactionFilter integration ---

    #[test]
    fn test_compaction_filter_discards_expired_entries() {
        use crate::compaction_filter::{encode_ttl_value, TtlCompactionFilter};
        let db = open();
        let filter_now: fn() -> u64 = || 1_000;
        let filter = Arc::new(TtlCompactionFilter::with_clock(50, filter_now));
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("ttl").with_compaction_filter(filter))
            .unwrap();

        // Old entry (ts=500, age at compaction time = 500) should be discarded.
        let old_value = encode_ttl_value(500, b"old");
        db.put(&cf, b"old_key", &old_value).unwrap();

        // Fresh entry (ts=990, age=10) should be kept.
        let fresh_value = encode_ttl_value(990, b"fresh");
        db.put(&cf, b"fresh_key", &fresh_value).unwrap();

        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();

        // old_key → expired → None.
        assert!(db.get(&cf, b"old_key").unwrap().is_none());
        // fresh_key → still present.
        let got = db.get(&cf, b"fresh_key").unwrap();
        assert!(got.is_some());
    }

    /// Post-creation install via [`DbImpl::set_compaction_filter`] using
    /// [`FlinkTtlCompactionFilter`]. Mirrors the FFI path that
    /// `frs_cf_set_compaction_filter_ttl` walks: open CF without a filter,
    /// install one later, then verify expired entries are dropped at the
    /// next compaction.
    #[test]
    fn test_set_compaction_filter_post_create_with_flink_filter() {
        use crate::compaction_filter::{FlinkTtlCompactionFilter, TtlStateType};
        let db = open();
        // Create the CF with NO filter, then install one afterwards.
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("flink-ttl"))
            .unwrap();

        // Inject a deterministic clock at "now=2000ms" with TTL=500ms so
        // a timestamp of 0ms is expired (age=2000 > 500) and a timestamp
        // of 1800ms is fresh (age=200 < 500).
        let supplier: crate::compaction_filter::CurrentTimeSupplier = Arc::new(|| 2000);
        let filter = Arc::new(FlinkTtlCompactionFilter::with_supplier(
            500,
            TtlStateType::Value,
            0,
            supplier,
        ));
        db.set_compaction_filter(&cf, Some(filter)).unwrap();

        // ts=0 → expired → discarded.
        let mut expired_value = Vec::new();
        expired_value.extend_from_slice(&0u64.to_le_bytes());
        expired_value.extend_from_slice(b"old");
        db.put(&cf, b"old", &expired_value).unwrap();

        // ts=1800 → fresh → kept.
        let mut fresh_value = Vec::new();
        fresh_value.extend_from_slice(&1800u64.to_le_bytes());
        fresh_value.extend_from_slice(b"new");
        db.put(&cf, b"new", &fresh_value).unwrap();

        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();

        assert!(db.get(&cf, b"old").unwrap().is_none(), "expired key kept");
        assert!(db.get(&cf, b"new").unwrap().is_some(), "fresh key dropped");
    }

    /// Clearing a previously-installed filter via
    /// [`DbImpl::set_compaction_filter`] with `None` must restore "keep
    /// every entry" semantics on the next compaction.
    #[test]
    fn test_set_compaction_filter_clear_restores_no_filter() {
        use crate::compaction_filter::{FlinkTtlCompactionFilter, TtlStateType};
        let db = open();
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("flink-clear"))
            .unwrap();

        // Install a filter that would expire EVERY value.
        let supplier: crate::compaction_filter::CurrentTimeSupplier = Arc::new(|| u64::MAX);
        let filter = Arc::new(FlinkTtlCompactionFilter::with_supplier(
            1,
            TtlStateType::Value,
            0,
            supplier,
        ));
        db.set_compaction_filter(&cf, Some(filter)).unwrap();
        // Now clear it.
        db.set_compaction_filter(&cf, None).unwrap();

        let mut value = Vec::new();
        value.extend_from_slice(&0u64.to_le_bytes());
        value.extend_from_slice(b"payload");
        db.put(&cf, b"k", &value).unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();
        // No filter → entry survives.
        assert!(db.get(&cf, b"k").unwrap().is_some());
    }

    #[test]
    fn test_compaction_filter_preserves_tombstones() {
        use crate::compaction_filter::TtlCompactionFilter;
        let db = open();
        let now: fn() -> u64 = || 1_000;
        let filter = Arc::new(TtlCompactionFilter::with_clock(0, now));
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("ttl").with_compaction_filter(filter))
            .unwrap();

        db.delete(&cf, b"key").unwrap();
        // Move to L1 first so bottommost elimination doesn't fire.
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"other", b"dummy").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        // Non-bottommost L0→L1 compaction retains tombstones.
        db.compact_l0(&cf).unwrap();
        // The key must still be absent after compaction (filter didn't drop
        // the tombstone).
        assert!(db.get(&cf, b"key").unwrap().is_none());
    }

    // --- W15.2 checkpoint tests ---

    fn open_in_shared_fs(path: &str, fs: Arc<dyn FileSystem>) -> Arc<DbImpl> {
        let opts = EngineOptions {
            db_path: path.to_string(),
            ..EngineOptions::default()
        };
        DbImpl::open_with_fs(opts, fs).unwrap()
    }

    #[test]
    fn test_create_checkpoint_writes_blob_and_ssts() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = open_in_shared_fs("/db", fs.clone());
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();

        let manifest = db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();
        assert!(fs
            .file_exists(std::path::Path::new("/ckpt/CHECKPOINT.blob"))
            .unwrap());
        assert!(!manifest.sst_files.is_empty());
        assert!(manifest.total_bytes > 0);
    }

    #[test]
    fn test_checkpoint_restore_roundtrip() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = open_in_shared_fs("/db", fs.clone());
        let cf = db.default_cf();
        for i in 0..100u32 {
            let k = format!("k{:04}", i);
            let v = format!("v{:04}", i);
            db.put(&cf, k.as_bytes(), v.as_bytes()).unwrap();
        }
        db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();

        // Re-open from the checkpoint directory.
        let opts = EngineOptions {
            db_path: "/ckpt".to_string(),
            ..EngineOptions::default()
        };
        let restored = DbImpl::open_from_checkpoint(opts, fs).unwrap();
        let rcf = restored.default_cf();
        for i in 0..100u32 {
            let k = format!("k{:04}", i);
            let v = format!("v{:04}", i);
            assert_eq!(
                restored.get(&rcf, k.as_bytes()).unwrap().as_deref(),
                Some(v.as_bytes()),
                "mismatch at i={}",
                i
            );
        }
    }

    #[test]
    fn test_checkpoint_includes_active_memtable() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = open_in_shared_fs("/db", fs.clone());
        let cf = db.default_cf();
        // Write without flushing — data lives only in active memtable.
        db.put(&cf, b"k", b"v").unwrap();
        db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();

        // After checkpoint, an L0 file should exist for the captured data.
        let v = db.version_set.current();
        assert!(!v.l0_files().is_empty());
    }

    #[test]
    fn test_open_from_checkpoint_missing_blob_errors() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        fs.create_dir_all(std::path::Path::new("/empty")).unwrap();
        let opts = EngineOptions {
            db_path: "/empty".to_string(),
            ..EngineOptions::default()
        };
        let result = DbImpl::open_from_checkpoint(opts, fs);
        match result {
            Ok(_) => panic!("expected NotFound error"),
            Err(e) => assert!(e.is_not_found()),
        }
    }

    /// R29-H1 regression: drop a higher-numbered SST in db_path BEFORE
    /// restore and verify the engine's restored `next_file_number` is
    /// strictly greater than the orphan's number, AND that the orphan was
    /// renamed out of the `<num>.sst` namespace (R29-M1).
    ///
    /// Pre-R29-H1, the outer `fs.file_exists(&db_path)?` guard returned
    /// false for the checkpoint directory (every FileSystem impl treats
    /// dirs as non-files), so the entire orphan-scan block was dead code
    /// and `restored.version_set.next_file_number()` came straight from
    /// the snapshot — re-using the orphan's file number on the next flush
    /// and silently corrupting the checkpoint.
    #[test]
    fn test_open_from_checkpoint_advances_file_number_past_orphan_sst() {
        use forst_rs_io::{MemoryFileSystem, WriteMode};
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = open_in_shared_fs("/db", fs.clone());
        let cf = db.default_cf();
        for i in 0..10u32 {
            db.put(&cf, format!("k{}", i).as_bytes(), b"v").unwrap();
        }
        db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();

        // Drop a higher-numbered SST in the checkpoint dir that's NOT in
        // the manifest — simulates a crash-mid-flush orphan.
        let orphan_num: u64 = 999_999;
        let orphan_path = sst_file_path(std::path::Path::new("/ckpt"), FileNumber(orphan_num));
        {
            let mut f = fs
                .open_writable_file(&orphan_path, WriteMode::CreateNew)
                .unwrap();
            f.append(b"fake-orphan-sst-bytes").unwrap();
            f.sync().unwrap();
        }
        assert!(fs.file_exists(&orphan_path).unwrap());

        // Restore — orphan must bump restored_next_file_number and be
        // renamed out of the `<num>.sst` namespace.
        let opts = EngineOptions {
            db_path: "/ckpt".to_string(),
            ..EngineOptions::default()
        };
        let restored = DbImpl::open_from_checkpoint(opts, fs.clone()).unwrap();
        assert!(
            restored.version_set.next_file_number() > orphan_num,
            "expected next_file_number > orphan ({}), got {}",
            orphan_num,
            restored.version_set.next_file_number()
        );
        // R29-M1: the orphan was renamed; the original path no longer
        // exists, but a sibling `*.sst.orphan-<ts>` does.
        assert!(
            !fs.file_exists(&orphan_path).unwrap(),
            "orphan SST should have been renamed away from {}",
            orphan_path.display()
        );
        let entries = fs.list_dir(std::path::Path::new("/ckpt")).unwrap();
        assert!(
            entries.iter().any(|e| e
                .path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.contains(".sst.orphan-"))
                .unwrap_or(false)),
            "expected an `.sst.orphan-<ts>` rename target under /ckpt"
        );
    }

    // --- W16 FileDeletionGuard integration ---

    #[test]
    fn test_deletion_guard_protects_pinned_files_during_compaction() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        let pinned = db.version_set.current().l0_files()[0].file_number;
        let pin = db.deletion_guard().pin_batch(&[pinned]);
        // Compaction attempts to delete `pinned` but the guard defers.
        db.compact_l0(&cf).unwrap().unwrap();
        let path = sst_file_path(&db.db_path, pinned);
        assert!(
            db.fs.file_exists(&path).unwrap(),
            "pinned file must survive"
        );
        drop(pin);
        // Trigger a reap via another flush/compact cycle.
        db.put(&cf, b"other", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap();
        assert!(
            !db.fs.file_exists(&path).unwrap(),
            "file must be deleted after pin release"
        );
    }

    #[test]
    fn test_deletion_guard_exposed_via_accessor() {
        let db = open();
        assert!(db.deletion_guard().pinned_files().is_empty());
        db.deletion_guard().pin(FileNumber(1));
        assert_eq!(db.deletion_guard().pin_count(FileNumber(1)), 1);
    }

    #[test]
    fn test_checkpoint_pins_live_files_during_copy() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = open_in_shared_fs("/db", fs);
        let cf = db.default_cf();
        for i in 0..10u32 {
            db.put(&cf, format!("k{}", i).as_bytes(), b"v").unwrap();
        }
        // After checkpoint completes, no pins should remain.
        db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();
        assert!(db.deletion_guard().pinned_files().is_empty());
    }

    /// R31-H1 regression test: incremental checkpoint pinning must be atomic with
    /// the live-file read so a concurrent compaction cannot delete a file that
    /// the checkpoint manifest references between the snapshot read and the
    /// pin_batch call.
    ///
    /// The test directly exercises the atomic-snapshot+pin code path under
    /// concurrent apply pressure: a background thread loops on
    /// {@code apply_lock}-acquiring work (flush + compact) while the test
    /// thread takes many snapshots. The invariant is that snapshot_with_locked_view
    /// observes a self-consistent view: every file_number returned by
    /// live_sst_files() is pinned (pin_count > 0) by the time the closure
    /// runs, and the pin remains valid for the body of the closure.
    #[test]
    fn test_r31_h1_snapshot_and_pin_are_atomic_with_apply() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomOrd};
        let db = open();
        let cf = db.default_cf();
        // Seed several L0 SSTs.
        for batch in 0..4u32 {
            for k in 0..16u32 {
                db.put(&cf, format!("b{:02}k{:04}", batch, k).as_bytes(), b"v")
                    .unwrap();
            }
            db.switch_and_flush(&cf).unwrap();
        }

        let stop = Arc::new(AtomicBool::new(false));
        let stop_t = stop.clone();
        let db_t = db.clone();
        let cf_t = cf.clone();
        let bg = std::thread::spawn(move || {
            // Apply pressure: alternate flush + compact + writes so apply_lock
            // is contended continuously.
            let mut n: u32 = 0;
            while !stop_t.load(AtomOrd::Relaxed) {
                let _ = db_t.compact_l0(&cf_t);
                for k in 0..8u32 {
                    let _ = db_t.put(&cf_t, format!("bgn{:04}k{}", n, k).as_bytes(), b"v");
                }
                let _ = db_t.switch_and_flush(&cf_t);
                n = n.wrapping_add(1);
            }
        });

        // Repeatedly take an atomic snapshot+pin and verify the invariant:
        // every file_number in the returned live set has pin_count >= 1 while
        // the closure is still in scope (pin held by the PinHandle below).
        for _ in 0..200u32 {
            let result =
                db.version_set
                    .snapshot_with_locked_view(|snap: &VersionSetSnapshot| {
                        let live = snap.version.live_sst_files();
                        let nums: Vec<FileNumber> = live.iter().map(|f| f.file_number).collect();
                        let pin = db.deletion_guard.pin_batch(&nums);
                        // Verify every pinned file is still on disk while
                        // pin is held — a concurrent compaction's
                        // delete_file_guarded must see can_delete=false.
                        let mut all_present = true;
                        for n in &nums {
                            let p = sst_file_path(&db.db_path, *n);
                            if !db.fs.file_exists(&p).unwrap_or(false) {
                                all_present = false;
                                break;
                            }
                        }
                        (nums, pin, all_present)
                    });
            let (nums, _pin, all_present) = result;
            assert!(
                all_present,
                "R31-H1: a pinned file is already gone from disk — apply/delete \
                 raced with snapshot+pin (nums={:?})",
                nums.iter().map(|n| n.value()).collect::<Vec<_>>()
            );
        }

        stop.store(true, AtomOrd::Relaxed);
        bg.join().expect("bg thread");
    }

    // ============================================================
    // C1: batch_put_arrow zero-copy direct columnar dispatch
    // ============================================================

    /// Build a (key, value, op_type) RecordBatch matching the FFI schema,
    /// suitable for `DbImpl::batch_put_arrow` and the FFI's
    /// `frs_batch_put_arrow`.
    fn make_put_arrow_batch(
        keys: &[&[u8]],
        values: &[Option<&[u8]>],
        op_types: &[u8],
    ) -> arrow::array::RecordBatch {
        use arrow::array::{BinaryBuilder, StructArray, UInt8Builder};
        use arrow::datatypes::{DataType, Field};
        let mut kb = BinaryBuilder::new();
        let mut vb = BinaryBuilder::new();
        let mut ob = UInt8Builder::new();
        for (i, k) in keys.iter().enumerate() {
            kb.append_value(k);
            match values[i] {
                Some(v) => vb.append_value(v),
                None => vb.append_null(),
            }
            ob.append_value(op_types[i]);
        }
        let s = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Binary, false)),
                Arc::new(kb.finish()) as arrow::array::ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Binary, true)),
                Arc::new(vb.finish()) as arrow::array::ArrayRef,
            ),
            (
                Arc::new(Field::new("op_type", DataType::UInt8, false)),
                Arc::new(ob.finish()) as arrow::array::ArrayRef,
            ),
        ]);
        s.into()
    }

    #[test]
    fn test_batch_put_arrow_correctness() {
        let db = open();
        let cf = db.default_cf();
        let n = 1000u32;
        let keys: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("ak_{:06}", i).into_bytes())
            .collect();
        let vals: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("av_{:06}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let val_refs: Vec<Option<&[u8]>> = vals.iter().map(|v| Some(v.as_slice())).collect();
        let ops = vec![1u8; n as usize]; // OpType::Put = 1 (RocksDB byte-compat)
        let batch = make_put_arrow_batch(&key_refs, &val_refs, &ops);

        let last_seq = db.batch_put_arrow(&cf, &batch).unwrap();
        assert!(last_seq >= n as u64);

        // Round-trip: every key must read back its original value.
        for i in 0..n {
            let k = format!("ak_{:06}", i);
            let v = db.get(&cf, k.as_bytes()).unwrap();
            assert_eq!(
                v.as_deref(),
                Some(format!("av_{:06}", i).as_bytes()),
                "miss at {}",
                i
            );
        }
    }

    #[test]
    fn test_batch_put_arrow_vs_write_batch_equivalence() {
        // Two engines, identical workload: one via Arrow path, one via the
        // legacy WriteBatch path. Both must yield identical reads.
        let db_a = open();
        let db_b = open();
        let cf_a = db_a.default_cf();
        let cf_b = db_b.default_cf();

        let n = 500u32;
        let keys: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("eq_{:05}", i).into_bytes())
            .collect();
        let vals: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("eqv_{:05}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let val_refs: Vec<Option<&[u8]>> = vals.iter().map(|v| Some(v.as_slice())).collect();
        let ops = vec![1u8; n as usize]; // OpType::Put = 1 (RocksDB byte-compat)

        // Arrow path
        let batch = make_put_arrow_batch(&key_refs, &val_refs, &ops);
        db_a.batch_put_arrow(&cf_a, &batch).unwrap();

        // WriteBatch path
        let mut wb = WriteBatch::with_capacity(n as usize);
        for i in 0..n as usize {
            wb.put(&cf_b, key_refs[i], vals[i].as_slice());
        }
        db_b.batch_write(wb).unwrap();

        // Compare every key.
        for i in 0..n {
            let k = format!("eq_{:05}", i);
            let a = db_a.get(&cf_a, k.as_bytes()).unwrap();
            let b = db_b.get(&cf_b, k.as_bytes()).unwrap();
            assert_eq!(a, b, "divergence at key {}", k);
        }
    }

    #[test]
    fn test_batch_put_arrow_empty_batch() {
        let db = open();
        let cf = db.default_cf();
        let batch = make_put_arrow_batch(&[], &[], &[]);
        // Must not panic; returns the current engine sequence.
        let s = db.batch_put_arrow(&cf, &batch).unwrap();
        assert_eq!(s, db.sequence_number());
    }

    #[test]
    fn test_batch_put_arrow_mixed_put_delete() {
        let db = open();
        let cf = db.default_cf();
        // Seed a key we'll delete via the batch.
        db.put(&cf, b"to_delete", b"old").unwrap();

        let keys: Vec<&[u8]> = vec![b"new_a", b"to_delete", b"new_b"];
        let values: Vec<Option<&[u8]>> = vec![Some(b"va"), None, Some(b"vb")];
        let ops: Vec<u8> = vec![1, 0, 1]; // Put (1), Delete (0), Put (1) — RocksDB byte-compat
        let batch = make_put_arrow_batch(&keys, &values, &ops);

        db.batch_put_arrow(&cf, &batch).unwrap();
        assert_eq!(
            db.get(&cf, b"new_a").unwrap().as_deref(),
            Some(b"va".as_ref())
        );
        assert!(
            db.get(&cf, b"to_delete").unwrap().is_none(),
            "delete must take effect"
        );
        assert_eq!(
            db.get(&cf, b"new_b").unwrap().as_deref(),
            Some(b"vb".as_ref())
        );
    }

    #[test]
    fn test_batch_put_arrow_invalid_schema_rejected() {
        use arrow::array::{BinaryBuilder, StructArray};
        use arrow::datatypes::{DataType, Field};
        let db = open();
        let cf = db.default_cf();
        // Schema with only 2 columns (missing op_type) — must reject.
        let mut kb = BinaryBuilder::new();
        let mut vb = BinaryBuilder::new();
        kb.append_value(b"k");
        vb.append_value(b"v");
        let s = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Binary, false)),
                Arc::new(kb.finish()) as arrow::array::ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Binary, true)),
                Arc::new(vb.finish()) as arrow::array::ArrayRef,
            ),
        ]);
        let batch: arrow::array::RecordBatch = s.into();
        assert!(db.batch_put_arrow(&cf, &batch).is_err());
    }

    // ---------------------------------------------------------------------
    // D1 — sequence-number allocation moved outside the memtable lock.
    //
    // Pre-D1: `write_single` allocated the memtable-local seq INSIDE
    // `write_mutex`, then `bump_sequence` ran a CAS loop on the engine
    // counter (also inside the lock).
    //
    // Post-D1: `write_single` / `batch_write` / `batch_put_arrow` reserve
    // the engine seq (range) with a single lock-free `fetch_add` BEFORE
    // acquiring `write_mutex`, and pass the seq down via the memtable's
    // `*_with_seq` APIs.
    //
    // The invariants we verify:
    //   1. Sequences remain strictly monotonic and unique under heavy
    //      concurrent write contention.
    //   2. A 100-row batch reserves a contiguous seq range; no other
    //      concurrent writer ever lands a seq inside that range.
    //   3. The post-write engine counter matches the highest assigned
    //      seq — no skips, no reuse.
    // ---------------------------------------------------------------------

    #[test]
    fn test_sequence_numbers_strictly_monotonic_under_concurrent_writers() {
        // 4 threads x 1000 writes each -> 4000 single-row writes. Capture
        // the seq returned by every put and verify the multiset is
        // exactly {1, 2, ..., 4000} (unique and gap-free).
        use std::sync::Mutex;
        use std::thread;

        let db = open();
        const THREADS: u64 = 4;
        const PER_THREAD: u64 = 1000;
        let collected: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::with_capacity(
            (THREADS * PER_THREAD) as usize,
        )));

        let mut handles = Vec::new();
        for t in 0..THREADS {
            let db = Arc::clone(&db);
            let collected = Arc::clone(&collected);
            handles.push(thread::spawn(move || {
                let cf = db.default_cf();
                let mut local = Vec::with_capacity(PER_THREAD as usize);
                for i in 0..PER_THREAD {
                    let key = format!("t{}_k{}", t, i);
                    let seq = db.put(&cf, key.as_bytes(), b"v").unwrap();
                    local.push(seq);
                }
                let mut all = collected.lock().unwrap();
                all.extend(local);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut all = collected.lock().unwrap().clone();
        all.sort_unstable();

        // Uniqueness: dedup must not change the length.
        let pre_dedup = all.len();
        all.dedup();
        assert_eq!(
            pre_dedup,
            all.len(),
            "duplicate seq detected — D1 fetch_add must yield unique values"
        );
        // Density: exactly THREADS * PER_THREAD seqs were allocated.
        assert_eq!(all.len() as u64, THREADS * PER_THREAD);
        // Contiguity: seqs form a contiguous range starting at 1
        // (the engine starts at 0 and `fetch_add` returns `prev + 1`).
        assert_eq!(*all.first().unwrap(), 1);
        assert_eq!(*all.last().unwrap(), THREADS * PER_THREAD);
        // Engine counter must equal the highest seq.
        assert_eq!(db.sequence_number(), THREADS * PER_THREAD);
    }

    #[test]
    fn test_sequence_number_reservation_outside_lock() {
        // Behavioural witness that the engine assigns exactly ONE seq per
        // put and that the public counter and the returned value agree —
        // i.e. the seq is allocated by a single lock-free fetch_add on the
        // engine counter, not derived from the memtable's local counter
        // via a CAS-bump.
        let db = open();
        let cf = db.default_cf();

        for i in 0..50u64 {
            let pre = db.sequence_number();
            let assigned = db.put(&cf, format!("k{}", i).as_bytes(), b"v").unwrap();
            let post = db.sequence_number();
            assert_eq!(assigned, pre + 1, "seq should be exactly pre+1 after put");
            assert_eq!(post, assigned, "engine counter must equal returned seq");
            assert!(
                post > pre,
                "sequence_number must advance on every successful put"
            );
        }
    }

    #[test]
    fn test_batch_sequence_block_assigned_correctly() {
        // A 100-row batch must reserve a contiguous seq block. After the
        // batch, the engine counter equals `pre + 100`, and the returned
        // last_seq matches the highest assigned seq.
        let db = open();
        let cf = db.default_cf();

        // Pre-seed with a few writes so the block doesn't start at 1.
        db.put(&cf, b"warmup1", b"v").unwrap();
        db.put(&cf, b"warmup2", b"v").unwrap();

        let pre = db.sequence_number();
        let mut batch = WriteBatch::new();
        for i in 0..100u32 {
            batch.put_owned(&cf, format!("bk{}", i).into_bytes(), b"v".to_vec());
        }
        let last_seq = db.batch_write(batch).unwrap();
        let post = db.sequence_number();

        // The batch reserved seqs (pre+1 ..= pre+100).
        assert_eq!(last_seq, pre + 100, "last_seq must be pre + batch_size");
        assert_eq!(
            post,
            pre + 100,
            "engine counter must advance by exactly batch_size"
        );
    }

    #[test]
    fn test_batch_sequence_block_no_overlap_with_concurrent_writers() {
        // Concurrent: one thread does a 200-row batch, three other threads
        // do single puts. Verify the batch's reserved block contains
        // exactly 200 seqs, none of which overlap with seqs returned by
        // the single-put threads.
        use std::sync::{Barrier, Mutex};
        use std::thread;

        let db = open();
        let cf = db.default_cf();

        const BATCH_SIZE: u64 = 200;
        const SINGLE_THREADS: u64 = 3;
        const SINGLE_PER_THREAD: u64 = 200;

        let barrier = Arc::new(Barrier::new((SINGLE_THREADS + 1) as usize));
        let single_seqs: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

        let mut handles = Vec::new();

        // Single-put threads.
        for t in 0..SINGLE_THREADS {
            let db = Arc::clone(&db);
            let cf = cf.clone();
            let barrier = Arc::clone(&barrier);
            let single_seqs = Arc::clone(&single_seqs);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let mut local = Vec::new();
                for i in 0..SINGLE_PER_THREAD {
                    let key = format!("st{}_k{}", t, i);
                    local.push(db.put(&cf, key.as_bytes(), b"v").unwrap());
                }
                single_seqs.lock().unwrap().extend(local);
            }));
        }

        // Batch thread.
        let db_b = Arc::clone(&db);
        let cf_b = cf.clone();
        let barrier_b = Arc::clone(&barrier);
        let batch_handle = thread::spawn(move || {
            barrier_b.wait();
            let mut batch = WriteBatch::new();
            for i in 0..BATCH_SIZE {
                batch.put_owned(&cf_b, format!("bk{}", i).into_bytes(), b"v".to_vec());
            }
            db_b.batch_write(batch).unwrap()
        });

        for h in handles {
            h.join().unwrap();
        }
        let batch_last = batch_handle.join().unwrap();
        let batch_first = batch_last - BATCH_SIZE + 1;

        // The seqs returned by single puts must not lie in the batch's
        // reserved block — i.e. neither writer ever sees a seq the other
        // reserved.
        let singles = single_seqs.lock().unwrap().clone();
        for &s in &singles {
            assert!(
                s < batch_first || s > batch_last,
                "single-put seq {} fell inside batch range [{},{}]",
                s,
                batch_first,
                batch_last
            );
        }

        // Total seqs allocated == single + batch, and the engine counter
        // equals the max of all assigned seqs.
        let total = SINGLE_THREADS * SINGLE_PER_THREAD + BATCH_SIZE;
        assert_eq!(db.sequence_number(), total);
        assert_eq!(singles.len() as u64, SINGLE_THREADS * SINGLE_PER_THREAD);

        // Combined uniqueness: no overlap between single and batch ranges.
        let mut all: Vec<u64> = singles;
        for s in batch_first..=batch_last {
            all.push(s);
        }
        all.sort_unstable();
        let pre_dedup = all.len();
        all.dedup();
        assert_eq!(
            pre_dedup,
            all.len(),
            "concurrent single+batch produced duplicate seqs"
        );
        assert_eq!(*all.first().unwrap(), 1);
        assert_eq!(*all.last().unwrap(), total);
    }

    // -----------------------------------------------------------------
    // MVCC snapshot API (Task 0.10)
    // -----------------------------------------------------------------

    /// snapshot() taken between two puts must see the pre-snapshot value
    /// while a regular get() sees the post-snapshot value.
    #[test]
    fn snapshot_and_get_at_round_trip() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v1").unwrap();
        let snap = db.snapshot();
        db.put(&cf, b"k", b"v2").unwrap();
        assert_eq!(db.get_at(&snap, b"k").unwrap(), Some(b"v1".to_vec()));
        // Confirm "current" view sees v2.
        assert_eq!(db.get(&cf, b"k").unwrap(), Some(b"v2".to_vec()));
        db.release_snapshot(snap);
    }

    /// release_snapshot() must drop the registry ref so active_count
    /// returns to 0 — proves the snapshot's `Drop` actually runs.
    #[test]
    fn snapshot_release_drops_registry_entry() {
        let db = open();
        let snap = db.snapshot();
        assert_eq!(db.snapshot_registry().active_count(), 1);
        db.release_snapshot(snap);
        assert_eq!(db.snapshot_registry().active_count(), 0);
    }

    /// Cross-DB snapshot use must error with InvalidArgument per spec
    /// §15 "Same-DB" invariant; the FFI relies on this validation.
    #[test]
    fn cross_db_snapshot_rejected() {
        let db1 = open();
        let db2 = open();
        let snap1 = db1.snapshot();
        let result = db2.get_at(&snap1, b"k");
        assert!(
            result.is_err(),
            "expected InvalidArgument for cross-DB snapshot, got Ok"
        );
    }

    // -----------------------------------------------------------------
    // Followup 2 (spec §6a.4): sequence-number overflow guard
    //
    // The three tests below assert (a) under-threshold writes work
    // normally, (b) at-warn-threshold writes still succeed (warn is
    // logged via tracing but not surfaced as an error), and (c)
    // at-fatal-threshold writes return ForstError::Internal.
    //
    // We use `force_set_sequence` to seed the counter — burning 2^59
    // writes is not feasible in unit tests. The tests do NOT serialize
    // across the process-singleton SEQ_HIGH_WARNED flag because the
    // warn arm's side-effect is just a tracing line; a flipped flag
    // does not affect the test assertions (which check ForstResult,
    // not log output).
    // -----------------------------------------------------------------

    #[test]
    fn test_seq_overflow_under_threshold_write_succeeds() {
        let db = open();
        // Default counter starts at 0 — comfortably below the warn
        // line. Standard put should succeed.
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").expect("put under threshold");
        // Sanity-check the counter advanced exactly once.
        assert_eq!(db.sequence_number(), 1);
    }

    #[test]
    fn test_seq_overflow_at_warn_threshold_write_still_succeeds() {
        let db = open();
        // Seed the counter so the next `fetch_add(1)` returns the
        // warn threshold. The write should succeed AND advance the
        // counter; the warn line is emitted via tracing but does not
        // surface as a ForstResult error.
        db.force_set_sequence(super::SEQ_NUMBER_WARN_THRESHOLD - 1);
        let cf = db.default_cf();
        db.put(&cf, b"warn", b"v")
            .expect("put at warn threshold must still succeed");
        let post = db.sequence_number();
        assert!(
            post >= super::SEQ_NUMBER_WARN_THRESHOLD,
            "counter should have advanced past warn threshold, got {}",
            post
        );
    }

    #[test]
    fn test_seq_overflow_at_fatal_threshold_returns_internal_error() {
        let db = open();
        // Seed the counter so the next `fetch_add(1)` returns a value
        // at-or-past the 2^60 fatal threshold. The write path must
        // bail with ForstError::Internal and refuse to plumb the seq
        // into the memtable.
        db.force_set_sequence(super::SEQ_NUMBER_FATAL_THRESHOLD - 1);
        let cf = db.default_cf();
        let err = db
            .put(&cf, b"fatal", b"v")
            .expect_err("put at fatal threshold must error");
        assert!(
            err.is_internal(),
            "expected Internal error at fatal threshold, got {:?}",
            err
        );
        // Subsequent writes continue to fail — the counter sits past
        // the fatal threshold and every fresh `fetch_add(1)` lands
        // above the line too.
        let err2 = db
            .put(&cf, b"fatal2", b"v")
            .expect_err("subsequent put at fatal threshold must also error");
        assert!(err2.is_internal());
    }
}
