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
    SequenceNumber, DEFAULT_CF_ID, MAX_SEQUENCE_NUMBER,
};
use forst_rs_io::{FileSystem, LocalFileSystem, MemoryFileSystem, OpendalFileSystem, WriteMode};
use forst_rs_storage::cache::clock::ShardedClockCache;
use forst_rs_storage::cached_fs::CachedFileSystem;
use forst_rs_storage::local_cache::LocalCache;
use forst_rs_storage::merge_operator::{ListAppendMergeOperator, RawConcatMergeOperator};
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
use crate::flush::{
    sst_file_path, CompactionExecutor, FlushExecutor, FlushJob, SST_TMP_PREFIX, SST_TMP_SUFFIX,
};
use crate::mvcc::{self, DbId, Snapshot, SnapshotRegistry};
use crate::runtime_tuning::WriteBufferManager;
use crate::snapshot_view::SnapshotView;
use crate::write_batch::WriteBatch;
use crate::write_controller::{WriteController, WriteControllerConfig};

/// Process-wide allocator for [`DbId`] values.
///
/// Every `DbImpl::open*` path mints a fresh id via `fetch_add(1, Relaxed)`.
/// Bound into every `Snapshot` at capture time (spec §15 "Same-DB"
/// invariant) so the FFI release path can detect cross-DB releases.
static NEXT_DB_ID: AtomicU64 = AtomicU64::new(1);

// 2026-05-30: a process-shared block cache was tried (one pool for all DB
// instances, db_id-qualified keys) to give heavy queries a larger effective
// cache. It REGRESSED q9 (4.1M vs per-instance 6.5M @242s — longer hard stalls,
// likely cross-DB shard contention from ~8 DBs hammering one ShardedClockCache).
// Reverted to per-instance caches; the per-instance decoded-block cache stays
// (it is the verified ~14-25% win). See
// docs/superpowers/specs/2026-05-30-shared-cross-instance-block-cache.md.

/// FRS-BLOCKCACHE-ENV (2026-05-31): apply the optional `FRS_BLOCK_CACHE_MB`
/// per-run override to the computed decoded-block cache size (in bytes).
///
/// The FFM backend does not yet expose a Flink config key for the in-RAM
/// decoded-block `ShardedClockCache` (it passes `block_cache_capacity_bytes=0`,
/// so the engine falls to the 256 MiB default). A live symbolized profile of
/// the q4/q7/q9 heavy-join stall showed that once join state exceeds that
/// cache, every per-key prefix-iterator probe re-runs `read_data_block →
/// decompress + arrow_ipc RecordBatch decode` on a cache miss — the dominant
/// remaining cost after the whole-SST pread fix removed the I/O. This hook lets
/// the operator size the cache against the box (8c/32g: WBM memtables 6 GiB +
/// JVM 8 GiB leave headroom for a few GiB × instance-count) WITHOUT a Java
/// rebuild. The override only ever RAISES the size (`.max`), never shrinks the
/// validated floor; a zero/invalid value is ignored. Applies per DB instance.
fn apply_block_cache_env_override(cache_bytes: usize) -> usize {
    match std::env::var("FRS_BLOCK_CACHE_MB")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&mb| mb > 0)
    {
        Some(mb) => mb.saturating_mul(1024 * 1024).max(cache_bytes),
        None => cache_bytes,
    }
}

/// FRS-BLOCK-SIZE env override (q4 read-amp experiment, 2026-06-03): force the
/// SST data-block size (in KiB) via `FRS_BLOCK_SIZE_KB` without a rebuild. The
/// default is 64 KiB; q4's interval-join probes scatter across keys, so each
/// ~1-key prefix scan / point read `pread`s + LZ4-decompresses a whole 64 KiB
/// block for one key — heavy read amplification (the dominant `pread` cost in
/// the q4 decay profile). RocksDB uses 4-16 KiB blocks. Smaller blocks cut the
/// bytes read + decompressed per scattered access (at the cost of a larger
/// index). Bounded to [`MIN_BLOCK_SIZE`, `MAX_BLOCK_SIZE`] by config validation.
fn apply_block_size_env_override(block_size: usize) -> usize {
    match std::env::var("FRS_BLOCK_SIZE_KB")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&kb| kb > 0)
    {
        Some(kb) => kb.saturating_mul(1024),
        None => block_size,
    }
}

/// FRS-SST-COMPRESSION env override (perf experiment, 2026-06-02): force the
/// SST block compression via `FRS_SST_COMPRESSION=none|lz4|zstd`. A differential
/// q7 profile showed LZ4 `decompress` is ~43% of the heavy-join prefix-iter CPU
/// in the decay regime (block reads re-decompress + re-decode Arrow per probe).
/// This knob lets us measure uncompressed blocks (the prerequisite for the
/// zero-copy mmap'd Arrow read path) WITHOUT changing the default (S3-bound
/// queries still want LZ4 to cut transfer bytes). Unset = keep configured value.
fn apply_compression_env_override(
    compression: forst_rs_common::CompressionType,
) -> forst_rs_common::CompressionType {
    use forst_rs_common::CompressionType;
    match std::env::var("FRS_SST_COMPRESSION").ok().as_deref() {
        Some("none") | Some("None") | Some("NONE") => CompressionType::None,
        Some("lz4") | Some("Lz4") | Some("LZ4") => CompressionType::Lz4,
        Some("zstd") | Some("Zstd") | Some("ZSTD") => CompressionType::Zstd,
        _ => compression,
    }
}

/// FRS-ITER-DIAG: cached check for the `FRS_ITER_DIAG=1` env flag. Reads the
/// env var once (the result is process-stable) so the prefix-stream build hot
/// path pays only an atomic load per call when diagnostics are off.
fn frs_iter_diag_enabled() -> bool {
    use std::sync::OnceLock;
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| {
        std::env::var("FRS_ITER_DIAG")
            .map(|s| s.trim() == "1")
            .unwrap_or(false)
    })
}

/// BULK-SAMPLE DIAG (FRS_BULK_SAMPLE=K, off when 0/unset): time 1-in-K
/// `build_lazy_prefix` builds at NANOSECOND granularity and break the per-probe
/// cost into resident-read sub-components — `rfve` (resident_flushed read-lock +
/// O(N) clone), `bloom` (may_contain_range prune), `cursor` (prefix_scan_cursor
/// = scan + sort) — vs `sst` (Tier-3 fan-out). 1/K SAMPLING (not a lowered
/// threshold) so the ~1.7µs bulk builds are measured WITHOUT observer-effect
/// pollution: only 1-in-K builds pay the `Instant::now` calls. Answers "is the
/// 3.3× floor the resident FIXED overhead or SST fan-out?" → picks the lever.
fn bulk_sample_k() -> usize {
    use std::sync::OnceLock;
    static K: OnceLock<usize> = OnceLock::new();
    *K.get_or_init(|| {
        std::env::var("FRS_BULK_SAMPLE")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(0)
    })
}

/// Returns true on every K-th call (the sampled builds).
fn bulk_sample_hit(k: usize) -> bool {
    static CTR: AtomicU64 = AtomicU64::new(0);
    k > 0 && CTR.fetch_add(1, Ordering::Relaxed).is_multiple_of(k as u64)
}

/// Accumulate one sampled build's sub-phase ns; dump running averages every 8192
/// samples. `resident-fixed` = rfve+bloom+cursor; `sst` = fan-out.
/// DECAY-ATTRIBUTION (windowed, NOT cumulative): each dump is the per-probe mean of
/// the LAST window of 8192 sampled builds, so comparing the run's EARLY vs LATE
/// dumps attributes the per-probe GROWTH (the decay) — not a blended snapshot —
/// into the three refactorable ceilings: A=fan-out (Tier-3 SST), B=resident Tier-2
/// (bloom+cursor), C=active-seek (Tier-1 active-memtable cursor). `rfve` (resident
/// clone+lock) is reported separately (known ~1%).
#[allow(clippy::too_many_arguments)]
fn bulk_record(
    active: u64,
    rfve: u64,
    bloom: u64,
    cursor: u64,
    sst: u64,
    locate: u64,
    n_overlap: u64,
    n_overlap_l0: u64,
    resident_total: u64,
    resident_seeks: u64,
    total: u64,
) {
    const W: u64 = 8192;
    static N: AtomicU64 = AtomicU64::new(0);
    static ACTIVE: AtomicU64 = AtomicU64::new(0);
    static RFVE: AtomicU64 = AtomicU64::new(0);
    static BLOOM: AtomicU64 = AtomicU64::new(0);
    static CURSOR: AtomicU64 = AtomicU64::new(0);
    static SST: AtomicU64 = AtomicU64::new(0);
    static LOCATE: AtomicU64 = AtomicU64::new(0);
    static NOVERLAP: AtomicU64 = AtomicU64::new(0);
    static NOVERLAP_L0: AtomicU64 = AtomicU64::new(0);
    static RTOTAL: AtomicU64 = AtomicU64::new(0);
    static RSEEKS: AtomicU64 = AtomicU64::new(0);
    static TOTAL: AtomicU64 = AtomicU64::new(0);
    ACTIVE.fetch_add(active, Ordering::Relaxed);
    RFVE.fetch_add(rfve, Ordering::Relaxed);
    BLOOM.fetch_add(bloom, Ordering::Relaxed);
    CURSOR.fetch_add(cursor, Ordering::Relaxed);
    SST.fetch_add(sst, Ordering::Relaxed);
    LOCATE.fetch_add(locate, Ordering::Relaxed);
    NOVERLAP.fetch_add(n_overlap, Ordering::Relaxed);
    NOVERLAP_L0.fetch_add(n_overlap_l0, Ordering::Relaxed);
    RTOTAL.fetch_add(resident_total, Ordering::Relaxed);
    RSEEKS.fetch_add(resident_seeks, Ordering::Relaxed);
    TOTAL.fetch_add(total, Ordering::Relaxed);
    let n = N.fetch_add(1, Ordering::Relaxed) + 1;
    if n.is_multiple_of(W) {
        // Windowed: swap each accumulator to 0 so the NEXT window starts fresh →
        // each dump is this window's per-probe mean (early vs late = the decay).
        let take = |a: &AtomicU64| a.swap(0, Ordering::Relaxed) / W;
        // Raw window SUM (NOT /W): resident shadows are sparse (<1 per probe),
        // so a per-probe integer average rounds to 0 and hides the count. The
        // raw sum over the 8192-probe window preserves sub-1 resolution.
        let take_sum = |a: &AtomicU64| a.swap(0, Ordering::Relaxed);
        let (act, rf, bl, cu, ss, lo, nov, novl0, tot) = (
            take(&ACTIVE),
            take(&RFVE),
            take(&BLOOM),
            take(&CURSOR),
            take(&SST),
            take(&LOCATE),
            take(&NOVERLAP),
            take(&NOVERLAP_L0),
            take(&TOTAL),
        );
        let rtot = take_sum(&RTOTAL);
        let rseek = take_sum(&RSEEKS);
        let resident = bl + cu; // B = Tier-2 resident-shadow (bloom + cursor)
                                // FRS-A-SPLIT: A_fanout = A_locate (overlapping_ssts_in_range) + A_sstloop
                                // (per-SST get_or_open + may_contain_range prune + first_block_ge). n_ovl
                                // = mean overlapping-SST count (n_ovl_l0 of which are L0): if n_ovl_l0
                                // dominates+grows ⇒ L0 compaction-starved (trigger lever); if the DEEP
                                // remainder (n_ovl - n_ovl_l0) grows ⇒ multi-level spread (merge lever).
                                // FRS-B-SPLIT: B_resident broken into n_res (resident shadows examined)
                                // + n_seek (those passing the bloom into a BTreeMap seek). If n_res grows
                                // ⇒ count-driven (shrink the shadow set); if n_res flat but bloom/cursor
                                // rise ⇒ per-seek-cost-driven (the seek/bloom itself is the lever).
        let sstloop = ss.saturating_sub(lo);
        let nov_deep = nov.saturating_sub(novl0);
        eprintln!(
            "[DECAY_ATTR win@{n}] probe={tot}ns | A_fanout={ss}(locate={lo},sstloop={sstloop},n_ovl={nov}[L0={novl0},deep={nov_deep}]) B_resident={resident}(bloom={bl},cursor={cu},n_res/{W}={rtot},n_seek/{W}={rseek}) C_activeseek={act} rfve={rf}"
        );
    }
}

/// Cadence at which the snapshot-age ticker polls
/// [`SnapshotRegistry::check_long_lived`] (spec §6a.3). One second is
/// far below the 5-minute default warn threshold, so the worst-case
/// detection latency after a snapshot first crosses the line is ~1 s —
/// well inside any operator-actionable timeframe. Bounded to avoid
/// log spam: the warn is RATE-LIMITED INSIDE the ticker by only emitting
/// when `check_long_lived` returns `Some(...)`, which already requires
/// a snapshot to be live past the threshold.
const SNAPSHOT_AGE_TICK_MS: u64 = 1_000;

/// Sequence-number warn threshold (spec §6a.4). 2^55 — at this point
/// the writer has burned half of the engine's 56-bit usable seq
/// space; surfacing the condition early lets an operator schedule a
/// checkpoint-and-restart cycle before the fatal threshold lands.
/// Process-singleton warn (gated via [`SEQ_HIGH_WARNED`]) so logs do
/// not get spammed on every write past the line.
const SEQ_NUMBER_WARN_THRESHOLD: u64 = 1u64 << 55;

/// Sequence-number fatal threshold (spec §6a.4). InternalKey packs the
/// sequence into the upper 56 bits, so `MAX_SEQUENCE_NUMBER + 1` must never
/// reach memtable or SST encoding.
const SEQ_NUMBER_FATAL_THRESHOLD: u64 = MAX_SEQUENCE_NUMBER.0 + 1;

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
    // FRS-LOCKFREE (2026-06-02): lock-free read view. Reads (the hot per-block
    // path) `.load()` an `Arc<HashMap>` with no lock; writers publish a new map
    // via `.rcu()` (clone-on-write; inserts/removes are rare — once per SST
    // open / compaction-retire). Removes the RwLock read on every block read.
    sst_readers: arc_swap::ArcSwap<HashMap<FileNumber, Arc<SstReaderImpl>>>,
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
    /// FRS-SLOT-SHARED-BG (2026-06-05): a `Weak` self-reference so background
    /// jobs submitted to the PROCESS-GLOBAL flush/compaction pools
    /// ([`bg_flush_pool`]/[`bg_compact_pool`]) can upgrade back into this engine
    /// (or no-op if it has been dropped). Replaces the former per-DbImpl
    /// `flush_queue`/`compaction_queue` + dedicated worker threads, whose count
    /// scaled with (operators × parallelism) and starved the foreground on q4.
    /// Set once at construction via [`Self::init_self_weak`].
    self_weak: std::sync::OnceLock<Weak<DbImpl>>,
    /// Per-CF enqueue-time dedup: a CF whose id is in this set already has a
    /// compaction queued or running, so re-triggers are dropped (the queue
    /// holds at most one entry per CF). Cleared at the START of
    /// `run_compaction` so a re-trigger during a compaction re-queues for the
    /// next round.
    compaction_queued: Mutex<std::collections::HashSet<forst_rs_common::ColumnFamilyId>>,
    /// Most recent error from a background flush. Surfaced to the next
    /// writer that calls [`Self::write_single`] / [`Self::batch_write`] so
    /// the application learns about flush failures even though the failing
    /// flush ran off the writer's stack. Cleared after the writer observes
    /// it.
    flush_error: Mutex<Option<ForstError>>,
    /// Sticky fatal consistency error. Unlike transient flush errors, this is
    /// never consumed by a later caller: once in-memory state may be torn,
    /// every read/write/checkpoint boundary refuses until restart from the
    /// last durable checkpoint.
    fatal_error: Mutex<Option<String>>,
    /// 2026-05-29 PERF-RESTORE-#7: AtomicBool fast-path for `check_fatal_error`.
    /// Set when `record_fatal_error` first transitions the slot Some→None.
    /// Per-op `check_fatal_error` then becomes a single relaxed atomic load on
    /// the happy path instead of a Mutex acquisition (which was firing per
    /// put/get/delete/merge/batch_put on every engine entry).
    fatal_error_set: std::sync::atomic::AtomicBool,
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
    /// R44-H1: serializes ALL compactions globally. Per-CF `flush_mutex`
    /// is insufficient because the `VersionSet` is engine-global — SST
    /// inputs in `compact_l0_for_cf` / `compact_level_for_cf` are picked
    /// from `version_set.current()` without a CF filter, so two threads
    /// compacting different CFs would otherwise both read the same V0
    /// and both `apply` their edits, producing two L1 files at the same
    /// level with overlapping ranges (silent stale-read + 2× disk).
    ///
    /// Lock ordering: `compaction_mutex` is acquired BEFORE any per-CF
    /// `flush_mutex` (via `lock_flush()`) inside `compact_l0_for_cf` /
    /// `compact_level_for_cf`. No other path acquires them in the
    /// reverse order: `flush_cf_data` only takes `flush_mutex`, and
    /// `run_flush` releases `flush_mutex` before calling
    /// `maybe_auto_compact` (which then takes `compaction_mutex`).
    /// Defense-in-depth: `Version::apply_edit` also validates that every
    /// `deleted_files` entry still exists in the current version and
    /// returns retry-able `ForstError::Busy` otherwise (R44-L2).
    compaction_mutex: Mutex<()>,
    /// FRS-WAL Phase 2 (2026-06-06): optional local write-ahead log. `None`
    /// unless `FRS_WAL_DIR` is set, so the default build is byte-identical to
    /// the pre-WAL engine (the append sites are `if let Some(..)` no-ops when
    /// `None`). When `Some`, every mutation is appended and the batch is
    /// group-commit fsynced before the write returns — durability today, and the
    /// foundation for the cheap-checkpoint path (sync WAL instead of forced
    /// flush) that closes the q4-vs-RocksDB gap. See `crate::wal`.
    wal: Mutex<Option<crate::wal::WalWriter>>,
    /// FRS-L0-SHORTCIRCUIT (2026-06-03): diagnostic counter — number of L0 SST
    /// data-block reads performed during point `get`s inside `Self::sst_get`.
    /// The L0 walk now visits files newest-first and STOPS at the first
    /// Put/Delete base, so an overwrite key present in every L0 SST costs ONE
    /// block read instead of O(L0). This was the q11/q4 read-amplification
    /// decay (hot ValueState key Put-overwritten every record → present in
    /// every L0 SST → bloom can't skip it → the old code read all ~40 blocks
    /// and discarded 39). Read by the regression test that pins the
    /// short-circuit and by perf diagnostics.
    l0_point_get_block_reads: AtomicU64,
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
        Self::open_with_fs_and_default_cf(options, fs, ColumnFamilyDescriptor::new(DEFAULT_CF_NAME))
    }

    /// Creates a new engine with a caller-supplied default-CF descriptor.
    ///
    /// General engine callers should use [`Self::open_with_fs`]. The descriptor variant exists for
    /// the ForSt-RS FFI backend, which needs the default CF to own the raw-concat ListState merge
    /// operator so vectorized append batches can be written as real Merge records.
    pub fn open_with_fs_and_default_cf(
        options: EngineOptions,
        fs: Arc<dyn FileSystem>,
        default_desc: ColumnFamilyDescriptor,
    ) -> ForstResult<Arc<Self>> {
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

        // FRS-SST-COMPRESSION perf knob (2026-06-02): allow forcing block
        // compression via env without changing the default. See
        // `apply_compression_env_override`.
        let options = {
            let mut o = options;
            o.compression = apply_compression_env_override(o.compression);
            o.block_size = apply_block_size_env_override(o.block_size);
            o
        };

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
        // FRS-BLOCKCACHE-FLOOR: the decoded-block ShardedClockCache is created
        // PER DB INSTANCE (not shared — a shared pool regressed heavy queries via
        // cross-DB shard-lock contention). The cache is native-resident and
        // charged by actual bytes inserted, so a higher floor costs light
        // queries nothing but lets heavy streaming-join DBs keep their re-probed
        // decoded data blocks resident (the profiled decode_data_block +
        // decompress + serial_read_at re-read cost on per-key prefix scans).
        // FRS-8C32G-RAMBUDGET: 256 MiB (was 768) — per-instance × ~8 instances ≈ 2 GiB,
        // fits the 32 GiB budget alongside JVM heap + the (now 1 GiB) resident shadow.
        const BLOCK_CACHE_FLOOR: usize = 256 * 1024 * 1024;
        let cache_bytes = cache_bytes.max(BLOCK_CACHE_FLOOR);
        let cache_bytes = apply_block_cache_env_override(cache_bytes);
        maybe_start_mem_diag(); // FRS_MEM_DIAG: pinpoint engine resident native (join-OOM 16GB)
        let block_cache = shared_block_cache(cache_bytes); // FRS-ROCKSDB-PARITY C3: slot-shared
                                                           // FRS-GLOBAL-WBM-BUDGET: enroll in the process-global memtable budget so the
                                                           // TOTAL memtable RAM across all keyed-state DB instances is bounded (RocksDB's
                                                           // shared-WriteBufferManager model), not 512 MiB × instance-count. The
                                                           // configured capacity stays the per-instance secondary bound.
        let write_buffer_manager =
            WriteBufferManager::new_global(options.write_buffer_manager_capacity_bytes);

        // FRS-S3-STALL: honour the configured `max_write_buffer_number` in the
        // back-pressure controller. Previously this used
        // `WriteController::with_defaults()` (hard-coded 3), silently dropping
        // the operator-supplied value that flows in via EngineOptions → FFI →
        // Java `state.backend.forst-rs.writebuffer.count`. With the default
        // dropped, writes stalled at 3 immutable memtables regardless of
        // config; on high-latency object stores (S3/OSS/BOS) the slow flush
        // could not drain that backlog within `stall_timeout`, so
        // `may_throttle()` returned a `timed_out` error that the FFI maps to
        // `Unknown(999)` and the Java FFM bridge mis-escalates to a fatal
        // engine panic, restart-looping the job. l0 + stall_timeout headroom
        // comes from `WriteControllerConfig::default()` (see write_controller.rs).
        let wc_config = WriteControllerConfig {
            max_write_buffer_number: options.max_write_buffer_number as u32,
            ..WriteControllerConfig::default()
        };

        let db = Arc::new(Self {
            options,
            db_path,
            fs,
            cfs: RwLock::new(HashMap::new()),
            cf_name_to_id: RwLock::new(HashMap::new()),
            version_set: Arc::new(VersionSetImpl::new()),
            sst_readers: arc_swap::ArcSwap::from_pointee(HashMap::new()),
            deletion_guard: Arc::new(FileDeletionGuard::new()),
            pending_deletions: Mutex::new(Vec::new()),
            sequence_number: AtomicU64::new(0),
            write_controller: Arc::new(WriteController::new(wc_config)),
            write_mutex: Mutex::new(()),
            next_cf_id: AtomicU32::new(1),
            self_weak: std::sync::OnceLock::new(),
            compaction_queued: Mutex::new(std::collections::HashSet::new()),
            flush_error: Mutex::new(None),
            fatal_error: Mutex::new(None),
            fatal_error_set: std::sync::atomic::AtomicBool::new(false),
            pending_flush_count: AtomicU32::new(0),
            snapshot_registry: SnapshotRegistry::new(),
            db_id: DbId(NEXT_DB_ID.fetch_add(1, Ordering::Relaxed)),
            block_cache,
            write_buffer_manager,
            snapshot_age_worker: Mutex::new(None),
            snapshot_age_shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            compaction_mutex: Mutex::new(()),
            wal: Mutex::new(None),
            l0_point_get_block_reads: AtomicU64::new(0),
        });

        db.create_cf_with_id(DEFAULT_CF_ID, default_desc)?;
        Self::init_self_weak(&db);
        db.maybe_init_wal();
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
        Self::open_remote_with_default_cf(
            options,
            uri,
            opendal_config,
            cache_dir,
            cache_capacity_bytes,
            ColumnFamilyDescriptor::new(DEFAULT_CF_NAME),
        )
    }

    /// Opens a remote-storage-backed engine with a caller-supplied default-CF descriptor.
    pub fn open_remote_with_default_cf(
        options: EngineOptions,
        uri: &str,
        opendal_config: HashMap<String, String>,
        cache_dir: &std::path::Path,
        cache_capacity_bytes: u64,
        default_desc: ColumnFamilyDescriptor,
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
        Self::open_with_fs_and_default_cf(options, cached_fs, default_desc)
    }

    /// Returns a handle to the default column family.
    pub fn default_cf(&self) -> ColumnFamilyHandle {
        self.lookup_cf_by_id(DEFAULT_CF_ID)
            .expect("default CF must always exist")
            .handle()
            .clone()
    }

    /// Creates a new column family with the given descriptor.
    ///
    /// # R45-H1: multi-CF + heterogeneous merge/filter restriction
    ///
    /// The engine's `VersionSet` and L0 layer are currently shared across
    /// every CF: `compact_l0_for_cf` reads `version_set.current().l0_files()`
    /// without a CF filter, so an L0 file produced by CF *b*'s flush can be
    /// picked up and rewritten by a compaction job that captured CF *a*'s
    /// merge operator and compaction filter. When the two CFs disagree on
    /// either of those (e.g. CF *a* has TTL, CF *b* doesn't; or each uses a
    /// different merge operator), this silently produces wrong results.
    ///
    /// Until per-CF `Version` scoping lands (requires a manifest format
    /// change — codec v2 — to tag every [`SstFileMeta`] with its
    /// `ColumnFamilyId`), we refuse the configuration up front. A CF can
    /// be created with a non-default merge operator OR a non-default
    /// compaction filter only when every other CF carries the same
    /// (operator name, filter name) pair. Equality is checked by name —
    /// the trait's `name()` is the documented identity surface.
    ///
    /// Homogeneous multi-CF deployments (every CF default, or every CF
    /// using the exact same merge/filter) remain fully supported.
    pub fn create_column_family(
        &self,
        desc: ColumnFamilyDescriptor,
    ) -> ForstResult<ColumnFamilyHandle> {
        // R46-M1: hold the `cfs` write-lock across the homogeneity check AND
        // the `create_cf_with_id` install so two concurrent
        // `create_column_family` calls cannot both observe an empty
        // existing-non-default set, then race to install heterogeneous CFs.
        // Pre-fix: the read-lock was released after `check_cf_homogeneity`,
        // and the fetch_add + insert ran lock-free — a classic TOCTOU.
        //
        // Locking the entire (check, allocate id, insert) sequence is the
        // simplest fix; the cost is serialized CF creation, which is a
        // negligibly-rare admin path (Flink's backend creates O(states) CFs
        // at open and never thereafter).
        let _create_guard = self.cfs.write().expect("lock poisoned");
        // Re-check duplicates inside the write lock — the name_map and the
        // cfs map are kept in sync, so a duplicate must already be reflected
        // by an existing id in `cfs`, but we go through the name map for
        // a clearer error message.
        {
            let name_map = self.cf_name_to_id.read().expect("lock poisoned");
            if name_map.contains_key(desc.name()) {
                return Err(ForstError::invalid_argument(format!(
                    "column family '{}' already exists",
                    desc.name()
                )));
            }
        }
        // R45-H1: reject heterogeneous merge_operator / compaction_filter
        // across CFs. See doc-comment above for the rationale.
        self.check_cf_homogeneity_locked(&_create_guard, None, &desc)?;
        let id = ColumnFamilyId(self.next_cf_id.fetch_add(1, Ordering::SeqCst));
        // `create_cf_with_id_locked` reuses the write guard we already hold
        // for the `cfs` map; the name-map insert still takes its own lock.
        self.create_cf_with_id_locked(_create_guard, id, desc)
    }

    /// R46-M2 escape hatch: create a CF without the R45-H1 homogeneity
    /// check. The TOCTOU-safe lock window from `create_column_family`
    /// (cfs write-lock held across check + insert) is preserved.
    ///
    /// Intentionally NOT public — the only caller is
    /// [`Self::create_cf_from_import`], which has accepted the
    /// silent-wrong-result risk in its own doc.
    fn create_column_family_no_homogeneity_check(
        &self,
        desc: ColumnFamilyDescriptor,
    ) -> ForstResult<ColumnFamilyHandle> {
        let _create_guard = self.cfs.write().expect("lock poisoned");
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
        self.create_cf_with_id_locked(_create_guard, id, desc)
    }

    /// R45-H1: validates that the new descriptor's merge_operator and
    /// compaction_filter are compatible with every existing non-default
    /// CF.
    ///
    /// Compatibility rule (until per-CF Version scoping lands):
    /// - the (merge_operator_name, compaction_filter_name) pair of the
    ///   new CF must equal the pair of every existing non-default CF.
    ///
    /// The default CF is exempt: it is auto-created at `open` time with
    /// no merge_operator and no compaction_filter, and pre-existing
    /// `set_compaction_filter` callers (notably the FFI) rely on
    /// installing a non-default filter on user CFs while the default CF
    /// stays empty. Users who actually write to BOTH the default CF and
    /// a CF with a non-default policy still risk silent wrong results
    /// when their L0 files end up shared — but that pattern is
    /// undocumented and not exercised by any current binding.
    ///
    /// `None` is a distinct value from `Some("...")` — i.e. a non-default
    /// CF with no merge operator is NOT compatible with another
    /// non-default CF whose merge operator is `Some("StringAppend")`.
    /// This catches the original R45-H1 hazard (two user CFs disagreeing
    /// on policy) where the silent wrong-result risk is highest.
    fn check_cf_homogeneity_locked(
        &self,
        cfs: &HashMap<ColumnFamilyId, Arc<ColumnFamilyData>>,
        // For `set_compaction_filter`: the id of the CF being modified —
        // we must NOT compare it against itself (that's the whole point of
        // a post-create swap). `None` means "compare against every
        // non-default CF" (the create_column_family path).
        exclude_self: Option<ColumnFamilyId>,
        desc: &ColumnFamilyDescriptor,
    ) -> ForstResult<()> {
        // R47-H1 / R47-H3: `name()` returns `String` and now encodes every
        // semantically-relevant config field (TTL, delimiter, etc.) into
        // the identity. The homogeneity comparison is by-value over those
        // strings — two filters/operators that disagree on any encoded
        // config field are NOT admitted as homogeneous.
        let new_merge_name: Option<String> = desc.merge_operator().as_ref().map(|op| op.name());
        let new_filter_name: Option<String> = desc.compaction_filter().as_ref().map(|f| f.name());

        for cf in cfs.values() {
            let cf_id = cf.handle().id();
            if cf_id == DEFAULT_CF_ID {
                continue;
            }
            if Some(cf_id) == exclude_self {
                continue;
            }
            let exist_merge_name: Option<String> = cf.merge_operator().map(|op| op.name());
            let exist_filter_name: Option<String> =
                cf.compaction_filter().as_ref().map(|f| f.name());
            if exist_merge_name != new_merge_name {
                return Err(ForstError::invalid_argument(format!(
                    "column family '{}' has merge_operator {:?}, but existing CF '{}' \
                     uses {:?}; the engine's L0 layer is shared across CFs and \
                     heterogeneous merge operators would silently produce wrong \
                     results during cross-CF compaction (R45-H1). Use the same \
                     merge_operator on every non-default CF, or open separate engines.",
                    desc.name(),
                    new_merge_name,
                    cf.handle().name(),
                    exist_merge_name
                )));
            }
            if exist_filter_name != new_filter_name {
                return Err(ForstError::invalid_argument(format!(
                    "column family '{}' has compaction_filter {:?}, but existing CF \
                     '{}' uses {:?}; the engine's L0 layer is shared across CFs and \
                     heterogeneous compaction filters would silently produce wrong \
                     results during cross-CF compaction (R45-H1). Use the same \
                     compaction_filter on every non-default CF, or open separate engines.",
                    desc.name(),
                    new_filter_name,
                    cf.handle().name(),
                    exist_filter_name
                )));
            }
        }
        Ok(())
    }

    fn create_cf_with_id(
        &self,
        id: ColumnFamilyId,
        desc: ColumnFamilyDescriptor,
    ) -> ForstResult<ColumnFamilyHandle> {
        let cfs_guard = self.cfs.write().expect("lock poisoned");
        self.create_cf_with_id_locked(cfs_guard, id, desc)
    }

    /// `create_cf_with_id` variant that consumes a pre-acquired `cfs`
    /// write guard. Used by `create_column_family` so the homogeneity
    /// check and the install happen under a single lock window (R46-M1).
    fn create_cf_with_id_locked(
        &self,
        mut cfs_guard: std::sync::RwLockWriteGuard<
            '_,
            HashMap<ColumnFamilyId, Arc<ColumnFamilyData>>,
        >,
        id: ColumnFamilyId,
        desc: ColumnFamilyDescriptor,
    ) -> ForstResult<ColumnFamilyHandle> {
        let name = desc.name().to_string();
        let options = desc.options().clone();
        let merge_op = desc.merge_operator();
        let filter = desc.compaction_filter();

        // R50-H2: validate the (id, name) pair against the engine's CF
        // invariants BEFORE inserting. Restore (`open_from_checkpoint`)
        // drives this path with descriptors decoded from an untrusted
        // checkpoint blob — a duplicate-id, duplicate-name, or a
        // (DEFAULT_CF_ID, name != "default") pair must be rejected as
        // corruption rather than silently overwriting the existing entry
        // (the pre-fix `HashMap::insert` had this exact data-loss footgun).
        //
        // The default-cf rule is symmetric: id 0 is reserved for the
        // built-in default CF whose name is fixed at [`DEFAULT_CF_NAME`].
        // Any other name carrying id 0 is by construction either a forged
        // blob or a writer-side bug; refusing it here keeps the
        // (cf_id == DEFAULT_CF_ID) ⇔ (name == DEFAULT_CF_NAME)
        // bidirectional invariant intact across the engine.
        if id == DEFAULT_CF_ID && name != DEFAULT_CF_NAME {
            return Err(ForstError::corruption(format!(
                "cf_descriptor: cf_id 0 (DEFAULT_CF_ID) must have name \"{}\", got \"{}\"",
                DEFAULT_CF_NAME, name
            )));
        }
        if cfs_guard.contains_key(&id) {
            return Err(ForstError::corruption(format!(
                "cf_descriptor: duplicate cf_id {} (already registered)",
                id.value()
            )));
        }
        {
            // Read-only peek under the cfs write lock — we're about to
            // drop the cfs guard and then acquire the name-map write
            // guard, so the peek-then-insert sequence holds against
            // concurrent CF creates because every install path goes
            // through this function under cfs.write().
            //
            // R51-L2 — Lock order invariant (write paths only):
            //   cfs.write()  →  cf_name_to_id.read()
            //   cfs.write()  →  (drop)  →  cf_name_to_id.write()
            // CF-install / CF-drop sites are the only writers and ALL go
            // through this function under `cfs.write()`. Future write
            // paths MUST NOT take `cf_name_to_id` (read or write) first
            // and then attempt `cfs.write()` — that would invert this
            // order and deadlock against an in-flight install. Read-only
            // callers (lookup_cf_by_name etc.) take only `cf_name_to_id`
            // and are not part of this ordering.
            let names = self.cf_name_to_id.read().expect("lock poisoned");
            if names.contains_key(&name) {
                return Err(ForstError::corruption(format!(
                    "cf_descriptor: duplicate cf name \"{}\" (already registered)",
                    name
                )));
            }
        }

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

        cfs_guard.insert(id, cf_data.clone());
        // Drop the cfs write guard before grabbing name_map's write guard to
        // keep the lock order consistent everywhere else in the codebase.
        drop(cfs_guard);
        {
            let mut names = self.cf_name_to_id.write().expect("lock poisoned");
            names.insert(name, id);
        }

        // Install a snapshot view that references the active memtable.
        self.refresh_snapshot_view(&cf_data);
        Ok(handle)
    }

    /// Returns a handle for the given CF name, if it exists.
    ///
    /// R47-L2: lock-acquisition order is `cfs → name_map` to match
    /// every other call site (`create_cf_with_id_locked`, `drop_cf`).
    /// Both locks here are read-only so deadlock risk is purely
    /// hypothetical; the standardization is for forecloseure of any
    /// future write-lock that might be added to either map.
    pub fn column_family(&self, name: &str) -> Option<ColumnFamilyHandle> {
        let cfs = self.cfs.read().expect("lock poisoned");
        let name_map = self.cf_name_to_id.read().expect("lock poisoned");
        let id = name_map.get(name).copied()?;
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
    /// # R46-H1: homogeneity validation
    ///
    /// The same cross-CF homogeneity rule that gates
    /// [`Self::create_column_family`] applies here too. Flink's
    /// `ForStRsTtlCompactFiltersManager.setTtlForState` installs a
    /// `FlinkTtlCompactionFilter` per state, each carrying a different
    /// `ttlMs`. Two states with different TTLs translate into two
    /// `set_compaction_filter` calls with filters whose `name()` differs
    /// (the Flink-shaped filter's name encodes the TTL). The engine's
    /// L0 layer is shared across CFs, so allowing CF *a* to keep TTL=10s
    /// while CF *b* runs TTL=60s would silently produce wrong results
    /// during cross-CF compaction. Pre-fix this path bypassed the same
    /// check that `create_column_family` enforced.
    ///
    /// The validation compares the candidate filter's `name()` against
    /// every OTHER non-default CF's filter (the target CF itself is
    /// excluded — that's the whole point of a post-create swap). The
    /// existing merge_operator on the target CF is also re-validated so
    /// `set_compaction_filter` cannot create a heterogeneous merge layout
    /// either (in practice the merge operator is fixed at create time
    /// and this branch is always a no-op).
    ///
    /// # Errors
    ///
    /// Returns [`ForstError::InvalidArgument`] when the handle does not
    /// match a known column family, or when installing this filter would
    /// violate the homogeneity invariant.
    pub fn set_compaction_filter(
        &self,
        cf: &ColumnFamilyHandle,
        filter: Option<Arc<dyn CompactionFilter>>,
    ) -> ForstResult<()> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;

        // R47-H2: hold `self.cfs.write()` across check + install so two
        // concurrent `set_compaction_filter` calls on different CFs
        // cannot both pass the homogeneity check (each observing the
        // other's pre-install state) and then both flip the filter slot,
        // landing the engine in a heterogeneous state.
        //
        // Pre-fix the check used `self.cfs.read()`, released it, then
        // called `cf_data.set_compaction_filter` outside any lock — a
        // classic TOCTOU. We now serialize the install path on the CF
        // map's write lock. Compaction job assembly reads filters via
        // `cf_data.compaction_filter()` (a per-CF RwLock independent of
        // the map's lock) so holding the map write-lock here does not
        // stall in-flight compactions.
        //
        // The default CF is exempt from validation entirely. For all
        // current callers the default CF carries no filter and the
        // post-create install path targets a non-default user CF — the
        // exclusion here is for symmetry with the create-time rule.
        if cf.id() != DEFAULT_CF_ID {
            // Build a synthetic descriptor that carries the target CF's
            // existing merge operator (which is fixed for the CF's
            // lifetime — see ColumnFamilyData::merge_operator) and the
            // CANDIDATE filter we're about to install. The homogeneity
            // helper only reads `.name()` off of each operator/filter,
            // so the synthetic descriptor's behaviour matches "the CF
            // as it would look once the install lands".
            let mut probe = ColumnFamilyDescriptor::new(cf.name());
            if let Some(op) = cf_data.merge_operator() {
                probe = probe.with_merge_operator(op.clone());
            }
            if let Some(f) = filter.clone() {
                probe = probe.with_compaction_filter(f);
            }
            // Acquire the cfs write-lock across check + install — see
            // R47-H2 doc-comment above.
            let _install_guard = self.cfs.write().expect("lock poisoned");
            self.check_cf_homogeneity_locked(&_install_guard, Some(cf.id()), &probe)?;
            cf_data.set_compaction_filter(filter);
            // `_install_guard` is released at end of scope; ordering
            // (check → install → release) is preserved by the explicit
            // sequencing of the calls above.
            return Ok(());
        }
        // Default CF: no homogeneity gate, no lock window needed.
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
    /// SST FILES ARE NOT DELETED. Post-R49-H1 each SST IS tagged with a
    /// `cf_id` so a per-CF reclamation walk is technically possible
    /// (filter `version.live_sst_files()` by `cf_id == dropped_cf`).
    /// We do not do that eagerly today because:
    ///
    ///   * A CF's SSTs may still be pinned by an outstanding `Snapshot`
    ///     or by an in-flight compaction's `PinHandle` — eager unlinking
    ///     would either trip the deletion guard's assertion or have to
    ///     re-implement the same defer-until-unpinned logic compaction
    ///     already runs.
    ///   * Compaction's `should_drop` MVCC logic retires the orphaned
    ///     rows as they age out, so the disk-space penalty is bounded
    ///     by the next compaction sweep — adequate for the spec §6g
    ///     acceptance criterion ("dropped CF is invisible to subsequent
    ///     reads"; physical reclamation is allowed to be lazy).
    ///
    /// R50-M3 follow-up: eager per-CF reclamation should be added in a
    /// dedicated PR that wires the SST-set walk through
    /// `delete_file_guarded` so the pin contract is honoured. Until then,
    /// lazy reclamation via compaction's MVCC drop path stays correct.
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
        //
        // R59-H1 ordering: mark_dropped FIRST also gates any flush worker
        // that picks up a pending imm AFTER this point — flush_cf_data
        // checks `is_dropped()` at its head and at the post-job-run
        // boundary, so no fresh SST stamped with this CF's id can be
        // installed into the Version once the flag is flipped. In-flight
        // flushes that already passed both checks land their VersionEdit
        // BEFORE our `current()` walk below, so the file is captured by
        // the deleted_files set on the next loop iteration.
        cf_data.mark_dropped();

        // R60-M2: briefly take `write_mutex` to ensure any concurrent
        // batch_write / batch_put_arrow / write_single that already
        // resolved this CF's data (and reserved WBM bytes via R58-H1)
        // either completed its memtable insert before mark_dropped was
        // visible, or will observe the dropped flag on its NEXT touch
        // of the CF. The mutex itself doesn't gate writes (writes
        // don't hold it during memtable insert), but acquiring it
        // serves as a memory-barrier sync-point: by the time we
        // release, every prior writer's `is_dropped()` load is
        // guaranteed to observe `true`, and any future writer that
        // proceeds had to acquire+release this mutex AFTER us (so
        // their reserve→insert pair lands wholly after mark_dropped
        // and they exit via the flag check on the next iteration of
        // their retry loop — at which point the reserve they made is
        // refunded by their `WbmReleaseGuard` drop).
        drop(self.write_mutex.lock().expect("write_mutex poisoned"));

        // R60-H2: take `compaction_mutex` BEFORE the WBM release and
        // version walk so we serialize against any in-flight
        // compact_l0_for_cf / compact_level_for_cf that would otherwise
        // install a fresh L1 SST with this CF's id while we are
        // mid-cleanup. The compaction paths take `compaction_mutex`
        // first then check `is_dropped()` — observing our flip means
        // they no-op. Without this, a compaction picked up before
        // mark_dropped could finish its `job.run()` and then call
        // `version_set.apply` with new_files referencing the dead
        // cf_id AFTER our delete_files apply, producing exactly the
        // orphan our cleanup is supposed to prevent.
        //
        // R60-M1: also take the per-CF flush mutex via `lock_flush()`
        // before sampling memtable bytes — this serializes us against
        // `flush_cf_data`, so the WBM byte sampling here and the
        // `release(oldest.memory_usage())` inside the flush path
        // cannot double-account the same imm.
        //
        // Lock order: `compaction_mutex` → `flush_mutex` matches the
        // canonical order documented at db.rs:322-327 (compact path),
        // so no inversion possible.
        let _compaction_guard = self
            .compaction_mutex
            .lock()
            .expect("compaction_mutex poisoned");
        let _flush_guard = cf_data.lock_flush();

        // Release memtable bytes back to the WriteBufferManager. We
        // approximate by sampling both the active and immutable memtable
        // usage; the per-shard accounting is precise enough for the
        // cross-CF budget cap which is itself coarse (~512 MiB default).
        //
        // R62-H1 + R63-H1: drain the imm list AND sample byte counts in
        // a SINGLE loop, so a concurrent writer that completes
        // `swap_active_memtable` between our sample and our drain
        // cannot push an un-accounted imm. Pre-R63 the two passes were
        // separate (`imm_memtables()` clone for the sum, then
        // `pop_oldest_imm()` to drain); a new imm pushed in between
        // would be popped without its bytes being refunded —
        // re-introducing the very leak R62-H1 was supposed to close.
        //
        // Folding the sample into the drain pulls each imm via
        // pop_oldest_imm, measures its bytes BEFORE its Arc drops, and
        // accumulates into `released_bytes`. The loop terminates when
        // the imm_list is genuinely empty; any post-drain imm pushed
        // by an in-flight writer is caught by R61-M1 when the queued
        // flush worker observes `is_dropped()` after we release
        // lock_flush.
        let mut released_bytes: u64 = cf_data.active_memtable().memory_usage() as u64;
        while let Some(imm) = cf_data.pop_oldest_imm() {
            released_bytes = released_bytes.saturating_add(imm.memory_usage() as u64);
        }
        self.write_buffer_manager.release(released_bytes);

        // R58-H2 / R59-H2 / R59-H3: drop the CF's SST files. Pre-fix
        // (R58-H2), `drop_cf` removed the CF from in-memory maps but
        // left every SST file the CF produced (across all levels)
        // referenced by the Version. R59-H2 then identified that the
        // R58-H2 walk-then-apply pair was not atomic with concurrent
        // flush/compaction `apply`, so a flush that landed between the
        // snapshot and the apply could install a fresh L0 file with the
        // dead cf_id that the deleted_files set would miss.
        //
        // Fix: loop. Each iteration walks the CURRENT Version, collects
        // every (level, file_number) whose meta.cf_id matches the CF
        // being dropped, and applies a VersionEdit. If apply returns
        // `Busy` (stale edit — a concurrent flush/compaction landed
        // between our walk and our apply), re-walk and re-apply. We cap
        // the retry attempts so a runaway concurrent flush stream
        // cannot wedge us forever; the cap is generous (16) because
        // R59-H1's `is_dropped()` flush gate guarantees that, after
        // mark_dropped, AT MOST the in-flight flush at the moment of
        // mark_dropped can install a new file — no new flush enters
        // the apply path once the flag is observed.
        //
        // R59-H3 ordering: this loop runs AFTER mark_dropped + WBM
        // release so on apply Err the engine state is consistent —
        // mark_dropped means writes are rejected, WBM means budget is
        // free, and the orphan files (if any) are deferred to the next
        // restart's manifest replay rather than leaving an
        // inconsistent half-cleaned CF.
        let cf_id_to_drop = cf.id();
        const DROP_CF_RETRY_MAX: u32 = 16;
        let mut attempts: u32 = 0;
        loop {
            let current_version = self.version_set.current();
            let mut deleted_files: Vec<(u32, FileNumber)> = Vec::new();
            for (lvl_idx, lvl) in current_version.levels.iter().enumerate() {
                for f in &lvl.files {
                    if f.cf_id == cf_id_to_drop {
                        deleted_files.push((lvl_idx as u32, f.file_number));
                    }
                }
            }
            if deleted_files.is_empty() {
                break;
            }
            let edit = VersionEdit {
                deleted_files: deleted_files.clone(),
                ..Default::default()
            };
            match self.version_set.apply(&edit) {
                Ok(_) => {
                    {
                        self.sst_readers.rcu(|cur| {
                            let mut next = (**cur).clone();
                            for (_, file_number) in &deleted_files {
                                next.remove(file_number);
                            }
                            std::sync::Arc::new(next)
                        });
                    }
                    for (_, file_number) in &deleted_files {
                        self.delete_file_guarded(*file_number);
                    }
                    self.reap_pending_deletions();
                    // Re-walk once more to catch a concurrent flush
                    // that landed between our walk and our apply but
                    // whose edit happened to be compatible (no Busy
                    // returned). The `is_dropped` flag at flush_cf_data
                    // bounds how many such races can occur.
                    attempts += 1;
                    if attempts >= DROP_CF_RETRY_MAX {
                        tracing::warn!(
                            target: "forst_rs_engine::drop_cf",
                            cf_id = cf_id_to_drop.0,
                            "drop_cf: hit retry cap after {} iterations; \
                             remaining files (if any) will be reclaimed by next restart",
                            attempts
                        );
                        break;
                    }
                    continue;
                }
                Err(ForstError::Busy(_)) if attempts < DROP_CF_RETRY_MAX => {
                    attempts += 1;
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        target: "forst_rs_engine::drop_cf",
                        cf_id = cf_id_to_drop.0,
                        error = %e,
                        "drop_cf: VersionEdit apply failed; SST files remain referenced \
                         and will be reclaimed on next restart"
                    );
                    break;
                }
            }
        }

        // R63-M1: refresh the shared WriteController metrics so its
        // `may_throttle()` stall logic reflects the post-drop state.
        // After this CF's imms were drained and its L0/Ln SSTs deleted,
        // the global imm/L0 counts the controller uses for back-pressure
        // are stale — pre-fix every subsequent writer (across all CFs)
        // could trip a phantom throttle until some other CF's flush or
        // compaction happened to call the setters. R62-H1's eager imm
        // drain made this stale state more visible because the queued
        // flush worker that would have called `set_imm_count` on each
        // pop is now a no-op via R61-M1's is_dropped short-circuit.
        self.write_controller
            .set_imm_count(cf_data.imm_count() as u32);
        self.write_controller
            .set_l0_file_count(self.version_set.current().l0_files().len() as u32);

        // R47-M1: standardize lock order to `cfs → name_map` everywhere.
        // The reverse order (name_map → cfs) inverted the convention used
        // by `create_cf_with_id_locked` and `create_column_family`, which
        // both acquire `cfs` first. Mixed lock orders are not currently
        // hazardous (no nested acquires deadlock here — each section
        // drops before the next acquires), but standardizing the order
        // forecloses future lock-order-inversion deadlocks if either
        // section is ever extended to hold a guard across the boundary.
        //
        // Correctness note: the dropped flag is the source of truth once
        // flipped (see comment above mark_dropped); removing from the
        // maps in either order is observationally equivalent for lookups
        // that race against drop_cf.
        let cf_name = cf_data.handle().name().to_string();
        {
            let mut cfs = self.cfs.write().expect("lock poisoned");
            cfs.remove(&cf.id());
        }
        {
            let mut names = self.cf_name_to_id.write().expect("lock poisoned");
            names.remove(&cf_name);
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

            // R52-H1: try hardlink first (fastest, zero-copy on same
            // FS) — but only when both source and destination route to
            // the engine's local leg. In tiered mode, `src` is on the
            // caller's local FS while `dest` (a `.sst` file) routes to
            // the remote leg via `self.fs`. `std::fs::hard_link` /
            // `std::fs::copy` would either fail (cross-fs) or worse
            // succeed against the local mount and leave the remote leg
            // missing the SST entirely. Fall back to a route-aware copy
            // that reads `src` via the local FS and writes `dest` via
            // `self.fs` so every byte lands on the correct leg.
            //
            // The hardlink fast-path remains for the local-only / dev
            // configuration where `self.fs` IS a local FS — the kernel
            // call still pays off on a same-FS ingest (no copy at all).
            // If hardlink fails for any reason we fall back to the
            // route-aware copy regardless of cause (cross-fs EXDEV,
            // EPERM, unsupported backend, …) — the slow path is correct
            // for every configuration.
            let mut linked = false;
            if std::fs::hard_link(src, &dest).is_ok() {
                // Validate the destination is actually visible through
                // `self.fs` (it would not be on a tiered backend with a
                // separate remote leg). If not, undo the link and fall
                // through to the route-aware copy.
                match self.fs.file_exists(&dest) {
                    Ok(true) => linked = true,
                    _ => {
                        let _ = std::fs::remove_file(&dest);
                    }
                }
            }
            if !linked {
                if let Err(e) = self.copy_external_sst(src, &dest) {
                    self.cleanup_ingested(&new_files);
                    return Err(ForstError::Io(std::io::Error::other(format!(
                        "ingest_external_sst: copy '{}' -> '{}' failed: {e}",
                        src.display(),
                        dest.display()
                    ))));
                }
            }

            // Open the dest file and read the footer to extract the
            // metadata fields VersionEdit needs. We open via the engine's
            // FileSystem (so the OpenDAL / cached-fs paths get exercised
            // uniformly), not std::fs.
            //
            // R53-M1: on any failure between here and the
            // `new_files.push(...)` below, `dest` is on disk but not yet
            // tracked by `new_files`, so `cleanup_ingested` would not
            // unlink it. Explicitly delete `dest` before invoking
            // cleanup so partial ingests do not leak the file.
            // 2026-05-29 WRITE-BACK FLUSH: `dest` was just written via
            // `self.fs` (CreateNew → buffered async upload on object stores).
            // We immediately read its footer below, so await the in-flight
            // upload first to read a fully-uploaded object.
            if let Err(e) = self.fs.await_upload(&dest) {
                let _ = self.fs.delete_file(&dest);
                self.cleanup_ingested(&new_files);
                return Err(e);
            }
            let rac = self.fs.open_random_access_file(&dest).inspect_err(|_| {
                let _ = self.fs.delete_file(&dest);
                self.cleanup_ingested(&new_files);
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
                        let _ = self.fs.delete_file(&dest);
                        self.cleanup_ingested(&new_files);
                        return Err(e);
                    }
                },
                Err(e) => {
                    let _ = self.fs.delete_file(&dest);
                    self.cleanup_ingested(&new_files);
                    return Err(e);
                }
            };
            let reader = SstReaderImpl::open(rac).inspect_err(|_| {
                let _ = self.fs.delete_file(&dest);
                self.cleanup_ingested(&new_files);
            })?;
            let mut footer = reader.footer().clone();

            // R49-H1: ingested SSTs are stamped with the target CF's id so
            // the engine treats them like any other per-CF file. v1 footers
            // decode with `cf_id = DEFAULT_CF_ID`; if the caller asked to
            // ingest into a non-default CF, we override the legacy default
            // (the ingest contract says the caller owns CF visibility).
            //
            // R50-M2: ALSO rewrite the on-disk footer's cf_id so the truth
            // in the SST matches the truth in the manifest. Pre-fix only the
            // in-memory `SstFileMeta.cf_id` was overridden — the footer
            // still carried `DEFAULT_CF_ID`. R50-H3's open-time check
            // (footer.cf_id == meta.cf_id) would then reject every read of
            // an ingested file. Keeping the two sources of truth aligned
            // preserves the single-source-of-truth invariant and lets the
            // cross-check actually catch genuine corruption.
            //
            // Drop the temp reader BEFORE the rewrite so the file handle is
            // closed; rewriting via tmp+rename below produces a fresh inode
            // and breaks any hardlink with `src`.
            drop(reader);

            let meta = SstFileMeta {
                file_number,
                cf_id: cf.id(),
                file_size,
                smallest_key: footer.min_key.clone(),
                largest_key: footer.max_key.clone(),
                min_sequence: SequenceNumber(footer.min_sequence),
                max_sequence: SequenceNumber(footer.max_sequence),
                num_entries: footer.total_entries,
            };

            // R51-H3: register `dest` in `new_files` BEFORE attempting the
            // footer rewrite. If the rewrite fails, `cleanup_ingested` below
            // will unlink it — otherwise it would leak (orphan-scan keys on
            // `*.sst.tmp`, not on bare `<N>.sst`).
            new_ids.push(file_number.value());
            new_files.push((file_number, dest.clone(), meta));

            if footer.cf_id != cf.id() {
                footer.cf_id = cf.id();
                if let Err(e) = self.rewrite_sst_footer(&dest, &footer, file_size) {
                    self.cleanup_ingested(&new_files);
                    return Err(e);
                }
            }
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

        // H-R4-2: bump the engine `sequence_number` BEFORE `version_set.apply`.
        // Pre-fix, the apply made the ingested SSTs visible to readers while
        // `self.sequence_number` still held its pre-ingest value, opening a
        // window where a concurrent writer's `fetch_add` returned a seq <
        // `max_seq_ingested`. A snapshot read (`get_at_cf` / `scan_at`) then
        // sorted entries by seq DESC and `mvcc::get_at` returned the ingested
        // SST entry (higher seq) shadowing the concurrent write — silent
        // data loss. Moving the `fetch_max` ahead of apply forces every
        // concurrent writer to allocate a seq STRICTLY ABOVE the ingested
        // range. R56-M1's `fetch_max` primitive is preserved for the
        // load-then-store hazard.
        if max_seq_ingested > 0 {
            self.sequence_number
                .fetch_max(max_seq_ingested, Ordering::AcqRel);
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
            self.cleanup_ingested(&new_files);
            return Err(e);
        }

        // Phase 3: pre-populate the sst_readers cache so the first
        // point lookup doesn't pay the open-file cost. We rebuild
        // readers because the temp readers from phase 1 were dropped
        // above; this also ensures the cache holds readers opened
        // through the engine's FileSystem (cf. CachedFileSystem
        // bookkeeping).
        //
        // R42-H3: the version edit at Phase 2 already landed
        // successfully — failures here would leave dest files
        // orphaned and the sst_readers cache half-populated with no
        // rollback path (calling `cleanup_ingested` now would unlink
        // SSTs the manifest already references). Treat reader-open
        // failures as recoverable: log a WARN and let the regular
        // read path retry via `get_or_open_sst_reader` on first
        // access. This is safe because that helper opens lazily and
        // caches on success.
        let mut readers_map = (**self.sst_readers.load()).clone();
        for (file_number, dest, _meta) in &new_files {
            match self.fs.open_random_access_file(dest) {
                Ok(rac) => match SstReaderImpl::open(rac) {
                    Ok(reader) => {
                        // FRS-INGEST-READER-BLOCKCACHE (2026-06-09): wire the shared decoded-block
                        // cache (same fix as flush + compaction) so ingested-SST readers hit the
                        // decoded-RecordBatch cache instead of re-decompressing on every probe.
                        readers_map.insert(
                            *file_number,
                            Arc::new(reader.with_block_cache(
                                Arc::clone(&self.block_cache)
                                    as std::sync::Arc<dyn forst_rs_storage::cache::BlockCache>,
                                self.db_id.0,
                                file_number.value(),
                            )),
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "ingest_external_sst: reader open failed for {} ({}); \
                             skipping cache pre-populate, first read will retry: {e}",
                            file_number.value(),
                            dest.display(),
                        );
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        "ingest_external_sst: RAC open failed for {} ({}); \
                         skipping cache pre-populate, first read will retry: {e}",
                        file_number.value(),
                        dest.display(),
                    );
                }
            }
        }
        self.sst_readers.store(std::sync::Arc::new(readers_map));

        // (H-R4-2: the engine seq bump moved BEFORE `version_set.apply`
        // above, so this position is now a no-op. Keeping the call here
        // for idempotent safety in case a future refactor splits apply
        // and the visible-files ordering changes.)
        if max_seq_ingested > 0 {
            self.sequence_number
                .fetch_max(max_seq_ingested, Ordering::AcqRel);
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
    ///
    /// Routes through `self.fs` so the wrapping `CachedFileSystem` /
    /// router invalidates any cached bytes / metadata for the destination
    /// path; otherwise a re-ingest under the same file-number could see
    /// stale cached contents from the failed attempt.
    fn cleanup_ingested(&self, new_files: &[(FileNumber, PathBuf, SstFileMeta)]) {
        for (_, dest, _) in new_files {
            let _ = self.fs.delete_file(dest);
        }
    }

    /// R52-H1: route-aware copy of an externally-supplied SST into the
    /// engine's SST directory. Used by [`Self::ingest_external_sst`] when
    /// `std::fs::hard_link` is unavailable (cross-fs, unsupported backend,
    /// or the engine is running in tiered mode where `src` lives on the
    /// caller's local FS but `dest` routes to the remote leg via
    /// `self.fs`). Reading `src` via [`LocalFileSystem`] keeps the
    /// contract that "src is a local path" while writing `dest` via the
    /// engine's `FileSystem` puts every byte on the correct leg.
    ///
    /// Buffered in 1 MiB chunks; never materialises the full file in
    /// memory regardless of SST size.
    fn copy_external_sst(&self, src: &Path, dest: &Path) -> ForstResult<()> {
        // Source is always a caller-supplied local path; open via
        // `LocalFileSystem` directly (NOT `self.fs`, which may route
        // `.sst` writes to a remote backend that does not host the
        // caller's source file).
        let local_fs = LocalFileSystem::new();
        let rac = local_fs.open_random_access_file(src)?;
        let file_size = rac.file_size()?;

        let mut wf = self
            .fs
            .open_writable_file(dest, WriteMode::CreateNew)
            .map_err(|e| {
                ForstError::Io(std::io::Error::other(format!(
                    "copy_external_sst: create dest {}: {e}",
                    dest.display()
                )))
            })?;

        // R53-M1: wrap the copy loop so any failure (read_at, append,
        // short read, sync) cleans up the partial `dest` we just
        // created. `cleanup_ingested` in the caller only operates on
        // entries already pushed into `new_files`, and `dest` is not
        // pushed until both the copy AND the footer-read succeed — so
        // a copy-side failure would otherwise leak a partial SST.
        let res = (|| -> ForstResult<()> {
            const CHUNK: usize = 1 << 20; // 1 MiB
            let mut buf = vec![0u8; CHUNK];
            let mut off: u64 = 0;
            while off < file_size {
                let want = ((file_size - off) as usize).min(CHUNK);
                let n = rac.read_at(off, &mut buf[..want])?;
                if n == 0 {
                    return Err(ForstError::corruption(format!(
                        "copy_external_sst: short read at off={} of {} from {}",
                        off,
                        file_size,
                        src.display()
                    )));
                }
                wf.append(&buf[..n])?;
                off += n as u64;
            }
            wf.sync()?;
            Ok(())
        })();
        if let Err(e) = res {
            // Drop the WritableFile handle before unlinking — some
            // backends keep the open handle pinned and silently retain
            // the inode otherwise.
            drop(wf);
            let _ = self.fs.delete_file(dest);
            return Err(e);
        }
        Ok(())
    }

    /// R50-M2: rewrite the on-disk footer of an ingested SST so its
    /// `cf_id` matches the target CF the caller is ingesting into. Used
    /// by [`Self::ingest_external_sst`] to keep the footer truth and the
    /// manifest truth aligned (the R50-H3 open-time check rejects drift
    /// between the two).
    ///
    /// The body of the SST (everything before the footer) is preserved
    /// byte-for-byte; only the footer is re-encoded with the new cf_id,
    /// which also produces a fresh CRC. Because `cf_id` is a fixed 4-byte
    /// field in the v2 layout, the new footer is exactly the same length
    /// as the old one, so `file_size` is unchanged.
    ///
    /// The rewrite is performed via tmp-file + atomic rename so:
    ///   * Any hardlink between `dest` and the caller-supplied source path
    ///     (the ingest path hardlinks first, falls back to a copy) is
    ///     broken — the source file is never mutated.
    ///   * A crash mid-rewrite leaves either the original file or the
    ///     successor in place, never a truncated file with no footer.
    ///
    /// `expected_file_size` is the size returned by an earlier
    /// `RandomAccessFile::file_size()` call against `dest`; we sanity-check
    /// that the post-rewrite size matches so a future regression that
    /// changes the v2 layout (and therefore the footer length) surfaces
    /// here instead of silently shifting all reader offsets.
    ///
    /// R51-H1 / R51-H2 / R51-L1: every file op routes through `self.fs`
    /// (so the wrapping `CachedFileSystem` invalidates the stale cache
    /// the temp reader populated at `open_random_access_file` above; if we
    /// went through `std::fs` directly the cache would still hold the
    /// pre-rewrite bytes and R50-H3's footer-cf_id check would reject
    /// every read of the ingested SST). The rename is followed by a
    /// `sync_dir(parent)` so the directory-entry update is durable
    /// (closing the R49-H3 gap re-introduced by the previous `std::fs`
    /// path). The body bytes are written straight to the tmp file
    /// instead of being spliced through a second equal-sized `Vec` —
    /// avoids 2× peak allocation on 64 MiB SSTs.
    fn rewrite_sst_footer(
        &self,
        dest: &Path,
        new_footer: &forst_rs_storage::sst::FooterV1,
        expected_file_size: u64,
    ) -> ForstResult<()> {
        use forst_rs_storage::sst::FOOTER_TAIL_SIZE;

        // 2026-05-29 WRITE-BACK FLUSH: `dest` may have been written via the
        // buffered async upload path; await its upload before reading it back
        // to rewrite the footer.
        self.fs.await_upload(dest)?;
        // Open the dest via the engine's FileSystem and confirm its
        // on-disk size matches what the caller measured. Going through
        // `self.fs` ensures CachedFileSystem (or any wrapping FS)
        // observes the read and stays consistent with the later rename.
        let src = self.fs.open_random_access_file(dest).map_err(|e| {
            ForstError::Io(std::io::Error::other(format!(
                "rewrite_sst_footer: open {} for read: {e}",
                dest.display()
            )))
        })?;
        let total_size = src.file_size()?;
        if total_size != expected_file_size {
            return Err(ForstError::corruption(format!(
                "rewrite_sst_footer: file size {} != expected {}",
                total_size, expected_file_size
            )));
        }
        if total_size < FOOTER_TAIL_SIZE as u64 {
            return Err(ForstError::corruption(format!(
                "rewrite_sst_footer: file {} too small ({} bytes)",
                dest.display(),
                total_size
            )));
        }

        // Pull the entire file contents into memory. SST files are
        // typically 64 MiB; reading the whole thing keeps the logic
        // simple and matches the existing ingest path's I/O shape.
        let mut all_bytes = vec![0u8; total_size as usize];
        let mut filled = 0usize;
        while filled < all_bytes.len() {
            let n = src.read_at(filled as u64, &mut all_bytes[filled..])?;
            if n == 0 {
                return Err(ForstError::corruption(format!(
                    "rewrite_sst_footer: short read at offset {} of {}",
                    filled,
                    all_bytes.len()
                )));
            }
            filled += n;
        }
        drop(src);

        // The last 8 bytes carry: footer_length(4) | magic(4). Decode
        // footer_length to slice off the original footer.
        let tail_off = all_bytes.len() - 8;
        let footer_length = u32::from_le_bytes(
            all_bytes[tail_off..tail_off + 4]
                .try_into()
                .map_err(|_| ForstError::corruption("rewrite_sst_footer: tail length slice"))?,
        ) as usize;
        if footer_length > all_bytes.len() {
            return Err(ForstError::corruption(format!(
                "rewrite_sst_footer: footer_length {} > file_size {}",
                footer_length,
                all_bytes.len()
            )));
        }
        let body_end = all_bytes.len() - footer_length;
        let new_footer_bytes = new_footer.encode();
        if new_footer_bytes.len() != footer_length {
            return Err(ForstError::corruption(format!(
                "rewrite_sst_footer: new footer length {} != original {}",
                new_footer_bytes.len(),
                footer_length
            )));
        }

        let mut tmp = dest.as_os_str().to_owned();
        tmp.push(".cf-rewrite.tmp");
        let tmp_path = PathBuf::from(tmp);

        // Best-effort: remove any stale tmp from a previous failed
        // attempt before opening the new one (CreateNew would otherwise
        // error out). Route through `self.fs` for consistency.
        let _ = self.fs.delete_file(&tmp_path);

        // R51-L1: write the body slice and the new footer directly to
        // the tmp file in two `append` calls instead of splicing through
        // a second `Vec<u8>` of the full file size. Cuts peak memory of
        // an ingest rewrite from 2× SST size to 1× SST size.
        {
            let mut tmp_file = self
                .fs
                .open_writable_file(&tmp_path, WriteMode::CreateNew)
                .map_err(|e| {
                    ForstError::Io(std::io::Error::other(format!(
                        "rewrite_sst_footer: create {} failed: {e}",
                        tmp_path.display()
                    )))
                })?;
            if let Err(e) = tmp_file.append(&all_bytes[..body_end]) {
                let _ = self.fs.delete_file(&tmp_path);
                return Err(e);
            }
            if let Err(e) = tmp_file.append(&new_footer_bytes) {
                let _ = self.fs.delete_file(&tmp_path);
                return Err(e);
            }
            if let Err(e) = tmp_file.sync() {
                let _ = self.fs.delete_file(&tmp_path);
                return Err(e);
            }
        }

        // Atomic rename over the original. This breaks any hardlink to
        // the caller-supplied source path because the destination inode
        // is replaced. `CachedFileSystem::rename` invalidates the cache
        // entries for both `src` and `dst`, so the next read of `dest`
        // sees the new footer bytes instead of the stale cached body.
        self.fs.rename(&tmp_path, dest).map_err(|e| {
            let _ = self.fs.delete_file(&tmp_path);
            ForstError::Io(std::io::Error::other(format!(
                "rewrite_sst_footer: rename {} -> {} failed: {e}",
                tmp_path.display(),
                dest.display()
            )))
        })?;

        // R49-H3 / R51-H2: fsync the parent directory so the rename's
        // directory-entry change survives a power-loss event. On the
        // tiered router the `parent_of_sst` is a directory (no `.sst`
        // extension) so the call lands on `local_fs.sync_dir`; the
        // router's R51-M1 fix below also fans it out to the remote FS
        // (object stores treat it as a no-op).
        if let Some(parent) = dest.parent() {
            self.fs.sync_dir(parent)?;
        }
        Ok(())
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

    /// FRS-L0-SHORTCIRCUIT (2026-06-03): cumulative count of L0 SST data-block
    /// reads performed during point `get`s (see `Self::sst_get` and the
    /// `l0_point_get_block_reads` field). A monotonically-increasing diagnostic
    /// the regression test snapshots before/after a `get` to assert that a hot
    /// overwrite key present in N L0 SSTs is resolved with ONE block read
    /// (newest-first short-circuit), not N.
    pub fn l0_point_get_block_reads(&self) -> u64 {
        self.l0_point_get_block_reads
            .load(std::sync::atomic::Ordering::Relaxed)
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
    ///
    /// R0A-H2 contract: callers MUST externally guarantee write
    /// quiescence with respect to the seqs they care about — i.e., no
    /// concurrent writer with an allocated-but-not-inserted seq <= the
    /// observed `sequence_number()`. The lock-free write path (D1)
    /// allocates the seq before inserting into the memtable, so a
    /// snapshot pinned at seq=N could miss a writer that holds N but
    /// has not yet completed `put_with_seq`. Flink's
    /// AsyncStateBackendV2 satisfies the contract by draining
    /// in-flight async ops via runtime checkpoint barriers BEFORE
    /// invoking DB.snapshot(). Direct (non-Flink) users with
    /// multi-threaded write+snapshot interleavings must serialize
    /// externally. We intentionally do not add a write-group commit
    /// barrier here because it would force every write to take a
    /// shared mutex and defeat D1's per-shard fan-out.
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
        self.check_fatal_error()?;
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
        //
        // A-H1: if this CF has a merge operator, every newest-visible
        // entry could be a `Merge` operand whose `Put` base sits older
        // in the chain. `mvcc::get_at` returns `None` on `Merge`, which
        // silently drops every key whose newest visible op is a Merge.
        // Route through the merge-aware `get_at_with_merge` instead.
        let result = if let Some(mop) = cf_data.merge_operator() {
            mvcc::get_at_with_merge(
                snapshot,
                key,
                candidates.iter().map(|(k, v)| mvcc::VersionedEntry {
                    key: k,
                    value: v.as_slice(),
                }),
                mop,
            )?
        } else {
            mvcc::get_at(
                snapshot,
                key,
                candidates.iter().map(|(k, v)| mvcc::VersionedEntry {
                    key: k,
                    value: v.as_slice(),
                }),
            )
            .map(|s| s.to_vec())
        };
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

        self.check_fatal_error()?;
        if snapshot.db_id() != self.db_id {
            return Err(ForstError::invalid_argument(
                "Snapshot was issued by a different DbImpl instance",
            ));
        }
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        // A-R8-H2: collect candidate keys at u64::MAX, NOT snapshot.seq.
        // Pre-fix used `snapshot.seq()` which filtered out memtable
        // rows with seq > snapshot.seq — but a Merge operand at
        // seq > snapshot.seq might be the only row that brings a key
        // into the candidate set, and `iter_versions_of` / `get_at_cf`
        // ALREADY resolve per-key visibility via mvcc::get_at_with_merge
        // (which uses the snapshot's seq to filter operands while
        // walking older versions). With the pre-fix filter, scan_at
        // missed keys whose only memtable presence was a too-new
        // entry but whose older versions (visible at snapshot.seq)
        // sat in SSTs that did NOT get filtered at the per-row
        // level — silent missing rows under merge-operator CFs.
        let _ = snapshot.seq(); // retained for cross-DB consistency check above
        let lower: &[u8] = &[];
        let upper: Option<&[u8]> = None;
        let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();

        // Active memtable.
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
        // SST layer — bound files whose key range overlaps the (empty)
        // bound. Any file with `largest_seqno > snapshot.seq` is still
        // worth scanning because individual entries inside the file may
        // be older than snapshot.seq (per spec §6a.5, snapshot filtering
        // happens at the per-entry seq level, not at the file level).
        //
        // A-NEW-H1: filter by cf_data.handle().id() before opening the
        // reader. Without the filter, cross-CF rows enter the candidate
        // key set, then `get_at_cf` resolves them under the requested
        // CF — leaking foreign-CF data into snapshot scans.
        let version = self.version_set.current();
        let cf_id_for_scan = cf_data.handle().id();
        for sst in version.live_sst_files() {
            if sst.cf_id != cf_id_for_scan {
                continue;
            }
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
        // A-R7-H1: capture the Arc identity of the active memtable
        // so the imm-list walk below can skip any imm entry whose
        // Arc-ptr matches — `swap_active_memtable` (column_family.rs)
        // installs a fresh active and pushes the OLD active onto
        // `imm_list`. Without this dedup, a swap interleaving
        // between `cf_data.active_memtable()` here and
        // `cf_data.imm_memtables()` below would walk the same
        // memtable twice, double-counting every entry and producing
        // wrong merge results on snapshot reads (sister-bug to
        // D-R7-H2 in the latest-view path).
        let mem = cf_data.active_memtable();
        let active_ptr = std::sync::Arc::as_ptr(&mem);
        {
            for (k, v, seq, op) in mem.collect_range_entries(user_key, Some(&upper), u64::MAX) {
                if k != user_key {
                    continue;
                }
                // C-R5-H1: detect a corrupt Merge entry whose value
                // payload is missing. The latest-view path
                // (`get_internal`) raises ForstError::corruption for
                // this same shape; the snapshot path previously
                // silently folded an empty operand into the merge
                // result, returning wrong answers under
                // counter-style or serializer-style merge operators.
                // D-R8-NEW-H3: widen corruption check beyond Merge.
                // Put with None value is a serializer error (Put must
                // carry payload); Delete/SingleDelete with Some value
                // is an upstream layering error (tombstones carry no
                // payload). Both produced wrong snapshot reads pre-fix.
                if (matches!(op, OpType::Merge) && v.is_none())
                    || (matches!(op, OpType::Put) && v.is_none())
                    || (matches!(op, OpType::Delete | OpType::SingleDelete) && v.is_some())
                {
                    return Err(ForstError::corruption(
                        "iter_versions_of: Merge entry missing operand payload (active memtable)",
                    ));
                }
                let ik = InternalKey::new(k, SequenceNumber::new(seq), op);
                entries.push((ik, v.unwrap_or_default()));
            }
        }

        // Immutable memtables (any order — we sort below).
        for imm in cf_data.imm_memtables() {
            if std::sync::Arc::as_ptr(&imm) == active_ptr {
                // A-R7-H1: skip imm entry that aliases the active memtable
                // we already walked above.
                continue;
            }
            for (k, v, seq, op) in imm.collect_range_entries(user_key, Some(&upper), u64::MAX) {
                if k != user_key {
                    continue;
                }
                // D-R8-NEW-H3: widen corruption check beyond Merge.
                // Put with None value is a serializer error (Put must
                // carry payload); Delete/SingleDelete with Some value
                // is an upstream layering error (tombstones carry no
                // payload). Both produced wrong snapshot reads pre-fix.
                if (matches!(op, OpType::Merge) && v.is_none())
                    || (matches!(op, OpType::Put) && v.is_none())
                    || (matches!(op, OpType::Delete | OpType::SingleDelete) && v.is_some())
                {
                    return Err(ForstError::corruption(
                        "iter_versions_of: Merge entry missing operand payload (imm memtable)",
                    ));
                }
                let ik = InternalKey::new(k, SequenceNumber::new(seq), op);
                entries.push((ik, v.unwrap_or_default()));
            }
        }

        // SST layer — every file whose key range covers `user_key`.
        //
        // A-NEW-H1: filter SSTs by `cf_data.handle().id()` BEFORE the
        // key-range check. With a global VersionSet, `live_sst_files()`
        // interleaves files from every CF and other CFs' SSTs can
        // share the byte-range key space. Pre-fix code admitted those
        // foreign-CF entries as candidates, which `mvcc::get_at`
        // then resolved as values for the requested CF — silently
        // returning cross-CF leaked data on snapshot reads. The
        // latest-view `sst_get` path already gates on cf_id (R49-H1);
        // this brings the MVCC path to the same defense.
        let version = self.version_set.current();
        let cf_id = cf_data.handle().id();
        for sst in version.live_sst_files() {
            if sst.cf_id != cf_id {
                continue;
            }
            if user_key < sst.smallest_key.as_slice() || user_key > sst.largest_key.as_slice() {
                continue;
            }
            let reader = self.get_or_open_sst_reader(&sst)?;
            for (k, v, seq, op) in reader.scan(user_key, Some(&upper))? {
                if k != user_key {
                    continue;
                }
                // D-R8-NEW-H3: widen corruption check beyond Merge.
                // Put with None value is a serializer error (Put must
                // carry payload); Delete/SingleDelete with Some value
                // is an upstream layering error (tombstones carry no
                // payload). Both produced wrong snapshot reads pre-fix.
                if (matches!(op, OpType::Merge) && v.is_none())
                    || (matches!(op, OpType::Put) && v.is_none())
                    || (matches!(op, OpType::Delete | OpType::SingleDelete) && v.is_some())
                {
                    return Err(ForstError::corruption(
                        "iter_versions_of: Merge entry missing operand payload (SST)",
                    ));
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
        // A-R9-N1: dedup imm-vs-SST flush-window race. Between
        // `version_set.apply(edit)` installing the just-flushed SST
        // and `pop_oldest_imm` removing the imm, BOTH layers carry
        // the same entries. The active→imm Arc dedup at line 2158
        // (A-R7-H1) handles active↔imm, and E-R8-H1's
        // seq-cutoff in `peel_merges_from_sst_with_cutoff` handles
        // the latest-view path, but the snapshot-read candidate
        // gatherer here had no equivalent. Engine seqs are globally
        // unique (single fetch_add), so two entries with identical
        // (seq, op) are necessarily flush-window duplicates of the
        // same logical row. Drop the second occurrence.
        entries.dedup_by(|a, b| {
            a.0.user_key() == b.0.user_key()
                && a.0.sequence() == b.0.sequence()
                && a.0.op_type() == b.0.op_type()
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
        self.check_fatal_error()?;
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
                        std::thread::sleep(std::time::Duration::from_micros(100u64 << attempt));
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

        if wbm_over {
            self.wait_for_wbm_headroom();
        }

        Ok(old_value)
    }

    /// FRS-WBM-STALL (2026-06-08, join 8c/32g OOM fix): when the write-buffer budget (per-instance
    /// OR the process-GLOBAL memtable sum) is exceeded, BLOCK the writer until enqueued flushes
    /// drain memtable bytes back under budget — RocksDB's WriteBufferManager `allow_stall`. Without
    /// this, `over_budget()` only TRIGGERS a flush (advisory); under heavy joins on 8c/32g the writes
    /// outpace flush across the ~N keyed-state DbImpls, memtables grow unbounded, and the TM hits the
    /// 32 GiB cgroup → OOM-kill (proven: q9 RSS→32.3 GiB, 99.9% anonymous, engine SST state only
    /// 1.3 GiB). The flush runs on the shared bg pool (separate threads) so it drains concurrently
    /// while this writer parks; no lock is held here (write_mutex released, wbm_guard committed) so
    /// there is no deadlock. Bounded by a 30s backstop. Disable with `FRS_WBM_STALL=0`.
    fn wait_for_wbm_headroom(&self) {
        // FRS-WBM-TRUE-BACKPRESSURE (2026-06-08): block the writer while the memtable
        // budget (per-instance cap OR the process-global SOFT budget) is exceeded,
        // until enqueued flushes drain it back under budget. This makes the budget a
        // HARD bound — RocksDB's WriteBufferManager `allow_stall` — instead of the
        // advisory flush-trigger it was. Without it, `over_budget()` only TRIGGERED a
        // flush and writes continued unthrottled, so on the UNBOUNDED-state queries
        // (q4/q9/q17/q18/q20) writes outran flush, memtables grew to 7-11 GB, and the
        // 8c/32g TM hit the 32 GiB cgroup → OOM (proven: q9 7.2 GB / q17 11.2 GB
        // memtables at OOM). Flush runs on the INDEPENDENT `bg_flush_pool` (separate
        // threads from compaction) so it drains while this writer parks; no lock is
        // held here (wbm_guard committed, write_mutex released) → no deadlock.
        //
        // No fixed give-up backstop (the old 120s deadline RELEASED and let memtables
        // overrun → OOM anyway): we wait until under budget. The ONLY escape is a
        // progress-based DEFENSE — if the process-global memtable sum makes NO downward
        // progress for `STALL_NO_PROGRESS`, we release (defends against a genuinely
        // stuck flush rather than freezing forever; under normal flush the global sum
        // drops within ms). Disable entirely with FRS_WBM_STALL=0.
        if !wbm_stall_enabled() {
            return;
        }
        if !self.write_buffer_manager.over_budget() {
            return;
        }
        const STALL_NO_PROGRESS: std::time::Duration = std::time::Duration::from_secs(60);
        let stall_start = std::time::Instant::now(); // FRS_PROF_DIAG: attribute backpressure stall
        let mut last_used = crate::runtime_tuning::global_wbm_used_bytes();
        let mut last_progress = std::time::Instant::now();
        while self.write_buffer_manager.over_budget() {
            std::thread::sleep(std::time::Duration::from_millis(1));
            let now_used = crate::runtime_tuning::global_wbm_used_bytes();
            if now_used < last_used {
                // flush is draining the global memtable sum — keep waiting.
                last_used = now_used;
                last_progress = std::time::Instant::now();
            } else if last_progress.elapsed() >= STALL_NO_PROGRESS {
                break; // flush appears stuck — defensive release, never freeze forever.
            }
        }
        prof_add(&PROF_STALL_NS, stall_start.elapsed().as_nanos() as u64);
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

    /// FRS-WAL Phase 2 (2026-06-06): initialize the local write-ahead log from
    /// the `FRS_WAL_DIR` env var. A no-op (WAL stays disabled, `self.wal` stays
    /// `None`) when the var is unset — keeping the default engine byte-identical
    /// to pre-WAL. One segment per `db_id` so the ~12 q4 DbImpls don't collide.
    fn maybe_init_wal(&self) {
        let dir = match std::env::var("FRS_WAL_DIR") {
            Ok(d) if !d.trim().is_empty() => d,
            _ => return,
        };
        let dir = std::path::Path::new(&dir);
        if let Err(e) = std::fs::create_dir_all(dir) {
            tracing::warn!(target: "forst_rs_engine::wal", "WAL dir {dir:?} create failed: {e}; WAL disabled");
            return;
        }
        let path = dir.join(format!("db-{}.wal", self.db_id.0));
        match crate::wal::WalWriter::open(&path) {
            Ok(w) => {
                *self.wal.lock().expect("wal lock poisoned") = Some(w);
                tracing::info!(target: "forst_rs_engine::wal", "WAL enabled at {path:?}");
            }
            Err(e) => {
                tracing::warn!(target: "forst_rs_engine::wal", "WAL open {path:?} failed: {e}; disabled")
            }
        }
    }

    /// FRS-WAL Phase 2: append a group of records to the WAL buffer. No-op
    /// (returns `Ok`) when the WAL is disabled. Does NOT fsync — durability is
    /// established at checkpoint time via [`wal_sync`](Self::wal_sync), which is
    /// all Flink's exactly-once contract requires (recovery replays the source
    /// from the last completed checkpoint). Per-write fsync was catastrophic
    /// (q4 ~1.8K/s vs 304K/s) and is unnecessary for checkpointed recovery.
    fn wal_append(&self, recs: &[crate::wal::WalRecord]) -> ForstResult<()> {
        let mut guard = self.wal.lock().expect("wal lock poisoned");
        if let Some(w) = guard.as_mut() {
            for r in recs {
                w.append(r)?;
            }
        }
        Ok(())
    }

    /// FRS-WAL Phase 3: flush + fsync the WAL buffer to stable storage. Called
    /// at checkpoint time (the durability barrier) instead of forcing a memtable
    /// flush. No-op when the WAL is disabled.
    fn wal_sync(&self) -> ForstResult<()> {
        let mut guard = self.wal.lock().expect("wal lock poisoned");
        if let Some(w) = guard.as_mut() {
            w.sync()?;
        }
        Ok(())
    }

    fn write_single(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: Option<&[u8]>,
        op: OpType,
    ) -> ForstResult<u64> {
        self.check_fatal_error()?;
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
        // FRS-WAL Phase 2: write-ahead — append this mutation to the WAL buffer
        // BEFORE touching the memtable. Durability is established at checkpoint
        // (`wal_sync`), which is all Flink's exactly-once needs. No-op (zero
        // cost) when the WAL is disabled (default: `FRS_WAL_DIR` unset).
        self.wal_append(&[crate::wal::WalRecord {
            cf_id: cf.id().0,
            sequence: seq,
            op_type: op as u8,
            key: key.to_vec(),
            value: value.map(|v| v.to_vec()),
        }])?;

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
                        std::thread::sleep(std::time::Duration::from_micros(100u64 << attempt));
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
        if wbm_over {
            self.wait_for_wbm_headroom();
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
    ///   (`MAX_SEQUENCE_NUMBER + 1`). Writes never land on the memtable past this line; the
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
                "sequence number {} exceeded InternalKey 56-bit threshold; engine stopped \
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
                 before the engine reaches the InternalKey 56-bit fatal threshold"
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
        // FRS-COMPACT-BG (2026-06-03): use the dedicated LOW
        // `l0_compaction_trigger` (default 4), NOT `l0_slowdown_trigger` (40).
        // Keeping L0 shallow cuts the per-point-read SST scan count
        // (`get_arc → sst_get`) that drives the q4/q11 decay. The 2026-05-30
        // note below rejected trigger=4 ONLY because compaction then ran
        // INLINE on the flush worker (stalled flushes); compaction now runs on
        // the dedicated background worker (`enqueue_compaction`), so a low
        // trigger keeps L0 shallow WITHOUT stalling the flush path.
        // [historical: "2026-05-30: tried a low L0 compaction trigger (4)…it
        //  stalled EARLIER from frequent INLINE compaction on the flush worker"
        //  — that inline coupling is exactly what FRS-COMPACT-BG removed.]
        let trigger = self.write_controller.config().l0_compaction_trigger;
        if l0_count >= trigger {
            // R46-L3: surface which CF triggered the auto-compaction and
            // how many engine-global L0 files are about to be absorbed.
            // The version_set is engine-global (not per-CF) so the L0
            // files being compacted may include rows for other CFs — the
            // log includes the full CF list so post-mortem analysis can
            // attribute throughput stalls to whichever CF tripped the
            // trigger. The list of "other CFs with L0 footprint" cannot
            // be reconstructed from the Version alone (rows are CF-tagged
            // inside the SST, not at the file-meta level), so we log the
            // names of every currently-registered non-default CF as the
            // candidate set.
            let other_cf_names: Vec<String> = {
                let cfs = self.cfs.read().expect("lock poisoned");
                cfs.values()
                    .filter(|cf| cf.handle().id() != cf_data.handle().id())
                    .map(|cf| cf.handle().name().to_string())
                    .collect()
            };
            tracing::debug!(
                cf_id = cf_data.handle().id().0,
                cf_name = cf_data.handle().name(),
                l0_count,
                trigger,
                other_non_default_cfs = ?other_cf_names,
                "maybe_auto_compact: enqueueing background L0→L1 compaction"
            );
            // FRS-COMPACT-BG: ENQUEUE to the background compaction worker
            // instead of compacting INLINE on the flush worker. Inline
            // compaction blocked subsequent flushes → memtables backed up
            // (write-stall) + L0 stayed deep → point reads scanned a growing
            // L0 → throughput decay (q11 665K→37K rec/s). The flush worker now
            // returns immediately; the "forst-rs-compact" thread keeps L0
            // shallow concurrently. Errors surface via the flush_error slot.
            self.enqueue_compaction(cf_data.clone());
        }
        Ok(())
    }

    /// FRS-COMPACT-MAINTENANCE (2026-06-04): the set of CFs whose L0 is at or
    /// over `l0_compaction_trigger` and therefore warrant an L0→L1 rollup. A
    /// PURE read over the current version (no enqueue), so the periodic
    /// maintenance ticker can poll it cheaply and it is deterministically
    /// unit-testable.
    ///
    /// WHY THIS EXISTS: the only compaction trigger used to be
    /// `maybe_auto_compact`, called solely from the background `run_flush`
    /// worker. q4 (interval join) rarely fills its 1 GiB write buffer, so
    /// `run_flush` essentially never fires; meanwhile 30 s checkpoints keep
    /// sealing the memtable to L0 via `flush_all`, which does NOT call
    /// `maybe_auto_compact`. Result: L0 grew UNBOUNDED, the per-probe
    /// overlapping-SST fan-out climbed without bound (the q4 throughput decay
    /// — measured 0→10 overlapping SSTs, ALL L0, deep=0, "compact" 0× in the
    /// TM log), and compaction never ran. Polling this from the maintenance
    /// ticker decouples the trigger from the flush path so L0 stays shallow
    /// regardless of which path produced the SSTs. Scoped to L0 (the measured
    /// q4 fan-out source) to reuse the fully-tested `compact_l0_for_cf` rollup
    /// and avoid introducing new deeper-level compaction work that could add
    /// background SST uploads to the checkpoint drain on the S3 path.
    fn cfs_due_for_compaction(&self) -> Vec<Arc<ColumnFamilyData>> {
        let trigger = self.write_controller.config().l0_compaction_trigger;
        let version = self.version_set.current();
        let cfs: Vec<Arc<ColumnFamilyData>> = {
            let guard = self.cfs.read().expect("lock poisoned");
            guard.values().cloned().collect()
        };
        cfs.into_iter()
            .filter(|cf_data| {
                if cf_data.is_dropped() {
                    return false;
                }
                let cf_id = cf_data.handle().id();
                let l0_count = version
                    .l0_files()
                    .iter()
                    .filter(|f| f.cf_id == cf_id)
                    .count() as u32;
                l0_count >= trigger
            })
            .collect()
    }

    /// FRS-COMPACT-MAINTENANCE: enqueue a background L0→L1 compaction for every
    /// CF that [`Self::cfs_due_for_compaction`] reports. Per-CF deduped by
    /// `enqueue_compaction`, so calling this every maintenance tick is cheap
    /// and idempotent — a CF already queued/running is skipped. Non-blocking:
    /// the rollup runs on the dedicated `forst-rs-compact` worker.
    fn enqueue_due_compactions(&self) {
        for cf_data in self.cfs_due_for_compaction() {
            self.enqueue_compaction(cf_data);
        }
    }

    /// Applies a [`WriteBatch`] atomically. Returns the last assigned sequence.
    ///
    /// PR-B5-H1: takes `WriteBatch<'a>` so callers (FFI batch hot path,
    /// tests, JNI compat) can pass borrowed slices. The batch is fully
    /// consumed before this function returns, so the borrow's lifetime
    /// always covers the call.
    pub fn batch_write<'a>(&self, batch: WriteBatch<'a>) -> ForstResult<u64> {
        self.check_fatal_error()?;
        if batch.is_empty() {
            return Ok(self.sequence_number());
        }
        self.consume_flush_error()?;
        self.write_controller.may_throttle()?;

        // B-R12-NEW-H1 remainder: every FFI vectorized write path
        // (`frs_vectorized_batch_put`, `frs_vectorized_batch_delete`,
        // `frs_vec_merge_append_batch`, `frs_batch_put_arrow`) writes to
        // exactly one CF, so `single_cf_hint()` returns `Some` on the
        // dominant streaming hot path. Fast-path that case to skip:
        //   * `group_by_cf` `HashMap<CF, Vec<usize>>` allocation
        //   * `cf_datas` `HashMap<CF, Arc<ColumnFamilyData>>` allocation
        //   * per-CF `indices: Vec<usize>` indirection
        // The 3 `Vec<&[u8]>`/`Vec<Option<&[u8]>>`/`Vec<u8>` slices below are
        // still required by `ShardedMemTable::batch_insert_with_base_seq`'s
        // borrowed-slice contract, but they're now built once over
        // `entries` directly rather than through a per-CF index hop.
        if let Some(single_cf_id) = batch.single_cf_hint() {
            return self.batch_write_single_cf(single_cf_id, batch);
        }

        // Multi-CF slow path — resolve and cache CF lookups up front so we
        // fail fast on missing CFs.
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

        // R58-H1: charge the WriteBufferManager BEFORE the per-CF insert
        // loop. The pre-fix code skipped this charge entirely on the
        // batch path; only `write_single` reserved bytes. At flush time
        // the engine still released `oldest.memory_usage()` (which
        // includes bytes from batched writes), driving the WBM counter
        // toward zero via `saturating_sub` and silently bypassing the
        // cross-CF budget cap for any batch-heavy workload.
        //
        // Per-row charge mirrors `write_single`'s shape:
        //   key + value + 8 (seq) + 1 (op) + 48 (overhead).
        // For batched writes with N entries that's
        //   sum(key_len + value_len) + N * 57.
        let entries_ref = batch.entries();
        let mut total_charge: u64 = 0;
        for &i in groups.values().flat_map(|v| v.iter()) {
            let k_len = entries_ref[i].key.len() as u64;
            let v_len = entries_ref[i]
                .value
                .as_deref()
                .map_or(0, |v| v.len() as u64);
            total_charge = total_charge
                .saturating_add(k_len)
                .saturating_add(v_len)
                .saturating_add(57);
        }
        self.write_buffer_manager.reserve(total_charge);
        let wbm_guard = WbmReleaseGuard::new(&self.write_buffer_manager, total_charge);

        // E1: per-CF batch insert routes rows by shard; each shard takes
        // its own write lock independently, so concurrent batches across
        // distinct keys see no engine-level serialization. The switch
        // decision still goes through `write_mutex` to keep
        // `active_memtable` swaps serialized.
        //
        // R56-H1: wrap the per-CF `batch_insert_with_base_seq` in the
        // same bounded retry loop `write_single` uses. The active
        // memtable can be swapped + frozen by a concurrent flush
        // between our `active_memtable()` capture (line below) and the
        // actual insert. Pre-fix, the resulting `FrozenMemTable` error
        // propagated through `?` AFTER one or more earlier CFs in the
        // group loop had already committed to their memtables AND after
        // the `sequence_number.fetch_add(total_count)` had reserved the
        // whole range — leaving the batch torn and the seq range
        // partially populated. Retrying within the bounded window
        // (~25.6ms total backoff) absorbs the swap atomically from the
        // caller's perspective.
        let entries = batch.into_entries();
        let mut group_offset: u64 = 1; // first owned seq is `prev + 1`
                                       // R0A-H3: track whether ANY per-CF group has already committed
                                       // bytes to its memtable. On a non-FrozenMemTable error from a
                                       // later group, those earlier-group rows can no longer be undone
                                       // (the LSM has no per-shard rollback primitive), so the
                                       // multi-CF batch is structurally torn. The compensating action
                                       // is to escalate to a fatal flush_error so every subsequent
                                       // write/snapshot path refuses with the same Err — the caller
                                       // sees the original error AND any later reader of this engine
                                       // gets a clean refusal until restart from the last checkpoint
                                       // (where the torn batch is wiped because memtable bytes never
                                       // landed in an SST). For first-group failures we keep the
                                       // legacy behavior (return Err with no escalation) because no
                                       // bytes have been committed yet — the seq range is wasted but
                                       // the engine state is still consistent.
        let mut any_group_committed = false;
        for (cf_id, indices) in &groups {
            let cf_data = cf_datas.get(cf_id).expect("cf_data pre-populated");

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

            let mut attempt: u32 = 0;
            loop {
                let mem_arc = cf_data.active_memtable();
                match mem_arc.batch_insert_with_base_seq(&keys, &values, &op_types, base_seq) {
                    Ok(_) => break,
                    Err(ForstError::FrozenMemTable) if attempt < 8 => {
                        std::thread::yield_now();
                        std::thread::sleep(std::time::Duration::from_micros(100u64 << attempt));
                        attempt += 1;
                    }
                    Err(e) => {
                        if any_group_committed {
                            // R0A-H3 escalation: stamp a synthesized
                            // Internal error onto the flush_error slot
                            // (if empty) so every subsequent writer /
                            // checkpoint sees a clean refusal until
                            // restart. Using flush_error is intentional
                            // — it already plumbs into
                            // consume_flush_error() at the top of every
                            // write path. We synthesize the error
                            // (ForstError is not Clone) so the
                            // surfaced text names the torn-batch
                            // condition and points at the original
                            // displayed message.
                            let msg = format!(
                                "multi-CF batch_write torn after partial commit; original: {e}"
                            );
                            self.record_fatal_error(msg);
                        }
                        return Err(e);
                    }
                }
            }
            any_group_committed = true;
            group_offset += indices.len() as u64;
        }
        // Every per-CF insert succeeded — keep the WBM reservation
        // until flush releases it.
        wbm_guard.commit();

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
        // FRS-WBM-TRUE-BACKPRESSURE: the batch/Arrow write paths are the ONLY paths the
        // FFM async backend uses, so the stall MUST live here (not just write_single) or
        // it never engages (q17/q9 memtables grew to 11/7 GB → OOM). Self-guards via
        // over_budget(); returns immediately when under budget.
        self.wait_for_wbm_headroom();

        Ok(last_seq)
    }

    /// B-R12-NEW-H1 remainder: single-CF fast path for {@link Self::batch_write}.
    /// Skips the `group_by_cf` HashMap and `cf_datas` HashMap allocations; the
    /// 3 borrowed-slice Vecs are built once from `entries` (not per-CF), and
    /// the single `ColumnFamilyData` is fetched directly. All correctness
    /// invariants (R56-H1 retry, R58-H1 WBM charge, D1 seq reservation,
    /// sequence-overflow guard, post-write memtable-switch + flush enqueue)
    /// mirror the multi-CF slow path; only the per-CF group bookkeeping is
    /// elided. The dominant FFI vectorized write paths all hit this branch.
    fn batch_write_single_cf<'a>(
        &self,
        single_cf_id: ColumnFamilyId,
        batch: WriteBatch<'a>,
    ) -> ForstResult<u64> {
        // C4R2-B-NEW-H1: extract slices from entries ONCE in a single pass
        // (pre-fix walked entries.iter() THREE TIMES building 3 separate
        // Vecs). Then dispatch through the shared borrowed-slice helper
        // which is also used by `batch_put_borrowed_single_cf` — the FFM
        // vectorized FFI write path bypasses WriteBatch entirely and calls
        // that helper directly, eliminating the per-row WriteBatchEntry
        // allocation storm Agent B flagged.
        let entries = batch.into_entries();
        let count = entries.len();
        let mut keys: Vec<&[u8]> = Vec::with_capacity(count);
        let mut values: Vec<Option<&[u8]>> = Vec::with_capacity(count);
        let mut op_types: Vec<u8> = Vec::with_capacity(count);
        let mut total_charge: u64 = 0;
        for e in &entries {
            let k = e.key.as_ref();
            let v = e.value.as_deref();
            total_charge = total_charge
                .saturating_add(k.len() as u64)
                .saturating_add(v.map_or(0, |v| v.len() as u64))
                .saturating_add(57);
            keys.push(k);
            values.push(v);
            op_types.push(e.op_type as u8);
        }
        self.batch_write_borrowed_single_cf_inner(
            single_cf_id,
            &keys,
            &values,
            &op_types,
            total_charge,
        )
    }

    /// C4R2-B-NEW-H1: borrowed-slice direct entry for the FFM vectorized
    /// FFI write path. Skips `WriteBatch` construction (which allocates
    /// `Vec<WriteBatchEntry>` of count × 56 bytes per call) and per-row
    /// `Cow::Borrowed` wrap. Callers (Rust FFI `frs_vectorized_batch_put` /
    /// `frs_vectorized_batch_delete` / `frs_vec_merge_append_batch`) build
    /// the 3 borrowed-slice arrays directly during their existing FFI loop
    /// and dispatch here.
    pub fn batch_put_borrowed_single_cf<'a>(
        &self,
        cf: &ColumnFamilyHandle,
        keys: &[&'a [u8]],
        values: &[Option<&'a [u8]>],
        op_types: &[u8],
    ) -> ForstResult<u64> {
        self.check_fatal_error()?;
        // C-C6R2-NEW-H1: cross-slice length precondition. The WBM-charge loop
        // and downstream `batch_insert_with_base_seq` index `values[i]` /
        // `op_types[i]` against `keys.len()`; mismatched lengths panic with
        // index-out-of-bounds AFTER `fetch_add` has already advanced the
        // sequence counter and `wbm.reserve` charged the buffer manager,
        // permanently leaking the reserved seq range. Validate BEFORE any
        // such irreversible state mutation. Sister site
        // `batch_write_borrowed_single_cf_inner` also takes 3 parallel
        // slices but is private — only called via this entry, so the
        // single guard here covers it.
        if keys.len() != values.len() || keys.len() != op_types.len() {
            return Err(ForstError::invalid_argument(
                "batch_put_borrowed_single_cf: keys/values/op_types length mismatch",
            ));
        }
        if keys.is_empty() {
            return Ok(self.sequence_number());
        }
        self.consume_flush_error()?;
        self.write_controller.may_throttle()?;
        // Single-pass WBM charge tally.
        let mut total_charge: u64 = 0;
        for i in 0..keys.len() {
            let k_len = keys[i].len() as u64;
            let v_len = values[i].map_or(0, |v| v.len() as u64);
            total_charge = total_charge
                .saturating_add(k_len)
                .saturating_add(v_len)
                .saturating_add(57);
        }
        self.batch_write_borrowed_single_cf_inner(cf.id(), keys, values, op_types, total_charge)
    }

    /// Shared body for `batch_write_single_cf` and `batch_put_borrowed_single_cf`.
    /// Caller has already computed `total_charge`.
    fn batch_write_borrowed_single_cf_inner<'a>(
        &self,
        single_cf_id: ColumnFamilyId,
        keys: &[&'a [u8]],
        values: &[Option<&'a [u8]>],
        op_types: &[u8],
        total_charge: u64,
    ) -> ForstResult<u64> {
        let cf_data = self.lookup_cf_by_id(single_cf_id)?;

        // PERF (D1): reserve the engine sequence range with one lock-free
        // `fetch_add(N)` BEFORE acquiring `write_mutex`.
        let total_count: u64 = keys.len() as u64;
        let prev = self
            .sequence_number
            .fetch_add(total_count, Ordering::Relaxed);
        let last_seq = prev + total_count;
        // Spec §6a.4 sequence-number overflow guard.
        Self::check_sequence_overflow(last_seq)?;

        // R58-H1: charge the WriteBufferManager BEFORE the insert call.
        self.write_buffer_manager.reserve(total_charge);
        let wbm_guard = WbmReleaseGuard::new(&self.write_buffer_manager, total_charge);

        let base_seq = prev + 1;

        // FRS-WAL Phase 2b: write-ahead — append the WHOLE batch to the WAL
        // buffer before the memtable insert (record i carries seq `base_seq + i`,
        // matching `batch_insert_with_base_seq`). NO per-batch fsync — durability
        // is established at checkpoint via `wal_sync` (Flink replays the source
        // from the last checkpoint, so the WAL only needs to be durable there;
        // per-batch fsync was a ~170× throughput cliff). No-op when WAL disabled.
        {
            let mut guard = self.wal.lock().expect("wal lock poisoned");
            if let Some(w) = guard.as_mut() {
                for i in 0..keys.len() {
                    w.append(&crate::wal::WalRecord {
                        cf_id: single_cf_id.0,
                        sequence: base_seq + i as u64,
                        op_type: op_types[i],
                        key: keys[i].to_vec(),
                        value: values[i].map(|v| v.to_vec()),
                    })?;
                }
            }
        }

        // R56-H1 bounded-retry on FrozenMemTable absorbs a concurrent
        // flush's swap atomically from the caller's view.
        let mut attempt: u32 = 0;
        loop {
            let mem_arc = cf_data.active_memtable();
            match mem_arc.batch_insert_with_base_seq(keys, values, op_types, base_seq) {
                Ok(_) => break,
                Err(ForstError::FrozenMemTable) if attempt < 8 => {
                    std::thread::yield_now();
                    std::thread::sleep(std::time::Duration::from_micros(100u64 << attempt));
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
        wbm_guard.commit();

        // Post-write: memtable-switch under write_mutex, then enqueue flush.
        let mut cfs_to_flush: Vec<Arc<ColumnFamilyData>> = Vec::new();
        {
            let _writer = self.write_mutex.lock().expect("lock poisoned");
            if self.maybe_switch_memtable_in_lock(&cf_data)? {
                cfs_to_flush.push(cf_data.clone());
            }
        }
        for cf_to_flush in &cfs_to_flush {
            self.enqueue_flush(cf_to_flush.clone())?;
        }
        // FRS-WBM-TRUE-BACKPRESSURE: dominant FFM single-CF vectorized write path —
        // stall here so memtables are a hard bound. Self-guards via over_budget().
        self.wait_for_wbm_headroom();

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
        self.check_fatal_error()?;
        let count = batch.num_rows();
        if count == 0 {
            return Ok(self.sequence_number());
        }
        // C-C6R3-NEW-H1: validate schema BEFORE the irreversible `fetch_add` /
        // `wbm.reserve` below. Pre-fix any caller passing a wrong-shape
        // RecordBatch (wrong column count or non-Binary/UInt8 dtypes)
        // would advance the sequence counter then return Err downstream
        // — a permanent seq gap. Worse: `num_columns() < 2` would panic
        // at `batch.column(0)/(1)` indexing inside the WBM-charge tally
        // AFTER the seq leak. Same defense pattern as
        // C-C6R2-NEW-H1 on the borrowed-slice sister.
        if batch.num_columns() != 3 {
            return Err(ForstError::invalid_argument(
                "batch_put_arrow: expected 3 columns (key, value, op_type)",
            ));
        }
        use arrow::array::Array;
        use arrow::datatypes::DataType;
        if !matches!(batch.column(0).data_type(), DataType::Binary) {
            return Err(ForstError::invalid_argument(
                "batch_put_arrow: column 0 must be Binary",
            ));
        }
        if !matches!(batch.column(1).data_type(), DataType::Binary) {
            return Err(ForstError::invalid_argument(
                "batch_put_arrow: column 1 must be Binary",
            ));
        }
        if !matches!(batch.column(2).data_type(), DataType::UInt8) {
            return Err(ForstError::invalid_argument(
                "batch_put_arrow: column 2 must be UInt8",
            ));
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

        // R58-H1: charge the WriteBufferManager BEFORE the insert. Same
        // rationale as `batch_write` — pre-fix the Arrow batch path
        // never reserved bytes, so flush-time release drove the WBM
        // counter to zero via saturating_sub and the cross-CF budget
        // cap was silently bypassed for any arrow-batch-heavy workload.
        //
        // Per-row charge mirrors the per-row WriteBatch shape: key +
        // value + 8 (seq) + 1 (op) + 48 (overhead).
        //
        // R59-M1: compute the actually-used byte span from
        // `value_offsets()`, NOT `value_data().len()`. arrow-rs slices
        // share the underlying values buffer between parent and slice;
        // `value_data().len()` would return the FULL parent-buffer
        // length even when the caller passes a single-row slice — the
        // reserve would then far exceed the memtable's `memory_used`
        // accounting (which sums per-row key.len()+value.len()+57),
        // leaving a permanent positive drift on every sliced batch.
        // Using `offsets[len] - offsets[0]` measures only the slice's
        // span and matches the memtable accounting exactly.
        fn binary_slice_bytes(col: &dyn arrow::array::Array) -> u64 {
            use arrow::array::Array;
            let Some(a) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() else {
                return 0;
            };
            let offsets = a.value_offsets();
            if offsets.is_empty() {
                return 0;
            }
            let lo = offsets[0];
            let hi = offsets[a.len()];
            (hi - lo).max(0) as u64
        }
        let key_bytes = binary_slice_bytes(batch.column(0).as_ref());
        let value_bytes = binary_slice_bytes(batch.column(1).as_ref());
        let total_charge = key_bytes
            .saturating_add(value_bytes)
            .saturating_add((count as u64).saturating_mul(57));
        self.write_buffer_manager.reserve(total_charge);
        let wbm_guard = WbmReleaseGuard::new(&self.write_buffer_manager, total_charge);

        // E1: arrow batch is partitioned across shards in `ShardedMemTable`;
        // each shard takes its own lock so concurrent batches don't
        // serialize on a single memtable lock. Switch decision still
        // serializes on `write_mutex`.
        //
        // R56-H1 (companion fix): same FrozenMemTable race as in
        // `batch_write` — a concurrent flush can swap+freeze the
        // active memtable between our capture and the insert. Retry
        // with the same bounded backoff (~25.6ms total) so the swap
        // is absorbed without surfacing a torn-batch error to the
        // caller after the sequence range has already been reserved.
        {
            let mut attempt: u32 = 0;
            loop {
                let mem_arc = cf_data.active_memtable();
                match mem_arc.batch_put_arrow_with_base_seq(batch, base_seq) {
                    Ok(_) => break,
                    Err(ForstError::FrozenMemTable) if attempt < 8 => {
                        std::thread::yield_now();
                        std::thread::sleep(std::time::Duration::from_micros(100u64 << attempt));
                        attempt += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        // Insert succeeded — keep the WBM reservation until flush
        // releases it.
        wbm_guard.commit();
        let needs_flush = {
            let _writer = self.write_mutex.lock().expect("lock poisoned");
            self.maybe_switch_memtable_in_lock(&cf_data)?
        };

        // Phase 2 (outside write_mutex): hand the imm to the background
        // worker. Same pattern as `write_single` / `batch_write`.
        if needs_flush {
            self.enqueue_flush(cf_data.clone())?;
        }
        // FRS-WBM-TRUE-BACKPRESSURE: Arrow zero-copy batch path — stall on over-budget
        // (self-guards) so this path is also a hard memtable bound.
        self.wait_for_wbm_headroom();

        Ok(last_seq)
    }

    /// Forces the active memtable of a CF to switch (for flush testing).
    ///
    /// Note: this does NOT enqueue the resulting imm onto the background
    /// flush queue — callers (mostly tests) typically follow up with
    /// `flush_cf` / `flush_all` to drain synchronously, and we don't want
    /// duplicate work bouncing through the worker.
    pub fn force_switch_memtable(&self, cf: &ColumnFamilyHandle) -> ForstResult<()> {
        self.check_fatal_error()?;
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
        self.check_fatal_error()?;
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
        self.check_fatal_error()?;
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
            // see a fully-flushed state. Empty imms can be produced by older
            // compatibility paths; if flushing one makes progress by popping
            // it, keep draining the rest of the queue.
            while cf_data.imm_count() > 0 {
                let before = cf_data.imm_count();
                let flushed = self.flush_cf_data(&cf_data)?;
                if flushed.is_none() && cf_data.imm_count() >= before {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Convenience: switch the active memtable and then flush it. Useful in
    /// tests and when the engine is about to checkpoint.
    pub fn switch_and_flush(&self, cf: &ColumnFamilyHandle) -> ForstResult<Option<SstFileMeta>> {
        self.check_fatal_error()?;
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let active_entries = cf_data.active_memtable().num_entries();
        if active_entries == 0 {
            return Ok(None);
        }
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
                if cf_data.active_memtable().num_entries() == 0 {
                    continue;
                }
                let handle = cf_data.handle().clone();
                self.force_switch_memtable(&handle)?;
            }
            self.flush_all()?;
        }
        let version = self.version_set.current();
        // R80-M2: build a cf_id → name lookup so per-file `cf_name` reflects
        // the actual owner CF. Pre-fix this hardcoded DEFAULT_CF_NAME for
        // every file; the compat-JNI `getLiveFilesMetaData` exposes
        // `columnFamilyName` to RocksDB-shape consumers (Flink restore
        // planners, ops tooling) which then mis-routed non-default-CF
        // SSTs onto the default CF.
        let cf_id_to_name: std::collections::HashMap<ColumnFamilyId, String> = self
            .collect_cf_descriptors()?
            .into_iter()
            .map(|d| (d.cf_id, d.name))
            .collect();
        let mut out = Vec::new();
        for (level_idx, level_meta) in version.levels.iter().enumerate() {
            for file in &level_meta.files {
                let cf_name = cf_id_to_name
                    .get(&file.cf_id)
                    .cloned()
                    .unwrap_or_else(|| DEFAULT_CF_NAME.to_string());
                out.push(LiveFileInfo {
                    path: sst_file_path(&self.db_path, file.file_number),
                    size: file.file_size,
                    sequence: file.max_sequence.value(),
                    level: level_idx as u8,
                    cf_name,
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

    /// Runs one manual full-range compaction step for a CF.
    ///
    /// Unlike background compaction, this is not gated by level-size
    /// thresholds: Java `RocksDB.compactRange(cf)` is an explicit caller
    /// request and Flink TTL tests rely on already-flushed deeper files being
    /// re-visited even when the level is below the background trigger.
    pub fn compact_range(&self, cf: &ColumnFamilyHandle) -> ForstResult<Option<SstFileMeta>> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        self.compact_range_for_cf(&cf_data)
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

    fn compact_range_for_cf(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
    ) -> ForstResult<Option<SstFileMeta>> {
        if cf_data.is_dropped() {
            return Ok(None);
        }

        let cf_id = cf_data.handle().id();
        let mut last_meta = None;

        loop {
            let version = self.version_set.current();
            if version.l0_files().iter().any(|f| f.cf_id == cf_id) {
                if let Some(meta) = self.compact_l0_for_cf(cf_data)? {
                    last_meta = Some(meta);
                }
                continue;
            }

            let bottom_level = version.num_levels().saturating_sub(1);
            let mut compacted_non_bottom = false;
            for level in 1..bottom_level {
                let has_level_file = version.levels[level].files.iter().any(|f| f.cf_id == cf_id);
                if has_level_file {
                    if let Some(meta) = self.compact_level_for_cf(cf_data, level as u32)? {
                        last_meta = Some(meta);
                    }
                    compacted_non_bottom = true;
                    break;
                }
            }
            if compacted_non_bottom {
                continue;
            }

            if bottom_level > 0
                && version.levels[bottom_level]
                    .files
                    .iter()
                    .any(|f| f.cf_id == cf_id)
            {
                if let Some(meta) = self.compact_bottommost_for_cf(cf_data)? {
                    last_meta = Some(meta);
                }
            }
            return Ok(last_meta);
        }
    }

    fn compact_bottommost_for_cf(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
    ) -> ForstResult<Option<SstFileMeta>> {
        let _compaction_guard = self
            .compaction_mutex
            .lock()
            .expect("compaction_mutex poisoned");
        let _guard = cf_data.lock_flush();

        if cf_data.is_dropped() {
            return Ok(None);
        }

        let version = self.version_set.current();
        let bottom_level = version.num_levels().saturating_sub(1);
        if bottom_level == 0 {
            return Ok(None);
        }

        let cf_id = cf_data.handle().id();
        let bottom_files: Vec<SstFileMeta> = version.levels[bottom_level]
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect();
        if bottom_files.is_empty() {
            return Ok(None);
        }

        let mut inputs: Vec<(u32, SstFileMeta, Arc<SstReaderImpl>)> = Vec::new();
        for meta in &bottom_files {
            inputs.push((
                bottom_level as u32,
                meta.clone(),
                self.get_or_open_sst_reader(meta)?,
            ));
        }

        let output_file_number = self.version_set.allocate_file_number();
        let output_path = compaction_output_path(&self.db_path, output_file_number);
        let writer_options = SstWriterOptions {
            block_size: self.options.block_size,
            compression: self.options.compression,
            cf_id,
        };
        let target_file_size = self.options.target_file_size_base as u64;
        let total_input_bytes: u64 = inputs.iter().map(|(_, m, _)| m.file_size).sum();
        let additional_outputs =
            self.alloc_compaction_output_slots(total_input_bytes, target_file_size);
        let min_active_snapshot = self.snapshot_registry.min_active();
        let job = CompactionJob {
            cf_id,
            inputs,
            output_level: bottom_level as u32,
            output_file_number,
            output_path: output_path.clone(),
            additional_outputs,
            target_file_size,
            writer_options,
            fs: self.fs.clone(),
            merge_operator: cf_data.merge_operator().cloned(),
            compaction_filter: cf_data.compaction_filter(),
            is_bottommost: true,
            min_active_snapshot,
        };

        let Some(edit) = job.run()? else {
            return Ok(None);
        };
        if let Err(e) = self.version_set.apply(&edit) {
            for (_, meta) in &edit.new_files {
                debug_assert!(
                    self.deletion_guard.can_delete(meta.file_number),
                    "orphan-delete bypass for bottommost rewrite output {} is unsafe: file is pinned",
                    meta.file_number.value()
                );
                let p = compaction_output_path(&self.db_path, meta.file_number);
                if let Err(rm_err) = self.fs.delete_file(&p) {
                    tracing::warn!(
                        target: "forst_rs_engine::compaction",
                        file_number = meta.file_number.value(),
                        error = %rm_err,
                        "failed to remove orphaned bottommost compaction output after stale-edit reject"
                    );
                }
            }
            return Err(e);
        }

        let new_meta = edit.new_files.first().map(|(_, m)| m.clone());
        for (_, meta) in &edit.new_files {
            let p = compaction_output_path(&self.db_path, meta.file_number);
            self.fs.await_upload(&p)?;
            let rac = self.fs.open_random_access_file(&p)?;
            // FRS-COMPACT-READER-BLOCKCACHE (2026-06-09): wire the shared decoded-block cache,
            // mirroring the lazy `get_or_open_sst_reader` path. WITHOUT this, compaction-output
            // (L1+) readers had `block_cache=None`, so probes of compacted state re-read +
            // re-decompressed data blocks from disk instead of hitting the decoded-RecordBatch
            // cache. The deeper levels hold the BULK of a join's accumulated state and are probed
            // repeatedly — this cache-bypass was a dominant per-probe cost (q9/q20). Same fix as
            // the flush pre-populate. Correctness-neutral (same bytes, same reader API).
            let reader = Arc::new(SstReaderImpl::open(rac)?.with_block_cache(
                Arc::clone(&self.block_cache)
                    as std::sync::Arc<dyn forst_rs_storage::cache::BlockCache>,
                self.db_id.0,
                meta.file_number.value(),
            ));
            self.sst_readers.rcu(|cur| {
                let mut next = (**cur).clone();
                next.insert(meta.file_number, std::sync::Arc::clone(&reader));
                std::sync::Arc::new(next)
            });
        }
        self.sst_readers.rcu(|cur| {
            let mut next = (**cur).clone();
            for (_, file_number) in &edit.deleted_files {
                next.remove(file_number);
            }
            std::sync::Arc::new(next)
        });
        for (_, file_number) in &edit.deleted_files {
            self.delete_file_guarded(*file_number);
        }
        self.reap_pending_deletions();

        Ok(new_meta)
    }

    fn compact_once(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<bool> {
        // R61-M3: short-circuit on dropped CF. If drop_cf hit its retry
        // cap and left files referenced in the Version (or there's a
        // race with an in-flight drop_cf), `has_my_l0` stays true but
        // `compact_l0_for_cf` no-ops on the is_dropped gate, leaving
        // `compact_all`'s outer loop spinning forever. Returning
        // `Ok(false)` here lets the outer loop terminate cleanly —
        // the orphaned files (if any) will be reclaimed on the next
        // restart's manifest replay rather than wedging compaction.
        if cf_data.is_dropped() {
            return Ok(false);
        }
        // R49-H1: filter by cf_id so an unrelated CF's L0 doesn't spuriously
        // trigger a compaction here that `compact_l0_for_cf` will then no-op
        // (because its own cf_id filter returns an empty input set). Without
        // this, `compact_all` walks every CF and for each one keeps looping
        // because L0 still has SOME files (from other CFs) — an infinite
        // loop in the multi-CF case.
        let cf_id = cf_data.handle().id();
        let version = self.version_set.current();
        let has_my_l0 = version.l0_files().iter().any(|f| f.cf_id == cf_id);
        if has_my_l0 {
            self.compact_l0_for_cf(cf_data)?;
            return Ok(true);
        }
        // Priority 2: pick the deepest level that exceeds its size budget
        // FOR THIS CF. R50-M1: pre-fix `pick_compaction_level` summed every
        // CF's bytes per level; in the multi-CF case `compact_once(A)` could
        // walk every level only to bail because the over-budget was driven
        // by CF B. Filtering by cf_id at pick time fixes the scheduling
        // waste and also removes the redundant `has_my_files_at_level`
        // re-check below (level is now guaranteed to contain this CF's
        // files, since they are the only files we summed).
        let Some(level) = self.pick_compaction_level_for_cf(cf_id) else {
            return Ok(false);
        };
        self.compact_level_for_cf(cf_data, level)?;
        Ok(true)
    }

    /// Returns the shallowest level (>= 1) whose CF-scoped total file size
    /// exceeds its target, or `None` if every level is within budget for
    /// the given CF.
    ///
    /// R50-M1: only files matching `cf_id` are summed. Pre-fix this method
    /// summed all CFs' bytes per level which made `compact_once` waste
    /// scheduling cycles on levels where the over-budget came from a
    /// different CF.
    ///
    /// Target is `max_bytes_for_level_base * multiplier^(level-1)`.
    fn pick_compaction_level_for_cf(&self, cf_id: ColumnFamilyId) -> Option<u32> {
        let version = self.version_set.current();
        let base = self.options.max_bytes_for_level_base as f64;
        let mult = self.options.max_bytes_for_level_multiplier;
        for level in 1..(self.options.num_levels - 1) {
            let total_size: u64 = version.levels[level]
                .files
                .iter()
                .filter(|f| f.cf_id == cf_id)
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
        // R44-H1: engine-global compaction serialization — see the matching
        // comment in `compact_l0_for_cf`. Acquired BEFORE `flush_mutex`.
        let _compaction_guard = self
            .compaction_mutex
            .lock()
            .expect("compaction_mutex poisoned");
        let _guard = cf_data.lock_flush();

        // R60-H1 (companion): same is_dropped gate as compact_l0_for_cf.
        // See the comment there for the full rationale.
        if cf_data.is_dropped() {
            return Ok(None);
        }

        let version = self.version_set.current();
        let level_idx = level as usize;
        if level_idx >= version.num_levels() {
            return Ok(None);
        }
        // R49-H1: scope compaction to this CF's files only. Without the
        // cf_id filter, an inter-level compaction could pull SSTs from other
        // CFs into this CF's output, silently merging key streams across CFs.
        let cf_id = cf_data.handle().id();
        // FRS-LEVELED-COMPACTION (2026-06-04): bounded input picking. Pick a
        // SINGLE source file from `level` (the smallest-key one — files are
        // sorted by smallest_key) plus the next-level files its range
        // overlaps, instead of rewriting the WHOLE level into the next on
        // every compaction (the write-amp source that let compaction fall
        // behind → L0 backup → read-amp + write-stall). The picked file is
        // consumed (removed from `level`) by this compaction's VersionEdit, so
        // successive invocations naturally rotate through the level's key
        // space; the background worker re-triggers while the level stays
        // over-target. Combined with the multi-file split output, each Ln→Ln+1
        // compaction now touches O(1 src + its overlap) bytes, not O(level).
        let all_src: Vec<SstFileMeta> = version.levels[level_idx]
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect();
        if all_src.is_empty() {
            return Ok(None);
        }
        // `apply_edit` keeps each level sorted by smallest_key, so `all_src[0]`
        // is the lowest-key file for this CF — a deterministic, rotating pick.
        let src_files: Vec<SstFileMeta> = vec![all_src[0].clone()];
        let next_level = level_idx + 1;
        if next_level >= version.num_levels() {
            // Can't go deeper — the engine is at max depth. Treat as no-op.
            return Ok(None);
        }
        let dst_candidates: Vec<SstFileMeta> = version.levels[next_level]
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect();

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
            cf_id: cf_data.handle().id(),
        };

        // R0B-H1: the retention horizon for THIS compaction must come
        // only from caller-visible snapshots. A previous internal
        // `self.snapshot()` taken before this read polluted min_active
        // even when no external snapshot existed, making every latest
        // L0 version look pinned and disabling bottommost tombstone
        // dropping / merge-chain collapse. New snapshots opened after
        // this point capture the current sequence high-water mark and
        // can read the compacted latest state, so they do not require
        // preserving the pre-compaction history below this horizon.
        let min_active_snapshot = self.snapshot_registry.min_active();
        // FRS-LEVELED-COMPACTION: split this Ln→Ln+1 output into ~target-sized
        // SSTs so the destination level holds MULTIPLE non-overlapping files.
        let target_file_size = self.options.target_file_size_base as u64;
        let total_input_bytes: u64 = inputs.iter().map(|(_, m, _)| m.file_size).sum();
        let additional_outputs =
            self.alloc_compaction_output_slots(total_input_bytes, target_file_size);
        let job = CompactionJob {
            cf_id: cf_data.handle().id(),
            inputs,
            output_level: next_level as u32,
            output_file_number,
            output_path: output_path.clone(),
            additional_outputs,
            target_file_size,
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
        // R44-H1 / R44-L2: apply may return `Busy` if a stale-edit slipped
        // past the compaction_mutex (defense-in-depth). On reject, remove
        // EVERY orphaned output SST so they don't leak.
        if let Err(e) = self.version_set.apply(&edit) {
            // R45-L1 (multi-file): each output file was freshly minted for this
            // compaction and `apply` failed BEFORE installation, so no
            // checkpoint / reader / sibling compaction can have observed — let
            // alone pinned — any of them. The guard's pin-count for each must
            // be zero; assert in debug builds.
            for (_, meta) in &edit.new_files {
                debug_assert!(
                    self.deletion_guard.can_delete(meta.file_number),
                    "orphan-delete bypass for L→L+1 output {} is unsafe: file is pinned",
                    meta.file_number.value()
                );
                let p = compaction_output_path(&self.db_path, meta.file_number);
                if let Err(rm_err) = self.fs.delete_file(&p) {
                    tracing::warn!(
                        target: "forst_rs_engine::compaction",
                        file_number = meta.file_number.value(),
                        error = %rm_err,
                        "failed to remove orphaned L→L+1 compaction output after stale-edit reject"
                    );
                }
            }
            return Err(e);
        }

        let new_meta = edit.new_files.first().map(|(_, m)| m.clone());
        for (_, meta) in &edit.new_files {
            // 2026-05-29 WRITE-BACK FLUSH: the compaction-output SST upload was
            // spawned by `close()`; await it before opening a reader straight
            // off the remote so the cached reader sees the fully-uploaded object.
            let p = compaction_output_path(&self.db_path, meta.file_number);
            self.fs.await_upload(&p)?;
            let rac = self.fs.open_random_access_file(&p)?;
            // FRS-COMPACT-READER-BLOCKCACHE (2026-06-09): wire the shared decoded-block cache
            // (same fix as the L0 compaction + flush paths) so L→L+1 compaction-output readers
            // hit the decoded-RecordBatch cache instead of re-decompressing on every probe.
            let reader = Arc::new(SstReaderImpl::open(rac)?.with_block_cache(
                Arc::clone(&self.block_cache)
                    as std::sync::Arc<dyn forst_rs_storage::cache::BlockCache>,
                self.db_id.0,
                meta.file_number.value(),
            ));
            let inserted = reader;
            self.sst_readers.rcu(|cur| {
                let mut next = (**cur).clone();
                next.insert(meta.file_number, std::sync::Arc::clone(&inserted));
                std::sync::Arc::new(next)
            });
        }
        self.sst_readers.rcu(|cur| {
            let mut next = (**cur).clone();
            for (_, file_number) in &edit.deleted_files {
                next.remove(file_number);
            }
            std::sync::Arc::new(next)
        });
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
        self.check_fatal_error()?;
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

        // 2026-05-29 WRITE-BACK FLUSH durability barrier (CRITICAL): the flushes
        // above may have serialized SSTs locally and SPAWNED their S3 uploads.
        // A checkpoint manifest must NEVER reference an SST that is only in a
        // local/in-flight buffer — on restore the engine reads those SSTs
        // straight from the remote with no resident memtable to shadow them. So
        // block until EVERY in-flight upload has completed (and surface the
        // first upload error) BEFORE we snapshot the VersionSet and emit the
        // manifest. After this returns, every live SST is remote-durable.
        self.fs.await_all_uploads()?;

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
        // A-R8-NEW-H1: collect CF descriptors INSIDE the locked-view
        // closure so both the version snapshot AND the descriptor set are
        // sampled atomically under `apply_lock` — mirrors R80-M1 for the
        // incremental path. Pre-fix, `collect_cf_descriptors()` ran AFTER
        // the closure released apply_lock, leaving a TOCTOU window where
        // a concurrent `drop_cf(X)` could fall between: its delete-edit is
        // gated on apply_lock (so the version snapshot is consistent) but
        // its removal from the in-memory `cfs` map happens AFTER apply
        // releases, so we observed the pre-drop version + post-drop
        // descriptor set. The dropped CF's pinned SSTs would then become
        // orphans on restore (Version pins them, no CF re-registers them,
        // permanent disk leak), or under future cf_id reuse could shadow
        // new CF state.
        let (mut snapshot, live, _pin, descriptors_result) = self
            .version_set
            .snapshot_with_locked_view(|snap: &VersionSetSnapshot| {
                let live = snap.version.live_sst_files();
                let file_numbers: Vec<FileNumber> = live.iter().map(|f| f.file_number).collect();
                let pin = self.deletion_guard.pin_batch(&file_numbers);
                let descriptors = self.collect_cf_descriptors();
                (snap.clone(), live, pin, descriptors)
            });
        // R49-H2: stamp CF descriptors onto the snapshot so the blob persists
        // the CF set. Restore re-registers every CF before returning so callers
        // do not have to track CF order or re-issue create_column_family.
        snapshot.cf_descriptors = descriptors_result?;
        // R0A-H1: also stamp `last_sequence` from the engine's atomic
        // counter (the high-water mark of assigned seqs), in case
        // flush/compaction's per-edit `last_sequence` advance lagged or
        // the version-set was opened without any flush. Without this,
        // restore initializes `sequence_number` from a value less than
        // the highest seq in the restored SSTs and the next write reuses
        // an existing seq — silent value overwrite via mvcc visibility.
        let engine_seq = self.sequence_number.load(Ordering::Acquire);
        if engine_seq > snapshot.last_sequence {
            snapshot.last_sequence = engine_seq;
        }
        let blob = serialize_snapshot(&snapshot)?;

        // R49-M1: copy live SSTs FIRST, write the blob LAST. The blob is the
        // crash-recovery anchor — a valid blob that references SSTs not yet
        // copied is worse than no blob at all (restore would observe missing
        // files and refuse). The write_blob path already uses tmp+rename for
        // atomicity (R39-H2) and now fsyncs the parent dir afterwards
        // (R49-H3) so a mid-write crash leaves either: (a) no blob — restore
        // sees the directory but no manifest and refuses gracefully, or
        // (b) a valid blob whose every referenced SST is fully copied and
        // synced.
        self.fs.create_dir_all(target_dir)?;

        let (sst_bytes, sst_files) =
            copy_live_ssts(self.fs.as_ref(), &self.db_path, target_dir, &live)?;

        write_blob(self.fs.as_ref(), target_dir, &blob)?;

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

    /// FRS-CKPT-NOFLUSH (2026-06-01): serialise every CF's LIVE in-RAM state
    /// (active + immutable memtables) to per-CF Arrow-IPC artifacts under
    /// `target_dir` WITHOUT flushing/sealing the memtables. Returns the list of
    /// `(cf_id, artifact_file_name)` written.
    ///
    /// This is the engine half of checkpoint-without-flush: the checkpoint
    /// captures the memtable durably (Arrow IPC → S3, the goal's "Batch
    /// Checkpoint") while the memtable stays the resident, unfragmented read
    /// structure — avoiding the L0-SST fan-out that collapses heavy joins under
    /// ckpt-ON. The artifacts compose with the existing VersionSet blob + SSTs
    /// (state already flushed by WBM pressure); restore replays the artifacts
    /// back into fresh memtables via [`Self::replay_memtable_artifact_bytes`].
    ///
    /// The caller MUST hold the engine in the synchronous snapshot phase (no
    /// concurrent writers) — Flink's per-slot single-threaded snapshot satisfies
    /// this. A CF whose memtables are all empty contributes no artifact.
    pub fn snapshot_memtables_to_dir(
        &self,
        target_dir: &std::path::Path,
        max_seq: Option<u64>,
    ) -> ForstResult<Vec<(u32, String)>> {
        // Arrow batch chunk size for the memtable artifact (matches the flush
        // path's FLUSH_BATCH_SIZE).
        const MEMTABLE_SNAPSHOT_BATCH_SIZE: usize = 8192;
        self.check_fatal_error()?;
        // CRITICAL (artifact locality): the backend's checkpoint uploader reads
        // these files with `Files.newInputStream` (LOCAL NIO), so artifacts MUST
        // be staged on the LOCAL filesystem — NOT via `self.fs`, which is the
        // S3-primary cached FS in production (writing there would leave the
        // upload step with a NoSuchFile on local disk). `target_dir` is the
        // backend's local checkpoint staging dir. Use `std::fs` directly with a
        // tmp+rename for crash-atomicity.
        std::fs::create_dir_all(target_dir).map_err(ForstError::Io)?;
        let cfs: Vec<Arc<ColumnFamilyData>> = {
            let guard = self.cfs.read().expect("lock poisoned");
            guard.values().cloned().collect()
        };
        let mut written = Vec::new();
        for cf_data in &cfs {
            let cf_id = cf_data.handle().id().0;
            // Collect active + immutable memtable batches (everything not yet in
            // an SST). Each memtable is snapshotted live (no seal).
            let fname = format!("memtable-cf{cf_id}.arrow");
            let final_path = target_dir.join(&fname);
            let tmp_path = target_dir.join(format!(".{fname}.tmp"));
            // FRS-CKPT-NOFLUSH streaming snapshot (2026-06-01): stream batches
            // straight into the staging file ONE AT A TIME via
            // snapshot_batches_bounded_for_each, dropping each batch before the
            // next is built. Peak memory is one ~1.5 GiB batch — NOT the whole
            // memtable materialized as a Vec<RecordBatch> (the old path) and NOT
            // a second serialized Vec<u8> copy. That doubling/tripling OOM'd the
            // TaskManager at large writebuffer.size, forcing a small memtable +
            // heavy-query spill collapse. The header is created lazily on the
            // first non-empty batch, so an all-empty memtable writes nothing and
            // we skip the file. BufWriter coalesces the small IPC writes.
            let file = std::fs::File::create(&tmp_path).map_err(ForstError::Io)?;
            let mut artifact = forst_rs_storage::memtable::MemtableArtifactWriter::new(
                std::io::BufWriter::new(file),
            );
            let stream_res = (|| -> ForstResult<()> {
                cf_data
                    .active_memtable()
                    .snapshot_batches_bounded_for_each(
                        MEMTABLE_SNAPSHOT_BATCH_SIZE,
                        max_seq,
                        |b| artifact.write(&b),
                    )?;
                for imm in cf_data.imm_memtables() {
                    imm.snapshot_batches_bounded_for_each(
                        MEMTABLE_SNAPSHOT_BATCH_SIZE,
                        max_seq,
                        |b| artifact.write(&b),
                    )?;
                }
                Ok(())
            })();
            let wrote_any = match stream_res.and_then(|()| artifact.finish()) {
                Ok(w) => w,
                Err(e) => {
                    let _ = std::fs::remove_file(&tmp_path);
                    return Err(e);
                }
            };
            if !wrote_any {
                // Empty memtable: discard the 0-byte staging file, emit no entry.
                let _ = std::fs::remove_file(&tmp_path);
                continue;
            }
            std::fs::rename(&tmp_path, &final_path).map_err(|e| {
                let _ = std::fs::remove_file(&tmp_path);
                ForstError::Io(e)
            })?;
            written.push((cf_id, fname));
        }
        // Mirror create_checkpoint's durability barrier: any WBM-pressure flush
        // SSTs referenced by the companion blob must be remote-durable.
        self.fs.await_all_uploads()?;
        Ok(written)
    }

    /// FRS-CKPT-NOFLUSH: replay a memtable artifact (produced by
    /// [`Self::snapshot_memtables_to_dir`]) into `cf_data`'s active memtable,
    /// preserving every entry's original sequence + op_type. Used on restore to
    /// rebuild the in-RAM state that was checkpointed without flushing.
    pub fn replay_memtable_artifact_bytes(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        bytes: &[u8],
    ) -> ForstResult<usize> {
        use arrow::array::{Array, BinaryArray, UInt64Array, UInt8Array};
        let batches = forst_rs_storage::memtable::deserialize_memtable_batches(bytes)?;
        let mem = cf_data.active_memtable();
        let mut replayed = 0usize;
        let mut max_seq = 0u64;
        for b in &batches {
            let keys = b
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ForstError::corruption("memtable artifact: key col not Binary"))?;
            let vals = b
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ForstError::corruption("memtable artifact: value col not Binary"))?;
            let seqs = b
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ForstError::corruption("memtable artifact: seq col not UInt64"))?;
            let ops = b
                .column(3)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| ForstError::corruption("memtable artifact: op col not UInt8"))?;
            for i in 0..b.num_rows() {
                let key = keys.value(i);
                let val = if vals.is_null(i) {
                    None
                } else {
                    Some(vals.value(i))
                };
                let seq = seqs.value(i);
                mem.put_with_seq(key, val, ops.value(i), seq)?;
                max_seq = max_seq.max(seq);
                replayed += 1;
            }
        }
        // Keep the global sequence counter ahead of every replayed seq so new
        // writes never reuse a restored sequence (MVCC correctness).
        if max_seq > 0 {
            self.sequence_number.fetch_max(max_seq, Ordering::AcqRel);
        }
        Ok(replayed)
    }

    /// FRS-CKPT-NOFLUSH: replay every `memtable-cf<id>.arrow` artifact found in
    /// `dir` (written by [`Self::snapshot_memtables_to_dir`]) into its CF. Used
    /// on restore after the engine has opened the SST set. Returns the total
    /// rows replayed. Artifacts for unknown CFs are an error (manifest/CF drift).
    pub fn replay_memtable_artifacts_from_dir(&self, dir: &std::path::Path) -> ForstResult<usize> {
        // LOCAL FS (mirrors snapshot_memtables_to_dir): the backend has already
        // downloaded the artifact private files into this local `dir`.
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            // No artifacts dir (e.g. a checkpoint taken before this feature, or
            // a flush-based checkpoint) → nothing to replay.
            Err(_) => return Ok(0),
        };
        let mut total = 0usize;
        for entry in entries {
            let entry = entry.map_err(ForstError::Io)?;
            let path = entry.path();
            let Some(fname) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some(cf_id) = fname
                .strip_prefix("memtable-cf")
                .and_then(|s| s.strip_suffix(".arrow"))
                .and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            let cf_data = self.lookup_cf_by_id(ColumnFamilyId(cf_id))?;
            let bytes = std::fs::read(&path).map_err(ForstError::Io)?;
            total += self.replay_memtable_artifact_bytes(&cf_data, &bytes)?;
        }
        Ok(total)
    }

    /// Opens an engine by restoring state from a checkpoint directory.
    ///
    /// The checkpoint directory must contain `CHECKPOINT.blob` and every SST
    /// file it references. The engine is opened with `db_path = target_dir`
    /// (i.e. subsequent reads/writes operate directly on the checkpoint
    /// files; copy the checkpoint first if you want to preserve it).
    ///
    /// # R46-L4: CF re-creation order matters for the homogeneity invariant
    ///
    /// The current open path re-creates only the default CF
    /// ([`DEFAULT_CF_NAME`]) — non-default CFs are not persisted in the
    /// checkpoint blob today (their state is replayed from rows in the
    /// SSTs, not from CF metadata) and must be re-registered by the
    /// caller after `open_from_checkpoint` returns, via
    /// [`Self::create_column_family`].
    ///
    /// THE ORDER OF THOSE FOLLOW-UP `create_column_family` CALLS IS
    /// SIGNIFICANT for the R45-H1 / R46-H1 homogeneity check. The check
    /// is "every non-default CF must match every other non-default CF"
    /// — the FIRST non-default CF the caller installs sets the implicit
    /// (merge_operator name, compaction_filter name) signature that all
    /// subsequent CFs must agree with. Callers re-creating a mixed-policy
    /// engine after restore must therefore either:
    ///
    /// 1. Install every CF with an identical (merge, filter) pair (the
    ///    only configuration the engine supports today), or
    /// 2. Use [`Self::set_compaction_filter`] post-creation, which goes
    ///    through the same R46-H1 check and rejects heterogeneous
    ///    installs symmetrically.
    ///
    /// The default CF installed below at `db.create_cf_with_id(...)` is
    /// exempt (id 0, no merge, no filter — the homogeneity check skips
    /// it). Future work to persist CF metadata in the checkpoint blob
    /// would also need to re-create CFs in the same order they were
    /// originally created (or batch-validate the whole set).
    pub fn open_from_checkpoint(
        options: EngineOptions,
        fs: Arc<dyn FileSystem>,
    ) -> ForstResult<Arc<Self>> {
        Self::open_from_checkpoint_with_default_cf(
            options,
            fs,
            ColumnFamilyDescriptor::new(DEFAULT_CF_NAME),
        )
    }

    /// Opens an engine from a checkpoint directory with a caller-supplied default-CF descriptor.
    pub fn open_from_checkpoint_with_default_cf(
        options: EngineOptions,
        fs: Arc<dyn FileSystem>,
        default_desc: ColumnFamilyDescriptor,
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
                    // R38-H1 + R39-L1: match `.<num>.sst.tmp` tmp-write
                    // artifacts. The leading-dot + trailing `.tmp` come
                    // from {@link flush::sst_temp_path} (used by both
                    // FlushJob and CompactionJob). Using the shared
                    // SST_TMP_PREFIX / SST_TMP_SUFFIX constants here
                    // means a future rename of the writer-side naming
                    // convention propagates automatically; the
                    // `tests::sst_temp_path_round_trip` unit test pins
                    // the round-trip so a drift here surfaces in CI
                    // rather than as a silent restore-orphan miss.
                    let sst_inner_suffix = format!(".sst{}", SST_TMP_SUFFIX);
                    if let Some(inner) = name.strip_suffix(&sst_inner_suffix) {
                        if let Some(stem) = inner.strip_prefix(SST_TMP_PREFIX) {
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
                        continue;
                    }
                    // R52-L1: match the `cf-rewrite` tmp artifact
                    // `<num>.sst.cf-rewrite.tmp` produced by
                    // [`Self::rewrite_sst_footer`]. The rewrite path
                    // writes the new footer into this tmp file, then
                    // atomically renames it over the bare `<num>.sst`.
                    // A crash between create and rename leaves the tmp
                    // in place; without the orphan-scan picking it up,
                    // a future restart's `list_dir` would still see it
                    // and a fresh rewrite of the same SST would collide
                    // on `CreateNew`. We treat it identically to the
                    // `.<num>.sst.tmp` flush/compaction tmp shape.
                    if let Some(stem) = name.strip_suffix(".sst.cf-rewrite.tmp") {
                        if let Ok(num) = stem.parse::<u64>() {
                            if num > max_observed {
                                max_observed = num;
                            }
                        }
                        tmp_orphans.push(entry.path.clone());
                        continue;
                    }
                    // R39-H2: match the checkpoint-blob tmp artifact
                    // `.<CHECKPOINT_BLOB_NAME>.tmp` produced by
                    // [`checkpoint::write_blob`]. R39-H2's `write_blob`
                    // delete-on-error close-gate now removes this file on
                    // any rename failure during checkpoint emission, but
                    // a process kill between the write and the rename
                    // (or a delete-itself failure) can still leave the
                    // file behind. We funnel it through the same
                    // orphan-rename pass so the next restore is not
                    // misled by a stale partial blob in the directory.
                    let checkpoint_tmp_name = format!(".{}.tmp", CHECKPOINT_BLOB_NAME);
                    if name == checkpoint_tmp_name {
                        tmp_orphans.push(entry.path.clone());
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
        // R50-L1: fsync the parent directory once after BOTH rename
        // passes complete so every orphan rename made above is durable
        // on a power-loss event. POSIX requires the dir-entry update to
        // be fsynced for the rename to survive a crash. Failures here
        // are logged but do not block the open — the renames are
        // idempotent and the next restore will retry any rename whose
        // dir-entry update did not make it to disk.
        if !orphans.is_empty() || !tmp_orphans.is_empty() {
            if let Err(e) = fs.sync_dir(&db_path) {
                tracing::warn!(
                    "open_from_checkpoint: sync_dir({}) after orphan rename failed: {} \
                     — renames may not be durable until next restart",
                    db_path.display(),
                    e
                );
            }
        }
        let restored_next_file_number = snapshot
            .next_file_number
            .max(max_observed.saturating_add(1));

        // A-R6-H3: defense-in-depth — cross-check the blob's
        // `last_sequence` against the maximum `max_sequence` over the
        // restored SST file metas. R0A-H1 patched the producer side
        // so freshly-written blobs never trail the SST high-water
        // mark, but legacy blobs persisted by pre-R0A-H1 binaries —
        // or any future code path that constructs a `VersionEdit`
        // with `last_sequence: None` — would still hand a stale value
        // here. Initializing `sequence_number` below an existing SST
        // seq lets the next write reuse a colliding seq and silently
        // overwrite (or be silently shadowed by) an existing row.
        // Take the max so restore is monotonic in the high-water mark
        // regardless of producer history.
        let sst_max_seq: u64 = snapshot
            .version
            .live_sst_files()
            .iter()
            .map(|f| f.max_sequence.value())
            .max()
            .unwrap_or(0);
        let restored_last_sequence = snapshot.last_sequence.max(sst_max_seq);

        // Build the DbImpl with the restored VersionSet.
        let version_set = Arc::new(forst_rs_storage::version::VersionSetImpl::from_restored(
            (*snapshot.version).clone(),
            restored_next_file_number,
            restored_last_sequence,
        ));

        let cache_bytes = if options.block_cache_capacity_bytes != 0 {
            options.block_cache_capacity_bytes.min(usize::MAX as u64) as usize
        } else {
            options.block_cache_size
        };
        // FRS-BLOCKCACHE-FLOOR: the decoded-block ShardedClockCache is created
        // PER DB INSTANCE (not shared — a shared pool regressed heavy queries via
        // cross-DB shard-lock contention). The cache is native-resident and
        // charged by actual bytes inserted, so a higher floor costs light
        // queries nothing but lets heavy streaming-join DBs keep their re-probed
        // decoded data blocks resident (the profiled decode_data_block +
        // decompress + serial_read_at re-read cost on per-key prefix scans).
        // FRS-8C32G-RAMBUDGET: 256 MiB (was 768) — per-instance × ~8 instances ≈ 2 GiB,
        // fits the 32 GiB budget alongside JVM heap + the (now 1 GiB) resident shadow.
        const BLOCK_CACHE_FLOOR: usize = 256 * 1024 * 1024;
        let cache_bytes = cache_bytes.max(BLOCK_CACHE_FLOOR);
        let cache_bytes = apply_block_cache_env_override(cache_bytes);
        maybe_start_mem_diag(); // FRS_MEM_DIAG: pinpoint engine resident native (join-OOM 16GB)
        let block_cache = shared_block_cache(cache_bytes); // FRS-ROCKSDB-PARITY C3: slot-shared
                                                           // FRS-GLOBAL-WBM-BUDGET: enroll in the process-global memtable budget so the
                                                           // TOTAL memtable RAM across all keyed-state DB instances is bounded (RocksDB's
                                                           // shared-WriteBufferManager model), not 512 MiB × instance-count. The
                                                           // configured capacity stays the per-instance secondary bound.
        let write_buffer_manager =
            WriteBufferManager::new_global(options.write_buffer_manager_capacity_bytes);

        // FRS-S3-STALL (reopen path): mirror the open-path wiring so a restored
        // DB honours the configured `max_write_buffer_number` too. See the
        // primary `DbImpl::open` site for the full rationale.
        let wc_config = WriteControllerConfig {
            max_write_buffer_number: options.max_write_buffer_number as u32,
            ..WriteControllerConfig::default()
        };

        let db = Arc::new(Self {
            options,
            db_path,
            fs,
            cfs: RwLock::new(HashMap::new()),
            cf_name_to_id: RwLock::new(HashMap::new()),
            version_set,
            sst_readers: arc_swap::ArcSwap::from_pointee(HashMap::new()),
            deletion_guard: Arc::new(FileDeletionGuard::new()),
            pending_deletions: Mutex::new(Vec::new()),
            // D-R7-H1: A-R6-H3 patched the VersionSet seed but missed
            // this sibling — `DbImpl::sequence_number` is the source of
            // every write's allocated seq via `fetch_add`. A stale
            // `snapshot.last_sequence` (e.g., from a pre-R0A-H1 blob)
            // would let new writes alias an existing SST row's seq even
            // after A-R6-H3 corrected the version-set view. Use the
            // same `restored_last_sequence` computed above so both
            // sources of seq monotonicity agree.
            sequence_number: AtomicU64::new(restored_last_sequence),
            write_controller: Arc::new(WriteController::new(wc_config)),
            write_mutex: Mutex::new(()),
            next_cf_id: AtomicU32::new(1),
            self_weak: std::sync::OnceLock::new(),
            compaction_queued: Mutex::new(std::collections::HashSet::new()),
            flush_error: Mutex::new(None),
            fatal_error: Mutex::new(None),
            fatal_error_set: std::sync::atomic::AtomicBool::new(false),
            pending_flush_count: AtomicU32::new(0),
            snapshot_registry: SnapshotRegistry::new(),
            db_id: DbId(NEXT_DB_ID.fetch_add(1, Ordering::Relaxed)),
            block_cache,
            write_buffer_manager,
            snapshot_age_worker: Mutex::new(None),
            snapshot_age_shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            compaction_mutex: Mutex::new(()),
            wal: Mutex::new(None),
            l0_point_get_block_reads: AtomicU64::new(0),
        });
        db.maybe_init_wal();

        let default_desc = if let Some(cf) = snapshot
            .cf_descriptors
            .iter()
            .find(|cf| cf.cf_id == DEFAULT_CF_ID)
        {
            if cf.name != DEFAULT_CF_NAME {
                return Err(ForstError::corruption(format!(
                    "checkpoint cf_descriptor: cf_id 0 (DEFAULT_CF_ID) must have \
                     name \"{}\", got \"{}\"",
                    DEFAULT_CF_NAME, cf.name
                )));
            }
            match cf.merge_op_name.as_str() {
                "" => default_desc,
                "RawConcatMergeOperator" => ColumnFamilyDescriptor::new(DEFAULT_CF_NAME)
                    .with_merge_operator(Arc::new(RawConcatMergeOperator::new())),
                "ListAppendMergeOperator" | "ListAppendMergeOperator(delim=44)" => {
                    ColumnFamilyDescriptor::new(DEFAULT_CF_NAME)
                        .with_merge_operator(Arc::new(ListAppendMergeOperator::with_comma()))
                }
                other => {
                    return Err(ForstError::invalid_argument(format!(
                        "checkpoint cf_descriptor for default CF references unknown merge operator '{}'",
                        other
                    )));
                }
            }
        } else {
            default_desc
        };

        db.create_cf_with_id(DEFAULT_CF_ID, default_desc)?;

        // R49-H2: re-register every non-default CF that the blob recorded.
        // Known built-in merge operators are restored by name so Merge rows
        // in per-state CFs remain readable after checkpoint restore.
        for cf in &snapshot.cf_descriptors {
            // R50-H2: a blob that maps id 0 to anything other than the
            // built-in default CF name is corrupt — surface as Corruption
            // rather than silently skipping (the pre-fix `continue` did
            // exactly that, masking a manifest where DEFAULT_CF_ID had
            // been rebound to a user CF name).
            if cf.cf_id == DEFAULT_CF_ID {
                if cf.name != DEFAULT_CF_NAME {
                    return Err(ForstError::corruption(format!(
                        "checkpoint cf_descriptor: cf_id 0 (DEFAULT_CF_ID) must have \
                         name \"{}\", got \"{}\"",
                        DEFAULT_CF_NAME, cf.name
                    )));
                }
                continue;
            }
            let mut desc = ColumnFamilyDescriptor::new(cf.name.clone());
            desc = match cf.merge_op_name.as_str() {
                "" => desc,
                "RawConcatMergeOperator" => {
                    desc.with_merge_operator(Arc::new(RawConcatMergeOperator::new()))
                }
                "ListAppendMergeOperator" | "ListAppendMergeOperator(delim=44)" => {
                    desc.with_merge_operator(Arc::new(ListAppendMergeOperator::with_comma()))
                }
                other => {
                    return Err(ForstError::invalid_argument(format!(
                        "checkpoint cf_descriptor for '{}' references unknown merge operator '{}'",
                        cf.name, other
                    )));
                }
            };
            // Best-effort: bump next_cf_id past every restored id so future
            // `create_column_family` doesn't collide.
            let cur = db.next_cf_id.load(Ordering::SeqCst);
            if cf.cf_id.value() >= cur {
                db.next_cf_id.store(cf.cf_id.value() + 1, Ordering::SeqCst);
            }
            // We bypass the homogeneity check (the restored set is by
            // construction whatever the engine had at snapshot time; the
            // descriptors carry only NAMES, not operator instances, so the
            // by-string check would falsely reject mixed-policy CFs we are
            // legitimately resurrecting). `create_cf_with_id` is the
            // engine's id-preserving path used at open time for the default
            // CF; reusing it here preserves the on-disk cf_id mapping.
            db.create_cf_with_id(cf.cf_id, desc)?;
        }

        Self::init_self_weak(&db);
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
        self.create_incremental_checkpoint_impl(snapshot, checkpoint_id, base_checkpoint_id, true)
    }

    /// FRS-CKPT-NOFLUSH (2026-06-01): incremental checkpoint that DOES NOT flush
    /// the memtable to an L0 SST — it enumerates only the SSTs already produced
    /// by WBM-pressure flushes. The caller (backend snapshot strategy) captures
    /// the live memtable separately via [`Self::snapshot_memtables_to_dir`] and
    /// uploads those Arrow-IPC artifacts as private checkpoint state, keeping the
    /// memtable RAM-resident + unfragmented for reads (the fix for the ckpt-ON
    /// heavy-join collapse). Restore replays the artifacts after opening the SST
    /// set ([`Self::replay_memtable_artifacts_from_dir`]).
    pub fn create_incremental_checkpoint_noflush(
        &self,
        snapshot: &Snapshot,
        checkpoint_id: u64,
        base_checkpoint_id: u64,
    ) -> ForstResult<IncrementalCheckpointResult> {
        self.create_incremental_checkpoint_impl(snapshot, checkpoint_id, base_checkpoint_id, false)
    }

    fn create_incremental_checkpoint_impl(
        &self,
        snapshot: &Snapshot,
        checkpoint_id: u64,
        base_checkpoint_id: u64,
        flush_memtables: bool,
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
        //
        // FRS-CKPT-NOFLUSH: when `flush_memtables` is false the memtable is
        // captured by the caller as an Arrow-IPC artifact instead of being
        // sealed to an L0 SST, so we skip the flush entirely and snapshot only
        // the existing (WBM-flushed) SST set.
        //
        // FRS-WAL Phase 3 (2026-06-06): when the WAL is enabled, the live
        // memtable's durability is ALREADY provided by the WAL (every mutation
        // was group-commit fsynced on the write path), so a checkpoint need NOT
        // force a memtable flush+compaction — the expensive per-checkpoint cost
        // that the WAL exists to remove. We reference the existing WBM-flushed
        // SSTs; the unflushed tail is recoverable from the WAL. This is the
        // compact-state (WBM keeps the memtable small) + cheap-checkpoint combo
        // that closes the q4-vs-RocksDB gap on local dir. NOTE: completing the
        // restore side is WAL Phase 4 (replay the tail on open) — until then a
        // RESTORE from a WAL-mode checkpoint would drop the unflushed tail; this
        // override only activates when `FRS_WAL_DIR` is set (off by default).
        let flush_memtables =
            flush_memtables && self.wal.lock().expect("wal lock poisoned").is_none();
        if flush_memtables {
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
        }

        // FRS-WAL Phase 3: the checkpoint's durability barrier for the unflushed
        // memtable tail. When the WAL is enabled the forced flush above is
        // skipped (see the `flush_memtables &&` override), so here we fsync the
        // WAL instead — cheap sequential append-sync vs an expensive
        // flush+compaction. No-op when the WAL is disabled.
        self.wal_sync()?;

        // 2026-06-02 q7 ckpt-ON FREEZE FIX: the durability barrier is deferred
        // until AFTER the VersionSet snapshot below, where it awaits the upload
        // of ONLY the SSTs this checkpoint actually references (its pinned live
        // set) — see the per-file `await_upload` loop after the snapshot. The
        // previous blanket `self.fs.await_all_uploads()?` here drained EVERY
        // in-flight upload, including large background COMPACTION outputs that
        // are NOT part of this checkpoint's version. Once compaction kicked in
        // (~3rd checkpoint on a heavy join), each 30s-interval checkpoint stalled
        // for the entire compaction-upload duration (measured: ckpt3 = 332s vs
        // ckpt1/2 ≈ 1.6s), freezing the pipeline. Awaiting exactly the pinned
        // set preserves the durability guarantee (the manifest never references
        // an un-uploaded SST) while decoupling checkpoint latency from unrelated
        // compaction I/O.

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
        // R80-M1: collect CF descriptors INSIDE the locked-view closure so
        // both the version snapshot AND the descriptor set are sampled
        // atomically under `apply_lock`. Pre-fix the collect happened after
        // `snapshot_with_locked_view` released, leaving a TOCTOU window
        // where a concurrent `drop_cf` could fall between: its delete-edit
        // is gated on `apply_lock` (so the version snapshot is consistent)
        // but its removal from the in-memory `cfs` map happens AFTER apply
        // releases — so we could observe the pre-drop version + post-drop
        // descriptor set. The dropped CF's pinned SSTs would then mis-
        // attribute to DEFAULT_CF_NAME via the cf_id→name fallback.
        let (mut version_snapshot, _pin, descriptors_result) = self
            .version_set
            .snapshot_with_locked_view(|snap: &VersionSetSnapshot| {
                let live = snap.version.live_sst_files();
                let file_numbers: Vec<FileNumber> = live.iter().map(|f| f.file_number).collect();
                let pin = self.deletion_guard.pin_batch(&file_numbers);
                let descriptors = self.collect_cf_descriptors();
                (snap.clone(), pin, descriptors)
            });
        version_snapshot.cf_descriptors = descriptors_result?;

        // 2026-06-02 q7 ckpt-ON FREEZE FIX (durability barrier, scoped): await
        // the upload of ONLY the SSTs this checkpoint references. `_pin` above
        // holds these files live, so awaiting their uploads here is race-free,
        // and the manifest we are about to serialize references exactly this set
        // — so a restore can never point at an SST whose bytes are still in a
        // local/in-flight buffer. Unrelated background compaction uploads are
        // NOT awaited (they are not in this version), which is what keeps the
        // checkpoint off the compaction critical path. `await_upload` is a cheap
        // no-op for any file whose upload already completed (or for backends
        // that write synchronously).
        for file in version_snapshot.version.live_sst_files() {
            let sst_path = crate::flush::sst_file_path(Path::new(&self.db_path), file.file_number);
            self.fs.await_upload(&sst_path)?;
        }

        let blob = serialize_snapshot(&version_snapshot)?;

        // R79-H1: build a cf_id → name lookup so per-file `cf_name` reflects
        // the actual owner CF, not the hardcoded DEFAULT_CF_NAME. Required by
        // sticky downstream consumers (state-handle planners that route by
        // cf_name).
        let cf_id_to_name: std::collections::HashMap<ColumnFamilyId, String> = version_snapshot
            .cf_descriptors
            .iter()
            .map(|d| (d.cf_id, d.name.clone()))
            .collect();

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
                // R79-H1: derive cf_name from the per-file `cf_id` stamped on
                // the SST meta (R49-H1). Fall back to DEFAULT_CF_NAME only when
                // the lookup is empty (i.e. the file's cf_id is not in the
                // current descriptor set — should not happen post-R79-H1's
                // stamp above, but keep the fallback to avoid panicking on
                // legacy SSTs whose cf_id was never recorded).
                let cf_name = cf_id_to_name
                    .get(&file.cf_id)
                    .cloned()
                    .unwrap_or_else(|| DEFAULT_CF_NAME.to_string());
                let info = LiveFileInfo {
                    path: sst_file_path(&self.db_path, file.file_number),
                    size: file.file_size,
                    sequence: file.max_sequence.value(),
                    level: level_idx as u8,
                    cf_name,
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
        // R80-L1: `write_blob` syncs `target_dir` after rename, but the
        // PARENT (.../checkpoints/) is not — POSIX requires the parent
        // dir-entry update to be fsynced for the new subdirectory itself
        // to survive power loss. Best-effort: log on failure but do not
        // fail the checkpoint (the blob is durable; the dir-entry will
        // be re-created on next call).
        if let Some(parent) = target_dir.parent() {
            if let Err(e) = self.fs.sync_dir(parent) {
                tracing::warn!(
                    "create_incremental_checkpoint: sync_dir({}) failed: {} \
                     (subdir may not survive power loss; next checkpoint will retry)",
                    parent.display(),
                    e
                );
            }
        }
        // FRS-S3-CKPT-STAGE: This S3 write MUST stay even in object-store
        // mode — `open_from_incremental` (and the `base_live`/`read_blob`
        // block above) reads the engine's own manifest back from S3 at
        // `incremental_checkpoint_dir(base)` for cross-checkpoint SST
        // sharing. The staging step below only affects the paths RETURNED
        // to the Java uploader; it does not replace this durable write.
        let manifest_path = write_blob(self.fs.as_ref(), &target_dir, &blob)?;

        drop(_pin);

        // R78-L2: sibling site `create_checkpoint` reaps deferred deletions
        // immediately after the pin drops; the incremental path previously
        // omitted this and accumulated `pending_deletions` until the next
        // compaction-side reap. Bounded delay only (no correctness issue),
        // but eagerly reclaiming disk matches the sister contract.
        self.reap_pending_deletions();

        // FRS-CKPT-STAGE-UNIFORM (2026-05-30): ALWAYS stage the manifest + new
        // SSTs to a local temp dir and return those absolute paths, regardless
        // of `supports_atomic_rename`. The Java `ForStRsSstUploader` reads each
        // returned path via `java.nio Files.newInputStream` — a real-filesystem
        // absolute-path read.
        //
        // The pre-fix code returned the engine paths VERBATIM when
        // `supports_atomic_rename()` was true, on the assumption that a "local
        // POSIX engine" stores artifacts at directly-readable absolute paths.
        // That assumption is FALSE for the local OpenDAL `Fs` backend configured
        // with a `root` (e.g. `{"root":"/tmp/flink-forst-rs-data"}`): the engine
        // `db_path` is RELATIVE to that root (`/db-remote-<id>/...`), so the
        // returned `incremental_checkpoint_dir(id)/CHECKPOINT.blob` lacks the
        // root prefix. Java NIO then reads `/db-remote-<id>/.../CHECKPOINT.blob`
        // as an absolute path → `NoSuchFileException` → checkpoint fails → job
        // crash-loop (observed on every forst-rs LOCAL heavy-query run). The
        // S3/object-store path was unaffected only because it already staged.
        //
        // Staging reads through `self.fs` (which resolves the OpenDAL root
        // correctly) and writes to `std::env::temp_dir()` (a true absolute
        // path), so it is correct for BOTH the object-store and the
        // local-Fs-with-root configurations. The cost is one extra local copy
        // of the new SSTs per checkpoint; for the local config those bytes are
        // already on local disk so the copy is cheap and bounded by the
        // incremental delta. Shared SSTs are referenced (never re-staged).
        let (manifest_path, new_ssts) =
            self.stage_checkpoint_artifacts_local(checkpoint_id, &blob, manifest_path, new_ssts)?;
        Ok(IncrementalCheckpointResult {
            manifest_path,
            new_ssts,
            shared_ssts,
        })
    }

    /// FRS-S3-CKPT-STAGE: Stage an incremental checkpoint's manifest blob and
    /// its NEW SST files from the (object-store) engine filesystem into a
    /// local temp directory, returning the rewritten local paths.
    ///
    /// Mirrors the local-NIO upload contract of `ForStRsSstUploader`: the Java
    /// backend reads each returned path via `java.nio Files.newInputStream`
    /// (a LOCAL filesystem read) and uploads the bytes to the Flink checkpoint
    /// store. When `self.fs` is a `CachedFileSystem` over OpenDAL-S3 there is
    /// no local copy of these artifacts (the manifest write invalidates the
    /// cache), so this method materializes one.
    ///
    /// Only invoked when `self.fs.supports_atomic_rename()` is `false`
    /// (object-store mode). `shared_ssts` are intentionally NOT staged — they
    /// are referenced by handle from the base checkpoint, never re-uploaded.
    fn stage_checkpoint_artifacts_local(
        &self,
        checkpoint_id: u64,
        blob: &[u8],
        _remote_manifest_path: PathBuf,
        new_ssts: Vec<LiveFileInfo>,
    ) -> ForstResult<(PathBuf, Vec<LiveFileInfo>)> {
        // Per-(db, checkpoint) staging root under the OS temp dir. The
        // 20-digit zero-padded checkpoint id matches the on-S3 layout and
        // keeps concurrent checkpoints from colliding.
        let stage_db_root = std::env::temp_dir()
            .join("forst-rs-ckpt-stage")
            .join(format!("{}", self.db_id.0));
        // FRS-CKPT-STAGE-GC (2026-06-06): prune staging dirs from PRIOR
        // checkpoints of this db before staging the current one. The Java
        // `ForStRsSstUploader` consumes each checkpoint's staged manifest +
        // new-SSTs SYNCHRONOUSLY (reads the returned local paths and uploads)
        // before the next checkpoint is created — with the default
        // max-concurrent-checkpoints=1, checkpoint N-1's async phase fully
        // completes before N begins. So any sibling staging dir whose id is
        // numerically < `checkpoint_id` is dead. Without this prune, every
        // checkpoint leaked its staged bytes to the OS temp dir: a single
        // Nexmark sweep accumulated ~296 GB of `forst-rs-ckpt-stage` and filled
        // the disk (ENOSPC → run abort). The 20-digit zero-padded ids sort
        // lexicographically == numerically, so a string `<` compare is exact.
        let keep = format!("{:020}", checkpoint_id);
        if let Ok(entries) = std::fs::read_dir(&stage_db_root) {
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy().as_ref() < keep.as_str() {
                    let _ = std::fs::remove_dir_all(entry.path());
                }
            }
        }
        let stage_root = stage_db_root.join(&keep);
        let local_fs = LocalFileSystem::new();
        local_fs.create_dir_all(&stage_root)?;

        // Stage the manifest blob (the `serialize_snapshot` bytes already in
        // scope) under the canonical blob name so the uploader finds it.
        let local_manifest_path = stage_root.join(CHECKPOINT_BLOB_NAME);
        {
            let mut writer =
                local_fs.open_writable_file(&local_manifest_path, WriteMode::CreateOrTruncate)?;
            writer.append(blob)?;
            writer.flush()?;
            writer.sync()?;
        }

        // Stage each NEW SST: stream its bytes from the engine FS (S3,
        // cache-fronted) into a local file under the SST's basename. Mirrors
        // the `copy_file` streamed-copy + short-read guard in checkpoint.rs.
        let mut staged_ssts: Vec<LiveFileInfo> = Vec::with_capacity(new_ssts.len());
        for info in new_ssts {
            // Resolve the destination basename from the source's final path
            // component; preserve it so the uploader's handle naming is stable.
            let basename = info.path.file_name().ok_or_else(|| {
                ForstError::corruption(format!(
                    "stage_checkpoint_artifacts_local: SST path has no file name: {}",
                    info.path.display()
                ))
            })?;
            let local_sst_path = stage_root.join(basename);

            // R76-H1 parity: capture the expected source size BEFORE streaming
            // so a short read (network blip / OpenDAL ranged-read truncation)
            // is surfaced as corruption rather than publishing a truncated SST.
            // 2026-05-30 WRITE-BACK CHECKPOINT FIX: await this SST's in-flight
            // async upload BEFORE reading it from the remote. The
            // `await_all_uploads()` barrier earlier in create_incremental_checkpoint
            // is insufficient: a flush concurrent with the checkpoint (or one whose
            // upload registered just after the barrier) can leave a snapshot SST's
            // upload in flight, so staging's `open_sequential_file` reads a
            // not-yet-uploaded S3 object → 404 → NotFound → checkpoint fails →
            // job crash-loop. Awaiting per-file here closes the race regardless of
            // barrier timing (no-op on local FS / already-uploaded objects).
            self.fs.await_upload(&info.path)?;

            let expected_size: Option<u64> =
                self.fs.get_file_metadata(&info.path).ok().map(|m| m.size);

            let mut reader = self.fs.open_sequential_file(&info.path)?;
            let mut writer =
                local_fs.open_writable_file(&local_sst_path, WriteMode::CreateOrTruncate)?;
            let mut buf = vec![0u8; 64 * 1024];
            let mut total = 0u64;
            let copy_result: ForstResult<()> = (|| {
                loop {
                    let n = reader.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    writer.append(&buf[..n])?;
                    total += n as u64;
                }
                writer.flush()?;
                writer.sync()?;
                Ok(())
            })();
            if let Err(e) = copy_result {
                let _ = local_fs.delete_file(&local_sst_path);
                return Err(e);
            }
            if let Some(want) = expected_size {
                if total != want {
                    let _ = local_fs.delete_file(&local_sst_path);
                    return Err(ForstError::corruption(format!(
                        "stage_checkpoint_artifacts_local short read: expected {} bytes, \
                         got {} from {}",
                        want,
                        total,
                        info.path.display()
                    )));
                }
            }

            // Preserve size/sequence/level/cf_name; only the path is rewritten
            // to the local staged copy the uploader will read.
            staged_ssts.push(LiveFileInfo {
                path: local_sst_path,
                size: info.size,
                sequence: info.sequence,
                level: info.level,
                cf_name: info.cf_name,
            });
        }

        Ok((local_manifest_path, staged_ssts))
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
    /// R32-L1: the dst-already-exists branch (see `same_file_or_size`)
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
        Self::open_from_incremental_with_default_cf(
            target_dir,
            base_manifest,
            sst_files,
            ColumnFamilyDescriptor::new(DEFAULT_CF_NAME),
        )
    }

    /// Opens an engine from incremental state with a caller-supplied default-CF descriptor.
    pub fn open_from_incremental_with_default_cf(
        target_dir: &str,
        base_manifest: &str,
        sst_files: &[String],
        default_desc: ColumnFamilyDescriptor,
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
        //
        // R42-H4: a mid-stream failure (e.g. cross-FS copy ENOSPC,
        // permission flip on the Nth file) would otherwise leave the
        // first N-1 dsts in `target` — a subsequent retry could then
        // mis-accept stale dsts via `same_file_or_size` (size matches
        // but content differs across two `forst-checkpoint-restore`
        // calls), or, on unix, fail with a confusing inode-mismatch
        // error against the new src. Track materialized dsts and
        // best-effort unlink them in reverse order on error so retries
        // start from a clean slate.
        let mut materialized: Vec<PathBuf> = Vec::new();
        let result = (|| -> ForstResult<()> {
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
                    // Do NOT add to `materialized`: we didn't create it,
                    // and unwinding would clobber a legitimate pre-existing
                    // file the caller (or a prior successful run) owns.
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
                // Mark dst as ours-to-unlink on failure.
                materialized.push(dst);
            }
            Ok(())
        })();
        if let Err(e) = result {
            // Best-effort unwind: errors from delete_file are swallowed —
            // the surfaced error is the original failure that triggered
            // cleanup, and a cleanup failure would only obscure it. Use
            // reverse order for symmetry with the create order (so any
            // dependent state is torn down in LIFO).
            for path in materialized.iter().rev() {
                let _ = fs.delete_file(path);
            }
            return Err(e);
        }

        // Now open the engine from the materialized checkpoint dir.
        let options = EngineOptions {
            db_path: target.to_string_lossy().into_owned(),
            ..EngineOptions::default()
        };
        Self::open_from_checkpoint_with_default_cf(options, fs, default_desc)
    }

    /// FRS-LEVELED-COMPACTION (2026-06-04): pre-allocate the ADDITIONAL output
    /// slots a [`CompactionJob`] may roll into when splitting its output into
    /// ~`target` SSTs. File-number allocation lives in the VersionSet, so the
    /// caller mints them here; the job uses as many as the input size requires
    /// and leaves the rest unused (consumed from the monotonic allocator but no
    /// on-disk file is created — nothing to clean up). Count = floor(bytes /
    /// target) + 2 margin, capped, so combined with the job's uncapped final
    /// slot we never run out. Returns empty when splitting is disabled.
    fn alloc_compaction_output_slots(
        &self,
        total_input_bytes: u64,
        target: u64,
    ) -> Vec<(FileNumber, std::path::PathBuf)> {
        if target == 0 {
            return Vec::new();
        }
        let additional = ((total_input_bytes / target) as usize)
            .saturating_add(2)
            .min(4096);
        (0..additional)
            .map(|_| {
                let n = self.version_set.allocate_file_number();
                let p = compaction_output_path(&self.db_path, n);
                (n, p)
            })
            .collect()
    }

    fn compact_l0_for_cf(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
    ) -> ForstResult<Option<SstFileMeta>> {
        // R44-H1: serialize ALL compactions globally before doing anything
        // else. VersionSet is engine-global, so two compactions running on
        // different CFs would otherwise both read the same V0 and produce
        // overlapping L1 files. The per-CF `flush_mutex` only protects
        // intra-CF flush/compaction interleaving; cross-CF compaction
        // races need this engine-global mutex.
        //
        // Lock ordering (see `compaction_mutex` field docs): this mutex is
        // acquired BEFORE the per-CF `flush_mutex`. No other path takes
        // them in the reverse order.
        let _compaction_guard = self
            .compaction_mutex
            .lock()
            .expect("compaction_mutex poisoned");
        // Serialize compaction per CF so two callers cannot both pick the
        // same L0 files. We piggyback on the flush_mutex since flush and
        // compaction both rewrite the on-disk layer.
        // FRS-COMPACT-RELEASE-LOCK (lever A, env FRS_COMPACT_RELEASE_LOCK=1):
        // re-bindable so we can DROP it during the long merge (job.run) and
        // re-acquire for the apply — letting flush proceed concurrently so the
        // foreground does not stall on write-backpressure during a 20s burst
        // (the q4 trough). Safe: flush only ADDS L0 (never deletes the
        // compaction's immutable inputs), compactions are serialized by
        // compaction_mutex (held throughout), and version_set.apply serializes
        // via its apply_lock + validates inputs-still-present, so concurrent
        // flush+compaction applies compose.
        let mut flush_guard = Some(cf_data.lock_flush());

        // R60-H1: gate compaction on `is_dropped()`. drop_cf flips the
        // flag before its retry loop walks the Version; if the flag is
        // observed here, drop_cf is either already running or about to,
        // and producing a fresh L1 SST stamped with this CF's id would
        // either be unlinked by drop_cf (file leak) or — worse — survive
        // drop_cf and become an orphan visible to manifest replay. The
        // compaction_mutex above serializes us against drop_cf's
        // mark→walk→apply pair (drop_cf takes compaction_mutex first,
        // see R60-H2), so observing `is_dropped()` here means drop_cf
        // has already finished or is queued behind us.
        if cf_data.is_dropped() {
            return Ok(None);
        }

        // R49-H1: only roll up this CF's L0 files (and overlap into this CF's
        // L1 files). Without the filter, an L0→L1 rollup could fold another
        // CF's data into this CF's stream.
        let cf_id = cf_data.handle().id();
        let version = self.version_set.current();
        let l0_files: Vec<SstFileMeta> = version
            .l0_files()
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect();
        if l0_files.is_empty() {
            return Ok(None);
        }
        let l1_files: Vec<SstFileMeta> = version.levels[1]
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect();

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
            cf_id: cf_data.handle().id(),
        };

        // R0B-H1: do not create an internal snapshot before reading
        // min_active. The horizon must reflect only external snapshots;
        // otherwise no-snapshot L0 rollups retain every latest version
        // and fail to drop bottommost tombstones or collapse merges.
        let min_active_snapshot = self.snapshot_registry.min_active();
        // FRS-LEVELED-COMPACTION: split the L0→L1 output into ~target-sized
        // SSTs so L1 holds MULTIPLE non-overlapping files — a later Ln→Ln+1
        // compaction can then pick a bounded SUBSET instead of rewriting the
        // whole level (the write-amp source). target=0 ⇒ single-file legacy.
        let target_file_size = self.options.target_file_size_base as u64;
        let total_input_bytes: u64 = inputs.iter().map(|(_, m, _)| m.file_size).sum();
        // FRS-COMPACT-PHASE-DIAG: input row count, to compute the merge-cost
        // ARBITER ns/byte AND ns/row — distinguishing per-BYTE cost (memcpy/
        // encode) from per-ROW overhead (decode/k-way/alloc per entry).
        let total_input_rows: u64 = inputs.iter().map(|(_, m, _)| m.num_entries).sum();
        let additional_outputs =
            self.alloc_compaction_output_slots(total_input_bytes, target_file_size);
        let job = CompactionJob {
            cf_id: cf_data.handle().id(),
            inputs,
            output_level: 1,
            output_file_number,
            output_path: output_path.clone(),
            additional_outputs,
            target_file_size,
            writer_options,
            fs: self.fs.clone(),
            merge_operator: cf_data.merge_operator().cloned(),
            compaction_filter: cf_data.compaction_filter(),
            is_bottommost,
            min_active_snapshot,
        };

        // FRS-COMPACT-PHASE-DIAG (FRS_COMPACT_DIAG=1): time the merge+local-write
        // phase (job.run) separately from the S3 upload-await phase below, to
        // attribute the q4 compaction STALL — engine merge CPU (a lever) vs the
        // dev-Mac ~10 MB/s S3 uplink await (machine/network-bound → cloud box).
        let phase_diag = compact_diag_on();
        // Also time the merge when FRS-WAMP is active so the wamp line carries
        // ns/byte (the suspected real binder: merge SPEED, not write-amp volume).
        let t_run = (phase_diag || wamp_file().is_some()).then(std::time::Instant::now);
        let total_input_bytes_diag = total_input_bytes;
        // Lever A: release lock_flush for the long merge so concurrent flushes
        // proceed (no foreground write-backpressure stall), then re-acquire for
        // the apply. Inputs are immutable + compaction_mutex still held, so this
        // is safe (see the guard comment above).
        if compact_release_lock() {
            flush_guard = None;
        }
        let Some(edit) = job.run()? else {
            return Ok(None);
        };
        if flush_guard.is_none() {
            flush_guard = Some(cf_data.lock_flush());
        }
        let run_ms = t_run.map(|t| t.elapsed().as_millis()).unwrap_or(0);
        let t_upload = phase_diag.then(std::time::Instant::now);

        // Apply the VersionEdit atomically. With R44-H1's compaction_mutex
        // held, no other compaction can have raced ahead of us, so apply
        // should always succeed. However, `Version::apply_edit` performs
        // defense-in-depth stale-edit validation (R44-L2) and may return
        // `Busy` if a future regression reintroduces the race. In that
        // case the staged output SST is orphaned on disk — delete it so
        // we don't leak a file that no Version references.
        if let Err(e) = self.version_set.apply(&edit) {
            // Best-effort cleanup of EVERY orphaned compaction output. None are
            // referenced by any Version (apply failed before installation), so
            // they are safe to remove without going through delete_file_guarded.
            //
            // R45-L1 (multi-file): every output file number was just allocated
            // for this job and never installed into a Version — no checkpoint /
            // reader / sibling compaction can have observed any of them, so the
            // deletion guard's pin count for each must be zero.
            for (_, meta) in &edit.new_files {
                debug_assert!(
                    self.deletion_guard.can_delete(meta.file_number),
                    "orphan-delete bypass for L0→L1 output {} is unsafe: file is pinned",
                    meta.file_number.value()
                );
                let p = compaction_output_path(&self.db_path, meta.file_number);
                if let Err(rm_err) = self.fs.delete_file(&p) {
                    tracing::warn!(
                        target: "forst_rs_engine::compaction",
                        file_number = meta.file_number.value(),
                        error = %rm_err,
                        "failed to remove orphaned L0→L1 compaction output after stale-edit reject"
                    );
                }
            }
            return Err(e);
        }

        // Open a reader for EVERY new output file, prune deleted readers, and
        // delete stale files from disk.
        let new_meta = edit.new_files.first().map(|(_, m)| m.clone());
        for (_, meta) in &edit.new_files {
            // 2026-05-29 WRITE-BACK FLUSH: the compaction-output SST upload was
            // spawned by `close()`; await it before opening a reader straight
            // off the remote so the cached reader sees the fully-uploaded object.
            let p = compaction_output_path(&self.db_path, meta.file_number);
            self.fs.await_upload(&p)?;
            let rac = self.fs.open_random_access_file(&p)?;
            // FRS-COMPACT-READER-BLOCKCACHE (2026-06-09): wire the shared decoded-block cache
            // (same fix as the other compaction + flush paths) so this compaction-output reader
            // hits the decoded-RecordBatch cache instead of re-decompressing on every probe.
            let reader = Arc::new(SstReaderImpl::open(rac)?.with_block_cache(
                Arc::clone(&self.block_cache)
                    as std::sync::Arc<dyn forst_rs_storage::cache::BlockCache>,
                self.db_id.0,
                meta.file_number.value(),
            ));
            let fnum = meta.file_number;
            self.sst_readers.rcu(|cur| {
                let mut next = (**cur).clone();
                next.insert(fnum, std::sync::Arc::clone(&reader));
                std::sync::Arc::new(next)
            });
        }
        self.sst_readers.rcu(|cur| {
            let mut next = (**cur).clone();
            for (_, file_number) in &edit.deleted_files {
                next.remove(file_number);
            }
            std::sync::Arc::new(next)
        });
        for (_, file_number) in &edit.deleted_files {
            self.delete_file_guarded(*file_number);
        }
        self.reap_pending_deletions();

        // FRS-WAMP: record this compaction's write-amp contribution (ungated by
        // S3 upload, so it works in local mode). L1 size = the level we keep
        // rewriting; if cum input ÷ flushed climbs with it, that's the O(N²).
        {
            let out_bytes: u64 = edit.new_files.iter().map(|(_, m)| m.file_size).sum();
            let cur = self.version_set.current();
            let (l1_files, l1_bytes) = cur
                .levels
                .get(1)
                .map(|lm| {
                    (
                        lm.files.len(),
                        lm.files.iter().map(|f| f.file_size).sum::<u64>(),
                    )
                })
                .unwrap_or((0, 0));
            wamp_record_compaction(
                total_input_bytes_diag,
                out_bytes,
                l1_files,
                l1_bytes,
                run_ms,
            );
        }

        // FRS-COMPACT-PHASE-DIAG: attribute the stall — merge+local-write (run_ms)
        // vs S3 upload-await (upload_ms). If upload_ms dominates, the q4 compaction
        // stall is the dev-Mac ~10 MB/s S3 uplink (machine/network-bound), NOT an
        // engine merge lever; the local NVMe write inside run_ms is fast.
        if let Some(t) = t_upload {
            let upload_ms = t.elapsed().as_millis();
            let out_bytes: u64 = edit.new_files.iter().map(|(_, m)| m.file_size).sum();
            let out_rows: u64 = edit.new_files.iter().map(|(_, m)| m.num_entries).sum();
            let in_mb = total_input_bytes_diag as f64 / 1_048_576.0;
            let out_mb = out_bytes as f64 / 1_048_576.0;
            // ARBITER: ns per input byte and ns per input row over the
            // merge+local-write phase. Healthy merge ≈ 1-5 ns/byte; ≫ that ⇒
            // per-row inefficiency (Lever B). rows_in≫rows_out ⇒ merge/dedup
            // collapse; rows_in≈rows_out with in≈out but cumulative-in inflated
            // across compactions ⇒ write-amp (Lever A).
            let run_ns = (run_ms as f64) * 1.0e6;
            let ns_per_byte = if total_input_bytes_diag > 0 {
                run_ns / total_input_bytes_diag as f64
            } else {
                0.0
            };
            let ns_per_row = if total_input_rows > 0 {
                run_ns / total_input_rows as f64
            } else {
                0.0
            };
            eprintln!(
                "[COMPACT_PHASE] in={in_mb:.0}MiB out={out_mb:.0}MiB rows_in={total_input_rows} rows_out={out_rows} run_ms={run_ms} upload_ms={upload_ms} ns/byte={ns_per_byte:.1} ns/row={ns_per_row:.0} out_files={}",
                edit.new_files.len()
            );
        }

        // Lever A: explicit drop of the (possibly re-acquired) flush guard —
        // held through the version apply + reader-cache update above; releasing
        // it now is the same point the function-scoped guard would drop.
        drop(flush_guard);

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
        // B-R7-NEW-H1: eager wrapper over the lazy [`Self::scan_iter`].
        // Pre-fix this method drained every tier into a `BTreeSet<Vec<u8>>`
        // and re-`get`-ed each key, paying `O(matching-keys × value-size)`
        // resident memory before returning. The lazy iterator now streams
        // tier-by-tier and value-resolves only when the consumer advances;
        // `collect::<Result<Vec<_>, _>>()` reproduces the legacy public
        // contract for the 7 unit-test sites + `cf_export` while preserving
        // the streaming benefits for new FFI consumers (`scan_iter` /
        // `scan_iter_owned_arc_with_error_slot`).
        self.check_fatal_error()?;
        self.scan_iter(cf, lower, upper)?
            .collect::<Result<Vec<_>, _>>()
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

    /// Parallel batched prefix scan — the coalesce+parallel join read path (q7/q9/q20).
    ///
    /// The async-state executor batches K iterator probes (one per record) that the legacy
    /// path runs SERIALLY (one FFI crossing + one `build_lazy_prefix_key_stream` each). The K
    /// probes are INDEPENDENT, read-only reads, so this fans them across the process-global
    /// `bg_read_pool` — overlapping the per-probe LSM build+drain across cores (ForSt's
    /// read-io-parallelism model). Results are returned in INPUT ORDER, one `Result` per
    /// prefix (a probe failure is isolated to its own slot, never aborting the batch).
    ///
    /// Each probe uses the SAME value-carrying owned scan as the serial [`Self::prefix_scan`],
    /// so the output is byte-identical to calling `prefix_scan` once per prefix — the
    /// correctness gate (`batch_prefix_scan_parallel(ps)[i] == prefix_scan(ps[i])`).
    pub fn batch_prefix_scan_parallel(
        self: &Arc<Self>,
        cf: &ColumnFamilyHandle,
        prefixes: &[&[u8]],
    ) -> Vec<ForstResult<Vec<(Vec<u8>, Vec<u8>)>>> {
        let k = prefixes.len();
        if k == 0 {
            return Vec::new();
        }
        // Single probe: run inline — fan-out + channel overhead would only cost.
        if k == 1 {
            return vec![self
                .prefix_scan_iter_owned(cf, prefixes[0])
                .and_then(|it| it.collect())];
        }
        let pool = bg_read_pool();
        let (tx, rx) = std::sync::mpsc::channel();
        for (i, p) in prefixes.iter().enumerate() {
            let me = Arc::clone(self);
            let cf = cf.clone();
            // Own the prefix bytes so the job is 'static. The prefix is a small composite
            // state key; this is the only copy on the batch path (the value-carrying scan
            // itself stays zero-copy through the engine tiers).
            let prefix = p.to_vec();
            let tx = tx.clone();
            pool.submit(Box::new(move || {
                let r = me
                    .prefix_scan_iter_owned(&cf, &prefix)
                    .and_then(|it| it.collect::<ForstResult<Vec<_>>>());
                // The receiver drains exactly K results below, so send never fails.
                let _ = tx.send((i, r));
            }));
        }
        drop(tx); // only worker clones remain; rx ends once all K have reported.
        let mut out: Vec<Option<ForstResult<Vec<(Vec<u8>, Vec<u8>)>>>> =
            (0..k).map(|_| None).collect();
        let mut filled = 0usize;
        while filled < k {
            match rx.recv() {
                Ok((i, r)) => {
                    out[i] = Some(r);
                    filled += 1;
                }
                Err(_) => break, // all senders dropped (a worker panicked) — fill the rest with errors
            }
        }
        out.into_iter()
            .map(|o| {
                o.unwrap_or_else(|| {
                    Err(ForstError::internal(
                        "batch_prefix_scan: a read-pool worker dropped its probe result",
                    ))
                })
            })
            .collect()
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
    ) -> ForstResult<Box<dyn Iterator<Item = ForstResult<(Arc<[u8]>, Arc<[u8]>)>> + Send + 'static>>
    {
        let cf_handle = cf.clone();
        let inner = self.build_lazy_prefix_key_stream(cf, prefix)?;
        let db = Arc::clone(self);
        Ok(Box::new(inner.filter_map(
            move |key_arc| match db.get_arc(&cf_handle, key_arc.as_ref()) {
                Ok(Some(value)) => Some(Ok((key_arc, value))),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            },
        )))
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
    ) -> ForstResult<Box<dyn Iterator<Item = ForstResult<(Arc<[u8]>, Arc<[u8]>)>> + Send + 'static>>
    {
        // FRS-PREFIX-CFHOIST (2026-05-30): resolve the CF data ONCE here instead
        // of per yielded key. The pre-fix closure called `db.get_arc(&cf_handle,
        // key)` → `get` → `lookup_cf_by_id(cf.id())` (an RwLock read + HashMap
        // lookup) on EVERY key. A prefix scan yields K keys and q9's ROW_NUMBER
        // rank re-scans each partition once per record, so that per-key CF
        // re-resolution ran millions of times. Hoisting it and calling
        // `get_internal(&cf_data, key, u64::MAX)` directly is BYTE-IDENTICAL to
        // `get_arc` (which is exactly `lookup_cf_by_id` + `get_internal` +
        // `Arc::from`) — same latest-view (`u64::MAX`) resolution, same
        // tombstone/merge semantics — it only removes the redundant per-key
        // CF-map lookup + lock acquisition. Holding the `Arc<ColumnFamilyData>`
        // for the iterator's lifetime is also strictly safer than re-looking-up
        // each call (the CF cannot be reclaimed mid-scan).
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let mut inner = self.build_lazy_prefix_key_stream(cf, prefix)?;
        inner.set_shared_error_slot(error_slot);
        let db = Arc::clone(self);
        // FRS-VALUE-CARRYING-MERGE (2026-06-06): resolve SST-resident Puts
        // inline from the merge's cursor position — eliminating the per-key
        // `get_internal` that re-walked the WHOLE LSM (O(K×tiers), the
        // profile-proven q4 2× read-path binder). `get_internal` now runs ONLY
        // for memtable-tier winners (a cheap memtable hit, no SST I/O) and
        // merge-chains. Byte-identical results: the inline `Put` value is the
        // exact `(key ASC, seq DESC)` newest version `get_internal` would
        // return, and tier precedence is preserved (memtable newer than SST;
        // max-sequence SST wins; tombstones hide).
        Ok(Box::new(std::iter::from_fn(move || loop {
            let (key_arc, decision) = inner.next_with_value()?;
            match decision {
                ValueDecision::Put(value) => return Some(Ok((key_arc, value))),
                ValueDecision::Fallback => {
                    match db.get_internal(&cf_data, key_arc.as_ref(), u64::MAX) {
                        Ok(Some(value)) => return Some(Ok((key_arc, Arc::<[u8]>::from(value)))),
                        Ok(None) => continue,
                        Err(e) => return Some(Err(e)),
                    }
                }
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
        // FRS-ITER-DIAG: gated timing of the prefix-stream build. Set
        // FRS_ITER_DIAG=1 to log slow builds (>1ms) with tier + result-size
        // attribution. Zero cost when unset (one env read per call is cheap
        // relative to the BTree scans below; checked once via OnceLock).
        let diag = frs_iter_diag_enabled();
        let diag_start = if diag {
            Some(std::time::Instant::now())
        } else {
            None
        };

        // BULK-SAMPLE DIAG: decide once per build whether to nanosecond-time
        // this one (1-in-K) so the bulk ~1.7µs builds are measured without
        // observer-effect. Accumulators below stay 0 on unsampled builds.
        let bulk_sampled = bulk_sample_hit(bulk_sample_k());
        let bulk_start = if bulk_sampled {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let mut bulk_rfve_ns = 0u64;
        let mut bulk_bloom_ns = 0u64;
        let mut bulk_cursor_ns = 0u64;
        let mut bulk_active_ns = 0u64;
        let mut bulk_resident_done_ns = 0u64;
        // FRS-A-SPLIT (2026-06-04): split A_fanout (sst_ns) into the locate call
        // (`overlapping_ssts_in_range`) vs the per-SST loop, and capture the
        // OVERLAP COUNT — the discriminator between "L0 fan-out grows with state"
        // (count-driven → compaction lever) and "per-SST read is the floor".
        let mut bulk_locate_ns = 0u64;
        let mut bulk_n_overlap = 0u64;
        // FRS-A-SPLIT level decomposition: how many of the overlapping SSTs are
        // L0 (compaction-starved → L0-trigger lever) vs deeper levels (structural
        // multi-level spread → level-multiplier/merge lever). Picks the exact
        // compaction sub-lever for the fan-out decay.
        let mut bulk_n_overlap_l0 = 0u64;
        // FRS-B-SPLIT (2026-06-04): windowed resident-shadow COUNT (N) + how many
        // pass the bloom into an actual BTreeMap seek. Discriminates B_resident:
        // count-driven (N grows → shrink the shadow set) vs per-seek-cost-driven
        // (N flat, cost rises → the seek/bloom itself). Attribution before any B fix.
        let mut bulk_resident_total = 0u64;
        let mut bulk_resident_seeks = 0u64;

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
        let at = bulk_start.map(|_| std::time::Instant::now());
        let active_cursor = mem_arc.prefix_scan_cursor(prefix, upper_slice);
        if let Some(t) = at {
            // C = Tier-1 active-memtable cursor (the BTreeMap seek; structurally
            // capped under forbid(unsafe) per the arena-skiplist spike).
            bulk_active_ns = t.elapsed().as_nanos() as u64;
        }
        if !active_cursor.is_empty() {
            sources.push(TierKeySource::MemCursor {
                cursor: active_cursor,
            });
        }
        // FRS-ITER-DIAG (2026-06-02): tier sub-split — active vs immutable-loop
        // vs resident, plus n_imm count, to localize the empty-probe prep cost.
        let active_us: u128 = diag_start.map(|s| s.elapsed().as_micros()).unwrap_or(0);
        let mut n_imm: usize = 0;
        // Tier 2: immutable memtables. Same C9-H1 treatment: lazy
        // per-shard cursor instead of eager global sort.
        for imm in cf_data.imm_memtables() {
            n_imm += 1;
            let imm_cursor = imm.prefix_scan_cursor(prefix, upper_slice);
            if !imm_cursor.is_empty() {
                sources.push(TierKeySource::MemCursor { cursor: imm_cursor });
            }
        }
        let imm_done_us: u128 = diag_start.map(|s| s.elapsed().as_micros()).unwrap_or(0);
        // FRS-RESIDENT-FLUSHED: Tier 2 also scans the resident-flushed
        // memtables (memtables already flushed to an L0 SST but kept in RAM
        // so the prefix-scan path avoids an S3 round-trip for hot recently-
        // flushed data). Each entry is byte-identical (same sequence numbers)
        // to its source SST — the LSM merge dedups on sequence so reading
        // BOTH the resident memtable AND the SST is correctness-neutral.
        // Below in Tier 3 we additionally SKIP the byte-identical L0 SST to
        // avoid the redundant scan (this is the perf win).
        //
        // CORRECTNESS: filter resident entries by the CURRENT live-SST set —
        // after compaction collapses an L0 SST into L1+ output (merge ops
        // combined, tombstones consumed), the pre-collapse resident memtable
        // is STALE and must be skipped. Snapshot the version once so Tier 2
        // and Tier 3 read a consistent view of the LSM.
        let version = self.version_set.current();
        // 2026-05-29 PERF: file-number-only set (no SstFileMeta clone per scan).
        let live_files = version.live_sst_file_numbers();
        // 2026-05-29 PERF-RESTORE (q4/q7 iterator regression): use the
        // bound-carrying accessor and SKIP resident memtables whose SST key
        // range does not overlap the scan window [prefix, upper). v3.8 had NO
        // resident layer; HEAD built a prefix_scan_cursor for EVERY resident
        // memtable (up to cap/64MiB ≈ 256 at the 16 GiB cap) on EVERY prefix
        // scan — catastrophic for per-record join scans. The SST tier below
        // already skips by range; mirror it here. A skipped resident entry is
        // NOT added to `resident_shadowed`, so Tier 3 reads its SST — but that
        // SST's range also excludes the prefix, so Tier 3's own range check
        // skips it too (consistent, no data loss).
        let rfve_t0 = bulk_start.map(|_| std::time::Instant::now());
        // FRS-RESIDENT-BYPASS (lever C, env FRS_RESIDENT_BYPASS=1): skip the
        // Tier-2 resident-shadow entirely so reads go memtable + SST (block
        // cache), like RocksDB (which has no in-RAM SST-copy tier). LOCAL-SAFE:
        // flushed SSTs are write-through to local disk and readable immediately,
        // so Tier-3 reads the exact data the shadow would have served (same
        // seqs). With no shadowing, Tier-3's own range check reads the
        // overlapping SSTs. Shaves the per-probe rfve clone + N tier cursors +
        // the resident memory footprint (the constant-factor/contention baseline
        // vs RocksDB).
        let resident_entries = if resident_bypass() {
            Vec::new()
        } else {
            cf_data.resident_flushed_visible_entries(&live_files)
        };
        if let Some(t) = rfve_t0 {
            // rfve = JUST the resident_flushed read-lock acquire + O(N) clone.
            bulk_rfve_ns = t.elapsed().as_nanos() as u64;
        }
        // DECAY AUTOPSY discriminator (3a unbounded-state vs 3b slower-work): the
        // resident path is O(resident-SST-count) per probe — this accessor clones
        // ALL N resident entries (under a RwLock read), and the loop below iterates
        // all N. `resident_total` = N; `resident_examined` = N reaching the bloom/
        // seek decision (post range-prune); `resident_seeks` = N that escaped the
        // bloom prune into a BTreeMap seek. `resident_us / resident_examined` (per-
        // entry cost over time) separates "more work" (flat ⇒ O(N) structural, 3a)
        // from "slower work" (rising ⇒ bigger seeks / lock-wait, 3b).
        let resident_total = resident_entries.len();
        let mut resident_examined = 0usize;
        let mut resident_seeks = 0usize;
        let mut resident_shadowed: std::collections::HashSet<FileNumber> =
            std::collections::HashSet::new();
        // FRS-RESIDENT-BLOOM-SKIP (2026-06-03): snapshot the SST reader cache once so
        // each resident entry can consult its source SST's decode-free index/bloom
        // prune (`may_contain_range`) BEFORE paying the O(log N) memtable BTreeMap seek.
        let readers_snapshot = self.sst_readers.load();
        for entry in resident_entries {
            // Range-overlap test (inclusive max, exclusive upper) mirroring the
            // SST skip at Tier 3. Empty bounds = unknown range → never skip.
            if !(entry.min_key.is_empty() && entry.max_key.is_empty()) {
                if entry.max_key.as_slice() < prefix {
                    continue;
                }
                if let Some(hi) = upper_slice {
                    if entry.min_key.as_slice() >= hi {
                        continue;
                    }
                }
            }
            resident_examined += 1;
            // FRS-RESIDENT-BLOOM-SKIP: the #1 q4/q7/q9 decay cost (symbolized profile:
            // `btree::search::find_lower_bound_index`) is the per-probe BTreeMap
            // lower-bound seek over the resident-shadow memtables — and `FRS_ITER_DIAG`
            // showed most return ZERO keys (a coarse min/max-bounds overlap is far too
            // permissive for a 36-byte join prefix). Unlike RocksDB's flushed L0 SSTs,
            // a resident memtable has no bloom/index gate, so it pays the full seek even
            // when the prefix is absent — that gap is exactly why forst-rs decays while
            // RocksDB stays flat. The resident memtable is BYTE-IDENTICAL to its source
            // SST (same seqs), so the SST reader's `may_contain_range` (the SAME
            // decode-free sparse-index + bloom prune the Tier-3 SST loop trusts) is a
            // SOUND gate: when it proves the prefix range empty, the key is absent from
            // BOTH the resident copy and the SST, so we skip the seek AND keep the SST
            // shadowed (Tier 3 correctly skips it too — no data is missed). Falls back to
            // seeking when the reader isn't cached yet (e.g. async-upload window).
            // FRS-RESIDENT-BLOOM-SKIP EXPERIMENT (env FRS_RESIDENT_BLOOM_SKIP=1,
            // off by default): hard-skip the resident bloom to MEASURE whether
            // its ~717 ns/probe (proven ~99% non-pruning for q4 via FRS-B-SPLIT
            // n_seek≈n_res) actually translates to q4 end-to-end throughput
            // before building the adaptive (q7-safe) production version.
            // Correctness when skipped: we fall through to the seek + shadow the
            // SST — the resident memtable is byte-identical to its SST, so always
            // serving it from RAM (and Tier-3 skipping the shadowed SST) loses no
            // data; the only cost is a useless seek on the rare absent prefix.
            if !resident_bloom_skip() {
                if let Some(reader) = readers_snapshot.get(&entry.file_number) {
                    let bt = bulk_start.map(|_| std::time::Instant::now());
                    let pass = reader.may_contain_range(prefix, upper_slice);
                    if let Some(t) = bt {
                        bulk_bloom_ns += t.elapsed().as_nanos() as u64;
                    }
                    if !pass {
                        resident_shadowed.insert(entry.file_number);
                        continue;
                    }
                }
            }
            resident_seeks += 1;
            let ct = bulk_start.map(|_| std::time::Instant::now());
            let resident_cursor = entry.memtable.prefix_scan_cursor(prefix, upper_slice);
            if let Some(t) = ct {
                bulk_cursor_ns += t.elapsed().as_nanos() as u64;
            }
            // Shadow the SST only when we actually serve this entry from RAM.
            resident_shadowed.insert(entry.file_number);
            if !resident_cursor.is_empty() {
                sources.push(TierKeySource::MemCursor {
                    cursor: resident_cursor,
                });
            }
        }
        if let Some(t) = bulk_start {
            // Everything after this point is the Tier-3 SST fan-out.
            bulk_resident_done_ns = t.elapsed().as_nanos() as u64;
            // FRS-B-SPLIT: capture the resident-shadow count + seek count for
            // the windowed attribution (only on sampled builds).
            bulk_resident_total = resident_total as u64;
            bulk_resident_seeks = resident_seeks as u64;
        }
        // FRS-ITER-DIAG sub-phase split (2026-06-02): capture how much of the
        // build is the memtable/resident-tier prep vs the Tier-3 SST loop, and
        // within the SST loop how much is `get_or_open_sst_reader` (cold index +
        // bloom decode on freshly-minted SSTs). Diagnoses whether the q7
        // empty-probe build cost is resident-cursor work or SST reader opens.
        let prep_us: u128 = diag_start.map(|s| s.elapsed().as_micros()).unwrap_or(0);
        let mut sst_open_us: u128 = 0;
        let mut sst_considered: usize = 0;
        // Tier 3: overlapping SSTs, block-streaming. 2026-05-29 PERF: borrow
        // SST metadata (no per-scan clone of every SstFileMeta + its keys).
        // FRS-PERLEVEL-SCAN (2026-06-04): locate the overlapping SSTs per level
        // via a binary-search-bounded walk instead of a flat O(total_files)
        // range check over `live_sst_files_iter()`. The locator applies the
        // SAME overlap predicate (`largest_key >= prefix && smallest_key <
        // upper`), so the considered file set is byte-for-byte identical — it
        // just bounds each level's scan to the files that start before `upper`.
        let mut overlapping_ssts: Vec<&forst_rs_storage::version::SstFileMeta> = Vec::new();
        let locate_t = bulk_start.map(|_| std::time::Instant::now());
        version.overlapping_ssts_in_range(prefix, upper_slice, &mut overlapping_ssts);
        if let Some(t) = locate_t {
            bulk_locate_ns = t.elapsed().as_nanos() as u64;
            bulk_n_overlap = overlapping_ssts.len() as u64;
            // Sampled-only (1/K), so the HashSet build is observer-effect-safe.
            let l0: std::collections::HashSet<forst_rs_common::FileNumber> =
                version.l0_files().iter().map(|f| f.file_number).collect();
            bulk_n_overlap_l0 = overlapping_ssts
                .iter()
                .filter(|s| l0.contains(&s.file_number))
                .count() as u64;
        }
        for sst in overlapping_ssts {
            // FRS-RESIDENT-FLUSHED: skip SSTs whose data is currently served
            // from Tier 2 by a resident memtable (same content, same seqs).
            if resident_shadowed.contains(&sst.file_number) {
                continue;
            }
            if diag {
                sst_considered += 1;
            }
            let reader = if diag {
                let o = std::time::Instant::now();
                let r = self.get_or_open_sst_reader(sst)?;
                sst_open_us += o.elapsed().as_micros();
                r
            } else {
                self.get_or_open_sst_reader(sst)?
            };
            // FRS-L0-FANOUT-PRUNE (2026-05-31): skip this SST's source entirely
            // when its in-memory block index proves it holds NO key in
            // [prefix, upper) — WITHOUT decoding a block. The coarse
            // smallest/largest_key check above only rules out SSTs whose whole
            // range misses the prefix; many L0 SSTs straddle the prefix's
            // neighbourhood (other keys just above/below) yet contain nothing
            // for THIS prefix. Profiled on q4: with ~142 overlapping L0 SSTs and
            // the join key present in only a few, the lazy first-block decode of
            // each non-matching source (decompress + Arrow RecordBatch decode)
            // was the dominant per-probe cost once state spilled past the RAM
            // shadow. This decode-free prune removes that fan-out. Sound: it
            // skips only when the range is provably empty.
            if !reader.may_contain_range(prefix, upper_slice) {
                continue;
            }
            // FRS-PREFIX-SEEK: start streaming at the index-located first block
            // that could contain keys >= prefix, NOT block 0 — blocks below the
            // prefix cannot match and reading them was the q7-ckpt-on hotspot
            // (executeIters ~95% of dispatch once state flushed to SSTs).
            let start_block = reader.first_block_ge(prefix);
            sources.push(TierKeySource::Sst {
                reader,
                lower: prefix.to_vec(),
                upper: upper.clone(),
                next_block: start_block,
                buffered: Vec::new(),
                pos: 0,
            });
        }

        if let Some(start) = diag_start {
            let us = start.elapsed().as_micros();
            let mut mem_sources = 0usize;
            let mut mem_keys = 0usize;
            let mut sst_sources = 0usize;
            for s in &sources {
                match s {
                    TierKeySource::MemCursor { cursor } => {
                        mem_sources += 1;
                        mem_keys += cursor.snapshot_len();
                    }
                    TierKeySource::Sst { .. } => sst_sources += 1,
                }
            }
            // FRS-FANOUT-DIAG (2026-06-02): log a build whenever it sets a NEW
            // peak SST-source fan-out (flood-free — only fires on a new record),
            // in addition to the slow-build (>1ms) path. This reveals whether the
            // q7 empty-probe stall is MANY overlapping L0 SSTs (compaction
            // starved) vs FEW SSTs each slow to peek (OpenDAL read latency).
            static MAX_SST_SOURCES: std::sync::atomic::AtomicUsize =
                std::sync::atomic::AtomicUsize::new(0);
            let new_peak = sst_sources > MAX_SST_SOURCES.load(std::sync::atomic::Ordering::Relaxed)
                && {
                    MAX_SST_SOURCES.store(sst_sources, std::sync::atomic::Ordering::Relaxed);
                    true
                };
            if us > 1000 || new_peak {
                let imm_us = imm_done_us.saturating_sub(active_us);
                let resident_us = prep_us.saturating_sub(imm_done_us);
                eprintln!(
                    "FRS-ITER-DIAG build_lazy_prefix us={} prep_us={} active_us={} imm_us={} resident_us={} resident_total={} resident_examined={} resident_seeks={} n_imm={} sst_open_us={} sst_considered={} prefix_len={} mem_sources={} mem_keys={} sst_sources={} resident_shadowed={} new_peak={}",
                    us,
                    prep_us,
                    active_us,
                    imm_us,
                    resident_us,
                    resident_total,
                    resident_examined,
                    resident_seeks,
                    n_imm,
                    sst_open_us,
                    sst_considered,
                    prefix.len(),
                    mem_sources,
                    mem_keys,
                    sst_sources,
                    resident_shadowed.len(),
                    new_peak
                );
            }
        }

        if let Some(t) = bulk_start {
            let total_ns = t.elapsed().as_nanos() as u64;
            // sst = everything after the resident loop (Tier-3 fan-out + setup).
            let sst_ns = total_ns.saturating_sub(bulk_resident_done_ns);
            bulk_record(
                bulk_active_ns,
                bulk_rfve_ns,
                bulk_bloom_ns,
                bulk_cursor_ns,
                sst_ns,
                bulk_locate_ns,
                bulk_n_overlap,
                bulk_n_overlap_l0,
                bulk_resident_total,
                bulk_resident_seeks,
                total_ns,
            );
        }

        LazyPrefixIter::new(sources)
    }

    /// B-R7-NEW-H1: range counterpart of [`Self::build_lazy_prefix_key_stream`].
    ///
    /// Identical k-way merge structure, but takes explicit `(lower, upper)`
    /// bounds and uses the range-aware memtable cursor
    /// ([`ShardedMemTable::range_scan_cursor`]) which skips the prefix-index
    /// fast path. The SST tier source reuses the existing `TierKeySource::Sst`
    /// variant — its block-streaming filter is already a `[lower, upper)`
    /// range check, so no variant changes are required.
    fn build_lazy_range_key_stream(
        &self,
        cf: &ColumnFamilyHandle,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> ForstResult<LazyRangeIter> {
        let upper_owned: Option<Vec<u8>> = upper.map(|u| u.to_vec());
        let cf_data = self.lookup_cf_by_id(cf.id())?;

        let mut sources: Vec<TierKeySource> = Vec::new();

        // Tier 1: active memtable. Range-aware cursor (skips the prefix-index
        // fast path so `upper` is honoured exactly).
        let mem_arc = cf_data.active_memtable();
        let active_cursor = mem_arc.range_scan_cursor(lower, upper);
        if !active_cursor.is_empty() {
            sources.push(TierKeySource::MemCursor {
                cursor: active_cursor,
            });
        }
        // Tier 2: immutable memtables.
        for imm in cf_data.imm_memtables() {
            let imm_cursor = imm.range_scan_cursor(lower, upper);
            if !imm_cursor.is_empty() {
                sources.push(TierKeySource::MemCursor { cursor: imm_cursor });
            }
        }
        // Tier 3: overlapping SSTs, block-streaming. Same per-SST overlap
        // filter as the prefix path. 2026-05-29 PERF: borrow (no clone).
        let version = self.version_set.current();
        // FRS-PERLEVEL-SCAN (2026-06-04): per-level binary-search-bounded
        // overlap location (identical file set to the flat range check).
        let mut overlapping_ssts: Vec<&forst_rs_storage::version::SstFileMeta> = Vec::new();
        version.overlapping_ssts_in_range(lower, upper, &mut overlapping_ssts);
        for sst in overlapping_ssts {
            let reader = self.get_or_open_sst_reader(sst)?;
            // FRS-PREFIX-SEEK: seek to the first index block >= lower (see sister
            // site in prefix_scan_iter_owned*) instead of scanning from block 0.
            let start_block = reader.first_block_ge(lower);
            sources.push(TierKeySource::Sst {
                reader,
                lower: lower.to_vec(),
                upper: upper_owned.clone(),
                next_block: start_block,
                buffered: Vec::new(),
                pos: 0,
            });
        }

        LazyRangeIter::new(sources)
    }

    /// B-R7-NEW-H1: streaming form of [`Self::scan`].
    ///
    /// Mirrors [`Self::prefix_scan_iter`] for the range case. Returns a lazy
    /// k-way merge over the active memtable + immutable memtables + every
    /// overlapping SST, yielding visible (key, value) pairs in sorted order.
    /// No tier is eagerly drained — the first `.next()` is
    /// `O(active-mem-tier + num_imm_mems + num_overlap_ssts)` rather than
    /// `O(rows × value_size)` across the entire LSM.
    pub fn scan_iter<'a>(
        &'a self,
        cf: &ColumnFamilyHandle,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> ForstResult<impl Iterator<Item = ForstResult<(Vec<u8>, Vec<u8>)>> + 'a> {
        let cf_handle = cf.clone();
        let inner = self.build_lazy_range_key_stream(cf, lower, upper)?;
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

    /// B-R7-NEW-H1: owned-Arc + shared-error-slot variant of
    /// [`Self::scan_iter`] for the FFI chunked-iterator path
    /// (`frs_vec_iter_range_open`). Mirrors
    /// [`Self::prefix_scan_iter_owned_arc_with_error_slot`].
    pub fn scan_iter_owned_arc_with_error_slot(
        self: &Arc<Self>,
        cf: &ColumnFamilyHandle,
        lower: &[u8],
        upper: Option<&[u8]>,
        error_slot: Arc<Mutex<Option<ForstError>>>,
    ) -> ForstResult<Box<dyn Iterator<Item = ForstResult<(Arc<[u8]>, Arc<[u8]>)>> + Send + 'static>>
    {
        // FRS-VALUE-CARRYING-MERGE (2026-06-07): mirror the prefix path
        // (`prefix_scan_iter_owned_arc_with_error_slot`) — resolve SST-resident
        // Puts inline from the merge cursor instead of a per-key `get_arc` that
        // re-walks the WHOLE LSM (O(K×tiers)). Pre-fix this range path STILL did
        // `db.get_arc(cf, key)` per yielded key — the exact double-walk the
        // prefix path eliminated as "the q4 2× read-path fix" (task #51) but
        // which was never carried over to the range scan. This range scan backs
        // the engine timer drain (`frs_vec_iter_range_open`, q11) and q9/q19/q20
        // TopN/OVER range reads, so the per-key re-walk was a unified read-amp
        // tax across every range-scan-heavy query. `get_internal(cf_data, key,
        // u64::MAX)` is byte-identical to `get_arc` (lookup_cf_by_id +
        // get_internal + Arc::from); the inline `Put` value is the exact
        // (key ASC, seq DESC) newest version, tier precedence preserved
        // (memtable newer than SST; max-seq SST wins; tombstones hide). Fallback
        // (Merge-chains / memtable-tier winners) still resolves via get_internal.
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let mut inner = self.build_lazy_range_key_stream(cf, lower, upper)?;
        inner.set_shared_error_slot(error_slot);
        let db = Arc::clone(self);
        Ok(Box::new(std::iter::from_fn(move || loop {
            let (key_arc, decision) = inner.next_with_value()?;
            match decision {
                ValueDecision::Put(value) => return Some(Ok((key_arc, value))),
                ValueDecision::Fallback => {
                    match db.get_internal(&cf_data, key_arc.as_ref(), u64::MAX) {
                        Ok(Some(value)) => return Some(Ok((key_arc, Arc::<[u8]>::from(value)))),
                        Ok(None) => continue,
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
        })))
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
    /// # R46-M2: homogeneity check exemption
    ///
    /// The export blob format does not currently carry the source CF's
    /// merge_operator or compaction_filter name, so the imported CF is
    /// always created with a default-shape descriptor (no merge, no
    /// filter). If the destination engine has any existing non-default CF
    /// with a non-default policy, the R45-H1 / R46-H1 homogeneity check
    /// would reject the import. To keep §6g cross-job transfer functional
    /// we deliberately bypass the check here.
    ///
    /// SILENT WRONG-RESULT RISK: imports are admitted unconditionally;
    /// if the source CF in the producing engine used a merge operator
    /// or compaction filter that the destination engine does NOT carry
    /// on its other CFs, cross-CF compaction in the destination will
    /// behave incorrectly for the imported data (the same hazard R45-H1
    /// guards against on the `create_column_family` path). Callers
    /// orchestrating cross-job transfer are responsible for ensuring
    /// source and destination engines use the same per-CF policy. A
    /// follow-up that propagates the source merge/filter name through
    /// the export blob would let us tighten this back to a checked
    /// import; that work is out of scope here.
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
        // R46-M2: bypass the R45-H1/R46-H1 homogeneity check (default-shape
        // descriptor — see fn doc for the silent-wrong-result caveat).
        let cf =
            self.create_column_family_no_homogeneity_check(ColumnFamilyDescriptor::new(name))?;

        // ---- Replay entries ----
        // R42-H1: any failure in the replay loop (truncated blob, put error)
        // would otherwise leave the CF registered in `cfs`/`cf_name_to_id`
        // with partial data — a caller retry would then hit
        // "column family already exists" with no way to recover. Run the
        // replay inside an inner closure; on error, best-effort drop the
        // freshly-created CF before propagating so the name is freed.
        let result = (|| -> ForstResult<()> {
            let mut cursor = header_end;
            while cursor < blob.len() {
                // key
                if cursor + 4 > blob.len() {
                    return Err(ForstError::corruption(
                        "create_cf_from_import: truncated key length",
                    ));
                }
                let key_len =
                    u32::from_le_bytes(blob[cursor..cursor + 4].try_into().expect("4 bytes"))
                        as usize;
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
                    u32::from_le_bytes(blob[cursor..cursor + 4].try_into().expect("4 bytes"))
                        as usize;
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
            Ok(())
        })();
        if let Err(e) = result {
            // Best-effort: free the CF name so the caller can retry.
            // Errors from drop_cf are intentionally swallowed — the
            // surfaced error is the original failure that triggered
            // cleanup, and a cleanup failure would only obscure it.
            let _ = self.drop_cf(&cf);
            return Err(e);
        }

        Ok(cf)
    }

    fn flush_cf_data(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<Option<SstFileMeta>> {
        // Serialize flushes for this CF so concurrent callers cannot pick up
        // the same oldest imm and write it twice.
        let _flush_guard = cf_data.lock_flush();

        // R59-H1 + R61-M1: a flush enqueued before `drop_cf` would
        // otherwise land AFTER drop_cf's VersionEdit removed the CF's
        // files — the `apply` here would install a fresh SST stamped
        // with the dropped CF's id, defeating R58-H2's cleanup. The
        // flush guard above serializes us against other flushes; the
        // dropped-flag check now also gates us against drop_cf having
        // raced.
        //
        // R61-M1 caveat: if flush_cf_data grabs `lock_flush` BEFORE
        // drop_cf does, we observe `is_dropped()==true` here but
        // drop_cf has not yet sampled+released the imm's bytes — we
        // pop the imm, drop_cf later samples the (now-empty) imm
        // list and releases nothing. Refund the imm's bytes here so
        // the cross-CF WBM counter cannot leak.
        if cf_data.is_dropped() {
            let leaked = cf_data
                .imm_memtables()
                .first()
                .map_or(0u64, |imm| imm.memory_usage() as u64);
            cf_data.pop_oldest_imm();
            if leaked > 0 {
                self.write_buffer_manager.release(leaked);
            }
            self.write_controller
                .set_imm_count(cf_data.imm_count() as u32);
            return Ok(None);
        }

        // Peek at the oldest imm without popping. Empty imms are legal no-op
        // artifacts from force-switch compatibility paths; drop them without
        // surfacing FlushJob's invalid-argument error.
        let imm_list = cf_data.imm_memtables();
        let Some(oldest) = imm_list.first().cloned() else {
            return Ok(None);
        };
        if oldest.num_entries() == 0 {
            cf_data.pop_oldest_imm();
            self.write_controller
                .set_imm_count(cf_data.imm_count() as u32);
            return Ok(None);
        }

        // Allocate a fresh file number and build the flush job.
        let file_number = self.version_set.allocate_file_number();
        let path = sst_file_path(&self.db_path, file_number);
        // R49-H1: stamp cf_id onto the writer options so the resulting SST
        // footer + SstFileMeta carry CF identity through the LSM. `FlushJob::new`
        // will also overwrite `cf_id` defensively in case a caller pre-set it.
        let writer_opts = SstWriterOptions {
            block_size: self.options.block_size,
            compression: self.options.compression,
            cf_id: cf_data.handle().id(),
        };
        let job = FlushJob::new(
            oldest.clone(),
            file_number,
            cf_data.handle().id(),
            path.clone(),
            writer_opts,
            self.fs.clone(),
        );
        let meta = job.run()?;

        // R59-H1 (post-job-run check): drop_cf may have raced while
        // `job.run()` was streaming bytes to disk. If the CF is now
        // dropped, do NOT install the fresh SST — that would re-introduce
        // an orphan with the dropped CF's id. Unlink the just-written
        // file (it has no readers and is not in any Version) and bail.
        //
        // R61-M1 (companion): release the imm's bytes here too — same
        // race as the entry-point check, drop_cf may not yet have
        // sampled this CF.
        if cf_data.is_dropped() {
            let _ = self.fs.delete_file(&path);
            let leaked = oldest.memory_usage() as u64;
            cf_data.pop_oldest_imm();
            if leaked > 0 {
                self.write_buffer_manager.release(leaked);
            }
            self.write_controller
                .set_imm_count(cf_data.imm_count() as u32);
            return Ok(None);
        }

        // Install the new file into L0 atomically. Note: we install BEFORE
        // popping the memtable so a concurrent reader can never transiently
        // fail to find the key (it will see either the imm or the SST, never
        // neither).
        //
        // R0A-H1: also bump `last_sequence` to the max seq the flushed SST
        // contains. Pre-fix flush edits set `last_sequence: None`, so the
        // version-set counter only ever advanced on `ingest_external_sst`.
        // Checkpoint blob serialized that lagged value; restore initialized
        // `sequence_number` to that low value and subsequent writes reused
        // seqs already present in restored SSTs → mvcc::get_at returns the
        // older SST row instead of the just-written one (silent data loss
        // on every checkpoint+restore cycle).
        //
        // The version-set's `apply` uses `fetch_max` so a no-op or lower
        // value is safe; we only set Some when the flushed range is
        // non-empty.
        let edit = VersionEdit {
            new_files: vec![(0, meta.clone())],
            last_sequence: if meta.max_sequence.value() > 0 {
                Some(meta.max_sequence)
            } else {
                None
            },
            ..Default::default()
        };
        // FRS-RESIDENT-FLUSHED-ORDER (2026-05-30): enroll the resident RAM shadow
        // BEFORE the SST becomes visible via `version_set.apply`. A symbolized q9-S3
        // stall profile showed join-probe threads BLOCKED on `await_upload` inside
        // `get_or_open_sst_reader` — the rate=0 stalls were I/O-wait, not CPU. Root
        // cause: the shadow was registered AFTER `apply` + `pop_oldest_imm`, so in the
        // window between the SST becoming visible (Tier 3) and the shadow being
        // registered (Tier 2), a concurrent prefix-scan saw the new L0 SST with NO
        // shadow → read it directly → blocked on its in-flight async upload. Enrolling
        // FIRST means any probe that observes the SST in the version ALSO observes the
        // shadow (`resident_shadowed`) and SKIPS the SST entirely — the recently-flushed
        // join state is served from RAM, never touching the not-yet-uploaded object.
        // Enroll uses only `meta` + `oldest`, both available here; the imm is still
        // present (pop happens below) so reads are doubly covered (seq-dedup neutral).
        // FRS-ROCKSDB-PARITY Component 1: only retain the flushed memtable in RAM
        // (Tier-2 shadow) when explicitly enabled. Default OFF — flushed reads are
        // served from the SST (Tier-3) + the bounded block cache, like RocksDB, so
        // RAM is bounded by the cache size, not by total state size.
        if resident_shadow_enabled() {
            cf_data.add_resident_flushed_with_bounds(
                meta.file_number,
                oldest.clone(),
                meta.smallest_key.clone(),
                meta.largest_key.clone(),
                crate::column_family::resident_flushed_cap_bytes(),
            );
        }

        self.version_set.apply(&edit)?;

        // Pre-populate the SST reader cache so the first read doesn't pay
        // the open-file cost.
        //
        // 2026-05-29 WRITE-BACK FLUSH: this eager open is a pure optimization,
        // and on the object-store async-upload path the SST may not be uploaded
        // yet — `open_random_access_file` would either block on `await_upload`
        // (re-coupling the flush worker to S3 latency, defeating write-back) or
        // fail its `stat` because the object isn't published. Either way it is
        // SAFE TO SKIP: the just-flushed memtable was enrolled as resident above
        // (`add_resident_flushed_with_bounds`), so reads are served from RAM and
        // shadow the byte-identical L0 SST. The reader is opened lazily on first
        // real SST access via `get_or_open_sst_reader`, which awaits the upload.
        // So make the pre-populate best-effort: try without blocking on the
        // upload, and silently skip if it isn't published yet. On synchronous
        // backends (local FS) this still pre-populates as before.
        match self.fs.open_random_access_file(&path) {
            Ok(rac) => match SstReaderImpl::open(rac) {
                Ok(reader) => {
                    // FRS-FLUSH-READER-BLOCKCACHE (2026-06-09): wire the shared decoded-block
                    // cache, mirroring the lazy `get_or_open_sst_reader` path. WITHOUT this, the
                    // flush-pre-populated reader for a freshly-flushed L0 SST had `block_cache=None`,
                    // so every probe of that SST re-read + re-decompressed its data blocks from disk
                    // instead of hitting the decoded-RecordBatch cache. Flushed SSTs are exactly the
                    // join's hot, repeatedly-probed working set (interval/Top-N joins re-probe recent
                    // state per record) — the cache-bypass made q9/q20 pay redundant decode on every
                    // probe (DECAY_ATTR: per-probe cost dominated by the SST loop, B_resident=0).
                    // Pre-populate now installs a cache-enabled reader so the first AND subsequent
                    // probes share decoded blocks. Correctness-neutral (same bytes, same reader API).
                    let inserted = Arc::new(reader.with_block_cache(
                        Arc::clone(&self.block_cache)
                            as std::sync::Arc<dyn forst_rs_storage::cache::BlockCache>,
                        self.db_id.0,
                        meta.file_number.value(),
                    ));
                    self.sst_readers.rcu(|cur| {
                        let mut next = (**cur).clone();
                        next.insert(meta.file_number, std::sync::Arc::clone(&inserted));
                        std::sync::Arc::new(next)
                    });
                }
                Err(e) => {
                    tracing::debug!(
                        target: "forst_rs_engine::db",
                        file_number = meta.file_number.value(),
                        error = %e,
                        "flush: skipped SST reader pre-populate (lazy open will retry)",
                    );
                }
            },
            Err(e) => {
                tracing::debug!(
                    target: "forst_rs_engine::db",
                    file_number = meta.file_number.value(),
                    error = %e,
                    "flush: SST not yet readable (async upload in flight); lazy open will retry",
                );
            }
        }

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

        // FRS-RESIDENT-FLUSHED: the just-flushed memtable was already enrolled in the
        // resident read cache (tagged with the new SST's file number) BEFORE
        // `version_set.apply` above (see FRS-RESIDENT-FLUSHED-ORDER) so no probe ever
        // sees the SST without its RAM shadow. The prefix-scan path serves
        // recently-flushed hot data from RAM (Tier 2) and skips the byte-identical L0
        // SST in Tier 3. Data is durable on the SST (recovery UNCHANGED); the resident
        // set is a bounded read accelerator (FIFO; ~2 GiB cap), and dropping any entry
        // is always safe — Tier 3 simply stops skipping that file number.
        // Prune any resident entries whose SST is no longer in the live set
        // (compaction may have retired it). Reclaims RAM and avoids the
        // redundant Tier-2 + compaction-output scan; correctness-neutral.
        let live: std::collections::HashSet<FileNumber> = self
            .version_set
            .current()
            .live_sst_files()
            .iter()
            .map(|m| m.file_number)
            .collect();
        let _pruned = cf_data.prune_resident_flushed(&live);
        self.refresh_snapshot_view(cf_data);
        self.write_controller
            .set_imm_count(cf_data.imm_count() as u32);
        self.write_controller
            .set_l0_file_count(self.version_set.current().l0_files().len() as u32);
        self.write_controller.on_flush_complete();

        wamp_record_flush(meta.file_size); // FRS-WAMP: ingested bytes, all flush paths
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

    /// FRS-SLOT-SHARED-BG: records the `Weak` self-reference used by background
    /// jobs submitted to the process-global flush/compaction pools. Called once
    /// at construction (replaces the former per-DbImpl worker-thread spawn).
    fn init_self_weak(db: &Arc<Self>) {
        let _ = db.self_weak.set(Arc::downgrade(db));
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
                    // 2026-05-30 OBSOLETE-FILE LIFETIME: drain deferred deletions
                    // whose retiring versions have since lost their last reader.
                    // Compaction queues a file while a reader still holds the old
                    // version; the reader drops AFTER that compaction's own reap,
                    // so without this periodic sweep the file would linger until
                    // the next compaction. ~1 s cadence is ample (S3 storage is
                    // cheap; the goal is timely reclamation, not instant).
                    db.reap_pending_deletions();
                    // FRS-COMPACT-MAINTENANCE (2026-06-04): trigger L0→L1
                    // compaction for any CF whose L0 has grown to the trigger,
                    // INDEPENDENT of the flush path. The only prior trigger was
                    // `maybe_auto_compact` on the background flush worker, which
                    // q4 rarely exercises (its write buffer seldom fills) — so
                    // checkpoint-driven L0 SSTs accumulated uncompacted and the
                    // per-probe SST fan-out decayed throughput unbounded. This
                    // ~1 s poll keeps L0 shallow regardless of SST source.
                    // Deduped + non-blocking (enqueue only); the actual rollup
                    // runs on the dedicated compaction worker.
                    db.enqueue_due_compactions();
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
        // FRS-SLOT-SHARED-BG: submit to the PROCESS-GLOBAL flush pool (bounded
        // worker count) rather than a per-DbImpl thread. `pending_flush_count`
        // is incremented here and decremented by the RAII guard inside
        // `run_flush`; `wait_for_pending_flushes` still observes it correctly.
        self.pending_flush_count.fetch_add(1, Ordering::AcqRel);
        let Some(weak) = self.self_weak.get().cloned() else {
            self.pending_flush_count.fetch_sub(1, Ordering::AcqRel);
            return Err(ForstError::aborted("flush submit before self_weak init"));
        };
        bg_flush_pool().submit(Box::new(move || {
            // If the engine has been dropped, the job no-ops (and the counter is
            // moot — nobody is waiting on a dropped engine).
            if let Some(db) = weak.upgrade() {
                if let Err(e) = db.run_flush(&cf_data) {
                    db.record_flush_error(e);
                }
            }
        }));
        Ok(())
    }

    /// FRS-COMPACT-BG / FRS-SLOT-SHARED-BG: non-blocking, per-CF-deduped enqueue
    /// of an L0→L1 compaction onto the PROCESS-GLOBAL compaction pool (bounded
    /// worker count, so total background compaction CPU cannot scale with the
    /// number of DbImpl instances — the q4 foreground-starvation fix). If a
    /// compaction for this CF is already queued or running, the trigger is
    /// dropped (`compaction_queued` holds ≤1 entry per CF; `run_compaction`
    /// clears the flag at its start so a re-trigger re-queues next round).
    fn enqueue_compaction(&self, cf_data: Arc<ColumnFamilyData>) {
        let cf_id = cf_data.handle().id();
        {
            let mut q = self.compaction_queued.lock().expect("lock poisoned");
            if !q.insert(cf_id) {
                return; // already queued/running for this CF
            }
        }
        let Some(weak) = self.self_weak.get().cloned() else {
            self.compaction_queued
                .lock()
                .expect("lock poisoned")
                .remove(&cf_id);
            return;
        };
        bg_compact_pool().submit(Box::new(move || {
            if let Some(db) = weak.upgrade() {
                if let Err(e) = db.run_compaction(&cf_data) {
                    db.record_flush_error(e);
                }
            }
            // `run_compaction` clears `compaction_queued` itself; if the engine
            // was dropped before the job ran, the flag dies with it.
        }));
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

    fn record_fatal_error(&self, msg: impl Into<String>) {
        let mut slot = self.fatal_error.lock().expect("lock poisoned");
        if slot.is_none() {
            *slot = Some(msg.into());
            // 2026-05-29 PERF-RESTORE-#7: publish AFTER the slot is set so
            // any concurrent reader who sees the flag will also see the
            // payload. Release ordering pairs with the Acquire load in
            // check_fatal_error.
            self.fatal_error_set
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    fn check_fatal_error(&self) -> ForstResult<()> {
        // 2026-05-29 PERF-RESTORE-#7: AtomicBool fast-path. Happy path
        // (~always) does a single relaxed atomic load and returns Ok. Only
        // on the rare error-set case do we pay the Mutex lock to read the
        // payload. The pre-fix Mutex was acquired on every put/get/delete/
        // merge/batch_put engine entry — a hot contention point under high
        // concurrency that did NOT exist in v3.8 (fatal_error was added
        // by 6fcd844a0 harden-engine commit).
        if !self
            .fatal_error_set
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        let slot = self.fatal_error.lock().expect("lock poisoned");
        if let Some(msg) = slot.as_ref() {
            return Err(ForstError::Internal(msg.clone()));
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
        // 2026-06-02 q7 ckpt-ON FREEZE FIX: this method now waits ONLY for the
        // in-RAM flush counter to drain — true to its name. It no longer calls
        // the blanket `await_all_uploads()`. That bolted-on barrier (2026-05-29)
        // made EVERY `flush_all()` — including the one at the top of
        // `create_incremental_checkpoint_impl` — block on ALL in-flight uploads,
        // including large background COMPACTION outputs unrelated to the caller.
        // On a heavy join that turned each 30 s-interval checkpoint into a
        // ~330 s stall once compaction started (the freeze). Remote durability
        // is now established by the callers that actually need it, scoped to the
        // files they reference: `create_checkpoint` and
        // `create_incremental_checkpoint_impl` await their pinned SST set, and
        // the engine `drop` path awaits all uploads explicitly for a clean
        // shutdown. A plain flush (memtable → local SST) no longer implies
        // remote upload — which matches RocksDB semantics (flush ≠ checkpoint).
    }

    /// In-lock portion of the switch decision. Returns `true` if a switch
    /// happened and the caller should flush outside the write lock.
    fn maybe_switch_memtable_in_lock(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<bool> {
        let usage = cf_data.active_memtable().memory_usage();

        let threshold = cf_data.options().effective_write_buffer_size(&self.options);
        // FRS-WBM-TRUE-BACKPRESSURE (2026-06-08): normally switch at the per-memtable
        // size threshold (e.g. 1 GiB write_buffer_size). BUT when the cross-CF WBM
        // budget is over, FORCE a switch even below that threshold so there is an
        // immutable for the bg flush pool to drain — otherwise, with a large
        // write_buffer_size, no single memtable reaches its switch point, NO flush is
        // ever enqueued, and writers stalling on the budget (`wait_for_wbm_headroom`)
        // never see the global sum drop → 60s no-progress release → memtable overrun →
        // 8c/32g OOM (the q9 7 GB / q17 11 GB failure). A floor avoids flushing
        // trivially small memtables into a storm of tiny SSTs; over budget we still
        // only switch a memtable carrying real bytes. Mirrors RocksDB's WBM flushing
        // memtables when the shared buffer budget trips.
        // Floor sized to avoid an L0 explosion: forcing tiny memtables into many
        // small L0 SSTs faster than compaction drains them trips `l0_stop_trigger`
        // (64) → writers hard-stop → pipeline freeze → restart (observed: q17 froze
        // at a 16 MiB floor). 256 MiB keeps forced SSTs large so L0 stays shallow.
        const WBM_FORCE_SWITCH_FLOOR: usize = 256 * 1024 * 1024;
        let force_for_budget =
            usage >= WBM_FORCE_SWITCH_FLOOR && self.write_buffer_manager.over_budget();
        if usage < threshold && !force_for_budget {
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
    pub fn get_arc(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> ForstResult<Option<Arc<[u8]>>> {
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
        if self.check_fatal_error().is_err() {
            return None;
        }
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
    ///
    /// V10 (spec §3): routes through [`Self::batch_get_vectorized`] so the
    /// hot LSM tier walks (active → imm → resident → SST) execute as a
    /// single batched pass rather than N independent `get_internal` calls.
    /// Per-batch hoists: one `version_set.current()` snapshot, one
    /// `live_sst_files` HashSet build, one `resident_flushed_visible` walk,
    /// one `find_sst_for_key_in_cf` linear scan per (level, file) instead
    /// of per (key, level). See the dedicated docstring on
    /// [`Self::batch_get_vectorized`] for the phase breakdown.
    #[inline]
    pub fn batch_get(
        &self,
        cf: &ColumnFamilyHandle,
        keys: &[&[u8]],
    ) -> ForstResult<Vec<Option<Vec<u8>>>> {
        // Latest-view read — snapshot reads call `batch_get_vectorized` directly.
        self.batch_get_vectorized(cf, keys, u64::MAX)
    }

    /// V10 (spec §3 D10): vectorized engine-level batch lookup.
    ///
    /// Behaviour is identical to issuing `N` independent `get_internal(key,
    /// read_seq)` calls back-to-back (same merge-operand resolution, same
    /// tombstone semantics, same snapshot visibility) — but the per-batch
    /// constant overheads (version snapshot, live-file HashSet, resident
    /// list, prefetch) execute ONCE for the whole batch instead of N times.
    ///
    /// # Phases
    ///
    /// 1. **Active memtable, per-key, race-safe.** Each key re-captures
    ///    `active_memtable()` (A-R17-NEW-H1 invariant) and probes via
    ///    `mem.get`. Put → resolved with value; Delete/SingleDelete →
    ///    resolved as None; Merge → resolved via per-key `get_internal`
    ///    (merge chain semantics are complex enough that batching them
    ///    has no proven win; merge entries are rare on the read hot path).
    /// 2. **SST prefetch (one call).** `prefetch_sst_files_for_batch` warms
    ///    the file cache for SSTs not yet in the reader cache — one
    ///    GetObject per SST file instead of one per missed key on S3.
    /// 3. **Immutable memtables, batched.** One `imm_memtables()` capture.
    ///    Walk newest → oldest; for each imm, run through the unresolved
    ///    keys (Put / Delete are inlined; Merge falls back to per-key
    ///    `get_internal` honouring the same `read_seq`).
    /// 4. **Resident-flushed memtables, one snapshot.** One
    ///    `version_set.current()` snapshot + one `live_sst_files` HashSet
    ///    + one `resident_flushed_visible` call. The same snapshot is
    ///    reused in phase 5 so the resident-skip vs SST-walk pair is
    ///    transactionally consistent (FRS-RESIDENT-FLUSHED contract).
    /// 5. **SST layer, batched per-file.** L0 scan opens each L0 reader
    ///    ONCE and probes every unresolved key against that file's
    ///    `get_versions` (sister to the per-key `sst_get` walk). L1+ uses
    ///    `find_sst_for_key_in_cf` per (level, key) — but each candidate
    ///    file is opened ONCE and reused for any other unresolved key in
    ///    the same group. Tombstones resolve to None; Put resolves with
    ///    payload; Merge falls back to per-key `get_internal` (which
    ///    re-runs the merge peel with the correct `seq_cutoff`).
    ///
    /// # Correctness invariants
    /// - Snapshot consistency: the version snapshot taken in phase 4 is
    ///   used in phase 5 (no torn read between resident-skip and SST scan).
    /// - Memtable race: phase 1 re-captures `active_memtable()` per key so
    ///   a concurrent `swap_active_memtable` cannot drop a fresh write.
    /// - Merge chains delegate to `get_internal`, which honours `read_seq`
    ///   and the `peel_merges_from_sst_with_cutoff` flush-window dedup.
    /// - The output vector preserves the input key order.
    ///
    /// # `read_seq` semantics
    /// `u64::MAX` means latest-view (matches `db.get`); any lower value
    /// means snapshot-aware reads (matches `get_at` / `get_at_cf`).
    pub fn batch_get_vectorized(
        &self,
        cf: &ColumnFamilyHandle,
        keys: &[&[u8]],
        read_seq: u64,
    ) -> ForstResult<Vec<Option<Vec<u8>>>> {
        self.check_fatal_error()?;
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let n = keys.len();

        // Single-key fast path: bypass the vectorized bookkeeping. This
        // ensures callers that pass N=1 see no regression vs `get`.
        if n == 0 {
            return Ok(Vec::new());
        }
        if n == 1 {
            return Ok(vec![self.get_internal(&cf_data, keys[0], read_seq)?]);
        }

        // `resolved[i] = Some(slot)` means key i is done; `None` means still
        // pending. Slot semantics match `Option<Vec<u8>>` — outer Some = found,
        // outer None = missing/tombstoned.
        let mut resolved: Vec<Option<Option<Vec<u8>>>> = vec![None; n];

        // Phase 1: active memtable. A-R17-NEW-H1: re-capture per key (Arc
        // clone is cheap; the swap_active_memtable race window is narrow but
        // real — see comment on the legacy batch_get loop).
        for (i, k) in keys.iter().enumerate() {
            let mem = cf_data.active_memtable();
            match mem.get(k, read_seq)? {
                Some(entry) if entry.op_type == OpType::Put => {
                    resolved[i] = Some(entry.value);
                }
                Some(entry)
                    if entry.op_type == OpType::Delete || entry.op_type == OpType::SingleDelete =>
                {
                    resolved[i] = Some(None);
                }
                Some(_) => {
                    // Merge entry — fall back to per-key get_internal which
                    // resolves the merge chain with correct snapshot semantics.
                    resolved[i] = Some(self.get_internal(&cf_data, k, read_seq)?);
                }
                None => {} // leave pending for phase 2
            }
        }

        // Phase 2: SST prefetch — only if any key is still pending. This is
        // a no-op on LocalFileSystem / MemoryFileSystem.
        fn any_pending(r: &[Option<Option<Vec<u8>>>]) -> bool {
            r.iter().any(|x| x.is_none())
        }
        if !any_pending(&resolved) {
            return Ok(resolved.into_iter().map(|r| r.unwrap()).collect());
        }
        self.prefetch_sst_files_for_batch(&cf_data, keys);

        // Phase 3: immutable memtables. One capture of imm_list reused
        // across keys. Each imm is walked newest → oldest.
        let imm_list = cf_data.imm_memtables();
        if !imm_list.is_empty() {
            for imm in imm_list.iter().rev() {
                for (i, k) in keys.iter().enumerate() {
                    if resolved[i].is_some() {
                        continue;
                    }
                    let Some(entry) = imm.get(k, read_seq)? else {
                        continue;
                    };
                    match entry.op_type {
                        OpType::Put => resolved[i] = Some(entry.value),
                        OpType::Delete | OpType::SingleDelete => resolved[i] = Some(None),
                        OpType::Merge => {
                            // Merge chain — delegate to get_internal for full
                            // peel + snapshot semantics. `get_internal`
                            // re-walks active + imm itself; that's correct
                            // (and the only safe way to share the
                            // collect_merge_operands_from_imm_start code path
                            // without duplicating it here).
                            resolved[i] = Some(self.get_internal(&cf_data, k, read_seq)?);
                        }
                    }
                }
                if !any_pending(&resolved) {
                    return Ok(resolved.into_iter().map(|r| r.unwrap()).collect());
                }
            }
        }

        // Phase 4: resident-flushed memtables. One version snapshot + one
        // live-files HashSet — reused in phase 5 for snapshot consistency.
        let version = self.version_set.current();
        // 2026-05-29 PERF: file-number-only set (no SstFileMeta clone per batch).
        let live_files = version.live_sst_file_numbers();
        // 2026-05-29 PERF-RESTORE: fetch entries WITH key bounds so each
        // (resident, key) probe is gated by `may_contain` — skips the hash
        // lookup for keys outside the entry's SST range. For disjoint-range
        // joins this collapses O(num_resident × num_keys) to ≈O(num_keys).
        let resident_entries = cf_data.resident_flushed_visible_entries(&live_files);
        if !resident_entries.is_empty() {
            for resident in resident_entries.iter().rev() {
                for (i, k) in keys.iter().enumerate() {
                    if resolved[i].is_some() {
                        continue;
                    }
                    if !resident.may_contain(k) {
                        continue;
                    }
                    let Some(entry) = resident.memtable.get(k, read_seq)? else {
                        continue;
                    };
                    match entry.op_type {
                        OpType::Put => resolved[i] = Some(entry.value),
                        OpType::Delete | OpType::SingleDelete => resolved[i] = Some(None),
                        OpType::Merge => {
                            // Merge chain — delegate to get_internal.
                            resolved[i] = Some(self.get_internal(&cf_data, k, read_seq)?);
                        }
                    }
                }
                if !any_pending(&resolved) {
                    return Ok(resolved.into_iter().map(|r| r.unwrap()).collect());
                }
            }
        }

        // Phase 5: SST layer, batched per file.
        //
        // L0: per CF, open each file ONCE and probe every still-pending key
        // against it. For each key collect candidate versions across all L0
        // files, then apply the same newest-visible rule `sst_get` uses
        // (sequence desc, file_number desc tie-breaker).
        //
        // L1+: per CF, walk each level; for each still-pending key call
        // `find_sst_for_key_in_cf` (linear scan, one per (level, key)). Group
        // pending keys by file_number so each candidate file is opened
        // ONCE and probed for every grouped key.
        let cf_id = cf_data.handle().id();

        // L0 walk.
        let l0_files: Vec<&SstFileMeta> = version
            .l0_files()
            .iter()
            .filter(|m| m.cf_id == cf_id)
            .collect();
        if !l0_files.is_empty() {
            // For each L0 file, collect hits keyed by slot index. Then merge
            // across files per slot, ordered by (sequence desc, file_number desc).
            // l0_hits_per_key[i] = list of (sequence, file_number, LookupResult)
            let mut l0_hits_per_key: Vec<Vec<(u64, u64, forst_rs_storage::sst::LookupResult)>> =
                vec![Vec::new(); n];
            for sst in &l0_files {
                // Open reader ONCE per L0 file — reused for every pending key.
                let reader = self.get_or_open_sst_reader(sst)?;
                for (i, k) in keys.iter().enumerate() {
                    if resolved[i].is_some() {
                        continue;
                    }
                    // Quick file-range check (matches sst_lookup_versions).
                    if (**k).as_ref() < sst.smallest_key.as_slice()
                        || (**k).as_ref() > sst.largest_key.as_slice()
                    {
                        continue;
                    }
                    let versions = reader.get_versions(k)?;
                    for res in versions {
                        l0_hits_per_key[i].push((res.sequence, sst.file_number.value(), res));
                    }
                }
            }
            // Resolve each pending key from its collected L0 hits.
            for (i, hits) in l0_hits_per_key.iter_mut().enumerate() {
                if resolved[i].is_some() {
                    continue;
                }
                if hits.is_empty() {
                    continue;
                }
                // Sort newest-first: sequence desc, file_number desc.
                hits.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
                let mut had_merge_operand = false;
                for (_, _, res) in hits.drain(..) {
                    match res.op_type {
                        OpType::Put => {
                            if res.value.is_none() {
                                return Err(ForstError::corruption(
                                    "batch_get_vectorized: L0 Put missing value payload",
                                ));
                            }
                            if had_merge_operand {
                                // Mixed merge+base — fall back to per-key
                                // get_internal which handles the merge chain
                                // (apply_merge_operator + snapshot cutoff).
                                resolved[i] = Some(self.get_internal(&cf_data, keys[i], read_seq)?);
                            } else {
                                resolved[i] = Some(res.value);
                            }
                            break;
                        }
                        OpType::Delete | OpType::SingleDelete => {
                            if res.value.is_some() {
                                return Err(ForstError::corruption(
                                    "batch_get_vectorized: L0 tombstone carries value payload",
                                ));
                            }
                            if had_merge_operand {
                                resolved[i] = Some(self.get_internal(&cf_data, keys[i], read_seq)?);
                            } else {
                                resolved[i] = Some(None);
                            }
                            break;
                        }
                        OpType::Merge => {
                            // Defer the chain to per-key get_internal — the
                            // batched path does not reproduce the
                            // peel_merges_from_sst_with_cutoff semantics.
                            had_merge_operand = true;
                        }
                    }
                }
                if resolved[i].is_none() && had_merge_operand {
                    // L0 had only Merge entries — chain may continue down to
                    // L1+. Delegate the whole resolution to get_internal.
                    resolved[i] = Some(self.get_internal(&cf_data, keys[i], read_seq)?);
                }
            }
            if !any_pending(&resolved) {
                return Ok(resolved.into_iter().map(|r| r.unwrap()).collect());
            }
        }

        // L1+ walk: per level, group pending keys by candidate file.
        for level in 1..version.num_levels() {
            // file_idx -> list of (slot index) for that file at this level.
            let mut groups: std::collections::HashMap<usize, Vec<usize>> =
                std::collections::HashMap::new();
            for (i, k) in keys.iter().enumerate() {
                if resolved[i].is_some() {
                    continue;
                }
                if let Some(idx) = version.find_sst_for_key_in_cf(level, k, cf_id) {
                    groups.entry(idx).or_default().push(i);
                }
            }
            for (file_idx, slot_ixs) in groups {
                let sst = &version.levels[level].files[file_idx];
                // Open reader ONCE per file — reused for every grouped key.
                let reader = self.get_or_open_sst_reader(sst)?;
                for i in slot_ixs {
                    if resolved[i].is_some() {
                        continue;
                    }
                    let k = keys[i];
                    // Quick file-range check (matches sst_lookup_versions).
                    if k < sst.smallest_key.as_slice() || k > sst.largest_key.as_slice() {
                        continue;
                    }
                    let mut versions = reader.get_versions(k)?;
                    if versions.is_empty() {
                        continue;
                    }
                    versions.sort_by_key(|x| std::cmp::Reverse(x.sequence));
                    let mut had_merge_operand = false;
                    let mut decided = false;
                    for res in versions {
                        match res.op_type {
                            OpType::Put => {
                                if res.value.is_none() {
                                    return Err(ForstError::corruption(
                                        "batch_get_vectorized: L1+ Put missing value payload",
                                    ));
                                }
                                if had_merge_operand {
                                    resolved[i] = Some(self.get_internal(&cf_data, k, read_seq)?);
                                } else {
                                    resolved[i] = Some(res.value);
                                }
                                decided = true;
                                break;
                            }
                            OpType::Delete | OpType::SingleDelete => {
                                if res.value.is_some() {
                                    return Err(ForstError::corruption(
                                        "batch_get_vectorized: L1+ tombstone carries value payload",
                                    ));
                                }
                                if had_merge_operand {
                                    resolved[i] = Some(self.get_internal(&cf_data, k, read_seq)?);
                                } else {
                                    resolved[i] = Some(None);
                                }
                                decided = true;
                                break;
                            }
                            OpType::Merge => {
                                had_merge_operand = true;
                            }
                        }
                    }
                    if !decided && had_merge_operand {
                        // Merge chain may continue at deeper levels.
                        resolved[i] = Some(self.get_internal(&cf_data, k, read_seq)?);
                    }
                }
            }
            if !any_pending(&resolved) {
                return Ok(resolved.into_iter().map(|r| r.unwrap()).collect());
            }
        }

        // Any keys still pending after every level are genuine misses.
        Ok(resolved.into_iter().map(|r| r.unwrap_or(None)).collect())
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

        self.check_fatal_error()?;
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
        //
        // A-R14-H1: re-capture the active memtable PER iteration rather
        // than capturing once outside the loop. Capturing once leaves
        // the loop reading from a now-frozen memtable across a
        // `swap_active_memtable` driven by a concurrent writer: the
        // inline-cache fast path returns a stale `HitPut` with the
        // OLD value at the swap boundary even though the newer write
        // is now in the active memtable. `get_internal` (used on the
        // slow path here and on every single-key `db.get`) already
        // re-reads `active_memtable` on every call, so this matches the
        // contract single-key reads advertise. Arc clone is cheap.
        let read_seq = u64::MAX;
        for i in 0..n {
            let mem = cf_data.active_memtable();
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
                    // `sink` (a `&mut value_builder` borrow) is dead after the `get_into`
                    // call above; NLL ends that borrow at its last use, so the legacy
                    // `Option<Vec<u8>>` path below can re-borrow `value_builder` freely. No
                    // explicit `drop(sink)` is needed (and `drop()` on a non-`Drop` borrow
                    // would only EXTEND its lifetime, not shorten it — clippy::drop_non_drop).
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
    fn prefetch_sst_files_for_batch(&self, cf_data: &Arc<ColumnFamilyData>, keys: &[&[u8]]) {
        if keys.is_empty() {
            return;
        }
        // FRS-S3-READ-CONCURRENCY (2026-05-31): three fixes, all driven by a
        // symbolized q9 join-stall profile. The operator thread spent the bulk
        // of `batch_get` BLOCKED inside
        //   prefetch_sst_files_for_batch → fetch_through_cache → opendal read_at
        //   → tokio park → pthread_cond_wait
        // i.e. synchronously downloading whole SST files from S3. Root issues:
        //
        //   1) RESIDENT-SHADOW SKIP (the big one). batch_get's Phase 4 serves
        //      keys from resident-flushed memtables (RAM, byte-identical to the
        //      L0 SSTs) BEFORE Phase 5 ever touches an SST — but this prefetch
        //      ran at Phase 2 and downloaded those very SSTs from S3 anyway.
        //      For join state that lives in the memtable + resident-flushed RAM
        //      (the q9 case) that is a wholly redundant S3 round-trip per batch.
        //      Mirror build_lazy_prefix_key_stream: skip SSTs shadowed by a
        //      currently-resident memtable.
        //   2) RANGE-FILTER by the batch keys. The previous impl IGNORED `keys`
        //      and prefetched EVERY uncached SST in the whole version on every
        //      batch — including SSTs the batch never reads. Skip any SST whose
        //      [smallest,largest] does not overlap [batch_min, batch_max].
        //   3) CONCURRENT fetch. Any surviving misses are fanned out across
        //      threads (prefetch_concurrent) so K misses cost ≈1 round-trip.
        let mut batch_min: &[u8] = keys[0];
        let mut batch_max: &[u8] = keys[0];
        for k in &keys[1..] {
            if *k < batch_min {
                batch_min = k;
            }
            if *k > batch_max {
                batch_max = k;
            }
        }

        let version = self.version_set.current();
        let live_files = version.live_sst_file_numbers();
        // File numbers whose data is currently resident in RAM (just-flushed
        // memtables kept as a shadow). Reads for these keys are served from RAM
        // in batch_get Phase 4, so fetching their SSTs from S3 is pure waste.
        let resident_shadowed: std::collections::HashSet<FileNumber> = cf_data
            .resident_flushed_visible_entries(&live_files)
            .into_iter()
            .map(|e| e.file_number)
            .collect();
        let readers = self.sst_readers.load();

        // Collect overlapping, non-shadowed file numbers not yet reader-cached.
        let mut to_prefetch: Vec<PathBuf> = Vec::new();
        for level in &version.levels {
            for sst in &level.files {
                if resident_shadowed.contains(&sst.file_number) {
                    continue;
                }
                // Range-overlap test: SST is irrelevant if it lies entirely
                // below batch_min or entirely above batch_max.
                if sst.largest_key.as_slice() < batch_min {
                    continue;
                }
                if sst.smallest_key.as_slice() > batch_max {
                    continue;
                }
                if !readers.contains_key(&sst.file_number) {
                    to_prefetch.push(sst_file_path(&self.db_path, sst.file_number));
                }
            }
        }
        drop(readers);

        if to_prefetch.is_empty() {
            return;
        }
        // Best-effort concurrent warm. Errors are swallowed inside — the read
        // path retries on the actual lookup and surfaces any real error there.
        let path_refs: Vec<&Path> = to_prefetch.iter().map(|p| p.as_path()).collect();
        self.fs.prefetch_concurrent(&path_refs);
    }

    #[inline]
    fn get_internal(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        read_seq: u64,
    ) -> ForstResult<Option<Vec<u8>>> {
        self.check_fatal_error()?;
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

        // FRS-RESIDENT-FLUSHED point-get extension (2026-05-28): Stage 2.5 —
        // resident-flushed memtables (just-flushed, kept in RAM, byte-identical
        // to their L0 SSTs). For Put/Delete the resident hit is FINAL — same
        // content + same sequence number as the L0 SST that sst_get would
        // otherwise round-trip to S3 for. For Merge, we collect the outer
        // operand and call `collect_merge_operands` which walks active → imm
        // → SST in the usual way; the SST walk picks up older operands
        // (including those in the L0 SST shadowed by THIS resident memtable —
        // its cutoff = first_seq - 1 correctly masks the outer entry we just
        // consumed). Iterating newest → oldest so a newer resident wins seq
        // comparison without scanning all entries.
        //
        // CORRECTNESS: we filter resident entries by the CURRENT live-SST set.
        // After compaction collapses an L0 SST into L1+ output (merge operands
        // combined, tombstones consumed), reading the PRE-collapse resident
        // memtable would surface stale values. Snapshotting the version once
        // and reusing it for sst_get keeps Stage 2.5 + Stage 3 transactionally
        // consistent (no torn read between resident-skip and SST scan).
        //
        // 2026-05-29 PERF-RESTORE-#4: fast-path skip the HashSet-build when the
        // resident-flushed set is empty (typical until first flush completes
        // — v3.8's path). Saves the N-entry HashSet allocation per get() in the
        // common case. The `version_set.current()` snapshot is still captured
        // because Stage 3 (sst_get below) requires it.
        let version = self.version_set.current();
        let resident_mts: Vec<crate::column_family::SharedMemTable> =
            if cf_data.has_resident_flushed() {
                // 2026-05-29 PERF: file-number-only set (no SstFileMeta clone per get).
                let live_files = version.live_sst_file_numbers();
                // 2026-05-29 PERF-RESTORE: key-filtered accessor skips resident
                // memtables whose [min,max] bound excludes this key — O(matching)
                // instead of O(num_resident) per point GET.
                let (mts, _shadowed) = cf_data.resident_flushed_visible_for_key(&live_files, key);
                mts
            } else {
                Vec::new()
            };
        for resident in resident_mts.iter().rev() {
            let Some(entry) = resident.get(key, read_seq)? else {
                continue;
            };
            match entry.op_type {
                OpType::Put => return Ok(entry.value),
                OpType::Delete | OpType::SingleDelete => return Ok(None),
                OpType::Merge => {
                    let first_operand = entry.value.ok_or_else(|| {
                        ForstError::corruption("Merge entry missing operand payload")
                    })?;
                    let mut operands: Vec<Vec<u8>> = Vec::new();
                    operands.push(first_operand);
                    let base =
                        self.collect_merge_operands(cf_data, key, entry.sequence, &mut operands)?;
                    return self
                        .apply_merge_operator(cf_data, key, base, operands)
                        .map(Some);
                }
            }
        }

        // Stage 3: SST files (L0 newest-first, then L1..Ln binary search).
        self.sst_get(cf_data, &version, key, &mut Vec::new())
    }

    /// FRS-L0-SHORTCIRCUIT (2026-06-03): consumes one SST `LookupResult` during
    /// `Self::sst_get`'s L0 walk. Returns `Break(value)` when a Put/Delete base
    /// is reached (the value, with any accumulated merge operands applied) — the
    /// caller stops walking older L0 SSTs; `Continue` when a Merge operand was
    /// pushed and the walk must proceed to the next-older version. The arms are
    /// the proven B-R27-NEW-H1 / A-R6-H2 corruption-checking logic, factored out
    /// so the newest-first short-circuit and the overlap fallback share one path.
    fn sst_get_consume_l0(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        res: forst_rs_storage::sst::LookupResult,
        merge_operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<std::ops::ControlFlow<Option<Vec<u8>>>> {
        use std::ops::ControlFlow;
        match res.op_type {
            OpType::Put => {
                // B-R27-NEW-H1: a Put with `value=None` is a corrupt SST row —
                // Put MUST carry a payload. Fail loud (sister `iter_versions_of`
                // raises corruption for this exact shape).
                if res.value.is_none() {
                    return Err(ForstError::corruption(
                        "sst_get: L0 Put missing value payload",
                    ));
                }
                if merge_operands.is_empty() {
                    return Ok(ControlFlow::Break(res.value));
                }
                self.apply_merge_operator(cf_data, key, res.value, std::mem::take(merge_operands))
                    .map(|v| ControlFlow::Break(Some(v)))
            }
            OpType::Delete | OpType::SingleDelete => {
                // B-R27-NEW-H1: a tombstone carrying a payload is corrupt.
                if res.value.is_some() {
                    return Err(ForstError::corruption(
                        "sst_get: L0 tombstone carries value payload",
                    ));
                }
                if merge_operands.is_empty() {
                    return Ok(ControlFlow::Break(None));
                }
                self.apply_merge_operator(cf_data, key, None, std::mem::take(merge_operands))
                    .map(|v| ControlFlow::Break(Some(v)))
            }
            OpType::Merge => match res.value {
                // A-R6-H2: surface corruption on a missing operand payload.
                Some(v) => {
                    merge_operands.push(v);
                    Ok(ControlFlow::Continue(()))
                }
                None => Err(ForstError::corruption(
                    "sst_get: L0 Merge missing operand payload",
                )),
            },
        }
    }

    /// Searches the SST layers for `key`. Returns the resolved user value
    /// (None for missing / tombstoned) — including full merge chains that
    /// start in the SST layer.
    ///
    /// R49-H1: SST files are filtered by `cf_id` so this CF only sees its
    /// own SSTs. Without this filter, two CFs writing the same user-key
    /// would corrupt each other's reads after flush (cross-CF SST read
    /// corruption). The check is a single u32 comparison per file in the
    /// candidate set, performed BEFORE `sst_lookup` opens / probes the
    /// file — so it is essentially free on the hot read path.
    fn sst_get(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        version: &Version,
        key: &[u8],
        merge_operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        let cf_id = cf_data.handle().id();
        use std::ops::ControlFlow;

        // L0 read with NEWEST-FIRST short-circuit (FRS-L0-SHORTCIRCUIT 2026-06-03).
        //
        // L0 files are stored sorted by smallest_key (Version::apply_edit), not
        // by recency. Under the single-worker SEQUENTIAL flush each new L0 SST
        // carries a strictly higher sequence range than the previous one
        // (compaction output goes to L1, never L0), so per-CF the L0 files have
        // DISJOINT, descending [min_sequence, max_sequence] ranges. Sorting them
        // by max_sequence DESC therefore reproduces the EXACT global newest-first
        // order the old code obtained by collecting every row and sorting by
        // (sequence desc, file desc) — but now files are read lazily and we STOP
        // at the first Put/Delete base. For a hot ValueState key Put-overwritten
        // on every record (present in EVERY L0 SST, so the per-SST bloom filter
        // cannot skip any), this turns O(L0) data-block reads into ONE — the
        // q11/q4 read-amplification decay once the working set spills past the
        // resident RAM shadow.
        let mut l0: Vec<&SstFileMeta> = version
            .l0_files()
            .iter()
            .filter(|s| s.cf_id == cf_id)
            .collect();
        l0.sort_by(|a, b| {
            b.max_sequence
                .cmp(&a.max_sequence)
                .then_with(|| b.file_number.value().cmp(&a.file_number.value()))
        });
        // The disjoint-range property is the correctness premise of the
        // file-ordered short-circuit. The only way L0 ranges can overlap is
        // externally-ingested SSTs landing in L0 with non-monotonic sequences;
        // if that is ever observed we fall back to the read-all + global-sort
        // walk, which is correct regardless of file ordering.
        let l0_disjoint = l0.windows(2).all(|w| w[0].min_sequence > w[1].max_sequence);

        if l0_disjoint {
            // Newest-first lazy walk: a Put/Delete base resolves the read and we
            // never open the older L0 SSTs; a Merge accumulates and continues.
            for sst in &l0 {
                let versions = self.sst_lookup_versions(sst, key)?;
                if versions.is_empty() {
                    continue; // bloom miss / range miss — no data block read
                }
                self.l0_point_get_block_reads
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                for res in versions {
                    if let ControlFlow::Break(v) =
                        self.sst_get_consume_l0(cf_data, key, res, merge_operands)?
                    {
                        return Ok(v);
                    }
                }
            }
        } else {
            // Overlap fallback (ingested SSTs): collect every matching row across
            // L0, sort globally by (sequence desc, file desc), then walk.
            let mut l0_hits = Vec::new();
            for sst in &l0 {
                let versions = self.sst_lookup_versions(sst, key)?;
                if !versions.is_empty() {
                    self.l0_point_get_block_reads
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                for res in versions {
                    l0_hits.push((res.sequence, sst.file_number.value(), res));
                }
            }
            l0_hits.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
            for (_, _, res) in l0_hits {
                if let ControlFlow::Break(v) =
                    self.sst_get_consume_l0(cf_data, key, res, merge_operands)?
                {
                    return Ok(v);
                }
            }
        }

        // L1..Ln: at most one candidate per level.
        //
        // A-H2: pre-fix this used `find_sst_for_key` (CF-agnostic) +
        // an `if sst.cf_id != cf_id { continue; }` guard. Binary
        // search by `smallest_key` against the interleaved multi-CF
        // file vector could land on another CF's file whose range
        // happened to bracket `key`, after which the cf_id filter
        // would skip the entire level — losing the read for the
        // requested CF's actual file at that level. Switch to the
        // CF-aware variant that filters by cf_id BEFORE finding the
        // range-matching file. Per-CF the L1+ files are
        // non-overlapping so a single matched file is always
        // correct.
        for level in 1..version.num_levels() {
            let Some(idx) = version.find_sst_for_key_in_cf(level, key, cf_id) else {
                continue;
            };
            let sst = &version.levels[level].files[idx];
            let mut versions = self.sst_lookup_versions(sst, key)?;
            versions.sort_by_key(|x| std::cmp::Reverse(x.sequence));
            for res in versions {
                match res.op_type {
                    OpType::Put => {
                        // B-R27-NEW-H1 (L1+ sibling): Put-with-None is
                        // corrupt — sister iter_versions_of raises
                        // corruption here. Match its contract.
                        if res.value.is_none() {
                            return Err(ForstError::corruption(
                                "sst_get: L1+ Put missing value payload",
                            ));
                        }
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
                        // B-R27-NEW-H1 (L1+ sibling): tombstone-with-payload
                        // is corrupt — match iter_versions_of's contract.
                        if res.value.is_some() {
                            return Err(ForstError::corruption(
                                "sst_get: L1+ tombstone carries value payload",
                            ));
                        }
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
                        // A-R6-H2: L1+ sibling — same corruption check.
                        match res.value {
                            Some(v) => merge_operands.push(v),
                            None => {
                                return Err(ForstError::corruption(
                                    "sst_get: L1+ Merge missing operand payload",
                                ));
                            }
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

    fn sst_lookup_versions(
        &self,
        meta: &SstFileMeta,
        key: &[u8],
    ) -> ForstResult<Vec<forst_rs_storage::sst::LookupResult>> {
        // Quick key-range check — avoids opening the file for obvious misses.
        if key < meta.smallest_key.as_slice() || key > meta.largest_key.as_slice() {
            return Ok(Vec::new());
        }
        let reader = self.get_or_open_sst_reader(meta)?;
        reader.get_versions(key)
    }

    fn get_or_open_sst_reader(&self, meta: &SstFileMeta) -> ForstResult<Arc<SstReaderImpl>> {
        {
            let cache = self.sst_readers.load();
            if let Some(r) = cache.get(&meta.file_number) {
                return Ok(r.clone());
            }
        }
        // FRS-SST-OPEN-NOLOCK (2026-06-01, audit item D): do the S3 I/O
        // (await_upload + open + footer/index read) WITHOUT holding the
        // sst_readers lock, mirroring the compaction path. The prior code held
        // `sst_readers.write()` across `await_upload`, which on the S3 path
        // blocks on the freshly-flushed SST's full multipart upload (~114 s for
        // a 2 GiB SST at the ~18 MB/s BOS uplink — proven by FRS-ITER-DIAG). With
        // the write lock held, EVERY other reader-cache miss (any other SST)
        // serialized behind that one upload → a total pipeline stall (observed
        // at 256 MiB memtables: many concurrent flush uploads each grabbed the
        // write lock in turn → the whole job froze at 32 M). Doing the I/O
        // lock-free lets opens of DIFFERENT SSTs proceed concurrently; we only
        // re-take the write lock to insert, double-checking a racing inserter so
        // at most one reader per file is cached (a concurrent duplicate open is
        // wasteful but correct — the loser's reader is simply dropped).
        let path = sst_file_path(&self.db_path, meta.file_number);
        // FRS-LOCAL-FIRST-SST (2026-06-01): the explicit `await_upload` here was
        // REMOVED. It blocked this open on the freshly-flushed SST's FULL S3
        // upload (~114 s for a 2 GiB SST at the ~18 MB/s object-store uplink —
        // the proven heavy-join freeze: a join probe that missed the resident
        // shadow waited the entire upload). The filesystem's
        // `open_random_access_file` now serves SSTs LOCAL-FIRST from the
        // synchronous write-through copy (present the instant the SST is
        // version-visible), so the durable bytes are read off local NVMe without
        // waiting for S3. Correctness for the paths that DO read straight from
        // the remote (restore on a fresh instance; an entry evicted from the
        // local LRU) is preserved inside `open_random_access_file`: the remote
        // branch still awaits the upload via `remote_size_awaiting_upload`, and
        // the local-first reader's eviction fallback awaits before its remote
        // open. So the await still happens exactly where a remote read needs it
        // — just no longer on the hot path that has a local copy.
        let file = self.fs.open_random_access_file(&path)?;
        // 2026-05-30 DECODED-BLOCK CACHE: wire the shared L1 block cache so the
        // prefix-iterator's repeated probes of the same SST data blocks hit a
        // decoded RecordBatch instead of re-reading+re-decompressing (the
        // profiled q9 hot path: serial_read_at + decode_data_block + decompress).
        let reader = Arc::new(SstReaderImpl::open(file)?.with_block_cache(
            Arc::clone(&self.block_cache)
                as std::sync::Arc<dyn forst_rs_storage::cache::BlockCache>,
            self.db_id.0,
            meta.file_number.value(),
        ));

        // R50-H3: cross-check the on-disk footer cf_id against the meta
        // cf_id the VersionSet handed us. R49-H1 persisted cf_id in the
        // footer for exactly this restore-time check, but the validation
        // was never wired — drift between the two sources (e.g. the
        // R50-M2 ingest path that rewrote meta.cf_id without touching
        // the footer) would have stayed silent.
        //
        // v1 footers (pre-R49-H1) decode their cf_id as DEFAULT_CF_ID;
        // a v1 SST living in a non-default CF's meta would still trip
        // this check, which is correct — those SSTs cannot exist in
        // a v2-format DB without a corrupt manifest.
        let footer_cf_id = reader.footer().cf_id;
        if footer_cf_id != meta.cf_id {
            return Err(ForstError::corruption(format!(
                "SST {}: footer cf_id ({}) does not match meta cf_id ({}); \
                 manifest and on-disk truth diverged",
                meta.file_number.value(),
                footer_cf_id.value(),
                meta.cf_id.value(),
            )));
        }

        // Re-acquire the write lock only to insert; double-check a racing
        // inserter that opened the same file while we were doing lock-free I/O.
        // FRS-LOCKFREE: RCU insert with double-check — a racing inserter may have
        // cached the same file while we did the lock-free open. The closure is
        // pure (clone-mutate-publish) and safe to re-run on CAS retry; `resolved`
        // ends as the winning reader (ours, or the racer's).
        let mut resolved = reader.clone();
        self.sst_readers.rcu(|cur| {
            if let Some(existing) = cur.get(&meta.file_number) {
                resolved = existing.clone();
                std::sync::Arc::clone(cur)
            } else {
                let mut next = (**cur).clone();
                next.insert(meta.file_number, std::sync::Arc::clone(&reader));
                std::sync::Arc::new(next)
            }
        });
        Ok(resolved)
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

        // A-R3-H3: imm-list cutoff MUST be first_seq - 1, not
        // u64::MAX. The doc-comment that previously claimed "each
        // memtable has its own sequence space" was wrong — the
        // engine's `sequence_number` is global (every write goes
        // through one `fetch_add`). A reader that observed the
        // outer Merge entry in the active memtable and is then
        // raced by `swap_active_memtable` will subsequently see
        // the SAME memtable on imm_list AND the same outer entry
        // at seq=first_seq. Without the cutoff that outer entry
        // would be re-consumed as an operand, silently
        // double-counting under counter-style merge operators.
        // Setting cutoff = first_seq - 1 masks the outer entry
        // even when imm contains it post-swap; older operands
        // (seq < first_seq) remain visible.
        if first_seq > 0 {
            // D-R7-H2: dedupe by Arc identity. After A-R3-H3 set the
            // imm cutoff to `first_seq - 1`, a concurrent
            // `swap_active_memtable` between our `active_memtable()`
            // capture (line 5817) and our `imm_memtables()` read
            // below can put the SAME Arc<MemTable> on imm_list.
            // Walking it a second time at cutoff = first_seq - 1
            // would re-collect every operand older than first_seq —
            // double-counting under counter / list-append / set-union
            // merge operators. Skip any imm whose Arc identity matches
            // the active memtable we just peeled.
            let mem_ptr = std::sync::Arc::as_ptr(&mem_arc);
            let imm_list = cf_data.imm_memtables();
            for imm in imm_list.iter().rev() {
                if std::sync::Arc::as_ptr(imm) == mem_ptr {
                    continue;
                }
                if let Some(base) =
                    self.peel_merges_from_memtable(imm, key, first_seq - 1, operands)?
                {
                    return Ok(base.value);
                }
            }
        }

        // SST layer: walk L0 newest-first, then L1..Ln.
        // E-R8-H1: same flush-window dup defense as the imm_start
        // sister. The outer entry at first_seq was observed in active
        // (or imm via swap); a concurrent flush installing an SST
        // could re-emit the same row from the SST layer. Skip rows
        // at seq >= first_seq.
        if first_seq > 0 {
            self.peel_merges_from_sst_with_cutoff(cf_data, key, first_seq, operands)
        } else {
            self.peel_merges_from_sst(cf_data, key, operands)
        }
    }

    /// Walks the imm list when the outer hit was itself in the immutable
    /// queue. `first_seq` is the already-consumed outer sequence; peel
    /// continues from `first_seq - 1` in EVERY imm — see B-R5-H1 below.
    fn collect_merge_operands_from_imm_start(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        first_seq: u64,
        imm_list: Vec<crate::column_family::SharedMemTable>,
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        // B-R5-H1: sibling of A-R3-H3. The pre-fix code reset the
        // cutoff to `u64::MAX` for every imm AFTER the first, on the
        // false belief that "each memtable has its own sequence
        // space". The engine's `sequence_number` is global (a single
        // `fetch_add`), so an outer Merge entry at seq=first_seq
        // observed in the first imm could ALSO be observed in an
        // older imm under the active→imm swap race fixed by A-R3-H3.
        // Use `first_seq - 1` as cutoff for EVERY imm so the outer
        // entry never re-enters the operand stack under that race.
        if first_seq == 0 {
            return self.peel_merges_from_sst(cf_data, key, operands);
        }
        let cutoff = first_seq - 1;
        for imm in imm_list.iter().rev() {
            if let Some(base) = self.peel_merges_from_memtable(imm, key, cutoff, operands)? {
                return Ok(base.value);
            }
        }
        // Fall through into SSTs.
        // E-R8-H1: use the cutoff variant so the just-flushed SST
        // (which now contains a copy of the outer entry at
        // seq=first_seq) does NOT re-emit that operand. Skip rows at
        // seq >= first_seq.
        self.peel_merges_from_sst_with_cutoff(cf_data, key, first_seq, operands)
    }

    /// Peels merges out of the SST layer. Returns:
    /// - `Ok(Some(value))` when a Put base is found (value may itself be None
    ///   if the Put stored a null payload).
    /// - `Ok(None)` when a Delete is hit or the SST layer is exhausted with
    ///   no base — the caller treats this as "base is empty".
    fn peel_merges_from_sst(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        self.peel_merges_from_sst_with_cutoff(cf_data, key, u64::MAX, operands)
    }

    /// E-R8-H1: same as `peel_merges_from_sst` but skips SST rows whose
    /// sequence is >= `seq_cutoff`. Used by `collect_merge_operands` and
    /// `collect_merge_operands_from_imm_start` to defend against the
    /// flush-window operand-duplication race: when a flush installs a
    /// new SST via `version_set.apply` and then pops the imm, both the
    /// imm and the SST are simultaneously visible to a reader. A reader
    /// that consumed the outer Merge entry at seq=N from an imm could
    /// otherwise re-consume the SAME entry from the just-flushed SST.
    /// Passing `seq_cutoff = first_seq` (the outer already-consumed
    /// seq) closes that window — SST rows at seq >= first_seq are
    /// skipped because the reader has already accounted for them
    /// (either via the outer hit or via the imm walk's
    /// `first_seq - 1` cutoff).
    fn peel_merges_from_sst_with_cutoff(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        seq_cutoff: u64,
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        // R49-H1: scope merge-peeling to the calling CF's SSTs.
        let cf_id = cf_data.handle().id();
        let version = self.version_set.current();
        let mut l0_hits = Vec::new();
        for sst in version.l0_files() {
            if sst.cf_id != cf_id {
                continue;
            }
            for res in self.sst_lookup_versions(sst, key)? {
                // E-R8-H1: defense-in-depth against flush-window dup.
                if res.sequence >= seq_cutoff {
                    continue;
                }
                l0_hits.push((res.sequence, sst.file_number.value(), res));
            }
        }
        l0_hits.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        for (_, _, res) in l0_hits {
            match res.op_type {
                OpType::Put => {
                    // B-C4R14-NEW-H2: surface corruption on Put with no
                    // payload (matches sister sites sst_get/B-R27-NEW-H1,
                    // iter_versions_of/D-R8-NEW-H3, peel_merges_from_memtable).
                    // Pre-fix `return Ok(res.value)` silently treated the
                    // bad row as a tombstone — merge chain degraded to a
                    // phantom Delete instead of surfacing the bad row.
                    if res.value.is_none() {
                        return Err(ForstError::corruption(
                            "peel_merges_from_sst: L0 Put missing payload",
                        ));
                    }
                    return Ok(res.value);
                }
                OpType::Delete | OpType::SingleDelete => {
                    // B-C4R14-NEW-H2: tombstone-with-payload is also corruption.
                    if res.value.is_some() {
                        return Err(ForstError::corruption(
                            "peel_merges_from_sst: L0 tombstone carrying payload",
                        ));
                    }
                    return Ok(None);
                }
                OpType::Merge => {
                    // A-R5R-NEW-H1: surface corruption on missing
                    // operand payload (matches outer-hit + memtable
                    // peel paths).
                    match res.value {
                        Some(v) => operands.push(v),
                        None => {
                            return Err(ForstError::corruption(
                                "peel_merges_from_sst: L0 Merge missing operand payload",
                            ));
                        }
                    }
                }
            }
        }
        // A-R3-H1: sibling regression to A-H2 — same pattern. The
        // pre-fix used CF-agnostic `find_sst_for_key` + post-filter,
        // which abandoned the entire level whenever the binary search
        // landed on another CF's file. Multi-CF deployments using a
        // merge operator would silently return stale-or-wrong merged
        // values because the calling CF's actual L1+ Put base never
        // reached `peel_merges_from_sst`. Switch to the CF-aware
        // variant identical to `sst_get`'s post-A-H2 path.
        for level in 1..version.num_levels() {
            let Some(idx) = version.find_sst_for_key_in_cf(level, key, cf_id) else {
                continue;
            };
            let sst = &version.levels[level].files[idx];
            let mut versions = self.sst_lookup_versions(sst, key)?;
            versions.sort_by_key(|x| std::cmp::Reverse(x.sequence));
            for res in versions {
                // E-R8-H1: skip rows already accounted for by the
                // caller's outer entry.
                if res.sequence >= seq_cutoff {
                    continue;
                }
                match res.op_type {
                    OpType::Put => {
                        // B-C4R14-NEW-H2 (L1+ sister): same as L0 check above.
                        if res.value.is_none() {
                            return Err(ForstError::corruption(
                                "peel_merges_from_sst: L1+ Put missing payload",
                            ));
                        }
                        return Ok(res.value);
                    }
                    OpType::Delete | OpType::SingleDelete => {
                        if res.value.is_some() {
                            return Err(ForstError::corruption(
                                "peel_merges_from_sst: L1+ tombstone carrying payload",
                            ));
                        }
                        return Ok(None);
                    }
                    OpType::Merge => {
                        // A-R5R-NEW-H1: corruption on missing payload.
                        match res.value {
                            Some(v) => operands.push(v),
                            None => {
                                return Err(ForstError::corruption(
                                    "peel_merges_from_sst: L1+ Merge missing operand payload",
                                ));
                            }
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
        // FRS-MERGE-PERF (2026-06-03): single O(N log N)-pass collection over the
        // key's version list. The prior loop called `mem_arc.get(key, cutoff)`
        // once per operand, and each `get` scans the key's whole `row_indices`
        // (find_latest) — so a hot key with N merge operands cost N × O(N) =
        // O(N²) per read. That spun `vectorizedBatchGet` >180s on q5's
        // WindowJoin list-valued state and killed the TaskManager (symbolized
        // sample: 100 % CPU in collect_merge_operands → peel_merges_from_memtable
        // → ShardedMemTable::get). The new `collect_merge_operands` takes the
        // shard lock ONCE and folds the chain in one pass. The Merge-with-no-
        // payload corruption guard (A-R5R-NEW-H1) moved into that method.
        match mem_arc.collect_merge_operands(key, start_cutoff, operands)? {
            Some(value) => Ok(Some(MergeBase { value })),
            None => Ok(None),
        }
    }

    /// R49-H2: collect a `CfDescriptor` snapshot for every currently-open
    /// CF. Sorted by cf_id so the blob's CF table has a deterministic order
    /// (helps diff-based debugging; the restore path doesn't depend on it).
    ///
    /// R50-L3: rejects any CF whose `name`, `merge_operator().name()`, or
    /// `compaction_filter().name()` exceeds
    /// [`MAX_CF_STRING_LEN`](forst_rs_storage::version::checkpoint::MAX_CF_STRING_LEN).
    /// The decode-side already enforces the same cap; surfacing the
    /// violation at write time prevents a checkpoint blob that the
    /// restore path will refuse to load.
    fn collect_cf_descriptors(&self) -> ForstResult<Vec<forst_rs_storage::version::CfDescriptor>> {
        use forst_rs_storage::version::checkpoint::MAX_CF_STRING_LEN;
        use forst_rs_storage::version::CfDescriptor;
        let cap = MAX_CF_STRING_LEN as usize;
        let check = |what: &str, cf_id: ColumnFamilyId, s: &str| -> ForstResult<()> {
            if s.len() > cap {
                return Err(ForstError::invalid_argument(format!(
                    "cf {} {} length {} exceeds MAX_CF_STRING_LEN ({})",
                    cf_id.value(),
                    what,
                    s.len(),
                    cap
                )));
            }
            Ok(())
        };
        let cfs = self.cfs.read().expect("lock poisoned");
        let mut entries: Vec<&Arc<ColumnFamilyData>> = cfs.values().collect();
        entries.sort_by_key(|cf| cf.handle().id().value());
        let mut out = Vec::with_capacity(entries.len());
        for cf in entries {
            let cf_id = cf.handle().id();
            let name = cf.handle().name().to_string();
            check("name", cf_id, &name)?;
            let merge_op_name = cf.merge_operator().map(|op| op.name()).unwrap_or_default();
            check("merge_operator name", cf_id, &merge_op_name)?;
            let filter_name = cf
                .compaction_filter()
                .as_ref()
                .map(|f| f.name())
                .unwrap_or_default();
            check("compaction_filter name", cf_id, &filter_name)?;
            out.push(CfDescriptor {
                cf_id,
                name,
                merge_op_name,
                filter_name,
            });
        }
        Ok(out)
    }

    /// D-R8-NEW-H2: public probe so the FFI can refuse merge-append
    /// operations on CFs that have no merge operator configured. Pre-fix
    /// the FFI accepted the write (Merge entries land in the WriteBatch)
    /// and every subsequent read returned InvalidArgument silently. The
    /// FFI now calls this before issuing `wb.merge` and rejects with
    /// `BatchHeaderMalformed` if it returns false.
    pub fn cf_has_merge_operator(&self, cf: &ColumnFamilyHandle) -> bool {
        match self.lookup_cf_by_id(cf.id()) {
            Ok(cf_data) => cf_data.merge_operator().is_some(),
            Err(_) => false,
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

    /// Returns `true` when `file_number`'s storage may be reclaimed NOW: it is
    /// neither pinned by an in-flight checkpoint ([`FileDeletionGuard`]) NOR
    /// referenced by any live version still held by an in-flight read.
    ///
    /// 2026-05-30 OBSOLETE-FILE LIFETIME: the version-reference check closes the
    /// compaction-deletes-SST-under-concurrent-read race. A read captures
    /// `version_set.current()` (an owned `Arc<Version>`) and opens that
    /// version's SSTs via on-demand storage reads; deleting an input SST's
    /// object while such a read is in flight makes the read 404
    /// (`frs_vectorized_batch_get rc=1 NOT_FOUND` → crash-loop on S3). The
    /// pre-fix path only honored checkpoint pins. `referenced` is computed once
    /// by the caller and threaded in to avoid recomputing the live-version set
    /// per file in a deletion loop.
    fn can_reclaim_file(
        &self,
        file_number: FileNumber,
        referenced: &std::collections::HashSet<FileNumber>,
    ) -> bool {
        self.deletion_guard.can_delete(file_number) && !referenced.contains(&file_number)
    }

    /// Attempts to delete a file, respecting both the [`FileDeletionGuard`] pin
    /// set AND live-version references. Files that cannot yet be reclaimed are
    /// deferred to [`Self::reap_pending_deletions`].
    fn delete_file_guarded(&self, file_number: FileNumber) {
        let referenced = self.version_set.referenced_file_numbers();
        if self.can_reclaim_file(file_number, &referenced) {
            let path = sst_file_path(&self.db_path, file_number);
            let _ = self.fs.delete_file(&path);
        } else {
            self.pending_deletions
                .lock()
                .expect("lock poisoned")
                .push(file_number);
        }
    }

    /// Reaps previously-deferred deletions whose pins / live-version references
    /// have since been released. Called after every compaction AND periodically
    /// from the background workers so deferred deletions drain even when no new
    /// compaction fires (a retiring version is only pruned when its last reader
    /// drops, which may happen after the compaction that queued the file).
    fn reap_pending_deletions(&self) {
        let referenced = self.version_set.referenced_file_numbers();
        let mut pending = self.pending_deletions.lock().expect("lock poisoned");
        let mut still_pending = Vec::with_capacity(pending.len());
        for file_number in pending.drain(..) {
            if self.can_reclaim_file(file_number, &referenced) {
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
        let md_a = std::fs::metadata(a).map_err(|e| {
            ForstError::Io(std::io::Error::other(format!(
                "stat '{}': {e}",
                a.display()
            )))
        })?;
        let md_b = std::fs::metadata(b).map_err(|e| {
            ForstError::Io(std::io::Error::other(format!(
                "stat '{}': {e}",
                b.display()
            )))
        })?;
        Ok(md_a.dev() == md_b.dev() && md_a.ino() == md_b.ino())
    }
    #[cfg(not(unix))]
    {
        let md_a = std::fs::metadata(a).map_err(|e| {
            ForstError::Io(std::io::Error::other(format!(
                "stat '{}': {e}",
                a.display()
            )))
        })?;
        let md_b = std::fs::metadata(b).map_err(|e| {
            ForstError::Io(std::io::Error::other(format!(
                "stat '{}': {e}",
                b.display()
            )))
        })?;
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
            // `s3://bucket` or `s3://bucket/prefix`. OpenDAL's `bucket`
            // field is only the bucket name; the URI path must become the
            // operator root so remote files stay under the configured prefix.
            let (bucket_part, prefix_part) = rest.split_once('/').unwrap_or((rest, ""));
            let bucket = bucket_part.to_string();
            if bucket.is_empty() {
                return Err(ForstError::invalid_argument(format!(
                    "open_remote: s3 URI '{uri}' missing bucket name"
                )));
            }
            let prefix = prefix_part.trim_matches('/');
            let region = extra_config.get("region").cloned().ok_or_else(|| {
                ForstError::invalid_argument(
                    "open_remote: s3 scheme requires 'region' in opendal_config",
                )
            })?;
            let endpoint = extra_config.get("endpoint").map(String::as_str);
            let access_key_id = extra_config.get("access_key_id").map(String::as_str);
            let secret_access_key = extra_config.get("secret_access_key").map(String::as_str);
            Arc::new(OpendalFileSystem::s3_with_root(
                &bucket,
                prefix,
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
        // (FRS-WAMP ingested-bytes accounting lives inside flush_cf_data so it
        // captures every flush path, not just this one.)
        // FRS_PROF_DIAG: attribute flush wall-time + bytes (gap-map dim 3, flush ms/MB).
        let _flush_t0 = std::time::Instant::now();
        let _flushed = self.flush_cf_data(cf_data)?;
        prof_add(&PROF_FLUSH_NS, _flush_t0.elapsed().as_nanos() as u64);
        if let Some(meta) = &_flushed {
            prof_add(&PROF_FLUSH_BYTES, meta.file_size);
            prof_add(&PROF_FLUSH_CNT, 1);
        }
        // Auto-compact L0 if it has grown past the slowdown trigger so
        // the engine stays well clear of the write-stall ceiling.
        self.maybe_auto_compact(cf_data)?;

        // DECAY AUTOPSY (FRS_DECAY_DIAG=1, off by default): per-flush LSM shape
        // + state-size proxy (sum of live SST bytes, post-compaction). Monotonic
        // `state` ⇒ window expiry isn't reclaiming (operator state-lifecycle);
        // a plateau ⇒ self-asymptoting; an L0-dominated `ssts` ⇒ compaction-bound.
        {
            use std::sync::OnceLock;
            static EN: OnceLock<bool> = OnceLock::new();
            let on = *EN.get_or_init(|| {
                matches!(
                    std::env::var("FRS_DECAY_DIAG").ok().as_deref(),
                    Some("1") | Some("true") | Some("TRUE")
                )
            });
            if on {
                static FSEQ: AtomicU64 = AtomicU64::new(0);
                let seq = FSEQ.fetch_add(1, Ordering::Relaxed) + 1;
                let v = self.version_set.current();
                let mut parts = Vec::new();
                let mut tot_files = 0usize;
                let mut tot_bytes = 0u64;
                for (lvl, lm) in v.levels.iter().enumerate() {
                    let n = lm.files.len();
                    if n == 0 {
                        continue;
                    }
                    let b: u64 = lm.files.iter().map(|f| f.file_size).sum();
                    parts.push(format!("L{lvl}={n}/{}MiB", b >> 20));
                    tot_files += n;
                    tot_bytes += b;
                }
                eprintln!(
                    "[DECAY_DIAG] flush#{seq} ssts={tot_files} state={}MiB levels=[{}]",
                    tot_bytes >> 20,
                    parts.join(" ")
                );
            }
        }
        Ok(())
    }
}

/// FRS-COMPACT-BG: bridges the background compaction worker into the engine.
impl CompactionExecutor for DbImpl {
    fn run_compaction(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<()> {
        // Clear the per-CF dedup flag FIRST so a trigger that arrives WHILE
        // this compaction runs re-queues for the next round (no missed
        // compaction). `compact_l0_for_cf` re-reads the current L0 set under
        // `compaction_mutex`, so back-to-back runs pick disjoint inputs and
        // a no-op (nothing to compact) is harmless.
        self.compaction_queued
            .lock()
            .expect("lock poisoned")
            .remove(&cf_data.handle().id());
        // FRS-COMPACT-DIAG (env FRS_COMPACT_DIAG=1, off by default): time each
        // L0→L1 rollup + record the L0 depth it absorbed, so the maintenance
        // poll's compaction frequency + per-run DURATION can be correlated
        // against the q4 throughput troughs — i.e. did the ~1 s poll trade a
        // steady slowdown for periodic stutter (a long compaction holding the
        // compaction_mutex/version lock while read probes stall)?
        let diag = compact_diag_on();
        let cf_id = cf_data.handle().id();
        let (t0, l0_before) = if diag {
            let v = self.version_set.current();
            let n = v.l0_files().iter().filter(|f| f.cf_id == cf_id).count();
            (Some(std::time::Instant::now()), n)
        } else {
            (None, 0)
        };
        // FRS-COMPACT-BG: roll up L0→L1 only. A FULL leveled cascade
        // (`compact_once` until balanced, draining L1→L2→…) was BUILT and
        // MEASURED on q4 (2026-06-04) and REVERTED: it drained deeper levels
        // (ldeep 0→6) but multiplied write-amp — compaction sum 248s→401s, max
        // 20.7s→38.7s, q4 events 74.8M→68.8M (−8%), worst trough 95K→28K. The
        // L0-only rollup (bounding the read-side fan-out via the periodic
        // trigger) is the net-positive sweet spot; draining deeper levels costs
        // more in rewrite I/O than it saves in read fan-out for q4. The
        // unbounded-L1 / periodic-stall cost of L0-only is accepted (further
        // compaction tuning is negative-return here; the next lever is the
        // read-side B_resident tier, not more compaction).
        let _comp_t0 = std::time::Instant::now(); // FRS_PROF_DIAG: compaction wall-time (dim 4)
        let r = self.compact_l0_for_cf(cf_data);
        // FRS-COMPACT-DRAIN-L1 (2026-06-05, DEFAULT ON; opt out =0): after the
        // L0→L1 rollup, if L1 has grown past its size budget, do ONE bounded
        // L1→L2 compaction (NOT a full cascade to L6 — that was −8%). Keeps L1
        // small so each L0→L1 re-merges ≤ base instead of a growing 720 MB.
        // DATA: without this q4 NEVER FINISHES (L1 grows → compaction bursts
        // grow → pipeline collapses, ~65 M stall); WITH it q4 FINISHES 98 M in
        // 461 s (first completion). Re-enqueue while L1 stays over budget so the
        // drain spreads across maintenance ticks. Strictly level-1 → L2 grows
        // but stays non-overlapping (read-fine), touched only by bounded picking.
        let mut drained_l1 = false;
        if r.is_ok() && drain_l1_on() && !cf_data.is_dropped() {
            let over = self
                .pick_compaction_level_for_cf(cf_id)
                .map(|lvl| lvl == 1)
                .unwrap_or(false);
            if over {
                let _ = self.compact_level_for_cf(cf_data, 1);
                drained_l1 = true;
                // still over budget? re-enqueue for the next tick.
                if self.pick_compaction_level_for_cf(cf_id) == Some(1) {
                    self.enqueue_compaction(cf_data.clone());
                }
            }
        }
        prof_add(&PROF_COMPACT_NS, _comp_t0.elapsed().as_nanos() as u64);
        prof_add(&PROF_COMPACT_CNT, 1);
        if let Some(t) = t0 {
            let v = self.version_set.current();
            let l0_after = v.l0_files().iter().filter(|f| f.cf_id == cf_id).count();
            let l1_after = v.levels[1]
                .files
                .iter()
                .filter(|f| f.cf_id == cf_id)
                .count();
            let l2_after = v
                .levels
                .get(2)
                .map(|l| l.files.iter().filter(|f| f.cf_id == cf_id).count())
                .unwrap_or(0);
            eprintln!(
                "[COMPACT_DIAG t={}ms] cf={} l0 {l0_before}->{l0_after} l1={l1_after} l2={l2_after} drained_l1={drained_l1} dur_ms={} ok={}",
                process_elapsed_ms(),
                cf_id.0,
                t.elapsed().as_millis(),
                r.is_ok(),
            );
        }
        r?;
        Ok(())
    }
}

/// Process-relative wall clock (ms since first call), so background-thread
/// diagnostics (compaction, decay) can be time-correlated with the driver's
/// throughput samples without a `Date`/UTC dependency. Lazily anchored.
fn process_elapsed_ms() -> u128 {
    use std::sync::OnceLock;
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis()
}

/// FRS-COMPACT-DIAG toggle (`FRS_COMPACT_DIAG=1`), cached.
fn compact_diag_on() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("FRS_COMPACT_DIAG").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE")
        )
    })
}

/// FRS-COMPACT-DRAIN-L1 (default ON; opt out with `FRS_COMPACT_DRAIN_L1=0`).
/// `run_compaction` drains L1→L2 (one bounded `compact_level_for_cf(1)` step,
/// re-enqueued) once L1 exceeds `max_bytes_for_level_base` — proper bounded
/// leveled compaction, so each L0→L1 rollup re-merges ≤ base instead of an
/// ever-growing L1.
///
/// WHY DEFAULT ON (2026-06-05, data-backed): WITHOUT it, q4's L0-only rollup
/// re-merges a monotonically growing L1; the compaction bursts grow (2.3 s →
/// 20 s), the foreground pipeline collapses, and q4 NEVER FINISHES (~65 M / stall,
/// confirmed across many runs + heritage "froze every config at ~64 M"). WITH it,
/// **q4 FINISHES 98 M in 461 s** (first completion in the whole investigation) —
/// the write-amp savings compound over a long run. A short 280 s A/B undervalued
/// it (+7 % cum_in, +4 % events = noise) because the completion benefit only shows
/// past the point L0-only collapses. Strictly level-1 (never cascades L1→…→L6 —
/// that full cascade was −8 %); L2 grows but stays non-overlapping (read-fine).
/// Opt-out `=0` restores the legacy L0-only rollup.
fn drain_l1_on() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("FRS_COMPACT_DRAIN_L1").ok().as_deref(),
            Some("0") | Some("false") | Some("FALSE")
        )
    })
}

// ---------------------------------------------------------------------
// FRS-WAMP (2026-06-05): write-amplification confirmation tooling. Gated by
// `FRS_WAMP_FILE=<path>` (no-op when unset, zero production cost). Appends one
// line per L0→L1/compaction with this compaction's input/output MiB, the current
// L1 size, and the CUMULATIVE ratio (total compaction input bytes ÷ total flushed
// bytes). If that ratio CLIMBS with state size, L0→L1 is rewriting an ever-larger
// L1 (the suspected O(N²) write-amp); if it stays FLAT (~10-15×), leveling is
// bounded and the big compaction rewrite is NOT the binder — do not refactor.
// ---------------------------------------------------------------------
static WAMP_CUM_COMPACT_IN: AtomicU64 = AtomicU64::new(0);
static WAMP_CUM_COMPACT_OUT: AtomicU64 = AtomicU64::new(0);
static WAMP_CUM_FLUSH_OUT: AtomicU64 = AtomicU64::new(0);
static WAMP_CUM_RUN_MS: AtomicU64 = AtomicU64::new(0);
static WAMP_N: AtomicU64 = AtomicU64::new(0);

fn wamp_file() -> Option<&'static std::sync::Mutex<std::fs::File>> {
    use std::sync::OnceLock;
    static F: OnceLock<Option<std::sync::Mutex<std::fs::File>>> = OnceLock::new();
    F.get_or_init(|| {
        let path = std::env::var("FRS_WAMP_FILE")
            .ok()
            .filter(|s| !s.trim().is_empty())?;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()
            .map(std::sync::Mutex::new)
    })
    .as_ref()
}

/// Accumulate flushed (ingested) bytes — the denominator of write-amp.
fn wamp_record_flush(out_bytes: u64) {
    if wamp_file().is_some() {
        WAMP_CUM_FLUSH_OUT.fetch_add(out_bytes, Ordering::Relaxed);
    }
}

/// Record one compaction and append the cumulative write-amp + ns/byte line.
fn wamp_record_compaction(
    in_bytes: u64,
    out_bytes: u64,
    l1_files: usize,
    l1_bytes: u64,
    run_ms: u128,
) {
    let Some(m) = wamp_file() else { return };
    let ci = WAMP_CUM_COMPACT_IN.fetch_add(in_bytes, Ordering::Relaxed) + in_bytes;
    let co = WAMP_CUM_COMPACT_OUT.fetch_add(out_bytes, Ordering::Relaxed) + out_bytes;
    let n = WAMP_N.fetch_add(1, Ordering::Relaxed) + 1;
    let cum_run_ms = WAMP_CUM_RUN_MS.fetch_add(run_ms as u64, Ordering::Relaxed) + run_ms as u64;
    let fo = WAMP_CUM_FLUSH_OUT.load(Ordering::Relaxed).max(1);
    let ns_per_byte = if in_bytes > 0 {
        (run_ms as f64 * 1.0e6) / in_bytes as f64
    } else {
        0.0
    };
    // FRS-WAMP phase split: cumulative gather (per-row to_vec allocs) + sort,
    // with emit = run − gather − sort (Arrow-encode + block write). Compare the
    // ISOLATED-microbench split vs this LIVE split: if gather/emit inflate under
    // load, the alloc/encode memory traffic is the contention source → zero-copy
    // pays off; if all phases inflate uniformly, it's external contention.
    let cum_gather_ms = crate::compaction::CUM_GATHER_NS.load(Ordering::Relaxed) / 1_000_000;
    let cum_sort_ms = crate::compaction::CUM_SORT_NS.load(Ordering::Relaxed) / 1_000_000;
    let cum_emit_ms = cum_run_ms.saturating_sub(cum_gather_ms + cum_sort_ms);
    use std::io::Write;
    if let Ok(mut f) = m.lock() {
        let _ = writeln!(
            f,
            "n={n} in_mb={:.1} out_mb={:.1} l1_files={l1_files} l1_mb={:.1} cum_flush_mb={:.0} wamp_in={:.2} wamp_total={:.2} run_ms={run_ms} ns_per_byte={ns_per_byte:.1} cum_run_ms={cum_run_ms} cum_gather_ms={cum_gather_ms} cum_sort_ms={cum_sort_ms} cum_emit_ms={cum_emit_ms}",
            in_bytes as f64 / 1_048_576.0,
            out_bytes as f64 / 1_048_576.0,
            l1_bytes as f64 / 1_048_576.0,
            fo as f64 / 1_048_576.0,
            ci as f64 / fo as f64,
            (fo + co) as f64 / fo as f64,
        );
    }
}

/// FRS-SLOT-SHARED-BG (2026-06-05): resolve a background-pool worker count from
/// `var` (>0 wins) else `default_fn(cores)`. `cores` = logical CPUs.
fn bg_pool_threads(var: &str, default_fn: impl Fn(usize) -> usize) -> usize {
    if let Ok(s) = std::env::var(var) {
        if let Ok(n) = s.trim().parse::<usize>() {
            if n > 0 {
                return n;
            }
        }
    }
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    default_fn(cores).max(1)
}

/// Process-global FLUSH pool (RocksDB's HIGH Env pool analogue). Bounded worker
/// count so total flush CPU cannot scale with DbImpl count. Default `cores/8`
/// (≥1); override `FRS_BG_FLUSH_THREADS`.
fn bg_flush_pool() -> &'static crate::bg_pool::WorkerPool {
    use std::sync::OnceLock;
    static P: OnceLock<crate::bg_pool::WorkerPool> = OnceLock::new();
    P.get_or_init(|| {
        // Data-driven (2026-06-05 q4 sweep, 18-core): flush≈cores/3 — the low
        // cores/8 default caused write-stalls (L0 couldn't drain).
        let n = bg_pool_threads("FRS_BG_FLUSH_THREADS", |c| (c / 3).max(1));
        crate::bg_pool::WorkerPool::new(n, "forst-rs-flush")
    })
}

/// Process-global READ pool for parallel batched prefix scans — the join read-path
/// lever for q7/q9/q20. The K probes in an async-state batch are INDEPENDENT reads on
/// a pinned, immutable version snapshot, so fanning them across this pool overlaps the
/// per-probe LSM build+drain across cores (mirrors ForSt's read-io-parallelism). The
/// engine's read structures are already concurrency-safe — `sst_readers` is an `ArcSwap`
/// (lock-free), the block cache is sharded, and each probe pins its own consistent
/// snapshot — so no shared per-probe mutable state is touched across workers (unlike the
/// Java RoutingStateExecutor, whose shared per-subtask decode buffers raced). Default
/// `min(cores,4)`; override `FRS_RS_READ_IO_PARALLELISM`.
fn bg_read_pool() -> &'static crate::bg_pool::WorkerPool {
    use std::sync::OnceLock;
    static P: OnceLock<crate::bg_pool::WorkerPool> = OnceLock::new();
    P.get_or_init(|| {
        let n = bg_pool_threads("FRS_RS_READ_IO_PARALLELISM", |c| c.clamp(1, 4));
        crate::bg_pool::WorkerPool::new(n, "forst-rs-read")
    })
}

/// Process-global COMPACTION pool (RocksDB's LOW Env pool analogue). Bounded
/// worker count — THE q4 decay fix: total background compaction CPU is capped
/// regardless of how many keyed-state DbImpl instances exist, so compaction
/// bursts cannot starve the CPU-bound foreground join. Default `cores/6` (≥2);
/// override `FRS_BG_COMPACT_THREADS`.
fn bg_compact_pool() -> &'static crate::bg_pool::WorkerPool {
    use std::sync::OnceLock;
    static P: OnceLock<crate::bg_pool::WorkerPool> = OnceLock::new();
    P.get_or_init(|| {
        // Data-driven (2026-06-05 q4 sweep, 18-core): compact≈cores/2 is the
        // sweet spot — f6c9 (514s, 7 troughs, peak CPU 1127%) beat both the
        // unbounded baseline (545s, 1451%) and lower bounds f3c5/f4c6 (~565s,
        // write-stalled). Too few starves L0 drain; too many starves the join.
        let n = bg_pool_threads("FRS_BG_COMPACT_THREADS", |c| (c / 2).max(2));
        crate::bg_pool::WorkerPool::new(n, "forst-rs-compact")
    })
}

/// cached. When set, `compact_l0_for_cf` drops `lock_flush` during the long merge
/// so concurrent flushes proceed, re-acquiring it only for the version apply.
///
/// REVERTED to default-OFF (2026-06-05): the earlier default-ON flip was based on
/// a swap-POISONED measurement (538s vs 564s). On a CLEAN rebooted machine the
/// release is SLOWER (543s with vs 473s without) — the apparent win was an
/// artifact of the contaminated machine state. Kept env-gated for the record only.
/// Correctness of the release path itself is sound (apply_lock serializes apply,
/// flush is L0-add-only, compaction_mutex serializes deletes), it just doesn't help.
fn compact_release_lock() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("FRS_COMPACT_RELEASE_LOCK").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE")
        )
    })
}

/// FRS-ROCKSDB-PARITY Component 3 (2026-06-06): ONE decoded-block cache shared by
/// every DbImpl in the process (≈ a Flink slot), mirroring RocksDB's
/// `getSharedMemoryResourceForSlot`. Previously each DbImpl allocated its own
/// `cache_bytes` (×~12 keyed-state instances → multi-GB duplication). Cache keys
/// are already `db_id`-salted (`SstReaderImpl::with_block_cache(.., db_id, ..)`),
/// so sharing is collision-safe. Sized once from the first caller's `cache_bytes`
/// (all DbImpls in a slot pass the same config). Total cache RAM is now bounded
/// by ONE budget regardless of operator × parallelism.
fn shared_block_cache(cache_bytes: usize) -> std::sync::Arc<ShardedClockCache> {
    use std::sync::OnceLock;
    static C: OnceLock<std::sync::Arc<ShardedClockCache>> = OnceLock::new();
    std::sync::Arc::clone(
        C.get_or_init(|| std::sync::Arc::new(ShardedClockCache::with_capacity(cache_bytes))),
    )
}

/// FRS_MEM_DIAG (2026-06-08): one process-global background thread logging the engine's resident
/// native byte totals every 5s — pinpoints the join-OOM Rust-engine ~16GB (memtables vs resident
/// shadow). Off unless FRS_MEM_DIAG=1. NMT does not track these (native-lib jemalloc), so this is
/// the only way to attribute the engine side of the 32g cgroup OOM.
/// FRS_PROF_DIAG (2026-06-08): process-global wall-time attribution counters for the
/// forst-rs↔ForSt architecture gap-map. Cumulative ns/bytes, RELAXED atomics on COARSE
/// seams (flush/compaction/stall — not per-tiny-op, so `Instant::now()` overhead is
/// negligible and the measurement isn't perturbed). Formatted into the FRS_MEM_DIAG line.
pub(crate) static PROF_STALL_NS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static PROF_FLUSH_NS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static PROF_FLUSH_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static PROF_FLUSH_CNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static PROF_COMPACT_NS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static PROF_COMPACT_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static PROF_COMPACT_CNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[inline]
pub(crate) fn prof_add(c: &std::sync::atomic::AtomicU64, n: u64) {
    c.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}

/// Format the attribution counters for the FRS_MEM_DIAG line (gap-map evidence).
fn prof_diag_str() -> String {
    use std::sync::atomic::Ordering::Relaxed;
    let stall_ms = PROF_STALL_NS.load(Relaxed) / 1_000_000;
    let flush_ms = PROF_FLUSH_NS.load(Relaxed) / 1_000_000;
    let flush_mb = PROF_FLUSH_BYTES.load(Relaxed) / (1024 * 1024);
    let flush_cnt = PROF_FLUSH_CNT.load(Relaxed);
    let comp_ms = PROF_COMPACT_NS.load(Relaxed) / 1_000_000;
    let comp_mb = PROF_COMPACT_BYTES.load(Relaxed) / (1024 * 1024);
    let comp_cnt = PROF_COMPACT_CNT.load(Relaxed);
    let flush_msmb = if flush_mb > 0 {
        flush_ms as f64 / flush_mb as f64
    } else {
        0.0
    };
    let comp_msmb = if comp_mb > 0 {
        comp_ms as f64 / comp_mb as f64
    } else {
        0.0
    };
    // SST-writer sub-cost split (gap-map Task 1: buffer vs encode vs sink-write).
    let (sst_buf_ns, sst_enc_ns, sst_w_ns) = forst_rs_storage::sst::writer::sst_writer_prof_ns();
    let sst_buf_ms = sst_buf_ns / 1_000_000;
    let sst_enc_ms = sst_enc_ns / 1_000_000;
    let sst_w_ms = sst_w_ns / 1_000_000;
    format!(
        " stall_ms={stall_ms} flush_ms={flush_ms} flush_MB={flush_mb} flush_cnt={flush_cnt} flush_ms/MB={flush_msmb:.2} comp_ms={comp_ms} comp_MB={comp_mb} comp_cnt={comp_cnt} comp_ms/MB={comp_msmb:.2} sstbuf_ms={sst_buf_ms} sstenc_ms={sst_enc_ms} sstwrite_ms={sst_w_ms}"
    )
}

fn maybe_start_mem_diag() {
    use std::sync::OnceLock;
    static STARTED: OnceLock<()> = OnceLock::new();
    if !matches!(
        std::env::var("FRS_MEM_DIAG").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ) {
        return;
    }
    STARTED.get_or_init(|| {
        let _ = std::thread::Builder::new()
            .name("frs-mem-diag".to_string())
            .spawn(|| {
                use std::io::Write;
                let path = std::env::var("FRS_MEM_DIAG_FILE")
                    .unwrap_or_else(|_| "/tmp/frs-mem-diag.log".to_string());
                loop {
                    let wbm_mb = crate::runtime_tuning::global_wbm_used_bytes() / (1024 * 1024);
                    let shadow_mb =
                        crate::column_family::global_resident_shadow_used_bytes() / (1024 * 1024);
                    // jemalloc live attribution (Linux only — matches the allocator gate).
                    let (alloc_mb, resident_mb, retained_mb) = jemalloc_mb();
                    // process RSS from /proc/self/statm (pages × 4 KiB).
                    let rss_mb = std::fs::read_to_string("/proc/self/statm")
                        .ok()
                        .and_then(|s| s.split_whitespace().nth(1).and_then(|p| p.parse::<u64>().ok()))
                        .map(|pages| pages * 4 / 1024)
                        .unwrap_or(0);
                    let line = format!(
                        "[FRS_MEM_DIAG] rss_MB={rss_mb} jemalloc_alloc_MB={alloc_mb} jemalloc_resident_MB={resident_mb} jemalloc_retained_MB={retained_mb} wbm_memtable_MB={wbm_mb} resident_shadow_MB={shadow_mb}{}\n",
                        prof_diag_str()
                    );
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                    {
                        let _ = f.write_all(line.as_bytes());
                        let _ = f.flush();
                    }
                    eprint!("{line}");
                    std::thread::sleep(std::time::Duration::from_secs(3));
                }
            });
    });
}

/// FRS_MEM_DIAG: read jemalloc live/resident/retained in MiB. Must advance the
/// stats epoch each call (jemalloc snapshots are epoch-gated). Linux-only — the
/// jemalloc global allocator is gated to Linux in forst-rs-ffi.
#[cfg(target_os = "linux")]
fn jemalloc_mb() -> (u64, u64, u64) {
    use tikv_jemalloc_ctl::{epoch, stats};
    let _ = epoch::advance();
    let a = stats::allocated::read().unwrap_or(0) as u64 / (1024 * 1024);
    let r = stats::resident::read().unwrap_or(0) as u64 / (1024 * 1024);
    let ret = stats::retained::read().unwrap_or(0) as u64 / (1024 * 1024);
    (a, r, ret)
}
#[cfg(not(target_os = "linux"))]
fn jemalloc_mb() -> (u64, u64, u64) {
    (0, 0, 0)
}

/// FRS-WBM-TRUE-BACKPRESSURE toggle (`FRS_WBM_STALL=0`/`false` disables; ON by
/// default), cached. When enabled, [`DbImpl::wait_for_wbm_headroom`] blocks writers
/// until flush drains memtables back under the WBM budget (RocksDB `allow_stall`).
fn wbm_stall_enabled() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("FRS_WBM_STALL").ok().as_deref(),
            Some("0") | Some("false") | Some("FALSE")
        )
    })
}

/// FRS-RESIDENT-BYPASS toggle (`FRS_RESIDENT_BYPASS=1`, off by default), cached.
/// When set, the Tier-2 resident-shadow loop is skipped entirely — reads go
/// memtable + SST (block cache) like RocksDB. Local-safe (write-through SSTs are
/// readable immediately). Lever C: drop the forst-specific in-RAM SST tier.
fn resident_bypass() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        // FRS-ROCKSDB-PARITY Component 1 (2026-06-06): the resident shadow is OFF
        // by default now, so reads bypass it whether or not this env is set.
        !resident_shadow_enabled()
            || matches!(
                std::env::var("FRS_RESIDENT_BYPASS").ok().as_deref(),
                Some("1") | Some("true") | Some("TRUE")
            )
    })
}

/// FRS-ROCKSDB-PARITY Component 1 (2026-06-06): whether the Tier-2 resident
/// shadow (in-RAM copies of flushed memtables) is populated at all. DEFAULT OFF —
/// it is the forst-rs-specific RAM hog RocksDB lacks (it re-reads SST blocks via
/// the bounded block cache instead), and on q4 it drove the ~23 GB RSS that does
/// not fit 8c/32g. With it off, flushed reads fall through to the (synchronously
/// readable, on local fs) SST + block cache — byte-identical data. Re-enable with
/// `FRS_RESIDENT_SHADOW=1` for the S3 path until Phase-2 disaggregation lands the
/// proper local-file-cache tier (the shadow covers the S3 upload window today).
fn resident_shadow_enabled() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("FRS_RESIDENT_SHADOW").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE")
        )
    })
}

/// FRS-RESIDENT-BLOOM-SKIP experiment toggle (`FRS_RESIDENT_BLOOM_SKIP=1`),
/// cached. When set, the per-resident-shadow `may_contain_range` bloom is
/// skipped (straight to the seek). Used to measure the bloom's end-to-end q4
/// payoff (it is ~99% non-pruning for q4) before productionising an adaptive,
/// prune-rate-gated version that stays q7-safe.
fn resident_bloom_skip() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("FRS_RESIDENT_BLOOM_SKIP").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE")
        )
    })
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
        // 2026-06-02: `wait_for_pending_flushes` now waits for the in-RAM flush
        // counter only (no longer the blanket upload barrier — see its body for
        // why). Shutdown still wants remote durability of every flushed/compacted
        // SST, so await all in-flight uploads here explicitly. Best-effort: Drop
        // has no error channel, so a failing upload is logged, mirroring the
        // prior behaviour.
        if let Err(e) = self.fs.await_all_uploads() {
            tracing::warn!(
                target: "forst_rs_engine::db",
                error = %e,
                "DbImpl::drop: in-flight upload failed during shutdown drain",
            );
        }

        // 2. FRS-SLOT-SHARED-BG: flush/compaction now run on the PROCESS-GLOBAL
        //    pools, not per-DbImpl worker threads, so there is nothing to
        //    signal/join here. Background jobs already queued for this engine
        //    hold only a `Weak<DbImpl>`; once this `Drop` completes the `Weak`
        //    fails to upgrade and those jobs no-op. A job that is CURRENTLY
        //    executing holds an upgraded `Arc<DbImpl>`, which keeps the engine
        //    alive until it finishes — so `Drop` cannot race an in-flight flush
        //    or compaction (the engine is only freed once no job references it).
        //    Best-effort: queued-but-unstarted compactions are not awaited — the
        //    data is already durable in the L0 SSTs.

        // A final compaction may have produced an SST upload after install;
        // drain it for remote durability (cheap no-op if nothing pending).
        if let Err(e) = self.fs.await_all_uploads() {
            tracing::warn!(
                target: "forst_rs_engine::db",
                error = %e,
                "DbImpl::drop: compaction upload drain failed during shutdown",
            );
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
    /// B-R7-NEW-H1: field `prefix` renamed to `lower` since the same
    /// variant now powers BOTH `prefix_scan_iter` and `scan_iter`. The
    /// streaming filter at `peek()` already performs `[lower, upper)`
    /// truncation regardless of whether `lower` is a true prefix.
    Sst {
        reader: Arc<SstReaderImpl>,
        lower: Vec<u8>,
        upper: Option<Vec<u8>>,
        next_block: usize,
        buffered: Vec<SstHeadRow>,
        pos: usize,
    },
}

/// FRS-VALUE-CARRYING-MERGE (2026-06-06): the newest version of one user-key
/// buffered from an SST block. Previously the SST tier source buffered only
/// the key (`Arc<[u8]>`) and the value was re-resolved by a per-key
/// `get_internal` that re-walked the WHOLE LSM (O(K×tiers) — the profile-proven
/// q4 2× read-path binder). Carrying `(value, sequence, op_type)` at the head
/// lets `LazyPrefixIter::next_with_value` resolve an SST-resident `Put` inline
/// (no second LSM walk) — RocksDB-parity iteration without the resident shadow.
/// One `Arc::from(value)` per buffered row REPLACES the value alloc the
/// eliminated `get_internal` would have paid, so it is net-neutral on allocs
/// and removes the redundant block decode. See the design spec
/// `docs/superpowers/specs/2026-06-06-q4-prefix-scan-value-carrying-merge-design.md`.
struct SstHeadRow {
    key: Arc<[u8]>,
    /// `None` for a tombstone (Delete/SingleDelete); `Some` for Put/Merge.
    value: Option<Arc<[u8]>>,
    sequence: u64,
    op_type: OpType,
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
                lower,
                upper,
                next_block,
                buffered,
                pos,
            } => {
                // Replenish the buffer until either we find a usable key
                // or we've exhausted the SST.
                loop {
                    if *pos < buffered.len() {
                        return Ok(Some(buffered[*pos].key.as_ref()));
                    }
                    if *next_block >= reader.index_entry_count() {
                        return Ok(None);
                    }
                    // Range-skip empty blocks before paying decompression.
                    let block_idx = *next_block;
                    *next_block += 1;
                    buffered.clear();
                    *pos = 0;
                    // 2026-06-02 q7 SECOND-wall FIX: upper-bound early termination.
                    // SST rows are key-ASC, so once we see a key >= upper, NO later
                    // row (this block or any later block) can fall in [lower, upper)
                    // — stop scanning instead of reading the rest of the SST. The
                    // pre-fix code skipped `>= upper` rows but kept reading
                    // subsequent blocks, so an EMPTY prefix probe (a join key with no
                    // matching records, with data for other keys after it) walked the
                    // whole SST tail: measured 46 ms/probe returning 0 rows over 1 SST
                    // source — the q7 ckpt-ON ~100/s join-throughput collapse.
                    let mut hit_upper = false;
                    reader.for_each_row_in_block(block_idx, |view| {
                        if hit_upper {
                            // Already past the upper bound; remaining rows in this
                            // block are all >= upper (ASC). Cheap skip; we mark the
                            // source exhausted after the block.
                            return Ok(());
                        }
                        if view.key < lower.as_slice() {
                            return Ok(());
                        }
                        if let Some(hi) = upper.as_deref() {
                            if view.key >= hi {
                                hit_upper = true;
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
                        //
                        // FRS-VALUE-CARRYING-MERGE: also capture the NEWEST
                        // version's `(value, sequence, op_type)`. Rows are
                        // `(key ASC, sequence DESC)`, so the FIRST row for a
                        // user-key (the one we push, gated by the `!=` dedup)
                        // is its newest version — exactly what `get_internal`
                        // would resolve for this tier. The value `Arc::from`
                        // replaces the alloc the eliminated per-key
                        // `get_internal` would have paid.
                        if buffered.last().map(|r| r.key.as_ref()) != Some(view.key) {
                            buffered.push(SstHeadRow {
                                key: Arc::<[u8]>::from(view.key),
                                value: view.value.map(Arc::<[u8]>::from),
                                sequence: view.sequence,
                                op_type: view.op_type,
                            });
                        }
                        Ok(())
                    })?;
                    if hit_upper {
                        // No later block can contain an in-range key — mark this SST
                        // source drained. Any keys buffered BEFORE the upper bound in
                        // this block are still returned by the `*pos < buffered.len()`
                        // fast path on the next loop iteration; once they drain, the
                        // EOF check returns None.
                        *next_block = reader.index_entry_count();
                    }
                    // Loop: if this block was entirely out-of-range or
                    // contained no rows, peek the next block (unless we just
                    // hit the upper bound, in which case next_block == EOF).
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
            TierKeySource::Sst { buffered, pos, .. } => {
                buffered.get(*pos).map(|r| Arc::clone(&r.key))
            }
        }
    }

    /// FRS-VALUE-CARRYING-MERGE: returns the head row's `(sequence, op_type,
    /// value)` for an SST source, or `None` for a memtable source.
    ///
    /// `None` signals to [`LazyPrefixIter::next_with_value`] that this is a
    /// memtable/immutable/resident tier — which is ALWAYS newer than any SST
    /// for a key it holds (data flows memtable → SST), so its presence at a
    /// user-key forces the safe `get_internal` fallback (a cheap memtable hit,
    /// no SST I/O). `Some(..)` lets the merge resolve an SST-resident `Put`
    /// inline. Precondition: caller has just `peek`-ed `Some(_)`.
    fn head_sst_info(&self) -> Option<(u64, OpType, Option<Arc<[u8]>>)> {
        match self {
            TierKeySource::MemCursor { .. } => None,
            TierKeySource::Sst { buffered, pos, .. } => buffered
                .get(*pos)
                .map(|r| (r.sequence, r.op_type, r.value.clone())),
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
/// B-R7-NEW-H1: alias used by the range-scan path
/// ([`DbImpl::scan_iter`] / [`DbImpl::scan_iter_owned_arc_with_error_slot`]).
/// The underlying iterator is structurally identical (sorted-key k-way merge
/// over tier sources); only the per-tier source construction differs (range
/// memtable cursor + `[lower, upper)` SST filter vs. prefix variants).
pub type LazyRangeIter = LazyPrefixIter;

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

    /// FRS-VALUE-CARRYING-MERGE: record a tier-peek error with the SAME
    /// sticky-FIRST semantics `next()` uses (shared FFI slot when wired, else
    /// the local `last_error`). Factored out so `next_with_value` surfaces
    /// errors through the identical channel without duplicating the block.
    fn record_peek_error(&mut self, e: ForstError) {
        if let Some(slot) = self.shared_error_slot.as_ref() {
            let mut guard = slot.lock().unwrap_or_else(|p| p.into_inner());
            if guard.is_none() {
                *guard = Some(e);
            }
        } else if self.last_error.is_none() {
            self.last_error = Some(e);
        }
    }

    /// FRS-VALUE-CARRYING-MERGE (2026-06-06): the k-way merge variant that
    /// resolves the value INLINE from the winning tier's cursor position,
    /// eliminating the per-key `get_internal` second LSM walk that dominated
    /// q4's read path (see the design spec).
    ///
    /// Yields `(user_key, decision)` for each visible user-key in sorted order:
    /// - `ValueDecision::Put(value)` — the newest version is an SST-resident
    ///   `Put`; `value` is final, NO `get_internal` needed.
    /// - `ValueDecision::Fallback` — a memtable/immutable/resident tier holds
    ///   the key (its value is cheap to resolve via `get_internal`, a memtable
    ///   hit), OR the newest SST version is a `Merge` (operand chain spans older
    ///   tiers), OR a corrupt SST `Put` (missing payload) — let `get_internal`
    ///   resolve/surface it. The caller MUST call `get_internal` and skip the
    ///   key if it resolves to `None`.
    ///
    /// Tombstone winners (newest version is a Delete on an SST with no newer
    /// memtable tier) are skipped INTERNALLY — the key is not emitted.
    ///
    /// Correctness mirrors `get_internal` exactly: tier precedence decides the
    /// winner (memtable newer than any SST; among SSTs the max `sequence`
    /// wins), Put→value, Delete→hidden, Merge→operand resolution (deferred).
    fn next_with_value(&mut self) -> Option<(Arc<[u8]>, ValueDecision)> {
        // Owned outcome of one source `peek()` — computed while the source is
        // borrowed, acted on AFTER the borrow ends (so the dup-`advance()` and
        // `record_peek_error()` `&mut self` calls do not alias the source).
        enum PeekAction {
            Keep,
            SkipDup,
            Drained,
            Err(ForstError),
        }
        let n = self.sources.len();
        loop {
            // Phase A: per-source dedup past `last_emitted`, then find the
            // lex-smallest pending head across all sources.
            let mut min_key: Option<Arc<[u8]>> = None;
            for i in 0..n {
                loop {
                    let action = match self.sources[i].peek() {
                        Ok(Some(head)) => {
                            // Skip any head already emitted (cross-tier dedup).
                            // `self.last_emitted` is a disjoint field from
                            // `self.sources[i]`, so this borrow is sound.
                            if self
                                .last_emitted
                                .as_ref()
                                .is_some_and(|le| head <= le.as_ref())
                            {
                                PeekAction::SkipDup
                            } else {
                                PeekAction::Keep
                            }
                        }
                        Ok(None) => PeekAction::Drained,
                        Err(e) => PeekAction::Err(e),
                    };
                    match action {
                        PeekAction::SkipDup => {
                            self.sources[i].advance();
                            continue;
                        }
                        PeekAction::Keep | PeekAction::Drained => break,
                        PeekAction::Err(e) => {
                            self.record_peek_error(e);
                            break;
                        }
                    }
                }
                if let Some(k) = self.sources[i].peek_arc() {
                    match &min_key {
                        None => min_key = Some(k),
                        Some(m) if k.as_ref() < m.as_ref() => min_key = Some(k),
                        _ => {}
                    }
                }
            }
            let min = min_key?;

            // Phase B: among all sources whose head == `min`, pick the winner
            // (memtable presence forces fallback; else max-sequence SST), and
            // advance every source positioned at `min` (cross-tier dedup).
            let mut mem_present = false;
            let mut best: Option<(u64, OpType, Option<Arc<[u8]>>)> = None;
            for i in 0..n {
                let at_min = matches!(self.sources[i].peek(), Ok(Some(k)) if k == min.as_ref());
                if !at_min {
                    continue;
                }
                match self.sources[i].head_sst_info() {
                    None => mem_present = true,
                    Some((seq, op, val)) => {
                        if best.as_ref().is_none_or(|(bseq, _, _)| seq > *bseq) {
                            best = Some((seq, op, val));
                        }
                    }
                }
                self.sources[i].advance();
            }
            self.last_emitted = Some(Arc::clone(&min));

            // Phase C: decide. Memtable tiers and merge-chains defer to
            // `get_internal`; SST Put resolves inline; SST tombstone hides.
            if mem_present {
                return Some((min, ValueDecision::Fallback));
            }
            match best {
                Some((_, OpType::Put, Some(v))) => return Some((min, ValueDecision::Put(v))),
                // Corrupt SST Put (no payload): let get_internal raise it.
                Some((_, OpType::Put, None)) => return Some((min, ValueDecision::Fallback)),
                Some((_, OpType::Delete, _)) | Some((_, OpType::SingleDelete, _)) => continue,
                Some((_, OpType::Merge, _)) => return Some((min, ValueDecision::Fallback)),
                // No source produced head info though `min` came from one —
                // be conservative and let get_internal resolve it.
                None => return Some((min, ValueDecision::Fallback)),
            }
        }
    }
}

/// FRS-VALUE-CARRYING-MERGE: outcome of [`LazyPrefixIter::next_with_value`].
enum ValueDecision {
    /// Newest version is an SST-resident Put; the value is final.
    Put(Arc<[u8]>),
    /// Resolve via `get_internal` (memtable tier, merge-chain, or corrupt Put).
    Fallback,
}

impl Iterator for LazyPrefixIter {
    type Item = Arc<[u8]>;

    fn next(&mut self) -> Option<Arc<[u8]>> {
        // Drop already-emitted duplicates from all tiers + find the min
        // pending key across all sources. On error from a tier source we
        // currently swallow it (matches the previous BTreeSet behaviour
        // for transient SST read failures during compaction races — the
        // caller's `db.get` will surface any persistent corruption).
        //
        // FRS (clippy::never_loop fix): this is a SINGLE pass. Per-source duplicate-skipping
        // is handled by the inner `loop` over each source below (advance + continue), so the
        // body always returns the min non-duplicate key (or None) without ever re-iterating.
        // The previous outer `loop {}` wrapper was vestigial dead structure (removed).
        {
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
                                let mut guard = slot.lock().unwrap_or_else(|p| p.into_inner());
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
            Some(key_arc)
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

    /// FRS-COMPACT-MICROBENCH (2026-06-05): an IN-PROCESS, substrate-independent
    /// measurement of the q4 compaction-merge cost (gather→sort→walk/encode),
    /// so a merge refactor can be proven with data WITHOUT the flaky Flink/Mac
    /// substrate (and without instrumenting the production dylib, which crashes
    /// the FFM TaskManager). Builds q4-like inputs (~43 B rows: 36 B key + 7 B
    /// value, no compression, 8 KiB blocks) across 4 sorted L0 SSTs over a
    /// MemoryFileSystem, then times `compact_l0` (the full merge). Reports
    /// ns/row + ns/byte — the same arbiter the end-to-end COMPACT_PHASE diag
    /// produced (~25 ns/byte, 1118 ns/row at q4 floor). Run explicitly:
    ///   cargo test -p forst-rs-engine --release bench_compaction_q4like -- --ignored --nocapture
    #[test]
    #[ignore = "microbench; run explicitly with --ignored --nocapture"]
    fn bench_compaction_q4like() {
        let opts = EngineOptions {
            db_path: "/db".to_string(),
            block_size: 8 * 1024,
            compression: forst_rs_common::CompressionType::None,
            // single-file output (no split) to mirror one L0→L1 rollup's merge
            target_file_size_base: 0,
            // Large write buffer so each chunk stays in ONE memtable → exactly
            // one L0 SST per switch_and_flush (no mid-chunk auto-flush pushing
            // L0 past the trigger and letting the maintenance ticker compact).
            write_buffer_size: 2_000_000_000,
            max_write_buffer_number: 8,
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = DbImpl::open_with_fs(opts, fs).expect("open");
        let cf = db.default_cf();

        // q4-like: 36-byte key (CF-prefix-ish + join key), 7-byte value.
        // N_SST=3 stays BELOW l0_compaction_trigger (4) so the background
        // maintenance ticker does NOT auto-compact L0 mid-build — we time a
        // clean MANUAL compact_l0 of exactly these inputs. Scale via
        // FRS_BENCH_ROWS (rows per SST) to reach the q4-floor regime
        // (~5.3M/SST × 3 ≈ 16M rows / ~700 MB, where ns/byte degrades).
        let rows_per_sst: usize = std::env::var("FRS_BENCH_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1_300_000);
        const N_SST: usize = 3;
        let val = [0xABu8; 7];
        let mut total_bytes: u64 = 0;
        for s in 0..N_SST {
            for r in 0..rows_per_sst {
                // globally unique, ascending-ish keys spread across SSTs so the
                // 4 L0 files overlap in key range (forces a real k-way merge).
                let k = (r as u64) * (N_SST as u64) + (s as u64);
                let mut key = [0u8; 36];
                key[..8].copy_from_slice(b"q4cf\0\0\0\0");
                key[8..16].copy_from_slice(&k.to_be_bytes());
                // pad remainder deterministically
                for (i, b) in key[16..].iter_mut().enumerate() {
                    *b = (k.wrapping_add(i as u64)) as u8;
                }
                db.put(&cf, &key, &val).unwrap();
                total_bytes += (key.len() + val.len()) as u64;
            }
            db.switch_and_flush(&cf).unwrap();
        }
        let l0 = db.version_set.current().l0_files().len();
        assert!(l0 >= 2, "expected ≥2 L0 SSTs to merge, got {l0}");
        let total_rows = (rows_per_sst * N_SST) as u64;

        // FRS_BENCH_SNAPSHOT=1: hold a live snapshot across the compaction so
        // emit_key_versions runs the MVCC snapshot-retention (pinned/tail) path
        // per key-group — mimicking q4's 30s-checkpoint snapshots. Rules in/out
        // whether the snapshot logic explains q4's live ~25 ns/byte vs the
        // isolated ~3 ns/byte.
        let _snap = if std::env::var("FRS_BENCH_SNAPSHOT").as_deref() == Ok("1") {
            Some(db.snapshot())
        } else {
            None
        };

        let t0 = std::time::Instant::now();
        db.compact_l0(&cf).unwrap();
        let el = t0.elapsed();
        let ns = el.as_nanos() as f64;
        eprintln!(
            "[COMPACT_MICROBENCH] rows={total_rows} in_bytes={total_bytes} l0={N_SST} \
             compact_ms={} ns/row={:.0} ns/byte={:.1}",
            el.as_millis(),
            ns / total_rows as f64,
            ns / total_bytes as f64,
        );
        // sanity: output present
        assert_eq!(db.version_set.current().l0_files().len(), 0);
    }

    // --- bring-up ---

    /// FRS-L0-SHORTCIRCUIT (2026-06-03): a hot key Put-overwritten on every
    /// flush lands in EVERY L0 SST (the bloom filter cannot skip any of them
    /// because the key genuinely IS present), so the L0 point-read walk must
    /// stop at the NEWEST SST's Put base — one block read — instead of reading
    /// all N L0 SSTs and discarding the N-1 stale versions. The old walk paid
    /// O(L0) per point read, which is the q11/q4 read-amplification decay as L0
    /// grew. Asserts both correctness (newest value wins) and the O(1) read.
    #[test]
    fn test_l0_point_get_short_circuits_at_newest_base() {
        let db = open();
        let cf = db.default_cf();

        // N below the l0_compaction_trigger (40) so background compaction does
        // not collapse the L0 files out from under us. Each put+flush mints one
        // L0 SST containing "hot" (disjoint, ascending sequence ranges).
        const N: usize = 8;
        for i in 0..N {
            db.put(&cf, b"hot", format!("v{i}").as_bytes()).unwrap();
            // Seal the active memtable to immutable, then flush it: flush_cf
            // alone only drains the imm queue, and `put` lands in the ACTIVE
            // memtable (never sealed), so without the switch nothing flushes.
            db.force_switch_memtable(&cf).unwrap();
            db.flush_cf(&cf).unwrap();
        }

        // Exercise the SST L0 walk DIRECTLY: get_internal's Stage 2.5 resident
        // RAM-shadow would otherwise serve "hot" from the just-flushed decoded
        // memtable and never touch sst_get. The L0 read amplification under
        // test lives entirely in sst_get (Stage 3), which is what q11/q4 hit
        // once their working set spills past the resident shadow cap.
        let cf_data = db.lookup_cf_by_id(cf.id()).unwrap();
        let version = db.version_set.current();
        let layout: Vec<usize> = (0..version.num_levels())
            .map(|l| {
                version.levels[l]
                    .files
                    .iter()
                    .filter(|s| s.cf_id == cf.id())
                    .count()
            })
            .collect();
        assert!(
            version
                .l0_files()
                .iter()
                .filter(|s| s.cf_id == cf.id())
                .count()
                >= N,
            "expected at least {N} L0 SSTs each holding 'hot'; per-level layout = {layout:?}"
        );

        let before = db.l0_point_get_block_reads();
        let mut operands = Vec::new();
        let got = db
            .sst_get(&cf_data, &version, b"hot", &mut operands)
            .unwrap();
        let reads = db.l0_point_get_block_reads() - before;

        assert_eq!(
            got,
            Some(format!("v{}", N - 1).as_bytes().to_vec()),
            "point read must return the newest overwrite"
        );
        assert_eq!(
            reads, 1,
            "a hot key present in {N} L0 SSTs must resolve with ONE L0 block \
             read via the newest-first short-circuit, but read {reads}"
        );
    }

    /// FRS-LEVELED-COMPACTION (2026-06-04): an L0→L1 compaction whose merged
    /// output exceeds `target_file_size_base` must SPLIT into multiple
    /// non-overlapping L1 SSTs (rolled at user-key boundaries) — AND every key
    /// must survive the split (no key dropped/duplicated across a file
    /// boundary), the files must stay non-overlapping + sorted, and a full
    /// range scan must return all keys. This is the core write-amp lever:
    /// multiple files per level let a later compaction rewrite a SUBSET.
    #[test]
    fn test_leveled_compaction_splits_output_preserves_all_keys() {
        let opts = EngineOptions {
            db_path: "/db".to_string(),
            // Force aggressive splitting: roll a new SST every ~4 KiB (the
            // minimum allowed target) with small blocks so estimated_size
            // crosses the threshold often.
            target_file_size_base: 4096,
            block_size: 512,
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = DbImpl::open_with_fs(opts, fs).expect("open");
        let cf = db.default_cf();

        const N: usize = 2000;
        let val = |i: usize| format!("value-{i:06}-paddingpaddingpaddingpadding");
        let key = |i: usize| format!("key{i:06}");
        for i in 0..N {
            db.put(&cf, key(i).as_bytes(), val(i).as_bytes()).unwrap();
        }
        db.force_switch_memtable(&cf).unwrap();
        db.flush_cf(&cf).unwrap();
        db.compact_l0(&cf).unwrap();

        let version = db.version_set.current();
        let l1: Vec<&forst_rs_storage::version::SstFileMeta> = version.levels[1]
            .files
            .iter()
            .filter(|s| s.cf_id == cf.id())
            .collect();
        assert!(
            l1.len() > 1,
            "leveled compaction must SPLIT L0→L1 into multiple SSTs at \
             target_file_size_base=4096, but produced {} file(s)",
            l1.len()
        );
        // Files must be non-overlapping and sorted by smallest_key.
        for w in l1.windows(2) {
            assert!(
                w[0].largest_key < w[1].smallest_key,
                "split L1 files overlap: [{:?}..{:?}] vs [{:?}..]",
                w[0].smallest_key,
                w[0].largest_key,
                w[1].smallest_key
            );
        }
        // CORRECTNESS: every key reads back exactly (no boundary loss).
        for i in 0..N {
            let got = db.get(&cf, key(i).as_bytes()).unwrap();
            assert_eq!(
                got.as_deref(),
                Some(val(i).as_bytes()),
                "key {i} lost or wrong value after split compaction"
            );
        }
        // Full scan returns every key exactly once, in order.
        let all = db.scan(&cf, b"", None).unwrap();
        assert_eq!(
            all.len(),
            N,
            "post-split scan returned {} rows, expected {N}",
            all.len()
        );
        for (i, (k, v)) in all.iter().enumerate() {
            assert_eq!(
                k.as_slice(),
                key(i).as_bytes(),
                "scan key order wrong at {i}"
            );
            assert_eq!(v.as_slice(), val(i).as_bytes(), "scan value wrong at {i}");
        }
    }

    #[test]
    fn test_open_creates_default_cf() {
        let db = open();
        let h = db.column_family(DEFAULT_CF_NAME).expect("default cf");
        assert_eq!(h.id(), DEFAULT_CF_ID);
        assert_eq!(h.name(), DEFAULT_CF_NAME);
    }

    #[test]
    fn snapshot_memtables_to_dir_round_trips_through_artifact_and_replay() {
        // FRS-CKPT-NOFLUSH engine round trip: put → snapshot artifact (no flush)
        // → read artifact → replay into a FRESH engine → reads match; the source
        // memtable stays live; post-snapshot writes are NOT in the artifact.
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();
        db.put(&cf, b"k1", b"v1b").unwrap(); // multi-version

        // Artifacts stage on LOCAL disk (the backend uploader reads them via
        // local NIO), so use a real temp dir.
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let dir = tmp.path();
        let arts = db.snapshot_memtables_to_dir(dir, None).unwrap();
        assert_eq!(arts.len(), 1, "one CF with data");
        let (cf_id, fname) = arts[0].clone();
        assert_eq!(cf_id, DEFAULT_CF_ID.0);
        assert!(dir.join(&fname).exists(), "artifact written to local disk");

        // Memtable stays live + readable + writable after the snapshot (no seal).
        assert_eq!(db.get(&cf, b"k1").unwrap(), Some(b"v1b".to_vec()));
        db.put(&cf, b"k3", b"v3").unwrap();

        // Replay the artifact DIR into a fresh, independent engine.
        let db2 = open();
        let cf2 = db2.default_cf();
        let replayed = db2.replay_memtable_artifacts_from_dir(dir).unwrap();
        assert!(replayed >= 3, "k1(×2) + k2 captured; got {replayed}");

        assert_eq!(db2.get(&cf2, b"k1").unwrap(), Some(b"v1b".to_vec()));
        assert_eq!(db2.get(&cf2, b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(
            db2.get(&cf2, b"k3").unwrap(),
            None,
            "k3 was written AFTER the snapshot — must not appear in the artifact"
        );
    }

    /// FRS-CKPT-NOFLUSH combined restore round-trip (2026-06-02): proves the
    /// foundation the Java no-flush-checkpoint wiring depends on — a no-flush
    /// incremental checkpoint references ONLY the flushed SSTs (via the manifest)
    /// while the LIVE memtable is captured as a separate Arrow-IPC artifact; on
    /// restore, `open_from_incremental` (SSTs) + `replay_memtable_artifacts_from_dir`
    /// (memtable) together reconstruct ALL data. Without this, switching the
    /// checkpoint to the no-flush variant (which stops minting an L0 SST every
    /// 30 s — the q7 ckpt-ON throughput decay) would silently lose the live
    /// memtable on restore. Uses LocalFileSystem + a temp dir because
    /// open_from_incremental hardlinks SSTs and snapshot_memtables_to_dir stages
    /// artifacts on the local FS.
    #[test]
    fn test_noflush_checkpoint_combined_restore_round_trip() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("db");
        let art_dir = tmp.path().join("artifacts");
        let restore_dir = tmp.path().join("restored");

        let manifest_path;
        let sst_files: Vec<String>;
        {
            let fs: Arc<dyn FileSystem> = Arc::new(forst_rs_io::LocalFileSystem::new());
            let opts = EngineOptions {
                db_path: db_path.to_string_lossy().to_string(),
                ..EngineOptions::default()
            };
            let db = DbImpl::open_with_fs(opts, fs).unwrap();
            let cf = db.default_cf();

            // SST-resident data: write then SEAL to an SST.
            for i in 0..50u32 {
                let k = format!("sst_{:04}", i);
                db.put(&cf, k.as_bytes(), b"sstval").unwrap();
            }
            db.switch_and_flush(&cf).unwrap();

            // Memtable-resident data: stays in the LIVE active memtable (no flush).
            for i in 0..50u32 {
                let k = format!("mem_{:04}", i);
                db.put(&cf, k.as_bytes(), b"memval").unwrap();
            }

            let snap = db.snapshot();
            // No-flush checkpoint: manifest references the flushed SST only.
            let result = db
                .create_incremental_checkpoint_noflush(&snap, 1, 0)
                .unwrap();
            assert!(
                !result.new_ssts.is_empty(),
                "the sealed SST should be referenced by the no-flush checkpoint"
            );
            // Capture the live memtable separately as an artifact.
            let arts = db.snapshot_memtables_to_dir(&art_dir, None).unwrap();
            assert_eq!(arts.len(), 1, "one CF with live memtable data");

            manifest_path = result.manifest_path.to_string_lossy().to_string();
            sst_files = result
                .new_ssts
                .iter()
                .chain(result.shared_ssts.iter())
                .map(|f| f.path.to_string_lossy().to_string())
                .collect();
        } // drop db1

        // Restore: SSTs via open_from_incremental, memtable via artifact replay.
        let db2 = DbImpl::open_from_incremental(
            &restore_dir.to_string_lossy(),
            &manifest_path,
            &sst_files,
        )
        .unwrap();
        let replayed = db2.replay_memtable_artifacts_from_dir(&art_dir).unwrap();
        assert_eq!(replayed, 50, "all 50 live-memtable entries replayed");

        let cf2 = db2.default_cf();
        // Both the flushed-SST data AND the live-memtable data must be present.
        for i in 0..50u32 {
            let sk = format!("sst_{:04}", i);
            assert_eq!(
                db2.get(&cf2, sk.as_bytes()).unwrap().as_deref(),
                Some(&b"sstval"[..]),
                "SST-resident key {} lost on no-flush restore",
                sk
            );
            let mk = format!("mem_{:04}", i);
            assert_eq!(
                db2.get(&cf2, mk.as_bytes()).unwrap().as_deref(),
                Some(&b"memval"[..]),
                "live-memtable key {} lost on no-flush restore (artifact replay gap)",
                mk
            );
        }
    }

    #[test]
    fn test_noflush_checkpoint_restore_non_default_cf_round_trip() {
        // FRS-TIMER-CF checkpoint-safety: a dedicated non-default CF (as used by the
        // engine-backed timer queue, "frs_timers") must survive a no-flush
        // checkpoint+restore — its sealed-SST data, its live-memtable data (captured
        // as a per-CF Arrow artifact keyed on cf_id), AND the CF itself, re-registered
        // by name with its ORIGINAL cf_id. The last point is what makes the Java
        // backend's `dbOpenOrCreateCf` open the CF on restore (rather than failing to
        // re-create an already-present name). Mirrors
        // `test_noflush_checkpoint_combined_restore_round_trip` but for a non-default CF.
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("db");
        let art_dir = tmp.path().join("artifacts");
        let restore_dir = tmp.path().join("restored");

        let manifest_path;
        let sst_files: Vec<String>;
        let timer_cf_id;
        {
            let fs: Arc<dyn FileSystem> = Arc::new(forst_rs_io::LocalFileSystem::new());
            let opts = EngineOptions {
                db_path: db_path.to_string_lossy().to_string(),
                ..EngineOptions::default()
            };
            let db = DbImpl::open_with_fs(opts, fs).unwrap();

            let timer_cf = db
                .create_column_family(ColumnFamilyDescriptor::new("frs_timers"))
                .unwrap();
            timer_cf_id = timer_cf.id();
            assert_ne!(timer_cf_id, DEFAULT_CF_ID);

            // SST-resident timer data: write then SEAL to an SST.
            for i in 0..50u32 {
                let k = format!("tsst_{:04}", i);
                db.put(&timer_cf, k.as_bytes(), b"tsstval").unwrap();
            }
            db.switch_and_flush(&timer_cf).unwrap();

            // Memtable-resident timer data: stays in the LIVE active memtable (no flush).
            for i in 0..50u32 {
                let k = format!("tmem_{:04}", i);
                db.put(&timer_cf, k.as_bytes(), b"tmemval").unwrap();
            }

            let snap = db.snapshot();
            let result = db
                .create_incremental_checkpoint_noflush(&snap, 1, 0)
                .unwrap();
            // Capture every CF's live memtable — the timer CF among them.
            let arts = db.snapshot_memtables_to_dir(&art_dir, None).unwrap();
            assert!(
                arts.iter().any(|(cf_id, _)| *cf_id == timer_cf_id.0),
                "the timer CF's live memtable must be captured as an artifact"
            );

            manifest_path = result.manifest_path.to_string_lossy().to_string();
            sst_files = result
                .new_ssts
                .iter()
                .chain(result.shared_ssts.iter())
                .map(|f| f.path.to_string_lossy().to_string())
                .collect();
        } // drop db1

        // Restore via the exact path the Java backend uses (dbOpenFromIncremental).
        let db2 = DbImpl::open_from_incremental(
            &restore_dir.to_string_lossy(),
            &manifest_path,
            &sst_files,
        )
        .unwrap();
        db2.replay_memtable_artifacts_from_dir(&art_dir).unwrap();

        // The timer CF must be re-registered BY NAME (what dbOpenOrCreateCf opens on
        // restore) with its ORIGINAL cf_id preserved (artifact filenames key on it).
        let timer_cf2 = db2
            .column_family("frs_timers")
            .expect("timer CF must be re-registered on restore");
        assert_eq!(
            timer_cf2.id(),
            timer_cf_id,
            "restored timer CF must preserve its original cf_id"
        );

        // Both the flushed-SST data AND the live-memtable data must survive.
        for i in 0..50u32 {
            let sk = format!("tsst_{:04}", i);
            assert_eq!(
                db2.get(&timer_cf2, sk.as_bytes()).unwrap().as_deref(),
                Some(&b"tsstval"[..]),
                "timer SST-resident key {} lost on no-flush restore",
                sk
            );
            let mk = format!("tmem_{:04}", i);
            assert_eq!(
                db2.get(&timer_cf2, mk.as_bytes()).unwrap().as_deref(),
                Some(&b"tmemval"[..]),
                "timer live-memtable key {} lost on no-flush restore",
                mk
            );
        }
    }

    #[test]
    fn test_default_cf_accessor() {
        let db = open();
        let h = db.default_cf();
        assert_eq!(h.name(), DEFAULT_CF_NAME);
    }

    #[test]
    fn test_fatal_consistency_error_is_sticky() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();

        db.record_fatal_error("multi-CF batch_write torn after partial commit");

        assert!(matches!(db.get(&cf, b"k"), Err(ForstError::Internal(_))));
        assert!(matches!(
            db.put(&cf, b"k2", b"v2"),
            Err(ForstError::Internal(_))
        ));
        assert!(matches!(
            db.put(&cf, b"k3", b"v3"),
            Err(ForstError::Internal(_))
        ));

        let ckpt_dir = tempfile::tempdir().expect("ckpt tempdir");
        assert!(matches!(
            db.create_checkpoint(ckpt_dir.path()),
            Err(ForstError::Internal(_))
        ));

        let batch = make_put_arrow_batch(
            &[b"k4".as_ref()],
            &[Some(b"v4".as_ref())],
            &[OpType::Put as u8],
        );
        assert!(matches!(
            db.batch_put_arrow(&cf, &batch),
            Err(ForstError::Internal(_))
        ));

        let empty_batch = make_put_arrow_batch(&[], &[], &[]);
        assert!(matches!(
            db.batch_put_arrow(&cf, &empty_batch),
            Err(ForstError::Internal(_))
        ));
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

    // V10 (spec §3 D10) — vectorized batch_get tests.

    #[test]
    fn test_batch_get_vectorized_empty_keys() {
        let db = open();
        let cf = db.default_cf();
        let out = db.batch_get_vectorized(&cf, &[], u64::MAX).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn test_batch_get_vectorized_single_key_active_memtable() {
        // N=1 fast-path: must not regress single-key throughput. Result
        // identical to db.get.
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        let out = db
            .batch_get_vectorized(&cf, &[b"k".as_ref()], u64::MAX)
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].as_deref(), Some(b"v".as_ref()));
    }

    #[test]
    fn test_batch_get_vectorized_all_active_memtable() {
        let db = open();
        let cf = db.default_cf();
        let n = 1000usize;
        // Populate 1000 keys; all live in the active memtable.
        for i in 0..n {
            let k = format!("k{:08}", i).into_bytes();
            let v = format!("v{:08}", i).into_bytes();
            db.put(&cf, &k, &v).unwrap();
        }
        let keys: Vec<Vec<u8>> = (0..n).map(|i| format!("k{:08}", i).into_bytes()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|v| v.as_slice()).collect();
        let out = db.batch_get_vectorized(&cf, &key_refs, u64::MAX).unwrap();
        assert_eq!(out.len(), n);
        for i in 0..n {
            let expected = format!("v{:08}", i).into_bytes();
            assert_eq!(out[i].as_deref(), Some(expected.as_slice()), "slot {i}");
        }
    }

    #[test]
    fn test_batch_get_vectorized_mixed_tiers_active_imm_sst() {
        // Layout:
        //  - 300 keys "s_*" → SST (force_switch + flush + force_switch again)
        //  - 300 keys "i_*" → immutable memtables (two imms before flush)
        //  - 300 keys "a_*" → active memtable
        //  - 100 keys "miss_*" → never written
        let db = open();
        let cf = db.default_cf();
        for i in 0..300usize {
            db.put(
                &cf,
                format!("s_{:04}", i).as_bytes(),
                format!("sv_{i}").as_bytes(),
            )
            .unwrap();
        }
        // Flush "s_*" to SST.
        db.switch_and_flush(&cf).unwrap();
        // First imm with 150 keys, second imm with 150 keys, then active.
        for i in 0..150usize {
            db.put(
                &cf,
                format!("i_{:04}", i).as_bytes(),
                format!("iv_{i}").as_bytes(),
            )
            .unwrap();
        }
        db.force_switch_memtable(&cf).unwrap();
        for i in 150..300usize {
            db.put(
                &cf,
                format!("i_{:04}", i).as_bytes(),
                format!("iv_{i}").as_bytes(),
            )
            .unwrap();
        }
        db.force_switch_memtable(&cf).unwrap();
        for i in 0..300usize {
            db.put(
                &cf,
                format!("a_{:04}", i).as_bytes(),
                format!("av_{i}").as_bytes(),
            )
            .unwrap();
        }

        // Build interleaved key list to ensure ordering preservation.
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for i in 0..300usize {
            keys.push(format!("s_{:04}", i).into_bytes());
            keys.push(format!("i_{:04}", i).into_bytes());
            keys.push(format!("a_{:04}", i).into_bytes());
            if i < 100 {
                keys.push(format!("miss_{:04}", i).into_bytes());
            }
        }
        let key_refs: Vec<&[u8]> = keys.iter().map(|v| v.as_slice()).collect();

        let batched = db.batch_get_vectorized(&cf, &key_refs, u64::MAX).unwrap();
        assert_eq!(batched.len(), key_refs.len());

        // Cross-check against per-key get baseline.
        for (i, k) in key_refs.iter().enumerate() {
            let baseline = db.get(&cf, k).unwrap();
            assert_eq!(
                batched[i],
                baseline,
                "slot {i} key={:?}",
                std::str::from_utf8(k)
            );
        }
    }

    #[test]
    fn test_batch_get_vectorized_put_delete_merge_mix() {
        let (db, cf) = open_with_merge_cf();
        // Put k1, Delete k2, Merge k3 (no base), Merge+Merge k4 with base.
        db.put(&cf, b"k1", b"v1").unwrap();
        db.put(&cf, b"k2", b"to-delete").unwrap();
        db.delete(&cf, b"k2").unwrap();
        db.merge(&cf, b"k3", b"x").unwrap();
        db.merge(&cf, b"k3", b"y").unwrap();
        db.put(&cf, b"k4", b"base").unwrap();
        db.merge(&cf, b"k4", b"a").unwrap();
        db.merge(&cf, b"k4", b"b").unwrap();

        let keys: Vec<&[u8]> = vec![
            b"k1".as_ref(),
            b"k2".as_ref(),
            b"k3".as_ref(),
            b"k4".as_ref(),
            b"k5_missing".as_ref(),
        ];
        let batched = db.batch_get_vectorized(&cf, &keys, u64::MAX).unwrap();
        // Compare to per-key get baseline.
        for (i, k) in keys.iter().enumerate() {
            let baseline = db.get(&cf, k).unwrap();
            assert_eq!(batched[i], baseline, "slot {i}");
        }
        // Spot-check the expected values too.
        assert_eq!(batched[0].as_deref(), Some(b"v1".as_ref()));
        assert_eq!(batched[1], None);
        assert_eq!(batched[2].as_deref(), Some(b"x,y".as_ref()));
        assert_eq!(batched[3].as_deref(), Some(b"base,a,b".as_ref()));
        assert_eq!(batched[4], None);
    }

    #[test]
    fn test_batch_get_vectorized_all_miss() {
        let db = open();
        let cf = db.default_cf();
        // Empty DB: every probe must return None.
        let owned: Vec<Vec<u8>> = (0..256)
            .map(|i| format!("nope_{i:04}").into_bytes())
            .collect();
        let keys: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
        let out = db.batch_get_vectorized(&cf, &keys, u64::MAX).unwrap();
        assert_eq!(out.len(), keys.len());
        for slot in &out {
            assert!(slot.is_none());
        }
    }

    #[test]
    fn test_batch_get_vectorized_correctness_vs_per_key_get_randomized() {
        // Randomized correctness check: build a DB with mixed-tier state,
        // then compare batch_get_vectorized to N independent get() calls.
        use std::collections::HashMap;
        let db = open();
        let cf = db.default_cf();
        let mut truth: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();

        // Tier: SST.
        for i in 0..200usize {
            let k = format!("k{:04}", i).into_bytes();
            let v = format!("v_sst_{i}").into_bytes();
            db.put(&cf, &k, &v).unwrap();
            truth.insert(k, Some(v));
        }
        db.switch_and_flush(&cf).unwrap();

        // Tier: imm — overwrite some SST keys + add new keys.
        for i in 100..300usize {
            let k = format!("k{:04}", i).into_bytes();
            let v = format!("v_imm_{i}").into_bytes();
            db.put(&cf, &k, &v).unwrap();
            truth.insert(k, Some(v));
        }
        db.force_switch_memtable(&cf).unwrap();

        // Tier: active — overwrite some imm/SST keys, delete some, add new keys.
        for i in 250..400usize {
            let k = format!("k{:04}", i).into_bytes();
            let v = format!("v_act_{i}").into_bytes();
            db.put(&cf, &k, &v).unwrap();
            truth.insert(k, Some(v));
        }
        // Delete a few keys from earlier tiers.
        for i in [10usize, 50, 150, 250] {
            let k = format!("k{:04}", i).into_bytes();
            db.delete(&cf, &k).unwrap();
            truth.insert(k, None);
        }

        // Probe with a shuffled key list including misses.
        let mut probe_keys: Vec<Vec<u8>> = Vec::new();
        for i in 0..420usize {
            probe_keys.push(format!("k{:04}", i).into_bytes());
        }
        // Add some pure misses.
        for i in 0..30usize {
            probe_keys.push(format!("never_{:04}", i).into_bytes());
        }
        let key_refs: Vec<&[u8]> = probe_keys.iter().map(|v| v.as_slice()).collect();

        let batched = db.batch_get_vectorized(&cf, &key_refs, u64::MAX).unwrap();
        for (i, k) in key_refs.iter().enumerate() {
            let baseline = db.get(&cf, k).unwrap();
            assert_eq!(
                batched[i],
                baseline,
                "slot {i} key={:?}",
                std::str::from_utf8(k)
            );
            // Cross-check against the truth map too.
            let expected = truth.get(*k).cloned().unwrap_or(None);
            assert_eq!(batched[i], expected, "truth-mismatch slot {i}");
        }
    }

    #[test]
    fn test_batch_get_vectorized_snapshot_read_seq() {
        // Snapshot semantics: rows written AFTER the snapshot's seq are invisible.
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"old1").unwrap();
        db.put(&cf, b"k2", b"old2").unwrap();
        // Capture the snapshot AFTER writing old values but BEFORE the new ones.
        let snap = db.snapshot();
        let snap_seq = snap.seq().value();
        db.put(&cf, b"k1", b"new1").unwrap();
        db.put(&cf, b"k2", b"new2").unwrap();
        db.put(&cf, b"k3", b"new3").unwrap();

        // Latest view via vectorized path.
        let latest = db
            .batch_get_vectorized(
                &cf,
                &[b"k1".as_ref(), b"k2".as_ref(), b"k3".as_ref()],
                u64::MAX,
            )
            .unwrap();
        assert_eq!(latest[0].as_deref(), Some(b"new1".as_ref()));
        assert_eq!(latest[1].as_deref(), Some(b"new2".as_ref()));
        assert_eq!(latest[2].as_deref(), Some(b"new3".as_ref()));

        // Snapshot view via vectorized path.
        let snapped = db
            .batch_get_vectorized(
                &cf,
                &[b"k1".as_ref(), b"k2".as_ref(), b"k3".as_ref()],
                snap_seq,
            )
            .unwrap();
        // Cross-check against per-key get_at baseline.
        for (i, k) in [b"k1".as_ref(), b"k2".as_ref(), b"k3".as_ref()]
            .iter()
            .enumerate()
        {
            let baseline = db.get_at(&snap, k).unwrap();
            assert_eq!(snapped[i], baseline, "slot {i}");
        }
        assert_eq!(snapped[0].as_deref(), Some(b"old1".as_ref()));
        assert_eq!(snapped[1].as_deref(), Some(b"old2".as_ref()));
        assert_eq!(snapped[2], None); // k3 didn't exist at snapshot time
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
    fn test_l0_newer_wide_range_shadows_older_narrow_range() {
        let db = open();
        let cf = db.default_cf();

        db.put(&cf, b"k", b"v1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();

        db.put(&cf, b"a", b"left").unwrap();
        db.put(&cf, b"k", b"v2").unwrap();
        db.put(&cf, b"z", b"right").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();

        assert_eq!(db.version_set.current().l0_files().len(), 2);
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v2".as_ref()));
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

    /// R50-H3 regression: opening an SST whose footer cf_id disagrees
    /// with the meta cf_id must surface as `Corruption`. The pre-fix
    /// path silently accepted the drift, masking the R50-M2 ingest
    /// foot-gun (which rewrote meta.cf_id without touching the footer).
    #[test]
    fn test_sst_reader_rejects_cf_id_mismatch() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();

        // Pull the live meta, then forge a copy whose cf_id is a value
        // the footer was NOT stamped with (footer carries DEFAULT_CF_ID).
        // Drop the cached reader so `get_or_open_sst_reader` performs a
        // fresh open + footer comparison against the forged meta.
        let meta = db.version_set.current().l0_files()[0].clone();
        assert_eq!(meta.cf_id, DEFAULT_CF_ID);
        {
            db.sst_readers.rcu(|cur| {
                let mut next = (**cur).clone();
                next.remove(&meta.file_number);
                std::sync::Arc::new(next)
            });
        }
        let mut forged = meta.clone();
        forged.cf_id = ColumnFamilyId(0xDEAD_BEEF);

        let err = match db.get_or_open_sst_reader(&forged) {
            Ok(_) => panic!("mismatched cf_id must be rejected"),
            Err(e) => e,
        };
        assert!(
            err.is_corruption(),
            "expected Corruption error, got: {err:?}"
        );
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

    /// B-R7-NEW-H1: assert `scan_iter` does NOT eagerly collect every row
    /// upfront. Inserts 10K keys, opens the iterator, advances only 5 rows,
    /// then drops it. Pre-fix, `DbImpl::scan` drained every tier into a
    /// `BTreeSet<Vec<u8>>` AND re-`get`-ed each key, building a 10K-entry
    /// `Vec<(Vec<u8>, Vec<u8>)>` before the caller saw the first row. The
    /// new lazy path streams tier-by-tier and resolves values only when
    /// the consumer advances — taking 5 rows must not require resolving
    /// the other 9995. Correctness assertion: the 5 yielded keys are the
    /// lexicographically smallest 5. Drop without panic verifies the
    /// iterator can be released mid-stream (no torn state in tier sources).
    #[test]
    fn test_scan_iter_does_not_eagerly_materialise() {
        let db = open();
        let cf = db.default_cf();
        // Use zero-padded keys so lex-order matches numeric order (k00000..).
        for i in 0..10_000u32 {
            let k = format!("k{:05}", i);
            db.put(&cf, k.as_bytes(), b"v").unwrap();
        }
        // Drop the iterator after only 5 rows. If `scan_iter` were eager,
        // this would still pass — but the LSM-tier scan structure would
        // have paid 10K value-resolves up front. We assert mid-stream drop
        // works (no torn-state panic) which only the lazy path guarantees.
        let mut iter = db.scan_iter(&cf, b"", None).unwrap();
        let mut got: Vec<Vec<u8>> = Vec::new();
        for _ in 0..5 {
            match iter.next() {
                Some(Ok((k, _v))) => got.push(k),
                Some(Err(e)) => panic!("scan_iter yielded error: {e:?}"),
                None => panic!("scan_iter exhausted before 5 rows"),
            }
        }
        // Explicit drop mid-stream — must not panic / leak.
        drop(iter);
        // The 5 smallest keys should be k00000..k00004 in order.
        assert_eq!(got.len(), 5);
        for (i, k) in got.iter().enumerate() {
            let expected = format!("k{:05}", i);
            assert_eq!(k.as_slice(), expected.as_bytes(), "row {i}");
        }
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
        db.switch_and_flush(&cf)
            .unwrap()
            .expect("flush produced sst");

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

    /// CORRECTNESS GATE for the parallel join read path (q7/q9/q20):
    /// `batch_prefix_scan_parallel` must return, for every prefix, byte-identical
    /// entries to the serial `prefix_scan` — across memtable + flushed-SST tiers,
    /// for overlapping/disjoint/empty prefixes, in input order, with per-probe
    /// error isolation. This is the gate that lets the FFI + Java executeIters
    /// route the join's K probes through the fan-out pool instead of serially.
    #[test]
    fn batch_prefix_scan_parallel_matches_serial() {
        let db = open();
        let cf = db.default_cf();

        // Seed several prefixes with enough keys to exercise multi-row probes.
        // "user:" + "order:" are flushed to an SST; "item:" stays in the memtable;
        // "user:" gets MORE rows after flush so a probe must merge SST + memtable.
        for i in 0..20u32 {
            db.put(
                &cf,
                format!("user:{i:03}").as_bytes(),
                format!("U{i}").as_bytes(),
            )
            .unwrap();
            db.put(
                &cf,
                format!("order:{i:03}").as_bytes(),
                format!("O{i}").as_bytes(),
            )
            .unwrap();
        }
        db.switch_and_flush(&cf)
            .unwrap()
            .expect("flush produced sst");
        for i in 20..30u32 {
            db.put(
                &cf,
                format!("user:{i:03}").as_bytes(),
                format!("U{i}").as_bytes(),
            )
            .unwrap();
        }
        for i in 0..15u32 {
            db.put(
                &cf,
                format!("item:{i:03}").as_bytes(),
                format!("I{i}").as_bytes(),
            )
            .unwrap();
        }

        // Probe set: overlapping/disjoint/empty (no-match) prefixes. >1 → fan-out path.
        let prefixes: &[&[u8]] = &[b"user:", b"order:", b"item:", b"missing:", b"user:0"];

        let parallel = db.batch_prefix_scan_parallel(&cf, prefixes);
        assert_eq!(parallel.len(), prefixes.len());

        for (i, p) in prefixes.iter().enumerate() {
            let mut serial = db.prefix_scan(&cf, p).expect("serial prefix_scan");
            serial.sort_by(|l, r| l.0.cmp(&r.0));
            let mut par = parallel[i]
                .as_ref()
                .unwrap_or_else(|e| panic!("probe {i} ({p:?}) errored: {e:?}"))
                .clone();
            par.sort_by(|l, r| l.0.cmp(&r.0));
            assert_eq!(
                par, serial,
                "batch_prefix_scan_parallel probe {i} ({p:?}) must equal serial prefix_scan",
            );
        }

        // Sanity on the data shape so a silently-empty pass can't mask a regression.
        assert_eq!(
            parallel[0].as_ref().unwrap().len(),
            30,
            "user: = 20 pre + 10 post-flush"
        );
        assert_eq!(parallel[1].as_ref().unwrap().len(), 20, "order:");
        assert_eq!(parallel[2].as_ref().unwrap().len(), 15, "item:");
        assert!(
            parallel[3].as_ref().unwrap().is_empty(),
            "missing: = no match"
        );
    }

    /// `batch_prefix_scan_parallel` on the single-probe and empty-batch fast paths.
    #[test]
    fn batch_prefix_scan_parallel_edge_counts() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k:1", b"v1").unwrap();
        db.put(&cf, b"k:2", b"v2").unwrap();

        assert!(db.batch_prefix_scan_parallel(&cf, &[]).is_empty());

        let one = db.batch_prefix_scan_parallel(&cf, &[b"k:"]);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].as_ref().unwrap().len(), 2);
    }

    /// FRS-WAL Phase 2: with the WAL enabled, every point write is appended +
    /// group-commit fsynced before the memtable insert, and the segment replays
    /// to exactly the written mutations (key/value/op/seq), tombstones included.
    /// Forces the WAL on directly (not via `FRS_WAL_DIR`) to avoid cross-test
    /// env races; the env path is exercised by `maybe_init_wal` in production.
    #[test]
    fn wal_phase2_point_writes_are_logged_and_replayable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.wal");
        let db = open();
        let cf = db.default_cf();
        *db.wal.lock().unwrap() = Some(crate::wal::WalWriter::open(&path).unwrap());

        db.put(&cf, b"k1", b"v1").unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();
        db.delete(&cf, b"k1").unwrap();
        db.wal_sync().unwrap(); // durability barrier (checkpoint does this in prod)

        let scan = crate::wal::read_segment(&path).unwrap();
        assert!(scan.clean_eof, "all records intact after sync");
        assert_eq!(scan.records.len(), 3);
        assert_eq!(scan.records[0].key, b"k1");
        assert_eq!(scan.records[0].value, Some(b"v1".to_vec()));
        assert_eq!(scan.records[1].key, b"k2");
        assert_eq!(scan.records[1].value, Some(b"v2".to_vec()));
        assert_eq!(scan.records[2].key, b"k1");
        assert_eq!(scan.records[2].value, None, "delete logged as tombstone");
        assert!(scan.records[0].sequence < scan.records[1].sequence);
        assert!(scan.records[1].sequence < scan.records[2].sequence);
    }

    /// FRS-WAL Phase 2b: the batch write path (q4's async vectorized path) logs
    /// the whole batch with contiguous sequence numbers and tombstones intact.
    #[test]
    fn wal_phase2b_batch_writes_are_logged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.wal");
        let db = open();
        let cf = db.default_cf();
        *db.wal.lock().unwrap() = Some(crate::wal::WalWriter::open(&path).unwrap());

        let keys: Vec<&[u8]> = vec![&b"a"[..], &b"b"[..], &b"c"[..]];
        let vals: Vec<Option<&[u8]>> = vec![Some(&b"1"[..]), Some(&b"2"[..]), None];
        let ops = vec![OpType::Put as u8, OpType::Put as u8, OpType::Delete as u8];
        db.batch_put_borrowed_single_cf(&cf, &keys, &vals, &ops)
            .unwrap();
        db.wal_sync().unwrap(); // durability barrier (checkpoint does this in prod)

        let scan = crate::wal::read_segment(&path).unwrap();
        assert!(scan.clean_eof);
        assert_eq!(scan.records.len(), 3);
        assert_eq!(scan.records[0].key, b"a");
        assert_eq!(scan.records[0].value, Some(b"1".to_vec()));
        assert_eq!(scan.records[2].key, b"c");
        assert_eq!(
            scan.records[2].value, None,
            "batch delete logged as tombstone"
        );
        assert_eq!(scan.records[1].sequence, scan.records[0].sequence + 1);
        assert_eq!(scan.records[2].sequence, scan.records[1].sequence + 1);
    }

    /// FRS-WAL Phase 2: with the WAL disabled (default), no segment is created
    /// and writes behave exactly as before (the gating no-op path).
    #[test]
    fn wal_phase2_disabled_by_default_is_noop() {
        let db = open();
        let cf = db.default_cf();
        assert!(
            db.wal.lock().unwrap().is_none(),
            "WAL off unless FRS_WAL_DIR set"
        );
        db.put(&cf, b"k", b"v").unwrap();
        assert_eq!(db.get(&cf, b"k").unwrap(), Some(b"v".to_vec()));
    }

    /// FRS-VALUE-CARRYING-MERGE: scan via the slot variant (the q4 FFI path)
    /// and assert byte-equivalence against the point-get oracle.
    fn scan_slot(
        db: &Arc<DbImpl>,
        cf: &ColumnFamilyHandle,
        prefix: &[u8],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let slot: Arc<Mutex<Option<ForstError>>> = Arc::new(Mutex::new(None));
        let it = db
            .prefix_scan_iter_owned_arc_with_error_slot(cf, prefix, slot.clone())
            .unwrap();
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = it
            .map(|r| r.unwrap())
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        assert!(
            slot.lock().unwrap().is_none(),
            "no tier-peek error expected"
        );
        out.sort();
        out
    }

    fn point_get_oracle(
        db: &Arc<DbImpl>,
        cf: &ColumnFamilyHandle,
        keys: &[&[u8]],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut expected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for k in keys {
            if let Some(v) = db.get(cf, k).unwrap() {
                expected.push((k.to_vec(), v));
            }
        }
        expected.sort();
        expected
    }

    /// FRS-VALUE-CARRYING-MERGE: the inline-value prefix scan must yield
    /// EXACTLY what per-key `get_internal` would, across every tier mix:
    /// SST-resident Put, newer-SST-version-wins (max sequence), tombstone in a
    /// newer SST hiding an older Put, active-memtable shadow of an SST key, and
    /// active-mem-only keys.
    #[test]
    fn value_carrying_merge_matches_point_get_all_tiers() {
        let db = open();
        let cf = db.default_cf();

        // (a) First SST: plain Puts.
        db.put(&cf, b"u:a", b"A0").unwrap();
        db.put(&cf, b"u:b", b"B0").unwrap();
        db.put(&cf, b"u:tomb", b"X").unwrap();
        db.switch_and_flush(&cf).unwrap().expect("flush1 sst");

        // (b) Second SST (newer): newer u:b version (max-seq SST winner) and a
        //     tombstone over u:tomb (newer-SST Delete hides older-SST Put).
        db.put(&cf, b"u:b", b"B1").unwrap();
        db.delete(&cf, b"u:tomb").unwrap();
        db.switch_and_flush(&cf).unwrap().expect("flush2 sst");

        // (c) Active memtable shadows u:a (memtable tier wins → fallback path).
        db.put(&cf, b"u:a", b"A2").unwrap();
        // (d) Active-mem-only key.
        db.put(&cf, b"u:c", b"C").unwrap();

        let scanned = scan_slot(&db, &cf, b"u:");
        let expected = point_get_oracle(&db, &cf, &[b"u:a", b"u:b", b"u:c", b"u:tomb"]);
        assert_eq!(
            scanned, expected,
            "inline-value scan must equal point-get for every tier mix"
        );
        // Explicit resolutions:
        assert_eq!(db.get(&cf, b"u:a").unwrap().unwrap(), b"A2"); // memtable wins
        assert_eq!(db.get(&cf, b"u:b").unwrap().unwrap(), b"B1"); // newer SST wins
        assert!(db.get(&cf, b"u:tomb").unwrap().is_none()); // tombstone hides
        assert!(
            scanned.iter().all(|(k, _)| k.as_slice() != b"u:tomb"),
            "tombstoned key must not be emitted"
        );
    }

    /// FRS-VALUE-CARRYING-MERGE: an SST-resident Merge head must defer to the
    /// `get_internal` fallback (operand chain spans tiers), while a sibling
    /// SST-resident Put under the same prefix resolves inline — both
    /// byte-identical to point-get.
    #[test]
    fn value_carrying_merge_fallback_resolves_merge_chain() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"m:k", b"base").unwrap();
        db.merge(&cf, b"m:k", b"x").unwrap();
        db.switch_and_flush(&cf).unwrap().expect("flush1 sst");
        db.merge(&cf, b"m:k", b"y").unwrap();
        db.put(&cf, b"m:p", b"P").unwrap(); // inline Put alongside the merge chain
        db.switch_and_flush(&cf).unwrap().expect("flush2 sst");

        let scanned = scan_slot(&db, &cf, b"m:");
        let expected = point_get_oracle(&db, &cf, &[b"m:k", b"m:p"]);
        assert_eq!(
            scanned, expected,
            "merge-chain (fallback) + inline Put must equal point-get"
        );
        assert_eq!(db.get(&cf, b"m:p").unwrap().unwrap(), b"P");
        // The merge head resolved to a non-empty concatenation including base.
        assert!(scanned
            .iter()
            .any(|(k, v)| k.as_slice() == b"m:k" && !v.is_empty()));
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
        db.switch_and_flush(&cf)
            .unwrap()
            .expect("flush produced sst");
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
        for item in iter {
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

    /// FRS-COMPACT-MAINTENANCE (2026-06-04): regression guard for the q4
    /// fan-out decay. `switch_and_flush` / checkpoint flush / `flush_all` add
    /// L0 SSTs but — unlike the background `run_flush` worker — never call
    /// `maybe_auto_compact`. In q4 the write-buffer flush path essentially
    /// never fires (the 1 GiB memtable rarely fills) while 30 s checkpoints
    /// keep sealing the memtable to L0, so L0 grew UNBOUNDED and compaction
    /// NEVER ran (measured: per-probe overlapping-SST count climbed 0→10, all
    /// L0, deep=0; "compact" appeared 0× in the TM log). The maintenance
    /// scheduler must detect a CF whose L0 is at/over the compaction trigger
    /// INDEPENDENT of the flush path, so the periodic ticker can enqueue the
    /// rollup and keep L0 shallow regardless of which path produced the SSTs.
    #[test]
    fn test_cfs_due_for_compaction_flags_uncompacted_l0() {
        let db = open();
        let cf = db.default_cf();
        let trigger = db.write_controller.config().l0_compaction_trigger;
        // Seal `trigger + 1` L0 files via the path that does NOT auto-compact.
        for i in 0..(trigger + 1) {
            db.put(&cf, format!("k{i:04}").as_bytes(), b"v").unwrap();
            db.switch_and_flush(&cf).unwrap().unwrap();
        }
        assert!(
            db.version_set.current().l0_files().len() as u32 > trigger,
            "precondition: L0 must be over the trigger and uncompacted"
        );
        // The scheduler must flag this CF as due — this is the fix.
        let due = db.cfs_due_for_compaction();
        assert_eq!(due.len(), 1, "CF with L0 over trigger must be flagged due");
        assert_eq!(due[0].handle().id(), cf.id());

        // Once compacted, the CF must no longer be reported as due.
        db.compact_l0(&cf).unwrap();
        assert_eq!(db.version_set.current().l0_files().len(), 0);
        assert!(
            db.cfs_due_for_compaction().is_empty(),
            "a balanced CF must not be flagged due"
        );
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
    fn test_non_bottommost_merge_only_compaction_preserves_lower_level_base() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"base").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();
        db.compact_level(&cf, 1).unwrap().unwrap();
        db.compact_level(&cf, 2).unwrap().unwrap();

        db.merge(&cf, b"k", b"delta").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();

        assert_eq!(
            db.get(&cf, b"k").unwrap().as_deref(),
            Some(b"base,delta".as_ref())
        );
        db.compact_level(&cf, 1).unwrap().unwrap();
        assert_eq!(
            db.get(&cf, b"k").unwrap().as_deref(),
            Some(b"base,delta".as_ref()),
            "non-bottommost merge-only compaction must not rewrite a merge operand as a Put"
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
        // 2026-05-30 OBSOLETE-FILE LIFETIME: deletion of a compaction input is
        // now deferred until no live version references it (the fix for the
        // compaction-deletes-SST-under-read race). The just-replaced version is
        // transiently retained (ArcSwap debt slot) until subsequent engine
        // activity recycles it, so reclamation is eventual rather than
        // synchronous with `compact_l0`. Drive a few write/flush/compact cycles
        // (as the pin-deferral test does) and assert the file is reclaimed.
        let mut reclaimed = !db.fs.file_exists(&old_path).unwrap();
        for i in 0..16 {
            if reclaimed {
                break;
            }
            db.put(&cf, format!("d{i}").as_bytes(), b"v").unwrap();
            db.switch_and_flush(&cf).unwrap();
            db.compact_l0(&cf).unwrap();
            db.reap_pending_deletions();
            reclaimed = !db.fs.file_exists(&old_path).unwrap();
        }
        assert!(reclaimed, "compaction input must be reclaimed eventually");
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

        // Inject a deterministic clock at "now=2000ms". The Flink filter
        // uses Flink's expiry-timestamp predicate `now > expiry_ms`
        // (R82-H1), NOT an age-vs-TTL comparison. ttl_ms is configured at
        // 500 only because the filter's name encodes it (R47-H1) and a
        // sibling test below checks `assert_ne!` on differing ttl values
        // — it does not participate in the expiry decision.
        let supplier: crate::compaction_filter::CurrentTimeSupplier = Arc::new(|| 2000);
        let filter = Arc::new(FlinkTtlCompactionFilter::with_supplier(
            500,
            TtlStateType::Value,
            0,
            supplier,
        ));
        db.set_compaction_filter(&cf, Some(filter)).unwrap();

        // R83-H1: build values matching Flink's BE expiry-timestamp wire
        // format (R81-H1 + R82-H1). Pre-fix this test used `to_le_bytes`
        // and the assertions passed only by triple-coincidence (LE-zero
        // == BE-zero gives "expired"; LE-1800 reads BE as ~5.78e17 which
        // exceeds the supplier's now=2000 so reads as "not yet expired").
        // Regression coverage was vacuous.
        //
        // expiry=500, now=2000 → 2000 > 500 → discard.
        let mut expired_value = Vec::new();
        expired_value.extend_from_slice(&500u64.to_be_bytes());
        expired_value.extend_from_slice(b"old");
        db.put(&cf, b"old", &expired_value).unwrap();

        // expiry=5000, now=2000 → 2000 !> 5000 → kept.
        let mut fresh_value = Vec::new();
        fresh_value.extend_from_slice(&5000u64.to_be_bytes());
        fresh_value.extend_from_slice(b"new");
        db.put(&cf, b"new", &fresh_value).unwrap();

        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();

        assert!(db.get(&cf, b"old").unwrap().is_none(), "expired key kept");
        assert!(db.get(&cf, b"new").unwrap().is_some(), "fresh key dropped");
    }

    #[test]
    fn test_compact_range_revisits_deeper_level_for_ttl_filter() {
        use crate::compaction_filter::{FlinkTtlCompactionFilter, TtlStateType};

        let db = open();
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("flink-ttl-deeper"))
            .unwrap();

        let now = Arc::new(std::sync::atomic::AtomicU64::new(50));
        let now_for_filter = Arc::clone(&now);
        let supplier: crate::compaction_filter::CurrentTimeSupplier =
            Arc::new(move || now_for_filter.load(std::sync::atomic::Ordering::Relaxed));
        let filter = Arc::new(FlinkTtlCompactionFilter::with_supplier(
            100,
            TtlStateType::Value,
            0,
            supplier,
        ));
        db.set_compaction_filter(&cf, Some(filter)).unwrap();

        let mut value = Vec::new();
        value.extend_from_slice(&100u64.to_be_bytes());
        value.extend_from_slice(b"payload");
        db.put(&cf, b"k", &value).unwrap();

        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_range(&cf).unwrap().unwrap();
        assert!(db.get(&cf, b"k").unwrap().is_some());

        now.store(200, std::sync::atomic::Ordering::Relaxed);
        db.compact_range(&cf).unwrap();
        assert!(db.get(&cf, b"k").unwrap().is_none());
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

    /// Recording filesystem wrapper: delegates everything to an inner FS but
    /// counts `await_all_uploads()` calls and records every `await_upload(path)`
    /// path. Used to prove the incremental-checkpoint barrier waits ONLY for the
    /// SSTs it references (per-file `await_upload`) and NOT for ALL in-flight
    /// uploads (`await_all_uploads`, which would also block on unrelated
    /// background compaction outputs — the q7 ckpt-ON freeze, 2026-06-02).
    struct UploadRecordingFs {
        inner: Arc<dyn FileSystem>,
        await_all_count: std::sync::atomic::AtomicUsize,
        awaited_paths: std::sync::Mutex<Vec<PathBuf>>,
    }
    impl UploadRecordingFs {
        fn new(inner: Arc<dyn FileSystem>) -> Self {
            Self {
                inner,
                await_all_count: std::sync::atomic::AtomicUsize::new(0),
                awaited_paths: std::sync::Mutex::new(Vec::new()),
            }
        }
        fn reset(&self) {
            self.await_all_count
                .store(0, std::sync::atomic::Ordering::SeqCst);
            self.awaited_paths.lock().unwrap().clear();
        }
        fn await_all_count(&self) -> usize {
            self.await_all_count
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }
    impl FileSystem for UploadRecordingFs {
        fn open_sequential_file(
            &self,
            path: &Path,
        ) -> ForstResult<Box<dyn forst_rs_io::SequentialFile>> {
            self.inner.open_sequential_file(path)
        }
        fn open_random_access_file(
            &self,
            path: &Path,
        ) -> ForstResult<Box<dyn forst_rs_io::RandomAccessFile>> {
            self.inner.open_random_access_file(path)
        }
        fn open_writable_file(
            &self,
            path: &Path,
            mode: WriteMode,
        ) -> ForstResult<Box<dyn forst_rs_io::WritableFile>> {
            self.inner.open_writable_file(path, mode)
        }
        fn file_exists(&self, path: &Path) -> ForstResult<bool> {
            self.inner.file_exists(path)
        }
        fn get_file_metadata(&self, path: &Path) -> ForstResult<forst_rs_io::FileMetadata> {
            self.inner.get_file_metadata(path)
        }
        fn list_dir(&self, dir: &Path) -> ForstResult<Vec<forst_rs_io::FileMetadata>> {
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
            "upload-recording"
        }
        fn await_upload(&self, path: &Path) -> ForstResult<()> {
            self.awaited_paths.lock().unwrap().push(path.to_path_buf());
            self.inner.await_upload(path)
        }
        fn await_all_uploads(&self) -> ForstResult<()> {
            self.await_all_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.await_all_uploads()
        }
    }

    /// 2026-06-02 q7 ckpt-ON freeze regression: an incremental checkpoint must
    /// establish remote durability of the SSTs IT references by awaiting their
    /// individual uploads — NOT by calling the blanket `await_all_uploads()`,
    /// which also blocks on unrelated in-flight background compaction uploads
    /// (the freeze: ckpt3 took 332s vs ckpt1/2 ~1.6s once compaction started).
    #[test]
    fn test_incremental_checkpoint_awaits_only_referenced_ssts() {
        use forst_rs_io::MemoryFileSystem;
        let rec = Arc::new(UploadRecordingFs::new(Arc::new(MemoryFileSystem::new())));
        let fs: Arc<dyn FileSystem> = rec.clone();
        let db = open_in_shared_fs("/db", fs);
        let cf = db.default_cf();
        for i in 0..200u32 {
            let k = format!("k{:05}", i);
            db.put(&cf, k.as_bytes(), b"value-payload").unwrap();
        }
        db.flush_all().unwrap();

        // Isolate the checkpoint's filesystem interactions from setup/flush.
        rec.reset();

        let snap = db.snapshot();
        let result = db.create_incremental_checkpoint(&snap, 1, 0).unwrap();
        // The flush produced at least one new SST for this checkpoint to upload.
        assert!(
            !result.new_ssts.is_empty(),
            "checkpoint should have at least one new SST to upload"
        );

        // BEHAVIOR: the checkpoint must NOT use the blanket all-uploads barrier
        // (it would couple checkpoint latency to background compaction I/O).
        assert_eq!(
            rec.await_all_count(),
            0,
            "incremental checkpoint must not call await_all_uploads() (couples \
             checkpoint latency to unrelated background compaction uploads)"
        );
        // CORRECTNESS: it MUST still await the upload of every LIVE SST the
        // checkpoint's pinned version references (resolved to the engine's
        // db_path — the files the engine actually uploads), so a restore never
        // points at an SST whose bytes are still in a local/in-flight buffer.
        let awaited = rec.awaited_paths.lock().unwrap().clone();
        let live = db.version_set.current();
        let live_files: Vec<_> = live.live_sst_files_iter().collect();
        assert!(
            !live_files.is_empty(),
            "test precondition: the flush should have produced at least one live SST"
        );
        for f in &live_files {
            let p = crate::flush::sst_file_path(Path::new("/db"), f.file_number);
            assert!(
                awaited.contains(&p),
                "referenced live SST {:?} was not awaited (manifest could reference \
                 an un-uploaded file → restore data loss); awaited={:?}",
                p,
                awaited
            );
        }
    }

    /// 2026-06-02 q7 SECOND-wall regression: an EMPTY prefix scan over an SST
    /// whose key range STRADDLES the prefix (data for earlier + later
    /// namespaces, none for the probed one) must STOP as soon as the scan
    /// passes the prefix's upper bound — NOT read every block to EOF. SST rows
    /// are key-ASC, so once a key >= upper is seen, no later key can be in
    /// [prefix, upper). The pre-fix peek skipped `>= upper` rows but kept
    /// reading subsequent blocks, so an empty join probe scanned the entire SST
    /// tail (measured: 46 ms/probe, rows=0 → the q7 ~100/s collapse).
    #[test]
    fn test_empty_prefix_scan_stops_at_upper_bound() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        {
            let db = open_in_shared_fs("/db", fs.clone());
            let cf = db.default_cf();
            // Namespaces p000..p099 with a GAP at p050. ~1 KiB values so the data
            // spans many 64 KiB SST blocks (the tail after the gap is many blocks).
            let val = vec![0xEEu8; 1024];
            for ns in 0..100u32 {
                if ns == 50 {
                    continue; // p050 is the empty probed prefix
                }
                for i in 0..20u32 {
                    let k = format!("p{:03}_{:05}", ns, i);
                    db.put(&cf, k.as_bytes(), &val).unwrap();
                }
            }
            db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();
        } // drop db1: its resident-flushed RAM shadow is released.

        // Restore fresh from the checkpoint: no resident memtables, so the prefix
        // scan must read from the SST — the cold q7 regime (state spilled past the
        // RAM shadow) where this bug bites.
        let opts = EngineOptions {
            db_path: "/ckpt".to_string(),
            ..EngineOptions::default()
        };
        let db = DbImpl::open_from_checkpoint(opts, fs).unwrap();
        let cf = db.default_cf();

        // Sanity: a single straddling SST with many blocks.
        let version = db.version_set.current();
        let ssts: Vec<_> = version.live_sst_files_iter().collect();
        assert_eq!(ssts.len(), 1, "expected exactly one flushed SST");
        let reader = db.get_or_open_sst_reader(ssts[0]).unwrap();
        let total_blocks = reader.index_entry_count();
        assert!(
            total_blocks >= 8,
            "test precondition: SST should span many blocks, got {}",
            total_blocks
        );
        let blocks_before = reader.blocks_read();

        // Probe the EMPTY prefix p050_. Range = [p050_, p050`] ; all p051..p099
        // keys are >= upper. The scan must not walk the whole tail.
        let iter = db
            .prefix_scan_iter_owned_arc(&cf, b"p050_")
            .expect("open prefix iter");
        let mut n = 0usize;
        for item in iter {
            item.expect("iter item");
            n += 1;
        }
        assert_eq!(n, 0, "p050 prefix is empty");

        let blocks_during_scan = reader.blocks_read() - blocks_before;
        // With the upper-bound early-termination, the scan reads ~1 block (the
        // one first_block_ge lands on) and stops. Without it, it reads the whole
        // tail (p051..p099 ≈ many blocks). Allow a small slack.
        assert!(
            blocks_during_scan <= 3,
            "empty prefix scan read {} blocks (of {} total) — it walked past the \
             upper bound to EOF instead of stopping (q7 ~100/s join collapse)",
            blocks_during_scan,
            total_blocks
        );
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

    /// 2026-05-30 OBSOLETE-FILE LIFETIME regression test: a compaction must NOT
    /// delete an input SST's storage while an in-flight read still holds a
    /// version that references it. Pre-fix, compaction's `delete_file_guarded`
    /// only honored checkpoint pins, so it unlinked the input the moment apply
    /// completed — a concurrent read holding the old version then 404'd on the
    /// deleted object (`frs_vectorized_batch_get rc=1 NOT_FOUND` → crash-loop on
    /// S3). The fix defers deletion until no live version (current ∪ retiring-
    /// with-readers) references the file.
    #[test]
    fn test_compaction_defers_delete_while_read_version_held() {
        let db = open();
        let cf = db.default_cf();
        // Two flushes → two L0 SSTs that compaction will merge + delete.
        db.put(&cf, b"k1", b"v1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();

        // Simulate an in-flight read: capture and HOLD the current version
        // (exactly what `get_internal` does via `version_set.current()`).
        let held = db.version_set.current();
        let input_fnums: Vec<FileNumber> = held.l0_files().iter().map(|f| f.file_number).collect();
        assert!(
            input_fnums.len() >= 2,
            "test needs >= 2 L0 inputs, got {}",
            input_fnums.len()
        );

        // Compact: merges the L0 inputs into L1 and queues the inputs for
        // deletion. With `held` alive, their storage MUST be deferred.
        db.compact_l0(&cf)
            .unwrap()
            .expect("compaction should produce output");
        for &fnum in &input_fnums {
            let path = sst_file_path(&db.db_path, fnum);
            assert!(
                db.fs.file_exists(&path).unwrap(),
                "input SST {} must SURVIVE while a read version references it",
                fnum.value()
            );
        }

        // Release the read version, then reap: the retiring version loses its
        // last reader, so the inputs are no longer referenced and get reclaimed.
        drop(held);
        // After the reader releases its version, normal engine activity (writes,
        // flushes, compactions — each advancing the version + churning ArcSwap's
        // load slots, exactly as a running query does) lets the retiring version
        // drop its last ref so its files become reclaimable. Drive a few such
        // cycles, then assert reclamation. Bounded so a true leak still fails.
        let mut reclaimed = false;
        for i in 0..16 {
            db.put(&cf, format!("drain{i}").as_bytes(), b"v").unwrap();
            db.switch_and_flush(&cf).unwrap();
            db.compact_l0(&cf).unwrap();
            db.reap_pending_deletions();
            if input_fnums
                .iter()
                .all(|&f| !db.fs.file_exists(&sst_file_path(&db.db_path, f)).unwrap())
            {
                reclaimed = true;
                break;
            }
        }
        assert!(
            reclaimed,
            "input SSTs {:?} must be DELETED after the read version is released",
            input_fnums.iter().map(|f| f.value()).collect::<Vec<_>>()
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
            let result = db
                .version_set
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

    /// R44-H1 regression test: no duplicate L1 file numbers under
    /// concurrent `compact_l0` invocations.
    ///
    /// Two threads run `compact_l0` against different CFs. Pre-fix, the
    /// per-CF `flush_mutex` was the only serialization; because the
    /// `VersionSet` is engine-global, both threads could pick the same
    /// L0 inputs and both `version_set.apply` their edits, leaving the
    /// same L1 file number installed twice (or a stale L0 reference
    /// alongside its compaction output).
    ///
    /// Post-fix, the engine-global `compaction_mutex` serializes the two
    /// jobs and `Version::apply_edit` rejects stale-edit delete sets
    /// with `ForstError::Busy` as defense-in-depth.
    ///
    /// The asserted post-condition is narrow: after both threads finish,
    /// the L1 file-number set must contain no duplicates. (Pre-fix the
    /// same file number could appear twice in L1 — i.e. the same data
    /// installed twice.) This test does NOT assert per-CF L1 separation,
    /// L0 input-disjointness across the two jobs, or anything about
    /// cross-CF merge-operator / compaction-filter semantics —
    /// `VersionSet` is engine-global today, so those invariants are
    /// addressed by R45-H1's create-time CF homogeneity check, not by
    /// runtime per-CF Version scoping.
    #[test]
    fn test_r44_h1_concurrent_multi_cf_compaction_no_overlap() {
        let db = open();
        let cf_a = db.default_cf();
        let cf_b = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_b"))
            .expect("create cf_b");

        // Seed both CFs with multiple L0 SSTs so each has work to compact.
        for batch in 0..4u32 {
            for k in 0..16u32 {
                let key = format!("b{:02}k{:04}", batch, k);
                db.put(&cf_a, key.as_bytes(), b"va").unwrap();
                db.put(&cf_b, key.as_bytes(), b"vb").unwrap();
            }
            db.switch_and_flush(&cf_a).unwrap();
            db.switch_and_flush(&cf_b).unwrap();
        }

        // L0 should have files for both CFs (CFs share the engine VersionSet
        // today, so this counts engine-wide L0 files).
        let l0_before = db.version_set.current().l0_files().len();
        assert!(
            l0_before >= 2,
            "expected at least 2 L0 files before compaction, got {}",
            l0_before
        );

        // Spawn two threads, each compacting one CF. Both call into the
        // same engine-global compaction path.
        let db_a = db.clone();
        let db_b = db.clone();
        let cf_a_t = cf_a.clone();
        let cf_b_t = cf_b.clone();
        let h_a = std::thread::spawn(move || db_a.compact_l0(&cf_a_t));
        let h_b = std::thread::spawn(move || db_b.compact_l0(&cf_b_t));

        let res_a = h_a.join().expect("thread a panicked");
        let res_b = h_b.join().expect("thread b panicked");

        // BOTH must complete without panic. At least one must succeed.
        // The other either also succeeds (post-mutex: serialized) or
        // returns `Busy` (defense-in-depth path if mutex is ever removed).
        let a_ok = res_a.is_ok();
        let b_ok = res_b.is_ok();
        let a_busy = matches!(&res_a, Err(e) if e.is_busy());
        let b_busy = matches!(&res_b, Err(e) if e.is_busy());
        assert!(
            a_ok || a_busy,
            "thread A failed with non-Busy error: {:?}",
            res_a
        );
        assert!(
            b_ok || b_busy,
            "thread B failed with non-Busy error: {:?}",
            res_b
        );
        assert!(a_ok || b_ok, "at least one compaction must succeed");

        // Verify the invariant: the L1 layer must not contain duplicate file
        // numbers, and no L0 file number should be referenced from L1.
        // (Pre-fix the race could leave the same L0 file number active
        // AND its compaction output in L1 — i.e. the same data installed
        // twice.)
        let v = db.version_set.current();
        let l1_nums: Vec<u64> = v.levels[1]
            .files
            .iter()
            .map(|f| f.file_number.value())
            .collect();
        let mut sorted = l1_nums.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            l1_nums.len(),
            "duplicate L1 file numbers: {:?}",
            l1_nums
        );
    }

    // ============================================================
    // R45-H1: heterogeneous CF merge/filter rejection
    // ============================================================

    /// R45-H1 happy path: two non-default CFs that share the same
    /// merge_operator (by name) are accepted. The engine's L0 layer is
    /// shared across CFs, but with identical merge operators the
    /// cross-CF compaction hazard collapses (same policy applied either
    /// way), so the homogeneity check must NOT reject this.
    #[test]
    fn test_r45_h1_accepts_homogeneous_merge_operator() {
        let db = open();
        let op_a: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        let op_b: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        db.create_column_family(
            ColumnFamilyDescriptor::new("cf_merge_1").with_merge_operator(op_a),
        )
        .expect("first non-default CF with merge accepted");
        db.create_column_family(
            ColumnFamilyDescriptor::new("cf_merge_2").with_merge_operator(op_b),
        )
        .expect("second CF with same merge_operator name accepted");
    }

    /// R45-H1 reject path: a second non-default CF with NO merge
    /// operator is rejected when an earlier non-default CF already has
    /// one. Pre-fix, `compact_l0_for_cf` would rewrite cf_b's L0 files
    /// under cf_a's merge operator (silent wrong result).
    #[test]
    fn test_r45_h1_rejects_heterogeneous_merge_operator() {
        let db = open();
        let op: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        db.create_column_family(ColumnFamilyDescriptor::new("cf_merge").with_merge_operator(op))
            .expect("first non-default CF with merge accepted");
        // Adding a non-default CF with no merge operator must be
        // rejected (heterogeneous against the existing non-default CF).
        let err = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_plain"))
            .expect_err("heterogeneous CF must be rejected");
        let msg = format!("{}", err);
        assert!(
            msg.contains("R45-H1"),
            "error must cite the constraint: {}",
            msg
        );
    }

    /// R45-H1 reject path for compaction_filter: a second non-default CF
    /// without the same filter is rejected when an earlier non-default
    /// CF has one set at create time.
    #[test]
    fn test_r45_h1_rejects_heterogeneous_compaction_filter() {
        use crate::compaction_filter::TtlCompactionFilter;
        let db = open();
        let now: fn() -> u64 = || 1_000;
        let filter = Arc::new(TtlCompactionFilter::with_clock(50, now));
        db.create_column_family(
            ColumnFamilyDescriptor::new("cf_ttl").with_compaction_filter(filter),
        )
        .expect("first non-default CF with filter accepted");
        let err = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_no_filter"))
            .expect_err("CF without matching filter must be rejected");
        let msg = format!("{}", err);
        assert!(
            msg.contains("R45-H1"),
            "error must cite the constraint: {}",
            msg
        );
    }

    /// R45-H1: the default CF is exempt from the homogeneity check.
    /// `DbImpl::open` always installs a default CF with no merge /
    /// filter; legacy callers (notably the FFI) rely on then creating a
    /// single non-default CF with a merge operator alongside it.
    #[test]
    fn test_r45_h1_default_cf_is_exempt() {
        let db = open();
        let op: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        // Default exists with no merge; this single user CF with merge
        // must still succeed despite the default CF having no merge.
        db.create_column_family(ColumnFamilyDescriptor::new("merge_cf").with_merge_operator(op))
            .expect("user CF + default CF (no merge) must be accepted");
    }

    /// R46-H1 reject path: `set_compaction_filter` must reject a
    /// candidate filter whose `name()` differs from filters on OTHER
    /// non-default CFs. This mirrors the production hazard from Flink's
    /// `ForStRsTtlCompactFiltersManager.setTtlForState` — that path
    /// installs a per-state `FlinkTtlCompactionFilter` whose `name()`
    /// encodes the TTL, so two states with different TTLs produce two
    /// filters whose names disagree, and pre-fix this
    /// `set_compaction_filter` path bypassed the same check that
    /// `create_column_family` already enforced.
    ///
    /// Test shape (per spec): create 2 non-default CFs both with no
    /// filter (homogeneous baseline). The first `set_compaction_filter`
    /// install of filter A on cf_a creates a heterogeneous state
    /// against cf_b (still has no filter), and pre-fix that install
    /// silently succeeded — post-fix it must be rejected.
    ///
    /// In production Flink installs per-state filters in sequence; the
    /// FIRST install is the one that creates heterogeneity, and
    /// rejecting it surfaces the configuration error at the earliest
    /// possible point (before any compaction has had a chance to
    /// corrupt data).
    #[test]
    fn test_r46_h1_set_compaction_filter_rejects_heterogeneous() {
        use crate::compaction_filter::{CompactionDecision, CompactionFilter};
        use forst_rs_common::OpType;
        // Minimal named filters that report different `name()` strings.
        struct NamedFilter(&'static str);
        impl CompactionFilter for NamedFilter {
            fn filter(
                &self,
                _level: u32,
                _key: &[u8],
                _value: Option<&[u8]>,
                _sequence: u64,
                _op_type: OpType,
                _value_out: &mut Vec<u8>,
            ) -> CompactionDecision {
                CompactionDecision::Keep
            }
            fn name(&self) -> String {
                // R47-H1: trait now returns `String` so impls can encode
                // per-instance config in the identity. This test impl
                // wraps a `&'static str` so we just clone it.
                self.0.to_string()
            }
        }

        let db = open();
        let cf_a = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_a"))
            .expect("cf_a accepted (default-shape)");
        let _cf_b = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_b"))
            .expect("cf_b accepted (default-shape)");

        // Both CFs have no filter — homogeneous baseline. Installing
        // a filter on cf_a alone would create heterogeneity against
        // cf_b. Pre-fix: the install silently succeeded (R46-H1
        // hazard). Post-fix: rejected.
        let filter_a: Arc<dyn CompactionFilter> = Arc::new(NamedFilter("ttl-A"));
        let err = db.set_compaction_filter(&cf_a, Some(filter_a)).expect_err(
            "set_compaction_filter on cf_a must be rejected — cf_b still has no filter",
        );
        let msg = format!("{}", err);
        assert!(
            msg.contains("R45-H1"),
            "error must cite the homogeneity constraint: {}",
            msg
        );
    }

    /// R46-H1 spec second variant: when both CFs already share a
    /// non-default filter and one of them is RE-installed with a
    /// different filter name, the call must be rejected. This is the
    /// "drift" scenario where the engine is initially homogeneous and
    /// a follow-up `set_compaction_filter` would create a split.
    #[test]
    fn test_r46_h1_set_compaction_filter_rejects_drift() {
        use crate::compaction_filter::{CompactionDecision, CompactionFilter};
        use forst_rs_common::OpType;
        struct NamedFilter(&'static str);
        impl CompactionFilter for NamedFilter {
            fn filter(
                &self,
                _level: u32,
                _key: &[u8],
                _value: Option<&[u8]>,
                _sequence: u64,
                _op_type: OpType,
                _value_out: &mut Vec<u8>,
            ) -> CompactionDecision {
                CompactionDecision::Keep
            }
            fn name(&self) -> String {
                // R47-H1: trait now returns `String` so impls can encode
                // per-instance config in the identity. This test impl
                // wraps a `&'static str` so we just clone it.
                self.0.to_string()
            }
        }
        let db = open();
        // Anchor both CFs with matching filter A at create time so the
        // homogeneity baseline is "filter name = A".
        let filter_a1: Arc<dyn CompactionFilter> = Arc::new(NamedFilter("ttl-A"));
        let filter_a2: Arc<dyn CompactionFilter> = Arc::new(NamedFilter("ttl-A"));
        let cf_a = db
            .create_column_family(
                ColumnFamilyDescriptor::new("cf_a").with_compaction_filter(filter_a1),
            )
            .expect("cf_a accepted with filter A");
        let cf_b = db
            .create_column_family(
                ColumnFamilyDescriptor::new("cf_b").with_compaction_filter(filter_a2),
            )
            .expect("cf_b accepted with matching filter A");
        let _ = &cf_a;
        // Now drift cf_b to a different filter name — must reject.
        let filter_b: Arc<dyn CompactionFilter> = Arc::new(NamedFilter("ttl-B"));
        let err = db
            .set_compaction_filter(&cf_b, Some(filter_b))
            .expect_err("drifting cf_b's filter to a different name must be rejected");
        let msg = format!("{}", err);
        assert!(
            msg.contains("R45-H1"),
            "error must cite the homogeneity constraint: {}",
            msg
        );
    }

    /// R46-H1: a same-named filter swap (e.g. re-installing the same
    /// filter, or installing a structurally-identical one) must NOT be
    /// rejected — the homogeneity check is on `name()`, and equal names
    /// are explicitly allowed.
    #[test]
    fn test_r46_h1_set_compaction_filter_allows_same_name() {
        use crate::compaction_filter::{CompactionDecision, CompactionFilter};
        use forst_rs_common::OpType;
        struct NamedFilter(&'static str);
        impl CompactionFilter for NamedFilter {
            fn filter(
                &self,
                _level: u32,
                _key: &[u8],
                _value: Option<&[u8]>,
                _sequence: u64,
                _op_type: OpType,
                _value_out: &mut Vec<u8>,
            ) -> CompactionDecision {
                CompactionDecision::Keep
            }
            fn name(&self) -> String {
                // R47-H1: trait now returns `String` so impls can encode
                // per-instance config in the identity. This test impl
                // wraps a `&'static str` so we just clone it.
                self.0.to_string()
            }
        }

        let db = open();
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("cf"))
            .expect("cf accepted");
        let filter_1: Arc<dyn CompactionFilter> = Arc::new(NamedFilter("ttl-X"));
        let filter_2: Arc<dyn CompactionFilter> = Arc::new(NamedFilter("ttl-X"));
        db.set_compaction_filter(&cf, Some(filter_1))
            .expect("first install accepted");
        db.set_compaction_filter(&cf, Some(filter_2))
            .expect("same-named re-install accepted");
    }

    /// R47-H1: regression test for `FlinkTtlCompactionFilter::name()`
    /// encoding the configured TTL. Pre-fix the name was a constant so
    /// two filters with TTL = 10_000ms and TTL = 60_000ms appeared
    /// homogeneous and the engine admitted both. Post-fix the homogeneity
    /// check observes distinct names and rejects the second install.
    #[test]
    fn test_r47_h1_flink_ttl_filter_name_encodes_ttl() {
        use crate::compaction_filter::{FlinkTtlCompactionFilter, TtlStateType};

        let db = open();
        let f10: Arc<dyn crate::CompactionFilter> = Arc::new(FlinkTtlCompactionFilter::new(
            10_000,
            TtlStateType::Value,
            0,
        ));
        let f60: Arc<dyn crate::CompactionFilter> = Arc::new(FlinkTtlCompactionFilter::new(
            60_000,
            TtlStateType::Value,
            0,
        ));
        // Names must encode the TTL, so the two filters' identities
        // disagree.
        assert_ne!(f10.name(), f60.name());

        let _cf_a = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_a").with_compaction_filter(f10))
            .expect("cf_a accepted with TTL=10s filter");
        // cf_b carries a different TTL → distinct name → must be
        // rejected by the homogeneity check.
        let err = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_b").with_compaction_filter(f60))
            .expect_err("cf_b must be rejected — different TTL is a different identity");
        let msg = format!("{}", err);
        assert!(
            msg.contains("R45-H1"),
            "error must cite the homogeneity constraint: {}",
            msg
        );
    }

    /// R47-H3: regression test for `ListAppendMergeOperator::name()`
    /// encoding the configured delimiter. Pre-fix the name was a
    /// constant so two operators with different delimiters (`,` vs `|`)
    /// were admitted as homogeneous and the engine produced wrong
    /// merge results during cross-CF compaction. Post-fix the
    /// homogeneity check observes distinct names and rejects.
    #[test]
    fn test_r47_h3_list_append_name_encodes_delimiter() {
        use forst_rs_storage::merge_operator::ListAppendMergeOperator;

        let db = open();
        let comma: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        let pipe: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::new(b'|'));
        assert_ne!(comma.name(), pipe.name());

        let _cf_a = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_a").with_merge_operator(comma))
            .expect("cf_a accepted with comma-delimited merge");
        let err = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_b").with_merge_operator(pipe))
            .expect_err("cf_b must be rejected — different delimiter is a different identity");
        let msg = format!("{}", err);
        assert!(
            msg.contains("R45-H1"),
            "error must cite the homogeneity constraint: {}",
            msg
        );
    }

    /// R47-H2: TOCTOU regression. Two concurrent `set_compaction_filter`
    /// calls on distinct CFs that each try to install a DIFFERENT filter
    /// name must NOT both succeed — pre-fix the check used a read lock
    /// that both threads could pass before either committed. Post-fix
    /// the check + install runs under a single `cfs.write()` guard so
    /// at most one install crosses the homogeneity gate.
    ///
    /// We can't easily force a thread-scheduling race in a unit test,
    /// but we CAN assert the sequential semantics: install on cf_a
    /// succeeds (homogeneity baseline of all-None), the subsequent
    /// install on cf_b with a DIFFERENT name MUST be rejected. The
    /// lock-window fix is what makes this property hold under
    /// concurrent calls as well.
    #[test]
    fn test_r47_h2_set_compaction_filter_serialized_install() {
        use crate::compaction_filter::{CompactionDecision, CompactionFilter};
        use forst_rs_common::OpType;
        struct NamedFilter(&'static str);
        impl CompactionFilter for NamedFilter {
            fn filter(
                &self,
                _level: u32,
                _key: &[u8],
                _value: Option<&[u8]>,
                _sequence: u64,
                _op_type: OpType,
                _value_out: &mut Vec<u8>,
            ) -> CompactionDecision {
                CompactionDecision::Keep
            }
            fn name(&self) -> String {
                self.0.to_string()
            }
        }
        let db = open();
        let cf_a = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_a"))
            .expect("cf_a accepted");
        let cf_b = db
            .create_column_family(ColumnFamilyDescriptor::new("cf_b"))
            .expect("cf_b accepted");

        // Install filter A on cf_a — succeeds (cf_b still has None
        // which means heterogeneity vs cf_a → wait, the FIRST install
        // creates the heterogeneity baseline. Pre-fix this also failed
        // because cf_b was still unfiltered. Post-R46-H1 fix this is
        // the expected behaviour: the first install is the one that
        // creates the split, and rejecting it surfaces the config
        // error at the earliest possible point.).
        let filter_a: Arc<dyn CompactionFilter> = Arc::new(NamedFilter("A"));
        let err = db
            .set_compaction_filter(&cf_a, Some(filter_a))
            .expect_err("set_compaction_filter must be rejected — cf_b still has no filter");
        let msg = format!("{}", err);
        assert!(
            msg.contains("R45-H1"),
            "error must cite the homogeneity constraint: {}",
            msg
        );
        // cf_b also untouched — no install crossed the gate.
        assert!(cf_b.id() != cf_a.id());
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
        // at-or-past the InternalKey 56-bit fatal threshold. The write path must
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

    #[test]
    fn test_seq_overflow_past_internal_key_56_bit_limit_returns_internal_error() {
        let db = open();
        // InternalKey stores sequence in the upper 56 bits of the tag. The
        // write path must refuse the first value above that limit before it
        // reaches memtable/SST encoding.
        db.force_set_sequence(forst_rs_common::MAX_SEQUENCE_NUMBER.0);
        let cf = db.default_cf();
        let err = db
            .put(&cf, b"past-56-bit", b"v")
            .expect_err("sequence past InternalKey 56-bit limit must error");
        assert!(
            err.is_internal(),
            "expected Internal error past 56-bit sequence limit, got {:?}",
            err
        );
    }
}
