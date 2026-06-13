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

//! Flush pipeline: frozen memtable → on-disk SST file. See `2.8_read_write_paths.md` §2.4.
//!
//! A [`FlushJob`] is a self-contained unit of work that:
//! 1. Pulls sorted `RecordBatch`es from a frozen [`forst_rs_storage::memtable::VectorizedMemTable`].
//! 2. Feeds them row-by-row through [`SstWriterImpl`], producing the
//!    complete SST file bytes.
//! 3. Writes those bytes atomically to disk via the [`FileSystem`]
//!    abstraction (use `rename` from a temp file to guarantee crash
//!    safety).
//! 4. Returns an [`SstFileMeta`] ready to be recorded in the
//!    [`VersionSet`](forst_rs_storage::version::VersionSetImpl).

use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, Weak};

use arrow::array::{Array, BinaryArray, BinaryBuilder, UInt64Array, UInt8Array};
use forst_rs_common::{ColumnFamilyId, FileNumber, ForstError, ForstResult, SequenceNumber};
use forst_rs_io::{FileSystem, WriteMode};
use forst_rs_storage::sst::{SstFileInfo, SstWriterImpl, SstWriterOptions};
use forst_rs_storage::version::{SstFileMeta, VlogSegmentMeta};
use forst_rs_storage::vlog::{vlog_segment_path, VlogWriter};

use crate::column_family::{ColumnFamilyData, SharedMemTable};

/// Batch size used when converting memtable rows to SST entries. Larger
/// batches reduce per-row overhead but increase peak memory usage during
/// flush. 8192 matches the default Arrow batch size across the project.
const FLUSH_BATCH_SIZE: usize = 8192;

/// R39-L1: leading-dot prefix and trailing `.tmp` suffix used to name
/// mid-write SST files (both [`FlushJob::temp_path`] and the
/// compaction-job tmp helper produce `.<basename>.tmp`). Centralised here
/// so the orphan-scan in `db.rs::open_from_checkpoint` references the
/// SAME constants used by the writers — a future change to the naming
/// convention will fail loudly at the call sites rather than silently
/// drop orphan-rename coverage. A `tests::temp_path_constants_match`
/// unit test pins the writer-side string against these constants.
pub(crate) const SST_TMP_PREFIX: &str = ".";
pub(crate) const SST_TMP_SUFFIX: &str = ".tmp";

/// Build the tmp path corresponding to a final SST `path` (writers call
/// this to derive the staging file; the restore orphan-scan reverses it).
pub(crate) fn sst_temp_path(path: &Path) -> PathBuf {
    let mut base = path.to_path_buf();
    let existing = base
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    base.set_file_name(format!("{}{}{}", SST_TMP_PREFIX, existing, SST_TMP_SUFFIX));
    base
}

/// FRS-WA-V2a-2 (write-path redesign survey §3.1/§6 stage V2): per-flush
/// KV-separation directive. Present ⇒ this flush diverts Put values whose
/// length is `>= min_blob_size` into the append-once value-log segment
/// `<segment_id>.vlog` (created LAZILY on the first qualifying value — a
/// flush with no qualifying value creates no file and reports no segment)
/// and stores `BlobRef(ValuePointer)` rows in the SST instead. Eligibility
/// (per-CF policy: Unbounded + no merge operator + no compaction filter) is
/// decided by the CALLER ([`crate::DbImpl`]) — the job applies the
/// directive mechanically.
#[derive(Debug, Clone, Copy)]
pub struct KvSepSpec {
    /// Pre-allocated segment file id (same counter as SST numbers).
    pub segment_id: FileNumber,
    /// Values shorter than this stay inline.
    pub min_blob_size: usize,
}

/// A single flush operation: one frozen memtable → one SST file.
pub struct FlushJob {
    memtable: SharedMemTable,
    file_number: FileNumber,
    /// R49-H1: column family that owns the memtable being flushed. Stamped
    /// onto the resulting SST footer and `SstFileMeta` so per-CF SST
    /// isolation is enforced end-to-end.
    cf_id: ColumnFamilyId,
    file_path: PathBuf,
    options: SstWriterOptions,
    fs: Arc<dyn FileSystem>,
    /// FRS-WA-V2a-2: KV-separation directive (None = classic flush).
    kv_sep: Option<KvSepSpec>,
}

impl FlushJob {
    /// Constructs a new flush job. The `file_path` must be an absolute or
    /// engine-relative path at which the SST file will be written. The
    /// memtable must already be frozen.
    ///
    /// R49-H1: `cf_id` is captured from the caller's
    /// [`ColumnFamilyData::handle().id()`] so the produced SST carries
    /// CF identity all the way through the LSM. The writer options'
    /// `cf_id` field is overwritten with this value — callers should
    /// not pre-set it.
    pub fn new(
        memtable: SharedMemTable,
        file_number: FileNumber,
        cf_id: ColumnFamilyId,
        file_path: PathBuf,
        mut options: SstWriterOptions,
        fs: Arc<dyn FileSystem>,
    ) -> Self {
        options.cf_id = cf_id;
        Self {
            memtable,
            file_number,
            cf_id,
            file_path,
            options,
            fs,
            kv_sep: None,
        }
    }

    /// FRS-WA-V2a-2: arms KV separation for this flush (builder-style).
    pub fn with_kv_separation(mut self, spec: KvSepSpec) -> Self {
        self.kv_sep = Some(spec);
        self
    }

    /// Runs the flush synchronously. Returns the metadata describing the
    /// produced SST file, ready to feed into a `VersionEdit`.
    ///
    /// PR-D2 (Z3-10, C-R3-H1..3): the flush pipeline streams data blocks
    /// directly to the on-disk temp file through
    /// [`SstWriterImpl::streaming`] — there is no intermediate `Vec<u8>`
    /// holding the entire SST in memory. Peak flush memory is bounded by
    /// one in-flight Arrow data block plus the bloom + sparse-index
    /// sections (proportional to block count, not byte count).
    pub fn run(self) -> ForstResult<SstFileMeta> {
        self.run_kv().map(|(meta, _)| meta)
    }

    /// FRS-WA-V2a-2: variant of [`Self::run`] that also reports the value-log
    /// segment this flush produced (None when KV separation was not armed or
    /// no value qualified). Durability order: the vlog segment is fsynced
    /// BEFORE the SST finishes/publishes, so a published pointer row can
    /// never reference un-durable value bytes (the WiscKey ordering).
    pub fn run_kv(self) -> ForstResult<(SstFileMeta, Option<VlogSegmentMeta>)> {
        // 1. Pull sorted RecordBatches from the sharded memtable.
        //    `to_flush_batches` requires every shard to be frozen; the
        //    engine must have done that via `ShardedMemTable::freeze`
        //    before scheduling the flush. The sharded impl merges across
        //    shards, returning a globally sorted (key ASC, seq DESC) stream.
        if !self.memtable.is_frozen() {
            return Err(ForstError::invalid_argument(
                "FlushJob: memtable is not frozen; call freeze() first",
            ));
        }
        if self.memtable.num_entries() == 0 {
            return Err(ForstError::invalid_argument(
                "FlushJob: memtable is empty; nothing to flush",
            ));
        }
        let batches = self.memtable.to_flush_batches(FLUSH_BATCH_SIZE)?;

        // 2. Open the temp file and stream the SST directly into it. We
        //    write to a temp file and rename into place so a mid-write
        //    crash never leaves a partial SST that the engine might pick up.
        let parent = self.file_path.parent().ok_or_else(|| {
            ForstError::invalid_argument(format!(
                "flush target has no parent directory: {}",
                self.file_path.display()
            ))
        })?;
        self.fs.create_dir_all(parent)?;
        // FRS-S3-SSTRENAME: on local FS, stage to a `.tmp` file and rename into
        // place so a mid-write crash never leaves a partial SST under the final
        // name. On object stores there is no atomic rename (OpenDAL surfaces it
        // as Unsupported), so stream straight to the final key — the multipart
        // upload only publishes on a successful close, giving the same
        // crash-atomic guarantee without a rename.
        let atomic_rename = self.fs.supports_atomic_rename();
        let write_path = if atomic_rename {
            self.temp_path()
        } else {
            self.file_path.clone()
        };
        // FRS-S3-ORPHAN-FIX: on object stores (no atomic rename) the SST
        // streams straight to its FINAL path. After a crash + restart with
        // checkpointing off, the file-number counter resets and re-allocates
        // a number whose SST from the failed attempt still lingers on S3, so
        // `CreateNew` fails with "file already exists" and the job crash-loops
        // forever. A collision here can only be such a stale orphan (file
        // numbers are uniquely allocated within a run), so overwriting is
        // correct — the freshly-flushed SST is authoritative. The local-FS
        // branch writes to a unique temp path and keeps `CreateNew` so a
        // genuine number-reuse bug is still caught.
        let write_mode = if atomic_rename {
            WriteMode::CreateNew
        } else {
            WriteMode::CreateOrTruncate
        };
        // R40-M1: wrap the writer/flush/sync block in a closure so any `?`-propagated error from
        // `writer.add`, `writer.finish`, `writable.flush`, or `writable.sync` triggers a best-
        // effort delete of the staging tmp file before we propagate. The pre-existing R38-H1 fix
        // only covered the post-block `fs.rename` failure; an earlier writer-flow throw would
        // leave `.<num>.sst.tmp` orphaned in-process until the restore orphan-scan picked it up
        // on next restart. Mirrors the same wrapper in `compaction.rs` so all SST-write sites
        // share one tmp-leak-safe contract.
        // FRS-WA-V2a-2: lazily-created value-log writer (lives outside the
        // closure so the segment meta survives it; the error path below
        // deletes the partial segment alongside the partial SST).
        let mut vlog: Option<VlogWriter> = None;
        let info_result: ForstResult<SstFileInfo> = (|| {
            let mut writable = self.fs.open_writable_file(&write_path, write_mode)?;
            let writer_inner = SstWriterImpl::with_options(self.options.clone());
            let mut writer = writer_inner.streaming(&mut *writable);

            // 3. Feed each row into the streaming SST writer. We iterate
            //    with `add()` so completed data blocks stream to the temp
            //    file as soon as they fill — no full-SST `Vec<u8>` is ever
            //    allocated. The writer preserves its sorted-order
            //    invariant (entries arrive in sorted (key ASC, seq DESC)
            //    order matching `to_flush_batches`).
            for batch in &batches {
                let rows = batch.num_rows();
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| ForstError::corruption("flush batch: key column not Binary"))?;
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        ForstError::corruption("flush batch: value column not Binary")
                    })?;
                let seqs = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        ForstError::corruption("flush batch: sequence column not UInt64")
                    })?;
                let ops = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .ok_or_else(|| {
                        ForstError::corruption("flush batch: op_type column not UInt8")
                    })?;

                // B-NEW-H1: bulk-batch dispatch hoists global min/max key
                // bound computation (one copy at row 0 and row N-1 — flush
                // input is sorted ASC) out of the per-row interleave.
                // Block-boundary flush is still respected by the writer.
                let _ = rows;
                // FRS-GARBAGE-DRAIN: count tombstones entering L0 (vectorized
                // u8 scan, ~free) so the maintenance drain can force deep
                // compaction when dead data accumulates (the q9/q20 decay).
                let tombs = ops
                    .values()
                    .iter()
                    .filter(|&&o| {
                        o == forst_rs_common::OpType::Delete as u8
                            || o == forst_rs_common::OpType::SingleDelete as u8
                    })
                    .count() as u64;
                crate::db::note_flushed_tombstones(tombs, ops.len() as u64);
                // FRS-WA-V2a-2: divert qualifying values to the value log and
                // substitute BlobRef pointer rows. Batches with no qualifying
                // row pass through untouched (zero-copy default path).
                if let Some(spec) = self.kv_sep {
                    if let Some((sep_values, sep_ops)) =
                        self.separate_batch_values(spec, values, ops, &mut vlog)?
                    {
                        writer.add_batch(keys, &sep_values, seqs, &sep_ops)?;
                        continue;
                    }
                }
                writer.add_batch(keys, values, seqs, ops)?;
            }

            // FRS-WA-V2a-2 durability order: the value log is fsynced BEFORE
            // the SST finishes (and thus before the rename/close publishes
            // any pointer row referencing it).
            if let Some(w) = vlog.as_mut() {
                w.sync()?;
            }
            let info = writer.finish()?;
            writable.flush()?;
            writable.sync()?;
            Ok(info)
        })();
        let info = match info_result {
            Ok(info) => info,
            Err(e) => {
                // Best-effort cleanup; orphan-scan on restart still covers any residual file.
                let _ = self.fs.delete_file(&write_path);
                // FRS-WA-V2a-2: also drop the partial value-log segment — no
                // pointer into it was ever published (the SST died with it).
                if let (Some(spec), true) = (self.kv_sep, vlog.is_some()) {
                    let _ = self
                        .fs
                        .delete_file(&vlog_segment_path(parent, spec.segment_id.value()));
                }
                return Err(e);
            }
        };
        // On object stores `write_path == file_path` and the upload already
        // published atomically on close — no rename or dir-fsync needed.
        if atomic_rename {
            // R38-H1: best-effort cleanup of the temp file on rename failure
            // (EXDEV, cross-FS, transient I/O). Without this, a failed flush
            // leaves a `.<num>.sst.tmp` orphan that the restore scan in
            // `open_from_checkpoint` did not previously recognise. We delete
            // before propagating the error; if delete itself fails the file
            // remains visible to the next restore, which now matches
            // `.*.sst.tmp` and renames it out of the active naming space.
            if let Err(e) = self.fs.rename(&write_path, &self.file_path) {
                let _ = self.fs.delete_file(&write_path);
                // FRS-WA-V2a-2: the SST never published — drop its segment.
                if let (Some(spec), true) = (self.kv_sep, vlog.is_some()) {
                    let _ = self
                        .fs
                        .delete_file(&vlog_segment_path(parent, spec.segment_id.value()));
                }
                return Err(e);
            }
            // R49-H3: fsync(parent_dir) so the rename's directory entry change
            // is durable. Without this, a power-loss event between rename and
            // the next checkpoint could leave the directory entry pointing at
            // nothing (the file inode survives but the dirent doesn't), so
            // the SST goes missing on restart.
            if let Err(e) = self.fs.sync_dir(parent) {
                tracing::warn!(
                    "FlushJob: sync_dir({}) failed after rename: {} (R49-H3); \
                     SST contents are on disk but the directory entry may be \
                     lost on power-failure restart",
                    parent.display(),
                    e
                );
            }
        }

        // 4. Build the SstFileMeta (and, FRS-WA-V2a-2, the vlog segment
        //    meta) that the VersionSet will record.
        let vlog_meta = match (self.kv_sep, vlog) {
            (Some(spec), Some(w)) => Some(VlogSegmentMeta {
                segment_id: spec.segment_id.value(),
                cf_id: self.cf_id,
                file_size: w.size(),
                // FRS-WA-V2b: every appended payload byte starts live
                // (exactly one pointer row per append in this SST).
                live_bytes: w.payload_bytes(),
            }),
            _ => None,
        };
        Ok((
            Self::info_to_meta(self.file_number, self.cf_id, info),
            vlog_meta,
        ))
    }

    /// FRS-WA-V2a-2: if `values`/`ops` contain at least one qualifying row
    /// (a non-null `Put` payload of `>= spec.min_blob_size` bytes), append
    /// those payloads to the value log (creating the segment lazily) and
    /// return substituted `(values, ops)` arrays where each qualifying row
    /// became `(ValuePointer bytes, BlobRef)`. Returns `None` when nothing
    /// qualifies — the caller keeps the original arrays untouched.
    fn separate_batch_values(
        &self,
        spec: KvSepSpec,
        values: &BinaryArray,
        ops: &UInt8Array,
        vlog: &mut Option<VlogWriter>,
    ) -> ForstResult<Option<(BinaryArray, UInt8Array)>> {
        let put = forst_rs_common::OpType::Put as u8;
        let qualifies = |row: usize| {
            ops.value(row) == put
                && !values.is_null(row)
                && values.value(row).len() >= spec.min_blob_size
        };
        let rows = ops.len();
        if !(0..rows).any(qualifies) {
            return Ok(None);
        }
        let dir = self
            .file_path
            .parent()
            .ok_or_else(|| ForstError::invalid_argument("flush target has no parent directory"))?;
        let mut new_values = BinaryBuilder::new();
        let mut new_ops: Vec<u8> = Vec::with_capacity(rows);
        for row in 0..rows {
            if qualifies(row) {
                if vlog.is_none() {
                    *vlog = Some(VlogWriter::create(
                        self.fs.as_ref(),
                        dir,
                        spec.segment_id.value(),
                    )?);
                }
                let w = vlog.as_mut().expect("vlog writer just ensured");
                let ptr = w.append(values.value(row))?;
                new_values.append_value(ptr.encode());
                new_ops.push(forst_rs_common::OpType::BlobRef as u8);
            } else {
                if values.is_null(row) {
                    new_values.append_null();
                } else {
                    new_values.append_value(values.value(row));
                }
                new_ops.push(ops.value(row));
            }
        }
        Ok(Some((new_values.finish(), UInt8Array::from(new_ops))))
    }

    fn temp_path(&self) -> PathBuf {
        // R39-L1: delegate to the centralised helper so the writer and
        // restore-orphan-scan use the same naming convention.
        sst_temp_path(&self.file_path)
    }

    fn info_to_meta(
        file_number: FileNumber,
        cf_id: ColumnFamilyId,
        info: SstFileInfo,
    ) -> SstFileMeta {
        // R49-H1: stamp cf_id onto the meta record so the engine's `sst_get`
        // and friends can filter by CF. We assert agreement with the writer-
        // side value to catch any future drift between `SstWriterOptions::cf_id`
        // and the `SstFileMeta::cf_id` install path.
        debug_assert_eq!(info.cf_id, cf_id);
        SstFileMeta {
            file_number,
            cf_id,
            file_size: info.file_size,
            smallest_key: info.min_key,
            largest_key: info.max_key,
            min_sequence: SequenceNumber(info.min_sequence),
            max_sequence: SequenceNumber(info.max_sequence),
            num_entries: info.entry_count,
            // FRS-WA-V1: the death stamp is applied by `flush_cf_data` AFTER
            // the job returns (it needs the CF's lifecycle + event-time clock,
            // which the FlushJob deliberately doesn't know about).
            max_death: 0,
        }
    }
}

/// Computes the standard SST file path for a given file number under a
/// database directory. Format: `<db_path>/<file_number:06>.sst`.
pub fn sst_file_path(db_path: &Path, file_number: FileNumber) -> PathBuf {
    db_path.join(format!("{:06}.sst", file_number.value()))
}

// ---------------------------------------------------------------------
// Background flush worker plumbing (B1, design §2.4.2).
//
// Writers move flush work off the critical path by enqueuing a
// [`FlushRequest`] onto a [`FlushQueue`]; a single worker thread spawned
// by [`crate::DbImpl`] drains the queue and runs `flush_cf_data` for
// each request. Backpressure is provided by the existing
// [`crate::WriteController`] (`max_write_buffer_number` cap on the
// imm queue) — when too many imms are queued the WriteController stalls
// new writers until the worker drains one and calls
// `set_imm_count(new_lower)`.
// ---------------------------------------------------------------------

/// A single asynchronous flush job: "please flush the next imm in this CF."
///
/// We carry the [`Arc<ColumnFamilyData>`] (rather than the imm itself) so
/// the worker can look up the *current* oldest imm under the per-CF flush
/// mutex. This lets the worker collapse multiple requests for the same CF
/// into one no-op when the queue is bursty (the second request finds an
/// empty imm list and returns early).
#[allow(dead_code)] // FRS-SLOT-SHARED-BG: legacy per-DbImpl queue, superseded by crate::bg_pool shared pool
pub(crate) struct FlushRequest {
    pub cf_data: Arc<ColumnFamilyData>,
}

/// MPSC channel used to hand flush requests from writer threads to the
/// background worker. The receiver is held inside the queue under a Mutex
/// so the worker can take it once on startup; the sender is cheaply
/// cloneable via the queue's `enqueue` method.
///
/// Bounded capacity prevents a runaway producer from ballooning queued
/// requests; the bound is large enough that ordinary backpressure flows
/// through the WriteController instead.
#[allow(dead_code)] // FRS-SLOT-SHARED-BG: legacy per-DbImpl queue, superseded by crate::bg_pool shared pool
pub(crate) struct FlushQueue {
    tx: SyncSender<FlushRequest>,
    rx: Mutex<Option<Receiver<FlushRequest>>>,
}

#[allow(dead_code)] // FRS-SLOT-SHARED-BG: legacy per-DbImpl queue, superseded by crate::bg_pool shared pool
impl FlushQueue {
    /// Constructs a new queue with the given bounded capacity. Returns the
    /// queue plus a one-time-takeable receiver consumer (kept inside the
    /// queue under a Mutex so the worker can acquire it on startup).
    pub(crate) fn new(capacity: usize) -> Self {
        let (tx, rx) = sync_channel::<FlushRequest>(capacity);
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
        }
    }

    /// Hands the receiver to the worker thread. Returns `None` if a worker
    /// has already taken it (which would be a programming error — only one
    /// worker is intended).
    pub(crate) fn take_receiver(&self) -> Option<Receiver<FlushRequest>> {
        self.rx.lock().expect("lock poisoned").take()
    }

    /// Non-blocking enqueue — used by writers on the critical path. If the
    /// queue is full (worker is far behind), falls back to a blocking
    /// `send`. This still bounds the writer's wait by the worker's flush
    /// latency rather than the kernel write/syscall latency on every
    /// switch, which is the whole point of B1.
    ///
    /// Returns `Err` only if the receiver has been dropped (i.e. the engine
    /// is shutting down). The writer should treat that as a no-op since the
    /// engine is going away anyway.
    pub(crate) fn enqueue(&self, req: FlushRequest) -> ForstResult<()> {
        match self.tx.try_send(req) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(req)) => self.tx.send(req).map_err(|_| {
                ForstError::aborted("flush queue receiver dropped (engine shutting down)")
            }),
            Err(TrySendError::Disconnected(_)) => Err(ForstError::aborted(
                "flush queue receiver dropped (engine shutting down)",
            )),
        }
    }
}

/// Trait abstracting the engine method the flush worker calls back into.
/// We keep this private to avoid pulling `DbImpl` into this module's
/// public surface.
pub(crate) trait FlushExecutor: Send + Sync {
    /// Synchronously flush the oldest imm of `cf_data`. Returns `Ok(())`
    /// even if the imm list is empty (treated as a no-op).
    fn run_flush(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<()>;
}

/// Worker loop: drains the flush queue and dispatches each request to the
/// engine via the [`FlushExecutor`] callback.
///
/// Holds `Weak<E>` so the engine can be dropped while the worker is mid-
/// recv; in that case the upgrade fails and we exit. On a normal shutdown
/// the engine drops the queue's `tx`, which closes the channel and
/// `recv()` returns `Err`.
///
/// Errors from `run_flush` are recorded via `record_error` so the next
/// writer can observe and surface them. We never panic the worker on
/// flush errors — the engine should remain usable for reads even if a
/// flush is failing repeatedly.
#[allow(dead_code)] // FRS-SLOT-SHARED-BG: legacy per-DbImpl queue, superseded by crate::bg_pool shared pool
pub(crate) fn flush_loop<E>(
    rx: Receiver<FlushRequest>,
    engine_weak: Weak<E>,
    record_error: impl Fn(ForstError) + Send + 'static,
) where
    E: FlushExecutor + 'static,
{
    while let Ok(req) = rx.recv() {
        // If the engine has been dropped, exit cleanly.
        let Some(engine) = engine_weak.upgrade() else {
            break;
        };

        if let Err(e) = engine.run_flush(&req.cf_data) {
            // Stash the error for the next writer to surface. We continue
            // looping so subsequent flushes (possibly for other CFs) get a
            // chance — a transient I/O hiccup shouldn't permanently disable
            // background flushing.
            record_error(e);
        }
    }
}

// ---------------------------------------------------------------------
// FRS-COMPACT-BG (2026-06-03): background L0→L1 compaction worker.
//
// Previously `run_flush` called `maybe_auto_compact` INLINE on the single
// flush-worker thread, so a large L0→L1 compaction blocked subsequent
// flushes → memtables backed up (write-stall) AND L0 stayed deep → point
// reads (`get_arc → sst_get`) scanned a growing L0 → throughput DECAY
// (NexMark q11 665K→37K rec/s; q4 likewise). Mirroring the flush-worker
// pattern, compaction now runs on its OWN thread: the flush worker just
// ENQUEUES a request and returns immediately, keeping L0 shallow without
// stalling flushes. Concurrency is already safe — `compact_l0_for_cf`
// takes `compaction_mutex` (engine-global) before any per-CF `flush_mutex`,
// and `version_set.apply` is serialized with stale-edit (R44-L2) validation.
// ---------------------------------------------------------------------

/// A single asynchronous L0→L1 compaction job for a CF. Like
/// [`FlushRequest`], carries only the CF so the worker re-reads the current
/// L0 set under `compaction_mutex` (bursty duplicates collapse to a no-op).
#[allow(dead_code)] // FRS-SLOT-SHARED-BG: legacy per-DbImpl queue, superseded by crate::bg_pool shared pool
pub(crate) struct CompactionRequest {
    pub cf_data: Arc<ColumnFamilyData>,
}

/// MPSC channel handing compaction requests from the flush worker to the
/// background compaction worker. Same shape as [`FlushQueue`].
#[allow(dead_code)] // FRS-SLOT-SHARED-BG: legacy per-DbImpl queue, superseded by crate::bg_pool shared pool
pub(crate) struct CompactionQueue {
    tx: SyncSender<CompactionRequest>,
    rx: Mutex<Option<Receiver<CompactionRequest>>>,
}

#[allow(dead_code)] // FRS-SLOT-SHARED-BG: legacy per-DbImpl queue, superseded by crate::bg_pool shared pool
impl CompactionQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        let (tx, rx) = sync_channel::<CompactionRequest>(capacity);
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
        }
    }

    pub(crate) fn take_receiver(&self) -> Option<Receiver<CompactionRequest>> {
        self.rx.lock().expect("lock poisoned").take()
    }

    /// Non-blocking enqueue from the flush worker. Returns `Err` only if the
    /// receiver was dropped (engine shutting down).
    pub(crate) fn enqueue(&self, req: CompactionRequest) -> ForstResult<()> {
        match self.tx.try_send(req) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(req)) => self.tx.send(req).map_err(|_| {
                ForstError::aborted("compaction queue receiver dropped (engine shutting down)")
            }),
            Err(TrySendError::Disconnected(_)) => Err(ForstError::aborted(
                "compaction queue receiver dropped (engine shutting down)",
            )),
        }
    }
}

/// Trait abstracting the engine method the compaction worker calls back into.
pub(crate) trait CompactionExecutor: Send + Sync {
    /// Run an L0→L1 compaction for `cf_data` (no-op if nothing to compact).
    fn run_compaction(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<()>;
}

/// Worker loop: drains the compaction queue and dispatches to the engine via
/// [`CompactionExecutor`]. Mirrors [`flush_loop`] exactly (Weak-upgrade exit,
/// record-error-and-continue, channel-close shutdown).
#[allow(dead_code)] // FRS-SLOT-SHARED-BG: legacy per-DbImpl queue, superseded by crate::bg_pool shared pool
pub(crate) fn compaction_loop<E>(
    rx: Receiver<CompactionRequest>,
    engine_weak: Weak<E>,
    record_error: impl Fn(ForstError) + Send + 'static,
) where
    E: CompactionExecutor + 'static,
{
    while let Ok(req) = rx.recv() {
        let Some(engine) = engine_weak.upgrade() else {
            break;
        };
        if let Err(e) = engine.run_compaction(&req.cf_data) {
            record_error(e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_common::{CompressionType, OpType, DEFAULT_CF_ID};
    use forst_rs_io::MemoryFileSystem;
    use forst_rs_storage::memtable::ShardedMemTable;

    type MemEntry<'a> = (&'a [u8], Option<&'a [u8]>, u8);

    fn make_memtable(entries: &[MemEntry<'_>]) -> SharedMemTable {
        let mem = ShardedMemTable::with_defaults();
        for (k, v, op) in entries {
            mem.put(k, *v, *op).unwrap();
        }
        mem.freeze();
        Arc::new(mem)
    }

    fn default_writer_opts() -> SstWriterOptions {
        SstWriterOptions {
            block_size: 4 * 1024,
            compression: CompressionType::None,
            cf_id: DEFAULT_CF_ID,
        }
    }

    #[test]
    fn test_flush_rejects_unfrozen_memtable() {
        let mem = ShardedMemTable::with_defaults();
        let shared = Arc::new(mem);
        let fs = Arc::new(MemoryFileSystem::new());
        let job = FlushJob::new(
            shared,
            FileNumber(1),
            DEFAULT_CF_ID,
            PathBuf::from("/db/000001.sst"),
            default_writer_opts(),
            fs,
        );
        let err = job.run().unwrap_err();
        assert!(err.to_string().contains("not frozen"));
    }

    #[test]
    fn test_flush_rejects_empty_memtable() {
        let mem = ShardedMemTable::with_defaults();
        mem.freeze();
        let shared = Arc::new(mem);
        let fs = Arc::new(MemoryFileSystem::new());
        let job = FlushJob::new(
            shared,
            FileNumber(1),
            DEFAULT_CF_ID,
            PathBuf::from("/db/000001.sst"),
            default_writer_opts(),
            fs,
        );
        let err = job.run().unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_flush_writes_sst_file() {
        let mem = make_memtable(&[
            (b"a", Some(b"1"), OpType::Put as u8),
            (b"b", Some(b"2"), OpType::Put as u8),
            (b"c", None, OpType::Delete as u8),
        ]);
        let fs = Arc::new(MemoryFileSystem::new());
        let path = PathBuf::from("/db/000007.sst");
        let job = FlushJob::new(
            mem,
            FileNumber(7),
            DEFAULT_CF_ID,
            path.clone(),
            default_writer_opts(),
            fs.clone(),
        );
        let meta = job.run().unwrap();
        assert_eq!(meta.file_number, FileNumber(7));
        assert_eq!(meta.cf_id, DEFAULT_CF_ID);
        assert_eq!(meta.num_entries, 3);
        assert_eq!(meta.smallest_key, b"a");
        assert_eq!(meta.largest_key, b"c");
        assert!(fs.file_exists(&path).unwrap());
    }

    #[test]
    fn test_flush_tmp_file_cleaned_up_on_success() {
        let mem = make_memtable(&[(b"a", Some(b"1"), OpType::Put as u8)]);
        let fs = Arc::new(MemoryFileSystem::new());
        let path = PathBuf::from("/db/000001.sst");
        let job = FlushJob::new(
            mem,
            FileNumber(1),
            DEFAULT_CF_ID,
            path.clone(),
            default_writer_opts(),
            fs.clone(),
        );
        job.run().unwrap();
        let tmp = PathBuf::from("/db/.000001.sst.tmp");
        assert!(!fs.file_exists(&tmp).unwrap());
    }

    #[test]
    fn test_flush_captures_min_max_sequence() {
        let mem = ShardedMemTable::with_defaults();
        // Use put_with_seq so seq numbering is deterministic across shards.
        mem.put_with_seq(b"a", Some(b"1"), OpType::Put as u8, 1)
            .unwrap();
        mem.put_with_seq(b"b", Some(b"2"), OpType::Put as u8, 2)
            .unwrap();
        mem.put_with_seq(b"a", Some(b"3"), OpType::Put as u8, 3)
            .unwrap();
        mem.freeze();
        let shared = Arc::new(mem);

        let fs = Arc::new(MemoryFileSystem::new());
        let job = FlushJob::new(
            shared,
            FileNumber(1),
            DEFAULT_CF_ID,
            PathBuf::from("/db/000001.sst"),
            default_writer_opts(),
            fs,
        );
        let meta = job.run().unwrap();
        assert_eq!(meta.min_sequence, SequenceNumber(1));
        assert_eq!(meta.max_sequence, SequenceNumber(3));
    }

    #[test]
    fn test_flush_many_entries_spans_multiple_blocks() {
        let mem = ShardedMemTable::with_defaults();
        for i in 0..2000u32 {
            let key = format!("k{:06}", i);
            let value = format!("v{:06}", i);
            mem.put_with_seq(
                key.as_bytes(),
                Some(value.as_bytes()),
                OpType::Put as u8,
                i as u64 + 1,
            )
            .unwrap();
        }
        mem.freeze();
        let shared = Arc::new(mem);
        let fs = Arc::new(MemoryFileSystem::new());
        let job = FlushJob::new(
            shared,
            FileNumber(42),
            DEFAULT_CF_ID,
            PathBuf::from("/db/000042.sst"),
            default_writer_opts(),
            fs,
        );
        let meta = job.run().unwrap();
        assert_eq!(meta.num_entries, 2000);
        assert!(meta.file_size > 0);
        assert_eq!(meta.smallest_key, b"k000000");
        assert_eq!(meta.largest_key, b"k001999");
    }

    #[test]
    fn test_flush_parent_dir_is_created() {
        let mem = make_memtable(&[(b"a", Some(b"1"), OpType::Put as u8)]);
        let fs = Arc::new(MemoryFileSystem::new());
        let deep = PathBuf::from("/a/b/c/d/000001.sst");
        let job = FlushJob::new(
            mem,
            FileNumber(1),
            DEFAULT_CF_ID,
            deep.clone(),
            default_writer_opts(),
            fs.clone(),
        );
        job.run().unwrap();
        assert!(fs.file_exists(&deep).unwrap());
    }

    #[test]
    fn test_sst_file_path_formatting() {
        let base = PathBuf::from("/db");
        let p = sst_file_path(&base, FileNumber(42));
        assert_eq!(p, PathBuf::from("/db/000042.sst"));
    }

    #[test]
    fn test_sst_file_path_large_number() {
        let base = PathBuf::from("/db");
        let p = sst_file_path(&base, FileNumber(123456789));
        // 6-digit padding but numbers larger than 6 digits are still valid.
        assert_eq!(p, PathBuf::from("/db/123456789.sst"));
    }

    /// R39-L1: pins the tmp-naming round-trip — the orphan-scan in
    /// `db::open_from_checkpoint` reverses this convention with the
    /// same SST_TMP_PREFIX / SST_TMP_SUFFIX constants. A future
    /// change to the prefix/suffix here (e.g. dropping the dot)
    /// must also update the scan; this test fails noisily in CI if
    /// the round-trip is broken.
    #[test]
    fn test_sst_temp_path_round_trip() {
        let final_path = PathBuf::from("/db/000042.sst");
        let tmp = sst_temp_path(&final_path);
        let tmp_name = tmp.file_name().unwrap().to_str().unwrap();
        // Writer-side naming matches "<PREFIX><basename><SUFFIX>".
        assert!(tmp_name.starts_with(SST_TMP_PREFIX));
        assert!(tmp_name.ends_with(SST_TMP_SUFFIX));
        // And matches the literal the scan currently expects.
        assert_eq!(tmp, PathBuf::from("/db/.000042.sst.tmp"));
    }

    /// R39-L1: end-to-end orphan-detection test. Create a deliberately
    /// orphaned tmp file in the db dir, then assert that
    /// `open_from_checkpoint`'s scan picks it up via the
    /// SST_TMP_PREFIX/SUFFIX-based match. Implementing the full
    /// open_from_checkpoint round-trip here is heavyweight, so we
    /// pin the predicate the scan uses — it composes the literal
    /// the writer emits, exercising the constants from the consumer
    /// direction.
    #[test]
    fn test_sst_temp_naming_detects_orphan() {
        // Writer emits this name for FileNumber(7).
        let final_path = PathBuf::from("/db/000007.sst");
        let tmp = sst_temp_path(&final_path);
        let name = tmp.file_name().unwrap().to_str().unwrap();
        // Scan predicate (mirrors db.rs orphan-scan logic).
        let sst_inner_suffix = format!(".sst{}", SST_TMP_SUFFIX);
        let inner = name
            .strip_suffix(&sst_inner_suffix)
            .expect("scan must accept writer-side suffix");
        let stem = inner
            .strip_prefix(SST_TMP_PREFIX)
            .expect("scan must accept writer-side prefix");
        let num: u64 = stem.parse().expect("inner stem must parse as u64");
        assert_eq!(num, 7);
    }
}
