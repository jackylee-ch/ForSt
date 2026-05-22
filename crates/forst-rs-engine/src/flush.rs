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

use arrow::array::{Array, BinaryArray, UInt64Array, UInt8Array};
use forst_rs_common::{FileNumber, ForstError, ForstResult, SequenceNumber};
use forst_rs_io::{FileSystem, WriteMode};
use forst_rs_storage::sst::{SstFileInfo, SstWriterImpl, SstWriterOptions};
use forst_rs_storage::version::SstFileMeta;

use crate::column_family::{ColumnFamilyData, SharedMemTable};

/// Batch size used when converting memtable rows to SST entries. Larger
/// batches reduce per-row overhead but increase peak memory usage during
/// flush. 8192 matches the default Arrow batch size across the project.
const FLUSH_BATCH_SIZE: usize = 8192;

/// A single flush operation: one frozen memtable → one SST file.
pub struct FlushJob {
    memtable: SharedMemTable,
    file_number: FileNumber,
    file_path: PathBuf,
    options: SstWriterOptions,
    fs: Arc<dyn FileSystem>,
}

impl FlushJob {
    /// Constructs a new flush job. The `file_path` must be an absolute or
    /// engine-relative path at which the SST file will be written. The
    /// memtable must already be frozen.
    pub fn new(
        memtable: SharedMemTable,
        file_number: FileNumber,
        file_path: PathBuf,
        options: SstWriterOptions,
        fs: Arc<dyn FileSystem>,
    ) -> Self {
        Self {
            memtable,
            file_number,
            file_path,
            options,
            fs,
        }
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
        let tmp_path = self.temp_path();
        let info = {
            let mut writable = self
                .fs
                .open_writable_file(&tmp_path, WriteMode::CreateNew)?;
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
                    .ok_or_else(|| ForstError::corruption("flush batch: value column not Binary"))?;
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
                    .ok_or_else(|| ForstError::corruption("flush batch: op_type column not UInt8"))?;

                for i in 0..rows {
                    let key = keys.value(i);
                    let value = if values.is_null(i) {
                        None
                    } else {
                        Some(values.value(i))
                    };
                    let seq = seqs.value(i);
                    let op = ops.value(i);
                    writer.add(key, value, seq, op)?;
                }
            }

            let info = writer.finish()?;
            writable.flush()?;
            writable.sync()?;
            info
        };
        self.fs.rename(&tmp_path, &self.file_path)?;

        // 4. Build the SstFileMeta that the VersionSet will record.
        Ok(Self::info_to_meta(self.file_number, info))
    }

    fn temp_path(&self) -> PathBuf {
        let mut base = self.file_path.clone();
        let existing = base
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        base.set_file_name(format!(".{}.tmp", existing));
        base
    }

    fn info_to_meta(file_number: FileNumber, info: SstFileInfo) -> SstFileMeta {
        SstFileMeta {
            file_number,
            file_size: info.file_size,
            smallest_key: info.min_key,
            largest_key: info.max_key,
            min_sequence: SequenceNumber(info.min_sequence),
            max_sequence: SequenceNumber(info.max_sequence),
            num_entries: info.entry_count,
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
pub(crate) struct FlushQueue {
    tx: SyncSender<FlushRequest>,
    rx: Mutex<Option<Receiver<FlushRequest>>>,
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_common::{CompressionType, OpType};
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
            path.clone(),
            default_writer_opts(),
            fs.clone(),
        );
        let meta = job.run().unwrap();
        assert_eq!(meta.file_number, FileNumber(7));
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
}
