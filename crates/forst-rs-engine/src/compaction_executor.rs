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

//! Remote / offloaded compaction (paper pillar 6b — `2026-06-13-remote-compaction-design.md`).
//!
//! A [`CompactionMergeExecutor`] is the *strategy* for **executing** a picked
//! [`CompactionJob`]: produce the output SST file(s) and return the metadata
//! [`VersionEdit`] for the caller to install atomically. The PICK (input
//! selection, output-level / file-number allocation, snapshot horizon) and the
//! INSTALL (`version_set.apply`) stay in `db.rs` and are identical for every
//! strategy — only the byte work (the k-way merge + output write) moves.
//!
//! - [`LocalCompactionExecutor`] — DEFAULT. Runs `job.run()` in-process on the
//!   calling background thread; byte-for-byte today's behaviour.
//! - [`RemoteEmulatedCompactionExecutor`] — the OFFLOAD path (flag-gated,
//!   default OFF). Hands the job to a dedicated worker pool; the pool thread
//!   runs the SAME `job.run()`, reading inputs and writing outputs THROUGH the
//!   job's `Arc<dyn FileSystem>` (the opendal/cached remote stack on the disagg
//!   operating mode), so the calling (TaskManager) thread does ~0 compaction
//!   CPU and ~0 compaction I/O. The result `VersionEdit` is sent back over a
//!   channel; the caller installs it exactly as before.
//!
//! [`CompactionJobDescriptor`] is the **portable unit** a true out-of-process /
//! cross-machine worker would receive (RocksDB `CompactionServiceInput`): inputs
//! by IDENTITY (file number + physical path + key/seq bounds), output level,
//! pre-allocated output file numbers, the snapshot horizon (the MVCC contract),
//! and the merge/filter semantics by NAME. It encodes to a flat little-endian
//! byte blob (the repo's checkpoint-blob convention — no serde dependency) so
//! the "describe → serialize → ship → reconstruct → execute" path is exercised
//! in-repo by the round-trip falsifier; a real transport is Phase 3.

use std::sync::mpsc;
use std::sync::Arc;

use forst_rs_common::{
    ColumnFamilyId, CompressionType, FileNumber, ForstError, ForstResult, SequenceNumber,
};
use forst_rs_io::FileSystem;
use forst_rs_storage::sst::{SstReaderImpl, SstWriterOptions};
use forst_rs_storage::version::{SstFileMeta, VersionEdit};

use crate::bg_pool::WorkerPool;
use crate::compaction::{CompactionJob, KvGcSpec};

/// Which executor strategy is in force (for diagnostics / asserts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionMergeExecutorKind {
    /// In-process, on the calling background thread (default).
    Local,
    /// Locally-emulated offload: a separate worker pool runs the merge.
    RemoteEmulated,
}

/// Strategy for executing a picked compaction job.
///
/// Implementors MUST be deterministic with respect to the job: a job executed
/// by any implementor produces a byte-identical output SST and an equal
/// [`VersionEdit`] (the byte-identical falsifier IT enforces this). The merge
/// is a pure function of (inputs, output file numbers, snapshot horizon,
/// merge/filter semantics, kv-gc spec); the only thing that varies is WHERE the
/// CPU/IO happens.
pub trait CompactionMergeExecutor: Send + Sync {
    /// Execute `job`, returning the metadata edit (or `None` if the merge
    /// produced no output — e.g. every input row was dropped). The caller
    /// installs the edit via `version_set.apply` under `apply_lock`.
    fn execute(&self, job: CompactionJob) -> ForstResult<Option<VersionEdit>>;

    fn kind(&self) -> CompactionMergeExecutorKind;
}

/// DEFAULT executor: run the merge in-process. Identical to calling
/// `job.run()` directly (zero behaviour change when the offload flag is OFF).
#[derive(Debug, Default)]
pub struct LocalCompactionExecutor;

impl CompactionMergeExecutor for LocalCompactionExecutor {
    fn execute(&self, job: CompactionJob) -> ForstResult<Option<VersionEdit>> {
        job.run()
    }

    fn kind(&self) -> CompactionMergeExecutorKind {
        CompactionMergeExecutorKind::Local
    }
}

/// Locally-emulated REMOTE executor (paper pillar 6b, flag-gated default OFF).
///
/// The job is moved to a dedicated background `WorkerPool` (so the merge CPU
/// and the input/output I/O happen OFF the calling thread); the calling thread
/// blocks on the result channel and then installs the returned edit exactly as
/// the local path does. Because the moved `CompactionJob` carries its own
/// `Arc<dyn FileSystem>`, on the disagg operating mode the pool thread reads
/// inputs and writes outputs through the (remote, opendal-emulated) FS — the
/// full remote code path minus the network. Blocking-recv is the simplest
/// faithful emulation of "the work happened elsewhere"; the offload is proven
/// by per-thread CPU accounting in the mini-bench, not by whether the caller
/// waits (a true async offload is Phase 3).
pub struct RemoteEmulatedCompactionExecutor {
    pool: Arc<WorkerPool>,
    /// When set (env `FRS_REMOTE_COMPACTION_SERIALIZE=1`), the worker round-
    /// trips the job through [`CompactionJobDescriptor`] encode→decode→rebuild
    /// before executing, exercising the full serialize path in-process. Off by
    /// default (the round-trip is covered by a dedicated UT; on the hot path it
    /// only re-opens readers the caller already opened, adding cost with no
    /// functional gain on a shared-process emulation).
    serialize_round_trip: bool,
}

impl RemoteEmulatedCompactionExecutor {
    /// Build with a dedicated single-worker offload pool (bounded background
    /// CPU; background-class so its reads are LRU-exempt — FRS-CACHE-BG-EXEMPT).
    pub fn new() -> Self {
        Self::with_workers(1)
    }

    pub fn with_workers(n: usize) -> Self {
        Self {
            pool: Arc::new(WorkerPool::new_compaction(n, "forst-rs-remote-compact")),
            serialize_round_trip: serialize_round_trip_env(),
        }
    }
}

impl Default for RemoteEmulatedCompactionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionMergeExecutor for RemoteEmulatedCompactionExecutor {
    fn execute(&self, job: CompactionJob) -> ForstResult<Option<VersionEdit>> {
        let serialize = self.serialize_round_trip;
        let (tx, rx) = mpsc::channel::<ForstResult<Option<VersionEdit>>>();
        // Move the whole (Send) job onto the offload thread. `CompactionJob`
        // is `Send` — every field is an owned value or an `Arc<dyn _>` whose
        // trait is `Send + Sync` (FileSystem / MergeOperator / CompactionFilter)
        // or `Arc<SstReaderImpl>` (shared across engine threads already).
        self.pool.submit(Box::new(move || {
            let result = if serialize {
                run_job_via_descriptor_round_trip(job)
            } else {
                job.run()
            };
            // Sender drop on a panicking job ⇒ caller observes Disconnected.
            let _ = tx.send(result);
        }));
        // Block until the offloaded merge completes. A worker panic drops `tx`
        // during unwind (bg_pool::catch_unwind), so `recv` returns
        // `Disconnected` — mapped to a hard error; nothing is installed and the
        // next compaction cycle re-picks (idempotent re-run).
        match rx.recv() {
            Ok(result) => result,
            Err(_) => Err(ForstError::internal(
                "remote-emulated compaction worker disconnected (job panicked or pool dropped); \
                 no VersionEdit produced — version left untouched, will be re-picked",
            )),
        }
    }

    fn kind(&self) -> CompactionMergeExecutorKind {
        CompactionMergeExecutorKind::RemoteEmulated
    }
}

/// FRS_REMOTE_COMPACTION_SERIALIZE — round-trip every offloaded job through the
/// serializable descriptor before executing (CI exercise of the full transport
/// path in one process). Default OFF.
fn serialize_round_trip_env() -> bool {
    matches!(
        std::env::var("FRS_REMOTE_COMPACTION_SERIALIZE")
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Run a job by first projecting it to a [`CompactionJobDescriptor`], encoding
/// to bytes, decoding, and rebuilding a fresh `CompactionJob` (reopening input
/// readers from their physical paths through the same FS). This is the
/// in-process stand-in for "ship the descriptor to a remote worker, which
/// reconstructs and runs it". Used by the optional serialize round-trip path
/// and by the round-trip falsifier UT.
fn run_job_via_descriptor_round_trip(job: CompactionJob) -> ForstResult<Option<VersionEdit>> {
    let fs = job.fs.clone();
    let merge_operator = job.merge_operator.clone();
    let compaction_filter = job.compaction_filter.clone();
    let desc = CompactionJobDescriptor::from_job(&job);
    drop(job); // release the original readers; the worker reopens its own.
    let bytes = desc.encode();
    let decoded = CompactionJobDescriptor::decode(&bytes)?;
    let rebuilt = decoded.rebuild(fs, merge_operator, compaction_filter)?;
    rebuilt.run()
}

// ---------------------------------------------------------------------------
// Portable job descriptor (RocksDB CompactionServiceInput analogue)
// ---------------------------------------------------------------------------

/// One input file, by IDENTITY (no reader handle). A remote worker opens its
/// own reader over `physical_path` through the shared DFS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionInputFile {
    pub level: u32,
    pub meta: SstFileMeta,
    /// Physical path/key of the input on the (shared) FS — what the worker
    /// opens. In the working dir this equals the SST path for `meta`.
    pub physical_path: String,
}

/// The serializable description of a compaction job — the portable unit a true
/// out-of-process worker would receive (`2026-06-13-remote-compaction-design.md`
/// §3.2). Holds everything needed to reconstruct and run the merge EXCEPT the
/// live trait objects (FS / merge operator / compaction filter), which the
/// worker re-binds locally (FS injected; merge operator looked up by name via
/// [`forst_rs_storage::merge_operator::merge_operator_by_name`]; filter
/// supplied by the caller — there is no global filter registry, matching
/// RocksDB which ships filter factory config in cf options).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionJobDescriptor {
    pub cf_id: ColumnFamilyId,
    pub inputs: Vec<CompactionInputFile>,
    pub output_level: u32,
    pub output_file_number: FileNumber,
    pub output_path: String,
    pub additional_outputs: Vec<(FileNumber, String)>,
    pub target_file_size: u64,
    pub block_size: u64,
    pub compression: u8,
    pub is_bottommost: bool,
    /// THE MVCC CONTRACT: the worker drops a version iff `seq <
    /// min_active_snapshot` AND a newer version exists — identical to the
    /// local path (`mvcc::should_drop`).
    pub min_active_snapshot: SequenceNumber,
    /// Merge operator by name (empty = none). Reconstructed via the registry.
    pub merge_operator_name: String,
    /// `true` iff the source job carried a compaction filter — the worker MUST
    /// be supplied an equivalent filter or the descriptor is not portable for
    /// this job (decode preserves the flag; `rebuild` requires the caller pass
    /// the same filter when set).
    pub has_compaction_filter: bool,
    /// WA-V2b vlog GC directive (encoded inline; `None` = no GC).
    pub kv_gc: Option<KvGcSpec>,
}

const DESC_MAGIC: u32 = 0x5243_4A44; // "RCJD"
const DESC_VERSION: u16 = 1;

impl CompactionJobDescriptor {
    /// Project a live job to its portable descriptor (identity-only inputs).
    pub fn from_job(job: &CompactionJob) -> Self {
        let inputs = job
            .inputs
            .iter()
            .map(|(level, meta, _reader)| CompactionInputFile {
                level: *level,
                meta: meta.clone(),
                physical_path: sst_path_for(&job.output_path, meta.file_number),
            })
            .collect();
        Self {
            cf_id: job.cf_id,
            inputs,
            output_level: job.output_level,
            output_file_number: job.output_file_number,
            output_path: path_to_string(&job.output_path),
            additional_outputs: job
                .additional_outputs
                .iter()
                .map(|(n, p)| (*n, path_to_string(p)))
                .collect(),
            target_file_size: job.target_file_size,
            block_size: job.writer_options.block_size as u64,
            compression: job.writer_options.compression as u8,
            is_bottommost: job.is_bottommost,
            min_active_snapshot: job.min_active_snapshot,
            merge_operator_name: job
                .merge_operator
                .as_ref()
                .map(|m| m.name())
                .unwrap_or_default(),
            has_compaction_filter: job.compaction_filter.is_some(),
            kv_gc: job.kv_gc.clone(),
        }
    }

    /// Reconstruct a runnable [`CompactionJob`] from this descriptor: reopen a
    /// reader over each input's physical path through `fs`. `merge_operator` /
    /// `compaction_filter` are re-bound by the caller (the worker side):
    /// `merge_operator` MUST match `merge_operator_name` (looked up via the
    /// registry when not supplied), and a `compaction_filter` MUST be supplied
    /// when `has_compaction_filter`.
    pub fn rebuild(
        self,
        fs: Arc<dyn FileSystem>,
        merge_operator: Option<Arc<dyn forst_rs_storage::merge_operator::MergeOperator>>,
        compaction_filter: Option<Arc<dyn crate::compaction_filter::CompactionFilter>>,
    ) -> ForstResult<CompactionJob> {
        if self.has_compaction_filter && compaction_filter.is_none() {
            return Err(ForstError::invalid_argument(
                "CompactionJobDescriptor::rebuild: descriptor carries a compaction filter but \
                 none was supplied — the remote worker cannot run the job faithfully",
            ));
        }
        // Re-bind the merge operator: prefer the supplied instance; else look
        // it up by name from the registry (the remote-worker path).
        let merge_operator = match merge_operator {
            Some(m) => Some(m),
            None if !self.merge_operator_name.is_empty() => Some(
                forst_rs_storage::merge_operator::merge_operator_by_name(&self.merge_operator_name)
                    .ok_or_else(|| {
                        ForstError::invalid_argument(format!(
                            "CompactionJobDescriptor::rebuild: unknown merge operator {:?}",
                            self.merge_operator_name
                        ))
                    })?,
            ),
            None => None,
        };
        let mut inputs: Vec<(u32, SstFileMeta, Arc<SstReaderImpl>)> =
            Vec::with_capacity(self.inputs.len());
        for inf in self.inputs {
            let file = fs.open_random_access_file(std::path::Path::new(&inf.physical_path))?;
            let reader = Arc::new(SstReaderImpl::open(file)?);
            inputs.push((inf.level, inf.meta, reader));
        }
        Ok(CompactionJob {
            cf_id: self.cf_id,
            inputs,
            output_level: self.output_level,
            output_file_number: self.output_file_number,
            output_path: std::path::PathBuf::from(self.output_path),
            additional_outputs: self
                .additional_outputs
                .into_iter()
                .map(|(n, p)| (n, std::path::PathBuf::from(p)))
                .collect(),
            target_file_size: self.target_file_size,
            writer_options: SstWriterOptions {
                block_size: self.block_size as usize,
                compression: compression_from_u8(self.compression),
                cf_id: self.cf_id,
            },
            fs,
            merge_operator,
            compaction_filter,
            is_bottommost: self.is_bottommost,
            min_active_snapshot: self.min_active_snapshot,
            kv_gc: self.kv_gc,
        })
    }

    /// Encode to a flat little-endian blob (the repo's no-serde convention).
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&DESC_MAGIC.to_le_bytes());
        b.extend_from_slice(&DESC_VERSION.to_le_bytes());
        b.extend_from_slice(&self.cf_id.0.to_le_bytes());
        put_u32(&mut b, self.inputs.len() as u32);
        for inf in &self.inputs {
            b.extend_from_slice(&inf.level.to_le_bytes());
            encode_meta(&mut b, &inf.meta);
            put_str(&mut b, &inf.physical_path);
        }
        b.extend_from_slice(&self.output_level.to_le_bytes());
        b.extend_from_slice(&self.output_file_number.0.to_le_bytes());
        put_str(&mut b, &self.output_path);
        put_u32(&mut b, self.additional_outputs.len() as u32);
        for (n, p) in &self.additional_outputs {
            b.extend_from_slice(&n.0.to_le_bytes());
            put_str(&mut b, p);
        }
        b.extend_from_slice(&self.target_file_size.to_le_bytes());
        b.extend_from_slice(&self.block_size.to_le_bytes());
        b.push(self.compression);
        b.push(self.is_bottommost as u8);
        b.extend_from_slice(&self.min_active_snapshot.0.to_le_bytes());
        put_str(&mut b, &self.merge_operator_name);
        b.push(self.has_compaction_filter as u8);
        match &self.kv_gc {
            None => b.push(0),
            Some(spec) => {
                b.push(1);
                let mut segs: Vec<u64> = spec.relocate.iter().copied().collect();
                segs.sort_unstable();
                put_u32(&mut b, segs.len() as u32);
                for s in segs {
                    b.extend_from_slice(&s.to_le_bytes());
                }
                b.extend_from_slice(&spec.output_segment_id.0.to_le_bytes());
                put_str(&mut b, &path_to_string(&spec.db_dir));
                // FRS-WA-V2c: relocation output codec (self-describing records).
                b.push(spec.vlog_compression as u8);
            }
        }
        b
    }

    /// Decode a blob produced by [`Self::encode`].
    pub fn decode(buf: &[u8]) -> ForstResult<Self> {
        let mut c = Cursor { buf, pos: 0 };
        let magic = c.u32()?;
        if magic != DESC_MAGIC {
            return Err(ForstError::corruption(
                "CompactionJobDescriptor::decode: bad magic",
            ));
        }
        let version = c.u16()?;
        if version != DESC_VERSION {
            return Err(ForstError::corruption(format!(
                "CompactionJobDescriptor::decode: unsupported version {version}"
            )));
        }
        let cf_id = ColumnFamilyId(c.u32()?);
        let n_inputs = c.u32()? as usize;
        let mut inputs = Vec::with_capacity(n_inputs);
        for _ in 0..n_inputs {
            let level = c.u32()?;
            let meta = decode_meta(&mut c)?;
            let physical_path = c.string()?;
            inputs.push(CompactionInputFile {
                level,
                meta,
                physical_path,
            });
        }
        let output_level = c.u32()?;
        let output_file_number = FileNumber(c.u64()?);
        let output_path = c.string()?;
        let n_add = c.u32()? as usize;
        let mut additional_outputs = Vec::with_capacity(n_add);
        for _ in 0..n_add {
            let n = FileNumber(c.u64()?);
            let p = c.string()?;
            additional_outputs.push((n, p));
        }
        let target_file_size = c.u64()?;
        let block_size = c.u64()?;
        let compression = c.u8()?;
        let is_bottommost = c.u8()? != 0;
        let min_active_snapshot = SequenceNumber(c.u64()?);
        let merge_operator_name = c.string()?;
        let has_compaction_filter = c.u8()? != 0;
        let kv_gc = match c.u8()? {
            0 => None,
            _ => {
                let n_seg = c.u32()? as usize;
                let mut relocate = std::collections::HashSet::with_capacity(n_seg);
                for _ in 0..n_seg {
                    relocate.insert(c.u64()?);
                }
                let output_segment_id = FileNumber(c.u64()?);
                let db_dir = std::path::PathBuf::from(c.string()?);
                let vlog_compression = match c.u8()? {
                    0 => CompressionType::None,
                    1 => CompressionType::Lz4,
                    2 => CompressionType::Zstd,
                    b => {
                        return Err(ForstError::corruption(format!(
                            "compaction descriptor: invalid vlog_compression byte {b}"
                        )))
                    }
                };
                Some(KvGcSpec {
                    relocate,
                    output_segment_id,
                    db_dir,
                    vlog_compression,
                })
            }
        };
        Ok(Self {
            cf_id,
            inputs,
            output_level,
            output_file_number,
            output_path,
            additional_outputs,
            target_file_size,
            block_size,
            compression,
            is_bottommost,
            min_active_snapshot,
            merge_operator_name,
            has_compaction_filter,
            kv_gc,
        })
    }
}

// --- encode/decode helpers (little-endian, length-prefixed) ----------------

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn put_str(b: &mut Vec<u8>, s: &str) {
    put_u32(b, s.len() as u32);
    b.extend_from_slice(s.as_bytes());
}

fn put_bytes(b: &mut Vec<u8>, s: &[u8]) {
    put_u32(b, s.len() as u32);
    b.extend_from_slice(s);
}

fn encode_meta(b: &mut Vec<u8>, m: &SstFileMeta) {
    b.extend_from_slice(&m.file_number.0.to_le_bytes());
    b.extend_from_slice(&m.cf_id.0.to_le_bytes());
    b.extend_from_slice(&m.file_size.to_le_bytes());
    put_bytes(b, &m.smallest_key);
    put_bytes(b, &m.largest_key);
    b.extend_from_slice(&m.min_sequence.0.to_le_bytes());
    b.extend_from_slice(&m.max_sequence.0.to_le_bytes());
    b.extend_from_slice(&m.num_entries.to_le_bytes());
    b.extend_from_slice(&m.max_death.to_le_bytes());
}

fn decode_meta(c: &mut Cursor<'_>) -> ForstResult<SstFileMeta> {
    Ok(SstFileMeta {
        file_number: FileNumber(c.u64()?),
        cf_id: ColumnFamilyId(c.u32()?),
        file_size: c.u64()?,
        smallest_key: c.bytes()?,
        largest_key: c.bytes()?,
        min_sequence: SequenceNumber(c.u64()?),
        max_sequence: SequenceNumber(c.u64()?),
        num_entries: c.u64()?,
        max_death: c.u64()?,
    })
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn take(&mut self, n: usize) -> ForstResult<&[u8]> {
        if self.pos + n > self.buf.len() {
            return Err(ForstError::corruption(
                "CompactionJobDescriptor::decode: truncated blob",
            ));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> ForstResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> ForstResult<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> ForstResult<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> ForstResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> ForstResult<Vec<u8>> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    fn string(&mut self) -> ForstResult<String> {
        let v = self.bytes()?;
        String::from_utf8(v)
            .map_err(|_| ForstError::corruption("CompactionJobDescriptor::decode: bad utf8 string"))
    }
}

fn path_to_string(p: &std::path::Path) -> String {
    p.to_string_lossy().into_owned()
}

fn compression_from_u8(v: u8) -> CompressionType {
    match v {
        0 => CompressionType::None,
        2 => CompressionType::Zstd,
        _ => CompressionType::Lz4,
    }
}

/// The SST path for a given file number, derived from a sibling output path's
/// directory (compaction inputs and outputs share the working dir). Uses the
/// canonical [`crate::flush::sst_file_path`] naming so the path a remote worker
/// reconstructs matches what the engine wrote.
fn sst_path_for(sibling: &std::path::Path, file_number: FileNumber) -> String {
    let dir = sibling
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    path_to_string(&crate::flush::sst_file_path(dir, file_number))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta(fnum: u64) -> SstFileMeta {
        SstFileMeta {
            file_number: FileNumber(fnum),
            cf_id: ColumnFamilyId(7),
            file_size: 4096,
            smallest_key: b"aaa".to_vec(),
            largest_key: b"zzz".to_vec(),
            min_sequence: SequenceNumber(10),
            max_sequence: SequenceNumber(20),
            num_entries: 100,
            max_death: 0,
        }
    }

    fn sample_descriptor(with_gc: bool) -> CompactionJobDescriptor {
        CompactionJobDescriptor {
            cf_id: ColumnFamilyId(7),
            inputs: vec![
                CompactionInputFile {
                    level: 0,
                    meta: sample_meta(11),
                    physical_path: "/db/000011.sst".to_string(),
                },
                CompactionInputFile {
                    level: 1,
                    meta: sample_meta(12),
                    physical_path: "/db/000012.sst".to_string(),
                },
            ],
            output_level: 1,
            output_file_number: FileNumber(20),
            output_path: "/db/000020.sst".to_string(),
            additional_outputs: vec![(FileNumber(21), "/db/000021.sst".to_string())],
            target_file_size: 64 * 1024 * 1024,
            block_size: 64 * 1024,
            compression: 1,
            is_bottommost: true,
            min_active_snapshot: SequenceNumber(15),
            merge_operator_name: "RawConcatMergeOperator".to_string(),
            has_compaction_filter: false,
            kv_gc: if with_gc {
                let mut relocate = std::collections::HashSet::new();
                relocate.insert(3u64);
                relocate.insert(5u64);
                Some(KvGcSpec {
                    relocate,
                    output_segment_id: FileNumber(99),
                    db_dir: std::path::PathBuf::from("/db"),
                    vlog_compression: forst_rs_common::CompressionType::Lz4,
                })
            } else {
                None
            },
        }
    }

    /// The descriptor is a LOSSLESS portable unit: encode→decode is the
    /// identity. This is the serialization half of the portability falsifier
    /// (the byte-identical-output half is an engine IT).
    #[test]
    fn descriptor_encode_decode_round_trip_is_identity() {
        for with_gc in [false, true] {
            let desc = sample_descriptor(with_gc);
            let bytes = desc.encode();
            let decoded = CompactionJobDescriptor::decode(&bytes).expect("decode");
            assert_eq!(desc, decoded, "round-trip must be lossless (gc={with_gc})");
            // Re-encode the decoded value: byte-identical (canonical form).
            assert_eq!(bytes, decoded.encode(), "encoding must be canonical");
        }
    }

    #[test]
    fn descriptor_decode_rejects_bad_magic_and_truncation() {
        let bytes = sample_descriptor(false).encode();
        // Corrupt magic.
        let mut bad = bytes.clone();
        bad[0] ^= 0xFF;
        assert!(CompactionJobDescriptor::decode(&bad).is_err(), "bad magic");
        // Truncate.
        assert!(
            CompactionJobDescriptor::decode(&bytes[..bytes.len() - 4]).is_err(),
            "truncation must error, not panic"
        );
        assert!(CompactionJobDescriptor::decode(&[]).is_err(), "empty");
    }

    #[test]
    fn local_executor_kind() {
        assert_eq!(
            LocalCompactionExecutor.kind(),
            CompactionMergeExecutorKind::Local
        );
    }

    #[test]
    fn remote_executor_kind() {
        assert_eq!(
            RemoteEmulatedCompactionExecutor::new().kind(),
            CompactionMergeExecutorKind::RemoteEmulated
        );
    }
}
