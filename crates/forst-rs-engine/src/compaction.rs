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

//! Compaction pipeline. See `2.6_compaction_design.md`.
//!
//! [`CompactionJob`] merges a set of input SST files into a single output
//! SST file, consolidating multiple versions per key, applying bottommost
//! tombstone elimination, and resolving merge chains when a merge operator
//! is available. This is the core building block for both the L0 rollup
//! and L1..Ln size-tiered compactions.
//!
//! The minimal W15 flow exposed via [`crate::DbImpl::compact_l0`] picks ALL L0
//! files plus any overlapping L1 files and produces a single new L1 file
//! containing the resolved state for every key in the input range.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// FRS-WAMP phase split (2026-06-05): cumulative ns spent in the GATHER (per-row
// key/value to_vec into Vec<CompactionEntry>) and SORT phases of CompactionJob,
// summed across all compactions. db.rs's wamp line reads these and derives
// emit_ms = run_ms − gather − sort, so we can attribute the live 21 ns/byte to
// intrinsic alloc/encode vs external cache/bandwidth contention before deciding
// whether a zero-copy streaming merge is worth building. Always-accumulated
// (Instant::now is cheap); only READ when FRS_WAMP_FILE is set.
pub(crate) static CUM_GATHER_NS: AtomicU64 = AtomicU64::new(0);
pub(crate) static CUM_SORT_NS: AtomicU64 = AtomicU64::new(0);

use forst_rs_common::{ColumnFamilyId, FileNumber, ForstError, ForstResult, SequenceNumber};
use forst_rs_io::{FileSystem, WritableFile};
use forst_rs_storage::merge_operator::MergeOperator;
use forst_rs_storage::sst::{
    writer::StreamingSstWriter, SstBlockCursor, SstFileInfo, SstReaderImpl, SstWriterImpl,
    SstWriterOptions,
};
use forst_rs_storage::version::{SstFileMeta, VersionEdit};

use crate::compaction_filter::{CompactionDecision, CompactionFilter};
use crate::flush::{sst_file_path, sst_temp_path};
use crate::mvcc;

/// Description of a single compaction task: merge `inputs` into a new file
/// `output_file_number` placed at level `output_level`.
pub struct CompactionJob {
    /// R49-H1: column family this compaction belongs to. Stamped onto the
    /// output `SstFileMeta` and the SST footer so per-CF SST isolation is
    /// enforced through compaction as well as flush. Every input meta must
    /// agree with this cf_id (debug-asserted on entry).
    pub cf_id: ColumnFamilyId,
    pub inputs: Vec<(u32 /* level */, SstFileMeta, Arc<SstReaderImpl>)>,
    pub output_level: u32,
    pub output_file_number: FileNumber,
    pub output_path: PathBuf,
    /// FRS-LEVELED-COMPACTION (2026-06-04): additional pre-allocated output
    /// slots `(file_number, path)` for splitting the compaction output into
    /// multiple ~`target_file_size` SSTs. Empty ⇒ single-file output (the
    /// legacy behaviour). The caller (db.rs) allocates these from the
    /// VersionSet (file-number allocation lives there) sized to
    /// `ceil(total_input_bytes / target_file_size) + margin`; the job uses as
    /// many as splitting requires and silently leaves the rest unused (the
    /// file numbers are consumed from the monotonic allocator but no on-disk
    /// file is created for them, so there is nothing to clean up).
    pub additional_outputs: Vec<(FileNumber, PathBuf)>,
    /// FRS-LEVELED-COMPACTION: roll to the next output slot once the in-flight
    /// SST's estimated size reaches this many bytes, splitting ONLY at
    /// user-key boundaries so every version of a key lands in exactly one
    /// file (point-get / scan correctness). `0` disables splitting — the
    /// output is a single file, byte-for-byte the legacy behaviour. The
    /// final available slot is uncapped, so the job can never run out of
    /// slots (worst case the last file is larger than target).
    pub target_file_size: u64,
    pub writer_options: SstWriterOptions,
    pub fs: Arc<dyn FileSystem>,
    pub merge_operator: Option<Arc<dyn MergeOperator>>,
    pub compaction_filter: Option<Arc<dyn CompactionFilter>>,
    /// `true` when the output file lives at the bottommost level — Delete
    /// tombstones can be eliminated because no older SST contains data for
    /// the key.
    pub is_bottommost: bool,
    /// Smallest sequence number held by any live snapshot at the moment
    /// this job was constructed (snapshot of
    /// [`crate::mvcc::SnapshotRegistry::min_active`]). Versions with
    /// `seq >= min_active_snapshot` are pinned for snapshot reads and
    /// must be retained verbatim — see [`crate::mvcc::should_drop`] and
    /// spec §6a.5. Pass `SequenceNumber(u64::MAX)` when there are no
    /// snapshots, which lets the consolidation logic run unconstrained.
    pub min_active_snapshot: SequenceNumber,
}

impl CompactionJob {
    /// Runs the compaction synchronously and returns a [`VersionEdit`] that
    /// the caller should apply atomically to the [`forst_rs_storage::version::VersionSetImpl`].
    ///
    /// PR-D2 (Z3-10, C-R3-H1..3): the output SST is streamed directly to
    /// the temp file via [`SstWriterImpl::streaming`] — no full-SST
    /// `Vec<u8>` is materialised. Inputs are read block-by-block through
    /// [`SstReaderImpl::scan_borrowed`], which exposes Arrow-backed zero-
    /// copy [`forst_rs_storage::sst::RowView`]s; the per-entry copy into
    /// `CompactionEntry` is the single materialisation point (unavoidable
    /// because the merge sort outlives each individual block's
    /// `RecordBatch`).
    pub fn run(self) -> ForstResult<Option<VersionEdit>> {
        // FRS-ZERO-COPY-MERGE (2026-06-05): the DEFAULT path is the streaming
        // k-way merge (`run_streaming`) — it replaces the alloc-heavy global
        // `gather Vec<CompactionEntry>` + `sort` with a heap over per-input
        // pull cursors, materializing only one tiny per-key version group at a
        // time. Output is byte-equivalent (same per-key groups → same
        // `emit_key_versions`). The legacy gather-sort body below is retained
        // ONLY for the opt-in (and previously refuted) parallel sub-compaction
        // (`FRS_COMPACT_PARALLEL`), which random-accesses the materialized
        // vector by index range and so cannot stream.
        if !compaction_parallel_env() {
            return self.run_streaming();
        }

        // R49-H1 defense-in-depth: every input must already belong to the
        // same CF as the compaction job. The engine-side caller (db.rs)
        // builds compaction inputs by reading one CF's file list, so a
        // cross-CF input here would be a structural bug — we trip a debug
        // assertion and continue in release. Once the engine wires per-CF
        // version lists this check becomes a hard error.
        for (_, meta, _) in &self.inputs {
            debug_assert_eq!(
                meta.cf_id,
                self.cf_id,
                "CompactionJob cf_id {:?} ≠ input file {} cf_id {:?} — cross-CF input",
                self.cf_id,
                meta.file_number.value(),
                meta.cf_id,
            );
        }

        // 1. Gather every entry from every input SST, tagging each with the
        //    source file_number so we can break ties when two SSTs use the
        //    same memtable-local sequence (each VectorizedMemTable starts
        //    counting seqs from 1).
        let _t_gather = std::time::Instant::now();
        let mut all: Vec<CompactionEntry> = Vec::new();
        for (level, meta, reader) in &self.inputs {
            let _ = level;
            let file_num = meta.file_number.value();
            // Stream via `scan_borrowed` — no `Vec<u8>` per row inside the
            // reader. The owned copy is paid here, ONCE per entry, into the
            // sort buffer (which has to be owned to outlive the block).
            reader.scan_borrowed(&meta.smallest_key, None, |view| {
                all.push(CompactionEntry {
                    key: view.key.to_vec(),
                    value: view.value.map(|v| v.to_vec()),
                    sequence: view.sequence,
                    op_type: view.op_type,
                    file_number: file_num,
                });
                Ok(())
            })?;
        }
        CUM_GATHER_NS.fetch_add(_t_gather.elapsed().as_nanos() as u64, Ordering::Relaxed);

        if all.is_empty() {
            // Nothing to merge — still produce a VersionEdit that deletes
            // the (empty) inputs. Callers can use this to clear out stray
            // L0 files that contained only bottommost tombstones.
            let deleted = self
                .inputs
                .iter()
                .map(|(lvl, m, _)| (*lvl, m.file_number))
                .collect();
            // R0A-H1: preserve the input files' max sequence so the
            // version-set high-water mark does not regress after a
            // compaction that drops every entry. Without this, a
            // checkpoint taken right after such a compaction would
            // initialize the restored engine's `sequence_number` from a
            // stale value, allowing later writes to reuse seqs of any
            // SSTs that survived. We compute the max across all inputs.
            let max_in_seq = self
                .inputs
                .iter()
                .map(|(_, m, _)| m.max_sequence.value())
                .max()
                .unwrap_or(0);
            let last_seq = if max_in_seq > 0 {
                Some(forst_rs_common::SequenceNumber(max_in_seq))
            } else {
                None
            };
            return Ok(Some(VersionEdit {
                deleted_files: deleted,
                new_files: Vec::new(),
                next_file_number: None,
                last_sequence: last_seq,
            }));
        }

        // 2. Sort by (key ASC, effective_sequence DESC). The effective
        //    sequence uses file_number as the high bits so a newer SST file
        //    wins over an older one even if they share a memtable-local seq.
        let _t_sort = std::time::Instant::now();
        all.sort_by(|a, b| match a.key.cmp(&b.key) {
            std::cmp::Ordering::Equal => b.effective_seq().cmp(&a.effective_seq()),
            ord => ord,
        });
        CUM_SORT_NS.fetch_add(_t_sort.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // 3. Walk keys, consolidating versions per key, and stream the
        //    output SST directly to the temp file via the streaming writer.
        //    We apply:
        //    - Delete tombstones: drop all older versions for the same key;
        //      emit the tombstone only if NOT bottommost.
        //    - Merge chains: if a merge operator is present, collapse via
        //      full_merge once we reach the Put base (or exhaust the chain).
        //
        // PR-D2: the output file is streamed block-by-block — peak memory
        // is bounded by one in-flight Arrow data block plus the bloom +
        // sparse-index sections (proportional to block count, not byte
        // count). No full-SST `Vec<u8>` is allocated.

        // FRS-S3-SSTRENAME: object stores have no atomic rename, so stream each
        // compacted SST straight to its final key (multipart upload publishes
        // atomically on close). Local FS keeps the temp→rename convention for
        // crash-atomic publication. Mirrors the flush.rs branch.
        let atomic_rename = self.fs.supports_atomic_rename();
        // FRS-S3-ORPHAN-FIX (sister to flush.rs): on object stores the output
        // streams to its FINAL path; a stale orphan from a crashed attempt with
        // the same file number is overwritten (CreateOrTruncate). Local FS keeps
        // CreateNew on the temp path for crash-atomic publication.
        let write_mode = if atomic_rename {
            forst_rs_io::WriteMode::CreateNew
        } else {
            forst_rs_io::WriteMode::CreateOrTruncate
        };
        if let Some(parent) = self.output_path.parent() {
            self.fs.create_dir_all(parent)?;
        }

        // FRS-LEVELED-COMPACTION (2026-06-04): output slots — slot 0 is the
        // primary (output_file_number/output_path), followed by any caller-
        // pre-allocated additional slots. The output is split into multiple
        // ~`target_file_size` SSTs, rolling to the next slot ONLY at a user-key
        // boundary so every version of a key lands in exactly one file
        // (point-get / scan correctness). The final available slot is uncapped,
        // so we can never run out of slots — worst case the last file exceeds
        // target. `target_file_size == 0` ⇒ single file (legacy behaviour).
        let mut slots: Vec<(FileNumber, PathBuf)> =
            Vec::with_capacity(1 + self.additional_outputs.len());
        slots.push((self.output_file_number, self.output_path.clone()));
        slots.extend(self.additional_outputs.iter().cloned());
        let target = self.target_file_size;

        // R40-M1 (multi-file): each produced entry is a finished, published SST
        // `(file_number, info)`. Any `?` error inside the writer flow propagates
        // after a best-effort tmp cleanup of the in-flight file; files already
        // published from earlier slots are referenced in the returned
        // VersionEdit's `new_files`, which the caller deletes on a failed apply
        // (and the restore orphan-scan sweeps any residue on restart).
        // FRS-COMPACT-PARALLEL (2026-06-05, default ON; opt out =0): emit the
        // output SSTs CONCURRENTLY across cores instead of one-at-a-time. The
        // single-threaded merge left the Mac's other cores idle during a 25 s
        // L1→L2 burst while the foreground starved (the q4 trough). Partition
        // the sorted `all` into `slots.len()` contiguous, key-boundary-aligned
        // ranges and emit each to its own non-overlapping output SST on a
        // separate thread (std::thread::scope). Each partition reuses the exact
        // serial emit (`emit_one_sst` → `emit_key_versions`), so output is
        // byte-equivalent; partitions are disjoint key ranges ⇒ no shared
        // mutable state. Burst wall-time ≈ serial/N.
        // OPT-IN (FRS_COMPACT_PARALLEL=1) while validating: the partitioning is
        // by row-count into slots.len() files, which is data-correct (disjoint
        // key ranges, all versions preserved) but does NOT yet byte-match the
        // serial size-based file layout, so it stays off-by-default until an
        // A/B + the layout-equivalence is settled. Default = serial.
        let parallel = slots.len() > 1
            && all.len() > slots.len()
            && matches!(
                std::env::var("FRS_COMPACT_PARALLEL").ok().as_deref(),
                Some("1") | Some("true") | Some("TRUE")
            );
        let produced: Vec<(FileNumber, SstFileInfo)> = if parallel {
            // Partition by ENCODED-SIZE (≈ key+value+overhead bytes ≥ target),
            // snapped to the next user-key boundary — matching the serial slot
            // path's `estimated_size >= target` rolling, so the parallel output
            // has the SAME file count/boundaries as serial (no file-count
            // explosion → no extra downstream compaction). Cap partitions at
            // slots.len() (the final partition takes the remainder).
            let n = slots.len();
            let total = all.len();
            let part_target = if target > 0 { target } else { u64::MAX };
            let mut bounds: Vec<usize> = vec![0];
            let mut acc: u64 = 0;
            let mut i = 0usize;
            while i < total && bounds.len() < n {
                acc +=
                    (all[i].key.len() + all[i].value.as_ref().map_or(0, |v| v.len()) + 16) as u64;
                i += 1;
                if acc >= part_target && i < total {
                    // snap to the next user-key boundary
                    while i < total && all[i].key == all[i - 1].key {
                        i += 1;
                    }
                    if i >= total {
                        break;
                    }
                    bounds.push(i);
                    acc = 0;
                }
            }
            bounds.push(total);
            // Contiguous, key-boundary-aligned (range → slot) pairs. Each range
            // gets slot[pi]; ranges are disjoint key intervals.
            let ranges: Vec<(usize, usize)> = bounds
                .windows(2)
                .filter(|w| w[0] < w[1])
                .map(|w| (w[0], w[1]))
                .collect();
            let all_ref = &all;
            let this: &CompactionJob = &self;
            let results: Vec<ForstResult<Option<(FileNumber, SstFileInfo)>>> =
                std::thread::scope(|scope| {
                    let handles: Vec<_> = ranges
                        .iter()
                        .enumerate()
                        .map(|(pi, &(s, e))| {
                            let (fnum, path) = slots[pi].clone();
                            scope.spawn(move || {
                                this.emit_one_sst(
                                    all_ref,
                                    s..e,
                                    fnum,
                                    &path,
                                    atomic_rename,
                                    write_mode,
                                )
                            })
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|h| {
                            h.join().unwrap_or_else(|_| {
                                Err(ForstError::corruption(
                                    "compaction partition thread panicked",
                                ))
                            })
                        })
                        .collect()
                });
            let mut produced: Vec<(FileNumber, SstFileInfo)> = Vec::new();
            let mut first_err: Option<ForstError> = None;
            for r in results {
                match r {
                    Ok(Some(x)) => produced.push(x),
                    Ok(None) => {}
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
            if let Some(e) = first_err {
                // Best-effort cleanup: every partition's output file is not yet
                // referenced by any Version, so delete any that were written.
                for (_, path) in &slots[..ranges.len()] {
                    let _ = self.fs.delete_file(path);
                    let _ = self.fs.delete_file(&sst_temp_path(path));
                }
                return Err(e);
            }
            // Shared apply + reader-open finalize runs below with this produced.
            produced
        } else {
            let write_outcome: ForstResult<Vec<(FileNumber, SstFileInfo)>> = (|| {
                let mut produced: Vec<(FileNumber, SstFileInfo)> = Vec::new();
                let mut i = 0usize;
                let mut slot_idx = 0usize;
                while i < all.len() {
                    let last_slot = slot_idx + 1 >= slots.len();
                    let (cur_fnum, cur_path) = slots[slot_idx].clone();
                    let write_path = if atomic_rename {
                        sst_temp_path(&cur_path)
                    } else {
                        cur_path.clone()
                    };
                    // Write ONE output file in its own scope so the streaming
                    // writer's borrow of `wf` is released before we roll slots.
                    let (_file_emitted, info_opt): (u64, Option<SstFileInfo>) = {
                        let mut wf = self.fs.open_writable_file(&write_path, write_mode)?;
                        // R49-H1: stamp this compaction's cf_id onto the writer
                        // options so the SST footer + SstFileMeta carry CF identity.
                        let mut writer_opts = self.writer_options.clone();
                        writer_opts.cf_id = self.cf_id;
                        let mut writer =
                            SstWriterImpl::with_options(writer_opts).streaming(&mut *wf);
                        let mut file_emitted = 0u64;
                        while i < all.len() {
                            let key_end = {
                                let key = &all[i].key;
                                let mut j = i + 1;
                                while j < all.len() && all[j].key == *key {
                                    j += 1;
                                }
                                j
                            };
                            // Versions for this key, newest first.
                            let versions = &all[i..key_end];
                            i = key_end;
                            self.emit_key_versions(&mut writer, versions, &mut file_emitted)?;
                            // Roll to the next slot at this user-key boundary when
                            // the file has content, splitting is enabled, we are NOT
                            // on the final slot, the file reached target, and keys
                            // remain. Splitting only between keys keeps every
                            // version of a key in one file.
                            if !last_slot
                                && target > 0
                                && file_emitted > 0
                                && i < all.len()
                                && writer.estimated_size() >= target
                            {
                                break;
                            }
                        }
                        if file_emitted == 0 {
                            // Bottommost-tombstone-only input: emit nothing. The
                            // streaming writer requires ≥ 1 entry, so drop it
                            // unfinished and best-effort clean up the tmp file. Not
                            // referenced by any VersionEdit. R38-L2: warn on delete
                            // failure (restore orphan-scan still catches it).
                            drop(writer);
                            drop(wf);
                            if let Err(e) = self.fs.delete_file(&write_path) {
                                tracing::warn!(
                                    "CompactionJob: zero-emit tmp delete failed for {}: {} \
                                 (R38-L2; restore orphan-scan will rename on restart)",
                                    write_path.display(),
                                    e
                                );
                            }
                            (0, None)
                        } else {
                            let info = writer.finish()?;
                            wf.flush()?;
                            wf.sync()?;
                            (file_emitted, Some(info))
                        }
                    };

                    if let Some(info) = info_opt {
                        // Publish this file: rename on local FS (the upload already
                        // published it on object stores). R38-H1: best-effort tmp
                        // cleanup on rename failure before propagating.
                        if atomic_rename {
                            if let Err(e) = self.fs.rename(&write_path, &cur_path) {
                                let _ = self.fs.delete_file(&write_path);
                                return Err(e);
                            }
                            // R49-H3: fsync(parent_dir) so the dirent change is
                            // durable across power loss (best-effort).
                            if let Some(parent) = cur_path.parent() {
                                if let Err(e) = self.fs.sync_dir(parent) {
                                    tracing::warn!(
                                    "CompactionJob: sync_dir({}) failed after rename: {} (R49-H3)",
                                    parent.display(),
                                    e
                                );
                                }
                            }
                        }
                        produced.push((cur_fnum, info));
                    }

                    // Advance to the next slot only if more keys remain (i.e. we
                    // rolled mid-stream). When the inner loop consumed everything,
                    // `i == all.len()` and the outer loop exits — so `slot_idx`
                    // never exceeds the final slot and no slot is reused.
                    if i < all.len() {
                        slot_idx += 1;
                    }
                }
                Ok(produced)
            })();
            write_outcome?
        };

        // Zero-emit across ALL slots (e.g. everything was a bottommost
        // tombstone): emit a deletion-only VersionEdit. R0A-H1: keep
        // last_sequence at the input max so the version counter does not
        // regress.
        if produced.is_empty() {
            let max_in_seq = self
                .inputs
                .iter()
                .map(|(_, m, _)| m.max_sequence.value())
                .max()
                .unwrap_or(0);
            let last_seq = if max_in_seq > 0 {
                Some(forst_rs_common::SequenceNumber(max_in_seq))
            } else {
                None
            };
            return Ok(Some(VersionEdit {
                deleted_files: self
                    .inputs
                    .iter()
                    .map(|(lvl, m, _)| (*lvl, m.file_number))
                    .collect(),
                new_files: Vec::new(),
                next_file_number: None,
                last_sequence: last_seq,
            }));
        }

        // 6. Build the VersionEdit: add EVERY produced file at output_level,
        // remove all inputs. R49-H1: stamp the job's cf_id onto each meta.
        let mut new_files: Vec<(u32, SstFileMeta)> = Vec::with_capacity(produced.len());
        let mut max_out_seq = 0u64;
        for (fnum, info) in produced {
            debug_assert_eq!(info.cf_id, self.cf_id);
            max_out_seq = max_out_seq.max(info.max_sequence);
            new_files.push((
                self.output_level,
                SstFileMeta {
                    file_number: fnum,
                    cf_id: self.cf_id,
                    file_size: info.file_size,
                    smallest_key: info.min_key,
                    largest_key: info.max_key,
                    min_sequence: forst_rs_common::SequenceNumber(info.min_sequence),
                    max_sequence: forst_rs_common::SequenceNumber(info.max_sequence),
                    num_entries: info.entry_count,
                },
            ));
        }
        let deleted = self
            .inputs
            .iter()
            .map(|(lvl, m, _)| (*lvl, m.file_number))
            .collect();
        // R0A-H1: stamp last_sequence with the max output seq across all
        // produced files so the version-set high-water mark tracks the
        // persistent maximum (checkpoint→restore correctness).
        let last_seq = if max_out_seq > 0 {
            Some(forst_rs_common::SequenceNumber(max_out_seq))
        } else {
            None
        };
        Ok(Some(VersionEdit {
            new_files,
            deleted_files: deleted,
            next_file_number: None,
            last_sequence: last_seq,
        }))
    }

    /// FRS-ZERO-COPY-MERGE (2026-06-05): the streaming k-way compaction merge.
    ///
    /// Replaces the global `gather Vec<CompactionEntry>` (2 heap allocs/row) +
    /// `sort` with a `BinaryHeap` over one [`SstBlockCursor`] per input. Pulls
    /// the globally-smallest key, collects ALL its versions into a small reused
    /// `group`, sorts that (tiny) group by `effective_seq` DESC to match the old
    /// global sort order, and feeds it to the UNCHANGED `emit_key_versions`.
    /// Peak working set = one block per input + one key's versions (vs the whole
    /// dataset), so the ~GB of sort traffic + the 400 MB resident vector that
    /// drove the memory-bandwidth-bound merge are gone. Output is byte-identical
    /// to [`Self::run`] (same per-key groups, same order, same emit), so the
    /// existing compaction suite is the equivalence gate.
    ///
    /// Multi-file output (`target_file_size` / `additional_outputs`) rolls to the
    /// next slot at a user-key boundary, identical to the serial path in `run`.
    fn run_streaming(self) -> ForstResult<Option<VersionEdit>> {
        for (_, meta, _) in &self.inputs {
            debug_assert_eq!(
                meta.cf_id,
                self.cf_id,
                "CompactionJob cf_id {:?} ≠ input file {} cf_id {:?} — cross-CF input",
                self.cf_id,
                meta.file_number.value(),
                meta.cf_id,
            );
        }

        // One pull cursor per input; track each input's file_number for the
        // effective-seq tie-break (newer file dominates).
        let mut cursors: Vec<SstBlockCursor> = Vec::with_capacity(self.inputs.len());
        let mut file_nums: Vec<u64> = Vec::with_capacity(self.inputs.len());
        for (_lvl, meta, reader) in &self.inputs {
            cursors.push(SstBlockCursor::new(Arc::clone(reader))?);
            file_nums.push(meta.file_number.value());
        }

        let mut heap: std::collections::BinaryHeap<HeapKey> = std::collections::BinaryHeap::new();
        for (idx, c) in cursors.iter().enumerate() {
            if c.valid() {
                heap.push(HeapKey {
                    key: c.key().to_vec(),
                    idx,
                });
            }
        }

        // Empty merge: produce a deletion-only VersionEdit (preserve input max
        // seq so the high-water mark does not regress) — mirrors `run`.
        if heap.is_empty() {
            let max_in_seq = self
                .inputs
                .iter()
                .map(|(_, m, _)| m.max_sequence.value())
                .max()
                .unwrap_or(0);
            let last_seq = (max_in_seq > 0).then_some(SequenceNumber(max_in_seq));
            return Ok(Some(VersionEdit {
                deleted_files: self
                    .inputs
                    .iter()
                    .map(|(lvl, m, _)| (*lvl, m.file_number))
                    .collect(),
                new_files: Vec::new(),
                next_file_number: None,
                last_sequence: last_seq,
            }));
        }

        let atomic_rename = self.fs.supports_atomic_rename();
        let write_mode = if atomic_rename {
            forst_rs_io::WriteMode::CreateNew
        } else {
            forst_rs_io::WriteMode::CreateOrTruncate
        };
        if let Some(parent) = self.output_path.parent() {
            self.fs.create_dir_all(parent)?;
        }
        let mut slots: Vec<(FileNumber, PathBuf)> =
            Vec::with_capacity(1 + self.additional_outputs.len());
        slots.push((self.output_file_number, self.output_path.clone()));
        slots.extend(self.additional_outputs.iter().cloned());
        let target = self.target_file_size;

        let mut produced: Vec<(FileNumber, SstFileInfo)> = Vec::new();
        let mut slot_idx = 0usize;
        let mut group: Vec<CompactionEntry> = Vec::new();
        let mut members: Vec<usize> = Vec::new();

        // FRS-ZERO-COPY-MERGE fast path: when there is NO compaction filter AND
        // NO live snapshot (min_active == u64::MAX, so `emit_key_versions`'s
        // pinned slice is always empty), a key whose globally-newest version is
        // a Put is emitted as exactly that Put with every older version dropped
        // (compaction.rs Put branch). We can then write the newest Put's
        // BORROWED bytes straight to the writer — no `CompactionEntry` value
        // copy — and drain the shadowed older versions. Filters (Replace/Discard)
        // and snapshot pinning force the byte-equivalent copy fallback.
        let fast_eligible =
            self.compaction_filter.is_none() && self.min_active_snapshot.0 == u64::MAX;

        let write_outcome: ForstResult<()> = (|| {
            while !heap.is_empty() {
                let last_slot = slot_idx + 1 >= slots.len();
                let (cur_fnum, cur_path) = slots[slot_idx].clone();
                let write_path = if atomic_rename {
                    sst_temp_path(&cur_path)
                } else {
                    cur_path.clone()
                };
                let (file_emitted, info_opt): (u64, Option<SstFileInfo>) = {
                    let mut wf = self.fs.open_writable_file(&write_path, write_mode)?;
                    let mut writer_opts = self.writer_options.clone();
                    writer_opts.cf_id = self.cf_id;
                    let mut writer = SstWriterImpl::with_options(writer_opts).streaming(&mut *wf);
                    let mut file_emitted = 0u64;
                    while !heap.is_empty() {
                        let cur_key = heap.peek().expect("heap non-empty").key.clone();
                        // Pop every cursor currently positioned at `cur_key`.
                        // Each is at ITS newest `cur_key` version (on-disk order
                        // is key ASC, seq DESC), so the global newest is the
                        // member with the max effective_seq.
                        members.clear();
                        while heap.peek().map(|h| h.key == cur_key).unwrap_or(false) {
                            members.push(heap.pop().expect("peeked").idx);
                        }

                        let mut fast_done = false;
                        if fast_eligible {
                            let newest_idx = *members
                                .iter()
                                .max_by_key(|&&i| {
                                    ((file_nums[i] as u128) << 64) | (cursors[i].sequence() as u128)
                                })
                                .expect("members non-empty");
                            if cursors[newest_idx].op_type() == forst_rs_common::OpType::Put {
                                // Write the newest Put's BORROWED bytes directly
                                // (zero CompactionEntry copy) — equivalent to
                                // emit_key_versions' Put branch (emit newest,
                                // drop all older) when nothing is pinned/filtered.
                                writer.add(
                                    cursors[newest_idx].key(),
                                    cursors[newest_idx].value(),
                                    cursors[newest_idx].sequence(),
                                    forst_rs_common::OpType::Put as u8,
                                )?;
                                file_emitted += 1;
                                // Drain (drop) every shadowed version of cur_key
                                // across all members, then re-push their next key.
                                for &i in &members {
                                    while cursors[i].valid() && cursors[i].key() == cur_key {
                                        cursors[i].advance()?;
                                    }
                                    if cursors[i].valid() {
                                        heap.push(HeapKey {
                                            key: cursors[i].key().to_vec(),
                                            idx: i,
                                        });
                                    }
                                }
                                fast_done = true;
                            }
                        }

                        if !fast_done {
                            // Byte-equivalent fallback: materialize this key's
                            // versions and run the unchanged consolidation.
                            group.clear();
                            for &i in &members {
                                while cursors[i].valid() && cursors[i].key() == cur_key {
                                    group.push(CompactionEntry {
                                        key: cursors[i].key().to_vec(),
                                        value: cursors[i].value().map(|v| v.to_vec()),
                                        sequence: cursors[i].sequence(),
                                        op_type: cursors[i].op_type(),
                                        file_number: file_nums[i],
                                    });
                                    cursors[i].advance()?;
                                }
                                if cursors[i].valid() {
                                    heap.push(HeapKey {
                                        key: cursors[i].key().to_vec(),
                                        idx: i,
                                    });
                                }
                            }
                            // Newest-first within the key (index 0 = newest).
                            group.sort_by_key(|b| std::cmp::Reverse(b.effective_seq()));
                            self.emit_key_versions(&mut writer, &group, &mut file_emitted)?;
                        }

                        // Roll to the next slot at this user-key boundary once the
                        // file reached target and more keys remain.
                        if !last_slot
                            && target > 0
                            && file_emitted > 0
                            && !heap.is_empty()
                            && writer.estimated_size() >= target
                        {
                            break;
                        }
                    }
                    if file_emitted == 0 {
                        drop(writer);
                        drop(wf);
                        if let Err(e) = self.fs.delete_file(&write_path) {
                            tracing::warn!(
                                "CompactionJob(streaming): zero-emit tmp delete failed for {}: {}",
                                write_path.display(),
                                e
                            );
                        }
                        (0, None)
                    } else {
                        let info = writer.finish()?;
                        wf.flush()?;
                        wf.sync()?;
                        (file_emitted, Some(info))
                    }
                };
                let _ = file_emitted;

                if let Some(info) = info_opt {
                    if atomic_rename {
                        if let Err(e) = self.fs.rename(&write_path, &cur_path) {
                            let _ = self.fs.delete_file(&write_path);
                            return Err(e);
                        }
                        if let Some(parent) = cur_path.parent() {
                            if let Err(e) = self.fs.sync_dir(parent) {
                                tracing::warn!(
                                    "CompactionJob(streaming): sync_dir({}) failed: {}",
                                    parent.display(),
                                    e
                                );
                            }
                        }
                    }
                    produced.push((cur_fnum, info));
                }

                if !heap.is_empty() {
                    slot_idx += 1;
                }
            }
            Ok(())
        })();
        write_outcome?;

        // Zero-emit across all slots (e.g. all bottommost tombstones).
        if produced.is_empty() {
            let max_in_seq = self
                .inputs
                .iter()
                .map(|(_, m, _)| m.max_sequence.value())
                .max()
                .unwrap_or(0);
            let last_seq = (max_in_seq > 0).then_some(SequenceNumber(max_in_seq));
            return Ok(Some(VersionEdit {
                deleted_files: self
                    .inputs
                    .iter()
                    .map(|(lvl, m, _)| (*lvl, m.file_number))
                    .collect(),
                new_files: Vec::new(),
                next_file_number: None,
                last_sequence: last_seq,
            }));
        }

        let mut new_files: Vec<(u32, SstFileMeta)> = Vec::with_capacity(produced.len());
        let mut max_out_seq = 0u64;
        for (fnum, info) in produced {
            debug_assert_eq!(info.cf_id, self.cf_id);
            max_out_seq = max_out_seq.max(info.max_sequence);
            new_files.push((
                self.output_level,
                SstFileMeta {
                    file_number: fnum,
                    cf_id: self.cf_id,
                    file_size: info.file_size,
                    smallest_key: info.min_key,
                    largest_key: info.max_key,
                    min_sequence: forst_rs_common::SequenceNumber(info.min_sequence),
                    max_sequence: forst_rs_common::SequenceNumber(info.max_sequence),
                    num_entries: info.entry_count,
                },
            ));
        }
        let deleted = self
            .inputs
            .iter()
            .map(|(lvl, m, _)| (*lvl, m.file_number))
            .collect();
        let last_seq = (max_out_seq > 0).then_some(SequenceNumber(max_out_seq));
        Ok(Some(VersionEdit {
            new_files,
            deleted_files: deleted,
            next_file_number: None,
            last_sequence: last_seq,
        }))
    }

    /// FRS-COMPACT-PARALLEL (2026-06-05): emit ONE output SST covering the
    /// sorted entries `all[range]` (a contiguous, key-boundary-aligned key
    /// range). No mid-range slot rolling — the partition IS the file. Reuses
    /// `emit_key_versions` per key-group, so the per-partition output is
    /// byte-identical to what the serial slot loop would produce for that range.
    /// Returns `None` when the partition emits zero rows (e.g. all bottommost
    /// tombstones). Pure read of `&self` + `all` ⇒ safe to call concurrently
    /// across disjoint partitions.
    fn emit_one_sst(
        &self,
        all: &[CompactionEntry],
        range: std::ops::Range<usize>,
        fnum: FileNumber,
        path: &std::path::Path,
        atomic_rename: bool,
        write_mode: forst_rs_io::WriteMode,
    ) -> ForstResult<Option<(FileNumber, SstFileInfo)>> {
        let write_path = if atomic_rename {
            sst_temp_path(path)
        } else {
            path.to_path_buf()
        };
        let (file_emitted, info_opt): (u64, Option<SstFileInfo>) = {
            let mut wf = self.fs.open_writable_file(&write_path, write_mode)?;
            let mut writer_opts = self.writer_options.clone();
            writer_opts.cf_id = self.cf_id;
            let mut writer = SstWriterImpl::with_options(writer_opts).streaming(&mut *wf);
            let mut file_emitted = 0u64;
            let mut i = range.start;
            while i < range.end {
                let key_end = {
                    let key = &all[i].key;
                    let mut j = i + 1;
                    while j < range.end && all[j].key == *key {
                        j += 1;
                    }
                    j
                };
                let versions = &all[i..key_end];
                i = key_end;
                self.emit_key_versions(&mut writer, versions, &mut file_emitted)?;
            }
            if file_emitted == 0 {
                drop(writer);
                drop(wf);
                if let Err(e) = self.fs.delete_file(&write_path) {
                    tracing::warn!(
                        "CompactionJob: zero-emit tmp delete failed for {}: {}",
                        write_path.display(),
                        e
                    );
                }
                (0, None)
            } else {
                let info = writer.finish()?;
                wf.flush()?;
                wf.sync()?;
                (file_emitted, Some(info))
            }
        };
        let _ = file_emitted;
        if let Some(info) = info_opt {
            if atomic_rename {
                if let Err(e) = self.fs.rename(&write_path, path) {
                    let _ = self.fs.delete_file(&write_path);
                    return Err(e);
                }
                if let Some(parent) = path.parent() {
                    if let Err(e) = self.fs.sync_dir(parent) {
                        tracing::warn!(
                            "CompactionJob: sync_dir({}) failed after rename: {}",
                            parent.display(),
                            e
                        );
                    }
                }
            }
            Ok(Some((fnum, info)))
        } else {
            Ok(None)
        }
    }

    fn emit_key_versions<W>(
        &self,
        writer: &mut StreamingSstWriter<'_, W>,
        versions: &[CompactionEntry],
        emitted: &mut u64,
    ) -> ForstResult<()>
    where
        W: WritableFile + ?Sized,
    {
        debug_assert!(!versions.is_empty());
        // `versions` is sorted by sequence DESC — the newest version is at
        // index 0.

        use forst_rs_common::OpType;

        // ---------------------------------------------------------------
        // MVCC snapshot retention (spec §6a.5).
        //
        // Versions with `seq >= min_active_snapshot` are visible to at
        // least one live snapshot and must be emitted verbatim — they
        // cannot be folded into the newest-wins consolidation below.
        //
        // The slice splits into a snapshot-pinned prefix and an
        // unconstrained tail:
        //   * prefix `pinned = versions[..split_idx]` — every entry
        //     `seq >= min_active`. Emitted as-is via the per-entry
        //     `mvcc::should_drop` check (which here returns `false` for
        //     all of them, since `seq >= min_active` ⇒ keep). For each
        //     successfully emitted entry we mark "newer emitted for this
        //     user_key" so subsequent older entries below `min_active`
        //     can be reclaimed.
        //   * tail `tail = versions[split_idx..]` — `seq < min_active`.
        //     The newest tail entry (or the only pinned entry, if the
        //     tail is empty AND the pinned slice has length ≤ 1) feeds
        //     into the existing newest-wins consolidation that handles
        //     SingleDelete elision, Delete tombstone shedding, and
        //     Merge-chain resolution.
        //
        // When `min_active == u64::MAX` (no live snapshots), pinned is
        // empty and behaviour collapses to pre-MVCC compaction.
        let split_idx = versions
            .iter()
            .position(|v| v.sequence < self.min_active_snapshot.0)
            .unwrap_or(versions.len());
        let pinned = &versions[..split_idx];
        let tail = &versions[split_idx..];

        // Track whether we've already emitted any version for this user
        // key in the current call. The MVCC contract says a tail entry
        // can be dropped only when a newer version exists for the same
        // key — once we emit any pinned entry, that flag is satisfied
        // for every tail entry that follows.
        let mut newer_emitted_for_key = false;
        for v in pinned {
            // `should_drop` returns `false` for `seq >= min_active`, so
            // this is effectively an unconditional emit; the call form
            // documents the policy and stays consistent with the tail
            // path's gating.
            if mvcc::should_drop(
                SequenceNumber(v.sequence),
                v.op_type,
                newer_emitted_for_key,
                self.min_active_snapshot,
            ) {
                continue;
            }
            writer.add(&v.key, v.value.as_deref(), v.sequence, v.op_type as u8)?;
            *emitted += 1;
            newer_emitted_for_key = true;
        }

        // If the tail is empty there is nothing left to consolidate —
        // every snapshot-pinned version has already been written.
        if tail.is_empty() {
            return Ok(());
        }
        // A-NEW-H2: when pinned is non-empty, we used to drop the
        // ENTIRE tail on the theory that "newer_emitted satisfies the
        // shadowing rule for every tail entry". That's wrong for the
        // snapshot at S = min_active: its read of a key is
        // `max(v.seq | v.seq <= min_active)`. If the smallest pinned
        // version has `seq > min_active`, the snapshot at S =
        // min_active never sees pinned and instead needs the FLOOR
        // version in tail (the largest version with seq <
        // min_active). Pre-fix code reclaimed that floor, leaving
        // snapshot-at-min_active reads returning `None` instead of
        // the floor's value.
        //
        // The fix: when pinned is non-empty AND
        // `pinned[last].sequence > min_active_snapshot`, also emit
        // the floor (tail[0]) so reads at S = min_active see it.
        // Older tail entries (tail[1..]) are shadowed by the floor
        // for every S in [min_active, pinned[last].sequence) and can
        // be reclaimed.
        //
        // When `pinned[last].sequence == min_active`, the smallest
        // pinned version IS the floor for S = min_active, so tail[0]
        // is shadowed and we can drop it (matches pre-fix behavior
        // for that sub-case).
        if newer_emitted_for_key {
            let smallest_pinned = pinned.last().unwrap();
            let smallest_pinned_seq = smallest_pinned.sequence;
            // A-R3-H2: the prior A-NEW-H2 rule emitted tail[0] only
            // when the smallest pinned seq STRICTLY EXCEEDED
            // min_active. That's correct only when smallest_pinned
            // is a TERMINAL op (Put/Delete/SingleDelete). When
            // smallest_pinned is a Merge — possibly at
            // seq == min_active — the snapshot at S = min_active
            // sees the Merge operand and the merge-aware reader
            // (mvcc::get_at_with_merge) must walk to an older
            // Put/Delete to resolve the chain. Dropping the tail
            // would leave that walk with no base.
            //
            // The corrected rule:
            // 1. If smallest_pinned_seq > min_active OR the pinned
            //    slice contains ANY non-terminal Merge: we must
            //    preserve enough tail to give the merge-aware
            //    reader a Put/Delete base for the snapshot at
            //    seq <= smallest_pinned_seq.
            // 2. We walk tail forward, emitting Merge operands and
            //    stopping at the first Put/Delete/SingleDelete
            //    (terminal). Anything older than the terminal is
            //    shadowed for every active snapshot and can be
            //    reclaimed.
            // 3. If the entire tail is Merge entries (no terminal),
            //    we emit ALL of them — the chain has no base and
            //    the merge operator's full_merge handles `base=None`.
            //
            // We DO NOT skip is_bottommost handling for the floor —
            // floor retention is required precisely to serve a
            // live snapshot, so reclamation would defeat the fix.
            let need_floor = smallest_pinned_seq > self.min_active_snapshot.0
                || pinned.iter().any(|v| v.op_type == OpType::Merge);
            if need_floor {
                for entry in tail.iter() {
                    writer.add(
                        &entry.key,
                        entry.value.as_deref(),
                        entry.sequence,
                        entry.op_type as u8,
                    )?;
                    *emitted += 1;
                    match entry.op_type {
                        OpType::Put | OpType::Delete | OpType::SingleDelete => break,
                        OpType::Merge => continue,
                    }
                }
            }
            let _ = smallest_pinned; // suppress unused-warning when need_floor=false
            return Ok(());
        }
        // Pinned was empty (no live snapshots see this key's history) —
        // fall through to the pre-MVCC newest-wins consolidation over the
        // tail. The tail's newest is the only candidate the reduction may
        // emit; older tail entries are shadowed.
        let reduction = tail;
        let newest = &reduction[0];

        // Run the optional compaction filter on the consolidation root —
        // if it says Discard, drop the root (only when bottommost; see
        // A-R3-NEW-H1 below). (Pinned siblings emitted above are not
        // re-evaluated; they're contractually required by an active
        // snapshot.)
        if let Some(ref filter) = self.compaction_filter {
            let mut scratch = Vec::new();
            let decision = filter.filter(
                self.output_level,
                &newest.key,
                newest.value.as_deref(),
                newest.sequence,
                newest.op_type,
                &mut scratch,
            );
            match decision {
                CompactionDecision::Discard => {
                    // A-R3-NEW-H1: gate Discard on `is_bottommost`,
                    // mirroring A-R6-H1 / Delete / SD-pair gates.
                    // `versions` is only this compaction's view of the
                    // user key; OLDER versions for the same user_key
                    // may live in SST files outside this compaction's
                    // input set. Unconditionally dropping `newest` at
                    // a non-bottommost level resurrects those stale
                    // versions on subsequent reads, breaking the
                    // filter's intent (e.g. TTL expiry: an expired
                    // Put@s1 with an older Put@s0 underneath would
                    // resurrect s0 on reads after the s1 drop). At
                    // non-bottommost, fall through to the newest-wins
                    // emit below; bottommost compaction will re-apply
                    // the filter and drop the entry safely.
                    if self.is_bottommost {
                        return Ok(());
                    }
                }
                CompactionDecision::Keep => {}
                CompactionDecision::Replace => {
                    writer.add(
                        &newest.key,
                        Some(&scratch),
                        newest.sequence,
                        newest.op_type as u8,
                    )?;
                    *emitted += 1;
                    return Ok(());
                }
            }
        }

        // The reduction below mirrors the pre-MVCC newest-wins logic.
        // It operates over `reduction`; the variable `versions` is
        // shadowed so each per-shape branch sees the correct slice.
        let versions = reduction;

        match newest.op_type {
            OpType::SingleDelete => {
                // SingleDelete optimization (conservative RocksDB semantics).
                //
                // Contract: `SingleDelete` is valid only when the caller
                // guarantees the key has been `put` at most once since the
                // previous delete-family op for this key. Callers that
                // violate the contract get Delete-equivalent semantics
                // below; they do NOT get data corruption.
                //
                // Policy: Only the strict `[SingleDelete, Put]` pair
                // (exactly two versions visible to THIS compaction) is
                // elided — both entries are dropped from this compaction's
                // output. Every other shape — stacked SingleDeletes,
                // intervening Merges, additional shadowed Puts — falls
                // back to the regular Delete path.
                //
                // Rationale: with exactly two versions in `versions`, the
                // SingleDelete contract implies there is no older Put for
                // this user key shadowed underneath (if there were, it
                // would appear in `versions`). In every other shape the
                // safe answer is to retain the tombstone, because
                // resurrecting a stale Put from a lower level would
                // violate read-after-delete semantics.
                //
                // Note: `versions` is this compaction's view of the user
                // key (newest-first, across its input SSTs + memtables).
                // Keys living in SST files outside this compaction's
                // input set are unaffected by elision.
                if versions.len() == 2 && versions[1].op_type == OpType::Put && self.is_bottommost {
                    // A-R6-H1: drop-both elision REQUIRES `is_bottommost`.
                    // Pre-fix the elision ran on any compaction whose input
                    // view happened to contain exactly {SD, Put}, but
                    // `versions` is only this compaction's input — lower-
                    // level SSTs outside the input set may still hold an
                    // older Put for the same user_key. Dropping both
                    // entries at a non-bottommost level resurrects that
                    // stale Put on subsequent reads, breaking
                    // read-after-delete semantics. The Delete branch
                    // below already gates correctly on `is_bottommost`;
                    // the SD-pair branch was the outlier.
                    return Ok(());
                }
                // Fallback: behave exactly like Delete.
                if self.is_bottommost {
                    return Ok(());
                }
                writer.add(
                    &newest.key,
                    newest.value.as_deref(),
                    newest.sequence,
                    newest.op_type as u8,
                )?;
                *emitted += 1;
            }
            OpType::Delete => {
                if self.is_bottommost {
                    // Drop the tombstone AND every older version entirely.
                    return Ok(());
                }
                // Retain the tombstone; drop older versions (the tombstone
                // hides them).
                writer.add(
                    &newest.key,
                    newest.value.as_deref(),
                    newest.sequence,
                    newest.op_type as u8,
                )?;
                *emitted += 1;
            }
            OpType::Put => {
                // Emit the Put (most recent value). Drop every older version
                // for this key since they're shadowed.
                writer.add(
                    &newest.key,
                    newest.value.as_deref(),
                    newest.sequence,
                    newest.op_type as u8,
                )?;
                *emitted += 1;
            }
            OpType::Merge => {
                // Walk backwards collecting merges until we hit a Put/Delete
                // or exhaust the versions list. If we have a merge operator,
                // resolve the chain into a single Put at the newest sequence.
                let mut operands: Vec<&[u8]> = Vec::new();
                let mut base: Option<&[u8]> = None;
                let mut stop_on_delete = false;
                for v in versions {
                    match v.op_type {
                        OpType::Merge => {
                            // A-R7-H2: surface corruption on missing
                            // operand payload — sibling of the
                            // read-side checks A-R5R-NEW-H1 /
                            // A-R6-H2 / C-R5-H1. Compaction is the
                            // last line of defense: if it silently
                            // absorbs a None-valued Merge into a
                            // collapsed Put, the read-side corruption
                            // detection NEVER fires again on that
                            // key (the bad row has been rewritten as
                            // well-formed output bytes). Raise here
                            // so the bad input is surfaced before
                            // compaction's collapse can hide it.
                            match v.value.as_deref() {
                                Some(val) => operands.push(val),
                                None => {
                                    return Err(ForstError::corruption(
                                        "compaction: Merge entry missing operand payload",
                                    ));
                                }
                            }
                        }
                        OpType::Put => {
                            base = v.value.as_deref();
                            break;
                        }
                        OpType::Delete | OpType::SingleDelete => {
                            stop_on_delete = true;
                            break;
                        }
                    }
                }

                if let Some(op) = &self.merge_operator {
                    // operands was newest-first; merge op expects oldest-first.
                    let reversed: Vec<&[u8]> = operands.iter().copied().rev().collect();
                    // D-R11-H1: split the "collapse" condition. The
                    // chain can be safely collapsed into a single
                    // synthetic Put@newest.seq ONLY when there is no
                    // intermediate snapshot that would be served by
                    // an older operand or the deletion tombstone:
                    //   - `base.is_some()`: a Put base exists IN this
                    //     compaction's input — collapse is at-most
                    //     equivalent to the read path.
                    //   - `self.is_bottommost`: no lower-level data
                    //     remains, so the synthesized Put covers
                    //     every visible snapshot at this level.
                    //   - `stop_on_delete && !self.is_bottommost`:
                    //     PRE-D-R11-H1 we collapsed here AND emitted
                    //     the Delete (A-R9-N2). That collapse drops
                    //     intermediate Merges, and a snapshot at S
                    //     in [delete.seq, newest.seq) would see the
                    //     Delete but lose the partial-merge result
                    //     it should have returned. The CORRECT
                    //     non-bottommost handling is to emit ALL
                    //     versions verbatim and let the bottommost
                    //     compaction collapse the chain once no
                    //     intermediate snapshot can land.
                    if base.is_some() || self.is_bottommost {
                        let merged = op.full_merge(&newest.key, base, &reversed)?;
                        writer.add(
                            &newest.key,
                            Some(&merged),
                            newest.sequence,
                            OpType::Put as u8,
                        )?;
                        *emitted += 1;
                    } else if stop_on_delete {
                        // D-R11-H1: emit every version verbatim — the
                        // Merges (newest → oldest) followed by the
                        // terminal Delete. The bottommost compaction
                        // will eventually do the collapse safely.
                        for v in versions {
                            writer.add(&v.key, v.value.as_deref(), v.sequence, v.op_type as u8)?;
                            *emitted += 1;
                        }
                    } else {
                        // A-R12-H3: Merge-only chain at non-bottommost MUST
                        // be emitted verbatim. Pre-fix we partial-merged the
                        // chain into a single Merge@newest.seq, but that
                        // breaks snapshot visibility identically to the
                        // collapse case D-R11-H1 already fixed: a reader at
                        // S in [oldest.seq, newest.seq) cannot see the
                        // collapsed Merge@newest.seq, and the intermediate
                        // Merges that would have served that snapshot have
                        // been deleted. Emit all versions verbatim and let
                        // the bottommost compaction collapse the chain once
                        // no intermediate snapshot can land below it.
                        let _ = reversed;
                        for v in versions {
                            writer.add(&v.key, v.value.as_deref(), v.sequence, v.op_type as u8)?;
                            *emitted += 1;
                        }
                    }
                } else {
                    // No merge operator — preserve every version exactly as
                    // written. This is correct but doesn't shrink the data.
                    // (If stop_on_delete is true we still emit the older
                    // Delete so subsequent reads see it.)
                    //
                    // A-R8-H3: at a BOTTOMMOST compaction this branch silently
                    // retains un-foldable Merge chains FOREVER — the LSM's
                    // reclamation invariant requires bottommost levels to
                    // collapse to a single terminal per user_key. If a Merge
                    // chain reaches bottommost without a merge operator, the
                    // CF was mis-configured (operator dropped between CF
                    // creation and a later restart). Surface that as
                    // ForstError::invalid_argument so the configuration drift
                    // is visible at compaction time rather than as silent
                    // unbounded SST growth.
                    if self.is_bottommost {
                        return Err(ForstError::invalid_argument(format!(
                            "compaction: bottommost CF has no merge operator but the input \
                             contains an un-folded Merge chain for key {:?} (len={}); the LSM \
                             reclamation invariant cannot be satisfied. The CF was likely \
                             re-opened without re-registering its merge operator.",
                            &newest.key,
                            versions.len()
                        )));
                    }
                    let _ = stop_on_delete;
                    for v in versions {
                        writer.add(&v.key, v.value.as_deref(), v.sequence, v.op_type as u8)?;
                        *emitted += 1;
                    }
                }
            }
        }
        Ok(())
    }
}

/// FRS-ZERO-COPY-MERGE: whether the opt-in parallel sub-compaction path is
/// requested (`FRS_COMPACT_PARALLEL`). When false (the default), `run()`
/// delegates to the streaming `run_streaming`.
fn compaction_parallel_env() -> bool {
    matches!(
        std::env::var("FRS_COMPACT_PARALLEL").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Heap element for the streaming k-way merge: orders inputs by current key
/// ASCENDING (so `BinaryHeap`, a max-heap, yields the smallest key first via the
/// reversed `Ord`). Ties on key are broken by `idx` for a total order; the
/// merge collects ALL same-key versions then re-sorts them by `effective_seq`
/// DESC, so intra-key heap order does not affect output.
struct HeapKey {
    key: Vec<u8>,
    idx: usize,
}
impl PartialEq for HeapKey {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.idx == other.idx
    }
}
impl Eq for HeapKey {}
impl Ord for HeapKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reversed: smaller key ⇒ "greater" so the max-heap pops it first.
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.idx.cmp(&self.idx))
    }
}
impl PartialOrd for HeapKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct CompactionEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    sequence: u64,
    op_type: forst_rs_common::OpType,
    /// File number the entry came from. Used to break ties when two inputs
    /// share a memtable-local sequence number (because each
    /// [`forst_rs_storage::memtable::VectorizedMemTable`] starts counting seqs from 1).
    file_number: u64,
}

impl CompactionEntry {
    fn effective_seq(&self) -> u128 {
        // Pack (file_number, sequence) into a single comparable key where a
        // higher file_number dominates (newer file > older file), and within
        // the same file the higher sequence dominates.
        ((self.file_number as u128) << 64) | (self.sequence as u128)
    }
}

/// Computes the output path for a compaction that produces `file_number`.
pub fn compaction_output_path(db_path: &std::path::Path, file_number: FileNumber) -> PathBuf {
    sst_file_path(db_path, file_number)
}

/// Helper that decides whether a level needs compaction based on its size.
pub fn level_needs_compaction(level_total_size: u64, target_size: u64) -> bool {
    level_total_size > target_size
}

/// Utility: ensures we got at least one input file.
pub fn validate_inputs(inputs: &[(u32, SstFileMeta)]) -> ForstResult<()> {
    if inputs.is_empty() {
        return Err(ForstError::invalid_argument(
            "CompactionJob: inputs must be non-empty",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_needs_compaction_threshold() {
        assert!(!level_needs_compaction(0, 100));
        assert!(!level_needs_compaction(100, 100));
        assert!(level_needs_compaction(101, 100));
    }

    #[test]
    fn test_validate_inputs_rejects_empty() {
        assert!(validate_inputs(&[]).is_err());
    }

    #[test]
    fn test_compaction_output_path() {
        let base = std::path::PathBuf::from("/db");
        let p = compaction_output_path(&base, FileNumber(7));
        assert_eq!(p, std::path::PathBuf::from("/db/000007.sst"));
    }
}
