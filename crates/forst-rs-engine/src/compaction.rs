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
use std::sync::Arc;

use forst_rs_common::{ColumnFamilyId, FileNumber, ForstError, ForstResult, SequenceNumber};
use forst_rs_io::{FileSystem, WritableFile};
use forst_rs_storage::merge_operator::MergeOperator;
use forst_rs_storage::sst::{
    writer::StreamingSstWriter, SstFileInfo, SstReaderImpl, SstWriterImpl, SstWriterOptions,
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
        // R49-H1 defense-in-depth: every input must already belong to the
        // same CF as the compaction job. The engine-side caller (db.rs)
        // builds compaction inputs by reading one CF's file list, so a
        // cross-CF input here would be a structural bug — we trip a debug
        // assertion and continue in release. Once the engine wires per-CF
        // version lists this check becomes a hard error.
        for (_, meta, _) in &self.inputs {
            debug_assert_eq!(
                meta.cf_id, self.cf_id,
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

        if all.is_empty() {
            // Nothing to merge — still produce a VersionEdit that deletes
            // the (empty) inputs. Callers can use this to clear out stray
            // L0 files that contained only bottommost tombstones.
            let deleted = self
                .inputs
                .iter()
                .map(|(lvl, m, _)| (*lvl, m.file_number))
                .collect();
            return Ok(Some(VersionEdit {
                deleted_files: deleted,
                new_files: Vec::new(),
                next_file_number: None,
                last_sequence: None,
            }));
        }

        // 2. Sort by (key ASC, effective_sequence DESC). The effective
        //    sequence uses file_number as the high bits so a newer SST file
        //    wins over an older one even if they share a memtable-local seq.
        all.sort_by(|a, b| match a.key.cmp(&b.key) {
            std::cmp::Ordering::Equal => b.effective_seq().cmp(&a.effective_seq()),
            ord => ord,
        });

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

        // Open the temp file up front so the streaming writer has a sink.
        // R39-L1: shared with FlushJob::temp_path via flush::sst_temp_path so
        // the restore orphan-scan reverses the SAME naming convention used
        // by both writers.
        let tmp_path = sst_temp_path(&self.output_path);
        if let Some(parent) = self.output_path.parent() {
            self.fs.create_dir_all(parent)?;
        }

        // R40-M1: wrap the writer/flush/sync block in a closure so any `?`-propagated error from
        // `emit_key_versions`, `writer.finish`, `wf.flush`, or `wf.sync` triggers a best-effort
        // delete of the staging tmp file before we propagate. The pre-existing R38-H1 fix only
        // covered the post-block `fs.rename` failure; an earlier writer-flow throw would leave
        // `.<num>.sst.tmp` orphaned in-process until the restore orphan-scan picked it up on
        // next restart. Mirrors the same wrapper in `flush.rs` so all SST-write sites share one
        // tmp-leak-safe contract.
        //
        // Closure returns:
        //   * `Ok(Some(info))` — wrote ≥ 1 row, ready to rename into place
        //   * `Ok(None)`       — zero-emit (bottommost tombstones); inner branch already
        //                        best-effort-deleted the tmp file and the caller short-circuits
        //                        with a deletion-only VersionEdit
        //   * `Err(e)`         — writer/flush/sync throw; caller cleans up the tmp file
        let write_outcome: ForstResult<Option<SstFileInfo>> = (|| {
            let mut wf = self
                .fs
                .open_writable_file(&tmp_path, forst_rs_io::WriteMode::CreateNew)?;
            // R49-H1: stamp this compaction's cf_id onto the writer options so
            // the resulting SST footer + SstFileMeta carry CF identity.
            let mut writer_opts = self.writer_options.clone();
            writer_opts.cf_id = self.cf_id;
            let writer_inner = SstWriterImpl::with_options(writer_opts);
            let mut writer = writer_inner.streaming(&mut *wf);

            let mut i = 0;
            let mut emitted = 0u64;
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

                self.emit_key_versions(&mut writer, versions, &mut emitted)?;
            }

            // 4. If we emitted zero rows (e.g. everything was a bottommost
            //    tombstone), drop the writer without finishing — the
            //    streaming-finish() call would have tried to emit a footer
            //    but the streaming writer requires ≥ 1 entry. The temp
            //    file may be partially written, but we never `rename` it,
            //    so the engine never sees it; the caller's `fs` cleanup
            //    will sweep it on the next compaction round. We also
            //    short-circuit with a deletion-only VersionEdit below.
            if emitted == 0 {
                drop(writer);
                drop(wf);
                // Best-effort tmp cleanup; the file may not exist if the
                // writer hasn't emitted anything yet, and the VersionEdit
                // doesn't reference it. R38-L2: surface delete failures
                // via a warn-level log so operators can spot a leaking
                // compaction tmp file (the restore orphan-scan in
                // open_from_checkpoint still catches it on next restart,
                // but a warn-line helps in-process diagnosis).
                if let Err(e) = self.fs.delete_file(&tmp_path) {
                    tracing::warn!(
                        "CompactionJob: zero-emit tmp delete failed for {}: {} \
                         (R38-L2; restore orphan-scan will rename on restart)",
                        tmp_path.display(),
                        e
                    );
                }
                return Ok(None);
            }

            let info = writer.finish()?;
            wf.flush()?;
            wf.sync()?;
            Ok(Some(info))
        })();
        let info = match write_outcome {
            Ok(Some(info)) => info,
            Ok(None) => {
                // Zero-emit: tmp already cleaned up by the inner branch; emit a deletion-only
                // VersionEdit.
                return Ok(Some(VersionEdit {
                    deleted_files: self
                        .inputs
                        .iter()
                        .map(|(lvl, m, _)| (*lvl, m.file_number))
                        .collect(),
                    new_files: Vec::new(),
                    next_file_number: None,
                    last_sequence: None,
                }));
            }
            Err(e) => {
                // Best-effort cleanup; orphan-scan on restart still covers any residual file.
                let _ = self.fs.delete_file(&tmp_path);
                return Err(e);
            }
        };
        // R38-H1: best-effort cleanup of the temp file on rename failure
        // (EXDEV, cross-FS, transient I/O). Without this, a failed compaction
        // leaves a `.<num>.sst.tmp` orphan. We delete before propagating
        // the error; if delete itself fails the file remains visible to the
        // next restore, which now matches `.*.sst.tmp` and renames it out
        // of the active naming space.
        if let Err(e) = self.fs.rename(&tmp_path, &self.output_path) {
            let _ = self.fs.delete_file(&tmp_path);
            return Err(e);
        }
        // R49-H3: fsync(parent_dir) so the rename's directory entry change
        // is durable across a power-loss event. Best-effort: a failure here
        // leaves the SST contents on disk; the next checkpoint cycle will
        // re-attempt the dirent sync via its own copy_live_ssts pass.
        if let Some(parent) = self.output_path.parent() {
            if let Err(e) = self.fs.sync_dir(parent) {
                tracing::warn!(
                    "CompactionJob: sync_dir({}) failed after rename: {} (R49-H3)",
                    parent.display(),
                    e
                );
            }
        }

        // 6. Build the VersionEdit: add the new file, remove all inputs.
        // R49-H1: stamp the job's cf_id onto the meta record.
        debug_assert_eq!(info.cf_id, self.cf_id);
        let meta = SstFileMeta {
            file_number: self.output_file_number,
            cf_id: self.cf_id,
            file_size: info.file_size,
            smallest_key: info.min_key,
            largest_key: info.max_key,
            min_sequence: forst_rs_common::SequenceNumber(info.min_sequence),
            max_sequence: forst_rs_common::SequenceNumber(info.max_sequence),
            num_entries: info.entry_count,
        };
        let deleted = self
            .inputs
            .iter()
            .map(|(lvl, m, _)| (*lvl, m.file_number))
            .collect();
        Ok(Some(VersionEdit {
            new_files: vec![(self.output_level, meta)],
            deleted_files: deleted,
            next_file_number: None,
            last_sequence: None,
        }))
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
        // If pinned was non-empty, every tail entry has `seq < min_active`
        // AND a newer version was already emitted for this user_key —
        // [`mvcc::should_drop`] returns `true` for all of them. Reclaim
        // the entire tail without running the consolidation logic
        // (which would resurrect the tail's newest entry as a Put / Delete
        // tombstone visible to readers below `min_active`, contradicting
        // MVCC's contract that everything below `min_active` is fair
        // game once shadowed).
        if newer_emitted_for_key {
            return Ok(());
        }
        // Pinned was empty (no live snapshots see this key's history) —
        // fall through to the pre-MVCC newest-wins consolidation over the
        // tail. The tail's newest is the only candidate the reduction may
        // emit; older tail entries are shadowed.
        let reduction = tail;
        let newest = &reduction[0];

        // Run the optional compaction filter on the consolidation root —
        // if it says Discard, drop the root. (Pinned siblings emitted
        // above are not re-evaluated; they're contractually required by
        // an active snapshot.)
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
                CompactionDecision::Discard => return Ok(()),
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
                if versions.len() == 2 && versions[1].op_type == OpType::Put {
                    // Drop both entries entirely from this compaction's output.
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
                            if let Some(ref val) = v.value {
                                operands.push(val.as_slice());
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
                    let merged = op.full_merge(&newest.key, base, &reversed)?;
                    writer.add(
                        &newest.key,
                        Some(&merged),
                        newest.sequence,
                        OpType::Put as u8,
                    )?;
                    *emitted += 1;
                } else {
                    // No merge operator — preserve every version exactly as
                    // written. This is correct but doesn't shrink the data.
                    // (If stop_on_delete is true we still emit the older
                    // Delete so subsequent reads see it.)
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
