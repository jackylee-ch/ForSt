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

use forst_rs_common::{FileNumber, ForstError, ForstResult};
use forst_rs_io::FileSystem;
use forst_rs_storage::merge_operator::MergeOperator;
use forst_rs_storage::sst::{SstReaderImpl, SstWriterImpl, SstWriterOptions};
use forst_rs_storage::version::{SstFileMeta, VersionEdit};

use crate::compaction_filter::{CompactionDecision, CompactionFilter};
use crate::flush::sst_file_path;

/// Description of a single compaction task: merge `inputs` into a new file
/// `output_file_number` placed at level `output_level`.
pub struct CompactionJob {
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
}

impl CompactionJob {
    /// Runs the compaction synchronously and returns a [`VersionEdit`] that
    /// the caller should apply atomically to the [`forst_rs_storage::version::VersionSetImpl`].
    pub fn run(self) -> ForstResult<Option<VersionEdit>> {
        // 1. Gather every entry from every input SST, tagging each with the
        //    source file_number so we can break ties when two SSTs use the
        //    same memtable-local sequence (each VectorizedMemTable starts
        //    counting seqs from 1).
        let mut all: Vec<CompactionEntry> = Vec::new();
        for (level, meta, reader) in &self.inputs {
            let _ = level;
            let scan = reader.scan(&meta.smallest_key, None)?;
            for (key, value, sequence, op_type) in scan {
                all.push(CompactionEntry {
                    key,
                    value,
                    sequence,
                    op_type,
                    file_number: meta.file_number.value(),
                });
            }
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

        // 3. Walk keys, consolidating versions per key. We apply:
        //    - Delete tombstones: drop all older versions for the same key;
        //      emit the tombstone only if NOT bottommost.
        //    - Merge chains: if a merge operator is present, collapse via
        //      full_merge once we reach the Put base (or exhaust the chain).
        let writer_opts = self.writer_options.clone();
        let mut writer = SstWriterImpl::with_options(writer_opts);
        let mut i = 0;
        let mut emitted = 0u64;

        while i < all.len() {
            let key_end = {
                let key = all[i].key.clone();
                let mut j = i + 1;
                while j < all.len() && all[j].key == key {
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
        //    tombstone), produce only a VersionEdit that deletes inputs.
        if emitted == 0 {
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

        // 5. Finalise the writer and write the SST file to disk atomically.
        let (bytes, info) = writer.finish()?;
        let tmp_path = {
            let mut base = self.output_path.clone();
            let existing = base
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            base.set_file_name(format!(".{}.tmp", existing));
            base
        };
        if let Some(parent) = self.output_path.parent() {
            self.fs.create_dir_all(parent)?;
        }
        {
            let mut wf = self
                .fs
                .open_writable_file(&tmp_path, forst_rs_io::WriteMode::CreateNew)?;
            wf.append(&bytes)?;
            wf.flush()?;
            wf.sync()?;
        }
        self.fs.rename(&tmp_path, &self.output_path)?;

        // 6. Build the VersionEdit: add the new file, remove all inputs.
        let meta = SstFileMeta {
            file_number: self.output_file_number,
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

    fn emit_key_versions(
        &self,
        writer: &mut SstWriterImpl,
        versions: &[CompactionEntry],
        emitted: &mut u64,
    ) -> ForstResult<()> {
        debug_assert!(!versions.is_empty());
        // `versions` is sorted by sequence DESC — the newest version is at
        // index 0.

        use forst_rs_common::OpType;
        let newest = &versions[0];

        // Run the optional compaction filter on the newest version — if it
        // says Discard, drop the entire key. (Running against all versions
        // would be wasteful since older versions are already shadowed.)
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
