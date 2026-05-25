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

//! Versioned read paths — see spec §6a.5.
//!
//! `get_at(snapshot, user_key)` returns the latest version with seq <= snapshot.seq,
//! Ok(None) if no version exists at snapshot time or if the latest visible version
//! is a deletion tombstone.

use crate::mvcc::Snapshot;
use forst_rs_common::{ForstResult, InternalKey, OpType};
use forst_rs_storage::merge_operator::MergeOperator;
use std::sync::Arc;

/// In-memory or SST entry abstraction for the reader.
pub struct VersionedEntry<'a> {
    pub key: &'a InternalKey,
    pub value: &'a [u8],
}

/// Reads the latest version of `user_key` with seq <= snapshot.seq from a
/// pre-sorted (user_key ASC, sequence DESC) iterator over candidate entries.
/// Returns the value (Some) if found and not a Delete tombstone; None otherwise.
///
/// PRE-A-H1 contract: this entry point treats `Merge` op-types as a
/// terminal "no value" — correct for CFs WITHOUT a merge operator.
/// CFs that have a registered merge operator MUST route through
/// [`get_at_with_merge`] instead; otherwise snapshot reads silently
/// drop every key whose newest visible entry is a Merge operand.
pub fn get_at<'a, I: Iterator<Item = VersionedEntry<'a>>>(
    snapshot: &Snapshot,
    user_key: &[u8],
    candidates: I,
) -> Option<&'a [u8]> {
    for entry in candidates {
        if entry.key.user_key() != user_key {
            // Past our user_key (sort order: user_key ASC).
            return None;
        }
        if entry.key.sequence().0 > snapshot.seq().0 {
            // This version is newer than the snapshot — skip.
            continue;
        }
        // First entry with seq <= snapshot.seq for this user_key wins.
        return match entry.key.op_type() {
            OpType::Delete | OpType::SingleDelete => None,
            OpType::Put => Some(entry.value),
            OpType::Merge => None, // see contract above
        };
    }
    None
}

/// A-H1: merge-aware snapshot get. Walks visible entries in
/// (user_key ASC, seq DESC) order; collects every `Merge` operand
/// (newest → oldest) until we hit a `Put` (base), `Delete`/
/// `SingleDelete` (base = None), exhaust the candidate stream, or
/// step off the requested user_key. Then invokes
/// `merge_op.full_merge(key, base, operands_oldest_to_newest)`.
///
/// Returns:
///   * `Ok(None)` — no visible version OR visible base is a deletion
///     AND there are no operands stacked on top.
///   * `Ok(Some(value))` — either a non-merged `Put`, or the result
///     of `full_merge`.
///   * `Err(...)` — `full_merge` returned an error.
///
/// Note: operands are collected newest→oldest by iteration order,
/// then reversed before being handed to `full_merge`, matching its
/// "oldest to newest" contract.
pub fn get_at_with_merge<'a, I: Iterator<Item = VersionedEntry<'a>>>(
    snapshot: &Snapshot,
    user_key: &[u8],
    candidates: I,
    merge_op: &Arc<dyn MergeOperator>,
) -> ForstResult<Option<Vec<u8>>> {
    // Newest-first operand stack (we'll reverse to oldest-first before
    // invoking full_merge).
    let mut operands_newest_first: Vec<Vec<u8>> = Vec::new();
    let mut base: Option<Vec<u8>> = None;
    let mut hit_terminal = false;

    for entry in candidates {
        if entry.key.user_key() != user_key {
            // Past our user_key — terminate the walk.
            break;
        }
        if entry.key.sequence().0 > snapshot.seq().0 {
            continue;
        }
        match entry.key.op_type() {
            OpType::Delete | OpType::SingleDelete => {
                hit_terminal = true;
                base = None;
                break;
            }
            OpType::Put => {
                hit_terminal = true;
                base = Some(entry.value.to_vec());
                break;
            }
            OpType::Merge => {
                operands_newest_first.push(entry.value.to_vec());
                // Continue walking older versions until we find a
                // terminal Put/Delete or exhaust the chain.
            }
        }
    }

    if operands_newest_first.is_empty() {
        // No merges — emulate `get_at`'s contract.
        return Ok(base);
    }

    // We have at least one operand; need to fold them. Build the
    // oldest→newest order required by full_merge.
    let mut ordered: Vec<&[u8]> = Vec::with_capacity(operands_newest_first.len());
    for op in operands_newest_first.iter().rev() {
        ordered.push(op.as_slice());
    }

    let base_slice: Option<&[u8]> = base.as_deref();
    // If we never hit a terminal, base remains None — semantically a
    // "first op is from None" merge, which matches Flink's
    // ListState semantics where `merge` on a never-put key yields
    // operand-list-only.
    let _ = hit_terminal;
    let merged = merge_op.full_merge(user_key, base_slice, &ordered)?;
    Ok(Some(merged))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::{DbId, SnapshotRegistry};
    use forst_rs_common::SequenceNumber;

    fn ik(uk: &[u8], seq: u64, op: OpType) -> InternalKey {
        InternalKey::new(uk.to_vec(), SequenceNumber::new(seq), op)
    }

    #[test]
    fn returns_latest_version_at_or_before_snapshot() {
        let reg = SnapshotRegistry::new();
        let snap = reg.capture(DbId(1), SequenceNumber::new(10));
        let keys = [
            ik(b"k", 12, OpType::Put),
            ik(b"k", 8, OpType::Put),
            ik(b"k", 5, OpType::Put),
        ];
        let vals = [b"v12".as_slice(), b"v8".as_slice(), b"v5".as_slice()];
        let candidates = keys
            .iter()
            .zip(vals.iter())
            .map(|(k, v)| VersionedEntry { key: k, value: v });
        assert_eq!(get_at(&snap, b"k", candidates), Some(b"v8".as_slice()));
    }

    #[test]
    fn skips_to_other_user_key_returns_none() {
        let reg = SnapshotRegistry::new();
        let snap = reg.capture(DbId(1), SequenceNumber::new(10));
        let keys = [ik(b"other", 5, OpType::Put)];
        let vals = [b"v".as_slice()];
        let candidates = keys
            .iter()
            .zip(vals.iter())
            .map(|(k, v)| VersionedEntry { key: k, value: v });
        assert_eq!(get_at(&snap, b"k", candidates), None);
    }

    #[test]
    fn delete_tombstone_returns_none() {
        let reg = SnapshotRegistry::new();
        let snap = reg.capture(DbId(1), SequenceNumber::new(10));
        let keys = [ik(b"k", 8, OpType::Delete), ik(b"k", 5, OpType::Put)];
        let vals = [b"".as_slice(), b"v5".as_slice()];
        let candidates = keys
            .iter()
            .zip(vals.iter())
            .map(|(k, v)| VersionedEntry { key: k, value: v });
        assert_eq!(get_at(&snap, b"k", candidates), None);
    }

    #[test]
    fn no_version_visible_returns_none() {
        let reg = SnapshotRegistry::new();
        let snap = reg.capture(DbId(1), SequenceNumber::new(3));
        let keys = [ik(b"k", 5, OpType::Put)];
        let vals = [b"v5".as_slice()];
        let candidates = keys
            .iter()
            .zip(vals.iter())
            .map(|(k, v)| VersionedEntry { key: k, value: v });
        assert_eq!(get_at(&snap, b"k", candidates), None);
    }
}
