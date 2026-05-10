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

//! Compaction policy for MVCC retention — see spec §6a.5.
//!
//! [`should_drop`] is the pure decision function used by the compaction
//! worker to decide whether a single internal-key entry can be discarded
//! while merging input SST files into output SSTs.
//!
//! Contract:
//!
//! * Returns `false` (keep) if the entry is needed by an active snapshot
//!   (`entry.seq >= min_active_snapshot`).
//! * Returns `true` (drop) only when `entry.seq < min_active_snapshot`
//!   AND a newer version for the same `user_key` exists in the merged
//!   input — that newer version serves every snapshot at
//!   `seq >= entry.seq + 1`, so this entry is shadowed for all live
//!   readers.
//! * Tombstones (`Delete` / `SingleDelete`) at the **tail** of the
//!   version chain (no newer version) are kept so readers see "deleted"
//!   rather than "missing" — this is required for correct MVCC reads
//!   below `min_active_snapshot`.

use forst_rs_common::{OpType, SequenceNumber};

/// Decide whether a versioned entry can be dropped during compaction.
///
/// See module docs for the contract. Pure function — no I/O, no state.
///
/// # Parameters
///
/// * `entry_seq` — sequence number stamped on the entry being considered.
/// * `entry_op` — operation type (`Put` / `Delete` / `Merge` / `SingleDelete`).
/// * `newer_version_exists` — `true` iff the compaction merger has already
///   emitted (or will emit) a newer version for the same `user_key` in the
///   output SST.
/// * `min_active_snapshot` — smallest sequence number held by any live
///   snapshot, as reported by [`crate::mvcc::SnapshotRegistry::min_active`].
///   Pass `SequenceNumber::new(u64::MAX)` (or any sentinel ≥ all entries)
///   when no snapshots are active.
#[inline]
pub fn should_drop(
    entry_seq: SequenceNumber,
    entry_op: OpType,
    newer_version_exists: bool,
    min_active_snapshot: SequenceNumber,
) -> bool {
    // Keep if any active snapshot might need this version.
    if entry_seq.0 >= min_active_snapshot.0 {
        return false;
    }
    // Below min_active: drop only if a newer version is visible to all
    // current snapshots. For tombstones, the same rule applies — but when
    // `newer_version_exists == false` the tombstone is the tail of the
    // version chain and must be retained so readers below min_active see
    // "deleted" rather than "missing".
    match entry_op {
        OpType::Delete | OpType::SingleDelete => newer_version_exists,
        OpType::Put | OpType::Merge => newer_version_exists,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_when_at_or_above_min_active() {
        assert!(!should_drop(
            SequenceNumber::new(10),
            OpType::Put,
            true,
            SequenceNumber::new(10)
        ));
        assert!(!should_drop(
            SequenceNumber::new(15),
            OpType::Put,
            true,
            SequenceNumber::new(10)
        ));
    }

    #[test]
    fn drop_below_min_active_when_newer_exists() {
        assert!(should_drop(
            SequenceNumber::new(5),
            OpType::Put,
            true,
            SequenceNumber::new(10)
        ));
    }

    #[test]
    fn keep_below_min_active_when_no_newer() {
        assert!(!should_drop(
            SequenceNumber::new(5),
            OpType::Put,
            false,
            SequenceNumber::new(10)
        ));
    }

    #[test]
    fn keep_tombstone_at_tail() {
        // Tombstone with no newer version: keep so readers see "deleted".
        assert!(!should_drop(
            SequenceNumber::new(5),
            OpType::Delete,
            false,
            SequenceNumber::new(10)
        ));
    }

    #[test]
    fn drop_tombstone_when_newer_put_exists() {
        assert!(should_drop(
            SequenceNumber::new(5),
            OpType::Delete,
            true,
            SequenceNumber::new(10)
        ));
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn drop_implies_safe_to_drop(
            entry_seq in 0u64..1000,
            min_active in 0u64..1000,
            newer_exists: bool,
            op_ord in 0u8..2,
        ) {
            let op = match op_ord {
                0 => OpType::Delete,
                _ => OpType::Put,
            };
            let dropped = should_drop(
                SequenceNumber::new(entry_seq),
                op,
                newer_exists,
                SequenceNumber::new(min_active),
            );
            if dropped {
                prop_assert!(entry_seq < min_active);
                prop_assert!(newer_exists);
            }
        }
    }
}
