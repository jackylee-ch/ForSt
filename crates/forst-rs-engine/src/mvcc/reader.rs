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
use forst_rs_common::{InternalKey, OpType};

/// In-memory or SST entry abstraction for the reader.
pub struct VersionedEntry<'a> {
    pub key: &'a InternalKey,
    pub value: &'a [u8],
}

/// Reads the latest version of `user_key` with seq <= snapshot.seq from a
/// pre-sorted (user_key ASC, sequence DESC) iterator over candidate entries.
/// Returns the value (Some) if found and not a Delete tombstone; None otherwise.
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
            OpType::Merge => None, // Merge not implemented in v1.
        };
    }
    None
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
