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

//! MemTable — in-memory sorted KV store for the LSM-tree write path.
//!
//! This module provides the [`crate::memtable::VectorizedMemTable`] implementation based on a hybrid
//! Sorted Run Array + BTreeMap index architecture (design doc 2.3).

pub mod sharded;
pub mod vectorized;

pub use sharded::{ShardedMemTable, DEFAULT_SHARD_COUNT, MAX_SHARD_COUNT};
pub use vectorized::VectorizedMemTable;

use forst_rs_common::OpType;

/// Convenience alias for a single scan row produced by
/// [`VectorizedMemTable::collect_range_entries`]:
/// `(key, value, sequence, op_type)`.
pub type ScanRow = (Vec<u8>, Option<Vec<u8>>, u64, OpType);

/// Result of a single-key point lookup in the MemTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetResult {
    /// The value bytes. `None` for delete tombstones.
    pub value: Option<Vec<u8>>,
    /// The sequence number of this entry.
    pub sequence: u64,
    /// The operation type (Put, Delete, Merge).
    pub op_type: OpType,
}

/// Output sink for batch point-lookups that accepts borrowed slices.
///
/// PR-C6-H2: callers that want to write read values directly into a
/// downstream column buffer (Arrow `BinaryBuilder`, packed byte stream,
/// …) implement this trait and pass `&mut self` into the engine /
/// memtable read path. The memtable inline-cache fast path then writes
/// the value bytes straight into the sink, skipping the per-key
/// `Vec<u8>` allocation that the legacy `Option<Vec<u8>>`-returning
/// signature forced.
///
/// Implementors must:
/// - Append exactly one entry per call (either `append_borrowed` or
///   `append_null`).
/// - Not store the borrow beyond the call (the producer may reuse the
///   underlying buffer immediately after).
pub trait ValueSink {
    /// Append a borrowed value to the sink.
    fn append_borrowed(&mut self, value: &[u8]);

    /// Append a null marker for a key that was not found or tombstoned.
    fn append_null(&mut self);
}

/// Outcome of a sink-aware memtable lookup.
///
/// `HitPut` / `HitTombstone` indicate the memtable already wrote into
/// the sink and the caller should NOT fall back. `Miss` means the
/// memtable has no entry for the key and the caller should attempt
/// the next lookup layer (immutable memtables, SSTs).
#[derive(Debug, PartialEq, Eq)]
pub enum SinkGetOutcome {
    /// Memtable returned a Put entry and wrote it into the sink.
    HitPut,
    /// Memtable returned a tombstone (Delete/SingleDelete) and wrote a
    /// null into the sink.
    HitTombstone,
    /// Memtable has no entry for the key — fall back required.
    Miss,
    /// Memtable returned a Merge entry — the inline fast path cannot
    /// resolve merges; caller must fall back to the full versioned
    /// read path.
    NeedsFullPath,
}

/// Configuration for a VectorizedMemTable.
#[derive(Debug, Clone)]
pub struct MemTableConfig {
    /// Maximum memory (bytes) before the MemTable should be frozen.
    /// Default: 64 MB.
    pub max_size: usize,
    /// Ratio of unsorted entries to sorted count that triggers a merge.
    /// When `unsorted_count > sorted_count * merge_ratio`, merge runs.
    /// Default: 0.25 (25%).
    pub unsorted_merge_ratio: f64,
}

impl Default for MemTableConfig {
    fn default() -> Self {
        Self {
            max_size: 64 * 1024 * 1024,
            unsorted_merge_ratio: 0.25,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_result_equality() {
        let a = GetResult {
            value: Some(b"hello".to_vec()),
            sequence: 1,
            op_type: OpType::Put,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_get_result_delete() {
        let r = GetResult {
            value: None,
            sequence: 5,
            op_type: OpType::Delete,
        };
        assert!(r.value.is_none());
        assert_eq!(r.op_type, OpType::Delete);
    }

    #[test]
    fn test_memtable_config_default() {
        let cfg = MemTableConfig::default();
        assert_eq!(cfg.max_size, 64 * 1024 * 1024);
        assert!((cfg.unsorted_merge_ratio - 0.25).abs() < f64::EPSILON);
    }
}
