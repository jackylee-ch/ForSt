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
