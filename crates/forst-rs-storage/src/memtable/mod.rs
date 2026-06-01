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

pub mod artifact;
pub mod sharded;
pub mod vectorized;

pub use artifact::{
    deserialize_memtable_batches, serialize_memtable_batches,
    serialize_memtable_batches_to_writer, MemtableArtifactWriter,
};
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

/// Borrowed value reference returned by the zero-copy point-lookup
/// primitive [`crate::memtable::VectorizedMemTable::get_borrowed`].
///
/// PR-B7-H2: callers that route through `prefix_scan_iter_owned`'s
/// streaming hot path want to skip the per-row `Vec<u8>` materialisation
/// baked into the legacy `get() -> Option<GetResult>` signature. This
/// enum exposes the two sources of value bytes the memtable holds:
///
/// - `Inline(&'a [u8])` — borrows directly from the `inline_value`
///   `Box<[u8]>` cache (the small-Put fast path) OR from the columnar
///   `value_data: Vec<u8>` buffer (the MVCC/oversized fallback). The
///   borrow is valid for `'a` (the lifetime of the `&self` borrow that
///   produced it); no per-key allocation occurs.
/// - `Heap(Vec<u8>)` — reserved for sources that legitimately materialise
///   a fresh allocation (e.g. future merge-operator results that combine
///   multiple stored versions into a new buffer). Currently unused on
///   the hot path; kept in the enum so the API is forward-compatible
///   without breaking changes.
///
/// Both variants implement [`AsRef<[u8]>`] for uniform consumption.
#[derive(Debug)]
pub enum MemtableValueRef<'a> {
    /// Value bytes borrowed from the memtable's own storage. No
    /// allocation; valid until the next write to this key.
    Inline(&'a [u8]),
    /// Value bytes owned by the caller. Used when the read path had to
    /// allocate (e.g. merge result). Rare on the hot path.
    Heap(Vec<u8>),
}

impl<'a> MemtableValueRef<'a> {
    /// Borrows the underlying bytes without consuming the reference.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            MemtableValueRef::Inline(s) => s,
            MemtableValueRef::Heap(v) => v.as_slice(),
        }
    }

    /// Consumes the reference into an owned `Vec<u8>`. For `Inline` this
    /// allocates and copies; for `Heap` it is a move (no copy). Callers
    /// that emit through a `Vec<u8>`-typed channel use this at the
    /// emission boundary, after the borrowed view is no longer needed.
    #[inline]
    pub fn into_owned(self) -> Vec<u8> {
        match self {
            MemtableValueRef::Inline(s) => s.to_vec(),
            MemtableValueRef::Heap(v) => v,
        }
    }
}

impl<'a> AsRef<[u8]> for MemtableValueRef<'a> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Borrowed-value form of [`GetResult`] returned by
/// [`crate::memtable::VectorizedMemTable::get_borrowed`].
///
/// PR-B7-H2: see [`MemtableValueRef`] for the rationale. The `value`
/// is `None` for delete tombstones, matching `GetResult` semantics.
#[derive(Debug)]
pub struct GetBorrowedResult<'a> {
    /// The value bytes. `None` for delete tombstones.
    pub value: Option<MemtableValueRef<'a>>,
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
