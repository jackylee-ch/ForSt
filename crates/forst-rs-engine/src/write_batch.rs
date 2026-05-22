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

//! Write batch representation. See `2.8_read_write_paths.md` §2.3.
//!
//! A [`WriteBatch`] groups multiple mutations (Put / Delete / Merge) that are
//! applied atomically in the order they were appended. The engine's
//! `batch_write` path allocates a contiguous range of sequence numbers and
//! dispatches each entry to its column family's memtable.

use std::borrow::Cow;
use std::collections::HashMap;

use forst_rs_common::{ColumnFamilyId, OpType};

use crate::column_family::ColumnFamilyHandle;

/// A single entry in a [`WriteBatch`].
///
/// PR-B5-H1 (zero-copy): `key`/`value` are stored as [`Cow<'a, [u8]>`] so the
/// FFI hot path (`frs_vectorized_batch_put`, `frs_vectorized_batch_delete`)
/// can push borrowed slices straight from the caller-owned input buffer
/// without a per-entry `Vec<u8>` alloc + memcpy. The batch is applied
/// synchronously inside the same FFI call, so the borrow always outlives
/// `db.batch_write`. Owned callers (tests, future async paths) hand in
/// `Cow::Owned` via the `*_owned` helpers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteBatchEntry<'a> {
    /// Target column family id.
    pub cf_id: ColumnFamilyId,
    /// User key.
    pub key: Cow<'a, [u8]>,
    /// Value payload. `None` for [`OpType::Delete`] / [`OpType::SingleDelete`].
    pub value: Option<Cow<'a, [u8]>>,
    /// Kind of mutation.
    pub op_type: OpType,
}

/// An ordered batch of mutations applied atomically by the engine.
///
/// Entries are appended in insertion order; the engine assigns sequence
/// numbers in the same order when dispatching the batch. The optional
/// lifetime `'a` is the lifetime of any borrowed source buffer used in
/// zero-copy appends (`put`/`delete`/`merge`/`single_delete` all borrow
/// their key/value slices). Use `'static` (the default) for batches that
/// don't borrow.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteBatch<'a> {
    entries: Vec<WriteBatchEntry<'a>>,
}

impl<'a> WriteBatch<'a> {
    /// Creates an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a batch with the given capacity hint.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entries: Vec::with_capacity(cap),
        }
    }

    /// Appends a Put mutation, borrowing `key` and `value` from the caller.
    ///
    /// PR-B5-H1 zero-copy: stores `Cow::Borrowed` references; no allocation
    /// happens here. The borrows must live until [`Self::into_entries`] is
    /// drained (typically the same FFI/engine call).
    pub fn put(&mut self, cf: &ColumnFamilyHandle, key: &'a [u8], value: &'a [u8]) -> &mut Self {
        self.entries.push(WriteBatchEntry {
            cf_id: cf.id(),
            key: Cow::Borrowed(key),
            value: Some(Cow::Borrowed(value)),
            op_type: OpType::Put,
        });
        self
    }

    /// Appends a Delete mutation, borrowing `key` from the caller.
    pub fn delete(&mut self, cf: &ColumnFamilyHandle, key: &'a [u8]) -> &mut Self {
        self.entries.push(WriteBatchEntry {
            cf_id: cf.id(),
            key: Cow::Borrowed(key),
            value: None,
            op_type: OpType::Delete,
        });
        self
    }

    /// Appends a SingleDelete mutation. Mirrors RocksDB's `SingleDelete`:
    /// it is semantically equivalent to `Delete` for point reads, but lets
    /// compaction elide both the tombstone and its matching `Put` in one
    /// pass when the caller can guarantee the key has been `Put` at most
    /// once since the last `Delete` or `SingleDelete`.
    ///
    /// Misuse is deterministic but wrong: if the key was `Put` multiple
    /// times, compaction may drop the `SingleDelete` together with only
    /// the newest `Put`, leaving older shadowed `Put`s visible on the
    /// next read. Use when the caller owns the write history (e.g.
    /// changelog producers, CDC sinks).
    pub fn single_delete(&mut self, cf: &ColumnFamilyHandle, key: &'a [u8]) -> &mut Self {
        self.entries.push(WriteBatchEntry {
            cf_id: cf.id(),
            key: Cow::Borrowed(key),
            value: None,
            op_type: OpType::SingleDelete,
        });
        self
    }

    /// Appends a Merge mutation, borrowing `key` and `operand`.
    pub fn merge(&mut self, cf: &ColumnFamilyHandle, key: &'a [u8], operand: &'a [u8]) -> &mut Self {
        self.entries.push(WriteBatchEntry {
            cf_id: cf.id(),
            key: Cow::Borrowed(key),
            value: Some(Cow::Borrowed(operand)),
            op_type: OpType::Merge,
        });
        self
    }

    /// Appends a Put with owned key/value buffers.
    ///
    /// Use this from callers that already own `Vec<u8>` and would otherwise
    /// have to clone into a temporary slice. The owned form converts into
    /// `Cow::Owned` directly.
    pub fn put_owned(
        &mut self,
        cf: &ColumnFamilyHandle,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> &mut Self {
        self.entries.push(WriteBatchEntry {
            cf_id: cf.id(),
            key: Cow::Owned(key),
            value: Some(Cow::Owned(value)),
            op_type: OpType::Put,
        });
        self
    }

    /// Returns the number of entries in the batch.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the batch has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns an iterator over the entries in insertion order.
    pub fn entries(&self) -> &[WriteBatchEntry<'a>] {
        &self.entries
    }

    /// Clears all entries, keeping the allocated capacity.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Groups entries by column family id, preserving per-cf insertion order.
    ///
    /// Returns a map from `ColumnFamilyId` to `(first_index, entries_slice_indices)`
    /// where `first_index` is the position of that CF's first entry in the
    /// original batch (used for sequence number allocation).
    pub fn group_by_cf(&self) -> HashMap<ColumnFamilyId, Vec<usize>> {
        let mut map: HashMap<ColumnFamilyId, Vec<usize>> = HashMap::new();
        for (i, e) in self.entries.iter().enumerate() {
            map.entry(e.cf_id).or_default().push(i);
        }
        map
    }

    /// Consumes the batch and returns the entries vector. Borrowed Cows
    /// retain their borrow; owned Cows retain their `Vec<u8>`.
    pub fn into_entries(self) -> Vec<WriteBatchEntry<'a>> {
        self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(id: u32, name: &str) -> ColumnFamilyHandle {
        ColumnFamilyHandle::new(ColumnFamilyId(id), name)
    }

    #[test]
    fn test_new_batch_is_empty() {
        let b = WriteBatch::new();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn test_put_appends_entry() {
        let h = handle(1, "default");
        let mut b = WriteBatch::new();
        b.put(&h, b"k", b"v");
        assert_eq!(b.len(), 1);
        let e = &b.entries()[0];
        assert_eq!(e.cf_id, ColumnFamilyId(1));
        assert_eq!(e.key.as_ref(), b"k");
        assert_eq!(e.value.as_deref(), Some(b"v".as_ref()));
        assert_eq!(e.op_type, OpType::Put);
    }

    #[test]
    fn test_delete_sets_none_value() {
        let h = handle(1, "default");
        let mut b = WriteBatch::new();
        b.delete(&h, b"k");
        assert_eq!(b.entries()[0].value, None);
        assert_eq!(b.entries()[0].op_type, OpType::Delete);
    }

    #[test]
    fn test_merge_records_operand() {
        let h = handle(1, "default");
        let mut b = WriteBatch::new();
        b.merge(&h, b"k", b"operand");
        assert_eq!(b.entries()[0].value.as_deref(), Some(b"operand".as_ref()));
        assert_eq!(b.entries()[0].op_type, OpType::Merge);
    }

    #[test]
    fn test_chained_put_put_delete() {
        let h = handle(1, "default");
        let mut b = WriteBatch::new();
        b.put(&h, b"k1", b"v1")
            .put(&h, b"k2", b"v2")
            .delete(&h, b"k3");
        assert_eq!(b.len(), 3);
        assert_eq!(b.entries()[0].key.as_ref(), b"k1");
        assert_eq!(b.entries()[1].key.as_ref(), b"k2");
        assert_eq!(b.entries()[2].key.as_ref(), b"k3");
    }

    #[test]
    fn test_clear_keeps_capacity() {
        let h = handle(1, "a");
        let mut b = WriteBatch::with_capacity(16);
        // Use put_owned so the keys outlive the per-iteration scope (the
        // borrowed `put` would tie the batch to each format!() temporary).
        for i in 0..5 {
            b.put_owned(&h, format!("k{i}").into_bytes(), b"v".to_vec());
        }
        assert_eq!(b.len(), 5);
        b.clear();
        assert!(b.is_empty());
    }

    #[test]
    fn test_group_by_cf_basic() {
        let a = handle(1, "a");
        let b_cf = handle(2, "b");
        let mut b = WriteBatch::new();
        b.put(&a, b"k1", b"v1")
            .put(&b_cf, b"k2", b"v2")
            .put(&a, b"k3", b"v3")
            .delete(&b_cf, b"k4");

        let groups = b.group_by_cf();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[&ColumnFamilyId(1)], vec![0, 2]);
        assert_eq!(groups[&ColumnFamilyId(2)], vec![1, 3]);
    }

    #[test]
    fn test_group_by_cf_empty_batch() {
        let b = WriteBatch::new();
        assert!(b.group_by_cf().is_empty());
    }

    #[test]
    fn test_into_entries_takes_ownership() {
        let h = handle(1, "x");
        let mut b = WriteBatch::new();
        b.put(&h, b"k", b"v");
        let entries = b.into_entries();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_default_is_new() {
        let a = WriteBatch::default();
        let b = WriteBatch::new();
        assert_eq!(a, b);
    }

    #[test]
    fn test_clone_preserves_entries() {
        let h = handle(3, "c");
        let mut b = WriteBatch::new();
        b.put(&h, b"k", b"v");
        let c = b.clone();
        assert_eq!(b, c);
    }
}
