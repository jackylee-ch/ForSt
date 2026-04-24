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

use std::collections::HashMap;

use forst_rs_common::{ColumnFamilyId, OpType};

use crate::column_family::ColumnFamilyHandle;

/// A single entry in a [`WriteBatch`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteBatchEntry {
    /// Target column family id.
    pub cf_id: ColumnFamilyId,
    /// User key.
    pub key: Vec<u8>,
    /// Value payload. `None` for [`OpType::Delete`] / [`OpType::SingleDelete`].
    pub value: Option<Vec<u8>>,
    /// Kind of mutation.
    pub op_type: OpType,
}

/// An ordered batch of mutations applied atomically by the engine.
///
/// Entries are appended in insertion order; the engine assigns sequence
/// numbers in the same order when dispatching the batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteBatch {
    entries: Vec<WriteBatchEntry>,
}

impl WriteBatch {
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

    /// Appends a Put mutation.
    pub fn put(&mut self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> &mut Self {
        self.entries.push(WriteBatchEntry {
            cf_id: cf.id(),
            key: key.to_vec(),
            value: Some(value.to_vec()),
            op_type: OpType::Put,
        });
        self
    }

    /// Appends a Delete mutation.
    pub fn delete(&mut self, cf: &ColumnFamilyHandle, key: &[u8]) -> &mut Self {
        self.entries.push(WriteBatchEntry {
            cf_id: cf.id(),
            key: key.to_vec(),
            value: None,
            op_type: OpType::Delete,
        });
        self
    }

    /// Appends a Merge mutation.
    pub fn merge(&mut self, cf: &ColumnFamilyHandle, key: &[u8], operand: &[u8]) -> &mut Self {
        self.entries.push(WriteBatchEntry {
            cf_id: cf.id(),
            key: key.to_vec(),
            value: Some(operand.to_vec()),
            op_type: OpType::Merge,
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
    pub fn entries(&self) -> &[WriteBatchEntry] {
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

    /// Consumes the batch and returns the owned entries vector.
    pub fn into_entries(self) -> Vec<WriteBatchEntry> {
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
        assert_eq!(e.key, b"k");
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
        b.put(&h, b"k1", b"v1").put(&h, b"k2", b"v2").delete(&h, b"k3");
        assert_eq!(b.len(), 3);
        assert_eq!(b.entries()[0].key, b"k1");
        assert_eq!(b.entries()[1].key, b"k2");
        assert_eq!(b.entries()[2].key, b"k3");
    }

    #[test]
    fn test_clear_keeps_capacity() {
        let h = handle(1, "a");
        let mut b = WriteBatch::with_capacity(16);
        for i in 0..5 {
            b.put(&h, format!("k{i}").as_bytes(), b"v");
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
