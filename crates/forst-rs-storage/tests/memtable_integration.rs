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

//! Integration tests for VectorizedMemTable.

use arrow::array::{Array, BinaryArray, UInt64Array};
use forst_rs_common::OpType;
use forst_rs_storage::memtable::{MemTableConfig, VectorizedMemTable};

/// M3 Partial Verification: batch_insert 100K → get all correct.
#[test]
fn test_m3_batch_insert_100k_get_all() {
    let mut mt = VectorizedMemTable::new(MemTableConfig {
        max_size: 512 * 1024 * 1024,
        unsorted_merge_ratio: 0.25,
    });

    let n = 100_000usize;

    // Insert in batches of 1000.
    for batch_start in (0..n).step_by(1000) {
        let batch_end = (batch_start + 1000).min(n);
        let keys: Vec<Vec<u8>> = (batch_start..batch_end)
            .map(|i| format!("m3k_{:08}", i).into_bytes())
            .collect();
        let values: Vec<Vec<u8>> = (batch_start..batch_end)
            .map(|i| format!("m3v_{:08}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let val_refs: Vec<Option<&[u8]>> = values.iter().map(|v| Some(v.as_slice())).collect();
        let ops = vec![1u8; batch_end - batch_start]; // Put (OpType::Put = 1, RocksDB byte-compat)

        mt.batch_insert(&key_refs, &val_refs, &ops).unwrap();
    }

    assert_eq!(mt.num_entries(), n);

    // Verify 100% hit rate.
    let mut hits = 0;
    for i in 0..n {
        let key = format!("m3k_{:08}", i);
        let expected_val = format!("m3v_{:08}", i);
        let result = mt.get(key.as_bytes(), u64::MAX).unwrap();
        assert!(result.is_some(), "key {} not found at index {}", key, i);
        let r = result.unwrap();
        assert_eq!(
            r.value,
            Some(expected_val.into_bytes()),
            "mismatch at {}",
            i
        );
        assert_eq!(r.op_type, OpType::Put);
        hits += 1;
    }
    assert_eq!(hits, n);
}

/// Verify merge entries are stored and retrievable, and that
/// ListAppendMergeOperator can resolve a chain of merge operands.
#[test]
fn test_merge_operator_with_memtable_entries() {
    use forst_rs_storage::merge_operator::{ListAppendMergeOperator, MergeOperator};

    let mut mt = VectorizedMemTable::with_defaults();

    // Put base value
    mt.put(b"list_key", Some(b"item1"), 0).unwrap();
    // Add merge operands
    mt.merge(b"list_key", b"item2").unwrap();
    mt.merge(b"list_key", b"item3").unwrap();
    mt.merge(b"list_key", b"item4").unwrap();

    // Simulate what the engine read path would do:
    // 1. Collect entries from newest to oldest
    // 2. When hitting a Put, call full_merge with base + operands (oldest to newest)
    let op = ListAppendMergeOperator::with_comma();

    // The base value from the Put
    let base = b"item1";
    // Operands in oldest-to-newest order
    let operands: Vec<&[u8]> = vec![b"item2", b"item3", b"item4"];

    let merged = op.full_merge(b"list_key", Some(base), &operands).unwrap();
    assert_eq!(merged, b"item1,item2,item3,item4");

    // Verify the memtable stores all 4 entries
    assert_eq!(mt.num_entries(), 4);

    // Verify get() returns the raw latest merge operand (not resolved)
    let result = mt.get(b"list_key", u64::MAX).unwrap().unwrap();
    assert_eq!(result.op_type, forst_rs_common::OpType::Merge);
    assert_eq!(result.value, Some(b"item4".to_vec()));
}

/// M3 Partial Verification: freeze → ImmutableMemTable → to_flush_batches sorted iteration.
#[test]
fn test_m3_freeze_to_flush_batches_sorted() {
    let mut mt = VectorizedMemTable::new(MemTableConfig {
        max_size: 512 * 1024 * 1024,
        unsorted_merge_ratio: 0.25,
    });

    let n = 10_000usize;
    for i in 0..n {
        let key = format!("fk_{:08}", i);
        let val = format!("fv_{:08}", i);
        mt.put(key.as_bytes(), Some(val.as_bytes()), 0).unwrap();
    }

    // Freeze and export.
    mt.freeze();
    assert!(mt.is_frozen());
    assert!(mt.put(b"fail", Some(b"no"), 0).is_err());

    let batches = mt.to_flush_batches(4096).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, n);

    // Verify sorted order across all batches.
    let mut prev_key: Vec<u8> = Vec::new();
    let mut count = 0;
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let seqs = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let key = keys.value(i).to_vec();
            assert!(key >= prev_key, "keys not sorted at count {}", count);
            assert!(seqs.value(i) > 0, "sequence must be positive");
            prev_key = key;
            count += 1;
        }
    }
    assert_eq!(count, n);
}
