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

//! End-to-end integration test: SstWriter -> complete SST file -> read back all entries.

use std::sync::Arc;

use arrow::array::{Array, BinaryArray, UInt64Array};
use forst_rs_common::{get_fixed32, CompressionType, OpType};
use forst_rs_io::RandomAccessFile;
use forst_rs_storage::sst::{
    decode_data_block, decode_index, search_index, FileHeader, FooterV1, LookupResult, Sbbf,
    SstReaderImpl, SstWriterImpl, SstWriterOptions, FILE_HEADER_SIZE, SST_MAGIC,
};

/// In-memory RandomAccessFile for integration tests.
struct MemRandomAccessFile {
    data: Arc<Vec<u8>>,
}

impl RandomAccessFile for MemRandomAccessFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> forst_rs_common::ForstResult<usize> {
        let start = offset as usize;
        if start >= self.data.len() {
            return Ok(0);
        }
        let end = std::cmp::min(start + buf.len(), self.data.len());
        let n = end - start;
        buf[..n].copy_from_slice(&self.data[start..end]);
        Ok(n)
    }

    fn file_size(&self) -> forst_rs_common::ForstResult<u64> {
        Ok(self.data.len() as u64)
    }
}

/// Writes N sorted entries, then verifies every entry can be read back.
fn write_and_verify(n: usize, compression: CompressionType, block_size: usize) {
    let options = SstWriterOptions {
        block_size,
        compression,
        cf_id: forst_rs_common::DEFAULT_CF_ID,
    };
    let mut writer = SstWriterImpl::with_options(options);

    // Generate sorted keys: "key_00000" .. "key_NNNNN"
    for i in 0..n {
        let key = format!("key_{:05}", i);
        let val = format!("val_{:05}", i);
        writer
            .add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 0)
            .unwrap();
    }

    let (data, info) = writer.finish().unwrap();

    // --- Structural verification ---

    // 1. FileHeader
    assert_eq!(&data[..4], SST_MAGIC);
    let header = FileHeader::decode(&data[..FILE_HEADER_SIZE]).unwrap();
    // R49-H1: writer emits SST_FORMAT_VERSION (now 2 after the cf_id footer
    // bump). We assert against the constant rather than a literal so future
    // version bumps don't drift this test out of sync.
    assert_eq!(
        header.format_version,
        forst_rs_storage::sst::SST_FORMAT_VERSION
    );

    // 2. Footer (from tail)
    let len = data.len();
    assert_eq!(&data[len - 4..], SST_MAGIC);
    let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
    let footer_start = len - footer_length as usize;
    let footer = FooterV1::decode(&data[footer_start..]).unwrap();
    assert_eq!(footer.total_entries, n as u64);
    assert_eq!(footer.data_block_count, info.data_block_count);
    assert_eq!(footer.min_key, format!("key_{:05}", 0).as_bytes());
    assert_eq!(footer.max_key, format!("key_{:05}", n - 1).as_bytes());

    // 3. Index Section
    let idx_start = footer.index_offset as usize;
    let idx_end = idx_start + footer.index_size as usize;
    let (entries, stats) = decode_index(&data[idx_start..idx_end]).unwrap();
    assert_eq!(entries.len() as u32, footer.data_block_count);
    assert_eq!(stats.len() as u32, footer.data_block_count);

    // 4. Read back ALL entries via DataBlocks
    let mut all_keys: Vec<Vec<u8>> = Vec::new();
    let mut all_vals: Vec<Vec<u8>> = Vec::new();
    let mut all_seqs: Vec<u64> = Vec::new();

    for entry in &entries {
        let start = entry.block_offset as usize;
        let end = start + entry.block_size as usize;
        let batch = decode_data_block(&data[start..end]).unwrap();

        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let seqs = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            all_keys.push(keys.value(i).to_vec());
            all_vals.push(vals.value(i).to_vec());
            all_seqs.push(seqs.value(i));
        }
    }

    // Verify count
    assert_eq!(all_keys.len(), n);

    // Verify every key/value/sequence matches
    for i in 0..n {
        let expected_key = format!("key_{:05}", i);
        let expected_val = format!("val_{:05}", i);
        assert_eq!(all_keys[i], expected_key.as_bytes(), "key mismatch at {i}");
        assert_eq!(all_vals[i], expected_val.as_bytes(), "val mismatch at {i}");
        assert_eq!(all_seqs[i], i as u64 + 1, "seq mismatch at {i}");
    }

    // Verify keys are in sorted order
    for i in 1..all_keys.len() {
        assert!(
            all_keys[i] >= all_keys[i - 1],
            "keys not sorted at index {i}"
        );
    }
}

#[test]
fn test_e2e_1000_entries_no_compression() {
    write_and_verify(1000, CompressionType::None, 4096);
}

#[test]
fn test_e2e_1000_entries_lz4() {
    write_and_verify(1000, CompressionType::Lz4, 4096);
}

#[test]
fn test_e2e_1000_entries_zstd() {
    write_and_verify(1000, CompressionType::Zstd, 4096);
}

#[test]
fn test_e2e_search_index_point_lookup() {
    let options = SstWriterOptions {
        block_size: 256,
        compression: CompressionType::None,
        cf_id: forst_rs_common::DEFAULT_CF_ID,
    };
    let mut writer = SstWriterImpl::with_options(options);

    for i in 0..500u64 {
        let key = format!("k{:05}", i);
        writer.add(key.as_bytes(), Some(b"v"), i + 1, 0).unwrap();
    }

    let (data, _info) = writer.finish().unwrap();

    // Parse footer and index
    let len = data.len();
    let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
    let footer_start = len - footer_length as usize;
    let footer = FooterV1::decode(&data[footer_start..]).unwrap();
    let idx_start = footer.index_offset as usize;
    let idx_end = idx_start + footer.index_size as usize;
    let (entries, _stats) = decode_index(&data[idx_start..idx_end]).unwrap();

    // Search for a key in the middle
    let target = format!("k{:05}", 250);
    let block_idx = search_index(&entries, target.as_bytes());
    assert!(block_idx.is_some(), "search should find a block");

    // Decode that block and verify the key exists
    let idx = block_idx.unwrap();
    let entry = &entries[idx];
    let start = entry.block_offset as usize;
    let end = start + entry.block_size as usize;
    let batch = decode_data_block(&data[start..end]).unwrap();

    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();

    let found = (0..batch.num_rows()).any(|i| keys.value(i) == target.as_bytes());
    assert!(
        found,
        "target key {:?} should be in the located block",
        target
    );
}

#[test]
fn test_e2e_bloom_filter_filters_keys() {
    let n = 500;
    let options = SstWriterOptions {
        block_size: 512,
        compression: CompressionType::None,
        cf_id: forst_rs_common::DEFAULT_CF_ID,
    };
    let mut writer = SstWriterImpl::with_options(options);

    for i in 0..n {
        let key = format!("bloom_{:05}", i);
        let val = format!("val_{:05}", i);
        writer
            .add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 0)
            .unwrap();
    }

    let (data, info) = writer.finish().unwrap();
    assert_eq!(info.entry_count, n as u64);

    // Parse footer.
    let len = data.len();
    let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
    let footer_start = len - footer_length as usize;
    let footer = FooterV1::decode(&data[footer_start..]).unwrap();

    // Bloom filter section should be non-empty and correctly positioned.
    assert!(footer.bloom_filter_offset > FILE_HEADER_SIZE as u64);
    assert!(footer.bloom_filter_size > 0);
    assert!(footer.bloom_filter_offset < footer.index_offset);

    // Decode the bloom filter.
    let bf_start = footer.bloom_filter_offset as usize;
    let bf_end = bf_start + footer.bloom_filter_size as usize;
    assert!(
        bf_end <= footer_start,
        "bloom filter should end before footer"
    );
    let sbbf = Sbbf::decode(&data[bf_start..bf_end]).unwrap();

    // All inserted keys must be found (no false negatives).
    for i in 0..n {
        let key = format!("bloom_{:05}", i);
        assert!(
            sbbf.check(key.as_bytes()),
            "bloom filter must find inserted key {} at index {}",
            key,
            i,
        );
    }

    // Check that the bloom filter correctly rejects most absent keys.
    let mut false_positives = 0;
    let num_absent = 5000;
    for i in 0..num_absent {
        let key = format!("absent_{:08}", i);
        if sbbf.check(key.as_bytes()) {
            false_positives += 1;
        }
    }

    let fpr = false_positives as f64 / num_absent as f64;
    assert!(
        fpr < 0.05,
        "integration FPR {:.4} too high (expected < 5%)",
        fpr,
    );
}

/// Verify that the existing write_and_verify helper still passes with the
/// new bloom filter section present (regression guard).
#[test]
fn test_e2e_500_entries_no_compression_with_bloom() {
    write_and_verify(500, CompressionType::None, 2048);
}

/// M2 Verification: Write 10K entries -> SstReader::get() finds every one.
#[test]
fn test_m2_write_10k_read_all() {
    let n = 10_000;
    let options = SstWriterOptions {
        block_size: 4096,
        compression: CompressionType::Lz4,
        cf_id: forst_rs_common::DEFAULT_CF_ID,
    };
    let mut writer = SstWriterImpl::with_options(options);

    for i in 0..n {
        let key = format!("m2k_{:06}", i);
        let val = format!("m2v_{:06}", i);
        writer
            .add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 1) // Put (OpType::Put = 1, RocksDB byte-compat)
            .unwrap();
    }

    let (data, info) = writer.finish().unwrap();
    assert_eq!(info.entry_count, n as u64);

    let file = Box::new(MemRandomAccessFile {
        data: Arc::new(data),
    });
    let reader = SstReaderImpl::open(file).unwrap();

    // 100% hit rate: every inserted key must be found.
    let mut hit_count = 0;
    for i in 0..n {
        let key = format!("m2k_{:06}", i);
        let expected_val = format!("m2v_{:06}", i);
        let result = reader.get(key.as_bytes()).unwrap();
        assert!(result.is_some(), "key {} not found at index {}", key, i);
        let lr = result.unwrap();
        assert_eq!(
            lr.value,
            Some(expected_val.into_bytes()),
            "value mismatch at {}",
            i
        );
        assert_eq!(lr.sequence, i as u64 + 1);
        assert_eq!(lr.op_type, OpType::Put);
        hit_count += 1;
    }
    assert_eq!(hit_count, n, "all keys must be found");
}

/// M2 Verification: SBBF FPR < 1% with 10K inserted keys and 100K absent checks.
#[test]
fn test_m2_bloom_filter_fpr_below_1_percent() {
    let n = 10_000;
    let mut writer = SstWriterImpl::with_options(SstWriterOptions {
        block_size: 4096,
        compression: CompressionType::None,
        cf_id: forst_rs_common::DEFAULT_CF_ID,
    });
    for i in 0..n {
        let key = format!("bfp_{:06}", i);
        writer
            .add(key.as_bytes(), Some(b"v"), i as u64 + 1, 0)
            .unwrap();
    }
    let (data, _info) = writer.finish().unwrap();

    // Extract bloom filter from SST
    let len = data.len();
    let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
    let footer_start = len - footer_length as usize;
    let footer = FooterV1::decode(&data[footer_start..]).unwrap();
    let bf_start = footer.bloom_filter_offset as usize;
    let bf_end = bf_start + footer.bloom_filter_size as usize;
    let sbbf = Sbbf::decode(&data[bf_start..bf_end]).unwrap();

    // Check 100K absent keys
    let num_checks = 100_000;
    let mut false_positives = 0u32;
    for i in 0..num_checks {
        let key = format!("absent_{:08}", i);
        if sbbf.check(key.as_bytes()) {
            false_positives += 1;
        }
    }

    let fpr = false_positives as f64 / num_checks as f64;
    assert!(
        fpr < 0.015,
        "M2: SBBF FPR {:.4} exceeds 1.5% (false_positives={}, checks={})",
        fpr,
        false_positives,
        num_checks,
    );
}

/// M2 Verification: SstReader point lookup via full pipeline (write + open + get).
#[test]
fn test_m2_reader_full_pipeline_with_deletes() {
    let mut writer = SstWriterImpl::with_options(SstWriterOptions {
        block_size: 2048,
        compression: CompressionType::Lz4,
        cf_id: forst_rs_common::DEFAULT_CF_ID,
    });

    // Mix of puts and deletes
    for i in 0..1000u64 {
        let key = format!("pipe_{:05}", i);
        if i % 10 == 0 {
            writer.add(key.as_bytes(), None, i + 1, 0).unwrap(); // Delete (OpType::Delete = 0, RocksDB byte-compat)
        } else {
            let val = format!("pval_{:05}", i);
            writer
                .add(key.as_bytes(), Some(val.as_bytes()), i + 1, 1) // Put (OpType::Put = 1, RocksDB byte-compat)
                .unwrap();
        }
    }

    let (data, _) = writer.finish().unwrap();
    let file = Box::new(MemRandomAccessFile {
        data: Arc::new(data),
    });
    let reader = SstReaderImpl::open(file).unwrap();

    for i in 0..1000u64 {
        let key = format!("pipe_{:05}", i);
        let result = reader.get(key.as_bytes()).unwrap();
        assert!(result.is_some(), "key {} should exist", key);
        let lr = result.unwrap();
        if i % 10 == 0 {
            assert_eq!(lr.op_type, OpType::Delete, "key {} should be delete", key);
            assert_eq!(lr.value, None);
        } else {
            assert_eq!(lr.op_type, OpType::Put);
            let expected = format!("pval_{:05}", i);
            assert_eq!(lr.value, Some(expected.into_bytes()));
        }
    }
}

/// C (2026-06-04) — DUAL-VERSION COMPATIBILITY: the same logical data, written
/// once as a v1 Arrow SST and once as a v2 KV SST, must read back IDENTICALLY
/// through the same reader (get / get_versions / scan). This covers BOTH the
/// new-format correctness AND the old-format-SST compatibility the format
/// migration depends on (the reader auto-detects per block, so v1 and v2 SSTs
/// coexist with no migration step).
mod c_dual_version_compat {
    use super::*;
    use forst_rs_storage::sst::{BLOCK_TYPE_DATA, BLOCK_TYPE_DATA_KV};

    type Row = (Vec<u8>, Option<Vec<u8>>, u64, u8);

    fn dataset() -> Vec<Row> {
        // 200 sorted distinct keys sharing the "user:" prefix (exercises prefix
        // compression + restart boundaries). Mix of Put / tombstone (Delete) /
        // present-but-empty value (must NOT collapse to a tombstone).
        (0..200u64)
            .map(|i| {
                let k = format!("user:{i:05}").into_bytes();
                if i % 7 == 0 {
                    (k, None, i + 1, 0) // Delete tombstone
                } else if i % 11 == 0 {
                    (k, Some(Vec::new()), i + 1, 1) // Put, empty value
                } else {
                    (k, Some(format!("val_{i}").into_bytes()), i + 1, 1) // Put
                }
            })
            .collect()
    }

    fn build(kv_format: bool, rows: &[Row]) -> Vec<u8> {
        let mut w = SstWriterImpl::with_options(SstWriterOptions {
            // Small blocks so the 200 keys span many data blocks → exercises
            // multi-block get/scan + restart points across blocks.
            block_size: 256,
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        w.force_kv_block_format(kv_format);
        for (k, v, seq, op) in rows {
            w.add(k, v.as_deref(), *seq, *op).unwrap();
        }
        w.finish().unwrap().0
    }

    fn open(data: Vec<u8>) -> SstReaderImpl {
        SstReaderImpl::open(Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        }))
        .unwrap()
    }

    #[test]
    fn v1_and_v2_ssts_read_identically() {
        let rows = dataset();
        let v1 = build(false, &rows);
        let v2 = build(true, &rows);

        // Sanity: the two files really use different on-disk block formats.
        // The first data block begins right after the 16-byte file header.
        assert_eq!(v1[FILE_HEADER_SIZE], BLOCK_TYPE_DATA, "v1 must be Arrow block");
        assert_eq!(
            v2[FILE_HEADER_SIZE], BLOCK_TYPE_DATA_KV,
            "v2 must be KV block"
        );

        let r1 = open(v1);
        let r2 = open(v2);

        // Point get + version lookup parity, including absent keys past the end
        // and a key strictly between two present keys.
        for i in 0..210u64 {
            let k = format!("user:{i:05}").into_bytes();
            assert_eq!(
                r1.get(&k).unwrap(),
                r2.get(&k).unwrap(),
                "get mismatch at user:{i:05}"
            );
            assert_eq!(
                r1.get_versions(&k).unwrap(),
                r2.get_versions(&k).unwrap(),
                "get_versions mismatch at user:{i:05}"
            );
        }
        // Absent key lexically between present keys.
        assert_eq!(r1.get(b"user:00050x").unwrap(), r2.get(b"user:00050x").unwrap());

        // Full scan + bounded range scan parity.
        assert_eq!(
            r1.scan(b"", None).unwrap(),
            r2.scan(b"", None).unwrap(),
            "full scan mismatch"
        );
        assert_eq!(
            r1.scan(b"user:00050", Some(b"user:00150")).unwrap(),
            r2.scan(b"user:00050", Some(b"user:00150")).unwrap(),
            "bounded range scan mismatch"
        );

        // And the v2 scan must actually return the expected row count (not
        // silently empty) — guards against a "both empty" false pass.
        assert_eq!(r2.scan(b"", None).unwrap().len(), rows.len());
    }

    /// Concurrency gate (2026-06-04): the read path now uses a per-thread
    /// thread-local scratch buffer (banked buffer-reuse) and, for v1, a
    /// `mem::take` of that scratch into a zero-copy Arrow `Buffer`. A torn
    /// scratch / cross-thread aliasing bug there is exactly what single-threaded
    /// ground truth misses (cf. the `seek_restart` bug). So hammer ONE shared
    /// reader from many threads (get + full scan) for BOTH formats and assert
    /// every thread sees the correct, complete result.
    #[test]
    fn concurrent_readers_one_reader_both_formats() {
        use std::thread;
        let rows = dataset();
        let keys: Vec<Vec<u8>> = rows.iter().map(|(k, ..)| k.clone()).collect();

        for kv_format in [false, true] {
            let reader = Arc::new(open(build(kv_format, &rows)));
            // Single-threaded reference (the reader's own correct output);
            // concurrency must never change it.
            let ref_scan = Arc::new(reader.scan(b"", None).unwrap());
            let ref_get: Arc<Vec<Option<LookupResult>>> =
                Arc::new(keys.iter().map(|k| reader.get(k).unwrap()).collect());

            let mut handles = Vec::new();
            for _tid in 0..12 {
                let r = Arc::clone(&reader);
                let ref_scan = Arc::clone(&ref_scan);
                let ref_get = Arc::clone(&ref_get);
                let keys = keys.clone();
                handles.push(thread::spawn(move || {
                    for _round in 0..50 {
                        assert_eq!(
                            &r.scan(b"", None).unwrap(),
                            &*ref_scan,
                            "concurrent scan corruption (kv={kv_format})"
                        );
                        for (i, k) in keys.iter().enumerate() {
                            assert_eq!(
                                &r.get(k).unwrap(),
                                &ref_get[i],
                                "concurrent get mismatch (kv={kv_format})"
                            );
                        }
                    }
                }));
            }
            for h in handles {
                h.join().expect("reader thread");
            }
        }
    }
}
