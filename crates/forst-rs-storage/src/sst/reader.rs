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

//! SST file reader for point lookups.
//!
//! [`SstReaderImpl`] opens a completed SST file (produced by [`SstWriterImpl`])
//! via a [`RandomAccessFile`] handle and serves point lookups through the
//! following algorithm:
//!
//! 1. Key-range check against footer min/max keys
//! 2. Bloom filter (SBBF) negative lookup
//! 3. Sparse index binary search to locate candidate DataBlock
//! 4. Read and decode the DataBlock (Arrow RecordBatch)
//! 5. Binary search within the RecordBatch for the target key
//! 6. Return the latest version (highest sequence) with op_type awareness

use arrow::array::{Array, BinaryArray, RecordBatch, UInt64Array, UInt8Array};
use forst_rs_common::{get_fixed32, ForstError, ForstResult, OpType};
use forst_rs_io::filesystem::RandomAccessFile;

use super::bloom_filter::Sbbf;
use super::data_block::decode_data_block;
use super::footer::{FooterV1, FOOTER_TAIL_SIZE};
use super::schema::{FILE_HEADER_SIZE, SST_MAGIC};
use super::sparse_index::{decode_index, search_index, BlockStats, SparseIndexEntry};

/// A single row produced by [`SstReaderImpl::scan`]:
/// `(key, value, sequence, op_type)`.
pub type SstScanRow = (Vec<u8>, Option<Vec<u8>>, u64, OpType);

/// Result of a point lookup: the value bytes and operation type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupResult {
    /// The value bytes. `None` if the operation is a Delete/SingleDelete.
    pub value: Option<Vec<u8>>,
    /// The sequence number of this entry.
    pub sequence: u64,
    /// The operation type.
    pub op_type: OpType,
}

/// Binary-searches a RecordBatch's key column for `target_key`.
///
/// Returns the index of the first row where key == target_key,
/// or `None` if the key is not present. The RecordBatch keys must
/// be sorted in ascending order.
fn search_key_in_batch(batch: &RecordBatch, target_key: &[u8]) -> Option<usize> {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("column 0 must be BinaryArray");

    let num_rows = keys.len();
    if num_rows == 0 {
        return None;
    }

    // Binary search for the first row where key >= target_key.
    let mut lo = 0usize;
    let mut hi = num_rows;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if keys.value(mid) < target_key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    if lo < num_rows && keys.value(lo) == target_key {
        Some(lo)
    } else {
        None
    }
}

/// An SST file reader that serves point lookups.
///
/// Created via [`SstReaderImpl::open`], which reads the footer, bloom filter,
/// and sparse index into memory. DataBlocks are read on-demand during `get()`.
pub struct SstReaderImpl {
    file: Box<dyn RandomAccessFile>,
    footer: FooterV1,
    index_entries: Vec<SparseIndexEntry>,
    #[allow(dead_code)] // Stored for future range-scan and compaction support.
    index_stats: Vec<BlockStats>,
    bloom_filter: Sbbf,
}

impl SstReaderImpl {
    /// Opens an SST file for reading.
    ///
    /// Reads the footer (from the file tail), then loads the bloom filter
    /// and sparse index sections into memory.
    pub fn open(file: Box<dyn RandomAccessFile>) -> ForstResult<Self> {
        let file_size = file.file_size()?;
        if file_size < (FILE_HEADER_SIZE + FOOTER_TAIL_SIZE) as u64 {
            return Err(ForstError::corruption(format!(
                "SST file too small: {} bytes",
                file_size
            )));
        }

        // Step 1: Read the last 8 bytes to get footer_length + magic.
        let mut tail = [0u8; 8];
        file.read_at(file_size - 8, &mut tail)?;
        if &tail[4..8] != SST_MAGIC {
            return Err(ForstError::corruption("SST file missing trailing magic"));
        }
        let (footer_length, _) = get_fixed32(&tail[0..4])?;

        // Step 2: Read and decode footer.
        let footer_start = file_size - footer_length as u64;
        let mut footer_buf = vec![0u8; footer_length as usize];
        file.read_at(footer_start, &mut footer_buf)?;
        let footer = FooterV1::decode(&footer_buf)?;

        // Step 3: Read and decode bloom filter.
        let mut bloom_buf = vec![0u8; footer.bloom_filter_size as usize];
        file.read_at(footer.bloom_filter_offset, &mut bloom_buf)?;
        let bloom_filter = Sbbf::decode(&bloom_buf)?;

        // Step 4: Read and decode sparse index.
        let mut index_buf = vec![0u8; footer.index_size as usize];
        file.read_at(footer.index_offset, &mut index_buf)?;
        let (index_entries, index_stats) = decode_index(&index_buf)?;

        Ok(Self {
            file,
            footer,
            index_entries,
            index_stats,
            bloom_filter,
        })
    }

    /// Returns a reference to the parsed footer.
    pub fn footer(&self) -> &FooterV1 {
        &self.footer
    }

    /// Returns a reference to the bloom filter.
    pub fn bloom_filter(&self) -> &Sbbf {
        &self.bloom_filter
    }

    /// Returns the number of data blocks in this SST file.
    pub fn data_block_count(&self) -> usize {
        self.index_entries.len()
    }

    /// Returns the total number of entries as recorded in the footer.
    pub fn total_entries(&self) -> u64 {
        self.footer.total_entries
    }

    /// Reads and decodes a DataBlock at the given offset and size.
    fn read_data_block(&self, block_offset: u64, block_size: u32) -> ForstResult<RecordBatch> {
        let mut buf = vec![0u8; block_size as usize];
        self.file.read_at(block_offset, &mut buf)?;
        decode_data_block(&buf)
    }

    /// Performs a point lookup for `key`.
    ///
    /// Returns the [`LookupResult`] for the latest version of the key
    /// (highest sequence number), or `None` if the key is not in this SST file.
    ///
    /// The lookup algorithm:
    /// 1. Key-range check: if key < min_key or key > max_key, return None
    /// 2. Bloom filter: if SBBF says "definitely not present", return None
    /// 3. Sparse index: binary search to find candidate DataBlock
    /// 4. Read and decode the DataBlock
    /// 5. Binary search within the RecordBatch for the key
    /// 6. Among matching rows, pick the one with the highest sequence number
    /// 7. If op_type is Delete/SingleDelete, return LookupResult with value=None
    pub fn get(&self, key: &[u8]) -> ForstResult<Option<LookupResult>> {
        // 1. Key-range check.
        if key < self.footer.min_key.as_slice() || key > self.footer.max_key.as_slice() {
            return Ok(None);
        }

        // 2. Bloom filter check.
        if !self.bloom_filter.check(key) {
            return Ok(None);
        }

        // 3. Sparse index binary search.
        let block_idx = match search_index(&self.index_entries, key) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        // 4. Read and decode the DataBlock.
        let entry = &self.index_entries[block_idx];
        let batch = self.read_data_block(entry.block_offset, entry.block_size)?;

        // 5. Binary search within the RecordBatch.
        let first_row = match search_key_in_batch(&batch, key) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        // 6. Find the row with the highest sequence number among matching keys.
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("column 0 must be BinaryArray");
        let sequences = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("column 2 must be UInt64Array");
        let op_types = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("column 3 must be UInt8Array");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("column 1 must be BinaryArray");

        let mut best_row = first_row;
        let mut best_seq = sequences.value(first_row);

        let mut row = first_row + 1;
        while row < batch.num_rows() && keys.value(row) == key {
            let seq = sequences.value(row);
            if seq > best_seq {
                best_seq = seq;
                best_row = row;
            }
            row += 1;
        }

        // 7. Build result based on op_type.
        let op_byte = op_types.value(best_row);
        let op = OpType::from_u8(op_byte).ok_or_else(|| {
            ForstError::corruption(format!("invalid op_type byte: {}", op_byte))
        })?;

        let value = match op {
            OpType::Delete | OpType::SingleDelete => None,
            OpType::Put | OpType::Merge => {
                if values.is_null(best_row) {
                    None
                } else {
                    Some(values.value(best_row).to_vec())
                }
            }
        };

        Ok(Some(LookupResult {
            value,
            sequence: best_seq,
            op_type: op,
        }))
    }

    /// Scans the SST file for all entries in the `[lower, upper)` key range,
    /// returning them as `(key, value, sequence, op_type)` tuples.
    ///
    /// Entries are ordered by `(key ASC, sequence DESC)` (the on-disk order
    /// produced by [`SstWriterImpl`]). Unlike [`SstReaderImpl::get`], this
    /// returns ALL versions of each key; callers resolve visibility and
    /// merges.
    pub fn scan(
        &self,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> ForstResult<Vec<SstScanRow>> {
        // Short-circuit if the scan range doesn't intersect [min_key, max_key].
        if let Some(hi) = upper {
            if hi <= self.footer.min_key.as_slice() {
                return Ok(Vec::new());
            }
        }
        if lower > self.footer.max_key.as_slice() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for (entry, stats) in self.index_entries.iter().zip(self.index_stats.iter()) {
            // Skip blocks whose key ranges lie entirely outside [lower, upper).
            if entry.last_key.as_slice() < lower {
                continue;
            }
            if let Some(hi) = upper {
                if stats.min_key.as_slice() >= hi {
                    break;
                }
            }

            let batch = self.read_data_block(entry.block_offset, entry.block_size)?;
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("column 0 must be BinaryArray");
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("column 1 must be BinaryArray");
            let sequences = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("column 2 must be UInt64Array");
            let op_types = batch
                .column(3)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("column 3 must be UInt8Array");

            for row in 0..batch.num_rows() {
                let key = keys.value(row);
                if key < lower {
                    continue;
                }
                if let Some(hi) = upper {
                    if key >= hi {
                        break;
                    }
                }
                let value = if values.is_null(row) {
                    None
                } else {
                    Some(values.value(row).to_vec())
                };
                let op = OpType::from_u8(op_types.value(row)).ok_or_else(|| {
                    ForstError::corruption(format!(
                        "invalid op_type in SST scan: {}",
                        op_types.value(row)
                    ))
                })?;
                out.push((key.to_vec(), value, sequences.value(row), op));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::schema::sst_schema;
    use crate::sst::writer::{SstWriterImpl, SstWriterOptions};
    use arrow::array::{BinaryArray, UInt64Array, UInt8Array};
    use forst_rs_common::CompressionType;
    use std::sync::Arc;

    /// In-memory RandomAccessFile backed by a Vec<u8>.
    struct MemRandomAccessFile {
        data: Arc<Vec<u8>>,
    }

    impl RandomAccessFile for MemRandomAccessFile {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
            let start = offset as usize;
            if start >= self.data.len() {
                return Ok(0);
            }
            let end = std::cmp::min(start + buf.len(), self.data.len());
            let n = end - start;
            buf[..n].copy_from_slice(&self.data[start..end]);
            Ok(n)
        }

        fn file_size(&self) -> ForstResult<u64> {
            Ok(self.data.len() as u64)
        }
    }

    fn write_test_sst(n: usize) -> Arc<Vec<u8>> {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 4096,
            compression: CompressionType::None,
        });
        for i in 0..n {
            let key = format!("key_{:05}", i);
            let val = format!("val_{:05}", i);
            writer
                .add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 0)
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();
        Arc::new(data)
    }

    #[test]
    fn test_open_reads_footer_correctly() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile {
            data: sst_data.clone(),
        });
        let reader = SstReaderImpl::open(file).unwrap();
        assert_eq!(reader.footer().total_entries, 100);
        assert_eq!(reader.footer().min_key, b"key_00000");
        assert_eq!(reader.footer().max_key, b"key_00099");
    }

    #[test]
    fn test_open_loads_bloom_filter() {
        let sst_data = write_test_sst(50);
        let file = Box::new(MemRandomAccessFile {
            data: sst_data.clone(),
        });
        let reader = SstReaderImpl::open(file).unwrap();
        // All inserted keys must pass bloom filter
        for i in 0..50 {
            let key = format!("key_{:05}", i);
            assert!(reader.bloom_filter().check(key.as_bytes()));
        }
    }

    #[test]
    fn test_open_loads_index() {
        let sst_data = write_test_sst(200);
        let file = Box::new(MemRandomAccessFile {
            data: sst_data.clone(),
        });
        let reader = SstReaderImpl::open(file).unwrap();
        assert!(reader.data_block_count() >= 1);
        assert_eq!(reader.total_entries(), 200);
    }

    #[test]
    fn test_open_rejects_too_small_file() {
        let tiny = Arc::new(vec![0u8; 10]);
        let file = Box::new(MemRandomAccessFile { data: tiny });
        let result = SstReaderImpl::open(file);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_rejects_bad_magic() {
        let bad = vec![0u8; 100];
        // No valid magic at tail
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(bad),
        });
        let result = SstReaderImpl::open(file);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // search_key_in_batch tests
    // -----------------------------------------------------------------------

    fn make_batch(keys: &[&[u8]], vals: &[&[u8]], seqs: &[u64], ops: &[u8]) -> RecordBatch {
        let schema = Arc::new(sst_schema());
        let key_array = BinaryArray::from_iter_values(keys.iter().copied());
        let val_array = BinaryArray::from_iter_values(vals.iter().copied());
        let seq_array = UInt64Array::from(seqs.to_vec());
        let op_array = UInt8Array::from(ops.to_vec());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(key_array),
                Arc::new(val_array),
                Arc::new(seq_array),
                Arc::new(op_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_search_key_in_batch_found() {
        let batch = make_batch(
            &[b"aaa", b"bbb", b"ccc", b"ddd"],
            &[b"v1", b"v2", b"v3", b"v4"],
            &[1, 2, 3, 4],
            &[0, 0, 0, 0],
        );
        assert_eq!(search_key_in_batch(&batch, b"bbb"), Some(1));
        assert_eq!(search_key_in_batch(&batch, b"aaa"), Some(0));
        assert_eq!(search_key_in_batch(&batch, b"ddd"), Some(3));
    }

    #[test]
    fn test_search_key_in_batch_not_found() {
        let batch = make_batch(
            &[b"aaa", b"ccc", b"eee"],
            &[b"v1", b"v2", b"v3"],
            &[1, 2, 3],
            &[0, 0, 0],
        );
        assert_eq!(search_key_in_batch(&batch, b"bbb"), None);
        assert_eq!(search_key_in_batch(&batch, b"zzz"), None);
    }

    #[test]
    fn test_search_key_in_batch_empty() {
        let schema = Arc::new(sst_schema());
        let batch = RecordBatch::new_empty(schema);
        assert_eq!(search_key_in_batch(&batch, b"any"), None);
    }

    #[test]
    fn test_search_key_in_batch_duplicate_keys_finds_first() {
        // Same key with different sequences — should find the first occurrence
        let batch = make_batch(
            &[b"key", b"key", b"key"],
            &[b"v1", b"v2", b"v3"],
            &[100, 50, 10],
            &[0, 0, 0],
        );
        assert_eq!(search_key_in_batch(&batch, b"key"), Some(0));
    }

    // -----------------------------------------------------------------------
    // get() tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_existing_key() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();

        let result = reader.get(b"key_00050").unwrap();
        assert!(result.is_some());
        let lr = result.unwrap();
        assert_eq!(lr.value, Some(b"val_00050".to_vec()));
        assert_eq!(lr.sequence, 51);
        assert_eq!(lr.op_type, OpType::Put);
    }

    #[test]
    fn test_get_first_key() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();
        let result = reader.get(b"key_00000").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Some(b"val_00000".to_vec()));
    }

    #[test]
    fn test_get_last_key() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();
        let result = reader.get(b"key_00099").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Some(b"val_00099".to_vec()));
    }

    #[test]
    fn test_get_missing_key_out_of_range() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();
        // Before min key
        assert!(reader.get(b"aaa").unwrap().is_none());
        // After max key
        assert!(reader.get(b"zzz").unwrap().is_none());
    }

    #[test]
    fn test_get_missing_key_in_range() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();
        // Key in range but not present
        assert!(reader.get(b"key_00100").unwrap().is_none());
    }

    #[test]
    fn test_get_delete_tombstone() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 4096,
            compression: CompressionType::None,
        });
        writer.add(b"aaa", Some(b"val"), 1, 0).unwrap();
        writer.add(b"bbb", None, 2, 1).unwrap(); // Delete
        writer.add(b"ccc", Some(b"val3"), 3, 0).unwrap();
        let (data, _) = writer.finish().unwrap();

        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();

        let result = reader.get(b"bbb").unwrap().unwrap();
        assert_eq!(result.op_type, OpType::Delete);
        assert_eq!(result.value, None);
    }

    #[test]
    fn test_get_all_keys_100_percent_hit() {
        let n = 500;
        let sst_data = write_test_sst(n);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();

        for i in 0..n {
            let key = format!("key_{:05}", i);
            let result = reader.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "key {} should be found", key);
            let lr = result.unwrap();
            let expected_val = format!("val_{:05}", i);
            assert_eq!(lr.value, Some(expected_val.into_bytes()));
        }
    }

    #[test]
    fn test_get_multi_block_small_block_size() {
        // Force many small blocks
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 128,
            compression: CompressionType::None,
        });
        for i in 0..200u64 {
            let key = format!("mb_{:05}", i);
            let val = format!("v_{:05}", i);
            writer
                .add(key.as_bytes(), Some(val.as_bytes()), i + 1, 0)
                .unwrap();
        }
        let (data, info) = writer.finish().unwrap();
        assert!(info.data_block_count > 1, "should have multiple blocks");

        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();

        // Verify all keys found
        for i in 0..200u64 {
            let key = format!("mb_{:05}", i);
            let expected_val = format!("v_{:05}", i);
            let result = reader.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "key {} not found", key);
            assert_eq!(result.unwrap().value, Some(expected_val.into_bytes()));
        }
    }

    #[test]
    fn test_get_with_lz4_compression() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 4096,
            compression: CompressionType::Lz4,
        });
        for i in 0..100u64 {
            let key = format!("lz4_{:05}", i);
            writer
                .add(key.as_bytes(), Some(b"value"), i + 1, 0)
                .unwrap();
        }
        let (data, _) = writer.finish().unwrap();
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();

        for i in 0..100u64 {
            let key = format!("lz4_{:05}", i);
            let result = reader.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "LZ4 key {} not found", key);
        }
    }

    #[test]
    fn test_get_with_zstd_compression() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 4096,
            compression: CompressionType::Zstd,
        });
        for i in 0..100u64 {
            let key = format!("zstd_{:05}", i);
            writer
                .add(key.as_bytes(), Some(b"value"), i + 1, 0)
                .unwrap();
        }
        let (data, _) = writer.finish().unwrap();
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();

        for i in 0..100u64 {
            let key = format!("zstd_{:05}", i);
            let result = reader.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Zstd key {} not found", key);
        }
    }
}
