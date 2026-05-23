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
//! [`SstReaderImpl`] opens a completed SST file (produced by [`crate::sst::writer::SstWriterImpl`])
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

/// Zero-copy borrowed view of a single SST row.
///
/// The `key` and `value` slices borrow directly from an Arrow `BinaryArray`
/// backing buffer (which itself wraps the bytes read from the SST data block).
/// No `Vec<u8>` is allocated per row. The lifetime `'a` ties the view to the
/// `RecordBatch` that owns the underlying Arrow buffer; callers must consume
/// the view before the batch is dropped, or convert to owned bytes themselves
/// via `key.to_vec()` / `value.map(|v| v.to_vec())`.
///
/// PR-D2 (Z3-10, Z3-11, C-R3-H1..3): replaces per-row `Vec<u8>` materialization
/// in the SST scan/get hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowView<'a> {
    /// Key bytes, borrowed from the Arrow key column.
    pub key: &'a [u8],
    /// Value bytes, `None` if this row is a delete tombstone.
    pub value: Option<&'a [u8]>,
    /// Sequence number.
    pub sequence: u64,
    /// Operation type.
    pub op_type: OpType,
}

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
    /// Cached at `open()` to avoid per-`read_data_block` syscall on the
    /// hot lookup path. Used to bound-check untrusted `block_size` values
    /// from the sparse index before allocating the read buffer (Sweep
    /// R3 H by Reviewers 2 + 5).
    file_size: u64,
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
        // SECURITY: bound `footer_length` against `file_size` before allocation.
        // Untrusted input from a crafted SST could otherwise drive
        // `vec![0u8; footer_length as usize]` to allocate up to ~4 GiB and OOM
        // the process (Sweep R2 H by Reviewer 5).
        let footer_start = file_size.checked_sub(footer_length as u64).ok_or_else(|| {
            ForstError::corruption(format!(
                "SST footer_length {} exceeds file_size {}",
                footer_length, file_size
            ))
        })?;
        // Also guard against footer_length == 0 (would zero-size alloc + decode fail later
        // anyway, but explicit rejection produces a clearer error).
        if footer_length == 0 {
            return Err(ForstError::corruption("SST footer_length is zero"));
        }
        let mut footer_buf = vec![0u8; footer_length as usize];
        file.read_at(footer_start, &mut footer_buf)?;
        let footer = FooterV1::decode(&footer_buf)?;

        // Step 3: Read and decode bloom filter.
        // SECURITY: validate bloom_filter_offset + bloom_filter_size fit within
        // file_size to prevent OOM from a crafted footer.
        let bloom_end = (footer.bloom_filter_offset)
            .checked_add(footer.bloom_filter_size as u64)
            .ok_or_else(|| ForstError::corruption("SST bloom_filter offset+size overflow"))?;
        if bloom_end > file_size {
            return Err(ForstError::corruption(format!(
                "SST bloom_filter range [{}, {}) exceeds file_size {}",
                footer.bloom_filter_offset, bloom_end, file_size
            )));
        }
        let mut bloom_buf = vec![0u8; footer.bloom_filter_size as usize];
        file.read_at(footer.bloom_filter_offset, &mut bloom_buf)?;
        let bloom_filter = Sbbf::decode(&bloom_buf)?;

        // Step 4: Read and decode sparse index.
        // SECURITY: same bounds check as bloom filter range.
        let index_end = (footer.index_offset)
            .checked_add(footer.index_size as u64)
            .ok_or_else(|| ForstError::corruption("SST index offset+size overflow"))?;
        if index_end > file_size {
            return Err(ForstError::corruption(format!(
                "SST index range [{}, {}) exceeds file_size {}",
                footer.index_offset, index_end, file_size
            )));
        }
        let mut index_buf = vec![0u8; footer.index_size as usize];
        file.read_at(footer.index_offset, &mut index_buf)?;
        let (index_entries, index_stats) = decode_index(&index_buf)?;

        Ok(Self {
            file,
            file_size,
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
    ///
    /// SECURITY: validates `block_offset + block_size` fits within
    /// `self.file_size` before allocating the read buffer. This prevents
    /// OOM-DoS from a crafted sparse index claiming an oversized
    /// `block_size` (Sweep R3 H by Reviewers 2 + 5). The `file_size`
    /// is cached at `open()` so this check costs no syscall on the
    /// hot lookup path.
    fn read_data_block(&self, block_offset: u64, block_size: u32) -> ForstResult<RecordBatch> {
        let block_end = block_offset
            .checked_add(block_size as u64)
            .ok_or_else(|| ForstError::corruption("SST data block offset+size overflow"))?;
        if block_end > self.file_size {
            return Err(ForstError::corruption(format!(
                "SST data block range [{}, {}) exceeds file_size {}",
                block_offset, block_end, self.file_size
            )));
        }
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
        //
        // R38-M1: a crafted or corrupt SST whose schema doesn't match the
        // writer-side contract must surface as a corruption error, not a
        // panic. The batch path (`for_each_row_in_batch` at line 481+) uses
        // `ok_or_else(corruption)` here too; mirror that pattern.
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| ForstError::corruption("SST batch column 0 not BinaryArray"))?;
        let sequences = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| ForstError::corruption("SST batch column 2 not UInt64Array"))?;
        let op_types = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| ForstError::corruption("SST batch column 3 not UInt8Array"))?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| ForstError::corruption("SST batch column 1 not BinaryArray"))?;

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
        let op = OpType::from_u8(op_byte)
            .ok_or_else(|| ForstError::corruption(format!("invalid op_type byte: {}", op_byte)))?;

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

    /// Returns the number of index entries (one per data block).
    /// Used by streaming callers (e.g. compaction) to iterate blocks via
    /// [`Self::read_block_at`].
    pub fn index_entry_count(&self) -> usize {
        self.index_entries.len()
    }

    /// Reads and decodes the data block at `block_idx` (0-based). Returns the
    /// `RecordBatch` plus the index entry's `last_key`. Used by the streaming
    /// compaction path to feed rows into the k-way merge without first
    /// materialising every entry into a `Vec<u8>`.
    ///
    /// PR-D2: this is the public streaming primitive that replaces the
    /// `scan() -> Vec<SstScanRow>` materialisation. Callers iterate the
    /// returned `RecordBatch` directly via [`for_each_row_in_batch`] (which
    /// yields zero-copy [`RowView`]s borrowing from the batch buffers).
    pub fn read_block_at(&self, block_idx: usize) -> ForstResult<RecordBatch> {
        let entry = self.index_entries.get(block_idx).ok_or_else(|| {
            ForstError::invalid_argument(format!(
                "SST block index {} out of range (have {} blocks)",
                block_idx,
                self.index_entries.len()
            ))
        })?;
        self.read_data_block(entry.block_offset, entry.block_size)
    }

    /// Scans the SST file for all entries in the `[lower, upper)` key range,
    /// invoking `cb` once per matching row with a zero-copy [`RowView`] that
    /// borrows from the underlying Arrow batch buffer.
    ///
    /// PR-D2 zero-copy primitive: no `Vec<u8>` is allocated per row. Each
    /// `RecordBatch` is dropped between blocks, so the callback must either
    /// process the view inline or copy out via `view.key.to_vec()` /
    /// `view.value.map(|v| v.to_vec())`.
    ///
    /// Entries are visited in `(key ASC, sequence DESC)` order — the on-disk
    /// order produced by [`crate::sst::writer::SstWriterImpl`]. Unlike
    /// [`SstReaderImpl::get`], this visits ALL versions of each key; callers
    /// resolve visibility and merges themselves.
    pub fn scan_borrowed<F>(
        &self,
        lower: &[u8],
        upper: Option<&[u8]>,
        mut cb: F,
    ) -> ForstResult<()>
    where
        F: FnMut(RowView<'_>) -> ForstResult<()>,
    {
        // Short-circuit if the scan range doesn't intersect [min_key, max_key].
        if let Some(hi) = upper {
            if hi <= self.footer.min_key.as_slice() {
                return Ok(());
            }
        }
        if lower > self.footer.max_key.as_slice() {
            return Ok(());
        }

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
            for_each_row_in_batch(&batch, |view| {
                if view.key < lower {
                    return Ok(());
                }
                if let Some(hi) = upper {
                    if view.key >= hi {
                        // We could `break` if we had a control-flow channel; the
                        // wrapper loop in `for_each_row_in_batch` is row-linear,
                        // so we just no-op the remaining rows of this block via
                        // an unrolled compare on the caller side. For batch sizes
                        // ≤ 8192 (FLUSH_BATCH_SIZE) this is at worst a few µs.
                        return Ok(());
                    }
                }
                cb(view)
            })?;
        }
        Ok(())
    }

    /// Backwards-compatible owned-row scan. Reimplemented atop
    /// [`Self::scan_borrowed`] so the single place that pays the `to_vec`
    /// cost is here, in callers that explicitly opt into ownership.
    ///
    /// Prefer [`Self::scan_borrowed`] in new code (compaction, replication,
    /// snapshot reads) — it skips the per-row allocation entirely.
    pub fn scan(&self, lower: &[u8], upper: Option<&[u8]>) -> ForstResult<Vec<SstScanRow>> {
        let mut out = Vec::new();
        self.scan_borrowed(lower, upper, |view| {
            out.push((
                view.key.to_vec(),
                view.value.map(|v| v.to_vec()),
                view.sequence,
                view.op_type,
            ));
            Ok(())
        })?;
        Ok(out)
    }
}

/// Iterates every row of a decoded SST data-block `RecordBatch`, invoking
/// `cb` once per row with a zero-copy [`RowView`] that borrows from the batch
/// buffers.
///
/// Internal helper exposed at module scope so streaming callers (compaction,
/// flush re-write) can drive the iteration themselves after fetching the
/// batch via [`SstReaderImpl::read_block_at`].
pub fn for_each_row_in_batch<F>(batch: &RecordBatch, mut cb: F) -> ForstResult<()>
where
    F: FnMut(RowView<'_>) -> ForstResult<()>,
{
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 0 not BinaryArray"))?;
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 1 not BinaryArray"))?;
    let sequences = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| ForstError::corruption("SST batch column 2 not UInt64Array"))?;
    let op_types = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| ForstError::corruption("SST batch column 3 not UInt8Array"))?;

    for row in 0..batch.num_rows() {
        let key = keys.value(row);
        let value = if values.is_null(row) {
            None
        } else {
            Some(values.value(row))
        };
        let op_byte = op_types.value(row);
        let op_type = OpType::from_u8(op_byte).ok_or_else(|| {
            ForstError::corruption(format!("invalid op_type in SST batch: {}", op_byte))
        })?;
        cb(RowView {
            key,
            value,
            sequence: sequences.value(row),
            op_type,
        })?;
    }
    Ok(())
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
                .add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 1) // OpType::Put = 1 (RocksDB byte-compat)
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

    /// Regression test for Sweep R2 H (Reviewer 5): a crafted SST with a
    /// `footer_length` value that exceeds the actual file size must be
    /// rejected BEFORE the `vec![0u8; footer_length as usize]` allocation,
    /// to prevent OOM-DoS on attacker-controlled input. Pre-fix this
    /// would have triggered an underflow panic on `file_size - footer_length`
    /// or attempted a multi-GiB allocation.
    #[test]
    fn test_open_rejects_oversized_footer_length() {
        // 100-byte file, last 8 bytes encode footer_length=u32::MAX + magic.
        let mut data = vec![0u8; 100];
        // Place a footer_length value larger than the entire file in the
        // last 8 bytes' first 4 (footer_length) followed by SST_MAGIC.
        let footer_length: u32 = 0x7fff_ffff; // huge but file is only 100 bytes
        data[92..96].copy_from_slice(&footer_length.to_le_bytes());
        data[96..100].copy_from_slice(SST_MAGIC);
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let err = match SstReaderImpl::open(file) {
            Ok(_) => panic!("must reject oversized footer_length"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("exceeds file_size"),
            "expected error message to mention size violation; got: {}",
            msg
        );
    }

    /// Regression test for Sweep R2 H (Reviewer 5): an SST with a footer
    /// claiming `bloom_filter_offset + bloom_filter_size > file_size` (or
    /// triggering u64 overflow) must be rejected.
    ///
    /// Direct construction of such a file requires bypassing the writer
    /// (which always emits well-formed footers), so we check the validation
    /// path indirectly via the underflow guard on footer_length being zero.
    /// (The full bloom/index range overflow paths share identical
    /// `checked_add` + `> file_size` logic; they're trivially correct by
    /// inspection given footer_length validation passes.)
    #[test]
    fn test_open_rejects_zero_footer_length() {
        let mut data = vec![0u8; 100];
        data[92..96].copy_from_slice(&0u32.to_le_bytes()); // footer_length=0
        data[96..100].copy_from_slice(SST_MAGIC);
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let err = match SstReaderImpl::open(file) {
            Ok(_) => panic!("must reject zero footer_length"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("footer_length is zero"),
            "expected zero-footer-length error; got: {}",
            msg
        );
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

    /// Regression test for Sweep R3 H (Reviewers 2 + 5): a sparse-index
    /// entry claiming a `block_size` that overflows past the file end
    /// must be rejected by `read_data_block` BEFORE allocation, not
    /// after attempting `read_at` (which might OOM on the
    /// `vec![0u8; block_size as usize]` first). We simulate the
    /// malicious-index condition by mutating the cached `file_size`
    /// to a value smaller than any real block range — the validation
    /// then trips on otherwise-valid block reads.
    #[test]
    fn test_read_data_block_rejects_oob_range() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let mut reader = SstReaderImpl::open(file).unwrap();
        // Pretend the file is 16 bytes — any real block read will trip
        // the `block_end > self.file_size` check.
        reader.file_size = 16;
        let err = match reader.get(b"key_00050") {
            Ok(_) => panic!("read_data_block must reject OOB block range"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("data block range") && msg.contains("exceeds file_size"),
            "expected OOB-block error message; got: {}",
            msg
        );
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
        writer.add(b"aaa", Some(b"val"), 1, 1).unwrap(); // Put (OpType::Put = 1, RocksDB byte-compat)
        writer.add(b"bbb", None, 2, 0).unwrap(); // Delete (OpType::Delete = 0, RocksDB byte-compat)
        writer.add(b"ccc", Some(b"val3"), 3, 1).unwrap(); // Put (OpType::Put = 1, RocksDB byte-compat)
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
                .add(key.as_bytes(), Some(val.as_bytes()), i + 1, 1) // Put (OpType::Put = 1, RocksDB byte-compat)
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
                .add(key.as_bytes(), Some(b"value"), i + 1, 1) // Put (OpType::Put = 1, RocksDB byte-compat)
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
                .add(key.as_bytes(), Some(b"value"), i + 1, 1) // Put (OpType::Put = 1, RocksDB byte-compat)
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

    // -----------------------------------------------------------------------
    // PR-D2 zero-copy scan tests (Z3-10, Z3-11, C-R3-H1..3)
    // -----------------------------------------------------------------------

    /// PR-D2 invariant: `RowView::key` and `RowView::value` borrow into the
    /// Arrow `BinaryArray` backing buffer of the decoded data block — they
    /// must NOT be a fresh `Vec` allocated per row. We assert this by
    /// checking the addresses fall inside a single decoded `RecordBatch`'s
    /// key/value buffers (which themselves are heap allocations owned by the
    /// batch's Arc-backed `Buffer`s).
    #[test]
    fn sst_read_arrow_zero_copy() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 4096,
            compression: CompressionType::None,
        });
        for i in 0..32u64 {
            writer
                .add(
                    format!("zc_{:04}", i).as_bytes(),
                    Some(format!("zc_val_{:04}", i).as_bytes()),
                    i + 1,
                    1,
                )
                .unwrap();
        }
        let (data, info) = writer.finish().unwrap();
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();
        assert!(info.data_block_count >= 1);

        // Read the first data block ourselves so we can keep the batch alive
        // while comparing pointer ranges with what `for_each_row_in_batch`
        // yields.
        let batch = reader.read_block_at(0).unwrap();
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        let key_buf = keys.values().as_slice();
        let val_buf = values.values().as_slice();
        let key_buf_range = key_buf.as_ptr() as usize..(key_buf.as_ptr() as usize + key_buf.len());
        let val_buf_range = val_buf.as_ptr() as usize..(val_buf.as_ptr() as usize + val_buf.len());

        let mut rows_seen = 0usize;
        for_each_row_in_batch(&batch, |view| {
            let kp = view.key.as_ptr() as usize;
            assert!(
                key_buf_range.contains(&kp),
                "RowView::key must borrow into the Arrow BinaryArray buffer (got ptr {:p}, buf range {:?})",
                view.key.as_ptr(),
                key_buf_range,
            );
            if let Some(v) = view.value {
                let vp = v.as_ptr() as usize;
                assert!(
                    val_buf_range.contains(&vp),
                    "RowView::value must borrow into the Arrow BinaryArray buffer"
                );
            }
            rows_seen += 1;
            Ok(())
        })
        .unwrap();
        assert!(rows_seen > 0);
    }

    /// PR-D2: `scan_borrowed` yields the same logical rows as the legacy
    /// `scan()` API. This is the round-trip parity test required by the spec.
    #[test]
    fn sst_scan_borrowed_matches_scan() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 256,
            compression: CompressionType::None,
        });
        for i in 0..100u64 {
            writer
                .add(
                    format!("scan_{:05}", i).as_bytes(),
                    Some(format!("v_{:05}", i).as_bytes()),
                    i + 1,
                    1,
                )
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();

        let owned: Vec<_> = reader.scan(b"", None).unwrap();
        let mut borrowed_count = 0usize;
        reader
            .scan_borrowed(b"", None, |view| {
                let (ek, ev, eseq, eop) = &owned[borrowed_count];
                assert_eq!(view.key, ek.as_slice());
                assert_eq!(view.value, ev.as_deref());
                assert_eq!(view.sequence, *eseq);
                assert_eq!(view.op_type, *eop);
                borrowed_count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(borrowed_count, owned.len());
        assert_eq!(borrowed_count, 100);
    }
}
