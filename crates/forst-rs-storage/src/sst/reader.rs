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

use forst_rs_common::{get_fixed32, ForstError, ForstResult, OpType};
use forst_rs_io::filesystem::RandomAccessFile;

use super::bloom_filter::Sbbf;
use super::footer::{FooterV1, FOOTER_TAIL_SIZE};
use super::schema::{FILE_HEADER_SIZE, SST_MAGIC};
use super::sparse_index::{decode_index, BlockStats, SparseIndexEntry};

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

/// An SST file reader that serves point lookups.
///
/// Created via [`SstReaderImpl::open`], which reads the footer, bloom filter,
/// and sparse index into memory. DataBlocks are read on-demand during `get()`.
pub struct SstReaderImpl {
    file: Box<dyn RandomAccessFile>,
    footer: FooterV1,
    index_entries: Vec<SparseIndexEntry>,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::writer::{SstWriterImpl, SstWriterOptions};
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
}
