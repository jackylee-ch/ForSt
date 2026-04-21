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

//! SST file writer.
//!
//! [`SstWriterImpl`] accumulates sorted key-value entries into DataBlocks
//! and produces a complete SST file (as in-memory bytes) conforming to the
//! ForSt-RS Arrow SST format:
//!
//! ```text
//! FileHeader (16B) | DataBlock₀ | … | DataBlockₙ | IndexSection | Footer
//! ```
//!
//! Bloom Filter is a placeholder (offset=0, size=0) until W7.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayBuilder, BinaryBuilder, RecordBatch, UInt64Builder, UInt8Builder};
use arrow::datatypes::Schema;

use forst_rs_common::{CompressionType, ForstError, ForstResult};

use super::data_block::encode_data_block;
use super::file_header::FileHeader;
use super::footer::{ChecksumType, FooterV1};
use super::schema::{sst_schema, SST_FORMAT_VERSION};
use super::sparse_index::{encode_index, BlockStats, SparseIndexEntry};

/// Information about a completed SST file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstFileInfo {
    /// Total file size in bytes.
    pub file_size: u64,
    /// Number of key-value entries across all DataBlocks.
    pub entry_count: u64,
    /// Number of DataBlocks.
    pub data_block_count: u32,
    /// Smallest key in the SST file.
    pub min_key: Vec<u8>,
    /// Largest key in the SST file.
    pub max_key: Vec<u8>,
    /// Smallest sequence number.
    pub min_sequence: u64,
    /// Largest sequence number.
    pub max_sequence: u64,
}

/// Options controlling SST file generation.
#[derive(Debug, Clone)]
pub struct SstWriterOptions {
    /// Target DataBlock size in bytes before flush. Default: 64KB.
    pub block_size: usize,
    /// Compression algorithm for DataBlocks. Default: LZ4.
    pub compression: CompressionType,
}

impl Default for SstWriterOptions {
    fn default() -> Self {
        Self {
            block_size: 64 * 1024,
            compression: CompressionType::Lz4,
        }
    }
}

/// Builder that accumulates sorted KV entries and produces an SST file.
pub struct SstWriterImpl {
    options: SstWriterOptions,
    schema: Arc<Schema>,
    buf: Vec<u8>,
    key_builder: BinaryBuilder,
    value_builder: BinaryBuilder,
    sequence_builder: UInt64Builder,
    op_type_builder: UInt8Builder,
    current_estimated_size: usize,
    index_entries: Vec<SparseIndexEntry>,
    block_stats: Vec<BlockStats>,
    total_entries: u64,
    global_min_key: Option<Vec<u8>>,
    global_max_key: Option<Vec<u8>>,
    global_min_sequence: u64,
    global_max_sequence: u64,
    finished: bool,
    /// Last added key for debug-mode sorted-order invariant check.
    last_added_key: Option<Vec<u8>>,
}

impl Default for SstWriterImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl SstWriterImpl {
    /// Creates a new writer with default options.
    pub fn new() -> Self {
        Self::with_options(SstWriterOptions::default())
    }

    /// Creates a new writer with the given options.
    pub fn with_options(options: SstWriterOptions) -> Self {
        let schema = Arc::new(sst_schema());
        let mut buf = Vec::with_capacity(options.block_size + 1024);
        buf.extend_from_slice(&FileHeader::default().encode());

        Self {
            options,
            schema,
            buf,
            key_builder: BinaryBuilder::new(),
            value_builder: BinaryBuilder::new(),
            sequence_builder: UInt64Builder::new(),
            op_type_builder: UInt8Builder::new(),
            current_estimated_size: 0,
            index_entries: Vec::new(),
            block_stats: Vec::new(),
            total_entries: 0,
            global_min_key: None,
            global_max_key: None,
            global_min_sequence: u64::MAX,
            global_max_sequence: 0,
            finished: false,
            last_added_key: None,
        }
    }

    /// Adds a single key-value entry. Entries MUST be added in sorted order
    /// (by key ascending, then sequence descending).
    pub fn add(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        sequence: u64,
        op_type: u8,
    ) -> ForstResult<()> {
        if self.finished {
            return Err(ForstError::invalid_argument(
                "cannot add entries after finish()",
            ));
        }

        debug_assert!(
            self.last_added_key
                .as_ref()
                .map_or(true, |prev| key >= prev.as_slice()),
            "entries must be added in sorted key order"
        );

        self.key_builder.append_value(key);
        match value {
            Some(v) => self.value_builder.append_value(v),
            None => self.value_builder.append_null(),
        }
        self.sequence_builder.append_value(sequence);
        self.op_type_builder.append_value(op_type);

        let entry_size = key.len() + value.map_or(0, |v| v.len());
        self.current_estimated_size += entry_size;

        // Update global stats.
        if self.global_min_key.is_none() || key < self.global_min_key.as_deref().unwrap() {
            self.global_min_key = Some(key.to_vec());
        }
        if self.global_max_key.is_none() || key > self.global_max_key.as_deref().unwrap() {
            self.global_max_key = Some(key.to_vec());
        }
        if sequence < self.global_min_sequence {
            self.global_min_sequence = sequence;
        }
        if sequence > self.global_max_sequence {
            self.global_max_sequence = sequence;
        }

        self.total_entries += 1;
        self.last_added_key = Some(key.to_vec());

        if self.current_estimated_size >= self.options.block_size {
            self.flush_block()?;
        }

        Ok(())
    }

    /// Returns the current estimated file size.
    pub fn estimated_size(&self) -> u64 {
        self.buf.len() as u64 + self.current_estimated_size as u64
    }

    /// Finishes writing the SST file. Returns the complete file bytes and file info.
    ///
    /// After calling `finish()`, no more entries can be added.
    pub fn finish(mut self) -> ForstResult<(Vec<u8>, SstFileInfo)> {
        if self.finished {
            return Err(ForstError::invalid_argument("finish() already called"));
        }
        self.finished = true;

        // Flush any remaining rows.
        if self.key_builder.len() > 0 {
            self.flush_block()?;
        }

        // If no entries were added, return an error.
        if self.total_entries == 0 {
            return Err(ForstError::invalid_argument(
                "cannot finish an SST file with zero entries",
            ));
        }

        // Write Index Section (Bloom Filter placeholder: offset=0, size=0).
        let index_offset = self.buf.len() as u64;
        let index_bytes = encode_index(&self.index_entries, &self.block_stats);
        let index_size = index_bytes.len() as u32;
        self.buf.extend_from_slice(&index_bytes);

        // Write Footer.
        let creation_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let data_block_count = self.index_entries.len() as u32;

        let info = SstFileInfo {
            file_size: 0, // placeholder, updated after footer write
            entry_count: self.total_entries,
            data_block_count,
            min_key: self.global_min_key.unwrap_or_default(),
            max_key: self.global_max_key.unwrap_or_default(),
            min_sequence: self.global_min_sequence,
            max_sequence: self.global_max_sequence,
        };

        let footer = FooterV1 {
            data_block_count,
            total_entries: self.total_entries,
            bloom_filter_offset: 0,
            bloom_filter_size: 0,
            index_offset,
            index_size,
            min_key: info.min_key.to_vec(),
            max_key: info.max_key.to_vec(),
            min_sequence: self.global_min_sequence,
            max_sequence: self.global_max_sequence,
            compression: self.options.compression,
            checksum_type: ChecksumType::Crc32c,
            creation_time,
            format_version: SST_FORMAT_VERSION,
        };
        self.buf.extend_from_slice(&footer.encode());

        let file_size = self.buf.len() as u64;
        let info = SstFileInfo { file_size, ..info };

        Ok((self.buf, info))
    }

    /// Flushes the current accumulated rows as a DataBlock.
    fn flush_block(&mut self) -> ForstResult<()> {
        let num_rows = self.key_builder.len();
        if num_rows == 0 {
            return Ok(());
        }

        let key_array = std::mem::replace(&mut self.key_builder, BinaryBuilder::new()).finish();
        let value_array = std::mem::replace(&mut self.value_builder, BinaryBuilder::new()).finish();
        let seq_array =
            std::mem::replace(&mut self.sequence_builder, UInt64Builder::new()).finish();
        let op_array = std::mem::replace(&mut self.op_type_builder, UInt8Builder::new()).finish();

        // Extract needed values before consuming arrays into Arc.
        let last_key = key_array.value(num_rows - 1).to_vec();
        let min_key = key_array.value(0).to_vec();
        let max_key = key_array.value(num_rows - 1).to_vec();
        let mut min_seq = u64::MAX;
        let mut max_seq = 0u64;
        for i in 0..seq_array.len() {
            let s = seq_array.value(i);
            if s < min_seq {
                min_seq = s;
            }
            if s > max_seq {
                max_seq = s;
            }
        }

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(key_array),
                Arc::new(value_array),
                Arc::new(seq_array),
                Arc::new(op_array),
            ],
        )
        .map_err(|e| ForstError::corruption(format!("failed to build RecordBatch: {e}")))?;

        let block_bytes = encode_data_block(&batch, self.options.compression)?;
        let block_offset = self.buf.len() as u64;
        let block_size = block_bytes.len() as u32;
        self.buf.extend_from_slice(&block_bytes);

        self.index_entries.push(SparseIndexEntry {
            last_key,
            block_offset,
            block_size,
        });

        self.block_stats.push(BlockStats {
            min_key,
            max_key,
            entry_count: num_rows as u32,
            min_sequence: min_seq,
            max_sequence: max_seq,
        });

        self.current_estimated_size = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::data_block::decode_data_block;
    use crate::sst::footer::FooterV1;
    use crate::sst::schema::{FILE_HEADER_SIZE, SST_MAGIC};
    use crate::sst::sparse_index::decode_index;
    use forst_rs_common::get_fixed32;

    #[test]
    fn test_writer_produces_valid_sst() {
        let mut writer = SstWriterImpl::new();
        writer.add(b"aaa", Some(b"v1"), 1, 0).unwrap();
        writer.add(b"bbb", Some(b"v2"), 2, 0).unwrap();
        writer.add(b"ccc", Some(b"v3"), 3, 0).unwrap();

        let (data, info) = writer.finish().unwrap();

        assert_eq!(info.entry_count, 3);
        assert_eq!(info.min_key, b"aaa");
        assert_eq!(info.max_key, b"ccc");
        assert_eq!(info.min_sequence, 1);
        assert_eq!(info.max_sequence, 3);
        assert!(info.file_size > 0);
        assert!(info.data_block_count >= 1);
        assert_eq!(&data[..4], SST_MAGIC);
        let len = data.len();
        assert_eq!(&data[len - 4..], SST_MAGIC);
    }

    #[test]
    fn test_writer_with_delete_tombstones() {
        let mut writer = SstWriterImpl::new();
        writer.add(b"key1", Some(b"val"), 1, 0).unwrap();
        writer.add(b"key2", None, 2, 1).unwrap();
        let (data, info) = writer.finish().unwrap();
        assert_eq!(info.entry_count, 2);
        assert!(data.len() > FILE_HEADER_SIZE);
    }

    #[test]
    fn test_writer_forces_flush_at_block_size() {
        let options = SstWriterOptions {
            block_size: 128,
            compression: CompressionType::None,
        };
        let mut writer = SstWriterImpl::with_options(options);
        for i in 0..100u64 {
            let key = format!("key_{:05}", i);
            let val = format!("value_{:05}", i);
            writer
                .add(key.as_bytes(), Some(val.as_bytes()), i + 1, 0)
                .unwrap();
        }
        let (_data, info) = writer.finish().unwrap();
        assert_eq!(info.entry_count, 100);
        assert!(
            info.data_block_count > 1,
            "should have multiple blocks with 128B block_size"
        );
    }

    #[test]
    fn test_writer_footer_has_valid_index_offset() {
        let mut writer = SstWriterImpl::new();
        writer.add(b"k1", Some(b"v1"), 1, 0).unwrap();
        let (data, _info) = writer.finish().unwrap();

        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();

        assert!(footer.index_offset > FILE_HEADER_SIZE as u64);
        assert!(footer.index_size > 0);
        assert_eq!(footer.bloom_filter_offset, 0);
        assert_eq!(footer.bloom_filter_size, 0);
        assert_eq!(footer.data_block_count, 1);
        assert_eq!(footer.total_entries, 1);
    }

    #[test]
    fn test_writer_index_section_roundtrips() {
        let options = SstWriterOptions {
            block_size: 64,
            compression: CompressionType::None,
        };
        let mut writer = SstWriterImpl::with_options(options);
        for i in 0..50u64 {
            let key = format!("k{:04}", i);
            writer.add(key.as_bytes(), Some(b"v"), i + 1, 0).unwrap();
        }
        let (data, info) = writer.finish().unwrap();

        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();

        let idx_start = footer.index_offset as usize;
        let idx_end = idx_start + footer.index_size as usize;
        let (entries, stats) = decode_index(&data[idx_start..idx_end]).unwrap();

        assert_eq!(entries.len(), info.data_block_count as usize);
        assert_eq!(stats.len(), info.data_block_count as usize);
        for i in 1..entries.len() {
            assert!(entries[i].last_key > entries[i - 1].last_key);
        }
    }

    #[test]
    fn test_writer_data_blocks_are_decodable() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 200,
            compression: CompressionType::None,
        });
        for i in 0..20u64 {
            let key = format!("key{:03}", i);
            let val = format!("val{:03}", i);
            writer
                .add(key.as_bytes(), Some(val.as_bytes()), i + 1, 0)
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();

        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();
        let idx_start = footer.index_offset as usize;
        let idx_end = idx_start + footer.index_size as usize;
        let (entries, _stats) = decode_index(&data[idx_start..idx_end]).unwrap();

        let mut total_rows = 0;
        for entry in &entries {
            let start = entry.block_offset as usize;
            let end = start + entry.block_size as usize;
            let batch = decode_data_block(&data[start..end]).unwrap();
            assert!(batch.num_rows() > 0);
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 20);
    }

    #[test]
    fn test_writer_empty_finish_errors() {
        let writer = SstWriterImpl::new();
        let result = writer.finish();
        assert!(result.is_err());
    }

    #[test]
    fn test_writer_estimated_size_grows() {
        let mut writer = SstWriterImpl::new();
        let s0 = writer.estimated_size();
        writer.add(b"key", Some(b"value"), 1, 0).unwrap();
        let s1 = writer.estimated_size();
        assert!(s1 > s0);
    }

    #[test]
    fn test_writer_lz4_compression() {
        let options = SstWriterOptions {
            block_size: 64 * 1024,
            compression: CompressionType::Lz4,
        };
        let mut writer = SstWriterImpl::with_options(options);
        for i in 0..10u64 {
            writer
                .add(format!("k{i}").as_bytes(), Some(b"v"), i, 0)
                .unwrap();
        }
        let (data, info) = writer.finish().unwrap();
        assert_eq!(info.entry_count, 10);
        assert!(!data.is_empty());
    }
}
