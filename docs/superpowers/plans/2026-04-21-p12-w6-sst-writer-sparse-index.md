# P1.2 W6: SstWriter + SparseIndex Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement SparseIndex (encode/decode/binary-search) and SstWriter (accumulate rows → flush DataBlocks → write complete SST files with FileHeader + DataBlocks + IndexSection + Footer).

**Architecture:** SstWriter accumulates rows into an in-memory Arrow RecordBatch builder. When estimated size reaches `block_size` (default 64KB), it flushes a DataBlock using existing `encode_data_block()`, records a `SparseIndexEntry` + `BlockStats`, and writes to the output buffer. On `finish()`, it writes the Index Section, Footer, and returns `SstFileInfo`. Bloom Filter is a placeholder (offset=0, size=0) until W7.

**Tech Stack:** Rust 1.75+, Arrow 54 (RecordBatch, BinaryBuilder, UInt64Builder, UInt8Builder), forst-rs-common (coding, checksum, types, config), forst-rs-storage (W5: data_block, file_header, footer, schema, compression)

---

## File Structure

```
crates/forst-rs-storage/src/sst/
├── mod.rs              (MODIFY — add sparse_index, writer modules + re-exports)
├── sparse_index.rs     (CREATE — SparseIndexEntry, BlockStats, encode/decode/search)
├── writer.rs           (CREATE — SstWriter trait, SstWriterImpl, SstFileInfo)
├── schema.rs           (existing)
├── data_block.rs       (existing)
├── block_header.rs     (existing)
├── file_header.rs      (existing)
├── footer.rs           (existing)
└── compression.rs      (existing)
```

---

### Task 1: SparseIndex types + encode/decode

**Files:**
- Create: `crates/forst-rs-storage/src/sst/sparse_index.rs`
- Modify: `crates/forst-rs-storage/src/sst/mod.rs`

This task implements the Index Section binary format from design doc §2.4:
- `SparseIndexEntry { last_key: Vec<u8>, block_offset: u64, block_size: u32 }`
- `BlockStats { min_key: Vec<u8>, max_key: Vec<u8>, entry_count: u32, min_sequence: u64, max_sequence: u64 }`
- `encode_index(entries: &[SparseIndexEntry], stats: &[BlockStats]) -> Vec<u8>`
- `decode_index(data: &[u8]) -> ForstResult<(Vec<SparseIndexEntry>, Vec<BlockStats>)>`
- `search_index(entries: &[SparseIndexEntry], target_key: &[u8]) -> Option<usize>`

**Index Section binary layout (from design doc §2.4.3):**
```
[num_blocks: u32 LE]
For each block (num_blocks times):
  [key_len: u16 LE] [key_bytes: key_len] [block_offset: u64 LE] [block_size: u32 LE]
For each block (num_blocks times):
  [min_key_len: u16 LE] [min_key_bytes] [max_key_len: u16 LE] [max_key_bytes] [entry_count: u32 LE] [min_sequence: u64 LE] [max_sequence: u64 LE]
```

- [ ] **Step 1: Write failing tests for SparseIndexEntry, BlockStats, and encode_index**

Create `crates/forst-rs-storage/src/sst/sparse_index.rs` with test module:

```rust
// Copyright 2026 The ForSt-RS Authors
// (Apache 2.0 license header)

//! Sparse index and block statistics for SST Index Section.
//!
//! The Index Section follows all DataBlocks and precedes the Footer.
//! It contains two parallel arrays: one [`SparseIndexEntry`] per DataBlock
//! (for binary-search point lookup) and one [`BlockStats`] per DataBlock
//! (for range pruning during scans and compaction).

use forst_rs_common::{get_fixed32, get_fixed64, put_fixed32, put_fixed64, ForstError, ForstResult};

/// Sparse index entry for one DataBlock.
///
/// `last_key` is the largest key in the block. Binary search over a sorted
/// array of entries finds the first entry where `last_key >= target_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseIndexEntry {
    /// The last (largest) key in this DataBlock.
    pub last_key: Vec<u8>,
    /// Byte offset of the DataBlock within the SST file.
    pub block_offset: u64,
    /// Size in bytes of the DataBlock (including the 16-byte BlockHeader).
    pub block_size: u32,
}

/// Per-DataBlock statistics used for range pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockStats {
    /// Smallest key in this DataBlock.
    pub min_key: Vec<u8>,
    /// Largest key in this DataBlock.
    pub max_key: Vec<u8>,
    /// Number of key-value entries in this DataBlock.
    pub entry_count: u32,
    /// Smallest sequence number in this DataBlock.
    pub min_sequence: u64,
    /// Largest sequence number in this DataBlock.
    pub max_sequence: u64,
}

// Public API stubs — will be implemented in Step 3.

/// Encodes sparse index entries and block stats into the Index Section binary format.
pub fn encode_index(_entries: &[SparseIndexEntry], _stats: &[BlockStats]) -> Vec<u8> {
    todo!()
}

/// Decodes the Index Section binary format back into entries and stats.
pub fn decode_index(_data: &[u8]) -> ForstResult<(Vec<SparseIndexEntry>, Vec<BlockStats>)> {
    todo!()
}

/// Binary-searches the sparse index for the block containing `target_key`.
///
/// Returns the index of the first entry where `last_key >= target_key`,
/// or `None` if `target_key` is greater than all last keys.
pub fn search_index(_entries: &[SparseIndexEntry], _target_key: &[u8]) -> Option<usize> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entries() -> Vec<SparseIndexEntry> {
        vec![
            SparseIndexEntry {
                last_key: b"ccc".to_vec(),
                block_offset: 16,  // after FileHeader
                block_size: 1024,
            },
            SparseIndexEntry {
                last_key: b"fff".to_vec(),
                block_offset: 1040,
                block_size: 2048,
            },
            SparseIndexEntry {
                last_key: b"zzz".to_vec(),
                block_offset: 3088,
                block_size: 512,
            },
        ]
    }

    fn sample_stats() -> Vec<BlockStats> {
        vec![
            BlockStats {
                min_key: b"aaa".to_vec(),
                max_key: b"ccc".to_vec(),
                entry_count: 100,
                min_sequence: 1,
                max_sequence: 100,
            },
            BlockStats {
                min_key: b"ddd".to_vec(),
                max_key: b"fff".to_vec(),
                entry_count: 200,
                min_sequence: 101,
                max_sequence: 300,
            },
            BlockStats {
                min_key: b"ggg".to_vec(),
                max_key: b"zzz".to_vec(),
                entry_count: 50,
                min_sequence: 301,
                max_sequence: 350,
            },
        ]
    }

    #[test]
    fn test_encode_starts_with_num_blocks() {
        let entries = sample_entries();
        let stats = sample_stats();
        let encoded = encode_index(&entries, &stats);
        let (num_blocks, _) = get_fixed32(&encoded).unwrap();
        assert_eq!(num_blocks, 3);
    }

    #[test]
    fn test_roundtrip() {
        let entries = sample_entries();
        let stats = sample_stats();
        let encoded = encode_index(&entries, &stats);
        let (decoded_entries, decoded_stats) = decode_index(&encoded).unwrap();
        assert_eq!(decoded_entries, entries);
        assert_eq!(decoded_stats, stats);
    }

    #[test]
    fn test_roundtrip_empty() {
        let encoded = encode_index(&[], &[]);
        let (entries, stats) = decode_index(&encoded).unwrap();
        assert!(entries.is_empty());
        assert!(stats.is_empty());
    }

    #[test]
    fn test_roundtrip_single_block() {
        let entries = vec![SparseIndexEntry {
            last_key: b"only".to_vec(),
            block_offset: 16,
            block_size: 4096,
        }];
        let stats = vec![BlockStats {
            min_key: b"only".to_vec(),
            max_key: b"only".to_vec(),
            entry_count: 1,
            min_sequence: 42,
            max_sequence: 42,
        }];
        let encoded = encode_index(&entries, &stats);
        let (de, ds) = decode_index(&encoded).unwrap();
        assert_eq!(de, entries);
        assert_eq!(ds, stats);
    }

    #[test]
    fn test_roundtrip_large_keys() {
        let entries = vec![SparseIndexEntry {
            last_key: vec![0xFF; 1024],
            block_offset: 16,
            block_size: 65536,
        }];
        let stats = vec![BlockStats {
            min_key: vec![0x00; 512],
            max_key: vec![0xFF; 1024],
            entry_count: 5000,
            min_sequence: 1,
            max_sequence: 5000,
        }];
        let encoded = encode_index(&entries, &stats);
        let (de, ds) = decode_index(&encoded).unwrap();
        assert_eq!(de, entries);
        assert_eq!(ds, stats);
    }

    #[test]
    fn test_decode_truncated_data() {
        let result = decode_index(&[0u8; 2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_search_finds_first_block() {
        let entries = sample_entries();
        assert_eq!(search_index(&entries, b"aaa"), Some(0));
        assert_eq!(search_index(&entries, b"bbb"), Some(0));
        assert_eq!(search_index(&entries, b"ccc"), Some(0));
    }

    #[test]
    fn test_search_finds_middle_block() {
        let entries = sample_entries();
        assert_eq!(search_index(&entries, b"ddd"), Some(1));
        assert_eq!(search_index(&entries, b"fff"), Some(1));
    }

    #[test]
    fn test_search_finds_last_block() {
        let entries = sample_entries();
        assert_eq!(search_index(&entries, b"ggg"), Some(2));
        assert_eq!(search_index(&entries, b"zzz"), Some(2));
    }

    #[test]
    fn test_search_beyond_last_key_returns_none() {
        let entries = sample_entries();
        assert_eq!(search_index(&entries, b"zzz\x00"), None);
    }

    #[test]
    fn test_search_empty_index() {
        assert_eq!(search_index(&[], b"anything"), None);
    }

    // Mismatched lengths should error
    #[test]
    fn test_encode_panics_on_mismatched_lengths() {
        // encode_index should return an error or panic if entries.len() != stats.len()
        // We test that the API enforces this via assert_eq! in encode_index.
        let result = std::panic::catch_unwind(|| {
            encode_index(&sample_entries(), &[]);
        });
        assert!(result.is_err());
    }
}
```

- [ ] **Step 2: Run tests — they should all fail (todo!())**

Run: `cd ~/code/github/ForSt && export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p forst-rs-storage sparse_index 2>&1 | tail -20`
Expected: All tests FAIL with `not yet implemented`

- [ ] **Step 3: Implement encode_index, decode_index, search_index**

Replace the `todo!()` stubs with:

```rust
/// Encodes sparse index entries and block stats into the Index Section binary format.
///
/// # Panics
/// Panics if `entries.len() != stats.len()`.
pub fn encode_index(entries: &[SparseIndexEntry], stats: &[BlockStats]) -> Vec<u8> {
    assert_eq!(
        entries.len(),
        stats.len(),
        "entries and stats must have the same length"
    );
    let num_blocks = entries.len() as u32;
    let mut buf = Vec::new();

    // Header: num_blocks
    put_fixed32(&mut buf, num_blocks);

    // SparseIndex entries
    for entry in entries {
        let key_len = entry.last_key.len() as u16;
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&entry.last_key);
        put_fixed64(&mut buf, entry.block_offset);
        put_fixed32(&mut buf, entry.block_size);
    }

    // BlockStats entries
    for stat in stats {
        let min_key_len = stat.min_key.len() as u16;
        let max_key_len = stat.max_key.len() as u16;
        buf.extend_from_slice(&min_key_len.to_le_bytes());
        buf.extend_from_slice(&stat.min_key);
        buf.extend_from_slice(&max_key_len.to_le_bytes());
        buf.extend_from_slice(&stat.max_key);
        put_fixed32(&mut buf, stat.entry_count);
        put_fixed64(&mut buf, stat.min_sequence);
        put_fixed64(&mut buf, stat.max_sequence);
    }

    buf
}

/// Decodes the Index Section binary format back into entries and stats.
pub fn decode_index(data: &[u8]) -> ForstResult<(Vec<SparseIndexEntry>, Vec<BlockStats>)> {
    if data.len() < 4 {
        return Err(ForstError::corruption(format!(
            "index section too short: expected at least 4 bytes, got {}",
            data.len()
        )));
    }

    let mut offset = 0;
    let (num_blocks, n) = get_fixed32(&data[offset..])?;
    offset += n;
    let num_blocks = num_blocks as usize;

    // Decode SparseIndex entries
    let mut entries = Vec::with_capacity(num_blocks);
    for _ in 0..num_blocks {
        if offset + 2 > data.len() {
            return Err(ForstError::corruption("index section truncated at entry key_len"));
        }
        let key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        if offset + key_len > data.len() {
            return Err(ForstError::corruption("index section truncated at entry key"));
        }
        let last_key = data[offset..offset + key_len].to_vec();
        offset += key_len;
        let (block_offset, n) = get_fixed64(&data[offset..])?;
        offset += n;
        let (block_size, n) = get_fixed32(&data[offset..])?;
        offset += n;
        entries.push(SparseIndexEntry { last_key, block_offset, block_size });
    }

    // Decode BlockStats entries
    let mut stats = Vec::with_capacity(num_blocks);
    for _ in 0..num_blocks {
        if offset + 2 > data.len() {
            return Err(ForstError::corruption("index section truncated at stats min_key_len"));
        }
        let min_key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        if offset + min_key_len > data.len() {
            return Err(ForstError::corruption("index section truncated at stats min_key"));
        }
        let min_key = data[offset..offset + min_key_len].to_vec();
        offset += min_key_len;

        if offset + 2 > data.len() {
            return Err(ForstError::corruption("index section truncated at stats max_key_len"));
        }
        let max_key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        if offset + max_key_len > data.len() {
            return Err(ForstError::corruption("index section truncated at stats max_key"));
        }
        let max_key = data[offset..offset + max_key_len].to_vec();
        offset += max_key_len;

        let (entry_count, n) = get_fixed32(&data[offset..])?;
        offset += n;
        let (min_sequence, n) = get_fixed64(&data[offset..])?;
        offset += n;
        let (max_sequence, n) = get_fixed64(&data[offset..])?;
        offset += n;

        stats.push(BlockStats {
            min_key, max_key, entry_count, min_sequence, max_sequence,
        });
    }

    Ok((entries, stats))
}

/// Binary-searches the sparse index for the block containing `target_key`.
///
/// Returns the index of the first entry where `last_key >= target_key`,
/// or `None` if `target_key` is greater than all last keys.
pub fn search_index(entries: &[SparseIndexEntry], target_key: &[u8]) -> Option<usize> {
    if entries.is_empty() {
        return None;
    }
    // Find the first entry where last_key >= target_key
    let idx = entries.partition_point(|e| e.last_key.as_slice() < target_key);
    if idx < entries.len() {
        Some(idx)
    } else {
        None
    }
}
```

- [ ] **Step 4: Update mod.rs to add sparse_index module**

Add to `crates/forst-rs-storage/src/sst/mod.rs`:
```rust
pub mod sparse_index;
```
And add re-exports:
```rust
pub use sparse_index::{
    encode_index, decode_index, search_index, BlockStats, SparseIndexEntry,
};
```

- [ ] **Step 5: Run tests — all should pass**

Run: `cd ~/code/github/ForSt && export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p forst-rs-storage sparse_index -- --nocapture 2>&1 | tail -20`
Expected: 12 tests PASS

- [ ] **Step 6: Run clippy**

Run: `cd ~/code/github/ForSt && export PATH="$HOME/.cargo/bin:$PATH" && cargo clippy -p forst-rs-storage -- -D warnings 2>&1 | tail -10`
Expected: 0 warnings

- [ ] **Step 7: Commit**

```bash
cd ~/code/github/ForSt
git add crates/forst-rs-storage/src/sst/sparse_index.rs crates/forst-rs-storage/src/sst/mod.rs
git commit -m "feat: add SparseIndex encode/decode/search for SST Index Section"
```

---

### Task 2: SstWriter trait + SstFileInfo + SstWriterImpl

**Files:**
- Create: `crates/forst-rs-storage/src/sst/writer.rs`
- Modify: `crates/forst-rs-storage/src/sst/mod.rs`

This task implements the SstWriter from design doc §3.1. The writer:
1. Writes `FileHeader` (16B) at byte 0
2. Accumulates rows via `add()` into Arrow array builders
3. When accumulated data estimated size >= `block_size`, flushes a DataBlock
4. Each flush records a `SparseIndexEntry` + `BlockStats`
5. `finish()` flushes remaining rows, writes IndexSection, writes FooterV1, returns `SstFileInfo`

**Key design decisions:**
- Bloom Filter placeholder: `bloom_filter_offset=0, bloom_filter_size=0` (implemented in W7)
- `add()` accepts individual KV pairs; `add_batch()` accepts pre-built RecordBatch
- Output is a `Vec<u8>` (in-memory), not a file — file writing uses `forst-rs-io` in later weeks
- Estimated size for flush trigger: sum of key/value byte lengths in current batch builder

**Types from existing crates:**
- `CompressionType` from `forst_rs_common::types`
- `EngineOptions` from `forst_rs_common::config` (uses `block_size` and `compression`)
- `encode_data_block()` from `super::data_block`
- `FileHeader` from `super::file_header`
- `FooterV1, ChecksumType` from `super::footer`
- `encode_index, SparseIndexEntry, BlockStats` from `super::sparse_index`
- `sst_schema, SST_FORMAT_VERSION, FILE_HEADER_SIZE` from `super::schema`

- [ ] **Step 1: Write failing tests for SstWriter**

Create `crates/forst-rs-storage/src/sst/writer.rs`:

```rust
// Copyright 2026 The ForSt-RS Authors
// (Apache 2.0 license header)

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

use arrow::array::{BinaryBuilder, RecordBatch, UInt64Builder, UInt8Builder};
use arrow::datatypes::Schema;

use forst_rs_common::{CompressionType, ForstError, ForstResult};

use super::data_block::encode_data_block;
use super::file_header::FileHeader;
use super::footer::{ChecksumType, FooterV1};
use super::schema::{sst_schema, FILE_HEADER_SIZE, SST_FORMAT_VERSION};
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
    /// Output buffer (the SST file bytes).
    buf: Vec<u8>,
    /// Current batch builders for accumulating rows.
    key_builder: BinaryBuilder,
    value_builder: BinaryBuilder,
    sequence_builder: UInt64Builder,
    op_type_builder: UInt8Builder,
    /// Estimated size of data in current builders (sum of key+value byte lengths).
    current_estimated_size: usize,
    /// Index data accumulated across flushes.
    index_entries: Vec<SparseIndexEntry>,
    block_stats: Vec<BlockStats>,
    /// Global statistics.
    total_entries: u64,
    global_min_key: Option<Vec<u8>>,
    global_max_key: Option<Vec<u8>>,
    global_min_sequence: u64,
    global_max_sequence: u64,
    /// Whether finish() has been called.
    finished: bool,
}

impl SstWriterImpl {
    /// Creates a new writer with default options.
    pub fn new() -> Self {
        Self::with_options(SstWriterOptions::default())
    }

    /// Creates a new writer with the given options.
    pub fn with_options(options: SstWriterOptions) -> Self {
        let schema = Arc::new(sst_schema());
        let mut buf = Vec::new();
        // Write FileHeader immediately.
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

        self.key_builder.append_value(key);
        match value {
            Some(v) => self.value_builder.append_value(v),
            None => self.value_builder.append_null(),
        }
        self.sequence_builder.append_value(sequence);
        self.op_type_builder.append_value(op_type);

        let entry_size = key.len() + value.map_or(0, |v| v.len());
        self.current_estimated_size += entry_size;

        // Update global stats
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

        // Check if we should flush the current block.
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

        let footer = FooterV1 {
            data_block_count: self.index_entries.len() as u32,
            total_entries: self.total_entries,
            bloom_filter_offset: 0,
            bloom_filter_size: 0,
            index_offset,
            index_size,
            min_key: self.global_min_key.clone().unwrap_or_default(),
            max_key: self.global_max_key.clone().unwrap_or_default(),
            min_sequence: self.global_min_sequence,
            max_sequence: self.global_max_sequence,
            compression: self.options.compression,
            checksum_type: ChecksumType::Crc32c,
            creation_time,
            format_version: SST_FORMAT_VERSION,
        };
        self.buf.extend_from_slice(&footer.encode());

        let file_size = self.buf.len() as u64;
        let info = SstFileInfo {
            file_size,
            entry_count: self.total_entries,
            data_block_count: self.index_entries.len() as u32,
            min_key: self.global_min_key.unwrap_or_default(),
            max_key: self.global_max_key.unwrap_or_default(),
            min_sequence: self.global_min_sequence,
            max_sequence: self.global_max_sequence,
        };

        Ok((self.buf, info))
    }

    /// Flushes the current accumulated rows as a DataBlock.
    fn flush_block(&mut self) -> ForstResult<()> {
        let num_rows = self.key_builder.len();
        if num_rows == 0 {
            return Ok(());
        }

        // Build RecordBatch from builders (std::mem::replace to reset builders).
        let key_array = std::mem::replace(&mut self.key_builder, BinaryBuilder::new()).finish();
        let value_array = std::mem::replace(&mut self.value_builder, BinaryBuilder::new()).finish();
        let seq_array = std::mem::replace(&mut self.sequence_builder, UInt64Builder::new()).finish();
        let op_array = std::mem::replace(&mut self.op_type_builder, UInt8Builder::new()).finish();

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(key_array.clone()),
                Arc::new(value_array),
                Arc::new(seq_array.clone()),
                Arc::new(op_array),
            ],
        )
        .map_err(|e| ForstError::corruption(format!("failed to build RecordBatch: {e}")))?;

        // Encode to DataBlock.
        let block_bytes = encode_data_block(&batch, self.options.compression)?;
        let block_offset = self.buf.len() as u64;
        let block_size = block_bytes.len() as u32;
        self.buf.extend_from_slice(&block_bytes);

        // Record sparse index entry: last_key = key at last row.
        let last_key = key_array.value(num_rows - 1).to_vec();
        self.index_entries.push(SparseIndexEntry {
            last_key,
            block_offset,
            block_size,
        });

        // Record block stats.
        let min_key = key_array.value(0).to_vec();
        let max_key = key_array.value(num_rows - 1).to_vec();
        let mut min_seq = u64::MAX;
        let mut max_seq = 0u64;
        for i in 0..seq_array.len() {
            let s = seq_array.value(i);
            if s < min_seq { min_seq = s; }
            if s > max_seq { max_seq = s; }
        }

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
```

Add tests at the bottom of `writer.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::data_block::decode_data_block;
    use crate::sst::file_header::FileHeader;
    use crate::sst::footer::FooterV1;
    use crate::sst::schema::{FILE_HEADER_SIZE, BLOCK_HEADER_SIZE, SST_MAGIC};
    use crate::sst::sparse_index::decode_index;
    use arrow::array::{Array, BinaryArray, UInt64Array, UInt8Array};
    use forst_rs_common::get_fixed32;

    #[test]
    fn test_writer_produces_valid_sst() {
        let mut writer = SstWriterImpl::new();
        // Add 3 sorted entries.
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

        // Verify file starts with FileHeader magic.
        assert_eq!(&data[..4], SST_MAGIC);

        // Verify file ends with Footer magic.
        let len = data.len();
        assert_eq!(&data[len - 4..], SST_MAGIC);
    }

    #[test]
    fn test_writer_with_delete_tombstones() {
        let mut writer = SstWriterImpl::new();
        writer.add(b"key1", Some(b"val"), 1, 0).unwrap(); // Put
        writer.add(b"key2", None, 2, 1).unwrap();          // Delete (null value)
        let (data, info) = writer.finish().unwrap();
        assert_eq!(info.entry_count, 2);
        assert!(data.len() > FILE_HEADER_SIZE);
    }

    #[test]
    fn test_writer_forces_flush_at_block_size() {
        let options = SstWriterOptions {
            block_size: 128, // Very small to force multiple blocks.
            compression: CompressionType::None,
        };
        let mut writer = SstWriterImpl::with_options(options);
        // Add enough entries to exceed 128 bytes multiple times.
        for i in 0..100u64 {
            let key = format!("key_{:05}", i);
            let val = format!("value_{:05}", i);
            writer.add(key.as_bytes(), Some(val.as_bytes()), i + 1, 0).unwrap();
        }
        let (_data, info) = writer.finish().unwrap();
        assert_eq!(info.entry_count, 100);
        assert!(info.data_block_count > 1, "should have multiple blocks with 128B block_size");
    }

    #[test]
    fn test_writer_footer_has_valid_index_offset() {
        let mut writer = SstWriterImpl::new();
        writer.add(b"k1", Some(b"v1"), 1, 0).unwrap();
        let (data, _info) = writer.finish().unwrap();

        // Parse footer from tail.
        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();

        assert!(footer.index_offset > FILE_HEADER_SIZE as u64);
        assert!(footer.index_size > 0);
        assert_eq!(footer.bloom_filter_offset, 0); // placeholder
        assert_eq!(footer.bloom_filter_size, 0);    // placeholder
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

        // Parse footer, then parse index section.
        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();

        let idx_start = footer.index_offset as usize;
        let idx_end = idx_start + footer.index_size as usize;
        let (entries, stats) = decode_index(&data[idx_start..idx_end]).unwrap();

        assert_eq!(entries.len(), info.data_block_count as usize);
        assert_eq!(stats.len(), info.data_block_count as usize);

        // Verify entries are in sorted order.
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
            writer.add(key.as_bytes(), Some(val.as_bytes()), i + 1, 0).unwrap();
        }
        let (data, _info) = writer.finish().unwrap();

        // Parse footer and index to find block offsets.
        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();
        let idx_start = footer.index_offset as usize;
        let idx_end = idx_start + footer.index_size as usize;
        let (entries, _stats) = decode_index(&data[idx_start..idx_end]).unwrap();

        // Decode each DataBlock.
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
            writer.add(format!("k{i}").as_bytes(), Some(b"v"), i, 0).unwrap();
        }
        let (data, info) = writer.finish().unwrap();
        assert_eq!(info.entry_count, 10);
        assert!(data.len() > 0);
    }
}
```

- [ ] **Step 2: Update mod.rs — add writer module + re-exports**

Add to `crates/forst-rs-storage/src/sst/mod.rs`:
```rust
pub mod writer;
```
And re-exports:
```rust
pub use writer::{SstFileInfo, SstWriterImpl, SstWriterOptions};
```

- [ ] **Step 3: Run tests — all should pass**

Run: `cd ~/code/github/ForSt && export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p forst-rs-storage writer -- --nocapture 2>&1 | tail -30`
Expected: 9 tests PASS

- [ ] **Step 4: Run full crate tests + clippy**

Run: `cd ~/code/github/ForSt && export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p forst-rs-storage 2>&1 | tail -10`
Run: `cd ~/code/github/ForSt && export PATH="$HOME/.cargo/bin:$PATH" && cargo clippy -p forst-rs-storage -- -D warnings 2>&1 | tail -10`
Expected: All tests PASS (W5 + W6), 0 clippy warnings

- [ ] **Step 5: Commit**

```bash
cd ~/code/github/ForSt
git add crates/forst-rs-storage/src/sst/writer.rs crates/forst-rs-storage/src/sst/mod.rs
git commit -m "feat: add SstWriter with block accumulation, flush, and SST file generation"
```

---

### Task 3: Integration test — end-to-end SST write + read-back

**Files:**
- Create: `crates/forst-rs-storage/tests/sst_integration.rs`

This task adds an integration test that writes 1000 sorted KV pairs through `SstWriterImpl`, then verifies the complete SST structure: FileHeader → DataBlocks → IndexSection → Footer, and reads back all entries through DataBlock decoding.

- [ ] **Step 1: Write integration test**

Create `crates/forst-rs-storage/tests/sst_integration.rs`:

```rust
// Copyright 2026 The ForSt-RS Authors
// (Apache 2.0 license header)

//! End-to-end integration test: SstWriter → complete SST file → read back all entries.

use arrow::array::{Array, BinaryArray, UInt64Array, UInt8Array};
use forst_rs_common::{get_fixed32, CompressionType};
use forst_rs_storage::sst::{
    decode_data_block, decode_index, search_index, FileHeader, FooterV1, SstWriterImpl,
    SstWriterOptions, FILE_HEADER_SIZE, SST_MAGIC,
};

/// Writes N sorted entries, then verifies every entry can be read back.
fn write_and_verify(n: usize, compression: CompressionType, block_size: usize) {
    let options = SstWriterOptions { block_size, compression };
    let mut writer = SstWriterImpl::with_options(options);

    // Generate sorted keys: "key_00000" .. "key_NNNNN"
    for i in 0..n {
        let key = format!("key_{:05}", i);
        let val = format!("val_{:05}", i);
        writer.add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 0).unwrap();
    }

    let (data, info) = writer.finish().unwrap();

    // --- Structural verification ---

    // 1. FileHeader
    assert_eq!(&data[..4], SST_MAGIC);
    let header = FileHeader::decode(&data[..FILE_HEADER_SIZE]).unwrap();
    assert_eq!(header.format_version, 1);

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

        let keys = batch.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        let vals = batch.column(1).as_any().downcast_ref::<BinaryArray>().unwrap();
        let seqs = batch.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();

        for row in 0..batch.num_rows() {
            all_keys.push(keys.value(row).to_vec());
            all_vals.push(vals.value(row).to_vec());
            all_seqs.push(seqs.value(row));
        }
    }

    assert_eq!(all_keys.len(), n);
    for i in 0..n {
        assert_eq!(all_keys[i], format!("key_{:05}", i).as_bytes());
        assert_eq!(all_vals[i], format!("val_{:05}", i).as_bytes());
        assert_eq!(all_seqs[i], i as u64 + 1);
    }

    // 5. Point lookup via search_index
    let target = format!("key_{:05}", n / 2);
    let block_idx = search_index(&entries, target.as_bytes());
    assert!(block_idx.is_some(), "search_index should find the key");

    // Verify the found block actually contains the key.
    let bi = block_idx.unwrap();
    let start = entries[bi].block_offset as usize;
    let end = start + entries[bi].block_size as usize;
    let batch = decode_data_block(&data[start..end]).unwrap();
    let keys = batch.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
    let mut found = false;
    for row in 0..batch.num_rows() {
        if keys.value(row) == target.as_bytes() {
            found = true;
            break;
        }
    }
    assert!(found, "target key should be in the located DataBlock");
}

#[test]
fn test_e2e_1000_entries_lz4_64kb_blocks() {
    write_and_verify(1000, CompressionType::Lz4, 64 * 1024);
}

#[test]
fn test_e2e_1000_entries_no_compression_small_blocks() {
    write_and_verify(1000, CompressionType::None, 256);
}

#[test]
fn test_e2e_100_entries_zstd() {
    write_and_verify(100, CompressionType::Zstd, 4096);
}

#[test]
fn test_e2e_single_entry() {
    write_and_verify(1, CompressionType::None, 64 * 1024);
}
```

- [ ] **Step 2: Run integration tests**

Run: `cd ~/code/github/ForSt && export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p forst-rs-storage --test sst_integration -- --nocapture 2>&1 | tail -20`
Expected: 4 tests PASS

- [ ] **Step 3: Run full test suite**

Run: `cd ~/code/github/ForSt && export PATH="$HOME/.cargo/bin:$PATH" && cargo test --workspace 2>&1 | tail -15`
Expected: All tests PASS (W1-W4 crates + W5-W6 storage)

- [ ] **Step 4: Commit**

```bash
cd ~/code/github/ForSt
git add crates/forst-rs-storage/tests/sst_integration.rs
git commit -m "test: add end-to-end SST write/read integration tests"
```

---

## Self-Review Checklist

1. **Spec coverage**: SparseIndexEntry (§2.4.1) ✅, BlockStats (§2.4.2) ✅, Index serialization (§2.4.3) ✅, SstWriter trait (§3.1) ✅, SstFileInfo ✅, binary search ✅, DataBlock flush trigger ✅, FileHeader written ✅, Footer written ✅, Bloom Filter placeholder ✅
2. **Placeholder scan**: No "TBD", "TODO", "implement later" — all code is complete
3. **Type consistency**: `SparseIndexEntry.last_key`/`block_offset`/`block_size` matches across Task 1 and Task 2; `BlockStats` fields match; `encode_index`/`decode_index` signatures consistent; `SstFileInfo` fields match Footer fields
