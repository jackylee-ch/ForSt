# ForSt-RS P1.2 W5: DataBlock + SST Header/Footer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the SST storage-layer building blocks — DataBlock encode/decode (Arrow RecordBatch serialization with compression), SST file header, and footer V1 — in a new `forst-rs-storage` crate.

**Architecture:** A new `forst-rs-storage` crate sits between `forst-rs-io` (filesystem) and the future engine layer. The `sst` module provides encode/decode functions for the three structural components of an SST file: the 16-byte file header, data blocks (Arrow IPC + compression + 16-byte block header), and the variable-length footer with CRC32C integrity checks.

**Tech Stack:** Rust 1.75, Arrow 54 (IPC StreamWriter/StreamReader), lz4_flex 0.11, zstd 0.13, crc32c 0.6

**Environment:** Before ANY `cargo` command, run: `export PATH="/home/users/lijunqing/.cargo/bin:/usr/bin:/bin:/usr/local/bin:$PATH"`

---

## File Structure

```
Cargo.toml                                    ← Modify: add forst-rs-storage to workspace members
crates/forst-rs-storage/
├── Cargo.toml                                ← Create: crate manifest
└── src/
    ├── lib.rs                                ← Create: #![forbid(unsafe_code)], pub mod sst
    └── sst/
        ├── mod.rs                            ← Create: re-exports
        ├── schema.rs                         ← Create: SST Arrow schema, magic, constants
        ├── compression.rs                    ← Create: compress/decompress for LZ4 + Zstd
        ├── block_header.rs                   ← Create: BlockHeader (16B) encode/decode
        ├── data_block.rs                     ← Create: DataBlock encode/decode
        ├── file_header.rs                    ← Create: FileHeader (16B) encode/decode
        └── footer.rs                         ← Create: ChecksumType enum, FooterV1 encode/decode
```

---

## Task 1: Create `forst-rs-storage` Crate + SST Schema + Compression Utilities

**Files:**
- Modify: `Cargo.toml` (workspace root, line 16-20)
- Create: `crates/forst-rs-storage/Cargo.toml`
- Create: `crates/forst-rs-storage/src/lib.rs`
- Create: `crates/forst-rs-storage/src/sst/mod.rs`
- Create: `crates/forst-rs-storage/src/sst/schema.rs`
- Create: `crates/forst-rs-storage/src/sst/compression.rs`

### Step 1.1: Add `forst-rs-storage` to workspace members

Edit `Cargo.toml` (workspace root). Change the `members` array:

```toml
[workspace]
members = [
    "crates/forst-rs-common",
    "crates/forst-rs-io",
    "crates/forst-rs-storage",
    "crates/forst-rs-test-utils",
]
```

Also add the internal crate to `[workspace.dependencies]`:

```toml
forst-rs-storage = { path = "crates/forst-rs-storage" }
```

### Step 1.2: Create crate Cargo.toml

Create `crates/forst-rs-storage/Cargo.toml`:

```toml
# Copyright 2026 The ForSt-RS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

[package]
name = "forst-rs-storage"
version = "0.1.0"
description = "SST file format: data blocks, index, bloom filter, header/footer"
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
forst-rs-common = { workspace = true }
arrow = { workspace = true }
lz4_flex = { workspace = true }
zstd = { workspace = true }

[dev-dependencies]
forst-rs-test-utils = { workspace = true }
```

### Step 1.3: Create `lib.rs`

Create `crates/forst-rs-storage/src/lib.rs`:

```rust
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

//! ForSt-RS storage layer: SST file format components.
//!
//! This crate provides the on-disk format building blocks for SST files:
//! data block encode/decode, file header, and footer serialization.

#![forbid(unsafe_code)]

pub mod sst;
```

### Step 1.4: Create `sst/mod.rs`

Create `crates/forst-rs-storage/src/sst/mod.rs`:

```rust
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

//! SST file format components.

pub mod schema;
pub mod compression;

pub use schema::{
    sst_schema, BLOCK_HEADER_SIZE, BLOCK_TYPE_DATA, FILE_HEADER_SIZE, SST_FORMAT_VERSION,
    SST_MAGIC,
};
pub use compression::{compress, decompress};
```

### Step 1.5: Write failing tests for `schema.rs`

Create `crates/forst-rs-storage/src/sst/schema.rs`:

```rust
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

//! SST schema definition and format constants.

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn test_sst_schema_has_four_columns() {
        let schema = sst_schema();
        assert_eq!(schema.fields().len(), 4);
    }

    #[test]
    fn test_sst_schema_column_names() {
        let schema = sst_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["key", "value", "sequence", "op_type"]);
    }

    #[test]
    fn test_sst_schema_column_types() {
        let schema = sst_schema();
        assert_eq!(*schema.field(0).data_type(), DataType::Binary);
        assert_eq!(*schema.field(1).data_type(), DataType::Binary);
        assert_eq!(*schema.field(2).data_type(), DataType::UInt64);
        assert_eq!(*schema.field(3).data_type(), DataType::UInt8);
    }

    #[test]
    fn test_sst_schema_nullability() {
        let schema = sst_schema();
        assert!(!schema.field(0).is_nullable(), "key must not be nullable");
        assert!(schema.field(1).is_nullable(), "value must be nullable");
        assert!(!schema.field(2).is_nullable(), "sequence must not be nullable");
        assert!(!schema.field(3).is_nullable(), "op_type must not be nullable");
    }

    #[test]
    fn test_magic_constant() {
        assert_eq!(SST_MAGIC, b"FRST");
    }

    #[test]
    fn test_format_version() {
        assert_eq!(SST_FORMAT_VERSION, 1);
    }

    #[test]
    fn test_block_header_size() {
        assert_eq!(BLOCK_HEADER_SIZE, 16);
    }

    #[test]
    fn test_file_header_size() {
        assert_eq!(FILE_HEADER_SIZE, 16);
    }

    #[test]
    fn test_block_type_data() {
        assert_eq!(BLOCK_TYPE_DATA, 0x01);
    }
}
```

- [ ] **Step 1.6: Run tests to verify they fail**

```bash
export PATH="/home/users/lijunqing/.cargo/bin:/usr/bin:/bin:/usr/local/bin:$PATH"
cd ~/code/github/ForSt && cargo test -p forst-rs-storage 2>&1 | tail -20
```

Expected: compilation error — `sst_schema`, `SST_MAGIC`, etc. are not defined yet.

- [ ] **Step 1.7: Implement `schema.rs`**

Add the following above the `#[cfg(test)]` block in `crates/forst-rs-storage/src/sst/schema.rs`:

```rust
use arrow::datatypes::{DataType, Field, Schema};

/// Magic bytes at the start of every SST file and at the end of the footer.
pub const SST_MAGIC: &[u8; 4] = b"FRST";

/// Current SST format version.
pub const SST_FORMAT_VERSION: u16 = 1;

/// Block type identifier for data blocks.
pub const BLOCK_TYPE_DATA: u8 = 0x01;

/// Size of a block header in bytes.
pub const BLOCK_HEADER_SIZE: usize = 16;

/// Size of the SST file header in bytes.
pub const FILE_HEADER_SIZE: usize = 16;

/// Returns the Arrow schema for SST data blocks.
///
/// Columns:
/// - `key`      (Binary, not nullable)  — user key bytes
/// - `value`    (Binary, nullable)      — value bytes (null for Delete/SingleDelete)
/// - `sequence` (UInt64, not nullable)  — MVCC sequence number
/// - `op_type`  (UInt8, not nullable)   — operation type discriminant
pub fn sst_schema() -> Schema {
    Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("op_type", DataType::UInt8, false),
    ])
}
```

- [ ] **Step 1.8: Run schema tests to verify they pass**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::schema 2>&1 | tail -20
```

Expected: all 9 schema tests pass.

- [ ] **Step 1.9: Write failing tests for `compression.rs`**

Create `crates/forst-rs-storage/src/sst/compression.rs`:

```rust
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

//! Compression and decompression utilities for SST data blocks.

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_common::CompressionType;

    #[test]
    fn test_compress_none_is_identity() {
        let data = b"hello world";
        let compressed = compress(data, CompressionType::None).unwrap();
        assert_eq!(compressed, data);
    }

    #[test]
    fn test_decompress_none_is_identity() {
        let data = b"hello world";
        let decompressed = decompress(data, CompressionType::None, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"The quick brown fox jumps over the lazy dog. ".repeat(100);
        let compressed = compress(&data, CompressionType::Lz4).unwrap();
        assert!(compressed.len() < data.len(), "LZ4 should compress repetitive data");
        let decompressed = decompress(&compressed, CompressionType::Lz4, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_roundtrip() {
        let data = b"The quick brown fox jumps over the lazy dog. ".repeat(100);
        let compressed = compress(&data, CompressionType::Zstd).unwrap();
        assert!(compressed.len() < data.len(), "Zstd should compress repetitive data");
        let decompressed = decompress(&compressed, CompressionType::Zstd, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_small_data() {
        let data = b"ab";
        let compressed = compress(data, CompressionType::Lz4).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Lz4, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_small_data() {
        let data = b"ab";
        let compressed = compress(data, CompressionType::Zstd).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Zstd, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_empty_data() {
        let data = b"";
        let compressed = compress(data, CompressionType::Lz4).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Lz4, 0).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_empty_data() {
        let data = b"";
        let compressed = compress(data, CompressionType::Zstd).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Zstd, 0).unwrap();
        assert_eq!(decompressed, data);
    }
}
```

- [ ] **Step 1.10: Run tests to verify they fail**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::compression 2>&1 | tail -20
```

Expected: compilation error — `compress` and `decompress` not defined.

- [ ] **Step 1.11: Implement `compression.rs`**

Add the following above the `#[cfg(test)]` block in `crates/forst-rs-storage/src/sst/compression.rs`:

```rust
use forst_rs_common::{CompressionType, ForstError, ForstResult};

/// Compresses `data` using the specified algorithm.
///
/// - `None`: returns a copy of the input unchanged.
/// - `Lz4`: uses raw LZ4 block compression (no frame header).
/// - `Zstd`: uses Zstd frame compression at level 3.
pub fn compress(data: &[u8], compression: CompressionType) -> ForstResult<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
        CompressionType::Zstd => zstd::encode_all(data, 3)
            .map_err(|e| ForstError::corruption(format!("Zstd compression failed: {e}"))),
    }
}

/// Decompresses `data` using the specified algorithm.
///
/// - `None`: returns a copy of the input unchanged.
/// - `Lz4`: expects data produced by [`compress`] with `Lz4` (size-prepended).
/// - `Zstd`: expects a valid Zstd frame.
///
/// `_uncompressed_size` is provided for future use with raw LZ4 (no size prefix).
/// Currently unused for `Lz4` since we use `compress_prepend_size`/`decompress_size_prepended`.
pub fn decompress(
    data: &[u8],
    compression: CompressionType,
    _uncompressed_size: usize,
) -> ForstResult<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Lz4 => lz4_flex::decompress_size_prepended(data)
            .map_err(|e| ForstError::corruption(format!("LZ4 decompression failed: {e}"))),
        CompressionType::Zstd => zstd::decode_all(data)
            .map_err(|e| ForstError::corruption(format!("Zstd decompression failed: {e}"))),
    }
}
```

- [ ] **Step 1.12: Run compression tests to verify they pass**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::compression 2>&1 | tail -20
```

Expected: all 8 compression tests pass.

- [ ] **Step 1.13: Run all storage crate tests + clippy**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage 2>&1 | tail -20
cd ~/code/github/ForSt && cargo clippy -p forst-rs-storage -- -D warnings 2>&1 | tail -10
```

Expected: 17 tests pass, 0 clippy warnings.

- [ ] **Step 1.14: Commit**

```bash
cd ~/code/github/ForSt
git add Cargo.toml crates/forst-rs-storage/
git commit -m "feat: add forst-rs-storage crate with SST schema and compression utilities

New crate forst-rs-storage provides the storage layer building blocks.
- SST Arrow schema: key(Binary), value(Binary,nullable), sequence(UInt64), op_type(UInt8)
- Format constants: SST_MAGIC, SST_FORMAT_VERSION, BLOCK_TYPE_DATA, header sizes
- Compression: LZ4 (lz4_flex) and Zstd with compress/decompress functions
- 17 tests, 0 clippy warnings"
```

---

## Task 2: BlockHeader (16B) + DataBlock Encode/Decode

**Files:**
- Create: `crates/forst-rs-storage/src/sst/block_header.rs`
- Create: `crates/forst-rs-storage/src/sst/data_block.rs`
- Modify: `crates/forst-rs-storage/src/sst/mod.rs`

### BlockHeader Layout (16 bytes)

```
Offset  Field               Size  Type
------  ------------------  ----  --------
0       block_type          1     u8 (0x01 = DataBlock)
1       compression         1     u8 (CompressionType discriminant)
2       reserved            2     u16 (always 0)
4       uncompressed_size   4     u32 LE
8       compressed_size     4     u32 LE
12      checksum            4     u32 LE (masked CRC32C of compressed data)
```

### DataBlock Serialization Flow

```
Encode: RecordBatch → Arrow IPC StreamWriter → raw bytes → compress → BlockHeader + compressed_data
Decode: BlockHeader → compressed_data → verify checksum → decompress → Arrow IPC StreamReader → RecordBatch
```

- [ ] **Step 2.1: Write failing tests for `block_header.rs`**

Create `crates/forst-rs-storage/src/sst/block_header.rs`:

```rust
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

//! 16-byte block header for SST data blocks.

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_common::CompressionType;

    #[test]
    fn test_block_header_encode_size() {
        let header = BlockHeader {
            block_type: BLOCK_TYPE_DATA,
            compression: CompressionType::Lz4,
            uncompressed_size: 1024,
            compressed_size: 512,
            checksum: 0xDEADBEEF,
        };
        let bytes = header.encode();
        assert_eq!(bytes.len(), BLOCK_HEADER_SIZE);
    }

    #[test]
    fn test_block_header_roundtrip() {
        let original = BlockHeader {
            block_type: BLOCK_TYPE_DATA,
            compression: CompressionType::Zstd,
            uncompressed_size: 65536,
            compressed_size: 32000,
            checksum: 0x12345678,
        };
        let bytes = original.encode();
        let decoded = BlockHeader::decode(&bytes).unwrap();
        assert_eq!(decoded.block_type, original.block_type);
        assert_eq!(decoded.compression, original.compression);
        assert_eq!(decoded.uncompressed_size, original.uncompressed_size);
        assert_eq!(decoded.compressed_size, original.compressed_size);
        assert_eq!(decoded.checksum, original.checksum);
    }

    #[test]
    fn test_block_header_decode_too_short() {
        let bytes = vec![0u8; 15];
        assert!(BlockHeader::decode(&bytes).is_err());
    }

    #[test]
    fn test_block_header_decode_invalid_compression() {
        let mut header = BlockHeader {
            block_type: BLOCK_TYPE_DATA,
            compression: CompressionType::None,
            uncompressed_size: 100,
            compressed_size: 100,
            checksum: 0,
        };
        let mut bytes = header.encode();
        bytes[1] = 99; // invalid compression type
        assert!(BlockHeader::decode(&bytes).is_err());
    }

    #[test]
    fn test_block_header_none_compression() {
        let header = BlockHeader {
            block_type: BLOCK_TYPE_DATA,
            compression: CompressionType::None,
            uncompressed_size: 200,
            compressed_size: 200,
            checksum: 0,
        };
        let bytes = header.encode();
        assert_eq!(bytes[1], 0); // CompressionType::None = 0
    }

    #[test]
    fn test_block_header_byte_layout() {
        let header = BlockHeader {
            block_type: 0x01,
            compression: CompressionType::Lz4,
            uncompressed_size: 0x00000100, // 256
            compressed_size: 0x00000080,   // 128
            checksum: 0xAABBCCDD,
        };
        let bytes = header.encode();
        assert_eq!(bytes[0], 0x01);       // block_type
        assert_eq!(bytes[1], 0x01);       // compression = Lz4
        assert_eq!(bytes[2], 0x00);       // reserved
        assert_eq!(bytes[3], 0x00);       // reserved
        // uncompressed_size LE: 0x100 = [0x00, 0x01, 0x00, 0x00]
        assert_eq!(&bytes[4..8], &[0x00, 0x01, 0x00, 0x00]);
        // compressed_size LE: 0x80 = [0x80, 0x00, 0x00, 0x00]
        assert_eq!(&bytes[8..12], &[0x80, 0x00, 0x00, 0x00]);
        // checksum LE: 0xAABBCCDD = [0xDD, 0xCC, 0xBB, 0xAA]
        assert_eq!(&bytes[12..16], &[0xDD, 0xCC, 0xBB, 0xAA]);
    }
}
```

- [ ] **Step 2.2: Run tests to verify they fail**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::block_header 2>&1 | tail -20
```

Expected: compilation error — `BlockHeader` struct not defined.

- [ ] **Step 2.3: Implement `block_header.rs`**

Add the following above the `#[cfg(test)]` block in `crates/forst-rs-storage/src/sst/block_header.rs`:

```rust
use forst_rs_common::{get_fixed32, put_fixed32, CompressionType, ForstError, ForstResult};

use crate::sst::schema::{BLOCK_HEADER_SIZE, BLOCK_TYPE_DATA};

/// A 16-byte header prepended to every compressed data block.
///
/// Layout (all multi-byte fields are little-endian):
/// ```text
/// [0]    block_type        u8
/// [1]    compression       u8
/// [2..4] reserved          u16 (always 0)
/// [4..8] uncompressed_size u32
/// [8..12] compressed_size  u32
/// [12..16] checksum        u32 (masked CRC32C of compressed data)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    /// Block type identifier (e.g., `BLOCK_TYPE_DATA = 0x01`).
    pub block_type: u8,
    /// Compression algorithm used for the block payload.
    pub compression: CompressionType,
    /// Size of the data before compression.
    pub uncompressed_size: u32,
    /// Size of the compressed data (after the header).
    pub compressed_size: u32,
    /// Masked CRC32C checksum of the compressed data.
    pub checksum: u32,
}

impl BlockHeader {
    /// Serializes the header into exactly [`BLOCK_HEADER_SIZE`] bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BLOCK_HEADER_SIZE);
        buf.push(self.block_type);
        buf.push(self.compression as u8);
        buf.push(0); // reserved
        buf.push(0); // reserved
        put_fixed32(&mut buf, self.uncompressed_size);
        put_fixed32(&mut buf, self.compressed_size);
        put_fixed32(&mut buf, self.checksum);
        debug_assert_eq!(buf.len(), BLOCK_HEADER_SIZE);
        buf
    }

    /// Deserializes a header from the first [`BLOCK_HEADER_SIZE`] bytes of `data`.
    pub fn decode(data: &[u8]) -> ForstResult<Self> {
        if data.len() < BLOCK_HEADER_SIZE {
            return Err(ForstError::corruption(format!(
                "block header too short: {} bytes, need {}",
                data.len(),
                BLOCK_HEADER_SIZE
            )));
        }

        let block_type = data[0];
        let compression_byte = data[1];
        let compression = match compression_byte {
            0 => CompressionType::None,
            1 => CompressionType::Lz4,
            2 => CompressionType::Zstd,
            other => {
                return Err(ForstError::corruption(format!(
                    "unknown compression type: {other}"
                )));
            }
        };

        let (uncompressed_size, _) = get_fixed32(&data[4..])?;
        let (compressed_size, _) = get_fixed32(&data[8..])?;
        let (checksum, _) = get_fixed32(&data[12..])?;

        Ok(Self {
            block_type,
            compression,
            uncompressed_size,
            compressed_size,
            checksum,
        })
    }
}
```

- [ ] **Step 2.4: Run block_header tests**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::block_header 2>&1 | tail -20
```

Expected: all 6 block_header tests pass.

- [ ] **Step 2.5: Write failing tests for `data_block.rs`**

Create `crates/forst-rs-storage/src/sst/data_block.rs`:

```rust
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

//! DataBlock encode/decode: Arrow RecordBatch ↔ compressed block bytes.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{BinaryArray, UInt64Array, UInt8Array};
    use arrow::record_batch::RecordBatch;
    use forst_rs_common::CompressionType;
    use crate::sst::schema::sst_schema;

    /// Helper: build a test RecordBatch with the SST schema.
    fn make_test_batch(
        keys: Vec<&[u8]>,
        values: Vec<Option<&[u8]>>,
        sequences: Vec<u64>,
        op_types: Vec<u8>,
    ) -> RecordBatch {
        let key_array = BinaryArray::from(keys);
        let value_array = BinaryArray::from(values);
        let seq_array = UInt64Array::from(sequences);
        let op_array = UInt8Array::from(op_types);
        RecordBatch::try_new(
            Arc::new(sst_schema()),
            vec![
                Arc::new(key_array),
                Arc::new(value_array),
                Arc::new(seq_array),
                Arc::new(op_array),
            ],
        )
        .expect("valid test batch")
    }

    #[test]
    fn test_encode_produces_header_plus_data() {
        let batch = make_test_batch(
            vec![b"key1", b"key2"],
            vec![Some(b"val1" as &[u8]), Some(b"val2")],
            vec![100, 99],
            vec![0, 0], // Put, Put
        );
        let encoded = encode_data_block(&batch, CompressionType::None).unwrap();
        // Must be at least BLOCK_HEADER_SIZE bytes
        assert!(encoded.len() > BLOCK_HEADER_SIZE);
    }

    #[test]
    fn test_roundtrip_no_compression() {
        let batch = make_test_batch(
            vec![b"alpha", b"beta", b"gamma"],
            vec![Some(b"v1" as &[u8]), None, Some(b"v3")],
            vec![300, 200, 100],
            vec![0, 1, 0], // Put, Delete, Put
        );
        let encoded = encode_data_block(&batch, CompressionType::None).unwrap();
        let decoded = decode_data_block(&encoded).unwrap();
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 4);

        // Verify key column
        let keys = decoded.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(keys.value(0), b"alpha");
        assert_eq!(keys.value(1), b"beta");
        assert_eq!(keys.value(2), b"gamma");

        // Verify value column (nullable)
        let values = decoded.column(1).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(values.value(0), b"v1");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), b"v3");

        // Verify sequence column
        let seqs = decoded.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(seqs.value(0), 300);
        assert_eq!(seqs.value(1), 200);
        assert_eq!(seqs.value(2), 100);

        // Verify op_type column
        let ops = decoded.column(3).as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(ops.value(0), 0); // Put
        assert_eq!(ops.value(1), 1); // Delete
        assert_eq!(ops.value(2), 0); // Put
    }

    #[test]
    fn test_roundtrip_lz4() {
        let batch = make_test_batch(
            vec![b"key1", b"key2"],
            vec![Some(b"value1" as &[u8]), Some(b"value2")],
            vec![10, 9],
            vec![0, 0],
        );
        let encoded = encode_data_block(&batch, CompressionType::Lz4).unwrap();
        let decoded = decode_data_block(&encoded).unwrap();
        assert_eq!(decoded.num_rows(), 2);

        let keys = decoded.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(keys.value(0), b"key1");
        assert_eq!(keys.value(1), b"key2");
    }

    #[test]
    fn test_roundtrip_zstd() {
        let batch = make_test_batch(
            vec![b"k1"],
            vec![Some(b"v1" as &[u8])],
            vec![1],
            vec![0],
        );
        let encoded = encode_data_block(&batch, CompressionType::Zstd).unwrap();
        let decoded = decode_data_block(&encoded).unwrap();
        assert_eq!(decoded.num_rows(), 1);
    }

    #[test]
    fn test_decode_corrupt_checksum() {
        let batch = make_test_batch(
            vec![b"key"],
            vec![Some(b"val" as &[u8])],
            vec![1],
            vec![0],
        );
        let mut encoded = encode_data_block(&batch, CompressionType::None).unwrap();
        // Corrupt the checksum (bytes 12..16 in the header)
        encoded[12] ^= 0xFF;
        let result = decode_data_block(&encoded);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_truncated_data() {
        let batch = make_test_batch(
            vec![b"key"],
            vec![Some(b"val" as &[u8])],
            vec![1],
            vec![0],
        );
        let encoded = encode_data_block(&batch, CompressionType::None).unwrap();
        // Truncate: remove last 10 bytes of compressed data
        let truncated = &encoded[..encoded.len() - 10];
        let result = decode_data_block(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_too_short_for_header() {
        let result = decode_data_block(&[0u8; 8]);
        assert!(result.is_err());
    }

    #[test]
    fn test_lz4_compresses_repetitive_data() {
        // Generate a batch with enough repetitive data to show compression benefit
        let keys: Vec<&[u8]> = (0..100).map(|_| b"same_key_repeated".as_slice()).collect();
        let values: Vec<Option<&[u8]>> = (0..100).map(|_| Some(b"same_value_repeated".as_slice())).collect();
        let seqs: Vec<u64> = (0..100).map(|i| 1000 - i).collect();
        let ops: Vec<u8> = vec![0; 100];
        let batch = make_test_batch(keys, values, seqs, ops);

        let none_encoded = encode_data_block(&batch, CompressionType::None).unwrap();
        let lz4_encoded = encode_data_block(&batch, CompressionType::Lz4).unwrap();
        assert!(
            lz4_encoded.len() < none_encoded.len(),
            "LZ4 should compress repetitive data: lz4={} vs none={}",
            lz4_encoded.len(),
            none_encoded.len()
        );
    }
}
```

- [ ] **Step 2.6: Run tests to verify they fail**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::data_block 2>&1 | tail -20
```

Expected: compilation error — `encode_data_block` and `decode_data_block` not defined.

- [ ] **Step 2.7: Implement `data_block.rs`**

Add the following above the `#[cfg(test)]` block in `crates/forst-rs-storage/src/sst/data_block.rs`:

```rust
use std::io::Cursor;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use forst_rs_common::{crc32c, mask_crc, unmask_crc, CompressionType, ForstError, ForstResult};

use crate::sst::block_header::BlockHeader;
use crate::sst::compression::{compress, decompress};
use crate::sst::schema::{BLOCK_HEADER_SIZE, BLOCK_TYPE_DATA};

/// Encodes an Arrow RecordBatch into a data block: `BlockHeader (16B) + compressed_data`.
///
/// Serialization flow:
/// 1. Serialize RecordBatch to Arrow IPC stream bytes (includes schema for self-describing blocks)
/// 2. Compress the IPC bytes using the specified algorithm
/// 3. Compute masked CRC32C checksum of the compressed bytes
/// 4. Prepend a 16-byte BlockHeader
pub fn encode_data_block(
    batch: &RecordBatch,
    compression: CompressionType,
) -> ForstResult<Vec<u8>> {
    // Step 1: Serialize to Arrow IPC bytes
    let mut ipc_buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buf, &batch.schema())
            .map_err(|e| ForstError::corruption(format!("Arrow IPC write init failed: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| ForstError::corruption(format!("Arrow IPC write failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| ForstError::corruption(format!("Arrow IPC finish failed: {e}")))?;
    }
    let uncompressed_size = ipc_buf.len() as u32;

    // Step 2: Compress
    let compressed = compress(&ipc_buf, compression)?;
    let compressed_size = compressed.len() as u32;

    // Step 3: Compute checksum
    let checksum = mask_crc(crc32c(&compressed));

    // Step 4: Build header + data
    let header = BlockHeader {
        block_type: BLOCK_TYPE_DATA,
        compression,
        uncompressed_size,
        compressed_size,
        checksum,
    };

    let mut result = Vec::with_capacity(BLOCK_HEADER_SIZE + compressed.len());
    result.extend_from_slice(&header.encode());
    result.extend_from_slice(&compressed);
    Ok(result)
}

/// Decodes a data block (`BlockHeader + compressed_data`) back into an Arrow RecordBatch.
///
/// Deserialization flow:
/// 1. Parse the 16-byte BlockHeader
/// 2. Extract compressed data, verify length matches header
/// 3. Verify CRC32C checksum
/// 4. Decompress to Arrow IPC bytes
/// 5. Deserialize Arrow IPC stream to RecordBatch
pub fn decode_data_block(data: &[u8]) -> ForstResult<RecordBatch> {
    // Step 1: Parse header
    if data.len() < BLOCK_HEADER_SIZE {
        return Err(ForstError::corruption(format!(
            "data block too short for header: {} bytes",
            data.len()
        )));
    }
    let header = BlockHeader::decode(data)?;

    // Step 2: Extract compressed data
    let compressed_data = &data[BLOCK_HEADER_SIZE..];
    if compressed_data.len() < header.compressed_size as usize {
        return Err(ForstError::corruption(format!(
            "data block truncated: have {} compressed bytes, header says {}",
            compressed_data.len(),
            header.compressed_size
        )));
    }
    let compressed_data = &compressed_data[..header.compressed_size as usize];

    // Step 3: Verify checksum
    let actual_checksum = mask_crc(crc32c(compressed_data));
    if actual_checksum != header.checksum {
        return Err(ForstError::corruption(format!(
            "data block checksum mismatch: stored={:#010x}, computed={:#010x}",
            header.checksum, actual_checksum
        )));
    }

    // Step 4: Decompress
    let ipc_bytes = decompress(
        compressed_data,
        header.compression,
        header.uncompressed_size as usize,
    )?;

    // Step 5: Deserialize Arrow IPC stream
    let cursor = Cursor::new(&ipc_bytes);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| ForstError::corruption(format!("Arrow IPC read init failed: {e}")))?;

    reader
        .next()
        .ok_or_else(|| ForstError::corruption("Arrow IPC stream contains no record batch"))?
        .map_err(|e| ForstError::corruption(format!("Arrow IPC read batch failed: {e}")))
}
```

- [ ] **Step 2.8: Update `sst/mod.rs` to include new modules**

Replace the contents of `crates/forst-rs-storage/src/sst/mod.rs`:

```rust
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

//! SST file format components.

pub mod block_header;
pub mod compression;
pub mod data_block;
pub mod schema;

pub use block_header::BlockHeader;
pub use compression::{compress, decompress};
pub use data_block::{decode_data_block, encode_data_block};
pub use schema::{
    sst_schema, BLOCK_HEADER_SIZE, BLOCK_TYPE_DATA, FILE_HEADER_SIZE, SST_FORMAT_VERSION,
    SST_MAGIC,
};
```

- [ ] **Step 2.9: Run all data_block tests**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::data_block 2>&1 | tail -25
```

Expected: all 8 data_block tests pass.

- [ ] **Step 2.10: Run full crate tests + clippy**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage 2>&1 | tail -20
cd ~/code/github/ForSt && cargo clippy -p forst-rs-storage -- -D warnings 2>&1 | tail -10
```

Expected: 31 tests pass (17 + 6 + 8), 0 clippy warnings.

- [ ] **Step 2.11: Commit**

```bash
cd ~/code/github/ForSt
git add crates/forst-rs-storage/src/sst/block_header.rs \
        crates/forst-rs-storage/src/sst/data_block.rs \
        crates/forst-rs-storage/src/sst/mod.rs
git commit -m "feat: add BlockHeader and DataBlock encode/decode with Arrow IPC

BlockHeader: 16-byte header with block_type, compression, sizes, CRC32C checksum.
DataBlock encode: RecordBatch -> Arrow IPC -> compress -> BlockHeader + data.
DataBlock decode: parse header -> verify checksum -> decompress -> Arrow IPC -> RecordBatch.
Supports None, LZ4, and Zstd compression with integrity verification.
14 new tests (6 block_header + 8 data_block)."
```

---

## Task 3: File Header (16B) + Footer V1 Serialize/Deserialize

**Files:**
- Create: `crates/forst-rs-storage/src/sst/file_header.rs`
- Create: `crates/forst-rs-storage/src/sst/footer.rs`
- Modify: `crates/forst-rs-storage/src/sst/mod.rs`

### File Header Layout (16 bytes)

```
Offset  Field           Size  Type
------  -------------   ----  --------
0       magic           4     [u8; 4] = b"FRST"
4       format_version  2     u16 LE
6       flags           2     u16 LE (reserved, currently 0)
8       reserved        8     [u8; 8] (zeros)
```

### Footer V1 Layout (variable length: 88 + key data)

```
Offset  Field               Size  Type
------  ------------------  ----  --------
0       data_block_count    4     u32 LE
4       total_entries       8     u64 LE
12      bloom_filter_offset 8     u64 LE
20      bloom_filter_size   4     u32 LE
24      index_offset        8     u64 LE
32      index_size          4     u32 LE
36      min_key_offset      4     u32 LE  (always = 76)
40      min_key_len         2     u16 LE
42      max_key_offset      4     u32 LE  (always = 76 + min_key_len)
46      max_key_len         2     u16 LE
48      min_sequence        8     u64 LE
56      max_sequence        8     u64 LE
64      compression         1     u8
65      checksum_type       1     u8
66      creation_time       8     u64 LE
74      format_version      2     u16 LE
--- fixed fields end (76 bytes) ---
76      min_key_data        var   [u8; min_key_len]
76+mkl  max_key_data        var   [u8; max_key_len]
...     footer_checksum     4     u32 LE (masked CRC32C of all bytes before this)
...     footer_length       4     u32 LE (total footer size including this + magic)
...     magic               4     b"FRST"
```

Read flow: read last 8 bytes of file → `footer_length(u32) + magic("FRST")` → seek to footer start → read `footer_length` bytes → parse.

- [ ] **Step 3.1: Write failing tests for `file_header.rs`**

Create `crates/forst-rs-storage/src/sst/file_header.rs`:

```rust
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

//! 16-byte SST file header.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::schema::{FILE_HEADER_SIZE, SST_FORMAT_VERSION, SST_MAGIC};

    #[test]
    fn test_file_header_encode_size() {
        let header = FileHeader::new(SST_FORMAT_VERSION, 0);
        let bytes = header.encode();
        assert_eq!(bytes.len(), FILE_HEADER_SIZE);
    }

    #[test]
    fn test_file_header_starts_with_magic() {
        let header = FileHeader::new(SST_FORMAT_VERSION, 0);
        let bytes = header.encode();
        assert_eq!(&bytes[0..4], SST_MAGIC);
    }

    #[test]
    fn test_file_header_roundtrip() {
        let original = FileHeader::new(1, 0x0042);
        let bytes = original.encode();
        let decoded = FileHeader::decode(&bytes).unwrap();
        assert_eq!(decoded.format_version, 1);
        assert_eq!(decoded.flags, 0x0042);
    }

    #[test]
    fn test_file_header_decode_bad_magic() {
        let mut bytes = FileHeader::new(1, 0).encode();
        bytes[0] = b'X'; // corrupt magic
        assert!(FileHeader::decode(&bytes).is_err());
    }

    #[test]
    fn test_file_header_decode_too_short() {
        let bytes = vec![0u8; 10];
        assert!(FileHeader::decode(&bytes).is_err());
    }

    #[test]
    fn test_file_header_byte_layout() {
        let header = FileHeader::new(1, 0);
        let bytes = header.encode();
        // magic "FRST"
        assert_eq!(&bytes[0..4], b"FRST");
        // format_version = 1 LE
        assert_eq!(&bytes[4..6], &[0x01, 0x00]);
        // flags = 0 LE
        assert_eq!(&bytes[6..8], &[0x00, 0x00]);
        // reserved = 8 zero bytes
        assert_eq!(&bytes[8..16], &[0u8; 8]);
    }

    #[test]
    fn test_file_header_default() {
        let header = FileHeader::default();
        assert_eq!(header.format_version, SST_FORMAT_VERSION);
        assert_eq!(header.flags, 0);
    }
}
```

- [ ] **Step 3.2: Run tests to verify they fail**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::file_header 2>&1 | tail -20
```

Expected: compilation error — `FileHeader` not defined.

- [ ] **Step 3.3: Implement `file_header.rs`**

Add the following above the `#[cfg(test)]` block in `crates/forst-rs-storage/src/sst/file_header.rs`:

```rust
use forst_rs_common::{ForstError, ForstResult};

use crate::sst::schema::{FILE_HEADER_SIZE, SST_FORMAT_VERSION, SST_MAGIC};

/// The 16-byte header at the start of every SST file.
///
/// Layout:
/// ```text
/// [0..4]  magic           b"FRST"
/// [4..6]  format_version  u16 LE
/// [6..8]  flags           u16 LE
/// [8..16] reserved        [u8; 8] (zeros)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    /// SST format version (currently 1).
    pub format_version: u16,
    /// Feature flags (reserved for future use).
    pub flags: u16,
}

impl FileHeader {
    /// Creates a new file header with the given version and flags.
    pub fn new(format_version: u16, flags: u16) -> Self {
        Self {
            format_version,
            flags,
        }
    }

    /// Serializes the header into exactly [`FILE_HEADER_SIZE`] bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(FILE_HEADER_SIZE);
        buf.extend_from_slice(SST_MAGIC);
        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.extend_from_slice(&self.flags.to_le_bytes());
        buf.extend_from_slice(&[0u8; 8]); // reserved
        debug_assert_eq!(buf.len(), FILE_HEADER_SIZE);
        buf
    }

    /// Deserializes a file header from the first [`FILE_HEADER_SIZE`] bytes of `data`.
    pub fn decode(data: &[u8]) -> ForstResult<Self> {
        if data.len() < FILE_HEADER_SIZE {
            return Err(ForstError::corruption(format!(
                "file header too short: {} bytes, need {}",
                data.len(),
                FILE_HEADER_SIZE
            )));
        }

        // Verify magic
        if &data[0..4] != SST_MAGIC {
            return Err(ForstError::corruption(format!(
                "invalid SST magic: expected {:?}, got {:?}",
                SST_MAGIC,
                &data[0..4]
            )));
        }

        let format_version = u16::from_le_bytes([data[4], data[5]]);
        let flags = u16::from_le_bytes([data[6], data[7]]);

        Ok(Self {
            format_version,
            flags,
        })
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        Self::new(SST_FORMAT_VERSION, 0)
    }
}
```

- [ ] **Step 3.4: Run file_header tests**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::file_header 2>&1 | tail -20
```

Expected: all 7 file_header tests pass.

- [ ] **Step 3.5: Write failing tests for `footer.rs`**

Create `crates/forst-rs-storage/src/sst/footer.rs`:

```rust
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

//! SST Footer V1: metadata section at the end of every SST file.

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_common::CompressionType;

    fn make_test_footer() -> FooterV1 {
        FooterV1 {
            data_block_count: 10,
            total_entries: 5000,
            bloom_filter_offset: 65536,
            bloom_filter_size: 4096,
            index_offset: 69632,
            index_size: 2048,
            min_key: b"aaa".to_vec(),
            max_key: b"zzz".to_vec(),
            min_sequence: 1,
            max_sequence: 5000,
            compression: CompressionType::Lz4,
            checksum_type: ChecksumType::Crc32c,
            creation_time: 1713657600,
            format_version: 1,
        }
    }

    #[test]
    fn test_checksum_type_values() {
        assert_eq!(ChecksumType::Crc32c as u8, 0);
        assert_eq!(ChecksumType::XxHash32 as u8, 1);
    }

    #[test]
    fn test_checksum_type_from_u8() {
        assert_eq!(ChecksumType::from_u8(0), Some(ChecksumType::Crc32c));
        assert_eq!(ChecksumType::from_u8(1), Some(ChecksumType::XxHash32));
        assert_eq!(ChecksumType::from_u8(2), None);
    }

    #[test]
    fn test_footer_encode_ends_with_magic() {
        let footer = make_test_footer();
        let bytes = footer.encode();
        assert_eq!(&bytes[bytes.len() - 4..], b"FRST");
    }

    #[test]
    fn test_footer_encode_length_field() {
        let footer = make_test_footer();
        let bytes = footer.encode();
        // footer_length is stored at bytes[len-8..len-4]
        let footer_length =
            u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
        assert_eq!(footer_length as usize, bytes.len());
    }

    #[test]
    fn test_footer_roundtrip() {
        let original = make_test_footer();
        let bytes = original.encode();
        let decoded = FooterV1::decode(&bytes).unwrap();
        assert_eq!(decoded.data_block_count, 10);
        assert_eq!(decoded.total_entries, 5000);
        assert_eq!(decoded.bloom_filter_offset, 65536);
        assert_eq!(decoded.bloom_filter_size, 4096);
        assert_eq!(decoded.index_offset, 69632);
        assert_eq!(decoded.index_size, 2048);
        assert_eq!(decoded.min_key, b"aaa");
        assert_eq!(decoded.max_key, b"zzz");
        assert_eq!(decoded.min_sequence, 1);
        assert_eq!(decoded.max_sequence, 5000);
        assert_eq!(decoded.compression, CompressionType::Lz4);
        assert_eq!(decoded.checksum_type, ChecksumType::Crc32c);
        assert_eq!(decoded.creation_time, 1713657600);
        assert_eq!(decoded.format_version, 1);
    }

    #[test]
    fn test_footer_roundtrip_empty_keys() {
        let footer = FooterV1 {
            data_block_count: 0,
            total_entries: 0,
            bloom_filter_offset: 0,
            bloom_filter_size: 0,
            index_offset: 0,
            index_size: 0,
            min_key: vec![],
            max_key: vec![],
            min_sequence: 0,
            max_sequence: 0,
            compression: CompressionType::None,
            checksum_type: ChecksumType::Crc32c,
            creation_time: 0,
            format_version: 1,
        };
        let bytes = footer.encode();
        let decoded = FooterV1::decode(&bytes).unwrap();
        assert!(decoded.min_key.is_empty());
        assert!(decoded.max_key.is_empty());
        assert_eq!(decoded.data_block_count, 0);
    }

    #[test]
    fn test_footer_roundtrip_long_keys() {
        let footer = FooterV1 {
            data_block_count: 1,
            total_entries: 1,
            bloom_filter_offset: 0,
            bloom_filter_size: 0,
            index_offset: 0,
            index_size: 0,
            min_key: vec![0xAA; 1000],
            max_key: vec![0xFF; 2000],
            min_sequence: 42,
            max_sequence: 42,
            compression: CompressionType::Zstd,
            checksum_type: ChecksumType::Crc32c,
            creation_time: 1713657600,
            format_version: 1,
        };
        let bytes = footer.encode();
        let decoded = FooterV1::decode(&bytes).unwrap();
        assert_eq!(decoded.min_key.len(), 1000);
        assert_eq!(decoded.max_key.len(), 2000);
        assert_eq!(decoded.min_key, vec![0xAA; 1000]);
        assert_eq!(decoded.max_key, vec![0xFF; 2000]);
    }

    #[test]
    fn test_footer_decode_bad_magic() {
        let footer = make_test_footer();
        let mut bytes = footer.encode();
        let len = bytes.len();
        bytes[len - 1] = b'X'; // corrupt magic
        assert!(FooterV1::decode(&bytes).is_err());
    }

    #[test]
    fn test_footer_decode_bad_checksum() {
        let footer = make_test_footer();
        let mut bytes = footer.encode();
        let len = bytes.len();
        // Corrupt the footer_checksum field (at len - 12)
        bytes[len - 12] ^= 0xFF;
        assert!(FooterV1::decode(&bytes).is_err());
    }

    #[test]
    fn test_footer_decode_too_short() {
        let bytes = vec![0u8; 20];
        assert!(FooterV1::decode(&bytes).is_err());
    }

    #[test]
    fn test_footer_decode_bad_length() {
        let footer = make_test_footer();
        let mut bytes = footer.encode();
        let len = bytes.len();
        // Corrupt the footer_length field (at len - 8) to a wrong value
        bytes[len - 8] = 0xFF;
        bytes[len - 7] = 0xFF;
        assert!(FooterV1::decode(&bytes).is_err());
    }

    #[test]
    fn test_footer_fixed_fields_size() {
        assert_eq!(FOOTER_FIXED_FIELDS_SIZE, 76);
    }

    #[test]
    fn test_footer_tail_size() {
        assert_eq!(FOOTER_TAIL_SIZE, 12);
    }

    #[test]
    fn test_footer_min_size() {
        // Minimum footer: 76 fixed + 0 key bytes + 12 tail = 88
        assert_eq!(FOOTER_FIXED_FIELDS_SIZE + FOOTER_TAIL_SIZE, 88);
    }

    #[test]
    fn test_footer_size_matches_expected() {
        let footer = make_test_footer();
        let bytes = footer.encode();
        // Expected: 76 + 3 (min_key "aaa") + 3 (max_key "zzz") + 12 (tail) = 94
        assert_eq!(bytes.len(), 76 + 3 + 3 + 12);
    }

    #[test]
    fn test_extract_footer_from_file_tail() {
        // Simulate reading the last 8 bytes of a file to get footer_length + magic
        let footer = make_test_footer();
        let bytes = footer.encode();
        let tail = &bytes[bytes.len() - 8..];
        let footer_length = u32::from_le_bytes(tail[0..4].try_into().unwrap());
        let magic = &tail[4..8];
        assert_eq!(magic, b"FRST");
        assert_eq!(footer_length as usize, bytes.len());
    }
}
```

- [ ] **Step 3.6: Run tests to verify they fail**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::footer 2>&1 | tail -20
```

Expected: compilation error — `FooterV1`, `ChecksumType`, etc. not defined.

- [ ] **Step 3.7: Implement `footer.rs`**

Add the following above the `#[cfg(test)]` block in `crates/forst-rs-storage/src/sst/footer.rs`:

```rust
use forst_rs_common::{
    crc32c, get_fixed32, get_fixed64, mask_crc, put_fixed32, put_fixed64, CompressionType,
    ForstError, ForstResult,
};

use crate::sst::schema::SST_MAGIC;

/// Size of the fixed (non-variable) fields section of the footer.
pub const FOOTER_FIXED_FIELDS_SIZE: usize = 76;

/// Size of the footer tail: checksum (4) + length (4) + magic (4).
pub const FOOTER_TAIL_SIZE: usize = 12;

// ---------------------------------------------------------------------------
// ChecksumType
// ---------------------------------------------------------------------------

/// Checksum algorithm used for data integrity verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChecksumType {
    /// CRC32C (Castagnoli) — the default, hardware-accelerated on x86_64.
    Crc32c = 0,
    /// xxHash32 — reserved for future use.
    XxHash32 = 1,
}

impl ChecksumType {
    /// Try to convert a raw `u8` into a [`ChecksumType`].
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(ChecksumType::Crc32c),
            1 => Some(ChecksumType::XxHash32),
            _ => None,
        }
    }
}

impl Default for ChecksumType {
    fn default() -> Self {
        ChecksumType::Crc32c
    }
}

// ---------------------------------------------------------------------------
// FooterV1
// ---------------------------------------------------------------------------

/// The metadata footer at the end of every SST file.
///
/// Contains offsets to the bloom filter and index sections, key range
/// statistics, and a self-describing checksum for integrity verification.
///
/// See the module-level documentation for the binary layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FooterV1 {
    pub data_block_count: u32,
    pub total_entries: u64,
    pub bloom_filter_offset: u64,
    pub bloom_filter_size: u32,
    pub index_offset: u64,
    pub index_size: u32,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub min_sequence: u64,
    pub max_sequence: u64,
    pub compression: CompressionType,
    pub checksum_type: ChecksumType,
    pub creation_time: u64,
    pub format_version: u16,
}

impl FooterV1 {
    /// Serializes the footer into bytes.
    ///
    /// The output ends with `footer_length(u32) + magic("FRST")` so that a
    /// reader can discover the footer by reading the last 8 bytes of the file.
    pub fn encode(&self) -> Vec<u8> {
        let min_key_len = self.min_key.len();
        let max_key_len = self.max_key.len();
        let total_size = FOOTER_FIXED_FIELDS_SIZE + min_key_len + max_key_len + FOOTER_TAIL_SIZE;
        let mut buf = Vec::with_capacity(total_size);

        // Fixed fields (76 bytes)
        put_fixed32(&mut buf, self.data_block_count);
        put_fixed64(&mut buf, self.total_entries);
        put_fixed64(&mut buf, self.bloom_filter_offset);
        put_fixed32(&mut buf, self.bloom_filter_size);
        put_fixed64(&mut buf, self.index_offset);
        put_fixed32(&mut buf, self.index_size);

        let min_key_offset = FOOTER_FIXED_FIELDS_SIZE as u32;
        let max_key_offset = min_key_offset + min_key_len as u32;
        put_fixed32(&mut buf, min_key_offset);
        buf.extend_from_slice(&(min_key_len as u16).to_le_bytes());
        put_fixed32(&mut buf, max_key_offset);
        buf.extend_from_slice(&(max_key_len as u16).to_le_bytes());

        put_fixed64(&mut buf, self.min_sequence);
        put_fixed64(&mut buf, self.max_sequence);
        buf.push(self.compression as u8);
        buf.push(self.checksum_type as u8);
        put_fixed64(&mut buf, self.creation_time);
        buf.extend_from_slice(&self.format_version.to_le_bytes());

        debug_assert_eq!(buf.len(), FOOTER_FIXED_FIELDS_SIZE);

        // Variable-length key data
        buf.extend_from_slice(&self.min_key);
        buf.extend_from_slice(&self.max_key);

        // Tail: checksum + length + magic
        let checksum = mask_crc(crc32c(&buf));
        put_fixed32(&mut buf, checksum);
        let footer_length = (buf.len() + 4 + 4) as u32; // +4 for length itself, +4 for magic
        put_fixed32(&mut buf, footer_length);
        buf.extend_from_slice(SST_MAGIC);

        debug_assert_eq!(buf.len(), total_size);
        buf
    }

    /// Deserializes a footer from its raw bytes.
    ///
    /// The caller is expected to pass exactly `footer_length` bytes, which they
    /// obtained by reading the last 8 bytes of the file (`footer_length` + magic).
    pub fn decode(data: &[u8]) -> ForstResult<Self> {
        let min_size = FOOTER_FIXED_FIELDS_SIZE + FOOTER_TAIL_SIZE;
        if data.len() < min_size {
            return Err(ForstError::corruption(format!(
                "footer too short: {} bytes, minimum {}",
                data.len(),
                min_size
            )));
        }

        let len = data.len();

        // Verify magic at end
        if &data[len - 4..] != SST_MAGIC {
            return Err(ForstError::corruption(format!(
                "invalid footer magic: expected {:?}, got {:?}",
                SST_MAGIC,
                &data[len - 4..]
            )));
        }

        // Verify footer_length matches data length
        let footer_length =
            u32::from_le_bytes(data[len - 8..len - 4].try_into().unwrap());
        if footer_length as usize != len {
            return Err(ForstError::corruption(format!(
                "footer length mismatch: field says {}, actual {}",
                footer_length, len
            )));
        }

        // Verify checksum (covers everything before the checksum field)
        let checksum_offset = len - FOOTER_TAIL_SIZE;
        let (stored_checksum, _) = get_fixed32(&data[checksum_offset..])?;
        let computed_checksum = mask_crc(crc32c(&data[..checksum_offset]));
        if stored_checksum != computed_checksum {
            return Err(ForstError::corruption(format!(
                "footer checksum mismatch: stored={:#010x}, computed={:#010x}",
                stored_checksum, computed_checksum
            )));
        }

        // Parse fixed fields
        let (data_block_count, _) = get_fixed32(&data[0..])?;
        let (total_entries, _) = get_fixed64(&data[4..])?;
        let (bloom_filter_offset, _) = get_fixed64(&data[12..])?;
        let (bloom_filter_size, _) = get_fixed32(&data[20..])?;
        let (index_offset, _) = get_fixed64(&data[24..])?;
        let (index_size, _) = get_fixed32(&data[32..])?;

        let (min_key_offset, _) = get_fixed32(&data[36..])?;
        let min_key_len = u16::from_le_bytes([data[40], data[41]]) as usize;
        let (max_key_offset, _) = get_fixed32(&data[42..])?;
        let max_key_len = u16::from_le_bytes([data[46], data[47]]) as usize;

        let (min_sequence, _) = get_fixed64(&data[48..])?;
        let (max_sequence, _) = get_fixed64(&data[56..])?;

        let compression_byte = data[64];
        let compression = match compression_byte {
            0 => CompressionType::None,
            1 => CompressionType::Lz4,
            2 => CompressionType::Zstd,
            other => {
                return Err(ForstError::corruption(format!(
                    "unknown compression type in footer: {other}"
                )));
            }
        };

        let checksum_type_byte = data[65];
        let checksum_type = ChecksumType::from_u8(checksum_type_byte).ok_or_else(|| {
            ForstError::corruption(format!(
                "unknown checksum type in footer: {checksum_type_byte}"
            ))
        })?;

        let (creation_time, _) = get_fixed64(&data[66..])?;
        let format_version = u16::from_le_bytes([data[74], data[75]]);

        // Extract variable-length key data
        let min_key_start = min_key_offset as usize;
        let min_key_end = min_key_start + min_key_len;
        let max_key_start = max_key_offset as usize;
        let max_key_end = max_key_start + max_key_len;

        if min_key_end > checksum_offset || max_key_end > checksum_offset {
            return Err(ForstError::corruption(
                "footer key data extends beyond checksum boundary",
            ));
        }

        let min_key = data[min_key_start..min_key_end].to_vec();
        let max_key = data[max_key_start..max_key_end].to_vec();

        Ok(Self {
            data_block_count,
            total_entries,
            bloom_filter_offset,
            bloom_filter_size,
            index_offset,
            index_size,
            min_key,
            max_key,
            min_sequence,
            max_sequence,
            compression,
            checksum_type,
            creation_time,
            format_version,
        })
    }
}
```

- [ ] **Step 3.8: Run footer tests**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage sst::footer 2>&1 | tail -25
```

Expected: all 16 footer tests pass.

- [ ] **Step 3.9: Update `sst/mod.rs` with new modules**

Replace `crates/forst-rs-storage/src/sst/mod.rs`:

```rust
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

//! SST file format components.

pub mod block_header;
pub mod compression;
pub mod data_block;
pub mod file_header;
pub mod footer;
pub mod schema;

pub use block_header::BlockHeader;
pub use compression::{compress, decompress};
pub use data_block::{decode_data_block, encode_data_block};
pub use file_header::FileHeader;
pub use footer::{ChecksumType, FooterV1, FOOTER_FIXED_FIELDS_SIZE, FOOTER_TAIL_SIZE};
pub use schema::{
    sst_schema, BLOCK_HEADER_SIZE, BLOCK_TYPE_DATA, FILE_HEADER_SIZE, SST_FORMAT_VERSION,
    SST_MAGIC,
};
```

- [ ] **Step 3.10: Run all storage crate tests + clippy**

```bash
cd ~/code/github/ForSt && cargo test -p forst-rs-storage 2>&1 | tail -25
cd ~/code/github/ForSt && cargo clippy -p forst-rs-storage -- -D warnings 2>&1 | tail -10
```

Expected: 54 tests pass (17 + 6 + 8 + 7 + 16), 0 clippy warnings.

- [ ] **Step 3.11: Run full workspace tests to verify no regressions**

```bash
cd ~/code/github/ForSt && cargo test --workspace 2>&1 | tail -20
cd ~/code/github/ForSt && cargo clippy --workspace -- -D warnings 2>&1 | tail -10
```

Expected: all workspace tests pass (357 existing + 54 new = 411), 0 clippy warnings.

- [ ] **Step 3.12: Commit**

```bash
cd ~/code/github/ForSt
git add crates/forst-rs-storage/src/sst/file_header.rs \
        crates/forst-rs-storage/src/sst/footer.rs \
        crates/forst-rs-storage/src/sst/mod.rs
git commit -m "feat: add SST file header and footer V1 serialization

FileHeader: 16-byte header with magic, format_version, flags.
FooterV1: variable-length footer with section offsets, key range stats,
CRC32C integrity checksum, and self-describing length field.
ChecksumType enum: Crc32c (default) and XxHash32 (reserved).
Footer read protocol: last 8 bytes → footer_length + magic → full parse.
23 new tests (7 file_header + 16 footer)."
```

---

## Summary

| Task | Files | Tests | Commit |
|------|-------|-------|--------|
| 1 | schema.rs, compression.rs, lib.rs, mod.rs, 2x Cargo.toml | 17 | `feat: add forst-rs-storage crate with SST schema and compression utilities` |
| 2 | block_header.rs, data_block.rs, mod.rs | 14 | `feat: add BlockHeader and DataBlock encode/decode with Arrow IPC` |
| 3 | file_header.rs, footer.rs, mod.rs | 23 | `feat: add SST file header and footer V1 serialization` |
| **Total** | **9 files** | **54** | **3 commits** |

### Post-W5 State
- New crate: `forst-rs-storage` with `sst` module
- Workspace test count: 357 + 54 = **411**
- Ready for W6: SstWriter (uses DataBlock encode + FileHeader + FooterV1)
