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

//! Block header encoding and decoding.
//!
//! Every data block in an SST file is preceded by a 16-byte header that
//! describes the block type, compression method, original and compressed
//! sizes, and a masked CRC32C checksum of the compressed payload.
//!
//! ```text
//! Offset  Field               Size  Encoding
//! 0       block_type          1     u8 (0x01 = DataBlock)
//! 1       compression         1     u8 (CompressionType discriminant)
//! 2       reserved            2     u16 LE (always 0)
//! 4       uncompressed_size   4     u32 LE
//! 8       compressed_size     4     u32 LE
//! 12      checksum            4     u32 LE (masked CRC32C of compressed data)
//! ```

use forst_rs_common::{get_fixed32, put_fixed32, CompressionType, ForstError, ForstResult};

use super::schema::BLOCK_HEADER_SIZE;

/// A 16-byte header that precedes every block in an SST file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHeader {
    /// Discriminant identifying the block type (e.g., [`super::schema::BLOCK_TYPE_DATA`]).
    pub block_type: u8,
    /// Compression algorithm used for the block payload.
    pub compression: CompressionType,
    /// Size of the block payload *before* compression, in bytes.
    pub uncompressed_size: u32,
    /// Size of the block payload *after* compression, in bytes.
    pub compressed_size: u32,
    /// Masked CRC32C checksum of the compressed payload.
    pub checksum: u32,
}

impl BlockHeader {
    /// Serializes this header into exactly [`BLOCK_HEADER_SIZE`] bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BLOCK_HEADER_SIZE);
        buf.push(self.block_type);
        buf.push(self.compression as u8);
        // reserved: 2 bytes, always 0
        buf.push(0);
        buf.push(0);
        put_fixed32(&mut buf, self.uncompressed_size);
        put_fixed32(&mut buf, self.compressed_size);
        put_fixed32(&mut buf, self.checksum);
        buf
    }

    /// Deserializes a [`BlockHeader`] from the first [`BLOCK_HEADER_SIZE`] bytes
    /// of `data`.
    ///
    /// Returns [`ForstError::Corruption`] if the slice is too short or the
    /// compression byte is not a valid [`CompressionType`] discriminant.
    pub fn decode(data: &[u8]) -> ForstResult<Self> {
        if data.len() < BLOCK_HEADER_SIZE {
            return Err(ForstError::corruption(format!(
                "block header too short: expected {} bytes, got {}",
                BLOCK_HEADER_SIZE,
                data.len()
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
                    "invalid compression type byte: {other}"
                )));
            }
        };
        // data[2..4] is reserved, skip

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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::schema::BLOCK_TYPE_DATA;

    fn sample_header() -> BlockHeader {
        BlockHeader {
            block_type: BLOCK_TYPE_DATA,
            compression: CompressionType::Lz4,
            uncompressed_size: 1024,
            compressed_size: 512,
            checksum: 0xDEAD_BEEF,
        }
    }

    #[test]
    fn test_encode_size_is_16() {
        let encoded = sample_header().encode();
        assert_eq!(encoded.len(), BLOCK_HEADER_SIZE);
    }

    #[test]
    fn test_roundtrip() {
        let header = sample_header();
        let encoded = header.encode();
        let decoded = BlockHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_decode_too_short() {
        let short = vec![0u8; BLOCK_HEADER_SIZE - 1];
        let result = BlockHeader::decode(&short);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("too short"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_decode_invalid_compression() {
        let mut encoded = sample_header().encode();
        // Set compression byte to an invalid value.
        encoded[1] = 99;
        let result = BlockHeader::decode(&encoded);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("invalid compression type byte"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_none_compression_byte() {
        let header = BlockHeader {
            compression: CompressionType::None,
            ..sample_header()
        };
        let encoded = header.encode();
        assert_eq!(encoded[1], 0, "CompressionType::None should encode as 0");
        let decoded = BlockHeader::decode(&encoded).unwrap();
        assert_eq!(decoded.compression, CompressionType::None);
    }

    #[test]
    fn test_exact_byte_layout() {
        let header = BlockHeader {
            block_type: BLOCK_TYPE_DATA,
            compression: CompressionType::Zstd,
            uncompressed_size: 0x0000_0100, // 256
            compressed_size: 0x0000_0080,   // 128
            checksum: 0xAABB_CCDD,
        };
        let encoded = header.encode();

        // byte 0: block_type = 0x01
        assert_eq!(encoded[0], 0x01);
        // byte 1: compression = Zstd = 2
        assert_eq!(encoded[1], 0x02);
        // bytes 2-3: reserved = 0
        assert_eq!(encoded[2], 0x00);
        assert_eq!(encoded[3], 0x00);
        // bytes 4-7: uncompressed_size = 256 (LE)
        assert_eq!(&encoded[4..8], &[0x00, 0x01, 0x00, 0x00]);
        // bytes 8-11: compressed_size = 128 (LE)
        assert_eq!(&encoded[8..12], &[0x80, 0x00, 0x00, 0x00]);
        // bytes 12-15: checksum = 0xAABBCCDD (LE)
        assert_eq!(&encoded[12..16], &[0xDD, 0xCC, 0xBB, 0xAA]);
    }
}
