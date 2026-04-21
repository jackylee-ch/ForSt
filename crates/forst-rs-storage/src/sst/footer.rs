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

//! SST Footer V1 encoding and decoding.
//!
//! The footer lives at the end of every SST file and is self-describing:
//! the last 8 bytes contain `footer_length` (u32 LE) and the magic `b"FRST"`,
//! which allows a reader to locate and parse the entire footer.
//!
//! ## Binary layout
//!
//! ```text
//! Offset  Field               Size  Encoding
//! 0       data_block_count    4     u32 LE
//! 4       total_entries       8     u64 LE
//! 12      bloom_filter_offset 8     u64 LE
//! 20      bloom_filter_size   4     u32 LE
//! 24      index_offset        8     u64 LE
//! 32      index_size          4     u32 LE
//! 36      min_key_offset      4     u32 LE  (always = 76)
//! 40      min_key_len         2     u16 LE
//! 42      max_key_offset      4     u32 LE  (always = 76 + min_key_len)
//! 46      max_key_len         2     u16 LE
//! 48      min_sequence        8     u64 LE
//! 56      max_sequence        8     u64 LE
//! 64      compression         1     u8
//! 65      checksum_type       1     u8
//! 66      creation_time       8     u64 LE
//! 74      format_version      2     u16 LE
//! --- 76 bytes fixed fields ---
//! 76      [min_key bytes]     variable
//! 76+mkl  [max_key bytes]     variable
//!         footer_checksum     4     u32 LE (masked CRC32C of everything before)
//!         footer_length       4     u32 LE (total footer size incl. length + magic)
//!         magic               4     b"FRST"
//! ```

use forst_rs_common::{
    crc32c, get_fixed32, get_fixed64, mask_crc, put_fixed32, put_fixed64, CompressionType,
    ForstError, ForstResult,
};

use super::schema::SST_MAGIC;

/// Size in bytes of the fixed-field portion of a V1 footer (before variable-length keys).
pub const FOOTER_FIXED_FIELDS_SIZE: usize = 76;

/// Size in bytes of the footer tail: checksum (4) + length (4) + magic (4).
pub const FOOTER_TAIL_SIZE: usize = 12;

// ---------------------------------------------------------------------------
// ChecksumType
// ---------------------------------------------------------------------------

/// Checksum algorithm used for integrity verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ChecksumType {
    /// CRC32C (Castagnoli) — the default.
    #[default]
    Crc32c = 0,
    /// xxHash32 — reserved for future use.
    XxHash32 = 1,
}

impl ChecksumType {
    /// Converts a raw byte to a [`ChecksumType`], returning `None` for
    /// unrecognized values.
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(ChecksumType::Crc32c),
            1 => Some(ChecksumType::XxHash32),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// FooterV1
// ---------------------------------------------------------------------------

/// A variable-length footer that closes every SST file (format version 1).
///
/// The footer carries section offsets, key-range statistics, and a CRC32C
/// integrity checksum. See the [module docs](self) for the full binary layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FooterV1 {
    /// Number of data blocks in the SST file.
    pub data_block_count: u32,
    /// Total number of key-value entries across all data blocks.
    pub total_entries: u64,
    /// Byte offset of the bloom filter section.
    pub bloom_filter_offset: u64,
    /// Size in bytes of the bloom filter section.
    pub bloom_filter_size: u32,
    /// Byte offset of the index section.
    pub index_offset: u64,
    /// Size in bytes of the index section.
    pub index_size: u32,
    /// Smallest key stored in the SST file.
    pub min_key: Vec<u8>,
    /// Largest key stored in the SST file.
    pub max_key: Vec<u8>,
    /// Smallest sequence number in the SST file.
    pub min_sequence: u64,
    /// Largest sequence number in the SST file.
    pub max_sequence: u64,
    /// Compression algorithm used for data blocks.
    pub compression: CompressionType,
    /// Checksum algorithm used for integrity verification.
    pub checksum_type: ChecksumType,
    /// SST file creation time (epoch milliseconds).
    pub creation_time: u64,
    /// SST format version (should match [`super::schema::SST_FORMAT_VERSION`]).
    pub format_version: u16,
}

impl FooterV1 {
    /// Serializes this footer into bytes.
    ///
    /// The returned buffer includes the fixed fields, variable-length keys,
    /// a masked CRC32C checksum, the total footer length, and the trailing
    /// magic bytes.
    pub fn encode(&self) -> Vec<u8> {
        let min_key_len = self.min_key.len() as u16;
        let max_key_len = self.max_key.len() as u16;
        let total_size = FOOTER_FIXED_FIELDS_SIZE
            + self.min_key.len()
            + self.max_key.len()
            + FOOTER_TAIL_SIZE;

        let mut buf = Vec::with_capacity(total_size);

        // Fixed fields (76 bytes)
        put_fixed32(&mut buf, self.data_block_count);
        put_fixed64(&mut buf, self.total_entries);
        put_fixed64(&mut buf, self.bloom_filter_offset);
        put_fixed32(&mut buf, self.bloom_filter_size);
        put_fixed64(&mut buf, self.index_offset);
        put_fixed32(&mut buf, self.index_size);
        put_fixed32(&mut buf, FOOTER_FIXED_FIELDS_SIZE as u32); // min_key_offset
        buf.extend_from_slice(&min_key_len.to_le_bytes());
        put_fixed32(
            &mut buf,
            (FOOTER_FIXED_FIELDS_SIZE as u32) + (min_key_len as u32),
        ); // max_key_offset
        buf.extend_from_slice(&max_key_len.to_le_bytes());
        put_fixed64(&mut buf, self.min_sequence);
        put_fixed64(&mut buf, self.max_sequence);
        buf.push(self.compression as u8);
        buf.push(self.checksum_type as u8);
        put_fixed64(&mut buf, self.creation_time);
        buf.extend_from_slice(&self.format_version.to_le_bytes());

        debug_assert_eq!(buf.len(), FOOTER_FIXED_FIELDS_SIZE);

        // Variable-length keys
        buf.extend_from_slice(&self.min_key);
        buf.extend_from_slice(&self.max_key);

        // Tail: checksum + length + magic
        let checksum = mask_crc(crc32c(&buf));
        put_fixed32(&mut buf, checksum);
        put_fixed32(&mut buf, total_size as u32);
        buf.extend_from_slice(SST_MAGIC);

        debug_assert_eq!(buf.len(), total_size);
        buf
    }

    /// Deserializes a [`FooterV1`] from a byte buffer.
    ///
    /// `data` must contain exactly one complete footer (typically obtained by
    /// reading the file tail to discover `footer_length`, then reading that
    /// many bytes).
    ///
    /// Returns [`ForstError::Corruption`] on any structural or checksum error.
    pub fn decode(data: &[u8]) -> ForstResult<Self> {
        let min_size = FOOTER_FIXED_FIELDS_SIZE + FOOTER_TAIL_SIZE;
        if data.len() < min_size {
            return Err(ForstError::corruption(format!(
                "footer too short: expected at least {} bytes, got {}",
                min_size,
                data.len()
            )));
        }

        let len = data.len();

        // Verify trailing magic
        if &data[len - 4..] != SST_MAGIC {
            return Err(ForstError::corruption(format!(
                "invalid footer magic: expected {:?}, got {:?}",
                SST_MAGIC,
                &data[len - 4..]
            )));
        }

        // Verify footer_length matches buffer size
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4])?;
        if footer_length as usize != len {
            return Err(ForstError::corruption(format!(
                "footer length mismatch: field says {} but buffer is {} bytes",
                footer_length, len
            )));
        }

        // Verify CRC32C checksum
        let (stored_checksum, _) = get_fixed32(&data[len - 12..len - 8])?;
        let computed_checksum = mask_crc(crc32c(&data[..len - 12]));
        if stored_checksum != computed_checksum {
            return Err(ForstError::corruption(format!(
                "footer checksum mismatch: stored {:#010X}, computed {:#010X}",
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
        let min_key_len = u16::from_le_bytes([data[40], data[41]]);
        let (max_key_offset, _) = get_fixed32(&data[42..])?;
        let max_key_len = u16::from_le_bytes([data[46], data[47]]);
        let (min_sequence, _) = get_fixed64(&data[48..])?;
        let (max_sequence, _) = get_fixed64(&data[56..])?;
        let compression_byte = data[64];
        let checksum_type_byte = data[65];
        let (creation_time, _) = get_fixed64(&data[66..])?;
        let format_version = u16::from_le_bytes([data[74], data[75]]);

        // Validate compression type
        let compression = match compression_byte {
            0 => CompressionType::None,
            1 => CompressionType::Lz4,
            2 => CompressionType::Zstd,
            other => {
                return Err(ForstError::corruption(format!(
                    "invalid compression type in footer: {other}"
                )));
            }
        };

        // Validate checksum type
        let checksum_type = ChecksumType::from_u8(checksum_type_byte).ok_or_else(|| {
            ForstError::corruption(format!(
                "invalid checksum type in footer: {checksum_type_byte}"
            ))
        })?;

        // Extract variable-length keys (bounds-check)
        let payload_end = len - FOOTER_TAIL_SIZE;
        let min_key_start = min_key_offset as usize;
        let min_key_end = min_key_start + min_key_len as usize;
        let max_key_start = max_key_offset as usize;
        let max_key_end = max_key_start + max_key_len as usize;

        if min_key_end > payload_end || max_key_end > payload_end {
            return Err(ForstError::corruption(format!(
                "key extends beyond footer payload: min_key [{}, {}), max_key [{}, {}), payload_end {}",
                min_key_start, min_key_end, max_key_start, max_key_end, payload_end
            )));
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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::schema::SST_FORMAT_VERSION;

    fn sample_footer() -> FooterV1 {
        FooterV1 {
            data_block_count: 10,
            total_entries: 5000,
            bloom_filter_offset: 16384,
            bloom_filter_size: 2048,
            index_offset: 18432,
            index_size: 512,
            min_key: b"aaa".to_vec(),
            max_key: b"zzz".to_vec(),
            min_sequence: 1,
            max_sequence: 5000,
            compression: CompressionType::Lz4,
            checksum_type: ChecksumType::Crc32c,
            creation_time: 1_700_000_000_000,
            format_version: SST_FORMAT_VERSION,
        }
    }

    // -----------------------------------------------------------------------
    // ChecksumType tests
    // -----------------------------------------------------------------------

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
        assert_eq!(ChecksumType::from_u8(255), None);
    }

    // -----------------------------------------------------------------------
    // FooterV1 encode tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_ends_with_magic() {
        let encoded = sample_footer().encode();
        let len = encoded.len();
        assert_eq!(&encoded[len - 4..], SST_MAGIC);
    }

    #[test]
    fn test_length_field() {
        let footer = sample_footer();
        let encoded = footer.encode();
        let len = encoded.len();
        let (stored_len, _) = get_fixed32(&encoded[len - 8..len - 4]).unwrap();
        assert_eq!(stored_len as usize, len);
    }

    #[test]
    fn test_size_matches_expected() {
        let footer = sample_footer();
        let encoded = footer.encode();
        let expected = FOOTER_FIXED_FIELDS_SIZE
            + footer.min_key.len()
            + footer.max_key.len()
            + FOOTER_TAIL_SIZE;
        assert_eq!(encoded.len(), expected);
    }

    // -----------------------------------------------------------------------
    // FooterV1 roundtrip tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_roundtrip() {
        let footer = sample_footer();
        let encoded = footer.encode();
        let decoded = FooterV1::decode(&encoded).unwrap();
        assert_eq!(decoded, footer);
    }

    #[test]
    fn test_roundtrip_empty_keys() {
        let footer = FooterV1 {
            min_key: vec![],
            max_key: vec![],
            ..sample_footer()
        };
        let encoded = footer.encode();
        let decoded = FooterV1::decode(&encoded).unwrap();
        assert_eq!(decoded, footer);
    }

    #[test]
    fn test_roundtrip_long_keys() {
        let footer = FooterV1 {
            min_key: vec![0xAA; 1024],
            max_key: vec![0xFF; 2048],
            ..sample_footer()
        };
        let encoded = footer.encode();
        let decoded = FooterV1::decode(&encoded).unwrap();
        assert_eq!(decoded, footer);
    }

    // -----------------------------------------------------------------------
    // FooterV1 decode error tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_bad_magic() {
        let mut encoded = sample_footer().encode();
        let len = encoded.len();
        encoded[len - 4] = b'X';
        let result = FooterV1::decode(&encoded);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("invalid footer magic"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_decode_bad_checksum() {
        let mut encoded = sample_footer().encode();
        // Corrupt a byte in the payload to trigger checksum mismatch
        encoded[0] ^= 0xFF;
        let result = FooterV1::decode(&encoded);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("checksum mismatch"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_decode_too_short() {
        let min_size = FOOTER_FIXED_FIELDS_SIZE + FOOTER_TAIL_SIZE;
        let short = vec![0u8; min_size - 1];
        let result = FooterV1::decode(&short);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("too short"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_decode_bad_length() {
        let mut encoded = sample_footer().encode();
        let len = encoded.len();
        // Overwrite footer_length with a wrong value, keep magic intact
        let wrong_len = (len as u32) + 99;
        encoded[len - 8] = wrong_len as u8;
        encoded[len - 7] = (wrong_len >> 8) as u8;
        encoded[len - 6] = (wrong_len >> 16) as u8;
        encoded[len - 5] = (wrong_len >> 24) as u8;
        let result = FooterV1::decode(&encoded);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("length mismatch"),
            "unexpected error: {err_msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Constants tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_fixed_fields_size() {
        assert_eq!(FOOTER_FIXED_FIELDS_SIZE, 76);
    }

    #[test]
    fn test_tail_size() {
        assert_eq!(FOOTER_TAIL_SIZE, 12);
    }

    #[test]
    fn test_min_size() {
        assert_eq!(FOOTER_FIXED_FIELDS_SIZE + FOOTER_TAIL_SIZE, 88);
    }

    // -----------------------------------------------------------------------
    // Integration: extract footer from file tail simulation
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_from_file_tail() {
        let footer = sample_footer();
        let encoded = footer.encode();

        // Simulate reading file tail: last 8 bytes contain length + magic
        let tail = &encoded[encoded.len() - 8..];
        assert_eq!(&tail[4..], SST_MAGIC);
        let (footer_length, _) = get_fixed32(&tail[0..4]).unwrap();

        // In real code, you would seek back footer_length bytes and read
        assert_eq!(footer_length as usize, encoded.len());
        let decoded = FooterV1::decode(&encoded).unwrap();
        assert_eq!(decoded, footer);
    }
}
