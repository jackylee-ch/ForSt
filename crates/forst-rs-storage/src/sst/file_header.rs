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

//! SST file header encoding and decoding.
//!
//! Every SST file begins with a 16-byte header:
//!
//! ```text
//! Offset  Field           Size  Encoding
//! 0       magic           4     b"FRST"
//! 4       format_version  2     u16 LE
//! 6       flags           2     u16 LE
//! 8       reserved        8     [u8; 8] zeros
//! ```

use forst_rs_common::{ForstError, ForstResult};

use super::schema::{FILE_HEADER_SIZE, SST_FORMAT_VERSION, SST_MAGIC};

/// A 16-byte header at the start of every SST file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    /// SST format version (currently [`SST_FORMAT_VERSION`] = 1).
    pub format_version: u16,
    /// Bit flags reserved for future use (currently 0).
    pub flags: u16,
}

impl FileHeader {
    /// Creates a new [`FileHeader`] with the given version and flags.
    pub fn new(format_version: u16, flags: u16) -> Self {
        Self {
            format_version,
            flags,
        }
    }

    /// Serializes this header into exactly [`FILE_HEADER_SIZE`] (16) bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(FILE_HEADER_SIZE);
        buf.extend_from_slice(SST_MAGIC);
        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.extend_from_slice(&self.flags.to_le_bytes());
        buf.extend_from_slice(&[0u8; 8]);
        buf
    }

    /// Deserializes a [`FileHeader`] from the first [`FILE_HEADER_SIZE`] bytes
    /// of `data`.
    ///
    /// Returns [`ForstError::Corruption`] if:
    /// - `data` is shorter than 16 bytes, or
    /// - the first 4 bytes are not the magic `b"FRST"`.
    pub fn decode(data: &[u8]) -> ForstResult<Self> {
        if data.len() < FILE_HEADER_SIZE {
            return Err(ForstError::corruption(format!(
                "file header too short: expected {} bytes, got {}",
                FILE_HEADER_SIZE,
                data.len()
            )));
        }

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
    /// Returns a header with [`SST_FORMAT_VERSION`] and flags = 0.
    fn default() -> Self {
        Self {
            format_version: SST_FORMAT_VERSION,
            flags: 0,
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_size() {
        let header = FileHeader::default();
        let encoded = header.encode();
        assert_eq!(encoded.len(), FILE_HEADER_SIZE);
    }

    #[test]
    fn test_starts_with_magic() {
        let encoded = FileHeader::default().encode();
        assert_eq!(&encoded[0..4], SST_MAGIC);
    }

    #[test]
    fn test_roundtrip() {
        let header = FileHeader::new(42, 0x00FF);
        let encoded = header.encode();
        let decoded = FileHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_decode_bad_magic() {
        let mut encoded = FileHeader::default().encode();
        encoded[0] = b'X';
        let result = FileHeader::decode(&encoded);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("invalid SST magic"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_decode_too_short() {
        let short = vec![0u8; FILE_HEADER_SIZE - 1];
        let result = FileHeader::decode(&short);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("too short"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_byte_layout() {
        let header = FileHeader::new(0x0102, 0x0304);
        let encoded = header.encode();

        // magic
        assert_eq!(&encoded[0..4], b"FRST");
        // format_version = 0x0102 LE
        assert_eq!(&encoded[4..6], &[0x02, 0x01]);
        // flags = 0x0304 LE
        assert_eq!(&encoded[6..8], &[0x04, 0x03]);
        // reserved = 8 zero bytes
        assert_eq!(&encoded[8..16], &[0u8; 8]);
    }

    #[test]
    fn test_default() {
        let header = FileHeader::default();
        assert_eq!(header.format_version, SST_FORMAT_VERSION);
        assert_eq!(header.flags, 0);
    }
}
