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

//! Block-level compression and decompression utilities.
//!
//! Supports three modes via [`CompressionType`]:
//! - `None` — passthrough (no compression)
//! - `Lz4` — fast compression via [`lz4_flex`] with prepended size
//! - `Zstd` — higher-ratio compression via [`zstd`] at level 3

use forst_rs_common::{CompressionType, ForstError, ForstResult};

/// Compress `data` using the specified [`CompressionType`].
///
/// Returns the compressed bytes. For [`CompressionType::None`], returns a copy
/// of the input unchanged.
pub fn compress(data: &[u8], compression: CompressionType) -> ForstResult<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
        CompressionType::Zstd => zstd::encode_all(data, 3)
            .map_err(|e| ForstError::corruption(format!("Zstd compression failed: {e}"))),
    }
}

/// Decompress `data` that was compressed with the specified [`CompressionType`].
///
/// The `_uncompressed_size` parameter is reserved for future use (e.g.,
/// pre-allocating the output buffer) and is currently ignored.
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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_identity() {
        let data = b"hello world";
        let compressed = compress(data, CompressionType::None).unwrap();
        assert_eq!(compressed, data);
        let decompressed = decompress(&compressed, CompressionType::None, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let compressed = compress(data, CompressionType::Lz4).unwrap();
        let decompressed =
            decompress(&compressed, CompressionType::Lz4, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_roundtrip() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let compressed = compress(data, CompressionType::Zstd).unwrap();
        let decompressed =
            decompress(&compressed, CompressionType::Zstd, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_small_data() {
        let data = b"ab";
        let compressed = compress(data, CompressionType::Lz4).unwrap();
        let decompressed =
            decompress(&compressed, CompressionType::Lz4, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_small_data() {
        let data = b"ab";
        let compressed = compress(data, CompressionType::Zstd).unwrap();
        let decompressed =
            decompress(&compressed, CompressionType::Zstd, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_empty() {
        let data = b"";
        let compressed = compress(data, CompressionType::Lz4).unwrap();
        let decompressed =
            decompress(&compressed, CompressionType::Lz4, 0).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_empty() {
        let data = b"";
        let compressed = compress(data, CompressionType::Zstd).unwrap();
        let decompressed =
            decompress(&compressed, CompressionType::Zstd, 0).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_reduces_size_for_repetitive_data() {
        // 4 KB of repetitive data should compress well.
        let data: Vec<u8> = (0..4096).map(|i| (i % 10) as u8).collect();

        let lz4_compressed = compress(&data, CompressionType::Lz4).unwrap();
        assert!(
            lz4_compressed.len() < data.len(),
            "LZ4 should reduce size: {} vs {}",
            lz4_compressed.len(),
            data.len()
        );

        let zstd_compressed = compress(&data, CompressionType::Zstd).unwrap();
        assert!(
            zstd_compressed.len() < data.len(),
            "Zstd should reduce size: {} vs {}",
            zstd_compressed.len(),
            data.len()
        );
    }
}
