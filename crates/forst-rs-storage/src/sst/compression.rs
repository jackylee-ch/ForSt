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
/// The `uncompressed_size` parameter is the trusted upper bound (read from
/// the BlockHeader at SST decode time) on the legitimate output size. After
/// decompression, the actual output is verified to match `uncompressed_size`.
/// This prevents a DoS via a crafted compressed frame whose internal size
/// header claims gigabytes — both `lz4_flex::decompress_size_prepended` and
/// `zstd::decode_all` would otherwise allocate based on the untrusted frame
/// header (Sweep R5 H by Reviewer 5).
pub fn decompress(
    data: &[u8],
    compression: CompressionType,
    uncompressed_size: usize,
) -> ForstResult<Vec<u8>> {
    let result: ForstResult<Vec<u8>> = match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Lz4 => lz4_flex::decompress_size_prepended(data)
            .map_err(|e| ForstError::corruption(format!("LZ4 decompression failed: {e}"))),
        CompressionType::Zstd => zstd::decode_all(data)
            .map_err(|e| ForstError::corruption(format!("Zstd decompression failed: {e}"))),
    };
    let out = result?;
    // SECURITY: validate the decompressed length against the trusted
    // BlockHeader-supplied size. Mismatch indicates a crafted compressed
    // frame; reject before passing the buffer downstream.
    if out.len() != uncompressed_size {
        return Err(ForstError::corruption(format!(
            "decompressed size {} does not match expected {} for {:?}",
            out.len(),
            uncompressed_size,
            compression
        )));
    }
    Ok(out)
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
        let decompressed = decompress(&compressed, CompressionType::Lz4, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_roundtrip() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let compressed = compress(data, CompressionType::Zstd).unwrap();
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
    fn test_lz4_empty() {
        let data = b"";
        let compressed = compress(data, CompressionType::Lz4).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Lz4, 0).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_empty() {
        let data = b"";
        let compressed = compress(data, CompressionType::Zstd).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Zstd, 0).unwrap();
        assert_eq!(decompressed, data);
    }

    /// Regression test for Sweep R5 H (Reviewer 5): if the actual
    /// decompressed length doesn't match the trusted `uncompressed_size`
    /// from the BlockHeader, the function must reject. Defends against
    /// crafted compressed frames whose internal size header lies.
    #[test]
    fn test_decompress_rejects_size_mismatch() {
        let data = b"some data of definite length";
        // Compress with LZ4 — actual decompressed length will be data.len().
        let compressed = compress(data, CompressionType::Lz4).unwrap();
        // Pass a wrong expected size — must reject.
        let err = match decompress(&compressed, CompressionType::Lz4, data.len() + 100) {
            Ok(_) => panic!("must reject size mismatch"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("decompressed size") && msg.contains("does not match expected"),
            "expected size-mismatch error; got: {}",
            msg
        );
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
