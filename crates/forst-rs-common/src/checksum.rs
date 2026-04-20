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

//! CRC32C (Castagnoli) checksum utilities for ForSt-RS.
//!
//! This module provides CRC32C checksum computation with the same masking
//! scheme used by RocksDB/LevelDB, ensuring on-disk compatibility with
//! existing SST files and WAL records.
//!
//! **Important:** RocksDB uses CRC32**C** (Castagnoli, polynomial 0x1EDC6F41),
//! NOT the IEEE CRC32 (polynomial 0x04C11DB7). The `crc32c` crate provides
//! the correct algorithm with hardware acceleration on x86_64 (SSE 4.2).
//!
//! # Masking
//!
//! RocksDB stores *masked* CRC values to avoid collisions between the
//! CRC of a block and the CRC of a block that happens to be a valid
//! CRC prefix. The mask is: `rotate_right(crc, 15) + 0xa282ead8`.

/// Computes the CRC32C (Castagnoli) checksum of `data`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Extends a running CRC32C with additional `data`.
#[inline]
pub fn crc32c_extend(crc: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(crc, data)
}

/// The masking constant used by RocksDB/LevelDB.
const MASK_DELTA: u32 = 0xa282_ead8;

/// Applies the RocksDB CRC masking to prevent the stored value from
/// coinciding with actual data patterns.
///
/// `masked = rotate_right(crc, 15) + MASK_DELTA`
#[inline]
pub fn mask_crc(crc: u32) -> u32 {
    crc.rotate_right(15).wrapping_add(MASK_DELTA)
}

/// Reverses the RocksDB CRC masking applied by [`mask_crc`].
#[inline]
pub fn unmask_crc(masked: u32) -> u32 {
    let rot = masked.wrapping_sub(MASK_DELTA);
    rot.rotate_left(15)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32c_empty() {
        // CRC32C of empty input is 0
        assert_eq!(crc32c(&[]), 0);
    }

    #[test]
    fn test_crc32c_known_vector() {
        // RFC 3720 test vector: CRC32C of "123456789" = 0xE3069283
        let crc = crc32c(b"123456789");
        assert_eq!(
            crc, 0xE3069283,
            "CRC32C of '123456789' should match RFC 3720 test vector"
        );
    }

    #[test]
    fn test_crc32c_hello() {
        let crc = crc32c(b"hello");
        // Verify deterministic
        assert_eq!(crc, crc32c(b"hello"));
        // Different input gives different result
        assert_ne!(crc, crc32c(b"world"));
    }

    #[test]
    fn test_crc32c_extend_equivalent() {
        let data = b"hello world";
        let full = crc32c(data);
        let partial = crc32c(b"hello ");
        let extended = crc32c_extend(partial, b"world");
        assert_eq!(full, extended);
    }

    #[test]
    fn test_mask_unmask_roundtrip() {
        for &crc in &[0u32, 1, 0xDEADBEEF, u32::MAX] {
            let masked = mask_crc(crc);
            let unmasked = unmask_crc(masked);
            assert_eq!(unmasked, crc, "roundtrip failed for {:#x}", crc);
        }
    }

    #[test]
    fn test_mask_changes_value() {
        let crc = crc32c(b"test data");
        let masked = mask_crc(crc);
        assert_ne!(crc, masked, "mask should change the value");
    }

    #[test]
    fn test_crc32c_large_data() {
        let data = vec![0u8; 4096];
        let crc = crc32c(&data);
        assert_eq!(crc, crc32c(&data)); // deterministic
        assert_ne!(crc, 0); // 4K of zeros should NOT hash to 0 with CRC32C
    }

    #[test]
    fn test_crc32c_incremental_chunks() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let full_crc = crc32c(data);

        let crc1 = crc32c(&data[..10]);
        let crc2 = crc32c_extend(crc1, &data[10..20]);
        let crc3 = crc32c_extend(crc2, &data[20..]);
        assert_eq!(full_crc, crc3);
    }

    #[test]
    fn test_crc32c_single_byte_values() {
        // Verify each single byte produces a unique CRC
        let crcs: Vec<u32> = (0u8..=255).map(|b| crc32c(&[b])).collect();
        let unique: std::collections::HashSet<u32> = crcs.iter().cloned().collect();
        assert_eq!(unique.len(), 256, "all single-byte CRCs should be unique");
    }
}
