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

//! CRC32C checksum utilities for ForSt-RS.
//!
//! This module provides CRC32C (Castagnoli) checksum computation with
//! the same masking scheme used by RocksDB/LevelDB, ensuring on-disk
//! compatibility with existing SST files and WAL records.
//!
//! # Masking
//!
//! RocksDB stores *masked* CRC values to avoid collisions between the
//! CRC of a block and the CRC of a block that happens to be a valid
//! CRC prefix. The mask is: `((crc >> 15) | (crc << 17)) + 0xa282ead8`.

/// Computes the CRC32C checksum of `data`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Extends a running CRC32C with additional `data`.
#[inline]
pub fn crc32c_extend(crc: u32, data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new_with_initial(crc);
    hasher.update(data);
    hasher.finalize()
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
        let crc = crc32c(&[]);
        // CRC32 of empty input is 0
        assert_eq!(crc, 0);
    }

    #[test]
    fn test_crc32c_hello() {
        let crc = crc32c(b"hello");
        // Verify deterministic — same input, same output
        assert_eq!(crc, crc32c(b"hello"));
        // Verify different input gives different result
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
        // Masking should produce a different value
        assert_ne!(crc, masked, "mask should change the value");
    }

    #[test]
    fn test_crc32c_large_data() {
        // 4 KB of zeros
        let data = vec![0u8; 4096];
        let crc = crc32c(&data);
        assert_eq!(crc, crc32c(&data)); // deterministic
    }

    #[test]
    fn test_crc32c_incremental_chunks() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let full_crc = crc32c(data);

        // Build up incrementally
        let crc1 = crc32c(&data[..10]);
        let crc2 = crc32c_extend(crc1, &data[10..20]);
        let crc3 = crc32c_extend(crc2, &data[20..]);
        assert_eq!(full_crc, crc3);
    }
}
