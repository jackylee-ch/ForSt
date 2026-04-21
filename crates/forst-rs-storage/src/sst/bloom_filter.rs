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

//! Split Block Bloom Filter (SBBF) implementation.
//!
//! This module provides a space-efficient probabilistic data structure used in
//! SST files to quickly determine whether a key *might* exist in a data block.
//! The SBBF design is based on the Parquet bloom filter specification: each
//! block is a 256-bit (8 x u32) word array, and keys are mapped to blocks via
//! the upper 32 bits of an xxHash64 digest.

/// Salt constants used for bit-setting within a 256-bit block.
/// Each salt produces one bit position (via `key_bits.wrapping_mul(salt) >> 27`),
/// giving 8 independent bit probes per block.
const SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947,
    0x5c6bfb31,
];

/// Computes the optimal number of 256-bit blocks for the given number of keys.
///
/// The formula targets ~1.95% false-positive rate at full capacity:
/// `ceil(num_keys * 21 / 512)`, with a minimum of 1.
pub fn optimal_num_blocks(num_keys: usize) -> usize {
    let blocks = (num_keys * 21 + 511) / 512;
    if blocks < 1 {
        1
    } else {
        blocks
    }
}

/// Determines which block a hash maps to.
///
/// Uses the upper 32 bits of the hash to select a block index via
/// fixed-point multiplication.
fn block_index_from_hash(hash: u64, num_blocks: u32) -> u32 {
    let upper = (hash >> 32) as u32;
    ((upper as u64 * num_blocks as u64) >> 32) as u32
}

/// Sets 8 bit positions in a single 256-bit block using the SALT constants.
fn block_insert(block: &mut [u32; 8], key_bits: u32) {
    for i in 0..8 {
        let bit_pos = key_bits.wrapping_mul(SALT[i]) >> 27;
        block[i] |= 1u32 << bit_pos;
    }
}

/// Checks whether all 8 bit positions in a block are set.
fn block_check(block: &[u32; 8], key_bits: u32) -> bool {
    for i in 0..8 {
        let bit_pos = key_bits.wrapping_mul(SALT[i]) >> 27;
        if block[i] & (1u32 << bit_pos) == 0 {
            return false;
        }
    }
    true
}

/// A Split Block Bloom Filter (SBBF).
///
/// Each element of `data` is a 256-bit block represented as `[u32; 8]`.
/// Keys are hashed with xxHash64, then the upper 32 bits select a block
/// and the lower 32 bits set/check 8 bit positions within that block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sbbf {
    data: Vec<[u32; 8]>,
}

impl Sbbf {
    /// Creates a new SBBF sized for the expected number of keys.
    ///
    /// All blocks are initialized to zero (no keys inserted).
    pub fn new(num_keys: usize) -> Self {
        let num_blocks = optimal_num_blocks(num_keys);
        Self {
            data: vec![[0u32; 8]; num_blocks],
        }
    }

    /// Returns the number of 256-bit blocks in this filter.
    pub fn num_blocks(&self) -> usize {
        self.data.len()
    }

    /// Returns the total size of the filter data in bytes.
    ///
    /// Each block is 8 x 4 = 32 bytes.
    pub fn size_in_bytes(&self) -> usize {
        self.data.len() * 32
    }

    /// Computes the xxHash64 digest of `key` with seed 0.
    pub fn hash_key(key: &[u8]) -> u64 {
        xxhash_rust::xxh64::xxh64(key, 0)
    }

    /// Inserts a pre-computed hash into the filter.
    pub fn insert_hash(&mut self, hash: u64) {
        let num_blocks = self.data.len() as u32;
        let block_index = block_index_from_hash(hash, num_blocks);
        let key_bits = hash as u32;
        block_insert(&mut self.data[block_index as usize], key_bits);
    }

    /// Checks whether a pre-computed hash *might* be present in the filter.
    ///
    /// Returns `true` if the hash may have been inserted (possible false positive),
    /// or `false` if it was definitely never inserted.
    pub fn check_hash(&self, hash: u64) -> bool {
        let num_blocks = self.data.len() as u32;
        let block_index = block_index_from_hash(hash, num_blocks);
        let key_bits = hash as u32;
        block_check(&self.data[block_index as usize], key_bits)
    }

    /// Inserts a key into the filter.
    pub fn insert(&mut self, key: &[u8]) {
        self.insert_hash(Self::hash_key(key));
    }

    /// Checks whether a key *might* be present in the filter.
    pub fn check(&self, key: &[u8]) -> bool {
        self.check_hash(Self::hash_key(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimal_num_blocks_zero_keys() {
        // 0 * 21 + 511 = 511 / 512 = 0 => clamped to 1
        assert_eq!(optimal_num_blocks(0), 1);
    }

    #[test]
    fn test_optimal_num_blocks_one_key() {
        // 1 * 21 + 511 = 532 / 512 = 1
        assert_eq!(optimal_num_blocks(1), 1);
    }

    #[test]
    fn test_optimal_num_blocks_100_keys() {
        // 100 * 21 + 511 = 2611 / 512 = 5
        assert_eq!(optimal_num_blocks(100), 5);
    }

    #[test]
    fn test_optimal_num_blocks_1000_keys() {
        // 1000 * 21 + 511 = 21511 / 512 = 42
        assert_eq!(optimal_num_blocks(1000), 42);
    }

    #[test]
    fn test_optimal_num_blocks_minimum_is_one() {
        assert!(optimal_num_blocks(0) >= 1);
        assert!(optimal_num_blocks(1) >= 1);
    }

    #[test]
    fn test_new_creates_zeroed_blocks() {
        let sbbf = Sbbf::new(100);
        assert_eq!(sbbf.num_blocks(), 5);
        for block in &sbbf.data {
            assert_eq!(*block, [0u32; 8]);
        }
    }

    #[test]
    fn test_size_in_bytes() {
        let sbbf = Sbbf::new(100);
        // 5 blocks * 32 bytes = 160
        assert_eq!(sbbf.size_in_bytes(), 160);
    }

    #[test]
    fn test_salt_constants_are_nonzero() {
        for salt in &SALT {
            assert_ne!(*salt, 0);
        }
    }

    #[test]
    fn test_salt_constants_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for salt in &SALT {
            assert!(seen.insert(*salt), "duplicate salt: {:#x}", salt);
        }
    }

    // ---- Task 3: insert / check tests ----

    #[test]
    fn test_insert_then_check_finds_key() {
        let mut sbbf = Sbbf::new(10);
        sbbf.insert(b"hello");
        assert!(sbbf.check(b"hello"));
    }

    #[test]
    fn test_check_absent_key_returns_false() {
        let sbbf = Sbbf::new(10);
        assert!(!sbbf.check(b"missing"));
    }

    #[test]
    fn test_insert_multiple_keys() {
        let mut sbbf = Sbbf::new(50);
        for i in 0..50u32 {
            sbbf.insert(&i.to_le_bytes());
        }
        for i in 0..50u32 {
            assert!(sbbf.check(&i.to_le_bytes()), "key {} not found", i);
        }
    }

    #[test]
    fn test_insert_from_hash() {
        let mut sbbf = Sbbf::new(10);
        let hash = Sbbf::hash_key(b"test-key");
        sbbf.insert_hash(hash);
        assert!(sbbf.check_hash(hash));
    }

    #[test]
    fn test_false_positive_rate_below_5_percent() {
        let num_inserted = 1000usize;
        let mut sbbf = Sbbf::new(num_inserted);

        // Insert keys 0..1000
        for i in 0..num_inserted as u32 {
            sbbf.insert(&i.to_le_bytes());
        }

        // Check 10 000 keys that were NOT inserted
        let num_checks = 10_000u32;
        let mut false_positives = 0u32;
        for i in (num_inserted as u32)..(num_inserted as u32 + num_checks) {
            if sbbf.check(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / num_checks as f64;
        assert!(
            fp_rate < 0.05,
            "false positive rate {:.4} exceeds 5%",
            fp_rate
        );
    }
}
