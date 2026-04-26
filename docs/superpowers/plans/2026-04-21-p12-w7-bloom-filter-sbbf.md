# P1.2 W7: Split Block Bloom Filter (SBBF) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a Split Block Bloom Filter (SBBF) for the ForSt-RS SST format and integrate it into SstWriterImpl so that SST files contain a real bloom filter section instead of the current placeholder (offset=0, size=0).

**Architecture:** The SBBF uses xxHash64 to hash each key, then maps it to one of N 256-bit blocks (each block = 8 × u32 words). Eight salt constants produce 8 bit-masks within the selected block. During SST writing, all key hashes are collected; at `finish()` time, the SBBF is built from the collected hashes, serialized between DataBlocks and the IndexSection, and its offset/size are recorded in the Footer.

**Tech Stack:** Rust 1.75+, `xxhash-rust` crate (xxh64 feature), `forst-rs-common` (error types, coding helpers), `forst-rs-storage` (SST writer)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `Cargo.toml` (workspace root) | Modify | Add `xxhash-rust` workspace dependency |
| `crates/forst-rs-storage/Cargo.toml` | Modify | Add `xxhash-rust` dependency |
| `crates/forst-rs-storage/src/sst/bloom_filter.rs` | Create | SBBF struct: new, insert, check, encode, decode |
| `crates/forst-rs-storage/src/sst/mod.rs` | Modify | Add `pub mod bloom_filter` + re-exports |
| `crates/forst-rs-storage/src/sst/writer.rs` | Modify | Collect key hashes, build & write SBBF at finish() |
| `crates/forst-rs-storage/tests/sst_integration.rs` | Modify | Add E2E test verifying bloom filter in SST |

---

## Task 1: Add xxhash-rust Dependency

**Files:**
- Modify: `Cargo.toml` (workspace root, line ~49)
- Modify: `crates/forst-rs-storage/Cargo.toml` (line ~28)

- [ ] **Step 1: Add xxhash-rust to workspace dependencies**

In the workspace root `Cargo.toml`, add this line in the `[workspace.dependencies]` section, after the `crc32c` line (line 49):

```toml
# Hashing — xxHash64 for Bloom Filter
xxhash-rust = { version = "0.8", features = ["xxh64"] }
```

The full `[workspace.dependencies]` section should now include:
```toml
# Checksums — CRC32C (Castagnoli), NOT IEEE CRC32
crc32c = "0.6"

# Hashing — xxHash64 for Bloom Filter
xxhash-rust = { version = "0.8", features = ["xxh64"] }
```

- [ ] **Step 2: Add xxhash-rust to forst-rs-storage dependencies**

In `crates/forst-rs-storage/Cargo.toml`, add this line in the `[dependencies]` section (after `zstd`):

```toml
xxhash-rust = { workspace = true }
```

The full `[dependencies]` section should now be:
```toml
[dependencies]
forst-rs-common = { workspace = true }
arrow = { workspace = true }
lz4_flex = { workspace = true }
zstd = { workspace = true }
xxhash-rust = { workspace = true }
```

- [ ] **Step 3: Verify the dependency resolves**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo check -p forst-rs-storage 2>&1 | tail -5
```
Expected: compilation succeeds (possibly with "unused" warnings which is fine)

- [ ] **Step 4: Commit**

```bash
cd ~/code/github/ForSt && git add Cargo.toml crates/forst-rs-storage/Cargo.toml && git commit -m "feat: add xxhash-rust dependency for SBBF bloom filter"
```

---

## Task 2: SBBF Core — Struct, Constructor, and Constants

**Files:**
- Create: `crates/forst-rs-storage/src/sst/bloom_filter.rs`
- Modify: `crates/forst-rs-storage/src/sst/mod.rs`

- [ ] **Step 1: Create bloom_filter.rs with the module skeleton and tests**

Create `crates/forst-rs-storage/src/sst/bloom_filter.rs` with the following content:

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

//! Split Block Bloom Filter (SBBF) implementation.
//!
//! The SBBF is a cache-friendly Bloom filter variant used by Apache Parquet.
//! Each filter is an array of 256-bit "blocks" (8 × u32 words). A key is
//! hashed with xxHash64; the upper 32 bits select a block, and the lower
//! 32 bits set/check 8 bits within that block via salt-based masking.
//!
//! Reference: Apache Parquet format specification — Bloom Filter.

use forst_rs_common::ForstResult;

/// The 8 salt constants from the Parquet SBBF specification.
/// Each salt is multiplied with the lower 32 bits of the hash to produce
/// a bit position (0–31) within the corresponding u32 word.
const SALT: [u32; 8] = [
    0x47b6137b,
    0x44974d91,
    0x8824ad5b,
    0xa2b7289d,
    0x705495c7,
    0x2df1424b,
    0x9efc4947,
    0x5c6bfb31,
];

/// Computes the optimal number of 256-bit blocks for a target false-positive
/// rate of ~1% (~10.5 bits per key).
///
/// Returns at least 1 block.
fn optimal_num_blocks(num_keys: usize) -> usize {
    // ~10.5 bits per key for ~1% FPR. Each block has 256 bits.
    // num_blocks = ceil(num_keys * 10.5 / 256)
    if num_keys == 0 {
        return 1;
    }
    // Use integer math: (num_keys * 21 + 511) / 512 ≈ ceil(num_keys * 10.5 / 256)
    let num_blocks = (num_keys * 21 + 511) / 512;
    num_blocks.max(1)
}

/// A Split Block Bloom Filter (SBBF).
///
/// The filter is an array of blocks, where each block is 8 × u32 = 256 bits.
/// Keys are hashed with xxHash64 (seed=0). The upper 32 bits of the hash
/// select a block index; the lower 32 bits produce 8 bit-masks via the
/// SALT constants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sbbf {
    /// The SBBF bitset, stored as blocks of 8 × u32 words.
    data: Vec<[u32; 8]>,
}

impl Sbbf {
    /// Creates a new empty SBBF sized for the given number of keys.
    ///
    /// The number of blocks is chosen to achieve ~1% false-positive rate.
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

    /// Returns the total size of the serialized filter in bytes.
    pub fn size_in_bytes(&self) -> usize {
        self.data.len() * 32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimal_num_blocks_zero_keys() {
        assert_eq!(optimal_num_blocks(0), 1);
    }

    #[test]
    fn test_optimal_num_blocks_one_key() {
        assert_eq!(optimal_num_blocks(1), 1);
    }

    #[test]
    fn test_optimal_num_blocks_100_keys() {
        // 100 * 21 / 512 ≈ 4.1 → ceil = 5
        let n = optimal_num_blocks(100);
        assert!(n >= 4 && n <= 5, "expected 4-5, got {n}");
    }

    #[test]
    fn test_optimal_num_blocks_1000_keys() {
        // 1000 * 10.5 / 256 ≈ 41 blocks
        let n = optimal_num_blocks(1000);
        assert!(n >= 40 && n <= 42, "expected ~41, got {n}");
    }

    #[test]
    fn test_optimal_num_blocks_minimum_is_one() {
        assert!(optimal_num_blocks(5) >= 1);
    }

    #[test]
    fn test_new_creates_zeroed_blocks() {
        let sbbf = Sbbf::new(100);
        assert!(sbbf.num_blocks() >= 4);
        for block in &sbbf.data {
            assert_eq!(*block, [0u32; 8]);
        }
    }

    #[test]
    fn test_size_in_bytes() {
        let sbbf = Sbbf::new(100);
        assert_eq!(sbbf.size_in_bytes(), sbbf.num_blocks() * 32);
    }

    #[test]
    fn test_salt_constants_are_nonzero() {
        for s in &SALT {
            assert_ne!(*s, 0);
        }
    }

    #[test]
    fn test_salt_constants_are_unique() {
        for i in 0..SALT.len() {
            for j in (i + 1)..SALT.len() {
                assert_ne!(SALT[i], SALT[j], "SALT[{i}] == SALT[{j}]");
            }
        }
    }
}
```

- [ ] **Step 2: Register the module in mod.rs**

In `crates/forst-rs-storage/src/sst/mod.rs`, add after line 23 (`pub mod sparse_index;`):

```rust
pub mod bloom_filter;
```

And add a re-export after line 34 (after the `sparse_index` re-exports):

```rust
pub use bloom_filter::Sbbf;
```

The full `mod.rs` should now have these module declarations:
```rust
pub mod block_header;
pub mod bloom_filter;
pub mod compression;
pub mod data_block;
pub mod file_header;
pub mod footer;
pub mod schema;
pub mod sparse_index;
pub mod writer;
```

And these re-exports should include:
```rust
pub use bloom_filter::Sbbf;
```

- [ ] **Step 3: Run tests to verify the struct compiles and tests pass**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test -p forst-rs-storage -- bloom_filter 2>&1 | tail -20
```
Expected: all 10 tests pass

- [ ] **Step 4: Commit**

```bash
cd ~/code/github/ForSt && git add crates/forst-rs-storage/src/sst/bloom_filter.rs crates/forst-rs-storage/src/sst/mod.rs && git commit -m "feat: add SBBF struct, constructor, and optimal_num_blocks"
```

---

## Task 3: SBBF Insert and Check (Probe)

**Files:**
- Modify: `crates/forst-rs-storage/src/sst/bloom_filter.rs`

- [ ] **Step 1: Write failing tests for insert and check**

Add these tests to the `mod tests` block in `bloom_filter.rs`, after the existing tests:

```rust
    #[test]
    fn test_insert_then_check_finds_key() {
        let mut sbbf = Sbbf::new(100);
        sbbf.insert(b"hello");
        assert!(sbbf.check(b"hello"));
    }

    #[test]
    fn test_check_absent_key_returns_false() {
        let sbbf = Sbbf::new(100);
        // An empty filter should not match any key.
        assert!(!sbbf.check(b"nonexistent"));
    }

    #[test]
    fn test_insert_multiple_keys() {
        let mut sbbf = Sbbf::new(100);
        let keys: Vec<Vec<u8>> = (0..50).map(|i| format!("key_{:04}", i).into_bytes()).collect();
        for key in &keys {
            sbbf.insert(key);
        }
        // All inserted keys must be found (no false negatives).
        for key in &keys {
            assert!(sbbf.check(key), "key {:?} should be found", key);
        }
    }

    #[test]
    fn test_insert_from_hash() {
        let mut sbbf = Sbbf::new(100);
        let h = Sbbf::hash_key(b"test_key");
        sbbf.insert_hash(h);
        assert!(sbbf.check_hash(h));
    }

    #[test]
    fn test_false_positive_rate_below_5_percent() {
        // Insert 1000 keys, then check 10000 absent keys.
        // FPR should be well below 5% for a filter sized at ~1%.
        let n = 1000;
        let mut sbbf = Sbbf::new(n);
        for i in 0..n {
            let key = format!("inserted_{:06}", i);
            sbbf.insert(key.as_bytes());
        }

        let mut false_positives = 0;
        let num_checks = 10_000;
        for i in 0..num_checks {
            let key = format!("absent_{:08}", i);
            if sbbf.check(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let fpr = false_positives as f64 / num_checks as f64;
        assert!(
            fpr < 0.05,
            "false positive rate {:.4} exceeds 5%",
            fpr
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test -p forst-rs-storage -- bloom_filter 2>&1 | tail -10
```
Expected: FAIL — methods `insert`, `check`, `hash_key`, `insert_hash`, `check_hash` do not exist yet.

- [ ] **Step 3: Implement hash_key, insert_hash, check_hash, insert, check**

Add the following methods to the `impl Sbbf` block in `bloom_filter.rs`, after the `size_in_bytes()` method:

```rust
    /// Computes the xxHash64 of a key with seed 0.
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

    /// Checks whether a pre-computed hash may be present in the filter.
    ///
    /// Returns `true` if the key _might_ be present (possible false positive),
    /// `false` if the key is _definitely_ absent.
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

    /// Checks whether a key may be present in the filter.
    ///
    /// Returns `true` if the key _might_ be present (possible false positive),
    /// `false` if the key is _definitely_ absent.
    pub fn check(&self, key: &[u8]) -> bool {
        self.check_hash(Self::hash_key(key))
    }
```

And add these helper functions *above* the `impl Sbbf` block (after the `optimal_num_blocks` function):

```rust
/// Selects a block index from a 64-bit hash using the multiply-shift trick.
///
/// Uses the upper 32 bits of the hash for block selection (Parquet convention).
fn block_index_from_hash(hash: u64, num_blocks: u32) -> u32 {
    let upper = (hash >> 32) as u32;
    ((upper as u64 * num_blocks as u64) >> 32) as u32
}

/// Sets 8 bits in a 256-bit block based on the salt constants.
fn block_insert(block: &mut [u32; 8], key_bits: u32) {
    for i in 0..8 {
        let bit_pos = key_bits.wrapping_mul(SALT[i]) >> 27;
        block[i] |= 1u32 << bit_pos;
    }
}

/// Checks whether all 8 salt-derived bits are set in a block.
fn block_check(block: &[u32; 8], key_bits: u32) -> bool {
    for i in 0..8 {
        let bit_pos = key_bits.wrapping_mul(SALT[i]) >> 27;
        if block[i] & (1u32 << bit_pos) == 0 {
            return false;
        }
    }
    true
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test -p forst-rs-storage -- bloom_filter 2>&1 | tail -20
```
Expected: all 15 tests pass (10 from Task 2 + 5 new)

- [ ] **Step 5: Commit**

```bash
cd ~/code/github/ForSt && git add crates/forst-rs-storage/src/sst/bloom_filter.rs && git commit -m "feat: add SBBF insert and check with xxHash64 hashing"
```

---

## Task 4: SBBF Encode and Decode

**Files:**
- Modify: `crates/forst-rs-storage/src/sst/bloom_filter.rs`

- [ ] **Step 1: Write failing tests for encode and decode**

Add these tests to the `mod tests` block in `bloom_filter.rs`:

```rust
    #[test]
    fn test_encode_size_matches() {
        let sbbf = Sbbf::new(100);
        let encoded = sbbf.encode();
        assert_eq!(encoded.len(), sbbf.size_in_bytes());
    }

    #[test]
    fn test_encode_empty_filter_is_all_zeros() {
        let sbbf = Sbbf::new(10);
        let encoded = sbbf.encode();
        assert!(encoded.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_roundtrip_empty() {
        let sbbf = Sbbf::new(50);
        let encoded = sbbf.encode();
        let decoded = Sbbf::decode(&encoded).unwrap();
        assert_eq!(decoded, sbbf);
    }

    #[test]
    fn test_roundtrip_with_data() {
        let mut sbbf = Sbbf::new(200);
        for i in 0..100 {
            sbbf.insert(format!("key_{i}").as_bytes());
        }
        let encoded = sbbf.encode();
        let decoded = Sbbf::decode(&encoded).unwrap();
        assert_eq!(decoded, sbbf);

        // Verify the decoded filter still finds all keys.
        for i in 0..100 {
            assert!(decoded.check(format!("key_{i}").as_bytes()));
        }
    }

    #[test]
    fn test_decode_invalid_size() {
        // 33 bytes is not a multiple of 32.
        let bad_data = vec![0u8; 33];
        let result = Sbbf::decode(&bad_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_empty_data() {
        // 0 bytes means 0 blocks — should fail.
        let result = Sbbf::decode(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_uses_little_endian() {
        let mut sbbf = Sbbf::new(1); // 1 block minimum
        // Manually set the first word to a known value.
        sbbf.data[0][0] = 0x01020304;
        let encoded = sbbf.encode();
        // Little-endian: least significant byte first.
        assert_eq!(encoded[0], 0x04);
        assert_eq!(encoded[1], 0x03);
        assert_eq!(encoded[2], 0x02);
        assert_eq!(encoded[3], 0x01);
    }

    #[test]
    fn test_from_hashes_builds_correct_filter() {
        let keys: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma"];
        let hashes: Vec<u64> = keys.iter().map(|k| Sbbf::hash_key(k)).collect();
        let sbbf = Sbbf::from_hashes(&hashes);
        for key in &keys {
            assert!(sbbf.check(key), "key {:?} should be found", key);
        }
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test -p forst-rs-storage -- bloom_filter 2>&1 | tail -10
```
Expected: FAIL — methods `encode`, `decode`, `from_hashes` do not exist yet.

- [ ] **Step 3: Implement encode, decode, and from_hashes**

Add the following methods to the `impl Sbbf` block in `bloom_filter.rs`, after the `check()` method:

```rust
    /// Builds an SBBF from a slice of pre-computed xxHash64 values.
    ///
    /// The filter is automatically sized for ~1% false-positive rate.
    pub fn from_hashes(hashes: &[u64]) -> Self {
        let mut sbbf = Self::new(hashes.len());
        for &h in hashes {
            sbbf.insert_hash(h);
        }
        sbbf
    }

    /// Serializes the SBBF bitset to bytes (little-endian, 32 bytes per block).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.data.len() * 32);
        for block in &self.data {
            for word in block {
                buf.extend_from_slice(&word.to_le_bytes());
            }
        }
        buf
    }

    /// Deserializes an SBBF from bytes produced by [`encode`](Self::encode).
    ///
    /// The byte length must be a positive multiple of 32 (one block = 32 bytes).
    pub fn decode(data: &[u8]) -> ForstResult<Self> {
        if data.is_empty() {
            return Err(forst_rs_common::ForstError::corruption(
                "SBBF data is empty",
            ));
        }
        if data.len() % 32 != 0 {
            return Err(forst_rs_common::ForstError::corruption(format!(
                "SBBF data size {} is not a multiple of 32",
                data.len()
            )));
        }
        let num_blocks = data.len() / 32;
        let mut blocks = Vec::with_capacity(num_blocks);
        for i in 0..num_blocks {
            let offset = i * 32;
            let mut block = [0u32; 8];
            for j in 0..8 {
                let w = offset + j * 4;
                block[j] = u32::from_le_bytes([data[w], data[w + 1], data[w + 2], data[w + 3]]);
            }
            blocks.push(block);
        }
        Ok(Self { data: blocks })
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test -p forst-rs-storage -- bloom_filter 2>&1 | tail -25
```
Expected: all 23 tests pass (15 from Task 3 + 8 new)

- [ ] **Step 5: Commit**

```bash
cd ~/code/github/ForSt && git add crates/forst-rs-storage/src/sst/bloom_filter.rs && git commit -m "feat: add SBBF encode/decode serialization and from_hashes builder"
```

---

## Task 5: Integrate SBBF into SstWriterImpl

**Files:**
- Modify: `crates/forst-rs-storage/src/sst/writer.rs`

- [ ] **Step 1: Write failing tests for bloom filter integration**

Add these tests to the `mod tests` block at the bottom of `writer.rs`, after the existing `test_writer_lz4_compression` test:

```rust
    #[test]
    fn test_writer_bloom_filter_present_in_footer() {
        let mut writer = SstWriterImpl::new();
        for i in 0..10u64 {
            writer
                .add(format!("k{i:03}").as_bytes(), Some(b"v"), i + 1, 0)
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();

        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();

        // Bloom filter should now have non-zero offset and size.
        assert!(
            footer.bloom_filter_offset > FILE_HEADER_SIZE as u64,
            "bloom_filter_offset should be after FileHeader, got {}",
            footer.bloom_filter_offset,
        );
        assert!(
            footer.bloom_filter_size > 0,
            "bloom_filter_size should be > 0"
        );
        // Bloom filter should come before the index section.
        assert!(
            footer.bloom_filter_offset < footer.index_offset,
            "bloom filter should precede index section"
        );
        assert_eq!(
            footer.bloom_filter_offset + footer.bloom_filter_size as u64,
            footer.index_offset,
            "bloom filter end should equal index start"
        );
    }

    #[test]
    fn test_writer_bloom_filter_data_is_valid_sbbf() {
        let mut writer = SstWriterImpl::new();
        let keys: Vec<String> = (0..50).map(|i| format!("key_{:04}", i)).collect();
        for (i, key) in keys.iter().enumerate() {
            writer
                .add(key.as_bytes(), Some(b"val"), i as u64 + 1, 0)
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();

        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();

        // Extract and decode the bloom filter section.
        let bf_start = footer.bloom_filter_offset as usize;
        let bf_end = bf_start + footer.bloom_filter_size as usize;
        let sbbf = crate::sst::bloom_filter::Sbbf::decode(&data[bf_start..bf_end]).unwrap();

        // All inserted keys must be found (no false negatives).
        for key in &keys {
            assert!(
                sbbf.check(key.as_bytes()),
                "bloom filter should find inserted key {:?}",
                key,
            );
        }
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test -p forst-rs-storage -- test_writer_bloom_filter 2>&1 | tail -10
```
Expected: FAIL — bloom_filter_offset is still 0, bloom_filter_size is still 0.

- [ ] **Step 3: Modify SstWriterImpl to collect key hashes**

In `writer.rs`, make these changes:

**3a.** Add the import for the bloom filter module. Add this line after the existing `use super::sparse_index::{encode_index, BlockStats, SparseIndexEntry};` line (line 39):

```rust
use super::bloom_filter::Sbbf;
```

**3b.** Add a `key_hashes` field to the `SstWriterImpl` struct. Add this line after the `last_added_key` field (line 97):

```rust
    /// Collected xxHash64 values of all keys, used to build the bloom filter at finish().
    key_hashes: Vec<u64>,
```

**3c.** Initialize `key_hashes` in both constructors. In `with_options()` (the `Self { ... }` block starting at line 118), add after `last_added_key: None,`:

```rust
            key_hashes: Vec::new(),
```

**3d.** In the `add()` method, compute and store the hash. Add this line right after `self.last_added_key = Some(key.to_vec());` (line 187):

```rust
        self.key_hashes.push(Sbbf::hash_key(key));
```

- [ ] **Step 4: Modify finish() to build and write the bloom filter**

In the `finish()` method, replace the section that writes the Index Section and Footer (lines 222–267) with the following. The key change is: build the SBBF from collected hashes, write it before the Index Section, and set the footer fields.

Replace this block (the comment and code from `// Write Index Section` through to the end of `finish()`):

```rust
        // --- Write Bloom Filter Section ---
        let bloom_filter_offset = self.buf.len() as u64;
        let sbbf = Sbbf::from_hashes(&self.key_hashes);
        let bloom_bytes = sbbf.encode();
        let bloom_filter_size = bloom_bytes.len() as u32;
        self.buf.extend_from_slice(&bloom_bytes);

        // --- Write Index Section ---
        let index_offset = self.buf.len() as u64;
        let index_bytes = encode_index(&self.index_entries, &self.block_stats);
        let index_size = index_bytes.len() as u32;
        self.buf.extend_from_slice(&index_bytes);

        // --- Write Footer ---
        let creation_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let data_block_count = self.index_entries.len() as u32;

        let info = SstFileInfo {
            file_size: 0, // placeholder, updated after footer write
            entry_count: self.total_entries,
            data_block_count,
            min_key: self.global_min_key.unwrap_or_default(),
            max_key: self.global_max_key.unwrap_or_default(),
            min_sequence: self.global_min_sequence,
            max_sequence: self.global_max_sequence,
        };

        let footer = FooterV1 {
            data_block_count,
            total_entries: self.total_entries,
            bloom_filter_offset,
            bloom_filter_size,
            index_offset,
            index_size,
            min_key: info.min_key.to_vec(),
            max_key: info.max_key.to_vec(),
            min_sequence: self.global_min_sequence,
            max_sequence: self.global_max_sequence,
            compression: self.options.compression,
            checksum_type: ChecksumType::Crc32c,
            creation_time,
            format_version: SST_FORMAT_VERSION,
        };
        self.buf.extend_from_slice(&footer.encode());

        let file_size = self.buf.len() as u64;
        let info = SstFileInfo { file_size, ..info };

        Ok((self.buf, info))
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test -p forst-rs-storage -- writer 2>&1 | tail -25
```
Expected: all writer tests pass, including the 2 new bloom filter tests.

- [ ] **Step 6: Run ALL storage tests to verify nothing is broken**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test -p forst-rs-storage 2>&1 | tail -30
```
Expected: all tests pass. Some existing tests that checked `bloom_filter_offset == 0` and `bloom_filter_size == 0` will now fail — see Step 7.

- [ ] **Step 7: Fix the existing test that asserts bloom_filter_offset==0**

The test `test_writer_footer_has_valid_index_offset` in `writer.rs` (around line 409) has these assertions:
```rust
        assert_eq!(footer.bloom_filter_offset, 0);
        assert_eq!(footer.bloom_filter_size, 0);
```

Replace those two lines with:
```rust
        assert!(
            footer.bloom_filter_offset > 0,
            "bloom filter should have non-zero offset"
        );
        assert!(
            footer.bloom_filter_size > 0,
            "bloom filter should have non-zero size"
        );
```

- [ ] **Step 8: Run ALL storage tests again to confirm the fix**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test -p forst-rs-storage 2>&1 | tail -30
```
Expected: ALL tests pass.

- [ ] **Step 9: Commit**

```bash
cd ~/code/github/ForSt && git add crates/forst-rs-storage/src/sst/writer.rs && git commit -m "feat: integrate SBBF bloom filter into SstWriterImpl"
```

---

## Task 6: Integration Test — E2E with Bloom Filter

**Files:**
- Modify: `crates/forst-rs-storage/tests/sst_integration.rs`

- [ ] **Step 1: Add the E2E bloom filter integration test**

Add the following imports to the top of `sst_integration.rs`, merging with the existing import block. Add `Sbbf` to the import from `forst_rs_storage::sst`:

Replace the current import line:
```rust
use forst_rs_storage::sst::{
    decode_data_block, decode_index, search_index, FileHeader, FooterV1, SstWriterImpl,
    SstWriterOptions, FILE_HEADER_SIZE, SST_MAGIC,
};
```

With:
```rust
use forst_rs_storage::sst::{
    decode_data_block, decode_index, search_index, FileHeader, FooterV1, Sbbf, SstWriterImpl,
    SstWriterOptions, FILE_HEADER_SIZE, SST_MAGIC,
};
```

Then add this new test function at the end of the file:

```rust
#[test]
fn test_e2e_bloom_filter_filters_keys() {
    let n = 500;
    let options = SstWriterOptions {
        block_size: 512,
        compression: CompressionType::None,
    };
    let mut writer = SstWriterImpl::with_options(options);

    for i in 0..n {
        let key = format!("bloom_{:05}", i);
        let val = format!("val_{:05}", i);
        writer
            .add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 0)
            .unwrap();
    }

    let (data, info) = writer.finish().unwrap();
    assert_eq!(info.entry_count, n as u64);

    // Parse footer.
    let len = data.len();
    let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
    let footer_start = len - footer_length as usize;
    let footer = FooterV1::decode(&data[footer_start..]).unwrap();

    // Bloom filter section should be non-empty and correctly positioned.
    assert!(footer.bloom_filter_offset > FILE_HEADER_SIZE as u64);
    assert!(footer.bloom_filter_size > 0);
    assert!(footer.bloom_filter_offset < footer.index_offset);

    // Decode the bloom filter.
    let bf_start = footer.bloom_filter_offset as usize;
    let bf_end = bf_start + footer.bloom_filter_size as usize;
    assert!(bf_end <= footer_start, "bloom filter should end before footer");
    let sbbf = Sbbf::decode(&data[bf_start..bf_end]).unwrap();

    // All inserted keys must be found (no false negatives).
    for i in 0..n {
        let key = format!("bloom_{:05}", i);
        assert!(
            sbbf.check(key.as_bytes()),
            "bloom filter must find inserted key {} at index {}",
            key,
            i,
        );
    }

    // Check that the bloom filter correctly rejects most absent keys.
    let mut false_positives = 0;
    let num_absent = 5000;
    for i in 0..num_absent {
        let key = format!("absent_{:08}", i);
        if sbbf.check(key.as_bytes()) {
            false_positives += 1;
        }
    }

    let fpr = false_positives as f64 / num_absent as f64;
    assert!(
        fpr < 0.05,
        "integration FPR {:.4} too high (expected < 5%)",
        fpr,
    );
}

/// Verify that the existing write_and_verify helper still passes with the
/// new bloom filter section present (regression guard).
#[test]
fn test_e2e_500_entries_no_compression_with_bloom() {
    write_and_verify(500, CompressionType::None, 2048);
}
```

- [ ] **Step 2: Run the integration tests**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test -p forst-rs-storage --test sst_integration 2>&1 | tail -20
```
Expected: all integration tests pass, including the new bloom filter test and all existing regression tests.

- [ ] **Step 3: Run the entire workspace test suite**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo test --workspace 2>&1 | tail -30
```
Expected: all tests pass across all crates.

- [ ] **Step 4: Run clippy**

Run:
```bash
cd ~/code/github/ForSt && export PATH="/home/users/lijunqing/.cargo/bin:$PATH" && cargo clippy --workspace -- -D warnings 2>&1 | tail -10
```
Expected: 0 warnings, 0 errors.

- [ ] **Step 5: Commit**

```bash
cd ~/code/github/ForSt && git add crates/forst-rs-storage/tests/sst_integration.rs && git commit -m "test: add end-to-end SBBF bloom filter integration tests"
```

---

## Summary

| Task | What | Files | Tests Added |
|------|------|-------|-------------|
| 1 | Add xxhash-rust dependency | 2 Cargo.toml files | — |
| 2 | SBBF struct + constructor + constants | bloom_filter.rs, mod.rs | 10 |
| 3 | SBBF insert + check (probe) | bloom_filter.rs | 5 |
| 4 | SBBF encode + decode + from_hashes | bloom_filter.rs | 8 |
| 5 | Integrate into SstWriterImpl | writer.rs | 2 |
| 6 | E2E integration tests | sst_integration.rs | 2 |

**Total new tests:** ~27
**Expected total test count after W7:** ~463 (436 existing + 27 new)
