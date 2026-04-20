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

//! Key-value data generators for tests.
//!
//! Provides deterministic data generation helpers that produce reproducible
//! test datasets for unit and integration tests involving KV stores, SST
//! files, and memtables.

/// Generates `count` key-value pairs with a given key prefix and value size.
///
/// Keys are formatted as `{key_prefix}{i:08}` (zero-padded to 8 digits)
/// to ensure lexicographic ordering matches numeric ordering.
///
/// Values are filled with a repeating byte pattern derived from the key
/// index, making it easy to verify data integrity without storing expected
/// values separately.
///
/// # Examples
///
/// ```
/// use forst_rs_test_utils::generate_kv_pairs;
///
/// let pairs = generate_kv_pairs(3, "key_", 16);
/// assert_eq!(pairs.len(), 3);
/// assert_eq!(&pairs[0].0, b"key_00000000");
/// assert_eq!(&pairs[1].0, b"key_00000001");
/// assert_eq!(pairs[0].1.len(), 16);
/// ```
pub fn generate_kv_pairs(count: usize, key_prefix: &str, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = format!("{}{:08}", key_prefix, i).into_bytes();
            let fill_byte = (i % 256) as u8;
            let value = vec![fill_byte; value_size];
            (key, value)
        })
        .collect()
}

/// Generates `count` sequential keys with zero-padded numeric suffixes.
///
/// Keys are formatted as `key{i:08}` (e.g., `key00000000`, `key00000001`).
/// This is useful for testing sorted iteration, range scans, and binary
/// search over key spaces.
///
/// # Examples
///
/// ```
/// use forst_rs_test_utils::generate_sequential_keys;
///
/// let keys = generate_sequential_keys(3);
/// assert_eq!(&keys[0], b"key00000000");
/// assert_eq!(&keys[1], b"key00000001");
/// assert_eq!(&keys[2], b"key00000002");
/// ```
pub fn generate_sequential_keys(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("key{:08}", i).into_bytes())
        .collect()
}

/// Generates a single key-value pair for a given index.
///
/// Useful when you need individual entries rather than a full batch.
/// The format matches [`generate_kv_pairs`] for consistency.
pub fn generate_kv_pair(index: usize, key_prefix: &str, value_size: usize) -> (Vec<u8>, Vec<u8>) {
    let key = format!("{}{:08}", key_prefix, index).into_bytes();
    let fill_byte = (index % 256) as u8;
    let value = vec![fill_byte; value_size];
    (key, value)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_kv_pairs_count() {
        let pairs = generate_kv_pairs(10, "k_", 32);
        assert_eq!(pairs.len(), 10);
    }

    #[test]
    fn test_generate_kv_pairs_key_format() {
        let pairs = generate_kv_pairs(3, "prefix_", 8);
        assert_eq!(&pairs[0].0, b"prefix_00000000");
        assert_eq!(&pairs[1].0, b"prefix_00000001");
        assert_eq!(&pairs[2].0, b"prefix_00000002");
    }

    #[test]
    fn test_generate_kv_pairs_value_size() {
        let pairs = generate_kv_pairs(5, "k", 100);
        for (_, v) in &pairs {
            assert_eq!(v.len(), 100);
        }
    }

    #[test]
    fn test_generate_kv_pairs_value_content() {
        let pairs = generate_kv_pairs(3, "k", 4);
        // Index 0 → fill byte 0
        assert_eq!(pairs[0].1, vec![0u8; 4]);
        // Index 1 → fill byte 1
        assert_eq!(pairs[1].1, vec![1u8; 4]);
        // Index 2 → fill byte 2
        assert_eq!(pairs[2].1, vec![2u8; 4]);
    }

    #[test]
    fn test_generate_kv_pairs_value_wraps_at_256() {
        let pairs = generate_kv_pairs(257, "k", 1);
        assert_eq!(pairs[256].1, vec![0u8; 1]); // 256 % 256 == 0
    }

    #[test]
    fn test_generate_kv_pairs_keys_are_sorted() {
        let pairs = generate_kv_pairs(100, "key_", 16);
        for i in 1..pairs.len() {
            assert!(pairs[i - 1].0 < pairs[i].0, "keys should be sorted");
        }
    }

    #[test]
    fn test_generate_kv_pairs_empty() {
        let pairs = generate_kv_pairs(0, "k", 8);
        assert!(pairs.is_empty());
    }

    #[test]
    fn test_generate_sequential_keys_count() {
        let keys = generate_sequential_keys(5);
        assert_eq!(keys.len(), 5);
    }

    #[test]
    fn test_generate_sequential_keys_format() {
        let keys = generate_sequential_keys(3);
        assert_eq!(&keys[0], b"key00000000");
        assert_eq!(&keys[1], b"key00000001");
        assert_eq!(&keys[2], b"key00000002");
    }

    #[test]
    fn test_generate_sequential_keys_sorted() {
        let keys = generate_sequential_keys(100);
        for i in 1..keys.len() {
            assert!(keys[i - 1] < keys[i], "keys should be sorted");
        }
    }

    #[test]
    fn test_generate_sequential_keys_empty() {
        let keys = generate_sequential_keys(0);
        assert!(keys.is_empty());
    }

    #[test]
    fn test_generate_kv_pair_single() {
        let (key, value) = generate_kv_pair(42, "test_", 10);
        assert_eq!(&key, b"test_00000042");
        assert_eq!(value.len(), 10);
        assert_eq!(value[0], 42u8);
    }

    #[test]
    fn test_generate_kv_pair_matches_batch() {
        let pairs = generate_kv_pairs(5, "k_", 16);
        for (i, pair) in pairs.iter().enumerate() {
            let (key, value) = generate_kv_pair(i, "k_", 16);
            assert_eq!(key, pair.0);
            assert_eq!(value, pair.1);
        }
    }
}
