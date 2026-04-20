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

//! Common assertion helpers for ForSt-RS tests.
//!
//! These helpers provide more informative error messages than raw `assert!`
//! macros when comparing byte slices, key-value pairs, and other common
//! test patterns.

/// Asserts that two byte slices are equal, printing both as lossy UTF-8
/// strings on failure for easier debugging.
///
/// # Panics
///
/// Panics with a descriptive message if the slices differ.
///
/// # Examples
///
/// ```
/// use forst_rs_test_utils::assert_bytes_eq;
///
/// assert_bytes_eq(b"hello", b"hello");
/// ```
pub fn assert_bytes_eq(actual: &[u8], expected: &[u8]) {
    if actual != expected {
        panic!(
            "byte mismatch:\n  actual:   {:?} (\"{}\")\n  expected: {:?} (\"{}\")",
            actual,
            String::from_utf8_lossy(actual),
            expected,
            String::from_utf8_lossy(expected),
        );
    }
}

/// Asserts that a list of key-value pairs is sorted by key in ascending
/// lexicographic order.
///
/// # Panics
///
/// Panics if any key is greater than or equal to the next key, indicating
/// the pairs are not strictly sorted.
///
/// # Examples
///
/// ```
/// use forst_rs_test_utils::assert_kv_pairs_sorted;
///
/// let pairs = vec![
///     (b"a".to_vec(), b"1".to_vec()),
///     (b"b".to_vec(), b"2".to_vec()),
///     (b"c".to_vec(), b"3".to_vec()),
/// ];
/// assert_kv_pairs_sorted(&pairs);
/// ```
pub fn assert_kv_pairs_sorted(pairs: &[(Vec<u8>, Vec<u8>)]) {
    for i in 1..pairs.len() {
        if pairs[i - 1].0 >= pairs[i].0 {
            panic!(
                "KV pairs not sorted at index {}: key[{}]={:?} >= key[{}]={:?}",
                i - 1,
                i - 1,
                String::from_utf8_lossy(&pairs[i - 1].0),
                i,
                String::from_utf8_lossy(&pairs[i].0),
            );
        }
    }
}

/// Asserts that a list of keys is sorted in ascending lexicographic order.
///
/// # Panics
///
/// Panics if any key is greater than or equal to the next key.
pub fn assert_keys_sorted(keys: &[Vec<u8>]) {
    for i in 1..keys.len() {
        if keys[i - 1] >= keys[i] {
            panic!(
                "Keys not sorted at index {}: key[{}]={:?} >= key[{}]={:?}",
                i - 1,
                i - 1,
                String::from_utf8_lossy(&keys[i - 1]),
                i,
                String::from_utf8_lossy(&keys[i]),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assert_bytes_eq_equal() {
        assert_bytes_eq(b"hello", b"hello");
        assert_bytes_eq(b"", b"");
        assert_bytes_eq(&[0, 1, 2], &[0, 1, 2]);
    }

    #[test]
    #[should_panic(expected = "byte mismatch")]
    fn test_assert_bytes_eq_different() {
        assert_bytes_eq(b"hello", b"world");
    }

    #[test]
    #[should_panic(expected = "byte mismatch")]
    fn test_assert_bytes_eq_different_length() {
        assert_bytes_eq(b"hello", b"hell");
    }

    #[test]
    fn test_assert_kv_pairs_sorted_valid() {
        let pairs = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        assert_kv_pairs_sorted(&pairs);
    }

    #[test]
    fn test_assert_kv_pairs_sorted_empty() {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        assert_kv_pairs_sorted(&pairs);
    }

    #[test]
    fn test_assert_kv_pairs_sorted_single() {
        let pairs = vec![(b"only".to_vec(), b"one".to_vec())];
        assert_kv_pairs_sorted(&pairs);
    }

    #[test]
    #[should_panic(expected = "KV pairs not sorted")]
    fn test_assert_kv_pairs_sorted_unsorted() {
        let pairs = vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"a".to_vec(), b"1".to_vec()),
        ];
        assert_kv_pairs_sorted(&pairs);
    }

    #[test]
    #[should_panic(expected = "KV pairs not sorted")]
    fn test_assert_kv_pairs_sorted_duplicate_keys() {
        let pairs = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"a".to_vec(), b"2".to_vec()),
        ];
        assert_kv_pairs_sorted(&pairs);
    }

    #[test]
    fn test_assert_keys_sorted_valid() {
        let keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        assert_keys_sorted(&keys);
    }

    #[test]
    fn test_assert_keys_sorted_empty() {
        let keys: Vec<Vec<u8>> = vec![];
        assert_keys_sorted(&keys);
    }

    #[test]
    #[should_panic(expected = "Keys not sorted")]
    fn test_assert_keys_sorted_unsorted() {
        let keys = vec![b"b".to_vec(), b"a".to_vec()];
        assert_keys_sorted(&keys);
    }
}
