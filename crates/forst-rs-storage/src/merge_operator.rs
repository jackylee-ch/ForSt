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

//! Merge operator trait and built-in implementations.
//!
//! A merge operator combines multiple values for the same key during
//! LSM-tree reads and compaction. The engine accumulates `OpType::Merge`
//! operands from newest to oldest, then invokes [`MergeOperator::full_merge`]
//! when a base `Put` or `Delete` is found, or [`MergeOperator::partial_merge`]
//! to combine adjacent operands at non-bottommost compaction levels.
//!
//! This module provides:
//! - [`MergeOperator`] — the trait that all merge operators implement.
//! - [`ListAppendMergeOperator`] — a built-in operator that concatenates
//!   values with a configurable delimiter (used by Flink `ListState`).
//! - [`RawConcatMergeOperator`] — a built-in operator that concatenates
//!   operands byte-for-byte without a delimiter.
//! - [`NumericAddMergeOperator`] — a built-in operator that sums 8-byte
//!   little-endian `i64` deltas (supports retraction via negative deltas).

use forst_rs_common::{ForstError, ForstResult};

/// A merge operator combines multiple values for the same key.
///
/// Used by the LSM-tree during reads and compaction to resolve
/// `OpType::Merge` entries. The engine accumulates merge operands
/// from newest to oldest, then calls `full_merge` when a base
/// `Put` or `Delete` is found, or `partial_merge` to combine
/// adjacent operands when no base value is available.
pub trait MergeOperator: Send + Sync {
    /// Merge a base value (if any) with a sequence of operands.
    ///
    /// - `key`: the user key being merged
    /// - `base_value`: the existing `Put` value, or `None` if the
    ///   chain starts from a `Delete` or reaches the bottommost level
    /// - `operands`: merge operands ordered from oldest to newest
    ///
    /// Returns the merged result value.
    fn full_merge(
        &self,
        key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> ForstResult<Vec<u8>>;

    /// Combine two adjacent merge operands when no base value is available.
    ///
    /// Called during compaction at non-bottommost levels. If partial merge
    /// is not supported, return an error and the engine will keep both
    /// operands as separate entries.
    fn partial_merge(&self, key: &[u8], left: &[u8], right: &[u8]) -> ForstResult<Vec<u8>>;

    /// Returns the identity of this merge operator (used for validation).
    ///
    /// # Identity contract (R46-L2, R47-H3)
    ///
    /// The returned string is the merge operator's IDENTITY — the
    /// engine's cross-CF homogeneity check (R45-H1 in
    /// `DbImpl::check_cf_homogeneity_locked`) compares operators ONLY by
    /// the value returned here. Implementers MUST treat this name as a
    /// uniqueness contract:
    ///
    /// * Two `MergeOperator` impls that return the same `name()` MUST
    ///   produce semantically-equivalent results from `full_merge` and
    ///   `partial_merge` for every input. If they don't, the engine
    ///   will admit them as "the same operator" across CFs and silently
    ///   produce wrong results during cross-CF L0 compaction.
    /// * Two impls with different semantics (e.g. ListAppend with `,`
    ///   vs ListAppend with `|`) MUST return distinct `name()` values.
    ///   R47-H3 closed this gap on the built-in [`ListAppendMergeOperator`]
    ///   by formatting the delimiter into the returned name.
    ///
    /// `String` (rather than `&str`) lets implementations format
    /// per-instance config without leaking a static buffer.
    fn name(&self) -> String;
}

/// A merge operator that concatenates values with a configurable delimiter.
///
/// Implements list-append semantics used by Flink's `ListState`:
/// - `full_merge("k", Some("a"), ["b", "c"])` produces `"a,b,c"`
/// - `full_merge("k", None, ["x", "y"])` produces `"x,y"`
/// - `partial_merge("k", "a", "b")` produces `"a,b"`
pub struct ListAppendMergeOperator {
    delimiter: u8,
}

impl ListAppendMergeOperator {
    /// Creates a new `ListAppendMergeOperator` with the given delimiter byte.
    pub fn new(delimiter: u8) -> Self {
        Self { delimiter }
    }

    /// Creates a new `ListAppendMergeOperator` with comma (`,`) as delimiter.
    pub fn with_comma() -> Self {
        Self::new(b',')
    }
}

impl MergeOperator for ListAppendMergeOperator {
    fn full_merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> ForstResult<Vec<u8>> {
        // Estimate capacity: base + delimiter+operand for each operand.
        let mut capacity = base_value.map_or(0, |v| v.len());
        for op in operands {
            capacity += 1 + op.len(); // delimiter + operand
        }

        let mut result = Vec::with_capacity(capacity);
        let mut has_segment = false;

        if let Some(base) = base_value {
            result.extend_from_slice(base);
            has_segment = true;
        }

        for op in operands {
            if has_segment {
                result.push(self.delimiter);
            }
            result.extend_from_slice(op);
            has_segment = true;
        }

        Ok(result)
    }

    fn partial_merge(&self, _key: &[u8], left: &[u8], right: &[u8]) -> ForstResult<Vec<u8>> {
        let mut result = Vec::with_capacity(left.len() + 1 + right.len());
        result.extend_from_slice(left);
        result.push(self.delimiter);
        result.extend_from_slice(right);
        Ok(result)
    }

    fn name(&self) -> String {
        // R47-H3: encode the delimiter so two list-append operators with
        // different delimiters are NOT admitted as homogeneous across
        // CFs. The numeric form keeps the identity stable for non-ASCII
        // delimiters (the delimiter is a raw `u8`).
        format!("ListAppendMergeOperator(delim={})", self.delimiter)
    }
}

/// A merge operator that concatenates operands byte-for-byte without a delimiter.
///
/// ForSt-RS ListState append operands are already self-delimiting serialized chunks. This operator
/// keeps the append path as real LSM Merge records without changing the byte stream with an extra
/// separator.
pub struct RawConcatMergeOperator;

impl RawConcatMergeOperator {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RawConcatMergeOperator {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeOperator for RawConcatMergeOperator {
    fn full_merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> ForstResult<Vec<u8>> {
        let total =
            base_value.map_or(0, |v| v.len()) + operands.iter().map(|v| v.len()).sum::<usize>();
        let mut out = Vec::with_capacity(total);
        if let Some(base) = base_value {
            out.extend_from_slice(base);
        }
        for op in operands {
            out.extend_from_slice(op);
        }
        Ok(out)
    }

    fn partial_merge(&self, _key: &[u8], left: &[u8], right: &[u8]) -> ForstResult<Vec<u8>> {
        let mut out = Vec::with_capacity(left.len() + right.len());
        out.extend_from_slice(left);
        out.extend_from_slice(right);
        Ok(out)
    }

    fn name(&self) -> String {
        "RawConcatMergeOperator".to_string()
    }
}

/// A merge operator that sums 8-byte little-endian `i64` deltas.
///
/// Both the base value and every operand are exactly 8 bytes encoding an
/// `i64` in little-endian order. Negative deltas implement retraction:
/// - `full_merge("k", Some(5), [+1, -2])` produces `4`
/// - `full_merge("k", None, [+1, +1, -1])` produces `1`
/// - `partial_merge("k", a, b)` produces `a + b`
///
/// Addition saturates at `i64::MAX` / `i64::MIN` rather than wrapping, and
/// a base or operand whose length is not exactly 8 bytes yields a
/// [`ForstError::Corruption`] (never a panic).
pub struct NumericAddMergeOperator;

impl NumericAddMergeOperator {
    pub fn new() -> Self {
        Self
    }

    /// Decodes an 8-byte little-endian `i64`, or returns a corruption error.
    fn decode_i64(bytes: &[u8]) -> ForstResult<i64> {
        let arr: [u8; 8] = bytes.try_into().map_err(|_| {
            ForstError::corruption(format!(
                "NumericAddMergeOperator: expected 8-byte little-endian i64, got {} bytes",
                bytes.len()
            ))
        })?;
        Ok(i64::from_le_bytes(arr))
    }
}

impl Default for NumericAddMergeOperator {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeOperator for NumericAddMergeOperator {
    fn full_merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> ForstResult<Vec<u8>> {
        let mut sum = match base_value {
            Some(base) => Self::decode_i64(base)?,
            None => 0,
        };
        for op in operands {
            sum = sum.saturating_add(Self::decode_i64(op)?);
        }
        Ok(sum.to_le_bytes().to_vec())
    }

    fn partial_merge(&self, _key: &[u8], left: &[u8], right: &[u8]) -> ForstResult<Vec<u8>> {
        let sum = Self::decode_i64(left)?.saturating_add(Self::decode_i64(right)?);
        Ok(sum.to_le_bytes().to_vec())
    }

    fn name(&self) -> String {
        // Identity contract (R46-L2): this name is compared by the cross-CF
        // homogeneity check — it must stay stable across releases.
        "NumericAddMergeOperator".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- MergeOperator trait tests ---

    #[test]
    fn test_list_append_name() {
        // R47-H3: name encodes the delimiter so two operators with
        // different delimiters are distinct identities.
        let op = ListAppendMergeOperator::with_comma();
        // comma = 0x2C = 44
        assert_eq!(op.name(), "ListAppendMergeOperator(delim=44)");
        let pipe = ListAppendMergeOperator::new(b'|');
        assert_ne!(op.name(), pipe.name());
    }

    #[test]
    fn test_list_append_trait_object() {
        let op: Box<dyn MergeOperator> = Box::new(ListAppendMergeOperator::with_comma());
        assert_eq!(op.name(), "ListAppendMergeOperator(delim=44)");
    }

    // --- full_merge tests ---

    #[test]
    fn test_full_merge_with_base_and_operands() {
        let op = ListAppendMergeOperator::with_comma();
        let result = op.full_merge(b"key", Some(b"a"), &[b"b", b"c"]).unwrap();
        assert_eq!(result, b"a,b,c");
    }

    #[test]
    fn test_full_merge_no_base() {
        let op = ListAppendMergeOperator::with_comma();
        let result = op.full_merge(b"key", None, &[b"x", b"y"]).unwrap();
        assert_eq!(result, b"x,y");
    }

    #[test]
    fn test_full_merge_base_only_no_operands() {
        let op = ListAppendMergeOperator::with_comma();
        let result = op.full_merge(b"key", Some(b"base"), &[]).unwrap();
        assert_eq!(result, b"base");
    }

    #[test]
    fn test_full_merge_single_operand_no_base() {
        let op = ListAppendMergeOperator::with_comma();
        let result = op.full_merge(b"key", None, &[b"only"]).unwrap();
        assert_eq!(result, b"only");
    }

    #[test]
    fn test_full_merge_empty_operands_no_base() {
        let op = ListAppendMergeOperator::with_comma();
        let result = op.full_merge(b"key", None, &[]).unwrap();
        assert_eq!(result, b"");
    }

    #[test]
    fn test_full_merge_empty_value_operands() {
        let op = ListAppendMergeOperator::with_comma();
        let result = op.full_merge(b"key", Some(b""), &[b"", b""]).unwrap();
        assert_eq!(result, b",,");
    }

    #[test]
    fn test_full_merge_many_operands() {
        let op = ListAppendMergeOperator::with_comma();
        let operands: Vec<&[u8]> = (0..100).map(|_| b"v" as &[u8]).collect();
        let result = op.full_merge(b"key", Some(b"base"), &operands).unwrap();
        // "base,v,v,...,v" = "base" + 100 * ",v"
        let expected_len = 4 + 100 * 2; // "base" + 100 * ",v"
        assert_eq!(result.len(), expected_len);
        assert!(result.starts_with(b"base,v"));
    }

    #[test]
    fn test_full_merge_custom_delimiter() {
        let op = ListAppendMergeOperator::new(b'|');
        let result = op.full_merge(b"key", Some(b"a"), &[b"b", b"c"]).unwrap();
        assert_eq!(result, b"a|b|c");
    }

    // --- partial_merge tests ---

    #[test]
    fn test_partial_merge_basic() {
        let op = ListAppendMergeOperator::with_comma();
        let result = op.partial_merge(b"key", b"left", b"right").unwrap();
        assert_eq!(result, b"left,right");
    }

    #[test]
    fn test_partial_merge_empty_left() {
        let op = ListAppendMergeOperator::with_comma();
        let result = op.partial_merge(b"key", b"", b"right").unwrap();
        assert_eq!(result, b",right");
    }

    #[test]
    fn test_partial_merge_empty_right() {
        let op = ListAppendMergeOperator::with_comma();
        let result = op.partial_merge(b"key", b"left", b"").unwrap();
        assert_eq!(result, b"left,");
    }

    #[test]
    fn test_partial_merge_custom_delimiter() {
        let op = ListAppendMergeOperator::new(b'\n');
        let result = op.partial_merge(b"key", b"line1", b"line2").unwrap();
        assert_eq!(result, b"line1\nline2");
    }

    // --- binary data tests ---

    #[test]
    fn test_full_merge_binary_data() {
        let op = ListAppendMergeOperator::new(0xFF);
        let result = op
            .full_merge(b"key", Some(&[0x00, 0x01]), &[&[0x02, 0x03], &[0x04]])
            .unwrap();
        assert_eq!(result, vec![0x00, 0x01, 0xFF, 0x02, 0x03, 0xFF, 0x04]);
    }

    // --- Send + Sync ---

    #[test]
    fn test_merge_operator_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ListAppendMergeOperator>();
    }

    #[test]
    fn test_merge_operator_arc_trait_object() {
        use std::sync::Arc;
        let op: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        let result = op.full_merge(b"k", Some(b"a"), &[b"b"]).unwrap();
        assert_eq!(result, b"a,b");
    }

    #[test]
    fn test_raw_concat_full_merge_preserves_operand_bytes() {
        let op = RawConcatMergeOperator::new();
        let result = op
            .full_merge(b"key", Some(b"BASE"), &[b"\0A", b"B\0"])
            .unwrap();
        assert_eq!(result, b"BASE\0AB\0");
    }

    #[test]
    fn test_raw_concat_partial_merge_has_no_separator() {
        let op = RawConcatMergeOperator::new();
        let result = op.partial_merge(b"key", b"A", b"B").unwrap();
        assert_eq!(result, b"AB");
    }

    // --- NumericAddMergeOperator tests ---

    fn le(v: i64) -> Vec<u8> {
        v.to_le_bytes().to_vec()
    }

    #[test]
    fn test_numeric_add_name_is_stable() {
        let op = NumericAddMergeOperator::new();
        assert_eq!(op.name(), "NumericAddMergeOperator");
    }

    #[test]
    fn test_numeric_add_sums_positive_deltas_no_base() {
        let op = NumericAddMergeOperator::new();
        let (a, b, c) = (le(1), le(2), le(3));
        let result = op.full_merge(b"key", None, &[&a, &b, &c]).unwrap();
        assert_eq!(result, le(6));
    }

    #[test]
    fn test_numeric_add_retraction() {
        let op = NumericAddMergeOperator::new();
        let (p1, p2, m1) = (le(1), le(1), le(-1));
        let result = op.full_merge(b"key", None, &[&p1, &p2, &m1]).unwrap();
        assert_eq!(result, le(1));
    }

    #[test]
    fn test_numeric_add_base_plus_operands() {
        let op = NumericAddMergeOperator::new();
        let base = le(10);
        let (a, b) = (le(5), le(-3));
        let result = op.full_merge(b"key", Some(&base), &[&a, &b]).unwrap();
        assert_eq!(result, le(12));
    }

    #[test]
    fn test_numeric_add_no_operands_returns_base() {
        let op = NumericAddMergeOperator::new();
        let base = le(42);
        let result = op.full_merge(b"key", Some(&base), &[]).unwrap();
        assert_eq!(result, le(42));
    }

    #[test]
    fn test_numeric_add_no_base_no_operands_is_zero() {
        let op = NumericAddMergeOperator::new();
        let result = op.full_merge(b"key", None, &[]).unwrap();
        assert_eq!(result, le(0));
    }

    #[test]
    fn test_numeric_add_saturates_at_max() {
        let op = NumericAddMergeOperator::new();
        let base = le(i64::MAX);
        let one = le(1);
        let result = op.full_merge(b"key", Some(&base), &[&one]).unwrap();
        assert_eq!(result, le(i64::MAX));
    }

    #[test]
    fn test_numeric_add_saturates_at_min() {
        let op = NumericAddMergeOperator::new();
        let base = le(i64::MIN);
        let neg = le(-1);
        let result = op.full_merge(b"key", Some(&base), &[&neg]).unwrap();
        assert_eq!(result, le(i64::MIN));
    }

    #[test]
    fn test_numeric_add_malformed_operand_is_corruption() {
        let op = NumericAddMergeOperator::new();
        let short: &[u8] = b"abc";
        let err = op.full_merge(b"key", None, &[short]).unwrap_err();
        assert!(err.is_corruption(), "expected Corruption, got {err:?}");
    }

    #[test]
    fn test_numeric_add_malformed_base_is_corruption() {
        let op = NumericAddMergeOperator::new();
        let err = op.full_merge(b"key", Some(b"too-long-9"), &[]).unwrap_err();
        assert!(err.is_corruption(), "expected Corruption, got {err:?}");
    }

    #[test]
    fn test_numeric_add_partial_merge_sums() {
        let op = NumericAddMergeOperator::new();
        let result = op.partial_merge(b"key", &le(7), &le(-2)).unwrap();
        assert_eq!(result, le(5));
    }

    #[test]
    fn test_numeric_add_partial_merge_saturates() {
        let op = NumericAddMergeOperator::new();
        let result = op.partial_merge(b"key", &le(i64::MAX), &le(1)).unwrap();
        assert_eq!(result, le(i64::MAX));
    }

    #[test]
    fn test_numeric_add_partial_then_full_matches_full() {
        // Associativity: full(base, [partial(a,b), c]) == full(base, [a, b, c])
        let op = NumericAddMergeOperator::new();
        let base = le(100);
        let (a, b, c) = (le(3), le(-7), le(11));
        let ab = op.partial_merge(b"key", &a, &b).unwrap();
        let combined = op.full_merge(b"key", Some(&base), &[&ab, &c]).unwrap();
        let flat = op.full_merge(b"key", Some(&base), &[&a, &b, &c]).unwrap();
        assert_eq!(combined, flat);
        assert_eq!(combined, le(107));
    }
}
