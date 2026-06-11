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
//! - [`NumericAddBeMergeOperator`] — a built-in operator that sums 8-byte
//!   BIG-endian `i64` deltas with WRAPPING two's-complement addition
//!   (byte-equivalent to Java `long +` over `DataOutputSerializer.writeLong`
//!   bytes; supports retraction via negative deltas).

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

/// A merge operator that sums 8-byte **big-endian** `i64` deltas with
/// **wrapping** two's-complement addition (OPT-N04 §4).
///
/// This is the operator for Flink `Long` accumulator state: Flink's
/// `LongSerializer` writes longs via `DataOutputSerializer.writeLong` in
/// network order (MSB first), and Java `long +` wraps on overflow. The
/// little-endian, saturating [`NumericAddMergeOperator`] is the wrong fold
/// for those bytes on two counts:
///
/// 1. **Endianness** — it would sum byte-swapped garbage.
/// 2. **Saturation is not associative near the rails**
///    (`sat(sat(MAX,1),-1) = MAX-1 != sat(sat(MAX,-1),1) = MAX`), so
///    compaction's freedom to `partial_merge` any adjacent operand pair
///    (order-independence) would change results. `wrapping_add` is fully
///    associative and commutative, the exact guarantee Java's wrapping `+`
///    gives the GET→fold→PUT path this operator replaces.
///
/// Semantics:
/// - `full_merge("k", Some(5), [+1, -2])` produces `4`
/// - `full_merge("k", None, [+1, +1, -1])` produces `1` (retraction =
///   negative deltas; no special casing)
/// - `partial_merge("k", a, b)` produces `a.wrapping_add(b)`
/// - a base or operand whose length is not exactly 8 bytes yields a
///   [`ForstError::Corruption`] (never a panic) — same contract as the
///   LE twin.
pub struct NumericAddBeMergeOperator;

impl NumericAddBeMergeOperator {
    pub fn new() -> Self {
        Self
    }

    /// Decodes an 8-byte big-endian `i64`, or returns a corruption error.
    fn decode_i64(bytes: &[u8]) -> ForstResult<i64> {
        let arr: [u8; 8] = bytes.try_into().map_err(|_| {
            ForstError::corruption(format!(
                "NumericAddBeMergeOperator: expected 8-byte big-endian i64, got {} bytes",
                bytes.len()
            ))
        })?;
        Ok(i64::from_be_bytes(arr))
    }
}

impl Default for NumericAddBeMergeOperator {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeOperator for NumericAddBeMergeOperator {
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
            sum = sum.wrapping_add(Self::decode_i64(op)?);
        }
        Ok(sum.to_be_bytes().to_vec())
    }

    fn partial_merge(&self, _key: &[u8], left: &[u8], right: &[u8]) -> ForstResult<Vec<u8>> {
        let sum = Self::decode_i64(left)?.wrapping_add(Self::decode_i64(right)?);
        Ok(sum.to_be_bytes().to_vec())
    }

    fn name(&self) -> String {
        // Identity contract (R46-L2): cross-checkpoint identity — compared
        // by the restore-by-name match and the cross-CF homogeneity check.
        // Fixed string, never versioned-by-config (it has no config) —
        // OPT-N04 §3.3 name-stability requirement.
        "NumericAddBeMergeOperator".to_string()
    }
}

/// THE name registry for built-in merge operators (OPT-N04 E3).
///
/// Resolves a stable operator identity string (the value returned by
/// [`MergeOperator::name`], persisted in checkpoint CF descriptors and
/// passed by name across the FFI) to a fresh operator instance. This is
/// the single source of truth shared by:
/// - `frs_db_create_cf_with_merge` (FFI create path),
/// - the checkpoint restore-by-name arms in `DbImpl::open_from_incremental`
///   (default-CF and non-default-CF descriptors),
/// - `DbImpl::create_cf_from_import_with_merge` (rescale/import path).
///
/// Accepted names:
/// - `"ListAppendMergeOperator"` (legacy alias) and
///   `"ListAppendMergeOperator(delim=44)"` (the R47-H3 identity of
///   [`ListAppendMergeOperator::with_comma`]) — comma list-append.
/// - `"RawConcatMergeOperator"` — byte-for-byte concatenation.
/// - `"NumericAddMergeOperator"` — 8-byte LE i64 saturating sum.
/// - `"NumericAddBeMergeOperator"` — 8-byte BE i64 wrapping sum
///   (Java `long +` equivalent; OPT-N04 §4).
///
/// Returns `None` for any unknown name; callers decide whether that is
/// `InvalidArgument` (create/import) or `Corruption`-adjacent (restore).
pub fn merge_operator_by_name(name: &str) -> Option<std::sync::Arc<dyn MergeOperator>> {
    match name {
        "ListAppendMergeOperator" | "ListAppendMergeOperator(delim=44)" => {
            Some(std::sync::Arc::new(ListAppendMergeOperator::with_comma()))
        }
        "RawConcatMergeOperator" => Some(std::sync::Arc::new(RawConcatMergeOperator::new())),
        "NumericAddMergeOperator" => Some(std::sync::Arc::new(NumericAddMergeOperator::new())),
        "NumericAddBeMergeOperator" => Some(std::sync::Arc::new(NumericAddBeMergeOperator::new())),
        _ => None,
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

    // --- NumericAddBeMergeOperator tests (OPT-N04 §4) ---

    fn be(v: i64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    /// Deterministic xorshift64* PRNG — no test-only deps needed.
    struct XorShift(u64);
    impl XorShift {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
        fn next_i64(&mut self) -> i64 {
            self.next() as i64
        }
        /// Random index in `0..n`.
        fn next_idx(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    #[test]
    fn test_numeric_add_be_name_is_stable() {
        let op = NumericAddBeMergeOperator::new();
        assert_eq!(op.name(), "NumericAddBeMergeOperator");
        // Must be distinct from the LE twin's identity.
        assert_ne!(op.name(), NumericAddMergeOperator::new().name());
    }

    #[test]
    fn test_numeric_add_be_byte_layout_is_big_endian() {
        // Byte-equivalence gate with Flink: DataOutputSerializer.writeLong
        // stores network order (MSB first). 1 + 2 == 3 in BE bytes.
        let op = NumericAddBeMergeOperator::new();
        let (a, b) = (be(1), be(2));
        let result = op.full_merge(b"key", None, &[&a, &b]).unwrap();
        assert_eq!(result, vec![0, 0, 0, 0, 0, 0, 0, 3]);
        // Sanity: BE bytes of 1 are NOT the LE bytes of 1.
        assert_ne!(be(1), le(1));
    }

    #[test]
    fn test_numeric_add_be_sums_positive_deltas_no_base() {
        let op = NumericAddBeMergeOperator::new();
        let (a, b, c) = (be(1), be(2), be(3));
        let result = op.full_merge(b"key", None, &[&a, &b, &c]).unwrap();
        assert_eq!(result, be(6));
    }

    #[test]
    fn test_numeric_add_be_retraction() {
        let op = NumericAddBeMergeOperator::new();
        let (p1, p2, m1) = (be(1), be(1), be(-1));
        let result = op.full_merge(b"key", None, &[&p1, &p2, &m1]).unwrap();
        assert_eq!(result, be(1));
    }

    #[test]
    fn test_numeric_add_be_retraction_below_zero() {
        // Retraction past zero is just a negative i64 — no clamping.
        let op = NumericAddBeMergeOperator::new();
        let base = be(5);
        let m = be(-8);
        let result = op.full_merge(b"key", Some(&base), &[&m]).unwrap();
        assert_eq!(result, be(-3));
    }

    #[test]
    fn test_numeric_add_be_base_plus_operands() {
        let op = NumericAddBeMergeOperator::new();
        let base = be(10);
        let (a, b) = (be(5), be(-3));
        let result = op.full_merge(b"key", Some(&base), &[&a, &b]).unwrap();
        assert_eq!(result, be(12));
    }

    #[test]
    fn test_numeric_add_be_no_operands_returns_base() {
        let op = NumericAddBeMergeOperator::new();
        let base = be(42);
        let result = op.full_merge(b"key", Some(&base), &[]).unwrap();
        assert_eq!(result, be(42));
    }

    #[test]
    fn test_numeric_add_be_no_base_no_operands_is_zero() {
        let op = NumericAddBeMergeOperator::new();
        let result = op.full_merge(b"key", None, &[]).unwrap();
        assert_eq!(result, be(0));
    }

    #[test]
    fn test_numeric_add_be_wraps_at_max() {
        // WRAPPING, not saturating — byte-equivalent to Java `long +`.
        let op = NumericAddBeMergeOperator::new();
        let base = be(i64::MAX);
        let one = be(1);
        let result = op.full_merge(b"key", Some(&base), &[&one]).unwrap();
        assert_eq!(result, be(i64::MIN)); // Long.MAX_VALUE + 1L == Long.MIN_VALUE
    }

    #[test]
    fn test_numeric_add_be_wraps_at_min() {
        let op = NumericAddBeMergeOperator::new();
        let base = be(i64::MIN);
        let neg = be(-1);
        let result = op.full_merge(b"key", Some(&base), &[&neg]).unwrap();
        assert_eq!(result, be(i64::MAX)); // Long.MIN_VALUE - 1L == Long.MAX_VALUE
    }

    #[test]
    fn test_numeric_add_be_partial_merge_sums() {
        let op = NumericAddBeMergeOperator::new();
        let result = op.partial_merge(b"key", &be(7), &be(-2)).unwrap();
        assert_eq!(result, be(5));
    }

    #[test]
    fn test_numeric_add_be_partial_merge_wraps() {
        let op = NumericAddBeMergeOperator::new();
        let result = op.partial_merge(b"key", &be(i64::MAX), &be(1)).unwrap();
        assert_eq!(result, be(i64::MIN));
    }

    #[test]
    fn test_numeric_add_be_malformed_operand_is_corruption() {
        let op = NumericAddBeMergeOperator::new();
        let short: &[u8] = b"abc";
        let err = op.full_merge(b"key", None, &[short]).unwrap_err();
        assert!(err.is_corruption(), "expected Corruption, got {err:?}");
    }

    #[test]
    fn test_numeric_add_be_malformed_base_is_corruption() {
        let op = NumericAddBeMergeOperator::new();
        let err = op.full_merge(b"key", Some(b"too-long-9"), &[]).unwrap_err();
        assert!(err.is_corruption(), "expected Corruption, got {err:?}");
    }

    #[test]
    fn test_numeric_add_be_associativity_at_overflow() {
        // The exact hazard that disqualifies the saturating twin (OPT-N04
        // §4): near the rails, fold order must not matter.
        //   wrap(wrap(MAX, 1), -1) == wrap(MAX, wrap(1, -1)) == MAX
        // whereas sat(sat(MAX,1),-1) = MAX-1 != sat(MAX, sat(1,-1)) = MAX.
        let op = NumericAddBeMergeOperator::new();
        let (max, p1, m1) = (be(i64::MAX), be(1), be(-1));

        // Left-fold via partial_merge: ((MAX + 1) + -1)
        let max_p1 = op.partial_merge(b"k", &max, &p1).unwrap();
        let left = op.partial_merge(b"k", &max_p1, &m1).unwrap();
        // Right-fold via partial_merge: (MAX + (1 + -1))
        let p1_m1 = op.partial_merge(b"k", &p1, &m1).unwrap();
        let right = op.partial_merge(b"k", &max, &p1_m1).unwrap();
        // Flat full_merge.
        let flat = op.full_merge(b"k", Some(&max), &[&p1, &m1]).unwrap();

        assert_eq!(left, right);
        assert_eq!(left, flat);
        assert_eq!(left, be(i64::MAX));

        // Demonstrate the LE/saturating twin really IS order-dependent here
        // (regression tripwire: if it is ever made wrapping, the BE twin is
        // no longer the only safe fold and this doc claim must be revised).
        let sat = NumericAddMergeOperator::new();
        let (smax, sp1, sm1) = (le(i64::MAX), le(1), le(-1));
        let s_left = {
            let t = sat.partial_merge(b"k", &smax, &sp1).unwrap();
            sat.partial_merge(b"k", &t, &sm1).unwrap()
        };
        let s_right = {
            let t = sat.partial_merge(b"k", &sp1, &sm1).unwrap();
            sat.partial_merge(b"k", &smax, &t).unwrap()
        };
        assert_ne!(s_left, s_right, "saturating add must be non-associative at the rail");
    }

    #[test]
    fn test_numeric_add_be_property_full_merge_matches_java_wrapping_fold() {
        // G1 property test: random (base, deltas[]) — full_merge must equal
        // a plain Java-style wrapping left-fold, byte-for-byte (BE).
        let op = NumericAddBeMergeOperator::new();
        let mut rng = XorShift(0x9E3779B97F4A7C15);
        for case in 0..200 {
            let has_base = case % 3 != 0;
            let base_v = rng.next_i64();
            let n = rng.next_idx(17); // 0..=16 operands
            let deltas: Vec<i64> = (0..n).map(|_| rng.next_i64()).collect();

            let mut expected: i64 = if has_base { base_v } else { 0 };
            for d in &deltas {
                expected = expected.wrapping_add(*d);
            }

            let base_bytes = be(base_v);
            let operand_bytes: Vec<Vec<u8>> = deltas.iter().map(|d| be(*d)).collect();
            let operand_refs: Vec<&[u8]> =
                operand_bytes.iter().map(|v| v.as_slice()).collect();
            let result = op
                .full_merge(
                    b"key",
                    if has_base { Some(base_bytes.as_slice()) } else { None },
                    &operand_refs,
                )
                .unwrap();
            assert_eq!(result, be(expected), "case {case} diverged from wrapping fold");
        }
    }

    #[test]
    fn test_numeric_add_be_property_partial_merge_order_independence() {
        // G1 property test: compaction may partial_merge ANY adjacent pair
        // in any order. Repeatedly collapse a random adjacent pair until one
        // operand remains; every collapse order must produce the same bytes
        // as the flat full_merge.
        let op = NumericAddBeMergeOperator::new();
        let mut rng = XorShift(0xDEADBEEFCAFEF00D);
        for case in 0..100 {
            let n = 2 + rng.next_idx(9); // 2..=10 operands
            let deltas: Vec<i64> = (0..n)
                .map(|_| {
                    // Mix extreme values in so wrap-around actually happens.
                    match rng.next_idx(4) {
                        0 => i64::MAX,
                        1 => i64::MIN,
                        _ => rng.next_i64(),
                    }
                })
                .collect();

            let operand_bytes: Vec<Vec<u8>> = deltas.iter().map(|d| be(*d)).collect();
            let operand_refs: Vec<&[u8]> =
                operand_bytes.iter().map(|v| v.as_slice()).collect();
            let flat = op.full_merge(b"key", None, &operand_refs).unwrap();

            // 5 random collapse orders per operand set.
            for _ in 0..5 {
                let mut chain = operand_bytes.clone();
                while chain.len() > 1 {
                    let i = rng.next_idx(chain.len() - 1);
                    let merged = op.partial_merge(b"key", &chain[i], &chain[i + 1]).unwrap();
                    chain[i] = merged;
                    chain.remove(i + 1);
                }
                let collapsed_ref: &[u8] = &chain[0];
                let via_partial = op.full_merge(b"key", None, &[collapsed_ref]).unwrap();
                assert_eq!(
                    via_partial, flat,
                    "case {case}: partial_merge collapse order changed the result"
                );
            }
        }
    }

    #[test]
    fn test_numeric_add_be_property_commutativity() {
        // Shuffle-invariance of the operand SET (commutativity + assoc):
        // any permutation of the deltas full_merges to the same bytes.
        let op = NumericAddBeMergeOperator::new();
        let mut rng = XorShift(0x123456789ABCDEF1);
        for case in 0..100 {
            let n = 2 + rng.next_idx(9);
            let mut deltas: Vec<i64> = (0..n).map(|_| rng.next_i64()).collect();
            let bytes = |ds: &[i64]| -> Vec<Vec<u8>> { ds.iter().map(|d| be(*d)).collect() };
            let merge = |obs: &[Vec<u8>]| -> Vec<u8> {
                let refs: Vec<&[u8]> = obs.iter().map(|v| v.as_slice()).collect();
                op.full_merge(b"key", None, &refs).unwrap()
            };
            let baseline = merge(&bytes(&deltas));
            // Fisher-Yates shuffle, 3 permutations.
            for _ in 0..3 {
                for i in (1..deltas.len()).rev() {
                    let j = rng.next_idx(i + 1);
                    deltas.swap(i, j);
                }
                assert_eq!(merge(&bytes(&deltas)), baseline, "case {case}: permutation diverged");
            }
        }
    }
}
