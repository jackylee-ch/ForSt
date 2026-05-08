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

//! Common type definitions for ForSt-RS.
//!
//! This module defines the fundamental types used throughout ForSt-RS,
//! matching the semantics of RocksDB's internal representations:
//!
//! - [`OpType`] — Operation types (Put, Delete, SingleDelete, Merge)
//! - [`FileNumber`] — SST/WAL file identifiers
//! - [`SequenceNumber`] — MVCC sequence numbers (56-bit, upper bits of packed u64)
//! - [`ColumnFamilyId`] — Column family identifiers
//! - [`CompressionType`] — Supported compression algorithms
//! - [`Level`] — LSM-tree level index
//! - [`InternalKey`] — Packed user_key + sequence + op_type
//! - [`KeyRange`] — Represents a key range \[start, end)

use std::cmp::Ordering;
use std::fmt;

use crate::error::{ForstError, ForstResult};

// ---------------------------------------------------------------------------
// OpType
// ---------------------------------------------------------------------------

/// Operation types matching RocksDB's `ValueType`.
///
/// Each write operation in the LSM-tree is tagged with an `OpType` that
/// determines how the engine processes the key-value pair during compaction
/// and reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OpType {
    /// Standard key-value insertion.
    Put = 0,
    /// Tombstone marker — deletes all prior versions of the key.
    Delete = 1,
    /// Optimised delete that assumes at most one prior `Put` exists.
    SingleDelete = 2,
    /// Merge operand — combined with existing value via a user-defined merge
    /// operator during compaction or read.
    Merge = 3,
}

impl fmt::Display for OpType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpType::Put => write!(f, "Put"),
            OpType::Delete => write!(f, "Delete"),
            OpType::SingleDelete => write!(f, "SingleDelete"),
            OpType::Merge => write!(f, "Merge"),
        }
    }
}

impl OpType {
    /// Try to convert a raw `u8` into an [`OpType`].
    ///
    /// Returns `None` if the value does not correspond to a known variant.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(OpType::Put),
            1 => Some(OpType::Delete),
            2 => Some(OpType::SingleDelete),
            3 => Some(OpType::Merge),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// FileNumber
// ---------------------------------------------------------------------------

/// Newtype wrapper for SST and WAL file identifiers.
///
/// File numbers are monotonically increasing and uniquely identify every file
/// created by the storage engine.  The [`Display`](fmt::Display) implementation
/// zero-pads to 6 digits (e.g. `000042`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileNumber(pub u64);

impl fmt::Display for FileNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:06}", self.0)
    }
}

impl FileNumber {
    /// Returns the raw `u64` value.
    #[inline]
    pub fn value(self) -> u64 {
        self.0
    }
}

// ---------------------------------------------------------------------------
// SequenceNumber
// ---------------------------------------------------------------------------

/// Newtype wrapper for MVCC sequence numbers.
///
/// RocksDB packs the sequence number into the upper 56 bits of a 64-bit
/// internal key tag (the low 8 bits hold an `OpType` discriminant). The
/// maximum representable sequence number is therefore [`MAX_SEQUENCE_NUMBER`]
/// (`u64::MAX >> 8`).
///
/// # Invariant — and how it is (only loosely) enforced
///
/// The 56-bit invariant is **not** enforced at construction time, because
/// the inner `u64` field is `pub` for ergonomic literal construction
/// (`SequenceNumber(42)`). Use [`Self::try_new`] for a checked constructor
/// when accepting untrusted input. Internal-key construction
/// ([`InternalKey::new`]) carries a `debug_assert!` that traps out-of-range
/// values in debug builds; release builds rely on caller diligence (matches
/// RocksDB's analogous `kMaxSequenceNumber` convention).
///
/// # R-loop r2 H#1 (recorded 2026-05-08)
///
/// A reviewer flagged the public-field/no-assert combination as a type-
/// invariant correctness concern: a `SequenceNumber(u64::MAX)` shifted left
/// by 8 collides with the OpType byte and silently corrupts the packed tag.
/// The packing happens in `forst-rs-storage` (downstream), which trusts the
/// invariant. The mitigation here adds (1) a `try_new` checked constructor,
/// (2) a `debug_assert!` in `InternalKey::new`, and (3) explicit doc on the
/// invariant + the open `pub` field. Making the field private is deferred
/// as a workspace-wide breaking change (would touch ~20 call sites in tests
/// and `forst-rs-engine`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SequenceNumber(pub u64);

/// The largest valid sequence number (56-bit range).
pub const MAX_SEQUENCE_NUMBER: SequenceNumber = SequenceNumber(u64::MAX >> 8);

impl fmt::Display for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl SequenceNumber {
    /// Returns the raw `u64` value.
    #[inline]
    pub fn value(self) -> u64 {
        self.0
    }

    /// Checked constructor: returns `Err` if `value > MAX_SEQUENCE_NUMBER`.
    ///
    /// Prefer this over the open `pub` field when accepting an untrusted
    /// `u64` (e.g. from FFI or on-disk decode), since constructing a value
    /// outside the 56-bit range corrupts the packed tag downstream.
    /// (R-loop r2 H#1, 2026-05-08.)
    #[inline]
    pub fn try_new(value: u64) -> ForstResult<Self> {
        if value > MAX_SEQUENCE_NUMBER.0 {
            return Err(ForstError::invalid_argument(format!(
                "sequence number {} exceeds 56-bit limit ({})",
                value, MAX_SEQUENCE_NUMBER.0
            )));
        }
        Ok(SequenceNumber(value))
    }
}

// ---------------------------------------------------------------------------
// ColumnFamilyId
// ---------------------------------------------------------------------------

/// Newtype wrapper for column family identifiers.
///
/// Column families partition the key-space within a single database instance.
/// The default column family always has id `0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnFamilyId(pub u32);

/// The id of the default column family.
pub const DEFAULT_CF_ID: ColumnFamilyId = ColumnFamilyId(0);

impl fmt::Display for ColumnFamilyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ColumnFamilyId {
    /// Returns the raw `u32` value.
    #[inline]
    pub fn value(self) -> u32 {
        self.0
    }
}

// ---------------------------------------------------------------------------
// CompressionType
// ---------------------------------------------------------------------------

/// Supported compression algorithms for SST data blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    /// No compression.
    None = 0,
    /// LZ4 — fast compression with moderate ratio.
    Lz4 = 1,
    /// Zstandard — higher ratio at the cost of CPU.
    Zstd = 2,
}

impl Default for CompressionType {
    /// The default compression algorithm is [`CompressionType::Lz4`].
    fn default() -> Self {
        CompressionType::Lz4
    }
}

impl fmt::Display for CompressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionType::None => write!(f, "None"),
            CompressionType::Lz4 => write!(f, "LZ4"),
            CompressionType::Zstd => write!(f, "Zstd"),
        }
    }
}

// ---------------------------------------------------------------------------
// Level
// ---------------------------------------------------------------------------

/// Newtype wrapper for an LSM-tree level index.
///
/// Level 0 is the memtable flush target; higher levels contain
/// progressively larger, sorted runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Level(pub u8);

/// Maximum number of LSM-tree levels supported by the engine.
pub const MAX_LEVELS: usize = 7;

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "L{}", self.0)
    }
}

impl Level {
    /// Returns the raw `u8` value.
    #[inline]
    pub fn value(self) -> u8 {
        self.0
    }

    /// Checked constructor: returns `Err` if `value >= MAX_LEVELS`.
    ///
    /// Prefer this over the open `pub` field when accepting an untrusted
    /// `u8` (e.g. from FFI or on-disk decode); `Level(value)` with
    /// `value >= MAX_LEVELS` would panic on downstream
    /// `levels[level.value() as usize]` indexing.
    /// (R-loop r4 H#2, 2026-05-08; mirrors `SequenceNumber::try_new`.)
    #[inline]
    pub fn try_new(value: u8) -> ForstResult<Self> {
        if (value as usize) >= MAX_LEVELS {
            return Err(ForstError::invalid_argument(format!(
                "level {} exceeds MAX_LEVELS ({})",
                value, MAX_LEVELS
            )));
        }
        Ok(Level(value))
    }
}

// ---------------------------------------------------------------------------
// InternalKey
// ---------------------------------------------------------------------------

/// A packed internal key consisting of a user key, a sequence number, and an
/// operation type.
///
/// ## Ordering
///
/// Internal keys are ordered by:
/// 1. **user_key** — ascending (lexicographic byte order)
/// 2. **sequence** — descending (newer entries first)
/// 3. **op_type** — ascending (by discriminant)
///
/// This ordering ensures that for a given user key the most recent version is
/// encountered first during iteration and compaction.
#[derive(Debug, Clone)]
pub struct InternalKey {
    user_key: Vec<u8>,
    sequence: SequenceNumber,
    op_type: OpType,
}

impl InternalKey {
    /// Creates a new [`InternalKey`].
    ///
    /// # Debug-only check
    ///
    /// Carries a `debug_assert!` that `sequence <= MAX_SEQUENCE_NUMBER`
    /// (the 56-bit limit imposed by RocksDB's tag-packing convention).
    /// In release builds the check is elided; callers handling untrusted
    /// `u64` values should use [`SequenceNumber::try_new`] beforehand.
    /// (R-loop r2 H#1, 2026-05-08.)
    pub fn new(user_key: Vec<u8>, sequence: SequenceNumber, op_type: OpType) -> Self {
        debug_assert!(
            sequence.0 <= MAX_SEQUENCE_NUMBER.0,
            "InternalKey: sequence {} exceeds 56-bit MAX_SEQUENCE_NUMBER {}; \
             out-of-range values silently corrupt the packed (seq << 8) | op_type tag downstream",
            sequence.0,
            MAX_SEQUENCE_NUMBER.0
        );
        Self {
            user_key,
            sequence,
            op_type,
        }
    }

    /// Returns a reference to the user key bytes.
    #[inline]
    pub fn user_key(&self) -> &[u8] {
        &self.user_key
    }

    /// Returns the sequence number.
    #[inline]
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// Returns the operation type.
    #[inline]
    pub fn op_type(&self) -> OpType {
        self.op_type
    }
}

impl PartialEq for InternalKey {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key
            && self.sequence == other.sequence
            && self.op_type == other.op_type
    }
}

impl Eq for InternalKey {}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // 1. user_key ascending
        self.user_key
            .cmp(&other.user_key)
            // 2. sequence descending (reverse)
            .then_with(|| other.sequence.cmp(&self.sequence))
            // 3. op_type ascending (by discriminant)
            .then_with(|| (self.op_type as u8).cmp(&(other.op_type as u8)))
    }
}

impl fmt::Display for InternalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display user_key as hex, followed by seq and op_type.
        for byte in &self.user_key {
            write!(f, "{:02x}", byte)?;
        }
        write!(f, " @ {} ({})", self.sequence, self.op_type)
    }
}

// ---------------------------------------------------------------------------
// KeyRange
// ---------------------------------------------------------------------------

/// Represents a half-open key range `[start, end)`.
///
/// An empty range is one where `start >= end` (lexicographically), or where
/// `end` is empty (the empty byte slice sorts before all non-empty slices).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    /// Inclusive lower bound.
    pub start: Vec<u8>,
    /// Exclusive upper bound.
    pub end: Vec<u8>,
}

impl KeyRange {
    /// Creates a new [`KeyRange`] with the given `start` (inclusive) and `end`
    /// (exclusive) bounds.
    pub fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        Self { start, end }
    }

    /// Returns `true` if `key` falls within `[start, end)`.
    pub fn contains(&self, key: &[u8]) -> bool {
        if self.is_empty() {
            return false;
        }
        key >= self.start.as_slice() && key < self.end.as_slice()
    }

    /// Returns `true` if this range overlaps with `other`.
    ///
    /// Two half-open ranges `[a, b)` and `[c, d)` overlap iff `a < d && c < b`.
    pub fn overlaps(&self, other: &KeyRange) -> bool {
        if self.is_empty() || other.is_empty() {
            return false;
        }
        self.start < other.end && other.start < self.end
    }

    /// Returns `true` if the range is empty (`start >= end`).
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

impl fmt::Display for KeyRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for b in &self.start {
            write!(f, "{:02x}", b)?;
        }
        write!(f, ", ")?;
        for b in &self.end {
            write!(f, "{:02x}", b)?;
        }
        write!(f, ")")
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // OpType tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_type_repr_values() {
        assert_eq!(OpType::Put as u8, 0);
        assert_eq!(OpType::Delete as u8, 1);
        assert_eq!(OpType::SingleDelete as u8, 2);
        assert_eq!(OpType::Merge as u8, 3);
    }

    #[test]
    fn test_op_type_from_u8_valid() {
        assert_eq!(OpType::from_u8(0), Some(OpType::Put));
        assert_eq!(OpType::from_u8(1), Some(OpType::Delete));
        assert_eq!(OpType::from_u8(2), Some(OpType::SingleDelete));
        assert_eq!(OpType::from_u8(3), Some(OpType::Merge));
    }

    #[test]
    fn test_op_type_from_u8_invalid() {
        assert_eq!(OpType::from_u8(4), None);
        assert_eq!(OpType::from_u8(255), None);
    }

    #[test]
    fn test_op_type_display() {
        assert_eq!(format!("{}", OpType::Put), "Put");
        assert_eq!(format!("{}", OpType::Delete), "Delete");
        assert_eq!(format!("{}", OpType::SingleDelete), "SingleDelete");
        assert_eq!(format!("{}", OpType::Merge), "Merge");
    }

    // -----------------------------------------------------------------------
    // FileNumber tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_file_number_display_zero_padded() {
        assert_eq!(format!("{}", FileNumber(0)), "000000");
        assert_eq!(format!("{}", FileNumber(1)), "000001");
        assert_eq!(format!("{}", FileNumber(42)), "000042");
        assert_eq!(format!("{}", FileNumber(999999)), "999999");
        // Numbers > 6 digits should not be truncated.
        assert_eq!(format!("{}", FileNumber(1_000_000)), "1000000");
    }

    #[test]
    fn test_file_number_ordering() {
        assert!(FileNumber(1) < FileNumber(2));
        assert!(FileNumber(100) > FileNumber(99));
        assert_eq!(FileNumber(5), FileNumber(5));
    }

    #[test]
    fn test_file_number_value() {
        assert_eq!(FileNumber(42).value(), 42);
    }

    // -----------------------------------------------------------------------
    // SequenceNumber tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_sequence_number_max_constant() {
        // Upper 56 bits: u64::MAX >> 8 = 0x00FF_FFFF_FFFF_FFFF
        assert_eq!(MAX_SEQUENCE_NUMBER.0, 0x00FF_FFFF_FFFF_FFFF);
        assert_eq!(MAX_SEQUENCE_NUMBER.0, (1u64 << 56) - 1);
    }

    #[test]
    fn test_sequence_number_ordering() {
        assert!(SequenceNumber(1) < SequenceNumber(2));
        assert!(SequenceNumber(100) > SequenceNumber(0));
    }

    #[test]
    fn test_sequence_number_display() {
        assert_eq!(format!("{}", SequenceNumber(12345)), "12345");
    }

    /// Regression test for R-loop r2 H#1: `SequenceNumber::try_new`
    /// rejects values exceeding the 56-bit MAX.
    #[test]
    fn test_sequence_number_try_new_in_range() {
        let s = SequenceNumber::try_new(0).unwrap();
        assert_eq!(s, SequenceNumber(0));
        let s = SequenceNumber::try_new(MAX_SEQUENCE_NUMBER.0).unwrap();
        assert_eq!(s, MAX_SEQUENCE_NUMBER);
    }

    #[test]
    fn test_sequence_number_try_new_rejects_above_56_bits() {
        let res = SequenceNumber::try_new(MAX_SEQUENCE_NUMBER.0 + 1);
        assert!(res.is_err(), "expected error, got {:?}", res);
        let err = res.unwrap_err();
        assert!(
            err.is_invalid_argument(),
            "expected InvalidArgument variant, got {:?}",
            err
        );
        let res = SequenceNumber::try_new(u64::MAX);
        assert!(res.is_err(), "u64::MAX must be rejected");
    }

    /// Regression test for R-loop r2 H#1: `InternalKey::new` debug-asserts
    /// that the sequence is within the 56-bit limit. Release builds elide
    /// this check; debug builds (where `cargo test` runs) catch the misuse.
    #[test]
    #[should_panic(expected = "exceeds 56-bit MAX_SEQUENCE_NUMBER")]
    fn test_internal_key_new_panics_on_oversize_sequence_in_debug() {
        // Release builds will NOT panic — this test only fires in debug.
        let _ = InternalKey::new(b"k".to_vec(), SequenceNumber(u64::MAX), OpType::Put);
    }

    /// Regression test for R-loop r4 H#2: `Level::try_new` rejects values
    /// `>= MAX_LEVELS` (which would panic on downstream level-array indexing).
    #[test]
    fn test_level_try_new_in_range() {
        for v in 0..MAX_LEVELS as u8 {
            let lvl = Level::try_new(v).unwrap();
            assert_eq!(lvl, Level(v));
        }
    }

    #[test]
    fn test_level_try_new_rejects_out_of_range() {
        let res = Level::try_new(MAX_LEVELS as u8);
        assert!(res.is_err(), "Level({}) should be rejected", MAX_LEVELS);
        let err = res.unwrap_err();
        assert!(err.is_invalid_argument());
        let res = Level::try_new(255);
        assert!(res.is_err(), "Level(255) should be rejected");
    }

    // -----------------------------------------------------------------------
    // ColumnFamilyId tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_default_cf_id() {
        assert_eq!(DEFAULT_CF_ID, ColumnFamilyId(0));
    }

    #[test]
    fn test_column_family_id_display() {
        assert_eq!(format!("{}", ColumnFamilyId(7)), "7");
    }

    // -----------------------------------------------------------------------
    // CompressionType tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_compression_type_default_is_lz4() {
        assert_eq!(CompressionType::default(), CompressionType::Lz4);
    }

    #[test]
    fn test_compression_type_repr_values() {
        assert_eq!(CompressionType::None as u8, 0);
        assert_eq!(CompressionType::Lz4 as u8, 1);
        assert_eq!(CompressionType::Zstd as u8, 2);
    }

    #[test]
    fn test_compression_type_display() {
        assert_eq!(format!("{}", CompressionType::None), "None");
        assert_eq!(format!("{}", CompressionType::Lz4), "LZ4");
        assert_eq!(format!("{}", CompressionType::Zstd), "Zstd");
    }

    // -----------------------------------------------------------------------
    // Level tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_level_display() {
        assert_eq!(format!("{}", Level(0)), "L0");
        assert_eq!(format!("{}", Level(6)), "L6");
    }

    #[test]
    fn test_level_ordering() {
        assert!(Level(0) < Level(1));
        assert!(Level(6) > Level(3));
    }

    #[test]
    fn test_max_levels_constant() {
        assert_eq!(MAX_LEVELS, 7);
    }

    // -----------------------------------------------------------------------
    // InternalKey tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_internal_key_accessors() {
        let key = InternalKey::new(b"hello".to_vec(), SequenceNumber(100), OpType::Put);
        assert_eq!(key.user_key(), b"hello");
        assert_eq!(key.sequence(), SequenceNumber(100));
        assert_eq!(key.op_type(), OpType::Put);
    }

    #[test]
    fn test_internal_key_ordering_user_key_ascending() {
        let a = InternalKey::new(b"aaa".to_vec(), SequenceNumber(1), OpType::Put);
        let b = InternalKey::new(b"bbb".to_vec(), SequenceNumber(1), OpType::Put);
        assert!(a < b);
    }

    #[test]
    fn test_internal_key_ordering_sequence_descending() {
        // Same user_key — higher sequence number should sort first (smaller).
        let newer = InternalKey::new(b"key".to_vec(), SequenceNumber(100), OpType::Put);
        let older = InternalKey::new(b"key".to_vec(), SequenceNumber(1), OpType::Put);
        assert!(
            newer < older,
            "newer (higher seq) should sort before older (lower seq)"
        );
    }

    #[test]
    fn test_internal_key_ordering_op_type_tiebreak() {
        // Same user_key and sequence — op_type ascending by discriminant.
        let put = InternalKey::new(b"key".to_vec(), SequenceNumber(1), OpType::Put);
        let del = InternalKey::new(b"key".to_vec(), SequenceNumber(1), OpType::Delete);
        assert!(put < del);
    }

    #[test]
    fn test_internal_key_equality() {
        let a = InternalKey::new(b"key".to_vec(), SequenceNumber(5), OpType::Merge);
        let b = InternalKey::new(b"key".to_vec(), SequenceNumber(5), OpType::Merge);
        assert_eq!(a, b);
    }

    #[test]
    fn test_internal_key_display() {
        let key = InternalKey::new(b"\x01\xab".to_vec(), SequenceNumber(42), OpType::Delete);
        assert_eq!(format!("{}", key), "01ab @ 42 (Delete)");
    }

    // -----------------------------------------------------------------------
    // KeyRange tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_key_range_contains() {
        let range = KeyRange::new(b"b".to_vec(), b"d".to_vec());
        assert!(!range.contains(b"a"));
        assert!(range.contains(b"b")); // inclusive start
        assert!(range.contains(b"c"));
        assert!(!range.contains(b"d")); // exclusive end
        assert!(!range.contains(b"e"));
    }

    #[test]
    fn test_key_range_overlaps() {
        let r1 = KeyRange::new(b"a".to_vec(), b"d".to_vec());
        let r2 = KeyRange::new(b"c".to_vec(), b"f".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));
    }

    #[test]
    fn test_key_range_no_overlap_adjacent() {
        // [a, c) and [c, f) do NOT overlap — they are adjacent.
        let r1 = KeyRange::new(b"a".to_vec(), b"c".to_vec());
        let r2 = KeyRange::new(b"c".to_vec(), b"f".to_vec());
        assert!(!r1.overlaps(&r2));
        assert!(!r2.overlaps(&r1));
    }

    #[test]
    fn test_key_range_is_empty() {
        // start == end
        let empty = KeyRange::new(b"x".to_vec(), b"x".to_vec());
        assert!(empty.is_empty());

        // start > end
        let inverted = KeyRange::new(b"z".to_vec(), b"a".to_vec());
        assert!(inverted.is_empty());

        // valid range
        let valid = KeyRange::new(b"a".to_vec(), b"z".to_vec());
        assert!(!valid.is_empty());
    }

    #[test]
    fn test_key_range_empty_contains_nothing() {
        let empty = KeyRange::new(b"x".to_vec(), b"x".to_vec());
        assert!(!empty.contains(b"x"));
        assert!(!empty.contains(b"a"));
    }

    #[test]
    fn test_key_range_empty_overlaps_nothing() {
        let empty = KeyRange::new(b"c".to_vec(), b"c".to_vec());
        let valid = KeyRange::new(b"a".to_vec(), b"z".to_vec());
        assert!(!empty.overlaps(&valid));
        assert!(!valid.overlaps(&empty));
    }

    #[test]
    fn test_key_range_display() {
        let range = KeyRange::new(b"\x00\xff".to_vec(), b"\x01\x00".to_vec());
        assert_eq!(format!("{}", range), "[00ff, 0100)");
    }
}
