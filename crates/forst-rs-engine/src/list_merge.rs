// SPDX-License-Identifier: Apache-2.0
//! List-append combiner for ListState's APPEND_MERGE primitive.
//!
//! Per umbrella spec §1 §a: ListState is the only state-type where the
//! combiner is fixed (byte concatenation), runs in the engine without
//! a JVM upcall, and is idempotent under any partial-merge schedule.
//! Reducing/Aggregating intentionally do NOT use this path — they
//! compose GET+combine+PUT via the RMW cache.

/// Stateless combiner. Takes a sequence of operands (each is a serialized
/// list element OR a previously folded multi-element blob) and returns
/// their byte-level concatenation.
///
/// The engine invokes `combine` at:
///   - read time (materializing a merge run for a ListState.get call)
///   - compaction (folding a merge run into a single contiguous value)
///
/// Idempotent under any schedule: `combine(combine(A,B), C)` == `combine(A,B,C)`.
pub struct ListMergeCombiner;

impl ListMergeCombiner {
    pub fn new() -> Self {
        Self
    }

    pub fn combine(&self, operands: &[Vec<u8>]) -> Vec<u8> {
        let total: usize = operands.iter().map(|o| o.len()).sum();
        let mut out = Vec::with_capacity(total);
        for op in operands {
            out.extend_from_slice(op);
        }
        out
    }

    /// Combine an "existing value" (folded blob from prior merges or first PUT)
    /// with N new operands. Used at read time when a base value already exists.
    pub fn combine_with_base(&self, base: &[u8], operands: &[Vec<u8>]) -> Vec<u8> {
        let total = base.len() + operands.iter().map(|o| o.len()).sum::<usize>();
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(base);
        for op in operands {
            out.extend_from_slice(op);
        }
        out
    }
}

impl Default for ListMergeCombiner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concatenates_in_arrival_order() {
        let c = ListMergeCombiner::new();
        let result = c.combine(&[b"A".to_vec(), b"B".to_vec(), b"C".to_vec()]);
        assert_eq!(result, b"ABC");
    }

    #[test]
    fn idempotent_under_repeated_combine() {
        let c = ListMergeCombiner::new();
        let intermediate = c.combine(&[b"A".to_vec(), b"B".to_vec()]);
        let final_ = c.combine(&[intermediate, b"C".to_vec()]);
        assert_eq!(final_, b"ABC");
    }

    #[test]
    fn empty_operand_list_returns_empty() {
        let c = ListMergeCombiner::new();
        assert_eq!(c.combine(&[]), Vec::<u8>::new());
    }

    #[test]
    fn combine_with_base() {
        let c = ListMergeCombiner::new();
        let result = c.combine_with_base(b"BASE", &[b"X".to_vec(), b"Y".to_vec()]);
        assert_eq!(result, b"BASEXY");
    }

    #[test]
    fn preserves_arbitrary_bytes() {
        let c = ListMergeCombiner::new();
        let result = c.combine(&[vec![0u8, 1, 2, 3], vec![0xFFu8, 0xFE], vec![], vec![0x55u8]]);
        assert_eq!(result, vec![0, 1, 2, 3, 0xFF, 0xFE, 0x55]);
    }
}
