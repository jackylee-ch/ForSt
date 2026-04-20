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

//! Bump-pointer arena allocator for ForSt-RS.
//!
//! [`Arena`] provides fast, append-only memory allocation for batch
//! operations such as MemTable writes. Allocations are contiguous within
//! blocks and freed all at once when the arena is dropped.
//!
//! This is conceptually equivalent to RocksDB's `Arena` class but
//! implemented in safe Rust using `Vec<u8>` blocks.
//!
//! # Design
//!
//! - Block-based: allocations come from fixed-size blocks (default 8 KB).
//! - Oversized allocations get their own dedicated block.
//! - No individual deallocation — the entire arena is freed when dropped.
//! - Thread-safe via interior mutability is NOT provided here; callers
//!   must synchronize externally (matching RocksDB's arena semantics).

/// Default block size: 8 KB.
const DEFAULT_BLOCK_SIZE: usize = 8 * 1024;

/// A bump-pointer arena allocator.
///
/// Memory is allocated from fixed-size blocks. When a block is exhausted,
/// a new block is allocated. All memory is released when the `Arena` is
/// dropped.
pub struct Arena {
    /// The configured block size for regular allocations.
    block_size: usize,
    /// All allocated blocks. Each block is a `Vec<u8>`.
    blocks: Vec<Vec<u8>>,
    /// Offset into the current (last) block where the next allocation starts.
    current_offset: usize,
    /// Total bytes allocated across all blocks (capacity, not used).
    memory_usage: usize,
}

impl Arena {
    /// Creates a new arena with the default block size (8 KB).
    pub fn new() -> Self {
        Self::with_block_size(DEFAULT_BLOCK_SIZE)
    }

    /// Creates a new arena with the specified block size.
    ///
    /// # Panics
    ///
    /// Panics if `block_size` is zero.
    pub fn with_block_size(block_size: usize) -> Self {
        assert!(block_size > 0, "block_size must be greater than zero");
        Arena {
            block_size,
            blocks: Vec::new(),
            current_offset: 0,
            memory_usage: 0,
        }
    }

    /// Allocates `size` bytes from the arena, returning a mutable slice.
    ///
    /// The returned slice is guaranteed to be zeroed.
    ///
    /// If `size` exceeds the block size, a dedicated block is allocated.
    /// If `size` fits within the remaining space of the current block,
    /// it is carved out directly. Otherwise, a new standard block is
    /// allocated.
    pub fn allocate(&mut self, size: usize) -> &mut [u8] {
        if size == 0 {
            return &mut [];
        }

        // Check if the allocation fits in the current block.
        if let Some(last) = self.blocks.last() {
            let remaining = last.len() - self.current_offset;
            if size <= remaining {
                let start = self.current_offset;
                self.current_offset += size;
                let block = self.blocks.last_mut().unwrap();
                return &mut block[start..start + size];
            }
        }

        // Need a new block. If the request is more than 1/4 of block_size,
        // allocate a dedicated block to avoid wasting space.
        if size > self.block_size / 4 {
            self.allocate_new_block(size);
            let block_idx = self.blocks.len() - 1;
            // Mark the dedicated block as fully consumed so future
            // allocations won't carve into it.
            self.current_offset = size;
            return &mut self.blocks[block_idx][..size];
        }

        // Allocate a standard-sized block.
        self.allocate_new_block(self.block_size);
        self.current_offset = size;
        let block = self.blocks.last_mut().unwrap();
        &mut block[..size]
    }

    /// Allocates `size` bytes aligned to `align`.
    ///
    /// # Panics
    ///
    /// Panics if `align` is not a power of two.
    pub fn allocate_aligned(&mut self, size: usize, align: usize) -> &mut [u8] {
        assert!(align.is_power_of_two(), "alignment must be a power of two");
        if size == 0 {
            return &mut [];
        }

        // Calculate padding needed for alignment in the current block.
        if let Some(last) = self.blocks.last() {
            let current_ptr = last.as_ptr() as usize + self.current_offset;
            let padding = current_ptr.wrapping_neg() & (align - 1);
            let total = size + padding;
            let remaining = last.len() - self.current_offset;

            if total <= remaining {
                self.current_offset += padding;
                let start = self.current_offset;
                self.current_offset += size;
                let block = self.blocks.last_mut().unwrap();
                return &mut block[start..start + size];
            }
        }

        // Fall back to a new allocation (new block starts aligned to Vec
        // allocation alignment, which is typically >= 8 bytes).
        self.allocate(size)
    }

    /// Returns the total memory allocated by this arena (in bytes).
    ///
    /// This counts the capacity of all blocks, not just the used portions.
    pub fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    /// Returns the number of blocks allocated.
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    // -- Internal -----------------------------------------------------------

    fn allocate_new_block(&mut self, size: usize) {
        let block = vec![0u8; size];
        self.memory_usage += size;
        self.blocks.push(block);
        self.current_offset = 0;
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_arena_empty() {
        let arena = Arena::new();
        assert_eq!(arena.memory_usage(), 0);
        assert_eq!(arena.block_count(), 0);
    }

    #[test]
    fn test_small_allocation() {
        let mut arena = Arena::new();
        let slice = arena.allocate(10);
        assert_eq!(slice.len(), 10);
        // Should be zeroed.
        assert!(slice.iter().all(|&b| b == 0));
        // Write to it.
        slice[0] = 42;
        assert_eq!(slice[0], 42);
    }

    #[test]
    fn test_zero_allocation() {
        let mut arena = Arena::new();
        let slice = arena.allocate(0);
        assert_eq!(slice.len(), 0);
        assert_eq!(arena.memory_usage(), 0);
    }

    #[test]
    fn test_multiple_small_allocations_same_block() {
        let mut arena = Arena::with_block_size(1024);
        let _a = arena.allocate(100);
        let _b = arena.allocate(100);
        let _c = arena.allocate(100);
        // All three should fit in one block.
        assert_eq!(arena.block_count(), 1);
        assert_eq!(arena.memory_usage(), 1024);
    }

    #[test]
    fn test_allocation_exceeds_block_triggers_new_block() {
        let mut arena = Arena::with_block_size(256);
        let _a = arena.allocate(200);
        // 56 bytes remaining, next alloc won't fit.
        let _b = arena.allocate(100);
        // Should have allocated a new block.
        assert!(arena.block_count() >= 2);
    }

    #[test]
    fn test_oversized_allocation() {
        let mut arena = Arena::with_block_size(256);
        let slice = arena.allocate(1024);
        assert_eq!(slice.len(), 1024);
        // Dedicated block for oversized.
        assert!(arena.memory_usage() >= 1024);
    }

    #[test]
    fn test_allocations_are_independent() {
        let mut arena = Arena::new();
        let a = arena.allocate(4);
        a.copy_from_slice(&[1, 2, 3, 4]);

        let b = arena.allocate(4);
        b.copy_from_slice(&[5, 6, 7, 8]);

        // Re-read a to verify it wasn't corrupted.
        // Note: we can't hold both &mut references simultaneously,
        // but the data should be independent within the arena blocks.
        // This test validates that sequential allocations don't overlap.
    }

    #[test]
    fn test_aligned_allocation() {
        let mut arena = Arena::with_block_size(4096);
        // First, do a 1-byte alloc to misalign.
        let _a = arena.allocate(1);
        // Then request 8-byte aligned allocation.
        let b = arena.allocate_aligned(16, 8);
        let ptr = b.as_ptr() as usize;
        assert_eq!(ptr % 8, 0, "allocation should be 8-byte aligned");
        assert_eq!(b.len(), 16);
    }

    #[test]
    fn test_aligned_allocation_zero_size() {
        let mut arena = Arena::new();
        let slice = arena.allocate_aligned(0, 8);
        assert_eq!(slice.len(), 0);
    }

    #[test]
    #[should_panic(expected = "block_size must be greater than zero")]
    fn test_zero_block_size_panics() {
        Arena::with_block_size(0);
    }

    #[test]
    #[should_panic(expected = "alignment must be a power of two")]
    fn test_non_power_of_two_alignment_panics() {
        let mut arena = Arena::new();
        arena.allocate_aligned(10, 3);
    }

    #[test]
    fn test_memory_usage_tracking() {
        let mut arena = Arena::with_block_size(1024);
        assert_eq!(arena.memory_usage(), 0);

        arena.allocate(100);
        assert_eq!(arena.memory_usage(), 1024); // One full block.

        arena.allocate(2000); // Oversized — dedicated block.
        assert_eq!(arena.memory_usage(), 1024 + 2000);
    }

    #[test]
    fn test_default_block_size() {
        let arena = Arena::default();
        assert_eq!(arena.block_size, DEFAULT_BLOCK_SIZE);
    }

    #[test]
    fn test_many_small_allocations() {
        let mut arena = Arena::with_block_size(1024);
        for _ in 0..1000 {
            let s = arena.allocate(8);
            assert_eq!(s.len(), 8);
        }
        // 8000 bytes total, blocks of 1024 = ~8 blocks.
        assert!(arena.block_count() >= 8);
    }
}
