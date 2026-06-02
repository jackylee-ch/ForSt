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

//! Non-moving segmented byte arena — the storage foundation for the lock-free
//! memtable (lock-free track P3, phase 1; see
//! `docs/superpowers/specs/2026-06-02-lock-free-memtable-design.md`).
//!
//! Each appended slice is stored **contiguously within a single fixed-size
//! chunk** (a slice never spans two chunks). Chunks are heap-allocated once and
//! **never moved, resized, or freed** for the arena's lifetime. Therefore a
//! [`ByteSpan`] `(chunk, offset, len)` returned by [`SegmentedBytes::append`]
//! stays valid — and [`SegmentedBytes::get`] returns the same bytes — no matter
//! how many further appends occur.
//!
//! This is the property the current `Vec<u8>` columnar arenas lack: a `Vec`
//! realloc **moves** its bytes, which would dangle a concurrent reader's offset.
//! With non-moving chunks, memtable readers can hold row pointers while writers
//! append concurrently — the RocksDB/ForsT arena model, kept Arrow-friendly
//! (each chunk is a contiguous byte buffer that flush can wrap into an Arrow
//! buffer without copying).
//!
//! Phase 1 keeps the existing per-shard `RwLock` (so `&mut self` append is fine);
//! later phases make the outer chunk list and the index lock-free.

/// A handle to a contiguous run of bytes within a [`SegmentedBytes`] arena.
/// Stable for the arena's lifetime (chunks never move).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteSpan {
    /// Index of the owning chunk.
    pub chunk: u32,
    /// Byte offset within the chunk.
    pub offset: u32,
    /// Length in bytes.
    pub len: u32,
}

/// Append-only byte arena backed by fixed-size, non-moving chunks.
pub struct SegmentedBytes {
    /// Heap buffers. Each `Box<[u8]>`'s data allocation is stable for life; the
    /// outer `Vec` may realloc (moving the `Box` headers) but NOT the chunk data
    /// the headers point to — so a `ByteSpan` remains valid. (The outer `Vec`
    /// itself is made lock-free in a later phase; phase 1 is `&mut self` only.)
    chunks: Vec<Box<[u8]>>,
    /// Bytes used in each chunk (the contiguous fill mark).
    used: Vec<usize>,
    /// Default chunk capacity for normal appends.
    chunk_cap: usize,
    /// Total bytes appended (sum of all `len`s).
    total: usize,
}

impl SegmentedBytes {
    /// Creates an empty arena whose normal chunks hold `chunk_cap` bytes
    /// (clamped to a small floor). A single appended slice larger than
    /// `chunk_cap` gets its own exact-sized dedicated chunk.
    pub fn with_chunk_capacity(chunk_cap: usize) -> Self {
        SegmentedBytes {
            chunks: Vec::new(),
            used: Vec::new(),
            chunk_cap: chunk_cap.max(64),
            total: 0,
        }
    }

    /// Appends `bytes` contiguously and returns a stable handle. The slice is
    /// placed entirely within one chunk: if it does not fit the current chunk's
    /// remaining room, a fresh chunk is started (the remainder is left unused);
    /// if it exceeds `chunk_cap`, a dedicated exact-sized chunk is allocated.
    pub fn append(&mut self, bytes: &[u8]) -> ByteSpan {
        let len = bytes.len();
        let fits_current = match self.chunks.last() {
            Some(chunk) => self.used[self.chunks.len() - 1] + len <= chunk.len(),
            None => false,
        };
        if !fits_current {
            let cap = len.max(self.chunk_cap);
            self.chunks.push(vec![0u8; cap].into_boxed_slice());
            self.used.push(0);
        }
        let ci = self.chunks.len() - 1;
        let off = self.used[ci];
        self.chunks[ci][off..off + len].copy_from_slice(bytes);
        self.used[ci] += len;
        self.total += len;
        ByteSpan {
            chunk: ci as u32,
            offset: off as u32,
            len: len as u32,
        }
    }

    /// Returns the bytes referenced by `span`. Panics only on a malformed span
    /// (out-of-range chunk/offset) — a programmer error, never reachable for a
    /// span this arena issued.
    #[inline]
    pub fn get(&self, span: ByteSpan) -> &[u8] {
        let start = span.offset as usize;
        let end = start + span.len as usize;
        &self.chunks[span.chunk as usize][start..end]
    }

    /// Total bytes appended.
    pub fn total_bytes(&self) -> usize {
        self.total
    }

    /// Number of chunks currently allocated.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_get_roundtrip() {
        let mut a = SegmentedBytes::with_chunk_capacity(1024);
        let s1 = a.append(b"hello");
        let s2 = a.append(b"world!!");
        let s3 = a.append(b"");
        assert_eq!(a.get(s1), b"hello");
        assert_eq!(a.get(s2), b"world!!");
        assert_eq!(a.get(s3), b"");
        assert_eq!(a.total_bytes(), 12);
        assert_eq!(a.chunk_count(), 1, "all fit in one chunk");
    }

    #[test]
    fn append_crosses_chunk_boundary_without_splitting() {
        // Small chunk so the third append cannot fit the first chunk's remainder.
        let mut a = SegmentedBytes::with_chunk_capacity(64);
        let s1 = a.append(&[1u8; 40]);
        let s2 = a.append(&[2u8; 20]); // 40+20=60 ≤ 64 → same chunk
        let s3 = a.append(&[3u8; 30]); // 60+30=90 > 64 → new chunk (no split)
        assert_eq!(s1.chunk, 0);
        assert_eq!(s2.chunk, 0);
        assert_eq!(s3.chunk, 1, "must start a new chunk, not split the row");
        assert_eq!(a.get(s1), &[1u8; 40][..]);
        assert_eq!(a.get(s2), &[2u8; 20][..]);
        assert_eq!(a.get(s3), &[3u8; 30][..]);
        assert_eq!(a.chunk_count(), 2);
    }

    #[test]
    fn oversized_value_gets_dedicated_chunk() {
        let mut a = SegmentedBytes::with_chunk_capacity(64);
        let big = vec![7u8; 500];
        let s = a.append(&big);
        assert_eq!(a.get(s), big.as_slice());
        assert_eq!(s.len, 500);
        // A subsequent small append must not corrupt the big value.
        let small = a.append(b"after");
        assert_eq!(a.get(s), big.as_slice());
        assert_eq!(a.get(small), b"after");
    }

    #[test]
    fn spans_stay_valid_after_many_appends_non_moving() {
        // Collect spans, then append far more data (forcing many new chunks),
        // and verify every original span still resolves to its bytes — the
        // non-moving guarantee that lets readers hold pointers under writes.
        let mut a = SegmentedBytes::with_chunk_capacity(128);
        let mut spans = Vec::new();
        for i in 0..200u32 {
            let payload = format!("row-{i:08}-{}", "x".repeat((i % 50) as usize));
            spans.push((a.append(payload.as_bytes()), payload));
        }
        // Force lots more growth.
        for _ in 0..1000 {
            a.append(&[0xABu8; 100]);
        }
        for (span, expected) in &spans {
            assert_eq!(
                a.get(*span),
                expected.as_bytes(),
                "span dangled after growth"
            );
        }
        assert!(a.chunk_count() > 1);
    }
}
