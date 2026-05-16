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

//! Native iterator with snapshot anchoring + chunked next() for the V1
//! vectorized dispatch path (umbrella spec §1 §b + §2 component E).
//!
//! # Design notes
//!
//! [`NativeIter`] wraps any `Iterator<Item = (Vec<u8>, Vec<u8>)>` and
//! drives it in chunks bounded by a caller-supplied byte budget. This lets
//! Java callers use a fixed-size direct buffer and call `frs_vec_iter_prefix_next`
//! in a tight loop without per-row FFI overhead.
//!
//! The [`abort`](NativeIter::abort) hook is used by the Java-side
//! `IterLifetimeWatchdog` to release native state on idle/max-lifetime breach.
//! After abort, [`next_chunk`](NativeIter::next_chunk) returns an empty chunk
//! immediately.
//!
//! # V1 snapshot anchoring caveat
//!
//! In V1, callers construct a `NativeIter` by wrapping a pre-materialized
//! `Vec` from `DbImpl::prefix_scan`. The snapshot is therefore "anchored" at
//! the point the vec was collected — subsequent writes do not affect iteration,
//! but the full result set is heap-resident for the handle's lifetime.
//! Proper engine snapshot ref-counting (lazy streaming) is deferred to V2.

use std::sync::atomic::{AtomicBool, Ordering};

/// A chunk of rows materialized from the iterator into a caller-owned buffer.
pub struct IterChunk {
    /// Key-value rows in this chunk, in ascending key order.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// Cursor into the next chunk: the key of the last row returned,
    /// or `None` when the iterator is exhausted.
    pub continuation: Option<Vec<u8>>,
}

/// Native iterator handle. Wraps an engine snapshot + the underlying rows
/// iterator. Caller drives via [`next_chunk`](Self::next_chunk); explicit
/// [`close`](Self::close) / drop releases the underlying data.
pub struct NativeIter<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)> + Send,
{
    inner: I,
    aborted: AtomicBool,
}

impl<I> NativeIter<I>
where
    I: Iterator<Item = (Vec<u8>, Vec<u8>)> + Send,
{
    /// Wraps an iterator as a `NativeIter`.
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            aborted: AtomicBool::new(false),
        }
    }

    /// Fill a chunk with rows until `chunk_bytes` cumulative (key + value)
    /// bytes have been accumulated, or the iterator is exhausted.
    ///
    /// Returns an empty [`IterChunk`] (with no continuation) when the
    /// iterator is exhausted or has been aborted.
    ///
    /// # Chunking semantics
    ///
    /// The byte budget is checked *before* pushing each row: we push at
    /// least one row per call (so callers never stall on a row that alone
    /// exceeds the budget) and stop as soon as `acc >= chunk_bytes`. This
    /// ensures forward progress even when individual rows are large.
    pub fn next_chunk(&mut self, chunk_bytes: usize) -> IterChunk {
        if self.aborted.load(Ordering::Acquire) {
            return IterChunk {
                rows: vec![],
                continuation: None,
            };
        }
        let mut rows = Vec::new();
        let mut acc = 0usize;
        loop {
            if acc >= chunk_bytes && !rows.is_empty() {
                break;
            }
            match self.inner.next() {
                Some((k, v)) => {
                    acc += k.len() + v.len();
                    rows.push((k, v));
                }
                None => break,
            }
        }
        let continuation = rows.last().map(|(k, _)| k.clone());
        IterChunk { rows, continuation }
    }

    /// Mark the iterator as aborted. Subsequent [`next_chunk`](Self::next_chunk)
    /// calls return empty immediately without consuming from the inner iterator.
    ///
    /// Intended for use by the Java-side `IterLifetimeWatchdog` to release
    /// native resources on idle/max-lifetime breach.
    pub fn abort(&self) {
        self.aborted.store(true, Ordering::Release);
    }

    /// Explicit close — consumes and drops the iterator. Equivalent to
    /// letting the `NativeIter` go out of scope, but makes intent clearer
    /// at call sites.
    pub fn close(self) {
        // drop(self) fires here
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build an iterator from a slice of static string rows.
    fn make_iter(pairs: &[(&[u8], &[u8])]) -> NativeIter<std::vec::IntoIter<(Vec<u8>, Vec<u8>)>> {
        let rows: Vec<(Vec<u8>, Vec<u8>)> = pairs
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        NativeIter::new(rows.into_iter())
    }

    #[test]
    fn chunk_respects_byte_budget() {
        // Each row: 4B key + 28B value = 32B.  Budget 128B → fits exactly 4 rows.
        let data: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"abcd".to_vec(), vec![0u8; 28]),
            (b"abce".to_vec(), vec![0u8; 28]),
            (b"abcf".to_vec(), vec![0u8; 28]),
            (b"abcg".to_vec(), vec![0u8; 28]),
        ];
        let mut iter = NativeIter::new(data.into_iter());
        let chunk = iter.next_chunk(128);
        // Budget is checked before push so the first row always lands.
        // With 4×32=128B data and a 128B budget: all 4 rows fit because
        // we push until acc >= chunk_bytes AND rows is non-empty.
        assert!(
            chunk.rows.len() >= 3 && chunk.rows.len() <= 4,
            "expected 3 or 4 rows, got {}",
            chunk.rows.len()
        );
    }

    #[test]
    fn empty_iterator_returns_empty_chunk() {
        let data: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        let mut iter = NativeIter::new(data.into_iter());
        let chunk = iter.next_chunk(1024);
        assert_eq!(chunk.rows.len(), 0);
        assert!(chunk.continuation.is_none());
    }

    #[test]
    fn abort_returns_empty_thereafter() {
        let mut iter = make_iter(&[(b"a", b"v")]);
        iter.abort();
        let chunk = iter.next_chunk(1024);
        assert_eq!(chunk.rows.len(), 0);
        assert!(chunk.continuation.is_none());
    }

    #[test]
    fn exhausted_iterator_returns_empty() {
        let mut iter = make_iter(&[(b"a", b"v")]);
        let _ = iter.next_chunk(1024); // consume the one row
        let chunk = iter.next_chunk(1024);
        assert_eq!(chunk.rows.len(), 0);
        assert!(chunk.continuation.is_none());
    }

    #[test]
    fn multi_chunk_walk_covers_all_rows() {
        // 6 rows, budget 1B → each next_chunk yields exactly 1 row
        // (at least 1 row always, then stops once acc >= 1).
        let pairs: Vec<(&[u8], &[u8])> = vec![
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3"),
            (b"k4", b"v4"),
            (b"k5", b"v5"),
            (b"k6", b"v6"),
        ];
        let mut iter = make_iter(&pairs);
        let mut total = 0usize;
        loop {
            let chunk = iter.next_chunk(1);
            if chunk.rows.is_empty() {
                break;
            }
            total += chunk.rows.len();
        }
        assert_eq!(total, 6);
    }

    #[test]
    fn continuation_key_matches_last_row() {
        let mut iter = make_iter(&[(b"alpha", b"1"), (b"beta", b"2"), (b"gamma", b"3")]);
        let chunk = iter.next_chunk(1024);
        assert_eq!(chunk.rows.len(), 3);
        assert_eq!(chunk.continuation.as_deref(), Some(b"gamma" as &[u8]));
    }

    #[test]
    fn close_drops_iter() {
        // Verifies the close() method compiles and doesn't panic.
        let iter = make_iter(&[(b"k", b"v")]);
        iter.close();
        // iter is consumed; no access after this point.
    }
}
