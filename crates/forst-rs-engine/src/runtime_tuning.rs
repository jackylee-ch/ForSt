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

//! Runtime tuning hooks (B-Prod-P7, spec §6d).
//!
//! Production knobs that operators tune without recompiling the engine:
//!
//! - **Block cache** — single shared LRU sized by
//!   `EngineOptions::block_cache_capacity_bytes`. Held by [`DbImpl`] and
//!   handed to SST readers as those wire it on the read path. The handle
//!   is exposed via [`DbImpl::block_cache`] so future readers (or the
//!   FFI tuning surface) can sample its hit-rate / current bytes.
//!
//! - **WriteBufferManager** — cross-CF memtable budget. The engine adds
//!   per-write deltas via [`WriteBufferManager::reserve`] and subtracts
//!   them on flush via [`WriteBufferManager::release`]. The total bytes
//!   currently held across all CFs is read by the writer hot path; once
//!   the running sum exceeds the configured cap the engine triggers an
//!   immediate flush of the largest CF rather than blocking writers
//!   (matches RocksDB's default `allow_stall = false` behaviour).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Cross-CF memtable budget tracker (spec §6d "WriteBufferManager").
///
/// Each engine owns one [`WriteBufferManager`] shared across every column
/// family. Per-write paths call [`Self::reserve`] before adding a key/value
/// pair to the active memtable and [`Self::release`] after a flush
/// successfully evicts a memtable's bytes. The running sum is observable
/// via [`Self::current_bytes`]; the cap is fixed at construction time and
/// readable via [`Self::capacity_bytes`].
///
/// A capacity of `0` means "unbounded" — `over_budget` returns `false`
/// regardless of the running sum, so consumers can wire the manager
/// uniformly and let the config decide whether the cap fires.
#[derive(Debug)]
pub struct WriteBufferManager {
    /// Configured cross-CF cap in bytes. `0` = unbounded.
    capacity: u64,
    /// Running sum of bytes reserved across all CF memtables. Updated via
    /// `Relaxed` atomics — the cap check is advisory (writers don't block
    /// on the running sum), so the only ordering requirement is that
    /// `current_bytes()` eventually reflects committed reservations.
    current: AtomicU64,
}

impl WriteBufferManager {
    /// Creates a new manager with the given capacity. `0` = unbounded.
    pub fn new(capacity_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            capacity: capacity_bytes,
            current: AtomicU64::new(0),
        })
    }

    /// Returns the configured cap in bytes (`0` = unbounded).
    #[inline]
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity
    }

    /// Returns the running sum of reserved bytes across every CF.
    #[inline]
    pub fn current_bytes(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// Reserves `n` bytes against the cross-CF budget. Always succeeds —
    /// reservation tracking is non-blocking and advisory; consumers
    /// observe `over_budget()` after the reserve to decide whether to
    /// trigger a flush. Saturates at `u64::MAX` rather than panicking on
    /// overflow.
    #[inline]
    pub fn reserve(&self, n: u64) {
        // Use `fetch_add` returning previous value so we can detect
        // saturation; explicit saturating add avoids wraparound on
        // pathological inputs (the FFI surface caps the per-CF
        // `write_buffer_size` at 1 TiB so realistic engines never
        // approach 2^64 bytes anyway).
        let prev = self.current.fetch_add(n, Ordering::Relaxed);
        if prev.checked_add(n).is_none() {
            // Wraparound — pin to u64::MAX for the next observer.
            self.current.store(u64::MAX, Ordering::Relaxed);
        }
    }

    /// Releases `n` bytes from the cross-CF budget. Saturates at `0`
    /// rather than wrapping when consumers double-release on flush
    /// retries.
    #[inline]
    pub fn release(&self, n: u64) {
        // Saturating subtract via CAS loop — `fetch_sub` would wrap on
        // an over-release, masking the bug as a multi-EiB running sum.
        loop {
            let cur = self.current.load(Ordering::Relaxed);
            let next = cur.saturating_sub(n);
            if self
                .current
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Returns `true` when the running sum exceeds the configured cap.
    /// Always `false` when capacity is `0` (unbounded).
    #[inline]
    pub fn over_budget(&self) -> bool {
        if self.capacity == 0 {
            return false;
        }
        self.current_bytes() > self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unbounded_capacity_never_over_budget() {
        let wbm = WriteBufferManager::new(0);
        wbm.reserve(u64::MAX / 2);
        assert!(!wbm.over_budget());
    }

    #[test]
    fn reserve_release_round_trip_zeroes_running_sum() {
        let wbm = WriteBufferManager::new(1024);
        wbm.reserve(512);
        wbm.reserve(256);
        assert_eq!(wbm.current_bytes(), 768);
        wbm.release(768);
        assert_eq!(wbm.current_bytes(), 0);
    }

    #[test]
    fn over_budget_fires_when_sum_exceeds_capacity() {
        let wbm = WriteBufferManager::new(1024);
        wbm.reserve(1025);
        assert!(wbm.over_budget());
    }

    #[test]
    fn over_budget_clears_after_release() {
        let wbm = WriteBufferManager::new(1024);
        wbm.reserve(2048);
        assert!(wbm.over_budget());
        wbm.release(1500);
        assert!(!wbm.over_budget());
    }

    #[test]
    fn release_saturates_at_zero_on_over_release() {
        let wbm = WriteBufferManager::new(1024);
        wbm.reserve(100);
        wbm.release(500); // over-release
        assert_eq!(wbm.current_bytes(), 0);
    }

    #[test]
    fn capacity_bytes_round_trips_construction_arg() {
        let wbm = WriteBufferManager::new(42);
        assert_eq!(wbm.capacity_bytes(), 42);
    }
}
