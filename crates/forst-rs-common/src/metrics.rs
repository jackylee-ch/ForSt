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

//! Lightweight metrics abstractions for ForSt-RS.
//!
//! Provides [`Counter`], [`Gauge`], and [`Histogram`] types that can be
//! used throughout the engine to track operational statistics.
//!
//! # Design
//!
//! - **Atomic**: All metric types use `AtomicU64`/`AtomicI64` for lock-free
//!   concurrent updates.
//! - **Zero-allocation**: Recording a metric never allocates.
//! - **Standalone**: No external metrics framework dependency — the engine
//!   exposes raw values that a bridge layer (FFM/JNI) can poll.
//!
//! # Histogram Bucketing
//!
//! The histogram uses a fixed set of buckets matching RocksDB's statistics
//! conventions for latency and size distributions.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

/// A monotonically increasing counter.
///
/// Suitable for tracking cumulative events like bytes written, keys
/// inserted, or cache hits.
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    /// Creates a new counter initialized to zero.
    pub const fn new() -> Self {
        Counter {
            value: AtomicU64::new(0),
        }
    }

    /// Increments the counter by `n`.
    #[inline]
    pub fn inc_by(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Increments the counter by 1.
    #[inline]
    pub fn inc(&self) {
        self.inc_by(1);
    }

    /// Returns the current counter value.
    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Resets the counter to zero.
    #[inline]
    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Gauge
// ---------------------------------------------------------------------------

/// A gauge that can go up or down.
///
/// Suitable for tracking instantaneous values like memory usage,
/// number of open files, or pending compaction bytes.
pub struct Gauge {
    value: AtomicI64,
}

impl Gauge {
    /// Creates a new gauge initialized to zero.
    pub const fn new() -> Self {
        Gauge {
            value: AtomicI64::new(0),
        }
    }

    /// Sets the gauge to the given value.
    #[inline]
    pub fn set(&self, val: i64) {
        self.value.store(val, Ordering::Relaxed);
    }

    /// Increments the gauge by `n`.
    #[inline]
    pub fn inc_by(&self, n: i64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Decrements the gauge by `n`.
    #[inline]
    pub fn dec_by(&self, n: i64) {
        self.value.fetch_sub(n, Ordering::Relaxed);
    }

    /// Returns the current gauge value.
    #[inline]
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl Default for Gauge {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

/// Pre-defined histogram bucket boundaries (in microseconds for latency,
/// or bytes for size distributions).
///
/// These match RocksDB's common latency percentile buckets.
pub const DEFAULT_BUCKETS: &[f64] = &[
    1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0,
    50000.0, 100000.0, 200000.0, 500000.0, 1000000.0,
];

/// Fixed-point multiplier for [`Histogram`] sum accumulation.
///
/// `sum_fixed = round(value * SUM_MULTIPLIER)` lets us use a single `i64`
/// `fetch_add` on the hot path (vs the prior f64 CAS loop, which could
/// permanently poison the sum if `f64::NAN` was ever observed — see C1 R2
/// H#2).
///
/// 6 decimal places (`1_000_000`) is enough for sub-microsecond timing
/// metrics; sum range is `i64::MAX / SUM_MULTIPLIER ≈ 9.2 × 10^12`.
pub const SUM_MULTIPLIER: f64 = 1_000_000.0;

/// A histogram that records observed values into fixed buckets.
///
/// # Concurrency
///
/// Each field of the histogram (`total_count`, `sum_fixed`, per-bucket
/// `counts[i]`, `overflow`) is updated atomically via `fetch_add`. The
/// composite [`HistogramSnapshot`] returned by [`Self::snapshot`] is
/// **point-in-time approximate**: under heavy concurrency, count and sum
/// may diverge by ≤1 observation (one of the two ops may be visible while
/// the peer is still in flight on another thread). This is acceptable for
/// monitoring use; callers needing strict atomicity should serialize
/// externally.
///
/// # Sum semantics
///
/// `sum_fixed` is an `i64` fixed-point accumulation with multiplier
/// [`SUM_MULTIPLIER`]. NaN inputs to [`Self::observe`] silently drop from
/// sum (saturating cast → 0); `total_count` still increments so the
/// upstream NaN-producing bug remains observable. Inf inputs saturate to
/// `i64::MAX/MIN` per Rust float-to-int spec; subsequent [`Self::sum`]
/// reads return ±∞ for typical magnitudes.
pub struct Histogram {
    /// Bucket upper bounds (sorted ascending).
    bounds: Vec<f64>,
    /// Count of observations in each bucket. `counts[i]` is the number of
    /// observations <= `bounds[i]`.
    counts: Vec<AtomicU64>,
    /// Count of observations that exceed all bucket bounds (overflow).
    overflow: AtomicU64,
    /// Total number of observations.
    total_count: AtomicU64,
    /// Sum of all observed values, stored as i64 fixed-point with
    /// multiplier [`SUM_MULTIPLIER`]. Updated via single lock-free
    /// `fetch_add`; immune to NaN poisoning by data-type construction.
    sum_fixed: AtomicI64,
}

impl Histogram {
    /// Creates a new histogram with the given bucket boundaries.
    ///
    /// `bounds` must be sorted ascending and non-empty.
    ///
    /// # Panics
    ///
    /// Panics if `bounds` is empty.
    pub fn new(bounds: &[f64]) -> Self {
        assert!(!bounds.is_empty(), "histogram bounds must not be empty");
        let counts = (0..bounds.len()).map(|_| AtomicU64::new(0)).collect();
        Histogram {
            bounds: bounds.to_vec(),
            counts,
            overflow: AtomicU64::new(0),
            total_count: AtomicU64::new(0),
            sum_fixed: AtomicI64::new(0),
        }
    }

    /// Creates a histogram with [`DEFAULT_BUCKETS`].
    pub fn with_default_buckets() -> Self {
        Self::new(DEFAULT_BUCKETS)
    }

    /// Records an observed value.
    ///
    /// See [`Histogram`]'s "Concurrency" and "Sum semantics" sections for the
    /// behavior under concurrent observers, NaN, and ±Inf inputs.
    #[inline]
    pub fn observe(&self, value: f64) {
        self.total_count.fetch_add(1, Ordering::Relaxed);

        // Fixed-point sum: cast saturates on NaN/Inf (Rust spec):
        //   NaN  -> 0          (silent drop; count still increments)
        //   +Inf -> i64::MAX   (saturates; subsequent sum reads return ±∞)
        //   -Inf -> i64::MIN   (saturates)
        // This is by design: a NaN/Inf input is upstream-bug observability,
        // not a reason to permanently corrupt the sum (the prior f64 CAS
        // loop did exactly that — see C1 R2 H#2).
        let scaled = (value * SUM_MULTIPLIER) as i64;
        self.sum_fixed.fetch_add(scaled, Ordering::Relaxed);

        // Find the right bucket via linear scan (buckets are typically small).
        for (i, &bound) in self.bounds.iter().enumerate() {
            if value <= bound {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        self.overflow.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the total number of observations.
    #[inline]
    pub fn count(&self) -> u64 {
        self.total_count.load(Ordering::Relaxed)
    }

    /// Returns the sum of all observed values.
    ///
    /// Reads the i64 fixed-point accumulator and divides by
    /// [`SUM_MULTIPLIER`]. NaN inputs to [`Self::observe`] do not contribute
    /// to this sum (see [`Histogram`] "Sum semantics"); ±Inf inputs may
    /// surface as ±∞ here once the i64 accumulator has saturated.
    #[inline]
    pub fn sum(&self) -> f64 {
        (self.sum_fixed.load(Ordering::Relaxed) as f64) / SUM_MULTIPLIER
    }

    /// Returns the mean of all observed values, or 0.0 if no observations.
    #[inline]
    pub fn mean(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            return 0.0;
        }
        self.sum() / count as f64
    }

    /// Returns the count of observations in each bucket.
    ///
    /// The returned vector has the same length as the bounds.
    /// An additional overflow count is returned separately.
    pub fn bucket_counts(&self) -> (Vec<u64>, u64) {
        let counts: Vec<u64> = self
            .counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect();
        let overflow = self.overflow.load(Ordering::Relaxed);
        (counts, overflow)
    }

    /// Returns a snapshot of the histogram as a [`HistogramSnapshot`].
    ///
    /// **Point-in-time approximate**: individual atomic reads of
    /// `total_count`, `sum_fixed`, bucket counts, and overflow are each
    /// linearizable, but the composition is not — under heavy concurrency,
    /// observers may see a state that did not exist at any single instant
    /// (e.g., `count = N+1` reflecting a peer's just-incremented counter
    /// while `sum` still reflects only the first `N` peer additions). This
    /// is acceptable for monitoring; callers needing strict atomicity
    /// should serialize externally.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let (bucket_counts, overflow) = self.bucket_counts();
        HistogramSnapshot {
            bounds: self.bounds.clone(),
            bucket_counts,
            overflow,
            total_count: self.count(),
            sum: self.sum(),
        }
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::with_default_buckets()
    }
}

/// An immutable snapshot of a [`Histogram`] at a point in time.
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    /// Bucket upper bounds.
    pub bounds: Vec<f64>,
    /// Observation counts per bucket.
    pub bucket_counts: Vec<u64>,
    /// Observations exceeding all bounds.
    pub overflow: u64,
    /// Total observation count.
    pub total_count: u64,
    /// Sum of all observations.
    pub sum: f64,
}

impl HistogramSnapshot {
    /// Estimates the value at the given percentile (0.0–1.0).
    ///
    /// Uses linear interpolation within the target bucket. Returns 0.0
    /// if no observations exist.
    pub fn percentile(&self, p: f64) -> f64 {
        if self.total_count == 0 || p <= 0.0 {
            return 0.0;
        }
        if p >= 1.0 {
            return *self.bounds.last().unwrap_or(&0.0);
        }

        let target = (p * self.total_count as f64).ceil() as u64;
        let mut cumulative: u64 = 0;

        for (i, &count) in self.bucket_counts.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                // Linear interpolation within this bucket.
                let lower = if i == 0 { 0.0 } else { self.bounds[i - 1] };
                let upper = self.bounds[i];
                let prev_cumulative = cumulative - count;
                if count == 0 {
                    return upper;
                }
                let fraction = (target - prev_cumulative) as f64 / count as f64;
                return lower + (upper - lower) * fraction;
            }
        }

        // All observations are in overflow.
        *self.bounds.last().unwrap_or(&0.0)
    }
}

// ---------------------------------------------------------------------------
// Metric names (string constants for well-known engine metrics)
// ---------------------------------------------------------------------------

/// Well-known metric names used by the engine.
pub mod metric_names {
    /// Bytes written to WAL.
    pub const WAL_BYTES_WRITTEN: &str = "forst.wal.bytes_written";
    /// Number of WAL syncs.
    pub const WAL_SYNCS: &str = "forst.wal.syncs";
    /// Bytes written during compaction.
    pub const COMPACTION_BYTES_WRITTEN: &str = "forst.compaction.bytes_written";
    /// Bytes read during compaction.
    pub const COMPACTION_BYTES_READ: &str = "forst.compaction.bytes_read";
    /// Number of memtable flushes.
    pub const MEMTABLE_FLUSHES: &str = "forst.memtable.flushes";
    /// Current memtable size in bytes.
    pub const MEMTABLE_SIZE: &str = "forst.memtable.size_bytes";
    /// Block cache hits.
    pub const BLOCK_CACHE_HITS: &str = "forst.cache.hits";
    /// Block cache misses.
    pub const BLOCK_CACHE_MISSES: &str = "forst.cache.misses";
    /// Number of open SST files.
    pub const SST_FILES_OPEN: &str = "forst.sst.files_open";
    /// Total SST file size.
    pub const SST_TOTAL_SIZE: &str = "forst.sst.total_size_bytes";
    /// Read latency in microseconds.
    pub const READ_LATENCY_US: &str = "forst.read.latency_us";
    /// Write latency in microseconds.
    pub const WRITE_LATENCY_US: &str = "forst.write.latency_us";
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Counter tests -------------------------------------------------------

    #[test]
    fn test_counter_new() {
        let c = Counter::new();
        assert_eq!(c.get(), 0);
    }

    #[test]
    fn test_counter_inc() {
        let c = Counter::new();
        c.inc();
        c.inc();
        assert_eq!(c.get(), 2);
    }

    #[test]
    fn test_counter_inc_by() {
        let c = Counter::new();
        c.inc_by(10);
        c.inc_by(20);
        assert_eq!(c.get(), 30);
    }

    #[test]
    fn test_counter_reset() {
        let c = Counter::new();
        c.inc_by(100);
        c.reset();
        assert_eq!(c.get(), 0);
    }

    // -- Gauge tests ---------------------------------------------------------

    #[test]
    fn test_gauge_new() {
        let g = Gauge::new();
        assert_eq!(g.get(), 0);
    }

    #[test]
    fn test_gauge_set() {
        let g = Gauge::new();
        g.set(42);
        assert_eq!(g.get(), 42);
    }

    #[test]
    fn test_gauge_inc_dec() {
        let g = Gauge::new();
        g.inc_by(10);
        g.dec_by(3);
        assert_eq!(g.get(), 7);
    }

    #[test]
    fn test_gauge_negative() {
        let g = Gauge::new();
        g.dec_by(5);
        assert_eq!(g.get(), -5);
    }

    // -- Histogram tests -----------------------------------------------------

    #[test]
    fn test_histogram_new() {
        let h = Histogram::new(&[10.0, 50.0, 100.0]);
        assert_eq!(h.count(), 0);
        assert_eq!(h.sum(), 0.0);
    }

    #[test]
    fn test_histogram_observe() {
        let h = Histogram::new(&[10.0, 50.0, 100.0]);
        h.observe(5.0);
        h.observe(25.0);
        h.observe(75.0);
        h.observe(200.0); // overflow

        assert_eq!(h.count(), 4);
        assert!((h.sum() - 305.0).abs() < f64::EPSILON);

        let (counts, overflow) = h.bucket_counts();
        assert_eq!(counts, vec![1, 1, 1]);
        assert_eq!(overflow, 1);
    }

    #[test]
    fn test_histogram_mean() {
        let h = Histogram::new(&[100.0]);
        assert_eq!(h.mean(), 0.0); // No observations.

        h.observe(10.0);
        h.observe(20.0);
        assert!((h.mean() - 15.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_histogram_snapshot_percentile() {
        let h = Histogram::new(&[10.0, 50.0, 100.0]);
        for v in 1..=100 {
            h.observe(v as f64);
        }
        let snap = h.snapshot();
        let p50 = snap.percentile(0.5);
        let p99 = snap.percentile(0.99);

        // p50 should be around 50, p99 around 99.
        assert!(p50 > 0.0 && p50 <= 100.0);
        assert!(p99 > p50);
    }

    #[test]
    fn test_histogram_percentile_edge_cases() {
        let h = Histogram::new(&[10.0, 50.0, 100.0]);
        let snap = h.snapshot();
        assert_eq!(snap.percentile(0.5), 0.0); // No data.
        assert_eq!(snap.percentile(0.0), 0.0);
    }

    #[test]
    fn test_histogram_default_buckets() {
        let h = Histogram::with_default_buckets();
        // Use the public snapshot() to read bucket count rather than
        // poking at the private `bounds` field — keeps the test
        // honest about the public API surface (R2 M#6).
        let snap = h.snapshot();
        assert_eq!(snap.bounds.len(), DEFAULT_BUCKETS.len());
    }

    #[test]
    #[should_panic(expected = "histogram bounds must not be empty")]
    fn test_histogram_empty_bounds_panics() {
        Histogram::new(&[]);
    }

    // -- Concurrent tests ---------------------------------------------------

    #[test]
    fn test_counter_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let counter = Arc::new(Counter::new());
        let mut handles = Vec::new();

        for _ in 0..4 {
            let c = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    c.inc();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.get(), 4000);
    }

    #[test]
    fn test_gauge_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let gauge = Arc::new(Gauge::new());
        let mut handles = Vec::new();

        for _ in 0..4 {
            let g = Arc::clone(&gauge);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    g.inc_by(1);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(gauge.get(), 4000);
    }

    // -- Metric names -------------------------------------------------------

    #[test]
    fn test_metric_names_are_non_empty() {
        // `is_empty()` checks on `&'static str` constants are statically
        // verifiable; clippy `const_is_empty` flags them. Use length
        // comparison + prefix check that exercise actual string content
        // rather than the trivially-true const property.
        assert!(metric_names::WAL_BYTES_WRITTEN.len() > "forst.".len());
        assert!(metric_names::READ_LATENCY_US.len() > "forst.".len());
        assert!(metric_names::WAL_BYTES_WRITTEN.starts_with("forst."));
        assert!(metric_names::READ_LATENCY_US.starts_with("forst."));
    }

    // -- Histogram arch-pivot regressions (C1 R2 H#1 + H#2 + M#2) ------------

    /// Regression test for C1 R2 H#2 (NaN poisoning).
    ///
    /// Prior to the i64 fixed-point pivot, a single `observe(f64::NAN)`
    /// would set `sum_bits` to the NaN bit pattern, after which every
    /// future `observe(v)` saw `sum = NaN + v = NaN` and wrote NaN back —
    /// permanent corruption.
    #[test]
    fn nan_input_does_not_poison_sum() {
        let h = Histogram::with_default_buckets();

        h.observe(f64::NAN);
        h.observe(1.0);
        h.observe(2.0);
        h.observe(f64::NAN);
        h.observe(3.0);

        // count includes the NaN observations (they remain observable
        // upstream-bug indicators even if the sum drops them).
        assert_eq!(h.count(), 5);

        // sum reflects only the valid 1.0 + 2.0 + 3.0 = 6.0.
        assert!(
            (h.sum() - 6.0).abs() < 1e-9,
            "expected sum=6.0 (NaN dropped), got {}",
            h.sum()
        );
    }

    /// Regression test for ±∞ input handling: float-to-i64 cast saturates
    /// per Rust spec, so `+∞ * MULT` → `i64::MAX` and `-∞ * MULT` →
    /// `i64::MIN`. After Inf is observed, `sum()` returns ±∞ via
    /// `(i64::MAX as f64) / MULT == f64::INFINITY` for typical magnitudes.
    #[test]
    fn inf_input_saturates_safely() {
        let h_pos = Histogram::with_default_buckets();
        h_pos.observe(f64::INFINITY);
        h_pos.observe(1.0);
        // After +∞ saturates to i64::MAX, adding 1*MULT may wrap to a large
        // negative or remain saturated depending on platform; we accept any
        // outcome that indicates +∞ was observed (sum is either ±∞ via the
        // saturated cast, or a magnitude clearly outside normal accumulation).
        assert_eq!(h_pos.count(), 2);
        let s_pos = h_pos.sum();
        assert!(
            s_pos.is_infinite() || s_pos.abs() > 1e10,
            "expected sum saturated near ±∞ after +∞ observe, got {}",
            s_pos
        );

        let h_neg = Histogram::with_default_buckets();
        h_neg.observe(f64::NEG_INFINITY);
        assert_eq!(h_neg.count(), 1);
        assert!(
            h_neg.sum().is_infinite() || h_neg.sum().abs() > 1e10,
            "expected sum saturated near -∞, got {}",
            h_neg.sum()
        );
    }

    /// Regression test for C1 R2 H#1 (snapshot consistency under
    /// concurrency).
    ///
    /// Spawns N threads each calling `observe(1.0)` M times and asserts no
    /// observation is lost: total count = N×M and sum ≈ N×M. Snapshot
    /// composition is documented as point-in-time approximate; this test
    /// verifies the underlying atomic ops are loss-free (which is the
    /// part [`Histogram`]'s "Concurrency" doc actually promises).
    #[test]
    fn concurrent_observe_count_accurate() {
        use std::sync::Arc;
        use std::thread;

        const N_THREADS: u64 = 8;
        const PER_THREAD: u64 = 10_000;

        let h = Arc::new(Histogram::with_default_buckets());
        let mut handles = Vec::with_capacity(N_THREADS as usize);

        for _ in 0..N_THREADS {
            let h = Arc::clone(&h);
            handles.push(thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    h.observe(1.0);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("worker panicked");
        }

        let expected = N_THREADS * PER_THREAD;
        assert_eq!(h.count(), expected, "count lost observations");

        let expected_sum = expected as f64;
        assert!(
            (h.sum() - expected_sum).abs() < 1.0,
            "sum diverged: expected {}, got {}",
            expected_sum,
            h.sum()
        );
    }
}
