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

/// A histogram that records observed values into fixed buckets.
///
/// The histogram is updated atomically — concurrent `observe()` calls
/// are safe without external synchronization.
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
    /// Sum of all observed values (stored as u64 bits of f64).
    sum_bits: AtomicU64,
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
            sum_bits: AtomicU64::new(0u64),
        }
    }

    /// Creates a histogram with [`DEFAULT_BUCKETS`].
    pub fn with_default_buckets() -> Self {
        Self::new(DEFAULT_BUCKETS)
    }

    /// Records an observed value.
    #[inline]
    pub fn observe(&self, value: f64) {
        self.total_count.fetch_add(1, Ordering::Relaxed);

        // Atomically add to sum using CAS loop on the f64 bits.
        loop {
            let old_bits = self.sum_bits.load(Ordering::Relaxed);
            let old_sum = f64::from_bits(old_bits);
            let new_sum = old_sum + value;
            let new_bits = new_sum.to_bits();
            if self
                .sum_bits
                .compare_exchange_weak(old_bits, new_bits, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

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
    #[inline]
    pub fn sum(&self) -> f64 {
        f64::from_bits(self.sum_bits.load(Ordering::Relaxed))
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
}
