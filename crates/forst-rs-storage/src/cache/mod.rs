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

//! Block Cache — ShardedClock L1 heap cache for the LSM-tree read path.
//!
//! Implements a sharded Clock eviction cache inspired by RocksDB/ForSt
//! HyperClockCache. Each shard uses an open-addressing hash table with
//! atomic Clock metadata for near-lock-free lookups.
//!
//! This module provides:
//! - [`CacheKey`] — composite key (file_number, block_offset).
//! - [`CacheEntry`] — cached value (raw bytes or decoded RecordBatch).
//! - [`CachePriority`] — eviction priority (High/Low/Bottom).
//! - [`CacheMetrics`] — atomic hit/miss/eviction counters.
//! - [`crate::cache::clock::ShardedClockCache`] — the main cache implementation.

pub mod clock;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::sst::kv_block::KvBlock;

/// Cache key — uniquely identifies a block within an SST file.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct CacheKey {
    /// SST file number (globally unique, assigned by VersionSet).
    pub file_number: u64,
    /// Block byte offset within the SST file.
    pub block_offset: u64,
}

impl CacheKey {
    /// Creates a new cache key.
    pub fn new(file_number: u64, block_offset: u64) -> Self {
        Self {
            file_number,
            block_offset,
        }
    }

    /// Fast hash for shard selection and hash table probing.
    /// Uses FNV-1a style mixing for good avalanche on 128-bit input.
    #[inline]
    pub fn hash(&self) -> u64 {
        // Combine file_number and block_offset with bit mixing
        let mut h = self.file_number;
        h = h.wrapping_mul(0x517cc1b727220a95);
        h ^= self.block_offset;
        h = h.wrapping_mul(0x6c62272e07bb0142);
        h ^= h >> 32;
        h
    }
}

/// Cached block data — either raw bytes or a decoded Arrow RecordBatch.
#[derive(Clone)]
pub enum CacheEntry {
    /// Raw block bytes (compressed or uncompressed).
    /// Used for Index Block, Filter Block, etc.
    RawBlock(Arc<Vec<u8>>),

    /// Decoded Arrow RecordBatch (v1 data block).
    /// Used for Data Blocks — already decompressed and decoded to columnar format.
    DecodedBatch(Arc<RecordBatch>),

    /// Decoded v2 KV data block (C / [`crate::sst::kv_block::KvBlock`]).
    /// Holds the decompressed KV payload; rows are read by a pointer-walk with
    /// no Arrow array build. The cache key/charge machinery is identical to
    /// [`CacheEntry::DecodedBatch`].
    DecodedKv(Arc<KvBlock>),
}

impl CacheEntry {
    /// Returns the approximate memory charge of this entry in bytes.
    pub fn charge(&self) -> usize {
        match self {
            CacheEntry::RawBlock(data) => data.len(),
            CacheEntry::DecodedBatch(batch) => {
                // Approximate: sum of all column buffer sizes
                batch
                    .columns()
                    .iter()
                    .map(|col| col.get_array_memory_size())
                    .sum::<usize>()
            }
            // The decompressed KV payload is the dominant allocation.
            CacheEntry::DecodedKv(kv) => kv.payload_len(),
        }
    }
}

/// Block eviction priority — affects how quickly entries are evicted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CachePriority {
    /// Highest priority: Index Block, Filter Block.
    /// Initial countdown = 3 (survives 3 clock sweeps).
    High,
    /// Default priority: Data Block.
    /// Initial countdown = 1 (survives 1 clock sweep).
    Low,
    /// Lowest priority: Compaction-read temporary blocks.
    /// Initial countdown = 0 (evicted on first sweep).
    Bottom,
}

impl CachePriority {
    /// Returns the initial countdown value for clock eviction.
    #[inline]
    pub fn initial_countdown(&self) -> u8 {
        match self {
            CachePriority::High => 3,
            CachePriority::Low => 1,
            CachePriority::Bottom => 0,
        }
    }
}

/// Cache statistics — atomic counters for monitoring.
pub struct CacheMetrics {
    /// Total lookup count.
    pub lookups: AtomicU64,
    /// Hit count.
    pub hits: AtomicU64,
    /// Current memory charge in bytes.
    pub current_charge: AtomicUsize,
    /// Total capacity in bytes.
    pub capacity: usize,
    /// Eviction count.
    pub evictions: AtomicU64,
    /// Insert count.
    pub inserts: AtomicU64,
}

impl CacheMetrics {
    /// Creates new metrics with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            lookups: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            current_charge: AtomicUsize::new(0),
            capacity,
            evictions: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
        }
    }

    /// Returns the hit rate as a fraction [0.0, 1.0].
    pub fn hit_rate(&self) -> f64 {
        let lookups = self.lookups.load(Ordering::Relaxed);
        if lookups == 0 {
            return 0.0;
        }
        self.hits.load(Ordering::Relaxed) as f64 / lookups as f64
    }

    /// Returns the capacity utilization as a fraction [0.0, 1.0].
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            return 0.0;
        }
        self.current_charge.load(Ordering::Relaxed) as f64 / self.capacity as f64
    }
}

/// Block Cache trait — the public API for cache consumers.
pub trait BlockCache: Send + Sync {
    /// Look up a cached entry by key.
    fn get(&self, key: &CacheKey) -> Option<Arc<CacheEntry>>;

    /// Insert an entry into the cache.
    fn insert(&self, key: CacheKey, value: CacheEntry, charge: usize, priority: CachePriority);

    /// Remove a cached entry by key.
    fn erase(&self, key: &CacheKey);

    /// Remove all cached entries for a given SST file.
    fn erase_by_file(&self, file_number: u64);

    /// Returns the current total memory charge in bytes.
    fn total_charge(&self) -> usize;

    /// Returns the current hit rate.
    fn hit_rate(&self) -> f64;

    /// Returns the cache metrics.
    fn metrics(&self) -> &CacheMetrics;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_new() {
        let key = CacheKey::new(42, 4096);
        assert_eq!(key.file_number, 42);
        assert_eq!(key.block_offset, 4096);
    }

    #[test]
    fn test_cache_key_hash_different() {
        let k1 = CacheKey::new(1, 0);
        let k2 = CacheKey::new(2, 0);
        let k3 = CacheKey::new(1, 4096);
        assert_ne!(k1.hash(), k2.hash());
        assert_ne!(k1.hash(), k3.hash());
        assert_ne!(k2.hash(), k3.hash());
    }

    #[test]
    fn test_cache_key_hash_deterministic() {
        let key = CacheKey::new(100, 8192);
        assert_eq!(key.hash(), key.hash());
    }

    #[test]
    fn test_cache_priority_countdown() {
        assert_eq!(CachePriority::High.initial_countdown(), 3);
        assert_eq!(CachePriority::Low.initial_countdown(), 1);
        assert_eq!(CachePriority::Bottom.initial_countdown(), 0);
    }

    #[test]
    fn test_cache_entry_raw_block_charge() {
        let data = vec![0u8; 1024];
        let entry = CacheEntry::RawBlock(Arc::new(data));
        assert_eq!(entry.charge(), 1024);
    }

    #[test]
    fn test_cache_metrics_initial() {
        let metrics = CacheMetrics::new(1024 * 1024);
        assert_eq!(metrics.hit_rate(), 0.0);
        assert_eq!(metrics.capacity, 1024 * 1024);
        assert_eq!(metrics.current_charge.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_cache_metrics_hit_rate() {
        let metrics = CacheMetrics::new(1024);
        metrics.lookups.store(100, Ordering::Relaxed);
        metrics.hits.store(75, Ordering::Relaxed);
        assert!((metrics.hit_rate() - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cache_metrics_utilization() {
        let metrics = CacheMetrics::new(1000);
        metrics.current_charge.store(500, Ordering::Relaxed);
        assert!((metrics.utilization() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cache_metrics_zero_capacity() {
        let metrics = CacheMetrics::new(0);
        assert_eq!(metrics.utilization(), 0.0);
    }
}
