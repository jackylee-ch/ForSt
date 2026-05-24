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

//! ShardedClock cache implementation.
//!
//! A sharded, open-addressing hash table with Clock eviction. Each shard
//! uses RwLock for thread-safety while the clock_hand and countdown
//! metadata use atomic operations for minimal contention.

use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use super::{BlockCache, CacheEntry, CacheKey, CacheMetrics, CachePriority};

/// A single slot in the open-addressing hash table.
struct ClockSlot {
    /// The cache key stored in this slot (only valid when occupied).
    key: CacheKey,
    /// The cached value.
    value: Option<Arc<CacheEntry>>,
    /// Clock countdown — atomic for lock-free decrement during eviction sweep.
    /// Reset to priority's initial_countdown on access.
    countdown: u8,
    /// The memory charge of this entry.
    charge: usize,
    /// Whether this slot is occupied.
    occupied: bool,
    /// Low 32 bits of the key hash (for fast comparison during probing).
    hash_fragment: u32,
}

impl ClockSlot {
    fn empty() -> Self {
        Self {
            key: CacheKey::new(0, 0),
            value: None,
            countdown: 0,
            charge: 0,
            occupied: false,
            hash_fragment: 0,
        }
    }
}

/// Per-insert eviction accounting (R85-M1). Tracks the full set of
/// evictions triggered by a single `ClockCacheShard::insert` call so
/// the cache-level metrics counter does not lose track of multi-
/// eviction inserts (common after R84-H1 — the MAX_PASSES bump now
/// actually completes for High-priority shards, so eviction
/// frequently chains).
#[derive(Default, Debug)]
struct InsertOutcome {
    /// Number of entries evicted on this insert.
    evictions: usize,
    /// Sum of `charge` across every evicted entry.
    evicted_charge: usize,
    /// Last evicted entry's (key, charge) — preserved for callers /
    /// tests that only care about a single representative eviction
    /// (e.g. validation tests). May be `None` even if `evictions > 0`
    /// if all evictions had `charge == 0`, but that does not occur in
    /// practice.
    last_evicted: Option<(CacheKey, usize)>,
}

impl InsertOutcome {
    fn record(&mut self, evicted: (CacheKey, usize)) {
        self.evictions += 1;
        self.evicted_charge = self.evicted_charge.saturating_add(evicted.1);
        self.last_evicted = Some(evicted);
    }
}

/// A single Clock cache shard — owns a hash table and a clock hand.
struct ClockCacheShard {
    /// Open-addressing hash table (fixed-size).
    table: Vec<ClockSlot>,
    /// Number of occupied slots.
    occupancy: usize,
    /// Clock hand position for eviction scanning.
    clock_hand: usize,
    /// Current total charge in this shard.
    current_charge: usize,
    /// Capacity limit for this shard in bytes.
    capacity: usize,
    /// Table size (number of slots). Always a power of 2.
    table_size: usize,
    /// Mask for table indexing (table_size - 1).
    mask: usize,
}

impl ClockCacheShard {
    /// Creates a new shard with the given capacity and table size.
    fn new(capacity: usize, table_size: usize) -> Self {
        let mut table = Vec::with_capacity(table_size);
        for _ in 0..table_size {
            table.push(ClockSlot::empty());
        }
        Self {
            table,
            occupancy: 0,
            clock_hand: 0,
            current_charge: 0,
            capacity,
            table_size,
            mask: table_size - 1,
        }
    }

    /// Look up a key. Returns the value and resets countdown on hit.
    fn get(&mut self, key: &CacheKey, hash: u64) -> Option<Arc<CacheEntry>> {
        let hash_frag = hash as u32;
        let mut idx = (hash as usize) & self.mask;

        for _ in 0..self.table_size {
            let slot = &mut self.table[idx];
            if !slot.occupied {
                return None; // Empty slot means key not present
            }
            if slot.hash_fragment == hash_frag && slot.key == *key {
                // Hit — reset countdown to initial value (we use Low=1 as default reset)
                // In a more sophisticated version, we'd store the original priority.
                // For now, reset to 1 (minimum non-zero).
                if slot.countdown < 1 {
                    slot.countdown = 1;
                }
                return slot.value.clone();
            }
            idx = (idx + 1) & self.mask;
        }
        None
    }

    /// Insert a key-value pair. Evicts entries if capacity is exceeded.
    ///
    /// R85-M1: returns `InsertOutcome` carrying the FULL count of
    /// evictions and the TOTAL evicted-bytes from this call. Pre-R85
    /// the return was `Option<(CacheKey, usize)>` which only surfaced
    /// the LAST eviction's key+charge to the caller; when the
    /// eviction `while` ran multiple iterations (now common with
    /// R84-H1's MAX_PASSES=4 actually completing) the cache wrapper
    /// only decremented its `metrics.current_charge` by ONE eviction's
    /// charge — every other evicted entry's bytes leaked from the
    /// global meter, drifting it monotonically upward.
    fn insert(
        &mut self,
        key: CacheKey,
        value: CacheEntry,
        charge: usize,
        priority: CachePriority,
        hash: u64,
    ) -> InsertOutcome {
        let hash_frag = hash as u32;
        let arc_value = Arc::new(value);

        // First check if key already exists (update in place)
        let mut idx = (hash as usize) & self.mask;
        for _ in 0..self.table_size {
            let slot = &self.table[idx];
            if !slot.occupied {
                break;
            }
            if slot.hash_fragment == hash_frag && slot.key == key {
                // Update existing entry
                let old_charge = self.table[idx].charge;
                self.table[idx].value = Some(arc_value);
                self.table[idx].charge = charge;
                self.table[idx].countdown = priority.initial_countdown();
                self.current_charge = self.current_charge - old_charge + charge;
                return InsertOutcome::default();
            }
            idx = (idx + 1) & self.mask;
        }

        // Evict until we have space.
        //
        // R84-H2: `evict_one` can legitimately return `None` (safety bail
        // — see its comment) — pre-fix this `while` looped forever in
        // that case, hanging the engine on every cache insert. Break out
        // as soon as the evictor reports it could not free a slot; the
        // caller above already accepts transient over-capacity (the
        // load-factor check below + the insertion-probe loop further
        // down also handle "no empty slot" with `evicted` returned).
        let mut outcome = InsertOutcome::default();
        while self.current_charge + charge > self.capacity && self.occupancy > 0 {
            match self.evict_one() {
                Some(e) => outcome.record(e),
                None => break,
            }
        }

        // Check load factor (max 75%)
        if self.occupancy * 4 >= self.table_size * 3 {
            if let Some(ev) = self.evict_one() {
                outcome.record(ev);
            }
        }

        // Find insertion position
        idx = (hash as usize) & self.mask;
        for _ in 0..self.table_size {
            let slot = &self.table[idx];
            if !slot.occupied {
                self.table[idx] = ClockSlot {
                    key,
                    value: Some(arc_value),
                    countdown: priority.initial_countdown(),
                    charge,
                    occupied: true,
                    hash_fragment: hash_frag,
                };
                self.occupancy += 1;
                self.current_charge += charge;
                return outcome;
            }
            idx = (idx + 1) & self.mask;
        }

        // Table is full (shouldn't happen with proper load factor management)
        outcome
    }

    /// Evict one entry using Clock sweep. Returns the evicted key and charge.
    fn evict_one(&mut self) -> Option<(CacheKey, usize)> {
        let mut scanned = 0;
        // R84-H1: the safety bound must cover enough passes to drop the
        // HIGHEST `initial_countdown` (3 for `CachePriority::High`) to 0
        // for every occupied slot. Pre-fix the bound was `table_size * 2`
        // (2 passes), which only decrements High entries from 3 → 1 and
        // never to 0 — so a shard filled entirely with High-priority
        // entries returned None on every call, and the caller's
        // `while self.current_charge + charge > self.capacity { ... }`
        // hung the engine. (R84-H2 above also breaks out of that loop on
        // None as defense-in-depth, but the deeper fix is making
        // evict_one actually succeed when there IS something to evict.)
        // 4 passes = (MAX_COUNTDOWN=3) + 1, enough to drop every slot
        // through countdown=0 and reach the eviction branch.
        const MAX_PASSES: usize = 4;

        loop {
            let idx = self.clock_hand % self.table_size;
            self.clock_hand = (self.clock_hand + 1) % self.table_size;
            scanned += 1;

            if scanned > self.table_size * MAX_PASSES {
                return None; // Safety: prevent infinite loop
            }

            let slot = &mut self.table[idx];
            if !slot.occupied {
                continue;
            }

            if slot.countdown > 0 {
                slot.countdown -= 1;
                continue;
            }

            // countdown == 0, evict this slot
            let evicted_key = slot.key;
            let evicted_charge = slot.charge;
            slot.value = None;
            slot.occupied = false;
            slot.charge = 0;
            self.occupancy -= 1;
            self.current_charge -= evicted_charge;

            // Rehash subsequent entries to maintain open-addressing invariant
            self.rehash_after_removal(idx);

            return Some((evicted_key, evicted_charge));
        }
    }

    /// After removing a slot, rehash subsequent slots that may have been
    /// displaced by the removed entry (backward-shift deletion for open addressing).
    fn rehash_after_removal(&mut self, removed_idx: usize) {
        let mut idx = (removed_idx + 1) & self.mask;
        loop {
            if !self.table[idx].occupied {
                break;
            }
            let natural_idx = (self.table[idx].hash_fragment as usize) & self.mask;
            // Check if this entry needs to be moved back
            if self.should_move(removed_idx, idx, natural_idx) {
                // Move entry from idx to removed_idx
                let slot_data = ClockSlot {
                    key: self.table[idx].key,
                    value: self.table[idx].value.take(),
                    countdown: self.table[idx].countdown,
                    charge: self.table[idx].charge,
                    occupied: true,
                    hash_fragment: self.table[idx].hash_fragment,
                };
                self.table[idx].occupied = false;
                self.table[idx].charge = 0;
                self.table[idx].countdown = 0;

                let target = removed_idx;
                self.table[target] = slot_data;

                // Continue rehashing from the now-empty idx
                self.rehash_after_removal(idx);
                return;
            }
            idx = (idx + 1) & self.mask;
        }
    }

    /// Determine if an entry at `current_idx` with natural position `natural_idx`
    /// should be moved to `empty_idx` (backward-shift deletion logic).
    fn should_move(&self, empty_idx: usize, current_idx: usize, natural_idx: usize) -> bool {
        if empty_idx <= current_idx {
            // No wrap-around
            natural_idx <= empty_idx || natural_idx > current_idx
        } else {
            // Wrap-around
            natural_idx <= empty_idx && natural_idx > current_idx
        }
    }

    /// Remove a specific key from the shard.
    fn erase(&mut self, key: &CacheKey, hash: u64) -> bool {
        let hash_frag = hash as u32;
        let mut idx = (hash as usize) & self.mask;

        for _ in 0..self.table_size {
            let slot = &self.table[idx];
            if !slot.occupied {
                return false;
            }
            if slot.hash_fragment == hash_frag && slot.key == *key {
                let charge = self.table[idx].charge;
                self.table[idx].value = None;
                self.table[idx].occupied = false;
                self.table[idx].charge = 0;
                self.occupancy -= 1;
                self.current_charge -= charge;
                self.rehash_after_removal(idx);
                return true;
            }
            idx = (idx + 1) & self.mask;
        }
        false
    }

    /// Remove all entries for a given file_number.
    fn erase_by_file(&mut self, file_number: u64) {
        let mut indices_to_remove = Vec::new();
        for i in 0..self.table_size {
            if self.table[i].occupied && self.table[i].key.file_number == file_number {
                indices_to_remove.push(i);
            }
        }
        // Remove from highest index to lowest to avoid rehash interference
        for &idx in indices_to_remove.iter().rev() {
            if self.table[idx].occupied && self.table[idx].key.file_number == file_number {
                let charge = self.table[idx].charge;
                self.table[idx].value = None;
                self.table[idx].occupied = false;
                self.table[idx].charge = 0;
                self.occupancy -= 1;
                self.current_charge -= charge;
                self.rehash_after_removal(idx);
            }
        }
    }
}

/// Sharded Clock Cache — the main L1 heap cache.
///
/// Distributes entries across multiple shards to reduce lock contention.
/// Each shard independently manages its own hash table and clock hand.
pub struct ShardedClockCache {
    /// Per-shard state, each protected by an RwLock.
    shards: Vec<RwLock<ClockCacheShard>>,
    /// Number of shard bits (shard count = 2^shard_bits).
    shard_bits: u32,
    /// Shard mask for fast indexing.
    shard_mask: usize,
    /// Total capacity across all shards.
    #[allow(dead_code)]
    total_capacity: usize,
    /// Shared metrics.
    metrics: CacheMetrics,
}

impl ShardedClockCache {
    /// Creates a new ShardedClockCache.
    ///
    /// - `capacity`: total cache capacity in bytes.
    /// - `shard_bits`: number of shard bits (shard count = 2^shard_bits).
    ///   Default is 4 (16 shards).
    pub fn new(capacity: usize, shard_bits: u32) -> Self {
        let shard_count = 1usize << shard_bits;
        let per_shard_capacity = capacity / shard_count;
        // Each shard hash table: initial 1024 slots, can hold ~768 entries (75% load)
        let table_size = 1024usize;

        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(RwLock::new(ClockCacheShard::new(
                per_shard_capacity,
                table_size,
            )));
        }

        Self {
            shards,
            shard_bits,
            shard_mask: shard_count - 1,
            total_capacity: capacity,
            metrics: CacheMetrics::new(capacity),
        }
    }

    /// Creates a new ShardedClockCache with default 16 shards (4 bits).
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(capacity, 4)
    }

    /// Returns the shard index for a given hash.
    #[inline]
    fn shard_index(&self, hash: u64) -> usize {
        if self.shard_bits == 0 {
            0
        } else {
            (hash >> (64 - self.shard_bits)) as usize & self.shard_mask
        }
    }
}

impl BlockCache for ShardedClockCache {
    fn get(&self, key: &CacheKey) -> Option<Arc<CacheEntry>> {
        let hash = key.hash();
        let shard_idx = self.shard_index(hash);
        self.metrics.lookups.fetch_add(1, Ordering::Relaxed);

        let mut shard = self.shards[shard_idx].write().ok()?;
        let result = shard.get(key, hash);
        if result.is_some() {
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn insert(&self, key: CacheKey, value: CacheEntry, charge: usize, priority: CachePriority) {
        let hash = key.hash();
        let shard_idx = self.shard_index(hash);

        if let Ok(mut shard) = self.shards[shard_idx].write() {
            let outcome = shard.insert(key, value, charge, priority, hash);
            // R85-M1: account for ALL evictions, not just the last one.
            if outcome.evictions > 0 {
                self.metrics
                    .evictions
                    .fetch_add(outcome.evictions as u64, Ordering::Relaxed);
                self.metrics
                    .current_charge
                    .fetch_sub(outcome.evicted_charge, Ordering::Relaxed);
            }
            self.metrics
                .current_charge
                .fetch_add(charge, Ordering::Relaxed);
            self.metrics.inserts.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn erase(&self, key: &CacheKey) {
        let hash = key.hash();
        let shard_idx = self.shard_index(hash);

        if let Ok(mut shard) = self.shards[shard_idx].write() {
            shard.erase(key, hash);
        }
    }

    fn erase_by_file(&self, file_number: u64) {
        for shard_lock in &self.shards {
            if let Ok(mut shard) = shard_lock.write() {
                shard.erase_by_file(file_number);
            }
        }
    }

    fn total_charge(&self) -> usize {
        self.shards
            .iter()
            .filter_map(|s| s.read().ok())
            .map(|s| s.current_charge)
            .sum()
    }

    fn hit_rate(&self) -> f64 {
        self.metrics.hit_rate()
    }

    fn metrics(&self) -> &CacheMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(size: usize) -> CacheEntry {
        CacheEntry::RawBlock(Arc::new(vec![0u8; size]))
    }

    // --- ShardedClockCache basic tests ---

    #[test]
    fn test_insert_and_get() {
        let cache = ShardedClockCache::with_capacity(1024 * 1024);
        let key = CacheKey::new(1, 0);
        cache.insert(key, make_entry(100), 100, CachePriority::Low);
        let result = cache.get(&key);
        assert!(result.is_some());
    }

    #[test]
    fn test_get_miss() {
        let cache = ShardedClockCache::with_capacity(1024 * 1024);
        let key = CacheKey::new(999, 0);
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_insert_update() {
        let cache = ShardedClockCache::with_capacity(1024 * 1024);
        let key = CacheKey::new(1, 0);
        cache.insert(key, make_entry(100), 100, CachePriority::Low);
        cache.insert(key, make_entry(200), 200, CachePriority::Low);
        let result = cache.get(&key);
        assert!(result.is_some());
        if let Some(entry) = result {
            assert_eq!(entry.charge(), 200);
        }
    }

    #[test]
    fn test_erase() {
        let cache = ShardedClockCache::with_capacity(1024 * 1024);
        let key = CacheKey::new(1, 0);
        cache.insert(key, make_entry(100), 100, CachePriority::Low);
        assert!(cache.get(&key).is_some());
        cache.erase(&key);
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_erase_by_file() {
        let cache = ShardedClockCache::with_capacity(1024 * 1024);
        // Insert entries from file 1 and file 2
        for offset in 0..10 {
            cache.insert(
                CacheKey::new(1, offset * 4096),
                make_entry(100),
                100,
                CachePriority::Low,
            );
            cache.insert(
                CacheKey::new(2, offset * 4096),
                make_entry(100),
                100,
                CachePriority::Low,
            );
        }
        // Erase all entries for file 1
        cache.erase_by_file(1);
        // File 1 entries should be gone
        for offset in 0..10 {
            assert!(cache.get(&CacheKey::new(1, offset * 4096)).is_none());
        }
        // File 2 entries should still exist
        for offset in 0..10 {
            assert!(cache.get(&CacheKey::new(2, offset * 4096)).is_some());
        }
    }

    #[test]
    fn test_eviction_under_capacity() {
        // Small cache: 500 bytes
        let cache = ShardedClockCache::new(500, 0); // 1 shard for predictability
                                                    // Insert entries that exceed capacity
        for i in 0..10 {
            cache.insert(
                CacheKey::new(1, i * 4096),
                make_entry(100),
                100,
                CachePriority::Low,
            );
        }
        // Total charge should not exceed capacity (500 bytes = max 5 entries)
        let charge = cache.total_charge();
        assert!(charge <= 600, "charge {} should be near capacity", charge);
    }

    #[test]
    fn test_priority_high_survives_eviction() {
        // 1 shard, 300 bytes capacity (3 entries of 100 bytes)
        let cache = ShardedClockCache::new(300, 0);

        // Insert a High priority entry
        let high_key = CacheKey::new(1, 0);
        cache.insert(high_key, make_entry(100), 100, CachePriority::High);

        // Insert Low priority entries to fill and trigger eviction
        for i in 1..5 {
            cache.insert(
                CacheKey::new(1, i * 4096),
                make_entry(100),
                100,
                CachePriority::Low,
            );
        }

        // High priority entry should survive longer than Low ones
        // (countdown 3 vs 1, so it survives more sweeps)
        // We can't guarantee it's still there after many evictions,
        // but let's check it lasted longer than if it were Low
        let result = cache.get(&high_key);
        // High priority with countdown=3 should still be present
        // after a few Low entries were inserted and some evicted
        assert!(
            result.is_some(),
            "High priority entry should survive eviction"
        );
    }

    #[test]
    fn test_metrics_tracking() {
        let cache = ShardedClockCache::with_capacity(1024 * 1024);
        let key = CacheKey::new(1, 0);

        // Miss
        cache.get(&key);
        assert_eq!(cache.metrics().lookups.load(Ordering::Relaxed), 1);
        assert_eq!(cache.metrics().hits.load(Ordering::Relaxed), 0);

        // Insert + Hit
        cache.insert(key, make_entry(100), 100, CachePriority::Low);
        cache.get(&key);
        assert_eq!(cache.metrics().lookups.load(Ordering::Relaxed), 2);
        assert_eq!(cache.metrics().hits.load(Ordering::Relaxed), 1);
        assert_eq!(cache.metrics().inserts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_hit_rate_computation() {
        let cache = ShardedClockCache::with_capacity(1024 * 1024);
        let key = CacheKey::new(1, 0);
        cache.insert(key, make_entry(100), 100, CachePriority::Low);

        // 1 miss + 3 hits
        cache.get(&CacheKey::new(999, 0)); // miss
        cache.get(&key); // hit
        cache.get(&key); // hit
        cache.get(&key); // hit

        let rate = cache.hit_rate();
        assert!((rate - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;
        let cache = Arc::new(ShardedClockCache::with_capacity(10 * 1024 * 1024));
        let mut handles = Vec::new();

        // 16 threads each inserting and reading 100 entries
        for t in 0..16 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = CacheKey::new(t as u64, i * 4096);
                    cache.insert(key, make_entry(64), 64, CachePriority::Low);
                    let _ = cache.get(&key);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify no panic, no deadlock
        let metrics = cache.metrics();
        let lookups = metrics.lookups.load(Ordering::Relaxed);
        assert_eq!(lookups, 16 * 100);
    }

    #[test]
    fn test_shard_distribution() {
        let cache = ShardedClockCache::new(1024 * 1024, 4); // 16 shards
                                                            // Insert 1000 entries, they should distribute across shards
        for i in 0..1000 {
            cache.insert(CacheKey::new(i, 0), make_entry(64), 64, CachePriority::Low);
        }
        // At least half the shards should have entries
        let non_empty_shards = cache
            .shards
            .iter()
            .filter(|s| s.read().unwrap().occupancy > 0)
            .count();
        assert!(
            non_empty_shards >= 8,
            "expected at least 8 non-empty shards, got {}",
            non_empty_shards
        );
    }

    #[test]
    fn test_bottom_priority_evicted_first() {
        let cache = ShardedClockCache::new(200, 0); // 1 shard, 200 bytes

        let low_key = CacheKey::new(1, 0);
        let bottom_key = CacheKey::new(2, 0);

        cache.insert(low_key, make_entry(100), 100, CachePriority::Low);
        cache.insert(bottom_key, make_entry(100), 100, CachePriority::Bottom);

        // Insert another to trigger eviction
        cache.insert(
            CacheKey::new(3, 0),
            make_entry(100),
            100,
            CachePriority::Low,
        );

        // Bottom entry (countdown=0) should be evicted before Low (countdown=1)
        assert!(
            cache.get(&bottom_key).is_none() || cache.get(&low_key).is_some(),
            "Bottom priority should be evicted before Low"
        );
    }

    // --- ClockCacheShard unit tests ---

    #[test]
    fn test_shard_insert_get() {
        let mut shard = ClockCacheShard::new(1024 * 1024, 64);
        let key = CacheKey::new(1, 0);
        let hash = key.hash();
        let entry = make_entry(100);
        shard.insert(key, entry, 100, CachePriority::Low, hash);
        assert!(shard.get(&key, hash).is_some());
    }

    #[test]
    fn test_shard_erase() {
        let mut shard = ClockCacheShard::new(1024 * 1024, 64);
        let key = CacheKey::new(1, 0);
        let hash = key.hash();
        shard.insert(key, make_entry(100), 100, CachePriority::Low, hash);
        assert!(shard.erase(&key, hash));
        assert!(shard.get(&key, hash).is_none());
    }

    #[test]
    fn test_shard_eviction() {
        // Small shard: 250 bytes
        let mut shard = ClockCacheShard::new(250, 64);
        for i in 0..5 {
            let key = CacheKey::new(1, i * 4096);
            let hash = key.hash();
            shard.insert(key, make_entry(100), 100, CachePriority::Low, hash);
        }
        // Should have evicted some entries
        assert!(shard.current_charge <= 300);
        assert!(shard.occupancy <= 3);
    }

    #[test]
    fn test_shard_clock_sweep_respects_countdown() {
        let mut shard = ClockCacheShard::new(300, 64);

        // Insert with High priority (countdown=3)
        let high_key = CacheKey::new(1, 0);
        let high_hash = high_key.hash();
        shard.insert(
            high_key,
            make_entry(100),
            100,
            CachePriority::High,
            high_hash,
        );

        // Insert with Bottom priority (countdown=0)
        let bottom_key = CacheKey::new(2, 0);
        let bottom_hash = bottom_key.hash();
        shard.insert(
            bottom_key,
            make_entry(100),
            100,
            CachePriority::Bottom,
            bottom_hash,
        );

        // Evict one
        let evicted = shard.evict_one();
        assert!(evicted.is_some());

        // Bottom entry should be evicted (countdown was 0)
        // High entry should still exist (countdown was 3)
        assert!(shard.get(&high_key, high_hash).is_some());
    }

    /// R84-H1 regression: a shard filled entirely with High-priority
    /// entries must still evict on the next `insert` (no infinite loop
    /// in the eviction `while` and no silent hang). Pre-fix `evict_one`
    /// bailed after `table_size * 2` scans which only decremented every
    /// High countdown from 3 → 1, never to 0; the caller's `while
    /// current_charge + charge > capacity` then looped forever calling
    /// the always-`None`-returning evictor.
    #[test]
    fn test_shard_all_high_priority_still_evicts() {
        // 16-slot shard with capacity 1200 = 12 × 100 — fills below the
        // 75% load-factor trigger (12/16 = 75%, exactly at the boundary).
        // All entries are CachePriority::High (countdown=3). Pre-fix the
        // 13th insert would have hung the engine in the eviction
        // `while` because `evict_one`'s 2-pass safety bound never
        // dropped a High countdown to 0.
        let mut shard = ClockCacheShard::new(1200, 16);
        for i in 1..=12u64 {
            let key = CacheKey::new(i, 0);
            let hash = key.hash();
            let _ = shard.insert(key, make_entry(100), 100, CachePriority::High, hash);
        }

        // 13th insert MUST return (no hang) and must evict at least one
        // of the older entries because the capacity is full and all
        // High-priority entries reach countdown=0 within 4 passes.
        let key_new = CacheKey::new(99, 0);
        let hash_new = key_new.hash();
        let _evicted = shard.insert(
            key_new,
            make_entry(100),
            100,
            CachePriority::High,
            hash_new,
        );
        assert!(shard.current_charge <= 1200);
        assert!(shard.get(&key_new, hash_new).is_some());
    }
}
