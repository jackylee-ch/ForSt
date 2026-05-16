# Nexmark Q3 Performance Analysis — Approaches Tried & Remaining Options

**Date**: 2026-05-14
**Target**: forst-rs ≤ 38s on Nexmark q3 (100M events, p=4, JDK 25 + ZGC)
**Current best**: 188s (0.20× vs rocksdb)
**Constraint**: Can only modify forst-rs backend code (Java + Rust), NOT Flink operators or core

## 1. Workload Characteristics

Nexmark q3: `auction INNER JOIN person ON auction.seller = person.id`
- 100M events: 2% person (2M), 6% auction (6M), 92% bid (ignored by q3)
- State: MapState with ~2M unique person keys
- Pattern: person stream writes, auction stream reads by seller ID
- RocksDB baseline: 38s (p=4, JDK 25 + ZGC)

## 2. Approaches TRIED (all failed to reach target)

### 2.1 Persistent MapState Write Cache (512K cap)
- **What**: Accumulate writes in Java HashMap, flush via batchPut. Read cache for recent lookups.
- **Result**: 188s (0.20×)
- **Why failed**: At 2M unique keys, read cache hit rate is near zero. Every read still does an FFM call. Write batching helps writes but reads dominate.

### 2.2 frs_get_fast (no catch_unwind, no Arc::clone)
- **What**: Skip Rust-side panic guard and Arc clone on the FFI hot path.
- **Result**: Reduced per-call from ~4.6µs to ~3µs. Not enough.
- **Why failed**: Saves ~1.5µs per call but 80M calls × 1.5µs = 120s savings is theoretical max. Actual savings less because other costs dominate.

### 2.3 frs_get_pinned_fast (zero Rust allocation for inline values)
- **What**: Try memtable pinned lookup first (no Vec allocation), fall back to regular get.
- **Result**: >200s (worse than 188s baseline)
- **Why failed**: At 2M keys, most lookups miss the active memtable (person was written earlier, memtable may have rotated). Pinned path returns FALLBACK, falls through to regular get anyway.

### 2.4 Lean MapState (no cache, direct calls)
- **What**: Remove all Java-side caching. Direct getPinnedFast for reads, direct put for writes.
- **Result**: >200s
- **Why failed**: Removing the write cache means every put is an individual FFM call. More total FFM calls than the cached version.

### 2.5 Write-batch only (no read cache)
- **What**: Write buffer with batchPut flush, but no read cache. Reads go directly to native.
- **Result**: ~250s (worst)
- **Why failed**: Write buffer adds HashMap lookup overhead on every read (checking if key is in buffer) without providing read-side benefit. Net negative.

### 2.6 JNI Scalar Shim (control experiment)
- **What**: Replace FFM with JNI for MapState get/put to test if FFM boundary is the bottleneck.
- **Result**: 232s (SLOWER than FFM)
- **Why failed**: Proves the bottleneck is NOT the FFM vs JNI boundary mechanism. The engine's per-call cost (RwLock + Vec alloc + hash lookup) is the same regardless of boundary type.

## 3. Root Cause (confirmed by JNI experiment)

The per-call cost breakdown (~2-3µs per state op):
- RwLock::read() acquisition: ~100ns (RocksDB: lock-free)
- Hash-index lookup: ~200ns (comparable to RocksDB skiplist)
- Vec<u8> allocation for result: ~500ns (RocksDB: PinnableSlice, zero-copy)
- Byte copy to caller buffer: ~100ns (RocksDB: DirectByteBuffer, zero-copy)
- FFM/JNI boundary crossing: ~150ns (comparable)
- Java-side byte[] allocation: ~200ns
- Total: ~1.3µs minimum per call

RocksDB achieves ~275ns because: lock-free reads + PinnableSlice + DirectByteBuffer.

With ~50-80M state ops in q3, the overhead is 50M × 1µs = 50s minimum above rocksdb's 38s = 88s theoretical floor. Actual is worse due to GC pressure from byte[] allocations.

## 4. Approaches NOT YET TRIED (within forst-rs backend constraint)

### 4.1 Lock-free Memtable Reads (Engine optimization)
- **What**: Replace `RwLock::read()` with epoch-based reclamation or crossbeam-epoch for concurrent reads without lock acquisition.
- **Expected gain**: ~100ns × 50M = 5s
- **Complexity**: High — requires redesigning memtable concurrency model
- **Risk**: Low — single-threaded Flink slot means no actual contention, but the lock acquisition syscall still costs ~100ns

### 4.2 Zero-copy PinnableSlice (Engine optimization)
- **What**: Return a reference-counted slice into the memtable instead of allocating Vec<u8>. The caller holds a "pin" that prevents the memtable from being freed.
- **Expected gain**: ~500ns × 50M = 25s (biggest single-item win)
- **Complexity**: Medium — `get_pinned` already exists but only works for active memtable inline values. Need to extend to immutable memtables.
- **Risk**: Medium — pin lifetime management across FFM boundary

### 4.3 FFM batchGet with Micro-batching at State Backend Level
- **What**: In `ForStRsMapState.get()`, accumulate N read requests and issue one `frs_batch_get(N)` call. Return results from a pre-fetched batch.
- **Challenge**: The synchronous `get()` API returns immediately. Can't defer.
- **Variant A — Speculative prefetch**: On first `get()` for a key prefix, prefetch all keys with that prefix via `frs_prefix_scan_arrow`. Cache results. Subsequent gets hit the cache.
- **Variant B — Mini-batch accumulator**: Flink's mini-batch framework calls `processElement` in batches. Intercept at the state backend level: on first state access in a mini-batch, scan all keys that will be accessed (from the mini-batch buffer) and prefetch them.
- **Expected gain**: With batch=64, per-key cost drops from ~3µs to ~300ns. 50M × 300ns = 15s overhead → total ~53s (1.4×)
- **Complexity**: High — requires understanding Flink's mini-batch internals
- **Risk**: High — may not be possible without changing Flink operator code

### 4.4 Prefix-scan Prefetch for Join Build Side
- **What**: For the person (build) side of the join, when a new person arrives, we write it. When an auction arrives, we read by seller ID. Since person IDs are sequential (1..2M), we can prefetch a range of persons into a Java-side cache on the first miss.
- **Implementation**: On `MapState.get(sellerId)` miss, call `frs_prefix_scan_arrow(prefix)` to load ALL persons for the current key-group into a local HashMap. Subsequent gets hit the HashMap.
- **Expected gain**: 1 FFM call per key-group instead of 1 per auction. With 128 key-groups and 6M auctions: 128 scans vs 6M gets. Massive reduction.
- **Complexity**: Medium — need to detect "first miss" and trigger bulk load
- **Risk**: Memory — loading all 2M persons into Java heap (~200MB). May cause GC pressure.
- **Mitigation**: Only prefetch for the current key-group (2M/128 = 15K persons per group = ~1.5MB)

### 4.5 Engine-level Read-ahead Cache (Rust side)
- **What**: Add a Rust-side LRU cache in front of the memtable. On `get()`, check cache first. On `batch_get()`, populate cache with results.
- **Expected gain**: Depends on temporal locality. For q3 join, same person may be looked up by multiple auctions → cache hit.
- **Complexity**: Low — simple HashMap in Rust
- **Risk**: Low — transparent optimization, no API change

### 4.6 Columnar State Layout (Arrow-native)
- **What**: Store person records in Arrow columnar format. Join lookups become Arrow filter operations on the column. Entire state is one Arrow RecordBatch per key-group.
- **Expected gain**: Eliminates per-row FFM calls entirely. One FFM call loads entire state.
- **Complexity**: Very high — requires redesigning state serialization
- **Risk**: High — may not fit Flink's per-key state model

### 4.7 Reduce Total State Operations
- **What**: Analyze why q3 has 50-80M state ops for 100M events. The join operator may be doing redundant state accesses. Optimize the MapState adapter to reduce unnecessary calls.
- **Implementation**: Profile which state methods are called and how often. Add counters.
- **Expected gain**: Unknown — depends on findings
- **Complexity**: Low — just instrumentation first

## 5. Recommended Priority (within constraint: only forst-rs backend)

| Priority | Approach | Expected Result | Effort |
|----------|----------|----------------|--------|
| **P0** | 4.4 Prefix-scan prefetch | ~45-55s (1.2-1.4×) | 2-3 days |
| **P1** | 4.2 Zero-copy PinnableSlice for imm memtables | -25s from any approach | 3-5 days |
| **P2** | 4.5 Engine read-ahead cache | -10-20s if locality exists | 1-2 days |
| **P3** | 4.7 Reduce total state ops (profile first) | Unknown | 1 day |
| **P4** | 4.3 FFM batchGet micro-batch | ~53s (1.4×) | 5+ days |
| **P5** | 4.1 Lock-free reads | -5s | 3-5 days |
| **P6** | 4.6 Arrow-native state | Theoretical parity | Weeks |

## 6. Recommended Next Step

**P0: Prefix-scan prefetch** is the highest-leverage approach within our constraint:
- On first `MapState.get()` miss for a key-group, call `frs_prefix_scan_arrow(prefix)` to bulk-load all entries for that state+key-group into a Java HashMap.
- Subsequent gets for the same key-group hit the HashMap (zero FFM calls).
- With 128 key-groups: 128 prefix scans × ~1ms each = 128ms total FFM cost (vs 6M × 3µs = 18s currently).
- The HashMap serves as a read cache with ~100% hit rate (all persons are pre-loaded).
- Memory: ~15K persons per key-group × ~100B each = ~1.5MB per group. Acceptable.
- This is implementable entirely within `ForStRsMapState` — no Flink operator changes needed.
