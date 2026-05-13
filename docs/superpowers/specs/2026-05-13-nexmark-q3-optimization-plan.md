# ForSt-RS Nexmark Q3 Performance Optimization Plan

**Date**: 2026-05-13
**Goal**: Achieve 1.x× performance parity with rocksdb on Nexmark q3 (high-cardinality join)
**Current**: forst-rs 65.6s vs rocksdb 28.3s (0.43×)
**Target**: forst-rs ≤ 28s (1.0× parity) — ideally <20s (1.4× faster)

---

## Problem Analysis

### Why forst-rs is 2.3× slower on Nexmark q3

| Factor | rocksdb (JNI) | forst-rs (FFM) | Gap |
|---|---|---|---|
| Per-call boundary crossing | ~50ns (JNI) | ~150ns (FFM critical) | 3× |
| Key serialization | byte[] copy to native | byte[] copy to MemorySegment | ~same |
| Engine lookup | C++ SkipList ~280ns | Rust hash-index ~32ns | forst-rs 8.8× faster |
| Value return | JNI GetByteArrayElements | MemorySegment.toArray() | ~same |
| **Total per-op** | **~400ns** | **~4,750ns** | **~12×** |

The measured 4.75µs per FFM call (38s / 8M ops) is much higher than the expected
~200ns (150ns FFM + 32ns engine). The extra ~4.5µs comes from:

1. **Arena allocation per call**: Each `lookupKv` creates a confined Arena for the
   key MemorySegment, allocates, copies, then closes the Arena. This is ~2-3µs.
2. **Value copy-out**: The returned `FrsBytes` is copied to a Java `byte[]` via
   `MemorySegment.toArray()` which allocates a new array each time. ~1µs.
3. **HashMap overhead in read cache**: `ByteArrayKey` creation + `Arrays.hashCode()`
   + HashMap lookup on every call, even when cache is disabled. ~0.5µs.

### Why rocksdb doesn't have this problem

RocksDB's JNI path uses a **direct byte buffer** approach:
- Key bytes are passed directly from Java heap to native via `GetByteArrayElements`
  (which can pin the array without copying on most JVMs)
- Value bytes are returned via `SetByteArrayRegion` (direct copy to pre-allocated array)
- No Arena allocation, no MemorySegment intermediary

---

## Optimization Strategies (FFM-only, no JNI)

### Strategy 1: Off-Heap Shared Memory Region (Highest Impact)

**Concept**: Pre-allocate a large off-heap MemorySegment shared between Java and Rust.
State keys/values are written directly into this region without per-call allocation.

**Implementation**:
```
┌─────────────────────────────────────────────────────┐
│  Shared Off-Heap Region (e.g., 64MB)                │
│  ┌──────────┬──────────┬──────────┬──────────┐      │
│  │ Key Slot │ Val Slot │ Key Slot │ Val Slot │ ...  │
│  │ (fixed)  │ (fixed)  │ (fixed)  │ (fixed)  │      │
│  └──────────┴──────────┴──────────┴──────────┘      │
│  Java writes key → slot[i]                           │
│  Rust reads key from slot[i], writes value → slot[i] │
│  Java reads value from slot[i]                       │
│  Zero allocation per call. Zero copy.                │
└─────────────────────────────────────────────────────┘
```

**Steps**:
1. At backend init, allocate a global `Arena` with a large MemorySegment (64MB)
2. Divide into fixed-size slots (e.g., 4KB per key+value pair, 16K slots)
3. For `get()`: write key bytes into slot, call `frs_lookup_kv_inplace(slot_ptr, key_len, &out_val_len)` — Rust reads key from the slot and writes value into the same slot
4. For `put()`: write key+value into slot, call `frs_put_inplace(slot_ptr, key_len, val_len)`
5. No Arena allocation, no MemorySegment creation, no byte[] copy

**Expected improvement**: Eliminates ~3µs per call (Arena + copy overhead)
**Estimated q3 time**: 65.6s - (8M × 3µs) = 65.6 - 24 = ~41s → still not parity

### Strategy 2: Batch FFI with Deferred Execution (Critical for Parity)

**Concept**: When adaptive buffer detects high miss rate, switch to batch mode.
Accumulate N state access requests, then issue one `batch_get` FFM call.

**Implementation for MapState**:
```java
// When bufferEnabled == false (high-cardinality detected):
// Instead of: for each event → get(key) → FFM call
// Do: accumulate 64 keys → one batch_get(64 keys) → distribute results

private byte[][] pendingKeys = new byte[64][];
private int pendingCount = 0;
private byte[][] pendingResults = null;
private int resultIndex = 0;

public UV get(UK key) {
    byte[] compositeKey = composite(key);
    // Check if we have a pre-fetched result
    if (pendingResults != null && resultIndex < pendingResults.length) {
        byte[] result = pendingResults[resultIndex++];
        // ... deserialize and return
    }
    // Accumulate key for batch
    pendingKeys[pendingCount++] = compositeKey;
    if (pendingCount >= 64) {
        pendingResults = linker.batchGet(db, cf, pendingKeys);
        pendingCount = 0;
        resultIndex = 0;
        // Return first result
    }
}
```

**Challenge**: This requires the caller to call `get()` multiple times before
consuming results — works for operators that process records in batches but NOT
for the current one-record-at-a-time processing model.

**Solution**: Implement at the **operator level** — the SQL join operator accumulates
records in a mini-batch buffer, then issues batch state access for all records at once.

**Expected improvement**: 64× reduction in FFM calls → 8M/64 = 125K FFM calls
**Estimated q3 time**: 28s + (125K × 150ns) = 28s + 0.02s ≈ **28s** (parity!)

### Strategy 3: Pre-allocated Key/Value Buffers (Medium Impact)

**Concept**: Eliminate per-call `byte[]` allocation for keys and values by reusing
pre-allocated buffers.

**Current path** (per get):
```
1. computeKey() → new byte[] (allocation)
2. new ByteArrayKey(bytes) → Arrays.hashCode (computation)
3. Arena.ofConfined() → new Arena (allocation)
4. arena.allocate(key.length) → native alloc
5. MemorySegment.copy(key → native) → memcpy
6. FFM call
7. MemorySegment.toArray() → new byte[] (allocation)
8. Arena.close() → native free
```

**Optimized path**:
```
1. Write key directly into pre-allocated off-heap MemorySegment (no byte[] alloc)
2. FFM call with pointer arithmetic (no Arena)
3. Read value from pre-allocated output MemorySegment (no byte[] alloc)
```

**Implementation**: Use `MemorySegment.ofArray(byte[])` with a thread-local reusable
buffer for keys, and a pre-allocated output segment for values.

**Expected improvement**: Eliminates ~1.5µs per call
**Estimated q3 time**: 65.6 - (8M × 1.5µs) = 65.6 - 12 = ~53s

### Strategy 4: Heap-Segment Direct Pass (JDK 25 Feature)

**Concept**: JDK 25's FFM with `Linker.Option.critical(true)` allows passing
**heap byte arrays directly** without copying to off-heap. The JVM pins the array
during the native call.

**Current**: `byte[] key` → copy to `MemorySegment` (off-heap) → pass pointer to Rust
**Optimized**: `byte[] key` → `MemorySegment.ofArray(key)` → pass directly (JVM pins)

This is already partially implemented (`critical(true)` is used), but the current
code still copies to a confined Arena. The fix: pass `MemorySegment.ofArray(key)`
directly to the critical-mode handle.

**Expected improvement**: Eliminates copy + Arena overhead (~2µs per call)
**Estimated q3 time**: 65.6 - (8M × 2µs) = 65.6 - 16 = ~49s

### Strategy 5: Combined Approach (Parity Path)

Combine strategies 2 + 4 for maximum impact:

1. **Heap-segment direct pass** (Strategy 4): Eliminate Arena allocation for
   individual calls. Brings per-call cost from 4.75µs to ~2µs.
2. **Batch FFI for high-cardinality** (Strategy 2): When adaptive buffer detects
   miss rate >90%, switch to batch mode. Accumulate 64 keys per batch_get call.
3. **Off-heap shared region** (Strategy 1): For the batch path, use a pre-allocated
   shared region to stage all 64 keys without per-key allocation.

**Combined estimated q3 time**: ~28-30s (parity with rocksdb)

---

## Implementation Priority

| # | Strategy | Impact | Effort | Priority |
|---|---|---|---|---|
| 1 | Heap-segment direct pass | -16s (49s) | 1 day | P0 |
| 2 | Batch FFI for MapState | -37s (28s) | 3 days | P0 |
| 3 | Off-heap shared region | -24s (41s) | 2 days | P1 |
| 4 | Pre-allocated buffers | -12s (53s) | 1 day | P1 |
| 5 | Mini-batch join operator | parity | 1 week | P2 |

**Recommended execution order**: Strategy 1 (heap-segment) → Strategy 2 (batch FFI)
These two combined should achieve parity. Strategy 5 (mini-batch operator) is the
nuclear option if the others don't suffice.

---

## Detailed Design: Batch FFI for MapState (Strategy 2)

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Flink SQL Join Operator                                      │
│                                                              │
│  Record arrives → setCurrentKey(key)                         │
│                 → mapState.get(userKey)                       │
│                                                              │
│  Current: each get() → 1 FFM call (150ns + engine 32ns)     │
│  Proposed: accumulate 64 gets → 1 batch_get FFM call         │
│            (150ns + 64×32ns = 2.2µs total = 34ns/key)        │
└─────────────────────────────────────────────────────────────┘
```

### MapState Batch Mode

When adaptive buffer detects miss rate >90%:

```java
// Switch to batch-ahead mode
private static final int BATCH_SIZE = 64;
private final byte[][] batchKeys = new byte[BATCH_SIZE][];
private final byte[][] batchResults = new byte[BATCH_SIZE][];
private int batchFilled = 0;
private boolean batchMode = false;

public UV get(UK key) {
    byte[] compositeKey = composite(key);
    
    if (!batchMode) {
        // Normal path (cache check + single FFM)
        return getSingle(compositeKey);
    }
    
    // Batch mode: accumulate key, return from pre-fetched results
    batchKeys[batchFilled] = compositeKey;
    if (batchFilled == 0) {
        // First key in batch — we need to pre-fetch
        // Issue: we can't return the result yet because batch isn't full
        // Solution: fall back to single get for the first call,
        //           but pre-fetch the NEXT 63 keys speculatively
        return getSingle(compositeKey);
    }
    // Return from pre-fetched batch
    byte[] raw = batchResults[batchFilled - 1];
    batchFilled++;
    if (batchFilled >= BATCH_SIZE) {
        // Issue next batch pre-fetch
        batchResults = linker.batchGet(db, cf, batchKeys);
        batchFilled = 0;
    }
    return deserialize(raw);
}
```

**Challenge**: The batch approach requires knowing future keys in advance. In the
current one-record-at-a-time model, we don't know the next 63 keys when processing
the first record.

**Solution**: Implement at the **operator level** with a mini-batch buffer:
1. Operator accumulates 64 records in a buffer
2. Extracts all state keys from the 64 records
3. Issues one `batch_get(64 keys)` call
4. Processes all 64 records with the pre-fetched state values

This requires modifying the Flink SQL join operator's `processElement()` method.

---

## Conclusion

Achieving 1.x× parity on Nexmark q3 is feasible with FFM-only optimizations:

1. **Short-term** (1-2 days): Heap-segment direct pass eliminates Arena overhead,
   bringing q3 from 65s to ~49s (0.58× vs rocksdb)

2. **Medium-term** (3-5 days): Batch FFI + mini-batch operator brings q3 to ~28-30s
   (1.0× parity with rocksdb)

3. **Long-term**: With the engine's 8.78× advantage fully exposed through batch FFI,
   forst-rs can potentially beat rocksdb on joins too — but only with operator-level
   mini-batching that amortizes the FFM boundary crossing.

The key insight: **FFM is NOT inherently slower than JNI** — the overhead comes from
per-call Arena allocation and byte[] copying. Eliminating these (via heap-segment
direct pass + batch FFI) makes FFM competitive with JNI while keeping the 8.78×
engine advantage.
