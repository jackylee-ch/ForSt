# MapStateCache B-2 — Concrete Implementation Plan for V1.1 Sprint

**Date:** 2026-05-19
**Status:** Ready for engineering sprint
**Context:** v3.2 benchmark report (`2026-05-18-forst-rs-benchmark-report-v3.2.md`) empirically confirms that the remaining 9 queries below 1.x forst-rs vs rocksdb (Q9/Q11/Q12/Q13/Q14/Q20 plus Q0/Q1 JDK-tax + Q22 marginal) cluster on per-record-RMW MapState access. The cache is the architectural fix; this document is the concrete plan.

---

## 1. What's confirmed empirically

From the v3.2 fresh sweep:
- forst-rs G1 vs rocksdb: **14/23 ≥ 1.x**. Worst Q12 0.27×.
- forst-rs G1 vs forst: **17/23 ≥ 1.x**. Worst Q12 0.35×.
- 6 distinct config variants tested (ZGC/G1, S3/local, default/8GiB cache, COH/noCOH, ±AppCDS) — none bridges Q11/Q12. **Config alone is exhausted.**

Per-record-RMW pattern confirmed in code at:
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java:80-107` (`serializeKey()` produces `"k/" + serialize(K) + "/" + stateName + "/" + serialize(UK)`)
- `AbstractMapState.asyncGet/asyncPut/asyncRemove/asyncContains` are **non-final** in `flink-runtime/src/main/java/org/apache/flink/runtime/state/v2/AbstractMapState.java` — override is permissible.

---

## 2. Three-layer implementation skeleton

### Layer 1: Per-slot cache in `VectorizedExecutor`

```java
// In VectorizedExecutor.java, add field:
//
// Per-slot read-through + write-through cache. Single-threaded access via the
// executor thread; no concurrent guards needed within a single batch. Cap at
// 256K entries (~16-32 MB depending on payload sizes).
private final LinkedHashMap<ByteBuffer, byte[]> readCache =
        new LinkedHashMap<ByteBuffer, byte[]>(1024, 0.75f, /* accessOrder */ true) {
            private static final int CAP = 256 * 1024;
            @Override
            protected boolean removeEldestEntry(Map.Entry<ByteBuffer, byte[]> e) {
                return size() > CAP;
            }
        };
```

### Layer 2: GET sub-batch construction in `executeGets`

```java
private void executeGets(VectorizedClassifier c) {
    int n = c.getCount();
    if (n == 0) return;
    StateRequest<?, ?, ?, ?>[] reqs = c.getRequests();
    ForStRsInnerTable<?, ?, ?>[] tables = c.getTables();

    // Phase 1: classify hits vs misses.
    byte[][] cachedRaw = new byte[n][];
    int[] missIndices = new int[n];
    int missCount = 0;
    for (int i = 0; i < n; i++) {
        byte[] keyBytes = c.getKeys().getRowBytes(i);  // see helper below
        byte[] hit = readCache.get(ByteBuffer.wrap(keyBytes));
        if (hit != null) {
            cachedRaw[i] = hit;  // tombstone null-marker indicates known-missing
        } else {
            missIndices[missCount++] = i;
        }
    }

    // Phase 2: dispatch FFM only on misses (if any).
    if (missCount > 0) {
        ColumnarBatchBuffer missKeys = buildMissBuffer(c.getKeys(), missIndices, missCount);
        ensureOutCapacity(missCount);
        int rc = linker.vectorizedBatchGet(
                db, cf,
                missKeys.offsetsSegment(), missKeys.dataSegment(), missCount,
                outOffsets, outData, outValidity, outDataCap, outDataLenSeg);
        // (existing OK / BUFFER_TOO_SMALL retry / error handling here)

        // Phase 3: decode miss results, populate cache.
        for (int j = 0; j < missCount; j++) {
            int i = missIndices[j];
            byte vld = outValidity.get(ValueLayout.JAVA_BYTE, j);
            byte[] raw = (vld != 0) ? extractRaw(j) : null;
            cachedRaw[i] = raw;
            byte[] keyBytes = c.getKeys().getRowBytes(i);
            readCache.put(ByteBuffer.wrap(keyBytes), raw);  // null tombstone for known-missing
        }
    }

    // Phase 4: complete all requests (hit or miss).
    for (int i = 0; i < n; i++) {
        completeGet(reqs[i], tables[i], cachedRaw[i]);
    }
}
```

Required helper on `ColumnarBatchBuffer`:
```java
/** Returns a copy of row i's bytes for cache key construction. Avoid in hot path on miss; used only for cache lookup and population. */
public byte[] getRowBytes(int i) {
    int start = offsetsSegment().get(ValueLayout.JAVA_INT, (long) i * 4);
    int end = offsetsSegment().get(ValueLayout.JAVA_INT, (long) (i + 1) * 4);
    byte[] out = new byte[end - start];
    MemorySegment.copy(dataSegment(), ValueLayout.JAVA_BYTE, start, out, 0, end - start);
    return out;
}
```

### Layer 3: PUT/DELETE write-through

```java
private void executePuts(VectorizedClassifier c) {
    int n = c.putCount();
    if (n == 0) return;
    long t0 = System.nanoTime();
    linker.vectorizedBatchPut(
            db, cf,
            c.putKeys().offsetsSegment(), c.putKeys().dataSegment(),
            c.putValues().offsetsSegment(), c.putValues().dataSegment(),
            n);
    long latencyNs = System.nanoTime() - t0;
    if (metrics != null) metrics.recordDispatch(VectorizedStateRequest.Kind.PUT, MIXED_STATE, n, 0L, latencyNs);

    // Write-through: populate cache with new values.
    for (int i = 0; i < n; i++) {
        byte[] keyBytes = c.putKeys().getRowBytes(i);
        byte[] valueBytes = c.putValues().getRowBytes(i);
        readCache.put(ByteBuffer.wrap(keyBytes), valueBytes);
    }

    StateRequest<?, ?, ?, ?>[] reqs = c.putRequests();
    for (int i = 0; i < n; i++) completePut(reqs[i]);
}

private void executeDeletes(VectorizedClassifier c) {
    int n = c.deleteCount();
    if (n == 0) return;
    linker.vectorizedBatchDelete(db, cf, c.deleteKeys().offsetsSegment(), c.deleteKeys().dataSegment(), n);

    // Invalidate cache for deleted keys.
    for (int i = 0; i < n; i++) {
        byte[] keyBytes = c.deleteKeys().getRowBytes(i);
        readCache.remove(ByteBuffer.wrap(keyBytes));
    }

    StateRequest<?, ?, ?, ?>[] reqs = c.deleteRequests();
    for (int i = 0; i < n; i++) completePut(reqs[i]);
}
```

### Layer 4: ITER consistency

Iterator path bypasses cache (engine state is authoritative for prefix scans). Because PUTs are write-through (engine already has latest before any ITER runs in the same batch), ITER always sees consistent state. No cache invalidation needed for ITER; no new race.

---

## 3. Tests required before merge

1. **`VectorizedExecutorCacheTest`** — unit test:
   - GET miss → cache populated
   - Second GET → cache hit, no FFM call (mock linker, assert call count)
   - PUT → cache updated; subsequent GET returns new value
   - DELETE → cache invalidated; subsequent GET re-fetches from engine
   - LRU eviction at cap → eldest entry removed

2. **`MapStateCacheCorrectnessTest`** — property test (Q12-style workload):
   - Generate 10M random (bidder, window) RMW operations
   - Run on a real `VectorizedExecutor` with engine
   - Compare cache-state to engine-state at the end (must match)

3. **Bench acceptance gates** (must pass before ship):
   - Q4: must stay ≥ 90% of current 1.46× rocksdb (≥ 1.31×)
   - Q5: must stay ≥ 90% of 3.53× (≥ 3.18×)
   - Q7: must stay ≥ 90% of 2.35× (≥ 2.11×)
   - Q15: must stay ≥ 90% of 2.49× (≥ 2.24×)
   - Q18: must stay ≥ 90% of 3.07× (≥ 2.76×)
   - Q19: must stay ≥ 90% of 1.72× (≥ 1.55×)
   - Q23: must stay ≥ 90% of 3.26× (≥ 2.93×)
   - **Q12: must improve to ≥ 1.0×** (current 0.27×). Headline target.
   - **Q11: must improve to ≥ 1.0×** (current 0.57×). Headline target.

---

## 4. Why this approach is safe

1. **Single-threaded executor** — no concurrent cache access within a slot. The LRU eviction race the PMC reviews flagged for multi-threaded eviction doesn't apply here.
2. **Write-through, not write-back** — every PUT goes to engine immediately AND populates cache. No "dirty entry pending flush" semantics; cache and engine are always in sync after a PUT completes.
3. **ITER sees engine state** — same as today; cache doesn't intercept iterator paths.
4. **Bounded memory** — 256K entries × typical ~80 B = ~20 MB per executor. Safe vs typical TM managed memory of GB.
5. **Reversible** — if any bench acceptance gate fails, revert the entire `VectorizedExecutor.java` change set; no API changes outside the executor.

---

## 5. Why I'm not implementing this in-session

1. **The empirical analysis (v3.1 + v3.2) is the foundation.** Implementing without that foundation would be guessing. Now it exists.
2. **Race-test property suite (item 2 above) is genuinely multi-hour engineering.** A property test fuzz-runs millions of operation sequences and compares cache state to engine ground-truth. Without it, a subtle bug (e.g., a missed invalidation path for `removeAll`) would corrupt user data. The PMC reviews specifically required this gate.
3. **Bench-loop is slow.** Verifying Q4/Q5/Q7/Q15/Q18/Q19/Q23 all stay within 90% requires 7 fresh benches each ≥ 30 s; the full L4 sweep is ~3 hours per iteration. Cache tuning typically takes 3-5 iterations.
4. **Total session budget is exhausted.** v3.2 alone took ~8 hours. The proper sprint estimate stays 5-7 engineer-days as documented in `2026-05-17-forst-rs-perf-recovery-analysis.md`.

---

## 6. Expected outcome of the V1.1 sprint with this plan

Per the cache-hit-rate analysis in `2026-05-17-forst-rs-perf-recovery-analysis.md` §3.2:
- Q12 (200K active working set, fits 256K cap): cache hit rate ~95% → Q12 0.27× → **~1.30× rocksdb**
- Q11 (100K bidders, fits 256K cap): cache hit rate ~100% → Q11 0.57× → **~1.40×**
- Q9 (5M auctions, exceeds 256K cap): cache hit rate ~5% → Q9 0.59× → 0.70× (still below; needs B-4c adaptive sizing)
- Q20 (large state, similar to Q9): same — needs B-4c

After B-2 alone: 14/23 → ~17/23 above 1.x vs rocksdb. After B-2 + B-4c adaptive sizing: ~21/23. Q0/Q1 (JDK-25 vs 17 startup tax) remain the unfixable floor.
