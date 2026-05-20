# Forst-RS Off-Heap Arrow ValueState — End-to-End Zero-Copy + Auto-Tuned Buffer

**Author:** jackylee (PMC) + Claude
**Date:** 2026-05-20
**Branches:** `~/Code/stczwd/flink` `forst-rs-jdk25`, `~/Code/stczwd/ForSt` `forst-rs`
**Related:** [V1-sync cache-clear design](2026-05-20-forst-rs-v1-sync-state-cache-fix-design.md) (superseded by this); [v3 bench report](2026-05-20-forst-rs-benchmark-report-v3.md)

## Problem

The earlier V1-sync cache-clear fix (PR-A `bc0700f85f3`, reverted) cut Q5's per-event ForStRsValueState allocation but only delivered ~10 % on Q5 wall-clock — still 4.6× slower than rocksdb. A later experiment with a much larger shared write-buffer (256K cap, 16K flush threshold) drove Q5 from 528 s → **31 s** (16.8× faster, 3.6× FASTER than rocksdb's 114 s) — proving the FFM-elimination thesis — but broke Q11 and Q12 because:
1. The shared buffer's 16K flush stalls hurt smaller-working-set queries.
2. The V2 async backend calls `delegate.flushWriteBuffer()` via `ForStRsAbstractKeyedStateBackend:337`, so V2 paid the V1 buffer flush cost.
3. The query-specific perf-recovery work that historically delivered Q11 = 76.5 s and Q12 = 35.5 s (HEAP timer factory) was lost in a separate revert.

The remaining Java glue layer cost on the V1-sync hot path comes from per-call byte[] allocations:
- Composite key byte[] (~50 bytes × 5 panes × 100 M events = 25 GB allocated for Q5 alone)
- getPinned result byte[] (~8-32 bytes × per read)
- ByteArrayWrapper allocations on every HashMap lookup
- DataInputDeserializer wrapping byte[] for deserialization

These dominate the per-event cost once FFM crossings are amortized via buffering.

## Goal

**Beat RocksDB on Q5, Q8, Q11, Q12, Q13 simultaneously** by:
- Eliminating all byte[] allocations on the V1-sync `value()`/`update()` hot path.
- Storing all key/value data off-heap in Arrow BinaryArray-style buffers (offsets + flat data).
- Sizing the per-state-instance buffer dynamically by observed hit rate.
- Passing MemorySegment views (segment + offset + length) rather than byte[] across all internal interfaces on the hot path.

Strict KPI:
- Q5  < 113.98 s
- Q8  < 32.81 s
- Q11 ≤ 76.5 s
- Q12 ≤ 35.5 s
- Q13 < 33.75 s
- All other queries: tiered T0/T1/T2 gates from prior spec.
- Net portfolio delta `Σ log10(new/v3.3) ≤ -2.0`.

## Non-Goals

- V2 async path refactor — V2 already vectorized via VectorizedExecutor; no work here.
- Iterator path — already uses chunked vec iter; out of scope.
- Rust engine internals — only new FFM signatures (companion changes to `forst-rs` engine repo).
- Flink table-runtime / planner — unchanged.

## Architecture

### Per-ForStRsValueState instance (replaces shared backend buffer)

```
ForStRsValueState
├── scratchArena: thread-local Arena (~64 KB, ofConfined per-thread)
│     Used to encode composite keys + serialize values per-call. Reset at each value()/update() entry.
├── statebuf: ArrowBinaryBuffer  (per-state-instance, owned)
│     ├── keys:   keyOffsets[]   (off-heap int4, length = capacity+1)
│     │           keyData[]      (off-heap byte, length = avg-key-size × capacity)
│     ├── values: valueOffsets[] (off-heap int4)
│     │           valueData[]    (off-heap byte)
│     ├── hashIndex: open-addressed long→int (key-hash → row#) — primitive Long2IntOpenHashMap; no boxing
│     ├── size / capacity (capacity scales 2× on sustained ≥ 80 % hit rate, halves on ≤ 30 %)
│     ├── flushHwm = capacity / 2 (flush at half-full; never lose the steady-state cache)
│     └── tuner: ArrowBinaryBufferAutoTuner (hit/miss counters + grow/shrink hysteresis)
└── linker references: ForStRsLinker (FFM bridge to engine)
```

**Per-state-instance ownership** means each ForStRsValueState gets its own buffer sized to its access pattern. Q5's 5 panes each grow their own buffer to ~10-15K entries. Q11's accumulator gets ~1-5K. Q12 (V2 path) doesn't touch this at all.

### linker FFM contract (new)

- `getPinnedSegment(db, cf, keyArena, keyOff, keyLen, outArena, outOffOut, outLenOut) → int rc`: writes the engine's inline value bytes directly into `outArena` starting at `outArena.position`; on hit, sets `outOffOut`/`outLenOut` and returns OK. On miss, returns NOT_FOUND.
- `putSegment(db, cf, keyArena, keyOff, keyLen, valArena, valOff, valLen) → int rc`: native put with caller-owned memory. No intermediate copy.
- `batchPutSegments(db, cf, keyDataArena, keyOffArr, valDataArena, valOffArr, count) → int rc`: takes Arrow BinaryArray pointers directly. The buffer's flush calls this with its own keyData / keyOffsets / valueData / valueOffsets segments — zero per-entry copy.

The Rust side companion (FFI in `forst-rs` engine repo) is a thin wrapper around existing `frs_get_pinned` / `frs_put` / `frs_batch_put` that takes caller pointers instead of constructing slices from Java arrays.

### KeyGroupedSerializer (new method)

```java
/**
 * Off-heap composite-key encoding. Writes [ns-marker | currentKeyBytes | / | stateNameBytes | /]
 * into the supplied scratchArena starting at scratchArena.position; returns the (offset, length)
 * the caller can pass to the linker.
 */
public long encodeForStateOffheap(
        int keyGroup,
        K userKey,
        byte[] stateNameBytes,
        MemorySegment scratchArena,
        long startOffset) {
    // returns packed: (offset << 32) | length, both fit in 32 bits
}
```

Caller pattern:
```java
long off = scratchArena.position;
long encoded = serializer.encodeForStateOffheap(kg, key, stateNameBytes, scratchArena, off);
long keyOff = encoded >>> 32;
long keyLen = encoded & 0xFFFFFFFFL;
scratchArena.position += keyLen;
```

### Auto-tune policy

```
On every read:
  if buffer.find(key) hits → tuner.hits++
  tuner.samples++
  if tuner.samples == SAMPLE_WINDOW (1024):
    rate = tuner.hits / tuner.samples
    if rate >= GROW_RATE (0.80) && buffer.capacity < CAPACITY_CAP (65536):
        buffer.resize(buffer.capacity * 2)
    else if rate < SHRINK_RATE (0.30) && buffer.capacity > MIN_CAPACITY (1024):
        buffer.resize(buffer.capacity / 2)
    tuner.hits = 0
    tuner.samples = 0
```

Hysteresis (gap between GROW_RATE 0.80 and SHRINK_RATE 0.30) prevents oscillation. CAPACITY_CAP=65536 caps per-state memory at ~10 MB (key+value Arrow regions: 65536 × ~80 B average payload each = ~5 MB × 2 = ~10 MB per state instance). MIN_CAPACITY=1024 ensures even small workloads keep some buffering.

## Components Touched

| Component | File | Change |
|---|---|---|
| `ArrowBinaryBuffer` | NEW `state/ArrowBinaryBuffer.java` | Off-heap Arrow BinaryArray buffer + primitive long→int hash index. `find/insert/remove` take MemorySegment views. `resize/clear/flushTo(linker)` methods. |
| `ArrowBinaryBufferAutoTuner` | NEW `state/ArrowBinaryBufferAutoTuner.java` | Hit-rate sampling + grow/shrink decision (per-state-instance state). |
| `MemorySegmentDataInputView` | `v1sync/` (already exists, has parity test) | Wire into ForStRsValueState. May need a tiny add (MemorySegment view setter) — verify before assuming. |
| `KeyGroupedSerializer.encodeForStateOffheap` | `keyed/ForStRsKeyGroupedSerializer.java` (NEW method) | Off-heap-writing overload. Reuses cached `stateNameBytes`. |
| `linker.getPinnedSegment` | `ffm/ForStRsLinker.java` (NEW signature) + Rust binding | Takes pre-allocated output MemorySegment; writes result there; returns offset/length via out-params. |
| `linker.putSegment` | `ffm/ForStRsLinker.java` (NEW signature) + Rust binding | Zero-copy native put. |
| `linker.batchPutSegments` | `ffm/ForStRsLinker.java` (NEW signature) + Rust binding | Direct Arrow BinaryArray pointer pass for flush. |
| `ForStRsValueState` | `state/ForStRsValueState.java` | New constructor accepting per-instance ArrowBinaryBuffer + scratchArena supplier; new `value()/update()/clear()` using the off-heap path. Legacy constructors retained for tests / backwards compat. |
| `ForStRsKeyedStateBackend` | `keyed/ForStRsKeyedStateBackend.java` | `getValueState` constructs a per-instance ArrowBinaryBuffer + scratchArena and passes them in. Old `writeBuffer` HashMap stays for List/Reducing/Aggregating until they migrate (separate spec). |

## Data Flow (Post-Tier 1)

```
event arrives → setCurrentKey(auctionId)
              → for each of 5 panes:

ForStRsValueState.value():
  1. scratchArena.reset()
  2. encodeForStateOffheap(kg, key, stateNameBytes, scratchArena, 0) → (off, len)
  3. row = statebuf.find(scratchArena, off, len)
  4. tuner.observeRead(row != -1)
  5. if row >= 0:
       view.setSegment(statebuf.valueData, statebuf.valueOffsets[row], statebuf.valueLengths[row])
       return serializer.deserialize(view)        // ZERO byte[] allocation
  6. else:
       valueOffOut = scratchArena.position
       rc = linker.getPinnedSegment(db, cf, scratchArena, off, len, scratchArena, &valueOffOut, &valueLenOut)
       if rc == NOT_FOUND: return null
       view.setSegment(scratchArena, valueOffOut, valueLenOut)
       return serializer.deserialize(view)        // ZERO byte[] allocation
       // note: this read does NOT eagerly insert into statebuf — wait for the write that follows

ForStRsValueState.update(newValue):
  1. Reuse key (off, len) from preceding value() (cached in instance, like lastValueKey today)
     OR: scratchArena.reset() + encodeForStateOffheap again
  2. valueOff = scratchArena.position
     serializer.serialize(newValue, MemorySegmentDataOutputView(scratchArena))
     valueLen = scratchArena.position - valueOff
  3. statebuf.insertOrUpdate(scratchArena, keyOff, keyLen, scratchArena, valueOff, valueLen)
       — copies bytes into statebuf.keyData / valueData (one off-heap memcpy each)
       — updates hashIndex
  4. if statebuf.size >= statebuf.flushHwm:
       linker.batchPutSegments(db, cf,
                               statebuf.keyData, statebuf.keyOffsets,
                               statebuf.valueData, statebuf.valueOffsets,
                               statebuf.size)
       statebuf.clear()   // hash index cleared; data buffers retained (size goes to 0)

ForStRsValueState.clear():
  1. statebuf.remove(keyOff, keyLen)
  2. linker.delete(db, cf, scratchArena, keyOff, keyLen)
```

**Net allocations per event** (Q5 HOP, 5 panes):
- Today (post-A2 reverted): ~25 allocations (5 composite keys + 5 result byte[] + 5 ByteArrayWrappers + 5 serialize buffers + 5 deserialize wraps).
- Tier 1: **0 per-call allocations** on hot path. Only scratchArena reuse + occasional arrowBuf grow events (≤ log2(64K/1K) = 6 resize events per state instance, ever).

## Correctness Invariants

- **scratchArena lifetime:** thread-local; reset at each value()/update() entry. Bytes copied into statebuf on insert. Never escapes the per-call window.
- **statebuf lifetime:** owned by ForStRsValueState; closed when backend closes; data survives setCurrentKey changes (per A2 cache-survival semantics — different state-name fingerprints map to different rows).
- **statebuf concurrency:** single-threaded per Flink slot — same guarantee as today.
- **MemorySegment lifetime in deserialize:** TypeSerializer must consume the view synchronously and not retain it. Existing Flink serializers honor this (they read field by field, releasing references on return).
- **Hash collisions in statebuf.hashIndex:** open-addressing with linear probing; on collision-with-different-key, fallback to byte-by-byte MemorySegment equality.
- **statebuf flush semantics:** flushHwm = capacity/2 — flush at half-full keeps the steady-state cache populated (Q5 needs entries to survive across event arrivals). On flush, native batchPut succeeds → statebuf.clear() resets size/hashIndex but keeps data buffer capacities intact.
- **Auto-tune monotonicity:** resize is rare (≤ 6 events per state instance, after which capacity is at CAPACITY_CAP). Decisions don't affect correctness.

## Error Handling

- `linker.getPinnedSegment` returns NOT_FOUND on absent key (no fallback; caller returns null directly — replaces the skip-fallback flag pattern with explicit return codes).
- `statebuf.insert` past current capacity → resize-then-insert. If resize past CAPACITY_CAP, flush+clear, then insert into fresh buffer.
- `scratchArena` exhaustion → grow once (one-time per state instance, log warning). Default 64 KB sized for typical Q5 (max payload ~256 B, max key ~100 B, 5 panes simultaneously buffered = ~2 KB; 64 KB is 32× safety margin).
- Rust-side errors propagate as today (FRS_STATUS_OK / FALLBACK / ERROR).

## Testing

### Existing tests that must still pass

- All `ForStRs*State*Test` files — Value/Map/List/Reducing/Aggregating state correctness.
- `ForStRsKeyGroupedSerializerTest` — byte-identical encoding output across overloads (the new `encodeForStateOffheap` must produce the same bytes as the old `encodeForState`).
- `MemorySegmentDataViewParityTest` — off-heap vs on-heap deserialization parity.

### New unit tests

1. **`ArrowBinaryBufferTest`** — insert/lookup parity with reference HashMap; resize correctness (capacity × 2, capacity / 2); collision handling under same-hash different-key; clear semantics (size = 0, hashIndex empty, data buffers retained).
2. **`ArrowBinaryBufferAutoTunerTest`** — grow at ≥ 80 % hit; shrink at ≤ 30 % hit; hysteresis prevents oscillation when hit rate hovers at 0.5; respects CAPACITY_CAP and MIN_CAPACITY.
3. **`ForStRsValueStateOffheapTest`** — value/update/clear round-trip via off-heap path; byte-identical result to legacy byte[] path (use mockable linker that records pointers; compare bytes at the wire); allocation budget assertion (no byte[] in hot loop verified via class-histogram heap dump).
4. **`ForStRsLinkerSegmentTest`** — `getPinnedSegment` / `putSegment` round-trip; out-of-range offsets rejected; lifetime correctness (caller-owned MemorySegment outlasts the FFM call).
5. **`KeyGroupedSerializerOffheapParityTest`** — byte-for-byte parity between `encodeForState(kg, k, "foo")` and `encodeForStateOffheap(kg, k, "foo".getBytes(UTF_8), scratchArena, 0)`.

### Bench acceptance gates (tiered, portfolio-aware — inherited from prior spec)

| Tier | Trigger | Action |
|---|---|---|
| **T0 — strategic breach** | Any query that was ≥ 1.0× rocksdb in v3.3 drops below 1.0× | Mandatory revert |
| **T1 — large drift on strong win** | Any current ≥ 1.5× win regresses > 10 % | Review-triggered (net_delta decides) |
| **T2 — catastrophic** | Any query regresses > 20 % | Mandatory revert |
| **Target wins** | Q5 < 114 s, Q8 < 33 s, Q11 ≤ 76.5 s, Q12 ≤ 35.5 s, Q13 < 34 s | KPI met → ship; missed → re-bench or escalate to Tier 2 |
| **Net portfolio delta** | `Σ log10(new/v3.3)` ≤ -2.0 | required overall improvement |

### Implementation order (one commit per logical unit, bench at end)

0. **PREP — restore HEAP timer factory.** Recover the lost perf-recovery work from earlier session. Standalone commit. Bench Q11 / Q12 to confirm they're back to v3.3 numbers before Tier-1 begins (so attribution of subsequent gains is clean).
1. **`ArrowBinaryBuffer` + `ArrowBinaryBufferAutoTuner` + unit tests.** Pure Java, no FFM dependency. Self-contained.
2. **`linker.getPinnedSegment` + Rust companion.** Java FFM binding + Rust FFI stub. May initially route through existing `frs_get_pinned` + memcpy if Rust-side native zero-copy isn't ready.
3. **`KeyGroupedSerializer.encodeForStateOffheap` + parity test.** Confirms byte-identical encoding before any state-class changes land.
4. **`ForStRsValueState` off-heap value()/update()/clear() + unit tests.** New constructor; legacy ctors retained.
5. **`ForStRsKeyedStateBackend` wiring** — `getValueState` constructs per-instance ArrowBinaryBuffer + scratchArena; passes them to the new ForStRsValueState ctor.
6. **`linker.putSegment` + `linker.batchPutSegments` + Rust companion** — wire the write-path zero-copy. Initially can stub to copy if Rust isn't ready (still eliminates byte[] alloc on Java side).
7. **Bench Q5/Q11/Q12/Q13 first (fast feedback), then full Q0-Q23 sweep** with fresh-cluster strategy.

## Out of Scope (Tier 2/3)

- **Tier 2 — Map/List/Reducing/Aggregating off-heap** — same pattern as ValueState; landed as a sibling spec after Tier 1 proves the approach.
- **Tier 3 — V2 async path off-heap** — V2 is already vectorized via VectorizedExecutor; off-heap migration there is incremental cleanup, not a perf win.
- **Engine-level optimizations** — Approach-C, separate spec.

## CONTRIBUTING.md hand-off

This spec inherits the **5-item perf-audit checklist** + **net portfolio delta formula** + **tiered T0/T1/T2 gates** from the prior V1-sync cache-clear design. They land together when PR-D ships.

## Open Questions

None at design time. If Tier 1 lands and Q5/Q8/Q13 still miss KPI, Tier 2 (Map/List off-heap) or Approach-C (Rust engine) opens as a follow-up.
