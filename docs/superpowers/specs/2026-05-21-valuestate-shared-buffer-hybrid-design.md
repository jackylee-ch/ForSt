# Hybrid V1-ValueState Shared-HashMap Buffer + Current MapState Off-Heap

**Author:** jackylee (PMC) + Claude
**Date:** 2026-05-21
**Branches:** `~/Code/stczwd/flink` `forst-rs-jdk25` (HEAD `e0809571483`), `~/Code/stczwd/ForSt` `forst-rs`
**Related:**
- v3.2 canonical baseline: [`docs/superpowers/specs/2026-05-18-forst-rs-benchmark-report-v3.2.md`](2026-05-18-forst-rs-benchmark-report-v3.2.md)
- 1a-1c.1 committed work supersedes parts of 1b which this spec partially reverts.

## Problem

The 1b.1/1b.2/1b.3 work refactored V1-sync `ForStRsValueState` from the shared-backend-HashMap write buffer (v3.2 mechanism) to a per-instance `ArrowBinaryBuffer`. This decision was made under the misconception that v3.2 Q5 = 32.45 s was a silent-data-loss artifact. **Re-reading the canonical v3.2 report confirms Q5 = 32.45 s was real** — at commit `c81654b3345`, V1 ValueState used the shared backend `Map<ByteArrayWrapper, byte[]>` with `MAX_BUFFER_ENTRIES = 524288`. Q5's 5-pane HOP working set fit comfortably in the single shared buffer; FFM crossings were rare; Q5 finished in 32 s. Q11/Q12 were bad at v3.2 (Q11 = 176 s, Q12 = 116 s) because the engine-backed timer queue was the default — NOT because of the buffer mechanism.

After 1b.1, each ValueState owns its own per-instance `ArrowBinaryBuffer`. For Q5's 5 panes per event, this means 5 separate buffers competing for grow events independently. Per-pane effective buffer is smaller; eviction churn hits; FFM crossings climb; Q5 regressed to 561 s.

The cost of recovering Q5 to ~32 s is **not** abandoning all of this session's work — only the V1-ValueState piece. The 1a foundation (`ArrowBinaryBuffer`, `AutoTuner`, `getPinnedSegment`, `encodeForStateOffheap`) remains as MapState's storage backend (1c.1 wins are independent: Q15 6×, Q9 16×, Q20 18×). The PREP HEAP timer factory remains (Q11/Q12 are now timer-bound, not buffer-bound — buffer mechanism choice is invariant for them).

## Goal

**Recover Q5 ≈ 32 s and Q8 ≈ 28 s simultaneously with current Q11/Q12/Q15/Q9/Q20 wins** by reverting V1-sync ValueState to v3.2's shared-backend-HashMap write-buffer mechanism while keeping every other 2026-05-20-21 session improvement intact.

Strict KPI:
- Q5  ≤ 50 s   (vs current 561 s; v3.2 was 32.45 s)
- Q8  ≤ 40 s   (vs current 55.6 s; v3.2 was 27.3 s)
- Q11 ≤ 80 s  (current 73.6 s — no regression)
- Q12 ≤ 35 s  (current 33.3 s — no regression)
- Q13 ≤ 45 s  (current 40.6 s — no regression; should improve)
- Q15 ≤ 25 s  (current 18.3 s — preserve win)
- Q9  ≤ 70 s  (current 56.9 s — preserve win)
- Q18 ≤ 75 s  (current 68.4 s — preserve win)
- Q20 ≤ 60 s  (current 48.8 s — preserve win)
- Q19 stays at its 1c.1 known-regression level (~117 s; separate spec)

## Non-Goals

- Removing 1a/1c.1 code — keep the foundation as MapState's storage.
- Touching V2 async path — unchanged.
- Rust engine — no FFM changes.
- Flink runtime — unchanged.

## Architecture

```
ForStRsKeyedStateBackend (HEAD e0809571483):
├── stateCache: Map<String, Object>            (per-stateName cache)
├── shared writeBuffer: Map<ByteArrayWrapper, byte[]>   (REINSTATED for V1 ValueState)
│     MAX_BUFFER_ENTRIES = 524288   (or current 4096 — see Section 2)
│     WRITE_BUFFER_FLUSH_THRESHOLD = 64  (current)
│     adaptive disable already in place
├── mapStateRegistry: Map<String, ForStRsMapState>  (per-MapState off-heap; unchanged)
├── ownedBuffers: List<ArrowBinaryBuffer>  (still owned for MapStates; ValueState is no longer wired)
├── setCurrentKey(K newKey):
│     - serialize newKey to currentKeyBytes
│     - keyGeneration++
│     - stateCache.clear()        (REINSTATED — V1 ValueState's keyPrefix embeds currentKey)
├── getValueState(stateName, valueSerializer):
│     ┌─────────────────────────────────────────────────────────┐
│     │  REVERT TO v3.2 PATTERN                                  │
│     │  byte[] prefix = buildPrefix(stateName);                 │
│     │  return new ForStRsValueState<>(                         │
│     │      linker, db, defaultCf, prefix, valueSerializer,     │
│     │      this::getFromWriteBuffer,                           │
│     │      this::putToWriteBuffer,                             │
│     │      this::deleteFromWriteBuffer);                       │
│     └─────────────────────────────────────────────────────────┘
└── getMapState(stateName, keySer, valSer):  (UNCHANGED — uses 1c.1 ArrowBinaryBuffer + tuner)
```

ValueState (legacy byte[]-prefix-with-hooks ctor — class lines 112-132) consumes the backend's shared write-buffer hooks. `keyComputer`-mode ctor (1b.1) remains in the file as dead code (kept for future revisitation / tests).

ArrowBinaryBuffer + AutoTuner remain in tree — used exclusively by MapState (and any future state class that opts in).

## Components Touched

| Component | File | Change |
|---|---|---|
| `getValueState` wiring | `ForStRsKeyedStateBackend.java` (around current line 327) | Switch from keyComputer-mode ctor (`new ForStRsValueState<>(linker, db, defaultCf, valueSerializer, () -> ..., kgSer, stateName, ..., buf, tuner)`) to byte[]-prefix-with-hooks ctor (`new ForStRsValueState<>(linker, db, defaultCf, buildPrefix(stateName), valueSerializer, this::getFromWriteBuffer, this::putToWriteBuffer, this::deleteFromWriteBuffer)`). Remove the per-instance `new ArrowBinaryBuffer(...)` + `ownedBuffers.add(buf)` lines for ValueState. |
| `setCurrentKey` | `ForStRsKeyedStateBackend.java` (around line 268) | Re-add `this.stateCache.clear();` at the end of setCurrentKey (it was removed in 1b.2). The legacy byte[]-prefix ValueState ctor bakes `currentKeyBytes` into the prefix at construction time, so each setCurrentKey must invalidate cached ValueStates. MapState is in `mapStateRegistry` (separate from `stateCache`) so it's unaffected. |
| `MAX_BUFFER_ENTRIES` | `ForStRsKeyedStateBackend.java` | Lift the existing constant from current 4096 → 524288 (matches v3.2). The shared HashMap can absorb Q5's working set comfortably at this size. The adaptive-disable mechanism keeps high-cardinality unhelpful-buffer workloads from paying HashMap-grow cost when hit rate is < 10%. |
| `WRITE_BUFFER_FLUSH_THRESHOLD` | `ForStRsKeyedStateBackend.java` | Lift from current 64 → 8192 (between v3.2's 524288 cap-flush and a flushable batch size; smaller than v3.2's effective batch so we don't stall on giant batchPut calls). |
| ValueState off-heap code | `ForStRsValueState.java` (lines 158-210 approx — the keyComputer-mode ctor + the off-heap branches in value/update/clear) | Keep as dead code. The byte[]-prefix ctors + their value/update/clear bodies (lines 89-156 approx) handle V1 again. |
| ArrowBinaryBuffer + AutoTuner | unchanged | Used by MapState only. |

## Data Flow (post-fix, Q5 example)

```
event:
  backend.setCurrentKey(auctionId)
    - serialize currentKey → currentKeyBytes
    - stateCache.clear()  (V1 ValueState instances reaped)

  for each of 5 panes (window-namespace):
    state = backend.getValueState("acc", LongSerializer.INSTANCE)
      - cache miss → new ForStRsValueState<>(linker, db, defaultCf, buildPrefix("acc"), ..., hooks)
      - cached in stateCache
    state.value():
      - lastValueKey = keyPrefix (already includes currentKeyBytes from ctor)
      - writeBufferGet.apply(lastValueKey) → HashMap lookup
        - HIT → return cached payload (zero FFM)
        - MISS → linker.getPinned(...) → byte[] (1 FFM)
    state.update(newAcc):
      - serialize → byte[] payload
      - writeBufferPut.accept(lastValueKey, payload)
        - HashMap.put — accumulates in shared buffer
        - flushes via frsBatchPut at WRITE_BUFFER_FLUSH_THRESHOLD or MAX_BUFFER_ENTRIES
```

For Q5: 5 panes × 100 M events × ~5 effective pane keys per event with high read-write locality (sliding-shared) → buffer hit rate ~95%+ → ~25 M FFM crossings instead of 1 B → wall-clock drops from 561 s to ~32 s.

## Correctness Invariants

- **stateCache.clear() on setCurrentKey** — restored. V1 ValueState's `keyPrefix` field is captured at construction with the current key embedded; subsequent setCurrentKey must invalidate the cached instance. Matches v3.2 semantics. Per-event ValueState allocation cost is dominated by buffer hit rate not allocation churn (per JFR analysis early in this session).
- **MapState cache survival** — `mapStateRegistry` (separate from `stateCache`) survives setCurrentKey. 1c.1's MapState instances use `compositeKeyComputer: Function<UK, byte[]>` which lazily reads `currentKeyBytes`, so survival is correct. Unchanged.
- **Shared write-buffer correctness** — exists in v3.2 and current code (line 165 of ForStRsKeyedStateBackend.java). The `writeBufferPut/Get/Delete` hooks route through this HashMap. Concurrency: single-threaded per Flink slot — same as today. Flush on checkpoint: existing `flushWriteBuffer()` method.
- **Read-after-write consistency** — `getFromWriteBuffer` is called before `linker.getPinned` in value(); writes go through `putToWriteBuffer` first. Reads see the latest write. Unchanged.
- **Snapshot/restore** — checkpoint flushes the write buffer (existing path); restore reads from engine state. Unchanged.

## Error Handling

No new error modes. All shared write-buffer paths are existing code reinstated, not new.

## Testing

### Existing tests that must still pass

- `ArrowBinaryBufferTest`, `ArrowBinaryBufferAutoTunerTest` — pure-Java, unaffected.
- `ForStRsValueStateOffheapTest` — exercises the off-heap ctor directly, still passes (the ctor lives, just is no longer called from `getValueState`).
- `ForStRsMapStateOffheapTest` — unaffected.
- `KeyGroupedSerializerOffheapParityTest` — unaffected.

### New / restored tests

1. **`ValueStateUsesSharedBufferTest`** — after `getValueState`, two consecutive `value()` reads on the same key should produce exactly ONE `linker.getPinned` call (the second hits the shared buffer). Use a mockable linker that counts calls.
2. **`ValueStateInstanceCachePerName`** — calling `getValueState("foo", ...)` twice between two `setCurrentKey` calls returns the SAME instance (cache hit). Calling it after a `setCurrentKey` produces a different instance (cache cleared).

### Bench acceptance gates (tiered, per existing T0/T1/T2)

| Q | Target | Notes |
|---|---|---|
| Q5  | ≤ 50 s   | recover v3.2's 32 s (with HEAP timer plus shared buffer ⇒ should hit) |
| Q8  | ≤ 40 s   | v3.2 was 27 s |
| Q11 | ≤ 80 s   | no regression vs current 73 s |
| Q12 | ≤ 35 s   | V2 path, invariant |
| Q13 | ≤ 45 s   | should improve |
| Q15 | ≤ 25 s   | preserve 1c.1 win (current 18 s) |
| Q9  | ≤ 70 s   | preserve 1c.1 win (current 57 s) |
| Q18 | ≤ 75 s   | preserve current 68 s |
| Q20 | ≤ 60 s   | preserve 1c.1 win (current 49 s) |
| Q22 | ≤ 35 s   | preserve current 32 s |
| Q19 | ≤ 130 s  | 1c.1 known regression; separate fix |

If any of Q5/Q8/Q13/Q11/Q12 misses target by > 20%, the design's premise is wrong — rollback and re-investigate. Q9/Q15/Q18/Q20 should be invariant (their wins come from MapState off-heap, not ValueState).

## Implementation Order — single PR

1. **Lift `MAX_BUFFER_ENTRIES`** (current `ForStRsKeyedStateBackend.java`) — 4096 → 524288.
2. **Lift `WRITE_BUFFER_FLUSH_THRESHOLD`** — 64 → 8192.
3. **`getValueState` switch** — use the byte[]-prefix-with-hooks ctor; remove the per-instance ArrowBinaryBuffer construction for ValueState. ValueState's off-heap code remains as dead code on the legacy path.
4. **Restore `stateCache.clear()` in `setCurrentKey`** — bring back the line 288 deletion (1b.2 removed it).
5. **Add the 2 new unit tests.**
6. **Bench Q5, Q8, Q11, Q12, Q13, Q15, Q9, Q18, Q20, Q22, Q19 with fresh-cluster strategy.**
7. **Commit + update v3 report (v3.8 section) with the recovered portfolio.**

## Why this is safe to ship

- **Reverts only the ValueState wiring** — touches one method in one file (`getValueState`) plus 3 constants + restore one line in `setCurrentKey`. ≤ 20 lines of code change.
- **Preserves all the architectural infrastructure** — 1a foundation, 1c.1 MapState off-heap, PREP HEAP timer factory, size-aware AutoTuner.
- **Empirical evidence** — v3.2 ran exactly this V1-ValueState mechanism and achieved Q5 = 32.45 s. The mechanism is proven.
- **Q11/Q12 robustness** — these are timer-bound, not buffer-bound. They didn't regress when we removed the per-instance buffer in 1b.1's design; they won't regress when we put back the v3.2 shared-buffer.

## Out of Scope (follow-on)

- Q19's 1c.1 regression (MapState off-heap write memcpy) — separate spec.
- Engine-level Q5 optimization (Approach-C) — if even with this fix Q5 stays ≥ 40 s, deeper engine work.
- Removing the dead 1b.1 off-heap ValueState code — keep for potential future use; YAGNI says don't delete working code that might be wanted later.
