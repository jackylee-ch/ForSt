# Backend-Shared Off-Heap ArrowBinaryBuffer for V1-Sync ValueState

**Author:** jackylee (PMC) + Claude
**Date:** 2026-05-21
**Branches:** `~/Code/stczwd/flink` `forst-rs-jdk25` (HEAD `e0809571483`), `~/Code/stczwd/ForSt` `forst-rs`
**Related:**
- v3.2 canonical baseline: [`docs/superpowers/specs/2026-05-18-forst-rs-benchmark-report-v3.2.md`](2026-05-18-forst-rs-benchmark-report-v3.2.md)
- 1a-1c.1 committed work supersedes parts of 1b which this spec partially redirects.
- **Supersedes** the prior draft that proposed reverting to byte[]-HashMap — that violated the zero-copy goal. This draft preserves zero-copy AND recovers Q5 perf.

## Problem

Two failure modes have been observed empirically this session:

1. **1b.1 per-instance off-heap (current state, HEAD `e0809571483`)** — Q5 = 561 s, regression 17× over v3.2. Per-pane ValueState gets its OWN `ArrowBinaryBuffer`; Q5's 5-pane HOP working set is fragmented across 5 buffers each managing independent grow/evict cycles → buffer churn dominates → FFM crossings dominate wall-clock.

2. **v3.2 shared on-heap HashMap (`Map<ByteArrayWrapper, byte[]>`)** — Q5 = 32.45 s. ONE big buffer absorbs Q5's working set efficiently. But the mechanism uses byte[] everywhere — composite keys, value payloads, HashMap wrappers, `linker.put(byte[], byte[])`. Violates the stated architectural goal of end-to-end zero-copy + vectorization.

**The lever is shared-vs-per-instance, NOT off-heap-vs-on-heap.** v3.2 was fast because the buffer was shared. 1b.1 was slow because the buffer was per-instance. Off-heap zero-copy is orthogonal to that lever.

## Goal

**Recover Q5 ≈ 32 s** by giving V1 ValueState a **shared off-heap `ArrowBinaryBuffer`** owned by the backend (replacing v3.2's on-heap HashMap and 1b.1's per-instance off-heap). One big shared off-heap buffer absorbs Q5's working set AND keeps the hot path zero-copy: MemorySegment views all the way through `value()/update()/clear()`.

Strict KPI:
- Q5  ≤ 50 s   (vs current 561 s; v3.2 was 32.45 s)
- Q8  ≤ 40 s   (vs current 55.6 s; v3.2 was 27.3 s)
- Q11 ≤ 80 s   (current 73.6 s — no regression)
- Q12 ≤ 35 s   (current 33.3 s — no regression)
- Q13 ≤ 45 s   (current 40.6 s — should improve)
- Q15 ≤ 25 s   (current 18.3 s — preserve 1c.1 win)
- Q9  ≤ 70 s   (current 56.9 s — preserve 1c.1 win)
- Q18 ≤ 75 s   (current 68.4 s)
- Q20 ≤ 60 s   (current 48.8 s — preserve 1c.1 win)

## Non-Goals

- Removing 1a/1c.1 code — keep foundation; MapState's per-instance buffer pattern stays as 1c.1's choice.
- V2 async path — unchanged; doesn't use this buffer.
- Rust engine — no FFM changes (existing `frsBatchPutArrow` reused).
- Flink runtime — unchanged.

## Architecture

```
ForStRsKeyedStateBackend:
├── stateCache: Map<String, ForStRsValueState> (per-stateName cache; survives setCurrentKey)
├── sharedValueStateBuf: ArrowBinaryBuffer  (NEW — single off-heap buffer for ALL V1 ValueState)
│     initialCapacity = 1024
│     maxCapacity = 524288  (matches v3.2's MAX_BUFFER_ENTRIES)
│     auto-tuner attached (existing AutoTuner; growth gated by size + hit-rate)
├── mapStateRegistry: Map<String, ForStRsMapState> (per-MapState off-heap; 1c.1 unchanged)
├── ownedBuffers: List<ArrowBinaryBuffer>  (now also includes sharedValueStateBuf)
├── scratchArenaTL: ThreadLocal<MemorySegment>  (unchanged)
├── setCurrentKey(K newKey):
│     - currentKeyBytes = serialize(newKey)
│     - keyGeneration++
│     - NO cache clear (ValueState instances are buffer-agnostic; they encode the current
│       composite key per call via their kgSerializer reference)
├── getValueState(stateName, valueSerializer):
│     - cache hit → return cached instance
│     - cache miss → new ForStRsValueState<>(
│         linker, db, defaultCf, valueSerializer,
│         scratchArenaTL::get,
│         kgSerializer,
│         stateName,
│         this::getCurrentKeyGroupIndex,
│         () -> getCurrentKey(),
│         sharedValueStateBuf,        // ← KEY: shared backend buffer, not per-instance
│         sharedValueStateTuner);
└── getMapState(...): UNCHANGED, uses per-instance ArrowBinaryBuffer per 1c.1.

ForStRsValueState (using existing 1b.1 off-heap ctor):
├── value()/update()/clear() unchanged from 1b.1 — they already operate on the
│   passed-in statebuf via MemorySegment APIs. They don't care whether the buffer
│   is per-instance or shared.
└── flushStateBuffer(): drains the buffer. Since the buffer is now shared across
    all V1 ValueState instances, EACH ValueState's flushStateBuffer drains the
    SAME backend buffer. Idempotent: if already empty, no-op.

Data flow on Q5 event:
  setCurrentKey(auctionId)               (no clear)
  for each of 5 panes:
    state = backend.getValueState("acc")  (cache hit after first call)
    state.value():
      encodeForStateOffheap(...) → (off, len) in scratch (different per pane via namespace)
      sharedValueStateBuf.find(scratch, off, len) → row or -1
      hit → MemorySegmentDataInputView on shared buffer (zero-copy, zero byte[])
      miss → linker.getPinnedSegment(...) into scratch (zero byte[] per Java side)
    state.update(newAcc):
      serialize into scratch via MemorySegmentDataOutputView (zero byte[])
      sharedValueStateBuf.insert(scratch, keyOff, keyLen, scratch, valOff, valLen)
      if buffer fills: flushTo → linker.batchPut (single FFM for N entries)
```

All 5 panes' state lives in ONE shared off-heap buffer. Working set ~50K-250K entries fits within 524K cap. Buffer hit rate stays high. FFM crossings rare. Q5 absorption mechanism = v3.2's shared mechanism, off-heap.

## Components Touched

| Component | File | Change |
|---|---|---|
| Shared buffer field | `ForStRsKeyedStateBackend.java` | Add `private final ArrowBinaryBuffer sharedValueStateBuf;` + `sharedValueStateTuner;` initialized in constructor at `(initial=1024, max=524288, tuner)`. |
| Shared buffer lifecycle | `ForStRsKeyedStateBackend.close()` | Already iterates ownedBuffers and closes each; add sharedValueStateBuf to ownedBuffers so it's freed on close. |
| `getValueState` | `ForStRsKeyedStateBackend.java` (around line 327) | Switch ctor call to pass `sharedValueStateBuf, sharedValueStateTuner` instead of constructing a per-instance one. Remove `ownedBuffers.add(buf)` (the shared one is already registered). |
| `setCurrentKey` | `ForStRsKeyedStateBackend.java` | Keep current (no clear) — works correctly because ValueState instances now lazily encode composite key per call from currentKeyBytes (the kgSerializer + keySupplier closure handles this). |
| Snapshot/close flush | `ForStRsKeyedStateBackend.java` | Existing iteration over stateCache + flushStateBuffer is correct. Each ValueState's flushStateBuffer drains the shared buffer; the first call empties it, subsequent calls are no-ops (size==0). |
| `MAX_BUFFER_ENTRIES` on legacy backend HashMap | `ForStRsKeyedStateBackend.java` | Legacy `Map<ByteArrayWrapper, byte[]> writeBuffer` becomes UNUSED for ValueState (was already unused after 1b.1). Existing code paths that call `getFromWriteBuffer/putToWriteBuffer/deleteFromWriteBuffer` are kept for any future legacy-mode callers (e.g., List/Reducing/Aggregating still use legacy ctor pathways), but ValueState no longer touches them. |

ValueState itself: unchanged. Its off-heap value/update/clear (committed in 1b.1) operates on whatever buffer it's given. The fix is purely at the wiring layer.

## Data Flow Comparison

| Path | 1b.1 (current, Q5 = 561 s) | v3.2 (Q5 = 32 s, byte[] everywhere) | **This design (Q5 ≈ 32 s, zero-copy)** |
|---|---|---|---|
| Composite key | off-heap in scratch | byte[] alloc | off-heap in scratch ✓ |
| Buffer storage | per-instance ArrowBinaryBuffer | shared HashMap<wrapper, byte[]> | **shared ArrowBinaryBuffer** ✓ |
| Read path | MemorySegmentDataInputView on instance buf | byte[] from HashMap → DataInputDeserializer | **MemorySegmentDataInputView on shared buf** ✓ |
| Write path | MemorySegmentDataOutputView into scratch → instance buf | DataOutputSerializer → getCopyOfBuffer → HashMap.put | **MemorySegmentDataOutputView into scratch → shared buf** ✓ |
| Cold-miss native call | linker.getPinnedSegment | linker.getPinned (byte[]) | **linker.getPinnedSegment** ✓ |
| Flush | per-instance batchPut | shared batchPut | **shared batchPut** ✓ |
| **byte[] on hot path** | none | many | **none** ✓ |
| **Q5 working set absorption** | fragmented across 5 per-pane buffers | one buffer | **one shared buffer** ✓ |

## Correctness Invariants

- **Shared buffer concurrency** — Flink keyed-state backend is single-threaded per slot; the shared buffer is single-threaded by construction. No locking needed.
- **stateCache survival across setCurrentKey** — ValueState instances are buffer-agnostic; they compute composite keys per call from the backend's `currentKeyBytes` via the kgSerializer + keySupplier closure. Survives setCurrentKey safely. (Same property as 1b.2's design.)
- **State-name disambiguation in the shared buffer** — composite keys include `[kg | userKey | / | stateName | /]`, so two different ValueStates with different stateNames have different composite keys. No collision in the shared buffer.
- **Flush correctness** — backend's `close()` iterates `stateCache` and calls `flushStateBuffer()` on each ValueState. The first call drains the shared buffer; subsequent are no-ops. Each ValueState's `flushStateBuffer` is idempotent (already checks `if (size == 0) return`).
- **Snapshot/checkpoint correctness** — existing path calls `flushStateBuffer()` on each cached ValueState before snapshot. Same drains-shared-buffer behavior. Data is durable in engine post-snapshot.
- **Auto-tuner sharing** — one auto-tuner observes reads from ALL ValueState instances. The aggregate hit rate + occupancy across all panes drives growth decisions. This is desired — the cap grows when the WHOLE workload's working set grows, not when one pane's does.

## Error Handling

No new error modes. All existing failure paths (`linker.getPinnedSegment` returns -1 on miss, `flushTo` propagates native errors, etc.) inherited from 1a/1b/1c.1.

## Testing

### Existing tests that must pass

- All current tests (33+ unit tests in the forst-rs backend) must still pass.
- `ForStRsValueStateOffheapTest` continues to exercise the off-heap ctor — still passes since the ctor signature is unchanged.

### New tests

1. **`SharedValueStateBufferTest`** — create two `ForStRsValueState` instances with different stateNames sharing the SAME `ArrowBinaryBuffer`. Verify writes to one don't collide with reads from the other. (Tests composite-key disambiguation.)
2. **`SharedBufferSurvivesSetCurrentKey`** — backend creates V1 ValueState, puts a value, calls `setCurrentKey(k2)`, then reads back with the original key — should still find the buffered write (since the shared buffer holds it indexed by composite-key that includes the original key).
3. **`SharedBufferFlushOnceFromAnyValueState`** — fill the shared buffer via ValueState A, call flushStateBuffer via ValueState B. Buffer drains correctly. Reads from ValueState A after flush still work (read goes through linker.getPinnedSegment to engine).

### Bench acceptance gates (tiered, fresh-cluster)

| Q | Target | Notes |
|---|---|---|
| Q5  | ≤ 50 s   | Q5 = recovery target |
| Q8  | ≤ 40 s   | v3.2 was 27 s; should be close |
| Q11 | ≤ 80 s   | no regression vs current 73 s |
| Q12 | ≤ 35 s   | V2 path invariant |
| Q13 | ≤ 45 s   | should improve |
| Q15 | ≤ 25 s   | preserve 1c.1 win |
| Q9  | ≤ 70 s   | preserve 1c.1 win |
| Q18 | ≤ 75 s   | preserve current |
| Q20 | ≤ 60 s   | preserve 1c.1 win |
| Q22 | ≤ 35 s   | preserve current |
| Q19 | ≤ 130 s  | known 1c.1 regression; separate spec |

If Q5 misses target, the design's "shared absorption" hypothesis is wrong — investigate (likely the working set is bigger than 524K) and consider lifting max OR adding LRU eviction.

## Implementation Order (single PR)

1. **Add `sharedValueStateBuf` field + tuner** to `ForStRsKeyedStateBackend`. Initialize in constructor. Add to `ownedBuffers` for lifecycle.
2. **Modify `getValueState`** — pass the shared buffer + tuner to ValueState's existing off-heap ctor.
3. **Verify `setCurrentKey`** stays without `stateCache.clear()` (it already does post-1b.2).
4. **Add 3 unit tests** described above.
5. **Build, deploy, bench Q5/Q8/Q9/Q11/Q12/Q13/Q15/Q18/Q19/Q20/Q22.**
6. **Commit + update v3 report with v3.8 section** if gates pass.

## Why this is the right design

- **Preserves the architectural goal**: zero-copy + vectorization end-to-end on V1 sync. No byte[] on hot path.
- **Empirically grounded**: v3.2 proved a shared 524K-entry buffer absorbs Q5's working set. We're using the same SHAPE, off-heap.
- **Surgical**: ≤ 30 lines of code change in `ForStRsKeyedStateBackend.java`. No changes to ValueState itself (1b.1 off-heap path stays).
- **Preserves all current wins**: HEAP timer factory (Q11/Q12), MapState off-heap (Q15/Q9/Q20), 1a foundation.
- **Future-friendly**: if a workload needs per-instance isolation (e.g., disjoint working sets), a future opt-in could wire per-instance buffer. The shared buffer is the default; per-instance is the future optimization.

## Out of Scope (follow-on)

- Q19's 1c.1 known regression — separate spec.
- Engine-level optimizations — Approach-C, separate spec.
- Migrating MapState to use the same shared-buffer pattern — could give Q19 recovery; experimental, not in this PR.
