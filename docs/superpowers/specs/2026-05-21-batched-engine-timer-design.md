# Batched Engine-Backed Timer Queue — Unified Single Timer Factory

**Author:** jackylee (PMC) + Claude
**Date:** 2026-05-21
**Branches:** `~/Code/stczwd/flink` `forst-rs-jdk25` (HEAD `e0809571483`), `~/Code/stczwd/ForSt` `forst-rs`

## Problem

Empirical truth table (measured 2026-05-21, current HEAD `e0809571483`):

| Timer factory | Q5 | Q11 | Q12 | Q8 |
|---|---|---|---|---|
| HEAP (current default) | 590 s | 73 s | 33 s | 53 s |
| FORSTRS engine-backed | **27 s** | 134 s | 131 s | **25 s** |

The two factories have diametrically opposite strengths. Per-operator selection is workable but pushes operator-knowledge into the platform layer. The architecturally correct answer is **a single batched engine-backed timer that beats both on all workloads**.

The current `ForStRsKeyGroupedInternalPriorityQueue` (engine-backed) issues per-timer FFM calls — `linker.put` per enqueue, `linker.get` + `linker.delete` per remove, `linker.delete` per poll. For Q11/Q12 with millions of timer ops, the per-call FFM cost (~500 ns) dominates wall-clock. A TODO comment in the file (line 136) confirms batched delete was deferred for "correctness-first" reasons.

Closing this gap requires batching the timer FFM crossings while preserving Flink's timer-ordering invariants.

## Goal

Single batched engine-backed timer queue (FORSTRS factory, default) that:
- **Q5 ≤ 30 s** (preserve current FORSTRS win)
- **Q8 ≤ 30 s** (preserve current FORSTRS win)
- **Q11 ≤ 75 s** (BEAT current HEAP 73 s, was 134 s un-batched)
- **Q12 ≤ 35 s** (BEAT current HEAP 33 s, was 131 s un-batched)
- **Other queries**: no regression

Net effect: HEAP timer factory becomes unnecessary; FORSTRS becomes the universal default.

## Non-Goals

- HEAP timer factory removal (keep as opt-in for compatibility / debugging).
- V2 async path — unchanged.
- Rust engine — no FFM signature changes; existing `frsBatchPut` / `frsVectorizedBatchDelete` / `frsBatchPrefixScan` are reused.
- Flink runtime — no changes to timer-service contract.

## Architecture — "Option 1' + Variant B (off-heap zero-copy)"

End-to-end goals: **batch execution + vectorization + zero-copy** all preserved. The pending buffer is an **off-heap binary min-heap** (not a Java `PriorityQueue`) so no Java objects are allocated per timer event.

```
ArrowTimerBuffer (NEW component — analog of ArrowBinaryBuffer for timer entries):
├── heapArray: MemorySegment of fixed-size entries (24 bytes each)
│     entry layout: { ts: long, op: int (ADD=1, REMOVE=2), keyOffset: int, keyLen: int }
│     min-heap by ts, indexed by array position (parent=(i-1)/2, children=2i+1/2i+2)
├── keyData: MemorySegment of variable-length composite key bytes (kg+ts+serialize(T))
├── hashIndex: open-addressed long → int (key-hash → heapArray row) for O(1) cancellation lookup
├── size / capacity (resize like ArrowBinaryBuffer; growth is rare)
└── all operations operate on MemorySegments — ZERO Java alloc on hot path

ForStRsKeyGroupedInternalPriorityQueue (refactored):
├── engine (current, unchanged):
│     Composite key layout: "q/" || stateName || "/" || kg(2B BE) || ts(8B BE) || serialize(T)
│     Engine accessed via linker.batchPut / linker.frsBatchPrefixScan / linker.frsVectorizedBatchDelete
├── pendingBuffer: ArrowTimerBuffer (above)
├── flush triggers (3 moments only):
│     1. advance() entry — before processing due timers (read boundary)
│     2. threshold reached — pendingBuffer.size ≥ 1024
│     3. checkpoint barrier — snapshot must see consistent state
└── scratchArena (thread-local): for composing the composite key per call (reused across calls)

External operations:
  add(T element):
    1. Compose key into scratchArena → (keyOffset, keyLen) view (off-heap, no byte[])
    2. hash = hash(scratchArena, keyOffset, keyLen)
    3. pendingBuffer.lookup(hash, scratchArena, keyOffset, keyLen):
       — found ADD → no-op (idempotent)
       — found REMOVE → CANCEL: remove from heap + hashIndex (add+remove cancel)
       — not found → copy key into pendingBuffer.keyData; insert ADD entry into heapArray (heap-swim up)
    4. if pendingBuffer.size ≥ 1024 → flushPendingToEngine

  remove(T element):
    1. Compose key into scratchArena
    2. hash + lookup as above
       — found ADD → CANCEL
       — found REMOVE → no-op
       — not found → insert REMOVE entry into heapArray
    3. threshold check

  peek() / iterator() (NON-flushing — merged view):
    Walk pendingBuffer's heap-min-walk (preorder by ts) interleaved with engine prefix scan.
    Suppress entries marked REMOVE in pendingBuffer. Return next due timer.
    O(log N) per heap step + O(B) engine batch read.

  advance(maxTimestamp):
    1. flushPendingToEngine — drain heapArray:
       — collect ADD entries → linker.batchPut keys
       — collect REMOVE entries → linker.frsVectorizedBatchDelete keys
       — pendingBuffer.clear() (heap size = 0, keyData reused next call)
    2. linker.frsBatchPrefixScan(engine kg-prefix) — single FFM, vectorized
    3. For each entry returned with ts ≤ maxTimestamp:
       — invoke trigger callback (key view is MemorySegment slice from the scan result)
       — accumulate keys into deleteBatch (MemorySegment array)
    4. linker.frsVectorizedBatchDelete(deleteBatch) — single FFM
    5. Close iterator handle

  snapshot(checkpointId):
    1. flushPendingToEngine
    2. Existing engine snapshot path

  close():
    1. flushPendingToEngine
    2. pendingBuffer.close() (releases Arena)
    3. Existing close
```

**Zero-copy contract** — at every hot-path interface:
- Composite key encoded directly into thread-local scratch `MemorySegment` (no byte[])
- Buffer storage in off-heap Arena (heapArray + keyData are MemorySegment regions)
- FFM crossings carry `MemorySegment` views (no Java→native byte[] copy)
- Iterator results carry `MemorySegment` views (no native→Java byte[] alloc)
- The only Java objects allocated per timer event: NONE on the hot path

## Four Implementation Invariants (Critical Checklist)

These four invariants are non-negotiable; missing any one degrades the design back to a slower variant:

1. **In-buffer add-remove cancellation** — when an `add(T)` follows by `remove(T)` (or vice-versa) BEFORE a flush, both operations cancel out and never reach the engine. For Q11 SESSION-window timer churn (session merging registers + cancels timers many times per bidder), this can eliminate the majority of FFM ops.

2. **Min-heap buffer (NOT FIFO)** — pendingBuffer is ordered by timestamp so the merged `peek()` view stays O(log N). A FIFO buffer would require O(N) scan to find next-due timer.

3. **`advance()` internal flush order** — strictly:
   1. Flush pending adds + removes to engine (writes complete)
   2. Batch prefix-scan engine for due timers
   3. Batch delete + invoke trigger callbacks
   This ordering ensures the engine view is consistent with the pendingBuffer at the moment of advance.

4. **Checkpoint-entry mandatory flush** — `snapshot()` flushes pendingBuffer to engine BEFORE the checkpoint state is captured. Otherwise pending adds/removes would not be in the snapshot → restore would lose timers → correctness violation.

## Components Touched

| Component | File | Change |
|---|---|---|
| `ForStRsKeyGroupedInternalPriorityQueue` | `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/timer/ForStRsKeyGroupedInternalPriorityQueue.java` | Add `pendingBuffer: PriorityQueue<TimerEntry>` + `flushPendingToEngine` + cancellation logic in `add`/`remove`. Replace `linker.put` / `linker.get`+`linker.delete` per call with buffer-pending pattern. Replace per-iter `linker.delete` with `linker.frsVectorizedBatchDelete` batch. Add merged `peek()` walking buffer + engine. |
| `linker` | `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java` | Verify existing `batchPut` / `frsVectorizedBatchDelete` / `frsBatchPrefixScan` have compatible signatures for the new caller. If not, add a convenience wrapper. |
| Default timer factory | `ForStRsAbstractKeyedStateBackend.java` | Change default in `pickTimerFactory()` from HEAP → FORSTRS. HEAP stays available via `-Dforst.rs.timer-service.factory=HEAP` for back-out. |
| Tests | `ForStRsKeyGroupedInternalPriorityQueueTest.java` (new or extend) | Unit tests for the 4 invariants: add-remove cancellation, min-heap ordering, advance() flush sequencing, snapshot-entry flush. |

## Data Flow (Q11 SESSION-window example)

```
Operator handles event for bidder B at time T:
  acc.update(...)         (state op — unchanged)
  timerService.registerEventTimeTimer(T + 10s)
    → add(Timer{bidder=B, ts=T+10s})
    → pendingBuffer.insert
       (size below 1024 — no flush)

Many events later, session merges and CANCELS earlier timer:
  timerService.deleteEventTimeTimer(T + 10s)
    → remove(Timer{bidder=B, ts=T+10s})
    → pendingBuffer.lookup → found ADD → CANCEL (remove from buffer)
       ← never reaches engine; net zero FFM cost for this timer

vs current code:
  add → linker.put (1 FFM call)
  remove → linker.get + linker.delete (2 FFM calls)
  total: 3 FFM calls for the same timer that ultimately did nothing

For Q11 with ~50% timer cancellation rate (session merging), this halves the
FFM cost on the timer path. Combined with batched flushes for the remaining
non-cancelled timers, expected Q11 wall-clock drops below the HEAP baseline.
```

## Correctness Invariants

- **Timer ordering**: peek()/poll() always return the smallest-timestamp pending due timer across BOTH pendingBuffer-ADD and engine. Min-heap maintenance ensures this is O(log N).
- **No double-fire**: `advance()` removes timers from both pendingBuffer (already gone if cancelled) AND engine (via batch delete). No timer is processed twice.
- **No miss-fire**: if the engine has a timer at ts T and pendingBuffer has a REMOVE for it, advance()'s flush step writes the delete to engine first; subsequent prefix-scan no longer sees it. Trigger is correctly NOT invoked.
- **Snapshot consistency**: pendingBuffer is flushed BEFORE snapshot. The engine state in the snapshot reflects all pending adds and removes.
- **Restore consistency**: on restore, pendingBuffer starts empty; all timer state lives in the engine and is restored via existing engine restore path. Existing checkpoint format unchanged.
- **Concurrency**: single-threaded per Flink slot — same guarantee as today. No locks.

## Error Handling

- **`pendingBuffer` capacity bounds**: max-cap at e.g. 65536 entries to prevent unbounded heap growth on pathological workloads. On reaching cap, force-flush even mid-add. Threshold 1024 means typical flush is small (1K entries).
- **Engine flush failure**: existing FFM error propagation (FRS_STATUS_ERROR) unchanged. Failures cause the operator to fail-fast as today.
- **HEAP fallback**: if FORSTRS path has a regression on some unforeseen workload, `-Dforst.rs.timer-service.factory=HEAP` restores the prior behavior.

## Testing

### Unit tests (new — at minimum 4 covering the 4 invariants)

1. **`addRemoveCancelsInBuffer`** — add T1, remove T1, check buffer is empty AND no FFM call made (mock linker counts calls).
2. **`minHeapPeekReturnsEarliest`** — add timers at ts=10, 5, 20, 1, peek returns T(ts=1) without flushing.
3. **`advanceFlushesThenScansThenDeletes`** — mock linker; verify call order: 1× batchPut/batchDelete (flush) → 1× prefixScan → 1× vectorizedBatchDelete.
4. **`snapshotFlushesPendingBuffer`** — buffer non-empty, call snapshot(), verify flush occurred before snapshot state captured.

### Existing tests that must still pass

- All `ForStRsKeyGroupedInternalPriorityQueueTest` — they test add/poll/peek behaviors; should all pass with new impl behind the same semantics.

### Bench acceptance gates

| Q | Target | Notes |
|---|---|---|
| Q5  | ≤ 30 s | preserve FORSTRS-engine win |
| Q8  | ≤ 30 s | preserve FORSTRS-engine win |
| Q11 | ≤ 75 s | BEAT current HEAP (was 134 s un-batched) |
| Q12 | ≤ 35 s | BEAT current HEAP (was 131 s un-batched) |
| Q9  | ≤ 65 s | preserve MapState 1c.1 win |
| Q15 | ≤ 25 s | preserve MapState 1c.1 win |
| Q20 | ≤ 60 s | preserve MapState 1c.1 win |
| Q13 | ≤ 45 s | within current noise |

If Q11/Q12 land above 75 s/35 s, the cancellation invariant isn't firing as expected — JFR profile, investigate.

## Implementation Order — single PR

1. **Implement TimerEntry + pendingBuffer min-heap** — pure Java unit, no FFM. Unit tests for cancellation + heap ordering.
2. **Wire add/remove → pendingBuffer with cancellation**. Unit tests asserting no linker calls for cancelled pairs.
3. **Wire advance() with 3-step internal order** (flush adds → batch scan → batch delete). Unit tests asserting call order.
4. **Wire snapshot() pre-flush**. Unit test asserting flush precedes snapshot.
5. **Wire merged peek()/iterator()** — non-flushing merged view. Unit tests for correctness.
6. **Change pickTimerFactory() default from HEAP → FORSTRS.**
7. **Build, deploy, bench** Q5/Q8/Q9/Q11/Q12/Q13/Q15/Q20 with fresh-cluster.
8. **Commit** if all KPI gates pass.

## Out of Scope (follow-on)

- Removing HEAP timer factory implementation — keep as fallback.
- Further optimization of timer composite-key encoding (off-heap MemorySegment path) — could come later.
- Rust-side direct `frs_timer_*_segment` FFI — engine could absorb the timer queue logic entirely. Approach-D.

## Open Questions

None at design time. The 4 implementation invariants are the critical surface area for review.
