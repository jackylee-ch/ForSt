# forst-rs beats community forst on Q12 — HEAP-timer-queue fix

**Status:** Landed. forst-rs Q12 = **35.539 s** at 2.81 M/s, vs community forst 42.707 s at 2.34 M/s. **forst-rs is 1.20× FASTER than forst.**
**Date:** 2026-05-19

## The structural difference

**Why was forst (community) ~2.75× faster than forst-rs on Q12 PROCTIME tumble?**

JFR profile of forst's Q12 showed `HeapPriorityQueueSet.add` as the top timer-queue frame — **forst uses an in-memory heap-based priority queue** for the async-V2 timer service (per `state.backend.forst.timer-service.factory` default + the heap-vs-engine branch in `ForStKeyedStateBackend.create()`).

forst-rs's `ForStRsAsyncKeyedStateBackend.create()` unconditionally returned `ForStRsKeyGroupedInternalPriorityQueue` — an engine-backed timer queue that issues FFM calls for every add/poll/remove. For Q12 (PROCTIME tumble, ~3M unique timers per task), this was a 32% CPU hot spot even after the prior write-behind-buffer fix on the timer-queue itself.

Engine-backed timers exist to survive task restarts without re-shipping state through checkpoints. But Flink's HEAP timer service IS checkpointable — it just serializes to the checkpoint stream rather than continuously persisting to the engine. The engine-backed path is the more durable but slower default; the HEAP path is correct for any deployment that does periodic checkpoints (which the user's setup does at 30s intervals).

## The fix

Two-file change in `flink-state-backends/flink-statebackend-forst-rs/src/main/java/`:

1. `keyed/ForStRsAsyncKeyedStateBackend.java` — the async-V2 path used by Q11/Q12:
   - Added JVM-flag-gated `TimerServiceFactory.HEAP` (default) vs `FORSTRS` (engine).
   - On HEAP, `create()` returns `new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128).create(...)` — the same Flink-runtime in-memory heap queue forst uses.
   - On FORSTRS, retains the engine-backed `ForStRsKeyGroupedInternalPriorityQueue` for workloads with timer cardinality > heap budget.
   - Added `numberOfKeyGroups` constructor parameter (required by `HeapPriorityQueueSetFactory` — without the GLOBAL key-group count, `KeyGroupRangeAssignment.assignToKeyGroup` computes wrong key groups and rejects elements). First attempt used the LOCAL range size; produced `IllegalArgumentException: KeyGroupRange does not contain key group N` per-record. The fix is to thread `parameters.getNumberOfKeyGroups()` from `ForStRsStateBackend.createAsyncKeyedStateBackend` through to the constructor.

2. `ForStRsStateBackend.java` — pass `parameters.getNumberOfKeyGroups()` to the async-keyed-state-backend constructor.

3. `keyed/ForStRsAbstractKeyedStateBackend.java` — applied the same HEAP/FORSTRS branch to the sync-V1 path's `create()` method for parity (Q12 doesn't traverse this path, but consistency matters).

The JVM flag controls the choice:

```bash
-Dforst.rs.timer-service.factory=HEAP    # default — fast, requires checkpointing
-Dforst.rs.timer-service.factory=FORSTRS # opt-in — engine-backed, durable without checkpoints
```

## Numbers

| Backend | Q12 wall-clock | Throughput |
|---|---|---|
| forst-rs (start of session, no fixes) | 124.435 s | 803.63 K/s |
| forst-rs (timer write-behind batchPut, earlier today) | ~117 s mean (114.9 / 118.4 / 118.9 over 3 runs) | ~852 K/s |
| forst (community Java + RocksDB JNI) | 42.707 s | 2.34 M/s |
| **forst-rs (HEAP-timer-queue, this fix)** | **35.539 s** | **2.81 M/s** |

**forst-rs vs forst-rs baseline: 3.50× speedup (124.4 → 35.5 s).**
**forst-rs vs forst (community): 1.20× speedup (42.7 → 35.5 s).** Meets the user's "1.x speedup over forst" target.

## Why the prior fixes didn't move the needle as much

The earlier timer-add write-behind buffer (`ForStRsKeyGroupedInternalPriorityQueue.flushPendingAdds`) was correct for the engine-backed path — it batches the per-key `linker.put` calls into one `linker.batchPut`. That fix delivered ~6 % wall-clock improvement.

But the **deeper problem** was that the engine-backed path was being used at all when an in-memory heap was the right choice. Even a perfectly-batched engine-backed timer queue is slower than the heap — every FFM crossing for a write batch is still slower than a Java HashMap insert + a heap `siftUp`.

The lesson: profile **forst's** Q12 to see what IT does differently, not just optimize forst-rs's same approach further. Forst's measurement told us "no engine calls for timers", which redirected us from a "make the engine-call faster" path to a "skip the engine-call entirely" path. The former was diminishing returns; the latter unlocked the 1.x target.

## Side benefit: the timer write-behind batchPut still helps under FORSTRS

The earlier write-behind buffer fix in `ForStRsKeyGroupedInternalPriorityQueue` remains valuable for the FORSTRS-mode opt-in path (large timer cardinality > heap budget). It now sits behind a feature-flag gate but is not the default.

## Cross-references

- [`2026-05-19-q12-timer-batchput-win.md`](./2026-05-19-q12-timer-batchput-win.md) — the earlier write-behind buffer fix, now superseded as the default Q12 path but still active under `-Dforst.rs.timer-service.factory=FORSTRS`.
- [`2026-05-19-q11q12-state-primitive-audit.md`](./2026-05-19-q11q12-state-primitive-audit.md) — the original Q11/Q12 audit. The "state primitive is ForStRsValueStateV2" finding is still correct; what we missed at the time was that the **timer service** uses a separate priority-queue primitive and that's where the bottleneck lived.
- [`2026-05-19-pmc-update-q12-batch-histogram.md`](./2026-05-19-pmc-update-q12-batch-histogram.md) — the §6 batch-histogram measurement showed engine ops were < 1 % of Q12 wall-clock when state ops were measured but **missed** the timer ops entirely (timer-queue uses `linker.put`, not the `VectorizedExecutor` paths the instrumentation watched). The measurement spike's blind spot is what led to the wrong V1.1 lane recommendations earlier in the session.
- CONTRIBUTING.md "Document the 3-layer state-class call stack with grep evidence" — applied here: we identified that the Q12 path goes through `AsyncStateWindowAggOperator → InternalTimerServiceAsyncImpl → ForStRsAsyncKeyedStateBackend.create → ForStRsKeyGroupedInternalPriorityQueue`. The fix lives at the third layer.
