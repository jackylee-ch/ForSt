# WA-V0 lifecycle watermark / event-time feed — scope finding

**Date:** 2026-06-13
**Author:** PMC+RD (Task-B investigation)
**Verdict:** OUT OF SCOPE for the forst-rs backend + engine. A backend-only feed does **NOT** exist; making the lifecycle write-amp fix effective requires a minimal **operator/runtime** change (one call site in `AbstractStreamOperator`).

---

## 1. Problem

The WA-V0 lifecycle write path (engine-side, default OFF) drops whole death-bucketed
segments at the **engine watermark** instead of compacting soon-dead bytes
(measured: compaction ≈ 87% of physical write bytes; TTL-segment FIFO-drop floor =
write-amp **0.98** vs 2.95 baseline). The Flink-side adoption
(`ForStRsLifecycleManager`, commit `cedca903f42`) wires three engine calls:

- `frs_cf_set_lifecycle(db, cf, kind, ttl)` — **wired** (driven from the state-register hook,
  gated by `forst.rs.lifecycle.segments`).
- `frs_cf_note_max_event_time(db, cf, eventTimeMs)` — **NOT fed**.
- `frs_cf_advance_watermark(db, cf, watermarkMs)` — **NOT fed**.

Because the two clocks are never advanced, the engine keeps `max_event_time = 0` /
`watermark = un-advanced`, so the V1 death stamp (`maxEventTime + ttl`) stays in the
future forever — i.e. **never premature** (the soundness contract the engine requires),
but also **never fires**. The fix is therefore currently INERT: correct, but reclaiming
nothing.

The scope question: can the **keyed state backend** obtain the current event-time /
watermark through a path the backend itself controls (no operator/runtime changes)?

## 2. Candidate paths examined (Flink 2.2, branch `forst-rs-jdk25`)

### 2.1 The timer service the backend owns — the most promising lead, but INSUFFICIENT

The forst-rs backend provides the priority-queue-set
(`ForStRsKeyGroupedInternalPriorityQueue`) that backs event-time timers. The operator
watermark reaches the timer service via:

```
AbstractStreamOperator.processWatermark(Watermark)
  → InternalTimeServiceManagerImpl.advanceWatermark(Watermark)            (line 212)
    → InternalTimerServiceImpl.advanceWatermark(long time)                (line 314)
      → tryAdvanceWatermark(long time, ShouldStopAdvancingFn)             (line 327)
```

In `tryAdvanceWatermark` (`flink-runtime/.../InternalTimerServiceImpl.java:327`) the
watermark `time` is stored in `InternalTimerServiceImpl.currentWatermark` — **runtime
state, not backend state** — and the queue is driven only by:

```java
while ((timer = eventTimeTimersQueue.peek()) != null
        && timer.getTimestamp() <= time ...) {
    keyContext.setCurrentKey(timer.getKey());
    eventTimeTimersQueue.poll();
    triggerTarget.onEventTime(timer);
}
```

The backend's queue sees only `peek()` / `poll()`. It is **never** handed the watermark
value. The backend's own batched `advance(long maxTimestamp, Consumer)` API
(`ForStRsKeyGroupedInternalPriorityQueue.java:1335`) is **not invoked** by Flink's
`InternalTimerServiceImpl` — Flink calls `poll()`, not `advance()`.

Why the queue cannot derive the watermark from poll observations: the highest timestamp
polled is only a **lower bound** on the watermark (the watermark may be far above the last
fired timer, and may advance with **no timers registered at all**). A death-stamp derived
from a lower-bound watermark would still be sound (never premature) but would lag
arbitrarily and reclaim nothing in the common "few/no timers" case — defeating the fix.
There is no backend-observable signal carrying the watermark *value*.

### 2.2 Async Execution Controller / RecordContext — no timestamp

`RecordContext` (`flink-runtime/.../asyncprocessing/RecordContext.java`) carries
`record / key / keyGroup / epoch / priority` + an opaque `extra` slot. **No timestamp /
event-time field.** The backend cannot read record event-time on the async write path.

### 2.3 `setCurrentKey` / keyed-state-backend context — no watermark

`InternalKeyContext` exposes `getCurrentKey()` / key-group only. No watermark, no
current-record-timestamp callback.

### 2.4 Backend interface hierarchy — no watermark callback

`ForStRsAsyncKeyedStateBackend implements AsyncKeyedStateBackend<K>` (and the V1-sync
`ForStRsKeyedStateBackend` / `ForStRsAbstractKeyedStateBackend`) inherit **no**
`processWatermark` / `advanceWatermark` / `onWatermarkAdvanced` method. Flink 2.2 keyed
state backends are not on the watermark propagation path.

## 3. Verdict

**(a) Backend-only feed exists? NO.**

The watermark originates in the runtime watermark-propagation layer, flows through the
operator's `processWatermark()` → `timeServiceManager.advanceWatermark()` chain (both
outside the forst-rs module), and reaches the backend only *indirectly* via timer firing
(which carries no watermark value). The record event-time is likewise never threaded onto
the keyed-value write path.

**(b) Therefore Task-B is OUT OF SCOPE** for the forst-rs-backend + engine. No runtime
code was changed (per the task's stop condition).

## 4. Minimal runtime change required (for a future, in-scope-by-approval PR)

One call site, in `flink-runtime`'s
`org.apache.flink.streaming.api.operators.AbstractStreamOperator`, on the watermark-emit
path (`emitWatermarkDirectly` / `processWatermark`):

```java
// after timeServiceManager.advanceWatermark(mark):
if (keyedStateBackend instanceof ForStRsAsyncKeyedStateBackend<?> frs
        && frs.getLifecycleManager() != null) {
    long allowedLatenessMs = /* operator's allowed-lateness, 0 if none */;
    frs.getLifecycleManager().advanceWatermark(mark.getTimestamp() - allowedLatenessMs);
}
```

Plus, on the write path, threading the record timestamp into
`ForStRsLifecycleManager.noteMaxEventTime` at the existing write-batch cadence (a per-batch
atomic-max — NOT per record). The record timestamp is available to the **operator**
(`StreamRecord.getTimestamp()`); it is not threaded into the keyed-value state write,
which is why the backend cannot self-observe it.

Notes for that change:
- `advanceWatermark` must subtract allowed-lateness slack so late events inside the
  allowed-lateness window keep state alive (the engine watermark only advances *past*
  lateness). `ForStRsLifecycleManager.advanceWatermark` already documents this caller
  contract.
- The hook must be a no-op unless `forst.rs.lifecycle.segments` is set and the backend is
  the forst-rs async backend, so default behaviour stays byte-identical.
- An instanceof check against the forst-rs backend type pulls a backend dependency into
  `flink-runtime`, which is the architectural reason this is a runtime-layer change rather
  than a backend-only one. A cleaner long-term form is a generic
  `WatermarkAwareKeyedStateBackend` SPI in `flink-runtime` that the forst-rs backend
  implements — but that is still a runtime change (new interface + the operator call site),
  i.e. out of the forst-rs-backend+engine scope.

## 5. Bottom line

The lifecycle write-amp fix (0.98× measured) **cannot be made effective within the
forst-rs-backend + engine scope alone**. It is correct and sound today (never premature),
but inert until the one operator-layer watermark/event-time feed above is added. The
backend-side machinery (`ForStRsLifecycleManager.advanceWatermark` / `noteMaxEventTime`,
the `frs_cf_*` FFI) is already in place and ready to be driven the moment that feed exists.
