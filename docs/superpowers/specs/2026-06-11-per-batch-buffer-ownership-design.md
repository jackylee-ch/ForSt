# Per-Batch Buffer Ownership (seal/swap) — Design

**Date:** 2026-06-11 · **Status:** user-approved direction (B-spike → A), spec for review
**Repos:** flink-statebackend-forst-rs only (NO Flink-runtime changes — the AEC contract
already supports non-blocking executors; verified: AEC ignores the container future,
consults `fullyLoaded()` at trigger, per-row futures drive completion via
CallbackRunnerWrapper).

## 1. Problem (proven by the 2026-06-11 q8@100M canary)

Two V2 state classes stage writes in per-state off-heap buffers that assume **lockstep**
(offer phase N+1 strictly after execute phase N):

| Buffer | Offer-time (mailbox) | Drain | Race under pipelining |
|---|---|---|---|
| `MapStateArrowBuffer` (`ForStRsMapStateV2.offHeapBuf`) | `asyncPut`/`asyncRemove` stage (completed future — never enters the executor); `asyncGet`/`asyncContains` probe (read-your-writes) | **Mailbox**: auto-flush watermark calls `linker.batchPut` directly; snapshot pre-hook `flushOffHeapBuffer()` | ORDERING: mailbox-direct engine writes overtake queued worker reads of the same key-group |
| `ListStateArrowBuffer` (`ForStRsAsyncListStateV2`) | `asyncAdd` stages (`amDirty`) | **Worker**: `classifier.flushOffHeapListBuffersIfDirty()` during batch execution; snapshot pre-hook | TORN MEMORY (two threads, one buffer) + ordering |

Value/Reducing/Aggregating V2 have **no** staging buffers — out of scope.

Canary evidence: routing-async (non-blocking dispatch, kg-affine FIFOs — executor-level
design sound, classifier buffers per-batch private) wedged once and under-emitted −58%
once on q8@100M. This also re-explains the 2026-06-10 coordinated-mode corruption
("mailbox-direct statebuf writes overtook queued reads") — it was never inline-specific.

Motivation (q9@100M symbolized profile): TM averages 1.7/8 cores under blocking routing;
the mailbox latch caps pipeline depth at 1. Pipelining is the only lever that converts the
6.3 idle cores; these buffers are its only blocker.

## 2. Approach B — validation spike (build FIRST, ~20 gated lines)

Under `FRS_RS_EXECUTOR=routing-async` ONLY (`parallelExecutorActive()`-style gate,
reusing the exact mode check that already disables the MapStateCache):

- `ForStRsMapStateV2`: construct with `offHeapBuf = null` (the existing null-path falls
  back to `super.asyncPut/asyncGet/...` → classifier PUT/GET requests).
- `ForStRsAsyncListStateV2`: skip the `ListStateArrowBuffer` staging (existing non-buffer
  path = heap APPEND_MERGE via classifier `amBuf`, already per-batch private).

Correctness argument: with no staging, ALL state effects flow through classifier-private
buffers and the engine. Same-batch read-your-writes: `executeBatchRequests` order is
PUT → DELETE → GET → ITER → APPEND_MERGE. Cross-batch: per-worker FIFO (same kg → same
worker, batch N enqueued before N+1). Same-key cross-record: AEC KeyAccountingUnit.
Snapshot: nothing staged; Trace E hooks become no-ops for these states.

Cost accepted for the spike: loses offer-time hit-serving (gets that the buffer answered
with a completed future now ride the executor) and put-coalescing. The spike measures the
NET of (pipelining gain − staging loss).

Gates (in order, all on 8c/32g):
1. q8@100M ×3 — out_rows in the 3,064,4xx band every run, else STOP (race still present
   somewhere → re-diagnose before any perf reading).
2. q9@100M routing-async+drain200K, back-to-back vs the 2215.6s same-day control.
3. Decision: q9 materially better (≥10%) → proceed to A. Flat/worse → the idle-core
   hypothesis under-delivers; STOP and re-profile under routing-async before building A.

## 3. Approach A — per-worker-sharded staging + seal/swap (the build)

Restores the staging buffer's coalescing + offer-time hit-serving under pipelining.

### 3.1 Sharding
Each staging buffer becomes `N = workerCount()` shards, routed `shard = kg % N` — the
SAME function as `RoutingRequestContainer.offer`, so a shard's rows always belong to
exactly one worker. Same key → same shard ⇒ dedupe and tombstone semantics unchanged.

### 3.2 Seal/swap at dispatch
`RoutingStateExecutor.executeBatchRequests` (mailbox, before dispatch) calls a new
`sealDirtyShards()` per registered state: for each dirty shard, atomically swap in a
fresh shard; the SEALED shard (immutable from that point — workers only read it to drain)
is appended to the owning worker's sub-batch as a pre-op drain item.

### 3.3 Worker drain-first
`VectorizedExecutor.executeBatchRequests` drains attached sealed shards (vectorized
`linker.batchPut` of the shard's live rows + tombstone deletes) BEFORE the existing
PUT → DELETE → GET → ITER pipeline. Write-before-read holds per batch; per-worker FIFO
holds across batches ⇒ a get/iter never misses an earlier staged write for its key-group.

### 3.4 Read-your-writes across generations
Offer-time lookups probe newest-first: fresh shard → pending sealed shards (FIFO list per
shard slot). Sealed shards stay probe-visible until reclaimed. Bounded by
`FRS_RS_MAX_INFLIGHT_BATCHES` generations (default 2×workers).

### 3.5 Reclaim (mailbox-owned lifecycle)
Worker marks a sealed shard `drained` (volatile). The MAILBOX reclaims drained shards at
the NEXT dispatch (close arena / return to pool) — no concurrent close vs a mailbox probe,
no refcounting. A probe racing the flag reads a drained-but-still-allocated shard: stale
read is impossible (engine already has the rows; probe hit returns the same bytes).

### 3.6 Watermark overflow
A shard hitting capacity mid-offer can no longer drain on the mailbox. It self-seals into
the pending list (swap fresh) — the next dispatch attaches it. Lookups already probe
pending shards, so nothing is lost. The auto-tuner keeps shard sizing adaptive.

### 3.7 Snapshot (Trace E barrier)
The pre-snapshot hook seals ALL dirty shards and drains every pending sealed shard
synchronously on the mailbox. Safe: the AEC has already drained in-flight records at the
barrier, so no worker is executing (verified contract), and lockstep snapshot semantics
are preserved bit-for-bit.

### 3.8 Lockstep modes unchanged
Under inline/routing/adaptive the shard count is still N but seal+drain degenerate to the
current behavior (dispatch drains synchronously in-line); zero behavior change for the
default path. All changes env-gated until gates pass.

## 4. Testing
- UT: shard routing (kg→shard stability), seal immutability, generation probe order,
  reclaim-only-after-drain, overflow self-seal, snapshot all-drain. Stub-worker contract
  tests extend `RoutingStateExecutorAsyncTest`.
- Canary: q8@100M ×3 exact (the discriminator that caught every prior attempt).
- Full 10M exactness sweep (20/22 baseline) under routing-async.
- Perf: q9@100M A/B vs same-day control; then q20, q7. Record everything in the sweep doc.
- GHA both repos.

## 5. Risks
- Probe-generation walk cost on the offer hot path (mitigate: generations rarely >1 when
  workers keep up; fresh-shard hit short-circuits).
- Iteration paths that today force a buffer flush at offer time must instead consume the
  pending-shard view or force a seal (audit `asyncEntries`/iterator setup during A's plan).
- Sizing: N shards × states × generations multiplies resident staging memory; auto-tuner
  caps apply per shard (divide existing caps by N as the starting policy).
