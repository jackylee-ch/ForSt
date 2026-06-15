# Stage-0 — q8 cache-corruption race: DETERMINISTIC repro + root cause + residual assessment

**Date:** 2026-06-15 · **Status:** repro SHIPPED (test-only, main); residual op-mix race still OPEN
**Repo:** flink-statebackend-forst-rs (test-only, JDK 25 module)
**Context:** Stage-0 of the two-regime executor design
(`2026-06-11-two-regime-executor-design.md` §4) is the BLOCKING gate for Approach-3
(coordination-free executor) / R2b. It requires a deterministic, in-repo reproduction of the q8
windowed-join wrong-output so the race can be root-caused and fixed. Prior attempts
(/tmp/s0-*-q8, /tmp/ra-*-q8) were free-running stress races → flaky → Stage-0 stayed open.

## 1. What shipped this cycle

`MapStateCacheConcurrentCorruptionTest` (cache package, 3 tests) — a **deterministic** (5/5 runs,
no sleeps, no stress loop) reproduction of the documented MapStateCache cross-thread race
(coordinated-executor design §2.2), plus a single-threaded control proving the logic is correct
when serialized.

- `concurrentInsertLosesAWrite_deterministic` — two concurrent inserts of DISTINCT keys with the
  `size` read-modify-write forced to interleave (CyclicBarrier pinned between `row = size++` and
  the publish steps). One key is **silently lost** (size advances by 1 for 2 inserts) even though
  both `put` calls returned normally. This is exactly the q8 symptom: missing window-join output
  rows.
- `concurrentPutVsPutIfAbsentStaleRead_deterministic` — replays the EXACT
  `ForStRsMapStateV2.asyncGet` production interleave: a mailbox `put(NEW)` races the GET-miss
  continuation's worker `putIfAbsent(ENGINE_OLD)`, with the barrier between the continuation's
  `findRow` (decides "absent") and its publish. Result: the stale engine value shadows the
  authoritative new value, or a duplicate row is created — a serialization violation impossible on
  the single mailbox thread.
- `singleThreadedIsCorrect_control` — the identical sequence on one thread is correct (both keys
  present, size==2, newest value wins). Proves concurrency is the cause, not the cache logic.

The corruption is forced via reflection on the `final` class's private internals
(`size`/`clock`/`values`, `appendKey`/`insertHashIndex`/`hashOf`), replaying the real `put` body
bit-for-bit with only a barrier inserted — so the reproduced corruption IS the production
corruption, not a model of it. Reflection-into-internals is an established pattern in this module
(`ForStRsMapStateV2CacheTest`).

## 2. Root cause (now deterministically proven, not inferred)

`MapStateCache` is documented SINGLE-THREADED ("No internal synchronization", relies on Flink's
per-record RecordContext lock). That holds under the **default depth-1 inline executor** — every
cache op runs on the mailbox thread.

Under ANY parallel/coordinated executor the contract is violated **by construction**:
`asyncGet` does the cache `lookup`/`put` on the **mailbox** thread, but on a miss registers
`.thenApply(v -> cache.putIfAbsent(keySnapshot, v))` whose continuation completes on the
**worker/completing** thread. Two threads then mutate an unsynchronized open-addressed off-heap
hash index. Every NEW-key insert is the non-atomic compound `row = size++; appendKey(row);
values[row]=…; insertHashIndex(h,row)` — a textbook lost-update.

## 3. Why this repro does NOT, by itself, unblock Approach-3 (honest scoping)

The cache race is **already mitigated in production**: `ForStRsMapStateV2.DISABLE_MAPSTATE_CACHE`
is wired to `parallelExecutorActive()`, so the cache is statically BYPASSED under every parallel
mode (`coordinated`/`routing`/`routing-async`/`two-regime`/`adaptive`). The recorded
q8 cache-off result (3,064,514 ≈ RocksDB 3,064,457) confirms the mitigation. So this deterministic
repro:
- **validates** the cache-off coupling rationale and is a permanent regression guard against anyone
  re-enabling the cache under a parallel executor (or building the PR-2 worker-confined cache
  without confinement), and
- **deterministically closes** one of the two known q8 races.

But the two-regime ledger (§2) records that q8 still under-emits **−77% with cache-off AND staging
buffers-off AND single-worker** — a SECOND, distinct residual race "inside our backend's execution
of the window-join op mix (LIST_ADD / ITER-at-fire / CLEAR)", root cause OPEN. That residual is the
true Approach-3 blocker, and it is NOT the cache.

## 4. Residual op-mix race — analysis + deterministic-repro design (next cycle)

Even with workers=1, the mailbox thread and the single worker thread are two threads: the mailbox
runs the AEC offer phase (and, on a watermark/window fire, the timer callback's
`ITER-at-fire` + `CLEAR`) while the worker drains queued `LIST_ADD` batches. The suspected hazard:
a window fire's ITER reads list state whose preceding LIST_ADDs are still queued on (or in-flight
at) the worker — a write/read reorder across the mailbox→worker boundary that the single-worker
FIFO does not order against the mailbox-side timer path.

Two-regime §3.3 invariant 3 claims `SERIAL_BETWEEN_EPOCH` full-drains before triggers
(`EpochManager.java:134`). The residual −77% says either (a) that drain does not cover the
pipelined worker queue under `routing-async`, or (b) the fire path issues a mailbox-direct
engine op that overtakes queued worker writes (the staging-buffer wedge already seen for MapState).

**Deterministic-repro design for the residual (proposed, NOT built this cycle — timeboxed):**
build it at the executor boundary, not the data-structure level — a `RoutingStateExecutor`-level
integration test with a SEEDED interleaving: enqueue a `LIST_ADD` batch to the (single) worker,
hold it at a test-controlled barrier inside the worker's drain, then run a mailbox-side
`ITER`+`CLEAR` for the same key-group, and assert the ITER observes the queued ADD (read-your-writes
across the boundary). This needs a test seam in the executor's worker-drain (a package-private
"pause point" latch, default no-op) — the same shape as the cache barrier here, lifted one layer.
That seam is the next concrete deliverable; it is a multi-component change (executor + classifier
+ timer path) and was correctly out of scope for this timeboxed cycle.

## 5. Test evidence

- `MapStateCacheConcurrentCorruptionTest`: 3/3 pass, 5/5 deterministic across reruns.
- Full cache package regression: 60/60 pass (no regression from the new file).
- Run: `JAVA_HOME=<jdk25> ./mvnw -pl flink-state-backends/flink-statebackend-forst-rs
  -Pforst-rs-jdk25 test -Dtest=MapStateCacheConcurrentCorruptionTest
  -Dforstrs.native.tests.skip=false -o` (module is JDK-25-gated; pure-Java test, no native lib).

## 6. Next-cycle candidate

The residual op-mix race (executor-boundary seam, §4) is the real Stage-0 long-pole. If it again
proves intractable in a single timeboxed cycle, the parallel pivot is the q20 Top-N lever — see
`2026-06-15-q20-topn-residual-wall-design.md` (this cycle's pivot deliverable).
