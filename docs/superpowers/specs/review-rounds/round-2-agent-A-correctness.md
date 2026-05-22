# Round 2 Agent A — Correctness / Data Consistency Review

Reviewer angle: criterion #1 — re-examine A1-H5, C-H4, D-H1 fixes for NEW
defects + look for analogous anti-patterns that Round 1 missed. Cold-read
plus targeted re-read of commit 66f153868fc surface area.

Already documented as architectural-debt and **not re-reported here unless a
new sub-issue was found**: A1-H1, A1-H2, A1-H3, A1-H4, A1-H6, E-CRIT-1,
E-CRIT-2, E-CRIT-3.

## Summary
- HIGH findings: 3
- MEDIUM findings: 3
- LOW findings: 2

---

## HIGH severity

### A2-H1 — `executeRequestSync` APPEND_MERGE path leaves StateRequest future uncompleted on both success AND failure
- **File:line:** `flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java:226-243`
- **Code snippet:**
  ```java
  @Override
  public void executeRequestSync(StateRequest<?, ?, ?, ?> request) {
      VectorizedClassifier single = new VectorizedClassifier(getKeys, putKeys, putValues, deleteKeys);
      single.reset();
      single.offer(request);
      executePuts(single);
      executeDeletes(single);
      executeGets(single);
      executeIters(single);
      AppendMergeBatchBuffer amBuf = single.appendMergeBuffer();
      if (amBuf != null && !amBuf.isEmpty()) {
          dispatchAppendMerge(amBuf);                 // completes AmReq internal future only
          // ← MISSING: per-row propagation to single.appendMergeRequests()[i].getFuture()
      }
      IterPrefixBatchBuffer ipBuf = single.iterPrefixBuffer();
      if (ipBuf != null && !ipBuf.isEmpty()) {
          dispatchIterPrefix(ipBuf);                  // completes IterPrefix internal future only
      }
  }
  ```
- **Failure mode:** The Round 2 fix to `executeBatchRequests` (lines 188-211)
  adds a per-row propagation loop that walks `amReqFutures` and either
  completes the parallel `StateRequest`'s framework future via `completePut`
  or `completePutExceptionally`. The sibling sync entry point
  `executeRequestSync`, used by `StateRequestHandler` for the V2 sync
  fallback path, was **not** updated. After `dispatchAppendMerge` completes
  the internal `AppendMergeRequest.future`, the parallel `StateRequest`'s
  `getFuture()` is never touched. The Flink async-state runtime waits for
  that framework future forever — the operator stalls on the next ListState
  request that takes the sync path. **This is the exact A1-H5 anti-pattern
  re-introduced** by the post-fix asymmetry between the two entry points,
  and is silent for both success (no `completePut`) and failure (no
  `completePutExceptionally`) cases. Same gap exists for ITER_PREFIX
  (line 240-242) since the original code never had per-row propagation there
  either — but Round 1 didn't flag it because the sync path was out of
  scope; with the new fix making executeBatchRequests' propagation explicit,
  the gap becomes a clear regression risk if any V2 caller falls back to
  sync (it does, see `executeRequestSync` callers in StateRequestHandler).
- **Repro scenario:** A V2 ListState.asyncAdd on a code path that takes the
  sync executor (e.g. blocking checkpoint barrier handoff, or a unit/perf
  test that calls `executor.executeRequestSync(req)`). The dispatch
  succeeds, the internal AmReq future resolves, but `req.getFuture()` stays
  pending → caller's `thenAccept` never fires → reference-count drain
  never completes → operator hangs.
- **Fix sketch:** Extract the new propagation loop from
  `executeBatchRequests` (lines 192-211) into a helper
  `propagateAmCompletions(amBuf, amReqs, amCount)` and call it from both
  `executeBatchRequests` AND `executeRequestSync`. Also add the equivalent
  propagation for ITER_PREFIX/ITER_RANGE in sync (their futures are typed
  `IterFirstChunk` so the helper differs but the pattern is identical).

---

### A2-H2 — `completePutExceptionally` swallows operand-typed exceptions, may break per-row diagnostics
- **File:line:** `flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java:826-836` + propagation site 192-211
- **Code snippet:**
  ```java
  for (int i = 0; i < amCount; i++) {
      CompletableFuture<Void> amFut = amReqFutures.get(i);
      if (amFut.isCompletedExceptionally()) {
          Throwable cause;
          try {
              amFut.getNow(null);                                       // (a)
              cause = new RuntimeException("...cause unavailable");
          } catch (Throwable t) {
              cause = t.getCause() != null ? t.getCause() : t;          // (b)
          }
          completePutExceptionally(amReqs[i], cause);
      } else { completePut(amReqs[i]); }
  }

  private static void completePutExceptionally(StateRequest<?,?,?,?> req, Throwable cause) {
      ((InternalAsyncFuture<Object>) req.getFuture())
              .completeExceptionally("ForSt-RS APPEND_MERGE dispatch failed", cause);  // (c)
  }
  ```
- **Failure mode:** Three issues stack:
  1. `dispatchAppendMergeBatch` (line 619-620) completes EVERY future in the
     batch with the **same** Throwable instance (`err`). When the propagation
     loop (line 195-211) walks `amReqFutures`, every per-row `getNow(null)`
     unwraps the SAME shared exception, but `completePutExceptionally`
     stamps it with a generic `"ForSt-RS APPEND_MERGE dispatch failed"`
     message. The original `FrsErrorCode` (engineering's diagnostic anchor)
     and the **row index** (-1 sentinel on the batched path,
     `FrsException(code, -1, new byte[0])`) are buried under the
     generic message. Flink's task-failure log will show
     `"ForSt-RS APPEND_MERGE dispatch failed"` with N-stack-deep
     `caused by: FrsException ... rowIndex=-1` — diagnosing which row /
     which state failed becomes impossible.
  2. Branch (b) does `t.getCause()`; for `FrsException` thrown via
     `future.completeExceptionally(new FrsException(...))`, `getNow(null)`
     wraps it in `CompletionException`, so `t.getCause()` yields the
     `FrsException` — correct. But for `FrsEnginePanicError extends Error`
     (line 30 of FrsEnginePanicError.java), Flink's `InternalAsyncFuture`
     contract treats `Throwable` causes but the rest of the runtime
     (StreamTask asyncOperationFailureHandler) checks
     `instanceof Exception` in many paths — an `Error` cause may bypass
     the configured `FatalErrorHandler` and instead bubble up
     incorrectly.
  3. The `FatalErrorHandler.onFatalError` was already invoked on line 612-614
     of `dispatchAppendMergeBatch` for the panic path. Re-propagating the
     same panic via per-row `completePutExceptionally` then causes Flink to
     ALSO fail the operator with the same panic — fatal-handler escalation
     runs twice (once already routed to the JVM uncaught handler, once via
     the framework future). With a `FatalErrorHandler` that calls
     `System.exit`, the second escalation is a no-op; with one that
     records-and-recovers, double-counting happens.
- **Repro scenario:** Engine returns `EngineIo` for batched APPEND_MERGE.
  `err = new FrsException(code, -1, new byte[0])`. The propagation loop
  completes 1000 StateRequest futures each with the generic message. Log
  contains 1000 identical "ForSt-RS APPEND_MERGE dispatch failed" with
  rowIndex=-1; the user cannot tell which ListState or which key was
  responsible.
- **Fix sketch:** (1) Build the wrapper exception lazily and include
  `FrsErrorCode code` + `stateName` + `int rowIdx` in the message — use
  `FrsException`'s existing format directly (no wrapper). Better: do not
  add a wrapper layer at all — call
  `((InternalAsyncFuture<Object>) req.getFuture()).completeExceptionally("", cause)`
  with an empty message, since the cause already encodes everything.
  (2) For the panic path, **skip** the per-row propagation when
  `FrsEnginePanicError` was already escalated to FatalErrorHandler —
  re-throw from `executeBatchRequests` so the framework sees the panic
  once at the container level (current code does the opposite: catches
  panic, completes all per-row futures normally with the panic cause,
  then returns success on the container future at line 219, hiding the
  panic from the framework's container-level error path).

---

### A2-H3 — Container future returns SUCCESS even when per-row APPEND_MERGE futures failed
- **File:line:** `flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java:185-222`
- **Code snippet:**
  ```java
  AppendMergeBatchBuffer amBuf = classifier.appendMergeBuffer();
  if (amBuf != null && !amBuf.isEmpty()) {
      dispatchAppendMerge(amBuf);             // may complete every future exceptionally
      ...                                       // per-row propagation loop
  }
  IterPrefixBatchBuffer ipBuf = classifier.iterPrefixBuffer();
  if (ipBuf != null && !ipBuf.isEmpty()) {
      dispatchIterPrefix(ipBuf);
  }
  return CompletableFuture.completedFuture(null);     // ← unconditional success
  ```
- **Failure mode:** `executeBatchRequests` returns `CompletableFuture<Void>`
  per Flink's `StateExecutor` interface. The Flink async-state runtime uses
  the container future to decide whether to schedule the next batch and
  whether to escalate. If ALL 1000 per-row APPEND_MERGE futures are
  exception-completed (engine outage), the container future is STILL
  returned `completedFuture(null)` — i.e., "batch succeeded". The runtime
  will release the batch's RecordContext refs and immediately schedule the
  next batch even though every record's state op just failed. Net effect:
  per-row failures get reported only via the individual record's stream of
  failures, but the batch-level scheduler keeps slamming the engine with
  the next batch, multiplying the failure count.
- **Repro scenario:** Engine returns `EngineCorrupted` on batched
  APPEND_MERGE → all 1000 row futures completed exceptionally → container
  returns success → runtime calls executor.executeBatchRequests again with
  the next 1000 rows → same failure → log spam + delayed task abort.
- **Fix sketch:** Track an "any-row-failed" flag in the propagation loop.
  If set, return `failedFuture(firstCause)` instead of
  `completedFuture(null)`. This mirrors how `executePuts` propagates via
  the catch (line 220) — the new code path needs the same back-pressure.

---

## MEDIUM severity

### A2-M1 — C-H4 Rust fix iterates `HashMap` for write ordering — non-deterministic put order across keys
- **File:line:** `crates/forst-rs-ffi/src/lib.rs:4022-4055`
- **Failure mode:** The fix uses `std::collections::HashMap<&[u8], Vec<&[u8]>>`
  with insertion ordering NOT preserved by Rust's default hasher
  (DefaultHasher / SipHash + randomized seed). The loop
  `for (key, ops) in grouped.iter()` then issues `db_ref.get` →
  `db_ref.put` per key in a hash-randomized order. Operand order WITHIN a
  key IS preserved (via `Vec::push`) — so per-key list semantics are
  correct. But across keys, the put order is randomized. For a single
  engine instance this doesn't affect correctness (each key's row is
  independent). HOWEVER, if a panic / Err return halts the loop midway
  (line 4045 or 4053 returns early), the set of "committed" keys is a
  non-deterministic subset — testing/repro/diff-debugging becomes harder
  because two runs with identical inputs commit different prefixes on
  failure. The pre-fix `Vec<Vec<u8>>` also had this hash-order, so the
  fix didn't introduce it — but it remains a known partial-failure
  non-determinism that should be a follow-up to A1-M3 (Round 1).
- **Fix sketch:** Use `IndexMap` (preserves insertion order) or sort the
  keys at iteration time. Or — preferred — accumulate all merged
  `(key, merged_value)` pairs first, then `db.batch_write(wb)` atomically
  (A1-M3 fix), which makes the partial-failure question moot.

### A2-M2 — Propagation loop's `isCompletedExceptionally` + `getNow(null)` is a per-row exception-throw on the hot path
- **File:line:** `flink-statebackend-forst-rs/.../VectorizedExecutor.java:195-207`
- **Failure mode:** Not a correctness bug, but: `getNow(null)` on an
  exceptionally-completed `CompletableFuture` THROWS `CompletionException`
  per JDK spec. The fix wraps in try/catch and uses the throw to extract
  the cause. For a batch of 4096 rows where 4096 futures are
  exception-completed (engine outage), this is 4096 exception
  construct-and-throws on the propagation path. JDK throw machinery is
  expensive in hot loops. Combined with A2-H3, in the all-failed case the
  cost is ~4096 × ~µs/throw = several ms of pure exception unwinding —
  ironic given the batch was supposed to fail fast. Use
  `CompletableFuture.exceptionally(t -> { /* capture */; throw t; })`
  pre-binding, or `f.handle((v, t) -> t)` to extract the cause without
  raising.
- **Fix sketch:** When dispatching, retain a parallel `Throwable[] errors`
  array and set per-row; the propagation loop reads from the array, no
  reflection-via-throw.

### A2-M3 — D-H1 default flip (G1 over ZGC+COH) is shipped but config-default-validator missing
- **File:line:** Verified existence of the flip (config-forst-rs.yaml.tpl
  templates default to G1). 
- **Failure mode:** No CI assertion that catches a re-flip. Round 1's D-H1
  was traced to a copy-paste of a ZGC+COH template that benchmarked well
  on Q11 but regressed Q12 by 30%. The next person to touch the template
  can re-introduce ZGC+COH without breaking tests. Add a smoke test in
  `flink-state-backends/flink-statebackend-forst-rs/src/test` that loads
  the shipped template and asserts the JVM flags do not contain
  `+UseZGC` / `+UseCompactObjectHeaders` unless an explicit override file
  is named.
- **Fix sketch:** Template-default lint as part of CI / pre-merge.

---

## LOW severity

### A2-L1 — `dispatchAppendMergeBatch` re-uses a single exception across all rows; `FrsException` rowIndex stays -1
- **File:line:** `flink-statebackend-forst-rs/.../VectorizedExecutor.java:608-621`
- **Failure mode:** On batch failure, every per-row future is completed with
  `new FrsException(code, -1, new byte[0])` — same instance for all rows.
  Diagnostics lose per-row resolution (which was already coarse). Either
  (a) construct per-row, or (b) include `int batchSize` in the detail bytes.

### A2-L2 — `dispatchAppendMergeBatch` runs `recordDispatch` AFTER the error path completes all futures
- **File:line:** `flink-statebackend-forst-rs/.../VectorizedExecutor.java:624-631`
- **Failure mode:** The `metrics.recordDispatch` for APPEND_MERGE on the
  failure path still records `count` rows and the full `bytesIn`, as if
  the batch had succeeded. Dispatch metrics now over-count successful
  rows when the batch failed. Move the `recordDispatch` call inside the
  OK branch, or pass an `errored=true` flag and let the metric route to
  a separate counter (already exists via `metrics.recordFfiError`, but
  the dispatch counter doesn't subtract).

---

## Verification of Round 1 fixes

| Round 1 finding | Round 2 verdict |
|-----------------|-----------------|
| A1-H5 | **PARTIAL FIX** — `executeBatchRequests` path covered; `executeRequestSync` not (A2-H1). Container future still misreports success on per-row failure (A2-H3). Exception wrapping degrades diagnostics (A2-H2). |
| C-H4 | **CORRECT for combiner semantics** — `combine_slices` preserves arrival order within a key. HashMap ordering across keys is non-deterministic but doesn't affect list semantics. A2-M1 follow-up only. |
| D-H1 | **CORRECT** — template default flipped. A2-M3 suggests a CI guard so it stays flipped. |

## Round-2 verdict
**Correctness posture: HIGH severity remains.** The A1-H5 fix landed but
left an analogous gap in the sync executor path (A2-H1), a diagnostics
regression (A2-H2), and a container-future-reports-success-on-row-failure
gap that interacts badly with the runtime's batch scheduler (A2-H3). All
three are mechanical fixes localised to `VectorizedExecutor.java`. The
Rust C-H4 fix is correct; D-H1 is correct. Recommend Round 3 patches
target A2-H1/A2-H2/A2-H3 together with a unit test that exercises the
sync executor APPEND_MERGE path under a faulting linker mock.
