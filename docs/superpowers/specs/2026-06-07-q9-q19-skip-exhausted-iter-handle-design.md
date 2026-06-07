# q9/q19 lever: skip iter-handle alloc/register for first-chunk-exhausted batch prefix opens

**Date:** 2026-06-07
**Status:** Designed (pinpointed lever). Implementation is the concrete next step for the per-query bar.
**Scope:** forst-rs engine FFI (`frs_vec_iter_prefix_open_batch`) + Java backend (`VectorizedExecutor.dispatchIterPrefix`, `FrsIterHandle`, `IterPrefixRequest.IterFirstChunk`, the MapState iterator consumer). In-scope (forst-rs only).

## Evidence (why this is THE q9/q19 lever)

- 3-backend NexMark: forst-rs LOSES q9 (965 vs rocksdb 534 / ForSt-Java 684) and q19 (378 vs 147 / 145) to **both** C++ engines.
- Five engine microbenches prove the forst-rs **engine** is parity-or-faster (point 3.5×, static range 1.07×, **churn 1.20×**). So the gap is the **Java backend iteration layer**, not the engine.
- q19 issues ~**6M** `MapState.forEachEntry` prefix-scans (engine `build_lazy_prefix_key_stream` count; `get_internal=0`), routed through `VectorizedExecutor.dispatchIterPrefix` → `frs_vec_iter_prefix_open_batch`.
- The TopN buffer is ≤10 entries, so the first 64KB chunk **always** drains the whole prefix. The engine **already detects this**: `fill_chunk_from_iter` returns `iter_exhausted=true` and the FFI calls `handle_state.drop_inner()` (FFI lib.rs:5493-5500). **Yet it still returns a non-zero handle**, and `dispatchIterPrefix` (VectorizedExecutor.java:2140-2157) then does, per row: `nextIterHandleId.incrementAndGet()` + `new FrsIterHandle(...)` + `slotScope.registerIter(fh)` + `new IterPrefixRequest.IterFirstChunk(fh, rows)` — and the consumer later closes the handle. **For ~6M exhausted iters that is ~6M × (handle alloc + register + close) of pure overhead** that the C++ engines' iterators don't pay.

## Design

Signal first-chunk-exhaustion across the FFI and skip the handle when set.

1. **FFI `frs_vec_iter_prefix_open_batch`:** when `iter_exhausted` for row `i`, after `drop_inner()`, write a sentinel `out_handles[i] = u64::MAX` (`FRS_ITER_EXHAUSTED_SENTINEL`) and DO NOT register/keep a live handle (the inner is already dropped; nothing to read). `0` stays = error; any other value = a real, registered handle. ABI-compatible (reuses the existing `out_handles` out-param; no `FrsChunk` struct change). Mirror in the single-shot `frs_vec_iter_prefix_open` if it has the same shape.
2. **`VectorizedExecutor.dispatchIterPrefix`:** branch on the handle value —
   - `0` → error (unchanged).
   - `FRS_ITER_EXHAUSTED_SENTINEL` → **do NOT** allocate `FrsIterHandle`, **do NOT** `registerIter`; complete the future with an `IterFirstChunk` carrying the first chunk + `exhausted=true` + a null handle.
   - else → real handle (unchanged).
3. **`IterPrefixRequest.IterFirstChunk` + consumer (MapState iterator):** add an `exhausted` flag (or null-handle contract). When exhausted, the consumer reads the first chunk and MUST NOT call `next()` or `close()` on a handle (there is none). Verify the existing consumer already stops at `firstChunkRows` when the chunk wasn't full — if so, the only change is tolerating a null handle.

## Correctness / risk

- The change is atomic across FFI+Java: the engine sentinel and the Java branch MUST land together (a SENTINEL handle handed to the old Java path would be treated as a live handle → UB on next()/close). Do not deploy half.
- Iterator lifetime: skipping `registerIter` is safe ONLY because the inner state is already dropped and no native handle is live for the exhausted row — there is nothing for the slot-scope turn-boundary cleanup to close. Verify `slotScope` has no other invariant that requires every opened row to be registered.
- Non-exhausted rows (first chunk full, more to read) are completely unchanged.

## Verification plan

- Engine: unit test that a tiny-prefix batch open returns `FRS_ITER_EXHAUSTED_SENTINEL` and a large-prefix one returns a real handle + correct multi-chunk drain.
- Java: MapState iteration unit tests (q11/q12/q15-style exact-count) stay green; add a test that an exhausted iter completes with no FrsIterHandle and yields all first-chunk rows.
- Perf: q19 + q9 full 92M before/after (best-of-2; NexMark ±60s). Target: close part of the ~230s q19 gap vs ForSt-Java. Also re-confirm q11/q12/q15 (same MapState iter path) don't regress.

## Why not done in-session

Correctness-sensitive ABI + iterator-lifetime change; rushing it at the tail of a very long session risks the verified wins (total flip + all-pass). Implement as a focused, fully-verified step.

## CORRECTION 2026-06-07 (arithmetic): this is NOT the q9/q19 lever — DO NOT implement as a "fix"
Sanity-check the magnitude before implementing: handle alloc + slotScope.registerIter + native close()
≈ ~300ns/iter-open. At ~6M opens that is ~2s; even at ~92M opens ~28s — vs q19's 233s gap to ForSt-Java
and the ~194s needed to reach 0.8×. So eliminating the exhausted-iter handle churn resolves ≤~10% of
the gap, NOT the gap. Per the goal's "each refactor must completely resolve its problem in one step,"
this does NOT qualify. CONCLUSION (now N-th confirmation): q9/q19 (and q4/q20/q11) are genuinely
DIFFUSE — every candidate lever this session (timer-CF, REFILL_BATCH, ordered-dispatch, handle-churn)
is variance / worse / refuted / <~10%. The per-query 1.x bar cannot be met by any single compliant
change; it is an irreducible multi-session sum, exactly as the q4 rigorous best-of-3 concluded. The
OVERALL-total goal IS met (forst-rs 3964 < rocksdb 4117, fastest of 3 backends) and the engine is
best-in-class (5 microbenches). Keep this spec as a documented NON-lever (prevents re-chasing).
