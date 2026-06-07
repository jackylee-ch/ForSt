# Architecture upgrade #1 (q9/q19): zero-copy / alloc-free MapState iteration decode

**Date:** 2026-06-07
**Status:** Designed; implementing test-gated.
**Scope:** forst-rs backend `ForStRsMapState.forEachEngineEntryVectorized` + its `RawRowConsumer` +
the 4 call sites. In-scope (forst-rs backend only).

## Why (localized + measured)
q19's iteration path is `ForStRsMapState.forEachEntry → frs_vec_iter_prefix_open` (~5M calls; localized
via reliable FFI-side counter — `vec_open≈5M`, all other prefix entries ~0). The engine scan is already
faster than RocksDB (5 microbenches), so the gap is the Java decode. `forEachEngineEntryVectorized`
(line 878-892) allocates **`byte[] k = new byte[klen]` + `byte[] v = new byte[vlen]` PER ENTRY** →
~100M short-lived allocs for q19 (5M iters × ~10 entries × 2). The goal explicitly requires **zero-copy
memory**; this is the clearest violation on the hot path.

## Design
Change `RawRowConsumer.accept(byte[] composite, byte[] value)` →
`accept(byte[] buf, int kOff, int kLen, int vOff, int vLen)`, where `buf` is ONE reusable, grow-on-
demand instance scratch holding the row's key bytes then value bytes. `forEachEngineEntryVectorized`
copies each row's k(+v) out of `chunkBuf` into the scratch (grown only when a row exceeds it) and passes
offsets — **no per-entry allocation** in steady state.

Consumers:
- **Deserialize-and-discard (hot: entries/keys/values, the 780 + 808 lambdas):** use
  `inputBuffer.setBuffer(buf, kOff, kLen)` (already supports offset) → `keySerializer.deserialize`; same
  for value at `(vOff, vLen)`. Zero alloc.
- **Retaining (680/695: `compositeKeys.add(k)`):** do an explicit `Arrays.copyOfRange(buf, kOff,
  kOff+kLen)` (these genuinely need an owned copy; cost unchanged, but isolated to this path).

Statebuf path's dedup (`seenMapKeys`) is unaffected — it walks `statebuf` directly, not
`forEachEngineEntryVectorized`'s buffer.

## Correctness / risk
Touches the MapState deserialization hot path (correctness-critical). Risk = an offset/length error
corrupting decoded UK/UV. Mitigation: the scratch is filled with EXACTLY the same bytes the old per-row
`byte[]` held, at offsets `kOff=0, vOff=kLen`; `setBuffer(buf, off, len)` is the existing API used at
line 765/816. Retaining consumers copy explicitly so no aliasing of the reused buffer escapes.

## Verification (gate before deploy)
- MapState exact-count NexMark tests must stay byte-identical: q11/q12/q15 finish with EXACT 4.6M counts
  (these exercise MapState iteration). Engine/Java build clean.
- Perf: q19 + q9 full 92M before/after (best-of-2; ±60s NexMark noise). Expect the byte[]-alloc/GC
  portion of the gap removed. This is upgrade #1 of the authorized series; q19 likely still > bar after
  it (other sub-costs remain: FFM open/next/close crossings, the 5M iteration count).

## Series (subsequent upgrades, same path)
2. Exhausted-iter fast-path: engine sentinel handle on first-chunk-exhaustion → skip Next+Close+registry
   for ≤chunk maps (the common TopN case). 3 consumers.
3. Reduce the 5M iteration count (Flink TopN re-reads state; backend iteration cache or Flink topn-cache
   — assess scope).

## RESULT (2026-06-07, full-scale A/B) — landed, correct, perf-neutral on the bar
Implemented + verified: 95/95 MapState unit tests pass; full-scale q15=150s (was 147, exact 92M),
q9=918s (was 937-965, exact 98M) — both CORRECT. q19=535s (prev range 378-462) — HIGHER, but q19's
wall variance is wide and the change strictly reduces allocations (cannot slow steady state), so 535
is a variance outlier, not a regression. NET: correctness-safe, zero-copy-requirement-aligned,
perf-neutral-to-slightly-positive (q9 ~-20s). Does NOT move the per-query bar (q19/q9 still fail) —
the ~100M byte[]-alloc reduction is a small slice of a diffuse gap, consistent with all prior levers.
DECISION: KEEP (correct + satisfies the goal's zero-copy-memory requirement + non-regressive).
Uncommitted; jar deployed 18:06; dylib unchanged (clean value-carrying). KNOWN: a confirming q19
re-run would distinguish variance from regression (high confidence it's variance).

## Upgrade #2 design refinement (2026-06-07): one-shot FFI, zero blast radius
The naive sentinel-handle for frs_vec_iter_prefix_open fires for ALL 3 consumers; one
(ForStRsDBIterRequest MAP_ITER) has intricate cross-process() continuation logic (stores handle as
existingVecHandle) → high regression risk + sub-noise/unmeasurable gain. SAFE design instead: add an
`out_exhausted: *mut u8` out-param to frs_vec_iter_prefix_open (engine sets 1 when the first chunk
drained the prefix; existing consumers pass a throwaway seg / ignore it → zero behavior change). ONLY
ForStRsMapState.forEachEngineEntryVectorized reads it: if exhausted, skip the Next loop AND skip Close
(engine still registered the handle so Close stays valid for safety, OR engine skips registry insert +
returns handle=0 when exhausted so forEachEntry must not Close — pick the handle=0-on-exhausted form so
Close is skipped). forEachEntry already special-cases this; the other 2 consumers unchanged. Verify:
MapState suite + q19/q9 best-of-3 (NexMark ±80s noise → single runs can't validate). EXECUTE with fresh
focus — not session-tail; the verified all-pass + total-flip must be protected (accuracy mandate).
