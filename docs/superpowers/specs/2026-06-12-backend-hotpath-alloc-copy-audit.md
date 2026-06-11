# Backend Hot-Path Allocation/Copy Audit (continuous-improvement M5 sweep)

**Date:** 2026-06-12
**Status:** AUDIT — ranked violations + fix sketches. **No code changed; do
not fix from this doc without the per-item gates.**
**Scope:** the Java backend's per-record/per-event paths
(`flink-statebackend-forst-rs`, read-only reference). Engine-side allocs are
covered by the S2 spec (`2026-06-12-s2-pinned-rows-loser-tree-design.md`).
**Impact classes:** `ALLOC` = per-event heap allocation (GC pressure,
TLAB churn); `COPY` = redundant byte movement; `CPU` = scalar work a JIT
intrinsic would vectorize.

---

## 0. What is already clean (audit baseline — don't re-fix)

- MapStateV2 hot path: `serializeMapEntryKeyShared` shared-buffer pattern,
  slice-consuming cache/buffer overloads (ForStRsMapStateV2.java:355-375);
  the only async-path alloc is the documented engine-miss
  `snapshotKeyForAsyncLambda` (:555-568) — required (lambda capture must
  outlive the shared buffer).
- Classifier staging: `serializeKeyInto`/`serializeValueInto` write straight
  into `ColumnarBatchBuffer` columns (B6-H1, VectorizedClassifier.java:
  1277-1289 alloc-elimination inventory); executor MemorySegment overloads
  killed the per-row `new byte[len]` (VectorizedExecutor.java:1290 comment).
- ReducingState/AggregatingState cache-hit fold: primitive-long path, zero
  boxing (B10-H2/B11-H2, ForStRsAsyncReducingStateV2.java:121-240).
- `FrsException(..., new byte[0])` sites (VectorizedExecutor.java:1553 etc.)
  — cold error paths; fine.

---

## 1. Ranked violations

### V1 — timer index peek/poll: fresh `byte[]` composite per event  [ALLOC+COPY, HIGH]

**Where:** `timer/ForStRsKeyGroupedInternalPriorityQueue.java`
- `copyIndexKey(pos)` — `new byte[kLen]` + segment→heap copy per call
  (:715-721).
- `peek()` → `decodeElementMemo(copyIndexKey(0))` (:1143). Per the method's
  own doc, "Flink calls this on every timer register and watermark advance"
  (:1129-1132) — and the memo-HIT case (the common one: repeated peeks of
  the same head) still pays the full alloc + copy + `Arrays.equals`
  (:724-727) before discovering it didn't need the bytes.
- `poll()` (:1102-1113) pays the alloc **plus a second copy**: the fresh
  composite is immediately copied back into `scratchSeg` just to call
  `pendingBuffer.find(scratchSeg, 0, len)` — heap→native→probe for bytes
  that already live in `liveIndex.keyDataSegment()`.

**Fix sketch (zero behavior change):**
1. `peek()`: memo-compare segment-direct — keep `peekMemoKey` but test
   `len == peekMemoKey.length && MemorySegment.mismatch(liveIndex
   .keyDataSegment().asSlice(kOff, kLen), MemorySegment.ofArray(peekMemoKey))
   == -1` BEFORE allocating; allocate only on memo miss (head changed).
2. `poll()`: call `pendingBuffer.find(liveIndex.keyDataSegment(), kOff,
   kLen)` directly (the find already takes `(seg, off, len)` — :1112) —
   deletes both the scratch copy and, on the pending-ADD-cancel branch, the
   composite alloc entirely. Materialize `composite` only for
   `decodeElementMemo` miss and the `pendingPollDeletes.add` staging branch
   (those genuinely need owned bytes).

**Impact:** one alloc+copy per timer register/watermark-advance/fire across
q5/q8/q11/q12-class windowed queries (millions-to-tens-of-millions of
events @100M). Class ALLOC (TLAB churn on the operator thread) — expect
GC-pressure relief, not a headline wall-time win; the timer-invariance
finding (roadmap §1.1: timers exonerated for q17/q20) caps expectations.
**Caution:** `removeAtOrAboveTs`/index mutation invalidate offsets — the
segment-direct compare must re-read `keyOffsetAt(0)` after any mutation
(pure-read peek is safe; poll reads before `removeAt(0)` — :1102 ordering
already does this).

### V2 — `ArrowTimerBuffer.hashOf`: scratch realloc on ANY size change + copy per hash  [CPU+ALLOC, MEDIUM-HIGH]

**Where:** `timer/ArrowTimerBuffer.java:617-634`. The thread-local scratch
is replaced whenever `scratch.length != len` (:625-627) — the doc comment
above (:609-612) claims realloc happens only on "a strict size increase",
**the code reallocs on any mismatch**: a queue serving two timer states
with different key widths (or keys with varint-width drift) alternates
sizes and reallocates on EVERY find/insert. Even size-stable callers pay an
unconditional segment→heap copy per hash (:630-632).

**Fix sketch:** compute the polynomial-31 hash directly over the
`MemorySegment`: 8-byte `getLong` strides folded with the precomputed
`31^k` power table (the same trick the `Arrays.hashCode` intrinsic uses,
per the comment :595-599), scalar tail ≤7 bytes. Bitwise-identical output
is a hard requirement (open-addressed slot layout compatibility, :601-603)
— property-test new-vs-old over random lengths/contents. Kills the copy,
the scratch, and the TL lookup.

**Impact:** CPU per pending-buffer find/insert (every staged timer add /
poll-cancel probe); ALLOC only in the alternating-width case. Medium
because it sits under V1's call sites.

### V3 — `ArrowTimerBuffer.rowKeyEquals`: per-byte segment loop  [CPU, MEDIUM]

**Where:** ArrowTimerBuffer.java:636-650 — scalar `seg.get(JAVA_BYTE, …)`
compare loop, run on every hash-probe hit/collision (find path under V1/V2).
**Fix sketch:** `MemorySegment.mismatch(keyData.asSlice(kStart, kLen), seg
.asSlice(offset, len)) == -1` — the mismatch intrinsic vectorizes (SWAR/
SIMD) and is branch-cheap for the common equal case. Same shape as the
sliceBytesEqual fix (V4); keep the `kLen != len` early-out.

### V4 — `DispatchOrderingHazards.sliceBytesEqual` (+ the :352 twin): scalar byte loops on the per-batch hazard path  [CPU, MEDIUM]

**Where:** DispatchOrderingHazards.java:429-443 (`sliceBytesEqual`), :352
(in-class hash-probe verify loop). These run on EVERY classified batch with
writes (`requiresOrderedDispatch{,Mixed}` — :46, :133), per probe-hit in the
small-side hash set (:252-255, :316-319) and per delete-conflict scan
(:110-124, :188+). With the L2a mixed-batch flip these predicates move onto
the default path for every batch — fixing this BEFORE the flip keeps the
flip's measured cost honest.
**Fix sketch:** `MemorySegment.mismatch(left.asSlice(leftStart, len),
right.asSlice(rightStart, len)) == -1`; for `sliceStartsWith` slice the
prefix length first. Pure CPU class; zero semantic change; existing hazard
UTs are the gate.

### V5 — iter drain: per-row `IteratorEntryView` allocation  [ALLOC, LOW-MEDIUM]

**Where:** ForStRsDBIterRequest.java:712-744 (`decodeChunkDirect`) — one
`new IteratorEntryView(chunkBuf, …)` per row (:730), per chunk, on the
MAP_ITER drain (q7/q11/q19/q20 iterator-heavy paths). The view is consumed
synchronously by `deserializeUserKey/deserializeUserValue` and never
escapes the loop iteration — escape analysis MAY scalar-replace it, but
the call is virtual through `iterableState` (megamorphic across states), so
don't bet on it.
**Fix sketch:** one mutable view instance per drain loop, `view.reset(keyOff,
klen, valOff, vlen)` per row. **Precondition (verify before fixing):** no
deserializer retains the view (audit `deserializeUserKey/Value` impls — the
zero-copy `VIEW_TL` pattern referenced at :466 suggests this contract
already exists). The `SimpleEntry` + UK/UV materializations are
contract-required (the returned iterator owns detached objects — the
join-OOM lesson) and stay.

### V6 — ListStateV2 `serializeKey`/`serializeValue` `getCopyOfBuffer()`  [ALLOC, LOW — verify caller first]

**Where:** ForStRsAsyncListStateV2.java:398, :455. Same legacy shape
MapStateV2 already migrated away from (:340-365 contrast). If LIST_ADD /
LIST_GET dispatch flows through the classifier's `serializeKeyInto` columns
(B6-H1 made that the hot path for appends — VectorizedClassifier.java:
1277-1289), these are cold framework fallbacks and NOT worth touching.
**Action:** call-graph check first; fix only if hot (then: shared-buffer +
`Into` overloads, the MapStateV2 pattern).

### V7 — housekeeping  [INFO]

- `ColumnarBatchBuffer.copyAt` (:210-220) has no non-test callers in main —
  candidate for deletion (dead per-row-alloc API; removing it prevents
  reintroduction).
- ArrowTimerBuffer doc drift: fix the :609-612 comment when V2 lands (it
  currently documents behavior the code doesn't have).

---

## 2. Priority order and expected return

| Rank | Item | Class | Why this order |
|---|---|---|---|
| 1 | V4 hazards mismatch | CPU | trivially safe, gates already exist, and it pre-cleans the L2a mixed-flip measurement path |
| 2 | V1 timer peek/poll | ALLOC+COPY | highest per-event frequency; pairs naturally with V2/V3 (same files, one PR, one canary run) |
| 3 | V2+V3 hash/equals | CPU(+ALLOC) | same PR as V1; property-test for hash identity |
| 4 | V5 iter view reuse | ALLOC | needs the no-retention audit; rides the iter-heavy queries |
| 5 | V6 list serializers | ALLOC | only after call-graph confirms hot |

Honest ceiling: these are constant-factor hygiene items, not levers — the
timer-invariance result (roadmap §1.1) says the timer path is not q9/q20's
gap. Expected: JFR alloc-rate drop on q12/q5 (measurable), wall-time within
box noise individually; their value is keeping the per-record paths
violation-free per the standing mandate, and de-noising future profiles.

## 3. Gates (per PR, standard law)

1. Backend suite green (114/0 baseline) + new property tests (hash identity
   V2; mismatch-equivalence V3/V4 over random slices incl. len 0/1/7/8/9).
2. JFR/`-XX:+PrintGCDetails`-class alloc-rate A/B on q12@10M (the
   timer-dense canary) for V1-V3 — alloc/sec must drop; wall within noise.
3. Lockstep exactness ×2 for anything touching poll/peek ordering (V1) —
   the Stage-0 timer-queue regression rule applies to ANY timer-path edit.
4. q5 windowed-value byte-exactness (pane counts mask value bugs).

---

## Appendix A — Adversarial review of commits landed on forst-rs since 277158765
(fresh-eyes pass, 2026-06-12; commits 7ad678d25, f164c35fd, dcc465b81,
43a473315 / merge b1ea944de)

**A1. `43a473315` garbage-drain DEFAULT ON @200K (roadmap L1) — rewrite
FAITHFUL, measurement gate OUTSTANDING.** Verified against the pre-image:
the old `garbage_drain_due()` was exactly `threshold > 0 && pressure >=
threshold` (the flush-ratio gate was already removed by GATE-V2 — pre-image
db.rs:217-233), so the new pure `garbage_drain_gate` 5-condition form drops
NOTHING: conditions 1-2 = old `garbage_drain_due`, 3 = old `!backoff`,
4-5 = old L1-floor closure. The two-step call passes the SAME captured
locals to both gate invocations (no racy re-read), and the boundary UT
covers every edge. **Finding (process, not code):** the roadmap's L1 gate
("q17 @100M no-regress ×3, q3/q8 no-regress, exact-rows on q9/q20") has not
been run for the flip itself; the commit relies on the 90d9fcbdf
attribution-reversal for the q17 exoneration. The flip is reasonable, but
the @100M validation set is now the FIRST thing the box must run — before
any further default changes stack on top (attribution discipline).

**A2. `7ad678d25` H1 catch_unwind + join timeouts — correct; two residuals.**
`POOL_JOIN_TIMEOUT = 300 s` is generous (a wedged probe at 300 s is a dead
query, not a false positive). Residual (i): on Timeout, a late-completing
`batch_open_prefix_iters_parallel` job's built `IterHandle` is dropped via
the failed channel send — engine-side resource release rides the handle's
Drop; recommend one leak-watchdog UT for the timeout path specifically
(panic path is tested, timeout path is not). Residual (ii): per-slot
timeout errors must surface to Java as query failure (deferred-error
machinery), never as empty probe results — covered by existing R15-M3/
R17-M1 plumbing for the iter path, worth one assertion in the Java suite.

**A3. `f164c35fd` M2 remote ramp ≥2 — matches the roadmap blocker exactly;
the test was updated honestly (asserts cold after block 1, ramp after
block 2, deep cap retained). No issues.

**A4. `dcc465b81` M3 telemetry — observation only, no enforcement. The
compaction-windowed spec (`2026-06-12-compaction-windowed-readpath-design
.md` §2.3) should share ONE budget-constant home with this when its
`FRS_COMPACT_PREFETCH_BUDGET` lands.

**A5. Cross-spec impact on this session's docs:** the compaction spec's
gate G2 (H1 catch_unwind prerequisite) is ALREADY SATISFIED by 7ad678d25.
File:line citations in all four 2026-06-12 specs reference base 277158765;
db.rs sites shifted by ~+60 lines after the merge — re-anchor during
implementation, the cited code is unchanged.
