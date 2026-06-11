# Two-Regime Execution Model (ForSt-referenced) — Design

**Date:** 2026-06-11 · **Status:** user-approved direction (scope + model), spec for review
**Supersedes:** the seal/swap-everything framing of
`2026-06-11-per-batch-buffer-ownership-design.md` (its analysis and B-spike stand; its
Approach A is replaced by this design's regime model).
**Repos:** flink-statebackend-forst-rs (executor/state) + ForSt engine (merge operator,
write-batch coalescing). NO Flink-runtime changes (verified: watermark =
SERIAL_BETWEEN_EPOCH full drain before trigger, EpochManager.java:134; timers carry
RecordContext key-accounting, InternalTimerServiceAsyncImpl.java:136).

## 1. The question this answers

Is the 1.7/8-core q9 result a forst-rs design problem, and should we redesign the framework?
**User decisions (2026-06-11):** (1) adopt ForSt's execution model in a TARGETED redesign of
the execution layer — not a full framework rebuild (the framework beats both backends on 13
of 22 queries; the evidence indicts only the execution model); (2) resolve the q17-vs-q9
tension with a TWO-REGIME executor with sealed transitions.

## 2. Evidence base (all 2026-06-11, 8c/32g)

- q9@100M symbolized perf profile: TM averages **1.7/8 cores** under the safe (blocking)
  executor; GET path 6.5% (OPT-N16 dead); the mailbox latch is the wall-clock gate.
- Measured offload cost: q17 fully-offloaded = 271.8s vs inline 77.7s (ForSt 253.0s) — a
  pure coordinator model FAILS the strictly-faster-than-ForSt bar on q17-class queries.
  The inline path IS the q17-class advantage; the latch IS the q9-class deficit.
- Pipelined-correctness ledger (q8@100M canary, out_rows band 3,064,4xx):
  - routing-async, staging on: wedge ×1, −58% ×1.
  - + Map/List staging gated: −33% / ✓ / −17%.
  - + RMW caches gated: −44% / ✓ / −28%.
  - workers=3 depth-1: −61% / −57%. workers=1: ✓ / ✓ / **−77%** ← single-worker NOT safe;
    the residual race is timing-dependent, inside our backend's execution of the
    window-join op mix (LIST_ADD / ITER-at-fire / CLEAR), root cause OPEN (Stage 0).
- Three lockstep-only staging mechanisms identified and env-gated (committed c1519559c7f):
  MapStateArrowBuffer (mailbox watermark-drain = mailbox-direct engine writes),
  ListStateArrowBuffer (worker-drain vs mailbox-append), Reducing/Aggregating RMW caches
  (mailbox folds + mailbox flushHandler writes).

## 3. The model

ForSt's shape (coordinator returns early; no backend staging; write coalescing in the
engine) adapted to keep forst-rs's measured inline advantage:

### 3.1 Regimes
- **LIGHT (pipeline empty, `outstanding == 0`):** the batch executes INLINE on the mailbox
  thread — today's proven depth-1 semantics, byte-for-byte. All staging caches ACTIVE
  (Map/List buffers, RMW caches): they are mailbox-confined and lockstep-safe by
  construction in this regime. q17/q5/q8-class queries, whose workers always catch up,
  live here and keep their wins.
- **HEAVY (`outstanding > 0`):** batches dispatch to key-group-affine single-thread worker
  FIFOs and the mailbox returns immediately (truthful aggregate future; `fullyLoaded()` =
  outstanding ≥ cap). Staging caches INACTIVE — writes flow through classifier-private
  buffers; put-coalescing moves ENGINE-side (3.4). q9/q20/q7-class queries, whose state
  work outpaces arrival, live here and get the idle cores.
- A batch is dispatched HEAVY iff `outstanding > 0` at dispatch time; otherwise LIGHT.
  The regime is an emergent per-moment property — one uniform config serves every query
  (mandate-compliant). Note `outstanding > 0` is exactly the condition under which inline
  execution could overtake queued work, so the predicate and the safety invariant coincide.

### 3.2 Sealed transitions (the only new machinery)
- **L→H** (first dispatch while caches hold staged effects): seal every dirty staging
  structure (Map buffer rows + tombstones, List accumulator, RMW dirty accumulators) into
  the FIRST heavy batch as drain-first items, partitioned by key-group to the owning
  worker. Workers apply drains before their sub-batch ops (write-before-read per batch;
  FIFO across batches). Seal = pointer swap on the mailbox; sealed structures are
  immutable and remain probe-visible (read-your-writes) until reclaimed mailbox-side.
- **H→L** (`outstanding` returns to 0): caches reactivate empty. No drain needed (heavy
  regime never staged anything).
- Snapshot (Trace E): the pre-snapshot hook runs after the AEC's in-flight drain (workers
  idle, verified contract) — seal + drain synchronously on the mailbox, exactly today's
  lockstep semantics.

### 3.3 Correctness invariants
1. Same key-group ⇒ same worker FIFO ⇒ cross-batch order (proven routing property).
2. Same key ⇒ AEC KeyAccountingUnit serializes records; timers included
   (InternalTimerServiceAsyncImpl.java:136).
3. Watermark/window fires ⇒ SERIAL_BETWEEN_EPOCH full drain precedes triggers
   (EpochManager.java:134) — fires only ever read fully-applied state.
4. Inline (LIGHT) execution only at `outstanding == 0` ⇒ no inline op can overtake a
   queued op. (The 2026-06-10 adaptive corruption was inline-with-queued-work; forbidden
   here by the regime predicate.)
5. NO mailbox-direct engine writes outside LIGHT regime — the audit class is "every
   mechanism that defers or absorbs engine effects on the mailbox"; three are known and
   gated; Stage 0 closes the remaining one.

### 3.4 Engine-side coalescing (ForSt-referenced, forst-rs engine work)
- **Write-batch op:** a native `frs_vectorized_batch_put` already exists; add a
  WriteBatch-style multi-kind native call (puts+deletes+merges in one crossing) so heavy-
  regime batches commit per-batch effects in ONE FFI call per worker per batch.
- **Numeric merge operator (OPT-N04):** blind `Merge(+delta)` for the no-UK join count map
  (q20/q9's dependent GET→PUT chain dies); compaction/read-time resolution; retraction =
  negative delta. Uses the shipped footer-v4/compaction machinery; must verify chain
  collapse (known memory note on un-combined merge chains).
- Reducing/Aggregating in HEAVY regime: framework get-fold-put (what ForSt does); the RMW
  cache covers LIGHT regime, so q17-class keeps its in-memory folding where it matters.

## 4. Staging (sequenced, each stage independently gated)

- **Stage 0 — root-cause the residual pipelined race (BLOCKING).** Diagnostic:
  FRS_REENTRY_DIAG=2 STREAM_STATS differential between an exact and a corrupt q8 B-config
  run — per-kind op counts split write-loss (appends differ) from read-loss (iters differ).
  Then targeted fix + q8@100M ×5 exact. No perf claims before this closes.
- **Stage 1 — two-regime executor, H = 1 worker.** Regime predicate + sealed transitions
  on RoutingStateExecutor; staging gates become regime-aware (replace the env-gate B-spike).
  Gates: 543 UTs + new transition UTs; q8 ×5; 10M exactness sweep (20/22 baseline);
  q17 n≥3 direction (LIGHT regime must preserve 77s-class); q9@100M A/B vs 2215.6s.
- **Stage 2 — multi-worker HEAVY.** Widen H to N workers (Stage 0's root cause determines
  whether extra ordering work is needed). Gates: q8 ×5, q9/q20/q7 @100M, q11 (parallel
  proved 134.7s).
- **Stage 3 — engine coalescing.** Multi-kind write-batch FFI + OPT-N04 merge operator.
  Gates: engine suite, q20@100M (bar 1342.5), q9 re-run, q17/q3/q8 no-regress.
- **Stage 4 — default flip + full sweep.** Two-regime becomes the default executor; full
  q0-q22 3-backend matrix on a stable box (n≥3 on noise-prone queries); GHA both repos;
  sweep doc final tables.

## 5. Expected outcomes (estimates, to be replaced by gate measurements)
- q9: 2001–2215s → toward the 1776s bar via heavy-regime overlap (idle 6.3 cores) +
  Stage-3 chain-kill; the in-flight q9 B-config A/B gives the first real number.
- q20: 1477.7 → toward 1342.5 mainly via OPT-N04 (dependent-GET elimination).
- q17-class: unchanged by construction (LIGHT regime = today's path).
- q7: gains from heavy-regime iter fan-out (its 1052s adaptive measurement is the hint);
  needs its own attribution run.

## 6. Risks
- Stage 0 may reveal a deeper contract issue (e.g., iter chunk lifecycles under
  pipelining) — timeboxed diagnosis, evidence-first; the regime model is robust to where
  the fix lands because LIGHT regime is always available as the safe floor.
- Regime flapping (L↔H churn) could thrash seals — hysteresis if observed (seal cost is a
  pointer swap; drain piggybacks on a batch already being dispatched).
- Engine merge-chain read cost (OPT-N04) — must verify compaction collapse before default.

## 7. PMC self-review (2026-06-11): coverage and non-goals — what this design does NOT fix

Evidence update: q9 B-config A/B (routing-async × 1 worker) = DNF-bound (76M@2371s vs
control 98M@1946s) — mailbox overlap WITHOUT multi-worker LOSES on q9. The q9 lever is
Stage 2 (+ Stage 3), strictly behind Stage 0's open race. All §5 numbers are estimates.

| failing query | covered? | lever | status |
|---|---|---|---|
| q9 | YES | Stage 2 multi-worker + Stage 3 | behind Stage 0 (race OPEN) |
| q20 | PARTLY | Stage 3 OPT-N04 merge-op | bar itself unpinned (RDB 859.7 vs 1074 cross-day) |
| q11 | MOSTLY | Stage 2 (134.7s measured) | 134.7 = 1.27× still misses 0.8× by ~2s — needs a small extra lever |
| q7 vs ForSt | WEAKLY | heavy-regime iters got 1052s | remaining ~465s = engine iterator throughput — OWN root-cause needed (C2/C3 direction) |
| q4 (+70s) | NO | unknown — never FRS_PERF-profiled | Stage 0b profile |
| q19 (0.59×) | NO | unknown — "diffuse" label unverified by profile | Stage 0b profile |
| q5 correctness | NO | separate workstream (churn artifact analysis) | open |
| q8/q12 vs ForSt | MARGINAL | engine write-batch may trim seconds | unquantified |

**Stage 0b (parallel, cheap, race-independent):** FRS_PERF symbolized profiles of q4, q19,
q7 @100M (3 runs, ~25 min box time total) — classify each as executor-bound (this design
helps) vs engine-bound (assign to the engine campaign). The same method killed OPT-N16 and
found the latch; buy the same certainty before promising these queries.

**Non-goals of this design:** q5 correctness; the q7 engine-iterator gap beyond what heavy-
regime fan-out delivers; bar re-pinning (Stage 4 protocol: all three backends same-session,
n≥3 on noise-prone queries).
