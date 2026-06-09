# ForStRS Join Performance — Round-2 New-Findings Investigation (DESIGN / PLAN)

> **Status:** design approved 2026-06-09 · this is the *plan*, not the deliverable.
> **Deliverable:** a single new document `2026-06-09-forstrs-join-perf-round2-newfindings.md`
> produced by the workflow described here.
> **Baseline:** `2026-06-09-forstrs-join-performance.md` ("v1"). v1 already owns the dominant
> root cause (async-state depth-1 inline FFM dispatch) + 12 OPTs. Round-2 must be **additive**, not
> a re-run.

## 1. Goal

Produce a **new** single-document spec with **new findings**, learning from v1 but provably different.
Four angles, all in scope (user-selected):

1. **Adversarial re-derivation** — independently re-read *current* `forst-rs` HEAD (commits that
   post-date v1, esp. the key-group-affine routing work) and *fresh* profile, to **falsify or refine**
   v1's thesis. Did the wall move? Is in-flight depth still pinned at 1?
2. **Net-new levers** — optimization surfaces v1's OPT-01…12 miss entirely: operator-side state-access
   patterns, iterator lifecycle, bloom/filter strategy, scheduling / thread-affinity / NUMA, checkpoint×join
   interaction.
3. **Borrow-from-ForSt-C++** — port-grade techniques from `main`-branch C++ ForSt that forst-rs lacks
   (offload coordinator, read-io-parallelism, coalesced multiGet, data layout), with `file:line` on both
   sides.
4. **Implementation-grade specs** — top findings rendered as `file:line` + function-signature change
   specs, ready to hand to `writing-plans`.

## 2. The differentiation contract (what keeps it "new")

Every finding carries a tag vs v1:

- `NEW` — absent from v1's OPT-01…12.
- `REFINES` — v1 had it; Round-2 adds fresh evidence or a correction.
- `OVERTURNS` — v1's claim is wrong on current code.
- `PORT` — a concrete ForSt-C++ technique to borrow (file:line both sides).

A finding that merely restates a v1 OPT is **dropped** — cross-referenced as "see v1 OPT-xx", never
re-written. New OPT ids are namespaced **OPT-N01, OPT-N02, …** so they never collide with v1.

If Round-2 **confirms** v1's thesis on fresh evidence, the doc says so plainly — a confirmed-on-fresh
result is a real finding; we do not manufacture disagreement.

## 3. Methodology — full 5×10 + adversarial verification (Workflow)

5 perspectives (prompt A–E) × 10 rounds. Each agent is handed **v1's conclusions + key file:line
anchors** (below) as *known baseline — do NOT repeat; find NEW / WRONG-on-current-code / better-in-C++*.

Perspectives: A Vectorization/Batch · B Memory/Zero-copy · C Arrow/Columnar · D CPU/SIMD ·
E LSM/Data-layout (PMC view).

Rounds:

| R | Target | Primary angle |
|---|---|---|
| 1 | Re-derive q7/q9/q20/q4 path maps on **current** code; confirm/deny v1 §3 chains | Adversarial |
| 2 | Map **net-new surfaces** (operator-side state access, iterator lifecycle, checkpoint×join) | Net-new |
| 3 | Read-path deep dive (3-way) | Adversarial |
| 4 | Read-path adversarial — try to falsify "read is cheap / wall is dispatch" | Adversarial |
| 5 | Write-path deep dive (3-way) | Net-new |
| 6 | Write-path adversarial | Net-new |
| 7 | Cross-boundary / memory / FFM | Net-new |
| 8 | Data-layout + key-encoding **+ port-grade ForSt-C++ borrowing** | Borrow-from-C++ |
| 9 | Vectorization / SIMD / Arrow concretization | Net-new |
| 10 | Cross-validation & convergence | All |

Then: **adversarial verification** — skeptic agents try to *refute* every `NEW`/`OVERTURNS` claim
(majority vote; unrefuted survives, tagged confidence). Then a **synthesizer** dedups, ranks by
frequency×severity×confidence, writes the single doc.

## 4. v1 baseline handed to every agent (challenge target)

- **Thesis:** forst-rs joins are slow because the state executor dispatches each batch *synchronously
  inline on the Flink mailbox thread through a blocking FFM downcall at in-flight depth 1*; the LSM read
  is cheap; micro-opts (vectorize/zero-copy/SIMD) are modest.
- **Key anchors:** `VectorizedExecutor.executeBatchRequests:408/460-463/552`, `fullyLoaded():1022-1025`,
  `dispatchIterPrefix:2219/2297`, `ForStRsLinker.frsVecIterPrefixOpenBatch:4026` (+ unused
  `…Parallel:4056`), blocking-FFM comment `:1275-1277`; `RoutingStateExecutor.java:146-207` (gated off,
  `FRS_RS_PARALLEL_EXECUTOR`); AEC `AsyncExecutionController` `:405/:453/:478/:498`,
  `ExecutionOptions.java:197-200`; ForSt contrast `ForStStateExecutor:148/167/232/289-291`,
  `ForStGeneralMultiGetOperation.java:80-182`, `read-io-parallelism` `ForStOptions.java:286`;
  engine read `db.rs:6861`, `lib.rs:5355`; write anti-pattern `add_batch` per-row (v1 OPT-07).
- **Codebases:** Flink `/Users/lijunqing/Code/stczwd/flink`; ForSt C++ = this repo `main`
  (`git show main:<path>`); forst-rs = this repo `forst-rs` HEAD (`crates/`).
- **Benchmark = NexMark** (NOT TPC-H — v1 corrected this). Joins: q7, q9, q20 (+q4 write-heavy).

## 5. Output skeleton (the new doc)

1. Context + relationship to v1 + current-code delta + the tag contract.
2. Methodology (5×10 + adversarial verify).
3. Re-derived path maps — **deltas vs v1 only**.
4. **Verdict on v1's thesis** (CONFIRMED / REFINED / OVERTURNED on live code) — the headline.
5. Master NEW-findings table (OPT-N##, blank Priority/Status, Confidence).
6. Per-finding specs — implementation-grade (file:line, fn signatures).
7. ForSt-C++ port catalog (PORT items).
8. RocksDB / ForSt absolute-advantage refresh.

## 6. Non-goals

- Not re-litigating v1's refuted ledger (parallel-executor flip, timer-CF, config band-aids) except to
  cite. - Not running 8c/32g benchmarks (code+profile reasoning only; each finding ships a Validation Plan).
- Not splitting into multiple files (HARD RULE: one deliverable doc).
