# C1 R1 (post-pivot) — Tech VP consolidated findings

> **Round:** R1 post-pivot (round counter reset to 0 after Pivot 1 i64 fixed-point sum, commit `9202be66d`)
> **Date:** 2026-05-08
> **Reviewers dispatched:** 9 of 10 (Dim 6 Performance still deferred — `baselines_built: false` per A1_reconciliation §4.3)
> **Authority:** `.planning/refactor-review/A1_reconciliation.md @ 33f85b1c5` + `REVIEW_PROTOCOL.md`

## Per-dimension raw totals

| Dim | Reviewer | H | M | L |
|---|---|---|---|---|
| 1 Memory safety | A1 | 0 | 3 | 6 |
| 2 Correctness | A2 | 4 | 5 | 5 |
| 3 Concurrency | A3 | 0 | 6 | 2 |
| 4 Test coverage | A4 | 3 | 10 | 6 |
| 5 Error handling | A5 | 2 | 5 | 4 |
| 6 Performance | DEFERRED | — | — | — |
| 7 Documentation | A7 | 3 | 9 | 4 |
| 8 Idiomatic Rust | A8 | 0 | 6 | 7 |
| 9 Security | A9 | 0 | 5 | 5 |
| 10 Integration | A10 | 0 | 4 | 5 |
| **Raw total** | | **12** | **53** | **44** |

## Tech VP dedup → final classification

### H findings (final, after dedup)

| # | Theme | Sources | Disposition |
|---|---|---|---|
| H#1 | Varint32/64 accept overlong encodings (5th/10th byte high-bit truncation) | A2-H1+H2, A1-M3, A4-M3+M4, A7-M6, A9-M1 (6 reviewers) | **Fix in this round** — wire-format compat with RocksDB; tuning |
| H#2 | `Histogram::new` doc claims "sorted ascending" precondition the code never enforces | A7-H1 (single but real doc/code mismatch) | **Fix in this round** — add assert; tuning |
| H#3 | `Histogram::counts[i]` field doc says cumulative semantics ("&lt;= bounds[i]") but code is bucket-exclusive | A7-H3 (single but real doc/code mismatch) | **Fix in this round** — fix doc; tuning |
| H#4 | `InternalKey::cmp` orders op_type ASCENDING — RocksDB tag ordering is DESCENDING for equal seq | A2-H3 (single, but H-class wire-compat concern) | **ESCALATE per A1 §3.3.3** — cross-boundary (touches storage / engine consumers); needs verification of whether ForSt-RS targets RocksDB iteration-order compat or defines its own. Defer to focused decision item. |

### M findings (final, after dedup) — Tier 1 fix-this-round

| # | Theme | Sources | Disposition |
|---|---|---|---|
| M#1 | `Histogram::sum_fixed` `fetch_add` wraps i64 (not saturating); doc claims saturating | A1-M2, A2-H4, A3-M1, A9-M3 (4 reviewers) | **Doc-fix this round**; saturating-add CAS variant deferred to next-round Tuning if perf budget allows |
| M#2 | "≤1 observation" Concurrency bound is too tight; actual is O(N concurrent observers) | A3-M3, A7-H2 | **Fix doc this round** |
| M#3 | `HistogramSnapshot` top-level doc still says "at a point in time" — contradicts post-pivot weakening | A7-M2 | **Fix doc this round** |
| M#4 | NaN/+Inf land in overflow bucket; `-Inf` lands in smallest bucket — undocumented | A2-M2, A7-M1 | **Fix doc + extend tests this round** |
| M#5 | `SUM_MULTIPLIER` is `pub` — calcifies internal representation as SemVer surface | A8-M2, A10-M2 | **Demote to `pub(crate)` this round** |
| M#6 | `ForstError` lacks `#[non_exhaustive]` — adding any new variant is a breaking change | A5-M1 | **Add `#[non_exhaustive]` this round** |
| M#7 | `HistogramSnapshot::percentile` "all observations in overflow" tail untested | A4-H2 | **Add test this round** (downgrade from H to M; underlying code is correct, just untested) |
| M#8 | `HistogramSnapshot::percentile` zero-count bucket interpolation branch untested | A4-H3 | **Add test this round** (downgrade) |
| M#9 | NaN/Inf bucket placement asserted only for sum, not for `bucket_counts()` | A4-H1 | **Extend tests this round** (downgrade) |
| M#10 | `Arena::allocate` OOM-aborts process; public doc doesn't say so | A5-H1, A7-M4, A9-M2 | **Doc this round; `try_allocate` API is architectural, escalate** |
| M#11 | `Histogram::observe` linear scan is O(n) per observation; `partition_point` is O(log n) | A3-flagged-implicitly, A8-M3, A9-M4 | **Defer to next round** — perf optimization, doesn't affect correctness, doesn't block H/M=0 if other items fixed |

### M findings — Tier 2 (defer to next round; no correctness risk)

A4 test-coverage M's (M#1–M#10 in A4): edge-case test additions; A5-M2/M5 (Arena/Histogram panic-on-config); A7-M3/M5/M7/M8/M9 (additional doc improvements); A8-M1/M4/M5/M6 (idiomatic improvements); A9-M5 (percentile saturating arithmetic); A10-M1/M3/M4 (visibility cleanup — deferred to consumer Cn rounds per A1 §3.3.3).

### L findings — defer to backlog

44 L findings spanning style, edge-case docs, alternative idioms. Tracked in `.planning/refactor-review/known_issues.md` if any are upgraded later; otherwise these accumulate as background polish.

## Architectural escalations (A1_reconciliation §3.3.3)

| Item | Source | Scope | Decision needed |
|---|---|---|---|
| `InternalKey::cmp` op_type ordering vs RocksDB tag-descending | A2-H3 | Cross-boundary (touches storage, engine, FFI consumers) | Does ForSt-RS preserve RocksDB iteration-order compat at the InternalKey layer, or define its own canonical order? If preserve → reverse ordering (and update test); if define-own → document the divergence prominently and verify no consumer code assumes RocksDB order. |
| `Arena::try_allocate` fallible API | A5-H1, R1 A5-H1 inheritance | Within C1 module boundary | New API design; not a 1-line tuning fix. Track for next pivot or focused refactor session. |
| `Histogram::sum` saturating-vs-wrapping accumulator + nan_dropped counter | A2-H4, A5-H2 | Within C1 module boundary | Decision item: doc the wrap (this round, M-fix) vs replace `fetch_add` with saturating CAS loop (next round) vs add `nan_dropped: AtomicU64` field (API expansion). |

## Consecutive-clean counter

After Tier-1 fixes: H_remaining = 1 (H#4 InternalKey, escalated), M_remaining = several deferred-to-next-round. Per `REVIEW_PROTOCOL.md` and `A1_reconciliation §1.3`, consecutive_clean stays 0 (round terminates clean iff H=0 AND M=0). This is expected for R1-post-pivot — the streak builds across multiple rounds.

## Pivot 1 verification (post-pivot evidence per Reviewer-3)

> Pre-pivot finding **H#1** (snapshot non-atomic compound state): **Resolved by doc reframing** — `metrics.rs:166-172, 294-301` documents point-in-time approximate; underlying ops are loss-free per `concurrent_observe_count_accurate`.
>
> Pre-pivot finding **H#2** (NaN poisoning in CAS sum loop): **Resolved by data-type change** — `(NaN as i64) == 0` per Rust spec since 1.45; `fetch_add(0)` is a no-op. Verified by `nan_input_does_not_poison_sum`.

The pivot is correct. R1-post-pivot findings are **new** (latent issues uncovered by the fresh review pass) or **doc-tightening** items the pivot's doc reframing didn't fully address.

## Round outcome

- Fixes landed: see commit `fix(common): C1 R1-post-pivot — H/M fixes` (next).
- Findings file: this document.
- Round counter advances: `current_round: 0 → 1`.
- consecutive_clean: stays 0 (M items remain).
- Architectural escalations: 3 items logged above for Tech VP / user decision before they can be addressed.
