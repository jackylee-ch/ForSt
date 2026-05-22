# Contributing to ForSt-RS

This document captures the conventions that protect the quality and performance of the ForSt-RS project. Sections marked **MUST** are hard requirements; **SHOULD** is strongly recommended.

---

## Performance work

### MUST: One variable per change

Every commit that targets a performance characteristic (FFI path, engine API, allocation pattern, GC config, dispatch pipeline) **must isolate exactly one variable**. Mixing multiple changes in one commit makes attribution impossible — when the bench moves, you cannot identify which change caused which delta, which makes both shipping and reverting ambiguous.

**Examples of "one variable":**
- Replace `db.get(cf, k)` loop with `db.batch_get(cf, &refs)` — one variable (the engine call shape).
- Add a `BATCH_GET_THRESHOLD` constant and gate dispatch on it — one variable (the gate value).
- Change `-XX:+UseZGC` to `-XX:+UseG1GC` — one variable (the GC).

**Examples of NOT one variable:**
- "Switch to batch_get AND increase cache size AND tune writebuffer" — three variables; if Q12 moves +5% you can't say whether it was the batch_get win, the cache helping, or the writebuffer hurting.
- "Refactor + perf opt" — the refactor's mechanical changes can mask a perf delta; refactor in a separate commit first.

If you genuinely need multiple coupled changes, **split the commit by hand** and bench each step individually. Yes, this is slower. It is also the only way to reliably attribute and roll back.

### MUST: Apply portfolio-aware regression gate

Every performance change is benched against the **representative query suite** (currently the 5-query v2 set Q0/Q3/Q5/Q7/Q8 + the 4 known-regressing queries Q11/Q12/Q13/Q14, with the optional full 23-query sweep before merge). The acceptance gate is:

- **No state-heavy win regresses below 90 % of its prior measurement.** (Example: if Q5 was 3.53× rocksdb, the post-change must be ≥ 3.18×.)
- **No previously-passing query (≥ 1.00×) drops below 1.00×.**
- The change must speed up most queries OR keep regressions within ±5 % across the suite. The "one outlier with > 5 % regression" case is NOT an auto-revert if the rest of the portfolio improves or stays within tolerance — the outlier becomes a tracked investigation item alongside the kept commit.

**Decision rule for borderline cases:**
- Most queries improved + 0 regressions > 5 % → **SHIP unconditionally.**
- Most queries within ±5 % + 1 regression > 5 % → **SHIP with a tracked follow-up to investigate the outlier.** Document the outlier's root cause hypothesis in the commit message; the outlier remains a known issue but does not block the portfolio improvement.
- Most queries within ±5 % + 2 or more regressions > 5 % → **REVERT.** Multiple outliers usually indicate the wrong design layer; revert and re-design.
- The targeted query (e.g., Q12 for an RMW-path change) does not need to improve for the commit to ship if the portfolio is healthy. A "fix targeting Q12 that didn't help Q12 but improved Q5" is acceptable to ship if no other regression exceeds tolerance.

**Do not "fix forward" by stacking more changes** when a regression is in tolerance — those new changes introduce additional variables and re-open the attribution problem. Track the outlier separately.

This portfolio-aware rule supersedes the earlier "any > 5 % regression auto-reverts" rule. The earlier rule was learned-from-failure of the 2026-05-19 Fix #1 experiment (commit `6404be302`) where a naive substitution won Q11 (-9.6 %) but regressed Q12 (+10.7 %). The portfolio-aware rule was learned-from-failure of the 2026-05-19 MapStateCache experiment (commit `4864631d614`, reverted in `9109fc9de61`, then restored in `5148d45b124`) where 6 of 7 queries were within tolerance or improved but a single Q20 outlier triggered a revert that destroyed a Q5 -9 % win. **Both lessons apply: one variable per change; portfolio outcome over single-query outcome.**

### MUST: Bench against the current `main` baseline, not against a remembered number

Benchmark variance is real (criterion ~5-10 % between runs on the same machine; macOS thermal throttling can add another 10 %). Numbers from prior commits drift. Before measuring your change, run the same query on the current `main` to establish the baseline — then run the same query with your change applied. **Compare those two same-session numbers, not your change vs a number from a week-old report.**

### MUST: Document the 3-layer state-class call stack with grep evidence

Any fix targeting a specific query — Nexmark, TPC-H, or otherwise — must list, in the commit message or the spec it implements, **the actual state-class call stack the query traverses, with at least three layers**: the operator class, the state primitive (Value/Map/List/Reducing/Aggregating), and the backend implementation class. Each layer must cite a file path and the grep that confirmed it.

**Example (good):**

```
Q12 (PROCTIME tumble) state-class call stack:
  Layer 1: AsyncStateWindowAggOperator
           flink-table-runtime/.../window/async/tvf/common/AsyncStateWindowAggOperator.java:75
           grep: "windowProcessor" in operators/window/async/tvf/common/
  Layer 2: WindowAsyncValueState<W> wrapping InternalValueState<RowData, W, RowData>
           flink-table-runtime/.../operators/window/async/tvf/state/WindowAsyncValueState.java
           grep: "WindowAsyncValueState\|WindowAsyncMapState\|WindowAsyncListState"
                 → ONLY WindowAsyncValueState exists; no MapState/ListState wrappers.
  Layer 3: ForStRsValueStateV2 (resolves via getOrCreateKeyedState on ValueStateDescriptor)
           flink-statebackend-forst-rs/.../state/ForStRsValueStateV2.java
           grep: "extends AbstractValueState"
```

**Example (bad — caused multi-day misdirection in 2026-05-19 MapStateCache work):**

```
Q11/Q12 fix: add LRU cache to MapStateV2 hot path.
```

The bad example assumed Q11/Q12 hit MapState. They do not. A single grep — `WindowAsyncMapState` returning zero hits across `flink-table-runtime/` — would have caught the misattribution before any code was written. Multiple engineer-days were spent building, benching, reverting, and re-shipping `MapStateCache` under that unvalidated assumption. The fix code is still in the tree but cannot help the target queries.

**Rule:** if you cannot produce three layers of evidence, the fix is targeting an assumed code path, not a verified one. Stop and grep. The cost of the grep is seconds; the cost of a wrong-layer fix is days.

This rule is grounded in [`2026-05-19-q11q12-state-primitive-audit.md`](docs/superpowers/specs/2026-05-19-q11q12-state-primitive-audit.md), the post-mortem that pinned the MapStateCache mis-attribution.

### MUST: Bench the wins, not just the targeted regression

When fixing a regression (e.g., Q12), it's tempting to bench only Q12. **Always bench at least one representative win (e.g., Q5 or Q7) in the same iteration.** Changes that fix the targeted query but inflate the existing wins are net-negative and must be caught early.

The 2026-05-19 Fix #1 attempt would have shipped if the engineer had benched Q11 only (a +9.6 % win) and not Q12 (a +10.7 % regression). Always bench both.

### SHOULD: Stop when the dominant cost moves outside your layer

If profiling or analysis shows the dominant cost has moved outside the layer you are currently optimizing — for example, you are tuning the forst-rs Java backend but the bottleneck is now in the Flink runtime's AEC dispatch policy, or you are tuning the engine's `frs_vectorized_batch_get` but the bottleneck is now in FFM crossing frequency — **document the ceiling, stop current-layer optimization, and surface the cross-layer fix as its own work item**.

The cross-layer fix needs its own design, its own bench cycle, and its own PR. It must **not** be smuggled into the current layer's PR. Two layers in one commit re-opens the attribution problem (you cannot tell which layer's change caused which delta) and obscures the fact that the original layer's optimizations are at their floor.

**Example (this happened on 2026-05-19):** the forst-rs-only Q11/Q12 speedup session minimized per-record Java overhead (composite key cache for RMW pairs) to the point where the remaining Q12 cost is bounded by `FFM crossings × engine per-op cost`. Both are Rust-engine concerns. The right move was to **stop adding Java-side optimizations**, document the ceiling in [`2026-05-19-q11q12-speedup-execution-plan.md`](docs/superpowers/specs/2026-05-19-q11q12-speedup-execution-plan.md), and surface `merge_compute_into` (RMW fusion engine API) as a separate V1.1 work item. Continuing to look for Java-side wins past that point would have produced churn with no measurable impact — and risked mixing in a wrong-layer fix.

**Mechanics:**

- In the PR or spec, write a one-paragraph **Ceiling Statement**: "this layer's cost contribution is X; the remaining cost is in layer Y. Continuing here yields diminishing returns. The next-larger lever is Z, which lives in layer Y and is filed as ticket/spec W."
- File the cross-layer fix as a separate work item with its own gating measurement (e.g., flamegraph or §6-style instrumentation).
- Do not mix the cross-layer change with the current layer's optimization PR.

This rule supersedes the implicit "keep grinding until you're out of ideas" stance. Stop when the data says you should.

### SHOULD: Async-Profiler / perf flamegraph before architectural changes

Before adding a new engine API, a new dispatch path, or a new cache layer, capture a flamegraph of the workload that the change targets. Without it, you are guessing where the cost is. The 2026-05-18 v3.2 analysis identified Q12's 853 ns/event gap as "FFM boundary + per-key engine call + byte[] alloc + small batch size" — but those four contributors aren't equal in weight, and prioritization without flamegraph data is a coin flip.

Tooling:
- **Async-Profiler** for the JVM side (FFM crossing cost, Java allocation hotspots): `async-profiler -d 60 -f /tmp/forst-rs-q12.html <pid>`
- **`perf` + `flamegraph.pl`** for the Rust side (engine-internal CPU): `perf record -F 99 -g -- <bench>; perf script | flamegraph.pl > /tmp/engine-q12.svg`

Attach the flamegraph (or a representative excerpt) to the PR description.

### SHOULD: Conditional prioritization

Not every documented optimization needs to ship. Use the empirical results from one fix to decide whether the next fix is needed. Example from the 2026-05-19 plan:

> **Launch the ValueStateCache work item only if Q12 remains < 0.7× rocksdb after `batch_get_into` (Fix #1d) lands.** If #1d closes Q12 to ≥ 0.7×, defer the cache to V1.2 — its complexity (race tests, per-key tracking, barrier flush sync) only justifies itself when the cheaper fix isn't enough.

Document the conditional gate in the work item itself; future you will thank you.

---

## Code review

### MUST: Performance commits include a benchmark table in the commit message

Every commit that targets performance must include a table of before/after numbers, structured per the "Revert on regression" gates. Example (from `6404be302`):

```
Benchmark results:
  Q11: 176.64 -> 159.62 (-9.6%, win)
  Q12: 116.56 -> 128.99 (+10.7%, REGRESSION)
  Q5:  32.45  ->  31.67 (-2.4%, noise)
  Q7:  195.48 -> 198.78 (+1.7%, noise)

Per revert-on-regression discipline: REVERTED.
```

This makes the commit log itself the audit trail.

### MUST: Rust-side changes reference dylib rebuild command + SHA-256 checksum

Performance commits that change Rust code in `crates/forst-rs-*/` must include in the commit message:

1. The exact rebuild command used to produce the dylib that was benched:

   ```
   Rebuild: cargo build -p forst-rs-ffi --release
   Deploy: cp target/release/libforst_rs_ffi.dylib \
           /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/libforst_rs_ffi.dylib
   ```

2. The SHA-256 checksum of the deployed dylib (proves the binary the bench measured matches the source the commit reflects):

   ```
   Dylib SHA-256: shasum -a 256 .../libforst_rs_ffi.dylib
                  <64-hex-char>  libforst_rs_ffi.dylib
   ```

**Why:** during the 2026-05-19 Fix #1 iteration, the difference between "Rust source committed" and "Rust dylib actually deployed for the bench" caused at least one false start where a stale dylib was producing prior-version numbers. Checksum the deployed artifact and put the value in the commit message — that lets any reviewer reproduce the exact bench.

### MUST: Benchmark reports include criterion repeat counts + variance

Every benchmark number in a commit message or perf-recovery report must come from a measurement that documents:

- **N (number of samples or iterations):** criterion default is 100 samples × 5 s collection. State the value if non-default. For Nexmark wall-clock, state the number of repeats (default: 1 per bench-suite run, so report explicitly).
- **Variance / confidence interval:** criterion auto-reports `[lower median upper]`; include the median + bounds, not just the point estimate. For Nexmark, report the wall-clock + a noise estimate (typically ±5-10 % on this hardware).

Example (good):

```
L1 point_lookup/100K: 29.01 ns [28.90 ns, 29.13 ns] (criterion, 100 samples, p < 0.05 vs prior 32.35 ns)
Q12 Nexmark: 116.56 s (single run, ±~10 % thermal/criterion variance estimate)
```

Example (bad — caused several false-positive deltas in this project's history):

```
Q12: 116s -> 128s slower
```

Without N and variance, "+10 %" might be noise. With them, reviewers can decide.

**Why:** the 2026-05-19 Fix #1b experiment showed Q5 at +5.5 % which is right on the noise boundary. With variance bounds reported, "+5.5 % ± 8 %" reads as inconclusive; without them, "+5.5 %" reads as a regression. The discipline forces honesty about measurement quality.

### SHOULD: Cross-reference the spec document

Performance specs live in `docs/superpowers/specs/`. Reference them in the commit message:

```
Implements Fix #1b per docs/superpowers/specs/2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md §5.
```

---

## Query characterization template (perf work)

Before benching any new Nexmark / SQL workload against forst-rs, fill in the SQL-to-state-shape table for the query. This forces explicit reasoning about the state operations the query exercises before drawing conclusions from wall-clock numbers.

| Field | What to fill in |
|---|---|
| **Q number / name** | e.g. `Q12 — PROCTIME tumble per bidder count` |
| **SQL shape** | One-line summary of the SQL — windowing pattern, join type, aggregation kind |
| **GROUP BY / partition keys** | Per-row operator key (the keyContext driving state) |
| **Dominant state operation** | windowed agg / per-record RMW / streaming join / iterator scan / TopN rank |
| **State class hit** | `ForStRsValueStateV2` / `ForStRsMapStateV2` / `ForStRsAsyncReducingStateV2` / etc. |
| **Cache-hit-rate expectation** | rough estimate ("high — same key hit many times per window" / "low — keys distributed uniformly") |
| **Iterator usage** | none / yes — prefix scan on `<column>` |

Example (Q12):

| Field | Value |
|---|---|
| Q number / name | Q12 — PROCTIME tumble per bidder count |
| SQL shape | `TUMBLE(B, PROCTIME(), INTERVAL '10' SECOND) GROUP BY bidder, window_start, window_end` |
| GROUP BY / partition keys | bidder |
| Dominant state operation | windowed COUNT — per-bid GET-modify-PUT on `MapState<(window_start, window_end), Long>` |
| State class hit | `ForStRsMapStateV2` |
| Cache-hit-rate expectation | ~95 % steady-state (each bidder in 1 active window; consecutive bids hit same key) |
| Iterator usage | none |

**Long-term goal:** auto-generate this table from `EXPLAIN PLAN_WITH_STATE_RESOURCES` output, then contribute the tooling upstream to Flink master so all state backends use the same characterization framework.

The template was extracted from the 2026-05-19 vectorization-violation audit (`docs/superpowers/specs/2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md`) where filling it in for 5 queries explicitly revealed that all 5 hit `ForStRsMapStateV2` — pinning the bottleneck before the perf data confirmed it.

---

## Testing

Pre-existing project conventions for tests (TDD, race-test gates for concurrent state classes, integration tests via `cargo test -p forst-rs-test-harness`) remain in force. See `docs/superpowers/specs/2026-05-16-forst-rs-vectorized-parity-design.md` §5 for the test taxonomy.

---

## How this section was written

This perf-work section was extracted from the empirical patterns of the 2026-05-15 → 2026-05-19 perf-recovery work series. The "one variable + revert" rule is not an a-priori best practice — it was learned by losing a Fix #1 experiment that mixed the right diagnosis (per-key loop is the bottleneck) with the wrong solution (intermediate `Vec<Option<Vec<u8>>>`). The rule exists to prevent that pattern from compounding.

Future contributors: when this rule feels expensive, remember that the alternative is shipping changes that win on one query and silently regress others. The cost of bench-each-step is bounded; the cost of an unattributed regression is unbounded.
