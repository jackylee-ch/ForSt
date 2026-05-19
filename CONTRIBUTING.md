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

### MUST: Revert on regression

Every performance change is benched against the **representative query suite** (currently the 5-query v2 set Q0/Q3/Q5/Q7/Q8 + the 4 known-regressing queries Q11/Q12/Q13/Q14, with the optional full 23-query sweep before merge). The acceptance gate is:

- **No state-heavy win regresses below 90 % of its prior measurement.** (Example: if Q5 was 3.53× rocksdb, the post-change must be ≥ 3.18×.)
- **No previously-passing query (≥ 1.00×) drops below 1.00×.**
- The targeted query (e.g., Q12 for an RMW-path change) must show a measurable improvement (≥ +5 %) for the change to land. Sub-5% changes are within criterion noise; do not merge them without an additional independent measurement.

**If any of these gates fail, revert. Do not "fix forward" by stacking more changes** — those new changes introduce additional variables and re-open the attribution problem.

This rule is empirically established. The 2026-05-19 `frs_vectorized_batch_get` Fix #1 experiment (commit then revert in `6404be302`) demonstrated its value: the naive substitution won Q11 (-9.6 % wall-clock) but regressed Q12 (+10.7 %). The "one variable + revert" discipline caught the regression cleanly and bounded the next API design (`batch_get_into` per spec `2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md`).

### MUST: Bench against the current `main` baseline, not against a remembered number

Benchmark variance is real (criterion ~5-10 % between runs on the same machine; macOS thermal throttling can add another 10 %). Numbers from prior commits drift. Before measuring your change, run the same query on the current `main` to establish the baseline — then run the same query with your change applied. **Compare those two same-session numbers, not your change vs a number from a week-old report.**

### MUST: Bench the wins, not just the targeted regression

When fixing a regression (e.g., Q12), it's tempting to bench only Q12. **Always bench at least one representative win (e.g., Q5 or Q7) in the same iteration.** Changes that fix the targeted query but inflate the existing wins are net-negative and must be caught early.

The 2026-05-19 Fix #1 attempt would have shipped if the engineer had benched Q11 only (a +9.6 % win) and not Q12 (a +10.7 % regression). Always bench both.

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

## Testing

Pre-existing project conventions for tests (TDD, race-test gates for concurrent state classes, integration tests via `cargo test -p forst-rs-test-harness`) remain in force. See `docs/superpowers/specs/2026-05-16-forst-rs-vectorized-parity-design.md` §5 for the test taxonomy.

---

## How this section was written

This perf-work section was extracted from the empirical patterns of the 2026-05-15 → 2026-05-19 perf-recovery work series. The "one variable + revert" rule is not an a-priori best practice — it was learned by losing a Fix #1 experiment that mixed the right diagnosis (per-key loop is the bottleneck) with the wrong solution (intermediate `Vec<Option<Vec<u8>>>`). The rule exists to prevent that pattern from compounding.

Future contributors: when this rule feels expensive, remember that the alternative is shipping changes that win on one query and silently regress others. The cost of bench-each-step is bounded; the cost of an unattributed regression is unbounded.
