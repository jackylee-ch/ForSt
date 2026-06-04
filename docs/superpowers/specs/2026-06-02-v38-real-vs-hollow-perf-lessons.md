# Lessons from v3.8: Real vs. Hollow Performance, and How to Build Faster *Correctly*

**Date:** 2026-06-02
**Scope:** forst-rs Flink state backend + engine, NexMark q0–q23, local JDK25 vs RocksDB JDK17.
**Status:** Investigation complete; this doc guides the next branch (forst-rs backend + forst-rs timer).

---

## 1. Why this doc exists

v3.8 reported large NexMark "wins" (e.g. q5 ≈ 3.26×, q7 ≈ 22.5× vs RocksDB). Before building on top of
v3.8 we asked a blunt question: **are those wins real, or is forst-rs fast because it silently does
less work / produces wrong output?**

The answer, established with a controlled same-input experiment (below): **some wins are real, some are
hollow — and the split is perfectly predicted by one structural property: event-time windowing.**

---

## 2. How we measured (the method that actually works)

NexMark's generator is **wall-clock paced**, so two runs never produce byte-identical results, and at
parallelism > 1 watermark interleaving adds more non-determinism. Comparing two *separately generated*
runs — even same-backend — is meaningless. Completion time and pane *counts* also hid the bug.

The method that works:

1. **Materialize the bid stream once** to CSV, then **sort by event-time into a single file** (fixed,
   identical bytes for every run).
2. **Replay that file to both backends** at **parallelism = 1**, watermark wide enough to drop nothing.
3. Compare **(a) final output values** and **(b) per-operator `read-records`/`write-records`** from the
   JM REST API.

Per-operator record counts are the key discriminator: they show whether forst-rs *processes the same
data volume* as RocksDB, or collapses it. Harness lives in `/tmp/corr-q5/` (materialize.sh, replay-*.sh,
run-gen.sh). **This should be productionized into a CI correctness gate** (see §6).

---

## 3. The finding: real vs hollow splits on event-time windowing

Five queries were tested directly on identical input (one per operator family); the rest are classified
by the same structural rule.

| Class | Queries | Verdict | Evidence |
|---|---|---|---|
| Stateless / source-bound | q0,q1,q2,q10,q14,q21,q22 | **REAL** (parity) | no state |
| Regular joins | **q3**,q13,q20,q23 | **REAL + correct** | q3 byte-exact (2,201,068 both) |
| Regular group-by aggregation | q4,q15,q16,**q17** | **REAL + correct** | q17: GroupAggregate in=4.6M both |
| OVER dedup / rank | **q18**,q19,q9 | **REAL** | q18: Deduplicate in=4.6M both |
| **Event-time window aggregate** | **q5**,q8 | **HOLLOW + WRONG** | q5: GlobalWindowAggregate **1.5M → 200** |
| **Event-time windowed/temporal join** | **q7** | **WRONG** | q7: join emits **0 vs 11** |
| Session window | q11 | predicted WRONG (windowed, untested) | structural |
| Proc-time window | q12 | N/A — `PROCTIME()`, inherently non-deterministic | structural |
| Unsupported | q6 | skipped by Flink SQL (both backends) | — |

**Bold = empirically tested on identical input.**

### Two distinct failure shapes
- **q5 (hollow):** the windowed-aggregate *merge* phase (`GlobalWindowAggregate`) emits ~200 of the
  ~1.5M records RocksDB emits. Output is **wrong and non-deterministic** (a race). Because almost
  nothing flows downstream, the window-join does **~7,750× less work** → the speedup is **unearned**.
- **q7 (wrong, not hollow):** the windowed `MAX` aggregate is correct (11 = 11), but the downstream
  **temporal join emits 0 rows** (deterministically) where RocksDB emits 11. The join still processed
  the full 4.6M input, so the time is "real work" — but the **answer is empty**, so the win is invalid.

### What is genuinely real
Every **non-windowed** path processes the **identical full data volume** as RocksDB and is correct:
regular joins (q3), regular group-by aggregation (q17), OVER dedup/rank (q18). These are forst-rs's
**legitimate** strengths and should be the headline performance story.

---

## 4. Root-cause surface (where the bug lives)

The defect is **confined to the event-time windowing path**, which has two ingredients RocksDB gets
right and forst-rs currently does not:

1. **Event-time timers** — windows fire on event-time timers. The forst-rs timer service (FORSTRS
   factory) is implicated for every windowed query. (Switching to HEAP timer did **not** fix q5, so the
   timer is necessary-but-not-sufficient — the window *state* path is also wrong.)
2. **Window-keyed state RMW** — the windowed aggregate's per-(key, window) accumulator updates appear to
   race / lose visibility under forst-rs's async state path (`GlobalWindowAggregate` merge collapse),
   and windowed/temporal-join state is not retained until the late window-fired row arrives (q7).

Non-windowed state (plain keyed get/put/scan, regular agg, dedup) is **solid** — q3/q17/q18 prove it.

---

## 5. Lessons (what we learn for "better performance")

1. **A faster windowed query is worthless if its result is wrong.** Treat windowed-query speed as
   *unverified* until its output values are validated. The q5/q7 "wins" must be retracted from any
   benchmark claim until fixed.
2. **Completion-time and pane-*count* parity are not correctness.** q5 matched on pane count (~6M) while
   every `num` value was wrong. Always validate *values*.
3. **Real, defensible wins are on non-windowed queries.** Keep optimizing those (zero-copy Arrow,
   batching, lock-free memtable — already in flight) and report *them* as the win.
4. **Performance and correctness are coupled here:** the hollow speed *is* the bug — fixing the
   windowed-state collapse will both correct the output and remove the unearned speed, giving an honest
   baseline to optimize from.
5. **The timer is on the critical path of all windowed queries** but is not the whole story; the window
   *state* path must be fixed alongside it.
6. **Methodology compounds:** a fixed-input replay + per-operator counts harness is cheap and should be
   permanent — it would have caught this before it became a "win."

---

## 6. Next-branch work plan (forst-rs backend + forst-rs timer)

Ordered by leverage:

1. **Correctness gate first (cheap, prevents regressions):** productionize the `/tmp/corr-q5` harness as
   a repeatable test — materialize a small fixed dataset, replay q3/q5/q7/q17/q18 on forst-rs vs
   RocksDB at p=1, assert identical output. Wire into CI. *Nothing windowed ships green until this
   passes.*
2. **Fix the windowed-aggregate state race (q5):** find why `GlobalWindowAggregate`'s per-(key,window)
   accumulator loses ~99.99% of updates — likely async-state RMW/visibility ordering on the merge
   phase. Target: q5 deterministic and byte-equal to RocksDB on fixed input.
3. **Fix windowed/temporal-join state retention (q7):** bid-side state must survive until the
   watermark-fired per-window max arrives. Target: q7 emits the correct 11 rows.
4. **Fix the forst-rs event-time timer** so window firing is correct and deterministic (the standing
   timer work) — verified by the same gate, not by completion time.
5. **Close the untested suspects:** confirm q8 (windowed join) and q11 (session window).
6. **Only then re-benchmark windowed queries**, validating values, and publish an honest q0–q23 table
   separating verified-correct wins from previously-hollow ones.

---

## 7. Appendix — evidence pointers

- Per-query memory: `project_v38_hollow_vs_real_classification_2026-06-02.md`,
  `project_v38_q5_believability_verified_2026-06-02.md`.
- Harness + raw per-operator counts: `/tmp/corr-q5/` (gen-all2.log, replay-*.log, q7-all.log).
- Incidental engine bug found: `<DbImpl as Drop>::drop` panics
  `Resource deadlock avoided (os error 11)` on teardown (benign post-FINISH, but real — fix on the
  backend branch).
