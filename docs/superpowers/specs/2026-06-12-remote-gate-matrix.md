# Remote Gate Matrix — round-2 (drain-ON tip) validation/falsification plan

**Date:** 2026-06-12
**Status:** MEASURE-FIRST CHECKPOINT PLAN (no code; written before round-1 results land)
**Parent:** `2026-06-12-q9-q20-longscan-roadmap.md` (the model under test)
**Box:** remote yq01 x86, NVMe, split-topo **2×TM 4c/16g + JM 2c/4g**, io_uring live
(TMs `seccomp=unconfined`), images/dirs identical to round-1
(`2026-06-08-8c32g-3backend-sweep-results.md` "REMOTE-x86 POPULATION BEGINS" block).

---

## 0. What round-1 is, what round-2 is

- **Round-1 (in flight):** streaming-read stack **WITHOUT** drain-default
  (engine ≈ pre-43a473315; drain threshold at the old 2M default). Queue:
  q7 uring-on → q7 uring-off → q7 rdb → q9 frs/rdb → q20 frs/rdb. Recorded
  pins so far: q1@1M 3.9s; **q8@100M 153.2s, out_rows 3,064,589** (sweep doc,
  REMOTE block).
- **Round-2 (this matrix):** same box, same topology, same flags-from-env
  (none set), **new tip**: engine `6e76b33be` lineage = drain DEFAULT-ON @200K
  (43a473315) + H1 pool `catch_unwind`/recv-timeouts (7ad678d25) + BE merge
  operator (fe771c000 — **inert**: no CF binds it, flag-gated feature unbuilt);
  Java jar **flink 72607e11097** = alloc-free timer peek/poll + SegmentHash +
  mismatch compares. `FRS_RS_MIXED_BATCH` stays default-OFF; merge-RMW (OPT-N04
  J1-J5) is NOT in this round.

The round-2-vs-round-1 delta is therefore **one integration tier** whose only
expected *perf-visible* member is the drain default; the drain is separable
in-session via `FRS_GARBAGE_DRAIN_TOMBSTONES=0` (the q17 control below).

### The population-scaling caveat (binding)

**Remote numbers are a NEW population — never compare absolute seconds to Mac
pins** (rule recorded in the sweep doc REMOTE block). All Mac-recorded numbers
(q9 2378.5/2001.2, q20 2045.8/1477.7, RDB 1449.5/1034.4, roadmap endpoint
"q9 ~1430-1600 s") are **8c/32g single-container Mac-docker seconds**. The
roadmap's q9 ~1430-1600 s endpoint is reachable *on the remote box* only as a
**ratio statement** (0.99-1.10× same-session RDB), not as a wall-clock number:
the remote box is split-topo, x86, NVMe, and round-1's q8 (153.2s vs Mac ~46s
class) already shows a ≫1 population scale factor. Every expectation below is
therefore expressed as (a) a **multiplier vs the round-1 same-box run** or
(b) a **same-session frs/rdb ratio** — the only two forms that transfer.

---

## 1. Preconditions (all must hold before the first timed run)

| # | Check | How |
|---|---|---|
| P1 | Round-1 matrix COMPLETE and its SUMMARY.md archived (round-2 deltas are defined against it) | `/ssd2/jackylee/frs-bench/logs/SUMMARY.md` |
| P2 | `.so` rebuilt from ForSt tip (≥ `6e76b33be`), host-built glibc-2.17⊂jammy as round-1; jar from flink `72607e11097` | record git SHAs + sha256 of both artifacts in the run log |
| P3 | Drain default verified ON@200K from the ENGINE LOG, not assumed (env `FRS_GARBAGE_DRAIN_TOMBSTONES` UNSET) | grep engine startup/drain log lines |
| P4 | `FRS_RS_MIXED_BATCH`, `FRS_RS_PARALLEL_EXECUTOR`, parallel-iter flags UNSET (defaults) | env dump in run log |
| P5 | io_uring active (TMs seccomp=unconfined), same as round-1 | startup log |
| P6 | Box idle check before each cell (no leftover TM/JM, page cache dropped or at least recorded) | `free`, process list |
| P7 | RocksDB runs use the SAME session/box/day as their frs pair (same-session law) | run ordering below |

---

## 2. The matrix (run in this order; later cells abort on earlier failures)

| # | Cell | n | Gate (pass) | Expected (traced — §3) | On fail |
|---|---|---|---|---|---|
| M0 | q1@1M frs smoke | 1 | finishes, out 1,000,000 | ~3.9 s ±50 % (E1) | A1 abort: harness/build broken |
| M1 | **q8@100M frs canary** | 1 | out_rows ∈ **2.95M-3.06M band**; wall ≤ 1.10× round-1's 153.2 s | ≈153 s (E2); drain should not trip on q8 | rows out of band → A2 CORRECTNESS abort. wall regress → A3 |
| M2 | **q3@100M frs + rdb pair** (point-get no-regress) | 1+1 | out_rows 2,201,068 EXACT both; frs/rdb ratio ∈ 0.90-1.10 | parity (E3) | ratio >1.10 → A3 (alloc/timer fixes or drain leak onto light path) |
| M3 | **q17@100M frs ×3 (drain-ON)** + **×1 control `FRS_GARBAGE_DRAIN_TOMBSTONES=0`** — control runs FIRST (lesson E4b) | 3+1 | out_rows 92,000,000 all; **median(drain-ON) ≤ 1.15× control**; no run >1.3× control | drain-ON ≈ control (E4: Mac attribution-reversal — drain variants and control landed in ONE band) | median >1.3× control → A4: drain gate leaks on this population — set `=0` for the rest of the round, file gate bug, drain default flip is FALSIFIED remotely |
| M4 | **q9@100M frs ×2 + rdb ×2** | 2+2 | out_rows **91,813,372 EXACT** every frs run (10× sentinel); both frs walls within 5 % of each other | frs ≈ **0.84 × round-1 q9-frs** (E5); frs/rdb ratio ≈ **1.3-1.5** (E6) | rows wrong → A2. delta < 60 % of model → D2 re-profile |
| M5 | **q20@100M frs ×2 + rdb ×2** | 2+2 | out_rows **93,201,404 EXACT**; **FINISHES** (no DNF) | if round-1 q20 finished: frs ≈ **0.72 × round-1** (E7); if round-1 DNF'd: any exact finish = L1 validated remotely; frs/rdb ratio ≈ **1.3-1.5** (E8) | DNF → A5 + D2 (drain model invalid on this population) |

Budget note: M4/M5 are the expensive cells (4 × ~30-60 min-class runs each).
M0-M3 are the cheap abort-early screen; do not reorder.

---

## 3. Evidence — every expected number traces to a recorded measurement

| # | Expectation | Recorded source |
|---|---|---|
| E1 | q1@1M ≈ 3.9 s | sweep doc REMOTE block: "q1@1M smoke FINISHED 3.9s, out 1,000,000" (commit 06b0faafa) |
| E2 | q8 canary 153.2 s / 3,064,589; band 2.95-3.06M (q8 is ±4 % nondeterministic — band, not exact, is its gate) | REMOTE block (06b0faafa); band: sweep doc :650-651 ("correct band (2.95–3.06M, == depth-1/N=1/RocksDB band)") |
| E3 | q3 parity, 2,201,068 exact | sweep doc :109 (Mac pair 36.8/36.7 s = 1.00×, identical rows both backends); rows also the seeded-replay byte-exact query (memory: correctness sweep 2026-06-01) |
| E4 | q17 drain-ON ≈ drain-OFF control (same band) | sweep doc "ATTRIBUTION REVERSAL (2026-06-11 07:25)": drain 200K/ratio 190.1, density 228.9, 2M 267.9, feedback 280.0, **FULLY OFF control 228.1 — same band**; plus memory q17-terminal note: q17-class single runs are noise — hence median-of-3 |
| E4b | control runs FIRST | sweep doc same block, recorded LESSON: "never judge a lever against a different-build, different-day baseline — the control run must come FIRST" |
| E5 | q9 round-2 ≈ 0.84 × round-1 | drain-threshold curve (sweep doc "q9 drain-threshold curve"): 2M 2378.5 s → 200K 2001.2 s = ×0.841, deterministic (rows identical across ≥6 runs). Round-1 remote ran the 2M-default engine; round-2 ships 200K default (43a473315) |
| E6 | q9 frs/rdb ratio ≈ 1.3-1.5 this round | Mac post-L1 ratio = 2001.2 / 1449.5 = **1.38×** (roadmap doc header + curve); band widened ±0.1 for population shift. The 1.05× bar is NOT expected this round — roadmap places bar-crossing at steps 3-4 (S2 + compaction-windowed), both unbuilt |
| E7 | q20 round-2 ≈ 0.72 × round-1 (if round-1 finished) | drain validation matrix (sweep doc "DRAIN-200K VALIDATION MATRIX"): q20 default-path 2045.8 s baseline (roadmap header) vs 200K **1477.7 s** = ×0.722; at the 2M config q20 also recorded **DNF 86.5M@1800** — hence the round-1-DNF branch |
| E8 | q20 frs/rdb ratio ≈ 1.3-1.5 | Mac post-L1 ratio = 1477.7 / 1034.4 = **1.43×** (roadmap header numbers) |
| E9 | Roadmap endpoint "q9 ~1430-1600 s" | roadmap §4 — **Mac-population model seconds** for steps 0-4 COMPLETE (incl. unbuilt L3/L4). On remote this maps ONLY to the ratio target 0.99-1.10×; do not gate any round-2 cell on it |

---

## 4. Abort criteria

- **A1 (harness):** M0 fails → stop; fix build/submit path; nothing is a perf signal.
- **A2 (correctness):** ANY out_rows mismatch (q8 band, q3/q9/q20 exact) → stop the
  matrix. Correctness before performance (standing rule). Bisect engine-vs-jar by
  rerunning the failing query with the round-1 `.so` + round-2 jar, then round-2
  `.so` + round-1 jar.
- **A3 (canary perf regress):** q8 wall >1.10× round-1 or q3 ratio >1.10 → do not
  proceed to M4/M5 until attributed (suspects in order: drain misfire on small-state
  — check drain log lines; timer/alloc Java delta — A/B with round-1 jar).
- **A4 (q17 drain robbery, remote):** median(drain-ON ×3) >1.3× control → the
  composite gate (volume+feedback+512MB floor, db.rs `garbage_drain_gate`) fails on
  this population. Set `FRS_GARBAGE_DRAIN_TOMBSTONES=0` for M4/M5 (they keep their
  win via explicit env =200000 — the recorded opt-in config), and the DEFAULT flip is
  falsified pending a gate fix.
- **A5 (DNF):** any frs run exceeding 2× its expected wall with rate→0 → kill, capture
  TM jstack + engine drain/compaction log tail before teardown (the evidence for D2).

---

## 5. Decision rules (what each outcome triggers)

| Outcome | Decision |
|---|---|
| **D1 — model holds:** q9 delta ≥ 60 % of E5's −16 % AND q20 finishes exact with delta ≥ 60 % of E7 (or first-finish vs round-1 DNF), canaries clean | **PROCEED to S2 (L3, `2026-06-12-s2-pinned-rows-loser-tree-design.md`) and compaction-windowed (L4, `2026-06-12-compaction-windowed-readpath-design.md`)** — the roadmap's next steps; their models (q20 −9-13 % and −6-10 %) carry bar-crossing. OPT-N04 Java work (J1-J5) proceeds in parallel only after its engine prereqs E1/E3 land (review finding, OPT-N04 spec §3.2/§3.3) |
| **D2 — model falsified:** q9/q20 deltas < 60 % of E5/E7, or q20 DNFs with drain ON | **STOP feature work; re-profile on the remote box** (roadmap §3 falsifier rule: residual would be diffuse serde/framework floor, not engine — further engine stages wasted). Deliverable of the re-profile: symbolized perf top-20 + drain/compaction telemetry on the live q9/q20 run |
| **D3 — drain robs remotely (A4 fired):** | Gate fix campaign BEFORE S2: the remote population is the deployment target; a default that robs there is not shippable. q9/q20 continue via explicit opt-in env (recorded config) so S2/L4 measurement is not blocked |
| **D4 — canary regress (A3) attributed to Java alloc/timer commits:** | Revert-candidate review of flink 72607e11097 deltas (they are perf-only; semantics verified in the 2026-06-12 PMC review) — but only after an n≥2 confirm; single-run regressions on this class of box are noise (q17 lesson) |
| **D5 — q9/q20 ratios already ≤ 1.10× rdb:** (upside surprise — population favors frs) | Re-rank roadmap: run q9/q20 n+2 to confirm, then S2/L4 still proceed (they are also q11/q19/q4 levers) but bar-pressure drops; promote OPT-N04 canary measurement (q8/q12) earlier |

---

## 6. Recording requirements

Every cell appends to `/ssd2/jackylee/frs-bench/logs/SUMMARY.md`: query, backend,
git SHAs (.so + jar), env dump, wall, out_rows, drain log line count + reclaim
totals, peak TM RSS. Ratios computed only within-session. Round-2 numbers become
the new remote pins; update the roadmap doc's "Current (recorded)" header ONLY
with remote-vs-remote statements.
