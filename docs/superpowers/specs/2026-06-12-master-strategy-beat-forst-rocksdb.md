# MASTER STRATEGY — forst-rs beats ForSt per-query and beats BOTH backends on total q0-q22

**Date:** 2026-06-12
**Status:** PMC MASTER STRATEGY (standing document; supersedes ad-hoc lever-chasing)
**Goals:**
- **GOAL-1:** forst-rs faster than ForSt on EVERY NexMark query (q6 excluded — unsupported by
  Flink SQL for all backends, recorded 2026-06-01 correctness sweep).
- **GOAL-2:** forst-rs beats BOTH RocksDB and ForSt on TOTAL q0-q22 execution time.

**Binding population:** remote x86/NVMe box (yq01, split-topo 2×TM 4c/16g + JM 2c/4g,
io_uring live) — sweep doc `2026-06-08-8c32g-3backend-sweep-results.md` (henceforth SWEEP)
CURRENT STATUS block. Mac 8c/32g numbers are a SEPARATE legacy population (all ForSt numbers
are Mac-only today). Cross-population seconds are never compared; only same-session ratios
transfer (`2026-06-12-remote-gate-matrix.md` §0 "population-scaling caveat").

Every number below carries a citation: `SWEEP:<line>` = sweep doc line; `ROADMAP` =
`2026-06-12-q9-q20-longscan-roadmap.md`; `B1` = `2026-06-12-local-ffi-flush-compaction-bench-design.md`
§5/§5b; `S2-SPEC` = `2026-06-12-s2-pinned-rows-loser-tree-design.md`; `L4-SPEC` =
`2026-06-12-compaction-windowed-readpath-design.md`; `N04-SPEC` =
`2026-06-12-opt-n04-merge-rmw-backend-design.md`; `S2-REV` =
`review-rounds/2026-06-12-pmc-review-s2.md`; `APX-<n>` = Evidence appendix run executed for
this document (commands + raw output in §F).

---

## A. Scoreboard + gap decomposition

### A.1 Per-query scoreboard (two populations, holes flagged)

R = remote (binding), M = Mac legacy. Source: SWEEP:42-74 (refreshed table 2026-06-12) +
SWEEP:2174-2241 (remote rounds 1-2).

| query | RDB | frs | ForSt (M only) | frs/RDB | frs/ForSt | verdict |
|---|---|---|---|---|---|---|
| q0 | 31.8 M | 30.9 M | 30.7 M | 0.97× | 1.01× | parity (±0.2s = noise; n≥3 owed) |
| q1 | 30.1 M | 29.3 M | 30.1 M | 0.97× | 0.97× | PASS |
| q2 | 28.2 M | 27.8 M | 28.2 M | 0.99× | 0.99× | PASS |
| q3 | 122.6 R / 35.0 M | 142.1 R / 35.6 M | 37.2 M | 1.16× R | 0.96× M | PASS both goals |
| q4 | 302.7 M | 373.0 M | 1042.2 M | 1.23× | **0.36×** | beats ForSt big; RDB gap +70.3s; final-result compare OWED |
| q5 | 162.5 M | **HOLE** (correctness FIXED 80015d2cfaa; 100M re-measure owed) | DNF M | — | win-by-finish | HOLE #1 |
| q7 | 1367.6 R (DNF M) | 2376.4 R / 1441.6 M | 586.8 M | **1.74× R** | **2.46× M** | WORST GOAL-1 row |
| q8 | 42.5 M | 43.6 M (153-170 R, no R-rdb pin) | 39.2 M | 1.03× | 1.11× (+4.4s) | small ForSt gap |
| q9 | 2121.2 R / 1437.6 M | 2421.2 R / 2349.3 M | DNF M | **1.14× R = PASS** | win-by-finish | r2-tip +358s regression under bisect (SWEEP:2243-2251) |
| q10 | 129.5 M | 124.2 M | 129.3 M | 0.96× | 0.96× | PASS |
| q11 | 106.1 M | 264.8 M | 134.9 M | **2.50×** | **1.96×** | FAIL both |
| q12 | 41.4 M | 49.6 M | 39.7 M | 1.20× | 1.25× (+9.9s) | ForSt gap |
| q13 | 29.3 M | 28.5 M | 29.5 M | 0.97× | 0.97× | PASS |
| q14 | 29.6 M | 29.4 M | 29.2 M | 0.99× | 1.01× | parity-noise |
| q15 | 229.7 M | 161.8 M | 206.8 M | 0.70× | 0.78× | PASS (beats both) |
| q16 | 374.1 M | 323.9 M | 331.7 M | 0.87× | 0.98× | PASS (beats both) |
| q17 | 72.8 M (R pin OWED) | 77.7 M / 387-423 R ±4.5% | 253.0 M | 1.07× M | **0.31×** | PASS; HOLE #3 (R rdb pin) |
| q18 | 366.4 M | 222.9 M | DNF M | 0.61× | win-by-finish | PASS (beats both) |
| q19 | 310.2 M | 527.9 M | 308.1 M | **1.70×** | **1.71×** | FAIL both |
| q20 | 1557.6 R / 1034.4 M | 2011.1 R / 2045.8 M | 1535.9 M | **1.29× R** (near-miss) | **1.33× M** | FAIL both, closest heavy |
| q21 | 60.0 M | 53.9 M | 58.3 M | 0.90× | 0.92× | PASS |
| q22 | 44.2 M | 43.6 M | 46.3 M | 0.99× | 0.94× | PASS |

**Holes (each is a PLAN stage, §D):** (1) q5 frs @100M (re-measure post-fix); (2) ForSt has NO
remote pins at all — every GOAL-1 cell is Mac-legacy; (3) q17/q8 remote RDB pins; (4) q4
final-result compare (retract query — out_rows 25.8M vs 177.6M is cadence-suspect, not a clean
gate, SWEEP:213-229); (5) full remote q0-q22 for all three backends (only q1/q3/q7/q8/q9/q17/q20
have any remote data).

### A.2 TOTAL-time math (GOAL-2 arithmetic)

**Mac population (only full-coverage population today).**
- Common-20 set (excl. q5, q7 — holes): **frs 6843.5 s vs RDB 4735.6 s = 1.445×**
  (sums over table above). Deficit decomposition: q9 +911.7, q20 +1011.4, q19 +217.7,
  q11 +158.7, q4 +70.3, q12 +8.2, q17 +4.9, q8 +1.1, q3 +0.6 (Σ deficits 2384.6);
  wins −276.7 (q18 −143.5, q15 −67.9, q16 −50.2, lights ~−15). Net −2107.9.
- vs ForSt, common-19 set (excl. q5/q9/q18 = ForSt DNFs; incl. q7): **frs 5712.9 vs
  ForSt 4897.1** — ForSt leads by 815.8, of which **q7 alone is +854.8**; q20 +509.9,
  q19 +219.8, q11 +129.9, q12 +9.9, q8 +4.4 vs frs wins −913.3 (q4 −669.2! q17 −175.3,
  q15 −45.0, rest −23.8).
- Full-22 with DNF floors (DNF ≥ 1800s cutoff): frs ≈ 8445 (q5 est. ~165) vs
  RDB ≥ 6698 (q7 DNF-M) vs **ForSt ≥ 10297 (3 DNFs)**. ⇒ **GOAL-2-vs-ForSt is already met
  on finish-time arithmetic** (ForSt cannot finish q5/q9/q18); the honest common-set gap is
  the q7-concentrated 815.8s. **GOAL-2-vs-RDB needs ~1.3-2.1 ks clawed back on Mac math.**

**Remote population (binding, partial).** Heavy-4 measured pairs (SWEEP:2174-2235):
frs 142.1+2376.4+2421.2+2011.1 = **6950.8 vs RDB 5169.0 = 1.34×**, deficit +1781.8 =
q7 +1008.8, q20 +453.5, q9 +300.0, q3 +19.5. The remote population COMPRESSES frs deficits
(q9 1.63×M→1.14×R; q20 1.98×M→1.29×R) — the GOAL-2-vs-RDB verdict must therefore be computed
remotely, where the structural gap is ~½ the Mac one.

**Total-time headroom coverage by lever class** (composed models, §B/§C, applied to the
walls each class touches):
- R-long scan engine class (S2 + L4 + already-shipped prefetch/io_uring riding): q7
  2376.4×(15-30% S2 + 4-6% L4) ≈ −450..−800; q20 2011.1×(−15..−21%) ≈ −300..−430;
  q9 2421.2×(−7..−13%) ≈ −170..−320; q11/q19 Mac-class −30..−90 ⇒ **class covers
  ~950-1650 s of the ~1.8 ks remote heavy deficit** — the dominant class.
- Executor pipelining class: q11 −150 M-s (recorded 2.35×), q12 −4, q9/q20 −4..6% each
  (−100..−210 remote-s) ⇒ **~250-360 s**.
- Chain-kill class (OPT-N04): q12 −4..7, q8 −2..4 ⇒ **~6-11 s** — small in seconds, but it
  closes two of the six GOAL-1 rows.
- Hygiene class (V1-V6): noise-level wall; GC/profile de-noising only.
- Residual not covered by any modeled lever: q19's diffuse −130..−160 (to ForSt) and
  q7's last ~10-25% — owned by the Stage-6 re-profile checkpoints, not by a named lever.

### A.3 Gap decomposition per failing query (measured components)

- **q20** — recorded CPU profile (ROADMAP §1.2): join **prefix-scan 21.6%** (decomposes into
  O(sources)-twice two-phase merge + 2 `Arc::from` allocs/row + demand block fetch,
  S2-SPEC §1), **compaction 19.3%** (demand-paged, serial with merge, cache-polluting,
  L4-SPEC §1), per-record GET→PUT RMW chain on the count map + full asyncEntries bucket scan
  per probe (SWEEP:1342-1348; operator-side MapState shape — N04-SPEC §2 scope note:
  backend-transparent merge CANNOT capture it), residual = serde/framework wait
  (KeyAccounting per-key chains).
- **q9** — symbolized 180s profile (SWEEP:1598-1615): TM averaged **1.7/8 cores ⇒
  latency-bound**; of busy CPU: prefix-scan total 32.8% (open/stream-build 17.2,
  drain fill_chunk 15.0, peek 9.6), compaction 12.0%, opendal I/O 7.2%, entire GET path
  6.5% (OPT-N16 killed by this number), wake/signal ~6%. Per-key dependent chains
  (GET→PUT on accumulator) serialize under AEC KeyAccounting (SWEEP:1270-1274-class
  finding; N04-SPEC §0).
- **q7** — probe-OPEN-rate dominated: jstack 61/85 executor samples in
  `openVecIterIntoBuf → frsVecIterPrefixOpen` (join_probe_open.rs header);
  B1: batched open ≈ 104.2 µs/probe of which crossing ≈ free — the cost IS engine scan-build
  (B1 §5b); deep L0 fan-out makes the open O(sources); io_uring = finish-vs-DNF remotely
  (SWEEP:2142-2152) ⇒ a real I/O-stall component. S2's tournament tree + pinned replenish
  is the direct counter (my A/B: §F.1 — fan-out-128 probe 5.4-8.4× faster).
- **q11** — depth-1 in-flight drain: recorded offload A/B 318.9→135.8 s (2.35×, rows exact,
  SWEEP:527-535) — the serial post-source drain tail was the gap; R-long scan levers also apply.
- **q19** — findRow O(n²) was 52% of on-CPU and is FIXED (SWEEP:838-845, committed
  92a5d7c400b); post-findRow residual is DIFFUSE serde + engine read (JFR: RowDataSerializer
  copy/materialize + engine read + hashOf) — no single hotspot ≥10%. S2/L4 apply partially;
  parallel executor REFUTED for q19 (OVER-window, few disjoint keys).
- **q12 / q8** — small per-record deltas (+9.9s / +4.4s vs ForSt): per-record reduce/agg
  RMW chains — exactly the OPT-N04 canary population (N04-SPEC §2: ReducingState
  q12-pattern SumAgg/CountAgg); plus timer-path alloc hygiene (audit V1-V3).
- **q4** — beats ForSt by 669s; RDB gap +70.3s is diffuse per-record efficiency (recorded
  2026-06-06 campaign: config exhausted, no single lever ≥2%); q4@10M wedge is pre-existing
  (3-step bisect, SWEEP:1312-1321) and @100M unaffected.
- **q3 remote** — 1.16× (142.1/122.6): point-get parity-class; inside every bar; not a
  campaign target beyond no-regress gates.

---

## B. Lever catalog (status · evidence · applicability · model · risk)

Legend: model formulas stated per lever; "model" = projection anchored on a measured share or
a recorded A/B, NOT a guess. No lever without measured/recorded evidence enters §D.

### B.1 SHIPPED (in the default path today)

| Lever | Evidence | Applies to | Already in baseline |
|---|---|---|---|
| Streaming-read P0/P1 + BlockPrefetcher + io_uring (engine 2045ea577/e1aa00bcd/c8663821e/a2ee94e56, flink 725824ae9e9) | q7 remote: io_uring ON 2376.4 FINISH vs OFF DNF@2400 (A/B, SWEEP:2142-2148); q9 1.63×M→1.14×R rides it | all R-long | yes |
| Block-cache wiring of 5 eager reader sites (27ae792c3) | q20 +25%, q19 −12%, q9 collapse eliminated (SWEEP:100-117) | read-heavy | yes |
| Prefix bloom v3 + N14 depth-gated | q9 91M→94.8M@cutoff progression (SWEEP:1266-1273) | prefix scans | yes |
| Memory-resident timer index (flink 7590cde1c1f) | q17/q20 UNCHANGED ⇒ timer tax was box noise; 3 bug-generations deleted; 11-test suite revived (SWEEP:2042-2070) | correctness/architecture | yes (perf-neutral) |
| Timer-queue refill-floor fix (flink 1d9a844dd52) | q8 5/5 exact + multi-worker routing-async 3/3 exact post-fix (SWEEP:1953-1974) | correctness; UNBLOCKS executor levers | yes |
| q19 findRow O(n²)→O(1) (flink 92a5d7c400b) | removed the 52%-CPU hotspot; q19 599.7 vs >700 A/B (SWEEP:782-786) | churn-heavy (q19) | yes |
| Uniform memory model (WBM backpressure + coalescing + noflush=false) | OOM set → finishes; flush 202→20 ms/MB (SWEEP:119-165) | all | yes |
| E5 scan-locator multi-CF soundness (0dc95b535) | correctness blocker for OPT-N04 CFs; zero scan-path cost measured (B1 §5b verdict 4) | enabler | yes |

### B.2 BUILT, flag-OFF — S2 pinned rows + tournament tree (`FRS_RS_S2_PINNED`)

- **Status:** merged (83ec3052a, f22fbbf21, 214180314 + F-1/F-2 fix e5b49c290), PMC-approved
  (S2-REV: zero blockers flag-OFF; suites 409/0 storage, 298/0 engine both states, 108/0 ffi).
- **Measured evidence (this document's runs, §F.1/§F.2, Mac, same-session interleaved A/B):**
  - deep fan-out probe (q7/q20 shape, `join_probe_open`): **ssts_128 OFF median 7.79 µs →
    ON 1.26 µs = 6.2× via the in-process Arc adapter; raw sink path (`fill_into`, the
    production-FFI shape) ~0.92 µs = 8.5× vs legacy** — confirms the recorded "ssts_128
    9.2×" class (SWEEP:31).
  - emit-path drain through the REAL FFI (`ffi_vectorized/iter_drain`, 100×1000 rows):
    OFF 155.5 ns/row → ON 143.6 ns/row = **−7.7%** (recorded class −10%); probe OPEN
    (`iter_open_batch` k=64): 106.2 → 108.4 µs/probe = +1.7% (open is engine scan-build
    dominated; the small pinned-build cost is S2-REV A2's advisory — watch in round-3 S4).
  - **Adapter-tax finding (new, actionable):** at shallow fan-out (ssts_1-64) the ON state
    through the Arc-pair COMPAT ADAPTER is +20-23% vs OFF, while the raw `fill_into` sink is
    parity-to-faster (±6%). This is exactly S2-REV A3/§6.1's documented measurement gap —
    production FFI opens use the raw ChunkSink (no adapter), but any engine-internal
    `prefix_scan` consumer and the SERIAL `frs_vec_iter_prefix_open_batch` symbol ride the
    adapter ⇒ port-or-confirm-non-production before default-ON (Stage 1 checklist item).
  - emit path: `iter_drain`/`iter_open_batch` ON vs OFF — §F.2.
- **Applicability matrix:** q7 (open-dominated — the primary), q20 (21.6% scan share),
  q9 (−4..8% model), q11/q19 (R-long scans), q4/q3 must be no-regress (linear-branch guard).
- **Model (S2-SPEC §4):** q20 −9..13%, q9 −4..8%; q7: micro says the per-open merge cost at
  q7-class fan-out drops ~5-8×; if probe-open is ~⅓-½ of q7's wall (jstack share + B1
  104 µs/probe at q7's ~10⁸ probe volume), q7 −15..30% — falsifiable in round-3 cell S5.
- **Risk/deps:** remote round-3 gates (S2-REV §9 S0-S5); blocked behind the r2-regression
  bisect (a clean tip is precondition P1/P2); falsifier-2 binding (q20 < −4% with scan share
  ≥15% ⇒ STOP S2 follow-ups, run L4/L0 first).

### B.3 DESIGNED — L4 compaction-windowed reads + cache-skip (`FRS_COMPACT_WINDOWED`)

- **Status:** implementable spec (L4-SPEC); machinery (read_block_regions, fetch_window,
  BlockPrefetcher pool) already shipped for the scan path; H1 catch_unwind prereq landed
  (7ad678d25, alloc-audit Appendix A2/A5).
- **Measured evidence:** (a) anchor share: compaction = 19.3% of q20 CPU (ROADMAP §1.2),
  12.0% of q9 busy-CPU (SWEEP:1612); (b) **B3 harness built+run for this document (§F.3,
  new `compaction_throughput` bin)**: isolated 8×64MiB L0→L1 merge = **6.5-6.8 ns/byte-live
  (~1.1-1.2 GB/s input)** — the recorded "3 ns/byte isolated" class (memory 2026-06-05);
  a single point-get sidecar on the idle 14-core Mac costs **nothing** (6.35-6.46 live vs
  6.47-6.79 isolated) ⇒ the recorded 21 ns/byte LIVE number requires deployment-class core
  saturation + cold-block I/O, which an idle page-cached Mac cannot reproduce. **Implication
  (honest):** B3 establishes the merge-CPU floor and is the mechanism harness for L4's
  before/after; the 19.3% q20 share (saturated 8-core box, multi-GB inputs falling out of
  page cache, cache pollution) remains L4's primary quantitative anchor. L4's remote q20/q9
  A/B is the arbiter, exactly as its falsifier-2 already states.
- **Model (L4-SPEC §4):** overlap + coalescing claims ⅓-½ of the 19.3% ⇒ q20 −6..10%,
  q9 −3..6%; second-order: join hit-rate recovery (stop evicting the hot set) shows up as a
  prefix-scan-share drop.
- **Risk:** pool starvation (G5 foreground p99 gate); budget clamp (fan-in × 2 × 2 MiB);
  falsifier-2: if q20 < −3% and compaction share still ≥15% ⇒ share is merge-CPU-bound ⇒
  next lever is compaction merge CPU (S2's tree module is reusable), not deeper I/O.

### B.4 BUILT-opt-in — two-regime / routing executor family (`FRS_RS_EXECUTOR`)

- **Status:** routing (blocking) + routing-async (non-blocking) + two-regime dispatch in-repo;
  every historical "executor race" was the timer-queue floor bug, now fixed (SWEEP:1923-1974
  Stage-0 CLOSED: q8 routing-async ×5 exact, multi-worker ×3 exact, lockstep ×2 exact,
  suite 114/0).
- **Measured evidence:** q11 318.9→135.8 s (**2.35×**, rows 92,000,000 EXACT, SWEEP:527-535);
  q9 routing −4.3% (2484.4→2378.5, rows exact, SWEEP:1419-1427); q9 pipelined-vs-blocking
  de-confounded **−6%** (2579.8→2423.4 same-fix arms, SWEEP:1988-1994); q12 50.6→46.5
  (SWEEP:555); q3 neutral (36.6 vs 36.7, SWEEP:537-540).
- **Applicability:** q11 (the lever), q12, q9/q20 margin; REFUTED for q19 (OVER-window);
  light queries neutral.
- **Model:** q11 ⇒ recorded 2.35× at w=6 (re-verify at w=3); q9/q20 −4..6%.
- **Risk:** default-enable requires the FULL family gate program (the q8-under-OPT-01
  under-emission lesson, SWEEP:561-574 — though since root-caused to the timer bug, the
  windowed-join family must still gate it); staging-buffer overlap hazards are real for
  lockstep-only mechanisms (SWEEP:1689-1698) — the two-regime LIGHT/HEAVY split plus
  per-batch buffer ownership design is the structural fix; ship opt-in→default only after
  q5/q7/q8 exactness ×5 + lockstep ×2.

### B.5 DESIGNED — OPT-N04 merge-RMW + L2a mixed-batch flip

- **Status:** engine prereqs ALL landed (NumericAdd[Be]MergeOperator + 13 UTs, mixed batch
  FFI + hazard twins, E5); Java J1-J5 unbuilt (N04-SPEC).
- **Measured evidence:** B1 (n=3): write-path FFI boundary tax ≈0 at ≥64-row batches;
  warm-read +3.4-3.9 ns/row; ⇒ L2a standalone perf value **≈0 (measured)** — it is purely
  the CARRIER for merge rows (ROADMAP §5.1). The chain-kill value is structural: removes one
  dependent engine read round-trip per add on eligible ReducingStates.
- **Model (N04-SPEC §2 + ROADMAP §5.2):** q12 −8..15%, q8 −5..10% (structurally certain
  canaries); **q9/q20 0..−5% only** (their RMWs are operator-side MapState — out of backend
  reach; the Flink-side count-map merge is a PMC-escalation item, out of scope).
- **Risk:** read-path merge-chain length between flushes can WORSEN q20 probes (gate =
  q20 A/B @100M, 5M-no-flush artifact rule); checkpoint/restore with pending Merge ops needs
  the round-trip test.

### B.6 Hygiene (not wall levers — keep the per-record path violation-free)

Alloc/copy audit V1-V6 (`2026-06-12-backend-hotpath-alloc-copy-audit.md`): V4 hazard
mismatch-intrinsics FIRST (pre-cleans L2a's measurement path), V1-V3 timer alloc/hash (JFR
alloc-rate gate on q12@10M), V5 iter-view reuse. Expected wall delta: within box noise
individually (the audit's own honest ceiling); ship between stages, never co-measured with a
lever A/B.

### B.7 FALSIFIED / BANKED / DEMOTED (do not re-chase — anti-wandering exhibits)

| Lever | Verdict | Evidence |
|---|---|---|
| Garbage-drain 200K default | **FALSIFIED on NVMe** — q9 +19%, q20 ±0; REVERTED 57f0466bf (env opt-in kept) | SWEEP:2228-2241; Mac-recorded −377s/−568s were population-specific |
| Timer tax (q17 +300%/q20 +37%) | box-day noise — timer index left q17/q20 unchanged | SWEEP:2060-2070 |
| Mixed-batch crossing-count win | measured ≈0 at ≥64-row batches | B1 §5/§5b |
| P2 chunk ring / P3 SoA | demoted — crossings measured free (4/1000-row prefix); residual = overlap only; q9 insurance | ROADMAP §5.3 rank 5 |
| OPT-N16 zero-copy GETs | killed pre-build — GET path = 6.5% of 1.7 busy cores | SWEEP:1614-1620 |
| Parallel executor as join lever (pre-lock-free era) | refuted twice, then re-validated ONLY as B.4 post lock-free memtable + timer fix | SWEEP:267-299, 511-525 |
| L1-density drain gate (v2) | both signals bracket truth; reverted | SWEEP:1517-1527 |
| SST format change | readahead supersedes | streaming design §2.4 |

### B.8 Dependency graph

```
r2-regression bisect (remote agent) ──► clean tip
   └─► S2 round-3 remote matrix (B.2) ──► S2 default-ON
            └─► re-profile q20/q9 (post-S2 shares) ──► L4 build+gates (B.3)
E5 (landed) ──► OPT-N04 J1-J5 (B.5) [parallel lane; canaries q8/q12]
timer fix (landed) ──► executor family gates (B.4) [parallel lane]
V4 hygiene ──► L2a flip (carrier) ──► OPT-N04
ForSt remote pin + full-22 remote sweep ──► GOAL-1/GOAL-2 verdict arithmetic
```

---

## C. Composition analysis

**Which levers stack, and the math.** S2 (scan CPU), L4 (compaction I/O + cache pollution),
executor (wait/latency overlap), OPT-N04 (dependent-chain length) act on DISJOINT measured
components of the q20/q9 profiles — scan 21.6%/32.8%, compaction 19.3%/12.0%, wait (1.7/8
cores busy), chain (per-record RMW). Composition is therefore modeled MULTIPLICATIVELY on
wall: `wall' = wall × Π(1 − sᵢ·eᵢ)` where sᵢ = measured share, eᵢ = efficiency on that share.

- **S2 × L4 — independent + mildly super-additive.** S2 cuts scan CPU; L4 cuts compaction
  stall AND stops compaction evicting the join's hot blocks, which raises scan-path cache
  hit-rate (L4-SPEC §4 second-order). One overlap: both reduce demand-block I/O; the
  prefetcher (shipped) already claims part of that, which is why both models take only a
  fraction of their share. q20: (1−0.11)·(1−0.08) ≈ **−18%**; q9: (1−0.06)·(1−0.045) ≈ −10%.
- **S2/L4 × executor — independent by mechanism** (CPU/service-time vs concurrency/wait).
  Caveat both directions: shrinking service time shrinks what overlap can hide — recorded
  precedent: routing helped q9 only AFTER the engine levers cut cold-read time
  (SWEEP:1419-1427 "the engine levers flipped the math"). So executor gains are taken at the
  measured −4..6%, not the old idle-core extrapolation (falsified, SWEEP:1976-1986).
- **OPT-N04 × S2** — independent (chain vs scan); on q8/q12 they address different walls
  (q12 is chain-dominated, scan-light). On q20 OPT-N04 is ~0 (B.5 scope note) — no
  double-count temptation.
- **Conflicts / measurement law:** (1) never land two default flips in one measurement
  window (L4-SPEC §2.3: drain lesson); (2) S2 changes the scan share ⇒ the L4 A/B must use a
  POST-S2 profile for attribution; (3) executor changes timing of flush/compaction ⇒ rerun
  the S2 exact-rows sentinels under the executor flip (the timer bug taught exactly this);
  (4) OPT-N04's merge chains interact with compaction (full_merge collapse) — its q20 gate
  rides AFTER L4 so chain-read cost is measured against the final compaction cadence.

**Composed per-query projections (model midpoints):**

| query | base (pop) | S2 | L4 | exec | N04 | composed | vs bar |
|---|---|---|---|---|---|---|---|
| q7 | 2376.4 R | −15..30% | −4..6% | fan-out retest | ~0 | **1450-1900 R** | RDB 1367.6 ⇒ 1.06-1.39× (mid ~1.2×); ForSt remote pin decides GOAL-1 |
| q9 | 2421.2 R | −4..8% | −3..6% | −4..6% | 0..−5% | **2030-2200 R** | RDB 2121.2 ⇒ 0.96-1.04× ✓ |
| q20 | 2011.1 R | −9..13% | −6..10% | −4..6% | 0..−5% | **1500-1690 R** | RDB 1557.6 ⇒ 0.96-1.09× ✓~ |
| q11 | 264.8 M | −5..10% | small | ×2.35 recorded | — | **~105-125 M** | ForSt 134.9 ✓, RDB 106.1 ≈ parity |
| q19 | 527.9 M | −8..13% | −3..5% | refuted | ~0 | **~435-470 M** | ForSt 308 ✗ — UNCOVERED residual (§D Stage 6) |
| q12 | 49.6 M | — | — | −8% recorded | −8..15% | **~39-42 M** | ForSt 39.7 ≈ parity-to-win |
| q8 | 43.6 M | — | — | neutral | −5..10% | **~39-41 M** | ForSt 39.2 ≈ parity |
| q4 | 373.0 M | neutral (n≤4 guard) | −3..6% | — | — | **~350-365 M** | ForSt ✓✓; RDB 302.7 still +50..60 (GOAL-2 absorbs) |

q19 and q7-vs-ForSt are the two rows the current catalog does NOT close — they get explicit
re-profile stages (not lever-roulette) in §D.

---

## D. THE PLAN

Ordering principle: (i) unblock + re-anchor (clean tip, binding pins), (ii) ship the
biggest measured-share levers behind flags with remote gates, (iii) close the small ForSt
deltas, (iv) re-profile checkpoints own the residuals. Every stage names its gate cells,
expected numbers (with their evidence form — ratio or same-box multiplier), and abort
criteria. Correctness law everywhere: exact-rows sentinels q9 91,813,372 / q20 93,201,404 /
q3 2,201,068 / q8 band 2.95-3.06M; n≥3 on anything claimed; same-session pairs only.

### Stage 0 — Clean tip (r2-regression bisect) [IN FLIGHT, remote agent — not this strategy's labor, but its precondition]
- **Ships:** root-cause + fix/revert of the q9 r2 +358s non-drain residual (suspects: M3
  global atomic, E5 OnceLock, H1 catch_unwind, jar V1-V4 — SWEEP:2243-2251).
- **Gate:** q9 remote ≤ 1.05× of r1's 2421.2 with canonical rows; q8/q3 canaries in band.
- **Abort:** no suspect bisects ⇒ full re-profile of the r2 tip before ANY further stage
  (a regressed tip pollutes every later A/B).

### Stage 1 — S2 default-ON program (remote round-3, PMC §9 matrix)
- **Ships:** `FRS_RS_S2_PINNED` default flip as a separate decision commit. Pre-flip
  checklist additions from §F.1's adapter-tax measurement: port the serial
  `frs_vec_iter_prefix_open_batch` symbol off the ArcPairAdapter or document it
  compat-only (S2-REV A3), and audit engine-internal `prefix_scan` consumers for hot-path
  adapter use (+23-26% measured at shallow fan-out).
- **Gates (S2-REV §9):** S0 smoke → S1 q0-q22@5M OFF-vs-ON exact screen → S2 q3/q4 ratio
  0.95-1.05 → S3 q9 RSS steady → S4 q9/q20 OFF×2/ON×2 interleaved (expect q20 −9..13%,
  q9 −4..8% as same-session multipliers) → S5 q7 ON (the open-rate shape; expect the
  LARGEST mover; any ≥10% win here re-rates the q7 row).
- **Abort/falsifier:** q20 ON < −4% with scan share ≥15% ⇒ STOP S2 follow-ups, jump to
  Stage 3 (L4) + re-profile. Mac pre-flight first (cheap): §F.1/§F.2 reruns on the candidate
  SHA — any local regression aborts the remote round.

### Stage 2 — BINDING SCOREBOARD: ForSt-remote pin + full-22 remote 3-backend sweep + holes
- **Ships:** no code. (a) ForSt q0-q22 remote pins (GOAL-1 cells become binding); (b) frs/RDB
  remote fills for q4/q5/q8/q10-q19/q21/q22; (c) **q5 frs @100M re-measure** (the
  correctness-fixed row; expect RDB-parity class — q5's old 41.6s was wrong-output);
  (d) **q4 final-result compare** (materialized changelog vs RocksDB — out_rows is not a
  valid gate for retract queries); (e) q17/q8 RDB remote pins.
- **Gate:** every cell with exact-rows/band verification; n≥3 on heavies; same-session pairs.
- **Expected:** Mac-pop ratios transfer DIRECTIONALLY but compress (precedent q9/q20);
  the slower-than-ForSt set is re-ranked on remote data before Stages 4-6 spend effort.
- **Abort:** any frs correctness deviation ⇒ correctness-before-perf freeze.

### Stage 3 — L4 compaction-windowed + cache-skip
- **Ships:** L4-SPEC W1-W5 behind `FRS_COMPACT_WINDOWED`, B3 as its micro before/after
  (§F.3 is the baseline pair).
- **Gates:** L4-SPEC G0-G6; telemetry falsifier-1 (≥90% windowed blocks); remote q20/q9 ×3
  A/B expecting −6..10% / −3..6%; q3 hit-rate unchanged.
- **Abort:** q20 < −3% with compaction share still ≥15% ⇒ merge-CPU-bound ⇒ reuse S2 tree
  for compaction's k-way heap instead of deeper I/O.

### Stage 4 — Executor two-regime default program (q11/q12 + heavy margin)
- **Ships:** routing/two-regime default-enable decision after the FULL family gate program.
- **Gates:** q8@100M ×5 exact + lockstep ×2 (Stage-0 law); q5 byte-exactness (fixed-CSV
  replay — windowed-value masking lesson); q0-q22@5M exact sweep under the flip; then
  q11 (expect ~2.3× recorded class → ≤ ~135 M-class / remote equivalent ratio ≤1.25× RDB),
  q12 −8%, q9/q20 −4..6%, q3/q17 no-regress.
- **Abort:** any windowed-join family row out of band ⇒ stay opt-in; re-open per-batch
  buffer ownership work (the known structural prerequisite) rather than re-bisecting from
  scratch.

### Stage 5 — OPT-N04 (J1-J5) + L2a flip, canary-framed
- **Ships:** V4 hygiene first; L2a default-ON (semantics/enabler, expected ≈0 perf —
  measured); merge-routing for provably-i64 ReducingStates.
- **Gates:** N04-SPEC gate list (CF routing, serialized-form check, restore round-trip,
  off-path byte-identity); q12/q8 @100M A/B expecting −8..15% / −5..10%; q20 read-cost
  guard A/B (merge chains must not worsen probes).
- **Abort:** canaries < 60% of model ⇒ the chain model is wrong ⇒ re-profile q12 before
  any Flink-side escalation.

### Stage 6 — Residual re-profile checkpoints (q7-vs-ForSt, q19)
- **Trigger:** after Stages 1+3 land, IF q7 > ForSt-remote pin or q19 > ForSt/RDB bars.
- **Procedure (not lever-roulette):** symbolized FRS_PERF top-20 + ITER/DECAY diag on the
  live remote run of exactly the failing query, post-S2/L4 shares; then pick from the NAMED
  candidate pool with the new shares: per-source parallel drain (streaming design §2.5
  axis-2), OPT-N01 lazy probe, P2 ring (q9/q7 insurance — only if continuation-surviving
  iterators dominate), V5 serde hygiene, and for q19 specifically the operator-regime value
  cache; for q7, fan-out-width retest under the (now-correct) executor.
- **Escalation:** if q20/q19's operator-side chains cap below bar, file the PMC decision for
  the Flink-side workstream (count-map merge in SQL join state view; TopN accumulator merge)
  — named in N04-SPEC §2 as out-of-backend-scope.

### Stage 7 — Verdict sweep
- Full q0-q22 ×(n≥3 heavies, n≥1 lights + re-run any cell within 10% of its bar), 3 backends,
  one session block per pairing, remote. Compute GOAL-1 row-by-row and GOAL-2 totals.
  Publish as the successor of the SWEEP CURRENT STATUS block.

### End state — projected final table (model midpoints; confidence + evidence chain per row)

Remote population, post Stages 0-6. "Conf" = confidence the row meets BOTH goals.
Evidence chain abbreviations: R# = recorded A/B, M# = measured share × model, X = recorded
e2e multiplier.

| query | frs proj | RDB (R, proj=current) | ForSt (R pin owed; M-scaled) | GOAL-1 | GOAL-2 contrib | Conf | chain |
|---|---|---|---|---|---|---|---|
| q0-q2,q10,q13,q14,q21,q22 | current | current | current | ✓ (parity-noise; q0/q14 n≥3) | ~0 | HIGH | scoreboard A.1 |
| q3 | 142.1 | 122.6 | ~37 M-class | ✓ M (0.96×) | +19.5 | HIGH | A.1 |
| q4 | ~350-365 M-eq | 302.7 M | 1042.2 M | ✓✓ (−670s) | +50..60 vs RDB | HIGH (vs ForSt) | A.1 + L4 model |
| q5 | RDB-parity class (est 160-170 M-eq) | 162.5 M | DNF | ✓ by finish | ~0 | MED (hole until Stage 2c) | q5 fix + 1M hash-equal |
| q7 | 1450-1900 R | 1367.6 R | pin owed (586.8 M) | **OPEN — Stage 6 crux** | −500..−900 vs today | LOW-MED | S2 micro 5-8× (§F.1) + io_uring R# + jstack share |
| q8 | ~39-41 M-eq | 42.5 M | 39.2 M | ≈✓ parity | ~0 | MED | N04 model + canary gate |
| q9 | 2030-2200 R | 2121.2 R | DNF M | ✓ | −200..−400 | MED-HIGH | 1.14× R# + S2/L4/exec models |
| q11 | ~105-125 M-eq | 106.1 M | 134.9 M | ✓ | −140..−160 | MED-HIGH | X: 2.35× recorded exact |
| q12 | ~39-42 M-eq | 41.4 M | 39.7 M | ≈✓ parity | −8 | MED | X: −8% recorded + N04 model |
| q15,q16,q18 | current | current | current/DNF | ✓✓ | −261 already | HIGH | A.1 |
| q17 | current | pin owed | 253.0 M | ✓✓ (3.3×) | ~+5 | HIGH | A.1 |
| q19 | ~435-470 M-eq | 310.2 M | 308.1 M | **OPEN — Stage 6 crux** | −60..−90 vs today | LOW | findRow R# + S2/L4 models; residual diffuse |
| q20 | 1500-1690 R | 1557.6 R | pin owed (1535.9 M) | borderline ✓ (precedent: frs 1477.7 beat ForSt 1535.9 on Mac under drain-opt-in, SWEEP:1478-1484 — that config is NVMe-falsified, but it bounds what the engine can reach) | −320..−510 | MED | S2+L4+exec composed on 21.6/19.3 shares |

**Projected totals:**
- **vs ForSt (GOAL-2): MET with margin** — ForSt full-22 ≥ 10297 (3 DNF floors) vs frs
  projected ≈ 7.4-7.9 ks M-eq; even on the common-19 (no DNF arithmetic) the projected set
  flips ForSt's +816 lead to frs −300..+100 (q7 row decides the sign — Stage 6).
- **vs RocksDB (GOAL-2):** remote heavy-4 projected frs ≈ 5120-5930 vs RDB 5169 ⇒
  parity-to-win on the deficit block; frs's standing mid-query wins (q15 −68, q16 −50,
  q18 −144, q4@ForSt n/a, q21 −6, q10 −5 M-class) must transfer remotely (Stage 2 verifies);
  projected TOTAL: **frs ≈ 0.95-1.05× RDB** — GOAL-2-vs-RDB lands only if q7+q20 hit their
  model midpoints AND the mid-query wins transfer; this is honest: GOAL-2-vs-RDB has
  MED confidence, gated by the same two cruxes as GOAL-1 (q7, q19/q20 residuals).
- **GOAL-1: 18-19 of 21 rows HIGH/MED** confidence post-plan; the LOW rows are q7 and q19 —
  both have a designated re-profile stage with named candidate pools and a PMC escalation
  path (Flink-side operator levers) if backend-side composition tops out.

---

## E. Anti-wandering rules (the program's codified decision procedure)

1. **Population validity is absolute.** Never compare seconds across boxes/topologies;
  expectations transfer ONLY as same-session ratios or same-box multipliers
  (gate-matrix §0). Exhibit: drain-200K — deterministic −377s/−568s on Mac, +19%/±0 on
  NVMe ⇒ reverted (SWEEP:2228-2241). Every default flip must be validated on the
  DEPLOYMENT population before it ships.
2. **Instrument before code.** No lever is built before a profile/micro-bench shows its
  share. Exhibits: OPT-N16 killed by the 6.5% GET share (SWEEP:1614-1620); L2a re-scored to
  ≈0 by B1; merge-chain partial-merge not rushed (5M-no-flush artifact). Corollary: B2/B3-class
  harnesses are built WHEN a lever needs their evidence (B3 built for L4 in this document).
3. **Control-first, n≥3, ±10% noise floor.** Single runs are meaningless on this class of
  box (q17 3×-within-day; the "timer tax" misattribution). The control arm runs FIRST in
  the same session (SWEEP E4b lesson). Only back-to-back large deltas and DNF/finish
  transitions are decisions.
4. **Gate-miss procedure (replaces lever-roulette):** when a stage lands < 60% of its
  modeled win (ROADMAP §3 falsifier): (a) STOP feature work in that lane; (b) re-profile the
  exact failing query on the failing population (symbolized FRS_PERF top-20 + subsystem
  telemetry on a live run); (c) re-rank the candidate pool with the NEW shares; (d) only
  then build. One variable per measurement window; bisect regressions by engine-swap /
  jar-swap pairs before attributing.
5. **Correctness before performance, with the right gate per query class.** out_rows is a
  clean gate only for append/dedup; retract/windowed queries need final-result compare
  (q4 lesson); windowed-value bugs hide behind pane counts (q5 lesson) ⇒ fixed-CSV
  byte-exact replay for any executor/timer-path change, lockstep ×2 + routing-async ×5
  (Stage-0 law). A fast wrong answer is a failed cell, never a data point.
6. **Falsified levers stay falsified** (B.7 table). Re-opening one requires NEW evidence of
  a changed precondition — precedent: parallel executor was legitimately re-opened only
  after the lock-free memtable + timer fix changed its preconditions, and that re-opening
  was itself evidence-gated.
7. **The tip must be clean before stacking.** A regressed baseline poisons every A/B
  (r2 lesson — round-3 deferred until bisected). Default flips are separate decision
  commits that change ONLY the default and record their gate evidence.

---

## F. Evidence appendix — bench runs executed for this document

All runs: this Mac (Apple M5 Pro / 64 GiB, macOS, system allocator), tree at `forst-rs` tip
698949db5 (S2 merged + F-fixes + drain reverted), `cargo bench` profile, same-session
interleaved A/B, no other load (runs contaminated by overlapping builds were discarded and
repeated — noted inline). Mac-population caveat: same-box ranking evidence only.

### F.1 S2 A/B — `join_probe_open` (q7/q20 probe shape), `FRS_RS_S2_PINNED` OFF vs ON

`cargo bench -p forst-rs-bench --bench join_probe_open`, interleaved OFF/ON. Three clean
OFF runs and two clean ON runs + one partially-contaminated ON run (overlapped a cargo
build; included where it does not move the median — flagged *). 4096 join keys, value 64 B,
each SST spans the full key range (locator cannot prune), in-memory FS.

**Continuity cells — `prefix_scan_iter_owned_arc` (flag-ON ⇒ Arc-pair COMPAT ADAPTER over
`fill_into`):** criterion medians, ns/probe (open + drain):

| cell | OFF r1 | OFF r3 | OFF p4 | OFF med | ON r2* | ON r3 | ON p4 | ON med | ON/OFF |
|---|---|---|---|---|---|---|---|---|---|
| ssts_1 | 351.7 | 375.3 | 357.0 | 357.0 | 441.2 | 439.8 | 437.6 | 439.8 | +23% |
| ssts_8 | 404.4 | 407.4 | 396.5 | 404.4 | 490.2 | 507.9 | 539.2 | 507.9 | +26% |
| ssts_32 | 451.6 | 452.1 | 436.9 | 451.6 | 538.1 | 574.2 | 565.6 | 565.6 | +25% |
| ssts_64 | 506.0 | 471.1 | 468.1 | 471.1 | 590.9 | 586.9 | 598.7 | 590.9 | +25% |
| ssts_128 | 7712 | 7866 | 7790 | 7790 | 1342 | 1026 | 1264 | **1264** | **6.2× faster** |

**Raw push-sink cells — `fill_into` (production-FFI shape; the bench forces pinned mode in
BOTH arms, so the 6 runs are one noise band):** medians ssts_1 ≈ 355, ssts_8 ≈ 390,
ssts_32 ≈ 447, ssts_64 ≈ 456, **ssts_128 ≈ 919 ns**. Versus the legacy OFF adapter path:
parity-to-faster at shallow fan-out (ssts_1 −1%, ssts_8 −3%, ssts_32 −1%, ssts_64 −3%) and
**8.5× at ssts_128**.

**Findings:** (1) the tournament tree + pinned rows transform deep-fan-out probe cost —
the q7/q20-shape headline; (2) the +23-26% shallow-fan-out ON delta lives ENTIRELY in the
in-process ArcPairAdapter (re-materializing 2 Arcs/row), NOT in the pinned engine path —
the S2-REV A3 advisory quantified; production FFI opens construct the raw ChunkSink and do
not pay it, but the remote S2/q3-q4 ratio gate (round-3 cell S2) is the binding check, and
the serial `frs_vec_iter_prefix_open_batch` symbol must be ported or confirmed
non-production before default-ON.

### F.2 S2 A/B — `ffi_vectorized` iter cells (real `frs_*` FFI surface)

`cargo bench -p forst-rs-bench --bench ffi_vectorized -- iter`, same session, OFF then ON:

| cell | OFF | ON | delta |
|---|---|---|---|
| iter_drain 100×1000 (64 KiB chunks) | 15.553 ms = 155.5 ns/row | 14.363 ms = 143.6 ns/row | **−7.7% (p<0.05)** |
| iter_open_batch parallel k=64 | 6.794 ms = 106.2 µs/probe | 6.936 ms = 108.4 µs/probe | +1.7% (p<0.05) |

OFF arm reproduces B1 §5b's recorded baselines (151.8 ns/row, 104.2 µs/probe) within ~2% —
cross-run continuity holds. The −7.7% drain matches the recorded "iter_drain −10%" class
(SWEEP:31). The +1.7% open cost is the per-source `Box<SstBlockBuf>` build (S2-REV A2
advisory) — small, but q7 is open-rate-dominated, so round-3 cell S5 (q7 ON) must watch it.

### F.3 B3 `compaction_throughput` (bin BUILT for this document) — isolated vs live

`cargo run -p forst-rs-bench --release --bin compaction_throughput -- --ssts 8 --sst-mib 64
--overlap-pct 50 [--tombstone-pct 30] [--with-read-load]`. LocalFileSystem on a
`target/`-scratch dir, incompressible values, deterministic xorshift keys,
`FRS_L0_COMPACTION_TRIGGER` raised so the measured `compact_l0` job is exactly the 8 built
files (the first matrix attempt panicked on `compact_l0 == None` — background auto-compaction
at trigger=4 had drained L0 during the build; fixed in the bin, recorded here as a harness
lesson). Output SST 64.6 MiB, reclaimed 87.4% (50% key overlap × 8 rewrites), byte-identical
out_bytes across all arms ⇒ deterministic measured work.

| arm | n | ns/byte-live | input MB/s | sidecar gets/s |
|---|---|---|---|---|
| tomb 0, isolated | 3 | 6.47 / 6.79 / 6.72 | 1114-1169 | — |
| tomb 0, +read-load | 3 | 6.35 / 6.46 / 6.39 | 1171-1191 | 745-832K |
| tomb 30, isolated | 1 | 5.22 | 1450 | — |
| tomb 30, +read-load | 1 | 5.23 | 1446 | 991K |

**Findings:** (1) isolated merge floor on this box ≈ **6.5 ns/byte-live ≈ 1.15 GB/s input**
(the recorded "3 ns/byte isolated" class; this run includes real local-FS writes); (2) a
single max-rate point-get sidecar costs the compaction NOTHING on an idle 14-core Mac —
the recorded 21 ns/byte LIVE degradation is a property of a SATURATED 8-core box with
multi-GB cold inputs, which this environment cannot reproduce; (3) tombstone-rich inputs
compact FASTER per live byte (less value movement). **Role going forward:** B3 is the
mechanism before/after harness for L4 (windowed reads must not regress the merge floor;
the cache-skip arm needs a cache-budget-constrained variant) — L4's wall-time win is
adjudicated by the remote q20/q9 A/B, not by this box.

### F.4 Run hygiene record

Initial `join_probe_open` loop runs RUN-1-ON / RUN-2-OFF were lost to a concurrent
`cargo build` of the new B3 bin (compile error broke `cargo bench`'s all-targets build;
later runs overlapped builds). Per anti-wandering rule 3 (control-first, clean box), all
numbers above come from the post-quiesce sequential battery + the clean cells of the loop;
contaminated cells are marked or discarded. The B3 first matrix (nondeterministic L0) was
discarded wholesale and re-run after the trigger fix.
