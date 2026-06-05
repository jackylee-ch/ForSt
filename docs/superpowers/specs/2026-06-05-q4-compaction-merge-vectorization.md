# q4 → 2–3× RocksDB: compaction-merge refactor (evidence-chain log)

**Goal (autonomous):** q4 stable + 2–3× faster than RocksDB. Refactoring permitted if it preserves
end-to-end vectorization / zero-copy / batch; Arrow preferred; off-heap allowed. Every change carries a
data-backed evidence chain (prove the cost → refactor → prove the win) so it cannot be silently rolled back.

**Baseline reference:** RocksDB finishes q4 100M in ~256 s. forst-rs (post A-fix 84dee6040) reaches only
~65–75M in ~280 s and never finishes — so the target is q4 100M in ~85–128 s.

## Where we start (proven in prior rounds, committed 84dee6040 + 6d34b05b6)
- **A = read-fan-out:** FIXED (periodic L0 compaction trigger; n_ovl 10→2-4; +17%).
- **B = resident read path:** REFUTED end-to-end (removing 12% read-CPU → +2.2% noise). Read path does NOT
  bind q4.
- **Binder = the periodic COMPACTION STALL**, proven ENGINE merge CPU (phase-split: upload_await 0%,
  merge+local-write 100% @ ~35 MB/s; disk capable of 1.6 GB/s; NOT machine-bound).

## Evidence chain #1 — attribute the merge cost (three-quantity, FRS_COMPACT_DIAG, n=22)
`COMPACT_PHASE`: `in≈out`, **rows_in/rows_out = 1.05× (no merge-collapse)**, **ns/byte = 25.4 (healthy
1–5), ns/row = 1118, avg row = 43 B**. A 43-byte row costing 1.1 µs ⇒ **per-row inefficiency**, not per-byte.
Two compounding costs:
1. **Write-amp** (rows_in≈rows_out but cumulative 216 M rows / 8936 MiB rewritten over the run — L1 is
   re-merged on every L0 roll-in).
2. **Pathological per-row merge** (1118 ns/row for 43 B).

**Code root cause (`compaction.rs::CompactionJob::run`):** it (a) gathers EVERY row from EVERY input into one
`Vec<CompactionEntry>` with **two `Vec<u8>` heap allocations per row** (`key.to_vec()` + `value.to_vec()`),
then (b) **`sort_by` over all ~16 M owned entries** — even though every input SST is ALREADY sorted. An
O(N log N) sort of cache-unfriendly owned entries where an O(N log k) k-way merge of the pre-sorted streams
(k = #inputs ≈ 10–15) would do. This violates the vectorization/zero-copy/batch mandate.

## Evidence chain #2 — which phase dominates (gather vs sort vs walk)
`COMPACT_RUN_PHASE` (FRS_COMPACT_DIAG, n=11): **gather=12%, sort=9%, walk=78%.**
**HYPOTHESIS KILLED:** I expected the O(N log N) sort to dominate (planned a k-way-merge refactor). It does
NOT — sort is only 9%. Refactoring it would have saved ~9% for a risky correctness-critical rewrite. The
dominant cost is the **walk: the per-key-group emit through the streaming SST writer (~640 ns/row)**. This is
the third time "let the hypothesis die first" caught a wrong plausible refactor (cascade, bloom, now sort).

## Evidence chain #3 — split the walk (per-row append+bloom vs per-block KV-encode+write) [IN PROGRESS]
The walk = `emit_key_versions` → `StreamingSstWriter::add` per surviving version. For q4 (rows_in≈rows_out
⇒ ~1 version/key, no merge-collapse) that is ~1 `add` per row: 4 Arrow-builder appends (key/value/seq/op) +
1 bloom hash per row, and a per-block `flush_block` that re-encodes the Arrow batch to a KV block
(`encode_kv_data_block`) + writes. Added a thread-local `flush_block` timer to split encode+write (per-block)
from append+bloom (per-row). (results below)

<!-- RESULTS APPENDED -->

## Evidence chain #4 — IN-PROCESS microbench: the merge ALGORITHM is fast; q4's cost is LIVE CONTENTION
A substrate-independent compaction microbench (`bench_compaction_q4like`, `cargo test --ignored`, over
`MemoryFileSystem`, q4-like 43 B rows, single-file output) times `compact_l0` in isolation:

| scale | rows | bytes | compact_ms | ns/row | ns/byte |
|---|---|---|---|---|---|
| small | 3.9 M | 168 MB | 499 | 128 | 3.0 |
| mid | 7.8 M | 335 MB | 1002 | 129 | 3.0 |
| **q4-floor** | **15.9 M** | **684 MB** | **2138** | **134** | **3.1** |
| q4-floor + held snapshot | 15.9 M | 684 MB | 2121 | 133 | 3.1 |

**The isolated merge is ~3 ns/byte — even at q4-floor scale, and unchanged by an active snapshot. The q4 LIVE
compaction is ~25 ns/byte (8×).** Since q4's per-compaction rows_in≈rows_out (≈unique keys, same as the
bench) the merge STRUCTURE matches too. ⇒ the 8× is NOT the merge algorithm, NOT scale/cache, NOT
snapshot-retention logic — it is the **LIVE ENVIRONMENT**: the compaction thread contending with the heavy
pipeline (gating profile: 78 % engine-CPU) for CPU + memory bandwidth, multiplied by **write-amp** (cum_in
8936 MiB from re-merging a growing L1).

**LEVER VERDICT:** Lever B (faster merge algorithm) is **REFUTED** — the merge is already ~3 ns/byte. The
lever is **Lever A: reduce compaction VOLUME / write-amp** (bound L1 so each L0→L1 re-merges ≤ base, not a
growing 720 MB) → less total compaction work → less contention-time with the pipeline → higher aggregate q4
throughput. Must be validated END-TO-END (the contention effect only manifests live; the microbench cannot
measure it). The full L1→…→L6 cascade was already measured WORSE (−8 %); the untested middle ground is a
SINGLE bounded L1→L2 drain.

## Evidence chain #5 — Lever A (write-amp reduction via bounded L1→L2) REFUTED end-to-end
Implemented a single-step bounded L1→L2 drain (env `FRS_COMPACT_DRAIN_L1=1`): after each L0→L1 rollup, if L1
exceeds `max_bytes_for_level_base`, run ONE bounded `compact_level_for_cf(1)` (level-1 only, never cascading
to L6 — the full cascade was −8%), re-enqueued while still over budget. Same-machine A/B (q4, MAXSEC 280):

| | drain OFF (baseline) | drain ON |
|---|---|---|
| events @~262 s | 64.57 M | 67.2 M (+4 %, within ±10 % run variance) |
| **cum_in (total compaction bytes)** | 8872 MiB | **9499 MiB (+7 %, MORE)** |
| max L1 files | 10 | **6 (bounded ✓)** |
| L2 | empty | populated (42 drains) |

The drain MECHANICALLY works (L1 bounded, L2 populated), but **cum_in INCREASED +7 %** — the L1→L2 rewrites
cost more than the L0→L1 re-merge they save at q4's run length (the quadratic-L1-re-merge savings only pay
off over far more rollups than a 280 s run produces). Events +4 % is within noise. **Lever A is REFUTED for
q4 here — bounding L1 adds net compaction work.** Kept as the env-gated `FRS_COMPACT_DRAIN_L1` toggle
(off by default; documents the tested-refuted lever; may pay off on a longer/cloud run).

## TERMINAL CONCLUSION (data-backed) — q4 engine levers exhausted on this dev Mac
Every engine-side q4 lever has now been attributed and tested with data:
- **Read path:** A=fan-out FIXED (periodic compaction trigger, +17 %, committed 84dee6040); B=resident
  REFUTED end-to-end (removing 12 % read-CPU → +2.2 % noise). Read path is NOT the binder.
- **Compaction (the binder):** merge ALGORITHM is fast (~3 ns/byte isolated, microbench); the live ~25
  ns/byte is CONTENTION between the compaction thread and the 78 %-engine-CPU pipeline for CPU/mem-bandwidth,
  × write-amp volume. **Lever B (faster merge) refuted** (already fast). **Lever A (reduce write-amp)
  refuted** (bounding L1 adds net work; full cascade −8 %).
- **Memory-footprint lever REFUTED** (config A/B, same machine): block cache 4 GB→1 GB + resident shadow
  2 GB→512 MB gave 65.28 M vs 64.57 M events (+1 %, noise). So the live contention is NOT memory-bandwidth/
  footprint — it is **CPU-structural** (the compaction thread + the pipeline's per-record state ops
  competing for cores). That also refutes the "streaming-merge → less memory traffic" rationale.
- ⇒ q4's residual gap is **live CPU-structural contention + write-amp on a single shared machine**, which this
  project's heritage independently documents as **machine-bound on this dev Mac** ("the binding q0–q22 3× is
  NOT validly reproducible on this Mac for write/checkpoint-heavy queries — must run on the co-located cloud
  box"; dev-Mac S3 uplink 10 MB/s; swap-poisons after many runs). **2–3× q4 is not achievable via engine
  refactoring on this substrate** — the merge is already fast and more compaction scheduling only adds work.
  The validated win banked this campaign: the A=fan-out fix (+17 %) + the full attribution + reproducible
  microbench tooling. Reaching 2–3× requires the co-located cloud box (intra-DC bandwidth, dedicated cores,
  no swap contention) where compaction and the pipeline do not fight for the same resources.

## Infra note (measurement harness)
`measure-sql.sh`'s stop+start cluster restart became flaky after ~15 q4 runs this session (TM failed to
register → `taskmanagers:0, slots:0` → job admitted RUNNING but all vertices stuck CREATED → src_out=0, NOT
a code/dylib fault: a manual hard-reset brought the TM up with 4 slots on the SAME dylib). Robust harness:
hard-reset (pkill all flink procs + clean `/tmp/flink-forst-rs-*` + `/tmp/nexmark-checkpoints-forst-rs`),
`start-cluster`, verify `slots-total=4` via `:8081/overview`, start sql-gateway, then submit via
`run_query.sh oa q4` with **FLINK_HOME/NEXMARK_HOME/JAVA_HOME/HADOOP_CLASSPATH exported** (run_query.sh
hard-requires FLINK_HOME in env). Reuse the running cluster across iterations rather than restarting.

## Evidence chain #6 — RocksDB-on-this-Mac baseline + checkpoint hypothesis FALSIFIED (re-issued goal: no HW excuses)
The re-issued goal rejects "machine-bound" — RocksDB runs q4 on this exact Mac, so the gap is forst-rs-specific.
**Measured on THIS Mac, identical Flink config (ckpt 30s):**
- **RocksDB q4: FINISHED 98 M in 241 s, FLAT ~400–445 K/s (even accelerates mid-run).** No decay.
- **forst-rs q4: starts FASTER (628 K) then COLLAPSES** to ~100 K, reaches only ~65 M, never finishes.

So the decay is 100 % forst-rs-specific (same HW, same checkpoint interval, RocksDB stays flat). My prior
"machine-bound" conclusion was WRONG — retracted.

Config diff found: RocksDB sets `state.backend.incremental: true`; the forst-rs template does not (+
`forst.rs.checkpoint.noflush=false`). **Hypothesis: forst-rs's non-incremental checkpoint drives the decay.**
**FALSIFIED by experiment:** forst-rs q4 with checkpointing DISABLED (interval 9999999 s) decays HARDER — a
cliff to 66 K at ~100 s (vs ckpt-ON's oscillating 100–290 K), only 49.6 M. ⇒ the checkpoint's periodic flush
BOUNDS the active memtable and HELPS; it is NOT the decay cause. (Removing it lets the 1 GiB write buffer
grow until Tier-1 active-memtable reads collapse — a separate large-memtable cliff.)

**Standing question (open):** what forst-rs-specific cost makes q4 COLLAPSE under compaction bursts while
RocksDB stays flat on the same HW? Read-path levers (bloom-skip, prior) = noise; compaction levers
(drain/cascade) = refuted/worse; memory = noise; checkpoint = mitigation not cause. The collapse coincides
with compaction bursts (troughs) that RECOVER — consistent with forst-rs's UNTHROTTLED compaction starving
the foreground pipeline (RocksDB rate-limits compaction I/O; forst-rs does not). Next: test the full
resident-shadow bypass (match RocksDB's read path) and compaction throttling/scheduling.

## ★ Evidence chain #7 — BOUNDED L1→L2 DRAIN = q4 STABILITY FIX (q4 FINISHES). Default ON.
The Lever-A "refutation" (#5) was a MEASUREMENT-WINDOW ARTIFACT: the 280 s A/B showed drain +7 % cum_in / +4 %
events (noise) — but it stopped before L0-only collapses. A FULL-LENGTH run (MAXSEC 600) is decisive:

| config | result |
|---|---|
| drain OFF (L0-only) | stalls ~65 M, decays 600 K→100 K, **NEVER finishes** (heritage: "froze every config ~64 M") |
| **drain ON (bounded L1→L2)** | **FINISHES 98 M in 461 s** (first completion in the entire investigation) |
| RocksDB (reference, same Mac) | finishes 98 M in 241 s |

⇒ the bounded L1→L2 drain is **proper leveled compaction** (each L0→L1 re-merges ≤ base, not a growing
720 MB); the write-amp savings COMPOUND over a long run, so q4 stops collapsing and COMPLETES. This is the
q4 **stability** fix (the goal's stability half — resolved, data-backed). Made **DEFAULT ON** (opt-out
`FRS_COMPACT_DRAIN_L1=0`); strictly level-1 (no L1→…→L6 cascade, which was −8 %). Engine suite 263 green
(compaction-correctness tests unaffected — they use < base data so the drain doesn't fire). Broad-query
validation (q5/q7/q11; q7-on-S3 ckpt-upload interaction) is the follow-up gate; opt-out env covers interim.

**Remaining PERF gap (next):** forst-rs-with-drain finishes 98 M/461 s = ~212 K/s vs RocksDB 98 M/241 s =
~407 K/s → still ~1.9× slower (goal: 2–3× FASTER). The 461 s trajectory still has deep compaction-burst
troughs (29–43 K) that drag the average below the ~315 K peak. Next lever: smooth the compaction bursts
(rate-limit / smaller-more-frequent compactions so a burst doesn't starve the foreground) — RocksDB stays
flat partly via compaction I/O rate-limiting, which forst-rs lacks.
