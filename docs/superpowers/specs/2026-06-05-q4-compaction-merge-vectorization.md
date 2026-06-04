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

## Infra note (measurement harness)
`measure-sql.sh`'s stop+start cluster restart became flaky after ~15 q4 runs this session (TM failed to
register → `taskmanagers:0, slots:0` → job admitted RUNNING but all vertices stuck CREATED → src_out=0, NOT
a code/dylib fault: a manual hard-reset brought the TM up with 4 slots on the SAME dylib). Robust harness:
hard-reset (pkill all flink procs + clean `/tmp/flink-forst-rs-*` + `/tmp/nexmark-checkpoints-forst-rs`),
`start-cluster`, verify `slots-total=4` via `:8081/overview`, start sql-gateway, then submit via
`run_query.sh oa q4` with **FLINK_HOME/NEXMARK_HOME/JAVA_HOME/HADOOP_CLASSPATH exported** (run_query.sh
hard-requires FLINK_HOME in env). Reuse the running cluster across iterations rather than restarting.
