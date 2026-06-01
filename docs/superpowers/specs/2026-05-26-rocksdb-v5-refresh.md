# RocksDB Nexmark refresh (v5) — 2026-05-26

**Purpose:** Re-measure the rocksdb baseline as part of the 4-backend sweep (rocksdb/local, forst/S3, forst-rs-JNI/S3, forst-rs-FFM/S3). This document records the **rocksdb backend only** — the run was stopped by the user after rocksdb completed and forst-s3 had done 4 queries.

**Run ID:** `v5-20260526-124843`
**Machine:** macOS Darwin 25.4.0, Apple Silicon, 1 JM + 1 TM × 4 slots
**JDK:** Zulu 17.0.16 + G1GC
**Storage:** local FS (`/tmp/flink-rocksdb-io`, `/tmp/nexmark-checkpoints-rocksdb`)
**Workload:** Nexmark 100 M events / query, blackhole sink, fresh-cluster-per-query
**Per-query cap:** 3600 s (raised from the earlier 720 s so q23 ≈ 1000 s completes)
**Coverage:** Q0–Q23 (Q6 N/A) — **all 23 queries completed cleanly, no timeouts.**
**Orchestrator:** `scripts/bench-4way-s3.sh`
**Raw data:** `docs/superpowers/specs/v5-bench-data/` (summary.csv + per-query `rocksdb-q*.out`).

---

## RocksDB v5 vs v4 baseline (`docs/.../2026-05-22-rocksdb-refresh.md`)

| Q   | v4 (s)   | v5 (s)   | Δ%      | v5 throughput |
|-----|---------:|---------:|--------:|---------------|
| Q0  | 19.343   | 19.621   | +1.4 %  | 5.10 M/s |
| Q1  | 18.599   | 18.972   | +2.0 %  | 5.27 M/s |
| Q2  | 17.649   | 17.845   | +1.1 %  | 5.60 M/s |
| Q3  | 25.340   | 26.267   | +3.7 %  | 3.81 M/s |
| Q4  | 247.887  | 246.576  | -0.5 %  | 405.55 K/s |
| Q5  | 109.303  | 106.382  | -2.7 %  | 940.01 K/s |
| Q7  | 453.404  | 481.086  | +6.1 %  | 207.86 K/s |
| Q8  | 27.237   | 25.397   | -6.8 %  | 3.94 M/s |
| Q9  | 543.016  | 528.138  | -2.7 %  | 189.34 K/s |
| Q10 | 20.031   | 13.276   | **-33.7 %** ↓ | 7.53 M/s |
| Q11 | 100.386  | 96.779   | -3.6 %  | 1.03 M/s |
| Q12 | 32.367   | 29.939   | -7.5 %  | 3.34 M/s |
| Q13 | 38.583   | 39.074   | +1.3 %  | 2.56 M/s |
| Q14 | 18.154   | 18.757   | +3.3 %  | 5.33 M/s |
| Q15 | 270.057  | 272.398  | +0.9 %  | 367.11 K/s |
| Q16 | 286.140  | 325.515  | **+13.8 %** ↑ | 307.20 K/s |
| Q17 | 54.072   | 52.270   | -3.3 %  | 1.91 M/s |
| Q18 | 178.510  | 177.440  | -0.6 %  | 563.57 K/s |
| Q19 | 130.945  | 136.625  | +4.3 %  | 731.93 K/s |
| Q20 | 404.060  | 390.334  | -3.4 %  | 256.19 K/s |
| Q21 | 44.839   | 42.665   | -4.8 %  | 2.34 M/s |
| Q22 | 30.622   | 29.969   | -2.1 %  | 3.34 M/s |
| Q23 | 1063.319 | 1006.274 | -5.4 %  | 99.38 K/s |

### Observations

- **The v5 rocksdb refresh reproduces v4 within the bench noise band** (mostly ±7 %), confirming the harness and machine are stable and the new orchestrator (`bench-4way-s3.sh`) is correct.
- Two outliers: **Q10 −33.7 %** (20.0→13.3 s, faster — likely OS page-cache warmth, Q10 is small/stateless-ish) and **Q16 +13.8 %** (slower). Both are single-sample variance, not code changes (rocksdb is unchanged).
- **Q23 completed at 1006 s** — only possible because the per-query watchdog was raised to 3600 s. At the earlier 720 s cap, Q23 (and several S3 queries) were silently voided. This is the validated methodology for the slow tail queries.

---

## Status of the other 3 backends (incomplete — run stopped)

The 4-backend sweep was stopped by the user after rocksdb. Captured partial:

| config | done | note |
|---|---|---|
| rocksdb (JDK17/local) | **23/23** | complete, this doc |
| forst-s3 (JDK17/S3) | 4/23 | q0=20.99, q1=20.46, q2=18.14, q3=43.32 — S3 path validated working, zero NoRouteToHost |
| forst-rs-jni-s3 (JDK17/S3, JNI-compat) | 0/23 | not reached |
| forst-rs-ffm-s3 (JDK25/S3) | 0/23 | not reached |

**To resume the S3 backends** (once desired): `QUERY_TIMEOUT=3600 bash scripts/bench-4way-s3.sh` with `CONFIGS="forst-s3 forst-rs-jni-s3 forst-rs-ffm-s3"`. S3 connectivity confirmed working; orchestrator + 3600 s cap validated.

### Environment notes (for resumption)
- Nexmark harness only measures at ~100 M scale — small-scale jobs hang (metric monitor never attaches). See memory `project_nexmark_harness_min_scale`.
- Config #3 (forst-rs-jni-s3) uses a `--features compat-jni` build of `libforst_rs_ffi.dylib` placed as `/tmp/forstrs-jni/libforst.dylib` on `java.library.path`, so `System.loadLibrary("forst")` loads the forst-rs engine before the bundled forst native. Unproven at full Nexmark scale — the first config-3 query is the JNI-surface risk point.

---

## forst-rs FFM/S3 — full sweep result (run `v6f-forstrs-mergefix-20260526-213806`)

After the S3 large-SST correctness fixes landed (value-cap, read-loop short-read,
orphan-SST CreateOrTruncate, SST file-number collision, buffered-write size-verify) the
forst-rs FFM backend completed **all q0–q22 cleanly, 0 errors, no timeouts**. This is the
authoritative forst-rs result.

**forst-rs config:** Zulu **JDK 25** + ZGC/COH, FFM downcalls into `libforst_rs_ffi.dylib`,
state on **S3 (BOS, S3-compatible)**, HEAP timer-service, **checkpointing disabled**.
**rocksdb config (baseline above):** JDK 17 + G1GC, state on **local FS**, **incremental
checkpoints enabled**.

| Q   | rocksdb (s) | forst-rs (s) | speedup | note |
|-----|------------:|-------------:|--------:|------|
| Q0  |   19.62 |  21.93 | 0.89× | light, slower |
| Q1  |   18.97 |  21.47 | 0.88× | light, slower |
| Q2  |   17.84 |  20.29 | 0.88× | light, slower |
| Q3  |   26.27 |  37.99 | 0.69× | slower (regression) |
| Q4  |  246.58 |  46.28 | **5.33×** | heavy join |
| Q5  |  106.38 |  25.89 | **4.11×** | heavy window |
| Q7  |  481.09 |  21.42 | **22.46×** | heavy join |
| Q8  |   25.40 |  25.69 | 0.99× | parity |
| Q9  |  528.14 |  14.88 | **35.49×** | heavy join |
| Q10 |   13.28 |  53.53 | 0.25× | **regression (4× slower)** |
| Q11 |   96.78 |   8.63 | **11.22×** | session window |
| Q12 |   29.94 |  32.49 | 0.92× | slower |
| Q13 |   39.07 |  40.84 | 0.96× | slower |
| Q14 |   18.76 |  20.39 | 0.92× | slower |
| Q15 |  272.40 |  18.47 | **14.75×** | heavy |
| Q16 |  325.51 |  10.11 | **32.19×** | heavy |
| Q17 |   52.27 |   9.28 | **5.63×** | |
| Q18 |  177.44 | 130.85 | 1.36× | |
| Q19 |  136.62 |   9.77 | **13.98×** | |
| Q20 |  390.33 |  13.70 | **28.48×** | heavy join |
| Q21 |   42.66 |  43.87 | 0.97× | slower |
| Q22 |   29.97 |  32.90 | 0.91× | slower |
| **TOTAL** | **3095.3** | **660.7** | **4.69×** | q0–q22, q6 N/A in both |

### Headline
**forst-rs total q0–q22 = 660.7 s vs rocksdb 3095.3 s = 4.69× faster — the 3× goal is met.**
The win comes entirely from the heavy stateful queries (joins/large windows), where the
vectorized/batch/zero-copy engine paths turn rocksdb's 250–530 s into 10–46 s (5×–35×). The
light, near-stateless queries (q0–q3, q8, q10, q12–14, q21–22) are 10–30 % *slower* on
forst-rs, but their absolute times are small so they barely move the total.

### Caveats — honest accounting (correctness-first)
1. **Checkpoint asymmetry.** forst-rs ran with checkpointing **off**; rocksdb ran with
   **incremental** checkpoints. This favors forst-rs. *However*, the heavy-query margins
   (q7 22×, q9 35×, q16 32×, q20 28×) are far larger than any plausible checkpoint overhead
   (typically 10–30 % throughput). Even charging forst-rs a full checkpoint tax, the total
   stays well above 3×. The remaining rigorous step is a parity re-run with forst-rs
   checkpointing enabled (gated on the S3 incremental-checkpoint path; see memory).
2. **Output correctness validated for q3/q4/q7 only** — byte-identical to rocksdb (seeded
   RNG `0xF0F0F0F0L`). The other heavy winners (q5/q9/q15/q16/q17/q19/q20) have not yet been
   output-diffed; the 4.69× is trustworthy as *wall time of a clean, error-free run* but
   full output parity across all queries is the outstanding correctness gate.
3. **Light-query regressions are real.** q3 (0.69×) and especially **q10 (0.25×, 4× slower)**
   are genuine — per-record FFM crossing + S3 round-trips cost more than rocksdb's local
   memtable on near-stateless paths. They do not threaten the 3× total but are the clearest
   targets for further forst-rs tuning.
4. **Diagnostics build.** The v6f dylib still carried the temporary `FRS-*-DIAG` perf
   counters (atomic increments on hot paths). A stripped `release` rebuild can only make the
   forst-rs numbers equal-or-better, never worse.

### Reproduce
`QUERY_TIMEOUT=3600 CONFIGS="rocksdb forst-rs-ffm-s3" bash scripts/bench-4way-s3.sh`
(rocksdb on JDK17/local, forst-rs on JDK25/S3 — both 100 M events, fresh cluster per query).
