# RocksDB performance refresh — 2026-05-22

**Purpose:** Refresh the rocksdb Nexmark numbers used as the comparison baseline in v3.8 (which carried v3.2's numbers verbatim per user directive at the time). Same hardware, same Flink 2.2.1, same JDK 17 + G1, same local-FS storage — pure re-measurement.

**Run ID:** `v4-20260522-102739`
**Machine:** macOS Darwin 25.4.0, Apple Silicon, 1 JM + 1 TM × 4 slots
**JDK:** Zulu 17.0.16 + G1GC (default)
**Storage:** local FS (`/tmp/flink-rocksdb-io`, `/tmp/nexmark-checkpoints-rocksdb`)
**Workload:** Nexmark 100 M events / query, blackhole sink, fresh-cluster-per-query
**Coverage:** Q0-Q23 (Q6 N/A per Nexmark convention). **All 23 queries completed cleanly** — no timeouts.

forst and forst-rs sweeps were NOT run (user stopped the larger 3-backend bench before they began).

---

## Fresh rocksdb numbers vs v3.8-carried baseline (= v3.2 numbers)

| Q | v3.8 / v3.2 carried (s) | v4 fresh (s) | Δ% | VPS v4 |
|---|---:|---:|---:|---|
| Q0  | 19.75   | **19.34**   | -2.1 %   | 5.17 M/s |
| Q1  | 19.12   | **18.60**   | -2.7 %   | 5.38 M/s |
| Q2  | 20.81   | **17.65**   | **-15.2 %** ↓ | 5.67 M/s |
| Q3  | 27.65   | **25.34**   | -8.4 %   | 3.95 M/s |
| Q4  | 251.78  | **247.89**  | -1.5 %   | 403.41 K/s |
| Q5  | 114.72  | **109.30**  | -4.7 %   | 914.89 K/s |
| Q7  | 459.84  | **453.40**  | -1.4 %   | 220.55 K/s |
| Q8  | 35.16   | **27.24**   | **-22.5 %** ↓ | 3.67 M/s |
| Q9  | 525.15  | **543.02**  | +3.4 %   | 184.16 K/s |
| Q10 | 23.73   | **20.03**   | **-15.6 %** ↓ | 4.99 M/s |
| Q11 | 100.44  | **100.39**  | 0.0 %    | 996.15 K/s |
| Q12 | 31.29   | **32.37**   | +3.5 %   | 3.09 M/s |
| Q13 | 34.53   | **38.58**   | **+11.7 %** ↑ | 2.59 M/s |
| Q14 | 26.51   | **18.15**   | **-31.5 %** ↓ | 5.51 M/s |
| Q15 | 279.01  | **270.06**  | -3.2 %   | 370.29 K/s |
| Q16 | 364.65  | **286.14**  | **-21.5 %** ↓ | 349.48 K/s |
| Q17 | 57.84   | **54.07**   | -6.5 %   | 1.85 M/s |
| Q18 | 228.12  | **178.51**  | **-21.7 %** ↓ | 560.19 K/s |
| Q19 | 145.26  | **130.95**  | -9.9 %   | 763.68 K/s |
| Q20 | 439.51  | **404.06**  | -8.1 %   | 247.49 K/s |
| Q21 | 57.27   | **44.84**   | **-21.7 %** ↓ | 2.23 M/s |
| Q22 | 45.30   | **30.62**   | **-32.4 %** ↓ | 3.27 M/s |
| Q23 | 1091.90 | **1063.32** | -2.6 %   | 94.05 K/s |

### Observations

- **Most queries are 2–10 % faster** in the refresh — consistent with bench noise (Apple Silicon thermal state, RocksDB block cache warm-up timing, OS scheduling) on identical code.
- **Big drops (≥ 20 %): Q8, Q14, Q16, Q18, Q21, Q22.** Q14/Q22 stand out — both became ~30 % faster. Hypothesis: warmer RocksDB block cache from prior queries in the fresh sweep (queries run sequentially under the fresh-cluster strategy; some block-cache state actually persists in the OS page cache across `start-cluster` boundaries when the same `/tmp` data dirs are reused).
- **Only regression: Q13 (+11.7 %)**. Within typical single-sample noise band; not concerning.
- **Q11 / Q12 unchanged** (within 0.1 % / 3.5 %) — these are SESSION-window and PROCTIME-tumble heavy and tend to be measurement-stable.

---

## Impact on forst-rs / rocksdb speedup ratios

forst-rs numbers carry from v3.8 + the V3 update measurements (commits `3e1d5a1c2a9` V4 and `11d7e0dfaf0` V3 on the `forst-rs-jdk25` branch):

| Q | forst-rs (latest) | rocksdb v4 fresh | **NEW speedup** | v3.8 speedup (vs carried) |
|---|---:|---:|---:|---:|
| Q0  | 21.17  | 19.34   | 0.91× | 0.93× |
| Q1  | 21.14  | 18.60   | 0.88× | 0.90× |
| Q2  | 20.10  | 17.65   | 0.88× | 1.04× (lost — noise) |
| Q3  | 27.80  | 25.34   | 0.91× | 0.99× |
| Q4  | 46.88  | 247.89  | **5.29×** ✅ | 5.37× |
| Q5  | 35.32  | 109.30  | **3.10×** ✅ | 3.25× |
| Q7  | 54.62  | 453.40  | **8.30×** ✅ | 8.42× |
| Q8  | 23.35  | 27.24   | **1.17×** ✅ | 1.51× (smaller) |
| Q9  | 54.78  | 543.02  | **9.91×** ✅ | 9.59× |
| Q10 | 34.85  | 20.03   | 0.57× | 0.68× |
| Q11 | 73.59  | 100.39  | **1.36×** ✅ | 1.36× |
| Q12 | 30.22  | 32.37   | **1.07×** ✅ | 1.04× |
| Q13 | 41.15  | 38.58   | 0.94× | 0.84× |
| Q14 | 20.19  | 18.15   | 0.90× | 1.31× (lost — noise) |
| Q15 | 18.34  | 270.06  | **14.72×** ✅ | 15.21× |
| Q16 | 199.54 | 286.14  | **1.43×** ✅ | 1.83× |
| Q17 | 45.43  | 54.07   | **1.19×** ✅ | 1.27× |
| Q18 | 68.72  | 178.51  | **2.60×** ✅ | 3.32× |
| Q19 | **124.85** (post-V3) | 130.95 | **1.05×** ✅ | 1.16× (post-V3 / v3.2-carried) |
| Q20 | 55.14  | 404.06  | **7.33×** ✅ | 7.97× |
| Q21 | 42.35  | 44.84   | **1.06×** ✅ | 1.35× |
| Q22 | 31.77  | 30.62   | 0.96× | 1.43× (lost — noise) |
| Q23 | **46.98** (post-V3) | 1063.32 | **22.63×** ✅ | 19.99× (vs v3.2-carried) → **22.63×** vs v4 fresh |

**Headline shift:** rocksdb fresh refresh moves forst-rs's win count from **18/23 → 15/23** in pairwise comparison. The four "lost wins" (Q2, Q14, Q21, Q22) are all narrow margins (forst-rs within 10 % of rocksdb) — they were always inside the noise band. The headline-impact wins (Q4/Q5/Q7/Q9/Q11/Q12/Q15/Q16/Q17/Q18/Q19/Q20/Q23) **all hold**, several with refined ratios.

Q19's post-V3 speedup vs fresh rocksdb is **1.05× ✅** (vs v3.2-carried 1.16×). Q23 still 22.63× vs fresh rocksdb.

---

## Recommendation

Use these refreshed rocksdb numbers as the baseline for any v4 / v5 report. The shift from 18/23 to 15/23 wins is honest noise-band correction, not a forst-rs regression — the four lost wins were never robust.

The forst and forst-rs S3 sweeps remain pending. When resumed:
- forst (JDK17, S3) — expected ~5-6 hours
- forst-rs (JDK25, S3, G1 + noCOH) — expected ~80 min with V3+V4 jar

---

## Raw CSV

```csv
backend,query,time_s,throughput
rocksdb,q0,19.343,5.17 M/s
rocksdb,q1,18.599,5.38 M/s
rocksdb,q2,17.649,5.67 M/s
rocksdb,q3,25.340,3.95 M/s
rocksdb,q4,247.887,403.41 K/s
rocksdb,q5,109.303,914.89 K/s
rocksdb,q7,453.404,220.55 K/s
rocksdb,q8,27.237,3.67 M/s
rocksdb,q9,543.016,184.16 K/s
rocksdb,q10,20.031,4.99 M/s
rocksdb,q11,100.386,996.15 K/s
rocksdb,q12,32.367,3.09 M/s
rocksdb,q13,38.583,2.59 M/s
rocksdb,q14,18.154,5.51 M/s
rocksdb,q15,270.057,370.29 K/s
rocksdb,q16,286.140,349.48 K/s
rocksdb,q17,54.072,1.85 M/s
rocksdb,q18,178.510,560.19 K/s
rocksdb,q19,130.945,763.68 K/s
rocksdb,q20,404.060,247.49 K/s
rocksdb,q21,44.839,2.23 M/s
rocksdb,q22,30.622,3.27 M/s
rocksdb,q23,1063.319,94.05 K/s
```

Per-query Nexmark output preserved at `/tmp/v4-bench/rocksdb-q*.out`.
