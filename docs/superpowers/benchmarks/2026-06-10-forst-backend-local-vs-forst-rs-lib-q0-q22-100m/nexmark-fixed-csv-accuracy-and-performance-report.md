# Nexmark Fixed-CSV Accuracy and Performance Report

Date: 2026-06-10

Scope: compare Flink ForSt backend with the community ForSt JNI library against the same Flink ForSt backend with the forst-rs compatible `libforstjni.so` replacement, using JDK17 and fixed CSV inputs for accuracy.

## Environment

| Item | Value |
| --- | --- |
| Docker image | `flink:2.2.1-jdk17-forst-bench-tools-20260609` |
| Java in image | OpenJDK 17.0.18 |
| Fixed CSV dataset | `nexmark-1m`: person 20,000; auction 60,000; bid 920,000; total 1,000,000 |
| Community JNI jar | `forstjni-0.1.8-community.jar`, sha256 `40897016956c2768580dc2f384f13fddc20f07660a844c4d75598e4ea51b420f` |
| forst-rs JNI lib | `libforstjni.so`, sha256 `9e843fd870cac992c838ee60c8c319c89547ca7c332c4abb9be42e0c6b7ff476` |
| Accuracy sink | `accuracy-file`, materialized changelog comparison |

## Accuracy Summary

The fixed CSV source is now used for all accuracy checks. Deterministic streaming queries are compared by materialized changelog hash. Q12 uses a query-specific processing-time invariant because wall-clock processing-time windows are not deterministic across runs. Q6 is validated in batch mode because the upstream Nexmark SQL comments already mark the streaming shape unsupported by Flink SQL.

Accuracy runs intentionally use a separate 1M input policy: generate the Nexmark datagen stream once into `person`, `auction`, and `bid` CSV files, then run both variants from that exact filesystem source. This keeps input ordering and content identical across variants. CSV-source runs are used only for correctness; they are not mixed into the 100M performance numbers because filesystem source overhead would distort the ForSt backend comparison. The current fixed CSV dataset has these hashes: person `8a1f48051ff6722457d77cfa5d022681a88efa9dde36c7d681daea272dce9ef2`, auction `85e7468372f078e89f529943911f1b2f65a57387c4376c804d0bc5441bf19863`, bid `5c147de8cf14bcf4f8d3d5f44aece33e62012239c1b85fffb81d6727a2023cb6`.

The consolidated manifest `artifacts/consolidated-q0-q22-fixed-csv-accuracy-20260610.tsv` selects the authoritative fixed-CSV evidence for each query and verifies all Q0-Q22 entries as PASS. It uses normal materialized changelog hash equality for deterministic queries, the corrected batch rewrite for Q6, the 1M batch parity plus 1k streaming native smoke for Q9, and the continuous-source processing-time invariant for Q12.

| Query | Status | Validation mode | Materialized rows / invariant | Native evidence | Run label / note |
| --- | --- | --- | --- | --- | --- |
| q0 | PASS | 1M streaming hash | 920000 / 920000 | unknown / unknown | prior fixed-CSV accuracy run |
| q1 | PASS | 1M streaming hash | 920000 / 920000 | unknown / unknown | prior fixed-CSV accuracy run |
| q2 | PASS | 1M streaming hash | 6939 / 6939 | unknown / unknown | prior fixed-CSV accuracy run |
| q3 | PASS | 1M streaming hash | 5856 / 5856 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q4 | PASS | 1M streaming hash | 5 / 5 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q5 | PASS | 1M streaming hash | 54 / 54 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q6 | PASS | 1M batch hash with stable tie-breakers | 55884 / 55884 | unknown / unknown | `q6-1m-batch-stable-accuracy-20260610170032`; streaming SQL remains planner-unsupported |
| q7 | PASS | 1M streaming hash | 10 / 10 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q8 | PASS | 1M streaming hash | 8444 / 8444 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q9 | PASS | 1M batch hash plus 1k streaming native smoke | 55884 / 55884 in batch; 106 / 106 in 1k streaming | 1k streaming: community JNI / forst-rs JNI | `csv-1m-accfile-p8-q9-batch-flush1024-20260610142713`; `q9-1k-stream-debug-20260610165239` |
| q10 | PASS | 1M streaming hash | 920000 / 920000 | unknown / unknown | prior fixed-CSV accuracy run |
| q11 | PASS | 1M streaming hash | 19911 / 19911 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q12 | PASS | 1M streaming continuous-source PROCTIME invariant | `sum(bid_count)=920000` both; `unique_bidders=19911` both | community JNI / forst-rs JNI | `q12-1m-p8-monitor2-debug-20260610164456`; full-row hash intentionally not used |
| q13 | PASS | 1M streaming hash | 920000 / 920000 | unknown / unknown | prior fixed-CSV accuracy run |
| q14 | PASS | 1M streaming hash | 260038 / 260038 | unknown / unknown | prior fixed-CSV accuracy run |
| q15 | PASS | 1M streaming hash | 1 / 1 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q16 | PASS | 1M streaming hash | 10004 / 10004 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q17 | PASS | 1M streaming hash | 59968 / 59968 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q18 | PASS | 1M streaming hash | 292739 / 292739 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q19 | PASS | 1M streaming hash | 440630 / 440630 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q20 | PASS | 1M streaming hash | 170599 / 170599 | community JNI / forst-rs JNI | prior fixed-CSV accuracy run |
| q21 | PASS | 1M streaming hash | 875082 / 875082 | unknown / unknown | prior fixed-CSV accuracy run |
| q22 | PASS | 1M streaming hash | 920000 / 920000 | unknown / unknown | prior fixed-CSV accuracy run |

## Q12 Correction

The earlier Q12 failure was caused by using a bounded CSV source with a PROCTIME query. The bounded filesystem source can finish before processing-time windows fire, so a zero-row result is possible without proving an engine correctness bug. The runner now supports `CSV_SOURCE_MONITOR_INTERVAL`; with `source.monitor-interval = '1 s'`, the source remains alive after consuming the fixed files and allows processing-time timers to emit.

The accepted Q12 invariant is not full-row hash equality because window start/end timestamps are wall-clock values and can legitimately differ. The accepted invariant is:

- both variants reach `SOURCE_DONE` or `FINISHED`,
- both variants have no negative changelog events,
- both variants produce at least one materialized Q12 row,
- both variants have `sum(bid_count) = expected bid input rows`,
- both variants have the same bidder set digest.

For run `q12-1m-p8-monitor2-debug-20260610164456`, both variants produced `sum(bid_count)=920000` and bidder digest `6ae2fa19a45f640886ad0f44c92952df5e68824185a8c66a6f3c955420a974b2`.

## Q6 Correction

The original Q6 SQL is marked unsupported in the Nexmark SQL file because Flink SQL cannot consume the required retractions in the streaming OVER shape. Batch runtime accepts the validation rewrite. The first 1M batch run mismatched because the rewritten winning-bid selection used `ORDER BY B.price DESC` without deterministic tie-breakers. The current validation rewrite uses stable ordering for the batch accuracy path:

- winning bid: `ORDER BY B.price DESC, B.dateTime ASC, B.bidder ASC`,
- seller average: `ORDER BY Q.bidTime, Q.id`.

Run `q6-1m-batch-stable-accuracy-20260610170032` passed with identical materialized hash `fae89ad0df47004c371892e7e17d1a0cf8741bbcde43bdd03ebddfdb00ab7067`.

## Q9 Correction

Q9 can execute in streaming mode with native ForSt state, but the 1M streaming fixed-CSV accuracy run was stopped after roughly two minutes because it only reached about 105k of 980k source rows and would exceed the intended small accuracy-validation resource envelope. Therefore the accuracy matrix uses the already completed 1M batch parity result and records a separate 1k streaming native smoke proof.

## Performance Status

The 100M performance run was restarted with the corrected environment: JDK17, jemalloc loaded through `LD_PRELOAD=/lib/x86_64-linux-gnu/libjemalloc.so.2`, checkpointing disabled, one TaskManager with 8 slots and 32GB process memory, and one benchmark container at a time. The benchmark runner now waits for bounded jobs to reach `FINISHED` unless a diagnostic run intentionally uses `MAXSEC`.

The full Q0-Q22 100M matrix is currently blocked by Q4. Q0-Q2 finish at 100M, but the forst-rs JNI replacement does not show a speedup there. Q4 is already unable to finish at 1M under the 8C/32G envelope, even after validating jemalloc, async state, mini-batch, and a write-heavy ForSt tuning profile. Running Q4 at 100M would be an unbounded time sink and would not produce the requested defensible comparison.

### Completed 100M Runs

Throughput below is computed as `100,000,000 / wall_seconds`. The REST `src_out` metric is not used as the denominator because it has query-dependent semantics in the fused SQL plan.

| Query | ForSt local seconds | forst-rs lib seconds | ForSt local events/s | forst-rs lib events/s | Speedup | Run label |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| q0 | 60.987 | 60.627 | 1,639,693 | 1,649,430 | 1.006x | `perf100m-q0q1q2q4q5-jemalloc-20260610180554` |
| q1 | 61.513 | 64.404 | 1,625,673 | 1,552,699 | 0.955x | `perf100m-q0q1q2q4q5-jemalloc-20260610180554` |
| q2 | 53.398 | 54.973 | 1,872,729 | 1,819,076 | 0.971x | `perf100m-q0q1q2q4q5-jemalloc-20260610180554` |

### Q3 Diagnostic

Q3 is a long-running stateful join. A 100M run was stopped after discovering the earlier run did not actually preload jemalloc and that the `expected_src` heuristic was invalid for q3. After enabling jemalloc, 10M diagnostics show only a small forst-rs-lib advantage.

| Query | Events | Variant | Mode | Seconds | Output rows | Native evidence | Run label |
| --- | ---: | --- | --- | ---: | ---: | --- | --- |
| q3 | 10,000,000 | ForSt local | FINISHED | 244.590 | 220,284 | community JNI | `diag-q3-10m-jemalloc-20260610175344` |
| q3 | 10,000,000 | forst-rs lib | FINISHED | 240.493 | 219,595 | forst-rs JNI | `diag-q3-10m-rslib-jemalloc-20260610175959` |

Q3 10M speedup is `1.017x`, so it does not support a 1.5x claim.

### Q4 Blocker

Q4 is the current blocker for a full Q0-Q22 100M run. It is a join plus two GroupAggregate operators. REST vertex metrics showed the source operator backpressured by the join/aggregate chain. The following 1M runs were used to avoid wasting the 100M resource envelope.

| Query | Events | Variant | Config | Mode | Wall seconds | Source rows | Expected source rows | Output rows | Run label |
| --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| q4 | 1,000,000 | ForSt local | mini-batch, no write-heavy profile | TIMEOUT | 604.748 | 891,932 | 980,000 | 1,273 | `tune-q4-1m-minibatch-jemalloc-20260610182954` |
| q4 | 1,000,000 | ForSt local | mini-batch + write-heavy profile | TIMEOUT | 903.809 | 980,000 | 980,000 | 2,760 | `tune-q4-1m-minibatch-writeheavy-20260610184342` |
| q4 | 1,000,000 | forst-rs lib | mini-batch + write-heavy profile | TIMEOUT | 904.050 | 980,000 | 980,000 | 2,174 | `tune-q4-1m-rslib-minibatch-writeheavy-20260610185952` |

Interpretation: forst-rs-lib is essentially tied with the community JNI path on Q4. The current bottleneck is not fixed by the JNI replacement, jemalloc, async state, mini-batch, or larger ForSt write buffers.

#### Q4 JFR Diagnosis

A focused 1M Q4 JFR run (`profile-q4-1m-local-jfr-20260610192347`) was captured with the same 8C/40G container envelope, JDK17 image, jemalloc preload, checkpointing disabled, mini-batch enabled, and write-heavy ForSt tuning. It timed out at 264.914s with 619,050 of 980,000 source rows consumed. JFR samples showed the hottest Java/native path in `Join[12] -> Calc[13] -> LocalGroupAggregate[14]`, especially:

- `org.forstdb.RocksDB.iterator`: 5,275 samples,
- `ForStSyncMapState$RocksDBMapIterator.loadCache`: 5,502 samples,
- `ForStSyncMapState$RocksDBMapIterator.hasNext`: 5,276 samples,
- `ForStOperationUtils.getForStIterator`: 5,275 samples,
- `RocksIterator.disposeInternal`: 226 samples.

The first attempted backend fix made the synchronous `MapState` iterator cache size configurable and raised the benchmark profile to `state.backend.forst.map-state.iterator-cache-size: 4096`. The patch was committed in Flink branch `forst-rs-jdk25` as `691ac31c19e` and benchmarked by installing a patched `flink-statebackend-forst-2.2.1.jar` over the JDK17 image jar.

#### Q4 Patched Backend Retest

The patched backend jar used for this retest had sha256 `039d28c734a6d1c6c901fa23d1c849182b19d2902f165a5b78e71fe984d04d9a`. The image-original backend jar hash was `a4cd9b71fdbccd4de4d913f5a428e0942d96a542c4a4d0b1abd830802ef14a98`. Both variants used the same patched backend jar, JDK17, jemalloc, disabled checkpoints, mini-batch, write-heavy ForSt config, and 8C/40G container envelope.

| Query | Events | Variant | Backend jar | Mode | Matrix wall seconds | Source target seconds | Source rows | Output rows | Native evidence | Run label |
| --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| q4 | 1,000,000 | ForSt local | patched map-cache jar | SOURCE_DONE | 807.920 | 807.920 | 980,000 | 2,004 | community JNI from jar extraction | `mapcache-q4-1m-20260610195301` |
| q4 | 1,000,000 | forst-rs lib | patched map-cache jar | SOURCE_PLATEAU | 853.754 | 818.272 | 980,000 | 2,214 | `/bench/native/libforstjni.so` | `mapcache-q4-1m-20260610195301` |

The 4096-entry cache setting did not materially improve Q4. ForSt local reached the 980k source target in 807.920s, while forst-rs-lib reached it in 818.272s. Using the source-target timestamp, forst-rs-lib is `0.987x` of local on this run; using the matrix wall seconds, it is `0.946x`. This disproves the idea that increasing iterator refill cache alone can unlock the requested 1.5x result. The JFR hotspot should be reinterpreted as many small MapState scans creating native iterators, not only large scans refilling every 128 entries. The next useful optimization point is reducing native iterator creation per MapState scan or changing the SQL aggregate state access pattern; continuing to tune JNI get/put or the current cache batch size is unlikely to close the gap.

## Artifacts

| Artifact | Path |
| --- | --- |
| Q12 corrected compare | `artifacts/q12-1m-p8-monitor2-debug-20260610164456.accuracy-compare.tsv` |
| Q6 corrected compare | `artifacts/q6-1m-batch-stable-accuracy-20260610170032.accuracy-compare.tsv` |
| Q9 streaming smoke compare | `artifacts/q9-1k-stream-debug-20260610165239.accuracy-compare.tsv` |
| Q0-Q2 100M performance TSV | `artifacts/perf100m-q0q1q2q4q5-jemalloc-20260610180554.tsv` |
| Q3 10M local diagnostic TSV | `artifacts/diag-q3-10m-jemalloc-20260610175344.tsv` |
| Q3 10M forst-rs-lib diagnostic TSV | `artifacts/diag-q3-10m-rslib-jemalloc-20260610175959.tsv` |
| Q4 1M mini-batch diagnostic TSV | `artifacts/tune-q4-1m-minibatch-jemalloc-20260610182954.tsv` |
| Q4 1M local write-heavy diagnostic TSV | `artifacts/tune-q4-1m-minibatch-writeheavy-20260610184342.tsv` |
| Q4 1M forst-rs-lib write-heavy diagnostic TSV | `artifacts/tune-q4-1m-rslib-minibatch-writeheavy-20260610185952.tsv` |
| Q4 1M patched map-cache backend TSV | `artifacts/mapcache-q4-1m-20260610195301.tsv` |
| Updated matrix runner | `scripts/run-matrix.sh` |
| Full-run container runner | `scripts/run-one-full.sh` |
| Full-run SQL measurement script | `scripts/measure-sql-full.sh` |
| Updated CSV accuracy runner | `scripts/measure-sql-csv-accuracy.sh` |
| Consolidated Q0-Q22 fixed-CSV accuracy manifest | `artifacts/consolidated-q0-q22-fixed-csv-accuracy-20260610.tsv` |
| Updated compare script | `scripts/compare-accuracy-output.py` |
