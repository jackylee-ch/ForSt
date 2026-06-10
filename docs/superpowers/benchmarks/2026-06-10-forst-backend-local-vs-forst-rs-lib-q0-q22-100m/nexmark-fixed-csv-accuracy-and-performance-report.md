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

Full Q0-Q22 100M performance comparison has not been rerun after the corrected accuracy pass. The old Q12-only 100M reference is retained only as historical context and must not be treated as the requested final Q0-Q22 performance result.

| Query | Variant | Runs | Avg seconds | Source rows/s | Speedup |
| --- | --- | ---: | ---: | ---: | ---: |
| q12 | ForSt local | 2 | 126.493 | 728,571 | 1.000x |
| q12 | ForSt backend + forst-rs lib | 2 | 121.813 | 755,278 | 1.038x |

Next required step: run the full 100M performance matrix under the 8C/32G TM envelope now that the accuracy matrix has a defensible query-specific policy.

## Artifacts

| Artifact | Path |
| --- | --- |
| Q12 corrected compare | `artifacts/q12-1m-p8-monitor2-debug-20260610164456.accuracy-compare.tsv` |
| Q6 corrected compare | `artifacts/q6-1m-batch-stable-accuracy-20260610170032.accuracy-compare.tsv` |
| Q9 streaming smoke compare | `artifacts/q9-1k-stream-debug-20260610165239.accuracy-compare.tsv` |
| Updated runner | `scripts/run-matrix.sh` |
| Updated CSV accuracy runner | `scripts/measure-sql-csv-accuracy.sh` |
| Updated compare script | `scripts/compare-accuracy-output.py` |
