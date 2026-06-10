# ForSt Backend vs forst-rs JNI Lib Nexmark Fixed-CSV Report

Date: 2026-06-10  
Host: `yq01-sys-hic-k8s-p40-0000.yq01`  
Image: `flink:2.2.1-jdk17-forst-bench-tools-20260609`  
JDK: OpenJDK 17.0.18  
Container budget used for accuracy: 8 CPUs, 40g container memory, Flink TM process memory 32768m  
Backend path: Flink ForSt backend, local primary/checkpoint directories  
Variants: `forst-local` vs `forst-rs-lib` replacing `libforstjni.so`

## Input Dataset

Accuracy input was generated once from Nexmark datagen and reused as CSV filesystem source for both variants.

| Dataset | Events | person | auction | bid | Size | Path |
|---|---:|---:|---:|---:|---:|---|
| `nexmark-1m` | 1,000,000 | 20,000 | 60,000 | 920,000 | ~197M | `/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610/csv/nexmark-1m` |
| `nexmark-1k-smoke` | 1,000 | 20 | 60 | 920 | ~232K | `/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610/csv/nexmark-1k-smoke` |

## Accuracy Method

The final accuracy check uses a custom `accuracy-file` table sink rather than blackhole/REST output counters. The compare script reads each variant's per-subtask files, normalizes changelog rows, materializes `+I/+U/-U/-D`, and compares materialized row multisets plus SHA-256 hashes.

Important limitations:

- REST `out_rows` is not a correctness oracle. For example, Q12 REST reported 920,000 output rows while the committed accuracy sink contained only thousands of materialized rows on baseline and zero rows on forst-rs-lib.
- Q12 is a PROCTIME 10-second tumbling window. Fixed CSV makes the input identical, but PROCTIME output is still wall-clock dependent. The current forst-rs-lib result is still a blocker because it produced zero materialized rows in both Q12 diagnostic reruns.
- Q6 is not a backend accuracy result because the current SQL template/rewrite fails in the Flink planner.
- Q9 is currently batch output parity only; it did not load either ForSt native library, so it is not a streaming backend proof.

## 1M Fixed-CSV Accuracy Summary

| Query | Status | Mode local / rs-lib | Materialized rows local / rs-lib | SHA local / rs-lib | Native local / rs-lib | Run label | Notes |
|---|---|---|---:|---|---|---|---|
| q0 | PASS | FINISHED / FINISHED | 920000 / 920000 | d0bca6763eb4 / d0bca6763eb4 | unknown / unknown | csv-1m-accfile-p8-q0-q22-nockpt-20260610123438 | streaming, checkpoint interval 999999s |
| q1 | PASS | FINISHED / FINISHED | 920000 / 920000 | b1d1b08639df / b1d1b08639df | unknown / unknown | csv-1m-accfile-p8-q0-q22-nockpt-20260610123438 | streaming, checkpoint interval 999999s |
| q2 | PASS | FINISHED / FINISHED | 6939 / 6939 | 09e444f31f34 / 09e444f31f34 | unknown / unknown | csv-1m-accfile-p8-q0-q22-nockpt-20260610123438 | streaming, checkpoint interval 999999s |
| q3 | PASS | SOURCE_PLATEAU / SOURCE_PLATEAU | 5856 / 5856 | 1f196ccbaa34 / 1f196ccbaa34 | community JNI / forst-rs JNI | csv-1m-accfile-p8-q0-q22-nockpt-20260610123438 | streaming, checkpoint interval 999999s |
| q4 | PASS | SOURCE_DONE / SOURCE_DONE | 5 / 5 | 1826f03f32be / 1826f03f32be | community JNI / forst-rs JNI | csv-1m-accfile-p8-q4-nockpt-max2400-20260610130708 | streaming, checkpoint interval 999999s, maxsec 2400 |
| q5 | PASS | SOURCE_PLATEAU / SOURCE_PLATEAU | 54 / 54 | a7efeb45e984 / a7efeb45e984 | community JNI / forst-rs JNI | csv-1m-accfile-p8-q5-q22-nockpt-max2400-20260610135726 | streaming, checkpoint interval 999999s |
| q6 | UNSUPPORTED | - | - | - | - | - | Current SQL template fails in Flink planner (`rownum`/bounded OVER rewrite issue). |
| q7 | PASS | SOURCE_DONE / SOURCE_DONE | 10 / 10 | d396a9252303 / d396a9252303 | community JNI / forst-rs JNI | csv-1m-accfile-p8-q5-q22-nockpt-max2400-20260610135726 | streaming, checkpoint interval 999999s |
| q8 | PASS | SOURCE_PLATEAU / SOURCE_PLATEAU | 8444 / 8444 | 20be7665422a / 20be7665422a | community JNI / forst-rs JNI | csv-1m-accfile-p8-q5-q22-nockpt-max2400-20260610135726 | streaming, checkpoint interval 999999s |
| q9 | PASS | FINISHED / FINISHED | 55884 / 55884 | 11919548e861 / 11919548e861 | unknown / unknown | csv-1m-accfile-p8-q9-batch-flush1024-20260610142713 | BATCH output parity only; native path not loaded |
| q10 | PASS | FINISHED / FINISHED | 920000 / 920000 | c24700c0e7d4 / c24700c0e7d4 | unknown / unknown | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q11 | PASS | FINISHED / FINISHED | 19911 / 19911 | fbca8572212c / fbca8572212c | community JNI / forst-rs JNI | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q12 | FAIL | FINISHED / FINISHED | 1250 / 0 | 728cc83956b6 / e3b0c44298fc | community JNI / forst-rs JNI | csv-1m-accfile-p8-q12-ckptdisabled-20260610160605 | PROCTIME 10s window; forst-rs-lib produced zero materialized rows even with checkpoint disabled. |
| q13 | PASS | FINISHED / FINISHED | 920000 / 920000 | 50aeff4eb8f9 / 50aeff4eb8f9 | unknown / unknown | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q14 | PASS | FINISHED / FINISHED | 260038 / 260038 | fa83880b971a / fa83880b971a | unknown / unknown | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q15 | PASS | FINISHED / FINISHED | 1 / 1 | d095c2b45fce / d095c2b45fce | community JNI / forst-rs JNI | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q16 | PASS | FINISHED / FINISHED | 10004 / 10004 | 53b4c10134a0 / 53b4c10134a0 | community JNI / forst-rs JNI | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q17 | PASS | SOURCE_DONE / SOURCE_DONE | 59968 / 59968 | 93ca62ba03ce / 93ca62ba03ce | community JNI / forst-rs JNI | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q18 | PASS | SOURCE_DONE / SOURCE_DONE | 292739 / 292739 | 956711f3c205 / 956711f3c205 | community JNI / forst-rs JNI | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q19 | PASS | SOURCE_DONE / SOURCE_DONE | 440630 / 440630 | 799443cdb14c / 799443cdb14c | community JNI / forst-rs JNI | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q20 | PASS | SOURCE_PLATEAU / SOURCE_PLATEAU | 170599 / 170599 | 82b2560a13cb / 82b2560a13cb | community JNI / forst-rs JNI | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q21 | PASS | FINISHED / FINISHED | 875082 / 875082 | d76cb671ac1c / d76cb671ac1c | unknown / unknown | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |
| q22 | PASS | FINISHED / FINISHED | 920000 / 920000 | a07447d05a92 / a07447d05a92 | unknown / unknown | csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413 | streaming, checkpoint interval 999999s |

## Q12 Diagnostics

| Variant of Q12 run | Status | Mode local / rs-lib | Materialized rows local / rs-lib | SHA local / rs-lib | Native local / rs-lib | Run label |
|---|---|---|---:|---|---|---|
| checkpoint disabled, async-state=true | FAIL | FINISHED / FINISHED | 1250 / 0 | 728cc83956b6 / e3b0c44298fc | community JNI / forst-rs JNI | csv-1m-accfile-p8-q12-ckptdisabled-20260610160605 |
| checkpoint disabled, async-state=false | FAIL | FINISHED / FINISHED | 3531 / 0 | 5b5347147b14 / e3b0c44298fc | community JNI / forst-rs JNI | csv-1m-accfile-p8-q12-ckptdisabled-asyncfalse-20260610161324 |

Q12 was rerun after adding `CHECKPOINT_INTERVAL=disabled` support to the runner, so the generated Flink config omitted the `execution.checkpointing` block. It was then rerun again with `table.exec.async-state.enabled=false`. Both runs still produced zero committed accuracy rows for `forst-rs-lib` while `forst-local` produced nonzero rows. This narrows the issue away from checkpoint transfer and away from only the async-state planner switch. The remaining suspect area is the PROCTIME window/timer plus ForSt JNI engine behavior under the Flink ForSt backend replacement library.

## Performance Status

Full Q0-Q22 100M performance was not rerun after this fixed-CSV accuracy pass because Q12 is still a correctness blocker and Q6/Q9 do not yet have equivalent streaming backend accuracy proof. Running 100M before resolving those gates would produce a table that is not defensible as a final comparison.

The only existing 100M reference remains the earlier Q12-only result:

| Query | Variant | Runs | Avg seconds | Avg throughput rows/s | Speedup vs local |
|---|---|---:|---:|---:|---:|
| q12 | ForSt local | 2 | 126.493 | 728,571 | 1.000x |
| q12 | ForSt backend + forst-rs lib | 2 | 121.813 | 755,278 | 1.038x |

Do not treat this as the requested final Q0-Q22 performance result. It was measured before the current fixed-output accuracy sink and before the Q12 zero-output blocker was isolated.

## Analysis

The fixed CSV approach is the right accuracy direction for deterministic Nexmark queries: it removed datagen variance and Q0-Q5, Q7-Q8, and Q10-Q11/Q13-Q22 all matched by materialized output hash where streaming results were available. Stateful queries such as Q11, Q15-Q20 also confirmed that the baseline loaded the jar-extracted community JNI and the replacement variant loaded `/bench/native/libforstjni.so`.

Two exceptions need to be handled before any 100M all-query claim:

1. Q12 cannot be signed off. It is PROCTIME based and not strictly deterministic under fixed CSV, but forst-rs-lib producing zero materialized rows across repeated diagnostic runs is a real blocker for the current engine/library combination.
2. Q6 and Q9 need separate treatment. Q6 needs a SQL/template or planner-compatible rewrite before backend comparison. Q9 needs a streaming backend-native run or must be explicitly excluded from backend accuracy.

Recommended next step: fix or isolate Q12 at the timer/window state boundary before launching the 100M full sweep. After Q12 is corrected, rerun 1M fixed-CSV accuracy with checkpoint disabled and log scanning enabled, then run 100M Q0-Q22 performance under the 8C/32G TM envelope.

## Artifacts

| Artifact | Path |
|---|---|
| `csv-1k-accfile-p8-q3-q4-nockpt-20260610122621.accuracy-compare.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1k-accfile-p8-q3-q4-nockpt-20260610122621.accuracy-compare.tsv` |
| `csv-1m-accfile-p8-q0-q22-nockpt-20260610123438.accuracy-compare.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q0-q22-nockpt-20260610123438.accuracy-compare.tsv` |
| `csv-1m-accfile-p8-q0-q22-nockpt-20260610123438.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q0-q22-nockpt-20260610123438.tsv` |
| `csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413.accuracy-compare.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413.accuracy-compare.tsv` |
| `csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q10-q22-stream2-flush1024-20260610143413.tsv` |
| `csv-1m-accfile-p8-q12-ckptdisabled-20260610160605.accuracy-compare.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q12-ckptdisabled-20260610160605.accuracy-compare.tsv` |
| `csv-1m-accfile-p8-q12-ckptdisabled-20260610160605.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q12-ckptdisabled-20260610160605.tsv` |
| `csv-1m-accfile-p8-q12-ckptdisabled-asyncfalse-20260610161324.accuracy-compare.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q12-ckptdisabled-asyncfalse-20260610161324.accuracy-compare.tsv` |
| `csv-1m-accfile-p8-q12-ckptdisabled-asyncfalse-20260610161324.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q12-ckptdisabled-asyncfalse-20260610161324.tsv` |
| `csv-1m-accfile-p8-q4-nockpt-max2400-20260610130708.accuracy-compare.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q4-nockpt-max2400-20260610130708.accuracy-compare.tsv` |
| `csv-1m-accfile-p8-q4-nockpt-max2400-20260610130708.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q4-nockpt-max2400-20260610130708.tsv` |
| `csv-1m-accfile-p8-q5-q22-nockpt-max2400-20260610135726.accuracy-compare.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q5-q22-nockpt-max2400-20260610135726.accuracy-compare.tsv` |
| `csv-1m-accfile-p8-q5-q22-nockpt-max2400-20260610135726.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q5-q22-nockpt-max2400-20260610135726.tsv` |
| `csv-1m-accfile-p8-q9-batch-flush1024-20260610142713.accuracy-compare.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q9-batch-flush1024-20260610142713.accuracy-compare.tsv` |
| `csv-1m-accfile-p8-q9-batch-flush1024-20260610142713.tsv` | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m/artifacts/csv-1m-accfile-p8-q9-batch-flush1024-20260610142713.tsv` |
| Work root | `/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610` |
| Benchmark docs root | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m` |
