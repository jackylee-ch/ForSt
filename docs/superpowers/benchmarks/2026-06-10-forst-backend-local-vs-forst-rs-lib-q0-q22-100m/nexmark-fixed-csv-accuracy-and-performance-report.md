# ForSt backend vs forst-rs lib Nexmark fixed-input report

Date: 2026-06-10  
Host: yq01-sys-hic-k8s-p40-0000.yq01  
Image: `flink:2.2.1-jdk17-forst-bench-tools-20260609`  
JDK: OpenJDK 17.0.18  
Container budget: 8 CPUs, 40g container memory, Flink TM process memory 32768m  
Backend path: Flink ForSt backend with local primary/checkpoint directories  
Variants: `forst-local` vs `forst-rs-lib` replacing `libforstjni.so`

## Input Dataset

Accuracy input is generated once from Nexmark datagen and then reused as CSV filesystem source for both variants.

| Dataset | Events | person | auction | bid | Size | Path |
|---|---:|---:|---:|---:|---:|---|
| `nexmark-1m` | 1,000,000 | 20,000 | 60,000 | 920,000 | ~197M | `/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610/csv/nexmark-1m` |
| `nexmark-1k-smoke` | 1,000 | 20 | 60 | 920 | ~232K | `/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610/csv/nexmark-1k-smoke` |

## Accuracy Results

### 1K CSV smoke, streaming, parallelism 1

This run verifies the fixed-input path before moving to 1M. Both variants loaded the expected native library in streaming mode.

| Query | Variant | Mode | wall_ms | src_out | out_rows | Native |
|---|---|---|---:|---:|---:|---|
| q3 | forst-local | SOURCE_PLATEAU | 61,774 | 20 | 20 | jar-extracted `libforstjni-linux64.so` |
| q3 | forst-rs-lib | SOURCE_PLATEAU | 61,893 | 20 | 20 | `/bench/native/libforstjni.so` |
| q4 | forst-local | SOURCE_PLATEAU | 61,891 | 980 | 159 | jar-extracted `libforstjni-linux64.so` |
| q4 | forst-rs-lib | SOURCE_PLATEAU | 61,931 | 980 | 159 | `/bench/native/libforstjni.so` |

### 1M CSV, streaming, parallelism 8

This is the meaningful backend accuracy smoke because it uses the same 8-way execution shape as the 8C performance environment and loads the intended native library on both variants.

| Query | Variant | Mode | wall_ms | src_out | expected_src | out_rows | Native | Result |
|---|---|---|---:|---:|---:|---:|---|---|
| q3 | forst-local | SOURCE_PLATEAU | 62,061 | 21,773 | 80,000 | 21,773 | jar-extracted `libforstjni-linux64.so` | pass |
| q3 | forst-rs-lib | SOURCE_PLATEAU | 62,128 | 21,773 | 80,000 | 21,773 | `/bench/native/libforstjni.so` | pass |
| q4 | forst-local | TIMEOUT | 424,882 | 126,323 | 980,000 | 12,367 | jar-extracted `libforstjni-linux64.so` | inconclusive |
| q4 | forst-rs-lib | NO_RESULT | 0 | 0 | 0 | 0 | unknown | skipped after local timeout |

### Method findings

| Finding | Evidence | Impact |
|---|---|---|
| Fixed CSV input is required and works | 1K p1 q3/q4 both matched; 1M p8 q3 matched | This should replace per-run datagen for accuracy checks. |
| Single parallelism is not a practical 1M gate for q3 | q3 p1 local timed out at 905.6s with `out_rows=21384` while still RUNNING | Accuracy should be checked under the 8-way execution shape or with a better sink/completion policy. |
| Batch mode finishes quickly but is not a ForSt backend proof | 1K batch q3 finished in ~1.6s but `native=unknown` | Batch can provide SQL baseline counts, but it does not validate the streaming ForSt native path. |
| Blackhole sink metrics are not a sufficient all-query correctness oracle | q4 p8 showed long-tail output and run-to-run source metric differences | Q0-Q22 accuracy needs a committed/hashable sink or query-specific completion policy before claiming full pass. |
| Current matrix runner has weak SIGINT behavior | Interrupting a run allowed the matrix to continue to the next variant/query | Runner should trap interruption and stop the matrix before more long accuracy runs. |

## Performance Reference

The only completed 100M performance comparison currently available is the earlier q12 run. It is included as reference, not as the requested Q0-Q22 final benchmark.

| Query | Variant | Runs | Avg seconds | Avg throughput rows/s | Speedup vs local |
|---|---|---:|---:|---:|---:|
| q12 | ForSt local | 2 | 126.493 | 728,571 | 1.000x |
| q12 | ForSt backend + forst-rs lib | 2 | 121.813 | 755,278 | 1.038x |

## Analysis

The corrected accuracy direction is fixed-input CSV generated once from Nexmark datagen. It removes the earlier per-run datagen noise and made q3/q4 pass at 1K and q3 pass at 1M with both native libraries loaded.

The current runner is not yet enough for a Q0-Q22 accuracy sign-off. Q4 demonstrates that blackhole sink metrics plus a generic plateau rule can timeout or vary by run. Running 100M Q0-Q22 performance before this accuracy gate is fixed would produce numbers that are hard to defend.

Recommended next step: replace blackhole-metric accuracy with a deterministic result sink for accuracy mode. The practical options are a bounded/committed filesystem output with hash comparison, a small custom checksum sink, or query-specific SQL rewrites that emit deterministic aggregate checksums. After Q0-Q22 pass on 1M fixed CSV, rerun 100M performance for both variants under 8C/32G and publish the final comparison table.

## Artifacts

| Artifact | Path |
|---|---|
| Work root | `/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610` |
| Benchmark docs root | `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m` |
| 1M q3/q4 p8 TSV | `/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610/results/csv-1m-p8-q3-q4-20260610114637.tsv` |
| 1M q3/q4 p1 TSV | `/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610/results/csv-1m-p1-q3-q4-20260610112526.tsv` |
| 1K p1 smoke TSV | `/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610/results/csv-1k-p1-q3-q4-20260610111714.tsv` |
