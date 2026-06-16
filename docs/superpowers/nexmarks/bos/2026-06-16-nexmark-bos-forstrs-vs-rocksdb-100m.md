# NexMark BOS 100M forst-rs vs RocksDB Report

Date: 2026-06-16

Status: current-version baseline complete.

Scope: NexMark `q0-q5` and `q7-q22`, 100M events, target TPS 10M, parallelism 8.

Resource model: 2 TaskManagers x 4c/16g, JobManager 2c/4g.

Image: `nexmark-bos:forst-rs-q4-latest-20260615`.

Code recorded by the final q9 run:

| repo | branch | commit |
| --- | --- | --- |
| ForSt | `forst-rs` | `4a6122f09` |
| Flink | `forst-rs-jdk25` | `824f3d133b1` |

Source artifacts:

| artifact | path |
| --- | --- |
| Summary CSV | `/ssd2/jackylee/frs-bench/nexmark-bos-full-8c32g-p8-20260615/results/summary.csv` |
| Generated summary | `/ssd2/jackylee/frs-bench/nexmark-bos-full-8c32g-p8-20260615/results/SUMMARY.md` |
| Previous working report | `/ssd2/jackylee/frs-bench/nexmark-bos-full-8c32g-p8-20260615/results/NEXMARK_BOS_FORSTRS_VS_ROCKSDB_CURRENT_REPORT.md` |

## BOS Validation

All benchmark rows in the final summary were rechecked against their `remote_path` and sanitized run logs.

| validation item | result |
| --- | ---: |
| Latest query/backend records | 44 |
| Finished records | 44/44 |
| forst-rs remote BOS records | 22/22 |
| RocksDB remote BOS records | 22/22 |
| Logs with `BOS config active` | 44/44 |
| Remote paths detected as local filesystem paths | 0 |
| Residual `nexmark-bos*` containers after the run | 0 |

forst-rs remote state paths use the BOS S3-compatible scheme under:

```text
s3://tal-poc-namespace/jackylee/test/nexmark-bos-.../forst-rs-...-bos/...
```

RocksDB checkpoint paths use the Hadoop BOS filesystem under:

```text
bos://tal-poc-namespace/jackylee/test/nexmark-bos-.../rocksdb-...-bos/checkpoints
```

The local `/tmp/jackylee/nexmark-bos/...` paths were used only as local cache or scratch directories. They were not the remote state or checkpoint location.

The RocksDB runs also logged that the BOS filesystem jar was present in the Flink runtime lib directory:

```text
RUN_FLINK_LIB_BOS_JAR_BYTES=11540595
EFFECTIVE_CONFIG_OK: rocksdb BOS config active ...
```

No auth values are recorded in this document.

## Executive Summary

Both backends completed all 22 scoped queries on BOS.

| backend | finished | total wall_s | total wall_min |
| --- | ---: | ---: | ---: |
| forst-rs | 22/22 | 8287.2 | 138.1 |
| RocksDB | 22/22 | 9468.2 | 157.8 |

forst-rs is faster by total wall time:

| metric | value |
| --- | ---: |
| Saved wall time | 1181.0s |
| Saved wall time | 19.7 min |
| Overall speedup, RocksDB / forst-rs | 1.14x |

forst-rs is faster on 16 queries:

```text
q0, q1, q2, q3, q4, q9, q10, q11, q13, q14, q15, q16, q18, q20, q21, q22
```

RocksDB is faster on 6 queries:

```text
q5, q7, q8, q12, q17, q19
```

Largest forst-rs wins:

| query | forst-rs wall_s | RocksDB wall_s | saved_s | RocksDB / forst-rs |
| --- | ---: | ---: | ---: | ---: |
| q15 | 743.3 | 1148.5 | 405.2 | 1.55x |
| q16 | 697.6 | 1050.4 | 352.8 | 1.51x |
| q9 | 1271.9 | 1544.1 | 272.2 | 1.21x |
| q20 | 937.7 | 1116.6 | 178.9 | 1.19x |
| q18 | 347.6 | 404.4 | 56.8 | 1.16x |

Largest forst-rs losses:

| query | forst-rs wall_s | RocksDB wall_s | lost_s | RocksDB / forst-rs |
| --- | ---: | ---: | ---: | ---: |
| q7 | 1051.7 | 916.7 | 135.0 | 0.87x |
| q17 | 249.4 | 213.1 | 36.3 | 0.85x |
| q5 | 417.3 | 393.4 | 23.9 | 0.94x |
| q19 | 438.1 | 425.2 | 12.9 | 0.97x |
| q12 | 117.2 | 110.1 | 7.1 | 0.94x |

## By Query Performance

`delta_s = RocksDB - forst-rs`. Positive values mean forst-rs is faster. `RocksDB / forst-rs > 1.00` also means forst-rs is faster.

| query | forst-rs wall_s | RocksDB wall_s | delta_s | RocksDB / forst-rs | faster | forst-rs out_rows | RocksDB out_rows |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| q0 | 60.2 | 68.0 | +7.8 | 1.13 | forst-rs | 100000000 | 100000000 |
| q1 | 57.2 | 63.0 | +5.8 | 1.10 | forst-rs | 100000000 | 100000000 |
| q2 | 55.2 | 58.1 | +2.9 | 1.05 | forst-rs | 100000000 | 100000000 |
| q3 | 89.5 | 94.3 | +4.8 | 1.05 | forst-rs | 2198663 | 2199384 |
| q4 | 891.6 | 955.7 | +64.1 | 1.07 | forst-rs | 25705706* | 176427694* |
| q5 | 417.3 | 393.4 | -23.9 | 0.94 | RocksDB | 29992881 | 29992435 |
| q7 | 1051.7 | 916.7 | -135.0 | 0.87 | RocksDB | 92000002 | 92000002 |
| q8 | 110.7 | 106.9 | -3.8 | 0.97 | RocksDB | 3066375 | 3066370 |
| q9 | 1271.9 | 1544.1 | +272.2 | 1.21 | forst-rs | 91212230 | 91211783 |
| q10 | 118.4 | 124.8 | +6.4 | 1.05 | forst-rs | 100000000 | 100000000 |
| q11 | 290.8 | 297.9 | +7.1 | 1.02 | forst-rs | 92000000 | 92000000 |
| q12 | 117.2 | 110.1 | -7.1 | 0.94 | RocksDB | 92000000 | 92000000 |
| q13 | 95.0 | 97.3 | +2.3 | 1.02 | forst-rs | 100000000 | 100000000 |
| q14 | 54.9 | 60.4 | +5.5 | 1.10 | forst-rs | 100000000 | 100000000 |
| q15 | 743.3 | 1148.5 | +405.2 | 1.55 | forst-rs | 92000000 | 92000000 |
| q16 | 697.6 | 1050.4 | +352.8 | 1.51 | forst-rs | 92000000 | 92000000 |
| q17 | 249.4 | 213.1 | -36.3 | 0.85 | RocksDB | 92000000 | 92000000 |
| q18 | 347.6 | 404.4 | +56.8 | 1.16 | forst-rs | 92000000 | 92000000 |
| q19 | 438.1 | 425.2 | -12.9 | 0.97 | RocksDB | 92000000 | 92000000 |
| q20 | 937.7 | 1116.6 | +178.9 | 1.19 | forst-rs | 93199487 | 93200671 |
| q21 | 112.4 | 127.5 | +15.1 | 1.13 | forst-rs | 100000000 | 100000000 |
| q22 | 79.5 | 91.8 | +12.3 | 1.15 | forst-rs | 100000000 | 100000000 |

`*` q4 note: see the q4 validation section below. The q4 `out_rows` values are the sink/Writer vertex `read-records` metric and are not a reliable final-result row-count comparison for this changelog aggregation query.

## Q4 Validation

q4 completed cleanly on both backends with BOS remote state:

| backend | status | wall_s | src_out | out_rows metric |
| --- | --- | ---: | ---: | ---: |
| forst-rs | FINISHED | 891.6 | 98000000 | 25705706 |
| RocksDB | FINISHED | 955.7 | 98000000 | 176427694 |

The q4 SQL submitted to both backends was byte-identical:

```text
sha256: 4592f8140b074f5c9c48efa500d76caa2d5ecba960a9edc08f4fde0cd09d571c
Q4_SQL_IDENTICAL=yes
```

The submitted q4 query was:

```sql
INSERT INTO nexmark_q4
SELECT
    Q.category,
    AVG(Q.final)
FROM (
    SELECT MAX(B.price) AS final, A.category
    FROM auction A, bid B
    WHERE A.id = B.auction AND B.`dateTime` BETWEEN A.`dateTime` AND A.expires
    GROUP BY A.id, A.category
) Q
GROUP BY Q.category;
```

Interpretation:

- q4 wall time is a valid run-time measurement for this BOS benchmark: forst-rs `891.6s`, RocksDB `955.7s`.
- q4 `out_rows` is not a valid final-result equality signal in this harness. The harness reads the Flink REST sink/Writer vertex `read-records` metric. For q4, which is a join plus inner aggregation plus outer aggregation, that metric reflects changelog/update traffic observed by the sink path, not the final category-level aggregate result.
- The q4 `out_rows` difference therefore cannot by itself prove a semantic result mismatch.
- A separate q4 correctness run should use a materialized sink, such as filesystem or a dedicated collecting sink, and compare the final retracted/upserted category aggregate values.

For the current performance summary, q4 wall time is included, while q4 output-count equality is explicitly excluded from correctness conclusions.

## q9 Root Cause and Final Profile

q9 was the only query that required additional memory tuning to finish under the 2 x 4c/16g resource model.

Observed failed signatures before the final successful run:

- Larger-memory profiles hit TaskManager cgroup OOM, with container exit code 137.
- A `10240m` process-size profile still OOMed around 59M source records.
- An `8192m` low-memory profile progressed to about 90.5M source records but still OOMed.

The successful q9 forst-rs run used a tighter profile:

| knob | value |
| --- | --- |
| TM process size | `7168m` |
| writebuffer size | `128mb` |
| writebuffer count | `1` |
| writebuffer manager capacity | `512mb` |
| block cache capacity | `256mb` |
| compaction max-background | `2` |
| flush max-background | `1` |
| async-state in-flight limit | `10000` |
| async-state buffer size | `2048` |
| vlog reader cache cap | `512` |
| vlog resident budget | `64mb` |
| KV adaptive pressure | enabled |

Final q9 result:

| backend | status | wall_s | src_out | out_rows |
| --- | --- | ---: | ---: | ---: |
| forst-rs | FINISHED | 1271.9 | 98000000 | 91212230 |
| RocksDB | FINISHED | 1544.1 | 98000000 | 91211783 |

q9 conclusion: the failure mode was TaskManager cgroup memory pressure, not BOS reachability. The successful profile reduced JVM/process memory, forst-rs native memory, async-state buffering, background work, and local file cache pressure enough to keep the run inside the 16GiB/TM cgroup.

## BOS Bottleneck Assessment

BOS access was not the root cause of the observed failures.

Evidence:

- All 44 latest backend/query cells completed with BOS remote state or checkpoint paths.
- q9 completed after memory-profile tuning, not after remote endpoint or retry changes.
- q9 failure signatures were cgroup OOM events, not remote filesystem exceptions.
- q4 completed on both backends with BOS state paths and identical SQL.

BOS/local-cache interaction still matters for memory accounting. The local `/tmp/jackylee/nexmark-bos/...` cache and scratch files can add Linux page-cache pressure to the TaskManager cgroup, especially on q9. This is a memory pressure issue around local cache and cgroup accounting, not evidence that the job ran local-only.

## Follow-up Work

Recommended next steps:

1. Validate q4 final semantic output with a materialized sink rather than the REST sink `read-records` metric.
2. Investigate q7 and q17, where RocksDB is meaningfully faster.
3. Investigate smaller RocksDB wins on q5, q8, q12, and q19.
4. Keep a separate q9 low-memory profile for the 2 x 4c/16g resource model.
5. After this baseline, rebuild a new `nexmark-bos*` image from the latest jars/test files and rerun the sensitive queries first: q4, q7, q9, q17, q5, q12, and q19.
