# NexMark BOS/S3 100M forst-rs vs RocksDB PMC Summary (1.14x)

Data date: 2026-06-16

Prepared: 2026-07-28

Source report:
`docs/superpowers/nexmarks/bos/2026-06-16-nexmark-bos-forstrs-vs-rocksdb-100m.md`

## Verdict

Under the 2 TaskManager x 4c/16g resource model, forst-rs completed the scoped
NexMark BOS/S3 100M benchmark faster than RocksDB.

| metric | forst-rs | RocksDB | result |
| --- | ---: | ---: | ---: |
| Finished queries | 22/22 | 22/22 | both complete |
| Total wall time | 8287.2s | 9468.2s | forst-rs saved 1181.0s |
| Total wall time | 138.1 min | 157.8 min | forst-rs saved 19.7 min |
| Overall speedup | - | - | RocksDB / forst-rs = 1.14x |

Performance shape:

- forst-rs was faster on 16 queries: q0, q1, q2, q3, q4, q9, q10, q11, q13,
  q14, q15, q16, q18, q20, q21, q22.
- RocksDB was faster on 6 queries: q5, q7, q8, q12, q17, q19.
- The largest forst-rs wins were q15, q16, q9, q20, and q18.
- The largest RocksDB wins were q7, q17, q5, q19, and q12.

Correctness position:

- A separate fixed-CSV BOS accuracy harness passed for the scoped q0-q22 query
  set.
- q4 correctness is based on materialized changelog equality, not the REST
  `out_rows` metric.
- q9 correctness is based on final-winner comparison per auction. Both backends
  matched the expected winners from the fixed CSV input.

## Run Scope

| item | value |
| --- | --- |
| Workload | NexMark q0-q5 and q7-q22 |
| Events | 100M |
| Target TPS | 10M |
| Parallelism | 8 |
| Resource model | 2 TaskManagers x 4c/16g, JobManager 2c/4g |
| Image | `nexmark-bos:forst-rs-q4-latest-20260615` |
| ForSt repo | `forst-rs`, commit `4a6122f09` |
| Flink repo | `forst-rs-jdk25`, commit `824f3d133b1` |

Remote state/checkpoint paths were BOS-backed:

- forst-rs state used the S3-compatible path under
  `s3://tal-poc-namespace/jackylee/test/.../forst-rs-...-bos/...`.
- RocksDB checkpoints used Hadoop BOS under
  `bos://tal-poc-namespace/jackylee/test/.../rocksdb-...-bos/checkpoints`.
- Local `/tmp/jackylee/nexmark-bos/...` paths were cache/scratch paths only,
  not the remote state or checkpoint location.

## Per-query Wall Time

`delta_s = RocksDB - forst-rs`. Positive values mean forst-rs was faster.

| query | forst-rs wall_s | RocksDB wall_s | delta_s | RocksDB / forst-rs | faster |
| --- | ---: | ---: | ---: | ---: | --- |
| q0 | 60.2 | 68.0 | +7.8 | 1.13 | forst-rs |
| q1 | 57.2 | 63.0 | +5.8 | 1.10 | forst-rs |
| q2 | 55.2 | 58.1 | +2.9 | 1.05 | forst-rs |
| q3 | 89.5 | 94.3 | +4.8 | 1.05 | forst-rs |
| q4 | 891.6 | 955.7 | +64.1 | 1.07 | forst-rs |
| q5 | 417.3 | 393.4 | -23.9 | 0.94 | RocksDB |
| q7 | 1051.7 | 916.7 | -135.0 | 0.87 | RocksDB |
| q8 | 110.7 | 106.9 | -3.8 | 0.97 | RocksDB |
| q9 | 1271.9 | 1544.1 | +272.2 | 1.21 | forst-rs |
| q10 | 118.4 | 124.8 | +6.4 | 1.05 | forst-rs |
| q11 | 290.8 | 297.9 | +7.1 | 1.02 | forst-rs |
| q12 | 117.2 | 110.1 | -7.1 | 0.94 | RocksDB |
| q13 | 95.0 | 97.3 | +2.3 | 1.02 | forst-rs |
| q14 | 54.9 | 60.4 | +5.5 | 1.10 | forst-rs |
| q15 | 743.3 | 1148.5 | +405.2 | 1.55 | forst-rs |
| q16 | 697.6 | 1050.4 | +352.8 | 1.51 | forst-rs |
| q17 | 249.4 | 213.1 | -36.3 | 0.85 | RocksDB |
| q18 | 347.6 | 404.4 | +56.8 | 1.16 | forst-rs |
| q19 | 438.1 | 425.2 | -12.9 | 0.97 | RocksDB |
| q20 | 937.7 | 1116.6 | +178.9 | 1.19 | forst-rs |
| q21 | 112.4 | 127.5 | +15.1 | 1.13 | forst-rs |
| q22 | 79.5 | 91.8 | +12.3 | 1.15 | forst-rs |

## Largest Deltas

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

## q9 Performance and Correctness

q9 was the only query that required additional memory tuning to finish under
the 2 x 4c/16g resource model. The successful baseline q9 run used a tighter
forst-rs profile:

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

Baseline q9 result:

| backend | status | wall_s | src_out | out_rows |
| --- | --- | ---: | ---: | ---: |
| forst-rs | FINISHED | 1271.9 | 98000000 | 91212230 |
| RocksDB | FINISHED | 1544.1 | 98000000 | 91211783 |

q9 conclusion:

- The q9 failure mode before tuning was TaskManager cgroup memory pressure, not
  BOS/S3 reachability.
- The successful profile reduced JVM/process memory, forst-rs native memory,
  async-state buffering, background work, and local file cache pressure enough
  to stay inside the 16GiB/TM cgroup.
- The fixed-CSV accuracy rerun confirmed that q9 final winning bids are correct
  on both backends.

q9 accuracy result:

| q9 accuracy item | value |
| --- | ---: |
| expected winner auctions | 5567 |
| forst-rs final winner auctions | 5567 |
| RocksDB final winner auctions | 5567 |
| backend final-winner differences | 0 |
| forst-rs wrong/missing/extra winners | 0/0/0 |
| RocksDB wrong/missing/extra winners | 0/0/0 |

## Follow-up q9 Confirmation

A later BOS/S3 q9-only confirmation run was executed after the 2026-06-16 full
baseline. This run is useful as a q9 check, but it does not replace the 1.14x
full-sweep baseline above.

| item | value |
| --- | --- |
| Run id | `bos-q9-frs-0618-1347-tight64` |
| Query/backend | q9 / forst-rs |
| Result | FINISHED |
| wall_s | 1502.1 |
| src_out | 98000000 |
| out_rows | 91212692 |
| q9 speedup vs RocksDB baseline | 1544.1 / 1502.1 = 1.03x |
| q9 regression vs 2026-06-16 forst-rs baseline | 1502.1 / 1271.9 = 1.18x |

Interpretation: the follow-up q9 run still finished faster than the historical
RocksDB q9 baseline, but it was slower than the 2026-06-16 forst-rs q9 baseline.
The project-level 1.14x claim remains tied to the 2026-06-16 q0-q22 full BOS/S3
sweep.

## Follow-up Work

Recommended next steps:

1. Investigate q7 and q17, where RocksDB remains meaningfully faster.
2. Investigate smaller RocksDB wins on q5, q8, q12, and q19.
3. Keep q9 as a separate low-memory profile for the 2 x 4c/16g resource model.
4. Rerun sensitive queries first after rebuilding the latest BOS image: q4, q7,
   q9, q17, q5, q12, and q19.
