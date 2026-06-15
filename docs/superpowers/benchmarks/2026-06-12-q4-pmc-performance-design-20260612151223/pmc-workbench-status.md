# PMC workbench status

Date: 2026-06-12
Agent id: `pmc-forst-q4-perf-20260612-1512`
Remote session: one active relay-backed session to `lijunqing@yq01-sys-hic-k8s-p40-0000.yq01.baidu.com`.
Connection rule: `sshdata00` is a shell entry command/alias, not `ssh sshdata00`. If this session drops, reconnect by running `sshdata00` from a shell where the alias is loaded, or use the relay/expect wrapper already available on this host.

## Current remote workspace

- Flink worktree: `/home/users/lijunqing/code/stczwd/flink-nexmark-jdk17-100m-20260612/nexmark-100m-work`
- ForSt repo: `/home/users/lijunqing/code/stczwd/ForSt`
- Draft dir: `/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-12-q4-pmc-performance-design-20260612151223`
- Main draft: `q4-pmc-performance-design-draft.md`

## Verified script details

`run-one-full.sh` disables checkpointing only when:

```bash
CHECKPOINT_INTERVAL=disabled
# or
CHECKPOINT_INTERVAL=none
```

Do not use `CHECKPOINT_INTERVAL=0` for the Q4 checkpoint-off A/B unless RD changes the script.

`run-matrix.sh` default container resources are:

- `CONTAINER_MEMORY=40g`
- `CONTAINER_CPUS=8`
- `PARALLELISM=8`
- `TM_SLOTS=8`

For Q4 mini diagnosis, override to smaller resources first:

```bash
CONTAINER_MEMORY=16g CONTAINER_CPUS=4 PARALLELISM=2 TM_SLOTS=2
```

## PMC execution order

1. Do not run another 100M Q4 until the mini completion gate is understood.
2. First run 100k Q4 no-plateau with checkpoint ON, one matrix job only.
3. If 100k completes, run 100k Q4 with `CHECKPOINT_INTERVAL=disabled`.
4. If checkpoint OFF is materially faster, RD starts with flush/checkpoint/readback counters.
5. If both ON and OFF are slow, RD starts with Q4 write-path and local SST read-path counters plus MB2/MB3 microbench.
6. Only after 100k is stable should 1M Q4 be retried.

## Corrected Q4 mini commands

Checkpoint ON:

```bash
cd /home/users/lijunqing/code/stczwd/flink-nexmark-jdk17-100m-20260612/nexmark-100m-work
RUN_LABEL=q4-mini100k-noplateau-$(date +%Y%m%d%H%M%S) \
QUERIES='q4' VARIANTS='forst-local forst-rs-lib' \
EVENTS_NUM=100000 TPS=10000 MAXSEC=600 POLL_SEC=5 \
CONTAINER_MEMORY=16g CONTAINER_CPUS=4 PARALLELISM=2 TM_SLOTS=2 \
DONE_ON_SRC=1 DONE_ON_PLATEAU=0 JFR_ENABLED=1 JFR_DELAY=20s JFR_DURATION=300s \
./scripts/run-matrix.sh
```

Checkpoint OFF:

```bash
cd /home/users/lijunqing/code/stczwd/flink-nexmark-jdk17-100m-20260612/nexmark-100m-work
RUN_LABEL=q4-mini100k-nockpt-$(date +%Y%m%d%H%M%S) \
QUERIES='q4' VARIANTS='forst-local forst-rs-lib' \
EVENTS_NUM=100000 TPS=10000 MAXSEC=600 POLL_SEC=5 \
CONTAINER_MEMORY=16g CONTAINER_CPUS=4 PARALLELISM=2 TM_SLOTS=2 \
DONE_ON_SRC=1 DONE_ON_PLATEAU=0 CHECKPOINT_INTERVAL=disabled \
JFR_ENABLED=1 JFR_DELAY=20s JFR_DURATION=300s \
./scripts/run-matrix.sh
```

## RD handoff summary

Minimum RD patch should be diagnostics-first:

- Read-path counters: open/close/read/pread counts, logical vs physical bytes, whole-SST and chunk cache hits/misses.
- Write-path counters: put/delete/merge/write-batch count, batch histogram, memtable insert ns, WAL copy bytes, flush/compaction durations, stall time.
- JNI family counters: get/put/delete/merge/write-batch/iterator open/next/close counts and nanos.
- Optional experiment flag: `FRS_LOCAL_DIRECT_PREAD=1`, local POSIX SST only, held fd + exact-block pread, S3 path unchanged.

Do not ask RD for broad rewrite or default behavior changes until MB1 plus one relevant microbench pins the blocker.

## Boundary update: route and change classification

Added user boundary on 2026-06-12:

- `ForSt backend + forst-rs lib` is the JDK17/general-purpose route. Target: stable >1.0x over current ForSt backend, not final peak performance.
- Final high-performance route remains `forst-rs backend + forst-rs/FFI`.
- Except for `compact_jni`, ForSt-RS fixes must consider FFI performance impact.
- All RD tasks must classify changes as `JNI-only`, `compact_jni-only`, or `FFI-shared`.

Updated acceptance:

- JNI-only: acceptable for JDK17 route if it improves `ForSt backend + forst-rs lib` and does not touch shared Rust/FFI behavior.
- compact_jni-only: evaluated only by compact_jni-specific benchmark; FFI impact can be N/A but must be stated.
- FFI-shared: any Rust engine/storage/cache/write-path/SST/iterator change needs FFI microbench or ForSt-RS backend evidence before default enablement.

Current Q4 implication:

- MB2 local SST exact-block read: FFI-shared.
- MB3 write-batch/retract: FFI-shared if Rust engine changes; JNI-only if only bridge batching changes.
- MB4 JNI boundary split: JNI-only unless it changes shared ABI/data layout.
- Q4 small completion gate remains first; after that, route-specific acceptance applies.

## Boundary update: Q4/Q7 resource-pressure revalidation

Added user boundary on 2026-06-12:

- Past Q4/Q7 timeout or incomplete results may have been affected by disk pressure.
- Do not permanently attribute Q4/Q7 to completion, measurement, state-path, or engine bottlenecks using old pressure-tainted results alone.
- QA must rerun Q4/Q7 using 1M fixed CSV after resource pressure is relieved.

QA acceptance now requires:

- Resource snapshot before and after each query: time, host, memory, disk, iostat if available, docker jobs, active Nexmark/ForSt/Flink/cargo processes.
- Serial single-query execution: one query and one variant at a time when possible; no overlapping heavy jobs.
- Same-input hash: record CSV path, row count, and input file hash manifest; compare outputs from the same input hash.
- Output compare: deterministic sorted row count plus hash; handle ordering by sorting before hashing.
- Timeout downgrade: if 1M times out, rerun 100k under the same clean-resource rules; if 100k passes but 1M fails, classify as `SCALE_BLOCKED`; if 100k fails, classify as `FUNCTIONAL_BLOCKED`.
- Resource taint: if disk pressure or competing jobs are present, classify as `RESOURCE_BLOCKED`, not durable backend evidence.

PMC implication: Q4 remains P0, but root-cause language must say `old evidence under possible disk pressure` until QA provides clean 1M fixed-CSV evidence.

## Boundary update: long 1M fixed-CSV accuracy first

Added user boundary on 2026-06-12:

- Current 1M accuracy can run for a long time; the goal is correctness evidence.
- Timeout-heavy queries such as Q4/Q7/Q9/Q11 should first get longer `MAXSEC` 1M fixed-CSV serial single-query reruns.
- Do not rush to 100k downgrade or profiler while a clean 1M run is still progressing.

Updated QA priority:

1. Long 1M fixed CSV, serial single query and variant, with resource snapshots and input/output hashes.
2. Classify as `LONG_1M_ACCEPTED`, `LONG_1M_MISMATCH`, `LONG_1M_RESOURCE_BLOCKED`, or `LONG_1M_SCALE_BLOCKED`.
3. Only after `LONG_1M_SCALE_BLOCKED` or mismatch should QA/PMC request 100k downgrade or profiler.

Suggested initial long budget: `MAXSEC=7200`, adjustable upward if the isolated run is clean and making progress.

## Boundary update: final 1M fixed-CSV accuracy gate

Added user boundary on 2026-06-12:

- QA is authorized to stop the old accuracy run and rerun the 1M fixed-CSV matrix with long timeout.
- PMC's final near-term gate is: ensure 1M Nexmark fixed-CSV accuracy is clean.
- Wait for QA's complete matrix before profiler/performance optimization judgment.

PMC classification after QA matrix:

- `ACCURACY_PASS`: correctness clean; query can enter performance/profiler triage.
- `ACCURACY_MISMATCH`: correctness/debugging first; no performance optimization decision.
- `ACCURACY_RESOURCE_BLOCKED`: rerun under cleaner resources; no durable conclusion.
- `ACCURACY_SCALE_BLOCKED`: profiler or smaller reproduction is valid to isolate scale cliff.

PMC must not use old timeout/incomplete evidence as final root cause once QA restarts under the long 1M fixed-CSV policy.

## Boundary update: 100K fixed-CSV hard gate and config audit

Added user boundary on 2026-06-12:

- 1M fixed CSV is no longer the only hard accuracy gate.
- Current hard gate: `100K fixed CSV correctness PASS` plus key-query configuration audit.
- `10K fixed CSV` is a smoke gate.
- `1M fixed CSV` becomes extended confidence/scale validation.
- Q4 1M slowness must be audited for suboptimal configuration before PMC treats it as an engine bottleneck.

QA acceptance now uses:

- `SMOKE_PASS_10K` / `SMOKE_FAIL_10K`
- `CORRECTNESS_PASS_100K` / `CORRECTNESS_MISMATCH_100K`
- `CONFIG_BLOCKED`
- `RESOURCE_BLOCKED`
- `CONFIDENCE_PASS_1M`
- `SCALE_SLOW_1M`

For Q4/Q7/Q9/Q11, QA must include config audit: CSV path/hash, query/variant/image/commit/lib sha, events/TPS/MAXSEC, done-on settings, resources, checkpoint/async/minibatch settings, output materialization, source/output trajectory, state dir size, native lib loaded, and resource snapshot.

PMC implication: profiler/performance optimization may proceed after `CORRECTNESS_PASS_100K` plus config audit. 1M is useful for confidence and scale diagnosis, but lack of 1M completion is not by itself a correctness blocker if 100K passes.

## Status update: Q4 100K mismatch

User reported Q4 100K fixed-CSV mismatch. PMC classification is now `CORRECTNESS_MISMATCH_100K` for Q4.

Current decision:

- Q4 performance/profiler work is blocked.
- Q4 goes to correctness-first debugging.
- Need QA/RD mismatch artifact: input hash, output row/hash per variant, first differing sorted rows, config audit, resource snapshot.
- Reopen Q4 performance only after `CORRECTNESS_PASS_100K`.
