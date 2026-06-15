# Q4 PMC performance design draft

Date: 2026-06-12
Owner: PMC agent `pmc-forst-q4-perf-20260612-1512`
Scope: Q4 only, ForSt backend + forst-rs lib first; ForSt-RS backend is a second lane after Q4 finishes reliably.
Status: draft, not a final conclusion.

## Current evidence

Remote host: `yq01-sys-hic-k8s-p40-0000.yq01.baidu.com`.
Flink worktree: `/home/users/lijunqing/code/stczwd/flink-nexmark-jdk17-100m-20260612/nexmark-100m-work`.
ForSt repo: `/home/users/lijunqing/code/stczwd/ForSt` at `49eb9aa26255` on branch `forst-rs`.

100M performance TSV: `results/perf100m-worktree-jdk17-20260612001856.tsv`.

| variant | query | mode | wall_ms | src_out | expected_src | out_rows | note |
|---|---:|---|---:|---:|---:|---:|---|
| forst-local | q4 | SOURCE_PLATEAU | 230201 | 359568 | 98000000 | 53827 | stable_src_polls=10;stable_out_polls=3 |
| forst-rs-lib | q4 | SOURCE_PLATEAU | 3516189 | 1253333 | 98000000 | 290596 | stable_src_polls=6;stable_out_polls=4 |

1M CSV accuracy TSV: `results/accuracy1m-csv-rate10k-all-20260612141609.tsv`.

| variant | query | mode | wall_ms | out_rows | note |
|---|---:|---|---:|---:|---|
| forst-local | q4 | TIMEOUT | 1201246 | 4469 | maxsec |
| forst-rs-lib | q4 | TIMEOUT | 1200627 | 13600 | maxsec |

Interpretation: Q4 is not merely a 100M scale issue. The 1M CSV run also fails to finish within 1200s for both variants. Therefore the first gate is Q4 completion and diagnosis at small scale, before claiming any 100M performance ratio.

## P0 decision

Q4 is P0 for the performance campaign.

Q4 must be treated as an execution-path failure until a small, deterministic run finishes. Full 100M timing is not a trustworthy comparison while `src_out << expected_src` and both variants report plateau or timeout.

Do not start another 100M Q4 run until the mini matrix below can complete or produce a pinned root cause.

## Hypothesis tree

### H0: Harness termination or source-progress accounting is wrong

Symptoms:
- 100M Q4 ends as SOURCE_PLATEAU with very low `src_out` for both variants.
- 1M CSV Q4 times out for both variants.

Data needed:
- Whether the source generator actually stops, backpressures, or is blocked by downstream state.
- Whether `src_out` is the right completion signal for Q4 under this SQL.
- Whether retract/changelog output makes `out_rows` unsuitable as a finish signal.

Falsifier:
- A 100k or 1M Q4 run with `DONE_ON_PLATEAU=0` reaches expected source and produces terminal FINISHED/SOURCE_DONE.

### H1: Q4 common ForSt backend path is write/flush/compaction bound

Symptoms:
- Both `forst-local` and `forst-rs-lib` fail 1M CSV completion.
- Q4 is interval join plus retract aggregation, which stresses put/delete/merge/write-batch and flush/compaction.

Profiler signals:
- High time in memtable insert, write batch apply, WAL copy, flush worker, SST writer, bloom/index build, compaction.
- Rising write stalls or pending compaction bytes.
- Checkpoint-induced read/write bursts every 30s.

Falsifier:
- Disabling checkpoint or reducing input to 100k still cannot move source/output, and profiles show source/backpressure rather than write/compaction.

### H2: Local SST read path still amplifies Q4 block reads

Prior docs pinned a local-cache/chunk path risk: whole-SST fast path misses can degrade to open+read+close of 1MiB chunks for small block reads. Q4 can hit this after state flush/compaction.

Profiler signals:
- High `open`, `close`, `read`, `pread`, `serial_read_at`, `chunk_bytes`, cache miss counters.
- Many reads per source record after first flush.
- State dir grows into compacted SSTs before throughput collapses.

Falsifier:
- Exact-block held-fd pread benchmark is not faster than current chunk path, or Q4 profile has no open/read storm.

### H3: ForSt-RS lib adds extra JNI/native bridge overhead

Symptoms:
- 100M `forst-rs-lib` Q4 runs much longer than `forst-local`, but consumes more rows before plateau.
- Native loaded lib differs: jar-extracted `libforstjni-linux64.so` vs bench `libforstjni.so`.

Profiler signals:
- Time in JNI get/put/write batch, iterator open/next/close, compact/openClose boundaries.
- High per-call native transition count, small batch sizes, repeated byte array copies.

Falsifier:
- Microbench shows JNI/native bridge overhead is below 10% of Q4 wall, while engine read/write dominates.

### H4: ForSt-RS backend async pipeline is a separate second-lane issue

Existing diagnosis points to depth-1 inline FFM dispatch for join-heavy queries. However current Q4 failure occurs with ForSt backend + forst-rs lib too, so this must not be the first root cause for Q4 completion.

Profiler signals:
- For ForSt-RS backend only: mailbox parks, completed futures, depth=1, blocking FFM iterator drain.

Falsifier:
- ForSt backend Q4 still fails with no ForSt-RS backend in path.

## Mini benchmark plan

All benchmarks must be small enough to leave at least 40G memory free on the host. Do not run concurrently with active accuracy/profiler jobs.

### MB1: Q4 completion mini matrix

Interface: Flink Q4 via existing `run-matrix.sh`.
Data scale: 100k, then 1M only after 100k finishes.
Resources: 2 parallelism, 2 slots, 4 CPU/16G container if using current defaults; one job at a time.
Variants: `forst-local`, `forst-rs-lib`.
Settings:
- `DONE_ON_PLATEAU=0` for first pass to avoid false plateau.
- `MAXSEC=600` for 100k, `MAXSEC=1800` for 1M.
- Checkpoint ON and OFF as A/B.

Pass standard:
- 100k must FINISH or reach expected source for both variants within 600s.
- If 100k cannot finish, Q4 is an execution-path bug/perf cliff, not a 100M optimization problem.

Nexmark mapping:
- Validates Q4 can make progress and gives a stable local reproduction before 100M.

### MB2: Local SST exact-block read vs chunk path

Interface: Rust storage microbench under `crates/forst-rs-bench` or a temporary bench file in RD worktree.
Data scale:
- Create 1-4 SST-like files, 128MB each.
- 100k random 16KB/32KB/64KB block reads with Q4-like locality.
Resources: single process, <=8G memory.
Variants:
- current chunk path: open/read/close or range-cache chunk path.
- proposed held-fd exact-block pread path.
Metrics:
- ops/s, p50/p95 latency, open_count, close_count, read_count, bytes_read, cache_hit/miss.
Pass standard:
- open/close count down >90%.
- p95 latency down >=2x or bytes_read/op down >=8x.

Nexmark mapping:
- Tests the Q4 post-flush read amplification hypothesis without running Nexmark.

### MB3: Q4 write-batch/retract pattern

Interface: Rust engine or JNI compat microbench.
Data scale:
- 1M operations over 100k keys.
- Mix: put 55%, merge/update 30%, delete/retract 15%.
- Batch sizes: 1, 16, 128, 1024.
Resources: <=8G memory, local temp dir only.
Metrics:
- ops/s, batch size histogram, WAL bytes, memtable insert ns/op, flush count/duration, compaction count/duration, stall time.
Pass standard:
- batch size >=128 improves throughput >=1.5x over per-row path.
- no write stall >5% of wall in 1M-op run.

Nexmark mapping:
- Q4 join + retract aggregation stresses the write path more than q7/q9/q20.

### MB4: JNI boundary hotpath split

Interface: existing `compat_jni_forst_backend_hotpaths` or new RD extension.
Data scale:
- 100k/1M get, put, delete, iterator-open, iterator-next, iterator-close, write-batch operations.
Resources: single process, <=8G memory.
Metrics:
- ns/op per JNI call family, bytes copied/op, batch size, native transition count.
Pass standard:
- any call family consuming >20% equivalent Q4 CPU becomes RD target.
- per-row JNI paths must have a batch alternative before production optimization is proposed.

Nexmark mapping:
- Separates ForSt engine work from ForSt-RS lib bridge cost.

### MB5: Checkpoint ON/OFF Q4 mini

Interface: existing Q4 mini matrix.
Data scale: 100k first, 1M second.
Resources: same as MB1.
Metrics:
- wall time, src_out trajectory, checkpoint duration, checkpoint bytes, read/write burst around checkpoint, state dir size.
Pass standard:
- If OFF completes and ON times out, checkpoint/flush/readback is P0.
- If both fail, focus on operator state path and write/read hotpath.

Nexmark mapping:
- Q4 production runs use checkpoint interval 30s; this tells whether checkpoint is the first-order blocker.

## Profiler metrics required

For Q4 mini/profiler runs collect:

- Java/JFR: TaskManager CPU, allocation, lock/park, native method, socket/file IO, GC, thread states.
- Native/system: `open`, `close`, `read`, `pread`, bytes_read, syscall latency, page faults, context switches.
- ForSt counters: memtable insert, write batch apply, flush count/duration, compaction count/duration, pending compaction bytes, write stall time.
- Cache counters: whole-SST hit/miss, chunk hit/miss, direct pread path count, read amplification bytes/logical_bytes.
- JNI counters: get/put/delete/merge/write-batch/iterator open/next/close call counts and total nanos.
- Flink counters: busy/backpressured/idle time, checkpoint duration/bytes, source records, sink records, async-state queue depth if ForSt-RS backend is used later.

## Commands for the remote execution agent

Run only when no other heavy run is active and at least 40G memory is free.

```bash
cd /home/users/lijunqing/code/stczwd/flink-nexmark-jdk17-100m-20260612/nexmark-100m-work
free -g
/home/work/dockerd/bin/docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' | grep -Ei 'NAMES|nexmark|accuracy|frs|forst' || true
```

100k completion run, no plateau stop:

```bash
RUN_LABEL=q4-mini100k-noplateau-$(date +%Y%m%d%H%M%S) \
QUERIES='q4' VARIANTS='forst-local forst-rs-lib' \
EVENTS_NUM=100000 TPS=10000 MAXSEC=600 PARALLELISM=2 TM_SLOTS=2 \
DONE_ON_SRC=1 DONE_ON_PLATEAU=0 JFR_ENABLED=1 JFR_DELAY=20s JFR_DURATION=300s \
./scripts/run-matrix.sh
```

100k checkpoint-disabled A/B, if script supports `CHECKPOINT_INTERVAL=disabled` or equivalent:

```bash
RUN_LABEL=q4-mini100k-nockpt-$(date +%Y%m%d%H%M%S) \
QUERIES='q4' VARIANTS='forst-local forst-rs-lib' \
EVENTS_NUM=100000 TPS=10000 MAXSEC=600 PARALLELISM=2 TM_SLOTS=2 \
DONE_ON_SRC=1 DONE_ON_PLATEAU=0 CHECKPOINT_INTERVAL=disabled \
./scripts/run-matrix.sh
```

After each run:

```bash
RUN_LABEL=<label>
awk -F '\t' 'NR==1 || $1==label {print}' label="$RUN_LABEL" results/${RUN_LABEL}.tsv
for f in results/${RUN_LABEL}-*q4.log; do
  echo "## $f"
  grep -E 'bench policy|RUNNING|RESULT|checkpoint|loaded native|disk usage|ERROR|Exception' "$f" | tail -120
done
```

## Minimal RD change request

RD should not start with broad rewrites. First add opt-in diagnostics and one isolated switch.

1. Add Q4 read-path counters in forst-rs storage:
   - `open_count`, `close_count`, `read_count`, `pread_count`, `logical_read_bytes`, `physical_read_bytes`, whole-SST cache hit/miss, chunk cache hit/miss.
2. Add write-path counters:
   - `put/delete/merge/write_batch` counts, batch size histogram, memtable insert nanos, WAL copy bytes, flush/compaction duration, write stall nanos.
3. Add JNI family counters in compat layer:
   - get/put/delete/merge/write-batch/iterator-open/iterator-next/iterator-close call count and total nanos.
4. Add an opt-in local SST exact-block read experiment, default OFF:
   - environment/config: `FRS_LOCAL_DIRECT_PREAD=1` or equivalent.
   - use held fd + positional pread for local POSIX SST reads.
   - do not change S3 path in this experiment.
5. Add or extend microbench targets for MB2-MB4.

Acceptance before production code review:
- 100k Q4 finishes for both variants.
- At least one microbench pins a bottleneck with >=2x local improvement or proves the hypothesis false.
- Q4 1M CSV either finishes or has a single profiler-backed blocker with counters.
- No production default change without accuracy plus perf evidence.

## Current PMC recommendation

First run MB1 100k no-plateau. If it cannot finish, profile that exact small run. If it finishes, run checkpoint ON/OFF and then MB2/MB3 depending on whether the profile is read/open dominated or write/flush dominated.

The current most likely first-order issue is common Q4 state-path pressure, not ForSt-RS backend async pipeline. ForSt-RS backend async-state pipelining remains important for q7/q9/q20 and later Q4 backend work, but it should not distract from making Q4 finish under ForSt backend + forst-rs lib first.

## Boundary update: JNI, compact_jni, and FFI-shared changes

User boundary added on 2026-06-12:

1. For ForSt-RS fixes, every change except `compact_jni` must account for FFI performance impact, because FFI remains the main path for later performance benchmarks.
2. `ForSt backend + forst-rs lib` is the JDK17/general-purpose route. Its target is stable improvement over the current ForSt backend, not final peak performance.
3. The final high-performance route remains `forst-rs backend + forst-rs/FFI`; therefore every Q4 microbench and RD proposal must declare whether it is JNI-only, compact_jni-only, or FFI-shared.

### Change classification

| Class | Scope | Allowed examples | Must not do | Acceptance gate |
|---|---|---|---|---|
| JNI-only | `ForSt backend + forst-rs lib` compatibility/JDK17 bridge path | JNI call batching, byte-array copy reduction, JNI call counters, JDK17 smoke/perf parity | Change FFI ABI, FFI buffer ownership, or Rust engine semantics without FFI measurement | Stable Q4 mini completion and >1.0x over ForSt backend for this route; no claim of final peak route |
| compact_jni-only | Narrow compact JNI surface that is intentionally not shared with FFI | compact/openClose JNI mechanics, compact_jni-specific call shape, compatibility experiments | Use compact_jni evidence to justify FFI production changes | compact_jni benchmark improves its own target; FFI impact is explicitly N/A |
| FFI-shared | Rust engine, storage, cache, write path, SST, iterator, batching, memory layout, counters used by both JNI and FFI | local direct pread, SST writer batching, memtable/write-batch changes, iterator engine changes, cache admission, shared Rust counters | Optimize JNI while regressing FFI or leaving FFI unmeasured | Must include FFI microbench or ForSt-RS backend perf evidence before merge/default enablement |

### Revised acceptance standards

For `ForSt backend + forst-rs lib`:

- Goal: stable >1.0x over current ForSt backend on Q4 after Q4 can complete at mini scale, then 1M, then selected 100M.
- Do not over-design this path as the final performance ceiling. It is valuable for JDK17 and general compatibility.
- JNI-only changes can be accepted if they improve this route and do not touch shared Rust/FFI behavior.

For `forst-rs backend + forst-rs/FFI`:

- Goal: final high-performance route; Q4 design must preserve or improve FFI path.
- Any shared Rust engine/storage/cache/write-path optimization must produce FFI evidence, not only JNI evidence.
- For Q4, FFI evidence may initially be a microbench matching the same interface shape: iterator prefix scan, get/put/write-batch, SST exact-block read, SST writer build, or async-state dispatch depth.

### Implications for current Q4 plan

- MB2 local SST exact-block read is FFI-shared because it changes Rust storage/read behavior. RD must measure FFI or ForSt-RS backend impact before any default change.
- MB3 Q4 write-batch/retract pattern is FFI-shared if it changes Rust write path, but JNI-only if it only batches JNI calls above an unchanged engine API.
- MB4 JNI boundary hotpath split is JNI-only unless it changes Rust ABI/data layout. Its result cannot justify FFI defaults by itself.
- A future `compact_jni` experiment must be documented separately as compact_jni-only and should not be mixed into Q4 FFI acceptance.
- Q4 completion gate remains route-neutral: first prove a small Q4 can finish, then classify the bottleneck and choose the correct route.

### RD handoff update

Every RD proposal must include a classification header:

```text
Change class: JNI-only | compact_jni-only | FFI-shared
Affected route: ForSt backend + forst-rs lib | forst-rs backend + forst-rs/FFI | both
Primary benchmark: <mini/micro benchmark name>
FFI impact evidence: required | not applicable because compact_jni-only | pending and not mergeable
Default behavior change: yes/no
```

No shared Rust storage/engine optimization should be accepted with only JNI evidence. The exception is `compact_jni`, which must be explicitly scoped as compact_jni-only.

## Boundary update: Q4/Q7 timeout under disk pressure requires revalidation

User boundary added on 2026-06-12:

Past Q4/Q7 timeout or incomplete results may have been affected by machine disk pressure. If current resources have improved, Q4/Q7 must not be permanently classified as completion or measurement failures from old runs alone. The next QA gate is a fresh 1M fixed-CSV accuracy revalidation under a recorded resource snapshot.

### Revised interpretation

- Current Q4 evidence remains sufficient to mark Q4 as P0 for revalidation and diagnosis.
- It is not sufficient to permanently attribute Q4/Q7 to measurement, completion, state-path, or engine bottlenecks until a fresh resource-clean 1M fixed-CSV run is available.
- Any PMC/RD root-cause statement for Q4/Q7 must distinguish:
  - `old evidence under possible disk pressure`,
  - `fresh resource-snapshotted 1M fixed-CSV evidence`, and
  - `microbench/profiler evidence`.

### QA revalidation acceptance criteria

QA should rerun Q4 and Q7 with the following controls before PMC treats timeout/incomplete as durable:

1. Resource snapshot before and after each query:
   - `date -Is`, `hostname`, `free -g`, `df -h` for the work/state/tmp disks, `iostat -xz 1 5` if available, docker container list, and active user processes matching Nexmark/ForSt/Flink.
   - Record whether other Nexmark, cargo build, profiler, compaction-heavy, or docker jobs were running.
2. Serial single-query execution:
   - Run one query and one variant at a time; do not overlap Q4/Q7 with other accuracy or profiler jobs.
   - Use fixed CSV input, not generator-only input, and keep the same CSV directory across variants.
3. Same-input verification:
   - Record CSV directory, row count, and a stable hash manifest of the input files.
   - Compare ForSt backend vs ForSt-RS route outputs on the same input hash.
   - For deterministic queries, compare sorted output row count plus hash. If Q7 has nondeterministic ordering, sort before hashing.
4. Timeout handling:
   - If 1M still times out, do not immediately classify the backend as wrong.
   - First downgrade to 100k fixed CSV under the same resource snapshot and single-query rule.
   - If 100k also times out, request a dedicated low-pressure resource window and collect profiler/JFR on the 100k run.
   - If 100k passes but 1M times out, classify as scale/performance cliff and hand PMC/RD the 100k-vs-1M delta plus profiler.
5. Acceptance states:
   - `ACCEPTED`: both variants finish 1M fixed CSV, outputs match by row count and hash, and resource snapshot shows no material pressure.
   - `RESOURCE_BLOCKED`: run overlaps with disk pressure, low free disk, high iowait, or competing heavy jobs; result is not a durable correctness/performance signal.
   - `SCALE_BLOCKED`: 100k passes but 1M times out under clean resources; treat as performance cliff needing profiler.
   - `FUNCTIONAL_BLOCKED`: 100k fails or output hash mismatches under clean resources; treat as correctness/execution bug before performance tuning.

### QA command template

Use this shape, adjusting only label, query, variant, and CSV path:

```bash
cd /home/users/lijunqing/code/stczwd/flink-nexmark-jdk17-100m-20260612/nexmark-100m-work
LABEL=qa-fixedcsv-1m-q4q7-$(date +%Y%m%d%H%M%S)
{
  echo "== resource pre =="
  date -Is
  hostname
  free -g
  df -h . state tmp /home/users/lijunqing 2>/dev/null || true
  iostat -xz 1 5 2>/dev/null || true
  ps -u "$USER" -o pid,etime,cmd | grep -Ei 'nexmark|run-matrix|flink|forst|cargo|accuracy|perf' | grep -v grep || true
  /home/work/dockerd/bin/docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' | grep -Ei 'NAMES|nexmark|accuracy|forst|flink' || true
} | tee "results/${LABEL}.resource-pre.log"

# Run serially: one query x one variant at a time. Example Q4 both variants through the matrix loop:
RUN_LABEL="$LABEL" \
QUERIES='q4 q7' VARIANTS='forst-local forst-rs-lib' \
EVENTS_NUM=1000000 TPS=10000 MAXSEC=1800 POLL_SEC=5 \
CONTAINER_MEMORY=16g CONTAINER_CPUS=4 PARALLELISM=2 TM_SLOTS=2 \
DONE_ON_SRC=1 DONE_ON_PLATEAU=0 \
MEASURE_SCRIPT=/bench/scripts/measure-accuracy-csv.sh \
CSV_DIR=/bench/accuracy/csv-1m-rate10k \
ACCURACY_FILE_OUTPUT=1 ACCURACY_FLUSH_EVERY=1024 \
./scripts/run-matrix.sh

{
  echo "== resource post =="
  date -Is
  free -g
  df -h . state tmp /home/users/lijunqing 2>/dev/null || true
  iostat -xz 1 5 2>/dev/null || true
} | tee "results/${LABEL}.resource-post.log"
```

Note: if `run-matrix.sh` cannot enforce strict serial per variant beyond its normal nested loop, QA should run separate labels per `(query, variant)` to preserve isolation.

## Boundary update: long 1M fixed-CSV accuracy runs before downgrade/profiler

User boundary added on 2026-06-12:

Current 1M accuracy runs are allowed to run for a long time. For timeout-heavy queries such as Q4, Q7, Q9, Q11, the immediate goal is correctness evidence, not quick performance classification. Therefore PMC should prefer longer `MAXSEC` 1M fixed-CSV single-query reruns before downgrading to 100k or entering profiler work.

### Revised QA priority

1. Run 1M fixed CSV as serial single-query, single-variant jobs with a longer `MAXSEC`.
2. Capture resource snapshots and input/output hashes as before.
3. Wait for correctness evidence unless the run is clearly resource-blocked or externally killed.
4. Only downgrade to 100k if the long 1M run cannot produce useful evidence because of resource pressure, infrastructure failure, or repeated non-progress under a clean resource snapshot.
5. Only move to profiler after correctness status is known or after a long clean run shows a durable scale/performance cliff.

### Updated timeout handling

Timeout handling is now staged:

- `LONG_1M_PENDING`: 1M fixed CSV is still running and producing progress. Do not interrupt just to get a faster answer.
- `LONG_1M_ACCEPTED`: 1M fixed CSV finishes and output row/hash comparison passes.
- `LONG_1M_MISMATCH`: 1M fixed CSV finishes but output row/hash differs; correctness bug takes priority over performance.
- `LONG_1M_RESOURCE_BLOCKED`: 1M fixed CSV overlaps disk pressure or competing heavy jobs; rerun in a cleaner window.
- `LONG_1M_SCALE_BLOCKED`: long 1M fixed CSV under clean resources shows no meaningful progress or times out at the extended budget; then run 100k fixed CSV and/or profiler to isolate the cliff.

### Suggested long 1M command shape

Use separate labels per query and variant if strict isolation is needed. Prefer Q4/Q7/Q9/Q11 first.

```bash
cd /home/users/lijunqing/code/stczwd/flink-nexmark-jdk17-100m-20260612/nexmark-100m-work
LABEL=qa-fixedcsv-1m-long-q4-$(date +%Y%m%d%H%M%S)
QUERY=q4
VARIANT=forst-local
{
  echo "== resource pre =="
  date -Is
  hostname
  free -g
  df -h . state tmp /home/users/lijunqing 2>/dev/null || true
  iostat -xz 1 5 2>/dev/null || true
  ps -u "$USER" -o pid,etime,cmd | grep -Ei 'nexmark|run-matrix|flink|forst|cargo|accuracy|perf' | grep -v grep || true
  /home/work/dockerd/bin/docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' | grep -Ei 'NAMES|nexmark|accuracy|forst|flink' || true
} | tee "results/${LABEL}.resource-pre.log"

RUN_LABEL="$LABEL" \
QUERIES="$QUERY" VARIANTS="$VARIANT" \
EVENTS_NUM=1000000 TPS=10000 MAXSEC=7200 POLL_SEC=10 \
CONTAINER_MEMORY=16g CONTAINER_CPUS=4 PARALLELISM=2 TM_SLOTS=2 \
DONE_ON_SRC=1 DONE_ON_PLATEAU=0 \
MEASURE_SCRIPT=/bench/scripts/measure-accuracy-csv.sh \
CSV_DIR=/bench/accuracy/csv-1m-rate10k \
ACCURACY_FILE_OUTPUT=1 ACCURACY_FLUSH_EVERY=1024 \
./scripts/run-matrix.sh

{
  echo "== resource post =="
  date -Is
  free -g
  df -h . state tmp /home/users/lijunqing 2>/dev/null || true
  iostat -xz 1 5 2>/dev/null || true
} | tee "results/${LABEL}.resource-post.log"
```

PMC note: `MAXSEC=7200` is a starting point for long correctness evidence; QA may raise it if the run is clean, isolated, and still making progress.

### Impact on profiler and 100k downgrade

Profiler and 100k downgrade are now second-stage tools, not first response. Use them when:

- the long 1M run is clean but cannot finish or stops making progress,
- output mismatches require locating a correctness divergence,
- or QA needs a smaller reproduction after a clean long 1M failure.

## Boundary update: final 1M fixed-CSV accuracy gate before profiler/perf optimization

User boundary added on 2026-06-12:

QA is authorized to stop the old accuracy run and rerun the 1M fixed-CSV matrix with a long timeout. PMC's accuracy gate is now the final near-term gate: ensure 1M Nexmark fixed-CSV correctness is clean before profiler-driven performance optimization or RD tuning decisions.

### Final accuracy gate

PMC must wait for QA's complete 1M fixed-CSV matrix before treating Q4/Q7/Q9/Q11 or any other timeout-heavy query as a performance optimization target.

A query is allowed into profiler/performance triage only after one of these QA outcomes is available:

- `ACCURACY_PASS`: both target variants finish 1M fixed CSV and output row/hash comparison passes on the same input hash.
- `ACCURACY_MISMATCH`: 1M fixed CSV finishes but output differs; correctness work takes priority over performance.
- `ACCURACY_RESOURCE_BLOCKED`: result is tainted by resource pressure or competing jobs; rerun required before PMC conclusion.
- `ACCURACY_SCALE_BLOCKED`: long clean 1M fixed CSV cannot finish or stops making progress; PMC may then request profiler/100k reduction to isolate the scale cliff.

### PMC decision rule

- Do not enter profiler/performance optimization for a query while QA's long 1M fixed-CSV run is still pending and making progress.
- Do not use old timeout/incomplete runs as final evidence once QA has restarted the matrix under the new policy.
- If QA reports `ACCURACY_PASS`, then use performance/profiler to optimize throughput.
- If QA reports `ACCURACY_MISMATCH`, stop performance work for that query and route to correctness debugging.
- If QA reports `ACCURACY_RESOURCE_BLOCKED`, ask for a cleaner rerun, not RD changes.
- If QA reports `ACCURACY_SCALE_BLOCKED`, profiler and smaller reproductions become valid next steps.

### Matrix-level acceptance

The gate is matrix-level, not just Q4-local. PMC should wait for QA's full 1M fixed-CSV matrix summary, including:

- query, variant, mode, wall time, row count, output hash, input hash, resource snapshot reference;
- explicit list of `ACCURACY_PASS`, `ACCURACY_MISMATCH`, `ACCURACY_RESOURCE_BLOCKED`, and `ACCURACY_SCALE_BLOCKED` queries;
- skipped Q6 with known unsupported status, if still applicable.

Only the `ACCURACY_PASS` and `ACCURACY_SCALE_BLOCKED` buckets are eligible for performance/profiler planning. `ACCURACY_MISMATCH` goes to correctness. `ACCURACY_RESOURCE_BLOCKED` goes to rerun.

## Boundary update: 100K fixed-CSV correctness gate plus key-query config audit

User boundary added on 2026-06-12:

Accuracy data scale may be reduced. `100K fixed CSV` is now the better hard gate for correctness. `10K fixed CSV` is a smoke gate. `1M fixed CSV` is no longer the only hard gate; it becomes extended confidence validation after the 100K gate and configuration audit. PMC also needs to inspect why Q4 1M is so slow and whether the current run configuration is suboptimal.

### Revised current hard gate

Current hard gate before profiler/performance optimization:

1. `10K fixed CSV smoke`: fast sanity check for harness, input hashing, output materialization, and obvious correctness failures.
2. `100K fixed CSV correctness PASS`: required for key queries before performance work.
3. Key-query configuration audit: required for Q4/Q7/Q9/Q11 and any query that times out or shows anomalous throughput.

`1M fixed CSV` is now an extended confidence gate, not the only gate. It should be run after the 100K gate passes or when QA/PMC needs higher confidence for a query already understood at 100K.

### QA acceptance states

For each `(query, variant)` pair, QA should report one of:

- `SMOKE_PASS_10K`: 10K fixed CSV finishes and output hash is comparable.
- `SMOKE_FAIL_10K`: 10K fails, mismatches, or cannot materialize output; stop and debug correctness/harness.
- `CORRECTNESS_PASS_100K`: 100K fixed CSV finishes and output row/hash matches the baseline for the same input hash.
- `CORRECTNESS_MISMATCH_100K`: 100K finishes but output differs; correctness debugging before performance work.
- `CONFIG_BLOCKED`: query cannot be judged because runtime/config is suspect, incomplete, or inconsistent across variants.
- `RESOURCE_BLOCKED`: run is tainted by disk/memory/CPU pressure or competing jobs.
- `CONFIDENCE_PASS_1M`: optional 1M fixed CSV finishes and matches after the 100K gate.
- `SCALE_SLOW_1M`: 100K passes, but 1M is too slow; this is a scale/performance investigation, not a correctness blocker by itself.

### Key-query configuration audit

For Q4/Q7/Q9/Q11 and any timeout-heavy query, QA must attach a config audit before PMC accepts performance conclusions:

- Input and execution:
  - CSV path, input row count, input hash manifest.
  - query id, variant, image tag, Flink worktree commit, ForSt repo commit/lib sha.
  - `EVENTS_NUM`, `TPS`, `MAXSEC`, `POLL_SEC`, `DONE_ON_SRC`, `DONE_ON_PLATEAU`, `MEASURE_SCRIPT`, `CSV_DIR`.
- Resource shape:
  - `CONTAINER_CPUS`, `CONTAINER_MEMORY`, `PARALLELISM`, `TM_SLOTS`, TaskManager process memory, state cache limit.
  - resource snapshots before/after: memory, disk, iostat if available, active jobs/containers.
- State/checkpoint settings:
  - `CHECKPOINT_INTERVAL` including whether disabled/none.
  - async-state enabled, mini-batch enabled/latency/size.
  - ForSt tuning profile and write-heavy parameters if set.
- Output materialization:
  - accuracy file output enabled/disabled, flush frequency, output directory, sorted output row count/hash.
- Runtime behavior:
  - source progress trajectory, output progress trajectory, checkpoint durations, state dir size, loaded native lib.

### Q4 1M slowness audit

Q4 1M slowness should be treated as a configuration-plus-scale question until audited. Before claiming engine bottleneck, QA/PMC should check:

1. Whether Q4 is running with an unnecessarily low parallelism/slots or too small container CPU/memory for 1M.
2. Whether `DONE_ON_PLATEAU` prematurely classifies progress or hides true long-running completion.
3. Whether checkpoint interval, file output flush frequency, and accuracy output materialization dominate runtime.
4. Whether fixed CSV TPS/source pacing is set consistently across variants.
5. Whether Q4 has different behavior with checkpoint disabled after 100K correctness passes.
6. Whether disk pressure or competing jobs are present.

### Suggested QA sequence

Recommended order for Q4/Q7/Q9/Q11:

1. 10K smoke, serial single-query/variant.
2. 100K fixed CSV correctness, serial single-query/variant, with full config audit.
3. If 100K passes, run targeted 1M only as confidence/scale validation.
4. If 1M is slow but 100K is correct, classify as `SCALE_SLOW_1M` and inspect config before profiler.
5. Enter profiler only after `CORRECTNESS_PASS_100K` and config audit are available, or after `CORRECTNESS_MISMATCH_100K` needs correctness localization.

### QA command shape: 100K correctness gate

```bash
cd /home/users/lijunqing/code/stczwd/flink-nexmark-jdk17-100m-20260612/nexmark-100m-work
LABEL=qa-fixedcsv-100k-q4-$(date +%Y%m%d%H%M%S)
QUERY=q4
VARIANT=forst-local
{
  echo "== config/resource pre =="
  date -Is
  hostname
  git -C /home/users/lijunqing/code/stczwd/flink-nexmark-jdk17-100m-20260612/nexmark-100m-work rev-parse --short=12 HEAD || true
  git -C /home/users/lijunqing/code/stczwd/ForSt rev-parse --short=12 HEAD || true
  free -g
  df -h . state tmp /home/users/lijunqing 2>/dev/null || true
  iostat -xz 1 5 2>/dev/null || true
  /home/work/dockerd/bin/docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' | grep -Ei 'NAMES|nexmark|accuracy|forst|flink' || true
  find accuracy -maxdepth 3 -type f | sort | sed -n '1,120p'
} | tee "results/${LABEL}.config-pre.log"

RUN_LABEL="$LABEL" \
QUERIES="$QUERY" VARIANTS="$VARIANT" \
EVENTS_NUM=100000 TPS=10000 MAXSEC=1800 POLL_SEC=10 \
CONTAINER_MEMORY=16g CONTAINER_CPUS=4 PARALLELISM=2 TM_SLOTS=2 \
DONE_ON_SRC=1 DONE_ON_PLATEAU=0 \
MEASURE_SCRIPT=/bench/scripts/measure-accuracy-csv.sh \
CSV_DIR=/bench/accuracy/csv-100k-rate10k \
ACCURACY_FILE_OUTPUT=1 ACCURACY_FLUSH_EVERY=1024 \
./scripts/run-matrix.sh
```

If `csv-100k-rate10k` does not exist, QA may generate it or use the existing fixed CSV source, but must record the exact path and input hash.

## Status update: Q4 100K mismatch moves Q4 to correctness-first

User-reported boundary on 2026-06-12: Q4 has a 100K fixed-CSV mismatch.

PMC classification: `CORRECTNESS_MISMATCH_100K` for Q4 until QA provides corrected evidence.

Implications:

- Q4 must not enter profiler or performance optimization as a throughput problem yet.
- Q4 is now a correctness/debugging gate, not a performance gate.
- RD/QA next evidence should include exact input hash, output row count/hash for each variant, mismatch sample, sorting/normalization method, query config audit, and resource snapshot.
- Only after Q4 reaches `CORRECTNESS_PASS_100K` can PMC reopen Q4 performance profiling or 1M confidence/scale validation.

Suggested next action for QA/RD:

1. Confirm the mismatch is deterministic by rerunning Q4 100K on the same fixed CSV input and same output normalization.
2. Produce a minimal mismatch artifact: first differing rows after sort, row count delta, hash delta, and output directory paths.
3. Run Q4 10K smoke with the same compare path to check whether the mismatch reproduces at smaller scale.
4. If Q4 10K passes but 100K mismatches, classify as scale-triggered correctness bug and collect state/checkpoint/config details.
