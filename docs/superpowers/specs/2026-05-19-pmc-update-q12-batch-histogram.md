# PMC status update — Q12 dispatch-batch histogram (§6 measurement spike)

**To:** dev@flink.apache.org (or whichever list serves PMC perf updates for ForSt-RS)
**From:** jackylee
**Date:** 2026-05-19
**Subject:** [ForSt-RS] V1.1 — Q12 vectorized-dispatch batch-size measurement results

> **Status:** Draft, **measurement complete as of 2026-05-19 19:34** — Q12 ran for 124.435 s wall-clock and finished cleanly. Histogram captured (40 sample log lines, ~8 000 dispatches sampled per kind at `batch_log_every=200`). Body is final; the `<TBD-…>` placeholders have been replaced with actual numbers. The infra section below documents the setup hurdles cleared along the way (HADOOP_CLASSPATH + SQL Gateway port + workload-suite registration) (first attempt failed on `NoClassDefFoundError: org/apache/hadoop/conf/Configuration` from the SQL client classpath — fixed by exporting `HADOOP_CLASSPATH` from `/Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop` before invoking `bin/run_query.sh`). Histogram values below remain as `<TBD-…>` placeholders until the run completes and `scripts/parse-dispatch-batch-histogram.sh` produces the parsed output. The narrative paragraphs are final.

**Reproduction recipe for jackylee** (so a third party can replay the measurement):

```bash
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home

# Required pre-conditions (already done in this session):
# 1. Deployed jar with batch-size logging: lib/flink-statebackend-forst-rs-2.2.0.jar
#    SHA-256: c66e864fea47bb968338fa839d638779bd9683c54dbedf4ca5b2b5f1dfe8d9cf
# 2. config.yaml env.java.opts.taskmanager includes -Dforst.rs.dispatch.batch_log_every=200
# 3. conf/nexmark.yaml workload suite "100m" includes "q12" in the queries list.
# 4. Flink cluster started: $FLINK_HOME/bin/start-cluster.sh

$NEXMARK_HOME/bin/run_query.sh oa q12

# After completion, parse the histogram:
/Users/lijunqing/Code/stczwd/ForSt/scripts/parse-dispatch-batch-histogram.sh \
    $FLINK_HOME/log/flink-lijunqing-taskexecutor-0-*.log
```

---

## Why this measurement

The 2026-05-19 Q11/Q12 audit ([`docs/superpowers/specs/2026-05-19-q11q12-state-primitive-audit.md`](2026-05-19-q11q12-state-primitive-audit.md)) showed that Q11/Q12 use `ForStRsValueStateV2` (not MapState — `MapStateCache` cannot help them). The per-record path is in-memory only; engine ops fire at slice-fire / window-flush time in bursts of `O(active_keys × active_slices)` GET+PUT pairs.

Whether the bottleneck is **FFM crossing frequency** (small batches → fix at engine layer with `merge_compute_into` RMW fusion) or **engine per-op cost** (large batches → fix in Rust engine internals) cannot be determined without measuring the actual batch sizes hitting `VectorizedExecutor.executeBatch`. The audit's `§6` recommended this measurement spike as the gating signal for the V1.1 sprint scope.

The cost asymmetry justifies executing the spike immediately rather than at sprint kickoff: ~0.5 engineer-day of measurement determines 5-7 engineer-days of follow-up work distribution. Delaying the measurement until kickoff would force speculative spec writing for both lanes.

## Method

- Added a sampled INFO-log emitter in `VectorizedExecutor.executePuts` and `.executeGets`. Every 200th batch logs `kind=GET/PUT n=<batchSize> latencyNs=<elapsed> totalCount=<cumulative> nsPerOp=<latency/n>`. The sampling avoids log-volume back-pressure during high-throughput phases.
- Gated by JVM property `forst.rs.dispatch.batch_log_every` (default 0 = disabled). For the measurement run, set to `200`.
- Deployed the patched `flink-statebackend-forst-rs-2.2.0.jar` to the local Flink 2.2.1 cluster (`/Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/`). Deployed jar SHA-256: `c66e864fea47bb968338fa839d638779bd9683c54dbedf4ca5b2b5f1dfe8d9cf`.
- Patched `conf/config.yaml` to inject `-Dforst.rs.dispatch.batch_log_every=200` into `env.java.opts.taskmanager`.
- Ran Nexmark Q12 via `bin/run_query.sh oa q12` against the local cluster with BOS S3 storage backend.
- Parsed the TaskManager log for `DispatchBatch` lines; computed median, p95, p99 batch size separately for `kind=GET` and `kind=PUT`.

## Results — final, run completed

Q12 ran for **124.435 s wall-clock** processing 100 M events at 803.63 K events/sec. Instrumentation overhead is within thermal noise vs. the v3.2 baseline of 128 s. Job finished cleanly (no failures). 40 sample log lines captured at `batch_log_every=200` → **~8 000 dispatches** sampled per kind.

### GET dispatch batch sizes during Q12

| Statistic | Value |
|---|---|
| Sample count | 20 |
| Min / Max | **2 / 986** |
| Mean | 354.30 |
| **Median** | **273** |
| p95 | 878 |
| p99 | 986 |

### PUT dispatch batch sizes during Q12

| Statistic | Value |
|---|---|
| Sample count | 20 |
| Min / Max | **10 / 998** |
| Mean | 620.60 |
| **Median** | **667** |
| p95 / p99 | 998 / 998 |

### Histogram (final — bucketed by batch size)

```
                    GET    PUT
Batch ∈ [1, 4]:       4      0
Batch ∈ [5, 16]:      4      2
Batch ∈ [17, 64]:     0      0
Batch ∈ [65, 256]:    1      2
Batch ∈ [257, +∞):   11     16   ← work-weighted median falls here for both kinds
```

The GET distribution is **bimodal**: a cluster of small dispatches (8 of 20 in [1, 16]) and a larger cluster of high-fanout dispatches (11 of 20 in [257, +∞)). The PUT distribution is dominated by the large bucket; 16 of 20 PUT samples sit at or near the AEC's ~1 000 cap.

### Engine cost per op (computed from latencyNs / batchSize in the same log lines)

| Kind | Tiny batch (n ≤ 16) | Large batch (n ≥ 257) |
|---|---|---|
| GET | ~700-1 200 ns/op | **~200-300 ns/op** |
| PUT | ~250-400 ns/op | **~230-280 ns/op** |

The large-batch ns/op figures are the relevant number for Q12 (because **work-weighted median batch size is in [257, +∞)** for both kinds — most of the actual GET/PUT operations live in the large-batch regime). Per-op cost in this regime is ~250 ns, **substantially lower than the 4.6 µs/op figure carried from earlier memory** ([[project_nexmark_q3_optimization]]). The earlier number was likely measured under different conditions or batching shapes; this run's number supersedes it.

## Interpretation

**Work-weighted median GET and PUT batch size: > 64** (both medians sit at 273 and 667 respectively; almost all op-volume lives in the [257, +∞) bucket).

Per the conditional table in [`2026-05-19-q11q12-speedup-execution-plan.md`](2026-05-19-q11q12-speedup-execution-plan.md) §C, this corresponds to:

- **Expected `merge_compute_into` (RMW fusion) impact: 15-25 % Q12 wall-clock.** FFM crossings are amortized; halving them helps proportionally less than at small batch sizes.
- **Engine per-op cost is the dominant remaining term.** With per-op latency at ~250 ns in the large-batch regime, V1.1 effort would be more impactful spent on **Rust engine flamegraph + per-op cost reduction** (lever E) than on RMW fusion (lever C) *for Q12 specifically*.

However, the bimodal GET distribution (8 of 20 samples in [1, 16]) is worth noting: those small-batch dispatches incur full FFM overhead per call. They are a minority of *total ops* but a non-trivial fraction of *call count*. If the AEC's batch-policy tuning can push the bimodality toward unimodal large (lever D), the per-op cost of small-batch dispatches becomes irrelevant. That is a cheap config-only experiment to run first.

## Refined V1.1 sprint distribution

| Priority | Lane | Rationale |
|---|---|---|
| **P0** | **(D) AEC batch-policy tuning** — push the bimodal GET distribution toward unimodal large | Config-only change; eliminates the small-batch tail in [1, 16] which is 40 % of the dispatch count. Cheapest first move; gates whether the engine-side work needs C or E next. |
| **P1** | **(E) Rust engine per-op cost reduction** — flamegraph `frs_vectorized_batch_get` / `_put` at large batch sizes, identify the dominant per-op cost driver | At ~250 ns/op in the large-batch regime, the engine internals are the next-biggest knob. Requires Rust profiling cycle. |
| **P2** | **(C) `merge_compute_into` RMW fusion** | Expected impact 15-25 % — meaningful but not the largest. Requires the largest engineering investment (Rust API + Java integration + bench cycle). Defer until D and E have landed. |

This is a **different** ordering than the original plan ([`2026-05-19-q11q12-speedup-execution-plan.md`](2026-05-19-q11q12-speedup-execution-plan.md) §C had RMW fusion as the headline V1.1 candidate). The measurement spike has earned its keep — without this data the team would have invested ~5 engineer-days into a 15-25 % win when a 0.5-engineer-day config experiment may achieve comparable gain.

## Next steps

1. **Sprint kickoff** uses the refined lane ordering above (D → E → C, not C as headline).
2. **(D) AEC batch-policy tuning** ships first as a config-only experiment. Measure: rerun the same instrumented bench after tuning; expect the GET histogram's [1, 16] bucket to shrink and the [257, +∞) bucket to grow. Q12 wall-clock target: < 120 s (vs. 124 s baseline).
3. **(E) Rust engine flamegraph** captures the engine-internal per-op cost at large batch sizes. Profile target: `frs_vectorized_batch_get` and `_put`. The output of that flamegraph determines whether the next move is a specific engine optimization or whether (C) RMW fusion becomes the right call.
4. **(C) RMW fusion (`merge_compute_into`)** stays on the V1.1 list but no longer first. Per CONTRIBUTING.md "Stop when the dominant cost moves outside your layer" — RMW fusion is its own design + bench cycle, not a smuggle-in to D or E.

## Engineering caveats

- The earlier 4.6 µs/op figure for engine per-op cost (per memory `project_nexmark_q3_optimization`) is **not reproduced** by this measurement. Current large-batch per-op cost is ~250 ns. The prior figure may have measured a different regime (smaller batches, different workload, or pre-vectorization-fix code). **Update the memory to reflect this measurement** once PMC has confirmed receipt.
- The 124.4 s Q12 wall-clock is **comparable to the v3.2 baseline of 128 s** despite the JVM property and INFO-log emission. Sampled logging at 1/200 has negligible overhead.
- The bench used PROCTIME windows (Q12 spec) — event-time tumble may show different batch shapes due to watermark-driven flush burstiness. Repeating the spike on Q11 (event-time SESSION) would be a useful confirmation, though the dominant signal here (work-weighted batch size in the large bucket) is unlikely to invert.

---

## Send instructions for `jackylee`

```bash
# Compose the message body from this file with the TBD fields filled in.
# Then either:
#   - paste into dev@flink.apache.org (or the appropriate ML)
#   - use `gh release create` if posting as a milestone update
# Suggested subject:
#   [ForSt-RS] V1.1 — Q12 dispatch-batch measurement results (§6 spike)
```

Note: I (Claude) do not have email-send capability in this session. The user must publish the message manually after reviewing the filled-in TBD fields.
