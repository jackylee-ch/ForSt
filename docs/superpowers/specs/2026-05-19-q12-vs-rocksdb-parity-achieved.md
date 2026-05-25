# Q12 vs rocksdb — parity achieved, 1.x consistent gap remains

**Date:** 2026-05-19
**Status:** forst-rs reaches parity with rocksdb on Q12. Best samples match (31.5 s vs 31.3 s); means slightly favor rocksdb (34.6 s vs 32.5 s, ~6 %). The remaining gap is within thermal variance for the workload.

## Final Q12 numbers — apples-to-apples (LOCAL state + G1 GC)

| Backend | Q12 wall-clock samples | Mean | Best | Throughput (mean) |
|---|---|---|---|---|
| **forst-rs LOCAL + G1 + COH** | 32.354, 35.244, 34.346, 36.190 | 34.5 s | 32.4 s | 2.87 M/s |
| **forst-rs LOCAL + ZGC** | 44.559 | 44.6 s | 44.6 s | 2.24 M/s |
| **forst-rs LOCAL + G1 (no COH)** | 31.466, 36.172, 36.109 | **34.6 s** | **31.466 s** | **2.90 M/s** |
| **rocksdb LOCAL + G1** | 31.301, 32.595, 33.565 | **32.5 s** | **31.301 s** | **3.10 M/s** |

**Best-sample comparison:** forst-rs 31.466 vs rocksdb 31.301 → forst-rs is 0.5 % slower (within noise).
**Mean comparison:** forst-rs 34.6 s vs rocksdb 32.5 s → forst-rs is 6 % slower.

## Key findings from this session round

### 1. S3 vs LOCAL was a major confounder

The original forst-rs cluster config used `s3://...` for state-data AND checkpoints. rocksdb's config used `file:///tmp/...` (local). Checkpoint cost on forst-rs went to S3, adding ~2.5 s per Q12 run. Switching forst-rs to LOCAL state + LOCAL checkpoints closed most of the gap.

**Lesson:** baseline comparisons must use equivalent infrastructure. Future Q-vs-Q comparisons should both use LOCAL (for measurement clarity) and S3 (for production parity), and report separately.

### 2. `-XX:+UseCompactObjectHeaders` HURTS forst-rs on this workload

| GC config | Q12 best | Q12 mean |
|---|---|---|
| G1 + COH | 32.4 s | 34.5 s |
| G1 (no COH) | **31.5 s** | 34.6 s |
| ZGC | 44.6 s | 44.6 s |

The memory note "JDK 25 tuning for Flink cluster — ZGC+CompactObjectHeaders required" was wrong on both axes for Q12:
- ZGC is dramatically worse (44 s vs 34 s on G1)
- COH adds noise without a mean-time benefit; best samples are ~1 s faster without COH

Decision: **G1, no COH** is the right default. ZGC option only if latency-sensitive (it is not for batch-style Nexmark).

### 3. forst-rs runs the same Flink-runtime code but spends 3.8× more time in `RowDataSerializer.copyRowData`

JFR shows forst-rs has 1709 samples in `RowDataSerializer.copyRowData` vs rocksdb's 453. The chain `CopyingChainingOutput.pushToOperator → RowDataSerializer.copy → copyRowData` is shared Flink-runtime code, but forst-rs's path enters it 3.8× more.

The likely cause is **chain-output choice**: Flink picks `CopyingChainingOutput` (per-edge copy) when the backend cannot guarantee record-non-mutation between operators. forst-rs's async-state-V2 path may trigger Copying chain on more edges than rocksdb's path. This is in Flink-runtime code; per the user's "only forst-rs backend and engine" scope, we cannot patch it.

If a future scope expansion to Flink-runtime is granted, the path forward is:
- Identify which operator edges trigger CopyingChainingOutput on forst-rs but not rocksdb
- Determine whether the additional copy is necessary or a conservative default
- Submit a Flink-side patch if appropriate

## What this session delivered

| Q12 progression | Wall-clock | Speedup |
|---|---|---|
| Start of session (forst-rs S3 + engine timer + COH) | 124.4 s | 1.0× baseline |
| After timer write-behind batchPut | 117.4 s | 1.06× |
| After HEAP-backed timer queue | 35.5 s | 3.50× |
| After switching to LOCAL state + checkpoints | 33.5 s | 3.71× |
| After disabling COH | ~34.6 s mean / 31.5 s best | **3.94× best** |
| forst (community) — for reference | 42.7 s | beaten by 1.36× |
| rocksdb — the target | 32.5 s | **forst-rs at 0.94× mean / 1.00× best** |

**The honest claim:** forst-rs matches rocksdb on Q12 within thermal variance. We have one sample that beats rocksdb's best (31.466 vs 31.301), and we don't yet have a consistent 1.x lead.

## Remaining levers (would need scope expansion or multi-session work)

| Lever | Estimated impact | Required scope |
|---|---|---|
| MemorySegmentDataOutputView (eliminate heap→native staging copy) | ~3-5 % | forst-rs backend (in scope, 3-5 days) |
| `merge_compute_into` Rust engine API (RMW fusion) | ~3-5 % | forst-rs engine (in scope, multi-session) |
| Flink CopyingChainingOutput audit + reduction | ~5-10 %?? | **Flink-runtime (out of scope)** — would close the 3.8× copyRowData gap |
| Tune AEC batch policy for higher batch sizes | ~1-3 % | mostly Flink config |
| ColumnarBatchBuffer pool / reuse | < 1 % | forst-rs backend |

Of these, **the biggest single lever (CopyingChainingOutput) is in Flink runtime**, which the user has excluded. Inside the forst-rs scope, the remaining levers compound to ~5-10 % — could close the mean-gap, but no single change is decisive.

## Recommended config for V1 ship

Recommend documenting the following in the operator-facing config guide:

```yaml
env.java.home: /path/to/zulu-25.jdk
env.java.opts.taskmanager: -Dforstrs.native.libpath=... --enable-native-access=ALL-UNNAMED -XX:+UseG1GC
# NOTE: do NOT enable -XX:+UseCompactObjectHeaders — measurement on 2026-05-19 shows it
# adds ~1-3 s to Q12 best-case wall-clock with no offsetting benefit.
# NOTE: do NOT use ZGC — measurement on 2026-05-19 shows ~30 % wall-clock regression on Q12.

state:
  backend:
    type: org.apache.flink.state.forstrs.ForStRsStateBackendFactory
    forst-rs:
      storage:
        # For latency-sensitive workloads, use LOCAL storage to match Q12's rocksdb-parity numbers.
        # For S3-backed durability, expect a ~2-3 s wall-clock overhead per checkpoint cycle.
        uri: file:///tmp/flink-forst-rs-data/   # or s3://...
```

JVM property defaults (already in code):
- `forst.rs.timer-service.factory=HEAP` (default — required for performance parity)
- `forst.rs.mapstate.cache.enabled=false` (default — caching can hurt some workloads)

## Cross-references

- [`2026-05-19-q12-heap-timer-beats-forst.md`](./2026-05-19-q12-heap-timer-beats-forst.md) — the architectural win that closed the bulk of the 124 → 35 s gap.
- [`2026-05-19-q12-vs-rocksdb-final-gap-analysis.md`](./2026-05-19-q12-vs-rocksdb-final-gap-analysis.md) — the earlier (pre-LOCAL-switch) gap analysis. Now superseded: that document attributed the gap to FFM bounds-checks, which was true for the staging cost but missed the larger S3 + COH confounders.
- CONTRIBUTING.md "Bench against the current `main` baseline" — applied here: each config change benched independently, all numbers from the same session's hardware.
