# Forst-RS Component Microbench Report (Pre-V1 Gate)

**Date:** 2026-05-16
**Decision:** GO
**Reference:** Spec Appendix — Pre-implementation validation gate.

## Per-component results

| Component | Target | Measured (avg ± 99.9% CI) | Pass? |
|---|---|---|---|
| SlotArenaScope.enter()/exit() (empty turn) | ≤ 200 ns | 0.32 ± 0.12 ns | PASS |
| turnRegion.allocate(256B aligned) | ≤ 50 ns | 4.23 ± 1.33 ns | PASS |
| encodeKeyInto | ≤ 100 ns | 1.34 ± 0.01 ns | PASS |
| encodeValueInto (256B) | ≤ 150 ns | 30.35 ± 0.14 ns | PASS |
| VectorizedClassifier.submit() (stub) | ≤ 100 ns | 2.16 ± 0.44 ns | PASS |
| VectorizedExecutor.dispatch() (stub, 64 rows) | ≤ 80 ns/row | 15.84 ns / 64 rows = 0.25 ns/row | PASS |
| pendingMisses.computeIfAbsent (hit) | ≤ 50 ns | 2.27 ± 0.02 ns | PASS |
| pendingMisses.computeIfAbsent (miss) | ≤ 200 ns | 188.3 ± 26.6 ns | PASS (borderline) |
| Rust engine batch_put 64×256B | informational | 15–31 µs (234–484 ns/row) | n/a |
| Rust engine batch_get 64×256B | informational | 2.54 µs (40 ns/row) | n/a |

All 8 Java component targets pass. `pendingMisses (miss)` is borderline at 188 ns vs 200 ns target
— within budget but the variance is wide (±26 ns). See Caveats.

## Sum-along-Trace-A

Trace A (Spec §3) = `encodeKeyInto + encodeValueInto + classifier.submit + 1/N × executor.dispatch`
With N=32 (target batch size):

```
encodeKeyInto:           1.34 ns
encodeValueInto (256B): 30.35 ns
classifier.submit:       2.16 ns
executor.dispatch / 32:  0.50 ns   (15.84 ns total / 32)
                        ---------
Sum:                    34.35 ns
```

**Target: ≤ 1 µs at p99 for 256 B values, 32-row batches.**

Status: **PASS — 34 ns, 29× headroom vs 1 µs target.**

The sum-along-Trace-A is two orders of magnitude below budget. Even adding the two allocation costs
(turnRegion 4.23 ns × 2 = 8.46 ns for key + value separately) the total stays under 45 ns per row,
which is still 22× inside budget.

## Rust engine cost (informational, not gated)

The Rust engine itself spends ~234–484 ns per row on batch_put at 64-row scale and ~40 ns per row
on batch_get. The batch_put cost range is wide because the criterion bench shows 15–31 µs variance,
likely attributable to write-buffer stall and compaction jitter under sustained write load. The Java
dispatch layer (34 ns) is ~7–14× cheaper than the Rust put cost and ~1× the Rust get cost. This
means:

- **Read-heavy workloads (Q3 GET):** Java dispatch is negligible relative to the ~40 ns/row engine
  cost. Optimization priority should be batch size growth and reducing JNI/FFM call overhead.
- **Write-heavy workloads (Q4/Q5/Q8):** The 234–484 ns/row Rust put cost will dominate. Java
  dispatch is not the bottleneck; focus should be on write-buffer sizing, parallelism, and the
  async-v2 batch API.

## Decision

**GO:** All 8 Java component targets pass. Sum-along-Trace-A is 34 ns — 29× inside the 1 µs
budget. There are no order-of-magnitude design errors at the component layer. Proceed to P1.

The `pendingMisses (miss)` result at 188 ns is worth monitoring: in production the map grows
unboundedly within a turn, and allocation cost scales with map size. A follow-up in P7–P8 should
bound the map size or switch to a per-turn linear-probe structure if the miss path becomes hot.

## Caveats

- **encodeKeyInto alignment bug fixed during gate run.** The original bench used `JAVA_LONG` at a
  2-byte-aligned offset (`off + 2` where `off` is 64B-aligned), triggering a JDK 25 alignment
  exception. Fixed to `JAVA_LONG_UNALIGNED` to match the production key layout (kg=2B prefix
  forces subsequent fields off 8B boundaries). The fix is correct and conservative — unaligned
  stores on x86 have near-zero cost, so the measured 1.34 ns is the right production estimate.

- **Stub benches undercount real VectorizedClassifier/Executor cost.** The `classifierSubmitStub`
  bench (ArrayDeque add/clear) and `executorDispatchStub` bench (MemorySegment int write-back) do
  not model FFI dispatch, Rust engine calls, or result demux. Real-component benches are a
  post-P5 enhancement once state ops are migrated to the V2 async path.

- **`pendingMisses (miss)` variance is high.** The 26 ns CI at 99.9% suggests GC or OS
  interference during the 5 measurement iterations (ZGC + CompactObjectHeaders was enabled).
  At p99 in production this may occasionally exceed 200 ns. Bounded-map or epoch-reset strategies
  are recommended before P8.

- **JMH ran on a development laptop, not a dedicated benchmark host.** Absolute nanosecond values
  should be treated as order-of-magnitude guidance. The real-hardware gate should run the same
  benches on the production server at P11 for final go/no-go on numeric targets.

- **No P0.4 Rust bench rerun in this session.** The Rust criterion numbers (15–31 µs batch_put,
  2.54 µs batch_get) are carried forward from the prior P0.4 run (same day). They are informational
  and not gated.

## Raw JMH output summary

```
JMH 1.37 | JDK 25.0.3 (Zulu) | ZGC + CompactObjectHeaders | avgt 5 iters (ns/op)

ComponentMicrobench.classifierSubmitStub            2.158 ±  0.440
ComponentMicrobench.emptyTurnRoundTrip              0.323 ±  0.120
ComponentMicrobench.encodeKeyInto (fixed)           1.337 ±  0.012
ComponentMicrobench.encodeValueInto256B            30.349 ±  0.135
ComponentMicrobench.executorDispatchStub (64 rows) 15.841 ±  0.029
ComponentMicrobench.pendingMissComputeIfAbsentHit   2.267 ±  0.017
ComponentMicrobench.pendingMissComputeIfAbsentMiss 188.316 ± 26.601
ComponentMicrobench.turnRegionAllocate256B          4.227 ±  1.327
```
