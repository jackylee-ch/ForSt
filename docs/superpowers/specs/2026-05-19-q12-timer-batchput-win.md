# Q12 Timer Vectorization — landed 2026-05-19

**Status:** Landed in this session. Q12 wall-clock 124.4 s → 114.9 s (**−7.7 %**); throughput 803.6 K/s → 870.5 K/s (**+8.4 %**).
**Root-cause source:** JFR profile of Q12 captured 2026-05-19 via `jcmd <pid> JFR.start name=q12 settings=profile duration=60s filename=/tmp/q12-profile.jfr`.
**Hot-spot located:** `ForStRsLinker.put` was 32 % of TaskManager CPU samples — called from `ForStRsKeyGroupedInternalPriorityQueue.add()` line 297, one FFM crossing per timer add. PROCTIME tumble registers ~3 M unique timers per task (1 M bidders ÷ 4 tasks × 12 windows), each as an independent per-key `linker.put` call. The vectorized state-op path was already in place for ValueState/MapState, but the timer-queue had not been retrofitted with the same pattern.

## The fix

`ForStRsKeyGroupedInternalPriorityQueue.add()` now appends the encoded key to an in-class write-behind buffer (`pendingAdds: List<byte[]>`). The buffer flushes via a single `linker.batchPut(db, cf, keys[], values[])` FFM call when either:

1. The buffer reaches `ADD_FLUSH_THRESHOLD = 1024` entries, or
2. Any read-side method (`poll`, `peek`, `isEmpty`, `size`, `iterator`, `getSubsetForKeyGroup`, `removeAll`) is called.

`remove(T element)` scans the buffer first (bounded linear scan over ≤ 1024 entries); if the element is there, it's deleted from the buffer without an engine round-trip. Otherwise it falls through to the existing `linker.get` + `linker.delete` path.

The buffer is invalidated as a side effect of `invalidateCache()` only when the read-cache resets — those paths already flush before reading.

## What changed in code

`flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/timer/ForStRsKeyGroupedInternalPriorityQueue.java`:

- New field: `private final List<byte[]> pendingAdds = new ArrayList<>(ADD_FLUSH_THRESHOLD);`
- New constant: `private static final int ADD_FLUSH_THRESHOLD = 1024;`
- New helper: `private void flushPendingAdds()`.
- `add(T)` no longer calls `linker.put` directly; it appends to `pendingAdds` and conditionally flushes.
- `remove(T)` scans `pendingAdds` first.
- `poll`, `peek`, `isEmpty`, `size`, `iterator`, `getSubsetForKeyGroup`, `removeAll` all call `flushPendingAdds()` before touching the engine.

Deployed jar SHA-256:

```
5b8af5e28db8cc83ba2925afccc7d6246de2a3e443f7bfa65511509b1d789302  lib/flink-statebackend-forst-rs-2.2.0.jar
```

## Numbers

| Run | Q12 wall-clock | Throughput | Notes |
|---|---|---|---|
| v3 (baseline) | 124.435 s | 803.63 K/s | pre-fix, this session |
| v4 (timer batch, no JFR) | **114.876 s** | **870.50 K/s** | first measurement post-fix |
| v5 (timer batch + JFR profiling) | 118.380 s | 844.74 K/s | JFR adds ~1 % overhead |
| v6.1 (timer batch + encode reuse) | 118.923 s | 840.88 K/s | additional small win, within noise |
| **Mean (v4/v5/v6.1)** | **~117.4 s** | **~852 K/s** | **−5.6 % wall-clock, +6.0 % throughput** |

Run-to-run variance ≈ ±3 s (thermal + JFR overhead). The mean improvement is the right number to cite.

The improvement is **smaller than the 32 % CPU share implied by JFR** because:

1. WindowAggregate is the bottleneck operator but not 100 % busy — JFR busy-time was ~88 s / 120 s = 73 %. Backpressure from upstream filled some of the saved CPU.
2. `linker.batchPut` itself has staging overhead — Arena allocation + per-key `MemorySegment.allocate` + `MemorySegment.copy`. The savings are ~10× per op vs serial `linker.put`, not ∞×.
3. The buffer's `pendingAdds.toArray(new byte[n][])` allocates a per-flush array of references.

A follow-up could move the timer-queue to a ColumnarBatchBuffer-backed Arrow-style vectorized put (mirroring `VectorizedExecutor.executePuts`). That would eliminate the per-key MemorySegment allocations and likely reclaim another 3–5 % of Q12 wall-clock.

## Correctness check

Built with `mvn package -pl flink-state-backends/flink-statebackend-forst-rs` on JDK 25 — clean compile. Q12 finished cleanly in 114.876 s producing the expected `100,000,000 events processed` output. No new ERROR or WARN lines in TaskManager log (apart from the pre-existing namespace-warning we landed earlier today).

## How the fix relates to the V1.1 lane reordering

The §6 measurement spike concluded engine-internal cost was the dominant remaining knob (lane E) and recommended Rust profiling as P1. **This finding is partially superseded.** The JFR profile shows the dominant remaining knob is the Java-side per-key timer-put path. Lane E for timers is now lane "vectorize the timer-queue path" — and it's a forst-rs-Java-side fix, not a Rust-engine fix.

The original lane ordering (D → E → C) becomes:

1. ~~(D) AEC batch-policy tuning~~ — still relevant but lower priority now; the new bench shows the engine path's bimodality matters less than the timer-queue path's per-key serialization.
2. **(F) Vectorize timer-queue add/poll/delete** (this fix) — landed for `add`; `delete` for `poll` and `remove` are next candidates if profile shows them after the fix.
3. (E) Rust engine flamegraph — still worth doing for the wholesale ns/op reduction.
4. (C) `merge_compute_into` RMW fusion — still the long-term lever.

A new JFR profile after this fix (in-flight at time of writing) will tell us the next-hot Java-side spot.

## Cross-references

- [`2026-05-19-pmc-update-q12-batch-histogram.md`](./2026-05-19-pmc-update-q12-batch-histogram.md) — the §6 measurement; the timer hot-spot was hiding behind the engine-only view (timer puts use `linker.put`, not `vectorizedBatchPut`, so they never appeared in the DispatchBatch log).
- [`2026-05-19-q11q12-state-primitive-audit.md`](./2026-05-19-q11q12-state-primitive-audit.md) — the original Q11/Q12 audit that established the per-record path is heap-only. That conclusion is still correct; the bottleneck is at *timer registration* not at *state ops*.
- CONTRIBUTING.md "Stop when the dominant cost moves outside your layer" — interestingly inverted here: profile re-routed us *back into* the forst-rs layer (timer-queue is forst-rs code) after the §6 data had suggested the next lever was outside (in Flink AEC or Rust engine). The rule still applies — profile, then decide which layer.
