# Size-Aware ArrowBinaryBuffer Auto-Tune — Q5 wins without Q11/Q12 regression

**Author:** jackylee (PMC) + Claude
**Date:** 2026-05-21
**Branches:** `~/Code/stczwd/flink` `forst-rs-jdk25` (currently at `633af3d3be1`), `~/Code/stczwd/ForSt` `forst-rs`
**Related:** [Off-heap Arrow state design](2026-05-20-forst-rs-offheap-arrow-state-design.md); 1a/1b/1c.1 commits.

## Problem

Empirical evidence from this session's experiments:

| ValueState cap | Q5 (HOP, large working set) | Q11 (SESSION, small working set) | Q12 (V2 async) |
|---|---|---|---|
| 1 024 | ~586 s (too small, evictions) | **76 s (good)** | 32 s |
| 65 536 (current 1b) | 515 s | 117 s (regression) | 32 s |
| 262 144 (experiment) | **31 s** | 130 s (worse) | 32 s |

The auto-tuner from `1a.2 ArrowBinaryBufferAutoTuner` grows the buffer on hit-rate alone. Q11's session-window pattern has 100 % hit rate at any size because the same accumulator key is hit repeatedly — so the tuner grows the buffer all the way to the cap **even though the actual working set is ~1 K entries**. The buffer never fills past 1 K but the `resize(...)` events (Arena allocation + hash-index rebuild) at each doubling cost Q11 measurable time. Larger cap → more wasted resize events → worse Q11.

Q5's HOP shared-slice pattern has 50 K–250 K active pane keys. With cap 65 536 the buffer constantly evicts and `find` hits low hit rate at full load → high FFM crossing rate → 515 s. With cap 262 144 the buffer fits the working set, hit rate stays high, FFM crossings drop → 31 s.

V2-async queries (Q12 confirmed; Q4, Q7, Q15 likely) don't touch this buffer at all — they dispatch through `VectorizedExecutor` + `frsVecBatch*`. So they're invariant to the ValueState cap.

## Goal

Lift ValueState `MAX_CAPACITY` to **1 048 576** (1 M) so Q5 can grow into its actual working set, AND **make `ArrowBinaryBufferAutoTuner` size-aware** so workloads with small working sets (Q11, Q13) stay at `MIN_CAPACITY=1024` regardless of hit rate. Result: Q5 wins via the larger cap; Q11/Q12 don't regress because the tuner refuses to grow a buffer that isn't full enough to justify a resize.

## Non-Goals

- V2 async path (`ForStRsValueStateV2`, `MapStateV2`, etc.) — unchanged, no buffer.
- MapState — its cap is already 524 288 (per-class constant from 1c.1); only ValueState is changed here.
- Rust engine — no FFI changes.
- Flink runtime / planner — unchanged.

## Architecture

```
ArrowBinaryBufferAutoTuner observes BOTH:
  - hit_count, sample_count (existing) → hit_rate
  - last_observed_size, last_observed_capacity (NEW) → occupancy

shouldResizeTo() decision:
  GROW   if   hit_rate ≥ GROW_RATE (0.80)
          AND occupancy ≥ GROW_OCCUPANCY (0.70)
          AND currentCapacity < MAX_CAPACITY
  SHRINK if   hit_rate ≤ SHRINK_RATE (0.30)
          AND occupancy ≤ SHRINK_OCCUPANCY (0.20)
          AND currentCapacity > MIN_CAPACITY
  ELSE no change.

ValueState MAX_CAPACITY lifted: 65 536 → 1 048 576 (1 M).
MapState cap unchanged (524 288 — set per-instance in 1c.1).

Per-query behavior:
  Q5 (HOP, ~150 K working set):
    Buffer fills up at small caps → occupancy ≥ 0.70 → tuner grows
    repeatedly: 1024 → 2048 → ... → 131072 → 262144 → 524288 → 1048576
    At cap, occupancy stays around 70-95 %, FFM crossings drop, hit rate stays high.

  Q11 (SESSION, ~1 K working set):
    Buffer hit rate 100 % at any size, BUT occupancy at cap 1024 is ~1.0
    after first session window; insert returns hit. After a few sessions
    finish, accumulator keys are removed; occupancy stays low. Tuner sees
    occupancy < 0.70 → no grow. Cap stays at MIN_CAPACITY.

  Q12 (V2 async):
    Doesn't use this buffer at all. Invariant.

  Q9 / Q20 (iterator-heavy, V1 sync MapState):
    MapState cap is 524 288 per 1c.1 commit — unchanged here.
    The size-aware gate applies to MapState too (same AutoTuner class),
    further refining 1c.1's behavior for MapState patterns with small
    working sets.
```

## Components Touched

| Component | File | Change |
|---|---|---|
| `ArrowBinaryBufferAutoTuner` | `state/ArrowBinaryBufferAutoTuner.java` | Add `lastSize` / `lastCapacity` fields. Change `observeRead(boolean)` → `observeRead(boolean wasHit, int currentSize, int currentCapacity)`. Update `shouldResizeTo(int)` to gate on occupancy. |
| `ArrowBinaryBuffer.MAX_CAPACITY` | `state/ArrowBinaryBuffer.java` | Raise from 65 536 to 1 048 576. (MapState already uses its own per-instance cap of 524 288 — that constant `MAX_CAPACITY_MAP_STATE` stays as-is for explicitness.) |
| `ForStRsValueState.value()` off-heap path | `state/ForStRsValueState.java` | Update `tuner.observeRead(...)` call to pass `statebuf.size(), statebuf.capacity()`. |
| `ForStRsMapState.get()` off-heap path | `state/ForStRsMapState.java` | Same — pass size + capacity to the tuner. |
| `ArrowBinaryBufferAutoTunerTest` | `test/.../ArrowBinaryBufferAutoTunerTest.java` | Update existing tests to pass size + capacity; ADD tests for: (a) high hit-rate + low occupancy → no grow; (b) high hit-rate + high occupancy → grow; (c) low hit-rate + low occupancy → shrink; (d) low hit-rate + high occupancy → no change (workload churning; cap is correct). |

## Data Flow

Per read on V1-sync ValueState (off-heap mode):

```
state.value():
  scratch = scratchArenaSupplier.get()
  encoded = encodeForStateOffheap(kg, key, stateNameBytes, scratch, 0)
  keyOff = encoded >>> 32; keyLen = encoded & 0xFFFFFFFF

  row = statebuf.find(scratch, keyOff, keyLen)
  tuner.observeRead(row >= 0, statebuf.size(), statebuf.capacity())  // CHANGED

  if (row >= 0) ...
  else ... linker.getPinnedSegment(...) ...
```

Per ~1024 reads, AutoTuner's internal sample-window completes. `shouldResizeTo(currentCapacity)` evaluates the new dual gate. Caller (e.g., `statebuf.insert` after a flush, or an external resize trigger) uses the returned cap value.

**Current (pre-this-design) wiring** — `ArrowBinaryBuffer.insert` grows the buffer unconditionally when `size >= capacity` (resize 2× up to global MAX_CAPACITY). The tuner is constructed alongside the buffer but never consulted; `observeRead` increments counters and `shouldResizeTo` is dead code at the call sites. The buffer's natural growth is driven solely by full-buffer inserts.

**Q11 regression mechanism explained** — when MAX_CAPACITY = 65536, Q11's buffer grows through multiple full-buffer events as session-window accumulators accumulate distinct bidders. Each `resize(2× cap)` allocates a fresh `Arena.ofShared()`, copies the data, rebuilds the hashIndex (`insertHashIndex` for every existing row). For Q11's pattern of MANY distinct bidder keys over the run (each session creates a new key; old sessions retire but the buffer doesn't shrink between resizes), the buffer climbs 1024 → 2048 → ... → 65536 — paying the resize cost at each step. With MAX_CAPACITY=1024 (the original experiment), there's no headroom to grow, so the buffer flushes more often (cheap `batchPut`) but skips the resize churn entirely. The flush path is cheaper than the resize path.

**New wiring (this design)** — `ArrowBinaryBuffer` consults the tuner at TWO points:

1. **Full-buffer insert path** (`insert` when `size >= capacity`):
   - Call `int suggested = tuner.shouldResizeTo(currentCapacity)`.
   - If `suggested > currentCapacity` AND size-gate passed: `resize(suggested)`.
   - Else (gate refused): `flushTo(linker, db, cf)` + `clear()` + insert at current capacity. No resize, no Arena alloc, no hashIndex rebuild.

2. **Sample-window boundary** (every `SAMPLE_WINDOW = 1024` reads, inside `observeRead`):
   - At the moment the window completes, evaluate the gate using the buffer's CURRENT size/capacity.
   - If gate suggests shrink (low hit + low occupancy), the tuner returns a smaller cap on next `shouldResizeTo`; insert/flush callers don't react to shrink unless `size <= newCap`. For now: only act on grow suggestions at insert-time. Shrink decisions are advisory until a future `compact()` call (out of scope).

The dual-gate effect for the three target queries:

- **Q5 (HOP, large WS)**: full-buffer inserts fire repeatedly. Tuner sees `occupancy ≈ 1.0` (size==cap by definition) AND high hit rate → grow approved at each step. Buffer grows naturally to 1 048 576 (or to the actual WS size, whichever smaller).
- **Q11 (SESSION, small WS, mass-overwrite of accumulator keys)**: full-buffer insert fires only when buffer fills (depends on actual key count). If WS is small (say 2K), buffer fills at cap 2K → tuner says "grow to 4K, occupancy=1.0, OK" → grow. Q11 still pays a few resize events but stops MUCH earlier than the prior unbounded growth. With WS ~ 5K, buffer settles at 8K — far below the prior 65K landing. **This is the key insight: the gate caps growth to (next-power-of-2-above-WS), not to MAX_CAPACITY.**
- **Q12 (V2)**: no buffer, unaffected.

The size-gate's protective role: **for buffers that aren't actually full**, no growth happens. The prior dead-code tuner couldn't have triggered growth anyway (since `shouldResizeTo` was never called), but the new wiring ensures we don't ACCIDENTALLY trigger growth when adding tuner consultation at insert.

## Correctness Invariants

- **Tuner state transition is monotonic**: a single decision is made per `SAMPLE_WINDOW = 1024` observations. No race conditions.
- **Buffer state during resize is consistent**: `resize(newCap)` allocates a fresh Arena, re-inserts all rows, closes old Arena. Single-threaded per slot.
- **Occupancy is observed at the moment of `observeRead`** — slight lag (the tuner sees occupancy as of the last 1024 reads, not the current value). Acceptable for tuning purposes.
- **MAX_CAPACITY raise is safe** — Arena.ofShared can allocate large segments; failure mode is `OutOfMemoryError` at the allocate call, which propagates as a Flink task error (existing failure surface).
- **No breaking change to AutoTuner API** if the new method takes the same name but more args — existing callers (only `ForStRsValueState` and `ForStRsMapState`) are updated in lockstep.

## Error Handling

- Tuner is pure Java, no FFM. No new error modes.
- `MAX_CAPACITY = 1 048 576` at ~80 B average payload = ~80 MB key data + 80 MB value data + 16 MB hash index ≈ **180 MB per state instance per slot**. At 4 slots = 720 MB. With JVM at 5-6 GB heap, acceptable.
- If multiple ValueStates per operator grow to MAX, memory pressure could trigger GC. Mitigated by: (a) most ValueStates don't grow to MAX (the size-gate prevents Q11-style growth); (b) the JVM heap config in `config-forst-rs-local.yaml.tpl` is generous.

## Testing

### Updated existing tests

- `ArrowBinaryBufferAutoTunerTest` — six existing tests pass two extra args (size, cap). Update each test's observeRead loops accordingly. Existing semantics preserved: tests that simulate high hit rate now also simulate high occupancy (just pass `size=cap`).

### New tests

1. **growIsGatedByOccupancy** — 1024 reads all hit, size=100, cap=1024 → occupancy=0.10 → shouldResizeTo returns cap unchanged (no grow despite high hit).
2. **shrinkIsGatedByOccupancy** — 1024 reads with 10 % hit, size=900, cap=1024 → occupancy=0.88 → shouldResizeTo returns cap unchanged (workload is churning, but buffer is right-sized).
3. **growWhenBothGatesPass** — 1024 reads, 100 % hit, size=900, cap=1024 → shouldResizeTo returns 2048.
4. **shrinkWhenBothGatesPass** — 1024 reads, 10 % hit, size=100, cap=1024 → shouldResizeTo returns max(512, MIN_CAPACITY)=1024 (since MIN=1024, no shrink possible).
5. **respectsMaxCapacityAtNewLimit** — start at 524 288, 100 % hit + high occupancy → grows to 1 048 576 (the new ValueState MAX_CAPACITY), no further.

### Bench acceptance gates

| Query | Target |
|---|---|
| Q5  | ≤ 300 s (vs current 515 s) — real measurable improvement |
| Q11 | ≤ 80 s (vs current 77 s) — no regression |
| Q12 | ≤ 35 s (vs current 32 s) — no regression |
| Q9  | within noise of v3.3 |
| Q20 | within noise of v3.3 |
| Q13, Q17, Q19 (other V1 sync) | within noise OR improvement |

If Q5 ≤ 300 s AND no other query regresses > 10 %, commit. If Q5 hits the cap and stays slow (≥ 400 s), the gap is NOT in buffer cap and the structural-gap finding stands — open Approach-C (Rust engine work) as a follow-up spec.

## Implementation Order

One commit per logical unit. Bench at the end.

1. **AutoTuner refactor** — extend `observeRead` signature, add occupancy gate to `shouldResizeTo`, update unit tests, add new gating tests.
2. **ValueState + MapState wiring** — update `observeRead` call sites to pass size + capacity.
3. **MAX_CAPACITY lift** — change constant 65 536 → 1 048 576 in `ArrowBinaryBuffer.java`.
4. **Bench** Q5, Q11, Q12, Q9, Q13, Q19, Q20 with fresh-cluster strategy.
5. **Commit + v3 report v3.6 section** if gates pass.

## Out of Scope (Follow-on)

- **Q5 structural floor** — empirical evidence from this session's experiments suggests Q5 has a ~120-200 s floor even with infinite buffer (the dominant cost shifts to deserialization + the Flink-runtime sync window-agg processor itself). If this design lands Q5 between 150-300 s, that confirms; closing below 150 s requires Approach-C (Rust engine) or Flink-runtime planner change.
- **MapState Q19 known regression** — separate follow-on (opt-in or Rust FFI), tracked in 1c.1 commit message.
- **1c.2 batched MapState.clear()** — separate, not affected by this change.

## Open Questions

None at design time.
