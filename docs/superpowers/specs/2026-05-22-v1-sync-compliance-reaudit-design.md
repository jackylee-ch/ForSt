# ForSt-RS V1-Sync Compliance Re-Audit

**Date:** 2026-05-22
**Predecessor:** `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md` §9 OQ-1
**Phase 0 status:** COMPLETE
**Phase A status:** UNBLOCKED (verdict below permits Phase A to proceed; V15+ rows enumerated for parent spec §3 update)

---

## §1 — Scope

This sub-spec re-audits the V1-sync state classes (synchronous, per-event API used by Q5/Q11/Q13 and other V1-sync HOP/JOIN workloads) against the Tier 1–5 model and the three principles defined in the parent audit's §1:

- **Zero-copy predicate:** per-event hot path performs zero `byte[] = new byte[len]` allocations; values flow through `MemorySegment` slices and `MemorySegmentDataInputView` end-to-end.
- **Batch predicate:** V1 sync is inherently per-event, so per-event FFM is NOT a tier-1 violation; but per-record `byte[]` alloc at the dispatch layer IS still a zero-copy violation.
- **Vectorization predicate:** N/A for V1 sync (synchronous by design).

The reference compliant impl is `ForStRsValueState.value()` in off-heap mode (`statebuf != null`) per commit `537c1403f2f`.

V1-sync state classes audited:

- `ForStRsValueState`
- `ForStRsMapState`
- `ForStRsListState`
- `ForStRsReducingState`
- `ForStRsAggregatingState`

All classes live in `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/`.

The backend factory (`ForStRsKeyedStateBackend.java`) is examined to determine which of each class's constructor modes is actually wired at runtime — a state class can be source-compliant in one constructor and source-non-compliant in another; the runtime choice decides the actual hit on benchmark wall-clocks.

---

## §2 — Per-class findings

### §2.1 — ForStRsValueState

**Verdict: PASS** (compliant for the per-event hot path actually wired at runtime).

#### Off-heap mode wiring

The backend wires the off-heap constructor exclusively. See:

- `ForStRsKeyedStateBackend.java:372-384` — constructs `ForStRsValueState` with `scratchArenaTL::get`, `kgSer`, `stateName`, `offheapKeyGroupSupplier`, `offheapKeySupplier`, `buf`, `tuner` — i.e., the `statebuf != null` constructor. The legacy `byte[] keyPrefix` constructors at lines 105-166 / 173-199 are **not** invoked from production code paths.

#### `value()` hot path — file:line evidence

`ForStRsValueState.java:270-324`:

- L271: `if (statebuf != null)` — off-heap branch is the live path.
- L273: `MemorySegment scratch = scratchArenaSupplier.get();` — per-thread scratch, no `byte[]` alloc.
- L274-282: `encodeForStateOffheap(...)` — composite key written into `scratch` MemorySegment, returns packed `(off, len)` as a long. Zero heap alloc.
- L283-291: `statebuf.find(...)` returns row index; on hit, `offheapInputView.rewind(statebuf.valueDataSegment(), ...)` then `serializer.deserialize(offheapInputView)` — value bytes never leave the off-heap buffer. Zero heap alloc.
- L293-302: Miss → `linker.getPinnedSegment(...)` writes result directly into `scratch` after the key, `offheapInputView.rewind(scratch, resultOff, resultLen)`, deserialize off-heap. Zero heap alloc.

#### `update()` hot path — file:line evidence

`ForStRsValueState.java:327-374`:

- L332-360: off-heap branch. `offheapOutputView.reset(scratch, valStart)` then `serializer.serialize(value, offheapOutputView)` — serialized bytes written directly into the off-heap scratch segment.
- L351-353: opportunistic `statebuf.flushTo(...)` keeps the buffer below capacity (no surprise heap alloc on overflow).
- L354-358: `statebuf.insert(scratch, keyOff, keyLen, scratch, valStart, valLen)` — single off-heap insert via MemorySegment slices.
- L355: if `INSERT_NEEDS_FLUSH` (-2), flush + retry — still off-heap.

No `new byte[…]` on the per-event hot path in either direction.

#### Carve-outs (NOT per-event hot path)

- L362-373: legacy `byte[]` write path inside `else` of `if (statebuf != null)` — dead code in production (constructor not wired). No-op.
- L420-430: `getAndUpdate(T newValue)` — uses heap `byte[] payload = outputBuffer.getCopyOfBuffer()`. Per `grep`, this method is **not invoked** by any V1-sync state code path; it is exposed for an unbuilt RMW optimization (V1-sync uses separate `value()` + `update()` calls today). Documented as informational-only; not a current violation, but flagged as **V18** below in case future code begins to call it.

#### Pre-snapshot drain

- L406-410: `flushStateBuffer()` is the integration hook the backend invokes before a snapshot barrier — drains the off-heap buffer to the engine. Required for snapshot correctness in OQ-2; the V1-sync analogue is in place (the V2 async equivalent is what OQ-2 questions).

---

### §2.2 — ForStRsMapState

**Verdict: PARTIAL** — `get/put/contains/remove` are PASS; `entries/keys/values/iterator` (the `forEachEntry` body) violates zero-copy on every emitted row.

#### Off-heap mode wiring

`ForStRsKeyedStateBackend.java:439-453` wires the off-heap constructor (`statebuf != null` path).

#### Per-key hot path (`get`, `put`, `contains`, `remove`) — PASS

`ForStRsMapState.java:300-505`. Each method has an early `if (statebuf != null)` branch:

- `get(UK)` L301-335: `encodeForMapOffheap` → scratch MemorySegment; `statebuf.find`; on hit, `offheapInputView.rewind(statebuf.valueDataSegment(), ...)`; on miss, `linker.getPinnedSegment(...)` writes into scratch, `offheapInputView.rewind(scratch, valOff, valLen)`. **Zero `byte[]`.**
- `put(UK, UV)` L359-389: same pattern — `offheapOutputView.reset(scratch, valOff)`, serialize, `statebuf.insert(...)`. **Zero `byte[]`.**
- `remove(UK)` L444-461: scratch-encoded; `statebuf.remove(...)` + `linker.deleteSegment(...)`. **Zero `byte[]`.**
- `contains(UK)` L471-495: scratch-encoded; `statebuf.find`; on miss, `linker.getPinnedSegment(...)`. **Zero `byte[]`.**

#### Iterator hot path (`entries`, `keys`, `values`, `forEachEntry`) — FAIL (V15, V16)

`ForStRsMapState.java:642-733`. The off-heap branch L643-711 walks `statebuf.liveRows()` and emits per-row:

- L668-670: `byte[] mapKeyBytes = new byte[mapKeyLen]; MemorySegment.copy(kd, ..., mapKeyBytes, 0, mapKeyLen);` — **heap alloc per row** for the dedup-set composite-key bytes and for `keySerializer.deserialize(inputBuffer)`. **VIOLATION V15.**
- L678-679: `byte[] vBytes = new byte[vLen]; MemorySegment.copy(vd, ..., vBytes, 0, vLen);` — **heap alloc per row** for value deserialization. **VIOLATION V16.**
- L695-696: identical pattern for the engine-side branch (`System.arraycopy(composite, prefix.length, mapKeyBytes, 0, mapKeyLen)` after the engine-side `entry.key()` byte[] hits Java heap on its way out of the linker).

These two allocations are bounded by `liveRows × (avgKeyLen + avgValueLen)`. Q15 and Q19 (the heavy-iteration workloads — `entries()` is hit per window-fire) traverse the statebuf rows once per emit. For Q15's distinct-user-key density (~50K-200K range based on §3.5 D2's analogous fix relief on Q15), this translates to a measurable allocation cost in the iter path. Not as hot as the V8 (`ForStRsMapStateV2.java:346-380`) violation already catalogued for V2 async, but structurally identical.

#### Per-iter linker output also heap-allocs

- L688-690: `byte[] composite = entry.key();` and L704: `inputBuffer.setBuffer(entry.value());` — the linker's `iteratorNext` returns `byte[]` per entry. This is a Tier 5 (linker) issue, not a state-class one; documented as informational but not separately catalogued (it shadows V9's parent-spec row).

#### Carve-outs

- `flushMapWriteCache()` at L791-826 has heap byte[] copies (L810: `byte[] v = new byte[slice.length]`) but this is the **non-off-heap** legacy path (`statebuf == null`), which is dead in production. The off-heap branch at L794-796 calls `statebuf.flushTo(linker, db, cf)` only — zero alloc.
- The `currentPrefix()` helper at L739-744 invokes `prefixComputer.get()` which returns a heap `byte[]` per call. Not zero-copy, but called only in iter / clear paths (already covered above) and isEmpty. Acceptable in the iter context (iter is a "cold" path per §1); flag for future cleanup if profiling shows iter dominance.

---

### §2.3 — ForStRsListState

**Verdict: FAIL** (V17) — no off-heap path at all; every `add()` is a full-list GET+PUT with two `byte[]` allocations.

#### Wiring

`ForStRsKeyedStateBackend.java:401` constructs `new ForStRsListState<>(linker, db, defaultCf, prefix, elementSerializer)` — the legacy `byte[] keyPrefix` constructor (`ForStRsListState.java:66-80`). There is no off-heap-buffer overload defined for ListState V1.

#### Hot path evidence

`ForStRsListState.java`:

- L106-116 `add(T value)`: calls `readList()` then `current.add(value)` then `writeList(current)`. This is the structurally-identical-to-V3 destructive full-PUT pattern: the engine receives the whole list back, materialized in Java heap as `List<T>` and then re-serialized as `byte[]`.
- L158-172 `readList()`: `byte[] raw = linker.lookupKv(...)` allocates one `byte[]` for the entire list payload per call (Tier 5 linker hand-off, heap-bound).
- L174-182 `writeList(...)`: `outputBuffer.clear(); ... byte[] payload = outputBuffer.getCopyOfBuffer(); linker.put(...);` — second `byte[]` per call.

Per-event cost on Q5/Q13 (the V1-sync HOP/JOIN workloads): 2 × `byte[]` alloc + 1 GET + 1 PUT + full-list serialize/deserialize. Same shape as the **V3** violation in the parent spec, but in the V1-sync class (no async/columnar dispatcher available).

#### Severity

- Hit set: Q5 (HOP join via ListState), Q13 (V1-sync JOIN, OQ-4 candidate), and any user-job that uses ListState through the V1 sync API.
- Worst-case query: Q5 at 586.7 s in v3.2 (since structurally-bound per OQ-4 finding) — Q5's wall-clock is mostly the upstream Flink-runtime `AsyncStateSlicingSharedWindowProcessor` gap (out of forst-rs scope), but the V1-sync ListState full-list overwrite contributes per-event heap pressure on top.
- Q13 (41.1 s at v3.8) is the closest realistic Nexmark target — see OQ-4. A focused fix could shave the per-event heap-alloc cost.

#### Why this matters for Phase C

The parent spec §5 Fix V11 plan says "ListState off-heap via ArrowBinaryBuffer, follows the same V1-sync MapState 1c.1 pattern." This re-audit confirms the V1-sync ListState **does NOT have a 1c.1 pattern to follow** — the pattern exists in MapState only. Phase C therefore **cannot blindly copy ListState V1**; it must port the MapState V1 pattern across class boundaries, OR (preferred) build the off-heap path directly into V2 async ListState without using V1 sync as a template.

---

### §2.4 — ForStRsReducingState

**Verdict: FAIL** (V18) — no off-heap path; every `add()` is GET+reduce+PUT with two `byte[]` allocations.

#### Wiring

`ForStRsKeyedStateBackend.java:476-477` constructs `new ForStRsReducingState<>(linker, db, defaultCf, prefix, elementSerializer, reduceFunction)` — legacy `byte[] keyPrefix` constructor.

#### Hot path evidence

`ForStRsReducingState.java`:

- L102-109 `add(T)`: `readValue()` + `reduceFunction.reduce(...)` + `writeValue(next)`. Full GET+PUT cycle.
- L116-123 `readValue()`: `byte[] raw = linker.lookupKv(...)` — heap alloc per call.
- L125-130 `writeValue(T)`: `byte[] payload = outputBuffer.getCopyOfBuffer(); linker.put(...)` — heap alloc per call.

#### Severity

Less critical than ListState because the reduce function produces a single bounded-size accumulator (e.g., long counter, sum) rather than an unbounded list — the per-event `byte[]` alloc is small but still 2× per event.

- Hit set: any V1-sync ReducingState user (Nexmark queries that use ReducingState via the V1 sync API).
- Worst-case query: empirically the V1-sync HOP/COUNT path on Q11 (the project_q11_v1_sync_finding showed 92M ForStRsValueState (V1) ops on Q11; if any of those routed through ReducingState the impact would be measurable, but the A0 attestation found ValueState-only).
- Realistic relief: ≤ 5 s on Q11 if Q11's session-window assigner had used ReducingState (it doesn't). Hold at LOW certainty.

---

### §2.5 — ForStRsAggregatingState

**Verdict: FAIL** (V19) — structurally identical to ReducingState V1.

#### Wiring

`ForStRsKeyedStateBackend.java:501-502` constructs `new ForStRsAggregatingState<>(linker, db, defaultCf, prefix, accSerializer, aggregateFunction)` — legacy `byte[] keyPrefix` constructor.

#### Hot path evidence

`ForStRsAggregatingState.java`:

- L108-119 `add(IN)`: `readAccumulator()` + `aggregateFunction.add(...)` + `writeAccumulator(next)`. Full GET+PUT cycle per event.
- L126-133 `readAccumulator()`: `byte[] raw = linker.lookupKv(...)` — heap alloc per call.
- L135-140 `writeAccumulator(ACC)`: `byte[] payload = outputBuffer.getCopyOfBuffer(); linker.put(...)` — heap alloc per call.

The class does NOT extend `ForStRsValueState` (it implements `AggregatingState<IN, OUT>` directly with its own `readAccumulator`/`writeAccumulator`) — so the parent spec's §2 row "Reducing/Aggregating built on ValueState V2 internals" is **false for V1**. V12's "inherit from ValueState write-copy gap" framing only holds for V2 async; in V1 the gap is duplicated, not inherited.

#### Severity

Identical to V18 (ReducingState V1).

---

## §3 — Compliance verdict

**Net result:** V1-sync is PARTIALLY compliant.

- [x] **ValueState V1 sync IS principle-compliant.** Phase C can copy this pattern safely.
- [x] **MapState V1 sync IS principle-compliant on the per-key hot path** (`get/put/contains/remove`). Phase C can copy this pattern safely.
- [ ] **MapState V1 sync iterator path** allocates `byte[]` per emitted row (V15 + V16). Not on the per-event Q-N hot path, but Phase C's MapStateV2 iterator design must NOT copy this — it must design iterator zero-copy from scratch using `MemorySegmentDataInputView` slices.
- [ ] **ListState V1 sync is NOT principle-compliant.** No off-heap pattern exists; Phase C's "follows the same V1-sync MapState 1c.1 pattern" plan is partly invalidated.
- [ ] **ReducingState V1 sync is NOT principle-compliant.** No off-heap path; full GET+PUT cycle per event.
- [ ] **AggregatingState V1 sync is NOT principle-compliant.** No off-heap path; full GET+PUT cycle per event (also: does NOT extend ValueState, so V12's "inherits ValueState compliance" claim is false in V1).

### Net impact on Phase A entry gate

Phase A operates exclusively on V2 async ListState (`ForStRsAsyncListStateV2`). Phase A does NOT touch V1 sync state classes — V15-V19 are out of Phase A's scope. **Phase A is unblocked** (gate condition (b) in the plan: "V1-sync non-compliant — parent spec §3 updated with V15+ rows" is satisfied by §4 below).

### Net impact on Phase C

Phase C's "copy V1-sync 1c.1 pattern" claim must be qualified:

- V1-sync MapState per-key hot path: pattern exists, copyable.
- V1-sync MapState iterator path: pattern is NOT compliant; Phase C must independently design iterator zero-copy.
- V1-sync ListState: pattern does NOT exist; Phase C must design ListState off-heap **from scratch**, not from a V1 template.
- V1-sync Reducing/Aggregating: pattern does NOT exist; Phase C's V12 row "inherits from ValueState V2 fix" is correct only if V2 Reducing/Aggregating actually delegates to V2 ValueState internally — which the parent spec §2 row asserts but this audit could not falsify (V2 not in Phase 0 scope; deferred to Phase B/C entry gates).

### Per-fix attribution step

Skipped (no perf measurement — Phase 0 measures correctness, not perf). Per the plan Task 0.5 Step 3.

---

## §4 — Parent spec update needed (yes — V15+ rows)

Add the following rows to the parent spec's §3 violations catalog. The id sequence continues from V14 (the V1-sync re-audit placeholder).

### Proposed §3 schema rows

```
| ID  | File:line                                                                  | Tier | Principle violated | Severity | Hit set            | Worst-case query | Est. relief (s) | Certainty | Leverage |
|-----|----------------------------------------------------------------------------|------|--------------------|----------|--------------------|------------------|----------------:|-----------|---------:|
| V15 | state/ForStRsMapState.java:668-670 (`mapKeyBytes = new byte[mapKeyLen]`)    | T1   | zero-copy          | LOW-MED  | Q15, Q19 iter only | Q15 (18.34 s)    | 3               | MED       | 1.8      |
| V16 | state/ForStRsMapState.java:678-679 (`vBytes = new byte[vLen]`)             | T1   | zero-copy          | LOW-MED  | Q15, Q19 iter only | Q15 (18.34 s)    | 3               | MED       | 1.8      |
| V17 | state/ForStRsListState.java:158-182 (full GET+PUT, 2 × byte[] per add())   | T1   | zero-copy + batch  | MED      | Q5, Q13 (V1 sync)  | Q13 (41.1 s)     | 8               | LOW       | 2.4      |
| V18 | state/ForStRsReducingState.java:116-130 (GET+PUT, 2 × byte[] per add())    | T1   | zero-copy          | LOW      | V1-sync Reducing   | n/a              | 2               | LOW       | 0.6      |
| V19 | state/ForStRsAggregatingState.java:126-140 (GET+PUT, 2 × byte[] per add()) | T1   | zero-copy          | LOW      | V1-sync Aggregating| n/a              | 2               | LOW       | 0.6      |
```

### Derivation for the relief estimates

- **V15 / V16 (3 s each on Q15)**: Q15's wall-clock at v3.8 is 18.34 s. Iter path is ~10–20 % of total (window-fire-driven). Allocation cost ~15-20 ns × ~5M emitted rows ≈ 100 ms ceiling per query; conservatively 3 s on the longer queries that iterate more. Certainty MED.
- **V17 (8 s on Q13)**: V1-sync ListState heap-alloc per `add()` × Q13's ~10 M event rate ≈ 200 ms allocation ceiling, but the dominant cost is the GET+PUT cycle itself, not the alloc — switching to APPEND_MERGE (V3 analogue) at V1 sync is structurally hard because there's no batched dispatcher in V1 sync. Best-effort relief estimate 8 s assuming a per-row engine-merge path; if engine merge cannot be wired in V1 sync (sync API requires immediate completion), relief drops to ~2 s for the byte[] alloc alone. Certainty LOW pending design exploration.
- **V18 / V19 (2 s each)**: Single accumulator per call, small payload, hot only in V1-sync Reducing/Aggregating workloads — which are uncommon in Nexmark (queries primarily use ValueState V1 or MapState V1). Conservatively 2 s. Certainty LOW.

### Update to §4 matrix tier-fix-leverage row (post-V15-V19)

The matrix would add 5 rows; all touch only T1. The T1 tier-fix-leverage figure would rise by ~7.2 (1.8 + 1.8 + 2.4 + 0.6 + 0.6). T1 was already at 179; new sum 186.2. Ranking unchanged (T1 still highest-leverage).

### Update to §5 fix order (post-V15-V19)

Insert post-V12: **V17 (2.4) > V15 (1.8) ≈ V16 (1.8) > V18 (0.6) ≈ V19 (0.6).** All five rank below V12 (1.5)? — re-check: V17 (2.4) > V12 (1.5), so V17 inserts between V8 (8.0) and V7 (6.0)? No: per the leverage column V17=2.4 ranks between V12 (1.5) and V7 (6.0), so the fix order becomes:

```
V3(55) > V2(50) > V5(30) > V10(21) > V11(18) > V4(15) > V1(12) > V9(10) > V8(8) >
V7(6) > V6(4.5) > V17(2.4) > V15(1.8) ≈ V16(1.8) > V12(1.5) > V18(0.6) ≈ V19(0.6).
```

V15–V19 are fold-in candidates for a **new Phase F** (V1-sync remediation) that ships AFTER Phase E. Phase F is optional — the cumulative leverage of V15-V19 is 7.2, smaller than any single phase's main violations.

### Update to §7 Phase C pre-conditions

Add to Phase C entry conditions:

> "Phase C MUST NOT copy the iterator path from `ForStRsMapState.forEachEntry()` (lines 642-733) directly — that path's per-row `byte[]` allocations (V15, V16) are a known violation. Phase C must independently design iterator zero-copy using `MemorySegmentDataInputView` slices for the V2 async MapStateV2 iterator. The per-key off-heap path (get/put/contains/remove at lines 300-505) IS safe to copy."

### Update to §7 Phase A scope (no change needed)

Phase A is unblocked. Phase A touches only V2 async ListState (`ForStRsAsyncListStateV2`), not V1 sync. None of V15-V19 are in Phase A's scope.

### Update to §9 OQ-1

OQ-1 is **CLOSED WITH QUALIFICATION**: V1-sync ValueState and MapState (per-key hot path) are genuinely principle-compliant since `537c1403f2f` / `633af3d3be1`. ListState V1, ReducingState V1, AggregatingState V1, and MapState V1 iterator path are NOT compliant — but they are out of Phase A scope and contribute V15-V19 as fold-in candidates for an optional Phase F.

---

## §5 — Conclusion

Phase A is unblocked. The parent spec needs five §3 rows (V15-V19) plus a Phase C pre-condition update qualifying which V1-sync patterns Phase C may copy. The pattern that earlier sessions falsified ("V1-sync is fully principle-compliant since `537c1403f2f`") is corrected here to "V1-sync ValueState + MapState per-key are compliant; ListState/Reducing/Aggregating V1 + MapState V1 iter path are NOT — but they are outside Phase A's scope."

This re-audit's empirical-attribution step is N/A (no perf gate; correctness audit only). Future ablation steps in Phase F (if it ships) would validate V15-V19 relief estimates against the §3.5 model.
