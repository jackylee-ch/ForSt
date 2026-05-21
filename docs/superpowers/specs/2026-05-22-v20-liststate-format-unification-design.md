# V20 — ListState V2 Dual-Class Serialization Format Unification

**Date:** 2026-05-22
**Predecessor:** `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md` §3 V20 + §3.5 Discovery 2026-05-21
**Purpose:** Resolve the partial-wiring gap between `ForStRsAsyncListStateV2` (Flink-integrated, destructive-PUT format) and `ForStRsListStateV2` (standalone, APPEND_MERGE format) so that V3 (LIST_ADD → APPEND_MERGE classifier routing for Q19) can ship safely.
**Branch:** `forst-rs-jdk25` (Flink), `forst-rs` (ForSt). Baseline HEAD: post-V4 (commits `3e1d5a1c2a9` and `e51648b678a` on flink; `50098dc74` and `7b632e555` on ForSt).

---

## §1 — Goal & non-goals

### Goal
Land a single, backward-compatible serialization format for ListState V2 that supports both destructive PUT (LIST_UPDATE) and APPEND_MERGE (LIST_ADD / LIST_ADD_ALL) without snapshot-compat break. Unblock V3 implementation in the same session.

### Non-goals
- Class unification (merging `ForStRsAsyncListStateV2` and standalone `ForStRsListStateV2` into one). Both classes are kept; only their serialization decoder is unified.
- Snapshot version-tagging or format migration logic. Format B (chosen below) is naturally backward-compatible with the legacy single-chunk format, so no migration is needed.
- Flink-runtime changes. All work stays inside `flink-state-backends/flink-statebackend-forst-rs/`.

---

## §2 — Format choice

Two candidates were considered (§3.5 Discovery 2026-05-21):

| Format | Layout | Pros | Cons | Decision |
|---|---|---|---|---|
| **A** — count-free | `[elem_bytes]+` until EOF | Smallest payload. Simplest reader. | Snapshot-compat BREAK with v3.8 (legacy is `[count][elem]`). LIST_UPDATE requires a different encoding than LIST_ADD. | **rejected** |
| **B** — multi-chunk | `[count][elem_bytes×count]+` until EOF | Snapshot-compat with v3.8 (legacy `[count=1][elem]` reads as one chunk → EOF). LIST_UPDATE and LIST_ADD use the same encoding (LIST_UPDATE = single chunk; LIST_ADD = single-chunk operand that the engine appends). Format already shipped in standalone V2's `get(byte[])` decoder (lines 173-194). | Slightly larger payload (4 bytes per merge operand). | **APPROVED** |

**Format B is the only format that preserves v3.8 snapshot compat.** No migration code required.

### B verified backward compatible with existing snapshots
v3.8's `ForStRsAsyncListStateV2.serializeValueInto` writes `[count=1][elem]` for LIST_ADD (destructive PUT). After K appends, the stored value is just the **last** `[count=1][elem]` (because each PUT overwrites). Reading that with the Format B loop-until-EOF decoder: reads `count=1`, reads 1 element, hits EOF, returns the singleton list — identical to what the old single-chunk decoder returns. ✓

### B handles LIST_UPDATE correctly
LIST_UPDATE remains a destructive PUT with payload `[count=N][elems]`. The Format B decoder reads `count=N`, reads N elements, hits EOF, returns the N-element list. ✓

### B handles LIST_ADD via APPEND_MERGE
Post-V3, LIST_ADD emits an `AppendMergeRequest` with operand `[count=1][elem]`. After K appends to the same key, the engine's merge operator concatenates the operands, producing `[count=1][e0][count=1][e1]…[count=1][e(K-1)]`. The Format B decoder reads each `[count=1][e_i]` chunk in order, returns the full K-element list. ✓

### B handles mixed update/add sequences
LIST_UPDATE then LIST_ADD: destructive PUT of `[count=N][elems]` then APPEND_MERGE of `[count=1][new_elem]` → stored = `[count=N][elems][count=1][new_elem]` → decoder reads N elements then 1 element → returns (N+1)-element list. ✓

---

## §3 — ListStateV2 dual-class strategy

Both classes are kept. Their serialization decoders are unified to Format B via a small shared utility.

### §3.1 — `ForStRsAsyncListStateV2.deserializeValue` change

**Today:**
```java
public Object deserializeValue(byte[] raw) {
    if (raw == null || raw.length == 0) {
        return new CompleteStateIterator<V>(Collections.emptyList());
    }
    try {
        valueIn.setBuffer(raw);
        int count = valueIn.readInt();           // reads ONE count
        List<V> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(elementSerializer.deserialize(valueIn));
        }
        return new CompleteStateIterator<>(list);
    } catch (IOException e) {
        throw new RuntimeException(
                "ForStRsAsyncListStateV2: failed to deserialize value", e);
    }
}
```

**After V20:**
```java
public Object deserializeValue(byte[] raw) {
    if (raw == null || raw.length == 0) {
        return new CompleteStateIterator<V>(Collections.emptyList());
    }
    try {
        valueIn.setBuffer(raw);
        List<V> list = new ArrayList<>();
        // Format B: loop [count][elems*] chunks until EOF.
        while (valueIn.available() > 0) {
            int count = valueIn.readInt();
            if (count < 0) {
                throw new IOException(
                        "ForStRsAsyncListStateV2: negative count in merged payload");
            }
            for (int i = 0; i < count; i++) {
                list.add(elementSerializer.deserialize(valueIn));
            }
        }
        return new CompleteStateIterator<>(list);
    } catch (IOException e) {
        throw new RuntimeException(
                "ForStRsAsyncListStateV2: failed to deserialize value", e);
    }
}
```

Identical logic to standalone V2's `get(byte[])` lines 180-189.

### §3.2 — `ForStRsAsyncListStateV2.serializeValueInto` unchanged

The `[count=1][elem]` encoding for LIST_ADD and `[count=N][elems]` for LIST_UPDATE / LIST_ADD_ALL is already correct under Format B. No change needed.

### §3.3 — No new shared utility class

We do NOT extract a `ListStateFormatBCodec` class. The change is 5 lines in `deserializeValue` — DRY at the class level would be a stronger argument once a third ListState class appears.

---

## §4 — Classifier routing for V3 (LIST_ADD → APPEND_MERGE)

### §4.1 — Add `recordAppendMerge` to `VectorizedClassifier.offer()`

**Today** the classifier routes LIST_ADD via `recordPut` (destructive overwrite):

```java
case LIST_ADD:
case LIST_ADD_ALL:
case MAP_PUT:
// ...
    if (stateRequest.getPayload() == null) {
        recordDelete(table, (StateRequest) stateRequest);
    } else {
        recordPut(table, (StateRequest) stateRequest);
    }
    break;
```

**After V3** the classifier checks `listStateNames` registry; if the state name is a registered ListState, route to a new `recordAppendMerge` method:

```java
case LIST_ADD:
case LIST_ADD_ALL:
    if (stateRequest.getPayload() == null) {
        recordDelete(table, (StateRequest) stateRequest);
    } else if (listStateNames.contains(table.getStateName())) {
        recordAppendMerge(table, (StateRequest) stateRequest);
    } else {
        // Should not happen — ListState API guarantees LIST_ADD only fires
        // on ListState instances. But for safety, fall back to PUT.
        recordPut(table, (StateRequest) stateRequest);
    }
    break;
// MAP_PUT etc. unchanged
case VALUE_UPDATE:
case LIST_UPDATE:
case MAP_PUT:
// ... (unchanged: recordPut)
```

Note: `LIST_UPDATE` stays on the PUT path (destructive replace, by API contract).

### §4.2 — `recordAppendMerge` implementation

```java
private <K, N, V> void recordAppendMerge(
        ForStRsInnerTable<K, N, V> table, StateRequest<K, N, ?, ?> request) {
    ensureAppendMergeBuffer();
    // Serialize key + value into temporary byte[] buffers (same as recordPut),
    // then wrap in heap-backed MemorySegments. dispatchAppendMergeBatch copies
    // into off-heap opsDataSeg before the FFI call, so heap-backed slices are
    // valid for the request's lifetime.
    byte[] keyBytes = table.serializeKey(request);
    byte[] valBytes = table.serializeValue(request.getPayload());
    if (valBytes == null) {
        // Defensive — null payload should have been caught by the LIST_ADD null check
        // above; route to delete for safety.
        recordDelete(table, request);
        return;
    }
    MemorySegment keySlice = MemorySegment.ofArray(keyBytes);
    MemorySegment valSlice = MemorySegment.ofArray(valBytes);
    AppendMergeRequest amReq =
            new AppendMergeRequest(table.getStateName(), keySlice, new MemorySegment[] {valSlice});
    appendMergeBuffer.append(amReq);
    // Per-request future completion: we need to plumb the StateRequest's future
    // through to amReq.future(). See §4.3.
}
```

### §4.3 — StateRequest ↔ AppendMergeRequest future plumbing

`StateRequest` carries a Flink-runtime `InternalAsyncFuture` (the one user code awaits). `AppendMergeRequest` has its own `CompletableFuture<Void>`. The dispatch path completes `amReq.future()`; we need that to translate to the underlying `StateRequest`'s future.

Two implementation options:
- **Option Y — `request.completeWith(amReq.future())`**: hook the Flink runtime's completion onto the `amReq.future()`. Mirrors how `recordPut` completes the request after the executor's PUT FFI call. Need to find the exact completion method on `StateRequest`.
- **Option Z — track in parallel list**: maintain a `List<StateRequest>` parallel to `appendMergeBuffer.futures()` and complete each at dispatch end. Symmetric with `putRequests` etc.

**Choose Option Z** — symmetric with existing PUT/GET/DELETE paths in the classifier (which all maintain parallel `*Requests` arrays). Add an `appendMergeRequests` array to `VectorizedClassifier`; at dispatch end (in `VectorizedExecutor.dispatchAppendMergeBatch`), complete each StateRequest's future from its corresponding amReq future result.

### §4.4 — `ForStRsInnerTable.getStateName()`

The audit reveals `ForStRsInnerTable` doesn't expose a state name directly. The state name is needed by `recordAppendMerge` to construct `AppendMergeRequest`. Two options:
- Add `String getStateName()` to the `ForStRsInnerTable` interface (default method returns `null`, override in `ForStRsAsyncListStateV2` to return `stateName`).
- Track state name in a `Map<ForStRsInnerTable, String>` registered alongside `listStateNames`.

**Choose interface-default option** — single-line interface extension, clean.

---

## §5 — ForStRsAsyncListStateV2 init: register with classifier

The classifier's `listStateNames` registry must include `ForStRsAsyncListStateV2`'s state name. Today only standalone `ForStRsListStateV2` registers (line 91 of its file).

Add to `ForStRsAsyncListStateV2`'s constructor (or to a `setClassifier(VectorizedClassifier)` method invoked by the backend):

```java
// In ForStRsAsyncKeyedStateBackend, where the state is constructed:
ForStRsAsyncListStateV2<K, N, V> state = new ForStRsAsyncListStateV2<>(...);
classifier.registerListState(stateName);
```

The exact registration site depends on where `ForStRsAsyncListStateV2` is instantiated. The `ForStRsAsyncKeyedStateBackend.createListState(...)` path is the natural location.

---

## §6 — Test plan

### §6.1 — Unit tests (correctness)

| Test | Class | Asserts |
|---|---|---|
| `asyncAdd_K_entries_returns_all_in_order` | new `ForStRsAsyncListStateV2V20Test` | 1000 `asyncAdd(i).get()` calls, `asyncGet()` returns `[0, 1, …, 999]` in order |
| `asyncAddAll_preserves_order_within_call` | same | Single `asyncAddAll([10, 20, 30])` then `asyncGet()` returns `[10, 20, 30]` |
| `asyncUpdate_replaces_full_list` | same | `asyncAdd(1, 2, 3)` then `asyncUpdate([99])` then `asyncGet()` returns `[99]` |
| `asyncClear_empties_list` | same | `asyncAdd(1, 2)` then `asyncClear()` then `asyncGet()` returns `[]` |
| `mixed_update_then_add` | same | `asyncUpdate([1, 2])` then `asyncAdd(3)` then `asyncGet()` returns `[1, 2, 3]` |
| `deserializeValue_legacy_single_chunk` | same | byte[] = encoded `[count=1][elem=42]` produced by v3.8 path → decoded as `[42]` (snapshot-compat regression test) |
| `deserializeValue_multi_chunk` | same | byte[] = `[1][1][1][2][1][3]` → decoded as `[1, 2, 3]` (Format B core) |

### §6.2 — Snapshot restore test

Existing `ForStRsListStateV2RestoreTest` (if present) should be re-run; if absent, add a minimal restore test:

```java
@Test
void asyncListStateV2_restore_from_legacy_v38_snapshot() {
    // Manually write a legacy [count=1][elem] payload via the destructive PUT
    // path; close backend; reopen; verify asyncGet() returns the singleton list.
}
```

### §6.3 — Nexmark Q19 bench gate

Per §3.5 D3 (re-derived 2026-05-22): Q19 conservative estimate **30-40 s relief** on the 135 s baseline. Gate: Q19 ≤ 105 s post-V3 (= 135 - 30, lower bound). Aspirational: Q19 ≤ 80 s (= 135 - 55, optimistic upper bound matching the original D3 estimate, achievable if the RocksDB engine's get-combine-put internal cost is amortized by merge operator efficiency).

Per the §7 per-fix attribution rule:
- Measure Q19 + the 5 regression-gate queries (Q11/Q12/Q15/Q20/Q23) on V3 jar
- If Q19 relief in [21, 39] s and no regression > 5%: attribution PASSES
- Outside band: trigger ablation (revert V3 alone, re-bench Q19, confirm baseline returns)

---

## §7 — Risk surface

### §7.1 — Classifier-path complexity
The new `recordAppendMerge` adds an off-heap allocation per LIST_ADD. Heap-backed MemorySegments are used (`MemorySegment.ofArray(byte[])`); the `dispatchAppendMergeBatch` already copies into off-heap `opsDataSeg`. No new Arena lifecycle to manage.

### §7.2 — Snapshot compat
Format B subsumes the legacy single-chunk format (verified §2). No migration code or version tag required. Add a regression test (§6.1, `deserializeValue_legacy_single_chunk`) to prevent future format breaks.

### §7.3 — LIST_ADD null payload handling
Defensive check in `recordAppendMerge` routes null payload to `recordDelete` (matches today's LIST_ADD-null semantics in the classifier's switch).

### §7.4 — Ordering invariant
`asyncAdd(a).get(); asyncAdd(b).get()` must produce `[a, b]` (in-call order). Format B preserves this because the engine's merge operator concatenates operands in submit order, and our APPEND_MERGE submits within a single batch maintain submit order (StateRequest queue is FIFO per record-context).

### §7.5 — Multiple list-states in one batch
A batch may contain LIST_ADD requests for multiple distinct ListState names. Each request's `state name + serialized key` is unique, so they don't collide in `dispatchAppendMergeBatch`'s engine-side grouping. Existing test `ClassifierGuardsTest` covers multi-state APPEND_MERGE submission.

### §7.6 — Pre-snapshot flush
APPEND_MERGE writes go through the engine immediately at `dispatchAppendMergeBatch` time (within the batch). They are persisted before the snapshot barrier completes, same as PUT. No new pre-flush hook needed.

---

## §8 — Implementation phases

This session targets the full implementation. Sub-phases:

| Sub-phase | Files | Tests | Commit message |
|---|---|---|---|
| **V20.1** — Format B decoder in `ForStRsAsyncListStateV2.deserializeValue` | `ForStRsAsyncListStateV2.java:200-217` | `deserializeValue_legacy_single_chunk` + `deserializeValue_multi_chunk` | `feat(state-forst-rs): V20.1 — ListState V2 Format B decoder` |
| **V20.2** — `ForStRsInnerTable.getStateName()` interface method | `ForStRsInnerTable.java` + impls | trivially exercised | `feat(state-forst-rs): V20.2 — ForStRsInnerTable.getStateName()` |
| **V3.1** — Classifier `recordAppendMerge` | `VectorizedClassifier.java` | new path-coverage test | `feat(state-forst-rs): V3.1 — classifier APPEND_MERGE for LIST_ADD` |
| **V3.2** — `ForStRsAsyncListStateV2` registers with classifier | `ForStRsAsyncKeyedStateBackend.java` or constructor | registration test | `feat(state-forst-rs): V3.2 — register AsyncListStateV2 for APPEND_MERGE` |
| **V3.3** — Bench gate + commit | bench script | Q19/Q11/Q12/Q15/Q20/Q23 sweep | `bench: V3 attribution PASS / FAIL` |

---

## §9 — D3 re-derivation

Original §3.5 D3 estimated 55 s relief on Q19 by pro-rata-ing the Q12 batched-timer fix. The 2026-05-21 Discovery falsified that analogy (Q12 fix was a timer queue rewrite, not a ListState fix).

**V20 re-derivation:**
- Q19 hot path today: each bid record → ListState.asyncGet (read full list) + ListState.asyncAdd (destructive PUT of single element). Per record: 1 engine GET + 1 engine PUT.
- Q19 hot path post-V3: each bid record → ListState.asyncAdd (APPEND_MERGE single element). Per record: 1 engine APPEND_MERGE (which is internally 1 GET + 1 combine + 1 PUT, but only ONE engine round-trip from the Java side).
- Saving per record: 1 FFM crossing (the GET) + 1 deserialization of the existing list + 1 ArrayList allocation. At 100 M records, that's ~100 M deserializations + ~100 M ArrayList allocs avoided.
- Estimated relief: 30-40 s on Q19's 135 s. (Conservative; upper bound 55 s if the per-record GC + ArrayList alloc cost is larger than projected.)

**D3 new certainty: MED** (analogous-fix-shape proven by V4 but workload-specific dependency on RocksDB merge operator's get-combine-put efficiency).

---

## §10 — Parent spec update needed

After this sub-spec lands and V3 ships:
- §3 V3 description: replace "asyncAdd → full-PUT" with "asyncAdd → APPEND_MERGE via classifier routing"; un-mark STALE.
- §3 V20: mark CLOSED with link to this sub-spec.
- §3.5 D3: update to "30-40 s relief, MED certainty, see V20 sub-spec §9".
- §3.5 Discovery 2026-05-21: append closure note pointing to this sub-spec.
- §7 Phase A.2 ablation table: replace "TBD" Q19 gate with `[21, 39] s` range.

---

## End-of-spec checklist

- [x] §1 goal + non-goals explicit
- [x] §2 format choice rationale + snapshot-compat proof
- [x] §3 dual-class strategy (no merge; decoder unification only)
- [x] §4 classifier routing design (Option Z parallel request list)
- [x] §5 registration site (async backend constructor)
- [x] §6 test plan (7 unit tests + 1 snapshot restore + bench gate)
- [x] §7 risk surface (6 sub-items)
- [x] §8 implementation phases (5 sub-phases)
- [x] §9 D3 re-derivation with new certainty
- [x] §10 parent spec follow-up items
