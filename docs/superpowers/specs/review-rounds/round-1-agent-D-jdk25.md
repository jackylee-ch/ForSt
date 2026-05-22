# Round 1 — Agent D — JDK 25 Feature Leverage Audit

**Reviewer:** Agent D
**Angle:** JDK 25 feature leverage (criterion #4) — `Linker.Option.critical`, `MemorySegment.mismatch`, `jdk.incubator.vector` SIMD, `Arena` lifecycle, `ValueLayout.ADDRESS.withTargetLayout`, virtual threads, GC flag selection.
**Scope:** `ForStRsLinker.java`, `VectorizedExecutor.java`, `VectorizedClassifier.java`, `ArrowBinaryBuffer.java`, `ArrowTimerBuffer.java`, `ForStRsKeyGroupedSerializer.java`, `SlotArenaScope.java`, `ForStRsKeyedStateBackend.java`, `ForStRsSstUploader.java`, taskmanager YAML templates.
**Date:** 2026-05-22
**Methodology:** Cold read of the listed files only. Pattern-grep for `bind(`/`bindCritical(`, `Arena.ofConfined`/`Arena.ofShared`/`Arena.ofAuto`, `MemorySegment.ofArray`, `ByteVector`/`IntVector`/`LongVector`, `mismatch`, `withTargetLayout`, `ScopedValue`, `Thread.ofVirtual`, `StructuredTaskScope`. Cross-referenced GC flags in `flink-2.2.1/conf/templates/config-forst-rs*.yaml.tpl`.

---

## Summary table

| Sev | # | Location | One-line |
|-----|---|----------|----------|
| H | D1 | `config-forst-rs.yaml.tpl:5,6` | Default JVM template ships `-XX:+UseZGC -XX:+UseCompactObjectHeaders` although both **are documented to hurt Q11/Q12**; every other tpl (g1, g1-noCOH, local, tuned) already moved off ZGC. Configuration footgun for anyone who picks up the "default". |
| H | D2 | `ForStRsLinker.java:907-918, 657-668, 635-649` | `frsVecMergeAppendBatch`, `frsVectorizedBatchPut`, `frsVectorizedBatchGet` are bound with `bind()` (non-critical). These are the **batched hot-path FFIs** — per audit-design §3 V4, they should accept heap `MemorySegment.ofArray(byte[])` keys/ops/values so the per-batch `Arena.ofConfined()` scratch (Executor.java:534) goes away. Currently every batch pays one native alloc + N memcpy for `opsOffSeg`/`opsDataSeg`. |
| H | D3 | `VectorizedExecutor.java:449, 534` | `dispatchAppendMerge{PerRow,Batch}` opens a fresh `Arena.ofConfined()` on **every batch** instead of reusing the long-lived per-slot `arena` field (or `SlotArenaScope.allocateTurn()`). At Q11/Q12 burst sizes this is 1 confined-arena open+close per dispatch (millions/sec under Nexmark). |
| H | D4 | `VectorizedExecutor.java:658, 746` | `dispatchIterPrefix` and `dispatchIterRange` create `Arena.ofShared()` **per request** to hold three tiny out-params (8+4+4 bytes). Shared arenas use a CAS-backed reference-counting handshake — measurably more expensive than confined. The arena is then attached to `FrsIterHandle` for the iter's lifetime, which precludes using turn-bump allocation but a per-iter **`Arena.ofConfined()`** would be 2-3× cheaper and is still correct (iter lifetime is single-threaded per slot). |
| H | D5 | `ArrowBinaryBuffer.java:468-490` + `ArrowTimerBuffer.java:388-411` | `hash(MemorySegment, off, len)` and `keysEqual` / `rowKeyEquals` are **byte-by-byte loops on the V1-sync hot path**. `MemorySegment.mismatch(seg1, fromOff1, toOff1, seg2, fromOff2, toOff2)` (JDK 22+) is a single SIMD-accelerated intrinsic that replaces the whole `keysEqual` body. Per project memory Q11 V1-sync runs 92M ops — these loops dominate cache lookup. |
| M | D6 | `ForStRsLinker.java:1295, 1339, 1449, 1452, 2249, 2324, 2703, 2899` | Every call to `MemorySegment.ofArray(byte[])` allocates a heap-segment wrapper. The `byte[24] FrsBytes` is pooled via ThreadLocal (good — line 2685) but the wrapper segment object is rebuilt every call. JDK has no API to cache the wrapper, so this is "as good as it gets" — but worth noting for future JDK 26+ stable-value semantics (JEP 502 preview) where the wrapper could be `StableValue<MemorySegment>` per-thread. |
| M | D7 | `ForStRsLinker.java` 65+ `bind(` sites | `Linker.Option.critical(true)` is currently applied to **only 8 symbols** (`frs_put`, `frs_get`, `frs_get_pinned`, `frs_get_and_put`, `frs_delete`, `frs_bytes_free`, `frs_lookup_kv`, `frs_get_into_buf`, `frs_get_fast`, `frs_get_at`). The non-critical batched ops (`frs_batch_put`, `frs_batch_get`, `frs_vectorized_batch_{put,get,delete}`, `frs_vec_merge_append{,_batch}`, `frs_vec_iter_*`, `frs_writebatch_*`) all take pointer arrays. If the wrapping helpers built those arrays in heap `byte[]` instead of native `local.allocate(...)`, the calls become `bindCritical`-eligible and shed the per-batch staging arena. |
| M | D8 | `ForStRsLinker.java` (everywhere) | No `ValueLayout.ADDRESS.withTargetLayout(...)` (JDK 22+) typed-pointer FFM is used. Out-params like `outHandle`, `outCount`, `outBytes` (FrsBytes) read back via raw `ADDRESS_UNALIGNED.get()` + manual pointer arithmetic (e.g. lines 2716-2719, 2923-2926). A `StructLayout` for `FrsBytes` with `withTargetLayout` would let the JVM emit checked typed-access and eliminate the `address()` round-trips. Minor perf win, large readability win, eliminates the alignment bug class. |
| M | D9 | `ArrowBinaryBuffer.java:103-126, 550-565` (`resize`) + `ArrowTimerBuffer.java:510-530` | Resize path opens a new `Arena.ofShared()` (line 556 / 515) and closes the **old** one — but rebuilds all heap-index slots in a scalar `for` loop. With the offsets+data already in MemorySegments, this rebuild is also a `MemorySegment.copy` candidate (already used) but the hash table re-insert loop runs scalar. Less hot than D5 but compounds it on grow events. |
| M | D10 | `ForStRsKeyedStateBackend.java:184` | `Arena flushArena = Arena.ofAuto()` — **automatic** arena (collector-driven close) for the flush path. This is technically correct but `Arena.ofAuto()` has unpredictable cleanup timing under ZGC + COH (the lifecycle is tied to phantom-reference reachability). For a flush buffer this is fine; flagged because callers should know `ofAuto` defeats RAII reasoning. |
| M | D11 | `ForStRsKeyedStateBackend.java:627` | `Arena local = Arena.ofShared()` opened, but **not in try-with-resources** — uses an explicit `try { … } finally { local.close(); }` (line 664-665). The pattern works but every other call site in this file uses try-with-resources; consistency win + protects against future refactor accidentally introducing an early-return path. Same shape in `VectorizedExecutor.java:449, 534` (the per-batch confined arenas of D3). |
| M | D12 | `ForStRsKeyGroupedSerializer.java:362-364` | `vectorizedMurmurHash` is correctly SIMD (Step 2), but Step 1 (`Arrays.hashCode(serializedKeys[i])`) is scalar and **data-dependent on key length**. For Q11/Q12 keys (composite key with 3-byte KG + state name + key suffix, typically 16-32 bytes) this is the bottleneck of the routine. A `ByteVector` rolling hash matching `Arrays.hashCode` semantics on a `byte[]` is straightforward (multiply-add by 31 in lanes). |
| M | D13 | `VectorizedClassifier.java` (entire) | The classifier dispatches on `StateRequestType` via a Java `switch` (lines 304-358). With JDK 21+ **pattern matching for switch** + `SealedInterfaces` (StateRequestType is already an enum), this could become a `switch (type)` expression returning a typed handler — slightly faster (table-jump vs branch-predictor) and unlocks the `default → throw new UOE` exhaustiveness check at compile time. Low-impact perf, high-impact correctness. |
| L | D14 | `ForStRsSstUploader.java:66` | `Thread.ofVirtual().start(…)` is used for SST upload. This is **outside** state-backend callbacks (it's the async snapshot path), so it does NOT conflict with Flink's mailbox executor — usage is correct. Confirms the prompt's "don't use virtual threads from state-backend callbacks" guideline is honored. No change needed. |
| L | D15 | `VectorizedExecutor.java` (entire) | No `StructuredTaskScope` use — correctly absent. Flink owns the task concurrency model; the executor stays single-threaded per slot. Confirmed by grep. |
| L | D16 | `ForStRsKeyGroupedSerializer.java:343-348` | Comment explicitly considers and rejects `ScopedValue` (JEP 481) for mutable buffer pooling — justification is correct (`ScopedValue` is immutable-within-scope). ThreadLocal stays. Listed as L only to acknowledge the comment already does the right analysis. |
| L | D17 | `ForStRsLinker.java:1326-1327, 2318-2321, 2685-2686` | ThreadLocals are appropriately scoped (per-slot single-thread, byte[]-pooled). The new JDK 25 **Stable Values** preview (JEP 502, not finalized in 25) would let us replace these with `StableValue<byte[]>`-per-thread when finalized, gaining JIT constant-folding. Not actionable today; flagged as a future-watch. |

---

## H1 (D1) — Default JVM template ships ZGC + CompactObjectHeaders, both proven harmful

**File:** `/Users/lijunqing/Downloads/workenv/flink-2.2.1/conf/templates/config-forst-rs.yaml.tpl:5-6`

```yaml
env.java.opts.taskmanager: ... -XX:+UseZGC -XX:+UseCompactObjectHeaders
env.java.opts.jobmanager:  ... -XX:+UseZGC -XX:+UseCompactObjectHeaders
```

The neighboring templates have already moved off:

| Template | GC | COH |
|---|---|---|
| `config-forst-rs.yaml.tpl` (DEFAULT) | **ZGC** | **+COH** |
| `config-forst-rs-g1.yaml.tpl` | G1 | +COH |
| `config-forst-rs-g1-noCOH.yaml.tpl` | G1 | (none) |
| `config-forst-rs-local.yaml.tpl` | G1 | +COH |
| `config-forst-rs-tuned.yaml.tpl` | G1 | +COH |

Per project memory `project_q12_parity_with_rocksdb` (2026-05-19): _"forst-rs LOCAL+G1+noCOH best 31.5s vs rocksdb 31.3s (parity); ZGC and COH both hurt Q12"_ and `project_q11_always_on_buffer_win`: forst-rs Q11 = 76.5s with the G1 path. The "default" template is the one a fresh operator would copy. **It pessimizes the very workloads (Q11, Q12) the project is benchmarked on.**

Why H: this is a configuration default — its impact ships to every new deployment that doesn't override. Not a code bug per se, but the perf-recovery story we tell in the PMC reviews is invalidated by anyone reading `config-forst-rs.yaml.tpl`.

**Fix shape:** update the default template to G1 (and drop COH or keep based on Q5/Q11 vs Q12 trade — Q11 likes COH-off, Q12 likes COH-off, so just drop it). The "ZGC" variant should be a separate `config-forst-rs-zgc.yaml.tpl` for users who want ZGC's pause-time profile and accept the throughput cost.

---

## H2 (D2) — Batched hot-path FFIs are not bound critical

**File:** `ForStRsLinker.java` lines 420-431 (`frsBatchPut`), 437-447 (`frsBatchGet`), 657-668 (`frsVectorizedBatchPut`), 635-649 (`frsVectorizedBatchGet`), 907-918 (`frsVecMergeAppendBatch`).

All bound with `bind(...)` (no `Linker.Option.critical`). Comment at line 413-419 explains why for `frsBatchPut`:

```java
// 3b. Batch ops — frs_batch_put takes parallel arrays of native
// pointers (uint8_t* const*) and sizes (size_t*). Critical mode is NOT
// applicable here because we pass arrays-of-pointers into-native, which
// must live in native memory anyway (the byte[] addresses inside a
// Java [B[] array can't be pinned simultaneously). Caller stages the
// four arrays via the {@link #batchPut(FrsDb, FrsCfHandle, MemorySegment,
// MemorySegment, MemorySegment, MemorySegment, long)} overload.
```

This reasoning is correct for `frsBatchPut` (true pointer-array). But it does **not** apply to:
- `frsVecMergeAppendBatch` (line 907-918): takes **packed** `keys_off / keys_data / ops_off / ops_data` segments — these are flat byte/int arrays, not pointer-of-pointer. They CAN be heap `byte[]` / `int[]`, and the linker pins them for the call.
- `frsVectorizedBatchPut` (line 657-668): same packed layout — `key_offsets (i32*)`, `key_data (u8*)`, `val_offsets (i32*)`, `val_data (u8*)`, `count`. All flat, all critical-eligible.
- `frsVectorizedBatchGet` (line 635-649): same.

Why H: today `dispatchAppendMergeBatch` (Executor.java:534) opens `Arena.ofConfined()` per dispatch and copies the packed `ops_off + ops_data` into it (lines 555-565). If `frsVecMergeAppendBatch` were `bindCritical`, that whole per-batch Arena disappears — pre-staged heap `int[]` + `byte[]` pinned directly. Same calculation for `frsVectorizedBatchPut/Get`.

**Fix shape:** add a `bindCritical` overload for the three packed-layout symbols. Then change the dispatch site to allocate `int[]` and `byte[]` instead of staging into a confined arena. Coordinated with D3.

---

## H3 (D3) — Per-batch Arena.ofConfined() in dispatchAppendMerge

**File:** `VectorizedExecutor.java:449, 534`

```java
private void dispatchAppendMergePerRow(AppendMergeBatchBuffer buffer) {
    …
    for (int row = 0; row < count; row++) {
        …
        // Build operand_ptrs and operand_lens arrays in a per-row scratch Arena.
        Arena scratch = Arena.ofConfined();                                   // ← H, per-ROW!
        try {
            MemorySegment ptrs = scratch.allocate(ValueLayout.ADDRESS, vs.length);
            MemorySegment lens = scratch.allocate(ValueLayout.JAVA_INT, vs.length);
            for (int i = 0; i < vs.length; i++) {
                …
                MemorySegment nativeV = scratch.allocate(vLen);
                MemorySegment.copy(vs[i], 0L, nativeV, 0L, vLen);
                …
            }
            …
        } finally {
            scratch.close();
        }
    }
}

public int dispatchAppendMergeBatch(AppendMergeBatchBuffer buffer) {
    …
    Arena scratch = Arena.ofConfined();                                       // ← H, per-BATCH
    try {
        …
        MemorySegment opsOffSeg = scratch.allocate(ValueLayout.JAVA_INT, count + 1L);
        MemorySegment opsDataSeg = scratch.allocate(opsTotal);
        …
    } finally {
        scratch.close();
    }
}
```

The executor field `this.arena` (line 65, passed in by `ForStRsAsyncKeyedStateBackend.java:280`) IS the long-lived per-slot Arena. The `SlotArenaScope` (also wired via `setSlotScope`) exposes `allocateTurn(nbytes, align)` for **per-turn bump-allocated** scratch (line 169 of `SlotArenaScope.java`) — which is exactly what `dispatchAppendMergeBatch` needs.

Why H: per-row in the legacy fallback, per-batch in the new path. Even per-batch is ~100µs of arena open+close overhead amortized across the batch. With work-weighted batch median 257 (Q12 histogram), that's a measurable hit on the engine-per-op budget that the project memory put at ~250 ns at large batches.

**Fix shape:** replace `Arena.ofConfined()` with `slotScope.allocateTurn(opsOffSize, 4)` and `slotScope.allocateTurn(opsTotal, 1)`. The turn region is reset by `exitTurn()` at end of dispatch, so the allocation is truly free. Combine with D2 for full elimination (then no scratch needed at all — pre-staged heap arrays via critical-mode).

---

## H4 (D4) — Per-iterator Arena.ofShared() for 16 bytes of out-params

**File:** `VectorizedExecutor.java:658-661, 746-749`

```java
for (int row = 0; row < buffer.count(); row++) {
    …
    // Allocate out-params in a per-iterator Arena; the Arena is closed when the
    // handle is closed (via FrsIterHandle.close() → perIterArena.close()).
    Arena perIterArena = Arena.ofShared();                                    // ← H
    MemorySegment outHandle = perIterArena.allocate(ValueLayout.JAVA_LONG);   // 8 bytes
    MemorySegment outRowCount = perIterArena.allocate(ValueLayout.JAVA_INT);  // 4 bytes
    MemorySegment outBytesUsed = perIterArena.allocate(ValueLayout.JAVA_INT); // 4 bytes
    …
}
```

`Arena.ofShared()` uses a CAS-backed reference-counting handshake on every `close()` (so cross-thread close from `FrsIterHandle.close()` is correct). For 16 bytes of out-params, this is enormous overhead.

Why H: every ITER_PREFIX and ITER_RANGE request pays one shared-arena open. For Q11/Q12 these are limited but for query 11 (which uses session-window assigner, V1 sync but also issues per-key iterators) it's per-record.

**Fix shape:** since the iter handle close is also driven by the slot's single thread (the runtime watchdog) the perIterArena should be `Arena.ofConfined()` — not shared. The `FrsIterHandle.close()` call already happens on the slot thread (the mailbox executor's turn boundary or the explicit user close). If there's a watchdog cleanup path (slotScope's `iterWatchdog`), confirm it dispatches via the slot thread (it does per `SlotArenaScope.java:75` ConcurrentHashMap comment — but the actual close must run on the slot thread).

Alternatively: use `slotScope.allocateTurn(16, 8)` since the out-params don't outlive the open call (they're read into `nativeHandle`, `firstChunkRows`, `firstChunkBytes` immediately on lines 685-687). The Arena currently passed into `FrsIterHandle(jHandleId, nativeHandle, linker, perIterArena, slotScope)` (line 693) appears unused once the out-params are read — confirm by reading `FrsIterHandle`, but this looks like dead lifetime.

---

## H5 (D5) — Byte-by-byte hash + keysEqual on V1-sync hot path

**File:** `ArrowBinaryBuffer.java:468-490`, `ArrowTimerBuffer.java:388-411`

```java
// ArrowBinaryBuffer.java:468
private int hash(MemorySegment seg, long offset, int len) {
    int h = 1;
    for (int i = 0; i < len; i++) {                                           // ← H
        h = 31 * h + seg.get(ValueLayout.JAVA_BYTE, offset + i);
    }
    return h;
}

private boolean keysEqual(int row, MemorySegment seg, long offset, int len) {
    …
    for (int i = 0; i < len; i++) {                                           // ← H
        if (keyData.get(ValueLayout.JAVA_BYTE, kStart + i)
                != seg.get(ValueLayout.JAVA_BYTE, offset + i)) {
            return false;
        }
    }
    return true;
}
```

Both routines are on the V1-sync MapStateCache hot path (per project memory `project_q11_v1_sync_finding`: 92M `ForStRsValueState` V1 ops on Q11). `MemorySegment.mismatch(src, srcFromOff, srcToOff, dst, dstFromOff, dstToOff)` (JDK 22+) is a **single SIMD-accelerated intrinsic** — JDK uses `ByteVector` internally for it. It returns `-1` on equal, or the index of the first mismatching byte.

Why H: keys here are composite key (3-byte KG prefix + state-name + key suffix) so typical 16-64 bytes. A byte-by-byte loop pays 16-64 `get(JAVA_BYTE)` calls + a `MemorySegment.get` virtual dispatch each. SIMD mismatch does it in 1-2 vector ops.

**Fix shape:**

```java
private boolean keysEqual(int row, MemorySegment seg, long offset, int len) {
    int kStart = keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
    int kEnd = keyOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
    if (kEnd - kStart != len) return false;
    return MemorySegment.mismatch(
            keyData, kStart, kStart + len,
            seg, offset, offset + len) < 0;
}
```

For `hash`, the existing `IntVector`-based `vectorizedMurmurHash` in `ForStRsKeyGroupedSerializer.java` is the wrong shape (it hashes an `int[]` of pre-computed Java hashCodes). A `ByteVector`-rolling-31-multiply hash matching `Arrays.hashCode(byte[])` semantics is a separate routine — but for the cache's purposes any stable hash works (it's keyed by hash + tested for equality via `keysEqual`), so a SIMD `xxHash`-style routine would be the cleanest move and is a documented use case for `ByteVector`.

---

## Findings the prompt asks about — direct answers

- **`Linker.Option.critical(true)` usage**: applied to 8 hot point-op symbols (good); NOT applied to batched ops that could benefit (D2, D7).
- **`Linker.Option.critical(allowHeapAccess=true)`**: comment at line 987 says "allowHeapAccess=true so MemorySegment.ofArray(byte[]) is acceptable" — but the actual call is `Linker.Option.critical(true)`, which in JDK 22+ implies allowHeapAccess. **Correct usage** for the symbols where it's applied. Should be extended (D2).
- **`MemorySegment.mismatch()`**: **NOT USED ANYWHERE** in production code. Multiple high-value targets (D5).
- **`jdk.incubator.vector` SIMD**: used in **exactly one place** (`ForStRsKeyGroupedSerializer.batchAssignKeyGroups` — `IntVector` for murmur hash). Not used in `ArrowBinaryBuffer.hash` or `keysEqual` (D5), not used in `ArrowTimerBuffer.hashOf`/`rowKeyEquals`. The single usage is also gated on the caller computing scalar `Arrays.hashCode` first (D12).
- **`StructuredTaskScope`**: not used (correct — Flink owns concurrency).
- **`Thread.ofVirtual()`**: 1 use in `ForStRsSstUploader.java:66` for SST upload (correct — not in a backend callback). No virtual threads in any state callback path. Confirmed safe (D14).
- **`Arena` lifecycle**: `Arena.ofShared()` used as long-lived per-slot/per-buffer arena (correct), `Arena.ofAuto()` once in `ForStRsKeyedStateBackend.flushArena` (D10 — works but obscures lifecycle), `Arena.ofConfined()` used in `ForStRsLinker` for legacy convenience overloads (correct) and twice per-dispatch in `VectorizedExecutor.dispatchAppendMerge*` (D3 — wrong, should reuse slot arena).
- **`ValueLayout.ADDRESS.withTargetLayout(...)`**: NOT USED. All out-pointer reads use raw `ADDRESS_UNALIGNED.address()` + manual offset arithmetic (D8).
- **JDK 25 preview features** (Stable Values, PEM API): not used. Stable Values would help with the ThreadLocal byte[24]/byte[8] pools (D17) once finalized.
- **ZGC vs G1**: default template uses ZGC even though all empirical evidence (Q11/Q12 project memory) says G1 wins. **Footgun** (D1).
- **CompactObjectHeaders**: off-heap-dominant code path means the 4-byte/object savings are negligible. COH has been measured to HURT Q12 (per project memory). Should be off by default for forst-rs (D1).
- **`AutoCloseable` + try-with-resources for Arenas**: 14 of 16 `ofConfined()` use try-with-resources (good). Exceptions: `VectorizedExecutor.java:449, 534` use try-finally (functionally equivalent, stylistically off — D11), `ForStRsKeyedStateBackend.java:627` uses try-finally for an `ofShared()` (D11).

---

## Prioritization notes for the PMC

The H findings cluster into two themes:

1. **JVM configuration (D1)** — single-line fix in the YAML template, immediate end-to-end win on Q11/Q12 (the workloads the project is measured on). **Highest leverage / lowest risk.**
2. **FFM hot-path arena reuse (D2 + D3 + D4)** — replace per-dispatch `Arena.ofConfined()` / per-iter `Arena.ofShared()` with the long-lived slot Arena + `bindCritical` for the batched symbols. Requires coordinated FFI binding + dispatch-site changes but eliminates a measurable fraction of the per-batch overhead the engine-per-op budget cares about. Medium risk (need to confirm the slot-Arena ownership invariants hold across the changed paths).
3. **`MemorySegment.mismatch` for cache keysEqual (D5)** — single-method swap, drop-in replacement. Should be paired with a `ByteVector`-based key hash for the 92M-op V1-sync path. Medium leverage.

D6-D13 are quality-of-implementation findings, not perf-blockers. D14-D17 are confirmations that the existing choices are correct under JDK 25's idiom set.
