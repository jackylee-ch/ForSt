# Round 4 — Agent D — JDK 25 Leverage Audit

**Reviewer:** Agent D
**Angle:** Verify Round 1–3 D-track findings closed; surface new JDK 25 leverage opportunities.
**Date:** 2026-05-22
**Repo under review:** Flink statebackend (`/Users/lijunqing/code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/`) on branch `forst-rs-jdk25` @ `329ba5f9063`.

---

## Per-PR verification (5 PRs + 1 surgical + 1 config item)

| PR / item | Site | Verified | Notes |
|---|---|---|---|
| **PR-F2** | `ForStRsLinker.java:60-152` | ✅ CLOSED | `FRS_BYTES_LAYOUT`, `FRS_BYTES_LAYOUT_UNALIGNED`, `FFI_ARROW_ARRAY_LAYOUT`, `FRS_CHUNK_*_LAYOUT` defined; 11 `VarHandle` constants (`FRS_BYTES_DATA[_U]`, `FRS_BYTES_LEN[_U]`, `ARROW_ARRAY_{N_BUFFERS,N_CHILDREN,BUFFERS,CHILDREN,RELEASE}`, `FRS_CHUNK_{BUF_PTR,BUF_CAP,ROW_COUNT,BYTES_USED}`). 19 typed access sites (lines 1774, 1912-13, 1984-5, 3017-8, 3027-8, 3222-3, plus `ARROW_ARRAY_RELEASE` at 2212). **Closes J4-6 / D-R2-2.** |
| **PR-B2** (critical-mode) | `ForStRsLinker.java:1113` (`bindCritical`) + 15 sites | ✅ CLOSED | `bindCritical()` defined; applied at 450 (`frs_put`), 462 (`frs_get`), 476 (`frs_get_pinned`), 488 (`frs_get_and_put`), 501 (`frs_delete`), 570 (`frs_batch_get_arrow`), 585 (`frs_bytes_free`), 612 (`frs_lookup_kv`), 623 (`frs_get_into_buf`), 638 (`frs_get_fast`), 765 (`frs_vectorized_batch_put`), 784 (`frs_vectorized_batch_delete`), 875 (`frs_get_at`), 1010 (`frs_vec_iter_prefix_open_batch`), 1041 (`frs_vec_merge_append_batch`). All batched FFIs with bounded safepoint windows are critical. **Closes V2-11 / D-H2.** |
| **PR-B2** (FlatStateCache) | `FlatStateCache.java:49-50, 167-173` | ✅ CLOSED | `INT_VH = MethodHandles.byteArrayViewVarHandle(int[].class, BIG_ENDIAN)`; `readInt`/`writeInt` reduced to single `INT_VH.get`/`set`. **Closes D-R3-3.** |
| **PR-M1** | `ForStRsKeyedStateBackend.java:251, 254-260, 1077-1086` | ✅ CLOSED | `private final List<Arena> threadLocalArenas = new CopyOnWriteArrayList<>()`; `scratchArenaTL` initializer captures `Arena.ofShared()` into the list; `close()` iterates and closes each, then clears (lines 1077-1086). **Closes D-R2-5.** |
| **PR-E3** | `VectorizedExecutor.java:888, 906-955` | ✅ CLOSED | `dispatchIterPrefix` uses `arena.allocate(...)` (the executor's long-lived arena) for `prefixesOff`, `prefixesData`, `outHandles`, `outChunks` (lines 932, 936, 950, 954). Cited spec comment "replace the per-request Arena.ofShared() + per-request" at 906. **Closes D-R2-1 / D-H4.** |
| **V2-9 surgical** | `ArrowBinaryBuffer.java:624-634` (`keysEqual`) | ✅ CLOSED | Uses `keyData.asSlice(kStart, len).mismatch(seg.asSlice(offset, len)) < 0`. **Closes J4-5 partial.** |
| **D-H1 (GC default)** | `ForStRs{Async,}KeyedStateBackend.java:107-128` | ⚠ DOC-CODE MISMATCH | Javadoc states "Default is HEAP" but `System.getProperty("forst.rs.timer-service.factory", "FORSTRS")` — the fallback string is still `FORSTRS`. The code's actual default routes Q11/Q12 timers through the engine. No `config-forst-rs.yaml.tpl` exists in either repo. **NOT CLOSED — see D-R4-1.** |

---

## Summary table

| Sev | # | Location | One-line |
|---|---|---|---|
| H | D-R4-1 | `ForStRsAbstractKeyedStateBackend.java:120` + `ForStRsAsyncKeyedStateBackend.java:127` | `System.getProperty("forst.rs.timer-service.factory", "FORSTRS")` — fallback is `FORSTRS`, not `HEAP`. Javadoc above the call (lines 107-111, 114-118) explicitly states "Default is HEAP" and references `project_q12_heap_timer_beats_forst` (which proves HEAP recovers v3.3 baselines). **The fallback string must be `"HEAP"`.** This is a 1-character fix that reopens D-H1, and absent it the Q11/Q12 wall-clock improvement memorialized in 2026-05-19 specs is not the runtime default. |
| H | D-R4-2 | `VectorizedExecutor.java:806, 810, 817, 823-824` + `ColumnarBatchBuffer.java` family | Round 3 D-R3-2 (`JAVA_INT` → `JAVA_INT_UNALIGNED`) **NOT applied** in any batch-1..9 PR. All offset reads/writes in `dispatchAppendMergeBatch` and `dispatchIterPrefix` still use `JAVA_INT` — the JIT continues to emit alignment checks. Same fix shape, ~10 sites, mechanical. |
| H | D-R4-3 | `VectorizedExecutor.java:808-817` (`dispatchAppendMergeBatch`) | Round 3 D-R3-4 bulk-copy (replace `count+1` `opsOffSeg.set(...)` calls with one `MemorySegment.copy(opsOffsets, 0, opsOffSeg, JAVA_INT_UNALIGNED, 0L, count+1)`) **NOT applied**. Q12 batch median = 257 ⇒ 257 store calls reduced to 1 intrinsic. |
| H | D-R4-4 | `ArrowTimerBuffer.java:405` (`hashOf`) | Round 3 D-R3-1 vectorized hash (`ByteVector.fromMemorySegment` rolling-31) **NOT applied** — still `for (i=0..len) h = 31*h + seg.get(JAVA_BYTE, off+i)` byte-by-byte. Per `project_q12_timer_batchput_win`, this is on every `OP_ADD`/`OP_REMOVE` in the timer burst window. Vector API dep already approved (`ForStRsKeyGroupedSerializer` imports `jdk.incubator.vector.*`). |
| M | D-R4-5 | `VectorizedExecutor.java:700, 785` (`dispatchAppendMerge` + `dispatchAppendMergeBatch`) | Two `Arena.ofConfined()` per-batch (not per-event) allocations introduced by V4 / Phase-A.1 batch dispatch. Acceptable cost per batch, but a single `ThreadLocal<Arena.ofShared()>` mirroring `scratchArenaTL` (with `threadLocalArenas` registration) would eliminate ~50 ns × batches/sec on the APPEND_MERGE hot path. |
| M | D-R4-6 | `ForStRsKeyedStateBackend.java:118-136` | **Stable Values (JEP 502 candidate)** — `arena`, `linker`, `db`, `defaultCf`, `keySerializer`, `keyGroupRange`, `numberOfKeyGroups`, `flushArena`, `flushKeysOffSeg`, `flushKeysDataSeg`, `flushValsOffSeg`, `flushValsDataSeg` are all `private final`, set in the constructor, never reassigned. JEP 502 (Stable Values, finalized JDK 25) lets these escape `final`-field memory-fence semantics into compile-time-constant inlining — relevant for the per-record `setCurrentKey` path that reads `numberOfKeyGroups` and `keyGroupRange.getStartKeyGroup()` 92M times on Q11. |
| L | D-R4-7 | `ForStRsKeyedStateBackend.java:200, ForStRsRestoreOperation.java:536` | `Arena.ofShared()` at lifecycle boundary (`flushArena = Arena.ofAuto()` is auto-managed; restore staging arena is try-with-resources). No per-event leak. Documented for reference. |
| L | D-R4-8 | Test files (33 sites in `src/test/java`) | All test-scope `Arena.ofConfined/ofShared` are try-with-resources; no leak surface. Excluded from D-R2-1 audit. |

**HIGH count: 4** (D-R4-1, D-R4-2, D-R4-3, D-R4-4). M: 2. L: 2.

---

## D-R4-1 — Timer-service default mismatch (BLOCKING for Q11/Q12 perf parity)

`ForStRsAbstractKeyedStateBackend.java:118-122`:
```java
private static TimerServiceFactory pickTimerFactory() {
    String prop =
            System.getProperty("forst.rs.timer-service.factory", "FORSTRS").trim().toUpperCase();
    return "HEAP".equals(prop) ? TimerServiceFactory.HEAP : TimerServiceFactory.FORSTRS;
}
```

Javadoc above says "Default is HEAP". Code default-arg string is `"FORSTRS"`. Result: out-of-box behavior is engine-backed (regressed Q11/Q12). Per `project_q12_heap_timer_beats_forst.md`, the fix that delivers the 1.20× speedup is precisely `forst.rs.timer-service.factory=HEAP` default. Identical mismatch present in `ForStRsAsyncKeyedStateBackend.java:127`. **One-line fix in two files.**

---

## D-R4-2 — `JAVA_INT_UNALIGNED` not applied (Round 3 D-R3-2 deferred through 27 PRs)

All offset-segment accesses in `VectorizedExecutor.java` still use `JAVA_INT`:
- Line 108 (allocate `outOffsets`)
- Lines 587, 589 (read in output decode)
- Lines 684, 687 (read in MERGE per-row dispatch)
- Lines 806, 810, 817 (alloc + write in `dispatchAppendMergeBatch`)
- Lines 823-824 (read in batch dispatch)
- Lines 932, 938, 946 (alloc + write in `dispatchIterPrefix`)
- Lines 1066-7, 1095-6 (per-iter out-params)
- Line 1287 (resize)

`ForStRsDBIterRequest` and `ForStRsLinker` already use `JAVA_INT_UNALIGNED` consistently. The split-dialect makes the executor side slower than the linker side for no reason. Mechanical rename; no risk.

---

## D-R4-3 — Bulk-copy `opsOffsets[]` in `dispatchAppendMergeBatch`

`VectorizedExecutor.java:808-817`:
```java
int writeOff = 0;
for (int row = 0; row < count; row++) {
    opsOffSeg.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, opsOffsets[row]);
    MemorySegment op = valueSliceLists.get(row)[0];
    long opLen = op.byteSize();
    MemorySegment.copy(op, 0L, opsDataSeg, writeOff, opLen);
    writeOff += (int) opLen;
    bytesIn += opLen;
}
opsOffSeg.set(ValueLayout.JAVA_INT, (long) count * Integer.BYTES, opsTotal);
```

Hoist out a single `MemorySegment.copy(opsOffsets, 0, opsOffSeg, JAVA_INT_UNALIGNED, 0L, count + 1)` before the loop; the loop body shrinks to the data copy. JDK ≥ 22 intrinsifies this to `Unsafe.copyMemory`.

---

## D-R4-4 — Vector API for `ArrowTimerBuffer.hashOf`

`ArrowTimerBuffer.java:405-411` is still byte-by-byte polynomial-31. Round 3 D-R3-1 fix shape stands. Vector API dep already in scope (`ForStRsKeyGroupedSerializer.java:25-27` imports `IntVector`, `VectorOperators`, `VectorSpecies`). At 92M Q12 timer ops with N-byte composite key, replacing with a single SIMD pass (`ByteVector.fromMemorySegment` + lane fold, or `MemorySegment.toArray(JAVA_BYTE)` + `Arrays.hashCode` — already SIMD-intrinsified on JDK 16+) recovers the Q12 timer-burst margin.

---

## D-R4-5 — Per-batch `Arena.ofConfined` in V4 APPEND_MERGE

`VectorizedExecutor.java:700, 785` — new `Arena.ofConfined()` per batch (NOT per event). Acceptable steady-state cost (~50 ns alloc × batches/sec), but a `ThreadLocal<Arena>` shared with `scratchArenaTL` and registered into `threadLocalArenas` (closing-out via PR-M1's `close()`) eliminates it. Defer.

---

## D-R4-6 — JEP 502 Stable Values

`ForStRsKeyedStateBackend.java` has 12+ once-set-then-immutable fields. JEP 502 (finalized JDK 25) provides `StableValue<T>` so the JIT treats reads as compile-time constants, bypassing the `final`-field volatile-acquire fence cost. The candidates the prompt called out:
- `keySerializer` (line 122)
- `keyGroupRange` (line 133)
- `numberOfKeyGroups` (line 136) — read 92M times via `KeyGroupRangeAssignment.assignToKeyGroup` on Q11

All confirmed: `private final`, set once in constructor at line 420-423, never reassigned. Conversion is mechanical: `private final StableValue<...> keySerializer = StableValue.of();` + `.setOrThrow(...)` in the constructor. Worth a separate PR — measurable on the `setCurrentKey` per-record path.

---

## Direct answers to prompt

1. **Section 4 + D-R2-1/2/5 + D-R3-1/2/3:**
   - D-R2-1 (PR-E3 long-lived arena): ✅ CLOSED
   - D-R2-2 (PR-F2 typed VarHandles): ✅ CLOSED
   - D-R2-5 (PR-M1 thread-local arena list): ✅ CLOSED
   - D-R3-1 (Vector hash on `ArrowTimerBuffer`): ❌ NOT APPLIED (re-raised as D-R4-4)
   - D-R3-2 (`JAVA_INT_UNALIGNED`): ❌ NOT APPLIED (re-raised as D-R4-2)
   - D-R3-3 (`byteArrayViewVarHandle` in `FlatStateCache`): ✅ CLOSED (PR-B2)
2. **D-H1 (GC config):** ❌ partial — javadoc claims HEAP default, code falls back to FORSTRS. D-R4-1.
3. **D-H2 (critical-mode FFIs):** ✅ CLOSED (15 sites incl. batched).
4. **D-H4 (per-request arena):** ✅ CLOSED in `dispatchIterPrefix`; ⚠ new per-batch sites at lines 700, 785 (M-level, D-R4-5).
5. **New SIMD opportunities (batches 4-9):** D-R4-4 (timer hash) is the lift; no other new hot loops added.
6. **Stable Values (JEP 502):** D-R4-6 — 12 candidates in `ForStRsKeyedStateBackend`; `numberOfKeyGroups` + `keyGroupRange` on the `setCurrentKey` path are highest-leverage.

H: 4, M: 2, L: 2
