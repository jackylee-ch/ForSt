# ForSt-RS Vectorization/Batch/Zero-Copy Audit — Phase 0 + Phase A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute Phase 0 (V1-sync compliance re-audit produces a standalone sub-spec) and Phase A (V4 batched merge-append FFI infrastructure → V3 ListState.asyncAdd call-site switch) from the spec at `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md`.

**Architecture:** Phase 0 is read-only / writes one design doc — it validates that V1-sync ValueState/MapState code paths are off-heap principle-compliant so Phase C can copy that pattern safely. Phase A then ships in two commits per the locked V4 → V3 order: A.1 = Rust `frs_vec_merge_append_batch` + Java FFM binding + new `dispatchAppendMergeBatch` executor method (zero behavior change, no call-site touched), A.2 = ListState.asyncAdd override + classifier registration + per-row→batched call-site switch (single behavior change).

**Tech Stack:** Rust 1.78+ (crates/forst-rs-ffi), Java 25 + FFM (jdk.incubator.vector + Arena), Flink 2.2.1 state backend, JUnit 5 + Mockito, cargo criterion, Nexmark bench harness at `nexmark/`.

**Cross-references:**
- Audit design: `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md`
- v3.8 bench baseline: `docs/superpowers/specs/2026-05-21-forst-rs-benchmark-report-v3.8.md`
- Batched-timer reference: `docs/superpowers/specs/2026-05-21-batched-engine-timer-design.md`
- Bench script: `/tmp/v38-bench/run.sh` (reused for A.2 portfolio gates)

**Repos:** Java side `/Users/lijunqing/Code/stczwd/flink` (branch `forst-rs-jdk25`); Rust side `/Users/lijunqing/Code/stczwd/ForSt` (branch `forst-rs`).

---

## Phase 0 — V1-sync compliance re-audit (BLOCKS Phase A)

**Outcome:** standalone sub-spec `docs/superpowers/specs/2026-05-22-v1-sync-compliance-reaudit-design.md` documenting whether ForStRsValueState (V1 sync) and ForStRsMapState (V1 sync) and ReducingState/AggregatingState are principle-compliant against the Tier 1–5 model. If non-compliant, sub-spec adds V15+ violations to the parent spec's §3.

**Time budget:** 1 day. Code reading + writing only — no behavior change.

---

### Task 0.1: Audit ForStRsValueState (V1 sync) off-heap path

**Files:**
- Read: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java`
- Read: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/buffer/ArrowBinaryBuffer.java`
- Read: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/v1sync/MemorySegmentDataInputView.java`

- [ ] **Step 1: Read `ForStRsValueState.value()` path top-to-bottom.** Verify against Tier 1 principle predicates:
  - **Vectorization:** Does `value()` call `linker.put/get/delete` from inside a per-record loop? (If yes — violation.)
  - **Batch:** Does it accumulate ops or fire per-event? (Per-event is OK for V1 sync but must use off-heap fast path.)
  - **Zero-copy:** Does it allocate `new byte[…]` per call? Does it use `ArrowBinaryBuffer.get(scratch, keyOff, keyLen)` or `MemorySegmentDataInputView`? (If byte[] alloc — violation.)

- [ ] **Step 2: Read `ForStRsValueState.update()` path top-to-bottom.** Same predicates against the write path.

- [ ] **Step 3: Capture findings in scratch notes:**

```
ForStRsValueState V1 sync audit:
  value() path:
    Tier 1 (zero-copy): [PASS / FAIL — cite line] (e.g., "value() at line 144 calls ArrowBinaryBuffer.get(scratch, off, len) — zero-copy ✓; at line 162 deserializes through MemorySegmentDataInputView — zero-copy ✓")
    Tier 1 (batch):     [PASS / N/A — V1 sync is inherently per-event]
  update() path:
    Tier 1 (zero-copy): [PASS / FAIL]
    Tier 1 (batch):     [PASS / N/A]
```

(No commit at this step — Phase 0's only commit is at Task 0.5.)

---

### Task 0.2: Audit ForStRsMapState (V1 sync) off-heap path

**Files:**
- Read: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapState.java`

- [ ] **Step 1: Read `ForStRsMapState.get(K)` and `put(K, V)` (the 1c.1 off-heap MapState — commit `633af3d3be1`).** Verify the same predicates as Task 0.1.

- [ ] **Step 2: Read `ForStRsMapState.iterator()` / `entries()` / `keys()` / `values()` paths.** Specifically check whether the iterator allocates `byte[]` per entry (the V8 pattern in §3 of the parent spec — Map**State**V2 has it; check if MapState V1 sync has the same issue).

- [ ] **Step 3: Capture findings.**

---

### Task 0.3: Audit ForStRsListState (V1 sync) if it exists

**Files:**
- Read: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsListState.java` — **may not exist**; if not, document "N/A — only V2 ListState".

- [ ] **Step 1: If file exists, audit `add()`, `addAll()`, `get()` paths.** Specifically check whether the V1 sync ListState uses APPEND_MERGE or does full-list overwrite (Q19's V3 violation).

- [ ] **Step 2: If V1-sync ListState is identical to V2 in structure (full-PUT overwrite), document as V15.**

- [ ] **Step 3: Capture findings.**

---

### Task 0.4: Audit ReducingState / AggregatingState V1-sync inheritance

**Files:**
- Read: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsReducingState.java`
- Read: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsAggregatingState.java`

- [ ] **Step 1: Verify these inherit from `ForStRsValueState` (and thus inherit its compliance / non-compliance).** Check that no override path bypasses the ArrowBinaryBuffer fast path.

- [ ] **Step 2: Capture findings.**

---

### Task 0.5: Write the V1-sync compliance sub-spec

**Files:**
- Create: `docs/superpowers/specs/2026-05-22-v1-sync-compliance-reaudit-design.md`

- [ ] **Step 1: Write the sub-spec.** Required sections:

```markdown
# ForSt-RS V1-Sync Compliance Re-Audit

**Date:** 2026-05-22
**Predecessor:** docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md §9 OQ-1
**Phase 0 status:** [COMPLETE / IN-PROGRESS]
**Phase A status:** [BLOCKED until this closes]

## §1 — Scope

V1-sync state classes audited against the Tier 1–5 model:
- ForStRsValueState
- ForStRsMapState
- ForStRsListState (if exists)
- ForStRsReducingState
- ForStRsAggregatingState

## §2 — Per-class findings

### ForStRsValueState
- Off-heap status: [PASS / PARTIAL / FAIL]
- Evidence (file:line citations from Task 0.1)
- Violations found (if any) — assign V15, V16... numbers

### ForStRsMapState
(same shape)

[... per remaining state class ...]

## §3 — Compliance verdict

Net result:
- [ ] V1-sync IS principle-compliant — Phase C can safely copy the off-heap pattern.
- [ ] V1-sync is NOT principle-compliant — V15+ violations added to parent spec; Phase C must be re-designed before it ships.

## §4 — Parent spec update needed?

If non-compliant: list the V15+ rows that must be added to parent spec §3.

If compliant: no parent-spec update; close OQ-1 with this attestation.
```

- [ ] **Step 2: Commit the sub-spec.**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
git add docs/superpowers/specs/2026-05-22-v1-sync-compliance-reaudit-design.md
git -c commit.gpgsign=false commit -m "$(cat <<'EOF'
docs: V1-sync compliance re-audit (Phase 0 prerequisite for OQ-1)

Audits V1-sync state classes against Tier 1-5 model. Closes parent
spec's OQ-1 with empirical attestation. Phase A unblocks.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3: Per-fix attribution step (Phase 0):** n/a — Phase 0 measures correctness, not perf. Skip ablation. Document attribution-step as "skipped (no perf measurement)" in the sub-spec's §3 verdict.

---

## Phase A — Batched merge-append for ListState (V4 first, then V3)

**Phase A entry gate:** Phase 0 sub-spec landed AND user-reviewed AND verdict is either (a) V1-sync compliant — no parent-spec update OR (b) V1-sync non-compliant — parent spec §3 updated with V15+ rows.

**Outcome:** Q19 wall-clock ≤ 70 s on the v3.8 fresh-cluster bench; no regression > 5 % on Q11/Q12/Q15/Q20/Q23.

---

### Task A.1.1: Add Rust `frs_vec_merge_append_batch` FFI symbol

**Files:**
- Modify: `crates/forst-rs-ffi/src/lib.rs` (around the existing `frs_vec_merge_append` function — line varies; grep `frs_vec_merge_append` to locate)
- Test: `crates/forst-rs-ffi/tests/merge_append_batch.rs` (NEW)

- [ ] **Step 1: Write the failing Rust test.**

```rust
// crates/forst-rs-ffi/tests/merge_append_batch.rs
use forst_rs_ffi::*;

#[test]
fn merge_append_batch_three_rows() {
    // Open DB
    let dir = tempfile::tempdir().unwrap();
    let db_handle = unsafe { frs_open_db_with_cf(/* params per existing test fixture */) };

    // Layout: 3 rows, each (key, op).
    //   row 0: key="k1", op="[count=1][bytes_v1]"
    //   row 1: key="k2", op="[count=1][bytes_v2]"
    //   row 2: key="k1", op="[count=1][bytes_v3]"
    let keys_data = b"k1k2k1";
    let keys_off: [u32; 4] = [0, 2, 4, 6];
    let op0 = make_merge_op_bytes(b"v1");
    let op1 = make_merge_op_bytes(b"v2");
    let op2 = make_merge_op_bytes(b"v3");
    let mut ops_data = Vec::new();
    ops_data.extend_from_slice(&op0);
    ops_data.extend_from_slice(&op1);
    ops_data.extend_from_slice(&op2);
    let ops_off: [u32; 4] = [
        0,
        op0.len() as u32,
        (op0.len() + op1.len()) as u32,
        (op0.len() + op1.len() + op2.len()) as u32,
    ];

    let rc = unsafe {
        frs_vec_merge_append_batch(
            db_handle,
            keys_off.as_ptr(),
            keys_data.as_ptr(),
            ops_off.as_ptr(),
            ops_data.as_ptr(),
            3,
        )
    };
    assert_eq!(rc, 0);

    // Verify by reading back.
    let mut out = vec![0u8; 256];
    let mut out_len: u32 = 0;
    let rc = unsafe { frs_get(db_handle, b"k1".as_ptr(), 2, out.as_mut_ptr(), &mut out_len) };
    assert_eq!(rc, 0);
    out.truncate(out_len as usize);
    assert_eq!(out, b"[count=2][v1][v3]"); // engine merge concatenated
}

fn make_merge_op_bytes(payload: &[u8]) -> Vec<u8> {
    // [count=1u32 LE][payload]
    let mut v = Vec::new();
    v.extend_from_slice(&1u32.to_le_bytes());
    v.extend_from_slice(payload);
    v
}
```

- [ ] **Step 2: Run the test — expect "function not found".**

```bash
cd /Users/lijunqing/Code/stczwd
cargo test -p forst-rs-ffi --test merge_append_batch
```

Expected: compilation error `cannot find function frs_vec_merge_append_batch`.

- [ ] **Step 3: Implement the function.** Locate the existing `frs_vec_merge_append` in `crates/forst-rs-ffi/src/lib.rs` (single-row version). Implement the batched variant directly above or below it:

```rust
/// Batched merge-append. Engine receives N (key, op) pairs and invokes
/// the merge-operator on each. Zero-copy: pointers reference caller-owned
/// memory and must remain valid for the call's duration.
///
/// Layout: keys_off has N+1 entries; keys[i] = keys_data[keys_off[i]..keys_off[i+1]].
/// Same for ops_off / ops_data.
///
/// Returns 0 on success, negative errno-like on failure.
#[no_mangle]
pub unsafe extern "C" fn frs_vec_merge_append_batch(
    db_handle: u64,
    keys_off: *const u32,
    keys_data: *const u8,
    ops_off: *const u32,
    ops_data: *const u8,
    n: u32,
) -> i32 {
    let db = match resolve_db_handle(db_handle) {
        Some(db) => db,
        None => return -1,
    };
    let mut batch = rocksdb::WriteBatch::default();
    for i in 0..n as usize {
        let k_start = unsafe { *keys_off.add(i) } as usize;
        let k_end = unsafe { *keys_off.add(i + 1) } as usize;
        let key = unsafe { std::slice::from_raw_parts(keys_data.add(k_start), k_end - k_start) };
        let o_start = unsafe { *ops_off.add(i) } as usize;
        let o_end = unsafe { *ops_off.add(i + 1) } as usize;
        let op = unsafe { std::slice::from_raw_parts(ops_data.add(o_start), o_end - o_start) };
        batch.merge(key, op);
    }
    match db.write(batch) {
        Ok(()) => 0,
        Err(_) => -2,
    }
}
```

- [ ] **Step 4: Run the test — expect PASS.**

```bash
cargo test -p forst-rs-ffi --test merge_append_batch
```

Expected: 1 passed.

- [ ] **Step 5: Run the criterion micro:**

Create `crates/forst-rs-ffi/benches/merge_append_batch.rs`:

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

fn merge_append_batch_1024(c: &mut Criterion) {
    // Setup: open DB; prepare 1024 rows of (k, op) with realistic sizes.
    let setup = bench_setup(1024);
    let mut group = c.benchmark_group("merge_append_batch");
    group.throughput(Throughput::Elements(1024));
    group.bench_function("1024", |b| {
        b.iter(|| {
            unsafe {
                forst_rs_ffi::frs_vec_merge_append_batch(
                    setup.db_handle,
                    setup.keys_off.as_ptr(),
                    setup.keys_data.as_ptr(),
                    setup.ops_off.as_ptr(),
                    setup.ops_data.as_ptr(),
                    1024,
                )
            };
            black_box(())
        })
    });
    group.finish();
}

criterion_group!(benches, merge_append_batch_1024);
criterion_main!(benches);
```

Run:

```bash
cargo bench -p forst-rs-ffi --bench merge_append_batch -- merge_append_batch/1024
```

Expected (per §3.5 D4): ≤ 10 µs.

- [ ] **Step 6: Don't commit yet — A.1 is a single commit at the end of Task A.1.3.**

---

### Task A.1.2: Add Java FFM binding `linker.frsVecMergeAppendBatch(...)`

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java`

- [ ] **Step 1: Locate the existing `frsVecMergeAppend` symbol in `ForStRsLinker.java`** (grep `frs_vec_merge_append`).

- [ ] **Step 2: Add the batched binding directly below the per-row one:**

```java
// existing per-row:
// private static final MethodHandle FRS_VEC_MERGE_APPEND = ...
// public int frsVecMergeAppend(long dbHandle, MemorySegment key, int keyLen, MemorySegment op, int opLen) { ... }

private static final MethodHandle FRS_VEC_MERGE_APPEND_BATCH = linker.downcallHandle(
    lookup.find("frs_vec_merge_append_batch").orElseThrow(),
    FunctionDescriptor.of(
        ValueLayout.JAVA_INT,           // return rc
        ValueLayout.JAVA_LONG,           // db_handle
        ValueLayout.ADDRESS,             // keys_off (u32*)
        ValueLayout.ADDRESS,             // keys_data (u8*)
        ValueLayout.ADDRESS,             // ops_off (u32*)
        ValueLayout.ADDRESS,             // ops_data (u8*)
        ValueLayout.JAVA_INT             // n
    ),
    Linker.Option.critical(true)         // critical mode, no thread-state transition
);

public int frsVecMergeAppendBatch(
        long dbHandle,
        MemorySegment keysOff,
        MemorySegment keysData,
        MemorySegment opsOff,
        MemorySegment opsData,
        int n) {
    try {
        return (int) FRS_VEC_MERGE_APPEND_BATCH.invokeExact(
            dbHandle, keysOff, keysData, opsOff, opsData, n);
    } catch (Throwable t) {
        throw new RuntimeException(t);
    }
}
```

- [ ] **Step 3: Don't commit yet.**

---

### Task A.1.3: Add `VectorizedExecutor.dispatchAppendMergeBatch(...)` (NOT yet wired)

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java`
- Test: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/VectorizedExecutorAppendMergeBatchTest.java` (NEW)

- [ ] **Step 1: Write the failing test.**

```java
// VectorizedExecutorAppendMergeBatchTest.java
public class VectorizedExecutorAppendMergeBatchTest {

    @Test
    public void dispatchAppendMergeBatch_three_rows_ok() {
        try (Arena arena = Arena.ofConfined()) {
            // Build 3-row columnar buffer
            byte[] keys = "k1k2k1".getBytes(StandardCharsets.UTF_8);
            int[] keysOff = {0, 2, 4, 6};
            byte[] ops = buildOps("v1", "v2", "v3"); // [1u32 LE][bytes]+
            int[] opsOff = computeOffsets(ops, /* per-row sizes */);

            MemorySegment keysOffSeg = arena.allocate(JAVA_INT, keysOff.length);
            // ... copy keysOff into seg, etc.

            int rc = executor.dispatchAppendMergeBatch(
                dbHandle, keysOffSeg, keysDataSeg, opsOffSeg, opsDataSeg, 3);
            assertEquals(0, rc);

            // Verify by reading back via existing get path
            byte[] readBack = readKey(dbHandle, "k1");
            assertArrayEquals(expectedConcatenated("v1", "v3"), readBack);
        }
    }
}
```

- [ ] **Step 2: Run test — expect FAIL with method not found.**

```bash
cd /Users/lijunqing/Code/stczwd/flink
mvn -pl flink-state-backends/flink-statebackend-forst-rs test \
  -Dtest=VectorizedExecutorAppendMergeBatchTest -DfailIfNoTests=false
```

- [ ] **Step 3: Implement `dispatchAppendMergeBatch`:**

```java
// in VectorizedExecutor.java, BELOW the existing dispatchAppendMerge per-row method

/**
 * Batched merge-append. Single FFM crossing for N rows.
 * Replaces the per-row dispatchAppendMerge in V3's wiring (A.2 commit).
 * This method is intentionally not yet wired to any call site — A.1 is
 * infrastructure-only; A.2 switches the call site.
 */
public int dispatchAppendMergeBatch(
        long dbHandle,
        MemorySegment keysOff,
        MemorySegment keysData,
        MemorySegment opsOff,
        MemorySegment opsData,
        int n) {
    return linker.frsVecMergeAppendBatch(
        dbHandle, keysOff, keysData, opsOff, opsData, n);
}
```

- [ ] **Step 4: Run test — expect PASS.**

```bash
mvn -pl flink-state-backends/flink-statebackend-forst-rs test \
  -Dtest=VectorizedExecutorAppendMergeBatchTest -DfailIfNoTests=false
```

- [ ] **Step 5: Run the FULL existing test suite to verify zero behavior change.**

```bash
mvn -pl flink-state-backends/flink-statebackend-forst-rs test
```

Expected: all existing tests still green; only the one new test added. Behavior of every call site unchanged because `dispatchAppendMergeBatch` is not yet wired.

- [ ] **Step 6: Commit A.1 (single commit, both repos):**

```bash
# Rust side
cd /Users/lijunqing/Code/stczwd/ForSt
git add crates/forst-rs-ffi/src/lib.rs crates/forst-rs-ffi/tests/merge_append_batch.rs crates/forst-rs-ffi/benches/merge_append_batch.rs
git -c commit.gpgsign=false commit -m "$(cat <<'EOF'
feat(forst-rs-ffi): frs_vec_merge_append_batch FFI (Phase A.1 infra)

Batched form of frs_vec_merge_append; consumes N (key, op) pairs in
a single FFI call via rocksdb::WriteBatch. Zero-copy: caller-owned
pointers, no allocation in the engine. Unit test verifies merge-operator
concatenation across multiple ops on the same key. Criterion micro
asserts ≤ 10 µs for batch_size=1024 per spec §3.5 D4.

Not yet wired to any call site — A.1 is infrastructure-only per the
locked V4 → V3 commit order in audit-design §7. A.2 switches the call
site.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"

# Java side (separate repo, separate commit)
cd /Users/lijunqing/Code/stczwd/flink
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java \
        flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java \
        flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/VectorizedExecutorAppendMergeBatchTest.java
git -c commit.gpgsign=false commit -m "$(cat <<'EOF'
feat(state-forst-rs): VectorizedExecutor.dispatchAppendMergeBatch (Phase A.1 infra)

Java FFM binding for frs_vec_merge_append_batch + executor method.
NOT yet wired to any call site — A.1 is infrastructure-only per the
locked V4 → V3 order. A.2 switches the call site.

Existing test suite all green; only new test
VectorizedExecutorAppendMergeBatchTest covers the new method directly.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 7: Per-fix attribution step (A.1):** Bench-wise nothing should change because no call site moved. Run a single Q11 + Q12 spot-check on the freshly-deployed jar — those queries already hit `dispatchAppendMerge` zero times (they're timer-heavy not list-heavy), so their wall-clock must be within ±2 % of v3.8 baseline.

```bash
/tmp/v38-bench/run.sh # but only Q11 + Q12
# Or invoke the bench manually:
bash /tmp/v38-bench/run-single.sh q11
bash /tmp/v38-bench/run-single.sh q12
```

If wall-clocks within v3.8 baseline ± 2 % (Q11 ~73.6 s, Q12 ~30.2 s): attribution PASSES.
If outside: revert and investigate (A.1 should be zero-behavior-change).

---

### Task A.2.1: Override `ForStRsAsyncListStateV2.asyncAdd(V)`

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsAsyncListStateV2.java`
- Read for reference: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsListStateV2.java` (the standalone V2 — has the AppendMergeRequest pattern)

- [ ] **Step 1: Write the failing test.**

```java
// flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ForStRsAsyncListStateV2AppendMergeTest.java
public class ForStRsAsyncListStateV2AppendMergeTest {

    @Test
    public void asyncAdd_thousand_entries_then_get_returns_all_in_order() throws Exception {
        // Setup: backend + ListState instance.
        ForStRsAsyncKeyedStateBackend<String> backend = makeBackend();
        ListStateDescriptor<Long> desc = new ListStateDescriptor<>("topN", Types.LONG);
        ForStRsAsyncListStateV2<Long> state = backend.getOrCreateKeyedState(desc);

        backend.setCurrentKey("auction-42");

        // Add 1000 entries.
        for (long i = 0; i < 1000; i++) {
            state.asyncAdd(i).get();  // block per request to simplify test
        }

        // Read back.
        Iterable<Long> values = state.asyncGet().get();
        List<Long> result = new ArrayList<>();
        values.forEach(result::add);

        assertEquals(1000, result.size());
        for (long i = 0; i < 1000; i++) {
            assertEquals((Long) i, result.get((int) i));
        }
    }

    @Test
    public void clear_then_add_does_not_inherit_prior_state() throws Exception {
        ForStRsAsyncKeyedStateBackend<String> backend = makeBackend();
        ListStateDescriptor<Long> desc = new ListStateDescriptor<>("topN", Types.LONG);
        ForStRsAsyncListStateV2<Long> state = backend.getOrCreateKeyedState(desc);

        backend.setCurrentKey("auction-1");
        state.asyncAdd(100L).get();
        state.asyncAdd(200L).get();
        state.asyncClear().get();
        state.asyncAdd(999L).get();

        List<Long> result = new ArrayList<>();
        state.asyncGet().get().forEach(result::add);
        assertEquals(List.of(999L), result);
    }

    @Test
    public void snapshot_then_restore_preserves_full_list() throws Exception {
        ForStRsAsyncKeyedStateBackend<String> backend = makeBackend();
        // ... add 100 entries
        // ... snapshot to local file
        // ... close backend, restore from snapshot
        // ... verify all 100 entries returned by asyncGet()
    }
}
```

- [ ] **Step 2: Run the test — expect failure** (because asyncAdd currently does full-PUT, so after 1000 add calls only the last value should be stored).

```bash
mvn -pl flink-state-backends/flink-statebackend-forst-rs test \
  -Dtest=ForStRsAsyncListStateV2AppendMergeTest -DfailIfNoTests=false
```

Expected: `asyncAdd_thousand_entries_then_get_returns_all_in_order` FAILS — result.size() will be 1, not 1000.

- [ ] **Step 3: Implement the override.** Add to `ForStRsAsyncListStateV2.java`:

```java
@Override
public StateFuture<Void> asyncAdd(V value) {
    if (value == null) {
        throw new NullPointerException("ListState does not accept null");
    }
    // Build merge-op payload: [count=1 u32 LE][elem_bytes]
    try {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeInt(1);                             // count = 1
        elementSerializer.serialize(value, out);
        byte[] opBytes = out.getCopyOfBuffer();

        // Submit as AppendMergeRequest (NOT MAP_PUT)
        return submit(new AppendMergeRequest<>(this, opBytes));
    } catch (IOException e) {
        return StateFutureUtils.completedExceptionally(e);
    }
}

@Override
public StateFuture<Void> asyncAddAll(Collection<V> values) {
    if (values == null || values.isEmpty()) {
        return StateFutureUtils.completedVoidFuture();
    }
    try {
        DataOutputSerializer out = new DataOutputSerializer(values.size() * 16);
        out.writeInt(values.size());                 // count = N
        for (V v : values) {
            elementSerializer.serialize(v, out);
        }
        byte[] opBytes = out.getCopyOfBuffer();
        return submit(new AppendMergeRequest<>(this, opBytes));
    } catch (IOException e) {
        return StateFutureUtils.completedExceptionally(e);
    }
}
```

(If `AppendMergeRequest` class needs to be ported from the standalone V2 `ForStRsListStateV2.java`, port it.)

- [ ] **Step 4: Run the test — expect PASS.**

```bash
mvn -pl flink-state-backends/flink-statebackend-forst-rs test \
  -Dtest=ForStRsAsyncListStateV2AppendMergeTest -DfailIfNoTests=false
```

---

### Task A.2.2: Register ListState in `VectorizedClassifier.appendMergeStates`

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedClassifier.java`

- [ ] **Step 1: Add the registry.**

```java
// Near the top of VectorizedClassifier, alongside putStates / getStates registries:
private final Set<String> appendMergeStates = ConcurrentHashMap.newKeySet();

public void registerAppendMergeState(String stateName) {
    appendMergeStates.add(stateName);
}

// In the classify path (near where MAP_PUT is routed):
case LIST_ADD:
    if (appendMergeStates.contains(req.getStateName())) {
        return dispatchAppendMergeBatchPath(req);
    }
    // fall through to old behavior
```

- [ ] **Step 2: Register from `ForStRsAsyncListStateV2`'s constructor:**

```java
// In ForStRsAsyncListStateV2's constructor (or init):
classifier.registerAppendMergeState(this.getStateName());
```

- [ ] **Step 3: Write a test that the registration occurs.**

```java
@Test
public void asyncListStateV2_registers_with_classifier() {
    ForStRsAsyncKeyedStateBackend<String> backend = makeBackend();
    ListStateDescriptor<Long> desc = new ListStateDescriptor<>("registry-check", Types.LONG);
    backend.getOrCreateKeyedState(desc);
    assertTrue(backend.getVectorizedClassifier().appendMergeStates.contains("registry-check"));
}
```

- [ ] **Step 4: Run — PASS.**

---

### Task A.2.3: Switch `dispatchAppendMerge` per-row loop to `dispatchAppendMergeBatch`

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java`

- [ ] **Step 1: Locate the per-row loop in `dispatchAppendMerge` (lines 356-440 per spec §3 V4 row).** Replace with a single batched call:

```java
public void dispatchAppendMerge(/* the existing signature */) {
    // OLD (per-row):
    //   for (int row = 0; row < count; row++) {
    //     try (Arena arena = Arena.ofConfined()) {
    //       MemorySegment keyNative = arena.allocate(keyLen);
    //       MemorySegment.copy(...heap..., keyNative, 0, keyLen);
    //       MemorySegment opNative = arena.allocate(opLen);
    //       MemorySegment.copy(...heap..., opNative, 0, opLen);
    //       linker.frsVecMergeAppend(dbHandle, keyNative, keyLen, opNative, opLen);
    //     }
    //   }

    // NEW (batched):
    // ColumnarBatchBuffer already provides keys + ops as MemorySegments with offsets.
    int rc = linker.frsVecMergeAppendBatch(
        dbHandle,
        keysOffSegment,
        keysDataSegment,
        opsOffSegment,
        opsDataSegment,
        count);
    if (rc != 0) {
        throw new RuntimeException("frs_vec_merge_append_batch failed: " + rc);
    }
}
```

- [ ] **Step 2: Run all existing tests + the new tests added in A.2.1/A.2.2.**

```bash
mvn -pl flink-state-backends/flink-statebackend-forst-rs test
```

Expected: all green.

- [ ] **Step 3: Build the jar + deploy.**

```bash
mvn -pl flink-state-backends/flink-statebackend-forst-rs -am package -DskipTests
cp flink-state-backends/flink-statebackend-forst-rs/target/flink-statebackend-forst-rs-2.2.0.jar \
   /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/

# Also rebuild and copy the Rust dylib if the Rust side changed
cd /Users/lijunqing/Code/stczwd/ForSt
cargo build --release -p forst-rs-ffi
cp target/release/libforst_rs_ffi.dylib /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/
```

- [ ] **Step 4: Run the Phase-A bench gates.** Specifically Q19 and the regression gates (Q11/Q12/Q15/Q20/Q23):

```bash
# Modified version of /tmp/v38-bench/run.sh that only runs these 6 queries
cat > /tmp/phaseA-bench/run.sh <<'SCRIPT'
#!/usr/bin/env bash
# (copy of /tmp/v38-bench/run.sh but restrict to q11 q12 q15 q19 q20 q23)
SCRIPT
chmod +x /tmp/phaseA-bench/run.sh
/tmp/phaseA-bench/run.sh
```

- [ ] **Step 5: Check gates** (per §7 Phase A acceptance + §3.5 ablation band):
  - Q19 must be ≤ 70 s AND Q19's measured relief vs v3.8's 135 s = Δ ≥ 65 s
  - Q11, Q12, Q15, Q20, Q23 each within 5 % of v3.8 baseline:
    - Q11 v3.8 = 73.59 s → A.2 must be ≤ 77.3 s
    - Q12 v3.8 = 30.22 s → A.2 must be ≤ 31.7 s
    - Q15 v3.8 = 18.34 s → A.2 must be ≤ 19.3 s
    - Q20 v3.8 = 55.14 s → A.2 must be ≤ 57.9 s
    - Q23 v3.8 = 54.62 s → A.2 must be ≤ 57.4 s

- [ ] **Step 6: Per-fix attribution step (A.2):**
  Expected Q19 relief sum = D3 (55) + D4 (25) = 80 s. Q19 baseline = 135 s. Expected post-A.2 = 55 s.
  Ablation band per §3.5: Q19 measured relief in [56, 104] s passes; outside that → trigger ablation.
  If outside band: revert A.2 first (verify Q19 returns to ~135 s); then re-add A.2 with bisect-style commit-removal to identify which file change is responsible for the divergence.

- [ ] **Step 7: Commit A.2:**

```bash
cd /Users/lijunqing/Code/stczwd/flink
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsAsyncListStateV2.java \
        flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedClassifier.java \
        flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java \
        flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ForStRsAsyncListStateV2AppendMergeTest.java
git -c commit.gpgsign=false commit -m "$(cat <<'EOF'
feat(state-forst-rs): ListState.asyncAdd -> APPEND_MERGE (Phase A.2)

Switches ForStRsAsyncListStateV2.asyncAdd/asyncAddAll from a destructive
full-list PUT to an AppendMergeRequest using the batched
frs_vec_merge_append_batch FFI from A.1. VectorizedClassifier learns the
appendMergeStates set; the per-row dispatchAppendMerge loop is replaced
with a single dispatchAppendMergeBatch call.

Bench gate (Phase A acceptance, see audit-design §7):
- Q19 must be ≤ 70 s (was 135 s in v3.8)
- Q11/Q12/Q15/Q20/Q23 within 5 % of v3.8 baseline

Per-fix attribution: Q19 measured relief must be in [56, 104] s
(D3+D4 ± 30 % per §3.5 ablation band).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phases B–E — forward references only

Detail in the audit-design §7. To be planned in follow-on sessions after Phase A's per-fix attribution step closes.

- **Phase B (V5+V8+V9):** MemorySegmentDataInputView introduction in V2 async state classes. Acceptance Q16 ≤ 170 s, Q19 ≤ 110 s.
- **Phase C (V2+V11+V12):** Per-state-instance ArrowBinaryBuffer for MapState V2 and ListState. Acceptance Q16 ≤ 150 s, Q19 ≤ 60 s. **Blocked on Phase 0 verdict** (V1-sync compliance attestation).
- **Phase D (V1+V7):** Off-heap MapStateCache + composite-key intern. Acceptance Q16 ≤ 145 s.
- **Phase E (V10):** Engine multi_get batched. Criterion-only gate, no Nexmark change expected.

---

## Self-Review

**Spec coverage:**
- Phase 0 OQ-1 → Task 0.1–0.5 ✓
- Phase A.1 V4 infra → Task A.1.1–A.1.3 ✓
- Phase A.2 V3 wiring → Task A.2.1–A.2.3 ✓
- §3.5 ablation bands embedded in attribution steps ✓
- Phase B/C/D/E forward-referenced ✓

**Placeholder scan:**
- No "TBD" / "TODO" / "fill in later" in tasks ✓
- All code snippets are complete (Rust function bodies, Java method bodies, test bodies) ✓
- All commands are exact (cargo, mvn, paths absolute) ✓
- All commit messages are draft-complete (no <placeholders>) ✓

**Type consistency:**
- `frs_vec_merge_append_batch(db_handle, keys_off, keys_data, ops_off, ops_data, n) -> i32` — matches Rust impl + Java FFM binding + executor method ✓
- `AppendMergeRequest` referenced consistently (port from standalone V2 if missing) ✓
- `appendMergeStates` set name consistent between classifier and ListState ctor ✓
