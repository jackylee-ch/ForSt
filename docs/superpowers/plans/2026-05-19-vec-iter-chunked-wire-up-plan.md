# Vectorized Iterator Chunked Wire-Up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the already-built `frs_vec_iter_prefix_next` chunked iterator API into `ForStRsDBIterRequest.process()`, replacing the 128 per-entry `linker.iteratorNext()` FFM calls with a single chunked FFM call per drain. Targets Q9 (884.76 s) and Q20 (892.09 s) regressions in v3.2.

**Architecture:** Two independent commits with separate benches per CONTRIBUTING.md "one variable per change" discipline. Commit A isolates the FFM call frequency win by replacing the loop with one chunked call; Commit B isolates the byte[] allocation win by introducing a slice-based view that defers byte[] materialization until deserialize time. Each commit's effect is measured separately so the empirical weights of "FFM call frequency" vs "byte[] allocation" can be added to the 853 ns predicted-vs-measured table in `2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md` §5 Diag-B.

**Tech Stack:** Java 25 FFM, Rust forst-rs-ffi, criterion benches, Nexmark wall-clock. Commits only touch one repo at a time — Commit A is Java-only (`~/Code/stczwd/flink`), Commit B is Java-only too (introduces a new Java type).

**Spec:** `docs/superpowers/specs/2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md` violation #1.
**Discipline:** `CONTRIBUTING.md` perf-work section.

---

## File structure

**Commit A — Java files modified:**
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsDBIterRequest.java` — replace `process()` loop with chunked drain helper; add `existingVecHandle` field for continuation
- `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ForStRsDBIterRequestTest.java` — new unit test asserting one FFM call per drain (via mock linker)

**Commit B — Java files modified:**
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsDBIterRequest.java` — replace per-entry IteratorEntry byte[] alloc with `IteratorEntryView` slice-based decode at completion time
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/IteratorEntryView.java` — NEW record carrying (chunkBuf, keyOff, keyLen, valOff, valLen)
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsIterableState.java` — add `deserializeUserKey(IteratorEntryView)` / `deserializeUserValue(IteratorEntryView)` default methods that delegate to byte[] versions for backwards compat (state classes can override for zero-copy)
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java` — override the new IteratorEntryView-based deserialize methods to read directly from MemorySegment slice via DataInputDeserializer

---

## Task 1: Commit A — chunked drain helper in ForStRsDBIterRequest.process()

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsDBIterRequest.java:144-160`
- Test: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ForStRsDBIterRequestTest.java` (new file)

### - [ ] Step 1: Write the failing test

Create the test file with mock-linker assertions for the chunked drain behavior:

```java
package org.apache.flink.state.forstrs;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ForStRsDBIterRequestTest {

    @Test
    void process_uses_one_chunked_call_not_per_entry_loop() {
        ForStRsLinker linker = mock(ForStRsLinker.class);
        FrsDb db = mock(FrsDb.class);
        FrsCfHandle cf = mock(FrsCfHandle.class);

        // Arrange: chunked call returns 64 rows in one call, then 0 (exhausted).
        when(linker.frsVecIterPrefixOpen(any(), any(), any(), anyInt(), any(), anyInt(), any(), any(), any()))
                .thenReturn(0);  // ok
        when(linker.frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any()))
                .thenReturn(0);  // ok

        try (Arena arena = Arena.ofConfined()) {
            byte[] prefix = "k/test/".getBytes();
            // Construct a minimal MAP_ITER request inline — no test-fixture helper needed.
            // ForStRsIterableState is a mocked interface; the request's process() only consults
            // it during completeWithEntries, which we tolerate as a downstream mock.
            ForStRsIterableState<?, ?, ?, ?> mockState = mock(ForStRsIterableState.class);
            org.apache.flink.runtime.asyncprocessing.StateRequest<?, ?, ?, ?> sr =
                    mock(org.apache.flink.runtime.asyncprocessing.StateRequest.class);
            when(sr.getFuture()).thenReturn(
                    mock(org.apache.flink.core.asyncprocessing.InternalAsyncFuture.class));
            ForStRsDBIterRequest<?, ?, ?, ?> req = new ForStRsDBIterRequest<>(
                    prefix,
                    sr,
                    org.apache.flink.runtime.asyncprocessing.StateRequestType.MAP_ITER,
                    mockState,
                    null);  // no existing legacy iterator
            req.process(linker, db, cf, arena);
        }

        // Assert: ONE chunked call, ZERO legacy per-entry iteratorNext calls.
        verify(linker, times(1)).frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any());
        verify(linker, never()).iteratorNext(any());
    }
}
```

### - [ ] Step 2: Run test to verify it fails

Run: `cd ~/Code/stczwd/flink && mvn -pl flink-state-backends/flink-statebackend-forst-rs test -Dtest=ForStRsDBIterRequestTest`

Expected: FAIL — `linker.iteratorNext()` is invoked 128 times (current `process()` impl); `frsVecIterPrefixNext` is invoked 0 times.

### - [ ] Step 3: Add `existingVecHandle` field + getter to ForStRsDBIterRequest

Modify `ForStRsDBIterRequest.java` field block (around line 58):

```java
@Nullable private FrsIterator existingIterator;
/** Non-zero if continuation uses the vectorized iter path (frs_vec_iter_prefix_*). */
private long existingVecHandle = 0L;
private String stateName = "unknown";
```

And add getter (near line 103 `hasExistingIterator()`):

```java
public boolean hasExistingIterator() {
    return existingIterator != null || existingVecHandle != 0L;
}

public long getExistingVecHandle() {
    return existingVecHandle;
}

public void setExistingVecHandle(long handle) {
    this.existingVecHandle = handle;
}
```

### - [ ] Step 4: Replace the per-entry loop in process() with chunked drain

Replace `ForStRsDBIterRequest.java:128-161` (the entire `process()` method) with:

```java
public void process(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
    if (originalRequestType == StateRequestType.MAP_IS_EMPTY) {
        // Open + one chunk (cap=8 KB is plenty for at-most-one row decode) + close.
        long handle = openVecIter(linker, db, cf, prefix, arena);
        MemorySegment chunkBuf = arena.allocate(8 * 1024);
        MemorySegment outRc = arena.allocate(ValueLayout.JAVA_INT);
        MemorySegment outBu = arena.allocate(ValueLayout.JAVA_INT);
        int rc = linker.frsVecIterPrefixNext(handle, chunkBuf, (int) chunkBuf.byteSize(), outRc, outBu);
        if (rc != 0) {
            linker.frsVecIterPrefixClose(handle);
            throw new RuntimeException("frs_vec_iter_prefix_next rc=" + rc);
        }
        int rowCount = outRc.get(ValueLayout.JAVA_INT, 0);
        linker.frsVecIterPrefixClose(handle);
        boolean isEmpty = (rowCount == 0);
        ((InternalAsyncFuture<Boolean>) (InternalAsyncFuture<?>) request.getFuture())
                .complete(isEmpty);
        return;
    }

    // Open or continue via vec iter handle.
    long handle = existingVecHandle;
    if (handle == 0L) {
        handle = openVecIter(linker, db, cf, prefix, arena);
    }

    // Single chunked drain. Buffer sized for ~128 entries × avg row (~80 B) + 8 B header per row.
    MemorySegment chunkBuf = arena.allocate(64 * 1024);
    MemorySegment outRc = arena.allocate(ValueLayout.JAVA_INT);
    MemorySegment outBu = arena.allocate(ValueLayout.JAVA_INT);
    int rc = linker.frsVecIterPrefixNext(handle, chunkBuf, (int) chunkBuf.byteSize(), outRc, outBu);
    if (rc != 0) {
        linker.frsVecIterPrefixClose(handle);
        throw new RuntimeException("frs_vec_iter_prefix_next rc=" + rc);
    }
    int rowCount = outRc.get(ValueLayout.JAVA_INT, 0);

    // Parse chunk buffer into IteratorEntry[]. Layout per write_chunk_into_buf:
    //   for each row: [u32 keyLenLE][u32 valLenLE][keyBytes][valBytes]
    ForStRsLinker.IteratorEntry[] entries = new ForStRsLinker.IteratorEntry[rowCount];
    int off = 0;
    for (int i = 0; i < rowCount; i++) {
        int klen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
        off += 4;
        int vlen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
        off += 4;
        byte[] keyCopy = new byte[klen];
        MemorySegment.copy(chunkBuf, ValueLayout.JAVA_BYTE, off, keyCopy, 0, klen);
        off += klen;
        byte[] valCopy = new byte[vlen];
        MemorySegment.copy(chunkBuf, ValueLayout.JAVA_BYTE, off, valCopy, 0, vlen);
        off += vlen;
        entries[i] = new ForStRsLinker.IteratorEntry(keyCopy, valCopy);
    }

    boolean encounterEnd = (rowCount < CACHE_SIZE_LIMIT);
    if (encounterEnd) {
        linker.frsVecIterPrefixClose(handle);
        existingVecHandle = 0L;
    } else {
        existingVecHandle = handle;
    }

    completeWithEntries(entries, encounterEnd, /* continuationIter (legacy) = */ null);
}

private long openVecIter(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, byte[] prefix, Arena arena) {
    MemorySegment prefixSeg = arena.allocate(prefix.length);
    MemorySegment.copy(prefix, 0, prefixSeg, ValueLayout.JAVA_BYTE, 0, prefix.length);
    MemorySegment outHandle = arena.allocate(ValueLayout.JAVA_LONG);
    MemorySegment outRc = arena.allocate(ValueLayout.JAVA_INT);
    MemorySegment outBu = arena.allocate(ValueLayout.JAVA_INT);
    // chunkBuf=0/null on open (first chunk requested via next, not embedded in open call).
    // Some impls inline the first chunk in open; here we keep the calls separate for clarity.
    int rc = linker.frsVecIterPrefixOpen(
            db, cf, prefixSeg, prefix.length,
            /* chunkBuf */ MemorySegment.NULL, /* cap */ 0,
            outHandle, outRc, outBu);
    if (rc != 0) {
        throw new RuntimeException("frs_vec_iter_prefix_open rc=" + rc);
    }
    return outHandle.get(ValueLayout.JAVA_LONG, 0);
}
```

Also add the needed imports at the top of the file:

```java
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
```

Note: the `completeWithEntries` second argument `continuationIter (legacy) = null` is intentional — Commit A drops the legacy `FrsIterator` continuation path. ForStRsMapIterator's `continuationIter` parameter remains for backwards compat but is unused on the vec path. The continuation is held in `existingVecHandle` on `this` instead. The ForStRsMapIterator will need to know about this; see Task 1, Step 5.

### - [ ] Step 5: Update ForStRsMapIterator to use vec-handle continuation

Currently `ForStRsMapIterator` holds a `FrsIterator` for continuation and calls `iterableState.getStateRequestHandler()` to schedule the next batch. The new continuation handle is held on the `ForStRsDBIterRequest` instance, so the iterator needs to carry a reference to the request (or to the vec handle directly) when issuing the continuation.

Open `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsMapIterator.java`. Find the field `private final FrsIterator continuationIter;` and the constructor that takes it. Add:

```java
/** Vec-iter continuation handle (0L if not present). */
private final long continuationVecHandle;
```

Update the constructor signature to accept both (the FrsIterator stays for `MAP_IS_EMPTY` legacy path nobody uses but keep for compat):

```java
public ForStRsMapIterator(
        State asState,
        StateRequestType requestType,
        StateRequestHandler handler,
        Collection<T> cache,
        boolean encounterEnd,
        @Nullable FrsIterator continuationIter,
        long continuationVecHandle) {
    // ... existing assignments ...
    this.continuationVecHandle = continuationVecHandle;
}
```

In `ForStRsDBIterRequest.completeWithEntries`, pass the vec handle:

```java
ForStRsMapIterator<Map.Entry<UK, UV>> entryIter =
        new ForStRsMapIterator<>(
                iterableState.asState(),
                StateRequestType.MAP_ITER,
                handler,
                mapEntries,
                encounterEnd,
                continuationIter,  // legacy null on vec path
                encounterEnd ? 0L : existingVecHandle);
```

Find the place in `ForStRsMapIterator` that constructs the continuation request — it builds a `ForStRsDBIterRequest` with `existingIterator` set. Change it to set `existingVecHandle` instead:

```java
ForStRsDBIterRequest<?, ?, ?, ?> continuationReq =
        new ForStRsDBIterRequest<>(
                /* prefix     */ null,  // continuation doesn't need prefix; uses existing handle
                /* request    */ newStateRequest,
                /* requestType*/ requestType,
                /* iterableSt */ iterableState,
                /* existingIter */ null);
continuationReq.setExistingVecHandle(continuationVecHandle);
```

### - [ ] Step 6: Run unit test to verify it passes

Run: `cd ~/Code/stczwd/flink && mvn -pl flink-state-backends/flink-statebackend-forst-rs test -Dtest=ForStRsDBIterRequestTest`

Expected: PASS — the assertion `verify(linker, times(1)).frsVecIterPrefixNext(...)` succeeds and `verify(linker, never()).iteratorNext(...)` succeeds.

### - [ ] Step 7: Run the full module test suite

Run: `cd ~/Code/stczwd/flink && mvn -pl flink-state-backends/flink-statebackend-forst-rs test`

Expected: all existing tests pass. Particularly check the `ForStRsKeyedStateBackendTest` and any test that exercises MapState iteration paths. If any test failure is in code that relied on `existingIterator` for continuation, fix the test to pass `existingVecHandle` instead, OR keep the dual-mode hasExistingIterator() that supports both. Do not delete the `FrsIterator existingIterator` field — keep it for backwards compat in case the iterator-watchdog code path needs it.

### - [ ] Step 8: Commit A — wire-up only

```bash
cd ~/Code/stczwd/flink
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsDBIterRequest.java \
        flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsMapIterator.java \
        flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ForStRsDBIterRequestTest.java
git commit -m "perf(forst-rs): wire chunked frs_vec_iter_prefix_next into ForStRsDBIterRequest

Replaces the 128 per-entry linker.iteratorNext() FFM calls in
ForStRsDBIterRequest.process() with a single linker.frsVecIterPrefixNext()
chunked call. Closes violation #1 from
docs/superpowers/specs/2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md.

Per-entry IteratorEntry byte[] copy semantics PRESERVED — Commit B will
introduce the slice-based view to isolate the allocation cost.

(Bench numbers, dylib SHA-256, criterion variance bounds populated by
Task 2 immediately after this commit.)
"
```

(Bench data table appended in the bench task; the commit message above is the wire-up commit. The amended commit after benching includes the table per CONTRIBUTING.md.)

---

## Task 2: Bench Commit A — measure FFM call frequency win

**Files:**
- Read: `docs/superpowers/specs/2026-05-18-forst-rs-benchmark-report-v3.2.md` for the v3.2 baseline numbers per query
- No code edits

### - [ ] Step 1: Verify the deployed dylib matches the bench source

Run:
```bash
shasum -a 256 /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/libforst_rs_ffi.dylib
```
Expected: `535dd7c43ed6a7e807849d83ef3b7bed0d3a4b753edad3a10ea227e0f188bfd8` (the baseline dylib SHA from commit `ed6ce8d09`). If different, redeploy:
```bash
cd ~/Code/stczwd/ForSt && cargo build -p forst-rs-ffi --release
cp target/release/libforst_rs_ffi.dylib /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/libforst_rs_ffi.dylib
shasum -a 256 /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/libforst_rs_ffi.dylib
```

### - [ ] Step 2: Rebuild + redeploy the forst-rs-jdk25 backend JAR

Run:
```bash
cd ~/Code/stczwd/flink && mvn -pl flink-state-backends/flink-statebackend-forst-rs -DskipTests package
cp flink-state-backends/flink-statebackend-forst-rs/target/flink-statebackend-forst-rs-2.2.0.jar \
   /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/flink-statebackend-forst-rs-2.2.0.jar
```
Expected: build success; updated jar in flink lib dir.

### - [ ] Step 3: Run Q9 (iter-heavy regressor, primary target)

Run:
```bash
cd /Users/lijunqing/Downloads/workenv/flink-2.2.1
pkill -9 -f 'StandaloneSession|TaskManagerRunner|SqlGateway|java.*Benchmark' 2>&1; sleep 5
rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache 2>&1
bash bin/run-nexmark-matrix.sh forst-rs-g1 q9 2>&1 | tee /tmp/v3.2-fix-commit-a-q9.log
```
Expected: a `Summary Average: EventsNum=100,000,000, Cores=0, Time=<X> s, ...` line. Record X.

### - [ ] Step 4: Run Q20 (iter-heavy regressor, secondary)

Run:
```bash
cd /Users/lijunqing/Downloads/workenv/flink-2.2.1
pkill -9 -f 'StandaloneSession|TaskManagerRunner|SqlGateway|java.*Benchmark' 2>&1; sleep 5
rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache 2>&1
bash bin/run-nexmark-matrix.sh forst-rs-g1 q20 2>&1 | tee /tmp/v3.2-fix-commit-a-q20.log
```
Expected: Summary line with new Q20 time.

### - [ ] Step 5: Run wins-protection suite Q5, Q15, Q18

Run each in isolation (cluster restart between, per `forst-rs-g1` driver behavior):
```bash
cd /Users/lijunqing/Downloads/workenv/flink-2.2.1
for q in q5 q15 q18; do
    pkill -9 -f 'StandaloneSession|TaskManagerRunner|SqlGateway|java.*Benchmark' 2>&1; sleep 5
    rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache 2>&1
    bash bin/run-nexmark-matrix.sh forst-rs-g1 $q 2>&1 | tee /tmp/v3.2-fix-commit-a-$q.log
done
```
Expected: three Summary lines (one per query).

### - [ ] Step 6: Compute deltas vs v3.2 baseline

v3.2 baseline numbers from `docs/superpowers/specs/2026-05-18-forst-rs-benchmark-report-v3.2.md`:
- Q9:  884.76 s
- Q15: 111.86 s
- Q18: 74.41 s
- Q20: 892.09 s
- Q5:  32.45 s

Build a table:

| Query | v3.2 baseline | Commit A | Delta | Verdict |
|---|---:|---:|---:|---|
| Q9  | 884.76 s | <new> | <%> | (target: >= +5% improvement) |
| Q20 | 892.09 s | <new> | <%> | (target: >= +5% improvement) |
| Q5  | 32.45 s  | <new> | <%> | (target: no regression below 90% of 32.45 → no slower than 36.06) |
| Q15 | 111.86 s | <new> | <%> | (target: no regression below 90% of 111.86 → no slower than 124.29) |
| Q18 | 74.41 s  | <new> | <%> | (target: no regression below 90% of 74.41 → no slower than 82.68) |

### - [ ] Step 7: Decide ship or revert

Per CONTRIBUTING.md "Revert on regression":
- If Q9 OR Q20 improvement < 5% (no measurable change): **REVERT Commit A**. The chunked path didn't help — investigate why (perhaps Flink isn't taking this code path in Q9/Q20; check via async-profiler).
- If any of Q5/Q15/Q18 regresses below 90% of v3.2: **REVERT Commit A**. The chunked path broke a state-heavy win.
- Otherwise: **AMEND Commit A's message** with the bench table, then proceed to Task 3.

For "amend with bench table":
```bash
cd ~/Code/stczwd/flink
git commit --amend -m "perf(forst-rs): wire chunked frs_vec_iter_prefix_next into ForStRsDBIterRequest

Replaces the 128 per-entry linker.iteratorNext() FFM calls in
ForStRsDBIterRequest.process() with a single linker.frsVecIterPrefixNext()
chunked call. Closes violation #1 from
docs/superpowers/specs/2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md.

Per-entry IteratorEntry byte[] copy semantics PRESERVED — Commit B
will introduce the slice-based view to isolate the allocation cost.

Benchmark results (criterion-style: median across 1 run, ±~10 % thermal
variance estimate per CONTRIBUTING.md):

  | Query | v3.2 baseline | Commit A   | Delta    | Verdict |
  |-------|--------------:|-----------:|---------:|---------|
  | Q9    | 884.76 s      | <new> s    | <%>      | win     |
  | Q20   | 892.09 s      | <new> s    | <%>      | win     |
  | Q5    | 32.45 s       | <new> s    | <%>      | noise   |
  | Q15   | 111.86 s      | <new> s    | <%>      | noise   |
  | Q18   | 74.41 s       | <new> s    | <%>      | noise   |

Rebuild: cd ~/Code/stczwd/flink && mvn -pl flink-state-backends/flink-statebackend-forst-rs -DskipTests package
Deploy: cp .../flink-statebackend-forst-rs-2.2.0.jar /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/
Dylib SHA-256 (unchanged from baseline): 535dd7c43ed6a7e807849d83ef3b7bed0d3a4b753edad3a10ea227e0f188bfd8

Closes violation #1.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

(Replace `<new>`, `<%>`, `<verdict>` with measured values.)

---

## Task 3: Commit B — IteratorEntryView slice-based decoder

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/IteratorEntryView.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsIterableState.java` — add default `deserializeUserKey(IteratorEntryView)` and `deserializeUserValue(IteratorEntryView)` methods
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java` — override the new IteratorEntryView methods for zero-copy decode
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsDBIterRequest.java:174,197,216` — use IteratorEntryView instead of IteratorEntry; defer byte[] materialization to deserialize time

### - [ ] Step 1: Write the failing test (zero allocations on the iter path)

Add to `ForStRsDBIterRequestTest.java`:

```java
@Test
void process_does_not_allocate_per_entry_byte_arrays() throws Exception {
    // Use a Mockito spy or a custom linker stub that records byte[] allocations.
    // Approach: instrument completeWithEntries to count IteratorEntry vs IteratorEntryView usage.
    // Acceptance: ZERO IteratorEntry constructions on the iter drain path; all decode
    // goes through IteratorEntryView -> deserializer reading from MemorySegment slice.
    //
    // Concrete check: when CACHE_SIZE_LIMIT (128) entries are drained, the per-iter heap
    // delta for byte[] objects (via java.lang.management) is exactly:
    //   = sizeof(byte[CACHE_SIZE_LIMIT]) reference array     (the entries[] holding views)
    //   + N × sizeof(MemorySegment slice view object)
    //   + (decoded UK + UV instance bytes from TypeSerializer)
    // and crucially does NOT include 2*128 = 256 byte[] copies of key/value bytes.
    //
    // Use a sun.management.ThreadMXBean.getThreadAllocatedBytes() before/after process(),
    // assert the delta is <= 16 KB (room for view objects + decoded instances on a
    // typical Q12-style payload of 32-byte keys + 16-byte values), vs the old path's
    // ~96 KB (256 byte[] × ~300 B avg with header padding).

    ForStRsLinker linker = mock(ForStRsLinker.class);
    // ... arrange a 128-row chunked drain ...

    com.sun.management.ThreadMXBean tmb = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
    long allocBefore = tmb.getThreadAllocatedBytes(Thread.currentThread().getId());

    try (Arena arena = Arena.ofConfined()) {
        byte[] prefix = "k/test/".getBytes();
        ForStRsDBIterRequest<?, ?, ?, ?> req = TestFixtures.iterRequestForMapIter(prefix);
        req.process(linker, mock(FrsDb.class), mock(FrsCfHandle.class), arena);
    }

    long allocAfter = tmb.getThreadAllocatedBytes(Thread.currentThread().getId());
    long delta = allocAfter - allocBefore;

    // Old path: 256 byte[] (~96 KB). New path: only view objects + decoded instances (~16 KB).
    assertThat(delta).isLessThan(32_000);  // 32 KB upper bound (generous)
}
```

### - [ ] Step 2: Run test to verify it fails

Run: `cd ~/Code/stczwd/flink && mvn -pl flink-state-backends/flink-statebackend-forst-rs test -Dtest=ForStRsDBIterRequestTest#process_does_not_allocate_per_entry_byte_arrays`

Expected: FAIL — Commit A's path allocates byte[] per entry. Delta > 32 KB.

### - [ ] Step 3: Create IteratorEntryView record

Create `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/IteratorEntryView.java`:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.flink.state.forstrs;

import org.apache.flink.annotation.Internal;

import java.lang.foreign.MemorySegment;

/**
 * Zero-copy view into a chunked iterator's MemorySegment buffer. References the (key, value)
 * byte ranges by offset+length rather than materializing them as separate {@code byte[]}
 * copies. The deserialization path reads directly from the segment slice via {@code
 * DataInputDeserializer.setBuffer(MemorySegment, off, len)}-style adapters.
 *
 * <p>Lifetime: the underlying {@code chunkBuf} MUST remain accessible until the view is
 * decoded (typically the {@code Arena} that owns {@code chunkBuf} is closed at turn boundary,
 * which happens AFTER {@code completeWithEntries} populates the {@code ForStRsMapIterator}
 * cache with decoded instances).
 */
@Internal
public record IteratorEntryView(
        MemorySegment chunkBuf,
        int keyOffset,
        int keyLength,
        int valueOffset,
        int valueLength) {

    /** Returns true if this view's value-range has length zero (a tombstone or missing value). */
    public boolean isValueEmpty() {
        return valueLength == 0;
    }

    /**
     * Materializes the key bytes as a heap {@code byte[]}. Use only when the legacy {@code
     * byte[]}-based deserialize path is the only option (e.g., a state class that doesn't
     * override the IteratorEntryView decoder). Prefer zero-copy decoders that read directly
     * from the segment slice.
     */
    public byte[] keyBytes() {
        byte[] out = new byte[keyLength];
        MemorySegment.copy(chunkBuf, java.lang.foreign.ValueLayout.JAVA_BYTE, keyOffset, out, 0, keyLength);
        return out;
    }

    /** Same as {@link #keyBytes()} but for the value range. */
    public byte[] valueBytes() {
        if (valueLength == 0) return null;
        byte[] out = new byte[valueLength];
        MemorySegment.copy(chunkBuf, java.lang.foreign.ValueLayout.JAVA_BYTE, valueOffset, out, 0, valueLength);
        return out;
    }
}
```

### - [ ] Step 4: Add IteratorEntryView default methods to ForStRsIterableState

Edit `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsIterableState.java`. Add:

```java
/**
 * Decode the user key from a chunked-iterator view. Default impl materializes byte[] and
 * delegates to {@link #deserializeUserKey(byte[], int)} for backwards compat. State classes
 * SHOULD override this to read directly from the segment slice via a MemorySegment-backed
 * {@code DataInputDeserializer}.
 *
 * @param view the chunk view
 * @param userKeyPrefixOffset bytes to skip at the start of the key (composite-key prefix length)
 */
default UK deserializeUserKey(IteratorEntryView view, int userKeyPrefixOffset) {
    return deserializeUserKey(view.keyBytes(), userKeyPrefixOffset);
}

/** Decode the user value from a chunked-iterator view. Same backwards-compat pattern. */
default UV deserializeUserValue(IteratorEntryView view) {
    return deserializeUserValue(view.valueBytes());
}
```

### - [ ] Step 5: Override the IteratorEntryView decoders in ForStRsMapStateV2

Edit `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java`. Add (near the existing `deserializeUserKey(byte[], int)` method):

```java
@Override
@SuppressWarnings("unchecked")
public UK deserializeUserKey(IteratorEntryView view, int userKeyPrefixOffset) {
    try {
        // Pull just the user-key range (skipping the composite-key prefix) into the input.
        // For now use a per-call byte[] for the slice — DataInputDeserializer doesn't accept
        // MemorySegment directly. Future optimization: write a MemorySegment-backed
        // DataInputView and skip this copy entirely.
        int rangeLen = view.keyLength() - userKeyPrefixOffset;
        byte[] buf = new byte[rangeLen];
        MemorySegment.copy(
                view.chunkBuf(),
                java.lang.foreign.ValueLayout.JAVA_BYTE,
                view.keyOffset() + userKeyPrefixOffset,
                buf,
                0,
                rangeLen);
        DataInputDeserializer in = new DataInputDeserializer(buf, 0, rangeLen);
        return userKeySerializer.deserialize(in);
    } catch (IOException e) {
        throw new RuntimeException("Failed to deserialize user key from view", e);
    }
}

@Override
@SuppressWarnings("unchecked")
public UV deserializeUserValue(IteratorEntryView view) {
    if (view.isValueEmpty()) return null;
    try {
        byte[] buf = new byte[view.valueLength()];
        MemorySegment.copy(
                view.chunkBuf(),
                java.lang.foreign.ValueLayout.JAVA_BYTE,
                view.valueOffset(),
                buf,
                0,
                view.valueLength());
        DataInputDeserializer in = new DataInputDeserializer(buf, 0, view.valueLength());
        return (UV) userValueSerializer.deserialize(in);
    } catch (IOException e) {
        throw new RuntimeException("Failed to deserialize user value from view", e);
    }
}
```

**NOTE — this Commit B is "scaffolding-only"** on the allocation reduction. The override above still allocates ONE `byte[]` per decode (the `buf` to pass to `DataInputDeserializer`). True zero-copy requires a `MemorySegmentInputView` that reads `DataInputView` bytes directly from the segment — implementable but extends beyond Commit B's scope. **This intentional limitation is what makes Commit B's measurement meaningful: it isolates the "outer byte[] copy" cost (the per-iter-entry alloc in the old `iteratorNext` path) from the "per-call alloc inside deserialize" cost (still present here).** If Commit B doesn't measurably help, the conclusion is that the inner-deserialize alloc is the dominant remainder — pointing to the next fix.

### - [ ] Step 6: Switch ForStRsDBIterRequest decode path to use IteratorEntryView

Replace the chunk-parse block in `process()` (the loop populating `IteratorEntry[]`) with:

```java
IteratorEntryView[] views = new IteratorEntryView[rowCount];
int off = 0;
for (int i = 0; i < rowCount; i++) {
    int klen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
    off += 4;
    int vlen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
    off += 4;
    int keyOff = off;
    off += klen;
    int valOff = off;
    off += vlen;
    views[i] = new IteratorEntryView(chunkBuf, keyOff, klen, valOff, vlen);
}
```

Then update `completeWithEntries` signature + body to take `IteratorEntryView[] views` instead of `IteratorEntry[] entries`. In each `case MAP_ITER/MAP_ITER_KEY/MAP_ITER_VALUE`, replace `iterableState.deserializeUserKey(e.key(), prefixLen)` with `iterableState.deserializeUserKey(views[i], prefixLen)` and similarly for the value.

### - [ ] Step 7: Run unit test (allocation delta check)

Run: `cd ~/Code/stczwd/flink && mvn -pl flink-state-backends/flink-statebackend-forst-rs test -Dtest=ForStRsDBIterRequestTest`

Expected: PASS — both the FFM-call-count test (Commit A) and the allocation-delta test (Commit B) succeed.

### - [ ] Step 8: Run full module tests

Run: `cd ~/Code/stczwd/flink && mvn -pl flink-state-backends/flink-statebackend-forst-rs test`

Expected: all pass.

### - [ ] Step 9: Commit B

```bash
cd ~/Code/stczwd/flink
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/IteratorEntryView.java \
        flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsIterableState.java \
        flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsDBIterRequest.java \
        flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java \
        flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ForStRsDBIterRequestTest.java

git commit -m "perf(forst-rs): IteratorEntryView slice-based decode (Commit B)

Eliminates the per-iter-entry IteratorEntry byte[] copy in ForStRsDBIterRequest
by introducing IteratorEntryView that references (key, value) ranges in the
chunkBuf MemorySegment by offset+length. Decode reads from the segment slice
at deserialize time, not at iteration time.

ForStRsMapStateV2 overrides the new IteratorEntryView-based decoders.
Backwards-compat: state classes that don't override fall back to the byte[]
path via the default methods in ForStRsIterableState (which materializes
byte[] via view.keyBytes() / view.valueBytes()).

Outer per-entry byte[] copy: ELIMINATED (was 2 × CACHE_SIZE_LIMIT × ~120 B = ~30 KB
per drain in Commit A path). Inner deserialize byte[]: STILL PRESENT (DataInputDeserializer
takes byte[]). True zero-copy needs a MemorySegmentInputView; deferred to V1.2.

(Bench numbers populated by Task 4.)
"
```

---

## Task 4: Bench Commit B — measure byte[] allocation win

**Files:**
- No edits

### - [ ] Step 1: Verify dylib unchanged from Task 2

Same SHA-256 expected.

### - [ ] Step 2: Rebuild + redeploy backend JAR

Same as Task 2 Step 2.

### - [ ] Step 3-5: Re-run Q9, Q20, Q5, Q15, Q18

Same script structure as Task 2 Steps 3-5. Log to `/tmp/v3.2-fix-commit-b-{q}.log`.

### - [ ] Step 6: Compute deltas vs Commit A

Build the table comparing Commit B vs Commit A (NOT vs v3.2 — we want to isolate the B effect):

| Query | Commit A | Commit B | Delta (vs A) | Cumulative vs v3.2 |
|---|---:|---:|---:|---:|
| Q9  | <A> | <B> | <%> | <%> |
| Q20 | <A> | <B> | <%> | <%> |
| Q5  | <A> | <B> | <%> | <%> |
| Q15 | <A> | <B> | <%> | <%> |
| Q18 | <A> | <B> | <%> | <%> |

### - [ ] Step 7: Decide ship Commit B or revert (keep Commit A only)

Per CONTRIBUTING.md "Revert on regression":
- If any of Q5/Q15/Q18 regresses below 90% of v3.2 (cumulative A+B drops a state-heavy win): **REVERT Commit B**. Keep Commit A.
- If Q9 or Q20 doesn't show further improvement on Commit B (vs Commit A): document as "Commit B did not measurably reduce per-entry byte[] overhead — likely because inner DataInputDeserializer alloc is the dominant remainder. Defer to V1.2 MemorySegmentInputView work item." Decision call: ship Commit B anyway if it doesn't regress (the architecture change positions for future zero-copy work) OR revert if no improvement is shown (avoid speculative code).
- Otherwise: ship and amend Commit B's message with the table.

### - [ ] Step 8: Update Diag-B predicted-vs-measured table in deep-analysis doc

Open `docs/superpowers/specs/2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md` §5 Diag-B and fill in measured values for two components based on Commit A and Commit B numbers:

```
| Component | Predicted | Measured (Commit A + B) | Delta |
|---|---|---|---|
| FFM call frequency (per-entry iteratorNext) | 50-100 ns × N | Q9 delta from Commit A: <measured> | <delta> |
| byte[] alloc + copy per iter entry | 30 ns × N | Q9 delta from Commit B: <measured> | <delta> |
```

Commit:

```bash
git add docs/superpowers/specs/2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md
git commit -m "docs: Diag-B measured values from Commit A + B benches"
```

---

## Task 5: Update audit doc with violation #1 fix status

**Files:**
- Modify: `docs/superpowers/specs/2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md`

### - [ ] Step 1: Mark violation #1 as FIXED

In the audit doc's Violation #1 section, add at the end:

```markdown
**Status: FIXED in Commit A (`<commit-A-sha>`) + Commit B (`<commit-B-sha>`).**
Q9 wall-clock: 884.76 s → <new> s = <ratio>× rocksdb (was 0.59×).
Q20 wall-clock: 892.09 s → <new> s = <ratio>× rocksdb (was 0.49×).
```

### - [ ] Step 2: Commit doc update

```bash
git add docs/superpowers/specs/2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md
git commit -m "docs: violation #1 fix landed — Q9/Q20 status updated"
```

---

## Notes

- **Per CONTRIBUTING.md:** every commit message above is the template; substitute measured values before each commit. The dylib SHA-256 and bench table are mandatory per the perf-discipline rules.
- **Total scope:** ~5 files modified, ~2 files created, ~200 lines of Java. No Rust changes. No FFM API changes. No Flink-runtime changes.
- **Risk:** medium. The continuation handle migration (FrsIterator → existingVecHandle) touches the iterator-watchdog path; the existing `IterLifetimeWatchdog` was designed for the Path B (FrsIterHandle) lifecycle. Verify watchdog tests still pass; if not, leave the watchdog wiring for V1.2 and accept that Commit A's iterators don't have watchdog protection (acceptable — they're scoped to a single async-state turn, much shorter than the watchdog timeout).
