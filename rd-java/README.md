# rd-java — backend hot-path alloc/copy fixes (audit V1–V4), delivered as edited COPIES

**Spec:** `docs/superpowers/specs/2026-06-12-backend-hotpath-alloc-copy-audit.md`
**Source of the copies:** `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs`
(read-only for this session — the flink repo is owned by another session; integrate by copying
the files below into the same package paths).

## Files

| File (under `src/main/java/org/apache/flink/state/forstrs/`) | Items | Status |
|---|---|---|
| `SegmentHash.java` | **NEW class** (V2+V4 shared helper) | copy-free strided polynomial-31 segment hash, bitwise-identical to `Arrays.hashCode(byte[])` |
| `DispatchOrderingHazards.java` | V4 | `sliceBytesEqual` → `MemorySegment.mismatch` intrinsic; `SliceKey.computeHash` FNV-1a per-byte loop → `SegmentHash.polynomial31` (see semantics note below) |
| `timer/ArrowTimerBuffer.java` | V2, V3, V7-doc | `hashOf` computed segment-direct (thread-local scratch + per-hash copy + realloc-on-any-width-change DELETED; the V7 doc-vs-code drift is resolved by deleting both the code and the stale comment); `rowKeyEquals` per-byte loop → `mismatch` |
| `timer/ForStRsKeyGroupedInternalPriorityQueue.java` | V1 | `peek()` memo-HIT is now alloc/copy-free (segment-direct `mismatch` against `peekMemoKey` BEFORE any `copyIndexKey`); `poll()` probes `pendingBuffer.find(liveIndex.keyDataSegment(), kOff, kLen)` directly (both the fresh-composite copy and the heap→scratchSeg copy-back deleted); composite materialized ONLY on memo-miss decode and the `pendingPollDeletes` staging branch |

Tests (under `src/test/java/org/apache/flink/state/forstrs/timer/`):

| File | What it adds |
|---|---|
| `ArrowTimerBufferTest.java` (copy, EXTENDED) | +3 tests: `segmentHashMatchesArraysHashCodeProperty` (V2 hash-identity property test, mandatory lens 0/1/7/8/9 + 500-case fuzz, random offsets), `findExactAndNearMissAtStrideBoundaryLengths` (V3 equality semantics at 1/7/8/9/16/17/33 incl. near-misses), `alternatingKeyWidthsFindExact` (the V2 alternating-width pathology, correctness half) |
| `TimerHeadMemoV1Test.java` (NEW) | 4 focused V1 regressions: repeated-peek memo hits, head-change between peeks must re-decode, pending-ADD-cancel poll branch, flush-then-poll staged-delete branch (+re-add round, anti-resurrection) |

`tools/RunTests.java` — minimal JUnit5 launcher used because no junit-platform-console jar is in
the local `~/.m2` (compile with the platform-launcher jars listed below).

## Exact-semantics notes (review focus)

1. **V1 poll reorder:** the pending-buffer probe is hoisted ABOVE `liveIndex.removeAt(0)`
   (segment offsets `kOff/kLen` are invalidated by index mutation — the audit's stated caution).
   `find()` only reads `pendingBuffer`; `removeAt(0)` only mutates `liveIndex` — order-equivalent.
2. **V1 memo aliasing:** on a memo HIT whose poll lands in the staged-delete branch,
   `pendingPollDeletes` receives the `peekMemoKey` array itself (exact composite bytes, owned,
   never mutated in place anywhere — `flushPollDeletes`/`vectorizedBatchDeleteKeys` only read).
3. **V4 `SliceKey.computeHash` is the ONE intentional internal change:** FNV-1a → polynomial-31.
   The audit lists the `:352` scalar loop under V4, but a hash fold cannot become `mismatch`;
   FNV-1a is inherently per-byte (data-dependent XOR between multiplies), so removing the scalar
   loop requires a stridable polynomial. The hash is transient per-batch `HashSet` probing —
   never persisted, never compared across processes — and `equals()` verifies bytes, so observable
   hazard decisions are unchanged (gated by the existing hazard suites, run green below).
   If review rejects any hash-function change, drop that hunk and keep only the
   `sliceBytesEqual` mismatch fix — they are independent.
4. **V2 hash identity is load-bearing** (open-addressed slot layout): `SegmentHash.polynomial31`
   is bitwise-identical to `Arrays.hashCode(byte[])` (accumulator 1, SIGNED bytes, `31*h+b`),
   property-tested in `segmentHashMatchesArraysHashCodeProperty`.
5. **Out of scope (per audit ranking/preconditions):** V5 (needs the no-retention audit of
   `deserializeUserKey/Value` impls first), V6 (needs the call-graph hotness check first),
   V7 `ColumnarBatchBuffer.copyAt` deletion (test-only callers exist; deletion belongs in an
   in-repo change, not a copy drop-in).

## Verification performed (2026-06-12, zulu-25, this worktree's dylib)

- `javac` (zulu-25) of all 4 main copies against the flink module `target/classes` +
  sibling `flink-{core,core-api,runtime,annotations}/target/classes`: **clean**.
- Test runs (REAL native lib: `-Dforstrs.native.libpath=<worktree>/target/release/libforst_rs_ffi.dylib`,
  built `cargo build --release -p forst-rs-ffi` at engine commit with the BE merge operator),
  my compiled copies FIRST on the classpath so they shadow `target/classes`:
  - `ArrowTimerBufferTest` (extended): **8/8**
  - `TimerHeadMemoV1Test` + `ForStRsKeyGroupedInternalPriorityQueueTest` +
    `ForStRsKeyGroupedInternalPriorityQueueBatchedTest` + `MultiKeygroupTimerFireTest`: **37/37**
  - `VectorizedExecutorOrderingHazardTest` + `VectorizedMixedBatchTest` (the V4 gate): **9/9**
  - `ClassifierOnClearMultiRowDrainTest` + `VectorizedAppendMergeBatchZeroCopyTest` +
    `AsyncDispatchInFlightParallelismTest` + `VectorizedExecutorContainerPoolTest`: **6/6**
- NOT run from here (integration gates, need the flink repo session): full backend suite
  (114/0 baseline) via maven, lockstep exactness ×2 (Stage-0 rule for ANY timer-path edit),
  JFR alloc-rate A/B on q12@10M, q5 windowed-value byte-exactness.

### Classpath recipe used

```
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
FL=/Users/lijunqing/Code/stczwd/flink
M2=$HOME/.m2/repository
CP="rd-java/out/test:rd-java/out/main:rd-java/out/tools:\
$FL/flink-state-backends/flink-statebackend-forst-rs/target/test-classes:\
$FL/flink-state-backends/flink-statebackend-forst-rs/target/classes:\
$FL/flink-core/target/classes:$FL/flink-runtime/target/classes:\
$FL/flink-annotations/target/classes:$FL/flink-core-api/target/classes:\
$FL/flink-metrics/flink-metrics-core/target/classes:\
$M2/org/assertj/assertj-core/3.27.3/assertj-core-3.27.3.jar:\
$M2/org/junit/jupiter/junit-jupiter-api/5.9.3/junit-jupiter-api-5.9.3.jar:\
$M2/org/junit/jupiter/junit-jupiter-engine/5.9.3/junit-jupiter-engine-5.9.3.jar:\
$M2/org/junit/platform/junit-platform-launcher/1.9.3/junit-platform-launcher-1.9.3.jar:\
$M2/org/junit/platform/junit-platform-engine/1.9.3/junit-platform-engine-1.9.3.jar:\
$M2/org/junit/platform/junit-platform-commons/1.9.3/junit-platform-commons-1.9.3.jar:\
$M2/org/opentest4j/opentest4j/1.2.0/opentest4j-1.2.0.jar:\
$M2/org/apiguardian/apiguardian-api/1.1.2/apiguardian-api-1.1.2.jar"
$JAVA_HOME/bin/java --enable-native-access=ALL-UNNAMED \
  -Dforstrs.native.libpath=$PWD/target/release/libforst_rs_ffi.dylib \
  -cp "$CP" RunTests <fully.qualified.TestClass>...
```

## Integration steps (for the flink-repo session)

1. Copy the 4 main files + 2 test files over the same paths in
   `flink-statebackend-forst-rs/src/{main,test}/java/...` (SegmentHash.java and
   TimerHeadMemoV1Test.java are NEW files).
2. `mvn -pl flink-state-backends/flink-statebackend-forst-rs test` (full 114/0 baseline gate).
3. Stage-0 law for the V1 timer edit: lockstep exactness ×2 + routing-async ×5 before any
   default flip rides on this.
4. Audit gate 2 (optional but recommended): JFR alloc-rate A/B on q12@10M — alloc/sec must drop,
   wall within box noise.
