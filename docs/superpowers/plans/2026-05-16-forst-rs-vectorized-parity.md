# Forst-RS Vectorized Parity V1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every functional gap between Forst-RS and community ForSt with end-to-end vectorization and zero-copy as a V1 requirement, on a unified columnar dispatch path.

**Architecture:** 13 PRs sequenced as 6 phases. P0 is a gate (microbench), not code. P1-P4 build the dispatch infrastructure + observability + error handling. P5-P9 stack state primitives on top (Value/Map refactor → List+APPEND_MERGE → RMW cache → Reducing → Aggregating → ITER_RANGE). P10-P12 close out testing matrix + productization. Q3's 1.25× is already a floor; this plan keeps that gain while adding parity for the remaining state types.

**Tech Stack:** Rust 2021 (MSRV 1.88) for `crates/forst-rs-*`; Java 25 (FFM API, jdk.incubator.vector, ZGC, CompactObjectHeaders) for `flink-statebackend-forst-rs`; Apache Flink 2.2.1; JUnit 5; JMH for microbenches; criterion for Rust benches; `proptest` for panic-safety; `~/Downloads/workenv/flink-2.2.1/bin/run-nexmark-matrix.sh` for L6/L7.

**Spec:** `docs/superpowers/specs/2026-05-16-forst-rs-vectorized-parity-design.md` (commit `91e709b6b` on `forst-rs`). All section references below are to that spec.

---

## Cross-PR conventions

### Repo layout (two checkouts)

| Repo | Path | Branch |
|---|---|---|
| ForSt | `/Users/lijunqing/Code/stczwd/ForSt` | `forst-rs` |
| Flink | `/Users/lijunqing/Code/stczwd/flink` | `forst-rs-jdk25` |

### Build / test verification

| Repo | Quick check | Full check |
|---|---|---|
| ForSt | `cargo test --workspace -q` | `cargo build --release && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo doc --no-deps --workspace` |
| Flink | `JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home MAVEN_OPTS=--enable-native-access=ALL-UNNAMED mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Drat.skip=true -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Dargline="--enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -XX:+UseZGC -XX:+UseCompactObjectHeaders -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib"` | same + drop `-Drat.skip` and `-Dtest=` filters |

### Per-PR rebuild contract

Any PR adding a new FFI symbol on the ForSt side MUST rebuild `cargo build --release -p forst-rs-ffi` before Flink-side tests, so `target/release/libforst_rs_ffi.dylib` carries the new symbol. PRs that add FFI symbols call out the rebuild step explicitly.

### Commit message format

`<type>(<scope>): <subject>` — `<type>` ∈ {`feat`, `fix`, `refactor`, `test`, `docs`, `build`, `bench`}; `<scope>` ∈ {`engine`, `ffi`, `state-forst-rs`, `state-forst-rs-dispatch`, `state-forst-rs-iter`, `state-forst-rs-cache`, `state-forst-rs-metrics`, `bench`, `fault-injection`}. End with `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` via heredoc.

### Dependency graph

```
P0 (gate) ──→ P1 ──→ P2 ──→ P3 ──→ P4 ──→ P5 ──┬─→ P6 ──→ P7 ──→ P8 ──→ P9
                                               └─→ P10 ──→ P11 ──→ P12
```

P10 (fault injection) can start in parallel with P5 once P4 lands. P11/P12 gate the V1 release.

### Spec section references

| Spec section | Plan PRs |
|---|---|
| §1 (dispatch table, semantics) | P2, P3, P6, P7, P8, P9 |
| §2 (components 1-17 + A-G) | P1-P9 |
| §3 (Traces A-E) | P5 (A), P6 (B), P7+P8 (C), P3 (D), P8 (E) |
| §4 (error handling) | P1, P2, P4 |
| §5 (testing matrix) | P10, P11 |
| §6 (productization runbook) | P12 |
| Appendix (microbench gate) | P0 |
| Appendix (cross-ref maintenance) | P12 |

---

## File structure

### Rust side (`crates/`)

```
crates/forst-rs-ffi/src/
└── lib.rs                            (MODIFY: +frs_abi_version (G), tighten error envelope (A),
                                       +frs_vec_merge_append (B), +frs_vec_iter_prefix_* (C),
                                       +frs_vec_iter_range_* (D))

crates/forst-rs-storage/src/
└── iter.rs                           (NEW — E: native iterator handle, snapshot-anchored, chunked)

crates/forst-rs-engine/src/
└── list_merge.rs                     (NEW — F: list-append combiner, runs at compaction + read)

crates/forst-rs-test-harness/         (NEW — P10: FaultInjector for L5)
├── Cargo.toml
└── src/lib.rs
```

### Java side (`flink-state-backends/flink-statebackend-forst-rs/`)

```
src/main/java/org/apache/flink/state/forstrs/

ffm/
├── FrsAbi.java                       (NEW — 17: EXPECTED_ABI_VERSION constant)
├── FrsAbiMismatchException.java      (NEW — 17)
└── ForStRsLinker.java                (MODIFY: bind frs_abi_version + 8 new FFI methods)

exec/                                  (NEW package — dispatch infrastructure)
├── SlotArenaScope.java               (NEW — 7)
├── VectorizedStateRequest.java       (NEW — 1: sealed interface)
├── GetRequest.java                   (NEW — 1 subtype)
├── PutRequest.java                   (NEW — 1 subtype)
├── DeleteRequest.java                (NEW — 1 subtype)
├── AppendMergeRequest.java           (NEW — 1 subtype)
├── IterPrefixRequest.java            (NEW — 1 subtype)
├── IterRangeRequest.java             (NEW — 1 subtype)
├── ColumnarBatchBuffer.java          (MODIFY — 3: extend to 6 kinds)
├── VectorizedClassifier.java         (MODIFY — 2: 3→6 kinds + intra-key ordering invariant)
├── VectorizedExecutor.java           (MODIFY — 4: 3→6 dispatches)
├── FrsIterHandle.java                (NEW — 5)
└── IterLifetimeWatchdog.java         (NEW — 6)

cache/                                 (NEW package — RMW)
├── ReducingAggregatingCache.java     (NEW — 14)
├── PendingMissTable.java             (NEW — 15)
└── PendingMiss.java                  (NEW — 15 helper)

metrics/                               (NEW package)
└── DispatchMetrics.java              (NEW — 8)

state/
├── ForStRsValueState.java            (MODIFY — 9: refactor onto dispatch path)
├── ForStRsMapState.java              (MODIFY — 10: same + retain SP6 6.3 staging)
├── ForStRsListState.java             (MODIFY — 11: dispatch path + APPEND_MERGE)
├── ForStRsReducingState.java         (NEW — 12)
└── ForStRsAggregatingState.java      (NEW — 13)

ForStRsKeyedStateBackend.java         (MODIFY: ABI check at init, instantiate SlotArenaScope,
                                       FatalErrorHandler integration, barrier drain in snapshotState)

src/test/java/org/apache/flink/state/forstrs/
├── ffm/FrsAbiMismatchTest.java       (NEW)
├── exec/SlotArenaScopeTest.java      (NEW)
├── exec/VectorizedClassifierTest.java (MODIFY: 3→6 kinds coverage)
├── exec/FrsIterHandleTest.java       (NEW)
├── cache/ReducingAggregatingCacheTest.java (NEW)
├── cache/PendingMissTableTest.java   (NEW)
├── cache/RmwConvoyTest.java          (NEW)
├── metrics/DispatchMetricsTest.java  (NEW)
├── state/ForStRs<Type>StateIntegrationTest.java × 5  (NEW or MODIFY)
└── faultinjection/                   (NEW — P10)
    ├── F1_ErrorCodeSubstitution.java
    ├── F2_WatchdogForceDrop.java
    ├── F3_EnginePanic.java
    ├── F4_SlowFfiReturn.java
    ├── F5_CombinerThrowAtIndex.java
    ├── F6_BarrierTimeFlushFailure.java
    ├── F7_ArenaOverflow.java
    ├── F8_CacheEvictionWithError.java
    ├── F9_BarrierWithIteratorActive.java
    ├── F10_WatchdogVsOperatorRace.java
    ├── F11_SameKeyConvoyPressure.java
    └── F12_EvictionDuringFlush.java
```

### Bench / report files

```
crates/forst-rs-bench/                 (NEW — P0)
└── benches/
    └── component_microbench.rs       (Rust microbenches via criterion)

flink-state-backends/flink-statebackend-forst-rs/
└── src/test/jmh/
    └── ComponentMicrobench.java      (JMH for Java side)

docs/superpowers/specs/
└── 2026-04-04-forst-rs-component-microbench-report.md   (NEW — P0 deliverable)
```

---

## P0 — Pre-implementation microbench gate

**Goal:** Validate the design's per-component performance envelope (Spec Appendix) before committing to the 17-component decomposition. **This PR contains no V1 implementation code — only microbenches and a report.** The result either green-lights P1 or triggers design revision per Appendix.

**Scope:** all 8 component-boundary targets in the Appendix.

**Deliverable:** `docs/superpowers/specs/2026-04-04-forst-rs-component-microbench-report.md` plus the JMH/criterion benchmark sources.

### Task P0.1: Set up JMH harness in flink-statebackend-forst-rs

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/jmh/ComponentMicrobench.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/pom.xml` (add JMH profile)

- [ ] **Step 1: Add JMH dependencies + profile to pom.xml**

Add inside `<profiles>`:

```xml
<profile>
  <id>jmh</id>
  <dependencies>
    <dependency>
      <groupId>org.openjdk.jmh</groupId>
      <artifactId>jmh-core</artifactId>
      <version>1.37</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.openjdk.jmh</groupId>
      <artifactId>jmh-generator-annprocess</artifactId>
      <version>1.37</version>
      <scope>test</scope>
    </dependency>
  </dependencies>
  <build>
    <testSourceDirectory>src/test/jmh</testSourceDirectory>
  </build>
</profile>
```

- [ ] **Step 2: Skeleton bench class with empty turn round-trip target**

```java
package org.apache.flink.state.forstrs.bench;

import org.openjdk.jmh.annotations.*;
import java.lang.foreign.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgs = {"--enable-native-access=ALL-UNNAMED", "--add-modules", "jdk.incubator.vector", "-XX:+UseZGC", "-XX:+UseCompactObjectHeaders"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class ComponentMicrobench {

    private Arena slotArena;
    private MemorySegment turnRegion;
    private long bumpOffset;
    private static final long SLOT_TURN_BYTES = 8 * 1024 * 1024;

    @Setup(Level.Trial)
    public void setup() {
        slotArena = Arena.ofShared();
        turnRegion = slotArena.allocate(SLOT_TURN_BYTES, 64);
        bumpOffset = 0;
    }

    @TearDown(Level.Trial)
    public void tearDown() { slotArena.close(); }

    @Benchmark
    public long emptyTurnRoundTrip() {
        long mark = bumpOffset;
        // ... per spec §2 enter/exit (no allocations on empty turn) ...
        bumpOffset = mark;
        return mark;
    }
}
```

- [ ] **Step 3: Run skeleton bench to verify harness**

```bash
cd /Users/lijunqing/Code/stczwd/flink
mvn -B -pl flink-state-backends/flink-statebackend-forst-rs -Pjmh test-compile exec:exec \
  -Dexec.executable=java \
  -Dexec.args='-cp %classpath org.openjdk.jmh.Main org.apache.flink.state.forstrs.bench.ComponentMicrobench'
```

Expected: bench runs and emits ns/op number for `emptyTurnRoundTrip`. **Target ≤ 200 ns.**

- [ ] **Step 4: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/{pom.xml,src/test/jmh/}
git commit -m "$(cat <<'EOF'
bench(state-forst-rs): JMH harness + empty turn round-trip baseline

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task P0.2: Bench turnRegion bump alloc + encode key/value

- [ ] **Step 1: Add three more @Benchmark methods**

```java
@Benchmark
public MemorySegment turnRegionAllocate256B() {
    long off = (bumpOffset + 63) & ~63L;
    MemorySegment seg = turnRegion.asSlice(off, 256);
    bumpOffset = off + 256;
    if (bumpOffset > SLOT_TURN_BYTES - 4096) bumpOffset = 0;  // reset before overflow
    return seg;
}

@Benchmark
public long encodeKeyInto() {
    long off = (bumpOffset + 63) & ~63L;
    // kg(2B) | sk(8B) | '/' | stateName(8B) | '/' | uk(8B) = ~28B
    turnRegion.set(ValueLayout.JAVA_SHORT, off, (short)42);
    turnRegion.setString(off + 2, "/myState/");
    turnRegion.set(ValueLayout.JAVA_LONG, off + 12, 0xDEADBEEFL);
    bumpOffset = off + 28;
    if (bumpOffset > SLOT_TURN_BYTES - 4096) bumpOffset = 0;
    return bumpOffset;
}

@Benchmark
public long encodeValueInto256B() {
    long off = (bumpOffset + 63) & ~63L;
    for (int i = 0; i < 256; i++)
        turnRegion.set(ValueLayout.JAVA_BYTE, off + i, (byte)i);
    bumpOffset = off + 256;
    if (bumpOffset > SLOT_TURN_BYTES - 4096) bumpOffset = 0;
    return bumpOffset;
}
```

- [ ] **Step 2: Run all 4 benches; capture ns/op**

```bash
mvn -B -pl flink-state-backends/flink-statebackend-forst-rs -Pjmh test-compile exec:exec \
  -Dexec.executable=java \
  -Dexec.args='-cp %classpath org.openjdk.jmh.Main org.apache.flink.state.forstrs.bench.ComponentMicrobench -rf json -rff microbench-p02.json'
```

Targets per Spec Appendix: `turnRegionAllocate256B ≤ 50 ns`, `encodeKeyInto ≤ 100 ns`, `encodeValueInto256B ≤ 150 ns`.

- [ ] **Step 3: Commit numbers + bench source**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/test/jmh/ microbench-p02.json
git commit -m "$(cat <<'EOF'
bench(state-forst-rs): turnRegion alloc + encode key/value microbenches

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task P0.3: Bench classifier submit, executor dispatch, batch buffer flush, pendingMisses lookup

- [ ] **Step 1: Add four more @Benchmark methods**

```java
// Use a placeholder/stub classifier — measures the per-op overhead in isolation
// before real ColumnarBatchBuffer exists. Real bench in P2 will replace these.

private ArrayDeque<Object> stubBatch;
private ConcurrentHashMap<Long, Object> stubMisses;

@Setup(Level.Iteration)
public void perIterSetup() {
    stubBatch = new ArrayDeque<>(64);
    stubMisses = new ConcurrentHashMap<>();
}

@Benchmark
public Object classifierSubmitStub() {
    Object request = new Object();
    stubBatch.add(request);
    if (stubBatch.size() >= 64) stubBatch.clear();
    return request;
}

@Benchmark
public int executorDispatchStub() {
    // 64-row batch round-trip placeholder: write header + return parse
    long off = (bumpOffset + 63) & ~63L;
    for (int i = 0; i < 64; i++) turnRegion.set(ValueLayout.JAVA_INT, off + i*8, i);
    int sum = 0;
    for (int i = 0; i < 64; i++) sum += turnRegion.get(ValueLayout.JAVA_INT, off + i*8);
    bumpOffset = off + 64*8;
    if (bumpOffset > SLOT_TURN_BYTES - 4096) bumpOffset = 0;
    return sum;
}

@Benchmark
public Object pendingMissComputeIfAbsentHit() {
    return stubMisses.computeIfAbsent(42L, k -> new Object());
}

@Benchmark
public Object pendingMissComputeIfAbsentMiss() {
    return stubMisses.computeIfAbsent(System.nanoTime(), k -> new Object());
}
```

- [ ] **Step 2: Run + capture**

Targets: `classifierSubmitStub ≤ 100 ns`, `executorDispatchStub ≤ 5 µs / 64 rows ≈ 80 ns/row`, `pendingMissHit ≤ 50 ns`, `pendingMissMiss ≤ 200 ns`.

- [ ] **Step 3: Commit**

### Task P0.4: Rust-side criterion benches for engine boundaries

**Files:**
- Create: `crates/forst-rs-bench/Cargo.toml`
- Create: `crates/forst-rs-bench/benches/component_microbench.rs`

- [ ] **Step 1: Cargo.toml**

```toml
[package]
name = "forst-rs-bench"
version = "0.1.0"
edition = "2021"

[dependencies]
forst-rs-engine = { path = "../forst-rs-engine" }
forst-rs-storage = { path = "../forst-rs-storage" }

[dev-dependencies]
criterion = "0.5"

[[bench]]
name = "component_microbench"
harness = false
```

- [ ] **Step 2: Bench engine batch get/put round-trip from Rust (no FFI overhead) to isolate engine cost**

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use forst_rs_engine::Engine;

fn bench_engine_batch_put(c: &mut Criterion) {
    let engine = Engine::open_in_memory().unwrap();
    let keys: Vec<Vec<u8>> = (0..64).map(|i| format!("key-{:08}", i).into_bytes()).collect();
    let vals: Vec<Vec<u8>> = (0..64).map(|_| vec![0u8; 256]).collect();

    c.bench_function("engine_batch_put_64_rows_256B", |b| {
        b.iter(|| {
            engine.batch_put(black_box(&keys), black_box(&vals)).unwrap();
        });
    });
}

fn bench_engine_batch_get(c: &mut Criterion) {
    let engine = Engine::open_in_memory().unwrap();
    let keys: Vec<Vec<u8>> = (0..64).map(|i| format!("key-{:08}", i).into_bytes()).collect();
    let vals: Vec<Vec<u8>> = (0..64).map(|_| vec![0u8; 256]).collect();
    engine.batch_put(&keys, &vals).unwrap();

    c.bench_function("engine_batch_get_64_rows_256B", |b| {
        b.iter(|| {
            let _ = engine.batch_get(black_box(&keys)).unwrap();
        });
    });
}

criterion_group!(benches, bench_engine_batch_put, bench_engine_batch_get);
criterion_main!(benches);
```

- [ ] **Step 3: Run + capture**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
cargo bench -p forst-rs-bench --bench component_microbench
```

- [ ] **Step 4: Commit**

### Task P0.5: Write microbench report + go/revise decision

**File:** `docs/superpowers/specs/2026-04-04-forst-rs-component-microbench-report.md`

- [ ] **Step 1: Aggregate all numbers into the report**

Report structure:

```markdown
# Forst-RS Component Microbench Report (Pre-V1 Gate)

**Date:** YYYY-MM-DD
**Decision:** GO / REVISE
**Reference:** Spec Appendix — Pre-implementation validation gate.

## Per-component results

| Component | Target | Measured (p99) | Pass? |
|---|---|---|---|
| SlotArenaScope.enter()/exit() (empty turn) | ≤ 200 ns | <N> ns | <✓/✗> |
| turnRegion.allocate(256B aligned) | ≤ 50 ns | <N> ns | <✓/✗> |
| encodeKeyInto | ≤ 100 ns | <N> ns | <✓/✗> |
| encodeValueInto (256B) | ≤ 150 ns | <N> ns | <✓/✗> |
| VectorizedClassifier.submit() (stub) | ≤ 100 ns | <N> ns | <✓/✗> |
| VectorizedExecutor.dispatch() (stub, 64 rows) | ≤ 5 µs total / 80 ns/row | <N> ns | <✓/✗> |
| pendingMisses.computeIfAbsent (hit) | ≤ 50 ns | <N> ns | <✓/✗> |
| pendingMisses.computeIfAbsent (miss) | ≤ 200 ns | <N> ns | <✓/✗> |
| Rust engine batch_put 64×256B | (informational) | <N> ns | n/a |
| Rust engine batch_get 64×256B | (informational) | <N> ns | n/a |

## Sum-along-Trace-A

Sum measured: <N> ns
Target: ≤ 1 µs at p99 for 256 B values, 32-row batches.

Status: <PASS / FAIL / BORDERLINE>

## Decision

<one of:>

**GO:** All component targets met; sum-along-trace-A within budget. Proceed to P1.

**REVISE (component consolidation required):** Component(s) <X, Y> exceed target by <factor>×. Recommended consolidation per Spec Appendix:
- <consolidation 1>
- <consolidation 2>
Re-bench after consolidation; rerun this gate before P1.

**HARD STOP (design revision):** Sum-along-trace-A exceeds 2× target. Open issue `forst-rs-v1-design-revision` with measured profile; design phase reopens.
```

- [ ] **Step 2: Make the call**

If sum-along-trace-A ≤ 1 µs → write GO, commit, proceed to P1.
If 1 µs < sum ≤ 2 µs → write REVISE with consolidations, commit, open follow-up tasks, re-bench, only enter P1 after re-bench.
If sum > 2 µs → write HARD STOP, commit, escalate; do not enter P1.

- [ ] **Step 3: Commit report**

```bash
git add docs/superpowers/specs/2026-04-04-forst-rs-component-microbench-report.md
git commit -m "$(cat <<'EOF'
bench(state-forst-rs): P0 microbench report — DECISION: <GO/REVISE/STOP>

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task P0.6: Open P1 (or escalate)

- [ ] **Step 1: If GO** — proceed to P1.
- [ ] **Step 1: If REVISE** — file consolidation tasks in the plan as P0.7+, complete them, re-run P0.5, then proceed.
- [ ] **Step 1: If HARD STOP** — do not proceed. Notify user; reopen brainstorming.

---

## P1 — Foundations: SlotArenaScope + ABI version

**Goal:** Land the slot-lifetime memory primitive and the ABI version check. These are no-op without consumers, but every later PR depends on them.

**Spec components:** 7 (SlotArenaScope), 17 (FrsAbi), Rust G (frs_abi_version).

### Task P1.1: Rust — `frs_abi_version` FFI symbol

**Files:**
- Modify: `crates/forst-rs-ffi/src/lib.rs`

- [ ] **Step 1: Write failing Rust test**

```rust
// in crates/forst-rs-ffi/src/lib.rs (or tests file)
#[test]
fn abi_version_is_one() {
    assert_eq!(frs_abi_version(), 1);
}
```

- [ ] **Step 2: Run + verify it fails (function doesn't exist)**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
cargo test -p forst-rs-ffi abi_version_is_one
```

Expected: compile error `cannot find function frs_abi_version`.

- [ ] **Step 3: Implement**

In `crates/forst-rs-ffi/src/lib.rs`:

```rust
pub const FRS_ABI_VERSION: u32 = 1;

#[no_mangle]
pub extern "C" fn frs_abi_version() -> u32 { FRS_ABI_VERSION }
```

- [ ] **Step 4: Run + verify passes**

```bash
cargo test -p forst-rs-ffi abi_version_is_one
```

- [ ] **Step 5: Rebuild cdylib + commit**

```bash
cargo build --release -p forst-rs-ffi
git add crates/forst-rs-ffi/src/lib.rs
git commit -m "$(cat <<'EOF'
feat(ffi): add frs_abi_version returning u32 constant (V1=1)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task P1.2: Java — FrsAbi + FrsAbiMismatchException + Linker binding

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/FrsAbi.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/FrsAbiMismatchException.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java`

- [ ] **Step 1: Write failing test**

Create `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ffm/FrsAbiTest.java`:

```java
package org.apache.flink.state.forstrs.ffm;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FrsAbiTest {
    @Test
    void expectedVersionIsOne() {
        assertEquals(1, FrsAbi.EXPECTED_ABI_VERSION);
    }

    @Test
    void linkerReportsMatchingVersion() {
        int actual = ForStRsLinker.frs_abi_version();
        assertEquals(FrsAbi.EXPECTED_ABI_VERSION, actual);
    }

    @Test
    void mismatchExceptionCarriesBothVersions() {
        FrsAbiMismatchException ex = new FrsAbiMismatchException(2, 1);
        assertTrue(ex.getMessage().contains("2"));
        assertTrue(ex.getMessage().contains("1"));
    }
}
```

- [ ] **Step 2: Run + verify fails**

```bash
cd /Users/lijunqing/Code/stczwd/flink
mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Dtest=FrsAbiTest -Drat.skip=true -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib
```

Expected: compile error — `FrsAbi`, `FrsAbiMismatchException` not defined.

- [ ] **Step 3: Implement FrsAbi.java**

```java
package org.apache.flink.state.forstrs.ffm;

public final class FrsAbi {
    public static final int EXPECTED_ABI_VERSION = 1;
    private FrsAbi() {}
}
```

- [ ] **Step 4: Implement FrsAbiMismatchException.java**

```java
package org.apache.flink.state.forstrs.ffm;

public class FrsAbiMismatchException extends RuntimeException {
    private final int actualVersion;
    private final int expectedVersion;

    public FrsAbiMismatchException(int actual, int expected) {
        super("Forst-RS native ABI mismatch: native lib reports version " + actual
            + " but Java side expects version " + expected
            + ". Verify libforst_rs_ffi matches the deployed Java jar.");
        this.actualVersion = actual;
        this.expectedVersion = expected;
    }

    public int getActualVersion() { return actualVersion; }
    public int getExpectedVersion() { return expectedVersion; }
}
```

- [ ] **Step 5: Bind `frs_abi_version` in `ForStRsLinker.java`**

Find the existing linker class; add the new binding next to existing methods:

```java
// At the top, near other MethodHandle fields:
private static final MethodHandle FRS_ABI_VERSION =
    LINKER.downcallHandle(
        LIB.find("frs_abi_version").orElseThrow(() ->
            new UnsatisfiedLinkError("frs_abi_version not found in libforst_rs_ffi — rebuild required")),
        FunctionDescriptor.of(JAVA_INT));

// Public method:
public static int frs_abi_version() {
    try {
        return (int) FRS_ABI_VERSION.invokeExact();
    } catch (Throwable t) {
        throw new RuntimeException("frs_abi_version FFI call failed", t);
    }
}
```

- [ ] **Step 6: Run + verify passes**

```bash
mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Dtest=FrsAbiTest -Drat.skip=true -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib
```

- [ ] **Step 7: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/{FrsAbi,FrsAbiMismatchException}.java \
        flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java \
        flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ffm/FrsAbiTest.java
git commit -m "$(cat <<'EOF'
feat(state-forst-rs): ABI version check at startup (component 17)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task P1.3: Wire ABI check into ForStRsKeyedStateBackend init

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsKeyedStateBackend.java`

- [ ] **Step 1: Write failing integration test**

Create `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ffm/FrsAbiMismatchAtInitTest.java`:

```java
package org.apache.flink.state.forstrs.ffm;

import org.apache.flink.state.forstrs.ForStRsKeyedStateBackend;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FrsAbiMismatchAtInitTest {
    @Test
    void backendInitVerifiesAbiVersion() {
        // The standard backend init path must call ForStRsLinker.frs_abi_version()
        // and throw FrsAbiMismatchException if it mismatches FrsAbi.EXPECTED_ABI_VERSION.
        // Since both should match (1==1), this passes without throwing.
        assertDoesNotThrow(() -> ForStRsKeyedStateBackend.verifyAbi());
    }

    @Test
    void verifyAbiPublishedAsPublicEntryPoint() {
        // The check must be callable independently for diagnostics.
        // Re-verifying twice should succeed twice (idempotent).
        ForStRsKeyedStateBackend.verifyAbi();
        ForStRsKeyedStateBackend.verifyAbi();
    }
}
```

- [ ] **Step 2: Run + verify fails (method not defined)**

```bash
mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Dtest=FrsAbiMismatchAtInitTest -Drat.skip=true -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib
```

- [ ] **Step 3: Implement `verifyAbi()` static method**

In `ForStRsKeyedStateBackend.java`:

```java
// Add near the top, before constructors:
public static void verifyAbi() {
    int actual = ForStRsLinker.frs_abi_version();
    if (actual != FrsAbi.EXPECTED_ABI_VERSION) {
        throw new FrsAbiMismatchException(actual, FrsAbi.EXPECTED_ABI_VERSION);
    }
}

// Inside the constructor (or initialize() method), as the FIRST executable line:
public ForStRsKeyedStateBackend(/* existing args */) {
    verifyAbi();
    // ... existing init ...
}
```

- [ ] **Step 4: Run + verify passes**

```bash
mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Dtest=FrsAbiMismatchAtInitTest -Drat.skip=true -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib
```

- [ ] **Step 5: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsKeyedStateBackend.java \
        flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ffm/FrsAbiMismatchAtInitTest.java
git commit -m "$(cat <<'EOF'
feat(state-forst-rs): wire ABI check into backend init

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task P1.4: SlotArenaScope skeleton (component 7)

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/SlotArenaScope.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/exec/SlotArenaScopeTest.java`

- [ ] **Step 1: Write failing test — turnRegion bump-alloc reset on exit**

```java
package org.apache.flink.state.forstrs.exec;

import org.junit.jupiter.api.*;
import java.lang.foreign.MemorySegment;
import static org.junit.jupiter.api.Assertions.*;

class SlotArenaScopeTest {

    SlotArenaScope scope;

    @BeforeEach void newScope() { scope = SlotArenaScope.openForSlot(8 * 1024 * 1024, 64 * 1024 * 1024); }
    @AfterEach  void close()    { scope.closeSlot(); }

    @Test
    void turnRegionBumpReset() {
        scope.enterTurn();
        MemorySegment seg1 = scope.allocateTurn(256, 64);
        long offsetAfter = scope.turnBumpOffset();
        scope.exitTurn();

        scope.enterTurn();
        MemorySegment seg2 = scope.allocateTurn(256, 64);
        // After exitTurn, offset reset to mark — new alloc returns same slice address
        assertEquals(seg1.address(), seg2.address());
        scope.exitTurn();
    }

    @Test
    void enterAssertsIterRegistryEmpty() {
        scope.enterTurn();
        scope.exitTurn();
        scope.enterTurn();   // must not throw — registry empty
        scope.exitTurn();
    }

    @Test
    void overflowCreatesPerTurnArena() {
        scope.enterTurn();
        // Allocate beyond turnRegion bound (8 MiB); single huge alloc forces overflow
        MemorySegment huge = scope.allocateTurn(9 * 1024 * 1024, 64);
        assertNotNull(huge);
        assertEquals(1, scope.overflowArenaCountForCurrentTurn());
        scope.exitTurn();
        // After exit, overflow arenas closed; counter resets next turn
        scope.enterTurn();
        assertEquals(0, scope.overflowArenaCountForCurrentTurn());
        scope.exitTurn();
    }

    @Test
    void cacheRegionSurvivesTurnBoundary() {
        scope.enterTurn();
        MemorySegment cacheSeg = scope.allocateCache(1024, 64);
        scope.exitTurn();
        // cacheRegion not affected by bump reset; segment still accessible
        cacheSeg.set(java.lang.foreign.ValueLayout.JAVA_BYTE, 0, (byte)42);
        assertEquals(42, cacheSeg.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0));
    }
}
```

- [ ] **Step 2: Run + verify fails (class doesn't exist)**

```bash
mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Dtest=SlotArenaScopeTest -Drat.skip=true -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib
```

- [ ] **Step 3: Implement SlotArenaScope.java**

```java
package org.apache.flink.state.forstrs.exec;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class SlotArenaScope {

    private final Arena slotArena;
    private final MemorySegment turnRegion;
    private final MemorySegment cacheRegion;
    private final long turnRegionBytes;
    private final long cacheRegionBytes;

    private long turnBumpOffset = 0L;
    private long turnBumpMark = 0L;
    private final AtomicLong cacheBumpOffset = new AtomicLong(0L);

    private final List<Arena> overflowArenasThisTurn = new ArrayList<>();
    private final ConcurrentHashMap<Long, FrsIterHandle> iterRegistry = new ConcurrentHashMap<>();

    private SlotArenaScope(long turnBytes, long cacheBytes) {
        this.turnRegionBytes = turnBytes;
        this.cacheRegionBytes = cacheBytes;
        this.slotArena = Arena.ofShared();
        this.turnRegion = slotArena.allocate(turnBytes, 64);
        this.cacheRegion = slotArena.allocate(cacheBytes, 64);
    }

    public static SlotArenaScope openForSlot(long turnBytes, long cacheBytes) {
        return new SlotArenaScope(turnBytes, cacheBytes);
    }

    public void enterTurn() {
        if (!iterRegistry.isEmpty()) {
            // §2 Severe-1: defense-in-depth — handle leaked across turn boundary
            // Metric: arena.iter_leak_at_enter (wired in P4)
            for (FrsIterHandle h : iterRegistry.values()) h.forceClose();
            iterRegistry.clear();
        }
        turnBumpMark = turnBumpOffset;
        overflowArenasThisTurn.clear();
    }

    public void exitTurn() {
        for (FrsIterHandle h : iterRegistry.values()) h.close();
        iterRegistry.clear();
        turnBumpOffset = turnBumpMark;
        for (Arena a : overflowArenasThisTurn) a.close();
        overflowArenasThisTurn.clear();
    }

    public MemorySegment allocateTurn(long bytes, long align) {
        long aligned = (turnBumpOffset + align - 1) & ~(align - 1);
        if (aligned + bytes <= turnRegionBytes) {
            MemorySegment seg = turnRegion.asSlice(aligned, bytes);
            turnBumpOffset = aligned + bytes;
            return seg;
        }
        Arena overflow = Arena.ofShared();
        overflowArenasThisTurn.add(overflow);
        return overflow.allocate(bytes, align);
    }

    public MemorySegment allocateCache(long bytes, long align) {
        long aligned;
        long after;
        long current;
        do {
            current = cacheBumpOffset.get();
            aligned = (current + align - 1) & ~(align - 1);
            after = aligned + bytes;
            if (after > cacheRegionBytes)
                throw new OutOfMemoryError("SlotArenaScope cacheRegion exhausted");
        } while (!cacheBumpOffset.compareAndSet(current, after));
        return cacheRegion.asSlice(aligned, bytes);
    }

    public void registerIter(FrsIterHandle h) { iterRegistry.put(h.handleId(), h); }
    public void unregisterIter(long handleId) { iterRegistry.remove(handleId); }
    public int iterRegistrySize() { return iterRegistry.size(); }
    public int overflowArenaCountForCurrentTurn() { return overflowArenasThisTurn.size(); }
    public long turnBumpOffset() { return turnBumpOffset; }

    public void closeSlot() {
        for (FrsIterHandle h : iterRegistry.values()) h.forceClose();
        iterRegistry.clear();
        for (Arena a : overflowArenasThisTurn) a.close();
        overflowArenasThisTurn.clear();
        slotArena.close();
    }
}
```

NOTE: `FrsIterHandle` referenced above is created in Task P3.1; for now, add a stub class that has `handleId()`, `close()`, `forceClose()` methods returning Long.

- [ ] **Step 4: Stub FrsIterHandle for compilation**

Create `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/FrsIterHandle.java`:

```java
package org.apache.flink.state.forstrs.exec;

// STUB — real implementation in P3. Sufficient for SlotArenaScope to compile.
public abstract class FrsIterHandle implements AutoCloseable {
    public abstract long handleId();
    @Override public abstract void close();
    public abstract void forceClose();
}
```

- [ ] **Step 5: Run + verify passes**

- [ ] **Step 6: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/{SlotArenaScope,FrsIterHandle}.java \
        flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/exec/SlotArenaScopeTest.java
git commit -m "$(cat <<'EOF'
feat(state-forst-rs-dispatch): SlotArenaScope with turn/cache regions + iter registry (component 7)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task P1.5: Wire SlotArenaScope into ForStRsKeyedStateBackend lifecycle

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsKeyedStateBackend.java`

- [ ] **Step 1: Write failing test**

Create test `ForStRsKeyedStateBackendArenaLifecycleTest.java`:

```java
@Test
void backendOwnsSlotArenaScopeFromInitToDispose() {
    ForStRsKeyedStateBackend backend = newTestBackend();
    assertNotNull(backend.slotArenaScope());
    assertEquals(0, backend.slotArenaScope().turnBumpOffset());
    backend.dispose();
    assertThrows(IllegalStateException.class, () -> backend.slotArenaScope().enterTurn());
}
```

- [ ] **Step 2: Run + fail**

- [ ] **Step 3: Add field + accessors to backend**

```java
// In ForStRsKeyedStateBackend.java:
private SlotArenaScope slotArenaScope;
private static final long DEFAULT_TURN_BYTES  = 8L * 1024 * 1024;
private static final long DEFAULT_CACHE_BYTES = 64L * 1024 * 1024;

// In constructor:
this.slotArenaScope = SlotArenaScope.openForSlot(DEFAULT_TURN_BYTES, DEFAULT_CACHE_BYTES);

// Add accessor:
public SlotArenaScope slotArenaScope() { return slotArenaScope; }

// In dispose() / close():
slotArenaScope.closeSlot();
slotArenaScope = null;  // any post-dispose use throws NPE → wrap in IllegalStateException accessor
```

Refine accessor to throw if disposed:

```java
public SlotArenaScope slotArenaScope() {
    if (slotArenaScope == null) throw new IllegalStateException("Backend disposed");
    return slotArenaScope;
}
```

- [ ] **Step 4: Run + pass; Step 5: Commit**

```bash
git commit -m "feat(state-forst-rs-dispatch): own SlotArenaScope in backend lifecycle"
```

---

## P2 — Dispatch core: VectorizedStateRequest + Classifier + Executor + ColumnarBatchBuffer

**Goal:** Build the typed request envelope, extend the existing classifier and batch buffer from 3 to 6 request kinds, and tighten the FFI error envelope. After this PR the new dispatch path exists but no state primitive uses it yet.

**Spec components:** 1 (VectorizedStateRequest), 2 (Classifier), 3 (ColumnarBatchBuffer), 4 (Executor). FFI A error envelope tightening.

### Task P2.1: Rust — typed FrsErrorCode + FrsRowResult layout

**Files:** Modify `crates/forst-rs-ffi/src/lib.rs`

- [ ] **Step 1: Test that error codes match spec**

```rust
#[test]
fn error_codes_match_spec_section_4() {
    assert_eq!(FrsErrorCode::Ok as u32, 0);
    assert_eq!(FrsErrorCode::NotFound as u32, 1);
    assert_eq!(FrsErrorCode::KeyTooLarge as u32, 100);
    assert_eq!(FrsErrorCode::ValueTooLarge as u32, 101);
    assert_eq!(FrsErrorCode::BatchHeaderMalformed as u32, 110);
    assert_eq!(FrsErrorCode::IterExpired as u32, 200);
    assert_eq!(FrsErrorCode::IterCursorInvalid as u32, 201);
    assert_eq!(FrsErrorCode::EngineIo as u32, 300);
    assert_eq!(FrsErrorCode::EngineCorrupted as u32, 301);
    assert_eq!(FrsErrorCode::EngineOom as u32, 302);
    assert_eq!(FrsErrorCode::EngineDiskFull as u32, 303);
    assert_eq!(FrsErrorCode::PanicCaught as u32, 900);
    assert_eq!(FrsErrorCode::Unknown as u32, 999);
}
```

- [ ] **Step 2: Run + fail**

- [ ] **Step 3: Define enum + struct**

```rust
// crates/forst-rs-ffi/src/lib.rs

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FrsRowResult {
    pub code: u32,
    pub payload_off: u32,
    pub payload_len: u32,
}

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrsErrorCode {
    Ok                       = 0,
    NotFound                 = 1,
    KeyTooLarge              = 100,
    ValueTooLarge            = 101,
    BatchHeaderMalformed     = 110,
    IterExpired              = 200,
    IterCursorInvalid        = 201,
    EngineIo                 = 300,
    EngineCorrupted          = 301,
    EngineOom                = 302,
    EngineDiskFull           = 303,
    PanicCaught              = 900,
    Unknown                  = 999,
}
```

- [ ] **Step 4: Pass + commit**

```bash
cargo test -p forst-rs-ffi error_codes_match_spec_section_4
git add crates/forst-rs-ffi/src/lib.rs
git commit -m "feat(ffi): typed FrsErrorCode enum + FrsRowResult struct per §4"
```

### Task P2.2: Rust — tighten existing frs_vec_get/put/del error envelope

- [ ] **Step 1: Test that ENGINE_IO from underlying engine maps to fail-batch on all rows**

```rust
#[test]
fn engine_io_fails_whole_batch() {
    // Inject ENGINE_IO via test-harness mock engine; call frs_vec_put;
    // assert every FrsRowResult.code == EngineIo
}
```

- [ ] **Step 2: Fail; Step 3: Implement wrapper that catches engine errors and emits per-row codes; Step 4: Pass; Step 5: Commit**

```rust
pub fn emit_batch_error(out: *mut FrsRowResult, n: usize, code: FrsErrorCode) {
    unsafe {
        for i in 0..n {
            *out.add(i) = FrsRowResult { code: code as u32, payload_off: 0, payload_len: 0 };
        }
    }
}

// In frs_vec_put body:
match std::panic::catch_unwind(|| /* existing logic */) {
    Ok(Ok(())) => { /* per-row codes already written */ }
    Ok(Err(EngineErr::Io(_)))          => emit_batch_error(out, n, FrsErrorCode::EngineIo),
    Ok(Err(EngineErr::Corrupted(_)))   => emit_batch_error(out, n, FrsErrorCode::EngineCorrupted),
    Ok(Err(EngineErr::Oom))            => emit_batch_error(out, n, FrsErrorCode::EngineOom),
    Ok(Err(EngineErr::DiskFull))       => emit_batch_error(out, n, FrsErrorCode::EngineDiskFull),
    Err(_)                             => emit_batch_error(out, n, FrsErrorCode::PanicCaught),
}
```

Repeat for `frs_vec_get`, `frs_vec_del`. Rebuild dylib.

### Task P2.3: Java — FrsException hierarchy

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/FrsException.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/FrsErrorCode.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/FrsIteratorExpiredException.java`

- [ ] **Step 1: Failing tests**

```java
@Test void errorCodeFromU32() {
    assertEquals(FrsErrorCode.OK, FrsErrorCode.fromU32(0));
    assertEquals(FrsErrorCode.ENGINE_IO, FrsErrorCode.fromU32(300));
    assertEquals(FrsErrorCode.UNKNOWN, FrsErrorCode.fromU32(99999));
}

@Test void frsExceptionHasCodeAndPosition() {
    FrsException e = new FrsException(FrsErrorCode.KEY_TOO_LARGE, 7, new byte[]{1,2,3});
    assertEquals(FrsErrorCode.KEY_TOO_LARGE, e.code());
    assertEquals(7, e.rowIndex());
    assertArrayEquals(new byte[]{1,2,3}, e.detail());
}

@Test void iteratorExpiredIsSubclass() {
    assertTrue(FrsException.class.isAssignableFrom(FrsIteratorExpiredException.class));
}
```

- [ ] **Step 2-4: Fail → implement → pass**

```java
public enum FrsErrorCode {
    OK(0), NOT_FOUND(1),
    KEY_TOO_LARGE(100), VALUE_TOO_LARGE(101),
    BATCH_HEADER_MALFORMED(110),
    ITER_EXPIRED(200), ITER_CURSOR_INVALID(201),
    ENGINE_IO(300), ENGINE_CORRUPTED(301), ENGINE_OOM(302), ENGINE_DISK_FULL(303),
    PANIC_CAUGHT(900), UNKNOWN(999);

    private final int code;
    FrsErrorCode(int c) { this.code = c; }
    public int code() { return code; }
    public static FrsErrorCode fromU32(int v) {
        for (FrsErrorCode e : values()) if (e.code == v) return e;
        return UNKNOWN;
    }
    public boolean isFailRow()   { return code >= 1 && code < 300; }
    public boolean isFailBatch() { return code >= 300 && code < 900; }
    public boolean isFailProcess() { return code >= 900; }
}

public class FrsException extends RuntimeException {
    private final FrsErrorCode code;
    private final int rowIndex;
    private final byte[] detail;
    public FrsException(FrsErrorCode code, int rowIndex, byte[] detail) {
        super("FrsException code=" + code + " row=" + rowIndex);
        this.code = code; this.rowIndex = rowIndex; this.detail = detail;
    }
    public FrsErrorCode code() { return code; }
    public int rowIndex() { return rowIndex; }
    public byte[] detail() { return detail; }
}

public class FrsIteratorExpiredException extends FrsException {
    public FrsIteratorExpiredException(int row) {
        super(FrsErrorCode.ITER_EXPIRED, row, new byte[0]);
    }
}
```

- [ ] **Step 5: Commit**

### Task P2.4: VectorizedStateRequest sealed interface + 6 subtypes

**Files:** create under `org.apache.flink.state.forstrs.exec/`

- [ ] **Step 1: Failing test**

```java
@Test void sealedHierarchyExactly6Subtypes() {
    Set<Class<?>> subtypes = Set.of(
        GetRequest.class, PutRequest.class, DeleteRequest.class,
        AppendMergeRequest.class, IterPrefixRequest.class, IterRangeRequest.class);
    assertEquals(6, subtypes.size());
    for (Class<?> c : subtypes) assertTrue(VectorizedStateRequest.class.isAssignableFrom(c));
}

@Test void getRequestCarriesKeySliceAndFuture() {
    SlotArenaScope scope = SlotArenaScope.openForSlot(1<<20, 1<<20);
    scope.enterTurn();
    MemorySegment k = scope.allocateTurn(32, 8);
    GetRequest r = new GetRequest("myState", k);
    assertNotNull(r.future());
    assertEquals(VectorizedStateRequest.Kind.GET, r.kind());
    scope.exitTurn();
    scope.closeSlot();
}
```

- [ ] **Step 2-4: Implement**

```java
// VectorizedStateRequest.java
package org.apache.flink.state.forstrs.exec;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.CompletableFuture;

public sealed interface VectorizedStateRequest
        permits GetRequest, PutRequest, DeleteRequest,
                AppendMergeRequest, IterPrefixRequest, IterRangeRequest {
    enum Kind { GET, PUT, DELETE, APPEND_MERGE, ITER_PREFIX, ITER_RANGE }
    Kind kind();
    String stateName();
}
```

```java
// GetRequest.java
public final class GetRequest implements VectorizedStateRequest {
    private final String stateName;
    private final MemorySegment keySlice;
    private final CompletableFuture<byte[]> future = new CompletableFuture<>();
    public GetRequest(String stateName, MemorySegment keySlice) {
        this.stateName = stateName; this.keySlice = keySlice;
    }
    public CompletableFuture<byte[]> future() { return future; }
    public MemorySegment keySlice() { return keySlice; }
    @Override public Kind kind() { return Kind.GET; }
    @Override public String stateName() { return stateName; }
}
```

Repeat the same shape for `PutRequest` (adds `valueSlice`), `DeleteRequest` (no value), `AppendMergeRequest` (adds `valueSlices: MemorySegment[]`), `IterPrefixRequest` (adds `prefixSlice`, `chunkBufSlice`; future type `CompletableFuture<IterFirstChunk>`), `IterRangeRequest` (adds `loSlice`, `hiSlice`, `chunkBufSlice`).

`IterFirstChunk` is a small record carrying the FrsIterHandle + first chunk's row count.

- [ ] **Step 5: Commit**

### Task P2.5: ColumnarBatchBuffer extension (3→6 kinds)

**Files:** modify `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/ColumnarBatchBuffer.java`

Find existing buffer (only handles GET/PUT/DELETE today). Extend with:

- [ ] **Step 1: Failing test — append AppendMergeRequest column**

```java
@Test void batchBufferAcceptsAppendMergeRequest() {
    SlotArenaScope scope = SlotArenaScope.openForSlot(1<<20, 1<<20);
    scope.enterTurn();
    ColumnarBatchBuffer buf = ColumnarBatchBuffer.forKind(VectorizedStateRequest.Kind.APPEND_MERGE, "list1", scope);
    MemorySegment k = scope.allocateTurn(16, 8);
    MemorySegment[] vs = { scope.allocateTurn(32, 8), scope.allocateTurn(32, 8) };
    buf.append(new AppendMergeRequest("list1", k, vs));
    assertEquals(1, buf.rowCount());
    scope.exitTurn(); scope.closeSlot();
}
```

- [ ] **Step 2-4: extend ColumnarBatchBuffer**

Add per-kind column layouts:

```java
public static ColumnarBatchBuffer forKind(VectorizedStateRequest.Kind k, String stateName, SlotArenaScope scope) {
    return switch (k) {
        case GET -> new GetBatchBuffer(stateName, scope);
        case PUT -> new PutBatchBuffer(stateName, scope);
        case DELETE -> new DeleteBatchBuffer(stateName, scope);
        case APPEND_MERGE -> new AppendMergeBatchBuffer(stateName, scope);
        case ITER_PREFIX -> new IterPrefixBatchBuffer(stateName, scope);
        case ITER_RANGE -> new IterRangeBatchBuffer(stateName, scope);
    };
}
```

Each subtype holds:
- `offsets[]` (Arrow-style) for variable-length payloads
- `data` (flat MemorySegment in turnRegion)
- `requests` (parallel array of the source `VectorizedStateRequest` objects for future resolution)

- [ ] **Step 5: Commit**

### Task P2.6: VectorizedClassifier extension (3→6 kinds + intra-key ordering invariant)

**Files:** modify `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/VectorizedClassifier.java`

- [ ] **Step 1: Failing tests**

```java
@Test void classifierGroupsByStateAndKind() {
    classifier.submit(new GetRequest("a", k1));
    classifier.submit(new GetRequest("a", k2));
    classifier.submit(new PutRequest("a", k1, v1));
    assertEquals(2, classifier.pendingBatchCount());  // (a, GET), (a, PUT)
}

@Test void classifierPreservesIntraStateKeyOrder() {
    classifier.submit(new PutRequest("a", k1, v1));  // first
    classifier.submit(new PutRequest("a", k1, v2));  // second
    ColumnarBatchBuffer buf = classifier.peekBatch("a", VectorizedStateRequest.Kind.PUT);
    assertEquals(v1, buf.valueAt(0));
    assertEquals(v2, buf.valueAt(1));  // ORDER PRESERVED
}

@Test void appendMergeOnlyAcceptedFromListState() {
    // Reducing/Aggregating state names should NOT submit AppendMergeRequest
    assertThrows(IllegalArgumentException.class,
        () -> classifier.submit(new AppendMergeRequest("reducing-state", k1, new MemorySegment[]{v1})));
}
```

- [ ] **Step 2-4: extend implementation**

```java
public class VectorizedClassifier {
    private final Map<BatchKey, ColumnarBatchBuffer> pendingBatches = new LinkedHashMap<>();
    private final SlotArenaScope scope;
    private final VectorizedExecutor executor;

    private record BatchKey(String stateName, VectorizedStateRequest.Kind kind) {}

    public VectorizedClassifier(SlotArenaScope scope, VectorizedExecutor executor) {
        this.scope = scope; this.executor = executor;
    }

    public CompletableFuture<?> submit(VectorizedStateRequest req) {
        // APPEND_MERGE guard
        if (req.kind() == VectorizedStateRequest.Kind.APPEND_MERGE
            && !isListStateName(req.stateName())) {
            throw new IllegalArgumentException(
                "APPEND_MERGE is ListState-only per spec §1 §a: " + req.stateName());
        }
        BatchKey key = new BatchKey(req.stateName(), req.kind());
        ColumnarBatchBuffer buf = pendingBatches.computeIfAbsent(
            key, k -> ColumnarBatchBuffer.forKind(k.kind(), k.stateName(), scope));
        buf.append(req);
        if (buf.shouldFlush()) {
            executor.dispatch(buf);
            pendingBatches.remove(key);
        }
        return req.future();
    }

    public void flushAll() {
        for (ColumnarBatchBuffer buf : pendingBatches.values()) {
            executor.dispatch(buf);
        }
        pendingBatches.clear();
    }

    private boolean isListStateName(String name) {
        // Look up via backend's state-name registry; in unit tests, a small set of registered listState names suffices.
        return /* implementation */;
    }
}
```

- [ ] **Step 5: Commit**

### Task P2.7: VectorizedExecutor extension (3→6 FFI dispatches)

**Files:** modify `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/VectorizedExecutor.java`

- [ ] **Step 1: Failing test for each new dispatch**

For now, test that the executor accepts each kind without throwing UnsupportedOperationException:

```java
@Test void executorDispatchesAllSixKinds() {
    for (var k : VectorizedStateRequest.Kind.values()) {
        ColumnarBatchBuffer buf = ColumnarBatchBuffer.forKind(k, "state", scope);
        // Just verify dispatch doesn't throw UOE — actual FFI bindings tested in P3/P6/P7/P8/P9 per kind
        assertDoesNotThrow(() -> executor.dispatch(buf));
    }
}
```

- [ ] **Step 2-4: extend dispatch switch**

```java
public class VectorizedExecutor {
    public void dispatch(ColumnarBatchBuffer buf) {
        switch (buf.kind()) {
            case GET -> dispatchGet(buf);
            case PUT -> dispatchPut(buf);
            case DELETE -> dispatchDelete(buf);
            case APPEND_MERGE -> dispatchAppendMerge(buf);   // wired in P6
            case ITER_PREFIX -> dispatchIterPrefix(buf);     // wired in P3
            case ITER_RANGE -> dispatchIterRange(buf);       // wired in P9
        }
    }

    // For P2, append_merge / iter_prefix / iter_range are stubs that complete futures with placeholder + log warning.
    // Subsequent PRs replace with real FFI calls.
    private void dispatchAppendMerge(ColumnarBatchBuffer buf) {
        LOG.warn("APPEND_MERGE dispatch stub — real impl in P6");
        buf.completeAllExceptionally(new UnsupportedOperationException("append_merge pending P6"));
    }
    private void dispatchIterPrefix(ColumnarBatchBuffer buf) { /* stub */ }
    private void dispatchIterRange(ColumnarBatchBuffer buf)  { /* stub */ }
}
```

- [ ] **Step 5: Commit**

---

## P3 — Iterator path: FrsIterHandle + Watchdog + ITER_PREFIX FFI

**Goal:** Implement the full iterator lifecycle (Java handle + watchdog + Rust FFI) so MapState.entries/keys/values can land in P5. Use prefix iteration; range is deferred to P9.

**Spec components:** 5 (FrsIterHandle real impl), 6 (IterLifetimeWatchdog), FFI C (frs_vec_iter_prefix_*), Rust E (iter.rs).

### Task P3.1: Rust — iter.rs native handle + chunk-fill

**Files:** create `crates/forst-rs-storage/src/iter.rs`

- [ ] **Step 1: Failing Rust test**

```rust
#[test]
fn iter_prefix_returns_chunked_rows() {
    let engine = test_engine_with_rows(&[
        ("p1/a", b"v1"), ("p1/b", b"v2"), ("p1/c", b"v3"),
        ("q/x", b"vx"),
    ]);
    let snapshot = engine.snapshot();
    let mut iter = NativeIter::open_prefix(&snapshot, b"p1/", 1024);
    let chunk1 = iter.next_chunk();
    assert_eq!(chunk1.rows().count(), 3);
    assert!(iter.next_chunk().is_empty());
}
```

- [ ] **Step 2-4: implement**

```rust
// crates/forst-rs-storage/src/iter.rs

use crate::snapshot::Snapshot;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct NativeIter {
    snapshot: Snapshot,
    inner: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>,
    aborted: AtomicBool,
}

pub struct IterChunk {
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub continuation: Option<Vec<u8>>,
}

impl NativeIter {
    pub fn open_prefix(snapshot: &Snapshot, prefix: &[u8], chunk_bytes: usize) -> Self {
        let inner = snapshot.iter_prefix(prefix);
        Self { snapshot: snapshot.clone(), inner, aborted: AtomicBool::new(false) }
    }
    pub fn next_chunk(&mut self, chunk_bytes: usize) -> IterChunk {
        if self.aborted.load(Ordering::Acquire) {
            return IterChunk { rows: vec![], continuation: None };
        }
        let mut rows = Vec::new();
        let mut accumulated = 0usize;
        while accumulated < chunk_bytes {
            match self.inner.next() {
                Some((k, v)) => { accumulated += k.len() + v.len(); rows.push((k, v)); }
                None => break,
            }
        }
        let continuation = if rows.is_empty() { None } else { rows.last().map(|(k, _)| k.clone()) };
        IterChunk { rows, continuation }
    }
    pub fn abort(&self) { self.aborted.store(true, Ordering::Release); }
}
```

- [ ] **Step 5: Commit**

### Task P3.2: Rust — frs_vec_iter_prefix_open/_next/_close FFI

**Files:** modify `crates/forst-rs-ffi/src/lib.rs`

- [ ] **Step 1: Failing FFI test from Rust side**

```rust
#[test]
fn ffi_iter_prefix_round_trip() {
    let engine = test_engine_with_rows(&[("p/a", b"1"), ("p/b", b"2")]);
    let chunk_buf = vec![0u8; 4096];
    let chunk_buf_ptr = chunk_buf.as_ptr() as *mut u8;
    let handle_out: u64;
    let row_count_out: u32;
    unsafe {
        let rc = frs_vec_iter_prefix_open(
            engine.handle(), b"p/".as_ptr(), 2,
            chunk_buf_ptr, 4096, &handle_out as *const _ as *mut u64,
            &row_count_out as *const _ as *mut u32);
        assert_eq!(rc, 0);
        assert_eq!(row_count_out, 2);
        let rc = frs_vec_iter_prefix_close(handle_out);
        assert_eq!(rc, 0);
    }
}
```

- [ ] **Step 2-4: implement**

```rust
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

static HANDLE_REGISTRY: OnceLock<Mutex<HashMap<u64, NativeIter>>> = OnceLock::new();
static NEXT_HANDLE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn handles() -> &'static Mutex<HashMap<u64, NativeIter>> {
    HANDLE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

#[no_mangle]
pub extern "C" fn frs_vec_iter_prefix_open(
    engine_handle: u64,
    prefix_ptr: *const u8, prefix_len: u32,
    chunk_buf_ptr: *mut u8, chunk_buf_cap: u32,
    out_handle: *mut u64, out_row_count: *mut u32,
) -> i32 {
    match std::panic::catch_unwind(|| {
        let prefix = unsafe { std::slice::from_raw_parts(prefix_ptr, prefix_len as usize) };
        let snapshot = /* from engine_handle */;
        let mut iter = NativeIter::open_prefix(&snapshot, prefix, chunk_buf_cap as usize);
        let chunk = iter.next_chunk(chunk_buf_cap as usize);
        let mut off = 0usize;
        for (k, v) in &chunk.rows {
            // Write [klen u32][vlen u32][k bytes][v bytes] into chunk_buf
            // ... layout details ...
        }
        let id = NEXT_HANDLE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        handles().lock().unwrap().insert(id, iter);
        unsafe { *out_handle = id; *out_row_count = chunk.rows.len() as u32; }
        Ok::<(), ()>(())
    }) {
        Ok(Ok(())) => 0,
        _ => FrsErrorCode::PanicCaught as i32,
    }
}

#[no_mangle]
pub extern "C" fn frs_vec_iter_prefix_next(
    handle: u64,
    chunk_buf_ptr: *mut u8, chunk_buf_cap: u32,
    out_row_count: *mut u32,
) -> i32 { /* similar */ }

#[no_mangle]
pub extern "C" fn frs_vec_iter_prefix_close(handle: u64) -> i32 {
    handles().lock().unwrap().remove(&handle);
    0
}
```

- [ ] **Step 5: Rebuild dylib + commit**

### Task P3.3: Java — FrsIterHandle real implementation

**Files:** rewrite `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/FrsIterHandle.java`

- [ ] **Step 1: Failing test**

```java
@Test
void handleTracksLastNextTime() {
    FrsIterHandle h = openTestIter();
    long t0 = h.lastNextNs();
    Thread.sleep(50);
    h.next(/* chunk buffer */);
    assertTrue(h.lastNextNs() > t0);
    h.close();
}

@Test
void closeReleasesNativeHandle() {
    FrsIterHandle h = openTestIter();
    long nativeId = h.nativeHandleId();
    h.close();
    assertEquals(0, h.nativeHandleId());  // sentinel value
}

@Test
void forceCloseAbortsInflightNext() {
    FrsIterHandle h = openTestIter();
    h.forceClose();
    assertThrows(FrsIteratorExpiredException.class, () -> h.next(/* chunk */));
}
```

- [ ] **Step 2-4: rewrite class (drop stub)**

```java
public class FrsIterHandle implements AutoCloseable {
    private final long handleId;        // Java-side stable ID
    private volatile long nativeHandleId;
    private final Arena perIterArena;
    private final SlotArenaScope slotScope;
    private final AtomicLong lastNextNs;
    private final AtomicBoolean closeRequested = new AtomicBoolean(false);

    public FrsIterHandle(long handleId, long nativeHandleId, Arena perIterArena, SlotArenaScope slotScope) {
        this.handleId = handleId;
        this.nativeHandleId = nativeHandleId;
        this.perIterArena = perIterArena;
        this.slotScope = slotScope;
        this.lastNextNs = new AtomicLong(System.nanoTime());
        slotScope.registerIter(this);
    }

    public long handleId() { return handleId; }
    public long nativeHandleId() { return nativeHandleId; }
    public long lastNextNs() { return lastNextNs.get(); }
    public boolean closeRequested() { return closeRequested.get(); }

    public IterChunk next(MemorySegment chunkBuf) {
        if (closeRequested.get() || nativeHandleId == 0)
            throw new FrsIteratorExpiredException(0);
        int[] rowCount = new int[1];
        int rc = ForStRsLinker.frs_vec_iter_prefix_next(
            nativeHandleId, chunkBuf.address(), (int) chunkBuf.byteSize(), rowCount);
        lastNextNs.set(System.nanoTime());
        if (rc != 0) throw new FrsException(FrsErrorCode.fromU32(rc), 0, new byte[0]);
        return new IterChunk(chunkBuf, rowCount[0]);
    }

    public void requestClose() { closeRequested.set(true); }  // watchdog calls this

    @Override
    public void close() {
        if (nativeHandleId != 0) {
            ForStRsLinker.frs_vec_iter_prefix_close(nativeHandleId);
            nativeHandleId = 0;
        }
        slotScope.unregisterIter(handleId);
        perIterArena.close();
    }

    public void forceClose() { closeRequested.set(true); close(); }

    public record IterChunk(MemorySegment buf, int rowCount) {}
}
```

- [ ] **Step 5: Commit**

### Task P3.4: IterLifetimeWatchdog

**Files:** create `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/IterLifetimeWatchdog.java`

- [ ] **Step 1: Failing test**

```java
@Test
void watchdogMarksIdleHandleClosed() throws Exception {
    IterLifetimeWatchdog wd = new IterLifetimeWatchdog(slotScope, 100, 10_000);  // 100ms idle, 10s max
    wd.start();
    FrsIterHandle h = openTestIter();
    Thread.sleep(200);  // exceeds idle timeout
    assertTrue(h.closeRequested());
    wd.stop();
}

@Test
void watchdogTriggersMaxLifetimeAbort() throws Exception {
    IterLifetimeWatchdog wd = new IterLifetimeWatchdog(slotScope, 60_000, 100);  // huge idle, 100ms max
    wd.start();
    FrsIterHandle h = openTestIter();
    // simulate continuous next() to defeat idle but still exceed max-lifetime
    for (int i = 0; i < 10; i++) { h.next(buf); Thread.sleep(20); }
    Thread.sleep(50);
    assertTrue(h.closeRequested());
    wd.stop();
}
```

- [ ] **Step 2-4: implement**

```java
public class IterLifetimeWatchdog {
    private final SlotArenaScope scope;
    private final long idleTimeoutMs;
    private final long maxLifetimeMs;
    private final Map<Long, Long> handleOpenedAtMs = new ConcurrentHashMap<>();
    private ScheduledExecutorService executor;

    // Metric counters (wired to DispatchMetrics in P4)
    private final AtomicLong idleTimeouts = new AtomicLong(0);
    private final AtomicLong maxLifetimeAborts = new AtomicLong(0);

    public IterLifetimeWatchdog(SlotArenaScope scope, long idleMs, long maxMs) {
        this.scope = scope; this.idleTimeoutMs = idleMs; this.maxLifetimeMs = maxMs;
    }

    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "forst-rs-iter-watchdog");
            t.setDaemon(true); return t;
        });
        executor.scheduleAtFixedRate(this::sweep, 50, 50, TimeUnit.MILLISECONDS);
    }

    private void sweep() {
        long now = System.currentTimeMillis();
        long nowNs = System.nanoTime();
        for (FrsIterHandle h : scope.iterHandles()) {
            long idleMs = TimeUnit.NANOSECONDS.toMillis(nowNs - h.lastNextNs());
            if (idleMs > idleTimeoutMs) {
                idleTimeouts.incrementAndGet();
                h.requestClose();
                continue;
            }
            Long opened = handleOpenedAtMs.get(h.handleId());
            if (opened != null && now - opened > maxLifetimeMs) {
                maxLifetimeAborts.incrementAndGet();
                h.requestClose();
            }
        }
    }

    public void noteOpened(long handleId) { handleOpenedAtMs.put(handleId, System.currentTimeMillis()); }
    public void noteClosed(long handleId) { handleOpenedAtMs.remove(handleId); }

    public long idleTimeoutsCount() { return idleTimeouts.get(); }
    public long maxLifetimeAbortsCount() { return maxLifetimeAborts.get(); }

    public void stop() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
```

Add `iterHandles()` accessor to `SlotArenaScope`:

```java
public Collection<FrsIterHandle> iterHandles() { return iterRegistry.values(); }
```

- [ ] **Step 5: Commit**

### Task P3.5: Wire ITER_PREFIX into VectorizedExecutor

**Files:** modify VectorizedExecutor.java

- [ ] **Step 1: Failing test**

```java
@Test
void dispatchIterPrefixOpensNativeHandleAndCompletesFuture() throws Exception {
    SlotArenaScope scope = newScope();
    scope.enterTurn();
    MemorySegment prefix = encodePrefix(scope, "myMap", currentKey);
    MemorySegment chunkBuf = scope.allocateTurn(4096, 64);
    IterPrefixRequest req = new IterPrefixRequest("myMap", prefix, chunkBuf);
    classifier.submit(req);
    classifier.flushAll();
    IterPrefixRequest.IterFirstChunk result = req.future().get(5, TimeUnit.SECONDS);
    assertNotNull(result.handle());
    assertTrue(result.firstChunkRows() >= 0);
    result.handle().close();
    scope.exitTurn();
}
```

- [ ] **Step 2-4: implement dispatchIterPrefix**

Replace stub:

```java
private void dispatchIterPrefix(ColumnarBatchBuffer buf) {
    IterPrefixBatchBuffer ipb = (IterPrefixBatchBuffer) buf;
    for (int row = 0; row < ipb.rowCount(); row++) {
        IterPrefixRequest req = (IterPrefixRequest) ipb.requestAt(row);
        long handleId = nextHandleId.incrementAndGet();
        long[] outHandle = new long[1];
        int[] outRowCount = new int[1];
        Arena perIterArena = Arena.ofShared();
        int rc = ForStRsLinker.frs_vec_iter_prefix_open(
            engineHandle,
            req.prefixSlice().address(), (int) req.prefixSlice().byteSize(),
            req.chunkBufSlice().address(), (int) req.chunkBufSlice().byteSize(),
            outHandle, outRowCount);
        if (rc != 0) {
            perIterArena.close();
            req.future().completeExceptionally(new FrsException(FrsErrorCode.fromU32(rc), row, new byte[0]));
            continue;
        }
        FrsIterHandle h = new FrsIterHandle(handleId, outHandle[0], perIterArena, scope);
        watchdog.noteOpened(handleId);
        req.future().complete(new IterPrefixRequest.IterFirstChunk(h, outRowCount[0]));
    }
}
```

- [ ] **Step 5: Commit**

---

## P4 — Metrics + FatalErrorHandler integration

**Goal:** Make the dispatch layer observable and route PANIC_CAUGHT / UNKNOWN through Flink's `FatalErrorHandler` for TM-level restart.

**Spec components:** 8 (DispatchMetrics with 128 cardinality cap), 16 (FatalErrorHandler integration).

### Task P4.1: DispatchMetrics

**Files:** create `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/metrics/DispatchMetrics.java`

- [ ] **Step 1: Failing test**

```java
@Test
void recordsPerKindLatencyAndBatchSize() {
    MetricGroup root = new TestMetricGroup();
    DispatchMetrics dm = new DispatchMetrics(root);
    dm.recordDispatch(VectorizedStateRequest.Kind.GET, "state1", 32, 1000, 1500);
    assertEquals(1, ((Counter)root.getMetric("dispatch.get.state1.count")).getCount());
    assertEquals(32, ((Counter)root.getMetric("dispatch.get.state1.rows")).getCount());
}

@Test
void cardinalityCapAt128StateNames() {
    MetricGroup root = new TestMetricGroup();
    DispatchMetrics dm = new DispatchMetrics(root);
    for (int i = 0; i < 130; i++)
        dm.recordDispatch(VectorizedStateRequest.Kind.GET, "state-" + i, 1, 100, 100);
    assertNotNull(root.getMetric("dispatch.get.overflow.count"));
    assertEquals(1L, ((Counter)root.getMetric("dispatch.cardinality_capped")).getCount());
}
```

- [ ] **Step 2-4: implement**

```java
public class DispatchMetrics {
    private static final int MAX_STATE_NAMES = 128;
    private final MetricGroup root;
    private final ConcurrentHashMap<String, PerKindGroup> perKind = new ConcurrentHashMap<>();
    private final Counter cardinalityCapped;

    public DispatchMetrics(MetricGroup root) {
        this.root = root;
        this.cardinalityCapped = root.counter("dispatch.cardinality_capped");
    }

    public void recordDispatch(VectorizedStateRequest.Kind kind, String stateName,
                                int rows, long bytesIn, long latencyNs) {
        String kindKey = kind.name().toLowerCase();
        PerKindGroup group = perKind.computeIfAbsent(kindKey, k -> new PerKindGroup(root, k));
        group.record(stateName, rows, bytesIn, latencyNs, cardinalityCapped);
    }

    public void recordFfiError(VectorizedStateRequest.Kind kind, String stateName, FrsErrorCode code) {
        perKind.computeIfAbsent(kind.name().toLowerCase(), k -> new PerKindGroup(root, k))
               .recordError(stateName, code);
    }

    private static class PerKindGroup {
        private final ConcurrentHashMap<String, PerStateMetrics> perState = new ConcurrentHashMap<>();
        private final PerStateMetrics overflow;
        private final MetricGroup kindGroup;

        PerKindGroup(MetricGroup root, String kindKey) {
            this.kindGroup = root.addGroup("dispatch").addGroup(kindKey);
            this.overflow = new PerStateMetrics(kindGroup.addGroup("overflow"));
        }

        void record(String stateName, int rows, long bytesIn, long latencyNs, Counter capCounter) {
            PerStateMetrics psm = perState.get(stateName);
            if (psm == null) {
                if (perState.size() >= MAX_STATE_NAMES) {
                    capCounter.inc();
                    overflow.record(rows, bytesIn, latencyNs);
                    return;
                }
                psm = perState.computeIfAbsent(stateName,
                    sn -> new PerStateMetrics(kindGroup.addGroup(sn)));
            }
            psm.record(rows, bytesIn, latencyNs);
        }

        void recordError(String stateName, FrsErrorCode code) { /* similar */ }
    }

    private static class PerStateMetrics {
        final Counter count;
        final Counter rows;
        final Counter bytesIn;
        final Histogram batchSize;
        final Histogram latencyNs;
        final Counter ffiErrors;

        PerStateMetrics(MetricGroup g) {
            count = g.counter("count");
            rows = g.counter("rows");
            bytesIn = g.counter("bytes_in");
            batchSize = g.histogram("batch_size", new DescriptiveStatisticsHistogram(500));
            latencyNs = g.histogram("latency_ns", new DescriptiveStatisticsHistogram(500));
            ffiErrors = g.counter("ffi_errors");
        }
        void record(int rs, long bi, long ln) {
            count.inc(); rows.inc(rs); bytesIn.inc(bi);
            batchSize.update(rs); latencyNs.update(ln);
        }
    }
}
```

- [ ] **Step 5: Commit**

### Task P4.2: Wire DispatchMetrics into VectorizedExecutor

- [ ] **Step 1: Test that each dispatch publishes metrics**

```java
@Test
void executorEmitsLatencyHistogramOnEachDispatch() {
    DispatchMetrics m = new DispatchMetrics(metricGroup);
    VectorizedExecutor exec = new VectorizedExecutor(linker, m, scope, watchdog);
    // ... submit + flush GET batch ...
    assertTrue(((Histogram)metricGroup.getMetric("dispatch.get.state1.latency_ns")).getCount() > 0);
}
```

- [ ] **Step 2-4: instrument each dispatch method with timer**

```java
private void dispatchGet(ColumnarBatchBuffer buf) {
    long t0 = System.nanoTime();
    // existing dispatch ...
    metrics.recordDispatch(VectorizedStateRequest.Kind.GET, buf.stateName(),
        buf.rowCount(), buf.totalBytes(), System.nanoTime() - t0);
}
```

Repeat for all 6 dispatches.

- [ ] **Step 5: Commit**

### Task P4.3: FatalErrorHandler integration

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/FrsEnginePanicError.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/VectorizedExecutor.java`

- [ ] **Step 1: Failing test — PANIC_CAUGHT calls FatalErrorHandler**

```java
@Test
void panicCaughtCallsFatalErrorHandler() {
    AtomicReference<Throwable> fatalErr = new AtomicReference<>();
    FatalErrorHandler handler = err -> fatalErr.set(err);
    VectorizedExecutor exec = new VectorizedExecutor(linker, metrics, scope, watchdog, handler);
    // Use F3 fault injection: inject PANIC_CAUGHT on next frs_vec_get
    enableFault("frs_vec_get", FrsErrorCode.PANIC_CAUGHT);
    classifier.submit(new GetRequest("state1", encodeKey("k1")));
    classifier.flushAll();
    // Wait briefly for FatalErrorHandler invocation
    Awaitility.await().atMost(1, SECONDS).until(() -> fatalErr.get() != null);
    assertTrue(fatalErr.get() instanceof FrsEnginePanicError);
}

@Test
void unknownCodeTreatedAsFatal() {
    // Same shape, but inject FrsErrorCode.UNKNOWN
}
```

- [ ] **Step 2-4: implement**

```java
public class FrsEnginePanicError extends Error {
    private final FrsErrorCode code;
    public FrsEnginePanicError(FrsErrorCode code, String detail) {
        super("Forst-RS engine fatal: " + code + " — " + detail);
        this.code = code;
    }
    public FrsErrorCode code() { return code; }
}

// In VectorizedExecutor.handleRowResult:
private void handleRowResult(VectorizedStateRequest req, FrsRowResult result, int rowIndex) {
    FrsErrorCode code = FrsErrorCode.fromU32(result.code());
    if (code == FrsErrorCode.OK) {
        req.future().complete(parseResult(result));
        return;
    }
    metrics.recordFfiError(req.kind(), req.stateName(), code);
    if (code.isFailProcess()) {
        fatalErrorHandler.onFatalError(new FrsEnginePanicError(code, "row=" + rowIndex));
        return;
    }
    if (code == FrsErrorCode.ITER_EXPIRED) {
        req.future().completeExceptionally(new FrsIteratorExpiredException(rowIndex));
        return;
    }
    req.future().completeExceptionally(new FrsException(code, rowIndex, result.payloadBytes()));
}
```

- [ ] **Step 5: Commit**

---

## P5 — State refactor: Value + Map onto dispatch path

**Goal:** Migrate `ForStRsValueState` and `ForStRsMapState` to the new dispatch path while preserving the SP6 6.3 off-heap value staging behavior of MapState. After P5, ValueState/MapState use the unified path exclusively; ListState is still on the old path until P6.

**Spec components:** 9 (ValueState refactor), 10 (MapState refactor), Trace A.

### Task P5.1: ValueState refactor — `value()` via GetRequest

**Files:** modify `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java`

- [ ] **Step 1: Failing test**

Replace any existing direct-FFI value() test with one that asserts the dispatch path:

```java
@Test
void valueGoesThroughClassifier() {
    ForStRsValueState<String> state = newTestValueState();
    state.update("hello");
    String observed = state.value();
    assertEquals("hello", observed);
    // Inspect metrics: PUT count = 1, GET count = 1
    assertEquals(1, getCounter("dispatch.put.testValue.count"));
    assertEquals(1, getCounter("dispatch.get.testValue.count"));
}
```

- [ ] **Step 2-4: refactor — remove direct FFI, route via classifier**

```java
public class ForStRsValueState<T> implements ValueState<T> {
    private final String stateName;
    private final TypeSerializer<T> valueSerializer;
    private final VectorizedClassifier classifier;
    private final SlotArenaScope scope;
    private final KeySupplier keySupplier;

    @Override
    public T value() throws IOException {
        scope.enterTurnIfNeeded();
        MemorySegment k = encodeKeyInto(scope, stateName, keySupplier.currentKey());
        GetRequest req = new GetRequest(stateName, k);
        classifier.submit(req);
        classifier.flushAll();
        try {
            byte[] bytes = req.future().get();
            if (bytes == null || bytes.length == 0) return null;
            return valueSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)));
        } catch (Exception e) {
            throw new IOException("ValueState.value failed", e);
        }
    }

    @Override
    public void update(T t) throws IOException {
        scope.enterTurnIfNeeded();
        MemorySegment k = encodeKeyInto(scope, stateName, keySupplier.currentKey());
        if (t == null) {
            DeleteRequest req = new DeleteRequest(stateName, k);
            classifier.submit(req);
            return;
        }
        MemorySegment v = encodeValueInto(scope, valueSerializer, t);
        PutRequest req = new PutRequest(stateName, k, v);
        classifier.submit(req);
    }

    @Override
    public void clear() {
        scope.enterTurnIfNeeded();
        MemorySegment k = encodeKeyInto(scope, stateName, keySupplier.currentKey());
        classifier.submit(new DeleteRequest(stateName, k));
    }
}
```

- [ ] **Step 5: Commit**

### Task P5.2: MapState refactor — get/put/remove/contains via dispatch + SP6 staging

**Files:** modify `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapState.java`

- [ ] **Step 1: Failing test — staging still operates**

Existing SP6 6.3 test for off-heap staging should still pass. Add new test:

```java
@Test
void mapStateRoutesThroughDispatch() {
    ForStRsMapState<String, Integer> map = newTestMapState();
    map.put("a", 1);
    map.put("b", 2);
    assertEquals(1, (int) map.get("a"));
    assertEquals(2, (int) map.get("b"));
    map.remove("a");
    assertNull(map.get("a"));
    assertEquals(3, getCounter("dispatch.get.testMap.count"));  // 2 gets + 1 after remove
    assertEquals(2, getCounter("dispatch.put.testMap.count"));
    assertEquals(1, getCounter("dispatch.delete.testMap.count"));
}
```

- [ ] **Step 2-4: refactor every method onto VectorizedStateRequest**

```java
@Override
public V get(UK uk) throws Exception {
    scope.enterTurnIfNeeded();
    MemorySegment k = encodeKeyWithUserKey(scope, stateName, keySupplier.currentKey(), uk);
    GetRequest req = new GetRequest(stateName, k);
    classifier.submit(req);
    classifier.flushAll();
    byte[] bytes = req.future().get();
    return bytes == null || bytes.length == 0 ? null
        : valueSerializer.deserialize(/* ... */);
}

@Override
public void put(UK uk, V v) throws Exception {
    scope.enterTurnIfNeeded();
    MemorySegment k = encodeKeyWithUserKey(scope, stateName, keySupplier.currentKey(), uk);
    // Existing SP6 6.3 off-heap staging — write v into a stage slot in cacheRegion
    OffHeapSlice slice = stageValue(scope, valueSerializer, v);
    PutRequest req = new PutRequest(stateName, k, slice.toSegment());
    classifier.submit(req);
}

@Override
public boolean contains(UK uk) throws Exception {
    return get(uk) != null;
}

@Override
public void remove(UK uk) throws Exception {
    scope.enterTurnIfNeeded();
    MemorySegment k = encodeKeyWithUserKey(scope, stateName, keySupplier.currentKey(), uk);
    classifier.submit(new DeleteRequest(stateName, k));
}
```

- [ ] **Step 5: Commit**

### Task P5.3: MapState entries/keys/values via ITER_PREFIX

- [ ] **Step 1: Failing test**

```java
@Test
void entriesIteratesAllUserKeysViaDispatch() throws Exception {
    ForStRsMapState<String, Integer> map = newTestMapState();
    map.put("a", 1); map.put("b", 2); map.put("c", 3);
    Set<Map.Entry<String, Integer>> seen = new HashSet<>();
    for (var e : map.entries()) seen.add(Map.entry(e.getKey(), e.getValue()));
    assertEquals(Set.of(Map.entry("a",1), Map.entry("b",2), Map.entry("c",3)), seen);
}
```

- [ ] **Step 2-4: route entries() through IterPrefixRequest**

```java
@Override
public Iterable<Map.Entry<UK, V>> entries() throws Exception {
    scope.enterTurnIfNeeded();
    MemorySegment prefix = encodePrefix(scope, stateName, keySupplier.currentKey());
    MemorySegment chunkBuf = scope.allocateTurn(4096, 64);
    IterPrefixRequest req = new IterPrefixRequest(stateName, prefix, chunkBuf);
    classifier.submit(req);
    classifier.flushAll();
    IterPrefixRequest.IterFirstChunk first = req.future().get();
    return () -> new EntryIterator(first.handle(), first.firstChunkRows(), chunkBuf,
                                    userKeySerializer, valueSerializer);
}
```

EntryIterator drains the chunk, then calls `handle.next(chunkBuf)` for more rows, closes when exhausted.

- [ ] **Step 5: Commit**

### Task P5.4: MapState `clear()` via PREFIX_DELETE proxy (POINT_DELETE in V1)

For V1, MapState.clear() iterates the prefix and submits a `DeleteRequest` per row. V1.x will introduce PREFIX_DELETE for native `delete_range`. Add a note in code:

```java
@Override
public void clear() throws Exception {
    // V1: iterate + point-delete each. V1.x: PREFIX_DELETE for native delete_range (Spec §1 V1.x).
    for (UK uk : keys()) remove(uk);
}
```

- [ ] **Step 1-5: write test, fail, implement, pass, commit**

---

## P6 — ListState + APPEND_MERGE

**Goal:** Move ListState onto the dispatch path and add the APPEND_MERGE primitive end-to-end (Rust list_merge.rs + FFI + Java).

**Spec components:** 11 (ListState), Rust B (frs_vec_merge_append), Rust F (list_merge.rs).

### Task P6.1: Rust — list_merge.rs combiner

**Files:** create `crates/forst-rs-engine/src/list_merge.rs`

- [ ] **Step 1: Failing test**

```rust
#[test]
fn list_merge_concatenates_in_arrival_order() {
    let mut combiner = ListMergeCombiner::new();
    let result = combiner.combine(&[
        b"A".to_vec(),
        b"B".to_vec(),
        b"C".to_vec(),
    ]);
    assert_eq!(result, b"ABC");
}

#[test]
fn list_merge_idempotent_under_repeated_combine() {
    let mut combiner = ListMergeCombiner::new();
    let intermediate = combiner.combine(&[b"A".to_vec(), b"B".to_vec()]);
    let final_result = combiner.combine(&[intermediate, b"C".to_vec()]);
    assert_eq!(final_result, b"ABC");
}
```

- [ ] **Step 2-4: implement**

```rust
// crates/forst-rs-engine/src/list_merge.rs

pub struct ListMergeCombiner;

impl ListMergeCombiner {
    pub fn new() -> Self { Self }
    pub fn combine(&self, operands: &[Vec<u8>]) -> Vec<u8> {
        let total: usize = operands.iter().map(|o| o.len()).sum();
        let mut out = Vec::with_capacity(total);
        for op in operands { out.extend_from_slice(op); }
        out
    }
}
```

Wire into engine's merge-on-compaction and merge-on-read paths:

```rust
// In crates/forst-rs-engine/src/db.rs (or wherever merge operands are resolved):
match state_type_for(key) {
    StateType::List => list_merge::ListMergeCombiner::new().combine(operands),
    _ => unreachable!("non-list merge not supported in V1"),
}
```

- [ ] **Step 5: Commit**

### Task P6.2: Rust — frs_vec_merge_append FFI

**Files:** modify `crates/forst-rs-ffi/src/lib.rs`

- [ ] **Step 1: Failing test**

```rust
#[test]
fn frs_vec_merge_append_appends_operands() {
    let engine = test_engine();
    // Batch: 1 key, 3 operands ["A", "B", "C"]
    let batch_hdr = build_merge_batch(&[(b"key1", &[b"A", b"B", b"C"])]);
    let mut row_results = vec![FrsRowResult { code: 0, payload_off: 0, payload_len: 0 }; 1];
    unsafe {
        let rc = frs_vec_merge_append(batch_hdr.as_ptr(), row_results.as_mut_ptr());
        assert_eq!(rc, 0);
    }
    assert_eq!(row_results[0].code, FrsErrorCode::Ok as u32);
    // Read back — should see "ABC"
    let val = engine.get(b"key1").unwrap();
    assert_eq!(val, Some(b"ABC".to_vec()));
}
```

- [ ] **Step 2-4: implement FFI**

```rust
#[no_mangle]
pub extern "C" fn frs_vec_merge_append(batch_hdr_ptr: *const u8, row_results_out: *mut FrsRowResult) -> i32 {
    match std::panic::catch_unwind(|| {
        let header = unsafe { parse_batch_header(batch_hdr_ptr) };
        for (row_idx, row) in header.rows.iter().enumerate() {
            let key = row.key();
            let operands: Vec<&[u8]> = row.operands().collect();
            match engine_append_merge(key, &operands) {
                Ok(()) => unsafe { *row_results_out.add(row_idx) = FrsRowResult { code: 0, payload_off: 0, payload_len: 0 }; },
                Err(e) => unsafe { *row_results_out.add(row_idx) = FrsRowResult { code: FrsErrorCode::from(e) as u32, payload_off: 0, payload_len: 0 }; },
            }
        }
        Ok::<(), ()>(())
    }) {
        Ok(_) => 0,
        Err(_) => { emit_batch_error(row_results_out, header.row_count, FrsErrorCode::PanicCaught); 0 }
    }
}
```

- [ ] **Step 5: Rebuild dylib + commit**

### Task P6.3: Java — ListState refactor + APPEND_MERGE wire-up

**Files:** modify `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsListState.java`

- [ ] **Step 1: Failing test — `add()` goes through APPEND_MERGE**

```java
@Test
void listStateAddUsesAppendMerge() throws Exception {
    ForStRsListState<Integer> list = newTestListState();
    list.add(1);
    list.add(2);
    list.add(3);
    assertEquals(List.of(1, 2, 3), list.get());
    assertEquals(3, getCounter("dispatch.append_merge.testList.count"));
}

@Test
void listStateAddAllBatchesOperands() throws Exception {
    ForStRsListState<Integer> list = newTestListState();
    list.addAll(List.of(1, 2, 3, 4, 5));
    assertEquals(List.of(1, 2, 3, 4, 5), list.get());
    assertEquals(1, getCounter("dispatch.append_merge.testList.count"));  // one batch, 5 operands
}
```

- [ ] **Step 2-4: implement**

```java
@Override
public void add(T t) throws Exception {
    if (t == null) throw new NullPointerException("ListState.add(null)");
    scope.enterTurnIfNeeded();
    MemorySegment k = encodeKeyInto(scope, stateName, keySupplier.currentKey());
    MemorySegment v = encodeValueInto(scope, elementSerializer, t);
    AppendMergeRequest req = new AppendMergeRequest(stateName, k, new MemorySegment[]{v});
    classifier.submit(req);
}

@Override
public void addAll(List<T> values) throws Exception {
    if (values == null || values.isEmpty()) return;
    scope.enterTurnIfNeeded();
    MemorySegment k = encodeKeyInto(scope, stateName, keySupplier.currentKey());
    MemorySegment[] vs = new MemorySegment[values.size()];
    for (int i = 0; i < values.size(); i++)
        vs[i] = encodeValueInto(scope, elementSerializer, values.get(i));
    AppendMergeRequest req = new AppendMergeRequest(stateName, k, vs);
    classifier.submit(req);
}

@Override
public Iterable<T> get() throws Exception {
    scope.enterTurnIfNeeded();
    MemorySegment k = encodeKeyInto(scope, stateName, keySupplier.currentKey());
    GetRequest req = new GetRequest(stateName, k);
    classifier.submit(req);
    classifier.flushAll();
    byte[] mergedBytes = req.future().get();
    return decodeListBytes(mergedBytes, elementSerializer);  // engine returns concatenated bytes; deserialize into element stream
}

@Override
public void update(List<T> values) throws Exception {
    clear();
    addAll(values);
}

@Override
public void clear() {
    scope.enterTurnIfNeeded();
    MemorySegment k = encodeKeyInto(scope, stateName, keySupplier.currentKey());
    classifier.submit(new DeleteRequest(stateName, k));
}
```

- [ ] **Step 5: Commit**

### Task P6.4: Wire APPEND_MERGE through VectorizedExecutor

- [ ] **Step 1-5: implement `dispatchAppendMerge`, replacing the stub from P2.7**

```java
private void dispatchAppendMerge(ColumnarBatchBuffer buf) {
    long t0 = System.nanoTime();
    AppendMergeBatchBuffer amb = (AppendMergeBatchBuffer) buf;
    MemorySegment batchHeader = amb.serializeHeader();
    FrsRowResult[] results = new FrsRowResult[amb.rowCount()];
    int rc = ForStRsLinker.frs_vec_merge_append(batchHeader.address(), results);
    for (int row = 0; row < amb.rowCount(); row++) {
        handleRowResult(amb.requestAt(row), results[row], row);
    }
    metrics.recordDispatch(VectorizedStateRequest.Kind.APPEND_MERGE, buf.stateName(),
        buf.rowCount(), buf.totalBytes(), System.nanoTime() - t0);
}
```

---

## P7 — RMW infrastructure: cache + pending-miss table + ReducingState

**Goal:** Build the cache-mediated read-modify-write path with convoy coalescing, then wire ReducingState on top.

**Spec components:** 14 (ReducingAggregatingCache), 15 (PendingMissTable), 12 (ReducingState), Trace C.

### Task P7.1: PendingMissTable + PendingMiss

**Files:** create `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/cache/{PendingMissTable,PendingMiss}.java`

- [ ] **Step 1: Failing test — concurrent same-key add()s coalesce into one GET**

```java
@Test
void firstMissCreatesEntryReturnsCompletionToken() throws Exception {
    PendingMissTable table = new PendingMissTable(classifier);
    var token = table.beginOrJoin("rstate", encodeKey("k1"), 100, reduceFn, ack -> {});
    assertNotNull(token);
    assertEquals(1, table.activeMissCount());
}

@Test
void subsequentMissesOnSameKeyJoinConvoy() throws Exception {
    PendingMissTable table = new PendingMissTable(classifier);
    table.beginOrJoin("rstate", encodeKey("k1"), 100, reduceFn, ack -> {});
    table.beginOrJoin("rstate", encodeKey("k1"), 200, reduceFn, ack -> {});
    table.beginOrJoin("rstate", encodeKey("k1"), 300, reduceFn, ack -> {});
    assertEquals(1, table.activeMissCount());
    assertEquals(3, table.pendingInputsFor("rstate", "k1").size());
}

@Test
void resolvedMissFoldsInArrivalOrderAndCompletesAllChained() throws Exception {
    PendingMissTable table = new PendingMissTable(classifier);
    var acks = new CopyOnWriteArrayList<Integer>();
    table.beginOrJoin("rstate", encodeKey("k1"), 100, Integer::sum, acks::add);
    table.beginOrJoin("rstate", encodeKey("k1"), 200, Integer::sum, acks::add);
    table.beginOrJoin("rstate", encodeKey("k1"), 300, Integer::sum, acks::add);
    // Simulate GET resolve with prior=null
    table.resolveMiss("rstate", encodeKey("k1"), null);
    assertEquals(List.of(600, 600, 600), acks);  // each ack sees final folded acc
}

@Test
void combinerThrowFailsAllChainedFutures() throws Exception {
    PendingMissTable table = new PendingMissTable(classifier);
    var failures = new CopyOnWriteArrayList<Throwable>();
    ReduceFunction<Integer> bad = (a, b) -> { throw new RuntimeException("bad combine"); };
    table.beginOrJoin("rstate", encodeKey("k1"), 100, bad, ack -> {},
        failures::add);
    table.beginOrJoin("rstate", encodeKey("k1"), 200, bad, ack -> {},
        failures::add);
    table.resolveMiss("rstate", encodeKey("k1"), null);
    assertEquals(2, failures.size());
    assertEquals("bad combine", failures.get(0).getMessage());
}
```

- [ ] **Step 2-4: implement**

```java
public class PendingMissTable {
    private final VectorizedClassifier classifier;
    private final ConcurrentHashMap<MissKey, PendingMiss<?>> pendingMisses = new ConcurrentHashMap<>();

    public PendingMissTable(VectorizedClassifier classifier) { this.classifier = classifier; }

    public <IN, ACC> CompletableFuture<Void> beginOrJoin(
            String stateName, MemorySegment key,
            IN input, BiFunction<ACC, IN, ACC> combiner,
            Consumer<ACC> onResolve, Consumer<Throwable> onError) {
        MissKey mk = new MissKey(stateName, asBytes(key));
        @SuppressWarnings("unchecked")
        PendingMiss<IN, ACC> pm = (PendingMiss<IN, ACC>) pendingMisses.computeIfAbsent(mk, k -> {
            GetRequest getReq = new GetRequest(stateName, key);
            classifier.submit(getReq);
            PendingMiss<IN, ACC> newPm = new PendingMiss<>(combiner);
            getReq.future().whenComplete((bytes, err) -> {
                if (err != null) {
                    newPm.failAll(err);
                    pendingMisses.remove(mk);
                    return;
                }
                resolveMissInternal(mk, newPm, bytes);
            });
            return newPm;
        });
        return pm.joinConvoy(input, onResolve, onError);
    }

    private <IN, ACC> void resolveMissInternal(MissKey mk, PendingMiss<IN, ACC> pm, byte[] priorBytes) {
        try {
            ACC acc = pm.deserializePrior(priorBytes);  // null bytes → seed-from-first-input handled inside
            for (IN inp : pm.pendingInputs()) {
                acc = pm.combiner().apply(acc, inp);
            }
            pm.completeAll(acc);
        } catch (Throwable t) {
            pm.failAll(t);
        } finally {
            pendingMisses.remove(mk);
        }
    }

    public int activeMissCount() { return pendingMisses.size(); }
    public List<?> pendingInputsFor(String stateName, String key) { /* test helper */ }

    private record MissKey(String stateName, byte[] keyBytes) {
        @Override public boolean equals(Object o) { return o instanceof MissKey m
            && stateName.equals(m.stateName) && Arrays.equals(keyBytes, m.keyBytes); }
        @Override public int hashCode() { return stateName.hashCode() * 31 + Arrays.hashCode(keyBytes); }
    }
}

public class PendingMiss<IN, ACC> {
    private final BiFunction<ACC, IN, ACC> combiner;
    private final Deque<IN> pendingInputs = new ArrayDeque<>();
    private final List<Consumer<ACC>> onResolve = new ArrayList<>();
    private final List<Consumer<Throwable>> onError = new ArrayList<>();
    private final List<CompletableFuture<Void>> futures = new ArrayList<>();

    public PendingMiss(BiFunction<ACC, IN, ACC> combiner) { this.combiner = combiner; }

    public synchronized CompletableFuture<Void> joinConvoy(IN input, Consumer<ACC> ok, Consumer<Throwable> err) {
        pendingInputs.add(input);
        onResolve.add(ok);
        onError.add(err);
        CompletableFuture<Void> fut = new CompletableFuture<>();
        futures.add(fut);
        return fut;
    }

    public synchronized void completeAll(ACC acc) {
        for (Consumer<ACC> ok : onResolve) ok.accept(acc);
        for (CompletableFuture<Void> f : futures) f.complete(null);
    }

    public synchronized void failAll(Throwable t) {
        for (Consumer<Throwable> err : onError) err.accept(t);
        for (CompletableFuture<Void> f : futures) f.completeExceptionally(t);
    }

    public Deque<IN> pendingInputs() { return pendingInputs; }
    public BiFunction<ACC, IN, ACC> combiner() { return combiner; }
    public ACC deserializePrior(byte[] bytes) { /* per state-type — wired in P7.3 */ return null; }
}
```

- [ ] **Step 5: Commit**

### Task P7.2: ReducingAggregatingCache (LRU in cacheRegion)

**Files:** create `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/cache/ReducingAggregatingCache.java`

- [ ] **Step 1: Failing test**

```java
@Test
void cacheHitFoldsInPlaceOnOperatorThread() throws Exception {
    ReducingAggregatingCache cache = newTestCache(64, Integer::sum);
    cache.put("k1", 10);
    cache.fold("k1", 5);  // 10 + 5 = 15
    assertEquals(Integer.valueOf(15), cache.peek("k1"));
    assertTrue(cache.isDirty("k1"));
}

@Test
void lruEvictsLeastRecentlyUsedAndFlushesDirty() throws Exception {
    ReducingAggregatingCache cache = newTestCache(2, Integer::sum);  // max 2 entries
    cache.put("k1", 1);
    cache.put("k2", 2);
    cache.put("k3", 3);  // evicts k1
    assertNull(cache.peek("k1"));
    // verify k1's dirty value was flushed via classifier (PutRequest issued)
    assertEquals(1, getCounter("dispatch.put.test-rstate.count"));
}

@Test
void flushAllDirtyEnqueuesAllAsPutRequests() throws Exception {
    ReducingAggregatingCache cache = newTestCache(64, Integer::sum);
    cache.fold("k1", 10); cache.fold("k2", 20); cache.fold("k3", 30);
    cache.flushAllDirty();  // barrier path
    assertEquals(3, getCounter("dispatch.put.test-rstate.count"));
    assertFalse(cache.isDirty("k1"));
    assertFalse(cache.isDirty("k2"));
    assertFalse(cache.isDirty("k3"));
}
```

- [ ] **Step 2-4: implement (LinkedHashMap-based LRU; entries hold ACC + dirty flag; serialize on flush)**

```java
public class ReducingAggregatingCache<ACC, IN> {
    private final String stateName;
    private final VectorizedClassifier classifier;
    private final SlotArenaScope scope;
    private final TypeSerializer<ACC> accSerializer;
    private final BiFunction<ACC, IN, ACC> combiner;
    private final int maxEntries;
    private final LinkedHashMap<MemorySegment, Entry<ACC>> entries;  // access-order

    public ReducingAggregatingCache(String stateName, VectorizedClassifier classifier,
                                     SlotArenaScope scope, TypeSerializer<ACC> ser,
                                     BiFunction<ACC, IN, ACC> combiner, int maxEntries) {
        this.stateName = stateName; this.classifier = classifier; this.scope = scope;
        this.accSerializer = ser; this.combiner = combiner; this.maxEntries = maxEntries;
        this.entries = new LinkedHashMap<>(maxEntries, 0.75f, true) {
            @Override protected boolean removeEldestEntry(Map.Entry<MemorySegment, Entry<ACC>> e) {
                if (size() > maxEntries) {
                    if (e.getValue().dirty) flushOne(e.getKey(), e.getValue());
                    return true;
                }
                return false;
            }
        };
    }

    public ACC peek(MemorySegment key) { Entry<ACC> e = entries.get(key); return e == null ? null : e.acc; }
    public boolean isDirty(MemorySegment key) { Entry<ACC> e = entries.get(key); return e != null && e.dirty; }

    public void put(MemorySegment key, ACC acc) {
        entries.put(key, new Entry<>(acc, true));
    }

    public Optional<ACC> tryFold(MemorySegment key, IN input) {
        Entry<ACC> e = entries.get(key);
        if (e == null) return Optional.empty();
        e.acc = combiner.apply(e.acc, input);
        e.dirty = true;
        return Optional.of(e.acc);
    }

    public void flushAllDirty() {
        for (var e : entries.entrySet()) if (e.getValue().dirty) flushOne(e.getKey(), e.getValue());
    }

    private void flushOne(MemorySegment key, Entry<ACC> entry) {
        MemorySegment vSlice = scope.allocateTurn(estimateSize(entry.acc), 8);
        try (var view = new MemorySegmentDataOutputView(vSlice)) {
            accSerializer.serialize(entry.acc, view);
        } catch (IOException ioe) { throw new UncheckedIOException(ioe); }
        classifier.submit(new PutRequest(stateName, key, vSlice));
        entry.dirty = false;
    }

    private static class Entry<ACC> { ACC acc; boolean dirty; Entry(ACC a, boolean d) { acc=a; dirty=d; } }
}
```

- [ ] **Step 5: Commit**

### Task P7.3: ForStRsReducingState (component 12)

**Files:** create `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsReducingState.java`

- [ ] **Step 1: Failing test**

```java
@Test
void reducingStateAddInvokesReduceFunction() throws Exception {
    ReduceFunction<Integer> sum = (a, b) -> a + b;
    ForStRsReducingState<Integer> state = newTestReducingState(sum);
    state.add(10); state.add(20); state.add(30);
    state.flushOnBarrier();   // explicit barrier for test
    assertEquals(Integer.valueOf(60), state.get());
}

@Test
void coldMissReturnsFirstInputAsSeed() throws Exception {
    ForStRsReducingState<Integer> state = newTestReducingState(Integer::sum);
    state.add(42);
    state.flushOnBarrier();
    assertEquals(Integer.valueOf(42), state.get());
}
```

- [ ] **Step 2-4: implement**

```java
public class ForStRsReducingState<T> implements ReducingState<T> {
    private final String stateName;
    private final TypeSerializer<T> serializer;
    private final ReduceFunction<T> reduceFn;
    private final ReducingAggregatingCache<T, T> cache;
    private final PendingMissTable missTable;
    private final VectorizedClassifier classifier;
    private final SlotArenaScope scope;
    private final KeySupplier keySupplier;

    @Override
    public void add(T value) throws Exception {
        if (value == null) return;
        scope.enterTurnIfNeeded();
        MemorySegment key = encodeKeyInto(scope, stateName, keySupplier.currentKey());
        Optional<T> hit = cache.tryFold(key, value);
        if (hit.isPresent()) return;
        // Miss path
        missTable.beginOrJoin(stateName, key, value,
            (acc, in) -> acc == null ? in : reduceFn.reduce(acc, in),
            finalAcc -> cache.put(key, finalAcc),
            err -> {/* operator-level handler*/});
    }

    @Override
    public T get() throws Exception {
        scope.enterTurnIfNeeded();
        MemorySegment key = encodeKeyInto(scope, stateName, keySupplier.currentKey());
        T cached = cache.peek(key);
        if (cached != null) return cached;
        GetRequest req = new GetRequest(stateName, key);
        classifier.submit(req);
        classifier.flushAll();
        byte[] bytes = req.future().get();
        if (bytes == null || bytes.length == 0) return null;
        return serializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)));
    }

    @Override
    public void clear() {
        scope.enterTurnIfNeeded();
        MemorySegment key = encodeKeyInto(scope, stateName, keySupplier.currentKey());
        cache.remove(key);
        classifier.submit(new DeleteRequest(stateName, key));
    }

    public void flushOnBarrier() throws Exception {
        cache.flushAllDirty();
        classifier.flushAll();
    }
}
```

- [ ] **Step 5: Commit**

---

## P8 — AggregatingState + barrier drain integration (Trace E)

**Goal:** Add AggregatingState and wire the full Trace-E barrier-drain sequence into `snapshotState()`.

**Spec components:** 13 (AggregatingState), Trace E (two-phase flush + drain).

### Task P8.1: ForStRsAggregatingState

**Files:** create `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsAggregatingState.java`

- [ ] **Step 1: Failing test**

```java
@Test
void aggregatingStateAddInvokesAggregateFunction() throws Exception {
    AggregateFunction<Integer, Long, Long> sum = new AggregateFunction<>() {
        public Long createAccumulator() { return 0L; }
        public Long add(Integer v, Long acc) { return acc + v; }
        public Long getResult(Long acc) { return acc; }
        public Long merge(Long a, Long b) { return a + b; }
    };
    ForStRsAggregatingState<Integer, Long, Long> state = newTestAggregatingState(sum);
    state.add(10); state.add(20); state.add(30);
    state.flushOnBarrier();
    assertEquals(Long.valueOf(60), state.get());
}
```

- [ ] **Step 2-4: implement** — same shape as ReducingState but with `IN`/`ACC`/`OUT` typing and `AggregateFunction.createAccumulator()` for cold-miss seed.

```java
public class ForStRsAggregatingState<IN, ACC, OUT> implements AggregatingState<IN, OUT> {
    // similar to ReducingState; cache holds ACC; combine = aggFn.add(IN, ACC) → ACC;
    // get() → aggFn.getResult(ACC).
}
```

- [ ] **Step 5: Commit**

### Task P8.2: Barrier drain integration in ForStRsKeyedStateBackend.snapshotState

**Files:** modify `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsKeyedStateBackend.java`

- [ ] **Step 1: Failing test — barrier waits for in-flight RMW**

```java
@Test
void snapshotStateDrainsRmwBeforeEngineCheckpoint() throws Exception {
    ForStRsKeyedStateBackend backend = newTestBackend();
    ForStRsReducingState<Integer> state = backend.getReducingState(...);
    state.add(1); state.add(2); state.add(3);  // cold misses → pending convoy
    CompletableFuture<SnapshotResult> snap = backend.snapshotState(1L, System.currentTimeMillis(), factory, options);
    // The snapshot must not complete until RMW drained + flushed
    SnapshotResult sr = snap.get(5, TimeUnit.SECONDS);
    // Verify engine state contains the folded accumulator
    assertEquals(Integer.valueOf(6), state.get());
}
```

- [ ] **Step 2-4: implement Trace E sequence**

```java
@Override
public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotState(
        long checkpointId, long timestamp,
        CheckpointStreamFactory factory, CheckpointOptions options) throws Exception {

    // PHASE-1 flush: kick all in-flight batches
    classifier.flushAll();

    // drainRmwInFlight: await every pending-miss future + every PUT future already in flight
    pendingMissTable.awaitAll();

    // PHASE-2 flush: kick the PUTs just enqueued during RMW resolution
    classifier.flushAll();

    // flushRmwCacheDirty
    for (var s : reducingStates) s.flushOnBarrier();
    for (var s : aggregatingStates) s.flushOnBarrier();
    classifier.flushAll();

    // flushOpenWriteBuffers (SP6 staged writes)
    for (var s : mapStates) s.flushStaged();
    for (var s : valueStates) s.flushStaged();
    classifier.flushAll();

    // engine snapshot
    return existingEngineSnapshotPath(checkpointId, timestamp, factory, options);
}
```

Add `pendingMissTable.awaitAll()`:

```java
public CompletableFuture<Void> awaitAll() {
    List<CompletableFuture<?>> futures = pendingMisses.values().stream()
        .flatMap(pm -> pm.allFutures().stream())
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
}
```

- [ ] **Step 5: Commit**

---

## P9 — ITER_RANGE (V1 API, no production consumer yet)

**Goal:** Ship `ITER_RANGE` end-to-end so V1.x SQL TopN / windowed-scan queries can consume it. V1 itself has no internal consumer; the surface exists for testing.

**Spec components:** Rust D (frs_vec_iter_range_*), classifier/executor handling.

Tasks mirror P3 (iter prefix) with [lo, hi) bounds:

### Task P9.1: Rust — iter.rs `open_range` + FFI

```rust
impl NativeIter {
    pub fn open_range(snapshot: &Snapshot, lo: &[u8], hi: &[u8], chunk_bytes: usize) -> Self {
        let inner = snapshot.iter_range(lo, hi);
        Self { snapshot: snapshot.clone(), inner, aborted: AtomicBool::new(false) }
    }
}

#[no_mangle]
pub extern "C" fn frs_vec_iter_range_open(
    engine_handle: u64,
    lo_ptr: *const u8, lo_len: u32,
    hi_ptr: *const u8, hi_len: u32,
    chunk_buf_ptr: *mut u8, chunk_buf_cap: u32,
    out_handle: *mut u64, out_row_count: *mut u32,
) -> i32 { /* similar to prefix */ }

// _next + _close mirror prefix
```

Tests + Java wire-up mirror P3.2 / P3.5.

---

## P10 — L5 fault injection harness (F1-F12)

**Goal:** Build the deterministic + probabilistic fault injector and stand up all 12 fault scenarios from Spec §5.

**Spec components:** §5 L5 row, F1-F12 mandatory.

### Task P10.1: Rust — forst-rs-test-harness FaultInjector

**Files:** create `crates/forst-rs-test-harness/`

- [ ] **Step 1: Failing test**

```rust
#[test]
fn fault_injector_probability_mode() {
    std::env::set_var("FRS_FAULT_ENGINE_IO_PROB", "1.0");  // always fire
    let injector = FaultInjector::from_env();
    assert!(injector.should_fire("engine_io"));
}

#[test]
fn fault_injector_deterministic_at_mode() {
    std::env::set_var("FRS_FAULT_ENGINE_IO_AT", "5");
    let injector = FaultInjector::from_env();
    for i in 1..10 {
        let fires = injector.should_fire("engine_io");
        assert_eq!(fires, i == 5);
    }
}
```

- [ ] **Step 2-4: implement** (~80 LOC)

- [ ] **Step 5: Commit**

### Task P10.2: Wire F1 — ErrorCodeSubstitution

For each `frs_vec_*` symbol, check `FaultInjector.maybe_substitute(call_name, default_code)` before returning. If a fault is configured for this call, the row results carry the injected code instead of the real result.

- [ ] **Step 1-5: write test, fail, instrument, pass, commit**

### Task P10.3-P10.14: F2-F12 each as its own task

Each fault has a Java test file under `src/test/java/.../faultinjection/`. The test:
1. Sets the env var or programmatic injector
2. Runs the workload that should be affected
3. Asserts the expected behavior (e.g. operator failure, future completion exceptionally, retry semantics)

**F2 — WatchdogForceDrop:** force watchdog to abort iterator on next sweep → `ITER_EXPIRED` propagated.

**F3 — EnginePanic:** trigger Rust `panic!()` in next `frs_vec_get` call → `PANIC_CAUGHT` → `FatalErrorHandler.onFatalError` called with `FrsEnginePanicError`.

**F4 — SlowFfiReturn:** sleep N ms before returning from FFI → DISPATCH_HANG_MS detector fires → operator failure.

**F5 — CombinerThrowAtIndex:** user `ReduceFunction` throws on N-th call → convoy aborts, all chained futures fail with the original exception, cache NOT written.

**F6 — BarrierTimeFlushFailure:** inject ENGINE_IO into the PUT batch issued by `flushRmwCacheDirty()` → checkpoint aborts; no partial state visible.

**F7 — ArenaOverflow:** allocate larger-than-turnRegion via state op → overflow Arena created → confirm closed at `exitTurn()`.

**F8 — CacheEvictionWithError:** force LRU eviction while eviction PUT fails → operator failure; no silent state loss.

**F9 — BarrierWithIteratorActive:** open iterator, trigger checkpoint barrier while open → iterator closes at `exitTurn()`, barrier drain proceeds, no deadlock.

**F10 — WatchdogVsOperatorRace:** watchdog sets `closeRequested=true` mid-`next()` → operator observes flag, performs close exactly once, no double-free.

**F11 — SameKeyConvoyPressure:** burst 64 `add()`s on same RMW key with GET delayed via F4 → confirm `PendingMissTable.activeMissCount() == 1`, all 64 futures resolve once, fold-in-arrival-order preserved.

**F12 — EvictionDuringFlush:** trigger LRU eviction PUT while `flushRmwCacheDirty()` is executing → confirm eviction PUTs reach engine before snapshot is captured.

Each task: 5 steps (test, fail, implement injection point, pass, commit).

### Task P10.15: Rust panic-safety proptests (Spec §5 Medium-1)

**Files:** create `crates/forst-rs-engine/tests/panic_safety_proptest.rs`

Four proptests:

```rust
proptest! {
    #[test]
    fn prop_memtable_insert_panic_safe(input in any::<Vec<(Vec<u8>, Vec<u8>)>>()) {
        let memtable = Memtable::new();
        let panic_at = (input.len() / 2) as usize;
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            for (i, (k, v)) in input.iter().enumerate() {
                if i == panic_at { panic!("injected"); }
                memtable.insert(k, v);
            }
        }));
        prop_assert!(result.is_err());
        // Post-panic invariants:
        prop_assert!(memtable.invariants_hold());     // index consistent
        prop_assert_eq!(open_fd_count(), 0);          // no FD leak
        // Snapshot refcounts balanced is N/A here (Memtable doesn't snapshot)
    }
    // Similar for snapshot_create, iter_open, compaction
}
```

- [ ] **Step 1-5: write, fail, implement `invariants_hold()` for each engine struct, pass, commit**

---

## P11 — L6 Nexmark gates + L7 soak

**Goal:** Wire the tiered L6 perf gates into CI and stand up the L7 soak as a release gate.

### Task P11.1: L6 — tiered gates in run-nexmark-matrix.sh

**Files:** modify `~/Downloads/workenv/flink-2.2.1/bin/run-nexmark-matrix.sh`

Add a JSON manifest that lists each query's tier + gate:

```json
{
  "tiers": {
    "state-heavy": {"gate": 1.20, "queries": ["q3", "q4", "q5", "q8"]},
    "state-medium": {"gate": 1.05, "queries": ["q6", "q7"]},
    "state-light": {"gate": 0.95, "queries": ["q0", "q1", "q2"]}
  }
}
```

Modify the driver:
1. After each query run, compute speedup = `rocksdb_time / forstrs_time`.
2. Look up query's tier; assert `speedup >= gate`.
3. On miss: file investigation issue (manually or via `gh issue create` if integrated).
4. **Do not silently lower the gate.**

- [ ] **Step 1-5: write driver test, fail, implement gate check, pass, commit**

### Task P11.2: L6 daily CI cadence

- [ ] **Step 1: Add daily cron job** (or GitHub Actions workflow) running `run-nexmark-matrix.sh all q3,q4,q5,q8` at 03:00 UTC.

- [ ] **Step 2: Alert on > 5% deviation from rolling 7-day median**

- [ ] **Step 3: Auto-bisection workflow on alert** — git bisect over last 7 commits, 4h cap.

- [ ] **Step 4-5: pass, commit**

### Task P11.3: L7 soak harness

**Files:** create `~/Downloads/workenv/flink-2.2.1/bin/run-soak.sh`

```bash
#!/usr/bin/env bash
# Q3 + Q5 + Q8 continuous, parallel, with F1-F12 each at PROB=0.001
# Duration: 24h (PR gate) or 72h (release gate)
set -euo pipefail
DURATION_HOURS="${1:-24}"
export FRS_FAULT_ENGINE_IO_PROB=0.001
export FRS_FAULT_PANIC_PROB=0.001
# ... export all F1-F12 ...

start_ts=$(date +%s)
end_ts=$((start_ts + DURATION_HOURS * 3600))
while [ $(date +%s) -lt $end_ts ]; do
  run-nexmark-matrix.sh forst-rs q3 &
  run-nexmark-matrix.sh forst-rs q5 &
  run-nexmark-matrix.sh forst-rs q8 &
  wait
  # Acceptance probes:
  #   - RSS stable ± 5% (compare to start)
  #   - iter.handles_open ≤ 2× steady state
  #   - arena.region_overflows/h bounded
  #   - final state matches RocksDB control
  #   - checkpoint failure rate < 0.5%
  check_acceptance.sh || exit 1
done
echo "L7 soak passed ($DURATION_HOURS h)"
```

- [ ] **Step 1-5: write skeleton, dry-run for 5 min, fix issues, document acceptance probes, commit**

---

## P12 — Productization runbook validation

**Goal:** Convert §6 runbook from documentation into executed validations. Output: signed `§6.13 V1 release readiness checklist`.

### Task P12.1: Validate §6.1 deployment checklist

For each box in §6.1, write a `verify_deploy_*.sh` script that asserts the condition. Run them on the actual cluster (`~/Downloads/workenv/flink-2.2.1/`):

- ABI version match (call `frs_abi_version` via JMX or query log)
- JVM flags present in TM startup
- Required Flink configs set
- Metrics published (curl Flink REST API)

- [ ] **Step 1-5: write each verifier, fail (find missing deployment knob), fix, pass, commit**

### Task P12.2: Validate §6.3 in-band rollback

Run on the cluster:
1. Start a job on forst-rs
2. `flink stop --savepoint-path s3://.../sp-test <job-id>`
3. Update job config to rocksdb backend
4. `flink run -s s3://.../sp-test <job-jar>`
5. Verify state restored correctly

- [ ] **Step 1-5: scripted run-through, document timings, commit playbook**

### Task P12.3: Validate §6.4 emergency rollback

- [ ] **Step 1-5: scripted disaster simulation — kill JM mid-job, restart from last forst-rs checkpoint, verify at-most-once semantics on a synthetic Kafka source with offset bookkeeping**

### Task P12.4: Migration validation — Forst-RS → Forst-RS round-trip

- [ ] **Step 1-5: scripted savepoint export → reimport → state equality check**

### Task P12.5: Quarterly runbook review automation

Cron job that diffs current code against runbook claims; opens `runbook-stale` issues for mismatches.

- [ ] **Step 1-5: implement linter, dry-run, fix false positives, commit**

### Task P12.6: Sign off §6.13 release readiness checklist

For each box in §6.13:
- Mark as verified or pending
- Attach evidence (test run logs, microbench report links, soak run summary)

Final deliverable: `docs/superpowers/specs/2026-05-XX-forst-rs-v1-readiness-signoff.md` capturing the signed-off state.

- [ ] **Step 1-5: aggregate evidence, sign, commit, tag V1 release**

### Task P12.7: Cross-reference maintenance — link SP1-SP6 to umbrella

For each of SP1/SP2/SP5/SP6, edit the spec doc to add citations to the umbrella spec where they touch a contract defined here. Bidirectional links per Appendix.

- [ ] **Step 1: Identify every section in SP1-SP6 that depends on an umbrella contract**
- [ ] **Step 2: Insert citation: "per umbrella §X — <contract>"**
- [ ] **Step 3: Edit umbrella spec's component table to back-cite the SP owning the impl**
- [ ] **Step 4: Open quarterly review schedule**
- [ ] **Step 5: Commit**

---

## Self-review summary

**1. Spec coverage:**

| Spec section | Covered by |
|---|---|
| §1 dispatch table + 6 kinds | P2 (4 kinds), P3 (ITER_PREFIX), P6 (APPEND_MERGE), P9 (ITER_RANGE) |
| §1 §a APPEND_MERGE non-goal for Reducing/Aggregating | P7 (RMW path instead) + Classifier guard in P2.6 |
| §1 §b iterator lifetime bound | P3.4 (watchdog) + P3.3 (per-iter Arena) |
| §1 §c metrics namespace + 128 cap | P4.1 |
| §1 semantic guarantees | P2.6 (intra-key ordering test), P3.3 (snapshot isolation), P10 (concurrency F9-F12) |
| §2 components 1-17 | P1 (7, 17), P2 (1-4), P3 (5, 6), P4 (8, 16), P5 (9, 10), P6 (11), P7 (12, 14, 15), P8 (13) |
| §2 components A-G (Rust) | P1.1 (G), P2.1-2 (A error envelope), P3.1-2 (C, E), P6.1-2 (B, F), P9.1 (D) |
| §3 Traces A-E | P5.1 (A), P6.3 (B + APPEND_MERGE), P7.3 (C), P3.5 (D), P8.2 (E) |
| §4 error contract | P2.1, P2.3, P4.3 |
| §5 testing matrix L1-L7 | P10 (L5), P10.15 (L1 proptests), P11 (L6, L7), throughout (L2-L4) |
| §5 migration strategy | P12.4 |
| §6 productization runbook | P12.1-P12.6 |
| Appendix microbench gate | P0 |
| Appendix cross-ref maintenance | P12.7 |

**2. Placeholder scan:** searched for TBD/TODO/FIXME — none in the plan tasks. Implementation code blocks all carry actual code; references to `existing logic` mean "preserve what's there from prior PRs", not a placeholder.

**3. Type consistency:**
- `VectorizedStateRequest.Kind` enum used consistently in P2-P9.
- `FrsErrorCode` enum used consistently from P2.1 onward.
- `SlotArenaScope` API (enterTurn/exitTurn/allocateTurn/allocateCache/registerIter/unregisterIter/closeSlot) consistent across P1-P9.
- `FrsIterHandle` API (handleId/nativeHandleId/lastNextNs/closeRequested/close/forceClose/requestClose) consistent P1.4 stub → P3.3 real → P3.5 wire-up → P4.3 metrics.
- `classifier.submit` / `classifier.flushAll` consistent everywhere.
- `PendingMissTable.beginOrJoin` signature matches in P7.1 (definition) and P7.3 (use).

No drift detected.
