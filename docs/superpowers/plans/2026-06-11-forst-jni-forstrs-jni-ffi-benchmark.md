# ForSt JNI / ForSt-RS JNI / ForSt-RS FFI Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add and smoke-test a small hot-path benchmark matrix for ForSt JNI, ForSt-RS JNI, and ForSt-RS FFI.

**Architecture:** Keep JVM/JNI comparisons in the Flink benchmark harness and native FFI decomposition in the ForSt Rust criterion harness. Reuse existing `run-jmh-3way.sh` variants and the existing `compat_jni_forst_backend_hotpaths` bench instead of creating a new benchmark framework.

**Tech Stack:** Java 25, JNI, JDK FFM for existing ForSt-RS linker paths, Rust criterion, Cargo, Flink `flink-statebackend-forst-rs`.

---

### Task 1: Extend ForSt-RS FFI Criterion Coverage

**Files:**
- Modify: `crates/forst-rs-bench/benches/compat_jni_forst_backend_hotpaths.rs`
- Modify: `crates/forst-rs-bench/src/lib.rs`

- [ ] **Step 1: Add delete and prefix scan helpers**

Add helpers around `frs_delete`, `frs_prefix_lookup_open`, `frs_prefix_lookup_next`, and `frs_prefix_lookup_close`. Each helper must assert `FRS_STATUS_OK` and free returned `FrsBytes`.

- [ ] **Step 2: Add delete benchmark**

Add `compat_jni_forst_backend/delete_then_get` with one in-memory DB per timed function. The timed body writes a key, deletes it, and verifies a follow-up `frs_get` returns zero length.

- [ ] **Step 3: Add prefix scan benchmark**

Add `compat_jni_forst_backend/prefix_scan` with 512 keys sharing one prefix. The timed body opens a prefix iterator, drains it, closes it, and returns the scanned row count.

- [ ] **Step 4: Run Rust smoke**

Run:

```bash
cargo fmt --all -- --check
cargo bench -p forst-rs-bench --bench compat_jni_forst_backend_hotpaths --no-run
cargo bench -p forst-rs-bench --bench compat_jni_forst_backend_hotpaths -- --noplot
```

Expected: all commands pass, and the benchmark prints rows for write batch, join multi-get, compacted join probe, delete, and prefix scan.

### Task 2: Extend ForSt-RS JNI Java Mirror

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/forstdb/RocksDB.java`

- [ ] **Step 1: Declare missing benchmark-only native methods**

Add declarations for:

```java
public static native byte[][] batchGet(long handle, long cfHandle, byte[][] keys);
public static native byte[][] multiGet(long handle, byte[][] keys, int[] offsets, int[] lengths);
public static native byte[][] multiGet(
        long handle, byte[][] keys, int[] offsets, int[] lengths, long[] cfHandles);
public static native long prefixLookupOpen(
        long handle, long cfHandle, byte[] prefix, int prefixOff, int prefixLen);
public static native byte[][] prefixLookupNext(long iterHandle);
public static native void prefixLookupClose(long iterHandle);
public static native long iteratorOpen(long handle, long cfHandle);
public static native void iteratorSeek(long iterHandle, byte[] key, int keyOff, int keyLen);
public static native byte[][] iteratorNext(long iterHandle);
public static native void iteratorClose(long iterHandle);
```

- [ ] **Step 2: Compile ForSt-RS JNI smoke class**

Run the existing runner after the benchmark class is added in Task 3.

### Task 3: Add JVM Hot-Path Benchmark Classes

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/jmh/ForStRsJniHotPathBenchmark.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/java-community/org/apache/flink/state/forstrs/jmh/ForStCommunityHotPathBenchmark.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/test/java-community/org/forstdb/RocksDB.java`

- [ ] **Step 1: Add ForSt-RS JNI hot-path class**

The class must use `org.forstdb.RocksDB` flat methods and report:

- `openClose`
- `pointGet`
- `pointPut`
- `delete`
- `batchPut`
- `batchGet`
- `multiGet`
- `prefixScan`
- `flushCompactRead`

It must use `bench.warmup.s` and `bench.measure.s` like `ForStCompareBenchmark`.

- [ ] **Step 2: Add community ForSt hot-path class**

The class must use `Options`, `WriteOptions`, and `WriteBatch` for community
ForSt. It must report comparable rows for:

- `openClose`
- `pointGet`
- `pointPut`
- `delete`
- `writeBatch`
- `multiGet`, if the local community JNI symbol resolves

If a symbol throws `UnsatisfiedLinkError`, print `unsupported` for that row and continue.

- [ ] **Step 3: Add community multiGet declarations**

Add long-form `multiGet` overload declarations to the community `RocksDB` mirror so the benchmark can attempt the same API as ForSt-RS JNI.

### Task 4: Update Runner and Documentation

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/run-jmh-3way.sh`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/JMH_BENCHMARK.md`
- Create: `docs/superpowers/benchmarks/2026-06-11-forst-jni-forstrs-jni-ffi-hotpaths/README.md`

- [ ] **Step 1: Add optional benchmark mode**

Support:

```bash
./run-jmh-3way.sh forst-rs hotpaths
./run-jmh-3way.sh forst hotpaths
```

Default mode remains the existing throughput benchmark.

- [ ] **Step 2: Capture result files**

Write results to:

```text
/tmp/jmh-results-forst-rs-hotpaths-default.txt
/tmp/jmh-results-forst-hotpaths-default.txt
```

- [ ] **Step 3: Document smoke commands and table schema**

Update docs with the new hotpaths mode and explain that ForSt-RS FFI criterion is the ceiling, while ForSt/ForSt-RS JNI are the primary Java comparison.

### Task 5: Verify and Commit

**Files:**
- All files changed above.

- [ ] **Step 1: Run local Rust smoke**

Run Task 1 commands in the ForSt worktree.

- [ ] **Step 2: Run local JVM smoke**

Run:

```bash
cd flink-state-backends/flink-statebackend-forst-rs
FORST_RS_LIB=/private/tmp/forst-compat-jni-forst-backend/target/release/libforst_rs_ffi.dylib \
  BENCH_WARMUP_S=1 BENCH_MEASURE_S=2 ./run-jmh-3way.sh forst-rs hotpaths
```

If `/tmp/forstjni-community.dylib` is available, also run:

```bash
BENCH_WARMUP_S=1 BENCH_MEASURE_S=2 ./run-jmh-3way.sh forst hotpaths
```

- [ ] **Step 3: Commit and push**

Commit ForSt docs/bench changes in the ForSt worktree. Commit Flink benchmark changes in the Flink repo without including unrelated dirty files. Push both branches after successful smoke checks.
