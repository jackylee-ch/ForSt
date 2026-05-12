# B-Prod Phase 2: Performance + Full Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hit 3× warm-cache / 2-3× cold-cache perf bars (forst-rs/S3 vs rocksdb/local), complete all state-type parity (async keyed state SPI), and close remaining followup gaps so forst-rs backend fully replaces forst backend in every scenario.

**Architecture:** 4 phases executed sequentially: (A) scale-up benchmarks to identify real bottlenecks at 1M-10M events, (B) targeted perf optimization based on profiling data, (C) full state-type parity (createAsyncKeyedStateBackend SPI + operator state already done), (D) remaining followup fixes (BatchedPoll, PerCfSst, L7-MiniClusterEnd2End, CommunityForstJni).

**Tech Stack:** Rust 2021 (MSRV 1.88), Java 25 (FFM), Apache Flink 2.2.0, criterion/JMH, OpenDAL 0.50, testcontainers (MinIO), S3 vector I/O (OpenDAL batch-read).

**Performance targets** (user-specified):
- **Warm-cache**: forst-rs FFI/JDK25/S3 ≥ **3×** rocksdb/JDK17/local
- **Cold-cache**: forst-rs FFI/JDK25/S3 ≥ **2-3×** rocksdb/JDK17/local (via S3 vector I/O / batch-read)
- **Same-storage**: forst-rs FFI/JDK25/S3 ≥ **5×** forst (no forst-rs lib)/JDK17/S3

**Current baseline** (LittleE2E 100k events, local-only, no checkpointing):
- rocksdb: 129,893 eps (7.7 µs/event)
- forst-rs: 89,950 eps (11.1 µs/event) — currently **0.69× rocksdb** (rocksdb 1.44× faster)
- Engine-level P5: forst-rs **4× rocksdb** on point lookups (engine wins not yet surfacing through Flink runtime overhead)

**Gap analysis** (why engine 4× doesn't show as through-Flink 4×):
- Flink runtime overhead per event: key-context setup, key-group encoding, namespace serialization, scheduler interaction
- FFM hop overhead vs rocksdb's bundled JNI (rocksdb-jni is a single native call; forst-rs FFM goes through Linker.downcallHandle + Arena management)
- The 100k-event workload is too small to amortize JIT warmup + MiniCluster scheduling overhead
- No batching: each state access is a separate FFM call (vs rocksdb's WriteBatch path)

---

## Phase A: Scale-Up Benchmarks (Week 1-2)

**Purpose**: Get real numbers at production-relevant scale before optimizing. Identify the actual bottleneck (FFM hop? key encoding? Flink scheduler? S3 latency?).

### Task A1: Scale LittleE2E to 1M-10M events with parallelism sweep

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/perf/LittleE2EPerfBench.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/run-little-e2e-perf.sh`
- Modify: `.github/workflows/little-e2e-perf-bench.yml`

- [ ] **Step 1: Add parallelism + event-count matrix to the bench**

In `LittleE2EPerfBench.java`, add `--parallelism` arg (default 2, max 8) and increase default events to 1M:
```java
int parallelism = Integer.parseInt(arg(args, "--parallelism", "2"));
long events = Long.parseLong(arg(args, "--events", "1000000"));
```

- [ ] **Step 2: Update run script to sweep parallelism**

In `run-little-e2e-perf.sh`, add a parallelism loop:
```bash
for par in 2 4 8; do
    run_variant "rocksdb (p=$par)" rocksdb "--parallelism" "$par"
    run_variant "forst-rs (p=$par)" forst-rs "--parallelism" "$par"
done
```

- [ ] **Step 3: Update GHA workflow default events to 1M**

In `.github/workflows/little-e2e-perf-bench.yml`:
```yaml
default: '1000000'
```

- [ ] **Step 4: Run locally at 1M events, parallelism=2,4,8**

```bash
cd /Users/lijunqing/Code/stczwd/flink
EVENTS=1000000 WARMUPS=2 ./flink-state-backends/flink-statebackend-forst-rs/run-little-e2e-perf.sh
```

Expected: RESULT lines for each (backend, parallelism) pair. Record numbers.

- [ ] **Step 5: Commit + push**

```bash
git add -A && git commit -m "bench(little-e2e): scale to 1M events + parallelism sweep (Phase A1)"
git push origin forst-rs-jdk25
```

### Task A2: Add checkpointing-enabled variant

**Files:**
- Modify: `LittleE2EPerfBench.java`

- [ ] **Step 1: Add `--checkpoint-interval` arg**

```java
long ckptInterval = Long.parseLong(arg(args, "--checkpoint-interval", "0"));
if (ckptInterval > 0) {
    env.enableCheckpointing(ckptInterval);
}
```

- [ ] **Step 2: Add checkpoint variant to run script**

```bash
run_variant "forst-rs (p=4, ckpt=5s)" forst-rs "--parallelism" "4" "--checkpoint-interval" "5000"
run_variant "rocksdb (p=4, ckpt=5s)" rocksdb "--parallelism" "4" "--checkpoint-interval" "5000"
```

- [ ] **Step 3: Run + record numbers**

- [ ] **Step 4: Commit + push**

### Task A3: Add real-S3 variant (MinIO testcontainer in bench)

**Files:**
- Modify: `LittleE2EPerfBench.java` (add `--storage-uri` arg)
- Modify: `run-little-e2e-perf.sh` (start MinIO, pass URI)

- [ ] **Step 1: Add `--storage-uri` arg to bench**

When set, configure `ForStRsOptions.storageUri(uri)` before creating the backend.

- [ ] **Step 2: Add MinIO start to run script**

```bash
# Start MinIO container for S3 variants
docker run -d --name minio-bench -p 9000:9000 \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio:RELEASE.2024-08-17T01-24-54Z server /data
# Create bucket
docker exec minio-bench mkdir -p /data/forst-rs-bench
S3_URI="s3://forst-rs-bench/"
S3_ENDPOINT="http://127.0.0.1:9000"
```

- [ ] **Step 3: Add S3 variants to the sweep**

```bash
run_variant "forst-rs (p=4, S3)" forst-rs "--parallelism" "4" \
    "--storage-uri" "$S3_URI" "--s3-endpoint" "$S3_ENDPOINT"
run_variant "rocksdb (p=4, local)" rocksdb "--parallelism" "4"
```

- [ ] **Step 4: Run + record the cross-stack comparison**

This is the measurement that directly answers the 3× bar question.

- [ ] **Step 5: Commit + push**

### Task A4: Profile forst-rs hot path (async-profiler flamegraph)

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/profile-forst-rs.sh`

- [ ] **Step 1: Write profiling script**

```bash
#!/usr/bin/env bash
# Profile forst-rs backend under LittleE2E workload using async-profiler
set -euo pipefail
AP_HOME="${AP_HOME:-/opt/async-profiler}"
EVENTS="${EVENTS:-1000000}"

# Run bench with profiler attached
"$JAVA_HOME/bin/java" \
    --enable-native-access=ALL-UNNAMED \
    -agentpath:"$AP_HOME/lib/libasyncProfiler.so=start,event=cpu,file=/tmp/forst-rs-flamegraph.html" \
    -Dforstrs.native.libpath="$CDYLIB" \
    -cp "$CP" \
    org.apache.flink.state.forstrs.perf.LittleE2EPerfBench \
    --backend forst-rs --events "$EVENTS" --warmups 1 --parallelism 4

echo "Flamegraph: /tmp/forst-rs-flamegraph.html"
```

- [ ] **Step 2: Run locally + analyze flamegraph**

Identify top CPU consumers. Expected candidates:
- `ForStRsLinker.put` / `ForStRsLinker.get` (FFM downcall overhead)
- `ForStRsKeyGroupedSerializer.encodeForState` (key encoding)
- `MemorySegment.ofArray` / Arena allocation
- Flink scheduler overhead (task dispatch, watermark processing)

- [ ] **Step 3: Document findings in `docs/superpowers/specs/2026-05-12-phase2-profiling.md`**

- [ ] **Step 4: Commit + push**

---

## Phase B: Performance Optimization (Week 3-6)

**Purpose**: Based on Phase A profiling, implement targeted optimizations to hit the perf bars.

### Task B1: Batch-put FFI (reduce per-event FFM hop count)

**Files:**
- Modify: `crates/forst-rs-ffi/src/lib.rs` (add `frs_batch_get`)
- Modify: `crates/forst-rs-engine/src/db.rs` (add `batch_get`)
- Modify: `ForStRsLinker.java` (add `batchGet` binding)
- Modify: `ForStRsAbstractKeyedStateBackend.java` (batch state access path)

- [ ] **Step 1: Add `frs_batch_get` FFI export**

```rust
/// Batch point-lookup: reads N keys in one FFM call, returning N values.
/// Amortizes the FFM hop overhead across N lookups.
#[no_mangle]
pub unsafe extern "C" fn frs_batch_get(
    db: FrsDb,
    cf: FrsCfHandle,
    keys: *const *const u8,      // array of key pointers
    key_lens: *const usize,      // array of key lengths
    count: usize,
    out_values: *mut FrsBytes,   // pre-allocated array of FrsBytes[count]
) -> i32 { ... }
```

- [ ] **Step 2: Bind in ForStRsLinker + add `batchGet(db, cf, byte[][] keys) -> byte[][]`**

- [ ] **Step 3: Wire into the keyed-state backend's hot path**

When Flink processes a batch of records for the same key-group, accumulate state accesses and flush as a batch-get + batch-put at the end of the batch window.

- [ ] **Step 4: Benchmark: measure per-event µs reduction**

- [ ] **Step 5: Commit + push**

### Task B2: S3 vector I/O for cold-cache reads (OpenDAL batch-read)

**Files:**
- Modify: `crates/forst-rs-storage/src/cached_fs.rs` (add prefetch/batch-read on cache miss)
- Modify: `crates/forst-rs-io/src/opendal_fs.rs` (expose OpenDAL's `batch` API)

- [ ] **Step 1: Implement `CachedFileSystem::prefetch_batch`**

When a cache miss occurs, instead of fetching one SST block at a time, issue a batch-read for all blocks in the SST's block index that are likely to be needed (based on the key range being scanned). OpenDAL 0.50 supports `Operator::read_with().range(offset..offset+len)` — issue N concurrent reads via `tokio::join!` or `FuturesUnordered`.

```rust
/// Prefetch multiple byte ranges from a single SST file in parallel.
/// Used on cache miss to amortize S3 round-trip latency across N blocks.
pub async fn prefetch_batch(
    &self,
    path: &str,
    ranges: &[(u64, u64)],  // (offset, length) pairs
) -> ForstResult<Vec<Bytes>> { ... }
```

- [ ] **Step 2: Wire into the SST reader's block-fetch path**

When `SstReader::read_block(block_handle)` misses the cache, instead of fetching just that one block, call `prefetch_batch` for all blocks in the same SST that the current scan/lookup might need (based on the bloom filter or block index).

- [ ] **Step 3: Benchmark cold-cache reads with prefetch vs without**

Expected: 2-5× improvement on cold-cache point lookups (amortizing S3 RTT across N blocks instead of paying it N times serially).

- [ ] **Step 4: Commit + push**

### Task B3: Reduce FFM hop overhead (critical-mode + heap-segment pooling)

**Files:**
- Modify: `ForStRsLinker.java` (pool MemorySegments for hot-path key/value buffers)
- Modify: `ForStRsAbstractKeyedStateBackend.java` (reuse key-encoding buffers)

- [ ] **Step 1: Pool key-encoding buffers**

Currently each `encodeForState` allocates a fresh `DataOutputSerializer(64)`. Pool these per-thread:
```java
private static final ThreadLocal<DataOutputSerializer> KEY_BUFFER =
    ThreadLocal.withInitial(() -> new DataOutputSerializer(256));
```

- [ ] **Step 2: Pool value-read buffers**

Currently each `get` allocates a fresh `byte[24]` for the FrsBytes out-struct. Pool:
```java
private static final ThreadLocal<byte[]> FRS_BYTES_BUF =
    ThreadLocal.withInitial(() -> new byte[24]);
```

- [ ] **Step 3: Benchmark: measure per-event µs reduction**

- [ ] **Step 4: Commit + push**

### Task B4: BatchedPoll FFI for timer service (B-Prod-followup-BatchedPoll)

**Files:**
- Modify: `crates/forst-rs-ffi/src/lib.rs` (add `frs_iterator_drain_n`)
- Modify: `ForStRsLinker.java` (bind `iteratorDrainN`)
- Modify: `ForStRsKeyGroupedInternalPriorityQueue.java` (use batched drain)

- [ ] **Step 1: Add `frs_iterator_drain_n` FFI export**

```rust
/// Drains up to N entries from an iterator, returning them as parallel
/// arrays (keys + values + count). Single FFM call replaces N individual
/// iterator_next calls.
#[no_mangle]
pub unsafe extern "C" fn frs_iterator_drain_n(
    iter: FrsIterator,
    max_count: usize,
    out_keys: *mut FrsBytes,     // pre-allocated array[max_count]
    out_values: *mut FrsBytes,   // pre-allocated array[max_count]
    out_actual_count: *mut usize,
) -> i32 { ... }
```

- [ ] **Step 2: Bind in ForStRsLinker**

- [ ] **Step 3: Wire into priority queue's `poll()` / `bulkPoll()` path**

- [ ] **Step 4: Scale timer IT to 1M events**

- [ ] **Step 5: Commit + push**

### Task B5: PerCfSst partitioning (B-Prod-followup-PerCfSst)

**Files:**
- Modify: `crates/forst-rs-engine/src/version_set.rs` (tag SSTs with CF ID)
- Modify: `crates/forst-rs-engine/src/flush.rs` (write per-CF SSTs)
- Modify: `crates/forst-rs-engine/src/compaction.rs` (compact within CF boundaries)
- Modify: `crates/forst-rs-engine/src/db.rs` (`drop_cf` now deletes CF-tagged SSTs)

- [ ] **Step 1: Add `cf_id: u32` field to SST metadata**

- [ ] **Step 2: Flush writes SSTs tagged with the source CF's ID**

- [ ] **Step 3: Compaction respects CF boundaries (only merges same-CF SSTs)**

- [ ] **Step 4: `drop_cf` now walks version_set and deletes CF-tagged SSTs**

- [ ] **Step 5: Tests: drop_cf reclaims storage; cross-CF reads unaffected**

- [ ] **Step 6: Commit + push**

---

## Phase C: Full State-Type Parity (Week 5-6)

**Purpose**: Wire `createAsyncKeyedStateBackend` through the SPI so forst-rs replaces forst in ALL scenarios.

### Task C1: createAsyncKeyedStateBackend SPI wire-up

**Files:**
- Modify: `ForStRsStateBackend.java` (override `createAsyncKeyedStateBackend`)
- Modify: `ForStRsAbstractKeyedStateBackend.java` (implement `AsyncKeyedStateBackend` interface)

- [ ] **Step 1: Override `createAsyncKeyedStateBackend` in ForStRsStateBackend**

```java
@Override
public <K> AsyncKeyedStateBackend<K> createAsyncKeyedStateBackend(
        KeyedStateBackendParameters<K> parameters) throws Exception {
    // Reuse the same construction path as createKeyedStateBackend but
    // return the backend cast to AsyncKeyedStateBackend (which it already
    // implements via the P8 async state classes).
    return (AsyncKeyedStateBackend<K>) createKeyedStateBackend(parameters);
}
```

- [ ] **Step 2: Make ForStRsAbstractKeyedStateBackend implement AsyncKeyedStateBackend**

The P8 async state classes (ForStRsAsyncValueState etc.) already exist. Wire them through the `AsyncKeyedStateBackend` interface methods.

- [ ] **Step 3: MiniCluster IT with async state API via SPI**

- [ ] **Step 4: Commit + push**

### Task C2: Verify all Flink state scenarios work through forst-rs

**Files:**
- Create: `ForStRsFullParityIT.java`

- [ ] **Step 1: Write comprehensive IT covering all state types**

```java
@Test void keyedValueState() { ... }
@Test void keyedListState() { ... }
@Test void keyedMapState() { ... }
@Test void keyedReducingState() { ... }
@Test void keyedAggregatingState() { ... }
@Test void asyncKeyedValueState() { ... }
@Test void operatorListState() { ... }
@Test void operatorUnionState() { ... }
@Test void broadcastState() { ... }
@Test void timerService() { ... }
@Test void windowingWithTimers() { ... }
```

All run through `state.backend = ForStRsStateBackendFactory` on a real MiniCluster.

- [ ] **Step 2: Run + verify all pass**

- [ ] **Step 3: Commit + push**

---

## Phase D: Remaining Followups (Week 6-7)

### Task D1: L7-MiniClusterEnd2End (env.enableCheckpointing + cancel + restart)

- [ ] Investigate the Flink-internal teardown race (`AsyncSnapshotCallable.<init>` + closed `cancelStreamRegistry`)
- [ ] If fixable in forst-rs scope: fix + add IT
- [ ] If upstream Flink issue: document + file upstream issue + add workaround IT with unbounded source

### Task D2: CommunityForstJni API mismatch

- [ ] Investigate `com.ververica:forstjni:0.1.8` vs `flink-statebackend-forst.ForStStateBackend.ensureForStIsLoaded()` signature mismatch
- [ ] Pin to compatible version or patch the call site
- [ ] Re-run LittleE2E with all 4 variants producing numbers

### Task D3: Re-measure at scale after optimizations

- [ ] Run the Phase A bench suite (1M events, parallelism=4, S3 variant) after Phase B optimizations land
- [ ] Produce the final comparison table for VP review
- [ ] Update VP status doc v4 with real numbers against the 3×/2-3×/5× bars

---

## Success criteria

| Bar | Target | How measured |
|---|---|---|
| Warm-cache: forst-rs/S3 vs rocksdb/local | ≥ 3× | LittleE2E 1M events, p=4, forst-rs with S3 (working set fits cache) vs rocksdb local |
| Cold-cache: forst-rs/S3 vs rocksdb/local | ≥ 2-3× | LittleE2E 1M events, p=4, forst-rs with S3 (cache cleared between runs) vs rocksdb local |
| Same-storage: forst-rs/S3 vs forst/S3 | ≥ 5× | LittleE2E 1M events, p=4, both on S3 |
| All state types via SPI | PASS | ForStRsFullParityIT (11 tests) |
| Timer service at 1M events | PASS | TumblingWindowIT scaled |
| env.enableCheckpointing MiniCluster IT | PASS or documented upstream blocker | D1 |

---

## Timeline

| Week | Phase | Deliverable |
|---|---|---|
| 1 | A1-A2 | Scale-up bench at 1M events + checkpointing variant |
| 2 | A3-A4 | Real-S3 bench + profiling flamegraph |
| 3 | B1-B2 | Batch-get FFI + S3 vector I/O prefetch |
| 4 | B3-B4 | FFM hop reduction + BatchedPoll |
| 5 | B5 + C1 | PerCfSst + AsyncKeyedStateBackend SPI |
| 6 | C2 + D1-D2 | Full parity IT + remaining followups |
| 7 | D3 | Final measurement + VP doc v4 |

**Total: ~7 weeks single-track; ~5 weeks with 2 implementers (Phase B tasks are parallelizable).**

---

## Risks

| Risk | Mitigation |
|---|---|
| 3× warm-cache bar not achievable (FFM hop is irreducible overhead) | Batch-get amortizes the hop; if still short, consider JNI fast-path for the hottest ops (hybrid FFM+JNI) |
| 2-3× cold-cache bar not achievable (S3 RTT is physics) | S3 vector I/O + aggressive prefetch; user's BOS has only 2-3× latency vs local (not 100×), so the bar is realistic for their infra |
| PerCfSst is a 2-3 week engine refactor that may destabilize | Gate behind a feature flag; run full test suite before + after |
| Community ForSt API mismatch may be unfixable without upstream patch | Document as "community ForSt variant not comparable in this branch"; focus on rocksdb-vs-forst-rs which is the VP-relevant comparison |
| Flink teardown race in MiniCluster bounded-source jobs | Use unbounded source with explicit job cancellation after N events; or patch upstream |
