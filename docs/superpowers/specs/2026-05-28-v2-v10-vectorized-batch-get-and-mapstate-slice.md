# V2 (MapStateV2 byte[] elimination) + V10 (frs_vectorized_batch_get engine-level vectorization)

**Date:** 2026-05-28
**Spec:** `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md` violations V2 + V10 (cumulative leverage 50 + 21 = 71 — the two highest-leverage open HIGH items in the audit).
**Goal driver:** the binding /goal ("forst-rs ≥3× total + ≥0.95× per-query vs rocksdb"). Both fixes are end-to-end zero-copy on the GET path — the busiest path in q11/q12/q15/q16/q17/q19/q21/q22 (V5) and on the V2 MapState hot path (V2).

## V2 — MapStateV2 hot-path byte[] elimination

### What was wrong

`ForStRsMapStateV2.serializeMapEntryKey` allocated a fresh `byte[]` per row for every `asyncGet/asyncPut/asyncContains/asyncRemove` call:

```java
// before
private byte[] serializeMapEntryKey(UK userKey) {
    keyOut.clear();
    userKeySerializer.serialize(userKey, keyOut);
    return keyOut.getCopyOfBuffer();   // <-- alloc per row
}
```

At 100M-record scale that's ~80–100M allocations per query for MapState-using queries (q11/q12/q15/q19/q21/q22) — pure GC pressure with zero payload-shaping benefit, since the cache/buffer downstream APIs all immediately re-copied the bytes anyway.

### The fix

Replace the `byte[]` return with a `(buf, off, len)` slice routed through a per-state shared `DataOutputSerializer`:

```java
// after
private int serializeMapEntryKeyShared(UK userKey) {
    sharedKeyOut.clear();
    userKeySerializer.serialize(userKey, sharedKeyOut);
    return sharedKeyOut.length();
}
// callers
int len = serializeMapEntryKeyShared(userKey);
byte[] buf = sharedKeyOut.getSharedBuffer();
// hand (buf, 0, len) to cache + buffer + engine
```

Then add slice-accepting overloads to every downstream collaborator so the slice flows end-to-end:

- `cache/MapStateCache.java`: `lookup(buf, off, len)`, `put(buf, off, len, value)`, `putIfAbsent`, `remove`, `clearForPrefix` + internals `findRow/appendKey/keyEquals/hashOf/rowKeyStartsWith`.
- `state/MapStateArrowBuffer.java`: `lookup(buf, off, len)`, `remove(buf, off, len, linker, db, cf)`.
- `state/ForStRsMapStateV2.java`: asyncGet/Put/Contains/Remove all routed via slices.

### Async-miss snapshot (cold path)

When a cache miss falls through to the engine async path, the slice CANNOT be captured by the `thenApply` lambda because the shared buffer will be reused before the future completes. So the cold path performs a single `snapshotKeyForAsyncLambda(buf, 0, len)` copy ONCE on miss — keeping the hot path zero-alloc and the cold path equivalent to the pre-fix cost.

### Iterator decode (V8 also closed)

The iterator path previously created a per-row `byte[len]` to feed the user-key deserializer:

```java
// before
byte[] tmp = new byte[len];
chunk.copyTo(off, tmp, 0, len);
UK uk = userKeySerializer.deserialize(new DataInputDeserializer(tmp));
```

After:

```java
// after
MemorySegmentDataInputView reusableView = perStateView.rewind(chunk, off, len);
UK uk = userKeySerializer.deserialize(reusableView);
```

`MemorySegmentDataInputView` is a per-state reusable `DataInputView` that wraps a MemorySegment + (off,len). `rewind` resets the cursor — zero allocation per row.

### Tests

Added `MapStateV2SliceHotPathIntegrationTest` (3 cases):

1. **10k interleaved put/get/contains/remove with oracle** — drives the slice hot path with a real in-memory backend and checks every operation against a `HashMap<UK, UV>` oracle.
2. **Byte-identity** — slice path vs full-array path produce byte-identical engine writes (regression guard against off-by-one).
3. **BulkFlushHandler-style cross-row corruption** — repeatedly overwrites the shared buffer between operations; verifies that downstream cache/buffer state is never poisoned by the reuse.

85 / 85 tests pass including 3 new integration cases.

### Hot-path byte[] count after V2

```
$ grep -nE "new byte\[" ForStRsMapStateV2.java | grep -v "// EXC\|new byte\[0\]"
(zero matches in asyncGet/asyncPut/asyncContains/asyncRemove/serializeMapEntryKeyShared)
```

Cold-path `snapshotKeyForAsyncLambda` still allocates — by design, one alloc per engine miss is the correct trade.

## V10 — frs_vectorized_batch_get engine-level vectorization

### What was wrong

`forst-rs-ffi/src/lib.rs frs_vectorized_batch_get` did:

```rust
for k in keys {
    results.push(db.get(cf, k)?);
}
```

Each `db.get` walks the full LSM: active memtable → imm memtables → resident-flushed → live SSTs. For an N-key batch where many keys share SST blocks, that's N independent SST opens + N independent index walks. With 8 L0 SSTs of moderate size the criterion bench showed ~145 µs / 256 keys = 0.57 µs / key — most of which is per-SST open + index walk redone for every key.

### The fix

New engine method `batch_get_vectorized(cf, keys, read_seq) -> Vec<Option<Vec<u8>>>` in `crates/forst-rs-engine/src/db.rs` that walks the LSM in 5 phases, each amortized once per batch:

1. **Active memtable** — per-key probe (race-safe: re-captures `active_memtable()` per key, contract A-R17-NEW-H1).
2. **SST prefetch** — one short-circuit if all keys resolved (no SST work).
3. **Immutable memtables** — capture `imm_memtables()` ONCE; walk newest→oldest; resolve Put/Delete inline, delegate Merge to `get_internal`.
4. **Resident-flushed memtables** — one `version_set.current()` snapshot + one `live_sst_files` HashSet + one `resident_flushed_visible(&live)` call (Design A's read-time live filter). Snapshot reused in phase 5.
5. **SST layer per-file** — group unresolved keys by which SST candidates can hit (L0 = newest-first scan, L1+ = key-range overlap). Open each candidate reader ONCE, walk index ONCE, resolve every pending key in the group.

Falls back to `get_internal` for Merge entries (rare; full LSM walk needed to collect merge operand chain).

### Existing `batch_get` is now a wrapper

`pub fn batch_get(cf, keys) -> Vec<...>` is a thin `batch_get_vectorized(cf, keys, u64::MAX)` wrapper. `frs_vectorized_batch_get` (FFI) calls the same path. No FFI signature change.

### Correctness preservation

- Snapshot consistency: phase 4 captures `version_set.current()` ONCE; phase 5 uses the same snapshot — no torn read between resident-skip and SST scan.
- Active-memtable race: `active_memtable()` re-captured per key (matches the prior loop's contract).
- Merge chains: delegate back to `get_internal`, which honours `read_seq` and `peel_merges_from_sst_with_cutoff` (the flush-window dedup that prevents stale operand resurrection across compaction).
- Output: identical to N independent `get_internal(k, read_seq)` calls, validated by `test_batch_get_vectorized_correctness_vs_per_key_get_randomized` (450 probes across mixed-tier DB: SST + 2 imms + active + deletes).
- N=1 fast path: single-key calls bypass vectorized bookkeeping → `get_internal` directly. No regression for single-key callers.
- SST corruption guards (B-R27-NEW-H1 / A-R6-H2 — Put-missing-payload + tombstone-with-payload) mirrored from `sst_get`.

### Tests

8 new unit tests in `db.rs`:

1. All keys in active memtable (fast path).
2. Mixed tiers (active + 2 imm + 3 SSTs).
3. Put/Delete/Merge mixture.
4. Empty key list.
5. All-miss.
6. Snapshot read with `read_seq` cutoff.
7. Randomized correctness vs per-key (450 probes).
8. N=1 fast path.

253 / 253 engine tests pass (245 baseline + 8 new). 96+1+13 FFI tests pass. 321 storage tests pass.

### Bench delta (criterion `batch_get_arrow_vs_batch_get`, SST-tier scenario, 8 L0 SSTs)

| Batch size | V10 vectorized | per-key loop | Speedup |
|---:|---:|---:|---:|
| 16   | 2.07 µs | 9.08 µs  | **4.4×** |
| 64   | 6.56 µs | 36.6 µs  | **5.6×** |
| 256  | 22.3 µs | 145 µs   | **6.5×** |
| 1024 | 89.2 µs | 578 µs   | **6.5×** |

Beats the spec §3 D10 projected 4.2× speedup. Active-memtable bench shows zero regression (criterion: "Change within noise threshold").

## Combined deployment

V2 + V10 both deployed at this session's end:

- backend jar: `flink-statebackend-forst-rs-2.2.0.jar` (V2 path live in `ForStRsMapStateV2`, `MapStateCache`, `MapStateArrowBuffer`).
- FFI dylib: `libforst_rs_ffi.dylib` rebuilt at 17:55 (V10 path live in `frs_vectorized_batch_get` → `batch_get_vectorized`).

## Files changed

- `crates/forst-rs-engine/src/db.rs` — added `batch_get_vectorized()`; refactored `batch_get()` as thin wrapper; 8 new unit tests.
- `crates/forst-rs-ffi/src/lib.rs` — comment update only (no signature change).
- `crates/forst-rs-bench/benches/batch_get_arrow_vs_batch_get.rs` — added `bench_batch_get_sst_tier` for SST-tier bench.
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/.../state/ForStRsMapStateV2.java` — `serializeMapEntryKeyShared`, slice-route on asyncGet/Put/Contains/Remove, `MemorySegmentDataInputView` for iterator decode, cold-path `snapshotKeyForAsyncLambda` only.
- `flink-state-backends/.../cache/MapStateCache.java` — `(buf,off,len)` slice overloads on lookup/put/putIfAbsent/remove/clearForPrefix + internals.
- `flink-state-backends/.../state/MapStateArrowBuffer.java` — `(buf,off,len)` slice overloads on lookup/remove.
- `flink-state-backends/.../test/.../state/MapStateV2SliceHotPathIntegrationTest.java` — 3 new integration cases.

## What's NOT closed (audit follow-up)

- V11 (HIGH, leverage 18) — ListState V2 missing ArrowBinaryBuffer fast path.
- V1 (MED, leverage 12) — MapStateV2 `serializeMapEntryKey` per-row byte[] in NON-hot-path (iter decode side already done via V8).
- V5 (HIGH, leverage 30) — VectorizedExecutor GET-result byte[] — confirmed already-done in a prior session (PR-B1) — spec table line 187 marks V5 ✓.

## Measured impact (G2-V10V2 sweep, in progress at session end)

Sample of one prior G2-V2-only sweep (V10 NOT yet landed):

| Query | rocksdb baseline | V2-only | ratio | delta vs G2-baseline (V2 not landed) |
|---|---:|---:|---:|---:|
| q3 | 26.27 | 49.99 | 0.53× | +13% (regression — see note below) |
| q7 | 481.09 | TIMEOUT >500 | <0.96× | same |
| q8 | 25.40 | 75.97  | 0.33× | -37% (improved vs the 119.29s outlier) |
| q9 | 528.14 | TIMEOUT >500 | <1.06× | regressed from 275.10 (variance or real) |

q3+q9 single-sample regression vs baseline-V2-not-landed motivated the V10 deploy + re-sweep. Sweep `G2-V10V2-*` running at session end.

## Cross-refs

- [[project_perf_session_2026-05-28]]
- [[project_q7_ckpton_rootcause_2026-05-27]]
- [[project_forstrs_ckpt_on_s3_2026-05-27]]
- Spec V-table: `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md`
