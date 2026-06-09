# Zero-copy columnar key path — eliminate per-record FFM alloc + key materialization (q7/q9/q20)

**Date:** 2026-06-09
**Status:** Approved design (lever 1 of the vectorized+zero-copy join read-path program).
**Scope:** forst-rs backend (`flink-statebackend-forst-rs`): the per-record state-key encoding +
the executor/state batch buffers. Engine untouched.

## Root cause (JFR-quantified, 2026-06-09)
A full-stack JFR of q9 (150s steady state) overturned every engine-read hypothesis (the engine read is
µs-cheap; `IOUtil.read` ≈1% on-CPU). Both backends use the SAME async-state framework
(`table.exec.async-state.enabled: true` in BOTH rocksdb + forst-rs configs), so the async/serde costs
are shared and cannot explain forst-rs being slower. The **forst-rs-specific differentiator** is the
per-record key/FFM bucket — **~20% of on-CPU (≈900/4433 samples)** — that RocksDB does NOT pay:

| on-CPU leaf | samples |
|---|---|
| `SegmentFactories.initNativeMemory` (per-record FFM native alloc) | 212 |
| `Unsafe.checkOffset` + `MemorySessionImpl.checkValidStateRaw` (FFM segment access/validity) | 197 + 45 |
| `ArrowBinaryBuffer.hash` + `MapStateCache.hashOf` (per-record key hash) | 230 + 93 |
| `MemorySegment.equalTo` (key equality) | 63 |
| `ForStRsMapStateV2.serializeMapEntryKeyShared` (key serde) | 59 |

**Refinement (code-confirmed 2026-06-09):** `MapStateCache` + `ArrowBinaryBuffer` ALREADY pool/reuse
their segments (grown rarely), so the ~20% is NOT one per-record alloc site — it is three diffuse
**Panama-FFM** costs: (a) `initNativeMemory` ~5% (occasional buffer grows), (b) **per-access FFM safety
checks** (`Unsafe.checkOffset` + `MemorySessionImpl.checkValidStateRaw`) ~5.5% fired on EVERY
`MemorySegment.get` — and `ArrowBinaryBuffer.hash` does a **per-byte `seg.get(JAVA_BYTE)`** (N
bounds-checked accesses per key), (c) the hash arithmetic ~7%. **So the highest-value, mandate-aligned
sub-lever is VECTORIZED WIDE-STRIDE segment access**: read 8 bytes at a time (`JAVA_LONG`) in the hash +
key-compare + key-copy loops, cutting the per-byte FFM bounds-checks ~8× (the access-check overhead that
JNI/critical-array RocksDB doesn't pay). This is on top of the columnar-buffer reuse below.

The system is also partly **wait-bound** (62,719 `ThreadPark` on the shared mailbox vs 4,433 on-CPU), so
this lever is expected to give a **solid-but-moderate** gain — it is the right FIRST step (removes the
mandate-violating per-record native alloc + is the forst-rs/RocksDB differentiator), and the foundation
for the O(1)-lookup and zero-copy-value levers that follow.

## Design — three parts (all honor batch-only / zero-copy / no-per-record-alloc)
**1. Reused columnar key arena (kill `initNativeMemory`).** Today each probe's composite key lands in a
freshly-`Arena.allocate`d `MemorySegment` (per-record native alloc → `initNativeMemory` + `checkValidStateRaw`
+ GC). Replace with a **per-executor, grown-on-demand, reused columnar buffer** (`offsets[K+1]` + packed
key bytes) — the AEC batch's K composite keys packed once into ONE reused segment, mirroring the existing
`batch_get` SoA scratch + `ArrowBinaryBuffer`'s reused arena. The FFI consumes `(offset,len)` slices into
the shared segment (MMP pointers), not per-record segments.

**2. Zero-copy key source from `BinaryRowData`.** `BinaryRowData` is already off-heap `MemorySegment`-backed.
Where the key/namespace originates from a binary row, copy its bytes **segment→segment** directly into the
columnar buffer (`MemorySegment.copy`), bypassing the `RowDataSerializer`→heap-`byte[]`→re-serialize round
trip on the forst-rs side (cuts `serializeMapEntryKeyShared` + forst-rs-side copies). Composite-prefix
framing (`KEY_PREFIX + keyGroup + / + stateName + / + userKey`) is written into the columnar buffer with
the SAME byte layout as today (byte-identical keys).

**3. Vectorized batch hash.** Compute the K composite-key hashes in ONE columnar pass over the packed
buffer (amortizes `ArrowBinaryBuffer.hash` + `MapStateCache.hashOf`), reusing the **engine-compatible hash
function** so the MapState cache slots + engine-side hash slots stay byte-aligned (no cache/engine drift).

## Correctness (GATE)
- **Byte-identical composite keys** and **identical hashes** to the current path (engine slot layout +
  cache layout unchanged) — proven by a UT: for representative key types (Row/long/string composite keys),
  the columnar path produces the exact same composite-key bytes + hash as the per-record path.
- The reused columnar buffer is **confined to the executor thread + batch lifetime** (no cross-batch
  aliasing; the FFI copies/consumes before the buffer is reused for the next batch) — matches the existing
  `batch_get` scratch contract.
- e2e: q7/q9/q20 + the iterator queries (q3/q11/q12/q15) `out_rows`/final-result == RocksDB on seeded
  input. No config change.

## Verification (before/after, recorded in 2026-06-08-8c32g-3backend-sweep-results.md)
- Re-run the JFR: `initNativeMemory` / `checkValidStateRaw` / forst-rs key-serde must drop OUT of the
  on-CPU top; the forst-rs-specific bucket should fall from ~20% toward RocksDB's near-0.
- q9/q7/q20 wall before/after; regression-check the passing set (q16/q17/q18 + light + q11/q12/q15).
- Both repos' GHA green.

## Sequencing (the broader program — this spec is lever 1)
- **Lever 1 (this):** zero-copy columnar keys — removes the per-record FFM alloc + key materialization.
- **Lever 2:** O(1) batched engine lookup (memtable hash + SST bloom/hash-index for a whole key-batch,
  no ordered scan) returning zero-copy value views.
- **Lever 3:** zero-copy value return (MemorySegment views, batch-decode via the `VIEW_TL` path generalized).
Each lever is its own one-pass, correctness-verified, before/after-measured change. q7/q9/q20 reaching the
≥0.8× RocksDB bar likely needs levers 1+2 together (the per-request cost must drop AND the wait must shrink);
honest expectation is that lever 1 alone narrows but may not fully close the gap.
