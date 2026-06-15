# Make UNIFORM KV-separation ON beat KV-sep OFF on q11/q17 — READ-PATH-ONLY fix

**Status:** IN PROGRESS (PMC-1, 2026-06-15)
**Branch:** `kvsep-on-readpath` (worktree `/tmp/frs-kvsepon`)
**Constraint (non-negotiable, user):** the remote/S3 on-disk data format MUST be UNIFORM — KV-sep ON for ALL values, ONE on-disk layout (key-LSM with blob-refs + vlog). NO dynamic/hybrid layout (some inline, some separated). The ENTIRE fix lives in the READ PATH.

## 1. Problem

Under UNIFORM KV-separation ON (every value separated into the vlog, one on-disk
format), the windowed-agg queries regress vs KV-sep OFF:

| query | KV-sep ON | KV-sep OFF | gap |
|-------|-----------|------------|-----|
| q11   | 215.8s    | 118.8s     | +97.0s (1.82×) |
| q17   | 150.7s    | 110.7s     | +40.0s (1.36×) |

q11/q17 are windowed-agg (Aggregating/Reducing) queries. Their hot path is a
per-record RMW on a SMALL numeric accumulator (q11 ~16–32 B counts, q17 ~40–80 B
sums): for each input record, **read accumulator → update in Java → write back**.

Under KV-sep ON every accumulator value is separated into the vlog, so the
per-record READ pays a vlog deref (an extra indirection / read-amp) that the
inline (OFF) path does not. That deref is the regression.

### Why the default config does NOT show this

`FRS_KV_MIN_BLOB_SIZE` defaults to **256** (db.rs `kv_min_blob_size`), so values
strictly shorter than 256 B stay INLINE even with `FRS_KV_SEPARATION=1`. q11/q17
accumulators (≤80 B) therefore stay inline under the DEFAULT and show no
regression. The regression appears only under the UNIFORM-format premise this
task targets: KV-sep ON for ALL values (min-blob-size driven to ~0 / pointer
floor), the single on-disk layout ops wants. The fix must make the read path
fast for separated small accumulators WITHOUT making the on-disk layout hybrid.

## 2. Code map (read path, KV-sep ON)

- **Accumulator GET flow (q11/q17):** Flink Aggregating/Reducing state → FFI
  `frs_vectorized_batch_get` (ffi/lib.rs ~2769/3347) → `db.batch_get` →
  `DbImpl::batch_get_vectorized` (db.rs 12987) → memtable tiers (Phase 1/3/4) →
  SST tier (Phase 5) → `finish_batch` (db.rs 14706). The Arrow sibling is
  `batch_get_arrow` (db.rs 13535).
- **Where the deref lands:** memtable tiers NEVER hold BlobRefs — "separation is
  flush-time only" (db.rs guards at imm/resident stages fail loud on a BlobRef).
  So a freshly-written accumulator is served INLINE from the active memtable; the
  vlog deref only fires once the accumulator has FLUSHED to an SST. The deref
  sites are the SST-tier `OpType::BlobRef` arms (db.rs 13260/13377/13495 in
  `batch_get_vectorized`; 13602 slow-path `get_internal` for `batch_get_arrow`).
- **vlog deref:** `DbImpl::vlog_deref` (db.rs 14493) → `VlogReader::get`
  (vlog.rs 275) → `parse_record` → `decompress` → returns an **owned `Vec<u8>`**.
- **Coalesce machinery (already present, default OFF):**
  - `FRS_VLOG_COALESCE_DEREF` — `batch_get_vectorized` defers each winning
    BlobRef pointer into `deferred`, resolved in ONE group-by-segment +
    sort-by-offset pass via `coalesced_vlog_deref_into` (db.rs 14514) →
    `VlogReader::get_coalesced` (vlog.rs 319, ONE ranged read per segment).
  - `FRS_VLOG_DEREF_FANOUT` / `FRS_VLOG_DEREF_LOCALITY` — per-segment parallel
    reads across the read-I/O pool (remote-only, locality-gated).
  - `FRS_VLOG_SCAN_COALESCE` / `FRS_VLOG_SCAN_READAHEAD` — the scan-path analogues.
- **The "candidate-D" residual (zero-copy gap):** even with coalesce ON,
  `get_coalesced` returns `Vec<Vec<u8>>` (one owned Vec per value, allocated in
  `parse_record`/`decompress`), `finish_batch` collects into
  `Vec<Option<Vec<u8>>>`, and the FFI/Arrow layer then **memcpy**s each value
  into the out_buf / `BinaryBuilder` (`append_value(&value)`). And
  `batch_get_arrow` does NOT use the coalesce path at all — its slow-path tail
  goes per-key through `get_internal` → inline `vlog_deref` (owned Vec) →
  `value_builder.append_value(&value)` (memcpy from the owned Vec).
- **`ValueSink` / `BinaryBuilderSink`:** db.rs 1566 — the existing zero-copy sink
  used ONLY for the active-memtable inline fast path in `batch_get_arrow`. The
  vlog-deref tier does not yet stream through it.

## 3. Approved design

### A — FOUNDATION (always)

Batch + coalesce + zero-copy the deref, and route the windowed-agg accumulator
reads through the V2 vectorized read path.

1. Extend `ValueSink` DOWN through the vlog-deref tier: add
   `VlogReader::get_into(ptr, &mut sink)` and
   `get_coalesced_into(ptrs, &mut [sink])` that **decompress/scatter the
   dereffed value directly into a REUSED Arrow `BinaryBuilder`** (offset+len),
   instead of materializing an owned `Vec<u8>` per value + memcpy. No per-value
   owned Vec, no byte[].
2. Make `batch_get_arrow` USE the coalesce path (it currently does not): defer
   SST-tier BlobRef rows and resolve them in one coalesced pass that streams
   straight into the value builder.
3. Coalesce a batch of records' accumulator GETs into ONE vlog read per segment
   (reuse `FRS_VLOG_COALESCE_DEREF` / `coalesced_vlog_deref_into`).

Byte-identical output; flag-gated where it could affect correctness.

### Profiler picks C vs B

Profile q11/q17 at the split (KV-sep ON, uniform) to find WHERE the deref cost
lands:

- **C (cheapest — deref on HOT/recently-written/memtable-resident accumulators):**
  read the value INLINE from memtable/recently-written first; deref the vlog ONLY
  for truly-flushed values. If we're derefing values still inline-available,
  that's avoidable indirection.
- **B (structural — per-record RMW over COLD/flushed accumulators dominates):**
  keep the active window's accumulators in an off-heap Arrow staging buffer for
  the window's lifetime so per-record RMW hits memory (zero deref); flush to the
  engine only at window-fire/checkpoint.
  - ⚠ **B MUST be gated by a checkpoint state-equivalence TDD test:** checkpoint
    with staged accumulators, restore, assert restored state is BYTE-IDENTICAL to
    the non-staged path AND windowed results are exact. Do not ship B without this
    gate green.

## 4. Profiler evidence

(filled in as collected — see §4.1 mini-bench, §4.2 q11/q17 profile)

## 5. Implementation log

(filled in)

## 6. Results

(filled in)
