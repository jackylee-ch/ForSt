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

### 4.1 windowed-agg RMW mini-bench (`windowed_agg_rmw.rs`)

Reproduces the per-record accumulator RMW over FLUSHED (SST-resident)
accumulators under uniform KV-sep (min-blob=22 ⇒ every value separated).
LocalFileSystem, acc_size=32 B, batch=256.

Small scale (20k keys, 200k records — warm working set, reads mostly re-hit):

```
  OFF             650.2 ns/rec
  ON-uniform      828.5 ns/rec   (+27%, the regression)
  ON+coalesce     687.2 ns/rec   (recovers ~78% of the gap)
```

Full scale (200k keys, 2M records — genuinely COLD, reads scattered across
many vlog segments):

```
  OFF            1107.2 ns/rec
  ON-uniform     1737.6 ns/rec   (+57%, matches q11's ~1.8x regression ratio)
  ON+coalesce    2397.4 ns/rec   (WORSE — coalesce HURTS here)
```

**Root cause:** under uniform KV-sep, a 32-byte accumulator separated into the
vlog forces a `VlogReader::get` that reads a **64 KiB chunk** (`VLOG_READ_CHUNK`)
per cold deref — catastrophic read-amp (64 KiB I/O for 32 B of value). At large
scale each batch's keys scatter across MANY single-flush segments (1-2 pointers
per segment), so `coalesced_vlog_deref_into`'s group-by-segment + sort + HashMap
overhead exceeds any benefit (each "group" is ~1 pointer → no coalescing to do)
and coalesce REGRESSES. Coalesce only wins when many pointers land in ONE
segment (the q9/q20 join-scan shape), not the q11/q17 scattered-point-RMW shape.

### 4.2 C-vs-B decision: **B (structural staging buffer)**

The deref lands on COLD/flushed accumulators read per-record. Crucially:

- **C's premise does NOT hold.** Once an accumulator flushes to SST under uniform
  KV-sep, the value lives ONLY in the vlog (the SST row is a BlobRef) — there is
  NO cheaper inline copy to read instead. The resident-flushed shadow (Phase 4,
  checked before the SST tier) DOES hold the pre-separation full value and is
  already served inline — but only while that shadow is alive; the deref fires
  exactly when no inline copy exists. So "read inline first, deref only flushed"
  is already what the engine does; there is no avoidable indirection for C to
  remove. C cannot help the truly-cold case.
- **B is the structural fix.** Keep the active window's accumulators in an
  off-heap Arrow staging buffer for the window's lifetime so per-record RMW hits
  memory (zero deref, zero SST read), flushing to the engine (uniform key-LSM +
  vlog) only at window-fire / checkpoint. This eliminates BOTH the SST read and
  the 64 KiB-chunk deref read-amp for the hot window working set.
  - ⚠ B is gated by the checkpoint state-equivalence TDD test (§5).
- **A (foundation) still applies** to the residual cold derefs that B does not
  cover (cross-window / restore / eviction): zero-copy the deref into the Arrow
  builder so each unavoidable deref costs one decompress-into-buffer, no owned
  Vec + memcpy. A is correctness-neutral and byte-identical.

### 4.3 Refinement: the cost is a once-per-flush-cycle deref read-amp

A key structural observation that re-frames C vs B for the RMW shape:

- Under uniform KV-sep a re-WRITTEN accumulator goes to the active memtable as a
  full INLINE value (separation is flush-time only). So after the first
  write-back, re-reads within the window hit the memtable (Phase 1) and NEVER
  reach the deref. Each accumulator is therefore dereffed AT MOST ONCE per flush
  cycle (first read after a flush → write-back makes it inline again).
- B's win (no deref for the hot window) requires NOT flushing the active window's
  accumulators, which needs WINDOW-BOUNDARY knowledge that lives in the Flink
  windowed-agg state layer + checkpoint capture — a cross-layer change, NOT an
  engine read-path change, and exactly the silent-corruption-risk surface the B
  equivalence gate warns about.
- The pure READ-PATH lever is therefore to make that unavoidable once-per-cycle
  deref CHEAP: kill the 64 KiB chunk read-amp (`get_point`) + (foundation A)
  coalesce/zero-copy the residual. This is what the profiler demands and what
  fits the "fix entirely in the read path / uniform format" constraint.

**Decision: ship the read-path point-deref (the A-family fix). B (Flink-layer
window staging) is deferred as a larger cross-layer change** — it is NOT an
engine read-path fix and would need the mandated checkpoint state-equivalence
gate before shipping.

## 5. Implementation log

All changes are READ-PATH only; the on-disk/vlog format is UNCHANGED (uniform
KV-sep). All flag-gated, default OFF, byte-identical when ON.

1. **`VlogReader::get_point`** (storage/vlog.rs): reads EXACTLY the record bytes
   (`header + ptr.len`) with one positioned read — no 64 KiB chunk fill, no
   chunk-cache pollution; honors an existing chunk-cache HIT. Same CRC +
   decompress as `get` ⇒ byte-identical value. The oversized branch is already a
   direct pread in `get`.
2. **`FRS_VLOG_POINT_DEREF` flag** + `vlog_point_deref_enabled()` /
   `set_vlog_point_deref_override()` (engine/db.rs), exported from lib.rs.
3. **Deref-tier routing** (engine/db.rs): `vlog_deref` (single-key inline path:
   single `get`, the non-coalesce batch arms, the RMW) routes through `get_point`
   when ON; `deref_one_segment_into` routes SINGLE-pointer groups (no coalesce
   locality — the scattered RMW shape) through `get_point` when ON.
4. **Tests (byte-identity gates):**
   - storage `test_vlog_get_point_byte_identical` — `get_point` == `get` across
     codecs, sizes, empty, oversized, and chunk-cache-hit.
   - engine `test_vlog_point_deref_byte_identical_windowed_agg_rmw` — fresh-db
     A/B: point ON vs OFF byte-identical for both single-key `get` and
     `batch_get_vectorized` (scattered, with/without coalesce), separated values
     across multiple segments, interleaved misses.
5. **Mini-bench** `crates/forst-rs-bench/src/bin/windowed_agg_rmw.rs`.

## 6. Results

### 6.1 Mini-bench (LocalFS; storage-layer COLD RMW)

q11-like (32 B accumulator, 200k keys, 2M records):

```
  OFF           1116.2 ns/rec
  ON-uniform    1791.6 ns/rec   (+60.5% regression)
  ON+coalesce   2397.7 ns/rec   (HURTS — scattered single-pointer segments)
  ON+point      1270.0 ns/rec   (closes 77% of the gap; +13.8% residual)
```

q17-like (72 B accumulator, 150k keys, 1.5M records):

```
  OFF           1119.0 ns/rec
  ON-uniform    1687.5 ns/rec   (+50.8% regression)
  ON+coalesce   3887.6 ns/rec   (HURTS)
  ON+point      1156.9 ns/rec   (closes 93% of the gap; +3.4% residual)
```

`FRS_VLOG_POINT_DEREF` collapses most/all of the storage-layer regression and
NEVER changes the bytes. On LocalFS the 64 KiB chunk is page-cache-cheap so this
UNDERSTATES the disagg/remote win (there each chunk = a 64 KiB ranged GET, vs a
~45 B point read) — the NEXMark disagg A/B (§6.2) is the real magnitude.

The residual gap is the structural SST→vlog double-indirection (2 lookups per
cold accumulator vs OFF's 1 inline read); eliminating it entirely would require
not-derefing-at-all = B (Flink-layer window staging), out of scope for a
read-path-only engine fix.

### 6.2 NEXMark disagg A/B (q11 + q17, 2x4c/16g split)

(pending — run after the live sweep's docker box is idle, per coordination rule)

Suggested arms (uniform KV-sep, min-blob driven low for the uniform premise):
- ON pre-fix  (FRS_KV_SEPARATION=1, FRS_VLOG_POINT_DEREF=0)
- ON post-fix (FRS_KV_SEPARATION=1, FRS_VLOG_POINT_DEREF=1)
- OFF         (FRS_KV_SEPARATION=0)
SUCCESS = q11/q17 ON post-fix FINISH exact rows AND beat their OFF walls
(q11 < 118.8s, q17 < 110.7s).

## 7. Correctness / honesty notes

- All read-path changes are flag-gated, DEFAULT OFF, byte-identical when ON
  (two dedicated byte-identity A/B tests + the existing kvsep/coalesce/vlog
  suites all green).
- Honest negative: this is the A-family read-path fix. It does NOT implement B
  (window staging). If the disagg A/B shows a residual ON-vs-OFF gap, the
  remaining cost is the SST→vlog double-indirection per cold accumulator, which
  only B (not-derefing) can remove — and B is a Flink-layer change gated by the
  checkpoint state-equivalence test.
