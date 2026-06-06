# q4 read-path: value-carrying prefix-scan merge (the 2× gap root cause)

**Date:** 2026-06-06. Evidence-backed perf design. This is the fundamental reason
forst-rs q4 runs ~2× slower than RocksDB on local dir, and the synthesis that lets us
keep the resident shadow OFF (8c/32g memory fit) WITHOUT losing read speed.

## Evidence (symbolized CPU profile, q4 steady-state, host)

Sampled the TM during q4 steady-state (build with `CARGO_PROFILE_RELEASE_STRIP=none
CARGO_PROFILE_RELEASE_DEBUG=1`). The dominant CPU, far above everything else:

```
4726  frs_vec_iter_prefix_open            lib.rs:4977   (prefix-scan open + first chunk)
4715  forst_rs_ffi::fill_chunk_from_iter  lib.rs:4912   (chunk drain — already zero-copy memcpy)
4714  FilterMap::next  (layer A = engine: get_internal per key, db.rs:5763)
4610  FilterMap::next  (layer B = ffi:    Result→IterKey conversion, lib.rs:5049)
2922  LazyPrefixIter::next                              (the k-way key merge)
```

Leaf costs inside the scan: `RawVecInner::finish_grow` / `MutableBuffer::reallocate` /
`GenericByteBuilder::append_value` (Arrow buffers grown not pre-sized), `memcmp`,
`ShardedMemTable::get`, btree `find_key_index`.

## Root cause

`build_lazy_prefix_key_stream` (db.rs:5784) does a k-way merge over ALL tiers
(active memtable + immutables + every overlapping SST) that emits each visible
**user-key exactly once** — KEYS ONLY. Its own doc-comment: *"Value resolution is the
caller's responsibility."* Then `prefix_scan_iter_owned_arc_with_error_slot`
(db.rs:5763) does **`get_internal(key, u64::MAX)` per yielded key** — a FULL point
lookup that walks the entire LSM AGAIN.

So a K-row prefix scan is **O(K × tiers)**: walk the LSM once to enumerate K keys,
then K more times (once per key) to resolve values. The `TierKeySource::Sst` block
reader (db.rs:9406) literally reads `view.value/sequence/op_type` from each row and
**discards all but `view.key`**. RocksDB's iterator yields key+value from one cursor
position → O(K + tiers). The authoring comment at db.rs:9485 even concedes the merge
compares are *"dwarfed by the `db.get(key)` resolve"* — known, accepted, never fixed.

This is why dropping the resident shadow (Spec C1) regressed q4 ~461→563s: the shadow
was an in-RAM short-circuit around this double-walk. The right fix is not to restore
the shadow (it doesn't fit 8c/32g) but to make the scan itself carry values.

## Design — carry values through the merge, resolve MVCC inline

Add a value-carrying path used by the hot `prefix_scan_iter_owned_arc*` callers; leave
the key-only `LazyPrefixIter` for callers that only need keys.

1. **Tier sources carry the head row's metadata**, not just the key:
   - `TierKeySource::Sst`: buffer `(key, value, sequence, op_type)` of the NEWEST
     version per user-key within each block. The within-block dedup already keeps the
     first row per user-key, and rows are `(key ASC, sequence DESC)` → first = newest.
   - `TierKeySource::MemCursor`: extend `MemTierCursor`
     (forst-rs-storage/memtable/sharded.rs) to expose `sequence`, `op_type`, and the
     value bytes at its head alongside the key.

2. **Merge resolves the winner inline** (`next_with_value`): find the min user-key;
   among all sources whose head == min user-key, pick the entry with the **max
   sequence** (newest). Then by its `op_type`:
   - `Put`   → yield `(key, value)` directly. **No `get_internal`.** (q4 join state.)
   - `Delete`→ skip the key (advance all sources at that user-key, continue).
   - `Merge` → return `(key, NONE)` so the caller falls back to `get_internal(key)`
     for the full operand-chain resolution (the agg operators; a minority of keys).
   Advance every source positioned at that user-key (cross-tier dedup, unchanged).

3. **Caller (db.rs:5763)** uses `next_with_value`: `Some((k, Some(v)))` → emit;
   `Some((k, None))` → `get_internal(k)` fallback; `None` → done. The per-key
   `get_internal` is eliminated for the dominant Put/Delete case.

4. **Output Arrow pre-sizing** (secondary win from the same profile): pre-allocate the
   chunk builders to the known `max_rows`/`max_bytes` cap so `finish_grow` /
   `MutableBuffer::reallocate` stop churning per row.

## Correctness gate (this touches the MVCC-critical read core)

- Replicates exactly `get_internal`'s rule: newest sequence across tiers wins;
  tombstone hides older; merge-chains defer to `get_internal`. Byte-equivalent.
- TDD: new storage tests (cursor yields newest (value,seq,op) per key, cross-block
  key spanning, tombstone) + engine tests (cross-tier newest-wins, tombstone-skip,
  merge-fallback) BEFORE wiring. Then full suite: `-p forst-rs-storage` (355) +
  `-p forst-rs-engine --lib` (264) + compaction/MVCC integration — all must stay green.
- q4 NexMark spot-check: result count unchanged vs the key-only path.

## Expected impact

Eliminating the per-key `get_internal` second LSM walk should roughly halve the
read-path CPU that dominates q4 → a large step toward RocksDB parity on local dir,
stacking with Spec C2 (arena index) and compaction tuning. Measure via the same
symbolized profile (the two ~4700-sample FilterMap frames should collapse to one).

## RESULT (2026-06-06, measured)

**Implemented + verified.** 266 engine tests green (264 existing byte-equivalent +
2 new get-equivalence oracles covering SST-Put-inline, max-seq-SST-winner,
newer-SST-tombstone-hides, memtable-shadow-fallback, SST-Merge-fallback).

**Profile CONFIRMS the fix worked:** the dominant prefix-scan frames collapsed —
`frs_vec_iter_prefix_open`/`fill_chunk_from_iter`/double-`FilterMap` went from
~4700 samples each to scattered small frames (new top frame = 73); `get_internal`
samples 4600+→32; `fill_chunk_from_iter` 4715→23. The read-path CPU is no longer
the bottleneck.

**BUT q4 wall-time is unchanged: 557s vs 563s (VCM off).** The prefix-scan CPU was
NOT the wall-limiting serial bottleneck on the 18-core host — spare cores absorbed
it. The real wall binder is the WRITE/CHECKPOINT/coordination path:
- Checkpoint every 30s, each **6.6→8.7 GB** (grows with state), durations **2-13.5s**;
  the two ~13s checkpoints align with the src-curve stalls. (RocksDB does incremental
  → small deltas.)
- Steady-state throughput ~176K/s vs RocksDB ~403K/s — cores NOT saturated, so it is
  coordination/backpressure-bound, not CPU-bound.

**Keep the change anyway** — it is the correct RocksDB-parity iteration architecture,
it is the enabler for running with the resident shadow OFF (8c/32g memory fit), and
its freed read-CPU matters MORE on the actual 8-core target than on this 18-core host
(where it read as wall-neutral). The next lever for beating RocksDB on local dir is
the checkpoint cost (incremental delta, not full-state) + steady-state write/compaction
throughput — a separate effort.
