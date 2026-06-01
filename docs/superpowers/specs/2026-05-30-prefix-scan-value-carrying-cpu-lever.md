# 2026-05-30 — The heavy-query CPU lever: value-carrying prefix scan

## Status
DESIGN (not yet implemented). This is the concrete next optimization after the
2026-05-30 reliable sweep established the goal is **CPU-bound, not S3-bound**.

## Context: corrected root cause
The reliable q0–q22 sweep (real JM wall-clock) measured forst-rs/S3 = **0.33×**
vs rocksdb/local (goal: ≥3×). Heavy joins/windows (q4/q7/q9/q15/q16/q18/q20) cap
>1500s at ~24K rec/s where rocksdb does ~170K/s. Isolation (q7-LOCAL ran at the
SAME ~24K/s as q7-S3 before a disk-full crash; prior 99.9% cache-hit + symbolized
profile) shows the wall is **engine per-record CPU**, largely storage-independent.
See [[project_full_sweep_2026-05-29]].

## The inefficiency (db.rs)
`prefix_scan_iter_owned_arc*` (db.rs:5034) is the join/rank probe path:
```rust
let mut inner = self.build_lazy_prefix_key_stream(cf, prefix)?;   // yields KEYS
Ok(Box::new(inner.filter_map(move |key_arc|
    match db.get_arc(&cf_handle, key_arc.as_ref()) {              // re-lookup VALUE
        Ok(Some(value)) => Some(Ok((key_arc, value))), ...
)))
```
`build_lazy_prefix_key_stream` (db.rs:5067) does a lazy k-way merge over the LSM
tiers (active memtable cursor + imm + resident-flushed + overlapping SSTs) and
yields each visible **key** once, deduped by sequence. Then for EACH key the
caller calls `get_arc` → `get` → `get_internal` (db.rs:6657) which **re-walks all
tiers from scratch** (active hash get + imm + resident + SST block read) to
resolve the value.

So a K-key prefix scan performs **K full point-lookups AFTER the merge already
visited the data holding those values**. q9's ROW_NUMBER rank re-iterates each
partition O(K)/record over millions of records → this doubled traversal is a
dominant component of the ~24K/s wall.

## The fix: carry the winning entry's value through the merge
The k-way merge already determines, per unique key, the **winning entry** (highest
sequence across tiers) — that is exactly what `get_internal` recomputes. Surface
the winning entry's `(value, op_type, sequence)` from the merge so the caller uses
it directly:

- **Put** (the common join case): the winning entry's value IS the visible value
  (`get_internal` returns `entry.value` for the highest-seq Put). Yield it
  directly — **skip `get_arc` entirely**.
- **Delete/SingleDelete**: key not visible → skip (matches `get_internal` → None).
- **Merge**: the visible value needs operand collection across lower tiers +
  `apply_merge_operator`. The merge stream cannot resolve this alone → **fall back
  to `get_arc`** for these keys only (rare in joins; correctness preserved).

### Required mechanical changes
1. `TierKeySource` (memtable cursor + SST block source) must surface
   `(key, value, op_type, sequence)` not just `key`. The memtable
   `prefix_scan_cursor` entries already hold value+op+seq; the SST block decode
   (`read_data_block` → RecordBatch) already has value+op+seq columns. No new
   reads — just stop discarding the columns already decoded.
2. `LazyPrefixIter` dedup: when it picks the highest-seq entry for a key (which it
   already must, to dedup correctly), emit that entry's `(op, value, seq)`.
3. New `prefix_scan_iter_owned_arc_valued` that yields `(key, op, Option<value>)`;
   the FFI/Java caller uses value for Put, skips Delete, and calls back into
   `get_arc` only for Merge.

### Correctness argument (zero-tolerance)
- `prefix_scan` reads the LATEST view (no snapshot seq filter), and the merge's
  highest-seq entry per key IS the latest — identical to what `get_internal`
  returns for Put/Delete. So Put/Delete fast-paths are byte-identical.
- Merge is the ONLY op needing cross-tier resolution, and it falls back to the
  existing `get_arc` path → unchanged behavior.
- Guard empirically: the storage + engine suites have prefix-scan + merge +
  tombstone correctness assertions; q3/q4/q7 output must stay byte-identical to
  rocksdb (prior validation baseline). Add a direct unit test:
  scan a CF with mixed Put/Delete/Merge across active+imm+SST tiers and assert the
  valued iterator == the key-iterator+get_arc result for every key.

## Measurement (machine is disk-full → use rate, not completion)
This machine is 99% full (cannot run forst-rs/LOCAL heavy queries; S3 heavy
queries cap at 1500s). Validate via **rate (rec/s) over the first ~5 min** of
q9-S3 (and q7-S3) vs the 24K/s baseline — a rate lift is the proxy for the
completion-time win. Then a full sweep on a disk-headroom machine for the binding
q0–q22 total.

## Honest expected payoff
Eliminating the second per-key tier walk should materially raise the rank/join
probe rate, but **one optimization will not reach 3× alone** (24K→~2×=48K/s is
still <rocksdb 170K/s). It compounds with: cutting per-probe FFM/Arrow crossing
overhead, and reducing `VectorizedMemTable::batch_insert` cost (the other profiled
half). Prior CPU attempts (FxHash, prefix_index O(N²) removal, live_sst_files
non-cloning, write-back) each helped but none alone broke the wall — the cost is
diffuse, so expect a multi-lever, multi-session campaign.

## CODE-REALITY UPDATE (2026-05-30, after reading the merge internals)
Investigating the implementation surfaced that the lever is DEEPER than the
"values are already in the merge" framing above:
- `MemTierCursor` (sharded.rs:888) holds **keys only** — `shards: Vec<Vec<Arc<[u8]>>>`
  — snapshotted by `VectorizedMemTable::prefix_scan_keys`, which DELIBERATELY
  returns key-only Arcs (cheap clone off the index) to keep the cursor's resident
  footprint `O(num_shards)`. The merge does NOT currently carry values.
- So "carry the value through the merge" requires `prefix_scan_keys` to ALSO fetch
  each value+op+seq from columnar storage at snapshot time — moving the value
  resolution, not eliminating it. Net win = replacing K per-key `get_arc` tier
  walks with one batched per-snapshot value fetch; real but smaller than implied,
  and for keys resident in the ACTIVE memtable `get_arc`'s first stage is already a
  single cheap hash lookup (`ShardedMemTable::get`), so the saving is largest only
  for SST-resident keys.
- Cross-tier winner MUST be by MAX SEQUENCE: `build_lazy_prefix_key_stream` does
  NOT order imm sources newest→oldest (it lacks the `.rev()` that `get_internal`
  uses), so a "first source wins" shortcut would return STALE values. Correct
  implementation must thread `sequence` through every source and pick the max.

This makes it a genuine multi-layer refactor (prefix_scan_keys → MemTierCursor →
TierKeySource → LazyPrefixIter winner-by-seq → new FFI method) with uncertain
per-lever payoff — consistent with the diffuse-cost finding (FxHash/prefix-removal/
write-back each helped but none alone broke the wall).

A SEPARATE, possibly higher-value sub-lever surfaced: the `prefix_scan_cursor`
SNAPSHOT itself is rebuilt EVERY probe (per-shard `prefix_scan_keys` scan + Arc
clones), and q9's ROW_NUMBER rank re-scans the SAME partition prefix once per
record → millions of redundant snapshot rebuilds. Reusing the snapshot across
consecutive same-prefix probes would cut this, but carries the same staleness
hazard (state mutates between records) and needs an invalidation hook.

## Why not implemented in this session
Correctness-critical merge-machinery refactor (touches TierKeySource +
LazyPrefixIter + SST/memtable cursors) under a zero-tolerance correctness mandate,
on a machine where the result cannot be cleanly measured. Rushing it at session
end risks a silent stale-value regression. Implement + test + rate-measure as the
first task of a focused next session (ideally with disk headroom).

## MEASURED NEGATIVE RESULT (2026-05-30): CF-hoist micro-opt is perf-neutral
Before the full value-carrying refactor, I landed the SAFE subset: hoist the
per-key `lookup_cf_by_id` out of the prefix-scan `filter_map` (call `get_internal`
with a cached `cf_data` instead of `get_arc` per key). Byte-identical semantics;
254 engine tests pass. **q9-S3 rate UNCHANGED: ~20.2K rec/s (6.9M @ 341s) vs the
~20K/s baseline (15.2M @ 805s).** So the per-key CF map lookup + RwLock read is
NOT a measurable component of the wall. The cost is the per-key TIER WALK itself
(`ShardedMemTable::get` hash + columnar value fetch, repeated per key) + the
write-side `VectorizedMemTable::batch_insert`. KEPT (clean, harmless saving) but it
does not advance the goal. Confirms the diffuse-cost picture: shaving per-key
setup doesn't help; only eliminating the redundant tier WALK (the deep
value-carrying refactor) or cutting batch_insert can move ~20K→~60K/s. Instrument
-driven: tried the cheap safe lever, measured, ruled it out.

## ★ CRITICAL CORRECTION (2026-05-30, traced before implementing): value-carrying is likely NEUTRAL — wrong lever
Tracing the implementation through the memtable internals overturns the premise:
- To carry values, `prefix_scan_entries` must read each value from `value_data[offset]`
  during the scan. But that value read is **INHERENT** — you must read the value to
  return it; `get_arc`/`get_internal` reads it too. Value-carrying does NOT eliminate
  the value read.
- The ONLY thing value-carrying eliminates is the SECOND `key→RowIndex` **hash-probe**
  (get_arc re-hashes the key to re-find the row the scan already located). That is the
  SAME class of micro-saving as the CF-hoist (removing a per-key `lookup_cf_by_id` +
  RwLock), which was MEASURED perf-NEUTRAL (q9-S3 held at ~20K/s). => value-carrying
  would almost certainly be neutral too. **Do NOT build the multi-file merge refactor —
  it is the wrong lever.** (This trace SAVED that wasted effort.)

## REVISED lever ranking — the real per-scan overhead
The dominant suspect is now the **per-scan cursor SNAPSHOT rebuild**, not value
resolution. `ShardedMemTable::prefix_scan_cursor` (sharded.rs:626) does, on EVERY
scan: for each of 16 shards → `write()` lock + `merge_if_dirty()` + `prefix_scan_keys`
(BTreeMap range collect + unsorted_lookup scan) + `keys.sort()`. That is
O(num_shards × (K + K log K)) PER SCAN. q9's ROW_NUMBER rank re-scans the same
partition once per record → the snapshot is rebuilt K times for a K-key partition →
~O(K² log K) total, vs rocksdb's O(K) iterator-per-scan. rocksdb achieves ~7× better
on the SAME operator (same O(K) re-iteration), so the gap IS this per-scan engine
constant, not the operator.

Candidate optimizations (for a focused session with measurement):
1. **Cheaper snapshot:** the per-shard `keys.sort()` is only needed because the
   prefix_index fast path returns insertion-order; if the scan reads from the SORTED
   `sorted_index` (BTreeMap, already ordered) and merges the small unsorted delta, the
   per-shard sort can be dropped or bounded. Verify the sort is actually on the hot path.
2. **Reuse snapshot across same-prefix scans** (the staleness-gated lever): q9 rank
   re-scans the SAME prefix per record; cache the cursor snapshot keyed by (cf, prefix,
   memtable-version) and invalidate on any write to that prefix. Highest potential win,
   but needs a correct invalidation hook (staleness = wrong results).
3. **Avoid the 16-shard fan-out per scan** when the prefix maps to few shards.

Measure each via the q9-S3 rate proxy (~5 min) vs the 20K/s baseline. None is a
micro-opt of the kind CF-hoist ruled out — they cut the per-scan SETUP, which scales
with the rank's re-scan count.

## ★★ SECOND MEASURED NEGATIVE RESULT (2026-05-30): FRS-SCAN-ARCKEY perf-neutral
IMPLEMENTED the precise lever from the revised ranking: `sorted_index` keys changed
`Vec<u8>` → `Arc<[u8]>` so the per-scan cursor snapshot does `Arc::clone` (refcount
bump) instead of `Arc::<[u8]>::from(key.as_slice())` (heap alloc + memcpy per matching
key), and the merge insert reuses the `Box<[u8]>` allocation via `Arc::from`.
Correctness-clean: storage 321/0 + engine 254/0 + integration green. **q9-S3 rate
UNCHANGED: ~19.3K rec/s (6.63M @ 343s) vs ~20K/s baseline.** KEPT (reduces allocation
pressure, aligns with the zero-copy principle, harmless) but it does NOT move the wall.

**Two measured negatives now (CF-hoist + Arc-key) DEFINITIVELY rule out per-scan
allocation/lookup as the wall.** Combined with the analysis that value-carrying only
saves a hash-probe (also that class), the ~20K/s heavy-query wall is NOT a single
micro-optimizable hotspot. It is the AGGREGATE of: (a) inherent per-op work (BTreeMap
range re-navigation touching K nodes per scan + value memcpy + FFM/Arrow chunk encode),
MULTIPLIED BY (b) q9 ROW_NUMBER rank's O(K)/record partition re-iteration (a FLINK
OPERATOR pattern, immutable). rocksdb runs the SAME operator pattern ~7× faster, so the
gap is forst-rs's per-op CONSTANT (vectorized-columnar memtable + FFM + Arrow) vs
rocksdb's skiplist — an architectural property, not a hotspot. Closing it would require
a fundamentally cheaper per-scan path (e.g. a held positioned iterator that avoids
re-navigating the BTreeMap per scan — but the cursor cannot hold the shard lock across
emit without blocking writers; that is the core design tension), i.e. a memtable
redesign, not an optimization. Snapshot REUSE across same-prefix scans remains the only
candidate that attacks (b)'s amplification, and it carries the staleness hazard.

## Cross-refs
- [[project_full_sweep_2026-05-29]] (the corrected CPU-bound diagnosis + 0.33×)
- 2026-05-30-blockcache-floor-and-stall-profile-ranking.md (the ~50/50 profile)
