# forst-rs RocksDB-parity memory model — design (fit 8c/32g)

**Date:** 2026-06-06. Brainstorming spec. Fixes the data-architecture memory bloat that makes
forst-rs unfit for the 8c/32g target. Approved scope: all three components, implemented together,
one byte-equivalence gate + one 8c/32g verification run.

## Problem (proven)

forst-rs holds **~23 GB RAM for q4's ~1.8 GB of on-disk state** (55.9 M live allocations) vs
RocksDB's ~6.4 GB. On 8c/32g this exceeds the cap → the incremental checkpoint fails with IO
(disk/mem pressure) and the job crash-loops at ~65M/98M events. Root cause is the data
architecture, in three multiplied sources:

1. **Dual per-row index with duplicated keys.** Each `VectorizedMemTable`
   (`forst-rs-storage/src/memtable/vectorized.rs`) keeps BOTH `index: BTreeMap<InternalKey,
   RowIndex>` and `hash_index: HashMap<KeyBuf, HashEntry>`. `InternalKey.user_key` and `KeyBuf`
   are each `SmallVec<[u8;48]>` that **own a key copy** (heap-spill > 48 B; q4 composite keys
   spill). Keys thus live 3× (key_arena + BTreeMap + HashMap) → millions of heap allocs.
2. **Resident shadow = whole flushed memtables retained in RAM** (Tier-2; column_family.rs
   `resident_flushed`). RocksDB has no such tier — it re-reads SST blocks via a bounded block
   cache. So flushed state is duplicated in RAM *with its indexes*.
3. **× ~12 DbImpl instances** (Join×4 + GroupAggregate×4×2), each with its own caches/indexes.

## Design

### Component 1 — Drop the resident shadow; serve flushed reads from the bounded block cache
- Stop populating Tier-2 (`add_resident_flushed*`); remove the resident-shadow tier from the read
  merge in `build_lazy_prefix_key_stream` (db.rs). Reads become: active memtable + immutables +
  SSTs, with SST data blocks served by the existing `ShardedClockCache` (decoded blocks).
- The decode-on-miss cost the shadow was added for is covered by a correctly-sized **shared**
  decoded-block cache (Component 3). RAM becomes bounded by cache size, not state size — RocksDB's
  read model.
- Keep `FRS_RESIDENT_BYPASS`-style gating only if needed for A/B; target is shadow OFF by default.

### Component 2 — Single arena-referencing ordered index (kill duplicate key copies)
- Drop `hash_index` (the `HashMap<KeyBuf,…>`). Keep ONE ordered index for the interval-join /
  MapState prefix-scan path.
- The index key references the **key_arena span** instead of owning a copy. `SegmentedBytes` is
  append-only (existing chunks never move), so a `ByteSpan` stays valid for the memtable's life.
- **Mechanism (resolves the std-`BTreeMap` no-custom-comparator constraint):** the index key is a
  newtype `ArenaKey { span: ByteSpan, arena: *const SegmentedBytes }` (or an `Arc<SegmentedBytes>`
  handle) whose `Ord`/`Eq` impl reads the bytes *through the arena handle* — so std `BTreeMap` works
  unchanged, the key bytes are NOT copied (they stay in the arena), and only a small fixed-size
  `ArenaKey` (span + handle, ~24 B, no heap) sits in each node. The arena handle is the memtable's
  own arena, so the raw-pointer form is sound for the memtable's lifetime; tests assert the
  no-realloc invariant. Point-get becomes a `BTreeMap` lookup (the role `hash_index` played).
- Net: each key stored ONCE (arena), one index, ~⅓ the index allocations. This is the
  correctness-critical hot-path change — gated hardest.

**IMPLEMENTED (2026-06-06) — adjusted for two realities found during build:**
1. **`forst-rs-storage` is `#![forbid(unsafe_code)]`**, so the spec's primary
   `ArenaKey { span, arena: *const SegmentedBytes }` (raw-pointer deref in `Ord`) is
   not permitted. The safe `Arc<SegmentedBytes>` variant would force a lock/refcount on
   EVERY `Ord` comparison in the BTreeMap hot path — a worse trade than the inline key.
2. The "millions of heap allocs from triple key storage" premise was **already
   eliminated** by the 2026-06-04 inline-key change (`InternalKey.user_key` is
   `SmallVec<[u8;48]>` — inline, no heap for the ≤48 B NexMark keys).

So C2 shipped its **safe, memory-reducing core**: the `hash_index`
(`HashMap<KeyBuf, HashEntry>` — a SECOND full copy of every key + a per-key
`HashEntry` + the HashMap buckets, ~56 B/key) is **removed entirely**. All point
lookups (`get` / `get_borrowed` / `get_into` / `get_pinned_ptr` /
`collect_merge_operands`) now resolve from the sorted BTreeMap `index` via a new
`idx_newest_visible(key, read_seq)` range scan (entries are (key ASC, seq DESC) → the
first entry with seq ≤ read_seq is the newest visible — byte-equivalent to the old
`hash_index` + `find_latest`). `find_latest`/`find_latest_borrowed` deleted. The
arena-referencing `ArenaKey` (to also drop the inline `InternalKey` copy) is the
deferred remainder — it needs an `unsafe` exception or an interior-mutable Arc arena;
not worth the per-compare cost given inline keys already avoid the heap.
- **Gate (passed):** 355 `forst-rs-storage` + 266 `forst-rs-engine` tests green,
  byte-equivalent (get/scan/merge-chain/tombstone/MVCC unchanged). Two-phase landing:
  (1) reroute reads to the BTreeMap with `hash_index` still maintained → suite green
  proves the read path; (2) delete the field + all 6 write-maintenance blocks.
- **Perf note:** point-get goes O(1) hash → O(log n) BTreeMap, but memtables are
  flush-bounded/small, so the delta is minor; the win is per-key memtable memory, which
  is what the 8c/32g OOM A/B showed is the binding resource (WBM cap needed to avoid
  OOM during the burst).

### Component 3 — Shared per-slot resources (managed-memory parity)
- One `Arc<ShardedClockCache>` per slot, shared across all DbImpls (today it's per-DbImpl,
  db.rs:625/4558). Cache keys salted by `db_id` to avoid cross-instance `(file_num, offset)`
  collisions. Sized from a configured fraction of the slot memory budget.
- Existing global WBM budget (`runtime_tuning.rs`) bounds total memtable bytes; existing global
  shadow budget becomes moot once Component 1 lands.
- Result: total native RAM = configured fraction of 32 GB, independent of operator×parallelism —
  RocksDB `getSharedMemoryResourceForSlot` parity.

## Allocator (already landed, keep)
jemalloc global allocator in `forst-rs-ffi` with `disable_initial_exec_tls` (dlopen-safe on Linux)
+ `_rjem_malloc_conf` decay. Returns freed memory to the OS; necessary on the constrained target.

## Correctness gate (continuous, not just at the end)
Although implemented "all at once," run the FULL gate after EACH component so regressions are
attributable: `cargo test -p forst-rs-storage` (355) + `-p forst-rs-engine --lib` (264) + the
compaction/MVCC integration tests. Output must be byte-equivalent (get/scan/merge-chain/tombstone/
snapshot semantics unchanged). New unit tests: arena-referencing index (ordered yield, point-get,
spill keys, versions) and shared-cache cross-instance isolation (no key bleed).

## 8c/32g verification (one run, the success criterion)
In the `--cpus=8 --memory=32g` container (host disk mounted for SST/checkpoint dirs to avoid the
60 GB Docker-VM disk limit): q4 must FINISH 98M with **peak anon RSS well under 32 GB** (target
≤ ~12-16 GB, RocksDB-class), no checkpoint IO/OOM failure. Then q0-q22 sweep + the RocksDB A/B.

## Phase 2 (next spec): full disaggregation — ForSt parity (NOT built now)

The final target is ForSt's disaggregated model: **S3 as primary store, local disk as hot cache
only**. This is a SEPARATE later phase — it fixes *disk*/checkpoint-cost/cloud-scale, NOT the RAM
bloat (memtables + indexes + decoded-block cache are in RAM regardless of where SSTs live), so it
must come AFTER the memory model. forst-rs already has partial pieces (OpenDAL S3 backend /
`primary-dir`, FRS-LOCAL-DIRECT-READ write-through cache, incremental checkpoint via
`new_ssts`/`shared_ssts`). Phase 2 scope: S3 as true primary, a **tiered bounded local SST *file*
cache** with eviction, async coalesced remote reads, FileMapping refcounting for checkpoint linking.

### Seams Phase 1 MUST preserve so Phase 2 isn't boxed out
- **Cache layering:** keep the read path as decoded-block-cache → SST-source, where the SST source
  is an abstraction (local file today, local-cache-over-S3 in Phase 2). Don't hardwire local-file
  assumptions into the block-cache or read-merge path.
- **Shared-resource budget (Component 3):** make the per-slot budget object able to also hold a
  *local-file-cache* budget later (one more bounded tier), not just the block cache + WBM.
- **SST access via the existing `FileSystem`/OpenDAL trait** (already abstracts local vs S3) — the
  memory work must not bypass it with direct local-fs calls on the read path.

## Risk & mitigation
Component 2 touches the hot, correctness-critical memtable index. Mitigation: keep the existing
`BTreeMap`/`HashMap` types behind a feature/flag during bring-up so the suite can A/B old-vs-new
index for byte-equivalence; remove the old path only once green. The arena-span comparator + the
append-only-chunk invariant (no realloc) are the load-bearing assumptions — assert them in tests.
