# Lock-free memtable design (RocksDB/ForsT-equivalent, Arrow-native)

**Status:** DESIGN — for review. **Date:** 2026-06-02. **Branch:** forst-rs.
**Lock-free track:** P3 (the high-value, architecturally-hard piece; P1 ArcSwap sst_readers DONE `1df5fa7fc`, P2 MVCC already lock-free on the hot path).

## Goal

Eliminate the memtable's per-shard `RwLock` from the per-record read AND write path so reads and
writes run **fully in parallel** (the user's directive: no pessimistic locks/mutexes on the hot path),
**while preserving the Arrow columnar layout** (the off-heap/zero-copy mandate). Match what RocksDB/ForsT
achieve with their concurrent memtable.

## How RocksDB / ForsT do it

- **Structure:** a lock-free **InlineSkipList** (`MemTableRep`). Keys+values are written into a
  **non-moving Arena** (segmented blocks; once allocated, never moved/freed until the whole memtable is
  dropped). Skiplist nodes hold pointers into the arena.
- **Writes:** `allow_concurrent_memtable_write=true` (default). Multiple writers insert concurrently —
  each claims arena space, builds its node, and CAS-links it into the skiplist. No global lock.
- **Reads:** fully lock-free skiplist traversal. Because the arena never moves, a reader's pointers stay
  valid even as concurrent writers append. Readers never block writers and vice-versa.
- **Sequence numbers:** a single atomic counter; each write claims a seq via fetch-add. MVCC visibility
  is a per-read seq filter over the skiplist's per-key version chain.
- **Flush:** the active memtable is *switched* to immutable (a pointer swap) and a fresh one installed;
  the immutable one is flushed without blocking writes to the new active one.

**Key enabler:** values live in a **non-moving arena**, and the only mutable shared structure is a
**lock-free ordered index**.

## forst-rs today (the divergence)

`VectorizedMemTable` (sharded behind `Vec<RwLock<…>>`, shards=1) already stores rows in **append-only
columnar arenas**: `key_data: Vec<u8>`, `value_data: Vec<u8>`, `sequences: Vec<u64>`, `op_types: Vec<u8>`
— MVCC writes only *append* new rows (existing rows are immutable). The mutable shared state is the
**indexes**: `sorted_index: BTreeMap<Arc<[u8]>, Vec<RowIndex>>` + `unsorted_lookup: HashMap` + a hash
index. The `RwLock` exists to guard those index mutations and the `Vec` appends (a `Vec` realloc *moves*
data, which would invalidate a concurrent reader's offsets).

So forst-rs is *one step* from RocksDB's model: the data is already columnar+append-only; what blocks
lock-freedom is (a) the `Vec` arenas can realloc-and-move, and (b) the `BTreeMap`/`HashMap` indexes
mutate in place under the lock.

## Approaches

### A — Lock-free skiplist index + non-moving segmented columnar arena (RECOMMENDED)

Directly mirror RocksDB while keeping Arrow columnar storage:
- **Arena:** replace each `Vec<u8>` (key_data, value_data) and `Vec<u64/u8>` (sequences, op_types) with a
  **segmented arena** — a list of fixed-size chunks (e.g. 1–4 MiB), appended via an atomic bump-pointer;
  a full chunk triggers a CAS-published new chunk. **Chunks never move**, so a `RowIndex` (chunk_id +
  offset + len) stays valid for concurrent readers regardless of appends. The chunks are still contiguous
  columnar bytes → Arrow `BinaryArray`/primitive arrays can be built zero-copy at flush by wrapping chunk
  buffers (ties into the zero-copy Arrow work).
- **Index:** replace `BTreeMap<Arc<[u8]>, Vec<RowIndex>>` with a **lock-free concurrent ordered map**
  (`crossbeam-skiplist::SkipMap<Box<[u8]>, …>`) keyed by encoded key → a lock-free per-key version chain
  (atomic-linked `RowIndex` nodes, newest-first). Point/prefix/range reads traverse the skiplist
  lock-free; the prefix-scan cursor iterates a skiplist range (no snapshot-clone needed — the skiplist is
  itself the consistent view under epoch protection).
- **Writes:** claim seq (atomic fetch-add), append columns to the arena (atomic bump), CAS-prepend the
  `RowIndex` to the key's version chain (insert the key into the skiplist if absent). Fully concurrent.
- **Reclamation:** `crossbeam-epoch` guards skiplist nodes + retired version chains; arena chunks are
  freed only when the whole (immutable, flushed) memtable is dropped — so no per-op reclamation cost.
- **MVCC:** per-row `sequences` already exist; reads filter the version chain by snapshot seq.
- **Flush / memtable-switch:** active→immutable is an `ArcSwap` pointer swap (reuses the P1 pattern);
  the immutable memtable's skiplist+arena are read-only, so `to_flush_batches` builds Arrow batches with
  no lock and (with the zero-copy work) by wrapping arena chunks directly.

**Pros:** true RocksDB/ForsT-equivalent (lock-free reads + concurrent writes); preserves columnar Arrow
layout; eliminates the prefix-scan snapshot-clone; removes the `unsorted_lookup`/merge machinery (the
skiplist is always sorted, killing the O(N) unsorted scan that hurt heavy joins).
**Cons:** largest rewrite of `VectorizedMemTable`; epoch-reclamation correctness is the dominant hazard;
the existing rich index (inline value cache, prefix-index) must be re-expressed on the skiplist.

### B — Seqlock/optimistic read, keep a brief write lock

Reads run optimistically (read a version counter, read data, re-check; retry on conflict) so they don't
block writers; writers take a short per-shard lock. **Rejected:** a seqlock over a `BTreeMap` that
structurally mutates mid-read is unsound (a reader can observe a torn tree / dangling node). Would
require freezing the index during reads anyway. Doesn't reach true concurrency.

### C — Shard-count tuning + immutable double-buffer (cheap interim)

Keep `RwLock` but (1) raise `memtable_shards` so a write to shard *i* doesn't block reads on shard *j*,
and (2) keep the active/immutable double-buffer for flush. **Tradeoff:** more shards multiply per-probe
prefix-scan traversal (the documented reason shards=1). A useful *interim* mitigation, **measured**, but
not the lock-free goal. Can ship first as a low-risk step while A is built.

## Recommendation

**Approach A**, built in phases, each TDD'd, with **C as an optional measured interim** if a quick win is
needed before A lands. A is the only option that delivers RocksDB/ForsT-grade parallelism *and* keeps the
Arrow columnar layout the rest of the engine (zero-copy reads, flush) depends on.

## Phased plan (each: TDD + the 333-test storage suite as the safety net)

1. **Segmented non-moving arena** behind the existing `VectorizedMemTable` API (internal swap of the
   `Vec<u8>` columns for chunked arenas; `RowIndex` gains chunk_id). No concurrency change yet — pure
   refactor, validated by the existing suite + a "chunk boundary" test.
2. **`crossbeam-skiplist` index** replacing `sorted_index` + `unsorted_lookup` (drop the merge machinery).
   Still under the shard `RwLock` for one commit (behavior-preserving), validated by the suite.
3. **Remove the `RwLock`**: writes = atomic seq + arena bump + skiplist CAS; reads = lock-free traversal;
   `crossbeam-epoch` reclamation. New concurrency tests: N writers + M readers, no torn reads, MVCC
   visibility under concurrent capture/release.
4. **`ArcSwap` active/immutable switch** for flush (reuse P1 pattern); flush builds Arrow batches from the
   immutable arena lock-free (compose with zero-copy Arrow).
5. **Drop `write_mutex`** (lock-free P4) once the memtable insert is the only write synchronization.

## Risks

- **Epoch-based reclamation** (use-after-free / leaks) — the dominant correctness hazard; `crossbeam-epoch`
  + exhaustive loom-style or stress concurrency tests.
- **MVCC visibility** under concurrent writers (per-key version-chain ordering vs snapshot seq).
- **Memory:** segmented arenas + skiplist node overhead vs the current packed `Vec`s; bound by the same
  write-buffer-size flush trigger.
- **Scope:** this is a core-data-structure rewrite — corruption risk is high, so phases 1–2 (behavior-
  preserving refactors validated by the existing suite) de-risk before the concurrency switch in phase 3.
- **`crossbeam-skiplist` / `crossbeam-epoch` deps** — add to the workspace (verify availability/licensing).

## Implementation status (2026-06-02)

- **Phase 1 — segmented non-moving arena: DONE.** `arena.rs` `SegmentedBytes` (e004fa3e7); KEY columns
  → arena (060693d84); VALUE columns → arena (b44ec2b76). `ByteSpan{chunk,offset,len}` per row; null/
  tombstone rows store a zero-length `ByteSpan::default()` sentinel gated by `value_nulls`.
- **Phase 2 — `crossbeam-skiplist` ordered index: DONE (e2fc210a2).** `sorted_index` BTreeMap +
  `unsorted_lookup` + `unsorted_entries` + `sorted_count` + `rowindex_vec_pool` + the merge machinery all
  replaced by one `SkipMap<InternalKey, RowIndex>` (`InternalKey{user_key:Arc<[u8]>, sequence}`, ordered
  user-key ASC / seq DESC via a custom `Ord` — separate fields, no packed-key prefix hazard, pinned by a
  unit test). `merge_if_dirty`/`merge_unsorted_to_sorted` are now no-ops kept for API compat; `freeze` no
  longer merges. Point reads still use `hash_index` (unchanged). This already removed BOTH the unsorted
  linear-scan wall AND (with the no-op merge) the write-lock-during-prefix-scan contention that the prior
  cursor took (the documented ~32% `lock_contended` motivation). Suite: storage 339 + engine 258 green.
- **Phase 3 — remove the `RwLock`: NOT STARTED (design fork below).**
- **Phases 4–5** — deferred (depend on 3).

## Phase 3 design fork (the lock-free read/write switch) — DECISION REQUIRED BEFORE CODING

The shard `RwLock` lives at `ShardedMemTable` (`Vec<RwLock<VectorizedMemTable>>`). To drop it, every
`VectorizedMemTable` write must be `&self`-safe (concurrent). Today writes mutate, per row: `key_arena`/
`value_arena` (`SegmentedBytes`, `&mut append`), `key_spans`/`value_spans`/`value_nulls`/`sequences`/
`op_types` (`Vec::push`), `hash_index` (`FxHashMap`), `next_sequence`, `memory_used`. The `index` SkipMap
is already `&self`. Two viable approaches, with a real correctness/perf tradeoff:

### A — Skiplist-only (SAFE, no `unsafe`, but changes point-read complexity)
Store the full row IN the SkipMap node: `SkipMap<InternalKey, RowVal>` where `RowVal { value:
Option<Arc<[u8]>>, op_type }` (seq is in the key). Then **delete** `hash_index` AND all columnar storage
(`*_arena`, `*_spans`, `value_nulls`, `sequences`, `op_types`) — the skiplist is the single source for
point reads, range reads, and flush.
- **Point read** = `index.lower_bound(Included(InternalKey{user_key, read_seq}))`; the first entry is the
  newest version with `seq ≤ read_seq` (within a user key, higher seq sorts first, so `read_seq` lands
  at-or-after the newer-than-read_seq versions). O(log N), lock-free.
- **Writes** = `index.insert` (`&self`) + atomic `next_sequence`/`memory_used`. No other shared mutation →
  `RwLock` drops out cleanly; no `unsafe`, no concurrent arena, no concurrent hashmap.
- **Flush** = iterate `index` (already done) reading value from the node.
- **Risk:** point reads become **O(log N)** vs today's O(1) `hash_index`. With no-flush mode keeping a
  multi-GB resident memtable, that is ~log2(10s of millions) ≈ 25 variable-length key comparisons per
  point get — a possible regression on point-read-heavy queries (the memory flags hash_index O(1) as hot).
  Mitigatable by keeping a small **concurrent** point cache, but that reintroduces approach B's hard part.

### B — Keep O(1) point reads (concurrent `hash_index` + concurrent arena) — FASTER, but `unsafe`/dep-heavy
Make each mutated structure concurrent: (1) `SegmentedBytes` → lock-free `&self` append (atomic
bump-pointer + CAS chunk publish over `UnsafeCell<[u8]>` chunks, `unsafe impl Sync`); (2) the 5 columnar
`Vec`s → a concurrent append-only typed column (atomic row-offset claim, non-moving segments) — or fold
the per-row data into the SkipMap node and keep columns only for flush; (3) `hash_index` → a concurrent
map (`dashmap`, or a second `SkipMap`, or sharded) with a concurrency-safe inline-value cache.
- **Pros:** preserves O(1) point reads + the inline-value fast path; true RocksDB-grade parallelism.
- **Cons:** hand-rolled `unsafe` lock-free arena (silent-corruption failure mode — the worst outcome for a
  storage engine), `crossbeam-epoch` reclamation, a new concurrent-map dependency, and the most code. The
  dominant correctness hazard; must be gated by N-writer/M-reader stress + (ideally) `loom` tests.

### Recommendation
**Gate phase 3 on a measurement first.** Phase 2 already removed the two documented memtable lock costs
(unsorted scan + write-lock-during-scan), so the *marginal* benefit of dropping the brief per-op
`RwLock` — under Flink's one-writer-per-slot model with `shards=1` — is now **unmeasured**. Before
investing in the high-risk approach B, run the post-phase-2 stack and check whether `RwLock` time is still
material in the join hot path (instrument-before-build, per project discipline). If it is: prefer **A**
unless a point-read microbench shows the O(log N) regression is real, in which case do **B** with the
arena built as an isolated, stress-tested primitive first. Either way, phase 3 is a from-scratch
concurrent change and must NOT be rushed in with the perf benefit unverified.

## Relationship to other specs

- Composes with [zero-copy Arrow SST](2026-06-02-zero-copy-arrow-sst-format.md): flush wraps arena chunks
  as Arrow buffers (no copy). Builds on the [lock-free engine](2026-06-02-lock-free-engine-design.md) P1
  ArcSwap pattern. Removing the `unsorted_lookup` scan also fixes the O(N) empty-prefix-probe cost seen in
  the heavy-join profiles.
- **Not a q7 fix** on its own (q7 is coordination-bound), but it is the central piece of the "parallel
  reads/writes, all-forst-rs, Arrow-native" mandate and helps every concurrent/keyed query.
