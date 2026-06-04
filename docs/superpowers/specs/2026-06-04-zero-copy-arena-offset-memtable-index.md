# Zero-copy arena-offset memtable index — better than RocksDB's InlineSkipList

**Date:** 2026-06-04
**Status:** DESIGN. Step 1 (inline-key) LANDED + measured (137M→34M allocs, 4×). Steps 2–3 (true
zero-copy offset index) designed below; gated on the lock-free `&self`-append arena.

---

## 1. Why this matters (measured)

`vmmap` proved the q4 memory wall is **allocation COUNT**, not size: the memtable indexes allocate a
**heap key per entry**, hitting ~137M live small allocations (≈32GB, fragmentation + malloc-lock
contention) — even though the key bytes are **already** in `key_arena` (columnar, for flush/value
reads). The index keys were pure duplicates. RocksDB stays flat because its arena `InlineSkipList`
holds keys in a bump arena (≈0 per-entry mallocs).

**We can do BETTER than InlineSkipList:** InlineSkipList copies each key *into* its node (one copy in
the arena). forst-rs already stores the key once in `key_arena` for the columnar path — so an index
that stores only the **arena offset** copies the key **zero** additional times. The key lives exactly
once; both the index (ordering) and the flush/value path (bytes) reference that one copy.

## 2. The two indexes today (`VectorizedMemTable`)

- `index: BTreeMap<InternalKey, RowIndex>` — sorted index for range/prefix scans. `InternalKey` held
  `user_key: Arc<[u8]>` → one heap alloc per version. **Step 1 (LANDED)** made it inline `SmallVec<[u8;48]>`
  → no per-entry malloc (cheap 48B memcpy instead). Measured: 137M→34M allocs.
- `hash_index: HashMap<Box<[u8]>, HashEntry>` — O(1) point lookups. Key is `Box<[u8]>` → one heap alloc
  per unique key. `HashEntry` adds `inline_value: Option<Box<[u8]>>` + `row_indices: Vec<RowIndex>`.
  This is the **dominant remaining ~34M allocs**.
- Columnar: `key_arena: SegmentedBytes` (non-moving chunks) + `key_spans: Vec<ByteSpan>` indexed by row
  offset; `key_at(offset) -> &[u8]`. **The key already lives here.**

## 3. The design — offset-only, zero key copy

**Core idea:** index entries store the **row offset** (u32) + sequence; the key bytes are read from
`key_arena` via `key_at(offset)` during comparison. No key in the index node at all.

```
KeyArena (shared, append-only):
    arena: SegmentedBytes        // chunks never move
    spans: ChunkedVec<ByteSpan>  // append-only, never reallocates (so a row offset is stable)
    fn key_at(&self, row: u32) -> &[u8]

Sorted index:  ordered structure of (row: u32, seq: u64), comparator = key_at(a.row) vs key_at(b.row),
               then seq DESC.
Point index:   hash map of (row) keyed by hash(key_at(row)), eq = key_at(row) == probe.
```

**The hinge — the comparator needs the arena.** `std::BTreeMap`/`HashMap` use self-contained
`Ord`/`Hash`, so the key must reach the arena. Two sound ways:

- **(A) `Arc<KeyArena>` in the key (SAFE, preferred):** `IndexKey { arena: Arc<KeyArena>, row: u32, seq: u64 }`.
  `Arc::clone` per entry is a **refcount bump, NOT an allocation** — so still zero per-entry mallocs,
  and zero key copy (Ord reads `arena.key_at(row)`). Requires the arena to be **`&self`-append**
  (interior-mutable) so one `Arc<KeyArena>` is shared by the memtable (appends) and every index key
  (reads). **Prerequisite: finish the lock-free `&self`-append arena** (`SegmentedBytes::append` is
  `&mut self` today — the documented "later phase"; `spans` must also become an append-only,
  never-reallocating chunked vec so a stored `row` offset stays valid under concurrent append).
- **(B) Raw `*const KeyArena` (UNSAFE, RocksDB-style):** smaller keys (no Arc), but self-referential +
  move/aliasing hazards (the arena must be pinned; Ord-during-`&mut`-insert aliases). Higher risk.

**Recommend (A):** safe, and the lock-free `&self` arena is already the in-progress direction
(SegmentedBytes chunks already never move; only `append` mutability + a chunked `spans` remain).

**Better than InlineSkipList because:** (1) no key copy into nodes (offset only); (2) the key is shared
with the columnar flush path (no second copy); (3) the ordered structure can be a cache-friendly
B-tree or an arena skiplist — independent of key storage; (4) point + range indexes share ONE key
store. Net per-entry index cost: ~16 bytes (Arc ptr + u32 + u64) + a refcount bump, vs InlineSkipList's
full key bytes copied per node.

## 4. Migration path (incremental, each measurable)

1. **Inline-key (DONE, measured 4×):** `InternalKey.user_key` `Arc<[u8]>`→`SmallVec<[u8;48]>`. Safe,
   tests green. Eliminates the sorted-index per-entry malloc. (Keeps a 48B inline copy — removed in 3.)
2. **hash_index dedup:** replace `Box<[u8]>` key with the same inline small-key (custom type impl
   `Borrow<[u8]>` + `Hash`/`Eq` on the slice so `get(&[u8])` needs no probe-copy), and make
   `HashEntry.inline_value` reference `value_arena` (offset) instead of `Box<[u8]>`. Removes the bulk of
   the remaining 34M allocs.
3. **Offset-only (true zero-copy):** land the lock-free `&self`-append `KeyArena` (arena + chunked
   spans), switch both indexes to `Arc<KeyArena>`-carrying offset keys (design A). Removes the inline
   copy AND the duplicate key bytes. Expected: alloc count → ~B-tree-node count (~10M→~1M), RSS →
   ≈ key_arena + value_arena + JVM (the data itself), no fragmentation, no malloc-lock contention →
   the measured uncompressed burst (579–758K/s, >2× RocksDB) **sustains**.

## 5. Verification (per step)

`vmmap` DefaultMallocZone allocation COUNT (137M → 34M done → target ~1–10M) + a q4 100M run to
completion on a machine with free RAM ≥ RSS (so compression doesn't confound). Full engine+storage
suite green at each step (the index is the core; TDD + the existing 339 storage / 312 engine tests).

## 6. Status

Step 1 landed + measured. Steps 2–3 are the focused follow-on; step 3 (the true zero-copy offset index)
is the user's "better than InlineSkipList, no copy" target and depends on completing the lock-free
`&self`-append arena. Combined with the already-landed resident-bloom-skip + global shadow/WBM budgets,
this removes the last memory driver of the q4 (and q7/q9/q16/q20) decay.
