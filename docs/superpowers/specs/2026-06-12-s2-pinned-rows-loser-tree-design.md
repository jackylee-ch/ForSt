# S2 Refinement — Pinned Rows + Loser-Tree Merge (roadmap step ④ / L3)

**Date:** 2026-06-12
**Status:** IMPLEMENTABLE SPEC (no code changed)
**Parent:** `2026-06-11-streaming-read-redesign-design.md` §2.2 (S2 sketch);
`2026-06-12-q9-q20-longscan-roadmap.md` L3.
**Target:** the two unaddressed components of q20's recorded **21.6 %
prefix-scan CPU share** — (a) two `Arc::from` heap allocations per emitted SST
row, (b) the O(sources)-twice-per-key two-phase merge scan.

---

## 1. Today's code, post-prefetcher (everything below verified against HEAD 277158765)

The BlockPrefetcher landed and reshaped `TierKeySource::Sst` — the S2 sketch
in the streaming design predates this; the refinement below is against the
NEW shape:

- `TierKeySource::Sst { fetcher: BlockPrefetcher, lower, upper, buffered:
  Vec<SstHeadRow>, pos }` (db.rs:10614-10620). Replenish =
  `fetcher.next_decoded()` (db.rs:10668; prefetch.rs:250) returning a
  **`DecodedBlock`** — `Arrow(RecordBatch)` or `Kv(Arc<KvBlock>)`
  (reader.rs:49-55), Arc-shared with the decoded-block cache
  (reader.rs:407-409 hit path).
- Per accepted row the source pays the alloc pair:
  `key: Arc::<[u8]>::from(view.key)`, `value: view.value.map(Arc::from)`
  (db.rs:10722-10727, `SstHeadRow` db.rs:10634-10640). The upper-bound
  early-termination calls `fetcher.terminate()` (db.rs:10739, prefetch.rs:241)
  — this contract must survive.
- The merge, `LazyPrefixIter::next_with_value` (db.rs:10905-11000): Phase A
  scans ALL sources for the min (db.rs:10920-10959, with per-source
  dedup-past-`last_emitted` :10927-10935), Phase B re-peeks ALL sources at
  min to detect memtable presence / pick max-seq SST and advance duplicates
  (db.rs:10965-10981), Phase C decides Put/Fallback/tombstone-skip
  (db.rs:10986-10998). The code itself concedes a heap "would save ~3×"
  (db.rs:10793-10800).
- Emit boundary: `last_emitted: Option<Arc<[u8]>>` (db.rs:10814),
  `ValueDecision::Put(Arc<[u8]>)` (db.rs:11004-11008); the FFI consumes
  `Box<dyn Iterator<Item = (IterKey, IterValue)>>` (Arc pairs, lib.rs:4884)
  and memcpys each field into the chunk via `fill_chunk_from_iter`
  (lib.rs:5071).
- **The key-bytes trap (new finding, drives the design):** v2 KV blocks are
  prefix-compressed; `KvBlock::for_each_row` reconstructs every full key into
  a **reused scratch `key_buf`** (kv_block.rs:308-317, `read_row_at`
  :485-488) — `view.key` is INVALID after the callback returns. Only
  `view.value` borrows the stable block `payload` (kv_block.rs:203-210 —
  "owns the decompressed payload bytes ... values borrow the payload; keys
  are reconstructed into a reused scratch"). ⇒ pinning the block alone is
  NOT sufficient for keys; a per-block key arena is mandatory for v2 (and
  unnecessary for v1 Arrow, whose `BinaryArray` key bytes are stable in the
  batch buffers).

---

## 2. Design

### 2.1 W1 — pinned block buffer (alloc-free rows)

Replace `buffered: Vec<SstHeadRow>` + `pos` with a reusable, pin-holding
block buffer owned by each `TierKeySource::Sst`:

```rust
/// One decoded block's accepted rows, alloc-free after warm-up.
struct SstBlockBuf {
    /// Pin: keeps the decoded payload alive (Arc<KvBlock> refcount or the
    /// RecordBatch's Arc'd Buffers). Replaced wholesale per block.
    pin: Option<DecodedBlock>,
    /// v2 keys delta-decoded once per block, back-to-back. `clear()`ed and
    /// re-extended per block — capacity stabilises after the first blocks,
    /// so steady-state cost is the (already-paid-today) byte copy, with
    /// ZERO allocations. Unused (left empty) for v1 Arrow blocks.
    key_arena: Vec<u8>,
    /// Row metadata; reused Vec (clear per block).
    rows: Vec<RowMeta>,
    pos: usize,
}
struct RowMeta {
    key: SliceRef,           // Arena(off,len) | Block(off,len)
    val: Option<SliceRef>,   // Block(off,len) for v2; Block-buffer ref for v1
    sequence: u64,
    op_type: OpType,
}
enum SliceRef { Arena(u32, u32), Block(u32, u32) }
```

Resolution: `Arena` indexes `key_arena`; `Block` indexes the pinned block's
stable bytes (v2: `KvBlock.payload`; v1: the `BinaryArray` values buffer).
Both resolve to `&[u8]` with zero copies and zero refcount traffic per row.

**W1a (storage, kv_block.rs):** a range-exposing visitor so the engine can
capture offsets instead of borrowed slices:

```rust
/// Like for_each_row, but (a) reconstructs each full key APPENDED into
/// `arena` (caller-owned, not cleared), yielding its (off,len), and
/// (b) yields the value as a (off,len) range into `self.payload`
/// (None for tombstones), plus seq/op. One pass, same decode work as
/// for_each_row minus nothing — strictly additive API.
pub fn for_each_row_ranges<F>(&self, arena: &mut Vec<u8>, cb: F) -> ForstResult<()>
where F: FnMut(RowRanges) -> ForstResult<()>;
```

Implementation reuses the existing entry-walk (`read_row_at` decode logic,
kv_block.rs:485+); the value range is already computed internally
(`value_tag` → payload range, kv_block.rs:160-175 writer layout shows
`[..][seq][op][key tail][value]` — the reader already knows the value's
payload offsets, it just hands out a slice today). For v1 Arrow blocks the
engine-side replenish reads ranges straight off the `BinaryArray` offsets
(no new storage API needed; `for_each_row_in_batch` gains a ranges twin or
the engine downcasts as the point-get path does).

**W1b (engine, db.rs replenish):** the `peek()` replenish loop
(db.rs:10658-10744) becomes: take `fetcher.next_decoded()` → `buf.pin =
Some(block)`, `key_arena.clear()`, `rows.clear()` → one
`for_each_row_ranges` pass applying the SAME filters in the SAME order
(below-lower skip, upper-bound `hit_upper` + `fetcher.terminate()`
contract db.rs:10731-10740, within-block user-key dedup db.rs:10699-10721 —
now comparing against the last arena row, an offset-resolved slice compare).
Per-row cost: the key-tail copy into the arena (≤ key bytes, identical bytes
to today's `Arc::from` copy) and a 24-byte `RowMeta` push — **the two
per-row heap allocations are gone**; the only steady-state allocation in the
whole replenish is none.

**W1c — emit boundary (the part the sketch hand-waved).** Today's item type
`(Arc<[u8]>, Arc<[u8]>)` would force the alloc pair right back at emit. Fix
= push-style fill for the FFI path:

- New engine method `LazyPrefixIter::fill_into(&mut self, sink: &mut dyn
  RowSink) -> ForstResult<FillOutcome>` where `RowSink::push(key: &[u8],
  value: &[u8]) -> bool` (false = chunk full, engine stops; the borrowed
  slices are valid only during the call — sink memcpys into the chunk
  buffer immediately, which is exactly what `fill_chunk_from_iter` already
  does with Arc contents, lib.rs:5071-5117 one `copy_nonoverlapping` per
  field). **SST bytes → chunk buffer stays exactly one copy**, now with
  zero intervening allocations or refcounts.
- Borrow soundness: Phase B advances past `min` BEFORE Phase C returns, but
  advancing only bumps `pos` (db.rs:10753) — the winning row's bytes live in
  `SstBlockBuf` until the NEXT replenish of that source, which cannot happen
  before the next `next`/`fill_into` step. The decision value is therefore
  valid for the duration of the sink push. (Same argument as today's
  `head_sst_info` Arc clone, db.rs:10781-10788, minus the clone.)
- `last_emitted: Option<Arc<[u8]>>` (db.rs:10814) becomes a reused
  `Vec<u8>` + `bool` (copy-into; ~32-byte memcpy per emit replaces an Arc
  clone — measured-class noise, zero alloc).
- `ValueDecision::Put(Arc<[u8]>)` (db.rs:11006) becomes `Put(SourceIdx)` —
  the caller resolves the winner's bytes from the source buffer; `Fallback`
  (memtable winner / merge chain / corrupt Put, db.rs:10986-10997) still
  goes through `get_internal` and its owned result (correctness path,
  unchanged, allocs allowed there).
- **Compat shim:** the in-process consumers (`prefix_scan` collector, engine
  tests, `Iterator for LazyPrefixIter` db.rs:11011+) keep an Arc-pair
  adapter implemented OVER `fill_into` (materialise per row) — off the hot
  path, zero behavior change, lets the q0-q22 byte-equiv harness compare
  paths directly.

### 2.2 W2 — loser tree

Replace the two linear phases with a tournament (loser) tree over source
indices, comparator on `(head_key, tier_rank, seq DESC)` where `tier_rank` =
0 for memtable cursors, 1 for SSTs (build order already encodes this —
memtable sources are pushed first, db.rs:6573-6598 vs :6753+):

- **Phase A collapse:** winner = tree root; per-source dedup-past-
  `last_emitted` happens on re-fight only for the sources that move (the
  current code re-checks every source every key, db.rs:10920-10959).
- **Phase B collapse:** equal keys surface consecutively (comparator makes
  the memtable source, else max-seq SST, surface FIRST). The decision is
  taken from the FIRST popped head — identical to `mem_present` /
  max-sequence rules (db.rs:10965-10998) — then duplicates are drained by
  popping while `head == min` (each pop = advance + re-fight = O(log n)).
- **n ≤ 4 keeps the linear scan** (build-time branch; tiny fan-outs are
  common for q3/q4-class probes and the linear scan wins there — and this
  caps regression risk for R-short).
- Comparisons per emitted key drop from `2n + dup·n` to `(1 + dups) ·
  log₂ n`; at q20-class fan-out (tens of sources) that is the predicted ~3×
  on the merge step (db.rs:10799).
- Error semantics preserved: a source whose `peek()` errors records via
  `record_peek_error` (sticky-first, db.rs:10873-10882) and is treated as
  drained for this scan — same as today's Phase A behavior (db.rs:10946-10949).

### 2.3 MVCC / lifetime analysis (the "pinned DecodedBlockBuf vs compaction
### file deletion" question — answer: NO new hazard, one bounded-memory note)

1. **A pin is heap memory, not a file reference.** Blocks are read via
   `pread` into an owned scratch then decompressed/decoded into owned
   storage (`read_decoded_block`, reader.rs:385-489; `KvBlock` owns its
   payload `Vec<u8>`, kv_block.rs:203-210; v1 `RecordBatch` owns Arc'd
   Buffers). There is no mmap anywhere on this path. Compaction deleting
   the source SST file after the block is decoded **cannot invalidate a
   pinned block** — the bytes are ours.
2. **Cache interaction:** the pin shares the cache's `Arc<KvBlock>`
   (reader.rs:407-409, :487) — cache eviction under pressure drops the
   cache's ref while the pin keeps the block alive. Bound: ≤1 pinned block
   per SST source (the next replenish drops the previous pin), i.e. ≤
   sources × 64 KiB decoded (+arena ≈ key bytes) per live iterator — a few
   MiB at q20 fan-out, freed by the existing exhaustion eager-free
   (`drop_inner`, lib.rs:5024) and by `fetcher.terminate()`. NOTE: pinned
   bytes are *uncounted* by the cache budget — same accounting class as the
   prefetcher's ready window (roadmap M3); fold both into M3's aggregate cap
   work, do not solve separately here.
3. **File lifetime during fetch is UNCHANGED by S2.** The fetcher holds
   `Arc<SstReaderImpl>` (prefetch.rs:180-191) whose open file handle makes
   local unlink-after-open safe (POSIX); the engine's deletion path
   consults `FileDeletionGuard` (file_deletion_guard.rs:23-30, pin/unpin
   API :55-95) for checkpoint-pinned files. The pre-existing exposure —
   a *remote-evicted* refetch racing a compaction's remote GC — predates
   the prefetcher and S2 and is strictly NARROWED by S2: a pinned row never
   triggers a block re-read, so the window in which the file must remain
   fetchable shrinks to the prefetch window itself.
4. **Snapshot visibility unchanged:** S2 touches representation only; the
   winner rules (memtable-presence → Fallback; max-seq among SSTs;
   tombstone skip) are byte-for-byte the same decision procedure, which the
   byte-equiv gate (G2) verifies mechanically.

### 2.4 Prefetcher composition note

Arena build runs on the consumer thread at replenish in v1 of this work
(same thread that pays the `for_each_row` walk today — no new overlap, no
new threading). Moving the ranges-pass onto the prefetch pool job (decode
*and* arena-build off the critical path, streaming design §2.1.4) is a
follow-up flag once W1 is byte-equiv-proven — it changes no interface
(`SstBlockBuf` would be produced pool-side and swapped in whole).

---

## 3. File-level work items

| # | File | Change | ≈LoC |
|---|---|---|---|
| W1a | `crates/forst-rs-storage/src/sst/kv_block.rs` | `for_each_row_ranges` (+`RowRanges` type) reusing read_row_at decode; UTs incl. prefix-compression edge (shared > arena tail) | 120 |
| W1a′ | `crates/forst-rs-storage/src/sst/reader.rs` | v1 ranges twin over `RecordBatch` (or engine-side downcast helper) | 60 |
| W1b | `crates/forst-rs-engine/src/db.rs` (TierKeySource, :10614-10744) | `SstBlockBuf` replaces `buffered`/`pos`; replenish via ranges pass; same filter/terminate semantics | 150 |
| W1c | db.rs (LazyPrefixIter emit, :10808-11009) + `crates/forst-rs-ffi/src/lib.rs` (:5071, :5224 iter construction) | `fill_into`/`RowSink`; `last_emitted` scratch; `ValueDecision::Put(SourceIdx)`; Arc-pair compat adapter for in-process API | 200 |
| W2 | db.rs (new `loser_tree` module or inline) | tournament tree keyed `(key, tier_rank, seq DESC)`; n≤4 linear fallback; dup-drain | 180 |
| W3 | diag | per-iter counters: rows emitted, allocs (should be 0 on SST-Put path), merge comparisons — behind `FRS_ITER_DIAG` | 40 |

Flag: `FRS_RS_S2_PINNED` (default OFF) selecting old/new replenish+merge at
iterator build; both paths share sources so the byte-equiv harness can A/B
in-process.

---

## 4. Falsifiable perf model (anchored on the recorded q20 profile)

Recorded: q20 prefix-scan = **21.6 %** of CPU (roadmap §1.2), decomposing
into (i) O(sources)-twice merge scan, (ii) 2 allocs/row, (iii) demand block
fetch/decode. The prefetcher (shipped, unmeasured on q9/q20 — roadmap L0)
addresses (iii); S2 addresses (i)+(ii):

- (i): merge step ~3× faster at q20 fan-out (code's own estimate db.rs:10799
  + log₂ argument §2.2) — if the merge scan is ~half the 21.6 %, this
  recovers ~7 points.
- (ii): 2 allocs + refcount traffic per row → 0; on the q4 campaign's
  measured alloc costs (~50-100 ns + pressure per Arc-from, db.rs:6480-6495
  comment block) at q20's row volume this is ~2-4 points.
- Model: **q20 −9..13 %, q9 −4..8 %** (q9's scans are longer but fewer;
  fan-out lower). Running totals per roadmap step 3.

**Falsifiers (measure, n≥3, @100M only):**
1. W3 alloc counter on the SST-Put path must read ~0 — if not, W1c leaked
   an alloc back in (e.g. the compat adapter on the hot path).
2. If q20 improves < 4 % AND the post-S2 profile still shows ≥ 15 % in the
   scan path, the share is fetch/decode-bound (iii) — STOP S2 follow-ups
   (no SoA, no arena-on-pool) and execute L4/L0 measurement first.
3. q3/q4 (R-short, small fan-out) must be within box noise — the n≤4 linear
   branch is the guard; a regression there means the branch threshold is
   wrong, not that S2 is invalid.

---

## 5. Gates

| # | Gate | Bar |
|---|---|---|
| G0 | storage (355) + engine (277+) suites, both flag states | 0 fail |
| G1 | new UTs: ranges-visitor vs for_each_row byte-equality property test over random blocks (v1+v2, compression on/off); loser-tree vs linear-scan equivalence over randomized multi-tier fixtures (incl. same-key cross-tier, tombstones, merge ops, dedup) | exact |
| G2 | q0-q22 @5M byte-equiv sweep, flag ON vs OFF (the existing harness) | byte-identical |
| G3 | q4/q3 point-get + short-probe no-regress @100M ×3 | within noise |
| G4 | iterator-leak watchdog + RSS steady on q9@100M (pins must not accumulate) | steady |
| G5 | q20/q9 @100M ×3 A/B vs same-day baseline | report vs model §4 |

**Zero-copy mandate check:** S2 removes the last per-row allocations on the
scan path and keeps SST→chunk at exactly one memcpy; no new copies are
introduced anywhere (the `last_emitted` 32-byte scratch copy replaces an
atomic refcount pair — net win). Batch/crossing protocol untouched.
