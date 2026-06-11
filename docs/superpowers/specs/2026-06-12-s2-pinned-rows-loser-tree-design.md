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

---

## 6. §work-order (PMC cycle 4 — RD GO-ORDER, roadmap §5.4 item 3)

Status: **GO.** E5 landed and APPROVED (`review-rounds/2026-06-12-pmc-review-e5.md`);
the L2 lane is unblocked and S2 is the top modeled q20 lever (roadmap §5.3
rank 2). Three commit-sized stages, strictly ordered; each commit must leave
the tree green with the flag default-OFF.

### 6.0 Two locked decisions (so RD does not relitigate)

**D1 — RowSink emit boundary: push-style `fill_into(&mut dyn RowSink)`
(W1c as specced) is THE boundary. DECIDED.** Rationale against the two
alternatives:
- *Pull-style lending iterator* (`next() -> Option<(&[u8], &[u8])>`) is the
  textbook shape but cannot be boxed as today's
  `Box<dyn Iterator<Item = (IterKey, IterValue)>>` without GAT/lending
  machinery, and it hands the caller a borrow that can dangle across the
  next `next()` — exactly the bug class the kv_block scratch finding (§1)
  punishes. Rejected.
- *Keep Arc pairs at the boundary* re-pays the two per-row allocations at
  emit, deleting W1's entire (ii) win. Rejected.
- Push-style wins because the borrow scope is syntactically enclosed in the
  `sink.push(key, value) -> bool` call (no escape possible — the §2.1 W1c
  borrow-soundness argument only has to hold for the duration of one call);
  the FFI consumer already memcpys immediately (`fill_chunk_from_iter`,
  lib.rs:5071 — one `copy_nonoverlapping` per field), so SST→chunk stays
  exactly one copy; and `push -> false` maps 1:1 onto the existing
  chunk-full backpressure. In-process callers (engine tests, `prefix_scan`
  collector, q0-q22 byte-equiv harness) use the Arc-pair compat adapter
  OVER `fill_into` — off the hot path.

**D2 — per-block key arena is MANDATORY for v2 KV blocks, not an
optimization. DECIDED** (the §1 kv_block scratch finding):
`KvBlock::for_each_row` reconstructs prefix-compressed keys into a reused
scratch `key_buf` (kv_block.rs:308-317, `read_row_at` :485-488) — `view.key`
is dead the moment the callback returns, so pinning the `DecodedBlock` alone
is sound for VALUES only. Every stage-1/2 reviewer checklist item touching
keys must verify: v2 keys → `SliceRef::Arena` (copied once into the
caller-owned `key_arena`, §2.1 W1a), v2 values → `SliceRef::Block`
(payload-stable, kv_block.rs:203-210), v1 Arrow keys AND values →
`SliceRef::Block` (BinaryArray buffers are stable; arena stays empty).
A stage that lets a v2 key escape as a Block ref is a memory-safety bug the
G1 property test must be constructed to catch (assert byte-equality AFTER
the full block walk completes, not per-row).

### 6.1 The bench gates — which criterion benches proxy the 21.6 % share

The recorded q20 prefix-scan share (roadmap §1.2) cannot be re-measured per
commit; these four existing benches decompose it and are the per-stage gates
(all in `crates/forst-rs-bench/benches/`, run n≥3 same-session, compare
median):

| Bench (criterion id) | What it proxies | S2 component it gates |
|---|---|---|
| `ffi_vectorized/iter_drain/ffi_chunked_drain_100x1000` (ffi_vectorized.rs:605) | per-row emit cost through the FULL path (merge → decision → fill_chunk → crossing); post-E5 baseline **151.8 ns/row** | (ii) the 2 allocs/row + W1c emit boundary |
| `ffi_vectorized/iter_open_batch/ffi_open_batch_parallel_k64` (ffi_vectorized.rs:622) | probe-open + scan build (locator, source build, first replenish); baseline **104.2 µs/probe** | W1b replenish restructure must not bloat open |
| `join_probe_open/ssts_{1,8,32,64,128}` (join_probe_open.rs:87) | the q20 interval-join probe shape with a FAN-OUT SWEEP — every SST spans the probed range so the locator can't prune; ssts_64/128 are the only micro cells where O(sources)-twice vs log₂ separates | (i) W2 loser tree (64/128 cells); the n≤4 linear-branch guard (1/8 cells) |
| `rocksdb_compare/hot_prefix_churn` + `range_scan_multilevel` (rocksdb_compare.rs:329, :240) | dup/tombstone-heavy churn (Phase-B drain) and multilevel merge, with the rocksdb reference number alongside | W2 dup-drain; cross-check vs rocksdb does not regress |

Known measurement gap, accepted: `join_probe_open` drains via
`prefix_scan_iter_owned_arc` (the in-process Arc API), so post-W1c it
measures the COMPAT ADAPTER, not the raw sink path. Stage 2 therefore adds
one bench cell driving `fill_into` directly (sink = byte-counting no-op) so
the adapter tax is itself measured — do NOT silently rewire the existing
cell (it is the continuity series).

### 6.2 Stage S2-1 — storage ranges visitor (W1a + W1a′), additive only

- **Code:** `for_each_row_ranges` + `RowRanges` in
  `crates/forst-rs-storage/src/sst/kv_block.rs` (~120 LoC), reusing the
  `read_row_at` decode walk; v1 ranges twin over `RecordBatch` in
  `sst/reader.rs` OR the engine-side downcast helper (~60 LoC — pick
  whichever keeps the BinaryArray offset math in ONE place). No engine call
  sites yet; zero behavior change.
- **Tests (gate to commit):** G1 property test — ranges-visitor vs
  `for_each_row` byte-equality over randomized blocks: v1+v2, compression
  on/off, and the prefix-compression edges (shared-prefix > previous arena
  tail; restart-point boundaries; single-row block; tombstone rows yield
  `val=None`). Per D2: equality asserted after the full walk (arena offsets
  resolved at the end), not per-row. Full storage suite green.
- **Bench gate:** none (additive API, nothing calls it). Do not run the
  matrix for this commit.

### 6.3 Stage S2-2 — engine pinned replenish + sink emit (W1b + W1c), flag-gated

- **Code:** `SstBlockBuf`/`RowMeta`/`SliceRef` replacing `buffered`/`pos` in
  `TierKeySource::Sst` (db.rs:10614-10744, ~150 LoC); `fill_into`/`RowSink`,
  `ValueDecision::Put(SourceIdx)`, `last_emitted` → reused `Vec<u8>`+`bool`,
  Arc-pair compat adapter (db.rs:10808-11009 + ffi/lib.rs:5071/:5224,
  ~200 LoC); flag `FRS_RS_S2_PINNED` default OFF selecting old/new at
  iterator build. Preserve verbatim: replenish filter order
  (below-lower skip → upper-bound `hit_upper` + `fetcher.terminate()`
  contract → within-block dedup), Phase A/B/C decision procedure, sticky
  `record_peek_error` semantics. W3 diag counters land HERE (rows emitted,
  allocs-on-SST-Put-path, merge comparisons; behind `FRS_ITER_DIAG`) —
  stage 3's gate needs the comparison counter pre-existing.
- **Tests (gate to commit):** engine suite green BOTH flag states; a
  flag-ON vs flag-OFF byte-equality engine test over a multi-tier fixture
  (memtable + L0 + L1, dups, tombstones, merge ops — reuse the E5 fixture
  builders); the W3 alloc counter reads 0 on the SST-Put path flag-ON
  (falsifier §4.1); leak check — drop the iterator mid-drain and assert pin
  release (no `Arc<KvBlock>` outstanding past `drop_inner`).
- **Bench gates (flag ON vs OFF, n≥3):** `iter_drain` ≤ baseline (model
  says measurably below 151.8; HARD gate is no-regress, report the delta);
  `iter_open_batch` within noise of 104.2; `join_probe_open` ssts_1/8
  within noise (adapter tax visible here — if ssts_1 regresses > noise the
  adapter is too fat, fix before commit); NEW direct-`fill_into` cell added
  per §6.1.

### 6.4 Stage S2-3 — loser tree (W2)

- **Code:** tournament tree keyed `(head_key, tier_rank, seq DESC)` (new
  `loser_tree` module or inline, ~180 LoC); n≤4 build-time linear branch;
  dup-drain by pop-while-equal; errored source = drained (sticky-first
  recording unchanged). Flag stays `FRS_RS_S2_PINNED` (one flag for the
  whole S2 unit — the tree only exists on the new path, so the A/B stays
  two-way and the byte-equiv harness needs no third state).
- **Tests (gate to commit):** G1 equivalence — loser-tree vs linear-scan
  merge over randomized multi-tier fixtures (same-key cross-tier with
  memtable-presence, max-seq SST tie-breaks, tombstones, merge-op fallback,
  dedup-past-last_emitted, source error mid-stream, n∈{1..64} sources
  crossing the n≤4 branch both ways); full suites both flag states.
- **Bench gates (n≥3):** `join_probe_open` ssts_64/128 improved (the log₂
  cells — model predicts ~3× on the merge step; HARD gate: ≥ measurable
  improvement outside noise, report vs model); ssts_1/8 within noise (the
  linear-branch guard — falsifier §4.3: a regression here means the
  threshold is wrong, not S2); `iter_drain` + `hot_prefix_churn` no-regress;
  W3 comparison counter shows the `(1+dups)·log₂n` vs `2n+dup·n` drop on a
  64-source fixture (cheap mechanical confirmation the tree is actually
  engaged).

### 6.5 After stage 3 (not per-commit)

G2 q0-q22 @5M byte-equiv sweep flag-ON vs OFF → then the Linux-box @100M
program: G3 (q3/q4 no-regress ×3), G4 (RSS steady q9@100M — pins must not
accumulate), G5 (q20/q9 ×3 A/B vs same-day baseline, report vs §4 model:
q20 −9..13 %, q9 −4..8 %). Flag default-ON is a SEPARATE decision commit
gated on G2-G5, per the L1-drain-gate precedent. §4 falsifier 2 stays
binding: if q20 < −4 % and the scan share is still ≥ 15 %, STOP S2
follow-ups (no SoA, no arena-on-pool §2.4) and run L4/L0 first.
