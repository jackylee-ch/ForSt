# Streaming Range/Prefix Read Redesign — Whole-System Design

**Date:** 2026-06-11
**Status:** DESIGN (analysis only — no code changed)
**Goal:** beat ForSt (C++ RocksDB-derived, `read-io-parallelism=3`) by ≥1.05× on
iterator-dominated workloads. Concrete bar: NEXMark **q7 @100M on 8c/32GB ≤ ~550 s**
(ForSt = 586.8 s; forst-rs serial best = 1441.6 s; with partial parallel-iterator
fan-out = 1052 s; RocksDB JNI DNF).

Binding mandates honored throughout: end-to-end vectorization, zero memory copy,
batch-only FFI crossings, off-heap Arrow-style layouts, no per-key/per-record execution.

---

## Part 1 — Code-grounded findings (what the read path actually does today)

### 1.1 Engine read path for a prefix/range scan, end to end

**Iterator construction (per probe).**
`DbImpl::prefix_scan_iter_owned_arc_with_error_slot`
(`crates/forst-rs-engine/src/db.rs:6446`) hoists the CF resolution once
(db.rs:6466), builds the lazy k-way merge via `build_lazy_prefix_key_stream`
(db.rs:6507), and wraps it in a value-carrying `from_fn` loop (db.rs:6479-6491):
`ValueDecision::Put` yields the SST-inline value; `ValueDecision::Fallback` calls
`get_internal` only for memtable-tier winners / merge chains (db.rs:6484). The
range-scan double-walk is gone — confirmed.

Tier sources assembled per probe (db.rs:6559-6825):
- active memtable cursor (db.rs:6573-6585), immutable memtables (db.rs:6592-6598);
- resident-flushed shadow entries, gated by the source-SST's decode-free
  index/bloom prune (db.rs:6637-6734);
- overlapping SSTs located per level (db.rs:6753), pruned by the v3 **prefix
  bloom** (db.rs:6794-6808, `FRS-PREFIX-BLOOM`) then `may_contain_range`
  (db.rs:6809, decode-free sparse-index prune, reader.rs:769), then seeded at
  `first_block_ge(prefix)` (db.rs:6816, reader.rs:745) — scans never start at
  block 0.

**SST file format.**
- Default `block_size` = 64 KiB, compression = LZ4
  (`crates/forst-rs-common/src/config.rs:266,268`).
- v2 KV block format is default-ON (`crates/forst-rs-storage/src/sst/kv_block.rs:~70`):
  **prefix-compressed** entries (`[shared varint][non_shared varint][value_tag]
  [seq fixed64][op u8][key tail][value]`, kv_block.rs:37-49) with **restart points
  every 16 entries** (`KV_RESTART_INTERVAL`, kv_block.rs:64) and intra-block binary
  search over restarts. v1 Arrow-IPC blocks coexist; the reader dispatches on the
  block-type byte (reader.rs:441-457).
- Per-SST in-memory metadata: footer, bloom (point + v3 prefix), **sparse index**
  (one entry per block: offset/size/last_key + per-block min_key stats,
  reader.rs:43, 274) — loaded at open; data blocks are demand-read.

**How blocks are fetched.**
`read_decoded_block` (reader.rs:385-489): one `read_at_exact` per block into a
**reused thread-local scratch** (reader.rs:431-440), then decompress+decode.
The underlying `RandomAccessFile` is `LocalFirstSstFile` (cached_fs.rs:751):
positional `pread(2)` straight from the local write-through copy into the caller
buffer (cached_fs.rs:769-789, local_cache.rs:223-231); on eviction it falls back
to the remote (opendal/S3) reader (cached_fs.rs:794-806). `READ_AT_DIAG` buckets
show warm preads <1-2 µs; ≥20 µs = page-in "cold" class (cached_fs.rs:712-748;
prior q7 evidence: 182 µs mean, 26 % cold once state outgrows the page cache,
db.rs:6803-6804).

**Block cache.** Sharded CLOCK cache modeled on HyperClockCache
(`crates/forst-rs-storage/src/cache/mod.rs:15-27`), keyed `(file_number,
block_offset)`, storing **decoded** entries (`Arc<KvBlock>` / `RecordBatch`) so a
hit skips pread+decompress+decode entirely (reader.rs:386-404). Scan-path fills
insert at `CachePriority::Low` (reader.rs:465-487) — sequential scans already
have *some* anti-pollution bias. Iteration touches the cache **once per block**,
not per entry (the tier source buffers a whole block, db.rs:10562-10643). The
eager pre-populate sites were fixed to use the block cache (commit 27ae792c3).

**Readahead/prefetch: NONE at block level.** The only prefetch in the tree is
*whole-file* S3→local-cache warming (`prefetch_files_concurrent`,
cached_fs.rs:215; engine call site db.rs:8537-8613 — and that is the **point-get
batch** path, not the iterator path). The iterator's block fetch is strictly
demand-paged and strictly serial with consumption: `TierKeySource::Sst::peek`
(db.rs:10549-10646) replenishes **one block at a time**, synchronously, on the
consumer's thread, only after the previous block's rows are fully drained. No
sequential-detection, no ramping, no multi-block reads, no overlap of I/O with
decode or with the merge. This is investigation question #1's headline answer.

**K-way merge.** `LazyPrefixIter::next_with_value` (db.rs:10804-10899) does an
**O(n_sources) linear scan twice per emitted key** (Phase A: find min across all
sources; Phase B: re-peek all sources at min to pick the winner). The code itself
concedes a heap "would save ~3×" (db.rs:10690-10699). q7's L0 fan-out regularly
produces tens of sources per probe, so the per-key merge cost is
O(sources × key_cmp), not O(log sources).

**Allocation behavior per step.** For every accepted SST row, the tier source
pays **two heap allocations**: `Arc::<[u8]>::from(view.key)` and
`view.value.map(Arc::from)` (db.rs:10622-10628, `SstHeadRow` db.rs:10537-10543),
plus the `buffered` Vec growth. Memtable rows emit pre-existing `Arc`s. Downstream
(`last_emitted`, FFI emit) is refcount-only — but the per-row alloc pair at the
block boundary violates the spirit of the zero-copy mandate and is pure per-row
overhead the C++ engines don't pay (RocksDB iterators pin the block and return
borrowed slices).

### 1.2 FFI/FFM crossing protocol

Wire format: rows packed `[klen u32 LE][vlen u32 LE][key][value]` into a caller
direct ByteBuffer (`crates/forst-rs-ffi/src/lib.rs:4742-4744`). Fill loop
`fill_chunk_from_iter` (lib.rs:5071-5117): one `copy_nonoverlapping` per field
from the Arc-owned bytes — single copy engine→Java buffer, with `put_back`
rollback on overflow.

- **Chunk size: fixed 64 KiB** (`CHUNK_BUF_CAP`, ForStRsDBIterRequest.java:77),
  never adapted.
- **Strict ping-pong, zero pipelining.** The engine iterator holds state in a
  16-shard `Mutex<HashMap>` registry (lib.rs:5036-5052). Chunk N+1 is produced
  **only inside the next `frs_vec_iter_prefix_next` call** (lib.rs:5320-5394),
  synchronously on the Java-calling thread: shard lock → HashMap lookup → pull
  rows through the merge (including any block pread + LZ4 decompress) → memcpy →
  return. While Java decodes chunk N, the engine does nothing for that iterator;
  while the engine fills, Java waits. There is **one** reused chunk buffer per
  executor (FRS-REUSE-CHUNKBUF, VectorizedExecutor.java:131, 1507), so
  double-buffering is impossible under the current protocol even in principle —
  Java must finish decoding before it may call next.
- **Two guaranteed-wasted crossings per probe.** `open` has no EOF channel, so
  the Java drain "always invoke[s] next() at least once" even when the first
  chunk exhausted the iterator (ForStRsDBIterRequest.java:331-377 and the batched
  path :482-499), then must call `frs_vec_iter_prefix_close` (lib.rs:5407). The
  engine *already knows* exhaustion at open time (`iter_exhausted` →
  `drop_inner()`, lib.rs:5245-5251) and keeps a useless light shell registered.
  q7's dominant case is an exhausted-in-first-chunk probe ⇒ ≥2 pure-overhead
  crossings (each with guarded panic-catch + shard mutex + registry op) per probe.
- **Copies per chunk:** engine-side one memcpy per row into the chunk; Java-side
  `decodeChunkDirect` deserializes each row to detached on-heap UK/UV before the
  next crossing (ForStRsDBIterRequest.java:307-322) — the second copy is the
  unavoidable heap materialization, but it sits **on the critical path between
  crossings** because of the single shared buffer.
- **The 1441.6→1052 s "parallel" lever is BUILD-only.**
  `frs_vec_iter_prefix_open_batch_parallel` (lib.rs:5739) Pass 2 fans out only
  the iterator **build** across `bg_read_pool` (lib.rs:5814-5819;
  `batch_open_prefix_iters_parallel` db.rs:6253-6306, pool = `min(cores,4)`,
  override `FRS_RS_READ_IO_PARALLELISM`, db.rs:10115-10120). Pass 3 — the first
  chunk fill, i.e. all block I/O + decompress + merge + memcpy for the common
  exhausted-in-one-chunk probe — runs **serially** on the single FFI-calling
  thread (lib.rs:5821-5878). The Java side partitions fresh opens (batched
  parallel) from continuations/IS_EMPTY (serial ping-pong)
  (VectorizedExecutor.java:1437-1500, flag `FRS_RS_PARALLEL_ITER`, :1423).

### 1.3 Data-layout properties that bound streaming throughput

- **Value-carrying merge:** done (db.rs:6470-6491); no double-walk; fallback only
  for memtable winners (cheap) and merge chains (correct).
- **Decompression:** paid once per block per cache *miss*; decoded-block cache
  hits skip it (reader.rs:386-404). Iteration does not re-touch the cache per
  entry (§1.1). LZ4 on a 64 KiB block ≈ 10-15 µs decode (model estimate,
  ~4-6 GB/s LZ4 decode).
- **Key encoding:** composite `[key-group][user key][ns]` prefixes; the v3 prefix
  bloom (db.rs:6794) and `first_block_ge` make seek pruning RocksDB-class.
- **Chunk wire format is NOT Arrow.** Row-major `[len][len][k][v]` interleaved —
  no offsets array, no columnar buffers; Java walks it with per-row varlen reads.
  It is *convertible* but not castable to an Arrow batch.

### 1.4 What RocksDB/ForSt actually do for streaming reads (model knowledge — not code-cited)

| Mechanism (RocksDB/ForSt) | What it does | forst-rs equivalent today |
|---|---|---|
| `FilePrefetchBuffer` + auto-readahead | per-iterator readahead buffer ramps 8 KiB → 256 KiB (doubling) once ≥2 sequential block reads are seen in a file; one big pread serves many blocks | **MISSING** — one 64 KiB pread per block, demand-only |
| `async_io` / double-buffered prefetch | two prefetch buffers; block N+1 fetched (FS async / io_uring) while N is consumed | **MISSING** |
| Multi-block vector I/O (`MultiRead`/`FSReadRequest`) | coalesce adjacent block reads into one syscall | **MISSING** on iterator path (batch_get coalesces by SST already — point-get path) |
| Readahead vs block cache separation | prefetched scan data does NOT evict hot point-get blocks (separate buffer; `fill_cache` control) | partial — scan fills use `CachePriority::Low` (reader.rs:474), but there is no prefetch buffer at all |
| Iterator pinning (`pin_data`, PinnableSlice) | iterator step returns borrowed slices into the pinned block; zero alloc per row | **MISSING** — 2 `Arc::from` allocs per SST row (db.rs:10622-10628) |
| Prefix bloom for seeks | skip files/memtables lacking the prefix | **PRESENT** (v3, db.rs:6794) |
| Coalesced multiGet | one I/O pass for K point gets | **PRESENT** (batch_get) |
| ForSt `read-io-parallelism=3` | parallel I/O workers serve iterator block fetches | **PARTIAL** — parallel build only; no parallel/async I/O inside an iterator, serial first-chunk drain |

**Conclusion of investigation.** forst-rs has already matched RocksDB on *pruning*
(blooms, sparse index, seek) and *cache* (decoded CLOCK cache). It is missing the
entire **streaming pipeline**: I/O anticipation (readahead/prefetch), I/O–compute
overlap (async double-buffering), alloc-free stepping (pinning), and
producer/consumer overlap across the FFI (pipelined chunks). On top of that, the
q7-class short-probe regime pays 2 dead crossings per probe and a serial
first-chunk drain. These are exactly the deltas the design below removes.

---

## Part 2 — Design

Two workload regimes share this path and the design must serve both:

- **R-short (q7, q4-join):** millions of short prefix probes (most exhausted in
  one chunk). Dominated by per-probe fixed costs: build, crossings, serial drain,
  per-row allocs. Readahead is nearly irrelevant; speculative I/O is actively
  harmful.
- **R-long (q9/q11/q19/q20):** fewer, long partition/range scans (ROW_NUMBER
  re-scans, TopN). Dominated by bytes/s: block fetch latency on the critical
  path, crossing count, chunk size, decode overlap.

### 2.1 I/O layer — `BlockPrefetcher` (per-SST-source prefetch state machine)

New storage-crate component owned by each `TierKeySource::Sst` (replacing the raw
`next_block: usize` counter, db.rs:10520):

```
struct BlockPrefetcher {
    reader: Arc<SstReaderImpl>,
    next_block: usize,          // demand cursor
    end_block: usize,           // first_block_ge(upper) clamp — never read past it
    blocks_consumed: u32,       // sequential-detection counter
    ra_blocks: u32,             // current readahead window, in blocks (0 = off)
    inflight: Option<PrefetchHandle>,  // raw bytes or decoded blocks being produced
}
```

**State machine (mirrors RocksDB auto-readahead, adapted to our two regimes):**

1. **Cold (blocks_consumed < 2):** demand-fetch exactly one block, synchronously,
   exactly as today (block-cache check first). *R-short probes never leave this
   state ⇒ zero speculative I/O, zero regression risk for q7/q3/q4.*
2. **Ramp (blocks_consumed ≥ 2):** sequentiality is *certain* inside a source
   (blocks are consumed in index order), so begin readahead: `ra_blocks = 2`,
   doubling each replenish up to a cap:
   - local-disk path (`LocalFirstSstFile` serving from the write-through cache):
     cap = **256 KiB** (4 blocks) — preads are µs-class; deeper windows only add
     memory.
   - remote/evicted path (opendal S3 fallback, cached_fs.rs:794-806): cap =
     **4 MiB** and enter ramp after the *first* block — round-trips are ms-class
     and a GetObject has high fixed cost. Expose `RandomAccessFile::is_local()`
     (default true; the cached-fs file answers per current serving tier).
3. **Multi-block reads:** one pread/GetObject spanning the next `ra_blocks`
   *physically contiguous* blocks (the sparse index gives exact offset+size per
   block, reader.rs:43; blocks are laid out back-to-back by the writer) — one
   syscall serves N blocks; slice per-block regions out of the single buffer.
4. **Double-buffered production:** when the consumer takes decoded block N, the
   prefetcher immediately submits the fetch+decompress+decode of `[N+1, N+ra]`
   to the shared **read-I/O pool** (§2.5) and stores the `PrefetchHandle`
   (a oneshot slot). The next replenish first claims the handle (usually already
   complete), then submits the following window — production of N+1 overlaps
   consumption of N. The decode (LZ4 + KvBlock pointer-walk) runs on the pool
   thread, off the consumer's critical path.
5. **Clamping:** never prefetch past `end_block` (computed once from the sparse
   index vs `upper`) and never past the file. Aborted/closed iterators drop
   their `PrefetchHandle`; the pool task's result is discarded on a dead handle
   (the buffer is pool-owned until claimed — no use-after-free surface).

**Block-cache interaction (don't pollute / don't bypass):**
- Every fetch — demand or prefetch — **checks the cache first** per block and
  removes cached blocks from the I/O window (split the window around hits). This
  preserves the q20 lesson (block-cache bypass was the join killer, commit
  27ae792c3): the hot join set keeps being served from cache.
- Demand blocks insert at `Low` (today's behavior, reader.rs:474). Ramped
  prefetch blocks insert at **`Bottom`** (mod.rs:108-127) once `ra_blocks ≥ 4`:
  a deep-streaming scan is the least likely data to be re-read, and `Bottom`
  makes it the first evicted — hot point-get blocks are never displaced. We do
  NOT bypass insertion entirely: q7-class re-probes of recently scanned windows
  are common (interval joins re-touch the same windows), and the decoded cache
  hit is our cheapest read.

### 2.2 Engine iterator — streaming, allocation-free, stall-free

**a) Alloc-free step via block pinning (RocksDB `pin_data` equivalent).**
Replace `SstHeadRow { key: Arc<[u8]>, value: Option<Arc<[u8]>>, … }`
(db.rs:10537) with a pinned-row representation:

```
struct PinnedRow { block: Arc<DecodedBlockBuf>, key: (u32,u32), value: Option<(u32,u32)>, seq: u64, op: OpType }
```

The tier source keeps the decoded block alive (`Arc` pin, one refcount per
*block*) and rows are `(offset,len)` views into it — the two per-row `Arc::from`
allocations (db.rs:10622-10628) disappear. v2 KV blocks need a one-time
delta-decode into a flat key arena per block (prefix compression means key bytes
are not contiguous on disk) — that is one bulk arena build per block (amortized,
done on the prefetch thread per §2.1.4), not per-row heap allocs. The FFI fill
then memcpys straight from the pinned block/arena into the chunk buffer —
**SST bytes → chunk buffer remains exactly one copy**, now with zero intervening
allocations. `last_emitted` becomes a reused `Vec<u8>` scratch (copy-into, no
realloc steady-state) instead of an `Arc` chain.

*Memory note:* a pin holds ≤1 block (64 KiB + arena) per SST source; with tens of
sources that is a few MiB per live iterator — bounded, and freed by the existing
exhaustion eager-free (`drop_inner`, lib.rs:5024).

**b) Loser-tree merge.** Replace the per-key two-phase O(n_sources) scan
(db.rs:10804-10899) with a loser tree (tournament tree) over the sources keyed by
`(head_key, tier_rank, seq)`, with the same dedup/visibility semantics:
- winner pop + sibling re-fight = O(log n) comparisons per emitted key;
- Phase-B's "find all sources at min" collapses into the tree's natural
  duplicate-draining order (equal keys surface consecutively; tier_rank ordering
  makes the first surfaced duplicate the decision-maker, identical to the current
  memtable-presence / max-seq rules);
- keep the linear scan for `n ≤ 4` (it wins on tiny fan-outs; pick by source
  count at build time). The code already predicts ~3× on the merge step
  (db.rs:10698).

**c) Never stall on anticipatable I/O.** With §2.1, a source's `peek()` replenish
is: claim completed prefetch → submit next window → return. The merge stalls only
if consumption outruns the I/O pool — self-correcting via the ramp. The demand
path (cold state) is unchanged, so empty probes stay as cheap as today
(bloom/index-pruned, db.rs:6794-6811, no block ever read).

### 2.3 FFI protocol — kill dead crossings, then pipeline

Ordered by leverage for q7, then q9-class:

**P0 — EOF flag + auto-close (no new buffers, ABI-compatible).**
`FrsChunk` already carries `_reserved: u32` (lib.rs:5486). Define bit 0 =
`CHUNK_EOF`. `open` / `open_batch(_parallel)` / `next` set it whenever
`fill_chunk_from_iter` reports `exhausted` (the engine already computes this —
lib.rs:5245, 5363). When open reports EOF the engine **does not register a
handle** (returns `handle = 0` + rows + EOF; today it registers a dead shell,
lib.rs:5287-5295). Java (`ForStRsDBIterRequest.process` :331-377 /
`processFromBatchedOpen` :482-499) skips both the mandatory trailing `next()`
and the `close()`. For the dominant q7 probe this removes **2 of ~3 crossings
plus 2 registry mutex ops plus the shell's lifetime**, and removes the watchdog
burden for one-shot probes. Single-chunk semantics, error paths (deferred-error
machinery lib.rs:5339-5392), and the abort contract are untouched: EOF is only
set when the deferred-error stash is empty, so a partial-chunk-then-error probe
still goes through the registered-handle path.

**P1 — parallel first-chunk fill (finish the 1441→1052 lever).**
In `frs_vec_iter_prefix_open_batch_parallel` Pass 3 (lib.rs:5821-5878), each
probe writes into its **own** disjoint chunk buffer (Java allocates
`chunkData[i*cap .. (i+1)*cap]`, VectorizedExecutor.java:1572-1580), so the fill
is embarrassingly parallel. Move the `fill_chunk_from_iter` + error-drain into
the same pool job that builds the iterator (one job = build + first-chunk fill +
EOF decision), keeping only registry insertion + out-pointer writes serial in
Pass 3. This puts block I/O, decompression, merge, and the memcpy of the common
exhausted-in-one-chunk probe **on K cores instead of 1** — the engine-side
equivalent of ForSt's read-io-parallelism finally covering the drain, not just
the build. Safety: the `IterHandle` is thread-confined until registered; buffers
are disjoint; out-descriptors written after join.

**P2 — pipelined chunk ring (replaces strict ping-pong for continuations).**
For iterators that survive their first chunk (R-long, plus q7's fat windows):

- Java allocates a per-iterator **ring of R = 4 chunk slots** (off-heap, from the
  executor's tiered buffer pool) + one 64-byte control segment
  `{produced_seq: u64, consumed_seq: u64, eof+err: u32, chunk_meta[R]}` written
  with VarHandle release/acquire on both sides.
- New symbol `frs_vec_iter_stream_start(handle, ring_desc)` hands the ring to the
  engine, which schedules a **producer task on the read-I/O pool**: fill slot
  `produced_seq % R` whenever `produced_seq − consumed_seq < R`, bump
  `produced_seq` (release). Java consumes slot `consumed_seq % R`, decodes
  (copying out to heap exactly as today), bumps `consumed_seq` (release). One
  atomic handoff per chunk, **zero FFI crossings in steady state**; Java crosses
  only to park/kick when the ring runs empty and a bounded spin fails
  (`frs_vec_iter_stream_wait`, which doubles as the error/EOF surface — engine
  errors land in the control word with the same sticky-first semantics as the
  current slot machinery, lib.rs:5339-5392).
- This makes the engine produce chunk N+1 while Java drains chunk N — the
  double-buffer the single reused `chunkBuf` (VectorizedExecutor.java:131)
  structurally forbids today — and removes the per-chunk shard-mutex + HashMap +
  guarded-panic crossing cost (lib.rs:5327-5338).
- **Adaptive chunk sizing:** slot capacity starts at 64 KiB; when an iterator
  produces ≥2 full chunks the next ring (continuation) is allocated from the
  256 KiB tier, then 1 MiB (cap). A 64 MiB partition scan goes from ~1000
  crossings to ~64 handoffs + a handful of crossings. R-short probes never
  allocate a ring at all (P0 ends them at open).

**P3 — chunk wire format: Arrow-style SoA, not Arrow IPC.**
Adopt a v2 chunk layout (version bit in the control word / `_reserved`):
`[n: u32][key_offsets: u32 × (n+1)][val_offsets: u32 × (n+1)][key_bytes][val_bytes]`.
- Same single memcpy per field engine-side (two running cursors instead of
  interleaved headers); Java decode becomes two offset-array walks —
  bounds-check-friendly, vectorizable, and **directly castable to two Arrow
  BinaryArray buffers** (offsets + data) for future zero-copy consumers, which
  satisfies the off-heap Arrow-layout mandate.
- Full Arrow IPC (FlatBuffers schema header per chunk, 8-byte alignment padding,
  Java Arrow reader on the hot path) is rejected: the schema is static and known
  to both sides; IPC framing is pure per-chunk overhead with no consumer that
  needs it. Revisit only if a downstream wants whole-batch Arrow handoff.

### 2.4 Data layout — verdict: do NOT change the SST format now

Analysis against q7-class scans:
- **Block size 64 KiB** (config.rs:266): a q7 probe touches ~1 block per
  overlapping source; bigger blocks would raise per-miss decompress latency and
  cache granularity (hurting q3/q4 point gets) for no probe-count reduction.
  R-long scans get their multi-block I/O from §2.1.3 *without* a format change —
  readahead composes N×64 KiB into one pread, which is strictly more flexible
  than baking 256 KiB blocks into the file.
- **Restart interval 16 + prefix compression** (kv_block.rs:64): intra-block seek
  is binary-search-over-restarts + ≤16 linear steps; for *streaming* (this
  design's subject) the block is walked linearly anyway, so restart density is
  irrelevant to throughput. The §2.2a per-block arena decode absorbs the
  prefix-compression cost once per block on the prefetch thread.
- **Prefix bloom v3** already shipped (db.rs:6794) — the format lever that
  mattered for read-volume.

Therefore the format does **not** measurably cap q7-class scans; the caps are
pipeline-shaped (crossings, serial drain, allocs, no prefetch). No migration is
proposed. (If later profiling shows v2-KV per-block arena decode dominating
R-long scans, the escape hatch is a v4 block with a restart-side key-offsets
array — coexists per block-type byte exactly like v1/v2 today; migration cost ≈
one writer flag + reader arm, no rewrite of existing SSTs.)

### 2.5 Parallelism — how this composes with the existing fan-out

Two orthogonal axes, explicitly layered:

1. **Across probes (exists, extended by P1):** K fresh probes per batch fan out
   over `bg_read_pool` = `min(cores, 4)` workers (db.rs:10116-10120,
   `FRS_RS_READ_IO_PARALLELISM`). P1 moves the first-chunk drain onto the same
   jobs. This is the q7 axis.
2. **Within an iterator (new, §2.1/§2.3):** per-SST-source `BlockPrefetcher`
   handles + the ring producer task run on a **shared read-I/O pool**, sized
   `clamp(cores/2, 2, 6)` threads — the ForSt `read-io-parallelism=3` equivalent,
   but per-source rather than per-iterator, so a single long scan (R-long) gets
   I/O parallelism = min(#sources, pool) even at parallelism-1. This is the
   q9/q11/q19/q20 axis.

Budgeting on the 8c/32g box: 4 probe workers + 3 I/O threads + producer tasks
co-scheduled on the I/O pool. Both pools are work-stealing-free bounded queues
(existing `bg_pool::WorkerPool`); the I/O pool tasks are short (one window
fetch+decode), so probe workers are never starved. Per-SST parallel prefetch is
chosen over per-iterator threads because q7's unit of work is the *probe* (already
parallel) while q9's unit is the *source window* — per-iterator threads would
leave single-iterator scans serial again.

---

## Part 3 — Throughput model and expected q7 number

All numbers below are **model/estimates** unless cited; they exist to rank the
levers and justify the target, not to predict to the second.

**Workload scale (estimate):** q7@100M ⇒ ~92 M bids ⇒ P ≈ 9×10⁷ iterator probes
(one MAP_ITER probe per bid-side join lookup), overwhelmingly exhausted in one
chunk (interval-join window rows ≈ tens × ~100 B ≈ a few KiB ≪ 64 KiB). Bytes
drained over the run ≈ 30-60 GB; bytes *scanned* (block reads incl. fan-out
waste) ≈ 2-4× that before pruning, mostly cache-served.

**Per-probe budget:** 1052 s / 9×10⁷ ≈ **11.7 µs/probe** end-to-end today;
ForSt 586.8 s ≈ 6.5 µs/probe; target ≤550 s ≈ 6.1 µs/probe.

**Decomposition of today's 11.7 µs (model, anchored on measured artifacts:
~4.6 µs engine-FFI per-call cost from the q3 campaign; FRS-PROBE-DIAG build/fill
split; JFR per-probe alloc evidence):**

| Component | est. µs/probe | removed by |
|---|---|---|
| Flink/source/serde/framework floor | ~5.0–5.5 | nothing here (ForSt pays it too) |
| mandatory trailing `next()` crossing (lock+registry+guarded call) | ~1.3 | **P0** |
| mandatory `close()` crossing + registry insert/remove of dead shell | ~1.2 | **P0** |
| serial first-chunk fill on 1 thread (block decode + merge + memcpy) — *queueing* cost behind K-1 peers | ~2.0 | **P1** (K≈4 workers ⇒ ~÷4) |
| per-row alloc pair + O(sources) merge scan | ~0.8 | **S2** (§2.2) |
| Java decode + misc | ~1.4 | partially P3 (SoA decode) |

**Win stack (multiplicative ordering, midpoints):**

| Stage | q7 model | running total |
|---|---|---|
| baseline (build-parallel) | — | 1052 s |
| **P0** EOF+auto-close: −2.5 µs × 9×10⁷ | −225 s | ~830 s |
| **P1** parallel first-chunk fill: −1.5 µs | −135 s | ~695 s |
| **S2** pinned rows + loser tree: −0.8 µs | −72 s | ~625 s |
| **P3** SoA chunk decode: −0.5 µs | −45 s | ~580 s |
| **§2.1** prefetch (q7 sees it only in cold/post-flush phases, 26 % cold preads at 182 µs, db.rs:6803) | −30–60 s | **~520–550 s** |

**Expected q7: ~520–550 s ⇒ 1.07–1.13× faster than ForSt's 586.8 s.** The ring
protocol (P2) and deep readahead contribute little to q7 but are the q9/q11/q19/
q20 levers (crossing count ÷16, I/O–decode overlap on long scans).

**Sensitivity / falsifier:** if Stage P0 measures < 120 s of win, the
crossing-cost estimate is wrong and the framework floor is higher than modeled —
in that case ≤550 s is unreachable from the read path alone and the campaign
should stop after P1/S2 and re-profile (this is why the cheapest stages go
first: they double as model probes).

---

## Part 4 — Staged implementation plan

Each stage: default-OFF env flag → gates → flip default. Every stage is
independently measurable on q7@100M (8c/32g docker, n≥3 per the box-noise rule).

| Stage | Scope (≈size) | Measure | Gates |
|---|---|---|---|
| **0. P0 EOF flag + auto-close** | FFI lib.rs (`_reserved` bit, skip-register-on-EOF) + Java drain loops (~200 LoC) | q7 wall, ITER_DISPATCH_DIAG crossing counts | engine+FFI suites; Java backend suite (114/0); q7 output exactness vs RocksDB-seeded run; q3/q4 no regression |
| **1. P1 parallel first-chunk fill** | lib.rs Pass 2/3 restructure (~150 LoC, engine untouched) | q7 wall | byte-identical q7 sample vs serial path; suites; TSAN-style review of handle confinement |
| **2. S2 pinned rows + loser tree** | engine db.rs TierKeySource/LazyPrefixIter + per-block arena (~600 LoC) | q7 + q9 wall; alloc profile (per-probe allocs → ~0) | full storage(355)+engine(277) suites; byte-equiv harness over q0-q22 5M correctness sweep; q4 point-get no-regress |
| **3. §2.1 BlockPrefetcher, sync multi-block first** | storage reader + tier source (~400 LoC); then async double-buffer on I/O pool (~300 LoC) | q9/q20/q19 wall; q7 no-regress; block-cache hit-rate telemetry (pollution check) | suites; cold-cache q7 phase improvement; cache hit-rate on q3 unchanged |
| **4. P2 ring protocol + adaptive chunks + P3 SoA format** | FFI new symbols + Java ring consumer (~800 LoC) | q9/q11/q19 wall; crossing counts | suites; lockstep exactness ×2; routing-async ×5 (the Stage-0 timer regression rule); only proceed if q9-class still fails the bar after Stage 3 |

**Correctness gates throughout** (project law: correctness before perf): seeded
same-input replay byte-exactness per query touched; the q5-class windowed-value
check (pane counts mask value bugs); no iterator-leak (watchdog + RSS steady on
q9@100M); abort/error-path tests for every new protocol state (EOF×deferred-error,
ring×abort).

**Explicit non-goals:** write path, WAL, compaction scheduling; SST on-disk
format changes (block size, restart interval, compression); memory-model /
resident-shadow changes; point-get & multiGet path changes; mmap; io_uring;
checkpointing.

---

## Part 5 — Alternatives considered

| Alternative | Verdict | Why |
|---|---|---|
| **io_uring** for block fetch (Linux) | Defer | True async pread with kernel-side batching — but dev is macOS (unmeasurable locally), opendal/S3 path can't use it, and the thread-pool double-buffer (§2.1.4) captures the overlap portably. Revisit if I/O-pool threads saturate at <NVMe bandwidth. |
| **mmap** SST files | Reject | READ_AT_DIAG shows warm preads <1-2 µs (cached_fs.rs:712-748); mmap removes the syscall but not the page-in (the ≥20 µs cold class, cached_fs.rs:738-741 comment), double-caches against the decoded block cache, and complicates the S3-evicted fallback path. |
| **Full Arrow IPC streaming** chunks | Reject (take SoA-lite, §2.3-P3) | Per-chunk FlatBuffers schema + padding overhead on a static schema; Java Arrow reader allocation behavior on the hot path; SoA layout delivers the castability without the framing. |
| **Bigger blocks (256 KiB)** | Reject | Helps only R-long (already served by multi-block readahead), hurts point-get latency + cache granularity + cold-miss decompress; format migration for a lever readahead supersedes. |
| **Async engine (tokio) per-op** | Reject | Already disproven in this repo: per-op async dispatch + opendal indirection measured as diffuse overhead on local I/O (q4 noflush investigation). The design keeps sync consumers + a small dedicated I/O pool. |
| **Per-iterator OS thread driving the whole scan** | Reject | Duplicates the probe-level fan-out for R-short, leaves R-long single-iterator scans with 1 I/O stream; per-source prefetch + ring producer composes with both (§2.5). |
| **Heap-merge value re-walk removal** | Already done | Value-carrying merge (db.rs:6470) — kept, built upon. |

---

## Part 6 — Self-review against the brief

- Every layer covered: I/O (§2.1), engine iterator (§2.2), FFI protocol (§2.3),
  data layout (§2.4 — explicit "do not change" verdict with reasoning),
  parallelism composition (§2.5), quantified model (§3), staged plan + gates +
  non-goals (§4), alternatives (§5).
- Every present-state claim carries a file:line citation; RocksDB/ForSt internals
  (§1.4) and all µs/probe numbers (§3) are explicitly marked model/estimate.
- Mandates: zero-copy preserved and *strengthened* (pinned rows remove the last
  per-row allocs; one memcpy SST→chunk; ring handoff is copy-free); batch-only
  crossings (P0 removes crossings, P2 amortizes to ~0/chunk); Arrow-style
  off-heap layout (P3 SoA); no per-record execution added anywhere.
- Expected outcome: **q7 ≈ 520–550 s vs ForSt 586.8 s (≥1.05×)** from stages
  P0+P1+S2+P3+prefetch, with P2/deep-readahead carrying q9/q11/q19/q20; cheapest
  stages first double as model falsifiers.
