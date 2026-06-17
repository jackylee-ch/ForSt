# q20 regular-join gap (1.73× RocksDB): profile, model, uniform upgrade design

**PMC-1 Performance — 2026-06-18.** Profile-first. The Phase-1 uniform-gate sweep
records **q20 forst-rs 1146.5 s vs rocksdb 661.7 s = 1.73× SLOWER** at 2×4c/16g
(TOPO=split), both exact (forst-rs 93,201,404 == rocksdb 93,201,404). q20 already
beats ForSt-C++ (1342 s); the gap is vs RocksDB. This doc establishes WHERE the
1.73× goes on the **current tip** (which already ships R-1 zero-copy point-get +
S2 pinned-rows/loser-tree), builds the architectural model, and proposes the
uniform upgrade.

---

## 0. q20 shape (ground truth)

```sql
INSERT INTO nexmark_q20
SELECT auction, bidder, price, ..., A.itemName, A.description, ...
FROM bid AS B INNER JOIN auction AS A ON B.auction = A.id
WHERE A.category = 10;
```

A **regular (unbounded, non-windowed) INNER JOIN** keyed on `bid.auction = auction.id`.
No time bound, no TTL → **both join sides accumulate state for the whole stream**.
Flink's `StreamingJoinOperator` builds two `JoinRecordStateView`s
(`flink-table-runtime/.../join/stream/state/JoinRecordStateViews.java`):

- **auction side** (`auction.id` IS the PK and == join key) →
  `JoinKeyContainsUniqueKey` → a **ValueState<RowData>** (one auction row per join
  key). Category=10 auctions are FEW. With KV-sep ON the >256 B auction row is
  blob-separated. Cheap side.
- **bid side** (keyed by `bid.auction`, NOT unique; bid has no declared PK) →
  `InputSideHasNoUniqueKey` → a **MapState<bidRow, Integer count>**. The map KEY is
  the **entire serialized bid record** (~100 B); the VALUE is a 4-byte multiplicity
  count. ~92 M distinct bids accumulate over the run.

Per bid record, the hot side does (StreamingJoinOperator.processElement, inner-join,
neither side outer):
1. `inputSideStateView.addRecord(bid)` →
   `cnt = recordState.get(bidRow); recordState.put(bidRow, cnt+1)` — a **dependent
   GET→PUT RMW whose key is the full ~100 B bid row**.
2. probe the auction side (ValueState by join key) and emit. Cheap.

So q20's per-record dominant cost is the **bid-side MapState RMW + the prefix-scan
of `entries()`** over a key space of ~92 M large keys, none expiring.

---

## 1. PROFILE (symbolized, CURRENT tip — S2 + R-1 ON)

Fresh symbolized `perf` capture (2026-06-18), CURRENT uniform config
(`FRS_KV_SEPARATION=true`, `FRS_RS_S2_PINNED=1`, `FRS_S2_FANOUT_MIN=8`, lz4),
single-TM 8c/36g, perf delay 500 s / dur 180 s — captured in deep steady state
(~60 M ingested, join + compaction both saturated). `target-linux/perf-q20-*.txt`.
Numbers are children % per perf-thread (one process, threads pinned by role).

**Two cost centers, ~equal:**

| thread / tree | children % | what it is |
|---|---|---|
| **Join `frs_vec_iter_prefix_open_batch`** → `prefix_scan_iter_owned_arc_with_error_slot` (19.6) → `build_lazy_prefix_key_stream_sel` (19.4) → `PrefixScanStream::fill_into` (10.6) → `ArcPairAdapter::next` (10.6, **S2 path**) → `LazyPrefixIter::next_step_pinned_LINEAR` (9.8) → `TierKeySource::peek` (9.0) | **30.6 %** | bid-side `getRecords()` / count-map prefix scan |
| **Compaction `CompactionJob::run`** → `run_streaming_with` (25.6) → `emit_key_versions` (16.0) → `add_internal` (15.3) → `flush_block` (11.4) + `encode_kv_data_block` (8.1) + `compress` (6.4); read side `load_next_nonempty` (7.1) → `read_decoded_block` (6.9) | **26.9 %** | compaction of the large-key count-map SSTs |
| Join `batch_get_vectorized` | 6.9 % | scattered bid-row GET (the RMW read half) |

**Top self-costs (flat, non-kernel):**

| self % | symbol | meaning |
|---|---|---|
| **6.83** | `memcmp` (Join) | comparing the huge ~100 B composite bid keys in the merge/peek/btree |
| **4.90** | `lz4_flex compress` (compaction) | recompressing the large-key SST blocks |
| **3.83** | `SstReaderImpl::may_contain_range` (Join) | per-probe SST-overlap metadata search at scan-open |
| **3.14** | BTreeMap `find_key_index` (Join) | memtable key search (huge keys) |
| 2.05 | `strncmp` (Join) | key compare |
| 1.42 | `Version::live_sst_file_numbers` (Join) | per-probe version SST-set walk |
| 1.15 | `decompress` (compaction) | |
| 1.09 | `Sbbf::check_hash` (Join) | bloom probe |
| 1.08 | `ShardedMemTable::get` | |
| 0.77 | `end_block_for_upper` (Join) | per-probe block-bound search |
| **0.57** | `getenv` (Join) | **config-gate env lookups on the hot path (not OnceLock-cached)** |

### 1a. Decisive reads of the fresh profile

1. **S2 IS engaging but in its LINEAR phase, not the loser tree.** The hot frame is
   `next_step_pinned_LINEAR`, not `…_tree`. The tree builds only at
   `sources.len() >= S2_TREE_MIN_SOURCES = 5` (db.rs:18864) and self-abandons when
   dup-dominated (db.rs:19208). q20's bid-prefix scans merge **< 5 sources** →
   **the loser tree never fires; only the pinned-rows alloc-free benefit applies,
   and the merge stays linear.** This is why shipping S2 did NOT collapse q20's
   ~21 % → still ~30 % prefix-scan share: **q20 is NOT a deep-fan-out problem.**
   The 2026-06-15 loser-tree mini-bench validated the lever at fan-out 8-64 — a
   regime q20's prefix scans do not reach.
2. **The prefix-scan cost is dominated by KEY COMPARISON over the huge bid keys**
   (`memcmp` 6.83 % + `strncmp` 2.05 % + btree `find_key_index` 3.14 %) and by
   **per-probe SST-set/overlap bookkeeping** (`may_contain_range` 3.83 % +
   `live_sst_file_numbers` 1.42 % + `end_block_for_upper` 0.77 % +
   `check_hash` 1.09 %) repeated for ~92 M probes. The ~100 B composite key is the
   q20-specific multiplier vs q11's ~16 B key.
3. **Compaction is the OTHER ~equal half, and its dominant term is the WRITE/encode
   side** (`add_internal` 15.3 % / `flush_block` 11.4 % / `encode_kv_data_block`
   8.1 % / lz4 `compress` 6.4 %), NOT the read side (`load_next_nonempty` /
   `read_decoded_block` ~7 %). **L4 (windowed compaction reads + cache bypass)
   targets only the ~7 % read side — it cannot touch the ~19 % write/encode cost.**
   The write cost is intrinsic to re-emitting + recompressing ~10 GB of large-key
   count-map SSTs.
4. **`getenv` 0.57 % self on the join hot path** — some config gates re-read env per
   call instead of caching behind `OnceLock`. Small but a pure, byte-identical win.

### 1b. Prior reference profile (2026-06-11, PRE-R1/PRE-S2)

`target-linux/perf-q20-{flat,graph}.txt`, children view, dominant trees:

| tree | children % | what it is |
|---|---|---|
| `frs_vec_iter_prefix_open` → `prefix_scan_iter_owned_arc_with_error_slot` → `build_lazy_prefix_key_stream` → `LazyPrefixIter::next_with_value` | **21.6 %** | bid-side `getRecords()`/`entries()` prefix scan over the count-map |
| `CompactionJob::run` → `emit_key_versions` + `SstWriterImpl::add_internal` + `load_next_nonempty`→`read_decoded_block` | **19.3 %** | compaction of the large-key count-map SSTs |
| `batch_get_vectorized` → memtable/SST point-get | **5.9 %** | scattered bid-row GET (RMW read half) |

Flat view: `memcmp` 4.9 %, BTree `find_key_index` 3.4 %, `may_contain_range` 2.1 %,
`ShardedMemTable::get` — i.e. the cost is dominated by **comparing/searching the
large composite bid keys** in the memtable BTreeMap and SST blocks. The huge key is
the q20-specific aggravator vs q11.

---

## 2. MODEL — why forst-rs is 1.73× RocksDB on q20

q20's CPU splits **~equally between a join-side prefix-scan (~31 %) and compaction
(~27 %)**, and BOTH terms are dominated by the **huge ~100 B composite bid key**, not
by a fixable structural inefficiency that S2/R-1/L4 happen to miss.

**Class:** q20 is the **scattered point-RMW + shallow prefix-scan read-amp class
(q11 family)** — NOT the q9 deep-partition-scan class (the loser tree's regime) and
NOT a write-amp-only gap. But it carries three q20-specific multipliers that q11
does not, and these multipliers are the gap:

1. **The map key is the full bid record (~100 B vs q11's ~16 B).** This is the single
   root cause that shows up in EVERY hot term:
   - join side: `memcmp` 6.8 % + `strncmp` 2.0 % + btree `find_key_index` 3.1 %
     (~12 % of total) is pure large-key comparison in the merge/peek/memtable;
   - join side: `may_contain_range` 3.8 % + `live_sst_file_numbers` 1.4 % +
     `end_block_for_upper` 0.8 % + bloom `check_hash` 1.1 % (~7 %) is per-probe
     SST-overlap bookkeeping that scales with the number of SSTs, which a large key
     inflates (fewer entries/block → more blocks/SSTs for the same row count);
   - compaction: `add_internal` 15.3 % / `flush_block` 11.4 % / `encode_kv_data_block`
     8.1 % / lz4 `compress` 6.4 % is re-emitting + recompressing ~10 GB of
     key-dominated SSTs — the key bytes ARE the compaction volume.
2. **No state expiry / unbounded join state.** ~92 M distinct bids accumulate forever
   → the read-your-writes `MapStateCache` thrashes totally in steady state → every
   `addRecord` RMW is a real scattered engine point-get + write-back, and `getRecords`
   re-scans the growing count-map.
3. **RocksDB does the SAME work through a more mature C path:** restart-interval key
   compare on raw block bytes with a decade-tuned prefix-bloom + block cache + a
   compaction encoder that is years ahead of `lz4_flex` + the v2 KV encoder. forst-rs
   already adopted the *shape* (R-1 zero-copy point-get on raw blocks; S2 pinned
   rows) — the residual ~1.73× is the **per-byte constant factor over a key that is
   6× larger than q11's**, on both the compare and the encode/compress paths.

**Why the shipped levers do NOT close it (measured, not modeled):**

- **S2 loser tree is the wrong regime.** The hot frame is
  `next_step_pinned_LINEAR`, not `…_tree`. q20's bid-prefix scans merge **< 5
  sources** (< `S2_TREE_MIN_SOURCES`), and the tree self-abandons when dup-dominated.
  The 2026-06-15 mini-bench validated the tree at fan-out 8-64 — a regime q20 never
  reaches. S2's pinned-rows alloc-free benefit DOES apply (and is kept), but the
  merge-speedup half is inapplicable to q20. **This overturns the roadmap's L3 =
  "top modeled q20 lever (−9-13 %)" — at full stack, L3 delivers only the pinned-row
  fraction, not the merge fraction.**
- **L4 attacks the wrong half of compaction.** Compaction's cost is ~⅔ WRITE/encode
  (`add_internal`/`flush_block`/`encode`/`compress` ≈ 19 % of total) and ~⅓ READ
  (`load_next_nonempty`/`read_decoded_block` ≈ 7 %). L4 (windowed reads + input-cache
  bypass) only overlaps/cheapens the READ ⅓ and stops cache pollution; it cannot
  reduce the dominant write/encode term. **Modeled L4 win on q20: at most a third of
  7 % ≈ 2-3 % of total + a second-order join cache-hit gain — far below the roadmap's
  −6-10 %.**

**Verdict:** q20's 1.73× is **mostly structural** — a per-byte constant-factor gap on
a 6×-larger key, split across compare (read) and encode/compress (compaction). It is
the q11 read-amp CLASS, aggravated by key size and unbounded state. There is **no
single dominant fixable term**; the cost is spread across compare + bookkeeping +
encode + compress, each a mature-C-vs-Rust constant factor.

---

## 3. DESIGN — uniform upgrade (hard constraints honored)

Given the model, the honest position is: **there is no large, low-risk, byte-identical
lever that closes q20 to ≤1.25× in the engine.** The dominant terms are constant
factors. The candidate levers, ranked by realistic win, all uniform / no per-query
branch / no Peter-for-Paul:

### D-1 (low-risk, byte-identical) — hot-path env-read elimination
`getenv` is **0.57 % self / 2.73 % children** on the join thread (env-name `strncmp`
scans). At least one config gate on the per-scan/per-probe path is not `OnceLock`-
cached across all worker threads. Audit `frs_vec_iter_prefix_open_batch` →
`build_lazy_prefix_key_stream_sel` → `TierKeySource::peek` and the FFI scan-open for
any `std::env::var` reached per scan; cache behind `OnceLock`/atomic. **Win: ~1-2 %
of total, byte-identical, zero semantic risk.** This is the only clearly-worth-it
lever and it is small.

### D-2 (medium, byte-identical) — per-probe SST-set bookkeeping hoist
`may_contain_range` 3.8 % + `live_sst_file_numbers` 1.4 % + `end_block_for_upper`
0.8 % (~6 % total) is recomputed per prefix-scan-open. For the join's repeated probes
into the SAME count-map CF, the version's live-SST set and per-prefix overlap are
largely stable between flushes. A version-epoch-keyed cache of the overlap result
(invalidated on version change) could remove most of the recompute — analogous to the
APPROACH-1 `PersistentProbeIter` already in-tree (db.rs:10957, gated
`FRS_PERSISTENT_PROBE_ITER`). **Win: up to ~4-5 % of total IF probe locality holds;
risk: correctness on version change + memory. Needs a mini-bench + byte-equiv gate
before any flip. Medium lift.**

### D-3 (large, NOT recommended now) — narrower count-map key
The root multiplier is the 100 B key. The only way to shrink it is operator-side: the
no-UK MapState key is the full bid row by Flink's design (`InputSideHasNoUniqueKey`).
A backend cannot change it (out of scope, and the OPT-N04 scope note already records
that backend-transparent merge cannot capture the no-UK count map). A Flink-side
change (hash the record to a compact surrogate key + store the row in the value) is a
correctness-sensitive operator rewrite — large, risky, and out of the engine's
uniform-config remit. **Banked, not for this cycle.**

### Rejected for q20
- **L3 / S2 loser tree** — wrong regime (< 5 sources); already ON, no further q20 win.
- **L4 compaction windowed reads** — attacks the ~7 % read ⅓, not the ~19 % write
  half; modeled ≤ 3 % on q20. (Still defensible as a uniform lever for q9's
  read-heavy compaction, but not a q20 closer — measure there, not here.)
- **OPT-N04 engine merge for the count** — MapState-shaped, backend-opaque (scope
  note); also risks a longer read-side merge chain on the per-probe count read.

---

## 4. Estimated win & honest ceiling

- **D-1 (env hoist):** ~1-2 % → q20 ≈ 1.70× (negligible vs the bar).
- **D-1 + D-2 (env + probe-bookkeeping cache):** plausibly ~5-7 % → q20 ≈ 1.60-1.64×
  IF D-2's locality assumption holds at 100M; still far from ≤1.25×.
- **≤1.25× RocksDB on q20 is NOT achievable in the engine alone.** The gap is a
  per-byte constant factor on a 6×-larger key, spread across key-compare (read) and
  encode+compress (compaction). Closing it needs either (a) a narrower operator-side
  count-map key (D-3, Flink-side, large/risky) or (b) maturing the compaction
  encoder + key-compare to RocksDB's tuned-C parity — a long micro-optimization
  campaign, not a single lever. forst-rs already BEATS ForSt-C++ on q20 (824 vs 1342
  single-TM; 1146 vs 1342-class at split), so the Rust engine is competitive with a
  mature C++ engine here; RocksDB's specific point-read + compaction maturity is the
  remaining delta.

**Recommendation:** ship **D-1** as a small byte-identical hygiene win; **prototype
D-2** behind a flag with a mini-bench falsifier (if it lands < 60 % of model, drop it);
**do NOT** spend a risky implementation cycle chasing ≤1.25× on q20 — the residual is
structural constant-factor, and the engineering ROI is better on the read-amp CLASS
queries where the key is small (q11/q17) and R-1/D-2 compound more favorably.

---

## 5. Evidence reproduction & cleanup

- Profile: `RES=36g FRS_PERF=1 FRS_PERF_DELAY=500 FRS_PERF_DUR=180 …
  run-one.sh q20 forst-rs-ffm-local` with a SYMBOLIZED Linux `.so`
  (`strip=none`, `debug=1`); outputs `target-linux/perf-q20-{flat,graph}.txt`
  (2026-06-18 capture). Single-TM 8c/36g (the split topology does not support
  `FRS_PERF`); the CPU-share SHAPE transfers, the absolute wall does not.
- q20 baselines (authoritative, split 2×4c/16g uniform): forst-rs 1146.5 s vs
  rocksdb 661.7 s = 1.73×, both exact 93,201,404 (origin/forst-rs tip 563f2df20).
- No code changed for the profile/model. The worktree Cargo.toml `strip` override
  was reverted at cleanup.
