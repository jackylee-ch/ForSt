# Compaction Read-Path Windowed I/O + Cache Policy (roadmap step ⑤ / L4)

**Date:** 2026-06-12
**Status:** IMPLEMENTABLE SPEC (no code changed)
**Parent:** `2026-06-12-q9-q20-longscan-roadmap.md` L4.
**Target:** q20's recorded **19.3 % compaction CPU share** (and q9's heavy
compaction load) — currently paid on a read path with no readahead, no
I/O–merge overlap, and full cache pollution of the join's hot set.

---

## 1. Today's compaction read path (verified at HEAD)

- The streaming k-way compaction merge (`CompactionJob::run_streaming`, the
  DEFAULT since FRS-ZERO-COPY-MERGE — compaction.rs:114-126) pulls each input
  through an `SstBlockCursor` (reader.rs:1142-1226).
- `SstBlockCursor::load_next_nonempty` (reader.rs:1170-1198) fetches **one
  block at a time** via `read_decoded_block(entry.block_offset,
  entry.block_size)` (:1177-1179) — strictly demand-paged, strictly serial
  with the merge: the merge stalls for pread + decompress + decode on every
  block boundary of every input.
- `read_decoded_block` does a cache-first check (reader.rs:399-413) and then
  **inserts EVERY block it reads into the decoded-block cache at
  `CachePriority::Low`** (reader.rs:473-494). A compaction reads every block
  of every input exactly once ⇒ for a multi-GB compaction this streams the
  whole input set through the cache at the same priority as the join's data
  blocks, evicting the hot set the q20 probes depend on (the very lesson of
  the 27ae792c3 block-cache-bypass incident, inverted: then we under-used
  the cache; now compaction over-writes it).
- `CachePriority::Bottom` exists and is *documented as* "Compaction-read
  temporary blocks. Initial countdown = 0 (evicted on first sweep)"
  (cache/mod.rs:108-127) — **but nothing uses it**: the compaction path has
  no way to express a priority (read_decoded_block hard-codes `Low`).
- The machinery to do better is ALREADY SHIPPED for the scan path:
  - `read_block_regions` — vectored multi-region read, ONE io_uring
    submission per window on Linux local files, bit-identical serial-pread
    fallback (reader.rs:905-939).
  - `fetch_window` — pool-side window production: cache-first per block with
    window *splitting* around hits, contiguous miss runs coalesced into
    single vectored reads, decode on the pool thread, cache insert **at a
    caller-chosen `priority`** (prefetch.rs:388-468). It is already
    parameterized exactly where this design needs a policy knob.
  - `BlockPrefetcher` — double-buffered window state machine with safe
    cancellation via dropped oneshot (prefetch.rs:180-218, 241-247) and the
    shared `ReadIoPool` (`clamp(cores/2, 2, 6)` threads, prefetch.rs:95-103).

## 2. Design

### 2.1 Cache policy: cache-first READ, **no INSERT** for compaction inputs

Decision matrix considered:

| Option | Verdict |
|---|---|
| Keep `Low` insert (today) | the 19.3 %-share path keeps evicting the join hot set — rejected |
| Insert at `Bottom` | better, but an insert at capacity still does victim eviction + clock-hand advance, decrementing countdowns of resident Low/High entries — a multi-GB streaming pass still degrades the hot set, just slower; also pays per-block insert cost for entries that are dead on arrival (countdown 0) | 
| **Skip insert entirely (`fill_cache=false` equivalent), keep the cache-first check** | **chosen** — compaction reads each input block exactly once; nothing re-reads compaction inputs (future reads hit the compaction *output*); the cache-first check stays so recently-flushed blocks (often still cached from their flush-read or from foreground probes) are served for free, preserving the 27ae792c3 lesson |

Implementation: widen `fetch_window`'s `priority: CachePriority` parameter
(prefetch.rs:395-400) to

```rust
pub(crate) enum CacheFillPolicy {
    Insert(CachePriority),  // scan path: existing behavior
    Skip,                   // compaction inputs: read-only cache use
}
```

and thread it to the `cache_insert_decoded` call site (prefetch.rs:460;
reader.rs:882-903). `Bottom` remains available (and becomes the *fallback*
mode behind the same flag if the no-insert A/B regresses — see gates).

*Known second-order accepted in v1:* the cache-first **get** bumps the hit
entry's clock countdown (standard CLOCK behavior) — a compaction pass over
cached blocks slightly extends their life. Harmless (those blocks were
foreground-hot anyway); noted for the telemetry review.

### 2.2 Windowed, double-buffered input reads

New constructor on the existing prefetcher (honest reuse — the state machine,
pool, cancellation and `fetch_window` are shared; only the policy knobs
differ):

```rust
impl BlockPrefetcher {
    /// Compaction-input mode: full-file scan [0, n_blocks), fixed window
    /// (no cold state, no ramp — sequentiality is known a priori), cache
    /// fill policy Skip, gated by FRS_COMPACT_WINDOWED (not the scan gate).
    pub fn for_compaction(reader: Arc<SstReaderImpl>, window_blocks: u32) -> Self
}
```

Differences vs scan mode, all expressed in `new`-time fields (no new state
machine):
- `next_block = 0`, `end_block = index_entries.len()` (full file).
- `ra_blocks = window_blocks` from the start; the ramp logic
  (prefetch.rs:380-385) is bypassed (mode flag) — a compaction never has a
  "cold probe" phase to protect.
- `CacheFillPolicy::Skip` for window production; the *demand* fallback path
  (cold fetch inside `next_decoded`) also runs with Skip in this mode.
- Double-buffering identical to scan mode: deliver from `ready`, submit the
  next window when `ready` drains and nothing is in flight
  (prefetch.rs:250-258) — input I/O + decompress + KvBlock decode overlap
  the merge on the `ReadIoPool` thread instead of stalling it.

`SstBlockCursor` gains the windowed source:

```rust
enum BlockSource { Demand,                          // today: read_decoded_block
                   Windowed(BlockPrefetcher) }      // compaction mode
```

`load_next_nonempty` (reader.rs:1170-1198) swaps the per-block
`read_decoded_block` for `fetcher.next_decoded()` when windowed; the
`CursorInner::{Arrow, Kv}` row-stepping (:1181-1194, 1206-1226) is untouched
(`KvBlockCursor::new(kv)` already takes the `Arc<KvBlock>` the prefetcher
delivers). Construction: `CompactionJob::run_streaming` builds windowed
cursors when the flag is on; the refuted `FRS_COMPACT_PARALLEL` gather path
(compaction.rs:120-126) is explicitly NOT migrated.

### 2.3 Memory bound and pool interaction (the write-amp/stall section)

- **Window size:** default `window_blocks = 32` (= 2 MiB at the 64 KiB
  default block size, config.rs:266 — matches RocksDB's
  `compaction_readahead_size = 2 MB` class). Env
  `FRS_COMPACT_WINDOW_BYTES` for A/B.
- **Aggregate budget:** worst case = fan-in × (1 ready + 1 inflight) windows
  = `inputs × 2 × 2 MiB`. A 20-input L0→L1 job ⇒ 80 MiB transient — visible
  on the 8c/32g box. Clamp at job start:
  `window_blocks = min(default, FRS_COMPACT_PREFETCH_BUDGET / (2 × inputs ×
  block_size))`, budget default 64 MiB, floor 4 blocks (below the floor →
  fall back to Demand mode). This is the same uncounted-memory class as
  roadmap finding M3 (scan prefetch caps) — the budget constants should land
  in one place (`runtime_tuning.rs`) so M3's fix and this share accounting.
- **Pool starvation:** compaction window jobs run on the SAME `ReadIoPool`
  as foreground scan prefetch. Per-cursor inflight is already ≤1 (the
  double-buffer invariant), so a compaction contributes ≤ fan-in queued jobs,
  each short (one window fetch+decode). Risk is bounded but real on the
  6-thread cap; gate G5 watches foreground p99. If it bites, the escape is
  job tagging + a simple "foreground first" two-queue pop in `ReadIoPool` —
  designed but NOT built in v1 (YAGNI until G5 fails).
- **Write-amp:** unchanged by construction — this is a read-side change; the
  compaction picker, output sizing, and trigger thresholds are untouched.
  Second-order: faster input reads shorten compaction wall-time, which
  *reduces* L0 pileup and write-stall pressure (same direction as the L1
  drain-gate lever; their A/Bs must be run separately to stay attributable —
  do NOT land both in one measurement window).
- **Error/cancellation:** pool-job errors surface on `next_decoded` exactly
  as scan mode (oneshot carries `ForstResult`); job abort (engine shutdown,
  Weak<DbImpl> upgrade failure — db.rs:10490-10499) drops the cursor →
  drops the handle → producer send fails harmlessly (prefetch.rs:170-178).
  H1 (pool-worker panic ⇒ recv hang, roadmap L0 blocker, bg_pool.rs:79-93)
  applies to this path too — **L0's catch_unwind fix is a prerequisite for
  flipping this flag default-ON** (a panicking compaction window job must
  fail the compaction, not hang it).

## 3. Work items

| # | File | Change | ≈LoC |
|---|---|---|---|
| W1 | `crates/forst-rs-storage/src/sst/prefetch.rs` | `CacheFillPolicy`; thread through `fetch_window` (:395-468) + demand fallback; `for_compaction` constructor + mode flag (skip ramp) | 90 |
| W2 | `crates/forst-rs-storage/src/sst/reader.rs` | `BlockSource` in `SstBlockCursor` (:1142-1198); `cache_insert_decoded` honors Skip (:882-903) | 60 |
| W3 | `crates/forst-rs-engine/src/compaction.rs` | windowed cursor construction in `run_streaming` input setup; budget clamp from fan-in | 50 |
| W4 | `crates/forst-rs-engine/src/runtime_tuning.rs` | `FRS_COMPACT_WINDOWED` (default OFF), `FRS_COMPACT_WINDOW_BYTES`, `FRS_COMPACT_PREFETCH_BUDGET` | 20 |
| W5 | telemetry | per-compaction: blocks read, cache hits, window submissions, bytes prefetched; cache hit-rate counters already exist for G4 | 30 |

## 4. Falsifiable model

Anchors: compaction = 19.3 % of q20 CPU (recorded); the share decomposes into
(a) merge/emit CPU (untouched), (b) per-block stall = pread + LZ4 decompress
+ decode serial with the merge (overlapped by W1-W3 onto pool threads),
(c) cache insert/eviction churn (removed by Skip). The 182 µs/26 %-cold pread
class (cached_fs.rs:712-748 buckets) applies when inputs fall out of page
cache — exactly the multi-GB q9/q20 compactions.

- Direct: overlap + coalescing claims a third-to-half of the 19.3 % ⇒
  **q20 −6..10 %**; q9 compacts at least as heavily ⇒ **−3..6 %**.
- Second-order: join hit-rate recovery from un-polluting the cache — shows
  up as a prefix-scan-share drop, NOT a compaction-share drop (watch both
  in the after-profile).

**Falsifiers:**
1. W5 telemetry must show ≥ 90 % of compaction input blocks arriving via
   windows (not demand fallback) — if not, the budget clamp or block-size
   assumptions are wrong; fix before reading wall-times.
2. If q20 improves < 3 % at n≥3 AND the after-profile still shows ≥ 15 %
   compaction share, the share is merge-CPU-bound (a), not I/O-bound — the
   next lever is compaction merge CPU (loser tree for compaction's k-way
   heap — S2's W2 module is reusable there), not deeper I/O.
3. Cache hit-rate on q3 (point-get canary) must be UNCHANGED (Skip removes
   inserts that q3 never benefited from; any q3 hit-rate delta means the
   policy leaked into a foreground path).

## 5. Gates

| # | Gate | Bar |
|---|---|---|
| G0 | storage+engine suites; new UTs: windowed-vs-demand cursor byte-equality over random multi-block SSTs (v1+v2, compressed, empty blocks — the :1196 empty-block skip), budget-clamp floor behavior, Skip-policy insert-count == 0 | 0 fail / exact |
| G1 | compaction output byte-equivalence: full flush+compact cycle flag ON vs OFF, identical output SST bytes (the existing compaction equivalence harness) | byte-identical |
| G2 | L0 prerequisite: bg-pool `catch_unwind` fix landed (roadmap H1) | merged first |
| G3 | q0-q22 @5M correctness sweep flag ON | all finish, counts exact |
| G4 | cache hit-rate telemetry q3 + q20 @100M: q3 unchanged; q20 data-block hit-rate ↑ | directionally per model |
| G5 | foreground scan p99 (ITER latency diag) during active compaction: no regression > noise (pool starvation check) | within noise |
| G6 | q20/q9 @100M ×3 A/B; write-stall counters unchanged; RSS delta ≤ budget | report vs model §4 |

**Mandate check:** read-side only; zero new copies (windows decode from one
packed buffer exactly as the shipped scan path, prefetch.rs:443-462);
batch-only untouched; no config drift (flags default-OFF, ForSt-parity
options unchanged).
