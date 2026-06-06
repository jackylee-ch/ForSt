# q4 binder = memory-bandwidth-bound compaction merge → zero-copy streaming merge

**Date:** 2026-06-05. Confirmed root cause + implementation plan. Clean rebooted 18-core M5 Pro, swap=0.

## Confirmed diagnosis (every alternative ruled out with data)

forst-rs q4 = ~514-545s vs RocksDB 241s (~2.1×). Decay = throughput sawtooth (bursts 500-640K →
troughs 38-110K). Binder isolated by elimination + instrumentation:

| candidate | verdict | evidence |
|---|---|---|
| memory swap | ❌ ruled out | clean reboot, swap=0 whole run, RSS 27-45G < 64G, still decays |
| background scheduling | ❌ not the binder | Component A (bounded shared pool) caps peak CPU 1451→1127% but finish ~unchanged |
| write-amplification | ❌ ruled out | instrumented: wamp_total plateaus ~1.9-2.1×, per-compaction input CAPPED ~400MB (not O(N²)); LOWER than RocksDB (~10-15×) |
| **compaction merge speed** | ✅ **THE binder** | **21-37 ns/byte LIVE vs 3.3 isolated = 7-11×**; each compaction 2.5-12s → L0 write-stall |

**Phase split (gather=per-row to_vec allocs, sort, emit=Arrow re-encode+write):**
- ISOLATED (microbench, 3.3 ns/byte): gather 28% / sort 14% / **emit 58%**.
- LIVE (~21-37 ns/byte): gather 38% / **sort 9%** / emit 53%.

The compute phase (sort) inflates LEAST under load; the memory-traffic phases (gather+emit =
86-91%) inflate most ⇒ the 7-11× penalty is **memory-BANDWIDTH contention** between compaction
and the foreground interval-join over the shared memory controller. The merge moves ~3× the data
(gather copy → sort → emit re-encode copy), and that traffic is both its own cost AND what it
steals from the join.

**Why zero-copy is the correct lever (not core-isolation):** core-pinning does not reduce
bandwidth contention (shared memory controller). The only way to cut bandwidth contention is to
cut bandwidth *demand* — which zero-copy does (3× → ~1× data moved). Component A reduces *how many*
compactions run at once; zero-copy reduces *how much memory each moves*.

## The fix — zero-copy streaming k-way merge (compaction.rs)

Replace `CompactionJob::run`'s gather-sort-reencode with a streaming merge:
```
TODAY:  for each input SST: scan_borrowed → push CompactionEntry{key:to_vec, value:to_vec}  (2 allocs/row)
        all.sort_by(key, seq)                                                                 (sort owned Vec)
        walk groups → emit_key_versions → StreamingSstWriter (Arrow re-encode)                (full copy)
TARGET: k-way min-heap over PULL iterators (one current borrowed RowView per input SST)
        pop smallest (key, effective_seq) → consolidate versions inline → stream to writer
        no per-row Vec alloc, no intermediate Vec<CompactionEntry>, no sort pass
```

### Prerequisite (foundational): pull-based SST iterator
The reader today is PUSH-only (`scan_borrowed(cb)`); `scan` returns owned `Vec<SstScanRow>`. A
k-way merge holds k current entries simultaneously, so each input needs a **pull iterator** that
owns its current decoded block and yields a borrowed `RowView` into it, advancing within the block
and loading the next block on exhaustion. New in `forst-rs-storage/src/sst/`:
`SstBlockCursor` — `current() -> Option<RowView>`, `advance() -> Result<()>`. TDD: ordered yield,
block-boundary crossing, exhaustion, empty SST.

### Merge core (compaction.rs)
`BinaryHeap<Reverse<HeapKey>>` where `HeapKey = (key bytes, effective_seq, input_idx)`. To compare
without owning, each heap entry holds the input_idx and the cursor's current key is read on demand;
simplest correct first cut keeps a small per-input current-key `Box<[u8]>` (one key alloc per
*advance*, not per row pair — far less than today's 2 allocs/row incl. values). Preserve EXACTLY
the existing consolidation in `emit_key_versions`: newest-wins, delete-tombstone drop (emit only if
not bottommost), merge-chain `full_merge` collapse, and the multi-file `target_file_size` rolling
at user-key boundaries.

## Correctness gate
- Reuse the full compaction suite (263 engine tests incl. `test_compact_level_overlapping_files_*`,
  merge-chain, tombstone, multi-file). Output must be byte-equivalent to today.
- New unit tests for `SstBlockCursor` + a `merge_streaming` equivalence test (same inputs → same
  output as the gather-sort path).
- A/B (clean machine, NO `FRS_WAMP_FILE` — its per-compaction mutex serializes & distorts):
  measure live ns/byte before/after (target 21→single digits), per-compaction run_ms (target
  seconds→sub-second), trough depth, and q4 finish vs 545s baseline / 241s RocksDB.

## Payoff bound (to be confirmed empirically by the A/B)
Cutting merge traffic 3×→1× should cut both intrinsic (isolated 3.3→~1.5 ns/byte) and the
bandwidth-contention multiplier. Estimated live ns/byte 21→~8-10, compaction 8s→~3s → most
write-stall troughs removed. Expected to move q4 materially toward RocksDB but likely NOT all the
way to 241s alone — the remaining gap is the foreground V1-sync per-record path (separate lever,
out of the state-backend's batching scope as q4's interval-join is a Flink sync operator).

## Status
- Component A (shared bounded bg pool) LANDED, default-on (flush=cores/3, compact=cores/2), 264 tests green.
- Write-amp + phase-split instrumentation in place behind `FRS_WAMP_FILE` (zero prod cost; REMOVE or keep gated).
- Next: implement the pull iterator + streaming merge per above (own session, TDD).
- See [[project_q4_binder_is_merge_speed_2026-06-05]].
