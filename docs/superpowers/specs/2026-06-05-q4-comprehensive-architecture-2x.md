# q4 → 2× RocksDB + flat curve: COMPREHENSIVE multi-level architecture program

**Goal:** 2× over RocksDB on the state backend with ZERO q4 decay (flat curve), via end-to-end
vectorization / zero-copy / batch / SIMD + concurrency. Each change proven by FULL-LENGTH A/B (throughput +
flat curve) under a correctness gate. **Stacked**, not single-point — small per-change gains (e.g. +2%)
COMPOUND; do not dismiss them as noise.

**Why one change isn't enough (user's thesis, accepted):** the live CPU stress = insufficient concurrency +
batch → poor instruction/data cache hit rates + no SIMD. The fix is multi-level and compounding.

## Foundation (committed, same-machine data)
- RocksDB q4 (this Mac): 98M / 241s, FLAT ~400K (incremental ckpt).
- forst-rs A-fix (84dee6040): periodic L0 trigger, +17%.
- forst-rs drain default-on (595faad76): bounded L1→L2 → **q4 FINISHES 98M/461s** (stability; was never
  finishing). Still ~1.9× slower + troughs.

## The compounding stack (each: design → microbench/A-B → full-length flat-curve A/B → correctness gate)
1. **CONCURRENCY — parallel sub-compaction.** Today the merge is single-threaded (global compaction_mutex,
   1 forst-rs-compact thread); a 25s L1→L2 burst leaves the Mac's other cores idle and starves the
   foreground → troughs. Partition the sorted merge output by key range → emit N non-overlapping output SSTs
   on N threads (std::thread::scope; emit_key_versions reused per partition). Burst 25s → ~25/N s → shallower
   trough → flatter. [highest flat-curve leverage]
2. **CACHE/ZERO-COPY — columnar gather.** Today compaction gathers `Vec<CompactionEntry{key:Vec,value:Vec}>`
   = 2 scattered heap allocs/row → pointer-chasing → poor data-cache hit rate, amplified live by pipeline
   cache pressure. Move to contiguous key/value byte buffers + offset arrays (Arrow-style) + index-sort →
   sequential, cache-friendly, fewer allocs.
3. **BATCH/SIMD — vectorized codec + bloom + compare.** KV decode/encode, bloom hashing, and key compares
   are per-row scalar. Batch over contiguous buffers + SIMD (std::simd / jdk.incubator.vector is already
   enabled on the JVM side; Rust side use packed ops) for the hot loops.
4. **Read path:** stack the resident-bloom-skip (+2%) + resident-bypass to match RocksDB's leaner read.

## Evidence log (appended as each lands)
- [stacking thesis test] drain + bloom-skip full-length vs drain-alone (461s): (pending)

## Evidence log
- **Stacking-thesis test (drain + bloom-skip, full-length MAXSEC 600):** finished 98M ~505s vs drain-alone
  461s. The bloom-skip's short-window +2% did NOT compound positively at full length — within the large
  trough variance (28K–227K swings). ⇒ the troughs (compaction bursts) dominate; small read-path wins are
  swamped by them. Confirms the burst is the target.
- **Parallel sub-compaction (CONCURRENCY level) IMPLEMENTED** (`emit_one_sst` + `thread::scope` partition
  emit in `CompactionJob::run`, env `FRS_COMPACT_PARALLEL`). Correctness: partitions are disjoint
  key-boundary-aligned ranges of the sorted merge → all versions preserved, non-overlapping output. Gated
  OPT-IN (default off) because row-count partitioning does not yet byte-match the serial size-based file
  layout (broke a layout-asserting test under default-on); engine suite 263 green with it default-off.
  A/B vs serial-drain (461s): q4 finished ~564s with 139 compactions / 962s total compaction (vs ~22 serial) — the row-count partitioning makes more/smaller files → more downstream drain compactions → MORE total work, negating the parallelism. NO gain (within trough variance, not faster). REFUTED as implemented; needs layout-preserving partitioning (same file count as serial, parallel emit) to isolate the concurrency benefit. Kept opt-in default-off.

## Evidence chain — parallel compaction REFUTED (both partitionings); reveals the real direction
- row-count partitioning: q4 ~564s, 139 compactions / 962s compaction (file-count explosion).
- **size-based partitioning (layout-equivalent to serial; suite 263 green parallel-on):** q4 did NOT finish
  in 600s, **n=189 / 1315s total compaction**, troughs deeper (16–33K).
- **MECHANISM:** parallel compaction spawns N threads that steal cores from the FOREGROUND pipeline during
  each burst → the foreground (the throughput we want) starves harder → deeper troughs → slower. On a shared
  machine, MORE compaction concurrency = WORSE. RocksDB RATE-LIMITS compaction (gives it LESS) to protect the
  foreground — the OPPOSITE of "add compaction concurrency".
- **⇒ Corrected direction:** the concurrency/batch lever must target the FOREGROUND per-record path (process
  records in batches → fewer FFM crossings + engine ops → lower foreground CPU → less contention → flatter),
  NOT compaction parallelism. Plus compaction THROTTLING (cap its core/IO use). q4's interval-join arrives
  per-record (Flink V1-sync operator); batching its state ops within forst-rs is the lever, but the operator
  itself is Flink-runtime (out of the backend's scope) — the in-scope piece is the MapState/write-buffer
  batching + a throttled single-thread compaction (current default) which already finishes (461s).
