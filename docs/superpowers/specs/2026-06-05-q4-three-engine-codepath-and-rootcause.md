# q4 across RocksDB / Forst / Forst-rs — full code-path overview + root-cause (why Forst-rs trails RocksDB)

**Date:** 2026-06-05. Architect-level synthesis of the whole investigation, grounded in same-machine data.
**One-line answer:** Forst-rs's q4 engine is *competitive cold* (it starts FASTER than RocksDB, 628K vs
590K rec/s) but **decays under sustained load while RocksDB stays flat** — because Forst-rs's
**compaction is unthrottled and contends with the foreground for the same cores**, and its leveled
compaction is less write-amp-efficient than RocksDB's, so total CPU under load exceeds capacity → throughput
collapses in bursts. RocksDB wins by *protecting the foreground* (rate-limited compaction) + incremental
checkpoint + a lower-constant-factor C++ per-record path — not by a faster cold path.

---

## 1. The shared q4 execution chain (identical for all three engines)

```
NexMark q4 SQL  (AVG(final price) per category over an interval join)
  SELECT category, AVG(final) FROM (
    SELECT MAX(B.price) final, A.category
    FROM auction A, bid B
    WHERE A.id=B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
    GROUP BY A.id, A.category)
  GROUP BY category
        │
  Flink SQL Gateway → planner → ExecNode graph
        │
  Source(datagen) → Calc/WatermarkAssigner ─┐
                                            ├─► IntervalJoin[10]  ← THE state-heavy operator
  Source(datagen) → Calc/WatermarkAssigner ─┘        │  buffers both sides until watermark > expires
        │                                            ▼
  GroupAggregate[13] (MAX per auction) → GroupAggregate[16] (AVG per category) → Sink
```
- The **IntervalJoin** operator buffers every bid + auction in keyed state until the watermark passes the
  interval upper bound, then joins + evicts. This is the state firehose: ~100M rows in, GBs of buffered
  state, per-record put + per-record probe + timer-driven eviction.
- **Critical property:** the interval-join operator is a Flink **V1-SYNC** keyed operator — it calls
  `state.put` / `state.get` **one record at a time** and blocks on each. The per-record arrival cadence is
  set by Flink, NOT by the state backend. (Heritage: q4/session-window joins force V1-sync; the async/batched
  V2 state path is unavailable to this operator.) ⇒ the backend cannot batch record *arrival*; it can only
  batch *within* a record or at flush.
- All three engines see the **same operator, same per-record call pattern, same checkpoint cadence (30s)**.
  So any throughput difference is the **state backend / engine**, not the query or the hardware.

## 2. RocksDB path (JNI) — the reference. Same Mac: 98M / **241s, FLAT ~400-445K/s, finishes**
```
IntervalJoin → RocksDBKeyedStateBackend (Flink) → JNI → RocksDB C++:
  put  → WriteBatch → MemTable (lock-free skiplist) → WAL(off here)
  get  → MemTable + immutable memtables + L0..Ln SSTs via BLOCK CACHE (LRU)
  flush→ memtable → L0 SST (background)
  compaction → LEVELED, RATE-LIMITED (rocksdb rate_limiter + max_background_jobs),
               bounded per-level write-amp (~L0:4, Lk = base·10^(k-1))
  checkpoint → state.backend.incremental: TRUE → only NEW SSTs hard-linked/uploaded
```
Why it's flat: (a) compaction is **rate-limited + parallel-but-capped** so it never starves the foreground;
(b) **incremental** checkpoints are cheap regardless of state size; (c) decades-tuned C++ keeps the
per-record constant factor low, leaving CPU headroom for compaction to keep the LSM shallow.

## 3. Forst (community, original disaggregated) path (JNI) — reference
```
IntervalJoin → ForStKeyedStateBackend → JNI → ForSt C++ (RocksDB fork) with DISAGGREGATED remote state:
  state lives in remote object store (FileMappingManager link/refcount), local cache;
  INCREMENTAL checkpoints by linking remote SSTs (no re-upload).
```
Forst's design avoids full-state checkpoint cost via remote linkable SSTs. (Forst-rs adopted the
local-first cache but the checkpoint linkage differs — see §5.)

## 4. Forst-rs path (Panama FFM → Rust engine) — same Mac: 98M / **461s, DECAYS then finishes**
Java backend (Flink module): `ForStRsKeyedStateBackend` → V1 `ForStRsValueState` / `MapState` (q4 = V1-sync)
→ Panama **FFM downcall per record** → Rust engine (`forst-rs-engine`):
```
WRITE  batch_write/put (db.rs:2875) → ShardedMemTable (BTreeMap index) ; flush worker → L0 SST
READ   get_arc → build_lazy_prefix_key_stream (db.rs) — a k-way LazyPrefixIter MERGING:
         Tier-1 active memtable cursor (BTreeMap seek)
       + Tier-2 immutable memtables
       + Tier-2 RESIDENT SHADOW  ← in-RAM copies of flushed L0 SSTs (NO RocksDB analogue)
       + Tier-3 overlapping SSTs (block cache + KV-codec decode)
COMPACT maintenance ticker → run_compaction (db.rs:8852) → compact_l0_for_cf
         → CompactionJob::run (compaction.rs:102):
             gather ALL rows → Vec<CompactionEntry{key:Vec,value:Vec}>  (2 heap allocs/row)
             → sort_by(key,seq)
             → walk key-groups → emit_key_versions → StreamingSstWriter (Arrow builders → KV block)
         (+ FRS_COMPACT_DRAIN_L1 default-on: one bounded L1→L2 step when L1 over base)
CKPT   create_incremental_checkpoint_impl (noflush=false): seals memtable to L0 + references SSTs
```
forst-rs-specific structures absent in RocksDB: the **resident shadow** (Tier-2 in-RAM SST copy), the
**FFM-per-record** boundary, the **KV-codec re-encode** in compaction, and **single-threaded, unthrottled**
compaction (one `forst-rs-compact` thread under a global `compaction_mutex`).

## 5. Root cause — why Forst-rs cannot (yet) surpass RocksDB on q4 (data-backed, every lever tested)
Measured this session, same Mac, full-length A/Bs:

| observation | data |
|---|---|
| Forst-rs cold start is FASTER than RocksDB | 628K vs 590K rec/s @40s |
| …but Forst-rs DECAYS; RocksDB stays FLAT | forst-rs 600K→100K (troughs 29–43K); rdb flat ~400-445K |
| Decay is forst-rs-specific (not HW) | same Mac, same ckpt cfg; RocksDB finishes 98M/241s, forst-rs collapsed at ~65M pre-fix |
| Stability: bounded L1→L2 drain → q4 FINISHES | 98M/461s (first completion; was never finishing) |
| compaction merge is FAST isolated, SLOW live | microbench ~3 ns/byte at q4 scale; LIVE ~25 ns/byte = contention |
| "more concurrency for compaction" REFUTED | parallel sub-compaction = 564s / didn't-finish-600s — threads steal foreground cores |
| read-path CPU reduction = NOISE | bloom-skip −12% read-CPU → +2% (within trough variance) |
| memory-footprint reduction = NOISE | cache 4G→1G + shadow 2G→512M → +1% |

**The mechanism (3 compounding, forst-rs-specific causes):**
1. **Compaction ⊥ foreground core contention.** Forst-rs runs compaction unthrottled on a thread that
   competes with the IntervalJoin pipeline (78% engine-CPU) for the same cores. When a burst runs, the
   foreground starves → the trough. The merge is cheap *alone* (3 ns/byte) but expensive *live* (25). RocksDB
   rate-LIMITS compaction; adding concurrency to forst-rs compaction makes it WORSE (proven). The fix is
   *protecting the foreground* (throttle/yield compaction), not parallelizing it.
2. **Write-amplification.** L0→L1 re-merges a growing L1 (full-range memtable flushes overlap all L1). The
   default-on bounded L1→L2 drain stops the unbounded growth (so q4 finishes) but moves growth toward L2;
   forst-rs lacks RocksDB's fully-balanced, bounded-per-level leveled scheduling, so total bytes compacted
   (and thus total contending CPU) exceeds RocksDB's.
3. **Per-record constant factor + extra tiers.** FFM-per-record (vs JNI), the resident-shadow tier merge
   (RocksDB has none), and the KV decode→re-encode round-trip in compaction raise forst-rs's total CPU per
   record. Individually small (each read-side cut measured as noise) but they raise the *baseline*, leaving
   less headroom so contention bites sooner.

**Why no single change reached 2×:** the binder is the *sum* of foreground per-record CPU + compaction CPU
exceeding machine capacity under load. Read-side cuts (noise) don't touch the binder; compaction parallelism
worsens it; the one structural win (bounded drain) buys *completion* not speed. RocksDB stays flat because
its total CPU stays under capacity (low C++ constant factor + rate-limited compaction + incremental ckpt).

**The data-pointed path to actually beat RocksDB (ranked):**
- **(a) Throttle/yield compaction** (RocksDB-style rate limiter) so bursts never starve the foreground →
  flat curve. Highest-confidence flat-curve lever; opposite of the refuted parallelization.
- **(b) Lower the foreground per-record cost** via *batched* state ops — fewer FFM crossings + fewer engine
  ops per record. This needs the join to deliver records in batches = the Flink async/batched (V2) state
  path, which the interval-join operator does not use → **requires a Flink-runtime/operator change, out of
  the state-backend's scope** as currently bounded. This is the only lever that raises the *ceiling* (cold
  ~600K) toward 2×; without it, the best in-scope outcome is RocksDB *parity* (flat ~400K), not 2×.
- **(c) Remove the resident-shadow tier on local** (match RocksDB's memtable+blockcache read) + incremental
  checkpoint by SST-linking (match Forst) — shave the constant factor / contention baseline.

**Honest bound:** with the IntervalJoin fixed as a V1-sync per-record operator (out of backend scope), the
in-scope ceiling is ~RocksDB parity (flat, finishing), reached via (a)+(c). Surpassing RocksDB by 2× on q4
specifically requires batched state delivery (b), i.e. an operator/runtime change beyond the state backend —
OR a workload (heavy joins like q7/q9, where forst-rs already wins 22–35× on the co-located cloud box per
heritage) where the per-record sync constraint doesn't dominate.
