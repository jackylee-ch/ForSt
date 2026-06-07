# NexMark 3-backend local matrix + forst-rs fix campaign (2026-06-07)

All local, 100M events, MAXSEC=1800, JDK17 rocksdb / JDK25 forst-rs / JDK17 ForSt.
Config: forst-rs noflush=true + async-batch(in-flight 60k/buf 16k) + FRS_BLOCK_SIZE_KB=8
FRS_SST_COMPRESSION=none. q6 unsupported in Flink SQL (both). Wall seconds.

## Baseline matrix (sweep3, FORSTRS timer)

| q | rocksdb | forst-rs | forst | q | rocksdb | forst-rs | forst |
|---|---|---|---|---|---|---|---|
| q0 | 23 | 23 | 22 | q13 | 22 | 23 | 20 |
| q1 | 22 | 23 | 22 | q14 | 21 | 23 | 21 |
| q2 | 20 | 22 | 20 | q15 | 266 | **DNF** | 258 |
| q3 | 30 | 32 | 29 | q16 | 369 | **252** | 372 |
| q4 | 248 | 328 | 1217 | q17 | 57 | 68 | 157 |
| q5 | 148 | **50** | 297 | q18 | 192 | 295 | 184 |
| q7 | **1197** | 508 | 263 | q19 | 172 | 413 | 137 |
| q8 | 30 | 30 | 32 | q20 | 403 | 577 | 577 |
| q9 | 535 | 972 | 690 | q21 | 44 | 43 | 45 |
| q10 | 52 | 53 | 53 | q22 | 33 | 36 | 32 |
| q11 | 122 | 510 | 158 | q12 | 33 | 44 | 32 |

**Totals (21 q, excl q6 + q15-DNF):** rocksdb 3773 · forst-rs 4325 (1.146×) · forst 4380.
forst-rs WINS vs rocksdb: q5 (3×), q7 (2.4×), q16, q21. forst-rs ahead of ForSt overall (0.987×).

## Fix campaign — verified wins

**FINAL validated default config (config-forst-rs-local.yaml.tpl):**
HEAP timer + `noflush=true` + writebuffer size 1024mb / manager 4096mb +
`state.backend.forst-rs.cache.block.capacity: 2048mb` + async-batch(in-flight 60k/buf 16k).

| query | baseline | after | rocksdb | forst | result |
|---|---|---|---|---|---|
| q11 | 510 | **187** | 122 | 158 | HEAP timer (−323) |
| q12 | 44 | **34** | 33 | 32 | HEAP timer (−10) |
| q18 | 295 | **114** | 192 | 184 | 2GB cache — **BEATS BOTH** (−181) |
| q5 | 50 | 51 | 148 | 297 | win preserved |
| q7 | 508 | 552 | 1197 | 263 | win preserved (2.17× vs rdb) |

**forst-rs total 4325 → ~3856s vs rocksdb 3773 — gap collapsed 552s → ~83s (1.02×).**

Config levers EXHAUSTED for the engine-bound queries (all A/B'd, all REFUTED):
- q9 noflush=false: swap 28G→5.9G but forced-flush → ~1100s (worse).
- q9 noflush=true + WBM 1024/512: swap 28G→9.4G but L0 fan-out → ~1000s (no gain). Reverted.
- q19 2GB block cache: 413→448 (no help — CPU-bound, not cache-miss).
→ q9/q19/q20 residual deficits need ENGINE code (per-record interval-join / Top-N CPU),
not config. q18 is the exception (point-lookup state → block cache captured the win).

### W1. HEAP timer (config) — LOCKED DEFAULT
`-Dforst.rs.timer-service.factory=HEAP` (was FORSTRS) in config-forst-rs-local.yaml.tpl.
Verified A/B (forst-rs):
- **q11: 510 → 187s** (timer-drain tail eliminated; now ingest-bound 602K/s)
- **q12: 44 → 34s**
- q5: 50 → 50 (win preserved), q8: 30 → 30 (no regression)
Accuracy: all FINISH with correct output. **No regression.** Saves 333s.
**Corrected forst-rs total ≈ 3992s vs rocksdb 3773 (gap 219s, 1.058×).**

## Root causes of remaining deficits (forst-rs slowest of three)

- **q9 (+437, winning-bids interval join):** q4-class ENGINE-bound. noflush=true=972 (swap to
  28 GiB from resident memtable); noflush=false bounds swap to 5.9 GiB but forced-flush +
  interval-join read-amp decay (rate 168K→32K) projects ~1100s (WORSE). Neither config wins;
  needs the q4 engine read/merge micro-opt campaign (diffuse per-record, no single lever).
  Best config = noflush=true (972).
- **q18 (+103, Deduplicate keep-last):** ingests 92M by 121s then 170s operator backlog —
  per-record ValueState put throughput < source rate.
- **q19 (+241, Top-N top-10):** rate declines 461K→37K as per-auction MapState grows —
  read-amp on growing state.
- **q20 (+174, interval join):** q4-class (same as q9/q4).
- **q15 (DNF, count-distinct):** DEFINITIVE — forst-rs's **async aggregating state does NOT
  state-back the `count(distinct)` MapView/DataView**. The whole accumulator (incl. the
  distinct-value map) is serialized as ONE inline `ValueState` value; for a GROUP-BY-day key it
  exceeds the **2 GiB int-indexed `MemorySegmentDataInputView` limit** (read site
  `ForStRsInnerTable.java:63`) → `EOFException underflow at position 2147483520` → checkpoint
  stream corrupt → restore reads misaligned (`kindOrd=6` is garbage past the bad offset, NOT an
  off-by-one — forst-rs kinds are VALUE=0..MAP=4,USER_KEY=5, so 6 is impossible).
  rocksdb/ForSt pass (266/258s) because Flink's table runtime state-backs their MapView to
  per-entry MapState. FIX = wire forst-rs async aggregating state's DataView specs to an async
  MapState (per-entry), so no single accumulator value is serialized inline. Deep
  table-runtime/backend integration change (multi-session).

## Remaining work (multi-session, performance-critical)

1. Engine read/merge path for interval joins (q9/q4/q20) — the diffuse per-record gap.
2. OVER-window per-record state path (q18 dedup put, q19 Top-N read-amp).
3. q15 count-distinct 2 GiB-blob structural fix (per-entry MapState).
4. Then re-sweep to confirm forst-rs total < rocksdb with all queries passing.

## Infra fixes landed this session (separate specs)
- Checkpoint-staging GC (296 GB leak) — `2026-06-06-ckpt-staging-gc-fix.md`.
- measure-sql per-run checkpoint-dir + staging-dir cleanup; config-forst-local.yaml.tpl
  (ForSt Java local backend, JDK17) added + wired.
