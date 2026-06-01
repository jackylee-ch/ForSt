# Recheck: is S3 read/write still the bottleneck for forst-rs? — NO (evidence)

Date: 2026-06-01
Status: DIAGNOSTIC (profile-backed). Corrects the long-standing "S3 is the wall"
framing.

## Why v3.8 / ForsT (ckpt-OFF) is faster than forst-rs ckpt-ON

ckpt-OFF keeps keyed state in the RAM `VectorizedMemTable` (decoded, BTree-
indexed). Reads are vectorized RAM access — no SST reads, no decode. ckpt-ON
force-flushes that state to S3 SSTs, so reads pay decompress + Arrow-IPC decode
→ the collapse. The 2026-06-01 work (checkpoint-without-flush + cache-bypass +
large memtable) reproduces the resident-state fast path under ckpt-ON; q4 then
hit 554 K/s (faster than RocksDB) while state stayed resident.

## Is S3 read/write still the bottleneck? — NO, not the primary one

**S3 READ — no.**
- Resident state (large memtable) → ZERO SST reads; reads hit the RAM BTree. The
  q4 profile at 554 K/s showed the cost was the per-record `MapStateCache`, not
  I/O.
- Spilled state → the earlier read-amp + write-through + decoded-block-cache work
  already serves SST reads from LOCAL disk (`fetch_through_cache` /
  `RangeCachedRandomAccessFile` → local `pread`), not direct S3. The residual is
  **decompress + Arrow decode (CPU)**. The q9 spill *freeze* was
  `RwLock::lock_contended` (2110 samples); `opendal`/`tokio` S3-upload threads
  were ~18 samples → S3 was NOT the freeze.

**S3 WRITE — partial / bursty, not steady-state.** Flush is S3-primary but ASYNC
(buffered → multipart on `close()`, off the critical path). Only a LARGE
memtable flush on spill (big upload + WBM pressure) stalls ingest → the periodic
rate dips (e.g. q9 208K→60K→recover). So S3 write *throughput* contributes to
spill-flush bursts, but isn't the steady-state wall.

## The actual current bottlenecks (all CPU/concurrency, profiled)

1. Per-record `MapStateCache` key-compare → fixed by cache-bypass (q11 2.39×).
2. Memtable `RwLock` contention (merge-under-write-lock on scan) → read-lock fix.
3. SST Arrow-decode on spill (CPU) — only for state exceeding RAM.
4. Snapshot/flush materializing the whole memtable as Arrow (~3× mem) → OOM at
   4 GB; streaming snapshot is the fix.
5. Intrinsic Flink join operator + `RowDataSerializer` per-record (immutable).

## Implication for strategy

The local-cache / read-amplification work already neutralized S3 read latency —
which is why more S3 tuning stopped helping and why batching the per-row CPU
(the user's directive) produced the wins. The remaining levers are: (a) keep
state RESIDENT (avoid spill — needs streaming snapshot so a larger memtable
doesn't OOM), and (b) batch/cheapen per-record CPU. NOT faster S3.
