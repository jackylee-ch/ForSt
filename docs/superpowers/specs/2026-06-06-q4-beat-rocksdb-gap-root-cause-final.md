# q4 beat-RocksDB-on-local: definitive gap root-cause (2026-06-06)

After finishing the memory-model spec (C1+C2+C3), the value-carrying merge, and the
jemalloc-macOS-crash fix, q4 on the host (local dir) is **522s** vs RocksDB **243s** =
**2.15× slower**. This documents — with measurements — exactly why, so the remaining
work is scoped, not guessed.

## What is NOT the cause (ruled out with data)

- **Reads / prefix-scan** — was the dominant CPU; the value-carrying merge collapsed it
  (profile: prefix-scan frames 4700→<73). Read path is now RocksDB-parity.
- **Memory / swap** — C1+C2+C3 fit q4 in 8c/32g (peak 24.7GB, no OOM). Not swapping on host.
- **Checkpoint *cost* being the whole story** — disabling checkpoints made it 2.7× SLOWER
  (checkpoint-flush is load-bearing; bounds memtables).
- **A single hotspot** — the steady-state CPU profile is DIFFUSE (top frame ~88 of a large
  multi-thread sample). There is no 50%-function to optimize.

## The two structural gaps vs RocksDB (measured)

q4 522s ≈ **~144s checkpoint + ~378s steady-state**.

### 1. No WAL → expensive checkpoints (~144s, 26%)
`noflush=false` forces a full memtable flush (+ resulting compaction) at every 30s
checkpoint — measured ~8s marginal per checkpoint (15s-interval A/B: +145s for ~18 extra
checkpoints). RocksDB has a **write-ahead log**: a checkpoint just fsyncs the WAL and
references existing SSTs — sub-second. forst-rs has no WAL, so the memtable can only be
made durable by flushing it. **Fix = add a WAL** so checkpoints stop forcing flush+compaction.

### 2. Per-op async dispatch overhead (~378s steady-state vs RocksDB's 243s)
Steady-state ~188K/s vs RocksDB ~403K/s, with CPU cores NOT saturated (18-core host) →
coordination/backpressure-bound, not compute-bound. The profile shows the cost spread
across the async machinery:
- tokio task spawn/poll/park churn (~200 samples) — a task per async op.
- **67 `forst-rs-opendal` threads** (per-DbImpl runtimes × ~12 instances) + opendal
  indirection on every SST read/write, even on local dir.
- C2's BTreeMap point-get (~200 samples) — the O(log n) cost traded for memory (wall-neutral
  on the 18-core host; matters more on 8 cores).
RocksDB uses a tight native synchronous path. **Fixes (in leverage order):**
(a) one shared tokio/opendal runtime per slot instead of per-DbImpl (extends C3/Component A's
shared-resource model — fewer threads, less scheduling churn);
(b) a synchronous local-FS fast path that bypasses opendal+tokio for local-dir SST I/O
(keep opendal behind the FileSystem trait for the S3/disagg Phase-2 seam);
(c) deeper request batching so fewer async tasks are spawned per record batch.

## Honest conclusion
Beating RocksDB on q4 local is **not reachable by tuning or incremental fixes** — it needs
the WAL (gap 1) and the async-dispatch/opendal reduction (gap 2). Each is a substantial,
correctness-sensitive subsystem (WAL recovery ordering; shared-runtime lifecycle), i.e.
multi-session work. Everything cheaper has been done and measured. The read path is
RocksDB-parity, memory fits 8c/32g, and q4 finishes reliably at 522s — a solid, correct
baseline to build the two structural levers on.
