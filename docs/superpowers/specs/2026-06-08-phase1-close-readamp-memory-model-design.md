# Phase-1 Close: read-amp + 8c/32g memory model so forst-rs beats RocksDB AND ForSt per-query

**Date:** 2026-06-08
**Goal:** Close the Phase-1 gate — on 8c/32g Docker, forst-rs runs ALL q0–q22 with accuracy,
beats RocksDB overall, and per-query is ≥0.8× RocksDB OR ≤+50s AND faster than ForSt-Java.
**Status of inputs:** grounded in measured 8c/32g + host data captured 2026-06-08 (see
`memory/project_join_oom_native_unbounded_2026-06-08.md`).

---

## 1. What is already DONE this session (keep, verify, commit when asked)

1. **Java FFM iter-leak fix** — `flink-statebackend-forst-rs/.../ForStRsDBIterRequest.java`:
   `executeIters` passed the long-lived executor arena to `process()`; `parseChunkInto`
   leaked per-chunk view snapshots forever (~9 GB+ off-heap, uncounted by Flink). Routed to
   the per-call confined `scratch` arena. 17 UTs green. q9: 6M-DNF → 60M on 8c/32g.
2. **fadvise(DONTNEED)** on written local SSTs — `forst-rs-io/src/opendal_backend.rs` (nix,
   `#![forbid(unsafe_code)]`-safe). Page cache 8→3.5 GB. 209 io tests green.
3. **8c/32g engine config budget** — `scripts/templates-linux/config-forst-rs-local.yaml.tpl`:
   writebuffer 512mb / WBM 3072mb / compaction 6 / flush 4 / block cache 1024mb / in-flight 20000.
4. **Harness fixes** — disk watchdog floor 388→360 GiB; container `/tmp` bind-mounted to host
   volume (was a 59 GB Docker.raw overlay → "No space" crash mis-read as OOM).

## 2. Measured root cause of the remaining gap (8c/32g, q9)

At ~60 M events q9 dies; jemalloc-ctl + RSS + docker-stats attribution:
- **anon RSS 30.3 GB / cgroup 31.94 GiB (99.8%)** → transient memory OOM.
- Split: Java ~13 GB (heap + bounded FFM + AEC in-flight) + engine ~11 GB (memtables ≤3 GB +
  block cache 1 GB + **~7 GB compaction/flush transient**) + page cache ~3.5 GB.
- **Throughput decays 53K→7K/s** = compaction debt: small (512 MB) memtables flood L0 faster
  than 6 compaction threads clear it → read-amp on every join probe grows → rate collapses.

**The frontier:** SST size == memtable size. Big memtables → few L0 (fast) but huge compaction
transient + page cache (OOM). Small memtables → low mem but L0-stop stall (slow). Config alone
cannot win both. Same mechanism gates q9/q11/q19/q20/q4 vs both RocksDB and ForSt.

## 3. The fix — four engine modules (each independently testable + verifiable)

### Module A — Compaction output already splits; bound compaction INPUT/transient memory
**Problem:** the ~7 GB engine transient is concurrent compactions each buffering large inputs.
**Fix:** cap per-compaction working memory: stream compaction inputs block-by-block (k-way merge
over block iterators) instead of materializing whole input SSTs; cap total in-flight compaction
bytes by a budget (e.g. 1.5 GB) so concurrency self-limits under pressure rather than by thread
count. **Verify:** unit test asserting peak transient under a synthetic compaction; 8c/32g q9
mem-diag shows engine transient < 3 GB.

### Module B — Decouple SST size from memtable size (target_file_size)
**Problem:** 512 MB memtables → 512 MB L0 SSTs → either too few (mem) or too many (read-amp).
**Fix:** flush + compaction emit fixed ~64 MB SSTs (RocksDB `target_file_size_base`) regardless
of memtable size. Then use LARGE memtables (1024 MB, few flushes, low write-amp) WITHOUT large
SSTs (bounded compaction units, bounded read-amp). **Verify:** L0 file count stays bounded; q9
throughput stops decaying (rate flat, not 53K→7K); byte-identical output (existing v1/v2 suite).

### Module C — Effective write-stall memory model (RocksDB allow_stall)
**Problem:** `db.rs:2664` 30 s backstop lets the source burst past the WBM cap → spikes.
**Fix:** block writers while over the global budget with NO premature proceed (flush runs on the
independent bg pool → no deadlock); this backpressures the AEC → source. Budget derived from a
single "engine RAM" number sized to fit 8c/32g alongside heap. **Verify:** q9 burst anon never
exceeds budget; no cgroup OOM across the full 100 M run.

### Module D — Top-N / interval-join read-path CPU (q19, q9)
**Problem:** q19 Top-N rank + q9 interval-join do per-record scans whose CPU grows with state.
**Fix:** extend the value-carrying range-scan (task #51/#54) to the Top-N and interval-join
access patterns so a rank/probe is one vectorized batch read, not O(rows×tiers) point-gets.
**Verify:** q19/q9 rate stays flat at scale; counts byte-identical to RocksDB.

### Separate investigation — q7 (forst-rs 508 vs ForSt 263)
forst-rs beats RocksDB 2.4× on q7 but ForSt-Java is 2× faster. Never analyzed. Profile q7 on
8c/32g (interval-join + timer firing) to find ForSt's structural advantage before designing a fix.

## 4. Sequencing (each: implement → UT → 8c/32g e2e exact-count + perf → document)
1. Module C (smallest, unblocks "q9/q20/q4 FINISH on 8c/32g" — the DNF gate). 
2. Module B (decouple SST size — unblocks throughput without memory regression).
3. Module A (bound compaction transient — margin).
4. Re-sweep q4/q9/q11/q19/q20 on 8c/32g vs RocksDB + ForSt.
5. Module D for residual q19/q9 CPU; q7 investigation.
6. Full q0–q22 3-backend 8c/32g sweep → confirm per-query bar → then Phase 2.

## 5. Constraints (standing)
Zero-copy / Arrow / batch only, no byte[]/per-record paths. Test each backend with its own
timer on 8c/32g Docker. No interactive verification. Commit only when asked. Keep ≥400 GB disk
free after tests. Revert diagnostic instrumentation (mem-diag, jemalloc-ctl, RSS sampler) before commit.
