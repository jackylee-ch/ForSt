# Concurrent k-way scan cold-start prime — design (Phase-2 cycle 1)

**Date:** 2026-06-13
**Status:** PMC design — evidence gathered, implementation proposed (NOT yet built)
**Author:** PMC-2 (Phase-2 disaggregated state)
**Flag:** `FRS_SCAN_COLD_PRIME` (default-OFF; byte-identical when OFF)

---

## 0. The finding (one paragraph)

The single highest-leverage not-yet-ported disaggregated read optimization is
**concurrent cold-start of a multi-source scan's first blocks**. ForSt hides
remote cache-miss latency on the heavy-I/O tail (the paper's L1×L4 term,
PVLDB 18(12):4856-4857) with depth-3 *parallel* read threads
(`ForStStateExecutor.java:59-66`). forst-rs already HAS the parallel-read
machinery — the read-I/O pool, `clamp(cores/2,2,6)` workers
(`crates/forst-rs-storage/src/sst/prefetch.rs:228-288`) — but the k-way merge
that powers every prefix/range scan **seeds its K sources sequentially** at
cold-start, so the K independent first-block remote GETs are issued
back-to-back and the scan pays **K × remote-RTT serially** before it can emit a
single row. The fix is to prime all SST sources' first blocks concurrently
through the existing pool, exactly as `BlockPrefetcher::for_compaction` already
primes its first window at construction (`prefetch.rs:401-419`).

---

## 1. Evidence — the bottleneck is structural and confirmed absent

### 1.1 The serial seed (code-grounded)

`DbImpl::build_lazy_prefix_key_stream` / `build_lazy_range_key_stream`
(`db.rs:9588`, `:10035`) build a `Vec<TierKeySource>` and wrap it in a
`LazyPrefixIter`. Each overlapping SST becomes a `TierKeySource::Sst` holding a
`BlockPrefetcher::new(...)` (`db.rs:9929-9942`, `:10120`). `BlockPrefetcher::new`
does **not** submit a window — the cold cursor is demand-paged: the first
window is only submitted on the first `next_decoded()` call
(`prefetch.rs:371-387`, contrast `for_compaction` `:401-419` which DOES prime).

The merge's first step seeds every source's head in a **sequential `for`
loop**:
- tree path: `db.rs:15970-15972`
  ```
  for src in sources.iter_mut() {
      ensure_head_pinned(src, le, shared_error_slot, last_error, &mut comparisons);
  }
  *tree = Some(TournamentTree::build(sources, &mut comparisons));
  ```
- linear path: `db.rs:16111-16113` (identical loop)
- value path (`next_with_value`) seeds heads via the same per-source `peek`.

`ensure_head_pinned` → `TierKeySource::peek` → `fetcher.next_decoded()`
(`db.rs:15223`) → first `maybe_submit_window` + **synchronous wait** for the
block. With K SST sources over remote (cache-miss) state, source `i`'s GET
cannot start until source `i-1`'s `next_decoded()` returns ⇒ **K serial
round-trips at scan cold-start**.

### 1.2 Why this is the dominant disagg term for the join/OVER classes

The competitive analysis ranks cache-miss remote reads on the heavy-I/O tail
(L1×L4) as the #1 of ForSt's 0.15× loss
(`2026-06-13-disagg-competitive-analysis.md:53-59,320-324`). q9/q19/q20 are
*iterator-dominated* (`MEMORY.md`: "lever is C2/C3 parallel iterators";
`2026-06-08-...parallel-coalesced-readpath-design.md`) and issue **many small
prefix probes**, each a fresh merge cold-start over the overlapping L0/L1 SSTs.
`FRS-FANOUT-DIAG` (`db.rs:9959-9971`) recorded SST-source fan-out peaks in the
tens on q7 — exactly the high-K regime where the serial seed is most costly.
`prefetch_sst_files_for_batch` (`db.rs:12025-12101`) ALREADY fans out the
point-`batch_get` whole-file warm concurrently (`prefetch_concurrent`,
`cached_fs.rs:228-256`, `MAX_CONCURRENT_FETCH=8`) — the **scan/iterator path
has no equivalent**. That asymmetry is the gap.

### 1.3 Confirmed NOT already done

- `BlockPrefetcher::new` does not prime (`prefetch.rs:371-387`); only
  `for_compaction` does (`:401-419`).
- The merge seeds serially (§1.1); no `prime_all` / concurrent warm exists on
  the `LazyPrefixIter` build or first-step paths (grepped: no `prime`,
  `warm_first`, `prime_all` in `db.rs`).
- The read-I/O pool exists and is the right vehicle but is currently only
  driven from *within* a single `BlockPrefetcher`'s readahead, never to prime
  *across* sources.

### 1.4 Micro-benchmark — projected benefit

`crates/forst-rs-bench/src/bin/scan_cold_start.rs` (this cycle). Models the
cold-start latency term ONLY: K first-block GETs, serial (current merge
head-seed) vs concurrent through a `min(K, pool)`-wide pool (proposed prime),
per-GET RTT = `FRS_MODEL_RTT_MS`. Pool = `clamp(cores/2,2,6)` = 6 here.

**RTT = 23 ms (recorded dev-Mac→BOS, `s3bw.rs`):**

| fanout K | serial (ms) | concurrent (ms) | speedup |
|---|---|---|---|
| 2  | 49.1  | 25.0  | 1.97× |
| 4  | 98.8  | 25.9  | 3.82× |
| 8  | 202.1 | 50.0  | 4.04× |
| 16 | 400.8 | 76.1  | 5.27× |
| 32 | 800.0 | 149.3 | 5.36× |

**RTT = 2 ms (≥50 Gb/s intra-DC online box, `FRS_MODEL_RTT_MS=2`):**

| fanout K | serial (ms) | concurrent (ms) | speedup |
|---|---|---|---|
| 2  | 5.0  | 2.2  | 2.31× |
| 4  | 9.6  | 2.6  | 3.73× |
| 8  | 19.9 | 5.0  | 3.94× |
| 16 | 39.5 | 7.5  | 5.29× |
| 32 | 77.8 | 14.8 | 5.25× |

**Reading.** The cold-start latency drops ~2–5.4×, bounded at ~min(K, pool).
The win is per *fresh cold scan* and compounds across the thousands of small
prefix probes a join/OVER query issues against remote state. At the online box
(K=16, RTT=2 ms) it removes ~32 ms from every fresh cold scan cold-start. It is
**zero** on warm/cached scans (no GET) and on K=1 scans — hence the must-be-OFF
and must-no-op-when-warm constraints below.

**Scope/honesty:** this models the cold-start latency tail, NOT steady-state
throughput (where the per-source readahead already overlaps) and NOT warm hits.
It is the cold-scan-startup term — which is exactly the L1×L4 tail the paper
names. The end-to-end NexMark win requires the online-box S3 A/B (user-gated);
this is the local design→micro-bench evidence stage per standing rules.

---

## 2. Proposed implementation (flag-gated, default-OFF, byte-identical when OFF)

### 2.1 Mechanism

Add a `prime_cold_sources_concurrent` step invoked **once**, right after the
`Vec<TierKeySource>` is built and before `LazyPrefixIter::new_clipped`
(`db.rs:10024` and the range sister `:10120`-ish). It:

1. Walks the sources; for each `TierKeySource::Sst` whose `BlockPrefetcher`
   reader `is_remote` (NOT `is_local_file()`) AND whose first block is **not
   already cache-resident**, captures a cheap closure that calls
   `fetcher.prime_first_window()` (a new `BlockPrefetcher` method that submits
   the first window WITHOUT consuming — i.e. the construction-time half of
   `for_compaction`, but ramp/regime-preserving for the scan path).
2. Submits those closures to the existing read-I/O pool
   (`prefetch::read_io_pool()`), bounded fan-out, and **joins** (a barrier) so
   that by the time the merge's first `ensure_head_pinned` runs, every primed
   source's first window is already in flight or ready — the per-source
   `next_decoded()` then claims a ready/in-flight handle instead of issuing a
   cold synchronous GET.

Because step 2 only PRIMES (submits + makes the handle in-flight) and the merge
still consumes through the existing `next_decoded()` state machine, the
**emitted bytes and merge order are unchanged** — only the *timing* of the K
GETs changes. When the flag is OFF, the prime step is skipped entirely and the
merge cold-starts exactly as today (byte-identical, instruction-identical on the
hot path).

### 2.2 No-op guards (correctness + zero-cost-when-warm)

- **Flag OFF** (`FRS_SCAN_COLD_PRIME` unset) ⇒ prime step not called.
- **K = 1** (single SST source, or zero) ⇒ nothing to overlap, skip.
- **Local regime** (`is_local_file()` true) ⇒ preads are µs-class, RTT≈0, skip
  (mirrors the prefetcher's local-vs-remote regime split, `prefetch.rs:373`).
- **First block already cache-resident** ⇒ no GET would be issued; skip so a
  warm scan submits zero pool jobs (the bench's "zero on warm" property).
- **Memtable-only sources** ⇒ no I/O; skip.

### 2.3 Cancellation / safety

A primed-but-unconsumed window is already safe: `BlockPrefetcher::terminate`
(`prefetch.rs:450-459`) and the `PrefetchHandle` oneshot-drop discard semantics
(`prefetch.rs:328-338`) release the M3 telemetry charge and discard the decoded
window if the scan is aborted before reaching that source. The prime adds no new
lifetime hazard — it only moves the *submit* earlier. The pool workers already
survive job panics (`catch_unwind`, `prefetch.rs:259`).

The memory budget is bounded: priming submits at most one window per remote
SST source, and the existing M3 `PREFETCH_BUFFERED_BYTES` accounting
(`prefetch.rs:188-198`) already tracks ready+inflight bytes; priming K sources
at once raises the transient ceiling to K × (one remote window = 4 MiB cap,
`prefetch.rs:84`) — same order as the merge would buffer anyway as it walks the
sources, just front-loaded. A `FRS_SCAN_COLD_PRIME_MAX` cap (default = pool
size) bounds the concurrent submit wave for very high K.

### 2.4 TDD regression test (engine)

`crates/forst-rs-engine/src/db.rs` tests (or a new `tests/` IT):
1. **Byte-identical when OFF/ON**: build a CF with multiple overlapping SSTs +
   memtable, run a prefix scan with the flag OFF and ON, assert the emitted
   (key, value) sequence is identical (the prime changes timing only).
2. **No-op when warm**: with all SSTs cache-resident, assert the prime submits
   zero pool jobs (a counter hook) — proves zero overhead on the warm path.
3. **No-op when K=1 / local / memtable-only**: assert skip.
4. **Cancellation**: open a scan over remote sources with the flag ON, drop it
   after the first row, assert `PREFETCH_BUFFERED_BYTES` returns to baseline
   (no leaked primed windows).

### 2.5 Mini-benchmark proving the win

Extend `scan_cold_start.rs` into an engine-driven arm once built: a CF loaded
to span K overlapping SSTs behind a `LatencyFileSystem` wrapper (injects
`FRS_MODEL_RTT_MS` per `open_random_access_file`/`read_at` first touch), measure
the wall of the first row of a fresh cold prefix scan, flag OFF vs ON. Target:
reproduce the ≥2× cold-start reduction at K≥4 from §1.4 on the real merge path.

---

## 3. Why this over the other candidates

| Candidate | Status | Verdict |
|---|---|---|
| **Concurrent scan cold-start prime** (this doc) | confirmed absent (§1.3); read-path; join/OVER tail; 2–5.4× cold-start (§1.4); machinery already exists | **CHOSEN** — highest leverage on the binding read class, low risk (timing-only), reuses the pool |
| Requester-class cache exemption | ALREADY SHIPPED (`local_cache.rs:95-98,503`) | done |
| Write-only admission + promoteLimit + count-to-promote | ALREADY SHIPPED (`local_cache.rs:60-98,180-197`) | done |
| Restore cache pre-seed | ALREADY SHIPPED (`cached_fs.rs:721-723`, `db.rs:7993` etc.) | done |
| Epoch-decayed cold-list counts | FIFO stand-in present; analysis says cheaper, not a gap | skip |
| Persistent-pool whole-file prefetch (vs per-wave `thread::scope`) | minor (`cached_fs.rs:248-256` spawns per wave) | low leverage; backlog |
| Remote compaction | ALREADY SHIPPED (CompactionExecutor trait) | done |

---

## 4. Implementation plan (next cycle, after this report)

1. `BlockPrefetcher::prime_first_window(&mut self)` — submit-only (no consume),
   regime-preserving, idempotent, no-op if already primed/ready/local. + unit
   tests (submit count, idempotence, local no-op).
2. `LazyPrefixIter` / build-site `prime_cold_sources_concurrent` with the §2.2
   guards + the `FRS_SCAN_COLD_PRIME` gate + submit-count test hook.
3. Engine regression tests §2.4.
4. Engine-driven bench arm §2.5 (LatencyFileSystem).
5. `cargo fmt --all --check` + `cargo clippy --all-targets` (engine+bench+ffi) +
   touched tests; push `HEAD:forst-rs`.

**Constraints honored:** vectorized/batch/zero-copy (prime touches whole
windows, no per-key/byte work; no `byte[]`/copy added); config unchanged
(noflush=false, 1 G write buffer); flag default-OFF byte-identical.

---

## 5. CYCLE-2 IMPLEMENTATION — LANDED (2026-06-13)

Implemented exactly as specced; flag `FRS_SCAN_COLD_PRIME` default-OFF,
byte-identical when OFF.

- **`BlockPrefetcher::prime_first_window()` + `wants_priming()`**
  (`prefetch.rs`): submit-only (enqueues the 1-block `[next_block,next_block+1)`
  window the cold demand path would read, at the same `Insert(Low)` priority),
  regime-preserving (ramp still entered by `next_decoded` at `consumed>=2`),
  idempotent (no double-submit — `wants_priming` returns false once in-flight).
  No-op guards: disabled / compaction / local / mid-stream / EOF / first-block
  cache-resident. 6 new unit tests (byte-identity OFF-vs-ON across formats,
  idempotence, each skip-guard, warm no-op, terminate releases the charge).
- **`prime_cold_sources_concurrent()`** (`db.rs`, on `LazyPrefixIter`): runs
  ONCE (guarded by `cold_prime_done`) at the top of `next_step_pinned`
  (pinned tree + linear), `next_inner` (legacy key), and
  `next_with_value_inner` (value-carrying) — i.e. before every merge path's
  first head-seed. Submits all REMOTE cold SST sources' first windows to the
  read-I/O pool, then the merge's per-source `peek`→`next_decoded` claims the
  in-flight handle (the barrier is the existing in-flight-claim await). Guards:
  flag-OFF / K≤1 / per-source guards. `cold_primed_sources()` test hook counts
  actual submits (the "zero jobs when warm" gate).
- **Cancellation**: reuses `BlockPrefetcher::terminate` + oneshot-drop; no new
  lifetime. Unit + engine drop tests confirm `prefetch_buffered_bytes()`
  returns to baseline after a mid-scan drop (no leaked primed windows).

### Results

- **OFF-vs-ON byte-identity (THE gate): PASS.** Engine ITs assert the emitted
  (key,value) sequence with the prime forced ON is byte-identical to OFF AND to
  the `prefix_scan` oracle, on BOTH the pinned and legacy merge paths, across
  the multi-tier fixture (overlapping L1+L0 SSTs + memtable) — non-KV-sep AND
  KV-separation-ON (vlog BlobRef deref). Plus the prefetcher byte-identity unit
  test across v1/v2 block formats × None/LZ4.
- **Engine-driven bench arm** (`scan_cold_start.rs`, real `DbImpl` prefix scan
  over K overlapping SSTs behind a `LatencyFileSystem`, remote regime, full
  cold-drain wall, OFF vs ON):

  | SSTs K | OFF (ms) | ON (ms) | speedup | (RTT) |
  |---|---|---|---|---|
  | 2 | 37.5 | 22.6 | 1.66× | 15 ms |
  | 4 | 74.7 | 23.1 | 3.24× | 15 ms |
  | 8 | 154.6 | 40.7 | 3.79× | 15 ms |
  | 4 | 27.7 | 7.9 | 3.51× | 5 ms (smoke) |
  | 8 | 58.6 | 14.8 | 3.95× | 5 ms (smoke) |

  The real merge path reproduces the §1.4 model: ~1.7–4× cold-start reduction,
  bounded at ~min(K, pool=6). Zero pool jobs on the local/warm path (asserted).
- **Suites**: storage lib 461/0, engine lib 382/0 (+ 2 explicit KV-sep gates),
  prefetcher 22/0; `cargo fmt --all --check` + `clippy --all-targets`
  (storage+engine+bench+ffi) clean. Additive only — no FFI ABI change.

The end-to-end NexMark win still requires the online-box S3 A/B (user-gated),
per standing rules; this is the in-repo design→implementation→evidence stage.
