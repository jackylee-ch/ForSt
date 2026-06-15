# Write-side QoS — a flush-priority lane in the upload in-flight budget (Phase-2 cycle 8, Item B)

**Date:** 2026-06-15
**Status:** PMC design — root-cause mini-benched, io-crate mechanism implemented + unit-tested THIS cycle. Engine SET-point one-liner pending NEXMark unblock (Phase-2 NEXMark gated this cycle).
**Author:** PMC-2 (Phase-2 disaggregated state)
**Flag:** `FRS_UPLOAD_FLUSH_RESERVED` (count of reserved flush-lane permits; **default 0 = OFF**; byte-AND-behaviour-identical when OFF)
**Composes with:** `FRS_ASYNC_FLUSH_UPLOAD` (the synchronous-on-flush-thread permit
admission this prioritises), `FRS_UPLOAD_BYTE_BUDGET_MIB` (the byte-regime budget the
reserved lane is carved from).

---

## 0. The finding (one paragraph)

`opendal_backend.rs` bounds resident upload memory with ONE `upload_sem` — a
`tokio::sync::Semaphore` of `MAX_INFLIGHT_UPLOADS` permits (count regime, default) or
a byte budget (`FRS_UPLOAD_BYTE_BUDGET_MIB`). Every spawned upload — FLUSH output
(L0 SST) AND COMPACTION output (L1+ SST) — competes for that one budget with NO class
distinction. The two classes have opposite criticality: a **flush** upload's in-flight
permit, with `FRS_ASYNC_FLUSH_UPLOAD` ON, is acquired SYNCHRONOUSLY on the flush
thread in `close_writer` BEFORE the upload spawns — so a full budget BLOCKS the flush
loop, which stalls memtable rotation / WriteBufferManager reclaim → write stall →
ingest backpressure. A **compaction** upload is background/throughput — large outputs,
no operator waiting on any one. Under write pressure a burst of large compaction
uploads can occupy the WHOLE budget, so the next flush queues behind them: the
class-blind budget lets background compaction STARVE latency-critical flush.

---

## 1. Root-cause evidence (mini-bench, before)

`crates/forst-rs-bench/src/bin/upload_flush_qos.rs` models the in-flight budget as a
permit pool (COUNT regime — the production default), with a saturating compaction
stream (`budget` concurrent workers issuing 64-MiB SST uploads back-to-back) and a
flush stream (steady cadence of 4-MiB latency-critical uploads). The metric is the
flush ACQUIRE latency — the time the flush thread blocks for an in-flight permit, the
ingest-stall proxy. At a throttled endpoint (`FRS_REMOTE_BW_MBPS=256`,
`budget=8 = MAX_INFLIGHT_UPLOADS`):

| policy | flush p99 (ms) | flush max (ms) | compaction rate (MiB/ms) |
|--------|---------------:|---------------:|-------------------------:|
| **blind** (current single `upload_sem`) | **2779.5** | 5754.3 | 1.84 |
| reserved (1 of 8 slots flush-only) | **0.0** | 0.0 | 1.68 |

The class-blind budget makes a flush wait up to **2.78 s** (a whole compaction upload)
behind saturating compaction — directly a multi-second ingest stall. (At the 50 Gb/s
ceiling every upload is sub-ms so there is no contention to study — the starvation is a
THROTTLED-endpoint phenomenon, exactly the disagg regime this targets.)

---

## 2. The fix

Carve `reserved` permits out of the total budget into a flush-only lane (a second
semaphore). A flush-class `close_writer` first `try_acquire`s the reserved lane WITHOUT
blocking; if it gets the permits there, saturating compaction never delayed it.
Otherwise — and for every compaction-class upload — it falls back to blocking on the
shared lane, exactly as before. The TOTAL budget is unchanged (reserved is carved from
it, shared = total − reserved), so resident-upload memory stays bounded. Compaction
stays **work-conserving**: it uses the full shared lane (`total − reserved`), so its
rate is retained to ~`(total − reserved)/total`.

| policy | flush p99 | compaction rate retained |
|--------|----------:|-------------------------:|
| reserved (1/8) | 2779.5 ms → **0.0 ms** (−2.78 s) | **0.91×** |

A 1-of-8 reserved lane eliminates the flush stall while keeping 91 % of compaction
throughput — exactly the QoS trade the lever is for.

### 2.1 Class hint — thread-local, zero trait churn

The flush-vs-compaction class is known by the engine caller, not the `FileSystem`
trait. Rather than thread a class parameter through `open_writable_file` (≈ 10 trait
impls + every caller), the class is a **thread-local** (`THREAD_UPLOAD_CLASS`, default
`Compaction`). `close_writer` runs on the SAME worker thread that produced the SST
(the flush thread for a flush, the compaction thread for a compaction — the existing
code comments confirm `close_writer` is synchronous on that thread), so a thread-local
set at the top of the flush body classifies correctly. `ThreadUploadClassGuard::flush()`
is an exception-safe RAII guard the engine flush worker wraps its body in. Compaction
threads leave the default. **Any thread that has not declared itself a flush thread
uses the shared lane exactly as today** — so the mechanism is safe even before the
engine SET-point lands.

### 2.2 Byte-identity / OFF

`FRS_UPLOAD_FLUSH_RESERVED=0` (default) ⇒ the flush lane is built with 0 permits and
the shared lane is the full budget. The flush-class `try_acquire` on a 0-permit lane
ALWAYS fails, so every upload blocks on the shared (full-budget) lane — byte-AND-
behaviour-identical to the prior single-semaphore path. The reserved size also clamps
so the shared lane keeps ≥ 1 permit (compaction can never deadlock).

---

## 3. Implementation (this cycle — io crate, self-contained)

`crates/forst-rs-io/src/opendal_backend.rs`:

* `UPLOAD_FLUSH_RESERVED_ENV` + `upload_flush_reserved_permits()` (clamped to
  `budget − 1`) + `set_upload_flush_reserved_override` (test hook).
* `UploadClass` {Flush, Compaction} + `THREAD_UPLOAD_CLASS` thread-local +
  `thread_upload_class()` + `ThreadUploadClassGuard` (RAII set/restore).
* `build_upload_semaphores()` → `(shared, flush)` pair carved from the total budget;
  both `OpendalFileSystem` constructors use it; `OpendalWritableFile` carries the
  `flush_sem` clone.
* `close_writer`: flush-class uploads `try_acquire_many_owned` the reserved lane first,
  else block on the shared lane (the `FRS_ASYNC_FLUSH_UPLOAD` synchronous-admission
  path — the one that gates ingest). The OwnedSemaphorePermit type is lane-agnostic, so
  the held-for-the-whole-upload semantics are unchanged.

### 3.1 TDD

* `thread_upload_class_guard_sets_and_restores` — default Compaction; guard sets +
  restores, including nested.
* `reserved_flush_lane_sizing_and_clamp` — OFF ⇒ shared = full budget / flush empty;
  reserve 2 ⇒ carved from total (total unchanged); over-reserve clamps to keep shared ≥ 1.
* `reserved_flush_lane_lets_flush_bypass_saturated_shared` — THE QOS invariant: with the
  shared lane saturated, a FLUSH-class `close_writer` proceeds (reserved lane) while a
  COMPACTION-class one BLOCKS until the shared lane frees.
* `reserved_flush_lane_on_is_durable` — flush-class uploads via the reserved lane land
  byte-exact (no lost / dup / truncated SST) under the durability harness.
* Pre-existing `async_flush_upload_*` and `byte_budget_*` suites: GREEN (no regression —
  OFF is byte-identical, and the saturation test still acquires the full budget from the
  shared lane).

---

## 4. Engine SET-point (one line, deferred to NEXMark unblock)

The only remaining wiring is to wrap the engine's flush body in
`ThreadUploadClassGuard::flush()` so its `close_writer`s are classified as flush. That
is a single guard at the top of `DbImpl::run_flush` (`db.rs`). It is deferred to the
cycle that validates end-to-end on NEXMark (Phase-2 NEXMark is gated this cycle); the
io-crate mechanism is complete, correct, and byte-identical OFF without it, and the
mini-bench proves the win the SET-point unlocks.

---

## 5. Honest negatives + next item

* The mini-bench measures a permit-pool MODEL, not the live S3 path (mock-S3 only per
  the cycle constraints). It faithfully reproduces the `upload_sem` admission
  semantics, but the absolute p99 (2.78 s) is the modelled throttled-endpoint figure,
  not a measured NEXMark number — the end-to-end ingest-stall reduction awaits the
  engine SET-point + a NEXMark run.
* The reserved lane only helps the SYNCHRONOUS (`FRS_ASYNC_FLUSH_UPLOAD` ON) admission
  path — the path where flush actually blocks ingest. With the flag OFF the permit is
  acquired in-task (no flush-thread block), so there is nothing to prioritise; the
  reserved lane is inert there (documented).
* The class is per-THREAD, so it is correct only while `close_writer` runs on the
  producing worker (true today). If a future change defers `close_writer` to a generic
  pool thread, the class must travel with the writer instead (a `flush_sem`-vs-not
  decision captured at `open_writable_file` time) — noted for that refactor.

**Next-cycle disagg perf item:** async remote WRITE-path latency hiding symmetric to the
read-side depth-D — overlap the compaction-INPUT prefetch (downloading the next
compaction's input SSTs) with the current compaction's merge+upload, so a remote
compaction is not serially `download → merge → upload` per job. (Candidate; root-cause
to be mini-benched first.)
