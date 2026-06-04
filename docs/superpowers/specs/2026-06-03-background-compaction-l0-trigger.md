# forst-rs: background compaction worker + low L0 trigger (heavy-state read-amp decay fix)

**Date:** 2026-06-03
**Component:** `crates/forst-rs-engine/src/flush.rs` (new compaction worker) +
`db.rs` (wiring) + `write_controller.rs` (`l0_compaction_trigger`).

---

## 1. Problem — throughput DECAY on heavy-state queries (q4, q11)

After the timer-O(N²) and merge-chain-O(N²) fixes, NexMark q4/q11 still failed — but not
as hangs: they **decayed**. q11 fell 665K → 37K rec/s as state grew and never finished;
RocksDB q11 holds a steady ~800K rec/s. A symbolized native profile (early-t120 vs
deep-t310 samples) placed the dominant cost in the engine **point-read path**
`DbImpl::get_arc → get → get_internal → sst_get` (deep sample: `sst` frames = 620, the
runaway). So each point read scanned a **growing L0**, and per-read cost climbed with the
L0 file count.

Two coupled root causes:

1. **Compaction ran INLINE on the flush worker.** `FlushExecutor::run_flush` did
   `flush_cf_data(cf)` *then* `maybe_auto_compact(cf)` on the **same single worker
   thread**. A large L0→L1 compaction blocked subsequent flushes → memtables backed up
   (write-stall) and L0 stayed deep.
2. **The L0 compaction trigger was 40.** `maybe_auto_compact` used
   `WriteControllerConfig::l0_slowdown_trigger = 40`, so L0 grew to ~40 SSTs before
   compacting → every point read did ~40 per-SST bloom/index probes → read-amp → decay.

A 2026-05-30 attempt at trigger=4 *worsened* it — because compaction was inline, a low
trigger meant frequent inline compactions that stalled the flush worker even earlier.

## 2. Fix — two changes that only work together

### (a) Background compaction worker (decouple from flush)
Mirroring the existing flush-worker infrastructure, `flush.rs` gains `CompactionRequest`,
`CompactionQueue`, `CompactionExecutor`, and `compaction_loop` (a `forst-rs-compact`
thread). `maybe_auto_compact` now **enqueues** a compaction request (non-blocking, with a
per-CF dedup set `compaction_queued` so the queue holds ≤1 entry per CF and re-queues if a
trigger arrives mid-compaction) instead of compacting inline. The flush worker returns
immediately. Concurrency is safe under the existing contract: `compact_l0_for_cf` takes
the engine-global `compaction_mutex` before any per-CF `flush_mutex`, `version_set.apply`
is serialized with stale-edit (R44-L2) validation, and `delete_file_guarded` defers SST
unlink while pinned, so concurrent flush ∥ compaction ∥ lock-free reads stay correct.
Errors route into the existing `flush_error` slot; `Drop` joins the compaction worker
after the flush worker and drains a final upload. Engine tests: **compaction 58/0,
flush 25/0**.

### (b) Low L0 compaction trigger
New `WriteControllerConfig::l0_compaction_trigger = 4` (separate from
`l0_slowdown_trigger = 40` / `l0_stop_trigger = 64`). `maybe_auto_compact` uses it. With
compaction now on its own thread, trigger=4 keeps L0 shallow **without** the inline
flush-stall that doomed the 2026-05-30 attempt — so point reads scan ~4 SSTs, not ~40.

## 3. Result (measured 2026-06-03)

- **q11: 67K/s-decaying → sustained ~400–460K/s with NO decay, reached the full 92M
  source** (best run). vs RocksDB ~800K/s steady — now **competitive** (was an 18× decay
  collapse / never-finished).
- **Variance observed:** a second identical-build run decayed to ~80K/s. Likely the
  single background-compaction thread + engine-global `compaction_mutex` not always
  keeping L0 shallow under q11's write rate, compounded by CPU contention from concurrent
  builds and an intermittent NexMark-harness restart. So the fix **eliminates the decay
  when compaction keeps up, but is not yet *reliably* steady at 100M scale.**

## 4. Follow-ups (the residual ~2× vs RocksDB + reliability)

- **Parallel compaction:** the engine-global `compaction_mutex` serializes all
  compactions, so the single worker is the throughput ceiling. Per-CF (or per-L0-range)
  compaction concurrency would let compaction reliably keep L0 shallow under high write
  rates — the reliability fix.
- **Skiplist memtable index + cheaper SST point reads:** closes the remaining ~2× vs
  RocksDB's point-read path.
- **NexMark-harness restart:** q11/q4 runs intermittently restart ~140s in via an
  external harness cancel+resubmit (`MetricReporter` "0 TMs" quirk) — NOT a forst-rs
  failure (zero `FrsException`/panic/`FAILED`; TM never crashes); it adds run-to-run
  variance and budget pressure but is out of forst-rs scope.

## 5. Verification status (measured 2026-06-03) — trigger=4 REGRESSED q7; REVERTED to 40

Regression sweep with `l0_compaction_trigger=4`:
- **q5: FINISHED 35.6 s** (was 46.9 s — *improved*, no regression).
- **q8: FINISHED 31.9 s** (consistent).
- **q7: REGRESSED.** At MAXSEC 800 it **decayed to ~13-32 K/s** (only 41 M @783 s),
  whereas *before* this change q7 ran **steady at ~248 K/s**. Root cause: with compaction
  every 4 L0 files, the **single** background-compaction thread (+ engine-global
  `compaction_mutex`) cannot keep up with q7's heavy write rate → L0 grows anyway → read-amp
  → decay, plus the constant compaction churns CPU/IO that q7's processing needs.
- **q4:** same heavy-write profile as q7 — same regression risk.

**The two heavy queries want OPPOSITE triggers and a single static value + single-thread
compaction cannot serve both:**
- q11 (read-bound, session-window point reads) → wants a **LOW** trigger (shallow L0,
  less read-amp). trigger=4 fixed its decay (67 K → ~400 K/s).
- q7/q4 (write-bound, one heavy join CF) → want a **HIGH** trigger (less compaction
  overhead; the single thread can't compact fast enough at a low trigger). trigger=4
  broke them.

**Decision: REVERTED `l0_compaction_trigger` 4 → 40** (= slowdown trigger, i.e. the
pre-change behavior) to avoid regressing the previously-working q7/q4. The
background-compaction worker (decouple-from-flush) is **kept** — it is architecturally
correct and, at trigger=40, behaves like the original (L0 rarely reaches 40, so bg vs
inline barely differs) → no regression. q5/q8 stay fixed (their wins came from the
O(N²) timer + merge-chain fixes, not the trigger).

**The PROPER fix that serves BOTH (future work): PARALLEL compaction** — RocksDB runs
`level0_file_num_compaction_trigger=4` *with* multiple background-compaction threads /
subcompactions, so a low trigger keeps L0 shallow without falling behind on writes. forst-rs
needs the same: per-CF (and ideally per-L0-range subcompaction for a single heavy CF)
compaction concurrency, which requires relaxing the engine-global `compaction_mutex` to
finer-grained locking. Only then can a low trigger fix q11 without regressing q7/q4.
