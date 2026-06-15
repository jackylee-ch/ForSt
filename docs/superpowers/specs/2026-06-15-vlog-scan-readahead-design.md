# Disagg scan readahead / prefetch pipelining — design + mini-bench (Phase-2 cycle 6)

**Date:** 2026-06-15
**Status:** PMC design — evidence gathered, implemented + micro-benched THIS cycle.
**Author:** PMC-2 (Phase-2 disaggregated state)
**Flag:** `FRS_VLOG_SCAN_READAHEAD` (default-OFF; byte-identical *and order-identical* when OFF)
**Window:** reuses `FRS_VLOG_SCAN_COALESCE_WINDOW`; `FRS_VLOG_SCAN_READAHEAD_WINDOW`
(clamped `[1, 65536]`) optionally OVERRIDES it (unset ⇒ the coalesce window)
**Composes with:** `FRS_VLOG_SCAN_COALESCE` (the windowed scan-deref this pipelines —
its `ScanCoalesceIter` window machinery is the unit of look-ahead),
`FRS_VLOG_COALESCE_DEREF` / `FRS_VLOG_DEREF_FANOUT` (the per-window coalesce + segment fan-out),
`FRS_KV_SEPARATION` (the baseline that creates vlog-resident values).

---

## 0. The finding (one paragraph)

The cycle-5 `FRS_VLOG_SCAN_COALESCE` lever made a streaming scan over KV-separated
state resolve its `BlobRef` derefs **one window at a time** — each window of up to
`W` rows groups its blob pointers by segment, offset-sorts, and (optionally) fans
the per-segment reads across the read-I/O pool, emitting in the SAME key order
(`ScanCoalesceIter`, `db.rs:18136`). But the windows are still processed
**serially relative to consumption**: `ScanCoalesceIter::next` drains window `k`'s
`ready` queue, and only when it is EMPTY does it call `fill_window()`, which BLOCKS
on window `k+1`'s coalesced remote reads before the consumer can pull the next row.
On the disaggregated/remote path that is **one window-worth of remote RTT of
dead time between every window** — the consumer sits idle while window `k+1`'s
segment GETs happen. This is exactly the gap ForSt closes with async
`readahead_size`: prefetch the next block(s) WHILE the current ones are consumed.
The fix is the iterator-side analogue — **one-window-deep look-ahead**: while the
consumer drains window `k`, asynchronously assemble window `k+1` (peek the inner
cursor) and kick its coalesced per-segment reads onto the read-I/O pool, so by the
time window `k` empties, window `k+1`'s values are already in flight (or done),
hiding the per-window RTT behind consumption. Byte-identical OUTPUT **and ORDER**
(the look-ahead pulls exactly the rows the inner cursor would have yielded, in the
same order — only WHEN the reads happen changes); timing-only; default-OFF.

---

## 1. Evidence — the inter-window stall is structural and confirmed serial

### 1.1 The serial window boundary (code-grounded)

`ScanCoalesceIter::next` (`db.rs:18270`):

```rust
fn next(&mut self) -> Option<Self::Item> {
    loop {
        if let Some(row) = self.ready.pop_front() {
            return Some(row);          // drain window k (no I/O — already resolved)
        }
        if self.inner_done { return None; }
        self.fill_window();            // BLOCKS on window k+1's remote derefs
    }
}
```

`fill_window` pulls up to `W` rows, then runs `coalesced_vlog_deref_into` — which,
on the remote path, issues `M` per-segment ranged reads (serial, or fanned-out
under `FRS_VLOG_DEREF_FANOUT`, but still **synchronous to this call**). The
consumer cannot pull window `k+1`'s first row until ALL of window `k+1`'s reads
return. So a scan of `N` rows over `W`-row windows pays `ceil(N/W)` window-RTTs
**none of which overlap consumption**. The within-window dimension (coalesce +
fan-out) is already optimal; the **inter-window** dimension is the gap.

### 1.2 Why this is the next lever (not the within-window one)

The within-window levers (`COALESCE_DEREF`, `DEREF_FANOUT`) collapse `W` scattered
reads into `M ≤ W` segment reads and overlap those `M`. But they do nothing for
the *boundary between windows*: window `k+1` cannot start until the consumer asks
for it. Look-ahead removes that boundary stall, and it **composes** — window
`k+1`'s in-flight read is itself a coalesced (and optionally fanned-out) per-
segment read; readahead only changes WHEN it is launched.

---

## 2. Design — one-window-deep look-ahead inside `ScanCoalesceIter`

### 2.1 State machine

`ScanCoalesceIter` gains an optional `prefetch: Option<WindowPrefetch>` holding the
NEXT window's in-flight resolution. The lifecycle, all inside `next`:

```
ready empties:
    if readahead engaged:
        if no prefetch in flight: assemble+launch window k        // first window
        join window k → ready                                     // overlapped read
        if !inner_done: assemble+launch window k+1                // look one ahead
    else: fill_window()                                           // legacy blocking
```

The look-ahead is **exactly one window deep** — at most one window is in flight
beyond the one being consumed. This bounds the extra buffered rows to `2·W` and
the extra in-flight read to one window's segments.

### 2.2 Assemble vs resolve split (order preservation)

`fill_window` is split into two phases that BOTH preserve slot (== key) order:

* **assemble** (`assemble_window`) — pull up to `W` rows from the inner cursor and
  classify each into a `ScanSlot` (`Ready` for Put/Fallback-hit, `PendingBlob` for
  a decoded pointer, `Err`/`Skip` otherwise), recording `(slot_index, pointer)` in
  `deferred`. **No I/O on the Blob path** — only the pointer decode. This is the
  synchronous, cheap, order-defining phase; it runs on the CONSUMER thread so the
  inner cursor (and its version pin) is touched by exactly one thread, in order.
* **resolve** (`resolve_window_slots`) — given assembled `slots` + `deferred`, run
  `coalesced_vlog_deref_into` (the per-segment coalesce + optional fan-out) and
  flatten resolved rows into a ready vector in slot order. This is the I/O phase;
  under readahead it is the part submitted to the pool.

Because **assemble** (which advances the inner cursor) always runs on the consumer
thread, and **resolve** only touches the segment readers + the disjoint
`resolved[slot]` slots it owns, the look-ahead never races the cursor and never
reorders emitted rows. The emitted sequence is identical to the legacy
`fill_window` regardless of when resolve ran.

### 2.3 The prefetch handoff (non-blocking launch, lazy join)

`WindowPrefetch` carries the assembled `slots` and a completion `mpsc::Receiver`
that yields the fully resolved `VecDeque<ForstResult<(key, value)>>`. `launch`
submits ONE job to the read-I/O pool (a single `pool.submit`, not a barrier — the
consumer is the barrier) that runs `resolve_window_slots` over an upgraded
`Weak<DbImpl>` and sends the resolved rows back. `join` blocks on `recv()` — but by
then the consumer has spent window `k`'s drain time, so the read has overlapped.
The very first window (before any drain has happened) is launched-then-immediately
joined, so cold-start parity holds (one un-hidden RTT, exactly as the model says).

### 2.4 Cancellation (early iterator drop)

On early drop of the iterator, the in-flight prefetch job holds only a
`Weak<DbImpl>`, the assembled keys (Arcs), and the sender half. When the iterator
(and its `Receiver`) drop, the job's `send` becomes a no-op (receiver gone) and
the resolved buffer is dropped — no value is emitted, nothing leaks, no thread is
stranded (the pool worker finishes its current job and returns to the pool). The
`Weak` upgrade also fails cleanly if the DB itself is being torn down mid-flight,
surfacing a benign corruption error that is never observed (receiver gone).

### 2.5 Version-invalidation / flush mid-scan

The inner cursor's version pin is unchanged: **assemble** (the only phase that
calls `inner.next_with_value()`) runs on the consumer thread, in order, exactly as
the legacy path. A flush/compaction mid-scan invalidates the cursor identically
whether or not the look-ahead ran — the cursor's own version logic governs which
rows it yields. The prefetch only resolves pointers the cursor ALREADY yielded
(a published vlog segment is immutable, so a pointer's bytes never change after
emit). So the look-ahead cannot observe a different row set than the inline path.

### 2.6 Engagement guards (zero work when not applicable)

Readahead engages only when ALL hold (checked once at `ScanCoalesceIter::new`):

* `FRS_VLOG_SCAN_READAHEAD` ON (default OFF ⇒ legacy `fill_window`, byte-for-byte);
* the FS is **remote** (`!self.db.fs.is_local()`) — local derefs are µs-class
  preads with no RTT to hide, and the pool hop would be pure overhead;
* `self_weak` is available (needed to form the `'static` pool job) — else fall
  back to the blocking `fill_window`.

When any guard fails the iterator behaves byte-AND-timing-identically to the
cycle-5 `ScanCoalesceIter` (which itself is byte-identical to the inline path).

---

## 3. Byte-identity & order TDD

`test_vlog_scan_readahead_byte_identical_kvsep_remote` (db.rs):

* KV-separation ON, `RemoteFakeFs`, three flush waves ⇒ ≥3 vlog segments so a
  scan window spans `M ≥ 2` segments.
* Arm OFF (`set_vlog_scan_readahead_override(Some(false))`) and Arm ON
  (`Some(true)`), each over a **fresh deterministic** build, scanned through
  `scan_iter_owned_arc_with_error_slot` (the FFI range path) under SCAN_COALESCE.
* Assert: ON row vector == OFF row vector **including order** (the gate), and each
  key carries its correct latest value vs a per-`get` oracle.
* A SMALL readahead window (`FRS_VLOG_SCAN_READAHEAD_WINDOW=4`) forces many window
  boundaries so the look-ahead actually pipelines (not one giant window).

`test_vlog_scan_readahead_midscan_overwrite_and_early_drop`:

* mid-scan overwrite + flush between window boundaries ⇒ confirms the look-ahead
  resolves the SAME rows the cursor yields (version-pin parity);
* early `drop` of the iterator after a few rows ⇒ no panic, no strand, no leaked
  prefetch (the receiver-gone path); a follow-up scan still returns correct data.

`test_vlog_scan_readahead_flag_default_off_and_override`: the gate defaults OFF and
the override flips deterministically.

---

## 4. Mini-bench — sim-S3 latency hiding (contention-robust, std-only)

`vlog_scan_readahead.rs` (`forst-rs-bench`) models the CROSS-WINDOW latency term:
a multi-window scan of `Wn` windows, each window costing one modeled coalesced
remote read (`FRS_MODEL_RTT_MS`) plus a modeled per-window CONSUME time
(`FRS_MODEL_CONSUME_MS`, the downstream operator draining the window's rows).

* **serial** (cycle-5): per window, `read` THEN `consume` — wall ≈
  `Wn·(rtt + consume)`.
* **readahead** (this cycle): window `k+1`'s read overlaps window `k`'s consume —
  wall ≈ `rtt + Wn·max(rtt, consume)` (the first read can't be hidden; thereafter
  each window costs the larger of its read or its consume).

The hidden term is `Wn·min(rtt, consume)` — approaching one window's RTT amortized
per window. The bench is contention-robust (a bounded pool mirroring
`prefetch.rs::read_io_pool`, Mutex+Condvar barrier) and `--smoke` asserts the win
holds (> 1.3× when `consume ≈ rtt`).

---

## 5. Composition with the rest of the disagg read path

```
KV-SEPARATION         → values live in vlog segments (the baseline)
SCAN_COALESCE         → per-window group-by-segment + offset-sort (within-window)
COALESCE_DEREF/FANOUT → the per-window coalesce + per-segment overlap
SCAN_READAHEAD (this) → overlap window k+1's coalesced read with window k's drain
SCAN_OPEN_FANOUT/PRIME→ overlap the cold SST-reader opens at scan build
```

Each lever is orthogonal and default-OFF; the uniform-best config stacks them.

---

## 6. Next-cycle candidates

1. **Async flush ↔ upload pipelining** — overlap the memtable→SST flush encode
   with the SST→remote upload (today serial), the write-path sibling of this
   read-path look-ahead.
2. **Negative-cache for absent segments** — a scan/batch that repeatedly probes
   keys whose segment was GC'd pays a remote miss each time; a small negative
   cache (segment_id → absent) short-circuits.
3. ~~**Multi-window depth ≥ 2 readahead**~~ — **DONE this cycle (cycle 7), see §7.**

---

## 7. Cycle-7 addendum — configurable look-ahead DEPTH (the read-bound fix)

**Flag:** `FRS_VLOG_SCAN_READAHEAD_DEPTH` (default `1` ⇒ byte-AND-timing-identical
to the cycle-6 one-window-deep path); clamped `[1, 64]`; test/bench hook
`set_vlog_scan_readahead_depth_override`.

### 7.1 Why depth-1 was insufficient on the remote tier

The cycle-6 look-ahead keeps **exactly one** window in flight. It hides at most
`min(rtt, consume)` of the per-window remote read: while the consumer drains window
`k` (cost `consume`), window `k+1`'s read (cost `rtt`) overlaps — but only for
`consume` of it. When **`rtt > consume`** (the disagg-typical regime: a coalesced
remote GET dominates a light downstream operator drain), the consumer still stalls
`rtt − consume` per window. Mini-bench (RTT=23 ms, consume=rtt/4, Wn=32): depth-1
= **1067 ms ≈ 1.01× over serial** — the look-ahead is nearly inert because one
in-flight window can't cover a read 4× longer than a drain.

### 7.2 Design — a depth-`D` in-flight FIFO

`ScanCoalesceIter` now holds `prefetch: VecDeque<WindowPrefetch>` (was
`Option<WindowPrefetch>`) and a `depth` resolved once at construction. `next`:

```
ready empties:
    refill: while !inner_done && prefetch.len() < depth: assemble+launch a window
    join the FRONT (oldest) window → ready
    (loop refills again next time ready empties)
```

`D` windows resolve **concurrently** on the read-I/O pool, so a resolved window is
ready the moment the consumer finishes the previous drain, as long as
`D · consume >= rtt`. Steady state becomes `consume`-bound — the iterator-side
analogue of ForSt's multiple parallel read threads. Effective concurrency is
`min(D, pool_width)` (`pool_width = clamp(cores/2, 2, 6)`). **Order is preserved**:
`assemble_window` (the only phase that advances the inner cursor) still runs on the
consumer thread in window order; only the FIFO of resolves grows. Windows resolve
out of order on the pool but are JOINED front-first, restoring emit order. Extra
buffered rows bounded to `(D + 1) · W`. Depth `1` ⇒ the FIFO holds ≤ 1 entry =
the cycle-6 path exactly.

### 7.3 Byte-identity TDD

* `test_vlog_scan_readahead_depth_byte_identical_kvsep_remote` — remote KV-sep
  scan, depths {1, 2, 4, 8} each reproduce the readahead-OFF baseline EXACTLY
  (rows + order) and each engages (launches ≥ 2 windows).
* `test_vlog_scan_readahead_depth_default_and_override` — default `1`, override
  clamps to `[1, 64]`, clearing restores default.
* Engine lib suite: **428/0** (4 ignored); fmt / clippy / rustdoc-strict clean.

### 7.4 Mini-bench — depth-D latency hiding (sim-S3, RTT=23 ms, 50 Gb/s throttle)

`vlog_scan_readahead.rs` extended to a depth-`D` FIFO model (`readahead_scan_depth`)
sweeping depth × windows × consume/rtt. Read-bound regime (consume = rtt/4), Wn=32:

| depth | wall (ms) | vs serial | vs depth-1 |
|------:|----------:|----------:|-----------:|
| 1     | 1067      | 1.01×     | 1.00×      |
| 2     |  549      | 1.97×     | 1.94×      |
| 4     |  295      | 3.66×     | 3.61×      |
| 8     |  250      | 4.32×     | **4.26×**  |

Honest negatives: when `consume >= rtt` (drain-bound) depth-1 already saturates and
deeper adds ~nothing (depth-2 ≈ depth-8); and depth-1 can trail serial by ~2-6% on
small scans (the pool-hop is pure overhead when the read can't be hidden) — depth-2+
always recovers. So **depth is the lever specifically for the read-bound disagg
regime** (`rtt > consume`), which is exactly the disaggregated-S3 case the goal
targets. `--smoke` asserts deep-over-depth-1 > 1.3× in the read-bound regime.

### 7.5 Next-cycle candidate

* **Resident-bytes budget on look-ahead depth** — auto-tune `D` from the observed
  rtt/consume ratio and a buffered-bytes cap, so the pipeline self-sizes to the
  measured remote latency instead of a static env knob. **DONE — §8 (cycle 8).**

---

## 8. Cycle 8 — ADAPTIVE depth (self-sizing from measured rtt/consume + a byte budget)

**Flag:** `FRS_VLOG_SCAN_READAHEAD_ADAPTIVE` (default-OFF; byte-AND-timing-identical
when OFF — the iterator holds the static cycle-7 depth for its whole life).
**Budget:** `FRS_VLOG_SCAN_READAHEAD_BUDGET_MIB` (default 16 MiB).
**Composes with:** the cycle-7 depth pipeline (it self-sizes the very `depth` the
cycle-7 refill loop consumes; the static `FRS_VLOG_SCAN_READAHEAD_DEPTH` env becomes
the controller's CEILING).

### 8.1 The finding (why static D is not enough)

Cycle 7 made `D` a static env knob. But the right `D` depends on the *measured*
remote latency vs the downstream drain — a single per-job-cluster value cannot be
right for both a read-bound regime (rtt ≫ consume, wants a deep pipeline) and a
drain-bound one (consume ≥ rtt, wants the minimal depth). Picking one static D per
query is exactly the per-query-knob the project forbids. The pipeline should
**self-size** from what it observes.

### 8.2 The signal (measured, not modeled)

Two times are measured per joined window:

* **`rtt`** — how long the coalesced remote read actually took, timed *inside the
  pool job* (`launch_prefetch`'s `Instant::now()` around `resolve_window_slots`).
  This is the TRUE read latency even after the join stops blocking because the read
  was hidden — the only place the real cost is still visible.
* **`consume`** — the PURE downstream drain: the wall from the previous join's
  completion to this join's *start*, captured **before** the blocking `recv()`. It
  deliberately EXCLUDES the recv block (the unhidden read) — folding the block into
  `consume` would conflate it with `rtt` and make the depth under-grow in the very
  read-bound regime depth is for. (This subtlety cost one mini-bench iteration: the
  naive "wall between joins" measure inflated `consume` and the controller settled
  at 2 instead of the optimal depth — see 8.5.)

Both feed an EWMA (`alpha = 1/4`): responsive within a handful of windows, immune to
single-window noise. Average row bytes (key+value) is also EWMA'd for the budget.

### 8.3 The control law

```
target = ceil(rtt_ewma / consume_ewma) + 1
clamped to min(pool_width, byte_cap, static_ceiling), floored at 1
```

**The `+1` is load-bearing.** In the FIFO pipeline the window being JOINED was
launched `D-1` consumes ago (the consumer popped one slot and is draining it while
the rest resolve). The join stops blocking once `(D-1)·consume >= rtt`, i.e.
`D >= rtt/consume + 1`. So `D = ceil(rtt/consume) + 1`: there must always be at
least one slot BEYOND the one being consumed for ANY overlap. A deeply drain-bound
regime therefore targets `2` (one read hidden behind one drain), never `1` (which
joins the read it just launched — zero overlap). This corrects the cycle-7 model's
implicit `D = ceil(rtt/consume)` and is why cycle-7's depth-1 ≈ serial in the table.

Three hard ceilings, resolved once at construction:

* `pool_width` (`clamp(cores/2, 2, 6)`, via `read_io_pool_width()`) — concurrency
  above the read-I/O pool is unrealisable.
* `byte_cap` = largest `D` with `(D+1)·window·avg_row_bytes <= budget` — recomputed
  from the EWMA row size, so WIDE vlog values shrink the cap (memory safety).
* `static_ceiling` = `FRS_VLOG_SCAN_READAHEAD_DEPTH` — kept as an explicit max
  override.

The controller only changes WHEN reads issue, never which rows or their order →
correctness-neutral at any value (the FIFO join restores emit order).

### 8.4 Byte-identity TDD

* `test_vlog_scan_readahead_adaptive_byte_identical_kvsep_remote` — remote KV-sep
  scan, adaptive ON (ceiling 16, budget 64 MiB) reproduces the readahead-OFF
  baseline EXACTLY (rows + order) and engages (≥ 2 windows launched).
* `test_adaptive_depth_ctl_control_law` — the controller in isolation: cold ⇒ 1;
  read-bound rtt/consume=4 ⇒ `ceil(4)+1 = 5`; drain-bound ⇒ 2; pool-width,
  static-ceiling, and byte-budget caps each bite at the expected value.
* `test_vlog_scan_readahead_adaptive_flag_and_budget` — flag defaults OFF, budget
  defaults 16 MiB, overrides + clamps deterministic.
* Engine lib suite: **431/0** (4 ignored); fmt / clippy / rustdoc-strict clean.

### 8.5 Mini-bench — adaptive vs BEST static, across regimes (no per-regime knob)

`vlog_scan_readahead.rs` extended with `AdaptiveCtl` (mirrors `AdaptiveDepthCtl`)
and `adaptive_scan` (the cycle-7 FIFO loop with the refill target = live
`target_depth()`). For each regime it computes the best static depth (min over
{1,2,3,4,6,8}, 3-run mean) and the adaptive run, asserting the controller CONVERGES
to the minimal-optimal depth (read-bound) and SETTLES at the minimal overlapping
depth 2 (drain-bound) — WITHOUT a per-regime knob.

Full run (RTT=12 ms, 50 Gb/s throttle, Wn=256, pool 6):

| consume/rtt | best static | adaptive (ms) | settled-D | adpt/best | adpt/serial |
|------------:|------------:|--------------:|----------:|----------:|------------:|
| 0.125 (very read-bound) | 8 | 741 | **6** (= pool cap) | 1.21× | 5.50× |
| 0.25 | 8 | 1002 | **5** (= ceil(4)+1) | 1.05× | 4.56× |
| 0.5  | 8 | 1923 | **3** (= ceil(2)+1) | 1.04× | 2.82× |
| 1.0  | 6 | 3757 | **2** (= ceil(1)+1) | 1.04× | 1.92× |
| 2.0 (drain-bound) | 6 | 6959 | **2** | **1.01×** | 1.52× |

The controller CONVERGES to the analytic optimum `min(ceil(rtt/consume)+1, pool=6)`
— {6, 5, 3, 2, 2} — in every regime and stays within **1.01×–1.21×** of the best
static depth's wall, with NO per-regime knob, never regressing the drain-bound case
(1.01×). The 1.21× at the extreme read-bound end is the depth-1→6 warmup ramp (a
fixed cost) plus that the bench's static sweep may exceed the pool width (the
controller correctly caps at the realisable pool). Convergence (settled-D = the
analytic optimum) is the load-bearing claim; the wall ratio is a loose sanity guard
(≤ 2.5× in `--smoke`) since real-sleep jitter on a shared CI box makes a tight bound
unreliable.

**Honest negatives.** (1) The adaptive run carries a fixed warmup ramp (depth grows
1→target over ~target windows) — on a SHORT scan the ramp is a larger fraction, so
the static optimum, if known, is marginally faster; the adaptive win is that NO
per-regime knob is needed and it never over-buffers. (2) When best-static is in a
consume-bound tie (every depth ≥ 2 ties), the printed `best static` column picks an
arbitrary deep depth — the controller deliberately picks the SMALLEST optimum (least
buffering), which the bench rewards via the drain-bound settle-at-2 assertion, not
the noisy argmin. (3) The `consume` measurement excluding the recv block is essential
(see 8.2) — the naive measure regressed convergence.

### 8.6 Next-cycle candidate

* **Write-side QoS: a flush-priority lane in the upload in-flight budget** — the
  shared `upload_sem` is class-blind, so a burst of large compaction-output uploads
  can occupy the whole in-flight budget and STARVE flush uploads, whose completion
  unblocks memtable rotation → ingest (write stall). Reserve a fraction of the
  budget for flush-class uploads. (Item B this cycle — see its own design note.)
