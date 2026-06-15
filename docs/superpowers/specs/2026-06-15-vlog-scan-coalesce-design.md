# SCAN-PATH inline vlog-deref → windowed coalesce + fan-out — design + mini-bench (Phase-2 cycle 5)

**Date:** 2026-06-15
**Status:** PMC design — evidence gathered, implemented + micro-benched THIS cycle.
**Author:** PMC-2 (Phase-2 disaggregated state)
**Flag:** `FRS_VLOG_SCAN_COALESCE` (default-OFF; byte-identical when OFF)
**Window:** `FRS_VLOG_SCAN_COALESCE_WINDOW` (default 256, clamped `1..=65536`)
**Composes with:** `FRS_VLOG_DEREF_FANOUT` (the per-segment fan-out it reuses),
`FRS_VLOG_COALESCE_DEREF` (the coalesce machinery it reuses),
`FRS_KV_SEPARATION` (the baseline that creates vlog-resident values).

---

## 0. The finding (one paragraph)

Cycle 4 parallelized the coalesced vlog-deref **for the point `batch_get`
path** (`FRS_VLOG_DEREF_FANOUT`). But the **single-iterator SCAN path** —
`DbImpl::prefix_scan_iter_owned_arc[_with_error_slot]`, the value-carrying drain
every join-probe / range-scan consumer goes through — never used the coalesce at
all. Its value resolver matches on `next_with_value`'s `ValueDecision`, and for a
KV-separated row (`ValueDecision::Blob(ptr)`) it calls `db.vlog_deref(ptr)`
**INLINE, per-row, fully serial** (`db.rs:~10157`). With KV-separation ON a scan
over `N` separated values pays **`N` remote GETs back-to-back** — the dominant
remote-read cost for scan-heavy disaggregated state, and exactly the asymmetry
ForSt's natively-batched iterator does not have. The fix is to make the iterator
buffer a WINDOW of rows, DEFER the window's `Blob` derefs, resolve them in ONE
coalesced pass via the cycle-3/cycle-4 machinery (`coalesced_vlog_deref_into` →
group-by-segment + offset-sort + optional `FRS_VLOG_DEREF_FANOUT`), and emit the
window **preserving the inner merge's exact key order**. The careful part is the
ordering: the iterator must defer + reorder value RESOLUTION (by segment/offset,
across the pool) while keeping the OUTPUT in key order — solved by fixed window
slots. Byte-identical when OFF and ON; default-OFF.

---

## 1. Evidence — the scan path never coalesced

### 1.1 The inline per-row deref (code-grounded)

`prefix_scan_iter_owned_arc_with_error_slot` builds the value-carrying drain
(`db.rs`, the `std::iter::from_fn` after `next_with_value`):

```rust
ValueDecision::Put(value)  => return Some(Ok((key_arc, value))),
ValueDecision::Blob(ptr)   => return Some(db.vlog_deref(ptr.as_ref()).map(...)), // ONE serial remote GET, per row
ValueDecision::Fallback    => { /* get_internal (memtable / merge-chain) */ }
```

Every separated row is one `vlog_deref` = `get_or_open_vlog_reader(seg).get(&ptr)`
= one remote round-trip, issued strictly one-after-another as the consumer pulls
`next()`. There is no batching, no grouping by segment, no overlap. For a scan
that returns `N` separated rows spanning `M` segments the wall is `N × RTT`.

### 1.2 The machinery already exists — it just wasn't wired to the scan

`DbImpl::coalesced_vlog_deref_into(deferred, resolved)` (cycle 3) already:
1. groups `(slot, ValuePointer)` by `segment_id`,
2. sorts each group by `offset` (chunk-cache HIT / one ranged read per segment),
3. (cycle 4, when `FRS_VLOG_DEREF_FANOUT` + remote + `M ≥ 2`) fans the
   per-segment reads across the read-I/O pool and barriers,
4. scatters each value into `resolved[slot]`.

`batch_get_vectorized` uses it (point-get path). The scan path simply never built
the `deferred` list — it derefed inline instead. This cycle wires the scan to the
SAME pass.

---

## 2. Design — windowed coalesce with order-preserving emit

### 2.1 Flag + window (`db.rs`)

* `vlog_scan_coalesce_enabled()` — `FRS_VLOG_SCAN_COALESCE` master flag, read LIVE
  (atomic test-override + env), default OFF. `set_vlog_scan_coalesce_override`
  for tests.
* `vlog_scan_coalesce_window()` — `FRS_VLOG_SCAN_COALESCE_WINDOW`, default 256,
  clamped `1..=65536` via `OnceLock`.

### 2.2 The order-preserving iterator (`ScanCoalesceIter`, `db.rs`)

When `vlog_scan_coalesce_enabled()` the builder returns a `ScanCoalesceIter`
instead of the inline `from_fn` (one branch; OFF path byte-for-byte unchanged).
It owns the inner `LazyPrefixIter`, the `cf_data`, the window size, and a
`ready: VecDeque<ForstResult<(Arc<[u8]>, Arc<[u8]>)>>` emit queue.

**`fill_window`** drains up to `window` rows from the inner cursor into a
`Vec<ScanSlot>` — the slot order IS the inner-merge key order:

| `ValueDecision` | slot |
|---|---|
| `Put(v)` | `Ready(key, v)` — resolved in place |
| `Fallback` → `get_internal` `Some(v)` | `Ready(key, v)` |
| `Fallback` → `get_internal` `None` | `Skip` (the inline `continue` — row vanished) |
| `Blob(ptr)` decode OK | `PendingBlob(key)` + push `(slot_idx, ptr)` to `deferred` |
| `Blob(ptr)` decode err / `get_internal` err | `Err(e)` — surfaced in-band at this slot |

Then ONE `coalesced_vlog_deref_into(deferred, &mut resolved)` fills the pending
slots. Finally the window is emitted **in slot index order** into `ready`:
`Ready` → `Ok((k,v))`, `Err` → `Err(e)`, `Skip` → nothing, `PendingBlob` →
`Ok((k, resolved[slot]))`.

**`next`** pops `ready`; when empty and the inner is not exhausted it fills the
next window (looping so an all-`Skip` window doesn't return a spurious `None`).

### 2.3 Why emit order is preserved (the careful part)

The value RESOLUTION is reordered (the coalesce groups by segment and sorts by
offset, and the fan-out runs segments concurrently), but each value is scattered
back to **its own fixed slot** (`resolved[slot]`), and the slots are emitted in
**index order == inner-merge key order**. So the OUTPUT row sequence is
identical to the inline path: slot `i` always emits the row the inline path would
have emitted `i`-th, with the exact same value (a slot's value depends only on
its own pointer, never on which segment-read served it or in what order). The
window size changes only batching granularity, never the emitted sequence.

### 2.4 Error parity

Tier-peek errors still land in the shared error slot the inner iterator owns
(drained by the FFI consumer) — unchanged. Value-resolution errors surface
in-band as `Some(Err(..))` at the row's slot, matching the inline path. A
coalesced-deref hard error (unreadable segment) aborts the window: it surfaces at
the first pending Blob slot and the rest of the window's pending Blobs are
dropped (first-error-wins — the serial `?` the coalesced pass already uses).
Rows that resolved before it still emit ahead, preserving order.

---

## 3. TDD — byte-identity (engine lib tests, `db.rs`)

`test_vlog_scan_coalesce_byte_identical_kvsep_remote` — KV-sep ON over a
`RemoteFakeFs` (so coalesce + fan-out engage), 180 keys under a `row` prefix,
THREE flush waves ⇒ ≥ 2 vlog segments, plus: a few off-prefix `zzz*` keys (must
be EXCLUDED by the scan), a `step_by(23)` DELETE wave (tombstones ⇒ genuine
MISSES the scan must hide), and overwrite waves (scattered latest values across
segments). Asserts:
* OFF scan == the sorted-key + latest-value oracle (independent third path),
* ON scan (with `FRS_VLOG_SCAN_COALESCE` + `FRS_VLOG_DEREF_FANOUT` forced ON)
  == OFF scan, row-for-row AND in order,
* ON scan == the oracle.

`test_vlog_scan_coalesce_flag_default_off_and_override` — default-OFF + override.
`test_vlog_scan_coalesce_window_clamp` — window accessor is always `≥ 1`.

All three pass.

---

## 4. Mini-bench — sim-S3 modeled-RTT (`forst-rs-bench/src/bin/vlog_scan_coalesce.rs`)

Models the SCAN latency term: `N` separated rows over `M` segments, window `W`.
* SERIAL (current): `N × RTT` (one inline `vlog_deref` per row).
* COALESCE (no fan): `Σ_windows segs_touched × RTT` (serial per segment).
* COALESCE + FAN-OUT: `Σ_windows ceil(segs_touched / pool) × RTT`.
`segs_touched` per window = `min(W, M)` (contiguous-flush round-robin layout).
Pool width mirrors `prefetch.rs` (`clamp(cores/2, 2, 6)`). std-only,
`forbid(unsafe_code)`-clean, smoke-gated (`-- --smoke` asserts > 2× at M ≥ 4).

**Full sweep** (window = 256, pool = 6; ratios are RTT-independent):

| rows N | segs M | serial | coalesce | vs serial | +fanout | vs serial |
|---:|---:|---:|---:|---:|---:|---:|
| 256 | 4 | N×RTT | Σsegs×RTT | **63×** | +pool | **243×** |
| 1024 | 8 | — | — | **33×** | — | **127×** |
| 4096 | 16 | — | — | **16×** | — | **85×** |
| 4096 | 64 | — | — | **4×** | — | **23×** |
| 16384 | 32 | — | — | **8×** | — | **43×** |

The coalesce alone collapses `N → Σ-segments` (the 4-63× column); the fan-out adds
the `÷pool` term on top (the 23-243× column). The win scales with how scan-heavy
and how segment-sparse the working set is — i.e. exactly the disaggregated regime
this Phase targets. (Smoke at RTT = 4 ms: +fanout 35× at the smallest case.)

---

## 5. Next-cycle candidate (continue scanning ForSt / papers)

The scan now coalesces its CURRENT window, but it still resolves the window
SYNCHRONOUSLY before emitting the first row — the consumer blocks on the whole
window's segment reads. The next async-remote-I/O lever is **scan readahead /
prefetch pipelining**: while the consumer drains window `k`, asynchronously
kick off window `k+1`'s coalesced segment reads on the read-I/O pool (a
one-window-deep look-ahead), so the per-window remote latency is hidden behind
consumption rather than paid in series between windows. This is the iterator-side
analogue of ForSt's `readahead_size` / async prefetch and composes directly with
this cycle's window machinery (the deferred list of window `k+1` is built from a
peek of the inner cursor). Secondary candidates banked: (a) async
flush↔upload pipelining (overlap the local SST flush with its remote upload
instead of `await_all_uploads` draining serially), and (b) a negative-cache for
vlog/SST misses on the disagg scan (skip the remote round-trip for known-absent
segments after a tombstone-heavy compaction).
