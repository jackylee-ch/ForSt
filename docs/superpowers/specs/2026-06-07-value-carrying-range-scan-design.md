# Value-carrying RANGE scan (eliminate per-key get_arc re-walk)

**Date:** 2026-06-07
**Status:** Implemented + correctness-verified (engine lib suite 277/0). Perf measurement in progress.
**Scope:** forst-rs engine only (`crates/forst-rs-engine/src/db.rs`). One function rewritten to mirror
the already-shipped prefix-scan equivalent. No FFI/Java change.

## Problem (the unified #2 lever)

The 5 NexMark queries failing the per-query 0.8× bar — q9 (0.56×), q11 (0.23×), q19 (0.34×),
q20 (0.71×), and partly q4 — all bottleneck on the engine **range scan**. Inspection found the cause:

`DbImpl::scan_iter_owned_arc_with_error_slot` (the range scan behind `frs_vec_iter_range_open`) yielded
each key then resolved its value with **`db.get_arc(cf, key)` per key** — a full LSM point-get
(`lookup_cf_by_id` + walk every tier) for every row the scan emits, i.e. **O(rows × tiers)**.

This is the exact double-walk that FRS-VALUE-CARRYING-MERGE removed for the **prefix** path
(`prefix_scan_iter_owned_arc_with_error_slot`) — logged as "the q4 2× read-path fix" (task #51). That
fix resolves SST-resident Puts inline from the merge cursor and only falls back to `get_internal` for
memtable-tier winners / merge-chains. **It was never carried over to the range path.** Both paths share
the same lazy k-way merge type (`LazyRangeIter = LazyPrefixIter`, a type alias) and the same
`next_with_value()` / `ValueDecision` API, so the range path already had the machinery — it just
wasn't used.

Worst offender: the **engine timer drain** (q11). The timer range scan reads (key, value) rows but the
Java side **discards the value** (`readRangeIntoCache` skips the value payload). So the old code did a
full per-key `get_arc` LSM re-walk solely to produce a value that is immediately thrown away.

## Fix

Rewrite `scan_iter_owned_arc_with_error_slot` to mirror the prefix path exactly:
- hoist `cf_data = lookup_cf_by_id(cf.id())` once (not per key),
- drive `inner.next_with_value()`,
- `ValueDecision::Put(value)` → yield the inline value (no get),
- `ValueDecision::Fallback` → `get_internal(cf_data, key, u64::MAX)` (memtable-tier / merge-chains only).

Byte-identical to the prior `get_arc` resolution (`get_arc` == `lookup_cf_by_id` + `get_internal` +
`Arc::from`); tier precedence and tombstone/merge semantics preserved — the same guarantees already
proven for the prefix path.

## Why it should help

- Eliminates a per-row LSM re-walk on every range-scan-heavy query (q9 TopN/ROW_NUMBER, q19/q20 OVER,
  q11 timer drain). For SST-resident Puts (the dominant case over flushed state) the value is now read
  from the merge cursor's already-decoded block instead of a fresh tier walk.
- It is a CONCRETE, specific fix (not the diffuse per-byte gap) with a PROVEN precedent (the prefix
  version delivered q4's 2× read-path improvement).

## Verification

- Engine lib suite: 277 passed / 0 failed (range/scan/merge/iter correctness intact). Scan tests 15/0.
- Perf (full 92M NexMark, all FINISHED with correct event counts):

  | query | before | after | Δ | ratio rdb/frs |
  |---|---|---|---|---|
  | q11 | 529 | **216** | **−313 (−59%)** | 0.23× → **0.57×** |
  | q19 | 428 | 379 | −49 | 0.34× → 0.39× |
  | q9  | 952 | 937 | −15 | 0.56× → 0.57× |
  | q20 | 578 | 554 | −24 | 0.71× → 0.74× |

- **Interpretation:** MAJOR win for timer-driven queries (q11 — its timer range scan reads (k,v) then
  DISCARDS v, so the old per-key `get_arc` was pure waste). MARGINAL for q9/q19/q20 because those use
  the PREFIX scan path, which was already value-carrying (task #51); their residual gap is the diffuse
  per-record floor (q4-class), not the range scan. The −313s on q11 (+ q5/q7/q8 timer paths) likely
  flips the original total-time criterion (~−400s total). The fix is unambiguous (4.5σ beyond the
  ±60s NexMark variance), unlike the timer-CF / REFILL_BATCH levers refuted earlier this session.
- **Still below the per-query 0.8× bar:** q4 0.76×, q9 0.57×, q11 0.57×, q19 0.39×, q20 0.74×. q11
  next lever = keys-only range scan (skip value resolution entirely for the value-discarding timer
  drain). q9/q19/q20/q4 = the diffuse prefix/point-get/merge floor.

## Follow-up (if values-discarded scans need more)

The timer scan discards values entirely. A keys-only range iterator (skip BOTH the inline Put copy and
the get_internal fallback) would remove all value resolution for timers — a larger change (new FFI +
engine method + Java wiring). Land + measure the value-carrying fix first; add keys-only only if the
timer refill is still value-bound.
