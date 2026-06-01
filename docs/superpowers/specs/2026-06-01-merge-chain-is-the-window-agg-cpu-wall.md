# 2026-06-01 — The window-aggregate CPU wall: un-combined merge-operand chains

## Decisive profile (symbolized, instrument-before-guess)
Built a symbol-preserving dylib (`CARGO_PROFILE_RELEASE_STRIP=none
CARGO_PROFILE_RELEASE_DEBUG=2`; the normal release `strip="symbols"` hid inner
frames), deployed, ran q5 at 5M (no checkpoint → pure CPU, 15K/s steady-state),
and `sample`d the TM. The ENTIRE busy state-executor thread stack is:

```
DbImpl::batch_get_vectorized
  → DbImpl::get_internal
    → DbImpl::collect_merge_operands   (full sample weight)
```

## Root cause
q5 (sliding-window COUNT) — and every aggregating/reducing state — stores its
accumulator as engine **Merge ops**. forst-rs's memtable APPENDS one entry per
merge op (no partial-merge combine). `get_internal` on a Merge key calls
`collect_merge_operands` → `peel_merges_from_memtable`, which walks the ENTIRE
operand chain for that key (every un-flushed merge entry, seq < cutoff) and applies
the merge operator. A window accumulator merged N times before a read costs O(N)
per read; with per-record read+merge over the window that is **O(N²)** — the 15K/s
wall. This is forst-rs-fixable and **independent of S3** (no checkpoint at 5M).

## The fix (RocksDB / ForsT parity): partial-merge-on-write
RocksDB keeps this O(1) by **combining merge operands in the memtable on insert**
(PartialMerge for associative operators) so the chain length stays ~1. forst-rs
should do the same: when `VectorizedMemTable` inserts a Merge op for a key that
already has an un-flushed Merge entry (no intervening Put), apply the merge
operator's partial/full merge to COLLAPSE the two operands into one, replacing the
entry. Then `collect_merge_operands` walks O(1) per read.

Requirements / care (correctness-sensitive — zero-tolerance):
- The merge operator (currently in the engine `apply_merge_operator`) must be
  reachable from the memtable insert path, and must expose an associative
  partial-merge (full_merge with base=None works for associative ops; COUNT/SUM/
  list-append/set-union all qualify — the existing operators).
- Preserve sequence semantics: the combined entry takes the NEWEST seq; readers at
  an older snapshot seq must still see the pre-combine view → only combine operands
  whose seqs are both ≤ the memtable's min live snapshot, OR keep combine to the
  active (unsealed) memtable where no snapshot pins intermediate seqs. Simplest safe
  scope: combine only within the active memtable's unsorted buffer for the same key
  since the last Put, gated on no pinned snapshot in that seq range.
- Validate: storage + engine merge/tombstone tests; q5/q6 output byte-identical to
  rocksdb; re-profile q5 (expect collect_merge_operands to vanish, rate ≫ 15K/s).

This is THE lever for window-aggregate / reducing / aggregating heavy queries
(q5/q6 + the aggregating paths in others) toward the ≥2× target, and it mirrors
exactly how RocksDB and ForsT keep merge reads cheap. It is the next focused
implementation (TDD, given the merge-semantics correctness sensitivity).

## ★ CORRECTION (verified before implementing — avoided optimizing an artifact)
`compaction.rs` (lines 173–522) ALREADY collapses merge chains via `full_merge`,
with snapshot-aware tail preservation (it keeps enough Merge tail for any pinned
snapshot's merge-aware reader). So **merge operands ARE combined at compaction** in
production. The q5 15K/s was measured at **5M events where NO flush/compaction fires**
(state < 2048mb memtable cap + checkpoint-without-flush) → all operands sit in ONE
active memtable forever, never compacted → an **artificially unbounded chain**. At
100M, the memtable fills → flush → L0 SSTs → compaction combines operands → the chain
is bounded (worst case = one memtable-lifetime, not the whole run).

Implication: partial-merge-on-write would help only the IN-ACTIVE-MEMTABLE chain for
hot keys (a bounded, smaller win than the 5M number implied), and it carries the MVCC
snapshot hazard. NOT worth a correctness-risky rush — compaction already handles the
dominant (cross-flush) case. This is an instrument-before-implement save: the 5M-scale
profile OVERSTATED the cost because it suppressed the very mechanism (flush+compaction)
that bounds it in production.

Residual real lever (smaller): in-active-memtable operand combine for hot keys, gated
on no live snapshot in the combined seq range — defer unless a 100M cloud profile shows
collect_merge_operands still dominant WITH flushes active.

## Cross-refs
- 2026-06-01-correctness-sweep-q0-q22.md (q5 = 15K/s pure-CPU wall at 5M)
- 2026-06-01-session-reorient-and-cache-bypass-ab.md (other landed CPU wins)
