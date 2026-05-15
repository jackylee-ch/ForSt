# SP2 — Iterator Vectorization (Map entry / key / value)

**Date:** 2026-05-16
**Status:** Design — implementation pending
**Parent:** `2026-05-15-forst-rs-whole-program-vectorization-design.md` §SP2

## Goal

Replace the per-`next()` iterator FFM crossing with batched Arrow-streamed
`next_batch(max_n)` calls. Today `FrsIterator.next()` does one FFM call per
entry (open + next + close at iterator boundaries); on Nexmark Q7 / Q8 with
large MapState scans this dominates runtime.

## Approach (3 options, recommendation = A)

### A — Caller-owned output buffers + streaming `next_batch` (recommended)

Mirror the shape of SP3's poll-ahead cache for the PQ, but generalised: the
iterator handle stays open across batches, and each `next_batch` writes ≤
`max_n` entries into caller-owned key/value Arrow BinaryArray buffers in one
FFM call. Java decodes via the SP4 `MemorySegmentDataInputView` adapter (true
zero-copy return).

**Pros**: One FFM call per ~256 entries. Reuses primitives already shipped.
Per-shape variants (KEYS, VALUES, ENTRY) are a single FFI with a flag.

**Cons**: Iterator handle lifetime spans batches — needs explicit close on
backend dispose. Native materialization continues using engine's
`IteratorState`; no semantic change.

### B — Engine returns Arrow RecordBatch via C Data Interface

The engine builds an Arrow RecordBatch on each `next_batch`; Java receives it
via the Arrow C Data Interface (same as `frs_batch_get_arrow` already does).

**Pros**: Better interop with Arrow-aware downstream consumers.

**Cons**: Arrow C-Data-Interface release stubs are tricky; release-callback
overhead per batch adds ~150ns. No win over A on bench numbers.

### C — Eager materialization (status quo, batch size = ∞)

Open the iterator, drain all entries to a Java-side ArrayList eagerly. Return
items from the list on each `next()`.

**Pros**: Simple. Already how `prefixGetAll` works for the PQ cache.

**Cons**: Memory unbounded for large scans (Q7's 100k-entry MapState would
hold all entries in Java heap). Not suitable.

### Recommendation

**A**, with `max_n` defaulting to 256 (tuned by micro-bench).

## Components

### Rust FFI (new)

```c
// Open: returns an opaque handle whose lifetime spans multiple next_batch calls.
int frs_vectorized_iter_open(
    FrsDb, FrsCfHandle,
    const int32_t* prefix_offsets, const uint8_t* prefix_data, size_t prefix_count,
    int shape,   /* 0 = ENTRY, 1 = KEY_ONLY, 2 = VALUE_ONLY */
    FrsVectorizedIter* out_handle);

// Pull next ≤ max_n entries into caller-owned buffers.
// out_count receives the number of entries written.
int frs_vectorized_iter_next_batch(
    FrsVectorizedIter handle,
    size_t max_n,
    int32_t* out_key_offsets,  uint8_t* out_key_data,  size_t out_key_cap,
    int32_t* out_val_offsets,  uint8_t* out_val_data,  size_t out_val_cap,
    size_t* out_count,
    size_t* out_key_bytes,
    size_t* out_val_bytes);

int frs_vectorized_iter_close(FrsVectorizedIter handle);
```

`shape == KEY_ONLY` skips populating the val buffers (val pointers may be
null). `shape == VALUE_ONLY` skips key buffers. `shape == ENTRY` populates
both. Buffers grow-and-retry via `out_*_bytes` reporting on
`BUFFER_TOO_SMALL`, mirroring `frs_vectorized_batch_get`.

### Engine

Wraps existing `IteratorState` materialization but yields batched output. The
multi-prefix variant deduplicates entries — necessary when `prefix_offsets`
contains overlapping ranges. ~200 LOC.

### Java — VectorizedMapIterator

- Holds the FFI handle + key/value Arrow output buffers (borrowed from
  `VectorizedRuntime`'s pool).
- `hasNext()`: if cursor < current batch size return true; else trigger refill.
- `next()`: returns the current entry as a `Map.Entry<K, V>` decoded via
  `MemorySegmentDataInputView`.
- `close()`: returns buffers to pool, closes the FFI handle.

### VectorizedClassifier extension

Iter requests already accumulate in a separate list (sequential dispatch).
The new `VectorizedMapIterator` replaces today's `ForStRsMapIterator` which
calls `FrsIterator.next()` per entry. The classifier itself doesn't change.

### Specializations

- `MAP_ITER_KEY` (return only keys) — open with `shape=KEY_ONLY`.
- `MAP_ITER_VALUE` (return only values) — open with `shape=VALUE_ONLY`.
- `MAP_ITER` (entry stream) — open with `shape=ENTRY`.

These are wired in `ForStRsDBIterRequest.process` based on `originalRequestType`.

## Tests

- Rust: open + next_batch + close cycle on 1000-entry CF; verify ordering;
  verify shape variants populate the right buffers.
- Java: `VectorizedMapIterator` over a 100k-entry MapState; iterate to
  exhaustion; assert ordering matches forst.
- Boundary: `max_n=256` on a 257-entry state → 2 batches, last batch carries 1.
- Buffer growth: small initial out_data_cap; verify retry-after-BUFFER_TOO_SMALL
  path.

## Bench gates

- MapState iter throughput ≥ 1M entries/s (single-thread).
- Q7 / Q8 complete on local-cluster bench (previously "did not complete").

## Risks

1. **Handle leaks** — iterator handles must close on backend dispose / on
   exception in `hasNext`. Mitigation: `try-with-resources` enforced by the
   `AutoCloseable` Java wrapper; backend `dispose` walks an open-iterator
   registry.
2. **Concurrent mutation during iter** — Flink contract says no concurrent
   writes to the iterated state. Engine snapshot semantics already handle
   this (iterator opens an MVCC snapshot). Caller-side bugs would surface as
   stale-read; document but don't enforce.
3. **Refill amplification** — if the user calls `hasNext()` then bails out
   without `next()`, the refill already materialized 256 entries. Wasteful
   but not incorrect; `max_n` defaults to 256 so amplification is bounded.
