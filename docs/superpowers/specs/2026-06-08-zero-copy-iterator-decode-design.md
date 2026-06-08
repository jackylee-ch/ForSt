# Zero-Copy Iterator Decode — join-family read-path fix (q7/q9/q20)

**Date:** 2026-06-08
**Status:** Design (approved direction; implement + verify in one pass).
**Scope:** forst-rs backend (`flink-statebackend-forst-rs`). Engine untouched.

## Root-cause model (data + code, brainstorming-verified)
- **Profile (q20, FRS_BULK_SAMPLE):** `[DECAY_ATTR] probe≈4-5µs, n_ovl=2-3 SSTs (L0=1), sstloop≈2.5-3.3µs,
  B_resident=0`. → **read-amp is REFUTED** (only 2-3 overlapping SSTs/probe; LSM shape fine; engine read
  is healthy). The earlier "join read-amp" hypothesis is disproved by direct measurement.
- **Arithmetic:** q20 ≈15-20K rec/s on 4 busy slots; AEC batches ~1000 rec; ~60ms/batch ≈ 60µs/rec, of
  which only ~5µs is the engine. → **~90% of per-record cost is AROUND the engine read** (the Java
  async-state decode), not the LSM.
- **Code audit (the culprit):** the join probe scans matched entries via the iterator path →
  `ForStRsDBIterRequest.completeWithEntries` (:500-555) → `deserializeUserKey/Value(IteratorEntryView)`.
  The chunk arrives ZERO-COPY as off-heap `IteratorEntryView` slices, BUT the default decoders
  (`ForStRsIterableState:45-56`) bridge through **`IteratorEntryView.keyBytes()/valueBytes()`**
  (`IteratorEntryView:57-74`) which do **`new byte[len]` + off-heap→heap copy PER ENTRY**. So per matched
  entry: 2× `new byte[]` + 2× full copy + 2× deserialize. For q20 (auction⋈bid, many bids/auction → many
  entries/probe), this per-entry alloc+copy × entries × records dominates. This is the FORBIDDEN
  per-record / memory-copy pattern. (Point-GET path was already fixed — VectorizedExecutor:1295
  MemorySegment overload. Only the iterator/join-scan path still copies — code-flagged "deferred V1.2".)

## Fix: MemorySegment-backed DataInputView (zero-copy view decode)
1. **New `MemorySegmentDataInputView implements org.apache.flink.core.memory.DataInputView`** — wraps an
   FFM `MemorySegment` + base offset + length, running a position cursor. Implements all `DataInput`
   reads (readByte/Int/Long/Short/Char/Float/Double/Boolean/UTF/readFully/skipBytes…) **big-endian**
   (matching Flink's `DataInputDeserializer`/`DataOutputView` byte order), reading bytes directly from
   the segment (`seg.get(JAVA_BYTE, pos)`), no `byte[]` materialization of the whole entry.
2. **Override the view-based decoders** in the state path (where `deserializeUserKey(byte[],off)` /
   `deserializeUserValue(byte[])` are implemented): add `deserializeUserKey(IteratorEntryView, off)` /
   `deserializeUserValue(IteratorEntryView)` that point a `MemorySegmentDataInputView` at the slice
   `[keyOffset+off, keyLength)` / `[valueOffset, valueLength)` and call `serializer.deserialize(view)`
   directly. No `keyBytes()/valueBytes()`. `completeWithEntries` already passes the view → no rewiring.
3. Keep `keyBytes()/valueBytes()` only for any remaining legacy callers; the hot iterator path stops
   using them.

**What it eliminates:** the per-entry `new byte[]` + full off-heap→heap copy (2× per entry). The
serializer still reads incrementally, but now straight from off-heap; the whole-entry alloc+copy is gone.

## Correctness (GATE — a decode bug corrupts results)
- Unit test: for representative serializers (the q20 key/value types — Row/long/string), a value
  serialized to a `byte[]`, wrapped both as `DataInputDeserializer(byte[])` and as
  `MemorySegmentDataInputView(seg)`, must `deserialize()` to EQUAL objects (byte-for-byte read parity).
- e2e: q20 (and q9/q7) `out_rows` / final result identical to RocksDB on seeded input (the join-family
  accuracy gate — also resolves whether the earlier out_rows mismatches were real).

## Performance (before/after, recorded in the sweep doc)
- q20 wall @ locked config: before = DNF@1300s (59.8M). Target: finishes, and ≤1.25× RocksDB (need
  RocksDB q20 8c/32g baseline) + faster than ForSt (need ForSt q20 8c/32g).
- Re-profile `[DECAY_ATTR]`/per-record cost: the ~60µs/record should drop substantially.
- Applies to the whole join family (q7/q9/q20) — verify all three.

## Constraints honored
Config UNCHANGED (noflush=false, write_buffer_size=1G — this is an architecture fix, not config).
Zero-copy / no-`byte[]` / batch — directly serves the mandate. Engine untouched (backend-only).
