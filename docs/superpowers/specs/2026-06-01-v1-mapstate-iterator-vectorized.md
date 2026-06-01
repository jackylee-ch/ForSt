# 2026-06-01 — V1-sync MapState iteration vectorized (chunked drain)

## Change
`ForStRsMapState.forEachEntry` (the V1-sync entries()/keys()/values() path) previously
walked the engine with a per-entry loop: `while ((e = linker.iteratorNext(iter)) != null)`
— ONE FFM downcall + ONE heap byte[] alloc PER ENTRY. Replaced both engine-walk sites
(statebuf-dedup mode + plain mode) with a new `forEachEngineEntryVectorized` that uses the
chunked `frsVecIterPrefixOpen/Next` drain already used by the V2 path: each FFM crossing
returns up to a 64 KiB chunk of rows ([u32 klen][u32 vlen][key][value] per row), cutting
crossings from O(entries) → O(entries/chunk). Row bytes are copied out before the next
chunk overwrites the buffer (no snapshot arena needed). A throwing `RawRowConsumer`
functional interface propagates the deserializer's IOException.

## Scope / honesty
- The Flink SYNC operator still calls the iteration once PER RECORD (immutable runtime
  model) — that per-record boundary cannot be removed in forst-rs scope. What this change
  removes is the per-ENTRY FFM crossing WITHIN each scan → the iteration is now batched/
  vectorized like V2. Biggest benefit: window/session queries (q11/q12/q15) that iterate
  MapState heavily.
- V1 point get (`getFast`→byte[]) and `clear` (per-entry delete) still use per-call FFI;
  iteration was the dominant per-record-sync cost and is the one addressed here.
- V2 path was already E2E-vectorized (VectorizedExecutor batch dispatch).

## Correctness (zero-tolerance) — VERIFIED
q11/q12/q15 (the V1-iteration-heavy queries) at 5M after the change: all FINISHED with
src_out EXACTLY 4,600,000 — identical to the pre-change sweep. Java build clean (JDK25).
