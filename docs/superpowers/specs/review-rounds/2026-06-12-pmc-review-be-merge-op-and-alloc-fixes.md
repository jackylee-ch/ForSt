# PMC review 2026-06-12 — RD delivery: BE merge operator (engine) + alloc/copy fixes V1-V4 (Java)

Reviewer: PMC architect (adversarial). Scope: engine commit `fe771c000` vs spec
`2026-06-12-opt-n04-merge-rmw-backend-design.md`; flink commit `72607e11097`.

## Item 1 — Engine: NumericAddBeMergeOperator + FFI arm + restore arms

**Verdict: APPROVE (delivered code), with two spec items OUTSTANDING (blockers
for J1-J5, not for this commit).**

Verified (code read + tests run locally this session):
- **Byte contract:** `i64::from_be_bytes`/`to_be_bytes` matches Flink
  `DataOutputSerializer.writeLong` network order; **wrapping_add** matches Java
  `long +` incl. associativity at the rails (merge_operator.rs:304-360; the
  associativity-at-overflow UT also proves the LE/saturating twin IS
  order-dependent — good regression tripwire). len≠8 → Corruption, no panic.
  Property tests: 200-case Java-fold equivalence, 100×5 partial-merge collapse
  orders with MAX/MIN, permutation invariance. All 50 merge_operator UTs green
  (run locally).
- **FFI name arm:** lib.rs:1262 + UT (BE put+merge+merge→folded GET) green.
- **Restore-by-name (E2):** both match sites (db.rs:~5185 default-CF arm,
  ~5233 non-default arm); noflush round-trip test restores via
  `open_from_incremental` + artifact replay, asserts cf_id preservation, fold
  ==8/==4, then flush+compact re-verify; flush-ckpt test splits the chain
  across SST+memtable. Both green locally (engine 2/2).

Outstanding vs spec (must land BEFORE the Java J1-J5 stage):
- **E1 NOT delivered:** `check_cf_homogeneity_locked` (db.rs:1119-1176) still
  rejects heterogeneous merge operators across non-default CFs. Today this
  passes vacuously (timer-CF default-OFF), but spec §3.2 required E1 in the
  same PR as E2, or the fallback made explicit. If the fallback is chosen, the
  timer-CF/merge-RMW mutual exclusion must be ENFORCED at backend init, not
  just documented.
- **E3 NOT delivered:** `create_cf_from_import` still attaches no operator
  (spec §3.3) — rescale round-trip with pending merges is an open gate.

## Item 2 — Java: flink 72607e11097 (alloc/copy V1-V4)

**Verdict: APPROVE.**

- **SliceKey FNV-1a → SegmentHash.polynomial31:** safe. Both call sites
  (DispatchOrderingHazards.java:251/:315) build per-call HashSets — hashes are
  transient, never persisted/cross-process, equals() byte-verifies
  (hash+len fast-path then `sliceBytesEqual`). Distribution change has no
  observable effect beyond probe cost.
- **polynomial31 stride math VERIFIED by hand:** LE `getLong` ⇒ memory byte i
  == `(byte)(v>>>8i)`; folded with 31^(7-i)·…, h·31^8 per stride — exactly the
  Arrays.hashCode recurrence unrolled by 8; signed-byte sign-extension
  preserved (cast binds tighter than `*`); scalar ≤7-byte tail; len==0 → 1.
  Identity is property-tested at the mandated 0/1/7/8/9 boundaries + 500 fuzz
  cases at non-zero offsets (ArrowTimerBufferTest) — required because
  ArrowTimerBuffer's open-addressed slots store the hash.
- **Timer peek/poll rewrite preserves EXACT semantics:**
  (a) memo behavior identical to the pre-image `decodeElementMemo` (stale memo
  after poll, byte-equality gate, refresh-on-miss — incl. the rebinder
  interaction, unchanged); (b) `pendingBuffer.find()` hoist above
  `removeAt(0)` is order-equivalent: find reads only pendingBuffer, the probe
  key bytes are read from liveIndex BEFORE the mutation — the audit's
  offset-invalidation hazard is handled (kOff/kLen consumed strictly
  pre-removal, peek path is pure-read); (c) `owned == peekMemoKey` aliasing
  into pendingPollDeletes is safe: the array is never mutated in place and
  `vectorizedBatchDeleteKeys` copies into staging segments.
- **mismatch compares:** semantics-identical (len equality pre-checked;
  equal-length empty ranges → -1).
- Tests: TimerHeadMemoV1Test 4/0 + ArrowTimerBufferTest 8/0 re-run locally
  against the current dylib (JDK 25; spotless/checkstyle skipped — local
  google-java-format reflows 157 PRE-EXISTING files, toolchain variance, not
  this commit).

## Notes
- The 4 memo/staging branches of poll are covered by the new focused tests;
  broader gate remains the full timer suites + Stage-0 lockstep rule (stated
  in the test class doc — correct).
