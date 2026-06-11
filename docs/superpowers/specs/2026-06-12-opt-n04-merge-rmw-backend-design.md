# OPT-N04 — Merge-RMW Backend Design (roadmap step ③ / L2b)

**Date:** 2026-06-12
**Status:** IMPLEMENTABLE SPEC (no code changed)
**Parent:** `2026-06-12-q9-q20-longscan-roadmap.md` L2 (rides the L2a `FRS_RS_MIXED_BATCH` flip)
**Goal:** kill the per-record dependent GET→PUT round-trip on eligible
Reducing/Aggregating accumulator states by emitting engine `Merge` deltas
(blind writes) folded by `NumericAddMergeOperator`-class operators, while
preserving byte-exact observable state, checkpoint/restore, and the
zero-copy / batch-only mandates.

---

## 0. The two-sentence mechanism

Today `asyncAdd` on a cache **miss** is a *dependent* chain: engine GET →
operator-thread fold → cache → barrier PUT
(`AbstractReducingState.asyncAdd`, flink-runtime
`state/v2/AbstractReducingState.java:60-82`: `asyncGetInternal().thenCompose(…
updateInternal(...))`; the forst-rs override only short-circuits on cache
*hit*, `ForStRsAsyncReducingStateV2.java:344+`). With merge routing, an add
never needs the old value at all: the state emits a `MIXED_KIND_MERGE` row
carrying the **delta**, the engine folds it associatively (memtable read-time
single-pass fold db.rs:9484-9492; compaction full_merge compaction.rs:1282-1308),
and the dependent read disappears — halving the per-key chain length that
AEC's KeyAccounting serializes (recorded round-2 finding, sweep doc
`2026-06-08-8c32g-3backend-sweep-results.md:1270-1274`).

---

## 1. Shipped machinery this design rides (all cited, all in-tree today)

| Piece | Where | Status |
|---|---|---|
| `NumericAddMergeOperator` (LE i64 saturating sum, retraction via negative deltas, corruption on len≠8) | `crates/forst-rs-storage/src/merge_operator.rs:221-273` | shipped + 13 UTs (:439-543) |
| CF-with-operator creation FFI (`"NumericAddMergeOperator"` recognised) | `crates/forst-rs-ffi/src/lib.rs:1233-1271` (`frs_db_create_cf_with_merge`, name match :1254-1259) | shipped, **not bound in Java linker** (ForStRsLinker.java binds only `frs_db_create_cf` :538 and `frs_db_create_cf_from_import` :1135) |
| Single-crossing mixed write batch (kinds 0/1/2 = Delete/Put/Merge, Arrow offsets layout, per-row caps, lazy `cf_has_merge_operator` guard on first Merge row) | `crates/forst-rs-ffi/src/lib.rs:3445-3552` → engine `DbImpl::batch_put_borrowed_single_cf` (db.rs:3446) | shipped, flag-off (`FRS_RS_MIXED_BATCH`, VectorizedExecutor.java:127-135) |
| Mixed-staging classifier: `MIXED_KIND_MERGE = 2` rows already flow for heap ListState appends | VectorizedClassifier.java:126, :1262-1276 | shipped (Stage-3 Unit-2) |
| Ordering-hazard twin routing same-key cross-kind conflicts to offer-order sync dispatch | DispatchOrderingHazards.java:133-180 (`requiresOrderedDispatchMixed`) | shipped |
| Two-regime machinery: `RegimeSwitch` injected into Reducing/Aggregating V2 states | RegimeSwitch.java:26-34; ForStRsAsyncReducingStateV2.java:285-303 (`setRegimeSwitch`, `rmwCacheUsable`); backend wiring ForStRsAsyncKeyedStateBackend.java:1128-1133, :1154-1158 | shipped (Stage-1 Task 6/7) |
| RMW cache with barrier drain: `flushOnBarrier()` on every registered instance pre-snapshot | ForStRsAsyncKeyedStateBackend.java:1559-1592 (PHASE 1.d); flush handler `rmwFlushToEngine` wired :1127/:1153 | shipped (PR-C3) |
| **Provable-sum detection**: `tryUnwrapPrimitiveSumReducer` — SerializedLambda introspection that recognises `Long::sum` / `Integer::sum` / `(a,b)->a+b` method refs and rejects domain-mismatched lambdas (A12-H1) | ForStRsAsyncReducingStateV2.java:200-205, :640+ | shipped (B11-H2) — *this is the eligibility oracle* |
| Long-specialized cache selection by serializer type (`LongSerializer` / `IntSerializer`) | ForStRsAsyncReducingStateV2.java:179-184 (`usePrimitiveLongCache`) | shipped (B10-H2) |
| Noflush checkpoint artifacts preserve op_type per entry (col 3 `UInt8`, replayed via `put_with_seq(key, val, op, seq)`) | db.rs:4651-4705 (`replay_memtable_artifact_bytes`) | shipped — Merge ops survive noflush ckpt |
| Per-CF compaction isolation: level picked per CF, inputs filtered by cf_id, SSTs stamped (R49-H1) | db.rs:4159-4227 (`pick_compaction_level_for_cf`, cf_id filter :4227); compaction.rs:55-61 (`CompactionJob.cf_id`), :128-143 (cross-CF input debug_assert) | shipped |

---

## 2. Eligibility detection — what is provably i64-add, and where the backend sees it

The backend NEVER sees SQL-level "COUNT/SUM(BIGINT)" — by the time state is
registered, SQL aggregates are generated code. What the backend *does* see, at
`createReducingState` (ForStRsAsyncKeyedStateBackend.java:1104-1133), is the
`(valueSerializer, reduceFunction)` pair, and both have shipped oracles:

**A state is merge-eligible iff ALL of:**
1. `valueSerializer instanceof LongSerializer` — exactly the existing
   `usePrimitiveLongCache` test minus the `IntSerializer` arm
   (ForStRsAsyncReducingStateV2.java:179-181). `IntSerializer` is **excluded**
   in v1: its stored form is 4-byte BE int; widening storage to 8 bytes would
   change the checkpointed byte format of the state (restore break) — the
   in-memory long-promotion (B10-H2) is cache-only and does not license an
   on-disk widening.
2. `tryUnwrapPrimitiveSumReducer(reduceFunction, /*isInt=*/false) != null`
   (:640) — the lambda's implMethod is one of the known primitive-sum forms.
   This is *sound* (introspection of the actual implementation method, not a
   behavioral probe): `Long::sum` and `(a,b) -> a + b` are additive and
   commutative-associative over i64 two's-complement... **with one caveat:
   overflow.** Java `+` wraps; `NumericAddMergeOperator` **saturates**
   (merge_operator.rs:258, `saturating_add`). See §4 (operator contract) — the
   shipped BE twin must WRAP, not saturate, to be byte-equivalent with the
   GET→fold→PUT path.
3. The state is a `ForStRsAsyncReducingStateV2`. **AggregatingStateV2 is
   v1-EXCLUDED**: `AggregateFunction.add(in, acc)` is not introspectable as
   additive in general (acc type ≠ input type; generated SQL accumulators are
   opaque). It keeps the RMW cache path. A per-state-name opt-in env list
   (`FRS_RS_MERGE_RMW_STATES=name1,name2`) is the escape hatch for operators
   the user can vouch for — default empty.
4. TTL is not configured for the state (TTL wraps the value bytes with a
   timestamp → no longer an 8-byte add domain; the TTL compaction filter also
   must never see this CF — see §3 homogeneity note).

Detection runs ONCE at state registration; the decision is a `final boolean
mergeRouted` field — same JIT-speculation pattern as `usePrimitiveLongCache`
(B10-H2 comment, ForStRsAsyncReducingStateV2.java:115-119).

**Honest scope note (falsifies part of the roadmap model if wrong):** q20's
per-record count RMW lives in Flink SQL's
`JoinRecordStateViews$InputSideHasNoUniqueKey` — a **MapState<RowData,Integer>**
whose increment is done in operator code (get → +1 → put), invisible to the
backend. q9's TopN accumulator is similarly Map/Value-state shaped.
**Backend-transparent merge routing CANNOT capture those.** What it captures
is the Reducing-state population: q12-pattern SumAgg/CountAgg window reduces
(the very workload the B10/B11 cache work targeted —
ForStRsAsyncReducingStateV2.java:178 comment "Q12 SumAgg / CountAgg ... hit
the combiner millions of times per second"), q5/q7/q8-class window aggregates
that register ReducingState. Therefore: **the q9/q20 −15..25 % roadmap model
is NOT guaranteed by this design alone**; the q9/q20 A/B is the arbiter, and
the canaries (q8/q12) are where the win is structurally certain. If q9/q20
need the same treatment, the follow-up is an *operator-visible* lever
(count-map merge in the SQL join state view = a Flink-side change, out of this
backend's scope; flagged for the PMC report).

---

## 3. CF and operator routing

### 3.1 Why not the default CF

The default CF carries `RawConcatMergeOperator` (every `frs_db_open*` path:
lib.rs:160-168, `raw_concat_cf_descriptor`) because ListState appends are
Merge rows there (default CF has RawConcat — q8957 doc test lib.rs:8957). One
CF = one operator (`ColumnFamilyData.merge_operator`, column_family.rs:344-349
"FIXED AT CREATE TIME"). An i64-add delta concatenated by RawConcat is silent
corruption. ⇒ eligible states move to a dedicated CF.

**Decision: ONE shared CF for all merge-routed states**, name
**`agg-merge-i64`**, operator `NumericAddBeMergeOperator` (§4). Per-state CFs
buy nothing (states are already disambiguated by the composite key's
state-name segment — `k/<key>/<stateName>/<ns>`,
ForStRsAsyncReducingStateV2.java:310-322) and would multiply memtables,
flush units and checkpoint blob entries.

### 3.2 The R45-H1 homogeneity wall, and why it is passable

`check_cf_homogeneity_locked` (db.rs:1075-1132) rejects any non-default CF
whose merge operator differs from every existing non-default CF's. Facts:

- The default CF is **excluded** from the comparison (db.rs:1095).
- In the production NexMark config the only other non-default CF is the timer
  CF, which is **flag-gated default-OFF** (timer-CF lever refuted 2026-06-07;
  ForStRsAsyncKeyedStateBackend.java:1893-1894 binds the queue to the "legacy
  shared defaultCf"). ⇒ today, creating `agg-merge-i64` as the *first*
  non-default CF passes vacuously, and additional merge-routed engines stay
  homogeneous (all `agg-merge-i64` states share the one CF).
- BUT: enabling the timer CF (RawConcat via `frs_db_create_cf`, lib.rs:1214)
  alongside `agg-merge-i64` would be REJECTED. And the restore path already
  bypasses the check on purpose ("legitimately resurrecting mixed-policy
  CFs", db.rs:5194-5198) — so heterogeneous CF sets are already a supported
  runtime condition.
- The check's stated reason — "the engine's L0 layer is shared across CFs"
  (db.rs:1107-1110) — is stale: compaction level-picking, input selection and
  output stamping are per-CF (db.rs:4159-4227 cf_id filter; compaction.rs
  cf_id debug_assert :128-143 — "Once the engine wires per-CF version lists
  this check becomes a hard error").

**Engine work item E1 (prerequisite):** retire R45-H1 for merge operators —
(a) promote the compaction cross-CF-input `debug_assert` to a hard
`ForstError::corruption` (compaction.rs:134-143 and the streaming twin
:579-587), (b) drop the merge-operator arm of `check_cf_homogeneity_locked`
(keep the compaction-filter arm unless the same per-CF audit is done for
filters), (c) add a test: two CFs with different operators, interleaved
writes, full flush+compact cycle, byte-exact reads per CF. ~60 LoC + tests.
*Fallback if E1 is rejected in review:* keep R45-H1 and ship with the
documented constraint "timer-CF flag and merge-RMW flag are mutually
exclusive" — acceptable since timer-CF is refuted-and-off, but it is a
landmine; E1 is preferred.

### 3.3 Creation, restore, and name-stability

- **Create:** backend binds `frs_db_create_cf_with_merge` in `ForStRsLinker`
  (new downcall, signature mirrors `frs_db_create_cf` + a const operator-name
  string). CF is created lazily at the first eligible state registration,
  before any write to it (engine rejects Merge rows on a CF without an
  operator — lib.rs:3532-3536, D-R8-NEW-H2).
- **Checkpoint blob:** CF descriptors persist `merge_op_name` (R49-H2 snapshot
  collection db.rs:9495-9500). **Engine work item E2:** the restore-by-name
  match has NO arm for the numeric operator — db.rs:5132-5146 (default CF)
  and db.rs:5173-5187 (non-default) both error with "unknown merge operator"
  for anything but RawConcat/ListAppend. Add the
  `"NumericAddBeMergeOperator"` arm (+ keep `"NumericAddMergeOperator"`
  recognised for completeness). Without E2, **any checkpoint taken with this
  feature ON is unrestorable** — E2 ships in the same PR as E1, before any
  Java work, with a create→write-merges→ckpt→restore→read round-trip test.
- **Import/rescale path:** `create_cf_from_import` attaches NO operator
  (db.rs:7341 — descriptor without merge op). Verify (gate, not assumed):
  the export blob materialises resolved values (export reads collapse merge
  chains), so imported CFs contain only Puts and the missing operator is
  benign for *imported* data — but the recreated CF must still get the
  operator for *future* merges. Work item E3: thread the operator name
  through `frs_db_create_cf_from_import` (new optional arg or post-create
  validation that the backend re-creates via the with-merge path then
  imports). Add a rescale round-trip test with pending merges at export time.
- **Name stability:** operator `name()` is the cross-checkpoint identity
  (merge_operator.rs:64-86 identity contract; R46-L2 "must stay stable
  across releases", :268-271). `NumericAddBeMergeOperator` returns a fixed
  string, never versioned-by-config (it has no config).

---

## 4. Serialized accumulator format — the byte-layout check (FAILS for the shipped operator)

**Flink's Long bytes are BIG-endian.** `LongSerializer.serialize` →
`DataOutputView.writeLong` → `DataOutputSerializer.writeLong`
(flink-core `core/memory/DataOutputSerializer.java:212-221`): on
little-endian hardware the value is `Long.reverseBytes`-ed before the raw
`UNSAFE.putLong` — i.e. network order, MSB first.

**The shipped engine operator is LITTLE-endian** (`i64::from_le_bytes`,
merge_operator.rs:229-237) **and SATURATING** (`saturating_add`, :258, :264).
Routing Flink accumulator bytes through it would (a) sum byte-swapped
garbage, and (b) even with swapped inputs, diverge from Java semantics at
overflow (Java `+` wraps; q12-class counters won't overflow i64, but
byte-equivalence is the gate and "won't overflow" is not a proof).

**Decision: ship a new engine operator, do not transform bytes in the
backend.**

```rust
/// merge_operator.rs — sums 8-byte BIG-endian i64 deltas with WRAPPING
/// two's-complement addition (byte-equivalent to Java `long +`).
pub struct NumericAddBeMergeOperator;   // name(): "NumericAddBeMergeOperator"
// full_merge: i64::from_be_bytes, fold with wrapping_add, to_be_bytes
// partial_merge: same. len != 8 → ForstError::corruption (same as LE twin).
```

Why this side of the boundary:
- The stored bytes remain **exactly** what today's PUT path stores
  (`serializeValueBytes` output, ForStRsAsyncReducingStateV2.java:246-251) —
  GETs, checkpoint contents, savepoint compatibility and the off-path
  byte-identity gate all hold trivially. A backend-side LE byte-swap would
  fork the state's serialized format from its serializer (restore into
  non-merge-routed code would read swapped longs = silent corruption).
- Wrapping vs saturating: `wrapping_add` is associative and commutative, so
  partial_merge order (compaction may fold any adjacent pair,
  compaction.rs:221-223) cannot change the result — same guarantee Java's
  wrapping `+` gives the GET→fold→PUT path. Saturation is NOT associative
  near the rails (`sat(sat(MAX,1),-1) = MAX-1 ≠ sat(MAX,0) = MAX` ordering
  hazards) — one more reason the LE/saturating twin is the wrong fold for
  this use even ignoring endianness.
- The delta written by `asyncAdd` and the absolute value written by
  `updateInternal`/clear use the SAME 8-byte BE layout — `Put` rows (absolute
  rebase) and `Merge` rows (delta) coexist on one key with no format split.

**Engine work item E4:** operator + UT battery mirroring the LE twin's
(merge_operator.rs:439-543) plus an associativity-at-overflow test, plus the
FFI name-match arm (lib.rs:1254-1259) and restore arms (E2). ~120 LoC.

---

## 5. Write path — state-side staging and the mixed batch

### 5.1 The delta cache (replaces the absolute-value RMW cache for routed states)

In merge mode the per-state cache keeps its alloc-free hit path but changes
meaning: it accumulates the **pending delta** (a primitive `long`, starting
at 0) instead of the folded absolute value.

- `asyncAdd(v)`: fold `v` into the cached delta (`LongBinaryOperator` =
  `+`, same `LongReducingAggregatingCache` machinery) — zero engine I/O, **no
  miss-resolve GET ever** (the miss path seeds delta = v and is done). This
  is the chain kill: today's miss does `asyncGetInternal()` (a dependent
  engine round-trip serialized by KeyAccounting); merge mode does nothing.
- `flushOnBarrier()` / LRU eviction: emit ONE `Merge` row per dirty key
  carrying the 8-byte BE delta, via the mixed batch (kind=2), then zero the
  entry. Today's flush emits a PUT of the absolute value
  (`rmwFlushToEngine`, ForStRsAsyncKeyedStateBackend.java:1127); the merge
  flush reuses the same handler signature with an op discriminator.
- `asyncGet()` / `asyncGetInternal()`: engine GET (folds the chain via
  full_merge) **plus** the in-cache pending delta added on the operator
  thread. Reads stay correct without flushing first.
- `clear()` / `updateInternal(v)` (absolute writes): emit a `Put` (rebase) /
  `Delete` row and reset the cached delta to 0. Put-after-merges is the
  chain terminator the read path already understands (base = newest Put,
  db.rs:8737-8778).
- Retraction needs no special casing: negative deltas are just negative
  i64s; `retract`-style usage flows through the same `asyncAdd` (the
  operator sums signed values — merge_operator.rs:212-216 documents the LE
  twin's identical contract).

Regime interaction: `rmwCacheUsable()` (ForStRsAsyncReducingStateV2.java:296-303)
currently disables the cache under HEAVY regime because cache folds +
flush-handler PUTs race in-flight worker batches. **Merge mode is strictly
safer under HEAVY**: deltas commute with everything except same-key
Put/Delete/Get, and those hazards are exactly what
`requiresOrderedDispatchMixed` already detects (DispatchOrderingHazards.java:
138-141 GET×PUT, :150-156 GET×MERGE and PUT×MERGE same-key checks). v1 keeps
the regime gate as-is (delta cache active only when `rmwCacheUsable()`);
under HEAVY, `asyncAdd` emits a per-record `MIXED_KIND_MERGE` row into the
classifier — still a blind write, still no GET, just less batching of deltas.
Relaxing the gate is a follow-up measured separately.

### 5.2 Batch dispatch — the single-CF constraint

`frs_vectorized_batch_mixed` is **single-CF** (lib.rs:3445-3456 takes one
`cf`; engine `batch_put_borrowed_single_cf` db.rs:3446), and the
VectorizedExecutor is constructed against `defaultCf`
(ForStRsAsyncKeyedStateBackend.java:1279). Routed states' rows (their GETs
too) target `agg-merge-i64`. v1 resolution: **one extra crossing per
executor batch** — the classifier partitions rows by CF handle (a second
mixed-column set for the agg CF; GET rows for routed states go into a
second `ForStRsDBGetRequest` against the agg CF). Batch-only mandate holds
(2 crossings per batch ≠ per-record); the crossing count delta is noise next
to the removed per-record GETs. A multi-CF mixed FFI (per-row cf column) is
explicitly deferred — it complicates the validated offsets layout
(lib.rs:3402-3427) for a constant-factor win.

Ordering hazards across the two CFs: none — CFs are disjoint keyspaces;
hazard twins run per-CF batch. Within the agg CF batch the existing
`requiresOrderedDispatchMixed` predicates apply unchanged (Merge×Get
same-key → offer-order sync dispatch).

### 5.3 What the classifier needs (Java work items)

- J1: bind `frs_db_create_cf_with_merge` in ForStRsLinker (+ FrsCfHandle
  plumbing, close on backend dispose — mirror defaultCf lifecycle,
  ForStRsAsyncKeyedStateBackend.java:346).
- J2: `mergeRouted` eligibility in `createReducingState` (§2), CF handle
  injection into the state (`ForStRsInnerTable` gains `cfHandle()` default =
  defaultCf).
- J3: delta-mode `LongReducingAggregatingCache` variant (fold-into-delta +
  flush-as-merge callback; ~the existing class parameterized by a flush op
  kind).
- J4: classifier second-CF mixed columns + executor second dispatch +
  GET-request CF routing.
- J5: env flag `FRS_RS_MERGE_RMW` (default OFF), plus the per-state opt-in
  list for AggregatingState (§2.3). Requires `FRS_RS_MIXED_BATCH` ON
  (hard precondition check at backend init — merge rows need the mixed
  carrier; the per-kind path has no Merge column for non-ListState rows).

---

## 6. Read path — when chains collapse, and the cost model

Where merge operands get folded today:
- **Memtable:** single-pass shard-lock-once fold at read
  (`collect_merge_operands`, db.rs:9205, rewritten after the q5 O(N²)
  incident — db.rs:9479-9492). Cost ≈ O(chain) with one lock.
- **SST read:** operand accumulation walks tiers until a Put/Delete base or
  chain exhaustion (db.rs:8737-8778, 8824), then one `full_merge`.
- **Flush does NOT collapse** (op types preserved into SSTs); **compaction
  collapses** via snapshot-aware full_merge/partial_merge
  (compaction.rs:173-522, :1282-1308).

Chain-length model (the §risk-3 arbiter from the roadmap):
- LIGHT regime with delta cache: ≤1 Merge row per key per **barrier**
  (30 s default) + LRU evictions ⇒ chains of ~single digits between
  compactions. GET cost increase: negligible.
- HEAVY regime (per-record merge rows): chain length per key between
  flushes ≈ adds-per-key-per-flush-interval — q12-class hot counters could
  reach 10³-10⁴ operands. GET-at-window-fire then pays O(chain) — but a
  reducing state is read ~once per window fire vs incremented per record,
  so the trade is (N adds × saved GET) vs (1 read × O(N) fold): strictly
  favorable, EXCEPT the 5M-no-flush artifact regime (memory:
  merge-chain-is-the-window-agg-cpu-wall — at small scale nothing
  compacts). **Hence the law: measure only @100M (gate G6).**
- Pathology guard: cap re-used from the q5 fix — if a GET observes a chain
  longer than `FRS_RS_MERGE_CHAIN_REBASE` (default 4096), the state's next
  flush emits a rebasing `Put` (absolute = engine value + pending delta)
  instead of a `Merge`. Cheap (the GET already materialised the fold) and
  bounds worst-case read cost deterministically. (Engine-side rebase during
  reads is rejected: a read-triggered write violates MVCC snapshot reads
  and the read path's lock discipline.)

---

## 7. Checkpoint / restore with pending Merge operands

1. **Flush-based checkpoints:** Merge rows flow into SSTs with op preserved
   (kv layout carries op u8 — wal.rs:65 documents the op byte incl. Merge;
   SST read path collects operands §6) — nothing new.
2. **Noflush checkpoints:** memtable artifacts persist `(key, value, seq,
   op)` columns and replay via `put_with_seq` preserving op + seq
   (db.rs:4651-4705). Pending Merge operands therefore survive noflush
   ckpt/restore *mechanically*; the gate is a **restore round-trip test
   with a live chain**: write Put(5), Merge(+1), Merge(+2) → noflush ckpt →
   restore → GET == 8, then compact → GET == 8 (test lands engine-side with
   E4).
3. **Barrier ordering:** PHASE 1.d drains every registered state's cache
   BEFORE the engine snapshot (ForStRsAsyncKeyedStateBackend.java:1559-1592),
   so the checkpoint contains the flushed delta — the cache itself is never
   checkpoint state. Unchanged by this design (the flush emits Merge instead
   of Put).
4. **Restore-by-name:** E2 (§3.3) is the blocker; ships first.
5. **Downgrade story:** a checkpoint with pending Merge rows in the agg CF
   is NOT readable by a binary without the BE operator registered (restore
   errors at db.rs:5181-5186 — fail-closed, no silent corruption). Release
   note + the flag staying OFF until E1-E4 are in the deployed dylib.

---

## 8. Fallback (ineligible / disabled)

Any state failing §2 keeps today's exact path: RMW cache + GET-on-miss +
PUT-on-barrier; flag OFF ⇒ no CF is created, no linker downcall is made, no
classifier columns allocated (all new code behind `final boolean` fields =
JIT-dead). The eligibility decision is logged once per state at registration
(`INFO: state '<name>' merge-routed: <reason>` / not) — the q8/q12 canary
runs must show the expected states routed before any timing is read.

---

## 9. Gates (every one falsifiable, ordered)

| # | Gate | Pass bar |
|---|---|---|
| G0 | E1-E4 engine UTs + storage(355)/engine(277+) suites green | 0 fail |
| G1 | BE operator byte-equivalence property test: random (base, deltas[]) — full_merge == Java BigInteger-free wrapping fold; partial/full associativity shuffle test | exact |
| G2 | Restore round-trips: flush-ckpt + noflush-ckpt + rescale-import, each with live merge chains | exact values, no "unknown merge operator" |
| G3 | Java backend suite (114/0 baseline) + new J1-J5 UTs; off-flag byte-identity re-verified (L2a gate) | 114+/0 |
| G4 | Seeded same-input replay byte-exactness on q8/q12 (the canaries — windowed-value check, not pane counts; q5-class lesson) flag ON vs OFF | byte-identical output |
| G5 | Lockstep exactness ×2 + routing-async ×5 (Stage-0 timer-regression rule) | exact |
| G6 | @100M 8c/32g n≥3: q8/q12 A/B (expected win), q9/q20 A/B (model arbiter), q17/q3 no-regress | q8/q12 ≥ recorded-noise improvement; q9/q20 reported honestly per §2 scope note; no canary regress >box noise |
| G7 | Chain telemetry: max observed operand chain at GET (new counter) under q12@100M | < rebase cap; rebase counter ≈ 0 in LIGHT |

**Falsifiable model:** q12's reducing-add population is ~1 add/record on the
hot window counter; LIGHT-regime delta cache already absorbs hits, so the
marginal win = eliminated **miss-resolve GETs** + eliminated per-barrier PUT
serialization pressure + halved KeyAccounting chain on miss keys. Model:
q12 −8..15 %, q8 −5..10 %; q9/q20: 0..−5 % from this lever alone (their RMWs
are MapState-shaped, §2). If q8/q12 show <3 % at n≥3, the miss-rate
assumption is wrong (cache hit-rate ≈ 1 ⇒ chain already dead) — STOP, do not
chase q9/q20 with this lever, and promote the operator-visible count-map
merge (Flink-side) into the roadmap instead.

## 10. Mandate compliance

- **Batch-only / no per-record exec:** adds ride the existing classifier
  batches; LIGHT regime *reduces* engine ops to ≤1/key/barrier; +1 crossing
  per batch for the second CF (constant, §5.2).
- **Zero-copy:** delta bytes are written once into the mixed value column
  (`serializeValueInto` pattern, VectorizedClassifier.java:1276-1287); no new
  per-row byte[] (the 8-byte BE encode goes straight into the column buffer).
- **Arrow-preferred:** mixed batch already uses the Arrow BinaryArray offsets
  layout (lib.rs:3407-3410).
- **Config frozen to ForSt parity:** no engine option changes; one new CF +
  flag-gated behavior.
