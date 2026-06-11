# q9/q20 Long-Scan + RMW-Chain Roadmap — next-lever design

**Date:** 2026-06-12
**Status:** DESIGN + PRIORITIZED ROADMAP (no code changed; written as the Task-B half of the
2026-06-11 adversarial review of the shipped streaming-read / timer-index / Unit-2 stack)
**Binding bar:** q9 and q20 ≤ **1.05× RocksDB** on the single-container 8c/32g 100M topology.
Current (recorded): q9 **2398.5 s** vs RDB 1449.5 (1.65×, bar = ~1522 s, gap −876 s);
q20 **2045.8 s** vs RDB 1034.4 (1.98×, bar = ~1086 s, gap −960 s).

---

## 1. Evidence base (what we now know is, and is not, the gap)

1. **Timer tax is DEAD as an explanation.** The memory-resident timer index (flink
   7590cde1c1f) removed *every* engine read from timer poll/peek
   (`ForStRsKeyGroupedInternalPriorityQueue.poll/peek` serve from the off-heap
   `liveIndex` head, zero FFM on the engine read side) — and q17 and q20 are
   **UNCHANGED**. The prior "timer tax" attribution was box-day noise. q9 verdict
   pending but expected to match. ⇒ q9/q20's gap lives in (a) the engine long-scan
   read path (R-long regime, streaming-read design
   `2026-06-11-streaming-read-redesign-design.md` §1.1/§2) and (b) per-record
   dependent chains.
2. **q20 CPU profile (recorded):** join prefix-scan **21.6 %**, compaction
   **19.3 %**. The prefix-scan share decomposes (design §1.1) into the
   O(sources)-per-key two-phase merge scan (db.rs:10804-10899; the code itself
   predicts ~3× from a tournament tree, db.rs:10690-10699), two `Arc::from`
   heap allocations per emitted SST row (db.rs:10622-10628), and demand-paged
   block fetch. The first two are **NOT addressed by anything shipped** —
   stage S2 of the design (§2.2 pinned rows + loser tree) is still unbuilt.
3. **Compaction is NOT prefetcher-eligible today.** The shipped `BlockPrefetcher`
   (ForSt c8663821e) is wired only into `TierKeySource::Sst` (db.rs:6766/6904).
   Compaction inputs go through `SstBlockCursor::load_next_nonempty`
   (storage/sst/reader.rs:1172-1180): strictly one demand `read_decoded_block`
   per block, serial with the merge, **and inserting every compaction input
   block into the decoded block cache** — q20 burns 19.3 % CPU on a path with
   neither readahead nor I/O–merge overlap, while polluting the cache the join
   probes depend on.
4. **q9/q20 per-record dependent chains (recorded, round-2 sweep doc
   `2026-06-08-8c32g-3backend-sweep-results.md:1270-1274`):** q20's no-UK join
   does a per-record **GET→PUT RMW on the count map** (plus a full asyncEntries
   bucket scan per probe); AEC's KeyAccounting serializes same-key records, so
   the GET is a *dependent* round-trip per record. q9 has the same shape on its
   winning-bid accumulator. The named lever is **OPT-N04**: replace the RMW
   with an engine merge (blind write) — `NumericAddMergeOperator` exists
   (ffi lib.rs:1257, storage merge_operator.rs:221) and the single-crossing
   mixed write batch (`frs_vectorized_batch_mixed`, lib.rs:3445) is shipped and
   wired flag-off (`FRS_RS_MIXED_BATCH`, VectorizedExecutor flink 45cc3ed888c).
5. **The biggest *recorded* lever is not new code at all.** The garbage-drain
   threshold at 200K (opt-in env) produced, deterministically (identical
   out_rows across ≥3 runs): **q9 2378.5 → 2001.2 s (−377 s)** and
   **q20 DNF → 1477.7 s** vs the ~2045.8 default-path baseline (sweep doc
   :1371-1415). It is held opt-in only because the drain gate lacks a third
   condition (a minimum live-volume / level-size floor) and therefore robs
   q17-class small-state queries (>300 s).
6. **The just-shipped streaming-read stack (P0 EOF/auto-close 2045ea577, P1
   parallel first-chunk fill e1aa00bcd, BlockPrefetcher c8663821e, io_uring
   a2ee94e56, Java drain flink 725824ae9e9) has NOT yet been measured on
   q9/q20.** The 2398.5/2045.8 baselines predate it. The prefetcher +
   io_uring directly target the R-long demand-paged block fetch (the 182 µs /
   26 %-cold pread class, cached_fs.rs:712-748) — its win must be measured
   before any further attribution, per the box-noise rule (n≥3).

---

## 2. Levers evaluated, with quantified models

All percentages are models anchored on the recorded shares/numbers above;
they rank levers, they do not predict to the second.

### L0 — Measure the shipped stack + fix review blockers (cost: 0 new perf code)

Run q9/q20 ×3 on the Linux box with today's tip (prefetcher default-ON,
io_uring default-ON on Linux, P0/P1 in the jar). Model: P0/P1 mostly serve
R-short, but the prefetcher's double-buffered windows + io_uring vectored
reads overlap block fetch+decode with the merge on exactly the q9/q20 scan
shape ⇒ **−0 to −10 %** each query; could be more on the evicted tier.
**Blocking review fixes first** (Task-A findings): (H1) pool-worker panic ⇒
permanent worker death ⇒ `rx.recv()` hang in P1 batch open and in
`BlockPrefetcher::next_decoded` (bg_pool.rs:79-93 runs jobs without
`catch_unwind`; prefetch.rs ReadIoPool same pattern; db.rs P1 recv loop has no
timeout) — wrap pool jobs in `catch_unwind` + send-on-panic; (M2) remote
regime ramps after the FIRST block (prefetch.rs `REMOTE_RAMP_AFTER=1`) so
every 1-block probe over an evicted SST issues a speculative 2-block remote
read — make remote ramp require 2 consumed blocks like local (keep the deep
4 MiB cap once ramped) or gate on scan-length hint; (M3) cap aggregate
prefetch memory (tens of sources × 4 MiB ready + 4 MiB inflight per source is
an uncounted ~0.5 GB/iterator worst case on the box that OOM'd at 30.7 GB).

### L1 — Garbage-drain third gate condition → default 200K (recorded win)

~50-LoC gate fix (add a live-volume/level-size floor so q17-class small-state
queries never drain-thrash), then flip the 200K threshold to default.
**Recorded effect: q9 −377 s, q20 −568 s** (deterministic, exact rows).
Model after L1: q9 ≈ 2001 s, q20 ≈ 1478 s.
Gates: q17 @100M no-regress ×3 (the exact query it robbed), q3/q8 no-regress,
exact-rows on q9/q20, suites.

### L2 — OPT-N04: kill the per-record GET→PUT chain via engine merge,
### riding the FRS_RS_MIXED_BATCH flip

Two sub-steps:

**L2a — flip `FRS_RS_MIXED_BATCH` default-on.** Engine + Java are shipped and
tested (engine batch-mixed validation lib.rs:3445-3530; hazard-predicate twins
`DispatchOrderingHazards.requiresOrderedDispatchMixed` route every cross-kind
same-key/prefix conflict to offer-order sync dispatch, so the one semantic
delta — heap merges becoming engine-visible to same-batch iterators — is
confined to batches with no key overlap, which AEC's per-key serialization
already guarantees can't observe each other). Win itself is small (3 crossings
→ 1); it is the *carrier* for L2b.
Gates: off-path byte-identity (already claimed; re-verify via module suite),
mixed-on A/B q3/q8/q17 no-regress, lockstep exactness ×2 + routing-async ×5
(the Stage-0 timer-regression rule), q9/q20 exact-rows.

**L2b — merge-RMW staging (the actual chain killer).** Route the
Reducing/Aggregating count/accumulator states
(`registeredAsyncReducingStates` / `registeredAsyncAggregatingStates` — the
Stage-1 Task-7 regime machinery already in-repo) so that `asyncAdd`-style
updates emit a `MIXED_KIND_MERGE` row instead of GET→combine→PUT. Per record
this removes one *dependent* engine read round-trip and halves the per-key
chain length that KeyAccounting serializes.
**Hard prerequisites (these are the gates):**
1. **CF / operator routing:** the default CF carries `RawConcatMergeOperator`
   (ListState append path); a numeric-add accumulator needs
   `NumericAddMergeOperator` ⇒ count states must live in a CF with the right
   operator (per-state CF registration exists) — one CF cannot have two merge
   operators.
2. **Serialized-form check:** Flink's COUNT/SUM accumulator bytes must equal
   the 8-byte LE i64 the engine operator folds; if the SQL serializer wraps it
   (row header/null bits), ship a matching custom merge operator instead.
   Engage per-state only when the check passes; everything else keeps RMW.
3. **Read path:** GETs over merged keys must full_merge operand chains —
   exists (collect_merge_operands; compaction collapses chains,
   compaction.rs:173-522). RISK: q20 reads the count map per probe; if chains
   between flushes are long, read cost can *worsen* — the q20 A/B is the
   arbiter, and the 5M-no-flush artifact lesson applies (measure only @100M).
4. **Checkpoint/restore with pending Merge ops** in memtable artifacts
   (noflush replay must preserve op types) — add a restore round-trip test.
Model: q20 (chain + bucket-scan dominated per round-2 doc): **−15 to −25 %**;
q9: **−10 to −20 %**.

### L3 — S2: pinned rows + loser tree (design §2.2 — unbuilt stage)

Replace `SstHeadRow {Arc<[u8]>,…}` with `(block-pin, offset/len)` rows (kills
2 allocs/row, db.rs:10622-10628) and the two-phase O(sources) scan with a
loser tree (db.rs:10804-10899; predicted ~3× on the merge step). Directly
attacks q20's 21.6 % prefix-scan share and every other scan consumer.
Model: cut the prefix-scan share ~half ⇒ **q20 −9 to −13 %, q9 −4 to −8 %**.
Gates: storage+engine suites, q0-q22 5M byte-equiv sweep, q4/q3 point-get
no-regress, alloc-profile (per-probe allocs → ~0).

### L4 — Compaction windowed reads + cache policy (the missed 19.3 %)

Wire the already-shipped machinery into `SstBlockCursor`:
(a) multi-block windows via `read_block_regions` (one io_uring submission per
window on Linux, reader.rs:913) double-buffered on the read-I/O pool —
overlaps input I/O+decompress with the compaction merge; (b) **stop inserting
compaction input blocks into the decoded cache** (RocksDB equivalent:
`fill_cache=false` + dedicated 2 MB readahead) — compaction full-file scans
currently both pay insert cost and evict the join's hot set at `Low`.
Model: a third-to-half of the 19.3 % share + second-order join hit-rate gain ⇒
**q20 −6 to −10 %**, q9 −3 to −6 % (q9 compacts heavily too).
Gates: compaction byte-equiv tests, cache hit-rate telemetry on q3/q20,
write-stall behavior unchanged (bounded window memory).

### L5 — P2 pipelined chunk ring + adaptive chunk sizing (design §2.3-P2)

Per-iterator ring of 4 slots, engine producer task fills slot N+1 while Java
drains N; zero crossings steady-state; chunk 64 KiB → 1 MiB adaptive. Post-P0,
q20's join probes mostly auto-close at open (R-short) — the ring applies only
to continuation-surviving iterators, i.e. q9/q11/q19's long partition scans
and q20's fat windows. Model: **q9 −5 to −10 %, q20 −2 to −5 %**.
Gates: new-protocol abort/error matrix (EOF×deferred-error, ring×abort),
iterator-leak watchdog coverage for ring producers, lockstep ×2.

### L6 — P3 SoA chunk layout (design §2.3-P3)

Offsets-array wire format, vectorizable Java decode, Arrow-castable. Model:
−1 to −3 % each. Do last; it rides the same flag machinery as P2.

### Rejected / deferred for this campaign
- **Parallel executor rewrite** — refuted twice (memory: parallel-exec refuted
  2026-06-09; only the build/fill fan-out survived as P1).
- **SST format changes** — design §2.4 verdict stands (readahead supersedes).
- **Timer further work** — q17/q20 invariance proves no headroom there.

---

## 3. The roadmap (ordered; each step independently gated)

| # | Lever | Effort | q9 model | q20 model | Running model (q9 / q20) |
|---|---|---|---|---|---|
| 0 | L0 measure shipped stack + fix H1/M2/M3 | S | −0-10 % | −0-10 % | 2160-2400 / 1840-2046 |
| 1 | L1 drain gate third condition → default 200K | S | −377 s (recorded) | −568 s (recorded) | ~2001 / ~1478 (recorded floors) |
| 2 | L2a mixed-batch flip → L2b OPT-N04 merge-RMW | M | −10-20 % | −15-25 % | 1600-1800 / 1110-1260 |
| 3 | L3 S2 pinned rows + loser tree | M-L | −4-8 % | −9-13 % | 1500-1700 / **990-1130** |
| 4 | L4 compaction windows + cache bypass | M | −3-6 % | −6-10 % | **1430-1600** / 920-1060 |
| 5 | L5 P2 ring (+ L6 P3) — only if q9 still > bar | L | −6-12 % | −3-7 % | 1300-1500 / margin |

**Bars: q9 ≤ 1522 s, q20 ≤ 1086 s.** The model crosses the q20 bar at step 3-4
and the q9 bar at step 3-4, with step 5 as q9 insurance. Steps 1-2 carry ~70 %
of the modeled win and are the cheapest — they double as model falsifiers: if
L1+L2 land < 60 % of their modeled win, STOP and re-profile (the residual
would then be diffuse serde/framework floor, not engine, and further engine
stages would be wasted).

**Sequencing rationale.** L1 first because it is recorded, deterministic and
~50 LoC. L2 before L3/L4 because q20's round-2 profile says the residual gap
is operator-chain dominated, and L2's gates (CF routing, serializer check) are
the long poles — start them early. L3 before L4 because the 21.6 % share is
bigger than the third-of-19.3 % L4 can claim, and L3's byte-equiv gate
machinery (5M sweep) already exists. L5/L6 last: biggest protocol risk,
smallest q20 relevance post-P0.

**Correctness law (unchanged):** every step ships default-OFF behind an env
flag → seeded same-input byte-exactness on the touched queries → n≥3 @100M on
the Linux box → flip default. q9/q20 exact-rows (91,813,372 / 93,201,404 at
the recorded configs) are the regression sentinels.

---

## 4. Expected outcome

- **q9: ~1430-1600 s after steps 0-4** (0.99-1.10× RDB; bar 1522) — bar met at
  the model midpoint; step 5 (P2 ring) is the contingency that buys another
  6-12 %.
- **q20: ~920-1060 s after steps 0-4** (0.89-1.03× RDB; bar 1086) — bar met
  with margin at midpoint; if L2b's per-probe merge-chain read cost eats the
  win (risk #3 above), fall back to count-map caching in the operator regime
  layer and lean on L3+L4, which alone model q20 ≈ 1.15-1.25× — then a second
  profile round (named candidates: lazy probe OPT-N01, per-source parallel
  drain §2.5-axis-2 widening) closes the rest.
