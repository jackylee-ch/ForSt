# WA-V1 Flink-side adoption package — default-ON gate program

Date: 2026-06-13 · Owner: write-amplification lane (PMC+RD standing agent)
Engine state at this commit: V0 FFI plumbing + V1 death-bucketed segments
(`FRS_LIFECYCLE_SEGMENTS`, DEFAULT OFF) + PMC-review actions R7 (fixed),
R2 (stamped backpressure ceiling, landed) and R10 (lifecycle state persists
across restore, blob v4, landed). Survey: `docs/superpowers/specs/
2026-06-13-write-path-redesign-survey.md` §9; review: `docs/superpowers/
specs/2026-06-12-wa-v1-lifecycle-segments-pmc-review.md`.

## 1. What to adopt (Flink repo is read-only from this worktree)

| Copy in `wa-java/` | Destination (flink-statebackend-forst-rs) | Kind |
|---|---|---|
| `ForStRsLifecycleManager.java` | `src/main/java/org/apache/flink/state/forstrs/ForStRsLifecycleManager.java` | whole file |
| `ForStRsLinker.lifecycle-fragment.java` | `…/state/forstrs/ffm/ForStRsLinker.java` | 3 MERGE-INTO blocks (field decls, constructor binds, wrapper methods) |

Wiring points (V0, all additive — details in the manager's class doc):
state-register → `setLifecycleForState`/`setLifecycleFromTtlConfig`;
write path → `noteMaxEventTime` at WRITE-BATCH cadence (max of batch,
BEFORE the batch lands — the stamping soundness contract); watermark path →
`advanceWatermark(watermark − allowedLateness)` at watermark cadence.
TTL mapping per state class: windowed aggs (q5/q8/q11) ttl = window size +
allowed lateness; interval join (q7) ttl = upper − lower interval bound;
timer CFs declare `KIND_TIMER` (declared-but-unacted in V1).

## 2. Byte-exact correctness cells (R1 read-filter-contract falsifier, SQL level)

Premature drop is the falsifier for the whole design (review R1/R4): the
engine may only reclaim state the runtime promises never to read. The
recorded q5 lesson (2026-06-02) is that windowed-value bugs MASK under
completion-time/pane-count checks — every cell below is BYTE-EXACT output
comparison against rocksdb on the same seeded 5M datagen, NOT a count check.

Run matrix (same binary, same session, p=1 source where the recorded
harness used it; `FRS_LIFECYCLE_SEGMENTS=1` + adopted wiring vs rocksdb):

| Cell | Query | Why it gates | Pass criterion |
|---|---|---|---|
| C1 | q5 @5M | hopping-window agg — the recorded masking case (2026-06-02 nondeterminism); window state + timers both lifecycle | output byte-identical to rocksdb, 2 consecutive runs |
| C2 | q8 @5M | tumbling-window join — two windowed inputs, drop on either side corrupts the join | byte-identical, 2 runs |
| C3 | q11 @5M | session windows — variable window end stresses the death-stamp bound (stamp from note-before-write, merge-extended sessions) | byte-identical, 2 runs |
| C4 | q5 @5M, allowed-lateness > 0 + late events in source | the watermark−lateness subtraction is the ONLY thing keeping late-event state alive | byte-identical incl. late-firing panes |
| C5 | q5 @5M flag OFF, wiring ON | V0 inertness at SQL level (wiring alone changes nothing) | byte-identical to pre-adoption run |
| C6 | restore mid-run (q5 @5M, checkpoint → cancel → restore) | R10: descriptor+clocks persist (blob v4); post-restore flush must stamp soundly, drops resume | byte-identical to uninterrupted run |

Cell harness pattern: the recorded controlled-replay rig (sorted CSV replay,
byte-compare sink — the 2026-06-02 q5 rig under /tmp/corr-q5) or the
measure-sql.sh seeded-datagen A/B; either qualifies as long as the
comparison is byte-level on sorted output.

## 3. Performance gates (after §2 is green)

| Gate | Bench | Pass criterion |
|---|---|---|
| P1 | remote q7 iostat A/B (survey §6 V1 gate c) | physical write MB/s cut ≥ 3× vs flag-OFF same box; if not, P3 was misattributed → STOP, re-profile before V2 |
| P2 | remote q7 wall | directional: toward the leave-saturation model (~1380–1600 s vs 2376 recorded); no-regress is the hard floor |
| P3 | q5/q8/q11 5M wall | no-regress vs flag-OFF (lifecycle CFs gain drop-instead-of-compact; read fan-out bounded by cohort merge + R2 ceiling) |

## 4. Default-ON disposition (from the PMC review)

Flip `FRS_LIFECYCLE_SEGMENTS` default only when ALL of:
1. §2 C1–C6 green (R1's SQL-level falsifier).
2. R2 ceiling landed — DONE (engine commit "WA-V1 R2", default ceiling
   4× cohort trigger, env `FRS_LIFECYCLE_STAMPED_CEILING`).
3. R10 persistence landed — DONE (blob v4; restore resumes expiry).
4. §3 P1 confirms the write-volume cut transfers to the remote box.

Knobs an operator may need: `FRS_LIFECYCLE_COHORT_TRIGGER` (default 24),
`FRS_LIFECYCLE_STAMPED_CEILING` (default 96), `FRS_LIFECYCLE_DROP_IGNORE_
SNAPSHOTS` (default OFF/conservative), lifecycle-CF memtable sizing
guidance (survey §9.2-1b: ≈ live-window/4-8 where RAM allows).
