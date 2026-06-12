# PMC review rounds — WA-V1 death-bucketed lifecycle segments

Date: 2026-06-12 · Reviewer: the implementing agent, adversarial pass (work-queue item 3)
Subject: commits 1e4843b89 (V0) + 411708e3b (V1) on `forst-rs` worktree.
Charter: watermark races, restore of segmented CFs, MVCC — plus anything else
that falsifies "premature drop is impossible" or "default path unchanged".
Verdict scale: H = must fix before default-ON; M = must document + test before
default-ON; L = noted.

## Round 1 — correctness of expiry itself

**R1 (M) — resurrection through whole-file drop.** Dropping an expired stamped
segment S removes (a) values and (b) TOMBSTONES in S. If an UNSTAMPED older
file X (mixed-rollup output, pre-flag file, restored legacy) holds an older
version/value of a key whose newer version/tombstone lived only in S, that
older version becomes newest-visible — resurrection. *Why it is sound anyway:*
death stamps are monotone in seq for Windowed CFs, so anything in X shadowed
by S has event-time ≤ the shadowing entry's, hence death ≤ S.max_death < wm —
i.e. every resurrectable row is ITSELF past its death. The system therefore
rests on the **read-filter contract**: the backend never reads state past
`watermark − lateness` (this is exactly what declaring `Windowed{ttl}` means;
ForSt's FlinkCompactionFilter + RocksDB `ignore_snapshots` compaction filters
rest on the same contract). ACTION (pre-default-ON): the q5/q8/q11 byte-exact
cells are the falsifier for the contract at the SQL level; engine-side it is
documented on `SstFileMeta::max_death` and here. NOT a code defect.

**R2 (M) — stalled watermark ⇒ unbounded, backpressure-exempt fan-out.** If
writes continue while the watermark stalls (idle source, broken wiring),
segments accumulate; cohort merge-once bounds FRESH count at ~trigger/2 but
COHORTS accumulate (never re-merged) ⇒ run count grows ~1 per merge batch,
probes degrade along the P9 dose curve, and stamped files are exempt from L0
slowdown so nothing pushes back. imm-count backpressure still bounds RAM.
ACTION (before default-ON): either (a) count stamped runs above a generous
ceiling (e.g. 4× trigger) back into the slowdown trigger, or (b) cohort-of-
cohorts second-level merge. Both small; deferred — flag is default-OFF and
the bench shapes can't exhibit it (watermark = write head by construction).

**R3 (L) — snapshot-defer TOCTOU.** `lifecycle_drop_expired` checks
`active_count() == 0` then applies; a snapshot captured in between still
observes the drop. The conservative default is best-effort narrowing, not a
guarantee; soundness comes from R1's contract (anything dropped is expired).
Documented in the method doc. Closing it fully would need registry
integration (capture-side fence) — not worth it given the contract.

**R4 (PASS) — premature drop.** Stamp = `max_event_time_at_flush + ttl`,
bound advanced BEFORE the rows it covers, monotone ⇒ stamp ≥ true max death
(over-stamp only). Verified by: UT (no drop at wm ≤ stamp incl. equality) +
1.5 M+ live-key point-gets across four 3×90 s bench cells, 0 misses.

## Round 2 — MVCC / structural invariants

**R5 (PASS) — L0 seq-disjointness under cohort merge.** The L0 read order is
reconstructed by sorting on disjoint seq ranges; a cohort output spans its
inputs' ranges. Selection = oldest seq-contiguous prefix of FRESH segments;
cohorts are always older than fresh (merge takes the oldest), expired are
older still and excluded from both, so the prefix is contiguous among ALL of
the CF's L0 files. Defense-in-depth: explicit span-overlap check skips the
merge (warn) instead of corrupting order. Single-file output (the §9.2-1
measured fix) removes the multi-output same-span case entirely.

**R6 (PASS) — cohort merge MVCC.** Reuses `CompactionJob` verbatim:
`min_active_snapshot` honored, `is_bottommost = false` (tombstones never
dropped here — conservative), cf-purity hard-checked by
`check_inputs_single_cf`, apply-side stale-edit validation + orphan cleanup
mirror `compact_l0_for_cf`. Racing edits (flush adds, drops, rollups) resolve
via `Busy` → skip/retry-next-tick; gate slot (cf, L0) serializes against
same-CF rollups and drop_cf's exclusive acquisition.

**R7 (M→FIXED) — `lifecycle_merged` marker leak.** Markers were removed on
lifecycle drop/merge-consume but NOT when stamped files were retired by a
mixed rollup or drop_cf ⇒ slow unbounded HashSet growth. FIXED this round:
`run_lifecycle_maintenance` retains only live file numbers. Residual benign
race: a concurrently-applied cohort's marker can be pruned before its insert
is visible ⇒ that cohort may be re-merged at most once more (same bounded
semantics as the documented restore case).

**R8 (L) — cohort file size.** Single-run outputs can reach GBs for wide
windows (N×64 MB inputs). Streaming writer + block index handle it; read
cost inside one run is binary-searched. Watch on remote (upload burst size);
the memtable-sizing guidance (§9.2-1b) reduces N before merging engages.

## Round 3 — restore / checkpoint of segmented CFs

**R9 (PASS) — stamps round-trip.** Blob v3 carries `max_death`; emitted only
when a stamp exists (no-lifecycle snapshots byte-identical v2 — verified by
UT); v1/v2 decode 0. CRC covers the new bytes.

**R10 (M, documented) — what does NOT persist:** CF lifecycle descriptor,
watermark, event-time bound, merged-markers. Consequences, all bounded:
(a) post-restore drops resume only after the backend re-declares lifecycle +
re-advances the watermark (Flink re-registers state and re-emits watermarks
on restore — standard); (b) a flush before the first `note_max_event_time`
emits stamp 0 ⇒ that segment is immortal and falls back to the classic
rollup path (wasteful, never wrong); (c) restored cohorts may re-merge once.
If (a) ever proves too lazy in practice, persist the descriptor in the CF
table (blob v4) — deferred.

**R11 (PASS) — physical lifetime.** Drops route through
`delete_file_guarded`: checkpoint pins (FileDeletionGuard) and live-version
references defer the unlink; with a FileMappingManager attached the delete is
`unlink()` with refcounts (checkpoint-linked segments survive). Logical
removal is what stops new reads — exactly the compaction-input retirement
discipline, no new machinery.

## Round 4 — default-path safety + performance

**R12 (PASS, measured).** Flag OFF: no stamping (UT), v2 blobs byte-identical
(UT), `backpressure_l0_count` degenerates to `len()`; same-session cell
default-v1post = 7.42 / 542 µs vs 7.35 / 552 baseline — within noise.
Workspace 1417/0; clippy -D warnings clean.

**R13 (L) — ticker cost.** `enqueue_due_lifecycle_maintenance` is O(files)
fast-pathed (no stamped file anywhere ⇒ return) and O(cfs × files) when
lifecycle is active; both trivial at realistic counts. Holding
`lifecycle_merged` while counting per CF is a Mutex on the 1 s ticker — noise.

**R14 (L) — serial compaction gate.** Cohort merges hold the gate across the
merge like rollups do (status-quo parity); in serial mode they serialize with
all compactions of the instance. Concurrent mode (`FRS_COMPACT_CONCURRENT=1`)
scopes to (cf, L0) slots. No new inversion: gate is taken before any lock the
job takes; flush_mutex is never taken on this path.

## Disposition

| ID | Sev | State |
|----|-----|-------|
| R1 resurrection / read-filter contract | M | documented; SQL byte-exact cells owed pre-default-ON |
| R2 stalled-watermark fan-out | M | deferred lever named (stamped-ceiling slowdown or cohort²); default-OFF shields |
| R3 snapshot TOCTOU | L | documented |
| R7 merged-marker leak | M | **fixed this round** |
| R10 non-persisted lifecycle state | M | documented; blob-v4 escape hatch named |
| R8/R13/R14 | L | noted |

No H findings. V1 stays default-OFF until: R1's SQL-level falsifier cells run
green, R2's ceiling lands, and the remote q7 iostat A/B (survey §9.3 gate c)
confirms the write-volume cut transfers.
