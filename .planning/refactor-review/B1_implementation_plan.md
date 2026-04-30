# Refactor-Review v2.1 Infrastructure Update Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Update three refactor-review infrastructure documents (`REVIEW_PROTOCOL.md`, `COMMIT_MANIFEST.md`, `SESSION_HANDOFF.md`) per `A1_reconciliation.md` §3 and §5 (committed at `5b82b8d67`), then prepare C1 R2 dispatch under arch-pivot authority.

**Architecture:** Three sequential doc-edit tasks (each: read current → identify insertion point → apply edit → verify with grep → commit) followed by one cross-doc consistency-check task and one C1 R2 dispatch-preparation task. Doc edits use `grep`-based positive/negative tests in lieu of unit tests since artifacts are markdown; each task's verification step has explicit expected-output values.

**Tech Stack:** `Edit` / `Write` tools for doc edits; `grep` / `awk` / `wc` for verification; `git` for commits; `Agent` tool dispatch deferred to operational execution outside this plan.

---

## Files

| Path | Action | Purpose |
|---|---|---|
| `.planning/refactor-review/REVIEW_PROTOCOL.md` | Modify (insert ~50 lines after current line 75) | Add §B arch-pivot sub-loop section |
| `.planning/refactor-review/COMMIT_MANIFEST.md` | Modify (replace Cn table at lines 15–25) | Add Done Criteria + Estimated Nexmark Contribution columns |
| `.planning/refactor-review/SESSION_HANDOFF.md` | Modify (replace STATE block at lines 63–80; append session-history rows around line 89) | Clear blocker, advance to `B_R2_READY` phase |
| `.planning/refactor-review/C1-arch-pivot-log.md` | Create (empty template) | Ready to receive arch-pivot entries during R2+ |

No code changes. No `Cargo.toml` edits. No crate edits.

---

## Out of Scope (require separate plans)

- **Sub-task d (Nexmark Gate-Zero baseline)** — independent multi-week subsystem (Flink GHA research + harness + 5× measurement runs + sign-off). Needs its own brainstorming + plan round.
- **Sub-task e (GHA rebuild — 6 workflows + `nexmark-baseline.yml`)** — independent CI/CD subsystem; spans Rust + Java + E2E + bench + security + release lanes. Needs its own plan.
- **Actual execution of C1 R2 review rounds** — operational work performed under `REVIEW_PROTOCOL.md`, not a discrete plan; happens session-by-session per the existing protocol.
- **Architectural pivot implementations** — those happen IN response to R2 findings; cannot pre-plan without the findings.

---

## Tasks

### Task 1: Add §B arch-pivot sub-loop to `REVIEW_PROTOCOL.md`

**Files:**
- Modify: `.planning/refactor-review/REVIEW_PROTOCOL.md` (insert ~50 lines after current line 75, after the "Perf gate (Dimension 6 binding)" section, before "Fix-commit discipline")

- [ ] **Step 1.1: Verify current `REVIEW_PROTOCOL.md` has no arch-pivot content (negative test)**

Run: `grep -ci 'arch-pivot\|Architecture-Pivot' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/REVIEW_PROTOCOL.md`

Expected: `0`

- [ ] **Step 1.2: Apply the edit**

Use Edit tool on `/Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/REVIEW_PROTOCOL.md`.

`old_string` (use the existing trailing block as the anchor — find the unique line at end of "Perf gate" section, before "Fix-commit discipline"):

```
If measured throughput < 3x baseline → harness emits `PERF FAIL: bench=X expected=3x actual=2.1x` and exits 1.
Agent 6 treats any such FAIL as **[H]** in the round.

## Fix-commit discipline
```

`new_string`:

```
If measured throughput < 3x baseline → harness emits `PERF FAIL: bench=X expected=3x actual=2.1x` and exits 1.
Agent 6 treats any such FAIL as **[H]** in the round.

## Architecture-Pivot Sub-Loop (per `A1_reconciliation.md` §3)

When a Round-N reviewer raises an `[H]` perf finding of the form
"benchmark X cannot reach 3× under current architecture", this sub-loop activates.
Authoritative spec: `.planning/refactor-review/A1_reconciliation.md` (committed at 5b82b8d67).

### Triage (Tech VP role)

| Class | Definition | Path |
|---|---|---|
| Tuning | Adjustable params, alignment, prefetch, cache size, batch granularity | Standard fix-in-place; regression test added; advance round normally |
| Architectural | Data structure choice, allocation strategy, FFI shape, locking model, layout | Arch-pivot sub-task per below |

### Arch-pivot rules

1. **Module boundary** (A1 §3.3.1): Pivot must stay within current Cn's module boundary
   (column "Scope" in `COMMIT_MANIFEST.md`). E.g., during C1 (`forst-rs-common`) review,
   may rewrite Arena, varint, checksum, metrics, config — but NOT `forst-rs-storage` SST
   format (C3 territory) or `forst-rs-engine` (C7).

2. **Round counter resets to 0** (A1 §3.3.2): On every arch-pivot, the
   `consecutive_clean_rounds` counter resets to 0. Pivoted code is treated as
   "new code"; needs a fresh 10-clean streak from scratch. Rationale: every
   architectural change gets full multi-round scrutiny under all 10 dimensions;
   hidden regressions cannot sneak through on a partially-clean prior streak.
   Schedule cost: ~10–15 round-days per pivot, acknowledged.

3. **Cross-boundary escalation** (A1 §3.3.3): If pivot truly cannot stay inside Cn,
   Tech VP picks per-case at a STOP-and-Confirm with the user:
   - **(i)** Defer the affected bench to Cn+k where the dependency module lives.
     Recorded in `known_issues.md` with rationale + Nexmark-impact estimate.
   - **(ii)** Re-order the C1–C9 sequence to put the dependency commit first.
     Recorded as Tech VP decision in `Cn-arch-pivot-log.md` with new dependency DAG.

### Output artifacts per pivot

- **Pivot commit format**: `arch(Cn): pivot <component> for bench <X> — <one-line reason>`
- **Per-Cn cumulative log**: `.planning/refactor-review/Cn-arch-pivot-log.md`
  Each entry contains:
  - Timestamp (ISO 8601)
  - Original [H] finding (round number, agent, finding text)
  - Root cause (architectural, one paragraph)
  - Redesign approach (one paragraph)
  - Before / after benchmark numbers (raw + ratio vs RocksDB)
  - Post-pivot round count (counter reset to 0)

## Fix-commit discipline
```

- [ ] **Step 1.3: Verify positive — new section landed**

Run: `grep -c 'Architecture-Pivot Sub-Loop' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/REVIEW_PROTOCOL.md`

Expected: `1`

Run: `grep -c 'Round counter resets to 0' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/REVIEW_PROTOCOL.md`

Expected: `1`

Run: `grep -c '5b82b8d67' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/REVIEW_PROTOCOL.md`

Expected: `≥ 1`

- [ ] **Step 1.4: Verify markdown still well-formed (code-fence balance)**

Run: `awk '/^```/{c++} END{print c}' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/REVIEW_PROTOCOL.md`

Expected: even number (every code fence has a matching close)

- [ ] **Step 1.5: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
git add .planning/refactor-review/REVIEW_PROTOCOL.md
git commit -m "$(cat <<'EOF'
docs(planning): REVIEW_PROTOCOL — add §B arch-pivot sub-loop per A1 §3

Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 5b82b8d67

Adds the only protocol-level modification from the v2.1 framework:
- Triage rule (tuning vs architectural)
- Module-boundary constraint for pivots
- Counter-resets-on-pivot rule (correctness over schedule)
- Cross-boundary escalation path (Tech VP per-case)
- Output artifact specs (pivot commit format, per-Cn log)

Resolves: C1 R1 blocker — gives reviewers and Tech VP an explicit
authority path to redesign architecturally-stuck components.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Annotate `COMMIT_MANIFEST.md` with Done Criteria + Nexmark Contribution columns

**Files:**
- Modify: `.planning/refactor-review/COMMIT_MANIFEST.md` (replace 9-row table at lines 15–25)

- [ ] **Step 2.1: Verify current `COMMIT_MANIFEST.md` does NOT have new columns (negative test)**

Run: `grep -ci 'done criteria\|estimated nexmark contribution' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/COMMIT_MANIFEST.md`

Expected: `0`

- [ ] **Step 2.2: Apply edit — replace existing Cn table with annotated version**

Use Edit tool on `/Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/COMMIT_MANIFEST.md`.

`old_string`:

```
| # | ID | Scope | Source commits (forst-rs) | Benchmarks | Perf target |
|---|-----|-------|---------------------------|-----------|-------------|
| C1 | **common** | types, coding, checksum, arena, metrics, config | parts of `forst-rs-common` history | 20 micro: varint, crc32c, arena alloc | ≥3x RocksDB util |
| C2 | **io** | LocalFS, MemoryFS, AsyncIO, ObjectStore, Router | `forst-rs-io` parts | 15: read/write/seek, prefetch, async | ≥3x Flink FS |
| C3 | **storage: sst** | SST writer/reader, bloom (SBBF), index, compression, block_header, footer, schema | `22290c5` + sst/* from earlier | 30: seq/random read/write, compressed vs uncompressed, bloom FPR | **≥3x RocksDB SST** (scan) / ≥1x (point) |
| C4 | **storage: memtable + merge** | VectorizedMemTable, ListAppendMergeOperator | `136dde0`, `2bade9e`, memtable history | 20: insert/lookup/merge-chain resolution, Arrow batch vs SkipList | **≥3x RocksDB skiplist** |
| C5 | **storage: cache** | ShardedClockCache (16-shard, Clock eviction) | `89afd1c` | 15: hit/miss/insert/shard contention | **≥3-5x LRU**, ≥3x RocksDB block cache |
| C6 | **storage: version + checkpoint** | VersionSet (ArcSwap), VersionEdit, checkpoint blob | `37453b7` | 10: version switch, edit apply, checkpoint roundtrip | ≥5x RocksDB manifest |
| C7 | **engine: core** | DbImpl, ColumnFamily, WriteBatch, SnapshotView, WriteController, read/write/merge paths | `007d74f` (W12) | 40 E2E: put/get/delete/merge, batch × small/large, concurrent | **≥3-5x RocksDB** (batch/scan); ≥1x point |
| C8 | **engine: background** | Flush, Compaction (incl. SingleDelete conservative elision), CompactionFilter, FileDeletionGuard | `b595036` (W13-W15) + `aa61712` R3 fix | 20: flush throughput, compaction rate, TTL filter | ≥3x RocksDB |
| C9 | **FFI + Arrow + bench infra** | frs_* C ABI, Arrow C Data Interface zero-copy, forst-rs-bench scaffolding | `ee12651` (W16) + all `fix(ffi,*)` from R1-R2 | 25: FFI overhead, Arrow batch put/get, prefix_scan | ≥5x JNI |
```

`new_string`:

```
| # | ID | Scope | Source commits (forst-rs) | Benchmarks | Perf target | Done Criteria | Estimated Nexmark Contribution |
|---|-----|-------|---------------------------|-----------|-------------|---------------|--------------------------------|
| C1 | **common** | types, coding, checksum, arena, metrics, config | parts of `forst-rs-common` history | 20 micro: varint, crc32c, arena alloc | ≥3x RocksDB util | All 13+ benches in `forst-rs-bench/benches/C1/` pass `perf_gate` ≥3x; `H=0 ∧ M=0` × 10 consecutive rounds | Indirect: foundational utilities; perf wins compose into all queries. No direct query-gating |
| C2 | **io** | LocalFS, MemoryFS, AsyncIO, ObjectStore, Router | `forst-rs-io` parts | 15: read/write/seek, prefetch, async | ≥3x Flink FS | All benches in `forst-rs-bench/benches/C2/` pass `perf_gate`; `H=0 ∧ M=0` × 10 | Direct: improves remote-storage read/write throughput. Primarily impacts queries with high remote-state IO (q11–q14 windowed joins, q16 session-window) |
| C3 | **storage: sst** | SST writer/reader, bloom (SBBF), index, compression, block_header, footer, schema | `22290c5` + sst/* from earlier | 30: seq/random read/write, compressed vs uncompressed, bloom FPR | **≥3x RocksDB SST** (scan) / ≥1x (point) | All benches pass; SST disk-format roundtrip lossless; `H=0 ∧ M=0` × 10 | Direct: gates all stateful queries. Largest single contributor to E2E speedup; impacts q0 through q22 |
| C4 | **storage: memtable + merge** | VectorizedMemTable, ListAppendMergeOperator | `136dde0`, `2bade9e`, memtable history | 20: insert/lookup/merge-chain resolution, Arrow batch vs SkipList | **≥3x RocksDB skiplist** | All benches pass; ListAppend merge semantics verified against RocksDB; `H=0 ∧ M=0` × 10 | Direct: gates write-heavy queries. Impacts q5 (hot bidder), q7 (highest bid), q8 (monitor new users) |
| C5 | **storage: cache** | ShardedClockCache (16-shard, Clock eviction) | `89afd1c` | 15: hit/miss/insert/shard contention | **≥3-5x LRU**, ≥3x RocksDB block cache | All benches pass; cache concurrency tested under 16+ threads; `H=0 ∧ M=0` × 10. **Also gated by Nexmark `baseline.json` existence per A1 §4.3** | Direct: gates queries with high temporal locality. Impacts q0, q1, q2 (recent state), q5 (hot bidder), q11 (session windowing) |
| C6 | **storage: version + checkpoint** | VersionSet (ArcSwap), VersionEdit, checkpoint blob | `37453b7` | 10: version switch, edit apply, checkpoint roundtrip | ≥5x RocksDB manifest | All benches pass; checkpoint roundtrip lossless including empty/large states; `H=0 ∧ M=0` × 10 | Indirect: improves checkpoint latency, not steady-state query latency. Impacts E2E recovery time, not q0–q22 throughput |
| C7 | **engine: core** | DbImpl, ColumnFamily, WriteBatch, SnapshotView, WriteController, read/write/merge paths | `007d74f` (W12) | 40 E2E: put/get/delete/merge, batch × small/large, concurrent | **≥3-5x RocksDB** (batch/scan); ≥1x point | All benches pass; cross-CF isolation verified; concurrent put/get under 16+ threads; `H=0 ∧ M=0` × 10 | Direct: gates all queries via batch path. Largest single E2E win expected; impacts every query |
| C8 | **engine: background** | Flush, Compaction (incl. SingleDelete conservative elision), CompactionFilter, FileDeletionGuard | `b595036` (W13-W15) + `aa61712` R3 fix | 20: flush throughput, compaction rate, TTL filter | ≥3x RocksDB | All benches pass; flush+compaction concurrent stress test; TTL filter correctness; `H=0 ∧ M=0` × 10 | Indirect: keeps steady-state perf from regressing under load. Long-running queries (q11 session, q14 windowed joins) most affected |
| C9 | **FFI + Arrow + bench infra** | frs_* C ABI, Arrow C Data Interface zero-copy, forst-rs-bench scaffolding | `ee12651` (W16) + all `fix(ffi,*)` from R1-R2 | 25: FFI overhead, Arrow batch put/get, prefix_scan | ≥5x JNI | All benches pass; FFI roundtrip lossless across types; Arrow zero-copy verified (no memcpy); `H=0 ∧ M=0` × 10. **Gated by C5 (Nexmark baseline) per A1 §4.3** | Direct via Arrow path: gates Java↔Rust boundary throughput. Impacts every query that crosses FFI (i.e., all of them) |
```

- [ ] **Step 2.3: Verify positive — both new columns present**

Run: `grep -c 'Done Criteria' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/COMMIT_MANIFEST.md`

Expected: `1` (header row only)

Run: `grep -c 'Estimated Nexmark Contribution' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/COMMIT_MANIFEST.md`

Expected: `1`

Run: `grep -cE 'Indirect|Direct' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/COMMIT_MANIFEST.md`

Expected: `≥ 9` (one per Cn row)

- [ ] **Step 2.4: Verify all 9 Cn rows still present**

Run: `grep -cE '^\| C[1-9] ' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/COMMIT_MANIFEST.md`

Expected: `9`

- [ ] **Step 2.5: Verify A1 spec referenced for C5/C9 gating**

Run: `grep -c 'A1 §4.3' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/COMMIT_MANIFEST.md`

Expected: `2` (one in C5 row, one in C9 row)

- [ ] **Step 2.6: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
git add .planning/refactor-review/COMMIT_MANIFEST.md
git commit -m "$(cat <<'EOF'
docs(planning): COMMIT_MANIFEST — annotate Cn table with Done Criteria + Nexmark contribution

Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 5b82b8d67

Adds two columns:
- Done Criteria per Cn: explicit termination signature
  (perf_gate pass + H=0 ∧ M=0 × 10)
- Estimated Nexmark Contribution per Cn: query-set + direct/indirect
  classification

Cn entries reflect existing per-commit perf targets (3-5x vs RocksDB)
and are unchanged from prior manifest. C5 and C9 gain explicit
'gated by Nexmark baseline.json existence' note per A1 §4.3.

Nexmark contribution column is best-current-estimate; will be refined
once .planning/nexmark/baseline.json lands (separate plan, sub-task d).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Update `SESSION_HANDOFF.md` STATE block

**Files:**
- Modify: `.planning/refactor-review/SESSION_HANDOFF.md` (replace STATE block at lines 63–80; append session-history rows)

- [ ] **Step 3.1: Verify current STATE has the blocker (negative test)**

Run: `grep -c 'CRITICAL_BLOCKER.*MUST AskUserQuestion' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md`

Expected: `1`

- [ ] **Step 3.2: Replace STATE yaml block**

Use Edit tool on `/Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md`.

`old_string` (the existing yaml block bounded by triple backticks, found between `## STATE` heading and `## Session history` heading — match the unique opening `last_updated: 2026-04-25T05:30:00Z`):

```
last_updated: 2026-04-25T05:30:00Z
last_session_id: B+
phase: B_IN_PROGRESS_BLOCKED
current_commit: C1
current_commit_sha: 68c0bd464
current_round: 1
round_1_status: ALL_10_AGENTS_COMPLETE_AGGREGATED
round_1_findings_file: .planning/refactor-review/C1-R1-findings.md
consecutive_clean: 0
baselines_built: false
r1_totals: "H=26, M=52, L=55 (10/10 agents FINAL)"
CRITICAL_BLOCKER: "User's hard 3x-vs-RocksDB target proved architecturally impossible for 7 of 13 C1 benchmarks. Next session MUST AskUserQuestion before fixing."
c1_agents_ids:
  completed: [a315094bb0c2a8251, a2d4de574d4fbf736, a1f6a0849a1a1375a, af2d68d6c4b38ee8d, aed0445efd7332014, aba5767e53bb42477, ab0a8aac6094f9a24, a978e1c69634eee26, aa7ead384e348c0d0]
  pending: [afecebf860d223f09]  # A5 Errors
next_action: "Session C: (1) AskUserQuestion on 3x target regime (tiered vs hard vs abandon); (2) collect A5; (3) apply fixes per user decision; (4) commit fix; (5) Round 2"
```

`new_string`:

```
last_updated: 2026-04-30T11:30:00Z
last_session_id: D
phase: B_R2_READY
authoritative_spec: ".planning/refactor-review/A1_reconciliation.md @ 5b82b8d67"
arch_pivot_authority: ACTIVE
current_commit: C1
current_commit_sha: 68c0bd464
current_round: 1  # R1 done; R2 ready to dispatch
round_1_status: TRIAGED_R2_READY_TO_DISPATCH
round_1_findings_file: .planning/refactor-review/C1-R1-findings.md
consecutive_clean: 0
baselines_built: false  # blocked on C5 dependency per A1 §4.3; not blocking C1 R2
r1_totals: "H=26, M=52, L=55 (10/10 agents FINAL — A5 included)"
CRITICAL_BLOCKER: RESOLVED  # via A1 §3 arch-pivot sub-loop (user-confirmed 2026-04-30)
c1_agents_ids:
  completed: [a315094bb0c2a8251, a2d4de574d4fbf736, a1f6a0849a1a1375a, af2d68d6c4b38ee8d, aed0445efd7332014, aba5767e53bb42477, ab0a8aac6094f9a24, a978e1c69634eee26, aa7ead384e348c0d0, afecebf860d223f09]
  pending: []
expected_r2_focus: |
  Re-review C1 with arch-pivot authority active. Per C1-R1-findings.md
  Agent 6 architectural analysis, 9+ benches that previously could not
  reach 3x (CRC32C, Counter inc, Gauge set, Histogram observe, Varint
  encode, Varint decode, Fixed32 encode, Fixed64 encode, Arena) will
  surface as architectural [H] findings in R2.
  - For each: triage tuning vs architectural per REVIEW_PROTOCOL §B.
  - If architectural: fork arch-pivot sub-task within forst-rs-common
    boundary (NOT crossing into forst-rs-storage or other crates).
  - On every arch-pivot landing: counter resets to 0; new 10-clean
    streak starts. Schedule cost ~10-15 round-days per pivot accepted.
next_action: |
  Session E: Dispatch C1 R2 with arch-pivot authority active.
  (1) Verify all infrastructure updates are in HEAD (REVIEW_PROTOCOL §B,
      COMMIT_MANIFEST Done Criteria + Nexmark columns, this STATE update,
      C1-arch-pivot-log.md template).
  (2) Launch 10 parallel review agents per REVIEW_PROTOCOL.md (10 fixed
      dimensions; agent-id pattern continues from c1_agents_ids).
  (3) Aggregate R2 findings to .planning/refactor-review/C1-R2-findings.md.
  (4) Triage R2 H findings per REVIEW_PROTOCOL §B (tuning vs architectural).
  (5) For architectural: arch-pivot per A1 §3.3 (counter resets);
      log entry in C1-arch-pivot-log.md;
      for tuning: standard fix-in-place.
  (6) Commit fixes; update STATE (last_updated, current_round,
      consecutive_clean); advance round.
```

- [ ] **Step 3.3: Append session-history rows**

Use Edit tool on the same file. `old_string`:

```
| C | TBD | Phase B | C1 continue | 1→? | Aggregate R1, fix H/M, continue rounds |
| ... | | | | | |
```

`new_string`:

```
| C | 2026-04-30 | Phase B | Brainstorming | 1 | A1 reconciliation spec written + committed at 5b82b8d67; arch-pivot authority granted; user confirmed correctness-over-schedule trade-off |
| D | 2026-04-30 | Phase B | Infra update | 1 | REVIEW_PROTOCOL §B added, COMMIT_MANIFEST annotated, SESSION_HANDOFF STATE updated, C1-arch-pivot-log template created |
| E | TBD | Phase B | C1 R2 | 1→2 | Dispatch 10 review agents under v2.1 protocol; first round under arch-pivot authority |
| ... | | | | | |
```

- [ ] **Step 3.4: Verify STATE update landed (positive tests)**

Run: `grep -c 'phase: B_R2_READY' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md`

Expected: `1`

Run: `grep -c 'CRITICAL_BLOCKER: RESOLVED' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md`

Expected: `1`

Run: `grep -c 'arch_pivot_authority: ACTIVE' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md`

Expected: `1`

Run: `grep -c '5b82b8d67' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md`

Expected: `≥ 2` (authoritative_spec field + session-history row C)

- [ ] **Step 3.5: Verify negative — old blocker text removed**

Run: `grep -c 'MUST AskUserQuestion\|architecturally impossible' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md`

Expected: `0`

- [ ] **Step 3.6: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
git add .planning/refactor-review/SESSION_HANDOFF.md
git commit -m "$(cat <<'EOF'
docs(planning): SESSION_HANDOFF — clear C1 blocker, advance to B_R2_READY

Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 5b82b8d67

STATE block changes:
- phase: B_IN_PROGRESS_BLOCKED → B_R2_READY
- CRITICAL_BLOCKER: <perf-target-unachievable text> → RESOLVED
  (resolved via A1 §3 arch-pivot sub-loop)
- arch_pivot_authority: ACTIVE (new field)
- authoritative_spec: pointer to A1_reconciliation.md @ 5b82b8d67 (new field)
- expected_r2_focus: enumeration of expected architectural findings (new field)
- next_action: rewritten — describes R2 dispatch and triage flow

Session history: appends rows for sessions C (brainstorming),
D (this infra update), E (TBD, C1 R2 dispatch).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Cross-doc consistency verification

**Files:**
- Read-only: all three modified docs + `.planning/refactor-review/A1_reconciliation.md`

- [ ] **Step 4.1: Cross-check arch-pivot semantics across docs**

Run all three commands:

```bash
grep -A2 'counter resets' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/REVIEW_PROTOCOL.md
grep -A2 'counter resets\|§B.2' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/A1_reconciliation.md
grep 'arch_pivot_authority' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md
```

Expected: all three references agree on "counter resets to 0 on every arch-pivot". `arch_pivot_authority: ACTIVE` present in SESSION_HANDOFF.

- [ ] **Step 4.2: Cross-check Cn module boundaries in MANIFEST and PROTOCOL**

Run:

```bash
grep -cE '^\| C[1-9] ' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/COMMIT_MANIFEST.md
grep -E 'forst-rs-common|forst-rs-storage|C3 territory|C7' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/REVIEW_PROTOCOL.md
```

Expected: `9` Cn rows in MANIFEST; PROTOCOL §B references `forst-rs-common`, "C3 territory", "C7" (the boundary examples).

- [ ] **Step 4.3: Verify no orphan references to old state**

Run: `grep -i 'MUST AskUserQuestion\|architecturally impossible\|B_IN_PROGRESS_BLOCKED' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/*.md | grep -v A1_reconciliation.md`

Expected: NO output (old state strings only allowed inside A1 spec which references them historically).

- [ ] **Step 4.4: Verify all three updated docs reference A1 spec by SHA**

Run: `for f in REVIEW_PROTOCOL COMMIT_MANIFEST SESSION_HANDOFF; do echo -n "$f: "; grep -c '5b82b8d67' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/$f.md; done`

Expected: each file `≥ 1`. (REVIEW_PROTOCOL: 1 in §B header. COMMIT_MANIFEST: 0 — fix in step 4.5 if missing. SESSION_HANDOFF: 2.)

If `COMMIT_MANIFEST.md = 0`: Edit to add `Authoritative spec: A1_reconciliation.md @ 5b82b8d67` line right under the table title heading. Re-run.

- [ ] **Step 4.5: Verify expected_r2_focus enumerates the architecturally-stuck benches**

Run: `grep -A1 'expected_r2_focus' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md | grep -E 'CRC32C|Varint|Counter|Histogram|Arena'`

Expected: at least 3 of those bench names present in `expected_r2_focus` block.

- [ ] **Step 4.6: STOP — present cross-check results to user**

If any of 4.1–4.5 fails: stop, fix, re-run that check. Do not advance to Task 5 with broken cross-references.

If all pass: report all-clear to user with counts:
- "Task 1 commit: <sha>; Task 2 commit: <sha>; Task 3 commit: <sha>"
- "Cross-doc consistency: all 5 checks pass"
- "Ready to advance to Task 5 (C1 arch-pivot log template + R2 dispatch prep)"

---

### Task 5: Schedule C1 R2 dispatch (sub-task f kickoff)

**Files:**
- Create: `.planning/refactor-review/C1-arch-pivot-log.md` (empty template)

- [ ] **Step 5.1: Create C1 arch-pivot log template**

Use Write tool. Path: `/Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/C1-arch-pivot-log.md`

Content:

````markdown
# C1 — Arch-Pivot Cumulative Log

Per `A1_reconciliation.md` §3.4, every arch-pivot during C1 review adds an entry below.

Authoritative spec: `.planning/refactor-review/A1_reconciliation.md` @ 5b82b8d67.
Protocol section: `.planning/refactor-review/REVIEW_PROTOCOL.md` "Architecture-Pivot Sub-Loop".

## Pivot entries

(none yet — C1 R2 not yet dispatched)

## Entry template (copy below for each new pivot)

```
### Pivot N: <component> for bench <X>

- **Timestamp**: <ISO 8601, e.g. 2026-05-15T14:23:00Z>
- **Triggered by**: Round <N> [H] finding from Reviewer-<K>
- **Original finding text**: "<verbatim from C1-R<N>-findings.md>"
- **Root cause** (architectural):
  <one paragraph; what about the current architecture makes 3x impossible>
- **Redesign approach**:
  <one paragraph; new structure / strategy>
- **Before / after benchmark numbers**:
  - Before: bench <X> @ <Y>x vs RocksDB
  - After: bench <X> @ <Y>x vs RocksDB
- **Pivot commit**: `<sha>`
- **Round counter post-pivot**: 0 (per A1 §3.3.2 / REVIEW_PROTOCOL §B rule 2)
- **Cross-boundary?**: no / yes
  - if yes: escalation rationale + Tech VP decision link to known_issues.md or
    Cn-arch-pivot-log decision entry
```
````

- [ ] **Step 5.2: Verify created**

Run: `test -f /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/C1-arch-pivot-log.md && echo OK`

Expected: `OK`

Run: `grep -c 'Entry template' /Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/C1-arch-pivot-log.md`

Expected: `1`

- [ ] **Step 5.3: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
git add .planning/refactor-review/C1-arch-pivot-log.md
git commit -m "$(cat <<'EOF'
docs(planning): C1 arch-pivot log template — ready for R2 entries

Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 5b82b8d67

Empty template per A1 §3.4. Each arch-pivot during C1 R2+ appends an entry
documenting: trigger finding, root cause, redesign approach, before/after
bench numbers, pivot commit SHA, post-pivot round count.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 5.4: Compose R2 dispatch prompt template (do NOT dispatch yet)**

This step produces a draft prompt to be used in the next session when actually dispatching the 10 review agents. Save as draft text inside this plan's PR description / handoff notes; do NOT commit a separate file.

Template:

```
Round 2 Agent K reviewing commit 68c0bd464 at ~/code/github/ForSt-review/.
Dimension: <name from REVIEW_PROTOCOL.md "Agent dimensions">.

Read: git show 68c0bd464. Focus files: forst-rs-common/* (full crate).
Benchmarks produced by C1 live in forst-rs-bench/benches/C1/.

NEW IN R2: §B Architecture-Pivot Sub-Loop is ACTIVE per
.planning/refactor-review/REVIEW_PROTOCOL.md "Architecture-Pivot Sub-Loop".

When raising an [H] perf finding, classify as:
  - Tuning: parameter / alignment / prefetch / cache size
  - Architectural: data structure / allocation / FFI / locking / layout

Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 5b82b8d67

OUTPUT (strict — same as R1):
## Agent K — <dim> — Round 2 — C1
### Findings
- [H/M/L] title (file:line): desc; reproduction if applicable
  - For [H] perf findings: include classification "tuning" or "architectural"
### Summary
H: n, M: n, L: n
```

- [ ] **Step 5.5: STOP — present plan completion to user**

This is the terminal step. Items completed in this plan:

| Sub-task | Implementation |
|---|---|
| (a) REVIEW_PROTOCOL §B added | Task 1 |
| (b) SESSION_HANDOFF STATE updated | Task 3 |
| (c) COMMIT_MANIFEST columns added | Task 2 |
| Cross-doc consistency verified | Task 4 |
| (f) C1 R2 dispatch prepared (log template + prompt template) | Task 5 |

Items NOT in this plan (require separate brainstorming + plans, per scope-out):

| Sub-task | Reason out-of-scope |
|---|---|
| (d) Nexmark Gate-Zero | Multi-week subsystem (Flink GHA research + harness + measurements + sign-off). Independent. |
| (e) GHA rebuild | 6 workflows + nexmark-baseline.yml across Rust/Java/E2E/bench/security/release lanes. Independent. |
| Actual C1 R2 round execution | Operational work performed under existing REVIEW_PROTOCOL.md, not a discrete plan. |

Tell user verbatim:
> "Infrastructure updates landed in 4 commits. Three independent next-step lanes are now unblocked, each with their own plan-needs:
>
> 1. **C1 R2 dispatch** (sub-task f, operational): launch 10 parallel review agents per REVIEW_PROTOCOL.md and `expected_r2_focus` in SESSION_HANDOFF STATE. No new plan needed — uses existing protocol.
>
> 2. **Nexmark Gate-Zero** (sub-task d): needs new brainstorming + plan round. Recommendation: invoke `superpowers:brainstorming` to scope the Flink GHA research path before any harness work.
>
> 3. **GHA rebuild** (sub-task e): needs new brainstorming + plan round. Recommendation: scope each of the 6 workflows + nexmark-baseline.yml in its own brainstorming session, since each touches different subsystems.
>
> Lanes 2 and 3 can run parallel with lane 1. Lane 2 must complete before C5 review starts (per A1 §4.3)."

---

## Self-Review

Run inline before saving (per writing-plans skill):

**1. Spec coverage:**

| A1 spec section | Plan task |
|---|---|
| §3 (arch-pivot sub-loop) | Task 1 ✓ |
| §5 row B (annotate MANIFEST) | Task 2 ✓ |
| §5 row D (resume D loop) | Task 5 (kickoff prep) ✓ |
| §9.a REVIEW_PROTOCOL update | Task 1 ✓ |
| §9.b SESSION_HANDOFF update | Task 3 ✓ |
| §9.c COMMIT_MANIFEST annotation | Task 2 ✓ |
| §9.d Nexmark Gate-Zero | Out-of-scope, called out in "Out of Scope" + Task 5.5 ✓ |
| §9.e GHA rebuild | Out-of-scope, called out ✓ |
| §9.f C1 R2 schedule | Task 5 (template + prompt prep) ✓ |
| §3.4 per-Cn arch-pivot log | Task 5.1 ✓ |
| §4.3 C5 gating by Nexmark | Task 2 (C5 row Done Criteria) ✓ |

**2. Placeholder scan:** No "TBD" / "TODO" / "implement later" inside steps. The C1-arch-pivot-log.md "Entry template" section uses `<placeholder>` syntax, but that's the *template literal* itself, not a missing requirement. The MANIFEST C5/C9 rows reference "Nexmark baseline.json" which lands in a future plan — flagged in Done Criteria text and called out in Out-of-Scope.

**3. Type/identifier consistency:**

| Identifier | Usage |
|---|---|
| `arch_pivot_authority: ACTIVE` (yaml field) | Task 3 STATE block + Task 4.1 verification |
| `phase: B_R2_READY` (yaml value) | Task 3 STATE block + Task 4 verification |
| `5b82b8d67` (spec SHA) | Tasks 1, 2, 3, 4, 5 — referenced consistently |
| `Architecture-Pivot Sub-Loop` (section heading) | Task 1 inserted text + Task 4.1 grep |
| `arch(Cn): pivot ...` (commit format) | Task 1 §B output artifacts + Task 5.1 log entry template |
| `consecutive_clean_rounds` (counter name) | Task 1 §B rule 2 + Task 3 STATE block (`consecutive_clean: 0`) |

All consistent. No drift.

**4. Plan size:** 5 tasks, 26 steps total. Each step is 2-5 minutes. Total estimated execution time: ~2.5 hours by a human; ~30 minutes by a parallel-agent executor. Reasonable plan size.

Self-review pass: ✅. No fixes required.

---

## Reference Files

- `/Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/A1_reconciliation.md` (committed at `5b82b8d67`) — authoritative spec this plan implements
- `/Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/REVIEW_PROTOCOL.md` — Task 1 target
- `/Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/COMMIT_MANIFEST.md` — Task 2 target
- `/Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md` — Task 3 target
- `/Users/lijunqing/Code/stczwd/ForSt/.planning/refactor-review/C1-R1-findings.md` — source of truth for `expected_r2_focus` enumeration in Task 3.2
