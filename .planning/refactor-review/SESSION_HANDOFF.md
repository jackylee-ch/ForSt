# Session Handoff — State Machine

## Current state (as of 2026-04-25, session A finished)

```
phase: A (infrastructure done)
worktree: NOT YET CREATED
current_commit: none
current_round: 0
consecutive_clean: 0
baselines_built: false
```

## Next session (B) should execute

### Immediate actions (first 30 min)

```bash
# 1. Create worktree
cd ~/code/github/ForSt
git worktree add ../ForSt-review -b review-loop forst-rs

# 2. Verify
cd ~/code/github/ForSt-review
git log --oneline -1  # should show 4b0099c59

# 3. Reset to pre-refactor base for clean replay
# (discuss with user: reset to main branch? keep forst-rs tip?)
# Default: keep forst-rs tip; Cn review runs on-top.

# 4. Build baselines
mkdir -p ~/code/baselines
cd ~/code/baselines
git clone --depth 1 --branch v8.11.3 https://github.com/facebook/rocksdb
cd rocksdb && make static_lib -j$(nproc)
```

### Per-commit loop (semi-automated)

```
for commit_id in C1 C2 C3 C4 C5 C6 C7 C8 C9:
    run session with:
        "Continue refactor-review for <commit_id>.
         Read .planning/refactor-review/*.md.
         Current state in this file below.
         Execute: ensure Cn committed → add benches → round N review.
         Stop when consecutive_clean=10 or round=120.
         At session end, update STATE below."
```

## Session B-N: per-session work item

Each session should:
1. Read this STATE block → resume where left off
2. Launch 10 parallel agents (ref REVIEW_PROTOCOL.md)
3. Extract findings → fix H/M → commit → verify
4. Advance round count
5. Update STATE block before context runs out
6. Emit continuation prompt for next session

## STATE (update this at end of every session)

```yaml
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

## Session history

| Session | Date | Phase | Commit | Rounds | Status |
|---------|------|-------|--------|--------|--------|
| A | 2026-04-25 | Phase A | infra | N/A | 4 planning docs written |
| B | 2026-04-25 | Phase B | C1 setup + R1 launch | 0→1 | worktree created, 13 benchmarks added, Round 1 agents launched |
| C | 2026-04-30 | Phase B | Brainstorming | 1 | A1 reconciliation spec written + committed at 5b82b8d67; arch-pivot authority granted; user confirmed correctness-over-schedule trade-off |
| D | 2026-04-30 | Phase B | Infra update | 1 | REVIEW_PROTOCOL §B added, COMMIT_MANIFEST annotated, SESSION_HANDOFF STATE updated, C1-arch-pivot-log template created |
| E | TBD | Phase B | C1 R2 | 1→2 | Dispatch 10 review agents under v2.1 protocol; first round under arch-pivot authority |
| ... | | | | | |

## Risk register

1. **3-5x perf KPI not achievable for all benchmarks**: per user directive 2026-04-25 conv., hard target stands. Expect some Cn to stall indefinitely at 120 rounds with M-level perf issues. Accept 120-round cap as fallback exit.
2. **Baseline build broken**: pin RocksDB v8.11.3; if build fails on target host, fall back to Docker image `facebookresearch/rocksdb:v8.11.3`.
3. **Context exhaustion mid-commit**: STATE block must be updated BEFORE running risky long tool calls. Every 5 rounds → snapshot STATE.
4. **Agent output parse failures**: REVIEW_PROTOCOL.md mandates strict "H: n, M: n, L: n" final line. Enforcer: any agent output lacking this line → rerun that agent (up to 2 retries).
