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
last_updated: 2026-04-25T04:30:00Z
last_session_id: B
phase: B_IN_PROGRESS
current_commit: C1
current_commit_sha: 68c0bd464  # commit with C1 benchmarks added on review-loop
current_round: 1
round_1_status: agents_launched_awaiting_results
consecutive_clean: 0
baselines_built: false  # using reference numbers from baseline.json
baselines_deferred_to: "user or background task per BUILD_STATUS.md"
c1_benchmarks_added: 13  # of 20 planned; 7 TODO for next session
c1_agents_ids:
  - a315094bb0c2a8251  # A1 Memory
  - a2d4de574d4fbf736  # A2 Correctness
  - a1f6a0849a1a1375a  # A3 Concurrency
  - af2d68d6c4b38ee8d  # A4 Test coverage
  - afecebf860d223f09  # A5 Error handling
  - aed0445efd7332014  # A6 Performance
  - aba5767e53bb42477  # A7 Docs
  - ab0a8aac6094f9a24  # A8 Rust idiomatic
  - a978e1c69634eee26  # A9 Security
  - aa7ead384e348c0d0  # A10 Integration
next_action: "Wait for 10 agents to complete (~8-10 min), aggregate H/M findings, fix H+M, commit fix, start Round 2"
```

## Session history

| Session | Date | Phase | Commit | Rounds | Status |
|---------|------|-------|--------|--------|--------|
| A | 2026-04-25 | Phase A | infra | N/A | 4 planning docs written |
| B | 2026-04-25 | Phase B | C1 setup + R1 launch | 0→1 | worktree created, 13 benchmarks added, Round 1 agents launched |
| C | TBD | Phase B | C1 continue | 1→? | Aggregate R1, fix H/M, continue rounds |
| ... | | | | | |

## Risk register

1. **3-5x perf KPI not achievable for all benchmarks**: per user directive 2026-04-25 conv., hard target stands. Expect some Cn to stall indefinitely at 120 rounds with M-level perf issues. Accept 120-round cap as fallback exit.
2. **Baseline build broken**: pin RocksDB v8.11.3; if build fails on target host, fall back to Docker image `facebookresearch/rocksdb:v8.11.3`.
3. **Context exhaustion mid-commit**: STATE block must be updated BEFORE running risky long tool calls. Every 5 rounds → snapshot STATE.
4. **Agent output parse failures**: REVIEW_PROTOCOL.md mandates strict "H: n, M: n, L: n" final line. Enforcer: any agent output lacking this line → rerun that agent (up to 2 retries).
