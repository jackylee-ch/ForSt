# Levers-ON e2e NexMark validation plan — the "beat BOTH RocksDB and ForSt" proof

**Status:** READY TO FIRE (script + config + protocol prepared; NO NexMark run performed
to author this doc). **Author:** PMC-1 (Phase-1 query perf). **Date:** 2026-06-15.

**Why this doc exists.** The Phase-1 query-perf BUILD is exhausted — every priority-query
lever is shipped in the engine (`origin/forst-rs`) and the Flink backend (branch
`readside-r2a`). What is NOT yet done is the **full-stack-ON e2e measurement**: turning
on, per query, *all* the levers that query wants *together* and proving whether the
priority queries beat BOTH baselines (rocksdb AND forst C++). This is the goal-critical
measurement. Prior numbers (`best-config.tsv`, the 8c/32g status doc) are per-lever or
conservative-winner measurements; none is the combined full-built-stack run. This plan
makes that run **one command** when the box frees.

> **DO NOT run NexMark/docker while the uniform sweep is in flight.** This cycle is
> SCRIPT/CONFIG/DOC only. The commands in §5 are to be fired AFTER the box is free.

---

## 1. The full-stack-ON per-query flag profile

All flag names verified against the engine source (`crates/forst-rs-engine/src/db.rs`,
line refs in `tools/nexmark-local/configs/best-config.tsv` header) and the OPT-N04
merge-RMW backend design (`docs/superpowers/specs/2026-06-12-opt-n04-merge-rmw-backend-design.md`).
Every lever is **default-OFF** in the engine; the validate profile turns the per-query
set ON. Encoded in `tools/nexmark-local/scripts/run-best.sh` → `apply_validate()` and
dry-runnable with `run-best.sh validate print`.

### 1.1 Read-amp joins — q7 / q9 / q20 (and q4 / q19, same family)

The shared join stack (`join_stack()` in run-best.sh):

| Lever | Env flag | Value | Source (db.rs) | What it does |
|---|---|---|---|---|
| KV separation | `FRS_KV_SEPARATION` | `true` | 15331 | LSM carries 36-B value pointers → cheap leveling, value-carrying read path |
| Min blob size | `FRS_KV_MIN_BLOB_SIZE` | `256` | 15460 | separate only values ≥256 B |
| Trivial move | `FRS_TRIVIAL_MOVE` | `true` | 15417 | move non-overlapping L0 runs without rewrite |
| S2 pinned | `FRS_RS_S2_PINNED` | `1` | 15529 | loser-tree merge on the deep-probe path |
| Adaptive S2 (R1) | `FRS_S2_FANOUT_MIN` | `8` | 15601 | arm the loser tree only when probe fan-out ≥ 8 (deep/shallow split) |
| Coalesced vlog deref (A) | `FRS_VLOG_COALESCE_DEREF` | `1` | 312 | batched value-log deref on the probe side; byte-identical output |
| Probe bloom prune (MR-1) | `FRS_RS_PROBE_BLOOM_PRUNE` | `1` | 677 | metadata-resident prefix-bloom skips bloom-negative SSTs with NO cold open |
| Leveled hot CF (Approach-1) | `FRS_RS_LEVELED_HOT_CF` | `1` | 712 | armed CFs (fan-out ≥ `_FANOUT_MIN`, default 8) get leveled bottom → bounds per-probe source COUNT |
| Persistent probe iter (Approach-1) | `FRS_PERSISTENT_PROBE_ITER` | `1` | 16850 | reusable per-(CF,version) iterator; amortizes source-set locate across probes |

Tuning knobs (left at engine default unless overridden): `FRS_RS_LEVELED_HOT_CF_FANOUT_MIN`
(8), `FRS_RS_LEVELED_HOT_CF_L0_TRIGGER` (4).

**Resource:** q7 / q20 run at **8c/32g** (`TOPO=split`, 2×TM 4c/16g + 1×JM 2c/4g).
**q9 runs at 8c/36g single-TM** (`TOPO=single`, `SINGLE_TM_CPUS=8`, `SINGLE_TM_MEM=36g`,
`FRS_TM_PROCESS_SIZE=16384m`, `FRS_JM_PROCESS_SIZE=3072m`) — KV-sep's resident vlog
state OOMs the split's 16g cgroup, so q9 needs the bigger per-TM memory. The KV-sep
resident bounds are layered on (`FRS_VLOG_READER_CACHE_CAP=2048`,
`FRS_VLOG_RESIDENT_BUDGET_MB=512`, `FRS_KV_ADAPTIVE_PRESSURE=1`). This is the per-query
topology exception the user allowed; it does not change any other query's run.

### 1.2 Windowed / OVER — q8 / q11 / q12 / q18

The window stack (`window_stack()`):

| Lever | Env flag | Value | Source | What it does |
|---|---|---|---|---|
| Merge-RMW (Approach-2 / A2) | `FRS_RS_MERGE_RMW` | `1` | OPT-N04 design J5 | backend RMW via engine merge ops (windowed-agg accumulator write path) |
| Routing-adaptive executor (Approach-3 / R2a) | `FRS_RS_EXECUTOR` | `routing-adaptive` | `ForStRsAsyncKeyedStateBackend.java:1319` | drain-tail + cross-probe overlap; q11 2.35× (318.9→135.7s) |

Per-state opt-in escape hatch (unused by default): `FRS_RS_MERGE_RMW_STATES=name1,name2`;
merge-chain rebase cap `FRS_RS_MERGE_CHAIN_REBASE` (default 4096).
**Resource:** 8c/32g `TOPO=split`.

### 1.3 q17 — zero-handoff inline carve-out

`FRS_RS_EXECUTOR=routing-adaptive` ONLY (selects the iter-free zero-handoff path for the
unbounded keyed group-agg). **KV-sep OFF. No join stack, no merge-RMW.** q17 is STRUCTURAL
vs RocksDB (its gap is the async framework coordination floor, mini-bench-confirmed not a
read-path defect) and is a decisive **win vs ForSt (0.33×, beats it 3.3×)** on this path.
The carve-out is the defensive imperative: keep q17 off any parallel/merge path that
would re-introduce handoff.

### 1.4 q19 — KV-sep ON (already wins)

Full join stack (§1.1), 8c/32g. q19 is the #1 KV-sep beneficiary (flag-ON 216 vs OFF
464, +115%) and already BEATS BOTH backends (0.82× RDB, 1.18× ForSt). Validated here as a
no-regress confirmation that the *added* MR-1 / leveled / persistent levers do not hurt it.

### 1.5 Summary table (what `run-best.sh validate print` emits)

| query | KV-sep | S2 (pin/R1) | coalesce A | MR-1 | leveled-1 | persist-1 | merge-RMW | executor | resource |
|---|---|---|---|---|---|---|---|---|---|
| q4  | ON | 1 / 8 | 1 | 1 | 1 | 1 | – | default | 8c/32g |
| q7  | ON | 1 / 8 | 1 | 1 | 1 | 1 | – | default | 8c/32g |
| q9  | ON | 1 / 8 | 1 | 1 | 1 | 1 | – | default | **8c/36g** |
| q19 | ON | 1 / 8 | 1 | 1 | 1 | 1 | – | default | 8c/32g |
| q20 | ON | 1 / 8 | 1 | 1 | 1 | 1 | – | default | 8c/32g |
| q8  | OFF | – | – | – | – | – | 1 | routing-adaptive | 8c/32g |
| q11 | OFF | – | – | – | – | – | 1 | routing-adaptive | 8c/32g |
| q12 | OFF | – | – | – | – | – | 1 | routing-adaptive | 8c/32g |
| q18 | OFF | – | – | – | – | – | 1 | routing-adaptive | 8c/32g |
| q17 | OFF | – | – | – | – | – | – | routing-adaptive | 8c/32g |

---

## 2. The validation jar / branch requirement

**The validation jar MUST be built from the Flink backend branch `readside-r2a`**
(tip `5897c2260e0` at time of writing). That branch carries:

- **Approach-2 (A2) merge-RMW** backend wiring (`FRS_RS_MERGE_RMW`).
- **Approach-3 / R2a `routing-adaptive` executor** (`FRS_RS_EXECUTOR=routing-adaptive`).
- **The q8 op-mix race FIX** (`5897c2260e0`) that previously gated the routing-adaptive
  default-flip — repro now GREEN 593/0. Without this fix the q8 byte-exact gate (§3.2)
  CANNOT pass, so the jar branch is load-bearing for the correctness gate.

**Engine .so:** `origin/forst-rs` (the same tip that ships all §1 read-amp levers). Build
both before validating:

```bash
# forst-rs Linux .so (Dockerized 8c/32g harness):
bash tools/nexmark-local/scripts/run-8c32g.sh build
# Flink state-backend jar from the readside-r2a branch (NOT main/forst-rs):
#   on the box: git -C $FLINK_SRC checkout readside-r2a   (verify tip 5897c2260e0)
bash tools/nexmark-local/scripts/run-8c32g.sh jar
```

The runner deploys `flink-statebackend-forst-rs-2.2.0.jar` into `$FLINK/lib/`; confirm it
was built from `readside-r2a` (the jar has no version suffix, so verify the source branch
checkout before `jar`).

---

## 3. A/B + canary correctness protocol

### 3.1 The "beat BOTH" cross-backend comparison (q7 / q9 / q20, plus q4 / q19)

For each priority query, run THREE arms at the SAME resource, strictly serial:

- **A** = forst-rs full-stack-ON (the §1 profile).
- **B** = rocksdb baseline (no forst-rs flags; lz4 for fairness).
- **C** = forst-local (ForSt C++ baseline).

`run-best.sh validate <query>` runs all three (ARMS default
`forst-rs-ffm-local rocksdb forst-local`). **PASS = A's wall ≤ B's AND ≤ C's** (beat
BOTH). For q9 all three arms run at 8c/36g single-TM so the comparison is apples-to-apples.

### 3.2 Lever-attribution A/B (full-stack-ON vs flags-OFF)

To prove a beat-both win is the STACK, not box noise: `run-best.sh validate-ab <query>`
runs forst-rs full-stack-ON (arm A) then forst-rs all-levers-OFF (arm B, engine defaults +
lz4), SAME resource, SAME .so/jar, back to back. **A meaningfully faster than B** attributes
the win to the built stack. Expected per the per-lever history: q7 +35% (937→695-class),
q19 +115%, q20 +16%, q4 +23% — the combined stack should match-or-beat those component
deltas.

### 3.3 Approach-3 default-flip correctness gate

Before `routing-adaptive` can become the executor default, the gate (status doc §"Approach-3
default-flip gate"):

1. **q8 exact band ×3 byte-exact** — run q8 three times under `FRS_RS_EXECUTOR=routing-adaptive`;
   `out_rows` (from the harness RESULT line) must be IDENTICAL across all three runs AND
   equal the rocksdb baseline's `out_rows`. (This is the gate the q8 op-mix race blocked;
   the `readside-r2a` fix is what makes it passable.)
2. **q17 carve-out trace** — confirm q17 under routing-adaptive selects the zero-handoff
   inline path (no parallel handoff); wall ≈ 83.7s class, `out_rows` == rocksdb.
3. **q11 / q20 / q9 no-regress** — routing-adaptive must not regress these vs their
   best-config walls (q11 ≤ ~135.7s, q20 ≤ ~824s, q9 finishes).

### 3.4 q11 / q12 at 100M (window-row hold)

Confirm R2a + Approach-2 hold their measured wins at full 100M scale: q11 ≤ ~135.7s and
q12 ~41s, both with `out_rows` matching the rocksdb baseline (window rows correct, not
just completion). These are part of `validate sweep`.

### 3.5 Cross-backend row-match (correctness, every query)

For EVERY validated query, the forst-rs `out_rows` in the harness RESULT line
(`measure-sql.sh:229`) must equal the rocksdb arm's `out_rows`. A faster-but-wrong arm is
a FAIL regardless of wall. Capture all three arms' RESULT lines and diff `out_rows`.

---

## 4. Reproducibility on BOTH macOS + origin-Linux

The validate machinery defers to the UNMODIFIED `run-8c32g.sh`, which already carries the
cross-platform detection (`uname -s` → Darwin vs Linux; per-OS REPO/WORKENV/PLAT/IMG
defaults; physical-RAM auto-detect for the 36g headroom warning; io_uring seccomp default
ON for Linux / no-op for macOS). The new validate flags are forwarded through the SAME
docker `-e` passthrough block. No platform-specific branch is added in run-best.sh — so the
exact same `run-best.sh validate ...` command works on both:

- **macOS** (Apple-Silicon dev box): arm64 Linux containers under Docker Desktop. Note
  the 35g Mac: q9's 36g profile assumes ≥40 GiB physical; run-8c32g.sh WARNS if it won't
  fit — override `SINGLE_TM_MEM` on a smaller box.
- **origin Linux box** (x86_64, `/ssd2/$USER/ForSt`): amd64 containers, jemalloc preload
  ON, io_uring ON. Set `REPO`/`FLINK`/`IMG`/`PLAT` per the README §8, and
  `FRS_CTMP_BASE` to a dockerd-servable scratch disk (pick-disk.sh).

---

## 5. The exact command(s) to run when the box frees

```bash
# 0) Build (engine .so + jar from readside-r2a). One-time + on engine/backend changes.
bash tools/nexmark-local/scripts/run-8c32g.sh build
#    on the box, ensure $FLINK_SRC is on branch readside-r2a (tip 5897c2260e0) FIRST:
bash tools/nexmark-local/scripts/run-8c32g.sh jar

# 1) DRY-RUN — confirm the resolved per-query full-stack-ON flag sets (NO run):
bash tools/nexmark-local/scripts/run-best.sh validate print          # all queries
bash tools/nexmark-local/scripts/run-best.sh validate print q7       # one query

# 2) THE beat-both proof — one query, 3 arms (forst-rs ON vs rocksdb vs forst C++):
bash tools/nexmark-local/scripts/run-best.sh validate q7
bash tools/nexmark-local/scripts/run-best.sh validate q9             # auto 8c/36g
bash tools/nexmark-local/scripts/run-best.sh validate q20

# 3) Full sweep — all 10 priority queries × 3 arms, serial:
bash tools/nexmark-local/scripts/run-best.sh validate sweep
#    subset: QUERIES="q7 q9 q20" bash .../run-best.sh validate sweep
#    arms subset: ARMS_VALIDATE="forst-rs-ffm-local rocksdb" bash .../run-best.sh validate q7

# 4) Lever-attribution A/B (full-stack-ON vs flags-OFF, same resource):
bash tools/nexmark-local/scripts/run-best.sh validate-ab q7
bash tools/nexmark-local/scripts/run-best.sh validate-ab q20

# 5) Approach-3 default-flip correctness gate (q8 byte-exact ×3):
for i in 1 2 3; do
  FRS_RS_EXECUTOR=routing-adaptive bash tools/nexmark-local/scripts/run-best.sh validate q8
done
#    then diff the three forst-rs out_rows AND vs the rocksdb out_rows (must all match).
```

Origin-Linux box: prefix with the box env (README §8), e.g.
`REPO=/ssd2/$USER/ForSt FLINK=~/workenv/flink-2.2.1 IMG=forst-bench:x86 PLAT=linux/amd64
FRS_CTMP_BASE=/ssd2/$USER/frs-tmp bash tools/nexmark-local/scripts/run-best.sh validate sweep`.

---

## 6. Verification performed for this prep cycle (no NexMark run)

- `bash -n` clean on `run-best.sh` and `run-8c32g.sh`.
- `shellcheck -S warning` clean on both (only info-level SC2030/SC2031 subshell-local
  notes, which are the intended per-query env-isolation design, matching the existing
  `run_one`/`run_q9_36g` blocks).
- `run-best.sh validate print` dry-runs all 10 priority queries' resolved flag sets
  correctly (§1.5 table reproduced exactly) with clean exit.
- Every flag name grep-verified against `crates/forst-rs-engine/src/db.rs` (read-amp +
  S2 + persistent-probe) and the OPT-N04 design (merge-RMW) and the best-config.tsv
  header (executor, Flink-side line ref).
