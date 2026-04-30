# N2 — Nexmark Gate-Zero Implementation Plan

> **For agentic workers:** This plan has TWO lanes:
> - **Lane A (LLM-executable)**: vendoring, scaffolding, schema/template files, CI smoke workflow. Tasks 1–5.
> - **Lane B (user-hardware-required)**: actual cluster deployment, baseline runs, sign-off. Tasks 6–8 are hand-off documentation.
>
> Lane A tasks use checkbox (`- [ ]`) syntax. Lane B tasks are descriptive — they execute outside an LLM session.

**Goal:** Land all infrastructure required to establish a Nexmark baseline using the canonical `nexmark/nexmark` harness with `state.backend.type=forst`, then hand off the actual measurement run to the user.

**Architecture:** Vendor canonical Nexmark harness at pinned SHA → build script wrapper → cluster config templates pointing at original ForSt → empty baseline.json schema + sign-off doc → optional CI smoke workflow. Real-cluster baseline measurement is user-driven (Lane B).

**Tech Stack:** Git submodule (or vendored copy), Maven (harness build), shell wrapper scripts, JSON schema for baseline, GHA workflow.

**Authoritative spec:** `.planning/refactor-review/A1_reconciliation.md` §4 @ commit `107bc0ce2`.
**Research basis:** `.planning/refactor-review/N1_nexmark_research.md` @ `eed47d954`.

---

## Files

| Path | Action | Lane |
|---|---|---|
| `.gitmodules` | Modify (add nexmark submodule) — OR vendor `nexmark/` directly | A1 |
| `nexmark/` (submodule or vendored) | Create | A1 |
| `nexmark-config/flink-config-baseline.yaml` | Create | A2 |
| `nexmark-config/nexmark.yaml` | Create | A2 |
| `nexmark-config/build-baseline.sh` | Create | A3 |
| `.planning/nexmark/baseline.json.template` | Create | A4 |
| `.planning/nexmark/baseline-signoff.md.template` | Create | A4 |
| `.github/workflows/nexmark-baseline.yml` | Create (optional CI smoke) | A5 |
| `.planning/nexmark/USER_RUNBOOK.md` | Create (Lane B hand-off doc) | A5 |

---

## Out of Scope

- Actual baseline measurement runs (Lane B; requires user cluster)
- ForSt-RS variant runs (only original ForSt baseline in scope here; ForSt-RS variant lands as part of Phase G/H)
- Cross-running with Spark / other engines
- Modifying the canonical Nexmark queries (we use them as-is)

---

## Tasks (Lane A — LLM-executable)

### Task 1: Vendor `nexmark/nexmark` at pinned commit

**Decision needed before starting**: submodule vs vendored copy.

**Recommendation: vendor as a submodule.** Reasons: (i) lighter checkout footprint; (ii) explicit upstream pin; (iii) cleaner git diff on our repo; (iv) easier upstream-track updates. Pinned SHA: `6b3646c` (2025-12-26 — last stable per N1 §1.2).

**Files:**
- Modify or create: `/Users/lijunqing/Code/stczwd/ForSt/.gitmodules`
- New directory: `/Users/lijunqing/Code/stczwd/ForSt/nexmark/`

- [ ] **Step 1.1: Verify no existing `nexmark/` path**

```
test -e /Users/lijunqing/Code/stczwd/ForSt/nexmark && echo "EXISTS — STOP" || echo "OK — does not exist"
```
Expected: `OK — does not exist`

- [ ] **Step 1.2: Add submodule pinned at `6b3646c`**

```
cd /Users/lijunqing/Code/stczwd/ForSt
git submodule add https://github.com/nexmark/nexmark.git nexmark
cd nexmark
git checkout 6b3646c
cd ..
```

- [ ] **Step 1.3: Verify pin**

```
cd /Users/lijunqing/Code/stczwd/ForSt/nexmark
git rev-parse HEAD
```
Expected: `6b3646c...` (full SHA starts with 6b3646c)

```
ls /Users/lijunqing/Code/stczwd/ForSt/nexmark
```
Expected: includes `nexmark-flink/`, `nexmark-spark/`, `pom.xml`, `README.md`, `GUIDE_V2.md`

- [ ] **Step 1.4: Commit submodule addition**

```
cd /Users/lijunqing/Code/stczwd/ForSt
git add .gitmodules nexmark
git commit -m "$(cat <<'EOF'
feat(nexmark): vendor nexmark/nexmark @ 6b3646c as submodule

Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 107bc0ce2
Research: .planning/refactor-review/N1_nexmark_research.md @ eed47d954

Vendoring the canonical Nexmark harness at the latest stable upstream
commit (2025-12-26, "Fix unexpected job cancellation"). Path b per N1
research — Flink upstream has no Nexmark CI to mirror; this canonical
community harness is the only viable path.

The harness explicitly supports state.backend.type=forst per
GUIDE_V2.md, which lets us baseline against original ForSt without
patching the runner.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Create config templates pointing at original ForSt

**Files:**
- Create: `/Users/lijunqing/Code/stczwd/ForSt/nexmark-config/flink-config-baseline.yaml`
- Create: `/Users/lijunqing/Code/stczwd/ForSt/nexmark-config/nexmark.yaml`

The canonical harness ships with `nexmark-flink/src/main/resources/conf/config_v2.yaml` and `nexmark.yaml` templates. We override only the fields specific to baseline measurement: state backend, checkpoint dir, IO temp dirs.

- [ ] **Step 2.1: Create `nexmark-config/flink-config-baseline.yaml`**

Use Write tool. Path: `/Users/lijunqing/Code/stczwd/ForSt/nexmark-config/flink-config-baseline.yaml`

Content:

```yaml
# ForSt-RS Nexmark Baseline — Flink overrides
#
# Layered on top of nexmark/nexmark-flink/src/main/resources/conf/config_v2.yaml.
# Apply by: cp this file to flink/conf/config.yaml on master node, OR merge
# the relevant sections.
#
# Authoritative spec: .planning/refactor-review/A1_reconciliation.md §4 @ 107bc0ce2

# State backend — the variable we are baselining
state.backend.type: forst

# Checkpoint storage — REQUIRED for stateful queries (q3..q22 use state)
# Replace <HDFS_HOST> with your HDFS namenode address.
execution.checkpointing.dir: hdfs://<HDFS_HOST>/checkpoints/nexmark-baseline

# Local IO temp dirs — recommend SSDs for L0/L1 cache
# Replace with your worker-node local paths
io.tmp.dirs: /mnt/disk1/tmp,/mnt/disk2/tmp

# Default parallelism — matches nexmark canonical config
parallelism.default: 8

# JM/TM addresses — set during cluster deployment
# jobmanager.rpc.address: <set on master node>
```

- [ ] **Step 2.2: Create `nexmark-config/nexmark.yaml`**

Use Write tool. Path: `/Users/lijunqing/Code/stczwd/ForSt/nexmark-config/nexmark.yaml`

Content:

```yaml
# ForSt-RS Nexmark Baseline — Nexmark runner overrides
#
# Layered on top of nexmark/nexmark-flink/src/main/resources/conf/nexmark_v2.yaml.
# Replace <MASTER_HOST> with your master-node address.
#
# Authoritative spec: .planning/refactor-review/A1_reconciliation.md §4 @ 107bc0ce2

# Metric reporter — collects Cores × Time(s) per query
nexmark.metric.reporter.host: <MASTER_HOST>
nexmark.metric.reporter.port: 9099

# Workload — keep canonical defaults for comparability
# (override only if baseline numbers must match a specific scale)
# nexmark.workload.events.num: 100000000
# nexmark.workload.bid.proportion: 92
# nexmark.workload.auction.proportion: 6
# nexmark.workload.person.proportion: 2

# Query selection: 'all' = q0..q23 (q23 added 2025-12-25)
# nexmark.query.set: all
```

- [ ] **Step 2.3: Verify both files created**

```
ls -la /Users/lijunqing/Code/stczwd/ForSt/nexmark-config/
```
Expected: shows both `flink-config-baseline.yaml` and `nexmark.yaml`

- [ ] **Step 2.4: Commit**

```
cd /Users/lijunqing/Code/stczwd/ForSt
git add nexmark-config/
git commit -m "$(cat <<'EOF'
feat(nexmark): config templates for original-ForSt baseline run

Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 107bc0ce2

Two override files layered on top of nexmark/nexmark-flink shipping
templates:

- flink-config-baseline.yaml: pins state.backend.type=forst, sets
  checkpoint dir + io.tmp.dirs placeholders for cluster operator
  to fill in
- nexmark.yaml: metric reporter host + workload defaults (commented
  for canonical comparability)

User runs the canonical 'nexmark/bin/run_query.sh all' on their
cluster after copying these files into the deployed nexmark/ + flink/
directories per GUIDE_V2.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Create build wrapper script

**Files:**
- Create: `/Users/lijunqing/Code/stczwd/ForSt/nexmark-config/build-baseline.sh`

This is a thin wrapper around `nexmark/nexmark-flink/build.sh` that produces `nexmark-flink.tgz` plus our config overrides packaged together for cluster deployment.

- [ ] **Step 3.1: Write the wrapper**

Use Write tool. Path: `/Users/lijunqing/Code/stczwd/ForSt/nexmark-config/build-baseline.sh`

Content:

```bash
#!/usr/bin/env bash
#
# Build the Nexmark baseline package — invokes the canonical harness build
# and bundles our config overrides.
#
# Usage:   ./nexmark-config/build-baseline.sh
# Output:  nexmark-baseline.tgz (containing nexmark-flink.tgz + config overrides)
#
# Authoritative spec: .planning/refactor-review/A1_reconciliation.md §4

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NEXMARK_DIR="${REPO_ROOT}/nexmark/nexmark-flink"
CONFIG_DIR="${REPO_ROOT}/nexmark-config"
OUT_DIR="${REPO_ROOT}/build/nexmark-baseline"

if [[ ! -d "${NEXMARK_DIR}" ]]; then
  echo "ERROR: ${NEXMARK_DIR} not found. Did you initialize submodules?"
  echo "Run:   git submodule update --init --recursive"
  exit 1
fi

echo "[1/3] Building canonical nexmark-flink package..."
cd "${NEXMARK_DIR}"
./build.sh
cd "${REPO_ROOT}"

echo "[2/3] Bundling config overrides..."
mkdir -p "${OUT_DIR}"
cp "${NEXMARK_DIR}/nexmark-flink.tgz" "${OUT_DIR}/"
cp "${CONFIG_DIR}/flink-config-baseline.yaml" "${OUT_DIR}/"
cp "${CONFIG_DIR}/nexmark.yaml" "${OUT_DIR}/"

echo "[3/3] Creating combined tarball..."
cd "${REPO_ROOT}/build"
tar czf nexmark-baseline.tgz nexmark-baseline/
mv nexmark-baseline.tgz "${REPO_ROOT}/"
cd "${REPO_ROOT}"

echo ""
echo "✓ Build complete: ${REPO_ROOT}/nexmark-baseline.tgz"
echo "  Contents: nexmark-flink.tgz + config overrides"
echo ""
echo "Next steps (user, on cluster):"
echo "  1. scp nexmark-baseline.tgz to master node"
echo "  2. tar xzf nexmark-baseline.tgz; cd nexmark-baseline"
echo "  3. Follow .planning/nexmark/USER_RUNBOOK.md"
```

- [ ] **Step 3.2: Make executable + verify**

```
chmod +x /Users/lijunqing/Code/stczwd/ForSt/nexmark-config/build-baseline.sh
test -x /Users/lijunqing/Code/stczwd/ForSt/nexmark-config/build-baseline.sh && echo OK
```
Expected: `OK`

- [ ] **Step 3.3: Commit**

```
cd /Users/lijunqing/Code/stczwd/ForSt
git add nexmark-config/build-baseline.sh
git commit -m "$(cat <<'EOF'
feat(nexmark): build-baseline.sh wrapper around canonical harness

Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 107bc0ce2

Thin wrapper: invokes nexmark/nexmark-flink/build.sh, then bundles the
config overrides + the canonical nexmark-flink.tgz into a single
nexmark-baseline.tgz that the user scp's to the cluster master node.

Pre-flight check: errors out if nexmark/ submodule is uninitialized.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Create baseline.json schema + sign-off templates

**Files:**
- Create: `/Users/lijunqing/Code/stczwd/ForSt/.planning/nexmark/baseline.json.template`
- Create: `/Users/lijunqing/Code/stczwd/ForSt/.planning/nexmark/baseline-signoff.md.template`

User fills these in after running on their cluster. The templates lock the schema so future ForSt-RS-variant runs produce comparable JSON.

- [ ] **Step 4.1: Write `baseline.json.template`**

Use Write tool. Path: `/Users/lijunqing/Code/stczwd/ForSt/.planning/nexmark/baseline.json.template`

Content:

```json
{
  "_meta": {
    "schema_version": 1,
    "spec_ref": ".planning/refactor-review/A1_reconciliation.md @ 107bc0ce2",
    "research_ref": ".planning/refactor-review/N1_nexmark_research.md @ eed47d954",
    "metric": "cores_x_time_seconds",
    "metric_definition": "CPU cores consumed × elapsed wall-clock time in seconds — the canonical Nexmark composite metric per nexmark/nexmark/README.md",
    "lower_is_better": true
  },
  "_provenance": {
    "harness_repo": "https://github.com/nexmark/nexmark",
    "harness_sha": "6b3646c",
    "flink_version": "<TODO: e.g. 2.2.0>",
    "flink_sha": "<TODO: full SHA from apache/flink>",
    "forst_variant": "original",
    "forst_sha": "<TODO: SHA of the ForSt build used>",
    "state_backend_type": "forst",
    "checkpoint_storage": "<TODO: e.g. hdfs://...>",
    "run_timestamps_utc": {
      "warmup_runs": ["<TODO: 3 ISO 8601 timestamps>"],
      "measurement_runs": ["<TODO: 5 ISO 8601 timestamps>"]
    },
    "operator": "<TODO: name or email of person who ran the baseline>"
  },
  "_hardware": {
    "worker_count": 4,
    "cores_per_worker": 16,
    "ram_per_worker_gb": 32,
    "ssd_per_worker_gb": 100,
    "network_gbps": 1,
    "hdfs_data_node_count": "<TODO>",
    "notes": "<TODO: any deviation from nexmark/nexmark/GUIDE_V2.md minimums>"
  },
  "_workload": {
    "events_total": 100000000,
    "bid_proportion": 92,
    "auction_proportion": 6,
    "person_proportion": 2,
    "notes": "Defaults from nexmark.yaml; document any overrides here"
  },
  "results": {
    "q0":  {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q1":  {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q2":  {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q3":  {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q4":  {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q5":  {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q7":  {"cores_x_time_seconds": null, "stddev": null, "n": 5, "_note": "q6 not supported by Flink — see FLINK-19059"},
    "q8":  {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q9":  {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q10": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q11": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q12": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q13": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q14": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q15": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q16": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q17": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q18": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q19": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q20": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q21": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q22": {"cores_x_time_seconds": null, "stddev": null, "n": 5},
    "q23": {"cores_x_time_seconds": null, "stddev": null, "n": 5, "_note": "Added 2025-12-25 in nexmark/nexmark@8063de6"}
  }
}
```

- [ ] **Step 4.2: Write `baseline-signoff.md.template`**

Use Write tool. Path: `/Users/lijunqing/Code/stczwd/ForSt/.planning/nexmark/baseline-signoff.md.template`

Content:

```markdown
# Nexmark Baseline Sign-off

**Baseline file:** `.planning/nexmark/baseline.json` @ commit `<TODO>`
**Signed by:** `<TODO: user name>`
**Sign-off date:** `<TODO: ISO 8601>`

## Confirmation

By signing this document, I confirm:

- [ ] The baseline numbers in `.planning/nexmark/baseline.json` were produced
      on the hardware described in `_hardware` field
- [ ] All 23 supported queries (q0–q5, q7–q23) ran to completion across
      all 5 measurement runs
- [ ] The state backend was original ForSt (`state.backend.type: forst`)
- [ ] Reproducibility: a fresh run on the same hardware would produce
      numbers within 1× stddev of the recorded median
- [ ] Per-query stddev / median ratio is acceptable (< 10% on each query;
      flag any outliers below)

## Signed-off Numbers

(Copy the `results` block from baseline.json here as a record snapshot.)

```json
<TODO: paste results block>
```

## Outliers / Notes

<TODO: any queries with high variance, OOMs, retries, or other concerns>

## Authoritative Spec Reference

- `.planning/refactor-review/A1_reconciliation.md` §4 @ `107bc0ce2`
- `.planning/refactor-review/N1_nexmark_research.md` @ `eed47d954`
- `.planning/refactor-review/N2_nexmark_plan.md` @ `<this plan's commit>`

After this sign-off lands on `forst-rs` branch, **C5 (`forst-rs-storage` cache)
review may begin** per A1 §4.3 gating rule.
```

- [ ] **Step 4.3: Verify both templates created**

```
ls /Users/lijunqing/Code/stczwd/ForSt/.planning/nexmark/
```
Expected: shows `baseline.json.template` and `baseline-signoff.md.template`

- [ ] **Step 4.4: Commit**

```
cd /Users/lijunqing/Code/stczwd/ForSt
git add .planning/nexmark/
git commit -m "$(cat <<'EOF'
feat(nexmark): baseline.json + sign-off templates

Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 107bc0ce2

Locks the schema for baseline.json (Cores × Time(s) per query, with
stddev and n=5 measurement runs) and for baseline-signoff.md
(user-countersigned acceptance of the numbers).

Schema version 1. Future ForSt-RS-variant runs MUST produce JSON
matching this schema for comparability with the baseline.

q6 is intentionally absent (not Flink-supported per FLINK-19059).
q23 is included with a note about its 2025-12-25 addition.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Create user runbook + optional CI smoke workflow

**Files:**
- Create: `/Users/lijunqing/Code/stczwd/ForSt/.planning/nexmark/USER_RUNBOOK.md`
- Create: `/Users/lijunqing/Code/stczwd/ForSt/.github/workflows/nexmark-baseline.yml` (optional)

The runbook is the hand-off doc that the user follows on their cluster. The CI smoke workflow is OPTIONAL — it builds the harness on every PR but does NOT run the actual baseline (which would require GHA cluster runners).

- [ ] **Step 5.1: Write `USER_RUNBOOK.md`**

Use Write tool. Path: `/Users/lijunqing/Code/stczwd/ForSt/.planning/nexmark/USER_RUNBOOK.md`

Content:

```markdown
# Nexmark Baseline — User Runbook

This runbook walks the user through producing the original-ForSt
Nexmark baseline. Hardware-bound; cannot be done in an LLM session.

**Authoritative spec:** `.planning/refactor-review/A1_reconciliation.md` §4 @ `107bc0ce2`
**Research:** `.planning/refactor-review/N1_nexmark_research.md` @ `eed47d954`
**Harness:** `nexmark/` submodule pinned at `nexmark/nexmark@6b3646c`

## Phase 1 — Pre-flight (local laptop)

1. **Initialize submodule** (one-time):
   ```bash
   cd /path/to/ForSt
   git submodule update --init --recursive
   ```

2. **Build the baseline package**:
   ```bash
   ./nexmark-config/build-baseline.sh
   ```
   Output: `nexmark-baseline.tgz` (contains nexmark-flink.tgz + config overrides)

3. **Decide on hardware path**:
   - **(α) Real cluster** (recommended for canonical baseline): 4 worker
     nodes minimum, 16 cores, 32GB RAM, 100GB SSD each, HDFS available,
     1Gbps LAN. Numbers will be comparable to published Flink Nexmark.
   - **(β) Cloud spin-up**: AWS EMR or GCP Dataproc with equivalent specs.
     Acceptable but verify the spec against `_hardware` field schema.
   - **(γ) Docker-compose harness** (NOT for baseline; smoke runs only):
     does not produce canonical numbers.

## Phase 2 — Cluster setup (master + workers)

Per `nexmark/nexmark/GUIDE_V2.md`:

1. Download Flink 2.2.x (matches our integration target)
2. `scp nexmark-baseline.tgz` to master node
3. Extract and merge config overrides:
   ```bash
   tar xzf nexmark-baseline.tgz
   cd nexmark-baseline
   tar xzf nexmark-flink.tgz
   mv nexmark-flink nexmark
   tar xzf flink-2.2.x-bin.tgz
   mv flink-2.2.x flink

   # Apply our config overrides
   cp flink-config-baseline.yaml flink/conf/config.yaml.override
   # Manually merge into flink/conf/config.yaml — set:
   #   - jobmanager.rpc.address (master IP)
   #   - execution.checkpointing.dir (replace <HDFS_HOST>)
   #   - io.tmp.dirs (your local SSD paths)

   cp nexmark.yaml nexmark/conf/nexmark.yaml.override
   # Manually merge — set nexmark.metric.reporter.host (master IP)

   # Copy generator JARs
   cp nexmark/lib/*.jar flink/lib/
   ```

4. Configure `flink/conf/workers` with worker IPs
5. `scp -r nexmark/ flink/` to each worker
6. Start cluster: `flink/bin/start-cluster.sh`
7. Setup nexmark: `nexmark/bin/setup_cluster.sh`

## Phase 3 — Baseline runs

1. **Warmup** (3 runs — DO NOT record):
   ```bash
   nexmark/bin/run_query.sh all  # 1st warmup
   nexmark/bin/run_query.sh all  # 2nd
   nexmark/bin/run_query.sh all  # 3rd
   ```
   Each takes ~50 minutes. Watch for variance — if stddev keeps shrinking
   between warmups, do more warmups until stable.

2. **Measurement** (5 runs — record each):
   ```bash
   for i in 1 2 3 4 5; do
     timestamp=$(date -u +%FT%TZ)
     echo "Run $i at $timestamp"
     nexmark/bin/run_query.sh all > "run-$i-$timestamp.log"
   done
   ```

3. **Aggregate**:
   - For each query (q0–q5, q7–q23), compute median and stddev across the 5 runs
   - Cores × Time is reported in the run logs by the harness's metric collector
   - Populate `.planning/nexmark/baseline.json` from the template
     (`cp .planning/nexmark/baseline.json.template .planning/nexmark/baseline.json`)
   - Fill in all `<TODO>` fields in `_provenance`, `_hardware`, `_workload`
   - Fill in `results` with median Cores×Time, stddev, n=5

4. **Commit baseline**:
   ```bash
   git add .planning/nexmark/baseline.json
   git commit -m "feat(nexmark): original-ForSt baseline"
   ```

## Phase 4 — Sign-off

1. Copy `.planning/nexmark/baseline-signoff.md.template` to
   `.planning/nexmark/baseline-signoff.md`
2. Fill in TODOs (your name, date, baseline commit SHA, paste results block)
3. Run sanity checks against the checklist
4. Commit:
   ```bash
   git add .planning/nexmark/baseline-signoff.md
   git commit -m "chore(nexmark): sign off on original-ForSt baseline"
   ```

## After Sign-off

Per A1 §4.3: **C5 (`forst-rs-storage` cache) review is now unblocked.**
The baseline numbers also become the gate for Phase H acceptance — the
ForSt-RS variant must hit 30–40% Cores×Time reduction vs these numbers
on the same hardware.

## Troubleshooting

- **Run hangs / OOMs on q14, q16, q22**: typically state-backend related;
  check `state.backend.type` is set correctly and HDFS checkpoint dir is
  writable.
- **High variance across runs**: usually a cold-cache effect; add more
  warmups OR keep cluster warm between measurement runs.
- **Worker disconnects**: check `flink/conf/workers` IPs are reachable
  from master via passwordless SSH.
- **Metric reporter port collisions**: default 9099; change in
  `nexmark.yaml` if needed.
```

- [ ] **Step 5.2: Write `nexmark-baseline.yml` GHA (optional smoke build)**

Use Write tool. Path: `/Users/lijunqing/Code/stczwd/ForSt/.github/workflows/nexmark-baseline.yml`

Content:

```yaml
# CI smoke build for the Nexmark baseline harness.
#
# DOES NOT run actual baseline measurements (those require real cluster
# hardware — see .planning/nexmark/USER_RUNBOOK.md).
#
# DOES verify:
#   - submodule init works
#   - nexmark-flink Maven build succeeds
#   - build-baseline.sh wrapper produces nexmark-baseline.tgz
#
# Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 107bc0ce2

name: nexmark-baseline-smoke

on:
  push:
    paths:
      - 'nexmark-config/**'
      - '.gitmodules'
      - '.github/workflows/nexmark-baseline.yml'
  pull_request:
    paths:
      - 'nexmark-config/**'
      - '.gitmodules'
      - '.github/workflows/nexmark-baseline.yml'
  workflow_dispatch:

jobs:
  build-harness:
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - name: Checkout (with submodules)
        uses: actions/checkout@v4
        with:
          submodules: recursive

      - name: Verify nexmark submodule pin
        run: |
          cd nexmark
          PINNED_SHA=$(git rev-parse HEAD)
          EXPECTED_PREFIX=6b3646c
          if [[ ! "$PINNED_SHA" =~ ^${EXPECTED_PREFIX} ]]; then
            echo "Submodule pin drift: expected ${EXPECTED_PREFIX}*, got ${PINNED_SHA}"
            exit 1
          fi
          echo "✓ Submodule pinned at ${PINNED_SHA}"

      - name: Set up JDK 11
        uses: actions/setup-java@v4
        with:
          java-version: '11'
          distribution: 'temurin'

      - name: Cache Maven repository
        uses: actions/cache@v4
        with:
          path: ~/.m2/repository
          key: ${{ runner.os }}-m2-${{ hashFiles('nexmark/**/pom.xml') }}
          restore-keys: ${{ runner.os }}-m2-

      - name: Build via wrapper
        run: ./nexmark-config/build-baseline.sh

      - name: Verify output artifact
        run: |
          test -f nexmark-baseline.tgz || (echo "missing nexmark-baseline.tgz"; exit 1)
          ls -lh nexmark-baseline.tgz

      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: nexmark-baseline-build
          path: nexmark-baseline.tgz
          retention-days: 7
```

- [ ] **Step 5.3: Verify files**

```
ls /Users/lijunqing/Code/stczwd/ForSt/.planning/nexmark/USER_RUNBOOK.md
ls /Users/lijunqing/Code/stczwd/ForSt/.github/workflows/nexmark-baseline.yml
```
Expected: both exist

- [ ] **Step 5.4: Commit**

```
cd /Users/lijunqing/Code/stczwd/ForSt
git add .planning/nexmark/USER_RUNBOOK.md .github/workflows/nexmark-baseline.yml
git commit -m "$(cat <<'EOF'
feat(nexmark): user runbook + CI smoke workflow

Authoritative spec: .planning/refactor-review/A1_reconciliation.md @ 107bc0ce2

USER_RUNBOOK.md is the hand-off doc — walks user through the four
phases (pre-flight build, cluster setup, baseline runs, sign-off).
This is hardware-bound work that LLM cannot execute; runbook is the
explicit hand-off.

nexmark-baseline.yml is a CI SMOKE workflow — verifies submodule pin,
Maven build, and wrapper output. DOES NOT run actual baseline
(would need GHA cluster runners). Catches drift in the build path.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Tasks (Lane B — User-hardware-required, hand-off only)

These tasks cannot be executed in an LLM session. They are documented for
the user to execute on their cluster.

### Task 6 (Lane B): Run pre-flight + cluster setup

Per `USER_RUNBOOK.md` Phases 1–2.

**Done criteria:**
- `git submodule update --init --recursive` exit 0
- `./nexmark-config/build-baseline.sh` exit 0; `nexmark-baseline.tgz` created
- Cluster running: `flink list` returns the 8 TaskManagers

### Task 7 (Lane B): Establish baseline numbers

Per `USER_RUNBOOK.md` Phase 3.

**Done criteria:**
- 3 warmup runs complete (logs retained but not counted in baseline)
- 5 measurement runs complete; per-query Cores×Time captured
- `.planning/nexmark/baseline.json` populated from template; all `<TODO>` fields filled
- baseline.json committed to `forst-rs` branch

### Task 8 (Lane B): Sign-off

Per `USER_RUNBOOK.md` Phase 4.

**Done criteria:**
- `.planning/nexmark/baseline-signoff.md` filled out
- Sign-off committed
- `forst-rs` branch's `SESSION_HANDOFF.md` STATE updated:
  `baselines_built: false` → `baselines_built: true`
- C5 review is unblocked per A1 §4.3 gating rule

---

## Self-Review

**Spec coverage check (against A1 §4):**
- §4.1 Goal: ✓ (plan produces baseline + sign-off)
- §4.2 step 1 Research: ✓ (already done in N1, referenced)
- §4.2 step 2 Vendor harness: Task 1 (Lane A)
- §4.2 step 3 Establish baseline: Task 7 (Lane B, hand-off)
- §4.2 step 4 Sign-off: Task 8 (Lane B, hand-off)
- §4.3 Owner & gates: documented in USER_RUNBOOK.md "After Sign-off" section
- §4.3 Tracking column in COMMIT_MANIFEST: existing (committed in B1 plan execution)

**Placeholder scan:** All `<TODO>` markers are inside *templates that the user fills in* (baseline.json.template, baseline-signoff.md.template, USER_RUNBOOK.md sample paths). These are not LLM-task placeholders. Lane A tasks have NO TODOs.

**Consistency:**
- `nexmark/` submodule path consistent across Tasks 1, 3, 5
- `nexmark-config/` path consistent across Tasks 2, 3
- `.planning/nexmark/` path consistent across Tasks 4, 5
- Schema version 1 in baseline.json — future variants (ForSt-RS) must match
- All commit messages reference A1 spec @ 107bc0ce2

**Plan size:** 5 Lane A tasks, ~20 steps. Each step 2-5 min. Lane A executable in one session by a single agent. Lane B hand-off cleanly documented.

Self-review pass: ✅.

---

## Execution Note

Lane A tasks (1–5) are mechanical doc/config writes with `Edit`/`Write` tool calls. No code changes; no Cargo / Rust touches. Each task ends with a single git commit. Recommended execution mode: **subagent-driven (parallel-safe across tasks 2/3/4/5; task 1 must finish first because all later tasks reference the submodule path)**.

Lane B tasks are explicitly user-side; do not dispatch them to subagents.
