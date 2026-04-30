# Nexmark Baseline — User Runbook

This runbook walks the user through producing the original-ForSt
Nexmark baseline. Hardware-bound; cannot be done in an LLM session.

**Authoritative spec:** `.planning/refactor-review/A1_reconciliation.md` §4 @ `107bc0ce2`
**Research:** `.planning/refactor-review/N1_nexmark_research.md` @ `eed47d954`
**Plan:** `.planning/refactor-review/N2_nexmark_plan.md` @ `58484044e`
**Harness:** `nexmark/` submodule pinned at `nexmark/nexmark@6b3646c` (pending user authorization to vendor — see Phase 0)

## Phase 0 — One-time submodule authorization (required first)

The `nexmark/nexmark` upstream harness must be vendored as a submodule.
Per N2 Lane A Task 1, this requires explicit user authorization to
integrate external code. Run this once:

```bash
cd /path/to/ForSt
git submodule add https://github.com/nexmark/nexmark.git nexmark
cd nexmark && git checkout 6b3646c && cd ..
git add .gitmodules nexmark
git commit -m "feat(nexmark): vendor nexmark/nexmark @ 6b3646c as submodule"
```

After this lands, Phase 1 can run.

## Phase 1 — Pre-flight (local laptop)

1. **Ensure submodule initialized** (one-time after Phase 0):
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
