# Doc index — tools/nexmark-local

Pointers to the canonical design / procedure specs (kept in
`docs/superpowers/specs/`, not duplicated here, so they stay the single source
of truth). Paths are relative to the repo root.

## Procedure / config (read before running)

- **`docs/superpowers/specs/2026-06-14-remote-nexmark-config-v3.md`**
  Canonical UNIFORM config + flag values, deploy + concurrency etiquette.
  Maps config → harness (`run-8c32g.sh`) → templates. *The* config authority.
- **`docs/superpowers/specs/2026-06-13-nexmark-e2e-test-runbook.md`**
  Self-contained build/fire/record runbook (image, .so, jar, fire sequence,
  result parsing). *The* procedure authority. MOCK-S3 only; strictly serial.

## Disaggregated-state / S3 simulation

- **`docs/superpowers/specs/2026-06-13-phase2-disaggregated-state-design.md`**
  The disaggregated-state design (remote SST store, local cache, file mapping,
  link-mode checkpoints). Background for why the remote leg exists.
- **`docs/superpowers/specs/2026-06-13-disagg-s3emulation-nexmark-results.md`**
  & **`...-disagg-s3emulation-nexmark-validation.md`**
  Prior S3-emulation NexMark runs + validation.
- **`docs/superpowers/specs/2026-06-13-disagg-competitive-analysis.md`**
  ForSt `FileOwnershipDecider` / non-SST-local routing competitive analysis.
- **`docs/superpowers/specs/2026-06-14-disagg-s3-lock-assessment.md`**
  Disagg lock assessment (concurrency on the remote path).
- **`docs/superpowers/specs/2026-06-13-remote-compaction-design.md`**
  Remote compaction offload design.

## Adaptive / dynamic optimization (the "one config" lever)

- **`docs/superpowers/specs/2026-06-14-adaptive-kvsep-dynamic-design.md`**
  Adaptive KV-separation — dynamic engine behavior under the uniform config.
- **`docs/superpowers/specs/2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md`**
  Per-query root-cause + dynamic repair (no per-query config).
- **`docs/superpowers/specs/2026-06-14-q9-kvsep-oom-rootcause.md`**
  q9 KV-sep OOM root cause.
- **`docs/superpowers/specs/2026-06-13-forst-optimization-catalog.md`**
  Catalog of ForSt optimizations to mirror.

## LOCAL S3 simulation throttle (this package)

- Implementation: `crates/forst-rs-io/src/throttle.rs`
  (`RateLimiter`, `ThrottledFileSystem`, env `FRS_REMOTE_BW_MBPS`).
- Wiring: `crates/forst-rs-engine/src/db.rs` → `wrap_remote_bw_throttle()`.
- Smoke: `crates/forst-rs-engine/tests/remote_bw_throttle_it.rs`.
- Driver + config: `tools/nexmark-local/scripts/run-s3sim.sh`,
  `tools/nexmark-local/configs/config-forst-rs-s3sim.yaml.tpl`.
