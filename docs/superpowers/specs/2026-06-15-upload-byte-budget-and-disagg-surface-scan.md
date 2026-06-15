# Upload Byte-Budget + Honest Disagg-Perf-Surface Scan

**Date:** 2026-06-15
**Owner:** PMC-2 (Phase-2 Disaggregated State)
**Base:** `origin/forst-rs` tip `324a85b78` (FRS_ASYNC_FLUSH_UPLOAD just shipped).
**Scope:** (1) byte-aware in-flight upload budget (flag-gated, default-OFF); (2) an honest high-leverage scan for the NEXT genuinely-big disagg perf lever.

---

## Part 1 — Byte-aware in-flight upload budget (`FRS_UPLOAD_BYTE_BUDGET_MIB`)

### Problem

The in-flight buffered-upload backpressure is a fixed **COUNT** cap
(`MAX_INFLIGHT_UPLOADS = 8`, `opendal_backend.rs`). Each upload reserves **one**
permit regardless of size. Under a slow remote, a burst of LARGE compaction
outputs can pin `8 × largest_SST` of resident RAM — the count cap bounds the
*number* of concurrent uploads, not the *bytes*. Symmetrically it under-utilises
the channel for small SSTs: 8 tiny L0 flushes is a needlessly low ceiling.

### Design

A second admission regime, selected by `FRS_UPLOAD_BYTE_BUDGET_MIB` (default
OFF):

- **Count regime (default, unset/0):** `MAX_INFLIGHT_UPLOADS` permits, 1 per
  upload. **Byte-identical** to the prior path.
- **Byte regime (positive MiB):** the `upload_sem` semaphore is sized to
  `budget_mib` permits (1 permit = 1 MiB, `UPLOAD_BUDGET_UNIT_BYTES`); each
  upload reserves `ceil(sst_bytes / 1 MiB)` permits via `acquire_many[_owned]`,
  clamped to `[1, budget]` so an SST larger than the whole budget runs **alone**
  rather than deadlocking on a permit count it can never satisfy.

This bounds the **sum** of resident upload bytes at `~budget_mib` while letting
**many** small SSTs proceed concurrently. It composes with
`FRS_ASYNC_FLUSH_UPLOAD` (the TRUE-bound path that holds the permit on the flush
thread before spawn) — there the byte budget is a hard resident-RAM bound;
with async-flush OFF the permits still scale by size for consistency but are held
only for the network transfer.

Recommended value: ~25–50% of the `WriteBufferManager` budget, so upload-resident
memory tracks the write memory the engine is already sized for.

**Implementation:** all in `crates/forst-rs-io/src/opendal_backend.rs`:
- `UPLOAD_BYTE_BUDGET_MIB_ENV`, `UPLOAD_BUDGET_UNIT_BYTES`, `upload_byte_budget_mib()` (OnceLock).
- Pure, unit-testable helpers `byte_budget_total_permits()` / `byte_budget_permits_for()`.
- `upload_sem_total_permits()` (sizes the semaphore at construction) and
  `upload_permits_for(nbytes)` (per-upload reservation).
- `close_writer` acquires `upload_permits_for(expected)` via `acquire_many_owned`
  on both the prefetched (async-flush ON) and in-task (OFF) acquire sites.
- 4 unit tests (round-up, oversized-clamp, many-small-few-large against a real
  `Semaphore`, count-regime byte-identity). 31/31 backend tests, 272 io-lib tests green.

### Mini-bench result (`upload_byte_budget`, mock — no network, no NEXMark)

400 SSTs, 10% large (64–256 MiB, largest 254 MiB), modeled 64 MiB/s remote.

| budget | count-cap peak | byte-budget peak | small-SST concurrency | wall (count→byte) | verdict |
|---|---|---|---|---|---|
| **1024 MiB** (matched envelope) | 1488 MiB (worst 2032) | **1024 MiB** | 8 → **50** | 17.6 s → 20.2 s (+15%) | **EFFECTIVE** |
| 256 MiB (tight RAM) | 1488 MiB | **256 MiB** (5.8× lower) | 8 → 20 | 17.6 s → 92.5 s (5.3×) | INCONCLUSIVE (wall) |

### Honest verdict

**The byte budget is a real, correct *resident-bound* lever, NOT a throughput
lever — and it is only "free" at a budget sized to the count cap's intended
envelope.** At a matched 1024 MiB budget it caps resident at exactly the budget
(vs the count cap's 1488 MiB observed / 2032 MiB worst-case `8 × largest`) AND
*raises* small-SST concurrency 8 → 50, for a modest +15% wall. Pushed tight
(256 MiB) it bounds resident 5.8× lower but serialises large uploads and
regresses wall 5.3× — the honest trade-off when RAM, not throughput, is the hard
constraint (e.g. an 8c/32g box where 8 × 254 MiB resident would OOM-pressure the
cgroup).

**Recommendation:** keep it **default-OFF**; it is a targeted relief valve for
the *specific* failure mode where large-SST upload bursts over a slow remote pin
unbounded resident RAM (the q9/q20-class join-state flush bursts on the 8c/32g
box). It is **marginal over the count cap on dev-box / fast-remote regimes** and
the smoke-scale bench honestly reports MARGINAL there. It is **worth keeping**
(small, clean, correctness-preserving, composes with async-flush) but should be
enabled only when a resident-RAM bound is needed, with the budget sized to the
count cap's envelope — not below it.

---

## Part 2 — Honest scan for the next high-leverage disagg perf lever

Method: re-verified each prompt-named candidate against the engine at `file:line`
plus the prior cycle's full paper-coverage audit (`2026-06-14-paper-coverage-audit.md`,
which read the PVLDB paper cover-to-cover and concluded the §5 backend surface is
captured). Independent confirmation below.

| Candidate (prompt-named) | Status in forst-rs | Evidence | Magnitude / verdict |
|---|---|---|---|
| **Parallel / pipelined RESTORE download** | **Already moot + present** | Restore is **link-based, zero-download** (`db.rs` `open_from_linked_checkpoint_instant…`, file-mapping points at remote SSTs, reads lazy/cache-warmed). The optional cache-warm is `FRS_RESTORE_BG_FILL` → `background_fill.rs` — already a **paced multi-worker pool** (`:174` spawn, `workers` param, token-bucket pace). | LOW. Disagg restore needs no bulk download; the warm-back is already parallel. No high-magnitude gap. |
| **Compaction-output direct-to-remote streaming (skip local round-trip)** | **Not a round-trip to remove** | Compaction/flush write through `CachedFileSystem` → the local cache copy is written via `CachePopulatingWritableFile` **AND** the buffered async upload runs in parallel (`cached_fs.rs:115` `open_local_first_sst`, write-back design `:33-40`). The local copy is the **read cache** (load-bearing — it fixed the heavy-join FREEZE, `:121-125`), not a staging buffer. | NEGATIVE. "Skip local" would *remove* the local-first read that serves probes during the upload → regress, not improve. |
| **Read-path block-level coalescing across SSTs** | **Within-SST present; cross-SST is read-path/PMC-1** | Contiguous-block coalescing into one vectored read exists **within** an SST (`prefetch.rs:862` "ONE vectored read", test `:1285`). **Value-log** deref is coalesced **across objects** by segment (`FRS_VLOG_COALESCE` / `FRS_VLOG_SCAN_COALESCE`, `db.rs:288`/`:344`). Cross-SST block coalescing is bounded by each SST being a separate remote object (one ranged GET can't span objects). | LOW-MED and **out of PMC-2 lane** (read path = PMC-1). The cross-object batching that matters (shared vlog) is already coalesced. |
| **Adaptive prefetch-window sizing** | Scan readahead shipped (fixed/one-deep) | `FRS_VLOG_SCAN_READAHEAD` (cycle-6), scan-coalesce window `vlog_scan_coalesce_window()` (`db.rs:392`, env-tunable). | LOW. Adaptive sizing over the tunable window is an **<10% incremental tuning** item, not a magnitude lever. |
| **Remote compaction OFFLOAD scheduling (compute on a remote worker)** | Trait + emulated executor present; real endpoint user-gated | `compaction_executor.rs:109` `RemoteEmulatedCompactionExecutor`, offload `:142-169`, round-trip test `:668`. Matches the paper's own "experimental feature [36]". | MED but **endgame / user-gated** — a real remote compactor *service* is an infra deliverable (a stateless compactor process + scheduler), not an in-engine perf lever this cycle can land. The in-engine seam is done. |

### Headline finding — the disagg PERF surface is genuinely thinning

Every prompt-named candidate is either **already shipped**, **moot under the
link-based disagg model**, **actively undesirable** (direct-to-remote), or
**out of PMC-2's lane** (read-path cross-SST = PMC-1) or **infra-endgame**
(real remote-compactor service). This independently corroborates the prior
cycle's paper-coverage audit: **there is no genuinely-missing, high-magnitude,
in-lane (cache/checkpoint/routing/write-path) disagg optimization left.**

The shipped Phase-2 stack is deep: KV-sep + coalesced/fanned-out vlog deref +
scan-coalesce + scan-readahead (read), QoS rate-split + WAL-DELTA link-ckpt +
async-flush-upload-bounded + (now) byte-budget (write), instant link-restore +
paced parallel warm, multi-tier cache + epoch-decay admission, remote-compaction
seam. The remaining items are all **<10% / incremental tuning**, **hygiene**
(vlog GC/rescale-clip — correctness, not throughput), **infra-endgame** (remote
compactor service), or **PMC-1's read path**.

### Next-cycle recommendation

**Shift Phase-2 from BUILDING to E2E VALIDATION.** The high-leverage build work
is done; the real remaining uncertainty is whether the *combined* shipped stack
delivers on the disagg-on-S3 regime end-to-end. Concretely:

1. **E2E mock-S3 NEXMark validation of the combined stack** (the disagg-relevant
   queries: q4/q9/q20 join-state + checkpoint-under-load), enabling the relief
   flags together (`FRS_ASYNC_FLUSH_UPLOAD` + `FRS_UPLOAD_BYTE_BUDGET_MIB` sized
   to WBM + `FRS_UPLOAD_RATE_SPLIT` + WAL-DELTA link ckpt) — confirm no
   interaction regressions and quantify the aggregate resident-bound + ckpt-
   deadline win. (Done by the orchestrator's uniform sweep / a dedicated e2e
   agent — NOT inline.)
2. **Close the vlog-lifecycle hygiene punch-list** (P1 GC/tombstone, P2 rescale-
   clip reclaim) — a *correctness/space-safety* track, separable from perf,
   that gates "lockable under sustained disagg" not "faster".
3. Treat any remaining read-path leverage (cross-SST coalescing, single-iterator
   parallel read) as **PMC-1's** consolidated perf track.

This is the honest call: **building is done; validate what's built.**

---

## Constraints honored

Flag-gated, default-OFF, byte-identical when OFF (count regime → 1 permit/upload,
semaphore sized to `MAX_INFLIGHT_UPLOADS`). TDD (pure-function + real-Semaphore
tests). No NEXMark, no real S3 — mock mini-bench only. `cargo fmt` + `clippy
--all-targets` + `RUSTDOCFLAGS=-D warnings cargo doc` clean; io suite 272+31 green.
