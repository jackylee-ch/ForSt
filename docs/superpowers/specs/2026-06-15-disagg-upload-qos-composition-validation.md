# Disagg UPLOAD-side QoS composition + interference validation (Phase-2, cycle-9)

**Status:** mini-bench validated, diagnostic fix shipped, suites green.
**Cross-ref:** the READ-side analogue — [`2026-06-15-disagg-readpool-contention-validation.md`](2026-06-15-disagg-readpool-contention-validation.md)
(read-pool FIFO head-of-line block + `FRS_RS_READ_POOL_FAIRNESS` foreground-first fix).
This doc is the symmetric WRITE-side check: do the three upload-side QoS levers
COMPOSE under a saturating compaction-output upload stream?

## Question

Cycle-5 found + fixed read-IO pool contention. The symmetric write-side question:
under a SATURATING compaction-output upload stream competing with a
latency-critical flush upload, do the three upload QoS levers compose or interfere?

| Lever | Env | Mechanism (in `opendal_backend.rs` / `throttle.rs`) |
|---|---|---|
| Reserved flush lane | `FRS_UPLOAD_FLUSH_RESERVED` (261366a06) | carves `reserved` permits into a flush-only lane; flush `try_acquire_many_owned` reserved-first, else shared |
| Byte budget | `FRS_UPLOAD_BYTE_BUDGET_MIB` | sizes the semaphore as a MiB budget; each upload reserves `ceil(bytes/1 MiB)` permits |
| Rate-split | `FRS_UPLOAD_RATE_SPLIT` | paces COMPACTION-class write *bytes* at `compaction_share` (def 0.5) of the remote BW |

Each was validated in ISOLATION (`upload_flush_qos`, `upload_byte_budget`,
`disagg_write_backpressure`). They touch THREE different seams of the SAME upload
— semaphore *weight*, lane *choice*, per-byte *pacing* — so they can interfere.

## Mini-bench

`crates/forst-rs-bench/src/bin/disagg_upload_qos_compose.rs` — a faithful timing
model of the upload admission path (no engine state, no S3), the write-side
analogue of `disagg_readpool_contention`. It mirrors the real semantics exactly:
a two-lane permit budget (`upload_sem` shared + `flush_sem` reserved, flush
reserved-first), per-upload permits = `upload_permits_for` (1 in count regime,
`ceil(mib)` in byte regime), and a compaction permit HOLD time lengthened by
rate-split's sub-rate (the `ThrottleShared.charge` model). A flush stream issues
small latency-critical uploads at a steady cadence; a compaction stream saturates
the budget with 64-MiB SSTs. Run: `cargo run -p forst-rs-bench --bin
disagg_upload_qos_compose --release [-- --smoke]`.

### Results (256 MiB/s throttled endpoint, budget 8 count / 192 MiB byte, flush 4 MiB, compaction 64 MiB)

**COUNT regime — per-lever marginal flush p99:**

| cell | flush p99 | resv hit | note |
|---|---|---|---|
| all-OFF | ~1.2–2.3 s | 0% | compaction saturates the class-blind budget |
| +reserved | **~0 ms** | 100% | the cycle-8 win |
| +rate-split (no resv) | **~3.6–8.6 s WORSE** | 0% | H2: slower compaction holds permits longer |
| +reserved +rate-split | **~0 ms** | 100% | compose cleanly — reserved DOMINATES |

**BYTE regime, TIGHT budget (128 MiB ≈ 2 compaction SSTs):**

| cell | flush p99 | resv hit | note |
|---|---|---|---|
| byte no-QoS (resv=0) | ~5.1 s | 0% | flush queues behind compaction |
| all-ON, resv=1 (count intuition) | ~0 ms | **0%** | H1: a 1-permit lane can't grant a 4-permit flush |
| all-ON, resv=flush_mib (4) | ~0 ms | **100%** | composed — lane sized to a whole flush SST |

## Findings

**H2 — rate-split × reserved (CONFIRMED interference, fixed by composition).**
Rate-split *alone* (no reserved lane) makes the flush tail WORSE than all-OFF
(~1.2 s → ~3.6–8.6 s): pacing slows compaction bytes, so each compaction holds
its shared permit longer, and a flush queuing on the (absent) reserved lane waits
behind a slower drain. But with the reserved lane ON, flush never touches the
shared lane, so reserved+rate-split compose cleanly (flush p99 → ~0). The reserved
lane DOMINATES — it must be ON whenever rate-split is ON.

**H1 — byte-budget × reserved UNIT mismatch (CONFIRMED footgun).** When the byte
budget is on, the reserved lane is also in MiB-permit units (the resolver shares
`upload_sem_total_permits`), so a 4-MiB flush needs `ceil(4)=4` permits. A
`reserved=1` lane (count intuition: "one flush slot") can NEVER grant them →
`try_acquire_many_owned(4)` always fails → the reserved lane is a **silent no-op**
(reserved-hit 0%). Sizing it to `flush_mib` MiB-permits restores 100% hit.

**Surprise (honest):** in the byte regime the flush *p99 stays ~0 either way* — a
4-permit flush is tiny against even a 128-MiB shared budget, so the **byte budget
itself supplies the granular shared-lane headroom that prevents flush
starvation**. The reserved lane is the COUNT-regime fix; the byte budget is an
INDEPENDENT structural fix for the same starvation. They do not conflict — but a
mis-sized reserved lane under the byte budget silently does nothing.

**Work-conservation:** the reserved lane carves only `flush_mib` permits, leaving
`budget - flush_mib` always available to compaction (structural). The
wall-normalized compaction-rate dip (reserved-only 0.51× of no-QoS) is a QUEUEING
artifact — the no-QoS cell's flush queues for seconds, lengthening its wall and
inflating its rate — NOT starvation. All-ON vs reserved-only retains ~0.50×,
which is rate-split INTENTIONALLY pacing compaction to its 0.5 share (the lever
working).

## Fix shipped

The H1 interference is a CONFIGURATION footgun, not a code defect — the levers
compose correctly when the reserved lane is sized in MiB. The minimal fix
(analogous in spirit to the read-pool fairness fix, but diagnostic since no data
path is wrong): `warn_reserved_lane_undersized_for_byte_budget` in
`opendal_backend.rs::build_upload_semaphores`. It emits a ONE-TIME WARN when BOTH
levers are already ON and the reserved lane is `1` MiB-permit (sub-SST, the silent
no-op), telling the operator to size the lane to >= one flush SST in MiB (or rely
on the byte budget alone). It logs only — changes NO permits, NO bytes, NO data
path — so it is byte-identical whether or not it fires. Pure predicate
`reserved_lane_is_undersized` is unit-tested.

## Correctness / safety

- All three levers remain flag-gated default-OFF, byte-identical OFF (their own
  tests + the engine UTs).
- The new diagnostic fires only when both levers are ON and the lane is sub-SST;
  it is a `tracing::warn!` with no behavioural effect.
- `forst-rs-io` opendal_backend tests 36/0 (incl. new `reserved_lane_undersized_predicate`).
  fmt + clippy + `RUSTDOCFLAGS="-D warnings" cargo doc` clean on `forst-rs-io` +
  `forst-rs-bench`. `--smoke` asserts: COUNT reserved cuts p99; H2 rate-split-alone
  does NOT fix but reserved+rate-split compose; H1 sizing signal (resv-hit 0%→100%);
  byte budget keeps flush tail low either way.

## Honest negatives / scope

- TIMING MODEL, not a 100M NEXMark run. Faithfully mirrors the permit-contention
  structure (two-lane budget, MiB-weighted permits, paced compaction hold), not
  real object-store RTT variance, opendal `.concurrent()` multipart constants, or
  multi-tenant noise. The H1/H2 findings are structural and hold regardless of the
  exact constants; absolute latencies are regime-dependent.
- The contended regime is a THROTTLED endpoint (default 256 MiB/s). At
  `FRS_REMOTE_BW_MBPS=6250` (50 Gb/s) every upload is sub-ms ⇒ no contention to
  study — the disagg levers only matter on a bandwidth-limited channel.

## Verdict

The three upload QoS levers **compose** once correctly configured, with two real
interactions surfaced and addressed: **H2** (rate-split alone worsens the flush
tail; the reserved lane must accompany it — they then compose), and **H1** (under
the byte budget the reserved lane must be sized in MiB or it silently no-ops —
now a one-time WARN, and moot anyway because the byte budget itself bounds flush
starvation). The reserved lane is the COUNT-regime fix; the byte budget is the
independent BYTE-regime fix; rate-split paces compaction by design. Diagnostic
fix shipped, byte-identical, suites green. **The Phase-2 read AND write disagg
QoS surfaces are now both validated + composed at the mini-bench level** — PMC-2
should HOLD for the real-BOS / online-box NEXMark + S3 e2e run (gated on Phase-1
release / user OK).
