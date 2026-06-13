# Write-amplification — the fundamental, reusable fix for forst-rs (local LSM + disagg/remote)

Date: 2026-06-13 · Author: PMC-1 architecture arm · Worktree: `forst-rs` (base `c03dc5d2a`)
Builds on (do NOT duplicate): `2026-06-12-sorted-run-discipline-design.md` (M1-M6 + churn_probe
evidence), `2026-06-13-write-path-redesign-survey.md` (KV-sep/TTL-segment paradigm cells),
`2026-06-13-master-strategy-beat-forst-rocksdb.md` (scoreboard + S3 model),
`2026-06-13-phase2-disaggregated-state-design.md` (FileMappingManager, opendal-fs),
`2026-06-13-remote-compaction-design.md` (`CompactionMergeExecutor` seam),
`2026-06-13-wa-v0-lifecycle-watermark-feed-scope.md` (lifecycle fix is INERT — runtime untouched).

Evidence rule honored: every quantitative claim below traces to a measurement in §1 or a
`file:line` code citation verified against the worktree at base `c03dc5d2a`. Anything not
backed by a measurement is labeled **needs micro-bench** or **model**.

---

## 0. Headline

The write-amp fix is **not a new lever** — the levers exist and are measured. The fundamental,
reusable move is to **promote the compaction *picking* decisions out of the two inline copies in
`db.rs` into ONE shared `CompactionPolicy` module**, mirroring the already-shared
`CompactionMergeExecutor` *execution* seam. Today:

- **Execution** is already one strategy, two call sites: `compact_l0_for_cf` (`db.rs:8757`) and
  `compact_level_for_cf` (`db.rs:5894`) both call `self.current_compaction_executor().execute(job)`
  — Local in-process OR RemoteEmulated offload (`compaction_executor.rs:65`). REUSABLE by design.
- **Picking** is the opposite: the M1 clean-cut (`overlap_scoped_clean_cut`, `db.rs:13948`), M4
  dynamic-level/score/min-overlap-ratio/tombstone-compensation (`db.rs:5638-5784`, `compute_base_level`
  `:14016`, `dynamic_level_target` `:14034`, `compensated_file_size` `:14047`), and WA-V3 trivial-move
  short-circuit (`db.rs:8641`, `:5815`) are **inlined and partially duplicated** across the two seams.
  That duplication is why "fundamental" today means "edit two places and hope they agree", and why the
  disagg/remote-compaction path inherits the policy only by accident (it executes whatever db.rs picked).

**The design:** a `CompactionPolicy` trait + a single `SortedRunPolicy` implementation that owns
*what to compact* (input scope, output level, source file, trivial-move eligibility, backpressure run
counts), consumed by **both** compaction seams locally **and** carried in the `CompactionJobDescriptor`
so the **remote/offloaded worker picks under the identical policy**. One policy object, four call
sites that all defer to it (L0-local, Ln-local, L0-remote-describe, Ln-remote-describe) — provably
reusable because there is exactly one implementation and the byte-identical falsifier
(`remote_compaction_it.rs`, already green) gates any divergence.

**Projected per-query write-amp reduction** (churn_probe ratio, then the q-class wall it gates):

| Lever (all already built, default state) | churn_probe write-amp | gates priority queries |
|---|---|---|
| Baseline (current default, M4 dynamic-levels ON) | **7.68×** (sorted-run §2.1 cell A) | all write-bound: q4,q7,q9,q11,q17,q19,q20 |
| + bigger memtable (already locked 1 G; cell I @256 MiB) | **4.45×** (survey §2.2 cell I) | within-paradigm floor |
| + `FRS_TRIVIAL_MOVE` (WA-V3, default OFF) | **0.98×** on non-overlapping rollups (wa-v0:153) | skewed/per-key-group CFs, migration |
| + `FRS_KV_SEPARATION` (WA-V2a, default OFF) | **1.55×** q7-churn (wa-v0:152); cell E′ key-LSM 2.10× → model 1.36× | q7/q9/q20 (value-heavy join state) |
| RocksDB intrinsic on the same workload | 3.91× (sorted-run §2.1 cell E) | the bar |

The fundamental fix is to make these **one policy** (so they compose correctly and apply identically
local + remote) and to land the default flips behind the policy's flag surface with the ON/OFF
byte-identical contract. The wall-clock benefit is bounded by §1's iostat: at 7.68× the remote disk is
**98-99 % util, writes 435-682 MB/s > reads 190-292 MB/s** (sorted-run §1.1); halving write volume
lifts the disk out of saturation, which is the measured binding resource for q7/q9/q20 on the box.

---

## 1. Root cause — grounded in churn/iostat (verified in code)

### 1.1 The measurement (cite, do not re-run)

`churn_probe` (`crates/forst-rs-bench/src/bin/churn_probe.rs`, q7-interval-join shape, 200 K rows/s,
200 B incompressible values, ~1 GiB live, 90 s × 3, medians — sorted-run §2):

```
default     write_amp=7.68 p50_late=649µs  p99=2205µs  last_l0=1     ← current frs
rocksdb     write_amp=3.91 p50_late=162µs  p99=279µs   last_l0=2     ← the bar (1.96× fewer bytes)
nocompact   write_amp=0.97 p50_late=2997µs           last_l0=41    ← compaction owns ~87% of bytes
bigbuf256   write_amp=4.45 (survey cell I)                          ← within-paradigm floor
```

iostat A/B, same box/same disk, q7 @100M (sorted-run §1.1): **ForSt** writes ~110 MB/s @ **6 % util**
(leveled discipline + Snappy, hot set cached); **forst-rs** writes 435-682 MB/s + reads 190-292 MB/s @
**98-99 % util**. Per-event: ForSt ≈ 1.4 KB/event, forst-rs ≈ 13 KB/event ≈ **~10×**. The remote q7 gap
is an **I/O-volume** problem (write-amp × compression × read-amp), NOT probe CPU (S2 micro-win was only
+3.1 % at 100M — disagg §3.1) and NOT the FFM boundary (≈0, q7-analysis).

### 1.2 Why 7.68× — the picking decisions that cause the extra rewrites (file:line)

Three classes of decision, all verified in the current code:

1. **L0→L1 input scope (the historical W1).** `compact_l0_for_cf` now scopes the rollup to the
   *overlap-subset, clean-cut* of the output level (`out_overlap_files = overlap_scoped_clean_cut(...)`,
   `db.rs:8633`; fixpoint expansion `:13948-13996`) — NOT the whole CF level. **This is already fixed**;
   the 7.68× is what remains AFTER it. Residual: under uniform hash keys (q7's buckets) L0 usually spans
   the whole space, so the overlap subset ≈ the whole level — the win lands for skewed/narrow CFs
   (timer CFs, per-key-group CFs), not for q7's uniform churn. (sorted-run §4 M1 effect bound.)

2. **Level cascade depth — the single largest contributor (the historical M4).** Legacy mode cascaded
   L1→L2→…→Ln through fixed `base × mult^(L-1)` targets, paying a rewrite per level. **This is already
   fixed and default-ON**: `dynamic_levels_from_env()` defaults true (`db.rs:14002`); `compute_base_level`
   (`:14016`, RocksDB `CalculateBaseBytes` parity) anchors targets at the bottom level's actual size and
   sends an L0 rollup straight to its resting base level (`rollup_output_level`, `:5604`); the descent
   picker uses **min-overlapping-ratio** (`overlap_bytes/compensated_size`, `:5753-5784`,
   `kMinOverlappingRatio` parity) and **tombstone-compensated** sizes (`compensated_file_size`, `:14047`,
   weights deletes ×2·avg so TTL garbage descends toward annihilation). The 7.68× cell already runs with
   this ON — it is the measured floor of the *within-paradigm* scheme. The code comment at `:5748-5750`
   records the falsifier: max-compensated-size-only picking ended at write-amp ~8.2; the min-overlap-ratio
   pick is what brought it down to the 7.68 regime.

3. **Every byte is rewritten, never moved; one merge at a time (W2/W3).** There is no metadata-only
   re-level on the default path: `FRS_TRIVIAL_MOVE` (`trivial_move_enabled()`, `db.rs:14658`, **default
   OFF**) adds the WA-V3 short-circuit that emits a delete-at-Ln + add-at-Ln+1 `VersionEdit` with zero
   rewrite when inputs are mutually disjoint with no destination overlap and no compaction filter
   (`db.rs:8641` L0, `:5815` Ln). Measured: non-overlapping rollups 2.95→**0.98×** (wa-v0:153). Under
   uniform q7 keys most descents DO overlap → trivial-move alone is small for q7 (sorted-run §4 M2), but
   it is the enabling primitive and is free on skewed CFs + during migration.

**Conclusion (root cause restated):** the M1 clean-cut and M4 dynamic-levels fixes are LANDED and
default-ON, so the 7.68× is **not** a cascade-depth or whole-level-rewrite artifact anymore — it is the
intrinsic rewrite volume of a *leveled LSM that rewrites values on every compaction* at ~1 GiB live
state with uniform-hash churn. Closing the last 7.68→3.91 (RocksDB) and below is therefore NOT another
picking tweak — it is (a) **rewriting fewer bytes** (KV-separation: only keys+pointers ride the
treadmill; values written once) and (b) **moving instead of rewriting** where ranges allow
(trivial-move), under (c) **one shared policy** so both compose correctly and apply local + remote
identically. That is the fundamental, reusable fix; §2.

> **needs micro-bench (honest gap):** the residual 7.68× with M1+M4 already ON has NOT been
> decomposed into "flush floor + clean-cut residue + descent rewrites" by a per-phase counter on the
> *current* tip. The FRS-WAMP tooling (`FRS_WAMP_FILE`, `db.rs:14055+`) emits the cumulative ratio per
> compaction; a churn_probe run with it ON (a future, perf-window-safe micro-bench) would attribute the
> residual. The design does not depend on that attribution — the paradigm moves (KV-sep/trivial-move)
> attack the rewrite *volume* regardless of which level dominates it.

---

## 2. The fundamental fix — one shared sorted-run-discipline policy, two (four) call sites

### 2.1 What is already reusable, and the one gap

| Concern | Today | Reusable? |
|---|---|---|
| **Execution** (k-way merge + output write) | `CompactionMergeExecutor` trait (`compaction_executor.rs:65`), `LocalCompactionExecutor` (=`job.run()`) / `RemoteEmulatedCompactionExecutor`; both seams call `current_compaction_executor().execute(job)` (`db.rs:5894`, `:8757`) | **YES** — one strategy, two call sites; byte-identical falsifier green (`remote_compaction_it.rs`) |
| **Install** (`VersionEdit` apply) | `version_set.apply` under `apply_lock`, validates inputs present (R44-L2) | **YES** — identical for every strategy |
| **Picking** (input scope, output level, src file, trivial-move eligibility, run-count backpressure) | INLINED in `compact_l0_for_cf` + `compact_level_for_cf`, partially duplicated (the two trivial-move short-circuits `:8641`/`:5815`, the two clean-cut/overlap computations `:8633`/`:5801`, the two `is_bottommost` derivations `:8690`/`:5849`) | **NO — this is the gap** |

The execution seam already proves the pattern. The fix applies the *same* pattern to picking.

### 2.2 The `CompactionPolicy` trait (the picking strategy seam)

```rust
// crates/forst-rs-engine/src/compaction_policy.rs   (new)

/// Strategy for *picking* a compaction: given an immutable Version and a CF,
/// decide WHAT to compact. Produces either a metadata-only re-level
/// (trivial move — no merge) or a `CompactionPlan` the caller turns into a
/// `CompactionJob` and hands to the `CompactionMergeExecutor`. The PLAN is the
/// portable, executor-agnostic description of the merge; the EXECUTION
/// (local/remote) and the INSTALL (`version_set.apply`) are unchanged.
pub trait CompactionPolicy: Send + Sync {
    /// Pick the next compaction for `cf` at the L0→base seam.
    fn pick_l0_rollup(&self, v: &Version, cf: ColumnFamilyId, ctx: &PolicyCtx)
        -> Option<CompactionDecision>;
    /// Pick the next Ln→Ln+1 descent for `cf` (or None if every level fits).
    fn pick_level_descent(&self, v: &Version, cf: ColumnFamilyId, ctx: &PolicyCtx)
        -> Option<CompactionDecision>;
    /// L0 run count that feeds the write_controller slowdown/stop triggers
    /// (counts lifecycle segments / shadow runs into the budget — M5/M6).
    fn backpressure_run_count(&self, v: &Version, cf: ColumnFamilyId) -> usize;
    fn kind(&self) -> CompactionPolicyKind;
}

pub enum CompactionDecision {
    /// Metadata-only re-level (WA-V3 trivial move) — no merge, no executor.
    TrivialMove { deletes: Vec<(u32, FileNumber)>, adds: Vec<(u32, SstFileMeta)> },
    /// A real merge to run through the CompactionMergeExecutor.
    Merge(CompactionPlan),
}

/// The executor-agnostic merge description (everything the seam computes BEFORE
/// `CompactionJob` is built: input identities + levels, output level, bottommost
/// flag). File-number allocation + reader-open + snapshot horizon stay caller-side
/// (the primary's monotonic authority), exactly as the remote descriptor requires.
pub struct CompactionPlan {
    pub inputs: Vec<(u32 /*level*/, SstFileMeta)>,
    pub output_level: u32,
    pub is_bottommost: bool,
}

/// Read-only inputs the policy needs that are NOT on the Version: tombstone
/// counts from cached reader footers, level geometry, the dynamic-levels and
/// trivial-move flag state. Passed by ref so the policy does ZERO I/O.
pub struct PolicyCtx<'a> {
    pub num_levels: usize,
    pub max_bytes_for_level_base: u64,
    pub max_bytes_for_level_multiplier: f64,
    pub dynamic_levels: bool,
    pub trivial_move: bool,
    pub tombstones: &'a dyn Fn(FileNumber) -> u64, // cached-reader footer lookup
    pub compaction_filter_active: bool,
}

pub enum CompactionPolicyKind { SortedRun }

/// THE one implementation. Owns the M1 clean-cut, M4 dynamic-level/score/
/// min-overlap-ratio/tombstone-compensation, and WA-V3 trivial-move logic that
/// is inlined in db.rs today. No per-query branches — one discipline.
pub struct SortedRunPolicy;
```

`SortedRunPolicy::pick_l0_rollup` is the body of `compact_l0_for_cf` from "pick L0 files" through the
clean-cut + trivial-move-eligibility + `is_bottommost` derivation, returning a `CompactionDecision`.
`pick_level_descent` is the body of `compact_level_for_cf` from `pick_compaction_level_for_cf`
(`db.rs:5638`) through min-overlap-ratio src pick + overlap gather + trivial-move-eligibility. The
*moved* code is the picking; the *kept-in-db.rs* code is file-number allocation, reader open,
`min_active_snapshot = snapshot_registry.min_active()`, job build, and the executor/install calls.

### 2.3 Where it plugs in — proving REUSABILITY by design

**Local LSM path (two call sites, one policy):**

```
compact_l0_for_cf (db.rs:8757)                compact_level_for_cf (db.rs:5894)
  decision = policy.pick_l0_rollup(v,cf,ctx)    decision = policy.pick_level_descent(v,cf,ctx)
  match decision {                              match decision {
    TrivialMove{..} => version_set.apply(edit)    TrivialMove{..} => version_set.apply(edit)
    Merge(plan)     => {                          Merge(plan)     => {
      build CompactionJob from plan + fs +          build CompactionJob from plan + fs +
        merge_op + filter + min_active_snapshot       merge_op + filter + min_active_snapshot
      current_compaction_executor().execute(job)    current_compaction_executor().execute(job)
      version_set.apply(edit)                       version_set.apply(edit)
```

**Disagg / remote-compaction path (same policy, carried in the descriptor):** the
`CompactionJobDescriptor` (`remote-compaction-design §3.2`, built `compaction_executor.rs`) already
captures inputs-by-identity + output level + pre-allocated output file numbers + `min_active_snapshot`
+ merge/filter-by-name. The picking that produced those inputs is `SortedRunPolicy`'s `CompactionPlan`.
The remote worker does **not** re-pick — the primary picks under `SortedRunPolicy`, serializes the
resulting plan into the descriptor, and the worker executes the merge it was handed. So:

- the **primary** (TM) runs `SortedRunPolicy` once (local CPU, cheap — pure metadata, no I/O);
- the **worker** (remote / offloaded) runs the SAME merge the policy chose, reading inputs / writing
  outputs through the job's `Arc<dyn FileSystem>` (the opendal/cached remote stack on the disagg mode,
  `compaction_executor.rs:28-32`);
- **install** is the same `version_set.apply` edit either way.

**Four call sites, one policy object:** L0-local, Ln-local, L0-remote-describe, Ln-remote-describe all
go through the single `SortedRunPolicy`. There is exactly one implementation; the byte-identical
falsifier (`remote_compaction_produces_byte_identical_version_state`, `remote_compaction_it.rs`, already
green) proves the local and remote executions of a policy-chosen plan agree to the byte (mod the
12-byte `creation_time` footer). Adding `pick_*` to the descriptor's provenance does not change what
crosses the wire — the descriptor already carries the plan; the policy just becomes the *named* author
of it instead of two inline copies. **This is the reusability proof: refactor, not re-architecture —
the seam the remote path needs already exists; we are removing the duplication that made "fundamental"
mean "two places".**

### 2.4 Why this is the *fundamental* fix and not a mitigation

Mitigations (lz4/KV-sep/trivial-move/S2) each attack one term. The fundamental property is: **the
rewrite *volume* — the 87 % of physical bytes that compaction owns (survey P2) — is governed by ONE
policy that (a) rewrites fewer bytes (KV-sep makes the LSM carry 36 B keys+pointers, not 236 B rows —
survey §3.1, value-log GC = segment unlink on the FileMappingManager), and (b) moves instead of
rewriting where ranges allow (trivial-move), and (c) does so identically on local disk and on remote
S3.** No per-query branch exists in `SortedRunPolicy`; the same discipline serves q4 (write-heavy
join+agg), q7/q9/q20 (value-heavy join state), q11/q19 (OVER-window/dedup), q17 (group-agg). A query is
helped exactly insofar as it is rewrite-bound (§6).

---

## 3. Composition with the existing mitigations (they stay; the fix reduces the bytes beneath them)

| Mitigation | State | How it composes with the shared policy |
|---|---|---|
| **lz4 SST compression** (`config.rs:268` default; remote runner pins `none`, sorted-run W5/M5) | shipped; a config flip on the remote runner | Pure divisor on the *physical* bytes the policy writes. Policy reduces logical rewrite count; lz4 shrinks each. Orthogonal multiply. M5 = stop pinning `FRS_SST_COMPRESSION=none` on the remote so frs matches RocksDB/ForSt Snappy — independent config commit. |
| **KV-separation** (`FRS_KV_SEPARATION`, `db.rs:14572`, default OFF; `kv_gc` in `CompactionJob`) | built | The policy picks the SAME inputs; the *executor* relocates only over-cutoff vlog pointers (`kv_gc` carried in plan→job→descriptor). Key-LSM stays shallow (cell E′ 2.10× vs 7.68×) → the policy now disciplines ~6× fewer bytes. Merge-operand CFs (ListState `RawConcatMergeOperator`, ffi `lib.rs:162`) are exempt (survey P12). |
| **Trivial-move** (`FRS_TRIVIAL_MOVE`, `db.rs:14658`, default OFF) | built | Becomes the `CompactionDecision::TrivialMove` arm of `SortedRunPolicy` — runs FIRST, never reaches the executor (remote-compaction §2.1: TM 0, remote 0). Composes with KV-sep (a non-overlapping rollup re-levels keys+pointers by metadata; values untouched). |
| **S2 pinned-rows / loser-tree** (`FRS_RS_S2_PINNED`, `s2_pinned_enabled`) | built, flag-OFF | Read-side; benefits from the bounded fan-in the policy maintains (heap width = run count). Under KV-sep the loser-tree merges 36 B entries not 236 B (survey §3.1) → S2 buffers shrink ~6×. Compose, don't conflict. |
| **M4 dynamic levels** (`FRS_DYNAMIC_LEVELS`, default ON, `db.rs:14002`) | shipped default-ON | IS the core of `SortedRunPolicy` (base-level/score/min-overlap-ratio/compensation). The refactor moves this code into the policy module verbatim; behavior unchanged (the `force_dynamic_levels`/`force_fixed_levels` test hooks `:5546`/`:5553` become policy-construction choices). |
| **1 G memtable + 6 GB WBM** (locked config) | shipped | First-order write-amp lever in the current scheme (cell I 4.45×); bigger flushes amortize the L0 rollup. The policy's `backpressure_run_count` feeds the same `write_controller` triggers; M5 tightens 40/64→20/36 once concurrency lands (sorted-run M5). |

The mitigations are **not replaced** — the shared policy is the substrate they all plug into, which is
why "one policy" makes them compose correctly instead of fighting (e.g. trivial-move must run before the
executor; KV-sep's `kv_gc` must ride the same plan the policy chose; M6 must count lifecycle/shadow runs
into the same backpressure budget the policy reports).

---

## 4. Disagg-state applicability — sorted-run discipline when SSTs are remote/offloaded

**On remote-primary (Phase-2), write-amp ≈ upload-amp.** Every physical byte flush/compaction writes is
a byte uploaded to S3 (write-through, `cached_fs.rs:33-40`; survey P4). The master-strategy S3 model
makes this explicit: `δ_upload(q) ≈ write_amp × ingest_bytes / BW` as background interference
(master-strategy §3.2). disagg-competitive §3.1 quantifies it: at 7.68× a q7-class ~40-80 MB/s ingest
becomes **~300-600 MB/s sustained PUT traffic** — exactly the 435-682 MB/s the iostat capture showed
hitting local disk — and the write-through cache means **those bytes still hit local disk too, so the
disk stays the binding resource** (the same compounding loop that DNF'd q7). At 3.91× (RocksDB parity)
the term halves; KV-sep's 1.55× quarters it.

**So the dominant remote cost IS compaction-rewrite churn** — disagg-competitive §4.3 names it "the
single biggest risk: write-amp × remote-primary = continuous S3 upload amplification feeding the same
disk-saturation loop." This ties directly to PMC-2's mock-S3 evidence that **KV-separation's lever is
compaction churn**: the key-LSM (cell E′) is 2.10× because its live set is ~137 MiB (4 M × 36 B) and
never grows the cascade, while the 1 GiB of *values* lands in append-once logs whose GC on S3 is a
`unlink()` through the FileMappingManager (1.4 µs/link vs 4.5 ms/copy = **3172×**, phase2 §8;
`file_mapping.rs` register/link/unlink are file-kind-agnostic). Streaming-state death order ≈ arrival
order, so a value-log segment ages out *as a whole* — value-log GC for lifecycle CFs is exactly segment
expiry, zero re-writes (survey §3.1).

**How the shared policy behaves when SSTs are remote:**

1. **Picking is unchanged** — it is pure metadata over the Version (file metas, key bounds, cached
   footer tombstone counts). No I/O, so it costs the same whether the files live on NVMe or S3. The
   policy runs on the primary (TM) regardless of where the bytes are.
2. **Execution moves off the TM** via the RemoteEmulated executor — the worker reads inputs / writes
   outputs through the opendal stack, so the TM does ~0 compaction CPU and ~0 compaction I/O (measured
   ~130-180× TM-CPU cut, remote-compaction §6.3). The policy's *choice of fewer/smaller rewrites*
   directly divides the worker's S3 read+write volume.
3. **Trivial-move is strictly better remotely** — a metadata-only re-level uploads NOTHING (the file
   stays the same S3 object, only its level-in-the-manifest changes); on S3 a rewrite would be a
   full GET-inputs + PUT-output. Every trivial-move the policy emits saves one S3 round-trip of file
   bytes. This is the link-checkpoint synergy: outputs are DFS objects, "install" is the same adopt/link
   (remote-compaction §3.6).
4. **The picking discipline shrinks file COUNT** (M-moves + 1 G memtables), which directly cuts S3
   PUT/DELETE metadata-op pressure (disagg-competitive A3) — a second remote-only win the local
   iostat doesn't see.

**Boundary (honest):** every S3 *performance* number here is a **model / needs the online box**. The
dev Mac is 10 MB/s to BOS (recorded 2026-06-01) — no valid S3 perf evidence comes from it. What is
*locally* measurable is in §5. The disagg applicability claim rests on (a) the code path being real
(opendal-fs emulation + RemoteEmulated executor, both with green ITs) and (b) the write-amp ratio being
the term that multiplies into `δ_upload` — both verified; the *magnitude* on a 50 Gb/s co-located box is
A1/A2 in the assumptions register (disagg-competitive §3.3) and must be validated remotely.

---

## 5. Rollout — flag-gated, byte-identical-when-OFF, default-OFF

### 5.1 The contract

- **Refactor stage (the policy module) is behavior-preserving and ON by construction**: moving the
  inline picking into `SortedRunPolicy` must produce byte-identical compaction outputs. Gate: the full
  engine suite + a new property test asserting `SortedRunPolicy` chooses the SAME inputs/output-level as
  the pre-refactor inline code over randomized Versions (§5.3 T1). No flag — it is a pure extraction.
- **Each *paradigm* lever stays behind its existing flag, default-OFF**: `FRS_TRIVIAL_MOVE`,
  `FRS_KV_SEPARATION` (both already default-OFF, `db.rs:14658`/`:14572`). The policy reads
  `PolicyCtx.trivial_move`/the kv-sep flag; OFF ⇒ the `TrivialMove` arm is never taken and `kv_gc` is
  `None` ⇒ byte-identical to today.
- **The config flips (M5: 20/36 triggers, lz4-on-remote) are separate config-only commits**, each with
  its own remote A/B (sorted-run §6 S5). Default unchanged until the A/B clears the no-regression gate.
- **M4 dynamic-levels stays default-ON** (it already is, and is the measured 7.68× floor); the
  `FRS_DYNAMIC_LEVELS=0` escape hatch is preserved for the same-session A/B falsifier.

### 5.2 ON-vs-OFF measurement protocol (all locally runnable EXCEPT the online-box row)

**Locally measurable (mock-S3 + churn_probe — do these in a perf-window-isolated session):**

1. **churn_probe write-amp ratio**, same-session A/B, n=3 medians, per lever:
   - baseline (default) → expect 7.68× (re-baseline; survey A′ got 7.83, ±2 %).
   - `FRS_TRIVIAL_MOVE=1` → expect ≈baseline on uniform q7 keys (small), 0.98× on the `--no-deletes`
     non-overlapping variant (wa-v0:153).
   - `FRS_KV_SEPARATION=1` → expect q7-churn ≈1.55× (wa-v0:152), key-LSM cell ≈2.10× (survey E′).
   - Gate **G1**: the combined ON stack write-amp median < 4.0 (from 7.68) with probe p50 late ≤ 700 µs.
2. **mock-S3 upload-amp** via `OpendalFileSystem::local`/`memory://` emulation (opendal_backend
   `:362`): assert physical-write bytes == upload bytes (write-through), and that trivial-move emits
   ZERO upload bytes for a re-leveled file (count PUTs on the recording opendal leg — the same
   count-assertion pattern the phase2 ITs use). Gate **G2**: trivial-move rollup → 0 PUTs of file bytes.
3. **remote-compaction byte-identical falsifier** (already green): a plan executed Local vs
   RemoteEmulated yields byte-identical outputs (mod 12-byte creation_time). Gate **G3**: stays green
   after the policy refactor (the policy now authors the plan both executors run).
4. **offload TM-CPU** mini-bench (`remote_compaction_offload.rs`, already exists): caller-thread
   compaction CPU Local vs Remote (recorded ~13 ms → ~0.1 ms). Gate **G4**: direction holds (TM CPU → 0).

**Needs the online box (NOT valid on the dev Mac — standing rule):**

5. **per-query NexMark A/B** on the remote 8c/32g x86/NVMe box, same-session pairs, n≥3, for the
   priority set q4/q7/q9/q11/q17/q19/q20: baseline vs the ON stack (trivial-move + kv-sep + lz4-on-remote
   + 20/36 triggers). Gate **G5**: q7 wall ≤ ~1370 s (ForSt-remote 1379.7 ≈ RDB 1367.6 is the de-facto
   bar, sorted-run §1); write-MB/s share of disk drops ~proportionally to the write-amp cut; q9/q20
   improve 15-30 % from de-saturation (disagg-competitive §3.3 projections). **No S3 perf claim from the
   Mac.**
6. **no-regression** (G6): q17 (passing group-agg) and the light source-bound set (q3,q0-2,q10,q12-14,
   q21,q22) must not regress > 5 % under the ON stack default-flip candidate.

### 5.3 TDD test list (write the falsifiers FIRST)

- **T1 policy-equivalence (the refactor gate):** over randomized synthetic Versions,
  `SortedRunPolicy::pick_l0_rollup`/`pick_level_descent` choose the SAME inputs, output level, and
  trivial-move eligibility as the pre-refactor inline `compact_l0_for_cf`/`compact_level_for_cf` would.
  Falsifies any behavior drift from the extraction.
- **T2 trivial-move-OFF byte-identity:** with `trivial_move=false` the policy NEVER returns
  `TrivialMove`; engine output is byte-identical to the legacy path (existing trivial-move-compose test
  `test_cycle1_kvsep_and_trivial_move_compose_metadata_only_and_exact` stays green).
- **T3 kv-sep-OFF byte-identity:** with kv-sep OFF, `kv_gc` is `None` in every plan; output byte-identical.
- **T4 policy → descriptor → remote round-trip:** a plan authored by `SortedRunPolicy`, serialized into
  `CompactionJobDescriptor`, executed by the worker (round-trip via `FRS_REMOTE_COMPACTION_SERIALIZE=1`),
  yields a byte-identical output to the local run (extends `remote_compaction_idempotent_via_descriptor_round_trip`).
- **T5 backpressure run-count:** `backpressure_run_count` counts lifecycle segments + resident-shadow
  runs into the L0 budget (M6) so the trigger reflects true probe fan-out; unit-tested against
  synthetic Versions with shadow/segment runs.
- **T6 dynamic-level pure-fn parity:** `compute_base_level`/`dynamic_level_target`/`compensated_file_size`
  (already unit-tested as free fns) keep their tests after moving into the policy module — pure-fn parity.
- **T7 MVCC safety unchanged:** the policy never touches `min_active_snapshot` (caller-side); a
  read-while-compact randomized property test stays green under the policy path ×5.
- **T8 concurrency (M3, when it lands):** racing picks under per-level-pair exclusivity → apply-side
  reject + repick; no overlapping outputs; ×5 (sorted-run G3/G4).

---

## 6. Projected benefit per priority query (with reasoning + measure-locally vs online-box boundary)

Reasoning rule: a query benefits from the write-amp fix in proportion to how **rewrite-bound** (write-MB/s
saturating the disk) it is. The §1 iostat says q7-class is write-bound; the JFR history (sweep
2026-06-09) says q9/q20 are *also* partly async-wait-bound (the executor depth-1 lever, H2, is a
SEPARATE axis); q11/q19 are engine read-path-bound (disagg-competitive §3.3 marks them out of the disagg
question). So:

| Query | What it is | Rewrite-bound? | Projected write-amp benefit | Reasoning | Boundary |
|---|---|---|---|---|---|
| **q7** (interval join) | value-heavy join state, ~50 % TTL deletes | **YES — the canonical case** | write-amp 7.68→~1.55× (KV-sep) → disk leaves 98-99 % saturation | iostat: writes 435-682 MB/s @ 99 % util; halving→quartering write volume directly de-saturates the binding resource; ForSt does it at 1.4 KB/event | **online box** for wall (≤~1370 s G5); churn_probe locally for the ratio |
| **q9** (join) | growing inner-join state | PARTLY (also async-wait, H2) | 15-30 % wall from de-saturation; ratio →~1.55× | same churn class as q7, smaller probe component (disagg §3.3); q9 already 1.14× on r1 — write-amp keeps it there under remote-primary δ_upload | **online box** for wall; r2-regression bisect must resolve first |
| **q20** (join, output-amplifying) | large growing join state + 93 M output | PARTLY (write+coordination, sweep DECAY_DIAG) | →~1.0-1.2× (disagg §3.3); ratio →~1.55× | rate oscillates with compaction cycles (sweep) → fewer rewrites = fewer steals; near-miss 1.29× today | **online box** for wall |
| **q4** (join + retract agg) | write-heavy, compaction-sensitive (q4 ckpt-flush load-bearing, memory) | **YES** | ratio →~1.55-2.0×; wall improves as compaction merge-bytes drop | q4 is the write-heaviest; compaction merge CPU + S3 upload both ride write volume; trivial-move helps the agg-state CF if ranges allow | **online box** for wall (q4 0.98× M today — keep under remote δ_upload) |
| **q11** (OVER-window) | dedup/window-agg | NO (engine read-path) | small — not the lever | disagg §3.3 marks q11 out of the disagg question; needs the read-path/executor lever, not write-amp | excluded from the write-amp verdict bar |
| **q17** (group-agg) | bounded-ish group state | NO (passes, 0.94-1.03×) | neutral — must NOT regress (G6) | already passes; the fix must be neutral here (don't rob Peter) | local G6 + online confirm |
| **q19** (OVER-window) | OVER dedup | NO (engine read-path) | small — not the lever | same as q11; engine read-path gap (0.59× today), S3 changes nothing | excluded from the write-amp verdict bar |

**Headline projection:** the write-amp fix is the lever for the **rewrite-bound joins q4/q7/q9/q20** —
write-amp 7.68→~1.55× (KV-sep) / 0.98× (trivial-move on favorable ranges), which on the disk-saturated
remote box translates to leaving 98-99 % util and a projected q7 ≤~1370 s (the ForSt/RDB bar), q9
holding 1.14×, q20 →~1.0-1.2×, q4 holding ≤1.0×. For **q11/q17/q19** the fix is **neutral by design**
(q17 must not regress; q11/q19 are read-path-bound and excluded from the write-amp bar — they need the
separate executor/read-path lever). What is locally measurable: the churn_probe write-amp ratio,
mock-S3 upload-amp = write-amp, the byte-identical remote falsifier, and TM-CPU offload. What needs the
online box: every per-query NexMark wall and every S3 upload-amp magnitude — no S3 perf claim is valid
from the dev Mac.

---

## 7. Staged implementation (commit-sized; design only — do NOT implement here)

1. **P1 — extract `compaction_policy.rs` + `SortedRunPolicy`** (refactor; T1/T6 gates; byte-identical).
   Move the inline picking from both seams into the policy; db.rs keeps alloc/open/snapshot/job/execute/
   install. No flag. This is the fundamental, reusable structural change — one policy, four call sites.
2. **P2 — carry the `CompactionPlan` provenance into `CompactionJobDescriptor`** (T4); the remote worker
   executes a policy-authored plan. No behavior change (descriptor content unchanged; the policy is now
   the named author).
3. **P3 — default-flip A/B program** (config-only, each its own commit + remote A/B): `FRS_TRIVIAL_MOVE`,
   `FRS_KV_SEPARATION`, lz4-on-remote, 20/36 triggers — gated by G1/G2/G5/G6.
4. **P4 — M3 per-level-pair concurrency** (the only structurally risky stage; `FRS_COMPACT_CONCURRENT=1`
   default-OFF until T8 ×5 + sorted-run G3/G4). Composes with the policy (concurrency is execution-side).

The lifecycle write-amp path (wa-v0, 0.98× measured floor) stays a documented, ready-to-activate
capability — INERT under the "keep runtime untouched" decision (wa-v0 §DECISION) — and is NOT on this
fundamental fix's critical path. The fundamental fix relies only on the engine-transparent mechanisms
that need no operator feed: the shared sorted-run policy (M1/M4 default-ON), trivial-move, and
KV-separation.
