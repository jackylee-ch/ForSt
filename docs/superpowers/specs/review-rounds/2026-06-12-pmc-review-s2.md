# PMC Review Cycle 5 — S2 unit (pinned rows + tournament tree, flag `FRS_RS_S2_PINNED` default-OFF)

**Date:** 2026-06-12
**Reviewer:** PMC architect (standing), cycle 5
**Tree reviewed:** `forst-rs` tip `46fc70f3c` (S2 merged)
**Commits reviewed:**
- `83ec3052a` feat(storage): S2-1 range-exposing row visitors (W1a/W1a')
- `f22fbbf21` feat(engine,ffi): S2-2 pinned block replenish + push-style sink emit
- `214180314` feat(engine,storage): S2-3 tournament-tree merge + seek-aware pinned replenish

**Verdict: APPROVE — all three commits.** Zero blockers at flag-OFF (the
shipped default). Two flag-ON error-path findings (F-1, F-2) and three
advisories (A1–A3) recorded below; **F-1 and F-2 must be resolved (or
explicitly accepted-documented) before the §6.5 default-ON decision commit**
— they are unreachable while the flag is OFF and sit on error paths only.

**Suites re-run for this review (medium of the gate matrix, this box):**
- storage: **409/0** (+ aux targets 23/0, 3 ignored micro)
- engine: **298/0 flag-OFF** and **298/0 flag-ON** (`FRS_RS_S2_PINNED=1`)
- ffi: **108/0 flag-OFF** and **108/0 flag-ON**

---

## 1. Borrow/lifetime soundness (hunt item 1)

### 1.1 The deferred Put-winner refight — semantically identical to legacy? YES

Verified phase-by-phase against `next_with_value` (db.rs:12127) — the legacy
reference — for both the linear pinned merge (S2-2) and the tree (S2-3):

| Decision component | Legacy `next_with_value` | Pinned linear | Pinned tree | Verdict |
|---|---|---|---|---|
| Per-source dedup | `head <= last_emitted` → advance (Phase A inner loop) | identical (`ensure_head_pinned`, strict `<=`) | identical (dup-drain on surfaced winner + `ensure_head_pinned`) | ✓ |
| Min selection | lex-smallest head | identical (immutable `head_key` pass) | comparator key component | ✓ |
| Memtable presence | ANY memtable at min ⇒ Fallback | identical (`head_info() == None`) | rank 0 < rank 1 at equal key ⇒ memtable surfaces FIRST ⇒ Fallback — encodes `mem_present` exactly | ✓ |
| Max-seq SST tiebreak | strict `seq > bseq` ⇒ FIRST (lowest-index) source kept on equal seq | identical strict `>` | comparator `seq DESC` then `idx ASC` ⇒ lowest index on tie | ✓ |
| Tombstone winner | skip key, advance all at min; older versions shadowed via dedup | identical (Phase C `continue`) | key copied to `last_emitted_buf` BEFORE the skip ⇒ losers at K drain as dups — same shadow | ✓ |
| Merge / corrupt-Put | Fallback → `get_internal` | identical | identical | ✓ |
| Conservative `None` arm | Fallback | identical | `head_info` None ⇒ Fallback branch (consistent with `head_key` within one iteration — both read `pbuf.rows.get(pos)`) | ✓ |

The DEFERRAL itself changes only *when* the winning source's next `peek`
(replenish) happens — after the emit instead of inside the same step. Since
`advance()` on a pinned source is a pos-bump ONLY (db.rs `TierKeySource::advance`,
verified: no replenish) and replenish is reachable solely through `peek()` /
`ensure_head_pinned`, which the merge calls only at the TOP of the NEXT step
(after `sink.push` returned), the winning row's `(src, row)` indices resolve
against the still-pinned block at emit time. Tree path: the only mutations on
the winning source between row-index capture and emit are `advance` (pos
bump); memtable/tombstone/Fallback winners refight immediately, which is safe
because their emits need no pinned bytes (key lives in the owned
`last_emitted_buf` scratch; Fallback value comes from `get_internal`).
**Sound, and order-identical under every tier-rank/seq tiebreak I could
construct** (mem-over-SST, max-seq SST, equal-seq cross-SST, tombstone
shadowing newer/older Put, merge stacking → Fallback resolves the operand
order via `get_internal`, i.e., the operand-stacking question is delegated to
the unchanged legacy resolution path in BOTH modes).

### 1.2 Within-block dedup scope — pinned == legacy

Legacy replenish does `buffered.clear()` per decoded block (db.rs:11132), so
its `buffered.last()` dedup is **per-block**, exactly the scope of the pinned
`last_accepted` local. Same-user-key rows split across a block boundary are
deduped at the MERGE level (`head <= last_emitted`) in both modes — verified
the newest version is always consumed first (SST order key ASC/seq DESC +
`seek_restart`'s strict-`<` restart landing), so the cross-block residue can
only be an older version, which the merge dedup skips. ✓

### 1.3 D2 key arena — any dead-pointer escape? NO

Every v2 key produced by the ranges visitors is `SliceRef::Arena` (the G1
tests carry a trap arm asserting a v2 key can never escape as a Block ref);
arena is append-only during a walk; `SstBlockBuf` clears `key_arena` only
together with `rows`/`pos` (replenish or exhaustion), so no live
`PinnedRowMeta` can outlive its arena bytes. The S2-3 seek visitor
reconstructs SKIPPED prefix-chain keys into a LOCAL scratch and copies
scratch→arena at the first yielded row — the yielded refs obey D2. The G1
property tests resolve POST-walk, which is precisely the construction that
would catch a reused-scratch escape. ✓

### 1.4 Partial-block error state — identical to legacy

On a post-checksum parse error mid-walk, `pbuf.rows` retains the rows accepted
before the error and the source presents them as heads after the error is
recorded — byte-identical to legacy (`buffered` partially filled, then `?`).
Checksum failures (the realistic case) surface at `next_decoded()` with an
empty buffer ⇒ drained, both modes. Covered by the S2-3 mid-stream test in
legacy/pinned-linear/pinned-tree. ✓

## 2. MVCC / snapshot (hunt item 2)

- **Pinned `SstBlockBuf` vs compaction file deletion:** the pin is a DECODED
  in-memory block (`Arc<KvBlock>` refcount or the `RecordBatch`'s Arc'd
  buffers) — file deletion cannot invalidate it. File-read lifetime during
  iteration is governed by the same `BlockPrefetcher` (holding
  `Arc<SstReaderImpl>`) as legacy — UNCHANGED by S2; S2 only adds memory
  pinning on top. No new hazard vs version retirement: the iterator's sources
  hold their readers for the iterator's lifetime exactly as before.
- **Seq filtering:** both paths are latest-reads (`u64::MAX` at the
  `get_internal` fallback; per-block newest-version-first selection identical
  filter-for-filter — see §1.2 table). No snapshot-seq parameter exists on
  either path, so "identical to legacy" holds vacuously and exactly.
- **Pin bound:** ≤ 1 decoded block per LIVE source, eagerly released at
  source exhaustion AND on stream drop (the leak test asserts the exact
  refcount drop). The empirical 100M-scale check for pin accumulation is gate
  G4 below — keep it.

## 3. Tournament tree (hunt item 3)

- **Comparator vs engine tier contract:** `(key, tier_rank, seq DESC, idx)`
  with rank 0 = memtable (seq treated as `u64::MAX`), rank 1 = SST. Verified
  this encodes the linear Phase-B table exactly (see §1.1). Multi-memtable
  ties break on index → Fallback → `get_internal` resolves true
  newest-across-memtables — same as legacy `mem_present`.
- **Winner-only advance vs legacy advance-all-at-min:** equal-key losers
  surface as subsequent root winners, are detected as dups
  (`head <= last_emitted`) and drained via `ensure` + `log₂ n` replay; net
  source positions converge to the linear outcome. Drained/errored heads
  always lose fights ⇒ root `TREE_NONE`/drained ⇒ correct termination.
- **Adaptive abandonment (tree→linear mid-stream):** one-way, decided at a
  STEP BOUNDARY only (top of `next_step_pinned_tree`). It drops the tree and
  the pending `deferred_refight` — safe because linear Phase A.1 re-ensures
  EVERY source's head (with dedup-past-`last_emitted`) each step, so the
  un-refought winner source is replenished/deduped on the first linear step.
  No row drop (linear scans all sources for min) and no duplicate (dedup
  boundary `last_emitted_buf` is shared state across both procedures).
  Verified no path mixes the keys-only/`next_with_value` dedup state
  (`last_emitted: Arc`) with the pinned dedup state (`last_emitted_buf`) on
  one iterator: `fill_into` branches once on the build-time `pinned` flag.
- **n≤4 linear branch:** byte-equality is gated by the randomized fixture
  rounds {1,3,4} (below threshold) and {6,16,48} (above), prefix + range,
  vs legacy AND `prefix_scan`. Tree-engagement assert ties the branch to
  `S2_TREE_MIN_SOURCES` exactly.

## 4. FFI Pinned backend (hunt item 4)

- **All three production opens** construct Pinned when ON: `frs_vec_iter_prefix_open`,
  `frs_vec_iter_range_open`, `frs_vec_iter_prefix_open_batch_parallel` (raw
  `ChunkSink`, no adapter). The SERIAL `frs_vec_iter_prefix_open_batch`
  (lib.rs:5981) stays on `prefix_scan_iter_owned_arc_with_error_slot`, which
  flag-ON returns the `ArcPairAdapter` — correct (byte-equal), carries the
  documented adapter materialisation; acceptable since the parallel batch is
  the production path. Mixed Boxed/Pinned registry coexistence is safe: every
  handle self-describes and `fill_chunk_from_iter` dispatches per-handle.
- **Chunk-boundary rows:** never split — whole-row framing both modes. The
  overflow row is COPIED into reused pending buffers (push contract: returning
  `false` means the sink kept the row) and is delivered FIRST on the next
  fill, before the `exhausted` check — so a stash + Exhausted-next-fill loses
  nothing. Row-larger-than-chunk: `(0,0,false)` non-progress — byte-identical
  contract to legacy `put_back` (verified against lib.rs:5237-5243).
- **EOF (P0):** `finish_probe_fill` is now SHARED between backends —
  `eof = exhausted && !error_pending`, terminal/deferred-error transitions
  identical. Pass-3 `None` fill (pool panic/timeout) → `EngineIo`, unchanged.
- **Error mid-fill:** sticky-FIRST into the same shared slot the open/next
  callers already drain; rows written before the error are returned with the
  chunk and the error surfaces after — matches legacy ordering at the FFI
  boundary. (The in-process adapter does NOT match — see F-2.)
- **Abort/watchdog:** `is_aborted` checked BEFORE the Pinned dispatch (same
  early-return); `drop_inner`/handle drop releases the stream ⇒ all pins
  (leak test asserts the exact refcount). `next_row`/`put_back` unreachable
  for Pinned — verified the only callers live inside `fill_chunk_from_iter`
  AFTER the dispatch; the debug_assert documents the invariant.

## 5. Seek-aware `for_each_row_ranges_from` (hunt item 5)

- `seek_restart` (kv_block.rs:610) returns the LAST restart with
  key **strictly <** target (`lo.saturating_sub(1)`) — the strict-`<` is what
  preserves the newest version when the target user key spans multiple
  restarts; restart entries are corruption-checked to `shared == 0`
  (`full_key_at_restart`), so the walk always starts on a self-contained key.
  Edges: target ≤ all keys → restart 0 (full walk); target > all keys → walk
  from the last restart, zero yields after skips; `num_restarts == 0` → 0.
- Mid-block reconstruction: skipped rows chain through the LOCAL scratch with
  the same `shared > prev_len` corruption guard as the full visitor;
  `scratch.truncate(e.shared) + extend` is the standard prefix-chain step and
  the first entry after a restart has `shared == 0` so the chain roots
  correctly. First yielded row copies scratch→arena (D2), subsequent rows
  chain through `prev_arena` — identical arithmetic to the full visitor.
- Early stop (`Ok(false)`) returns without touching fetcher state; the engine
  caller sets `hit_upper` and calls `fetcher.terminate()` — same contract as
  legacy upper-bound termination (rows accepted before the bound still drain).
- v1 twin: lower-bound bsearch over sorted keys + absolute (un-rebased)
  `value_offsets` pairing with `value_data()` — offset math centralized in
  `batch_key_value_data` per the work order. Property tests cover both
  formats incl. stop-truncation and existing/between/before/after targets.

## 6. Flag-OFF byte-identity (no shared-state mutation when OFF)

Verified by code audit + suite runs:
- Engine: flag-OFF takes the original iterator construction branch verbatim;
  the legacy replenish loop's only delta is the `mat_allocs` Cell increment
  (diag counter, no output effect) and a per-source `Box<SstBlockBuf>`
  default alloc at build. No global state is mutated (the OnceLock flag cache
  and the test-only override atomic are read-only on this path).
- FFI: flag-OFF constructs `IterBackend::Boxed` and the legacy fill loop runs
  unchanged behind one enum-dispatch check.
- Empirical: full engine+ffi suites green in BOTH states; the committed
  `iter_drain` OFF gate (14.7/15.4 ms vs 151.8 ns/row clean baseline) bounds
  the OFF-path perf delta at noise.

---

## 7. Findings

### F-1 (MEDIUM — flag-ON only, fallback-error path only): pinned scan terminates at the first fallback-resolution error; legacy continues
Legacy `prefix_scan_iter_owned_arc_with_error_slot` is a `from_fn` loop that
yields `Some(Err)` and CONTINUES the merge on the next pull (db.rs:6713-6725);
the FFI filter_map records sticky-first and keeps scanning — rows after the
errored key are still delivered. Pinned `fill_chunk_from_pinned` parks
`exhausted = true` on the first `fill_into` `Err` — the scan terminates at the
errored key. Both record sticky-FIRST into the same slot and the Java caller
observes the error through the identical R17-M3/R18-M4 machine, so the
operation fails either way; but the delivered-row prefix differs. The S2-3
mid-stream-error gate covers TIER-PEEK errors only (error-as-drained —
identical both modes), NOT `get_internal` fallback errors. **Required before
default-ON:** either align (continue-after-fallback-error in `fill_into`) or
record the pinned (more conservative) behavior as the intended contract with
a test.

### F-2 (LOW-MEDIUM — flag-ON, in-process `ArcPairAdapter` only, error path only): buffered rows orphaned behind an error
S2-3's adaptive adapter returns `Some(Err)` immediately while rows pushed in
the SAME fill sit in `buf`; later `next()` calls pop those rows AFTER the
error. A consumer that stops at the first `Err` drops rows legacy would have
delivered BEFORE the error; one that continues sees rows after it. Trivial
fix: drain `buf` before yielding the stashed error. FFI never traverses the
adapter; affects engine-internal consumers only.

### A1 (advisory): `SliceRef::resolve` computes `offset + len` in u32
Wraps (release) / panics (debug) past 4 GiB. Both fields are individually
u32-guarded at construction and the backing stores are per-block/per-arena
(≪ 4 GiB), and a wrap produces a slice panic, not UB — but do the add in
usize for free hygiene.

### A2 (advisory): per-source `Box<SstBlockBuf>` is allocated flag-OFF too
One heap alloc per SST source per iterator build that the OFF path never
uses. Bounded by the no-regress gate; fold into `Option`/lazy-init if the
build path ever shows up in a profile.

### A3 (advisory): serial `frs_vec_iter_prefix_open_batch` rides the adapter when ON
Correct but pays the 2-Arc-per-row materialisation. If any production caller
still uses the serial batch symbol, port it to
`batch_open_prefix_streams_parallel_map`'s stream shape (or the single-open
pinned construction) before default-ON; otherwise document it as
compat-only.

## 8. Per-commit verdicts

| Commit | Verdict | Notes |
|---|---|---|
| `83ec3052a` S2-1 ranges visitors | **APPROVE** | Additive, no call sites, property gates are the strong post-walk construction; A1 only. |
| `f22fbbf21` S2-2 pinned replenish + RowSink + FFI Pinned | **APPROVE** | W1c sound; filter pipeline verbatim vs legacy; F-1/F-2 are flag-ON error-path divergences to resolve pre-default-ON; A2/A3. |
| `214180314` S2-3 tournament tree + seek replenish | **APPROVE** | Comparator encodes Phase-B exactly; abandonment safe at step boundary; seek arithmetic verified incl. restart edges. |

---

## 9. §6.5 default-ON gate program — REMOTE round-3 matrix (actionable)

NexMark is remote-only now: the spec's "G2 @5M byte-equiv sweep local" is
**replaced** by a remote screen. Shape = `2026-06-12-remote-gate-matrix.md`
(preconditions table → ordered abort-early cells → evidence-traced
expectations; same-session law; ratios only, never cross-population seconds).
This is **round-3** on the same box/topology (yq01 x86 NVMe, 2×TM 4c/16g +
JM 2c/4g, io_uring on).

### Preconditions (all must hold before the first timed run)
| # | Check | How |
|---|---|---|
| P1 | Round-2 matrix complete, SUMMARY archived (round-3 deltas defined against it) | `/ssd2/jackylee/frs-bench/logs/SUMMARY.md` |
| P2 | `.so` from ForSt tip ≥ `214180314` **plus the F-1/F-2 resolution commit**; jar unchanged from round-2; record SHAs + sha256 | run log |
| P3 | Flag wiring VERIFIED from the engine log in both states (`FRS_RS_S2_PINNED` unset = OFF; `=1` = ON) — grep the flag echo per cell, never assume | startup log |
| P4 | All other flags default (`FRS_RS_MIXED_BATCH`, parallel-executor, drain env UNSET — drain rides its committed default) | env dump |
| P5 | io_uring active, same images/dirs as round-2 | startup log |
| P6 | Box idle before each cell | `free`, ps |
| P7 | Every A/B pair (OFF vs ON) same session/box/day, interleaved | run ordering |

### Cells (in order; later cells abort on earlier failures)
| # | Cell | n | Gate (pass) | On fail |
|---|---|---|---|---|
| S0 | q1@1M ON smoke | 1 | finishes, out 1,000,000 | abort: build/flag wiring broken |
| S1 | **G2-remote: q0–q22 @5M, OFF then ON, same session** | 1+1 per q | out_rows EXACT-equal per query OFF vs ON (the byte-equiv screen at the smallest full-coverage scale the remote harness runs) | any mismatch → CORRECTNESS abort, file against the S2 merge, default-ON is dead until root-caused |
| S2 | **G3: light no-regress** q3@100M OFF+ON, q4@100M OFF+ON | 1+1 each | q3 out 2,201,068 EXACT both; ON/OFF wall ratio ∈ 0.95–1.05 (linear-branch guard at production shape) | >1.05 → `S2_TREE_MIN_SOURCES`/linear-branch suspect; do NOT flip default |
| S3 | **G4: RSS steady** q9@100M ON | 1 | TM RSS sampled (≥1/min): no monotonic growth attributable to pins; peak ≤ 1.05× same-session OFF run | growth → pin-release leak at scale; abort + heap-profile |
| S4 | **G5: heavy A/B** q9@100M OFF×2/ON×2 interleaved; q20@100M OFF×2/ON×2 | 4+4 | out_rows EXACT every run (q9 91,813,372; q20 93,201,404); ON ≥ OFF outside noise (HARD no-regress); report vs §4 model (q20 −9..13 %, q9 −4..8 %) | regress → A/B attribution; **falsifier-2 binding:** q20 ON worse than −4 % with scan share ≥15 % → STOP S2 follow-ups (no SoA, no arena-on-pool), run L4/L0 first |
| S5 | q7@100M ON | 1 | finishes, out_rows == round-2 q7 pin; wall ≤ 1.05× same-session OFF | iterator-heavy regression screen (q7 is the open-rate-dominated shape) |

### Decision rule
Default-ON flip = a SEPARATE decision commit (per the L1-drain-gate
precedent), gated on: S1–S5 all-pass **and** F-1/F-2 resolved **and** the
flip commit itself changes only the default + records the S-cell evidence.
Mac-local pre-flight (cheap, before burning remote budget): rerun the engine
A/B suites + `join_probe_open` ssts_{1,8,64,128} and `iter_drain`/`hot_prefix_churn`
n≥3 on the candidate SHA — any local regression aborts the remote round.
