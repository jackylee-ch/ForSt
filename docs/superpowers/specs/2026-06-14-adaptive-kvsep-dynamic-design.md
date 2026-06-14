# Adaptive / dynamic KV-separation — ONE uniform config for all 8 priority queries

**Date:** 2026-06-14
**Author:** PMC-1 architecture arm (DESIGN ONLY — no builds/benches/NEXMark run; a perf pass is live on this Mac)
**Base:** `origin/forst-rs` @ `dbd220afc` (worktree hard-reset to it)
**Engine:** `/Users/lijunqing/Code/stczwd/ForSt` · **Flink backend:** `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs`

## HARD CONSTRAINT (user directive)
ALL 8 priority queries run under the **SAME uniform config**. Per-query config is FORBIDDEN. The only
permitted lever is **dynamic / runtime-adaptive engine behavior under one config** — the engine senses
value size / access pattern / memory pressure at runtime and decides per-value/per-CF whether to
separate, bounded so it can never OOM or regress another query. Every repair: **one uniform config +
runtime-adaptive + flag-gated default-OFF + byte-identical when OFF**, with an FFI/value-size/churn
**micro-bench gate BEFORE any NexMark** (not NexMark itself).

---

## 0. The problem, restated from the clean V3 verdict passes

From `2026-06-08-8c32g-3backend-sweep-results.md`, the §"V3 FULL 8-QUERY" (flag-ON, tip 1cda0e724) and
§"V3 FLAG-OFF" (tip 15436bfde) passes are an apples-to-apples A/B of the KV-sep lever stack
(`FRS_SST_COMPRESSION=lz4 FRS_KV_SEPARATION=true FRS_TRIVIAL_MOVE=true FRS_RS_S2_PINNED=1`) vs the
ForSt-matching default (all forst-rs write/read levers OFF, lz4 only):

| query | flag-ON frs/rdb | flag-OFF frs/rdb | ON→OFF delta | verdict |
|---|---|---|---|---|
| **q4** | **1.09× PASS** | 1.33× FAIL | OFF +23% slower | **KV-sep NEEDED** (write/rewrite-bound retract+agg) |
| **q7** | **1.16× PASS** | 1.62× FAIL | OFF +35% slower | **KV-sep NEEDED** (heaviest windowed-join; clearest beneficiary) |
| **q20** | **1.26× NEAR** | 1.44× FAIL | OFF +16% slower | **KV-sep NEEDED** (heavy interval-join, value-carrying read path) |
| **q9** | **DNF OOM ×3** | **1.70× FINISHES** (1828.7s, beats ForSt 2002.5) | ON OOM-kills @~75M | **KV-sep HARMFUL — OOM** |
| **q11** | 2.08× FAIL | **1.12× PASS** | ON +82% slower | **KV-sep HARMFUL** (windowed MapState-agg drain) |
| **q17** | 2.23× FAIL | 1.55× (closer, beats ForSt 2.22×) | ON +36% slower | **KV-sep HARMFUL** (windowed-agg accumulator RMW drain) |

(q12/q19 are not KV-sep-sensitive in the sense that matters here: q12 is source-bound parity; q19 is a
flag-ON win — value-carrying interval-join + Top-N — already PASSes both bars and belongs with the
"NEEDED" family.)

**No uniform on/off flag works:** turning the master flag ON wins q4/q7/q20 but OOMs q9 and badly
regresses q11/q17; turning it OFF fixes q9/q11/q17 but loses q4/q7/q20. **Adaptive separation —
the engine deciding per-value/per-CF under one config — is mandatory.**

The existing knob `FRS_KV_MIN_BLOB_SIZE` (default 128 B, floor 22 = `VALUE_POINTER_LEN+1`;
`db.rs:14897-14913`) is a value-SIZE threshold — the natural "engine decides per value under one
config" lever. **Mission step 1: does a single tuned uniform threshold cleanly separate the two
families? Investigated below — the answer is "size sorts the families correctly, but the threshold
alone is NOT sufficient" — q9 is the counterexample that forces a richer mechanism.**

---

## 1. Value-size distribution per query (the central question)

### 1.1 What each query's state stores (from the SQL + operator shapes)

| query | operator / state shape | value stored | size class | source |
|---|---|---|---|---|
| **q4** | auction⋈bid join buffer + running agg (retract) | full **Auction** rows (itemName + LONG description + 6 longs + 2 ts + extra) | **LARGE ~400-600 B** | `q4.sql`; bench join payload 256 B `nexmark_disagg_s3.rs:306` |
| **q7** | TUMBLE(10s) MAX(price) ⋈ bid band-join | buffered **Bid** rows (channel + url + extra + 3 longs + ts) | **LARGE ~200-280 B** | `q7.sql`; bench `[0u8;256] // join payload — past kv_min_blob_size (128)` `nexmark_disagg_s3.rs:306` |
| **q9** | interval-join + ROW_NUMBER (Rank/dedup) | full **Auction+Bid** joined rows buffered per key for rank | **LARGE ~512 B** | `q9.sql` (`SELECT A.*, B.…`); bench `[0u8;512]` |
| **q20** | bid⋈auction (category filter, expand) | full **Auction** rows (q4 schema) + Bid side | **LARGE ~600-800 B** | `q20.sql` |
| **q11** | SESSION(10s) GROUP BY bidder, COUNT | per-(bidder,session) **count** accumulator (single BIGINT + window bookkeeping) | **SMALL ~16-32 B** | `q11.sql` |
| **q17** | GROUP BY auction, day — count/min/max/avg/sum + filtered counts | one numeric **accumulator** row per (auction, day): ~7-8 × int64 | **SMALL ~40-80 B** | `q17.sql` |

(CF-lifecycle check, `column_family.rs:183-200`: KV-sep only fires on `Unbounded` CFs. The Flink
backend's `frs_cf_set_lifecycle` wiring exists but is **gated OFF by default** (`forst.rs.lifecycle.
segments` unset) AND window state is created with no TTL, so it would map to `Unbounded` even if the
gate were on — `ForStRsAsyncKeyedStateBackend.java:948-976`, `ForStRsLifecycleManager.java:92-109`,
`WindowOperator.java:246-250`. **Net: EVERY query's state CF is `Unbounded` → KV-sep is *eligible* for
all of them; the only per-value gate today is the 128 B size threshold.** So size really is the lever
the engine currently uses to discriminate — confirming step-1's framing.)

### 1.2 FINDING — size sorts the families CORRECTLY, but the threshold alone is INSUFFICIENT

**The distributions are well-separated and on the "right" side for the write/rewrite-bound family:**
- KV-sep-NEEDED family (q4/q7/q9/q20): **LARGE, ~256-800 B**, comfortably above any sane threshold.
- KV-sep-HURTS-on-merge-drain family (q11/q17): **SMALL, ~16-80 B**. q11 (~16-32 B) is *below* the
  current 128 B default — its tiny accumulators **already stay inline today**, so size is doing its
  job for q11. q17 (~40-80 B) is also below 128 → also already inline.

So **for q11 and q17 the size threshold IS the mechanism that should keep them out of the vlog** — and
indeed their accumulators are below 128 B, so the 128 B default already excludes them. Why then did
flag-ON regress q11/q17 by +82%/+36%? **Because the flag-ON regression is NOT primarily KV-sep writing
their tiny accumulators to the vlog** (the threshold blocks that). It is the **read/drain-path cost of
the rest of the lever stack** that ships with `FRS_KV_SEPARATION=true` (the §3-q11/q17 root cause in
`2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md`: the windowed-agg accumulator RMW *drain* phase
is slower under the flag-ON read path / the legacy merge the S2 lever fixes; q11 also pays the
depth-1 serial drain tail). The "KV-sep HURTS q11/q17" verdict in the V3 doc is really "**the lever
stack that travels with the KV-sep flag hurts the windowed-agg drain**," and a value-SIZE gate already
keeps the actual separation off their small values.

⇒ **For q11/q17, a uniform size threshold of ~128-256 B is already correct** (their values are smaller,
so they are never separated). The residual q11/q17 regression is a READ-PATH problem, addressed by the
complementary R1/R2a/R3 levers (§4), not by KV-sep tuning.

**The counterexample that breaks "size-threshold alone": q9.** q9 stores **LARGE ~512 B** values — by
size it is squarely in the "separate me" family, exactly like q7/q20. A size threshold says **YES,
separate q9** — and that is precisely what OOM-kills it. So **size-threshold alone gives the WRONG
answer for q9**: it would (and did) separate q9, and separating q9 busts the cgroup. The failure is not
that q9's values are small; it is that **q9's access pattern + the unbounded resident vlog state +
the Flink-side join/Rank heap make separation unaffordable in memory even though it is desirable by
size.** No single tuned `FRS_KV_MIN_BLOB_SIZE` can express "separate q7/q20 (large) but NOT q9 (also
large)" — they overlap in size. **Therefore size-threshold alone is insufficient; a richer
memory-bounded adaptive signal is required.** (Mission step 2 fails; proceed to step 3.)

### 1.3 Why q9 OOMs even though its values are large (the discriminator)

From `2026-06-14-q9-kvsep-oom-rootcause.md` (code-grounded, file:line):
- KV-sep diverts each `Put ≥128 B` to an append-only `.vlog` segment (one segment per flush,
  `flush.rs:384-431`), leaving a 21 B `BlobRef` inline. Reads deref through `vlog_readers`
  (`db.rs:978`).
- **Reclaim fires only when a segment's `live_bytes==0`** (`db.rs:12686-12692`), and the default
  `FRS_VLOG_GC_AGE_CUTOFF=0` disables relocation (`db.rs:14806-14835`) — segments can only die WHOLE.
  That default was tuned on **q7-shaped FIFO-death churn** (deaths follow arrival order → segments die
  whole naturally). **q9 is a multi-way interval JOIN + Rank: death order ≠ arrival order** → every
  flush segment ends up a mix of long- and short-lived pointers → mostly-dead segments never reach 0 →
  the resident reader set grows monotonically with the run.
- The bounded LRU **already shipped** (`FRS_VLOG_READER_CACHE_CAP`, default 2048, `db.rs:123-142`,
  `vlog.rs:329 DEFAULT_VLOG_READER_CACHE_CAP`) and DID work at the engine layer (block-cache hit ~80%,
  resident oscillated 9-14 GiB), **but q9 STILL OOM'd @~75M** (V3 confirm run, tip 9e0438eb7): the
  dominant final-phase pressure is the **Flink-side join+Rank state heap**, not the vlog reader cache.
  2 TM×16g + 1 JM×4g = 36g > 35g Mac RAM, and KV-sep ADDS the `vlog_readers` map + per-segment
  bookkeeping ON TOP of the flag-OFF path that fit at ~15.8 GiB peak. KV-sep ON rode ~2 GiB higher than
  flag-OFF on the SAME join-state pressure and that ~2 GiB is what crossed the cgroup.

**So q9's separation cost is memory, and it is additive on top of an already-near-the-limit footprint.
The adaptive mechanism for q9 must be able to say "this CF wants separation by size, BUT separating it
is pushing resident state toward the cap — so back off / spill / don't separate further" — i.e. a
MEMORY-PRESSURE signal, not just a size signal.**

---

## 2. Conclusion on size-threshold sufficiency (mission steps 1-2)

- **Size sorts the two families correctly** (large = q4/q7/q9/q20; small = q11/q17), and the small
  family already stays inline under the existing 128 B default — so a uniform threshold near 128-256 B
  is necessary and is the right *first* gate.
- **But size-threshold ALONE is NOT sufficient**: q9 (large values, wants separation by size) OOMs
  when separated, and is indistinguishable BY SIZE from q7/q20 (which benefit from separation). The
  signals q9 needs (resident-vlog pressure, scattered-death reclaim failure) are orthogonal to value
  size. **⇒ proceed to a richer adaptive heuristic (mission step 3): size as the eligibility gate +
  a never-OOM memory bound that makes separating q9 safe (so q9 can KEEP the write-amp benefit AND
  fit), composed with the read-side levers that fix the residual q11/q17 drain.**

---

## 3. Adaptive KV-separation design (mission step 3) — one uniform config

The mechanism has **three layers**, all evaluated by the engine at runtime under one config:

### Layer A (eligibility, exists) — value-SIZE gate `FRS_KV_MIN_BLOB_SIZE`
Keep the per-value size threshold as the FIRST decision: a `Put` is a separation *candidate* only if
`value.len() >= min_blob_size`. **Recommended uniform value: 256 B** (raise from 128).
- Rationale: the NEEDED family is 256-800 B → 256 B still catches ALL of them; the HURTS family is
  16-80 B → 256 B keeps every q11/q17 accumulator inline with margin (so even if a future window CF
  grows its accumulator past 128 it stays inline up to 256). 256 also matches the bench join-payload
  fixture (`nexmark_disagg_s3.rs:306`). This widens the inline band for small-accumulator queries at
  zero cost to the large-join family.
- This layer is **necessary but not sufficient** (q9, §1.2) — it cannot tell q9 from q7/q20.

### Layer B (the new adaptive signal) — per-CF MEMORY-PRESSURE-gated separation with a never-OOM bound
This is the layer that lets q9 be separated SAFELY (or backed off) without per-query config. The engine
already bounds the reader *handles* (`FRS_VLOG_READER_CACHE_CAP`); what it lacks is a bound on the
**resident vlog WORKING-SET bytes** and a runtime back-off when that bound is approached. Two
sub-mechanisms:

**B1 — charge vlog resident state to a shared byte budget + back off separation under pressure
(primary, the never-OOM bound).**
The flag-ON regression for q9 is "KV-sep moved the dominant byte source OUT of the charged block cache
into an UNCHARGED set" (rootcause §3). Fix: give the vlog reader/chunk-buffer resident state a
**charged byte budget** that shares a pool with (or sits alongside) the block cache, and gate
*continued separation* on headroom:
- Track `vlog_resident_bytes` = Σ over live readers of (open-handle state + ≤64 KiB chunk) + the
  `Version::vlog_segments` Vec cost. (The LRU cache at `db.rs:123` already owns the reader set; add a
  charged byte counter to it.)
- New uniform knob `FRS_VLOG_RESIDENT_BUDGET_MB` (default = a fraction of the global WBM budget, e.g.
  ~512-1024 MB so it is small vs the 6 GB WBM and the 16 g cgroup). When `vlog_resident_bytes` exceeds
  the budget, **the LRU evicts to byte-budget** (not just count) — drop LRU readers' handles + 64 KiB
  chunks. This makes resident vlog cost `O(budget)` independent of segment count, which is the q9
  monotonic-growth driver. A miss re-opens the immutable segment (correctness-trivial, already proven
  for the count cap).
- **Back-off under sustained pressure (the adaptive decision):** at flush time, `kv_sep_spec_for`
  (`db.rs:12624`) consults a runtime `should_separate_now(cf)` that returns `false` when
  `vlog_resident_bytes` has been at/over budget AND the CF's reclaim rate is ~0 (segments not reaching
  `live_bytes==0` — the q9 scattered-death signature, measurable from the GC accounting deltas
  `version/mod.rs:565-568`). When it returns false, that flush writes values **inline** (byte-identical
  to flag-OFF for that flush). Net effect: a FIFO-death CF (q7) never trips the back-off (it reclaims,
  resident stays low) → keeps full separation; a scattered-death CF under memory pressure (q9) **stops
  adding new resident vlog state once near the budget** → resident vlog cost is capped and q9 fits,
  while the already-separated portion keeps its write-amp benefit. **This is the "engine decides per-CF
  at runtime under one config" signal the mission asks for: value size (Layer A) AND resident-pressure
  + reclaim-rate (Layer B1).**

**B2 — adaptive vlog-GC relocation for scattered-death CFs (complementary, bounds disk/space-amp).**
Keep `FRS_VLOG_GC_AGE_CUTOFF=0` (the q7-tuned default — relocation OFF, no write-amp) for FIFO-death
CFs, but **auto-enable a modest relocation cutoff when the engine observes a CF is NOT reclaiming**
(reclaim-rate ~0 while segment count climbs — the same scattered-death signal as B1). For such a CF,
relocate the OLDEST cutoff-fraction of segments (`kv_gc_spec_for_compaction` already has the
machinery, `db.rs:12651-12680`) so mostly-dead segments drain WHOLE and reclaim fires. This bounds the
on-disk segment count + the `vlog_segments` Vec + page-cache (rootcause #2/#3), trading some
write-amp ONLY on the CFs that actually need it (q9), leaving q7 untouched. Lower priority than B1
(B1 alone bounds the RAM that OOMs q9; B2 bounds disk/space-amp and the secondary Vec-clone cost).

**Why this satisfies "one uniform config":** all four knobs (`FRS_KV_MIN_BLOB_SIZE`,
`FRS_VLOG_RESIDENT_BUDGET_MB`, `FRS_VLOG_READER_CACHE_CAP`, the auto-relocation trigger) have a SINGLE
value for every query. The *behavior* differs per query because the engine measures each CF's value
size, resident pressure, and reclaim rate AT RUNTIME and decides. q7/q20 separate fully (large values,
FIFO death, low resident); q9 separates until pressure, then backs off + relocates (large values,
scattered death, high resident); q11/q17 never separate (small values, Layer A blocks them). No
per-query flag.

### Layer C (defensive) — the q17/q11 read-path is NOT a KV-sep problem; compose the read-side levers
Per §1.2, q11/q17's flag-ON regression is the windowed-agg DRAIN read path, not their (inline) values.
Those are closed by `2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md`'s R1/R2a/R3 — see §4. The
KV-sep design's only obligation to q11/q17 is **Layer A keeping their values inline** (done) and **not
shipping a read-path regression with the flag** (the lever stack must be decomposed so KV-sep ≠ "also
turn on the slower legacy merge"; the adaptive S2 lever R1 is what makes the deep-probe merge fast
WITHOUT taxing shallow scans).

---

## 4. Composition with the read-side levers (mission step 4)

KV-sep (write-amp) closes q4/q7/q20; it does NOT close the residual read-path gaps. From
`2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md`:

| lever | what it does | which queries | composes with adaptive KV-sep how |
|---|---|---|---|
| **R1 adaptive S2** (per-scan, fan-out-gated loser-tree, `FRS_S2_FANOUT_MIN`) | deep-probe merge 8-18× faster; **and with KV-sep ACTIVE the legacy merge EXPLODES 18.09µs→S2 1.00µs** (cycle-3) | q7 (+q9/q20) | **S2 is what PREVENTS KV-sep from regressing the deep-fan-out read path** — they are co-dependent: separate large values (write win) + loser-tree merge the BlobRef rows (read win). Adaptive (by `n_overlap`) so shallow q3/q4/q17 scans keep the legacy byte-identical path. |
| **R2a adaptive executor depth** (iter batches → kg-affine workers; iter-free stay inline) | cross-probe overlap; q11 318.9→135.7s (2.35×), q7 1441→1052s | q11, q7 | orthogonal to KV-sep (executor vs storage). Closes the q11 depth-1 drain tail that KV-sep can't touch. |
| **q17 zero-handoff carve-out** | keep iter-free point-RMW batches on depth-1 no-split inline path | q17 (defensive) | ensures the executor lever (R2a) does not regress q17; KV-sep already leaves q17 inline (Layer A). q17 stays its 76.7s path. |
| **R3 MergingWindowSet cache** (`FRS_RS_WINSET_CACHE`, per-key recurrence) | amortize the per-record O(N) `forEachEntry` session-map re-drain (`ForStRsMapState.java:725`) | q11 (secondary) | orthogonal; the secondary q11 cause after R2a. |

**Residual-gap closure picture:** q7 still 1.46× behind ForSt even with KV-sep → R1 (deep-probe merge)
+ R2a (overlap) attack the read-path/engine residue. q11 needs R2a (the drain tail) + R3. q17 needs
the defensive carve-out so the executor default doesn't break it. **Adaptive KV-sep + R1 together are
the q7/q9/q20 stack; R2a/R3/carve-out are the q11/q17 stack; R1's adaptive-by-fan-out and KV-sep's
adaptive-by-size+pressure together let one config serve all.**

---

## 5. Never-OOM bound (mission step 3 requirement, made explicit)

The bound tying to the P2 vlog clip-reclaim + the partly-built resident cap:
- **Already built:** `FRS_VLOG_READER_CACHE_CAP` (count cap, default 2048) bounds reader HANDLES; the
  P2 `gc_sweep`/tombstone + clip-reclaim path (commit `4fd512e83`) drains reclaimable segments.
- **The gap (this design's B1):** a count cap does NOT bound BYTES, and it does not back off
  *creating* new resident state. q9 OOM'd WITH the count cap because (a) the 64 KiB chunks × cap is
  still ~128 MB of vlog buffers AND (b) the dominant additive pressure is that KV-sep keeps adding
  resident bookkeeping on top of an already-near-cap Flink heap. **B1's byte budget + pressure back-off
  is the resident-SEGMENT bound the mission calls for** — when resident vlog bytes approach the budget,
  the engine (i) evicts reader chunks to the byte budget and (ii) stops separating new flushes for that
  CF (spills them inline). This guarantees `vlog_resident_bytes ≤ FRS_VLOG_RESIDENT_BUDGET_MB`
  regardless of how scattered the death pattern is → **q9 can never OOM from the vlog side even when it
  separates.** The remaining Flink-side heap pressure is a topology/cgroup issue (1 TM, or smaller
  per-TM heap on the 35 g Mac) noted in the V3 doc — out of scope for the engine, but B1 removes the
  ~2 GiB of additive vlog pressure that was the difference between flag-ON OOM and flag-OFF fit.

---

## 6. Implementation-ready plan (mission step 5) — ordered, flag-gated default-OFF, micro-bench-gated

Each step is byte-identical when OFF and is gated on a **micro-bench (FFI / value-size / churn — NOT
NexMark)** before it can flip a default; the uniform-config NEXMark verdict runs only after all gates
pass.

**Step 1 — Raise the uniform size threshold default 128 → 256 B (Layer A).** `db.rs:14903`.
- *Behavior:* widens the inline band; q11/q17 (already <128) unaffected; q4/q7/q9/q20 (≥256) still
  separated. Byte-identical for values <128 or ≥256; only 128-255 B values change (none of the 6
  queries live there).
- *Micro-bench gate:* `churn_probe.rs` / `vlog_reader_cache_footprint.rs` with `--value-size` swept
  {16,32,64,80,128,256,512} — assert separation fires iff size ≥256; segment count for the small-value
  arm = 0. UT asserting `kv_sep_spec_for` separates a 256 B Put and inlines an 80 B Put.

**Step 2 — Byte-budget the vlog reader cache + add `vlog_resident_bytes` accounting (Layer B1, part 1).**
Extend the existing LRU (`db.rs:123-142`, `VlogReaderCache`) to evict by charged bytes (handle state +
chunk) under `FRS_VLOG_RESIDENT_BUDGET_MB` (default OFF = `u64::MAX` = today's count-only behavior).
- *Micro-bench gate:* extend `vlog_reader_cache_footprint.rs` (already measures uncapped vs cap=2048)
  with a byte-budget arm: open N segments, assert resident bytes ≤ budget and a re-open on eviction is
  byte-identical. PASS = resident plateaus at the budget (not O(N)).

**Step 3 — Pressure-gated separation back-off (Layer B1, part 2 — the adaptive decision).**
`should_separate_now(cf)` consulted by `kv_sep_spec_for` (`db.rs:12624`): returns false when resident
≥ budget AND CF reclaim-rate ~0. Gated `FRS_KV_ADAPTIVE_PRESSURE` default-OFF (OFF = always-separate,
i.e. today's flag-ON behavior).
- *Micro-bench gate:* a churn-probe arm with a synthetic scattered-death CF (non-FIFO key overwrite)
  — assert resident vlog bytes stay ≤ budget and separation backs off to inline once over budget;
  a FIFO-death arm (q7-shape) — assert it NEVER backs off (reclaims, stays under budget, full
  separation kept). UT: `should_separate_now` toggles on a forced over-budget + zero-reclaim fixture.

**Step 4 — Adaptive vlog-GC relocation auto-trigger (Layer B2).** Auto-set a modest cutoff for CFs
with reclaim-rate ~0 + climbing segment count; keep cutoff 0 for FIFO-death CFs. Gated
`FRS_VLOG_GC_ADAPTIVE` default-OFF (OFF = static `FRS_VLOG_GC_AGE_CUTOFF`, today's behavior).
- *Micro-bench gate:* `churn_probe.rs` scattered-death arm — assert auto-relocation drains mostly-dead
  segments (live segment count plateaus) without inflating q7-FIFO write-amp (FIFO arm: no relocation,
  write-amp unchanged ~1.55×).

**Step 5 — Read-side levers (compose, separate spec).** R1 adaptive S2 (`FRS_S2_FANOUT_MIN`, default
`u32::MAX`=OFF) gated on the cycle-3 `join_probe_open` matrix (deep cells match `wa_s2`, shallow/churn
match `default`); R2a + q17 carve-out gated on the deterministic q8 correctness repro + the
iter-free-batch zero-handoff Java microbench; R3 gated on a `forEachEntry` Java microbench. (Detailed
in `2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md` §3.)

**Step 6 — Uniform-config NEXMark verdict (LAST, after all gates pass).** One config:
`FRS_SST_COMPRESSION=lz4 FRS_KV_SEPARATION=true FRS_KV_MIN_BLOB_SIZE=256
FRS_VLOG_RESIDENT_BUDGET_MB=<tuned> FRS_KV_ADAPTIVE_PRESSURE=1 FRS_VLOG_GC_ADAPTIVE=1
FRS_S2_FANOUT_MIN=<tuned> FRS_RS_S2_PINNED=1 FRS_TRIVIAL_MOVE=true` — verify per query:
q4/q7/q20 still separate + PASS (≤1.25× RDB, beat ForSt), q9 FINISHES (resident vlog ≤ budget, no
OOM; needs ≤1661.6s to beat ForSt), q11/q17 inline + back to flag-OFF speeds (q11 PASS, q17 closer +
beats ForSt). This single config = the deliverable.

---

## 7. Per-step risk / confidence

- **Step 1 (size 256):** HIGH confidence, trivial — distributions are well-separated; only re-tunes
  an existing knob. Lowest risk.
- **Step 2 (byte budget):** HIGH — extends a shipped, tested cap from count to bytes; correctness is
  the same re-open-on-miss already proven.
- **Step 3 (pressure back-off):** MEDIUM — the adaptive decision; needs the reclaim-rate signal to be
  cheap + correct. The "false → inline this flush" path is byte-identical to flag-OFF per flush, so
  correctness is inherited; the risk is *tuning* (back off too early → lose write-amp on q9; too late
  → OOM). The micro-bench sizes the budget before NexMark.
- **Step 4 (auto-relocation):** MEDIUM — re-introduces write-amp on the targeted CF; must verify it
  does NOT regress q7 (the FIFO arm gate). Lower leverage than B1 (B1 alone bounds the OOM RAM).
- **Step 5 (read levers):** R1 HIGH (byte-equality proven), R2a/q17-carve-out gated on the
  long-standing q8 windowed-join race (the historical blocker — do not flip default without the
  deterministic repro), R3 LOW leverage.

---

## 8. Net answer to the mission

1. **Value-size finding:** the two families ARE cleanly separated by size (NEEDED = large 256-800 B;
   HURTS = small 16-80 B), and the small family already stays inline under the existing threshold — so
   a uniform threshold (recommend **256 B**) is the necessary first gate. **But size-threshold ALONE
   is NOT sufficient:** q9 stores LARGE values (wants separation by size, indistinguishable from
   q7/q20) yet OOMs when separated. The discriminator q9 needs (resident-vlog pressure + scattered-death
   reclaim failure) is orthogonal to value size.
2. **Recommended adaptive mechanism:** **Layer A** uniform size gate (256 B) + **Layer B1** per-CF
   memory-pressure-gated separation with a CHARGED byte budget + back-off (the never-OOM bound, the new
   adaptive signal — value size AND resident pressure AND reclaim rate, decided per-CF at runtime under
   one config) + **Layer B2** adaptive vlog-GC relocation for scattered-death CFs. q11/q17 are NOT a
   KV-sep tuning problem — their values stay inline; their residual regression is the read path, closed
   by the complementary **R1/R2a/R3** levers.
3. **Ordered plan:** Step 1 size→256 (trivial) → Step 2 byte-budget the reader cache → Step 3
   pressure-gated back-off (the adaptive decision) → Step 4 auto-relocation → Step 5 read levers
   (R1/R2a/R3) → Step 6 the single uniform-config NexMark verdict. Each flag-gated default-OFF,
   byte-identical when OFF, micro-bench-gated (FFI/value-size/churn — `churn_probe.rs`,
   `vlog_reader_cache_footprint.rs`, `join_probe_open.rs`) before any NexMark.

### File:line index
- `crates/forst-rs-engine/src/db.rs:14897-14913` — `kv_min_blob_size` (Layer A threshold, 128→256)
- `crates/forst-rs-engine/src/db.rs:12624-12642` — `kv_sep_spec_for` (Layer A+B3 decision site)
- `crates/forst-rs-engine/src/db.rs:123-142` — `vlog_reader_cache_cap` (Layer B1 — add byte budget)
- `crates/forst-rs-engine/src/db.rs:12651-12680` — `kv_gc_spec_for_compaction` (Layer B2 relocation)
- `crates/forst-rs-engine/src/db.rs:14806-14835` — `vlog_gc_age_cutoff_percent` (B2 auto-trigger)
- `crates/forst-rs-engine/src/db.rs:12686-12692` — `kv_gc_reap_dead_segments` (reclaim, live_bytes==0)
- `crates/forst-rs-storage/src/vlog.rs:64-78,329` — `VALUE_POINTER_LEN`, `DEFAULT_VLOG_READER_CACHE_CAP`
- `crates/forst-rs-storage/src/version/mod.rs:230,547,565-568` — `vlog_segments` (reclaim accounting)
- `crates/forst-rs-engine/src/column_family.rs:183-200` — `CfLifecycle` (Unbounded gate)
- `crates/forst-rs-bench/src/bin/{vlog_reader_cache_footprint,churn_probe,join_probe_open}.rs` — micro-bench gates
- Flink: `ForStRsAsyncKeyedStateBackend.java:948-976`, `ForStRsLifecycleManager.java:92-109`,
  `WindowOperator.java:246-250` — lifecycle wiring gated OFF ⇒ all CFs `Unbounded`
