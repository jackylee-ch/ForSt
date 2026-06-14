# KV-separation optimal for ALL 8 queries under ONE config — profiler investigation + both-sides root-cause model

**Date:** 2026-06-14
**Author:** PMC-1 (state-backend/engine architect) — DEEP PROFILER + MINI-BENCH INVESTIGATION ONLY.
No production `.rs` behavior changed; no NexMark run. Instrumentation + micro-benches live in the
`kvsep-profile` worktree (`crates/forst-rs-bench/src/bin/vlog_deref_latency.rs`).
**Base:** `origin/forst-rs` @ `cd25e7d47` (worktree `/tmp/kvprof-wt`, branch `kvsep-profile`).
**Hardware:** Mac (system allocator; local FS). Latency numbers are same-box, release build.

## HARD CONSTRAINT (user reframe — the goal of this investigation)

> "Achieve optimal performance for ALL queries **WHILE USING kv-separate**."

KV-separation stays **ON uniformly**. The fix is to make its **downsides vanish**, NOT to disable or
back off separation. The adaptive pressure-back-off (`FRS_KV_ADAPTIVE_PRESSURE`) is **REFUTED**: it
robbed the beneficiaries (q4 311→406, q7 695→949, q20 824→875 ≈ flag-OFF) while q9 *still* OOM'd. A
mechanism that "stops separating under pressure" is the wrong lever — it throws away the write-amp win
that is the entire point. This investigation builds the both-sides root-cause model and proposes a fix
that keeps separation **always ON** and makes reads + memory cost ~0.

---

## 0. Verdict data this investigation must reconcile (given)

| query | flag-ON | flag-OFF | KV-sep verdict | side that hurts |
|---|---|---|---|---|
| q4  | **311** | 384 | WANTS-ON (write/rewrite join) | — |
| q7  | **695** | 937 | WANTS-ON (heaviest windowed join) | — |
| q19 | **216** | 464 | WANTS-ON (interval join + Top-N) | — |
| q20 | **824** | 956 | WANTS-ON (heavy interval join) | — |
| q9  | **OOM** | 1828 | HURTS — OOM | **memory** (read+resident) |
| q11 | 216 | **119** | HURTS — +82% | **read/drain** |
| q17 | 151 | **111** | HURTS — +36% | **read/drain** |

The two families overlap in value size (q9 stores LARGE ~512 B, like q7/q20 which benefit), so a
size threshold cannot separate them. The downsides must be attacked at their mechanism, not gated.

---

## 1. READ-SIDE downside (Q1) — where the extra cycles go, and whether inline values pay

### 1.1 Inline values pay NOTHING extra on the read path (code-proven)

The read path branches on `OpType` per row. A separated value is `OpType::BlobRef`; an inline value
(q11/q17 small accumulators, below the 256 B threshold — `db.rs:15456 kv_min_blob_size` default 256)
is a normal `OpType::Put`. Every read site dispatches on this op:

- **Scan / value-carrying drain** (the q11/q17 windowed-agg drain path):
  `LazyPrefixIter::next_with_value` (`db.rs:17135-17258`). Phase C decision table (`db.rs:17243-17256`):
  `OpType::Put → ValueDecision::Put(v)` returns the value Arc **directly** — no decode, no reader
  lookup, no deref. Only `OpType::BlobRef → ValueDecision::Blob(ptr)` (`db.rs:17252`) carries pointer
  bytes that the consumer derefs (`db.rs:9905-9909`, `10628-10632`).
- **Point get / batch_get** (`batch_get_vectorized`, `db.rs:11869`): every tier match arm resolves
  `OpType::Put → resolved[i] = Some(entry.value)` inline (`db.rs:11918-11920, 12007, 12109, 12208,
  12304`). Only `OpType::BlobRef` arms call `self.vlog_deref` (`db.rs:12140, 12238, 12336`).
- **MVCC collection** (`iter_versions_of`, `db.rs:3564`): `if matches!(op, OpType::BlobRef)` gates the
  deref (`db.rs:3570`); Put rows take the plain `entries.push` arm (`db.rs:3575-3576`).

**FINDING (Q1, high confidence — code-grounded):** with KV-sep ON, a value that stays **inline takes
the byte-identical read path it takes with KV-sep OFF.** The only added work is a per-row `OpType`
discriminant compare that already exists for Put/Delete/Merge — a predictable branch, effectively
free. **There is NO unconditional deref or vlog-reader code that fires for inline values.**

⇒ The q11/q17 flag-ON regression is therefore **NOT** the KV-sep read path acting on their (inline)
values. It is the rest of the lever stack that historically shipped with `FRS_KV_SEPARATION=true`
(the windowed-agg merge/drain read path — see `2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md`
R1/R2a/R3, and the cycle-3 finding that with KV-sep ACTIVE the legacy deep-probe merge explodes
18.09µs → S2 1.00µs). **The KV-sep flag must be decomposed from those read-path levers; once a value
is inline, KV-sep ON ≈ OFF for it. No KV-sep-side change is needed for q11/q17 beyond keeping the
size gate.** (Flagged: the residual q11/q17 fix lives in the read-side spec, not here.)

### 1.2 The REAL read-side downside is the SEPARATED-value deref under SCATTERED access (q9 probe side)

This is where KV-sep ON genuinely costs cycles. Mini-bench `vlog_deref_latency` (one segment, key-order
append = the flush shape; `VlogReader::get` is the deref). **ns per deref, release, local FS:**

| value size | cold SEQ (scan locality) | random (point-get) | hot HIT (cache floor) | **COALESCED (fix)** |
|---|---|---|---|---|
| 64 B   | 30.0  | **2361.2** | 18.3  | **22.5** |
| 512 B  | 98.4  | **2982.7** | 58.5  | **89.8** |
| 4096 B | 412.3 | **3333.9** | 210.8 | **401.4** |

(lz4 arm tracks within ~10%; full table in the bench output.)

**Root cause of the read-side downside — the single-slot 64 KiB chunk cache (`vlog.rs:234-283`).**
`VlogReader` holds ONE chunk `(start_offset, Vec<u8>)` behind a `Mutex` (`vlog.rs:240-241`). On a miss
it reads a **forward 64 KiB granule** (`VLOG_READ_CHUNK`, `vlog.rs:234`) and keeps it (`vlog.rs:281`).

- **Scan/sequential derefs HIT** the chunk (consecutive key-order values sit contiguously) → ~30-400
  ns, near the `hot HIT` CPU floor. This is the q4/q7/q9/q20 *value-carrying drain* path
  (`next_with_value` → `ValueDecision::Blob`) — already cheap. ✅
- **Random / scattered derefs THRASH** the single slot: each deref reads a fresh 64 KiB chunk, returns
  one value (e.g. 64 B), and the next scattered deref evicts it → **~2400-3400 ns regardless of value
  size** (dominated by the 64 KiB read+discard, NOT the value). For a 64 B value this is **~1000× read
  amplification** (read 64 KiB to return 64 B) and **~80-130× the CPU floor.** ❌

**q9 hits exactly the bad case.** q9 is an interval JOIN + ROW_NUMBER(Rank): the probe side issues
**scattered point-gets** (`frs_get`/`frs_batch_get` → `db.get`/`batch_get_vectorized`,
`lib.rs:1731, 2031, 3346`), and each separated row is dereffed **per-key inline** through
`self.vlog_deref` (`db.rs:12140/12238/12336`) — **NO coalescing**. So q9's probe side pays the
~3000 ns scattered-deref tax on every separated join row, on top of (and amplifying) its memory
pressure. This is the read-side half of "KV-sep HURTS q9".

**FINDING (Q1, high confidence — mini-bench + code):** the read-side downside is (a) the single-slot
chunk cache thrashing on scattered access, and (b) `batch_get_vectorized` dereffing per-key with no
physical-location coalescing. Inline values are unaffected; the cost is real only for SEPARATED values
read in scattered order — the q9 probe pattern.

### 1.3 CAN the deref be driven to ~zero? YES — coalescing proves it (Q4 evidence)

The `COALESCED` arm takes the **same scattered batch** as `random` but **sorts the derefs by
(segment, offset)** before issuing them, then the caller reorders results back to key order (zero
correctness cost — values are independent). Result:

- 64 B: **2361 → 22 ns (105×)**; 512 B: **2983 → 90 ns (33×)**; 4096 B: **3334 → 401 ns (8×)**.

Coalesced scattered deref lands **at the scan-locality floor**. The 64 KiB chunk that was wasted
per-value is now amortized over every value that falls in it. **The read-side downside is eliminable to
~0 by batched offset-coalescing — KV-sep ON reads ≈ OFF reads once derefs in a batch are coalesced.**

---

## 2. MEMORY downside (Q2) — quantify q9's split; is the vlog resident eliminable?

### 2.1 The vlog reader resident IS already bounded (mini-bench `vlog_reader_cache_footprint`)

`VlogReaderCache` (`vlog.rs:359`) is now a bounded LRU with BOTH a count cap (`DEFAULT_VLOG_READER_
CACHE_CAP=2048`, `vlog.rs:329`) AND a charged byte budget (`with_capacity_and_budget`, `vlog.rs:392`;
charge `VLOG_READER_CHARGE_BYTES = 64 KiB + 4 KiB`, `vlog.rs:340`). Measured (50 000 segments touched):

| arm | resident readers | chunk bytes | RSS delta |
|---|---|---|---|
| uncapped (pre-fix)        | 50 000 | **3.05 GiB** | 871 MiB |
| count-cap (2048)          | 2 048  | 128 MiB | 1.5 MiB |
| **byte-budget (64 MiB)**  | 963    | **60.2 MiB** | 688 KiB |

**FINDING (Q2, high confidence):** resident vlog-reader cost is now **O(budget), not O(segments)** —
~64-128 MiB, *independent* of how many segments q9 ever touches. **The vlog reader cache is NO LONGER
the q9 OOM driver.** (This is why the byte-budget bound, on its own, did not stop the OOM — it was
already bounded; the OOM is elsewhere.)

### 2.2 The q9 OOM split (from the byte budget + the V3 verdict + `2026-06-14-q9-kvsep-oom-rootcause.md`)

q9 OOM'd @~75-83 M of 100 M rows on the 35 G Mac (2 TM×16g + 1 JM×4g = **36g > 35g RAM** — the cgroup
arithmetic is the proximate trigger). The engine-side vlog resident is bounded to ≤128 MiB (§2.1). The
dominant resident is the **Flink-side join + Rank (ROW_NUMBER) state heap** — q9 is the single heaviest
join in the set and buffers full joined rows per key for ranking. KV-sep ON rode **~2 GiB higher** than
flag-OFF on the SAME join-state pressure, and that ~2 GiB is what crossed the cgroup. Where the ~2 GiB
additive comes from with the reader cache already bounded:

1. **Double residency during separation, not the reader cache.** The flush separates the *post-flush*
   memtable into the vlog (`flush.rs:286-294`), but the **resident shadow** enrolls the PRE-separation
   memtable with FULL values (`db.rs:12009-12011` comment: "the resident shadow enrolls the
   PRE-separation memtable (full values)"). So for the window a flushed memtable is still resident,
   q9 holds full values in the shadow AND pointer rows + readable chunks in the vlog path — strictly
   more than flag-OFF, which holds only the shadow.
2. **`Version::vlog_segments` Vec cloned per edit** (`version/mod.rs:547`) — small per entry (~32 B)
   but grows with live-segment count and adds allocator churn under a tight cgroup (`rootcause` #3,
   HYPOTHESIS — needs a heap profile to size; likely secondary).
3. **The scattered-deref read tax (§1.2) lengthens the run**, so q9 spends MORE wall-clock at its
   peak join-state size, raising the probability of crossing the cgroup at the worst moment.

**FINDING (Q2, mixed confidence):** the OOM is **NOT eliminable vlog-reader resident** (already
bounded, high confidence). It is the box arithmetic (36g>35g) + the Flink join/Rank heap (out of
engine scope) + a ~2 GiB *additive* from KV-sep that IS partly eliminable in the engine: (a) the
pre-separation shadow double-residency, and (b) the run-length inflation from the slow scattered deref
(fixed by §1.3 coalescing). **Architectural change that makes KV-sep add ~0 resident:** keep the byte
budget (done), AND make the separated read path coalesced+fast so the run is shorter and the peak
window narrower; the remaining Flink-heap + cgroup pressure is a topology decision (1 TM, or smaller
per-TM heap on a 35 G box) the engine cannot fix. **Flagged for measurement:** a heap profile of a
q9-shape run is needed to attribute the ~2 GiB precisely between (1) shadow double-residency and (2)
Vec/manifest churn — this investigation bounds the reader cache (the previously-blamed structure) and
shows it is NOT the driver, but the exact split of the residual ~2 GiB is HYPOTHESIS.

### 2.3 Streaming / mmap the vlog (the "add ~0 RSS" architectural question)

The reader uses positioned `pread` via `read_at` (`local_fs.rs:102-107`), buffering a 64 KiB chunk per
reader. Two ways to drive the separated-value resident toward ~0:
- **Keep pread, shrink+share the chunk:** the 64 KiB granule is the resident unit (§2.1). A
  coalesced batch (§1.3) needs the chunk only for the duration of the batch — a *transient*,
  per-batch coalesce buffer (dropped after the batch) adds ~0 *steady-state* resident vs the current
  per-reader retained chunk. This is the lowest-risk "add ~0 resident" lever and composes with the fix.
- **mmap the segment (disagg caveat):** mmap makes the OS page cache the resident bound (already
  charged to the box, evictable under pressure) — separated values add ~0 *process-heap* RSS on local
  FS. BUT segments live on opendal/remote on the disagg path (`is_local()` false,
  `opendal_backend.rs:617`), where mmap is not available — the remote fetch must be explicit. So mmap
  is a local-only optimization; the **coalesced explicit fetch (§1.3) is the disagg-correct form** and
  is the recommended primary. (Flagged: mmap is a possible local-FS-only add-on, lower priority.)

---

## 3. WRITE-SIDE benefit (Q3) — what MUST be preserved

The benefit is the documented and verdict-confirmed premise: KV-sep keeps the key-LSM small (21 B
pointer rows instead of 256-800 B values), so **compaction rewrites pointers, not values** — the
write-amp reduction that wins q4/q7/q19/q20. From `2026-06-13-write-path-redesign-survey.md` /
`q9-kvsep-oom-rootcause` §2: relocation-OFF default (`FRS_VLOG_GC_AGE_CUTOFF=0`) gives ~1.55× write-amp
on q7-FIFO churn vs ~3.1× if relocation rewrites — and the verdict data (q7 695 vs 937, q4 311 vs 384,
q19 216 vs 464, q20 824 vs 956) is the end-to-end confirmation. The "FIFO-death whole-segment reclaim"
(deaths follow arrival order → segments die whole → reclaim fires for free) is what makes the q7/q19
family cheap. **The fix must keep KV-sep ALWAYS ON and must NOT re-introduce write-amp on these CFs**
(i.e. must NOT auto-enable relocation broadly, and must NOT back off separation). The coalesced read
fix (§1.3) and the byte budget (§2.1) are **read/memory-side only** — they do not touch the write path,
so the write benefit is preserved by construction.

(Micro-bench note: `churn_probe --kvsep` measures assembled write-amp + resident vlog over a FIFO-churn
workload; the smoke run is too short to trigger compaction, so the write-amp number must come from a
full-duration run. The benefit is not in dispute — it is the user's stated premise and the verdict
data confirms it — so this investigation did not re-measure it; flagged if a fresh number is wanted:
`churn_probe --kvsep --trivial-move --runs 3` full duration.)

---

## 4. BOTH-SIDES architecture (Q4) — make KV-sep a pure win everywhere under ONE config

The lever is exactly the user's hypothesis: **make vlog deref zero-overhead on reads (coalesced) +
make vlog resident ~0 (bounded, transient buffers)** so KV-sep ON is a pure win. Evidence:

- Read side: coalescing drives scattered deref to the scan-locality floor (§1.3, 8-105×). It does NOT
  touch shallow/inline scans (q11/q17 stay on the byte-identical Put path, §1.1).
- Memory side: the reader cache is already O(budget) (§2.1); a transient per-batch coalesce buffer
  adds ~0 steady-state resident (§2.3).
- Write side: untouched → benefit preserved (§3).

This **generalizes R1 (adaptive S2, 18µs→1µs deep-probe merge):** R1 made the *key-LSM* k-way merge
fast under deep fan-out; the deref-coalesce is the **value-log analogue** — make the *value* fetch fast
under scattered fan-out. Together: separate large values (write win) + loser-tree the BlobRef rows
(R1 read win) + coalesce the value derefs (this, value-log read win). All three are "fast under
fan-out, byte-identical when shallow" — adaptive by structure, not by per-query config.

### Recommended approach — **A: coalesced batched deref + transient buffer (keep separation always ON)**

The primary, disagg-correct fix.

1. **Coalesce derefs in `batch_get_vectorized`.** Collect the BlobRef pointers across the batch
   (instead of dereffing per-key at `db.rs:12140/12238/12336`), group by `segment_id`, sort by
   `offset`, deref in offset order against a shared per-batch buffer, then scatter results back to key
   slots. Mini-bench: 8-105× on the scattered pattern, lands at the scan floor.
2. **Coalesce derefs in the scan/value-carrying path** (`next_with_value` consumers, `db.rs:9905,
   10628`): the `ValueDecision::Blob` values already arrive in key order (= ~offset order within a
   segment) so they already HIT the chunk (§1.2 cold-SEQ ~100 ns) — low priority, but a batched drain
   could still coalesce across segments.
3. **Transient coalesce buffer** (don't grow the retained per-reader chunk): the coalesce reads ranged
   bytes for the batch into a scratch buffer dropped after the batch → ~0 steady-state resident
   added (§2.3).
4. **Keep the byte budget on the reader cache** (already shipped, §2.1) for the steady-state handle
   bound.

**Trade-offs:** (+) zero correctness risk (values independent; reorder is pure); (+) disagg-correct —
coalescing turns N scattered remote fetches into ~1 ranged GET, the win GROWS on remote (§5);
(+) write path untouched → benefit preserved; (+) inline values untouched. (−) batch_get must buffer
pointers + reorder (small CPU + alloc, dwarfed by the 8-105× I/O win); (−) point-`get` (N=1) cannot
coalesce — but a single deref is one chunk read, the floor, not the thrash case.

### Alternative B — **mmap segments on local FS + page-cache as the resident bound**

Make separated values add ~0 *process-heap* RSS by mmap'ing segments; deref = a slice into the mapped
region (no per-deref Vec until the value is handed up). **Trade-offs:** (+) ~0 heap RSS on local FS;
(−) **does NOT work on disagg** (opendal/remote, `is_local()` false) — the remote case still needs the
explicit coalesced fetch, so B cannot be the primary; (−) mmap page faults on scattered access still
read a full page (4 KiB) per value — better than 64 KiB but worse than coalesced. **Recommendation:** B
is a *local-only add-on* under A, not a replacement.

### Alternative C — **second-tier value cache (charged, shared with block cache)**

Cache hot *deref results* (not just chunks) in a charged LRU shared with the block cache, so repeated
derefs of the same value (re-probed join keys) hit memory. **Trade-offs:** (+) helps re-probe-heavy
joins; (−) does NOT help the FIRST scattered deref of each value (q9's dominant pattern is one deref
per row, not re-probe) — the mini-bench's `random` arm is all first-touch, so C would not move it; (−)
adds a charged structure to tune. **Recommendation:** C is orthogonal and lower-leverage than A;
consider only if a re-probe profile shows value-reuse.

### RECOMMENDATION

**Adopt A (coalesced batched deref + transient buffer), keep the reader byte budget, keep separation
ALWAYS ON.** It is the only option that (i) drives the read-side downside to ~0 by the measured 8-105×,
(ii) is disagg-correct and in fact *better* on remote, (iii) preserves the write benefit untouched, and
(iv) needs no per-query config and no back-off. B (mmap) is an optional local-FS add-on; C is orthogonal
and only if a reuse profile justifies it. The q11/q17 residual is NOT a KV-sep problem (§1.1) — it is
closed by the read-side spec's R1/R2a/R3, independently.

---

## 5. Disagg applicability (Q5)

On the disaggregated path the vlog segment is remote (opendal, `is_local()` false,
`opendal_backend.rs:617`), so a deref becomes a **remote ranged GET** — RTT-bound, not pread-bound. The
scattered-deref pathology is **far worse remotely**: N scattered point-gets = N remote round-trips. The
coalesce (§1.3, approach A) is exactly the disagg-critical fix — sort by (segment, offset) and issue
**one ranged GET per segment** (or a coalesced multi-range GET) instead of N. The local mini-bench's
8-105× is a *lower bound* on the remote win, where each saved deref also saves an RTT (the
`scan_cold_start` / `prime_cold_reader_opens_concurrent` machinery, `db.rs:13367`, already proves
overlap-the-RTT pays on remote). The byte budget (§2.1) matters MORE remotely (each resident reader may
pin remote-file bookkeeping). **Coalescing + bounded readers is the disagg-correct both-sides fix;
mmap (alt B) is local-only and does not apply remotely** — another reason A is the primary.

---

## 6. Net root-cause model (both sides) and what changes

**The model.** KV-sep is a *pure write-amp win* (preserve, §3). Its two downsides are NOT intrinsic to
separation — they are artifacts of the read path's *single-slot 64 KiB chunk cache* and the *per-key,
un-coalesced deref*:

- **Read downside:** scattered (point-get / join-probe) derefs thrash the single chunk slot → ~3000 ns
  / ~1000× read-amp for small values (q9 probe side). Inline values (q11/q17) pay nothing extra — their
  regression is a *different lever* in the stack, not KV-sep (§1.1).
- **Memory downside:** the vlog *reader* resident is already bounded to ≤128 MiB (§2.1) — NOT the OOM
  driver. q9's OOM is box arithmetic (36g>35g) + the Flink join/Rank heap + a ~2 GiB engine additive
  (pre-separation shadow double-residency + run-length inflation from the slow scattered deref). Fixing
  the read side shortens the run and narrows the peak window; the rest is topology.

**The fix (one config, separation always ON):** coalesce batched derefs by physical location
(transient buffer) → scattered deref drops to the scan floor (8-105× measured), eliminating the read
downside and shortening q9; keep the reader byte budget for the resident bound. Disagg-correct and
better on remote. Write benefit untouched. q11/q17 need only the size gate (already 256 B) + the
independent read-side R1/R2a/R3.

**Confidence:** §1.1 (inline = free) HIGH (code). §1.2-1.3 (scattered deref cost + coalesce fix) HIGH
(mini-bench + code). §2.1 (reader resident bounded) HIGH (mini-bench). §2.2 (q9 ~2 GiB additive split)
MIXED — reader cache ruled out HIGH; the exact split of the residual ~2 GiB between shadow-double-
residency and Vec churn is HYPOTHESIS (needs a q9-shape heap profile). §3 (write benefit) GIVEN
(verdict data). §5 (disagg) MODEL (the local 8-105× is a lower bound; a remote multi-range mini-bench
would quantify it — flagged).

---

## 7. Flagged for further measurement (before/with implementation)

1. **q9-shape heap profile** to attribute the residual ~2 GiB additive (shadow double-residency vs
   `vlog_segments` Vec churn vs other) — §2.2 HYPOTHESIS. A jemalloc/heaptrack run on a q9-shape churn
   workload with KV-sep ON vs OFF.
2. **Remote multi-range deref mini-bench** (opendal against an S3 emulation) to quantify the disagg
   coalesce win (§5) — the local 8-105× is a lower bound.
3. **Full-duration `churn_probe --kvsep` write-amp number** if a fresh write-amp figure is wanted (§3)
   — the smoke run does not trigger compaction.
4. **A coalesced-deref micro-bench wired into `batch_get_vectorized`** (instrumentation arm) to confirm
   the engine-level batch path realizes the storage-level 8-105× before any NexMark.

### File:line index (evidence)
- `crates/forst-rs-storage/src/vlog.rs:234` — `VLOG_READ_CHUNK` (64 KiB single-slot granule, the thrash source)
- `crates/forst-rs-storage/src/vlog.rs:255-283` — `VlogReader::get` (single-slot chunk cache, miss = forward 64 KiB read)
- `crates/forst-rs-storage/src/vlog.rs:329,340,359-392` — `VlogReaderCache` count cap + byte budget (resident bound, O(budget))
- `crates/forst-rs-engine/src/db.rs:13329-13334` — `vlog_deref` (per-pointer decode + reader.get; the per-key deref)
- `crates/forst-rs-engine/src/db.rs:12140,12238,12336` — `batch_get_vectorized` per-key inline deref (NO coalescing — the fix site)
- `crates/forst-rs-engine/src/db.rs:17243-17256` — `next_with_value` Phase C (Put inline / BlobRef carry — inline pays nothing)
- `crates/forst-rs-engine/src/db.rs:3564-3576` — `iter_versions_of` BlobRef gate (MVCC deref only on BlobRef)
- `crates/forst-rs-engine/src/flush.rs:286-294,384-396` — separation at flush (write path, preserve)
- `crates/forst-rs-engine/src/db.rs:12009-12011` — resident shadow enrolls PRE-separation memtable (full values — the double-residency note)
- `crates/forst-rs-io/src/local_fs.rs:102-107` / `opendal_backend.rs:617` — pread (local) vs remote (disagg deref = ranged GET)
- `crates/forst-rs-bench/src/bin/vlog_deref_latency.rs` — the deref-latency + coalesce mini-bench (this investigation)
- `crates/forst-rs-bench/src/bin/vlog_reader_cache_footprint.rs` — the resident-budget mini-bench (O(budget) bound)
- Prior: `docs/superpowers/specs/2026-06-14-q9-kvsep-oom-rootcause.md`, `2026-06-14-adaptive-kvsep-dynamic-design.md` (the REFUTED back-off), `2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md` (R1/R2a/R3 read-side levers)
