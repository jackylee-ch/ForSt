# Sorted-run discipline for forst-rs — H1 confirmation + leveled-compaction design

Date: 2026-06-12 · Author: PMC architect agent (sorted-run-discipline brief) · Worktree: `forst-rs`
Companion: `2026-06-12-forst-architecture-q7-analysis.md` (H1 hypothesis source).
Evidence rule honored: every design claim below traces to a measurement in §1/§2 or a code citation in §3.

---

## 0. Verdict (executive)

**H1 is CONFIRMED — with a corrected mechanism split.** The architecture doc's H1 said
"tiered overlapping L1..Ln + L0\@40/64 + shadow tier ⇒ unbounded probe fan-out". The code
+ microbench show forst-rs **already maintains non-overlapping L1+ per CF** (§3.1), so the
read-side fan-out driver is **L0 depth alone** — and it is real and large (microbench: probe
p50 649 µs → 2 997 µs as L0 grows 5 → 40; §2 cell C). The **bigger, previously-underweighted
half is the write side**: the current compaction scheme produces **write-amp ≈ 7.6×**
(±0.1 across 3 cells × 3 runs) under a q7-shaped churn at only ~1 GiB live state, exactly
matching the remote q7 iostat signature (writes 435-682 MB/s > reads 190-292 MB/s at
98-99 % disk util — `q7-analysis §1 Q7P32`). q7 on the remote box is therefore primarily a
**disk-write-volume problem** (H1-write/H4) compounded by an **L0-depth read problem when
compaction falls behind** (H1-read).

ForSt q7 remote pin (PART A): **FINISHED 1379.7 s, out_rows 92,000,002 EXACT — ForSt-remote ≈ RocksDB-remote (1367.6 s); the de-facto remote q7 bar is ~1 370-1 380 s, NOT a Mac-ratio extrapolation (the Mac 2.46× does not transfer); frs must claw back 2376.4 → ~1380**.

Top design moves (§4), each sized by the measurements: (M1) overlap-scoped + clean-cut
compaction input picking (today every L0→L1 rollup rewrites the CF's ENTIRE L1 —
`db.rs:5963-5977`); (M2) trivial move; (M3) concurrent per-level-pair compactions (today ALL
compactions per DbImpl serialize on one global mutex — `db.rs:5917`); (M4) L0 trigger
discipline 20/36 once M1-M3 hold; (M5) SST compression parity on the remote runner
(frs runs `FRS_SST_COMPRESSION=none` while RocksDB/ForSt run Snappy — a pure disk-bytes
multiplier on top of the 7.6×); (M6) shadow-tier integration into the run-count budget.

---

## 1. PART A — ForSt q7 remote pin

| Cell | Result | Source |
|---|---|---|
| q7 remote 8c/32g, ForSt (forst-local, JDK17) | **FINISHED wall=1379.7 s, src_out=92,000,188, out_rows=92,000,002 (exact parity with frs and RocksDB)** | `logs/q7forst.log`, tag `q7forst` |
| q7 remote, forst-rs `FRS_IO_URING=on` | 2376.4 s (out 92,000,002) | SWEEP:2142-2148 |
| q7 remote, RocksDB | 1367.6 s | SWEEP:2147 |
| q7 Mac, ForSt | 586.8 s | master-strategy §A q7 row |

Launch notes (reproducibility):
- Runner: `/ssd2/jackylee/frs-bench/q7forst-run.sh` = `q5z-run.sh` + `-e LD_LIBRARY_PATH=`
  in `DKR_COMMON`; env `TEMPLATES=$HOME/workenv/q5-templates JDK17_IN_IMG=/opt/java/openjdk
  TOPO=split`; network pre-created `frs-q7forst-net` subnet `192.168.218.0/24`.
- **Blocker found & fixed (was killing every forst-local attempt on the box, incl. the
  q5-trio's q5z forst2/3/4 crash-loops):** the bench image
  `flink:2.2.1-jdk17-forst-bench-tools-20260609` bakes `LD_LIBRARY_PATH=/opt/flink/lib/native`
  containing a **stale 10.27 MB `libforstjni.so`** that lacks
  `Java_org_forstdb_ColumnFamilyOptions_setTableFactory`; `System.loadLibrary("forstjni")`
  prefers it over the self-consistent `forstjni-0.1.8.jar` native (13.76 MB linux64,
  verified locally: class declares `setTableFactory(long,long)` and the jar's `.so` exports
  it). Symptom: `UnsatisfiedLinkError: ...setTableFactory(long, long)` → job RESTARTING
  crash-loop, src_out=0. Fix: empty `LD_LIBRARY_PATH` in the container env.

### 1.1 ForSt q7 steady-phase iostat (30 s × 4, nvme2n1 — the bench data disk) — direct H1 evidence

| Interval | rMB/s | wMB/s | w/s | await | %util |
|---|---|---|---|---|---|
| 2 | 0.00 | 108.7 | 924 | 4.7 ms | **6.3 %** |
| 3 | 0.00 | 100.6 | 859 | 5.1 ms | **5.9 %** |
| 4 | 0.00 | 116.4 | 995 | 5.9 ms | **6.6 %** |

vs forst-rs q7 mid-run on the SAME disk (Q7P32 capture, q7-analysis §1): reads
190-292 MB/s + writes 435-682 MB/s at **98-99 % util**, await ≤ 38 ms, queue 96-230.

**ForSt does NOT show the write-saturation.** Same query, same topology (split, 2 TM ×
4c/16g), same box: ForSt writes ~110 MB/s (Snappy-compressed flush+compaction, leveled
discipline) and reads ~0 MB/s physical (hot set fully cached: block cache from managed
memory + OS page cache); forst-rs pushes 4-6× the write volume plus hundreds of MB/s of
physical reads and pins the disk. Normalized per event: ForSt ≈ 110 MB/s ÷ ~80 K ev/s
≈ **1.4 KB written/event**; forst-rs ≈ ~550 MB/s ÷ ~41 K ev/s ≈ **13 KB/event — ~10×**.
H1 confirmed end-to-end: the q7 gap is I/O volume (write-amp × compression × read-amp),
not probe CPU (S2 falsifier) and not the FFM boundary (q7-analysis §2.1).

---

## 2. PART B — H1 microbench (churn_probe)

Bench: `crates/forst-rs-bench/src/bin/churn_probe.rs` (new, committed with this doc).
Workload = q7 interval-join shape: two interleaved append streams (`a…`/`b…` prefixed,
4 096 buckets/stream), 200 K rows/s sustained (rate-capped, identical across cells),
200 B incompressible values, TTL deletes trailing 4 M rows (~1 GiB live steady-state),
1 thread of continuous `prefix_scan` probes (the exact q7 probe API), 90 s × 3 runs/cell,
medians. Instruments: (1) write-amp via a byte-counting `FileSystem` wrapper over
`LocalFileSystem` (physical appended bytes ÷ logical put bytes); (2) per-level live-file
counts every 5 s; (3) windowed probe p50/p99; (4) engine `FRS_BULK_SAMPLE` DECAY_ATTR
n_ovl lines (supporting). Mac, system allocator, same-session A/B only.

### 2.1 Cell matrix (medians of 3)

| Cell | Config | write-amp | probe p50 late | probe p99 late | p50 degr. (early→late) | L0 at end |
|---|---|---|---|---|---|---|
| A default | trigger 4 / slowdown 40 / stop 64 | **7.68** | 649 µs | 2 205 µs | 3.57× | 1 |
| B tight | slowdown 20 / stop 36 | **7.54** | 688 µs | 2 213 µs | 3.62× | 0 |
| C nocompact | compaction off (dose-response) | **0.97** | **2 997 µs** | 3 498 µs | **10.94×** | **41** |
| D leveled-emu | + `compact_all` sweep every 15 s | **7.76** | 693 µs | 2 146 µs | 3.38× | 0 |
| E rocksdb | identical workload, RocksDB 8.x leveled (LZ4, WAL off, same buffer/level geometry) | **3.91** | **162 µs** | 279 µs | 3.65× | 2 |

### 2.2 Findings

1. **Write-amp ≈ 7.6× is intrinsic to the current scheme, not a starvation artifact.**
   A, B and D converge at 7.5-7.7× while compaction keeps up (L0 ≤ 5 throughout). The
   trigger knobs (B) change nothing — they only bind when compaction falls behind.
2. **Read-side fan-out dose-response (H1-read), clean and monotone** (cell C run 0):

   | t | L0 files | probe p50 | probe p99 | probes/5s |
   |---|---|---|---|---|
   | 5 s | 4 | 2 µs | 90 µs | 526 K |
   | 20 s | 16 | 649 µs | 961 µs | 7 490 |
   | 35 s | 32 | 1 768 µs | 2 279 µs | 2 806 |
   | 50-80 s | 40 (pinned at slowdown trigger) | ~3 000 µs | 3 400-4 500 µs | ~1 650 |

   Probe cost scales ~linearly with sorted-run count; at the default slowdown trigger
   (L0=40) probes run **4.6× slower** than the disciplined case and probe throughput is
   ÷4.6. The remote q7 history ("q7 248K→13K decay when one bg thread couldn't keep up",
   `write_controller.rs:76-84`) is this curve at production scale.
3. **The two failure modes are exclusive locally but COMPOUND remotely.** Locally either
   compaction keeps up (write-amp 7.6×, probes fast) or it doesn't (write-amp ~1, probes
   4.6× slow). On the remote shared NVMe, q7's ingest×7.6 write volume saturates the disk
   (98-99 % util), which slows compaction, which deepens L0, which multiplies probe I/O —
   the compounding loop the iostat capture shows mid-run.
4. **RocksDB on the IDENTICAL workload: write-amp 3.91 (vs 7.68 = frs writes 1.96× the
   physical bytes) and probe p50 162 µs (vs 649 µs = 4.0×; p99 279 vs 2 205 µs = 7.9×),
   at the same 200 K rows/s.** Perfectly reproducible (3.91 in all three runs). Its layout
   is `[L0=2, …, L5=3, L6≈20]` — `level_compaction_dynamic_level_bytes` (default true in
   RocksDB 8.x, `advanced_options.h:691`) sends data to the resting level in ONE hop,
   while forst-rs cascades L1→L2→… through fixed 256 MB-base targets. The probe gap at
   EQUAL run-counts (frs [1,5,12] vs rdb [2,0,…,3,20] ≈ same fan-out) additionally shows
   a per-run probe-cost constant the discipline does not explain — consistent with the
   architecture doc's H5 residue; the write-amp gap, however, is pure discipline (W1-W4 +
   dynamic targets), measured on incompressible values, i.e. BEFORE compression parity
   (M5) widens it further in production.

### 2.3 Where the 7.6× comes from (code-traced, §3 anchors)

Per 256 MB of ingest at ~1 GiB live state the engine writes: flush (1×) + L0→L1 rollup
rewriting **all of L0 + the CF's whole L1** (`db.rs:5954-5977`, no overlap subsetting:
≈ 2×) + L1→L2 descents (1 file + its next-level overlap ≈ ×(1+mult/k)) — and every byte
is rewritten, never moved (no trivial-move). Σ ≈ 7-8× at this state size, growing with
depth. RocksDB's leveled scheme has the same asymptotics but: subsets L0→L1 inputs
(`GetOverlappingInputs`, `compaction_picker.cc:443`), clean-cut expands instead of
whole-level (`ExpandInputsToCleanCut`, `:218`), trivial-moves non-overlapping files
(`compaction.cc:519`), runs compactions concurrently (per-level-pair exclusivity +
subcompactions), and compresses (Snappy per level in Flink).

---

## 3. Code anatomy (what exists vs what's missing)

### 3.1 What forst-rs ALREADY has (do not rebuild)

- **Non-overlapping L1+ per CF** — maintained by construction: L0→L1 rollup consumes all
  L0 + all L1 (output = sorted, key-boundary split, `FRS-LEVELED-COMPACTION` multi-file
  outputs, `compaction.rs:66-83`); Ln→Ln+1 picks 1 src + ALL overlapping next-level files
  (`db.rs:4291-4323`). Inductively the level stays clean-cut.
- **Binary-search locator exploiting it** — `FRS-LOCATOR-LOWER-BSEARCH` + per-CF views
  (`version/mod.rs:698-870`): O(log + matched) per level, monotonicity VERIFIED per
  immutable Version, linear fallback when violated. Fan-out per probe is already
  ≈ L0_count + ~1/level (+ shadow) — confirmed by cell C (probe cost tracks L0 count).
- **Level size targets** — `max_bytes_for_level_base` 256 MB × mult 10
  (`config.rs:257-262`), shallowest-over-budget pick (`db.rs:4244-4261`).
- **Background trigger decoupled from flush** — `enqueue_due_compactions` from the
  1 s maintenance ticker (`db.rs:3200-3250, 8064-8071`), `l0_compaction_trigger=4`.
- **Probe attribution counters** — `FRS-A-SPLIT` n_ovl/n_ovl_l0 (`db.rs:373-441`).

### 3.2 The five write-side gaps vs RocksDB/ForSt (each = a design move in §4)

| # | Gap | forst-rs today | RocksDB/ForSt reference |
|---|---|---|---|
| W1 | L0→L1 input scope | ALL L0 + **ALL of the CF's L1**, unconditionally (`db.rs:5954-5977`) | overlap subset + clean-cut expansion (`compaction_picker.cc:218,443,485-537`) |
| W2 | Trivial move | absent (every byte rewritten; 0 grep hits) | `Compaction::IsTrivialMove` (`compaction.cc:519`), L0/non-L0 trivial-move pickers (`compaction_picker_level.cc:93-118`) |
| W3 | Compaction concurrency | engine-global `compaction_mutex` — ONE compaction at a time per DbImpl (`db.rs:5907-5933`, R44-H1); pool has cores/2 workers (`db.rs:10594-10610`) but they serialize | per-level-pair exclusivity via files-being-compacted sets + subcompactions |
| W4 | Pick policy | shallowest level over budget; rotating lowest-key src file (`db.rs:4244,4311-4313`) | highest `CompactionScore` first, compensated (tombstone-weighted) file sizes (`compaction_picker_level.cc:130,185`) |
| W5 | On-disk bytes | remote runner pins `FRS_SST_COMPRESSION=none` (q5z-run.sh ENVS); engine default is LZ4 (`config.rs:268`) | Flink ForSt/RocksDB: Snappy per level (`ForStConfigurableOptions.java:177-181`) |

Read-side residual: the resident-shadow tier is outside the level structure (per-probe
clone + bloom, `db.rs:6913-6920`) and is not counted by any trigger; and L0 slowdown/stop
40/64 (`write_controller.rs:93-95`) allows 2× RocksDB's run count (20/36,
`advanced_options.h:583,590`) before backpressure.

---

## 4. The design: leveled-compaction discipline

Principle: keep the existing invariant + locator (3.1); fix the write volume that
saturates the disk (3.2 W1-W5); then tighten the L0 budget the read side depends on.
One-pass architectural design; staged into commit-sized pieces in §6.

### M1 — Overlap-scoped, clean-cut L0→L1 picking (replaces whole-L1 input)

`compact_l0_for_cf` picks: all L0 files of the CF (unchanged — L0 files mutually overlap
under churn, and consuming all of L0 is what resets the run count), then **only the L1
files whose range overlaps the L0 union range**, expanded to a clean cut (forst-rs SSTs
are key-boundary split since FRS-LEVELED-COMPACTION, so "clean cut" = the overlap subset
is already boundary-aligned; the expansion loop is a safety check, not a rewrite source).
Output: split files at `target_file_size` (existing machinery). The level invariant is
preserved: output range == input union range, disjoint from the untouched L1 remainder.

- Effect bound (measured): at 1 GiB live the L1 remainder is the non-overlapped fraction
  of 256 MB; with q7's hash-bucket keys L0 usually spans the whole space so the win is
  modest **at L1** — the real win is that the SAME picker generalizes to Ln→Ln+1 (W4) and
  enables M2. With skewed/narrow L0 ranges (timer CFs, per-key-group CFs) the win is large.
- MVCC: unchanged — same `min_active_snapshot` plumbed into `CompactionJob`
  (`compaction.rs:93-99`); inputs are immutable; `version_set.apply` already validates
  inputs-still-present (`db.rs:5924-5932` comment).

### M2 — Trivial move

In `compact_level_for_cf`: if the picked src file's range overlaps ZERO next-level files
(binary search on the existing sorted level — same predicate the locator uses), emit a
VersionEdit that **re-levels the file meta** (delete-at-Ln + add-at-Ln+1 with identical
file number/size/range) — no I/O. Mirrors `Compaction::IsTrivialMove`. Preconditions
(RocksDB parity): no compaction filter active for the CF, no merge-operand consolidation
pending for that file (forst-rs files are CF-pure per R49-H1, so the per-CF merge
operator check is file-local).

- Effect bound (measured): every avoided rewrite saves `file_size` physical bytes ×2
  (read+write). Under uniform-hash q7 keys most descents DO overlap, so M2 alone is small
  for q7 — but it is the enabling primitive for M4's compaction-debt drain and free
  elsewhere. (Honest sizing: cell-A files_per_level shows L2 absorbing ~12 files/90 s;
  trivial-movable fraction at uniform keys ≈ 0. Labeled as **model**: wins appear for
  skewed CFs and during migration §5.)

### M3 — Concurrent compactions (per-level-pair exclusivity)

Replace the engine-global `compaction_mutex` serialization with RocksDB's model: a
`files_being_compacted` set on the Version (or per-CF level-pair locks): L0→L1 for CF X
may run concurrently with L2→L3 for CF X and with any compaction of CF Y. Flush stays
concurrent (already is — L0-add-only composes, see `FRS-COMPACT-RELEASE-LOCK` analysis
`db.rs:10612-10620`). Apply-side: `version_set.apply` already serializes and validates;
extend validation to reject edits whose inputs were consumed by a racing job (retry-pick).

- WHY this is load-bearing for q7: the write-amp bytes (7.6×) must move through ONE
  serialized stream today; q7 churns multiple CFs (two join sides + timers). Concurrency
  doesn't reduce bytes but removes the L0-backlog → fan-out coupling (cell C's regime)
  when a long Ln descent blocks the L0 rollup. The 2026-06-05 data point ("compact ≈
  cores/2 sweet spot", `db.rs:10602-10607`) shows the pool is there; the mutex wastes it.
- Risk: R44-H1 existed because two compactions on one Version produced overlapping L1
  outputs. The fix is input-set disjointness (level-pair + key-range exclusivity), not
  global serialization. Gate: the existing concurrent-compaction UTs + a new racing-pick
  property test (§7 G4).

### M4 — Score-based picking, tombstone compensation, and DYNAMIC level targets

Port three RocksDB picker ideas (`compaction_picker_level.cc:130-185`):
(a) score = level_bytes / target, L0 score = file_count / trigger, pick the HIGHEST score
(today: shallowest-over-budget → deep levels starve while L1 churns); (b) compensated
file size — weight delete tombstones (q7's TTL deletes; the bench workload is ~50 %
deletes by count at steady state) so tombstone-heavy files compact first and garbage
doesn't ride to the bottom level repeatedly; (c) **dynamic level targets**
(`level_compaction_dynamic_level_bytes = true` default since RocksDB 8.x,
`advanced_options.h:691`): anchor targets at the LAST non-empty level's actual size and
derive upward (`target(Ln-1) = size(Ln)/mult`), so small/medium state skips the
L1→L2→…→Ln cascade entirely. **This is the single largest measured contributor to the
E-cell gap**: at identical workload RocksDB's layout is `[L0, –, –, –, –, L5, L6]` —
data takes ONE hop to its resting level — while forst-rs cascades through fixed
256 MB-base targets (`db.rs:4244-4261`, `config.rs:260`), paying a rewrite per level.
Implementation: target computation swap in `pick_compaction_level_for_cf` + rollup
output-level selection (smallest change with the largest write-amp effect).

### M5 — L0 trigger discipline 20/36 + on-disk compression parity (config, gated)

Cell B proves 20/36 costs nothing when compaction keeps up; the 2026-05-30 trigger=4
regression was the inline-compaction coupling, removed by FRS-COMPACT-BG. AFTER M1+M3
land, flip defaults `l0_slowdown_trigger 40→20`, `l0_stop_trigger 64→36`
(`write_controller.rs:94-95`). Separately (independent, remote-runner config): stop
pinning `FRS_SST_COMPRESSION=none` on the remote — engine default LZ4 (`config.rs:268`);
RocksDB/ForSt cells run Snappy. NexMark rows compress 2-3×: this is a direct divisor on
the 435-682 MB/s write stream AND on read bytes. (The bench's incompressible values make
§2's 7.6× a compression-independent floor; production q7 bytes shrink further.)

### M6 — Shadow/resident tier inside the run budget

The resident-flushed shadow (`db.rs:6913-6920, 7884-7904`) serves reads from RAM but is
invisible to the L0 trigger math, and its per-probe cost (bloom+clone) is paid even when
the data is also in L0 (it IS L0's content, kept resident). Design: count resident-shadow
entries-as-runs into the L0 score (M4) so the trigger reflects true probe fan-out, and
prune the shadow on rollup completion (already done — `prune_resident_flushed`,
`db.rs:7985`). No structural change; bookkeeping only.

### Interaction with shipped levers

- **Prefetcher windows** (`sst/prefetch.rs`): assume intra-file contiguity — M1/M2 don't
  change file contents; M3 increases concurrent SST creation, same as today's
  flush+compact overlap. No interaction.
- **S2 loser-tree + pinned rows**: benefits from bounded fan-in (heap width = run count);
  M5's tighter L0 directly shrinks S2's heap. Compose, don't conflict.
- **Compaction-windowed reads (L4)**: M3 raises concurrent compaction read pressure —
  L4's windowed/double-buffered reads (`runtime_tuning.rs:115-150`) become MORE valuable;
  keep gated on iostat share as before.
- **io_uring**: reduces per-stall cost; discipline reduces stall count. Independent axes.
- **Block-cache bypass fix (27ae792c3)**: compaction outputs pre-populate readers WITH
  cache; M3's higher compaction concurrency multiplies the value of that fix.

---

## 5. Migration & safety

- **Restart with existing tiered data**: none needed — the on-disk format and the level
  invariant are unchanged (L1+ is already non-overlapping; M1-M4 change WHICH inputs are
  picked and WHEN, not the file format or the invariant). Old DBs compact under the new
  picker organically. A one-time `compact_all` is available but NOT required (cell D shows
  sweeps add no read benefit when the invariant already holds).
- **MVCC/snapshot safety**: all moves preserve the existing rules — `min_active_snapshot`
  pins versions (`compaction.rs:93-99`, `mvcc::should_drop`); trivial move (M2) moves
  whole files so per-key version ordering across files is untouched (key ranges disjoint
  by the level invariant); M3's concurrent applies serialize through `version_set.apply`'s
  existing apply-lock + input validation.
- **Failure modes**: M3 racing picks → apply-side reject + repick (idempotent; file
  numbers are allocate-once). M2 re-level edit crash-mid-apply → manifest replay sees
  either old or new level, both valid (same file). M1 narrower inputs → MORE frequent,
  smaller rollups; the maintenance ticker dedup (`enqueue_compaction` per-CF dedup,
  `db.rs:3243-3250`) already rate-limits.

---

## 6. Staged implementation (commit-sized)

1. **S1 (M1)**: overlap-subset + clean-cut L0→L1 picking. ~150 LoC in `compact_l0_for_cf`
   + UTs (invariant property test: post-compaction level disjointness). Falsifier: B-cell
   write-amp must drop measurably (§7 G1) — if not, W1 wasn't binding at bench scale; keep
   for skewed CFs, proceed.
2. **S2 (M2)**: trivial move in `compact_level_for_cf` + manifest round-trip UT.
3. **S3 (M4)**: score-based pick + tombstone compensation (pure picker change, UT-able
   against synthetic Versions).
4. **S4 (M3)**: per-level-pair concurrency (the only structurally risky stage; lands
   behind `FRS_COMPACT_CONCURRENT=1` default-OFF until G4 passes ×5).
5. **S5 (M5/M6)**: default flips (20/36, LZ4-on-remote, shadow-in-score) — config-only,
   each its own commit with its own remote A/B.

---

## 7. Gates (falsifiers, from §2 cells + remote A/B)

- **G1 (microbench, after S1)**: churn_probe cell A write-amp median < 6.0 (from 7.68)
  with probe p50 late ≤ 700 µs unchanged. Fail ⇒ W1 not binding at this scale; re-rank.
- **G2 (microbench, after S3)**: cell C variant with compaction ON at 2× write-rate
  (starvation regime): L0 stays < 20 and p50 late < 1 500 µs. Fail ⇒ picker not the
  starvation fix; look at compaction ns/byte (B3 bench) instead.
- **G3 (microbench, after S4)**: same starvation cell, `FRS_COMPACT_CONCURRENT=1` vs 0:
  L0 backlog duration strictly shorter, no correctness diff in a randomized
  read-while-compact property test ×5.
- **G4 (correctness)**: existing engine suite + new racing-pick property test green ×5;
  q5/q8 exact-output remote checks (the q5z correctness harness) before any default flip.
- **G5 (remote, the binding one)**: q7 A/B same-session: baseline vs S1-S5 stack vs
  S1-S5+`FRS_SST_COMPRESSION=lz4`. Targets traced from §1/§2: write volume share of disk
  must drop ~proportionally to write-amp reduction; q7 wall target ≤ 1 367.6 s (RocksDB
  bar) — ForSt-remote landed 1379.7 s ≈ RDB, so 1367.6 IS the de-facto q7 bar on this box. Secondary: q9/q20 same-session A/B (same churn class, memory:
  q9 2904.6 R3 / q20 2201.6 R3 contended references — re-baseline first).
- **G6 (no-regression)**: q4 (write-heavy, compaction-sensitive) and q17 must not regress
  > 5 % same-session; the 2026-06-05 "compact=cores/2" sweet spot re-validated after S4.

## 8. Expected wins (traced, honest)

- Write volume: 7.6× → modeled 4-5× from M1+M4 at bench scale (G1 measures); ÷2-3 more
  from LZ4 on compressible production bytes (M5; bench floor is compression-independent).
  On the remote disk-saturated regime, write MB/s is the binding resource ⇒ wall-time
  improvement is ~proportional once the disk leaves saturation (iostat util < 90 %).
- q7: 2 376 s → **~1 400-1 700 s** (leave-saturation model + L0-depth probe relief; G5
  decides). The PART-A pin reset the target: ForSt-remote = **1 379.7 s ≈ RocksDB-remote
  1 367.6 s**, so the remote bar is ~1 370-1 380 and the Mac 2.46× ForSt ratio does NOT
  transfer (Mac page-cache/disk regime). H1 levers are sized to REACH the bar; going
  below it is H2 (depth-1 executor) + H5 (engine constant) territory, per the
  architecture doc's ranking.
- q9/q20: same churn class, smaller probe component ⇒ expect 15-30 % wall from
  de-saturation alone (G5 secondary).

---

## Appendix A — evidence (raw)

- Microbench outputs: `/tmp/churn_probe_results/{A-default,B-tight,C-nocompact,D-leveled-emu,E-rocksdb}.jsonl`
  (+ `.stderr` DECAY_ATTR lines). Bench source committed at
  `crates/forst-rs-bench/src/bin/churn_probe.rs`. Cells A-D median lines:

```
MEDIANS label=default     write_amp=7.68 p50_late_us=649  p99_late_us=2205 p50_degradation=3.57x  last_l0=1  (n=3)
MEDIANS label=tight       write_amp=7.54 p50_late_us=688  p99_late_us=2213 p50_degradation=3.62x  last_l0=0  (n=3)
MEDIANS label=nocompact   write_amp=0.97 p50_late_us=2997 p99_late_us=3498 p50_degradation=10.94x last_l0=41 (n=3)
MEDIANS label=leveled-emu write_amp=7.76 p50_late_us=693 p99_late_us=2146 p50_degradation=3.38x last_l0=0 (n=3)
MEDIANS label=rocksdb     write_amp=3.91 p50_late_us=162  p99_late_us=279  p50_degradation=3.65x  last_l0=2  (n=3)
```

- Remote q7 iostat (frs, mid-run, Q7P32 capture): nvme2n1 98-99 % util, reads 190-292 MB/s,
  writes 435-682 MB/s, await ≤ 38 ms, queue 96-230; box ~18 % idle / 8-10 % iowait.
- ForSt q7 pin log: `/ssd2/jackylee/frs-bench/logs/q7forst.log` (+ `/tmp/q7forst-q7-forst-local.out` on the box).
- C++ anchors (via `git show main:` in this repo): `db/version_set.cc:939` (LevelIterator),
  `db/compaction/compaction.cc:519` (IsTrivialMove), `db/compaction/compaction_picker.cc:218,443,485-537`
  (clean-cut/overlap picking), `db/compaction/compaction_picker_level.cc:93-185` (score pick,
  L0/non-L0 trivial-move), `include/rocksdb/advanced_options.h:583,590` (20/36).
- forst-rs anchors: `crates/forst-rs-engine/src/db.rs:5903-6010` (whole-L1 rollup),
  `:4244-4330` (level pick), `:5907-5933` (global mutex), `:10594-10610` (pool),
  `crates/forst-rs-engine/src/write_controller.rs:93-95` (40/64),
  `crates/forst-rs-storage/src/version/mod.rs:698-870` (locator),
  `crates/forst-rs-common/src/config.rs:257-268` (geometry + LZ4 default).
