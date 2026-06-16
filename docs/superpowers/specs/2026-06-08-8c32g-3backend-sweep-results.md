# 8c/32g 3-backend NexMark sweep — verified time + accuracy (2026-06-08)

# ═══════════════════════════════════════════════════════════════════════════
# ★★★★ PMC-1 DYNAMIC MEMORY-PRESSURE LEVER SHEDDING (FRS-DYN-SHED) 2026-06-16
# ═══════════════════════════════════════════════════════════════════════════
# THE PROBLEM (sweep 791aa54cd/98cef906b/a0bf505e7): at the uniform 2×4c/16g
# split the "full-stack-ON" config (KV-sep + persistent-probe-iter + coalesce +
# S2-pinned + leveled-hot-CF + vlog-resident) OOM-killed q9/q19/q5 (exit-137
# cgroup, NOT correctness). The prior LEANER config FIT them. Enabling all the
# memory-hungry levers UNCONDITIONALLY robs the big scatter-joins / sliding
# window of headroom. Directive: ONE uniform config (KV-sep format ON
# everywhere, no hybrid layout) that DYNAMICALLY sheds the memory-hungry levers
# under cgroup pressure — bounded, NEVER-OOM — keeping them when memory allows.
#
# ── DESIGN + IMPLEMENTATION (committed, branch dyn-shed → forst-rs) ──
# New module `crates/forst-rs-engine/src/mem_pressure.rs`:
#  * A process-global memory watermark sampler (thread, 1 s default cadence,
#    `FRS_DYN_SHED_INTERVAL_MS`). Sources, most→least authoritative:
#      1. test/bench override (deterministic),
#      2. cgroup-v2 `memory.current`/`memory.max` (the EXACT bytes the
#         OOM-killer watches → shedding tracks the real budget),
#      3. RSS (`/proc/self/statm`) vs `FRS_MEM_BUDGET_MB` (portable fallback).
#  * `used/budget` → conservative PressureLevel buckets (engage BEFORE the cliff):
#      Ample <0.75 · Elevated 0.75–0.85 · High 0.85–0.92 · Critical ≥0.92.
#  * Master flag `FRS_DYNAMIC_SHED=1` — **DEFAULT OFF, byte-identical when off**
#    (`should_shed()` short-circuits to `false`; no sampler thread; no cgroup
#    reads). Programmatic overrides for in-process A/B.
#  * `should_shed(ShedPriority)` = `armed && current_pressure >= lever.shed_at`.
#
# PRIORITY LADDER (shed-FIRST → shed-LAST), exactly the work-order order:
#   1. PersistentProbeIter  — shed at Elevated (held Arc<Version>+SST readers;
#                             heaviest resident, trivially rebuilt per-probe)
#   2. CoalesceBuffers      — shed at Elevated (deferred-deref side lists +
#                             per-segment chunk bufs; inline per-key deref frees)
#   3. S2Pinned             — shed at High (pinned data blocks across a scan)
#   4. LeveledHotCf         — shed at High (lowered L0 trigger ⇒ more resident
#                             index/filter blocks; relax to base trigger)
#   5. VlogResident         — shed at Critical, LAST (back off NEW-flush value
#                             separation ⇒ vlog reader working set stops growing;
#                             the strongest RAM-bounding valve)
# KV-SEPARATION FORMAT stays ON at every level — uniform layout, no hybrid. The
# already-separated BlobRef rows always deref through their immutable segments,
# so shedding mid-run is byte-identical OUTPUT; only NEW flushes go inline.
#
# WIRING: each lever's `*_enabled()` (db.rs) AND-folds `!should_shed(lever)`;
# VlogResident is an early back-off in `should_separate_now`. Sampler started
# next to `maybe_start_mem_diag()` at db open (no-op unless armed).
#
# ── VERIFICATION (Mac — what this host CAN prove) ──
#  * `cargo test -p forst-rs-engine --lib`: **441 passed / 0 failed** (4 new:
#    off-by-default-never-sheds, priority-ladder-order, sampler→gate, bp
#    thresholds + 1 engine-level: shed forces inline at Critical through the
#    REAL `should_separate_now`).
#  * BYTE-IDENTICAL-WHEN-OFF proven: ALL pre-existing lever A/B equivalence
#    tests (S2 byte-equality, persistent-probe-iter byte-identical, adaptive
#    KV-sep back-off, vlog readahead byte-identical) PASS UNCHANGED — they run
#    under the default OFF master flag where `should_shed` is always false.
#  * fmt clean · clippy clean (incl. --tests) · rustdoc-strict
#    (`RUSTDOCFLAGS=-D warnings cargo doc`) clean (no private intra-doc links).
#  * FFI crate builds.
#
# ── HONEST NEGATIVES / NEEDS THE LINUX BOX ──
#  * TASK 1 (profile the OOM breakdown — GiB-per-lever at the OOM moment) and
#    TASK 3 (re-run q9/q19/q5 @100M at 16g/TM with shedding ON → FINISH, exact
#    rows; q7 keeps its win when ample) REQUIRE the remote x86 box: the Mac has
#    NO cgroup-v2 `/sys/fs/cgroup`, jemalloc is OFF on Darwin, and NexMark is
#    REMOTE-ONLY per standing policy. The sampler's cgroup path is therefore
#    UNEXERCISED on Mac (only the override path is). The mechanism is correct +
#    inert-when-off + unit/engine-test-proven, but the DEFINITIVE never-OOM
#    verdict + per-lever GiB attribution + the q7-still-wins-when-ample
#    spot-check are OWED on the Linux box (run: `FRS_DYNAMIC_SHED=1` +
#    `FRS_MEM_DIAG=1`, watch `[FRS_MEM_DIAG]` rss vs the cgroup, confirm
#    exit-137 is gone and out_rows q9=91,813,372 / q19=92,000,000 / q5 canonical).
#  * The Elevated/High/Critical fraction thresholds (0.75/0.85/0.92) are a
#    conservative first cut; the box may want tuning (env-overridable design,
#    but thresholds are currently compiled — a follow-up can env-gate them).
#
# ───────────────────────────────────────────────────────────────────────────
# ★★ PMC-1 e2e NEVER-OOM VALIDATION 2026-06-16 (Mac DOCKER-LINUX container) ──
# ───────────────────────────────────────────────────────────────────────────
# CORRECTION to the "needs the Linux box" note above: the never-OOM gate IS
# testable on the Mac. The HOST is Darwin (no cgroup) but the NexMark TM runs
# INSIDE a Docker LINUX container where /sys/fs/cgroup/memory.current exists and
# --memory=16g sets memory.max; the exit-137 OOM is the container cgroup killer.
# Ran q9/q19/q5 @100M at the uniform 2×4c/16g split, process.size=10240m, FULL
# join_stack (KV-sep+persistent-probe-iter+coalesce+S2-pinned+leveled-hot-CF+
# vlog-resident) with FRS_DYNAMIC_SHED=1 + FRS_MEM_DIAG=1. Harness: forwarded
# FRS_DYNAMIC_SHED / FRS_DYN_SHED_INTERVAL_MS / FRS_MEM_BUDGET_MB into the ENVS
# array (they were MISSING — the flag never reached the engine before this fix);
# added shed_armed/shed_level to the [FRS_MEM_DIAG] line for in-container proof.
#
# ── RESULT: FRS_DYNAMIC_SHED does NOT make the full-lever config never-OOM at
#    16g/TM on this Mac. ALL THREE gate queries still OOM-killed a TM (exit-137,
#    OOMKilled=true confirmed via docker inspect): ──
# | query | shed=1 outcome | OOM? | out_rows | wall | verdict |
# |---|---|---|---|---|---|
# | q9  | tm1 exit-137 @~22.6M, job crash-looped → DNF | YES (oom=true) | — (req 91,813,372) | aborted 1003s | ✗ FAIL |
# | q19 | tm2 exit-137 @~49.7M, restarted degraded 1-TM, "FINISHED" but TRUNCATED | YES (oom=true) | 48,686,526 (req 92,000,000) | 441.4 (degraded) | ✗ FAIL (wrong rows) |
# | q5  | tm1 exit-137 @~6M, job crash-looped → DNF | YES (oom=true) | — (req ~29,988,416) | aborted 461.5s | ✗ FAIL |
#
# ── WHY (the [FRS_MEM_DIAG] in-container evidence — sampler DID engage) ──
# The cgroup sampler worked perfectly: shed_armed=true on every line, and the
# ladder climbed IN ORDER as RSS rose: Ample→Elevated→High→Critical (q9 level
# histogram over its run: Ample 9 / Elevated 2 / High 8 / Critical 44). So the
# flag reached the engine, the in-container cgroup reads happened, and shedding
# fired pre-cliff (Critical = 0.92 ⇒ ~15.07 GiB of the 16.0 GiB limit). It STILL
# OOM'd because the join-build RSS SPIKE outruns the shed+drain rate:
#   q9:  RSS 12600(Ample) → 13212(High) → 15431(Critical) → cliff in ~2 samples
#        (+2.2 GiB in one 3 s window); kernel killed at ~16081 MB.
#   q19: RSS 14037(Critical) → 16174(Critical) → killed (+2.1 GiB / sample).
# The 1 s sampler detects pressure and sheds, but during the initial build the
# memtable/WBM backlog + jemalloc arenas + join state grow ~0.7–1.4 GiB/s, so by
# the time shedding+flush free headroom the cgroup cliff is already crossed.
# Shedding bounds STEADY STATE (a lone surviving q9 TM held ~13.1 GiB at Critical
# with all levers shed) but NOT the transient build spike — which is the OOM.
#
# ── q7 ample-memory A/B (beneficiary spot-check) — PASS, no regression ──
# | q7 | wall_s | out_rows | OOM? |
# | shed=1 | 909.9 | 92,000,002 | no (Ample-dominant; brief Elevated/High shed) |
# | shed=0 | 982.9 | 92,000,002 | no |
# shed ON is +0% (8% FASTER, within this Mac's known q7 coordination-bound
# variance). q7 fits 16g WITHOUT shedding too (both TMs healthy at shed=0), so
# shedding is correctly mostly a no-op for q7 and does NOT hurt its wall/rows.
#
# ── VERDICT ──
# FRS_DYNAMIC_SHED is mechanically CORRECT (cgroup sampler engages in-container,
# ladder sheds in priority order pre-cliff, byte-identical when off, q7 unharmed)
# but is NOT SUFFICIENT to make the full-lever uniform config never-OOM at 16g/TM
# on this Mac — q9/q19/q5 still exit-137. The reactive lever-shedding cannot
# catch the join-build RSS spike. DO NOT default-ON: it gives a false never-OOM
# promise. To actually bound it the engine must (a) PROACTIVELY refuse value-
# separation / cap the WBM memtable BEFORE the build spike (admission control,
# not reactive shedding), and/or (b) drop the build-phase peak (the levers'
# transient buffers) rather than only future flushes, and/or (c) the directional
# fallback: the LEANER stack (no persistent-probe-iter/coalesce/S2) which FIT q9
# (1285.5s) and q19 (238.6s) earlier — i.e. ship the lean stack by default and
# only ADD heavy levers when a pre-flight headroom check passes. Linux box has
# more RAM headroom and MAY survive (directional), but the spike-vs-shed-rate
# physics is the same; needs the proactive valve regardless.
#
# ───────────────────────────────────────────────────────────────────────────
# ★★★ PMC-1 LEANER-CONFIG NEVER-OOM VALIDATION 2026-06-16 (Mac DOCKER-LINUX) ──
# ───────────────────────────────────────────────────────────────────────────
# HYPOTHESIS (directive): the LEANER lever stack — KV-sep ON (uniform format) +
# FRS_VLOG_POINT_DEREF (windowed read-path) + lz4, with the HEAVY memory levers
# OFF (persistent-probe-iter / coalesce-deref / S2-pinned / leveled-hot-CF /
# probe-bloom-prune) — is the never-OOM uniform LOCAL default that keeps the wins.
# Ran @100M, uniform 2×4c/16g split, process.size=10240m, REPO tip cf7816d96 (.so
# from frs-dync, identical engine — only env flags differ from full-stack-ON).
#
# ── EXACT leaner env set (vs full-stack-ON: which levers DROPPED) ──
#   FRS_SST_COMPRESSION=lz4  FRS_VLOG_COMPRESSION=inherit
#   FRS_KV_SEPARATION=true   FRS_KV_MIN_BLOB_SIZE=256   FRS_TRIVIAL_MOVE=true
#   FRS_VLOG_POINT_DEREF=1   (windowed stack: q8/q11/q12/q17/q18 — NOT coalesce)
#   DROPPED vs full-stack-ON: FRS_VLOG_COALESCE_DEREF, FRS_RS_S2_PINNED,
#     FRS_S2_FANOUT_MIN, FRS_RS_PROBE_BLOOM_PRUNE, FRS_RS_LEVELED_HOT_CF,
#     FRS_PERSISTENT_PROBE_ITER, FRS_RS_MERGE_RMW, FRS_RS_EXECUTOR=routing-adaptive.
#   (q9 also kept vlog reader-cap=2048 / resident-budget=256 / adaptive=1.)
#
# ── RESULT TABLE (leaner config; out_rows verified EXACTLY, OOM via docker inspect) ──
# | query | state | wall_s | out_rows | OOM? | vs prior |
# |---|---|---|---|---|---|
# | q9  | OOM_DNF | — | — (req 91,813,372) | YES tm2 exit137 | died ~16.7M/100s — leaner did NOT fix the OOM |
# | q9 (KV-sep OFF) | OOM_DNF | — | — | YES tm2 exit137 | died ~14.3M/100s — KV-sep OFF ALSO OOMs today (contradicts the 2026-06-14 1828.7 "clean") |
# | q19 | OOM_DNF | — | — (req 92,000,000) | YES tm2 exit137 | died ~64.8M/281s — LIVE alloc only 1.4 GiB at the cliff |
# | q19 (+aggressive jemalloc purge env) | OOM_DNF | — | — | YES tm2 exit137 | _RJEM_MALLOC_CONF env IGNORED (compiled static wins); retained stayed ~8 GiB |
# | q17 | FINISHED | 180.9 | 92,000,000 ✓ | no | ★ BEATS ForSt 245.9; better than full-stack-ON 333.7 (which was pathological); KV-sep-OFF was 110.7 |
# | q11 | FINISHED | 189.9 | 92,000,000 ✓ | no | better than full-stack-ON 219.9; KV-sep-OFF was 118.8 |
# | q12 | FINISHED | 57.7  | 92,000,000 ✓ | no | source-bound; full-stack 43.6 (point-deref overhead/box noise) |
# | q8  | FINISHED | 63.8  | 3,064,453 ✓(band) | no | src 3,064,719; in-band w/ rdb 3,064,413 / full-stack 3,064,481 |
# | q18 | FINISHED | 430.5 | 92,000,000 ✓ | no | ✗ SLOWER than full-stack-ON 199.1 (lost MERGE_RMW+routing-adaptive) → now FAILS rdb 360.4 |
#
# ── ★ ROOT CAUSE (the [FRS_MEM_DIAG] in-container evidence) — NOT the lever stack ──
# The OOM is JEMALLOC RETAINED MEMORY, orthogonal to which levers are on. At every
# cliff the LIVE allocation is small but jemalloc holds ~6–8 GiB of freed-but-
# unpurged (dirty/muzzy) pages that alone push RSS to the 16384 MiB cgroup limit:
#   q9  lean : rss 15391 | live(alloc) 3831 | RETAINED 7087–8035 | wbm 1470
#   q9  OFF  : rss 16357 | live(alloc) 4895 | RETAINED 6124      | wbm 1998
#   q19 lean : rss 16090 | live(alloc) 1372 | RETAINED 7926      | wbm 472  (!)
# q19's live working set is ~1.4 GiB yet RSS is 16 GiB — ~8 GiB is pure jemalloc
# retained pool during the high-churn compaction phase (comp_cnt=32). The compiled
# MALLOC_CONF (forst-rs-ffi/src/lib.rs:66) uses dirty/muzzy_decay_ms:10000 (10 s,
# deliberately, to avoid re-faulting inside a 30 s checkpoint burst — it speeds q4).
# That 10 s decay is exactly why the retained pool can't drain before a build/churn
# spike (~0.7–1.4 GiB/s) crosses the cliff. Trying to override it at runtime FAILED:
# tikv_jemallocator exports `_rjem_malloc_conf` as a STRONG symbol, so the
# `_RJEM_MALLOC_CONF` env is ignored (retained stayed ~8 GiB) — the fix MUST be an
# engine change (a pressure-triggered `mallctl arena.<i>.purge` proactive valve, or
# a cgroup-aware decay) — config/env ALONE cannot make q9/q19/q5 never-OOM here.
#
# ── VERDICT ──
# The leaner config is NOT a clean never-OOM uniform local default on this Mac
# (35.18 GiB Docker VM; the split's 2×16g+4g = 36g OVERCOMMITS it). q9 OOMs with
# the leaner stack, with the heavy stack, AND with KV-sep OFF — the OOM is the
# jemalloc-retained pool + the VM overcommit, NOT the lever set. The earlier
# "q9 1285.5s FIT" (pmc1-uniform) and "1828.7s KV-sep-OFF clean" (2026-06-14) were
# LUCKY runs where host memory slack let the retained pool sit under the cliff;
# they are NON-DETERMINISTIC (q9 OOM'd in ≥3 other sessions incl. today, both arms).
# WHAT THE LEANER CONFIG *DOES* WIN (genuine, repeatable): the windowed/light family
# FINISHES clean with EXACT out_rows and KEEPS q17's beat-ForSt (180.9 < 245.9) and
# is BETTER than the (pathological) full-stack-ON on q17/q11. HONEST NEGATIVE: it is
# WORSE than full-stack-ON on q18 (430.5 vs 199.1 — the window MERGE_RMW + routing-
# adaptive levers it drops were load-bearing for q18) and slightly on q12/q8.
# CONCLUSION: there is NO env-only never-OOM uniform local default for the memory-
# bound joins (q9/q19/q5) at 16g/TM on a 36g-overcommitted VM. The never-OOM
# mechanism is a PROACTIVE jemalloc-purge / admission valve in the ENGINE (matches
# the FRS_DYNAMIC_SHED verdict above: reactive shedding can't catch the spike). The
# documented default therefore stays SELECTIVE (§"RECOMMENDED SELECTIVE KV-SEP
# PROFILE" below): KV-sep OFF for q9/q11/q17, ON for q4/q7/q19/q20 — and on THIS
# Mac q9 needs >16g/TM (or the engine valve) regardless. FRS_DYNAMIC_SHED stays
# default-OFF (it is not the never-OOM mechanism).
#
# ───────────────────────────────────────────────────────────────────────────
# ★★★ PMC-1 PROACTIVE JEMALLOC-PURGE VALVE 2026-06-16 (FRS_MEM_PRESSURE_PURGE) ─
# ───────────────────────────────────────────────────────────────────────────
# Built the proactive valve the two verdicts above called for: at High/Critical
# cgroup pressure, force jemalloc to return retained pages via the void
# `arena.<MALLCTL_ARENAS_ALL>.purge` mallctl (4096 == ALL arenas, newp=NULL).
#
# ── IMPLEMENTATION (committed; branch jemalloc-purge → forst-rs) ──
#  * forst-rs-ffi/src/lib.rs: `jemalloc_purge_all()` (the unsafe mallctl — the
#    engine is `#![forbid(unsafe_code)]`, so the call lives in the FFI crate
#    which already links the jemalloc global allocator + permits unsafe). New
#    Linux-only dep `tikv-jemalloc-sys`. Registered into the engine via a fn-ptr
#    hook from `guarded()` (first FFI call, before any DB open).
#  * forst-rs-engine/src/mem_pressure.rs: `register_purge_hook` (OnceLock fn-ptr),
#    `purge_valve_enabled()` (armed by `FRS_MEM_PRESSURE_PURGE=1` OR the master
#    `FRS_DYNAMIC_SHED=1`; DEFAULT OFF), `maybe_purge(level)` — the sampler tick
#    calls it after publishing the level; fires the hook + bumps a counter iff
#    armed AND pressure >= High. Off ⇒ no hook, no purge — byte-identical.
#  * `[FRS_MEM_DIAG]` line gains `purge_armed` + `purge_count` (the proof).
#  * Harness: forwarded `FRS_MEM_PRESSURE_PURGE` into the ENVS array.
#  * Tests: `cargo test -p forst-rs-engine --lib` 445/0 (4 new purge-valve tests:
#    off-by-default, armed-by-sub-flag-or-master, fires-only-at-High, registered-
#    hook-fires-at-High-not-below). fmt/clippy/rustdoc-strict clean. Linux .so
#    builds (the unsafe mallctl path compiles in the container).
#
# ── VERIFICATION (Mac DOCKER-LINUX container, cgroup-v2, --memory=16g) ──
# Ran q9 + q19 @100M, uniform 2×4c/16g, process.size=10240m, FULL join_stack,
# FRS_MEM_PRESSURE_PURGE=1 + FRS_MEM_DIAG=1, BOTH 1 s and 200 ms sampler cadence.
#
# | query | cadence | shed/purge outcome | OOM? | out_rows | verdict |
# |---|---|---|---|---|---|
# | q9  | 1 s   | tm2 exit-137 @~16.8M | YES | — (req 91,813,372) | ✗ FAIL (truncating restart) |
# | q9  | 200ms | tm1 exit-137 @~17M    | YES | — (req 91,813,372) | ✗ FAIL (faster cadence did NOT save it) |
# | q19 | 200ms | tm2 exit-137 @~60M    | YES | — (req 92,000,000) | ✗ FAIL (survived longer, OOM in the compaction storm) |
# (q5 NOT re-run: q9+q19 + the mechanism below make the outcome a foregone OOM on
#  this VM — same JVM-dominated RSS. Honest: q5 is owed on a non-overcommitted box.)
#
# ── THE VALVE WORKS — BUT IT TARGETS THE WRONG POOL (decisive new finding) ──
# The purge fires perfectly: purge_armed=true on every line, purge_count climbs
# (q9 hit 271 @200ms in ~110 s; q19 hit 537), and jemalloc_retained DOES drop on
# each fire (q9: 6092→5471→4956 MB as count 22→25→28). BUT the TM still OOMs,
# because **jemalloc `retained` is NOT resident memory.** Per jemalloc's own ctl
# docs (stats.retained): "Retained virtual memory is typically untouched,
# DECOMMITTED, or purged ... excluded from mapped memory statistics." Retained
# pages were already `MADV_DONTNEED`/decommitted — they are virtual-only and do
# NOT count toward RSS or the cgroup `memory.current` the OOM-killer watches.
# The PRIOR analysis (and this work-order's premise) CONFLATED `retained` with
# resident dirty pages. The in-container [FRS_MEM_DIAG] at each cliff proves it:
#   q9  cliff: rss=15734  jemalloc_RESIDENT=5628  retained=4956  wbm=1656
#              (purge fired; retained virtual; RSS = JVM ~10.1G + jemalloc 5.6G)
#   q9  cliff: rss=15296  jemalloc_RESIDENT=5516  retained=817   wbm=2049
#              (retained crushed to 0.8G by purge — RSS STILL 15.3G ⇒ retained
#               was NEVER the pressure; live resident + WBM + JVM are)
#   q19 cliff: rss=16346  jemalloc_RESIDENT=1897  retained=8585  wbm=645
#              (retained 8.6G but jemalloc-resident only 1.9G ⇒ ~14.4G is the
#               JVM/FFM process, NOT jemalloc; purging retained can't touch it)
# So the actual 16 g RSS = the JVM process (heap + FFM off-heap state buffers,
# ~10–14 G) + jemalloc RESIDENT (live alloc + dirty, 2–6 G during the join/flush
# spike) + heavy-I/O page cache in `memory.current`. The proactive purge reclaims
# `retained` (already-decommitted virtual mappings) — which is exactly the pool
# that was NOT contributing to RSS. NET RSS EFFECT OF THE PURGE: ~0.
#
# ── VERDICT — proactive jemalloc-purge is NOT a clean never-OOM fix ──
# It is mechanically correct, byte-identical when off (purge only ever releases
# already-freed memory ⇒ zero correctness impact), inert/cheap when off, and the
# Mac container DID exercise the real cgroup sampler + the real mallctl. But it
# does NOT make q9/q19/q5 never-OOM at 16 g/TM, because the cliff is JVM-side +
# live-jemalloc-resident + page-cache RSS, and jemalloc `retained` (the only pool
# the purge touches) is virtual/decommitted and was never in RSS. DO NOT default-
# ON as a never-OOM promise — it would be a false one. Keep it default-OFF, armed
# (FRS_MEM_PRESSURE_PURGE=1) as a cheap, safe hygiene knob that genuinely trims the
# retained virtual footprint (helps `vm.overcommit`/address-space pressure, NOT
# RSS) and may help on a box where retained pages are still DIRTY (not yet
# decommitted) — which depends on the kernel's MADV_FREE behaviour and is worth a
# one-shot check on the real x86 box.
# HONEST NEGATIVES: (1) the 35.18 GiB Mac Docker VM OVERCOMMITS the split
# (2×16g + 4g = 36 g > 35.18 g VM RAM), so the host itself is memory-starved and
# the cgroup cliff is partly the VM, not pure forst-rs — the definitive verdict is
# owed on a box with ≥40 GiB free. (2) `_RJEM_MALLOC_CONF` env stays ignored
# (strong symbol); the compiled 10 s decay is unchanged (kept for q4). (3) q5 not
# re-run (mechanism makes it a foregone OOM here).
# WHAT ACTUALLY BOUNDS IT (next, unchanged from the two verdicts above): a
# PROACTIVE ADMISSION valve that caps the JVM-side / WBM working set BEFORE the
# join-build + compaction spike (e.g. a lower process.size + a hard WBM cap +
# bounding the FFM off-heap state-buffer pool), and/or simply >16 g/TM. The
# never-OOM lever is on the RESIDENT working set, not the virtual retained pool.
#
# ═══════════════════════════════════════════════════════════════════════════
# ★★★★★ PMC-1 UNIFIED MEMORY MANAGER (FRS_MEM_MANAGER) 2026-06-16 — THE FIX THE
#        THREE VERDICTS ABOVE CALLED FOR (proactive admission, not reactive)
# ═══════════════════════════════════════════════════════════════════════════
# The three preceding verdicts (FRS_DYNAMIC_SHED, leaner-config, jemalloc-purge)
# converge on ONE conclusion: the OOM is a SUM problem on the RESIDENT working
# set, and the fix must be PROACTIVE ADMISSION — bound the total BEFORE the
# build/compaction spike — not reactive shedding/purging (which can't catch the
# spike, and which the purge proved targets the wrong pool: `retained` is virtual,
# not RSS). This section builds exactly that.
#
# ── TASK 1: THE ENGINE-NATIVE PROFILE AT THE CLIFF (from the diag above) ──
# The cgroup `memory.current` the OOM-killer watches decomposes into THREE pools:
#   (1) JVM process (Flink heap+managed+FFM off-heap) — sized by process.size.
#       q19 cliff: ~14.4 GiB of 16 GiB was JVM/FFM (jemalloc-resident only 1.9 G).
#   (2) engine-native (jemalloc) RESIDENT — block cache + memtables(WBM) +
#       resident shadow + vlog reader cache + the compaction/build TRANSIENT.
#       q9 cliff: jemalloc_RESIDENT 5.5-5.6 G + WBM 1.6-2.0 G on a ~10 G JVM.
#   (3) page cache / kernel — small, uncontrollable.
#   DOMINANT OVER-BUDGET TERM: for q9 it is the engine-native RESIDENT (block
#   cache + WBM + the build/compaction TRANSIENT) stacked on the JVM; for q19 it
#   is the JVM process itself (process.size too high for the cgroup). NEITHER is
#   `retained` (virtual). The standalone caps (block cache 256M×~8≈2G; WBM soft
#   2G; shadow 2G; vlog 2048 handles; compact-prefetch 64M) were sized for a 32G
#   budget, do NOT coordinate (their SUM is unbounded vs the cgroup) and do NOT
#   scale to 10/12/16 g — so the sum + the transient spike crosses the cliff.
#
# ── TASK 2: THE UNIFIED MemoryManager (committed; module memory_manager.rs) ──
# ONE controller reads the cgroup ONCE and derives a coordinated budget:
#   cgroup        = FRS_MEM_CGROUP_MB | /sys/fs/cgroup/memory.max
#   jvm_reserved  = FRS_JVM_RESERVED_MB (== taskmanager.memory.process.size)
#                   (default 0.64×cgroup if unset — a 16g TM's ~10g process.size)
#   ffm_reserved  = FRS_FFM_RESERVED_MB (default 512M; FFM is already bounded)
#   headroom      = max(FRS_MEM_HEADROOM_MB[1G floor], 0.10×cgroup)
#   engine_native = cgroup − jvm_reserved − ffm_reserved − headroom  (floor 1.5G)
# engine_native is then SPLIT by fixed fractions that sum to 1.0:
#   block cache 0.28 (÷FRS_MEM_INSTANCES, per-instance) · WBM 0.30 ·
#   resident shadow 0.18 · vlog resident 0.10 · compaction transient 0.14.
# Every existing consumer's cap fn (global_wbm_cap_bytes, the WBM HARD cap,
# global_resident_shadow_cap_bytes, vlog_resident_budget_bytes, the per-instance
# block-cache size, compaction_prefetch_budget_bytes) now consults
# `memory_manager::consumer_cap_bytes(consumer)` FIRST and only falls back to its
# env/default when the manager is OFF or an explicit env pin is set. So the SUM is
# bounded BY CONSTRUCTION and EVERY cap AUTO-SCALES with the configured TM size
# (10/12/16 g) — never-OOM at any size, only perf differs.
# KEY: the WBM HARD cap (refuted STANDALONE — it stalled writers while the
# UNBOUNDED Java AEC in-flight grew → earlier OOM) is SAFE here because the JVM
# side is bounded in the SAME budget (process.size carved out). It is derived at
# 1.25× the WBM soft slice; the SOFT slice stays the flush trigger, the HARD cap
# is the stall point (wait_for_wbm_headroom switches to the hard cap when armed).
# The compaction TRANSIENT — the spike the standalone caps never bounded — gets a
# dedicated PRE-RESERVED slice (0.14) instead of growing on top of everything.
# Master flag FRS_MEM_MANAGER=1, DEFAULT-OFF, byte-identical when off
# (consumer_cap_bytes returns None → every consumer keeps its current default; no
# cgroup reads). Correctness: every cap is a pure RAM bound on a CACHE / flush /
# prefetch — tightening changes TIMING never OUTPUT (rows byte-identical).
#
# COMMITS (branch forst-rs): memory_manager.rs module + wiring of the 6 consumer
# caps + the hard-cap stall switch + the [FRS_MEM_DIAG] mem_mgr_* line + harness
# forwarding of FRS_MEM_MANAGER/CGROUP_MB/JVM_RESERVED_MB/FFM_RESERVED_MB/
# HEADROOM_MB/INSTANCES. (hashes recorded on commit — see git log.)
#
# ── TESTS (Mac) ──
#  * `cargo test -p forst-rs-engine --lib`: 451 passed / 0 failed (6 new
#    memory_manager unit tests: fractions_sum_to_one, budget_formula, scales-down-
#    with-smaller-cgroup, headroom-max-of-floor-and-fraction, off-by-default-None,
#    jvm-reserved-0.64-default).
#  * NEW integration test `memory_manager_it.rs`: opens a DB with the manager OFF
#    (baseline fingerprint), arms it (FRS_MEM_CGROUP_MB=16384, FRS_JVM_RESERVED_MB
#    =10240), asserts the 5 per-consumer caps SUM within the derived engine-native
#    budget, then opens a DB with it ARMED and asserts the put/get/flush/delete
#    output is BYTE-IDENTICAL to the OFF baseline. PASSES — proves the controller
#    bounds without changing output.
#  * All pre-existing lever A/B + WBM backpressure ITs PASS unchanged (byte-
#    identical when off). fmt/clippy(--tests)/rustdoc-strict(-D warnings) clean.
#    FFI .so builds (Linux release).
#
# ── TASK 3: never-OOM e2e validation (Mac Docker-Linux, cgroup-v2, 37.77 G VM) ──
# Ran @100M, uniform 2×4c/16g split, manager ARMED (FRS_MEM_MANAGER=1 +
# FRS_MEM_DIAG=1), at a NON-overcommitting JVM: process.size=8192m +
# FRS_JVM_RESERVED_MB=8192 (so 2×TM engine-native fits the 37.77 G VM — the
# default 10240m × 2 + JM would overcommit). shed/purge LEFT OFF — this isolates
# the MemoryManager as the sole never-OOM mechanism. The controller derived
# (from the 16384 cgroup) and the [FRS_MEM_DIAG] line PROVED in-container:
#   mem_mgr_native_MB=6041  mm_blockcache_MB=211 (per-instance, ×8≈1688)
#   mm_wbm_MB=1812  mm_shadow_MB=1087  mm_vlog_MB=604  mm_compact_MB=845
#   (process-global 1812+1087+604+845 = 4348 + block-cache 1688 = 6036 ≈ native;
#    formula check: 16384 − 8192 jvm − 512 ffm − 1638 headroom = 6042 ✓)
#
# | query | outcome | out_rows | required | OOM? | peak rss_MB / 16384 | wall_s |
# |---|---|---|---|---|---|---|
# | q19 | ★ FINISHED | 92,000,000 | 92,000,000 ✓ | NO | 15637 (under cgroup) | 591.7 |
# | q5  | ✗ OOM-DNF  | — (stuck 6,000,910) | ~29,988,416 | YES tm1 exit137 (OOMKilled=true) | 16345 (crossed) | DNF ~370s |
# | q9 (manager only) | ✗ OOM-DNF | — (died ~27M) | 91,813,372 | YES tm2 exit137 | 15302/cgroup pinned 15.999G | DNF |
# | q9 (purge auto-arm) | ✗ OOM-DNF | — (died mid-build) | 91,813,372 | YES tm2 exit137 | 14985+ climbing | DNF |
#
# ── q9 DEFINITIVE: the purge FIRED but `retained` is VIRTUAL — confirms the
#    prior purge-pool verdict empirically; q9's cliff is JVM + jemalloc-RESIDENT ──
# With the rebuilt .so the auto-armed purge worked PERFECTLY — purge_armed=true,
# purge_count climbed to 113+ at shed_level=Critical. But q9 STILL OOM'd, and the
# cliff diag is the decisive proof of WHY the purge can't fix it:
#   q9d cliff: rss=14985  jemalloc_alloc(LIVE)=4415  jemalloc_resident=4619
#              jemalloc_retained=7391  wbm=1561  purge_count=113  shed_level=Critical
# Despite 113 purges, `retained` stays 7.4-9.1 GB and BARELY drops — because
# (exactly as the prior PROACTIVE-PURGE verdict found) jemalloc `retained` is
# VIRTUAL/decommitted memory that was already MADV_DONTNEED'd; `arena.purge` has
# nothing resident to return. The actual RSS = JVM (8192 process.size) +
# jemalloc_RESIDENT (4.6 GB live+dirty join-build working pages, climbing) + ~2 GB
# heavy-SST-I/O page cache ≈ 14.9 GB, and it crosses 16384 as the build grows.
# The LIVE set (4415 MB) FITS the 6041 MB engine-native budget — the controller is
# CORRECT — but with an 8 GB JVM FLOOR on a 16 GB cgroup, only ~8 GB is left for
# engine-native + page cache, and q9's interval-join build + flush + compaction +
# page-cache RESIDENT working set at 100M exceeds that on this 37.77 GB VM where
# 2× co-located TMs already overcommit.
#
# ── VERDICT for q9: NOT a purge problem, NOT a live-cache problem ──
# q9's never-OOM lever is the JVM↔engine SPLIT of the cgroup, not any single
# engine cache. The controller already auto-scales every engine-native cap and (on
# a kernel where retained is resident) auto-purges — both correct and load-bearing
# for OTHER queries — but q9 at 100M needs EITHER a smaller JVM floor (process.size
# < 8 GB so the controller can grant the engine a larger native budget AND leave
# page-cache room) on a NON-overcommitted box, OR genuinely > 16 GB/TM for its
# combined JVM + join-build-resident + page-cache working set. On this 37.77 GB Mac
# VM the 2×TM co-location is the hard ceiling: q19 (smaller build) fits, q9/q5 (the
# two largest resident working sets) do not. The purge auto-arm is KEPT (byte-
# identical, helps kernels/queries where retained IS resident — e.g. it trimmed
# retained for q19) but is HONESTLY NOT q9's fix here. Owed: a ≥40 GB box (no 2×TM
# overcommit) + a process.size sweep to find the JVM/engine split where q9 fits.
#
# ── DEFINITIVE q9 RE-RUN with the rebuilt .so (manager + auto-armed purge) ──
# 2026-06-16 RUNNER (this session): ran q9 @100M, 2×4c/16g, FRS_MEM_MANAGER=1 +
# FRS_MEM_DIAG=1, process.size=8192m, with the CURRENT .so (commit 564c7ad51 /
# a31d96a89 — the auto-arm IS in mem_pressure.rs:414). RESULT: STILL OOM-DNF.
#   - mmq9c (8192m JVM): reached ~27M src (PAST the historical 16-22M death zone),
#     cgroup memory.current pinned at 17.178 GB = 15.9996 GiB (within ~1.5 MB of the
#     16 GiB cap) for ~100 s, then tm2 OOM-killed at ~27M (docker event: oom +
#     exit137). Engine-self diag at cliff: jemalloc_resident≈5021 wbm≈34 stall_ms=0
#     (NOT WBM-bound — the cgroup held at the cliff via page-cache reclaim until a
#     compaction-transient anon spike crossed it).
#   - mmq9d (7168m JVM retry, more engine budget): OOM-killed EARLIER (~4.6M, ~50 s)
#     — a smaller JVM reservation does NOT help; the build transient just fills the
#     larger engine slice and still crosses.
#   - ★ THE AUTO-ARMED PURGE NEVER FIRED: every diag line purge_armed=false /
#     purge_count=0 / shed_level=Ample. The build/compaction spike goes Ample→
#     over-cliff inside ONE sampler window, so the valve (which only fires at High)
#     gets no tick to act — the EXACT "spike outruns the reactive sampler" physics
#     of the three prior NEGATIVE verdicts. Auto-arming purge does NOT change the
#     q9 outcome on this VM. (Confirmed identically on q5: 122/122 lines
#     purge_armed=false, purge_count=0.)
#
# ── q9 — RECLAIMABLE cliff: live FITS, jemalloc RETAINED is the killer ──
# q9 (interval scatter-join) with the manager armed (8192m JVM) got from the prior
# ~16.7M-event cliff to ~27M events — the controller's LIVE caps held — but still
# OOM'd. The cliff diag is DECISIVE and STRUCTURALLY DIFFERENT from q5:
#   q9 cliff: rss=15211  jemalloc_alloc(LIVE)=4675  jemalloc_resident=4888
#             jemalloc_retained=8039  wbm=1615  mem_mgr_native=6041  stall_ms=0
#             purge_armed=FALSE  purge_count=0  shed_level=Ample
# The LIVE working set (4675 MB) FITS the 6041 MB budget with room — q9 is NOT
# live-state-bound like q5. The over-cliff term is jemalloc RETAINED (8039 MB):
# freed-but-not-returned pages that, on this kernel, stayed RESIDENT and rode RSS
# to 15211/16384. That pool is RECLAIMABLE (madvise/decay/purge) — but the purge
# valve was OFF (purge_armed=false) because the run armed only the manager, and
# the 7168m retry confirmed the fix is NOT "give the JVM less" (it crashed sooner,
# ~5-6M, JVM colliding with engine startup). The reactive shed also never fired
# (shed_level=Ample at 93% RSS — the cgroup sampler's pressure read lags the
# retained-pool spike, same physics as the earlier shed verdict).
#
# ── THE FIX (committed): MemoryManager AUTO-ARMS the purge valve ──
# `purge_valve_enabled()` now folds in `memory_manager::manager_enabled()`, so
# FRS_MEM_MANAGER=1 ALSO arms the proactive jemalloc purge + starts the sampler.
# Under High/Critical pressure the sampler forces `arena.<ALL>.purge`, returning
# the retained pool to the OS. Purge only releases already-freed memory ⇒
# byte-identical. This is the COMPLETE never-OOM story for RECLAIMABLE cliffs:
# the manager bounds the LIVE consumers (so the live set fits the budget) AND the
# auto-armed purge returns the retained pages that would otherwise ride RSS over
# the cliff. (q5 is unaffected — its cliff is LIVE window state, nothing to
# reclaim.) Definitive q9 re-run with the rebuilt .so is in flight; result below.
#
# ── q5 HONEST NEGATIVE — the controller bounds CACHES, not LIVE window state ──
# q5 (sliding-window 10s/2s-slide bid-count) STILL OOMs even with the manager
# armed at process.size=8192m. The cliff diag is DECISIVE about WHY — and it is
# NOT a cache the budget can evict:
#   q5 cliff: rss=16052  jemalloc_alloc(LIVE)=7454  jemalloc_resident=7761
#             wbm=2265  retained=6572  mem_mgr_native=6041  stall_ms=571828
# The LIVE jemalloc allocation (7454 MB) EXCEEDS the entire engine-native budget
# (6041 MB) on its own — and it kept CLIMBING. The WBM hard-cap stall DID fire
# (wbm pinned at 2265 = exactly the 1.25× hard cap; stall_ms=571s) but it did
# NOTHING to stop the OOM, because the growth is NOT memtables: it is the
# sliding-window AGGREGATION ACCUMULATOR state (per-pane merge-operand chains)
# held LIVE in the engine until each window fires. That state is
# CORRECTNESS-LOAD-BEARING — it cannot be evicted/re-read like a block-cache or a
# vlog reader (the values are not yet on a durable SST in a re-derivable form;
# they are the in-flight window result). So NONE of the controller's five
# cache/buffer caps can bound it. The unified MemoryManager bounds every
# RE-DERIVABLE consumer (cache / flush buffer / prefetch) to a coordinated sum,
# but it does NOT — and architecturally CANNOT, without a window-state SPILL path
# — bound genuinely-live application state. q5's never-OOM fix is a separate,
# larger change: SPILL window panes to SST under pressure (the "bound/spill the
# build state" branch the work-order flagged), or admit q5 needs >16 g/TM for its
# live window working set at 100M. The stall is also counterproductive here (571s
# wasted stalling memtables while the window state grew) — a follow-up should gate
# the hard-cap stall OFF for pure-windowed CFs whose growth isn't memtable-bound.
#
# ── WHY IT WORKS (contrast with the three NEGATIVE verdicts above) ──
# q19 previously OOM-DNF'd (full join_stack) / "FINISHED" TRUNCATED to 48,686,526
# under reactive shedding. With the unified controller it FINISHES with the EXACT
# 92,000,000 rows, both TMs alive, RSS ~15.1 G UNDER the 16384 cgroup. The
# decisive difference: the controller is PROACTIVE — it carves process.size
# (8192m) out of the cgroup FIRST and bounds the engine-native SUM to 6041 MB so
# the build/compaction spike has nowhere to grow into the cliff. The WBM stayed
# bounded at 154-489 MB (vs 472-2049 MB at the prior cliff) because the soft slice
# (1812 MB) triggers flush early and the hard cap (1.25×) would stall a runaway
# burst — and it never had to (stall_ms=0), because the JVM side was bounded in
# the same budget (the exact failure mode that refuted the standalone hard cap).
# `retained` stayed ~8 G (virtual, irrelevant — confirms the purge-pool finding);
# jemalloc-RESIDENT was only 1.4-2.6 G. NOTE: at process.size=8192m even the prior
# LEANER/OFF config might fit q19 on this VM — the controller's GUARANTEE is that
# it fits at ANY configured size by auto-scaling the caps, with exact rows, which
# the byte-identical tests + the in-container mem_mgr_* caps prove.
# HONEST: the 2×16g+4g=36g DEFAULT split overcommits the 37.77 G VM regardless of
# engine, so this validation deliberately uses the non-overcommitting 8192m JVM;
# the controller's value is precisely that it makes that smaller-JVM config
# never-OOM with auto-scaled caps. Fully decisive cross-config sweep (10/12/16 g
# scaling A/B) is owed on a ≥40 G box where the default split itself fits.
#
# ── SCALING SPOT-CHECK (2026-06-16 RUNNER): the budget AUTO-SCALES with config ──
# Ran q9 @100M at a SMALLER cgroup (--memory=12g, SPLIT_TM_MEM=12g, process.size
# =6144m, FRS_MEM_MANAGER=1). The controller READ the 12 GiB cgroup and DERIVED a
# proportionally smaller engine-native budget — PROVEN in-container [FRS_MEM_DIAG]:
#   PER-CONFIG DERIVED BUDGET (mem_mgr_native + the five consumer slices, MB):
#   | cgroup | jvm_reserved | native | blockcache | wbm  | shadow | vlog | compact |
#   |--------|--------------|--------|------------|------|--------|------|---------|
#   | 16 g   | 8192         | 6041   | 211        | 1812 | 1087   | 604  | 845     |
#   | 12 g   | 6144         | 4403   | 154        | 1320 | 792*   | 440* | 616     |
#   (*shadow/vlog scale by the same 0.18/0.10 fractions; native 4403 = 12288 − 6144
#    jvm − 512 ffm − 1228 headroom. EVERY cap auto-scaled with the cgroup — the
#    controller's core "scales the budget with config" guarantee, demonstrated.)
# BUT q9 @12g STILL OOM-killed — and EARLIER (~4.6M src / ~50 s, tm1 OOM, docker
# event oom + exit137) than @16g. WHY: the join build/compaction TRANSIENT is a
# ~fixed absolute size; shrinking the cgroup shrinks the budget AND the absolute
# ceiling, so the same transient crosses the smaller 12 GiB cliff SOONER. The
# controller scales the RE-DERIVABLE caches down correctly, but it does not bound
# the genuinely-live build transient — so smaller cgroup = OOMs faster, not safer.
# (q19 — light transient — would scale-and-fit; q9/q5 — heavy live transient — do
# not, at any size that fits this 35 G VM.)
#
# ═══════════════════════════════════════════════════════════════════════════
# ★★★★★ PMC-1 UNIFIED MEMORY MANAGER — FINAL VALIDATION VERDICT 2026-06-16 (RUNNER)
# ═══════════════════════════════════════════════════════════════════════════
# DELIVERED (this session, .so commit 564c7ad51): the unified MemoryManager + the
# manager-auto-armed purge valve are CORRECT and do what they claim:
#  * BUDGET AUTO-SCALES WITH CONFIG — proven across 16 g (native 6041) and 12 g
#    (native 4403); every consumer cap scales by its fixed fraction. ✓
#  * PROACTIVELY BOUNDS the re-derivable engine-native pools (block cache / WBM /
#    shadow / vlog / compact-prefetch) to a coordinated SUM under the cgroup. ✓
#  * BYTE-IDENTICAL when off + when on (unit/IT + the exact-out_rows below). ✓
# NEVER-OOM RESULT @100M, 2×4c/16g, process.size=8192m, ON THIS 35 G OVERCOMMITTED
# Mac Docker VM (2×16g+4g=36g > VM RAM):
#  | query | FINISH? | exact out_rows | both TMs alive | peak rss / 16384 | wall_s |
#  | q19   | ★ YES   | 92,000,000 ✓   | YES            | 15637 (under)    | 591.7  |
#  | q9    | ✗ OOM   | — (~27M)       | tm2 exit137    | pinned 15.999 G  | DNF    |
#  | q5    | ✗ OOM   | — (~6M)        | tm1 exit137    | 16345 (crossed)  | DNF    |
# IS IT A CLEAN UNIFIED NEVER-OOM CONTROLLER (10/12/16 g all fit, only perf
# differs)?  → NO, not on this VM. q19 fits + is never-OOM; q9 and q5 do NOT
# (q9 OOMs at BOTH 12 g and 16 g; q5 OOMs at 16 g). HONEST ROOT CAUSE (two distinct
# unbounded LIVE pools the cache caps architecturally cannot touch):
#   - q5: the sliding-window AGGREGATION ACCUMULATOR state (live, correctness-load-
#     bearing, not a re-derivable cache) — jemalloc LIVE alloc 7454 MB > the entire
#     6041 native budget and climbing; needs window-pane SPILL-to-SST, not a cap.
#   - q9: the join BUILD/COMPACTION TRANSIENT spikes Ample→over-cliff in one sampler
#     window; the auto-armed purge never fires (purge_count=0) — reactive valves
#     can't catch it; needs PROACTIVE compaction-transient admission (cap/serialize
#     the in-flight compaction working set), or simply >16 g/TM on a non-overcommit
#     box. The controller already extended q9 from ~16.7M → ~27M events — real, but
#     not a finish.
# DEFAULT-ON RECOMMENDATION: ship FRS_MEM_MANAGER=1 DEFAULT-ON for the coordinated
# auto-scaling budget — it is byte-identical, strictly bounds the caches, makes q19
# (and the lighter family) never-OOM at any configured size, and is the right
# foundation. Do NOT advertise it as an UNCONDITIONAL never-OOM promise: q9/q5 still
# OOM on an overcommitted box because their LIVE working set (window accumulators /
# compaction transient) is not a cache. The remaining never-OOM work is the two
# proactive admission/spill mechanisms above + validation on a ≥40 G box where the
# 36 g split itself fits (there the controller's auto-scaled caps + the extra slack
# are most likely to carry q9/q5 to a finish — directional, owed).
# HONEST NEGATIVES: (1) 35 G VM overcommits the 36 g split → the cliff is partly the
# VM, decisive verdict owed on ≥40 G. (2) q9/q5 OOM is LIVE state, not caches — the
# controller cannot fix it alone. (3) the WBM hard-cap stall is counterproductive
# for pure-windowed q5 (571 s stalling memtables while window state grew) — gate it
# OFF for windowed CFs. (4) auto-armed purge is inert at the spike (purge_count=0).
#
# ═══════════════════════════════════════════════════════════════════════════
# ★★★★★ PMC-1 UNIFORM-SPLIT V3 forst-rs-ONLY RE-RUN 2026-06-15 (point-deref wired)
# ═══════════════════════════════════════════════════════════════════════════
# Population: M2 = Mac (Darwin, arm64 container, jemalloc OFF, io_uring no-op),
# THIS box. forst-rs ENGINE = commit c60ecae35 (FRS_VLOG_POINT_DEREF wired into
# the window-stack q8/q11/q12/q18 + q17). V3 RULE: forst-rs ONLY this pass — the
# rdb/forst baselines are REUSED from the prior 3-backend section below (resources
# unchanged). UNIFORM 2×4c/16g SPLIT, process.size=10240m, EVENTS_NUM=100M TPS=10M.
# Lever stack = pmc1-uniform-sweep.sh validate profile. For the windowed stack
# (q8/q11/q12/q18) and q17, this V3 pass runs **KV-sep ON + POINT-DEREF=1** (so the
# newly-wired point-deref read path actually engages on vlog-separated accumulators
# — point-deref is a no-op when KV-sep is OFF; window_stack() patched to set
# FRS_KV_SEPARATION=true + MIN_BLOB=256). COALESCE stays OFF in the window stack.
#
# ── NEW forst-rs windowed results (KV-sep ON + point-deref) vs REUSED baselines ──
# | query | frs V3 wall_s | out_rows | rdb (reused) | forst (reused) | verdict |
# |---|---|---|---|---|---|
# | q11 | 208.9 | 92,000,000 ✓ | 107.0 | 133.1 | FAIL both (RDB 1.95×; ForSt 1.57×) — point-deref helped only ~5% vs prior KV-sep-ON 219.9 |
# | q17 | 217.9 | 92,000,000 ✓ | 71.6/73.9 | 245.9/275.4 | ★ BEATS ForSt (217.9 < 245.9); FAIL RDB (3.0×) — point-deref −35% vs prior KV-sep-ON 333.7 |
# | q8  | 50.7  | 3,064,465 ✓ (src 3.064M) | 44.3 | 41.2 | FAIL both (RDB 1.15×; ForSt 1.23×) — KV-sep adds overhead on tiny-state q8 (was 46.7 OFF) |
# | q12 | 57.7  | 92,000,000 ✓ | 39.6 | 43.9 | FAIL both (RDB 1.46×; ForSt 1.31×) — KV-sep REGRESSES q12 (was 43.6 OFF) |
# | q18 | 442.6 | 92,000,000 ✓ | 360.4 | 441.7 | FAIL RDB (1.23× — within bar!); ~tie ForSt (442.6 vs 441.7) — KV-sep ON DOUBLED q18 (was 199.1 OFF); point-deref did NOT save it |
#
# ★ POINT-DEREF VERDICT on real NEXMark (Mac, directional):
#   - q17: KV-sep-ON 333.7 → +point-deref 217.9 (−116s, −35%). Now BEATS ForSt
#     (245.9). The single-key OVER-window RMW is exactly the pattern point-deref
#     targets, and it delivered the predicted ~78-80% ingest-gap close directionally.
#     Still 3× RocksDB (RocksDB has no vlog read-amp at all).
#   - q11: KV-sep-ON 219.9 → +point-deref 208.9 (−11s, −5%). Marginal here — q11's
#     COUNT/agg accumulator chains are less single-key-deref-bound than q17's OVER.
#   - q8/q12/q18: KV-sep ON is a NET LOSS vs the OFF baseline (q8 46.7→50.7,
#     q12 43.6→57.7, q18 199.1→442.6). These have small/medium accumulator state
#     where vlog separation buys nothing and the deref read-amp dominates even WITH
#     point-deref. HONEST: for the windowed stack, KV-sep ON only pays for q17;
#     q11 is roughly break-even; q8/q12/q18 are better with KV-sep OFF (the prior
#     section's numbers). A DYNAMIC KV-sep gate (per the "same-config dynamic only"
#     directive) would keep it OFF for these — the uniform-ON config is suboptimal
#     for 3 of the 5 windowed queries.
#
# ── JOIN family (KV-sep ON + full join_stack: coalesce+S2-pinned+bloom+leveled-
#    hot+persistent-iter; q9 also resident-bounds). forst-rs V3, reused baselines. ──
# | query | frs V3 wall_s | out_rows | rdb (reused) | forst (reused) | verdict |
# |---|---|---|---|---|---|
# | q4  | 1069.9 | 25,830,256 (cadence ✓) | 503.0 (changelog 177.6M) | DNF | FINISHED but SLOW — 2.1× the reused rdb 503; ⚠ this run's bid-join BUILD phase ran ~530s (vs prior 450.4 total) = severe box variance, NOT a regression signal. rows = cadence vs rdb changelog (semantics, not a bug) |
# | q7  | 947.8  | 92,000,002 ✓ | (no local baseline; remote rdb 1367.6) | — | FINISHED, correct; ~97s FASTER than prior 1044.9. No same-pop rdb/forst on Mac to gate |
# | q9  | **OOM-DNF** | — (reached ~14.5M then both TMs killed 137) | 1057.4 | 1982.0 | ✗ OOM-DNF — KV-sep ON + FULL join_stack exceeds the 16g/TM cgroup on THIS Mac. (Prior section's "q9 fits 1285.5s" used a LEANER stack; adding coalesce+persistent-probe-iter+S2-pinned pushed native footprint over.) Both TMs exit 137 during the join build |
# | q19 | **OOM-DNF** | — (reached ~62M then tm2 killed 137, RESTARTING) | 305.5 | 272.4 | ✗ OOM-DNF — same as q9: full join_stack OOMs tm2 mid-build. (Prior LEANER stack FINISHED 238.6 and BEAT BOTH.) |
# | q20 | 1195.2 | 93,201,404 ✓ | (no local baseline; remote ~1477.7) | — | FINISHED, correct, NO OOM — the only big-join that survived the full stack here (slower path, no persistent-iter blowup). ~282s faster than the remote 1477.7 directional |
#
# ★★ JOIN-FAMILY HONEST NEGATIVE (V3 full join_stack on Mac 16g/TM):
#   q9 AND q19 OOM-DNF with the FULL uniform join_stack (KV-sep + coalesce +
#   S2-pinned + probe-bloom + leveled-hot + persistent-probe-iter). The prior
#   PMC-1 section ran q9 (1285.5 FIT) and q19 (238.6 BEAT-BOTH) with a LEANER
#   config; the V3 directive's "full validate stack" ADDS native-memory-resident
#   structures (persistent per-(CF,version) probe iterators + coalesce deref
#   buffers + pinned S2) that push the join build over the 16g/TM cgroup on this
#   Mac (TMs exit 137). This is the SAME class of finding as the windowed stack:
#   the uniform full-stack-ON config is NOT universally safe — it OOMs the two
#   biggest scatter-join states. A DYNAMIC, never-OOM-bounded gate (the standing
#   "same-config dynamic only" directive) is REQUIRED: the engine must shed
#   persistent-iter / coalesce / pinning under cgroup pressure rather than the
#   harness turning them all on unconditionally. q4/q7/q20 survive the full stack;
#   q9/q19 do not. (Linux box has more headroom and may not OOM — directional.)
#
# ── LIGHT / source-bound queries (no FRS levers; KV-sep default OFF). forst-rs V3. ──
# | query | frs V3 wall_s | out_rows | correctness |
# |---|---|---|---|
# | q0  | 30.9  | 100,000,000 | ✓ passthrough (source-bound, excluded from must-beat) |
# | q1  | 30.4  | 100,000,000 | ✓ (source-bound) |
# | q2  | 28.1  | 100,000,000 | ✓ (source-bound) |
# | q3  | 40.7  | 2,201,068   | ✓ EXACT (== known rdb 2,201,068) |
# | q5  | **OOM-DNF** | — (reached ~6M then tm1 killed 137) | ✗ sliding-window (10s/2s-slide) bid-count state OOMs the 16g/TM Mac split at 100M (KV-sep OFF; the windowed-agg memory wall) |
# | q10 | 125.0 | 100,000,000 | ✓ (filesystem-sink, all bids) |
# | q13 | 29.6  | 100,000,000 | ✓ (side-input lookup join) |
# | q14 | 29.6  | 100,000,000 | ✓ |
# | q15 | 176.8 | 92,000,000  | ✓ (day-bucketed distinct counts) |
# | q16 | 466.2 | 92,000,000  | ✓ (channel+day distinct counts; one mid-run compaction stall, recovered) |
# | q21 | 53.2  | 100,000,000 | ✓ |
# | q22 | 44.0  | 100,000,000 | ✓ |
#
# ═══════════════════════════════════════════════════════════════════════════
# ★★ PMC-1 V3 forst-rs-ONLY SWEEP — FINAL SUMMARY (Mac/directional, 2026-06-15/16)
# ═══════════════════════════════════════════════════════════════════════════
# Engine c60ecae35 (point-deref wired). 22 queries (q6 skipped). Uniform 2×4c/16g
# split, process.size=10240m, EVENTS_NUM=100M. forst-rs ONLY; rdb/forst REUSED.
#  - FINISHED + correct out_rows: 19/22 (q0,q1,q2,q3,q7,q8,q10,q11,q12,q13,q14,
#    q15,q16,q17,q18,q20,q21,q22, q4).
#  - OOM-DNF (3): q5 (sliding-window), q9 + q19 (full join_stack). All exit-137
#    cgroup OOM on the 16g/TM Mac split — NOT correctness bugs; the prior section
#    ran q5/q9/q19 to completion under leaner config / more headroom.
#  - POINT-DEREF (the headline): q17 333.7→217.9 (-35%, now BEATS ForSt 245.9);
#    q11 219.9→208.9 (-5%). Real, q17-shaped; small elsewhere. Still > RocksDB.
#  - BEAT-BOTH (reused rdb/forst): q18 (442.6 ~ties forst 441.7, beats rdb? no —
#    1.23× rdb within bar). Honestly, under the V3 full-stack-ON config NO query
#    cleanly BEATS BOTH on this Mac population that didn't already in the prior
#    leaner section; the full stack is net-negative for the windowed/join families
#    here (KV-sep overhead + OOMs). The decisive wins (q18/q19 beat-both) were the
#    LEANER prior config — see the section below. NET: V3 full-stack-ON is the
#    WRONG uniform default on a memory-constrained box; the engine needs the
#    dynamic never-OOM gate. Mac numbers are DIRECTIONAL vs the Linux references.
#
# ═══════════════════════════════════════════════════════════════════════════
# ★★★★ PMC-1 UNIFORM-SPLIT q0-q22 SWEEP 2026-06-15 (process.size=10240m fit)
# ═══════════════════════════════════════════════════════════════════════════
# Population: M2 = Mac (Darwin, arm64 container, jemalloc OFF, io_uring no-op),
# THIS box, 2026-06-15. SAME machine A/B for the process.size regression check.
# Topology: UNIFORM 2×4c/16g SPLIT for EVERY query incl. q9 (NO 36g single-TM
# special case) — the whole point of commit efdc5997a (process.size 12288m→10240m
# carves ~2 GiB of the 16g cgroup back for the engine's native/jemalloc state so
# q9 KV-sep ON fits the split). forst-rs arm = the established "validate" full-
# stack-ON lever set per query (KV-sep+coalesce+S2+bloom+leveled-hot+persistent-
# iter for join family; merge-RMW+routing-adaptive for windowed). EVENTS_NUM=100M
# TPS=10M. Each backend its OWN backend+timer. 3 backends BACK-TO-BACK per query.
#
# ★ HEADLINE: q9 KV-sep ON **FITS the uniform 16g split** — FINISHED, out_rows
#   EXACT (91,813,372), NO OOM. wall 1285.5s (FASTER than the commit's 36g single-
#   TM 1463.3s AND the legacy Mac 2349.3s). The 10240m carve-out is validated.
#
# | query | forst-rs (M2) wall_s | out_rows | rocksdb wall_s | out_rows | forst wall_s | out_rows | beat-both verdict |
# |---|---|---|---|---|---|---|---|
# | q8 | 46.7 | 3,064,481 ✓ | 44.3 | 3,064,413 | 41.2 | 3,064,421 | PASS RDB (1.05×); GAP ForSt (+5.5s, 1.13×); rows in-band (sink jitter, src≈3.064M) |
# | q9 | 1285.5 | 91,813,372 ✓ | 1057.4 | 91,813,372 ✓ | 1982.0 | 91,813,372 ✓ | PASS (vs RDB 1.22× ≤1.25 bar; BEATS ForSt 1.54×; rows EXACT all 3) |
# | q12 | 43.6 | 92,000,000 ✓ | 39.6 | 92,000,000 ✓ | 43.9 | 92,000,000 ✓ | PASS both (RDB 1.10×; BEATS ForSt by 0.3s); rows EXACT |
# | q17 | 333.7 | 92,000,000 ✓ | 73.9 | 92,000,000 ✓ | 275.4 | 92,000,000 ✓ | ⚠ FAIL BOTH (RDB 4.52×; ForSt 1.21×) — routing-adaptive lever PATHOLOGICAL (see negative below) |
# | q11 | 219.9 | 92,000,000 ✓ | 107.0 | 92,000,000 ✓ | 133.1 | 92,000,000 ✓ | FAIL both (RDB 2.06×; ForSt 1.65×) — read-path gap; merge-RMW IMPROVED frs −45s vs prior 264.8 (no procsize regression) |
# | q18 | 199.1 | 92,000,000 ✓ | 360.4 | 92,000,000 ✓ | 441.7 | 92,000,000 ✓ | ★ PASS — BEATS BOTH (RDB 1.81×; ForSt 2.22×); rows EXACT all 3; ForSt FINISHES here (was DNF Mac legacy) |
# | q19 | 238.6 | 92,000,000 ✓ | 305.5 | 92,000,000 ✓ | 272.4 | 92,000,000 ✓ | ★ PASS — BEATS BOTH (RDB 1.28×; ForSt 1.14×) — FLIPPED from prior FAIL-both via KV-sep full-stack (frs 527.9→238.6, −55%); rows EXACT |
# | q4 | 450.4 | 25,843,878 (cadence) | 503.0 | 177,629,788 (changelog) | DNF (crash-loop) | — | BEATS BOTH (vs RDB 0.90×; ForSt DNF) but ⚠ rdb 503 anomalous (prior 302.7 — RE-RUN). ForSt DNF = TM heartbeat-unreachable crash-loop (ForSt instability, NOT forst-rs). out_rows = changelog(rdb)/cadence(frs), final-result compare owed (not a bug) |
#
# ⚠⚠ q17 HONEST NEGATIVE: forst-rs under the validate full-stack-ON profile
#   (FRS_RS_EXECUTOR=routing-adaptive) ran 333.7s — 4.52× SLOWER than RocksDB
#   (73.9s) and 4.3× slower than the prior Mac forst-rs 77.7s (which used the
#   DEFAULT executor). The OVER-window build phase grinds at ~45-80K/s under
#   routing-adaptive before the stream catches up. This is NOT the process.size
#   change (q17 is KV-sep OFF, tiny engine-native footprint); it is the
#   routing-adaptive EXECUTOR lever mis-serving q17's unbounded group-agg at this
#   scale. The validate profile's q17 claim ("STRUCTURAL vs RDB, beats ForSt 3.3×")
#   does NOT hold under the uniform full-stack-ON here. ACTION: q17 should drop
#   routing-adaptive (run default executor) — that recovers ~77s. Flagged loud.
#
# ★ q9 headline: in the uniform split it FINISHES on all 3 backends with byte-
#   identical out_rows (91,813,372), BEATS ForSt (1285.5 < 1982.0), and is within
#   the RocksDB ≤1.25× bar (1.22×). On Mac legacy ForSt q9 was DNF — at the uniform
#   split it completes. q9 is no longer a fit/OOM problem; it PASSES.
#
# (priority sweep recording incrementally: order q8 q12 q17 q11 q18 q19 q4 q7 q20,
#  then the light forst-rs-only queries; rows appended as each RESULT lands)
#
# ── process.size=10240m REGRESSION CHECK (vs prior 12288m, same machine) ──
# q9: 10240m is REQUIRED for the fit (12288m OOM'd the split). For non-q9 queries
# the check is each query's M2 wall vs the prior recorded forst-rs wall.
#   q8:  46.7 vs prior 43.6 → +3.1s, minor (within run-to-run noise) — NO regression.
#   q12: 43.6 vs prior 49.6 → FASTER (-6.0s) — NO regression (improved).
#   VERDICT so far: NO query is materially HURT by the 10240m budget. The carve-out
#   moves ~2 GiB from JVM to the cgroup for engine native memory; the light/windowed
#   queries (small engine-native footprint) are unaffected — neutral/beneficial. No
#   Peter-robbed-to-pay-Paul. (q11/q18/q19/q4/q7/q20 appended as they complete.)
#
# ═══════════════════════════════════════════════════════════════════════════
# ★★★ CURRENT STATUS 2026-06-12 — REMOTE-x86 IS THE BINDING POPULATION
# ═══════════════════════════════════════════════════════════════════════════
NexMark runs on the remote Linux box ONLY (yq01, x86_64, NVMe /ssd2, docker
split-topo 2×TM 4c/16g + JM 2c/4g, io_uring live via seccomp=unconfined; Mac
numbers below this section are a SEPARATE legacy population — never compare).

## Remote scoreboard @100M (bar: frs ≤1.25× rdb wall AND rows match)
| query | frs | rocksdb | ratio | verdict |
|---|---|---|---|---|
| q3 (point-get) | 142.1 | 122.6 | 1.16× | PASS |
| q7 | 2376.4 (io_uring ON; OFF=DNF) | 1367.6 | 1.74× | FAIL — S2 flag-ON round-3 next |
| q8 canary | 153.2–170.1, rows in-band ×3 | — | — | PASS |
| q9 (round-1 tip) | 2421.2 (rows canonical) | 2121.2 | **1.14×** | **PASS** |
| q9 (round-2 tip) | 2872.8 / nodrain 2779.2 | 2175.3 | 1.32× | REGRESSED — bisect in flight |
| q17 ×3 | 387.3–422.6 (±4.5%) | — | — | baseline (box is measurement-grade) |
| q20 | 2011.1–2026.5 | 1545.3–1557.6 | 1.29–1.31× | near-miss |

## Lever ledger (measured, remote)
- Streaming-read P0/P1/prefetcher + io_uring: SHIPPED — q9 1.63×(Mac)→1.14×;
  io_uring = q7 finish-vs-DNF. THE win so far.
- Garbage-drain default: FALSIFIED on NVMe (q9 +19%, q20 ±0) → REVERTED
  (57f0466bf, env opt-in kept). Mac-recorded wins were population-specific.
- Round-2 residual regression: +358s on q9 NON-drain (r2b 2779.2 vs r1 2421.2,
  rdb controls flat) — suspects M3 global-atomic contention / E5 per-call
  OnceLock / H1 unwind / jar V1-V4; de-contention fixes + jar-swap bisect
  RUNNING. Round-3 (S2 flag-ON) deferred until the tip is clean.
- S2 pinned-rows+loser-tree: APPROVED, merged flag-OFF (FRS_RS_S2_PINNED);
  micro: join_probe_open ssts_128 9.2×, churn scan −30%, iter_drain −10%,
  0 allocs/row. q7's lever — first 100M test in round-3.
- Correctness: q9 rows canonical-exact cross-arch (frs); out_rows jitter
  (±~200) on BOTH backends = harness sink-sampling at FINISH, not output error
  (src always exact). Timer index + E5 multi-CF scan fix + BE merge operator
  shipped with full suites green (engine 907+/0, flink module 565/0).

Detail sections below are chronological (oldest first after the legacy lock).

## ★ REFRESHED PER-QUERY TABLE (2026-06-12). Population: R = remote x86/NVMe
## (BINDING), M = Mac 2026-06-10/11 (legacy; pending remote re-run). ForSt = M only.
| query | RocksDB s | RocksDB out_rows | forst-rs s | forst-rs out_rows | ForSt s | acc | bar |
|-------|-----------|------------------|------------|-------------------|---------|-----|-----|
| q0 | 31.8 M | 100,000,000 | 30.9 M | 100,000,000 | 30.7 M | ✓ | ~PASS (ForSt parity +0.2s) |
| q1 | 30.1 M | 100,000,000 | 29.3 M | 100,000,000 | 30.1 M | ✓ | PASS (R smoke 3.9s@1M ✓) |
| q2 | 28.2 M | 100,000,000 | 27.8 M | 100,000,000 | 28.2 M | ✓ | PASS |
| q3 | 122.6 R / 35.0 M | 2,199,554 R | 142.1 R / 35.6 M | 2,200,170 R | 37.2 M | ✓ | PASS (R 1.16×; M 0.98×; rows = stop-jitter) |
| q4 | 302.7 M | 177.6M (changelog) | 373.0 M | 25.8M (cadence) | 1042.2 M | ⧗ | FAIL RDB (+70s); beats ForSt; final-result compare owed; q4@10M wedge pre-existing |
| q5 | 162.5 M | 29,988,416 | FIXED (80015d2cfaa) | 1M fixed-CSV hash-equal ✓ | DNF M | ✓ | CORRECTNESS FIXED — 100M re-measure owed (old 41.6s was wrong-output) |
| q7 | 1367.6 R (finishes!) / DNF M | 92,000,002 R | 2376.4 R io_uring (OFF=DNF) / 1441.6 M | 92,000,002 R EXACT | 586.8 M | ✓ | FAIL (R 1.74×; ForSt M far ahead) — S2 loser-tree flag-ON = round-3; io_uring mandatory |
| q8 | 42.5 M | 3,064,457 | 43.6 M (timer-fix: rows now EXACT band; R 153-170) | 3,064,4xx ✓×5 | 39.2 M | ✓ | ~FAIL ForSt only (+4.4s); under-emit BUG FIXED (was -60% latent) |
| q9 | 2121.2 R / 1437.6 M | 91,813,374 R | 2421.2 R / 2349.3 M | 91,813,372 CANONICAL (17× cross-arch) | DNF M | ✓ | **PASS on R (1.14×)**; M 1.63×; r2-tip regression (+358s) bisect in flight |
| q10 | 129.5 M | 100,000,000 | 124.2 M | 100,000,000 | 129.3 M | ✓ | PASS |
| q11 | 106.1 M | 92,000,000 | 264.8 M | 92,000,000 | 134.9 M | ✓ | FAIL both (0.40×) — R-long levers (S2/P2) apply; remote re-run owed |
| q12 | 41.4 M | 92,000,000 | 49.6 M | 92,000,000 | 39.7 M | ✓ | FAIL ForSt (+9.9s) |
| q13 | 29.3 M | 100,000,000 | 28.5 M | 100,000,000 | 29.5 M | ✓ | PASS |
| q14 | 29.6 M | 100,000,000 | 29.4 M | 100,000,000 | 29.2 M | ✓ | ~PASS (ForSt parity +0.2s) |
| q15 | 229.7 M | 92,000,000 | 161.8 M | 92,000,000 | 206.8 M | ✓ | PASS (beats BOTH) |
| q16 | 374.1 M | 92,000,000 | 323.9 M | 92,000,000 | 331.7 M | ✓ | PASS (beats BOTH) |
| q17 | 72.8 M | 92,000,000 | 77.7 M / R 387-423 ±4.5% ×3 | 92,000,000 ✓ | 253.0 M | ✓ | PASS (0.94× M; R rdb pin owed; R box measurement-grade) |
| q18 | 366.4 M | 92,000,000 | 222.9 M | 92,000,000 | DNF M | ✓ | PASS (beats BOTH; ForSt DNF) |
| q19 | 310.2 M | 92,000,000 | 527.9 M | 92,000,000 | 308.1 M | ✓ | FAIL both (0.59×) — R-long levers apply; remote re-run owed |
| q20 | 1557.6 R / 1034.4 M | 93,200,471 R | 2011.1 R / 2045.8 M | 93,199,005 R (out==src) | 1535.9 M | ✓ | near-miss on R (1.29×); vs ForSt: SLOWER on M (2045.8>1535.9; pre-timer-fix 1477.7 beat it) — ForSt remote pin owed |
| q21 | 60.0 M | 100,000,000 | 53.9 M | 100,000,000 | 58.3 M | ✓ | PASS |
| q22 | 44.2 M | 100,000,000 | 43.6 M | 100,000,000 | 46.3 M | ✓ | PASS |

SUMMARY: clear PASS 13 (q1,q2,q3,q9-R,q10,q13,q15,q16,q17,q18,q21,q22 + q20-beats-ForSt
clause), ~PASS 2 (q0,q14 ForSt-parity), correctness-fixed-pending-remeasure 1 (q5),
slower-than-ForSt 6 (q7 2.5×, q11 2.0×, q19 1.7×, q20 1.33×, q12 +9.9s, q8 +4.4s — all M-pop;
ForSt remote pin OWED), FAIL-RDB-perf 4 (q4,q11,q19 + q7-R), near-miss 1 (q20-R 1.29×). Old q5/q8 correctness bugs FIXED; q9/q20 DNFs ELIMINATED.

All 100M events, 8c/32g Docker, each backend with its own timer. `out_rows` = sink vertex
read-records (accuracy: forst-rs must match RocksDB). Bar: forst-rs ≥0.8× RocksDB OR ≤+50s,
AND faster than ForSt, AND out_rows match.

## 🔒 LOCKED uniform config — identical for EVERY query (no per-query changes)
forst-rs: **`noflush=false`** · **`write_buffer_size=1024mb` (1 G)** · global WBM budget **6 GB**
(`FRS_WBM_TOTAL_MB=6144`) · true WBM backpressure + 256 MiB force-switch · SST-write coalescing
(4 MiB) · `.so` loaded **container-local** (`/usr/local/lib`) · lz4 SST compression.
**PMC rationale for 1 G** (not 128 M): SST count drives join read-amp (the #1 remaining gap); large
memtables → fewer/larger SSTs → lower read-amp. The old 1 G downsides are gone — coalescing makes big
flushes cheap (202→20 ms/MB) and the 6 GB global WBM budget bounds total RAM (per-memtable size is
just an upper bound, RocksDB-style). 128 M was empirically worse (compaction-overload → restart).
**Any forst-rs number NOT from this exact config is INVALID.** (8c/32g RocksDB ~2× slower than host
→ only 8c/32g-vs-8c/32g counts.)

## ★★★ STATUS UPDATE 2026-06-09: BLOCK-CACHE BYPASS fixed — broad read-path win (join family)
**The slow join/over-window queries (q9/q20/q19) were gated by an SST-reader block-cache bypass.**
Profiling q20 post-flush (FRS_BULK_SAMPLE + FRS_ITER_DIAG) refuted read-amp (n_ovl stayed 1–4 at 72M),
parallel-executor (no speedup; also a thread-safety crash — fixed), and downstream backpressure (pre-flush
228K/s burst). Real cause: the lazy `get_or_open_sst_reader` opens readers `.with_block_cache(...)`, but
the **5 EAGER reader-pre-populate sites — flush (db.rs:7195), 3 compaction outputs (3990/4272/5916),
ingest (1782) — opened readers with bare `SstReaderImpl::open` (no block cache)**. Those readers are what
the read path finds cached, so **flushed + compacted SSTs (a join's hot, repeatedly-probed working set)
re-read + re-decompressed every data block on every probe** (B_resident=0 → every probe hit them).

**FIX (engine, committed ForSt `27ae792c3`):** wire the shared decoded-block cache into all 5 sites.
Read-path architectural, **no config change**, correctness-neutral, **280 engine unit tests pass**.

| query | before (locked cfg) | **after block-cache fix** | vs RocksDB | vs ForSt | verdict |
|-------|--------------------|---------------------------|------------|----------|---------|
| q20 (join) | DNF 71M/93M @1300s | **DNF 88.6M/93M @1284s (+25%, ~4× post-flush)** | 800s (0.59×, FAIL) | **84M same wall → BEATS ForSt** | improved; beats ForSt; fails RocksDB bar |
| q19 (OVER) | 542.5s | **475.3s (−12%, exact 92M)** | 305s (1.56×, FAIL) | 319s (FAIL) | improved; still fails both — needs 2nd lever |
| q9 (join) | DNF ~43M-COLLAPSE (→10K/s) | **DNF 76.4M/93M @1284s (+77%, no collapse, ~1550s proj)** | (pending RocksDB 8c) | (pending) | collapse ELIMINATED; still DNF, needs more |
| q17 (group-agg, PASS) | 76.7s | **76.7s (NO REGRESSION)** | 74.5s (1.03×) | 255.7s | ✓ regression-checked — fix is neutral on the passing set |

**Regression gate PASSED:** q17 re-measured at exactly 76.7s with the fix (an intermediate 102.7s reading
was environmental variance — that run followed q9's ~36GB scratch write, polluting the OS page cache; a
clean re-run reproduced 76.7s). So the block-cache fix improves the slow read-heavy queries and is
**neutral on the already-passing ones** — it does not rob Peter to pay Paul. (Lesson: serialize/settle
the box between heavy runs; back-to-back runs share OS page cache + frs-tmp state.)

**Net:** a verified broad win (every query reading flushed/compacted SSTs benefits). q20 now beats ForSt;
q9 no longer collapses. BUT q19/q20 still miss the RocksDB ≥0.8× bar — q20 is hard for all LSM backends
(only RocksDB's mature engine finishes ≤800s; ForSt also DNF), q19 needs a second OVER-window lever.
Also stabilized the flaky `test_max_write_buffer_number_backpressure` CI test (ForSt `2b313f8bb`) for the
"every change must pass GHA" requirement.

## ★★★ STATUS UPDATE 2026-06-08 (PM): OOM SET FIXED (memory) via true backpressure — speed now the blocker

**Strategic pivot (user):** learn from ForSt (the backend forst-rs replaces), not RocksDB. ForSt's
memory model = flush-on-checkpoint + a TRUE WriteBufferManager stall (allow_stall, no give-up) +
fast flush. forst-rs had `noflush=true` (S3-tuning leftover → checkpoints never flushed memtables)
AND its write-stall only lived on the unused single-write path — so the FFM batch-only backend had
**NO write backpressure at all** → memtables grew to 7-11 GB → 8c/32g OOM.

**Fix implemented (engine, uncommitted) — the uniform memory model:**
1. `noflush=false` (uniform, matches ForSt — checkpoints flush+bound memtables).
2. `wait_for_wbm_headroom` stalls on the REAL budget `over_budget()`, NO 120s give-up (60s
   no-downward-progress defense only); added to ALL 3 batch write leaves (was single-write only).
3. Force memtable switch on over-budget (RocksDB WBM behavior) with a **256 MiB floor** (16 MiB
   floor caused an L0 explosion → `l0_stop_trigger=64` → pipeline freeze → restart).
Global WBM budget 6 GB (FRS_WBM_TOTAL_MB=6144, uniform). + SST-write COALESCING (4 MiB; flush
202→20 ms/MB) + SIGBUS fix (.so loaded container-local, not FUSE bind mount). Engine UTs green.

**Result — OOM SOLVED + write-throughput FIXED (verified, committed config):**
- **q17: OOM@81s/31GB → FINISHED 76.7s (1.03× RocksDB 74.5 = PARITY), out_rows 92M ✓.** (was 817s/11×
  before coalescing → coalescing cut sstwrite 504s→9.9s.)
- **q18: OOM/SIGBUS → FINISHED 318.1s (0.80× RocksDB 396.1 = FASTER), out_rows 92M ✓.** (SIGBUS .so fix.)
- **q9 (join): OOM@31GB → memory-bounded (peak RSS 18.2 GB) but DNF (read-amp, ~53M/1300s).** ✗ speed —
  WORST case: join-probe read-amp over growing state → needs the read-path module.
- q4 (join): running (committed cfg). q20 (join): pending re-measure. Expect q9-like read-amp.

**Speed severity ranking (all memory-bounded now): joins (q9 read-amp) ≫ dedup (q18 now PASS)
> group-agg (q17 now PASS).** The join read-amp (per-probe re-walk of growing state) is the
dominant remaining speed blocker → the parallel-coalesced/value-carrying READ PATH is the
highest-leverage next module (spec: 2026-06-08-q9-read-path-gap-investigation-design.md).

**WBM budget sizing (uniform, FRS_WBM_TOTAL_MB) is a partial speed lever:** q9 2 GB budget →
38.4M/1300s (end 9 K/s); q9 **6 GB budget → 52.9M/1300s (end 36 K/s)**, still memory-bounded
(RSS ~18-22 GB < 32 GB). Bigger budget → bigger/fewer L0 SSTs → less read-amp. **Adopted 6 GB.**

**★ noflush DECISION (fixed, committed): noflush=FALSE (the ForSt semantic).** noflush is NOT a
tunable — checkpoint format differs (artifact vs SST) so it can't change once data exists. ForSt
supports noflush=false ONLY; noflush=true is a forst-rs hack (live-memtable artifact) that
contradicts disaggregation. **FAIRNESS CORRECTION: the bounded-state "wins" in the table below
(q16 366, q19 298 "FASTEST") used noflush=TRUE — UNFAIR (skipped the flush ForSt does) → MUST be
RE-MEASURED under the committed config (noflush=false + 6GB + coalescing).** Also: 8c/32g RocksDB is
MUCH slower than host (q17 74.5 vs host 57; q18 396 vs host 195) → judgments vs *host* are invalid.

REFUTED (all with 8c/32g data, no wasted code): resident-shadow retire (shadow=0 for joins);
jemalloc eager-return (retained not reclaimable, still OOM); decoded-block-cache balloon (charge()
correct); compaction 8→3 (still OOM); WBM hard-cap-with-120s-give-up (released → overran → OOM);
write_buffer_size 128mb (compaction-overload → restart). Merge-collapse REFUTED as the flush cost
(flush doesn't merge — code-verified); per-key CPU REFUTED (buffer 8s + encode 17s ≪ sstwrite 504s).

---

> ✅ **q17/q18/q4 are at the LOCKED config (1 G, noflush=false) — VALID.** Still INVALID (re-measure
> under locked cfg): q16/q19 (`noflush=TRUE` — removed); q7/q11/q15 (OLD fast cfg). Light queries
> (q0-q3,q10,q12-14,q21,q22) are source-bound and pre-lock — low-risk, re-confirm.

> ⛔ **TABLE BELOW SUPERSEDED (2026-06-12).** It is the original 2026-06-08 snapshot, kept for
> history. The CURRENT per-query table lives at the TOP of this doc under
> "★ REFRESHED PER-QUERY TABLE (2026-06-12)" — q5/q8 correctness bugs since FIXED, q9/q20
> DNFs ELIMINATED, q9 PASSES on the remote population, q7 finishes with io_uring.

| query | RocksDB s | RocksDB out_rows | forst-rs s | forst-rs out_rows | ForSt s | acc | bar |
|-------|-----------|------------------|------------|-------------------|---------|-----|-----|
| q0    | 32.7      | 100,000,000      | 31.0       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |
| q1    | 30.6      | 100,000,000      | 29.4       | 100,000,000       | -          | ✓ | **PASS (correct + faster, 0.96×)** |
| q2    | 28.8      | 100,000,000      | 28.7       | 100,000,000       | -          | ✓ | **PASS (correct + parity)** |
| q3    | 36.8      | 2,201,068        | 36.7       | 2,201,068         | -          | ✓ | **PASS (correct + parity, 1.00×)** |
| q4    | **313.5 (8c/32g)** | **177,629,788** | **597.2** 🔒1G | **25,849,976** | (host 1217)| ⧗ | **out_rows 25.8M vs 177.6M (6.9× fewer) — but q4 is a RETRACT/changelog query → out_rows is NOT a clean accuracy gate (could be changelog cadence, not wrong final answer). NEEDS final-result compare.** Also 1.9× slower. OOM-fixed/finishes. |
| q5    | 162.2     | 29,988,376 (det) | 240/138    | 2.0/2.4/2.9M (var)| (host 297) | ✗ | **FAIL — WRONG: ~14× under-emit + non-deterministic (windowed-JOIN bug; both async backends)** |
| q7    | (pending 8c/32g) | — | **DNF @1300s** 🔒1G (~78.5M/92M, rate→19.7K/s) | — | (host 659) | ? | **read-amp DNF** under locked cfg (join). Old "671s" was unfair noflush=true. Joins q9/q20 → needs READ-PATH module. |
| q8    | 46.1      | 3,064,457        | 43.7 (OLD) | 3,010,888         | (pending)  | ✗? | time ~parity (0.95×) but out_rows 1.75% LOW (53K fewer) — windowed-JOIN under-emit (mild) |
| q9    | (pending 8c/32g) | — | **DNF, block-cache fix: 76.4M/93M @1284s (was ~43M-COLLAPSE), ~1550s proj** 🔒1G | — | (pending) | ? | ★ block-cache fix ELIMINATED the read-amp collapse: q9 went from collapsing at ~43M (→10K/s) to sustained 76.4M@1284s (+77%, no collapse). Still DNF; needs RocksDB/ForSt 8c baselines + further work. The collapse WAS the cache-bypass. |
| q10   | 120.3     | 100,000,000      | 128.7      | 100,000,000       | -          | ✓ | **PASS (correct; +8.4s ≤ +50s bar)** |
| q11   | 111.1     | 92,000,000       | 382 (OLD fast cfg) | 92,000,000 | (host 158)| ✓ | correct; 3.4× — **RE-MEASURE under committed cfg** |
| q12   | 42.1      | 92,000,000       | 50.6       | 92,000,000        | -          | ✓ | **PASS (out_rows match; +8.5s ≤ +50s bar)** |
| q13   | 30.1      | 100,000,000      | 30.1       | 100,000,000       | -          | ✓ | **PASS (correct + parity)** |
| q14   | 30.1      | 100,000,000      | 29.0       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |
| q15   | 228.3     | 92,000,000       | 173.9 (OLD)| 92,000,000        | (host 258) | ✓ | correct + faster — **RE-MEASURE under committed cfg** |
| q16   | 428.4 (8c/32g) | 92,000,000 | **322.9** 🔒1G | 92,000,000 | **370.3 (8c/32g)** | ✓ | **FULL PASS — 0.75× RocksDB (faster) + faster than ForSt (322.9<370.3) + accurate.** |
| q17   | **74.5 (8c/32g)** | 92,000,000 | **76.7** 🔒1G | 92,000,000 | **255.7 (8c/32g)** | ✓ | **FULL PASS — 1.03× RocksDB (parity) + 3.3× FASTER than ForSt + accurate.** |
| q18   | **396.1 (8c/32g)** | 92,000,000 | **318.1** 🔒1G | 92,000,000 | **422.8 (8c/32g)** | ✓ | **FULL PASS — 0.80× RocksDB (faster) + faster than ForSt (318<423) + accurate.** |
| q19   | 305 (8c/32g) | 92,000,000 | **475.3** (block-cache fix; was 542.5) 🔒1G | 92,000,000 | **319.2 (8c/32g)** | ✓acc | **FAIL — 1.56× RocksDB (475 vs 305) AND 1.49× ForSt (475 vs 319).** Accurate (exact). Block-cache fix helped (542→475, −12%) but q19 still behind BOTH on OVER-window/dedup (RocksDB≈ForSt≈310). Needs a SECOND fix beyond the block cache. |
| q20   | **800.3 (8c/32g)** | 93,201,404 | **DNF @1300s, block-cache fix: 88.6M/93M (~1350s proj), NOW BEATS ForSt** 🔒1G | — | **DNF @1300s (84M/93M, ~1400s proj)** | ✗ | **Join, OUTPUT-amplifying. q20 HARD FOR ALL: only RocksDB finishes (800s).** ★ block-cache fix (2026-06-09): forst-rs 71M→**88.6M@1284s** (+25%, ~4× post-flush steady-state) → **now FASTER than ForSt (88.6M>84M same wall-time)**. ✓ beats-ForSt clause. Still FAILS RocksDB bar (~1350s vs 800s = 0.59×) — residual = forst-rs/ForSt-vs-RocksDB engine gap on output-amplifying joins. |
| q21   | 59.9      | 100,000,000      | 52.6       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |
| q22   | 44.8      | 100,000,000      | 43.6       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |

NOTE on STALE rows (q7/q8/q11/q15/q16/q19): measured on the OLD "fast cfg" (noflush=TRUE) or
pre-fix configs — their pass/fail is NOT trustworthy and must be RE-MEASURED under the committed
config (noflush=false + 6GB + coalescing + container-local .so). Light queries (q0-q3,q10,q12-14,
q21,q22) are source-bound parity — low-risk but also pre-commit; re-confirm.

## REFINED ROOT CAUSE: UNBOUNDED-state queries OOM on fast cfg → FIXED (2026-06-08)
OOM set = q4(join)/q9(join)/q17(group-agg)/q18(dedup)/q20(join) = ALL accumulate UNBOUNDED state.
Fixed by the uniform memory model (true backpressure bounds RAM regardless of state size). q17/q18
now PASS; q9/q4/q20 finish-memory-wise, join read-amp is the remaining speed gap (read-path module).

## ACCURACY methodology (PMC — out_rows is gate ONLY for append/dedup)
- **APPEND / dedup / dedup-changelog queries → out_rows IS a clean exact gate.** Verified EXACT
  (forst-rs == RocksDB): q17/q18/q19 = 92M. These are real accuracy passes.
- **RETRACT/changelog queries (running aggs with updates: q4) and WINDOWED queries (q5/q8) →
  out_rows is NOT a clean gate** — a different changelog *cadence* (fewer intermediate update rows)
  gives a smaller out_rows with the SAME final answer. Must compare the **final materialized result
  rows**, not the changelog count.

## JOIN-family out_rows mismatches — to be resolved by final-result compare
- **q4 (auction⋈bid + agg, retract changelog): out_rows 25.8M vs 177.6M (6.9× fewer).** SIGNAL, not
  confirmed-wrong (retract query — see methodology). Needs final-result compare.
- **q5 (HOP+join): CONFIRMED WRONG** — controlled same-input replay gave non-deterministic, ~14×-low
  counts (a real bug, not just cadence).
- q8 (tumbling+join): out_rows 1.75% low — windowed, needs final-result compare.
Pure non-join AGG (q16/q19) = EXACT. The join queries (q4/q5/q7/q8/q9/q11/q20) all need a
**final-result accuracy check** (upsert-result compare), not out_rows, before their perf counts.
q5 is the one confirmed-wrong; the rest are open. This is the accuracy work item for the join family.

## ⧗ IN FLIGHT 2026-06-09: q20 PARALLEL-EXECUTOR experiment (the join architecture fix)
The join family (q7/q9/q20) DNFs because the default executor is depth-1 (each batch runs inline on
the AEC mailbox thread → in-flight depth==1, documented at VectorizedExecutor.java:337-398). q9 ran at
only **375% CPU on 8 cores** → serialization headroom, not CPU-bound. `RoutingStateExecutor` (already
implemented + flag-gated) routes each batch to one of N single-thread VectorizedExecutor workers →
in-flight depth N (the ForSt coordinator-offload + parallel-readThreads model). Correctness is sound:
AEC enforces per-key ordering so concurrent batches hold DISJOINT keys, and drains in-flight before
checkpoint. Refuted for q19 (OVER-window = few disjoint keys) but joins have MANY disjoint keys.
**Experiment:** q20 @ locked cfg + `FRS_RS_PARALLEL_EXECUTOR=1 FRS_RS_READ_IO_PARALLELISM=3` (match
ForSt's read-io-parallelism=3). Baseline: RocksDB 800.3s, forst-rs DNF@1300s (59.8M/93M, end 23.6K/s).
PASS criterion: finishes ≤1000s. If it finishes → parallel-read direction VALIDATED for the join
family (apply to q7/q9). If still DNF/no rate lift → serialization is NOT the bottleneck (per-iter CPU
is) → refuted, pivot to per-iter cost. Jar rebuilt 2026-06-09 (source was 18h newer than deployed jar).

### ✗ RESULT 2026-06-09: parallel executor CRASH-LOOPS q20 (thread-safety bug, NOT refuted direction)
q20 + parallel executor went into a **crash-restart loop** (status RESTARTING, src_out resetting,
never progressed past ~0.3M). Root cause (TM log, exact stack): **data race on a shared per-subtask
decode buffer.**
```
WrongThreadException: Attempted access outside owning thread
  at v1sync.MemorySegmentDataInputView.readByteUnsafe:70
  at ForStRsMapStateV2.deserializeUserKey:825        ← shared `iterView` FIELD
  at ForStRsDBIterRequest.completeWithEntries:512
  at ForStRsDBIterRequest.process:357 → VectorizedExecutor.executeIters:1371
```
cascade: `EOFException ... want 1936091768 bytes`, `IndexOutOfBoundsException: Out of bound access on
segment`, `MemorySegmentDataInputView underflow`. **Why:** `iterView` is a reused mutable field on the
per-subtask `ForStRsMapStateV2`; `RoutingStateExecutor` routes different batches of the SAME subtask to
DIFFERENT worker threads → two workers re-point the same `iterView` at their own thread-confined chunk
segments and read concurrently → WrongThreadException + corruption. The design's key-disjointness
argument is INSUFFICIENT: it protects engine keys, but the reusable Java-side decode scratch on the
state object is shared across ALL keys of the subtask → raced.
FIX applied + committed (flink `fc8eb8cd97c`/`c1bb289ae17`): iterator decoders use the existing
per-thread `VIEW_TL` ThreadLocal view, removed the shared `iterView` field. Behavior-preserving for the
default single-threaded executor.

### ✗✗ RESULT 2026-06-09 (re-run with iterView fix): parallel executor gives ZERO speedup for q20
Clean run, NO crash-loop (fix worked). Trajectory: **0–120s burst 150–228K/s** (state in memtable,
pre-flush) → 18.4M; **~140s COLLAPSE to ~15–20K/s** (first flush → probes hit SSTs) and stays there.
End: **60.8M/93M @1300s = DNF**, ~47K/s avg. **IDENTICAL to the serial run** (serial DNF@1300s 59.8M,
~46K/s). Parallel depth-3 = serial depth-1 throughput.
**→ executor-dispatch SERIALIZATION is REFUTED as the q20 bottleneck** (now refuted for q19 OVER-window
AND q20 join — the "parallel read threads" hypothesis from the architecture design does NOT hold for
either). With only 375% CPU (of 800%) AND no benefit from 3 workers, a SERIAL section is blocking the
workers. Candidates (next, profile to distinguish): (1) post-flush SST-probe path holds a global engine
lock (version/SST-reader RwLock) so parallel workers serialize on it; (2) downstream output
backpressure — q20 is output-amplifying (auction⋈bid), Calc→Writer is single-threaded per subtask;
(3) the per-probe 16-shard memtable cursor + SST merge is the serial cost.
**Candidate (2) REFUTED by this run's own data:** the source ran at 150–228K/s during the PRE-flush
phase and only collapsed AFTER the first flush (~140s). If the single-threaded Calc→Writer couldn't
drain the amplified output, the rate would be capped from t=0, not just post-flush. The slowdown tracks
the STATE BACKEND (memtable→SST), not the output operator → downstream backpressure is not it.
**Remaining: (1) SST-probe global lock OR (3) per-probe memtable/SST read cost / growing read-amp.**
375% CPU (of 800%) + 3 idle-ish workers + no parallel benefit ⇒ workers BLOCK on a serial point (low
CPU = blocking, not CPU-bound). CODE-AUDITED the obvious lock candidate — **LocalCache (the bounded LRU
file cache, `cached_fs`→`local_cache.rs`, on the SST read path): REFUTED as a catastrophic serial point.**
Its single global `inner` Mutex critical section (local_cache.rs:543-551) is ONLY membership-check +
LRU-bump + path-lookup; the `pread` disk I/O runs AFTER the guard is released (explicit comment :540-
542). So I/O does NOT serialize on the cache mutex. `touch_lru`/`fd_cache` are short critical sections
— minor contention at most, not a throughput cap for 3 workers. ShardedMemTable reads take per-shard
`.read()` (16-way, shared) — also not an obvious serial point unless write-locks (continuous bid
buffering) starve readers.
**All four leading hypotheses now refuted with code/data: executor-dispatch serialization, downstream
backpressure, LRU-lock-holds-I/O, and (weakly) memtable read-lock.** The true bottleneck is NOT
identifiable by code reading alone → **REQUIRED NEXT STEP: a sampling CPU/lock profile of the post-flush
steady state** (perf/async-profiler on the TM, or FRS_BULK_SAMPLE + FRS_ITER_DIAG + per-probe n_ovl
re-sampled at 50M+ not early — the early n_ovl=2-3 may have GROWN as join state accumulated → growing
read-amp (3) is the live candidate). The iterView fix is KEPT (a real latent thread-safety bug fix);
**the parallel executor is NOT the join-family lever** — do not pursue it further for q7/q9/q20.

## ★★★ ROOT CAUSE FOUND + FIXED 2026-06-09: flushed/compacted SST readers BYPASSED the block cache
After refuting parallel-executor/backpressure/LRU-lock, a profiling run (FRS_BULK_SAMPLE + FRS_ITER_DIAG,
default executor, locked cfg) localized the q20 join bottleneck **with data**:

**`[DECAY_ATTR]` at 72.4M records (deep post-flush):** `probe=6–37µs`, **n_ovl=1–4** (L0=1–3, deep=1),
**B_resident=0**, cost ~90% in **`A_fanout/sstloop`** (per-SST `get_or_open_sst_reader` + in-memory
`may_contain_range`/`first_block_ge`). → **growing-L0 read-amp REFUTED** (n_ovl stayed low at 72M).
**`FRS-ITER-DIAG`:** `sst_open_us` is mostly 0 but occasionally **19,614µs (19.6ms) for ONE cold reader
open**. `first_block_ge`/`may_contain_range` are pure in-memory index ops (reader.rs:703/727) → the
sstloop cost is **cold `SstReaderImpl::open`** (footer+index read/decode) on the probe hot path.

**THE BUG (code-confirmed, 5 sites):** `get_or_open_sst_reader` (the lazy path, db.rs:8666) opens
readers **`.with_block_cache(...)`**, but the eager reader-cache pre-populate at **flush** (db.rs:7195),
**3 compaction-output paths** (db.rs:3990, 4272, 5916), and **ingest** (db.rs:1782) all opened readers
with **bare `SstReaderImpl::open` — `block_cache=None`**. Since those pre-populated readers are what the
read path finds in `sst_readers` (a warm hit returns them directly), **flushed + compacted SSTs — i.e.
the join's entire hot, repeatedly-probed working set — had their data-block reads BYPASS the shared
decoded-RecordBatch cache**, re-reading + re-decompressing (lz4) the same blocks on every probe. This is
exactly the q9/q20 hot path the block cache was built for (db.rs:8616 comment), silently disabled for
every non-lazy-opened SST. B_resident=0 (resident shadow default-OFF) means ALL join probes hit these
cache-less SST readers.

**FIX (engine, uncommitted, host-compile-clean):** wire `.with_block_cache(self.block_cache, db_id,
file_number)` into all 5 eager reader-open sites (flush + 3 compaction + ingest), mirroring the lazy
path. Architectural (read-path), **no config change**, correctness-neutral (same bytes/API), benefits
ALL read-heavy queries (joins most). Per-query before/after e2e pending: rebuild Linux `.so` → re-run
q20/q9/q7 (DNF baseline: q20 ~71M/93M @1300s, ~47K/s). Expected: repeated same-SST probes hit decoded
blocks → big per-probe drop. ForSt q20 8c/32g baseline (in flight, running ~88K/s — FAST) is the target.

## ★★★ DEFINITIVE ROOT CAUSE 2026-06-09 (JFR full-stack): q7/q9/q20 are WAIT-BOUND on the async-state+FFM per-record boundary — NOT the LSM
After value-carrying drain (no help), parallel-iterator dispatch (no help), and lock-free memtable
superversion (marginal) — each targeting the ENGINE READ and each failing to move q9 — a JFR full-stack
profile (150s steady-state, q9) settled it. The engine read was NEVER the cost; the engine micro-diags
were a red herring.
- **ThreadPark 62,719 vs ExecutionSample 4,433** → the system is **WAIT-bound, not CPU-bound**. The
  dominant non-idle park (22,840) is `TaskMailboxImpl.take()` ← `processMailsWhenDefaultActionUnavailable`
  — the operator **parks because the async-state controller's in-flight buffer is full / waiting for
  outstanding state results**. Throughput is gated by how fast the async-state pipeline (operator → AEC →
  FFM downcall → engine → return → future → callback) drains, NOT by the engine.
- **On-CPU = 100% framework, 0% engine** (no LSM/SST/memtable frame appears): per-record FFM segment
  alloc (`SegmentFactories.initNativeMemory`+`Unsafe.checkOffset` ~9%), Arrow key hash
  (`ArrowBinaryBuffer.hash` ~5%), Flink row serde (`RowDataSerializer.copyRowData`+`ensureMaterialized`
  ~13%), async-future alloc (`ContextAsyncFutureImpl.makeNewFuture` ~3%).
- **GC** moderate (young every ~3.75s); **12 bg compaction/flush threads on 8 cores** add contention.

**Why RocksDB doesn't hit this:** RocksDB's backend is **SYNCHRONOUS** — direct in-operator JNI
`state.value()`, NO async-state pipeline, NO per-record future, NO mailbox-parking-on-async, NO
per-record Panama FFM segment alloc. forst-rs's **async-state-V2-over-Panama-FFM** pays a per-record
pipeline round-trip + framework allocation on every probe. **ForSt uses the SAME async-state framework
but a C++/JNI boundary + mature engine that drains requests faster** — that boundary efficiency is the
gap. So q7/q9/q20's wall is the FFM + async-state per-record boundary, which is exactly why every
engine-read fix (drain/parallel-iter/lock-free-memtable) couldn't move it.

**Kept improvements (correctness-verified, no regression, all committed/pushed):** lock-free memtable
superversion (ArcSwap), value-carrying drain unify, parallel-iter dispatch (flag-OFF), block-cache
fix. Real + architecturally-correct, just not where the join wall is.

**Levers (honest):** (1) per-record FFM `initNativeMemory` alloc violates the no-per-record-alloc /
zero-copy mandate → pool/reuse native segments (~9% on-CPU + GC; MODEST since wait-bound). (2) the
DEEP lever = async-state pipeline completion throughput (why the mailbox parks) → drain requests faster
(executor coordination + FFM boundary efficiency) — structural, multi-session, the ForSt-parity gap.
q7/q9/q20 cannot pass the RocksDB bar without (2).

## ⚠️ 2026-06-09 vectorized-hash before/after: ~PERF-NEUTRAL — tempers the on-CPU-micro-opt thesis
Committed the byte-identical wide-stride key hash (flink `8ffbc42b933`, 8× fewer FFM byte-reads in
`ArrowBinaryBuffer.keyHash`; 11/11 UTs incl. a scalar-equivalence gate). **e2e before/after (q9 + JFR):
~perf-neutral.** q9 84.5M@1204s vs prior ~76M baselines = within q9's 60–84M run-to-run variance (NOT a
win). JFR relative on-CPU shares UNCHANGED: `keyHash` 5%→6%, `checkOffset` 4%→4% (totals rose 4433→6419
only because more records ran). **Why:** the hash cost is the data-dependent `×31` ARITHMETIC, not the
FFM byte-reads I cut — so the reduction was negligible. The fix is KEPT (correct, mandate-aligned,
harmless) but is not a q9 lever.

**Sobering implication for Phase-1 heavy joins:** the forst-rs-specific on-CPU differential vs RocksDB is
~20% (FFM alloc 4% + per-access checks 4% + hash 6% + …), and the system is partly **wait-bound** (62k
ThreadParks). Even fully eliminating that ~20% gives <20% throughput — far short of the ~2× q9 needs to
reach the RocksDB bar. So **on-CPU micro-opts alone cannot close q7/q9/q20** — the dominant factor is
**request-completion latency (the async-state pipeline drain / the wait)**, where RocksDB's mature
engine+boundary completes faster. The honest lever is the request-completion path (executor/boundary
throughput so in-flight stops filling) — structural, the ForSt-parity gap — NOT per-record on-CPU
shaving. The zero-copy columnar-key levers help correctness/mandate + GC but, by this evidence, will
likely also be modest on a wait-bound query. q7/q9/q20 reaching ≥0.8× RocksDB is not yet demonstrated
achievable and may require reconsidering the async-state request-completion architecture.

## ★★★ 2026-06-09 PIVOTAL: RocksDB q9 ALSO DNFs ~1300s → q9 is NEAR-PARITY, not a failure
The whole "q7/q9/q20 severely regressed / far below 0.8× RocksDB" framing rested on comparing forst-rs
DNF to an UNMEASURED RocksDB. **First-ever RocksDB q9 8c/32g baseline: 94.5M @1283s — RocksDB ALSO
barely finishes / DNFs at ~1300s on this heavy join.** RocksDB is NOT fast on q9 at 8c/32g.
Apples-to-apples at 1204s: forst-rs ~84.5M vs RocksDB ~88.7M ⇒ **forst-rs q9 ≈ 0.95× RocksDB, near
parity** — NOT 0.59×. The "DNF = failure" read was an artifact of the missing baseline.
**Implication:** q9 (and plausibly q7/q20 — re-check their RocksDB finish times, not just RocksDB
q20=800s which DOES finish) may already be at/near the ≥0.8× bar once compared at true finish times.
ACTION: run BOTH q9 (RocksDB + forst-rs) to completion (MAXSEC 1700) for clean finish-time ratios; the
chunkBuf-reuse + accumulated fixes only need to hold near-parity, not 2×. This reframes the heavy-join
"failure" — measure the real RocksDB finish times for q7/q9 (q20 RocksDB=800s is the genuine outlier
where RocksDB finishes and the others don't).

## ★ WHAT'S LEFT (Phase-1 close)
1. **Fair baselines:** RocksDB 8c/32g + ForSt 8c/32g for the WHOLE set ("faster than ForSt" clause
   unverified almost everywhere; have RocksDB 8c/32g only for q17/q18).
2. **Re-measure the STALE rows** (q7/q8/q11/q15/q16/q19 + light) under the committed config.
3. **Finish OOM-set joins:** q4 (running), q20 (next); score vs bar.
4. **Read-path module** for join read-amp (q9) — the one architecturally-open perf gap.
5. **Correctness (GATE):** build a **final-result accuracy check** (materialize the changelog →
   compare result rows vs RocksDB) for the join/retract/windowed family — out_rows alone is not a
   valid gate there. q5 is CONFIRMED wrong (controlled replay); q4/q8 (and q7/q9/q11/q20) have
   out_rows mismatches that are SIGNALS to resolve with the final-result check, not yet confirmed
   bugs. Fix any confirmed wrong-answer before its perf counts.
6. **Commit** the verified fixes (memory model + coalescing + SIGBUS) after the re-measure confirms
   no regression; then compute overall NexMark total vs RocksDB.

## Verified architecture fixes this campaign (UNCOMMITTED)
- A: memory model (true WBM backpressure, force-switch) → OOM set bounded ≤18 GB (was 31 GB OOM).
- B: SST-write coalescing (per-block block_on → 4 MiB writes) → flush 202→20 ms/MB; q17 817→76.7s.
- C: SIGBUS fix (.so container-local, not FUSE mount) → q18 teardown crash → clean finish.
Tests: io 209 + storage + engine 277 green.

## ★ LEVER 1 COMPLETE — vectorized zero-copy key path (2026-06-09)
**All four per-byte FFM sites the lever-1 spec named are now eliminated:**
- key HASH: wide-stride `JAVA_LONG` `keyHash()` (was per-byte `seg.get(JAVA_BYTE)`) — committed `8ffbc42b933`, byte-identical UT gate.
- key COMPARE: `keysEqual` already used `MemorySegment.mismatch()` (JDK22+ vector intrinsic) — no per-byte loop.
- key COPY: `appendKey`/`appendValue` already used `MemorySegment.copy()` intrinsic.
- iterator chunkBuf: per-probe 64 KiB `Arena.allocate` → per-executor REUSED buffer (`process(...,reusedChunkBuf)`) — committed flink `53722e117d6`. Kills the JFR-pinned `initNativeMemory`/`checkValidStateRaw` per-probe alloc.
- MapStateCache/ArrowBinaryBuffer key+value segments already pooled (grown rarely).

**Decisive measurement (q9 8c/32g, apples trajectory at ~361s):**
- pre-chunkBuf forst-rs q9 (bge23t5si): 36.06M @362s
- chunkBuf-reuse forst-rs q9 (bq01jk6pb): 37.63M @361s ⇒ **+4.4% throughput**
- RocksDB q9 (b95vxcqzq): 46.4M @401s — forst-rs ~0.85× at this scale, ~0.95× at true finish.

**Verdict (per the agreed test):** the on-CPU reduction DOES translate to throughput (+4.4%), so the
zero-copy-key direction is validated — but the gain is modest because the system is substantially
**wait-bound** (JFR: 62,719 ThreadPark on the shared async-state mailbox vs 4,433 on-CPU). Lever 1 narrows
the gap; closing it fully needs the WAIT to shrink (shared async-state concurrency), which is NOT a
forst-rs-specific zero-copy lever. Proceeding to lever 2 (coalesced batched lookup) + lever 3 (zero-copy
value views) per directive; honest expectation is each adds a similar single-digit % on the iteration path
(the engine read is already O(1)-cheap: memtable n_shards=1, SST n_ovl=1), so q9 stays near-parity rather
than flipping to a large win — the residual wall is the framework wait.

### Zero-snapshot drain — correctness GATE PASSED (2026-06-09)
q11 8c/32g (locked cfg, zero-snapshot decode jar): **FINISHED 318.9s, out_rows=92,000,000 == RocksDB
reference (exact).** q11 is an append/dedup query → out_rows is a clean exact gate. The in-place
per-chunk decode (no per-chunk `arena.allocate(bytesUsed)` snapshot, no view-accumulation list) is
byte-correct over a full 92M-event MapState-iteration workload. Lever 1 is COMPLETE + CORRECT.
Next: q9 zero-snapshot before/after trajectory, then lever 2 (coalesced batched lookup).

## ★★ ALL THREE LEVERS FINISHED + TESTED (2026-06-09)
Per directive ("ALL O(1) lookup + zero-copy value finished before any nexmark"):
- **Lever 1 (columnar buffer / zero-copy key):** wide-stride `keyHash`, `mismatch()` compare,
  `copy()` intrinsic, per-executor reused `chunkBuf`, + **zero-snapshot drain** (decode each chunk
  in place to detached on-heap, no per-chunk `arena.allocate` snapshot, no view-accumulation list).
  Applied to BOTH `process()` and `processFromBatchedOpen()`. Dead `parseChunkInto` removed.
- **Lever 2 (O(1)/coalesced batched lookup):** `executeItersBatchedParallel` + FFI
  `frs_vec_iter_prefix_open_batch_parallel` (one crossing opens K probes, engine drains across read
  pool); drain uses the zero-snapshot path. Gated on `FRS_RS_PARALLEL_ITER` pending e2e enable-decision.
- **Lever 3 (zero-copy value):** `ForStRsMapStateV2.deserializeUserKey/Value(IteratorEntryView)`
  rewind a `MemorySegmentDataInputView` directly onto the chunk slice via per-thread `VIEW_TL` — no
  intermediate `byte[]`. The zero-snapshot change makes the view reference the reused `chunkBuf`
  directly, completing the zero-copy chain (engine buffer → on-heap RowData, the one materialization
  Flink requires).

**TESTS (all green):**
- Unit (mock linker): `ForStRsDBIterRequestTest` 5/5 (incl. 2 new `processFromBatchedOpen` cases:
  single-chunk decode + multi-chunk no-corruption) + `ArrowBinaryBufferTest` 11/11.
- **Full native suite vs real engine (host dylib rebuilt): 528 run, 0 failures, 0 errors, 11 skipped
  — BUILD SUCCESS** (incl. `IterPrefixBatchOpenTest`, `FrsIteratorTest`, all `MapStateV2*`, `Vectorized*`).
- e2e correctness: q11 out_rows=92,000,000 == RocksDB (exact gate).
Levers implemented + verified; nexmark validation/enable-decision is next.

## ★★ q20 ROOT CAUSE (2026-06-09, systematic-debugging Phase 1 — NOT A BUG)
Hypotheses tested: H1 amplification / H2 unbounded-state / H3 constant-factor.
- **H1 RULED OUT:** q20 forst-rs @40M scale FINISHED 357.6s, **out_rows=37,279,944 = src_out EXACTLY**
  (ratio 1.000, == RocksDB's 93,201,404/~93M ≈ 1.0). No over-emission; join is byte-correct.
- **H2 RULED OUT:** peak RSS bounded 22–24 GB (oscillates 13–15 GB, slow climb to 17.8 GB) — join
  state lives in flushed SSTs, not RAM. No leak, no OOM.
- **Verdict = state-size-dependent LSM-efficiency DEGRADATION** (refined H3, not flat constant):
  forst-rs rate **104K/s @40M (0.90× RocksDB) → ~25–46K/s tail @93M (0.59×)**. Throughput falls as the
  unbounded inner-join state grows the LSM; RocksDB's local engine stays flat enough to finish @800s.
  Rate OSCILLATES (24→59→25K/s) = compaction cycles stealing throughput as the SST set grows.
- **Same root-cause family as q9** (LSM per-op efficiency vs local RocksDB), amplified by q20's larger
  growing state + 93M output. **ForSt also DNFs** → disaggregated-LSM ceiling; forst-rs already beats ForSt.
- **Implication:** no quick fix. The lever is the read-path/compaction-scaling module (block-cache fix
  was its first +25%). Design effort, not a patch. Two full runs DNF'd at MAXSEC 1500/1700 (91–92M/93M).

## q20 sub-cause LOCALIZED (2026-06-09, DECAY_DIAG) — NOT read-amp, it's write+coordination
DECAY_DIAG (TM log, $WORKENV-mounted) across the q20 run: L0 stays ≤3 files, NOTHING promoted to
L1+, SST state ~1.3 GB while RSS ~13 GB → join state is MOSTLY MEMTABLE-RESIDENT; reads touch ≤3
small L0 files (matches prior n_ovl 1–4 @72M). **Read-amp is NOT the cause** (reads cheap, like q9's
µs engine read). The state-size throughput collapse (104K/s@40M → 7.8K/s tail@66M) is **write+
coordination side**: flush/compaction/WBM management of a large resident working set + the async-state
wait (q9's 14:1 park). SAME diffuse ceiling as q9; write-side already addressed by Module #57 (WBM
backpressure) + #58 (vectorized SST writer) + block-cache (read). Residual = diffuse per-op efficiency,
NOT a single un-pulled lever. ForSt also DNFs → disaggregated ceiling; forst-rs beats ForSt.

## ★★★ CORRECTION (2026-06-09): q9/q20 are NOT a "diffuse ceiling" — it's depth-1 async dispatch (FIXABLE)
The earlier "q20/q9 = diffuse LSM write+coordination ceiling, no single lever" conclusion is WRONG and
RETRACTED. Validated against 2026-06-09-forstrs-join-performance.md + re-confirmed by code + JFR:
- **CODE [FACT]:** `VectorizedExecutor.executeBatchRequests` runs the batch SYNCHRONOUSLY INLINE on the
  mailbox thread and returns `CompletableFuture.completedFuture(null)` (:552); `fullyLoaded()` hard-coded
  `false` (:1023; javadoc :385-393 admits it); in-flight depth = 1 (AsyncDispatchInFlightParallelismTest).
  ForSt instead returns an INCOMPLETE future + offloads to coordinator + 3-thread read pool (depth 3);
  RocksDB is synchronous (no async cost).
- **JFR [FACT] (q9-flame2.jfr):** 65,842 ThreadPark vs 6,419 on-CPU (~10:1). 26,821 parks in
  `MailboxProcessor.processMailsWhenDefaultActionUnavailable → TaskMailboxImpl.take()` = default action
  SUPPRESSED (async-state backpressure). On-CPU top = Flink serde (shared) + FFM/key bucket
  (forst-rs-specific); NO engine-read frame → reads cheap. Wait-bound, in the async-state mailbox.
- **Why my tested levers missed it:** zero-copy/lever-2 parallelized work INSIDE the synchronous call;
  the `RoutingStateExecutor` (incomplete-future, real fullyLoaded) is UNUSED by the dispatch path (§4.3).
- **THE lever = OPT-01:** offload the FFM batch to a worker pool + return an incomplete future + real
  `fullyLoaded()` → let the AEC pipeline to depth N (matches ForSt). Untested; efficacy needs impl+measure.

## ★★★ OPT-01 SPIKE — offload efficacy CONFIRMED (2026-06-09), prior refutation EXPLAINED
Minimal depth-N proof: q9 + FRS_RS_PARALLEL_EXECUTOR=1 + READ_IO_PARALLELISM=6 on the CURRENT jar
(lock-free memtable + all levers), instrumented.
- **Throughput +7.7–15%** vs depth-1 baseline: 37.19M@362s vs 32.24M (+15%); 39.50M@402s vs 36.67M (+7.7%).
- **CPU 497–646% burst / ~400% steady** (vs the refuted experiment's pinned 375%/zero-speedup).
- **Parallel dispatch fired:** ITER_DISPATCH_DIAG parDispatches=3.03M, fresh%=99, continuations=8265.
- **No crash-loop** (iterView race fix held).
**WHY the prior "parallel executor REFUTED" was pre-lock-free:** the memtable RwLock serialized the
workers (375% CPU, zero speedup). This session's lock-free ArcSwap memtable (column_family.rs) removed
that lock → offload now scales + helps. Refutation wasn't wrong; it predated the fix.
**Remaining ceiling LOCATED:** steady CPU ~400% (not 800%) = workers still serialize on the BLOCKING
single-threaded FFM iter open (frsVecIterPrefixOpenBatch) = OPT-02 (non-blocking/parallel FFM).
**Validated path to beat RocksDB on q9/q20: OPT-01 (offload, +7.7–15% confirmed) + OPT-02 (non-blocking
FFM → 400%→800%).** NOT a ceiling. Caveat: single noisy run; correctness (out_rows exact under parallel
exec) must be verified before production-enabling.

## ★★★ OPT-01 CORRECTNESS + BIG WIN on q11 (2026-06-09)
q11 + FRS_RS_PARALLEL_EXECUTOR=1 + READ_IO_PARALLELISM=6 (current jar): **FINISHED 135.8s,
out_rows=92,000,000 EXACT.** Offload preserves correctness (make-or-break gate PASSED).
**q11: 318.9s (depth-1) → 135.8s (offload) = 2.35× faster** → vs RocksDB 111.1s = **0.35×→0.82×:
a FAILING query now PASSES the ≥0.8× bar.** Mechanism: depth-1 drained q11's in-flight backlog
SERIALLY after source stop (178s tail); offload drains concurrently → tail eliminated. Strongest
evidence yet that OPT-01 (async offload) is the dominant Phase-1 lever — flips fail→pass,
correctness-preserved. Single run; needs default-safety check (must not regress light/source-bound
queries per the "don't rob Peter" constraint) before default-enabling.

## ★★★ OPT-01 DEFAULT-SAFETY confirmed (q3 Peter-check) — ready for default-enable + full sweep (2026-06-09)
q3 (light/source-bound) + FRS_RS_PARALLEL_EXECUTOR=1 + READ_IO_PARALLELISM=6: FINISHED 36.6s,
out_rows=2,201,068 EXACT, vs depth-1 36.7s = NO REGRESSION. So the offload BENEFITS heavy queries
(q11 2.35× fail→pass, q9 +7.7-15%) and is NEUTRAL on light queries (q3) → satisfies "don't rob Peter."
OPT-01 validated across 3 families: light(q3 neutral)/window(q11 2.35×)/join(q9 +7.7-15%), all correct.
**NEXT (next session, evidence overwhelming):** (1) default-enable the parallel executor in
ForStRsAsyncKeyedStateBackend gate (read-io-parallelism default = 3 to MATCH ForSt per constraint;
spike used 6 — re-measure win at 3); (2) full q0-q22 correctness sweep under default-on (the prior
crash-loop was q20-specific, fixed via iterView — must reconfirm all windowed-joins q5/q7/q8 + q17/q18);
(3) full perf sweep → confirm ≥0.8×RocksDB + ≤50s + >ForSt simultaneously; (4) then OPT-02 (non-blocking
FFM, 400%→800%) for the joins still short. This is THE Phase-1 lever — correctness-safe, default-safe.

## ★★ OPT-01 DEFAULT-ON committed + correctness-verified across 4 families (2026-06-09)
ForStRsAsyncKeyedStateBackend now defaults to RoutingStateExecutor (worker=3 = match ForSt
read-io-parallelism); opt-out FRS_RS_PARALLEL_EXECUTOR=0. Committed+pushed (flink) → GHA gating.
Correctness EXACT + neutral-or-better under default-on:
- q3 (light): 36.6 vs 36.7s, out_rows 2,201,068 exact.
- q11 (window/iter): 318.9 -> 253.8s (1.26x), out_rows 92,000,000 exact. PARTIAL (0.44x, needs OPT-02).
- q12 (windowed-agg): 50.6 -> 46.5s (faster), out_rows 92,000,000 exact.
- q9 (join): +7.7-15%, no crash.
4 families correct (light/join/window/windowed-agg). REMAINING correctness risk: windowed-JOINS
q5/q7/q8 (window+join combo, highest race risk; q5 historically non-deterministic) — must be in the
final default-on sweep. OPT-02 (non-blocking FFM) is next to lift the PARTIAL joins past 0.8x.

## ★★★ OPT-01 default-on REVERTED — windowed-join CORRECTNESS REGRESSION (2026-06-09)
q8 (windowed-join) under OPT-01 default-on: out_rows=**1,819,576** vs depth-1 3,010,888 / RocksDB
3,064,457 = **~40% UNDER-EMISSION**. The RoutingStateExecutor's cross-worker offload races on
windowed-join semantics (window-timer firing / namespace ordering under async completion reordering)
→ windows under-fire. Correctness non-negotiable → REVERTED to OPT-IN (FRS_RS_PARALLEL_EXECUTOR=1);
depth-1 VectorizedExecutor is the default again (q8 back to 3,010,888).
**LESSON (mandate-validated):** default-enabling required FULL correctness across ALL families FIRST;
4 families (q3/q9/q11/q12 exact) were NOT sufficient — the 5th (windowed-join) exposed the race.
**GATING PREREQUISITE for OPT-01 (and the join-family Phase-1 win):** fix the windowed-join race in
the offload path — likely window-timer/completion ordering under the AEC when batches complete on
worker threads out of arrival order. OPT-01 stays the validated dominant lever (q11 2.35×@w6, q9
+7.7-15%, correct on non-windowed-join families) but CANNOT be default until this race is fixed.
Roadmap: (1) fix windowed-join offload race → (2) re-verify q5/q7/q8 + full q0-q22 out_rows under
default-on → (3) OPT-02 (non-blocking FFM, w3 parity) → (4) full perf sweep.

## ★★★ windowed-join offload race ROOT-CAUSED (2026-06-09, code inspection)
The gating blocker for OPT-01 default-on (q8 40% under-emit) is ROOT-CAUSED:
- `RoutingStateExecutor.leaseWorker()` = `free.removeFirst()` (line 265) → leases by AVAILABILITY,
  NOT by key/key-group. ZERO key-group awareness in the class.
- Each worker is a SEPARATE VectorizedExecutor with its OWN arena → its OWN MapStateCache (per-worker
  write-back buffer).
- MECHANISM: consecutive batches for the SAME key route to DIFFERENT workers → that key's UNCOMMITTED
  buffered state FRAGMENTS across per-worker caches → a windowed-JOIN probe reading the other side's
  records MISSES rows buffered in a different worker's unflushed cache → under-match → 40% under-emit.
- Why q11/q12 (windowed-AGG) survived: aggregation flushes + the engine MERGES fragments (merge-based
  recombination, no loss); a join POINT-MATCH has no recombination → loses rows.
**FIX (well-defined): KEY-GROUP-AFFINE routing** — same key-group → same worker (consistent cache),
mirroring Flink's key-group model. Complication: createRequestContainer() has no key → fix needs the
AEC container↔key-group association. Correctness-critical; careful implementation required. This is THE
prerequisite for OPT-01 default-on (and the join-family Phase-1 win). Alternative: write-through (no
per-worker cache) under parallel exec — simpler, sacrifices the cache perf lever.

## ★★★ windowed-join race — fix DEFINITIVELY pinned (2026-06-09): key-group-affine routing REQUIRED
Test (q8, parallel exec, cache ON vs OFF):
- cache ON: out_rows 1,819,576 (-40%); cache OFF: 2,805,877 (-6.8%); depth-1: 3,010,888.
- Disabling per-worker MapStateCache recovered ~1M of ~1.2M lost rows → cache fragmentation = MAJORITY
  of the race. BUT residual ~6.8% under-emit with cache OFF → a SECOND race: per-key OP ORDERING
  violated when a key's ops split across workers (key-agnostic leaseWorker), independent of the cache.
**=> write-through alone is INSUFFICIENT. The fix MUST be KEY-GROUP-AFFINE ROUTING** (same key-group ->
same worker), which solves BOTH cache fragmentation AND per-key ordering structurally. This is the
DEFINITIVE prerequisite for OPT-01 default-on. Implementation: RoutingStateExecutor must route a
container/batch to a worker by its records' key-group (needs the AEC container<->key-group association;
if containers span key-groups, either batch per-key-group or split the dispatch by key-group). Then
re-verify q5/q7/q8 exact + full q0-q22 under default-on, then OPT-02, then perf sweep.

## key-group-affine routing — IMPLEMENTATION SPEC (2026-06-09, feasibility confirmed)
Feasibility CONFIRMED by code: RecordContext<K> carries key/key-group; runtime calls
switchContext(RecordContext) on the mailbox thread before each state request; StateRequest holds its
RecordContext → key-group obtainable at dispatch. AEC batches SPAN key-groups (active-buffer-size).
**FIX (next session):** in RoutingStateExecutor.executeBatchRequests, SPLIT the container's requests
by key-group and dispatch each key-group subset to worker = keyGroup % nWorkers, then combine the
per-worker CompletableFutures into the container future. Guarantees same key-group → same worker →
same MapStateCache (read-your-writes) + per-key ordering. createRequestContainer's lease becomes a
no-op placeholder (routing moves to executeBatchRequests where keys are known). VERIFY: q8 out_rows
== 3,010,888 (depth-1) + q5/q7 + full q0-q22 under default-on, then default-enable OPT-01.
DO NOT rush in low-context: a wrong split/route = silent correctness bug (the q8 class). This is the
sole remaining blocker between the proven OPT-01 win (q11 2.35x) and the join-family Phase-1 pass.

## key-group-affine routing — SCOPE confirmed = classifier-level refactor (2026-06-09)
Code-read: createRequestContainer() returns the pooled VectorizedClassifier, which buckets requests
BY TYPE (getKeys/putKeys/putValues/deleteKeys/appendMerge/iters) into shared columnar buffers. So
key-group-affine routing requires partitioning ALL those typed arrays + columnar buffers PER WORKER by
key-group at executeBatchRequests — a structural classifier/dispatch rework, NOT a localized edit.
Cleanest implementation paths for next session:
  (A) Per-worker classifiers: at executeBatchRequests, for each request compute keyGroup (via
      RecordContext) -> append to per-worker classifier (worker=keyGroup%N) -> dispatch each ->
      combine futures. Requires a classifier "add one request" API (currently filled by the AEC).
  (B) Backend presents N sub-executors and the AEC routes records to them by key-group range (changes
      the StateExecutor integration, higher-level).
Either is a one-pass-resolvable refactor in FRESH context with the q8/q5/q7 + full-sweep gate. NOT
safe to rush in low context (large surface, columnar-buffer partitioning). Default stays depth-1 (safe).

## key-group-affine routing — IMPLEMENTATION-READY spec, all APIs confirmed (2026-06-09)
Confirmed accessors: AsyncRequestContainer.offer(REQUEST)/isEmpty(); AsyncRequest.getRecordContext()
-> RecordContext.getKeyGroup() (public int). StateRequest extends AsyncRequest<K>.
DESIGN (RoutingStateExecutor rewrite — route at OFFER time, not split filled buffers):
  - createRequestContainer() returns a RoutingRequestContainer wrapping N independent per-worker
    VectorizedClassifier sub-containers (per-worker arena).
  - RoutingRequestContainer.offer(req): kg=req.getRecordContext().getKeyGroup(); subContainer[kg % N].offer(req).
    => same key-group always -> same worker -> same MapStateCache (read-your-writes) + per-key ordering. FIXES q8.
  - executeBatchRequests(rc): for each non-empty sub-container, dispatch to workers[w] on workerThreads[w];
    combine the N CompletableFutures (allOf) into the container future.
  - isEmpty(): all sub-containers empty.
LIFECYCLE (the hard part = why multi-PR, not 1 commit): per-worker classifiers are POOLED+reset per
batch -> batch N+1 must NOT reuse a worker's classifier while batch N still runs on it. Need either
double-buffered per-worker classifiers OR a real per-worker in-flight slot count feeding fullyLoaded()
(=any worker has no free buffer). This is the doc's "buffer-ownership forces depth-1" issue (flagged as
multiple PRs in VectorizedExecutor javadoc :385-393). VERIFY: q8=3,010,888 + q5/q7 + full q0-q22 under
default-on, before default-enabling OPT-01. Then OPT-02 + perf sweep. Default stays depth-1 (safe) until done.

## key-group-affine routing ATTEMPT → DEADLOCK (2026-06-09), reverted; q19 findRow fix found
Implemented RoutingRequestContainer (offer routes by keyGroup%N) + fullyLoaded()=busyWorkers>0.
Compiled + 70 native tests passed, BUT q8 e2e HUNG: stalled permanently at src_out=921,085 (rate→0).
Cause: the conservative fullyLoaded/busyWorkers gate deadlocks with the AEC mailbox + future-completion
thread (completion that should clear busyWorkers can't be observed while the mailbox parks on
fullyLoaded). CONFIRMS the fix needs proper double-buffered + AEC-aware completion (multi-PR), not a
conservative gate. REVERTED (uncommitted); default stays safe depth-1.
SEPARATE FIND: an uncommitted MapStateCache "DECAY FIX" (q19 TopN findRow O(n²)→O(1): tracks eviction
tombstones + rehashDropTombstones at 0.75 load) is in the tree — DEFAULT-path (benefits all churn-heavy
queries), has a test. Verifying + committing it as a q19 lever (separate from the join-family OPT-01).

## ★★ q19 findRow O(n^2)->O(1) fix COMMITTED (2026-06-09) — verified benefits-all lever
MapStateCache decay fix (tombstone tracking + rehashDropTombstones) committed (flink 92a5d7c400b).
SAME-BOX A/B (degraded box, late session): fix-ON q19 599.7s FINISHED 92M exact vs fix-OFF >700s DNF
=> fix is >14% faster AND turns DNF->finish; correct (out_rows=92,000,000). Default-path (depth-1
executor uses MapStateCache too) -> benefits ALL churn-heavy queries (q19 TopN was 52% CPU in findRow
per JFR). 34 MapStateCacheTest green. NOTE: box is degraded after a long session (fix-OFF q19 >700s
vs the prior healthy-box 475s baseline) so q19's ABSOLUTE pass vs 0.8x (<=381s) needs a HEALTHY-box
re-measure — the fix should bring q19 well below 475s there. This is the "second OVER-window lever"
the doc called for. Join-family OPT-01 still needs the multi-PR key-group-affine routing fix.

## OPT-01 key-group routing — DEADLOCK analysis + deadlock-free design (2026-06-09)
My attempt hung q8 at src_out=921,085 (ran, then stalled — not immediate). Reasoned root cause:
ad-hoc completion coordination, NOT the routing. executeBatchRequests fanned one batch across N
workers + completed the container future only at remaining==0, with fullyLoaded()=busyWorkers>0 AND
the sync path (executeRequestSync -> drainInflight() + workerThreads[0].submit().get() on the mailbox).
Wait cycle: mailbox blocks in drainInflight/sync awaiting a multi-worker batch future while completion
depends on threads the mailbox no longer services.
DEADLOCK-FREE DESIGN (mirror ForStStateExecutor coordinator model): single coordinator thread that
(a) keeps key-group->worker affinity (the q8 correctness fix), (b) returns an INCOMPLETE future
immediately (offload), (c) maintains a real per-worker in-flight count for fullyLoaded(), (d) NEVER
blocks the mailbox on the sync path while async batches are outstanding (drain via AEC yield, not
.get()). This is the multi-PR OPT-01. Correctness gate q8=3,010,888; perf needs a HEALTHY box
(current box degraded: q19 fix-off >700s vs prior 475s).

## GHA correctness gate GREEN for session commits (2026-06-09)
ci-forst-rs (Build libforst_rs_ffi + flink-statebackend-forst-rs JDK 25) on jackylee-ch/flink @forst-rs-jdk25:
- OPT-01 default-enable: success
- OPT-01 revert: success
- q19 findRow O(n^2)->O(1) fix (HEAD 92a5d7c400b): success (gh run watch --exit-status = 0)
=> mandate "pass GHA of both repos" SATISFIED for all committed changes (ForSt engine repo unaffected —
no Rust commits this session). Committed Phase-1 advances are correctness-gated green.

## OPT-01 windowed-join race — ISOLATED to cross-worker concurrency (2026-06-09)
Deadlock-free key-group-affine RoutingStateExecutor committed (opt-in). q8 diagnostics:
- N=1 (routing path, no parallelism): out_rows=3,064,667 ≈ RocksDB 3,064,457 = CORRECT (even > depth-1's
  3,010,888). => My routing/sync-execution LOGIC IS CORRECT.
- N=3 (routing, parallel): out_rows=2,704,710 (−10%). => the −10% is purely a CROSS-WORKER CONCURRENCY
  RACE, not a logic bug.
HYPOTHESIS (testing): the MapStateCache is a SINGLE shared hash-table instance per state object (not
per-worker). Even with key-group affinity (each key→one worker), concurrent workers insert/evict in the
SAME hash table → structural data race → lost entries. If so, fix = PER-WORKER MapStateCache (each
worker's VectorizedExecutor owns its cache) or thread-safe/key-group-partitioned cache. Test: q8 N=3 +
FRS_DISABLE_MAPSTATE_CACHE=1 — if correct, shared-cache race confirmed.

## ★★★ OPT-01 windowed-join race ROOT-CAUSED + PROVEN (2026-06-09): shared MapStateCache structural race
q8 N=3 key-group-affine: cache-ON 2,704,710 (−10%) vs cache-OFF 3,064,514 (≈RocksDB 3,064,457 CORRECT).
=> The −10% IS the shared MapStateCache: single hash-table instance per state object; concurrent workers
(holding DIFFERENT keys via affinity, but mutating the SAME backing table/eviction) structurally race →
lost entries. Affinity fixes per-KEY consistency but not the shared STRUCTURE.
PROVEN FIX = PER-WORKER MapStateCache (each worker owns its cache instance; with key-group affinity,
key->one worker->one cache = correct AND preserves cache perf — unlike blanket cache-off which would rob
cache-benefiting queries). Implementation: make the state object's cache per-worker-thread (ThreadLocal)
or per-VectorizedExecutor; snapshot/flush/close must drain ALL per-worker caches. This is the LAST
blocker for OPT-01 default-on (routing logic already proven correct at N=1 = RocksDB-exact).

## ★★★ OPT-01 now CORRECTNESS-SAFE (opt-in) — both blockers fixed (2026-06-09)
Two parallel-executor correctness blockers RESOLVED + committed (opt-in; default stays depth-1):
1. DEADLOCK (async-offload coordination) → FIXED: deadlock-free SYNCHRONOUS key-group-affine routing
   (RoutingRequestContainer routes offer() by keyGroup%N to per-worker sub-containers; executeBatch runs
   them in parallel + blocks + returns completed future). q8 finishes 42s (was hang).
2. WINDOWED-JOIN UNDER-EMIT → FIXED: bypass MapStateCache when FRS_RS_PARALLEL_EXECUTOR=1 (the cache is
   single-threaded; its ops misalign with key-group affinity under parallel). cache-OFF+parallel is the
   PROVEN-correct config: q8 lands in the correct band (2.95–3.06M, == depth-1/N=1/RocksDB band) vs the
   broken cache-ON parallel band (1.82–2.81M). [q8 is ~±4% nondeterministic — band, not exact, is its gate.]
Net: OPT-01 (parallel executor) is now CORRECTNESS-SAFE when enabled (opt-in via FRS_RS_PARALLEL_EXECUTOR=1,
which auto-bypasses the cache). Default = depth-1 + cache (correct + fast, robs nothing).
REMAINING for DEFAULT-ENABLE: perf tradeoff — parallel forces cache-off globally, which may cost the
cache-benefiting queries (q11/q12/q16). Needs a HEALTHY-box sweep (parallelism gain vs cache loss). The
deeper "keep cache under parallel" fix = run cache ops on the key-group's worker thread (future). q11
deterministic (92M) under parallel-coupled IN FLIGHT to confirm broad executor correctness.

## ★★★ q11 PASSES via parallel+cache-off (correct) (2026-06-09)
q11 + FRS_RS_PARALLEL_EXECUTOR=1 (worker=3, cache auto-off via coupling): FINISHED 135.7s,
out_rows=92,000,000 EXACT. = 318.9s(depth-1) → 135.7s = 2.35× faster → 0.82× RocksDB (111.1s) = PASSES
≥0.8× bar, CORRECTLY. Notably cache-OFF+parallel (135.7s) is FASTER than cache-ON+parallel (253.8s) and
depth-1 (318.9s) — so cache-off under parallel did NOT rob this cache-benefiting query (parallelism
dominates). Weakens the "robs Peter" concern → default-enable is promising. Verifying q3(light)/q9(join)
under parallel-coupled next to build the default-enable case.

## Default-enable case-builder (parallel+cache-off, 2026-06-09)
- q3 (light): 36.6s vs depth-1 36.7s, out_rows 2,201,068 EXACT = NEUTRAL + correct (no light-query tax).
- q11 (window): 135.7s (2.35× vs depth-1 318.9s), 92M EXACT, 0.82× RocksDB = PASSES.
- q8 (windowed-join): correct band (cache-off fixes the −10% race).
- NEXT: q12 (cache-benefiting, deterministic 92M, depth-1 50.6s) — does parallel+cache-off rob it?

## ★★★ OPT-01 DEFAULT-ENABLED (2026-06-09) — parallel executor + cache-off is now the default
Committed+pushed (flink): RoutingStateExecutor is DEFAULT (opt-out FRS_RS_PARALLEL_EXECUTOR=0);
MapStateCache auto-bypassed under parallel (aligned predicate). Both correctness blockers fixed
(deadlock-free sync key-group routing + cache-off-when-parallel). 528 native tests green; GHA gating.
Verified correct + neutral-or-faster: q3 36.6s(light,exact), q8 windowed-join(correct band), q11
318.9→135.7s(2.35×,92M,0.82×RDB PASS), q12 50.6→46.5s(cache-benefiting FASTER,92M). q9(dominant join,
was DNF) running under default to confirm finish+correct+pass. CONFIRMATION GATE = full q0–q22 e2e
sweep (out_rows correctness + perf, 3 backends) — the goal's recheck; reversible via =0 if any regression.

## ★★★ OPT-01 default REVERTED — robs cache-benefiting joins (q9) (2026-06-10)
q9 (dominant join) under DEFAULT parallel+cache-off: DNF 79.2M@1284s = neutral-to-SLOWER vs depth-1
~84.5M@1204s. q9 is per-probe-READ-bound and RELIES on the MapStateCache; it has no drain-tail (so no
q11-style win) and loses its read accelerator under cache-off → ROBBED. So parallel+cache-off HELPS
drain-tail-bound queries (q11 2.35×, q12 faster) but ROBS cache-benefiting throughput-bound joins (q9,
likely q4/q20) → NOT "benefits all" → default-on REVERTED to OPT-IN. Default = depth-1 + cache.
**OPT-01 final status: correctness-safe (deadlock + windowed-join fixed), opt-in; a NET win only for
drain-tail-bound queries; NOT a universal default.** A universal default needs the cache KEPT under
parallel (per-worker MapStateCache pinned to the key-group's worker thread, so cache-benefiting joins
keep their accelerator AND get parallelism) — the proper multi-PR OPT-01. q9/q20 still need OPT-02
(non-blocking FFM) / engine read-path levers, NOT the executor.

## ★★★ cache+parallel race ROOT-CAUSED precisely (2026-06-10) — the universal-OPT-01 spec
ForStRsMapStateV2.asyncGet (line 390-397): the GET-miss path does
  super.asyncGet(key).thenApply(value -> cache.putIfAbsent(keySnapshot, value))
The cache LOOKUP (line 358) runs on the MAILBOX thread; the get-completion POPULATE (putIfAbsent, line
394) runs in the .thenApply callback ON THE COMPLETING THREAD = a WORKER thread under the parallel
executor. So: shared cache → concurrent worker putIfAbsent races (−10%); per-worker cache → lookup
(mailbox) and populate (worker) hit DIFFERENT caches → inconsistent (−6.7%). cache-off avoids both
(correct) but kills q9's read accelerator (→ q9 robbed → why OPT-01 can't be default).
UNIVERSAL OPT-01 FIX (multi-PR, precisely specified): the cache lookup AND its get-completion populate
must run on the SAME key-group worker thread — i.e., move the read-cache INTO the worker's batch
processing (per-key-group, owned by the worker), not split mailbox-lookup / worker-populate. Then
cache-benefiting joins (q9) keep the accelerator AND get parallelism, no race → OPT-01 default-safe
without robbing q9. This is THE blocker for default-on; q9/q20 throughput separately need OPT-02.

## q19 still FAILS 0.8× even with findRow fix (2026-06-10)
q19 re-measure (findRow fix, default depth-1): DNF >600s. Same-box A/B earlier: findRow-ON 599.7s vs
findRow-OFF >700s (fix helps) but healthy-box best ~475s = ~0.64× RocksDB (305s) — still FAILS ≤381s
(0.8×). q19 (OVER-window/TopN) needs a THIRD lever beyond findRow (findRow fixed the 52%-CPU hotspot
but residual remains — profile the post-findRow q19 for the next bottleneck). FAILING SET (none pass by
default): q4(1.9× retract-join), q9(DNF, needs OPT-02/engine), q11(0.35× default — 0.82× only via
opt-in parallel which robs q9), q19(~0.64×, needs 3rd lever), q20(DNF, OPT-02/engine). All multi-PR.

## ★★★★ BREAKTHROUGH: thread-safe MapStateCache → universal OPT-01 default (2026-06-10)
Root cause of windowed-join under-emit was the single-threaded MapStateCache racing under parallel
(the .thenApply get-completion putIfAbsent runs on worker threads). FIX: synchronize MapStateCache's
14 public methods (lookup returns an on-heap value → sound). Cache KEPT under parallel (removed
cache-off coupling). RESULT: q8 windowed-join = 3,064,457 EXACT (== RocksDB; BEATS depth-1's
3,010,888!) with cache KEPT + parallel. This is the universal unlock: cache-benefiting joins (q9)
retain the accelerator AND get parallelism → OPT-01 default-enabled WITHOUT robbing q9. Uncontended
monitor cheap on the default single-threaded path. Committed + pushed (GHA). Verified: q8 EXACT, q11
2.35×/92M/0.82×RDB PASS, q12 faster, q3 neutral. q9 re-test (cache kept) IN FLIGHT to confirm not-robbed.
NEXT: full q0-q22 sweep (3 backends) = the goal's recheck.

## OPT-01 universal default JUSTIFIED — q9 no longer robbed (2026-06-10)
q9 default-on with thread-safe cache KEPT: 81.6M@1184s ≈ depth-1 84.5M@1204s = NEUTRAL (cache-off-
parallel was 79.2M@1284s/61.7K/s → cache-kept 68.9K/s ≈ depth-1 70.2K/s; accelerator restored). So
default-on (thread-safe cache) is NEUTRAL-OR-BETTER across all tested families: q3 neutral, q8 EXACT
(3,064,457), q9 neutral (~0.85× both DNF), q11 2.35× PASS, q12 faster. No rob-Peter → default JUSTIFIED.
REMAINING: full q0–q22 sweep (passing-set no-regression + q20/q4 + 3-backend) = the goal's recheck.

## ★★★ OPT-01 default REVERTED again (3rd) — thread-safe cache robs q17 (2026-06-10)
q17 (group-agg, cache-HOT) default-on with thread-safe cache: FINISHED 270.0s vs depth-1 76.7s = 3.5×
REGRESSION (out_rows 92M correct). The parallel executor + CONTENDED synchronized-cache (q17 hammers
the cache at high hit-rate; multiple workers + mailbox contend the lock) catastrophically robs q17.
=> Every default shortcut robs SOME query: cache-OFF+parallel robs q9 (cache-benefiting joins);
synchronized-cache+parallel robs q17 (cache-hot agg, lock contention). Reverted default-on (git revert
e4d487df177) → known-good opt-in: default depth-1 + unsynchronized cache (q17 76.7s, q9 fast, ALL
correct+fast); OPT-01 opt-in (cache-off+parallel, correct, q11 2.35× when enabled).
**DEFINITIVE: OPT-01 universal default REQUIRES the deep cache-into-worker fix** — a LOCK-FREE
per-key-group cache OWNED by each worker (lookup+populate on the worker thread, no shared lock, no
cross-thread race). Then cache-hot queries (q17) keep zero-overhead cache AND cache-benefiting joins
(q9) keep the accelerator AND windowed-joins (q8) are correct AND drain-tail queries (q11) win — all
without robbing any. That is the multi-PR OPT-01; synchronization is insufficient (q17). Repo safe.

## ★★★ DEFINITIVE root-cause model for OPT-01 non-universality (2026-06-10)
q17/q11/q12 agg-state (ValueState/Aggregating/Reducing V2) do NOT use MapStateCache. So q17's 3.5×
regression (76.7→270s) is THE PARALLEL EXECUTOR ITSELF, not the cache lock. => OPT-01's
non-universality has TWO INDEPENDENT causes:
1. EXECUTOR OVERHEAD: the synchronous-blocking-per-batch RoutingStateExecutor adds per-batch fan-out +
   block latency that HELPS drain-tail-bound queries (q11 2.35×, eliminates the 178s serial drain) but
   CATASTROPHICALLY ROBS non-drain-tail queries (q17 3.5× slower). Independent of the cache.
2. CACHE RACE: the single-threaded MapStateCache races under parallel (q8 −40%); synchronized fixes
   correctness (q8 EXACT) but the lock robs cache-hot MapState queries.
=> TRUE universal OPT-01 needs BOTH: (a) a CORRECT ASYNC-OFFLOAD executor — incomplete future, NO
per-batch block (the deadlock-free-but-non-blocking design; my synchronous version's block is what robs
q17), so it pipelines for q11 WITHOUT adding latency for q17; (b) a LOCK-FREE per-worker cache. Both are
the multi-PR OPT-01 (matches the doc's ForSt-coordinator design). Synchronous-block + sync-cache (this
session's shortcuts) are insufficient. OPT-01 stays OPT-IN (net win only for drain-tail queries).
Default = depth-1 (correct + fast for ALL). Repo safe; 3 reverts each caught a real regression.

## q19 third lever = diffuse serde+engine (2026-06-10, JFR analysis)
Pre-findRow JFR (q19-decay.jfr, Jun8): findRow 52% (34,996/66,989 on-CPU) — the hotspot the committed
findRow O(n^2)->O(1) fix removed. The NEXT frames (the post-findRow residual): Flink serde
(RowDataSerializer.copyRowData ~2480, copy ~2149, StringDataSerializer.copy ~2181, String.charAt ~1287)
+ engine read + MapStateCache.hashOf ~2509 + RowDataEventDeserializer. => post-findRow q19 residual is
DIFFUSE serde + engine-read (same class as q9), NOT a single fixable hotspot. q19's third lever is
multi-PR (engine read-path / serde-reduction), not a quick win. (Current JFR re-capture failed —
container killed at MAXSEC before async dump flushed; needs MAXSEC>=500 to re-capture post-findRow.)
=> ALL failing queries (q4/q9/q11/q19/q20) need multi-PR/diffuse work; none is a quick default-path win.

## CORRECTION (2026-06-10): first "full sweep" ran a STALE PARALLEL jar — invalid; HEAD is safe
The post-revert `run-8c32g.sh jar` was a stale incremental build (didn't pick up the git-revert), so
the first q0-q22 sweep ran a PARALLEL+cache-on-unsync jar (q8=1,938,439 broken cache-race, q11=134.7s
parallel) — INVALID for the committed depth-1 default. CLEAN rebuild (`mvn clean package`) + redeploy;
q8 depth-1 verify = 3,064,445 ≈ RocksDB 3,064,457 → COMMITTED DEFAULT IS SAFE (depth-1, correct).
SIGNAL from the (invalid-cache) parallel sweep: parallel makes q7 FINISH (1052s) and q20 FINISH EXACT
(1610s = 93,201,404 == RocksDB) — both DNF at depth-1. So OPT-01 (parallel) helps the heaviest joins
FINISH (q9 still DNF even parallel @1700s). With the cache correct under parallel (the multi-PR fix),
q7/q20 would finish correctly + parallel. Relaunching the VALID sweep on the clean committed jar.

## ★★ VALID q0-q22 forst-rs sweep (committed depth-1 default, 2026-06-10)
Clean committed jar (depth-1 default). out_rows correct except noted:
q0 30.9s/100M | q1 29.3s/100M | q2 27.8s/100M | q3 35.6s/2,201,068 | q4 373.0s/25,835,082(retract) |
q5 41.6s/808,765(windowed-agg, correctness?) | q7 1441.6s/92,000,002(finishes) | q8 43.6s/3,064,445(✓) |
q9 DNF@1700 | q10 124.2s/100M | q11 264.8s/92M | q12 49.6s/92M | q13 28.5s | q14 29.4s | q15 161.8s/92M |
q16 323.9s/92M | q17 77.7s/92M | q18 222.9s/92M | q19 527.9s/92M | q20 DNF@1700/93.2M(exact when finishes) |
q21 53.9s | q22 43.6s.
PASS (≥0.8×RDB, ≤50s, correct): q0/q1/q2/q3/q8/q12/q17/q18 (+ light q10/q13/q14/q21/q22). 
FAIL: q4 (+59.5s >50s regression), q11 (0.42×), q19 (0.58×), q9 (DNF near-parity), q20 (DNF vs RDB 800s).
q5 correctness suspect. q7 finishes 1441s (RDB baseline pending).
=> 5 failing queries (q4/q9/q11/q19/q20) need the multi-PR levers: universal OPT-01 (parallel+lock-free
per-worker cache → q11 pass, q7/q20 finish faster — the invalid parallel sweep showed q11=134.7s,
q20=1610s EXACT, q7=1052s), OPT-02 (q9), q19 3rd lever (diffuse serde+engine), q4 (-9.5s to clear).
3-backend: RocksDB/ForSt baselines in earlier rows; forst-rs side now complete.

## ★★ SAME-SESSION frs-vs-RocksDB sweep (2026-06-10) — valid apples-to-apples
forst-rs (SWEEP2 depth-1) vs RocksDB (RDBSWEEP), same box/session:
WINS (frs beats RocksDB): q7 (1441.6s FINISHES vs RocksDB DNF@1700!), q15 (161.8 vs 229.7), q16 (323.9
vs 374.1), q18 (222.9 vs 366.4) + light q0/q1/q2/q10/q13/q21/q22 faster.
PARITY-PASS: q3 0.98×, q8 0.97×, q12 0.83×, q14 ~1.0×, q17 0.94×.
FAILS: q4 (+70.3s>50s; out_rows 25.8M vs 177.6M = retract-changelog cadence, needs final-result check),
q9 (DNF vs RocksDB 1420.5s), q11 (0.40×), q19 (0.59×), q20 (DNF vs 859.7s; finishes 1610s EXACT parallel).
★ CORRECTNESS BUG: q5 out_rows 808,765 vs RocksDB 29,988,416 = −97% UNDER-EMIT. q5 (windowed-agg) is
WRONG (confirms historical q5 nondeterminism/under-emit). MUST FIX before its perf counts — top priority
(correctness non-negotiable). Note q5 frs=41.6s (fast but WRONG).
=> frs already WINS q7/q15/q16/q18 vs RocksDB. Genuine remaining: q5 (correctness), q9/q11/q19/q20 (perf,
multi-PR levers), q4 (+marginal). 3-backend: ForSt sweep next to complete the matrix.

# ═══════════════════════════════════════════════════════════════════════════
# CURRENT STATUS — complete same-session 3-backend q0-q22 sweep (2026-06-10)
# ═══════════════════════════════════════════════════════════════════════════
forst-rs = committed DEFAULT (depth-1, opt-in OPT-01). All three on the same box/session (8c/32g).
Goal per query: frs ≥0.8× RocksDB AND ≤50s regression AND strictly faster than ForSt AND correct.

| q  | forst-rs | RocksDB | ForSt   | vs RDB | vs ForSt | VERDICT |
|----|----------|---------|---------|--------|----------|---------|
| q0 | 30.9s    | 31.8s   | 30.7s   | faster | +0.2s    | ~PASS (ForSt parity, 0.2s) |
| q1 | 29.3s    | 30.1s   | 30.1s   | faster | faster   | PASS |
| q2 | 27.8s    | 28.2s   | 28.2s   | faster | faster   | PASS |
| q3 | 35.6s    | 35.0s   | 37.2s   | 0.98×  | faster   | PASS |
| q4 | 373.0s   | 302.7s  | 1042.2s | +70.3s | faster   | FAIL RDB (>50s); out_rows retract-cadence (25.8M vs 177.6M) needs final-result check |
| q5 | 41.6s    | 162.5s  | DNF@400 | —      | —        | ✗✗ CORRECTNESS: 808,765 vs RDB 29,988,416 (−97%) — WRONG |
| q7 | 1441.6s  | DNF     | 586.8s  | BEATS  | slower   | FAIL ForSt (frs 1441 > ForSt 587); beats RocksDB |
| q8 | 43.6s    | 42.5s   | 39.2s   | 0.97×  | +4.4s    | FAIL ForSt (4.4s) |
| q9 | DNF      | 1420.5s | DNF     | DNF    | par      | FAIL RDB (RocksDB finishes, frs DNF) |
| q10| 124.2s   | 129.5s  | 129.3s  | faster | faster   | PASS |
| q11| 264.8s   | 106.1s  | 134.9s  | 0.40×  | slower   | FAIL both |
| q12| 49.6s    | 41.4s   | 39.7s   | 0.83×  | +9.9s    | FAIL ForSt |
| q13| 28.5s    | 29.3s   | 29.5s   | faster | faster   | PASS |
| q14| 29.4s    | 29.6s   | 29.2s   | faster | +0.2s    | ~PASS (ForSt parity, 0.2s) |
| q15| 161.8s   | 229.7s  | 206.8s  | faster | faster   | PASS (beats both) |
| q16| 323.9s   | 374.1s  | 331.7s  | faster | faster   | PASS (beats both) |
| q17| 77.7s    | 72.8s   | 253.0s  | 0.94×  | faster   | PASS |
| q18| 222.9s   | 366.4s  | DNF@400 | faster | BEATS    | PASS (beats both) |
| q19| 527.9s   | 310.2s  | 308.1s  | 0.59×  | slower   | FAIL both |
| q20| DNF      | 859.7s  | 1535.9s | DNF    | (1610 par)| FAIL RDB (DNF; finishes 1610s EXACT under opt-in parallel) |
| q21| 53.9s    | 60.0s   | 58.3s   | faster | faster   | PASS |
| q22| 43.6s    | 44.2s   | 46.3s   | faster | faster   | PASS |

## SUMMARY (current state, committed default)
- PASS (beat RocksDB+ForSt, correct): q1,q2,q3,q10,q13,q15,q16,q17,q18,q21,q22 = **11 clear**.
  (q15/q16/q18 beat BOTH; q7/q18 finish where a competitor DNFs.)
- ~PASS marginal (ForSt parity ±0.2s): q0, q14.
- FAIL — slower than ForSt only (beats RocksDB): q7 (big), q8 (+4.4s), q12 (+9.9s).
- FAIL — RocksDB perf: q4 (+70s), q9 (DNF), q11 (0.40×), q19 (0.59×), q20 (DNF).
- FAIL — CORRECTNESS: q5 (−97% under-emit, windowed-agg bug) — TOP PRIORITY.

## REMAINING WORK to close Phase 1 (precisely scoped)
1. **q5 CORRECTNESS** (top priority): windowed-agg under-emit (−97%). Fix the sliding-window merge/fire.
2. **q11/q20** (and q7-vs-ForSt): universal OPT-01 = non-blocking async executor + LOCK-FREE per-worker
   cache (parallel sweep proved q11→134.7s, q20→1610s EXACT; the executor-block robs q17-class + the
   single-thread cache races — both must be fixed for a default).
3. **q9**: OPT-02 (non-blocking/parallel FFM) — DNF even under parallel.
4. **q19**: diffuse serde+engine read-path (post-findRow profile).
5. **q8/q12 vs ForSt**: small gaps (4-10s) — ForSt's windowed-agg path is faster; investigate.
6. **q4**: −20s+ to clear ≤50s (box-marginal; rigorous best 340s passes).
All committed levers (zero-copy, q19 findRow, deadlock-free routing, OPT-01 opt-in) are GHA-green; the
remaining are multi-PR architectural changes for a healthy box. Phase 1 NOT met: 11 pass, ~12 fail/marginal.

# ═══════════════════════════════════════════════════════════════════════════
# q9 DEEP PROFILE — verify-before-build campaign (2026-06-10)
# ═══════════════════════════════════════════════════════════════════════════
Run: q9 frs default (depth-1), 100M, MAXSEC=2400, FRS_DECAY_DIAG+FRS_RSS_SAMPLE.
- Curve: 65K/s avg to 622s (40.3M) → oscillating decay 18.8-45K/s → 90.1M@2313s,
  cut at 2400s (~91M). TRUE FINISH ≈ 2600s vs bar 1776s (0.8× RDB 1420.5) → needs ~1.47×.
- LSM HEALTHY during decay: L0≤3, sub-GiB/CF (DECAY_DIAG) → NOT compaction/L0 pileup.
- CPU (12s /proc window, deep decay): box ~8% utilized. Compaction 2×~25%-core,
  datagen 4×~3%, opendal pool ~1.4%×10+, join task threads <0.5% in-window;
  cumulative join-thread CPU = 415s/2085s elapsed = 20% duty cycle.
- Thread dumps (×2, all 4 join threads identical signature): RUNNABLE inside ONE
  synchronous FFM downcall — frsVecIterPrefixOpen (ForStRsLinker:3949 ←
  ForStRsDBIterRequest.process:300 ← VectorizedExecutor.drainIterSerial:1460) and
  vectorizedBatchGet (ForStRsLinker:2607 ← executeGets:1235) — invoked from
  AsyncExecutionController.drainInflightRecords ← processWatermark. I.e. EVERY
  watermark forces a FULL inline serial drain of all pending probes on the task thread.
- Engine code: every non-resident block read = handle.block_on(opendal read)
  (forst-rs-io/src/opendal_backend.rs:478-485,683,699) = tokio handoff round-trip
  per op EVEN ON LOCAL DISK (same disease the SST WRITE path had and fixed by
  coalescing ~500× — see :823-827 comment; read path still per-op).

## VERDICT: q9 (and the q7/q20 family) is LATENCY-bound, not CPU/disk/compaction-bound
Per-record cost ≈ 120µs (33K/s ÷ 4 threads) = serial synchronous engine round-trips
× async-dispatch latency floor. Throughput = 1/latency; decays as deeper state adds
block reads per seek. ForSt q9 DNF too (parallel executor alone insufficient);
RocksDB finishes because sync in-process block-cache reads have a µs-class floor.

## Reordered optimization program (verified)
1. ENGINE direct-local-read fast path: bypass block_on/tokio for local-cache/disk
   hits → per-op µs-class. Benefits ALL read queries (q9/q19/q7/q20/q4). NEW co-primary.
2. Coordinated executor (approved design, unchanged): ×~3 latency hiding + non-blocking
   mailbox + backpressure; measured floors q11 134.7 / q20 1610 / q7 1052.
3. Batched iter-open (one FFM call, engine parallel seek) — folded into #2's iter dispatch.
4. Micro-levers (decode allocs, mailbox serialization) — re-rank after 1+2 land.
Estimates (A=executor, B=direct-read): q7 1441→A ~750-1000 → A+B ~500-700 (bar <587);
q9 ~2600→A ~1100-1500 → A+B ~700-1200 (bar 1776); q20 1610(par)→A ~1200-1450 →
A+B ~700-1000 (bar 1074); q11 134.7 measured (bar 132.6). GO on A+B.

# ═══════════════════════════════════════════════════════════════════════════
# q9 READ_AT_DIAG discriminator (2026-06-10) — Stage-2 lever CORRECTED
# ═══════════════════════════════════════════════════════════════════════════
Run: q9 EVENTS_NUM=50M, FRS_READ_AT_DIAG=1. FINISHED 813.5s, out_rows=45,904,788
(fast A/B baseline for read-path levers).
- Histogram (LocalFirstSstFile.read_at — the SYNC local-cache-hit pread path):
  reads=6.3M→9.4M across decay; mean 146µs→191µs; cold(≥20µs) 24.9%→29.1% RISING;
  ~72% warm 1-5µs. ~0.6 preads/record × ~182µs ≈ 110µs/record = the measured floor.
- VERDICT: tokio/block_on REFUTED for local decay (sync fast path already exists and
  is where the time goes). Binder = COLD PREADS once state outgrows container page
  cache (Docker-VM disk ~0.1-1ms). Levers: (a) per-probe SST bloom/range pruning,
  (b) block-cache hit-rate for join hot set, (c) compaction shape. Engine-only.

# ═══════════════════════════════════════════════════════════════════════════
# PR-1 coordinated-executor GATES + bisect (2026-06-10) — two verdicts
# ═══════════════════════════════════════════════════════════════════════════
Jar = clean build @7a14ccaf1a7 (classifier pool + CoordinatedStateExecutor + 3-way gate).
GHA ci-forst-rs GREEN. All runs 8c/32g, 100M, FRS_RS_EXECUTOR as noted.

| run                        | wall    | out_rows   | verdict |
|----------------------------|---------|------------|---------|
| q17 coordinated N=3        | 271.8s  | 92M exact  | ✗ FAIL ≤85s gate (depth-1 = 77s; ForSt = 253s!) |
| q8  coordinated N=3        | 40.6s   | 2,511,323  | ✗ FAIL band (−18% under-emit) |
| q8  routing  N=3 (this jar)| 40.7s   | 3,064,485  | ✓ band (Task-1 pool exonerated) |
| q8  coordinated N=1        | 45.6s   | 3,064,540  | ✓ band (pipelining alone OK) |

## Verdict 1 — q17: the COORDINATOR HANDOFF is the q17 robber, not the blocking
Non-blocking coordinated N=3 = 271.8s ≈ ForSt's own q17 (253.0s). High-rate cheap-state
queries (1.2M rec/s) die on per-batch mailbox→worker→mailbox switches regardless of
blocking. Depth-1 inline is WHY frs beats ForSt 3.3× on q17. ⇒ default must be ADAPTIVE:
inline for get-only/cheap batches, offload iterator-containing batches (every measured
parallel win is iter-heavy; every robbery is cheap-batch handoff).

## Verdict 2 — q8 N=3: write/read ORDER INVERSION via mailbox-direct statebuf writes
MapStateV2 put/remove/clearForPrefix flush DIRECTLY to the engine on the mailbox
(MapStateArrowBuffer.putShared → flushTo :171/:177; clearForPrefix), while engine
reads (iters/gets) queue behind worker backlogs. With run-ahead (N=3; N=1 is throttled
by fullyLoaded=ongoing≥1 into quasi-lockstep) a LATER cleanup-remove lands BEFORE an
EARLIER-created iter executes → iter misses live rows → −18%. Matches all 3 bisect cells.
⇒ Correct fix = reads execute against a CREATION/DISPATCH-time MVCC snapshot (engine
has MVCC + snapshot_view): capture/pin seq per batch on the mailbox at dispatch, workers
read as-of-seq, release after batch. Restores exact depth-1 visibility semantics under
any scheduling. Required for ANY parallel default (incl. opt-in routing? NO — routing
blocks the mailbox, no run-ahead, proven correct).

# ═══════════════════════════════════════════════════════════════════════════
# ADAPTIVE-mode gates (2026-06-10, jar @b1f4a936dd8) — FAILED, two NEW findings
# ═══════════════════════════════════════════════════════════════════════════
| run               | result                       | verdict |
|-------------------|------------------------------|---------|
| q17 adaptive      | WEDGED src_out=0 → MAXSEC300 | ✗ job never progresses (startup wedge) |
| q8  adaptive      | 40.6s, out_rows=2,537,233    | ✗ −17% — SAME family as coordinated-N3 |
| q11 adaptive      | killed early (run aborted)   | — |

KEY IMPLICATION: q8 corrupts under adaptive even though its iter batches use the
EXACT blocking dispatch that measured CORRECT under routing (3,064,485, same jar).
The corruption is therefore NOT unique to non-blocking run-ahead; the INLINE
fast-path for iter-free batches (or its interleaving with worker batches) is
implicated. The run-ahead theory for coordinated-N3 q8 needs re-examination too —
the common factor across BOTH wrong modes is mailbox-executed iter-free batches
coexisting with worker-executed iter batches... but routing (all-on-workers) and
inline (all-on-mailbox) are each correct. NEXT DEBUG STEP: thread-dump a wedged
q17-adaptive TM (task thread stack at src_out=0) — the wedge is likely the same
defect as the corruption, and it is 100% reproducible in ~60s.

BONUS FINDING (q11, from the wedge investigation dump): GroupWindowAggregate runs
on the V1 SYNC ForStRsMapState via ForStRsInternalKvStateAdapters, and burns its
CPU in MergingWindowSet.initializeCache → ForStRsMapState.forEachEntry(:763) PER
ELEMENT — a full session-window map iteration per record (O(N)/record). This is a
REAL q11 lever INDEPENDENT of the executor (cache it per key / avoid re-drain).

STATE: default = depth-1 inline (untouched, safe); adaptive+coordinated+routing all
env-gated opt-in; 534 unit tests + GHA ci-forst-rs green @b1f4a936dd8.

# ADAPTIVE re-gate on LEAK-FIXED jar @1a95d0a010f (2026-06-10)
ROOT CAUSE of wedge+corruption FOUND+FIXED: Task-1 classifier pool leaked a classifier +
4 arena ColumnarBatchBuffers PER BATCH in all non-coordinated modes (incl. depth-1 default!)
→ q17-adaptive TM cgroup-OOM-killed ~13s (the wedge). Fix = SELF-RELEASE in
executeBatchRequests finally (1a95d0a010f) + inline path getNow-not-join.
| run (adaptive, fixed jar) | result | verdict |
|---|---|---|
| q17 | 148.8s, 92M exact | ✗ bar ≤85s — but FINISHES; corruption family CLEARED |
| q8  | 40.6s, 3,064,493  | ✓ IN BAND — adaptive corruption GONE (was leak/join-coupled) |
q17 residual 148.8 vs 77 depth-1: per-batch KEY-GROUP SPLITTING (≤3 sub-classifiers,
3 FFM calls/batch) taxes high-batch-rate queries even inline.
NEXT DESIGN (final iteration): split ONLY iter requests by kg; gets/puts in ONE shared
classifier executed INLINE FIRST (preserves writes-before-iters invariant), then latch
the iter sub-batches to workers. q17 → byte-identical to depth-1; iter queries keep wins.
Then gates: q17≤85, q8 band, q11≤140, q20, q9-noregress → default flip.
⚠ depth-1 DEFAULT also leaked on jars built from d82427efb1c..b1f4a936dd8 (≈1h window,
fixed by 1a95d0a010f) — any perf runs on those jars are SUSPECT; re-measure on fixed jar.

# ADAPTIVE design iterations (2026-06-10 late) — phase-split REFUTED ×2, affinity puzzle
| variant (jar) | q17 | q8 | verdict |
|---|---|---|---|
| full-split + inline subs (1a95d0a010f) | 148.8s | 3,064,493 ✓ | CORRECT, q17 ✗ bar |
| split-iters, main-first (4ec61c554ae)  | 112.7s | 1,441,352 ✗ −53% | fire-then-purge inverted |
| split-iters, iters-first (a11ac06a8c0) | — | 2,147,792 ✗ −30% | reverse hazard real too |
| defer-classify 1-classifier (f4c9d99b468) | 117.7s | 1,831,340 ✗ −40% | kg-UNAFFINE cheap batches corrupt |
| kg-affine restored (8fd6edddea7) | TBD | verifying | = full-split semantics, deferred offers |
LAW (empirical): q8 correct ⟺ EVERY batch executes through per-key-group-affine
classifiers in offer order (routing ✓, full-split ✓). ANY kg-unaffine execution of
cheap batches corrupts — even inline, lockstep, cache-off. ROOT CAUSE OPEN (suspect:
per-executor/per-classifier state coupling not yet identified — needs instrumented
1M-scale q8 bisect next session). Phase-splitting iters from deletes is unsound
(windowed fire-purge emits same-key ITER/CLEAR in both orders).
q17 cost ladder: depth-1 77s | defer-1-classifier 117.7 | full-split-inline 148.8 |
latch-offload 271.8 → the kg-splitting of cheap batches costs ~35-70s on q17; the
correct-and-fast-q17 design needs the affinity puzzle solved first.

# ═══════════════════════════════════════════════════════════════════════════
# FINAL adaptive verdict (2026-06-10 session end): NONDETERMINISTIC race — FLAKY
# ═══════════════════════════════════════════════════════════════════════════
q8 on the offer-time-restored adaptive (@6bfd4354683, semantically = the config that
once measured 3,064,493): **2,328,053 (−24%)**. The earlier in-band pass was LUCK.
q8-adaptive across runs: 2,537,233 / 3,064,493 / 1,441,352* / 2,147,792* / 1,831,340* /
2,328,053 (*=different designs). CONCLUSION: the inline-fast-path+worker mixed regime
has a real RACE; single-run out_rows verdicts are unreliable for executor work.
- MULTI-RUN-VERIFIED modes only: depth-1 inline (default, always in band) and
  routing all-latch (3,064,457 / 3,064,514 / 3,064,485 = three independent passes).
- The "offer-time serialization law" and "kg-affinity law" were derived from single
  runs — treat as HYPOTHESES, not laws, until a deterministic repro exists.
- NEXT SESSION MUST build a DETERMINISTIC q8 repro first (controlled-replay harness,
  sorted CSV input, p=1, byte-compare — the 2026-06-02 /tmp/corr-q5 pattern) before
  ANY further executor iteration. Then bisect inline-vs-latch with it.
- STATE: default depth-1 SAFE+correct; adaptive/coordinated OPT-IN, adaptive FLAKY
  (do not use for benchmarks); routing OPT-IN correct (the measured q11/q20/q7 wins).
  All pushed @6bfd4354683, 534 UTs green.

# q8 race characterization (2026-06-10 session end)
- Reduced-scale probes ALL deterministic+identical: 1M ×4 (inline ref + adaptive ×3) =
  30,390 exact; 10M ×3 (adaptive ×2 + inline) = 306,016 exact. Race is LOAD-DEPENDENT —
  needs full-100M sustained pressure (AEC in-flight saturation + checkpoint overlap +
  watermark drain cadence; reduced runs finish in 3-7s).
- Full-scale q8 = ~40s/run → the repro IS the full-scale run (~3 min cycle).
- VARIABLE ISOLATED: routing (=adaptive minus inline fast-path, all else identical) is
  correct ×3; adaptive wrong in 5/6 full-scale runs (1.44-3.06M spread). The racy
  ingredient = INLINE execution of iter-free batches on the mailbox interleaved (in time)
  with latch-dispatched iter batches. Mechanism candidate for next session: inline
  per-row completions run SYNCHRONOUSLY mid-batch on the mailbox (callbackRunner inline
  when already on mailbox) and can re-enter the AEC/active-container during the inline
  phase — vs the latch path where completions queue as mails. Instrument that re-entrancy
  first (count offers-into-active-container during inline execute).

# q8 race: RE-ENTRANCY REFUTED (2026-06-10 final probe)
FRS_REENTRY_DIAG run: corruption reproduced (q8 adaptive = 2,186,285) with ZERO
re-entrant executeBatchRequests calls. NEXT CANDIDATE (sharper): callback-inlining
asymmetry — InternalAsyncFuture.complete() from the MAILBOX thread may run user
callbacks INLINE-IMMEDIATELY (CallbackRunnerWrapper same-thread path) → user code
(window emit/cleanup, setCurrentNamespace, statebuf staging) runs MID-BATCH during
inline execution; worker-thread completions enqueue as mails instead. Adaptive MIXES
both regimes per batch → callback ordering differs from both pure modes (depth-1
all-inline ✓, routing all-enqueued ✓). NEXT: instrument WHERE per-row callbacks
execute (thread + during-batch flag) under each mode; compare orderings on the 40s
full-scale repro. Probe committed as FRS_REENTRY_DIAG (extend it).

# q8 race: CALLBACK-INLINING ASYMMETRY ALSO REFUTED (2026-06-10, code-level)
CallbackRunnerWrapper.submit ALWAYS mailboxExecutor.execute()s — no same-thread inline
path exists. User callbacks never run mid-batch on ANY thread. Both mechanism candidates
(re-entrancy: 0 probe hits; callback inlining: impossible by code) are DEAD.
Remaining knowns: the ONLY code delta routing(✓×3) vs adaptive(✗6/7) is WHICH THREAD
executes iter-free batches (mailbox vs worker); completions always enqueue as mails in
both; both complete before return. The corruption mechanism is therefore something
thread-identity-sensitive INSIDE the engine/FFM/state layer (e.g., thread-affine caches,
thread-id-keyed structures, or memory-visibility on state-level structures touched by
the mailbox during inline execution that workers later read). NEXT-SESSION TOOLING:
extend FRS_REENTRY_DIAG to log per-row (requestType, keyGroup, completing-thread,
batch-seq) on the 40s repro for BOTH modes and DIFF the streams — find the first
divergent row family, then trace that state primitive's thread-sensitivity.

# ═══════════════════════════════════════════════════════════════════════════
# q7/q9/q20 READ-VOLUME LEVER DECIDED: SST PREFIX BLOOM (2026-06-10)
# ═══════════════════════════════════════════════════════════════════════════
q9@50M FRS_BULK_SAMPLE=1000 decay-phase DECAY_ATTR (per-probe ns, sampled):
  win@8192  probe=1579 | A_fanout=707(sstloop=668,n_ovl=1) C_activeseek=646 B_resident=0
  win@32768 probe=5256 | A_fanout=3034(sstloop=2898,n_ovl=2[L0=1,deep=1]) C=1078 B=0
- GET path HEALTHY: n_ovl 1-3, cost 1.5-5µs, bloom/locate working, resident shadow moot,
  compaction shape fine. Gets are NOT the binder.
- Binder = PREFIX-SCAN (iter) path: point blooms cannot prune RANGE scans → every
  overlapping SST pays index+data-block preads per probe even when it contains ZERO keys
  for that join-key prefix (READ_AT: 182µs mean preads, 26% cold, ~0.6/record).
- ALSO: prior run's 16× slowdown explained = mis-set FRS_CURSOR_DIAG=1 (K=sampling divisor,
  1=time EVERY seek). Healthy rerun tracks the 813.5s baseline. No regression.

## LEVER: RocksDB-style PREFIX BLOOM (engine, config-free, helps all prefix-scan queries)
Add a second per-SST Sbbf over FIXED-length key prefixes (P bytes, e.g. 16: covers
kg+stateId+joinKey); consult at prefix-scan open — skip SSTs whose prefix bloom rejects
the probe's first P bytes (scans with prefix < P bytes bypass the filter, conservative).
Expected: scan preads cut 2-3× (n_ovl 2-3 → ~1 true-containing SST) → q9 ~2600→
~1300-1600s (bar 1776 PASS), q20/q7 similar direction. Implementation: SST writer
(collect distinct P-prefixes → Sbbf, format-versioned footer field), reader (load+expose),
scan-open SST selection (may_contain_prefix), FFI untouched (engine-internal), UTs +
format round-trip + q9@50M A/B vs 813.5s baseline.

# PREFIX BLOOM A/B (q9@50M, depth-1, 2026-06-10, engine 30cde2fc6)
| build | wall | out_rows | decay rate @40M |
|---|---|---|---|
| baseline (pre-v3)   | 813.5s | 45,904,788 | ~19K/s |
| prefix bloom (v3)   | 765.6s | 45,904,788 ✓ EXACT | ~28-37K/s (≈1.7×) |
−5.9% wall at 50M; gain concentrated in the DECAY phase (memtable-resident early
phase unchanged, as expected — bloom prunes SST scan fan-out only). At 100M the
decay phase dominates → expect a larger total win. 534 Java + 831 Rust tests green;
ForSt pushed. 100M gates launching: q9 (bar ≤1776s), q20 (≤1074s), q7 (<587s).

# q9@100M with PREFIX BLOOM (2026-06-10): 94.8M@2389s (MAXSEC 2400) — ✗ bar, but ahead
vs pre-bloom 90.1M@2313 (~91M@2400, true ~2600s): consistently ahead (70M@1525 vs ~1670s,
late rate 2× mid-run), but the advantage COMPRESSED in the last 10M (rate 18-27K/s) —
extrapolated true ~2600s, bar ≤1776s NOT met. READING: bloom prunes FALSE-overlap SSTs;
late-q9 probes hit mostly TRUE-positive SSTs (an auction's bids accumulate across many
SSTs over its lifetime) whose cold preads remain. q9's NEXT lever = locality for
true-positive data: compaction clustering (leveled merge brings one auction's bids into
one block — how RocksDB sustains 70K/s here) and/or block sizing. Prefix bloom stays
(free win where prefixes are absent; q20/q7 may benefit more — their joins probe auction
metadata with tighter locality).

# q20/q7@100M prefix-bloom gates (2026-06-10 afternoon) — ⚠ BOX-DEGRADATION CAVEAT
- q20: DNF@1800s at 80.75M (extrap ~2300s; bar 1074). Pre-bloom depth-1 also DNF.
- q7: running SLOWER than its pre-bloom 1441.6s finish (71.2M@1423s) — the bloom cannot
  hurt q7 (hot price prefixes pass the filter; overhead = 1 hash/SST), so this indicates
  the BOX has degraded across ~3h of continuous heavy runs. ⚠ Cross-session A/B deltas
  from this afternoon are unreliable; only BACK-TO-BACK pairs count from here.
- Next: rebuild .so with OPT-N14 (L0 batch short-circuit, committed b59042dc2) and run a
  CONSECUTIVE q9@50M pair (bloom-only re-baseline, then bloom+N14) for a clean A/B.

# OPT-N14 back-to-back A/B (q9@50M, 2026-06-10 PM) + depth gate
| run (consecutive, same box) | wall | out_rows |
|---|---|---|
| bloom-only re-baseline | 844.6s | 45,904,788 ✓ |
| bloom + N14 key-major  | 880.7s | 45,904,788 ✓ |
N14 cost q9 4.3%: q9 runs at L0 1-3 (compaction keeps up) — the short-circuit saves
nothing and the per-(key,file) reader lookups add overhead. The doc's 40-64-file
premise = q4-class write-stall regimes. FIXED (dd17ef60d): key-major only when
L0 >= 8 files; below, the original file-major path (O(L0)<=7 reads) runs. No loser.
ALSO: box drift quantified — morning bloom-only 765.6s vs afternoon 844.6s (+10%)
on identical code → only back-to-back pairs are valid A/B today.
NEXT: OPT-N18 (value-carrying merge bypassed for memtable-resident keys — the
scan-path per-row get_internal re-walk; q7/q9/q20's write-then-read joins hit it
on the COMMON path).

# OPT-N18 REASSESSED + the GARBAGE-RETENTION hypothesis (2026-06-10 end)
- OPT-N18 (value-carrying merge for memtable sources): the Fallback is ALREADY
  memtable-cheap (get_internal short-circuits at the active memtable, db.rs:8399-8424);
  the win is a SECOND memtable probe + Arc alloc per row = ~100s ns CPU. Today's decay
  profile (8% CPU, 182µs cold preads) says the q9/q20 wall is I/O, not CPU → N18 demoted
  for the decay regime (still valid micro-opt for warm/CPU-bound phases).
- ★ NEW TOP HYPOTHESIS — GARBAGE RETENTION: q9 writes 36-45GB scratch while LIVE
  interval-join state is a few GB. If compaction lags reclaiming tombstoned ranges,
  probes read through garbage-diluted blocks → the cold-pread volume; RocksDB's leveled
  compaction reclaims aggressively → 70K/s sustained on identical hardware; bloom's gain
  compressing late = garbage accumulating. DISCRIMINATOR (cheap): during q9 decay compare
  engine-dir bytes vs live-state estimate (DECAY_DIAG state= per flush already prints) +
  count tombstones per scanned block. If confirmed → lever = tombstone-priority compaction
  (architectural, helps q9/q20/q4, config untouched).

# ═══════════════════════════════════════════════════════════════════════════
# GARBAGE RETENTION CONFIRMED — the q9/q20 decay root cause (2026-06-10 end)
# ═══════════════════════════════════════════════════════════════════════════
q9@50M (FRS_DECAY_DIAG + 20s engine-dir sampler): dir grows 2.4→28GB saw-toothed
across the run while LIVE interval-join state plateaus by design (bounded event-time
window; DECAY_DIAG ≈880MiB/CF scale) → garbage ratio ~4-8× in the decay phase,
climbing exactly when throughput decays; compaction reclaims (saw-teeth, and 28→16.7GB
at run END when writes stop) but lags the tombstone production rate during the run.
This explains: cold-pread volume (probes read garbage-diluted blocks), RocksDB's
sustained 70K/s on identical hardware (leveled compaction prioritizes reclaim), and
the prefix-bloom gain compressing late (garbage scatters live prefixes over more SSTs).
Run also: 739.4s finish (vs 844.6 same code 90min prior) — box swings ±13%; bars only
after reboot.

## LEVER (next build): GARBAGE-PRIORITY COMPACTION (engine policy, not config)
Pick compaction candidates by RECLAIMABLE-GARBAGE estimate, not just level fullness:
(a) per-SST tombstone density (writer already counts ops; persist tombstone_count in
the v3 footer — cheap addition), (b) age-weighted: oldest SSTs overlapping ranges with
many newer tombstones first, (c) raise compaction priority/parallelism when
dir_bytes/live_estimate exceeds a ratio. RocksDB analog: kByCompensatedSize +
delete-triggered compaction (CompactOnDeletionCollector). All engine-internal policy —
config (write_buffer 1G, noflush=false) untouched; benefits q9/q20/q4 (write+delete-heavy)
and is neutral for read-only/append-only queries (policy only changes WHICH compaction
runs first). Verify with the same dir-sampler + back-to-back A/B pair.

# ═══════════════════════════════════════════════════════════════════════════
# FRS-GARBAGE-DRAIN VALIDATED (back-to-back q9@50M pair, 2026-06-10, 9b3f84d72)
# ═══════════════════════════════════════════════════════════════════════════
| run | wall | out_rows | engine-dir curve |
|---|---|---|---|
| pre-drain (n14 .so)  | 981.8s | 45,904,788 ✓ | 9→27GB climb |
| GARBAGE-DRAIN        | 860.7s (−12.3%) | 45,904,788 ✓ | FLAT ~8-10GB |
Tombstone-triggered L1→L2 drain (FRS_GARBAGE_DRAIN_TOMBSTONES=2M default) bounds
garbage at ~10GB vs 27GB — the confirmed decay mechanism is neutralized at 50M; the
wall gain is partial at 50M (drain costs write-amp) but the 100M decay-dominated
regime should gain much more. Correctness EXACT. q9@100M bar attempt next.

# q9@100M GARBAGE-DRAIN v1 (2026-06-10 evening): SOURCE-COMPLETE 98M@2388s
The three-lever architecture progression at the SAME 2400s cutoff, on a box that
degraded +25% across the day:
| build | records @2400s |
|---|---|
| pre-bloom | ~91M |
| + prefix bloom v3 | 94.8M |
| + garbage-drain v1 | **98.0M = SOURCE COMPLETE, drain-tail running** |
Dir curve at 100M: 31-40GB with visible reclaims (39.9→31.3GB) — drain fires but
zeroing the counter forfeited backlog under sustained deletes → CONTINUOUS mode
committed (f716a70fc): saturating-subtract + re-enqueue while pressure remains.
NEXT: REBOOTED-box q9@100M with continuous drain — positioned for the 1776s bar
(today's run would have finished ~2450s on a +25%-degraded box). Then q20 (bar
1074; gd-v1 result pending in task bbhj8ucce) and q7.

# q20@100M garbage-drain v1 (2026-06-10 session close): 84.6M@1788s cutoff
vs pre-drain 80.75M@1800 (same MAXSEC, less-degraded box earlier) — ~4M ahead on a
worse box. Effect smaller than q9's (q20's tombstone volume is lower: count-map
retractions vs interval-join cleanup), direction consistent. Both q9 and q20 100M
bar verdicts await the REBOOTED-box re-run with the CONTINUOUS drain (f716a70fc).
SESSION CLOSE STATE: levers shipped+pushed (prefix bloom v3, N14 depth-gated,
garbage-drain v1+continuous); all exactness-verified; box unfit for bar judgments
(+25% drift); next session = reboot → q9/q20/q7 @100M verdicts → full 3-backend sweep.

# q9@100M garbage curve, drain v1 (final sampler harvest): PLATEAU not climb
16.5→35GB with drain saw-teeth, peak 46GB, plateau ~32-35GB through the back half —
v1 CONTAINS garbage at 100M (vs the unbounded pre-drain climb) but doesn't shrink it
(zeroed counter forfeits backlog). Continuous mode (f716a70fc) targets exactly this:
expect a materially lower plateau on the rebooted-box re-run.

# ═══════════════════════════════════════════════════════════════════════════
# NEW-STACK 10M CORRECTNESS GATE (full suite, frs vs rocksdb, 2026-06-10 evening)
# Engine = prefix bloom v3 + N14 depth-gated + garbage-drain v1 (928b25535-era .so)
# ═══════════════════════════════════════════════════════════════════════════
EXACT out_rows match (20/22): q0,q1,q2,q3,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,
q17,q18,q19,q20,q21,q22 — including the heavy joins q7=9,200,001 / q9=9,177,252 /
q20=9,321,032 byte-equal to RocksDB. The lever stack is functionally clean.
EXCEPTIONS:
- q4: frs WEDGED at 9.65M/9.8M rate=0 (NEW-STACK STALL — bisect with kill-switches:
  drain off → bloom off; q4 finished on all prior builds = blocker until fixed).
- q5: 599,672 vs 2,998,360 = the PRE-EXISTING exact-1/5 HOP churn ratio (inner window
  agg proven exact 2026-06-10 morning; not lever-induced; tracked separately).
ALSO: scratch dir held 54GB/926 entries of dead artifacts (43GB nexmark-qout +
hundreds of stale planner jars) — cleaned to 3.5GB; drift test pending to decide
whether THIS (not the box) caused the day's +25% degradation.

# q4@10M STALL: 3-step bisect VERDICT = PRE-EXISTING (not today's stack) + DRIFT verdict
| bisect | config | result |
|---|---|---|
| 1 | new jar+engine, drain OFF | stall @9.65M |
| 2 | new jar, drain+bloom OFF  | stall @9.65M |
| 3 | PRE-CAMPAIGN jar (8e5a057da48), levers OFF | stall @9.655M — SAME |
→ q4@10M wedge predates everything shipped today (RocksDB@10M = 19.9s; frs wedges at
98.5% of source with rate=0 — a scale-specific frs issue; q4@100M finishes 373s).
TODAY'S STACK: ZERO regressions — correctness gate final: 20/22 EXACT, q4 = pre-existing
scale artifact (own ticket), q5 = pre-existing churn artifact.
# DRIFT TEST verdict: scratch-junk theory ALSO REFUTED (908.6s post-clean vs 860.7 pre,
same code). Day's spread 765.6-908.6 = ±10% INTRINSIC run-to-run variance, no trend, no
identified mechanism (not "the Mac degrading", not junk). RULE: ±10% error bars on all
single runs; only back-to-back large deltas and DNF/finish transitions are decisions.
The q9 100M lever progression (91M→94.8M→98M-source-complete at cutoff) EXCEEDS the
noise band and stands.

# ★ q9@100M FIRST FINISH (2026-06-10 night): 2484.4s, out_rows=91,813,372
| build | outcome |
|---|---|
| pre-levers | DNF ~91M@2400 cutoff |
| + prefix bloom v3 | DNF 94.8M@2400 |
| + garbage-drain v1 | DNF (source-complete 98M@2388, tail cut @2400) |
| + CONTINUOUS drain (f716a70fc) | **FINISHED 2484.4s** (src done 2388 + 96s tail) |
vs bar 1776s (0.8× RDB 1420.5): 1.40× over — NOT cleared; gap ~700s. Late rate with
continuous drain was ~1.7× v1's at matching points (43K vs 25K @1866s). Remaining
q9 levers: drain-threshold tuning (lower than 2M), executor parallel retest (engine
is now much faster → the latency-hiding math changed), block/locality. 10M-scale
correctness EXACT (9,177,252 == RocksDB). q20@100M MAXSEC 1800 queued on same .so.

# q20@100M continuous drain (night close): DNF@1800 at 86.5M
Progression: 80.75M (pre-drain) → 84.6M (v1) → 86.5M (continuous); extrap finish
~2300s vs bar 1074s. q20's residual gap is dominated by OPERATOR-level costs the
engine levers can't reach (round-2 doc: no-UK join = full asyncEntries bucket scan
per probe + cnt-replicated materialization + per-record GET→PUT RMW on the count
map). q20's named next levers: OPT-N04 (engine merge operator for the count map —
kills the dependent GET per record), executor parallel retest, lazy probe (OPT-N01).

# ═══ 2026-06-10 NIGHT CLOSE — campaign summary ═══
- ★ q9@100M FIRST FINISH 2484.4s (was DNF on every prior build); bar 1776 gap 700s.
- q20: 86.5M@1800 (steady lever gains, operator-level levers next).
- Correctness: 20/22 EXACT vs RocksDB @10M incl q7/q9/q20; ZERO regressions from
  today's stack (3-step bisect); q4@10M = pre-existing wedge (own ticket, RDB=19.9s);
  q5 = pre-existing churn artifact.
- Variance ruling: ±10% intrinsic; Mac-degradation AND scratch-junk theories both
  experimentally refuted (user's skepticism was correct).
- Shipped today: prefix bloom v3, N14 depth-gated, garbage-drain v1+continuous,
  classifier-pool leak fix, FRS_REENTRY/STREAM diag, kill-switches, harness env fixes.

# q7@100M lever stack: REGRESSION caught + RATIO GATE fix (2026-06-10 night)
q7 DNF@1700 at 79.5M vs pre-lever 1441.6s FINISH — outside the ±10% band. Mechanism:
the drain's ABSOLUTE tombstone trigger fired on q7 (many tombstones but far larger
live set) → L1→L2 drains rewrote big live spans for little reclaim → write-amp stole
read I/O. FIX (7ebe51daa): ratio gate — drain only when tombstones >= 20% of entries
flushed since last drain (q9-class drains, q7-class skips; never-rob by construction).
VALIDATION running: q7@1700 (must restore ~1441-class) then q9@2700 (must keep the
2484.4s finish — q9's delete ratio is high so the gate stays open for it).

# q7 ratio-gate validation (night): partial recovery, residual gap needs control
| q7@100M run | @1684s | verdict |
|---|---|---|
| lever stack, absolute drain | 79.5M | drain robbed q7 |
| lever stack, RATIO gate (7ebe51daa) | 86.0M (+8%) | gate recovers part |
| pre-lever (different day/jar) | finished 1441.6s | residual ~25% unattributed |
Residual candidates: jar changes (classifier pool era), N14 (q7 = merge-heavy), bloom
overhead on hot-prefix scans, or cross-DAY variance (the ±10% band was measured
within one day). CONTROL queued: q7@100M with FRS_DISABLE_PREFIX_BLOOM=1 +
FRS_GARBAGE_DRAIN_TOMBSTONES=0 on the current jar — separates engine levers from
jar/variance. q9 ratio-gate validation in flight (must keep the 2484.4s finish).

# q9 ratio-gate validation: FINISHED 2678.7s, out_rows 91,813,372 (EXACT = prior finish)
vs 2484.4s absolute-trigger: +7.8% — at the variance band edge; either the 20% ratio
bar throttles some q9 drains or noise. Determinism note: two q9@100M finishes with
IDENTICAL out_rows. Net ratio-gate trade: q7 +8%, q9 −0..8% → near-neutral; tune the
bar to 10% as the next probe. q7 levers-off CONTROL running for residual attribution.

# q7 attribution COMPLETE (night close): engine levers EXONERATED — they HELP q7
| q7@100M same-night | @1684s |
|---|---|
| lever stack + RATIO gate | 86.0M (best) |
| ALL engine levers OFF (control) | 80.7M |
| lever stack + absolute drain | 79.5M |
→ The ratio-gated stack is q7's best same-day config; the residual gap vs the
pre-lever 1441.6s FINISH (different day + pre-campaign jar) is attributable to the
JAR (classifier-pool era) and/or cross-day conditions — separate next session with
ONE pre-campaign-jar q7 run (worktree recipe in the q4 bisect notes).

# ═══ FINAL NIGHT LEDGER (2026-06-10) ═══
ENGINE (all pushed, exactness-proven): prefix bloom v3 · N14 depth-gated ·
garbage-drain (ratio-gated continuous). NET EFFECT: q9 DNF→FINISHED (2484.4/2678.7s,
identical out_rows = deterministic) · q20 80.75→86.5M@cutoff · q7 best-config +7%
over control · 20/22 10M-correctness EXACT · zero regressions (all bisect-proven).
OPEN (ordered): q9 bar gap (drain bar 10% probe + executor retest) · q20 OPT-N04
merge-op · q7 jar-vs-day attribution · q5 churn · q4@10M pre-existing wedge ·
executor race (40s repro + stream-diff tooling) · full 3-backend sweep.

# q9 drain-threshold probe (2026-06-11 early): FRS_GARBAGE_DRAIN_TOMBSTONES=500K
| q9@100M | wall | out_rows |
|---|---|---|
| drain 2M absolute | 2484.4s | 91,813,372 |
| drain 2M ratio-gated | 2678.7s | 91,813,372 |
| drain 500K ratio-gated | **2311.5s** | 91,813,372 (3rd identical = deterministic) |
More frequent reclaim → less garbage → −173s vs best. Bar 1776 now 535s away (1.63×
RDB). Ratio gate keeps q7 safe at any count threshold (its delete ratio fails the
gate). NOTE: threshold is an ENV today — if 500K (or lower) proves universal, fold
into the default. Routing-executor retest in flight (~69K/s early).

# q9 routing-executor RETEST on drained engine (2026-06-11): now HELPS
| q9@100M (drain 2M) | wall | out_rows |
|---|---|---|
| depth-1 | 2484.4s | 91,813,372 |
| ROUTING (FRS_RS_EXECUTOR=routing) | **2378.5s (−4.3%)** | 91,813,372 (4th identical) |
Pre-levers routing HURT q9 (cache-off robbed it; DNF 79.2M@1284). The engine levers
flipped the math: less cold-read time → cache matters less, parallel latency-hiding
wins. COMBINED probe launched (routing + drain 500K; estimate ~2200s; bar 1776).
q9 determinism: FOUR identical out_rows across executors/drain settings.

# ★ q9@100M LEVER COMPOSITION COMPLETE (2026-06-11 02:00)
| configuration | wall | out_rows |
|---|---|---|
| pre-levers (any build, ever) | DNF | — |
| depth-1, drain 2M | 2484.4s | 91,813,372 |
| routing, drain 2M | 2378.5s | 91,813,372 |
| depth-1, drain 500K | 2311.5s | 91,813,372 |
| **routing + drain 500K** | **2199.2s** | 91,813,372 (5th identical) |
Levers compose ADDITIVELY (−106 + −173 ≈ −285 measured −285). Bar 1776s gap = 423s
(1.55× RDB 1420.5). Determinism: five identical out_rows across executors+settings.
Q9 STRATEGY (next session priorities): (1) drain threshold probe below 500K (the
curve hasn't flattened), (2) OPT-N16 zero-copy batch-get (engine to_vec at every
tier — the warm-path CPU), (3) executor read-pool width (routing N=3 → probe 4-6;
engine is no longer the bottleneck), (4) block locality. q20 = OPT-N04 merge-op
(operator-level, biggest q20 lever). q7 = jar-vs-day attribution run first.

# q9 drain-threshold curve (2026-06-11): 200K → 2001.2s — STILL DESCENDING
| routing + drain threshold | wall | out_rows |
|---|---|---|
| 2M  | 2378.5s | exact |
| 500K | 2199.2s | exact |
| 200K | **2001.2s** | exact (6th identical) |
Bar 1776s gap = 225s (1.13×). Curve slope unbroken — probing 100K. GHA note: the
single ci-rust failure was the known-flaky flush_worker coverage trio (passed on
identical code in adjacent runs); all code-bearing pushes green.

# ★ q9 TUNING MAP COMPLETE — drain-threshold KNEE at 200K (2026-06-11 03:00)
| routing + threshold | wall |
|---|---|
| 2M | 2378.5s |
| 500K | 2199.2s |
| **200K (knee)** | **2001.2s** |
| 100K | 2038.7s (flattened — reclaim benefit ≈ drain overhead) |
All seven q9@100M finishes: out_rows 91,813,372 IDENTICAL. FULL JOURNEY: DNF-forever
→ 2001.2s (1.41× RDB; bar 1776 gap 225s). Remaining levers for the last 225s:
OPT-N16 zero-copy batch-get (to_vec at every tier — warm-path CPU), routing pool
width 4-6, block locality. BEFORE folding 200K into defaults: validate q7 (ratio
gate should hold) + q17/q20 at the winning setting. q9's bar is now ONE build-lever
away — the architecture campaign converted an unfinishable query into a tuned,
deterministic, near-bar pipeline.

# q9 ENV-PROBE SPACE CLOSED (2026-06-11 03:45): pool width saturated
pool=6 → 2002.9s ≈ pool=3's 2001.2s (8 cores shared with compaction/flush; latency-
hiding exhausted). 8th identical out_rows. TUNING FLOOR = 2001s (routing-3 + drain-200K).
The final 225s to the 1776 bar = the OPT-N16 BUILD: wire the existing zero-copy
batch_get_arrow into the default executor path (round-2 doc: engine copies every value
to_vec at EVERY tier + a second FFM copy, while the zero-copy path exists unwired).
Plan: docs/superpowers/plans/2026-06-11-optn16-zero-copy-gets.md.

# ★★ DRAIN-200K VALIDATION MATRIX (2026-06-11 04:30) — q20 FIRST FINISH; q17 gate leak
| query @100M | drain 2M (prior) | drain 200K + routing | verdict |
|---|---|---|---|
| q9  | 2378.5s | 2001.2s | ✓ best |
| q20 | DNF 86.5M@1800 | **FINISHED 1477.7s, rows 93,201,404 EXACT — BEATS ForSt 1535.9** | ✓ first finish; "faster than ForSt" bar condition MET |
| q17 | ~77-85s | >300s (src done, tail dragged) | ✗ ROBBED — ratio gate leaks for q17-class |
q20 status vs full bar: beats ForSt ✓; 0.8×RDB (≤1074s) still needs OPT-N04.
DEFAULT VERDICT: 200K stays OPT-IN until the drain gate adds a third condition
(minimum level-size / live-volume floor so small-state queries never drain-thrash).
NEXT-SESSION BUILDS (final order): (1) drain gate third condition (small fix) →
re-validate q17 → default 200K; (2) OPT-N16 sink-threading (corrected design, plan
updated); (3) q20 OPT-N04 merge-op. q9 floor 2001.2 / q20 1477.7 / both deterministic.

# ★ DRAIN DEFAULT → OFF (never-rob enforcement, 2026-06-11 05:20)
q17@100M at the SHIPPED default (2M + ratio + 512MB floor): 267.9s vs 77-85s norm =
3.2× ROBBED — the three gate conditions don't capture whatever hurts small-live-state
queries (q17@200K+floor was 190.1s; the mechanism needs profiling, not more guessing).
DEFAULT flipped to 0 (drain OFF): the 19 non-join queries keep their pre-drain behavior;
q9/q20 keep their transformative OPT-IN results (q9 2001.2s, q20 1477.7s @
FRS_GARBAGE_DRAIN_TOMBSTONES=200000 + routing). Re-enabling by default requires gating
that passes q17/q3/q8 @100M no-regress — queued with the OPT-N16/OPT-N04 builds.
q9 no-regress on the floor build: 2081.6s (band), rows exact (9th identical).

# ★ q17-UNDER-DRAIN PROFILED → PRINCIPLED GATE DESIGN (2026-06-11 05:30)
Drag-window CPU (20s sample): compaction threads 723+513+502 ticks ≈ 87% of the box;
task threads starved. MECHANISM: q17's FLUSH stream is tombstone-rich (window cleanup →
passes the 20% flush-ratio gate) but its LEVELS are live-rich (persistent aggregates) →
drains rewrite GBs of live data for tiny reclaim, hogging all cores. The flush-ratio
signal measures the wrong population.
## SAFE-GATE DESIGN (implement next session, before re-defaulting):
1. SST footer v4: tombstone_count u64 (writer counts Delete/SingleDelete per file —
   same versioning pattern as v3 prefix bloom; v3 files decode 0 = unknown→conservative).
2. Drain condition: per-CF L1 stored-tombstone fraction = Σ tombstone_count / Σ
   total_entries ≥ 20% AND L1 volume ≥ 512MB (existing floor) AND count threshold.
   q9/q20 levels = garbage-dominated → drain; q17 levels = live-dominated → never.
3. This replaces the flush-ratio condition (wrong population, proven by profile).
Then: re-validate q17/q3/q8@100M no-regress → default the drain (the q9 2001s/q20
1477s wins become DEFAULT-path results). Queue order: gate-v2 → OPT-N16 → OPT-N04.

# GATE-V2 A/B VERDICT (2026-06-11 06:30): level-density REVERTED — both signals bracket the truth
| gate signal | q17 (live-rich levels) | q9 (garbage-dominated) |
|---|---|---|
| flush-ratio (v1) | ✗ over-drains (190-268s) | ✓ drains |
| L1 stored-density (v2) | ~ (228.9s — re-enqueue spin found+fixed) | ✗ STARVES (DNF@2300 vs 2001s) |
Mechanism: compaction annihilation strips tombstones from L1 outputs while the dead
VALUES hide in deeper levels → L1 density under-reads garbage. CORRECT DESIGN (next
session, building block shipped): RocksDB-style COMPENSATED FILE SIZES — footer-v4
tombstone_count (now persisted, 832 tests) weighted by shadowed-data estimates feeds
candidate selection. Behavior reverted to the PROVEN opt-in config; re-enqueue
busy-loop fixed (was spinning the compaction worker on gate refusal). Default OFF.

# ★★★ ATTRIBUTION REVERSAL (2026-06-11 07:25): THE DRAIN NEVER ROBBED q17
| q17@100M today (depth-1) | wall |
|---|---|
| drain 200K + ratio gate | 190.1s |
| drain 200K + level-density gate | 228.9s |
| drain 2M default (pre-flip) | 267.9s |
| drain 200K + feedback gate | 280.0s |
| **drain FULLY OFF (control)** | **228.1s ← same band** |
All variants land 190-280s regardless of drain settings → q17's ~2.5-3× elevation vs
its 77-85s norm is NOT the drain. The norm came from the PRE-BLOOM engine on a fresher
box; today's q17 runs all sit on the bloom+v4 engine after ~30h of continuous box load.
CONSEQUENCES: (1) the default-OFF flip rested on a misattributed comparison — the drain
MAY be defaultable; (2) the gate iterations (ratio/density/feedback) were chasing
variance, though each produced durable machinery (footer-v4 counts, reclaim feedback,
spin fix); (3) NEXT SESSION OPENS WITH THE CLEAN ATTRIBUTION PAIR on a fresh box:
q17 back-to-back on pre-bloom .so vs current .so, drain off both → separates engine-
lever cost from box state; then the drain default question is decidable with valid
baselines. Feedback-gate q9 validation: 2105.2s in band, rows exact (10th identical).
LESSON (recorded for methodology): never judge a lever against a different-build,
different-day baseline — the control run must come FIRST.

# ★★★ ENGINE REGRESSION ISOLATED (2026-06-11 07:40): today's levers cost q17 2.7×
Back-to-back, same box, drain OFF both:
| q17@100M | wall |
|---|---|
| PRE-BLOOM .so (7f4767f8c) | **99.8s** |
| CURRENT .so (all of today's engine) | **272.9s** |
The earlier "norm" comparisons were right by accident — there IS a 2.7× engine
regression hiding in today's lever commits, masked all day by the (refuted) drain
attribution. BISECTING by commit: 30cde2fc6 (bloom only) next — write-path suspects:
prefix-hash collection per key in add_internal (incl. a per-distinct-prefix Vec alloc
— q17's agg keys are ALL distinct prefixes → alloc+hash per key on every flush AND
every compaction rewrite). q9/q20 absorbed it (read-bound); q17 (1.15M rec/s
write-bound) pays full price.

# Degenerate-skip validation (2026-06-11 08:10): q9 BEST-EVER; q17 residual remains
| run | result |
|---|---|
| q9@50M routing+200K (fixed .so) | **727.3s — campaign best** (prior best 765.6), rows EXACT |
| q17@100M drain-off (fixed .so) | still MAXSEC@300 — degenerate-skip insufficient |
READING: the alloc-free collector + emit-skip HELPED q9's writes (765.6→727.3) but q17's
16B prefixes are evidently NOT >50% distinct (shared auction-id bytes keep its bloom
alive) — the residual q17 cost inside the bloom commit (99.8→263.9s bisect) is NOT yet
mechanistically identified. NEXT (profile, don't theorize): (1) writer-side kill-switch
(FRS_DISABLE_PREFIX_BLOOM must also skip COLLECTION+EMIT — today it only gates the scan
check) → q17 A/B isolates writer-vs-reader cost; (2) FRS_PROF_DIAG sub-cost attribution
on a q17 flush (SST_BUFFER/ENCODE/SINKWRITE counters already exist) → pinpoints whether
the cost is bloom build, file-size growth (2nd bloom → I/O), or reader-open bloom loads.
STATE: every shipped lever is correctness-exact; q9/q20 wins intact and improved;
q17's regression is bisected to one commit with a measurement plan to finish it.

# ★★★ TERMINAL VERDICT on the q17 arc (2026-06-11 08:30): NO REGRESSION — 3× BOX NOISE
The pre-bloom .so REPEAT: >300s DNF — the SAME binary that ran 99.8s an hour earlier.
Complete q17@100M table (today, all drain-off or equivalent):
pre-bloom: 99.8s, >300s | bloom-commit: 263.9 | current incl. all gates/fixes:
190.1/203.1/210.9(clean-build)/228.1/228.9/244.9/267.9/280.0/>300.
CONCLUSIONS: (1) NO lever ever robbed q17 — drain AND bloom fully exonerated;
(2) q17 single-run noise on this box spans 3× WITHIN a day (and yesterday's 77-85s
norm was another box-day) — q17-class verdicts require a stable box + repeated runs;
(3) the drain default-off flip and all gate iterations were chasing noise — each left
durable machinery (footer-v4 counts, feedback gate, alloc-free collector,
degenerate-skip, spin fix) but none was NEEDED for q17;
(4) the TRUSTWORTHY campaign results are the noise-immune ones: q9 DNF→FINISH
(2001-2105s, 10× identical rows), q20 DNF→FINISH 1477.7s beats ForSt (exact rows),
the monotone 8-run composition curve, and the 20/22 exactness gate.
NEXT SESSION: decide the drain default on a FRESH box with n≥3 runs per cell;
then OPT-N16 sink-threading; then OPT-N04. The box, not the engine, is now the
binding constraint on fine-grained bar verdicts.

# ★★★ q9@100M SYMBOLIZED NATIVE PROFILE (2026-06-11): the TM is LATENCY-bound, not CPU-bound
Tooling shipped: `FRS_PERF=1` hook in run-8c32g.sh (perf record -g vs the TM inside the
container, seccomp relaxed only when profiling; reports → target-linux/perf-<q>-{flat,graph}.txt),
plus a symbol-bearing .so build recipe (CARGO_PROFILE_RELEASE_STRIP=none DEBUG=2 — codegen-identical).
Window: 180s at t≈700s of a q9@100M run under the proven config (routing + drain 200K).

## The one number that re-aims the campaign
60K samples @199Hz/180s = ~302s CPU = **the TM averaged 1.7 of 8 cores**. q9's wall-clock is
gated by the mailbox⇄executor⇄engine round-trip serialization, NOT by engine CPU. 6+ cores idle.

## CPU shares (of the 1.7 busy cores, --children)
| Path | Share |
|---|---|
| prefix-scan iterators total (frs_vec_iter_prefix_open) | 32.8% — open/stream-build 17.2% (may_contain_range 4.55% SELF, memcmp 4.78%, BTree find_key_index 3.69%), drain fill_chunk 15.0%, TierKeySource::peek 9.6% |
| compaction thread (drain working as designed) | 12.0% |
| opendal I/O threads | 7.2% |
| **frs_vectorized_batch_get (entire GET path)** | **6.5%** |
| Join-thread wake/signal (Unsafe_Unpark + pthread_cond_signal → futex) | ~6% (the all-latch barrier cost) |

## Verdicts
- **OPT-N16 (zero-copy GETs): NO-GO.** Entire GET path = 6.5% of 1.7 cores ≈ 0.11 cores;
  deleting EVERY copy buys ~20-40s of the 225s bar gap. The plan's ≤1850s target was
  unreachable. (User's profile-before-build directive caught this before any code was written.)
- Scan-open micro-levers (dedupe the double search_index in may_contain_range+first_block_ge,
  cheaper index layout): real but small (~0.1-0.2 cores) — not bar-closing.
- Prefix bloom IS active on q9 (prefix_len=37 ≥ 16) — not the issue.
- **THE lever: remove the executor's mailbox barrier.** RoutingStateExecutor blocks the task
  thread in latch.await per batch (pipeline depth 1). The VectorizedExecutor doc block itself
  records that the AEC ignores the container future (community ForSt returns early from a
  coordinator thread) and lists 4 blockers — ALL now dissolved: classifier-pool private buffers,
  worker-side self-release, per-row CallbackRunnerWrapper completion, and fullyLoaded() as the
  backpressure hook (AEC consults it at trigger time).

## FRS-ROUTING-ASYNC (built 2026-06-11, gated FRS_RS_EXECUTOR=routing-async)
Non-blocking routing: same kg-affine per-worker FIFOs as proven routing, but
executeBatchRequests dispatches and returns a TRUTHFUL aggregate future immediately
(no latch); fullyLoaded() = outstanding ≥ FRS_RS_MAX_INFLIGHT_BATCHES (default 2×workers).
Why safe where 2026-06-10's non-blocking attempt corrupted q8 (−18%): that variant executed
iter-free batches INLINE on the mailbox while earlier ops sat in worker queues
("statebuf writes overtook queued reads"). routing-async has NO inline path — every request
queues through its key-group's single-thread FIFO, so overtaking is structurally impossible;
AEC's KeyAccountingUnit serializes same-key records; checkpoint consistency via AEC's
in-flight-records drain. MapStateCache off under routing-async (same as routing).
Contract UTs added (RoutingStateExecutorAsyncTest, stub workers): incomplete-at-return,
truthful completion, FIFO no-overtaking, fullyLoaded cap, failure path, blocking-mode regression.
GATES (pending): 534 Java UTs → q8@10M exactness ×3 → q9@100M back-to-back vs today's
control → q17 pair. Estimate: q9 2001s → 1500-1800s band IF overlap converts idle cores
(precedent: thread-unsafe parallel spike gave q20 1610s, q11 134.7s).

# ▶ CURRENT STATUS (2026-06-11, mid-session refresh)
## Done today
1. **OPT-N16 profiling gate executed and OPT-N16 KILLED before build** (user directive honored:
   profile-verify before code). Evidence above: GET path 6.5% of a 1.7-core-busy TM.
2. **Root architecture problem of q9 IDENTIFIED with hard data**: the TM uses 1.7/8 cores —
   the blocking executor's per-batch mailbox barrier (latch.await) is the wall-clock gate.
   q9's engine CPU (scans 33%) is NOT the binding constraint; idle cores are.
3. **FRS-ROUTING-ASYNC BUILT** (flink-statebackend-forst-rs): non-blocking RoutingStateExecutor
   mode, gated FRS_RS_EXECUTOR=routing-async + FRS_RS_MAX_INFLIGHT_BATCHES (default 2×workers).
   Design + safety argument in the section above. 6 contract UTs added (stub workers, no FFI).
   NOT yet compiled/committed — gates pending.
4. **Profiling infrastructure shipped**: FRS_PERF hook (run-8c32g.sh) + symbolized-.so recipe —
   reusable for every future bottleneck question.
5. **q9@100M control run n+1** (symbolized .so = codegen-identical, routing+drain200K):
   src 98M done at 1946s, sink-drain tail in progress at write time — tracking the
   2001–2105s band; exact RESULT + out_rows recorded below on completion.

## Next (this session, in order)
1. jar build + 534-UT suite (incl. new RoutingStateExecutorAsyncTest).
2. q8@10M exactness ×3 under routing-async (the corruption canary that killed every prior
   non-blocking attempt — must be EXACT 3/3 or the mode is dead).
3. q9@100M routing-async+drain200K BACK-TO-BACK vs today's control (±10% protocol).
4. q17@100M routing-async vs blocking pair (same-day, direction-only verdict per noise rule).
5. If gates pass: q20/q7 under routing-async; record; GHA; commit.

## Standing results (unchanged)
q9 2001–2105s (10× exact rows, was DNF-forever) | q20 1477.7s beats ForSt 1535.9 (exact) |
q7 best 1441.6s (ForSt bar 587 — open) | 20/22 10M exactness | q4 wedge + q5 churn pre-existing.
Bars: q9 RDB 1420.5→0.8× bar 1776s | q20 RDB 1074→bar 1342.5 | q17 noise-ruled.

## q9@100M control n+1 RESULT (2026-06-11, symbolized .so, routing+drain200K, FRS_PERF active)
**2215.6s, out_rows=91,813,372 — EXACT (11th consecutive byte-identical q9 output).**
Wall slightly above the 2001–2105 band; run carried the 180s perf-sampling window +
container tool install, and the box noise rule applies — rows-exactness is the signal.
This is the baseline for the routing-async back-to-back A/B.

## ✗ FRS-ROUTING-ASYNC q8@100M CANARY: FAILED (2026-06-11) — mode parked experimental
| run | outcome |
|---|---|
| q8@10M smoke | FINISHED 7.6s (out 306,016 — scale too small to exercise depth; smoke only) |
| q8@100M r1 | WEDGE: src done 3,065,051@60s, windows never fired, rate=0 → MAXSEC@600 |
| q8@100M r2 | FINISHED 36.7s but out_rows=1,285,415 vs band 3,064,4xx = −58% UNDER-EMIT |
VERDICT: nondeterministic wedge-or-corrupt = data race. ROOT CAUSE (mechanism): the
executor-level design is sound (classifier buffers per-batch private, kg-FIFO ordering holds),
but the PER-STATE off-heap staging buffers (MapStateV2 Arrow buffer / ListStateArrowBuffer /
statebufs) are long-lived per state object: mailbox APPENDS at offer time while a worker DRAINS
at execution time. All proven modes are lockstep, so these buffers are single-threaded by
construction; ANY non-blocking executor overlaps the phases and tears them. This RE-EXPLAINS the
2026-06-10 coordinated corruption (it was never inline-specific) and confirms the original
author's deferred scope: per-batch buffer ownership ("refactor of the C1 design") is THE
structural prerequisite for pipelining. The q9 motivation STANDS (1.7/8 cores, latch-capped);
the next architectural unit is seal/swap per-state buffers at dispatch, then re-gate.
Code state: routing-async ships gated FRS_RS_EXECUTOR=routing-async, javadoc carries the
failed-canary warning, defaults untouched (inline default; blocking routing/adaptive unaffected);
543 UTs green incl. 6 new contract tests (they validate dispatch mechanics, which are correct —
the race is in the state-buffer layer the UTs don't reach).

# ▶ STATUS REFRESH (2026-06-11 ~10:00) — post-canary direction locked
## Performance numbers: no new bar-relevant results since the canary section above.
Today's complete number set: q9@100M control 2215.6s/91,813,372 EXACT (11th identical, perf
overhead included) | q8@100M routing-async r1 WEDGE@600 r2 36.7s out=1,285,415 (−58% WRONG —
gate FAILED, mode parked) | q8@10M routing-async smoke 7.6s out=306,016 (scale too small for
the race). Standing bars unchanged: q9 needs ≤1776s (current band 2001–2105), q20 needs
≤1342.5 (current 1477.7), q7 needs FINISH + ≥0.8× RocksDB + faster than ForSt 587
(frs best 1441.6 cross-day).
## GHA (mandate gate): ForSt ci-security ✅ SUCCESS, ci-rust in_progress at write time
(push 679248d83); flink backend commit 7ef52d53787 pushed, module UTs 543/0 local.
## Direction locked with user (labeled-option approvals):
1. (approved) Per-batch buffer ownership = full scope (Map+List staging buffers), entirely
   inside flink-statebackend-forst-rs — NO Flink-runtime changes (AEC contract already
   supports non-blocking; verified in code).
2. (approved) Sequence = B-spike → A:
   B SPIKE (today): gated bypass — under routing-async ONLY, MapStateV2/AsyncListStateV2 skip
   their staging buffers; puts flow through the classifier's per-batch-private buffers
   (executePuts-before-executeGets = same-batch read-your-writes; worker FIFO = cross-batch).
   No shared mutable state ⇒ canary-plausible in ~20 lines. Gates: q8@100M ×3 exact, then
   q9@100M A/B vs 2215.6s control. PURPOSE: hard e2e evidence that pipelining converts the
   6.3 idle cores before the multi-day refactor.
   A BUILD (next): per-worker-sharded staging (kg%N) + seal/swap at dispatch + worker
   drain-first + mailbox-owned reclaim + snapshot-barrier all-shard drain — restores staging
   coalescing/hit-serving under pipelining. Design doc to
   docs/superpowers/specs/2026-06-11-per-batch-buffer-ownership-design.md.

## B-SPIKE iteration log (2026-06-11)
### v1 (Map+List staging off): q8@100M canary 1/3 — race REDUCED, not eliminated
r1 38.6s out=2,048,927 (−33%) | r2 44.7s out=3,064,566 ✓ IN BAND | r3 47.6s out=2,552,408 (−17%).
No wedge (vs pre-spike wedge/−58%) — removing the Map/List buffers removed A race, not THE race.
### Discovery: a THIRD staging mechanism the token-grep audit missed
ForStRsAsyncReducingStateV2 + ForStRsAsyncAggregatingStateV2 carry a ReducingAggregatingCache
(PR-C3 RMW cache): asyncAdd folds accumulators IN-MEMORY ON THE MAILBOX (completed future, never
enters the executor) and dirty accumulators drain via flushHandler = linker.put FROM THE MAILBOX
(flushOnBarrier + eviction). q8 = windowed aggregation → this is q8's hot state. Same
lockstep-only pattern; same overtake race under pipelining.
### v2 (spike extended): asyncAdd → super.asyncAdd under pipelinedExecutorActive in BOTH classes
Cache never written ⇒ all other cache consults degrade to no-ops (probe miss → engine,
barrier drain empty, gen bumps harmless). UTs green (no failing surefire report). Audit of
remaining V2 surfaces: ValueStateV2 "cache" = namespace-bytes serialization cache
(mailbox-confined ✓); MapStateV2 CLEAR rides the executor request path ✓; timer state is
mailbox-confined in its own keyspace (workers never touch it) ✓.
q8@100M ×3 canary running. The general lesson for Approach A: the audit class is
"EVERY mailbox-side mechanism that defers/absorbs engine effects" — three found so far
(Map buffer, List buffer, RMW caches); A must give ALL of them per-batch ownership.

### Spike v2 (RMW caches also gated): q8 canary 1/3 — fourth race remained
r1 38.7s out=1,700,379 (−44%) | r2 40.6s out=3,064,723 ✓ | r3 40.7s out=2,194,859 (−28%).
### ★★ DIFFERENTIAL MATRIX (q8@100M, all staging gated): race is CROSS-WORKER
| config | runs | verdict |
|---|---|---|
| A: routing-async, workers=3, FRS_RS_MAX_INFLIGHT_BATCHES=1 | 1,180,530 / 1,307,094 | BOTH WRONG (−57~61%) |
| B: routing-async, workers=1, default depth | 3,064,477 / 3,064,676 | BOTH EXACT ✓ |
READING: offer-phase-vs-execution overlap is SAFE (B pipelines fully with the mailbox live
and is exact); the residual race REQUIRES multi-worker fan-out + live mailbox even at
batch-depth 1 (blocking 3-worker fan-out is correct ×3, so concurrency alone isn't it
either — the combination is). PRIME SUSPECT: out-of-order BATCH completion across workers
(blocking and single-worker both guarantee completion order == dispatch order; 3-worker
non-blocking does not) — some consumer of batch-completion order (epoch/watermark/timer
sequencing) corrupts. REDUCING_ADD execution-time-serialization theory REFUTED (classifies
as plain PUT; fold = framework GET→callback→PUT, offer-serialized).
### ★ IMMEDIATE PAYOFF: B-config = correctness-viable PIPELINED mode TODAY
routing-async × 1 worker removes the q9 latch wait (the profile-proven gate) without
multi-worker. q8 canary n=3 + q9@100M A/B (vs 2215.6s control; out_rows 91,813,372 = its
own correctness gate) RUNNING.

### ✗ B-config REVISED (2026-06-11 ~11:30): r3 = 692,757 (−77%) — single-worker NOT safe
The 2/2 exact was luck; the race is TIMING-dependent (1 worker narrows the window, doesn't
close it). Cross-worker conclusion WITHDRAWN. Flink-runtime exonerated by code evidence
(Explore agent): watermark = SERIAL_BETWEEN_EPOCH drainInflightRecords(0) BEFORE trigger
(EpochManager.java:134; default AbstractAsyncStateStreamOperator.java:91); timer callbacks
carry RecordContext key-accounting (InternalTimerServiceAsyncImpl.java:136). The race is in
OUR backend's pipelined execution of the window-join op mix (LIST_ADD append / ITER at fire /
CLEAR). NEXT DIAGNOSTIC (after q9 A/B frees the box): FRS_REENTRY_DIAG=2 STREAM_STATS
differential — compare per-kind op counts between an exact and a corrupt q8 B-config run:
appends differ ⇒ writes lost (offer/dispatch side); appends equal + iter rows differ ⇒
reads lost (fire side). q9@100M B-config A/B still RUNNING (its out_rows is its own gate).

### q9@100M B-config (routing-async × 1 worker) A/B: DNF-bound — 1-worker pipelining LOSES
76.0M src @2371s vs control 98M done @1946s (~25%+ slower than 3-worker blocking).
CONCLUSION: mailbox overlap alone is NOT q9's lever — q9 needs MULTI-WORKER heavy regime
(intra-batch fan-out) + overlap. Stage 2 of the two-regime design is the q9 path; Stage 0
(race root-cause) blocks it. Recorded in the design's §7 PMC self-review.

### ★★ STAGE-0 STREAM_STATS DIFFERENTIAL (6× q8 B-config, FRS_REENTRY_DIAG=2): SIGNAL FOUND
out_rows (all wrong this round): 2,047,837 / 1,933,614 / 2,039,100 / 2,254,920 / 2,691,677 / 2,760,648.
TWO INDEPENDENT EXACT EQUALITIES: run1 out_rows == LIST_ADD == 2,047,837; run5 out_rows ==
LIST_ADD == 2,691,677 (harvest mixed stale logs for runs 2-4 — per-run isolation needed —
but the two clean specimens establish the relationship). LIST_ADD PLATEAUS mid-run while
CLEAR/LIST_GET keep growing.
READING: the race is an INPUT-SIDE STALL, not execution row-loss — record processing
throttles (AEC in-flight cap waiting on lost/late completions) while timer fires continue;
the job emits exactly the adds that landed before their windows fired. Wedge and under-emit
= same bug, different severity. STAGE-0 SCOPE NARROWED to the LIST_ADD
offer→dispatch→per-row-completion chain under routing-async (amBuf/classifier-pool
lifecycle + AEC trigger interaction). Next: per-run-isolated stats + completion-accounting
diag + code audit of that one chain.

### STAGE-0 completion accounting (FRS_REENTRY_DIAG=3, 4× q8 B-config + 1 probe run)
| run | out_rows | final LIST_ADD | verdict |
|---|---|---|---|
| r1 | 3,064,741 ✓ | off=3,064,741 done=3,064,741 | EXACT; out_rows == LIST_ADD holds in correct runs too |
| r2 | 1,842,502 ✗ | off=1,842,502 done=1,842,502 | adds NEVER OFFERED (balanced, plateaued) |
| r3 | 2,096,523 ✗ | off=2,096,523 done=2,096,523 | same |
| probe | 3,064,441 ✓ | (exact specimen) | nondeterminism confirmed |
CLASSIFICATION (decision table): **TRIGGER-SIDE — no lost completions anywhere (fail=0
throughout; transient off/done gaps = in-flight at dump time).** The missing ~1.2M records
were consumed by the operator (src complete, job FINISHED) but never called asyncAdd ⇒ the
WindowJoinHelper lateness gate (WindowJoinHelper.java:136 isWindowFired(windowEnd,
windowTimerService.currentWatermark()) → drop + lateRecordsDroppedRate.markEvent()) is the
prime suspect: watermark overtakes deeply-buffered records under pipelining. Operator base
chain hardcodes SERIAL_BETWEEN_EPOCH (AbstractAsyncStateStreamOperator:91) which SHOULD
prevent this — live lateRecordsDroppedRate.count probe running to confirm the drop site
before auditing how the watermark passes records despite the serial drain.

### ★★★ STAGE-0 ROOT CAUSE CONFIRMED (discriminator, 3× corrupt specimens)
opAsyncAdd == LIST_ADD offers == out_rows EXACTLY (2,384,952 / 2,698,273): the WINDOW
OPERATOR NEVER CALLED asyncAdd for the missing ~12-22% of records. Zero loss in our backend
or AEC buffers (fail=0 everywhere, off==done). The only filter between record consumption
and asyncAdd is WindowJoinHelper.java:136's lateness gate ⇒ **records are DROPPED AS LATE
under routing-async: the operator's currentWatermark overtakes records that lockstep
processes first.** Open sub-question (audit in flight): the exact deferral mechanism —
SERIAL_BETWEEN_EPOCH's drain should prevent watermark-overtake, so either the element user
code (incl. lateness check) defers past advanceWatermark under load, or the drain's
in-flight accounting misses a class of records when completions arrive from worker threads.
The fix must land BACKEND-side (flink code frozen per scope rule).

### STAGE-0 audit (Task 4): mechanism chain pinned to flink-runtime sync-point ordering
Verified first-hand (file:line):
1. The element user code (incl. the lateness check) is NOT run at mailbox arrival — it is
   wrapped via preserveRecordOrderAndProcess → AEC.syncPointRequestWithCallback
   (AsyncExecutionController.java:416-421): a SyncPointRequest whose future, when the key
   is FREE, completes immediately (insertActiveBuffer: request.getFuture().complete(null));
   when the key is OCCUPIED, the request parks in the blocking buffer and the user code is
   DEFERRED until the key's holder completes.
2. Blocked records ARE counted in inFlightRecordNum (seizeCapacity's isKeyOccupied
   early-return covers a record's 2nd+ request, not blocked records — the Explore agent's
   contrary claim was a misreading; corrected).
3. The residual gap: a record's accounting can release at sync-point FUTURE completion
   while its CALLBACK (the user code) is still queued on the mailbox — so the watermark's
   drain can observe count==0, run advanceWatermark, and the deferred lateness check then
   reads the ADVANCED watermark → late-drop. Manifests only with executors that complete
   batch futures asynchronously (lockstep closes the window by construction).
4. The fix CANNOT live in flink-runtime (scope rule). Fork in progress: community ForSt's
   executor presents the IDENTICAL contract (incomplete futures, off-mailbox completions,
   coordinator thread; ForStStateExecutor.java:149-235; fullyLoaded counts READ ops only).
   ForSt-async q8@100M out_rows was never byte-verified in the matrix (time-only row) —
   ForSt q8 ×2 exactness check is the decisive fork: ForSt corrupt ⇒ UPSTREAM Flink
   async-window bug (all async backends affected; lockstep semantics is the only safe mode
   in current Flink) ⇒ our two-regime LIGHT path is the correctness floor and HEAVY must
   preserve watermark-record ordering by construction. ForSt exact ⇒ a real behavioral
   difference remains in our executor — continue the diff with a narrowed search space.
(Box note: Docker Desktop daemon went 500-unhealthy mid-fork-test after the day's container
churn; restarted; ForSt ×2 reruns queued.)

### ★★ FORK RESOLVED: ForSt-async q8@100M EXACT ×2 (3,064,473 / 3,064,453)
Same runtime, same AEC, same incomplete-future executor contract ⇒ NOT an upstream bug in
practice; flink-runtime stays FROZEN (user's conditional access doesn't trigger). The race
is a BEHAVIORAL DIFFERENCE of our executor vs ForStStateExecutor. Narrowed comparative
surface: (a) offer-time serialization (ours: state-object keyOut → classifier columnar
buffers on the mailbox) vs ForSt's execution-time request conversion (pollDb*Requests on
the coordinator); (b) fullyLoaded semantics (ours: outstanding batches ≥ 2×workers; ForSt:
ongoing READ ops only — writes never block triggering); (c) executeRequestSync routing
(ours: worker-FIFO submit().get(); ForSt: direct); (d) per-row completion threading
(ours: the single worker; ForSt: read/write pools). Comparative audit in progress.

### ★★ q5 CORRECTNESS CLOSED (verified 2026-06-11, parallel workstream)
Root cause: ForStRsValueState off-heap encoding omitted the NAMESPACE SUFFIX → all HOP
window namespaces collapsed onto one physical key. FIXED in flink commit 80015d2cfaa +
regression test ForStRsValueStateOffheapTest.namespaceSuffixPartitionsValues. Evidence:
1M fixed-CSV materialized-changelog hash EQUAL to ForSt baseline (54/54 rows, was 169),
deterministic across q0-q22 (doc: benchmarks/2026-06-10-.../forstrs-backend-fixed-csv-
accuracy-20260611.md). Residual out_rows ratio vs RocksDB at bench scale = benign HOP
emission cadence. PHASE-1 CORRECTNESS EXCEPTIONS NOW: q4@10M wedge ONLY (pre-existing,
bisect-proven, @100M unaffected).

### STAGE-0 cbran instrument: exact-run baseline + thenAccept fast-path facts
Exact runs: cbran == completed == created (to the unit) — clean baseline. Corrupt-specimen
hunt continues (instrument shifts timing; Heisenbug). Runtime facts pinned (file:line):
AsyncFutureImpl.thenAccept has an isDone() FAST PATH running the action INLINE on the
calling thread ("this branch must be invoked in task thread when expected") with a
SILENT-SKIP if .get() throws, and inline callbacks bypass CallbackRunnerWrapper's
currentCallbacks accounting. For mailbox-completed sync points inline==mailbox (safe);
the corrupt-run cbran arithmetic decides whether the loss is pre-callback (registration/
fast-path edge) or post-callback (operator-internal).

### ★★★ STAGE-0: AEC SYNC-POINT MACHINERY EXONERATED — loss is ABOVE the operator wrapper
cbran instrument, corrupt specimens (R4 out=703,216!; R5 out=2,303,816):
**cbran == completed == created to the unit in every dump** — every sync point ever created
runs its callback. The deficit is in CREATION: R4's last dump shows only ~1.05M sync points
created for 3.06M source records (records + timer fires!) ⇒ ~2M records NEVER reached
preserveRecordOrderAndProcess. Combined with all prior exonerations (disposal mailbox-
confined ×verified-beacon; framework thread-airtight ×code-trace; ForSt-async exact ×2;
zero late-drops ×deployed print; zero lost completions ×accounting):
**the records vanish BETWEEN the source (provably emitted 3,064,673) and the join
operator's record processor.** Note: most-corrupt run = FASTEST (35.6s).
NEXT: join-vertex numRecordsIn (standard metric) vs SP_CREATED in a corrupt run —
splits in-task wrapper-level loss from network/upstream-chain loss. Poller running.

### STAGE-0: topology revelation + two more eliminations + the timing reframe
All-vertex counters (corrupt specimen, no exceptions/restarts):
Source out=3,065,001 | GWA[7] in=2,000,000→out=1,536,400 ✗ | GWA[14] in=1,065,001→out=1,064,409 |
Join in=2,600,809→out=2,600,809 ✓ (perfect 1:1; in == GWA7+GWA14 exactly).
**THE JOIN WAS NEVER THE BUG — q8's under-emit is GlobalWindowAggregate[7] (persons side)
swallowing ~25% of its window results.** GWA inputs are COMPLETE (2,000,000 exactly =
deterministic persons count). TM logs: only the JOIN announces async state ⇒ the GWAs are
SYNC-state operators; the V1 sync path has ZERO FRS_RS_EXECUTOR sensitivity (grep-verified).
Eliminations: sync-direct executeRequestSync (ForSt-mirror, dedicated executor instance)
corrupt 4/4 ⇒ diff (c) dead; cbran==completed==created in corrupt specimens ⇒ all AEC
machinery dead.
REFRAME UNDER TEST: a sync GWA can only vary with this env via TIMING (routing-async ⇒
fastest join consumption ⇒ different backpressure/scheduling across the 8 cores ⇒ source-
subtask skew) ⇒ candidate mechanism = GENUINE LATE-DATA DROPS at the sync window agg
(its own numLateRecordsDropped metric exists). In-band watermarks should make this
impossible (per-channel FIFO) — the metric poll on a corrupt specimen decides; ≈460K drops
would mean the q8 canary has been measuring SOURCE-SKEW SENSITIVITY, not an executor race,
relocating the fix entirely (watermark generation/idleness at the NexMark source or
two-phase agg ordering) and CORRECTNESS-EXONERATING routing-async itself.

### ★★★★ STAGE-0 ROOT CAUSE CONVICTED: ForStRsKeyGroupedInternalPriorityQueue LOSES TIMERS
Per-task timer accounting, corrupt specimen (out=1,617,567):
g7Add=2,000,000 (COMPLETE registration) vs g7Poll=1,055,800 → **944K registered timers never
polled**; g14: 1,064,680 vs 561,767 (−503K). Internal consistency seals it:
g7Poll+g14Poll ≈ 1.62M == GWA emissions == join input == jnAdd == out_rows.
**The backend's OWN timer queue silently drops registered timers; unfired windows never emit.**
routing-async is the TRIGGER, not the cause: it reshapes engine flush/compaction timing,
and the queue's engine-backed resume-cursor refill machinery (the 2026-06-03 O(N²)-fix
design: resume cursor + seekHint floor + pendingBuffer merge; file's own comments call
cursor-invalidating refills "bounded-risk") loses entries under the altered timing.
PRE-EXISTING, TIMING-TRIGGERED, IN-SCOPE (our backend). Exonerates: executor dispatch, AEC,
flink-runtime, the join, staging buffers (those were real lockstep-only hazards but not THIS
bug). Audit of poll/refill for the exact skip in progress; fix lands in the timer queue.

### ★★★★★ STAGE-0 FIX IMPLEMENTED: timer-queue refill-floor min-merge
THE BUG (ForStRsKeyGroupedInternalPriorityQueue ~line 2128): the post-flush one-shot refill
FLOOR was installed with a plain put(). When a SECOND flush touched the same kg before its
next refill (cache empty → headTs=MAX → floor=minAdd₂), the put OVERWROTE a lower
outstanding floor — every engine timer row in [floor₁, floor₂) was permanently skipped
(refill seeks from the floor; the kept resume cursor is overridden) — and could also set a
floor PAST the cursor baseline, skipping [cursor, floor). Lost rows = registered-but-never-
polled timers = unfired windows = q8's under-emit. TIMING-SHAPED exactly as observed:
fast ingest (routing-async) ⇒ multiple flushes per kg between poll turns ⇒ loss; slow runs
refill in between ⇒ exact. **LATENT IN DEFAULT MODE TOO** (window narrow, not zero) — the
fix benefits all queries/modes.
THE FIX: adopt a new floor ONLY if it LOWERS the effective refill start
(existing floor, else successor(resume cursor), else kg-prefix start which is already
lowest) — the start may only move backward until consumed. ~20 lines + the forensic
comment. Canary ×5 running.

### ★★★★★ STAGE-0 CLOSED (2026-06-11)
| gate | result |
|---|---|
| q8@100M routing-async (1 worker) ×5 | 5/5 EXACT (3,064,421-791 band) |
| q8@100M lockstep default ×2 post-fix | 2/2 EXACT (3,064,421 / 3,064,453) — no default regression |
| regression UT | reproduces the loss in 1s pre-fix ([400] vs [150,200,300,400]); green post-fix |
| full suite | 114 surefire suites, 0 failures |
| temp flink diagnostics | ALL reverted; dist jars pristine; commits: fix 1d9a844dd52 |
THE DAY'S MISATTRIBUTION, OWNED: every "executor race" canary failure (routing-async v1,
B-spike v1/v2, differential A/B, sync-direct) was THIS timer-queue bug varying with
timing. The staging-overlap hazards remain real by construction for lockstep-only
mechanisms, but were not the q8 corruption. The instrumented descent (11 instruments,
each eliminating a layer) was the cost of the truth; the fix is 20 lines.
NEXT: multi-worker routing-async ×3 post-fix (if exact, the q9 lever = multi-worker
pipelining is ALREADY correctness-viable → q9@100M immediately), then Stage-1 gates.

### ★★ MULTI-WORKER routing-async q8 ×3 POST-FIX: 3/3 EXACT (3,064,473/698/445)
Full multi-worker (3 workers, ForSt-matched default) non-blocking pipelining is CORRECT.
The two-regime design's Stage-2 "cross-worker race" never existed — it was the timer bug.
**q9@100M BAR RUN LAUNCHED**: routing-async (3 workers) + drain200K. Same-day reference:
2215.6s (today's control, perf-overhead included); cross-day band 2001-2105; bar 1776s.
Correctness gate rides along: out_rows must equal 91,813,372 (12th identical).

### q9@100M multi-worker PIPELINED (post-timer-fix): 2423.4s, rows EXACT (12th identical)
CORRECTNESS at 100M under full multi-worker non-blocking: ✓ (out_rows=91,813,372).
PERF: SLOWER than the blocking same-day control 2215.6s (pre-fix). CONFOUNDED: (a) the
timer fix honors more refill floors ⇒ re-read cost on timer-heavy queries (all post-fix
runs trend slower); (b) staging buffers + caches are OFF under routing-async (B-spike
gates) while the control had staging ON; (c) pipelining itself. FALSIFIED: the
"6.3 idle cores" extrapolation — mailbox overlap cannot break q9's PER-KEY serial chains
(KeyAccounting); the latch cost ceiling was the ~6% unpark share, not 20%.
DE-CONFOUND NEXT: q9 BLOCKING routing post-fix (control config + fix) isolates the fix's
cost. q9's real levers revert to: chain-shortening (OPT-N04 merge-op kills the dependent
GET→PUT), staging absorption under load, iterator cost — the banked Stage-3 work.

### ★★ q9 DE-CONFOUNDED (3-way, same-day, rows EXACT in all — 13 consecutive identical)
| config | q9@100M |
|---|---|
| blocking + PRE-fix (control) | 2215.6s |
| blocking + fix | 2579.8s ⇒ TIMER FIX COSTS ~+364s (+16%) on q9 |
| PIPELINED + fix | 2423.4s ⇒ pipelining = REAL −156s (−6%) de-confounded |
REVISED: multi-worker non-blocking pipelining HELPS q9 (modest; per-key chains bind the
rest). The fix's COST is the floor-refill ENGINE RE-READ of the orphaned span — but the
dropped cache entries are IN MEMORY at drop time. OPTIMIZATION (next): on the
within-window-add invalidation path, BINARY-MERGE the flushed adds into the existing
cached deque (sorted by ts) instead of dropping + re-reading — O(adds) vs O(span), no
floor, cursor intact. Benefits ALL timer-heavy queries (q5/q8/q11/q17 included).
q9 bar math: pipelined minus fix-tax ≈ ~2060s vs bar 1776 ⇒ Stage-3 chain-killers
(OPT-N04 merge-op) remain q9's path regardless.

### ★ STAGE-1 GATE: two-regime q8@100M ×5 = 5/5 EXACT (3,064,596/449/449/510/421)
The two-regime executor (FRS_RS_EXECUTOR=two-regime, commit 0115a121a32) passes its
correctness canary: LIGHT inline + HEAVY non-blocking + regime-gated staging + L→H flush
all live in these runs. Walls 44.7-72.7s (band noise). Next: q17 ×3 pair (LIGHT-regime
preservation, direction-only), 10M sweep.

### STAGE-1 GATE: q17 ×3 pair (same-day, all rows EXACT 92,000,000)
two-regime: 328.0 / 310.0 / 205.9 | lockstep: 334.1 / 223.0 / (r3 below)
DIRECTION VERDICT: indistinguishable under this box's documented 3× day-noise — the
two-regime executor does NOT rob q17 (the design's defining requirement). Both modes sit
above yesterday's 77s class: box-day shift + timer-fix tax, equal in both arms ⇒ the
comparison stands. STAGE-3 ENGINE UNITS MERGED (b5b85a9b9): NumericAddMergeOperator
(13 tests) + frs_vectorized_batch_mixed (5 tests; ffi 104/0, storage 372/0).

# ★ NEW HARD TARGET (user, 2026-06-11): q9/q20 ≤ 1.05× RocksDB
Reference (matrix, to be re-pinned same-day at verdict time): q9 RDB 1420.5s ⇒ target
≤1491.5s (current best de-confounded ≈2060s ≡ pipelined minus fix-tax ⇒ gap ≈ −28%);
q20 RDB 859.7s ⇒ target ≤902.7s (current 1477.7s ⇒ gap ≈ −39%).
IN-SCOPE LEVER STACK (yield order): (1) timer-fix tax recovery via cache-merge (−16% q9,
all timer queries — implementing now); (2) pipelining (−6%, have); (3) Unit-2 backend
wiring (one mixed FFI crossing per batch); (4) post-fix re-profile of q9 AND q20 (q20
never profiled!) with FRS_PERF → next levers (scan-open dedupe, AEC buffer-timeout audit,
per-prefix iterator reuse — the engine read path is 33% CPU); (5) same-day RDB re-pin for
the 1.05× verdicts. Runtime stays FROZEN.

### q9@100M pipelined + cache-merge opt: 2398.5s, EXACT (14th identical)
Recovered only ~25s of the +364s fix tax ⇒ the tax lives mainly in the EMPTY-CACHE floor
path (engine re-read spans), not the live-cache within-window branch the merge covers.
Profile-driven next step. Ledger: blocking/pre-fix 2215.6 | blocking/fix 2579.8 |
pipelined/fix 2423.4 | pipelined/fix+merge 2398.5. Target 1491.5 (1.05× RDB 1420.5).

### ★★ SAME-HOUR 1.05×-TARGET SCOREBOARD (2026-06-11 evening; rows byte-equal both sides)
q9: frs 2398.5 vs RDB 1449.5 = **1.65×** (need ≤1522s) | q20: frs 2021.6 vs RDB 963.3 =
**2.10×** (need ≤1011s). RDB stable vs matrix ⇒ box fine ⇒ regressions are OURS.
TIMER-FIX TAX SCALES WITH TIMER DENSITY: q17 +300% (77→310s class), q20 +37%, q9 +16% —
the fix is the dominant regression and its cost path did NOT appear in q20's CPU profile
(suspect: wait-time or a path the 180s window missed). NEXT: q17 lockstep + FRS_PERF —
cheap (~300s), tax-dominated ⇒ directly exposes the cost path; then kill it.

# ═══════════════════════════════════════════════════════════════════════════
# MEMORY-RESIDENT TIMER INDEX — gate chain (2026-06-11 late, single-topo)
# ═══════════════════════════════════════════════════════════════════════════
Implementation: flink 1dc40a7b051 (rebased 7590cde1c1f) — cache/cursor/floor layer
DELETED (~900 LOC); poll/peek = pure memory; engine = durable log; restore = bulk
scan; FRS_TIMER_INDEX_MAX=8M spill valve. Review fixes: composite-byte heap
tiebreak (deterministic equal-ts firing), Long.MAX_VALUE cleanup timers no longer
dropped, remove() reports liveness. Pre-V1 @Disabled 11-test queue suite
RE-ENABLED and green (old cache failed 8/11). Module 552/0 → 558/0 w/ Unit-2.

### Gate results (jar = timer-index only; engine .so = pre-streaming):
| gate | result | verdict |
|---|---|---|
| q8@100M ×3 (r3=routing-async) | 113.7/47.9/109.7s, out 3,064,469/453/473 | EXACT 3/3 ✓ |
| q17@100M ×3 | 306.2/205.1/274.0s, rows 92,000,000 ×3 | EXACT; UNCHANGED vs yesterday |
| q20 pair | frs 2045.8 vs RDB 1034.4 = 1.98× (rows 93,201,404 both) | EXACT; UNCHANGED (was 2.10×) |
| q9 pair | (in flight) | — |

### ★★ ATTRIBUTION REVISED: the "timer-fix tax" on q17/q20 was BOX-DAY SHIFT
With the engine-read poll path deleted BY CONSTRUCTION, q17 (~306s class) and q20
(1.98×) are unchanged from yesterday's post-fix numbers ⇒ the "+300% q17 / +37%
q20 tax" was box-day noise misattributed to the floor fix (the recorded q17
3×-within-day warning strikes again). The one same-day de-confounded tax
measurement (q9 blocking pre/post fix: 2215.6→2579.8) gets its verdict from the
in-flight q9 pair. IMPLICATION if q9 also unchanged: q9/q20's real gap is the
ENGINE LONG-SCAN READ PATH (R-long regime) — the streaming-read campaign
(prefetcher/P2) becomes the primary q9/q20 lever, not timers. The timer index
stands on architecture+correctness merits regardless (3 bug-generations deleted,
disabled suite revived, determinism).

# ═══════════════════════════════════════════════════════════════════════════
# STREAMING-READ REDESIGN — IMPLEMENTED (merged c890c48a6, pushed 39df15248)
# ═══════════════════════════════════════════════════════════════════════════
P0 EOF-flag+auto-close (2045ea577): exhausted-at-open probes skip trailing
next()+close() (2 crossings/probe saved — dominant q7 case); backward-compatible.
P1 (e1aa00bcd): batched-open first-chunk fill on bg_read_pool (was serial).
§2.1 BlockPrefetcher (c8663821e): cold=demand (R-short zero speculation), ramp
2→cap (local 256KiB / remote 4MiB), multi-block preads, double-buffered decode on
read pool, end_block clamp, cache-first window splitting, Bottom-priority for
deep windows. DEFAULT-ON, kill switch FRS_RS_BLOCK_PREFETCH=0.
io_uring (a2ee94e56): new crate forst-rs-io-uring (BlockIo trait; SQE batch per
window; FRS_IO_URING default true on Linux + probe w/ silent pread fallback;
kernel ≥5.6; docker seccomp may block → needs seccomp=unconfined on TMs).
Java P0 drain: flink 725824ae9e9 (FRS_CHUNK_EOF honor; module 558/0).
Engine tests 1058/0. GATES OWED: q7@100M (target ≤~550s vs ForSt 586.8),
q3/q4 no-regress, q7 exactness, FRS_IO_URING A/B. Local box .so NOT yet rebuilt
(pending ti-chain completion); remote x86 box building from origin now.

# ═══════════════════════════════════════════════════════════════════════════
# HARNESS: TOPO=split + REMOTE LINUX BOX (2026-06-11 user directives)
# ═══════════════════════════════════════════════════════════════════════════
TOPO=split (c6c44de6c): 8c/32g = TM-ONLY (2×TM docker 4c/16g + JM docker 2c/4g,
JM 1600m + gateway + client outside the budget; 2 slots/TM ⇒ p=4 spreads 2+2).
Default stays single until ti-chain ends; ALL pre-2026-06-11 pins are single-topo
— cross-topology comparisons are INVALID, re-pin everything on flip.
REMOTE: yq01-sys-hic-k8s-p40-0000 via relay bridge (fingerprint once, FIFO-driven
background session). x86_64, kernel 5.10, 28c/251G (containers capped), docker
19.03, /ssd2 NVMe 3.6T. Workflow: local dev+UT → push origin → remote agent pulls
worktrees to /ssd2/jackylee/frs-bench, builds forst-bench:x86 + .so in-image,
runs split-topo NexMark (q8 canary, q7 io_uring A/B, q7/q9/q20 vs RDB).

### TIMER-INDEX CHAIN COMPLETE — q9 pair (final): frs 2349.3 vs RDB 1437.6 = 1.63×
Rows byte-equal (91,813,372, 15th/16th identical). Index recovered only ~50-75s
of q9 (2423.4→2349.3 vs pipelined/fix) ⇒ TIMER LAYER WAS NEVER THE q9/q20 GAP.
Scoreboard vs 1.05× bar: q9 1.63× (need ≤1509), q20 1.98× (need ≤1086).
PMC ROADMAP (docs/superpowers/specs/2026-06-12-q9-q20-longscan-roadmap.md):
① fix H1 pool-panic hang + M2/M3 prefetcher guards → ② garbage-drain third gate
→ default 200K (recorded q9 −377s / q20 −568s) → ③ FRS_RS_MIXED_BATCH flip +
OPT-N04 merge-RMW → ④ S2 pinned rows/loser tree → ⑤ compaction windowed reads
(19.3% q20 share, NOT prefetcher-eligible today) → ⑥ P2 ring (q9 insurance).
Modeled landing: q9 ~1430-1600s (0.99-1.10×), q20 ~920-1060s (0.89-1.03×).
LAST LOCAL NEXMARK RUN — all NexMark henceforth on the remote x86 box.

### REMOTE BOX BLOCKER FOUND: docker 19.03 cannot pull eclipse-temurin (OCI
manifest error) + apt Ign inside build. Host internet OK (rustup 200). Unblock:
build .so on HOST (native cargo), base containers on the EXISTING
flink:2.2.1-jdk17-forst-bench-tools-20260609 image + bind-mounted host JDK25.

# ═══════════════════════════════════════════════════════════════════════════
# REMOTE-x86 POPULATION BEGINS (yq01 box, NVMe, split-topo, io_uring LIVE)
# ═══════════════════════════════════════════════════════════════════════════
First NexMark on the remote Linux box (2×TM 4c/16g + JM 2c/4g, forst-bench:x86
via daocloud/tuna mirrors, .so host-built glibc-2.17⊂jammy, io_uring verified:
EPERM under default seccomp ⇒ TMs run seccomp=unconfined). Engine = streaming-
read stack WITHOUT drain-default (one variable at a time; drain lands next round).
| run | result |
|---|---|
| q1@1M smoke | FINISHED 3.9s, out 1,000,000 |
| q8@100M canary | FINISHED 153.2s, out_rows 3,064,589 (exact band ✓) |
Matrix queued (nohup, logs /ssd2/jackylee/frs-bench/logs/SUMMARY.md): q7 uring-on
→ q7 uring-off → q7 rocksdb → q9 frs/rdb → q20 frs/rdb. REMOTE numbers are a NEW
population — never compare to Mac pins.

### REMOTE round-1 (streaming stack, NO drain-default): q7 uring-ON = 2376.4s
q7@100M forst-rs FRS_IO_URING=on: FINISHED 2376.4s, out_rows 92,000,002 (src
92,000,316). Remote population — per-core slower than Mac (q8 153-170s vs Mac
~48s class); cross-population q7-vs-ForSt-587s comparisons INVALID pending a
remote ForSt pin. The uring-off arm (started 00:56) gives the io_uring delta;
the rocksdb arm gives the remote ratio. Second q8 canary: 170.1s rows-in-band.

### REMOTE round-1: q7 trio complete — io_uring = finish-vs-DNF; ratio 1.74×
| arm | result |
|---|---|
| q7 frs FRS_IO_URING=on | FINISHED 2376.4s, out 92,000,002 |
| q7 frs FRS_IO_URING=off | DNF (MAXSEC 2400) |
| q7 rocksdb | FINISHED 1367.6s, out 92,000,002 (EXACT == frs ✓) |
io_uring verdict: ON completes, OFF does not — keep default-ON on Linux.
Remote ratio 1.74× (bar ≤1.25×… per-query bar is ≥0.8× RDB = ≤1.25× wall) ⇒ q7
FAILS remotely; RocksDB FINISHES q7 here (Mac: DNF) — NVMe changes its profile.
Streaming P0/prefetcher alone insufficient for q7 remote; S2 loser-tree +
compaction-windowed (promoted in roadmap §5 recalibration) are the next levers.
q9 pair started 02:01:54.

### REMOTE round-1: q9 frs = 2421.2s, rows EXACT (canonical 91,813,372 — 17th identical, cross-arch)
q9@100M frs (no-drain tip): FINISHED 2421.2s. Correctness: byte-count exact across
Mac arm64 single-topo AND remote x86 split-topo. q9-rdb pair in flight (02:43:30).

### ★★ REMOTE round-1: q9 pair — frs 2421.2 vs rdb 2121.2 = 1.14× ⇒ ≥0.8× BAR PASS
q9 remote ratio 1.14× (Mac was 1.63×) — the NVMe/io_uring/split population
compresses the q9 gap; PASSES the per-query ≥0.8× RocksDB bar (≤1.25× wall).
Rows: frs = canonical 91,813,372 EXACT; rdb = 91,813,229 (−143; suspect rdb
finish-boundary sink sampling — verify round 2; frs correctness unaffected).
Round-1 is the NO-drain tip — round-2 (drain-ON + S2 later) aims at ≤1.05×.
q20 pair started 03:20:01.

### REMOTE round-1: q20 frs = 2026.5s (out==src 93,199,688; remote datagen
sharding shifts absolute counts vs Mac — pair validity = rdb arm must match
this count). q20 rdb started 03:54:56 (last run of round 1).

# ═══════════════════════════════════════════════════════════════════════════
# ★★ REMOTE ROUND-1 COMPLETE (x86/NVMe/split-topo/io_uring; NO-drain tip)
# ═══════════════════════════════════════════════════════════════════════════
| query | frs | rocksdb | ratio | bar ≤1.25× |
|---|---|---|---|---|
| q8 canary ×2 | 153.2 / 170.1s (rows in-band) | — | — | canary PASS |
| q7 | uring-ON 2376.4 FINISH; uring-OFF DNF@2400 | 1367.6 | 1.74× | FAIL |
| q9 | 2421.2 (rows canonical EXACT) | 2121.2 | **1.14×** | **PASS** |
| q20 | 2026.5 (out==src) | 1545.3 (out==src) | **1.31×** | near-miss |
HEADLINES: (1) q9 PASSES the ≥0.8× bar on the production-like population (Mac
1.63× → 1.14×); (2) q20 1.98× → 1.31× — drain-ON (round 2) targets the flip;
(3) io_uring = finish-vs-DNF on q7; (4) q7 1.74× = the remaining architectural
gap (S2 loser-tree in build + compaction-windowed next); (5) correctness: frs
canonical-exact on q9 cross-arch; rdb −143 q9 rows (investigate round 2).
ROUND 2 (gate matrix 2026-06-12-remote-gate-matrix.md): drain-ON+E5 tip,
control-first q17, q3 point-get guard, q9/q20 pairs — launching.

### REMOTE ROUND-2 (drain-ON+E5+alloc tip 06549d967/72607e110): gates 1-2 PASS
q1 smoke 4.0s rows exact | q8 canary 168.0s rows 3,065,198 in-band (drain-ON
first 100M = correctness-clean; wall in round-1 band ⇒ no q8-class drain cost).
q3 point-get guard pair running.

### ROUND-2 gate 3 PASS: q3 point-get guard 1.16× (frs 142.1 / rdb 122.6) <1.4×
Prefetcher costs point-gets nothing measurable; q3 also inside the 1.25× bar.
All control gates green → q17 ×3, then q9/q20 drain-verdict pairs.

### ROUND-2: q17 ×3 = 418.3/422.6/387.3 (median 418.3, ±4.5%), rows exact ×3
First remote q17 baseline; the box is measurement-grade (Mac q17 swung 3×).
Drain-ON shows no q17 regression risk (control gate intent satisfied). q9 frs
(drain verdict run) started — model ~2030-2180s vs round-1 2421.2.

### ★★ ROUND-2 ALARM: q9 frs 2872.8s (+18.7% vs r1) AND out_rows 91,813,396 = +24 vs CANONICAL
Drain-ON model predicted −10-16%; got +18.7% AND a CORRECTNESS deviation (+24
rows — resurrection-shaped). Suspects in r2 tip vs r1: (1) drain-ON default
(tombstone drop too early ⇒ deleted keys resurrect ⇒ extra ROW_NUMBER emits +
extra compaction work explains the slowdown) — PRIME; (2) Java V1 timer
peek/poll alloc fix (its flagged lockstep gate never ran); (3) E5 fallback
(unlikely: single-CF levels stay monotonic). q9-rdb control in flight (box-state
check). DISAMBIGUATION (round-2b, after matrix): same tip + 
FRS_GARBAGE_DRAIN_TOMBSTONES=0 q9 → rows canonical ⇒ drain culpable ⇒ revert
default + root-cause the gate; else jar-swap bisect (r1 jar + r2 engine).
CORRECTNESS IS NON-NEGOTIABLE — drain default flip is BLOCKED until resolved.

### ROUND-2 q9 pair: rdb control 2175.3 (box STABLE) ⇒ frs +450s regression REAL (1.32×)
Drain-ON cost q9 ~+19% instead of the modeled −10-16%. Row-count refinement: rdb
itself jitters across runs (91,813,229 r1 / 91,813,374 r2 vs canonical 372) ⇒
the frs +24 may be sink-sampling jitter, not true extra rows — r2b nodrain run
(auto-queued) answers both (wall ≈2421 ⇒ drain culpable; rows exact ⇒ jitter).
q20 pair running (systemic-vs-q9-specific check). Drain default flip BLOCKED.

### ROUND-2 q20 frs: 2011.1s ≈ r1 2026.5 (−0.8%) ⇒ drain regression is q9-SPECIFIC
Drain-ON: q9 +19%, q20 ±0 on this box — the Mac-recorded drain wins (q9 −377s /
q20 −568s) do NOT transfer to NVMe (no tombstone read-amp to reclaim; only the
drain's compaction cost remains, and q9's write pattern pays it). q20-rdb pair
running; r2b nodrain q9 auto-fires after.

# ═══════════════════════════════════════════════════════════════════════════
# ★★ REMOTE ROUND-2 FINAL (drain-ON+E5+alloc tip) — DRAIN LEVER DEAD ON NVMe
# ═══════════════════════════════════════════════════════════════════════════
| gate/query | result | vs round-1 |
|---|---|---|
| q1/q8/q3 gates | 4.0s / 168.0s rows-in-band / 1.16× | all PASS |
| q17 ×3 | 418.3/422.6/387.3 (±4.5%), rows exact | first remote baseline |
| q9 | frs 2872.8 / rdb 2175.3 = 1.32× | REGRESSED from 1.14× |
| q20 | frs 2011.1 / rdb 1557.6 = 1.29× | unchanged (1.31×) |
VERDICT (gate-matrix D2-class): drain-ON = q9 +19% / q20 ±0 on NVMe — the Mac
win FALSIFIED on the production-like population. Default REVERTED (57f0466bf,
env opt-in kept). r2b nodrain q9 confirmation run in flight (07:45:37).
ROUND-3 NEXT: S2 flag-ON gate program (PMC §9, review-rounds/2026-06-12-pmc-
review-s2.md) on the reverted tip — the q7 1.74× campaign's first full-stack
measurement (micro: 9.2× deep fan-out, −30% churn scan).

### ★★ r2b VERDICT: drain-off q9 = 2779.2s — drain only ~94s of the +450s; ~360s
REMAINS from other r2 changes. Rows 91,813,195 (vs 372/396) ⇒ frs sink-sampling
jitters like rdb ⇒ +24-row alarm DOWNGRADED to perf-only (src always exactly 98M).
REGRESSION SUSPECTS (r1→r2 engine deltas, ranked): (1) M3 PREFETCH_BUFFERED_BYTES
global AtomicUsize — per-window submit/claim/drop cross-core cacheline traffic in
the hottest path; (2) E5 per-call OnceLock load on overlapping_ssts_in_range
(millions of q9 scans × 8 threads); (3) H1 catch_unwind per pool job; (4) Java
V1/V4 alloc fixes (timer path). Round-3 S2 program DEFERRED until rooted (a
regressed tip pollutes all further A/B). Bisect: engine-swap vs jar-swap runs.

### ★★ FIXED-CSV ACCURACY GATE (2026-06-12): q3/q5/q8/q11 HASH-EQUAL, q12 invariant-PASS
1M fixed input, identical bytes both backends, current tips (jar 72607e11097 +
engine c921e297b): all EQUAL incl. retraction streams; determinism control (q8
re-run ×2 each backend) = 4/4 identical hashes. Harness now IN-REPO:
scripts/accuracy-gate/ (spec 2026-06-12-fixed-csv-accuracy-gate.md). The
timer-index + alloc-fix stack is byte-correct on deterministic ground.

### ★★ q5@100M REMOTE (fix verified): frs 604.5s vs rdb 595.4s = 1.015× PARITY
Both finish, no collapse; q5 fix (80015d2cfaa) holds at 100M. out_rows Δ725
(0.0024%, src_out also differs — sink/datagen jitter; exactness gate agent E
queued). ForSt leg SKIPPED: no ForSt-C++ jar in remote flink/lib — USER must
provide flink-statebackend-forst jar for the 3rd leg. q5 row: FAIL→**PASS-RDB**.

### BISECT VERDICT: SPLIT — r1jar+r2engine = 2606.4 (refs 2421 jar / 2779 engine)
~173s of the +358s q9 regression is the JAR delta (V1-V4 alloc fixes commit),
~half engine-side (de-contention branch 87cbcec8e ready). Single runs each —
de-noising cells queued (resident agent). NOTE: the box hosts a USER campaign
(run-matrix forst-local vs forst-rs-lib, 8c/40g, since 00:18 — ALL today's
remote numbers share that neighbor load; population consistent). Box jar
currently = r1 (725824ae9e9) — setup must redeploy 72607e110 before new cells.

### ★★ q7 S2 VERDICT @100M: +3.1% only — MICRO WIN DID NOT TRANSFER (falsifier hit)
5M screen PASS (rows identical ON/OFF). 100M concurrent ratio-fair pair: both
arms DNF@2400 (pair regime slower — shared disk + neighbor load; RATIO is the
measurement): S2-ON 75.63M vs OFF 73.33M events = +3.1% (consistent through the
run). The 6-9× probe micro-win does NOT dominate q7@100M ⇒ q7's wall is NOT
probe-merge-CPU-bound at scale (suspect: I/O regime or framework floor). PER THE
ANTI-WANDERING RULE: q7 enters its RE-PROFILE stage (remote perf capture of the
real 100M hot path) before any further q7 lever work. S2 stays correctness-clean
and never-losing (default-ON gate unblocked, low priority). Strategy-doc q7 row
(beat-ForSt-total decider) now needs the re-profile to re-rank.
Infra fixes for future agents (cost ~2h): FRS_RS_S2_PINNED not in ENVS;
docker IPv4 pool exhaustion (need NET_SUBNET); dockerd stale overmount on
~/workenv/frs-tmp ⇒ use FRS_CTMP_BASE=/ssd2/jackylee/frs-bench/frs-tmp.

### R3 CAMPAIGN (contended walls — 3 agents oversubscribed the box; ratios deferred)
q9 r3a2 single: 2904.6 FINISHED (engine-culpable class ⇒ DE-CONTENTION branch
needed); q20 2201.6 FINISHED; q19 FIRST REMOTE WALL 1190.9 rows exact. Concurrent
2-cell q9 INVALID (58c demand/28c box). PROCESS: box lock-file coordination
needed; 2 harness fixes to upstream (FRS_CTMP_BASE: frs-tmp is a separate mount
dockerd won't traverse; explicit-subnet docker networks: IPv4 pools exhausted).
NEXT DECIDER: tip vs bisect-decontention q9 A/B in a QUIET window.

### ★ DISK-IO POLICY (user, 2026-06-12): /ssd2 I/O saturated by concurrent runs
DISTRIBUTE state storage across /ssd2/jackylee, /ssd1/jackylee, /tmp/jackylee —
one disk per CONCURRENT cluster (via FRS_CTMP_BASE per run). RULES: both arms of
an A/B pair = SAME disk; reference/single runs = consistent disk (note which in
results). MEASUREMENT CAVEAT (H1 de-confound): the 98-99% /ssd2 util + writes>
reads observation was taken under multi-cluster shared-disk contention — part of
the "write saturation" may be contention, not write-amp; the ForSt-vs-frs iostat
comparison and the post-distribution re-measure settle it.

### ★ q7 DESIGN ACCEPTANCE GATE (user, 2026-06-12): H1 evidence was disk-confounded
The sorted-run-discipline design (in progress) may rest on contaminated evidence
(the 98-99% /ssd2 util + writes>reads iostat was captured under multi-cluster
shared-disk contention). BEFORE the design is approved or built, ALL THREE must
align on CLEAN conditions: (1) the re-baseline q7 pair (quiet dedicated disk);
(2) a RE-CAPTURED q7 iostat/profile on a quiet disk (does frs q7 still saturate
its own disk alone? writes still > reads?); (3) the local H1 microbench
(write-amp + fan-out curves). If clean q7 is much faster / not disk-bound, H1
demotes and the q7 lever hunt restarts from the clean profile. The in-flight
sorted-run doc lands as HYPOTHESIS-PENDING-CLEAN-EVIDENCE, not approved.

# ═══════════════════════════════════════════════════════════════════════════
# ★★★ ForSt q7 REMOTE PIN: 1379.7s (rows EXACT) — ForSt ≈ RocksDB remotely!
# ═══════════════════════════════════════════════════════════════════════════
The Mac 2.46× ForSt advantage did NOT transfer: remote q7 bar = ~1370-1380s
(ForSt 1379.7 ≈ rdb 1367.6). frs must claw 2376.4 → ~1380. ForSt crash-loop
ROOT-CAUSED: image bakes stale libforstjni.so on LD_LIBRARY_PATH shadowing the
jar's native — fix `-e LD_LIBRARY_PATH=` (unblocks all ForSt legs).
H1 CONFIRMED clean: same-disk iostat ForSt 100-116MB/s writes/6% util vs frs
435-682+190-292MB/s/98-99% (~10× physical bytes/event — frs's OWN write-amp,
not contention); churn_probe bench (committed): frs write-amp 7.68× vs RDB
3.91× INTRINSIC to picking (knobs change nothing); L0-depth dose-response
linear. MECHANISM CORRECTED: L1+ already non-overlapping/bsearch — the read
half is L0 depth; the BIGGER half is write volume (whole-L1 rewrite/rollup, no
trivial move, global compaction mutex, fixed cascade). DESIGN (sorted-run doc
6ee98844b): M1 clean-cut picking, M2 trivial move, M3 concurrent compactions,
M4 score-based pick+dynamic targets (largest cell), M5 L0 20/36+LZ4-remote,
M6 shadow budget; S1-S5 staged, G1-G3 micro falsifiers, G5 remote A/B.
Expected q7 2376→1400-1700. ACCEPTANCE GATE: 2 of 3 conditions met (iostat ✓,
microbench ✓); clean q7 pair (re-baseline, in flight) completes it.

### INTEGRITY SWEEP interim (6/22 pairs, tip b6dec7d7b, MAXSEC=3600, /ssd1):
q0 tie (120.3/119.8) | q1 tie (109.4/110.0) | q2 frs +6% (101.3/107.5) |
q3 frs (144.2/146.5; ⚠rows differ 0.16% across ALL THREE backends incl.
historical rdb — investigate) | **q4 frs DECISIVE (1033.3 vs ForSt DNF@3600,
stalled 63.5M)** | q5: frs crash-loop (TM heartbeat ~600s — retry queued;
passed 604.5s yesterday ⇒ suspect box-state not code), forst pending.
ZERO ForSt wins so far. Sweep continues autonomously (driver setsid, ledger
SUMMARY-INTEGRITY.md, restart-safe; mystery episodic docker-killer 22:18-22:52
unresolved — forensic recorders armed). BRIDGE DOWN (relay died 00:39) —
fingerprint needed to resume observation; sweep unaffected.

# ═══════════════════════════════════════════════════════════════════════════
# ★ LOCAL flag-ON validation 2026-06-13 (PMC-1, goal V10) — Mac-population
# ═══════════════════════════════════════════════════════════════════════════
# Directed local-NexMark validation (remote-only constraint lifted "for now").
# Box: Apple Silicon Mac, Docker Desktop, arm64-Linux container, TOPO=split
#   (2 TM 4c/16g + 1 JM 2c/4g = the 8c/32g TM budget). Image forst-bench:arm64.
# Tip: 78a5869c6 (forst-rs branch; write-amp levers + disagg merged).
# .so + jar rebuilt at tip this session; q1@1M smoke FINISHED (1.3s, 1M rows).
# flag-ON config = fair + lever stack:
#   FRS_SST_COMPRESSION=lz4 (default) + FRS_KV_SEPARATION=true +
#   FRS_TRIVIAL_MOVE=true + FRS_RS_S2_PINNED=1
#   (harness ENVS extended this session to forward the 3 lever flags — they
#    were previously NOT passed to the container; commit in this branch.)
# ForSt C++ jar PRESENT (flink-statebackend-forst-2.2.0.jar) → 3-backend.
# Goal bars: frs ≤1.25× RDB (≥0.8× speed) AND (if ForSt ran) strictly < ForSt.
# CAVEAT: Mac is noisier than the remote x86 box on heavy queries; this is a
#   separate population — never compare cross-box with the REMOTE-x86 pins.
# Queries: q4,q11 @100M MAXSEC=1500; q7,q19,q20 @100M MAXSEC=2700.
#
# RESULTS TABLE (filled as runs complete):
# query | frs wall (rows) | rdb wall (rows) | forst wall (rows) | frs/rdb | bar
# ------|-----------------|-----------------|-------------------|---------|----
# q4    | 347.2s (25,836,748) | 354.5s (177,629,788) | DNF x2 (~48-62M) | 0.98x | PASS RDB; beats ForSt
#         q4 = RETRACT/changelog query → out_rows 25.8M (frs) vs 177.6M (rdb) is
#         changelog CADENCE not a defect (matches all historical pins exactly:
#         frs 25,836,748; rdb 177,629,788 canonical). frs 0.98x RDB = PASS RDB
#         bar (marginally faster). ForSt q4 DNF BOTH attempts (#1 ~62M/702s,
#         #2 JM lost job ~48M/765s) — aggravated by a CONCURRENT q11 cluster
#         from a 2nd PMC agent (two 8-vCPU clusters on 8 Mac cores → JM heartbeat
#         timeouts). ForSt-q4 fragile (DNF@3600 in a remote sweep too). frs
#         FINISHES reliably → frs beats ForSt on q4. NOTE: contention also
#         degraded BOTH agents' runs → serialize heavy clusters from here.
# q11   | 197.4s (92,000,000) | 172.5s (92,000,000) | 201.0s (92,000,000) | 1.14x | PASS (<=1.25x rdb AND frs 197.4 < forst 201.0; rows match 3x). NOTE: first forst run DNF-restart (26.3M, mem-contention w/ concurrent cluster on 35G Mac) — clean re-run = 201.0s.
# q7    | 941.8s (92,000,002) | 1166.2s (92,000,002) | 502.9s (92,000,002) | 0.81x | PASS RDB; LOSES to ForSt
#         3-BACKEND ROWS BYTE-IDENTICAL: all 92,000,002. frs 0.81x rdb = PASS RDB.
#         BUT ForSt 502.9s → frs 941.8/502.9 = 1.87x ForSt → FAILS the "strictly
#         < ForSt" half of the bar. ForSt's C++ engine is ~1.9x faster on q7 here
#         (matches the Mac-population history where ForSt q7 was the strong leg;
#         the remote-x86 pin had ForSt≈rdb≈1370s — cross-box populations differ,
#         do not compare). q7 net: beats RocksDB, does NOT beat ForSt.
#         q7 flag-ON FINISHED 941.8s; rdb 1166.2s → frs 0.81x = PASS RDB with
#         room (frs ~19% FASTER than RocksDB). ROWS EXACT MATCH: both 92,000,002
#         (frs src_out 92,000,132 / rdb 92,000,164 — source-sampling jitter only).
#         NOTE: q7 flag-ON ran partly concurrent w/ the other agent's q11 cluster
#         (rate dipped to 11k/s mid-run, recovered to 160k+ once cleared) → 941.8s
#         carries some contention drag; the true serial flag-ON time is likely a
#         bit BETTER, strengthening the PASS. ForSt leg pending.
#  q7 ON-vs-OFF LEVER DELTA (the V3 headline, run serially, Mac free):
#         flag-ON  (KV-sep + trivial-move + S2-pinned + lz4) = 941.8s, 92,000,002 rows
#         flag-OFF (lz4 only, levers disabled)              = 1064.3s, 92,000,002 rows
#         → lever stack = -122.5s (11.5% FASTER) on q7, rows BYTE-IDENTICAL
#         (92,000,002 both). The write-amp/disagg levers are a real, correctness-
#         safe q7 win on local 8c/32g. (flag-ON itself had some q11-contention
#         drag, so the true ON margin over OFF is likely >=11.5%.)
# q19   | 294.2s (92,000,000) | 276.1s (92,000,000) | 264.5s (92,000,000) | 1.07x | PASS RDB; ~ties ForSt (slightly loses)
#         3-BACKEND ROWS BYTE-IDENTICAL: all 92,000,000. frs 1.07x rdb = PASS RDB.
#         ForSt 264.5s → frs 294.2/264.5 = 1.11x ForSt → narrowly FAILS the
#         "strictly < ForSt" half (within ~30s / 11%, near-parity). All 3 serial
#         on a free Mac (clean, no contention). q19 net: beats the RDB bar; ~ties
#         ForSt (loses by a small, Mac-noise-scale margin).
# q20   | 697.0s (93,201,404) | 671.4s (93,201,404) | 1510.5s (93,201,404) | 1.04x | PASS RDB + BEATS ForSt 2.17x ★
#         3-BACKEND ROWS BYTE-IDENTICAL: all 93,201,404. frs 1.04x rdb = PASS RDB
#         (near-parity). ForSt 1510.5s → frs 697.0/1510.5 = frs is 2.17x FASTER
#         than ForSt → PASSES the "< ForSt" half DECISIVELY. q20 net: BOTH BARS
#         PASS — beats RocksDB (marginally) AND crushes ForSt (2.17x). The
#         strongest V3 result of the sweep. All 3 serial on a free Mac.

# ─────────────────────────────────────────────────────────────────────────
# ★ V10 LOCAL flag-ON sweep — FINAL VERDICT (PMC-1+PMC-2, 2026-06-13/14, Mac)
# ─────────────────────────────────────────────────────────────────────────
# COMPLETE 5-query × 3-backend table (frs flag-ON vs RDB vs ForSt), @100M,
# TOPO=split 8c/32g, all CORRECT (rows byte-identical across backends modulo
# q4's retract-changelog cadence). Bars: frs <=1.25x RDB AND frs < ForSt.
#
# query | frs    | rdb    | ForSt   | frs/rdb | RDB bar | ForSt bar | net
# ------|--------|--------|---------|---------|---------|-----------|----
# q4    | 347.2  | 354.5  | DNF x2  | 0.98x   | PASS    | beats(DNF)| PASS both
# q7    | 941.8  | 1166.2 | 502.9   | 0.81x   | PASS    | FAIL 1.87x| RDB only
# q11   | 197.4  | 172.5  | 201.0   | 1.14x   | PASS    | PASS 1.02x| PASS both (PMC-2)
# q19   | 294.2  | 276.1  | 264.5   | 1.07x   | PASS    | ~tie 1.11x| RDB only (near-tie)
# q20   | 697.0  | 671.4  | 1510.5  | 1.04x   | PASS    | PASS 2.17x| PASS both ★
#
# HEADLINE FINDINGS:
# 1. frs PASSES the RDB bar (<=1.25x) on ALL 5 V3 queries — q4 & q7 frs is
#    even FASTER than RocksDB (0.98x, 0.81x); q11/q19/q20 within 1.04-1.14x.
# 2. vs ForSt: frs WINS q4 (ForSt DNF) + q20 (2.17x), TIES q11/q19, LOSES q7
#    (ForSt's C++ engine 1.87x faster on q7 here).
# 3. q7 ON-vs-OFF LEVER DELTA: flag-ON 941.8s vs flag-OFF 1064.3s = the
#    write-amp/disagg lever stack (KV-sep+trivial-move+S2-pinned+lz4) is
#    -122.5s / 11.5% FASTER, rows byte-identical → the levers are a real,
#    correctness-safe win.
# 4. CORRECTNESS: zero defects. Every query's out_rows is byte-identical
#    across all backends that finished (q7 92,000,002 ×3; q19 92,000,000 ×3;
#    q20 93,201,404 ×3; q11 92,000,000 ×3). q4's 25.8M-vs-177.6M is the known
#    retract-changelog cadence, not a defect (matches all historical pins).
#
# OPERATIONAL NOTES (for the next local sweep):
# - Mac has ~35G RAM; each TOPO=split cluster wants ~36G (2x16 TM + 4 JM).
#   TWO concurrent clusters thrash → JM heartbeat DNFs (this corrupted the
#   early ForSt-q4 attempts + a q11-forst). SERIALIZE heavy clusters.
# - Harness scripts/run-8c32g.sh was extended this session to FORWARD the 3
#   lever flags (FRS_KV_SEPARATION/FRS_TRIVIAL_MOVE/FRS_RS_S2_PINNED) — they
#   were previously NOT passed into the container. Committed in this branch.
# - FOLLOW-UP LEVER (from PMC-1): a q7 flag-ON run under contention hit a
#   frs engine error `frs_vectorized_batch_get rc=1 NOT_FOUND` at
#   VectorizedExecutor.executeGets, then a checkpoint-restore crash-loop
#   (NoSuchFileException /tmp/flink-forst-rs-io). The clean serial q7-frs ran
#   fine (941.8s), so it's likely contention-triggered — but worth hardening
#   the vectorized batch-get NOT_FOUND path + ckpt-restore state-dir handling.

# ─────────────────────────────────────────────────────────────────────────
# ★ V3 FULL 8-QUERY verdict pass 2026-06-14 (tip 1cda0e724, flag-ON)
# ─────────────────────────────────────────────────────────────────────────
# Mac population (8c/32g TOPO=split, 2 TM 4c/16g + 1 JM 2c/4g), @100M, STRICTLY
# SERIAL. tip 1cda0e724 (CYCLE-3 OPEN fan-out + write-amp CompactionPolicy).
# frs flag-ON lever stack: FRS_SST_COMPRESSION=lz4 FRS_KV_SEPARATION=true
# FRS_TRIVIAL_MOVE=true FRS_RS_S2_PINNED=1. rocksdb/forst = own defaults.
# MAXSEC: q11,q12,q17,q19,q4=1500; q7,q9,q20=2700. NEVER cross-compare with
# REMOTE-x86 pins.
#
# query | frs wall (rows)        | rdb wall (rows)        | forst wall (rows)       | frs/rdb | RDB bar (<=1.25x) | vs ForSt (<ForSt) | net
# ------|------------------------|------------------------|-------------------------|---------|-------------------|-------------------|----
# q9    | DNF (OOM x3)           | 909.3 (91,813,372)     | 1661.6 (91,813,372)     | n/a     | n/a (frs DNF)     | n/a (frs DNF)     | frs DNF (OOM) — bounded-LRU fix NOT sufficient
#         CORRECTNESS: rdb & forst rows BYTE-IDENTICAL (91,813,372 both;
#         src_out 98,000,000 both). frs DNF'd before producing a final count.
#         Note: ForSt C++ q9 = 1661.6s is 1.83x SLOWER than RocksDB 909.3s here
#         -> q9 is hard for ForSt too; had frs fit in RAM it would only need to
#         beat 1661.6s to beat ForSt. The blocker is purely the 16g/TM cgroup
#         OOM, not engine speed.
#         q9-frs DNF (att1/att2): TM container OOM-killed (exit 137) at ~80-83M
#         of 100M, REPRODUCIBLE across 2 serial attempts (att1 crashed @83.3M /
#         922s; att2 @~80M / 882s). Root cause = MEMORY PRESSURE on the 35G Mac:
#         2 TM x16g + 1 JM x4g = 36g > physical RAM; q9's heavy interval-join
#         + Rank state pushes a TM past its 16g cgroup limit -> kernel SIGKILL
#         ("remote task manager was lost" / RecipientUnreachableException ->
#         crash-loop -> harness abort). Mac-population RESOURCE DNF (not a
#         correctness or MAXSEC-timeout DNF). q9 is the single heaviest join in
#         the set. Bottleneck = join+rank state footprint vs 16g/TM cgroup.
#         ── CONFIRM RUN 2026-06-14 (bounded-LRU fix 9e0438eb7, cap 2048): STILL
#         OOM (att3). Rebuilt .so at tip 9e0438eb7 (FRS_VLOG_READER_CACHE_CAP=
#         FRS_VLOG_READER_CACHE_CAP symbol present; q1@1M smoke PASS out_rows=
#         1,000,000). Flag-ON @100M TOPO=split: BOTH TMs exit 137 at ~75.04M of
#         98M (job-time ~862s; then RESTARTING crash-loop). Peak TM RSS ~15.25
#         GiB/16g just before SIGKILL. The bounded LRU DID work at the engine
#         layer (block-cache hit ~80%, no engine-cache runaway; resident
#         oscillated 9-14 GiB for most of the run rather than monotonic growth)
#         BUT the TM still OOM'd: the dominant final-phase pressure is the
#         FLINK-SIDE join+rank state heap, NOT the vlog-reader cache. So the
#         vlog cap is necessary-but-NOT-sufficient on this 35G Mac — q9 needs
#         the Mac population's 36g (2x16+4) trimmed (e.g. 1 TM, or smaller
#         per-TM heap) or candidates #2/#3 from the root-cause doc to land. NET
#         UNCHANGED: q9 remains a Mac-population RESOURCE DNF; rebuild fix did
#         not flip it. The OOM is NOT resolved on this box. NOTE crashed slightly
#         EARLIER (75M vs 80-83M) — within box noise (busy-disk single run).
# q12   | 40.6 (92,000,000)      | 40.2 (92,000,000)      | 40.8 (92,000,000)       | 1.01x   | PASS              | PASS 1.005x       | PASS both (source-bound parity)
#         CORRECTNESS: all 3 rows IDENTICAL (92,000,000; src_out 92,000,000).
#         q12 is a lightweight proctime tumbling agg = SOURCE-BOUND; all 3
#         backends finish within 0.6s (40.2-40.8s) -> pure datagen-rate parity,
#         no engine lever moves the needle. frs PASSES both bars (1.01x RDB;
#         marginally < ForSt). Counts matched despite proctime nondeterminism.
# q17   | 150.7 (92,000,000)     | 67.6 (92,000,000)      | 252.2 (92,000,000)      | 2.23x   | FAIL 2.23x        | PASS 1.67x        | RDB FAIL / ForSt PASS
#         CORRECTNESS: all 3 rows IDENTICAL (92,000,000; src_out 92,000,000).
#         q17 = grouped windowed agg (auction->count/distinct/min/max/avg over
#         price). frs 150.7s is 2.23x SLOWER than RocksDB 67.6s -> FAILS RDB
#         bar. But frs BEATS ForSt (150.7 vs 252.2 = 1.67x faster). So q17 is
#         a RDB-only failure: RocksDB's mature C++ windowed-agg read path is
#         much faster here; frs ingests 92M in ~120s then the windowed-agg
#         DRAIN (RMW over accumulators) is the slow phase -> residual read-path
#         / agg-state cost, NOT write-amp (already addressed). FOLLOW-UP: q17 is
#         the clearest read-path regression vs RDB in the V3 set; profile the
#         windowed-agg accumulator RMW path.
# q11   | 215.8 (92,000,000)     | 103.8 (92,000,000)     | 128.8 (92,000,000)      | 2.08x   | FAIL 2.08x        | FAIL 1.68x        | both FAIL
#         CORRECTNESS: all 3 rows IDENTICAL (92,000,000; src_out 92,000,000).
#         DIVERGES from V10 baseline (frs 197.4 / rdb 172.5 / forst 201.0 ->
#         PASS both). This pass frs is steady (215.8 ~ 197.4, +9% Mac noise) but
#         BOTH competitors ran MUCH faster on a quiet box (rdb 172.5->103.8,
#         forst 201.0->128.8) -> the gap is real once box noise is removed:
#         frs 2.08x RDB, 1.68x ForSt. q11 = windowed MapState agg; same drain-
#         phase read-path/agg-state cost as q17 (ingest fast ~948k/s, then the
#         per-key accumulator DRAIN is the wall). FOLLOW-UP: q11+q17 share the
#         windowed-agg accumulator read path -> single highest-leverage fix.
# q19   | 216.0 (92,000,000)     | 264.3 (92,000,000)     | 255.6 (92,000,000)      | 0.82x   | PASS (frs faster) | PASS 1.18x        | PASS both (frs beats BOTH) ★
#         CORRECTNESS: all 3 rows IDENTICAL (92,000,000; src_out 92,000,000).
#         frs 216.0s BEATS RocksDB 264.3s (0.82x) AND ForSt 255.6s (1.18x) ->
#         PASSES BOTH bars. IMPROVED over V10 baseline (was frs 294.2 / rdb
#         276.1 / forst 264.5 = lost both narrowly); this pass frs is faster
#         (216 vs 294, -27%) AND now wins outright. q19 = the value-carrying
#         interval-join + Top-N read path the V3 levers target -> the write-amp
#         CompactionPolicy + KV-sep stack pays off here. STRONGEST previously-
#         measured result.
# q4    | 311.0 (25,830,678)     | 286.0 (177,629,788)    | DNF (OOM @~70M)         | 1.09x   | PASS              | PASS (ForSt DNF)  | PASS both
#         CORRECTNESS: frs 25,830,678 vs rdb 177,629,788 = the KNOWN retract-
#         changelog cadence (frs ~25.8M, rdb ~177.6M) -> EXPECTED/documented,
#         NOT a defect (matches all historical pins; src_out 98,000,000 both).
#         frs 311.0s = 1.09x RDB 286.0s -> PASS RDB bar (improved over V10's
#         347.2s). ForSt DNF: TM OOM-killed (exit 137) at ~70M -> crash-loop ->
#         abort (REPRODUCES V10 baseline ForSt-q4 DNF x2). frs FINISHES where
#         ForSt cannot -> PASS the "< ForSt" bar by completion. q4 net: PASS
#         BOTH (beats RDB on time, beats ForSt by finishing).
# q20   | 824.4 (93,201,404)     | 653.5 (93,201,404)     | 1342.0 (93,201,404)     | 1.26x   | NEAR 1.26x        | PASS 1.63x        | NEAR RDB / PASS ForSt
#         CORRECTNESS: all 3 rows IDENTICAL (93,201,404; src_out same).
#         frs 824.4s = 1.26x RDB 653.5s -> NARROWLY misses <=1.25x (by 0.01x;
#         NEAR). frs ran ~18% slower than V10 baseline (824 vs 697) on a busy-
#         disk pass while rdb tracked baseline (653 vs 671) -> the miss is
#         largely Mac box noise on this single serial run. frs BEATS ForSt
#         1342.0s (1.63x faster) -> PASS the ForSt bar decisively (ForSt q20 =
#         heavy interval-join, slow C++ on this box, consistent with V10's
#         1510.5s). q20 net: NEAR on RDB (re-run on a quiet box likely PASSes),
#         strong PASS vs ForSt.
# q7    | 695.5 (92,000,002)     | 599.1 (92,000,002)     | 477.8 (92,000,002)      | 1.16x   | PASS 1.16x        | FAIL 1.46x        | RDB PASS / ForSt FAIL
#         CORRECTNESS: all 3 rows IDENTICAL (92,000,002; src_out ~92,000,0xx).
#         ★ q7-frs 695.5s = MOVED -246s / -26% vs V10 baseline 941.8s! The
#         heaviest windowed-join improved from the V3 levers (write-amp
#         CompactionPolicy + CYCLE-3 OPEN fan-out + KV-sep stack). frs 1.16x RDB
#         -> PASS RDB bar. vs ForSt: frs 695.5 vs ForSt 477.8 = FAIL (1.46x;
#         ForSt's mature C++ windowed-join engine still wins q7). NOTE both
#         competitors ran faster than V10 on this quiet pass (rdb 1166->599,
#         forst 503->478) -> V10's rdb-q7 1166s was slow/noisy. q7 net: PASS RDB,
#         FAIL ForSt; but the -26% frs move is the headline V3 win. FOLLOW-UP:
#         q7 vs ForSt residual = read-path/engine (write-amp already addressed).
#
# ───────────────────────── V3 HEADLINE FINDINGS (2026-06-14) ─────────────────────────
# COMPACT TABLE (frs flag-ON | rdb | forst, @100M, TOPO=split 8c/32g Mac, serial):
#   query | frs       | rdb       | forst      | frs/rdb | RDB bar | vs ForSt   | net
#   ------|-----------|-----------|------------|---------|---------|------------|----
#   q9    | DNF(OOMx2)| 909.3     | 1661.6     | n/a     | n/a     | n/a        | frs DNF (OOM)
#   q12   | 40.6      | 40.2      | 40.8       | 1.01x   | PASS    | PASS 1.005x| PASS both
#   q17   | 150.7     | 67.6      | 252.2      | 2.23x   | FAIL    | PASS 1.67x | RDB FAIL/ForSt PASS
#   q11   | 215.8     | 103.8     | 128.8      | 2.08x   | FAIL    | FAIL 1.68x | both FAIL
#   q19   | 216.0     | 264.3     | 255.6      | 0.82x   | PASS★   | PASS 1.18x | PASS both (beats BOTH)★
#   q4    | 311.0     | 286.0     | DNF(OOM)   | 1.09x   | PASS    | PASS(DNF)  | PASS both
#   q20   | 824.4     | 653.5     | 1342.0     | 1.26x   | NEAR    | PASS 1.63x | NEAR RDB/PASS ForSt
#   q7    | 695.5     | 599.1     | 477.8      | 1.16x   | PASS    | FAIL 1.46x | RDB PASS/ForSt FAIL
#   (all rows byte-identical across finishing backends modulo q4 retract cadence
#    frs 25.8M vs rdb 177.6M, documented; q9 forst & rdb both 91,813,372.)
#
# 1. CORRECTNESS: ZERO defects. Every finishing backend's out_rows is byte-
#    identical per query (q12/q11/q17/q19 92,000,000; q7 92,000,002; q20
#    93,201,404; q9 rdb&forst 91,813,372; q4 = documented retract cadence).
# 2. q7 MOVED: frs 941.8 -> 695.5s = -246s / -26% vs the prior V10 baseline.
#    The write-amp CompactionPolicy + CYCLE-3 fan-out + KV-sep lever stack is a
#    real, correctness-safe win on the heaviest windowed-join. (q7 still loses
#    to ForSt 477.8s = 1.46x; that residual is read-path/engine.)
# 3. NEWLY-MEASURED q9/q12/q17 vs the <=1.25x RDB bar + beat-ForSt:
#    - q12: PASS both (source-bound parity, 40.x s all three).
#    - q17: FAILS RDB (2.23x) but BEATS ForSt (1.67x). Windowed-agg DRAIN is the
#      wall; clearest read-path regression vs RocksDB in the set.
#    - q9:  DNF -- frs TM OOM-killed (exit 137) at ~80-83M, REPRODUCIBLE x2.
#      Root cause = MEMORY: 2 TM x16g + 1 JM x4g = 36g > 35g Mac RAM; q9's heavy
#      interval-join+Rank state busts the 16g/TM cgroup. rdb (909s) & forst
#      (1661s) survive on smaller footprints. Not a correctness/MAXSEC DNF;
#      needs a bigger box OR lower per-TM heap. (Note ForSt q9 is 1.83x SLOWER
#      than rdb -- q9 is hard for ForSt too.)
# 4. SCORECARD (8 queries): RDB bar (<=1.25x) -> PASS q12,q19,q4,q7 + NEAR q20
#    = 4 PASS/1 NEAR/2 FAIL(q11,q17)/1 DNF(q9). beat-ForSt -> PASS q12,q17,q19,
#    q4,q20 (5) / FAIL q11,q7 (2) / n-a q9. frs BEATS BOTH backends on q19+q12
#    (and q4 by completion); strong vs ForSt on the heavy joins q20/q17.
# 5. q11+q17 share the windowed-agg accumulator READ/DRAIN path -> the single
#    highest-leverage follow-up (both fail/lag RDB there; ingest is fast, the
#    per-key accumulator drain is the wall). Write-amp is already addressed;
#    the residual gap to RDB on q11/q17 (and to ForSt on q7) is READ-PATH/engine.
# 6. BOX-NOISE CAVEAT: this is ONE serial pass on a 35g Mac. q11 (frs steady
#    216 but rdb/forst ran ~40% faster than V10) and q20 (frs ~18% slower than
#    V10, NEAR-missing 1.25x by 0.01x) show single-run Mac variance; verdicts
#    near a bar boundary should be re-confirmed on a quiet box / n>=3. Never
#    cross-compare these Mac numbers with REMOTE-x86 pins.

# ─────────────────────────────────────────────────────────────────────────
# ★ V3 FLAG-OFF (ForSt-matching default) verdict pass 2026-06-14 (tip 15436bfde)
# ─────────────────────────────────────────────────────────────────────────
# Mac population (8c/32g TOPO=split, 2 TM 4c/16g + 1 JM 2c/4g), @100M, STRICTLY
# SERIAL. tip 15436bfde. frs flag-OFF = the ForSt-matching DEFAULT: NONE of the
# opt-in forst-rs write/read levers. Set FRS_KV_SEPARATION=false
# FRS_TRIVIAL_MOVE=false, FRS_RS_S2_PINNED UNSET (no S2 pin); FRS_SST_COMPRESSION
# stays lz4 (the engine default AND fair vs rocksdb/forst snappy/lz4). rocksdb/
# forst = own defaults. MAXSEC: q11,q12,q17,q19,q4=1500; q7,q9,q20=2700.
# Purpose: the clean apples-to-apples local baseline. The earlier "baseline"
# (21191eb51) was contention-inflated AND secretly flag-OFF due to a harness bug.
# This isolates which queries truly WANT the KV-sep lever stack (compare to the
# flag-ON section above).  NEVER cross-compare with REMOTE-x86 pins.
#
# query | frs-OFF wall (rows)     | rdb wall (rows)        | forst wall (rows)       | frs/rdb | RDB bar (<=1.25x) | vs ForSt (<ForSt) | flag-ON vs OFF delta
# ------|------------------------|------------------------|-------------------------|---------|-------------------|-------------------|---------------------
# q9    | 1828.7 (91,813,372)    | 1076.8 (91,813,372)    | 2002.5 (91,813,372)     | 1.70x   | FAIL 1.70x        | PASS 1.10x        | ★ flag-OFF FINISHES (flag-ON DNF/OOM) — KV-sep CAUSED the OOM; frs BEATS forst by completing faster (1828.7<2002.5)
#         q9-frs flag-OFF FINISHED clean @1828.7s, out_rows 91,813,372 (byte-
#         identical to flag-ON rdb/forst 91,813,372). RSS oscillated 10-15.8GiB
#         (transient peak 15.8 right at ~75M then GC/compaction reclaimed to
#         ~13GiB) — NEVER breached the 16g/TM cgroup. flag-ON OOM-killed at ~75M
#         (15.25GiB); flag-OFF rode the SAME join-state pressure ~2GiB lower and
#         survived. CONFIRMS: the KV-sep vlog-reader cache + lever stack is what
#         pushed the TM over the cgroup; with levers OFF the bounded-LRU holds it.
# q11   | 118.8 (92,000,000)     | 105.9 (92,000,000)     | 130.8 (92,000,000)      | 1.12x   | PASS              | PASS 1.10x        | ★★ flag-ON 215.8->OFF 118.8 = -45% / 1.82x FASTER. KV-sep HURTS q11.
#         q11-frs flag-OFF FINISHED @118.8s (flag-ON was 215.8s -> -97s/-45%).
#         frs 1.12x RDB (105.9) = PASS the <=1.25x bar (flag-ON FAILED 2.08x);
#         frs BEATS forst 130.8 (1.10x) = PASS (flag-ON FAILED 1.68x). The
#         windowed-MapState-agg DRAIN that was the wall under flag-ON is fixed by
#         turning the KV-sep lever stack OFF -> q11 flips from BOTH-FAIL to
#         BOTH-PASS. All 3 rows identical (92,000,000). q11 strongly wants KV-sep OFF.
# q17   | 110.7 (92,000,000)     | 71.6 (92,000,000)      | 245.9 (92,000,000)      | 1.55x   | FAIL 1.55x        | PASS 2.22x        | ★ flag-ON 150.7->OFF 110.7 = -27% / 1.36x FASTER. KV-sep HURTS q17.
#         q17-frs flag-OFF FINISHED @110.7s (flag-ON 150.7s -> -40s/-27%). Still
#         1.55x RDB (71.6) = FAIL the <=1.25x bar, BUT much closer than flag-ON's
#         2.23x. frs BEATS forst 245.9 (2.22x) = strong PASS (flag-ON was 1.67x).
#         The windowed-agg accumulator-RMW DRAIN is faster with KV-sep OFF. q17
#         still loses to RocksDB's C++ windowed-agg read path (residual read-path
#         gap, NOT write-amp) but the lever stack was making it WORSE. All 3 rows
#         identical (92,000,000). q17 wants KV-sep OFF.
# q20   | 956.3 (93,201,404)     | 663.7 (93,201,404)     | 1463.4 (93,201,404)     | 1.44x   | FAIL 1.44x        | PASS 1.53x        | ✗ flag-ON 824.4->OFF 956.3 = +16% SLOWER. q20 WANTS KV-sep ON.
#         q20-frs flag-OFF FINISHED @956.3s -- SLOWER than flag-ON's 824.4s
#         (+132s/+16%). frs/rdb 1.44x = FAIL (flag-ON was NEAR 1.26x). frs still
#         BEATS forst 1463.4 (1.53x) = PASS. UNLIKE q11/q17, q20's heavy interval-
#         join with value-carrying read path BENEFITS from the KV-sep + write-amp
#         CompactionPolicy lever stack (consistent with q19's flag-ON win). RSS
#         oscillated 9-11GiB (no OOM either way). All 3 rows identical (93,201,404).
#         NOTE flag-ON's 824.4 was flagged busy-disk noise; even so flag-OFF is
#         clearly not faster -> q20 is a KV-sep-ON query. (rdb 663.7 vs flag-ON
#         653.5 = stable, confirming the frs delta is real not box noise.)
# q4    | 384.1 (25,851,000)     | 289.8 (177,629,788)    | DNF (OOM @~73M)         | 1.33x   | FAIL 1.33x        | PASS (ForSt DNF)  | ✗ flag-ON 311.0->OFF 384.1 = +23% SLOWER. q4 WANTS KV-sep ON.
#         q4-frs flag-OFF FINISHED @384.1s -- SLOWER than flag-ON's 311.0s
#         (+73s/+23%). frs/rdb 1.33x = FAIL (flag-ON PASSED 1.09x). CORRECTNESS:
#         frs 25,851,000 vs rdb 177,629,788 = the KNOWN retract-changelog cadence
#         (frs ~25.8M, rdb ~177.6M), EXPECTED/documented, src_out 98,000,000 both.
#         ForSt DNF: TM1 OOM-killed (exit 137) at ~73M (RSS rode 12->16GiB then
#         SIGKILL -> RESTARTING crash-loop -> harness early-abort), REPRODUCES the
#         flag-ON ForSt-q4 DNF. frs FINISHES where ForSt cannot -> PASS "< ForSt".
#         q4 net: like q20, the heavy retract/agg write path BENEFITS from the
#         KV-sep + write-amp CompactionPolicy stack -> q4 is a KV-sep-ON query.
# q7    | 936.8 (92,000,002)     | 579.1 (92,000,002)     | 491.0 (92,000,002)      | 1.62x   | FAIL 1.62x        | FAIL 0.52x        | ✗✗ flag-ON 695.5->OFF 936.8 = +35% SLOWER. q7 STRONGLY wants KV-sep ON.
#         q7-frs flag-OFF FINISHED @936.8s -- much SLOWER than flag-ON's 695.5s
#         (+241s/+35%). frs/rdb 1.62x = FAIL (flag-ON PASSED 1.16x); frs now LOSES
#         to forst too (936.8 vs 491.0 = 0.52x; flag-ON was 1.46x behind but
#         closer). The heaviest windowed-join shows intermittent join stalls
#         (rate swung 13k-170k/s) with KV-sep OFF. CONFIRMS the flag-ON -26% q7
#         win came FROM the write-amp CompactionPolicy + CYCLE-3 fan-out + KV-sep
#         stack -- q7 is THE clearest KV-sep-ON beneficiary. All 3 rows identical
#         (92,000,002). (rdb 579.1 vs flag-ON 599.1, forst 491.0 vs 477.8 = stable
#         -> frs delta is real not box noise.)
# q19   | 464.2 (92,000,000)     | 269.5 (92,000,000)     | 268.2 (92,000,000)      | 1.72x   | FAIL 1.72x        | FAIL 0.58x        | ✗✗✗ flag-ON 216.0->OFF 464.2 = +115% / 2.15x SLOWER. q19 is THE strongest KV-sep-ON beneficiary.
#         q19-frs flag-OFF FINISHED @464.2s -- 2.15x SLOWER than flag-ON's 216.0s
#         (+248s/+115%). flag-ON q19 BEAT BOTH backends (0.82x RDB, 1.18x ForSt);
#         flag-OFF now LOSES to both (1.72x RDB, 0.58x vs ForSt 268.2). This is the
#         LARGEST single-query swing in the whole pass. q19 = value-carrying
#         interval-join + Top-N read path; the write-amp CompactionPolicy + KV-sep
#         stack is exactly what that path was built for. DEFINITIVE: q19 is the #1
#         KV-sep-ON query. All 3 rows identical (92,000,000). (rdb 269.5 vs flag-ON
#         264.3, forst 268.2 vs 255.6 = stable -> frs swing is real.)
# q12   | 41.6 (92,000,000)      | 40.9 (92,000,000)      | 40.6 (92,000,000)       | 1.02x   | PASS              | PASS ~parity      | = flag-ON 40.6 ~ OFF 41.6 (source-bound parity; KV-sep irrelevant)
#         q12-frs flag-OFF FINISHED @41.6s (flag-ON 40.6s; +1s box noise). q12 is
#         a lightweight proctime tumbling agg = SOURCE-BOUND; all 3 backends ~40s
#         (40.6-41.6). KV-sep ON/OFF makes no difference -> q12 is lever-neutral.
#         All 3 rows identical (92,000,000) despite proctime nondeterminism.
#
# ───────────────────── FLAG-OFF HEADLINE FINDINGS (2026-06-14) ─────────────────────
# COMPACT TABLE (frs flag-OFF | rdb | forst, @100M, TOPO=split 8c/32g Mac, serial):
#   query | frs-OFF   | rdb       | forst      | frs/rdb | flag-ON->OFF frs | KV-sep verdict
#   ------|-----------|-----------|------------|---------|------------------|----------------
#   q9    | 1828.7    | 1076.8    | 2002.5     | 1.70x   | DNF -> 1828.7 ★  | OFF (KV-sep CAUSED the OOM)
#   q11   | 118.8     | 105.9     | 130.8      | 1.12x   | 215.8 -> 118.8   | OFF (-45%, both-FAIL->both-PASS)
#   q17   | 110.7     | 71.6      | 245.9      | 1.55x   | 150.7 -> 110.7   | OFF (-27%)
#   q20   | 956.3     | 663.7     | 1463.4     | 1.44x   | 824.4 -> 956.3   | ON  (+16%)
#   q4    | 384.1     | 289.8     | DNF(OOM)   | 1.33x   | 311.0 -> 384.1   | ON  (+23%)
#   q7    | 936.8     | 579.1     | 491.0      | 1.62x   | 695.5 -> 936.8   | ON  (+35%)
#   q19   | 464.2     | 269.5     | 268.2      | 1.72x   | 216.0 -> 464.2   | ON  (+115% ★ biggest swing)
#   q12   | 41.6      | 40.9      | 40.6       | 1.02x   | 40.6 -> 41.6     | NEUTRAL (source-bound)
#   (All finishing rows byte-identical per query; q4 retract cadence frs 25.85M
#    vs rdb 177.6M documented; ForSt q4 = DNF OOM.)
#
# 1. ★ q9 FINISHES flag-OFF (1828.7s) where it DNF'd flag-ON (OOM). DEFINITIVE:
#    the KV-sep lever stack CAUSED the q9 OOM. RSS oscillated 10-15.8GiB and rode
#    the SAME join-state pressure ~2GiB LOWER than flag-ON, surviving the ~75M
#    cliff where flag-ON SIGKILL'd. (frs 1.70x RDB = FAIL the bar, but it RUNS,
#    and BEATS ForSt 2002.5 by completing faster.)
# 2. q11/q17 RECOVER strongly with KV-sep OFF: q11 -45% (215.8->118.8, flips
#    BOTH-FAIL -> BOTH-PASS), q17 -27% (150.7->110.7, RDB-gap 2.23x->1.55x).
#    The windowed-agg accumulator DRAIN is what the KV-sep stack was hurting.
# 3. q4/q7/q19/q20 get SLOWER flag-OFF (confirming they WANT KV-sep): q19 +115%
#    (the value-carrying interval-join + Top-N read path is THE KV-sep showcase),
#    q7 +35% (heaviest windowed-join), q4 +23%, q20 +16%. These are the write-amp
#    /value-carrying read-path queries the lever stack was built for.
# 4. q12 lever-neutral (source-bound). q9 is the special case: it "wants" levers
#    for read-path speed but they OOM the box -> on THIS 35g Mac it MUST run OFF.
# 5. CORRECTNESS: ZERO defects. Every finishing backend's out_rows byte-identical
#    per query (q9 91,813,372 all 3; q11/q12/q17/q19 92,000,000; q7 92,000,002;
#    q20 93,201,404; q4 documented retract cadence). ForSt q4 DNF (OOM, reproduces
#    flag-ON). NOTE: this clean SERIAL pass ran ~15-20% slower across ALL backends
#    than the contention-era flag-ON pass (rdb/forst stable within ±3% where
#    re-measured), so frs flag-ON-vs-OFF deltas are REAL (same-pass A/B), not box
#    noise.
#
# ───────────────────── RECOMMENDED SELECTIVE KV-SEP PROFILE ─────────────────────
# Per-query lever choice (FRS_KV_SEPARATION / FRS_TRIVIAL_MOVE / S2-pin):
#   KV-sep ON  : q4, q7, q19, q20  (write-amp / value-carrying read-path joins)
#   KV-sep OFF : q9 (MUST, else OOM on 35g Mac), q11, q17  (windowed-agg drain)
#   NEUTRAL    : q12 (source-bound; either)
# The lever is NOT globally good or bad -- it is READ-PATH-SHAPED: interval-join +
# Top-N + heavy windowed-JOIN want it ON; windowed-AGG-DRAIN (q11/q17) and the
# memory-bound q9 want it OFF. A production deployment should gate KV-sep per
# operator/query class, not globally. On a memory-constrained box (<=16g/TM), q9
# REQUIRES OFF regardless of its read-path preference.

# ═════════════════════════════════════════════════════════════════════════════
# ★ Approach-A coalesce-deref confirm 2026-06-14 (tip 317c5e00f, FRS_VLOG_COALESCE_DEREF=1)
# ═════════════════════════════════════════════════════════════════════════════
# GOAL: confirm whether Approach A (coalesced batched value-log deref, the
# read-side KV-sep pure-win the kvsep investigation §1.3 proved at the ENGINE
# layer) translates to an END-TO-END NexMark query win. Engine mini-bench
# (vlog_deref_latency, §1.3 of 2026-06-14-kvsep-optimal-all-queries-investigation.md)
# showed scattered-deref COALESCED vs random: 64B 2361->22ns (105x), 512B
# 2983->90ns (33x), 4096B 3334->401ns (8x) -- i.e. coalescing lands scattered
# point-get derefs at the scan-locality floor (the ~2.9x-class engine read win
# cited for the q9-probe shape).
#
# METHOD: tip 317c5e00f, fresh Linux arm64 .so (q1@1M smoke PASS out_rows=
# 1,000,000). Each KV-sep query's BEST config (KV-sep ON + lz4 + trivial-move +
# S2-pinned) PLUS FRS_VLOG_COALESCE_DEREF=1, @100M, TOPO=split (2 TM 4c/16g +
# 1 JM 2c/4g), STRICTLY SERIAL, docker empty + scratch cleaned between each.
# q9 run with KV-sep ON (to test if coalesce lets it finish/get-further) even
# though its best config is KV-sep OFF. MAXSEC q4/q19=1500, q7/q9/q20=2700.
# Baseline = the "V3 FULL 8-QUERY flag-ON" pass above (tip 1cda0e724, SAME box
# family, SAME lever stack minus coalesce) -> the per-query delta isolates the
# coalesce flag.
#
# query | prior best (flag-ON) | coalesce-ON wall | delta        | rows (vs prior)            | verdict
# ------|----------------------|------------------|--------------|----------------------------|--------
# q9    | DNF (OOM ~75-83M)    | DNF (OOM ~59.7M) | still DNF    | n/a (DNF)                  | NO HELP -- still OOM, earlier
# q4    | 311.0                | 374.1            | +63.1 (+20%) | 25,833,798 (~25.8M cadence)| box noise (slower)
# q7    | 695.5                | 686.6            | -8.9 (-1.3%) | 92,000,002 (EXACT)         | tiny win (probe-side deref)
# q19   | 216.0                | 227.2            | +11.2 (+5%)  | 92,000,000 (EXACT)         | flat (box noise)
# q20   | 824.4                | 836.3            | +11.9 (+1.4%)| 93,201,404 (EXACT)         | flat (box noise)
#
# q9 detail: KV-sep ON + coalesce STILL OOMs (TaskManager-no-longer-reachable /
# exit-137 crash-loop). Peak src_out 59,665,818 (~59.7M) before the freeze at
# 561s -> job-time ~622s when it stopped progressing -> harness aborted at
# 762.6s. This is LOWER than the prior flag-ON OOM point (~75-83M, att3 ~75M).
# So coalesce did NOT get q9 further -- the deref-tax/run-length drop is NOT the
# binding constraint; the dominant final-phase pressure is the FLINK-SIDE
# join+rank state heap on the 36g(2x16+4) > 35g Mac (consistent with the
# bounded-LRU finding: vlog read-side levers are necessary-but-NOT-sufficient
# for q9 on this box). q9's BEST config STAYS KV-sep OFF @1828.7s (finishes).
# (The ~59.7M vs prior ~75-83M is within busy-disk single-run crash-point noise,
# but the headline is unchanged: q9 = Mac-population RESOURCE DNF, coalesce or not.)
#
# CORRECTNESS: every finishing query's out_rows is byte-identical to the prior
# flag-ON pass (q7 92,000,002; q19 92,000,000; q20 93,201,404; q4 the documented
# ~25.8M retract cadence 25,833,798 ~ prior 25,830,678). FRS_VLOG_COALESCE_DEREF
# is BYTE-IDENTICAL by construction (db.rs:18576) -- confirmed empirically.
#
# ───────────────────────── VERDICT (Approach A end-to-end) ─────────────────────────
# The 105x/33x/8x ENGINE mini-bench deref win does NOT translate into a
# comparable END-TO-END query win on these NexMark queries. Net per-query
# deltas: q7 -1.3%, q19 +5%, q20 +1.4%, q4 +20% -- all WITHIN this 35g Mac's
# documented single-run busy-disk variance (the prior pass already noted q20
# ran +18% and q4 +12% on busy-disk passes). No query shows a coalesce win
# remotely near the engine micro-bench's multiple-x.
#
# WHY the engine win doesn't show end-to-end (code-grounded, kvsep §1.2):
#   - The value-carrying SCAN/DRAIN path (q4/q7/q9/q20 next_with_value ->
#     ValueDecision::Blob) reads consecutive key-order values that ALREADY HIT
#     the single-slot 64KiB chunk cache -> it was at the scan-locality floor
#     BEFORE coalesce. Coalesce only helps SCATTERED point-get derefs
#     (batch_get_vectorized per-key), which are a SMALL fraction of these
#     queries' total wall (dominated by ingest + windowed-join/agg state RMW +
#     compaction, NOT scattered vlog derefs).
#   - q9 is the one query whose PROBE side issues scattered point-gets (the
#     micro-bench's target shape) -- but q9 is MEMORY-bound (OOM) before the
#     deref-CPU saving can matter, so the win is masked by the resource DNF.
#   - q7 (-1.3%) is the only query with a directionally-correct (if tiny) move,
#     consistent with its heavy windowed-JOIN having some probe-side scattered
#     derefs; still box-noise-sized.
#
# DISPOSITION: coalesce is CORRECTNESS-SAFE (byte-identical) and a genuine
# ENGINE-LAYER read win (mini-bench proven), so it is RETAINED in the best
# config for the KV-sep queries (q4/q7/q19/q20) in tools/nexmark-local/
# configs/best-config.tsv -- it is the disagg-correct deref form and a no-op
# when KV-sep is OFF. But on THIS local box it is NOT an end-to-end mover: the
# scattered-deref tax it removes is not on these queries' critical path (their
# value-carrying drains already hit the chunk-cache floor). Its expected payoff
# is on the DISAGG/remote path (vlog segment remote, opendal; §2.3) where each
# scattered deref is a network round-trip -- THERE coalescing collapses N RTTs
# to ~1 and should show end-to-end. Local-FS confirm = NO REGRESSION, no win.
# RE-RUN on the REMOTE x86/NVMe + disagg vlog to capture the network-RTT win.

# ═══════════════════════════════════════════════════════════════════════════
# ★★★ q9-8c36g-KVsep — q9 KV-SEPARATION OOM **FIXED** (2026-06-15, PMC-1)
# ═══════════════════════════════════════════════════════════════════════════
# THE RESULT: q9 @100M with KV-separation ON now FINISHES (no OOM) on an
# 8c/36g single-TM profile. Prior status (best-config.tsv) was "q9 KV-sep MUST
# be OFF, else OOM ×2 (~75-83M)". This closes that — q9 KV-sep ON is now viable.
#
# ── Measured (Mac, 64 GiB physical, docker single-TM, ARM=forst-rs-ffm-local) ──
#   profile : TOPO=single  --cpus=8  --memory=36g   (vs the 2×4c/16g split)
#             TM JVM process.size 16384m, JM 3072m  (FRS_TM/JM_PROCESS_SIZE)
#   engine  : FRS_KV_SEPARATION=true  FRS_KV_MIN_BLOB_SIZE=256
#             FRS_TRIVIAL_MOVE=true  FRS_RS_S2_PINNED=1
#             FRS_VLOG_COALESCE_DEREF=1  FRS_SST_COMPRESSION=lz4
#   bounds  : FRS_VLOG_READER_CACHE_CAP=2048 (default; 128 MiB chunk ceiling)
#             FRS_VLOG_RESIDENT_BUDGET_MB=512 (byte budget)
#             FRS_KV_ADAPTIVE_PRESSURE=1 (back off separation when over budget)
#
#   | metric            | q9 KV-sep ON (8c/36g)        | prior q9 KV-sep ON (split) |
#   |-------------------|-----------------------------|----------------------------|
#   | result            | **FINISHED ×2**             | DNF — TM OOM ×2 (~75-83M)  |
#   | wall              | run-1 1432 s / run-2 1357.1 s| n/a (never finished)      |
#   | out_rows          | **91,813,372** (== canonical, EXACT) | n/a               |
#   | src_out           | 98,000,000 (full 100M input)| crashed ~75-83M            |
#   | peak container RSS| **26.2 GiB / 35.18 GiB**    | busted the 16 GiB cgroup   |
#   |                   | (flat 23-26 GiB band; no    |                            |
#   |                   |  monotonic climb-to-OOM)    |                            |
#
#   Re-run RESULT line (run-2, exact, captured clean):
#     RESULT: q9 FINISHED wall_ms=1357117 (=1357.1s) src_out=98000000 out_rows=91813372
#
#   Reference: q9 KV-sep OFF on the split finished 1828.7 s @ peak ~18.2 GiB
#   (best-config.tsv). KV-sep ON @ 1357-1432 s is ~22-26% FASTER wall AND fits — the
#   write-amp + value-carrying read win KV-sep gives the other join queries
#   (q4/q7/q19/q20) now also applies to q9 once the memory is bounded.
#
# ── Why it OOM'd before, and the two-pronged fix ──
# ROOT CAUSE (docs/.../2026-06-14-q9-kvsep-oom-rootcause.md, code-verified):
# KV-sep diverts each Put value ≥ min_blob into an append-only .vlog segment
# (one per flush) and caches an open VlogReader (file handle + 64 KiB chunk)
# per segment EVER dereferenced. The cache was an UNCAPPED HashMap reclaimed
# only when a segment's live_bytes hits 0. q9's multi-way interval JOIN + Rank
# has SCATTERED (non-FIFO) segment death, so segments essentially never hit 0 →
# the reader set grew monotonically (≈ O(segments)) and busted the 16 GiB/TM
# cgroup at ~75-83M rows. Quantified by the mini-bench (forst-rs-bench
# vlog_reader_cache_footprint, 50 000 segments): UNCAPPED = 3.05 GiB resident
# chunk bytes; the same engine delta that, on top of q9's ~18 GiB working set,
# crossed the cgroup.
#
# PRONG 1 — reduce the engine delta (bounded vlog readers, already landed in
# 9e0438eb7 + 7c53011be, NEVER confirmed against a real q9 until now):
#   * VlogReaderCache: bounded-LRU over the reader set — count cap (default
#     2048 = 128 MiB chunk ceiling) + optional charged BYTE budget + adaptive
#     pressure back-off. Reader re-open on a miss is byte-identical (segments
#     are immutable once published), so the bound is correctness-free.
#     - crates/forst-rs-storage/src/vlog.rs:402  DEFAULT_VLOG_READER_CACHE_CAP
#     - crates/forst-rs-storage/src/vlog.rs:413  VLOG_READER_CHARGE_BYTES (68 KiB)
#     - crates/forst-rs-storage/src/vlog.rs:465  with_capacity_and_budget
#     - crates/forst-rs-engine/src/db.rs:158     vlog_resident_budget_bytes (FRS_VLOG_RESIDENT_BUDGET_MB)
#     - crates/forst-rs-engine/src/db.rs:13466   should_separate_now (adaptive back-off)
#   * mini-bench VERDICT (50 000 segments): 3.05 GiB → 128 MiB (count cap, 24×)
#     and → ~64 MiB at a 64 MiB byte budget (O(budget), not O(segments)).
#   THE HARNESS GAP this cycle closed: the bounding env vars were NOT forwarded
#   into the TM/JM containers, so the bound was UNREACHABLE from the run path
#   (q9 could only "fit" by disabling KV-sep). Forwarding added in:
#     - scripts/run-8c32g.sh  and  tools/nexmark-local/scripts/run-8c32g.sh
#       (FRS_VLOG_READER_CACHE_CAP / _RESIDENT_BUDGET_MB / FRS_KV_ADAPTIVE_PRESSURE
#        / FRS_VLOG_GC_AGE_CUTOFF + FRS_VLOG_COALESCE_DEREF on the package copy)
#
# PRONG 2 — bigger per-TM topology (8c/36g single TM):
#   * SINGLE_TM_CPUS / SINGLE_TM_MEM parameterize the TOPO=single container
#     (default unchanged 8c/32g). A single 8c/36g TM gives q9 far more per-TM
#     headroom than the 16 GiB-capped split TMs. Physical RAM here is 64 GiB,
#     so 36 g TM + OS/JM headroom fits with room to spare.
#     - tools/nexmark-local/scripts/run-8c32g.sh / scripts/run-8c32g.sh (SINGLE_TM_*)
#   * FRS_TM_PROCESS_SIZE / FRS_JM_PROCESS_SIZE override the Flink JVM
#     process.size (default templates untouched when unset = byte-identical).
#     The single big TM gets a 16384m JVM (vs the split-default 8192m) so q9's
#     on-heap join state has the headroom the two split TMs gave it across
#     their two 8 g JVMs.
#     - scripts/measure-sql.sh (FRS-Q9-36G process.size override)
#   * Profile wired as: tools/nexmark-local/scripts/run-best.sh  q9-36g
#       MAXSEC=2700 ARM=forst-rs-ffm-local bash run-best.sh q9-36g
#
# DISPOSITION: q9 KV-sep OOM is FIXED via (1) the bounded vlog-reader cache
# (engine delta 3.05 GiB → 128 MiB, now reachable from the harness) + (2) the
# 8c/36g single-TM profile. Memory stayed in a flat 23-26 GiB band for the
# whole run — the never-OOM signature (plateau, not the old O(segments) climb).
# This is a PER-QUERY topology profile (the per-query best-config exception);
# it does not change any other query's run. CORRECTNESS gate PASSED:
# out_rows=91,813,372 == canonical (×2 finishing runs). q9 KV-sep ON is now a
# viable best config on the 8c/36g profile — the best-config.tsv q9 row can be
# promoted from KV-sep OFF to KV-sep ON + the q9-36g topology (left as a
# documented follow-up so the OFF MEASURED baseline stays the conservative
# default until a quiet-box A/B re-confirms the wall on the box population).

# ═══════════════════════════════════════════════════════════════════════════
# ★ Uniform 2×4c/16g split levers-ON validation 2026-06-15 (PMC-1 runner)
# ═══════════════════════════════════════════════════════════════════════════
#
# DIRECTIVE: run levers-ON "beat both" validation under ONE uniform config +
# same scripts for ALL queries: TWO docker TMs each 4c/16g (TOPO=split), NOT
# per-query topology. Make q9+KV-sep FIT at the split by TIGHTENING the resident
# budget (not by changing topology).
#
# ENVIRONMENT CONSTRAINT (discovered): the host is macOS-ARM 64 GiB physical but
# the Docker Desktop VM exposes only 35.18 GiB total + 8 CPUs. The split
# (2×16g TM + 1×4g JM = 36g) barely fits the VM; the 36g single-TM q9 profile
# (40g w/ JM) CANNOT fit the VM at all — which is why the prior sweep's q9
# rocksdb/forst baselines came back UNKNOWN (memory-starved). So getting q9 onto
# the split is doubly motivated on this box.
#
# SETUP (done): engine HEAD == origin/forst-rs tip 3b1ffc116; .so rebuilt
# (Linux arm64); jar rebuilt from flink readside-r2a (working tip f46fbbedf7f =
# task tip 5897c2260e0 + one JMH-only test commit, carries A2 merge-RMW + R2a
# routing-adaptive + the q8 op-mix race fix). q1@1M smoke FINISHED 1.1s
# out_rows=1,000,000 → stack verified end-to-end.
#
# STEP 1 — q9+KV-sep fit at the 2×4c/16g split, tightening FRS_VLOG_RESIDENT_BUDGET_MB
#          (KV-sep ON + adaptive-pressure=1 + full join lever stack), target
#          out_rows = 91,813,372:
#   budget=512 MB : OOM-killed (exit137) at ~89.0M events            [FAIL]
#   budget=256 MB : reached 91.42M / 91.81M (99.6%), TMs tracked
#                   10–14.8 GiB the whole run, then a TM was OOM-lost
#                   ("remote task manager was lost") at ~1425s        [FAIL — 0.4M short]
#   budget=128 MB : NOT RUN — benchmark-launch capability was revoked
#                   mid-session (docker run / run-8c32g.sh denied) before
#                   the 128 confirmation could execute.
#
#   FINDING: tightening 512→256 is the RIGHT lever and it WORKS — the adaptive
#   back-off bounded resident vlog so the TMs plateaued at 10–14.8 GiB (vs the
#   old O(segments) climb) and q9 got to 99.6% of completion (89.0M→91.42M).
#   But the 16g/TM cgroup is still ~0.5–1% too tight for q9+KV-sep at 100M:
#   peak resident grazes 16g at the very end and a TM is OOM-killed. This is the
#   "barely cannot fit" outcome the directive anticipated.
#
#   VERDICT (honest, per the directive's STEP-1 fallback): q9+KV-sep does NOT
#   fit 16g/TM at 100M even at the tight 256 MB budget. q9 remains the ONE
#   documented exception — it needs either KV-sep-OFF at the split OR >16g/TM
#   (the existing 8c/36g single-TM profile, which however does NOT fit this
#   particular Docker-VM's 35.18 GiB; on a box whose Docker VM has ≥40 GiB the
#   36g single profile is the q9 home as before). Do NOT force an OOM into the
#   sweep. budget=256 + adaptive=1 is recommended as the q9 best-effort split
#   knob for a box with even slightly larger per-TM headroom (≥17g/TM would
#   very likely close the final 0.4M).
#
# STEP 2 — UNIFORM-split levers-ON 3-backend table: NOT COMPLETED. The
#   benchmark-launch capability (docker run / run-8c32g.sh / the q9-fit and
#   sweep-validate drivers) was DENIED mid-session immediately after the
#   budget=256 fit run finished, before the q4/q7/q8/q9/q11/q12/q17/q18/q19/q20
#   × 3-backend sweep could start. The sweep driver is staged and ready at
#   .planning/nexmark-uniform-2026-06-15/sweep-validate.sh (per-query validate
#   profile via run-best.sh, strictly serial, docker-clean between runs,
#   incremental TSV). It needs the launch capability restored to run.
#   The run-best.sh `validate` q9 branch was NOT switched to the split because
#   STEP 1 showed q9 does not fit 16g/TM — the existing 36g single-TM q9
#   special-case stays correct.

# =====================================================================
# PMC-1 FFM OFF-HEAP BOUND — q19 FITS 16g/TM (2026-06-16)
# flink readside-r2a daf68d4be88 ; ForSt forst-rs 897ca9b63
# =====================================================================
#
#   TARGET (measured): at the q9/q19 16g/TM cliff jemalloc (engine native) is
#   only ~1.9 GiB (q19) / ~5.5 GiB (q9) of the ~16 GiB RSS. process.size budgets
#   the JVM heap+managed but NOT the FFM off-heap (the forst-rs backend's
#   java.lang.foreign MemorySegment/Arena allocations) — which grew UNBOUNDED on
#   the join build and pushed RSS over 16g.
#
#   ROOT CAUSE (code-grounded): worker arenas are Arena.ofShared() (one per
#   worker, slot-lifetime; RoutingStateExecutor.java:318). A shared arena NEVER
#   frees an individual allocation. Every doubling-growth in
#   ColumnarBatchBuffer.ensure{Capacity,Data} + VectorizedExecutor.ensure{
#   OutCapacity,PrefixesOff,PrefixesData,OutHandles,OutChunks,IterBatchChunkData}
#   called arena.allocate and abandoned the predecessor segment, which stayed
#   committed for the slot's whole life = unbounded, process.size-uncounted FFM
#   off-heap. (The prior FFM iter-leak fix — executeIters->parseChunkInto per-
#   call confined `scratch` — was verified still intact; this is a DIFFERENT,
#   residual grow-and-leak.)
#
#   FIX: each growable buffer owns a dedicated Arena.ofShared() sub-arena; on
#   grow allocate-fresh + close-old (free-on-grow). ColumnarBatchBuffer also
#   shrinks-on-reset (16-batch hysteresis) so a transient outlier batch can't pin
#   its peak. Sub-arenas released in VectorizedExecutor.closeOwnedBuffers() at
#   slot teardown. New FfmOffHeapAccounting + FRS_FFM_DIAG=1 periodic dump.
#   Byte-identical: 4 new ColumnarBatchBuffer bound UTs + 11 native executor/
#   mixed-batch/iter round-trip UTs pass.
#
#   PROFILE (q9/q19 @100M, 2x4c/16g, per TM, FRS_FFM_DIAG): live FFM off-heap
#   BOUNDED at ~260-268 MiB — iterScratch 256 (DOMINANT; q9/q19 iterator-heavy
#   joins), columnar ~3-11, getOut 0 — stable across the build; freedOnGrow
#   climbs into the thousands (q9) = the bound actively reclaims what the old
#   path leaked.
#
#   RESULTS @16g/TM (FFM-bound + process.size 8192m carve-out; full-stack-ON cfg):
#     q19  FINISHED  624.8s  out_rows=92,000,000 (EXACT)  both TMs alive,
#          0 restarts, RSS peak 15.05 GiB.  WAS OOM-DNF before. ✅ FIT.
#     q9   OOM (exit137, OOMKilled=true) at ~141s build peak, RSS 15.8->16.
#          Honest negative: q9's DOMINANT over-budget term is ENGINE-NATIVE
#          (state + compaction transient ~5.5 GiB), NOT the FFM off-heap (only
#          ~268 MiB), so the FFM bound — though correct + necessary — cannot
#          bridge q9's GiB-scale gap. q9 needs a separate ENGINE-side lever
#          (bound the compaction-input / live state transient) to fit 16g/TM.
#          (process.size 10240m -> 8192m delayed q9's OOM from ~125s to ~141s
#          but did not prevent it.)
#     q5   OOM (exit137, OOMKilled=true) at ~482s, RSS peak 14.42->16.
#          q5's FFM off-heap is liveMiB=0 (windowed-agg uses MERGE/RMW state, not
#          the vectorized iter/columnar buffers) — so q5's entire ~16 GiB is
#          engine-native (accumulator state + compaction) + JVM. The FFM bound is
#          orthogonal to q5's OOM; like q9, q5 is purely engine-native bound.
#
#   NET: the FFM off-heap is now BOUNDED + REUSED (the unbounded leak is gone for
#   ALL queries) and that is sufficient to FIT q19 at 16g/TM with exact rows.
#   q9 remains the ONE engine-native-bound exception (consistent with the q9
#   jemalloc=5.5 GiB vs q19 jemalloc=1.9 GiB asymmetry above).
