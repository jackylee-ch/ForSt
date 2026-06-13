# Cycle-3 Read/Scan Lane — S2 × L4 × KV-sep Combined Read-Path Gate

**Date:** 2026-06-13
**Owner:** PMC-1 (Phase-1, read/scan lane)
**Tree measured:** `forst-rs` tip `8e699248b` + this cycle's two test commits (below).
**Scope:** ENGINE repo only (flink owned concurrently by PMC-2). Local-measurable
gates only — the remote q9/q20 @100M macro-gates are bridge-blocked and out of
scope this cycle (noted GATED throughout).
**Box:** macOS 14 (Darwin 25.5.0), Apple Silicon, release builds.
**Method:** criterion (each cell is criterion's own 100-sample — or 10-sample for
`iter_drain` — point estimate); each config run **3×**, the value tabled is the
**median-of-3** of those point estimates. Raw: `/tmp/c3_matrix_raw.tsv`,
`/tmp/c3_iterdrain_raw.tsv`, `/tmp/c3_sepactive_raw.tsv`.

Flags (all default-OFF; driven per-process via env, read once at process start):
`FRS_KV_SEPARATION=1` + `FRS_SST_COMPRESSION=lz4` = the **WA fair baseline** (the
write-amp work that shipped this cycle); `FRS_RS_S2_PINNED=1` = S2 (pinned-block
replenish + loser-tree merge); `FRS_COMPACT_WINDOWED=1` = L4 (windowed
compaction-input reads).

---

## 1. Headline

**S2 is the read-path lever; it composes cleanly with the KV-sep+lz4 fair
baseline; L4 is a no-op on the scan path (it only touches the compaction-input
path, which these benches never exercise).**

- **Deep-fan-out join probe (`join_probe_open/ssts_128`, the q20 interval-join
  shape): 7.54 µs → 0.90 µs with S2 ON — an ~8.4× cut**, and the win is
  IDENTICAL with KV-sep+lz4 ON (`wa_s2` 0.899 µs) and with L4 added
  (`wa_s2_l4` 0.889 µs). The loser-tree converts the O(sources)-twice merge
  into O(log n) exactly where the spec predicted (ssts_64→128 crossover).
- **Per-row emit (`iter_drain`): 15.09 ms → 13.49 ms with S2 ON (−10.6 %)**,
  i.e. ~151 → ~135 ns/row — the alloc-free pinned emit boundary.
- **S2 carries a small shallow-fan-out tax on the Arc-pair compat adapter
  only** (ssts_1 364 → 431 ns, +18 %); the raw sink path
  (`join_probe_open_fill_into/ssts_1`) shows NO tax (338 vs 338 ns) — the tax
  is the in-process adapter, not the engine path.
- **S2 regresses churn-heavy scans** (`hot_prefix_churn` 52.2 → 62.4 µs, +20 %;
  `range_scan_multilevel` 1.039 → 1.216 ms, +17 %) — the dup/tombstone-drain
  cost on the loser tree at small fan-out with heavy duplicate runs.
- **When KV-sep is ACTUALLY engaged (values ≥ threshold), S2 stops being
  optional: the legacy deep-probe EXPLODES to 18.09 µs (vlog deref per shadowed
  version) and S2 brings it back to 1.00 µs (~18×).** S2 is what prevents KV-sep
  from regressing the deep-fan-out read path (§3).

---

## 2. Combined gate table (median-of-3, ns unless noted)

| Bench cell | what it proxies | default | wa (KV-sep+lz4) | wa+S2 | wa+L4 | wa+S2+L4 |
|---|---|---|---|---|---|---|
| `join_probe_open/ssts_1`   | q3/q4 shallow probe (adapter) | 364 | 363 | 431 | 369 | 424 |
| `join_probe_open/ssts_8`   | shallow probe | 385 | 386 | 436 | 391 | 445 |
| `join_probe_open/ssts_32`  | mid fan-out | 414 | 427 | 478 | 425 | 472 |
| `join_probe_open/ssts_64`  | mid/deep | 433 | 441 | 505 | 461 | 494 |
| **`join_probe_open/ssts_128`** | **q20 deep fan-out (adapter)** | **7540** | **7620** | **899** | **7620** | **889** |
| `…_fill_into/ssts_1`   | shallow, RAW sink (always pinned) | 338 | 339 | 341 | 336 | 336 |
| `…_fill_into/ssts_64`  | mid, raw sink | 431 | 432 | 426 | 427 | 437 |
| **`…_fill_into/ssts_128`** | **deep, raw sink** | **813** | **824** | **823** | **812** | **845** |
| `iter_drain` (100×1000) | per-row emit full path | 15.09 ms | 15.07 ms | **13.49 ms** | 16.12 ms | **13.68 ms** |
| `hot_prefix_churn/1000` | dup/tombstone drain | 52.2 µs | 51.8 µs | 62.4 µs | 53.1 µs | 63.2 µs |
| `range_scan_multilevel/20000` | multilevel merge | 1.039 ms | 1.055 ms | 1.216 ms | 1.070 ms | 1.238 ms |

RocksDB reference (same box, this run): `range_scan_multilevel/rocksdb`
**1.220 ms**, `hot_prefix_churn/rocksdb` **61.7 µs** — forst-rs default BEATS
both; under S2 the churn/scan cells reach rocksdb parity (not a regression vs
the external reference, only vs the forst-rs default-OFF path).

### Reading the table

1. **S2 × KV-sep composition (work-order item 1+2).** Every S2 win is preserved
   when KV-sep+lz4 is ON: ssts_128 0.899 µs (`wa_s2`) ≈ 0.889 µs (`wa_s2_l4`) ≈
   the raw-sink floor 0.82 µs; iter_drain −10.6 % in both. S2's pinned/loser-tree
   path does not interact adversely with KV-sep's value placement on these
   (sub-threshold) benches. **The realistic combined config holds.**
2. **The `fill_into` series proves the win is the engine path, not the bench.**
   The raw push-style sink is ~0.82 µs at ssts_128 in ALL configs (it forces
   pinned internally). The legacy `join_probe_open/ssts_128` only reaches that
   speed when `FRS_RS_S2_PINNED` routes the production Arc-adapter drain through
   the same pinned merge — 7.54 → 0.90 µs. The residual 0.90 vs 0.82 is the
   Arc-pair adapter tax (in-process callers; the FFI chunked path does not pay
   it — it consumes the sink directly).
3. **L4 is read-path-neutral by construction.** `wa` vs `wa_l4` is within noise
   on every scan cell. `FRS_COMPACT_WINDOWED` reshapes only
   `CompactionJob`'s input reads (cache-skip + windowed I/O); these benches build
   state by flush with auto-compaction OFF and scan the flushed L0 set, so no
   compaction runs in the timed window. **L4's win is exclusively on the
   compaction-input path and is NOT measurable by these local read benches — it
   needs the compaction-heavy remote macro-gate (GATED).** Consequently S2×L4
   "composition" on the scan floor is trivial: they touch disjoint paths and the
   combined `wa_s2_l4` equals `wa_s2` everywhere.

---

## 3. KV-sep value-deref: the benches do not exercise it (methodological finding)

All four read benches use values BELOW the default blob threshold (128 B):
`join_probe_open` 64 B, `range_scan`/`hot_prefix_churn` ~11 B. So under the WA
baseline **nothing is relocated to the vlog** — `wa` ≈ `default` on every cell
because KV-sep is inert and lz4 has ~11–64 B to compress. The combined read-path
floor for SMALL-value workloads is therefore unchanged by KV-sep, which is the
honest result for q3/q4-class point/short-scan state.

To measure the actual vlog-deref read tax (the q7/q9/q20 join payload class that
IS ≥128 B in production), a supplementary arm lowers the threshold so the
existing 64 B values DO separate: `FRS_KV_MIN_BLOB_SIZE=22`
(`/tmp/c3_sepactive_raw.tsv`, median-of-3):

| Bench cell | `sep` (KV-sep ACTIVE, S2 OFF) | `sep+S2` | `sep+S2+L4` |
|---|---|---|---|
| **`join_probe_open/ssts_128`** | **18.09 µs** | **1.00 µs** | 0.95 µs |
| `…_fill_into/ssts_128` (raw sink) | 942 ns | 915 ns | 889 ns |
| `join_probe_open/ssts_1` | 365 ns | 433 ns | 423 ns |
| `hot_prefix_churn/1000` | 52.0 µs | 62.9 µs | 62.6 µs |
| `range_scan_multilevel/20000` | 1.061 ms | 1.224 ms | 1.225 ms |

**THE key interaction finding (work-order item 2).** When KV-sep is actually
engaged, the LEGACY deep-probe cost EXPLODES: `join_probe_open/ssts_128` goes
7.62 µs (non-separated `wa`) → **18.09 µs** (separated `sep`) — 2.4× worse,
because the legacy two-phase merge dereferences a vlog BlobRef for EVERY one of
the ~128 stale/shadowed versions it walks per key. **S2 rescues it to 1.00 µs
(~18× faster than the separated legacy path):** the loser-tree + pinned merge
only derefs the WINNING version (shadowed versions drain as dup-pops without a
value fetch). The raw sink path (`fill_into/ssts_128`) is robust to KV-sep
either way (0.89–0.94 µs) — it never derefs shadows. **So under the realistic
KV-sep+S2 combined config, S2 is not merely additive — it is what PREVENTS
KV-sep from regressing the deep-fan-out read path.** This strengthens, not
weakens, the S2 default-ON case for KV-sep-on deployments (still GATED on the
remote macro-gate).

The byte-exact correctness of the S2 pinned path WITH KV-sep ON and LARGE
(2 KiB, vlog-resident) values is proven by the new IT (§5).

---

## 4. Default-flip candidates (GATED — no default flipped this cycle)

Per the work order, defaults are NOT flipped without the remote macro-gate. The
evidence ranks one candidate:

- **`FRS_RS_S2_PINNED` → ON: STRONG candidate at deep fan-out, GATED.**
  +8.4× on the q20 deep-probe cell and −10.6 % per-row emit, composing with the
  KV-sep baseline. BUT two no-regress conditions are NOT yet clear locally:
  (a) the shallow-fan-out adapter tax (+18 % at ssts_1, the q3/q4 point/short
  class) — falsifier §4.3 of the S2 design says the n≤4 linear branch must keep
  this in noise; here it does NOT on the *adapter* path (the engine path is
  clean), so the adapter is the thing to thin OR the flip should be paired with
  the FFI sink path (which does not use the adapter); and (b) the churn/scan
  regression (+17–20 %). Both are acceptable IF the q20/q9 macro-gain dominates,
  which only the remote @100M G5 gate (q20 −9..13 %, q9 −4..8 % model) can
  confirm. **Recommendation: default-ON decision stays GATED on the remote G3
  (q3/q4 no-regress ×3) + G5 (q20/q9 ×3) per S2 design §6.5.** Do not flip now.
- **`FRS_COMPACT_WINDOWED` → ON: NOT a read-path flip candidate.** No read-path
  signal locally (by construction, §2.3). Its case must be made on the
  compaction macro-gate; out of scope this cycle.
- **`FRS_KV_SEPARATION` / `FRS_SST_COMPRESSION`: write-amp lane (PMC cycle-2),
  read-path-neutral for sub-threshold values; no read-path objection to the new
  fair baseline.**

---

## 5. Correctness ITs — triple-combined config (work-order item 2)

- **`s2_kvsep_pinned_byte_equality_combined`** (new, `db.rs`): forces
  KV-sep ON + 2 KiB values (vlog-resident after flush) over a multi-tier fixture
  (2 L0 → L1 rollup + overlapping L0 + inline memtable dups + deletes), then
  asserts the S2 pinned drain (flag-ON) is **byte-identical** to the legacy drain
  (flag-OFF), to the `prefix_scan` collector oracle, AND to the per-key
  point-get oracle — for prefix and range paths. This is the realistic
  KV-sep + S2 combined config; it proves the pinned/loser-tree path derefs vlog
  BlobRefs byte-exactly. PASS. (The pre-existing `s2_pinned_vs_legacy_*` test
  uses tiny inline values + default-OFF KV-sep, so it never touched vlog deref.)
- **`test_empty_prefix_scan_stops_at_upper_bound`** (fixed): this SST
  block-walk-termination test asserted a multi-block SST built from 1 KiB INLINE
  values; under KV-sep those values move to the vlog and the SST collapses to a
  single block, breaking the `total_blocks >= 8` precondition. FIX: build the
  multi-block SST from LARGE KEYS (~240 B, padded) instead — keys are never
  KV-separated, so the precondition is robust under any baseline and the fix
  needs NO global-override toggle (avoiding the parallel-suite race). Verified
  green under both default and KV-sep ON. Cycle-3 interaction finding — without
  the fix the engine suite FAILED under the KV-sep fair baseline.

### Suite gates

| Suite | default | triple (KV-sep+lz4+S2+L4) |
|---|---|---|
| storage lib | 456/0 (3 ign) | 456/0 |
| engine lib (single-threaded) | 378/0 (1 ign: the gate IT) | **378/0** |
| clippy (engine+storage, all-targets) | clean | — |

The triple-combined IT `s2_kvsep_pinned_byte_equality_combined` is `#[ignore]`
(it toggles the process-global `set_kv_separation_override`; the cycle-3 gate
runs it explicitly with `-- --ignored`). `test_empty_prefix_scan_stops_at_upper_bound`
was made baseline-robust WITHOUT any override toggle (multi-block SST now built
from large KEYS, which are never KV-separated) — it passes under both default
and KV-sep ON.

**PRE-EXISTING FLAKE FOUND (NOT this cycle's change; flag to PMC-2/orchestrator):**
`test_deletion_guard_protects_pinned_files_during_compaction` flakes ~37 %
(3/8 measured) on the CLEAN `origin/forst-rs` tip in the parallel suite, panicking
at the final `"file must be deleted after pin release"` assertion. Root cause is
reap-TIMING nondeterminism: after `drop(pin)` the deferred deletion is reaped by
version-retirement, and ONE `flush`+`compact_l0` cycle does not deterministically
drop the retiring version's last ref — its sibling
`test_compaction_defers_delete_while_read_version_held` already "drives a few
cycles" for exactly this reason; this test does not. It uses tiny inline values
and never touches KV-sep, so it is independent of the read/scan lane. Likely
surfaced/worsened by the recently-merged sst_compression/KV-sep work perturbing
background timing. Recommended fix (PMC-2 / engine-core, out of this lane): drive
a bounded reap loop like the sibling test before the final assertion.

---

## 6. Residual read lever (work-order item 3)

The recorded q19/q20 Java-side iterator-handle overhead is out of engine scope.
The engine-side residual the table surfaces is the **Arc-pair compat adapter
tax**: the production in-process drain (`prefix_scan_iter_owned_arc`) re-wraps
the alloc-free sink rows into `Arc` pairs, costing the +18 % shallow tax and the
0.90-vs-0.82 µs deep residual. The FFI chunked path
(`frs_vec_iter_prefix_open/_next` → `fill_chunk_from_iter`) consumes the sink
DIRECTLY and does not pay it — so the q20 production path (FFI) already avoids
the tax, and the adapter cost is confined to in-process engine callers. **Design
note:** if a future cycle wants the in-process callers on the fast path too, the
adapter should materialize via a small reused row buffer (not fresh `Arc`s per
row) — but it is off the q20 hot path, so this is low priority. No new
engine-side prefetch/coalesce lever is indicated by the scan-path data (L4
already owns the compaction-input coalesce; the prefetcher already owns the scan
window).

---

## 7. Updated Phase-1 remaining list (read/scan lane)

| Item | Status |
|---|---|
| S2 (pinned + loser tree) read-path win | **MEASURED locally: ~8.4× deep-probe, −10.6 % per-row; composes with KV-sep.** Default-ON GATED on remote G3+G5. |
| L4 (windowed compaction reads) | Built/merged default-OFF; **read-path-neutral by construction**, win only on compaction macro-gate (GATED, remote). |
| KV-sep × S2 byte-exact correctness | **CLOSED** (new triple-combined IT, §5). |
| Arc-pair compat adapter shallow tax | Identified (§6); in-process only, FFI path clean; low-priority follow-up. |
| q9/q20 @100M A/B vs same-day baseline (G5) | **GATED — remote bridge blocked, out of scope this cycle.** |
| q3/q4 no-regress @100M ×3 (G3) | **GATED — remote.** Local shallow-fan-out adapter tax flagged as the thing G3 must clear. |
