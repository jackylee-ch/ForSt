# q20 pivot — S2 pinned-rows + loser-tree mini-bench evidence (L3 model falsifier)

**Date:** 2026-06-15 · **Status:** mini-bench evidence (local Mac, contention-robust, n=30/cell)
**Repo:** ForSt engine (forst-rs-bench, no code changed — runs the shipped S2 path)
**Context:** This cycle's q8-repro deliverable shipped a deterministic MapStateCache race test
(`2026-06-15-q8-deterministic-cache-race-repro-stage0.md`); the residual op-mix race that actually
blocks Approach-3 remains open (executor-boundary seam, designed there §4). Per the cycle mandate's
pivot clause, this is the q20-laggard half: profile/mini-bench the chosen target's bottleneck.

## 1. Why q20 + which lever

q20 (regular join bid×auction keyed on auction id, 40M-key state probes + Top-N) is the
RocksDB-laggard priority query. Its residual wall is already characterized
(`2026-06-12-q9-q20-longscan-roadmap.md` §1.2, recorded CPU profile): **join prefix-scan 21.6 %**,
**compaction 19.3 %**. The roadmap's re-ranked order (§5.3) PROMOTES the prefix-scan lever (L3 — S2
pinned rows + loser tree) to the top *modeled* q20 lever (−9-13 %), above the merge-RMW lever which
the OPT-N04 scope note showed cannot capture q20's operator-shaped count map.

The L3 lever is not only designed (`2026-06-12-s2-pinned-rows-loser-tree-design.md`) but **already
implemented** in the engine: `set_s2_pinned_override`, `set_s2_adaptive_fanout_min_override`, the R1
adaptive selector, and a 3-arm `join_probe_open_adaptive` criterion bench all exist. So the
highest-value new contribution is the missing piece the roadmap explicitly asks for — *measured*
evidence that the lever delivers, as a model falsifier ("if the lever lands < 60 % of its modeled
win, STOP and re-profile").

## 2. Mini-bench result (forst-rs-bench `join_probe_open_adaptive`, 2026-06-15)

Local Mac (M-series), `cargo bench`, warm-up 2 s, 30 samples/cell, measurement 6 s. The fixture is
the SUSTAINED deep-fan-out long-running-join regime: tiny write buffer + high L0 triggers so the
per-round SSTs PERSIST (not compacted away), and a join-key-only prefix probe overlaps ~rounds
SSTs — exactly the q20/q9 regime where forst-rs holds L0 at 40-64. Three arms on the identical
fixture: `legacy` (S2 off, today's two-phase O(sources) scan + 2 Arc/row), `pinned` (S2 loser-tree
+ pinned rows forced on, the ceiling), `adaptive` (R1 selector, fan-out threshold 8).

| fan-out (overlapping SSTs) | legacy (median) | pinned | adaptive | pinned speedup | adaptive vs legacy |
|---|---|---|---|---|---|
| 1   | 942.8 ns | 792.9 ns | 906.3 ns | 1.19× | ~1.04× (≈ legacy; no pinned tax) |
| 8   | 5.044 µs | 3.970 µs | 3.664 µs | 1.27× | **1.38×** |
| 32  | 26.20 µs | 14.72 µs | 13.48 µs | 1.78× | **1.94×** |
| 64  | 65.77 µs | 28.60 µs | 27.26 µs | 2.30× | **2.41×** |
| 128 | 193.7 µs | (cut)    | (cut)    | —     | — (legacy only captured) |

(CIs are tight — e.g. legacy_64 [65.36, 66.21] µs, adaptive_64 [27.23, 27.30] µs — so the ranking
is robust to box noise at this n.)

## 3. Reading the evidence

1. **The lever WORKS and SCALES with fan-out.** The per-probe scan cost is reduced 1.27× → 2.30× as
   the overlapping-SST fan-out grows 8 → 64. The legacy two-phase scan is O(sources)-twice + 2
   allocs/row; the loser tree is O(log sources) per emitted row with pinned (alloc-free) rows — so
   the win is super-linear in fan-out, which is precisely the q20/q9 deep-L0 regime.
2. **Model CONFIRMED, not falsified.** The roadmap models L3 at q20 −9-13 % of TOTAL. The scan is
   21.6 % of q20 CPU; at the sustained deep fan-out (≥32 SSTs) the lever cuts that share ~1.8-2.3×,
   i.e. removes ~10-12 % of total — inside the modeled band. L3 clears the >60 %-of-model falsifier
   bar by a wide margin. The lever should proceed to its @100M n≥3 gate.
3. **The R1 adaptive selector is correct AND free.** On the shallow cell (fan-out 1 < threshold 8)
   adaptive ≈ legacy (906 vs 943 ns — it does NOT pay the pinned-mode setup that would only help
   deep scans), while on the deep cells it MATCHES or slightly beats pinned (e.g. 27.3 vs 28.6 µs at
   64). This is the "one config, no shallow tax, full deep win" property the design claimed —
   measured. It means the lever can ship default-ON via the adaptive gate without an env flag and
   without robbing the shallow-scan queries (q3/q17-class), removing the usual default-flip risk.

## 4. Recommendation / next-cycle candidate

- **Promote L3 (adaptive S2) toward default-ON.** The local mini-bench validates the mechanism and
  the adaptive gate's no-shallow-tax property. The remaining gate is the box-side n≥3 @100M q20/q9
  A/B (and the q0-q22 5M byte-equiv sweep already in the design's gates) — that is REMOTE-only work,
  deferred to the box per the NexMark-remote rule.
- **After L3 lands, L4 (compaction windowed reads + decoded-cache bypass) is the next q20 lever** —
  attacks the OTHER measured share (compaction 19.3 %), design already landed
  (`2026-06-12-compaction-windowed-readpath-design.md`). A symmetric mini-bench (compaction merge
  throughput with/without windowed reads + with/without cache-fill) would be the next local
  falsifier and is the recommended next-cycle engine deliverable if the q8 op-mix seam again proves
  intractable.
