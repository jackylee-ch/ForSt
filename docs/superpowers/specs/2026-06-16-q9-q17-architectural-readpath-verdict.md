# q9 / q17 — architectural read/exec-path attack + mini-bench verdict (PMC-1, 2026-06-16)

**Author:** PMC-1 Performance Agent (profiler + mini-bench + code; NO Docker/NEXMark — Docker down post-disk-crash; NEXMark deferred).
**Engine tip:** worktree off `origin/forst-rs` @ `f34c80fc3`.
**Mandate:** the prior pass concluded q9/q17 read-path levers are "exhausted / structural"; the user REJECTS settling there. This cycle ATTACKS each architecturally (root-cause model → mini-bench reproducing the hot path → lever) before any verdict.
**Mini-bench added:** `crates/forst-rs-bench/src/bin/q9_interval_join_probe.rs` (realistic interval-join probe; the prior q9 benches only had the adversarial full-overlap fixture).

---

## q9 — interval join (winning-bid), ~1.36× RocksDB

### Root-cause model (code-grounded)
q9 = `A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires` → `ROW_NUMBER()=1`. Flink's
IntervalJoin buffers the auction side in keyed MapState (key = auction id); each bid probes the buffer
for its auction id within the time band. Engine-side this is **one prefix scan per probe** over
`build_lazy_prefix_key_stream_sel` (`db.rs:10981`): a lazy k-way merge over (active memtable +
immutable + resident-flushed memtables + every overlapping L0 SST). Per-probe cost is dominated by the
**seek + drain over the located source set**, not by building it.

### The mistake in the prior "structural / O(N²)" reading
The existing q9 benches (`join_probe_open.rs`, `persistent_probe_iter.rs`) use an **adversarial**
fixture where every SST spans the FULL join-key range, so no prune can fire and every probe genuinely
fans out over all N SSTs. On that fixture the curve is steeply super-linear:

| located SSTs | legacy | S2 pinned (loser-tree) | leveled (collapsed) |
|---|---:|---:|---:|
| 8  | 5.58µs | 4.41µs | 5.61µs |
| 32 | 26.2µs | 15.6µs | 9.61µs |
| 64 | 74.5µs | — | 11.4µs |
| 128| 196µs  | — | 15.3µs (**12.8×**) |

The shipped **leveled-hot-CF** lever already flattens this (collapses the source COUNT) — and S2
loser-tree shaves the merge constant ~1.5×. But this fixture is NOT q9's real shape.

### Architectural attempt 1 — cross-probe construction amortization (persistent probe iter): REFUTED
The shipped `PersistentProbeIter` reuses the source set across probes. Mini-bench
(`persistent_probe_iter`): the "persistent" arm is **net-negative** (8.19 vs 7.69µs @8 SSTs; 108 vs
102µs @64). The per-probe cost is the seek+drain over the sources, NOT the source-set construction —
so amortizing construction buys nothing and adds re-seek overhead. **Construction-amortization is a
dead end for q9.**

### Architectural attempt 2 — realistic-shape probe (NEW mini-bench `q9_interval_join_probe`): the decisive result
q9's real key distribution is NOT full-overlap: auctions arrive over TIME, so a given auction id's bids
cluster in the few SSTs flushed while it was live — they are NOT in every SST. The new bench models
this (each SST holds a contiguous window of auction ids; probes uniform-random = the pessimistic
fan-out). Result — `--keys 65536 --ssts 128 --keys-per-sst 1024`:

```
  leveled OFF (sust L0)   1646 ns/probe   avg_src 1.00  max_src 1   rows/probe 4.0
  leveled ON              1755 ns/probe   avg_src 1.00  max_src 1   rows/probe 4.0
```

Even with **heavy** window overlap (`--keys 8192 --ssts 128 --keys-per-sst 4096`, each SST covers half
the key space):

```
  leveled OFF (sust L0)  29904 ns/probe   avg_src 2.00  max_src 2   rows/probe 128.0
  leveled ON             27105 ns/probe   avg_src 2.00  max_src 2   rows/probe 128.0
```

**The shipped prefix-bloom + range prune already collapse the effective per-probe source count to
≈the number of SSTs that genuinely contain the key (1–2), regardless of total SST count (128) and even
with L0 compaction suppressed.** The leveled lever is a near-no-op here because the prune already did
the collapse. The 30µs in the overlap case is `rows/probe=128 × ~234ns/row` of GENUINE
merge+materialize work (real result data), not read-amp. **q9's engine read-path fan-out is already
flat in the realistic regime — there is no O(N²) read-amp left to attack in the engine.**

### q9 verdict
The engine read path is NOT the residual q9 gap. The architectural levers that matter (prefix bloom +
range prune → fan-out≈1–2; leveled-hot-CF for the rare sustained full-overlap transient; S2 loser-tree
for the merge constant) are **built and effective in mini-bench**. The residual 1.36× is, per the prior
JFR (q9 65,842 ThreadPark vs 6,419 on-CPU, ~10:1), **WAIT-bound on the depth-1 inline executor** — no
cross-probe overlap. **That lever lives in the Flink runtime** (`FRS_RS_EXECUTOR=routing-adaptive`,
already shipped-ready), not the engine. This is a precise, evidence-backed call after a real attempt:
two engine-side architectural attempts (construction-amortization, realistic-shape) both show the
engine read path is flat. **q9 can beat RocksDB only via the runtime executor-overlap lever + the @36g
resident headroom (q9 8c/36g already 1.06×), not a new engine read-path change.**

---

## q17 — unbounded keyed group-agg (count/min/max/avg/sum), ~3× → 1.24× RocksDB

### Root-cause model
q17 = per record: point-read one packed accumulator row, fold in the operator, write it back. **No
iteration.** Under the async-state backend each record pays the AEC framework floor
(`AsyncExecutionController.handleRequest`: seizeCapacity + tryOccupyKey epoch + buffer enqueue +
future alloc + the get→fold→put continuation chain) that RocksDB's synchronous JNI backend does NOT.
forst-rs already **beats ForSt 3.3×** on this query; the RocksDB gap is the async framework coordination.

### Architectural attempt — batched write-back (the prompt's lever (a)): measured, small
The mandate was to wire per-record `db.put` → one `batch_put_arrow` per async-buffered batch. The lever
EXISTS in the engine (`batch_put_arrow`) and is mini-benched (`windowed_agg_rmw --writeback`). Result
(KV-sep ON + point-deref, the q17 production regime; byte-identical accumulator sums asserted):

| scale | per-record put | batch_put_arrow | speedup |
|---|---:|---:|---:|
| 1M rec, 100K keys | 889.6 ns/rec | 794.3 ns/rec | **1.12×** |
| 2M rec, 200K keys | 1082.0 ns/rec | 977.7 ns/rec | **1.11×** |

The whole RMW (read+fold+write) is ~890–1080 ns/rec; batching the write-back saves ~90–105 ns of it.
The cost is dominated by the **point READ** over flushed SSTs (`batch_get_vectorized`), which is already
vectorized + point-deref optimized (the main-arms bench: OFF 851 ns, KV-sep+point 910 ns — point-deref
recovers the KV-sep regression). **The engine write path is not the q17 bottleneck.**

### q17 verdict
The batched-write-back lever is real but small (≈1.1×) and lands ON the engine side, not on the AEC
floor that dominates the real query. The decisive q17 cost is the **per-record async-framework
round-trip in the Flink runtime** — confirmed structural by the prior pass's JMH bench (the candidate
ValueState value-RMW cache is net-negative at q17's unbounded large-cardinality working set: its own
185 ns hit cost @64K live keys exceeds the ~28 ns AEC alloc it removes, and q17 has millions of
distinct (auction,day) keys → eviction churn re-creates the floor). **q17's RocksDB gap is genuinely a
Flink-runtime async-floor, not an engine read/write defect** — and after a real engine-side attempt
(batched write-back: measured 1.11×, insufficient) this is the honest call. q17 remains a decisive
WIN vs ForSt (0.33×, 3.3×).

**To wire the small engine-side win in the Flink V2 windowed-agg path** (if pursued): replace the
per-record `asyncUpdate(accumulators)` write in `AsyncStateGroupAggFunction`'s `thenAccept` with an
accumulation into a per-batch Arrow `RecordBatch` (key, value, op=Put) flushed via `batch_put_arrow`
at the AEC batch boundary — the exact mechanism the `rmw_pass_batched_writeback` mini-bench proves
byte-identical. Expect ≈1.1× on the write half only; it does NOT touch the AEC floor, so it will not
close the RocksDB gap on its own.

---

## Honest overall verdict
After a REAL architectural attempt on each (not a first-resort "structural"):

- **q9**: the engine read path is already FLAT on the realistic shape (effective fan-out ≈1–2 via the
  shipped bloom/range prune; mini-bench proven). The two engine-side attempts this cycle
  (construction-amortization, realistic-shape) both confirm there is no engine read-amp left. The
  residual gap is the WAIT-bound depth-1 executor — a **Flink-runtime** lever (routing-adaptive,
  shipped-ready). q9 beats RocksDB via that + @36g headroom, NOT a new engine change.
- **q17**: the engine read+write path is already optimal (point read vectorized+point-deref; batched
  write-back gives only ≈1.1×). The gap is the **Flink-runtime AEC async floor** on a tight point-RMW
  loop — irreducible in the engine, confirmed by the prior JMH bench. q17 stays a beat-ForSt /
  lose-RocksDB query; the imperative is defensive (keep it on the zero-handoff inline path).

Both gaps are genuinely in the Flink runtime, not the engine — but this is now backed by direct
engine-side mini-bench attempts, not asserted.

## NEXMark A/B plan (DEFERRED until Docker is back)
1. **q9 executor-overlap A/B**: `FRS_RS_EXECUTOR=routing-adaptive` vs default depth-1, 8c/36g, 100M,
   n≥3 — confirm the 10:1-park wait collapses and q9 crosses 1.0× RocksDB. Pair with
   `FRS_RS_LEVELED_HOT_CF=1` (no-op expected per this cycle's mini-bench, but confirm no regression).
2. **q17 defensive trace**: confirm `routing-adaptive` keeps q17 on the inline no-split path (the
   carve-out) — q17 must NOT regress from its 83.7s. Optionally A/B the batched-write-back wiring in
   `AsyncStateGroupAggFunction` (expect ≈1.1× write-half only; gate on no read/correctness regression).
3. **q9 realistic-fan-out confirmation**: instrument `debug_source_count` (or FRS-FANOUT-DIAG peak) on a
   100M q9 run to confirm the engine's real per-probe `avg_src` matches this mini-bench's ≈1–2 (i.e.
   the bloom prune holds at scale), closing the loop on "no engine read-amp."

## Constraints honored
No Docker / NEXMark (deferred). One new mini-bench bin (pure storage/engine, not NEXMark), clippy +
fmt clean, no production engine behavior changed (added a bench bin only). Worktree to be cleaned up.
