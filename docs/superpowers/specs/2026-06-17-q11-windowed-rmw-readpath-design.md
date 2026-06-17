# q11 windowed-agg RMW read-path: profile, model, and read-path upgrade design

**PMC-1 Performance — 2026-06-17.** Profile-first (evidence before design). The
Phase-1 uniform-gate sweep found **q11 = ~2.0–2.65× RocksDB consistently**
(forst-rs 208.9 / 219.9 / 284 / 290 s vs rocksdb 107.0 s; ForSt-C++ 133.1 s) —
VM-INDEPENDENT, so a genuine engine read-path gap, not the VM-overcommit OOM
class that hits q9/q19. This doc establishes WHERE the gap is with isolated
engine microbenchmarks (no NexMark), builds the architectural model, and proposes
a uniform read-path upgrade.

---

## 0. q11 shape (ground truth)

```sql
INSERT INTO nexmark_q11
SELECT B.bidder, count(*) as bid_count,
       SESSION_START(B.dateTime, INTERVAL '10' SECOND),
       SESSION_END  (B.dateTime, INTERVAL '10' SECOND)
FROM bid B
GROUP BY B.bidder, SESSION(B.dateTime, INTERVAL '10' SECOND);
```

A **SESSION-window `count(*)` grouped by bidder.** Per bid record the Flink
windowed operator does, on the state backend:
- a `MergingWindowSet` mapping read/update (session windows MERGE as out-of-order
  events arrive — `MapState<Window,Window>`), and
- a per-window accumulator **get → +1 → put** (the `count(*)` long).

Both are **per-record SYNC read-modify-write** on the backend (Flink windowed
operators are synchronous; this is the structural `AbstractSliceSyncState` RMW the
memory notes name). The key space is millions of distinct (bidder, session)
keys → **scattered point access**, not a single hot key.

---

## 1. PROFILE — isolated engine microbenchmarks (the q11 read path)

All numbers are from this repo's own benches, built **with symbols**
(`CARGO_PROFILE_RELEASE_STRIP=none`) off `origin/forst-rs` tip
(`8ebcfc291`), run on the Mac VM. In-memory / local-FS only — box-safe, no
NexMark, no Docker.

### 1a. Accumulator RMW dispatch floor (`accumulator_rmw` + a distinct-key probe)

`crates/forst-rs-bench/examples/q11_rmw_probe.rs` (added for this profile,
removed after — see §6), 2M records over 4096 distinct keys (the q8/q11 shape):

| arm | ns/record | what it is |
|---|---|---|
| **A: get-fold-put, distinct keys** | **362.9** | **q11 today** — 2 engine ops + dependent get→put, in memtable |
| B: engine merge, distinct keys | 264.7 | 1 op, no dependent read |
| A2: get-fold-put, single hot key | 202.3 | q12/q17 hot-key shape (cache-warm) |
| **C: SST point-get, scattered, inline value** | **534.4 ns/get** | the **read half** once state has spilled to a compacted SST (1M distinct keys) |

Reading: in the steady state q11's per-record cost is dominated by the **read**
half of the RMW — a scattered inline point-get that costs **~534 ns** once the
working set has spilled out of the memtable into SSTs (and far more cold).

### 1b. The deref floor is IRRELEVANT to q11 (decisive)

`vlog_deref_latency` (200k records, **64 B** values = q11's 8-byte-class
accumulator shape, after subtracting framing):

| arm | uncompressed | lz4 |
|---|---|---|
| cold SEQ (scan locality) | 50.9 ns | 28.1 ns |
| random (point-get, 64 KiB chunk-fill `get`) | 2469.8 ns | 2428.1 ns |
| **random POINT (`get_point` fix)** | **342.4 ns** | 364.2 ns |
| hot HIT (cache-hit floor) | 20.1 ns | 23.1 ns |
| COALESCED (sort-by-offset) | 25.2 ns | 29.7 ns |
| ENGINE `batch_get_vectorized` coalesce OFF/ON @256 B local | 465 / 760 ns → **0.61× (coalesce HURTS local)** |
| SIM-S3 coalesce (200 µs RTT injected) | per-key 3874 GETs / 1058 ms → coalesced 1 GET / 2.2 ms = **484× (remote only)** |

**But q11's accumulator values are 8 bytes — far below `FRS_KV_MIN_BLOB_SIZE`
(default 256 B).** `flush.rs:392-396 separate_batch_values` only sends a value to
the value-log when `value.len() >= spec.min_blob_size`. So **q11's hot RMW values
are NEVER KV-separated; they stay inline in the SST KV blocks.** No vlog deref
fires on q11's hot path. The point-deref lever (342 ns vs 2470 ns) and the
coalesce lever do not touch q11 at all.

This **overturns the framing** that q11 is "windowed point-RMW under KV-sep +
point-deref auto-follow." The auto point-deref recovers q11/q17 only to the extent
those queries' *larger* state (e.g. the `MergingWindowSet` mapping payloads, if
they cross 256 B) is separated; the dominant **count accumulator path is inline**.

---

## 2. MODEL — why forst-rs is ~2× RocksDB on q11 specifically

q11's per-record cost = **(scattered inline point-get) + (RMW dispatch put) +
(window-merge mapping read/write) + (FFM crossing tax)**, all per record, all
synchronous. The gap vs RocksDB decomposes into three engine-side terms — none of
them KV-sep:

1. **Inline scattered point-get read-amp (dominant, ~534 ns measured).**
   A forst-rs SST point-get does: bloom probe → sparse-index search →
   `read_decoded_block` → **decode the whole data block** (v2 KV block;
   `sst/reader.rs:463`) → binary-search within the block for the key. For a
   *scattered* access pattern over millions of distinct session keys, the
   decoded-block cache (`reader.rs:476`, keyed by `(file_id, block_offset)`)
   **thrashes** — each probe pulls a different block, decodes it to extract one
   8-byte value, and evicts it before the next probe reuses it. RocksDB's point
   read is a mature C path: restart-interval binary search **directly on the raw
   block bytes** (no batch decode), a tuned LRU block cache, and per-SST bloom
   filters it has spent a decade tuning. RocksDB extracts the value with far less
   per-probe work and a warmer cache. **This is the same structural class as the
   q4 prefix-scan / q9 join-probe read-amp the memory notes call "LSM range-scan
   read-amp" — here in its POINT form.**

2. **The dependent get→put RMW (≈ +98 ns/record, Arm A 362.9 vs Arm B 264.7).**
   q11 issues TWO engine ops per record with a data dependency (read the
   accumulator, fold +1 in Java, write it back). The engine cannot pipeline the
   put behind the get; the put waits on the get's result. RocksDB pays the same
   logical 2 ops, so this is a *smaller* contributor to the *gap*, but it
   compounds with term 1 (the get is the expensive 534 ns scattered point-get,
   doubled in effect because the operator blocks on it).

3. **Per-record SYNC FFM crossing (structural, not measured here).** Every record
   crosses Java↔engine synchronously (windowed operators are not on the async/AEC
   batched path). The micro pays no crossing; production pays a fixed per-call FFM
   tax on top. This is a **Flink-structural floor** (see §5), shared with RocksDB
   in kind but heavier in forst-rs's FFM path than RocksDB's JNI bulk path.

**Verdict on the framing question:** the q11 gap is a **distinct inline read-path
gap (term 1, scattered point-get block-decode + cache thrash), NOT the KV-sep
scattered-point deref floor.** The KV-sep/point-deref/coalesce machinery is
orthogonal to q11's hot path. q11 belongs with q17 (same windowed-RMW shape — q17
is even worse, 217–333 s vs rocksdb 73.9 s) and shares term 1 with the join/scan
read-amp family.

---

## 3. DESIGN — uniform read-path upgrade (NOT yet implemented)

Goal: cut term 1 (scattered inline point-get) and term 2 (dependent RMW) under the
**one uniform config**, with no per-query branch and no Peter-for-Paul regression.
Hard constraints honored: E2E vectorization, zero-copy, batch-only, Arrow, no
per-record `byte[]`.

### Lever R-1 (primary) — restart-interval point-seek WITHOUT full block decode

**Problem:** `read_decoded_block` decodes the entire KV/Arrow data block to a
materialized structure before the binary search, even for a single-key point-get.
For scattered point-gets this is the dominant cost and the reason the decoded-block
cache thrashes (it caches whole decoded blocks that are used once).

**Design:** add a `point_lookup_in_block(raw_block, key) -> Option<ValueRange>`
that binary-searches the **raw (decompressed-only) block bytes** using the v2 KV
block's restart array, returning a zero-copy `(offset,len)` range into the block
buffer for the one matching value — never building the full decoded row set. The
SST point-get path (`sst_get_resolve`) calls this instead of
`read_decoded_block` + `search_key_in_batch`. Caching shifts from "decoded blocks"
(thrash) to "raw decompressed blocks" (smaller, and the LRU holds more of them →
higher hit rate for scattered keys). This is exactly RocksDB's point-read shape
and removes the per-probe Arrow/KV materialization. **Uniform & safe:** point-gets
exist in every stateful query; range scans keep the existing decoded path
(value-carrying scan already shipped). Estimated: 534 ns → ~250–300 ns (the
block-decode is roughly half of ARM C), i.e. **~1.8–2.1× on the read half**.

### Lever R-2 (secondary) — batched windowed-RMW via the AEC path

**Problem:** q11's RMW is per-record SYNC (term 2 + the crossing tax in term 3).

**Design:** route the windowed accumulator state through the **batched async path**
the join/AEC queries already use, so a *batch* of records' get-fold-put collapses
to one `batch_get_vectorized` (already coalesces by SST) for the reads + one
batched put. The folds stay in Java but are applied over the vectorized read
result, removing the per-record dependent stall and amortizing the FFM crossing
over N records. This is a **Flink-side executor change** (the windowed operator
must present its RMWs as a batch) and is the larger lift; it is gated behind
confirming the operator can buffer without breaking event-time/window-merge
semantics. **Uniform:** the same batched read path already serves q4/q7/q9/q20;
extending windowed operators onto it is additive. Estimated: removes the ~98 ns
dependent-RMW penalty AND the per-record crossing → the larger production win, but
structurally bounded (§5).

### Lever R-3 (engine-merge for count, BANKED — do NOT fold into uniform yet)

ARM B shows engine `Merge` (submit only the +1 delta, fold at read/compaction)
is 264.7 vs 362.9 ns — **~27% cheaper** by eliminating the dependent read. This is
the `FRS_RS_MERGE_RMW` lever, deliberately EXCLUDED from the uniform config
(best-config.tsv) because (a) it was never A/B-confirmed harmless across shapes and
(b) the read-cost guard (`accumulator_read_cost`) shows an un-compacted merge chain
walks O(K) operands — a q20-class regression risk between compactions. R-3 stays
banked behind R-1: once R-1 makes the *read* cheap, the dependent-read penalty R-3
removes is a smaller fraction, lowering the incentive and the risk-reward.

**Recommended order:** R-1 first (pure engine, uniform, no Flink change, no
semantic risk), measure, then R-2 if the gap persists.

---

## 4. Estimated win & honest ceiling

- **R-1 alone:** read half 534 → ~270 ns ⇒ q11 ≈ 2.0× → **~1.4–1.5×** (the read is
  the dominant per-record term; the rest is RMW dispatch + window-merge + crossing).
- **R-1 + R-2:** removes the dependent-stall and crossing amortization ⇒ plausibly
  **~1.2–1.3×**, approaching the ForSt-C++ point (133 s, 1.24× rocksdb) which is the
  realistic engine-parity target for this shape.
- **≤1.25× in the engine read path alone is NOT achievable** — part of q11 is a
  **Flink/structural floor**: the windowed operator's per-record SYNC RMW + the
  `MergingWindowSet` per-record mapping read/update + the FFM crossing are imposed
  by Flink's synchronous windowed-operator model, not the engine. RocksDB pays the
  same Flink structure but through a more mature point-read + JNI path; ForSt-C++ at
  1.24× is the honest floor a Rust engine of equal maturity reaches. **R-1 gets most
  of the way (≈1.4–1.5×); closing to ForSt-C++ parity (~1.25×) needs R-2 (the
  batched-RMW Flink-side change), and beating ForSt-C++ on this shape is unlikely
  because the residual is the shared Flink windowed-RMW structure.**

---

## 5. Cross-check vs prior investigations

- Memory: *"q11 = separate read-path gap"* — **CONFIRMED.** Distinct from the q4/q7
  WAL/KV-sep levers; it is the inline scattered point-get read-amp.
- Memory: *"value-carrying RANGE scan WIN"* (q11 529→216 s) — that fix addressed
  q11's MapState/window-set **range** drain (the `prefix_scan` value-carrying path,
  already shipped). The **residual 2× is the POINT-get RMW**, a different code path
  (`sst_get_resolve` / `read_decoded_block`), which R-1 targets. The two are
  complementary, not overlapping.
- Memory: *"windowed-agg SYNC-state RMW (AbstractSliceSyncState, Flink
  structural)"* — **CONFIRMED** as the §5 floor; R-2 is the only lever against it
  and it is a Flink-side change.

---

## 6. Evidence reproduction & cleanup

- Benches (this repo, no NexMark): `vlog_deref_latency`
  (`--records 200000 --value-size 64`) and `accumulator_rmw`; the distinct-key /
  SST-resident probe `q11_rmw_probe.rs` was added under
  `crates/forst-rs-bench/examples/` for this profile and **removed after**
  (numbers captured above). Build with `CARGO_PROFILE_RELEASE_STRIP=none` for
  symbolized frames.
- The full-stack q11 number is the uniform sweep
  (`tools/nexmark-local/pmc1-uniform-results.tsv`): q11 forst-rs 208.9 / 219.9 s,
  rocksdb 107.0 s, ForSt-C++ 133.1 s.
- No Docker / NexMark run was needed for this profile (box-safety: the Mac VM is
  fragile at 2×16 g; all evidence here is single-process in-memory/local-FS).
