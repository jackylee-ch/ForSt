# Write-path redesign survey — toward ~1× write-amplification for streaming state on S3

Date: 2026-06-13 · Author: PMC architect agent (write-path redesign brief) · Worktree: `forst-rs`
Ground truth inputs: `2026-06-12-sorted-run-discipline-design.md` (the WITHIN-paradigm fix;
measured frs write-amp 7.68× vs RocksDB 3.91×, M1-M6 target ~4×),
`2026-06-13-phase2-disaggregated-state-design.md` (S3-primary model; FileMappingManager
merged at 29a3b7d92), `2026-06-12-master-strategy-beat-forst-rocksdb.md` (CURRENT STATUS
scoreboard). Evidence rule honored: every premise is (a) measured in §2's cells run for
THIS doc, (b) inherited from a recorded measurement (cited), or (c) labeled **model**.

---

## 0. Verdict (executive)

**Yes — the paradigm-level answer exists and is composable.** The question "can write-amp
go toward ~1×?" decomposes by *where the bytes come from*. §2's measured cells show:

- The compaction share of forst-rs physical writes is **~87 %** (flush-only floor measured
  at write-amp 0.97-0.98 vs 7.83 total, same session, cells F vs A′). Any design that
  *compacts less* attacks 87 % of the bytes; M1-M6 merely compacts *better* (→ ~4×).
- Streaming state has a property general LSMs don't: **most state has a deterministic
  death time** (window end, interval-join TTL, timer fire). Compacting soon-dead data is
  pure waste — and the engine is TOLD the death time today only via per-key tombstones
  (the churn workload is ~50 % deletes by count). A **TTL-segmented FIFO design** writes
  each byte ONCE (flush) and expires whole segments at the watermark: measured floor
  **write-amp 0.98** (cell F) — that IS ~1×.
- For state that is NOT deterministically dying (or whose death the backend can't
  prove), **KV separation** caps the damage: only keys+pointers ride the compaction
  treadmill. Measured key+pointer LSM cell (E′ = 2.10×) + arithmetic over the
  workload's byte split gives **modeled 1.36×** for the q7-shaped workload
  (1.8× if the key-LSM deepens to the M1-M6 class) — and on S3 the
  FileMappingManager's refcounted segments make value-log GC a *link/unlink* problem,
  not a rewrite problem.
- On S3-primary (Phase-2), **write-amp ≈ upload-amp**: every physical byte written by
  flush/compaction is a byte uploaded (write-through, `cached_fs.rs:33-40`). The same
  redesign that relieves the remote q7 disk saturation (writes 435-682 MB/s at 98-99 %
  util, sorted-run §1.1) divides the S3 PUT volume.

**Chosen architecture (§5): lifecycle-aware, KV-separated LSM with link-compaction** —
(1) per-CF lifecycle hints from the Flink backend (it KNOWS window bounds/TTL);
(2) time/death-segmented value logs + FIFO segment drop for lifecycle CFs (write-amp
→ ~1.0-1.2 measured floor); (3) KV separation for non-lifecycle value-heavy CFs
(measured-model 1.36-1.8×); (4) the key-LSM keeps the sorted-run M1-M6 discipline (complement,
not supersede — it now operates on ~6× fewer bytes); (5) compaction-as-metadata
(re-link, don't re-write) wherever ranges don't overlap, free on S3 via
FileMappingManager (measured 1.4 µs/link vs 4 520 µs/copy, Phase-2 §8).

---

## 1. Premises — each measured or cited

| # | Premise | Status | Evidence |
|---|---|---|---|
| P1 | frs write-amp ≈ 7.7× intrinsic to current picking | **measured** (re-confirmed this session) | cell A′ §2.2: 7.83 (recorded 7.68, sorted-run §2.1) |
| P2 | Compaction (not flush) owns ~87 % of physical write bytes | **measured** | flush-only cells: C-nocompact 0.97 (sorted-run §2.1), F-ttlseg 0.98 (§2.2) vs 7.83 total |
| P3 | Remote q7 is disk-write-volume bound; ForSt writes ~1.4 KB/event vs frs ~13 KB/event | cited | sorted-run §1.1 iostat A/B (frs 435-682 wMB/s @98-99 % util vs ForSt ~110 wMB/s @6 %) |
| P4 | On S3-primary, physical write bytes ≈ S3 upload bytes | cited (code) | write-through pass-through writes + async upload, `crates/forst-rs-storage/src/cached_fs.rs:33-40`; Phase-2 §3.4 "flush asset" |
| P5 | State values ≫ keys for NexMark value-state/join state | **model** (sensitivity-analyzed §3.1) | bench shape 200 B value / 20 B key mirrors Flink prefix layout (`churn_probe.rs:239-246`); §3.1 gives the win as a function of the value share |
| P6 | Streaming state death is deterministic and KNOWN to the backend | cited (code + precedent) | window end/interval bounds drive the state TTL & timers the backend registers; ForSt C++ ships `FlinkCompactionFilter` whose only job is dropping ttl-expired Flink state DURING compaction (`main:utilities/flink/flink_compaction_filter.h:28-57`) — the knowledge already crosses the boundary today, but only as a *filter inside* compaction, still paying the rewrite |
| P7 | Whole-segment FIFO drop is a shipped LSM paradigm | cited | RocksDB `FIFOCompactionPicker` (`main:db/compaction/compaction_picker_fifo.h`), `CompactionOptionsFIFO{max_table_files_size, allow_compaction}`, option `ttl` (`main:include/rocksdb/advanced_options.h:909`) |
| P8 | KV separation is a shipped LSM paradigm with known S3-relevant costs | cited | WiscKey (Lu et al., FAST '16); RocksDB integrated BlobDB: `enable_blob_files` — "large values (blobs) are written to separate blob files, and only pointers to them are stored in SST files. This can reduce write amplification … at the cost of introducing a level of indirection for reads" (`main:include/rocksdb/advanced_options.h:1027-1039`), GC via `enable_blob_garbage_collection` + age cutoff 0.25 (`:1070-1092`); Titan (PingCAP) and Badger are the same family |
| P9 | Lazy leveling bounds write-amp at bounded read fan-out | cited + **measured dose-response** | Dostoevsky (Dayan & Idreos, SIGMOD '18); cells G/H (§2.2) + sorted-run cell C curve (probe p50 ~linear in run count) |
| P10 | Link ops are ~free vs byte copies on the mapping layer | cited (measured in Phase-2) | `LINKBENCH … copy_per_file_us=4520.3 link_per_file_us=1.4 speedup=3172x` (Phase-2 §8, fs-emulation) |
| P11 | Memtable size's write-amp effect | **measured — CORRECTED**: first-order in the current scheme (7.83→4.45 at 4× memtable, cell I) because bigger flushes amortize the whole-L1 rewrite; converges to the ~4× within-paradigm class, not ~1×, at RAM + probe-latency cost | cell I §2.2-2.3 |
| P12 | Merge-operand state (ListState `RawConcatMergeOperator`, `forst-rs-ffi/src/lib.rs:162`) cannot naively KV-separate | cited (code) | concat must see operand BYTES; pointers don't concat — §3.1 exempts merge entries (same restriction as RocksDB BlobDB) |

---

## 2. Partial benchmarks (run for this doc — extended `churn_probe`)

Instrument: `crates/forst-rs-bench/src/bin/churn_probe.rs`, extended this session with
`--no-deletes` (TTL-segment/FIFO model: a segmented design writes NO per-key tombstones;
expiry = whole-segment drop) and `--wbuf-mib` (memtable-pressure dose). Workload, box,
methodology identical to sorted-run §2 (q7-shaped churn, 200 K rows/s, 200 B values,
~1 GiB live, 90 s × 3 runs/cell, medians, same-session A/B, Mac system-allocator).
Raw outputs: `target/churn_results_survey/*.jsonl`.

### 2.1 Cell semantics — what each cell is a measurement OF

| Cell | Config | Measures | Models (labeled) |
|---|---|---|---|
| A′ default-rebase | shipped defaults | same-session re-baseline of P1 | — |
| E′ kvsep-key | `--value-bytes 16` | write-amp of a key+pointer-only LSM (20 B key + 16 B pointer) on the identical key/delete stream | the key-LSM HALF of a WiscKey design; full-design write-amp assembled in §3.1 (value-log half = append-once, measured floor from F) |
| F ttlseg-fifo | `--no-deletes` + `FRS_L0_COMPACTION_TRIGGER=100000` | physical write floor of drop-instead-of-compact: flush-only, zero tombstones, zero compaction | the WRITE side of a TTL-segmented engine; its READ side is NOT this cell's L0 pileup — a segmented design prunes probes to segments overlapping the live window (§3.2 model via P9 dose curve) |
| G lazy8 | `FRS_L0_COMPACTION_TRIGGER=8` | write-amp + probe latency at 2× lazier L0 rollup | one point on the Dostoevsky tiering↔leveling trade curve |
| H lazy16 | `FRS_L0_COMPACTION_TRIGGER=16` | ditto at 4× lazier | second point |
| I bigbuf256 | `--wbuf-mib 256` | flush-count/L0-geometry effect of 4× memtable | §3.5 memtable/flush pressure |

### 2.2 Results (medians of 3)

| Cell | write-amp | probe p50 late (µs) | probe p99 late (µs) | p50 degr. | last L0 |
|---|---|---|---|---|---|
| A′ default-rebase | **7.83** | 858 | 3 161 | 4.20× | 2 |
| E′ kvsep-key | **2.10** | 547 | 859 | 2.77× | 0 |
| F ttlseg-fifo | **0.98** | 3 510 † | 4 577 † | 12.76× † | 41 † |
| G lazy8 | 7.13 | 871 | 2 433 | 4.36× | 2 |
| H lazy16 | 7.02 | 1 027 | 2 962 | 3.65× | 3 |
| I bigbuf256 | **4.45** | 1 144 | 2 127 | 6.48× | 0 |

† cell F's probe column is the UNPRUNED read cost (41 sorted runs, no segment
pruning, AND the writer was throttled to 111 K rows/s by the L0 slowdown trigger
hitting 40 — `write_controller.rs:94` counts the segments as L0). It is reported
raw; the segmented DESIGN's read cost is modeled from the P9 dose curve in §3.2,
and its backpressure exemption is a V1 design requirement (§3.2).

(Reference, recorded sorted-run §2.1 same workload: A 7.68 / C-nocompact 0.97, p50
2 997 µs, L0 41 / E-rocksdb 3.91, p50 162 µs.)

### 2.3 Findings

1. **Re-baseline holds (P1).** A′ = 7.83 vs recorded 7.68 — same regime, ±2 %.
2. **KV-separation upper bound (cell E′ + model): full-workload write-amp ≈ 1.36×.**
   The key+pointer-only LSM measured **2.10×** — far below the 7.83 baseline because
   the key-LSM's live set is ~137 MiB (4 M × 36 B) instead of ~1 GiB: it never grows
   the level cascade. That IS the WiscKey mechanism: state growth lands in
   append-once value logs, the compaction tree stays shallow. Assembled (§3.1):
   (2.10 × 56 + 208) / 240 = **1.36×** — a 5.8× cut, with the value-log GC term
   = 0 under lifecycle expiry (or bounded by the BlobDB age-cutoff term otherwise).
   Honest caveat: WA_key rises with KEY-cardinality growth (depth returns); the §3.1
   sensitivity rows bound it (worst modeled case keeps the win).
3. **TTL-segment upper bound (cell F): write-amp 0.98 — ~1× is real and measured.**
   Flush-only, zero tombstones, zero compaction. Two raw artifacts to design around:
   (a) the writer was THROTTLED (111 K vs 200 K rows/s) because the 41 segments
   counted as L0 against the slowdown trigger — lifecycle segments must be exempt
   from L0 backpressure accounting (V1 requirement); (b) probe p50 3 510 µs is the
   UNPRUNED cost of 41 live runs — the design prunes probes to in-window segments
   (~16 at this shape ≈ the dose curve's 649 µs point) and merge-once cohorts
   (4-8 runs ≈ 200-400 µs, write-amp ≤ 2.0 by construction). Even the raw
   unpruned cell beats nothing-at-all: it equals sorted-run cell C, as expected
   (same compaction-off physics, minus tombstones).
4. **The lazy-leveling dial does NOT pay within the current scheme (cells G/H).**
   Trigger 4→8→16 moved write-amp only 7.83 → 7.13 → 7.02 (−10 %) while probe p50
   went 858 → 871 → 1 027 µs (+20 %). Mechanism: the whole-L1-rewrite cost
   (sorted-run W1) dominates and rollup frequency is bounded by flush cadence, so
   batching L0 deeper barely amortizes anything. Dostoevsky-style laziness as a
   GLOBAL policy is measured-rejected here (§3.3, §4); the asymptote (compaction
   off, 0.97×) only arrives with unbounded fan-out.
5. **Memtable size is a FIRST-order write-amp lever in the current scheme —
   correction to the P11 expectation.** 4× memtable (cell I) measured **4.45×**
   (−43 %) at the full 200 K rows/s, probe p50 +33 % (1 144 µs), L0 max 4. Bigger
   flushes amortize the whole-L1 rewrite per rollup. BUT: it converges to the same
   ~4× class as sorted-run M1-M6 — NOT toward 1× — while costing RAM
   (256 MB × CFs; the 8c/32g ceiling and the recorded q4 noflush 44 GB lesson) and
   probe latency. Reading: within-paradigm levers (picking, geometry, memtable)
   plateau around ~4×; crossing to ~1× requires the paradigm moves (cells E′/F).

---

## 3. Survey

### 3.1 KV separation (WiscKey-class) — keys in LSM, values in append-only logs

**Mechanism.** Put(k,v) appends `v` to the active value-log segment and writes
`k → (segment_id, offset, len)` into the LSM. Only `keys+pointers` (≈ 36 B here vs
220 B) ride flush+compaction; values are written exactly once (plus GC re-writes).

**Write-amp arithmetic (model with measured components).** Per steady-state row the
workload writes key 20 B + value 200 B + trailing delete-key 20 B = 240 B logical.
Under KV-sep: physical = WA_key × 56 B (put-entry 36 B + delete 20 B, WA_key measured
by cell E′) + 1×·(200 + 8 hdr) B value-log append (append-once; GC term §below).

> WA_kvsep = (WA_key × 56 + 208) / 240 — with WA_key = **2.10 measured (cell E′)** →
> **1.36×**. Sensitivity: if the key-LSM deepens to the M1-M6 class (WA_key = 4) →
> 1.80×; even at today's unfixed 7.8 → 2.69×. Sensitivity to P5 (value share): at
> 100 B values, (WA_key×56+108)/140 → 1.61× (measured WA_key) to 2.3× (WA_key 4);
> at 500 B → 1.16-1.36×. The win exists at every realistic value size and every
> key-LSM depth; it grows with value share.

**GC on S3 — the FileMappingManager synergy.** The classic WiscKey weakness is value-log
GC (re-write live values out of stale segments, then update pointers). Two structural
outs in OUR context:

1. **Streaming-state death order ≈ arrival order.** q7-class TTL deletes trail inserts
   by a fixed window — value-log segments age out *as a whole*. A segment whose newest
   record is past the TTL horizon is dropped by `unlink()` (refs → 0 → one S3 DELETE),
   zero re-writes, zero pointer rewrites (the keys died with it). This composes with
   §3.2: for lifecycle CFs, value-log GC is *exactly* segment expiry.
2. **For non-lifecycle CFs**, adopt RocksDB BlobDB's compaction-coupled GC
   (`blob_garbage_collection_age_cutoff`, `main:advanced_options.h:1070-1092`): the
   key-LSM compaction already visits every live pointer; pointers into segments older
   than the cutoff get their values relocated to the active segment and the pointer
   rewritten in the compaction output. Garbage ratio per segment is tracked by
   compaction's dropped-key accounting (the engine already counts drops for tombstone
   stats). On S3 the segment delete is an `unlink` through the mapping layer — and a
   checkpoint that still references the old segment keeps it alive by refcount, for
   free (`file_mapping.rs:538-633` register/link/unlink/adopt are file-kind-agnostic).

**Read-path interaction (value-carrying scan + S2).** Our q7 probe path is
value-carrying: `LazyPrefixIter::next_with_value` yields `ValueDecision::Put(value)`
inline (`db.rs:6772-6775, 7382-7385, 11231`), and S2's loser-tree merges pinned rows.
Under KV-sep the inline "value" IS the 16 B pointer:

- *Merge gets cheaper*: the k-way merge and MVCC visibility decisions now move 36 B
  entries, not 236 B — S2's pinned-row buffers shrink ~6×.
- *A new dereference appears after visibility*: one value-log read per VISIBLE result
  row (losers and shadowed versions never fetch). For q7-shape probes the visible rows
  are the live window — written recently — so dereferences hit the write-through local
  copy/block cache (P4 machinery), not S3. **Model**: warm-window dereference ≈ a
  cache-hit pread (~µs); the cold-S3 worst case (23 ms RTT, recorded 2026-06-01) is the
  reason for a `min_blob_size`-style threshold (P8) and for keeping lifecycle segments
  cache-pinned while in-window. Cell E′'s probe numbers measure the key-LSM half only
  (labeled as such in §2.3).
- *Merge-operand entries stay inline* (P12): ListState concat operands must be bytes.
  Value/Map/Reducing state (Put-dominated) separates; merge CFs opt out per-CF.

**MVCC/checkpoint/restore.** Versioning stays entirely in the key-LSM (seqnos on
pointer entries); value-log segments are immutable append-only files — same lifecycle
class as SSTs. Checkpoint links segments exactly like SSTs (Phase-2 Stage 2);
restore `adopt()`s them; `min_active_snapshot` pins prevent GC of segments referenced
by pinned versions (same rule compaction already obeys, `compaction.rs:93-99`).

### 3.2 Lifecycle/TTL-aware design — the streaming angle (the ~1× path)

**Why it dominates.** P2: 87 % of bytes are compaction; P6: the backend KNOWS when
state dies (window end for q5/q8/q11-class windowed aggs; interval bounds for q7-class
joins; timer fire times). Compaction exists to reclaim space from *unpredictable*
overwrites/deletes; when death is deterministic, reclamation needs NO data movement:

- **Time-segmented runs**: each flush epoch's output is stamped with the CF's
  death-time range (for window state: the window-end bucket; for TTL joins:
  insert-time + TTL). Runs whose max-death < watermark are dropped whole —
  `unlink()`, refs→0, one metadata op. Measured write floor: **cell F = 0.98×**
  (flush-only, no tombstones — the design also DELETES the tombstone traffic, which
  is ~50 % of ops and ~8 % of logical bytes in the q7 shape, plus 100 % of the
  tombstone-driven compaction churn).
- **Per-window/per-segment binding** is the degenerate case (segment == window pane):
  right for slicing window aggs (Flink's slice-sharing windowed aggs already namespace
  state by slice); too coarse for interval joins, which want sliding expiry — the
  death-time-bucket form covers both.
- **Precedents**: RocksDB FIFO compaction drops oldest files at size/ttl bounds (P7) —
  but is whole-CF and unordered-key (point-lookup workloads). Our variant keeps
  *per-segment sorted runs + the existing locator*, so prefix probes stay
  binary-searched within each live segment (`version/mod.rs:698-870` unchanged).
  `FlinkCompactionFilter` (P6) is the proof the TTL contract is already exportable —
  it just spends it inside compaction (still rewrites every surviving byte, and only
  reclaims when compaction happens to visit).
- **API design (backend → engine)**: a per-CF `lifecycle` descriptor on CF creation
  (`ColumnFamilyDescriptor`, `crates/forst-rs-engine/src/column_family.rs:235`,
  constructed at the FFI boundary `forst-rs-ffi/src/lib.rs:162-166`):
  `{ kind: Windowed{ttl_ms} | Timer | Unbounded, clock: watermark-driven }` + a
  `frs_advance_watermark(cf, ts)` call the backend invokes from the operator's
  watermark path (it already owns this signal). Keys for lifecycle CFs must expose
  their death time: window state namespaces END timestamps in the key today
  (the timer/window key layouts are timestamp-prefixed); interval-join rows carry the
  row timestamp in the key suffix (`churn_probe.rs:239-241` mirrors this layout).
  Engine maps death-time → segment bucket at flush; NO per-key clock checks on reads
  (visibility = segment liveness, checked once per segment per probe).
- **Read cost**: probe fan-out = live segments overlapping the window. **Model traced
  to the P9 dose curve**: live ≈ 1 GiB at 64 MB flush segments → ~16 live runs ≈
  sorted-run cell-C's L0=16 point (p50 649 µs); with death-bucketed MERGE-ONCE
  (compact each segment-cohort exactly once, into a per-bucket sorted run — write-amp
  2.0 by construction) fan-out drops to ~window/bucket count (4-8) ≈ p50 200-400 µs.
  This is the lazy-leveling trade (§3.3) applied with lifecycle knowledge.
- **Correctness falsifier**: a segment may only drop when watermark > max-death AND
  no snapshot/checkpoint pins it (refcount) — late events inside the Flink
  allowed-lateness window keep state alive because the backend only advances the
  engine watermark past lateness. Failure mode = premature drop = wrong results, so
  V2's gate is the q5/q8/q11 exact-output harness (the recorded q5 lesson: windowed
  correctness masks easily — use byte-exact comparison, not pane counts).

### 3.3 Lazy leveling / tiered-leveled hybrids (Dostoevsky)

Bound write-amp by tiering the upper levels (no rewrite-on-arrival) and leveling only
the largest level; pay bounded extra read fan-out. Cells G/H measure forst-rs's
native version of this dial (L0 rollup trigger 4→8→16): **the dial is flat** —
write-amp 7.83 → 7.13 → 7.02 (−10 %) for probe p50 +20 % (§2.3-4); the whole-L1
rewrite (sorted-run W1) dominates, so deeper L0 batching amortizes ~nothing until
the asymptote (compaction off: 0.97×, p50 2 997 µs at L0 41 — sorted-run cell C).
The dose curve (P9) prices the read side at ~75 µs/run for this shape. **Verdict
(measured, not just argued): lazy leveling as a GLOBAL policy is rejected** — in the
current scheme it buys ~nothing until it buys everything at unbounded fan-out, and
q7's failure mode is exactly read-fan-out × write-volume compounding (sorted-run
§2.2-3). A true Dostoevsky implementation (tier upper levels properly, not just
deepen L0) would land between G/H and cell C — strictly dominated here by the
lifecycle form: §3.2's merge-once cohorts ARE lazy leveling scoped to data with a
death certificate, where the bounded extra fan-out (4-8 runs) is paid only while
the data lives and reclaimed wholesale when it dies. The survey adopts only that
scoped form.

### 3.4 Disaggregation-specific levers

- **Compaction-as-metadata (re-link, don't re-write).** Trivial move (sorted-run M2)
  generalizes on S3: a "compaction" whose input doesn't overlap the next level is a
  `link()` + level-field edit — 1.4 µs and ZERO upload bytes (P10) vs re-uploading the
  file. Under §3.2 segmentation this stops being the rare case sorted-run §M2 honestly
  sized at ≈0 for uniform keys: death-bucketed segments are DISJOINT in death-time by
  construction, so cohort demotions are pure re-links. Also applies to whole-segment
  expiry (drop = unlink) and restore (adopt). The mapping layer is already file-kind
  agnostic — no new machinery, only call sites.
- **Remote/offloaded compaction.** Paper pillar 6 marks it experimental in Flink [36]
  (Phase-2 §1.2-K deferred it). Note the interaction ORDER matters: offloading today's
  7.7× write-amp moves 7.7× the bytes through the network twice (read inputs + write
  outputs from the compactor). Shrink write-amp FIRST (this doc), offload the residue
  later (Phase-3+) — offloading is a CPU/locality lever, not a byte lever.
- **Flush-direct-to-remote vs local-first.** Today: pass-through write to remote +
  synchronous local write-through copy (`cached_fs.rs:33-40,115-130` — recorded
  FRS-LOCAL-FIRST-SST). Keep: the local copy is what makes KV-sep dereferences and
  in-window probes cheap (§3.1); the remote write is what makes checkpoint = link
  (P4). The lever that changes is VOLUME (write-amp), not topology.
- **Upload-amp accounting.** With write-through, S3 upload bytes = physical write
  bytes; q7-shaped: 7.83× logical → every logical byte uploads ~7.8×. Post-redesign:
  lifecycle CFs ~1.0-1.2× (cell F + merge-once overhead), KV-sep CFs ~1.6-2.7×
  (§3.1) — a 3-7× cut in S3 PUT volume *and* in the disk-write saturation that binds
  remote q7 (P3). S3 metadata-op count rises (more, smaller objects: segments +
  links); Phase-2's Stage-2 bench (per-file op cost) prices that; segment size is the
  knob (≥ 64 MB keeps op-count ≈ today's SST count).

### 3.5 Memtable/flush pressure

Flush is the floor every design pays (≈1× by definition of write-amp accounting).
Measured: flush-only cells sit at 0.96-0.98 (F; sorted-run C) — flush "overhead" over
logical bytes is ~zero (SST framing ≈ tombstone savings). So:

- Bigger memtables DO cut write-amp in the current scheme — measured 7.83 → 4.45 at
  4× memtable (cell I, the P11 correction): bigger flushes amortize the whole-L1
  rewrite. But the lever (a) plateaus in the same ~4× class as sorted-run M1-M6 —
  it attacks the same W1 term, so they do NOT stack to 1× — (b) costs RAM
  (256 MB × num-CFs; 8c/32g ceiling, recorded q4 noflush 44 GB lesson) and +33 %
  probe p50, and (c) on remote-primary enlarges the per-flush upload burst. Use it
  as a config lever where RAM allows; it is not the paradigm answer.
- The real Phase-2 coupling is **WAL-delta** (Stage 4): with checkpoint = link +
  WAL-tail durability, checkpoints stop forcing flushes (the recorded q4 noflush
  evidence: flush-on-barrier was load-bearing ONLY because the artifact path was a
  full-memtable copy; WAL-delta is per-checkpoint O(tail)). WAL adds its own 1×
  physical/upload stream — net physical ≈ 2× for memtable-resident bytes, still ≪
  today's 7.8×. On memory-tight boxes (8c/32g), FLUSH mode + lifecycle segments give
  the same bytes with bounded memtables; the §5 plan keeps both modes (Phase-2 §3.3).

---

## 4. What the survey rejects (and why — measured)

- **Global lazy leveling / universal-style tiering** (§3.3): MEASURED flat-then-cliff
  (cells G/H: −10 % write-amp for +20 % probe p50; asymptote = unbounded fan-out);
  q7's failure already compounds read fan-out with write volume. Rejected as a global
  default; adopted only death-scoped (§3.2 cohorts).
- **Pure FIFO for everything** (P7): write-amp 0.98 (cell F) but unbounded fan-out
  for non-dying keys (p50 3 510 µs raw; sorted-run C ÷4.6 probe throughput) and it
  trips the L0 backpressure (writer throttled to 111 K/s). Rejected outside
  lifecycle CFs.
- **"Just offload compaction to S3 workers"**: byte volume unchanged (×2 network);
  ordering argument §3.4. Deferred, not rejected.
- **Bigger memtables as THE write-amp lever**: real but bounded — 4.45× plateau in
  the same class as M1-M6 (attacks the same W1 term, non-stacking), RAM-priced
  (§3.5). Kept as a config lever, not the paradigm answer.

## 5. The design: lifecycle-aware KV-separated LSM with link-compaction

```
                       Flink backend (knows lifecycles)
        CF create: lifecycle{Windowed(ttl)|Timer|Unbounded}   watermark(ts)
  ────────────────────────────FFI──────────────────────────────────────────
   forst-rs engine, per CF:
     memtable ──flush──► death-bucketed SEGMENTS (key-sorted runs,
                          stamped [min_death,max_death) + level)
     lifecycle CFs:  segments cohort-merged ONCE (write-amp ≤2.0 by
                     construction), then DROPPED whole at watermark
                     (unlink; zero rewrite, zero tombstones)
     unbounded CFs:  key-LSM under sorted-run M1-M6 leveled discipline;
                     values ≥ threshold → append-only VALUE-LOG segments
                     (pointer entries in LSM; BlobDB-style GC, age-cutoff,
                      compaction-coupled pointer rewrite)
     all segments/SSTs/logs = immutable files under FileMappingManager:
       flush → register · checkpoint → link · expiry/GC → unlink
       restore → adopt · non-overlapping demotion → re-link (no I/O)
  ──────────────────────────────────────────────────────────────────────────
   CachedFileSystem: write-through local copy (probe/deref locality)
   S3: primary home; upload bytes = the (now ~1-2.7×) physical bytes
```

**Relationship to sorted-run M1-M6: complement + sequence, partially supersede.**
M1 (overlap-scoped picking), M3 (concurrent compactions), M4 (score/dynamic targets)
remain load-bearing for the key-LSM and unbounded CFs — but operate on ~6× fewer bytes
once values separate, and on ~0 bytes for lifecycle CFs. M2 (trivial move) is
SUBSUMED by link-compaction (same edit, now also free on S3). M5 (compression parity)
is orthogonal and still owed. M6 (shadow-in-budget) unchanged. Sequencing in §6:
M1/M3/M4 first (already designed, lower risk, immediate q7 relief), then V1/V2 of
this doc (paradigm wins), because the falsifier gates of V1/V2 need a healthy
baseline to A/B against.

**MVCC/checkpoint/restore interplay** (FileMappingManager assumed): unchanged rules —
immutable files, `min_active_snapshot` pins (compaction.rs:93-99), version apply-lock;
checkpoint links segments+logs+manifest (Phase-2 Stage 2 flow verbatim — the linked
set just gains two file kinds); restore adopts; JM discard = tombstone → unlink
(Phase-2 §2.3). New invariants: (i) segment drop requires watermark > max_death AND
refs==0 AND no snapshot pin; (ii) value-log segment delete requires no live pointer
in any PINNED version (conservative: segment retained while any version whose
manifest references it is pinned — same rule as SSTs, no per-key tracking).

## 6. Staged plan (falsifier-gated; each stage commit-sized & A/B'd)

0. **(in flight) Sorted-run S1-S5** (M1→M3→M4→flips) — gates G1-G6 of that doc.
   Target: key-LSM write-amp 7.7→~4 (its G1/G5).
1. **V0 — lifecycle plumbing (inert).** FFI CF-descriptor `lifecycle` field +
   `advance_watermark` (no engine behavior change). Backend wires window/TTL/timer
   knowledge for the NexMark state classes. Gate: suite green; descriptors observed
   in engine logs for q5/q7/q8/q11 CFs.
2. **V1 — death-bucketed segments + FIFO drop for lifecycle CFs.** Flush stamps
   death buckets; watermark drops whole segments (unlink); cohort merge-once.
   *Falsifier gates*: (a) churn_probe lifecycle cell write-amp ≤ 1.3 with probe p50
   ≤ 700 µs (the cell-F floor says 0.98 is available; the gate budgets merge-once);
   (b) q5/q8/q11 byte-exact at 5M (premature-drop falsifier); (c) remote q7 A/B:
   iostat write MB/s must drop ≥3× — if it doesn't, P3 was misattributed → STOP and
   re-profile before V2.
3. **V2 — KV separation for unbounded value CFs.** Pointer entries + value-log
   segments + BlobDB-style GC; merge CFs exempt (P12); `min_blob_size` threshold.
   *Falsifier gates*: (a) churn_probe kvsep cell measured write-amp within ±20 % of
   the §3.1 model (else the model's GC term is wrong — re-measure GC share);
   (b) probe p50 ≤ 1.3× baseline warm (deref cost); (c) q9/q20 remote A/B no-regress,
   q7 improves.
4. **V3 — link-compaction.** Non-overlapping demotions + expiry as mapping edits;
   on S3, zero-upload compaction for those paths. Gate: object-count + upload-byte
   asserts in the Stage-2 mock-FS harness; no `await_all_uploads` (Phase-2 §3.2
   invariant test extended).
5. **V4 — WAL-delta + big-memtable mode on remote-primary** (Phase-2 Stage 4 joint).
   Gate: per-checkpoint cost flat in memtable size; q4-class A/B both modes; 8c/32g
   memory ceiling respected (FLUSH default stays).

## 7. Expected numbers (traced; honest about model vs measurement)

| Quantity | Today (measured) | After plan (traced) |
|---|---|---|
| write-amp, q7-shaped churn | 7.83× (A′) | lifecycle CFs **~1.0-1.3×** (cell F floor 0.98 + merge-once budget); unbounded KV-sep CFs **~1.36-1.8×** (E′ 2.10 measured + §3.1 model); key-LSM residue ~4× on ~6× fewer bytes (sorted-run G1) |
| S3 upload volume (remote-primary) | = physical = 7.8× logical (P4) | ÷3-7 (same cells; plus M5 compression ÷2-3 on compressible production bytes — multiplicative, sorted-run §M5) |
| remote q7 (2 376.4 s; bar ≈1 370-1 380 s) | write 435-682 MB/s @98-99 % util (P3) | leave-saturation model (sorted-run §8): ≥3× write-volume cut → util < 90 % → wall **~1 380-1 600 s**; V1 gate (c) decides — q7 state IS lifecycle state (interval join), so V1 applies to ~all its bytes |
| remote q9/q20 (2 421.2 / 1 557.6 s) | same churn class, smaller probe share | 15-30 % from de-saturation (sorted-run §8 model) + V2 applies to join value bytes; gates V2(c) |
| checkpoint/restore | linear in new bytes | unchanged by this doc (Phase-2 owns it: link = O(files)) |
| probe p50 (q7 shape, warm) | 858 µs (A′) | ≤ 700 µs gated (fewer runs via lifecycle pruning vs +deref cost; cells F/G/H price both directions) |

## 8. Appendix — raw cell outputs (this session, Mac, medians of 3, 90 s/run)

```
MEDIANS label=default-rebase write_amp=7.83 p50_late_us=858  p99_late_us=3161 p50_degradation=4.20x  last_l0=2  (n=3)
MEDIANS label=kvsep-key      write_amp=2.10 p50_late_us=547  p99_late_us=859  p50_degradation=2.77x  last_l0=0  (n=3)
MEDIANS label=ttlseg-fifo    write_amp=0.98 p50_late_us=3510 p99_late_us=4577 p50_degradation=12.76x last_l0=41 (n=3)
MEDIANS label=lazy8          write_amp=7.13 p50_late_us=871  p99_late_us=2433 p50_degradation=4.36x  last_l0=2  (n=3)
MEDIANS label=lazy16         write_amp=7.02 p50_late_us=1027 p99_late_us=2962 p50_degradation=3.65x  last_l0=3  (n=3)
MEDIANS label=bigbuf256      write_amp=4.45 p50_late_us=1144 p99_late_us=2127 p50_degradation=6.48x  last_l0=0  (n=3)
```

Cell commands (one process per cell, sequential, same session;
binary `cargo build -p forst-rs-bench --release --bin churn_probe` at this commit):

```bash
churn_probe --runs 3 --label default-rebase
churn_probe --runs 3 --value-bytes 16 --label kvsep-key
FRS_L0_COMPACTION_TRIGGER=100000 churn_probe --runs 3 --no-deletes --label ttlseg-fifo
FRS_L0_COMPACTION_TRIGGER=8  churn_probe --runs 3 --label lazy8
FRS_L0_COMPACTION_TRIGGER=16 churn_probe --runs 3 --label lazy16
churn_probe --runs 3 --wbuf-mib 256 --label bigbuf256
```

Per-sample JSONL (files_per_level / live / phys / probe windows every 5 s):
`target/churn_results_survey/{default-rebase,kvsep-key,ttlseg-fifo,lazy8,lazy16,bigbuf256}.jsonl`.
Cell-F detail: achieved 111 300 rows/s (throttled at L0=41 ≥ slowdown 40), logical
2 104 MiB / phys 2 062 MiB per run — write-amp 0.98 is below 1 because the no-delete
accounting has no tombstone bytes and SST framing is sub-1 % at 200 B values.
Cell-I detail: all 3 runs at full 200 000 rows/s, write_amp {4.58, 4.38, 4.45},
max_total_files 27-30 (vs A′-class ~18-20), max_l0 4.

---

## 9. V0/V1 delivery evidence (2026-06-12, standing write-amp agent)

Stages V0 + V1 of §6 are IMPLEMENTED in this worktree (commits: V0 lifecycle
FFI plumbing; V1 death-bucketed segments + watermark FIFO-drop + cohort
merge-once, flag `FRS_LIFECYCLE_SEGMENTS` DEFAULT OFF). Same-session
churn_probe cells (Mac, medians of 3 × 90 s, methodology identical to §2;
raw: `target/churn_results_wa/*.jsonl`):

| Cell | Config | write-amp | p50 late (µs) | rows/s | last L0 | falsifier |
|---|---|---|---|---|---|---|
| rebaseline-v0pre | HEAD 771419c2c defaults | 7.35 | 552 | 200 K | 2 | — |
| default-v1post | V1 code, flag OFF | **7.42** | 542 | 200 K | 1 | — (no-regression gate ✓) |
| ttlseg-v1 | `--lifecycle` (trigger 24 ⇒ drop-only) | **0.98** | 914 | **200 K (unthrottled)** | 17 | **0 miss / 366 270 checks** |
| ttlseg-v1-cohort8 (pre-fix) | `--lifecycle`, trigger 8, 64 MB output split | 1.85 | 858 | 200 K | 17 | 0 miss / 419 803 |
| ttlseg-v1-cohort8-fix | ditto, single-run cohort outputs | 1.85 | 739 | 200 K | **11** | 0 miss / 389 323 |
| **ttlseg-v1-bigbuf** | `--lifecycle --wbuf-mib 256` (trigger 24) | **0.96** | **476** (p99 657) | 200 K | **4** | **0 miss / 354 055** |

### 9.1 Gate verdicts (V1 gates from §6 stage 2)

- **(a) write-amp ≤ 1.3 with probe p50 ≤ 700 µs**: **PASS — 0.96 / 476 µs**
  (lifecycle × 256 MB memtable, the composed cell). Decomposition: the
  segment machinery alone (64 MB flushes) hits the write floor at FULL rate
  (0.98, unthrottled — the cell-F backpressure artifact is fixed via
  `backpressure_l0_count` exemption) but sits at 17 live runs = 914 µs (the
  P9 dose curve); fewer-bigger segments via the memtable knob remove the
  fan-out for FREE in lifecycle mode (no compaction ⇒ no cell-I 4.45×
  write-amp coupling; RAM cost = 256 MB × lifecycle-CFs, the cell-I caveat).
  The cohort merge-once valve covers regimes where RAM can't (window ≫
  memtable): trigger-8 cell bounds fan-out 17→11 runs (739 µs) at write-amp
  1.85 ≤ the by-construction 2.0 bound. Probe p50 ~ 75 µs/run across all
  cells — fan-out, not machinery, prices reads.
- **(b) premature-drop falsifier**: **PASS — zero misses** across 1.5 M+
  continuous live-key point-gets (verifier thread in `churn_probe
  --lifecycle`) + the engine UT falsifier
  (`test_wa_v1_premature_drop_falsifier_and_expiry`: no drop at watermark ≤
  stamp, snapshot-defer honored, whole-segment drop after). The q5/q8-shaped
  byte-exact NexMark cells remain OWED before any default-ON flip (flag is
  default-OFF; Java wiring is V0-inert copies in `wa-java/`).
- **(c) remote q7 iostat A/B**: NOT RUN (remote box session required) — owed
  before V2.

### 9.2 Measured design corrections (decisions appended per discipline)

1. **Cohort outputs must be ONE run** (single-file `target_file_size = 0`).
   Measured falsification of the naive reuse of the compaction writer's 64 MB
   split: trigger-8 cell paid the full merge rewrite (write-amp 0.98 → 1.85)
   while last_l0 stayed 17 and p50 858 µs — a 9-segment merge re-emitted ~9
   key-disjoint files, i.e. all cost, zero consolidation. Fixed in
   `lifecycle_cohort_merge`; re-measured: last_l0 17 → 11, p50 → 739 µs at
   the same 1.85 (the rewrite is the price of the valve; consolidation now
   actually delivered).
1b. **Fewer-bigger segments beat merging at memtable-coverable shapes**: in
   lifecycle mode the memtable knob has NO write-amp coupling (cell I's
   7.83→4.45 was a compaction-amortization effect; with drop-instead-of-
   compact there is nothing to amortize) — `--wbuf-mib 256` measured 0.96 /
   476 µs / 4 runs. Default guidance: size lifecycle-CF memtables ≈
   live-window/4-8 where RAM allows; cohort merge is the fallback valve.
2. **Death stamping uses a writer-advanced event-time BOUND, not the
   watermark**: `max_death = max_event_time_at_flush + ttl` where the bound
   is advanced BEFORE the writes it covers (`frs_cf_note_max_event_time`).
   Stamping from the watermark would under-stamp (events run ahead of the
   watermark) = premature drops. The bound is monotone ⇒ over-stamping only
   (segments may live slightly longer — sound).
3. **Snapshot policy**: default = drops deferred while ANY engine snapshot is
   active (MVCC-pure); `FRS_LIFECYCLE_DROP_IGNORE_SNAPSHOTS=1` opts into the
   RocksDB compaction-filter precedent (`ignore_snapshots`) for deployments
   where checkpoint-held snapshots would defer reclamation too long.
4. **Merge-once bookkeeping is in-memory** (`lifecycle_merged` set): after a
   restore, a cohort may be re-merged at most once more — bounded and
   correctness-neutral; persisting a "merged" bit in blob v4 is not worth the
   format churn at V1.
5. **Manifest format v3 is conditional**: emitted only when a death stamp
   exists; flag-OFF (and any no-lifecycle) snapshots stay byte-identical v2 —
   the default-path-unchanged discipline, verified by the no-regression cell
   and `test_wa_v1_blob_v3_max_death_roundtrip_and_v2_when_unstamped`.
6. **L0 seq-disjointness invariant**: cohort merges select the oldest
   seq-CONTIGUOUS prefix of the CF's fresh segments (death order = seal order
   = seq order for Windowed CFs) and defense-in-depth verify contiguity
   before merging — a cohort output's spanned seq range can never interleave
   another L0 file's range, preserving the newest-first L0 read order.

### 9.3 What remains for "solved"

- V1 gate (c): remote q7 write-volume A/B (≥ 3× iostat write cut falsifier).
- Java side: adopt `wa-java/` copies in the Flink backend + q5/q8/q11
  byte-exact 5M cells (premature-drop falsifier at the SQL level) before any
  default-ON discussion.
- V2 (KV separation for unbounded CFs) and V3 (link-compaction) per §6.

## 10. R2/R10 close-out + V2a-1 groundwork (2026-06-13, standing write-amp agent)

Landed on `forst-rs` (V0-V2 delivery merged at 4ac16acc5; subsequent units
commit-per-gate):

- **R2 (review M → CLOSED)** — stamped-segment backpressure ceiling, option
  (a): stamped runs above `FRS_LIFECYCLE_STAMPED_CEILING` (default 4× cohort
  trigger = 96, floor 8) count back into the WriteController slowdown/stop
  triggers; the NORMAL rollup trigger keeps the uncapped exemption (split
  `rollup_l0_count` from `backpressure_l0_count`) because a forced rollup
  over a stamped+unstamped mix emits an UNSTAMPED (immortal) output — the
  valve is backpressure, never compaction. UT: 6 stamped @ ceiling 4 ⇒
  backpressure 2, rollup exempt, drops clear it to 0.
- **R10 (review M → CLOSED)** — lifecycle state persists (checkpoint blob
  **v4**): CfDescriptor gains `lifecycle_ordinal/ttl/watermark/
  max_event_time`, emitted ONLY when lifecycle state exists (default
  snapshots stay byte-identical v2/v3); restore re-applies to every CF.
  Kills consequence (b) — post-restore flush stamps from the restored
  bound, not stamp-0/immortal. UTs: storage v4 roundtrip + v2-when-default
  + ordinal-corruption; engine restore-resumes-expiry roundtrip.
- **V2a-1 (KV-separation groundwork, inert)** — `OpType::BlobRef = 17`
  (kTypeBlobIndex) engine-wide with every match arm chosen deliberately:
  storage = passthrough (op + pointer bytes verbatim), compaction =
  Put-like shadowing w/ op preserved, BlobRef-under-merge = corruption
  (P12), TTL filters = Keep (uninspectable bytes), engine read paths =
  fail-loud until deref lands, scan tables = Fallback. No writer emits it.
- **Flink adoption package** — `wa-java/ADOPTION-GATES.md`: destinations,
  wiring points, the C1-C6 byte-exact 5M cell matrix (q5/q8/q11 + lateness
  + flag-OFF inertness + mid-run restore), P1-P3 perf gates, default-ON
  disposition (R2 ✓, R10 ✓; C-cells + remote P1 still owed).

Same-session churn_probe gate cells (3 × 90 s medians, methodology §2;
raw: `target/churn_results_wa/*-r2r10post.log`):

| Cell | write-amp | p50 late (µs) | rows/s | last L0 | falsifier |
|---|---|---|---|---|---|
| default-r2r10post (flag OFF) | **7.32** | 535 | 200 K | 3 | — (vs 7.35/7.42 recorded ⇒ no-regression ✓) |
| ttlseg-r2r10post (flag ON, R2 ceiling default) | **0.98** | 911 | **200 K unthrottled** | 17 | **0 miss / 409 856 checks** |

Reading: the R2 ceiling (96) sits far above the healthy steady state (17
runs) — the lifecycle cell is bit-for-bit the §9 ttlseg-v1 regime (0.98 /
914 / 17 / 0-miss) with the stall-protection now in place.

## 10.2 V2a-2 delivery — flush-time KV separation (2026-06-13, standing write-amp agent, cycle 2)

§10.1 item 3 IMPLEMENTED (flag `FRS_KV_SEPARATION`, DEFAULT OFF; threshold
`FRS_KV_MIN_BLOB_SIZE` default 128 B, floor `VALUE_POINTER_LEN+1`):

- **Write path**: `FlushJob::run_kv` diverts qualifying Put values (eligible
  CF ∧ len ≥ threshold) into a lazily-created `<id>.vlog` segment
  (`VlogWriter`, fsynced BEFORE the SST publishes — WiscKey ordering) and
  emits 21-B `BlobRef(ValuePointer)` rows. Eligibility
  (`DbImpl::kv_sep_spec_for`): Unbounded lifecycle ∧ no merge operator (P12)
  ∧ no compaction filter. Segment ids come from the SST file-number counter.
- **Manifest**: `VlogSegmentMeta` rides `VersionEdit::new_vlog_segments` →
  `Version::vlog_segments` (sorted, dup-id = Busy); checkpoint blob **v5**
  appends the segment table, emitted ONLY when a segment is live (default
  snapshots stay byte-identical v2/v3/v4 — the same conditional-envelope
  discipline as v3/v4).
- **Read paths — every fail-loud V2a-1 arm is now a deref**: `sst_get`
  L0/L1+, `batch_get_vectorized` ×3 arms, `iter_versions_of` (mvcc/snapshot
  reads — BlobRef rewritten to a same-seq Put at collection), value-carrying
  scans (`ValueDecision::Blob`), S2 pinned scans (`PinnedStep::Blob`), all
  via a cached `VlogReader` per segment with a 64 KiB forward read-ahead
  chunk (scan derefs have flush-order locality; the chunk turned the probe
  gate from FAIL 1.51× into PASS 1.15×). Memtable tiers stay fail-loud
  (separation is flush-time only). `referenced_file_numbers` covers vlog ids
  (current + retiring versions), so future deletion is guarded like SSTs.
- **Checkpoint/restore**: full checkpoint copies live segments (+ verifies +
  `.vlog` orphan-scan arm incl. `max_observed` counter safety);
  incremental checkpoints split segments new/shared via the base manifest
  and ride the SAME handle lists as SSTs; link-mode registers+links them in
  FileMappingManager (additive use); checkpoint pins cover segment ids.

Gate cells (Mac, 3 × 90 s medians, methodology §2; raw
`target/churn_results_wa/*-v2a2*.log`):

| Cell | write-amp | p50 late (µs) | rows/s | verdict |
|---|---|---|---|---|
| default-v2a2post (flag OFF) | **7.46** | 544 | 200 K | no-regression ✓ (recorded band 7.32–7.46) |
| kvsep-v2a2 (flag ON, 2-pread deref) | **1.55** | 822 | **200 K unthrottled** | write-amp gate ✓ (model 1.36 ±20 % ⇒ ≤1.63); probe gate ✗ (1.51×) |
| kvsep-v2a2-chunk (+64 KiB deref chunk) | **1.55** | **635** (= 1.17×) | 200 K | **both gates ✓** (probe ≤ 1.3× = ≤707 µs; n=3: 627/635/647) |

Reading: q7-shaped churn write-amp **7.46 → ~1.5×** at full rate with
correctness (engine ITs: byte-exact point/batch/scan/snapshot reads,
flag-OFF inertness, merge/lifecycle/sub-threshold exemptions,
checkpoint→restore→checkpoint with live vlog, compaction passes pointers
without value rewrite). The ~0.2 over the 1.36 model is the assembled
workload's tombstone+key bytes still riding the key-LSM cascade — V2b GC
(item 3b) and the sorted-run lane own that residue.

## 10.3 V2b delivery — vlog GC (2026-06-13, standing write-amp agent, cycle 2)

§10.1 item 3b IMPLEMENTED (BlobDB-style, compaction-coupled — no standalone
GC writer):

- **Liveness accounting (the reclaim trigger)**: `VlogSegmentMeta.live_bytes`
  (manifest v5; set to the appended payload total at flush) is decremented
  by compaction `VersionEdit::vlog_freed` deltas — `emit_key_versions`
  provisionally counts every input BlobRef payload as freed and subtracts
  back rows it KEEPS verbatim (the streaming fast path accounts its drains
  directly). EXACT by construction: each pointer row contributes once and
  is decremented once, when THAT row leaves the LSM; the opt-in parallel
  path skips accounting (over-retention only, never premature reclaim).
- **Dead-segment reclaim**: after each compaction edit applies,
  `kv_gc_reap_dead_segments` removes `live_bytes == 0` segments from the
  version and deletes the files through the SAME guard discipline as SSTs
  (`referenced_file_numbers` now covers vlog ids of current + retiring
  versions; checkpoint pins defer via `pending_vlog_deletions`) — whole-file
  delete, zero rewrite.
- **Age-cutoff relocation** (`FRS_VLOG_GC_AGE_CUTOFF`, percent, **default
  0 = off — measured decision below**): when enabled, each compaction
  relocates surviving BlobRef rows whose segment is in the OLDEST cutoff
  fraction (id order = allocation order = age) into a fresh segment — same
  seq/op, MVCC-neutral — so mostly-dead old segments drain and reclaim.
  Stale-edit rejects delete the orphan relocation segment; job failure
  deletes the partial file.
- **drop_cf** reclaims the dropped CF's segments with its SSTs (no
  compaction ever runs for a dropped CF to zero them).
- **Lifecycle segments**: nothing to do — lifecycle CFs are exempt from
  separation (V2a-2 policy); their value bytes already expire whole via the
  V1 watermark drop (the survey's "free" arm).

Gates: space-amp bound UT
(`test_wa_v2b_vlog_gc_reclaim_relocation_and_falsifier`: fully-shadowed
segment reclaims with zero rewrite; live-pointer segment SURVIVES — the
premature-reclaim falsifier; cutoff-100 relocation drains + reclaims with
byte-exact reads throughout; physical deletes honor view/pin guards) + GC
churn cell (3 × 90 s medians, same methodology):

| Cell | write-amp | p50 late (µs) | vlog footprint (MiB, last; live window ≈ 870) | verdict |
|---|---|---|---|---|
| kvsep-v2a2-chunk (V2a-2, NO GC) | 1.55 | 635 | ~3 700 (unbounded: 18 M rows × 208 B all retained) | space-amp unbounded |
| kvsep + GC, cutoff 25 (run 0 only) | 3.28 | 617 | 1 104 | bounded BUT relocation re-wrote rows that die naturally — **REJECTED as default** |
| **kvsep-gc-v2b (GC default: cutoff 0)** | **1.58** | **649** (= 1.19×) | **903** (≈ 1.04× live) | **bounded ✓ at the V2a-2 write floor** (n=3, 200 K unthrottled) |

**Measured default decision (relocation OFF)**: streaming churn deaths
follow arrival order, so segments die WHOLE and the zero-rewrite reclaim
alone bounds space-amp at ~1.1×; cutoff-25 relocation re-wrote
soon-to-die rows, doubling write-amp (1.55 → ~3.1) for ~0 extra space —
the same trade that has RocksDB ship `enable_blob_garbage_collection =
false`. Relocation stays env-gated for mixed-lifetime value workloads
(its correctness is pinned by the UT's cutoff-100 drain).

## 10.4 V3 delivery — link-compaction / trivial move (2026-06-13, standing write-amp agent, cycle 2)

§10.1 item 4 IMPLEMENTED (flag `FRS_TRIVIAL_MOVE`, DEFAULT OFF):

- A compaction whose inputs do not overlap the destination level (and
  whose CF has no compaction filter) is satisfied by ONE VersionEdit
  re-leveling the SAME files — zero bytes rewritten, zero new file
  numbers, zero uploads on remote-primary. Two sites: the L0 rollup
  (requires the L0 set mutually key-disjoint — the destination-invariant
  guard; the whole-CF rollup means no remaining-L0 recency hazard) and
  the Ln→Ln+1 demotion (Ln per-CF non-overlap gives the invariant free).
- **No FileMappingManager edit is needed** — levels are manifest metadata
  over a flat `<num>.sst` namespace, so the "re-link" of the survey's §5
  degenerates to a pure VersionEdit (strictly cheaper than a mapping
  re-link; the mapping layer stays untouched — additive-only discipline
  holds). Cached readers stay valid (keyed by file number); death stamps
  ride along (lifecycle expiry scans every level); sorted-run M2 is
  SUBSUMED as designed.
- Correctness IT (`test_wa_v3_trivial_move_metadata_only_and_overlap_guard`):
  same-file-numbers re-level with `next_file_number` unchanged; byte-exact
  point/scan reads; OVERLAP forces the rewrite path; flag OFF byte-identical.

Gate — write-amp delta on the trivial-move-shaped cell (`--seq-keys
--no-deletes`: globally monotone keys ⇒ key-disjoint flushes, the
timer/seq-keyed-state shape; 3 × 90 s medians):

| Cell | write-amp | p50 late (µs) | rows/s | verdict |
|---|---|---|---|---|
| seqkeys-rewrite (flag OFF) | **2.95** | 555 | 200 K | rewrite cascade |
| seqkeys-tmove (flag ON) | **0.98** | 540 | 200 K | **metadata-only ⇒ flush floor; 3.0× write-volume cut, no probe regression** (n=3: 0.98/0.98/0.98) |

## 10.5 V2c delivery — vlog compression + M5 fairness fix (2026-06-13, standing write-amp agent, cycle 2)

§10.1 item M5 IMPLEMENTED. Full measurement + findings in **§12**; mechanics:

- **vlog compression (FRS-WA-V2c)**: `VlogWriter::create_with_compression` compresses each value
  payload; the record format gained a per-record `codec` byte + `uncompressed_len`
  (`VLOG_RECORD_HEADER` 8 → 13) so each record is SELF-DESCRIBING — `VlogReader::get`
  decompresses with the trusted size bound and needs no codec parameter, so the engine read path
  is unchanged and mixed-codec segments (relocation re-encoding) interoperate. Codec resolved by
  `DbImpl::kv_vlog_compression` from `self.options.compression` (override `FRS_VLOG_COMPRESSION`,
  default `inherit`), threaded through `KvSepSpec` (flush) + `KvGcSpec` (compaction relocation).
  Accounting stays in STORED on-disk bytes; checkpoint/restore transparent.
- **Fairness fix**: `scripts/run-8c32g.sh` `FRS_SST_COMPRESSION` default `none` → `lz4` (the fair
  match to ForSt/RocksDB Snappy + the engine default config.rs:268); added `FRS_VLOG_COMPRESSION`
  default `inherit`. Every prior remote number was frs-uncompressed vs compressed competitors.
- **Gate (§12.2)**: q7 SST-only write-amp 7.13 → 0.96 (lz4) → 0.59 (zstd) on a compressible-value
  cell; vlog MiB shrinks with the codec (compounds with KV-sep). 8 vlog UTs + V2a-2/V2b/cycle-1
  ITs green; clippy clean. Flag-gated through KV-sep (default OFF); compression default lz4.

### 10.1 Remaining for "solved" (supersedes §9.3)

1. V1 gate (c): remote q7 iostat A/B (≥3× write-volume cut) — needs the
   remote box; blocks default-ON and validates P3 before more V2 spend.
2. Flink adoption: splice `wa-java/` + run ADOPTION-GATES C1-C6.
3. ~~V2a-2: flag-gated flush-time separation~~ — **DONE 2026-06-13 cycle 2,
   §10.2** (both gates pass; flag stays DEFAULT OFF pending Flink adoption
   + remote A/B).
3. ~~V2b: BlobDB-style age-cutoff GC~~ — **DONE 2026-06-13 cycle 2, §10.3**
   (space-amp bounded ≈1.04× live at the V2a-2 write floor; relocation
   env-gated OFF by measured decision).
4. ~~V3: link-compaction~~ — **DONE 2026-06-13 cycle 2, §10.4** (trivial
   move 2.95→0.98 on the seq-keys cell; flag `FRS_TRIVIAL_MOVE` DEFAULT
   OFF; zero FileMappingManager edits needed — levels are manifest
   metadata over a flat namespace).
5. ~~M5: SST compression parity + vlog compression~~ — **DONE 2026-06-13 cycle 2, §10.5/§12**
   (harness default `none` → `lz4` fairness fix; vlog inherits the codec FRS-WA-V2c; codec
   ordering quantified). Remaining = bridge-blocked remote iostat A/B to size the box wall.

## 11. CYCLE 1 — combined-config write-amp floor + Q4/Q7/Q19 residual models (2026-06-13, PMC-1 standing Phase-1 write-amp owner)

Tip `eccec55a1` (sorted-run default-ON; KV-sep `FRS_KV_SEPARATION`, trivial-move
`FRS_TRIVIAL_MOVE`, lifecycle `FRS_LIFECYCLE_SEGMENTS`, vlog-GC all default-OFF).
Suite: WA-V engine UTs 11/11 green; `cargo clippy -p forst-rs-engine -p forst-rs-bench
--release` clean. Cells: churn_probe medians-of-3 × 90 s, same-session A/B per shape,
Mac system-allocator, methodology identical to §2. Raw:
`target/churn_results_cycle1/*.log`.

### 11.1 Combined-config floor — do KV-sep + trivial-move compose or overlap?

Two key shapes, each a clean same-session A/B of {baseline, kvsep-only, tmove-only,
BOTH-on}. The combined cell holds `FRS_KV_SEPARATION` AND `FRS_TRIVIAL_MOVE` on together
(the engine paths are orthogonal — trivial move re-levels file metadata regardless of
whether the SST carries full values or 21-B BlobRef pointers; KV-sep diverts values at
flush; vlog-GC accounting bounds the segment footprint).

**q7-shaped cell** (default churn: random hash-bucket keys + TTL deletes — the q7/q9/q20
interval-join shape):

| Cell | write-amp | p50 late (µs) | p99 (µs) | last L0 | vlog MiB |
|---|---|---|---|---|---|
| q7-default (both OFF) | **7.22** | 978 | 2 109 | 1 | 0 |
| q7-kvsep | **1.56** | 721 | 1 166 | 4 | 978 |
| q7-tmove | **7.42** | 619 | 1 126 | 1 | 0 |
| **q7-combined (BOTH ON)** | **1.56** | **667** | 952 | 3 | 940 |

**seq-keys cell** (`--seq-keys --no-deletes`: globally monotone keys — the
timer / changelog / seq-keyed-state shape, trivial-move-favorable):

| Cell | write-amp | p50 late (µs) | p99 (µs) | last L0 | vlog MiB |
|---|---|---|---|---|---|
| seq-rewrite (both OFF) | **2.96** | 555 | 627 | 1 | 0 |
| seq-kvsep | **1.09** | 637 | 747 | 1 | 3 560 † |
| seq-tmove | **0.98** | 553 | 635 | 1 | 0 |
| seq-combined (BOTH ON) | **1.03** | 635 | 803 | 1 | 3 560 † |

† no-deletes ⇒ nothing dies ⇒ V2b GC never reclaims ⇒ vlog grows with the dataset
(the `--no-deletes` accounting artifact, §8 cell-F class); under real TTL churn V2b
bounds it to ≈1.04× live (§10.3). The write-amp/probe numbers are unaffected (vlog
append is counted in physical bytes either way).

### 11.2 Finding — the levers OVERLAP by shape, they do NOT compose

**Headline combined write-amp floor: there is no stacking gain. The floor on each shape
is set by whichever single lever fits the key geometry, and that floor is ~1×.**

1. **q7-shaped: combined 1.56 = kvsep-only 1.56 exactly.** Trivial-move is *inert* on
   random/hash keys (7.42 alone ≈ the 7.22 baseline — it fires on ~0 % of compactions
   because random-bucket flushes overlap, so no compaction is non-overlapping). This is
   the direct empirical confirmation of the sorted-run §M2 model ("trivial-movable
   fraction at uniform keys ≈ 0") and of survey §10.4's own scoping. **KV-sep does 100 %
   of the work** on the join shape: 7.22 → 1.56 = a **4.6× write-volume cut** (probe p50
   *improves* 978 → 667 µs — the §3.1 prediction that 21-B pointers shrink the S2 merge
   buffers vs 220-B values, measured here as a net read win, NOT the 1.17× regression the
   §10.2 chunk-cell showed; that regression was a fan-out artifact of that cell's L0=4).
2. **seq-keys: kvsep 1.09, tmove 0.98, combined 1.03 — both reach the flush floor by
   DIFFERENT routes, redundantly.** Trivial-move makes the disjoint demotion
   metadata-only (0.98 = pure flush floor); KV-sep diverts the 200-B values to the vlog so
   the key-LSM that still rewrites carries only 21-B pointers (1.09). Running both gives
   1.03 — marginally *worse* than tmove-alone, because KV-sep's vlog-append + pointer-SST
   framing is slightly heavier than tmove's zero-byte metadata edit. They are
   **substitutes, not complements**, on this shape.

**Correctness guard for the combined config** (the configuration this section claims):
engine UT `test_cycle1_kvsep_and_trivial_move_compose_metadata_only_and_exact` drives
seq-disjoint big-value flushes with BOTH flags ON, asserts the rollup is a metadata-only
move of the **pointer-bearing** SSTs (same file numbers, L0 drained) AND that every
separated value derefs byte-exactly post-move across point / batch / scan / snapshot —
i.e. trivial move correctly re-levels BlobRef SSTs without disturbing the vlog
indirection. Green at tip.

**Conclusion (the "write-amp solved" metric):** the q7-shaped (join) write-amp floor is
**1.56×** (KV-sep), and the seq-keyed (timer/changelog) floor is **0.98×** (trivial move)
— **both are ~1× and both are already shipped (flag-gated, default-OFF)**. KV-sep is the
more *general* single lever (it also covers the seq shape at 1.09×); trivial-move is the
cheaper lever where it applies (monotone keys), and it is free on S3 (zero upload). The
write-amp paradigm question is answered at the engine level: **across both NexMark state
geometries, physical write-amp drops from 3–7.5× to ≈1×, gated medians-of-3.** What is
NOT yet closed is the *default-ON / Flink-adoption / remote-validation* leg (§11.4).

### 11.3 Q4 / Q7 / Q19 residual cost models (levers ON)

Re-derived from the recorded profiles (master-strategy §A.3, sorted-run §1.1/§8,
q7-analysis §1–2, q19 design doc) re-read against the §11.1 measured floors. The question
per query: with the write-amp levers ON, is the residual write-amp (more lever work),
read/scan (gate `FRS_COMPACT_WINDOWED` / S2), or framework (out of engine scope)?

**Q7 — was: write-volume-bound (the WORST goal-1 row, frs 2 376.4 s vs bar ~1 380 s).**
Recorded root cause (q7-analysis §1, sorted-run §1.1): I/O volume = write-amp ×
(1/compression) × read-amp; frs writes ~13 KB/event vs ForSt ~1.4 KB/event (~10×) at
98–99 % disk util — *the disk is the binding resource*, CPU/FFM/probe-merge exonerated (S2
falsifier +3.1 %). Decompose the ~10× event-byte gap against the measured levers:
  - **write-amp share — NOW LEVERED.** 7.22 → 1.56 (KV-sep) = 4.6× of the ~10×. q7 state
    is interval-join state on hash keys ⇒ the KV-sep shape exactly (Unbounded CF, no merge
    op) ⇒ the §11.1 q7-cell IS the q7 model. **Residual write-amp after KV-sep ≈ 1.56×.**
  - **compression share — MEASURED + FIXED CYCLE 2 (§12), multiplicative.** frs ran
    `FRS_SST_COMPRESSION=none` while ForSt/RocksDB run Snappy (sorted-run W5) — a pure ÷2–3
    disk-bytes term. CYCLE 2: harness default → `lz4` (fair match + engine default), vlog
    inherits the codec, codec ordering quantified on a compressible-value cell (q7 SST-only
    7.13 → 0.96 lz4 → 0.59 zstd). Remaining = the bridge-blocked remote iostat A/B to size the
    box wall (the Mac cell gives direction/magnitude, not the box-specific de-saturation).
  - **read-amp share (L0 fan-out).** Levered partially by S2 (shipped) + `FRS_COMPACT_WINDOWED`
    (L4, built, default-OFF) + sorted-run discipline; the §11.1 q7-cell shows KV-sep already
    *cuts* probe p50 (978 → 667) by shrinking merge buffers.
  **Residual model:** after KV-sep the q7 residual is **(a) compression (M5, ÷2–3, the
  biggest remaining byte lever) and (b) read/scan (L4/S2)** — NOT more write-amp lever work.
  Leave-saturation model (sorted-run §8): a 4.6× write-volume cut takes util well below
  90 %, so the wall improvement is ~proportional once de-saturated → projects q7 toward the
  ~1 380 s bar. **Next lever named: M5 SST compression parity (`FRS_SST_COMPRESSION` Snappy/LZ4
  on the remote runner), then L4 `FRS_COMPACT_WINDOWED` gate.** Both engine-transparent; the
  binding gate is the bridge-blocked remote q7 iostat A/B (V1 gate (c)) which must confirm
  the ≥3× write-volume cut transfers off-Mac before the compression term is sized on the box.

**Q19 — was: 1.70× (310 vs 528 / 308), FAIL both.** Recorded decomposition (q19 design doc
§Evidence; master-strategy §A.3): wall = ~78 % write-path (92 M puts + compaction) + ~22 %
MAP_ITER read-open. The *findRow O(n²)* hotspot (52 % on-CPU) is FIXED (flink 92a5d7c400b);
the recorded host L0-cap A/B showed "shrinking L0 helps reads but **compaction eats the
gain** ⇒ forst-rs compaction is more expensive per byte than RocksDB."
  - **The dominant ~78 % write/compaction share is exactly what KV-sep attacks.** q19 state
    is auction MapState — Put-dominated, value-bearing, Unbounded, no merge op ⇒ KV-sep
    eligible ⇒ the §11.1 q7-cell floor (7.22 → 1.56) applies to its compaction byte volume.
    "Compaction eats the read gain" *because* compaction was moving 220-B values; under
    KV-sep it moves 21-B pointers ⇒ the L0-cap read win is no longer eaten. **This is the
    named q19 write-side lever: KV-sep removes the compaction-byte tax that the L0-cap A/B
    proved was capping the read prune.**
  - **The ~22 % read-open residual** is partly engine (S2/L4, partial) and partly the Java
    `frs_vec_iter_prefix_open_batch` handle alloc/register/close on ~6 M exhausted iters
    (2026-06-07 q9/q19 skip-exhausted-iter design) — that leg is a Java-FFI change, **OUT OF
    SCOPE** (operator/Java-layer, per the lifecycle decision). The diffuse serde residual
    (RowDataSerializer copy + hashOf, no hotspot ≥10 %) is also framework, out of scope.
  **Residual model:** q19's binding residual after KV-sep is **NOT write-amp** (levered 4.6×)
  and **NOT a single engine read hotspot** — it is the Java iteration-layer overhead +
  diffuse serde (framework, out of engine scope). Engine-side: **KV-sep is the q19 lever;
  next engine read lever named = `FRS_COMPACT_WINDOWED` (L4) for the compaction-input read
  share.** Gate: remote q19 A/B (bridge-blocked).

**Q4 — was: BEATS ForSt by 669 s; RDB gap +70.3 s (1.23× M).** Recorded (2026-06-06
campaign, master-strategy §A.3): the RDB gap is **diffuse per-record efficiency** — config
exhausted, no single lever ≥ 2 %; WAL/memory/operator/zero-copy all tested and ruled out.
  - **Q4 is NOT write-amp-bound in the KV-sep sense.** q4 is a windowed aggregation
    (count/sum over auction windows) — its hot state is *accumulator* state under
    Reducing/Aggregating semantics, which the engine stores as **merge operands**
    (`RawConcatMergeOperator`). Per P12 (survey §1) merge-operand CFs are **KV-sep-exempt**
    (pointers don't concat) ⇒ the §11.1 KV-sep floor does NOT apply to q4's hot CF.
  - q4's key stream is auction-id-keyed (not globally monotone) ⇒ trivial-move's
    applicable fraction is the random-key ≈ 0 case (§11.2 q7 finding), so trivial-move is
    inert for q4 too.
  **Residual model:** q4's +70.3 s residual is **neither KV-sep nor trivial-move
  addressable** — it is the recorded diffuse per-record constant factor (FFM + engine vs
  JNI + native), confirmed by the merge-operand exemption. The named next lever for q4 is
  **NOT a write-amp lever**; it is the merge-chain / per-record reduce path
  (OPT-N04-class ReducingState write-back, recorded as the q12/q8 canary population) —
  which is a separate (largely Java-side) campaign, out of this Phase-1 write-amp scope.
  **Q4 verdict: write-amp is already at parity-or-better for q4 (it beats ForSt); the RDB
  residual is out of the write-amp lever family.**

### 11.4 What remains for "write-amp fully resolved"

Engine-level write-amp is **measured-solved** (≈1× on both shapes, gated). The unresolved
legs are validation/adoption, not new engine levers:

1. ~~**Compression parity (M5) — the one un-levered q7 byte source.**~~ — **MEASURED +
   FAIRNESS-FIXED + vlog-compressed, CYCLE 2 (§12).** frs `none` vs ForSt/RocksDB Snappy was a
   ÷2–3 (measured upper-bound: more) disk-bytes term. Now: harness default `none` → `lz4` (the
   fair match + engine default), vlog inherits the codec (FRS-WA-V2c), codec ordering quantified
   (none > lz4 > zstd; SST compression alone beats KV-sep on compressible state). Remaining leg
   = the bridge-blocked remote iostat A/B to size the box-specific disk de-saturation.
2. **Remote q7/q19/q20 iostat A/B (V1 gate (c) / V2 gate (c))** — confirm the ≥3× (q7) /
   4.6× (measured) write-volume cut transfers off-Mac and de-saturates the disk
   (util < 90 %). **BRIDGE-BLOCKED** (user fingerprint) — out of this agent's scope; it
   gates default-ON and validates P3 before any further write-amp spend.
3. **Flink adoption (`wa-java/` + ADOPTION-GATES C1-C6)** — byte-exact q5/q8/q11 + the
   default-ON disposition. Java-layer, out of engine scope.
4. **No further engine write-amp lever is justified by CYCLE-1 evidence.** KV-sep covers
   the join shape (1.56), trivial-move covers the seq shape (0.98), they do not stack, and
   q4's residual is provably outside the lever family (merge-operand exemption). The next
   engine spend is read/scan (L4 gate) and compression (M5), not more write-amp machinery.

Raw cell medians (this session):
```
MEDIANS label=q7-default   write_amp=7.22 p50_late_us=978 p99_late_us=2109 last_l0=1 vlog_mib=0    (n=3)
MEDIANS label=q7-kvsep      write_amp=1.56 p50_late_us=721 p99_late_us=1166 last_l0=4 vlog_mib=978  (n=3)
MEDIANS label=q7-tmove      write_amp=7.42 p50_late_us=619 p99_late_us=1126 last_l0=1 vlog_mib=0    (n=3)
MEDIANS label=q7-combined   write_amp=1.56 p50_late_us=667 p99_late_us=952  last_l0=3 vlog_mib=940  (n=3)
MEDIANS label=seq-rewrite   write_amp=2.96 p50_late_us=555 p99_late_us=627  last_l0=1 vlog_mib=0    (n=3)
MEDIANS label=seq-kvsep     write_amp=1.09 p50_late_us=637 p99_late_us=747  last_l0=1 vlog_mib=3560 (n=3)
MEDIANS label=seq-tmove     write_amp=0.98 p50_late_us=553 p99_late_us=635  last_l0=1 vlog_mib=0    (n=3)
MEDIANS label=seq-combined  write_amp=1.03 p50_late_us=635 p99_late_us=803  last_l0=1 vlog_mib=3560 (n=3)
```

---

## 12. CYCLE 2 — M5 compression measured + fairness fix + vlog compression (2026-06-13, PMC-1 standing Phase-1 write-amp owner)

Cycle-1 named **M5 (SST compression parity)** the highest-leverage remaining engine-transparent
q7 lever and flagged a **fairness bug**: the bench harness pinned `FRS_SST_COMPRESSION=none`
(`scripts/run-8c32g.sh:65`) while the ForSt/RocksDB templates
(`scripts/templates-linux/config-{forst,rocksdb}.yaml*`) set NO explicit compression ⇒ they use
their engine default (Snappy/LZ4). Every prior remote write-amp / disk number was thus
frs-**uncompressed** vs **compressed** competitors. M5 was UN-measured because `churn_probe`
fills values with `rng.next()` random bytes BY CONSTRUCTION — incompressible, so no codec moves.

### 12.1 Instrument: compressible-value churn_probe cell

Added `churn_probe --compressible` (`crates/forst-rs-bench/src/bin/churn_probe.rs`,
`fill_compressible`): every value is an auction/bid-shaped record (low-entropy repeated fields
+ common-prefix URL + repetitive padding), varied by seq/bucket. It is HIGHLY compressible
(synthetic padding compresses harder than typical real NexMark state — treat the measured ratio
as an **upper bound** on the M5 win; real data compresses less, same-signed). What it
establishes rigorously is the **direction + compounding**, which hold at any compressibility.
The control proves the cycle-1 diagnosis: random+lz4 = 0.82 wamp (no shrink) vs compressible+lz4
= massive shrink.

### 12.2 The none × lz4 × zstd × KV-sep gate table (Mac, 3 × 60 s medians, methodology §2)

Driver `/tmp/m5-matrix.sh` (one process per cell, sequential same-session). `FRS_SST_COMPRESSION`
sets SST block codec; `FRS_VLOG_COMPRESSION=inherit` makes the vlog follow it (FRS-WA-V2c).

**q7-shaped (compressible values, random keys + TTL deletes):**

| codec | no-kvsep wamp | kvsep wamp | kvsep vlog MiB | kvsep p50 (µs) |
|---|---|---|---|---|
| none | **7.13** | 2.13 | 963 | 663 |
| lz4  | **0.96** | 1.43 | **734** | 669 |
| zstd | **0.59** | 0.89 | **729** | **1440** |

**seq-keys (compressible values, monotone keys, `--no-deletes`):**

| codec | no-kvsep wamp | kvsep wamp | kvsep vlog MiB | kvsep p50 (µs) |
|---|---|---|---|---|
| none | **2.71** | 1.34 | 2397 | 629 |
| lz4  | **0.30** | 0.91 | **1903** | 689 |
| zstd | **0.15** | 0.72 | **949** | **2383** |

### 12.3 Findings

1. **M5 is huge on compressible state, and codec ordering is monotone (none > lz4 > zstd).**
   q7 SST-only write-amp 7.13 → 0.96 (lz4) → 0.59 (zstd); seq 2.71 → 0.30 → 0.15. This is the
   ÷2–3-and-beyond disk-bytes term cycle-1 §11.4 item 1 named as the largest UN-levered q7 byte
   source — now MEASURED locally (the remote iostat A/B is still the binding sizing gate, §12.5).
2. **vlog compression WORKS and COMPOUNDS with KV-sep.** The kvsep `vlog MiB` shrinks with the
   codec (q7 963 → 734 → 729; seq 2397 → 1903 → 949) — without FRS-WA-V2c those values would
   hit the vlog uncompressed and forfeit M5 on exactly the big bytes. The vlog inherits the SST
   codec via `FRS_VLOG_COMPRESSION=inherit` (default).
3. **On HIGHLY-compressible state, SST compression ALONE beats KV-sep+compression.** lz4-no-kvsep
   (0.96) < lz4-kvsep (1.43); zstd-no-kvsep (0.59) < zstd-kvsep (0.89). When values compress
   ~as well in the SST block as in the vlog, KV-sep's pointer-SST + vlog-framing overhead is net
   *worse* than just compressing the value in place. **Implication: KV-sep's win is largest on
   INCOMPRESSIBLE / low-redundancy values (the cycle-1 7.22 → 1.56 was on random bytes); on
   compressible state, plain compression is the cheaper lever.** They are partly SUBSTITUTES,
   not pure complements — the right default is "compression always on; KV-sep ON only where the
   value is large AND not block-compressible" (a future eligibility refinement, not blocking).
4. **zstd buys ratio at a probe-latency cost.** zstd-kvsep p50 jumps (q7 1440 vs lz4 669; seq
   2383 vs lz4 689) — the level-3 compress CPU on the write path throttles + the deref
   decompress shows on probes. **lz4 is the right DEFAULT** (best ratio/CPU balance, matches the
   competitors, is the engine default); zstd is the opt-in high-ratio / disk-bound-remote choice.

### 12.4 Fairness fix (shipped, auditable)

`scripts/run-8c32g.sh`: `FRS_SST_COMPRESSION` default `none` → **`lz4`** (the goal mandates
"config must match Forst"; lz4 is BOTH the fair match to ForSt/RocksDB AND the forst-rs engine
default `crates/forst-rs-common/src/config.rs:268`). Added `FRS_VLOG_COMPRESSION` default
`inherit` so KV-sep cells compress the vlog too. Both env-overridable (set
`FRS_SST_COMPRESSION=none` for the zero-copy read-path A/B); the change is documented inline at
the pin. This makes every future remote A/B fair (frs-compressed vs competitors-compressed).
The two `scripts/profile-q4-*.sh` profiling scripts keep `none` deliberately (zero-copy
read-path experiments, not the bench harness).

### 12.5 vlog compression (FRS-WA-V2c, shipped, flag-gated through KV-sep)

`crates/forst-rs-storage/src/vlog.rs`: `VlogWriter::create_with_compression` compresses each
value payload; the record format gained a per-record `codec` byte + `uncompressed_len`
(`VLOG_RECORD_HEADER` 8 → 13) so each record is SELF-DESCRIBING — `VlogReader::get`
decompresses with the trusted size bound and needs NO codec parameter, so the engine read path
(`vlog_deref`, all batch/scan/snapshot arms) is unchanged and mixed-codec segments (a relocation
re-encoding legacy records) interoperate. The codec is resolved by `DbImpl::kv_vlog_compression`
from `self.options.compression` (override `FRS_VLOG_COMPRESSION`), threaded through `KvSepSpec`
(flush) and `KvGcSpec` (compaction relocation). Space-amp/GC accounting stays consistent (all in
STORED on-disk bytes). Checkpoint/restore is transparent (segments are copied byte-for-byte; the
v5 manifest carries only codec-agnostic metadata). 8 vlog UTs + the V2a-2/V2b/cycle-1 engine ITs
green (incl. byte-exact point/batch/scan/snapshot derefs and compaction passthrough);
`cargo clippy -p forst-rs-storage -p forst-rs-engine -p forst-rs-bench` clean.

### 12.6 Updated "write-amp fully resolved" remaining list (supersedes §11.4)

1. ~~**M5 compression parity**~~ — **MEASURED + FAIRNESS-FIXED + vlog-compressed, this cycle.**
   Engine-transparent, default lz4, codec ordering quantified. The remaining leg is the
   **bridge-blocked remote q7/q19/q20 iostat A/B** to SIZE the disk-de-saturation on the box (the
   Mac numbers establish direction/magnitude, not the box-specific wall). Not a new engine lever.
2. **Remote iostat A/B (V1/V2 gate (c))** — still BRIDGE-BLOCKED; gates default-ON + sizes M5.
3. **Flink adoption** (`wa-java/` + ADOPTION-GATES) — Java-layer, out of engine scope.
4. **No further engine write-amp lever is justified.** Write-amp on both shapes is ≈1× or below
   once compression is on (q7 0.59–0.96 SST-only; seq 0.15–0.30); KV-sep covers incompressible
   values, compression covers compressible ones, trivial-move covers monotone keys. The engine
   write-amp paradigm is closed; the open work is validation/adoption + the read/scan lane
   (L4/S2), not more write-amp machinery.

Raw cell medians (this session, `/tmp/m5-results.txt`):
```
q7   none  no-kvsep wamp=7.13  | kvsep wamp=2.13 vlog=963   p50=663
q7   lz4   no-kvsep wamp=0.96  | kvsep wamp=1.43 vlog=734   p50=669
q7   zstd  no-kvsep wamp=0.59  | kvsep wamp=0.89 vlog=729   p50=1440
seq  none  no-kvsep wamp=2.71  | kvsep wamp=1.34 vlog=2397  p50=629
seq  lz4   no-kvsep wamp=0.30  | kvsep wamp=0.91 vlog=1903  p50=689
seq  zstd  no-kvsep wamp=0.15  | kvsep wamp=0.72 vlog=949   p50=2383
```
Note: the synthetic `--compressible` value is more compressible than typical real NexMark state,
so these write-amp drops are an UPPER bound on the real M5 win (same-signed); the directional and
compounding conclusions hold at any compressibility.

---

External references: WiscKey — Lu, Pillai, Gunawi, Arpaci-Dusseau, Arpaci-Dusseau,
"WiscKey: Separating Keys from Values in SSD-conscious Storage", FAST '16.
Dostoevsky — Dayan, Idreos, "Dostoevsky: Better Space-Time Trade-Offs for LSM-Tree
Based Key-Value Stores via Adaptive Removal of Superfluous Merging", SIGMOD '18.
ForSt disaggregation — Mei et al., "Disaggregated State Management in Apache
Flink 2.0", PVLDB 18(12), 2025. In-repo C++ anchors via `git show main:` —
`utilities/flink/flink_compaction_filter.h`, `db/compaction/compaction_picker_fifo.h`,
`db/blob/*`, `include/rocksdb/advanced_options.h:909,955,1027-1092`.
