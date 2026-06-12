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

External references: WiscKey — Lu, Pillai, Gunawi, Arpaci-Dusseau, Arpaci-Dusseau,
"WiscKey: Separating Keys from Values in SSD-conscious Storage", FAST '16.
Dostoevsky — Dayan, Idreos, "Dostoevsky: Better Space-Time Trade-Offs for LSM-Tree
Based Key-Value Stores via Adaptive Removal of Superfluous Merging", SIGMOD '18.
ForSt disaggregation — Mei et al., "Disaggregated State Management in Apache
Flink 2.0", PVLDB 18(12), 2025. In-repo C++ anchors via `git show main:` —
`utilities/flink/flink_compaction_filter.h`, `db/compaction/compaction_picker_fifo.h`,
`db/blob/*`, `include/rocksdb/advanced_options.h:909,955,1027-1092`.
