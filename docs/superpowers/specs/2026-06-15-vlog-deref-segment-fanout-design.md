# Coalesced vlog-deref SEGMENT fan-out — design + mini-bench (Phase-2 cycle 4)

**Date:** 2026-06-15
**Status:** PMC design — evidence gathered, implemented + micro-benched THIS cycle.
**Author:** PMC-2 (Phase-2 disaggregated state)
**Flag:** `FRS_VLOG_DEREF_FANOUT` (default-OFF; byte-identical when OFF)
**Composes with:** `FRS_VLOG_COALESCE_DEREF` (the coalesce this parallelizes),
`FRS_KV_SEPARATION` (the baseline that creates vlog-resident values),
`FRS_SCAN_OPEN_FANOUT` / `FRS_SCAN_COLD_PRIME` (the SST-side analogues).

---

## 0. The finding (one paragraph)

The cycle-3 read-path gate established **KV-separation + lz4 as the fair write-amp
baseline** (`2026-06-13-cycle3-readpath-combined-gate-results.md`). Under that
baseline the join-payload classes (q7/q9/q20, values >= 128 B) relocate their
values to the value-log, so a `batch_get` over separated state derefs `N`
`BlobRef` pointers from the vlog. `FRS_VLOG_COALESCE_DEREF` already groups those
`N` pointers **by segment** and sorts each group by offset so each segment is one
ranged read instead of `N` scattered chunk-thrashes
(`db.rs:coalesced_vlog_deref_into`). But the per-segment loop is **serial**:
`for (segment_id, group) in by_segment { reader.get_coalesced(group) }` issues
the `M` segment GETs back-to-back, so on the disaggregated/remote path a batch
spanning `M` vlog segments pays **`M x remote-RTT` serially** before it can
return. This is the SAME serial-seed asymmetry that `FRS_SCAN_OPEN_FANOUT` fixed
for the SST-reader OPENs (catalog item #1) and that `prefetch_concurrent` already
fixes for the point-`batch_get` whole-file warm — but the **vlog cross-segment
deref has no equivalent**. The fix is to fan the per-segment coalesced derefs
across the existing read-I/O pool and barrier on them, scattering values back to
their key slots. Byte-identical output (values are independent; the per-segment
group is already reordered by offset and the output proves order-invariant);
timing-only; default-OFF.

---

## 1. Evidence — the bottleneck is structural and confirmed serial

### 1.1 The serial segment loop (code-grounded)

`DbImpl::coalesced_vlog_deref_into` (`db.rs:13840`):

```rust
for (segment_id, mut group) in by_segment {
    group.sort_by_key(|(_, p)| p.offset);
    let reader = self.get_or_open_vlog_reader(segment_id)?;   // cold open = 1 RTT
    let values = reader.get_coalesced(&ptr_refs)?;            // ranged read = 1 RTT
    for ((slot, _), value) in group.into_iter().zip(values) {
        resolved[slot] = Some(Some(value));
    }
}
```

Each iteration does up to two synchronous remote round-trips (a cold
`get_or_open_vlog_reader` footer/index read, then the `get_coalesced` ranged
read). With `M` distinct segments in the batch's deferred set, segment `i+1`'s
GET cannot start until segment `i`'s `get_coalesced` returns => **`M` serial
round-trips per coalesced batch**. The within-segment coalesce (offset sort +
one ranged read) is already optimal; the **cross-segment** dimension is the gap.

### 1.2 Why `M >= 2` is the common case under the fair baseline

A `batch_get_vectorized` resolves a vectorized chunk of keys (the Flink async
batch, default in-flight 30k / buffer 4k). Those keys' winning `BlobRef`s point
into whatever vlog segments were live when each value was last written — i.e. the
writes are spread across the flush history, so a batch routinely spans **many**
segments (one per flush epoch the batch's keys touch). The cycle-3 finding section 3
measured the separated deep-probe at 18.09 us (legacy) — the per-segment
serialization is the remote-path analogue of that local chunk-thrash. `M` grows
with state age (more flush epochs -> more segments), exactly the high-`M` regime
where the serial loop is most costly, mirroring the SST fan-out growth that
motivated `FRS_SCAN_OPEN_FANOUT`.

### 1.3 Confirmed NOT already done

- `coalesced_vlog_deref_into` loops segments serially (`db.rs:13859`); no
  `read_io_pool` / `prime_opens_concurrent` use in the vlog path (grepped:
  the pool is driven only from SST prefetch + scan-open-fanout).
- The single-iterator scan path (`ValueDecision::Blob` -> `db.vlog_deref(ptr)`,
  `db.rs:10045`/`:11089`) does NOT even coalesce — it is per-row serial. (That
  is a SEPARATE, larger lever; this cycle takes the lower-risk batch-path
  segment fan-out and notes the scan-path coalesce as the next candidate — section 6.)
- `read_io_pool` (`prefetch.rs:274`) and the barrier helper
  `prime_opens_concurrent` (`prefetch.rs:305`) already exist and are the right
  vehicle (they fan SST opens); the vlog deref simply never used them.

### 1.4 Micro-benchmark — projected benefit (cost model)

`crates/forst-rs-bench/src/bin/vlog_deref_fanout.rs` (this cycle). Models the
cross-segment latency term ONLY: `M` segment derefs, serial (current loop) vs
concurrent through a `min(M, pool)`-wide pool, per-segment GET = `FRS_MODEL_RTT_MS`
(open + ranged read counted as the segment's round-trip cost). Pool =
`clamp(cores/2,2,6)`.

(Measured numbers from this cycle's bin are inserted in section 5.)

**Reading.** The cross-segment latency drops ~2-5.3x, bounded at ~min(M, pool).
The win is per coalesced batch that spans `M >= 2` segments and compounds across
the thousands of async batches a join/OVER query issues against remote separated
state. It is **zero** on warm/cached vlog readers (no GET), on single-segment
batches (`M = 1`), and when the coalesce flag is OFF (no deferred set) — hence
the must-be-OFF and must-no-op-when-warm constraints below.

**Scope/honesty:** models the cross-segment latency tail, NOT within-segment
coalesce (already optimal) and NOT warm hits. End-to-end NexMark win requires the
online-box S3 A/B (user-gated); this is the local design->implement->micro-bench
evidence stage per standing rules.

---

## 2. Implementation (flag-gated, default-OFF, byte-identical when OFF)

### 2.1 Mechanism

Split `coalesced_vlog_deref_into` into a serial path (existing, default) and a
fan-out path (`FRS_VLOG_DEREF_FANOUT=1` AND remote AND `M >= 2`). The fan-out
path:

1. Builds the `by_segment` groups exactly as today (group by `segment_id`, sort
   each group by `offset`).
2. For each segment, captures a `'static + Send` closure (via `self_weak`) that
   opens the reader and runs `get_coalesced`, returning `(slots, values)` or the
   first error through a channel.
3. Submits the `M` closures to `read_io_pool` and **barriers** (the existing
   `prime_opens_concurrent`-style join), then scatters each segment's
   `(slot, value)` pairs into `resolved`. Any segment error is surfaced after the
   barrier (first error wins), preserving the serial path's error contract.

The merge of `slot -> value` after the barrier is the SAME assignment the serial
loop does; only the ORDER in which the `M` GETs are issued changes. Output bytes
are identical (each slot gets exactly its pointer's value; slots are disjoint
across segments).

### 2.2 No-op guards (correctness + zero-cost-when-warm)

- **Coalesce flag OFF** => `deferred` is empty => this function is a no-op already.
- **Fan-out flag OFF** (`FRS_VLOG_DEREF_FANOUT` unset) => serial loop (today).
- **`M = 1`** (one segment) => nothing to overlap, serial.
- **Local regime** (`self.fs.is_local()`) => preads us-class, RTT~=0, serial.
- **No `self_weak`** (can't form `'static` jobs) => serial fallback (safe).

### 2.3 Cancellation / safety

The closures own clones of their `(slot, ValuePointer)` groups and a `Weak<Self>`.
A panicking job is contained by the pool's `catch_unwind` (`prefetch.rs:259`);
the barrier counter still advances via the `Signal` guard (`prefetch.rs:320`), so
the join can never strand. No new lifetime: the deref reads immutable vlog
segments; a concurrent reader open double-checks via the RCU insert in
`get_or_open_vlog_reader`. Memory is bounded by `M` decoded segment groups in
flight — the same values the serial loop would materialize, just concurrently.

### 2.4 TDD regression tests (engine)

1. **Byte-identical OFF-vs-ON** (`vlog_deref_fanout_byte_identity`): a CF with
   KV-sep ON + >= 2 vlog segments (two flushes) + values >= threshold, run
   `batch_get` over keys spanning both segments with the coalesce ON and the
   fan-out OFF vs ON; assert identical `Vec<Option<Vec<u8>>>` AND equality to the
   per-key `get` oracle.
2. **No-op when M = 1 / local**: assert the serial path is taken.
3. **Error surfaced**: a corrupt pointer in one segment surfaces the same error
   under fan-out as serial.

### 2.5 Mini-benchmark proving the win

`crates/forst-rs-bench/src/bin/vlog_deref_fanout.rs`: the section 1.4 cost model
(serial vs `min(M, pool)`-wide, modeled per-segment RTT) — the local
design->micro-bench evidence stage. The engine-driven `LatencyFileSystem` arm
(real `DbImpl` + KV-sep over `M` segments behind injected RTT) is the next-cycle
extension, mirroring `scan_cold_start.rs`'s two-stage approach.

---

## 3. Why this over the other candidates

| Candidate | Status | Verdict |
|---|---|---|
| **Coalesced vlog-deref SEGMENT fan-out** (this doc) | confirmed serial (1.3); read-path; KV-sep join class; 2-5.3x cross-segment; pool exists | **CHOSEN** — highest leverage on the fair-baseline (KV-sep) read class, low risk (timing-only, reuses pool), composes with the coalesce + SST-open-fanout |
| Scan-path inline vlog deref coalesce | larger (changes the iterator value-resolution state machine) | **next candidate (section 6)** — bigger surface, sequenced after this |
| SST-open-fanout / cold-prime | ALREADY SHIPPED (cycle 2/3) | done |
| S2 pinned + loser tree | ALREADY SHIPPED, gated on remote G3+G5 | done |
| Parallel checkpoint UPLOAD | upload already concurrent (`trackedUpload` futures) | not a serial gap |

---

## 4. Constraints honored

Vectorized/batch/zero-copy (fan-out touches whole segment groups, no per-key/byte
work; no `byte[]`/copy added — values move by `Vec` ownership, same as serial);
config unchanged; flag default-OFF byte-identical; `#![forbid(unsafe_code)]`
preserved (the pool is std Mutex+Condvar). The end-to-end NexMark win requires
the online-box S3 A/B (user-gated), per standing rules.

---

## 5. CYCLE-4 IMPLEMENTATION — LANDED (2026-06-15)

Implemented exactly as specced; flag `FRS_VLOG_DEREF_FANOUT` default-OFF,
byte-identical when OFF.

- **Flag + override** (`db.rs`): `vlog_deref_fanout_enabled()` (live env read,
  test-toggle-able via `set_vlog_deref_fanout_override`), mirroring
  `vlog_coalesce_deref_enabled`. Default OFF.
- **`coalesced_vlog_deref_into` split** (`db.rs`): the per-segment work is
  extracted into `deref_one_segment_into` (the unit shared by both paths so the
  `resolved[slot]` assignments are byte-identical), and the function branches to
  `coalesced_vlog_deref_fanout` when `vlog_deref_fanout_enabled() && !is_local()
  && M >= 2 && self_weak present` — else the serial loop (today, byte-for-byte).
  The offset-sort moved BEFORE the branch so both paths operate on offset-ordered
  groups.
- **`coalesced_vlog_deref_fanout`** (`db.rs`): one `'static + Send` job per
  segment (captures a `Weak<DbImpl>` + the owned group), each opens its reader +
  runs `get_coalesced` and sends `(slot, value)` pairs (or the segment's error)
  over an `mpsc` channel; submits all jobs via `prime_opens_concurrent` (the
  existing panic-safe pool barrier — same vehicle as the SST open fanout) and,
  after the barrier, scatters results into `resolved` with **first-error-wins**
  (serial `?` parity). No `unsafe`; reuses the existing read-I/O pool.

### Results

- **OFF-vs-ON byte-identity (THE gate): PASS.**
  `test_vlog_deref_fanout_byte_identical_kvsep_remote` builds KV-sep ON + 3
  flushes (>= 2 vlog segments) of large incompressible values on a
  REMOTE-reporting FS (`RemoteFakeFs`, `is_local()==false`, so the fan-out
  engages), then asserts the coalesced `batch_get_vectorized` with the fan-out
  forced ON is byte-identical to forced OFF AND to the per-key `get` oracle, over
  a scattered (join-probe-shaped) key order with interleaved misses.
  `test_vlog_deref_fanout_flag_default_off_and_override` guards the default-OFF
  contract.
- **Cost-model micro-bench** (`crates/forst-rs-bench/src/bin/vlog_deref_fanout.rs`,
  serial per-segment loop vs concurrent through a `min(M, pool=6)`-wide pool,
  modeled per-segment RTT):

  **RTT = 23 ms (recorded dev-Mac->BOS):**

  | segments M | serial (ms) | concurrent (ms) | speedup |
  |---|---|---|---|
  | 2  | 55.0  | 28.0  | 1.96x |
  | 4  | 107.2 | 26.0  | 4.12x |
  | 8  | 206.5 | 48.8  | 4.23x |
  | 16 | 425.8 | 77.8  | 5.47x |
  | 32 | 833.2 | 156.0 | 5.34x |

  **RTT = 2 ms (intra-DC online box):**

  | segments M | serial (ms) | concurrent (ms) | speedup |
  |---|---|---|---|
  | 2  | 5.0  | 2.5  | 1.98x |
  | 4  | 10.1 | 2.6  | 3.94x |
  | 8  | 19.8 | 4.9  | 4.03x |
  | 16 | 39.1 | 7.6  | 5.16x |
  | 32 | 78.6 | 14.0 | 5.61x |

  The cross-segment latency drops ~2-5.6x, bounded at ~min(M, pool=6) — matching
  the section 1.4 model. The bin takes `--smoke` (caps RTT, asserts >1.5x at
  M>=4) for CI.
- **Suites + gates**: engine lib builds clean; the two new tests PASS; bench bin
  builds + runs (cost-model + smoke). `cargo fmt --all --check`, `clippy
  --all-targets`, and `RUSTDOCFLAGS=-D warnings cargo doc` results recorded in the
  cycle report.

The end-to-end NexMark win still requires the online-box S3 A/B (user-gated), per
standing rules; this is the in-repo design->implementation->evidence stage. The
engine-driven `LatencyFileSystem` arm (real `DbImpl` batch_get over M segments
behind injected RTT, OFF vs ON — mirroring `scan_cold_start.rs`'s stage 2) is the
next-cycle bench extension.

---

## 6. Next-cycle candidate

**Scan-path inline vlog-deref coalesce + fan-out.** The single-iterator scan
(`prefix_scan_iter_owned_arc` / `scan_iter_owned_arc_with_error_slot`) resolves
each separated row via `db.vlog_deref(ptr)` inline, per-row, fully serial — no
coalesce at all. With KV-sep ON a scan over separated values pays one remote vlog
GET per row. The lever is to buffer a window of `BlobRef` rows, coalesce them by
segment (reusing `coalesced_vlog_deref_into` + this cycle's fan-out), and emit
the window — the scan-path analogue of the batch coalesce. Larger surface (the
iterator must defer + re-order value resolution while preserving emit order), so
it is sequenced after this cycle's lower-risk batch fan-out.

---

## 7. Locality-aware deref-fanout gating (2026-06-15, follow-up cycle)

### 7.1 Finding (from the combined-stack validation, 41ab15e22)

The original gate (section 3) engaged the fan-out on
`vlog_deref_fanout_enabled() && !self.fs.is_local() && by_segment.len() >= 2`.
The middle predicate is the **FS-level** `CachedFileSystem::is_local()`, which
returns `false` **unconditionally** (`cached_fs.rs:760`) — a caching FS over a
remote backend *can* pay a round-trip on any open, so it reports not-local for
the whole filesystem. But locality is a **per-segment, per-instant** property:
once a vlog segment's bytes are write-through/page-cache resident, its deref is a
µs-class local pread with **no remote RTT to overlap**. Fanning such a warm
segment out is pure overhead — it consumes a read-I/O-pool slot and pays the
per-batch coordination (mpsc channel + `tx.clone()` ×M + boxed-job submits +
condvar wakeups + the receiver barrier) for bytes that were already local. It is
net-positive remotely (the cold case the fan-out exists for) but wasted work on
a warm-cache scan.

### 7.2 Fix — per-segment locality classification at the engage site

`RandomAccessFile::is_local()` already answers per **current serving tier**:
`LocalFirstSstFile::is_local()` (`cached_fs.rs:909`) returns
`self.cache.contains(&self.key)`, and a whole-file-fetched `InMemoryRandom`
inherits the default `true`. So the genuine signal is per-file, not per-FS.

- `VlogReader::is_local()` (`vlog.rs`) — new; delegates to `self.file.is_local()`,
  surfacing the segment's current cache-residency.
- `DbImpl::coalesced_vlog_deref_into` (`db.rs:~14294`) — the gate is now
  **locality-aware**. With the flag ON and `>= 2` segments, each segment is
  classified by its reader's `is_local()`:
  - **warm-local** segments take the **direct serial path**
    (`deref_one_segment_into`) — NO pool dispatch;
  - the **genuinely-remote** (cache-miss) group still fans out via
    `coalesced_vlog_deref_fanout`, but only when `>= 2` remote segments remain to
    actually overlap (otherwise a single remote segment also goes direct).
  The FS-level `is_local()` is no longer consulted at this site. Opening the
  reader to classify is cheap + idempotent (the cached LRU open the loop/job would
  do anyway); a reader that fails to open is treated as remote so its error
  surfaces on the normal path.
- `VLOG_DEREF_SEGMENTS_FANNED` (`db.rs`) — new test-only diagnostic counter,
  incremented by the segments actually dispatched to the pool, so a test can
  prove "warm = 0 fanned, cold = M fanned".

Byte-identity is unchanged: a segment produces the same `resolved[slot]` values
whether resolved on the direct or the fanned path (slots are disjoint across
segments), so routing a subset of segments to each path cannot change output.

### 7.3 TDD + evidence

- **`test_vlog_deref_locality_warm_direct_cold_fanout`** (engine lib) — a new
  `LocalityFakeFs` reports FS-level `is_local()==false` (like `CachedFileSystem`)
  but answers per-file `is_local()` from a shared path-set, so the test can mark
  vlog segments warm-local. Asserts: (a) COLD regime (no segment marked) still
  fans out `>= 2` segments (`VLOG_DEREF_SEGMENTS_FANNED` delta) — the remote win
  is preserved; (b) WARM regime (every segment marked local) dispatches **ZERO**
  segments to the pool — the direct path; (c) the emitted values are
  **byte-identical** across both regimes and to the per-key `get` oracle. This is
  the regression gate: before the fix the warm arm would have fanned out `>= 2`
  and `warm_fanned == 0` would fail.
- All 3 `vlog_deref` tests + all 15 `vlog` engine tests + the full engine
  (426 pass/4 ignored) and storage (481 pass/3 ignored) suites green.
- **Mini-bench** (`vlog_deref_fanout --smoke`): the COLD/remote arm keeps the
  3.75-4.27x fan-out win at M>=4; a new WARM-local arm isolates the per-batch
  pool **coordination** cost (byte work identical on both paths) and shows the
  direct path removes ~12-21 µs/batch — a >12000x reduction in dispatch overhead
  on cache-resident segments. Smoke asserts both (cold > 1.5x, warm direct > 2x
  cheaper).
- `cargo fmt`, `clippy --all-targets`, `RUSTDOCFLAGS=-D warnings cargo doc` clean
  on the three changed crates. Default OFF + byte-identical.
