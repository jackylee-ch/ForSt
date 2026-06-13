# q9 KV-separation OOM — code-level root-cause model (2026-06-14)

**Status:** DIAGNOSIS ONLY (no execution, no .rs changes). Built from reading the
KV-separation write/read/GC paths on branch `forst-rs` at tip `06408bb0f`
(base `1cda0e724`, the exact tip the V3 verdict pass ran).

**Scope:** explain *why* q9 flag-ON DNF'd with OOM ×2 on the 35 G Mac while
RocksDB (909.3 s) and ForSt (1661.6 s) both finished q9 cleanly
(91,813,372 rows), and propose a bounded fix DIRECTION. The trigger stack was
`FRS_SST_COMPRESSION=lz4 + FRS_KV_SEPARATION=true + FRS_TRIVIAL_MOVE=true +
FRS_RS_S2_PINNED=1` (V3 doc §"★ V3 FULL 8-QUERY verdict pass 2026-06-14",
line ~2469). The same doc's narrative blames the cgroup arithmetic
(`2 TM×16g + 1 JM×4g = 36g > 35g RAM`). That is the *proximate* trigger; this
note is the *code* reason the **flag-ON** stack — and historically only the
flag-ON stack — pushes q9's TM over its 16 g cgroup when the flag-OFF path
fit (memory: flag-OFF q9 "peak RSS 18.2 GB ... memory-bounded"; "q9 1.14×
PASS" remote). This is a NEW regression that appeared when the harness actually
started forwarding `FRS_KV_SEPARATION` to the engine.

---

## 1. The KV-separation memory path (where values live, what stays resident)

When `FRS_KV_SEPARATION` is ON and a CF is `Unbounded` lifecycle (q9's
join/Rank state CFs default to `Unbounded` — `column_family.rs:188,317`,
`CfLifecycle::default() == Unbounded`), the flush path diverts every `Put`
value `>= min_blob_size` (default 128 B, `db.rs:14874-14880`) out of the SST
and into an append-only `.vlog` segment, leaving a 21-byte `ValuePointer`
(`OpType::BlobRef`) inline in the key-LSM:

- write: `flush.rs:384-431` `separate_batch_values` → `VlogWriter::append`
  (`vlog.rs:186-206`). **One vlog segment is created per flush of a KV-sep CF**
  (`flush.rs:409-416`, lazy create on first qualifying row; `segment_id =
  version_set.allocate_file_number()` at `db.rs:12610`).
- read: a `BlobRef` row is dereferenced through `DbImpl::vlog_deref`
  (`db.rs:12761`) → `get_or_open_vlog_reader` (`db.rs:12735`) →
  `VlogReader::get` (`vlog.rs:254`).

### The two resident structures that grow with live-segment count

1. **`DbImpl::vlog_readers: RwLock<HashMap<u64, Arc<VlogReader>>>`**
   (`db.rs:978`). One entry per segment **ever dereferenced**. Each
   `VlogReader` holds an **open `RandomAccessFile` handle** + a
   `Mutex<Option<(u64, Vec<u8>)>>` **64 KiB chunk buffer** (`vlog.rs:233,
   237-241, 273-280`). The chunk buffer is allocated on first miss and kept for
   the reader's lifetime.
   - **No cap, no LRU, no budget, no charge() anywhere.** Verified by exhaustive
     grep — there is no `max_vlog*`, `vlog*cap`, `vlog*budget`, `vlog*evict`, or
     `vlog*lru` symbol in the tree. The cache is a plain `HashMap`.
   - The **only** removal path is `get_or_open_vlog_reader`'s sibling
     `kv_gc_reap_dead_segments` (`db.rs:12658`), which `remove`s a reader
     (`db.rs:12677-12680`) **only when that segment's `live_bytes` reaches 0**
     (`db.rs:12660-12665`).

2. **`Version::vlog_segments: Vec<VlogSegmentMeta>`** (`version/mod.rs:230`),
   cloned on **every** version edit (`apply_edit`, `version/mod.rs:547`
   `self.vlog_segments.clone()`). Each entry is small (~32 B), so this Vec is a
   secondary, not the dominant, RSS driver — but it grows monotonically with
   live-segment count and the clone-per-edit cost rises with it.

The `VlogReader` open-file handle is the more dangerous of the two: the engine
runs on opendal-backed files and the local cached-FS keeps per-file state; an
unbounded set of open handles + 64 KiB buffers is a per-segment ~tens-of-KiB
resident cost that scales with the number of *never-reclaimed* segments, and on
the disaggregated path each handle may also pin remote-file bookkeeping.

---

## 2. Why q9 specifically triggers it — its access pattern defeats reclaim

The reclaim trigger requires a segment to reach `live_bytes == 0`. `live_bytes`
only decrements through compaction's `vlog_freed` deltas (`version/mod.rs:565-568`,
saturating), and those deltas are produced by `KvGcState::note_input` /
`unfree` (`compaction.rs:147-164`): a `BlobRef` row's payload is counted as
"freed" only when that pointer row is **dropped or superseded inside a
compaction that ingests it**.

### The default `FRS_VLOG_GC_AGE_CUTOFF = 0` disables relocation (`db.rs:14798-14811`)

With cutoff 0, `kv_gc_spec_for_compaction` builds an **empty `relocate` set**
(`db.rs:12639-12645`) — accounting-only, **zero rewrites**. So a segment can
only ever be reclaimed by dying **WHOLE**: every one of its pointer rows must be
superseded/dropped through compaction with no surviving live pointer.

The default-0 decision (`db.rs:14791-14797`) was MEASURED on **q7-shaped
streaming churn**, where "deaths follow arrival order, so segments die WHOLE
naturally" (FIFO death ≈ FIFO arrival). **That assumption is q7-specific and
FALSE for q9.**

### q9 is a multi-way interval JOIN + Rank — death order ≠ arrival order

q9 (the heaviest join in the set; V3 doc line ~2483 "single heaviest join")
maintains join-buffer / Rank state keyed by join key. Under a join:
- Keys are **overwritten and superseded at scattered times** driven by the
  arriving stream, not in segment-allocation order.
- A given flush-segment therefore ends up holding a **mix** of long-lived and
  short-lived pointers. A handful of still-live pointers keep an otherwise
  mostly-dead segment **above `live_bytes == 0` indefinitely**.
- An old, mostly-dead segment's surviving pointer rows sit in **deep LSM
  levels**; in an LSM they may not be compacted for a long time, so even the
  *accounting* that would free them lags far behind.

Net: with relocation OFF (default) and non-FIFO death, **segments essentially
never reach `live_bytes == 0`** for the duration of the q9 run. `vlog_readers`
grows monotonically across ~80–83 M ingested rows (where the OOM hit, V3 doc
line ~2476-2477) — one open handle + 64 KiB buffer per flush segment q9 ever
read — and is never trimmed. This is exactly the structure that has **no cap**.

This is the WiscKey/BlobDB space- and reader-amplification failure mode: it is
acceptable for FIFO-death workloads (q7) and pathological for scattered-death
joins (q9) unless GC relocation OR a reader-cache bound is in force.

---

## 3. Cross-check vs RocksDB / ForSt (what bounds THEIR memory)

ForSt finished q9 at 1661.6 s and RocksDB at 909.3 s on the SAME box. What they
have that the forst-rs KV-sep path lacks:

- **Bounded file/handle + block cache.** RocksDB/ForSt cap open table readers
  via `max_open_files` and serve blocks from a fixed-size, **charged** block
  cache (LRU with a byte budget). forst-rs's `vlog_readers` is an **uncharged,
  uncapped HashMap** — there is no analogue of the block-cache byte budget for
  vlog readers. (Note the SST-side `sst_readers` map at `db.rs:957` is ALSO
  uncapped, but the flag-OFF path that historically FIT exercised only that map;
  KV-sep ADDS the `vlog_readers` map on top → strictly more resident state than
  the path that fit at 18.2 GB.)
- **BlobDB GC default discipline.** RocksDB ships
  `enable_blob_garbage_collection=false` too (db.rs:14794-14796 cites this), but
  RocksDB's blob *readers* are still served through the **bounded** block cache,
  so a non-reclaimed blob file does not pin an uncharged resident reader the way
  forst-rs's does. The forst-rs port copied BlobDB's GC-OFF default WITHOUT
  copying BlobDB's bounded reader/cache discipline.
- ForSt additionally uses a **bounded remote file cache** (memory: "ForSt
  disaggregated ... bounded LRU file cache over file-resident state") — the
  resident footprint is admission-controlled. forst-rs KV-sep has no such bound
  on the vlog side.

The flag-OFF forst-rs path fits because all value bytes ride the SST block
cache (bounded, charged). KV-sep **moves the dominant byte source OUT of the
charged cache into an UNCHARGED, UNCAPPED reader set** — that is the
regression.

---

## 4. Ranked root-cause candidates (code evidence)

**#1 (primary) — Unbounded `vlog_readers` cache, never reclaimed under q9's
non-FIFO death.** `db.rs:978` (no cap) + `db.rs:12658-12665` (reclaim gated on
`live_bytes==0`) + `db.rs:12639-12645` (default cutoff 0 ⇒ no relocation ⇒
mostly-dead segments never reach 0 under scattered join death). This is the
structure with *both* unbounded growth *and* a defeated reclaim trigger for
exactly q9's access pattern. **Most strongly supported by code.**

**#2 (contributing) — vlog-GC relocation default-OFF is q7-tuned, wrong for
joins.** `db.rs:14791-14811`. Even WITH a bounded reader cache, leaving cutoff 0
means segment *files* (disk space-amp) and the resident `vlog_segments` Vec grow
unbounded for q9; under tight RAM the extra page-cache + manifest churn
compounds #1. Turning relocation on (cutoff > 0) would drain mostly-dead
segments whole and let reclaim fire — but it re-introduces write-amp
(db.rs:14792-14794 measured 1.55→~3.1 on q7), so it is a *mitigation*, not the
clean fix.

**#3 (secondary / amplifier, lower confidence) — per-edit `vlog_segments.clone()`
+ per-compaction full-Vec scan.** `version/mod.rs:547` clones the whole Vec on
every edit; `db.rs:12628-12634` clones+filters it per compaction. Small per
entry, but grows with segment count and adds allocator pressure under an
already-tight 16 g cgroup. Unlikely to be the dominant RSS driver on its own
(HYPOTHESIS — needs a heap profile to size vs #1).

The q9 read path itself (`db.rs:11739/11837/11935` batch deref) materializes
each value per-batch and hands it up — it is **bounded** and NOT a leak. Ruled
out as the driver.

---

## 5. Memory-growth model

Let `S(t)` = number of distinct vlog segments q9 has dereferenced by time `t`.
- Flush rate of the KV-sep CFs ⇒ `S` grows roughly linearly with ingested rows
  (one segment per flush; `flush.rs:409`).
- Reclaim removes a segment only when `live_bytes==0`. Under cutoff 0 +
  scattered join death, reclaimed(t) ≈ 0 for most of the run.
- Resident vlog cost ≈ `S(t) × (open-handle state + ≤64 KiB chunk buffer)` +
  `S(t) × ~32 B` (manifest), with `S(t)` monotonically increasing.

The OOM hit at ~80–83 M of 100 M rows (V3 doc ~line 2476), i.e. late in the run
when `S(t)` is largest — consistent with a monotonically-growing,
never-trimmed resident set crossing the 16 g cgroup, NOT with a fixed
per-batch cost. This temporal signature is the discriminator between #1 (grows
with run length) and a bounded read-path cost (flat).

---

## 6. Proposed bounded-fix DIRECTION (design only — DO NOT implement this cycle)

Match ForSt's discipline: make the vlog reader set BOUNDED and CHARGED, the same
way the block cache bounds SST blocks.

**A. Cap + evict `vlog_readers` (primary, mirrors ForSt `max_open_files`).**
Replace the plain `HashMap` at `db.rs:978` with an LRU bounded by *count* (and
optionally by charged bytes including the 64 KiB chunk). On insert past the cap,
evict the least-recently-used reader (drop the `Arc`; the open handle + buffer
free). Readers are stateless w.r.t. correctness (segments are immutable once a
pointer is version-visible — `db.rs:12732-12734`), so eviction-then-reopen is
always safe; the cost is a re-open on a cache miss. Bound default ~ a few
thousand, or size off a byte budget. This directly removes the unbounded
structure and makes resident vlog cost O(cap), independent of `S(t)`.

**B. (optional, complementary) make vlog-GC relocation adaptive to lifecycle.**
Keep cutoff 0 for FIFO-death CFs (q7), but for `Unbounded` *join-shaped* CFs
auto-enable a modest relocation cutoff so mostly-dead segments drain and the
disk/manifest footprint stays bounded too. This bounds space-amp (#2/#3) but
trades write-amp; gate behind a flag and verify it does not regress q7.

**C. (defense-in-depth) charge vlog readers to a shared budget** so KV-sep
resident state competes in the SAME pool as the block cache rather than being
free off-budget — closing the "moved bytes out of the charged cache" gap from §3.

Recommended order: **A first** (smallest, highest-leverage, correctness-trivial
— it alone should let q9 fit), then re-measure before B/C.

---

## 7. To CONFIRM — needs a q9 repro run with heap/jemalloc profiling

This is a code-derived model; the following must be MEASURED before committing a
fix (do NOT run heavy benches this cycle — this is the owed follow-up):

1. **Run q9 flag-ON to (or near) the OOM point with jemalloc profiling**
   (`MALLOC_CONF=prof:true,prof_leak:true` / `jemalloc-ctl` heap dump). EXPECT:
   the resident growth dominated by `VlogReader` allocations (open-file state +
   64 KiB chunk `Vec`) and the `vlog_readers` HashMap, growing with row count.
2. **Track live vlog-segment count over time** (instrument
   `version.vlog_segments.len()` and `vlog_readers.read().len()` at intervals).
   EXPECT: both rise ~linearly and `kv_gc_reap_dead_segments` returns ~0 for q9
   under cutoff 0 — confirming reclaim never fires. Compare to q7 (should
   reclaim WHOLE segments → flat).
3. **A/B the fix direction A**: cap `vlog_readers` (e.g. 2048) and re-run q9
   flag-ON. EXPECT: peak RSS drops back toward the flag-OFF 18.2 GB and q9
   FINISHES (then it only needs to beat ForSt 1661.6 s to win — V3 doc
   line ~2473-2475).
4. **Sanity**: confirm q9's join/Rank CFs are actually classified `Unbounded`
   and that their values exceed `min_blob_size` (128 B) so KV-sep is in fact
   active for q9 (it should be — join buffers carry full Bid/Auction rows).
5. **Regression guard**: re-run q7 flag-ON with fix A to confirm the LRU cap
   does not slow the FIFO-death workload (q7's win was -26%, V3 doc line ~2542).

If the heap profile instead shows the dominant growth is the `vlog_segments`
Vec clone or some OTHER structure, fall back to candidate #3 — but the code
evidence ranks the uncapped `vlog_readers` cache (#1) as the prime suspect.

---

### File:line index

- `crates/forst-rs-engine/src/db.rs:978` — `vlog_readers` (uncapped HashMap) ← **primary**
- `crates/forst-rs-engine/src/db.rs:12735-12754` — `get_or_open_vlog_reader` (insert, no evict)
- `crates/forst-rs-engine/src/db.rs:12658-12688` — `kv_gc_reap_dead_segments` (reclaim gated on live_bytes==0)
- `crates/forst-rs-engine/src/db.rs:12623-12652` — `kv_gc_spec_for_compaction` (empty relocate at cutoff 0)
- `crates/forst-rs-engine/src/db.rs:14798-14811` — `vlog_gc_age_cutoff_percent` (DEFAULT 0, q7-tuned)
- `crates/forst-rs-engine/src/db.rs:12597-12613` — `kv_sep_spec_for` (Unbounded-only, min_blob 128)
- `crates/forst-rs-engine/src/flush.rs:384-431` — `separate_batch_values` (one segment per flush)
- `crates/forst-rs-storage/src/vlog.rs:233-282` — `VlogReader` (open handle + 64 KiB chunk, per reader)
- `crates/forst-rs-storage/src/version/mod.rs:230,547,565-568` — `vlog_segments` Vec + clone + freed decrement
- `crates/forst-rs-engine/src/compaction.rs:130-228` — `KvGcState` (freed accounting / relocation)
- `crates/forst-rs-common/.../column_family.rs:188,317` — `CfLifecycle::default()==Unbounded`
