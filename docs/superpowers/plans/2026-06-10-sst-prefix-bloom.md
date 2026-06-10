# SST Prefix Bloom (q7/q9/q20 read-volume lever) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut prefix-scan preads 2-3× by skipping SSTs that contain no keys for the probe's
join-key prefix — the verified q7/q9/q20 binder (point blooms can't prune range scans; every
overlapping SST pays index+data-block preads per probe; READ_AT 182µs mean, 26% cold).

**Architecture:** A second per-SST `Sbbf` built over FIXED-length key prefixes (`PREFIX_BLOOM_LEN
= 16` bytes — covers key-group + stateId + join key in the composite layout). Writer collects
distinct 16-byte prefixes (keys shorter than 16 bytes contribute their full length, padded
domain-separated); footer gains `prefix_bloom_offset/size` (format-version bump, old readers of
new files unsupported, new reader of OLD files = no prefix bloom → no pruning, conservative).
Scan-open consults `may_contain_prefix(&prefix[..16])` ONLY when the probe prefix is ≥16 bytes;
shorter probes bypass (correctness-conservative).

**Tech Stack:** Rust, `crates/forst-rs-storage/src/sst/{schema,writer,reader,bloom_filter}.rs`,
engine scan SST-selection in `crates/forst-rs-engine/src/db.rs` (prefix_scan path). No FFI/Java
changes (engine-internal).

**Evidence base:** sweep doc "q7/q9/q20 READ-VOLUME LEVER DECIDED" section (DECAY_ATTR: gets
healthy n_ovl 1-3 @1.5-5µs; binder = scan path; baseline q9@50M = 813.5s for A/B).

---

### Task 1: Footer format — prefix_bloom_offset/size + version bump

**Files:** Modify `crates/forst-rs-storage/src/sst/schema.rs` (footer codec, SST_FORMAT_VERSION),
test in the same module's round-trip tests.

- [ ] Read the current footer encode/decode + version checks (`schema.rs`, follow
  `bloom_filter_offset` plumbing end-to-end first).
- [ ] Add `prefix_bloom_offset: u64`, `prefix_bloom_size: u32` to the footer struct; bump
  `SST_FORMAT_VERSION`; decode tolerates the PRIOR version with both fields = 0 (no prefix
  bloom). Failing round-trip test first (encode new → decode → fields preserved; decode old
  bytes → zeros), then implement, then `cargo test -p forst-rs-storage`.
- [ ] Commit: `feat(sst): footer prefix-bloom fields + format version bump (back-compat read)`.

### Task 2: Writer — collect prefixes, emit prefix bloom

**Files:** Modify `crates/forst-rs-storage/src/sst/writer.rs` (BOTH writer variants — the
finish paths at ~:406 and ~:752 emit the existing key bloom; mirror there).

- [ ] On every key append, insert `&key[..min(16, key.len())]` into a `HashSet<[u8;16]>`-style
  collector (fixed array padded with 0 + length byte XOR'd in, so "abc" ≠ "abc\0..."). Constant
  `PREFIX_BLOOM_LEN: usize = 16` in schema.rs.
- [ ] At finish: build `Sbbf` from the distinct prefixes (same fpp parameters as the key bloom),
  write after the key bloom, record offset/size in the footer.
- [ ] Failing test: write SST with keys sharing 3 distinct 16-byte prefixes → reader (Task 3)
  `may_contain_prefix` true for those 3, false for 64 random others (fpp-tolerant assert ≥60/64
  false). Then implement; `cargo test -p forst-rs-storage`; commit.

### Task 3: Reader — load + expose may_contain_prefix

**Files:** Modify `crates/forst-rs-storage/src/sst/reader.rs` (open loads key bloom at ~:252;
mirror for prefix bloom when offset != 0).

- [ ] `pub fn may_contain_prefix(&self, probe_prefix: &[u8]) -> bool`: if no prefix bloom OR
  `probe_prefix.len() < PREFIX_BLOOM_LEN` → `true` (conservative); else test the padded 16-byte
  form of `probe_prefix[..16]`.
- [ ] Tests from Task 2 pass; old-format SST opens fine (no prefix bloom, always true). Commit.

### Task 4: Engine scan-open pruning

**Files:** Modify `crates/forst-rs-engine/src/db.rs` — the prefix-scan SST selection (the loop
that collects overlapping SST sources for a scan; find via the DECAY_ATTR `sstloop`
instrumentation site and `scan_iter` / prefix_scan source assembly).

- [ ] In the per-SST loop, after the existing key-range overlap check, add
  `if !reader.may_contain_prefix(prefix) { continue; }` — ONLY on the prefix-scan path (range
  scans with arbitrary bounds must NOT consult it).
- [ ] Extend the DECAY_ATTR counters with `n_pbloom_skip` so future runs show prune counts.
- [ ] MVCC/correctness argument (REQUIRED in the commit message): the prefix bloom is built
  over ALL keys in the SST including tombstones/merges — a bloom miss proves NO entry with that
  prefix exists in the SST at any seq, so skipping cannot change scan results.
- [ ] Failing engine test first: write enough to flush 3 SSTs with disjoint join-key prefixes;
  prefix_scan for one prefix; assert results identical with pruning on (compare against a
  no-prune build via a test-only env `FRS_DISABLE_PREFIX_BLOOM=1` gate — keep the gate, it is
  the A/B + kill switch). `cargo test -p forst-rs-engine` (all 266+) green. Commit.

### Task 5: Full verification

- [ ] Full Rust suite: `cargo test --workspace` green; clippy clean.
- [ ] Rebuild Linux .so (`run-8c32g.sh build`) + macOS dylib (`cargo build --release -p
  forst-rs-ffi`); 534 Java native tests green (format change is engine-internal but the dylib
  feeds the Java suite).
- [ ] Push ForSt; GHA ForSt pipeline green.
- [ ] **A/B (the gate):** q9@50M depth-1: baseline 813.5s vs prefix-bloom build; expect 1.5-2.5×
  on the decay phase; THEN q9@100M MAXSEC 2400 (was ~2600s true) — target ≤1776s (0.8× RDB).
  Then q20@100M (was DNF/1610 routing) and q7@100M (was 1441.6 depth-1). Correctness: q8 band +
  q11 exact rows + q3 byte-count. Record EVERYTHING in the sweep doc.

## Out of scope
- Per-CF configurable prefix length (fixed 16 first; revisit if a state layout misses).
- Java/FFI changes (none needed).
