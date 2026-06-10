# OPT-N16: Zero-Copy Batch GET (q9's final 225s) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans or subagent-driven-development.

**Goal:** Eliminate the double-copy on every resolved point GET (engine `to_vec()` per tier
+ FFM `copy_nonoverlapping` into Java) by wiring the EXISTING `batch_get_arrow` zero-copy
path into the default `VectorizedExecutor` GET dispatch. Mandate-aligned (forbid memcpy).

**Evidence:** round-2 findings doc OPT-N16 (file:line anchors there): `VectorizedMemTable::get
value_at(..).to_vec()`, `KvBlock::lookup payload[s..e].to_vec()`, SST `get_versions
values.value(row).to_vec()` — even on pure cache hits; `batch_get_arrow` ("eliminates all
per-value memcpy") reachable only from the non-default ForStRsStateExecutor. q9 tuning floor
2001.2s vs bar 1776s = 225s; gets are the warm-path CPU at ~33-45K probes/s.

**⚠ DESIGN CORRECTED (2026-06-11 04:00, contract read):** `batch_get_arrow` is NOT a
drop-in — its zero-copy covers ONLY active-memtable hits (BinaryBuilderSink, db.rs:8442-8466);
everything else falls back to PER-KEY get_internal with NO batched SST phase (no file
grouping, no L0 short-circuit) → would REGRESS q9's cold-heavy regime. Do NOT swap.

**Correct design — SINK-THREAD `batch_get_vectorized`:**
1. New engine method `batch_get_vectorized_into(cf, keys, read_seq, out: &mut dyn BatchValueSink)`
   where BatchValueSink appends (slot_idx, present, value_bytes) — phases resolve as today but
   Put values flow via the sink: memtable hits use the EXISTING get_into/ValueSink machinery
   (no Vec); SST/KvBlock values still own one copy (borrowing across block decode is the
   deeper lifetime work — defer) but skip the SECOND copy by writing into the sink's segment.
2. FFI: frs_vectorized_batch_get builds a SegmentSink over the caller's out_data/out_offsets
   (running offset; on overflow return BUFFER_TOO_SMALL exactly as today's retry contract).
   Engine Vec<Option<Vec<u8>>> materialization deleted from the hot path.
3. Env gate FRS_ZERO_COPY_GET=1 for A/B; old path kept until gates pass.
4. Rust suite + 534 Java + q8/q11 10M exactness gates.
5. A/B q9@100M routing+200K back-to-back. Target ≤1850s (one copy saved per resolved value
   at 33-45K probes/s, plus zero-alloc memtable hits).
6. Bar cleared → q17/q3/q8 no-regress → default-on → GHA → record.

**Also queued after:** q20 OPT-N04 (engine merge-operator for the join count map — kills the
per-record dependent GET; q20's biggest lever), q7 jar-vs-day attribution run, validate
drain-200K on q7/q17/q20 before folding into defaults.
