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

**Tasks:**
1. Read `batch_get_arrow` (engine + FFI) end-to-end; confirm its output contract (Arrow
   buffers + validity) matches what `VectorizedExecutor.executeGets` decodes (outOffsets/
   outData/outValidity). If contracts match → swap `vectorizedBatchGet` linker target to the
   arrow variant behind env `FRS_ZERO_COPY_GET=1` (A/B-able), per-row completion unchanged
   (completeGet already deserializes from segments).
2. Rust suite + 534 Java tests + q8/q11 10M exactness gates.
3. A/B: q9@100M routing+200K with/without FRS_ZERO_COPY_GET (back-to-back). Target ≤1850s.
4. If contract mismatch: minimal adapter on the FFI boundary (no Java-side changes), retest.
5. If bar cleared: q17/q3/q8 no-regress + default-on + GHA + record.

**Also queued after:** q20 OPT-N04 (engine merge-operator for the join count map — kills the
per-record dependent GET; q20's biggest lever), q7 jar-vs-day attribution run, validate
drain-200K on q7/q17/q20 before folding into defaults.
