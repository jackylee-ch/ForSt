# PMC decision — q4 active-memtable index: arena skiplist vs BTreeMap (and the q4 floor)

**Date:** 2026-06-04
**Decision asked:** should forst-rs take a `#![forbid(unsafe_code)]` exception to replace the active-memtable
ordered index (`std::BTreeMap`) with a bespoke contiguous **arena skiplist** (RocksDB-style), to cut the q4
active-cursor seek (~724 ns, ~31% of the floor per-probe cost)?

**Recommendation: NO — refuted by spike before the vote.** Evidence below. The gate (spike the structure,
don't vote on the promise "this skiplist is different") did its job: the promise is false on the evidence.

## Why (measured, not argued)
1. **Arena-skiplist spike** (`/tmp/arena-skiplist-spike`, N=4M × 36-byte keys, 2M seeks, all 3 verified to
   agree): **BTreeMap 742 ns/seek · crossbeam SkipMap 2289 · index-linked contiguous arena skiplist 3397
   (WORST).** BTreeMap is already the fastest. A fan-out-2 skiplist has ~22 levels vs BTreeMap's ~6–7
   high-fan-out levels ⇒ more cache-miss hops; and keys are read in sorted order ≠ arena insertion order ⇒
   scattered key reads even with a contiguous arena. (This is the SAME reason the 2026-06-03 SkipMap→BTreeMap
   revert happened; the spike confirms a contiguous arena does not escape it.)
2. **Cross-validation:** the live `CURSOR_DIAG` scan (724 ns) ≈ the spike's BTreeMap (742 ns) ⇒ the
   active-cursor IS the BTreeMap seek, already optimal.
3. **Memtable-size lever (b):** the seek is O(log N) — a 4× smaller memtable (256 vs 1024 MiB) gave 740 vs
   724 ns (no change), so the seek can't be tuned away by sizing; smaller memtables only add flush/fan-out.
4. **It's not even the gap:** RocksDB's own memtable skiplist pays the same class of O(log N) seek cost. So
   the active-cursor is not the forst-vs-RocksDB differentiator.

Steelman: a node-inlined-key, p=0.25 arena skiplist is the strongest variant; the burden of proof is a spike
of THAT beating 742 ns. The fan-out argument + "lock-free insert is moot single-threaded-per-slot" make the
prior strongly against. Until such a spike exists, no unsafe exception is justified.

## What this leaves for q4 (see `2026-06-04-q4-decay-autopsy.md` for the full chain)
- Active seek (~724 ns): IRREDUCIBLE / not the gap → no action.
- **Resident-shadow tier** (the one forst-SPECIFIC per-probe tier RocksDB lacks): the only structural
  candidate, but the local-warm bypass is **marginal** (~340 ns net — the bloom is a wash, relocated to
  Tier-3, not saved), **regime-risky** (cache-miss → S3 fallback = the prior shadow-skip revert), and needs
  a strict cross-tier MVCC gate. Parked; revisit only if a confirmed-positive net + reliable local-only gate
  are established first.
- SST fan-out (~25%): where C/compaction/cache already operate. Lock stalls (<1%) + clone (~1%): negligible.

**Net:** post-C, forst-rs q4 is near the achievable floor for its architecture (C/B1/get_range_into shipped
the real safe wins: read_at −12–14 %, decode-side → 0, q4 floor 197K→230K). The residual gap to RocksDB's
flat curve is structural (RocksDB has no resident-shadow tier + a single merged-iterator read path), and no
policy-clean throughput lever — and no justified unsafe lever — remains for q4. Recommend the PMC close the
arena-skiplist item as refuted and NOT open an unsafe exception on current evidence.

## Default-flip CORRECTNESS close-out (the real remaining gate — not perf polish)
C was flipped to the writer default (commit f375ef9d6). The #31 5/5 result is a PERF pass on 5 scan-heavy
queries; it does NOT close the **correctness** risk surface the flip creates: every deployment now writes v2
by default, so any rarely-exercised **Arrow-assuming cold path** in a query NOT yet tested could silently
mis-read a v2 block. Performance on the 13 untested queries is masked by source-bound queries; **correctness
is not** — so the gate is a v1-vs-v2 OUTPUT diff across all q0–q22, not a TPS comparison.
- **Cold-path code audit (2026-06-04) — de-risks it:** NO production path assumes Arrow blocks. `read_block_at`
  (the only v1-only API; errors on KV) has ZERO production callers (both hits = the error string + a
  v1-pinned test). Every `decode_data_block` / `DecodedBlock::Arrow` use is a test or sits INSIDE the
  centralized `read_decoded_block` dispatch that also handles KV. The FFI Arrow exports
  (`frs_batch_get_arrow`/`prefix_scan_arrow`) build Arrow from value bytes via the dispatched get/scan, so
  they're format-agnostic. ⇒ the read surface is centrally dispatched with no v1-assuming bypass; cold-path
  correctness risk is LOW and the full sweep is CONFIRMATION, not discovery.
- **Status:** NOT urgent — `FRS_SST_KV_BLOCK_FORMAT=0` + the honest coverage caveat cover the interim — but
  it is the NECESSARY gate before relying on the v2 default in production. Tracked so the 5/5 result does
  not let it slip indefinitely. (q0–q22 v1-vs-v2 output diff.)
