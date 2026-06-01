# 4GB memtable OS-OOM-kills the TM — resident is a dead end for heavy queries

Date: 2026-06-01
Status: MEASURED + STRATEGIC PIVOT. Reverted to 2048mb + cache-bypass.

## What was tried

writebuffer.size=4096mb + streaming snapshot + cache-bypass (FRS_DISABLE_MAPSTATE_CACHE=1),
q9 on S3. Both prior blockers were cleared:
- streaming snapshot → no snapshot-doubling OOM
- cache-bypass → no MapStateCache i32-offset overflow (the 36M crash)

Result: q9 progressed to ~53M records (vs 36M pre-bypass) at ~150-210 K/s, then
the TaskManager DIED at ~382s. JM log: HeartbeatTargetUnreachable + "Connection
refused" → TM process gone; then repeated NoResourceAvailableException (lost
slots). No hs_err file → OS OOM-kill, not a JVM OutOfMemoryError.

## Why it OOMs

`taskmanager.memory.process.size: 8192m` governs ONLY the JVM. forst-rs native
off-heap is INVISIBLE to Flink accounting and stacks on top:
- WriteBufferManager memtables: capacity 8gb (aggregate)
- resident shadow: ~1gb × N instances
- decoded block cache: ~256mb × N
- + 8gb JVM + OS page cache for the on-disk LocalCache

8 (JVM) + 8 (WBM) + ~8 (shadow) + 2 (decoded) ≈ 26gb + page cache → crosses 32gb
as memtables fill toward the WBM cap. writebuffer.size=4096 made single memtables
bigger so the WBM cap is reached chunkier/faster, tipping the TM over.

## Strategic pivot (the important part)

Raising the memtable to keep heavy-query state RESIDENT is a DEAD END. q9's join
state at 100M vastly exceeds any RAM budget — RocksDB wins q9 (534s) by spilling
efficiently to LOCAL DISK, not by holding it in RAM. Expecting forst-rs to hold
it resident just delays the spill and OOMs the TM.

So heavy queries (q4/q7/q9/q15/q17/q18/q19/q20 — the ones the 2GB sweep capped)
need an efficient SPILL path, NOT a bigger memtable:
- spilled SST reads served from LOCAL cache (done: read-amp + write-through)
- decoded-block cache (done: ~14-25% on q9)
- the REMAINING gap = the per-probe prefix-iterator constant (~7x rocksdb
  native): build_lazy_prefix_key_stream rebuilt every FFI iter-open (version
  snapshot + resident_flushed_visible + memtable prefix_scan + SST reader opens)
  + FFM crossing + Arrow encode + k-way merge, amplified by q9's ROW_NUMBER
  re-iterating each partition per record. This is the deep multi-session work.

## Kept / config

- writebuffer.size reverted to 2048mb (no TM OOM).
- cache-bypass (FRS_DISABLE_MAPSTATE_CACHE=1) KEPT — it is a real win (q11 2.39x,
  q8 0.45→0.99x) and clears the MapStateCache i32 overflow.
- streaming snapshot KEPT (correctness-clean, halves snapshot peak; useful if a
  future smaller-but-resident config is found).

## Next measurement

Re-run the heavy queries at 2048mb WITH cache-bypass (the 2GB sweep may not have
used bypass) to get the authoritative heavy-query-with-bypass total before
investing in the prefix-iterator perf work.
