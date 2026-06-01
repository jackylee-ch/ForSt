# 4GB memtable blocker: MapStateCache i32 offset overflow (NOT the memtable)

Date: 2026-06-01
Status: DIAGNOSED. Fix = run with cache-bypass (already-established path); the
i32 overflow is in the Java off-heap MapStateCache, which cache-bypass skips.

## Symptom

With writebuffer.size=4096mb + streaming snapshot, q9 ran clean past 36M records
and many checkpoints (NO OOM — streaming-snapshot fix validated), then the Join
task FAILED + restarted at ~403s:

```
java.lang.IndexOutOfBoundsException: Out of bound access on segment
  MemorySegment{ byteSize: 4294967296 }; new offset = -2147483409; new length = 233
  at MapStateCache.keyEquals(MapStateCache.java:666)
  at MapStateCache.findRow(MapStateCache.java:487)
  at MapStateCache.put(MapStateCache.java:266)
  at ForStRsMapStateV2.asyncPut(ForStRsMapStateV2.java:339)
```

`-2147483409 + 2^32 = 2147483887` (~2.147 GiB) → a >2 GiB offset into the cache's
4 GiB off-heap MemorySegment is computed/stored as a SIGNED i32 and wraps
negative. `byteSize = 4294967296` = exactly 4 GiB.

## Root cause — it's the CACHE, not the memtable

The failing segment is the **Java off-heap `MapStateCache`** backing store (the
per-record composite-key cache), NOT the Rust memtable. The Rust memtable read
path is safe at 4 GB: FFM iterator/batch_get return chunks bounded to
MAX_BATCH_BYTES = 1.5 GiB (Arrow i32 offsets stay < 2 GiB per chunk), and the
memtable indexes its own Vec data with usize. So the memtable is fine; the
MapStateCache's internal row addressing is i32 and overflows once the cache
exceeds 2 GiB.

This run did NOT set `FRS_DISABLE_MAPSTATE_CACHE=1`. The established cache-bypass
path (q11 2.39× faster — it routes per-record MapState GET/PUT to the engine's
batched SIMD batch_get_vectorized) bypasses MapStateCache ENTIRELY, so the i32
overflow cannot occur on that path. It is also the faster path and the intended
default.

## Why 2 GB "worked" and 4 GB crashed

At writebuffer.size=2048mb the cache stayed < 2 GiB so its i32 offsets never
overflowed (the 2 GB sweep capped on heavy queries but did not crash here). At
4096mb the cache grows past 2 GiB → overflow. So the 2 GB ceiling was masking
TWO independent limits: the snapshot OOM (now fixed by streaming) AND the
MapStateCache i32 offset (avoided by cache-bypass).

## Fix / next

1. Re-validate q9 at writebuffer.size=4096mb WITH `FRS_DISABLE_MAPSTATE_CACHE=1`
   (streaming snapshot avoids OOM; cache-bypass avoids the i32 overflow; Rust
   memtable read is i64/usize-safe). Expect q9 to stay resident (no spill
   collapse) and beat RocksDB's 534s.
2. Make cache-bypass the DEFAULT in ForStRsMapStateV2 (remove the env-var gate)
   so 4096mb is safe regardless of env — pending Java-jar rebuild.
3. (Lower priority — dead code on the bypass path) widen MapStateCache's row
   offset from int to long if the cache is ever kept.
