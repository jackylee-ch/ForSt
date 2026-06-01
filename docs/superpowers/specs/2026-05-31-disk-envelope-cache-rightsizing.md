# Disk-envelope right-sizing: on-disk cache 192→128 GiB for the ≤200G constraint

Date: 2026-05-31
Status: LANDED (config); applies to forst-rs queries in the in-progress sweep.

## Trigger

The /goal was re-issued with a new hard constraint: 100M NEXMark must run on
**8c/32g with at most 200 GiB disk** (or 4c/16g with ≤100 GiB). Both the RocksDB
baseline and forst-rs+S3 must fit this envelope.

## Problem

The SST write-through V2 work (2026-05-31) raised the on-disk LocalCache to
**192 GiB** (`cache-capacity-mb: 196608`) on the assumption of ~510 GiB free
disk. Under the new ≤200 GiB envelope that is invalid: the cache is an LRU
bounded by `cache-capacity-mb` and is the dominant local-disk consumer, so a
192 GiB cap alone nearly fills 200 GiB, leaving no room for local checkpoints,
Flink working dirs, or OS headroom.

## Why 128 GiB is safe for performance

- forst-rs writes SSTs **S3-primary** (buffered → async upload); the only local
  disk consumers are the write-through SST copies + read-miss cache population
  (both in the LocalCache) + small checkpoint/working dirs.
- The heavy-join working set is modest: q9's cache held **4.6 GB at 13M
  records**, extrapolating to ~35 GB at 100M. A 128 GiB cap holds that with no
  eviction of the hot read set (the eviction churn is what forced the
  2026-05-27 write-through revert; 128 GiB ≫ 35 GB avoids it).
- So 128 GiB preserves the write-through win (q9 collapse eliminated: 13M@583s
  vs 3.3M wall) while fitting the envelope: cache ≤128 GiB + checkpoints +
  working ≈ ≤135 GiB ≪ 200 GiB.

## Change

`config-forst-rs.yaml.tpl`: `cache-capacity-mb: 196608` → `131072` (128 GiB),
with a comment documenting the 200 GiB envelope and the 64 GiB value for the
4c/16g / 100 GiB profile.

Applied mid-sweep safely: `measure-sql.sh` re-substitutes `config.yaml` from the
template per query, and the sweep had **not yet reached any forst-rs query**
(still on the rocksdb baseline), so every forst-rs query in this run uses the
corrected 128 GiB cap.

## Verification / monitoring

- The LRU cap structurally bounds the cache; no runtime enforcement needed.
- During the forst-rs half, watch `df` and `du /tmp/flink-forst-rs-cache`;
  expected steady-state ≤ ~135 GiB total. (RocksDB baseline state lives in
  `/tmp/flink-rocksdb-io`; also expected well under 200 GiB.)

## Note

Stale per-test tmp artifacts (old q9 profiling logs/samples, prior sweep dirs)
were removed to free space, leaving only the active sweep dir.
