# Clean 2GB all-fix sweep — authoritative baseline BEFORE streaming snapshot

Date: 2026-06-01
Status: MEASURED (old dylib, 2 GB memtable, ckpt-ON). This is the baseline the
streaming-snapshot lever must beat.

## Result

forst-rs (ckpt-ON, S3, 2 GB writebuffer) vs RocksDB (local) on 100M NEXMark:

Queries forst-rs FINISHES (10), ratio = rocksdb/forst:
  q0 0.92  q1 0.93  q2 0.91  q3 0.98  q8 0.95  q12 0.93  q14 0.94
  q16 0.86  q21 0.94  q22 0.95   → subtotal 600.8s vs 673.9s = 0.89x

Queries forst-rs CAPS/NA (10) that RocksDB finished:
  q4 q5 q7 q9 q11 q15 q17 q18 q19 q20
  (RocksDB spends 3637s on these — they were forst-rs's 22-35x WINS in the
  ckpt-OFF v6f sweep; q7 22x, q9 35x, q16 32x, q20 28x.)
  (q10/q13 RESTARTING = NA-baseline queries, NA on RocksDB too; q6 NA both.)

## Diagnosis (consistent with the S3-not-the-bottleneck finding)

The 2 GB memtable spills heavy-query state to S3 L0 SSTs → each join probe pays
the decompress + Arrow-decode tax (the ckpt-ON collapse). The light/medium
queries (state fits 2 GB) run at ~0.89x — close but not winning. The heavy
queries cap because their state vastly exceeds 2 GB and collapses on spill.

The 2 GB ceiling was forced by the snapshot OOM: a 4 GB memtable + the old
snapshot path (which materialized the whole memtable as a Vec<RecordBatch> THEN
a second serialized Vec<u8>, ~3x peak) blew past 32 GB across 4 concurrent join
subtasks (NoResourceAvailableException = TM died).

## Next: validate the streaming-snapshot lever (landed this session)

The streaming snapshot (2026-06-01-streaming-memtable-snapshot-halves-ckpt-memory.md)
cuts snapshot peak to memtable + one ~1.5 GB batch. With it, writebuffer.size
can go to 4096mb (WBM still caps aggregate at 8 GB) so heavy-query state stays
RESIDENT (q4 already showed 554 K/s > RocksDB while resident). Validation steps:
1. cargo build --release -p forst-rs-ffi  → deploy dylib to $FLINK_HOME/lib
2. writebuffer.size 2048mb → 4096mb in config-forst-rs.yaml.tpl
3. re-run q4/q7/q9 (the capped heavy queries) → confirm no OOM + they finish
   resident (no spill-flush rate collapse).
If 4096 OOMs, back off to 3072 — but streaming removes the doubling that was
the OOM, so 4096 should hold.
