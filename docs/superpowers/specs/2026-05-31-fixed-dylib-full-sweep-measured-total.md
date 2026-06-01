# Measured full q0–q22 total — fixed dylib (read-amp pread + L0 prune + cache hook)

Date: 2026-05-31
Status: AUTHORITATIVE MEASUREMENT. Binding metric (q0–q22 total wall-clock,
forst-rs/S3 vs rocksdb/local-disk, 8c/32g, ckpt-ON, 100M).

## Method

- RocksDB baseline: the already-measured, valid run (rocksdb code unmodified) —
  `/tmp/sweep-20260531-065538/results.OLDCODE.tsv`, 4238.1s over the 20
  rocksdb-measurable queries (q6/q10/q13 are NA for BOTH backends — q6 is a
  SQL-validation "rownum" error, q10/q13 are file-sink crash-loops — symmetric,
  excluded).
- forst-rs/S3: full sweep on the **fixed dylib** (whole-SST `pread`
  `get_range` + `may_contain_range` L0 prune + `FRS_BLOCK_CACHE_MB` hook;
  sha 548d954…), `scripts/sweep-sql.sh`, MAXSEC=700 (heavy collapses cap at
  700s and report NA). `/tmp/sweep-forstfix-163133`.

## Per-query (ms→s)

| q | rocksdb | forst-rs | rk/fr |
|---|--------:|---------:|------:|
| q0 | 22.3 | 24.9 | 0.90× |
| q1 | 22.4 | 24.4 | 0.92× |
| q2 | 20.7 | 22.9 | 0.90× |
| q3 | 33.3 | 40.9 | 0.81× |
| q4 | 260.8 | **NA (collapse)** | — |
| q5 | 109.7 | 486.8 | 0.23× |
| q7 | 1502.6 | **NA** | — |
| q8 | 32.3 | 72.3 | 0.45× |
| q9 | 534.3 | **NA** | — |
| q11 | 153.8 | **NA** | — |
| q12 | 35.2 | 38.8 | 0.91× |
| q14 | 21.4 | 22.7 | 0.94× |
| q15 | 267.7 | **NA** | — |
| q16 | 335.2 | **NA** | — |
| q17 | 57.2 | 352.8 | 0.16× |
| q18 | 217.2 | **NA** | — |
| q19 | 141.9 | **NA** | — |
| q20 | 392.1 | **NA** | — |
| q21 | 44.4 | 45.2 | 0.98× |
| q22 | 33.5 | 35.3 | 0.95× |

(q6/q10/q13 NA for both — excluded.)

## Totals

- **Both-finished subset (12 queries): rocksdb 432.5s vs forst-rs 1167.0s =
  0.37×** (forst-rs ~2.7× *slower*).
- **Full (20 rocksdb-measurable queries; forst NA capped at 700s — a LOWER bound
  on forst-rs time): rocksdb 4238.1s vs forst-rs ≥ 7467.0s = ≤ 0.57×** (forst-rs
  ≥ ~1.76× *slower*).

## Verdict

The binding goal (≥ 3× faster total) is **measuredly not met** — forst-rs/S3 is
*slower* overall, and **9 heavy / large-state queries collapse entirely**
(q4, q7, q9, q11, q15, q16, q18, q19, q20). Even the light queries that finish
run 0.81–0.98× (marginally slower: FFM + JDK25 + S3-stat overhead). No query is
faster than rocksdb/local.

This is consistent with the root cause established this session (see
`2026-05-31-whole-sst-pread-prefix-iterator-wall.md`): once keyed state spills
past the in-RAM working set, every prefix-iterator probe pays block
decompress + **Arrow-IPC RecordBatch decode** from S3-backed SSTs — a per-access
tax RocksDB (plain byte-slice blocks on the OS page cache, local disk) does not
pay. The working set of heavy joins exceeds the 8c/32g RAM envelope, so the
decode cost is unavoidable and the queries collapse.

The fixed dylib's improvements (the `pread` whole-SST read removing 64× read
amplification; the decode-free L0 prune) are real and correctness-clean (578
unit tests green) and help the pre-spill phase, but cannot overcome the
structural ceiling.

## The lever that would change this verdict

A deep decode-path redesign, the only thing that attacks the measured wall:
- **Prefix bloom** in the SST (the existing per-full-key bloom cannot answer a
  prefix-existence query) → skip within-block decode for absent join keys, and
- **lazy/partial Arrow column decode** for scans (decode only the key column to
  locate the range; defer value decode to matched rows).

Both are SST format + reader changes requiring format versioning and rigorous
correctness validation (a wrong bloom → silent data loss, violating the
zero-tolerance bar) — a directed effort, not a blind autonomous deploy.
