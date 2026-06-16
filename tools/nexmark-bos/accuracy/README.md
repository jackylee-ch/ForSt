# NexMark BOS Accuracy Harness

This directory is independent from the BOS performance runners. It validates
correctness with a fixed 100K CSV input generated once from NexMark datagen, then
replays the same CSV through `forst-rs` and `rocksdb` with BOS-backed state or
checkpoints.

The harness may read Hadoop `core-site.xml` from inside the script to load
`fs.bos.access.key` and `fs.bos.secret.access.key` into environment variables for
the test process. It never prints those values. If `S3_ACCESS_KEY` and
`S3_SECRET_KEY` are already set, the script uses those instead.

## What It Checks

1. Generate fixed CSV input under local scratch:
   - `person`
   - `auction`
   - `bid`
2. Run each query twice:
   - `forst-rs` with remote state under BOS via S3-compatible URI
   - `rocksdb` with local RocksDB and checkpoint storage under `bos://`
3. Capture query output as changelog CSV:
   - `raw-changelog.csv`
   - `final-materialized.csv`
4. Compare materialized results between the two backends.

For update/retract queries, plain filesystem CSV is not enough because it cannot
represent changelog semantics. The runner captures `+I`, `+U`, `-U`, and `-D`
rows and materializes them offline before comparison.

## Basic Usage

```bash
cd ~/code/stczwd/ForSt

export IMG=nexmark-bos:forst-rs-q4-latest-20260615
export FLINK=/home/users/lijunqing/workenv/flink-2.2.1
export HADOOP_HOME=/home/users/lijunqing/workenv/hadoop-3.3.6
export HADOOP_CONF_DIR=/home/users/lijunqing/workenv/hadoop-3.3.6/etc/hadoop
export BOS_HADOOP_FS_JAR=/home/users/lijunqing/workenv/hadoop-3.3.6/share/hadoop/common/lib/bos-hadoop-fs-2.0.0.jar

# Set these in the shell; do not print them.
export S3_ENDPOINT=http://s3.bj.bcebos.com
export S3_BUCKET=tal-poc-namespace
export S3_REGION=us-east-1
export S3_PREFIX=jackylee/test/nexmark-bos-accuracy
# Optional. If unset, run-accuracy.sh loads them from core-site.xml internally.
export S3_ACCESS_KEY=...
export S3_SECRET_KEY=...

export REMOTE_PARENT=bos://tal-poc-namespace/jackylee/test
export LOCAL_BASE=/tmp/jackylee/nexmark-bos-accuracy

bash tools/nexmark-bos/accuracy/run-accuracy.sh q4
```

Run a subset:

```bash
QUERIES="q3 q4 q5 q9 q20" bash tools/nexmark-bos/accuracy/run-accuracy.sh sweep
```

Run all scoped queries:

```bash
QUERIES="q0 q1 q2 q3 q4 q5 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22" \
  bash tools/nexmark-bos/accuracy/run-accuracy.sh sweep
```

## Output Layout

```text
/tmp/jackylee/nexmark-bos-accuracy/<run-id>/
  input-100k/
    person/
    auction/
    bid/
  results/
    q4/
      forstrs/
        raw-changelog.csv
        final-materialized.csv
      rocksdb/
        raw-changelog.csv
        final-materialized.csv
      compare.txt
  verdicts.tsv
  compare.tsv
  SUMMARY.md
```

## Notes

- `q12` is processing-time based. The comparator reports an invariant verdict
  instead of strict row equality by default.
- In the 2026-06-16 BOS sweep with 100K fixed CSV input, all scoped queries
  except `q9` matched by materialized row equality; `q12` passed the invariant
  check. `q9` completed on both backends but produced a small row-level diff and
  should be analyzed separately before treating it as accuracy-clean.
- The harness uses `nexmark-bos-accuracy` names for containers, networks, local
  scratch, and remote prefixes to avoid touching concurrent performance tests.
- Cleanup is prefix-scoped; it only targets `nexmark-bos-accuracy*` resources.
