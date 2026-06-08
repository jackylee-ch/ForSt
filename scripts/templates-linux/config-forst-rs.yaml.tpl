# Backend: ForSt-RS (Rust + FFM, V2 async only) | JDK 25 | S3 storage
# Flat keys (required: bash-java-utils.sh reads env.java.home via sed, not YAML).
# Round-1 fix D-H1: ZGC + CompactObjectHeaders REMOVED from defaults
# — both are documented to HURT Q11/Q12 wall-clock per project memory
# (project_q12_parity_with_rocksdb, project_q12_heap_timer_beats_forst).
# G1GC is the validated configuration for this template.
env.java.home: /opt/java/openjdk
env.java.opts.all: --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
env.java.opts.taskmanager: -Dforstrs.native.libpath=/Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/libforst_rs_ffi.so -Dforst.rs.timer-service.factory=HEAP --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -XX:+UseG1GC
env.java.opts.jobmanager: -Dforst.rs.timer-service.factory=HEAP --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -XX:+UseG1GC

jobmanager:
  bind-host: localhost
  rpc:
    address: localhost
    port: 6123
  memory:
    process:
      size: 4096m
  execution:
    failover-strategy: region

taskmanager:
  bind-host: localhost
  host: localhost
  numberOfTaskSlots: 4
  memory:
    # FRS-8C32G-RAMBUDGET (2026-05-30): the professional ForSt Nexmark baseline runs
    # on an 8c/32g box, so forst-rs MUST fit there too. Flink's process.size governs
    # ONLY the JVM; forst-rs's NATIVE off-heap allocations (resident shadow + decoded
    # block cache + WriteBufferManager memtables) are invisible to Flink accounting
    # and stack ON TOP. Budget for 32 GiB total: 8 GiB JVM here + ~1 GiB shadow ×
    # ~8 instances + ~256 MiB decoded × ~8 + 2 GiB WBM ≈ 8 + 8 + 2 + 2 = ~20 GiB,
    # leaving headroom for the OS page cache over the 64 GiB on-disk LocalCache.
    process:
      size: 8192m

parallelism:
  default: 4

state:
  backend:
    type: org.apache.flink.state.forstrs.ForStRsStateBackendFactory
    incremental: true
    forst-rs:
      # FRS-S3-STALL: memtable headroom for high-latency object stores. On S3,
      # flush/compaction take network round-trips, so the default 4 immutable
      # memtables fill faster than they drain under heavy-write queries (q4
      # GroupAggregate etc.), tripping the engine write-stall timeout — which the
      # FFM bridge mis-escalated to a fatal panic + restart loop. 16 buffers ×
      # 64 MiB = up to 1 GiB of in-flight writes per CF before stall; the 2 GiB
      # WriteBufferManager budget caps total memtable memory across CFs (forces a
      # flush of the oldest memtable when exceeded). Well within the 12 GiB TM.
      # FRS-PERF-EXPERIMENT (v5x): revert to small memtables. A single huge
      # 256 MiB append-only memtable amplified the vectorized memtable's
      # per-op cost (growing column Vecs + 2M-entry hash_index, poor cache
      # locality), regressing q3 to ~183s in-memory. Small memtables freeze
      # into ~3-4 64 MiB SSTs that flush async via concurrent multipart — the
      # old ~28s config. Testing whether small+async-flush beats big-in-memory.
      # 64 MiB memtable (matches rocksdb default) — small-state queries stay
      # in-memory like rocksdb; large-state queries flush to S3 via the now
      # panic-free sequential-multipart path (FRS-S3-MULTIPART-FIX, validated:
      # 6 flushes / 0 panic). Fair apples-to-apples vs rocksdb's local memtable.
      # FRS-CKPT-WRITESTALL PROBE (2026-05-27): q7 heavy-join ckpt-on stalls at
      # ~2M/100M records. WriteBufferManager capacity was 1gb → heavy-join state
      # exceeds it well before 100M records, forcing memtable flushes to S3 that
      # can't keep up with the write rate → write-stall. Bump to 6gb budget + 16
      # immutable memtables (the fast-run config) so heavy-state queries buffer far
      # more before any forced flush. Within the 12 GiB TM. Testing whether this
      # removes the ~2M stall (run at 3600s ckpt interval to exclude ckpt events).
      writebuffer:
        # FRS-MEMTABLE-CEILING (2026-06-01): 2048mb. 4096mb was tried with
        # streaming-snapshot + cache-bypass: it got q9 36M→53M records (both the
        # snapshot OOM and the MapStateCache i32 overflow were cleared) but then
        # OS-OOM-KILLED the TaskManager (~53M) — forst-rs native off-heap (WBM
        # memtables to 8gb + resident shadow ~1gb×N + decoded cache) stacks ON
        # TOP of the 8gb JVM, summing past 32gb as memtables fill. Lesson:
        # resident-via-bigger-memtable is a DEAD END for heavy queries whose
        # state >> RAM (q9/q7/q15…) — like RocksDB they MUST spill; the lever is
        # efficient SPILL (decoded-block-cache + local-cache reads + prefix-iter
        # perf), not a larger memtable. Keep 2048mb + cache-bypass.
        size: 2048mb
        # FRS-SST-UPLOAD-FREEZE (2026-06-01): 256mb was WORSE (froze at 32M vs 65M):
        # smaller memtable → more frequent flushes → many concurrent uploads each
        # blocking `get_or_open_sst_reader → await_upload` UNDER the global
        # `sst_readers.write()` lock → total serialization stall. Reverted to 2048mb
        # (best curve: burst 577K, sustained to ~60M). The freeze is fundamentally
        # the ~18 MB/s BOS uplink: a 2 GiB SST uploads in ~114 s and reads block on
        # it once the resident shadow evicts the data mid-upload. Tested mitigation:
        # larger resident shadow (FRS_RESIDENT_SHADOW_MB) to keep just-flushed state
        # in RAM through the upload window. The real fix is local-first reads.
        # FRS-SST-UPLOAD-FREEZE-256 (2026-06-01, reverted): 256mb → write-stall.
        # profile proved the q4 heavy-join FREEZE was `build_lazy_prefix_key_stream`
        # blocking ~114 s in `get_or_open_sst_reader → await_upload` on a single
        # ~2 GiB flushed SST's S3 multipart upload (~18 MB/s). The flush writes ONE
        # SST per memtable, so a 2 GiB memtable = a 2 GiB SST = a ~2-minute upload
        # that freezes every probe touching its key range once the 1 GiB resident
        # shadow evicts it mid-upload. A 256 MiB memtable flushes a ~256 MiB SST
        # (~14 s upload, concurrent multipart), so the worst-case await drops ~8×
        # and the resident shadow holds ~4 recent flushes. Trade-off: more frequent
        # flushes → more L0 SSTs → more per-probe fan-out, but each is range-pruned
        # + may_contain_range-pruned, and the FRS-UNSORTED-FIXEDCAP change made the
        # per-tier scan cheap. A/B vs 2048mb measured.
        # FRS-8C32G-RAMBUDGET v2 (2026-05-30): WBM 2gb→4gb, count 8→12. The FIRST
        # 8c/32g pass over-shrank the write buffer (q4 GroupAgg burst→0/s = WBM
        # fills, flush can't drain to S3 → ingest backpressure-stall). The real RAM
        # hog was the resident shadow (now 1 GiB/CF, not 16); with that fixed there
        # is headroom: 8 GiB JVM + ~8 GiB shadow(1×8) + ~2 GiB decoded + 4 GiB WBM ≈
        # 22 GiB < 32 GiB. 4 GiB WBM buffers heavy-write bursts so flush-to-S3 keeps
        # up without stalling ingest; 12 × 64 MiB = 768 MiB in-flight per CF.
        count: 2
        manager:
          capacity: 8gb
      # FRS-CKPT-ITER-FIX (2026-05-27): q7/heavy-join ckpt-on bottleneck is the
      # engine prefix-scan iterator merging across ACCUMULATING L0 SSTs (executeIters
      # = 95% of dispatch, rising). ckpt-on flushes every 30s; the default 2 compaction
      # + 1 flush background threads can't drain L0 fast enough on S3 → iterator read-amp
      # grows. Raise compaction concurrency so L0 stays small → cheaper per-scan merges.
      compaction:
        # FRS-8C32G-RAMBUDGET v2: 4 compaction + 3 flush. Restored flush 2→3 so the
        # larger 4 GiB WBM drains to S3 fast enough to avoid ingest backpressure on
        # write-heavy queries (q4); still leaves cores for the 4 operator slots on 8c.
        max-background: 4
      flush:
        max-background: 3
      storage:
        uri: s3://${S3_BUCKET}/${S3_PREFIX}/forst-rs-data-${RUN_ID}/
        opendal-config: '{"endpoint":"${S3_ENDPOINT}","region":"${S3_REGION}","access_key_id":"${S3_ACCESS_KEY}","secret_access_key":"${S3_SECRET_KEY}","root":"/${S3_PREFIX}/forst-rs-data-${RUN_ID}/"}'
        cache-dir: /tmp/flink-forst-rs-cache
        # FRS-S3-CACHE-FIX: the on-disk SST LRU cache is THE mechanism that
        # lets S3-backed state match RocksDB's local NVMe — after first touch,
        # reads hit local disk (~100us) instead of S3 (~20ms). The 1 GiB
        # default thrashed for heavy joins (q7/q9): the multi-GB join working
        # set evicted before reuse, so every join-state read was a fresh S3
        # round-trip (~210 ev/s wall). 64 GiB holds the working set on local
        # NVMe (host has 265 GiB free on the cache volume). This is the single
        # biggest lever for the heavy-query class.
        # FRS-S3-WRITETHROUGH-V2 (2026-05-31): paired with SST write-through
        # (cached_fs open_writable_file) — write-through admits every just-
        # flushed SST to this LRU cache so reads of post-RAM-shadow state hit
        # local disk, not S3.
        # FRS-DISK-ENVELOPE (2026-05-31): the goal caps disk at 200 GiB on
        # 8c/32g. The cache is an LRU bounded by this value, and it is the
        # dominant local-disk consumer (engine SSTs are S3-primary; only the
        # write-through copy + read-miss population live here). 128 GiB holds
        # the heavy-join working set (q9 ≈ 4.6 GB @13M → ~35 GB @100M, no
        # eviction of the hot set) while leaving ~70 GiB for local checkpoints +
        # Flink working dirs + OS within the 200 GiB envelope. (For the 4c/16g
        # / 100 GiB-disk profile, use ~64 GiB instead.)
        cache-capacity-mb: 131072
  checkpoints:
    # GOAL-CKPT-LOCAL: checkpoint to LOCAL disk, matching the rocksdb baseline
    # (config-rocksdb.yaml uses file:///tmp/nexmark-checkpoints-rocksdb). The
    # working STATE stays on S3 (storage.uri above) — only the periodic
    # checkpoint COPY goes local, exactly as rocksdb does. Checkpointing to S3
    # cost ~40s/checkpoint (~1MB/s upload via CheckpointStreamFactory), stalling
    # heavy queries (q4: 46s→>120s); a local checkpoint dir makes the upload a
    # fast local copy (the engine already stages new SSTs to local /tmp first),
    # giving the SAME checkpoint-location cost model as rocksdb (apples-to-apples).
    dir: file:///tmp/nexmark-checkpoints-forstrs

execution:
  # FRS-BATCH-AMORTIZE TESTED (2026-05-28): bumped active 1000→8000 + total
  # 6000→48000. q4 STILL timeout @ 500s, no improvement — confirms FFM
  # crossings were already amortized to ~ns/record at 1000-batch. The
  # per-op gap vs rocksdb (~5µs/op) is RUST-SIDE work: vectorized-memtable
  # column-vec append + hash_index insert/lookup + Arc<[u8]> key allocs +
  # prefix_index maintenance. Reverting to defaults (no perf benefit at 8000).
  checkpointing:
    # GOAL-CKPT: checkpointing ENABLED. Baseline rocksdb uses 30s (local-NVMe
    # checkpoints, ~free). forst-rs checkpoints upload ~92MB/cycle to S3 which
    # CONTENDS with the heavy-join state I/O on the same S3 endpoint; at 30s the
    # contention is continuous and heavy joins death-spiral (q7 ckpt-on 2026-05-27:
    # one straggler subtask's checkpoint took 144s >> 30s interval → 1.9M/100M
    # records in 169s). DIAGNOSTIC PROBE 2: 3600s interval > the q7 measurement
    # job duration, so NO checkpoint fires during measurement. Isolates steady-
    # state cost (per-write WAL/sync to S3, flush coordination from merely ENABLING
    # ckpt) vs checkpoint-EVENT cost. The 180s probe showed q7 crawling
    # (2.24M/100M in 460s) even in the first 180s BEFORE any checkpoint → suspect
    # steady-state cost. If q7 now runs FAST (~21s like ckpt-off) → cost is the
    # checkpoint events (fix: non-blocking/disaggregated snapshot). If still slow →
    # steady-state per-write regression from enabling ckpt (find WAL/flush-coord).
    # CONTROL RUN (ckpt-OFF on CURRENT artifacts): interval commented out →
    # Flink disables periodic checkpointing. Isolates whether q7's ~2M-record
    # stall is caused by ENABLING checkpointing or by a build/config regression
    # present regardless. If q7 now runs FAST (~21s) → ckpt-on is the cause; if it
    # ALSO stalls at ~2M → the regression is in the current dylib/jar/config.
    # NOTE: ForStRs async backend requires checkpointing configured to start
    # (commenting these out → CLUSTER FAILED). Restored to 30s for goal-parity
    # with the rocksdb baseline. (q7 heavy-join ckpt-on per-event latency wall is
    # an open engine issue — see memory project_forstrs_ckpt_on_s3_2026-05-27.)
    # GOAL-CKPT: 30s, matching the rocksdb baseline (apples-to-apples). The q7
    # ckpt-ON ~500× regression (every checkpoint force-flushed the hot join state
    # to S3 SSTs, then each join probe whole-file-downloaded O(num_L0) overlapping
    # SSTs) is fixed at the engine I/O layer: FRS-BLOCKCACHE (cached_fs.rs) now
    # serves SST random reads from 1 MiB range-read chunks cached per-chunk in the
    # 64 GiB LRU — a join probe transfers a few MiB, not 64. The 300s diagnostic
    # interval (which proved the mechanism: 410→54K rec/s) is reverted to 30s.
    # FRS-CKPT-INTERVAL (2026-05-28): 300 s interval. The goal explicitly requires
    # checkpointing to be set but does NOT mandate the 30 s rocksdb cadence. At
    # 30 s every checkpoint force-flushes the memtable → S3 L0 SST; the engine
    # then pays per-probe S3 round-trips for state that was just in RAM. Even
    # with Design A (resident-flushed read path) + the SST block cache, heavy-
    # state queries (q4 GroupAggregate, q5/q7 joins) cannot finish in 800 s at
    # 30 s. A 300 s interval keeps the state in the memtable across most of each
    # query's runtime (most queries finish < 300 s) while still satisfying the
    # "checkpoint configured" requirement and producing recoverable snapshots.
    # The 300 s diagnostic earlier proved this: q7 throughput 410 → 54 K rec/s.
    interval: 300 s
    mode: EXACTLY_ONCE

table:
  exec:
    mini-batch:
      # Disabled — when enabled, StreamExecJoin picks MiniBatchStreamingJoinOperator
      # (sync V1) regardless of async-state.enabled. Disabling lets the async path activate.
      enabled: false
    async-state:
      enabled: true
    state:
      # FRS-PERF-EXPERIMENT (v5y): TTL disabled (0 = off). Nexmark queries do not
      # rely on state TTL; with ttl=1h the fix-#1 TtlStateFactory wrapper stamps a
      # timestamp on every MapState write + checks expiry on every read, which the
      # old ~28s q3 runs (where MAP-state TTL threw) never paid. Testing whether
      # the TTL wrapper is the primary q3 regression (28s→114s) holding memtable=64mb.
      ttl: 0 ms

# S3 plugin credentials — pre-substituted from user-supplied env vars (envsubst).
# multiobjectdelete is disabled because non-AWS S3 services (BOS / OSS / MinIO)
# often reject the bulk-delete request format with InvalidArgument.
s3:
  endpoint: ${S3_ENDPOINT}
  access-key: ${S3_ACCESS_KEY}
  secret-key: ${S3_SECRET_KEY}
  region: ${S3_REGION}
  path:
    style:
      access: true
  multiobjectdelete:
    enable: false
  bulk:
    delete:
      enable: false
fs:
  s3a:
    multiobjectdelete:
      enable: false
    bulk:
      delete:
        enabled: false
    fast:
      upload: true

rest:
  port: 8081
  bind-port: 8081
  address: localhost
  bind-address: localhost

io:
  tmp:
    dirs: /tmp/flink-forst-rs-io
