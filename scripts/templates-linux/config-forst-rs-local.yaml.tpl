# Backend: ForSt-RS (Rust + FFM, V2 async only) | JDK 25 | S3 storage
# Flat keys (required: bash-java-utils.sh reads env.java.home via sed, not YAML).
env.java.home: /opt/java/openjdk
env.java.opts.all: --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
env.java.opts.taskmanager: -Dforstrs.native.libpath=/usr/local/lib/libforst_rs_ffi.so -Dforst.rs.timer-service.factory=FORSTRS -Dforst.rs.checkpoint.noflush=false --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -XX:+UseG1GC
env.java.opts.jobmanager: -Dforst.rs.timer-service.factory=FORSTRS --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -XX:+UseG1GC

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
    # FRS-Q9-NATIVE-HEADROOM (2026-06-15, PMC-1): carve Flink's process.size DOWN
    # so the forst-rs engine's NATIVE (off-heap, jemalloc-in-the-.so) allocation
    # fits the per-TM cgroup. Flink does NOT account for the engine's native bytes
    # in process.size — they live in the cgroup ON TOP of it. At process.size=12288m
    # the JVM (heap+managed+network+overhead) + engine native (~5-6 GiB at the q9
    # join peak) summed to ~17-18 GiB > the 16g/TM split cgroup -> end-of-run
    # OOM-kill (q9 died ~0.4M rows short). Lowering to 10240m leaves ~6 GiB cgroup
    # headroom for the engine native; the JVM still gets 4.25 GiB heap + 3.5 GiB
    # managed, ample for the Flink-side join/operator state (forst-rs keeps its
    # state in engine-native memory, not Flink managed). VALIDATED: q9 @100M @2x4c/
    # 16g SPLIT, KV-sep ON, uniform config -> FINISHED 1463.3s, EXACT out_rows
    # 91,813,372, peak cgroup ~15.2 GiB (was OOM at 16). Uniform across ALL forst-rs
    # queries (no per-query config); other queries have smaller native peaks so the
    # extra headroom is harmless. NO RAM added — pure budget re-partition.
    process:
      size: 10240m

parallelism:
  default: 4

state:
  backend:
    type: org.apache.flink.state.forstrs.ForStRsStateBackendFactory
    forst-rs:
      storage:
        uri: file:///tmp/flink-forst-rs-data/
        opendal-config: '{"root":"/tmp/flink-forst-rs-data/"}'
        cache-dir: /tmp/flink-forst-rs-cache
        # 2026-06-02 q7 SECOND wall: the local NVMe SST cache defaults to 1024 MB.
        # Heavy-join state crosses ~1 GB at ~21M records — exactly where q7's
        # throughput collapsed to ~100/s (cache full → reads hit the slow
        # eviction-fallback open). Size it to hold the full working set (disk has
        # ample headroom) so hot SSTs stay local. Mirrors the q9-collapse fix.
        cache-capacity-mb: 65536
      # 2026-06-02 PERF (heavy-join drain): the engine defaults flush every
      # 64 MiB, leaving ~16 resident memtables (FRS_ITER_DIAG: resident_shadowed=16)
      # against the 1 GiB resident-shadow cap — so every async-join probe's
      # get_arc resolve traverses ~16 tiers per key. A 256 MiB write buffer cuts
      # that to ~4 resident memtables (4× fewer tier traversals + 4× fewer/larger
      # L0 SSTs → fewer cold reader opens). WBM caps active+immutable at 512 MiB
      # so total RAM stays bounded (≈ 512 MiB memtable + 1 GiB resident + 512 MiB
      # block cache ≈ 2 GiB/engine) well within the 12 GiB TM across slots.
      writebuffer:
        # FRS-Q4-STABILITY TEST (2026-06-04): 256mb→1024mb + WBM 512mb→4096mb.
        # Root cause of q4 decay = per-probe SST fan-out grows as L0 SSTs
        # accumulate from frequent small-memtable flushes; bigger memtables flush
        # ~4× less often → ~4× fewer L0 SSTs → lower fan-out → higher/flatter
        # floor. RAM stays bounded (64 GiB box; caches capped).
        # (b)-TRADEOFF MEASURED 2026-06-04: 256mb gave scan 740ns ≈ 1024mb's 724ns
        # (seek is O(log N) → 4× smaller N saves only ~2 levels ~10%, lost in noise)
        # AND no floor gain + more flushes → REVERTED to 1024mb (better for fan-out).
        # FRS-8C32G-MEMORY-MODEL (2026-06-08): the q4-era 1024mb/4096mb were sized
        # for a 64 GiB box. On 8c/32g a join (q9/q20/q4) bursts the ENGINE to ~18.7 GB
        # (memtables 6.9 GB overrunning the cap + flush/compaction transient buffers)
        # → anon RSS 30 GB → 32g cgroup OOM-kill. Shrink the engine memory envelope:
        # 256mb memtables flush small+often (smaller flush buffers), WBM caps the
        # cross-CF memtable sum at 2 GB, fewer bg threads = fewer concurrent buffers.
        size: 1024mb
        count: 4
        manager:
          capacity: 4096mb
      compaction:
        max-background: 8
      flush:
        max-background: 4
      cache:
        block:
          capacity: 2048mb
      # NOTE: block cache left at the 256 MiB default. 2026-06-03: bumping block
      # cache (2 GiB) + background compaction (8) + flush (4) threads was TESTED on
      # q4 and did NOT break the heavy-query rate collapse (346K→34K rec/s past
      # ~40M events) — confirming FRS-CACHE-STATS: the drain is CPU-bound (get_arc
      # memtable-tier walk per join probe), NOT cache-miss I/O or L0 compaction lag.
      # The real fix is the in-progress engine read-path work (skiplist memtable
      # index / lock-free memtable to cut per-probe tier-walk CPU), not config.
  checkpoints:
    dir: file:///tmp/nexmark-checkpoints-forst-rs

execution:
  async-state:
    in-flight-records-limit: 60000
    buffer-size: 16000
    buffer-timeout: 5000
  checkpointing:
    interval: 30 s
    mode: EXACTLY_ONCE
    tolerable-failed-checkpoints: 1000

table:
  exec:
    mini-batch:
      # Disabled — when enabled, StreamExecJoin picks MiniBatchStreamingJoinOperator
      # (sync V1) regardless of async-state.enabled. Disabling lets the async path activate.
      enabled: false
    async-state:
      enabled: true
    state:
      # 2026-05-29: TTL must be 0 — forst-rs MapState TTL is unsupported
      # (PR-A7 deferred MAP-state TTL; throws UnsupportedOperationException
      # on join "left-records" MapState). Matches the S3 config (ttl: 0 ms).
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
