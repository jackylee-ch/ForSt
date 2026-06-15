# Backend: ForSt-RS (Rust + FFM) | REMOTE/BOS DISAGGREGATED STATE
# ============================================================================
# This is the BOS (Baidu Object Storage = real remote S3) sibling of
# tools/nexmark-local/configs/config-forst-rs-s3sim.yaml.tpl. Where the s3sim
# template points the OpenDAL remote leg at a SECOND LOCAL DIRECTORY and caps
# its bandwidth with a software throttle, THIS template points the remote leg
# at a LIVE BOS S3 BUCKET. forst-rs state is DISAGGREGATED: SSTs (+ vlog blobs
# when KV-sep is on) live in BOS; only the LRU SST cache + scratch + the local
# checkpoint dir are on local NVMe.
#
# It deliberately mirrors scripts/templates-linux/config-forst-rs.yaml.tpl (the
# canonical S3 arm template the `forst-rs-ffm-s3` arm of measure-sql.sh feeds
# through `envsubst`) so this drop-in replacement keeps the SAME placeholders
# and is selected the SAME way:
#
#     TEMPLATES=tools/nexmark-bos/configs CONFIG=forst-rs-ffm-s3 \
#       S3_ENDPOINT=... S3_BUCKET=... S3_ACCESS_KEY=... S3_SECRET_KEY=... \
#       S3_REGION=... S3_PREFIX=... bash scripts/measure-sql.sh
#
# run-bos.sh wires all of that for you (it points TEMPLATES at this directory
# and drives the `forst-rs-ffm-s3` arm). Flat keys are required because
# bash-java-utils.sh reads env.java.home via sed, not a YAML parser.
#
# Placeholders (substituted by measure-sql.sh `envsubst "$S3VARS"`):
#   ${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET}
#   ${S3_REGION} ${S3_PREFIX} ${RUN_ID}
# (Exactly the var set in measure-sql.sh:S3VARS — VERIFIED against that script.)
#
# ============================================================================
# THE OPTIMIZATION KNOBS — NOT in this YAML. They are PROCESS ENV vars read by
# the Rust engine via std::env::var (getenv), forwarded into the TM/JM
# containers by run-8c32g.sh and set with recommended defaults by run-bos.sh:
#
#   FRS_VLOG_COALESCE_DEREF=1   coalesce N vlog derefs into ONE remote GET (the
#                               N->1 remote-GET win on KV-sep value reads)
#   FRS_KV_SEPARATION=true      KV separation ON (values to vlog blobs; the
#                               write-amp / value-carrying read-path lever)
#   FRS_KV_MIN_BLOB_SIZE=256    min value bytes routed to the vlog
#   FRS_REMOTE_BW_MBPS          remote-leg bandwidth cap (0/unset = the LIVE
#                               BOS link's real bandwidth; set a number only to
#                               MODEL a slower link on a fast box)
#   FRS_UPLOAD_RATE_SPLIT=1     QoS-split the upload bandwidth so a compaction
#                               burst can't starve flush->checkpoint
#   FRS_UPLOAD_COMPACTION_SHARE=0.5  compaction's share of the remote write rate
#   FRS_CKPT_LINK_MODE=1        WAL-DELTA / LINK checkpoint (link SSTs instead
#                               of re-uploading them -> cheap incremental ckpt)
#   FRS_REMOTE_NONSST_LOCAL=1   pin MANIFEST/CURRENT/OPTIONS/WAL/journal LOCAL
#                               (only SST-class objects go to BOS -> no chatty
#                               small-file S3 metadata traffic)
#   FRS_RESTORE_BG_FILL=1       instant-restore: paced background cache warm of
#                               adopted physicals (+ _WORKERS / _PACE_MB)
#   FRS_VLOG_RESIDENT_BUDGET_MB resident vlog-reader BYTE budget (bounds the
#                               KV-sep off-heap working set on a small cgroup)
#   FRS_CACHE_SPACE_LIMIT_MB    free-disk-headroom floor for the local LRU cache
#
# See docs/README.md "Optimization knobs" for what each does and when to tune.
# ============================================================================

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
    process:
      size: 12288m

parallelism:
  default: 4

state:
  backend:
    type: org.apache.flink.state.forstrs.ForStRsStateBackendFactory
    # Incremental checkpoints: the engine links new SSTs into BOS rather than
    # re-uploading the full state (paired with FRS_CKPT_LINK_MODE=1 below).
    incremental: true
    forst-rs:
      writebuffer:
        # Match the s3sim/ForSt uniform config: 1 GiB write buffer, WBM 4 GiB.
        # On a high-latency remote store, larger memtables flush less often ->
        # fewer, larger SSTs -> fewer remote round-trips per probe.
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
      storage:
        # REMOTE leg = the LIVE BOS S3 bucket. The engine reaches it through
        # OpenDAL's s3:// backend; opendal-config carries the BOS endpoint +
        # creds. Each run gets its OWN prefix (forst-rs-data-${RUN_ID}) so
        # concurrent runs / reruns never collide in the bucket.
        uri: s3://${S3_BUCKET}/${S3_PREFIX}/forst-rs-data-${RUN_ID}/
        opendal-config: '{"endpoint":"${S3_ENDPOINT}","region":"${S3_REGION}","access_key_id":"${S3_ACCESS_KEY}","secret_access_key":"${S3_SECRET_KEY}","root":"/${S3_PREFIX}/forst-rs-data-${RUN_ID}/"}'
        # LOCAL leg = the on-disk LRU SST cache (never goes to BOS). After first
        # touch, reads hit local NVMe (~100us) instead of BOS (~ms). Sized to
        # hold the heavy-join working set; the LRU is bounded by this size AND
        # by FRS_CACHE_SPACE_LIMIT_MB (free-disk floor) when that env is set.
        cache-dir: /tmp/flink-forst-rs-cache
        cache-capacity-mb: 131072
  checkpoints:
    # Checkpoint COPY goes to LOCAL disk (matches the rocksdb baseline). The
    # working STATE lives in BOS (storage.uri); with FRS_CKPT_LINK_MODE=1 the
    # checkpoint links the BOS SSTs instead of re-uploading them, so the local
    # checkpoint dir holds only the small manifest/metadata.
    dir: file:///tmp/nexmark-checkpoints-forstrs

execution:
  async-state:
    in-flight-records-limit: 60000
    buffer-size: 16000
    buffer-timeout: 5000
  checkpointing:
    # 30s matches the rocksdb/ForSt baseline (apples-to-apples). On a real
    # high-bandwidth BOS link with link-mode checkpoints this is cheap; on a
    # bandwidth-constrained link, widen it (e.g. 300 s) to keep checkpoint
    # upload off the heavy-query critical path.
    interval: 30 s
    mode: EXACTLY_ONCE
    tolerable-failed-checkpoints: 1000

table:
  exec:
    mini-batch:
      enabled: false
    async-state:
      enabled: true
    state:
      ttl: 0 ms

# S3 plugin credentials — pre-substituted from the user-supplied env vars by
# envsubst. multiobjectdelete is disabled because non-AWS S3 services (BOS / OSS
# / MinIO) reject the bulk-delete request format with InvalidArgument.
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
