# Backend: ForSt-RS (Rust + FFM) | LOCAL S3 SIMULATION (disaggregated state)
#
# Two distinct local directories model the disaggregated topology, exactly as
# the 2026-06-14 user directive describes ("use one directory as the S3
# directory and another as the local directory; when accessing the S3
# directory, limit bandwidth to 50 Gb/s"):
#
#   * ${S3_DIR}    — the REMOTE "S3" directory. SSTs (and only SSTs) live here,
#                    reached through OpenDAL's `fs://`/`file://` backend. Its
#                    byte traffic is what FRS_REMOTE_BW_MBPS throttles.
#   * ${LOCAL_DIR} — the LOCAL store: the LRU SST cache, Flink io.tmp scratch,
#                    and the checkpoint dir. NEVER throttled (native speed).
#
# The bandwidth cap is NOT a YAML key — it is the process env knob
# FRS_REMOTE_BW_MBPS (MiB/s), read by the Rust engine. Set it to 6250 for the
# 50 Gb/s regime (run-s3sim.sh exports it; the tools/nexmark-local copy of
# run-8c32g.sh forwards it into the TM/JM containers). Unset / 0 = OFF
# (byte-identical pass-through).
#
# Everything else MATCHES config-forst-rs-local.yaml.tpl (ForSt-matching
# uniform config: noflush=false, writebuffer 1G, WBM 4G, lz4). Per-query
# tuning is FORBIDDEN; the only allowed optimization is dynamic/adaptive
# engine behavior under this ONE config.

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
    forst-rs:
      storage:
        # REMOTE "S3" leg — OpenDAL local-FS backend rooted at the S3 dir.
        # In a real deployment this would be s3://bucket/prefix; here it is a
        # second local directory whose bandwidth FRS_REMOTE_BW_MBPS caps.
        uri: file://${S3_DIR}/
        opendal-config: '{"root":"${S3_DIR}/"}'
        # LOCAL leg — the LRU SST cache (never throttled). Sized to hold the
        # full hot working set so cache hits stay at native speed.
        cache-dir: ${LOCAL_DIR}/forst-rs-cache
        cache-capacity-mb: 65536
      writebuffer:
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
  checkpoints:
    dir: file://${LOCAL_DIR}/nexmark-checkpoints-forst-rs

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
      enabled: false
    async-state:
      enabled: true
    state:
      ttl: 0 ms

rest:
  port: 8081
  bind-port: 8081
  address: localhost
  bind-address: localhost

io:
  tmp:
    dirs: ${LOCAL_DIR}/flink-forst-rs-io
