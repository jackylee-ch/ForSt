# Backend: ForSt (community Java, disaggregated) on LOCAL disk | JDK 17
# Fair local A/B vs rocksdb-local and forst-rs-ffm-local: ForSt's UFS/async
# disaggregated code path, but primary-dir + checkpoints on a local file:// path
# (no S3) so it is comparable to the other local backends. (S3 perf is deferred
# to Phase 3 per the goal.)
# Flat keys (required: bash-java-utils.sh reads env.java.home via sed, not YAML).
env.java.home: /usr/lib/jvm/java-17-openjdk-arm64
env.java.opts.all: --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED

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
  # SLOT-SKEW FIX (2026-06-16, PMC-1): spread slots evenly across both TMs (Flink
  # 2.0 default load-balance.mode=NONE packs a job into the fewest TMs). Set
  # identically across all backend templates for a FAIR topology.
  load-balance:
    mode: SLOTS
  memory:
    process:
      size: 12288m

parallelism:
  default: 4

state:
  backend:
    type: forst
    forst:
      primary-dir: file:///tmp/flink-forst-data/
      cache:
        size-based-limit: 2gb
  checkpoints:
    dir: file:///tmp/nexmark-checkpoints-forst

execution:
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
    dirs: /tmp/flink-forst-io
