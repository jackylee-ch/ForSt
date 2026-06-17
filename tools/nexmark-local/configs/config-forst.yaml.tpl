# Backend: ForSt (community Java) | JDK 17 | S3 storage
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
  # identically across all backend templates for a FAIR topology. (Reference mirror
  # of scripts/templates-linux/config-forst.yaml.tpl — the harness uses templates-linux.)
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
      primary-dir: s3://${S3_BUCKET}/${S3_PREFIX}/forst-data-${RUN_ID}
      cache:
        size-based-limit: 2gb
  checkpoints:
    dir: s3://${S3_BUCKET}/${S3_PREFIX}/forst-checkpoints-${RUN_ID}

execution:
  checkpointing:
    interval: 30 s
    mode: EXACTLY_ONCE

table:
  exec:
    mini-batch:
      enabled: false
    async-state:
      enabled: true
    state:
      ttl: 1h

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
    dirs: /tmp/flink-forst-io
