# Backend: ForSt-RS (Rust + FFM, V2 async only) | JDK 25 | S3 storage
# Flat keys (required: bash-java-utils.sh reads env.java.home via sed, not YAML).
env.java.home: /opt/java/openjdk
env.java.opts.all: --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
env.java.opts.taskmanager: -Dforstrs.native.libpath=/Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/libforst_rs_ffi.so --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -XX:+UseG1GC -XX:+UseCompactObjectHeaders
env.java.opts.jobmanager: --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -XX:+UseG1GC -XX:+UseCompactObjectHeaders

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
        uri: s3://${S3_BUCKET}/${S3_PREFIX}/forst-rs-data-${RUN_ID}/
        opendal-config: '{"endpoint":"${S3_ENDPOINT}","region":"${S3_REGION}","access_key_id":"${S3_ACCESS_KEY}","secret_access_key":"${S3_SECRET_KEY}","root":"/${S3_PREFIX}/forst-rs-data-${RUN_ID}/"}'
        cache-dir: /tmp/flink-forst-rs-cache
        cache-capacity-mb: 8192
  checkpoints:
    dir: s3://${S3_BUCKET}/${S3_PREFIX}/forst-rs-checkpoints-${RUN_ID}

execution:
  checkpointing:
    interval: 30 s
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
    dirs: /tmp/flink-forst-rs-io
