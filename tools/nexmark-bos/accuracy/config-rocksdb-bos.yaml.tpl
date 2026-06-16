# Backend: RocksDB | JDK 17 | local working DB + BOS checkpoints.
# This template is used only by tools/nexmark-bos/accuracy.
env.java.home: ${JDK17}
env.hadoop.conf.dir: ${HADOOP_CONF_DIR}
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
  memory:
    process:
      size: 12288m

parallelism:
  default: ${NEXMARK_PARALLELISM}

state:
  backend:
    type: rocksdb
    incremental: true
    rocksdb:
      localdir: ${ROCKSDB_LOCAL_DIR}
      timer-service:
        factory: HEAP
      checkpoint:
        transfer:
          thread:
            num: 8
  checkpoints:
    dir: ${ROCKSDB_CHECKPOINT_URI}

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
      ttl: 1h

fs:
  allowed-fallback-filesystems: bos

security:
  module:
    factory:
      classes:
        - org.apache.flink.runtime.security.modules.JaasModuleFactory
        - org.apache.flink.runtime.security.modules.ZookeeperModuleFactory
  context:
    factory:
      classes:
        - org.apache.flink.runtime.security.contexts.NoOpSecurityContextFactory
  delegation:
    tokens:
      enabled: false

rest:
  port: 8081
  bind-port: 8081
  address: localhost
  bind-address: localhost

io:
  tmp:
    dirs: /tmp/flink-rocksdb-tmp
