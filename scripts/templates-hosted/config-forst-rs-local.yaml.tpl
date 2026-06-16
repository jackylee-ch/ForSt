# Backend: ForSt-RS (Rust + FFM, V2 async only) | JDK 25 | LOCAL file storage
#                                              ::  HOSTED-CI ACCURACY profile.
#
# Lightweight variant of scripts/templates-linux/config-forst-rs-local.yaml.tpl
# sized for a GitHub-hosted ubuntu-latest runner (~16 GB RAM, 4 vCPU). This is
# the ACCURACY gate, not the perf split: it replays a tiny 100K-event fixed CSV,
# so a single small TaskManager suffices. All storage is local file:// — NO S3,
# NO opendal-remote, NO Hadoop S3 plugin (none are needed to verify correctness).
#
# The forst-rs engine is loaded via JDK FFM from the .so the hosted job rebuilds
# from source each commit (-Dforstrs.native.libpath). No S3 placeholders here;
# the harness still runs envsubst with its allow-list, which is a no-op for this
# template (nothing to substitute).
#
# NOTE: env.java.home is INTENTIONALLY OMITTED. bash-java-utils.sh prefers
# config.yaml's env.java.home over the system JAVA_HOME, and on a hosted runner
# the JDK path is not fixed (setup-java installs it dynamically). By omitting it,
# Flink falls back to the JAVA_HOME the harness exports (JDK25 for forst-rs).
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
      size: 1600m
  execution:
    failover-strategy: region

taskmanager:
  bind-host: localhost
  host: localhost
  numberOfTaskSlots: 4
  memory:
    process:
      size: 6144m

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
        # Accuracy scale (100K events) keeps the working set tiny; a small cache
        # is ample and keeps the engine memory envelope inside the hosted runner.
        cache-capacity-mb: 1024
      writebuffer:
        size: 64mb
        count: 4
        manager:
          capacity: 512mb
      compaction:
        max-background: 2
      flush:
        max-background: 1
      cache:
        block:
          capacity: 256mb
  checkpoints:
    dir: file:///tmp/nexmark-checkpoints-forst-rs

execution:
  async-state:
    in-flight-records-limit: 6000
    buffer-size: 1000
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
      # forst-rs MapState TTL is unsupported (join "left-records" MapState throws
      # UnsupportedOperationException) — must be 0.
      ttl: 0 ms

rest:
  port: 8081
  bind-port: 8081
  address: localhost
  bind-address: localhost

io:
  tmp:
    dirs: /tmp/flink-forst-rs-io

# JDK 25 + Hadoop security: the forst-rs backend puts Hadoop 3.3.6 on the
# classpath (it needs org.apache.hadoop.conf.Configuration), which makes Flink's
# default HadoopModuleFactory run at installSecurityContext and call
# UserGroupInformation.getCurrentUser() -> javax.security.auth.Subject.getSubject().
# On JDK 25 that throws "UnsupportedOperationException: getSubject is not
# supported" (SecurityManager removed, JEP 486) and the JM+TM crash on boot
# (no TM registers -> ConnectException at INSERT). The earlier attempted fix
# -Djava.security.manager=allow is WORSE on JDK 25: the VM refuses to start with
# "java.lang.Error: A command line option has attempted to allow or enable the
# Security Manager. Enabling a Security Manager is not supported." (=allow only
# works on JDK 18-23). The correct JDK-25 fix is to NOT install Flink's Hadoop
# security module at all: local file:// state needs no Kerberos/UGI. Override the
# module list to drop HadoopModuleFactory so the getSubject path is never taken;
# hadoop-common stays on the classpath for plain Configuration use.
security:
  module:
    factory:
      classes: org.apache.flink.runtime.security.modules.JaasModuleFactory;org.apache.flink.runtime.security.modules.ZookeeperModuleFactory
