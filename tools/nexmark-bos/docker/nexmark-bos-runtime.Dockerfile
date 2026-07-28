FROM forst-bench:x86

USER root
RUN mkdir -p /opt/nexmark-bos/lib
COPY bos-hadoop-fs-2.0.0.jar /opt/nexmark-bos/lib/bos-hadoop-fs-2.0.0.jar
COPY flink-statebackend-forst-rs-2.2.0.jar /opt/nexmark-bos/lib/flink-statebackend-forst-rs-2.2.0.jar
COPY libforst_rs_ffi.so /opt/nexmark-bos/lib/libforst_rs_ffi.so

ENV BOS_HADOOP_FS_JAR=/opt/nexmark-bos/lib/bos-hadoop-fs-2.0.0.jar
WORKDIR /work
