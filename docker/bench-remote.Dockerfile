# Remote x86_64 bench image — docker 19.03-compatible (user-provided fix 2026-06-11).
#
# WHY THIS EXISTS: the IDC box runs docker 19.03, which cannot pull
# eclipse-temurin:25 from Docker Hub (OCI manifest mediaType error). The
# daocloud mirror (docker.m.daocloud.io) serves eclipse-temurin:17-jre-jammy in
# a 19.03-compatible manifest; JDK25 is COPIED from the build context (the
# host's ~/workenv JDK25 x64 distribution) instead of being pulled.
#
# Build (on the remote host):
#   mkdir -p /ssd2/jackylee/frs-bench/imgctx
#   cp -a ~/workenv/jdk25.0.2-linux_x64_gcc12 /ssd2/jackylee/frs-bench/imgctx/jdk25
#   cp <ForSt-worktree>/docker/bench-remote.Dockerfile /ssd2/jackylee/frs-bench/imgctx/Dockerfile
#   docker build -t forst-bench:x86 /ssd2/jackylee/frs-bench/imgctx
#
# The Rust toolchain is NOT in this image: the .so is built natively on the
# host (host glibc ≤ jammy's 2.35, so the host-built .so loads here — verify
# once with: docker run --rm -v <so>:/t/x.so forst-bench:x86 bash -c 'ldd /t/x.so').
FROM docker.m.daocloud.io/eclipse-temurin:17-jre-jammy
USER root
ENV DEBIAN_FRONTEND=noninteractive

# Relocate the base JRE17, then install JDK25 at the path the harness templates
# expect (config templates set env.java.home: /opt/java/openjdk = the JDK25).
RUN mv /opt/java/openjdk /opt/java/jre17
COPY jdk25 /opt/java/openjdk
ENV JDK25=/opt/java/openjdk
ENV JDK17=/opt/java/jre17
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=/opt/java/openjdk/bin:$PATH

# Harness needs: python3 (conf rewrite + JM polling), envsubst (gettext-base),
# curl, procps; libjemalloc2 for the TM-process LD_PRELOAD (engine jemalloc is
# statically bundled in the .so regardless). Mirror apt to aliyun for IDC
# reachability; jammy uses /etc/apt/sources.list (not .sources).
RUN sed -i 's|http://archive.ubuntu.com|http://mirrors.aliyun.com|g; s|http://security.ubuntu.com|http://mirrors.aliyun.com|g; s|http://ports.ubuntu.com|http://mirrors.aliyun.com|g' /etc/apt/sources.list && \
    apt-get update && apt-get install -y --no-install-recommends \
      python3 gettext-base curl procps findutils libjemalloc2 && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/lib/*-linux-gnu/libjemalloc.so.2 /usr/local/lib/libjemalloc-preload.so

WORKDIR /work
CMD ["bash"]
