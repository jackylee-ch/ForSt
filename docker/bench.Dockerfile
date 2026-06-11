# 8c/32g benchmark environment (arm64 Linux, native on Apple Silicon).
# Builds the forst-rs Linux .so and runs the Flink+Nexmark harness under
# `docker run --cpus=8 --memory=32g` for a TRUE 8c/32g resource regime.
FROM eclipse-temurin:25-jdk-jammy
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jdk-headless build-essential curl ca-certificates \
      python3 gettext-base procps findutils git pkg-config libjemalloc2 && \
    rm -rf /var/lib/apt/lists/*
# TM-process allocator (engine bundles its own _rjem_-prefixed jemalloc in the
# .so; this one governs the JVM's native allocations — FFM arenas → malloc).
# Arch-independent path for LD_PRELOAD across x86_64/aarch64 images.
RUN ln -s /usr/lib/*-linux-gnu/libjemalloc.so.2 /usr/local/lib/libjemalloc-preload.so
# JDK25 from the temurin base; JDK17 from Ubuntu repo.
ENV JDK25=/opt/java/openjdk
# arch-dependent: java-17-openjdk-arm64 on arm64, -amd64 on x86_64
RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17 
ENV JDK17=/usr/lib/jvm/java-17
# Rust toolchain (current stable; Ubuntu's is too old for the workspace edition).
ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo PATH=/opt/cargo/bin:/usr/bin:/bin:/usr/local/bin
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
WORKDIR /work
CMD ["bash"]
