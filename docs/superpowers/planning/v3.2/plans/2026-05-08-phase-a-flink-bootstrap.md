# Phase-A Flink Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to run this plan inline (cross-repo work — easier without subagent state coordination). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the Flink-side `flink-statebackend-forst-rs` Maven module with a minimal JDK 25 FFM bridge that loads `libforst_rs_ffi.dylib` and demonstrates a put/get round-trip via FFM into the ForSt-RS engine.

**Architecture:** New Maven module under `flink-state-backends/flink-statebackend-forst-rs/` (Flink repo, branch `forst-rs-jdk25`). Java side uses JDK 25 `java.lang.foreign.{Arena, MemorySegment, Linker, SymbolLookup}` to call into the cdylib. No JNI, no `forstjni` dependency. Module-info opens caller modules to FFM via `--enable-native-access`. Integration test loads cdylib from a system property pointing to the cargo-built artifact.

**Tech Stack:** JDK 25.0.3 (Zulu) + Maven (parent flink-state-backends 2.2.0) + JUnit 5 + Apache Flink 2.2 SPI + ForSt-RS cdylib (`libforst_rs_ffi.dylib` from `~/Code/stczwd/ForSt/target/release/`).

**MVP scope** (per user-confirmed option (a)): 6 frs_* function bindings + lifecycle (open_memory/close) + CF lifecycle (default_cf/cf_close) + put/get/bytes_free + 1 round-trip integration test. NOT in MVP: 5 state types, Async/Sync KeyedStateBackend, Checkpoint integration, mini-cluster regression — those are downstream Phase D L4/L5 per B1.

---

## File Structure (Flink repo)

All paths under `/Users/lijunqing/Code/stczwd/flink/` on branch `forst-rs-jdk25`:

```
flink-state-backends/
├── pom.xml                                                    [MODIFY: add module to <modules>]
└── flink-statebackend-forst-rs/                               [CREATE]
    ├── pom.xml                                                [CREATE]
    └── src/
        ├── main/
        │   ├── java/
        │   │   ├── module-info.java                           [CREATE]
        │   │   └── org/apache/flink/state/forstrs/
        │   │       ├── package-info.java                      [CREATE]
        │   │       ├── ForStRsStateBackend.java               [CREATE]
        │   │       ├── ForStRsStateBackendFactory.java        [CREATE]
        │   │       ├── ForStRsOptions.java                    [CREATE]
        │   │       ├── FrsStatus.java                         [CREATE]
        │   │       ├── FrsBackendException.java               [CREATE]
        │   │       └── ffm/
        │   │           ├── ForStRsLinker.java                 [CREATE]
        │   │           ├── FrsDb.java                         [CREATE]
        │   │           └── FrsCfHandle.java                   [CREATE]
        │   └── resources/
        │       └── META-INF/services/
        │           └── org.apache.flink.runtime.state.StateBackendFactory  [CREATE]
        └── test/
            └── java/
                └── org/apache/flink/state/forstrs/ffm/
                    └── ForStRsRoundTripIT.java                [CREATE]
```

ForSt-RS repo: no edits required — cdylib already builds.

---

## Pre-flight: Environment Setup

- [ ] **Step 0.1: Build cdylib (already done; verifying artifact)**

```bash
ls -la /Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib
```

Expected: file exists, size > 0. Already verified earlier this session.

- [ ] **Step 0.2: Set JAVA_HOME to JDK 25 for this session**

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
java -version 2>&1 | head -1
```

Expected: `openjdk version "25.0.3"`.

- [ ] **Step 0.3: Verify cdylib symbols (already done)**

```bash
nm -gU /Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib | grep -E '_frs_(db_open_memory|db_close|db_default_cf|put|get|bytes_free|cf_close)$'
```

Expected: 7 matching `_frs_*` lines (verified earlier this session).

---

## Task 1: Create new Maven module skeleton

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/pom.xml`
- Modify: `flink-state-backends/pom.xml` (add child module)

- [ ] **Step 1.1: Read current parent pom <modules> section**

```bash
grep -A 12 '<modules>' /Users/lijunqing/Code/stczwd/flink/flink-state-backends/pom.xml
```

- [ ] **Step 1.2: Add child module to parent pom**

In `flink-state-backends/pom.xml`, add `<module>flink-statebackend-forst-rs</module>` to the `<modules>` block, alphabetically after `flink-statebackend-forst`.

- [ ] **Step 1.3: Create module pom.xml**

Create `flink-state-backends/flink-statebackend-forst-rs/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">

    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-state-backends</artifactId>
        <version>2.2.0</version>
    </parent>

    <artifactId>flink-statebackend-forst-rs</artifactId>
    <name>Flink : State backends : ForSt-RS (Rust + JDK 25 FFM)</name>

    <packaging>jar</packaging>

    <properties>
        <maven.compiler.source>25</maven.compiler.source>
        <maven.compiler.target>25</maven.compiler.target>
        <maven.compiler.release>25</maven.compiler.release>
        <surefire.module.config>
            --enable-native-access=ALL-UNNAMED
        </surefire.module.config>
    </properties>

    <dependencies>

        <!-- core dependencies (provided by Flink runtime) -->

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${project.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- test dependencies -->

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>

    </dependencies>

</project>
```

- [ ] **Step 1.4: Verify Maven can read the new module**

```bash
cd /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs
mvn -N help:effective-pom 2>&1 | tail -20
```

Expected: effective-pom prints; no `[ERROR]` lines for missing parent or malformed XML.

- [ ] **Step 1.5: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/flink
git add flink-state-backends/pom.xml flink-state-backends/flink-statebackend-forst-rs/pom.xml
git commit -m "$(cat <<'EOF'
feat(state-forst-rs): L1 — bootstrap empty Maven module

Creates flink-statebackend-forst-rs module under flink-state-backends/,
registered in parent pom <modules> block. JDK 25 compile target;
--enable-native-access for surefire. No production code yet (subsequent
commits add module-info, FFM Linker, ForStRsStateBackend skeleton).

Part of v3.2 Phase-A Flink bootstrap (MVP option a per user 2026-05-08).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Add module-info, package-info, SPI placeholder

**Files:**
- Create: `flink-statebackend-forst-rs/src/main/java/module-info.java`
- Create: `flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/package-info.java`
- Create: `flink-statebackend-forst-rs/src/main/resources/META-INF/services/org.apache.flink.runtime.state.StateBackendFactory`

- [ ] **Step 2.1: Create module-info.java**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */

/**
 * Flink ForSt-RS state backend module.
 *
 * <p>Bridges Flink's {@link org.apache.flink.runtime.state.StateBackend}
 * SPI to the ForSt-RS native engine via JDK 25 Foreign Function & Memory API.
 * Native library: {@code libforst_rs_ffi.{dylib,so,dll}}.
 */
module org.apache.flink.state.forstrs {
    requires java.base;
    // FFM API is in java.base since JDK 22 — no separate require.

    exports org.apache.flink.state.forstrs;
}
```

- [ ] **Step 2.2: Create package-info.java**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */

/**
 * Public API for the ForSt-RS state backend.
 *
 * <p>Entry point: {@link ForStRsStateBackend}. Configuration:
 * {@link ForStRsOptions}. SPI factory: {@link ForStRsStateBackendFactory}.
 */
package org.apache.flink.state.forstrs;
```

- [ ] **Step 2.3: Create SPI registration file**

Path: `src/main/resources/META-INF/services/org.apache.flink.runtime.state.StateBackendFactory`

Content:

```
org.apache.flink.state.forstrs.ForStRsStateBackendFactory
```

- [ ] **Step 2.4: Compile (will fail — classes don't exist yet, that's expected at this step)**

```bash
cd /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs
mvn compile 2>&1 | tail -20
```

Expected: BUILD FAILURE because `ForStRsStateBackend`/`ForStRsOptions`/`ForStRsStateBackendFactory` don't exist yet. This is the red step before Task 3.

---

## Task 3: Add status enum, exception, opaque handle wrappers

**Files:**
- Create: `org/apache/flink/state/forstrs/FrsStatus.java`
- Create: `org/apache/flink/state/forstrs/FrsBackendException.java`
- Create: `org/apache/flink/state/forstrs/ffm/FrsDb.java`
- Create: `org/apache/flink/state/forstrs/ffm/FrsCfHandle.java`

- [ ] **Step 3.1: Write FrsStatus enum**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */
package org.apache.flink.state.forstrs;

/**
 * Mirrors the FRS_STATUS_* int codes from
 * {@code crates/forst-rs-ffi/src/lib.rs} (lines 60–95).
 * Stable ABI: ordinal does not matter; the int code does.
 */
public enum FrsStatus {
    OK(0),
    ERROR(1),
    NULL_ARG(2),
    NOT_FOUND(3),
    INVALID_ARGUMENT(4),
    PANIC(5),
    POISONED(6),
    IO(7),
    CORRUPTION(8),
    NOT_SUPPORTED(9),
    ABORTED(10),
    BUSY(11),
    TIMED_OUT(12),
    EXPIRED(13),
    INCOMPLETE(14);

    private final int code;

    FrsStatus(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static FrsStatus fromCode(int code) {
        for (FrsStatus s : values()) {
            if (s.code == code) {
                return s;
            }
        }
        throw new IllegalArgumentException("unknown FRS status code: " + code);
    }
}
```

- [ ] **Step 3.2: Write FrsBackendException**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */
package org.apache.flink.state.forstrs;

/** Thrown when a frs_* call returns a non-OK status. */
public class FrsBackendException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final FrsStatus status;

    public FrsBackendException(FrsStatus status, String message) {
        super(message + " (status=" + status + ")");
        this.status = status;
    }

    public FrsStatus status() {
        return status;
    }
}
```

- [ ] **Step 3.3: Write FrsDb opaque handle wrapper**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */
package org.apache.flink.state.forstrs.ffm;

import java.lang.foreign.MemorySegment;

/**
 * Opaque ForSt-RS database handle. Wraps a {@code FrsDb} pointer
 * (raw {@code *mut c_void} from the C ABI). Use try-with-resources
 * to ensure {@code frs_db_close} runs.
 */
public final class FrsDb implements AutoCloseable {

    private final ForStRsLinker linker;
    private MemorySegment handle;
    private boolean closed = false;

    FrsDb(ForStRsLinker linker, MemorySegment handle) {
        this.linker = linker;
        this.handle = handle;
    }

    public MemorySegment handle() {
        if (closed) {
            throw new IllegalStateException("FrsDb already closed");
        }
        return handle;
    }

    @Override
    public void close() {
        if (!closed) {
            linker.dbClose(handle);
            closed = true;
            handle = MemorySegment.NULL;
        }
    }
}
```

- [ ] **Step 3.4: Write FrsCfHandle opaque handle wrapper**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */
package org.apache.flink.state.forstrs.ffm;

import java.lang.foreign.MemorySegment;

/** Opaque column-family handle. */
public final class FrsCfHandle implements AutoCloseable {

    private final ForStRsLinker linker;
    private MemorySegment handle;
    private boolean closed = false;

    FrsCfHandle(ForStRsLinker linker, MemorySegment handle) {
        this.linker = linker;
        this.handle = handle;
    }

    public MemorySegment handle() {
        if (closed) {
            throw new IllegalStateException("FrsCfHandle already closed");
        }
        return handle;
    }

    @Override
    public void close() {
        if (!closed) {
            linker.cfClose(handle);
            closed = true;
            handle = MemorySegment.NULL;
        }
    }
}
```

---

## Task 4: Implement ForStRsLinker (the heart of the bridge)

**Files:**
- Create: `org/apache/flink/state/forstrs/ffm/ForStRsLinker.java`

- [ ] **Step 4.1: Write ForStRsLinker class**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */
package org.apache.flink.state.forstrs.ffm;

import org.apache.flink.state.forstrs.FrsBackendException;
import org.apache.flink.state.forstrs.FrsStatus;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * JDK 25 FFM bridge to libforst_rs_ffi.{dylib,so,dll}.
 *
 * <p>Loads the cdylib and binds 7 downcall MethodHandles for the MVP
 * surface: db_open_memory / db_close / db_default_cf / cf_close /
 * put / get / bytes_free.
 *
 * <p>Library lookup order:
 * <ol>
 *   <li>System property {@code forstrs.native.libpath} (absolute path)</li>
 *   <li>{@code System.loadLibrary("forst_rs_ffi")} fallback</li>
 * </ol>
 *
 * <p>Lifetime: the {@link Arena} given to the constructor owns the
 * library handle's symbol lookup; closing the Arena unloads the lib
 * (and invalidates all returned MemorySegments).
 *
 * <p>Reference: docs/design/2.5_ffm_bridge_design.md §3.
 */
public final class ForStRsLinker {

    /** {@code FrsBytes} struct layout: data ptr, len, capacity (24 bytes). */
    public static final StructLayout FRS_BYTES_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.ADDRESS.withName("data"),
            ValueLayout.JAVA_LONG.withName("len"),
            ValueLayout.JAVA_LONG.withName("capacity"));

    private final Linker linker;
    private final SymbolLookup lookup;

    private final MethodHandle frsDbOpenMemory;
    private final MethodHandle frsDbClose;
    private final MethodHandle frsDbDefaultCf;
    private final MethodHandle frsCfClose;
    private final MethodHandle frsPut;
    private final MethodHandle frsGet;
    private final MethodHandle frsBytesFree;

    public ForStRsLinker(Arena arena) {
        this.linker = Linker.nativeLinker();

        String explicit = System.getProperty("forstrs.native.libpath");
        if (explicit != null && !explicit.isBlank()) {
            this.lookup = SymbolLookup.libraryLookup(Path.of(explicit), arena);
        } else {
            // Falls back to OS lib loader path; libname forst_rs_ffi.
            System.loadLibrary("forst_rs_ffi");
            this.lookup = SymbolLookup.loaderLookup();
        }

        this.frsDbOpenMemory = bind("frs_db_open_memory",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsDbClose = bind("frs_db_close",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsDbDefaultCf = bind("frs_db_default_cf",
                FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS,   // db handle
                        ValueLayout.ADDRESS)); // out_cf

        this.frsCfClose = bind("frs_cf_close",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsPut = bind("frs_put",
                FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS,    // db
                        ValueLayout.ADDRESS,    // cf
                        ValueLayout.ADDRESS,    // key ptr
                        ValueLayout.JAVA_LONG,  // key_len
                        ValueLayout.ADDRESS,    // value ptr
                        ValueLayout.JAVA_LONG)); // value_len

        this.frsGet = bind("frs_get",
                FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS,    // db
                        ValueLayout.ADDRESS,    // cf
                        ValueLayout.ADDRESS,    // key ptr
                        ValueLayout.JAVA_LONG,  // key_len
                        ValueLayout.ADDRESS));  // out FrsBytes*

        this.frsBytesFree = bind("frs_bytes_free",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    }

    private MethodHandle bind(String name, FunctionDescriptor descriptor) {
        MemorySegment sym = lookup.find(name).orElseThrow(() ->
                new IllegalStateException("symbol not found in cdylib: " + name));
        return linker.downcallHandle(sym, descriptor);
    }

    /** Opens an in-memory ForSt-RS engine. Caller closes via {@link FrsDb#close()}. */
    public FrsDb dbOpenMemory(Arena arena) {
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpenMemory.invokeExact(outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_db_open_memory threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_memory");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /** Returns the default column family. Caller closes via {@link FrsCfHandle#close()}. */
    public FrsCfHandle dbDefaultCf(FrsDb db, Arena arena) {
        MemorySegment outCf = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbDefaultCf.invokeExact(db.handle(), outCf);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_db_default_cf threw: " + t.getMessage());
        }
        check(rc, "frs_db_default_cf");
        MemorySegment cfHandle = outCf.get(ValueLayout.ADDRESS, 0);
        return new FrsCfHandle(this, cfHandle);
    }

    /** Writes a key/value pair. */
    public void put(FrsDb db, FrsCfHandle cf, byte[] key, byte[] value) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg = local.allocate(key.length);
            MemorySegment.copy(key, 0, keySeg, ValueLayout.JAVA_BYTE, 0, key.length);
            MemorySegment valSeg = local.allocate(value.length);
            MemorySegment.copy(value, 0, valSeg, ValueLayout.JAVA_BYTE, 0, value.length);
            int rc;
            try {
                rc = (int) frsPut.invokeExact(
                        db.handle(), cf.handle(),
                        keySeg, (long) key.length,
                        valSeg, (long) value.length);
            } catch (Throwable t) {
                throw new FrsBackendException(FrsStatus.PANIC, "frs_put threw: " + t.getMessage());
            }
            check(rc, "frs_put");
        }
    }

    /** Returns the value for {@code key} or {@code null} if absent. */
    public byte[] get(FrsDb db, FrsCfHandle cf, byte[] key) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg = local.allocate(key.length);
            MemorySegment.copy(key, 0, keySeg, ValueLayout.JAVA_BYTE, 0, key.length);
            MemorySegment outBytes = local.allocate(FRS_BYTES_LAYOUT);
            int rc;
            try {
                rc = (int) frsGet.invokeExact(
                        db.handle(), cf.handle(),
                        keySeg, (long) key.length,
                        outBytes);
            } catch (Throwable t) {
                throw new FrsBackendException(FrsStatus.PANIC, "frs_get threw: " + t.getMessage());
            }
            check(rc, "frs_get");

            // Read FrsBytes struct: { *mut u8 data, usize len, usize capacity }
            long dataAddr = outBytes.get(ValueLayout.ADDRESS, 0).address();
            long len = outBytes.get(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS.byteSize());
            if (dataAddr == 0L) {
                return null; // not found
            }
            try {
                MemorySegment dataSeg = MemorySegment.ofAddress(dataAddr).reinterpret(len);
                byte[] copy = new byte[(int) len];
                MemorySegment.copy(dataSeg, ValueLayout.JAVA_BYTE, 0, copy, 0, (int) len);
                return copy;
            } finally {
                int freeRc;
                try {
                    freeRc = (int) frsBytesFree.invokeExact(outBytes);
                } catch (Throwable t) {
                    throw new FrsBackendException(FrsStatus.PANIC, "frs_bytes_free threw: " + t.getMessage());
                }
                check(freeRc, "frs_bytes_free");
            }
        }
    }

    /** Internal: invoked by {@link FrsDb#close()}. */
    void dbClose(MemorySegment handle) {
        try {
            int rc = (int) frsDbClose.invokeExact(handle);
            check(rc, "frs_db_close");
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_db_close threw: " + t.getMessage());
        }
    }

    /** Internal: invoked by {@link FrsCfHandle#close()}. */
    void cfClose(MemorySegment handle) {
        try {
            int rc = (int) frsCfClose.invokeExact(handle);
            check(rc, "frs_cf_close");
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_cf_close threw: " + t.getMessage());
        }
    }

    private static void check(int rc, String fn) {
        if (rc != FrsStatus.OK.code()) {
            throw new FrsBackendException(FrsStatus.fromCode(rc), fn);
        }
    }
}
```

---

## Task 5: Implement ForStRsStateBackend skeleton + Factory + Options

**Files:**
- Create: `ForStRsStateBackend.java`
- Create: `ForStRsStateBackendFactory.java`
- Create: `ForStRsOptions.java`

- [ ] **Step 5.1: Write ForStRsOptions stub**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */
package org.apache.flink.state.forstrs;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Configuration options for {@link ForStRsStateBackend}. */
public final class ForStRsOptions {

    /** Optional override for the cdylib path (defaults to System.loadLibrary). */
    public static final ConfigOption<String> NATIVE_LIB_PATH = ConfigOptions
            .key("state.backend.forstrs.native-lib-path")
            .stringType()
            .noDefaultValue()
            .withDescription(
                    "Absolute path to libforst_rs_ffi.{dylib,so,dll}. " +
                    "If unset, java.library.path is used via System.loadLibrary.");

    private ForStRsOptions() {
        // utility class
    }
}
```

- [ ] **Step 5.2: Write ForStRsStateBackend skeleton**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */
package org.apache.flink.state.forstrs;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.KeyedStateBackendParameters;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackendParametersImpl;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.delegate.DelegatingStateBackend;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;

/**
 * SKELETON {@link StateBackend} backed by ForSt-RS via JDK 25 FFM.
 *
 * <p>v3.2 Phase-A MVP scope: this class exists and is SPI-discoverable;
 * it does NOT yet implement {@code createKeyedStateBackend} or
 * {@code createOperatorStateBackend}. Those land in subsequent
 * Phase-D L4 (Async v2) and L5 (Sync v1) units per
 * {@code docs/superpowers/planning/v3.2/reports/B1_pr_split_plan.md}.
 *
 * @see ForStRsOptions
 * @see org.apache.flink.state.forstrs.ffm.ForStRsLinker
 */
public class ForStRsStateBackend implements StateBackend {

    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "forst-rs";
    }

    @Override
    public <K> org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws Exception {
        throw new UnsupportedOperationException(
                "ForStRsStateBackend.createKeyedStateBackend is not yet implemented " +
                "(v3.2 Phase-D L4/L5 work; current state is Phase-A MVP skeleton).");
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParametersImpl parameters) throws Exception {
        throw new UnsupportedOperationException(
                "ForStRsStateBackend.createOperatorStateBackend is not yet implemented " +
                "(v3.2 Phase-D L4/L5 work; current state is Phase-A MVP skeleton).");
    }
}
```

> **Note:** signatures of `createKeyedStateBackend` / `createOperatorStateBackend` may need adjustment — Flink 2.2 has both v1 and v2 variants. Verify the actual method signatures in `flink-runtime` `StateBackend` interface during step 5.4 compile and adjust. **If compile fails because the abstract method signature differs**, copy the exact signature(s) from the interface verbatim and add `throw new UnsupportedOperationException(...)` body.

- [ ] **Step 5.3: Write ForStRsStateBackendFactory**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */
package org.apache.flink.state.forstrs;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

/** SPI factory for {@link ForStRsStateBackend}. */
public class ForStRsStateBackendFactory implements StateBackendFactory<ForStRsStateBackend> {

    @Override
    public ForStRsStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader) {
        // MVP: configuration is parsed but not yet wired into the backend.
        return new ForStRsStateBackend();
    }
}
```

- [ ] **Step 5.4: Compile**

```bash
cd /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn compile 2>&1 | tail -30
```

Expected: BUILD SUCCESS (or specific compile errors about abstract method signatures — fix per step 5.2 note).

If errors about missing `createKeyedStateBackend` / `createOperatorStateBackend` signatures:
- Locate `StateBackend.java` in flink-runtime and copy the exact method signatures
- Update `ForStRsStateBackend.java` to match
- Recompile

- [ ] **Step 5.5: Commit (after compile green)**

```bash
cd /Users/lijunqing/Code/stczwd/flink
git add flink-state-backends/flink-statebackend-forst-rs/src/main
git commit -m "$(cat <<'EOF'
feat(state-forst-rs): L1+L2+L3 — module-info + FFM Linker + StateBackend skeleton

Adds the ForSt-RS state backend Java skeleton:
- module-info.java + package-info.java for org.apache.flink.state.forstrs
- ForStRsLinker (JDK 25 FFM bindings for 7 frs_* MVP functions)
- FrsDb + FrsCfHandle (try-with-resources opaque wrappers)
- FrsStatus + FrsBackendException (status code translation)
- ForStRsStateBackend (skeleton; createKeyedStateBackend throws UnsupportedOperationException)
- ForStRsStateBackendFactory (SPI factory)
- ForStRsOptions (state.backend.forstrs.native-lib-path config)
- META-INF/services/...StateBackendFactory registration

No StateBackend impl yet — that's Phase-D L4/L5 per
docs/superpowers/planning/v3.2/reports/B1_pr_split_plan.md.

Part of v3.2 Phase-A Flink bootstrap (MVP option a per user 2026-05-08).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Write integration test (the proof Phase A is done)

**Files:**
- Create: `src/test/java/org/apache/flink/state/forstrs/ffm/ForStRsRoundTripIT.java`

- [ ] **Step 6.1: Write integration test**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. ...
 */
package org.apache.flink.state.forstrs.ffm;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * End-to-end FFM round-trip: open in-memory ForSt-RS engine via JDK 25 FFM,
 * put and get a value, close. Proves Phase-A Flink-side bootstrap is
 * functional.
 *
 * <p>Requires the system property {@code forstrs.native.libpath} pointing
 * to {@code libforst_rs_ffi.{dylib,so,dll}}. Surefire is configured to
 * pass this at module level — see module pom.xml.
 */
class ForStRsRoundTripIT {

    @Test
    void putGetRoundTrip() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                 FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] key = "hello".getBytes(StandardCharsets.UTF_8);
                byte[] expectedValue = "world".getBytes(StandardCharsets.UTF_8);

                // Initially absent
                assertNull(linker.get(db, cf, key));

                // Put + get round-trip
                linker.put(db, cf, key, expectedValue);
                byte[] actualValue = linker.get(db, cf, key);

                assertArrayEquals(expectedValue, actualValue);
            }
        }
    }
}
```

- [ ] **Step 6.2: Wire native-lib path into surefire**

Modify the module's `pom.xml` `<properties>` to add a system property pointing to the cdylib (replacing the simple `surefire.module.config` from Task 1):

```xml
<properties>
    <maven.compiler.source>25</maven.compiler.source>
    <maven.compiler.target>25</maven.compiler.target>
    <maven.compiler.release>25</maven.compiler.release>
    <forstrs.native.libpath>${user.home}/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib</forstrs.native.libpath>
    <surefire.module.config>
        --enable-native-access=ALL-UNNAMED
        -Dforstrs.native.libpath=${forstrs.native.libpath}
    </surefire.module.config>
</properties>
```

> **Caveat:** the absolute path is macOS-specific. CI Linux would need `.so`. For MVP this is acceptable; the long-term answer is bundling the cdylib via a Maven classifier / native packaging plugin (Phase-F deliverable).

- [ ] **Step 6.3: Run integration test (should PASS)**

```bash
cd /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn test 2>&1 | tail -30
```

Expected: `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0` for `ForStRsRoundTripIT`.

If FAILURE/ERROR — diagnose:
- `UnsatisfiedLinkError`: `forstrs.native.libpath` not resolving — verify path expansion in surefire-config
- `IllegalStateException: symbol not found`: cdylib symbols missing — re-run `cargo build --release -p forst-rs-ffi`
- `FrsBackendException`: read the status code; debug specific frs_* call

- [ ] **Step 6.4: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/flink
git add flink-state-backends/flink-statebackend-forst-rs/pom.xml flink-state-backends/flink-statebackend-forst-rs/src/test
git commit -m "$(cat <<'EOF'
test(state-forst-rs): FFM round-trip integration test

Adds ForStRsRoundTripIT: opens in-memory ForSt-RS via FFM, asserts
get returns null on absent key, then put/get round-trip on
"hello" -> "world". Wires forstrs.native.libpath into surefire pointing
to ${user.home}/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib.

This test is the proof point for v3.2 Phase-A acceptance: cdylib loads
via JDK 25 FFM, opaque handles cross the boundary, status codes translate
correctly, FrsBytes struct is read back successfully.

Caveat: surefire native-lib path is macOS-absolute; CI Linux variant
deferred to Phase-F GHA delta (cross-repo integration-ci.yml).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Update Stage 3 verdict in A1 + SESSION_HANDOFF

**Files:**
- Modify: `~/Code/stczwd/ForSt/docs/superpowers/planning/v3.2/reports/A1_status_assessment.md` (§5.3)
- Modify: `~/Code/stczwd/ForSt/.planning/refactor-review/SESSION_HANDOFF.md` (v3.2 wrap banner)

- [ ] **Step 7.1: Update A1 §5.3 Stage 3 verdict**

Change "**Stage 3 (dual-repo integration skeleton): ❌ Not Done**" to "**Stage 3 (dual-repo integration skeleton): 🟡 Partial-strong (MVP done 2026-05-08)**". Add new evidence row referencing the integration test, the Maven module, the FFM Linker, and the ForStRsStateBackend skeleton SPI registration. Note that full L4/L5 (5 state types, Async/Sync) remain Phase-D work.

- [ ] **Step 7.2: Update SESSION_HANDOFF v3.2 wrap banner**

Append to the wrap banner: "Phase-A Flink bootstrap (L1+L2+L3 MVP) landed 2026-05-08; integration test green; Stage 3 verdict 🟡 Partial-strong."

- [ ] **Step 7.3: Commit (in ForSt repo)**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
git add docs/superpowers/planning/v3.2/reports/A1_status_assessment.md .planning/refactor-review/SESSION_HANDOFF.md
git commit -m "$(cat <<'EOF'
docs(v3.2): Phase-A Stage 3 verdict 🟡 Partial-strong after Flink bootstrap

After L1+L2+L3 MVP landed in Flink repo (forst-rs-jdk25 branch) with
ForStRsRoundTripIT green, Stage 3 (dual-repo integration skeleton) moves
from ❌ Not Done to 🟡 Partial-strong.

Evidence:
- flink-statebackend-forst-rs Maven module exists, registered in
  flink-state-backends parent pom
- module-info.java + 7 FFM downcall handles via Linker
- ForStRsStateBackend skeleton + ForStRsStateBackendFactory SPI
- ForStRsRoundTripIT round-trip via JDK 25 FFM
- Build artifact: libforst_rs_ffi.dylib loads cleanly

Open: full StateBackend impl (5 state types, Async v2 + Sync v1, Checkpoint
integration) deferred to Phase-D L4/L5.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Final verification

- [ ] **Step 8.1: Re-run integration test**

```bash
cd /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn test 2>&1 | tail -15
```

Expected: `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`.

- [ ] **Step 8.2: Verify ForSt cargo workspace still green**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
cargo check --workspace 2>&1 | tail -5
```

Expected: `Finished dev profile`, exit 0.

- [ ] **Step 8.3: git status both repos clean**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt && git status --short
cd /Users/lijunqing/Code/stczwd/flink && git status --short
```

Expected: both empty (all changes committed).

---

## Self-Review (post-write)

**1. Spec coverage** (against B1 §6.11 L1 + §6.12 L2 + §6.13 L3 acceptance):
- L1 module bootstrap: ✅ Task 1
- L2 FFM Linker + module-info: ✅ Tasks 2 + 4
- L3 ForStRsStateBackend + Factory: ✅ Task 5
- Integration test: ✅ Task 6 (proof point)

**2. Placeholder scan**: zero "TBD"/"TODO"/"implement later". All code blocks complete. The two `UnsupportedOperationException` throws in `ForStRsStateBackend` are EXPLICIT skeletons (per MVP scope) — they are not placeholders, they are deliberate pre-conditions that trip if Phase D L4/L5 is skipped.

**3. Type/name consistency**:
- `ForStRsLinker` class — referenced in 4 places (FrsDb, FrsCfHandle, RoundTripIT, javadoc); consistent capitalization ✓
- `FrsDb` / `FrsCfHandle` / `FrsBytes` / `FrsStatus` / `FrsBackendException` — all consistent ✓
- `frsDbOpenMemory` / `frsDbClose` / `frsDbDefaultCf` / `frsCfClose` / `frsPut` / `frsGet` / `frsBytesFree` — 7 MethodHandle fields named consistently ✓
- C ABI symbol names (`frs_db_open_memory` etc.) — verified present in `nm -gU` output earlier ✓

**4. Risk callouts handled in plan**:
- StateBackend abstract method signature drift (Flink 2.2 dual-track v1/v2) — Step 5.2 note covers this
- macOS-absolute path in surefire — Step 6.2 caveat surfaces this; cross-platform deferred to Phase-F
- Cross-repo coordination — explicit cd between repos at each step
