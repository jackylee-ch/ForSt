# Forst-RS V1-Sync stateCache Per-Event Clear Regression Fix — PR-A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate `ForStRsKeyedStateBackend.stateCache.clear()` per-event invalidation by switching `getValueState` to the keyComputer-mode constructor, recovering the V1-sync allocation pattern community ForSt achieves. Target: Q5 < 113.98 s, Q8 < 32.81 s, Q13 < 33.75 s.

**Architecture:** PR-A internal sequence A0 → A1 → A2, following RocksDB/LevelDB "new-code-commit / behavior-switch-commit" separation. A0 is verification-only (no production code change); A1 adds the new constructor additively (zero drift required); A2 flips the call site and removes the one-line `stateCache.clear()`. All three commits land together as PR-A.

**Tech Stack:** Java 25, JUnit 5, Flink 2.2.1, JDK 25 FFM, G1GC; Nexmark Q0-Q23 sweep on local fresh-cluster strategy; bench compare via tiered T0/T1/T2 gates + `net_delta = Σ log10(new/old)` formula.

**Spec:** [`docs/superpowers/specs/2026-05-20-forst-rs-v1-sync-state-cache-fix-design.md`](../specs/2026-05-20-forst-rs-v1-sync-state-cache-fix-design.md)

**Working branches:**
- `~/Code/stczwd/flink` branch `forst-rs-jdk25` (Java backend)
- `~/Code/stczwd/ForSt` branch `forst-rs` (Rust engine + benches + docs)

---

## File map

**Modified:**
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java` — new constructor + cached `stateNameBytes` field (A1)
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java` — `getValueState` rewires to new ctor; `setCurrentKey` removes `stateCache.clear()` line 288 (A2)

**Created:**
- `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ForStRsValueStateNewCtorTest.java` — A1 unit tests for the additive constructor
- `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackendCacheTest.java` — A2 unit tests: state-instance identity, allocation budget, key-group correctness
- `docs/superpowers/audits/2026-05-20-A0-Q11Q12-V2-path-attestation.md` — A0 attestation file with captured counter readings + 3-gate evaluation

**Temporarily modified (added then removed within A0):**
- `ForStRsValueStateV2.java`, `ForStRsValueState.java`, `ForStRsKeyedStateBackend.java` — temporary `AtomicLong` counters and a shutdown-hook stderr dump. Removed before A0 commit; only the attestation file is committed.

**Bench artifacts (not committed, captured in attestation/results):**
- `/tmp/pr-a-bench-results/` — per-commit Nexmark runs

---

## Task 1 (Commit A0): Q11/Q12 V2-async-path instrumented verification

**Goal:** Prove that Q11 and Q12 traverse `ForStRsValueStateV2` (V2 async path), not `ForStRsValueState` (V1 sync), so that A2's removal of `stateCache.clear()` cannot affect their measured numbers. Three falsifiable gates must all pass before A2 is allowed.

**Files:**
- Modify (temp): `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueStateV2.java`
- Modify (temp): `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java`
- Modify (temp): `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java`
- Create: `docs/superpowers/audits/2026-05-20-A0-Q11Q12-V2-path-attestation.md`

- [ ] **Step 1: Read ForStRsValueStateV2 to identify value/update entry points**

```bash
grep -n "public.*value\|public.*update\|class ForStRsValueStateV2" \
  ~/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueStateV2.java
```

Capture the line numbers of the public `value()` and `update(...)` method declarations for counter placement in Step 2.

- [ ] **Step 2: Add temporary counters to V2 ValueState**

Edit `ForStRsValueStateV2.java`. Add at the top of the class body (after the existing private fields, before the constructors):

```java
// TEMP A0 verification — counters dumped at JVM shutdown; removed before A0 commit.
public static final java.util.concurrent.atomic.AtomicLong A0_V2_VALUE = new java.util.concurrent.atomic.AtomicLong();
public static final java.util.concurrent.atomic.AtomicLong A0_V2_UPDATE = new java.util.concurrent.atomic.AtomicLong();
```

At the FIRST line of the public `value(...)` method body (inside the method, before any other statement), add:

```java
A0_V2_VALUE.incrementAndGet();
```

At the FIRST line of the public `update(...)` method body, add:

```java
A0_V2_UPDATE.incrementAndGet();
```

- [ ] **Step 3: Add temporary counters to V1 ValueState**

Edit `ForStRsValueState.java`. Add at the top of the class body (after `private byte[] lastValueKey;` at line 164):

```java
// TEMP A0 verification — removed before A0 commit.
public static final java.util.concurrent.atomic.AtomicLong A0_V1_VALUE = new java.util.concurrent.atomic.AtomicLong();
public static final java.util.concurrent.atomic.AtomicLong A0_V1_UPDATE = new java.util.concurrent.atomic.AtomicLong();
```

At the first line of `value()` (line 168, immediately after the method opening brace), add:

```java
A0_V1_VALUE.incrementAndGet();
```

At the first line of `update(T value)` (line 196), add:

```java
A0_V1_UPDATE.incrementAndGet();
```

- [ ] **Step 4: Add temporary counter to backend.setCurrentKey + shutdown hook**

Edit `ForStRsKeyedStateBackend.java`. Add at the top of the class body (near other static fields, e.g. above `private static final int WRITE_BUFFER_FLUSH_THRESHOLD`):

```java
// TEMP A0 verification — removed before A0 commit.
public static final java.util.concurrent.atomic.AtomicLong A0_SET_CURRENT_KEY = new java.util.concurrent.atomic.AtomicLong();

static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        System.err.println("===A0-COUNTERS===");
        System.err.println("V2_VALUE=" + org.apache.flink.state.forstrs.state.ForStRsValueStateV2.A0_V2_VALUE.get());
        System.err.println("V2_UPDATE=" + org.apache.flink.state.forstrs.state.ForStRsValueStateV2.A0_V2_UPDATE.get());
        System.err.println("V1_VALUE=" + org.apache.flink.state.forstrs.state.ForStRsValueState.A0_V1_VALUE.get());
        System.err.println("V1_UPDATE=" + org.apache.flink.state.forstrs.state.ForStRsValueState.A0_V1_UPDATE.get());
        System.err.println("SET_CURRENT_KEY=" + A0_SET_CURRENT_KEY.get());
        System.err.println("===END-A0-COUNTERS===");
    }, "A0-counter-dump"));
}
```

At the first line of `setCurrentKey(K newKey)` (line 269, immediately after the opening brace), add:

```java
A0_SET_CURRENT_KEY.incrementAndGet();
```

- [ ] **Step 5: Build the backend jar with temp counters**

```bash
cd ~/Code/stczwd/flink
mvn -pl flink-state-backends/flink-statebackend-forst-rs -am install -DskipTests -T 4
```

Expected: BUILD SUCCESS. The jar at `flink-state-backends/flink-statebackend-forst-rs/target/flink-statebackend-forst-rs-2.2.0.jar` exists.

- [ ] **Step 6: Deploy jar to Flink lib**

```bash
cp ~/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/target/flink-statebackend-forst-rs-2.2.0.jar \
   /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/flink-statebackend-forst-rs-2.2.0.jar
```

- [ ] **Step 7: Run Nexmark Q11 with fresh cluster, capture TaskManager stderr**

```bash
cd ~/Code/stczwd/ForSt
mkdir -p /tmp/a0-attestation
# Q11 isolated run, fresh cluster
bash <<'EOF' 2>&1 | tee /tmp/a0-attestation/q11.log
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home

cp "$FLINK_HOME"/conf/templates/config-forst-rs-local.yaml.tpl "$FLINK_HOME"/conf/config.yaml
sed -i '' 's/-XX:+UseG1GC -XX:+UseCompactObjectHeaders/-XX:+UseG1GC/g' "$FLINK_HOME"/conf/config.yaml
sed -i '' 's/-XX:+UseZGC/-XX:+UseG1GC/g' "$FLINK_HOME"/conf/config.yaml
cat >> "$FLINK_HOME"/conf/config.yaml <<'CONF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
CONF

"$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null
"$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null
pkill -9 -f Benchmark || true
pkill -9 -f TaskManagerRunner || true
pkill -9 -f StandaloneSession || true
pkill -9 -f SqlGateway || true
sleep 4
rm -rf /tmp/flink-forst-rs-data /tmp/flink-forst-rs-cache /tmp/nexmark-checkpoints-forst-rs

"$FLINK_HOME"/bin/start-cluster.sh
sleep 6
"$FLINK_HOME"/bin/sql-gateway.sh start
sleep 8

"$NEXMARK_HOME"/bin/run_query.sh oa q11
sleep 5

"$FLINK_HOME"/bin/stop-cluster.sh
"$FLINK_HOME"/bin/sql-gateway.sh stop
sleep 4

# Counter dump fires on JVM shutdown (TaskManagerRunner stderr → log file)
cp "$FLINK_HOME"/log/flink-*-taskexecutor-*.log /tmp/a0-attestation/q11-tm.log
grep -A 10 "===A0-COUNTERS===" /tmp/a0-attestation/q11-tm.log
EOF
```

Expected: stderr from TM contains a block:
```
===A0-COUNTERS===
V2_VALUE=<some large number>
V2_UPDATE=<some large number>
V1_VALUE=0
V1_UPDATE=0
SET_CURRENT_KEY=<some large number>
===END-A0-COUNTERS===
```

- [ ] **Step 8: Run Nexmark Q12 with fresh cluster, capture counters**

Same as Step 7 but with `q12` replacing `q11` and outputs to `/tmp/a0-attestation/q12.log` and `/tmp/a0-attestation/q12-tm.log`. Capture the `===A0-COUNTERS===` block.

- [ ] **Step 9: Evaluate the three falsifiable gates**

For EACH of Q11 and Q12, extract the five counter values and evaluate:

| Gate | Pass condition | Result |
|---|---|---|
| 1 Path identity | `V2_VALUE + V2_UPDATE > 0` AND `V1_VALUE + V1_UPDATE == 0` | PASS / FAIL |
| 2 Cardinality sanity | `V2_VALUE + V2_UPDATE` is within 2× of (input_records × expected_ops_per_record). Nexmark default is 100 M input records. For Q11 expect ≈ 1 op/record; for Q12 expect ≈ 1 op/record. So expected range is `[5e7, 2e8]`. | PASS / FAIL |
| 3 setCurrentKey cross-check | `SET_CURRENT_KEY` is within factor of 2 of `100e6`, AND `(V2_VALUE + V2_UPDATE)` is within factor of 2 of `SET_CURRENT_KEY × expected_ops` | PASS / FAIL |

If ANY gate fails on either Q11 or Q12, HALT the plan. Do not proceed to A1/A2. Investigate the failure (V2 path not actually used? counters wired wrong? per-record op assumption wrong?) and update the spec accordingly.

- [ ] **Step 10: Revert all temporary counter changes**

```bash
cd ~/Code/stczwd/flink
git checkout flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueStateV2.java
git checkout flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java
git checkout flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java
git status   # must show no modified files in main/java
```

- [ ] **Step 11: Rebuild and redeploy clean jar**

```bash
cd ~/Code/stczwd/flink
mvn -pl flink-state-backends/flink-statebackend-forst-rs -am install -DskipTests -T 4
cp ~/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/target/flink-statebackend-forst-rs-2.2.0.jar \
   /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/flink-statebackend-forst-rs-2.2.0.jar
```

- [ ] **Step 12: Write A0 attestation markdown**

Create `~/Code/stczwd/ForSt/docs/superpowers/audits/2026-05-20-A0-Q11Q12-V2-path-attestation.md` with the following content (replacing `<…>` with values captured in Step 7-9):

```markdown
# A0 Attestation — Q11/Q12 V2-async-path verification

**Date:** 2026-05-20
**Spec:** [V1-Sync state cache fix](../specs/2026-05-20-forst-rs-v1-sync-state-cache-fix-design.md)
**Purpose:** Verify Q11/Q12 traverse the V2 async ValueState path before PR-A's A2 commit removes `stateCache.clear()`.

## Captured counter readings

### Q11

| Counter | Value |
|---|---|
| `V2_VALUE` | `<n>` |
| `V2_UPDATE` | `<n>` |
| `V1_VALUE` | `<n>` |
| `V1_UPDATE` | `<n>` |
| `SET_CURRENT_KEY` | `<n>` |

### Q12

| Counter | Value |
|---|---|
| `V2_VALUE` | `<n>` |
| `V2_UPDATE` | `<n>` |
| `V1_VALUE` | `<n>` |
| `V1_UPDATE` | `<n>` |
| `SET_CURRENT_KEY` | `<n>` |

## Gate evaluation

### Q11

- **Gate 1 Path identity** (V2 > 0 AND V1 == 0): `<PASS|FAIL>`
- **Gate 2 Cardinality sanity** (V2 sum within 2× of 100 M × per-record-ops): `<PASS|FAIL>`
- **Gate 3 setCurrentKey cross-check** (V2 sum within 2× of SET_CURRENT_KEY × expected_ops): `<PASS|FAIL>`

### Q12

- **Gate 1 Path identity**: `<PASS|FAIL>`
- **Gate 2 Cardinality sanity**: `<PASS|FAIL>`
- **Gate 3 setCurrentKey cross-check**: `<PASS|FAIL>`

## Conclusion

`<All six gates PASS — proceeding to A1.>` OR `<Gate X failed for Q? — see investigation notes.>`

## Reproducibility

To rerun this verification, follow Task 1 in `docs/superpowers/plans/2026-05-20-forst-rs-v1-sync-cache-fix-PR-A-plan.md`. The counter-scaffold patches in Steps 2-4 must be re-applied and removed.
```

- [ ] **Step 13: Commit A0**

```bash
cd ~/Code/stczwd/ForSt
git add docs/superpowers/audits/2026-05-20-A0-Q11Q12-V2-path-attestation.md
git commit -m "$(cat <<'EOF'
docs: A0 — Q11/Q12 V2-async-path attestation (PR-A precondition)

Captures the counter readings from a temporary-instrumented Nexmark
Q11/Q12 run and evaluates the three falsifiable gates from spec
2026-05-20-forst-rs-v1-sync-state-cache-fix-design.md:

  Gate 1 Path identity:        V2 > 0 AND V1 == 0
  Gate 2 Cardinality sanity:   V2 sum within 2x of 100M x ops/record
  Gate 3 setCurrentKey cross:  V2 sum within 2x of setCurrentKey * ops

All six per-query gates PASS, so A2's removal of stateCache.clear()
cannot affect Q11/Q12 observed numbers. The temporary counter scaffold
was reverted; only this attestation lands.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2 (Commit A1): Additive constructor in ForStRsValueState (zero-drift)

**Goal:** Add a new constructor combining keyComputer mode with write-buffer hooks, cache `stateNameBytes` once per state instance, and confirm that adding this dead code produces zero drift on the Nexmark sweep. No call site changes — `getValueState` still uses the legacy constructor.

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ForStRsValueStateNewCtorTest.java`

- [ ] **Step 1: Add `stateNameBytes` field + new constructor to ForStRsValueState**

Edit `ForStRsValueState.java`. After the existing `private final Supplier<byte[]> keyComputer;` field (~line 67), add:

```java
    /** Pre-encoded UTF-8 bytes of the state name; cached once at construction (A1+B2). */
    private final byte[] stateNameBytes;
```

After the existing keyComputer-mode constructor that ends at line 156 (the one that takes `Supplier<byte[]> keyComputer`), add this NEW constructor that takes BOTH keyComputer AND write-buffer hooks:

```java
    /**
     * A1 additive constructor: kg-prefixed lazy key compute (Supplier-based) AND write-buffer
     * hooks. Used by ForStRsKeyedStateBackend.getValueState after A2 lands; until A2 this
     * constructor is dead code, ensuring A1's bench delta is zero.
     *
     * @param stateName the state name; used to pre-cache UTF-8 bytes for future encoder overloads
     */
    public ForStRsValueState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            TypeSerializer<T> serializer,
            Supplier<byte[]> keyComputer,
            String stateName,
            Function<byte[], byte[]> writeBufferGet,
            java.util.function.BiConsumer<byte[], byte[]> writeBufferPut,
            Consumer<byte[]> writeBufferDelete) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = null;
        this.serializer = serializer;
        this.keyComputer = keyComputer;
        this.stateNameBytes = stateName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
        this.writeBufferGet = writeBufferGet;
        this.writeBufferPut = writeBufferPut;
        this.writeBufferDelete = writeBufferDelete;
    }
```

Also update the three EXISTING constructors to initialize `stateNameBytes = null` (since they don't take a stateName):

In the constructor at line 89, add after the existing field assignments:
```java
        this.stateNameBytes = null;
```

In the constructor at line 112, add after the existing field assignments:
```java
        this.stateNameBytes = null;
```

In the constructor at line 139, add after the existing field assignments:
```java
        this.stateNameBytes = null;
```

- [ ] **Step 2: Add an accessor for tests**

At the bottom of the class (after `computeKey()` method at line 251), add:

```java
    /** Test accessor — returns the cached state-name UTF-8 bytes, or null in legacy modes. */
    byte[] stateNameBytesForTest() {
        return stateNameBytes;
    }
```

- [ ] **Step 3: Create unit test for the new constructor**

Create `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ForStRsValueStateNewCtorTest.java`:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forstrs.state;

import org.apache.flink.api.common.typeutils.base.LongSerializer;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A1 — verifies the new additive constructor on {@link ForStRsValueState} caches
 * {@code stateNameBytes} as UTF-8 of the supplied state name, and accepts both keyComputer
 * and write-buffer hooks together.
 */
class ForStRsValueStateNewCtorTest {

    @Test
    void newCtorCachesStateNameBytes() {
        String stateName = "my-state";
        ForStRsValueState<Long> state =
                new ForStRsValueState<>(
                        /* linker */ null,
                        /* db */ null,
                        /* cf */ null,
                        LongSerializer.INSTANCE,
                        /* keyComputer */ () -> new byte[] {1, 2, 3},
                        stateName,
                        /* writeBufferGet */ k -> null,
                        /* writeBufferPut */ (k, v) -> {},
                        /* writeBufferDelete */ k -> {});

        byte[] cached = state.stateNameBytesForTest();
        assertNotNull(cached, "stateNameBytes must be cached, not null");
        assertArrayEquals(
                stateName.getBytes(StandardCharsets.UTF_8),
                cached,
                "cached bytes must equal UTF-8 of stateName");
    }
}
```

- [ ] **Step 4: Compile and run the new unit test**

```bash
cd ~/Code/stczwd/flink
mvn -pl flink-state-backends/flink-statebackend-forst-rs test \
  -Dtest=ForStRsValueStateNewCtorTest -DfailIfNoTests=false
```

Expected: `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`.

- [ ] **Step 5: Run the full forst-rs backend test suite**

```bash
cd ~/Code/stczwd/flink
mvn -pl flink-state-backends/flink-statebackend-forst-rs test
```

Expected: BUILD SUCCESS. All existing tests pass.

- [ ] **Step 6: Deploy A1 jar and run Nexmark zero-drift bench (Q5/Q11/Q12)**

```bash
cd ~/Code/stczwd/flink
mvn -pl flink-state-backends/flink-statebackend-forst-rs -am install -DskipTests -T 4
cp ~/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/target/flink-statebackend-forst-rs-2.2.0.jar \
   /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/flink-statebackend-forst-rs-2.2.0.jar
```

Write the bench runner to `/tmp/a1-bench/run.sh`:

```bash
mkdir -p /tmp/a1-bench
cat > /tmp/a1-bench/run.sh <<'BENCHSCRIPT'
#!/usr/bin/env bash
set -uo pipefail
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
QUERY_TIMEOUT=900
OUTDIR=/tmp/a1-bench

restart() {
    "$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null
    "$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null
    pkill -9 -f Benchmark 2>/dev/null || true
    pkill -9 -f TaskManagerRunner 2>/dev/null || true
    pkill -9 -f StandaloneSession 2>/dev/null || true
    pkill -9 -f SqlGateway 2>/dev/null || true
    sleep 4
    rm -rf /tmp/flink-forst-rs-data /tmp/flink-forst-rs-cache /tmp/nexmark-checkpoints-forst-rs
    "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
    sleep 6
    "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
    sleep 8
    for i in 1 2 3 4 5; do
        curl -sf http://localhost:8081/jobs >/dev/null 2>&1 && return 0
        sleep 3
    done
    return 1
}

run_q() {
    local q=$1
    local out=$OUTDIR/${q}.out
    echo "=== [$(date +%H:%M:%S)] $q ==="
    restart || { echo "  cluster start failed"; return 1; }
    "$NEXMARK_HOME"/bin/run_query.sh oa "$q" > "$out" 2>&1 &
    local pid=$!
    local t=0
    while [[ $t -lt $QUERY_TIMEOUT ]]; do
        kill -0 $pid 2>/dev/null || break
        grep -q "^|$q " "$out" 2>/dev/null && { sleep 3; break; }
        sleep 15; t=$((t+15))
    done
    kill -0 $pid 2>/dev/null && { kill -9 $pid; pkill -9 -f Benchmark; sleep 3; echo "  WATCHDOG ${t}s"; }
    local line=$(grep "^|$q " "$out" 2>/dev/null | head -1)
    local time_s=$(echo "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$5); print $5}')
    local thr=$(echo "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$7); print $7}')
    echo "  $q -> time=${time_s}s thr=${thr}"
}

run_q q5
run_q q11
run_q q12

"$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null
"$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null
echo "=== DONE ==="
BENCHSCRIPT
chmod +x /tmp/a1-bench/run.sh
/tmp/a1-bench/run.sh 2>&1 | tee /tmp/a1-bench/run.log
```

Expected: `q5 -> time=… q11 -> time=… q12 -> time=…` lines at the end of `run.log`, with per-query out files in `/tmp/a1-bench/q5.out`, `q11.out`, `q12.out`. Capture the three wall-clock values for Step 7.

- [ ] **Step 7: Verify A1 zero-drift gate**

Compare A1 numbers against v3.3 baseline:

| Query | v3.3 baseline | A1 measured | Drift (%) | Tier |
|---|---|---|---|---|
| Q5  | 586.68 s | `<?>` | `<?>` | must be < ±5% — A1 changes are dead code at call sites |
| Q11 | 76.5 s   | `<?>` | `<?>` | must be < ±5% |
| Q12 | 35.5 s   | `<?>` | `<?>` | must be < ±5% |

If any drift exceeds ±5%, HALT and investigate (likely a class-loader / static-init side effect). Drift > ±5% means the new constructor is NOT actually dead code and the A2 attribution will be compromised.

- [ ] **Step 8: Commit A1**

```bash
cd ~/Code/stczwd/flink
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java \
  flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ForStRsValueStateNewCtorTest.java
git commit -m "$(cat <<'EOF'
feat(state-forst-rs): A1 — additive ctor on ForStRsValueState (zero-drift)

Adds a 4th constructor combining keyComputer mode + write-buffer hooks
+ cached stateNameBytes. No call sites changed: getValueState() still
uses the legacy static-prefix ctor, so this commit MUST show zero
drift on the Nexmark sweep. Confirmed-zero-drift becomes the audit
baseline for A2's behavior-switch attribution.

stateNameBytes is cached for B2's encodeForState overload but currently
unused; pre-staging the field keeps A1 strictly additive at the
production call sites while preparing the slot for the B chain.

PR-A from spec: docs/superpowers/specs/2026-05-20-forst-rs-v1-sync-state-cache-fix-design.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3 (Commit A2): Switch `getValueState` to new ctor + remove `stateCache.clear()`

**Goal:** Flip the behavior. `getValueState` now constructs ValueState via the keyComputer-mode ctor; `setCurrentKey` no longer clears the state cache. This is the minimum-diff commit that delivers the main-fix gain.

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackendCacheTest.java`

- [ ] **Step 1: Switch getValueState to the new constructor**

In `ForStRsKeyedStateBackend.java`, replace lines 325-336 (the `getValueState` body that calls `buildPrefix(stateName)` and `new ForStRsValueState<>(linker, db, defaultCf, prefix, ...)`) with:

```java
        ForStRsValueState<T> created =
                new ForStRsValueState<>(
                        linker,
                        db,
                        defaultCf,
                        valueSerializer,
                        () -> kgSerializer.encodeForState(
                                getCurrentKeyGroupIndex(), getCurrentKey(), stateName),
                        stateName,
                        this::getFromWriteBuffer,
                        this::putToWriteBuffer,
                        this::deleteFromWriteBuffer);
        stateCache.put(valueStateCacheKey(stateName), created);
        return created;
```

The `byte[] prefix = buildPrefix(stateName);` line is no longer needed for getValueState (other state types still use it; leave the buildPrefix method intact).

If `kgSerializer` isn't a backend field, locate the existing `ForStRsKeyGroupedSerializer<K> kgSerializer` field on the backend (search `kgSerializer` in this file) and use it; if not present, add it as a private final field populated in the constructor from existing key-serializer wiring (use `ForStRsKeyGroupedSerializer<>(keySerializer)` if a fresh instance is acceptable).

- [ ] **Step 2: Remove `stateCache.clear()` in setCurrentKey**

In `ForStRsKeyedStateBackend.java` at line 288, DELETE the line:

```java
        this.stateCache.clear();
```

Also delete the immediately preceding comment line (line 287):

```java
        // Invalidate the per-state cache because every entry's keyPrefix encodes the old key.
```

Replace those two lines with:

```java
        // Per-state ForStRsValueState now lazily computes its composite key from currentKeyBytes
        // (kgSerializer.encodeForState) on every value()/update()/clear() call, so the cache
        // entry survives key changes. setCurrentKey only bumps keyGeneration for stale-adapter
        // detection.
```

- [ ] **Step 3: Create A2 unit tests — state-instance identity**

Create `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackendCacheTest.java`:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.state.forstrs.state.ForStRsValueState;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * A2 — verifies setCurrentKey no longer wipes the per-stateName state cache, so the same
 * ValueState instance survives across key changes.
 */
class ForStRsKeyedStateBackendCacheTest {

    @Test
    void valueStateInstanceSurvivesKeyChange() {
        ForStRsKeyedStateBackend<Long> backend = TestBackendFactory.openInMemory(LongSerializer.INSTANCE);
        try {
            backend.setCurrentKey(1L);
            ForStRsValueState<Long> v1 = backend.getValueState("accumulator", LongSerializer.INSTANCE);
            assertNotNull(v1);

            backend.setCurrentKey(2L);
            ForStRsValueState<Long> v2 = backend.getValueState("accumulator", LongSerializer.INSTANCE);

            assertSame(v1, v2, "ValueState instance must survive setCurrentKey");
        } finally {
            backend.close();
        }
    }
}
```

If `TestBackendFactory.openInMemory` does not already exist (verify with `grep -r "TestBackendFactory" flink-state-backends/flink-statebackend-forst-rs/src/test/`), create it as a sibling file `TestBackendFactory.java` in the same package. Model on the existing `ForStRsSnapshotStrategyTest`'s backend-opening boilerplate (grep `new ForStRsKeyedStateBackend` in `src/test/`). Minimal viable factory:

```java
/*
 * Apache 2.0 license header omitted for brevity — copy from a sibling test file.
 */
package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.nio.file.Files;
import java.nio.file.Path;

final class TestBackendFactory {
    private TestBackendFactory() {}

    static <K> ForStRsKeyedStateBackend<K> openInMemory(TypeSerializer<K> keySerializer) throws Exception {
        Path tmp = Files.createTempDirectory("a2-test-");
        ForStRsLinker linker = ForStRsLinker.load();
        FrsDb db = linker.openDb(tmp.toString());
        FrsCfHandle cf = linker.defaultColumnFamily(db);
        return new ForStRsKeyedStateBackend<>(
                linker,
                db,
                cf,
                keySerializer,
                /* maxParallelism */ 128,
                /* keyGroupRange */ KeyGroupRange.of(0, 127));
    }

    static long keyInGroup(int targetGroup) {
        for (long candidate = 1; candidate < 1_000_000L; candidate++) {
            int kg = KeyGroupRangeAssignment.assignToKeyGroup(candidate, 128);
            if (kg == targetGroup) return candidate;
        }
        throw new IllegalStateException("no key in group " + targetGroup);
    }
}
```

If `ForStRsKeyedStateBackend`'s public constructor signature differs from the one above, adjust the factory to call whatever constructor / builder the existing snapshot test uses. The point is the same: open a backend pointing at a tmp dir, hand back the instance.

- [ ] **Step 4: Add allocation-budget test to the same test class**

Append to `ForStRsKeyedStateBackendCacheTest.java`:

```java
    @Test
    void valueStateAllocationCountIsBounded() throws Exception {
        ForStRsKeyedStateBackend<Long> backend = TestBackendFactory.openInMemory(LongSerializer.INSTANCE);
        try {
            // Burn 100K alternating-key calls; ValueState instance count must stay at 1
            // (one per stateName, not one per event).
            backend.setCurrentKey(1L);
            ForStRsValueState<Long> first = backend.getValueState("acc", LongSerializer.INSTANCE);

            java.util.Set<ForStRsValueState<Long>> distinctInstances =
                    java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
            distinctInstances.add(first);

            for (long k = 2; k <= 100_000L; k++) {
                backend.setCurrentKey(k);
                ForStRsValueState<Long> v = backend.getValueState("acc", LongSerializer.INSTANCE);
                distinctInstances.add(v);
            }

            org.junit.jupiter.api.Assertions.assertEquals(
                    1, distinctInstances.size(),
                    "expected exactly 1 ValueState instance across 100K key changes; saw "
                            + distinctInstances.size());
        } finally {
            backend.close();
        }
    }
```

- [ ] **Step 5: Add key-group correctness test to the same test class**

Append to `ForStRsKeyedStateBackendCacheTest.java`:

```java
    @Test
    void valueStateReadsDifferentValuesAcrossKeyGroups() throws Exception {
        ForStRsKeyedStateBackend<Long> backend = TestBackendFactory.openInMemory(LongSerializer.INSTANCE);
        try {
            // Pick two keys that hash into DIFFERENT key-groups for the default maxParallelism.
            long keyA = TestBackendFactory.keyInGroup(0);
            long keyB = TestBackendFactory.keyInGroup(1);

            backend.setCurrentKey(keyA);
            ForStRsValueState<Long> state = backend.getValueState("kg-test", LongSerializer.INSTANCE);
            state.update(100L);

            backend.setCurrentKey(keyB);
            state.update(200L);

            backend.setCurrentKey(keyA);
            org.junit.jupiter.api.Assertions.assertEquals(100L, state.value(),
                    "keyA must read its own value, not keyB's");

            backend.setCurrentKey(keyB);
            org.junit.jupiter.api.Assertions.assertEquals(200L, state.value(),
                    "keyB must read its own value, not keyA's");
        } finally {
            backend.close();
        }
    }
```

NOTE: `TestBackendFactory.keyInGroup(int)` is a helper that returns a `long` whose `KeyGroupRangeAssignment.assignToKeyGroup` falls into the requested group for the backend's `maxParallelism`. If absent, add it in the same test file by iterating candidate values until the assignment matches.

- [ ] **Step 6: Compile and run all new A2 unit tests**

```bash
cd ~/Code/stczwd/flink
mvn -pl flink-state-backends/flink-statebackend-forst-rs test \
  -Dtest=ForStRsKeyedStateBackendCacheTest -DfailIfNoTests=false
```

Expected: `Tests run: 3, Failures: 0, Errors: 0`. All three tests pass.

- [ ] **Step 7: Run the full forst-rs backend test suite**

```bash
cd ~/Code/stczwd/flink
mvn -pl flink-state-backends/flink-statebackend-forst-rs test
```

Expected: BUILD SUCCESS. All existing tests still pass.

- [ ] **Step 8: Deploy A2 jar**

```bash
cd ~/Code/stczwd/flink
mvn -pl flink-state-backends/flink-statebackend-forst-rs -am install -DskipTests -T 4
cp ~/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/target/flink-statebackend-forst-rs-2.2.0.jar \
   /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/flink-statebackend-forst-rs-2.2.0.jar
```

- [ ] **Step 9: Run the full Nexmark Q0-Q23 sweep with fresh-cluster strategy**

```bash
cd ~/Code/stczwd/ForSt
mkdir -p /tmp/a2-bench
# Reuse the fresh-cluster script; ensure summary.csv goes to /tmp/a2-bench/summary.csv
SUMMARY_OVERRIDE=/tmp/a2-bench/summary.csv \
  bash scripts/bench-forst-rs-fresh-cluster.sh 2>&1 | tee /tmp/a2-bench/sweep.log
```

If the script does not honor `SUMMARY_OVERRIDE`, edit a copy at `/tmp/a2-bench/run.sh` setting `OUTDIR=/tmp/a2-bench`.

- [ ] **Step 10: Evaluate the tiered T0/T1/T2 bench gates**

Compare A2 results in `/tmp/a2-bench/summary.csv` against v3.3:

```bash
cat /tmp/a2-bench/summary.csv | awk -F, '/^forst-rs/{print}' > /tmp/a2-bench/forst-rs.csv
```

For each query, compute `ratio = A2_time / v3.3_time`. Then evaluate:

- **T0 (mandatory revert):** any query whose v3.3 ratio vs rocksdb was ≥ 1.0× and whose A2 ratio drops below 1.0×.
- **T1 (review-triggered):** any query that was ≥ 1.5× win in v3.3 regressing > 10 % (ratio > 1.10).
- **T2 (mandatory revert):** any query with ratio > 1.20.
- **Target wins:** Q5 < 113.98 s AND Q8 < 32.81 s AND Q13 < 33.75 s.
- **Net portfolio delta:** `net_delta = Σ_query log10(A2_time / v3.3_time)`. Smaller (more negative) is better.

```bash
# Compute net_delta and target-win check
awk -F, '
  # external v3.3 baseline times pasted inline
  BEGIN {
    base["q0"]=8.456; base["q1"]=8.467; base["q2"]=9.512; base["q3"]=24.534;
    base["q4"]=46.81; base["q5"]=586.68; base["q7"]=75.234; base["q8"]=53.41;
    base["q9"]=82.4; base["q10"]=120; base["q11"]=76.5; base["q12"]=35.5;
    base["q13"]=42.42; base["q14"]=20; base["q15"]=108; base["q16"]=140;
    base["q17"]=45; base["q18"]=68; base["q19"]=110; base["q20"]=210;
    base["q21"]=45; base["q22"]=38; base["q23"]=180;
    sum = 0
  }
  /^forst-rs/ {
    q=$2; t=$3+0
    if (q in base && t > 0) {
      r = t / base[q]
      delta = log(r) / log(10)
      printf "%s: A2=%.2fs v3.3=%.2fs ratio=%.3f log10=%.3f\n", q, t, base[q], r, delta
      sum += delta
      if (q == "q5" && t > 113.98) print "  ⚠ Q5 KPI miss"
      if (q == "q8" && t > 32.81) print "  ⚠ Q8 KPI miss"
      if (q == "q13" && t > 33.75) print "  ⚠ Q13 KPI miss"
    }
  }
  END { printf "\nnet_delta = %.3f (smaller is better)\n", sum }
' /tmp/a2-bench/forst-rs.csv
```

Replace the `base[…]` inline numbers with whatever's locked-in from the v3.3 update to `2026-05-20-forst-rs-benchmark-report-v3.md` if those values have shifted at run time.

The acceptance call:
- All 3 target wins met AND no T0 OR T2 breach AND net_delta ≤ 0 → APPROVED → proceed to Step 11.
- Target wins met but T1 breach AND net_delta ≤ 0 → review-triggered (PMC decision based on net_delta).
- Any target win missed OR T0 breach OR T2 breach → REVERT A2, see Open Questions.

- [ ] **Step 11: Commit A2**

```bash
cd ~/Code/stczwd/flink
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java \
  flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackendCacheTest.java
git commit -m "$(cat <<'EOF'
fix(state-forst-rs): A2 — drop per-event stateCache.clear() in setCurrentKey

The Java-glue regression that made Nexmark Q5 5.15x slower than rocksdb,
Q8 1.63x, Q13 1.26x. setCurrentKey() previously wiped the per-stateName
state cache on every key change, forcing getValueState() to allocate a
fresh ForStRsValueState + keyPrefix byte[] per event. Community ForSt's
matching path allocates state once per (stateName) and computes the
composite key per call via serializeCurrentKeyWithGroupAndNamespace().

A2 mirrors that pattern: getValueState() now constructs ValueState via
the keyComputer-mode ctor added in A1 (additive zero-drift commit),
and setCurrentKey() drops the cache.clear() line — the cache survives
key changes; per-call composite-key encoding makes that safe.

PR-A from spec docs/superpowers/specs/2026-05-20-forst-rs-v1-sync-state-cache-fix-design.md
Tiered T0/T1/T2 gates met; Q5/Q8/Q13 KPI met; net_delta <= 0.
A0 Q11/Q12 V2-path verification attested in
docs/superpowers/audits/2026-05-20-A0-Q11Q12-V2-path-attestation.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 12: Update v3 benchmark report with A2 numbers**

Append a "v3.4 — PR-A A2 cache-clear fix" section to `~/Code/stczwd/ForSt/docs/superpowers/specs/2026-05-20-forst-rs-benchmark-report-v3.md`:

```markdown
---

## v3.4 update — PR-A A2 (stateCache.clear removal)

Build:
- Java backend SHA: `<git rev-parse HEAD on forst-rs-jdk25>`
- Dylib SHA-256: `<sha256sum of libforst_rs_ffi.dylib>`

### Headline (forst-rs vs rocksdb)

| Query | v3.3 | A2 | Δ | rocksdb | Gate met |
|---|---|---|---|---|---|
| q5  | 586.68 s | `<?>` | `<?>` | 113.98 s | `<KPI met / miss>` |
| q8  | 53.41 s  | `<?>` | `<?>` | 32.81 s  | `<KPI met / miss>` |
| q13 | 42.42 s  | `<?>` | `<?>` | 33.75 s  | `<KPI met / miss>` |

### Portfolio

`net_delta = <…>` (smaller is better). No T0 / T2 breaches; T1 trigger: `<none / list queries>`.

### Per-query (forst-rs only)

<paste output of the per-query awk run from Step 10>
```

- [ ] **Step 13: Commit the v3.4 report update**

```bash
cd ~/Code/stczwd/ForSt
git add docs/superpowers/specs/2026-05-20-forst-rs-benchmark-report-v3.md
git commit -m "$(cat <<'EOF'
docs: v3.4 bench results — PR-A A2 (stateCache.clear removal)

Captures the Nexmark Q0-Q23 sweep on the A2 build that drops
stateCache.clear() in setCurrentKey. Includes per-query
forst-rs/rocksdb/forst comparison, tiered-gate verdict, and
net portfolio delta.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Open Questions / Rollback Procedure

If Step 10's gate evaluation flags T0 or T2 breach (or all 3 target wins miss):

1. Revert A2 commit: `git -C ~/Code/stczwd/flink revert HEAD --no-edit`.
2. Re-deploy the pre-A2 jar.
3. Re-bench Q5/Q8/Q13 to confirm baseline restoration.
4. Update spec with the observed failure mode.
5. Open the Approach-C (Rust engine) follow-up spec.

A0 and A1 do NOT need rollback in this case — A0 is docs-only and A1 is dead code at production call sites.

---

## Hand-off to PR-B/C/D

PR-A landing completes the headline Q5/Q8/Q13 win. The remaining work — propagating the kg-prefixed ctor pattern to ListState/MapState/Reducing/Aggregating (PR-B1), adding the pre-encoded-stateName encoder overload (PR-B2), mutable-wrapper hardening sprint (PR-C), and CONTRIBUTING.md MUST rule + 5-item checklist + net_delta formula (PR-D) — is tracked in the spec's "Implementation summary" table and opens as follow-up plans.
