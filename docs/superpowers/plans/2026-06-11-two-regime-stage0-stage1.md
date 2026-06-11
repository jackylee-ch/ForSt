# Two-Regime Executor — Stage 0 + Stage 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the routing-async correctness race (Stage 0) and ship the two-regime executor with H=1 (Stage 1), per `docs/superpowers/specs/2026-06-11-two-regime-executor-design.md` §4.

**Architecture:** Stage 0 instruments offer-vs-completion accounting at the backend chokepoints, isolates per-run logs, reproduces the q8 stall, and fixes the located defect in the LIST_ADD offer→dispatch→completion chain. Stage 1 replaces the static B-spike env-gates with a dynamic regime switch: LIGHT (pipeline empty) executes inline on the mailbox with staging caches active; HEAVY queues to the single worker FIFO with caches bypassed; the L→H transition flushes dirty staging synchronously on the mailbox while nothing is in flight (race-free by construction).

**Tech Stack:** Java 25 FFM backend (`flink-statebackend-forst-rs`), JUnit 5 + AssertJ, 8c/32g docker bench harness. Scope rule: ONLY `flink-state-backends/flink-statebackend-forst-rs` and `~/Code/stczwd/ForSt` may change.

**Binding constraints (spec §8):** all drains stay vectorized off-heap (`flushTo` = existing batched FFI; no per-row engine crossings); full A/B (speed + per-query out_rows) after complete implementation, before any default flip.

**Environment notes for the executor of this plan:**
- Maven tests SKIP silently without the native lib: always pass
  `-Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib`. `BUILD SUCCESS` without `Tests run:` lines means tests did NOT run.
- Jar deploy is TCC-blocked on the host; use the docker-mount copy shown in Task 5 Step 6.
- Bench runs are exclusive: never run two `run-8c32g.sh` invocations concurrently.
- q8@100M exact band: out_rows = 3,064,4xx (references 3,064,445 / 3,064,457 / 3,064,514).

---

## Stage 0 — root-cause and fix the routing-async input-side stall

Evidence so far (sweep doc 2026-06-11): under `FRS_RS_EXECUTOR=routing-async` (all three
staging mechanisms gated), q8@100M under-emits with `out_rows == LIST_ADD offer count`
(two exact specimens: 2,047,837 and 2,691,677) and LIST_ADD plateaus mid-run while
CLEAR/LIST_GET keep growing ⇒ record processing stalls (AEC waits on completions that are
lost or starved) while timer fires continue. The defect is in the LIST_ADD
offer→dispatch→per-row-completion chain or the AEC-trigger/fullyLoaded interplay.

### Task 1: Completion-accounting diagnostic (offered vs completed per request type)

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/DiagCompletionCounters.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedClassifier.java` (offer side — extend the existing `recordStreamStat` at line ~682)
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java` (completion side)
- Test: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/DiagCompletionCountersTest.java`

- [ ] **Step 1: Write the failing test**

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
package org.apache.flink.state.forstrs;

import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit contract for {@link DiagCompletionCounters}: offered/completed/failed tallies + report. */
class DiagCompletionCountersTest {

    @Test
    void talliesAndReportsImbalance() {
        DiagCompletionCounters.resetForTests();
        DiagCompletionCounters.offered(StateRequestType.LIST_ADD);
        DiagCompletionCounters.offered(StateRequestType.LIST_ADD);
        DiagCompletionCounters.offered(StateRequestType.LIST_GET);
        DiagCompletionCounters.completed(StateRequestType.LIST_ADD);
        DiagCompletionCounters.failed(StateRequestType.LIST_GET);
        String report = DiagCompletionCounters.report();
        // LIST_ADD: offered 2, completed 1 → imbalance flagged with a leading '!'
        assertThat(report).contains("!LIST_ADD off=2 done=1 fail=0");
        // LIST_GET: offered 1, failed 1 → balanced (done+fail == off), no '!'
        assertThat(report).contains(" LIST_GET off=1 done=0 fail=1");
    }

    @Test
    void balancedTypeHasNoImbalanceMarker() {
        DiagCompletionCounters.resetForTests();
        DiagCompletionCounters.offered(StateRequestType.MAP_PUT);
        DiagCompletionCounters.completed(StateRequestType.MAP_PUT);
        assertThat(DiagCompletionCounters.report()).contains(" MAP_PUT off=1 done=1 fail=0");
        assertThat(DiagCompletionCounters.report()).doesNotContain("!MAP_PUT");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs && \
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home ../../mvnw -o -q \
  -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib \
  -Denforcer.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true \
  -Dtest=DiagCompletionCountersTest -DfailIfNoTests=true test
```
Expected: COMPILATION ERROR (`DiagCompletionCounters` does not exist).

- [ ] **Step 3: Implement DiagCompletionCounters**

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
package org.apache.flink.state.forstrs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * STAGE-0 diagnostic (FRS_REENTRY_DIAG=3): per-{@link StateRequestType} offered vs
 * completed vs failed tallies across the whole TM, dumped periodically and on JVM
 * shutdown. Purpose: split the routing-async q8 under-emit into TRIGGER STARVATION
 * (offered plateaus, done==offered) vs LOST COMPLETIONS (done+fail &lt; offered, AEC
 * stalls on the gap). Counters are process-global like the sibling STREAM_STATS; the
 * benchmark runs one job per TM so attribution is unambiguous.
 */
@Internal
public final class DiagCompletionCounters {

    public static final boolean ENABLED = "3".equals(System.getenv("FRS_REENTRY_DIAG"));

    private static final int N = StateRequestType.values().length;
    private static final AtomicLongArray OFFERED = new AtomicLongArray(N);
    private static final AtomicLongArray COMPLETED = new AtomicLongArray(N);
    private static final AtomicLongArray FAILED = new AtomicLongArray(N);

    static {
        if (ENABLED) {
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    () -> System.err.println("[DIAG_COMPLETION final] " + report()),
                                    "frs-diag-completion-dump"));
        }
    }

    private DiagCompletionCounters() {}

    public static void offered(StateRequestType t) {
        long n = OFFERED.incrementAndGet(t.ordinal());
        // Periodic dump every 2^20 offers of any type — cheap, mirrors STREAM_STATS cadence.
        if ((n & ((1L << 20) - 1)) == 0) {
            System.err.println("[DIAG_COMPLETION] " + report());
        }
    }

    public static void completed(StateRequestType t) {
        COMPLETED.incrementAndGet(t.ordinal());
    }

    public static void failed(StateRequestType t) {
        FAILED.incrementAndGet(t.ordinal());
    }

    /** One line per seen type: {@code [!]TYPE off=N done=N fail=N}; '!' marks done+fail < off. */
    public static String report() {
        StringBuilder sb = new StringBuilder();
        StateRequestType[] vals = StateRequestType.values();
        for (int i = 0; i < vals.length; i++) {
            long off = OFFERED.get(i);
            long done = COMPLETED.get(i);
            long fail = FAILED.get(i);
            if (off == 0 && done == 0 && fail == 0) {
                continue;
            }
            sb.append(done + fail < off ? '!' : ' ')
                    .append(vals[i])
                    .append(" off=").append(off)
                    .append(" done=").append(done)
                    .append(" fail=").append(fail);
        }
        return sb.toString();
    }

    /** Test hook. */
    static void resetForTests() {
        for (int i = 0; i < N; i++) {
            OFFERED.set(i, 0);
            COMPLETED.set(i, 0);
            FAILED.set(i, 0);
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Same command as Step 2. Expected: `Tests run: 2, Failures: 0, Errors: 0`.

- [ ] **Step 5: Wire the offer side**

In `VectorizedClassifier.java`, the dispatch entry already has (at ~line 682):
```java
        if (STREAM_STATS) {
            recordStreamStat(type);
        }
```
Change to:
```java
        if (STREAM_STATS) {
            recordStreamStat(type);
        }
        if (DiagCompletionCounters.ENABLED) {
            DiagCompletionCounters.offered(type);
        }
```

- [ ] **Step 6: Wire the completion side**

In `VectorizedExecutor.java`, find every per-row completion helper with:
```bash
grep -n "void completePut\|void completeGet\|void completeIter\|void completeDelete\|completePutExceptionally\|completeGetExceptionally\|markCompletedExceptionally" \
  /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java
```
At the TOP of each `completeXxx(StateRequest<?,?,?,?> req, ...)` helper body add exactly one
line — success helpers:
```java
        if (DiagCompletionCounters.ENABLED) {
            DiagCompletionCounters.completed(req.getRequestType());
        }
```
and each `completeXxxExceptionally(...)` helper:
```java
        if (DiagCompletionCounters.ENABLED) {
            DiagCompletionCounters.failed(req.getRequestType());
        }
```
ALSO wire `drainPendingFuturesExceptionally` in the same file (it fails rows in bulk):
inside its per-request loop bodies add the `failed(...)` line the same way. Per-row iter
results complete via `ForStRsDBIterRequest` futures — wire the iter completion at the
classifier's iter-future completion site found by:
```bash
grep -n "completeIter\|iterFuture.complete\|future.complete" \
  /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsDBIterRequest.java | head
```
(one `completed(request.getRequestType())` at the SUCCESS completion, one `failed(...)` at
the exceptional completion; the request object is a field of that class).

- [ ] **Step 7: Compile + full UT suite**

```bash
cd /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs && \
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home ../../mvnw -o -q \
  -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib \
  -Denforcer.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true test
grep -lE "Failures: [1-9]|Errors: [1-9]" target/surefire-reports/*.txt | head
```
Expected: no file listed (zero failures across the suite).

- [ ] **Step 8: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/flink && \
git add flink-state-backends/flink-statebackend-forst-rs && \
git commit -m "diag(forst-rs): FRS_REENTRY_DIAG=3 offered-vs-completed accounting (Stage-0 stall localization)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

### Task 2: Per-run-isolated diagnostics harvest in the bench runner

**Files:**
- Modify: `/Users/lijunqing/Code/stczwd/ForSt/scripts/run-8c32g.sh` (the `run)` case, inside the container script right before `bash scripts/measure-sql.sh`)

- [ ] **Step 1: Clear stale TM logs before each run + harvest diag after**

In the container heredoc (after the JFR/PERF hooks, before `bash scripts/measure-sql.sh`), add:
```bash
        rm -f '$FLINK'/log/*taskexecutor*.out '$FLINK'/log/*taskexecutor*.log 2>/dev/null || true
```
And REPLACE the final line `bash scripts/measure-sql.sh` with:
```bash
        bash scripts/measure-sql.sh
        grep -h 'STREAM_STATS\|DIAG_COMPLETION' '$FLINK'/log/*taskexecutor*.out 2>/dev/null | tail -6
```
(The container script runs with the workenv mounted; the trailing grep prints the run's own
diag lines into the harness output where the RESULT line lives — one self-contained record
per run, no cross-run contamination.)

- [ ] **Step 2: Syntax check + commit**

```bash
bash -n /Users/lijunqing/Code/stczwd/ForSt/scripts/run-8c32g.sh && echo OK
cd /Users/lijunqing/Code/stczwd/ForSt && git add scripts/run-8c32g.sh && \
git commit -m "bench: per-run TM-log isolation + diag harvest in run-8c32g.sh

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

### Task 3: Reproduce with accounting and classify the stall

- [ ] **Step 1: Rebuild + deploy the jar**

```bash
cd /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs && \
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home ../../mvnw -o -q -DskipTests \
  -Denforcer.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true package && \
docker run --rm --platform linux/arm64 \
  -v $PWD/target:/src -v /Users/lijunqing/Downloads/workenv:/Users/lijunqing/Downloads/workenv \
  forst-bench:arm64 bash -c \
  'cp /src/flink-statebackend-forst-rs-2.2.0.jar /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/ && echo deployed'
```

- [ ] **Step 2: Run 4× q8 with accounting (sequential, exclusive box)**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
for i in 1 2 3 4; do
  FRS_RS_EXECUTOR=routing-async FRS_RS_READ_IO_PARALLELISM=1 FRS_REENTRY_DIAG=3 \
    scripts/run-8c32g.sh run q8 forst-rs-ffm-local 600 s0-acct-r$i
done
grep -E "RESULT:|DIAG_COMPLETION final" /tmp/s0-acct-q8-*.out 2>/dev/null || \
grep -E "RESULT:|DIAG_COMPLETION final" /private/tmp/claude-*/**/tasks/*.output | tail -12
```

- [ ] **Step 3: Classify by the decision table and record in the sweep doc**

| observation (corrupt runs) | classification | go to |
|---|---|---|
| `!LIST_ADD` marker (done+fail < off) | LOST COMPLETIONS in dispatch | Task 4 audit, focus sites A+B |
| no `!` anywhere, LIST_ADD off ≪ ~3.06M | TRIGGER STARVATION (adds never offered — AEC stopped triggering / records held) | Task 4 audit, focus sites C+D |
| no `!`, LIST_ADD off ≈ band, out_rows wrong | adds landed AFTER their window fired (ordering, not stall) | Task 4 audit, focus sites B+E |

Append the table of the 4 runs (out_rows + final DIAG_COMPLETION line each) to
`docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md` under a
`### STAGE-0 completion accounting` heading, commit with message
`docs(forst-rs): Stage-0 completion-accounting classification`.

### Task 4: Audit the classified chain (STOP-AND-REPLAN checkpoint)

Audit sites (exact, all in `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/`):
- **A** `VectorizedExecutor.java` ~524-604: APPEND_MERGE completion loop — verify every
  `amReqs[i]` is completed exactly once when `offFutures[i]==null` and heap futures
  misalign (`heapIdx >= heapFuturesSize` ⇒ `amFut==null` ⇒ `completePut` — confirm the
  heap-row count from `amBuf.futures()` matches `amCount` when ALL rows are heap-path,
  i.e., the B-spike regime).
- **B** `VectorizedClassifier.java` DISPATCH_TABLE + `requiresOrderedDispatch`: does a
  batch mixing `LIST_ADD` (APPEND_MERGE, dispatched LAST) with `CLEAR` (delete-kind,
  dispatched FIRST) for the SAME key take the ordered path? If not, an add offered BEFORE
  a clear executes AFTER it (survives the clear) and vice versa — check both directions
  against the q8 fire/clear sequence.
- **C** `exec/RoutingStateExecutor.java` `dispatchNonBlocking` + `fullyLoaded()`:
  `outstanding` leak paths (exception between increment and loop; double-settle;
  `RejectedExecutionException` path) — instrument with a temporary assert if needed.
- **D** AEC interplay (READ-ONLY — flink-runtime may not change): `AsyncExecutionController`
  trigger conditions at lines ~328 and ~458: confirm under what conditions a buffered
  non-full batch waits indefinitely when `fullyLoaded()` flaps; if the defect is here, the
  FIX must live in our `fullyLoaded()` semantics (backend side), e.g., never report loaded
  with a sub-batch-size buffer pending.
- **E** `state/ForStRsAsyncListStateV2.java` + `ForStRsMapStateV2.buildDBPutRequest`
  CLEAR hook: confirm clear-vs-add ordering guarantees at offer time.

- [ ] **Step 1:** Read sites per the Task-3 classification; write the root-cause statement
  (mechanism + file:line) into the sweep doc under `### STAGE-0 ROOT CAUSE`.
- [ ] **Step 2:** STOP. Write the fix as an addendum section `## Stage-0 fix (addendum)` in
  THIS plan file with complete code + a regression UT reproducing the mechanism, then
  continue to Task 5. (The fix code depends on the located defect; writing it before the
  audit would be fiction. The addendum must follow this plan's task format.)

### Task 5: Fix + canary gate ×5

- [ ] **Step 1:** Implement the addendum fix + its regression UT; run the full suite
  (Task 1 Step 7 command); zero failures.
- [ ] **Step 2:** Rebuild + deploy the jar (Task 3 Step 1 commands).
- [ ] **Step 3: Canary ×5** (exclusive box):
```bash
cd /Users/lijunqing/Code/stczwd/ForSt
for i in 1 2 3 4 5; do
  FRS_RS_EXECUTOR=routing-async FRS_RS_READ_IO_PARALLELISM=1 \
    scripts/run-8c32g.sh run q8 forst-rs-ffm-local 600 s0-fix-r$i
done
```
GATE: all 5 runs FINISH with out_rows in 3,064,4xx. Any miss ⇒ back to Task 4 with the
new specimen. Record all 5 in the sweep doc.
- [ ] **Step 4: Commit** (fix + UT + sweep-doc results):
```bash
cd /Users/lijunqing/Code/stczwd/flink && git add flink-state-backends/flink-statebackend-forst-rs && \
git commit -m "fix(forst-rs): Stage-0 — <root-cause one-liner from audit>

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
cd /Users/lijunqing/Code/stczwd/ForSt && git add docs && git commit -m "docs(forst-rs): Stage-0 canary x5 results"
```

---

## Stage 1 — two-regime executor (H = 1 worker)

Replaces the static B-spike env-gates with a dynamic regime: the SAME job gets inline
(cache-on) execution while the pipeline is empty and queued (cache-off) execution under
load. Key simplification at H=1: the L→H flush happens on the mailbox at the 0→1
transition — nothing is in flight, so the existing synchronous vectorized `flushTo`
drains are race-free by construction; no sealed-handoff machinery until Stage 2.

### Task 6: RegimeSwitch + executor integration

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/RegimeSwitch.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/RoutingStateExecutor.java`
- Test: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/exec/RegimeSwitchTest.java`

- [ ] **Step 1: Write the failing test**

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
package org.apache.flink.state.forstrs.exec;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Contract for {@link RegimeSwitch}: light iff outstanding == 0; L→H hook fires once per transition. */
class RegimeSwitchTest {

    @Test
    void lightIffOutstandingZero() {
        RegimeSwitch rs = new RegimeSwitch();
        assertThat(rs.isLight()).isTrue();
        rs.batchDispatched();
        assertThat(rs.isLight()).isFalse();
        rs.batchSettled();
        assertThat(rs.isLight()).isTrue();
    }

    @Test
    void transitionHookFiresExactlyOncePerLtoH() {
        RegimeSwitch rs = new RegimeSwitch();
        AtomicInteger seals = new AtomicInteger();
        rs.setOnHeavyTransition(seals::incrementAndGet);
        rs.batchDispatched(); // L→H: hook fires
        rs.batchDispatched(); // already H: no hook
        assertThat(seals.get()).isEqualTo(1);
        rs.batchSettled();
        rs.batchSettled(); // back to L
        rs.batchDispatched(); // L→H again
        assertThat(seals.get()).isEqualTo(2);
    }
}
```

- [ ] **Step 2: Run to verify it fails** (same maven single-test command pattern as Task 1
  Step 2, `-Dtest=RegimeSwitchTest`). Expected: compilation error.

- [ ] **Step 3: Implement RegimeSwitch**

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
package org.apache.flink.state.forstrs.exec;

import org.apache.flink.annotation.Internal;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Two-regime signal (design doc 2026-06-11 §3): LIGHT iff no batch is outstanding.
 * {@link #batchDispatched()} is called ONLY on the mailbox thread (AEC dispatch is
 * mailbox-confined), so the L→H transition hook runs on the mailbox while nothing is in
 * flight — the spec's race-free flush point. {@link #batchSettled()} may be called from
 * worker threads; {@link #isLight()} reads are mailbox-side and see 0 only after the
 * settle, at which point no engine work is pending (safe to re-enable staging).
 */
@Internal
public final class RegimeSwitch {

    private final AtomicInteger outstanding = new AtomicInteger();
    private volatile Runnable onHeavyTransition = () -> {};

    public void setOnHeavyTransition(Runnable hook) {
        this.onHeavyTransition = hook;
    }

    /** True ⇔ pipeline empty ⇔ inline (cache-on) execution is safe. */
    public boolean isLight() {
        return outstanding.get() == 0;
    }

    /** Mailbox-side, before enqueueing the batch. Fires the L→H hook on 0→1. */
    public void batchDispatched() {
        if (outstanding.getAndIncrement() == 0) {
            onHeavyTransition.run();
        }
    }

    /** Any-thread, after the batch fully settles. */
    public void batchSettled() {
        outstanding.decrementAndGet();
    }

    /** For fullyLoaded()-style backpressure. */
    public int outstanding() {
        return outstanding.get();
    }
}
```

- [ ] **Step 4: Run the test — PASS.**

- [ ] **Step 5: Integrate into RoutingStateExecutor (mode `two-regime`)**

In `RoutingStateExecutor.java`:
1. Add fields after `maxOutstanding`:
```java
    /** Stage-1 two-regime mode (design 2026-06-11 §3); null in legacy modes. */
    final RegimeSwitch regime;
```
2. In BOTH constructors set `this.regime = nonBlocking ? new RegimeSwitch() : null;`
   (the two-regime mode reuses the nonBlocking ctor flag; the executor-mode string decides
   which behavior the backend asks for).
3. In `executeBatchRequests`, replace the existing `if (nonBlocking) { return dispatchNonBlocking(subs, busy); }`.
   Dispatch-policy rationale (read before coding): with H=1 and synchronous inner
   executors, a pure "inline when outstanding==0" rule would make HEAVY unreachable
   (inline always completes before return, so outstanding never leaves 0). The HEAVY
   trigger is therefore batch SHAPE: iter-containing or large batches (request count >
   `FRS_RS_INLINE_MAX`, default 256 = the AEC batch size) dispatch to the worker; small
   iter-free batches run inline ONLY while the pipeline is empty (`regime.isLight()`),
   which is simultaneously the no-overtaking safety condition (design §3.1):
```java
        if (nonBlocking) {
            boolean heavy = !regime.isLight() || anySubHasIters(subs, busy) || totalRequests(subs, busy) > inlineMax;
            if (!heavy) {
                try {
                    for (int id : busy) {
                        CompletableFuture<Void> inner = workers[id].executeBatchRequests(subs[id]);
                        Throwable t = inner.handle((v, e) -> e).getNow(null);
                        if (t != null) {
                            return CompletableFuture.failedFuture(t);
                        }
                    }
                    return CompletableFuture.completedFuture(null);
                } catch (Throwable t) {
                    return CompletableFuture.failedFuture(t);
                }
            }
            regime.batchDispatched();
            CompletableFuture<Void> agg = dispatchNonBlocking(subs, busy);
            agg.whenComplete((v, e) -> regime.batchSettled());
            return agg;
        }
```
   with the helper:
```java
    private static int totalRequests(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>>[] subs, java.util.List<Integer> busy) {
        int n = 0;
        for (int id : busy) {
            if (subs[id] instanceof org.apache.flink.state.forstrs.VectorizedClassifier vc) {
                n += vc.totalRequestCount();
            }
        }
        return n;
    }

    /** Env-tunable inline ceiling (FRS_RS_INLINE_MAX, default 256). */
    private static int inlineMax() {
        String s = System.getenv("FRS_RS_INLINE_MAX");
        if (s != null) {
            try {
                int v = Integer.parseInt(s.trim());
                if (v > 0) {
                    return v;
                }
            } catch (NumberFormatException ignore) {
            }
        }
        return 256;
    }
```
   (cache `inlineMax()` in a final field `inlineMax` set in the ctor). Add to
   `VectorizedClassifier` the missing accessor if absent:
```java
    /** Total requests offered into this classifier (all kinds). */
    public int totalRequestCount() {
        return getCount() + putCount() + deleteCount() + appendMergeCount() + iterRequests.size();
    }
```
   (verify each sub-count accessor exists with
   `grep -n "int getCount()\|int putCount()\|int deleteCount()\|int appendMergeCount()\|int iterCount()" VectorizedClassifier.java`;
   use the actual accessor names found).
4. `fullyLoaded()` becomes `return regime != null ? regime.outstanding() >= maxOutstanding : outstanding.get() >= maxOutstanding;`
   and `dispatchNonBlocking` stops touching the old `outstanding` field when `regime != null`
   (the agg-future `whenComplete` above settles it instead). Keep the legacy field for the
   plain routing-async mode.

- [ ] **Step 6: Backend gate** — in `ForStRsAsyncKeyedStateBackend.java` add a case
  `"two-regime"` next to `"routing-async"`, constructing
  `new RoutingStateExecutor(linker, db, defaultCf, dispatchMetrics, managedExecutors::add, false, true)`
  (same ctor; the mode string is what states consult — see Task 7).

- [ ] **Step 7: Full UT suite + commit** (commands as Task 1 Steps 7-8, message
  `feat(forst-rs): Stage-1 RegimeSwitch + two-regime dispatch (H=1)`).

### Task 7: Regime-aware staging (replaces the static B-spike gates)

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsAsyncReducingStateV2.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsAsyncAggregatingStateV2.java`
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAsyncKeyedStateBackend.java`

Design: states consult a SHARED RegimeSwitch (not the env) per operation. The backend owns
the executor's RegimeSwitch and exposes it via a static per-backend registry the states
already use for linker/db/cf (they receive those at construction — extend the same wiring).

- [ ] **Step 1:** Thread the switch: `ForStRsAsyncKeyedStateBackend` keeps
  `@Nullable private RegimeSwitch regimeSwitch;` set when constructing the `"two-regime"`
  executor (expose `RegimeSwitch regimeSwitch()` on `RoutingStateExecutor` returning the
  field). Pass it to states the same way `linker/db/cf` flow today: add an overloaded
  setter `void setRegimeSwitch(RegimeSwitch rs)` on `ForStRsMapStateV2`,
  `ForStRsAsyncReducingStateV2`, `ForStRsAsyncAggregatingStateV2` and call it right after
  construction in the backend's state-create switch (the backend already calls
  `registeredMapStatesV2.add(mapStateV2)` etc. — add the setter call beside each).
- [ ] **Step 2:** In `ForStRsMapStateV2`: keep the existing `pipelinedExecutorActive()`
  static for the legacy routing-async mode, and add the instance check:
```java
    @Nullable private RegimeSwitch regimeSwitch;

    public void setRegimeSwitch(RegimeSwitch rs) {
        this.regimeSwitch = rs;
    }

    /** Staging usable ⇔ legacy pipelined mode off AND (no regime switch OR regime is light). */
    private boolean stagingUsable() {
        if (offHeapBuf == null) {
            return false;
        }
        return regimeSwitch == null || regimeSwitch.isLight();
    }
```
  Replace every `if (offHeapBuf != null)` hot-path guard in asyncGet/asyncPut/asyncRemove/
  asyncContains with `if (stagingUsable())` (the ctor under `"two-regime"` constructs the
  buffer — revert the B-spike ctor gate to
  `!ForStRsMapStateV2.pipelinedExecutorActive()` only, since two-regime wants the buffer).
  The CLEAR hook and snapshot flush keep their `offHeapBuf != null` checks (they must drain
  regardless of regime).
- [ ] **Step 3:** L→H flush hook: in the backend, after creating the two-regime executor:
```java
        ra.regimeSwitch().setOnHeavyTransition(this::flushAllStagingForRegimeTransition);
```
  with (in the backend):
```java
    /**
     * Stage-1 L→H seal (design §3.2, H=1 simplification): runs on the MAILBOX with
     * outstanding == 0 — nothing in flight, so the synchronous vectorized drains are
     * race-free. Off-heap, batched FFI (spec §8 constraints).
     */
    private void flushAllStagingForRegimeTransition() {
        for (ForStRsMapStateV2<?, ?, ?, ?> s : registeredMapStatesV2) {
            s.flushOffHeapBuffer();
        }
        for (ForStRsAsyncListStateV2<?, ?, ?> s : registeredListStatesV2) {
            s.flushPreSnapshot();
        }
        for (ForStRsAsyncReducingStateV2<?, ?, ?> s : registeredAsyncReducingStates) {
            s.flushOnBarrier();
        }
        for (ForStRsAsyncAggregatingStateV2<?, ?, ?, ?, ?> s : registeredAsyncAggregatingStates) {
            s.flushOnBarrier();
        }
    }
```
  (verify the exact registry list names with
  `grep -n "registered.*V2" ForStRsAsyncKeyedStateBackend.java` and the exact flush method
  names with `grep -n "flushOnBarrier\|flushPreSnapshot\|flushOffHeapBuffer" state/*.java`;
  use what exists — all four flush methods are present per the Stage-0 exploration).
- [ ] **Step 4:** Same `setRegimeSwitch` + `stagingUsable()` pattern in
  `ForStRsAsyncReducingStateV2.asyncAdd` and `ForStRsAsyncAggregatingStateV2.asyncAdd`
  (replace the B-spike `if (ForStRsMapStateV2.pipelinedExecutorActive()) return super.asyncAdd(value);`
  with `if (!rmwCacheUsable()) return super.asyncAdd(value);` where `rmwCacheUsable()` is
  the analogous instance check), and gate the ListStateArrowBuffer use in the classifier
  hand-off the same way (`ForStRsAsyncListStateV2.recordAppendMergeOffHeap` first line:
  `if (buffer == null || (regimeSwitch != null && !regimeSwitch.isLight())) { return null; }`).
- [ ] **Step 5:** UTs: extend `RoutingStateExecutorAsyncTest` with a two-regime test class
  `TwoRegimeDispatchTest` (same stub-worker scaffolding) asserting: (1) small iter-free
  batch with idle pipeline executes on the CALLER thread (inline) and returns a DONE
  future; (2) batch with iters dispatches to the worker thread and returns an INCOMPLETE
  future; (3) the L→H hook runs exactly once before the first heavy dispatch (inject via
  `executor.regimeSwitch().setOnHeavyTransition(...)`). Complete test code mirrors Task 6
  Step 1 patterns plus `RoutingStateExecutorAsyncTest`'s `containerWithKeyGroups` helper —
  copy that helper verbatim into the new class.
- [ ] **Step 6:** Full suite + commit:
  `feat(forst-rs): Stage-1 regime-aware staging + L→H flush (two-regime mode)`.

### Task 8: Stage-1 gates (correctness before any perf reading)

- [ ] **Step 1:** Jar rebuild + deploy (Task 3 Step 1).
- [ ] **Step 2:** q8@100M ×5 under `FRS_RS_EXECUTOR=two-regime FRS_RS_READ_IO_PARALLELISM=1`
  — ALL exact (3,064,4xx) or back to the audit.
- [ ] **Step 3:** 10M exactness sweep, all 22 queries, two-regime vs RocksDB reference
  (the existing measure-sql harness; EVENTS_NUM=10000000): must match the 20/22 baseline
  (q4 wedge + q5 churn are the known pre-existing exceptions).
- [ ] **Step 4:** q17@100M ×3 two-regime (LIGHT-regime preservation): direction-consistent
  with same-day blocking-mode pair (box noise rule — n≥3, direction only).
- [ ] **Step 5:** q9@100M two-regime + drain200K back-to-back vs same-day control.
- [ ] **Step 6:** Record everything in the sweep doc; commit; push both repos; verify GHA
  (`gh run list` in each repo until conclusion=success).

### Stage-2/3/4 handoff
Stage 2 (multi-worker HEAVY: per-kg sealed-shard handoff replacing the H=1 mailbox flush),
Stage 3 (engine multi-kind write-batch FFI + OPT-N04 numeric merge operator in
~/Code/stczwd/ForSt), and Stage 4 (default flip + full 3-backend matrix + spec §8 full
A/B) get their own plans after Stage-1 gates pass, carrying forward the same constraints.
