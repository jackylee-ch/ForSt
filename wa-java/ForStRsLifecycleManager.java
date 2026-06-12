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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.util.HashMap;
import java.util.Map;

// =====================================================================================
// FRS-WA-V0 KNOWLEDGE-TRANSFER COPY (lives in the ForSt repo under wa-java/; the Flink
// repo is read-only from this worktree). Destination when adopted:
//   flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/
//       state/forstrs/ForStRsLifecycleManager.java
// Companion: ForStRsLinker.lifecycle-fragment.java (the three FFM bindings).
// Precedent for this transfer style: ForSt C++ ships utilities/flink/
// flink_compaction_filter.h — Flink state-lifecycle knowledge living in the storage
// repo. This class is the lifecycle-descriptor analogue of
// ForStRsTtlCompactFiltersManager (same package, same registration shape).
// =====================================================================================

/**
 * FRS-WA-V0 (2026-06-13 write-path redesign survey §3.2): per-CF state-LIFECYCLE manager — the
 * backend→engine contract that tells the engine when state dies, so the engine's (flag-gated,
 * default-OFF) V1 write path can drop whole death-bucketed segments at the watermark instead of
 * compacting soon-dead bytes (measured: compaction = ~87% of physical write bytes; TTL-segment
 * FIFO-drop floor = write-amp 0.98).
 *
 * <p>Three engine calls (all inert in V0 — the engine stores and logs them):
 *
 * <ul>
 *   <li>{@code frs_cf_set_lifecycle(db, cf, kind, ttl)} — once per CF at state-register time.
 *       kind: 0=Unbounded (default), 1=Windowed{ttl}, 2=Timer. Clock units are OURS (ms).
 *   <li>{@code frs_cf_note_max_event_time(db, cf, eventTimeMs)} — monotonic upper bound on the
 *       event-time of every entry written; MUST be advanced before (or atomically with) each
 *       write carrying that event-time. The engine samples it after sealing a memtable to derive
 *       a sound (never premature) segment death stamp {@code maxEventTime + ttl}.
 *   <li>{@code frs_cf_advance_watermark(db, cf, watermarkMs)} — from the operator watermark path,
 *       AFTER subtracting allowed-lateness slack (late events inside Flink's allowed-lateness
 *       window keep state alive because we only advance the engine watermark past lateness).
 * </ul>
 *
 * <p>WIRING POINTS in this backend (V0 wiring, all additive):
 *
 * <ul>
 *   <li><b>CF create / state register</b> — wherever {@link ForStRsTtlCompactFiltersManager
 *       #setTtlForState} is driven today (TTL-configured states), additionally call {@link
 *       #setLifecycleForState} with {@code KIND_WINDOWED} and the same ttlMs. For window-operator
 *       state (q5/q8/q11 class) the windowed-state CF registers with ttl = window size +
 *       allowed lateness; for interval joins (q7 class) ttl = upper bound − lower bound of the
 *       join interval. Timer CFs ({@code timer/ForStRsKeyGroupedInternalPriorityQueue} backing
 *       store) register {@code KIND_TIMER}.
 *   <li><b>Event-time bound</b> — the keyed backend already observes the record timestamp on the
 *       write path; {@link #noteMaxEventTime} is an atomic-max cheap enough to call per write
 *       batch (NOT per record — batch max suffices and keeps FFI crossings at the existing batch
 *       cadence).
 *   <li><b>Watermark</b> — {@code InternalTimeServiceManager}/operator {@code processWatermark}
 *       reaches the backend; forward {@code watermark - allowedLateness} via {@link
 *       #advanceWatermark} at watermark cadence (not per record).
 * </ul>
 */
@Internal
public final class ForStRsLifecycleManager {

    /** Engine lifecycle-kind ordinals (must match CfLifecycle::ordinal in the engine). */
    public static final int KIND_UNBOUNDED = 0;

    public static final int KIND_WINDOWED = 1;
    public static final int KIND_TIMER = 2;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final Map<String, LifecycleRegistration> registrations = new HashMap<>();

    public ForStRsLifecycleManager(ForStRsLinker linker, FrsDb db) {
        this.linker = linker;
        this.db = db;
    }

    /**
     * Declares the lifecycle of the CF backing {@code stateName}. Idempotent; the engine treats a
     * re-declaration as a replace (post-create swappable, mirroring the compaction-filter model).
     */
    public void setLifecycleForState(String stateName, FrsCfHandle cf, int kind, long ttlMs) {
        linker.cfSetLifecycle(db, cf, kind, ttlMs);
        registrations.put(stateName, new LifecycleRegistration(cf, kind, ttlMs));
    }

    /**
     * Maps a Flink {@link StateTtlConfig} onto the lifecycle surface: enabled TTL ⇒ Windowed with
     * the configured TTL in ms; disabled ⇒ Unbounded. Visibility/update semantics stay enforced at
     * the Flink runtime layer (read filtering) — segment drop only reclaims state the runtime
     * already promises never to read (same division of labor as the TTL compaction filter).
     */
    public void setLifecycleFromTtlConfig(String stateName, FrsCfHandle cf, StateTtlConfig ttl) {
        if (ttl != null && ttl.isEnabled()) {
            setLifecycleForState(stateName, cf, KIND_WINDOWED, ttl.getTimeToLive().toMillis());
        } else {
            setLifecycleForState(stateName, cf, KIND_UNBOUNDED, 0L);
        }
    }

    /**
     * Raises the written-event-time upper bound for {@code cf}. Call with the max event-time of a
     * just-flushed write batch (monotonic-max on the engine side; stale values are no-ops).
     */
    public void noteMaxEventTime(FrsCfHandle cf, long eventTimeMs) {
        linker.cfNoteMaxEventTime(db, cf, eventTimeMs);
    }

    /**
     * Advances the engine watermark for every registered lifecycle CF. {@code watermarkMs} must
     * already have allowed-lateness slack subtracted by the caller.
     */
    public void advanceWatermark(long watermarkMs) {
        for (LifecycleRegistration r : registrations.values()) {
            if (r.kind != KIND_UNBOUNDED) {
                linker.cfAdvanceWatermark(db, r.cf, watermarkMs);
            }
        }
    }

    /** Per-state registration record. */
    private static final class LifecycleRegistration {
        final FrsCfHandle cf;
        final int kind;
        final long ttlMs;

        LifecycleRegistration(FrsCfHandle cf, int kind, long ttlMs) {
            this.cf = cf;
            this.kind = kind;
            this.ttlMs = ttlMs;
        }
    }
}
