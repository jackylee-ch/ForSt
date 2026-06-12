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

// =====================================================================================
// FRS-WA-V0 KNOWLEDGE-TRANSFER FRAGMENT (lives in the ForSt repo under wa-java/; the
// Flink repo is read-only from this worktree). NOT a standalone compilation unit —
// three MERGE-INTO blocks below splice into:
//   flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/
//       state/forstrs/ffm/ForStRsLinker.java
// Engine exports (crates/forst-rs-ffi/src/lib.rs, section "2b. FRS-WA-V0"):
//   frs_cf_set_lifecycle(db, cf, kind: i32, ttl: u64) -> i32
//   frs_cf_advance_watermark(db, cf, watermark: u64) -> i32
//   frs_cf_note_max_event_time(db, cf, event_time: u64) -> i32
// All three return FRS_STATUS_* codes; NULL handles → NULL_ARG; unknown lifecycle
// ordinal → INVALID_ARGUMENT. ABI: no struct layout change (FRS_ABI_VERSION stays 1;
// additive symbols only).
// =====================================================================================

// --- MERGE-INTO #1: field declarations (next to `frsCfSetCompactionFilterTtl`) -------

//    private final MethodHandle frsCfSetLifecycle;
//    private final MethodHandle frsCfAdvanceWatermark;
//    private final MethodHandle frsCfNoteMaxEventTime;

// --- MERGE-INTO #2: constructor bind block (after the "7. TTL compaction filter"
//     bind of `frs_cf_set_compaction_filter_ttl`) --------------------------------------

//        // 7b. FRS-WA-V0 lifecycle plumbing — per-CF state-lifecycle descriptor +
//        // watermark/event-time clocks (inert engine-side until the V1 death-bucketed
//        // segment stage; see 2026-06-13-write-path-redesign-survey.md §3.2/§6).
//        this.frsCfSetLifecycle =
//                bind(
//                        "frs_cf_set_lifecycle",
//                        FunctionDescriptor.of(
//                                ValueLayout.JAVA_INT,
//                                ValueLayout.ADDRESS, // db
//                                ValueLayout.ADDRESS, // cf
//                                ValueLayout.JAVA_INT, // kind (0=Unbounded,1=Windowed,2=Timer)
//                                ValueLayout.JAVA_LONG)); // ttl (u64, Windowed only; ms)
//        this.frsCfAdvanceWatermark =
//                bind(
//                        "frs_cf_advance_watermark",
//                        FunctionDescriptor.of(
//                                ValueLayout.JAVA_INT,
//                                ValueLayout.ADDRESS, // db
//                                ValueLayout.ADDRESS, // cf
//                                ValueLayout.JAVA_LONG)); // watermark (u64, ms, monotonic)
//        this.frsCfNoteMaxEventTime =
//                bind(
//                        "frs_cf_note_max_event_time",
//                        FunctionDescriptor.of(
//                                ValueLayout.JAVA_INT,
//                                ValueLayout.ADDRESS, // db
//                                ValueLayout.ADDRESS, // cf
//                                ValueLayout.JAVA_LONG)); // event_time (u64, ms, monotonic)

// --- MERGE-INTO #3: public wrappers (next to `setCompactionFilterTtl`) ---------------

//    /**
//     * FRS-WA-V0: declares the CF's state lifecycle. kind: {@link
//     * ForStRsLifecycleManager#KIND_UNBOUNDED} / {@code KIND_WINDOWED} / {@code KIND_TIMER};
//     * {@code ttlMs} is read only for KIND_WINDOWED. Inert in V0 (engine stores + logs).
//     */
//    public void cfSetLifecycle(FrsDb db, FrsCfHandle cf, int kind, long ttlMs) {
//        int rc;
//        try {
//            rc = (int) frsCfSetLifecycle.invokeExact(db.handle(), cf.handle(), kind, ttlMs);
//        } catch (Throwable t) {
//            throw new FrsBackendException(
//                    FrsStatus.PANIC, "frs_cf_set_lifecycle threw: " + t.getMessage());
//        }
//        check(rc, "frs_cf_set_lifecycle");
//    }
//
//    /**
//     * FRS-WA-V0: advances the CF watermark clock (monotonic; stale values are no-ops).
//     * Callers subtract allowed-lateness slack BEFORE forwarding (late events inside the
//     * allowed-lateness window must keep state alive).
//     */
//    public void cfAdvanceWatermark(FrsDb db, FrsCfHandle cf, long watermarkMs) {
//        int rc;
//        try {
//            rc = (int) frsCfAdvanceWatermark.invokeExact(db.handle(), cf.handle(), watermarkMs);
//        } catch (Throwable t) {
//            throw new FrsBackendException(
//                    FrsStatus.PANIC, "frs_cf_advance_watermark threw: " + t.getMessage());
//        }
//        check(rc, "frs_cf_advance_watermark");
//    }
//
//    /**
//     * FRS-WA-V0: raises the CF's written-event-time upper bound (monotonic). MUST be kept
//     * ≥ the event-time of every entry written to the CF — advance with each write batch's
//     * max event-time, before (or atomically with) the batch publish.
//     */
//    public void cfNoteMaxEventTime(FrsDb db, FrsCfHandle cf, long eventTimeMs) {
//        int rc;
//        try {
//            rc = (int) frsCfNoteMaxEventTime.invokeExact(db.handle(), cf.handle(), eventTimeMs);
//        } catch (Throwable t) {
//            throw new FrsBackendException(
//                    FrsStatus.PANIC, "frs_cf_note_max_event_time threw: " + t.getMessage());
//        }
//        check(rc, "frs_cf_note_max_event_time");
//    }
