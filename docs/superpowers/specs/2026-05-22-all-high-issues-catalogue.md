# ForSt-RS — Cumulative HIGH-Issue Catalogue

**Date:** 2026-05-22
**Scope:** All HIGH / CRIT findings from the 5-agent multi-round review (Rounds 1, 2, and Round 3 — appended when complete).
**Branches at time of catalogue:** flink `forst-rs-jdk25` HEAD `63ea1c2904e`, ForSt `forst-rs` HEAD `c0df7eb71`.
**Status legend:** ✅ FIXED · 🔧 partial · ⛔ open · ❓ verification needed · 📐 architectural-debt

---

## Tally summary

| Source round | HIGH found | HIGH fixed | HIGH open |
|---|---:|---:|---:|
| Round 1 | 37 | 3 | 34 |
| Round 2 | 18 (incl 2 CRIT) | 3 | 15 |
| Round 3 | — (pending) | — | — |
| **Cumulative (incl R3 pending)** | **55+** | **6** | **49+** |

The companion spec `2026-05-22-high-issue-remediation-spec.md` proposes the staged plan to close all 49+ open items.

---

## Section 1 — Data Consistency & Correctness (CRITICAL tier)

### S1-1 ⛔ snapshot() returns SnapshotResult.empty() — no durability for Flink-managed handles
- **File:** `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAsyncKeyedStateBackend.java:387`
- **Sources:** A1-H1, E-CRIT-1
- **Root cause:** V1 deliberately ships placeholder snapshot. Engine writes SSTs to S3 on memtable flush so state IS durable, but Flink's checkpoint coordinator gets no KeyedStateHandle → no Flink-managed restore.
- **Blast radius:** Production-blocker for rescaling jobs; OK for benches (job runs to completion).
- **Status:** 📐 architectural-debt — documented as V1.1 work.

### S1-2 ⛔ VectorizedExecutor.flushDirty() is empty stub
- **File:** `flink-state-backends/.../VectorizedExecutor.java:238`
- **Sources:** A1-H2, A1-H4 (related)
- **Root cause:** Snapshot path calls flushDirty 3× expecting it to drain in-flight batches, but the method is empty. In-flight classifier buffers leak past the checkpoint barrier.
- **Blast radius:** Together with S1-1, this means any in-flight async batch on a checkpoint can be lost.
- **Status:** 📐 architectural-debt.

### S1-3 ⛔ Timer flushPendingToEngine() never called from snapshot path
- **File:** `flink-state-backends/.../timer/ForStRsKeyGroupedInternalPriorityQueue.java` (documented hook); `ForStRsAsyncKeyedStateBackend.snapshot()` never invokes it.
- **Source:** A1-H3
- **Root cause:** Documented as mandatory pre-snapshot hook (the batched-timer-design spec §5 critical invariants). The async backend never calls it.
- **Blast radius:** Pending timer ADD/REMOVE ops dropped on snapshot → timers fire incorrectly after restore.
- **Status:** ⛔ open — surgical fix possible once S1-1 lands.

### S1-4 ⛔ V2 keyed-state engine keys missing namespace
- **File:** state V2 classes — verification pending; locations identified by reviewer at `ForStRsValueStateV2.serializeKey`, `ForStRsMapStateV2.serializeKey`, etc.
- **Source:** E2-CRIT-1
- **Root cause:** V2 key format is `KEY_PREFIX + key + SLASH + stateName + SLASH` — namespace NOT encoded. Two windows over the same key with different namespaces collide.
- **Blast radius:** **Silent cross-window state corruption** on any non-VoidNamespace operator (most windowed aggregations).
- **Status:** ❓ verification needed — could be encoded somewhere else; if confirmed, surgical fix.

### S1-5 ⛔ FORSTRS timer kgSupplier returns startKeyGroup constant
- **File:** `flink-state-backends/.../keyed/ForStRsAsyncKeyedStateBackend.java:436` — `() -> keyGroupRange.getStartKeyGroup()`
- **Source:** E2-CRIT-2
- **Root cause:** Supplier returns a constant (the FIRST keygroup of the operator's range). peek/poll only sees timers from that keygroup; all others' timers are unreachable.
- **Blast radius:** Operators with parallelism > 1 or multi-keygroup ranges have undelivered timers → silent late-event corruption.
- **Status:** ⛔ open — needs verification of intended supplier semantics, then surgical fix.

### S1-6 ⛔ V1-sync offheapKeyGroupSupplier = () -> 0
- **File:** `flink-state-backends/.../keyed/ForStRsKeyedStateBackend.java:232`
- **Source:** E-CRIT-3
- **Root cause:** Every V1-sync state op uses key-group bucket 0 regardless of the actual key's hash.
- **Blast radius:** Rescaling broken for V1-sync deployments (Q5/Q11/Q13/etc. on community-Forst-API-compatible workloads). For single-parallelism this is invisible.
- **Status:** ⛔ open — surgical fix straightforward.

### S1-7 ⛔ ArrowTimerBuffer.drainTo() iterates heap-array index order
- **File:** `flink-state-backends/.../timer/ArrowTimerBuffer.java:drainTo`
- **Source:** E-CRIT-2
- **Root cause:** Public visitor iterates `heap[]` in array index order, not by polling the min-heap. Any caller relying on this for ordered drain (savepoint/migration) gets out-of-order timers.
- **Blast radius:** No current caller relies on order (verified by Round 1), but any future savepoint/migration path is a footgun.
- **Status:** ⛔ open — easy: rename to `drainUnordered` AND add an ordered `drainTo` that pops via removeMin.

### S1-8 ⛔ Silent data loss on LIST_ADD failure — FIXED ✅
- **File:** `VectorizedExecutor.executeBatchRequests` lines 188-218 (and the sync sibling `executeRequestSync`)
- **Sources:** A1-H5 (Round 1), A2-H1/H2/H3 (Round 2 follow-up)
- **Status:** ✅ FIXED in commits `66f153868fc` (R1) + `63ea1c2904e` (R2) on `forst-rs-jdk25`. Per-row futures correctly propagated; container future fails when any row fails; sync sibling path also patched.

### S1-9 ⛔ batched FFI exception leaves StateRequest futures dangling
- **File:** Locations like `executePuts`, `executeDeletes`, `executeGets` where `vectorizedBatchPut/Delete` throws.
- **Source:** A1-H4
- **Root cause:** Exception path returns `failedFuture(e)` for the container, but per-row `StateRequest.getFuture()`s are never completed → operator hangs forever waiting on those futures.
- **Blast radius:** Any transient engine error stalls the operator indefinitely.
- **Status:** ⛔ open — needs the same per-row propagation pattern S1-8 added for APPEND_MERGE applied to PUT/DELETE/GET paths.

### S1-10 ⛔ ValueStateV2.serializeKey caches composite key in RecordContext.extra without stateName
- **File:** `state/ForStRsValueStateV2.java` (serializeKey)
- **Source:** A1-H6
- **Root cause:** Multiple ValueStates in the same operator share the `RecordContext.extra` slot → wrong-state-key reads when more than one ValueState exists.
- **Blast radius:** Silent state-key cross-contamination across distinct ValueStates within the same operator.
- **Status:** ⛔ open — surgical fix: key the cache by `(operatorKeyContext, stateName)` tuple OR use distinct slot offsets per state.

### S1-11 ⛔ MapStateCache survives asyncClear()
- **File:** `cache/MapStateCache.java` — no hook in the final asyncClear() method
- **Source:** E2-HIGH-2
- **Root cause:** Cache entries from a window survive its asyncClear() because no `clear` callback is invoked on the cache; stale entries returned for the cleared window.
- **Blast radius:** Compounds with S1-4 (namespace missing): even if namespace fix lands, cache stale-read still occurs.
- **Status:** ⛔ open — wire the clear hook.

### S1-12 ⛔ State TTL silently disabled
- **File:** `keyed/ForStRsAsyncKeyedStateBackend.java` (`getOrCreateKeyedState` never reads `desc.getTtlConfig()`)
- **Source:** E2-HIGH-1
- **Root cause:** TTL configuration ignored. Users specifying `StateTtlConfig` see no actual expiry.
- **Blast radius:** Unbounded state growth on long-running TTL-configured jobs.
- **Status:** ⛔ open.

---

## Section 2 — Vectorization (HIGH tier, perf)

### V2-1 ⛔ dispatchAppendMergePerRow per-row Arena.ofConfined()
- **File:** `VectorizedExecutor.java:391-411`
- **Sources:** B-H1, D-H3
- **Status:** ⛔ — but not on hot path (V4 batched fast-path active). Could be deleted entirely if no caller needs multi-operand-per-row.

### V2-2 ⛔ frs_vectorized_batch_get per-key db.get() loop
- **File:** `crates/forst-rs-ffi/src/lib.rs:~2415-2436`
- **Source:** B-H2 (= V10 in audit-design spec)
- **Status:** ⛔ open — V10 in audit spec Phase E.

### V2-3 ⛔ dispatchIterPrefix/Range per-request Arena.ofShared
- **Files:** `VectorizedExecutor.java:673, 761`
- **Sources:** B-H3, D-H4, D-R2-1
- **Status:** ⛔ open — should use confined arena or slot-turn allocation.

### V2-4 ⛔ frs_vec_iter_prefix_open materializes full prefix into Vec<(Vec<u8>, Vec<u8>)>
- **Source:** B-H4
- **Root cause:** Undoes the "chunked iterator" abstraction; breaks S3 streaming.
- **Status:** ⛔ open.

### V2-5 ⛔ recordAppendMerge 4-buffer copy chain
- **File:** `VectorizedClassifier.java` recordAppendMerge
- **Source:** B-H5
- **Status:** ⛔ open — partially addressed by C-H4 fix on Rust side; Java side still has the chain.

### V2-6 ⛔ executeGets `byte[] = new byte[len]` per row
- **Files:** `VectorizedExecutor.java:321-322`, plus `Linker.getIntoBuf`/`getFast`
- **Sources:** B-H6, C-H1, C-H6 (V5 in audit)
- **Status:** ⛔ open — audit-design Phase B target.

### V2-7 ⛔ VectorizedClassifier.offer 27-case switch + Set.contains per record
- **Source:** B-H7
- **Status:** ⛔ open — branch-prediction unfriendly; perfect-hash dispatch alternative.

### V2-8 ⛔ MapStateCache `new BytesKey(byte[])` per call
- **File:** `cache/MapStateCache.java:66-79`
- **Sources:** B-H8 (V1 in audit), V7 in audit
- **Status:** ⛔ open — audit-design Phase D target.

### V2-9 ⛔ ArrowBinaryBuffer/TimerBuffer scalar hash/keysEqual loops
- **Sources:** B-H9, D-H5
- **Root cause:** Scalar byte-by-byte loops; `MemorySegment.mismatch()` and `jdk.incubator.vector.ByteVector` unused anywhere.
- **Status:** ⛔ open — drop-in `MemorySegment.mismatch` for keysEqual; SIMD ByteVector for hash.

### V2-10 ⛔ Linker.{get,put,delete}Segment allocate byte[] internally
- **File:** `ffm/ForStRsLinker.java`
- **Source:** B-H10
- **Root cause:** "Segment" overloads marshal to byte[]-FFI internally; zero-copy in name only.
- **Status:** ⛔ open.

### V2-11 ⛔ Batched FFIs NOT bound with critical(allowHeapAccess=true)
- **Source:** D-H2
- **Status:** ⛔ open — Round 2 D-R2 noted safety check needed.

### V2-12 ⛔ Iter handle map global Mutex<HashMap>
- **Files:** `crates/forst-rs-ffi/src/lib.rs` iter handle registry
- **Sources:** B2-H2, B2-H5
- **Status:** ⛔ open — serializes all iter opens cross-slot.

### V2-13 ⛔ B2-H1 — per-row volatile read in A1-H5 fix
- **File:** `VectorizedExecutor.executeBatchRequests` (post-Round 1 fix)
- **Source:** B2-H1
- **Root cause:** Round-1 fix added `isCompletedExceptionally()` volatile read per row on happy path.
- **Status:** ⛔ open — sideband `Throwable[]` from dispatcher would be branchless.

### V2-14 ⛔ B2-H3 — combine_slices still allocates per-key merged Vec
- **Source:** B2-H3
- **Status:** ⛔ open — single-operand passthrough OR rocksdb merge-operator path closes.

### V2-15 ⛔ B2-H4 — write_chunk_into_buf scalar length-prefix interleaved
- **Source:** B2-H4
- **Status:** ⛔ open — SoA wire format unlocks IntVector/ByteVector reads.

---

## Section 3 — Zero-copy (HIGH tier)

### Z3-1 ⛔ executeGets `byte[]` per GET row — covered by V2-6 above.

### Z3-2 ⛔ OpendalRandomAccessFile.read_at `buffer.to_vec()` + memcpy
- **File:** `crates/forst-rs-storage/src/opendal_random_access_file.rs:read_at`
- **Source:** C-H2
- **Status:** ⛔ open — Bytes::slice would be true zero-copy.

### Z3-3 ⛔ OpendalSequentialFile holds full S3 object in Vec<u8>
- **Source:** C-H2 (related)
- **Status:** ⛔ open — streaming reader pattern.

### Z3-4 ⛔ OpendalWritableFile::persist `self.buffer.clone()`
- **File:** `crates/forst-rs-storage/src/opendal_writable_file.rs:persist`
- **Source:** C-H3
- **Status:** ⛔ open — Bytes::clone is ref-count-only OR pass Buffer directly to opendal sink.

### Z3-5 ✅ frs_vec_merge_append_batch clones operands to Vec<Vec<u8>> — FIXED
- **Files:** `crates/forst-rs-ffi/src/lib.rs:4037-4053`, `crates/forst-rs-engine/src/list_merge.rs`
- **Source:** C-H4
- **Status:** ✅ FIXED in commit `0dfabaccd` (Rust side). Verified clean by Round 2 Agent C.

### Z3-6 ⛔ ForStRsMapStateV2.deserializeUser{Key,Value}(IteratorEntryView) allocates per iter entry
- **File:** `state/ForStRsMapStateV2.java:346-380`
- **Source:** C-H5 (V8 in audit)
- **Status:** ⛔ open — `MemorySegmentDataInputView` already exists, just needs threading through.

### Z3-7 ⛔ Linker.getIntoBuf/getFast byte[] per V1-sync GET — covered by V2-6.

### Z3-8 ⛔ AppendMergeBatchBuffer.append 4-copy chain
- **File:** `AppendMergeBatchBuffer.append`
- **Source:** C-H7
- **Status:** ⛔ open.

### Z3-9 ⛔ cached_fs::fetch_through_cache Vec without with_capacity
- **File:** `crates/forst-rs-storage/src/cached_fs.rs:182-198`
- **Source:** C-R2-H1
- **Status:** ⛔ open — single-line fix `Vec::with_capacity(file_size)`.

### Z3-10 ⛔ SST reader to_vec() per row defeats Arrow zero-copy
- **File:** `crates/forst-rs-engine/src/sst/reader.rs:319, 396, 404`
- **Source:** C-R2-H2
- **Status:** ⛔ open — borrowed `RowView<'a>` return shape.

### Z3-11 ⛔ Memtable scan/get clones key+value per row
- **File:** `crates/forst-rs-engine/src/memtable/vectorized.rs:642-643, 1293`
- **Source:** C-R2-H3
- **Status:** ⛔ open — symmetric to Z3-10.

---

## Section 4 — JDK 25 leverage (HIGH tier)

### J4-1 ✅ Template default ZGC+COH — FIXED
- **File:** `flink-2.2.1/conf/templates/config-forst-rs.yaml.tpl`
- **Source:** D-H1
- **Status:** ✅ FIXED in commit `0dfabaccd` (verified clean by Round 2).

### J4-2 ⛔ Batched FFIs not critical(allowHeapAccess=true) — covered V2-11.

### J4-3 ⛔ dispatchAppendMerge* per-row Arena.ofConfined — covered V2-1/V2-3.

### J4-4 ⛔ dispatchIterPrefix/Range Arena.ofShared per request — covered V2-3.

### J4-5 ⛔ Scalar hash/keysEqual loops — covered V2-9.

### J4-6 ⛔ Manual pointer arithmetic on FrsBytes / Arrow structs (6+ sites)
- **Files:** `Linker.java:1614, 1686, 2716, 1871, 1875, 1878, 1882`
- **Source:** D-R2-2
- **Status:** ⛔ open — `ValueLayout.ADDRESS.withTargetLayout(FRS_BYTES_LAYOUT)` replaces magic-number offsets.

---

## Section 5 — Flink streaming (HIGH tier)

### F5-1 ⛔ savepoint() throws UnsupportedOperationException
- **Source:** E-HIGH-1
- **Status:** ⛔ open — V1.1 work.

### F5-2 ⛔ ForStRsRestoreOperation.copyKeyGroup per-record FFM (rescale)
- **Source:** E-HIGH-2
- **Status:** ⛔ open — batched-put or SST ingest needed for rescale-friendly performance.

### F5-3 ⛔ executeBatchRequests returns already-completed future — no in-flight parallelism
- **Source:** E-HIGH-3
- **Status:** ⛔ open — async-state V2 framework expects pipelined dispatch.

### F5-4 ⛔ ForStRsStateExecutor.executeIters per-request frs_vec_iter_prefix_open
- **Source:** E-HIGH-5
- **Status:** ⛔ open — needs batched-prefix-open FFI primitive.

### F5-5 ❌ Timer-factory default FORSTRS — FALSE POSITIVE
- **Source:** E-HIGH-6
- **Status:** False positive. FORSTRS IS the batched off-heap variant per V4 session (commit `a5fd9f70dd6`).

### F5-6 ⛔ restoreWithRescaling serial dbOpen + per-handle synchronous SST downloads
- **Source:** E2-HIGH-3
- **Status:** ⛔ open — parallel SST prefetch needed.

---

## Section 6 — Round 1 follow-ups & MEDIUM tier (selected)

### M6-1 ⛔ ForStRsKeyedStateBackend.java:228 anonymous Arena.ofShared() never closed
- **File:** `keyed/ForStRsKeyedStateBackend.java:228-229`
- **Source:** D-R2-5
- **Severity:** MEDIUM
- **Status:** ⛔ open — pair Arena+segment in ThreadLocal; close on backend dispose.

### M6-2 ⛔ Multiple HashSet/registry race conditions
- **Source:** Multiple agents (A1-MED, E-related)
- **Severity:** MEDIUM
- **Status:** ⛔ open.

### M6-3 ⛔ 30+ `getCopyOfBuffer()` sites in serializer paths
- **Source:** Agent C Round 1 MEDIUMs
- **Status:** ⛔ open — bulk migration possible once Z3-6 / V2-6 land.

---

## Reports — full per-agent text under `docs/superpowers/specs/review-rounds/`

- Round 1: `round-1-agent-{A,B,C,D,E}-*.md` + `round-1-summary.md`
- Round 2: `round-2-agent-{A,B,C,D,E}-*.md` + `round-2-summary.md`
- Round 3: pending (5 agents in flight at catalogue time)

## Cumulative open count by category

| Category | Open | Fixed | Total |
|---|---:|---:|---:|
| Section 1 (Correctness/Durability) | 11 | 1 | 12 |
| Section 2 (Vectorization) | 15 | 0 | 15 |
| Section 3 (Zero-copy) | 9 | 1 | 10 |
| Section 4 (JDK 25) | 5 | 1 | 6 |
| Section 5 (Flink streaming) | 5 | 0 (+1 FP) | 6 |
| Section 6 (MEDIUMs, selected) | 3 | 0 | 3+ |
| **Total** | **48** | **3** | **52+** |

(Round 3 findings will append further if dispatched-agents return non-empty reports.)
