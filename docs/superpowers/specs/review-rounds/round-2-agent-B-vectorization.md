# Round 2 — Agent B Review: End-to-End Vectorization & Batch Execution

**Reviewer angle:** criterion #2 — eliminate per-record sync work, per-row FFM crossings, per-event allocations, virtual-call hotpoints, and branch-heavy if-chains. Drive columnar batches with SIMD-friendly hot loops and JDK 25 Vector API where applicable.

**Round 2 mandate:** look for NEW vectorization issues that Round 1 missed, with explicit attention to:
1. Hidden vectorization cost of the C-H4 `combine_slices` fix.
2. New per-row work introduced by the Round 1 `executeBatchRequests` propagation loop (lines 188-218).
3. Branch-prediction pressure in the new per-row error-check loop.
4. JDK 25 `ByteVector` opportunities on `MemorySegment` byte-equality/hash regions not flagged in Round 1.
5. Branchless / perfect-hash dispatch opportunities revealed by Round 1 docs.

Findings labeled `B2-Hn` (Round 2 HIGH) / `B2-Mn` (MED) / `B2-Ln` (LOW). Already documented in Round 1 are NOT re-reported; only NEW sub-issues are listed.

**Note on scope:** `VectorizedExecutor.java`, `VectorizedClassifier.java`, the `ColumnarBatchBuffer` family, and `ForStRsLinker.java` live in a sibling repo (`flink-statebackend-forst-rs`) that is not checked out in this worktree. Findings B2-H1..B2-H4 are derived from the Round 1 reports (round-1-agent-B-vectorization.md, round-1-agent-A-correctness.md, round-1-agent-D-jdk25.md) which contain verbatim code excerpts; the cross-references are Round-1 line numbers reproduced in those reports. The Rust-side observations (B2-H5 below, plus MED findings) were verified directly against `crates/forst-rs-engine/src/list_merge.rs` and `crates/forst-rs-ffi/src/lib.rs`.

---

## C-H4 verification: borrowed-slice path

**Verified directly** in `crates/forst-rs-engine/src/list_merge.rs:50-67` (`combine_slices`, `combine_with_base_slices`) and `crates/forst-rs-ffi/src/lib.rs:4022-4055`.

The borrowed-slice variants are strictly cheaper than the cloned-`Vec<Vec<u8>>` path that they replaced:
- Same single-pass `Vec::with_capacity(total) + extend_from_slice` inside the combiner.
- The grouping `HashMap<&[u8], Vec<&[u8]>>` keeps `&[u8]` slices into the caller's `keys_data`/`ops_data` instead of cloning to `Vec<u8>`. Net win: one heap alloc + one memcpy eliminated per operand per call.
- LLVM autovectorizes `extend_from_slice` into `memcpy` either way; SIMD width is unchanged. The fix recovers the FFI's design intent without introducing any new vectorization cost.

**Verdict: CORRECT — fix is at-or-better than the pre-fix path on all axes.** No new sub-issue. (One follow-up — `HashMap` ordering non-determinism — is owned by Agent A as A2-M1.)

---

## HIGH findings (NEW for Round 2)

### B2-H1 — Per-row `amReqFutures.get(i).isCompletedExceptionally()` loop pollutes BTB on the success hot path

**File:** `flink-statebackend-forst-rs/.../VectorizedExecutor.java:188-218` (Round 1 fix site for A1-H5)

Reproduced from Round-1 docs (Agent A round-2 cross-ref at A2-M2 quotes the same lines):

```java
for (int i = 0; i < amCount; i++) {
    CompletableFuture<Void> amFut = amReqFutures.get(i);   // virtual lookup on ArrayList
    if (amFut.isCompletedExceptionally()) {                 // ← per-row branch on hot path
        Throwable cause;
        try {
            amFut.getNow(null);
            cause = new RuntimeException("...cause unavailable");
        } catch (Throwable t) {
            cause = t.getCause() != null ? t.getCause() : t;
        }
        completePutExceptionally(amReqs[i], cause);
    } else {
        completePut(amReqs[i]);
    }
}
```

**Failure mode (perf, not correctness):** the success path (the overwhelmingly common case — 99.99%+ of dispatches succeed in steady state) STILL pays:

1. **`ArrayList.get(i)` per row** — non-eliminable virtual call returning an `Object` requiring an unchecked cast (`AppendMergeBatchBuffer.appendMergeRequestFutures()` returns `List<CompletableFuture<Void>>` per Round 1 doc B-H1, M3). For amCount=1024 that is 1024 virtual dispatches plus 1024 array-bounds checks. Compare to the parallel `StateRequest[]` putRequests array which is a direct `aaload`.
2. **`isCompletedExceptionally()` is a volatile read on `CompletableFuture.result`** — every iteration issues a `getfield` on a non-final volatile + acquire fence. Each iteration drains the store buffer. On a successful batch this fence is 100% wasted work.
3. **Branch-prediction footprint:** the JIT will profile the branch as "almost-never-taken" within a slot's lifetime (success rate ~1.0) and predict not-taken correctly — but the BTB entry for this branch competes with the BTB entries of every OTHER branch executed in `executeBatchRequests` (the GET/PUT/DELETE/ITER dispatch branches, the `executePuts`/`executeDeletes` propagation, the `metrics.recordDispatch` per-state-class branches). For a 27-state-type classifier (B-H7 in Round 1) plus 4 dispatch kinds, the BTB pressure inside one batch turn is already non-trivial. Adding a per-row branch with a near-saturated predicate to the inner loop pushes other branches out of BTB. This is a measurable JIT-microarchitecture cost in steady-state.

**Why this is new:** Round 1 critiqued B-H7 (27-case classifier switch — a forward-direction dispatch) and Round 1 Agent A flagged A1-H5 (correctness — must propagate). The COMPOSITION — Round-1-A's fix adds a per-row predicate-branch in series with the classifier — creates a NEW BTB pressure source that neither original finding observed.

**Fix shape (perf-side; correctness fix is Agent A's A2-H2/H3):**
1. **Track failures via a sideband `Throwable[] amErrors` array set by the dispatcher, not by polling `CompletableFuture` state.** Reset to all-null at batch start. Dispatcher writes per-row on failure (today it writes to all of them at once with the same instance per A2-L1; the array form costs an extra `n * 8` bytes but removes the volatile read). Then the propagation loop becomes:
   ```java
   for (int i = 0; i < amCount; i++) {
       Throwable t = amErrors[i];
       if (t == null) completePut(amReqs[i]);              // hot path, no volatile read
       else completePutExceptionally(amReqs[i], t);
   }
   ```
   The branch is now on a thread-local `Object[]` read; the JIT can profile-fold it to the always-null path under PGO once the success rate is observable. Same applies to Agent A's A2-M2 (per-row exception-throw).
2. **Two-phase dispatch** — peek the bulk-error flag set by `dispatchAppendMergeBatch` (single `boolean batchFailed`); if false, run the bulk `completePut` loop with NO branch. If true, fall into a slow path that walks `amErrors` for per-row attribution.
3. **Inline the loop into a sibling method per AppendMergeBuffer** to avoid the megamorphic `appendMergeRequestFutures()` call site (today, every dispatch path that owns an AppendMergeBuffer crosses the same accessor — three callers Round 1 named).

---

### B2-H2 — `frs_vec_iter_prefix_open` registers iterator under a global `Mutex<HashMap>` — serializes ALL iterator opens across slots

**File:** `crates/forst-rs-ffi/src/lib.rs:3492-3497, 3601-3606` (verified directly)

```rust
static ITER_HANDLES: OnceLock<Mutex<HashMap<u64, AnyNativeIter>>> = OnceLock::new();
static NEXT_ITER_ID: AtomicU64 = AtomicU64::new(1);

fn iter_handles() -> &'static Mutex<HashMap<u64, AnyNativeIter>> {
    ITER_HANDLES.get_or_init(|| Mutex::new(HashMap::new()))
}

// in frs_vec_iter_prefix_open:
let handle_id = NEXT_ITER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
iter_handles()
    .lock()
    .unwrap()                       // ← global mutex acquisition per open
    .insert(handle_id, native_iter);
```

**Failure mode:** every `frs_vec_iter_prefix_open` (and `_next` at line 3645, `_close`, `_range_open` at line 3794) acquires a single process-wide `std::sync::Mutex`. For a TaskManager running multiple slots concurrently (the default Flink deployment shape: 4-16 slots/TM), every slot's iterator-heavy query (Q11 session window, Q12 with timer-driven scans, Q15/16/19 with prefix iteration) contends on this one mutex. The lock is held across `HashMap::insert` (rehash possible) and across the entire `next_chunk` call (line 3650). This is a CLASSIC per-slot scaling killer that vectorization is supposed to avoid.

This is NEW relative to Round-1's B-H3 (per-row FFM dispatch in Java) and B-H4 (full materialization in Rust): Round 1 implicitly assumed the per-iterator cost was the Java-side Arena + FFI crossing. The hidden contention point is one layer deeper, in the Rust-side registry.

**Why this is HIGH:** affects every iter-heavy query at slot-count > 1 (every realistic deployment). With 8 slots issuing parallel `iter_open/next/close` cycles, expected serialization fraction is ≥ 50% of dispatch time on Q15/Q16/Q19. The pthread mutex under contention costs ~100ns even uncontended (one `lock cmpxchg` + memory fence); under contention it parks. For per-record iter access this is a per-record fence.

**Fix shape:**
1. **Shard the registry by `handle_id & (NSHARDS-1)`** — N=64 striped `Mutex<HashMap>`s. Eliminates cross-slot contention because `NEXT_ITER_ID` is unique per open.
2. **Better: thread-local iter storage with a transferable handle.** Since iter handles in Flink are slot-pinned (the mailbox executor owns them), the registry doesn't need cross-thread visibility — it just needs to be reachable on close. A `thread_local!` `RefCell<HashMap<...>>` removes the lock entirely. Confirm against `FrsIterHandle.close()` callers: Round-1 D4 fix sketch notes the close path is slot-thread driven. If true, thread-local works.
3. **Best (longer term):** return a `Box::leak`'d raw pointer as the handle and skip the registry entirely (caller owns the lifetime, close = `Box::from_raw + drop`). Same shape as RocksDB's iterator FFI. Removes both the lock AND the HashMap lookup on every `_next`.

Also affects `frs_vec_iter_prefix_next` (line 3645) and `_close` (line 3669+) which take the same mutex once per chunk. For chunked iteration that's 1 mutex/chunk × N chunks/iter × M iters/batch = lock contention scales with iterator chunk count.

---

### B2-H3 — `combine_slices` allocates a fresh `Vec<u8>` per (key, ops-group) — N independent allocs per batch, defeating the batched-merge vectorization premise

**File:** `crates/forst-rs-engine/src/list_merge.rs:50-67`, used from `crates/forst-rs-ffi/src/lib.rs:4042-4055` (verified directly)

```rust
// list_merge.rs:50
pub fn combine_slices(&self, operands: &[&[u8]]) -> Vec<u8> {
    let total: usize = operands.iter().map(|o| o.len()).sum();
    let mut out = Vec::with_capacity(total);    // ← alloc per call
    for op in operands {
        out.extend_from_slice(op);
    }
    out
}

// lib.rs:4042
for (key, ops) in grouped.iter() {
    let existing: Vec<u8> = match db_ref.get(cf_ref, key) { ... };
    let merged = if existing.is_empty() {
        combiner.combine_slices(ops.as_slice())     // ← allocates Vec per key
    } else {
        combiner.combine_with_base_slices(&existing, ops.as_slice())
    };
    if let Err(e) = db_ref.put(cf_ref, key, &merged) { ... }
}
```

**Failure mode:** the C-H4 fix correctly eliminated the per-OPERAND `Vec<u8>` clone. But it left a per-KEY `Vec<u8>` allocation: one `Vec::with_capacity` + the implicit `Vec::drop` after `db_ref.put` returns. For a typical Q19 batch of 1024 distinct keys (each with a single APPEND_MERGE operand), this is 1024 heap allocations + 1024 frees PER BATCH on the hot path. The combined cost (per umbrella audit's engine-per-op budget of ~250 ns) is comparable to the per-key engine `get`+`put` work itself.

This is NEW relative to Round-1 C-H4 which framed the fix as "borrowed slices solve the cloning problem". The cloning problem at the OPERAND level is solved; the allocation problem at the per-key MERGED-VALUE level is not.

**Compounding factor:** when `existing.is_empty()` (the most common case for "first APPEND_MERGE for a new key" workloads, e.g. session-window assigners) AND the group has exactly one operand, `combine_slices(&[op])` is doing a memcpy from a borrowed slice into a fresh `Vec<u8>` of identical size — i.e., a useless full copy of the operand bytes that already live in `ops_data`. The `db.put(cf, key, &merged)` then issues a SECOND memcpy into the engine's WriteBatch. Two memcpys for what should be a pass-through.

**Why HIGH:** Q19/Q11/Q12 listState APPEND_MERGE pressure passes through this path. The "batched" FFI's whole purpose was to amortize allocs; one per-key alloc preserved at the engine seam undermines the win.

**Fix shape:**
1. **Pass through directly for the single-operand + no-existing case:** add a fast-path `if existing.is_empty() && ops.len() == 1 { db_ref.put(cf, key, ops[0]) }` before invoking the combiner. Eliminates the alloc entirely for the dominant pattern. `db.put` takes `&[u8]`, so this is a one-line addition.
2. **Reusable scratch buffer:** thread-local `RefCell<Vec<u8>>` reused across the loop. Call `scratch.clear(); scratch.reserve(total); scratch.extend_from_slice(...)`. One alloc per batch instead of N. Requires care if `db.put` retains the slice (it copies into WriteBatch internally — confirm via `db.rs`).
3. **WriteBatch-level merge:** if the engine exposes a `MergeOperator` (it does — `list_merge.rs` is the combiner shape that maps onto RocksDB's MergeOperator interface), the FFI should call `db.merge(cf, key, operand_blob)` for each (key, concatenated-operands) pair and let the engine's MergeOperator do the read-modify-write at flush/read time, removing the per-batch read-modify-write loop AND its alloc. This is the V4-final shape the audit envisions.

---

### B2-H4 — `write_chunk_into_buf` packs rows into the caller buffer scalar byte-at-a-time per `klen`/`vlen` u32 LE encode

**File:** `crates/forst-rs-ffi/src/lib.rs:3508-3534` (verified)

The chunked iterator's writer encodes each `[klen u32 LE][vlen u32 LE][key bytes][value bytes]` row. Each 4-byte length field write is a separate `ptr::copy_nonoverlapping` of 4 bytes (or scalar write_u32_le). For chunks of ~1024 rows that's 2048 short writes plus 1024 bulk row writes interleaved — terrible auto-vectorization shape, because the small writes prevent LLVM from coalescing the bulk writes.

**Why HIGH:** every `frs_vec_iter_prefix_next` call runs this loop (line 3651). For Q15/16/19 prefix-iter queries chunking ~64KB at a time, this is the steady-state per-batch packer.

**Fix shape:** SoA layout — write all `[klen0..klenN]` packed into the buffer header, then `[vlen0..vlenN]`, then concatenated bodies. The Java side already knows how to parse a SoA Arrow BinaryArray-style buffer (the audit-design's `key_offsets, key_data, val_offsets, val_data` shape). Use that shape, write the offset arrays via single bulk memcpy (after computing prefix sums vectorized via `slice::iter().scan`), then bulk-memcpy all keys then all values. Net effect: 2 memcpys for offset arrays + 2 memcpys for body data per chunk, regardless of row count.

JDK 25 leverage on the Java side: with SoA layout, the Java reader can use `ByteVector.fromMemorySegment` on the offset arrays to compute spans in parallel.

---

### B2-H5 — `iter_handles().lock().unwrap()` on every `_next` chunk pull is a per-chunk fence even uncontended

**File:** `crates/forst-rs-ffi/src/lib.rs:3645-3656`

```rust
let mut guard = iter_handles().lock().unwrap();
let iter = match guard.get_mut(&handle) {
    Some(it) => it,
    None => return FrsErrorCode::IterCursorInvalid as i32,
};
let chunk = iter.next_chunk(chunk_buf_cap as usize);
```

Even with the B2-H2 contention fix, the `Mutex::lock` itself on a single-threaded slot path is a fenced atomic on every `_next`. For an iterator yielding 100 chunks, that's 100 unnecessary `lock cmpxchg` + barrier pairs. The mutex is then held for the entire `next_chunk` call (which inside does `Vec<u8>` allocs per row — Round-1 H4) — so it transitively widens the critical section.

**Why HIGH separate from B2-H2:** B2-H2 fixes cross-slot contention; this remaining issue is uncontended-but-still-fenced cost on the single-slot path. Independent.

**Fix shape:** the thread-local registry from B2-H2 fix#2 also fixes this. Or the raw-pointer handle from B2-H2 fix#3.

---

## MED findings (NEW for Round 2)

### B2-M1 — `frs_vec_merge_append_batch` `HashMap<&[u8], Vec<&[u8]>>` defeats prefetch on grouping loop

**File:** `crates/forst-rs-ffi/src/lib.rs:4022-4035`

Already flagged as L4 in Round 1 (non-determinism) and A2-M1 in Round-2-A (HashMap ordering). The NEW perf observation: the grouping loop allocates a fresh `Vec<&[u8]>` per distinct key (via `entry(key).or_insert_with(Vec::new).push(op)`), which is a small alloc per distinct key. For batches where each row has a distinct key (the common case post-shuffle), this is N small `Vec<&[u8]>` allocations whose only contents is one `&[u8]`.

**Fix:** when the batch is mostly-distinct-keys, skip the grouping entirely: walk `i in 0..n`, do `get`/`combine`/`put` row-at-a-time using stack `&[&[u8]]` of length 1. Detection heuristic: count distinct keys in a first pass (cheap, branchless); if equal to n, skip the HashMap. Or, more simply, fold the HashMap into a `Vec<(usize, usize)>` (key-range, op-range) sorted by key — then groups are runs of equal adjacent ranges, no per-group `Vec` alloc.

### B2-M2 — Round-1 docs reveal a clean perfect-hash dispatch opportunity in `VectorizedClassifier.offer` that nobody named

**File:** `flink-statebackend-forst-rs/.../VectorizedClassifier.java:304-358` (Round 1 B-H7)

Round 1 B-H7 critiqued the 27-case switch on `StateRequestType` and proposed "precompute a per-state-name routing token at registration time". The Round 1 doc didn't observe that **`StateRequestType` is already an enum with `ordinal()` returning a dense `[0..26]` int** — a perfect hash by construction. The 27-case switch should compile to a `tableswitch` bytecode (dense range) NOT a `lookupswitch` (sparse). If the JIT is currently emitting `lookupswitch` (due to the fall-through grouping at source level), refactoring to one-case-per-ordinal with a small per-ordinal handler table flips it to `tableswitch` (single bounds check + jump-table).

**Better:** replace the switch with a `Handler[] HANDLERS = new Handler[StateRequestType.values().length]` populated at class init, and call `HANDLERS[type.ordinal()].offer(this, request)`. Single indirect call, zero branches, predictor-free. JIT inlines monomorphic handlers when the type is stable per batch.

**Why MED not HIGH:** Round-1 B-H7 already names the issue at HIGH; this is just the cleaner mechanism. Promoting to HIGH would double-count.

### B2-M3 — `executeBatchRequests` propagation loop's `for (int i = 0; i < amCount; i++)` does not unroll (amCount is JIT-opaque)

**File:** Round-1 B-H1 line range / Round-2-A A2-M2 quote

The loop bound `amCount` is read from `amBuf.size()` at the top of the dispatch site (per Round 1 doc M3). The JIT can profile-fold this to a typical batch size but doesn't unroll because the body is non-trivial (volatile read + branch). With B2-H1 fix (sideband Throwable[]) and the body reduced to "load int + branch + call", the JIT can unroll 4x or 8x — but only if `amCount` is recognized as fixed-batch. A hint via `Math.min(amCount, MAX_BATCH)` (where MAX_BATCH is a final int) lets the JIT specialize. Not a HIGH because amCount is not the bottleneck once B2-H1 is fixed; flagged for completeness.

### B2-M4 — `combine_with_base_slices` allocates `Vec<u8>` sized to base + ops, but commits a redundant base-copy for any "rewrite after read" cycle

**File:** `crates/forst-rs-engine/src/list_merge.rs:59-67`

```rust
pub fn combine_with_base_slices(&self, base: &[u8], operands: &[&[u8]]) -> Vec<u8> {
    let total = base.len() + operands.iter().map(|o| o.len()).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(base);                   // ← always copies base
    for op in operands { out.extend_from_slice(op); }
    out
}
```

`base` is `existing: Vec<u8>` returned from `db_ref.get` (lib.rs:4043). Already owned and contiguous. The combiner allocates a NEW Vec and memcpy's `base` into it, then appends operands. Could just `existing.extend_from_slice(op_for_op_in_ops)` in place — saves one alloc + one memcpy per (key with existing-value) per batch. The current signature takes `&[u8]` for genericity, but the caller has a `Vec<u8>` it owns. Either:
1. Add `combine_with_owned_base(base: Vec<u8>, operands: &[&[u8]]) -> Vec<u8>` and let the caller hand off ownership.
2. At the call site (lib.rs:4050) skip the combiner entirely for the in-place case: `for op in ops.iter() { existing.extend_from_slice(op); }` then `db.put(cf, key, &existing)`.

Pairs with B2-H3 fix #2/#3.

### B2-M5 — `NEXT_ITER_ID.fetch_add(Ordering::Relaxed)` on a global atomic is per-open contention point

**File:** `crates/forst-rs-ffi/src/lib.rs:3493, 3602, 3794`

A single `AtomicU64` counter incremented by every slot on every iter open. Under multi-slot load, the cache line containing `NEXT_ITER_ID` ping-pongs between cores. Independent of B2-H2's HashMap lock contention because the atomic itself uses a different MESI line. Mitigation: per-thread counter with thread-id high bits packed into handle, or the raw-pointer handle from B2-H2 fix #3 (no global counter needed).

### B2-M6 — `MemorySegment.mismatch` opportunities beyond Round-1 D5 / B-H9

Round 1 named `ArrowBinaryBuffer.hash/keysEqual` and `ArrowTimerBuffer.hashOf/rowKeyEquals`. NEW sites the round-1 docs imply but don't name explicitly:

1. **`ForStRsKeyGroupedInternalPriorityQueue.keyPrefixMatches` (Round 1 H9 mentioned, but no separate fix shape)** — same byte-loop pattern, on the timer hot path for Q12.
2. **`MapStateCache.BytesKey.equals(byte[] a, byte[] b)`** — currently `Arrays.equals(byte[], byte[])` (scalar in HotSpot ≤ 23; SIMD-intrinsified in JDK 24+ via `vectorizedArraysEquals` stub but on `byte[]`, not MemorySegment). Replacing the cache key with a `MemorySegment` handle and using `mismatch` would unify with the Arrow buffer path.
3. **The new `executeRequestSync` / `executeBatchRequests` propagation loops** themselves don't compare bytes, but the GET-result deserialization in `executeGets` (Round 1 H6) copies bytes — `MemorySegment.copy` is already a memcpy intrinsic, so no SIMD win there. Flagged as non-issue for completeness.
4. **Engine-side `forst-rs-engine/src/db.rs` `batch_get` (line 2928) / `prefix_scan` (2352)** — `prefix_scan` iterates and clones; no byte-equality there. `batch_get` is unverified; if it walks keys for memtable lookup, Rust's `slice::eq` is already SIMD via `memcmp` intrinsic. No new finding.

### B2-M7 — `JAVA_INT` offset-array reads in `ColumnarBatchBuffer` could batch via `ByteVector`/`IntVector`

The Round-1 H9 / D5 focus on byte-level mismatch; equivalent for the **offset arrays** (which are int[] decoded from MemorySegment) is `IntVector.fromMemorySegment(IntVector.SPECIES_256, seg, off, ByteOrder.LITTLE_ENDIAN)`. For executor decode loops that walk offset arrays to compute spans (in `executeGets`, `executeIters`, `dispatchAppendMergeBatch` consumption), batching 8 int reads at once via IntVector + a `sub` lane op to get spans is a clean win. Not flagged in Round 1's D5 because D5 focused on byte equality; this is the int-array shape.

### B2-M8 — `write_chunk_into_buf`'s row-by-row encoding is a SoA opportunity match for Arrow IPC

**File:** `crates/forst-rs-ffi/src/lib.rs:3508-3534`

Tied to B2-H4 fix shape. Beyond the perf win, switching the chunk wire format to SoA enables direct zero-copy interop with Arrow-Java's `BinaryArray` (the audit's V11 endgame). The Java side already has Arrow-shaped consumers in V1-sync (per Round-1's cross-cutting observation #3); the iter path is the missing piece. Filed at MED because the fix is a wire-format change (coordinated across the FFI seam).

---

## LOW findings (NEW for Round 2)

### B2-L1 — `combine_slices` doesn't reuse the borrowed-slice fast-path comment from C-H4 for `combine_with_base_slices`

**File:** `crates/forst-rs-engine/src/list_merge.rs:47-50` vs `:59`

The doc comment names C-H4 only on `combine_slices`. The borrowed-slice rationale applies equally to `combine_with_base_slices`. Small doc completeness issue.

### B2-L2 — `iter_handles()` `get_or_init` uses `OnceLock` which itself is a CAS on first call

**File:** `crates/forst-rs-ffi/src/lib.rs:3495-3497`

Not a hot-path issue (first-call only). Flagged for completeness — once warmed, `get_or_init` is a relaxed load. No action.

### B2-L3 — `Vec<&[u8]>` inside `HashMap` re-grows per push; small-vec specialization would help

**File:** `crates/forst-rs-ffi/src/lib.rs:4034`

`grouped.entry(key).or_insert_with(Vec::new).push(op)` — `Vec::new()` starts at capacity 0; first push allocates capacity 4, second push grows to 8 if needed. For per-key operand counts of 1-3 (the common case post-shuffle), this is wasted reallocation. Use a `SmallVec<[&[u8]; 4]>` (smallvec crate) or a `Vec::with_capacity(2)` heuristic.

### B2-L4 — `ListMergeCombiner` is a unit struct constructed per FFI call

**File:** `crates/forst-rs-ffi/src/lib.rs:4041`

`let combiner = ListMergeCombiner::new();` constructs a zero-sized type per call. Zero-cost in optimized builds, but stylistically the combiner methods could just be associated `fn`s on the type with no `&self`. Cleanup nit.

### B2-L5 — `dispatchAppendMergeBatch` `recordDispatch` over-counts on failure (Agent A's A2-L2)

Listed only to acknowledge the cross-reviewer overlap; this is Agent A's territory but the perf-counter implication (the `DispatchMetrics` histogram now includes failed work in its bytesIn series) skews vectorization-perf analysis if used as a tuning signal.

---

## Cross-cutting observations for Round 2

1. **The C-H4 borrowed-slice fix is correct as a fix to the named issue, but doesn't deliver the full vectorization win because the read-modify-write loop in `frs_vec_merge_append_batch` (Rust side) still allocates a fresh merged-value `Vec<u8>` per key (B2-H3).** Moving to a true MergeOperator path or a single-operand-passthrough fast path closes the gap.

2. **Round-1 A1-H5's propagation-loop fix introduced a small but real BTB-pressure increase on the success hot path (B2-H1).** The sideband `Throwable[]` design pattern (state set by dispatcher, polled by propagator) eliminates the volatile read entirely. Same pattern resolves Agent A's A2-M2.

3. **Two NEW Rust-side contention/registry issues surfaced that Round 1 didn't reach (B2-H2 mutex contention, B2-H5 per-chunk fence).** Both have the same root cause — global `Mutex<HashMap>` for iter handles — and the same fix (thread-local registry or raw-pointer handle). Affects every iter-heavy query at slot count > 1, which is every realistic Flink deployment.

4. **The chunk wire format `write_chunk_into_buf` is scalar-shaped (B2-H4).** Switching to SoA per chunk simultaneously enables JDK 25 `IntVector`/`ByteVector` reads on the Java side AND lays groundwork for Arrow-IPC interop the audit's V11 calls for.

5. **JDK 25 leverage opportunities not flagged in Round 1's D-section:**
   - `IntVector.fromMemorySegment` over offset arrays (B2-M7).
   - SIMD-eligible per-row scanning of the chunk wire format once SoA (B2-H4 secondary win).
   - Branchless `Handler[ordinal()]` dispatch in classifier as a JDK 21+ idiom (B2-M2). The `tableswitch` vs perfect-array distinction matters because the JIT can profile-inline a typed handler array but cannot inline a 27-case switch beyond the first 2-3 hot cases.

---

## Summary

- **HIGH (new):** 5 (B2-H1 through B2-H5)
- **MED (new):** 8 (B2-M1 through B2-M8)
- **LOW (new):** 5 (B2-L1 through B2-L5)

**C-H4 verification:** CORRECT — borrowed-slice path is at-or-better than the cloned path on all axes. No new sub-issue from the fix itself; B2-H3 is a pre-existing alloc that the fix didn't address, not a regression introduced by it.

**New issue HIGH count: 5.**

**One-line gist per HIGH:**
- **B2-H1:** Round-1 A1-H5 fix added per-row `isCompletedExceptionally()` volatile read + branch in propagation loop, pollutes BTB on hot success path; sideband `Throwable[]` design removes it.
- **B2-H2:** `frs_vec_iter_prefix_open` registers iterators under a global `Mutex<HashMap>` — serializes all iter opens across slots; thread-local registry or raw-pointer handle eliminates it.
- **B2-H3:** `combine_slices` allocates per-key merged `Vec<u8>` — N allocs per batch, fixable via single-operand passthrough or MergeOperator path.
- **B2-H4:** `write_chunk_into_buf` packs rows scalar-byte-at-a-time per length prefix, defeats SIMD on the iter chunk path; SoA wire format enables `IntVector`/`ByteVector`.
- **B2-H5:** `iter_handles().lock()` on every `_next` chunk is a per-chunk atomic fence even uncontended; same fix as B2-H2.

**Top-3 leverage estimates (informal):**
1. **B2-H2 + B2-H5** (registry mutex/atomic) — affects every multi-slot iter-heavy deployment (i.e. nearly every prod Flink TM); single Rust-side fix.
2. **B2-H3** (per-key alloc in merge batch) — affects every APPEND_MERGE batch; Q19 / session-window-heavy workloads.
3. **B2-H1** (propagation loop BTB pressure) — affects every APPEND_MERGE dispatch; small per-record cost but on the hottest loop.

Tie-breaker B2-H4 (chunk wire format) is high-leverage but coordinated across the FFI seam.
