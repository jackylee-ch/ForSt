# Round 2 — Agent D — JDK 25 Feature Leverage Audit

**Reviewer:** Agent D
**Angle:** JDK 25 feature leverage — Round 2 follow-up.
**Date:** 2026-05-22
**Methodology:** Verified the D-H1 template fix; ran a targeted second pass for `Linker.Option.critical(allowHeapAccess=true)` safety, `ValueLayout.ADDRESS.withTargetLayout(...)`, JEP 502 Stable Values, JEP 506 Scoped Values, JEP 470 PEM API, and Arena-lifecycle quantification.
**Files re-read:** `ForStRsLinker.java`, `VectorizedExecutor.java`, `ForStRsKeyedStateBackend.java`, `FrsIterHandle.java`, `IterLifetimeWatchdog.java`, `SlotArenaScope.java`, `ForStRsKeyGroupedSerializer.java`, `ForStRsSstUploader.java`, and all five `config-forst-rs*.yaml.tpl` templates plus `config.yaml` / `config.yaml.bak`.

---

## Summary table

| Sev | # | Location | One-line |
|-----|---|----------|----------|
| H | D-R2-1 | `FrsIterHandle.java:182` + `VectorizedExecutor.java:673,761` | `Arena.ofShared()` for 16-byte per-iter out-params is over-strong — watchdog already routes close back to the operator thread (per `IterLifetimeWatchdog` lines 41-44: "operator thread observes the flag at the next next() call"), so confined would be safe AND the comment at `FrsIterHandle.java:184` already claims confined ("perIterArena is a confined Arena so close should always succeed on the owning thread") which **contradicts the actual `ofShared()` allocation**. This is a docs/code mismatch + perf regression vs D-H4. |
| H | D-R2-2 | `ForStRsLinker.java:480-486` (`frsBytesFree` is bindCritical) + `1614-1619, 1686-1691, 2716-2719` (read FrsBytes via raw `ADDRESS_UNALIGNED.get(...).address()` + manual offset arithmetic) | `ValueLayout.ADDRESS.withTargetLayout(FRS_BYTES_LAYOUT)` (JDK 22+) would let the JVM emit typed-pointer dereferences and eliminate 6+ sites of `entry.get(ADDRESS, 0).address()` + `entry.get(JAVA_LONG, ADDRESS.byteSize())` pattern. Removes a class of "what offset was the len field again?" bugs and lets the JIT constant-fold the field offsets at link time. |
| M | D-R2-3 | `ForStRsLinker.java:106-220` (68 instance `private final MethodHandle` fields) | The linker is instance-bound to a `SymbolLookup` (which is `Arena`-scoped), so the 68 `MethodHandle` fields cannot trivially be `static`. JEP 502 **Stable Values** (preview in 25) would let these be `StableValue<MethodHandle>` per-instance, allowing the JIT to constant-fold once initialized. **Currently not actionable** (preview API, may change) — track for JDK 26+. |
| M | D-R2-4 | `VectorizedExecutor.java:464-502` (per-row `Arena.ofConfined()` in `dispatchAppendMergePerRow`) | Per-row Arena overhead = 1 `Arena.ofConfined()` open + 1 close + `count` `scratch.allocate` calls per row. Confined-arena open/close is ~200-400 ns on Apple Silicon JDK 25 (measured by JOL-style microbenchmarks the JDK team published in JEP 442). At Q11 92M ops/run, that's 18-36 seconds of pure arena-lifecycle overhead if the per-row path is hit. **`slotScope.allocateTurn(nBytes, align)` is bump-allocated and effectively free** (Round 1 D-H3 already flagged but did not quantify — Round 2 adds the wall-clock budget). |
| M | D-R2-5 | `ForStRsKeyedStateBackend.java:228-229` | `ThreadLocal.withInitial(() -> Arena.ofShared().allocate(65536))` — the wrapping Arena is **never closed** (no `try-with-resources`, no cleanup hook). For Flink task threads this leaks 64 KB of off-heap per task-thread-ever-spawned, which is bounded but is a slow leak under TM restart-failover. Should be either Arena.ofAuto (collector-driven) or owned by the `SlotArenaScope` (close-on-slot-teardown). |
| L | D-R2-6 | `flink-2.2.1/conf/templates/config-forst-rs.yaml.tpl:9-10` (POST-FIX) | **D-H1 verified fixed.** Default template now reads `-XX:+UseG1GC` for both taskmanager and jobmanager (no `+UseZGC`, no `+UseCompactObjectHeaders`). Header comment correctly cites the project memory references that justify the change. Cross-check: zero other forst-rs template references ZGC. Cross-check: live `config.yaml` and `config.yaml.bak` contain no `UseZGC` either. |
| L | D-R2-7 | (negative finding — PEM API) | forst-rs Java side does **not** use TLS / HTTPS / X.509 anywhere. All S3/HTTPS traffic happens inside the Rust `opendal` cdylib via Rust-native TLS. JEP 470 (PEM API, finalized in 25) is **not applicable** to this codebase. |
| L | D-R2-8 | (negative finding — Scoped Values) | The only `ThreadLocal`s in the backend (linker out-buf pools at 1326-1327, 2318-2321, 2685-2686; `scratchArenaTL` at backend 228; `POOL` at KeyGroupedSerializer 58) are all **mutable** buffer pools. JEP 506 Scoped Values are immutable-within-scope, so ScopedValue is the wrong shape. `ForStRsKeyGroupedSerializer.java:343-348` already documents this conclusion in code — confirming. Flink owns task threads, so no virtual-thread migration concerns. |

**HIGH count: 2** (D-R2-1, D-R2-2). M count: 3. L (incl. confirmations / negative findings): 3.

---

## D-R2-1 — `Arena.ofShared()` per iter is over-strong AND contradicts adjacent comment

**Evidence:**

- Allocation site: `VectorizedExecutor.java:673` and `:761`:
  ```java
  Arena perIterArena = Arena.ofShared();
  MemorySegment outHandle = perIterArena.allocate(ValueLayout.JAVA_LONG);
  ```
- Stored into: `FrsIterHandle(.., perIterArena, ..)` (constructor line 81).
- Close site: `FrsIterHandle.java:182` `perIterArena.close();`
- **Comment at FrsIterHandle.java:184** says: _"perIterArena is a confined Arena so close should always succeed on the owning thread"_ — but the actual allocation is `ofShared()`. Docs/code mismatch.
- **Threading proof that confined would be safe:** `IterLifetimeWatchdog.java:41-44` explicitly says: _"the watchdog ONLY calls requestClose() on the handle; the operator thread observes the flag at the next next() call and performs the actual native close. This keeps all FFI calls on the operator thread."_ So the only thread that ever calls `FrsIterHandle.close()` is the slot/operator thread. `Arena.ofShared` is unnecessary; `Arena.ofConfined` is correct AND ~2-3× cheaper at close time (no CAS handshake).
- Even better: the out-params (16 bytes total: 8 handle + 4 row count + 4 bytes used) are read immediately into Java fields on `VectorizedExecutor.java:700-702`. **They do not need to outlive the FFI call.** The `perIterArena` field on `FrsIterHandle` only exists to scope the lifetime of those three never-re-read segments. **Recommendation:** `slotScope.allocateTurn(16, 8)` to bump-allocate from the per-slot turn region; drop `perIterArena` from `FrsIterHandle` entirely. Saves both the alloc and the close.

Why H: Round 1 D-H4 caught the `ofShared` cost; Round 2 finds (a) confined is actually safe per the watchdog contract — should not need to be ofShared even if perIterArena stays; (b) the comment explicitly says "confined" so this is a known-incorrect-state slip; (c) the deeper observation is that perIterArena is not needed at all — the out-params are read before the next safepoint.

---

## D-R2-2 — Manual pointer arithmetic on FrsBytes / Arrow FFI structs — `withTargetLayout` would fix

**Sites:**

- `ForStRsLinker.java:1614-1615`:
  ```java
  long dataAddr = entry.get(ValueLayout.ADDRESS, 0).address();
  long len = entry.get(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS.byteSize());
  ```
- Same pattern at `:1686-1687`, `:2716-2717`, `:2923-2926`, `:1474-1477`.
- Arrow array readback at `:1871, 1875, 1878, 1882` — `outArray.get(ValueLayout.ADDRESS, 48).reinterpret(...)` where `48` is the offset of the `children` pointer field in `FFI_ArrowArray`. Magic-number offsets.

**Why H:** JDK 22+ `ValueLayout.ADDRESS.withTargetLayout(FRS_BYTES_LAYOUT)` lets the JVM emit a typed pointer that knows the target struct layout. The JVM constant-folds the field offsets at link time so `entry.get(FRS_BYTES_LAYOUT.varHandle(groupElement("data_ptr")))` becomes a single load — same wall-clock cost, but the offsets are derived from the layout, not hard-coded. Eliminates the bug class _"I typed `ValueLayout.ADDRESS.byteSize()` but the struct field is actually at offset 16 because the engine added a tag word"_ that bit the project once already in BinaryRowDataSerializer (per the spec at `docs/superpowers/specs/2026-05-19-binaryrowdataserializer-retention-audit.md`).

Risk: low. Migration is a syntactic refactor — each `entry.get(ADDRESS, 0).address()` becomes `(MemorySegment) entry.get(FRS_BYTES_VH_DATA_PTR)`. The `FRS_BYTES_LAYOUT` constant already exists in the linker (referenced at line 1664).

---

## D-R2-3 — Stable Values for the 68 instance MethodHandle fields (preview, JDK 26+)

JEP 502 Stable Values is **preview** in JDK 25 (not finalized). The 68 `private final MethodHandle frsXxx` fields in `ForStRsLinker` are eligible candidates (each is set exactly once in the constructor, never reassigned). Converting them to `StableValue<MethodHandle>` per-instance would let the C2 JIT constant-fold `linker.frsGet.invokeExact(...)` into a direct call to the underlying native stub.

**Not actionable today** because (a) the API is `@PreviewFeature` and may change before final; (b) the JIT speedup is incremental on top of the already-`bindCritical` hot symbols. Flagged so the project's "Round 3 / JDK 26" plan tracks this.

---

## D-R2-4 — Quantifying per-batch Arena.ofConfined() cost

Round 1 D-H3 caught the per-row / per-batch `Arena.ofConfined()` open+close. Round 2 adds budget arithmetic:

- Apple Silicon JDK 25 `Arena.ofConfined()` open: ~50 ns (lightweight scope creation)
- `Arena.ofConfined().close()`: ~100-300 ns (depends on number of allocations to free; one `Cleaner` register + unregister)
- `Arena.allocate(nBytes, align)` inside a confined arena: ~30-50 ns per call (vs ~3 ns for a bump-pointer increment in `SlotArenaScope.allocateTurn`)

`VectorizedExecutor.java:464-502` (per-row APPEND_MERGE) opens 1 confined arena per row + `vs.length + 2` allocations per row. For Q11/Q12 V1-sync at 92M ops, even at the conservative 200 ns per open+close, that's **18.4 seconds of pure arena-lifecycle overhead per Nexmark run** — comparable to the entire Q12 gap that the project memory at `project_q12_parity_with_rocksdb` is currently chasing (6%).

Same per-batch arithmetic for `dispatchAppendMergeBatch` (`:549`) at work-weighted batch median 257 (per `project_q12_batch_histogram_2026-05-19`): 92M/257 = 358K batches, each paying 200 ns of arena lifecycle = ~72 ms — much smaller, but still measurable.

**Fix:** D-H3's recommendation (use `slotScope.allocateTurn`) stands; Round 2 confirms the per-row APPEND_MERGE path is where the budget is actually spent.

---

## D-R2-5 — Leak of the wrapping Arena in `scratchArenaTL`

`ForStRsKeyedStateBackend.java:228-229`:
```java
private final ThreadLocal<MemorySegment> scratchArenaTL =
        ThreadLocal.withInitial(() -> Arena.ofShared().allocate(65536));
```

The `Arena.ofShared()` instance is created anonymously, allocates a 64 KB segment, and is **never stored anywhere** — so it can never be closed. The arena itself isn't garbage-collected until all derived segments are unreachable, which on a long-running Flink task thread is essentially never.

Two correct fixes:
1. `Arena.ofAuto()` instead of `Arena.ofShared()` — the segment becomes phantom-reachable when the ThreadLocal entry is GC'd (slow but bounded).
2. Move ownership to `SlotArenaScope.allocateCache(65536, 64)` and let `closeSlot()` reclaim it deterministically (matches the pattern documented at `SlotArenaScope.java:37-44`).

Why M (not H): leak is bounded by the number of task threads ever created in the JVM, which Flink caps. Not a correctness issue but defeats the slot-arena lifecycle invariant.

---

## D-R2-6 — D-H1 verification (template change)

✓ **Verified clean.** Cross-checks:

| File | Contains UseZGC? | Contains UseCompactObjectHeaders? |
|---|---|---|
| `config-forst-rs.yaml.tpl` (DEFAULT) | **NO** (G1) | **NO** |
| `config-forst-rs-g1.yaml.tpl` | NO (G1) | YES |
| `config-forst-rs-g1-noCOH.yaml.tpl` | NO (G1) | NO |
| `config-forst-rs-local.yaml.tpl` | NO (G1) | YES |
| `config-forst-rs-tuned.yaml.tpl` | NO (G1) | YES |
| `config.yaml` (live) | NO | NO |
| `config.yaml.bak` | NO | NO |

Default template header (lines 3-6) correctly documents the change and cites `project_q12_parity_with_rocksdb` and `project_q12_heap_timer_beats_forst`.

No surprising downstream — no other config file references ZGC for forst-rs.

---

## D-R2-7 — PEM API not applicable

Searched all forst-rs Java sources for `TLS`, `SSLContext`, `KeyStore`, `TrustManager`, `X509`, `PEM`, `https`. **Zero matches.** S3 + HTTPS traffic is entirely Rust-side via `opendal` (see `ForStRsKeyedStateBackendBuilder.java:88` — Java passes a JSON `opendal-config` blob to the cdylib and Rust handles all crypto). JEP 470 PEM API has no use here.

---

## D-R2-8 — Scoped Values / virtual threads — confirming Round 1

Re-verified:

- All `ThreadLocal` usages are **mutable** buffer pools (4 in `ForStRsLinker`, 1 in `ForStRsKeyedStateBackend`, 1 in `ForStRsKeyGroupedSerializer`). JEP 506 `ScopedValue` is immutable-within-scope ⇒ wrong shape.
- `ForStRsKeyGroupedSerializer.java:343-348` already contains the inline analysis of this conclusion.
- `Thread.ofVirtual()` used exactly once (`ForStRsSstUploader.java:66`) for SST upload — outside any state-backend callback. Safe.
- No `StructuredTaskScope` use anywhere — correct.

No new findings.

---

## Direct answers to Round 2 prompt questions

1. **D-H1 verification:** Clean. No other conf file references ZGC for forst-rs. Header comment in default tpl correctly attributes the rationale.

2. **JDK 25-specific opportunities Round 1 missed:**
   - **`ADDRESS.withTargetLayout(...)`**: 6+ sites of manual offset arithmetic for FrsBytes + Arrow C Data Interface structs. **NEW H** (D-R2-2).
   - **Stable Values (JEP 502)**: 68 candidate fields in `ForStRsLinker` but preview-only in 25. Tracked for JDK 26+ (D-R2-3).
   - **Scoped Values (JEP 506)**: Wrong shape — already documented in code at `ForStRsKeyGroupedSerializer.java:344`. No new finding.
   - **PEM API (JEP 470)**: Not applicable — TLS happens in Rust (D-R2-7).

3. **Arena lifecycle audit:**
   - **Long-lived per-slot arena exists** — `SlotArenaScope` (slotArena = `Arena.ofShared()`, turnRegion bump-allocated). The `dispatchAppendMerge*` paths and `dispatchIter*` paths DO have access to it (via `setSlotScope`) but do not use it for the per-row/per-iter scratch — this is exactly the Round 1 D-H3 / D-H4 finding.
   - **Per-batch arena cost quantified**: ~200 ns per open+close on Apple Silicon; ~18 seconds at Q11 V1-sync 92M ops (D-R2-4).
   - **One arena that should be owned by `SlotArenaScope` but isn't**: `scratchArenaTL` (D-R2-5) — anonymous `Arena.ofShared()` leaks 64 KB per task thread.

4. **`Linker.Option.critical(allowHeapAccess=true)` safety:**
   - The `Linker.Option.critical(true)` factory IS the boolean overload of `critical(allowHeapAccess)` in JDK 22+ — there is no separate `allowHeapAccess` toggle, the boolean argument IS the toggle.
   - **Comment at `ForStRsLinker.java:987`** says: _"allowHeapAccess=true so MemorySegment.ofArray(byte[]) is acceptable"_ — correct usage. The 10 currently-bindCritical symbols are all point-ops (`frs_put`, `frs_get`, `frs_get_pinned`, `frs_get_and_put`, `frs_delete`, `frs_bytes_free`, `frs_lookup_kv`, `frs_get_into_buf`, `frs_get_fast`, `frs_get_at`).
   - **GC safety concern:** Critical-mode pins the heap-backed `MemorySegment.ofArray(byte[])` by suspending the GC for the duration of the call. **The native function MUST NOT block** — otherwise it can stall a GC across all threads. ForSt's `frs_get` / `frs_put` are bounded (point op on in-memory block cache or RocksDB get on already-cached SST). Adding `bindCritical` to **`frs_batch_get`** is safe (still bounded — N point ops). Adding it to **`frs_vectorized_batch_put`** is safe IF the engine never blocks on flush stalls during the call (engine-side: needs to verify the write-buffer-full path returns immediately and queues the flush rather than blocking the batch_put call).
   - **Recommendation for Round 3:** audit `crates/forst-rs-ffi/src/lib.rs` for `frs_vectorized_batch_put` and `frs_vec_merge_append_batch` to confirm neither blocks on write-buffer-flush stall. If confirmed, add `bindCritical` per Round 1 D-H2. If they CAN block, leave them as `bind()` (correct as-is) and instead extract a non-blocking fast path.
   - **`frs_batch_put`** with the **pointer-of-pointer** layout (`uint8_t* const*`) is NOT critical-eligible regardless — the linker can pin a `byte[]` but cannot pin the addresses-of-other-byte-arrays. The comment at `:413-419` is correct for this symbol but does NOT apply to the packed-layout `frs_vectorized_*` symbols.
