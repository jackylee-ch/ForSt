# A1 — ForSt-RS Status Assessment + Three-Goal Gap Analysis

> **v3.2 Phase A deliverable.** Authored as Tech VP / Coordinator on 2026-05-07.
> Authority chain: this A1 layers G-A / G-B / G-C analysis on top of `.planning/refactor-review/A1_reconciliation.md @ 33f85b1c5` (the existing arch-pivot authority). Where the v3.2 Master Prompt and the existing reconciliation conflict, **the existing reconciliation wins** per its self-declared supersession over Master Prompt v2 (`A1_reconciliation.md:4`); this A1 records the deltas, not contradictions.

---

## 0. Framing — v3.2 wraps the existing C1–C9 framework

Per the user's framework decision (recorded 2026-05-07): **"v3.2 wraps C1–C9"**. The existing 9-commit refactor-review framework (`COMMIT_MANIFEST.md`, `REVIEW_PROTOCOL.md`, `A1_reconciliation.md`, `SESSION_HANDOFF.md`) executes as v3.2 Phase D. v3.2 does NOT replace it. Concretely:

| v3.2 Phase | What runs |
|---|---|
| Phase A | THIS document — three-goal gap on top of the existing reconciliation |
| Phase B | Pointer to `.planning/refactor-review/COMMIT_MANIFEST.md` (already auto-generated 2026-04-25) **+ a new C10 for Delta Join `ForStRsLookupKv` surface** (G-C is not covered by C1–C9) |
| Phase C | Already done in part — 7 crates exist, 7 GHA workflows green; remaining: Flink-side `flink-statebackend-forst-rs` module bootstrap |
| Phase D | C1–C9 review-loop per existing `REVIEW_PROTOCOL.md` (currently mid-C1 R2 done; R3 ready) |
| Phase E | Embedded in Phase D — 10 fixed reviewer dimensions, terminate at `H=0 ∧ M=0 × 10 OR 120 rounds` (NOT v3.2's 7→12 / "10 rounds zero-High / 100-cap" — see §7) |
| Phase F | Already done by `G1_gha_rebuild_plan.md` + session E (2026-05-02). v3.2 Phase F = delta = cross-repo integration CI + Delta Join E2E lane |
| Phase G | Module benchmarks — Cn benches done in `forst-rs-bench/` micro tier; missing baselines (RocksDB v8.11.3, original ForSt) blocked on baseline build (per A1 §4.3, gates C5) |
| Phase H | Reuses `N2_nexmark_plan.md` Lane A (vendored at SHA `6b3646c`) + Lane B (user-hardware-bound); **adds Delta Join dedicated E2E + 72h soak** as new v3.2 obligations |
| Phase I/J | Net-new — final perf report + production-readiness verdict |

---

## 1. Read Docs Inventory + Summary (v3.2 §0.1 mandatory pre-step)

**34 markdown files / 29,016 lines** read under `docs/`:
- `docs/design/` — **13 files / 11,406 lines** — architectural decisions
- `docs/understanding/` — **17 files / 10,326 lines** — Wave-1 research grounding (R1–R13 gap analysis, FlinkStateBackend semantics, Fluss layout study, etc.)
- `docs/superpowers/plans/` — **5 files / 6,506 lines** — completed P1.2 task plans (W5–W9: SST writer/reader/bloom, MemTable)

### 1.1 Per-file design inventory

| File | Lines | Status | Purpose | Goal |
|---|---|---|---|---|
| `docs/design/2.1_overall_architecture.md` | 981 | Stable | 5-layer architecture, 14 core traits, 8-bottleneck response, 3-stage evolution | All |
| `docs/design/2.2_arrow_sst_format.md` | 670 | Stable | Arrow-based SST: 16B FileHeader, DataBlock=RecordBatch, SBBF, SparseIndex, FRST footer | G-A,G-B |
| `docs/design/2.3_memtable_design.md` | 964 | Stable | Hybrid SortedRunArray + BTreeMap; 4-column Arrow layout (key/value/seq/op_type) | G-A,G-B |
| `docs/design/2.4_block_cache_design.md` | 1121 | Stable | 3-tier cache: L1 ShardedClock (Heap), L2 NVMe SSD, L3 Remote | G-A,G-B |
| `docs/design/2.5_ffm_bridge_design.md` | 929 | Stable | 4-layer Java→Rust bridge; **22 core extern "C" fns** (12 DB + 5 Checkpoint + 5 Config); FFM-first / JNI-fallback (D-02-02 ratified); FFM ~22.7 ns vs JNI ~56.6 ns | **G-B** |
| `docs/design/2.6_compaction_design.md` | 1422 | Stable | Tiered (L0–L2) + Leveled (L3+); Arrow columnar merge-sort; CompactionFilter trait for TTL | G-A |
| `docs/design/2.7_checkpoint_integration.md` | 742 | Stable | Mode B Checkpoint: VersionSet snapshot + freeze MemTable (no forced flush, fixes B5); Restore <10 ms | G-A |
| `docs/design/2.8_read_write_paths.md` | 1001 | Stable | Write: WriteBatch → MemTable → Flush; Read: SnapshotView → MemTable → Imm → L0..Ln + 3-tier cache | G-A,G-B |
| `docs/design/2.9_flink_statebackend_adapter.md` | 1129 | Stable | `ForStRsStateBackend` factory, `ForStRsKeyedStateBackend` (Async + Sync); 5 state types, dual-track v1/v2 | **G-A & G-B** |
| `docs/design/2.11_rust_crate_structure.md` | 612 | **Stale on naming** — see §3.4 | 8 crates planned (incl. `forst-rs-api` + `forst-rs-bridge`); disk has 7 (no `forst-rs-api`; `forst-rs-bridge` renamed to `forst-rs-ffi`) | All |
| `docs/design/2.12_implementation_roadmap.md` | 1143 | **Decision gate D-02-03 unratified** | Three-phase 40-week roadmap (P1 16w + P2 12w + P3 12w); M1–M10 milestones; §9 codifies 10-Agent review + H/M=0 × 10 termination + 120-round cap (this is the upstream of the existing protocol) | All |
| `docs/design/2.13_deltajoin_localization.md` | 670 | Stable | DeltaJoin embedded in Flink TM via FFM; zero RPC, zero Fluss service; `ForStRsLocalLookupFunction` + `ForStRsMultiJoinMergeCoordinator` | **G-C** |

### 1.2 Understanding-doc highlights

`1.1` ForSt module inventory (~1500 src files); `1.2` ForSt runtime arch; `1.3` JNI surface (~1461 native methods, 4 transfer modes); `1.4` Flink 2.2 dual-track API (sync/v1 + async/v2); `1.5` ForSt incremental snapshot mechanics; `1.6` bmr-2.1.0 has no ForStDisagg*; `1.7` 8 bottlenecks ranked (B1 JNI > B2 double-serialization > B3 BlockCache miss > B4 write-lock > B5 Checkpoint flush > B6 Compaction > B7 Cache shard-lock > B8 nioBuffer copy); `1.8` R1–R13 gap matrix → P0/P1/P2; `1.9` Java↔Rust zero-copy feasibility (FFM ~22.7 ns vs JNI ~56.6 ns); `1.10` Fluss storage layout (informs G-C semantic alignment); `1.11` Parquet format notes (informs Arrow SST); `1.12` 15-scenario Flink matrix; `1.13` Rescaling (KeyGroupRangeAssignment, clipDB); `1.14` Schema Evolution (ForSt-RS does NOT manage; passes opaque bytes — constraint C4); `1.15` ForSt MANIFEST study; `1.16` internal-metadata feasibility (**Mode B selected**); `1.17` Delta Join feasibility (FLIP-486; Fluss 0.8 transparent rewrite).

### 1.3 Five architectural invariants (cite-locked)

1. **5-layer architecture** — `docs/design/2.1_overall_architecture.md:62-72`. Bridge (5) → API (4) → Engine (3) → Storage (2) → IO (1). Decision rationale at `2.1:915-931` (D1).
2. **Mode B pure-Checkpoint metadata (no MANIFEST file)** — `2.1:962-972` (D4); `1.16` selects Mode B over A/C; consequence: VersionSet is in-memory `ArcSwap`, Flush/Compaction are pure memory ops, recovery <10 ms.
3. **Arrow RecordBatch as the engine's internal data unit** — `2.1:973-981` (D5); MemTable, SST DataBlock, BlockCache all share the 4-column schema (key/value/seq/op_type). Cross-refs: `2.2:118`, `2.3:84-100`, `2.4:71-83`.
4. **FFM-first / JNI-fallback bridge** — `docs/design/2.5_ffm_bridge_design.md:24-39` + `docs/design/2.12:411-417` (D-02-02 ratified). Min C ABI surface = **22 functions**. FFM perf budget: ≥30% lower call overhead than JNI.
5. **DeltaJoin = embedded library, not a service** — `docs/design/2.13:14-29` + `docs/design/2.1:138-153`. ForSt-RS embeds in Flink TM, replaces Fluss's Lookup role only via FFM/C ABI; no cross-process RPC, no daemon. Storage tier: BlockCache + SSD warm cache, SST direct-write to S3.

### 1.4 Open questions / unresolved gates in docs

- `docs/design/2.12 D-02-03` — three-phase 40w roadmap formally unratified (de-facto ratified by ongoing C1–C9 work)
- `docs/design/2.12 D-02-05` — Phase 1 C ABI compat strategy (full vs semantic) — recommended "complete compat", unratified
- `docs/understanding/1.4` Open Questions 4/5/6 — Native Metrics, state migration TODO, sync v1 retention
- `docs/understanding/1.8` Phase 1 minimum viable scope, FLIP requirement, perf-baseline establishment
- **Plans coverage stops at W9 (MemTable)**; W10–W16 (VersionSet/Engine/Compaction/Checkpoint/C ABI) migrated to `.planning/refactor-review/B1_implementation_plan.md` + `COMMIT_MANIFEST.md`

---

## 2. ForSt-RS Repo Assessment (`forst-rs` branch)

### 2.1 Workspace layout

`/Users/lijunqing/Code/stczwd/ForSt` on branch `forst-rs`. Workspace has **7 crates / 26,432 LoC / 791 unit tests / 5 integration test files / 4 benches**:

| Crate | LoC | Tests | Benches | Module surface | Maps to docs/design |
|---|---|---|---|---|---|
| `forst-rs-common` | 3,222 | 132 | — | `arena`, `checksum`, `coding`, `config`, `error`, `metrics`, `types`; `#![forbid(unsafe_code)]` | crosscut |
| `forst-rs-engine` | 5,819 | 164 + 1 integration | — | `db` (`DbImpl`), `column_family`, `write_batch`, `write_controller`, `flush`, `compaction`, `compaction_filter`, `checkpoint`, `snapshot_view`, `file_deletion_guard` | 2.6, 2.7, 2.8 |
| `forst-rs-storage` | 8,800 | 253 + 3 integration | — | `sst/` (10 files), `memtable/vectorized.rs`, `cache/clock.rs`, `merge_operator.rs`, `version/{mod,checkpoint}.rs` | 2.2, 2.3, 2.4 |
| `forst-rs-io` | 5,178 | 182 | — | `filesystem`, `local_fs`, `memory_fs`, `object_store` (**Phase-1 skeleton; `MockObjectStore` only**), `router`, `async_io`, `ownership` | 2.13 §5.2 |
| `forst-rs-ffi` | 2,401 | 27 + 1 integration | — | One file, **28 `pub unsafe extern "C" fn`** exports + `FrsBytes`/`FrsDb`/`FrsCfHandle` opaque handles + `catch_unwind` panic guards | 2.5 |
| `forst-rs-bench` | 384 | 0 | 4 | `point_lookup.rs` (BM-1.1), `batch_ops.rs` (BM-1.2), `write_throughput.rs` (BM-1.3), `checkpoint.rs` (BM-1.4); criterion, in-memory only | 2.12 §8 |
| `forst-rs-test-utils` | 628 | 33 | — | `temp_dir`, `kv_gen`, `assert_helpers` | — |

**Recent commit cadence** (R5–R13 sweeps): 9 security-style hardening commits since R2 closed (2026-05-05). All defensive — `MAX_BATCH_COUNT=1_000_000` cap on FFI batch entrypoints, untrusted-size DoS guards at SST footer/data-block, deserialization OOM guards at checkpoint blob, lifetime-tightened `cstr_to_str`, OpType silent-default fix, NULL-pointer UB in batch FFI. **No functional regressions.** The R-sweep track runs in parallel to (not within) the C1 round counter.

### 2.2 GHA workflows (already exist — v3.2 prompt assumed none)

`.github/workflows/`: **7 files, all green per session E (2026-05-02)**:
- `ci-rust.yml` — fmt / clippy / build / test / `cargo llvm-cov` ≥ 80% gate (ratchet to ≥90%)
- `ci-bench.yml` — nightly criterion + GH Pages perf-trend
- `ci-e2e.yml` — stub (will activate when Java JAR exists)
- `ci-java.yml` — stub
- `ci-release.yml` — tag-driven multi-platform matrix (ubuntu/macos × x64/arm64); JDK 25.0.3 pinned
- `ci-security.yml` — cargo-audit + cargo-deny + dependabot
- `nexmark-baseline.yml` — N2 Lane A smoke build

### 2.3 Crate-decomposition discrepancies vs docs

| Item | Docs/design plan | Disk reality | Verdict |
|---|---|---|---|
| Layer 5 Bridge crate | `forst-rs-bridge` (`2.11`) | `forst-rs-ffi` | **Naming drift** — likely renamed by arch-pivot; `2.11` stale |
| Layer 4 API crate | `forst-rs-api` (`2.11`) | **Missing** | Either folded into `forst-rs-engine` or pivot-removed; A1_reconciliation should clarify (currently silent) |
| Future Layer 5 | `forst-rs-deltajoin` | Not present | Expected — Phase 2 (M8) |
| Future Layer 6 | `forst-rs-remote-service` | Not present | Expected — Phase 3 (M10) |

### 2.4 TODO/unimpl density

`grep -rEn 'todo!\(\)|unimplemented!\(\)|FIXME|TODO|panic!\("not yet'` across `crates/` returns **zero** matches. Stale `db.rs:23` doc comment ("Compaction (W15) and the C ABI bridge (W16) are not yet implemented") is outdated; both are landed.

---

## 3. Flink-Side Assessment (`forst-rs-jdk25` branch)

### 3.1 v3.2 prompt corrections

| v3.2 reference | Wrong | Correct |
|---|---|---|
| Branch name (§2.1, §3.1, §9.1, §14.1) | `forst-rd-jdk25` | **`forst-rs-jdk25`** |
| §7.2.2 expectation | "Whether Delta Join operator changes exist" | Delta Join is stock Apache upstream; **no fork-specific changes**. G-C scope is "implement an `AsyncTableFunction<RowData>` adapter," NOT "replace Fluss client" |
| §R14 / §13.2 / §14.3 framing | "Replace Fluss client integration" | This Flink fork has **zero `Fluss` references in source** (`grep -rln 'fluss\|Fluss' --include='*.java' --include='*.xml'` → 0 in src; only doc-comment mentions in `docs/content*/dev/table/tuning.md`). The Fluss-baseline benchmark is a separate, optional comparison setup |
| Flink upstream branch (§3.2 ref) | `release-2.2.0` | Apache Flink uses `release-2.2` (no `.0`) — also surfaced by `N1_nexmark_research.md` |
| GHA status (§7.2.2) | "Existing GHA state" | **10 workflows present**; JDK 25 lane wired (best-effort); 0 ForSt-RS-specific |

### 3.2 Diff scope vs upstream `release-2.2`

Only **5 commits** on `forst-rs-jdk25` ahead of upstream — all build-substrate / JDK 25 prep:

| SHA | Title | Theme |
|---|---|---|
| `7214718ae4d` | [FLINK-38912] Upgrade Netty to 4.2.6.Final | Build infra |
| `1adda46ce8a` | [FLINK-38280] Add JDK 24+ test skips | JDK 25 prep |
| `ac7846b5657` | [FLINK-38280] Code fixes for Jackson 2.20 + Netty 4.2 + JDK 26 forward-compat | JDK 25 prep |
| `67934a3c2ba` | [FLINK-38280] Add JDK 25 build, dependency, and CI infrastructure | **JDK 25 substrate** — flink-shaded 21.0, Hadoop 3.4.3, Pekko 1.4.0, java25-target Maven profile, Zulu 25 toolchain, JDK 25 lane in `template.flink-ci.yml` |
| `c6f486ef9f6` | [chore][docs] Add JDK 25 Phase-0 feasibility design and plan scaffolding | Docs only |

Themes: **100% JDK 25 substrate. Zero ForSt-RS, FFM, Delta Join, or Arrow integration.**

### 3.3 Module / FFM / Delta Join state

- `flink-statebackend-forst` exists (108 main + 32 test Java files) using `com.ververica:forstjni:0.1.8` (`flink-state-backends/flink-statebackend-forst/pom.xml:65`). Native loading at `ForStStateBackend.java:889` and `:975`. **G-A drop-in target identified**: replace this loader path with `libforst_rs.so` while preserving public class names/signatures.
- `flink-statebackend-forst-rs` module: **does NOT exist.** All scaffolding for G-B is greenfield.
- FFM call sites: `grep -rln 'java.lang.foreign.{Arena,MemorySegment,Linker}' --include='*.java'` → **0 hits**. `module-info.java` files: 0. `--enable-native-access` references: 1 (test scaffolding only).
- Delta Join: lives in `flink-table/flink-table-runtime` (`StreamingDeltaJoinOperator.java`, `StreamingDeltaJoinOperatorFactory.java`, `AsyncDeltaJoinRunner.java`, `DeltaJoinCache.java`). Lookup mechanism is stock `GeneratedFunction<AsyncFunction<RowData, Object>>` (`AsyncDeltaJoinRunner.java:108-132`). No Fluss, no `LookupKv`, no `prefixLookup` — these are Fluss's terms not present here.

### 3.4 Untracked Flink-side artifacts (relevant)

- `docs/superpowers/specs/2026-04-30-jdk25-upstream-prior-art-survey.md` (151 lines) — survey of upstream JDK 25 fixes already cherry-picked + 3 still-actionable cherry-picks (PR #28028 `Thread#stop`, PR #27008 Mockito 5.19, PR #27470 Pekko 1.4.0). Phase 4 (Netty 4.2 cleaner / JDK 25 SSL handshake) is genuinely net-new.
- `forst-backport-feasibility-report.md` (655 lines) — orthogonal: backport ForSt to Flink 1.17–1.19, not 2.2.0 target.
- `findings.md`, `progress.md`, `task_plan.md`, `docs/migrate/`, `docs/versions/` — Flink-side session leftovers, unaudited, no obvious A1 input.

---

## 4. Three-Goal Gap Analysis

### 4.1 G-A — Compat with existing ForSt interface (drop-in replacement)

| Interface / Capability | Original ForSt impl | Current ForSt-RS state | Gap | Fill strategy |
|---|---|---|---|---|
| `ForStStateBackend` Java surface | C++ ForSt + JNI via `com.ververica:forstjni:0.1.8` | 108 main + 32 test Java files exist on Flink side; **still loads `forstjni`, not `libforst_rs.so`** | Native loader swap | New paired PR (Flink-side): redirect `NativeLibraryLoader` to `libforst_rs.so`; FFI ABI must absorb every `org.forstdb.*` JNI surface used by Flink (~30+ files import) |
| Native lib | `forstjni` cdylib (RocksDB-fork) | `crates/forst-rs-ffi` exports 28 `frs_*` functions | **Naming + scope mismatch**: Flink side calls `org.forstdb.RocksDB.put` (etc.), ForSt-RS exports `frs_put`. G-A requires either (a) a JNI-shaped shim layer or (b) Flink-side rewrite to call `frs_*` via FFM | Decision pending — A1_reconciliation §3 silent on this. **Recommend escalating: which compat strategy? (a) JNI-shaped shim preserves binary compat for downstream Flink users; (b) FFM rewrite is cleaner but couples G-A to G-B**. Default per docs/2.5 D-02-02 = (b) FFM-first |
| 5 state types (Value/List/Map/Reducing/Aggregating) | Full | docs/2.9 design covers all 5; engine `crates/forst-rs-engine/db.rs` covers core CRUD; merge operator (`StringAppendOperator` analog → `ListAppendMergeOperator`) present | Java-side wiring depends on §8.3 path: under (a) JNI-shim, retarget existing `flink-statebackend-forst`; under (b) FFM-first, create new `flink-statebackend-forst-rs` module | Phase D paired-PR per `docs/design/2.9` — exact target depends on §8.3 decision |
| Sync (v1) + Async (v2) dual-track | Full (`ForStStateBackend.supportsAsyncKeyedStateBackend()` returns true at `:400`) | Engine supports both via `DbImpl` + `WriteController`; FFI exports cover sync path; **no async/futures FFI variants yet** | Async FFI surface | Add `frs_get_async` / `frs_batch_get_async` to `forst-rs-ffi`; tie into Tokio runtime; cross-FFI futures via callback handle (per docs/2.5) |
| Checkpoint (full + incremental) | Full + incremental | `forst-rs-engine::checkpoint` covers full snapshot; **incremental missing** | Incremental checkpoint | Implement delta-encoding in `checkpoint.rs`; required for v3.2 §13.3 perf target (≤80% of original ForSt) |
| Rescaling (`clipDBWithKeyGroupRange` + `IngestDB`) | Full | docs/2.7 design covers `clipDB`; engine code presence unverified in this audit | Verify + close gap | Audit `forst-rs-engine` for clipDB equivalent; add if missing |
| TTL via `CompactionFilter` | Full | `forst-rs-engine::compaction_filter::TtlCompactionFilter` present | None | ✅ |
| Snapshot/SnapshotView | Full | `forst-rs-engine::snapshot_view::SnapshotView` present; **not exported via FFI** | Export `frs_snapshot_create` / `frs_snapshot_release` | Add to `forst-rs-ffi` |
| Iterator (`Iterator`/`Cursor`) | Full | **Missing — only single-shot `frs_prefix_scan_arrow`** | Iterator FFI surface | Add `frs_iter_create/next/release` to `forst-rs-ffi` |
| Native Metrics interface | Full | docs/1.4 OQ4 unresolved | Decision item | Defer to G-A acceptance phase |
| Schema Evolution | TypeSerializerSnapshot 4-result matrix | ForSt-RS does NOT manage; passes opaque bytes (constraint C4 in `docs/design/2.1`) | None — by design | ✅ |

**G-A coverage in C1–C9**: C1 (common) → C2 (io) → C3 (sst) → C4 (memtable+merge) → C5 (cache) → C6 (version+checkpoint) → C7 (engine core) → C8 (engine background) → C9 (FFI + Arrow). Does **not include** the iterator/snapshot FFI exports or the incremental checkpoint. **Recommendation**: amend C9 to add iterator + snapshot exports OR add a new C-row.

### 4.2 G-B — forst-rs backend + FFM

| Interface / Capability | Expected | Current | Gap | Fill strategy |
|---|---|---|---|---|
| `ForStRsStateBackend` Java class | New `flink-statebackend-forst-rs` module per docs/2.9 + 2.11 | **Module does not exist** on Flink side | Greenfield | Phase D Flink-paired PR: create Maven module, wire into `flink-state-backends/pom.xml`, implement `ForStRsStateBackend` + `ForStRsStateBackendFactory` |
| `module-info.java` declarations | Required for JDK 25 FFM `--enable-native-access` opt-in | **0 `module-info.java` in repo** | Greenfield | Add to new module + any caller modules |
| FFM downcall path | `Linker.downcallHandle` calling `libforst_rs.so` symbols; ≥30% lower overhead than JNI | **Zero FFM call sites in production code** (`grep` confirms) | Greenfield | Implement in `forst-rs-java-bridge` per docs/2.5 4-layer arch: Java Shim → C ABI (`frs_*`) → Safe Adapter → Engine |
| Arrow zero-copy path | `Java ArrowBuf ↔ FFM MemorySegment ↔ Rust arrow-rs Buffer` (same native memory) | Rust side: `frs_batch_put_arrow` + `frs_batch_get_arrow` + `frs_prefix_scan_arrow` exist (G-B Arrow surface ready). **Java side: nothing.** | Java half greenfield | Implement `BufferRegistrationToken` + Arrow C Data Interface release-callback per docs/1.9 |
| `--enable-native-access` opt-in | JVM flag for caller modules | 1 hit in test scripts only | Greenfield | Add to `pom.xml` JVM args; document in user runbook |
| Config registration | `ForStRsStateBackendFactory` SPI in `META-INF/services/` | Missing (module doesn't exist) | Greenfield | Add SPI file in new module |
| Coverage in C1–C9 | C9 covers Rust-side FFI + Arrow surface | C9 not yet started; **Java side is out of C1–C9 scope** | New work-unit needed | **Recommend a new "L-series" of paired Flink-side commits** (L1: module bootstrap, L2: FFM Linker setup, L3: ForStRsStateBackend impl, L4: ForStRsKeyedStateBackend Async, L5: ForStRsKeyedStateBackend Sync) |

**G-B coverage in existing manifest**: C9 only covers Rust-side; Flink side (~5 paired commits L1–L5) is net-new.

### 4.3 G-C — Delta Join hookup

> **Major framing correction vs v3.2 §R14:** Apache Flink Delta Join in this `forst-rs-jdk25` fork has **zero Fluss references in source code** (`grep -rln 'fluss\|Fluss' --include='*.java' --include='*.xml'` → 0 in src). The lookup mechanism is stock `GeneratedFunction<AsyncFunction<RowData, Object>>` (`AsyncDeltaJoinRunner.java:108-132`). G-C work is therefore **"implement an `AsyncTableFunction<RowData>` adapter backed by `ForStRsLookupKv`"**, NOT "replace a Fluss client." This is **strictly easier** than v3.2 framed it. The "no Fluss service required" acceptance is **trivially satisfied** because Fluss isn't here in the first place.

| Interface / Capability | Expected (per v3.2 §R14) | Current | Gap | Fill strategy |
|---|---|---|---|---|
| `ForStRsLookupKv.get` (sync) | Java entry calling `frs_get` via FFM | `frs_get` exists in `forst-rs-ffi` (`lib.rs:494-577`); Java entry missing | Java-side adapter | New Flink module + FFM downcall |
| `ForStRsLookupKv.getAsync` | Java CompletableFuture wrapping `frs_get_async` | **Async FFI variants don't exist on Rust side**; Java side missing | Both sides | Add `frs_get_async` to `forst-rs-ffi` (Tokio + cross-FFI callback); implement Java wrapper |
| `ForStRsLookupKv.batchGet` | Java entry calling `frs_batch_get_arrow` | `frs_batch_get_arrow` exists (`lib.rs:925-1255`); Java entry missing | Java-side adapter | New Flink module |
| `ForStRsLookupKv.prefixScan` | Java entry calling `frs_prefix_scan_arrow` | `frs_prefix_scan_arrow` exists; Java entry missing | Java-side adapter | New Flink module |
| `ForStRsLookupKv.subscribeChangelog` | CDC stream subscription via Compaction-driven changelog | **Missing entirely on Rust side**; docs/2.13 §6 design exists, no FFI export | Both sides | Add `frs_changelog_subscribe` / `frs_changelog_poll` to `forst-rs-ffi`; engine-side: tap Compaction events; Java-side: thread + callback |
| Snapshot consistency w/ Checkpoint | Aligned with Checkpoint barriers | Engine has `SnapshotView`; FFI doesn't expose snapshot handles | FFI + protocol | Tied to G-A iterator/snapshot exports + Checkpoint barrier coordination |
| Flink Delta Join operator hookup point | `AsyncTableFunction<RowData>` adapter wiring into Flink's `LookupTableSource` codegen | **Missing** | New SQL connector or DynamicTableSource | Phase D Flink-paired PR — significant work, may need its own L-series |
| Config `table.exec.delta-join.kv-backend = forst-rs` | Flink ConfigOption + planner glue | **Missing** | Configuration | Add ConfigOption in `flink-table-api-java` or new connector module |
| Coverage in C1–C9 | **None — G-C is not in the C1–C9 manifest** | Need new C10 | New work-unit | **Recommend: add C10 for Rust-side Delta Join changelog FFI + scaffolding for L-series Java-side adapter** |

**Hard acceptance** (v3.2 Phase H Delta Join E2E):
- ≥1 Delta Join job runs E2E (≥2-way join) — net-new test infrastructure
- Throughput ≥80% of Fluss baseline — **requires standing up Fluss separately as a comparison baseline** (out of regular dev environment)
- P99 latency ≥80% of Fluss baseline — same caveat
- Deployment complexity: ✅ trivially satisfied (no Fluss service in deployment)

---

## 5. Stage Verdicts (v3.2 §7.2.4)

### 5.1 Stage 1 (architecture & design docs): ✅ Done

Evidence (≥5):
1. `docs/design/` has 13 files / 11,406 lines covering all 5 layers + 3 stages of evolution.
2. `docs/understanding/` has 17 files / 10,326 lines grounding the architecture in original ForSt + Flink + Fluss source study.
3. Top 5 invariants are cite-locked (§1.3 above) — 5-layer / Mode B / Arrow / FFM / DeltaJoin embedded.
4. R1–R13 gap matrix (`docs/understanding/1.8`) maps to P0/P1/P2 priorities; implementation order codified.
5. `.planning/refactor-review/A1_reconciliation.md @ 33f85b1c5` is the active arch-pivot authority, layered above docs/.
6. `REVIEW_PROTOCOL.md` codifies the 10-dimension review + termination rules — landed and operational.

**Caveats:** D-02-03 (40-week roadmap) and D-02-05 (C ABI compat strategy) are unratified gates. Crate naming drift (`forst-rs-bridge` → `forst-rs-ffi`) and Layer 4 `forst-rs-api` absence not formally reconciled in docs.

### 5.2 Stage 2 (core module code skeleton): 🟡 Partial

Evidence (≥5):
1. **All 7 planned crates exist on disk**: `forst-rs-{common, engine, ffi, io, storage, bench, test-utils}` (8th crate `forst-rs-api` from docs plan absent).
2. **26,432 LoC / 791 unit tests / 5 integration test files** — solid implementation footprint.
3. **M1 ✅ Done** (basic compile, all 7 crates + 7 GHA workflows green).
4. **M2 ✅ Done** (SST writer/reader/bloom; 1M-key write→read verified per `docs/superpowers/plans/2026-04-22-p12-w8-sst-reader.md`).
5. **M3 🟡 Partial** (MemTable W9 done; BlockCache via `ShardedClockCache` done; VersionSet via `ArcSwap` done — VersionSet R6 sweep fixed lost-update race, indicating active maintenance).
6. **M4 / M5 🟡 In progress via C1–C9 sweep** — currently at C1 R2 done, R3 ready; R5–R13 security sweeps shipping in parallel.
7. **28 FFI exports** in `forst-rs-ffi` cover lifecycle / point ops / batch / maintenance / checkpoint / introspection / Arrow; missing: changelog, iterator, snapshot, async.
8. **Zero TODO/unimpl markers** across `crates/` — unusually clean for a mid-flight refactor.

**Major gaps:**
- **OpenDAL not integrated** (Cargo.toml has `arrow` but not `opendal`). `forst-rs-io::object_store` is "Phase-1 skeleton" with `MockObjectStore` only. v3.2 §R8 mandates OpenDAL. **Single biggest gap.**
- **Hot/Warm/Cold tiering**: only L1 (heap `ShardedClockCache`); L2 (NVMe SSD) and L3 (object-store) tiers not present.
- **Incremental checkpoint missing**: `forst-rs-engine::checkpoint.rs` does full snapshot only.
- **No Delta Join CDC FFI** (changelog subscription).
- **No iterator / snapshot / async FFI** exports (G-A semantic gaps).
- **No SIMD module** with runtime CPU detection (v3.2 §R4 requirement).
- **Bench layer** has 4 benches but **no baseline comparisons** (vs RocksDB v8.11.3, vs original ForSt) — A1 §4.3 gates baseline build to C5.

### 5.3 Stage 3 (dual-repo integration skeleton): 🟡 Partial-strong (MVP done 2026-05-08)

> **Updated 2026-05-08**: Phase-A Flink bootstrap landed per `docs/superpowers/planning/v3.2/plans/2026-05-08-phase-a-flink-bootstrap.md`. Stage 3 advanced from ❌ Not Done to 🟡 Partial-strong via L1+L2+L3 MVP.

Evidence of progress (Flink-side MVP):
1. **`flink-statebackend-forst-rs` Maven module created** — registered in `flink-state-backends/pom.xml`, parent `flink-state-backends 2.2.0`, JDK 25 compile target.
2. **JDK 25 FFM bridge functional** — `ForStRsLinker` loads `libforst_rs_ffi.dylib` via `SymbolLookup.libraryLookup`, binds 7 downcall handles (`frs_db_open_memory`, `frs_db_close`, `frs_db_default_cf`, `frs_cf_close`, `frs_put`, `frs_get`, `frs_bytes_free`).
3. **Integration test green** — `ForStRsRoundTripTest.putGetRoundTrip` passes: opens in-memory ForSt-RS via FFM, asserts get returns null on absent key, asserts put/get round-trip on `"hello"` → `"world"`. Output: `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`.
4. **`ForStRsStateBackend` skeleton SPI-registered** — `META-INF/services/org.apache.flink.runtime.state.StateBackendFactory` lists `ForStRsStateBackendFactory`; class implements `StateBackend` interface with stub methods throwing `UnsupportedOperationException` (deliberate pre-condition for L4/L5).
5. **Status code translation** — `FrsStatus` enum mirrors Rust's 15 `FRS_STATUS_*` codes; `FrsBackendException` wraps non-OK returns with status preserved.
6. **Opaque handle lifecycle** — `FrsDb` and `FrsCfHandle` use try-with-resources to guarantee `frs_db_close`/`frs_cf_close` execute on scope exit.
7. **Cdylib symbols verified** — all 7 MVP functions present in `nm -gU libforst_rs_ffi.dylib`.

Open work (deferred to Phase D L4/L5 per `B1_pr_split_plan.md` §6.14–§6.15):
- 5 state types implementation (Value/List/Map/Reducing/Aggregating)
- Async v2 + Sync v1 dual-track API
- Checkpoint integration + Rescaling support
- Mini-cluster regression tests
- Cross-platform native lib bundling (currently macOS-absolute path in surefire)
- Old `flink-statebackend-forst` still loads `com.ververica:forstjni:0.1.8` — staying on JNI for backwards-compat per §8.3 (b) FFM-first decision (existing module is independently deprecable later).
- `ForStRsLookupKv` Java interface + Delta Join adapter — Phase D L6 (G-C).

---

## 6. Refactor Strategy Recommendation

### 6.1 Strategy: incremental cleanup, NOT full rewrite

The C1–C9 manifest IS the incremental refactor plan, with §3 arch-pivot escape valve. v3.2's instinct is to "auto-generate PRs from real code" (§8.1) — and that auto-generation already happened on 2026-04-25. **No re-generation.** Phase B becomes a thin pointer to `COMMIT_MANIFEST.md` plus extensions:

- **C10 (new)**: Delta Join Rust-side support — `frs_changelog_subscribe`, `frs_changelog_poll`, async FFI variants (`frs_get_async`, `frs_batch_get_async`), iterator + snapshot FFI exports. Roughly equivalent in scope to C9.
- **L1–L5 (new, paired Flink-side)**: Java module bootstrap → FFM Linker → `ForStRsStateBackend` → Async KeyedStateBackend → Sync KeyedStateBackend. (Names suggestive — final names auto-pick during Phase D execution.)
- **L6 (new)**: `AsyncTableFunction<RowData>` adapter + SQL planner glue for Delta Join (`table.exec.delta-join.kv-backend = forst-rs`).

### 6.2 Three-goal priority order: G-A → G-B+G-C parallel

- **G-A** (compat) is the foundation; C1–C9 already targets it. Ship G-A through Phase G first.
- **G-B and G-C share the Java-side FFM substrate**, so L1–L5 (G-B Java side) and L6 (G-C Java side) are best worked in parallel after C9 lands. The Rust side (C10) can begin once C9 stabilizes.
- v3.2's hierarchy diagram (§2.3) shows G-C depends on G-B's KV exposure; this remains true. But the diagram's "G-A is base" reading shouldn't slip into "ship G-A to production before G-B/G-C start" — the L-series can prepare in parallel.

### 6.3 Cross-repo coordination strategy

- **Adopt the `Cn ↔ Lk` pairing convention**: Cn is ForSt-RS-side, Lk is Flink-side; pairs reviewed together (10 dim each side); merge order ForSt → Flink (provider before consumer).
- **Cross-repo integration CI** (v3.2 Phase F delta): a new workflow `integration-ci.yml` that triggers when paired Cn + Lk both labeled `ready-for-integration`; checks out both repos, builds `libforst_rs.so` from ForSt, copies into Flink lib, runs `mvn -pl flink-state-backends/flink-statebackend-forst-rs verify`.
- **Worktree layout**: per v3.2 §3.3, but with the corrected branch name `forst-rs-jdk25`. Backup-push authorization: ✅ recommended one-time auth in Phase C entry.

### 6.4 Reviewer + termination rules: keep the existing protocol

Per `A1_reconciliation.md §2.3` (which explicitly rejected the v2/v3.2-equivalent rules):
- **10 fixed reviewer dimensions** (Memory safety / Correctness / Concurrency / Test coverage / Error handling / Performance / Documentation / Idiomatic Rust / Security / Integration) — NOT v3.2's 7-default with 12-escalation.
- **Termination = `H=0 ∧ M=0 × 10 consecutive` OR `120-round cap`** — NOT v3.2's "10 rounds zero-High / 100-cap".
- **§3 arch-pivot sub-loop** stays operative — counter resets on every pivot per `A1_reconciliation.md:91-94`.

### 6.5 Phase F / G / H reuse

- **Phase F**: ✅ done by G1 + session E (2026-05-02). v3.2 Phase F adds **only** `integration-ci.yml` + Delta Join E2E lane in `ci-e2e.yml`.
- **Phase G**: micro-bench layer exists (4 benches in `forst-rs-bench/`); needs baseline build (RocksDB v8.11.3 + original ForSt) — gated on C5 per A1 §4.3. Module-integration tier per v3.2 §13.2 needs new benches for `forst-arrow`, `forst-simd`, `forst-cache`, `forst-remote`, `Java FFM (JMH)`, `ForStRsLookupKv vs Fluss`.
- **Phase H**: Nexmark Lane A done via `nexmark-baseline.yml` (smoke build); Lane B (full-scale baseline) is hardware-bound, `baselines_built: false` per `SESSION_HANDOFF.md:77`. **Adds** Delta Join dedicated E2E + 72h soak (new v3.2 obligations).

---

## 7. Reconciliation with Existing Authority — v3.2 prompt deltas to absorb

| v3.2 §  | v3.2 says | Existing authority says | Reconciled |
|---|---|---|---|
| §0.1 | Read every doc under `~/code/stczwd/forst/docs/` | Path is `/Users/lijunqing/Code/stczwd/ForSt/docs/` (case + macOS) | ✅ Read; this section §1 |
| §2.1 | Flink branch `forst-rd-jdk25` | Actual: `forst-rs-jdk25` | Use `forst-rs-jdk25` |
| §2.5 / §R14 | Delta Join replaces Fluss client | This Flink fork has no Fluss; G-C is `AsyncTableFunction<RowData>` adapter | Re-frame; trivial deployment-complexity bar |
| §6 / §8.1 | Phase B auto-generates PR list | Already auto-generated 2026-04-25 = `COMMIT_MANIFEST.md` C1–C9 | Phase B = pointer + new C10 + L-series |
| §11.1 / §11.2 | 7 default reviewers, 12 on escalation | 10 fixed dimensions (`REVIEW_PROTOCOL.md:23-36`) | Keep 10 fixed |
| §11.3 | 100-round cap, 10 rounds zero High | `H=0 ∧ M=0 × 10` OR `120-round cap` (`A1_reconciliation.md §2.3` rejected the looser rule) | Keep stricter rule |
| §12 | Phase F = build all GHA workflows | 7 single-repo workflows already green; G1 done | Phase F = delta only (cross-repo CI + Delta Join lane) |
| §14.1 | Inline `git clone nexmark.git` | Vendored as submodule at SHA `6b3646c`; `nexmark-baseline.yml` exists | Use existing submodule + USER_RUNBOOK |
| §13 / §14 | Performance perf bars per metric | `A1_reconciliation §1` per-component ≥3× vs RocksDB; ≥30–40% Nexmark; harness `exit 1` on miss | Use stricter A1 bars; v3.2 metric list complements |
| Reports path | `reports/A1_status_assessment.md` | User-chosen: `docs/superpowers/planning/v3.2/reports/` | This file lives at chosen path |

### 7.1 Untracked stragglers cleanup

ForSt root has untracked `findings.md`, `progress.md`, `task_plan.md` — `planning-with-files` skill leftovers from the 2026-04-30 audit that fed into A1_reconciliation.md. **Recommendation**: add to `.gitignore` and `git rm` in a `chore: clean up planning-with-files session leftovers` commit. Their content is stale (pre-A1) and creates confusion about authority. Out of scope for this A1; flag for cleanup task.

---

## 8. Items Awaiting User Confirmation (Phase A STOP)

Phase A is interactive per v3.2 §0.3. Before entering Phase B, please confirm:

1. **Authority hierarchy**: this A1 layers G-A/G-B/G-C analysis above `.planning/refactor-review/A1_reconciliation.md @ 33f85b1c5`. Where conflicts exist, A1_reconciliation wins, this A1 records deltas. ✓ Confirm?

2. **Phase B output**: skip auto-generation. Phase B becomes a thin pointer file `docs/superpowers/planning/v3.2/reports/B1_pr_split_plan.md` referencing `COMMIT_MANIFEST.md` C1–C9 + new C10 (Delta Join Rust side) + new L1–L6 (Flink-side paired PRs for G-B + G-C). ✓ Confirm?

3. **G-A compat strategy** (open in `A1_reconciliation §3`): which path?
   - (a) JNI-shaped shim layer preserving `org.forstdb.*` binary compat for existing Flink-state-forst users (requires writing a JNI bridge that translates to FFM internally — complex)
   - (b) FFM rewrite — rewrite `flink-statebackend-forst` callers to use FFM directly via the new `flink-statebackend-forst-rs` module (cleaner; but couples G-A migration to G-B; existing user jobs would need to switch StateBackend factory — a config change, not code change)
   - Default per docs/2.5 D-02-02: **(b) FFM-first**. ✓ Confirm (b) or pick (a)?

4. **Reviewer/termination protocol**: keep `REVIEW_PROTOCOL.md` 10 fixed dimensions + `H=0 ∧ M=0 × 10` / 120-cap. Reject v3.2's looser rules per `A1_reconciliation §2.3`. ✓ Confirm?

5. **Delta Join framing correction**: G-C is "implement `AsyncTableFunction<RowData>` adapter backed by `ForStRsLookupKv`" (no Fluss in this Flink fork to replace). Fluss-baseline benchmark is OPTIONAL — requires standing up Fluss separately for the comparison number. ✓ Confirm framing? **Decide: do we run Fluss-baseline benchmarks at all, or accept "deployment-complexity bar trivially satisfied" without the perf-comparison number?**

6. **Untracked stragglers** (`findings.md`, `progress.md`, `task_plan.md` at repo root): defer cleanup or do it now? Recommendation: defer to a chore commit in Phase D between C-rounds.

7. **C10 + L1–L6 scope**: rough size estimate is ~7 paired PRs (~1 Rust + 6 Flink). Acceptable scope expansion vs the "9-commit C1–C9 frozen vocabulary" of A1_reconciliation §2.1? (A1_reconciliation §2.1 explicitly preserves C1–C9 as frozen — extending to C10 + L-series is a vocabulary addition, not a rename.)

---

## 9. Recommendation: enter Phase B?

**Yes, conditionally.** All Stage-1 prerequisites are met. Stage 2 is mid-flight via the C1–C9 sweep — Phase D execution will continue from `SESSION_HANDOFF.md` `phase: B_R3_READY` state and converge under `REVIEW_PROTOCOL.md`. Stage 3 (Flink-side integration skeleton) is the largest greenfield surface and will be addressed via the proposed L1–L6 paired-PR series.

**Conditions**:
- Items 1–7 in §8 confirmed by user.
- The **2 architectural Histogram H findings** deferred from C1 R2 (`SESSION_HANDOFF.md:82-95`, captured in the empty `C1-arch-pivot-log.md`) need a focused arch-pivot session before C1 can reach `consecutive_clean = 10`. This is independent of v3.2 Phase B entry.

**Recommended next action upon confirmation**: write `B1_pr_split_plan.md` under `docs/superpowers/planning/v3.2/reports/` as the thin pointer, then auto-advance per v3.2 §0.3 autonomous mode through Phase C / D / E until the next interactive milestone (Phase F).

---

## 10. Decisions Recorded (2026-05-08)

User confirmed Phase A finalization with recommended defaults via `/superpowers:using-superpowers` command "Please complete all incomplete parts in phase A, using the recommended method". The 7 items in §8 resolve as:

| # | Item | Decision |
|---|---|---|
| §8.1 | Authority hierarchy | ✅ **Confirmed.** This A1 layers G-A/G-B/G-C deltas above `.planning/refactor-review/A1_reconciliation.md @ 33f85b1c5`. Conflicts: A1_reconciliation wins. |
| §8.2 | Phase B output as pointer | ✅ **Confirmed.** B1 will be a thin pointer to `COMMIT_MANIFEST.md` C1–C9 + new C10 + L1–L6 spec. No regeneration of the existing manifest. |
| §8.3 | G-A compat strategy | ✅ **(b) FFM-first**. Per docs/2.5 D-02-02 ratified gate. Existing user jobs migrate via StateBackend factory config switch (a config change, not code change). The `flink-statebackend-forst-rs` module is the G-A target as well as G-B target — same module, different feature flags / API surfaces. Implies: existing `flink-statebackend-forst` module stays on JNI for backwards-compat (no edits) and is independently deprecable later. |
| §8.4 | Reviewer/termination protocol | ✅ **Confirmed.** Keep `REVIEW_PROTOCOL.md` 10 fixed dimensions + `H=0 ∧ M=0 × 10 / 120-cap`. v3.2's looser rules formally rejected per `A1_reconciliation §2.3`. Recorded in SESSION_HANDOFF v3.2 wrap banner. |
| §8.5 | G-C framing + Fluss-baseline | ✅ **Framing accepted.** G-C is "implement `AsyncTableFunction<RowData>` adapter backed by `ForStRsLookupKv`". **Fluss-baseline benchmarks: DEFERRED** — running them requires standing up a separate Fluss cluster on test hardware. Phase H §14.3 deployment-complexity bar trivially satisfied (no Fluss in deployment). Perf-comparison number is OPTIONAL; if needed later, a follow-up `H_deltajoin_vs_fluss.md` can produce it. |
| §8.6 | Stragglers cleanup | ✅ **Done now** (per user instruction "modify the code"). `findings.md`, `progress.md`, `task_plan.md` removed; `.gitignore` updated to prevent recurrence. |
| §8.7 | C10 + L1–L6 vocabulary extension | ✅ **Approved.** Extension (not rename) preserves A1_reconciliation §2.1 path-stability. Final names will be auto-picked during Phase D execution; suggestive names locked here for planning. |

### 10.1 Housekeeping landed in Phase A finalize

- `findings.md`, `progress.md`, `task_plan.md` deleted; `.gitignore` updated to ignore future planning-with-files leftovers (these are session-local; authoritative planning lives elsewhere).
- `docs/design/2.11_rust_crate_structure.md` got a STATUS DRIFT banner reconciling planned `forst-rs-bridge` → on-disk `forst-rs-ffi` and absent `forst-rs-api` (consolidated into `forst-rs-engine`).
- `.planning/refactor-review/SESSION_HANDOFF.md` got a v3.2 wrap layer pointer at the top, preserving SESSION_HANDOFF as STATE source-of-truth.

### 10.2 Open items NOT closed by Phase A (deferred to Phase D entry)

- **2 architectural Histogram H findings** from C1 R2 (`SESSION_HANDOFF.md:82-95`): these need a focused arch-pivot session before C1 can reach `consecutive_clean = 10`. Independent of v3.2 Phase B entry; blocks C1 termination but not Phase B planning.
- **R-sweep ↔ C-round track reconciliation**: R5–R13 commits are visible in git log post-R2 close. Are they R3+ executed-without-handoff-update, OR a parallel security-sweep track? Recommend confirming when Phase D resumes; not a Phase B blocker.
- **Baseline build for C5 perf gate**: `baselines_built: false` per `SESSION_HANDOFF.md:77`; A1_reconciliation §4.3 gates C5 entry on local RocksDB v8.11.3 baseline. This is hardware-time work, deferred to Phase D entry around C5.

### 10.3 v3.2-prompt corrections recorded for downstream use

- Flink branch: `forst-rd-jdk25` → `forst-rs-jdk25`
- Apache Flink upstream branch: `release-2.2.0` → `release-2.2`
- G-C framing: "replace Fluss client" → "implement `AsyncTableFunction<RowData>` adapter" (this Flink fork has zero Fluss references in source)
- Phase F: existing 7 GHA workflows already green; v3.2 Phase F is delta-only (cross-repo CI + Delta Join E2E lane)
- Reviewer pool: 7→12 escalation rejected; 10 fixed dimensions canonical
- Termination: "10 rounds zero-H / 100-cap" rejected; `H=0 ∧ M=0 × 10 / 120-cap` canonical

These should be cited in B1 (Phase B) so downstream phases inherit the corrections.

---

**End A1 (finalized 2026-05-08).** All §8 items resolved; housekeeping landed. Ready to enter Phase B.
