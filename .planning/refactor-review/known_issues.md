# ForSt-RS — Known Issues

Tracked residual issues that were either deferred during a refactor-review
round or surfaced by CI but not yet addressed. Each entry has a priority
(P0/P1/P2/P3) and an owning lane.

**Authoritative spec:** `.planning/refactor-review/A1_reconciliation.md` @ `33f85b1c5`.

---

## ✅ RESOLVED — Rustdoc strict mode re-enabled (2026-05-02)

All 14 residuals were fixed across `forst-rs-engine` and `forst-rs-storage` and `RUSTDOCFLAGS=-D warnings` was re-enabled in `.github/workflows/ci-rust.yml` `doc:` job. Verified locally with `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace --document-private-items` (exit 0).

Fix patterns applied:
- Same-impl method references → `[Self::method]` (write_controller.rs ×3)
- Cross-crate types → fully-qualified `[crate_name::module::Type]` (storage types referenced from engine)
- Same-crate cross-module → `[crate::module::Type]` (sst writer/reader, cache submodules, FileDeletionGuard)
- Nonexistent / private items → plain code-span (no link) or replaced with the actual existing item (e.g. `try_acquire_delete` → `can_delete`)

---

## ~~P2 — Rustdoc strict mode disabled (CI soft-warn)~~ (historical record below)

**Location:** `.github/workflows/ci-rust.yml` `doc:` job — `RUSTDOCFLAGS: -D warnings` is **commented out**, so the job runs as soft-warn.

**Why:** Initial CI run on commit `5e7550371` (2026-04-30) surfaced **~14 broken intra-doc links** across the crates listed below. These are real code-quality bugs in doc comments (mostly missing module qualification on `[`Symbol`]` references), but fixing them all is bulk dev work that belongs in C1 R2 review per `REVIEW_PROTOCOL.md` Dimension 7 (Documentation).

**Already fixed in `5e7550371` (forst-rs-io only)**:
- `crates/forst-rs-io/src/object_store.rs:200-207` — multipart trait method links → `Self::method` form
- `crates/forst-rs-io/src/object_store.rs:489` — `[FileSystemRouter]` → `[crate::router::FileSystemRouter]`
- `crates/forst-rs-io/src/object_store.rs:558` — `[MemoryFileSystem]` → `[crate::memory_fs::MemoryFileSystem]`
- `crates/forst-rs-io/src/router.rs:275` — `[FileSystemRouter::is_remote_file]` (private) → plain code-span (no link)

**Remaining (14 errors, deferred to C1 R2):**

| File | Line | Broken link | Suggested fix |
|---|---|---|---|
| `crates/forst-rs-engine/src/checkpoint.rs` | 30 | `[VersionSetImpl]` | `[crate::version_set::VersionSetImpl]` or correct module path |
| `crates/forst-rs-engine/src/column_family.rs` | 293 | `[DbImpl::flush_cf_data]` | `[crate::DbImpl::flush_cf_data]` or `Self::flush_cf_data` |
| `crates/forst-rs-engine/src/compaction.rs` | 23 | `[DbImpl::compact_l0]` | `[crate::DbImpl::compact_l0]` |
| `crates/forst-rs-engine/src/compaction.rs` | 58 | `[VersionSet]` | `[crate::version_set::VersionSet]` |
| `crates/forst-rs-engine/src/compaction.rs` | 367 | `[VectorizedMemTable]` | full module qualifier |
| `crates/forst-rs-engine/src/file_deletion_guard.rs` | 28 | `[FileDeletionGuard::try_acquire_delete]` | `[Self::try_acquire_delete]` |
| `crates/forst-rs-engine/src/flush.rs` | 18 | `[VectorizedMemTable]` | full module qualifier |
| `crates/forst-rs-engine/src/write_controller.rs` | 156 | `[may_throttle]` | `[Self::may_throttle]` |
| `crates/forst-rs-engine/src/write_controller.rs` | 157 | `[clear_stall]` | `[Self::clear_stall]` |
| `crates/forst-rs-engine/src/write_controller.rs` | 157 | `[on_flush_complete]` | `[Self::on_flush_complete]` |
| `crates/forst-rs-storage/src/cache/mod.rs` | 26 | `[ShardedClockCache]` | full module qualifier |
| `crates/forst-rs-storage/src/memtable/mod.rs` | 17 | `[MemTable]` | full module qualifier |
| `crates/forst-rs-storage/src/sst/reader.rs` | 17 | `[SstWriterImpl]` | full module qualifier |
| `crates/forst-rs-storage/src/sst/reader.rs` | 278 | `[SstWriterImpl]` | full module qualifier |

**How to clear:** during C1 R2 (or any later Cn round that touches these files), apply the fixes, then uncomment `RUSTDOCFLAGS: -D warnings` in `ci-rust.yml` `doc:` job. Each Cn that closes its rustdoc residuals must verify locally with `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace --document-private-items`.

**Owner:** Coder-Rust during the relevant Cn review (most likely C7 for engine, C3 for storage SST, C4 for memtable, C5 for cache).

---

## (no other open issues currently)

When future issues are deferred from a Cn review or CI run, append entries here with the same shape (priority, location, why-deferred, what-to-fix, owner).
