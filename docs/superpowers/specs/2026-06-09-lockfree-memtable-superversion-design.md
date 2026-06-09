# Lock-free memtable pointers (superversion) — the join read-path lever (q7/q9/q20)

**Date:** 2026-06-09
**Status:** Approved (implement + verify in one pass).
**Scope:** forst-rs engine (`crates/forst-rs-engine` — `column_family.rs`, callers in `db.rs`).

## Root cause (profiler-pinned, 2026-06-09)
The join slowness is NOT the LSM scan and NOT the value drain — both are cheap. Profiling refuted
each earlier hypothesis:
- `get_arc` drain → unified to value-carrying; **no improvement** (drain wasn't it).
- 16-shard memtable scan → **`n_shards=1`** here; `CURSOR_SUBCAUSE` shows the cursor is **~1–3µs**
  (lock_ns 42–125, scan_ns ~1.2µs, construct ~0).
- The `ITER_DIAG active_us`≈**1000µs** is **cumulative from function entry** and sits **outside** the
  cursor — it's the **memtable-pointer access**: `active_memtable()` = `self.active_memtable.read()`
  and `imm_memtables()` = `self.imm_list.read()` (column_family.rs:540/545) take **RwLock READ locks
  per probe**. A **memtable switch** (frequent under q9 ingest + WBM force-switch @256MiB) takes the
  **WRITE lock**, so every concurrent read probe **blocks ~1ms behind the switch**. Reads serialize
  behind switches.

This explains why parallel reads + value-carrying didn't help: more reader threads all block on the
same switch write-lock.

## Why RocksDB/ForSt don't hit this
They use a **lock-free superversion**: active memtable + immutable list + SST version are one
immutable bundle behind an atomic pointer. Readers grab it lock-free (atomic load + thread-local
refcount); a switch installs a **new** superversion via an **atomic swap**. **Readers never block on a
switch.** forst-rs guards the memtable pointers with a plain `RwLock` → reads block during a switch.

## Design: `ArcSwap<MemtableSet>` (mirror the existing `sst_readers` ArcSwap pattern)
Bundle the active memtable + immutable list into ONE immutable struct behind ONE `ArcSwap`, so a
reader always loads a **consistent** (active, imms) snapshot and never blocks:

```rust
struct MemtableSet { active: SharedMemTable, imms: Arc<Vec<SharedMemTable>> }
// ColumnFamilyData:
memtables: ArcSwap<MemtableSet>   // replaces  active_memtable: RwLock<..> + imm_list: RwLock<..>
```
- **Reads** (`active_memtable()`, `imm_memtables()`): `memtables.load()` → return `.active.clone()` /
  `.imms.clone()`. Lock-free; never blocks on a switch.
- **Switch** (freeze active → push to imms → new active): build a new `MemtableSet` and
  `memtables.rcu(|cur| …)` (atomic CAS). One swap = readers see old-or-new, never a torn mid-switch
  state (the consistency the two-separate-locks design couldn't guarantee).
- **pop_oldest_imm / add-imm / flush install**: same `rcu` clone-mutate-publish.

Single-writer note: writes (puts) into the *active* memtable's shards keep their own per-shard locks
(unchanged) — only the *pointer* to which memtable is active becomes lock-free. The active memtable
object is the same `Arc`; an in-flight put and a concurrent reader of the same active memtable are
already safe (the memtable is internally synchronized). The switch swaps the *pointer*, not the
memtable's internals.

## Correctness (GATE)
- A reader loading the set mid-switch sees either the pre-switch (active=A, imms=[…]) or post-switch
  (active=A', imms=[A,…]) — both contain A's keys (A is either active or the newest imm), so no key is
  ever invisible. (The two-RwLock design avoided this only by blocking the reader.)
- All existing engine UTs green (memtable switch/flush/recovery/iteration). Add a UT: concurrent
  prefix reads during a switch never miss a key and never block measurably.
- Byte-identical query results; no config change.

## Verification (before/after, recorded in 2026-06-08-8c32g-3backend-sweep-results.md)
- q9 flag-ON before/after (the ~1ms `active_us` stalls should vanish; throughput should stop
  oscillating/collapsing). Then q7/q20.
- Regression-check the passing set (q16/q17/q18 + the iterator queries q3/q11/q12/q15/q19).
- Both repos' GHA green.

## Note on the parallel-iterator work (A+B+C, flag-gated, default OFF)
Kept default-OFF. Once memtable reads are lock-free, re-evaluate whether parallel iterator dispatch
helps (it couldn't before because every reader blocked on the switch lock). May become a real win, or
may stay off if the per-probe engine work is already µs-cheap (then the residual gap is async-state/FFM
framework overhead, a separate lever).
