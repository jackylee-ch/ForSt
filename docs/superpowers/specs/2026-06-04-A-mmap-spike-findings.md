# A (mmap) spike findings + PMC recommendation

**Date:** 2026-06-04
**Status:** SPIKE DONE (throwaway microbench, `/tmp/mmap-spike`). Verdict measured against the
pre-committed gate from `2026-06-04-C-kv-block-format-vnext.md` §5b. Decision: see §4.

## 1. What was measured

After C landed, `read_at` (`LocalFirstSstFile::read_at` → std `FileExt::read_at`) + `_platform_memmove`
became the unambiguous #1 SST-read-path cost (decode side now 0, crc 0). A microbench mirroring q4's
access pattern — scattered random **8 KiB** block reads over a **512 MiB** local file, **WARM** (page-cache
resident) — compared four strategies (same offsets, 2M reads, 2 runs, stable):

| strategy | ns/read | page faults |
|---|---|---|
| (a) pread into a FRESH `vec![0u8;blk]` each read (**current path**) | ~865 | maj 0 |
| (b) pread into a REUSED buffer | ~745 | maj 0 |
| (c) **mmap, reference the slice — no copy** (what A does) | **~4.5** | maj 0 |
| (d) mmap + explicit copy into a buffer | ~378 | maj 0 |

## 2. Decomposition (warm)

- **(a)−(b) ≈ 85–170 ns/read = alloc + zero-fill** of the per-read `vec![0u8;blk]`. **Recoverable NOW with
  zero mmap, zero unsafe, no policy line** — just reuse the read buffer. A free pre-A win (caveat: the v1
  zero-copy path moves the Vec into an Arrow `Buffer`, so reuse there needs care; the KV path decompresses
  into its own buffer).
- **(b)−(d) ≈ 350–400 ns/read = the pread SYSCALL overhead** (kernel transition per read). (b) pays it, (d)
  doesn't — both copy 8 KiB. mmap removes this regardless of copy.
- **(d) ≈ 360–390 ns/read ≈ the 8 KiB `memcpy`** itself (mmap+copy, minus the ~4.5 ns access).
- **(c) ≈ 4.5 ns = mmap zero-copy:** removes BOTH the syscall AND the copy. ~165× faster than current (a),
  in the warm regime.

So warm `read_at` ≈ **syscall (~375 ns) + copy (~375 ns)**. mmap-no-copy removes both; mmap+copy removes
only the syscall.

## 3. The caveat the gate locked (and the spike's limit)

mmap removes the syscall + copy, NOT the **page-in**. This microbench is **WARM** with **zero major
faults** — it is the UPPER BOUND on mmap's win. q4's real regime is mixed: `read_at` fires only on a
decoded-block-cache MISS, and for scattered interval-join probes re-reading older SSTs the underlying raw
block may be **cold** in the OS page cache. In a cold read, mmap-no-copy still incurs the **major fault /
disk page-in that pread also pays** — recovering only the copy, not the (now-dominant) fault. **The cold
regime is UNMEASURED here** (reliable cache-drop needs `sudo purge` / Linux `fadvise(DONTNEED)`); it is the
key residual risk and must be priced before committing to A.

## 4. Verdict against the pre-committed gate

- **"How much actually lands":** WARM = the entire `read_at` (~750 ns/read) if the block can be used
  zero-copy, or ~half (syscall only, ~375 ns) if it must be copied out. Either way **large** — A clears
  the warm bar decisively.
- **"Whether faults appear":** WARM = none. COLD = they would (the page-in mmap cannot remove) — unmeasured.
- **Decision:** A **clears the warm bar → take to the PMC as its OWN design-doc item**, carrying three
  explicit conditions: (1) the win is warm-regime-bounded; cold-regime payoff is capped by fault cost and
  is unmeasured (the real risk); (2) the unsafe lives in `memmap2` (skirts `forbid(unsafe_code)`'s spirit)
  — a policy call; (3) a **free, policy-clean pre-A win exists** — buffer reuse removes the alloc+zero-fill
  (~85–170 ns/read, the (a)−(b) gap) with no mmap. **Do NOT implement A on the main line.** If the PMC
  wants A, gate it on a cold-regime re-measure (instrument q4: fraction of `read_at`s that major-fault).

## 4b. q4 `read_at` fault-rate MEASURED (2026-06-04, `FRS_READ_AT_DIAG=1`) — turns A's ceiling into a payoff

Instrumented `LocalFirstSstFile::read_at` latency histogram over a full q4 run (33.5M reads, 22 dumps,
KV+B1 config). **cold (≥20µs, i.e. likely a major fault / disk page-in) = 0.4–0.6%, ROCK-STABLE from the
first 1M reads through 33.5M as the LSM fills.** ~75% of reads are <2µs, ~97% <5µs, ~99.5% <20µs; mean
~2.2µs. **q4's read path is ~99.5% page-cache WARM — the cold regime that would defeat mmap does NOT
materialize** (the local-NVMe write-through SST cache + OS page cache keep blocks resident even under
scattered interval-join re-probes). So A is **NOT fault-capped for q4**; mmap's warm win applies.

**BUT mmap is not q4's biggest read lever.** Real `read_at` mean (~2200 ns) ≫ the spike's raw warm pread
(~745 ns). `get_range` (`local_cache.rs:504`) explains the ~1450 ns gap: per read it (1) takes a **mutex**
(membership check + LRU touch), (2) does its OWN `vec![0u8;len]` **alloc + zero-fill**, (3) preads into
that buffer, (4) returns a `Vec`, and then `read_at` does a **second `copy_from_slice`** into the caller's
buffer. So one logical block read = **lock + 2 allocs + 2 copies + 1 pread**.

- **A (mmap) addresses ~the pread + one copy ≈ ~745 ns of ~2200 ns ≈ ~34% of read_at** (and 0.5% of those
  are cold, where mmap saves only the copy). Real but bounded.
- **~66% of read_at is the LocalFirstSstFile WRAPPER** (mutex LRU touch + redundant get_range alloc/
  zero-fill + the double copy) — **removable with NO unsafe**: have `get_range` pread directly into the
  caller's buffer (one pread, zero intermediate `Vec`, zero second copy), and reduce the per-read lock.
  **This is the bigger, policy-clean lever and should precede any unsafe-policy spend on A.** (My banked
  buffer-reuse removed one of the two allocs — the reader-side scratch — but get_range's alloc+copy
  remain.)

**Revised A verdict:** A is viable for q4 (warm, not fault-capped) but recovers only ~1/3 of read_at; the
policy-clean get_range double-copy/lock elimination is the larger win. Sequence: bank buffer-reuse (done) →
eliminate get_range's redundant alloc + second copy (no unsafe) → re-profile → **re-price A on the residual
bare-pread syscall** before taking it to the PMC. A's unsafe-policy capital is only worth spending on what
remains after the free wins.

## 4c. get_range double-copy/alloc REMOVED + A re-priced via Amdahl (2026-06-04)

Landed `LocalCache::get_range_into` (preads DIRECTLY into the caller's buffer — no intermediate `Vec`,
no second `copy_from_slice`); `read_at` and `get_range` both route through it. **Correctness-gated at the
concurrency level** (the escalation): 16-thread shared-fd hammer (no torn reads) + 12-thread one-reader
get/scan under BOTH formats (the scratch→Arrow-Buffer path), plus storage 352 + engine 262 + ffi 96 + the
ground-truth test, all green under both v1 and v2-forced.

**Measured (q4, KV+B1, `FRS_READ_AT_DIAG`, steady-state INCREMENTAL mean — cumulative `mean×reads` deltas
between consecutive dumps, which wash out the early cold-start tail):**
- **before** (no get_range_into): ~**1650–1690 ns/read**.
- **after** (get_range_into): ~**1425–1500 ns/read** (cumulative still falling through 1720 ns at 54M reads).
- ⇒ **~12–14% off `read_at`**, net of equal diag overhead. The `<1µs` bucket share ~10×'d (warm reads got
  materially faster) — direct evidence the removed copy/alloc was real work. Cold stayed ~0.3–0.5%.

**Amdahl re-pricing of A on the residual ~1450 ns warm `read_at`:** mmap removes the pread syscall (~375 ns)
+ the pread's single disk→buf copy (~375 ns) ≈ **~745 ns ≈ ~50% of the now-residual read_at**. The other
~700 ns is the per-read mutex LRU-touch + fd-cache lookup + bookkeeping (policy-clean to attack; NOT mmap's).
But `read_at` is only ONE component of q4's total CPU (join operator, watermark drain, memtable RMW, etc.),
so A's *total-q4* impact is single-digit-%, behind the `forbid(unsafe_code)` line, with a cold-regime that's
negligible here (0.3%) — i.e. mmap would land its warm win, but on a shrinking slice. **Verdict: A is now
too thin to justify the unsafe fight** (matches the prediction). If `read_at` needs more, the next
policy-clean lever is the per-read LRU-touch lock (concurrency-gated), not mmap. A stays a documented PMC
option, not a recommended build.

## 5. Cheaper-than-A follow-ups surfaced (no unsafe)
- **Buffer reuse** on the read path: recover the (a)−(b) alloc+zero-fill (~10–20% of `read_at`) now.
- **Bigger blocks / fewer reads:** the per-read syscall is ~375 ns warm; `read_at` count ∝ block fan-out ÷
  block size. Larger blocks amortise the syscall (already a lever: `FRS_BLOCK_SIZE_KB`). C's prefix
  compression also shrinks bytes-copied per block.
- These shrink `read_at` without touching the unsafe-policy line — worth weighing before the mmap fight.
