// Copyright 2026 The ForSt-RS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! §2.1 (streaming-read redesign, 2026-06-11): per-SST-source block
//! prefetcher — the engine's `FilePrefetchBuffer`/auto-readahead equivalent.
//!
//! Each streaming SST tier source (`TierKeySource::Sst` in the engine's
//! `db.rs`) owns one [`BlockPrefetcher`] in place of its raw `next_block`
//! cursor. The state machine mirrors RocksDB's auto-readahead, adapted to the
//! two NEXMark regimes:
//!
//! 1. **Cold** (`blocks_consumed < ramp_after`): demand-fetch exactly one
//!    block, synchronously, exactly like the legacy path (block-cache check
//!    first, `CachePriority::Low` insert). R-short probes (q7-class — most
//!    exhausted in one chunk) never leave this state ⇒ zero speculative I/O.
//! 2. **Ramp** (`blocks_consumed >= ramp_after`): sequentiality is *certain*
//!    inside a source (blocks are consumed in index order), so readahead
//!    starts at `ra_blocks = 2`, doubling each window up to a cap:
//!    - local files (`RandomAccessFile::is_local() == true`): cap 256 KiB
//!      (4 × 64 KiB blocks), ramp after 2 consumed blocks;
//!    - remote/evicted files: cap 4 MiB (64 blocks — a GetObject round-trip
//!      has high fixed cost, so ramped windows go deep), ramp after 2
//!      consumed blocks like local (M2: ramping after the FIRST block made
//!      every 1-block probe over an evicted SST speculate remotely,
//!      violating "R-short never speculates").
//! 3. **Multi-block reads**: one positional read spanning the window's
//!    physically contiguous blocks (the sparse index gives exact offset+size
//!    per block); per-block regions are sliced out of the single buffer.
//! 4. **Double-buffered production**: the window fetch + decompress + decode
//!    runs on a shared read-I/O pool; the consumer claims the completed
//!    `PrefetchHandle` and the NEXT window is submitted before the claimed
//!    blocks are consumed — production of N+1 overlaps consumption of N.
//! 5. **Clamping**: never reads at/past `end_block` (computed once from the
//!    sparse index vs the scan's upper bound) nor past the file. Dropping the
//!    prefetcher (or [`BlockPrefetcher::terminate`]) drops the handle; the
//!    pool job's result is discarded on the dead channel (the window buffer
//!    is job-owned until claimed — no use-after-free surface).
//!
//! **Block-cache interaction (q20 lesson — never bypass):** every fetch,
//! demand or prefetch, checks the decoded-block cache first and SPLITS the
//! I/O window around hits. Demand blocks insert at `Low` (today's behaviour);
//! ramped prefetch inserts at `Bottom` once `ra_blocks >= 4` so deep
//! streaming scans are evicted first and never displace hot point-get blocks.
//! Insertion is never skipped on the SCAN path — interval-join re-probes of
//! recently scanned windows are common and the decoded cache hit is the
//! cheapest read.
//!
//! **Compaction-input mode (L4, 2026-06-12 windowed-readpath design):**
//! [`BlockPrefetcher::for_compaction`] reuses the same state machine / pool /
//! cancellation with three policy differences:
//! 1. full-file scan `[0, n_blocks)` with a FIXED window from the start (no
//!    cold state, no ramp — sequentiality is known a priori; the engine
//!    clamps the window size against the fan-in budget);
//! 2. `CacheFillPolicy::Skip`: cache-first READ stays (recently-flushed
//!    blocks are served for free — the 27ae792c3 lesson), but compaction
//!    input blocks are NEVER inserted — each input block is read exactly
//!    once and future reads hit the compaction *output*, so inserting (even
//!    at `Bottom`) only evicts the foreground's hot set;
//! 3. the demand fallback inside `next_decoded` also runs with Skip.

use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use forst_rs_common::{ForstError, ForstResult};

use crate::cache::CachePriority;
use crate::sst::reader::{DecodedBlock, SstReaderImpl};

/// Local-file readahead window cap: 256 KiB (preads are µs-class; deeper
/// windows only add memory).
const LOCAL_CAP_BYTES: u64 = 256 * 1024;
/// Remote-file readahead window cap: 4 MiB (round-trips are ms-class and a
/// GetObject has high fixed cost).
const REMOTE_CAP_BYTES: u64 = 4 * 1024 * 1024;
/// `ra_blocks` hard caps (assuming the default 64 KiB block size; windows are
/// additionally byte-trimmed against the caps above for other block sizes).
const LOCAL_CAP_BLOCKS: u32 = 4;
const REMOTE_CAP_BLOCKS: u32 = 64;
/// Sequential blocks consumed before the ramp starts. BOTH regimes require 2
/// consumed blocks (M2, 2026-06-11 PMC review): ramping remote after the
/// FIRST block violated the "R-short never speculates" invariant on the
/// evicted tier — every 1-block probe over an evicted SST issued a
/// speculative 2-block remote read. The remote regime keeps its DEEPER cap
/// (4 MiB / 64 blocks) once sequentiality is actually established.
const LOCAL_RAMP_AFTER: u32 = 2;
const REMOTE_RAMP_AFTER: u32 = 2;
/// Ramped prefetch inserts at `Bottom` once the window is at least this deep.
const BOTTOM_PRIORITY_RA: u32 = 4;
/// H1 (2026-06-11 PMC review): generous upper bound `next_decoded` may wait
/// for an in-flight window before giving up. Pool workers survive job panics
/// (catch_unwind), so this only fires on a queued-but-never-run / wedged job;
/// it converts a would-be infinite hang of the consuming (FFI/task) thread
/// into a `ForstError::TimedOut`. 300s is far above any legitimate window
/// fetch (µs-ms local, ms-class remote) yet below an operator-visible wedge.
const POOL_JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Runtime kill-switch: `FRS_RS_BLOCK_PREFETCH=0|false` disables speculation
/// entirely (every block demand-fetched — the pre-§2.1 behaviour, modulo the
/// result-identical `end_block` clamp). Default ON.
fn prefetch_enabled() -> bool {
    static E: OnceLock<bool> = OnceLock::new();
    *E.get_or_init(|| {
        !matches!(
            std::env::var("FRS_RS_BLOCK_PREFETCH").ok().as_deref(),
            Some("0") | Some("false") | Some("FALSE")
        )
    })
}

// ---------------------------------------------------------------------------
// M3 (2026-06-11 PMC review): aggregate prefetch-memory TELEMETRY (no
// enforcement yet). Tens of concurrently-open sources × (4 MiB ready +
// 4 MiB inflight) is an uncounted ~0.5 GB/iterator worst case on the
// 8c/32g box — this counter makes the budget observable on 100M runs.
// ---------------------------------------------------------------------------

/// Global aggregate of prefetcher-held bytes (on-disk block sizes), covering
/// BOTH claimed-but-undelivered `ready` blocks and submitted in-flight
/// windows, across every live [`BlockPrefetcher`] in the process.
/// Incremented at window submit; balance moves from inflight to ready at
/// claim (no net change); decremented at block delivery, window failure,
/// [`BlockPrefetcher::terminate`], and prefetcher drop.
///
/// SHARDED (2026-06-12 bisect de-contention, M3): the original single global
/// `AtomicUsize` was RMW-ed once per DELIVERED BLOCK on the consumer
/// (Flink task / FFI) threads — 8 task threads hammering one cache line
/// is exactly the cross-core ping-pong shape suspected in the q9@100M
/// r1→r2 regression. Each thread now picks a fixed cache-line-padded
/// shard (round-robin at first touch); adds/subs are `Relaxed` RMWs on
/// the thread's own line, and the telemetry read sums all shards. Shards
/// are SIGNED: a prefetcher charged on thread A may be dropped on thread
/// B (iterator close on another thread), driving B's shard negative —
/// the SUM stays exact.
const PREFETCH_CTR_SHARDS: usize = 16;

/// One cache line per shard — no false sharing between adjacent shards.
#[repr(align(64))]
struct PaddedCounter(std::sync::atomic::AtomicIsize);

static PREFETCH_BUFFERED_BYTES: [PaddedCounter; PREFETCH_CTR_SHARDS] =
    [const { PaddedCounter(std::sync::atomic::AtomicIsize::new(0)) }; PREFETCH_CTR_SHARDS];

/// The calling thread's fixed shard (assigned round-robin on first touch).
fn prefetch_ctr_shard() -> &'static PaddedCounter {
    use std::cell::Cell;
    thread_local! {
        static SHARD_IDX: Cell<usize> = const { Cell::new(usize::MAX) };
    }
    let idx = SHARD_IDX.with(|c| {
        let mut v = c.get();
        if v == usize::MAX {
            static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            v = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % PREFETCH_CTR_SHARDS;
            c.set(v);
        }
        v
    });
    &PREFETCH_BUFFERED_BYTES[idx]
}

/// Charge `n` on-disk bytes to the aggregate (window submit).
fn prefetch_charge_add(n: usize) {
    prefetch_ctr_shard()
        .0
        .fetch_add(n as isize, std::sync::atomic::Ordering::Relaxed);
}

/// Release `n` on-disk bytes from the aggregate (delivery / failure /
/// terminate / drop).
fn prefetch_charge_sub(n: usize) {
    prefetch_ctr_shard()
        .0
        .fetch_sub(n as isize, std::sync::atomic::Ordering::Relaxed);
}

/// Current aggregate of prefetcher ready+inflight bytes (M3 telemetry).
/// Sizes are on-disk (pre-decompression) block bytes — the I/O-side budget.
/// Sum over the shards; individual shards may be transiently negative (see
/// the sharding note above), the sum is clamped at 0.
pub fn prefetch_buffered_bytes() -> usize {
    PREFETCH_BUFFERED_BYTES
        .iter()
        .map(|s| s.0.load(std::sync::atomic::Ordering::Relaxed))
        .sum::<isize>()
        .max(0) as usize
}

/// Diag gate shared with the read-path instrumentation (`FRS_READ_AT_DIAG=1`,
/// cached_fs.rs): when set, each window submit logs the aggregate so 100M
/// runs can watch the prefetch memory budget.
fn prefetch_diag() -> bool {
    static D: OnceLock<bool> = OnceLock::new();
    *D.get_or_init(|| {
        matches!(
            std::env::var("FRS_READ_AT_DIAG").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE")
        )
    })
}

// ---------------------------------------------------------------------------
// Shared read-I/O pool (§2.5 axis 2): `clamp(cores/2, 2, 6)` threads, env
// override `FRS_RS_PREFETCH_THREADS`. Per-SST-source prefetch handles run
// here, so a single long scan gets I/O parallelism = min(#sources, pool) even
// at operator-parallelism 1. std-only (Mutex + Condvar + VecDeque) —
// preserves the crate's `#![forbid(unsafe_code)]`.
// ---------------------------------------------------------------------------

type Job = Box<dyn FnOnce() + Send + 'static>;

struct PoolShared {
    queue: Mutex<VecDeque<Job>>,
    cv: Condvar,
}

struct ReadIoPool {
    shared: Arc<PoolShared>,
}

impl ReadIoPool {
    fn new(n_workers: usize) -> Self {
        let shared = Arc::new(PoolShared {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
        });
        for _ in 0..n_workers.max(1) {
            let sh = Arc::clone(&shared);
            std::thread::Builder::new()
                .name("frs-readahead".to_string())
                .spawn(move || loop {
                    let job = {
                        let mut q = sh.queue.lock().unwrap_or_else(|p| p.into_inner());
                        loop {
                            if let Some(job) = q.pop_front() {
                                break job;
                            }
                            q = sh.cv.wait(q).unwrap_or_else(|p| p.into_inner());
                        }
                    };
                    // H1 (2026-06-11 PMC review): a panicking window job must
                    // not kill the worker — a fixed-size pool with dead
                    // workers eventually strands queued jobs forever, hanging
                    // every consumer blocked in `next_decoded`. The panic is
                    // contained; the job's oneshot `SyncSender` is dropped
                    // during unwind, so the waiting consumer observes a
                    // disconnect and converts it to a `ForstError`.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                })
                .expect("failed to spawn frs-readahead worker");
        }
        Self { shared }
    }

    fn submit(&self, job: Job) {
        let mut q = self.shared.queue.lock().unwrap_or_else(|p| p.into_inner());
        q.push_back(job);
        drop(q);
        self.shared.cv.notify_one();
    }
}

fn read_io_pool() -> &'static ReadIoPool {
    static POOL: OnceLock<ReadIoPool> = OnceLock::new();
    POOL.get_or_init(|| ReadIoPool::new(read_io_pool_width()))
}

/// The configured width (worker count) of the shared read-I/O pool —
/// `FRS_RS_PREFETCH_THREADS` or `clamp(cores/2, 2, 6)`. This is the ceiling on
/// how many of a scan's per-window reads can resolve CONCURRENTLY, so the
/// adaptive scan-readahead depth controller clamps its target depth to this
/// (a depth above the pool width cannot increase real concurrency). Computed
/// from the same inputs the pool is built from, so it matches the live pool
/// regardless of whether the pool has been initialised yet.
pub fn read_io_pool_width() -> usize {
    std::env::var("FRS_RS_PREFETCH_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(|| {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            (cores / 2).clamp(2, 6)
        })
        .max(1)
}

/// FRS-SCAN-OPEN-FANOUT (Phase-2 cycle 3, catalog item #1): submit `jobs` to
/// the shared read-I/O pool concurrently and BLOCK until all of them complete
/// (a barrier). This is the OPEN-side analogue of [`BlockPrefetcher`]'s
/// per-window submit: the scan merge-build site uses it to fan the K cold
/// remote SST-reader OPENs (footer + sparse-index fetch) across the pool
/// instead of opening them one at a time, so the K open round-trips overlap
/// (bounded by the pool width) before the per-source open loop consumes the
/// now-cached readers. Mirrors ForSt's `prefetch_concurrent`
/// (`MAX_CONCURRENT_FETCH=8`, cached_fs.rs) but reuses the existing read-I/O
/// pool rather than spawning a fresh `thread::scope` wave.
///
/// Each job runs on a pool worker (panic-contained — a panicking open does not
/// strand the barrier: the wrapper's `Signal` guard fires during the unwind, so
/// the counter still advances). Empty input is a no-op (zero pool jobs — the
/// "nothing to fan out" fast path).
pub fn prime_opens_concurrent(jobs: Vec<Box<dyn FnOnce() + Send + 'static>>) {
    if jobs.is_empty() {
        return;
    }
    let total = jobs.len();
    // Shared completion counter + condvar — the barrier the caller waits on.
    let done: Arc<(Mutex<usize>, Condvar)> = Arc::new((Mutex::new(0), Condvar::new()));
    let pool = read_io_pool();
    for job in jobs {
        let d = Arc::clone(&done);
        pool.submit(Box::new(move || {
            // Whether the open returns or panics, the `Signal` guard's Drop
            // increments the barrier counter so the wait below can never strand
            // (the worker's `catch_unwind` contains the panic; this guard fires
            // during the unwind before the worker re-arms for the next job).
            struct Signal(Arc<(Mutex<usize>, Condvar)>);
            impl Drop for Signal {
                fn drop(&mut self) {
                    let (m, cv) = &*self.0;
                    *m.lock().unwrap_or_else(|p| p.into_inner()) += 1;
                    cv.notify_all();
                }
            }
            let _signal = Signal(d);
            job();
        }));
    }
    let (m, cv) = &*done;
    let mut g = m.lock().unwrap_or_else(|p| p.into_inner());
    while *g < total {
        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
    }
}

/// FRS-VLOG-SCAN-READAHEAD (Phase-2 cycle 6): submit ONE fire-and-forget job to
/// the shared read-I/O pool and return IMMEDIATELY (the non-blocking,
/// no-barrier sibling of [`prime_opens_concurrent`]). The scan-readahead
/// iterator uses this to launch window `k+1`'s coalesced deref while the consumer
/// drains window `k`; the CONSUMER is the barrier (it joins the job's result
/// channel only when window `k` empties), so no completion counter is needed
/// here. The job runs under the pool's `catch_unwind` (a panic is contained and
/// the job's result channel disconnects, which the consumer converts to an
/// error / cancellation). Use this only when the work owns its own completion
/// signalling (a channel the caller waits on); for "do K opens then continue"
/// use the barriering [`prime_opens_concurrent`].
pub fn submit_read_job(job: Box<dyn FnOnce() + Send + 'static>) {
    read_io_pool().submit(job);
}

/// L4 (2026-06-12 compaction windowed-readpath design §2.1): what
/// [`fetch_window`] does with blocks it had to READ (cache misses).
/// The cache-first check (window splitting around hits) is unconditional —
/// only the INSERT side is a policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CacheFillPolicy {
    /// Scan path (existing behavior): insert every fetched block at the
    /// given priority (`Low` shallow / `Bottom` deep-ramp).
    Insert(CachePriority),
    /// Compaction inputs: read-only cache use (`fill_cache=false`
    /// equivalent). Blocks are decoded and returned but never inserted —
    /// a streaming compaction pass must not evict the foreground hot set
    /// (nor pay per-block insert cost for entries that are dead on arrival).
    Skip,
}

/// L4 W5 telemetry: per-prefetcher counters for compaction-input reads.
/// Summed across a job's cursors by the engine ⇒ per-compaction telemetry
/// (design falsifier 1: ≥90 % of input blocks must arrive via windows).
#[derive(Clone, Copy, Debug, Default)]
pub struct CompactionReadStats {
    /// Blocks delivered to the consumer from prefetched windows.
    pub window_blocks: u64,
    /// Blocks delivered via the (Skip-policy) demand fallback.
    pub demand_blocks: u64,
    /// Windows submitted to the read-I/O pool.
    pub window_submits: u64,
    /// On-disk bytes covered by submitted windows.
    pub prefetched_bytes: u64,
    /// Blocks served from the decoded-block cache (window splitting) instead
    /// of I/O.
    pub cache_hits: u64,
}

/// Result type a window-production job sends back through its oneshot:
/// the window's decoded blocks plus how many were decoded-cache hits.
type WindowResult = ForstResult<(Vec<DecodedBlock>, u32)>;

/// §2.1.4: oneshot slot for an in-flight window production. Dropping the
/// receiver (iterator closed/aborted) makes the producer's `send` fail and
/// the decoded window is discarded — safe cancellation.
struct PrefetchHandle {
    rx: Receiver<WindowResult>,
    /// `[start, end)` block indices this handle will produce.
    range: (usize, usize),
    /// On-disk bytes of the submitted window — the inflight share charged to
    /// [`PREFETCH_BUFFERED_BYTES`] (M3 telemetry).
    bytes: usize,
}

/// Per-SST-source prefetch state machine. See the module docs.
pub struct BlockPrefetcher {
    reader: Arc<SstReaderImpl>,
    /// Next block index NOT yet covered by `ready`/`inflight` (demand cursor).
    next_block: usize,
    /// First block index this source must never read (§2.1.5 clamp).
    end_block: usize,
    /// Sequential-detection counter: blocks DELIVERED to the consumer.
    blocks_consumed: u32,
    /// Current readahead window in blocks (0 = cold / readahead off).
    ra_blocks: u32,
    /// Claimed-but-undelivered decoded blocks, in index order, each paired
    /// with its ON-DISK size (the M3 telemetry charge released at delivery).
    ready: VecDeque<(u32, DecodedBlock)>,
    /// In-flight window production, if any.
    inflight: Option<PrefetchHandle>,
    /// Regime: local (shallow ramp) vs remote (deep ramp).
    local: bool,
    /// Speculation enabled (env gate; forced in tests via `with_regime`).
    enabled: bool,
    /// L4 compaction-input mode: fixed window (no ramp/doubling), regime
    /// byte-caps bypassed (the engine's fan-in budget clamp sizes the
    /// window), `CacheFillPolicy::Skip` everywhere (windows AND demand).
    compaction: bool,
    /// L4 W5: compaction-read telemetry (only updated in compaction mode).
    stats: CompactionReadStats,
}

impl BlockPrefetcher {
    /// `start_block` = the seek target (`first_block_ge`); `upper` = the
    /// scan's exclusive upper bound, clamped once against the sparse index.
    pub fn new(reader: Arc<SstReaderImpl>, start_block: usize, upper: Option<&[u8]>) -> Self {
        let end_block = reader.end_block_for_upper(start_block, upper);
        let local = reader.is_local_file();
        Self {
            reader,
            next_block: start_block,
            end_block,
            blocks_consumed: 0,
            ra_blocks: 0,
            ready: VecDeque::new(),
            inflight: None,
            local,
            enabled: prefetch_enabled(),
            compaction: false,
            stats: CompactionReadStats::default(),
        }
    }

    /// L4 (2026-06-12 windowed-readpath design §2.2) compaction-input mode:
    /// full-file scan `[0, n_blocks)`, FIXED `window_blocks` window from the
    /// start (no cold state, no ramp — sequentiality is known a priori; the
    /// regime byte-caps are bypassed because the engine already clamped the
    /// window against the fan-in prefetch budget), cache fill policy `Skip`
    /// for window production AND the demand fallback. Gated by
    /// `FRS_COMPACT_WINDOWED` at the ENGINE construction site (compaction.rs)
    /// — NOT by the scan gate `FRS_RS_BLOCK_PREFETCH`.
    ///
    /// The first window is submitted here, so it is in flight before the
    /// merge consumes the first block (I/O + decompress + decode overlap the
    /// merge from block 0).
    pub fn for_compaction(reader: Arc<SstReaderImpl>, window_blocks: u32) -> Self {
        let end_block = reader.index_entry_count();
        let local = reader.is_local_file();
        let mut pf = Self {
            reader,
            next_block: 0,
            end_block,
            blocks_consumed: 0,
            ra_blocks: window_blocks.max(1),
            ready: VecDeque::new(),
            inflight: None,
            local,
            enabled: true,
            compaction: true,
            stats: CompactionReadStats::default(),
        };
        pf.maybe_submit_window();
        pf
    }

    /// L4 W5: this prefetcher's compaction-read telemetry snapshot (all-zero
    /// for scan-mode prefetchers).
    pub fn compaction_stats(&self) -> CompactionReadStats {
        self.stats
    }

    /// Test hook: force the local/remote regime and the enabled flag,
    /// independent of the file backend and the env gate.
    #[cfg(test)]
    pub(crate) fn with_regime(mut self, local: bool, enabled: bool) -> Self {
        self.local = local;
        self.enabled = enabled;
        self
    }

    /// Current readahead window in blocks (0 = cold). Test/diag visibility.
    pub fn ra_blocks(&self) -> u32 {
        self.ra_blocks
    }

    /// FRS-SCAN-COLD-PRIME (Phase-2 cycle 2): does this source have a cold
    /// first-block remote GET that priming would overlap? `true` only when a
    /// concurrent prime would actually save a serial round-trip — every other
    /// case (the §2.2 no-op guards) returns `false` so the caller schedules
    /// ZERO pool work:
    /// - speculation disabled (`FRS_RS_BLOCK_PREFETCH=0`) ⇒ no prefetch surface;
    /// - compaction mode ⇒ already primed at construction;
    /// - local regime (`is_local_file()`) ⇒ preads are µs-class, RTT≈0;
    /// - already primed / mid-stream / at EOF (`inflight`, `ready`, or
    ///   `blocks_consumed > 0`, or cursor past `end_block`) ⇒ nothing cold to
    ///   overlap (idempotent: a second prime call is a no-op);
    /// - the first block is already decoded-cache-resident ⇒ the cold
    ///   `next_decoded` would serve it for free with NO GET (warm scan).
    ///
    /// This is a pure predicate — it schedules nothing and mutates nothing, so
    /// the caller can cheaply check it across all sources before committing to
    /// the concurrent prime wave.
    pub fn wants_priming(&self) -> bool {
        if !self.enabled
            || self.compaction
            || self.local
            || self.inflight.is_some()
            || !self.ready.is_empty()
            || self.blocks_consumed > 0
            || self.next_block >= self.end_block
        {
            return false;
        }
        // Warm first block ⇒ the cold demand read would be a free cache hit; a
        // prime would issue zero I/O anyway, so skip it (the "zero jobs when
        // warm" property).
        match self.reader.block_region(self.next_block) {
            Some((off, _)) => self.reader.cache_get_decoded(off).is_none(),
            None => false,
        }
    }

    /// FRS-SCAN-COLD-PRIME: submit the cold first block to the read-I/O pool
    /// WITHOUT consuming it, so the GET overlaps the merge's other sources'
    /// cold reads (and the merge's first head-seed) instead of paying a serial
    /// round-trip. Idempotent + regime-preserving:
    ///
    /// 1. No-op unless [`Self::wants_priming`] (all §2.2 guards) — so a double
    ///    call, a warm/local/compaction/disabled source, or a mid-stream source
    ///    schedules nothing.
    /// 2. Submits EXACTLY the 1-block window `[next_block, next_block+1)` the
    ///    cold demand path would have read, at the SAME `Insert(Low)` priority
    ///    (`read_decoded_block`'s policy). The block bytes, the cache insert,
    ///    and the ramp trajectory are therefore IDENTICAL to the unprimed cold
    ///    start — only the GET's *timing* moves earlier. `next_decoded` then
    ///    claims this in-flight handle (step 2) instead of issuing the cold
    ///    synchronous read (step 3); `blocks_consumed`/`ra_blocks` advance the
    ///    same way, so the merge's emitted bytes and order are unchanged.
    /// 3. The handle is cancellation-safe exactly like a ramped window: a
    ///    [`Self::terminate`] / drop before consumption discards it and
    ///    releases the M3 charge (no leaked primed window).
    pub fn prime_first_window(&mut self) {
        if !self.wants_priming() {
            return;
        }
        let start = self.next_block;
        let end = start + 1;
        let Some((_, size)) = self.reader.block_region(start) else {
            return;
        };
        let window_bytes = size as u64;
        // Cold demand reads insert at Low (`read_decoded_block`); match it so
        // the primed window is byte- AND cache-identical to the cold path.
        let policy = CacheFillPolicy::Insert(CachePriority::Low);
        let reader = Arc::clone(&self.reader);
        let (tx, rx) = std::sync::mpsc::sync_channel::<WindowResult>(1);
        read_io_pool().submit(Box::new(move || {
            let result = fetch_window(&reader, start, end, policy);
            let _ = tx.send(result);
        }));
        prefetch_charge_add(window_bytes as usize);
        if prefetch_diag() {
            let agg = prefetch_buffered_bytes();
            eprintln!(
                "[PREFETCH_DIAG] cold-prime submit block=[{start},{end}) bytes={window_bytes} aggregate_buffered={agg}"
            );
        }
        self.inflight = Some(PrefetchHandle {
            rx,
            range: (start, end),
            bytes: window_bytes as usize,
        });
        self.next_block = end;
        // NOTE: `ra_blocks` stays 0 (cold). The ramp is entered by
        // `next_decoded`'s demand/deliver path exactly as in the unprimed case
        // once `blocks_consumed >= ramp_after` — priming changes ONLY which
        // mechanism delivers block 0 (in-flight claim vs synchronous demand),
        // not the readahead trajectory.
    }

    /// The clamp computed from the sparse index vs the scan's upper bound.
    pub fn end_block(&self) -> usize {
        self.end_block
    }

    /// §2.1.5: stop this source — the consumer hit the scan's upper bound
    /// mid-block. Drops the in-flight handle (the pool job's result is
    /// discarded on the dead channel) and parks the cursor at `end_block`.
    /// Releases this source's entire M3 telemetry charge (idempotent).
    pub fn terminate(&mut self) {
        self.next_block = self.end_block;
        let held: usize = self.ready.iter().map(|&(sz, _)| sz as usize).sum::<usize>()
            + self.inflight.as_ref().map_or(0, |h| h.bytes);
        if held > 0 {
            prefetch_charge_sub(held);
        }
        self.ready.clear();
        self.inflight = None;
    }

    /// Delivers the next decoded block in index order, or `Ok(None)` when the
    /// source is exhausted (cursor at `end_block`). This is the single entry
    /// point replacing the legacy "read one block per `peek` replenish".
    pub fn next_decoded(&mut self) -> ForstResult<Option<DecodedBlock>> {
        // 1. Serve a claimed block. Double-buffering: if this drains `ready`
        //    and nothing is in flight, submit the next window BEFORE the
        //    consumer walks the delivered block's rows.
        if let Some((sz, block)) = self.ready.pop_front() {
            prefetch_charge_sub(sz as usize);
            self.on_delivered();
            if self.compaction {
                self.stats.window_blocks += 1;
            }
            if self.ready.is_empty() && self.inflight.is_none() {
                self.maybe_submit_window();
            }
            return Ok(Some(block));
        }

        // 2. Claim the in-flight window, then immediately submit the next one
        //    (production of N+1 overlaps consumption of N).
        if let Some(handle) = self.inflight.take() {
            // H1: the pool workers survive job panics (catch_unwind in the
            // worker loop), so a disconnect here means the window job itself
            // panicked; the timeout is the last-resort guard against a
            // queued-but-never-run job (wedged pool) hanging the consumer
            // (ultimately the Flink task thread inside the FFI) forever.
            // Either way the source must PARK AT EOF before erroring —
            // `next_block` was already advanced past the lost window at
            // submit time, so resuming on the demand path would silently
            // skip the window's blocks.
            let produced = match handle.rx.recv_timeout(POOL_JOIN_TIMEOUT) {
                Ok(p) => p,
                Err(e) => {
                    // The handle was already taken — release its inflight M3
                    // charge here (`terminate` only releases what it sees).
                    prefetch_charge_sub(handle.bytes);
                    self.terminate();
                    return Err(match e {
                        std::sync::mpsc::RecvTimeoutError::Disconnected => ForstError::internal(
                            "BlockPrefetcher: read-I/O pool worker dropped its window",
                        ),
                        std::sync::mpsc::RecvTimeoutError::Timeout => ForstError::timed_out(
                            "BlockPrefetcher: prefetch window not produced within join timeout",
                        ),
                    });
                }
            };
            match produced {
                Ok((blocks, cache_hits)) => {
                    debug_assert_eq!(blocks.len(), handle.range.1 - handle.range.0);
                    if self.compaction {
                        self.stats.cache_hits += cache_hits as u64;
                    }
                    // M3: the inflight charge transfers to `ready` (each
                    // block keeps its on-disk size) — no net counter change.
                    for (j, block) in blocks.into_iter().enumerate() {
                        let sz = self
                            .reader
                            .block_region(handle.range.0 + j)
                            .map_or(0, |(_, s)| s);
                        self.ready.push_back((sz, block));
                    }
                    self.maybe_submit_window();
                    // Recurse once into the ready-serve path (never deeper:
                    // `ready` is now non-empty or the window was empty ⇒ EOF).
                    if let Some((sz, block)) = self.ready.pop_front() {
                        prefetch_charge_sub(sz as usize);
                        self.on_delivered();
                        if self.compaction {
                            self.stats.window_blocks += 1;
                        }
                        return Ok(Some(block));
                    }
                    return Ok(None);
                }
                Err(e) => {
                    // A failed window aborts the source (same as a failed
                    // demand read on the legacy path). Park at EOF so retries
                    // don't re-issue I/O on a known-bad region. The handle
                    // was already taken, so release its inflight charge here
                    // (`terminate` only releases what it can still see).
                    prefetch_charge_sub(handle.bytes);
                    self.terminate();
                    return Err(e);
                }
            }
        }

        // 3. Cold / demand path. Scan mode: identical to the legacy per-block
        //    read (cache-first + Low insert inside `read_decoded_block`).
        //    Compaction mode (L4): the demand fallback must ALSO honor
        //    `CacheFillPolicy::Skip` — reuse the 1-block `fetch_window` run
        //    synchronously (cache-first check, no insert).
        if self.next_block >= self.end_block {
            return Ok(None);
        }
        let idx = self.next_block;
        let (off, size) = self
            .reader
            .block_region(idx)
            .ok_or_else(|| ForstError::internal("BlockPrefetcher: block index out of range"))?;
        let block = if self.compaction {
            let (mut blocks, cache_hits) =
                fetch_window(&self.reader, idx, idx + 1, CacheFillPolicy::Skip)?;
            self.stats.demand_blocks += 1;
            self.stats.cache_hits += cache_hits as u64;
            blocks.pop().expect("1-block window produces 1 block")
        } else {
            self.reader.read_decoded_block(off, size)?
        };
        self.next_block = idx + 1;
        self.on_delivered();
        if self.compaction {
            // Restore windowed flow for the remaining blocks (the demand
            // fallback only fires when no window was submittable).
            self.maybe_submit_window();
        } else {
            // Enter the ramp once sequentiality is established for the regime.
            let ramp_after = if self.local {
                LOCAL_RAMP_AFTER
            } else {
                REMOTE_RAMP_AFTER
            };
            if self.enabled && self.ra_blocks == 0 && self.blocks_consumed >= ramp_after {
                self.ra_blocks = 2;
                self.maybe_submit_window();
            }
        }
        Ok(Some(block))
    }

    /// Bookkeeping per block handed to the consumer.
    fn on_delivered(&mut self) {
        self.blocks_consumed = self.blocks_consumed.saturating_add(1);
        self.reader.note_block_read();
    }

    /// Submits the next `[next_block, next_block + ra)` window (clamped to
    /// `end_block`, byte-trimmed to the regime cap) to the read-I/O pool and
    /// doubles `ra_blocks` toward the cap. No-op when cold, disabled, already
    /// in flight, or at EOF.
    fn maybe_submit_window(&mut self) {
        if !self.enabled
            || self.ra_blocks == 0
            || self.inflight.is_some()
            || self.next_block >= self.end_block
        {
            return;
        }
        // L4 compaction mode: the window is already sized IN BLOCKS by the
        // engine's fan-in budget clamp — the scan regime byte-caps (256 KiB
        // local) must not shrink the 2 MiB-class compaction window.
        let cap_bytes = if self.compaction {
            u64::MAX
        } else if self.local {
            LOCAL_CAP_BYTES
        } else {
            REMOTE_CAP_BYTES
        };
        let start = self.next_block;
        let max_end = self.end_block.min(start + self.ra_blocks as usize);
        // Byte-trim the window against the regime cap (always >= 1 block).
        let mut end = start;
        let mut window_bytes: u64 = 0;
        while end < max_end {
            let Some((_, size)) = self.reader.block_region(end) else {
                break;
            };
            if end > start && window_bytes + size as u64 > cap_bytes {
                break;
            }
            window_bytes += size as u64;
            end += 1;
        }
        if end == start {
            return;
        }
        // §2.1: ramped prefetch inserts at Bottom once the window is deep;
        // shallow (just-ramped) windows keep today's Low. L4: compaction
        // inputs SKIP insertion entirely (read each block exactly once;
        // future reads hit the compaction output).
        let policy = if self.compaction {
            CacheFillPolicy::Skip
        } else if self.ra_blocks >= BOTTOM_PRIORITY_RA {
            CacheFillPolicy::Insert(CachePriority::Bottom)
        } else {
            CacheFillPolicy::Insert(CachePriority::Low)
        };
        let reader = Arc::clone(&self.reader);
        // Rendezvous-free oneshot: capacity 1 so the producer never blocks.
        let (tx, rx) = std::sync::mpsc::sync_channel::<WindowResult>(1);
        read_io_pool().submit(Box::new(move || {
            let result = fetch_window(&reader, start, end, policy);
            // Receiver dropped (iterator closed/aborted) ⇒ result discarded.
            let _ = tx.send(result);
        }));
        // M3 telemetry: charge the window at submit (released at delivery /
        // failure / terminate / drop). The aggregate (shard sum) is only
        // materialized when diag is on — the hot path does one Relaxed RMW
        // on this thread's own shard line.
        prefetch_charge_add(window_bytes as usize);
        if prefetch_diag() {
            let agg = prefetch_buffered_bytes();
            eprintln!(
                "[PREFETCH_DIAG] window submit blocks=[{start},{end}) bytes={window_bytes} aggregate_buffered={agg}"
            );
        }
        self.inflight = Some(PrefetchHandle {
            rx,
            range: (start, end),
            bytes: window_bytes as usize,
        });
        self.next_block = end;
        if self.compaction {
            // L4: FIXED window — no ramp doubling; record W5 telemetry.
            self.stats.window_submits += 1;
            self.stats.prefetched_bytes += window_bytes;
        } else {
            // Double toward the regime cap for the NEXT window.
            let cap_blocks = if self.local {
                LOCAL_CAP_BLOCKS
            } else {
                REMOTE_CAP_BLOCKS
            };
            self.ra_blocks = (self.ra_blocks * 2).min(cap_blocks);
        }
    }
}

impl Drop for BlockPrefetcher {
    /// M3: a dropped prefetcher releases its entire ready+inflight charge —
    /// the aggregate counter never leaks from abandoned iterators.
    /// (`terminate` is idempotent: after an explicit terminate the fields are
    /// already empty and this subtracts nothing.)
    fn drop(&mut self) {
        self.terminate();
    }
}

/// Pool-side window production: cache-first per block (window SPLIT around
/// hits — cached blocks are excluded from I/O), then ALL contiguous runs of
/// misses issued as ONE vectored read (`read_block_regions` — N read SQEs in
/// one io_uring submission on Linux, serial preads on the portable fallback;
/// bit-identical either way), decode (decompress + KvBlock pointer-walk) on
/// this pool thread, then apply the cache fill `policy` (L4: scan path
/// inserts at its priority; compaction inputs Skip). Returns the window's
/// blocks in index order plus the number of decoded-cache hits (W5).
fn fetch_window(
    reader: &SstReaderImpl,
    start: usize,
    end: usize,
    policy: CacheFillPolicy,
) -> ForstResult<(Vec<DecodedBlock>, u32)> {
    let n = end - start;
    let mut out: Vec<Option<DecodedBlock>> = (0..n).map(|_| None).collect();
    let mut cache_hits = 0u32;
    // Pass 1: cache hits (window splitting).
    let mut regions: Vec<(u64, u32)> = Vec::with_capacity(n);
    for idx in start..end {
        let (off, size) = reader
            .block_region(idx)
            .ok_or_else(|| ForstError::internal("prefetch window past sparse index"))?;
        regions.push((off, size));
        if let Some(hit) = reader.cache_get_decoded(off) {
            out[idx - start] = Some(hit);
            cache_hits += 1;
        }
    }
    // Pass 2: group cache-missing blocks into runs of PHYSICALLY contiguous
    // file ranges (the writer lays blocks back-to-back; verified
    // defensively). Each run is one I/O region.
    let mut io_regions: Vec<(u64, usize)> = Vec::new(); // (file_off, run_bytes)
    let mut run_blocks: Vec<(usize, usize)> = Vec::new(); // [run_start, run_end) window-local
    let mut i = 0usize;
    while i < n {
        if out[i].is_some() {
            i += 1;
            continue;
        }
        let run_start = i;
        let mut run_end = i + 1;
        while run_end < n
            && out[run_end].is_none()
            && regions[run_end].0 == regions[run_end - 1].0 + regions[run_end - 1].1 as u64
        {
            run_end += 1;
        }
        let run_bytes: usize = regions[run_start..run_end]
            .iter()
            .map(|&(_, s)| s as usize)
            .sum();
        io_regions.push((regions[run_start].0, run_bytes));
        run_blocks.push((run_start, run_end));
        i = run_end;
    }
    if !io_regions.is_empty() {
        // Pass 3: ONE vectored read for every run, packed back-to-back.
        let total: usize = io_regions.iter().map(|&(_, l)| l).sum();
        let mut buf = vec![0u8; total];
        reader.read_block_regions(&io_regions, &mut buf)?;
        // Pass 4: per-block decode from slices of the packed buffer.
        let mut cursor = 0usize;
        for &(run_start, run_end) in &run_blocks {
            for (j, &(off, size)) in regions.iter().enumerate().take(run_end).skip(run_start) {
                let slice = &buf[cursor..cursor + size as usize];
                cursor += size as usize;
                let decoded = reader.decode_block_from_slice(slice)?;
                match policy {
                    CacheFillPolicy::Insert(priority) => {
                        reader.cache_insert_decoded(off, &decoded, priority)
                    }
                    // L4 compaction inputs: read-only cache use — never
                    // insert (each input block is read exactly once).
                    CacheFillPolicy::Skip => {}
                }
                out[j] = Some(decoded);
            }
        }
        debug_assert_eq!(cursor, total);
    }
    Ok((
        out.into_iter()
            .map(|o| o.expect("every window slot filled above"))
            .collect(),
        cache_hits,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{BlockCache, CacheEntry, CacheKey, CacheMetrics};
    use crate::sst::writer::{SstWriterImpl, SstWriterOptions};
    use forst_rs_common::CompressionType;
    use forst_rs_io::filesystem::RandomAccessFile;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex as StdMutex;

    /// In-memory RandomAccessFile that counts positional reads (for proving
    /// multi-block coalescing and cache-hit window splitting).
    struct CountingMemFile {
        data: Arc<Vec<u8>>,
        reads: AtomicU64,
    }

    impl RandomAccessFile for CountingMemFile {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let start = offset as usize;
            if start >= self.data.len() {
                return Ok(0);
            }
            let end = std::cmp::min(start + buf.len(), self.data.len());
            buf[..end - start].copy_from_slice(&self.data[start..end]);
            Ok(end - start)
        }

        fn file_size(&self) -> ForstResult<u64> {
            Ok(self.data.len() as u64)
        }
    }

    /// Records every insert's (offset, priority); delegates nothing (no
    /// eviction — unbounded map), serves gets from the map.
    struct RecordingCache {
        entries: StdMutex<std::collections::HashMap<CacheKey, Arc<CacheEntry>>>,
        inserts: StdMutex<Vec<(u64, CachePriority)>>,
        metrics: CacheMetrics,
    }

    impl RecordingCache {
        fn new() -> Self {
            Self {
                entries: StdMutex::new(std::collections::HashMap::new()),
                inserts: StdMutex::new(Vec::new()),
                metrics: CacheMetrics::new(usize::MAX),
            }
        }
    }

    impl BlockCache for RecordingCache {
        fn get(&self, key: &CacheKey) -> Option<Arc<CacheEntry>> {
            self.entries.lock().unwrap().get(key).cloned()
        }

        fn insert(
            &self,
            key: CacheKey,
            value: CacheEntry,
            _charge: usize,
            priority: CachePriority,
        ) {
            self.inserts
                .lock()
                .unwrap()
                .push((key.block_offset, priority));
            self.entries.lock().unwrap().insert(key, Arc::new(value));
        }

        fn erase(&self, key: &CacheKey) {
            self.entries.lock().unwrap().remove(key);
        }

        fn erase_by_file(&self, file_number: u64) {
            self.entries
                .lock()
                .unwrap()
                .retain(|k, _| k.file_number != file_number);
        }

        fn total_charge(&self) -> usize {
            0
        }

        fn hit_rate(&self) -> f64 {
            0.0
        }

        fn metrics(&self) -> &CacheMetrics {
            &self.metrics
        }
    }

    /// Builds a multi-block SST (small block_size forces many blocks) with
    /// explicit compression and (optionally forced) block format.
    /// `kv_format`: `None` = env default, `Some(true)` = v2 KV,
    /// `Some(false)` = v1 Arrow. Returns the raw bytes.
    fn build_sst_with(
        n_rows: usize,
        compression: CompressionType,
        kv_format: Option<bool>,
    ) -> Arc<Vec<u8>> {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 1024,
            compression,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        if let Some(kv) = kv_format {
            writer.force_kv_block_format(kv);
        }
        for i in 0..n_rows {
            let key = format!("key_{:05}", i);
            let val = format!("val_{:05}_{}", i, "x".repeat(48));
            writer
                .add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 1)
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();
        Arc::new(data)
    }

    fn build_sst(n_rows: usize) -> Arc<Vec<u8>> {
        build_sst_with(n_rows, CompressionType::None, None)
    }

    fn open_reader(
        data: &Arc<Vec<u8>>,
        cache: Option<Arc<dyn BlockCache>>,
        reads: &Arc<Vec<u8>>,
    ) -> (Arc<SstReaderImpl>, Arc<CountingMemFile>) {
        let _ = reads;
        let file = Arc::new(CountingMemFile {
            data: Arc::clone(data),
            reads: AtomicU64::new(0),
        });
        // RandomAccessFile is implemented on the struct; clone the Arc into a
        // forwarding box so the test keeps the read counter.
        struct Fwd(Arc<CountingMemFile>);
        impl RandomAccessFile for Fwd {
            fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
                self.0.read_at(offset, buf)
            }
            fn file_size(&self) -> ForstResult<u64> {
                self.0.file_size()
            }
        }
        let mut reader = SstReaderImpl::open(Box::new(Fwd(Arc::clone(&file)))).unwrap();
        if let Some(c) = cache {
            reader = reader.with_block_cache(c, 1, 7);
        }
        (Arc::new(reader), file)
    }

    /// Drains every block through the prefetcher and collects all rows.
    fn drain_rows(pf: &mut BlockPrefetcher) -> Vec<(Vec<u8>, u64)> {
        let mut rows = Vec::new();
        while let Some(block) = pf.next_decoded().unwrap() {
            block
                .for_each_row(|view| {
                    rows.push((view.key.to_vec(), view.sequence));
                    Ok(())
                })
                .unwrap();
        }
        rows
    }

    /// Demand-only reference drain via the legacy per-block path.
    fn drain_rows_demand(reader: &SstReaderImpl) -> Vec<(Vec<u8>, u64)> {
        let mut rows = Vec::new();
        for idx in 0..reader.index_entry_count() {
            reader
                .for_each_row_in_block(idx, |view| {
                    rows.push((view.key.to_vec(), view.sequence));
                    Ok(())
                })
                .unwrap();
        }
        rows
    }

    /// §2.1 equal-results gate: a full prefetched drain over a multi-block SST
    /// yields exactly the rows of the legacy demand-paged walk, for both the
    /// local and remote regimes.
    #[test]
    fn prefetched_drain_equals_demand_drain() {
        let data = build_sst(400);
        for local in [true, false] {
            let (reader, _file) = open_reader(&data, None, &data);
            assert!(
                reader.index_entry_count() >= 8,
                "need a multi-block SST, got {} blocks",
                reader.index_entry_count()
            );
            let expected = drain_rows_demand(&reader);
            let mut pf =
                BlockPrefetcher::new(Arc::clone(&reader), 0, None).with_regime(local, true);
            let got = drain_rows(&mut pf);
            assert_eq!(got, expected, "local={local}");
        }
    }

    /// Ramp state transitions (local regime): cold for the first 2 blocks
    /// (ra=0, zero speculation), then ra=2 → 4 (cap) doubling per window.
    #[test]
    fn ramp_transitions_local() {
        let data = build_sst(400);
        let (reader, _file) = open_reader(&data, None, &data);
        let mut pf = BlockPrefetcher::new(Arc::clone(&reader), 0, None).with_regime(true, true);
        // Block 1: cold.
        assert!(pf.next_decoded().unwrap().is_some());
        assert_eq!(pf.ra_blocks(), 0, "cold after first block");
        // Block 2: ramp entered (>= 2 consumed) — first window (2 blocks)
        // submitted, ra doubled to 4 for the next window.
        assert!(pf.next_decoded().unwrap().is_some());
        assert_eq!(pf.ra_blocks(), 4, "ramped to 2, doubled to 4 after submit");
        // Drain a few more: cap holds at 4 (local 256 KiB regime).
        for _ in 0..6 {
            assert!(pf.next_decoded().unwrap().is_some());
        }
        assert_eq!(pf.ra_blocks(), LOCAL_CAP_BLOCKS, "local cap is 4 blocks");
    }

    /// Remote regime (M2): cold for the first 2 blocks exactly like local —
    /// a 1-block probe over an evicted SST must NEVER speculate remotely —
    /// then ramps and doubles toward the DEEPER 4 MiB / 64-block cap.
    #[test]
    fn ramp_transitions_remote_after_two_blocks() {
        let data = build_sst(400);
        let (reader, _file) = open_reader(&data, None, &data);
        let mut pf = BlockPrefetcher::new(Arc::clone(&reader), 0, None).with_regime(false, true);
        // Block 1: cold — zero speculation (the R-short invariant on the
        // evicted tier).
        assert!(pf.next_decoded().unwrap().is_some());
        assert_eq!(pf.ra_blocks(), 0, "remote stays cold after first block");
        // Block 2: ramp entered (>= 2 consumed) — first window (2 blocks)
        // submitted, ra doubled to 4 for the next window.
        assert!(pf.next_decoded().unwrap().is_some());
        assert_eq!(
            pf.ra_blocks(),
            4,
            "remote ramps after block 2 (ra=2 submitted, doubled to 4)"
        );
        let mut seen = 2;
        while pf.next_decoded().unwrap().is_some() {
            seen += 1;
        }
        assert_eq!(seen, reader.index_entry_count());
        assert!(pf.ra_blocks() <= REMOTE_CAP_BLOCKS);
        assert!(
            pf.ra_blocks() >= 8,
            "remote keeps doubling past the local cap"
        );
    }

    /// §2.1.5 clamp: with an upper bound that cuts the keyspace in half, the
    /// prefetcher never delivers (nor reads) blocks past `end_block`, and the
    /// rows match the demand walk truncated at the bound.
    #[test]
    fn clamps_at_end_block_from_upper_bound() {
        let data = build_sst(400);
        let (reader, file) = open_reader(&data, None, &data);
        let total_blocks = reader.index_entry_count();
        let upper = b"key_00200".to_vec();
        let mut pf =
            BlockPrefetcher::new(Arc::clone(&reader), 0, Some(&upper)).with_regime(true, true);
        let end = pf.end_block();
        assert!(
            end < total_blocks,
            "upper bound must clamp ({end} < {total_blocks})"
        );
        let mut delivered = 0;
        let mut rows = Vec::new();
        while let Some(b) = pf.next_decoded().unwrap() {
            delivered += 1;
            b.for_each_row(|v| {
                rows.push(v.key.to_vec());
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(delivered, end, "delivers exactly [0, end_block)");
        // Every key < upper appears; the first key >= upper may appear only
        // within the boundary block (caller-side filtering handles those).
        assert!(rows.iter().any(|k| k.as_slice() < upper.as_slice()));
        // No read ever touched bytes at/after block `end`'s offset.
        let (end_off, _) = reader.block_region(end).unwrap();
        // (CountingMemFile can't record offsets per read; instead prove via
        // the reader's own region table: a read past the clamp would have
        // required delivering > end blocks, asserted above. Additionally the
        // raw pread count is bounded by the block count before the clamp.)
        assert!(end_off > 0);
        let _ = file;
    }

    /// Multi-block coalescing: a ramped window of N contiguous blocks issues
    /// ONE positional read (not N), and cached blocks SPLIT the I/O window
    /// around hits.
    #[test]
    fn coalesces_contiguous_blocks_and_splits_around_cache_hits() {
        let data = build_sst(400);
        let cache = Arc::new(RecordingCache::new());
        let (reader, file) = open_reader(&data, Some(cache.clone()), &data);
        let blocks = reader.index_entry_count();
        assert!(blocks >= 8);

        // Pre-warm block index 4 into the cache via the demand path.
        let (off4, size4) = reader.block_region(4).unwrap();
        let d = reader.read_decoded_block(off4, size4).unwrap();
        reader.cache_insert_decoded(off4, &d, CachePriority::Low);

        let reads_before = file.reads.load(Ordering::SeqCst);
        // Window [2, 6) contains the cached block 4 → I/O runs are [2,4) and
        // [5,6): exactly 2 positional reads for 3 missing blocks.
        let (out, hits) =
            fetch_window(&reader, 2, 6, CacheFillPolicy::Insert(CachePriority::Low)).unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(hits, 1, "the pre-warmed block 4 is a decoded-cache hit");
        let reads_after = file.reads.load(Ordering::SeqCst);
        assert_eq!(
            reads_after - reads_before,
            2,
            "window split around the cache hit: [2,4) + [5,6) = 2 preads"
        );

        // And a fully-missing window of 4 blocks = exactly 1 pread.
        let reads_before = file.reads.load(Ordering::SeqCst);
        let (out, hits) =
            fetch_window(&reader, 8, 12, CacheFillPolicy::Insert(CachePriority::Low)).unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(hits, 0);
        assert_eq!(
            file.reads.load(Ordering::SeqCst) - reads_before,
            1,
            "4 contiguous missing blocks coalesce into ONE pread"
        );
    }

    /// Cache-priority contract: demand inserts at Low; once the ramp deepens
    /// (ra >= 4) prefetch windows insert at Bottom. Insertion is NEVER
    /// skipped.
    #[test]
    fn ramped_prefetch_inserts_bottom_priority() {
        let data = build_sst(400);
        let cache = Arc::new(RecordingCache::new());
        let (reader, _file) = open_reader(&data, Some(cache.clone()), &data);
        let blocks = reader.index_entry_count();
        let mut pf = BlockPrefetcher::new(Arc::clone(&reader), 0, None).with_regime(true, true);
        let mut n = 0;
        while pf.next_decoded().unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, blocks);
        let inserts = cache.inserts.lock().unwrap().clone();
        // EVERY fetched block was inserted (cache never bypassed).
        assert_eq!(inserts.len(), blocks, "no block bypasses cache insertion");
        // Demand blocks (cold state: first 2) inserted at Low.
        assert!(inserts[..2].iter().all(|&(_, p)| p == CachePriority::Low));
        // The first ramped window (ra=2 < 4) stays Low; deeper windows are
        // Bottom.
        assert!(
            inserts[4..]
                .iter()
                .all(|&(_, p)| p == CachePriority::Bottom),
            "deep-ramp windows insert at Bottom: {:?}",
            &inserts[4..]
        );
        assert!(
            inserts.iter().any(|&(_, p)| p == CachePriority::Bottom),
            "ramp must reach Bottom priority"
        );
    }

    /// Kill switch / cold-forever: with speculation disabled every block is
    /// demand-fetched and results are identical.
    #[test]
    fn disabled_prefetch_is_pure_demand_and_equal() {
        let data = build_sst(200);
        let (reader, _file) = open_reader(&data, None, &data);
        let expected = drain_rows_demand(&reader);
        let mut pf = BlockPrefetcher::new(Arc::clone(&reader), 0, None).with_regime(true, false);
        let got = drain_rows(&mut pf);
        assert_eq!(got, expected);
        assert_eq!(pf.ra_blocks(), 0, "speculation never engages when disabled");
    }

    /// terminate() drops the in-flight window and parks at EOF; subsequent
    /// next_decoded() returns None (safe cancellation).
    #[test]
    fn terminate_cancels_inflight_window() {
        let data = build_sst(400);
        let (reader, _file) = open_reader(&data, None, &data);
        let mut pf = BlockPrefetcher::new(Arc::clone(&reader), 0, None).with_regime(true, true);
        // Enter the ramp (a window goes in flight).
        assert!(pf.next_decoded().unwrap().is_some());
        assert!(pf.next_decoded().unwrap().is_some());
        pf.terminate();
        assert!(pf.next_decoded().unwrap().is_none());
        assert!(pf.next_decoded().unwrap().is_none());
    }

    /// H1 (i)+(iii): a panicking job does not kill a read-I/O pool worker —
    /// with a SINGLE worker, jobs submitted after the panic still run, and
    /// nothing hangs.
    #[test]
    fn pool_worker_survives_panicking_job() {
        let pool = ReadIoPool::new(1);
        let (tx, rx) = std::sync::mpsc::channel::<u8>();
        pool.submit(Box::new(|| panic!("deliberate test panic")));
        let tx2 = tx.clone();
        pool.submit(Box::new(move || {
            let _ = tx2.send(7);
        }));
        drop(tx);
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(10))
                .expect("worker died after a panicking job (or hang)"),
            7
        );
    }

    /// H1 (ii): a window job whose sender is dropped (the unwind path of a
    /// panicking job) surfaces as `Err` on the consumer — and the source
    /// parks at EOF instead of silently skipping the lost window's blocks.
    #[test]
    fn dropped_window_sender_errors_and_parks_at_eof() {
        let data = build_sst(400);
        let (reader, _file) = open_reader(&data, None, &data);
        let mut pf = BlockPrefetcher::new(Arc::clone(&reader), 0, None).with_regime(true, true);
        // Simulate a panicked window job: receiver installed, sender gone.
        let (tx, rx) = std::sync::mpsc::sync_channel::<WindowResult>(1);
        drop(tx);
        pf.inflight = Some(PrefetchHandle {
            rx,
            range: (0, 2),
            bytes: 0,
        });
        pf.next_block = 2; // as maybe_submit_window would have left it
        let err = match pf.next_decoded() {
            Err(e) => e,
            Ok(_) => panic!("dropped sender must error"),
        };
        assert!(
            matches!(err, ForstError::Internal(_)),
            "unexpected error kind: {err:?}"
        );
        // Parked at EOF — never resumes past the lost window.
        assert!(pf.next_decoded().unwrap().is_none());
    }

    /// M3: the aggregate ready+inflight counter goes up while a ramped
    /// prefetcher holds undelivered windows and is fully released once the
    /// prefetcher is dropped mid-stream (no leak from abandoned iterators).
    /// Tolerates concurrent tests by polling for release and asserting the
    /// counter never wraps.
    #[test]
    fn buffered_bytes_charge_released_on_drop() {
        let data = build_sst(400);
        let (reader, _file) = open_reader(&data, None, &data);
        let before = prefetch_buffered_bytes();
        {
            let mut pf =
                BlockPrefetcher::new(Arc::clone(&reader), 0, None).with_regime(false, true);
            // Consume past the ramp so windows are submitted (charged).
            for _ in 0..6 {
                assert!(pf.next_decoded().unwrap().is_some());
            }
            // pf dropped here mid-stream with ready and/or inflight bytes.
        }
        // The charge must drain back out (other tests may add their own
        // transient charges, so poll until OUR contribution is gone).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let now = prefetch_buffered_bytes();
            assert!(now < usize::MAX / 2, "counter wrapped (double release)");
            if now <= before {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "prefetch buffered-bytes charge leaked: before={before} now={now}"
            );
            std::thread::yield_now();
        }
    }

    /// M3 sharding smoke (2026-06-12 bisect de-contention): 8 threads charge
    /// concurrently on their own shards while the main thread releases the
    /// same total — cross-thread release legitimately drives individual
    /// shards NEGATIVE, but the shard SUM stays exact: it drains back to
    /// the baseline and never wraps. Tolerates concurrent tests' transient
    /// charges by polling for release (same pattern as the drop test above).
    #[test]
    fn sharded_counter_cross_thread_release_sum_exact() {
        let before = prefetch_buffered_bytes();
        const THREADS: usize = 8;
        const OPS: usize = 1000;
        const BYTES: usize = 4096;
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                std::thread::spawn(|| {
                    for _ in 0..OPS {
                        prefetch_charge_add(BYTES);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // Release the ENTIRE charge from this thread — a shard the producer
        // threads may never have touched goes negative; the sum must still
        // account exactly.
        for _ in 0..THREADS * OPS {
            prefetch_charge_sub(BYTES);
        }
        // Our net contribution is zero; poll for other tests' transient
        // charges to drain, asserting the (clamped) sum never wraps.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let now = prefetch_buffered_bytes();
            assert!(now < usize::MAX / 2, "counter wrapped (lost release)");
            if now <= before {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "sharded counter sum did not return to baseline: before={before} now={now}"
            );
            std::thread::yield_now();
        }
    }

    /// Starting mid-file (seek target from first_block_ge) delivers exactly
    /// the tail blocks — prefetch windows respect the start offset.
    #[test]
    fn starts_at_seek_target() {
        let data = build_sst(400);
        let (reader, _file) = open_reader(&data, None, &data);
        let blocks = reader.index_entry_count();
        let start = blocks / 2;
        let mut expected = Vec::new();
        for idx in start..blocks {
            reader
                .for_each_row_in_block(idx, |v| {
                    expected.push((v.key.to_vec(), v.sequence));
                    Ok(())
                })
                .unwrap();
        }
        let mut pf = BlockPrefetcher::new(Arc::clone(&reader), start, None).with_regime(true, true);
        let got = drain_rows(&mut pf);
        assert_eq!(got, expected);
    }

    // -----------------------------------------------------------------------
    // FRS-SCAN-COLD-PRIME (Phase-2 cycle 2): concurrent cold-start prime
    // -----------------------------------------------------------------------

    /// A prefetcher exists in a cold, remote, speculation-enabled regime with a
    /// non-cache-resident first block (the only case where priming saves a
    /// serial round-trip).
    fn fresh_remote(reader: &Arc<SstReaderImpl>) -> BlockPrefetcher {
        BlockPrefetcher::new(Arc::clone(reader), 0, None).with_regime(false, true)
    }

    /// G1 byte-identity: a primed cold start delivers EXACTLY the rows of an
    /// unprimed cold start over the same source (the prime moves only the
    /// timing of block 0's read), for both block formats × compression.
    #[test]
    fn primed_drain_equals_unprimed_drain_all_formats() {
        for kv in [false, true] {
            for compression in [CompressionType::None, CompressionType::Lz4] {
                let data = build_sst_with(400, compression, Some(kv));
                let (reader, _file) = open_reader(&data, None, &data);
                assert!(reader.index_entry_count() >= 8);
                // Reference: unprimed remote cold start.
                let mut unprimed = fresh_remote(&reader);
                let expected = drain_rows(&mut unprimed);
                // Primed remote cold start.
                let mut primed = fresh_remote(&reader);
                assert!(primed.wants_priming(), "fresh remote source wants priming");
                primed.prime_first_window();
                let got = drain_rows(&mut primed);
                assert_eq!(got, expected, "kv={kv} compression={compression:?}");
            }
        }
    }

    /// Idempotence: a second `prime_first_window` after the first does NOT
    /// double-submit (the source already has its block 0 in flight), and the
    /// rows are still the unprimed reference.
    #[test]
    fn prime_is_idempotent() {
        let data = build_sst(400);
        let (reader, _file) = open_reader(&data, None, &data);
        let mut unprimed = fresh_remote(&reader);
        let expected = drain_rows(&mut unprimed);

        let mut pf = fresh_remote(&reader);
        pf.prime_first_window();
        let range_after_first = pf.inflight.as_ref().map(|h| h.range);
        // Second call must early-return (already in flight ⇒ wants_priming false).
        assert!(
            !pf.wants_priming(),
            "already-primed source must not re-prime"
        );
        pf.prime_first_window();
        assert_eq!(
            pf.inflight.as_ref().map(|h| h.range),
            range_after_first,
            "second prime must not replace / double-submit the in-flight window"
        );
        let got = drain_rows(&mut pf);
        assert_eq!(got, expected);
    }

    /// §2.2 guards: each no-op case reports `wants_priming() == false` and
    /// `prime_first_window` schedules nothing (no in-flight handle appears).
    #[test]
    fn prime_skip_guards_are_no_ops() {
        let data = build_sst(400);

        // (a) local regime — preads are µs-class.
        let (reader, _f) = open_reader(&data, None, &data);
        let mut local = BlockPrefetcher::new(Arc::clone(&reader), 0, None).with_regime(true, true);
        assert!(!local.wants_priming(), "local regime must skip");
        local.prime_first_window();
        assert!(local.inflight.is_none(), "local prime scheduled nothing");

        // (b) speculation disabled.
        let (reader, _f) = open_reader(&data, None, &data);
        let mut disabled =
            BlockPrefetcher::new(Arc::clone(&reader), 0, None).with_regime(false, false);
        assert!(!disabled.wants_priming(), "disabled must skip");
        disabled.prime_first_window();
        assert!(disabled.inflight.is_none());

        // (c) mid-stream (already consumed a block) — nothing cold to overlap.
        let (reader, _f) = open_reader(&data, None, &data);
        let mut mid = fresh_remote(&reader);
        assert!(mid.next_decoded().unwrap().is_some());
        assert!(!mid.wants_priming(), "consumed source must skip");

        // (d) compaction mode — already primed at construction.
        let (reader, _f) = open_reader(&data, None, &data);
        let comp = BlockPrefetcher::for_compaction(Arc::clone(&reader), 4);
        assert!(!comp.wants_priming(), "compaction mode must skip");

        // (e) EOF — empty range after a tight upper bound.
        let (reader, _f) = open_reader(&data, None, &data);
        let upper = b"key_00000".to_vec(); // clamps end_block to 0 (or near it)
        let pf_eof = BlockPrefetcher::new(
            Arc::clone(&reader),
            reader.index_entry_count(),
            Some(&upper),
        )
        .with_regime(false, true);
        assert!(!pf_eof.wants_priming(), "at-EOF source must skip");
    }

    /// Warm-path no-op (the "zero pool jobs when warm" property): with the
    /// first block already decoded-cache-resident, a remote cold start would
    /// issue no GET, so priming must schedule nothing.
    #[test]
    fn prime_no_op_when_first_block_cache_resident() {
        let data = build_sst(400);
        let cache = Arc::new(RecordingCache::new());
        let (reader, _file) = open_reader(&data, Some(cache.clone()), &data);
        // Pre-warm block 0 into the decoded cache.
        let (off0, size0) = reader.block_region(0).unwrap();
        let d = reader.read_decoded_block(off0, size0).unwrap();
        reader.cache_insert_decoded(off0, &d, CachePriority::Low);

        let mut pf = fresh_remote(&reader);
        assert!(
            !pf.wants_priming(),
            "cache-resident first block must skip priming"
        );
        pf.prime_first_window();
        assert!(
            pf.inflight.is_none(),
            "warm first block primes nothing (zero pool jobs)"
        );
    }

    /// Cancellation: a primed-but-unconsumed window is released on terminate /
    /// drop — the M3 buffered-bytes charge returns to baseline (no leak).
    #[test]
    fn primed_window_released_on_terminate() {
        let data = build_sst(400);
        let (reader, _file) = open_reader(&data, None, &data);
        let before = prefetch_buffered_bytes();
        {
            let mut pf = fresh_remote(&reader);
            pf.prime_first_window();
            assert!(pf.inflight.is_some(), "prime put a window in flight");
            pf.terminate();
            assert!(pf.inflight.is_none(), "terminate dropped the primed window");
            assert!(pf.next_decoded().unwrap().is_none(), "parked at EOF");
        }
        // The charge must drain back to baseline (poll for concurrent tests).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let now = prefetch_buffered_bytes();
            assert!(now < usize::MAX / 2, "counter wrapped (double release)");
            if now <= before {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "primed-window charge leaked: before={before} now={now}"
            );
            std::thread::yield_now();
        }
    }

    // -----------------------------------------------------------------------
    // L4 compaction-input mode (2026-06-12 windowed-readpath design)
    // -----------------------------------------------------------------------

    /// (key, value, sequence, op_byte) — the full-row drain element.
    type FullRow = (Vec<u8>, Option<Vec<u8>>, u64, u8);

    /// Full-row demand-paged reference drain (key, value, seq, op).
    fn drain_full_demand(reader: &SstReaderImpl) -> Vec<FullRow> {
        let mut rows = Vec::new();
        for idx in 0..reader.index_entry_count() {
            reader
                .for_each_row_in_block(idx, |v| {
                    rows.push((
                        v.key.to_vec(),
                        v.value.map(|x| x.to_vec()),
                        v.sequence,
                        v.op_type as u8,
                    ));
                    Ok(())
                })
                .unwrap();
        }
        rows
    }

    /// Full-row drain through a prefetcher.
    fn drain_full(pf: &mut BlockPrefetcher) -> Vec<FullRow> {
        let mut rows = Vec::new();
        while let Some(block) = pf.next_decoded().unwrap() {
            block
                .for_each_row(|v| {
                    rows.push((
                        v.key.to_vec(),
                        v.value.map(|x| x.to_vec()),
                        v.sequence,
                        v.op_type as u8,
                    ));
                    Ok(())
                })
                .unwrap();
        }
        rows
    }

    /// G0 byte-equality: the compaction-mode windowed drain yields exactly
    /// the demand-paged rows for BOTH block formats (v1 Arrow / v2 KV) ×
    /// compression (None / LZ4), across window sizes that do and don't
    /// divide the block count.
    #[test]
    fn compaction_windowed_drain_equals_demand_all_formats() {
        for kv in [false, true] {
            for compression in [CompressionType::None, CompressionType::Lz4] {
                let data = build_sst_with(400, compression, Some(kv));
                let (reader, _file) = open_reader(&data, None, &data);
                assert!(
                    reader.index_entry_count() >= 8,
                    "need a multi-block SST (kv={kv}, {compression:?})"
                );
                let expected = drain_full_demand(&reader);
                for window_blocks in [3u32, 32] {
                    let mut pf =
                        BlockPrefetcher::for_compaction(Arc::clone(&reader), window_blocks);
                    let got = drain_full(&mut pf);
                    assert_eq!(
                        got, expected,
                        "kv={kv} compression={compression:?} window={window_blocks}"
                    );
                }
            }
        }
    }

    /// G0 Skip-policy cache assertion: a compaction-mode drain performs ZERO
    /// cache inserts (read-only cache use), while serving pre-warmed blocks
    /// from the cache (cache-first check preserved — the 27ae792c3 lesson)
    /// with correspondingly less I/O.
    #[test]
    fn compaction_mode_never_inserts_but_serves_cache_hits() {
        let data = build_sst_with(400, CompressionType::Lz4, Some(true));
        let cache = Arc::new(RecordingCache::new());
        let (reader, file) = open_reader(&data, Some(cache.clone()), &data);
        let blocks = reader.index_entry_count();
        assert!(blocks >= 8);

        // Pre-warm two blocks (as a foreground probe / flush-read would).
        for idx in [1usize, 5] {
            let (off, size) = reader.block_region(idx).unwrap();
            let d = reader.read_decoded_block(off, size).unwrap();
            reader.cache_insert_decoded(off, &d, CachePriority::Low);
        }
        let inserts_before = cache.inserts.lock().unwrap().len();
        let reads_before = file.reads.load(Ordering::SeqCst);

        let mut pf = BlockPrefetcher::for_compaction(Arc::clone(&reader), 4);
        let got = drain_full(&mut pf);

        // Compaction reads must NOT insert (Skip policy): insert count is
        // exactly the pre-warm count, nothing more.
        assert_eq!(
            cache.inserts.lock().unwrap().len(),
            inserts_before,
            "compaction-mode drain must perform ZERO cache inserts"
        );
        // The pre-warmed blocks were served from cache (W5 telemetry) ...
        assert_eq!(pf.compaction_stats().cache_hits, 2);
        // ... and rows are still exactly the demand-paged reference.
        let (reader_ref, _f) = open_reader(&data, None, &data);
        assert_eq!(got, drain_full_demand(&reader_ref));
        // I/O happened (the misses) but is bounded by the non-cached blocks.
        let preads = file.reads.load(Ordering::SeqCst) - reads_before;
        assert!(preads > 0 && (preads as usize) < blocks, "preads={preads}");
    }

    /// L4 §2.2: compaction mode uses a FIXED window from block 0 — no cold
    /// phase, no ramp doubling — and (falsifier 1) 100 % of blocks arrive
    /// via windows, zero via the demand fallback.
    #[test]
    fn compaction_mode_fixed_window_no_ramp_all_blocks_windowed() {
        let data = build_sst(400);
        let (reader, _file) = open_reader(&data, None, &data);
        let blocks = reader.index_entry_count();
        let window: u32 = 4;
        let mut pf = BlockPrefetcher::for_compaction(Arc::clone(&reader), window);
        // First window already in flight at construction (overlaps block 0).
        assert_eq!(pf.ra_blocks(), window, "fixed window from the start");
        let mut n = 0usize;
        while pf.next_decoded().unwrap().is_some() {
            n += 1;
            assert_eq!(pf.ra_blocks(), window, "window never ramps/doubles");
        }
        assert_eq!(n, blocks);
        let stats = pf.compaction_stats();
        assert_eq!(stats.window_blocks, blocks as u64, "all blocks windowed");
        assert_eq!(stats.demand_blocks, 0, "demand fallback never used");
        assert_eq!(
            stats.window_submits,
            (blocks as u64).div_ceil(window as u64),
            "ceil(blocks/window) submissions"
        );
        assert!(stats.prefetched_bytes > 0);
        assert_eq!(stats.cache_hits, 0, "no cache attached");
    }

    /// Compaction-mode termination parity with scan mode: dropping/terminating
    /// mid-stream releases the M3 charge and parks at EOF.
    #[test]
    fn compaction_mode_terminate_parks_at_eof() {
        let data = build_sst(400);
        let (reader, _file) = open_reader(&data, None, &data);
        let mut pf = BlockPrefetcher::for_compaction(Arc::clone(&reader), 8);
        assert!(pf.next_decoded().unwrap().is_some());
        pf.terminate();
        assert!(pf.next_decoded().unwrap().is_none());
    }

    /// FRS-SCAN-OPEN-FANOUT: `prime_opens_concurrent` runs EVERY submitted job
    /// and the barrier waits for all of them (more jobs than pool workers ⇒ the
    /// barrier still completes; all run exactly once).
    #[test]
    fn prime_opens_concurrent_runs_all_and_barriers() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counter = Arc::new(AtomicUsize::new(0));
        // More jobs than the pool width (clamp(cores/2,2,6) ≤ 6) to prove the
        // barrier handles a fanout wider than the pool.
        let n = 20usize;
        let mut jobs: Vec<Box<dyn FnOnce() + Send + 'static>> = Vec::with_capacity(n);
        for _ in 0..n {
            let c = Arc::clone(&counter);
            jobs.push(Box::new(move || {
                c.fetch_add(1, Ordering::SeqCst);
            }));
        }
        prime_opens_concurrent(jobs);
        // Barrier returned ⇒ every job has completed exactly once.
        assert_eq!(counter.load(Ordering::SeqCst), n);
    }

    /// FRS-SCAN-OPEN-FANOUT: an empty job list is a no-op (zero pool work) and
    /// returns immediately — the "nothing to fan out" fast path.
    #[test]
    fn prime_opens_concurrent_empty_is_noop() {
        let jobs: Vec<Box<dyn FnOnce() + Send + 'static>> = Vec::new();
        prime_opens_concurrent(jobs); // must return without blocking
    }

    /// FRS-SCAN-OPEN-FANOUT: a PANICKING job does not strand the barrier — the
    /// `Signal` guard fires during the unwind, so a sibling job's completion is
    /// still observed and the barrier returns (no hang).
    #[test]
    fn prime_opens_concurrent_panicking_job_does_not_strand_barrier() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counter = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&counter);
        let jobs: Vec<Box<dyn FnOnce() + Send + 'static>> = vec![
            Box::new(|| panic!("deliberate open-fanout test panic")),
            Box::new(move || {
                c.fetch_add(1, Ordering::SeqCst);
            }),
        ];
        // Must NOT hang: both job slots signal completion (the panicking one via
        // the guard's unwind drop).
        prime_opens_concurrent(jobs);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
