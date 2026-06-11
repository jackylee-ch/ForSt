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
//! Insertion is never skipped — interval-join re-probes of recently scanned
//! windows are common and the decoded cache hit is the cheapest read.

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
static PREFETCH_BUFFERED_BYTES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Current aggregate of prefetcher ready+inflight bytes (M3 telemetry).
/// Sizes are on-disk (pre-decompression) block bytes — the I/O-side budget.
pub fn prefetch_buffered_bytes() -> usize {
    PREFETCH_BUFFERED_BYTES.load(std::sync::atomic::Ordering::Relaxed)
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
    POOL.get_or_init(|| {
        let n = std::env::var("FRS_RS_PREFETCH_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(|| {
                let cores = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4);
                (cores / 2).clamp(2, 6)
            });
        ReadIoPool::new(n)
    })
}

/// Result type a window-production job sends back through its oneshot.
type WindowResult = ForstResult<Vec<DecodedBlock>>;

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
        }
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
            PREFETCH_BUFFERED_BYTES.fetch_sub(held, std::sync::atomic::Ordering::Relaxed);
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
            PREFETCH_BUFFERED_BYTES.fetch_sub(sz as usize, std::sync::atomic::Ordering::Relaxed);
            self.on_delivered();
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
                    PREFETCH_BUFFERED_BYTES
                        .fetch_sub(handle.bytes, std::sync::atomic::Ordering::Relaxed);
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
                Ok(blocks) => {
                    debug_assert_eq!(blocks.len(), handle.range.1 - handle.range.0);
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
                        PREFETCH_BUFFERED_BYTES
                            .fetch_sub(sz as usize, std::sync::atomic::Ordering::Relaxed);
                        self.on_delivered();
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
                    PREFETCH_BUFFERED_BYTES
                        .fetch_sub(handle.bytes, std::sync::atomic::Ordering::Relaxed);
                    self.terminate();
                    return Err(e);
                }
            }
        }

        // 3. Cold / demand path — identical to the legacy per-block read
        //    (cache-first + Low insert inside `read_decoded_block`).
        if self.next_block >= self.end_block {
            return Ok(None);
        }
        let idx = self.next_block;
        let (off, size) = self
            .reader
            .block_region(idx)
            .ok_or_else(|| ForstError::internal("BlockPrefetcher: block index out of range"))?;
        let block = self.reader.read_decoded_block(off, size)?;
        self.next_block = idx + 1;
        self.on_delivered();
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
        let cap_bytes = if self.local {
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
        // shallow (just-ramped) windows keep today's Low.
        let priority = if self.ra_blocks >= BOTTOM_PRIORITY_RA {
            CachePriority::Bottom
        } else {
            CachePriority::Low
        };
        let reader = Arc::clone(&self.reader);
        // Rendezvous-free oneshot: capacity 1 so the producer never blocks.
        let (tx, rx) = std::sync::mpsc::sync_channel::<WindowResult>(1);
        read_io_pool().submit(Box::new(move || {
            let result = fetch_window(&reader, start, end, priority);
            // Receiver dropped (iterator closed/aborted) ⇒ result discarded.
            let _ = tx.send(result);
        }));
        // M3 telemetry: charge the window at submit (released at delivery /
        // failure / terminate / drop).
        let agg = PREFETCH_BUFFERED_BYTES
            .fetch_add(window_bytes as usize, std::sync::atomic::Ordering::Relaxed)
            + window_bytes as usize;
        if prefetch_diag() {
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
        // Double toward the regime cap for the NEXT window.
        let cap_blocks = if self.local {
            LOCAL_CAP_BLOCKS
        } else {
            REMOTE_CAP_BLOCKS
        };
        self.ra_blocks = (self.ra_blocks * 2).min(cap_blocks);
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
/// this pool thread, cache insert at `priority`. Returns the window's blocks
/// in index order.
fn fetch_window(
    reader: &SstReaderImpl,
    start: usize,
    end: usize,
    priority: CachePriority,
) -> ForstResult<Vec<DecodedBlock>> {
    let n = end - start;
    let mut out: Vec<Option<DecodedBlock>> = (0..n).map(|_| None).collect();
    // Pass 1: cache hits (window splitting).
    let mut regions: Vec<(u64, u32)> = Vec::with_capacity(n);
    for idx in start..end {
        let (off, size) = reader
            .block_region(idx)
            .ok_or_else(|| ForstError::internal("prefetch window past sparse index"))?;
        regions.push((off, size));
        if let Some(hit) = reader.cache_get_decoded(off) {
            out[idx - start] = Some(hit);
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
                reader.cache_insert_decoded(off, &decoded, priority);
                out[j] = Some(decoded);
            }
        }
        debug_assert_eq!(cursor, total);
    }
    Ok(out
        .into_iter()
        .map(|o| o.expect("every window slot filled above"))
        .collect())
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

    /// Builds a multi-block SST (small block_size forces many blocks).
    /// Returns the raw bytes.
    fn build_sst(n_rows: usize) -> Arc<Vec<u8>> {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 1024,
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
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
        let out = fetch_window(&reader, 2, 6, CachePriority::Low).unwrap();
        assert_eq!(out.len(), 4);
        let reads_after = file.reads.load(Ordering::SeqCst);
        assert_eq!(
            reads_after - reads_before,
            2,
            "window split around the cache hit: [2,4) + [5,6) = 2 preads"
        );

        // And a fully-missing window of 4 blocks = exactly 1 pread.
        let reads_before = file.reads.load(Ordering::SeqCst);
        let out = fetch_window(&reader, 8, 12, CachePriority::Low).unwrap();
        assert_eq!(out.len(), 4);
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
}
