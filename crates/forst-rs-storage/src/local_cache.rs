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

//! Local LRU cache for SST files served out of remote object storage.
//!
//! [`LocalCache`] keeps a size-bounded set of fetched objects on the local
//! filesystem under a caller-supplied `cache_dir`. The cache key is the
//! logical path of the remote object (e.g. `/db/00000123.sst`). Each
//! cached object is stored as a single file under
//! `cache_dir/<sanitized-key>` whose contents are the byte-identical
//! payload of the remote object.
//!
//! # Design
//!
//! - **LRU policy**: tracked in software via a `VecDeque<String>` of keys
//!   ordered oldest→newest. On insert, if `current_bytes + new_size`
//!   exceeds `capacity_bytes`, evict from the front until it fits.
//! - **Concurrency**: a single `Mutex` guards both the LRU deque and the
//!   on-disk metadata map. Cache hits are served by reading the file off
//!   disk after dropping the lock; misses fall through to the OpenDAL
//!   layer above.
//! - **Persistence**: on construction, [`LocalCache::open`] scans the
//!   `cache_dir` and re-populates the in-memory map from existing files
//!   (using their on-disk size as the byte count and their mtime as the
//!   LRU position). This makes restarts cheap.
//! - **Capacity 0**: rejects all inserts. A zero-capacity cache behaves
//!   exactly like a pass-through (every read goes to the remote backend).
//!
//! # Why not bake into the engine read path?
//!
//! The engine's `RandomAccessFile` and `SequentialFile` traits are
//! filesystem-level. The cleanest place to interpose is at the
//! [`forst_rs_io::FileSystem`] layer — see
//! `forst-rs-io::CachedFileSystem` (P6) which composes a remote OpenDAL
//! backend with this `LocalCache` and presents a single `FileSystem` to
//! the engine. That keeps engine internals unchanged.

use std::collections::{HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::requester;

/// FRS-CACHE-ADMISSION (Phase-2 disagg, ForSt mechanism §2.1.1-2.1.3):
/// parameters of the *read-fill* admission policy. Mirrors ForSt's
/// `FileBasedCache` cold-list machinery: reads of uncached files accumulate
/// access counts and are only admitted (cached) after `access_before_promote`
/// touches; a key evicted `promote_limit`+ times is permanently blocked from
/// re-admission (the anti-thrash cap). Write-through puts (newly generated
/// SSTs) are NEVER gated — that is ForSt's "write-only admission" split.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionParams {
    /// Number of read-miss touches before a key's fill is admitted.
    /// ForSt: `accessBeforePromote` (`FileBasedCache.java:75-77,378-387`).
    pub access_before_promote: u32,
    /// A key evicted at least this many times is blocked from read-fill
    /// re-admission. ForSt: `promoteLimit` (`FileBasedCache.java:79-82`).
    pub promote_limit: u32,
    /// Bound on tracked cold/evicted keys (FIFO aging — the cheap stand-in
    /// for ForSt's epoch-decayed cold-list counts; old keys fall out of the
    /// tracker, which both bounds memory and decays stale counts).
    pub tracker_cap: usize,
}

impl Default for AdmissionParams {
    fn default() -> Self {
        Self {
            access_before_promote: 2,
            promote_limit: 3,
            tracker_cap: 65_536,
        }
    }
}

/// Cache behavior policy, flag-gated and default-OFF (legacy behavior is
/// byte-identical when both fields are off — the standing Phase-2 rule).
#[derive(Clone, Copy, Debug, Default)]
pub struct CachePolicy {
    /// FRS-CACHE-BG-EXEMPT (ForSt §2.1.4): when `true`, accesses from threads
    /// marked background ([`crate::requester`]) do NOT promote LRU order, do
    /// NOT count toward admission, and are excluded from the foreground
    /// hit-rate stats — so compaction scans cannot evict the operator hot set.
    pub background_exempt: bool,
    /// `Some(_)` enables read-fill admission gating (see [`AdmissionParams`]).
    /// `None` = legacy: every read miss may populate the cache.
    pub admission: Option<AdmissionParams>,
}

impl CachePolicy {
    /// Builds the policy from environment flags (both default OFF):
    /// - `FRS_CACHE_BG_EXEMPT=1` → background-thread exemption,
    /// - `FRS_CACHE_ADMISSION=1` → read-fill admission gating, tuned by
    ///   `FRS_CACHE_ADMISSION_PROMOTE` (touches before admit, default 2),
    ///   `FRS_CACHE_ADMISSION_EVICT_LIMIT` (evictions before permanent
    ///   block, default 3), `FRS_CACHE_ADMISSION_TRACKER_CAP` (default 65536).
    pub fn from_env() -> Self {
        fn flag(name: &str) -> bool {
            matches!(
                std::env::var(name).ok().as_deref(),
                Some("1") | Some("true") | Some("TRUE")
            )
        }
        fn num<T: std::str::FromStr>(name: &str, default: T) -> T {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }
        let admission = if flag("FRS_CACHE_ADMISSION") {
            let d = AdmissionParams::default();
            Some(AdmissionParams {
                access_before_promote: num("FRS_CACHE_ADMISSION_PROMOTE", d.access_before_promote)
                    .max(1),
                promote_limit: num("FRS_CACHE_ADMISSION_EVICT_LIMIT", d.promote_limit).max(1),
                tracker_cap: num("FRS_CACHE_ADMISSION_TRACKER_CAP", d.tracker_cap).max(16),
            })
        } else {
            None
        };
        Self {
            background_exempt: flag("FRS_CACHE_BG_EXEMPT"),
            admission,
        }
    }
}

/// Bounded tracker behind the admission policy: per-key read-miss access
/// counts (the "cold list") and per-key eviction counts (the thrash signal).
/// Both maps age FIFO at `cap` entries — a dropped key simply restarts its
/// count on the next touch (decay), and a dropped eviction record unblocks
/// the key (acceptable: permanent blocking only needs to hold while the key
/// is actively thrashing, which keeps its record fresh).
#[derive(Default)]
struct AdmissionTracker {
    counts: HashMap<String, u32>,
    counts_order: VecDeque<String>,
    evictions: HashMap<String, u32>,
    evictions_order: VecDeque<String>,
}

impl AdmissionTracker {
    /// Drops oldest entries until `map` is under `cap`. Order refs whose key
    /// is no longer present (already removed on admit) are skipped.
    fn trim(map: &mut HashMap<String, u32>, order: &mut VecDeque<String>, cap: usize) {
        while map.len() > cap {
            match order.pop_front() {
                Some(old) => {
                    map.remove(&old);
                }
                None => break,
            }
        }
        // PMC self-review (2026-06-12): an ADMITTED key is removed from `map`
        // but its `order` ref survives — without this reclaim the deque grows
        // by one String per admitted key forever (map stays under `cap`, so
        // the loop above never runs). `trim` is called once per NEW tracked
        // key (the only path that grows `order`), so checking here bounds the
        // deque to ≤ 2×cap with amortized-O(1) cost.
        if order.len() > cap.saturating_mul(2) {
            order.retain(|k| map.contains_key(k));
        }
    }

    /// Records one foreground read-miss touch of `key`; returns `true` when
    /// the touch reaches `access_before_promote` (the fill is admitted and the
    /// key's count is cleared).
    fn touch_and_should_admit(&mut self, key: &str, p: &AdmissionParams) -> bool {
        if self.evictions.get(key).copied().unwrap_or(0) >= p.promote_limit {
            return false; // blocked: thrashing key (ForSt promoteLimit)
        }
        match self.counts.get_mut(key) {
            Some(c) => {
                *c += 1;
                if *c >= p.access_before_promote {
                    self.counts.remove(key);
                    // Stale counts_order ref is skipped by trim later.
                    return true;
                }
                false
            }
            None => {
                if p.access_before_promote <= 1 {
                    return true;
                }
                self.counts.insert(key.to_string(), 1);
                self.counts_order.push_back(key.to_string());
                Self::trim(&mut self.counts, &mut self.counts_order, p.tracker_cap);
                false
            }
        }
    }

    /// Records that `key` was evicted from the cache.
    fn record_eviction(&mut self, key: &str, p: &AdmissionParams) {
        match self.evictions.get_mut(key) {
            Some(c) => *c = c.saturating_add(1),
            None => {
                self.evictions.insert(key.to_string(), 1);
                self.evictions_order.push_back(key.to_string());
                Self::trim(
                    &mut self.evictions,
                    &mut self.evictions_order,
                    p.tracker_cap,
                );
            }
        }
    }
}

/// Per-entry bookkeeping. `bytes` is the on-disk size; the LRU position
/// is implied by membership in the `lru` deque.
#[derive(Clone, Debug)]
struct Entry {
    bytes: u64,
    /// Generation of this key's most-recent LRU push. An `lru` deque entry
    /// `(key, g)` is the LIVE recency reference iff `entries[key].gen == g`;
    /// any older `(key, g')` left in the deque is stale and skipped on
    /// eviction. This makes `touch` O(1) (stamp + push, no scan/remove).
    gen: u64,
}

/// LRU file-cache backed by `cache_dir` on the local filesystem.
///
/// Thread-safe: every public method takes `&self`. A single `Mutex`
/// guards the LRU + map; on-disk reads/writes are performed AFTER the
/// guard is released to keep the critical section short.
pub struct LocalCache {
    cache_dir: PathBuf,
    capacity_bytes: u64,
    inner: Mutex<Inner>,
    /// Cache-hit counter (lock-free). A "hit" is a `get` that returns
    /// `Some(_)`. Sized to validate the concurrent-read fix on S3 — a low
    /// hit rate means cold scans dominate and concurrency pays off.
    hits: AtomicU64,
    /// Cache-miss counter: a `get` that returns `None`.
    misses: AtomicU64,
    /// Total `get` calls; used to throttle periodic stats logging.
    gets: AtomicU64,
    /// FRS-FDCACHE: bounded cache of OPEN read file handles keyed by logical
    /// cache key. The q7 sampled profile showed ~35% of the heavy-join hot
    /// thread in the `open()` syscall — `get_range` opened+closed the cache
    /// file on EVERY block read. SST cache files are write-once-immutable, so a
    /// retained read fd always returns consistent bytes; reusing it across the
    /// many block reads of one SST removes the per-block open/close syscalls.
    /// Purged on entry eviction / overwrite / invalidation so a cached fd never
    /// outlives (or aliases a new inode of) its on-disk file.
    fd_cache: Mutex<FdCache>,
    /// Count of ACTUAL `open()` calls made by `get_range` (a cache miss). A
    /// low value relative to block reads validates the fd-cache hit rate.
    fd_opens: AtomicU64,
    /// FRS-CACHE-BG-EXEMPT / FRS-CACHE-ADMISSION policy (default OFF ⇒
    /// byte-identical legacy behavior).
    policy: CachePolicy,
    /// Cold/eviction tracker behind the admission policy. Only locked when
    /// `policy.admission` is `Some(_)`.
    admission: Mutex<AdmissionTracker>,
    /// Background-thread hits/misses (recorded separately so the headline
    /// hit rate reflects the FOREGROUND working set only — ForSt §2.1.4).
    /// Always 0 unless `policy.background_exempt`.
    bg_hits: AtomicU64,
    bg_misses: AtomicU64,
    /// Read-fill admission outcomes (admitted / rejected). Always 0 unless
    /// `policy.admission` is `Some(_)`.
    admitted: AtomicU64,
    rejected: AtomicU64,
}

/// Bounded FIFO cache of open read file handles. `order` records insertion
/// order; stale refs (for keys since removed) are skipped on eviction. No
/// per-hit reorder — the working set (a handful of hot SSTs) sits well under
/// `cap`, so FIFO aging is sufficient and keeps `get` O(1) lock-free of churn.
struct FdCache {
    map: HashMap<String, Arc<fs::File>>,
    order: VecDeque<String>,
    cap: usize,
}

impl FdCache {
    fn new(cap: usize) -> Self {
        FdCache {
            map: HashMap::new(),
            order: VecDeque::new(),
            cap: cap.max(1),
        }
    }

    /// Returns a clone of the cached handle for `key`, if present.
    fn get(&self, key: &str) -> Option<Arc<fs::File>> {
        self.map.get(key).cloned()
    }

    /// Inserts `file` under `key`, evicting the oldest live handle if at
    /// capacity. If another thread already inserted `key` (open race), the
    /// EXISTING handle is kept and returned so all readers share one fd.
    fn insert(&mut self, key: String, file: Arc<fs::File>) -> Arc<fs::File> {
        if let Some(existing) = self.map.get(&key) {
            return existing.clone();
        }
        while self.map.len() >= self.cap {
            match self.order.pop_front() {
                Some(old) => {
                    // Skip stale order refs (key already removed).
                    if self.map.remove(&old).is_some() {
                        break;
                    }
                }
                None => break,
            }
        }
        self.map.insert(key.clone(), Arc::clone(&file));
        self.order.push_back(key);
        file
    }

    fn remove(&mut self, key: &str) {
        self.map.remove(key);
        // Leave the (now stale) `order` ref; it is reclaimed lazily on evict.
    }
}

#[derive(Default)]
struct Inner {
    /// Map of logical key -> entry metadata.
    entries: HashMap<String, Entry>,
    /// LRU order as generation-stamped references (front = oldest,
    /// back = most-recently-used). May contain STALE refs whose `gen` no
    /// longer matches `entries[key].gen` (superseded by a later touch or a
    /// removed entry); these are skipped on eviction and dropped by
    /// `compact`. Lazy stamping keeps `touch` O(1).
    lru: VecDeque<(String, u64)>,
    /// Sum of `entries[*].bytes`. Updated atomically with the map.
    current_bytes: u64,
    /// Monotonic generation counter; each push takes `next_gen()`.
    gen_counter: u64,
    /// Approximate count of stale `lru` refs, used only to decide WHEN to
    /// compact (correctness never depends on its accuracy — the `gen`
    /// comparison is authoritative).
    stale: usize,
}

impl Inner {
    fn next_gen(&mut self) -> u64 {
        self.gen_counter = self.gen_counter.wrapping_add(1);
        self.gen_counter
    }

    /// Marks `key` most-recently-used in O(1): stamp a fresh generation on
    /// the entry and push `(key, gen)`. The entry's previous `lru` ref
    /// becomes stale (skipped on eviction). Caller holds the lock and has
    /// confirmed membership.
    fn touch_lru(&mut self, key: &str) {
        let g = self.next_gen();
        let present = if let Some(e) = self.entries.get_mut(key) {
            e.gen = g;
            true
        } else {
            false
        };
        if present {
            self.stale += 1; // the prior (key, old_gen) ref is now stale
            self.lru.push_back((key.to_string(), g));
            self.maybe_compact();
        }
    }

    /// Records that `key`'s current `lru` ref just became stale (the entry was
    /// removed or its bytes superseded by a fresh push). O(1) — no scan; the
    /// stale ref is reclaimed lazily on eviction/compaction.
    fn mark_stale(&mut self) {
        self.stale += 1;
        self.maybe_compact();
    }

    /// Rebuilds `lru` keeping only the live ref per key (preserving order)
    /// when stale refs dominate, bounding deque growth to ~one ref per entry.
    /// Amortized O(1) per touch (runs at most every ~N touches).
    fn maybe_compact(&mut self) {
        if self.stale > 64 && self.stale.saturating_mul(2) > self.lru.len() {
            let mut fresh = VecDeque::with_capacity(self.entries.len());
            for (k, g) in std::mem::take(&mut self.lru) {
                if self.entries.get(&k).is_some_and(|e| e.gen == g) {
                    fresh.push_back((k, g));
                }
            }
            self.lru = fresh;
            self.stale = 0;
        }
    }
}

/// Positional read (`pread`) of `buf.len()` bytes at `offset` from `file`,
/// without disturbing any file cursor. Returns the bytes read (may be short
/// at EOF). On Unix this is a single `pread(2)`; elsewhere it falls back to a
/// seek+read (acceptable: the cache is only used on Unix targets in practice).
#[cfg(unix)]
fn pread(file: &fs::File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

#[cfg(not(unix))]
fn pread(file: &fs::File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = file.try_clone()?;
    f.seek(SeekFrom::Start(offset))?;
    f.read(buf)
}

impl LocalCache {
    /// Opens (or initializes) a cache rooted at `cache_dir` with the
    /// given byte budget. Creates the directory if it does not exist.
    /// Existing files in `cache_dir` are adopted into the cache (their
    /// on-disk size counts toward `current_bytes`); files exceeding the
    /// new capacity are evicted in arbitrary order on first overflow.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from creating or reading the directory.
    pub fn open(cache_dir: impl Into<PathBuf>, capacity_bytes: u64) -> io::Result<Self> {
        Self::open_with_policy(cache_dir, capacity_bytes, CachePolicy::from_env())
    }

    /// [`open`](Self::open) with an explicit [`CachePolicy`] (tests and
    /// callers that configure programmatically instead of via env flags).
    pub fn open_with_policy(
        cache_dir: impl Into<PathBuf>,
        capacity_bytes: u64,
        policy: CachePolicy,
    ) -> io::Result<Self> {
        let cache_dir = cache_dir.into();
        fs::create_dir_all(&cache_dir)?;

        let mut inner = Inner::default();
        for entry in fs::read_dir(&cache_dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if !meta.is_file() {
                continue;
            }
            let file_name = match entry.file_name().into_string() {
                Ok(s) => s,
                // Skip non-UTF-8 file names; they cannot have come from us.
                Err(_) => continue,
            };
            if file_name.starts_with(".tmp-") {
                let _ = fs::remove_file(entry.path());
                continue;
            }
            let key = unsanitize_key(&file_name);
            let bytes = meta.len();
            let g = inner.next_gen();
            inner.entries.insert(key.clone(), Entry { bytes, gen: g });
            inner.lru.push_back((key, g));
            inner.current_bytes = inner.current_bytes.saturating_add(bytes);
        }

        Ok(Self {
            cache_dir,
            capacity_bytes,
            inner: Mutex::new(inner),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            gets: AtomicU64::new(0),
            // 512 open fds: ample for the hot working set (a handful of
            // overlapping SSTs) while staying well under the JVM's raised fd
            // limit. Evicted FIFO; stale entries purged on removal.
            fd_cache: Mutex::new(FdCache::new(512)),
            fd_opens: AtomicU64::new(0),
            policy,
            admission: Mutex::new(AdmissionTracker::default()),
            bg_hits: AtomicU64::new(0),
            bg_misses: AtomicU64::new(0),
            admitted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        })
    }

    /// Returns the active [`CachePolicy`].
    pub fn policy(&self) -> CachePolicy {
        self.policy
    }

    /// FRS-CACHE-BG-EXEMPT: `true` when the CURRENT access should be treated
    /// as foreground for LRU/stats/admission purposes. Always `true` unless
    /// the policy opts in AND the calling thread is marked background.
    #[inline]
    fn foreground(&self) -> bool {
        !(self.policy.background_exempt && requester::is_background_thread())
    }

    /// FRS-CACHE-ADMISSION: decides whether a READ-MISS fill of `key` may be
    /// admitted into the cache (callers skip their `put` on `false`).
    ///
    /// - Admission disabled (`policy.admission == None`): always `true`
    ///   (legacy behavior).
    /// - Background-exempt thread: `false` without counting — compaction
    ///   reads neither admit nor accumulate promotion credit (ForSt §2.1.4).
    /// - Otherwise ForSt's count-to-promote: the key's foreground miss count
    ///   must reach `access_before_promote`, and a key evicted
    ///   `promote_limit`+ times is blocked (anti-thrash cap, §2.1.1-2.1.2).
    ///
    /// Write-through puts (newly generated SSTs) must NOT consult this —
    /// they call [`put`](Self::put) directly (write-only admission split).
    pub fn admit_read_fill(&self, key: &str) -> bool {
        let Some(params) = self.policy.admission else {
            return true;
        };
        if !self.foreground() {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let admit = self
            .admission
            .lock()
            .expect("admission tracker mutex poisoned")
            .touch_and_should_admit(key, &params);
        if admit {
            self.admitted.fetch_add(1, Ordering::Relaxed);
        } else {
            self.rejected.fetch_add(1, Ordering::Relaxed);
        }
        admit
    }

    /// FRS-CACHE-ADMISSION restore pre-seed (ForSt §2.1.6,
    /// `FileBasedCache.registerInCache`): primes the admission tracker for a
    /// file known to belong to the restored working set, so its FIRST
    /// foreground touch reaches the count-to-promote threshold and loads it
    /// back into the cache ASAP — pairs with instant-link restore (Stage 3:
    /// link first, cache warms by-demand-but-eagerly). The bytes are NOT
    /// fetched here; only the counter is seeded (count = threshold − 1).
    /// No-op when admission is disabled or the key is blocked/already ahead.
    pub fn pre_seed_admission(&self, key: &str) {
        let Some(params) = self.policy.admission else {
            return;
        };
        if params.access_before_promote <= 1 {
            return; // first touch admits anyway
        }
        let seed = params.access_before_promote - 1;
        let mut guard = self
            .admission
            .lock()
            .expect("admission tracker mutex poisoned");
        let tracker = &mut *guard; // split-borrow fields through the guard
        if tracker.evictions.get(key).copied().unwrap_or(0) >= params.promote_limit {
            return; // blocked keys are not resurrected by a restore seed
        }
        match tracker.counts.get_mut(key) {
            Some(c) => *c = (*c).max(seed),
            None => {
                tracker.counts.insert(key.to_string(), seed);
                tracker.counts_order.push_back(key.to_string());
                AdmissionTracker::trim(
                    &mut tracker.counts,
                    &mut tracker.counts_order,
                    params.tracker_cap,
                );
            }
        }
    }

    /// Returns `(admitted, rejected)` read-fill admission decisions so far
    /// (both 0 when admission is disabled).
    pub fn admission_stats(&self) -> (u64, u64) {
        (
            self.admitted.load(Ordering::Relaxed),
            self.rejected.load(Ordering::Relaxed),
        )
    }

    /// Returns `(hits, misses)` observed from BACKGROUND-class threads (both
    /// 0 unless `background_exempt` is on).
    pub fn bg_stats(&self) -> (u64, u64) {
        (
            self.bg_hits.load(Ordering::Relaxed),
            self.bg_misses.load(Ordering::Relaxed),
        )
    }

    /// FRS-FDCACHE: returns a shared open read handle for `key`, opening (and
    /// caching) the file on a miss. Returns `Ok(None)` if the on-disk file is
    /// gone (caller treats as a cache miss). The open happens OUTSIDE the
    /// `inner` lock so disk I/O never serializes concurrent readers.
    fn get_or_open_fd(&self, key: &str, on_disk: &Path) -> io::Result<Option<Arc<fs::File>>> {
        if let Some(f) = self
            .fd_cache
            .lock()
            .expect("fd cache mutex poisoned")
            .get(key)
        {
            return Ok(Some(f));
        }
        let file = match OpenOptions::new().read(true).open(on_disk) {
            Ok(f) => Arc::new(f),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        self.fd_opens.fetch_add(1, Ordering::Relaxed);
        let arc = self
            .fd_cache
            .lock()
            .expect("fd cache mutex poisoned")
            .insert(key.to_string(), file);
        Ok(Some(arc))
    }

    /// FRS-FDCACHE: drop any cached open handle for `key`. Called whenever the
    /// on-disk file is removed or replaced so a stale fd is never reused.
    fn purge_fd(&self, key: &str) {
        self.fd_cache
            .lock()
            .expect("fd cache mutex poisoned")
            .remove(key);
    }

    /// Returns `(hits, misses)` observed by [`get`](Self::get) so far.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }

    /// Records a hit/miss and emits a periodic `FRS-CACHE-STATS` line every
    /// 100k gets. Lock-free; called on every `get`. `fg=false` accesses
    /// (background threads under FRS-CACHE-BG-EXEMPT) are counted separately
    /// so the headline hit rate reflects the foreground working set only.
    fn record_get(&self, hit: bool, fg: bool) {
        match (fg, hit) {
            (true, true) => self.hits.fetch_add(1, Ordering::Relaxed),
            (true, false) => self.misses.fetch_add(1, Ordering::Relaxed),
            (false, true) => self.bg_hits.fetch_add(1, Ordering::Relaxed),
            (false, false) => self.bg_misses.fetch_add(1, Ordering::Relaxed),
        };
        let n = self.gets.fetch_add(1, Ordering::Relaxed) + 1;
        if n.is_multiple_of(100_000) {
            let hits = self.hits.load(Ordering::Relaxed);
            let misses = self.misses.load(Ordering::Relaxed);
            let total = hits + misses;
            let rate = if total == 0 {
                0.0
            } else {
                (hits as f64 / total as f64) * 100.0
            };
            let bg_hits = self.bg_hits.load(Ordering::Relaxed);
            let bg_misses = self.bg_misses.load(Ordering::Relaxed);
            let (admitted, rejected) = self.admission_stats();
            eprintln!(
                "FRS-CACHE-STATS: hits={hits} misses={misses} hit_rate={rate:.1}% \
                 bg_hits={bg_hits} bg_misses={bg_misses} admit={admitted} reject={rejected}"
            );
        }
    }

    /// Returns the cache directory.
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Returns the capacity in bytes.
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Returns the current on-disk usage in bytes.
    pub fn current_bytes(&self) -> u64 {
        self.inner
            .lock()
            .expect("local cache mutex poisoned")
            .current_bytes
    }

    /// Returns the number of cached entries.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("local cache mutex poisoned")
            .entries
            .len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if `key` is currently cached.
    pub fn contains(&self, key: &str) -> bool {
        self.inner
            .lock()
            .expect("local cache mutex poisoned")
            .entries
            .contains_key(key)
    }

    /// FRS-FDCACHE: number of ACTUAL `open()` syscalls made by `get_range`
    /// (cache misses). A low value relative to block reads confirms the fd
    /// cache is eliminating the per-block open/close that dominated the q7
    /// sampled profile.
    pub fn fd_opens(&self) -> u64 {
        self.fd_opens.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn fd_cache_len(&self) -> usize {
        self.fd_cache
            .lock()
            .expect("fd cache mutex poisoned")
            .map
            .len()
    }

    #[cfg(test)]
    fn fd_cache_contains(&self, key: &str) -> bool {
        self.fd_cache
            .lock()
            .expect("fd cache mutex poisoned")
            .map
            .contains_key(key)
    }

    /// FRS-LOCAL-FIRST-SST (2026-06-01): the byte length of the cached entry
    /// for `key`, or `None` on a miss. Used by the local-first SST read path to
    /// size a `RandomAccessFile` over the write-through copy WITHOUT a remote
    /// `await_upload` round-trip. Does NOT bump the LRU (it is a metadata-only
    /// probe issued at reader-open time, not a data access).
    pub fn entry_size(&self, key: &str) -> Option<u64> {
        self.inner
            .lock()
            .expect("local cache mutex poisoned")
            .entries
            .get(key)
            .map(|e| e.bytes)
    }

    /// Reads the cached bytes for `key`. Returns `Ok(None)` on miss.
    /// On hit, marks `key` as most-recently-used (foreground accesses only
    /// under FRS-CACHE-BG-EXEMPT — background reads must not reorder the LRU).
    pub fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let fg = self.foreground();
        // Critical section: check membership + bump LRU. Read the file
        // outside the lock so concurrent gets don't serialize on disk I/O.
        let on_disk = {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");
            if !inner.entries.contains_key(key) {
                self.record_get(false, fg);
                return Ok(None);
            }
            if fg {
                inner.touch_lru(key);
            }
            self.path_for(key)
        };

        match fs::read(&on_disk) {
            Ok(bytes) => {
                self.record_get(true, fg);
                Ok(Some(bytes))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                self.record_get(false, fg);
                // The metadata says we have it but the file is gone — surface
                // as a miss rather than an error so callers can re-fetch.
                self.drop_entry(key);
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Removes `key` from the in-memory metadata (map + LRU), decrementing the
    /// byte accounting. Used when an on-disk file vanished underneath a cached
    /// entry so the next access re-fetches rather than serving a phantom hit.
    fn drop_entry(&self, key: &str) {
        {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");
            if let Some(entry) = inner.entries.remove(key) {
                inner.current_bytes = inner.current_bytes.saturating_sub(entry.bytes);
                // The entry's live lru ref is now stale; reclaimed lazily (O(1)).
                inner.mark_stale();
            }
        }
        // FRS-FDCACHE: the on-disk file is being removed — purge any cached fd.
        self.purge_fd(key);
    }

    /// Reads exactly the `[offset, offset+len)` sub-range of the cached file
    /// for `key` WITHOUT materializing the whole file — a positional `pread`.
    ///
    /// This is the rocksdb-parity read primitive for the heavy-join hot path.
    /// A data-block read needs ~16 KiB; [`get`](Self::get) would `fs::read` the
    /// entire (up to 64 MiB whole-SST / 1 MiB chunk) cached file to slice out
    /// those 16 KiB — a 64×+ read amplification (in-kernel memcpy + heap alloc)
    /// paid per block, per overlapping SST, per probe. `get_range` reads only
    /// the bytes asked for.
    ///
    /// Returns `Ok(None)` on a cache miss (caller re-fetches), bumping the LRU
    /// on a hit exactly like `get`. Sound ONLY for write-once files (SSTs): a
    /// fixed `(key, offset)` maps to immutable bytes, so a positional read is
    /// always consistent. The returned vec may be SHORTER than `len` if the
    /// file ends before `offset+len`; the caller validates the length and
    /// falls back on a short read (defends against a poisoned/truncated entry).
    pub fn get_range(&self, key: &str, offset: u64, len: usize) -> io::Result<Option<Vec<u8>>> {
        if len == 0 {
            return Ok(Some(Vec::new()));
        }
        // Reuse the zero-second-copy path; existing owned-Vec callers pay the
        // one alloc + truncate here, while the hot SST read path uses
        // `get_range_into` directly (no intermediate Vec, no second copy).
        let mut buf = vec![0u8; len];
        match self.get_range_into(key, offset, &mut buf)? {
            Some(filled) => {
                buf.truncate(filled);
                Ok(Some(buf))
            }
            None => Ok(None),
        }
    }

    /// Like [`Self::get_range`], but preads DIRECTLY into the caller's `dst`
    /// buffer — no intermediate `Vec` allocation and no second copy (the SST
    /// read path's hot fast-path; ~66% of `read_at`'s cost was that wrapper
    /// waste, measured 2026-06-04). Returns `Ok(Some(n))` with `n` bytes filled
    /// (`n < dst.len()` only at EOF — a short read the caller validates), or
    /// `Ok(None)` on a cache miss (caller re-fetches). Bumps the LRU on a hit
    /// exactly like `get`. Sound ONLY for write-once files (SSTs): a fixed
    /// `(key, offset)` maps to immutable bytes, so positional reads into the
    /// caller buffer are always consistent, and a shared cached fd is safe
    /// because `pread` uses an explicit offset (no shared file cursor).
    pub fn get_range_into(
        &self,
        key: &str,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<Option<usize>> {
        if dst.is_empty() {
            return Ok(Some(0));
        }
        let fg = self.foreground();
        // Critical section: membership check + LRU bump. The pread happens
        // AFTER the guard is released (mirrors `get`) so disk I/O never
        // serializes concurrent readers on the single cache mutex.
        let on_disk = {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");
            if !inner.entries.contains_key(key) {
                self.record_get(false, fg);
                return Ok(None);
            }
            if fg {
                inner.touch_lru(key);
            }
            self.path_for(key)
        };

        // FRS-FDCACHE: reuse a cached open handle (no open()/close() per block).
        let file = match self.get_or_open_fd(key, &on_disk)? {
            Some(f) => f,
            None => {
                // Metadata says present but the file is gone — treat as a miss
                // and drop the stale entry so the caller re-fetches (mirrors
                // `get`'s NotFound handling).
                self.record_get(false, fg);
                self.drop_entry(key);
                return Ok(None);
            }
        };

        let len = dst.len();
        let mut filled = 0usize;
        while filled < len {
            let n = pread(&file, offset + filled as u64, &mut dst[filled..])?;
            if n == 0 {
                break; // EOF before len — short read, caller validates.
            }
            filled += n;
        }
        self.record_get(true, fg);
        Ok(Some(filled))
    }

    /// io_uring backend (streaming-read redesign): a shared open read handle
    /// for `key`'s on-disk cache file (via the FRS-FDCACHE), or `None` on a
    /// cache miss. Bumps the LRU like `get_range_into` so handle-based block
    /// reads keep the entry warm. Sound for the write-once SST files this
    /// cache stores: the fd stays valid (and its bytes immutable) even if the
    /// entry is later evicted/unlinked — Unix keeps the inode alive while the
    /// fd is open.
    pub fn file_handle(&self, key: &str) -> Option<Arc<fs::File>> {
        let fg = self.foreground();
        let on_disk = {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");
            if !inner.entries.contains_key(key) {
                return None;
            }
            if fg {
                inner.touch_lru(key);
            }
            self.path_for(key)
        };
        self.get_or_open_fd(key, &on_disk).ok().flatten()
    }

    /// Writes `data` into the cache under `key`. Evicts oldest entries
    /// as needed to keep `current_bytes <= capacity_bytes`. Returns
    /// `Ok(false)` and writes nothing when `data.len() > capacity_bytes`
    /// (the entry would never fit and would force eviction of everything).
    pub fn put(&self, key: &str, data: &[u8]) -> io::Result<bool> {
        let new_bytes = data.len() as u64;
        if new_bytes > self.capacity_bytes {
            // Reject inserts that exceed the entire budget — they cannot
            // be cached without thrashing every other entry.
            return Ok(false);
        }

        let path = self.path_for(key);
        let tmp_path = self.temp_path_for(key);
        let mut tmp = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)?;
        if let Err(e) = tmp.write_all(data).and_then(|_| tmp.sync_all()) {
            let _ = fs::remove_file(&tmp_path);
            return Err(e);
        }
        drop(tmp);

        // A-R11-H2: hold the bookkeeping mutex across the rename AND
        // the in-memory accounting update so concurrent same-key
        // writers cannot interleave on-disk + accounting in opposite
        // orders. Pre-fix, T1 (size 10) and T2 (size 20) could rename
        // in order T2→T1 (T1's rename atomic-replaces T2's content)
        // while serializing on the mutex in order T1→T2 — leaving the
        // on-disk content from T1 (10 bytes) but accounting reporting
        // 20 bytes, which then mis-sizes eviction decisions and yields
        // short SST reads at the storage layer. The dir.sync_all is
        // also moved inside the mutex so a failure there cannot leak
        // a published-but-unaccounted file (orphan only at fsync
        // failure; map-and-disk both rolled back).
        let to_evict = {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");

            if let Err(e) = fs::rename(&tmp_path, &path) {
                let _ = fs::remove_file(&tmp_path);
                return Err(e);
            }
            if let Err(e) = OpenOptions::new()
                .read(true)
                .open(&self.cache_dir)
                .and_then(|dir| dir.sync_all())
            {
                // D-R12-H2: the rename above atomically REPLACED any
                // existing file at `path`. If we now remove the new
                // file, the OLD file backing any prior entries[key] is
                // also gone — so we must drop that accounting too.
                // Pre-fix removed only the disk file, leaving an
                // accounting entry that pointed at a NotFound path —
                // subsequent reads through the cache would see an
                // entries-hit but disk-miss, returning corruption.
                let _ = fs::remove_file(&path);
                if let Some(prev) = inner.entries.remove(key) {
                    inner.current_bytes = inner.current_bytes.saturating_sub(prev.bytes);
                    inner.mark_stale();
                }
                return Err(e);
            }

            // If the key already exists, treat the put as an update: free
            // the old bytes from the accounting before deciding evictions.
            // Its stale lru ref is reclaimed lazily (no O(N) scan).
            if let Some(prev) = inner.entries.remove(key) {
                inner.current_bytes = inner.current_bytes.saturating_sub(prev.bytes);
                inner.mark_stale();
            }

            let mut evict = Vec::new();
            while inner.current_bytes.saturating_add(new_bytes) > self.capacity_bytes {
                let Some((victim, g)) = inner.lru.pop_front() else {
                    break;
                };
                // Evict only if this is the LIVE recency ref; a stale ref
                // (superseded/removed) is skipped (its entry is gone or has a
                // newer ref later in the deque).
                match inner.entries.get(&victim) {
                    Some(e) if e.gen == g => {
                        let bytes = e.bytes;
                        inner.entries.remove(&victim);
                        inner.current_bytes = inner.current_bytes.saturating_sub(bytes);
                        evict.push(victim);
                    }
                    _ => {
                        inner.stale = inner.stale.saturating_sub(1);
                    }
                }
            }

            let g = inner.next_gen();
            inner.entries.insert(
                key.to_string(),
                Entry {
                    bytes: new_bytes,
                    gen: g,
                },
            );
            inner.lru.push_back((key.to_string(), g));
            inner.current_bytes = inner.current_bytes.saturating_add(new_bytes);
            evict
        };

        // FRS-FDCACHE: the rename above replaced `key`'s file with a NEW inode;
        // any cached fd points at the old inode and must be dropped so the next
        // read opens the fresh file (correctness, not just fd hygiene).
        self.purge_fd(key);

        // FRS-CACHE-ADMISSION: count evictions per key — a key evicted
        // `promote_limit`+ times is blocked from read-fill re-admission
        // (ForSt's promoteLimit anti-thrash cap).
        if let Some(params) = self.policy.admission {
            if !to_evict.is_empty() {
                let mut tracker = self
                    .admission
                    .lock()
                    .expect("admission tracker mutex poisoned");
                for victim in &to_evict {
                    tracker.record_eviction(victim, &params);
                }
            }
        }

        // Best-effort unlink of evicted files. Errors are logged but not
        // propagated: a stale file on disk just wastes a few bytes until
        // the next cache restart drops it.
        for victim in to_evict {
            let path = self.path_for(&victim);
            let _ = fs::remove_file(&path);
            // Purge the evicted entry's fd so the OS handle is released.
            self.purge_fd(&victim);
        }

        Ok(true)
    }

    /// Removes `key` from the cache (both in-memory and on-disk).
    /// Returns `Ok(true)` if the entry existed.
    pub fn invalidate(&self, key: &str) -> io::Result<bool> {
        // A-R12-H2: hold the mutex across the fs::remove_file so a
        // concurrent put(key, new_data) cannot atomic-rename into the
        // path between our entries.remove() and our remove_file().
        // Pre-fix sequence:
        //   T1 invalidate(K): mutex → remove entries[K] → release mutex
        //   T2 put(K, new):   mutex → rename tmp→K → entries[K] = new → release
        //   T1 invalidate(K): fs::remove_file(K) DELETES T2's just-published file
        // Result: T2's caller observes put-OK with on-disk file MISSING.
        // Holding the lock across the unlink serializes the (in-memory,
        // on-disk) pair atomically. Note: on Unix, unlink does not block
        // open file handles, so a concurrent reader that already opened
        // the file before this lock acquisition still sees consistent
        // bytes; only the path-name binding flips.
        let mut inner = self.inner.lock().expect("local cache mutex poisoned");
        let Some(entry) = inner.entries.remove(key) else {
            return Ok(false);
        };
        inner.current_bytes = inner.current_bytes.saturating_sub(entry.bytes);
        inner.mark_stale(); // entry's lru ref is now stale; reclaimed lazily
        let path = self.path_for(key);
        let result = match fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(e),
        };
        drop(inner);
        // FRS-FDCACHE: file removed — purge any cached fd (after releasing the
        // inner lock so the fd-cache lock is never taken while holding `inner`).
        self.purge_fd(key);
        result
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.cache_dir.join(sanitize_key(key))
    }

    fn temp_path_for(&self, key: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        self.cache_dir.join(format!(
            ".tmp-{}-{}-{:?}",
            sanitize_key(key),
            nanos,
            std::thread::current().id()
        ))
    }
}

/// Maps a logical path (e.g. `/db/00000123.sst`) into a single safe
/// filename by URL-encoding `/` and any non-alphanumeric character.
/// This keeps the cache directory flat and avoids path-traversal risks.
fn sanitize_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len() + 8);
    for b in key.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' => out.push(b as char),
            _ => {
                out.push('%');
                out.push_str(&format!("{:02X}", b));
            }
        }
    }
    out
}

/// Inverse of [`sanitize_key`]. On malformed input (truncated `%XX`
/// escape, bad hex), returns the original byte verbatim — the cache
/// only uses the round-trip property to recover its state on restart;
/// it never depends on perfect inversion of arbitrary filenames.
fn unsanitize_key(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let bytes = name.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = &name[i + 1..i + 3];
            if let Ok(b) = u8::from_str_radix(hex, 16) {
                out.push(b as char);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    fn fresh_cache(capacity: u64) -> (TempDir, LocalCache) {
        let tmp = TempDir::new().expect("tempdir");
        let cache = LocalCache::open(tmp.path(), capacity).expect("open cache");
        (tmp, cache)
    }

    #[test]
    fn get_returns_none_on_miss() {
        let (_tmp, cache) = fresh_cache(1024);
        let v = cache.get("/db/missing.sst").expect("get");
        assert!(v.is_none());
        assert_eq!(cache.current_bytes(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn put_then_get_round_trips_bytes() {
        let (_tmp, cache) = fresh_cache(1024);
        let payload = b"some-cached-bytes";
        let inserted = cache.put("/db/00000001.sst", payload).expect("put");
        assert!(inserted, "put with capacity should succeed");
        let v = cache.get("/db/00000001.sst").expect("get").expect("hit");
        assert_eq!(v, payload);
        assert_eq!(cache.current_bytes(), payload.len() as u64);
        assert_eq!(cache.len(), 1);
        assert!(cache.contains("/db/00000001.sst"));
    }

    #[test]
    fn get_range_into_fills_caller_buffer_directly() {
        // FRS-2026-06-04: zero-intermediate-alloc, zero-second-copy read path.
        let (_tmp, cache) = fresh_cache(4096);
        let payload: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        assert!(cache.put("/db/7.sst", &payload).unwrap());

        // Middle sub-range filled directly into the caller's buffer.
        let mut dst = vec![0u8; 50];
        let n = cache
            .get_range_into("/db/7.sst", 100, &mut dst)
            .expect("ok")
            .expect("hit");
        assert_eq!(n, 50);
        assert_eq!(dst, &payload[100..150]);

        // Short read at EOF: returns fewer bytes; only [..n] is defined.
        let mut dst2 = vec![0xFFu8; 50];
        let n2 = cache
            .get_range_into("/db/7.sst", 990, &mut dst2)
            .expect("ok")
            .expect("hit");
        assert_eq!(n2, 10);
        assert_eq!(&dst2[..10], &payload[990..1000]);

        // Miss → None (caller falls back to remote).
        let mut d = [0u8; 10];
        assert!(cache
            .get_range_into("/db/nope.sst", 0, &mut d)
            .expect("ok")
            .is_none());

        // Zero-length → Some(0), no file touch.
        let mut empty: [u8; 0] = [];
        assert_eq!(
            cache.get_range_into("/db/7.sst", 0, &mut empty).unwrap(),
            Some(0)
        );

        // Equivalence: get_range (owned Vec) must equal get_range_into.
        let owned = cache.get_range("/db/7.sst", 100, 50).unwrap().unwrap();
        assert_eq!(owned, dst);
    }

    #[test]
    fn get_range_into_concurrent_shared_fd_no_torn_reads() {
        // Concurrency gate: 16 threads pread mixed sub-ranges of 8 shared files
        // concurrently into their own buffers via the shared cached fd + the
        // per-read LRU lock. Verifies no torn reads, no cross-thread
        // corruption, lock correctness — the silent-bug surface single-threaded
        // ground truth misses.
        let (_tmp, cache) = fresh_cache(8 * 1024 * 1024);
        let cache = Arc::new(cache);
        let nfiles = 8usize;
        let fsize = 64 * 1024usize;
        let content =
            |f: usize| -> Vec<u8> { (0..fsize).map(|i| ((f * 131 + i) & 0xff) as u8).collect() };
        let contents: Arc<Vec<Vec<u8>>> = Arc::new((0..nfiles).map(content).collect());
        for f in 0..nfiles {
            cache.put(&format!("/db/{f}.sst"), &contents[f]).unwrap();
        }

        let mut handles = Vec::new();
        for tid in 0..16usize {
            let cache = cache.clone();
            let contents = contents.clone();
            handles.push(thread::spawn(move || {
                let mut buf = vec![0u8; 4096];
                let blk = (fsize / 4096) as u64;
                let mut s = (tid as u64).wrapping_mul(0x9E37_79B1).wrapping_add(1);
                for _ in 0..5000 {
                    s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let f = (s >> 24) as usize % nfiles;
                    let off = ((s >> 8) % blk) * 4096;
                    let n = cache
                        .get_range_into(&format!("/db/{f}.sst"), off, &mut buf)
                        .unwrap()
                        .unwrap();
                    assert_eq!(n, 4096);
                    let o = off as usize;
                    assert_eq!(
                        &buf[..],
                        &contents[f][o..o + 4096],
                        "torn/cross read tid={tid} f={f} off={off}"
                    );
                }
            }));
        }
        for h in handles {
            h.join().expect("worker join");
        }
    }

    #[test]
    fn get_range_preads_exact_subrange() {
        let (_tmp, cache) = fresh_cache(4096);
        let payload: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        assert!(cache.put("/db/00000007.sst", &payload).unwrap());

        // Middle sub-range.
        let mid = cache
            .get_range("/db/00000007.sst", 100, 50)
            .expect("get_range")
            .expect("hit");
        assert_eq!(mid, &payload[100..150]);

        // Zero-length read is an empty hit (no file touch needed).
        let empty = cache
            .get_range("/db/00000007.sst", 0, 0)
            .expect("get_range");
        assert_eq!(empty, Some(Vec::new()));

        // Read clamped at EOF returns a SHORT vec (caller validates length and
        // falls back); it must not error or over-read.
        let tail = cache
            .get_range("/db/00000007.sst", 990, 50)
            .expect("get_range")
            .expect("hit");
        assert_eq!(tail, &payload[990..1000]);

        // Miss → None.
        assert!(cache
            .get_range("/db/nope.sst", 0, 10)
            .expect("get_range")
            .is_none());

        // get_range bumps the LRU like get (full-range read equals get).
        let full = cache
            .get_range("/db/00000007.sst", 0, payload.len())
            .expect("get_range")
            .expect("hit");
        assert_eq!(full, payload);
    }

    #[test]
    fn get_range_reuses_cached_fd_no_reopen_per_block() {
        // FRS-FDCACHE: the q7 sampled profile showed ~35% of the hot thread in
        // the open() syscall — LocalCache::get_range opened+closed the cache
        // file on EVERY block read. With an fd cache, repeated block reads of
        // the SAME (immutable) SST cache file must reuse one open File handle.
        let (_tmp, cache) = fresh_cache(8192);
        let payload: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        assert!(cache.put("/db/00000009.sst", &payload).unwrap());

        let r0 = cache.get_range("/db/00000009.sst", 0, 64).unwrap().unwrap();
        assert_eq!(r0, &payload[0..64]);
        assert_eq!(cache.fd_opens(), 1, "first get_range opens the file once");

        // Many subsequent block reads of the same key must NOT re-open.
        for off in [64u64, 128, 256, 512, 1024, 2048] {
            let r = cache
                .get_range("/db/00000009.sst", off, 64)
                .unwrap()
                .unwrap();
            assert_eq!(r, &payload[off as usize..off as usize + 64]);
        }
        assert_eq!(
            cache.fd_opens(),
            1,
            "repeated block reads of the same key reuse the cached fd (no re-open)"
        );

        // A different key opens its own fd exactly once.
        let p2 = vec![7u8; 256];
        assert!(cache.put("/db/0000000a.sst", &p2).unwrap());
        let _ = cache.get_range("/db/0000000a.sst", 0, 16).unwrap().unwrap();
        assert_eq!(cache.fd_opens(), 2, "a new key opens its own fd once");
    }

    #[test]
    fn evicting_an_entry_purges_its_cached_fd() {
        // The fd cache must not outlive the on-disk entry: when an entry is
        // evicted (file deleted), its cached fd must be dropped so the OS fd is
        // released and never reused for a stale path.
        let (_tmp, cache) = fresh_cache(220);
        let a = vec![0xAAu8; 100];
        let b = vec![0xBBu8; 100];
        let c = vec![0xCCu8; 100];
        assert!(cache.put("/db/a.sst", &a).unwrap());
        let _ = cache.get_range("/db/a.sst", 0, 10).unwrap().unwrap();
        assert_eq!(cache.fd_cache_len(), 1, "fd cached after a read");

        assert!(cache.put("/db/b.sst", &b).unwrap());
        // Inserting c evicts the oldest (a) — its fd must be purged.
        assert!(cache.put("/db/c.sst", &c).unwrap());
        assert!(!cache.contains("/db/a.sst"), "a was evicted");
        assert!(
            !cache.fd_cache_contains("/db/a.sst"),
            "evicted entry's fd must be purged from the fd cache"
        );
    }

    #[test]
    fn put_evicts_oldest_when_capacity_exceeded() {
        // Capacity for two 100-byte entries; insert a third and the
        // oldest must be evicted.
        let (_tmp, cache) = fresh_cache(220);
        let block_a = vec![0xAAu8; 100];
        let block_b = vec![0xBBu8; 100];
        let block_c = vec![0xCCu8; 100];

        assert!(cache.put("/a", &block_a).unwrap());
        assert!(cache.put("/b", &block_b).unwrap());
        assert!(cache.put("/c", &block_c).unwrap());

        assert!(!cache.contains("/a"), "oldest must have been evicted");
        assert!(cache.contains("/b"));
        assert!(cache.contains("/c"));
        assert_eq!(cache.current_bytes(), 200);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn lru_get_promotes_entry_to_mru() {
        let (_tmp, cache) = fresh_cache(220);
        let block = vec![0xEEu8; 100];

        assert!(cache.put("/a", &block).unwrap());
        assert!(cache.put("/b", &block).unwrap());
        // Access /a so it becomes MRU; /b is now the oldest.
        let _ = cache.get("/a").unwrap();
        assert!(cache.put("/c", &block).unwrap());

        assert!(
            cache.contains("/a"),
            "/a was promoted by get and must survive"
        );
        assert!(
            !cache.contains("/b"),
            "/b was oldest after get and must be evicted"
        );
        assert!(cache.contains("/c"));
    }

    #[test]
    fn lazy_lru_picks_true_victim_after_many_retouches_and_bounds_deque() {
        // Generation-stamped lazy LRU: many re-touches push stale refs into
        // the deque; eviction must still pick the genuine LRU victim, and the
        // deque must stay bounded via compaction.
        let (_tmp, cache) = fresh_cache(300);
        let block = vec![0x5Au8; 100];
        assert!(cache.put("/a", &block).unwrap());
        assert!(cache.put("/b", &block).unwrap());
        assert!(cache.put("/c", &block).unwrap()); // full: a,b,c

        // Hammer /a and /c (hundreds of touches → hundreds of stale refs and a
        // compaction). /b is never touched → it is the true LRU victim.
        for _ in 0..500 {
            assert!(cache.get("/a").unwrap().is_some());
            assert!(cache.get("/c").unwrap().is_some());
        }
        // Insert /d → must evict /b (the untouched, genuinely-oldest entry).
        assert!(cache.put("/d", &block).unwrap());
        assert!(cache.contains("/a"), "/a heavily touched, must survive");
        assert!(cache.contains("/c"), "/c heavily touched, must survive");
        assert!(cache.contains("/d"));
        assert!(!cache.contains("/b"), "/b never touched → true LRU victim");
        assert_eq!(cache.current_bytes(), 300);

        // Deque must be compacted to ~one ref per live entry, not 1000+ stale.
        {
            let inner = cache.inner.lock().unwrap();
            assert!(
                inner.lru.len() <= inner.entries.len() + 64,
                "lru deque must stay bounded (got {} for {} entries)",
                inner.lru.len(),
                inner.entries.len()
            );
        }
    }

    #[test]
    fn put_oversized_returns_false_and_does_not_evict() {
        let (_tmp, cache) = fresh_cache(100);
        let small = vec![0u8; 50];
        let huge = vec![0u8; 500];

        assert!(cache.put("/small", &small).unwrap());
        let admitted = cache.put("/huge", &huge).expect("put");
        assert!(!admitted, "oversized insert must be rejected");
        assert!(cache.contains("/small"));
        assert!(!cache.contains("/huge"));
        assert_eq!(cache.current_bytes(), 50);
    }

    #[test]
    fn capacity_zero_rejects_all_inserts() {
        let (_tmp, cache) = fresh_cache(0);
        let admitted = cache.put("/anything", b"x").expect("put");
        assert!(!admitted);
        assert_eq!(cache.len(), 0);
        assert!(!cache.contains("/anything"));
    }

    #[test]
    fn current_bytes_tracks_inserts_and_evictions_correctly() {
        let (_tmp, cache) = fresh_cache(500);
        let payload = vec![0u8; 100];
        for i in 0..3 {
            assert!(cache.put(&format!("/k{i}"), &payload).unwrap());
        }
        assert_eq!(cache.current_bytes(), 300);

        // Update an existing key with a larger value; current_bytes must
        // reflect the delta (300 - 100 + 200 = 400).
        let bigger = vec![1u8; 200];
        assert!(cache.put("/k1", &bigger).unwrap());
        assert_eq!(cache.current_bytes(), 400);
        assert_eq!(cache.len(), 3);

        // Invalidate one entry.
        assert!(cache.invalidate("/k0").unwrap());
        assert_eq!(cache.current_bytes(), 300);
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains("/k0"));
    }

    #[test]
    fn restart_repopulates_cache_from_existing_files() {
        let tmp = TempDir::new().expect("tempdir");
        let payload = b"durable-payload";
        {
            let cache = LocalCache::open(tmp.path(), 1024).expect("open");
            cache.put("/db/00000007.sst", payload).expect("put");
            assert_eq!(cache.current_bytes(), payload.len() as u64);
        }
        // Re-open: should pick up the existing file.
        let cache = LocalCache::open(tmp.path(), 1024).expect("re-open");
        assert!(cache.contains("/db/00000007.sst"));
        assert_eq!(cache.current_bytes(), payload.len() as u64);
        let v = cache.get("/db/00000007.sst").expect("get").expect("hit");
        assert_eq!(v, payload);
    }

    #[test]
    fn concurrent_put_and_get_smoke_16_threads() {
        // Working set: 16 threads × 32 entries × 256 B = 128 KiB. Provision the
        // cache to fit it all so this is a pure concurrency smoke (not also an
        // eviction race — that's a separate test). Under llvm-cov instrumentation
        // the original 64 KiB sizing caused LRU to evict each put before its
        // own get could observe it (CI run 25653863209).
        let (_tmp, cache) = fresh_cache(1024 * 1024);
        let cache = Arc::new(cache);

        let mut handles = Vec::new();
        for tid in 0..16u32 {
            let cache = cache.clone();
            handles.push(thread::spawn(move || {
                let mut payload = vec![0u8; 256];
                for i in 0..32u32 {
                    payload[0] = tid as u8;
                    payload[1] = i as u8;
                    let key = format!("/t{tid}/k{i}");
                    cache.put(&key, &payload).expect("put");
                    let got = cache.get(&key).expect("get");
                    assert!(got.is_some(), "post-put miss for {key}");
                    assert_eq!(got.unwrap()[..2], [tid as u8, i as u8]);
                }
            }));
        }
        for h in handles {
            h.join().expect("worker join");
        }
        // Bound preserved.
        assert!(cache.current_bytes() <= cache.capacity_bytes());
    }

    // -----------------------------------------------------------------------
    // FRS-CACHE-BG-EXEMPT / FRS-CACHE-ADMISSION (Phase-2 disagg, ForSt §2.1)
    // -----------------------------------------------------------------------

    fn policy_cache(capacity: u64, policy: CachePolicy) -> (TempDir, LocalCache) {
        let tmp = TempDir::new().expect("tempdir");
        let cache = LocalCache::open_with_policy(tmp.path(), capacity, policy).expect("open");
        (tmp, cache)
    }

    fn admission_on() -> CachePolicy {
        CachePolicy {
            background_exempt: true,
            admission: Some(AdmissionParams {
                access_before_promote: 2,
                promote_limit: 3,
                tracker_cap: 1024,
            }),
        }
    }

    #[test]
    fn default_policy_is_off_and_legacy_passthrough() {
        // The Phase-2 rule: flags default OFF ⇒ byte-identical legacy behavior.
        let (_tmp, cache) = fresh_cache(1024);
        assert!(!cache.policy().background_exempt);
        assert!(cache.policy().admission.is_none());
        // admit_read_fill is unconditionally true with admission disabled —
        // even from a background-marked thread.
        let _bg = crate::requester::BackgroundScope::enter();
        assert!(cache.admit_read_fill("/db/x.sst"));
        assert_eq!(cache.admission_stats(), (0, 0));
    }

    #[test]
    fn background_reads_do_not_promote_lru() {
        // ForSt §2.1.4: only foreground threads affect LRU order. A background
        // touch of /a must NOT save it from eviction; a foreground touch must.
        let policy = CachePolicy {
            background_exempt: true,
            admission: None,
        };
        let (_tmp, cache) = policy_cache(220, policy);
        let block = vec![0xEEu8; 100];

        // Background touch: /a stays the LRU victim.
        assert!(cache.put("/a", &block).unwrap());
        assert!(cache.put("/b", &block).unwrap());
        {
            let _bg = crate::requester::BackgroundScope::enter();
            assert!(cache.get("/a").unwrap().is_some(), "bg read still SERVES");
        }
        assert!(cache.put("/c", &block).unwrap());
        assert!(
            !cache.contains("/a"),
            "background get must NOT promote /a — it stays the eviction victim"
        );
        assert!(cache.contains("/b") && cache.contains("/c"));

        // Control: the same sequence with a FOREGROUND touch promotes /a.
        let (_tmp2, cache2) = policy_cache(220, policy);
        assert!(cache2.put("/a", &block).unwrap());
        assert!(cache2.put("/b", &block).unwrap());
        assert!(cache2.get("/a").unwrap().is_some());
        assert!(cache2.put("/c", &block).unwrap());
        assert!(cache2.contains("/a"), "foreground get promotes /a");
        assert!(!cache2.contains("/b"));

        // Stats split: bg accesses land in bg counters, not the headline rate.
        let (fg_hits, _) = cache.stats();
        let (bg_hits, _) = cache.bg_stats();
        assert_eq!(bg_hits, 1, "the bg get is counted separately");
        assert_eq!(fg_hits, 0);
    }

    #[test]
    fn background_get_range_does_not_promote_lru() {
        // Same exemption through the hot positional-read path.
        let policy = CachePolicy {
            background_exempt: true,
            admission: None,
        };
        let (_tmp, cache) = policy_cache(220, policy);
        let block = vec![0x5Au8; 100];
        assert!(cache.put("/a", &block).unwrap());
        assert!(cache.put("/b", &block).unwrap());
        let mut buf = [0u8; 16];
        {
            let _bg = crate::requester::BackgroundScope::enter();
            let n = cache.get_range_into("/a", 0, &mut buf).unwrap().unwrap();
            assert_eq!(n, 16);
        }
        assert!(cache.put("/c", &block).unwrap());
        assert!(!cache.contains("/a"), "bg get_range_into must not promote");
    }

    #[test]
    fn admission_gates_read_fills_count_to_promote() {
        // ForSt count-to-promote: the K-th foreground miss admits the fill.
        let (_tmp, cache) = policy_cache(4096, admission_on());
        assert!(
            !cache.admit_read_fill("/db/cold.sst"),
            "1st touch below access_before_promote=2 → rejected"
        );
        assert!(
            cache.admit_read_fill("/db/cold.sst"),
            "2nd touch reaches the threshold → admitted"
        );
        // Counts reset on admit: the next round starts over.
        assert!(!cache.admit_read_fill("/db/cold.sst"));
        assert!(cache.admit_read_fill("/db/cold.sst"));
        assert_eq!(cache.admission_stats(), (2, 2));
    }

    #[test]
    fn admission_promote_limit_blocks_thrashing_key() {
        // ForSt promoteLimit: a key evicted >= limit times is permanently
        // blocked from read-fill re-admission (anti-thrash cap). Capacity for
        // exactly TWO 100-byte entries; /hot1 + /hot2 churn /victim out.
        let (_tmp, cache) = policy_cache(220, admission_on());
        let block = vec![0xAB; 100];
        for round in 0..3 {
            // /victim is admitted (2 touches) and put, then evicted by churn.
            assert!(!cache.admit_read_fill("/victim"));
            assert!(cache.admit_read_fill("/victim"), "round {round} admit");
            assert!(cache.put("/victim", &block).unwrap());
            assert!(cache.put(&format!("/hot1-{round}"), &block).unwrap());
            assert!(cache.put(&format!("/hot2-{round}"), &block).unwrap());
            assert!(!cache.contains("/victim"), "round {round}: churned out");
        }
        // 3 evictions reached promote_limit=3 → blocked forever after.
        for _ in 0..8 {
            assert!(
                !cache.admit_read_fill("/victim"),
                "evicted >= promote_limit times → permanently blocked"
            );
        }
    }

    #[test]
    fn background_reads_do_not_accumulate_admission_credit() {
        // ForSt §2.1.4: only foreground threads update access counts. A burst
        // of background touches must neither admit nor advance the count.
        let (_tmp, cache) = policy_cache(4096, admission_on());
        {
            let _bg = crate::requester::BackgroundScope::enter();
            for _ in 0..10 {
                assert!(
                    !cache.admit_read_fill("/db/scan.sst"),
                    "background read-fill is always rejected"
                );
            }
        }
        // Foreground still needs its OWN 2 touches (bg touches counted 0).
        assert!(!cache.admit_read_fill("/db/scan.sst"));
        assert!(cache.admit_read_fill("/db/scan.sst"));
    }

    #[test]
    fn write_path_put_is_never_gated_by_admission() {
        // Write-only admission split: newly generated SSTs (write-through)
        // call `put` directly and are always cached, admission policy or not.
        let (_tmp, cache) = policy_cache(4096, admission_on());
        assert!(cache.put("/db/fresh-flush.sst", &[1, 2, 3]).unwrap());
        assert!(cache.contains("/db/fresh-flush.sst"));
    }

    #[test]
    fn admission_thrash_scenario_stabilizes_cache() {
        // The paper's thrash scenario (§5.4): cyclically scan a working set
        // 2× the cache budget. Legacy LRU admits every miss → every entry is
        // evicted before its next touch → the cache churns at full write
        // volume and hits stay ~0. With count-to-promote + promote_limit the
        // cache stops churning: after each key's eviction count reaches the
        // cap, fills cease (rejected) and the resident set stabilizes.
        let block = vec![0u8; 100];
        let keys: Vec<String> = (0..20).map(|i| format!("/db/{i:03}.sst")).collect();

        // Cell A — legacy (admission off): every round refills every key.
        let (_t1, lru) = policy_cache(1000, CachePolicy::default()); // fits 10 of 20
        let mut lru_fills = 0u64;
        for _round in 0..10 {
            for k in &keys {
                if lru.get(k).unwrap().is_none() && lru.admit_read_fill(k) {
                    lru.put(k, &block).unwrap();
                    lru_fills += 1;
                }
            }
        }
        assert_eq!(lru_fills, 200, "legacy LRU thrashes: 20 fills × 10 rounds");

        // Cell B — admission on: fills must stop once the thrash cap engages.
        let (_t2, adm) = policy_cache(1000, admission_on());
        let mut adm_fills = 0u64;
        for _round in 0..10 {
            for k in &keys {
                if adm.get(k).unwrap().is_none() && adm.admit_read_fill(k) {
                    adm.put(k, &block).unwrap();
                    adm_fills += 1;
                }
            }
        }
        assert!(
            adm_fills < lru_fills / 2,
            "admission policy must kill the thrash churn (fills {adm_fills} vs LRU {lru_fills})"
        );
        // And the cache ends up holding a stable resident subset.
        assert!(!adm.is_empty(), "a resident subset survives");
    }

    #[test]
    fn pre_seed_makes_first_touch_admit_but_never_unblocks() {
        // ForSt §2.1.6 restore pre-seeding: a seeded key's FIRST foreground
        // touch reaches count-to-promote; unseeded keys still need K touches.
        // Capacity fits exactly two 100-byte entries so the blocked-key
        // section below can actually thrash /v out promote_limit times.
        let (_tmp, cache) = policy_cache(220, admission_on());
        cache.pre_seed_admission("/db/restored.sst");
        assert!(
            cache.admit_read_fill("/db/restored.sst"),
            "seeded key admits on the first touch"
        );
        assert!(
            !cache.admit_read_fill("/db/unseeded.sst"),
            "unseeded keys keep the normal threshold"
        );

        // Seeding must not resurrect a thrash-blocked key.
        let block = vec![0xAB; 100];
        for round in 0..3 {
            assert!(!cache.admit_read_fill("/v"));
            assert!(cache.admit_read_fill("/v"));
            assert!(cache.put("/v", &block).unwrap());
            assert!(cache.put(&format!("/h1-{round}"), &block).unwrap());
            assert!(cache.put(&format!("/h2-{round}"), &block).unwrap());
        }
        cache.pre_seed_admission("/v");
        assert!(
            !cache.admit_read_fill("/v"),
            "pre-seed must not override the promote-limit block"
        );

        // Background pre-seeded key: bg touches still never admit.
        cache.pre_seed_admission("/db/restored2.sst");
        {
            let _bg = crate::requester::BackgroundScope::enter();
            assert!(!cache.admit_read_fill("/db/restored2.sst"));
        }
        assert!(
            cache.admit_read_fill("/db/restored2.sst"),
            "seed credit preserved for the first FOREGROUND touch"
        );

        // Disabled policy: a pure no-op.
        let (_t2, legacy) = fresh_cache(1024);
        legacy.pre_seed_admission("/x");
        assert!(legacy.admit_read_fill("/x"));
        assert_eq!(legacy.admission_stats(), (0, 0));
    }

    #[test]
    fn admission_tracker_order_deque_bounded_under_admit_traffic() {
        // PMC self-review regression: every ADMITTED key removes its count
        // but used to leave its order ref behind — under normal admit
        // traffic (many distinct keys each admitted after K touches) the
        // deque grew one String per admitted key forever. The reclaim must
        // bound it to ~2x the tracker cap.
        let cap = 64usize;
        let policy = CachePolicy {
            background_exempt: false,
            admission: Some(AdmissionParams {
                access_before_promote: 2,
                promote_limit: 100, // never block: pure admit traffic
                tracker_cap: cap,
            }),
        };
        let (_tmp, cache) = policy_cache(1 << 20, policy);
        for i in 0..5000 {
            let key = format!("/db/admit-{i}");
            assert!(!cache.admit_read_fill(&key));
            assert!(cache.admit_read_fill(&key), "2nd touch admits");
        }
        let tracker = cache.admission.lock().unwrap();
        assert!(
            tracker.counts_order.len() <= cap * 2,
            "counts_order leaked: {} refs for {} live counts (cap {})",
            tracker.counts_order.len(),
            tracker.counts.len(),
            cap
        );
    }

    #[test]
    fn admission_tracker_stays_bounded() {
        // FIFO aging: the tracker must never exceed its cap no matter how
        // many distinct keys touch it.
        let policy = CachePolicy {
            background_exempt: false,
            admission: Some(AdmissionParams {
                access_before_promote: 3, // touches stay below promote → counts retained
                promote_limit: 2,
                tracker_cap: 64,
            }),
        };
        let (_tmp, cache) = policy_cache(1 << 20, policy);
        for i in 0..1000 {
            let _ = cache.admit_read_fill(&format!("/db/k{i}"));
        }
        let tracker = cache.admission.lock().unwrap();
        assert!(
            tracker.counts.len() <= 64,
            "cold-count tracker exceeded cap: {}",
            tracker.counts.len()
        );
    }

    #[test]
    fn sanitize_key_round_trips_safe_chars() {
        let key = "/db/00000123.sst";
        let s = sanitize_key(key);
        assert!(!s.contains('/'), "slash must be encoded");
        assert_eq!(unsanitize_key(&s), key);

        let key2 = "weird name with spaces!";
        let s2 = sanitize_key(key2);
        assert_eq!(unsanitize_key(&s2), key2);
    }
}
