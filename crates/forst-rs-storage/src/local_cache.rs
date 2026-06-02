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
        })
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
    /// 100k gets. Lock-free; called on every `get`.
    fn record_get(&self, hit: bool) {
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
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
            eprintln!("FRS-CACHE-STATS: hits={hits} misses={misses} hit_rate={rate:.1}%");
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
    /// On hit, marks `key` as most-recently-used.
    pub fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        // Critical section: check membership + bump LRU. Read the file
        // outside the lock so concurrent gets don't serialize on disk I/O.
        let on_disk = {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");
            if !inner.entries.contains_key(key) {
                self.record_get(false);
                return Ok(None);
            }
            inner.touch_lru(key);
            self.path_for(key)
        };

        match fs::read(&on_disk) {
            Ok(bytes) => {
                self.record_get(true);
                Ok(Some(bytes))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                self.record_get(false);
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
        // Critical section: membership check + LRU bump. The pread happens
        // AFTER the guard is released (mirrors `get`) so disk I/O never
        // serializes concurrent readers on the single cache mutex.
        let on_disk = {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");
            if !inner.entries.contains_key(key) {
                self.record_get(false);
                return Ok(None);
            }
            inner.touch_lru(key);
            self.path_for(key)
        };

        // FRS-FDCACHE: reuse a cached open handle (no open()/close() per block).
        let file = match self.get_or_open_fd(key, &on_disk)? {
            Some(f) => f,
            None => {
                // Metadata says present but the file is gone — treat as a miss
                // and drop the stale entry so the caller re-fetches (mirrors
                // `get`'s NotFound handling).
                self.record_get(false);
                self.drop_entry(key);
                return Ok(None);
            }
        };

        let mut buf = vec![0u8; len];
        let mut filled = 0usize;
        while filled < len {
            let n = pread(&file, offset + filled as u64, &mut buf[filled..])?;
            if n == 0 {
                break; // EOF before len — short read, caller validates.
            }
            filled += n;
        }
        buf.truncate(filled);
        self.record_get(true);
        Ok(Some(buf))
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
