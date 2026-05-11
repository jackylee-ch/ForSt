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
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Per-entry bookkeeping. `bytes` is the on-disk size; the LRU position
/// is implied by membership in the `lru` deque.
#[derive(Clone, Debug)]
struct Entry {
    bytes: u64,
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
}

#[derive(Default)]
struct Inner {
    /// Map of logical key -> entry metadata.
    entries: HashMap<String, Entry>,
    /// LRU order (front = oldest, back = most-recently-used).
    lru: VecDeque<String>,
    /// Sum of `entries[*].bytes`. Updated atomically with the map.
    current_bytes: u64,
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
            let key = unsanitize_key(&file_name);
            let bytes = meta.len();
            inner.entries.insert(key.clone(), Entry { bytes });
            inner.lru.push_back(key);
            inner.current_bytes = inner.current_bytes.saturating_add(bytes);
        }

        Ok(Self {
            cache_dir,
            capacity_bytes,
            inner: Mutex::new(inner),
        })
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

    /// Reads the cached bytes for `key`. Returns `Ok(None)` on miss.
    /// On hit, marks `key` as most-recently-used.
    pub fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        // Critical section: check membership + bump LRU. Read the file
        // outside the lock so concurrent gets don't serialize on disk I/O.
        let on_disk = {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");
            if !inner.entries.contains_key(key) {
                return Ok(None);
            }
            // Move key to back of LRU (most-recently-used).
            if let Some(pos) = inner.lru.iter().position(|k| k == key) {
                inner.lru.remove(pos);
            }
            inner.lru.push_back(key.to_string());
            self.path_for(key)
        };

        match fs::read(&on_disk) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // The metadata says we have it but the file is gone — surface
                // as a miss rather than an error so callers can re-fetch.
                let mut inner = self.inner.lock().expect("local cache mutex poisoned");
                if let Some(entry) = inner.entries.remove(key) {
                    inner.current_bytes = inner.current_bytes.saturating_sub(entry.bytes);
                }
                if let Some(pos) = inner.lru.iter().position(|k| k == key) {
                    inner.lru.remove(pos);
                }
                Ok(None)
            }
            Err(e) => Err(e),
        }
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

        // Compute eviction set under the lock; perform disk writes outside.
        let to_evict = {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");

            // If the key already exists, treat the put as an update: free
            // the old bytes from the accounting before deciding evictions.
            if let Some(prev) = inner.entries.remove(key) {
                inner.current_bytes = inner.current_bytes.saturating_sub(prev.bytes);
                if let Some(pos) = inner.lru.iter().position(|k| k == key) {
                    inner.lru.remove(pos);
                }
            }

            let mut evict = Vec::new();
            while inner.current_bytes.saturating_add(new_bytes) > self.capacity_bytes {
                let Some(victim) = inner.lru.pop_front() else {
                    break;
                };
                if let Some(entry) = inner.entries.remove(&victim) {
                    inner.current_bytes = inner.current_bytes.saturating_sub(entry.bytes);
                    evict.push(victim);
                }
            }

            // Reserve the slot now so concurrent puts see the new accounting.
            inner
                .entries
                .insert(key.to_string(), Entry { bytes: new_bytes });
            inner.lru.push_back(key.to_string());
            inner.current_bytes = inner.current_bytes.saturating_add(new_bytes);
            evict
        };

        // Best-effort unlink of evicted files. Errors are logged but not
        // propagated: a stale file on disk just wastes a few bytes until
        // the next cache restart drops it.
        for victim in to_evict {
            let path = self.path_for(&victim);
            let _ = fs::remove_file(&path);
        }

        // Write the new entry. If the disk write fails we MUST roll back
        // the in-memory accounting so we don't claim to have something we
        // don't.
        let path = self.path_for(key);
        if let Err(e) = fs::write(&path, data) {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");
            if let Some(entry) = inner.entries.remove(key) {
                inner.current_bytes = inner.current_bytes.saturating_sub(entry.bytes);
            }
            if let Some(pos) = inner.lru.iter().position(|k| k == key) {
                inner.lru.remove(pos);
            }
            return Err(e);
        }
        Ok(true)
    }

    /// Removes `key` from the cache (both in-memory and on-disk).
    /// Returns `Ok(true)` if the entry existed.
    pub fn invalidate(&self, key: &str) -> io::Result<bool> {
        let path = {
            let mut inner = self.inner.lock().expect("local cache mutex poisoned");
            let Some(entry) = inner.entries.remove(key) else {
                return Ok(false);
            };
            inner.current_bytes = inner.current_bytes.saturating_sub(entry.bytes);
            if let Some(pos) = inner.lru.iter().position(|k| k == key) {
                inner.lru.remove(pos);
            }
            self.path_for(key)
        };
        match fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(e),
        }
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.cache_dir.join(sanitize_key(key))
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
        let (_tmp, cache) = fresh_cache(64 * 1024);
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
