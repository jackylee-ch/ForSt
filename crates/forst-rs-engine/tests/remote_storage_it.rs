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

//! Integration test for [`DbImpl::open_remote`] (B-Prod-P6 Task 6.4).
//!
//! Verifies that the URI-driven `open_remote` constructor:
//!
//!   1. Accepts a `memory://` URI and stands up an engine without panicking.
//!   2. Round-trips put/get against the resulting engine.
//!   3. Forces a flush so an SST actually lands in the OpenDAL backend,
//!      then confirms a subsequent read populates the local cache (proving
//!      the [`CachedFileSystem`] layer is wired correctly).
//!   4. Holds the cache to a finite byte budget — no entries means we
//!      have not warmed it; at least one entry means the read path went
//!      through the cache layer.

use std::collections::HashMap;

use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;

const N_ENTRIES: usize = 64;

fn key_for(i: usize) -> Vec<u8> {
    format!("remote_key_{i:04}").into_bytes()
}

fn val_for(i: usize) -> Vec<u8> {
    format!("remote_val_{i:04}").into_bytes()
}

#[test]
fn open_remote_memory_uri_round_trip_populates_cache() {
    let cache_dir = tempfile::TempDir::new().expect("cache tempdir");

    let opts = EngineOptions {
        db_path: "/db-remote".to_string(),
        write_buffer_size: 64 * 1024,
        ..EngineOptions::default()
    };

    // open_remote: memory:// remote, 64 MiB cache budget on the local FS.
    let db = DbImpl::open_remote(
        opts,
        "memory://",
        HashMap::new(),
        cache_dir.path(),
        64 * 1024 * 1024,
    )
    .expect("open_remote");
    let cf = db.default_cf();

    // Put N entries.
    for i in 0..N_ENTRIES {
        db.put(&cf, &key_for(i), &val_for(i)).expect("put");
    }

    // Force a flush so SST(s) actually land on the remote (memory://)
    // backend; otherwise reads stay in the active memtable and never
    // exercise the cache layer.
    db.switch_and_flush(&cf)
        .expect("switch_and_flush")
        .expect("flush must produce at least one SST");

    // Read every key — every read should hit the cache after the first
    // miss (cache layer fetches the SST from memory:// remote and pins
    // it in cache_dir).
    for i in 0..N_ENTRIES {
        let v = db
            .get(&cf, &key_for(i))
            .expect("get")
            .expect("value present");
        assert_eq!(v, val_for(i), "value mismatch at i={i}");
    }

    // Cache directory MUST contain at least one file now (the SST that
    // got fetched from the remote backend on the first read).
    let mut entries: Vec<_> = std::fs::read_dir(cache_dir.path())
        .expect("read cache dir")
        .filter_map(|e| e.ok())
        .collect();
    entries.retain(|e| e.metadata().map(|m| m.is_file()).unwrap_or(false));
    assert!(
        !entries.is_empty(),
        "cache dir must hold at least one fetched SST after reads (cache_dir={:?})",
        cache_dir.path()
    );
}

#[test]
fn open_remote_rejects_unknown_scheme() {
    let cache_dir = tempfile::TempDir::new().expect("cache tempdir");
    let opts = EngineOptions {
        db_path: "/db-bad-scheme".to_string(),
        ..EngineOptions::default()
    };
    let result = DbImpl::open_remote(opts, "ftp://nope/", HashMap::new(), cache_dir.path(), 1024);
    assert!(result.is_err(), "unknown scheme must fail");
    let err = result.err().unwrap();
    assert!(err.is_invalid_argument(), "unexpected error: {err}");
}

#[test]
fn open_remote_rejects_missing_scheme() {
    let cache_dir = tempfile::TempDir::new().expect("cache tempdir");
    let opts = EngineOptions {
        db_path: "/db-no-scheme".to_string(),
        ..EngineOptions::default()
    };
    let result = DbImpl::open_remote(
        opts,
        "no-colon-anywhere",
        HashMap::new(),
        cache_dir.path(),
        1024,
    );
    assert!(result.is_err(), "missing scheme must fail");
    assert!(result.err().unwrap().is_invalid_argument());
}
