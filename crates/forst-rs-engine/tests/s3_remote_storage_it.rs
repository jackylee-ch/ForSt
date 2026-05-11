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

//! Integration test: open [`DbImpl`] against a real MinIO S3-compatible
//! backend via [`DbImpl::open_remote`] (B-Prod-followup-3).
//!
//! Validates spec §6c "Disaggregated remote storage as primary" against a
//! real S3 service (not just `memory://`). Tests four properties:
//!
//!   1. `open_remote(s3://...)` returns an engine without error against
//!      a live S3 server.
//!   2. Writes propagate through the cached filesystem and a forced
//!      flush lands actual SST objects on the remote bucket.
//!   3. Reads after the flush return the original values — proving the
//!      cached FS is functionally correct end-to-end against S3.
//!   4. The SST objects are physically present in the bucket as seen by
//!      a fresh OpenDAL operator (i.e. they really live on S3, not just
//!      in the local cache directory).
//!
//! Container provisioning: we override the entrypoint to pre-create the
//! destination bucket as a top-level directory under `/data` BEFORE
//! `minio server` boots. MinIO's fs-mode backend scans `/data` on
//! startup and registers each top-level dir as a bucket, which avoids
//! pulling in the AWS SDK or a separate `mc` step.
//!
//! Gating: the test is behind the `s3-it` Cargo feature because it needs
//! a Docker daemon. Default `cargo test --workspace` skips it; CI runs
//! it explicitly on an `ubuntu-latest` job that has Docker pre-installed.

#![cfg(feature = "s3-it")]

use std::collections::HashMap;
use std::path::Path;

use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, OpendalFileSystem};
use tempfile::TempDir;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::{runners::SyncRunner, GenericImage, ImageExt};

const N_ENTRIES: u32 = 1_000;
const BUCKET: &str = "forst-rs-it";
// Pin the MinIO image tag to the same release the upstream
// `testcontainers-modules` `MinIO` module pins, so a CI run and a local
// repro drink from the same well.
const MINIO_TAG: &str = "RELEASE.2025-02-28T09-55-16Z";

fn key_for(i: u32) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

fn val_for(i: u32) -> Vec<u8> {
    format!("v{i}").into_bytes()
}

fn engine_options(db_path: &str) -> EngineOptions {
    EngineOptions {
        db_path: db_path.to_string(),
        // Small write buffer so we actually flush something during the
        // write phase rather than the whole batch sitting in the memtable.
        write_buffer_size: 64 * 1024,
        ..EngineOptions::default()
    }
}

#[test]
fn s3_round_trip_via_minio() {
    // 1. Start MinIO. We override the entrypoint to `sh -c` so we can
    //    `mkdir -p /data/<bucket>` BEFORE `minio server` boots. The
    //    fs-mode backend scans `/data` for top-level dirs on startup
    //    and registers each as a bucket — no separate CreateBucket
    //    RPC needed, no AWS SDK dependency, no `mc` client required.
    let bootstrap = format!("mkdir -p /data/{BUCKET} && exec minio server /data");
    let minio = GenericImage::new("minio/minio", MINIO_TAG)
        .with_exposed_port(ContainerPort::Tcp(9000))
        .with_wait_for(WaitFor::message_on_stderr("API:"))
        .with_entrypoint("sh")
        .with_env_var("MINIO_ROOT_USER", "minioadmin")
        .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_cmd(["-c", &bootstrap])
        .start()
        .expect("MinIO container failed to start (is Docker running?)");

    let host = minio.get_host().expect("get_host").to_string();
    let api_port = minio
        .get_host_port_ipv4(9000)
        .expect("get_host_port_ipv4(9000)");
    let endpoint = format!("http://{host}:{api_port}");

    // 2. Build the OpenDAL config map that `open_remote` expects for
    //    s3:// URIs. Credentials match the env we set above.
    let mut opendal_cfg = HashMap::new();
    opendal_cfg.insert("region".to_string(), "us-east-1".to_string());
    opendal_cfg.insert("endpoint".to_string(), endpoint.clone());
    opendal_cfg.insert("access_key_id".to_string(), "minioadmin".to_string());
    opendal_cfg.insert("secret_access_key".to_string(), "minioadmin".to_string());

    let uri = format!("s3://{BUCKET}/");
    let cache_dir = TempDir::new().expect("cache tempdir");
    let db_path = "/db-s3-it";

    // 3. Open the DB pointed at MinIO.
    let db = DbImpl::open_remote(
        engine_options(db_path),
        &uri,
        opendal_cfg.clone(),
        cache_dir.path(),
        64 * 1024 * 1024, // 64 MiB local cache
    )
    .expect("open_remote against MinIO");
    let cf = db.default_cf();

    // 4. Write N entries.
    for i in 0..N_ENTRIES {
        db.put(&cf, &key_for(i), &val_for(i))
            .unwrap_or_else(|e| panic!("put i={i}: {e}"));
    }

    // 5. Force a flush so SSTs land on the remote (otherwise reads stay
    //    in the active memtable and never exercise S3 at all).
    db.flush_all().expect("flush_all");

    // 6. Read every key back. Values must match. This validates the
    //    full path: memtable → SST builder → S3 → cached FS → reader.
    for i in 0..N_ENTRIES {
        let v = db
            .get(&cf, &key_for(i))
            .unwrap_or_else(|e| panic!("get i={i}: {e}"))
            .unwrap_or_else(|| panic!("missing value at i={i}"));
        assert_eq!(v, val_for(i), "value mismatch at i={i}");
    }

    // 7. List the bucket via a fresh OpenDAL S3 operator (NOT going
    //    through the engine's cached FS) and confirm SST objects
    //    actually exist on the remote. This proves the writes really
    //    went to S3, not just to the local cache directory.
    let sst_count = count_remote_sst_files(&endpoint, db_path);
    assert!(
        sst_count > 0,
        "expected >=1 SST object on the remote bucket after flush, found 0 \
         (writes must have stayed in the local cache, which would mean the \
         disaggregated path is broken)"
    );
}

/// Builds a fresh `OpendalFileSystem::s3` operator pointed at the same
/// MinIO endpoint and lists the engine's db_path, counting `.sst` entries.
fn count_remote_sst_files(endpoint: &str, db_path: &str) -> usize {
    let fs = OpendalFileSystem::s3(
        BUCKET,
        "us-east-1",
        Some(endpoint),
        Some("minioadmin"),
        Some("minioadmin"),
    )
    .expect("build fresh OpendalFileSystem::s3 for listing");

    // `db_path` is `/db-s3-it`; strip the leading slash because OpenDAL
    // S3 keys are bucket-relative.
    let dir = db_path.trim_start_matches('/');
    let entries = fs.list_dir(Path::new(dir)).expect("list bucket contents");

    entries
        .into_iter()
        .filter(|meta| meta.path.to_string_lossy().ends_with(".sst"))
        .count()
}
