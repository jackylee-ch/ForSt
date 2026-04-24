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

//! ForSt-RS performance benchmarks.
//!
//! See `docs/design/2.12_implementation_roadmap.md` Section 8 for the full
//! benchmark suite specification. This crate hosts:
//!
//! - `benches/point_lookup.rs` — BM-1.1 Point lookup latency / throughput
//! - `benches/batch_ops.rs` — BM-1.2 Batch put/get scaling
//! - `benches/write_throughput.rs` — BM-1.3 Sustained write throughput
//! - `benches/checkpoint.rs` — BM-1.4 Checkpoint/restore timing
//!
//! Run individual benchmarks via:
//!
//! ```bash
//! cargo bench -p forst-rs-bench --bench point_lookup
//! ```
//!
//! The library target hosts shared fixtures (key/value generators, engine
//! setup helpers) used by every benchmark binary.

use std::sync::Arc;

use forst_rs_common::EngineOptions;
use forst_rs_engine::{ColumnFamilyDescriptor, ColumnFamilyHandle, DbImpl};
use forst_rs_io::{FileSystem, MemoryFileSystem};

/// Creates a fresh in-memory engine at `/db` with the given buffer size.
/// All benchmarks use the in-memory filesystem to remove disk noise from
/// the measurements; absolute numbers therefore reflect the engine's
/// bookkeeping overhead rather than real-world I/O performance.
pub fn open_in_memory(write_buffer_size: usize) -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: "/db".to_string(),
        write_buffer_size,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open_in_memory")
}

/// Populates an engine with `count` sequential put entries. Keys have the
/// form `"k00000..0N"` and values the form `"v00000..0N"`.
pub fn seed_sequential(
    db: &Arc<DbImpl>,
    cf: &ColumnFamilyHandle,
    count: u32,
    key_fmt: &str,
    value_fmt: &str,
) {
    for i in 0..count {
        let k = format_interp(key_fmt, i);
        let v = format_interp(value_fmt, i);
        db.put(cf, k.as_bytes(), v.as_bytes()).expect("put");
    }
}

/// Tiny `format!` stand-in that accepts templates of the form `"{:08}"`.
fn format_interp(fmt: &str, i: u32) -> String {
    // This helper keeps the benchmark setup free of format! macro
    // invocations inside hot loops.
    if fmt == "k{:08}" {
        format!("k{:08}", i)
    } else if fmt == "v{:08}" {
        format!("v{:08}", i)
    } else if fmt == "k{:06}" {
        format!("k{:06}", i)
    } else if fmt == "v{:06}" {
        format!("v{:06}", i)
    } else {
        format!("k{}", i)
    }
}

/// Creates a column family with the given name on the given engine.
pub fn create_cf(db: &Arc<DbImpl>, name: &str) -> ColumnFamilyHandle {
    db.create_column_family(ColumnFamilyDescriptor::new(name))
        .expect("create_cf")
}
