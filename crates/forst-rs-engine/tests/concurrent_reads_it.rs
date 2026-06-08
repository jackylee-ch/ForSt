// M1 gate (2026-06-07): the async-state parallel-executor upgrade fans foreground
// reads across a read-IO thread pool, so multiple threads call `get_arc` / `batch_get`
// on ONE `Arc<DbImpl>` concurrently (while bg flush/compaction also runs). This test
// proves that is correct + panic-free across a mixed memtable+SST working set — the
// prerequisite for parallelizing VectorizedExecutor.
use std::sync::Arc;
use std::thread;

use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, MemoryFileSystem};

fn open() -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: "/db".to_string(),
        // Small write buffer so the 20k keys span several flushed SSTs + the live
        // memtable — concurrent reads then exercise both tiers under contention.
        write_buffer_size: 128 * 1024,
        memtable_shards: 16,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open")
}

const N: u32 = 20_000;

#[test]
fn concurrent_get_arc_correct_and_safe() {
    let db = open();
    {
        let cf = db.default_cf();
        for i in 0..N {
            db.put(
                &cf,
                format!("k{:06}", i).as_bytes(),
                format!("v{:06}", i).as_bytes(),
            )
            .unwrap();
        }
    }
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let cf = db.default_cf();
                for round in 0..3u32 {
                    for i in 0..N {
                        let got = db.get_arc(&cf, format!("k{:06}", i).as_bytes()).unwrap();
                        let want = format!("v{:06}", i);
                        assert_eq!(
                            got.as_deref(),
                            Some(want.as_bytes()),
                            "thread {t} round {round} key {i}"
                        );
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("reader thread panicked");
    }
}

#[test]
fn concurrent_batch_get_correct_and_safe() {
    let db = open();
    {
        let cf = db.default_cf();
        for i in 0..N {
            db.put(
                &cf,
                format!("k{:06}", i).as_bytes(),
                format!("v{:06}", i).as_bytes(),
            )
            .unwrap();
        }
    }
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let cf = db.default_cf();
                for _round in 0..3u32 {
                    // batch-get a window of keys (the executor's multiget path)
                    let keys: Vec<Vec<u8>> = (0..256)
                        .map(|i| format!("k{:06}", i).into_bytes())
                        .collect();
                    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                    let res = db.batch_get(&cf, &key_refs).unwrap();
                    for (i, r) in res.iter().enumerate() {
                        let want = format!("v{:06}", i);
                        assert_eq!(r.as_deref(), Some(want.as_bytes()), "batch key {i}");
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("batch reader thread panicked");
    }
}
