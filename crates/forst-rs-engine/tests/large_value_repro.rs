// Repro for the q4/q7 large-value crash: streaming-join MapState values
// (no-unique-key join buffers all records per key) legitimately exceed the
// old 1 MiB FFI cap. After raising the cap, the engine's batch_write / flush
// path itself fails (INVALID_ARGUMENT / CORRUPTION) or aborts. This test
// drives multi-MiB values through batch_write + flush + readback on a local
// in-memory FS to reproduce the engine-side defect without a cluster.

use std::sync::Arc;
use std::thread;

use forst_rs_common::EngineOptions;
use forst_rs_engine::{DbImpl, WriteBatch};
use forst_rs_io::{FileSystem, MemoryFileSystem, OpendalFileSystem};

fn opts(buf: usize) -> EngineOptions {
    EngineOptions {
        db_path: "/db".to_string(),
        write_buffer_size: buf,
        ..EngineOptions::default()
    }
}

/// Single large values spanning the old 1 MiB cap, each via its own
/// batch_write, with a 4 MiB memtable so writes flush between values.
#[test]
fn large_value_batch_write_flush_readback() {
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    let db = DbImpl::open_with_fs(opts(4 * 1024 * 1024), fs).unwrap();
    let cf = db.default_cf();

    let sizes = [
        512 * 1024usize,
        1024 * 1024,
        1024 * 1024 + 1, // just over the old MAX_KEY_LEN value cap
        2 * 1024 * 1024,
        3 * 1024 * 1024,
        8 * 1024 * 1024, // exceeds the 4 MiB write_buffer_size
    ];
    let mut stored: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (i, &sz) in sizes.iter().enumerate() {
        let key = format!("key{:04}", i).into_bytes();
        let val = vec![(i as u8).wrapping_add(1); sz];
        stored.push((key, val));
    }

    for (i, (k, v)) in stored.iter().enumerate() {
        let mut wb = WriteBatch::new();
        wb.put(&cf, k, v);
        db.batch_write(wb)
            .unwrap_or_else(|e| panic!("batch_write failed at i={i} val_len={}: {e:?}", v.len()));
    }

    db.flush_all().expect("flush_all");

    for (i, (k, v)) in stored.iter().enumerate() {
        let got = db
            .get(&cf, k)
            .unwrap_or_else(|e| panic!("get failed at i={i}: {e:?}"));
        let got = got.unwrap_or_else(|| panic!("value MISSING at i={i} key={k:?}"));
        assert_eq!(
            got.len(),
            v.len(),
            "value LENGTH mismatch at i={i}: got {} expected {}",
            got.len(),
            v.len()
        );
        assert!(got == *v, "value BYTES mismatch at i={i} len={}", v.len());
    }
}

/// Many large values in a SINGLE batch (mirrors a MapStateArrowBuffer flush
/// that accumulates several big join-state values before crossing the FFI).
#[test]
fn large_value_single_big_batch() {
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    let db = DbImpl::open_with_fs(opts(64 * 1024 * 1024), fs).unwrap();
    let cf = db.default_cf();

    // 20 values × ~3 MiB = ~60 MiB in one batch, under the 256 MiB aggregate
    // cap but well over a single 64 MiB memtable — forces a switch mid-batch.
    let n = 20usize;
    let mut stored: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for i in 0..n {
        let key = format!("k{:06}", i).into_bytes();
        let val = vec![(i as u8).wrapping_add(7); 3 * 1024 * 1024];
        stored.push((key, val));
    }
    let mut wb = WriteBatch::new();
    for (k, v) in &stored {
        wb.put(&cf, k, v);
    }
    db.batch_write(wb).expect("single big batch_write");
    db.flush_all().expect("flush_all");

    for (i, (k, v)) in stored.iter().enumerate() {
        let got = db
            .get(&cf, k)
            .unwrap()
            .unwrap_or_else(|| panic!("missing i={i}"));
        assert_eq!(got.len(), v.len(), "len mismatch i={i}");
        assert!(got == *v, "bytes mismatch i={i}");
    }
}

/// CONCURRENT q4/q7 stress: many threads batch_writing large values to a
/// shared DB on the object-store (opendal) backend while flushes fire
/// concurrently — mirrors 4 parallel join subtasks + async writes + a
/// concurrent flush worker. Surfaces races / write-stall / FrozenMemTable
/// retry-exhaustion / heap corruption under load.
#[test]
fn large_value_concurrent_object_store_stress() {
    let fs: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::memory().expect("memory opendal"));
    // 4 MiB memtable so a couple of multi-MiB values force a freeze+flush
    // while other threads keep writing → flush/write contention.
    let db = DbImpl::open_with_fs(opts(4 * 1024 * 1024), fs).expect("open");

    let n_threads = 4usize;
    let per_thread = 60usize;
    let mut handles = Vec::new();
    for t in 0..n_threads {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let cf = db.default_cf();
            for i in 0..per_thread {
                // sizes 256 KiB .. ~2 MiB, varied so freezes land mid-stream
                let sz = 256 * 1024 + (i % 8) * 256 * 1024;
                let key = format!("t{t:02}_k{i:04}").into_bytes();
                let val = vec![((t * 31 + i) as u8).wrapping_add(1); sz];
                let mut wb = WriteBatch::new();
                wb.put(&cf, &key, &val);
                if let Err(e) = db.batch_write(wb) {
                    return Err(format!("thread {t} i={i} len={sz}: {e:?}"));
                }
            }
            Ok(())
        }));
    }
    let mut errs = Vec::new();
    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => errs.push(e),
            Err(_) => errs.push("thread PANICKED".to_string()),
        }
    }
    assert!(
        errs.is_empty(),
        "concurrent large-value writes failed:\n{}",
        errs.join("\n")
    );

    db.flush_all().expect("flush_all");

    // Verify every value round-trips correctly (no corruption / torn writes).
    let cf = db.default_cf();
    for t in 0..n_threads {
        for i in 0..per_thread {
            let sz = 256 * 1024 + (i % 8) * 256 * 1024;
            let key = format!("t{t:02}_k{i:04}").into_bytes();
            let got = db
                .get(&cf, &key)
                .unwrap_or_else(|e| panic!("get t={t} i={i}: {e:?}"))
                .unwrap_or_else(|| panic!("MISSING t={t} i={i}"));
            assert_eq!(got.len(), sz, "len mismatch t={t} i={i}");
            assert_eq!(
                got[0],
                ((t * 31 + i) as u8).wrapping_add(1),
                "byte mismatch t={t} i={i}"
            );
        }
    }
}

/// THE q4/q7 PATH: large values flushed to an OBJECT-STORE backend (opendal
/// Memory scheme → supports_atomic_rename()==false → the direct-write /
/// multipart flush path that S3 uses, NOT the rename path MemoryFileSystem
/// takes). Write large values, flush, DROP, reopen from the shared operator
/// (forces reads from the flushed SSTs on the object store), verify.
#[test]
fn large_value_opendal_object_store_flush_restart() {
    // Shared in-memory opendal operator so a second fs reads the first's SSTs.
    let first = OpendalFileSystem::memory().expect("build memory opendal fs");
    let op = first.operator();
    let second = OpendalFileSystem::with_operator(op).expect("rebuild fs from operator");
    let fs_writer: Arc<dyn FileSystem> = Arc::new(first);
    let fs_reader: Arc<dyn FileSystem> = Arc::new(second);

    let sizes = [
        512 * 1024usize,
        1024 * 1024 + 1,
        2 * 1024 * 1024,
        5 * 1024 * 1024,
        9 * 1024 * 1024, // > 8 MiB MULTIPART_CHUNK_BYTES → multi-part upload
    ];
    let mut stored: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (i, &sz) in sizes.iter().enumerate() {
        let key = format!("ostore_key_{:04}", i).into_bytes();
        let val = vec![(i as u8).wrapping_add(11); sz];
        stored.push((key, val));
    }

    {
        let db = DbImpl::open_with_fs(opts(4 * 1024 * 1024), fs_writer).expect("open writer");
        let cf = db.default_cf();
        for (i, (k, v)) in stored.iter().enumerate() {
            let mut wb = WriteBatch::new();
            wb.put(&cf, k, v);
            db.batch_write(wb).unwrap_or_else(|e| {
                panic!("object-store batch_write i={i} len={}: {e:?}", v.len())
            });
        }
        db.flush_all().expect("object-store flush_all");

        // PRIMARY assertion — this is the q4/q7 runtime path: read back the
        // flushed values from the SAME live instance (its own SSTs on the
        // object store). Flink never reopens mid-job; it reads its own
        // flushed SSTs. If THIS fails, flush genuinely lost/corrupted the
        // large value on the direct-write object-store path.
        for (i, (k, v)) in stored.iter().enumerate() {
            let got = db
                .get(&cf, k)
                .unwrap_or_else(|e| panic!("SAME-INSTANCE get i={i}: {e:?}"))
                .unwrap_or_else(|| {
                    panic!(
                        "SAME-INSTANCE value MISSING after flush i={i} len={}",
                        v.len()
                    )
                });
            assert_eq!(
                got.len(),
                v.len(),
                "SAME-INSTANCE value LENGTH mismatch i={i}: got {} expected {}",
                got.len(),
                v.len()
            );
            assert!(
                got == *v,
                "SAME-INSTANCE value BYTES mismatch i={i} len={}",
                v.len()
            );
        }
        drop(db);
    }

    // SECONDARY: reopen from the shared operator. NOTE plain open_with_fs may
    // not auto-recover SSTs without a manifest/checkpoint, so a miss here is
    // not necessarily flush data loss — the same-instance check above is the
    // authoritative q4/q7 oracle.
    let db2 = DbImpl::open_with_fs(opts(4 * 1024 * 1024), fs_reader).expect("reopen reader");
    let cf2 = db2.default_cf();
    let mut recovered = 0usize;
    for (k, v) in &stored {
        if let Ok(Some(got)) = db2.get(&cf2, k) {
            if got == *v {
                recovered += 1;
            }
        }
    }
    eprintln!(
        "[repro] plain-reopen recovered {recovered}/{} large values (Flink recovers via \
         checkpoint, not plain reopen; informational only)",
        stored.len()
    );
}
