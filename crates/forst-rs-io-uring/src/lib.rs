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

//! Linux io_uring [`BlockIo`] backend for the SST block prefetcher
//! (streaming-read redesign, io_uring stage).
//!
//! Production runs on Linux/docker; the prefetcher's multi-region window
//! reads are submitted as one io_uring batch (N read SQEs, one
//! `submit_and_wait`) against the SST's local fd, instead of N serial
//! `pread(2)` calls. Results are bit-identical to
//! [`forst_rs_io::PreadBlockIo`] by contract (and by test).
//!
//! This lives in its OWN crate because pushing to the submission queue is
//! `unsafe` (the kernel reads the caller's buffers asynchronously), while
//! `forst-rs-io` / `forst-rs-storage` are `#![forbid(unsafe_code)]`. The
//! unsafe surface here is small and fully contained: buffers outlive the
//! ring round-trip because [`BlockIo::read_at_vectored`] blocks until every
//! completion is reaped before returning.
//!
//! Gating (resolved once per process):
//! - non-Linux: permanent stub — [`uring_available`] is `false` and
//!   [`UringBlockIo::new`] returns `None`.
//! - Linux: env `FRS_IO_URING=false|0` forces the pread fallback; otherwise
//!   a trivial ring setup is probed ONCE at first use — kernels without
//!   io_uring (< 5.6 for `IORING_OP_READ`, or seccomp/container-disabled)
//!   fall back silently.

#![allow(clippy::result_large_err)] // workspace-wide ForstError rationale (see forst-rs-common)

use std::fs::File;
use std::sync::Arc;

use forst_rs_io::BlockIo;

/// True when the io_uring backend is usable on this host (Linux + kernel
/// probe succeeded + `FRS_IO_URING` not disabled). Resolved once.
pub fn uring_available() -> bool {
    imp::uring_available()
}

/// io_uring-backed [`BlockIo`] over a local SST file handle.
///
/// Construct via [`UringBlockIo::new`], which returns `None` whenever the
/// backend is unavailable (non-Linux, gate off, probe failed) so callers can
/// chain straight into the [`forst_rs_io::PreadBlockIo`] fallback.
pub struct UringBlockIo {
    file: Arc<File>,
}

impl UringBlockIo {
    /// Wraps `file` if (and only if) the io_uring backend is available.
    pub fn new(file: Arc<File>) -> Option<Self> {
        if uring_available() {
            Some(Self { file })
        } else {
            None
        }
    }
}

impl BlockIo for UringBlockIo {
    fn read_at_vectored(
        &self,
        regions: &[(u64, usize)],
        buf: &mut [u8],
    ) -> forst_rs_common::ForstResult<()> {
        imp::read_at_vectored(&self.file, regions, buf)
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use std::cell::RefCell;
    use std::fs::File;
    use std::os::unix::io::AsRawFd;
    use std::sync::OnceLock;

    use forst_rs_common::{ForstError, ForstResult};
    use io_uring::{opcode, types, IoUring};

    /// Ring depth: bounded batch per `submit_and_wait`. Prefetch windows are
    /// ≤ 64 blocks (remote 4 MiB cap @ 64 KiB blocks) and cache-hit splitting
    /// only shrinks them, so one batch nearly always suffices; larger region
    /// lists are processed in chunks of this depth.
    const RING_ENTRIES: u32 = 64;

    pub(super) fn uring_available() -> bool {
        static AVAILABLE: OnceLock<bool> = OnceLock::new();
        *AVAILABLE.get_or_init(|| {
            // Runtime gate: default TRUE on Linux; FRS_IO_URING=false|0 opts out.
            if matches!(
                std::env::var("FRS_IO_URING").ok().as_deref(),
                Some("0") | Some("false") | Some("FALSE")
            ) {
                return false;
            }
            // Probe once with a trivial ring setup; ENOSYS / EPERM (seccomp,
            // old kernel) ⇒ silent pread fallback.
            IoUring::new(2).is_ok()
        })
    }

    thread_local! {
        /// One ring per read-I/O pool thread, built lazily. Rebuilding a ring
        /// per window would pay io_uring_setup (a syscall + mmaps) per read.
        static RING: RefCell<Option<IoUring>> = const { RefCell::new(None) };
    }

    pub(super) fn read_at_vectored(
        file: &File,
        regions: &[(u64, usize)],
        buf: &mut [u8],
    ) -> ForstResult<()> {
        forst_rs_io::check_vectored_args(regions, buf.len())?;
        if regions.is_empty() {
            return Ok(());
        }
        let fd = types::Fd(file.as_raw_fd());

        // Pre-compute each region's destination offset in `buf` (regions pack
        // back-to-back in order).
        let mut dst_offs = Vec::with_capacity(regions.len());
        let mut cursor = 0usize;
        for &(_, len) in regions {
            dst_offs.push(cursor);
            cursor += len;
        }

        RING.with(|cell| -> ForstResult<()> {
            let mut ring_slot = cell.borrow_mut();
            if ring_slot.is_none() {
                *ring_slot = Some(IoUring::new(RING_ENTRIES).map_err(|e| {
                    ForstError::internal(format!("io_uring setup failed post-probe: {e}"))
                })?);
            }
            let ring = ring_slot.as_mut().expect("ring initialised above");

            let base = buf.as_mut_ptr();
            for chunk_start in (0..regions.len()).step_by(RING_ENTRIES as usize) {
                let chunk_end = (chunk_start + RING_ENTRIES as usize).min(regions.len());
                let n = chunk_end - chunk_start;
                {
                    let mut sq = ring.submission();
                    for i in chunk_start..chunk_end {
                        let (off, len) = regions[i];
                        // SAFETY (pointer): `dst_offs[i] + len <= buf.len()`
                        // by check_vectored_args, and regions' destinations
                        // are disjoint by construction (back-to-back packing).
                        let ptr = unsafe { base.add(dst_offs[i]) };
                        let sqe = opcode::Read::new(fd, ptr, len as u32)
                            .offset(off)
                            .build()
                            .user_data(i as u64);
                        // SAFETY (lifetime): the buffer and fd outlive the
                        // kernel's async read — we `submit_and_wait` for ALL
                        // n completions below before touching/returning the
                        // buffer; chunk size ≤ RING_ENTRIES so push succeeds.
                        unsafe {
                            sq.push(&sqe).map_err(|e| {
                                ForstError::internal(format!("io_uring SQ push failed: {e}"))
                            })?;
                        }
                    }
                }
                ring.submit_and_wait(n).map_err(|e| {
                    ForstError::internal(format!("io_uring submit_and_wait failed: {e}"))
                })?;
                let mut reaped = 0usize;
                while reaped < n {
                    let Some(cqe) = ring.completion().next() else {
                        ring.submit_and_wait(1).map_err(|e| {
                            ForstError::internal(format!("io_uring wait failed: {e}"))
                        })?;
                        continue;
                    };
                    reaped += 1;
                    let i = cqe.user_data() as usize;
                    let (off, len) = regions[i];
                    let res = cqe.result();
                    if res < 0 {
                        return Err(ForstError::internal(format!(
                            "io_uring read at offset {} len {} failed: errno {}",
                            off,
                            len,
                            -res
                        )));
                    }
                    let got = res as usize;
                    if got < len {
                        // Rare mid-file short read (signal/page boundary):
                        // finish the tail with positional pread — keeps the
                        // result bit-identical to the fallback path. EOF
                        // inside a region is a corruption error (regions come
                        // from the SST sparse index).
                        finish_short_read(file, off, len, got, buf, dst_offs[i])?;
                    }
                }
            }
            Ok(())
        })
    }

    fn finish_short_read(
        file: &File,
        off: u64,
        len: usize,
        mut filled: usize,
        buf: &mut [u8],
        dst_off: usize,
    ) -> ForstResult<()> {
        use std::os::unix::fs::FileExt;
        while filled < len {
            let n = file
                .read_at(&mut buf[dst_off + filled..dst_off + len], off + filled as u64)
                .map_err(|e| {
                    ForstError::internal(format!("pread tail after short uring read: {e}"))
                })?;
            if n == 0 {
                return Err(ForstError::corruption(format!(
                    "read_at_vectored: short read at offset {} (filled {} of {})",
                    off, filled, len
                )));
            }
            filled += n;
        }
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::fs::File;

    use forst_rs_common::{ForstError, ForstResult};

    pub(super) fn uring_available() -> bool {
        false
    }

    pub(super) fn read_at_vectored(
        _file: &File,
        _regions: &[(u64, usize)],
        _buf: &mut [u8],
    ) -> ForstResult<()> {
        // Unreachable in practice: `UringBlockIo::new` returns `None` off
        // Linux, so no instance exists to call this on.
        Err(ForstError::not_supported(
            "io_uring BlockIo is Linux-only".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use super::*;

    /// Trait-level equivalence: both BlockIo impls must return identical
    /// bytes for the same multi-region read over a real file. Linux-only
    /// (skipped on macOS dev hosts); silently passes if the kernel lacks
    /// io_uring (the probe gate is exactly the production fallback).
    #[cfg(target_os = "linux")]
    #[test]
    fn uring_and_pread_return_identical_bytes() {
        use forst_rs_io::{BlockIo, PreadBlockIo, RandomAccessFile};
        use std::io::Write;
        use std::sync::Arc;

        struct FileRaf(Arc<std::fs::File>, u64);
        impl RandomAccessFile for FileRaf {
            fn read_at(&self, offset: u64, buf: &mut [u8]) -> forst_rs_common::ForstResult<usize> {
                use std::os::unix::fs::FileExt;
                self.0
                    .read_at(buf, offset)
                    .map_err(|e| forst_rs_common::ForstError::internal(e.to_string()))
            }
            fn file_size(&self) -> forst_rs_common::ForstResult<u64> {
                Ok(self.1)
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blocks.bin");
        let data: Vec<u8> = (0..1 << 20).map(|i| (i * 31 % 251) as u8).collect();
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&data)
            .unwrap();
        let file = Arc::new(std::fs::File::open(&path).unwrap());

        // Mixed window shapes: contiguous run, gap, tail region, 1-byte region.
        let regions: Vec<(u64, usize)> = vec![
            (0, 4096),
            (4096, 4096),
            (65536, 1),
            (123_456, 70_000),
            ((1 << 20) - 512, 512),
        ];
        let total: usize = regions.iter().map(|&(_, l)| l).sum();

        let mut pread_buf = vec![0u8; total];
        PreadBlockIo(&FileRaf(Arc::clone(&file), data.len() as u64))
            .read_at_vectored(&regions, &mut pread_buf)
            .unwrap();

        let Some(uring) = UringBlockIo::new(Arc::clone(&file)) else {
            eprintln!("io_uring unavailable on this kernel — fallback path is the product");
            return;
        };
        let mut uring_buf = vec![0u8; total];
        uring.read_at_vectored(&regions, &mut uring_buf).unwrap();

        assert_eq!(pread_buf, uring_buf, "io_uring bytes must equal pread bytes");
        // And both must equal the source data per region.
        let mut cursor = 0;
        for &(off, len) in &regions {
            assert_eq!(
                &uring_buf[cursor..cursor + len],
                &data[off as usize..off as usize + len]
            );
            cursor += len;
        }
    }

    /// Off-Linux the constructor must refuse (stub crate contract).
    #[cfg(not(target_os = "linux"))]
    #[test]
    fn unavailable_off_linux() {
        assert!(!super::uring_available());
        let f = tempfile::tempfile().unwrap();
        assert!(super::UringBlockIo::new(std::sync::Arc::new(f)).is_none());
    }
}
