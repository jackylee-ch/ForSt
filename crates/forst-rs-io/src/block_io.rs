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

//! §2.1 / io_uring backend (streaming-read redesign): the [`BlockIo`]
//! abstraction the SST block prefetcher issues its multi-region window reads
//! through.
//!
//! One call = N `(offset, len)` regions packed back-to-back, in order, into a
//! single caller buffer (zero-copy slice handoff downstream — the prefetcher
//! slices per-block regions out of the one buffer). Two implementations:
//!
//! - [`PreadBlockIo`] (here): serial positional reads over any
//!   [`RandomAccessFile`] — the portable fallback, used on non-Linux, when the
//!   file has no local fd (remote/S3 tier), or when the `FRS_IO_URING` gate is
//!   off. Bit-identical results to the io_uring backend by contract.
//! - `UringBlockIo` (crate `forst-rs-io-uring`): Linux io_uring submission of
//!   all N regions in one ring batch. Lives in its own crate because the
//!   submission queue requires `unsafe` and this crate (like the storage
//!   crate) is `#![forbid(unsafe_code)]`.

use forst_rs_common::{ForstError, ForstResult};

use crate::filesystem::RandomAccessFile;

/// Vectored positional block reader: reads every `(offset, len)` region into
/// `buf`, packed back-to-back in `regions` order.
///
/// Contract (both implementations):
/// - `buf.len()` MUST equal the sum of region lengths (checked).
/// - Every region is read FULLY; a short read (EOF inside a region) is an
///   error — block regions come from the SST sparse index and must exist.
/// - Implementations must be `Send + Sync` (called from read-I/O pool threads).
pub trait BlockIo: Send + Sync {
    /// Reads all `regions` into `buf` (packed back-to-back, in order).
    fn read_at_vectored(&self, regions: &[(u64, usize)], buf: &mut [u8]) -> ForstResult<()>;
}

/// Validates the shared `read_at_vectored` precondition.
pub fn check_vectored_args(regions: &[(u64, usize)], buf_len: usize) -> ForstResult<()> {
    let total: usize = regions.iter().map(|&(_, l)| l).sum();
    if total != buf_len {
        return Err(ForstError::invalid_argument(format!(
            "read_at_vectored: buf len {} != regions total {}",
            buf_len, total
        )));
    }
    Ok(())
}

/// Portable [`BlockIo`] over any [`RandomAccessFile`]: one full positional
/// read per region (the pre-io_uring behaviour, kept bit-identical).
pub struct PreadBlockIo<'a>(pub &'a dyn RandomAccessFile);

impl BlockIo for PreadBlockIo<'_> {
    fn read_at_vectored(&self, regions: &[(u64, usize)], buf: &mut [u8]) -> ForstResult<()> {
        check_vectored_args(regions, buf.len())?;
        let mut cursor = 0usize;
        for &(off, len) in regions {
            let dst = &mut buf[cursor..cursor + len];
            cursor += len;
            let mut filled = 0usize;
            while filled < len {
                let n = self.0.read_at(off + filled as u64, &mut dst[filled..])?;
                if n == 0 {
                    return Err(ForstError::corruption(format!(
                        "read_at_vectored: short read at offset {} (filled {} of {})",
                        off, filled, len
                    )));
                }
                filled += n;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MemFile(Vec<u8>);

    impl RandomAccessFile for MemFile {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
            let start = offset as usize;
            if start >= self.0.len() {
                return Ok(0);
            }
            let end = (start + buf.len()).min(self.0.len());
            buf[..end - start].copy_from_slice(&self.0[start..end]);
            Ok(end - start)
        }

        fn file_size(&self) -> ForstResult<u64> {
            Ok(self.0.len() as u64)
        }
    }

    #[test]
    fn pread_block_io_packs_regions_in_order() {
        let data: Vec<u8> = (0u8..=255).collect();
        let f = MemFile(data.clone());
        let regions = [(10u64, 4usize), (100, 8), (0, 2)];
        let mut buf = vec![0u8; 14];
        PreadBlockIo(&f).read_at_vectored(&regions, &mut buf).unwrap();
        assert_eq!(&buf[..4], &data[10..14]);
        assert_eq!(&buf[4..12], &data[100..108]);
        assert_eq!(&buf[12..], &data[0..2]);
    }

    #[test]
    fn pread_block_io_rejects_len_mismatch_and_eof() {
        let f = MemFile(vec![7u8; 32]);
        let mut buf = vec![0u8; 8];
        assert!(PreadBlockIo(&f)
            .read_at_vectored(&[(0, 4)], &mut buf)
            .is_err());
        // Region past EOF ⇒ short-read error.
        let mut buf = vec![0u8; 8];
        assert!(PreadBlockIo(&f)
            .read_at_vectored(&[(30, 8)], &mut buf)
            .is_err());
    }
}
