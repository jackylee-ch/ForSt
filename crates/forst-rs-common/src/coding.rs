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

//! Binary encoding/decoding utilities for ForSt-RS.
//!
//! This module provides fixed-width and variable-length integer encoding
//! compatible with RocksDB/LevelDB wire formats:
//!
//! - **Fixed-width**: Little-endian 32-bit and 64-bit integers
//! - **Varint**: LEB128-style variable-length encoding (1–5 bytes for u32,
//!   1–10 bytes for u64)
//!
//! All functions operate on byte slices with explicit bounds checking,
//! returning [`ForstError::Corruption`] on truncated or malformed input.

use crate::error::{ForstError, ForstResult};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum bytes a varint-encoded `u32` can occupy.
pub const MAX_VARINT32_LEN: usize = 5;

/// Maximum bytes a varint-encoded `u64` can occupy.
pub const MAX_VARINT64_LEN: usize = 10;

// ---------------------------------------------------------------------------
// Fixed-width encoding (little-endian)
// ---------------------------------------------------------------------------

/// Encodes a `u32` as 4 little-endian bytes, appending to `dst`.
#[inline]
pub fn put_fixed32(dst: &mut Vec<u8>, value: u32) {
    dst.extend_from_slice(&value.to_le_bytes());
}

/// Encodes a `u64` as 8 little-endian bytes, appending to `dst`.
#[inline]
pub fn put_fixed64(dst: &mut Vec<u8>, value: u64) {
    dst.extend_from_slice(&value.to_le_bytes());
}

/// Decodes a little-endian `u32` from the first 4 bytes of `src`.
///
/// Returns `(value, bytes_consumed)`. Errors if `src.len() < 4`.
#[inline]
pub fn get_fixed32(src: &[u8]) -> ForstResult<(u32, usize)> {
    if src.len() < 4 {
        return Err(ForstError::corruption(
            "insufficient bytes for fixed32 decode",
        ));
    }
    let value = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
    Ok((value, 4))
}

/// Decodes a little-endian `u64` from the first 8 bytes of `src`.
///
/// Returns `(value, bytes_consumed)`. Errors if `src.len() < 8`.
#[inline]
pub fn get_fixed64(src: &[u8]) -> ForstResult<(u64, usize)> {
    if src.len() < 8 {
        return Err(ForstError::corruption(
            "insufficient bytes for fixed64 decode",
        ));
    }
    let value = u64::from_le_bytes([
        src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
    ]);
    Ok((value, 8))
}

// ---------------------------------------------------------------------------
// Varint encoding (LEB128, RocksDB-compatible)
// ---------------------------------------------------------------------------

/// Encodes a `u32` as a varint, appending 1–5 bytes to `dst`.
///
/// Uses the same encoding as LevelDB/RocksDB: each byte stores 7 data bits
/// and a continuation bit (MSB). The last byte has MSB = 0.
pub fn put_varint32(dst: &mut Vec<u8>, mut value: u32) {
    loop {
        if value < 0x80 {
            dst.push(value as u8);
            break;
        }
        dst.push((value as u8) | 0x80);
        value >>= 7;
    }
}

/// Encodes a `u64` as a varint, appending 1–10 bytes to `dst`.
pub fn put_varint64(dst: &mut Vec<u8>, mut value: u64) {
    loop {
        if value < 0x80 {
            dst.push(value as u8);
            break;
        }
        dst.push((value as u8) | 0x80);
        value >>= 7;
    }
}

/// Decodes a varint-encoded `u32` from the start of `src`.
///
/// Returns `(value, bytes_consumed)`. At most [`MAX_VARINT32_LEN`] bytes
/// are read. Returns [`ForstError::Corruption`] on truncated or overlong
/// input.
pub fn get_varint32(src: &[u8]) -> ForstResult<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;

    for (i, &byte) in src.iter().enumerate() {
        if i >= MAX_VARINT32_LEN {
            return Err(ForstError::corruption("varint32 too long"));
        }
        let value_bits = (byte & 0x7F) as u32;
        // Guard against overflow: if shift >= 32 the value doesn't fit u32.
        if shift >= 32 {
            return Err(ForstError::corruption("varint32 overflow"));
        }
        result |= value_bits << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
    }

    Err(ForstError::corruption(
        "unterminated varint32: unexpected end of input",
    ))
}

/// Decodes a varint-encoded `u64` from the start of `src`.
///
/// Returns `(value, bytes_consumed)`. At most [`MAX_VARINT64_LEN`] bytes
/// are read.
pub fn get_varint64(src: &[u8]) -> ForstResult<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    for (i, &byte) in src.iter().enumerate() {
        if i >= MAX_VARINT64_LEN {
            return Err(ForstError::corruption("varint64 too long"));
        }
        let value_bits = (byte & 0x7F) as u64;
        if shift >= 64 {
            return Err(ForstError::corruption("varint64 overflow"));
        }
        result |= value_bits << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
    }

    Err(ForstError::corruption(
        "unterminated varint64: unexpected end of input",
    ))
}

/// Returns the number of bytes needed to varint-encode `value`.
#[inline]
pub fn varint32_length(mut value: u32) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

/// Returns the number of bytes needed to varint-encode `value`.
#[inline]
pub fn varint64_length(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Fixed encoding ------------------------------------------------------

    #[test]
    fn test_fixed32_roundtrip() {
        for &v in &[0u32, 1, 255, 256, 65535, u32::MAX] {
            let mut buf = Vec::new();
            put_fixed32(&mut buf, v);
            assert_eq!(buf.len(), 4);
            let (decoded, consumed) = get_fixed32(&buf).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(consumed, 4);
        }
    }

    #[test]
    fn test_fixed64_roundtrip() {
        for &v in &[0u64, 1, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            put_fixed64(&mut buf, v);
            assert_eq!(buf.len(), 8);
            let (decoded, consumed) = get_fixed64(&buf).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(consumed, 8);
        }
    }

    #[test]
    fn test_fixed32_short_input() {
        assert!(get_fixed32(&[1, 2, 3]).is_err());
        assert!(get_fixed32(&[]).is_err());
    }

    #[test]
    fn test_fixed64_short_input() {
        assert!(get_fixed64(&[1, 2, 3, 4, 5, 6, 7]).is_err());
        assert!(get_fixed64(&[]).is_err());
    }

    #[test]
    fn test_fixed_little_endian() {
        let mut buf = Vec::new();
        put_fixed32(&mut buf, 0x04030201);
        assert_eq!(&buf, &[0x01, 0x02, 0x03, 0x04]);
    }

    // -- Varint encoding -----------------------------------------------------

    #[test]
    fn test_varint32_single_byte() {
        for v in 0..128u32 {
            let mut buf = Vec::new();
            put_varint32(&mut buf, v);
            assert_eq!(buf.len(), 1);
            let (decoded, consumed) = get_varint32(&buf).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(consumed, 1);
        }
    }

    #[test]
    fn test_varint32_multi_byte() {
        let cases: &[(u32, usize)] = &[
            (128, 2),
            (16383, 2),
            (16384, 3),
            (2_097_151, 3),
            (2_097_152, 4),
            (268_435_455, 4),
            (268_435_456, 5),
            (u32::MAX, 5),
        ];
        for &(value, expected_len) in cases {
            let mut buf = Vec::new();
            put_varint32(&mut buf, value);
            assert_eq!(buf.len(), expected_len, "encoding length for {}", value);
            let (decoded, consumed) = get_varint32(&buf).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(consumed, expected_len);
        }
    }

    #[test]
    fn test_varint64_roundtrip() {
        let cases: &[u64] = &[
            0,
            1,
            127,
            128,
            16383,
            16384,
            u32::MAX as u64,
            u64::MAX >> 1,
            u64::MAX,
        ];
        for &v in cases {
            let mut buf = Vec::new();
            put_varint64(&mut buf, v);
            let (decoded, consumed) = get_varint64(&buf).unwrap();
            assert_eq!(decoded, v, "roundtrip for {}", v);
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn test_varint32_empty_input() {
        assert!(get_varint32(&[]).is_err());
    }

    #[test]
    fn test_varint64_empty_input() {
        assert!(get_varint64(&[]).is_err());
    }

    #[test]
    fn test_varint32_length() {
        assert_eq!(varint32_length(0), 1);
        assert_eq!(varint32_length(127), 1);
        assert_eq!(varint32_length(128), 2);
        assert_eq!(varint32_length(u32::MAX), 5);
    }

    #[test]
    fn test_varint64_length() {
        assert_eq!(varint64_length(0), 1);
        assert_eq!(varint64_length(127), 1);
        assert_eq!(varint64_length(128), 2);
        assert_eq!(varint64_length(u64::MAX), 10);
    }

    // -- Multiple values in sequence ----------------------------------------

    #[test]
    fn test_multiple_varints_sequential() {
        let mut buf = Vec::new();
        put_varint32(&mut buf, 100);
        put_varint32(&mut buf, 200);
        put_varint64(&mut buf, 300);

        let (v1, n1) = get_varint32(&buf).unwrap();
        assert_eq!(v1, 100);
        let (v2, n2) = get_varint32(&buf[n1..]).unwrap();
        assert_eq!(v2, 200);
        let (v3, _n3) = get_varint64(&buf[n1 + n2..]).unwrap();
        assert_eq!(v3, 300);
    }

    #[test]
    fn test_mixed_fixed_and_varint() {
        let mut buf = Vec::new();
        put_fixed32(&mut buf, 42);
        put_varint32(&mut buf, 999);
        put_fixed64(&mut buf, 123456789);

        let (v1, n1) = get_fixed32(&buf).unwrap();
        assert_eq!(v1, 42);
        let (v2, n2) = get_varint32(&buf[n1..]).unwrap();
        assert_eq!(v2, 999);
        let (v3, _) = get_fixed64(&buf[n1 + n2..]).unwrap();
        assert_eq!(v3, 123456789);
    }
}
