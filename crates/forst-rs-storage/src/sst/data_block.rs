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

//! DataBlock encode/decode using Arrow IPC serialization.
//!
//! A DataBlock is the primary unit of data storage in an SST file. Each block
//! contains an Arrow [`RecordBatch`] serialized via IPC streaming format,
//! optionally compressed, and preceded by a [`BlockHeader`].
//!
//! **Encode flow:**
//! 1. Serialize `RecordBatch` with Arrow IPC `StreamWriter`
//! 2. Compress the IPC bytes
//! 3. Compute masked CRC32C checksum
//! 4. Prepend a [`BlockHeader`]
//!
//! **Decode flow:**
//! 1. Parse the [`BlockHeader`] from the first 16 bytes
//! 2. Verify the checksum against the compressed payload
//! 3. Decompress to recover the IPC bytes
//! 4. Deserialize the `RecordBatch` with Arrow IPC `StreamReader`

use std::io::Cursor;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use forst_rs_common::{crc32c, mask_crc, CompressionType, ForstError, ForstResult};

use super::block_header::BlockHeader;
use super::compression::{compress, decompress};
use super::schema::{BLOCK_HEADER_SIZE, BLOCK_TYPE_DATA};

/// Encodes a [`RecordBatch`] into a data block (header + compressed payload).
///
/// The returned bytes contain:
/// - 16-byte [`BlockHeader`]
/// - Compressed Arrow IPC payload
pub fn encode_data_block(
    batch: &RecordBatch,
    compression: CompressionType,
) -> ForstResult<Vec<u8>> {
    // Step 1: Serialize RecordBatch to Arrow IPC stream bytes.
    let ipc_bytes = {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).map_err(|e| {
            ForstError::corruption(format!("Arrow IPC StreamWriter creation failed: {e}"))
        })?;
        writer
            .write(batch)
            .map_err(|e| ForstError::corruption(format!("Arrow IPC write failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| ForstError::corruption(format!("Arrow IPC finish failed: {e}")))?;
        buf
    };

    let uncompressed_size = ipc_bytes.len() as u32;

    // Step 2: Compress.
    let compressed = compress(&ipc_bytes, compression)?;
    let compressed_size = compressed.len() as u32;

    // Step 3: Compute masked CRC32C of compressed data.
    let checksum = mask_crc(crc32c(&compressed));

    // Step 4: Build header and prepend.
    let header = BlockHeader {
        block_type: BLOCK_TYPE_DATA,
        compression,
        uncompressed_size,
        compressed_size,
        checksum,
    };

    let mut result = header.encode();
    result.extend_from_slice(&compressed);
    Ok(result)
}

/// Decodes a data block (header + compressed payload) back into a [`RecordBatch`].
///
/// Verifies the CRC32C checksum before decompression.
pub fn decode_data_block(data: &[u8]) -> ForstResult<RecordBatch> {
    // Step 1: Parse header.
    if data.len() < BLOCK_HEADER_SIZE {
        return Err(ForstError::corruption(format!(
            "data block too short for header: expected at least {} bytes, got {}",
            BLOCK_HEADER_SIZE,
            data.len()
        )));
    }
    let header = BlockHeader::decode(data)?;

    // Step 2: Extract compressed payload.
    let payload_start = BLOCK_HEADER_SIZE;
    let payload_end = payload_start + header.compressed_size as usize;
    if data.len() < payload_end {
        return Err(ForstError::corruption(format!(
            "data block truncated: header says {} compressed bytes, but only {} available",
            header.compressed_size,
            data.len() - BLOCK_HEADER_SIZE
        )));
    }
    let compressed_data = &data[payload_start..payload_end];

    // Step 3: Verify checksum.
    let actual_checksum = mask_crc(crc32c(compressed_data));
    if actual_checksum != header.checksum {
        return Err(ForstError::corruption(format!(
            "data block checksum mismatch: expected 0x{:08X}, got 0x{:08X}",
            header.checksum, actual_checksum
        )));
    }

    // Step 4: Decompress.
    let ipc_bytes = decompress(
        compressed_data,
        header.compression,
        header.uncompressed_size as usize,
    )?;

    // Step 5: Deserialize RecordBatch from Arrow IPC stream.
    let cursor = Cursor::new(&ipc_bytes);
    let mut reader = StreamReader::try_new(cursor, None).map_err(|e| {
        ForstError::corruption(format!("Arrow IPC StreamReader creation failed: {e}"))
    })?;

    let batch = reader
        .next()
        .ok_or_else(|| ForstError::corruption("Arrow IPC stream contains no record batches"))?
        .map_err(|e| ForstError::corruption(format!("Arrow IPC read failed: {e}")))?;

    Ok(batch)
}

/// FRS-ZEROCOPY (2026-06-02): zero-copy decode of an uncompressed data block.
///
/// Takes the block as an Arrow [`Buffer`] (shared, ref-counted allocation) and
/// returns a [`RecordBatch`] whose column arrays SLICE that buffer — no
/// per-column copy, unlike [`decode_data_block`] (which deserializes through an
/// owned `Vec`). For uncompressed blocks the IPC payload is sliced zero-copy
/// from `block`; arrow's [`StreamDecoder`] keeps the array data zero-copy when
/// the payload is suitably aligned (it falls back to a copy only on
/// misalignment, so correctness never depends on alignment). Compressed blocks
/// must decompress into a fresh buffer first (no zero-copy possible), so this
/// is the win path only for `CompressionType::None` SSTs.
pub fn decode_data_block_zerocopy(block: &arrow::buffer::Buffer) -> ForstResult<RecordBatch> {
    use arrow::ipc::reader::StreamDecoder;

    let data = block.as_slice();
    if data.len() < BLOCK_HEADER_SIZE {
        return Err(ForstError::corruption(format!(
            "data block too short for header: expected at least {} bytes, got {}",
            BLOCK_HEADER_SIZE,
            data.len()
        )));
    }
    let header = BlockHeader::decode(data)?;
    let payload_start = BLOCK_HEADER_SIZE;
    let payload_end = payload_start + header.compressed_size as usize;
    if data.len() < payload_end {
        return Err(ForstError::corruption(format!(
            "data block truncated: header says {} compressed bytes, but only {} available",
            header.compressed_size,
            data.len() - BLOCK_HEADER_SIZE
        )));
    }
    let payload = &data[payload_start..payload_end];
    let actual_checksum = mask_crc(crc32c(payload));
    if actual_checksum != header.checksum {
        return Err(ForstError::corruption(format!(
            "data block checksum mismatch: expected 0x{:08X}, got 0x{:08X}",
            header.checksum, actual_checksum
        )));
    }

    // Obtain the IPC stream as an aligned Arrow Buffer. Uncompressed → a
    // zero-copy slice that shares `block`'s allocation; compressed → a fresh
    // owned buffer (one decompress copy, then zero-copy column views over it).
    let mut ipc_buf: arrow::buffer::Buffer = if header.compression == CompressionType::None {
        block.slice_with_length(payload_start, header.compressed_size as usize)
    } else {
        let ipc = decompress(
            payload,
            header.compression,
            header.uncompressed_size as usize,
        )?;
        arrow::buffer::Buffer::from_vec(ipc)
    };

    let mut decoder = StreamDecoder::new();
    match decoder
        .decode(&mut ipc_buf)
        .map_err(|e| ForstError::corruption(format!("Arrow IPC zero-copy decode failed: {e}")))?
    {
        Some(batch) => Ok(batch),
        None => Err(ForstError::corruption(
            "Arrow IPC stream contains no complete record batch",
        )),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray, UInt64Array, UInt8Array};
    use std::sync::Arc;

    /// Creates a test [`RecordBatch`] with the SST schema.
    fn make_test_batch(
        keys: Vec<&[u8]>,
        values: Vec<Option<&[u8]>>,
        sequences: Vec<u64>,
        op_types: Vec<u8>,
    ) -> RecordBatch {
        let schema = Arc::new(super::super::schema::sst_schema());

        let key_array = BinaryArray::from_iter_values(keys);
        let value_array = BinaryArray::from(values.into_iter().collect::<Vec<Option<&[u8]>>>());
        let sequence_array = UInt64Array::from(sequences);
        let op_type_array = UInt8Array::from(op_types);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(key_array),
                Arc::new(value_array),
                Arc::new(sequence_array),
                Arc::new(op_type_array),
            ],
        )
        .expect("test batch construction should not fail")
    }

    #[test]
    fn test_encode_produces_header_plus_data() {
        let batch = make_test_batch(vec![b"key1"], vec![Some(b"val1")], vec![1], vec![0]);
        let encoded = encode_data_block(&batch, CompressionType::None).unwrap();
        assert!(
            encoded.len() > BLOCK_HEADER_SIZE,
            "encoded block must be larger than header"
        );

        // Verify the header is parseable.
        let header = BlockHeader::decode(&encoded).unwrap();
        assert_eq!(header.block_type, BLOCK_TYPE_DATA);
        assert_eq!(header.compression, CompressionType::None);
        assert_eq!(
            encoded.len(),
            BLOCK_HEADER_SIZE + header.compressed_size as usize
        );
    }

    #[test]
    fn test_roundtrip_no_compression() {
        let batch = make_test_batch(
            vec![b"alpha", b"beta", b"gamma"],
            vec![Some(b"v1"), None, Some(b"v3")],
            vec![100, 200, 300],
            vec![0, 1, 0],
        );

        let encoded = encode_data_block(&batch, CompressionType::None).unwrap();
        let decoded = decode_data_block(&encoded).unwrap();

        // Verify all 4 columns.
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 4);

        let keys = decoded
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(keys.value(0), b"alpha");
        assert_eq!(keys.value(1), b"beta");
        assert_eq!(keys.value(2), b"gamma");

        let values = decoded
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), b"v1");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), b"v3");

        let seqs = decoded
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(seqs.value(0), 100);
        assert_eq!(seqs.value(1), 200);
        assert_eq!(seqs.value(2), 300);

        let ops = decoded
            .column(3)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(ops.value(0), 0);
        assert_eq!(ops.value(1), 1);
        assert_eq!(ops.value(2), 0);
    }

    #[test]
    fn test_roundtrip_lz4() {
        let batch = make_test_batch(
            vec![b"k1", b"k2"],
            vec![Some(b"v1"), Some(b"v2")],
            vec![10, 20],
            vec![0, 0],
        );
        let encoded = encode_data_block(&batch, CompressionType::Lz4).unwrap();
        let decoded = decode_data_block(&encoded).unwrap();
        assert_eq!(decoded.num_rows(), 2);

        let keys = decoded
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(keys.value(0), b"k1");
        assert_eq!(keys.value(1), b"k2");
    }

    #[test]
    fn test_roundtrip_zstd() {
        let batch = make_test_batch(
            vec![b"zk1", b"zk2"],
            vec![Some(b"zv1"), Some(b"zv2")],
            vec![30, 40],
            vec![0, 1],
        );
        let encoded = encode_data_block(&batch, CompressionType::Zstd).unwrap();
        let decoded = decode_data_block(&encoded).unwrap();
        assert_eq!(decoded.num_rows(), 2);

        let ops = decoded
            .column(3)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(ops.value(0), 0);
        assert_eq!(ops.value(1), 1);
    }

    #[test]
    fn test_decode_corrupt_checksum() {
        let batch = make_test_batch(vec![b"key"], vec![Some(b"val")], vec![1], vec![0]);
        let mut encoded = encode_data_block(&batch, CompressionType::None).unwrap();

        // Corrupt the checksum (bytes 12-15).
        encoded[12] ^= 0xFF;

        let result = decode_data_block(&encoded);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("checksum mismatch"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_decode_truncated_data() {
        let batch = make_test_batch(vec![b"key"], vec![Some(b"val")], vec![1], vec![0]);
        let encoded = encode_data_block(&batch, CompressionType::None).unwrap();

        // Truncate: keep header + only half of the payload.
        let truncated_len = BLOCK_HEADER_SIZE + 2;
        let truncated = &encoded[..truncated_len];

        let result = decode_data_block(truncated);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("truncated"), "unexpected error: {err_msg}");
    }

    #[test]
    fn test_decode_too_short_for_header() {
        let short = vec![0u8; 8];
        let result = decode_data_block(&short);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("too short"), "unexpected error: {err_msg}");
    }

    #[test]
    fn zerocopy_decode_roundtrips_and_aliases_input_buffer() {
        use arrow::buffer::Buffer;
        // Large binary values so the value buffer is a substantial, easily
        // located slice (and so a copy vs zero-copy is unambiguous).
        let val: Vec<u8> = (0..1024u32).map(|i| (i % 251) as u8).collect();
        let batch = make_test_batch(
            vec![b"key-aaaa", b"key-bbbb", b"key-cccc"],
            vec![
                Some(val.as_slice()),
                Some(val.as_slice()),
                Some(val.as_slice()),
            ],
            vec![11, 22, 33],
            vec![0, 1, 0],
        );
        let encoded = encode_data_block(&batch, CompressionType::None).unwrap();
        let block = Buffer::from_vec(encoded);

        let decoded = decode_data_block_zerocopy(&block).unwrap();

        // Correctness: identical to the copying decoder.
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 4);
        let keys = decoded
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(keys.value(0), b"key-aaaa");
        assert_eq!(keys.value(2), b"key-cccc");
        let values = decoded
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), val.as_slice());
        assert_eq!(values.value(2), val.as_slice());
        let seqs = decoded
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(seqs.value(1), 22);

        // Zero-copy: the value column's bytes must alias the input block buffer
        // (no per-column copy). value_data() is the contiguous value buffer.
        let vptr = values.value_data().as_ptr() as usize;
        let lo = block.as_ptr() as usize;
        let hi = lo + block.len();
        assert!(
            vptr >= lo && vptr < hi,
            "zero-copy violated: value data ptr {vptr:#x} not within input block [{lo:#x}, {hi:#x})"
        );
    }

    #[test]
    fn zerocopy_decode_matches_copying_decode_lz4() {
        use arrow::buffer::Buffer;
        // Compressed path: must still decode correctly (one decompress copy,
        // then zero-copy column views over the decompressed buffer).
        let batch = make_test_batch(
            vec![b"z1", b"z2"],
            vec![Some(b"vv1"), Some(b"vv2")],
            vec![7, 8],
            vec![0, 1],
        );
        let encoded = encode_data_block(&batch, CompressionType::Lz4).unwrap();
        let block = Buffer::from_vec(encoded);
        let decoded = decode_data_block_zerocopy(&block).unwrap();
        let keys = decoded
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(keys.value(0), b"z1");
        assert_eq!(keys.value(1), b"z2");
    }

    #[test]
    fn test_lz4_compresses_repetitive_data() {
        // Build a batch with highly repetitive data to ensure LZ4
        // produces smaller output than no compression.
        let n = 200;
        let keys: Vec<&[u8]> = (0..n).map(|_| b"repetitive_key_data".as_slice()).collect();
        let values: Vec<Option<&[u8]>> = (0..n)
            .map(|_| Some(b"repetitive_value_data".as_slice()))
            .collect();
        let sequences: Vec<u64> = (0..n as u64).collect();
        let op_types: Vec<u8> = vec![0; n];

        let batch = make_test_batch(keys, values, sequences, op_types);

        let encoded_none = encode_data_block(&batch, CompressionType::None).unwrap();
        let encoded_lz4 = encode_data_block(&batch, CompressionType::Lz4).unwrap();

        assert!(
            encoded_lz4.len() < encoded_none.len(),
            "LZ4 should compress repetitive data: lz4={} vs none={}",
            encoded_lz4.len(),
            encoded_none.len()
        );

        // Verify roundtrip still works.
        let decoded = decode_data_block(&encoded_lz4).unwrap();
        assert_eq!(decoded.num_rows(), n);
    }
}
