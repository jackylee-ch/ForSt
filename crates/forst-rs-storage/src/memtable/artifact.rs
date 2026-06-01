//! FRS-CKPT-NOFLUSH (2026-06-01): Arrow-IPC serialization of a memtable
//! snapshot (the `Vec<RecordBatch>` produced by
//! [`crate::memtable::ShardedMemTable::snapshot_batches`]) for the
//! checkpoint-without-flush path.
//!
//! A checkpoint serializes the LIVE memtable to one of these artifacts and
//! streams it to S3 (Arrow IPC, the goal's "Batch Checkpoint — Arrow IPC
//! streamed to S3, no Java heap"), instead of folding the memtable into an L0
//! SST. The memtable stays RAM-resident and unfragmented for reads (the
//! ckpt-OFF fast path), so heavy joins do not pay the S3-SST decode tax that
//! collapses them under ckpt-ON. On restore the artifact is replayed back into
//! a fresh memtable via `batch_insert_with_explicit_seqs` (sequences and all
//! versions preserved — see the round-trip test below and
//! `vectorized::snapshot_batches_round_trips_all_versions_*`).
//!
//! Format: a single Arrow IPC stream containing every batch (all batches share
//! the canonical memtable schema `key, value, sequence, op_type`). The reader
//! tolerates the empty-memtable case (no batches → empty artifact → empty
//! replay).

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use forst_rs_common::{ForstError, ForstResult};
use std::io::Cursor;
use std::sync::Arc;

/// The canonical memtable-snapshot schema, identical to the flush/SST batch
/// schema (`key Binary, value Binary?, sequence UInt64, op_type UInt8`).
fn artifact_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("op_type", DataType::UInt8, false),
    ]))
}

/// Serializes memtable-snapshot batches to a single Arrow IPC stream. An empty
/// `batches` (empty memtable) yields an empty `Vec<u8>` — `deserialize` maps it
/// back to no batches, so the round trip is exact for the empty case too.
pub fn serialize_memtable_batches(batches: &[RecordBatch]) -> ForstResult<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema).map_err(|e| {
        ForstError::corruption(format!(
            "memtable artifact StreamWriter creation failed: {e}"
        ))
    })?;
    for b in batches {
        writer
            .write(b)
            .map_err(|e| ForstError::corruption(format!("memtable artifact write failed: {e}")))?;
    }
    writer
        .finish()
        .map_err(|e| ForstError::corruption(format!("memtable artifact finish failed: {e}")))?;
    Ok(buf)
}

/// Streams memtable-snapshot batches as an Arrow IPC stream directly into
/// `writer`, dropping each batch immediately after it is written. Unlike
/// [`serialize_memtable_batches`], this never materializes a second full copy of
/// the memtable as an in-memory `Vec<u8>` — critical for the checkpoint-without-
/// flush path, where a multi-GB live memtable would otherwise be doubled in
/// memory (live memtable + serialized bytes) and OOM the TaskManager. The
/// caller passes the batches as an iterator (e.g. drained from a per-shard
/// builder) so they too can be released incrementally. An empty iterator
/// produces a zero-length stream, matching [`serialize_memtable_batches`]'s
/// empty case so the reader round-trips it to no batches.
pub fn serialize_memtable_batches_to_writer<W: std::io::Write>(
    batches: impl IntoIterator<Item = RecordBatch>,
    writer: W,
) -> ForstResult<()> {
    let mut iter = batches.into_iter();
    let Some(first) = iter.next() else {
        // Empty memtable: write nothing (matches serialize_memtable_batches's
        // empty -> empty Vec, which deserialize maps back to no batches).
        return Ok(());
    };
    let schema = first.schema();
    let mut sw = StreamWriter::try_new(writer, &schema).map_err(|e| {
        ForstError::corruption(format!(
            "memtable artifact StreamWriter creation failed: {e}"
        ))
    })?;
    sw.write(&first)
        .map_err(|e| ForstError::corruption(format!("memtable artifact write failed: {e}")))?;
    drop(first);
    for b in iter {
        sw.write(&b)
            .map_err(|e| ForstError::corruption(format!("memtable artifact write failed: {e}")))?;
        drop(b);
    }
    sw.finish()
        .map_err(|e| ForstError::corruption(format!("memtable artifact finish failed: {e}")))?;
    // Recover the inner writer and flush EXPLICITLY: a BufWriter passed in by
    // the caller only flushes on drop, which swallows I/O errors (e.g. ENOSPC)
    // and could leave a truncated artifact to be renamed into place. into_inner
    // returns the already-finished writer; surface any flush error here.
    let mut inner = sw
        .into_inner()
        .map_err(|e| ForstError::corruption(format!("memtable artifact into_inner failed: {e}")))?;
    inner.flush().map_err(ForstError::Io)?;
    Ok(())
}

/// Incremental Arrow-IPC writer for the checkpoint-without-flush snapshot. Feed
/// batches one at a time via [`Self::write`] (e.g. from
/// `snapshot_batches_bounded_for_each` across the active + immutable memtables);
/// each is written to the underlying stream and dropped by the caller, so peak
/// memory is one batch. The IPC header is created lazily on the FIRST non-empty
/// batch (so its schema is taken from real data and an all-empty memtable writes
/// nothing). [`Self::finish`] writes the EOS marker, flushes EXPLICITLY (so I/O
/// errors surface instead of being swallowed by a BufWriter drop-flush), and
/// returns whether any batch was written.
pub struct MemtableArtifactWriter<W: std::io::Write> {
    writer: Option<W>,
    sw: Option<StreamWriter<W>>,
}

impl<W: std::io::Write> MemtableArtifactWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: Some(writer),
            sw: None,
        }
    }

    /// Writes one batch. Empty batches are skipped (do not create the header).
    pub fn write(&mut self, batch: &RecordBatch) -> ForstResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.sw.is_none() {
            let w = self
                .writer
                .take()
                .expect("MemtableArtifactWriter writer already taken");
            let schema = batch.schema();
            self.sw = Some(StreamWriter::try_new(w, &schema).map_err(|e| {
                ForstError::corruption(format!(
                    "memtable artifact StreamWriter creation failed: {e}"
                ))
            })?);
        }
        self.sw
            .as_mut()
            .expect("StreamWriter present after lazy init")
            .write(batch)
            .map_err(|e| ForstError::corruption(format!("memtable artifact write failed: {e}")))
    }

    /// Finishes the stream and flushes. Returns `true` if any batch was written
    /// (the artifact has content), `false` if nothing was written (empty
    /// memtable — caller should discard the file rather than keep a 0-byte one).
    pub fn finish(mut self) -> ForstResult<bool> {
        let Some(mut sw) = self.sw.take() else {
            return Ok(false);
        };
        sw.finish()
            .map_err(|e| ForstError::corruption(format!("memtable artifact finish failed: {e}")))?;
        let mut inner = sw.into_inner().map_err(|e| {
            ForstError::corruption(format!("memtable artifact into_inner failed: {e}"))
        })?;
        inner.flush().map_err(ForstError::Io)?;
        Ok(true)
    }
}

/// Deserializes an Arrow IPC stream produced by [`serialize_memtable_batches`]
/// back into the batches. An empty input (empty memtable) yields no batches.
pub fn deserialize_memtable_batches(bytes: &[u8]) -> ForstResult<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        ForstError::corruption(format!(
            "memtable artifact StreamReader creation failed: {e}"
        ))
    })?;
    let mut batches = Vec::new();
    for b in reader {
        batches.push(
            b.map_err(|e| ForstError::corruption(format!("memtable artifact read failed: {e}")))?,
        );
    }
    Ok(batches)
}

/// Returns the canonical artifact schema (exposed for callers building empty
/// replay memtables / validating artifact column layout).
pub fn memtable_artifact_schema() -> Arc<Schema> {
    artifact_schema()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::{MemTableConfig, ShardedMemTable};

    fn cfg() -> MemTableConfig {
        MemTableConfig {
            max_size: 1 << 20,
            unsorted_merge_ratio: 0.25,
        }
    }

    #[test]
    fn empty_memtable_round_trips_to_empty_artifact() {
        let bytes = serialize_memtable_batches(&[]).unwrap();
        assert!(bytes.is_empty());
        let back = deserialize_memtable_batches(&bytes).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn sharded_snapshot_serialize_deserialize_round_trips() {
        // Build a sharded memtable with multi-version + tombstone, leave some
        // entries unsorted (no freeze), snapshot → IPC bytes → batches, and
        // verify the deserialized batches are row-identical to the snapshot.
        let mt = ShardedMemTable::new(4, cfg());
        // a@1=a1, b@2=b1, a@5=a2 (multi-version), c@3=tombstone.
        mt.put_with_seq(b"a", Some(b"a1"), 1, 1).unwrap();
        mt.put_with_seq(b"b", Some(b"b1"), 1, 2).unwrap();
        mt.put_with_seq(b"a", Some(b"a2"), 1, 5).unwrap();
        mt.put_with_seq(b"c", None, 0, 3).unwrap();

        let snap = mt.snapshot_batches(2).expect("snapshot_batches");
        let total_rows: usize = snap.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "4 versions across a(×2), b, c");

        let bytes = serialize_memtable_batches(&snap).expect("serialize");
        assert!(!bytes.is_empty());
        let back = deserialize_memtable_batches(&bytes).expect("deserialize");

        let back_rows: usize = back.iter().map(|b| b.num_rows()).sum();
        assert_eq!(back_rows, total_rows, "row count preserved through IPC");

        // Concatenate and compare schemas + a few cell values to confirm
        // byte-fidelity of the columnar payload.
        let schema = memtable_artifact_schema();
        let a = arrow::compute::concat_batches(&schema, snap.iter()).unwrap();
        let b = arrow::compute::concat_batches(&schema, back.iter()).unwrap();
        assert_eq!(a.num_rows(), b.num_rows());
        use arrow::array::{BinaryArray, UInt64Array, UInt8Array};
        let ak = a.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        let bk = b.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        let aseq = a.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
        let bseq = b.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
        let aop = a.column(3).as_any().downcast_ref::<UInt8Array>().unwrap();
        let bop = b.column(3).as_any().downcast_ref::<UInt8Array>().unwrap();
        for i in 0..a.num_rows() {
            assert_eq!(ak.value(i), bk.value(i), "key row {i}");
            assert_eq!(aseq.value(i), bseq.value(i), "seq row {i}");
            assert_eq!(aop.value(i), bop.value(i), "op row {i}");
        }
    }

    #[test]
    fn streaming_writer_matches_buffered_serializer_byte_for_byte() {
        // The streaming snapshot path must produce an IPC stream identical to
        // the buffered serializer's, so the same deserialize_memtable_batches
        // reader round-trips it. Build a representative snapshot, write it both
        // ways, and compare the bytes exactly.
        let mt = ShardedMemTable::new(4, cfg());
        mt.put_with_seq(b"a", Some(b"a1"), 1, 1).unwrap();
        mt.put_with_seq(b"b", Some(b"b1"), 1, 2).unwrap();
        mt.put_with_seq(b"a", Some(b"a2"), 1, 5).unwrap();
        mt.put_with_seq(b"c", None, 0, 3).unwrap();
        let snap = mt.snapshot_batches(2).expect("snapshot_batches");

        let buffered = serialize_memtable_batches(&snap).expect("buffered serialize");

        let mut streamed = Vec::new();
        serialize_memtable_batches_to_writer(snap.clone(), &mut streamed)
            .expect("streaming serialize");

        assert_eq!(
            buffered, streamed,
            "streaming writer must be byte-identical to buffered serializer"
        );

        // And it must still deserialize to the same rows.
        let back = deserialize_memtable_batches(&streamed).expect("deserialize streamed");
        let back_rows: usize = back.iter().map(|b| b.num_rows()).sum();
        assert_eq!(back_rows, 4);
    }

    #[test]
    fn streaming_writer_empty_iter_produces_empty_stream() {
        // Empty memtable: streaming an empty iterator must produce a zero-length
        // stream that deserialize maps back to no batches (parity with the
        // buffered empty case).
        let mut out = Vec::new();
        serialize_memtable_batches_to_writer(std::iter::empty(), &mut out).unwrap();
        assert!(out.is_empty(), "empty iter -> empty stream");
        assert!(deserialize_memtable_batches(&out).unwrap().is_empty());
    }
}
