/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forstrs;

import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Ordering-hazard predicates for {@link VectorizedExecutor}'s batch dispatch: decides whether a
 * classified batch carries a same-key / delete-conflict dependency that requires offer-order
 * (sync) dispatch instead of bucket-order vectorized dispatch. Covers both the per-kind staging
 * layout and the Stage-3 Unit-2 unified mixed-column layout (row-index-mapped twins). Extracted
 * verbatim from VectorizedExecutor (checkstyle FileLength); all predicates are pure functions of
 * the classifier's off-heap buffers.
 */
final class DispatchOrderingHazards {

    private DispatchOrderingHazards() {}

    static boolean requiresOrderedDispatch(VectorizedClassifier classifier) {
        // Stage-3 Unit-2: under mixed staging the put/delete/heap-merge KEYS
        // live in the unified mixed column (per-kind key buffers are empty),
        // so the hazard predicates read the same keys through the per-kind
        // row-index maps. Predicate semantics are IDENTICAL to the per-kind
        // path below — same batches route to offer-order sync dispatch.
        if (classifier.isMixedStaging()) {
            return requiresOrderedDispatchMixed(classifier);
        }
        int writes =
                classifier.putCount() + classifier.deleteCount() + classifier.appendMergeCount();
        if (writes == 0) {
            return false;
        }
        // A pure GET+PUT batch can still carry a real read-modify-write ordering
        // dependency for the same state cell. Window aggregates issue
        // asyncValue(window).thenCompose(asyncUpdate(window, acc)); if a later
        // callback lets the UPDATE and a subsequent GET for the same composite
        // key drain in one classifier, bucket-order dispatch (PUT before GET)
        // makes the GET observe the just-written accumulator instead of the
        // prior value. For HOP windows this over-counts by the overlap factor.
        //
        // Keep the performance restoration for non-overlapping GET+PUT batches,
        // but exact same composite-key overlap must preserve offer order.
        if (hasSameKey(
                classifier.getKeys(),
                classifier.getCount(),
                classifier.putKeys(),
                classifier.putCount())) {
            return true;
        }
        if (classifier.deleteCount() == 0 && classifier.appendMergeCount() == 0) {
            return false;
        }
        AppendMergeBatchBuffer appendMerge = classifier.appendMergeBuffer();
        ColumnarBatchBuffer heapAppendKeys = appendMerge == null ? null : appendMerge.keyBuffer();
        int heapAppendCount = heapAppendKeys == null ? 0 : heapAppendKeys.count();
        ColumnarBatchBuffer offHeapAppendKeys = classifier.offHeapAppendMergeKeys();
        int offHeapAppendCount = offHeapAppendKeys == null ? 0 : offHeapAppendKeys.count();
        if (hasSameKey(
                        classifier.getKeys(), classifier.getCount(), heapAppendKeys, heapAppendCount)
                || hasSameKey(
                        classifier.getKeys(),
                        classifier.getCount(),
                        offHeapAppendKeys,
                        offHeapAppendCount)
                || hasSameKey(
                        classifier.putKeys(), classifier.putCount(), heapAppendKeys, heapAppendCount)
                || hasSameKey(
                        classifier.putKeys(),
                        classifier.putCount(),
                        offHeapAppendKeys,
                        offHeapAppendCount)) {
            return true;
        }
        return hasDeleteOrderingHazard(
                classifier, heapAppendKeys, heapAppendCount, offHeapAppendKeys, offHeapAppendCount);
    }

    private static boolean hasDeleteOrderingHazard(
            VectorizedClassifier classifier,
            ColumnarBatchBuffer heapAppendKeys,
            int heapAppendCount,
            ColumnarBatchBuffer offHeapAppendKeys,
            int offHeapAppendCount) {
        int deleteCount = classifier.deleteCount();
        if (deleteCount == 0) {
            return false;
        }
        StateRequest<?, ?, ?, ?>[] deleteReqs = classifier.deleteRequests();
        ColumnarBatchBuffer deleteKeys = classifier.deleteKeys();
        for (int i = 0; i < deleteCount; i++) {
            boolean prefixDelete = deleteReqs[i].getRequestType() == StateRequestType.CLEAR;
            if (hasDeleteConflict(
                            deleteKeys, i, classifier.getKeys(), classifier.getCount(), prefixDelete)
                    || hasDeleteConflict(
                            deleteKeys, i, classifier.putKeys(), classifier.putCount(), prefixDelete)
                    || hasDeleteConflict(deleteKeys, i, heapAppendKeys, heapAppendCount, prefixDelete)
                    || hasDeleteConflict(
                            deleteKeys, i, offHeapAppendKeys, offHeapAppendCount, prefixDelete)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Stage-3 Unit-2: mixed-staging twin of {@link #requiresOrderedDispatch}. Same predicates,
     * same routing decisions — only the key SOURCE differs: put / delete / heap-merge keys are
     * rows of the unified {@code mixedKeys} column, addressed through the classifier's per-kind
     * row-index maps ({@code mixedPutRows} / {@code mixedDeleteRows} / {@code mixedMergeRows}).
     * Off-heap APPEND_MERGE rows never enter the mixed column and keep using {@code
     * offHeapAppendMergeKeys}, exactly as the per-kind path does.
     */
    static boolean requiresOrderedDispatchMixed(VectorizedClassifier c) {
        int writes = c.putCount() + c.deleteCount() + c.appendMergeCount();
        if (writes == 0) {
            return false;
        }
        ColumnarBatchBuffer mixedKeys = c.mixedKeys();
        // Same composite-key GET+PUT RMW dependency as the per-kind path.
        if (hasSameKeyRows(
                c.getKeys(), c.getCount(), null, mixedKeys, c.putCount(), c.mixedPutRows())) {
            return true;
        }
        if (c.deleteCount() == 0 && c.appendMergeCount() == 0) {
            return false;
        }
        ColumnarBatchBuffer offHeapAppendKeys = c.offHeapAppendMergeKeys();
        int offHeapAppendCount = offHeapAppendKeys == null ? 0 : offHeapAppendKeys.count();
        int heapMergeCount = c.mixedMergeRowCount();
        int[] heapMergeRows = c.mixedMergeRows();
        if (hasSameKeyRows(
                        c.getKeys(), c.getCount(), null, mixedKeys, heapMergeCount, heapMergeRows)
                || hasSameKey(c.getKeys(), c.getCount(), offHeapAppendKeys, offHeapAppendCount)
                || hasSameKeyRows(
                        mixedKeys,
                        c.putCount(),
                        c.mixedPutRows(),
                        mixedKeys,
                        heapMergeCount,
                        heapMergeRows)
                || hasSameKeyRows(
                        mixedKeys,
                        c.putCount(),
                        c.mixedPutRows(),
                        offHeapAppendKeys,
                        offHeapAppendCount,
                        null)) {
            return true;
        }
        return hasDeleteOrderingHazardMixed(
                c, offHeapAppendKeys, offHeapAppendCount, heapMergeCount, heapMergeRows);
    }

    /** Stage-3 Unit-2: mixed-staging twin of {@link #hasDeleteOrderingHazard}. */
    private static boolean hasDeleteOrderingHazardMixed(
            VectorizedClassifier c,
            ColumnarBatchBuffer offHeapAppendKeys,
            int offHeapAppendCount,
            int heapMergeCount,
            int[] heapMergeRows) {
        int deleteCount = c.deleteCount();
        if (deleteCount == 0) {
            return false;
        }
        StateRequest<?, ?, ?, ?>[] deleteReqs = c.deleteRequests();
        ColumnarBatchBuffer mixedKeys = c.mixedKeys();
        int[] deleteRows = c.mixedDeleteRows();
        for (int i = 0; i < deleteCount; i++) {
            boolean prefixDelete = deleteReqs[i].getRequestType() == StateRequestType.CLEAR;
            int dRow = deleteRows[i];
            if (hasDeleteConflictRows(
                            mixedKeys, dRow, c.getKeys(), c.getCount(), null, prefixDelete)
                    || hasDeleteConflictRows(
                            mixedKeys,
                            dRow,
                            mixedKeys,
                            c.putCount(),
                            c.mixedPutRows(),
                            prefixDelete)
                    || hasDeleteConflictRows(
                            mixedKeys, dRow, mixedKeys, heapMergeCount, heapMergeRows, prefixDelete)
                    || hasDeleteConflictRows(
                            mixedKeys,
                            dRow,
                            offHeapAppendKeys,
                            offHeapAppendCount,
                            null,
                            prefixDelete)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Stage-3 Unit-2: row-index-mapped variant of {@link #hasSameKey}. {@code leftRows} /
     * {@code rightRows} map logical index {@code i} to the physical buffer row; {@code null}
     * means identity (today's dense-buffer behavior). Same O(N+M) hash-probe shape.
     */
    private static boolean hasSameKeyRows(
            ColumnarBatchBuffer left,
            int leftCount,
            int[] leftRows,
            ColumnarBatchBuffer right,
            int rightCount,
            int[] rightRows) {
        if (left == null || right == null || leftCount == 0 || rightCount == 0) {
            return false;
        }
        ColumnarBatchBuffer small;
        int smallCount;
        int[] smallRows;
        ColumnarBatchBuffer large;
        int largeCount;
        int[] largeRows;
        if (leftCount <= rightCount) {
            small = left;
            smallCount = leftCount;
            smallRows = leftRows;
            large = right;
            largeCount = rightCount;
            largeRows = rightRows;
        } else {
            small = right;
            smallCount = rightCount;
            smallRows = rightRows;
            large = left;
            largeCount = leftCount;
            largeRows = leftRows;
        }
        java.util.HashSet<SliceKey> probe = new java.util.HashSet<>(smallCount * 2);
        for (int i = 0; i < smallCount; i++) {
            probe.add(new SliceKey(small, smallRows == null ? i : smallRows[i]));
        }
        for (int j = 0; j < largeCount; j++) {
            if (probe.contains(new SliceKey(large, largeRows == null ? j : largeRows[j]))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Stage-3 Unit-2: row-index-mapped variant of {@link #hasDeleteConflict}; {@code otherRows ==
     * null} means identity mapping.
     */
    private static boolean hasDeleteConflictRows(
            ColumnarBatchBuffer deleteKeys,
            int deleteRow,
            ColumnarBatchBuffer other,
            int otherCount,
            int[] otherRows,
            boolean prefixDelete) {
        if (other == null || otherCount == 0) {
            return false;
        }
        for (int i = 0; i < otherCount; i++) {
            int row = otherRows == null ? i : otherRows[i];
            if (prefixDelete
                    ? sliceStartsWith(other, row, deleteKeys, deleteRow)
                    : sliceEquals(deleteKeys, deleteRow, other, row)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasSameKey(
            ColumnarBatchBuffer left, int leftCount, ColumnarBatchBuffer right, int rightCount) {
        if (left == null || right == null || leftCount == 0 || rightCount == 0) {
            return false;
        }
        // B-C5R1-NEW-H4: O(N+M) hash-set probe replaces the prior O(N×M)
        // nested loop. For mixed batches of 1024 rows the old path did ~1M
        // scalar byte compares per drain just to detect ordering hazards;
        // the hash-set probe collapses this to N hashes + M probes. Build
        // the set on the smaller buffer to bound memory.
        ColumnarBatchBuffer small;
        int smallCount;
        ColumnarBatchBuffer large;
        int largeCount;
        if (leftCount <= rightCount) {
            small = left;
            smallCount = leftCount;
            large = right;
            largeCount = rightCount;
        } else {
            small = right;
            smallCount = rightCount;
            large = left;
            largeCount = leftCount;
        }
        // Build a HashSet of SliceKey wrappers over the smaller buffer.
        // Capacity sized to avoid rehash on typical batch sizes (256-1024).
        java.util.HashSet<SliceKey> probe = new java.util.HashSet<>(smallCount * 2);
        for (int i = 0; i < smallCount; i++) {
            probe.add(new SliceKey(small, i));
        }
        for (int j = 0; j < largeCount; j++) {
            if (probe.contains(new SliceKey(large, j))) {
                return true;
            }
        }
        return false;
    }

    /**
     * B-C5R1-NEW-H4 helper: wraps a (buffer, row) tuple with hashCode/equals
     * computed over the off-heap key bytes. Allocates one object per probe
     * step; the alternative O(N×M) nested-loop scalar compare scaled
     * quadratically with batch size and dominated mixed-batch drain latency.
     */
    private static final class SliceKey {
        private final ColumnarBatchBuffer buf;
        private final int row;
        private final int len;
        private final int hash;

        SliceKey(ColumnarBatchBuffer buf, int row) {
            this.buf = buf;
            this.row = row;
            int start = sliceStart(buf, row);
            int end = sliceEnd(buf, row);
            this.len = end - start;
            this.hash = computeHash(buf.dataSegment(), start, len);
        }

        private static int computeHash(MemorySegment seg, int off, int len) {
            // M5-V4 (hot-path alloc/copy audit 2026-06-12): the previous
            // FNV-1a fold was an inherently sequential 1-byte-at-a-time loop
            // (data-dependent XOR between multiplies — cannot be strided).
            // Replaced with the strided polynomial-31 segment hash (8 bytes
            // per getLong, recurrence broken via precomputed 31^k powers —
            // same shape as the Arrays.hashCode intrinsic). The hash value
            // CHANGES vs FNV-1a, which is safe here: SliceKey hashes are
            // transient per-batch HashSet probes (never persisted, never
            // compared across processes) and equals() verifies the bytes.
            return SegmentHash.polynomial31(seg, off, len);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SliceKey other)) {
                return false;
            }
            if (this.hash != other.hash || this.len != other.len) {
                return false;
            }
            return sliceEquals(this.buf, this.row, other.buf, other.row);
        }
    }

    private static boolean hasDeleteConflict(
            ColumnarBatchBuffer deleteKeys,
            int deleteRow,
            ColumnarBatchBuffer other,
            int otherCount,
            boolean prefixDelete) {
        if (other == null || otherCount == 0) {
            return false;
        }
        for (int i = 0; i < otherCount; i++) {
            if (prefixDelete
                    ? sliceStartsWith(other, i, deleteKeys, deleteRow)
                    : sliceEquals(deleteKeys, deleteRow, other, i)) {
                return true;
            }
        }
        return false;
    }

    private static boolean sliceEquals(
            ColumnarBatchBuffer left, int leftRow, ColumnarBatchBuffer right, int rightRow) {
        int leftStart = sliceStart(left, leftRow);
        int leftLen = sliceEnd(left, leftRow) - leftStart;
        int rightStart = sliceStart(right, rightRow);
        int rightLen = sliceEnd(right, rightRow) - rightStart;
        if (leftLen != rightLen) {
            return false;
        }
        return sliceBytesEqual(
                left.dataSegment(), leftStart, right.dataSegment(), rightStart, leftLen);
    }

    private static boolean sliceStartsWith(
            ColumnarBatchBuffer value, int valueRow, ColumnarBatchBuffer prefix, int prefixRow) {
        int valueStart = sliceStart(value, valueRow);
        int valueLen = sliceEnd(value, valueRow) - valueStart;
        int prefixStart = sliceStart(prefix, prefixRow);
        int prefixLen = sliceEnd(prefix, prefixRow) - prefixStart;
        if (prefixLen > valueLen) {
            return false;
        }
        return sliceBytesEqual(
                value.dataSegment(), valueStart, prefix.dataSegment(), prefixStart, prefixLen);
    }

    private static int sliceStart(ColumnarBatchBuffer buffer, int row) {
        return buffer.offsetsSegment().get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
    }

    private static int sliceEnd(ColumnarBatchBuffer buffer, int row) {
        return buffer.offsetsSegment().get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
    }

    private static boolean sliceBytesEqual(
            MemorySegment left, long leftStart, MemorySegment right, long rightStart, int len) {
        // M5-V4 (hot-path alloc/copy audit 2026-06-12): MemorySegment.mismatch
        // is a JIT intrinsic (SWAR/SIMD compare, branch-cheap for the common
        // equal case) — replaces the scalar per-byte loop that ran on every
        // classified batch with writes, per probe-hit and per delete-conflict
        // scan. Semantics identical: equal iff no mismatching byte in
        // [0, len); len == 0 compares empty ranges and returns -1 (equal).
        return MemorySegment.mismatch(
                        left, leftStart, leftStart + len, right, rightStart, rightStart + len)
                == -1;
    }
}
