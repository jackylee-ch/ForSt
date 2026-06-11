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

import org.apache.flink.annotation.Internal;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * M5-V2/V4 (hot-path alloc/copy audit 2026-06-12): copy-free polynomial-31 hash computed directly
 * over a {@link MemorySegment}.
 *
 * <p>Output is <b>bitwise-identical</b> to {@code java.util.Arrays.hashCode(byte[])} over the same
 * bytes: accumulator starts at {@code 1} and every byte (SIGNED) folds as {@code h = 31*h + b}.
 * That identity is a hard requirement for {@code ArrowTimerBuffer.hashOf} (open-addressed hash
 * slots store the hash — previously-populated slot layouts must remain compatible) and is
 * property-tested in {@code ArrowTimerBufferTest}.
 *
 * <p>The sequential per-byte recurrence is broken the same way the {@code Arrays.hashCode}
 * intrinsic does it: 8 bytes are read per stride ({@code getLong}, little-endian so byte order in
 * memory maps to shift order) and folded with the precomputed powers {@code 31^1..31^8}, scalar
 * tail of at most 7 bytes. Unlike the intrinsic route, this needs NO heap staging copy of the
 * segment slice, no thread-local scratch array, and no realloc-on-width-change.
 */
@Internal
public final class SegmentHash {

    private SegmentHash() {}

    /** Little-endian unaligned long view: byte at offset i == (byte) (v >>> (8*i)). */
    private static final ValueLayout.OfLong LE_LONG =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    // Precomputed 31^k (int wrap-around arithmetic, same as the folded recurrence).
    private static final int P31_2 = 31 * 31;
    private static final int P31_3 = P31_2 * 31;
    private static final int P31_4 = P31_3 * 31;
    private static final int P31_5 = P31_4 * 31;
    private static final int P31_6 = P31_5 * 31;
    private static final int P31_7 = P31_6 * 31;
    private static final int P31_8 = P31_7 * 31;

    /**
     * Polynomial-31 hash of {@code len} bytes of {@code seg} starting at {@code offset};
     * bitwise-identical to {@code Arrays.hashCode(byte[])} of the same bytes ({@code len == 0}
     * returns {@code 1}, matching {@code Arrays.hashCode(new byte[0])}).
     */
    public static int polynomial31(MemorySegment seg, long offset, int len) {
        int h = 1;
        long off = offset;
        final long end = offset + len;
        while (end - off >= 8) {
            long v = seg.get(LE_LONG, off);
            // h advances by 8 positions: h*31^8 + b0*31^7 + b1*31^6 + ... + b7.
            // Bytes stay SIGNED ((byte) casts sign-extend), matching Arrays.hashCode.
            h =
                    h * P31_8
                            + (byte) v * P31_7
                            + (byte) (v >>> 8) * P31_6
                            + (byte) (v >>> 16) * P31_5
                            + (byte) (v >>> 24) * P31_4
                            + (byte) (v >>> 32) * P31_3
                            + (byte) (v >>> 40) * P31_2
                            + (byte) (v >>> 48) * 31
                            + (byte) (v >>> 56);
            off += 8;
        }
        while (off < end) {
            h = 31 * h + seg.get(ValueLayout.JAVA_BYTE, off);
            off++;
        }
        return h;
    }
}
