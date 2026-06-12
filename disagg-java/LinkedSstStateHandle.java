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

package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.util.Optional;

/**
 * FRS-PHASE2 (disaggregated state, design §2.3 / §9 D4): handle for a LINK-mode checkpoint SST —
 * a chk-namespace LOGICAL path resolved through the engine's FileMappingManager. The path carries
 * NO bytes; cross-checkpoint sharing and physical lifetime live entirely in the engine's mapping
 * refcount layer.
 *
 * <p><b>Discard is DELEGATED (paper §5.2: "the JM delegates the deletion to the UFS").</b>
 * {@link #discardState()} is a deliberate no-op on the JM: the authoritative unlink runs TM-side
 * when the backend receives {@code notifyCheckpointSubsumed/Aborted} and calls {@code
 * ForStRsLinker#dbDiscardLinkedCheckpoint} (manifest-driven unlink loop; a physical object is
 * deleted exactly once when its last reference — working dir + other checkpoints — drains). A TM
 * that dies before the notification leaks the chk-k links; the engine's startup sweep reaps
 * abandoned checkpoint namespaces (leak-over-data-loss, design risk R1).
 *
 * <p><b>Never readable.</b> {@link #openInputStream()} throws — nothing on the JM may read SST
 * bytes through this handle, and the logical path has none. LINK mode is gated to {@code
 * SharingFilesStrategy.FORWARD/FORWARD_BACKWARD} (README D-J2), so savepoint/full-checkpoint
 * paths that materialize state never see this type.
 */
public final class LinkedSstStateHandle implements StreamStateHandle {

    private static final long serialVersionUID = 1L;

    /** Chk-namespace logical path: {@code <db_path>/checkpoints/<%020d ckpt>/<NNNNNN.sst>}. */
    private final String linkedPath;

    /** Logical (live-set) size in bytes — reported for state-size accounting only. */
    private final long size;

    public LinkedSstStateHandle(String linkedPath, long size) {
        this.linkedPath = linkedPath;
        this.size = size;
    }

    public String getLinkedPath() {
        return linkedPath;
    }

    @Override
    public void discardState() {
        // D-J1: delegated to the TM-side mapping layer via
        // dbDiscardLinkedCheckpoint(checkpointId) — never a direct delete from
        // the JM (the JM cannot see the refcounts; a direct delete would yank
        // a physical out from under sibling checkpoints / the working dir).
    }

    @Override
    public long getStateSize() {
        return size;
    }

    @Override
    public FSDataInputStream openInputStream() {
        throw new UnsupportedOperationException(
                "LinkedSstStateHandle is metadata-only (linked path "
                        + linkedPath
                        + " has no bytes); resolve through the engine mapping layer");
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        return Optional.empty();
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
        // The linked logical path IS the stable physical identity for registry
        // purposes: unique per (checkpoint, file) — registry-level dedup is
        // structurally a no-op under link mode (design §9 D4); sharing lives in
        // the engine refcounts.
        return new PhysicalStateHandleID(linkedPath);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LinkedSstStateHandle)) {
            return false;
        }
        LinkedSstStateHandle that = (LinkedSstStateHandle) o;
        return size == that.size && linkedPath.equals(that.linkedPath);
    }

    @Override
    public int hashCode() {
        return 31 * linkedPath.hashCode() + Long.hashCode(size);
    }

    @Override
    public String toString() {
        return "LinkedSstStateHandle{" + linkedPath + ", size=" + size + '}';
    }
}
