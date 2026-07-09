/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Which shard a cloned shard was cloned from -- recorded once, in the <em>target</em> shard's own
 * container, at clone time, so a later "delete this clone" operation can find its way back to the
 * source shard's {@link org.opensearch.serverless.storage.retention.DurablePinRegistry} and remove
 * {@link ShardCloner#clonePinId}'s pin, without needing any other index to remember the
 * relationship. {@link ShardCloner#clone} is the only writer; nothing else mutates a shard's
 * lineage once cloned.
 */
public final class CloneLineage implements Writeable {

    private final String sourceIndexUuid;
    private final int sourceShardId;

    public CloneLineage(String sourceIndexUuid, int sourceShardId) {
        this.sourceIndexUuid = Objects.requireNonNull(sourceIndexUuid, "sourceIndexUuid");
        this.sourceShardId = sourceShardId;
    }

    public CloneLineage(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceIndexUuid);
        out.writeVInt(sourceShardId);
    }

    public String sourceIndexUuid() {
        return sourceIndexUuid;
    }

    public int sourceShardId() {
        return sourceShardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CloneLineage)) return false;
        CloneLineage that = (CloneLineage) o;
        return sourceShardId == that.sourceShardId && sourceIndexUuid.equals(that.sourceIndexUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceIndexUuid, sourceShardId);
    }

    @Override
    public String toString() {
        return "CloneLineage{sourceIndex='" + sourceIndexUuid + "', sourceShard=" + sourceShardId + '}';
    }
}
