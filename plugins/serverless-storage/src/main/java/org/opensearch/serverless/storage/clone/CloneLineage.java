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

    /**
     * Creates a lineage record.
     *
     * @param sourceIndexUuid the index UUID this shard was cloned from.
     * @param sourceShardId the shard number within that index.
     */
    public CloneLineage(String sourceIndexUuid, int sourceShardId) {
        this.sourceIndexUuid = Objects.requireNonNull(sourceIndexUuid, "sourceIndexUuid");
        this.sourceShardId = sourceShardId;
    }

    /**
     * Deserializes a lineage record.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link CloneLineage}.
     */
    public CloneLineage(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt());
    }

    /** @param out stream to write this lineage's source {@code (indexUuid, shardId)} pair to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceIndexUuid);
        out.writeVInt(sourceShardId);
    }

    /** The index UUID this shard was cloned from. */
    public String sourceIndexUuid() {
        return sourceIndexUuid;
    }

    /** The shard number within {@link #sourceIndexUuid()}. */
    public int sourceShardId() {
        return sourceShardId;
    }

    /** @param o the object to compare against. */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CloneLineage)) return false;
        CloneLineage that = (CloneLineage) o;
        return sourceShardId == that.sourceShardId && sourceIndexUuid.equals(that.sourceIndexUuid);
    }

    /** Consistent with {@link #equals}. */
    @Override
    public int hashCode() {
        return Objects.hash(sourceIndexUuid, sourceShardId);
    }

    /** Diagnostic form only, not a wire format. */
    @Override
    public String toString() {
        return "CloneLineage{sourceIndex='" + sourceIndexUuid + "', sourceShard=" + sourceShardId + '}';
    }
}
