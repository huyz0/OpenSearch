/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * The core-hash-range-split counterpart of {@link ShardPartitionDescriptor}: marks a shard as
 * serving exactly the {@code [start, end]} hash sub-range of some other (pre-split) shard's full
 * document space, per core's {@code org.opensearch.cluster.metadata.SplitShardsMetadata}/{@code
 * ShardRange} model (dynamic-partitioning-plan.md Phase 0). Deliberately a separate record from
 * {@link ShardPartitionDescriptor} rather than an overload of it: that record's {@code
 * (partitionIndex, numPartitions)} shape is specific to this plugin's own, older equal-count split
 * mechanism ({@link ShardSplitter}/{@link org.opensearch.serverless.storage.clone.ShardCloner}, which
 * produces a brand-new index) and cannot represent an arbitrary hash range at all -- see
 * dynamic-partitioning-progress.md's "Task 12" entry for why these two mechanisms are sibling, not
 * identical, and why this record does not reuse the older one's storage or filtering machinery.
 *
 * <p>Write-once, same as {@link ShardPartitionDescriptor}: a shard's own descriptor is set exactly
 * once, during {@code Engine#recoverFromInPlaceSplit}, before this shard's manifest head becomes
 * visible.
 *
 * @param parentShardId the core shard ID this shard was split from.
 * @param start the inclusive lower bound of this shard's hash sub-range.
 * @param end the inclusive upper bound of this shard's hash sub-range.
 */
public record InPlaceSplitRangeDescriptor(int parentShardId, int start, int end) implements Writeable {

    /**
     * Validates the descriptor.
     *
     * @param parentShardId the core shard ID this shard was split from.
     * @param start the inclusive lower bound of this shard's hash sub-range.
     * @param end the inclusive upper bound of this shard's hash sub-range.
     */
    public InPlaceSplitRangeDescriptor {
        if (start > end) {
            throw new IllegalArgumentException("start (" + start + ") must be <= end (" + end + ")");
        }
    }

    /**
     * Deserializes a descriptor.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link InPlaceSplitRangeDescriptor}.
     */
    public InPlaceSplitRangeDescriptor(StreamInput in) throws IOException {
        this(in.readVInt(), in.readInt(), in.readInt());
    }

    /** @param out stream to write this descriptor's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(parentShardId);
        out.writeInt(start);
        out.writeInt(end);
    }

    /** The core shard ID this shard was split from. */
    @Override
    public int parentShardId() {
        return parentShardId;
    }

    /** The inclusive lower bound of this shard's hash sub-range. */
    @Override
    public int start() {
        return start;
    }

    /** The inclusive upper bound of this shard's hash sub-range. */
    @Override
    public int end() {
        return end;
    }
}
