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
 * Marks a shard as one of {@code numPartitions} logical partitions of some other (pre-split)
 * shard's full document space (rfc-serverless-opensearch.md &sect;16 Phase 5's "resharding-by-copy
 * (split/shrink via manifest rewrite + bundle copy -- no reindex; note: until bundles are
 * physically rewritten, readers of a split target apply a doc-routing partition filter, so the
 * transition is logical-first, physical-later)"). {@link ShardSplitter#split} writes exactly one
 * of these into each split target's own container; {@link RoutingPartitionFilter} is what a reader
 * shard consults it for.
 *
 * <p>Deliberately not stored inside {@link org.opensearch.serverless.storage.manifest.CommitManifest}
 * itself: a shard's partition assignment never changes across its own lifetime (a target created by
 * one split is never re-split into yet another descriptor in place -- a further split would create
 * new target shards of its own), so it belongs in a small, separate, write-once record next to
 * {@link org.opensearch.serverless.storage.clone.CloneLineage} rather than perturbing every
 * manifest generation's own schema for a value that's constant across all of them.
 *
 * @param partitionIndex which of {@code numPartitions} partitions this shard serves, in {@code [0, numPartitions)}.
 * @param numPartitions how many partitions the pre-split shard's document space was divided into; must be {@code >= 2}
 *                       (a "split" into exactly one partition is a plain {@link org.opensearch.serverless.storage.clone.ShardCloner clone}, not a split).
 */
public record ShardPartitionDescriptor(int partitionIndex, int numPartitions) implements Writeable {

    /**
     * Validates the descriptor.
     *
     * @param partitionIndex which partition this shard serves.
     * @param numPartitions how many partitions the pre-split shard was divided into.
     */
    public ShardPartitionDescriptor {
        if (numPartitions < 2) {
            throw new IllegalArgumentException("numPartitions must be >= 2, got " + numPartitions);
        }
        if (partitionIndex < 0 || partitionIndex >= numPartitions) {
            throw new IllegalArgumentException("partitionIndex must be in [0, " + numPartitions + "), got " + partitionIndex);
        }
    }

    /**
     * Deserializes a descriptor.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardPartitionDescriptor}.
     */
    public ShardPartitionDescriptor(StreamInput in) throws IOException {
        this(in.readVInt(), in.readVInt());
    }

    /** @param out stream to write this descriptor's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(partitionIndex);
        out.writeVInt(numPartitions);
    }

    /** Which of {@link #numPartitions()} partitions this shard serves. */
    @Override
    public int partitionIndex() {
        return partitionIndex;
    }

    /** How many partitions the pre-split shard's document space was divided into. */
    @Override
    public int numPartitions() {
        return numPartitions;
    }
}
