/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * One node's contribution to a {@code NodeCapacitySignal}, for a single role (writer or reader) --
 * see docs-site/src/content/docs/design/node-autoscaling.md "The signal" for field-level rationale.
 */
public final class NodeCapacityEntry implements Writeable, ToXContentObject {

    private final String nodeId;
    private final String nodeName;
    private final int assignedShardCount;
    private final boolean allShardsIdle;
    private final int idleShardCount;
    private final int hotAffinityShardCount;
    private final boolean draining;

    /**
     * Creates an entry.
     *
     * @param nodeId the node's id.
     * @param nodeName the node's name, for operator readability.
     * @param assignedShardCount how many shard copies of this role are currently assigned to this node.
     * @param allShardsIdle whether every one of those shard copies is individually idle by the same
     *                      per-shard criteria scale-to-zero uses -- deliberately not an average, so a
     *                      node with one hot shard among several idle ones is never flagged.
     * @param idleShardCount how many of {@code assignedShardCount} are individually idle.
     * @param hotAffinityShardCount how many of this node's shards have a fresh {@code
     *                              ReaderCacheAffinityMetadata} record pointing at this node --
     *                              writer entries always report {@code 0}, cache affinity is a
     *                              reader-only concept.
     * @param draining whether this node currently has an active drain (Phase 2) in effect.
     */
    public NodeCapacityEntry(
        String nodeId,
        String nodeName,
        int assignedShardCount,
        boolean allShardsIdle,
        int idleShardCount,
        int hotAffinityShardCount,
        boolean draining
    ) {
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.assignedShardCount = assignedShardCount;
        this.allShardsIdle = allShardsIdle;
        this.idleShardCount = idleShardCount;
        this.hotAffinityShardCount = hotAffinityShardCount;
        this.draining = draining;
    }

    /**
     * Deserializes an entry.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeCapacityEntry}.
     */
    public NodeCapacityEntry(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.nodeName = in.readString();
        this.assignedShardCount = in.readVInt();
        this.allShardsIdle = in.readBoolean();
        this.idleShardCount = in.readVInt();
        this.hotAffinityShardCount = in.readVInt();
        this.draining = in.readBoolean();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeString(nodeName);
        out.writeVInt(assignedShardCount);
        out.writeBoolean(allShardsIdle);
        out.writeVInt(idleShardCount);
        out.writeVInt(hotAffinityShardCount);
        out.writeBoolean(draining);
    }

    /** The node's id. */
    public String nodeId() {
        return nodeId;
    }

    /** The node's name. */
    public String nodeName() {
        return nodeName;
    }

    /** How many shard copies of this role are currently assigned to this node. */
    public int assignedShardCount() {
        return assignedShardCount;
    }

    /** Whether every shard copy assigned to this node is individually idle -- the drain-candidacy signal. */
    public boolean allShardsIdle() {
        return allShardsIdle;
    }

    /** How many of {@link #assignedShardCount()} are individually idle. */
    public int idleShardCount() {
        return idleShardCount;
    }

    /** How many of this node's shards have a fresh cache-affinity record pointing at this node. */
    public int hotAffinityShardCount() {
        return hotAffinityShardCount;
    }

    /** Whether this node currently has an active drain in effect. */
    public boolean draining() {
        return draining;
    }

    /**
     * @param builder the builder to append this entry's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("node_id", nodeId)
            .field("node_name", nodeName)
            .field("assigned_shard_count", assignedShardCount)
            .field("all_shards_idle", allShardsIdle)
            .field("idle_shard_count", idleShardCount)
            .field("hot_affinity_shard_count", hotAffinityShardCount)
            .field("draining", draining)
            .endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeCapacityEntry that = (NodeCapacityEntry) o;
        return assignedShardCount == that.assignedShardCount
            && allShardsIdle == that.allShardsIdle
            && idleShardCount == that.idleShardCount
            && hotAffinityShardCount == that.hotAffinityShardCount
            && draining == that.draining
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(nodeName, that.nodeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeName, assignedShardCount, allShardsIdle, idleShardCount, hotAffinityShardCount, draining);
    }
}
