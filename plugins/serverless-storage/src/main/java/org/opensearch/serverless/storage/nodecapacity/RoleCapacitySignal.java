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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * One role's (writer or reader) autoscaling signal, node autoscaling design doc part 1 ("The
 * signal") -- everything an external control plane needs to decide whether this role's node pool
 * should grow or shrink, without that control plane needing any other view into the cluster.
 */
public final class RoleCapacitySignal implements Writeable, ToXContentObject {

    private final List<NodeCapacityEntry> nodes;
    private final int unassignedShardCount;
    private final List<String> drainCandidates;
    private final int sustainedPressureTicks;
    private final Map<String, Integer> unassignedByIndex;

    /**
     * Creates a signal.
     *
     * @param nodes every node currently holding at least one shard copy of this role.
     * @param unassignedShardCount shard copies of this role allocation wants to place but cannot --
     *                             the primary scale-up trigger.
     * @param drainCandidates node ids that have passed this signal's own sustained-idleness
     *                        hysteresis; a pre-filter, not a command -- the control plane may apply
     *                        its own additional window on top.
     * @param sustainedPressureTicks how many consecutive evaluation ticks {@code
     *                               unassignedShardCount} has been nonzero, letting a poller
     *                               distinguish transient placement hiccups from real pressure.
     * @param unassignedByIndex per-index breakdown of {@code unassignedShardCount} -- node
     *                          autoscaling design doc part 4's "per-index scale-up fairness"
     *                          observability hook; a control plane may use it for per-tenant
     *                          policy, but this plugin never acts on it itself.
     */
    public RoleCapacitySignal(
        List<NodeCapacityEntry> nodes,
        int unassignedShardCount,
        List<String> drainCandidates,
        int sustainedPressureTicks,
        Map<String, Integer> unassignedByIndex
    ) {
        this.nodes = nodes;
        this.unassignedShardCount = unassignedShardCount;
        this.drainCandidates = drainCandidates;
        this.sustainedPressureTicks = sustainedPressureTicks;
        this.unassignedByIndex = unassignedByIndex;
    }

    /**
     * Deserializes a signal.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link RoleCapacitySignal}.
     */
    public RoleCapacitySignal(StreamInput in) throws IOException {
        this.nodes = in.readList(NodeCapacityEntry::new);
        this.unassignedShardCount = in.readVInt();
        this.drainCandidates = in.readStringList();
        this.sustainedPressureTicks = in.readVInt();
        this.unassignedByIndex = in.readMap(StreamInput::readString, StreamInput::readVInt);
    }

    /** @param out stream to write this signal's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(nodes);
        out.writeVInt(unassignedShardCount);
        out.writeStringCollection(drainCandidates);
        out.writeVInt(sustainedPressureTicks);
        out.writeMap(unassignedByIndex, StreamOutput::writeString, StreamOutput::writeVInt);
    }

    /** Every node currently holding at least one shard copy of this role. */
    public List<NodeCapacityEntry> nodes() {
        return nodes;
    }

    /** How many nodes currently hold at least one shard copy of this role. */
    public int nodeCount() {
        return nodes.size();
    }

    /**
     * The sum of {@link NodeCapacityEntry#assignedShardCount()} across every node -- the numerator
     * a headroom estimate (node autoscaling design doc part 4, "per-index scale-up fairness")
     * divides against a configured per-node shard capacity to decide how much expansion budget the
     * current fleet can actually absorb.
     */
    public int totalAssignedShardCount() {
        int total = 0;
        for (NodeCapacityEntry entry : nodes) {
            total += entry.assignedShardCount();
        }
        return total;
    }

    /** Shard copies of this role allocation wants to place but cannot -- the primary scale-up trigger. */
    public int unassignedShardCount() {
        return unassignedShardCount;
    }

    /** Node ids that have passed this signal's own sustained-idleness hysteresis. */
    public List<String> drainCandidates() {
        return drainCandidates;
    }

    /** How many consecutive evaluation ticks {@link #unassignedShardCount()} has been nonzero. */
    public int sustainedPressureTicks() {
        return sustainedPressureTicks;
    }

    /** Per-index breakdown of {@link #unassignedShardCount()}, by index name. */
    public Map<String, Integer> unassignedByIndex() {
        return unassignedByIndex;
    }

    /**
     * @param builder the builder to append this signal's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node_count", nodeCount());
        builder.field("unassigned_shard_count", unassignedShardCount);
        builder.startArray("nodes");
        for (NodeCapacityEntry entry : nodes) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
        builder.field("drain_candidates", drainCandidates);
        builder.field("sustained_pressure_ticks", sustainedPressureTicks);
        builder.startObject("unassigned_by_index");
        for (Map.Entry<String, Integer> entry : new TreeMap<>(unassignedByIndex).entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoleCapacitySignal that = (RoleCapacitySignal) o;
        return unassignedShardCount == that.unassignedShardCount
            && sustainedPressureTicks == that.sustainedPressureTicks
            && Objects.equals(nodes, that.nodes)
            && Objects.equals(drainCandidates, that.drainCandidates)
            && Objects.equals(unassignedByIndex, that.unassignedByIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodes, unassignedShardCount, drainCandidates, sustainedPressureTicks, unassignedByIndex);
    }
}
