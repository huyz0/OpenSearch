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
    private final int blockedUnassignedShardCount;
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
        this(nodes, unassignedShardCount, 0, drainCandidates, sustainedPressureTicks, unassignedByIndex);
    }

    /**
     * Creates a signal that separates unassigned shards allocation could place given more capacity
     * from unassigned shards no amount of capacity would help.
     *
     * @param nodes every node in this role's pool, including those holding no shards.
     * @param unassignedShardCount shard copies of this role that allocation wants to place and that
     *                             adding capacity could plausibly satisfy -- the scale-up trigger.
     * @param blockedUnassignedShardCount shard copies that are unassigned for a reason no new node
     *                                    would fix; see {@link #blockedUnassignedShardCount()}.
     * @param drainCandidates node ids that have passed this signal's own sustained-idleness hysteresis.
     * @param sustainedPressureTicks how many consecutive ticks {@code unassignedShardCount} has been nonzero.
     * @param unassignedByIndex per-index breakdown of {@code unassignedShardCount}.
     */
    public RoleCapacitySignal(
        List<NodeCapacityEntry> nodes,
        int unassignedShardCount,
        int blockedUnassignedShardCount,
        List<String> drainCandidates,
        int sustainedPressureTicks,
        Map<String, Integer> unassignedByIndex
    ) {
        this.nodes = nodes;
        this.unassignedShardCount = unassignedShardCount;
        this.blockedUnassignedShardCount = blockedUnassignedShardCount;
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
        this.blockedUnassignedShardCount = in.readVInt();
        this.drainCandidates = in.readStringList();
        this.sustainedPressureTicks = in.readVInt();
        this.unassignedByIndex = in.readMap(StreamInput::readString, StreamInput::readVInt);
    }

    /** @param out stream to write this signal's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(nodes);
        out.writeVInt(unassignedShardCount);
        out.writeVInt(blockedUnassignedShardCount);
        out.writeStringCollection(drainCandidates);
        out.writeVInt(sustainedPressureTicks);
        out.writeMap(unassignedByIndex, StreamOutput::writeString, StreamOutput::writeVInt);
    }

    /**
     * Every node in this role's pool.
     *
     * <p><b>Finding N-6.</b> This used to mean "every node currently holding at least one shard copy
     * of this role", because the accumulator behind it was only ever populated from inside the loop
     * over <em>assigned</em> shard routings. A completely empty node -- the ideal drain target, and
     * the whole of the available headroom on a fresh cluster -- was therefore invisible. Three things
     * broke on that: a control plane could not see idle capacity at all; an empty node could never be
     * a drain candidate, because drain candidates are drawn from these same entries; and the reader
     * scale-up headroom formula {@code nodeCount() * maxShardsPerReaderNode - totalAssignedShardCount()}
     * evaluated to zero on a fresh cluster, so expansion returned without expanding and stayed dead
     * until something else happened to assign a reader shard. Nodes are now seeded from the cluster's
     * membership, so a zero-shard node appears with {@code assignedShardCount == 0}.
     */
    public List<NodeCapacityEntry> nodes() {
        return nodes;
    }

    /** How many nodes are in this role's pool, whether or not they currently hold a shard. */
    public int nodeCount() {
        return nodes.size();
    }

    /**
     * Unassigned shard copies of this role that no amount of new capacity would place.
     *
     * <p><b>Finding N-3.</b> {@link #unassignedShardCount()} is documented as the primary scale-up
     * trigger, and it used to count <em>every</em> unassigned copy -- including ones held unassigned
     * on purpose by this plugin's own deciders (a scale-to-zero-suspended shard is by design pinned
     * {@code UNASSIGNED}) and ones that have exhausted {@code index.allocation.max_retries}. A
     * control plane following the documented contract would provision nodes forever against a shard
     * no node can accept, and {@code sustainedPressureTicks} made it look like escalating demand
     * because a permanently blocked shard produces a monotonically rising counter.
     *
     * <p>Worse, the same value gates reader scale-up: one stuck-warming reader node kept
     * {@code unassignedShardCount() > 0} true forever, which kept {@code readerCapacitySaturated}
     * true forever, which made reader replica expansion return immediately for every index in the
     * cluster. One misconfigured node silently disabled reader scale-up everywhere.
     *
     * <p>These shards are still reported, because they are a real and important condition -- they
     * are simply not a capacity signal, and separating them is what stops them being read as one.
     */
    public int blockedUnassignedShardCount() {
        return blockedUnassignedShardCount;
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
        builder.field("blocked_unassigned_shard_count", blockedUnassignedShardCount);
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
            && blockedUnassignedShardCount == that.blockedUnassignedShardCount
            && sustainedPressureTicks == that.sustainedPressureTicks
            && Objects.equals(nodes, that.nodes)
            && Objects.equals(drainCandidates, that.drainCandidates)
            && Objects.equals(unassignedByIndex, that.unassignedByIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            nodes,
            unassignedShardCount,
            blockedUnassignedShardCount,
            drainCandidates,
            sustainedPressureTicks,
            unassignedByIndex
        );
    }
}
