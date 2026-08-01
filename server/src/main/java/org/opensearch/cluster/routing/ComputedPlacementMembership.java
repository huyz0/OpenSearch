/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * The set of nodes that computed placement is calculated against.
 *
 * <p>C2 called the node set a correctness input rather than a convenience, because two coordinators
 * computing against different node lists produce different placement. C13 showed the same problem
 * arrives in time as well as in space: placement taken from {@code DiscoveryNodes} as-of-now moves a
 * shard the moment a node is briefly absent, the new owner has none of that shard's data, and it
 * recovers empty while looking perfectly healthy.
 *
 * <p>So membership is a value rather than an observation. It is published, versioned, and changes only
 * when something deliberately changes it. A node that stops responding is still a member, which is the
 * whole point: during a restart its shards stay assigned to it, requests fail and retry while it is
 * away, and when it comes back it still has its data and the same node id, because node ids are
 * persisted in the data path and reloaded rather than regenerated.
 *
 * <p><b>Ordering is part of the value.</b> Node ids are held sorted and deduplicated so that two
 * instances built from the same nodes in different orders are equal and produce identical placement.
 * rendezvous hashing is order-independent by construction, but relying on that would make
 * this contract weaker than it needs to be, and a weaker contract here is a split view of the cluster.
 *
 * <p><b>What this deliberately does not do.</b> It never removes a node. Removal is a decommission
 * decision, needs to move data before it takes effect, and is not implemented; membership therefore
 * grows monotonically for now. That is a recorded limitation rather than an oversight: adding removal
 * later is additive, whereas removing a node automatically on absence is the bug this class exists to
 * prevent.
 */
public final class ComputedPlacementMembership extends AbstractNamedDiffable<Custom> implements Custom {

    /**
     * Published in cluster state metadata, and persisted to the gateway rather than only shared at
     * runtime. Persistence is the requirement rather than a nicety: a membership that had to be
     * rediscovered after a restart would be empty exactly when the shards need it most, which is the
     * failure this class exists to prevent.
     */
    public static final String TYPE = "computed_placement_membership";

    /** No members and version zero: what an unconfigured cluster has, and what placement declines on. */
    public static final ComputedPlacementMembership EMPTY = new ComputedPlacementMembership(List.of(), List.of(), 0L);

    private final List<String> nodeIds;

    /**
     * The membership one version ago, retained so warmth can be computed instead of stored.
     *
     * <p>This is the whole reason an epoch is more than a version number. "Which node holds shard X warm"
     * is O(shards) if it is written down, which is the global structure this design exists to delete. It is
     * O(nodes) if it is derived: rendezvous-hash the shard against the previous member list and the answer
     * is where it used to live, available to any coordinator in the nanoseconds a lookup costs.
     *
     * <p>One generation, not N. Two is enough to answer "where was this before the change that just
     * happened", and each extra one is more cluster state for a question nobody asks. A shard that has
     * missed two membership changes has no warm holder worth chasing.
     */
    private final List<String> previousNodeIds;

    private final long version;

    private ComputedPlacementMembership(List<String> sortedNodeIds, List<String> previousNodeIds, long version) {
        this.nodeIds = sortedNodeIds;
        this.previousNodeIds = previousNodeIds;
        this.version = version;
    }

    /** Builds a membership from any collection of node ids, sorting and deduplicating them. */
    public static ComputedPlacementMembership of(Iterable<String> nodeIds, long version) {
        return of(nodeIds, List.of(), version);
    }

    /** Builds a membership that also remembers the list it replaced. */
    public static ComputedPlacementMembership of(Iterable<String> nodeIds, Iterable<String> previousNodeIds, long version) {
        return new ComputedPlacementMembership(sortedCopyOf(nodeIds), sortedCopyOf(previousNodeIds), version);
    }

    private static List<String> sortedCopyOf(Iterable<String> nodeIds) {
        TreeSet<String> sorted = new TreeSet<>();
        for (String nodeId : nodeIds) {
            sorted.add(Objects.requireNonNull(nodeId, "node id must not be null"));
        }
        return List.copyOf(sorted);
    }

    public ComputedPlacementMembership(StreamInput in) throws IOException {
        this.nodeIds = List.copyOf(in.readStringList());
        this.version = in.readVLong();
        this.previousNodeIds = List.copyOf(in.readStringList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(nodeIds);
        out.writeVLong(version);
        // After the version rather than beside the node ids, so the two lists cannot be transposed by a
        // reader that gets the order wrong: a membership silently swapped with its own predecessor would
        // place every shard one epoch in the past and look entirely healthy.
        out.writeStringCollection(previousNodeIds);
    }

    /** The members, sorted, as placement expects them. */
    public List<String> nodeIds() {
        return Collections.unmodifiableList(nodeIds);
    }

    /**
     * The members as of one version ago, or empty if this is the first.
     *
     * <p>Where a shard was warm before the most recent membership change. Empty is a real answer meaning
     * "there is no previous epoch", not a missing one, so a caller finding it should fall back to the
     * current membership rather than treat every shard as cold.
     */
    public List<String> previousNodeIds() {
        return Collections.unmodifiableList(previousNodeIds);
    }

    /** Bumped whenever the membership changes, so a stale view is recognisable as stale. */
    public long version() {
        return version;
    }

    public boolean isEmpty() {
        return nodeIds.isEmpty();
    }

    public boolean contains(String nodeId) {
        return nodeIds.contains(nodeId);
    }

    /**
     * The membership with these nodes added, or {@code this} when they are all already members.
     *
     * <p>Returning the same instance for a no-op is what lets a caller test whether anything changed
     * without comparing lists, and more importantly keeps the version from advancing on every cluster
     * state update. A version that moved when nothing changed would make every node re-derive placement
     * for no reason.
     */
    public ComputedPlacementMembership withNodes(Iterable<String> candidateNodeIds) {
        List<String> merged = new ArrayList<>(nodeIds);
        boolean changed = false;
        for (String nodeId : candidateNodeIds) {
            if (nodeIds.contains(nodeId) == false) {
                merged.add(nodeId);
                changed = true;
            }
        }
        if (changed == false) {
            return this;
        }
        return of(merged, nodeIds, version + 1);
    }

    /**
     * The membership with these nodes removed, or {@code this} when none of them are members.
     *
     * <p>Removal exists now, and the asymmetry that kept it out is still right for the case it was written
     * for. A node absent because it is restarting must keep its membership, because moving its shards to
     * nodes holding none of its data makes them recover empty while looking healthy, which is what C13 hit
     * and it is silent.
     *
     * <p>What changed is that "absent" stopped being one condition. Under autoscaling with scale-to-zero,
     * nodes genuinely leave, and a member list that only grows accumulates ids that will never come back.
     * A growing share of rendezvous weight then lands on nodes that do not exist, and those requests fail
     * and retry forever. So removal is expressible here and the decision about when to use it stays with
     * the caller, which is the only place that can tell a restart from a departure.
     */
    public ComputedPlacementMembership withoutNodes(Iterable<String> departedNodeIds) {
        List<String> remaining = new ArrayList<>(nodeIds);
        boolean changed = false;
        for (String nodeId : departedNodeIds) {
            if (remaining.remove(nodeId)) {
                changed = true;
            }
        }
        if (changed == false) {
            return this;
        }
        return of(remaining, nodeIds, version + 1);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    /**
     * API and gateway, so it is both visible to an operator and written to disk. Without the gateway
     * half a restart would start from an empty membership.
     */
    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("version", version);
        builder.startArray("node_ids");
        for (String nodeId : nodeIds) {
            builder.value(nodeId);
        }
        builder.endArray();
        return builder;
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ComputedPlacementMembership == false) {
            return false;
        }
        ComputedPlacementMembership other = (ComputedPlacementMembership) o;
        // previousNodeIds participates, because two memberships that differ only in what they replaced
        // are genuinely different: warmth is derived from it, so treating them as equal would let a
        // cluster state update carrying a corrected predecessor be dropped as a no-op.
        return version == other.version && nodeIds.equals(other.nodeIds) && previousNodeIds.equals(other.previousNodeIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeIds, previousNodeIds, version);
    }

    @Override
    public String toString() {
        return "ComputedPlacementMembership{version=" + version + ", nodeIds=" + nodeIds + "}";
    }
}
