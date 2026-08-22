/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

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
 * <p><b>Removal exists, and this paragraph used to say it did not.</b> It said removal "is not
 * implemented; membership therefore grows monotonically for now", which stopped being true when {@link
 * #withoutNodes} landed for scale-to-zero -- see that method's own javadoc for why a permanently departed
 * node has to be removable. A class javadoc that describes a safety property the class no longer has is
 * worse than no javadoc, because it is what a reader checks instead of the code. What is still true is the
 * asymmetry: this type can express a removal, and deciding when one is safe belongs to {@link
 * ComputedPlacementMembershipService}, which is the only place that can tell a restart from a departure.
 */
public final class ComputedPlacementMembership extends AbstractNamedDiffable<Custom> implements Custom {

    /**
     * Published in cluster state metadata, and persisted to the gateway rather than only shared at
     * runtime. Persistence is the requirement rather than a nicety: a membership that had to be
     * rediscovered after a restart would be empty exactly when the shards need it most, which is the
     * failure this class exists to prevent.
     *
     * <p><b>And for a while it was only claimed.</b> {@link #context()} declared {@code API_AND_GATEWAY}
     * and the plugin registered a {@code NamedWriteable} for the wire, but gateway persistence round-trips
     * through XContent and {@code Metadata.Builder.fromXContent} <em>skips</em> a custom it has no parser
     * for -- it logs "Skipping unknown custom object with type" and moves on, because the alternative is a
     * node refusing to start over a plugin it no longer has. So a full-cluster restart came back with no
     * membership at all: {@code ComputedRoutingTable.eligibleNodes} then fell through to the live
     * {@code DiscoveryNodes} during the join window, which is the time-varying node list this whole class
     * exists to stop placement from using, and the version restarted at 1 so nothing could recognise the
     * loss as loss. The parser registration ({@code ServerlessStoragePlugin#getNamedXContent}) and {@link
     * #fromXContent} are the other half of this declaration, not an optional extra.
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

    /**
     * <b>Every field, because this is the persisted form and it used to drop one.</b> {@code
     * previousNodeIds} was written to the wire and omitted here, so even once a parser existed a restart
     * would have come back with the current membership and no previous epoch. That is not a cosmetic loss:
     * {@code WarmCandidates} derives "where was this shard warm before the last change" from it, and an
     * empty previous epoch reads as a real answer meaning "there is no history", so every shard in the
     * cluster would have looked cold at exactly the moment a restart made warmth worth knowing. The
     * version is written for the same reason: a membership that restarts its version at 1 defeats every
     * staleness check that compares one.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(VERSION_FIELD, version);
        builder.startArray(NODE_IDS_FIELD);
        for (String nodeId : nodeIds) {
            builder.value(nodeId);
        }
        builder.endArray();
        builder.startArray(PREVIOUS_NODE_IDS_FIELD);
        for (String nodeId : previousNodeIds) {
            builder.value(nodeId);
        }
        builder.endArray();
        return builder;
    }

    static final String VERSION_FIELD = "version";
    static final String NODE_IDS_FIELD = "node_ids";
    static final String PREVIOUS_NODE_IDS_FIELD = "previous_node_ids";

    /**
     * Reads back what {@link #toXContent} wrote, which is what makes {@code API_AND_GATEWAY} true rather
     * than merely declared.
     *
     * <p>Registered by {@code ServerlessStoragePlugin#getNamedXContent}. The wire format ({@link
     * #writeTo}) is deliberately untouched by this: it is already deployed, and a membership that two
     * versions of a node serialise differently is the split view this class exists to prevent.
     *
     * <p>Unknown fields are skipped rather than rejected, matching how core's own metadata customs parse.
     * A node that meets a field a later version wrote should lose that field, not refuse to start and
     * leave the cluster without a membership at all -- which is the failure mode this whole parser is here
     * to remove.
     */
    public static ComputedPlacementMembership fromXContent(XContentParser parser) throws IOException {
        long version = 0L;
        List<String> nodeIds = new ArrayList<>();
        List<String> previousNodeIds = new ArrayList<>();

        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected a START_OBJECT for [" + TYPE + "] but got " + token);
        }
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                List<String> target = null;
                if (NODE_IDS_FIELD.equals(currentFieldName)) {
                    target = nodeIds;
                } else if (PREVIOUS_NODE_IDS_FIELD.equals(currentFieldName)) {
                    target = previousNodeIds;
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (target != null && token == XContentParser.Token.VALUE_STRING) {
                        target.add(parser.text());
                    }
                }
            } else if (token.isValue()) {
                if (VERSION_FIELD.equals(currentFieldName)) {
                    version = parser.longValue();
                }
            } else {
                parser.skipChildren();
            }
        }
        return of(nodeIds, previousNodeIds, version);
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
