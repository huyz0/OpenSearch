/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.IndexRoutingTable;

/**
 * C5. Decides which indices get computed placement, and installs the supplier.
 *
 * <p>Two conditions, deliberately separate. The node-level setting says whether this node uses computed
 * placement at all; the per-index check says whether a given index is one this plugin owns. Collapsing
 * them would mean enabling the feature reroutes every index in the cluster, including ones with local
 * disk and peer recovery, which is not a mistake that announces itself.
 *
 * <p>The gate is smaller than the plan expected because of how C3 landed. When the seam was going to be
 * a lazy routing table, the gate had to keep serverless entries out of the allocator's path while
 * leaving everything else in it. Since a serverless index instead publishes no routing entry at all,
 * "gated off" is simply "no supplier installed", and the fallback is the behaviour Phase A already
 * shipped.
 */
public final class ComputedPlacementGate {

    private ComputedPlacementGate() {}

    /**
     * Installs the supplier if computed placement is enabled on this node.
     *
     * <p>Idempotent by construction: the registry holds one reference and last registration wins, so a
     * second call replaces rather than accumulates.
     */
    public static void install(boolean enabled) {
        if (enabled == false) {
            return;
        }
        AbsentIndexRoutingSuppliers.register(ComputedPlacementGate::supply);
        // Without this the supplier is installed and never invoked. Index creation publishes a routing
        // entry for every index, and a supplier only runs when there is none, so the mechanism looks
        // wired and is dead. Registering both together is what makes it reachable.
        AbsentIndexRoutingSuppliers.registerUnpublished(ComputedPlacementGate::placementIsComputed);
        INSTALLED_NODES.incrementAndGet();
    }

    /**
     * How many nodes in this JVM currently hold placement installed.
     *
     * <p>The registries are static and a JVM hosts one node in production, so this counts to one there. It
     * exists for {@code InternalTestCluster}, which runs several: without it the first node to close
     * unregistered computed placement for every node still running, and a gated index whose placement has
     * gone has no routing table and cannot be reached at all.
     */
    private static final java.util.concurrent.atomic.AtomicInteger INSTALLED_NODES = new java.util.concurrent.atomic.AtomicInteger();

    /**
     * Releases one node's claim, clearing the suppliers only once the last node has gone.
     *
     * <p>What a node's {@code close()} calls, as against {@link #uninstall}, which resets unconditionally
     * and is what a test wants between methods.
     */
    public static void uninstallOneNode() {
        if (INSTALLED_NODES.get() <= 0 || INSTALLED_NODES.decrementAndGet() > 0) {
            return;
        }
        uninstall();
    }

    /** Removes the supplier, restoring Phase A's behaviour. Used by tests, and by the last node to close. */
    public static void uninstall() {
        INSTALLED_NODES.set(0);
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
    }

    /**
     * Computes the entry for an index this plugin owns, or declines.
     *
     * <p>Declining rather than throwing is what lets one cluster hold both kinds of index: a non-
     * serverless index with no routing entry is a genuine problem and should keep reporting no shard
     * available, exactly as it did before this hook existed.
     */
    static IndexRoutingTable supply(ClusterState state, IndexMetadata indexMetadata) {
        if (placementIsComputed(indexMetadata) == false) {
            return null;
        }
        return ComputedRoutingTable.build(indexMetadata, state);
    }

    /**
     * Whether this index's placement is computed rather than published, which is the same question as
     * whether it is gated.
     *
     * <p><b>The two must agree, and this is where that is enforced.</b> An index is gated exactly when its
     * placement is computed: a gated index has no cluster state entry and therefore can have no published
     * routing table, and an index whose routing is unpublished has nothing else to place it. Deciding them
     * independently permits the two unserviceable shapes this file's header names -- published routing with
     * no metadata, and metadata with no way to place it.
     *
     * <p>The second of those was reachable until the {@code serverless_} namespace was finished, and it
     * failed in the worst available way. An index carrying the storage setting but declined by the gate --
     * one with an alias, say, or a data stream backing index -- got a cluster state entry *and* unpublished
     * routing, so the ordinary allocator never assigned its shards and the on-demand opening path, which
     * only runs for gated indices, never opened them either. A write to it did not fail; it retried until
     * something above it timed out, because nothing tells a write that its shard is never coming.
     *
     * <p>So this reads the name, exactly as {@code DescriptorGate#gatable} does. An index outside the
     * namespace publishes routing and is allocated the ordinary way, which is what serverless storage did
     * before computed placement existed and is what an alias-bearing or data-stream index needs.
     */
    public static boolean placementIsComputed(IndexMetadata indexMetadata) {
        return indexMetadata != null
            && org.opensearch.cluster.metadata.DescriptorOnlyCreation.namesAServerlessIndex(indexMetadata.getIndex().getName())
            && ownsIndex(indexMetadata);
    }

    /**
     * Whether this plugin owns an index, read from the index's own settings.
     *
     * <p>Read from settings rather than inferred from the engine, because this is called during routing
     * resolution on nodes that may hold no shard of the index and therefore have no engine to ask.
     */
    public static boolean ownsIndex(IndexMetadata indexMetadata) {
        return indexMetadata != null && indexMetadata.getSettings().getAsBoolean("index.serverless_storage.enabled", false);
    }
}
