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
        AbsentIndexRoutingSuppliers.registerUnpublished(ComputedPlacementGate::ownsIndex);
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
        if (ownsIndex(indexMetadata) == false) {
            return null;
        }
        return ComputedRoutingTable.build(indexMetadata, state);
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
