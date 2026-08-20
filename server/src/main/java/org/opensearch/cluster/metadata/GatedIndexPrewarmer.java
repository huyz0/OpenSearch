/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.CoordinatorAffinityRouting;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Listens for data node fleet expansion and node topology changes, speculatively pre-warming
 * descriptor caches to prevent cold-read latency spikes under computed placement (RFC Item #4).
 *
 * <h2>Why this only prefetches its own affinity share, not the whole set</h2>
 *
 * This applier runs once per node -- {@link org.opensearch.indices.cluster.IndicesClusterStateService}
 * constructs one instance per node, and {@link ClusterStateApplier#applyClusterState} fires on every
 * node for the same cluster-state transition. Prefetching the full {@code activeGatedIndices} set
 * unfiltered, as this class did before {@link CoordinatorAffinityRouting} had a caller, means N nodes
 * each independently re-warm all M indices: N times the store reads a single node join should cost,
 * and every node's cache converges on holding the same full population rather than each holding its
 * own roughly M/N share -- destroying the affinity-driven cache locality {@link CoordinatorAffinityRouting}
 * exists to create ({@code getAffinityNode} is deterministic, so every node computing it against the
 * same cluster state agrees on which one owns a given name).
 *
 * <p>Filtering to "am I the affinity node for this name, under the state driving this event" bounds
 * the work: only the node(s) whose affinity assignment actually changed by this transition do
 * anything, which for rendezvous hashing is close to exactly the newly-joined node's own share --
 * rendezvous hashing's own minimal-disruption property means adding a node only reassigns names to
 * it, never moves a name between two nodes that were both already present.
 */
public class GatedIndexPrewarmer implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(GatedIndexPrewarmer.class);

    private final java.util.function.Supplier<Collection<String>> gatedIndexSupplier;

    public GatedIndexPrewarmer(java.util.function.Supplier<Collection<String>> gatedIndexSupplier) {
        this.gatedIndexSupplier = gatedIndexSupplier;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (event.nodesChanged() == false || DescriptorPrefetch.isRegistered() == false) {
            return;
        }

        DiscoveryNodes.Delta delta = event.nodesDelta();
        if (delta.added() == false) {
            return;
        }

        boolean hasNewDataNode = delta.addedNodes().stream().anyMatch(node -> node.isDataNode());
        if (hasNewDataNode == false) {
            return;
        }

        Collection<String> activeGatedIndices = gatedIndexSupplier.get();
        if (activeGatedIndices == null || activeGatedIndices.isEmpty()) {
            return;
        }

        DiscoveryNodes nodes = event.state().nodes();
        String localNodeId = nodes.getLocalNodeId();
        List<String> ownShare = new ArrayList<>();
        for (String indexName : activeGatedIndices) {
            DiscoveryNode affinityNode = CoordinatorAffinityRouting.getAffinityNode(indexName, nodes);
            if (affinityNode != null && localNodeId != null && localNodeId.equals(affinityNode.getId())) {
                ownShare.add(indexName);
            }
        }

        if (ownShare.isEmpty()) {
            return;
        }

        logger.info(
            "data node fleet expansion detected; speculatively pre-warming [{}] of [{}] gated index descriptors "
                + "this node has affinity for",
            ownShare.size(),
            activeGatedIndices.size()
        );

        DescriptorPrefetch.prefetch(
            ownShare,
            ActionListener.wrap(
                r -> logger.debug("speculative descriptor pre-warming completed"),
                e -> logger.warn("speculative descriptor pre-warming failed", e)
            )
        );
    }
}
