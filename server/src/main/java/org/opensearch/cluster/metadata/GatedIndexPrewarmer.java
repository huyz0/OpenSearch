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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.action.ActionListener;

import java.util.Collection;

/**
 * Listens for data node fleet expansion and node topology changes, speculatively pre-warming
 * descriptor caches to prevent cold-read latency spikes under computed placement (RFC Item #4).
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

        logger.info(
            "data node fleet expansion detected; speculatively pre-warming [{}] gated index descriptors",
            activeGatedIndices.size()
        );

        DescriptorPrefetch.prefetch(
            activeGatedIndices,
            ActionListener.wrap(
                r -> logger.debug("speculative descriptor pre-warming completed"),
                e -> logger.warn("speculative descriptor pre-warming failed", e)
            )
        );
    }
}
