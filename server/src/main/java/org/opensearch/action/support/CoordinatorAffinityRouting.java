/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility for computing coordinator node affinity for index names.
 *
 * <p>Uses consistent rendezvous hashing over node IDs to map an index name to a primary
 * candidate coordinator node, maximizing coordinator LRU descriptor cache hit ratios (Area B).
 */
public final class CoordinatorAffinityRouting {

    private CoordinatorAffinityRouting() {}

    /**
     * Finds the preferred coordinator node for an index name based on rendezvous weight.
     *
     * @param indexName the name of the index
     * @param clusterState current cluster state
     * @return the preferred DiscoveryNode, or null if no eligible nodes exist
     */
    public static DiscoveryNode getAffinityNode(String indexName, ClusterState clusterState) {
        if (indexName == null || clusterState == null) {
            return null;
        }
        return getAffinityNode(indexName, clusterState.nodes());
    }

    /**
     * Finds the preferred coordinator node for an index name based on rendezvous weight.
     *
     * @param indexName the name of the index
     * @param nodes discovery nodes
     * @return the preferred DiscoveryNode, or null if no eligible nodes exist
     */
    public static DiscoveryNode getAffinityNode(String indexName, DiscoveryNodes nodes) {
        if (indexName == null || nodes == null || nodes.getSize() == 0) {
            return null;
        }

        List<String> nodeIds = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            nodeIds.add(node.getId());
        }
        Collections.sort(nodeIds);

        String bestNodeId = getAffinityNodeId(indexName, nodeIds);
        return bestNodeId != null ? nodes.get(bestNodeId) : null;
    }

    /**
     * Computes the preferred node ID for an index name among a list of sorted node IDs.
     *
     * @param indexName the name of the index
     * @param sortedNodeIds sorted list of candidate node IDs
     * @return the node ID with highest rendezvous weight, or null if empty
     */
    public static String getAffinityNodeId(String indexName, List<String> sortedNodeIds) {
        if (indexName == null || sortedNodeIds == null || sortedNodeIds.isEmpty()) {
            return null;
        }

        byte[] indexNameBytes = indexName.getBytes(StandardCharsets.UTF_8);
        long indexHash = MurmurHash3.hash128(indexNameBytes, 0, indexNameBytes.length, 0, new MurmurHash3.Hash128()).h1;

        String bestNodeId = null;
        long maxWeight = Long.MIN_VALUE;

        for (String nodeId : sortedNodeIds) {
            byte[] nodeIdBytes = nodeId.getBytes(StandardCharsets.UTF_8);
            long weight = MurmurHash3.hash128(nodeIdBytes, 0, nodeIdBytes.length, indexHash, new MurmurHash3.Hash128()).h1;
            if (weight > maxWeight) {
                maxWeight = weight;
                bestNodeId = nodeId;
            }
        }

        return bestNodeId;
    }
}
