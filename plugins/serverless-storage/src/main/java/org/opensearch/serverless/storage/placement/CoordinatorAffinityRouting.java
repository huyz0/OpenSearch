/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility for computing a stable node affinity for index names.
 *
 * <p>Uses rendezvous (highest-random-weight) hashing over node IDs: every node in the cluster
 * independently computes the same preferred node for a given index name, with no coordination or
 * shared state, and the mapping is minimally disturbed when nodes join or leave -- only the index
 * names whose preferred node departed move. Callers use this to route repeated work for the same
 * index name to the same node, e.g. so that any per-index state cached on that node is reused
 * rather than rebuilt elsewhere.
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
        // Not sorted, which it used to be on every call.
        //
        // Rendezvous hashing is order-independent by construction -- the winner is the maximum of a set of
        // per-node weights, and a maximum does not care what order it is taken in. The sort was there to
        // make a tie deterministic, and that job now belongs to getAffinityNodeId's own tie-break, which
        // resolves ties to the lexicographically smallest node id: exactly what the sort produced, without
        // the sort. The answer for any input is unchanged.
        //
        // It is worth removing rather than leaving because of who calls this. GatedIndexPrewarmer runs
        // this once per active gated index on every node on every data-node join, so the sort was
        // O(indices x nodes log nodes) of allocation and comparison on a membership change, to compute
        // something that never needed the ordering.
        String bestNodeId = getAffinityNodeId(indexName, nodeIds);
        return bestNodeId != null ? nodes.get(bestNodeId) : null;
    }

    /**
     * Computes the preferred node ID for an index name among a list of candidate node IDs.
     *
     * <p>The list no longer has to be sorted, and the parameter name kept saying it did. Rendezvous
     * hashing takes a maximum over per-node weights, which is order-independent; the only thing ordering
     * ever settled was a tie, and the tie-break below settles it explicitly instead -- to the
     * lexicographically smallest node id, which is precisely what a sorted list produced. Callers that
     * still pass a sorted list get the same answer they always did.
     *
     * @param indexName the name of the index
     * @param nodeIds candidate node IDs, in any order
     * @return the node ID with highest rendezvous weight, or null if empty
     */
    public static String getAffinityNodeId(String indexName, List<String> nodeIds) {
        if (indexName == null || nodeIds == null || nodeIds.isEmpty()) {
            return null;
        }

        byte[] indexNameBytes = indexName.getBytes(StandardCharsets.UTF_8);
        long indexHash = MurmurHash3.hash128(indexNameBytes, 0, indexNameBytes.length, 0, new MurmurHash3.Hash128()).h1;

        String bestNodeId = null;
        long maxWeight = Long.MIN_VALUE;

        for (String nodeId : nodeIds) {
            byte[] nodeIdBytes = nodeId.getBytes(StandardCharsets.UTF_8);
            long weight = MurmurHash3.hash128(nodeIdBytes, 0, nodeIdBytes.length, indexHash, new MurmurHash3.Hash128()).h1;
            // The tie-break is the sort's only remaining job, done here where it costs one comparison on
            // the vanishingly rare equal weight rather than an O(n log n) sort on every call. Smallest id
            // wins, which is the node a sorted list would have reached first.
            if (weight > maxWeight || (weight == maxWeight && bestNodeId != null && nodeId.compareTo(bestNodeId) < 0)) {
                maxWeight = weight;
                bestNodeId = nodeId;
            }
        }

        return bestNodeId;
    }
}
