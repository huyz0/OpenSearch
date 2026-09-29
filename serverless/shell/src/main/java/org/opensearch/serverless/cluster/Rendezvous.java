/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.cluster.routing.Murmur3HashFunction;

import java.util.Collection;

/**
 * Picks one node for a key by highest random weight, so work can be split without anything to agree on.
 *
 * <p>For background jobs that should run once per key rather than once per node. Two nodes whose views of
 * membership differ may both pick themselves, and one whose view is empty picks nobody but itself; a job
 * split this way must therefore be safe to run twice and cheap to run late. What it buys is that in the
 * common case, with every node seeing the same members, each key is handled by exactly one of them.
 */
public final class Rendezvous {

    private Rendezvous() {}

    /**
     * Returns the node that should handle a key.
     *
     * <p>Ties are broken by node id, so every node computes the same answer from the same membership.
     *
     * @param key the unit of work
     * @param nodeIds the candidates; must not be empty
     * @return the chosen node id
     */
    public static String owner(String key, Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) {
            throw new IllegalArgumentException("no candidates for [" + key + "]");
        }
        String best = null;
        int bestWeight = 0;
        for (String nodeId : nodeIds) {
            final int weight = Murmur3HashFunction.hash(nodeId + "/" + key);
            if (best == null || weight > bestWeight || (weight == bestWeight && nodeId.compareTo(best) < 0)) {
                best = nodeId;
                bestWeight = weight;
            }
        }
        return best;
    }
}
