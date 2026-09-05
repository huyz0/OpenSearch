/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.serverless.membership.NodeLease;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Which search nodes should serve a shard.
 *
 * <p><b>This is a cache-affinity hint, and nothing more.</b> Any search node can serve any shard by
 * reading its manifest — that is what makes the object store the only source of truth and what makes
 * scale-to-zero possible. Placement exists so the <em>same</em> node keeps being asked for the same
 * shard, and its local copy stays warm; a wrong answer here costs a cold read, never a wrong result.
 *
 * <p>The moment placement becomes required rather than preferred, this design has reinvented the
 * stateful cluster it exists to escape: agreement on placement, drain-before-move, handoff. So nothing
 * downstream may treat this as authoritative, and the search path falls back to any reachable holder.
 *
 * <p><b>Deliberately not the shard's owner.</b> The owner is the <em>writer</em>, established by
 * compare-and-swap on the shard-head. Routing searches there would couple search capacity to write
 * capacity and make per-index search scale-to-zero meaningless — goals 2 and 3 of
 * {@code rfc-serverless-opensearch.md} §2. Reader activation needs no coordination at all
 * ({@code rfc-serverless-metadata-plane.md} §6), so readers are chosen by hashing, not by ownership.
 *
 * <p><b>Rendezvous hashing rather than modulo.</b> With {@code hash(shard) % n}, adding or losing one
 * node reshuffles almost every shard and throws away every cache. With highest-random-weight, it moves
 * roughly {@code 1/n} of them — which matters because the whole point of placement is that a node keeps
 * being asked for what it already holds.
 */
public final class ReaderPlacement {

    private ReaderPlacement() {}

    /**
     * Returns the search nodes that should serve a shard, best first.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param members the currently live leases
     * @param role the role a node must advertise to be a candidate
     * @param limit how many candidates to return
     * @return candidate node ids, most preferred first; empty if no node advertises the role
     */
    public static List<String> candidatesFor(String indexName, int shardId, Collection<NodeLease> members, String role, int limit) {
        final String key = indexName + "#" + shardId;
        final List<String> ranked = new ArrayList<>();
        for (NodeLease lease : members) {
            if (lease.roles().contains(role)) {
                ranked.add(lease.nodeId());
            }
        }
        // Highest random weight. Ties broken by node id so every node computes the same order from the
        // same membership -- two nodes disagreeing about placement is allowed, but disagreeing while
        // holding identical inputs would just be a bug.
        ranked.sort(
            Comparator.<String>comparingInt(nodeId -> Murmur3HashFunction.hash(nodeId + "/" + key))
                .reversed()
                .thenComparing(nodeId -> nodeId)
        );
        return ranked.size() <= limit ? List.copyOf(ranked) : List.copyOf(ranked.subList(0, limit));
    }
}
