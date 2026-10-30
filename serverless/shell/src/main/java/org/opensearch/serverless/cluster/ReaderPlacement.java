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
import java.util.PriorityQueue;

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
        if (limit < 0) {
            throw new IllegalArgumentException("candidate limit must be non-negative");
        }
        if (limit == 0) {
            return List.of();
        }
        final String key = indexName + "#" + shardId;
        final Comparator<Candidate> preferred = Comparator.comparingInt(Candidate::weight).reversed().thenComparing(Candidate::nodeId);
        // The worst retained candidate is first. The common two-reader route retains only two entries,
        // hashes each eligible member once, and never sorts the whole fleet for each searched shard.
        final PriorityQueue<Candidate> best = new PriorityQueue<>(Math.min(limit, 16), preferred.reversed());
        for (NodeLease lease : members) {
            if (lease.roles().contains(role)) {
                final Candidate candidate = new Candidate(lease.nodeId(), Murmur3HashFunction.hash(lease.nodeId() + "/" + key));
                if (best.size() < limit) {
                    best.add(candidate);
                } else if (preferred.compare(candidate, best.peek()) < 0) {
                    best.poll();
                    best.add(candidate);
                }
            }
        }
        final List<Candidate> ranked = new ArrayList<>(best);
        ranked.sort(preferred);
        return ranked.stream().map(Candidate::nodeId).toList();
    }

    private record Candidate(String nodeId, int weight) {
    }
}
