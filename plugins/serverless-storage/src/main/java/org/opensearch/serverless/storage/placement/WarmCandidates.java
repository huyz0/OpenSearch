/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.cluster.routing.ComputedPlacementMembership;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Which nodes are likely to still hold a shard's blocks in cache, computed rather than recorded.
 *
 * <h2>Why this is not a stored map</h2>
 *
 * The obvious implementation writes down "shard X was last served by node Y". `ReaderCacheAffinityMetadata`
 * does exactly that, in `IndexMetadata` custom data, and it has two problems. It is O(shards) of state,
 * which is the global structure this design exists to delete, and both of its consumers read it from
 * cluster state, which a serverless index has no entry in. So it is unreachable for precisely the indices
 * it was built for.
 *
 * <p>Placement is already a pure function of the shard and the member list. So warmth is a pure function
 * of the shard and the <em>previous</em> member list: wherever a shard used to be placed is where its cache
 * is, because deterministic placement is what put it there. Retaining one prior epoch, which T16 added, is
 * O(nodes) for the whole cluster instead of O(shards), and any coordinator can evaluate it in the
 * nanoseconds a rendezvous lookup costs.
 *
 * <h2>What this is and is not for</h2>
 *
 * A hint for choosing among replicas and for deciding what to pre-warm. Never an authority: a node that
 * held a shard last epoch may have evicted it, restarted, or never actually served a query for it. Being
 * wrong costs one cold read, which is the same thing being right costs when the cache has aged out.
 *
 * <p>Correctness never depends on it, because the write path is fenced by {@code ShardHead} CAS and the
 * read path can serve from any placement candidate. That is what lets this be approximate at all.
 */
public final class WarmCandidates {

    private WarmCandidates() {}

    /**
     * Nodes that likely hold this shard warm, best first, drawn from the epoch before the current one.
     *
     * <p>Returns empty when there is no previous epoch, which is a real answer rather than a missing one:
     * a cluster that has never changed membership has its shards exactly where placement says, so the
     * caller should use the current candidates rather than treat everything as cold.
     *
     * <p>Nodes no longer in the membership are dropped. A departed node's cache is not reachable, and
     * offering it as a candidate would send traffic somewhere that cannot answer, which is the failure a
     * stale affinity record produces and the reason the stored version needed a freshness check.
     */
    public static List<String> forShard(ComputedPlacementMembership membership, String indexUuid, int shardId, int candidateCount) {
        List<String> previous = membership.previousNodeIds();
        if (previous.isEmpty()) {
            return List.of();
        }
        List<String> warm = new ArrayList<>();
        for (String nodeId : RendezvousShardPlacement.candidates(previous, indexUuid, shardId, candidateCount)) {
            if (membership.contains(nodeId)) {
                warm.add(nodeId);
            }
        }
        return warm;
    }

    /**
     * Placement candidates ordered so the ones that are also warm come first.
     *
     * <p>The form a router actually wants. Every node that may hold the shard is present, so this never
     * narrows the choice, and the ones that can answer without a cold read are preferred. Ordering rather
     * than filtering is deliberate: a warm-only list would empty out exactly when the fleet has just
     * doubled, which S12 measured as the case where 12.4% of shards have no warm candidate at all, and
     * that is the moment a router most needs somewhere to send a request.
     */
    public static List<String> preferWarm(ComputedPlacementMembership membership, String indexUuid, int shardId, int candidateCount) {
        List<String> placement = RendezvousShardPlacement.candidates(membership.nodeIds(), indexUuid, shardId, candidateCount);
        List<String> warm = forShard(membership, indexUuid, shardId, candidateCount);
        if (warm.isEmpty()) {
            return placement;
        }
        // LinkedHashSet so a node that is both warm and a current candidate appears once, at its warm
        // position. Duplicates would skew any weighted choice made over this list.
        LinkedHashSet<String> ordered = new LinkedHashSet<>();
        for (String nodeId : warm) {
            if (placement.contains(nodeId)) {
                ordered.add(nodeId);
            }
        }
        ordered.addAll(placement);
        return List.copyOf(ordered);
    }
}
