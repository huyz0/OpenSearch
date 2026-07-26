/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Computes which nodes should hold a shard, instead of allocating and storing the answer.
 *
 * <p>S6 measured OpenSearch's allocator binding at the order of 100k active shards, with cold allocation
 * superlinear in shard count. The target is 100M indices at up to 100 shards each, so up to 10 billion
 * shards. Five orders of magnitude is not a tuning problem, and the fix is not to make the global pass
 * faster but to stop doing one: placement here is a pure function of the shard and the node list,
 * evaluated on demand for the one shard a request actually needs.
 *
 * <p>S12 measured this exact algorithm. Primary moves on a membership change track {@code
 * added/(N+added)} to two decimal places at every fleet size, distribution stays within 5% of even at
 * 200 nodes, and a lookup costs 80 to 359 ns. That is slower per request than reading a stored routing
 * table, and irrelevant next to a millisecond search; what matters is that the superlinear global pass
 * never happens.
 *
 * <h2>Rendezvous rather than a ring</h2>
 *
 * Highest random weight, not consistent hashing over a token ring. Both give bounded disruption, but a
 * ring needs virtual nodes to distribute evenly and the vnode count is a tuning knob that is wrong by
 * default and hard to change later. Rendezvous has no such knob: the distribution S12 measured is what
 * the algorithm gives, with nothing to configure.
 *
 * <h2>Determinism is a correctness property, not a nicety</h2>
 *
 * Two coordinators that disagree about where a shard lives have a split view of the cluster. Every input
 * to {@link #weight} is therefore fixed: the hash constants are pinned, node identity is its ID string
 * rather than its position in a list, and its test asserts the exact output
 * for known inputs so a well-meaning refactor of the mixing function fails loudly rather than silently
 * repartitioning a live cluster.
 */
public final class RendezvousShardPlacement {

    /**
     * Candidates per shard.
     *
     * <p>Three because it bounds how many caches hold a shard while leaving room to choose among them,
     * and because S12 measured what it buys: a single node joining leaves <em>no</em> shard without a
     * warm candidate, structurally, since one new node can displace at most one of K. Doubling the fleet
     * still leaves 12.4% cold, which is what pre-warm exists for rather than a larger K.
     */
    public static final int DEFAULT_CANDIDATE_COUNT = 3;

    private RendezvousShardPlacement() {}

    /**
     * The nodes that should hold a shard, best first.
     *
     * <p>Returns fewer than {@code candidateCount} when the cluster has fewer nodes, rather than padding
     * or failing: a two-node cluster should still place shards.
     */
    public static List<String> candidates(List<String> nodeIds, String indexUuid, int shardId, int candidateCount) {
        Objects.requireNonNull(nodeIds, "nodeIds");
        Objects.requireNonNull(indexUuid, "indexUuid");
        if (shardId < 0) {
            throw new IllegalArgumentException("shardId must not be negative, was " + shardId);
        }
        if (candidateCount < 1) {
            throw new IllegalArgumentException("candidateCount must be at least 1, was " + candidateCount);
        }
        if (nodeIds.isEmpty()) {
            return List.of();
        }

        int k = Math.min(candidateCount, nodeIds.size());
        long key = shardKey(indexUuid, shardId);

        String[] best = new String[k];
        long[] bestWeight = new long[k];
        Arrays.fill(bestWeight, Long.MIN_VALUE);

        for (String nodeId : nodeIds) {
            long w = weight(key, nodeId);
            // Most nodes lose on the first comparison and exit, which is what keeps this near O(nodes)
            // rather than O(nodes * k).
            if (w <= bestWeight[k - 1] && best[k - 1] != null) {
                continue;
            }
            int position = k - 1;
            while (position > 0 && (best[position - 1] == null || w > bestWeight[position - 1])) {
                bestWeight[position] = bestWeight[position - 1];
                best[position] = best[position - 1];
                position--;
            }
            bestWeight[position] = w;
            best[position] = nodeId;
        }

        List<String> result = new ArrayList<>(k);
        for (String nodeId : best) {
            if (nodeId != null) {
                result.add(nodeId);
            }
        }
        return result;
    }

    /** Convenience for the common case. */
    public static List<String> candidates(List<String> nodeIds, String indexUuid, int shardId) {
        return candidates(nodeIds, indexUuid, shardId, DEFAULT_CANDIDATE_COUNT);
    }

    /**
     * The single node that may write a shard.
     *
     * <p>A hint, not an authority. {@code ShardHead} CAS decides who actually holds the write lease, so a
     * stale view here costs a rejected attempt and a retry rather than two writers. That separation is
     * what lets placement be computed at all: if this had to be authoritative it would need consensus,
     * and consensus over 10 billion shards is the problem being escaped.
     */
    public static String primaryCandidate(List<String> nodeIds, String indexUuid, int shardId) {
        List<String> candidates = candidates(nodeIds, indexUuid, shardId, 1);
        return candidates.isEmpty() ? null : candidates.get(0);
    }

    /**
     * Mixes a shard identity and a node identity into a weight.
     *
     * <p>The 64-bit finalizer from MurmurHash3. Avalanche behaviour is not decoration here: rendezvous
     * distributes evenly only if the weights are independent across nodes, and a weak mix shows up
     * directly as a lopsided cluster.
     *
     * <p>Keyed on the node's ID string rather than its index in a list, so placement does not change when
     * the node list is reordered or when an unrelated node leaves. Ordering-dependent placement would
     * make every membership event a full reshuffle, which is the failure rendezvous exists to avoid.
     */
    static long weight(long shardKey, String nodeId) {
        long h = shardKey * 0x9E3779B97F4A7C15L + hash64(nodeId.getBytes(StandardCharsets.UTF_8));
        h ^= (h >>> 30);
        h *= 0xBF58476D1CE4E5B9L;
        h ^= (h >>> 27);
        h *= 0x94D049BB133111EBL;
        h ^= (h >>> 31);
        return h;
    }

    /** Combines the index UUID and shard id into one key, so both participate in the mix. */
    static long shardKey(String indexUuid, int shardId) {
        return hash64(indexUuid.getBytes(StandardCharsets.UTF_8)) * 31 + shardId;
    }

    /** FNV-1a. Chosen for being short and fixed; the avalanche comes from {@link #weight}'s finalizer. */
    private static long hash64(byte[] bytes) {
        long hash = 0xCBF29CE484222325L;
        for (byte b : bytes) {
            hash ^= (b & 0xFF);
            hash *= 0x100000001B3L;
        }
        return hash;
    }
}
