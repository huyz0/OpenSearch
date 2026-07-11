/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.cluster.routing.Murmur3HashFunction;

/**
 * Decides which of a {@link ShardPartitionDescriptor}'s partitions one document id belongs to --
 * the pure computation behind {@link PartitionFilteringDirectoryReader}.
 *
 * <p>Deliberately its own self-consistent partitioning of the document-id space, not a replication
 * of core's real shard-count consistent-hashing scheme ({@code IndexRouting}/{@code
 * OperationRouting}, used to pick a live shard for a routed request): those two only need to agree
 * with <em>each other</em>, never with core's, since a split target here is a plugin-level shard
 * identity, not one derived from {@code index.number_of_shards}/{@code index.number_of_routing_shards}
 * math. What matters for correctness is only that this hash is (a) a deterministic function of the
 * document id alone, so the same document always lands in the same partition across every reader
 * that evaluates it, and (b) partitions the id space completely and disjointly for a fixed {@code
 * numPartitions}, so every live document is visible from exactly one split target. {@link
 * Murmur3HashFunction} is reused rather than a new hash purely because it's already OpenSearch's
 * own default routing hash and therefore already a well-distributed, well-tested choice -- not
 * because bit-compatibility with core's routing decision matters here.
 */
public final class RoutingPartitionFilter {

    private RoutingPartitionFilter() {}

    /**
     * Whether a document with the given id belongs to {@code descriptor}'s partition.
     *
     * @param id the document's {@code _id}.
     * @param descriptor which partition (of how many) this reader is filtering to.
     */
    public static boolean matches(String id, ShardPartitionDescriptor descriptor) {
        int hash = Murmur3HashFunction.hash(id);
        int partition = Math.floorMod(hash, descriptor.numPartitions());
        return partition == descriptor.partitionIndex();
    }
}
