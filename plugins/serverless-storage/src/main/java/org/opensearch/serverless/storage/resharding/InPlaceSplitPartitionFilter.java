/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.routing.Murmur3HashFunction;

/**
 * Decides whether a document id belongs to an in-place split child's own hash range -- the pure
 * computation behind {@link InPlaceSplitFilteringDirectoryReader}.
 *
 * <p>Unlike {@link RoutingPartitionFilter} (deliberately its own self-consistent scheme, since it
 * only needs to agree with itself), this one has to reproduce core's real routing hash exactly:
 * {@link org.opensearch.cluster.routing.OperationRouting#generateShardId} computes
 * {@code Murmur3HashFunction.hash(effectiveRouting)} (the document's {@code _id}, absent an explicit
 * routing value) and resolves it to a child shard via {@code SplitShardsMetadata#getShardIdOfHash},
 * which in turn compares that same hash against each child's {@link ShardRange}. A GET-by-id request
 * is routed by core to exactly one child using that hash; this filter has to agree with that exact
 * same decision, or a document could become reachable by search but not GET (or vice versa), or
 * visible from more than one child at once.
 */
public final class InPlaceSplitPartitionFilter {

    private InPlaceSplitPartitionFilter() {}

    /**
     * Whether a document with the given effective routing value falls within {@code range}.
     *
     * @param effectiveRouting the document's routing value if it was indexed with a custom {@code
     *        _routing}, otherwise its {@code _id} -- the exact {@code effectiveRouting} core's {@link
     *        org.opensearch.cluster.routing.OperationRouting#generateShardId} hashes. (Indices with
     *        {@code index.routing_partition_size > 1}, whose hash also carries a per-document
     *        partition offset, are rejected up front by {@code MetadataInPlaceSplitShardService} and
     *        so never reach this filter.)
     * @param range the split child's own hash range.
     */
    public static boolean matches(String effectiveRouting, ShardRange range) {
        int hash = Murmur3HashFunction.hash(effectiveRouting);
        return range.contains(hash);
    }
}
