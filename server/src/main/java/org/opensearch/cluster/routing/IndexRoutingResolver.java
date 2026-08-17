/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collection;
import java.util.List;

/**
 * Phase C of {@code core-pluggability-refactor-plan.md}: the routing-table counterpart to {@link
 * org.opensearch.cluster.metadata.IndexMetadataResolver}. A plugin-supplied fallback for resolving an
 * index's {@link IndexRoutingTable} when it has none published (for example, a shard-placement scheme a
 * plugin computes rather than publishes to cluster state), and for answering whether an index needs a
 * published routing table at all.
 *
 * <p>Consulted from {@link RoutingTable#index(String)}/{@link RoutingTable#allShards()} on a miss and
 * from {@link RoutingNodes#localRoutingNode(ClusterState, String)} for shards a node holds that have no
 * routing-table entry, so the roughly twenty core call sites that previously had to remember to call a
 * static registry (previously {@code AbsentIndexRoutingSuppliers}) directly need no changes to correctly
 * handle an index this resolver answers for.
 *
 * <p>Registered via {@link org.opensearch.plugins.ClusterPlugin#getIndexRoutingResolver()}. Absent by
 * default, so an ordinary cluster resolves exactly as it always has.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexRoutingResolver {

    /**
     * Called only when {@code state.routingTable().index(indexMetadata.getIndex().getName())} already
     * returned nothing.
     *
     * @return the resolved {@link IndexRoutingTable}, or {@code null} if this resolver has no answer
     *         either -- callers must then fall back to their own default (typically: no shards).
     */
    @Nullable
    IndexRoutingTable resolve(ClusterState state, IndexMetadata indexMetadata);

    /**
     * Shards this resolver believes are locally assigned to {@code nodeId} but absent from the published
     * routing table -- the input {@link RoutingNodes#localRoutingNode(ClusterState, String)} needs to
     * splice a "computed placement" shard into a node's own local shard list. Empty by default: most
     * resolvers only need {@link #resolve} (an index-level answer); this is for the narrower case of a
     * shard that is never published at all, index-level routing table or not.
     */
    default Collection<ShardRouting> localShardsFor(ClusterState state, String nodeId) {
        return List.of();
    }

    /**
     * Whether {@code indexMetadata} should have a routing table published for it at all. {@code true} by
     * default (the ordinary case for every ordinary index) -- a resolver only needs to override this to
     * say {@code false} for an index whose shard placement it computes rather than publishes (e.g. a
     * scale-to-zero-style index), which is a distinct question from whether {@link #resolve} can currently
     * answer for the index right now.
     */
    default boolean shouldPublishRouting(IndexMetadata indexMetadata) {
        return true;
    }
}
