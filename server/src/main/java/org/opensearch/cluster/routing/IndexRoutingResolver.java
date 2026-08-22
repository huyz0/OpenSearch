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

/**
 * The routing-table counterpart to {@link
 * org.opensearch.cluster.metadata.IndexMetadataResolver}. A plugin-supplied fallback for resolving an
 * index's {@link IndexRoutingTable} when it has none published (for example, a shard-placement scheme a
 * plugin computes rather than publishes to cluster state), and for answering whether an index needs a
 * published routing table at all.
 *
 * <p><b>Wiring status -- kept accurate here rather than aspirational, since this fell out of sync with the
 * design once before.</b> {@link #resolve} is consulted by core from {@link
 * org.opensearch.cluster.ClusterState#getIndexRoutingTable(String)} (a design correction made over
 * this javadoc's original draft -- see that method's own javadoc for why a bare {@link RoutingTable} cannot
 * be the consultation point itself), and {@link #shouldPublishRouting} from {@link
 * RoutingTable#shouldPublishRouting} (its call-site migration). A third method, {@code
 * localShardsFor}, was declared for a "computed shards spliced into a node's local shard list" design that
 * production never used -- data nodes materialize computed shards through the on-demand opener in {@code
 * IndicesClusterStateService} instead -- and was removed along with its {@code
 * ClusterState#getLocallyComputedShards} consultation point.
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
     * returned nothing, and never from a thread where blocking would be unsafe -- see {@link
     * org.opensearch.cluster.ClusterStateMutationThreads}'s own javadoc, which {@code
     * ClusterState#getIndexRoutingTable(String)} (the sole caller of this method) checks before reaching
     * here, for the deadlock this guarantee exists to prevent. A {@code resolve} implementation therefore
     * does not need its own thread guard, but must still answer quickly for every other caller.
     *
     * @return the resolved {@link IndexRoutingTable}, or {@code null} if this resolver has no answer
     *         either -- callers must then fall back to their own default (typically: no shards).
     */
    @Nullable
    IndexRoutingTable resolve(ClusterState state, IndexMetadata indexMetadata);

    /**
     * Whether {@code indexMetadata} should have a routing table published for it at all. {@code true} by
     * default (the ordinary case for every ordinary index) -- a resolver only needs to override this to
     * say {@code false} for an index whose shard placement it computes rather than publishes (e.g. a
     * scale-to-zero-style index), which is a distinct question from whether {@link #resolve} can currently
     * answer for the index right now. <b>Not yet consulted by any core call site</b> -- see this interface's
     * own "wiring status" note above.
     */
    default boolean shouldPublishRouting(IndexMetadata indexMetadata) {
        return true;
    }
}
