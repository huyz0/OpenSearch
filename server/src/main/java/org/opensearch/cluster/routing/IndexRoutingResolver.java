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
 * <p><b>Wiring status -- kept accurate here rather than aspirational, since this fell out of sync with the
 * design once before.</b> Only {@link #resolve} is actually consulted by core today, from {@link
 * org.opensearch.cluster.ClusterState#getIndexRoutingTable(String)} (a design correction Phase C3 made over
 * this javadoc's original draft -- see that method's own javadoc for why a bare {@link RoutingTable} cannot
 * be the consultation point itself). {@link #localShardsFor} and {@link #shouldPublishRouting} are declared
 * here as part of the interface's intended shape, but are <b>not yet consulted by any core call site</b> --
 * every current caller of the shape {@code shouldPublishRouting} answers (index creation, split/merge,
 * scale-to-zero validation, snapshotting, gateway state recovery) still calls the pre-existing static
 * registry, {@code AbsentIndexRoutingSuppliers#shouldPublishRouting}, directly. Migrating those call sites is
 * Phase C4's job, not this interface's; declaring the methods first keeps the eventual migration a pure
 * call-site swap instead of also being an interface change.
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
     * Shards this resolver believes are locally assigned to {@code nodeId} but absent from the published
     * routing table -- intended for {@link RoutingNodes#localRoutingNode(ClusterState, String)} to splice a
     * "computed placement" shard into a node's own local shard list. Empty by default: most resolvers only
     * need {@link #resolve} (an index-level answer); this is for the narrower case of a shard that is never
     * published at all, index-level routing table or not. <b>Not yet consulted by {@code RoutingNodes}</b>
     * -- see this interface's own "wiring status" note above.
     */
    default Collection<ShardRouting> localShardsFor(ClusterState state, String nodeId) {
        return List.of();
    }

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
