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
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * The routing counterpart to {@link org.opensearch.cluster.metadata.SupplierBackedIndexMetadataResolver}:
 * a thin, generic adapter over the static registry ({@link AbsentIndexRoutingSuppliers}) rather than
 * plugin-owned logic -- see that class's own javadoc for why the adapter lives in {@code server/}.
 *
 * <p><b>Settled architecture: one authority per plane.</b> {@link AbsentIndexRoutingSuppliers} is the
 * node-level <em>authority</em> for computed routing; this adapter is the registration <em>front door</em>
 * a plugin returns from {@code ClusterPlugin#getIndexRoutingResolver()} to feed the state-scoped read path
 * ({@code ClusterState#getIndexRoutingTable}/{@code #resolveShard}/{@code #allShards}, {@code
 * RoutingTable#shouldPublishRouting}) from that authority. It holds no state of its own: every method
 * forwards to the registry, so the answers a state-scoped read gets and the answers the registry's few
 * deliberate direct callers get are the same answers by construction.
 *
 * <p>Implements both {@link IndexRoutingResolver} methods, not just {@link #resolve}, since {@link
 * AbsentIndexRoutingSuppliers} already has the equivalent of both ({@link
 * AbsentIndexRoutingSuppliers#supply(ClusterState, IndexMetadata)} and {@link
 * AbsentIndexRoutingSuppliers#shouldPublishRouting}) and forwarding both costs nothing extra here.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SupplierBackedIndexRoutingResolver implements IndexRoutingResolver {

    @Override
    public IndexRoutingTable resolve(ClusterState state, IndexMetadata indexMetadata) {
        return AbsentIndexRoutingSuppliers.supply(state, indexMetadata);
    }

    @Override
    public boolean shouldPublishRouting(IndexMetadata indexMetadata) {
        return AbsentIndexRoutingSuppliers.shouldPublishRouting(indexMetadata);
    }
}
