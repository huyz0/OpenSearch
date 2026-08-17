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

import java.util.Collection;

/**
 * Phase C5 of {@code core-pluggability-refactor-plan.md}: the routing counterpart to {@link
 * org.opensearch.cluster.metadata.SupplierBackedIndexMetadataResolver} -- see that class's own javadoc for
 * why this is a thin, generic adapter over the pre-existing static registry ({@link
 * AbsentIndexRoutingSuppliers}) rather than plugin-owned logic, and why it is safe to register additively,
 * ahead of Phase C4's call-site migration.
 *
 * <p>Implements all three {@link IndexRoutingResolver} methods, not just {@link #resolve}, since {@link
 * AbsentIndexRoutingSuppliers} already has the equivalent of all three ({@link
 * AbsentIndexRoutingSuppliers#supply(ClusterState, IndexMetadata)}, {@link
 * AbsentIndexRoutingSuppliers#shouldPublishRouting}, {@link AbsentIndexRoutingSuppliers#localShards}) and
 * forwarding all of them costs nothing extra here -- though as {@link IndexRoutingResolver}'s own "wiring
 * status" note documents, only {@link #resolve} has a core call site consulting it yet, so the other two
 * are currently reachable only by a caller that goes looking for this class directly (e.g. a future Phase
 * C4b call-site migration, or a test).
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

    @Override
    public Collection<ShardRouting> localShardsFor(ClusterState state, String nodeId) {
        return AbsentIndexRoutingSuppliers.localShards(state, nodeId);
    }
}
