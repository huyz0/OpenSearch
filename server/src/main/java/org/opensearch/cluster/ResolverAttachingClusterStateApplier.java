/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.cluster.metadata.IndexMetadataResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingResolver;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;

import java.util.Optional;

/**
 * Phase C of {@code core-pluggability-refactor-plan.md}: the one attachment point that gives a node's
 * {@link Metadata}/{@link RoutingTable} their plugin-supplied {@link IndexMetadataResolver}/{@link
 * IndexRoutingResolver}, if any {@code ClusterPlugin} on this node provides one.
 *
 * <p>Registered as a <b>high priority</b> {@link ClusterStateApplier} -- see {@link ClusterStateApplier}'s
 * own javadoc: high-priority appliers run before a newly-applied state becomes visible via {@link
 * ClusterService#state()}, so every reader on this node, including the very first one, sees a state whose
 * resolver is already attached.
 *
 * <p>Runs on <em>every</em> applied cluster state, not just the node's first, and is written to be a cheap
 * no-op on most of them: {@link Metadata#indexMetadataResolver()}/{@link RoutingTable#indexRoutingResolver()}
 * already carry a previously-attached resolver forward across the ordinary ways a new {@code Metadata}/{@code
 * RoutingTable} is produced from an existing one on this node ({@code Builder} copy-constructors and diff
 * application -- see {@code Metadata#resolver}'s javadoc for the full propagation story). Only a
 * from-scratch instance -- the node's bootstrap state, or a full (non-diff) cluster state sync, which
 * deserializes fresh {@code Metadata}/{@code RoutingTable} that never passed through this node's own
 * {@code Builder}/diff machinery -- actually needs the attach-on-null check below to do anything.
 */
public final class ResolverAttachingClusterStateApplier implements ClusterStateApplier {

    private final IndexMetadataResolver indexMetadataResolver;
    private final IndexRoutingResolver indexRoutingResolver;

    public ResolverAttachingClusterStateApplier(
        Optional<IndexMetadataResolver> indexMetadataResolver,
        Optional<IndexRoutingResolver> indexRoutingResolver
    ) {
        this.indexMetadataResolver = indexMetadataResolver.orElse(null);
        this.indexRoutingResolver = indexRoutingResolver.orElse(null);
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (indexMetadataResolver != null && state.metadata().indexMetadataResolver() == null) {
            state.metadata().attachIndexMetadataResolver(indexMetadataResolver);
        }
        if (indexRoutingResolver != null && state.routingTable().indexRoutingResolver() == null) {
            state.routingTable().attachIndexRoutingResolver(indexRoutingResolver);
        }
    }
}
