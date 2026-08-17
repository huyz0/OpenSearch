/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.plugins;

import org.opensearch.cluster.metadata.IndexCreationStrategy;
import org.opensearch.cluster.metadata.IndexMetadataResolver;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingResolver;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An extension point for {@link Plugin} implementations to customer behavior of cluster management.
 *
 * @opensearch.api
 */
public interface ClusterPlugin {

    /**
     * Return deciders used to customize where shards are allocated.
     *
     * @param settings Settings for the node
     * @param clusterSettings Settings for the cluster
     * @return Custom {@link AllocationDecider} instances
     */
    default Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return Collections.emptyList();
    }

    /**
     * Return {@link ShardsAllocator} implementations added by this plugin.
     * <p>
     * The key of the returned {@link Map} is the name of the allocator, and the value
     * is a function to construct the allocator.
     *
     * @param settings Settings for the node
     * @param clusterSettings Settings for the cluster
     * @return A map of allocator implementations
     */
    default Map<String, Supplier<ShardsAllocator>> getShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
        return Collections.emptyMap();
    }

    /**
     * Return {@link ExistingShardsAllocator} implementations added by this plugin; the index setting
     * {@link ExistingShardsAllocator#EXISTING_SHARDS_ALLOCATOR_SETTING} sets the key of the allocator to use to allocate its shards. The
     * default allocator is {@link org.opensearch.gateway.GatewayAllocator}.
     */
    default Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
        return Collections.emptyMap();
    }

    /**
     * Returns List of custom index name resolvers which can support additional custom wildcards.
     * @return List of {@link IndexNameExpressionResolver.ExpressionResolver}
     */
    default Collection<IndexNameExpressionResolver.ExpressionResolver> getIndexNameCustomResolvers() {
        return Collections.emptyList();
    }

    /**
     * Phase C of {@code core-pluggability-refactor-plan.md}: a fallback for resolving an index's {@link
     * org.opensearch.cluster.metadata.IndexMetadata} when {@link org.opensearch.cluster.metadata.Metadata}
     * has no entry for it -- see {@link org.opensearch.cluster.metadata.IndexMetadataResolver}'s own
     * javadoc for why this is a single seam rather than something every caller needs to know about.
     * Empty by default, so a node without this plugin resolves exactly as it always has.
     *
     * @opensearch.experimental
     */
    default Optional<IndexMetadataResolver> getIndexMetadataResolver() {
        return Optional.empty();
    }

    /**
     * Phase C of {@code core-pluggability-refactor-plan.md}: the routing-table counterpart to {@link
     * #getIndexMetadataResolver()} -- see {@link IndexRoutingResolver}'s own javadoc. Empty by default,
     * so a node without this plugin resolves exactly as it always has.
     *
     * @opensearch.experimental
     */
    default Optional<IndexRoutingResolver> getIndexRoutingResolver() {
        return Optional.empty();
    }

    /**
     * Phase D of {@code core-pluggability-refactor-plan.md}: a plugin-owned decision of which index
     * names/requests belong to a plugin-managed plane -- see {@link
     * org.opensearch.cluster.metadata.IndexCreationStrategy}'s own javadoc. Empty by default, so a node
     * without this plugin creates and deletes indices exactly as it always has.
     *
     * @opensearch.experimental
     */
    default Optional<IndexCreationStrategy> getIndexCreationStrategy() {
        return Optional.empty();
    }

    /**
     * Phase E2 of {@code core-pluggability-refactor-plan.md}: a plugin-owned policy tuning {@link
     * org.opensearch.indices.cluster.IndicesClusterStateService}'s on-demand-opened-index residency
     * bookkeeping (sweep interval, idle-eviction threshold, max-open ceiling) -- see {@link
     * org.opensearch.indices.cluster.IndexResidencyPolicy}'s own javadoc for exactly what this does and
     * does not move out of core. Empty by default, so a node without this plugin uses the same numeric
     * defaults it always has.
     *
     * @opensearch.experimental
     */
    default Optional<org.opensearch.indices.cluster.IndexResidencyPolicy> getIndexResidencyPolicy() {
        return Optional.empty();
    }

    /**
     * Called when the node is started
     *
     * @deprecated Use {@link #onNodeStarted(DiscoveryNode)} for newer implementations.
     */
    @Deprecated
    default void onNodeStarted() {}

    /**
     * Called when node is started. DiscoveryNode argument is passed to allow referring localNode value inside plugin
     *
     * @param localNode local Node info
     */
    default void onNodeStarted(DiscoveryNode localNode) {
        onNodeStarted();
    }

    /**
     * @return true if this plugin will handle cluster state management on behalf of the node, so the node does not
     * need to discover a cluster manager and be part of a cluster.
     *
     * Note that if any ClusterPlugin returns true from this method, the node will start in clusterless mode.
     */
    default boolean isClusterless() {
        return false;
    }
}
