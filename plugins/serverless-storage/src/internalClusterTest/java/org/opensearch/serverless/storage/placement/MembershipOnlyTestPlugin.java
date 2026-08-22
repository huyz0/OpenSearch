/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Test-only plugin carrying exactly the two registrations core used to make for computed-placement
 * membership before {@link ComputedPlacementMembership} and {@link ComputedPlacementMembershipService}
 * moved into this plugin: the {@code Metadata.Custom} NamedWriteables that {@code ClusterModule} used to
 * register, and the cluster-manager-side maintainer listener that {@code Node} used to add.
 *
 * <p>Deliberately <em>not</em> {@code ServerlessStoragePlugin}. The computed-placement ITs in this
 * package register their own fake suppliers with {@link
 * org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers}, and the real plugin's gate, filters, and
 * resolver would collide with those registrations. This class keeps each IT's environment exactly what
 * it was when the classes lived in core: membership machinery present, everything else absent.
 */
public class MembershipOnlyTestPlugin extends Plugin {

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, ComputedPlacementMembership.TYPE, ComputedPlacementMembership::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, ComputedPlacementMembership.TYPE, ComputedPlacementMembership::readDiffFrom)
        );
    }

    /**
     * The gateway half, mirroring {@code ServerlessStoragePlugin#getNamedXContent()}.
     *
     * <p>Without it this plugin reproduces the bug rather than the feature: the custom declares {@code
     * API_AND_GATEWAY}, gateway persistence round-trips through XContent, and a custom with no parser is
     * logged and skipped on read, so every full-cluster restart in a suite using this plugin would come
     * back with no membership at all.
     */
    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new org.opensearch.core.ParseField(ComputedPlacementMembership.TYPE),
                ComputedPlacementMembership::fromXContent
            )
        );
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        // Same gate Node.java used: membership is written by whichever node is elected, so every
        // cluster-manager-eligible node carries the maintainer and the service itself checks election.
        if (DiscoveryNode.isClusterManagerNode(environment.settings())) {
            clusterService.addListener(new ComputedPlacementMembershipService(clusterService));
        }
        return List.of();
    }
}
