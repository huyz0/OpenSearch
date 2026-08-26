/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.s0;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.MetaStateService;
import org.opensearch.index.IndexService;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.DefaultRecoverySettings;
import org.opensearch.index.engine.MergedSegmentWarmerFactory;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchService;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;

/**
 * Hand-rolled construction of the data plane for {@link S0ShellSpikeTests}. Disposable, like its caller.
 *
 * <p>Kept separate from the assertions so that the spike's answer to RFC §11 Q3 — "what is the actual
 * constructor closure" — is a single readable file rather than something inferred from a stack trace.
 */
final class S0Wiring {

    private S0Wiring() {}

    /**
     * RFC §5.1 in production form: a real {@link ClusterService} whose state is set locally.
     *
     * <p>The publisher applies to the local applier and completes — there are no peers. That is the
     * whole of {@code LocalOnlyPublisher}.
     */
    static ClusterService clusterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, DiscoveryNode local) {
        final ClusterService clusterService = new ClusterService(settings, clusterSettings, threadPool);
        // FINDING (S0): ClusterApplierService.doStart() requires a NodeConnectionsService as well as the
        // publisher/supplier pair on ClusterManagerService. RFC §2.3 recorded two required collaborators;
        // there are three. In the shell this is a real service driven by the membership view (§10) —
        // its job is to hold transport connections to the nodes named in the applied state. S0 has no
        // transport, so it is a no-op here.
        clusterService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
                onCompletion.run();
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {}
        });
        // §10.5: clusterManagerNodeId is deliberately NOT set. There is no election.
        final ClusterState initial = ClusterState.builder(new ClusterName(settings.get("cluster.name", "s0-cluster")))
            .nodes(DiscoveryNodes.builder().add(local).localNodeId(local.getId()).build())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        clusterService.getClusterApplierService().setInitialState(initial);
        clusterService.getClusterManagerService().setClusterStatePublisher((event, publishListener, ackListener) -> {
            clusterService.getClusterApplierService()
                .onNewClusterState("s0-local-publish", event::state, new ClusterApplier.ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) {
                        publishListener.onResponse(null);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        publishListener.onFailure(e);
                    }
                });
        });
        clusterService.getClusterManagerService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }

    /** The shell as the second caller of {@link ClusterApplier#onNewClusterState}. Coordinator is the first. */
    static void applyLocally(ClusterService clusterService, ClusterState state) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        clusterService.getClusterApplierService()
            .onNewClusterState("s0-projected-view", () -> state, new ClusterApplier.ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    failure.set(e);
                    latch.countDown();
                }
            });
        if (latch.await(30, TimeUnit.SECONDS) == false) {
            throw new AssertionError("cluster state was never applied");
        }
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    static IndicesService indicesService(
        Settings settings,
        Environment environment,
        NodeEnvironment nodeEnv,
        ThreadPool threadPool,
        ClusterService clusterService,
        ClusterSettings clusterSettings
    ) throws Exception {
        final NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Collections.emptyList());
        final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        final AnalysisRegistry analysisRegistry = new AnalysisRegistry(
            environment,
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
        // FINDING (S0): PluginsService is genuinely required — DataFormatRegistry calls filterPlugins()
        // during IndicesService construction. A real one with no plugins is what the shell will hold.
        final org.opensearch.plugins.PluginsService pluginsService = new org.opensearch.plugins.PluginsService(
            settings,
            null,
            null,
            Collections.emptyList()
        );
        return new IndicesService(
            settings,
            pluginsService,
            nodeEnv,
            xContentRegistry,
            analysisRegistry,
            new IndexNameExpressionResolver(new ThreadContext(settings)),
            new IndicesModule(Collections.emptyList()).getMapperRegistry(),
            namedWriteableRegistry,
            threadPool,
            new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS),
            new NoneCircuitBreakerService(),
            BigArrays.NON_RECYCLING_INSTANCE,
            new ScriptService(settings, emptyMap(), emptyMap()),
            clusterService,
            null,
            new MetaStateService(nodeEnv, xContentRegistry),
            Collections.emptyList(),
            emptyMap(),
            null,
            emptyMap(),
            null,
            () -> null,
            null,
            null,
            emptyMap(),
            DefaultRecoverySettings.INSTANCE,
            new org.opensearch.common.cache.module.CacheModule(new ArrayList<>(), settings).getCacheService(),
            org.opensearch.indices.DefaultRemoteStoreSettings.INSTANCE
        );
    }

    static SearchService searchService(
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        ClusterService clusterService,
        IndicesService indicesService,
        BigArrays bigArrays
    ) {
        return new SearchService(
            clusterService,
            indicesService,
            threadPool,
            new ScriptService(settings, emptyMap(), emptyMap()),
            bigArrays,
            new QueryPhase(),
            new FetchPhase(Collections.emptyList()),
            new org.opensearch.node.ResponseCollectorService(clusterService),
            new NoneCircuitBreakerService(),
            null,
            new org.opensearch.tasks.TaskResourceTrackingService(settings, clusterSettings, threadPool),
            Collections.emptyList(),
            Collections.emptyList(),
            null
        );
    }

    /** RFC §2.2: the shard lifecycle takes value objects, so a shell-owned reconciler can drive it. */
    static IndexShard openShard(IndexService indexService, ShardRouting routing, DiscoveryNode local, ClusterService clusterService)
        throws Exception {
        return indexService.createShard(
            routing,
            shardId -> {},
            RetentionLeaseSyncer.EMPTY,
            null,
            null,
            null,
            local,
            null,
            DiscoveryNodes.builder().add(local).localNodeId(local.getId()).build(),
            new MergedSegmentWarmerFactory(null, null, clusterService),
            null,
            null
        );
    }
}
