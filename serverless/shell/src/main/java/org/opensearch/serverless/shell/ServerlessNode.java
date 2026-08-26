/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.MetaStateService;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchService;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;

/**
 * A serverless node: the OpenSearch data plane with no control plane.
 *
 * <p>This is the shell described by {@code rfc-serverless-shell.md}. It constructs {@link IndicesService}
 * and {@link SearchService} directly and drives them from a node-local {@link ClusterState} handed to
 * {@link ClusterApplier}, rather than from a consensus-published one. It contains no
 * {@code Coordinator}, no {@code AllocationService}, no {@code GatewayMetaState} and no {@code Node} —
 * a property asserted by test, not by inspection.
 *
 * <p>What is proven about this construction, and what is not, is recorded in {@code s0-findings.md} and
 * {@code s1-findings.md}. Two findings from those probes are load-bearing here and are commented at
 * their call sites: the applier requires a {@link NodeConnectionsService}, and the data plane refuses
 * to activate a primary at term 0.
 *
 * <p>Phase 1 scope: lifecycle and the data plane. No transport, no REST, no metadata plane.
 */
public final class ServerlessNode implements Closeable {

    private final String nodeName;
    private final Settings settings;
    private final ThreadPool threadPool;
    private final NodeEnvironment nodeEnvironment;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final SearchService searchService;
    private final DiscoveryNode localNode;
    private volatile boolean started;

    /**
     * Constructs a node. Nothing is started until {@link #start()} is called.
     *
     * @param settings must carry {@code node.name}, {@code cluster.name} and {@code path.home}
     * @throws Exception if the node environment cannot be created or the data plane cannot be built
     */
    public ServerlessNode(Settings settings) throws Exception {
        this.settings = settings;
        this.nodeName = settings.get("node.name", "serverless-node");
        final Environment environment = new Environment(settings, null);
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        this.threadPool = new ThreadPool(settings);
        boolean success = false;
        try {
            this.nodeEnvironment = new NodeEnvironment(settings, environment);
            this.localNode = new DiscoveryNode(
                nodeName,
                nodeEnvironment.nodeId(),
                new TransportAddress(InetAddress.getLoopbackAddress(), 0),
                emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES,
                Version.CURRENT
            );
            this.clusterService = buildClusterService(clusterSettings);
            this.indicesService = buildIndicesService(environment, clusterSettings);
            this.searchService = buildSearchService(clusterSettings);
            success = true;
        } finally {
            if (success == false) {
                ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * A real {@link ClusterService} with no coordinator behind it — {@code rfc-serverless-shell.md} §5.1.
     */
    private ClusterService buildClusterService(ClusterSettings clusterSettings) {
        final ClusterService service = new ClusterService(settings, clusterSettings, threadPool);

        // S0/F1: the applier refuses to start without this. In a later phase it holds real transport
        // connections to the nodes named in the applied view; phase 1 has no transport.
        service.setNodeConnectionsService(new NodeConnectionsService(settings, threadPool, null) {
            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
                onCompletion.run();
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
                // no transport in phase 1
            }
        });

        // §10.5: clusterManagerNodeId is deliberately unset. There is no election, and S1 confirmed
        // that nothing in the data plane needs one.
        service.getClusterApplierService()
            .setInitialState(
                ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
                    .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build())
                    .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                    .build()
            );
        service.getClusterManagerService().setClusterStatePublisher(new LocalOnlyPublisher(service.getClusterApplierService()));
        service.getClusterManagerService().setClusterStateSupplier(service.getClusterApplierService()::state);
        return service;
    }

    private IndicesService buildIndicesService(Environment environment, ClusterSettings clusterSettings) throws Exception {
        final NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Collections.emptyList());
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
        // S0/F2: mandatory — DataFormatRegistry calls filterPlugins() during construction.
        final PluginsService pluginsService = new PluginsService(settings, null, null, Collections.emptyList());

        return new IndicesService(
            settings,
            pluginsService,
            nodeEnvironment,
            xContentRegistry,
            analysisRegistry,
            new IndexNameExpressionResolver(new ThreadContext(settings)),
            new IndicesModule(Collections.emptyList()).getMapperRegistry(),
            new NamedWriteableRegistry(Collections.emptyList()),
            threadPool,
            new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS),
            new NoneCircuitBreakerService(),
            BigArrays.NON_RECYCLING_INSTANCE,
            new ScriptService(settings, emptyMap(), emptyMap()),
            clusterService,
            null,                                   // Client — no node client in phase 1
            new MetaStateService(nodeEnvironment, xContentRegistry),
            Collections.emptyList(),
            emptyMap(),
            null,                                   // ValuesSourceRegistry
            emptyMap(),
            null,                                   // remote directory factory — serverless supplies its own
            () -> null,                             // RepositoriesService
            null,                                   // SearchRequestStats
            null,                                   // RemoteStoreStatsTrackerFactory
            emptyMap(),
            // The S0 spike used DefaultRecoverySettings, which lives in test/framework and is not
            // available to production code. A real shell builds these from settings.
            new org.opensearch.indices.recovery.RecoverySettings(settings, clusterSettings),
            new org.opensearch.common.cache.module.CacheModule(new ArrayList<>(), settings).getCacheService(),
            new org.opensearch.indices.RemoteStoreSettings(settings, clusterSettings)
        );
    }

    private SearchService buildSearchService(ClusterSettings clusterSettings) {
        return new SearchService(
            clusterService,
            indicesService,
            threadPool,
            new ScriptService(settings, emptyMap(), emptyMap()),
            new BigArrays(new PageCacheRecycler(settings), null, nodeName),
            new QueryPhase(),
            new FetchPhase(Collections.emptyList()),
            new org.opensearch.node.ResponseCollectorService(clusterService),
            new NoneCircuitBreakerService(),
            null,                                   // indexSearcherExecutor — no concurrent search yet
            new org.opensearch.tasks.TaskResourceTrackingService(settings, clusterSettings, threadPool),
            Collections.emptyList(),
            Collections.emptyList(),
            null
        );
    }

    /**
     * Starts the node. S0/F3: {@link IndicesService} and {@link SearchService} reject work until started.
     */
    public void start() {
        clusterService.start();
        indicesService.start();
        searchService.start();
        started = true;
    }

    /**
     * Hands this node a new node-local view. The shell is the second caller of
     * {@link ClusterApplier#onNewClusterState}; {@code Coordinator} is the first, and is absent here.
     *
     * @param state the projected view for this node
     * @param timeout how long to wait for the state to be applied
     * @throws Exception if application fails or times out
     */
    public void applyLocalView(ClusterState state, TimeValueLike timeout) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        clusterService.getClusterApplierService()
            .onNewClusterState("serverless-projected-view", () -> state, new ClusterApplier.ClusterApplyListener() {
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
        if (latch.await(timeout.millis(), TimeUnit.MILLISECONDS) == false) {
            throw new IllegalStateException("projected view was never applied on " + nodeName);
        }
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    /** Minimal timeout abstraction so the shell does not depend on a specific TimeValue import path. */
    @FunctionalInterface
    public interface TimeValueLike {
        /**
         * Returns the timeout.
         *
         * @return the timeout in milliseconds
         */
        long millis();
    }

    /**
     * Returns this node's identity as the data plane sees it.
     *
     * @return the local discovery node
     */
    public DiscoveryNode localNode() {
        return localNode;
    }

    /**
     * Returns the cluster service. Its state is a node-local view, not a consensus object.
     *
     * @return the cluster service
     */
    public ClusterService clusterService() {
        return clusterService;
    }

    /**
     * Returns the indices service.
     *
     * @return the indices service
     */
    public IndicesService indicesService() {
        return indicesService;
    }

    /**
     * Returns the search service.
     *
     * @return the search service
     */
    public SearchService searchService() {
        return searchService;
    }

    /**
     * Returns the thread pool.
     *
     * @return the thread pool
     */
    public ThreadPool threadPool() {
        return threadPool;
    }

    /**
     * Returns the node environment, which owns the data paths.
     *
     * @return the node environment
     */
    public NodeEnvironment nodeEnvironment() {
        return nodeEnvironment;
    }

    /**
     * Reports whether the node has been started.
     *
     * @return true once {@link #start()} has been called and before {@link #close()}
     */
    public boolean isStarted() {
        return started;
    }

    @Override
    public void close() {
        started = false;
        IOUtils.closeWhileHandlingException(searchService, indicesService, clusterService, nodeEnvironment);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
