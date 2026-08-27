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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.network.NetworkService;
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
import org.opensearch.http.HttpServerTransport;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rest.RestController;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchService;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.LocalViewProjector;
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.serverless.membership.MembershipSource;
import org.opensearch.serverless.rest.NotImplementedHandler;
import org.opensearch.serverless.rest.ServerlessHealthHandler;
import org.opensearch.serverless.rest.ServerlessRootHandler;
import org.opensearch.serverless.shard.ShardReconciler;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Netty4ModulePlugin;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.usage.UsageService;

import java.io.Closeable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
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
    private final RestController restController;
    private final TransportService transportService;
    private final HttpServerTransport httpServerTransport;
    private final NodeClient nodeClient;
    private volatile DiscoveryNode localNode;
    private volatile MembershipSource membershipSource;
    private volatile LocalViewProjector projector;
    private volatile ShardReconciler reconciler;
    private volatile boolean started;

    /**
     * Constructs a node. Nothing is started until {@link #start()} is called.
     *
     * @param settings must carry {@code node.name}, {@code cluster.name} and {@code path.home}
     * @throws Exception if the node environment cannot be created or the data plane cannot be built
     */
    public ServerlessNode(Settings settings) throws Exception {
        this.settings = withShellDefaults(settings);
        settings = this.settings;
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

            this.nodeClient = new NodeClient(settings, threadPool);
            this.restController = buildRestController();
            final NetworkModule networkModule = buildNetworkModule(clusterSettings);
            final Transport transport = networkModule.getTransportSupplier().get();
            this.transportService = new TransportService(
                settings,
                transport,
                null,
                threadPool,
                networkModule.getTransportInterceptor(),
                boundAddress -> new DiscoveryNode(
                    nodeName,
                    nodeEnvironment.nodeId(),
                    boundAddress.publishAddress(),
                    emptyMap(),
                    DiscoveryNodeRole.BUILT_IN_ROLES,
                    Version.CURRENT
                ),
                clusterSettings,
                Set.of(),
                NoopTracer.INSTANCE
            );
            this.httpServerTransport = networkModule.getHttpServerTransportSupplier().get();
            success = true;
        } finally {
            if (success == false) {
                ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Layers the shell's own transport choice under whatever the caller supplied.
     *
     * <p>A classic node discovers its transport through the plugin system and then fails at runtime if
     * nothing set {@code transport.type}. The shell supports exactly one transport, so leaving that as a
     * lookup with a single possible answer only creates a way to misconfigure it.
     */
    private static Settings withShellDefaults(Settings supplied) {
        return Settings.builder()
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4ModulePlugin.NETTY_TRANSPORT_NAME)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            .put(supplied)
            .build();
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
     * The REST surface is an explicit allowlist (decision D2). Anything not registered here does not
     * exist, and returns 404/501 rather than an empty success.
     */
    private RestController buildRestController() {
        final RestController controller = new RestController(
            Set.of(),
            null,
            nodeClient,
            new NoneCircuitBreakerService(),
            new UsageService()
        );
        controller.registerHandler(
            new ServerlessRootHandler(nodeName, ClusterName.CLUSTER_NAME_SETTING.get(settings).value(), nodeEnvironment::nodeId)
        );
        controller.registerHandler(
            new ServerlessHealthHandler(
                () -> started,
                () -> membershipSource == null ? 1 : membershipSource.current().size(),
                nodeEnvironment::nodeId
            )
        );

        // D2: these exist in classic OpenSearch and are absent here by design. Registering an explicit
        // refusal turns "no handler for uri" — which reads like a typo — into a statement.
        final String noGlobalState = "there is no cluster-wide state in a serverless cluster; "
            + "no node can answer this, and a node-local answer would be misleading";
        for (String path : new String[] {
            "/_cluster/health",
            "/_cluster/state",
            "/_cluster/stats",
            "/_cluster/settings",
            "/_cluster/reroute",
            "/_nodes",
            "/_cat/indices",
            "/_cat/shards",
            "/_cat/nodes",
            "/_cat/allocation" }) {
            controller.registerHandler(new NotImplementedHandler(path, noGlobalState));
        }
        return controller;
    }

    /**
     * The shell picks netty4 directly rather than discovering a transport through the plugin system.
     * There is exactly one supported transport, and pretending otherwise would add a lookup with one
     * possible answer.
     */
    private NetworkModule buildNetworkModule(ClusterSettings clusterSettings) {
        return new NetworkModule(
            settings,
            java.util.List.of(new Netty4ModulePlugin()),
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new PageCacheRecycler(settings),
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(Collections.emptyList()),
            new NamedXContentRegistry(Collections.emptyList()),
            new NetworkService(Collections.emptyList()),
            restController,
            clusterSettings,
            NoopTracer.INSTANCE,
            Collections.emptyList(),
            Collections.emptyList()
        );
    }

    /**
     * Supplies the membership view the health endpoint reports. Optional: a node with none reports that
     * it can see one member, itself, which is true.
     *
     * @param source the membership source, or null
     */
    public void setMembershipSource(MembershipSource source) {
        this.membershipSource = source;
    }

    /**
     * Starts the node. S0/F3: {@link IndicesService} and {@link SearchService} reject work until started.
     */
    public void start() {
        clusterService.start();
        indicesService.start();
        searchService.start();

        transportService.start();
        transportService.acceptIncomingRequests();
        httpServerTransport.start();

        // The identity in the initial view carried a placeholder address, because the real one is not
        // known until the transport binds. Replace it now that it is.
        localNode = transportService.getLocalNode();

        // Built here rather than in the constructor: both need the node identity, and the identity is
        // not final until the transport has bound and told us the address it got.
        projector = new LocalViewProjector(ClusterName.CLUSTER_NAME_SETTING.get(settings), localNode);
        reconciler = new ShardReconciler(indicesService, localNode);
        started = true;
    }

    /**
     * Projects the given truth into this node's local view, applies it, and opens every shard this node
     * owns. Idempotent.
     *
     * <p>This is the whole shell loop in one call: descriptors and shard-heads in, serving shards out.
     * Phase 3 replaces the arguments with reads from the object store; the shape does not change.
     *
     * @param descriptors the indices this node should know about
     * @param owned the shards this node owns, per shard-heads
     * @return the shards opened by this call
     * @throws Exception if projection, application or shard opening fails
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> applyTruth(
        java.util.Collection<IndexDescriptor> descriptors,
        java.util.Collection<ShardAssignment> owned
    ) throws Exception {
        ensureStarted();
        final ClusterState view = projector.project(descriptors, owned);
        applyLocalView(view, () -> 30_000L);
        return reconciler.ensureOpen(view, owned);
    }

    /**
     * Reads this node's truth from the metadata plane and applies it.
     *
     * <p>This is the whole steady-state loop, and it replaces cluster-state publication: no node tells
     * this one what to serve, it reads the object store and works it out. Phase 8 puts this on a timer
     * and wakes it early on a gossip epoch; phase 3 calls it explicitly.
     *
     * @param plane the metadata plane to read from
     * @return the shards opened by this call
     * @throws Exception if reading, projection or shard opening fails
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> syncFrom(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        ensureStarted();
        final org.opensearch.serverless.metadata.Truth truth = plane.truthFor(localNode.getId());
        final java.util.Set<org.opensearch.core.index.shard.ShardId> opened = applyTruth(truth.descriptors(), truth.assignments());

        // Closing here is NOT the rule §5.2 forbids, and the difference is the input, not the code.
        // ShardReconciler refuses to close on absence from a projected *view* because a view is a local
        // computation that can be wrong or incomplete. This set comes from shard-heads: if truth no
        // longer assigns a shard to this node, another node has already won it by compare-and-swap, and
        // continuing to hold it is the actual hazard. Hence the two entry points and their two inputs:
        // applyTruth takes a view and never closes; syncFrom takes truth and may.
        final java.util.Set<org.opensearch.core.index.shard.ShardId> stillOwned = new java.util.HashSet<>();
        for (ShardAssignment assignment : truth.assignments()) {
            final IndexMetadata metadata = clusterService.state().metadata().index(assignment.indexName());
            if (metadata != null) {
                stillOwned.add(new org.opensearch.core.index.shard.ShardId(metadata.getIndex(), assignment.shardId()));
            }
        }
        for (org.opensearch.core.index.shard.ShardId held : reconciler.openShards()) {
            if (stillOwned.contains(held) == false) {
                reconciler.releaseShard(held, "shard-head no longer assigns this shard to " + nodeName);
            }
        }
        return opened;
    }

    /**
     * Closes a shard this node has stopped owning.
     *
     * <p>Deliberately separate from {@link #applyTruth}: a shard is never closed because a view omitted
     * it, only because truth says ownership is gone. See {@code ShardReconciler} and
     * {@code s1-findings.md}.
     *
     * @param shardId the shard to release
     * @param reason why, for the log
     */
    public void releaseShard(org.opensearch.core.index.shard.ShardId shardId, String reason) {
        ensureStarted();
        reconciler.releaseShard(shardId, reason);
    }

    /**
     * Returns this node's shard reconciler.
     *
     * @return the reconciler, or null before {@link #start()}
     */
    public ShardReconciler reconciler() {
        return reconciler;
    }

    /**
     * Returns this node's view projector.
     *
     * @return the projector, or null before {@link #start()}
     */
    public LocalViewProjector projector() {
        return projector;
    }

    private void ensureStarted() {
        if (started == false) {
            throw new IllegalStateException("node " + nodeName + " has not been started");
        }
    }

    /**
     * Returns the address the HTTP layer actually bound to.
     *
     * @return the bound HTTP address
     */
    public org.opensearch.core.common.transport.BoundTransportAddress boundHttpAddress() {
        return httpServerTransport.boundAddress();
    }

    /**
     * Returns the address the transport layer actually bound to.
     *
     * @return the bound transport address
     */
    public org.opensearch.core.common.transport.BoundTransportAddress boundTransportAddress() {
        return transportService.boundAddress();
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
        IOUtils.closeWhileHandlingException(
            httpServerTransport,
            transportService,
            searchService,
            indicesService,
            clusterService,
            nodeEnvironment
        );
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
