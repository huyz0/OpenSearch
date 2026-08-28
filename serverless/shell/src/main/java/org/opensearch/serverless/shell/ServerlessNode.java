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
import java.util.Map;
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

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(ServerlessNode.class);

    /** Accepts writer activation: takes ownership of shards and indexes into them. */
    public static final String ROLE_INGEST = "ingest";

    /** Accepts reader activation: serves search from object-store-backed segments. */
    public static final String ROLE_SEARCH = "search";

    /** The store type that reads published segments lazily, a block at a time. */
    public static final String BLOCK_CACHE_STORE_TYPE = "serverless_block_cache";

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
    private volatile org.opensearch.serverless.metadata.MetadataPlane metadataPlane;
    private volatile org.opensearch.serverless.transport.ShardRouter router;
    private final Set<String> roles;
    private final org.opensearch.serverless.store.BlockCache blockCache;
    private final Map<String, IndexDescriptor> served = new java.util.concurrent.ConcurrentHashMap<>();
    private final Set<Map.Entry<String, Integer>> readerShards = java.util.concurrent.ConcurrentHashMap.newKeySet();
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
        // §10.4: role is a runtime attribute a node advertises, not a topology decision baked into a
        // cluster. One binary; an operator gets asymmetric scaling from two Deployments differing by one
        // environment variable, and role switching under load stays a policy question.
        this.roles = Set.copyOf(settings.getAsList("serverless.roles", java.util.List.of(ROLE_INGEST, ROLE_SEARCH)));
        // Block size is a real tuning knob, not a constant: it trades request count against wasted
        // bytes, and the right value depends on the object store's per-request cost and the index's
        // access pattern. Exposed so that can be measured rather than guessed.
        this.blockCache = new org.opensearch.serverless.store.BlockCache(
            settings.getAsInt("serverless.block_cache.block_size", org.opensearch.serverless.store.BlockCache.DEFAULT_BLOCK_SIZE),
            settings.getAsInt("serverless.block_cache.max_blocks", 4096)
        );
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
            engineFactoryProviders(),
            directoryFactories(),
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

    /**
     * The node's role decides its engine, which is what makes the two shard roles asymmetric rather
     * than merely differently labelled.
     *
     * <p>A node that does not accept writer activation runs {@link org.opensearch.index.engine.ReadOnlyEngine},
     * so a reader shard is <em>structurally</em> unable to write — not merely expected not to. That
     * matters because a reader opens a shard without acquiring its head: there is no compare-and-swap
     * behind it, and safety comes from the engine being incapable rather than from a convention.
     *
     * <p>Readers needing no coordination is the point, not a shortcut.
     * {@code rfc-serverless-metadata-plane.md} §6: readers are interchangeable caches, so any search
     * node can serve any shard by reading its manifest, and the directory tier just learns about it.
     */
    private
        java.util.Collection<
            java.util.function.BiFunction<
                org.opensearch.index.IndexSettings,
                org.opensearch.cluster.routing.ShardRouting,
                java.util.Optional<org.opensearch.index.engine.EngineFactory>>>
        engineFactoryProviders() {
        if (roles.contains(ROLE_INGEST)) {
            return Collections.emptyList();   // the default read-write engine
        }
        final org.opensearch.index.engine.EngineFactory readOnly = config -> new org.opensearch.index.engine.ReadOnlyEngine(
            config,
            null,
            null,
            true,
            java.util.function.Function.identity(),
            false
        );
        return java.util.List.of((indexSettings, routing) -> java.util.Optional.of(readOnly));
    }

    /**
     * The store type that makes a reader lazy.
     *
     * <p>Registered on every node; whether a shard uses it is decided per shard when it is opened, by
     * {@code ShardReconciler}. A writer keeps a local copy it can merge into; a reader reads blocks.
     */
    private Map<String, org.opensearch.plugins.IndexStorePlugin.DirectoryFactory> directoryFactories() {
        final org.opensearch.plugins.IndexStorePlugin.DirectoryFactory factory =
            new org.opensearch.plugins.IndexStorePlugin.DirectoryFactory() {
                @Override
                public org.apache.lucene.store.Directory newDirectory(
                    org.opensearch.index.IndexSettings indexSettings,
                    org.opensearch.index.shard.ShardPath shardPath
                ) throws java.io.IOException {
                    final org.apache.lucene.store.Directory local = org.apache.lucene.store.FSDirectory.open(shardPath.resolveIndex());
                    final var plane = metadataPlane;
                    if (plane == null) {
                        // No metadata plane means no manifest to read; a plain local directory is the truthful
                        // fallback rather than a half-built remote one.
                        return local;
                    }
                    final org.opensearch.core.index.shard.ShardId shardId = shardPath.getShardId();
                    return org.opensearch.serverless.store.BlockCacheDirectory.create(
                        local,
                        plane.blobStore(),
                        org.opensearch.serverless.metadata.RegisterMap.shardData(plane.basePath(), shardId.getIndexName(), shardId.id()),
                        blockCache,
                        shardId.getIndexName() + "#" + shardId.id()
                    );
                }

                @Override
                public org.apache.lucene.store.Directory newFSDirectory(
                    java.nio.file.Path location,
                    org.apache.lucene.store.LockFactory lockFactory,
                    org.opensearch.index.IndexSettings indexSettings
                ) throws java.io.IOException {
                    // This, not newDirectory, is the path the store actually takes. It hands over a
                    // location and settings rather than a ShardPath, so the shard identity has to be
                    // recovered from both: the index name from the settings, the shard number from the
                    // directory the location sits in ({data}/indices/{uuid}/{shard}/index).
                    final org.apache.lucene.store.Directory local = org.apache.lucene.store.FSDirectory.open(location, lockFactory);
                    final var plane = metadataPlane;
                    if (plane == null) {
                        return local;
                    }
                    final String indexName = indexSettings.getIndex().getName();
                    final int shardNumber;
                    try {
                        shardNumber = Integer.parseInt(location.getParent().getFileName().toString());
                    } catch (RuntimeException e) {
                        // An unexpected layout means we cannot say which shard this is, and guessing would
                        // attach one shard's directory to another's data. Local-only is the safe answer.
                        logger.warn("could not derive a shard number from " + location + "; serving it from local disk only", e);
                        return local;
                    }
                    return org.opensearch.serverless.store.BlockCacheDirectory.create(
                        local,
                        plane.blobStore(),
                        org.opensearch.serverless.metadata.RegisterMap.shardData(plane.basePath(), indexName, shardNumber),
                        blockCache,
                        indexName + "#" + shardNumber
                    );
                }
            };
        return Map.of(BLOCK_CACHE_STORE_TYPE, factory);
    }

    /**
     * Returns this node's block cache, whose counters say how much was actually fetched.
     *
     * @return the block cache
     */
    public org.opensearch.serverless.store.BlockCache blockCache() {
        return blockCache;
    }

    private SearchService buildSearchService(ClusterSettings clusterSettings) {
        return new SearchService(
            clusterService,
            indicesService,
            threadPool,
            new ScriptService(settings, emptyMap(), emptyMap()),
            new BigArrays(new PageCacheRecycler(settings), null, nodeName),
            new QueryPhase(),
            // Built by SearchModule, not by hand. A FetchPhase with no sub-phases returns hits with an
            // id and no _source, and reports success -- the fetch sub-phases are what load the source,
            // highlight, and so on. Constructing it empty looked harmless and silently dropped every
            // document body.
            new org.opensearch.search.SearchModule(settings, java.util.List.of()).getFetchPhase(),
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
        controller.registerHandler(new org.opensearch.serverless.rest.IndexAdminHandler(() -> metadataPlane));
        controller.registerHandler(new org.opensearch.serverless.rest.DocumentHandler(() -> this, () -> metadataPlane));
        controller.registerHandler(new org.opensearch.serverless.rest.SearchHandler(() -> this, () -> metadataPlane));
        controller.registerHandler(new org.opensearch.serverless.rest.CatalogHandler(() -> metadataPlane));
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

        // D2 and the enumeration rule together: listing a deployment's indices is an inventory
        // operation, not a serving one. Offering it here -- even paginated -- invites a caller to walk
        // a hundred million indices through a request path, so the request path does not offer it.
        controller.registerHandler(
            new NotImplementedHandler(
                "/_serverless/indices",
                "enumerating indices is a maintenance operation, not a serving one; "
                    + "look an index up by name, or run an offline inventory"
            )
        );
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
     * Gives this node's REST surface a metadata plane to read and write.
     *
     * <p>Set after construction because the plane is a deployment-level object and the REST routes are
     * registered while the node is still being built. Until it is set, the admin endpoints answer 503
     * saying exactly that, rather than answering as though nothing existed.
     *
     * @param plane the metadata plane, or null
     */
    public void setMetadataPlane(org.opensearch.serverless.metadata.MetadataPlane plane) {
        this.metadataPlane = plane;
        if (plane != null) {
            setMembershipSource(plane.membership());
            try {
                // Publish the lease immediately. It is this node's entry in the address book that peers
                // use to forward writes here, so a node that has not renewed yet is not merely invisible
                // -- it is unroutable, and a client's write would be refused with nowhere to send it.
                renewOwnLease(plane);
            } catch (java.io.IOException e) {
                logger.warn("could not publish this node's lease; peers will not be able to route to it", e);
            }
        }
    }

    private void renewOwnLease(org.opensearch.serverless.metadata.MetadataPlane plane) throws java.io.IOException {
        plane.membership()
            .renew(
                new org.opensearch.serverless.membership.NodeLease(
                    localNode.getId(),
                    localNode.getEphemeralId(),
                    localNode.getAddress().toString(),
                    roles,
                    0L
                )
            );
    }

    /**
     * Returns this node's shard router, which forwards work to the node that owns a shard.
     *
     * @return the router, or null before {@link #start()}
     */
    public org.opensearch.serverless.transport.ShardRouter router() {
        return router;
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
        router = new org.opensearch.serverless.transport.ShardRouter(this, transportService, () -> metadataPlane);
        router.registerHandlers();

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
        adopt(plane);
        reconciler.setSegmentPublishers(plane::segmentPublisher);
        reconciler.setWalStores(plane::walStore);
        final org.opensearch.serverless.metadata.Truth truth = plane.truthFor(localNode.getId());

        // Phase 4 has only writer shards, and a node that does not accept writer activation must not
        // open one. Phase 5 adds reader shards, at which point a search-only node opens those instead of
        // opening nothing -- so this gate is on the role, not on "has any role".
        final java.util.Collection<ShardAssignment> assignable = roles.contains(ROLE_INGEST) ? truth.assignments() : java.util.List.of();
        final java.util.Set<org.opensearch.core.index.shard.ShardId> opened = applyTruth(truth.descriptors(), assignable);

        // Closing here is NOT the rule §5.2 forbids, and the difference is the input, not the code.
        // ShardReconciler refuses to close on absence from a projected *view* because a view is a local
        // computation that can be wrong or incomplete. This set comes from shard-heads: if truth no
        // longer assigns a shard to this node, another node has already won it by compare-and-swap, and
        // continuing to hold it is the actual hazard. Hence the two entry points and their two inputs:
        // applyTruth takes a view and never closes; syncFrom takes truth and may.
        final java.util.Set<org.opensearch.core.index.shard.ShardId> stillOwned = new java.util.HashSet<>();
        for (ShardAssignment assignment : assignable) {
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
     * Takes ownership of a shard and opens it for writing.
     *
     * <p>One compare-and-swap, then the ordinary sync. A node that loses the race gets an empty result
     * rather than an exception, because losing is not an error: the winner's head is in the metadata
     * plane and the correct response is to route there, never to retry activation elsewhere.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shardNumber the shard
     * @return the shard, or empty if another node holds a live lease on it
     * @throws Exception if activation or opening fails
     */
    public java.util.Optional<org.opensearch.core.index.shard.ShardId> activateWriter(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber
    ) throws Exception {
        ensureStarted();
        if (roles.contains(ROLE_INGEST) == false) {
            throw new IllegalStateException("node " + nodeName + " does not accept writer activation; its roles are " + roles);
        }
        final org.opensearch.serverless.metadata.Acquisition acquisition = plane.activate(
            indexName,
            shardNumber,
            localNode.getId(),
            localNode.getEphemeralId()
        );
        if (acquisition.acquired() == false) {
            return java.util.Optional.empty();
        }
        syncFrom(plane);
        final IndexMetadata metadata = clusterService.state().metadata().index(indexName);
        return metadata == null
            ? java.util.Optional.empty()
            : java.util.Optional.of(new org.opensearch.core.index.shard.ShardId(metadata.getIndex(), shardNumber));
    }

    /**
     * Renews the leases this node holds, and lets go of anything it has lost.
     *
     * <p>This is the whole of failure detection, and it runs on the node that might be failing rather
     * than on one watching it. A node whose process is paused, partitioned or dead simply stops calling
     * this, its leases lapse, and another node acquires by compare-and-swap. Nothing needs to agree that
     * it died.
     *
     * <p>The release half matters as much as the renewal half. A node that has lost a shard must stop
     * serving it, and this is where it finds out — a renewal that comes back empty means the head no
     * longer names this node, which happens exactly when someone else won it.
     *
     * @param plane the metadata plane
     * @return the shards released because this node no longer owns them
     * @throws Exception if the metadata plane cannot be read or written
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> heartbeat(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        ensureStarted();
        final java.util.Set<org.opensearch.core.index.shard.ShardId> released = new java.util.LinkedHashSet<>();

        // Always, not only under §7's batching. Under batching this is what keeps the node's shards
        // alive; in either mode it is what keeps the node addressable, since peers resolve a forwarding
        // target from its lease. One write per tick per node, which is what phase 8 measured batching
        // down to anyway -- per-shard mode now pays it too, and that is the honest cost of being
        // reachable.
        renewOwnLease(plane);

        for (org.opensearch.core.index.shard.ShardId shardId : reconciler.openShards()) {
            if (reconciler.readerShards().contains(shardId)) {
                // Readers hold no lease, so there is nothing to renew and nothing to lose.
                continue;
            }
            if (plane.usesNodeLeaseLiveness()) {
                // The head still needs reading, because losing a shard is something only the head can
                // tell us -- but it is a read, not a write, and that is the whole saving.
                final var head = plane.heads().read(shardId.getIndexName(), shardId.id());
                if (head.isPresent() && localNode.getId().equals(head.get().ownerNodeId())) {
                    continue;
                }
            } else if (plane.heads().renew(shardId.getIndexName(), shardId.id(), localNode.getId()).isPresent()) {
                continue;
            }
            reconciler.releaseShard(shardId, "lease lost: the shard-head no longer names " + nodeName);
            // Drop the claim too. Leaving it is safe -- it is verified on read -- but a node that has
            // churned through shards for a year should not list a year of them to find today's.
            plane.forgetAssignment(localNode.getId(), shardId.getIndexName(), shardId.id());
            released.add(shardId);
        }
        return released;
    }

    /**
     * Serves a shard as a reader: opens the published commit without owning the shard.
     *
     * <p>Reader activation is the cheap half of the design. There is no compare-and-swap and no entry in
     * the shard-head, because a reader cannot conflict with anything — it does not write, and the node's
     * engine is read-only when it does not accept writer activation. Any number of search nodes may
     * serve the same shard at once.
     *
     * <p>Accumulating rather than replacing: a search node typically serves many shards, and each call
     * re-projects the full set so the view keeps describing everything this node holds.
     *
     * @param plane the metadata plane to read from
     * @param indexName the index to serve
     * @param shardNumber the shard to serve
     * @return the shard id now open as a reader
     * @throws Exception if the index does not exist, nothing has been published, or opening fails
     */
    public org.opensearch.core.index.shard.ShardId serveAsReader(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber
    ) throws Exception {
        ensureStarted();
        adopt(plane);
        reconciler.setSegmentPublishers(plane::segmentPublisher);
        reconciler.setWalStores(plane::walStore);
        final IndexDescriptor descriptor = plane.describe(indexName)
            .orElseThrow(() -> new IllegalArgumentException("no such index: " + indexName));

        served.put(indexName, descriptor);
        readerShards.add(new java.util.AbstractMap.SimpleEntry<>(indexName, shardNumber));

        final java.util.List<ShardAssignment> assignments = new java.util.ArrayList<>();
        for (var entry : readerShards) {
            final long term = plane.segmentPublisher(entry.getKey(), entry.getValue())
                .readManifest()
                .map(org.opensearch.serverless.store.CommitManifest::term)
                .orElse(1L);
            assignments.add(new ShardAssignment(entry.getKey(), entry.getValue(), term));
        }
        final ClusterState view = projector.project(served.values(), assignments);
        applyLocalView(view, () -> 30_000L);
        return reconciler.openReader(view, indexName, shardNumber);
    }

    /**
     * Indexes a document durably: the write reaches the object store's log before it is acknowledged.
     *
     * <p>This is the ordering that closes M10's window. Appending first means a crash between the append
     * and the apply replays a write the caller was never told about — harmless, because replay is
     * idempotent — while the reverse order would acknowledge a write that no successor could recover.
     * Cheap to get backwards and expensive to notice.
     *
     * @param shardId the shard to write to
     * @param id the document id
     * @param source the document source
     * @throws java.io.IOException if the shard is not held here, or the write fails
     */
    public void index(org.opensearch.core.index.shard.ShardId shardId, String id, String source) throws java.io.IOException {
        ensureStarted();
        final var shard = reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot index into " + shardId + ": not open on " + nodeName);
        }
        if (reconciler.readerShards().contains(shardId)) {
            throw new java.io.IOException("cannot index into " + shardId + ": it is open as a reader");
        }
        final var wal = reconciler.wal(shardId);
        if (wal != null) {
            wal.append(shard.getOperationPrimaryTerm(), new org.opensearch.serverless.store.WalRecord(id, source));
        }
        final var result = shard.applyIndexOperationOnPrimary(
            org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
            org.opensearch.index.VersionType.INTERNAL,
            new org.opensearch.index.mapper.SourceToParse(
                shardId.getIndexName(),
                id,
                new org.opensearch.core.common.bytes.BytesArray(source),
                org.opensearch.common.xcontent.XContentType.JSON
            ),
            org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
            0,
            org.opensearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false
        );
        if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
            // s0-findings.md F5: this is a value, not a throw. A caller that ignores it indexes nothing
            // and reports no error.
            throw new java.io.IOException("indexing " + id + " returned " + result.getResultType());
        }
        shard.sync();
    }

    /**
     * Publishes a shard's current commit to the object store, fenced by the owning term.
     *
     * @param shardId the shard to publish
     * @param term the term this node owns the shard at
     * @return the manifest published
     * @throws java.io.IOException if the shard is not held here, or a newer term already published
     */
    public org.opensearch.serverless.store.CommitManifest publishShard(org.opensearch.core.index.shard.ShardId shardId, long term)
        throws java.io.IOException {
        ensureStarted();
        return reconciler.publish(shardId, term);
    }

    /**
     * Returns the roles this node advertises.
     *
     * @return the roles
     */
    public Set<String> roles() {
        return roles;
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

    /**
     * Remembers the plane this node is serving.
     *
     * <p>The block-cache directory factory is handed a location and settings, not a metadata plane, so
     * it has to reach one through the node. A node that had only ever been given a plane as a method
     * argument would build local-only directories and silently serve nothing — which is how this was
     * found.
     */
    private void adopt(org.opensearch.serverless.metadata.MetadataPlane plane) {
        if (metadataPlane == null) {
            metadataPlane = plane;
        }
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
