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
    private final ServerlessPlugins plugins;
    private volatile org.opensearch.identity.IdentityService identityService;
    private final ThreadPool threadPool;
    private final NodeEnvironment nodeEnvironment;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final SearchService searchService;
    private final PluginsService pluginsService;
    private org.opensearch.serverless.rest.SystemIndices systemIndices;
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
    private volatile org.opensearch.serverless.reconcile.ReconcileSignals signals =
        org.opensearch.serverless.reconcile.ReconcileSignals.NONE;
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
        this(settings, java.util.List.of());
    }

    /**
     * Creates a node running the given plugins.
     *
     * <p>Plugin instances rather than a directory scan, because that is the shape a test and a daemon can
     * both use: {@code ServerlessBootstrap} can load from disk and hand the instances here, and a test can
     * pass one it wrote. Loading is not this constructor's business.
     *
     * @param settings the node settings
     * @param plugins the plugins to run
     * @throws Exception if the node cannot be built
     */
    public ServerlessNode(Settings settings, java.util.List<org.opensearch.plugins.Plugin> plugins) throws Exception {
        // Plugins first, before the settings are settled, because a plugin gets to contribute to them.
        //
        // The order is load-bearing. PluginsService is built from what the caller supplied, so a plugin is
        // constructed with the operator's configuration; updatedSettings then layers each plugin's
        // additionalSettings underneath it, so an explicit setting always beats a plugin's; and the shell's
        // own defaults go underneath both, so they apply only when nobody else had an opinion. Putting the
        // shell's transport choice on top -- which is where it used to be -- would have made
        // additionalSettings decorative for the one thing plugins most use it for.
        this.pluginsService = buildPluginsService(settings, new Environment(settings, null));
        final java.util.List<org.opensearch.plugins.Plugin> installed = new java.util.ArrayList<>(
            pluginsService.filterPlugins(org.opensearch.plugins.Plugin.class)
        );
        // Ones handed to this constructor come after the installed ones, so an operator's installation is
        // not silently outranked by something a test or an embedder passed in.
        installed.addAll(plugins);
        this.plugins = new ServerlessPlugins(installed);
        this.settings = withShellDefaults(this.plugins.settingsWith(settings));
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
        // Rebuilt from the settled settings, so anything a plugin contributed is visible to everything
        // below.
        final Environment environment = new Environment(settings, null);
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        this.threadPool = new ThreadPool(settings);
        boolean success = false;
        NodeEnvironment openedEnvironment = null;
        try {
            // Held in a local as well, because the failure path below cannot read a blank final -- and it
            // has to close this one thing above all others: NodeEnvironment holds node.lock.
            openedEnvironment = new NodeEnvironment(settings, environment);
            this.nodeEnvironment = openedEnvironment;
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

            // The client every REST handler is handed, wired to the shell's own operations.
            //
            // It used to be a bare NodeClient with no action registry, which the shell's own handlers
            // could ignore because they reach the node directly. A plugin's handler cannot: getRestHandlers
            // gives it this and nothing else, and every REST handler in the OpenSearch ecosystem is written
            // against it. Left unwired it would have thrown on the first call a real plugin made -- found
            // by writing the shell's authentication as a plugin, whose user-management endpoint needs to
            // write to an index from inside a handler.
            //
            // Overriding doExecute rather than calling initialize(...) is the point: initialize wants a map
            // of ActionType to TransportAction, which is the action layer S6.3 decided not to build. This
            // delegates to the same allowlist ServerlessClient enforces, so a plugin's handler and a
            // plugin's component get identical answers, including identical refusals.
            this.nodeClient = new NodeClient(settings, threadPool) {
                @Override
                public <
                    Request extends org.opensearch.action.ActionRequest,
                    Response extends org.opensearch.core.action.ActionResponse> void doExecute(
                        org.opensearch.action.ActionType<Response> action,
                        Request request,
                        org.opensearch.core.action.ActionListener<Response> listener
                    ) {
                    client().execute(action, request, listener);
                }
            };
            // Before the controller, because every handler it registers is wrapped in this.
            this.systemIndices = new org.opensearch.serverless.rest.SystemIndices(this.plugins.systemIndexPatterns(settings));
            this.restController = buildRestController(clusterSettings);
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
                // The node environment too, not just the thread pool. It was missing, and a constructor
                // that threw after taking node.lock leaked the file handle for the life of the process --
                // which on a real node means the data directory stays locked and a restart cannot use it.
                // Found by a test that makes construction fail on purpose; nothing before it did.
                IOUtils.closeWhileHandlingException(openedEnvironment);
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

    /**
     * Loads the plugins installed on this node, using core's own loader.
     *
     * <p><b>Core's {@code PluginsService} rather than a loader of our own</b>, which is the whole point.
     * Reading a {@code plugin-descriptor.properties}, checking that a plugin's declared OpenSearch version
     * matches this one, building a classloader over its jars, refusing a jar that collides with the
     * server's, ordering {@code extended.plugins} before their dependents — all of that is behaviour a
     * plugin author has already been tested against, and a second implementation of it would differ in
     * small ways that only show up on somebody's cluster. The shell was constructing this class with an
     * empty list purely because {@code IndicesService} demands one; giving it the real arguments is the
     * whole of loading a plugin from disk.
     *
     * <p><b>Modules are deliberately not loaded.</b> A classic node loads {@code modules/} the same way it
     * loads {@code plugins/}, and most of what is in there is machinery this shell replaced — transport is
     * chosen directly, and the rest assumes a cluster with a manager. An operator installs a plugin; nobody
     * installs a module.
     *
     * @param settings the node settings
     * @param environment the node environment, which knows where {@code plugins/} is
     * @return the loader, holding whatever was installed
     */
    private static PluginsService buildPluginsService(Settings settings, Environment environment) {
        final java.nio.file.Path pluginsDir = environment.pluginsDir();
        return new PluginsService(
            settings,
            environment.configDir(),
            null,                                        // modules: see above
            java.nio.file.Files.isDirectory(pluginsDir) ? pluginsDir : null,
            classpathPlugins(settings)
        );
    }

    /**
     * Describes the plugins named by {@code serverless.plugins}, which are already on the classpath.
     *
     * <p>The second way in, and a smaller one: no descriptor, no classloader, no jars — the class is simply
     * there. It exists because a distribution may ship a plugin rather than have it installed, and because
     * turning one on should not require editing the shell.
     *
     * <p><b>A name that does not resolve stops the node here, rather than in core.</b> Core's classpath path
     * logs the failure and carries on, which is right for its own tests and wrong for this: the thing most
     * likely to be listed in that setting is the thing enforcing authentication, and a node that started
     * without it and said so only in a log line is a node serving unguarded.
     *
     * @param settings the node settings
     * @return a descriptor per named class
     */
    private static java.util.Collection<org.opensearch.plugins.PluginInfo> classpathPlugins(Settings settings) {
        final java.util.List<org.opensearch.plugins.PluginInfo> described = new java.util.ArrayList<>();
        for (String name : settings.getAsList(ServerlessBootstrap.PLUGINS)) {
            final String className = name.trim();
            if (className.isEmpty()) {
                continue;
            }
            try {
                final Class<?> type = ServerlessNode.class.getClassLoader().loadClass(className);
                if (org.opensearch.plugins.Plugin.class.isAssignableFrom(type) == false) {
                    throw new IllegalArgumentException(
                        "[" + ServerlessBootstrap.PLUGINS + "] names [" + className + "], which is not an OpenSearch plugin"
                    );
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(
                    "[" + ServerlessBootstrap.PLUGINS + "] names [" + className + "], which is not on this node's classpath",
                    e
                );
            }
            described.add(
                new org.opensearch.plugins.PluginInfo(
                    className,
                    "named by " + ServerlessBootstrap.PLUGINS,
                    Version.CURRENT.toString(),
                    Version.CURRENT,
                    System.getProperty("java.specification.version"),
                    className,
                    null,
                    java.util.List.of(),
                    false
                )
            );
        }
        return described;
    }

    private IndicesService buildIndicesService(Environment environment, ClusterSettings clusterSettings) throws Exception {
        final NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Collections.emptyList());
        // Core's AnalysisModule rather than a hand-built empty registry.
        //
        // Loading a plugin and then ignoring what it declares is not hosting it. An AnalysisPlugin's whole
        // contribution is its tokenizers, filters and analyzers, and they arrive through exactly this
        // constructor; passing an empty list meant an installed analysis plugin loaded, logged, and did
        // nothing. The empty registry also had a second cost nobody had noticed: it left out core's own
        // built-in analyzers, which AnalysisModule registers as a matter of course.
        final AnalysisRegistry analysisRegistry = new org.opensearch.indices.analysis.AnalysisModule(
            environment,
            this.plugins.filter(org.opensearch.plugins.AnalysisPlugin.class)
        ).getAnalysisRegistry();
        // S0/F2: mandatory — DataFormatRegistry calls filterPlugins() during construction. It is now the
        // real one rather than an empty stub, so an installed plugin gets onIndexModule like it would on a
        // classic node.
        return new IndicesService(
            settings,
            pluginsService,
            nodeEnvironment,
            xContentRegistry,
            analysisRegistry,
            new IndexNameExpressionResolver(new ThreadContext(settings)),
            new IndicesModule(this.plugins.filter(org.opensearch.plugins.MapperPlugin.class)).getMapperRegistry(),
            new NamedWriteableRegistry(Collections.emptyList()),
            threadPool,
            indexScopedSettings(),
            circuitBreakerService(),
            bigArrays(),
            scriptService(),
            clusterService,
            null,                                   // Client — no node client in phase 1
            new MetaStateService(nodeEnvironment, xContentRegistry),
            engineFactoryProviders(),
            directoryFactories(),
            // Aggregations resolve a field's values source through this. It was null, which was invisible
            // for as long as aggregations were refused at the REST layer and became a NullPointerException
            // from inside QueryShardContext the moment one reached a shard.
            searchModule().getValuesSourceRegistry(),
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
            // Core's own writer engine, with one thing added: it knows this shard's history may continue
            // in an object-store log rather than a local translog, and replays it during recovery so the
            // sequence numbers survive. See ServerlessWriterEngine for why replay cannot happen later.
            final org.opensearch.index.engine.EngineFactory writer = config -> new org.opensearch.serverless.engine.ServerlessWriterEngine(
                config,
                shardId -> reconciler == null ? java.util.List.of() : reconciler.replayRecordsFor(shardId)
            );
            return java.util.List.of((indexSettings, routing) -> java.util.Optional.of(writer));
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
    /**
     * Where a shard's bytes live, which is not always where its index identity says.
     *
     * <p>A frozen view is opened as an index of its own, because a node cannot hold two shards with the
     * same identity and it may well already be serving the live one. That synthetic identity has a uuid
     * nothing ever wrote under, so the path has to come from the index it is a view <em>of</em> — carried
     * as a setting on the shard the reader opens.
     *
     * <p>Absent for every ordinary shard, which is the overwhelming majority, and then this is the index's
     * own uuid exactly as before.
     */
    static String storageUuidOf(org.opensearch.index.IndexSettings indexSettings) {
        final String source = indexSettings.getSettings().get(STORAGE_UUID_SETTING);
        return source == null ? indexSettings.getIndex().getUUID() : source;
    }

    /**
     * Core's index settings plus the shell's own.
     *
     * <p>Core validates every index setting against a registry and refuses one it has never heard of, which
     * is right — a typo in a setting name that was silently accepted would be a setting that silently did
     * nothing. The shell has exactly one of its own, so it registers it rather than turning the validation
     * off.
     *
     * <p>Private, because nothing outside this node should be setting it: it names where a shard reads its
     * bytes, and a caller who could set it could point one index's shard at another's data.
     *
     * @return the settings a shard is validated against
     */
    private IndexScopedSettings indexScopedSettings() {
        final java.util.Set<org.opensearch.common.settings.Setting<?>> known = new java.util.HashSet<>(
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
        );
        known.add(
            org.opensearch.common.settings.Setting.simpleString(
                STORAGE_UUID_SETTING,
                org.opensearch.common.settings.Setting.Property.IndexScope,
                org.opensearch.common.settings.Setting.Property.PrivateIndex
            )
        );
        return new IndexScopedSettings(settings, known);
    }

    /** Names the index whose storage a shard should read, when that is not the index it belongs to. */
    public static final String STORAGE_UUID_SETTING = "index.serverless.storage_uuid";

    /**
     * The frozen view a shard is serving, if it is serving one.
     *
     * <p>A view is opened as an index of its own whose uuid is the view's identifier, and it is the only
     * kind of shard that carries {@link #STORAGE_UUID_SETTING} — so the presence of that setting is what
     * distinguishes one, and its own uuid is the identifier.
     */
    static String frozenViewIdOf(org.opensearch.index.IndexSettings indexSettings) {
        return indexSettings.getSettings().get(STORAGE_UUID_SETTING) == null ? null : indexSettings.getIndex().getUUID();
    }

    /**
     * The secure-transport providers the plugins supply.
     *
     * <p><b>This was an empty list, and that made TLS through a plugin impossible rather than merely
     * undemonstrated.</b> Core's netty4 module already ships a TLS transport; what it needs is a
     * {@link org.opensearch.plugins.SecureTransportSettingsProvider} to get an {@code SSLEngine} from, and
     * the only way a plugin can supply one is through this collection. Passing nothing meant
     * {@code getSecureTransports} was never called, so naming the secure transport in settings produced
     * "unsupported transport type" — a plugin doing everything right and being told its transport does not
     * exist.
     *
     * <p>Core refuses more than one provider, which is the right rule and not this class's to soften: two
     * plugins each believing they configure the node's TLS is a deployment nobody can reason about.
     *
     * @return the factories, in plugin order
     */
    private java.util.List<org.opensearch.plugins.SecureSettingsFactory> secureSettingsFactories() {
        final java.util.List<org.opensearch.plugins.SecureSettingsFactory> factories = new java.util.ArrayList<>();
        for (org.opensearch.plugins.Plugin plugin : plugins.plugins()) {
            plugin.getSecureSettingFactory(settings).ifPresent(factories::add);
        }
        return factories;
    }

    private Map<String, org.opensearch.plugins.IndexStorePlugin.DirectoryFactory> directoryFactories() {
        final org.opensearch.plugins.IndexStorePlugin.DirectoryFactory factory =
            new org.opensearch.plugins.IndexStorePlugin.DirectoryFactory() {
                @Override
                public org.apache.lucene.store.Directory newDirectory(
                    org.opensearch.index.IndexSettings indexSettings,
                    org.opensearch.index.shard.ShardPath shardPath
                ) throws java.io.IOException {
                    return open(
                        org.apache.lucene.store.FSDirectory.open(shardPath.resolveIndex()),
                        indexSettings,
                        shardPath.getShardId().id()
                    );
                }

                @Override
                public org.apache.lucene.store.Directory newFSDirectory(
                    java.nio.file.Path location,
                    org.apache.lucene.store.LockFactory lockFactory,
                    org.opensearch.index.IndexSettings indexSettings
                ) throws java.io.IOException {
                    // This hands over a location rather than a ShardPath, so the shard number has to be
                    // recovered from the directory the location sits in
                    // ({data}/indices/{uuid}/{shard}/index).
                    final org.apache.lucene.store.Directory local = org.apache.lucene.store.FSDirectory.open(location, lockFactory);
                    final int shardNumber;
                    try {
                        shardNumber = Integer.parseInt(location.getParent().getFileName().toString());
                    } catch (RuntimeException e) {
                        // An unexpected layout means we cannot say which shard this is, and guessing would
                        // attach one shard's directory to another's data. Local-only is the safe answer.
                        logger.warn("could not derive a shard number from " + location + "; serving it from local disk only", e);
                        return local;
                    }
                    return open(local, indexSettings, shardNumber);
                }
            };
        return Map.of(BLOCK_CACHE_STORE_TYPE, factory);
    }

    /**
     * Builds one shard's directory, whichever way the store asked for it.
     *
     * <p><b>Both entry points come here, and that is the point.</b> They were written separately, and the
     * one core happened to call for a live shard grew the logic while the other kept deriving the storage
     * path from the shard's own uuid. That is right for every ordinary shard and silently wrong for a
     * frozen view, whose uuid is the view's and under which nothing was ever written — the view opened
     * onto an empty directory and reported an index with no segments, which reads like a corrupted shard
     * rather than a shell bug. Whatever decides which method core calls, it cannot decide correctness.
     *
     * @param local the shard's local directory
     * @param indexSettings the shard's settings, which say whose bytes it reads
     * @param shardNumber the shard
     * @return the directory to serve it from
     * @throws java.io.IOException if the manifest or its term containers cannot be read
     */
    private org.apache.lucene.store.Directory open(
        org.apache.lucene.store.Directory local,
        org.opensearch.index.IndexSettings indexSettings,
        int shardNumber
    ) throws java.io.IOException {
        final var plane = metadataPlane;
        if (plane == null) {
            // No metadata plane means no manifest to read; a plain local directory is the truthful
            // fallback rather than a half-built remote one.
            return local;
        }
        final String indexName = indexSettings.getIndex().getName();
        final var shardBase = org.opensearch.serverless.metadata.RegisterMap.shardData(
            plane.basePath(),
            indexName,
            storageUuidOf(indexSettings),
            shardNumber
        );
        // A frozen view opens the commit it froze, which is not the one the manifest register holds now --
        // that is the entire point of it. The view's identifier is this index's uuid, so the record is one
        // lookup away.
        final String viewId = frozenViewIdOf(indexSettings);
        if (viewId != null) {
            final var pit = plane.pointInTime(viewId);
            if (pit.isEmpty()) {
                // Released or expired between the search resolving it and the shard opening. An empty
                // directory here would look like an empty index; refusing says what happened.
                throw new java.io.IOException("the point in time [" + viewId + "] is no longer held");
            }
            return org.opensearch.serverless.store.BlockCacheDirectory.create(
                local,
                plane.blobStore(),
                shardBase,
                blockCache,
                viewId + "#" + shardNumber,
                pit.get().shards().get(shardNumber)
            );
        }
        return org.opensearch.serverless.store.BlockCacheDirectory.create(
            local,
            plane.blobStore(),
            shardBase,
            blockCache,
            indexName + "#" + shardNumber
        );
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
            scriptService(),
            // The node's allocator, not one built here with a null breaker service. An aggregator asks its
            // search context's BigArrays for the breaker and calls getBreaker on it before it has counted a
            // single document, so a null one is a NullPointerException at the top of every aggregation.
            bigArrays(),
            new QueryPhase(),
            // Built by SearchModule, not by hand. A FetchPhase with no sub-phases returns hits with an
            // id and no _source, and reports success -- the fetch sub-phases are what load the source,
            // highlight, and so on. Constructing it empty looked harmless and silently dropped every
            // document body.
            searchModule().getFetchPhase(),
            new org.opensearch.node.ResponseCollectorService(clusterService),
            circuitBreakerService(),
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
    private RestController buildRestController(ClusterSettings clusterSettings) {
        final RestController controller = new RestController(
            Set.of(),
            // The seam a plugin authenticates on: it sees every request before the handler does, can
            // refuse it, and can leave an identity in the ThreadContext. Passed here because the
            // controller takes it at construction, which is why plugins are held before this runs.
            plugins.restHandlerWrapper(threadPool.getThreadContext(), Set.of()),
            nodeClient,
            circuitBreakerService(),
            new UsageService()
        );
        // Every shell handler is registered behind the system-index guard, rather than each one checking:
        // a guard a handler has to remember to call is a guard that one handler will not call.
        final java.util.function.UnaryOperator<org.opensearch.rest.RestHandler> guarded =
            handler -> org.opensearch.serverless.rest.SystemIndices.guard(handler, systemIndices);
        controller.registerHandler(
            guarded.apply(
                new ServerlessRootHandler(nodeName, ClusterName.CLUSTER_NAME_SETTING.get(settings).value(), nodeEnvironment::nodeId)
            )
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.IndexAdminHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.DocumentHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.BulkHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.GetHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.MultiGetHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.UpdateHandler(() -> this, () -> metadataPlane)));
        // Plugin routes last, so a plugin cannot take over an endpoint the shell has already claimed:
        // RestController refuses a duplicate path rather than replacing it, and refusing loudly at boot
        // is the right answer -- a plugin that silently replaced the write path would be a system whose
        // behaviour depends on registration order.
        for (org.opensearch.rest.RestHandler handler : plugins.restHandlers(settings, controller, clusterSettings)) {
            controller.registerHandler(handler);
        }
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.SearchHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.CatalogHandler(() -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.StatsHandler(() -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.AliasHandler(() -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.PointInTimeHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.DeleteByQueryHandler(() -> this, () -> metadataPlane)));
        // Snapshot/restore, backed by this deployment's own object store rather than a distinct
        // repository backend -- R7's "out of scope" closed, not worked around. See m45-snapshot-notes.md.
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.RepositoryHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.SnapshotHandler(() -> metadataPlane, () -> this)));
        // The one piece of /_cluster/* not refused below: §9.3's /cluster/config is one register, exactly
        // like an index descriptor, and this node can answer for it the same honest way it answers for an
        // index -- unlike health, state or stats, which are genuinely cluster-wide and would mislead
        // coming from one node.
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.ClusterSettingsHandler(() -> metadataPlane, () -> this))
        );
        // Cluster-shaped endpoints this deployment can answer honestly. See ClusterHealthHandler for what
        // green is redefined to mean and why, and NodesHandler for why "no node can answer this" stopped
        // being true the moment leases became an address book.
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.ClusterHealthHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.NodesHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.MappingUpdateHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.FieldCapabilitiesHandler(() -> metadataPlane, () -> this))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.ListIndicesHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.SettingsUpdateHandler(() -> metadataPlane, () -> this))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.AnalyzeHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.TemplateHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.PipelineHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.MultiSearchHandler(() -> metadataPlane, () -> this)));
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
        // What is left after the answerable ones moved out. Each of these needs a snapshot across every
        // index, or an allocator, or a cluster-manager task queue -- three things this design does not have
        // and is not going to grow. The endpoints that only needed the lease registry or one index's shard
        // heads are served instead, above.
        for (String path : new String[] { "/_cluster/state", "/_cluster/stats", "/_cat" }) {
            controller.registerHandler(new NotImplementedHandler(path, noGlobalState));
        }

        // Refusals that used to share the "no cluster-wide state" reason and are not actually refused for
        // it. A caller reading that a listing is unavailable because there is no cluster state would
        // reasonably conclude the data does not exist -- when in fact it exists, is readable one index at a
        // time, and is refused because reading all of it at once is unbounded. Three of these are refused
        // because there is no allocator, which is a different thing again. This is the same stale-reason
        // shape M50 found in _bulk and M51 found in the node endpoints, in its third place.
        final String noClosedState = "an index here is being served or it does not exist: there is no closed state "
            + "for it to be opened from or closed into";
        final String noClusterManager = "there is no cluster manager, so there is no node to name as one";
        final String reshaping = "reshaping an index means rewriting every document into a different shard count, "
            + "and there is no _reindex here to do it with. An index that needs a different shard count is a new "
            + "index, written by the caller";
        final String simulate = "simulating template resolution is not routed; create the index and read back "
            + "GET /{index}, which reports exactly what it inherited";
        final String indexEnumeration = "aggregating across every index in the deployment is unbounded, and this "
            + "design refuses an answer it would have to truncate";
        final String noAllocator = "there is no allocator here: a shard is activated by the write that needs it, "
            + "not by a placement decision, so there is nothing to explain, retry or rebalance";
        final String enumeration = "listing every index in the deployment is unbounded, and this design refuses "
            + "an answer it would have to truncate. Use GET /_list/indices/{prefix}*, which resolves through "
            + "one bounded listing and refuses a prefix that matches more than the cap rather than cutting it "
            + "off -- OpenSearch added _list for this reason and left _cat alone";
        for (String[] refusal : new String[][] {
            { "/_cat/indices", enumeration },
            {
                "/_cat/shards",
                "the same unbounded listing as _cat/indices, one row per shard. GET "
                    + "/_cluster/health/{index}?level=shards reports one index's shards, bounded" },
            { "/_cat/aliases", "aliases are found by name here, not enumerated; GET /_alias/{name} answers for one" },
            {
                "/_cat/count",
                "counting every document in the deployment means visiting every index; GET "
                    + "/{index}/_count answers for one, and takes a prefix pattern" },
            {
                "/_cat/segments",
                "per-shard Lucene detail across every index, which is both unbounded and not " + "exposed for a single index either" },
            { "/_cluster/reroute", noAllocator },
            { "/_cluster/allocation/explain", noAllocator },
            { "/_cat/allocation", noAllocator },
            { "/_cat/recovery", noAllocator },
            { "/_cat/pending_tasks", "there is no cluster manager and no task queue, so there is nothing pending" },
            {
                "/_cat/thread_pool",
                "this reports per-node thread pools, and answering for the fleet needs the "
                    + "same fan-out /_nodes/stats is waiting on; GET /_serverless/stats answers for the node it is "
                    + "sent to" },
            { "/_list/indices", enumeration },
            { "/_list/shards", "the same unbounded listing as _cat/shards" } }) {
            controller.registerHandler(new NotImplementedHandler(refusal[0], refusal[1]));
        }

        // Refused for a reason of its own, because the general one is no longer true of it. A node can now
        // reach every other node -- that is what the lease registry is for -- so "no node can answer this"
        // would be wrong. What is missing is narrower and worth saying precisely.
        controller.registerHandler(
            new NotImplementedHandler(
                "/_nodes/stats",
                "each node can answer for itself at GET /_serverless/stats, and this node knows where the "
                    + "others are; what does not exist yet is the fan-out that would ask them and the "
                    + "accounting that would report which ones did not answer. Until that is built, asking "
                    + "one node for the fleet's statistics would return one node's statistics"
            )
        );
        controller.registerHandler(
            new NotImplementedHandler(
                "/_tasks",
                "there is no cluster-wide task registry: work here belongs to the node doing it and does not "
                    + "outlive it, so there is no task to look up by id from anywhere else"
            )
        );

        // D2 said unimplemented endpoints return 501 with a reason. Ten paths did; everything else a normal
        // client reaches for fell through to core's default handler and came back as
        // 400 {"error":"no handler found for uri ..."} -- a bare string, the wrong status, and
        // indistinguishable from a typo. These are the endpoints a client actually tries, each refused with
        // the reason it is refused for rather than with a generic one, because "not here" and "not here
        // *because*" are different answers and only the second is useful.
        final String noReindex = "bulk reindexing runs for hours and needs a task that survives the node "
            + "that started it; there is no cluster-wide task registry here. Read with search_after and "
            + "write with _bulk, which is the same work under the caller's own control";
        for (String[] refusal : new String[][] {
            {
                "/{index}/_refresh",
                "refresh is per-write here: pass refresh=true on the write that must be visible. A "
                    + "deployment-wide refresh would have to reach every node holding a shard of this index" },
            {
                "/{index}/_flush",
                "there is no flush to ask for: durability is the write-ahead log, which a write is in "
                    + "before it is acknowledged, and publication is the background reconciler's job" },
            {
                "/{index}/_forcemerge",
                "merging is the shard writer's own decision; there is no cluster-wide operation to trigger "
                    + "it, and forcing one on a shard this node may not own is not something this node can do" },
            {
                "/{index}/_explain/{id}",
                "explaining a score needs the scorer for one document on one shard; the fan-out here merges "
                    + "hits rather than exposing per-shard scoring internals" },
            { "/{index}/_termvectors/{id}", "term vectors are a per-shard Lucene detail this surface does not expose" },
            { "/_reindex", noReindex },
            { "/{index}/_update_by_query", noReindex },
            {
                "/_search/scroll",
                "scroll holds a search context open on a node; use a point in time (POST /{index}/_pit) with "
                    + "search_after, which is held in the object store and is not tied to one node" },
            {
                "/_scripts/{id}",
                // The old reason stopped being true the moment an engine was registered. Painless runs here
                // now; what a stored script needs is somewhere for ScriptService to look it up, and that is
                // cluster state. AWS OpenSearch Serverless refuses stored scripts for the same practical
                // reason and supports inline ones, which is where this lands too.
                "stored scripts resolve through cluster state -- ScriptService is a ClusterStateApplier and "
                    + "reads them from the cluster metadata -- and there is no cluster state here. Inline "
                    + "scripts work: send the source in the query, the aggregation or the update" },
            { "/_ilm/policy/{name}", "index lifecycle management is not part of this surface" },
            // Found by driving a running node rather than by reading this list: twenty-two endpoints a real
            // client reaches for were answering core's default 400 "no handler found for uri", which reads
            // as a typo. Each has a reason under one of the four the rest of this list already gives -- no
            // allocator, no cluster-wide state, no modules, no closed index -- and none of them said so.
            { "/_alias", "aliases are found by name here, not enumerated; GET /_alias/{name} answers for one" },
            { "/_stats", indexEnumeration },
            {
                "/{index}/_stats",
                "per-shard statistics are not exposed; GET /_serverless/stats answers for the node "
                    + "it is sent to, and GET /_cluster/health/{index}?level=shards reports one index's shards" },
            { "/{index}/_segments", "per-shard Lucene detail is not exposed" },
            { "/{index}/_recovery", noAllocator },
            { "/{index}/_shard_stores", noAllocator },
            {
                "/{index}/_cache/clear",
                "there is no cluster-wide operation to clear a cache, and clearing one on a "
                    + "shard this node may not own is not something this node can do" },
            { "/{index}/_open", noClosedState },
            { "/{index}/_close", noClosedState },
            {
                "/{index}/_rollover",
                "rollover creates the next index and moves an alias to it atomically; the alias "
                    + "move is the part this design cannot do, since each alias is its own compare-and-swap" },
            { "/{index}/_shrink/{target}", reshaping },
            { "/{index}/_split/{target}", reshaping },
            { "/{index}/_clone/{target}", reshaping },
            { "/_cluster/pending_tasks", "there is no cluster manager and no task queue, so there is nothing pending" },
            { "/_cat/master", noClusterManager },
            { "/_cat/cluster_manager", noClusterManager },
            {
                "/_cat/plugins",
                "plugins are per node; GET /_nodes lists the fleet and GET /_serverless/stats answers " + "for the node it is sent to" },
            {
                "/_cat/nodeattrs",
                "node attributes are not part of this deployment's node model; GET /_nodes reports " + "the roles a node does have" },
            {
                "/_cat/repositories",
                "a repository here is a namespace in this deployment's own object store; " + "GET /_snapshot lists them" },
            { "/_cat/snapshots", "GET /_snapshot/{repo} lists a repository's snapshots" },
            { "/_cat/tasks", "there is no cluster-wide task registry" },
            { "/_cat/fielddata", "fielddata is per-shard memory this surface does not report" },
            { "/_remote/info", "there are no remote clusters: cross-cluster search and replication are not part of " + "this design" },
            {
                "/_dangling",
                "a dangling index is one whose data outlived the cluster state naming it. There is no "
                    + "cluster state here, so an index exists exactly when its descriptor does" },
            {
                "/_scripts/painless/_execute",
                "the script-execution sandbox endpoint is not routed; inline scripts run " + "in a query, an aggregation or an update" },
            { "/_index_template/_simulate_index/{name}", simulate },
            { "/_index_template/_simulate/{name}", simulate },
            { "/_component_template/_simulate/{name}", simulate },
            // Found by comparing this surface against AWS OpenSearch Serverless and Elastic Cloud
            // Serverless: every one of these is an endpoint a real client reaches for, and every one of them
            // fell through to core's default 400 rather than the 501 D2 promises. Being absent is a
            // decision; looking like a typo is not.
            {
                "/{index}/_validate/query",
                "query validation is not implemented here; send the query to _search, which parses it the "
                    + "same way and reports a parse failure as a 400" },
            {
                "/_resolve/index/{name}",
                "index and alias resolution is not exposed; look an index up by name with GET /{index}, or "
                    + "use a prefix pattern on _search, which resolves with one bounded listing" },
            { "/{index}/_rank_eval", "ranking evaluation is not implemented here" },
            {
                "/_field_caps",
                "name an index or a prefix: GET /{index}/_field_caps answers for what it can reach, and "
                    + "asking every index in the deployment what fields it has is the inventory operation "
                    + "this design refuses" },
            {
                "/_aliases",
                "the bulk alias action API is not implemented; this shell has PUT, GET and DELETE on "
                    + "/_alias/{name}, which covers creating and removing an alias but not atomic swaps" },
            {
                "/{index}/_alias/{name}",
                "aliases are managed by name rather than per index here: use PUT /_alias/{name} with the " + "indices in the body" },
            { "/_search/pipeline/{id}", "search pipelines are not implemented here" },
            {
                "/_cat/templates",
                "listing templates as a table is not routed; GET /_index_template returns them all, and "
                    + "GET /_index_template/{prefix}* narrows by name" } }) {
            controller.registerHandler(new NotImplementedHandler(refusal[0], refusal[1]));
        }
        return controller;
    }

    /**
     * The shell picks netty4 directly rather than discovering a transport through the plugin system.
     * There is exactly one supported transport, and pretending otherwise would add a lookup with one
     * possible answer.
     */
    private NetworkModule buildNetworkModule(ClusterSettings clusterSettings) {
        // Netty first, then whatever plugins were installed.
        //
        // Core's NetworkModule resolves transport.type and http.type by name across every NetworkPlugin it
        // is given, and composes their transport interceptors. Handing it only Netty meant an installed
        // NetworkPlugin was loaded and then not asked -- which for the OpenSearch security plugin is the
        // difference between installing its TLS transport and not. The shell still supplies netty4 as the
        // default, so a node with no such plugin is unchanged.
        final java.util.List<org.opensearch.plugins.NetworkPlugin> networkPlugins = new java.util.ArrayList<>();
        networkPlugins.add(new Netty4ModulePlugin());
        networkPlugins.addAll(plugins.filter(org.opensearch.plugins.NetworkPlugin.class));
        return new NetworkModule(
            settings,
            networkPlugins,
            threadPool,
            bigArrays(),
            new PageCacheRecycler(settings),
            circuitBreakerService(),
            // Not empty, and it took a forwarded query to find out why. A search body crossing the
            // network is a SearchSourceBuilder, and its query serializes as a named writeable -- so an
            // empty registry reads it back as "Unknown NamedWriteable category [QueryBuilder]" on the
            // receiving node. Local shards answered fine and remote ones silently dropped out of the
            // coverage count, which is the shape of failure this design reports rather than hides.
            new NamedWriteableRegistry(searchModule().getNamedWriteables()),
            new NamedXContentRegistry(Collections.emptyList()),
            new NetworkService(Collections.emptyList()),
            restController,
            clusterSettings,
            NoopTracer.INSTANCE,
            Collections.emptyList(),
            secureSettingsFactories()
        );
    }

    /**
     * Registers who hears about writes and about ownership going wrong.
     *
     * <p>Optional. A node with no signals set still converges through the backstop pass; it just does so
     * on the clock's schedule rather than on its own evidence.
     *
     * @param signals the listener, or null to go back to hearing nothing
     */
    public void setSignals(org.opensearch.serverless.reconcile.ReconcileSignals signals) {
        this.signals = signals == null ? org.opensearch.serverless.reconcile.ReconcileSignals.NONE : signals;
    }

    /**
     * Returns the signal sink, so callers on the request path can report what they saw.
     *
     * @return the signals, never null
     */
    public org.opensearch.serverless.reconcile.ReconcileSignals signals() {
        return signals;
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

    /**
     * Returns this deployment's cluster name.
     *
     * <p>A name, and only a name: there is no cluster object behind it. It is reported because every
     * OpenSearch response that carries one carries it, and a client reading it should find what it
     * configured.
     *
     * @return the configured cluster name
     */
    public String clusterName() {
        return org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
    }

    private void renewOwnLease(org.opensearch.serverless.metadata.MetadataPlane plane) throws java.io.IOException {
        plane.membership()
            .renew(
                new org.opensearch.serverless.membership.NodeLease(
                    localNode.getId(),
                    localNode.getEphemeralId(),
                    localNode.getAddress().toString(),
                    roles,
                    0L,
                    localNode.getName(),
                    org.opensearch.Version.CURRENT.toString()
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

        // Last, and after `started`, because a plugin's createComponents may use the client -- which
        // refuses to work on a node that has not started. Everything a plugin can reach now exists.
        try {
            plugins.createComponents(this, new org.opensearch.env.Environment(settings, null));
        } catch (Exception e) {
            started = false;
            throw new IllegalStateException("a plugin failed to start; the node will not serve", e);
        }

        // After createComponents, because a plugin's action almost always takes one of its own components,
        // and before onNodeStarted, because a plugin told the node is up may immediately call its own
        // action.
        pluginActions = buildPluginActions();

        // And then tell them the node is up. A plugin that has to read its own index cannot do it while
        // components are being built; this is the event it waits for.
        plugins.onNodeStarted(localNode);
    }

    private PluginActions pluginActions = PluginActions.none();

    /**
     * Returns the actions the plugins provide.
     *
     * @return the registry, empty on a node whose plugins declared none
     */
    public PluginActions pluginActions() {
        return pluginActions;
    }

    /**
     * Builds the plugins' own transport actions.
     *
     * <p>What a constructor may be given is listed here rather than discovered: the node's own services,
     * and everything the plugins built. The order is most-specific first, so a plugin component that
     * happens to implement one of core's interfaces wins the parameter over the node's own — which is what
     * a plugin means when it declares that parameter.
     *
     * @return the registry
     */
    private PluginActions buildPluginActions() {
        final java.util.List<org.opensearch.plugins.ActionPlugin> actionPlugins = plugins.filter(org.opensearch.plugins.ActionPlugin.class);
        if (actionPlugins.isEmpty()) {
            return PluginActions.none();
        }
        final java.util.List<Object> available = new java.util.ArrayList<>(plugins.components());
        available.add(new org.opensearch.action.support.ActionFilters(new java.util.HashSet<>(plugins.actionFilters())));
        available.add(transportService);
        available.add(transportService.getTaskManager());
        available.add(threadPool);
        available.add(client());
        available.add(clusterService);
        available.add(settings);
        available.add(namedWriteableRegistry());
        available.add(indexNameExpressionResolver());
        available.add(circuitBreakerService());
        available.add(indicesService);
        return PluginActions.build(actionPlugins, available);
    }

    /**
     * The most indices a prefix pattern may match before the request is refused.
     *
     * <p>Configurable because it is a policy about how much fan-out a caller may ask for in one request,
     * not a fact about the store — and because a default of five hundred is unreachable in a test, which
     * would leave the refusal path unexercised.
     *
     * @return the cap
     */
    public int patternCap() {
        return settings.getAsInt(
            "serverless.search.pattern.max_indices",
            org.opensearch.serverless.metadata.MetadataPlane.DEFAULT_PATTERN_CAP
        );
    }

    /**
     * Opens one shard of a frozen view, so a search can read the commit it froze.
     *
     * <p>The view is opened as an index of its own — see
     * {@link org.opensearch.serverless.shard.ShardReconciler#openFrozenReader} for why it has to be — which
     * means a node can serve a frozen view of a shard it is at the same time writing to.
     *
     * @param plane the metadata plane
     * @param pit the frozen view
     * @param shardNumber the shard
     * @return the shard id the view was opened under
     * @throws Exception if the shard cannot be opened
     */
    public org.opensearch.core.index.shard.ShardId openFrozenView(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        org.opensearch.serverless.metadata.PointInTime pit,
        int shardNumber
    ) throws Exception {
        ensureStarted();
        adopt(plane);
        reconciler.setSegmentPublishers(id -> plane.segmentPublisher(id.getIndexName(), id.getIndex().getUUID(), id.id()));
        reconciler.setWalStores(id -> plane.walStore(id.getIndexName(), id.getIndex().getUUID(), id.id()));
        final IndexDescriptor descriptor = plane.describe(pit.index())
            .orElseThrow(() -> new IllegalArgumentException("no such index: " + pit.index()));
        final var manifest = pit.shards().get(shardNumber);
        if (manifest == null) {
            throw new IllegalArgumentException("the point in time [" + pit.id() + "] did not freeze shard " + shardNumber);
        }
        // Every shard's term, not just this one's: the view's IndexService is created once and updated as
        // each shard opens, and metadata that knew only one shard's term would push the others back to
        // zero.
        final java.util.Map<Integer, Long> terms = new java.util.HashMap<>();
        pit.shards().forEach((shard, commit) -> terms.put(shard, commit.term()));
        return reconciler.openFrozenReader(
            descriptor.toIndexMetadata(terms),
            pit.id(),
            shardNumber,
            manifest,
            DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build()
        );
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
        reconciler.setSegmentPublishers(id -> plane.segmentPublisher(id.getIndexName(), id.getIndex().getUUID(), id.id()));
        reconciler.setWalStores(id -> plane.walStore(id.getIndexName(), id.getIndex().getUUID(), id.id()));
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
        {
            // The lease first, and it has to be first. Under batched liveness a shard-head is not
            // self-describing: it names an owner, and whether that ownership is live is a fact stored in
            // the owner's node lease. A node that acquired a shard before publishing a lease would hold a
            // head that every peer reads as dead, and the shard would be stolen out from under it
            // immediately -- with both nodes believing they had won it fairly.
            //
            // The head's own stamp is a floor that covers this window, but only just: publishing the
            // lease first is what makes the invariant hold rather than merely usually hold.
            renewOwnLease(plane);
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
            // The head still needs reading, because losing a shard is something only the head can tell
            // us -- but it is a read, not a write, and that is the whole saving. This used to have an
            // else-branch that renewed each head individually; that mode is gone.
            final var head = plane.heads().read(shardId.getIndexName(), shardId.id());
            if (head.isPresent() && localNode.getId().equals(head.get().ownerNodeId())) {
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
        reconciler.setSegmentPublishers(id -> plane.segmentPublisher(id.getIndexName(), id.getIndex().getUUID(), id.id()));
        reconciler.setWalStores(id -> plane.walStore(id.getIndexName(), id.getIndex().getUUID(), id.id()));
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
     * <p>This is the ordering that closes M10's window — the record is in the log before this method
     * returns, and returning is what acknowledges the write. What changed is <em>where</em> in the method
     * the append happens: it used to come first, and now it comes after the engine has run, because the
     * sequence number the record carries does not exist until then. See {@link #appendOrRelease} for what
     * happens when the append is the thing that fails.
     *
     * @param shardId the shard to write to
     * @param id the document id
     * @param source the document source
     * @return what the engine assigned this write
     * @throws java.io.IOException if the shard is not held here, or the write fails
     */
    public WriteOutcome index(org.opensearch.core.index.shard.ShardId shardId, String id, String source) throws java.io.IOException {
        return index(shardId, id, source, org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO, 0L);
    }

    /**
     * Indexes a document, optionally only if it is still at the sequence number the caller expects.
     *
     * <p><b>The compare-and-swap is the engine's own, not a re-implementation.</b> {@code ifSeqNo} and
     * {@code ifPrimaryTerm} are passed straight through to
     * {@code IndexShard#applyIndexOperationOnPrimary}, where core compares them against the live version
     * map (falling back to a Lucene doc-values lookup) under the per-document lock it already takes. A
     * mismatch is a {@code VersionConflictEngineException}, which this method rethrows so the caller
     * cannot mistake a lost race for a completed write.
     *
     * @param shardId the shard to write to
     * @param id the document id
     * @param source the document source
     * @param ifSeqNo the sequence number the document must currently be at, or
     *     {@link org.opensearch.index.seqno.SequenceNumbers#UNASSIGNED_SEQ_NO} for an unconditional write
     * @param ifPrimaryTerm the primary term the document must currently be at, or 0 when unconditional
     * @return what the engine assigned this write
     * @throws org.opensearch.index.engine.VersionConflictEngineException if the condition was not met
     * @throws java.io.IOException if the shard is not held here, or the write fails
     */
    public WriteOutcome index(org.opensearch.core.index.shard.ShardId shardId, String id, String source, long ifSeqNo, long ifPrimaryTerm)
        throws java.io.IOException {
        return index(shardId, id, source, ifSeqNo, ifPrimaryTerm, false);
    }

    /**
     * Indexes a document, optionally only if no document with that id exists.
     *
     * <p><b>Create is one constant, not a feature.</b> Core's own {@code _create} is an ordinary index
     * operation at {@code Versions.MATCH_DELETED} — the engine compares against its live version map under
     * the per-document lock it already takes, and a document that is present fails the comparison. Nothing
     * here re-implements exists-checking, and in particular nothing does a read first, which would be a
     * race rather than a check.
     *
     * <p>A lost race arrives as a {@code VersionConflictEngineException}, the same type a failed
     * {@code if_seq_no} produces, and reaches the caller as a 409 by the same path.
     *
     * @param shardId the shard to write to
     * @param id the document id
     * @param source the document source
     * @param ifSeqNo the sequence number the document must currently be at, or
     *     {@link org.opensearch.index.seqno.SequenceNumbers#UNASSIGNED_SEQ_NO} for an unconditional write
     * @param ifPrimaryTerm the primary term the document must currently be at, or 0 when unconditional
     * @param requireAbsent whether the write must fail if the document already exists
     * @return what the engine assigned this write
     * @throws org.opensearch.index.engine.VersionConflictEngineException if the condition was not met
     * @throws java.io.IOException if the shard is not held here, or the write fails
     */
    public WriteOutcome index(
        org.opensearch.core.index.shard.ShardId shardId,
        String id,
        String source,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent
    ) throws java.io.IOException {
        return index(shardId, id, source, ifSeqNo, ifPrimaryTerm, requireAbsent, true);
    }

    /**
     * Indexes a document, optionally growing the mapping to fit it.
     *
     * <p><b>Dynamic mapping, which this shell did not have.</b> When a document carries a field the mapping
     * does not describe, core's engine does not index it and does not fail either: it returns
     * {@code MAPPING_UPDATE_REQUIRED} along with the mapping addition that would let the write succeed. A
     * classic node sends that addition to the cluster manager, waits for it to be applied, and retries.
     * There is no cluster manager here, and nothing was doing the equivalent — so an index created without a
     * complete mapping could not accept a single document, and every write of an undeclared field answered
     * with a 500. Every test in this repository happened to declare its mappings, which is why it went unseen.
     *
     * <p>The equivalent here is the one M52 already built: merge the addition into the descriptor under a
     * compare-and-swap, apply it to the open shard, and retry once. Concurrent writers adding different
     * fields both win, because that is what the compare-and-swap is for.
     *
     * @param shardId the shard to write to
     * @param id the document id
     * @param source the document source
     * @param ifSeqNo the sequence number the document must be at, or unassigned
     * @param ifPrimaryTerm the primary term the document must be at, or 0
     * @param requireAbsent whether the write must fail if the document already exists
     * @param mayGrowMapping whether a required mapping update should be applied and the write retried
     * @return what the engine assigned this write
     * @throws java.io.IOException if the shard is not held here, or the write fails
     */
    public WriteOutcome index(
        org.opensearch.core.index.shard.ShardId shardId,
        String id,
        String source,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent,
        boolean mayGrowMapping
    ) throws java.io.IOException {
        ensureStarted();
        final var shard = reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot index into " + shardId + ": not open on " + nodeName);
        }
        if (reconciler.readerShards().contains(shardId)) {
            throw new java.io.IOException("cannot index into " + shardId + ": it is open as a reader");
        }
        markUsed(shardId);
        // Apply first, then log. This is the reverse of what this path used to do, and the reason is the
        // whole point of carrying sequence numbers: the engine assigns one, so there is nothing worth
        // recording until it has run. The durability contract the javadoc above states is unchanged --
        // the record is in the log before the write is acknowledged, and acknowledgement is this method
        // returning. It is also the order classic OpenSearch uses internally: Lucene, then the translog,
        // then fsync.
        final var result = shard.applyIndexOperationOnPrimary(
            requireAbsent ? org.opensearch.common.lucene.uid.Versions.MATCH_DELETED : org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
            org.opensearch.index.VersionType.INTERNAL,
            new org.opensearch.index.mapper.SourceToParse(
                shardId.getIndexName(),
                id,
                new org.opensearch.core.common.bytes.BytesArray(source),
                org.opensearch.common.xcontent.XContentType.JSON
            ),
            ifSeqNo,
            ifPrimaryTerm,
            org.opensearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false
        );
        if (result.getResultType() == org.opensearch.index.engine.Engine.Result.Type.FAILURE) {
            // A conditional write that lost is a FAILURE carrying a VersionConflictEngineException rather
            // than a thrown one -- s0-findings.md F5's trap in its sharpest form, since the caller asked
            // a question and would otherwise be told the answer was yes.
            if (result.getFailure() instanceof org.opensearch.index.engine.VersionConflictEngineException conflict) {
                throw conflict;
            }
        }
        if (result.getResultType() == org.opensearch.index.engine.Engine.Result.Type.MAPPING_UPDATE_REQUIRED && mayGrowMapping) {
            // The document named a field the mapping does not have. Grow the mapping and write it again;
            // once, because a second MAPPING_UPDATE_REQUIRED after the update has been applied means
            // something other than a missing field, and retrying forever would hide it.
            growMapping(shardId, result.getRequiredMappingUpdate());
            return index(shardId, id, source, ifSeqNo, ifPrimaryTerm, requireAbsent, false);
        }
        if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
            // s0-findings.md F5: this is a value, not a throw. A caller that ignores it indexes nothing
            // and reports no error.
            throw new java.io.IOException("indexing " + id + " returned " + result.getResultType());
        }
        appendOrRelease(
            shardId,
            shard,
            java.util.List.of(
                new org.opensearch.serverless.store.WalRecord(id, source, result.getSeqNo(), result.getTerm(), result.getVersion())
            )
        );
        shard.sync();
        // The edge. Fired after the write is durable and applied, so a listener that publishes on it
        // can never publish a commit describing an operation the caller was not told about.
        signals.wrote(shardId);
        return new WriteOutcome(result.getSeqNo(), result.getTerm(), result.getVersion(), result.isCreated(), true);
    }

    /**
     * Appends a record to the log, and gives up the shard if that fails.
     *
     * <p><b>Why a failed append cannot simply be reported to the caller.</b> By the time this runs, the
     * operation is already in the engine. Reporting failure while leaving it there would make the refusal
     * a lie: this node would go on serving the document and would eventually publish it, so a caller told
     * the write failed would find it present, permanently. Releasing the shard is what makes the refusal
     * true — a successor rebuilds from the log, the log does not contain the operation, and the state the
     * caller was told about is the state that survives. Classic OpenSearch makes the same choice in the
     * same situation, failing the engine outright when a translog write fails.
     *
     * <p>The cost is a shard that has to be reacquired and replayed after a transient object-store
     * failure, which is the ordinary failover path and is already well covered. The alternative is a
     * silent divergence between what a caller was told and what the deployment keeps.
     *
     * @param shardId the shard being written to
     * @param shard the open shard
     * @param records the records to append, in order; empty is a no-op
     * @throws java.io.IOException if the append failed, after the shard has been released
     */
    private void appendOrRelease(
        org.opensearch.core.index.shard.ShardId shardId,
        org.opensearch.index.shard.IndexShard shard,
        java.util.List<org.opensearch.serverless.store.WalRecord> records
    ) throws java.io.IOException {
        final var wal = reconciler.wal(shardId);
        if (wal == null || records.isEmpty()) {
            return;
        }
        ensureOwnLeaseIsStillValid(shardId);
        try {
            wal.append(shard.getOperationPrimaryTerm(), records);
        } catch (Exception e) {
            reconciler.releaseShard(shardId, "the write-ahead log could not be appended to: " + e.getMessage());
            throw new java.io.IOException(
                "could not log " + records.size() + " operation(s); the shard has been released so it is not served",
                e
            );
        }
    }

    /**
     * Adds what a document needed to the index's mapping, durably, and applies it here.
     *
     * <p>The descriptor is the mapping's home, so this is the same compare-and-swap
     * {@code PUT /{index}/_mapping} performs — deliberately, because two ways of changing a mapping would be
     * two ways for it to drift. A lost swap re-reads and retries, so two writers introducing different fields
     * at the same time both keep theirs.
     *
     * @param shardId the shard whose write needed the field
     * @param addition what core says the mapping is missing
     * @throws java.io.IOException if the mapping cannot be stored
     */
    private void growMapping(org.opensearch.core.index.shard.ShardId shardId, org.opensearch.index.mapper.Mapping addition)
        throws java.io.IOException {
        final var plane = metadataPlane;
        if (plane == null || addition == null) {
            return;
        }
        final String indexName = shardId.getIndexName();
        for (int attempt = 0; attempt < 4; attempt++) {
            final long generation = plane.descriptorGeneration(indexName);
            final var current = plane.describe(indexName);
            if (current.isEmpty()) {
                return;
            }
            final String merged;
            final var mapperService = indicesService.createIndexMapperService(current.get().toIndexMetadata(java.util.Map.of()));
            try {
                if (current.get().mapping() != null) {
                    mapperService.merge(
                        org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME,
                        new org.opensearch.common.compress.CompressedXContent(current.get().mapping()),
                        org.opensearch.index.mapper.MapperService.MergeReason.MAPPING_RECOVERY
                    );
                }
                final var grown = mapperService.merge(
                    org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME,
                    new org.opensearch.common.compress.CompressedXContent(addition.toString()),
                    org.opensearch.index.mapper.MapperService.MergeReason.MAPPING_UPDATE
                );
                merged = unwrapMappingType(grown.mappingSource().string());
            } catch (Exception e) {
                throw new java.io.IOException("could not grow the mapping of " + indexName + " to fit the document", e);
            } finally {
                mapperService.close();
            }
            if (merged.equals(current.get().mapping())) {
                return;
            }
            final var updated = current.get().withMapping(merged);
            if (plane.updateDescriptor(updated, generation).isPresent()) {
                reconciler.refreshMapping(indexName, updated);
                return;
            }
        }
        throw new java.io.IOException("the mapping of " + indexName + " is being changed concurrently; retry the write");
    }

    /** Strips the single mapping type core wraps a merged mapping in, as MappingUpdateHandler does. */
    private static String unwrapMappingType(String source) throws java.io.IOException {
        final java.util.Map<String, Object> parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
            new org.opensearch.core.common.bytes.BytesArray(source),
            false,
            org.opensearch.common.xcontent.XContentType.JSON
        ).v2();
        final Object inner = parsed.get(org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME);
        if (parsed.size() != 1 || inner instanceof java.util.Map == false) {
            return source;
        }
        try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            @SuppressWarnings("unchecked")
            final java.util.Map<String, Object> body = (java.util.Map<String, Object>) inner;
            builder.map(body);
            return builder.toString();
        }
    }

    /**
     * Refuses the write if this node's own lease has already expired by its own clock.
     *
     * <p><b>Why the write path and not just the heartbeat.</b> A node learns it has lost a shard by reading
     * the shard-head, which it does on its heartbeat. A node that cannot reach the object store cannot read
     * the head <em>or</em> renew its lease, so today it keeps accepting writes for as long as the partition
     * lasts — the one case where the existing check is guaranteed not to fire is the one where it is needed.
     * Checking the lease this node last published costs nothing, needs no I/O, and is therefore affordable
     * on every write.
     *
     * <p><b>It bounds; it does not fence.</b> The check and the append are not atomic, so a pause between
     * them long enough to outlive the deadline still lands the append. That is the classic lease race and
     * it cannot be closed from the writer's side. It is closed on the reader's side instead, by the replay
     * cutoff in {@link org.opensearch.serverless.store.WalStore#position()}: a successor does not read what
     * was appended after it took over, whether the appender believed itself entitled or not.
     *
     * <p>The shard is released rather than merely refused, because a node past its own deadline has lost
     * every shard it held, not just this one's write — and a released shard is one a successor can take
     * cleanly instead of racing.
     *
     * @param shardId the shard being written to
     * @throws java.io.IOException if this node may no longer write
     */
    private void ensureOwnLeaseIsStillValid(org.opensearch.core.index.shard.ShardId shardId) throws java.io.IOException {
        final var plane = metadataPlane;
        if (plane == null) {
            return;
        }
        final long now = plane.clock().getAsLong();
        if (plane.membership().selfLeaseValidAt(now)) {
            return;
        }
        reconciler.releaseShard(shardId, "this node's own lease expired at or before " + now);
        throw new java.io.IOException(
            "refusing to write to "
                + shardId
                + ": this node's lease has expired, so it can no longer claim to hold the shard; "
                + "the shard has been released and the write was not acknowledged"
        );
    }

    /**
     * Deletes a document durably: the deletion reaches the object store's log before it is acknowledged.
     *
     * <p>Same ordering as {@link #index}, and for a sharper reason. A write lost between acknowledgement
     * and publication is a write the caller was told about and cannot find — bad. A <em>deletion</em>
     * lost the same way is worse than bad: replay rebuilds the shard from the log, and a deletion missing
     * from the log means the document it removed reappears, during a recovery that reports success. The
     * caller is told the delete worked, the failover is told the recovery worked, and the document is
     * there.
     *
     * @param shardId the shard to delete from
     * @param id the document id
     * @return what the engine assigned this deletion, including whether anything was actually removed
     * @throws java.io.IOException if the shard is not held here, or the delete fails
     */
    public WriteOutcome delete(org.opensearch.core.index.shard.ShardId shardId, String id) throws java.io.IOException {
        return delete(shardId, id, org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO, 0L);
    }

    /**
     * Deletes a document, optionally only if it is still at the sequence number the caller expects.
     *
     * <p>The same engine-owned compare-and-swap {@link #index(ShardId, String, String, long, long)}
     * describes, on the deletion path.
     *
     * @param shardId the shard to delete from
     * @param id the document id
     * @param ifSeqNo the sequence number the document must currently be at, or
     *     {@link org.opensearch.index.seqno.SequenceNumbers#UNASSIGNED_SEQ_NO} for an unconditional delete
     * @param ifPrimaryTerm the primary term the document must currently be at, or 0 when unconditional
     * @return what the engine assigned this deletion, including whether anything was actually removed
     * @throws org.opensearch.index.engine.VersionConflictEngineException if the condition was not met
     * @throws java.io.IOException if the shard is not held here, or the delete fails
     */
    public WriteOutcome delete(org.opensearch.core.index.shard.ShardId shardId, String id, long ifSeqNo, long ifPrimaryTerm)
        throws java.io.IOException {
        ensureStarted();
        final var shard = reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot delete from " + shardId + ": not open on " + nodeName);
        }
        if (reconciler.readerShards().contains(shardId)) {
            throw new java.io.IOException("cannot delete from " + shardId + ": it is open as a reader");
        }
        markUsed(shardId);
        // Apply, then log -- the same reordering, for the same reason, as the index path above.
        final var result = shard.applyDeleteOperationOnPrimary(
            org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
            id,
            org.opensearch.index.VersionType.INTERNAL,
            ifSeqNo,
            ifPrimaryTerm
        );
        if (result.getResultType() == org.opensearch.index.engine.Engine.Result.Type.FAILURE
            && result.getFailure() instanceof org.opensearch.index.engine.VersionConflictEngineException conflict) {
            throw conflict;
        }
        if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
            // A value, not a throw -- the same trap s0-findings.md F5 recorded for indexing.
            throw new java.io.IOException("deleting " + id + " returned " + result.getResultType());
        }
        appendOrRelease(
            shardId,
            shard,
            java.util.List.of(
                org.opensearch.serverless.store.WalRecord.deletion(id, result.getSeqNo(), result.getTerm(), result.getVersion())
            )
        );
        shard.sync();
        // The same edge a write raises: a deletion changes the shard as surely as an addition, and a
        // shard whose only recent change was a delete still needs publishing or the tombstone lives
        // nowhere but this node's disk and its log.
        signals.wrote(shardId);
        return new WriteOutcome(result.getSeqNo(), result.getTerm(), result.getVersion(), false, result.isFound());
    }

    /**
     * Applies a batch to one shard: one log append for all of it, then each operation in order.
     *
     * <p><b>Why this is not a loop over {@link #index} and {@link #delete}.</b> Those append one record
     * each, so a hundred-document request would be a hundred object-store PUTs — the write path priced
     * per document, which is what made {@code _bulk} worth building rather than worth faking. Here the
     * whole batch is one append, and the durability contract is unchanged: every record is in the log
     * before any of it is acknowledged, which is strictly the same promise a single write makes.
     *
     * <p><b>Ordering within the batch is preserved</b>, and it has to be. A batch that writes a document
     * and then deletes it must leave it gone, and one that deletes and then rewrites must leave it
     * present. Since a record's kind travels with it, replay reproduces the same sequence — see
     * {@link org.opensearch.serverless.store.WalStore#append(long, java.util.List)}.
     *
     * <p><b>A failed operation does not roll back the ones before it</b>, exactly as in classic
     * OpenSearch: a bulk is a batch, not a transaction. The failure is reported for its own item and the
     * rest of the batch stands.
     *
     * @param shardId the shard, which this node must own as a writer
     * @param operations the writes and deletions, in request order
     * @return one outcome per operation, in the same order
     * @throws java.io.IOException if the shard is not open here, is a reader, or the log append fails
     */
    public java.util.List<BulkOutcome> bulk(
        org.opensearch.core.index.shard.ShardId shardId,
        java.util.List<org.opensearch.serverless.store.WalRecord> operations
    ) throws java.io.IOException {
        final java.util.List<BulkOperation> plain = new java.util.ArrayList<>(operations.size());
        for (org.opensearch.serverless.store.WalRecord record : operations) {
            plain.add(BulkOperation.of(record));
        }
        return bulkOperations(shardId, plain);
    }

    /**
     * One operation in a batch, with whatever condition the caller attached to it.
     *
     * <p><b>Why this type exists.</b> The batch path used to take {@link org.opensearch.serverless.store.WalRecord}
     * — the write-ahead <em>log</em> record — as its input type. A log record describes what happened, so
     * there was nowhere in it for a condition, which describes what must be true before anything happens.
     * That is the whole reason {@code _bulk} refused {@code create} and per-item {@code if_seq_no} after
     * the single-document path gained both: not a missing capability, a missing input type.
     */
    public static final class BulkOperation {
        private final org.opensearch.serverless.store.WalRecord record;
        private final long ifSeqNo;
        private final long ifPrimaryTerm;
        private final boolean requireAbsent;

        /**
         * Creates an operation carrying a condition.
         *
         * @param record what to write or delete
         * @param ifSeqNo the sequence number the document must be at, or unassigned when unconditional
         * @param ifPrimaryTerm the primary term the document must be at, or 0 when unconditional
         * @param requireAbsent whether the write must fail if the document already exists
         */
        public BulkOperation(org.opensearch.serverless.store.WalRecord record, long ifSeqNo, long ifPrimaryTerm, boolean requireAbsent) {
            this.record = record;
            this.ifSeqNo = ifSeqNo;
            this.ifPrimaryTerm = ifPrimaryTerm;
            this.requireAbsent = requireAbsent;
        }

        /**
         * An unconditional operation.
         *
         * @param record what to write or delete
         * @return the operation
         */
        public static BulkOperation of(org.opensearch.serverless.store.WalRecord record) {
            return new BulkOperation(record, org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO, 0L, false);
        }

        /** @return the record */
        public org.opensearch.serverless.store.WalRecord record() {
            return record;
        }

        /** @return the sequence number the document must be at, or unassigned */
        public long ifSeqNo() {
            return ifSeqNo;
        }

        /** @return the primary term the document must be at, or 0 */
        public long ifPrimaryTerm() {
            return ifPrimaryTerm;
        }

        /** @return whether the document must not already exist */
        public boolean requireAbsent() {
            return requireAbsent;
        }
    }

    /**
     * Applies a batch whose items may carry conditions.
     *
     * @param shardId the shard to write to
     * @param batch the operations, in request order
     * @return one outcome per operation, in the same order
     * @throws java.io.IOException if the shard is not open here, is a reader, or the log append fails
     */
    public java.util.List<BulkOutcome> bulkOperations(org.opensearch.core.index.shard.ShardId shardId, java.util.List<BulkOperation> batch)
        throws java.io.IOException {
        ensureStarted();
        final var shard = reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot write to " + shardId + ": not open on " + nodeName);
        }
        if (reconciler.readerShards().contains(shardId)) {
            throw new java.io.IOException("cannot write to " + shardId + ": it is open as a reader");
        }
        if (batch.isEmpty()) {
            return java.util.List.of();
        }
        markUsed(shardId);

        // Applied first, then logged once for the whole batch -- the same reordering the single-document
        // path took, and the batch economics are unchanged: one PUT for the batch, not one per document.
        // Only operations that actually applied are logged, so replay never reproduces an item the caller
        // was told had failed.
        final java.util.List<BulkOutcome> outcomes = new java.util.ArrayList<>(batch.size());
        final java.util.List<org.opensearch.serverless.store.WalRecord> applied = new java.util.ArrayList<>(batch.size());
        for (BulkOperation item : batch) {
            final org.opensearch.serverless.store.WalRecord operation = item.record();
            try {
                if (operation.isDeletion()) {
                    final var result = shard.applyDeleteOperationOnPrimary(
                        org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                        operation.id(),
                        org.opensearch.index.VersionType.INTERNAL,
                        item.ifSeqNo(),
                        item.ifPrimaryTerm()
                    );
                    if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
                        // s0-findings.md F5 again: a failure here is a value, and a caller that reads
                        // only the exception channel reports success for an operation that did nothing.
                        outcomes.add(
                            conflicted(result)
                                ? BulkOutcome.conflicted(operation.id(), describe("deleting", result))
                                : BulkOutcome.failed(operation.id(), describe("deleting", result))
                        );
                        continue;
                    }
                    applied.add(
                        org.opensearch.serverless.store.WalRecord.deletion(
                            operation.id(),
                            result.getSeqNo(),
                            result.getTerm(),
                            result.getVersion()
                        )
                    );
                    outcomes.add(
                        BulkOutcome.deleted(operation.id(), result.isFound(), result.getSeqNo(), result.getTerm(), result.getVersion())
                    );
                } else {
                    final var result = shard.applyIndexOperationOnPrimary(
                        item.requireAbsent()
                            ? org.opensearch.common.lucene.uid.Versions.MATCH_DELETED
                            : org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                        org.opensearch.index.VersionType.INTERNAL,
                        new org.opensearch.index.mapper.SourceToParse(
                            shardId.getIndexName(),
                            operation.id(),
                            new org.opensearch.core.common.bytes.BytesArray(operation.source()),
                            org.opensearch.common.xcontent.XContentType.JSON
                        ),
                        item.ifSeqNo(),
                        item.ifPrimaryTerm(),
                        org.opensearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                        false
                    );
                    if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
                        // A lost condition is a FAILURE carrying a VersionConflictEngineException rather
                        // than a thrown one, so the item has to be inspected rather than only its type
                        // reported -- otherwise a caller that asked a compare-and-swap question is told
                        // only that "indexing returned FAILURE", which is not an answer to it.
                        outcomes.add(
                            conflicted(result)
                                ? BulkOutcome.conflicted(operation.id(), describe("indexing", result))
                                : BulkOutcome.failed(operation.id(), describe("indexing", result))
                        );
                        continue;
                    }
                    applied.add(
                        new org.opensearch.serverless.store.WalRecord(
                            operation.id(),
                            operation.source(),
                            result.getSeqNo(),
                            result.getTerm(),
                            result.getVersion()
                        )
                    );
                    outcomes.add(
                        BulkOutcome.indexed(operation.id(), result.getSeqNo(), result.getTerm(), result.getVersion(), result.isCreated())
                    );
                }
            } catch (Exception e) {
                // One bad document does not fail the batch. A mapping conflict on item 40 is item 40's
                // problem, and failing the other 99 would make a bulk request less useful than the loop
                // it replaces.
                final String message = e.getMessage() == null ? e.toString() : e.getMessage();
                outcomes.add(
                    e instanceof org.opensearch.index.engine.VersionConflictEngineException
                        ? BulkOutcome.conflicted(operation.id(), message)
                        : BulkOutcome.failed(operation.id(), message)
                );
            }
        }

        appendOrRelease(shardId, shard, applied);
        // One fsync and one edge for the batch, not one per document. The edge is raised after
        // everything is applied, so a publish it triggers describes the whole batch or none of it.
        shard.sync();
        signals.wrote(shardId);
        return outcomes;
    }

    /**
     * Describes a non-success engine result, naming the cause when there is one.
     *
     * @param result the engine's answer
     * @return whether the failure was a lost condition
     */
    private static boolean conflicted(org.opensearch.index.engine.Engine.Result result) {
        return result.getFailure() instanceof org.opensearch.index.engine.VersionConflictEngineException;
    }

    /**
     * Describes a non-success engine result, naming the cause when there is one.
     *
     * @param what the operation kind, for the message
     * @param result the engine's answer
     * @return a message for the failed item
     */
    private static String describe(String what, org.opensearch.index.engine.Engine.Result result) {
        if (result.getFailure() != null) {
            final String message = result.getFailure().getMessage();
            return message == null ? result.getFailure().toString() : message;
        }
        return what + " returned " + result.getResultType();
    }

    /**
     * What the engine assigned one single-document write or deletion.
     *
     * <p>These are the numbers real OpenSearch returns as {@code _seq_no}, {@code _primary_term} and
     * {@code _version}. They were always being generated here — the engine is core's own and assigns them
     * on every operation — and were simply discarded at this boundary. Returning them is what lets a
     * caller hold one and send it back as {@code if_seq_no}.
     *
     * <p>A plain class rather than a record because this module's javadoc check rejects records.
     */
    public static final class WriteOutcome {

        private final long seqNo;
        private final long primaryTerm;
        private final long version;
        private final boolean created;
        private final boolean found;

        /**
         * Creates an outcome.
         *
         * @param seqNo the sequence number assigned
         * @param primaryTerm the primary term the operation ran at
         * @param version the version assigned
         * @param created whether a write created the document rather than overwriting one
         * @param found whether a deletion found anything; always true for a write
         */
        public WriteOutcome(long seqNo, long primaryTerm, long version, boolean created, boolean found) {
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
            this.created = created;
            this.found = found;
        }

        /**
         * Returns the sequence number the engine assigned.
         *
         * @return the sequence number
         */
        public long seqNo() {
            return seqNo;
        }

        /**
         * Returns the primary term the operation ran at — this shard's shard-head term.
         *
         * @return the primary term
         */
        public long primaryTerm() {
            return primaryTerm;
        }

        /**
         * Returns the version the engine assigned.
         *
         * @return the version
         */
        public long version() {
            return version;
        }

        /**
         * Reports whether a write created the document rather than overwriting an existing one.
         *
         * @return true when the document did not exist before
         */
        public boolean created() {
            return created;
        }

        /**
         * Reports whether a deletion found the document. Always true for a write.
         *
         * @return true when there was something to remove
         */
        public boolean found() {
            return found;
        }
    }

    /**
     * What one operation in a batch did.
     *
     * <p>A plain class rather than a record because this module's javadoc check rejects records.
     */
    public static final class BulkOutcome {

        private final String id;
        private final boolean deletion;
        private final boolean found;
        private final String failure;
        private boolean conflict;
        private final long seqNo;
        private final long primaryTerm;
        private final long version;
        private final boolean created;

        private BulkOutcome(
            String id,
            boolean deletion,
            boolean found,
            String failure,
            long seqNo,
            long primaryTerm,
            long version,
            boolean created
        ) {
            this.id = id;
            this.deletion = deletion;
            this.found = found;
            this.failure = failure;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
            this.created = created;
        }

        /**
         * Records a document written, with the sequence identity the engine assigned it.
         *
         * @param id the document id
         * @param seqNo the sequence number assigned
         * @param primaryTerm the primary term it ran at
         * @param version the version assigned
         * @param created whether the document did not exist before this write
         * @return the outcome
         */
        public static BulkOutcome indexed(String id, long seqNo, long primaryTerm, long version, boolean created) {
            return new BulkOutcome(id, false, false, null, seqNo, primaryTerm, version, created);
        }

        /**
         * Records a deletion, which may or may not have found anything.
         *
         * @param id the document id
         * @param found whether the document was there
         * @param seqNo the sequence number assigned
         * @param primaryTerm the primary term it ran at
         * @param version the version assigned
         * @return the outcome
         */
        public static BulkOutcome deleted(String id, boolean found, long seqNo, long primaryTerm, long version) {
            return new BulkOutcome(id, true, found, null, seqNo, primaryTerm, version, false);
        }

        /**
         * Records an operation the engine refused.
         *
         * @param id the document id
         * @param reason why it failed
         * @return the outcome
         */
        public static BulkOutcome conflicted(String id, String reason) {
            final BulkOutcome outcome = failed(id, reason);
            outcome.conflict = true;
            return outcome;
        }

        /**
         * Whether this item failed because a condition it carried was not met.
         *
         * <p>Separated from any other failure because it is the one a caller asked a question with: a lost
         * compare-and-swap is a 409 and must not be retried blindly, while an ordinary item failure is a
         * 400 and usually can be.
         *
         * @return whether this was a version conflict
         */
        public boolean conflict() {
            return conflict;
        }

        public static BulkOutcome failed(String id, String reason) {
            return new BulkOutcome(
                id,
                false,
                false,
                reason == null ? "unknown failure" : reason,
                org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
                org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                org.opensearch.common.lucene.uid.Versions.NOT_FOUND,
                false
            );
        }

        /**
         * Returns the sequence number the engine assigned.
         *
         * @return the sequence number, unassigned for a failed item
         */
        public long seqNo() {
            return seqNo;
        }

        /**
         * Returns the primary term the operation ran at.
         *
         * @return the primary term, unassigned for a failed item
         */
        public long primaryTerm() {
            return primaryTerm;
        }

        /**
         * Returns the version the engine assigned.
         *
         * @return the version
         */
        public long version() {
            return version;
        }

        /**
         * Reports whether a write created the document rather than overwriting one.
         *
         * @return true when the document did not exist before
         */
        public boolean created() {
            return created;
        }

        /**
         * Returns the document id.
         *
         * @return the id
         */
        public String id() {
            return id;
        }

        /**
         * Reports whether this operation removed a document rather than adding one.
         *
         * @return true for a deletion
         */
        public boolean isDeletion() {
            return deletion;
        }

        /**
         * Reports whether a deletion found the document. Meaningless for a write.
         *
         * @return true when the document was there to delete
         */
        public boolean found() {
            return found;
        }

        /**
         * Returns why the operation failed, or null if it did not.
         *
         * @return the failure message, or null
         */
        public String failure() {
            return failure;
        }
    }

    private volatile org.opensearch.search.SearchModule searchModule;

    /**
     * The one {@link org.opensearch.search.SearchModule} this node builds, memoized.
     *
     * <p>It was constructed inline for its fetch phase and thrown away. It also carries the parsers for
     * every query, aggregation and suggester in the codebase, which is what a real query body needs to be
     * read at all — so it is kept rather than rebuilt per request.
     */
    private synchronized org.opensearch.search.SearchModule searchModule() {
        if (searchModule == null) {
            // A SearchPlugin's queries, aggregations and suggesters come in here. Same argument as
            // analysis: a plugin whose extension point is ignored has not been hosted.
            searchModule = new org.opensearch.search.SearchModule(settings, plugins.filter(org.opensearch.plugins.SearchPlugin.class));
        }
        return searchModule;
    }

    /**
     * The registry that can read a search body.
     *
     * <p>Separate from the node's other registry, which is deliberately empty: nothing else in the shell
     * parses user-supplied structured content, and an empty registry is the honest default for a surface
     * that is an allowlist. A search body is the one place that needs the full set.
     *
     * @return a registry containing the search parsers
     */
    public synchronized org.opensearch.core.xcontent.NamedXContentRegistry searchXContentRegistry() {
        if (searchRegistry == null) {
            searchRegistry = new org.opensearch.core.xcontent.NamedXContentRegistry(searchModule().getNamedXContents());
        }
        return searchRegistry;
    }

    private volatile org.opensearch.core.xcontent.NamedXContentRegistry searchRegistry;

    private volatile ServerlessClient client;

    private volatile org.opensearch.watcher.ResourceWatcherService resourceWatcherService;
    private volatile org.opensearch.script.ScriptService scriptService;
    private volatile org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * The file watcher a plugin is given.
     *
     * <p>Real, and running. A plugin that keeps configuration on disk — the OpenSearch security plugin is
     * the obvious one — watches it through this and reloads when it changes. The shell watches nothing
     * itself, which was the excuse for passing null; a collaborator a plugin needs is not made unnecessary
     * by the host not needing it.
     *
     * @return the watcher service
     */
    public synchronized org.opensearch.watcher.ResourceWatcherService resourceWatcherService() {
        if (resourceWatcherService == null) {
            resourceWatcherService = new org.opensearch.watcher.ResourceWatcherService(settings, threadPool);
        }
        return resourceWatcherService;
    }

    /**
     * The script service, with Painless registered.
     *
     * <p>Chosen by name rather than discovered from disk, which is how this shell has always taken its
     * transport. A plugin handed this service can compile and run a script; a language nothing registers
     * still fails with core's own "cannot compile: no lang registered", in the same words a classic node
     * uses when a language's module is not installed.
     *
     * @return the script service
     */
    public synchronized org.opensearch.script.ScriptService scriptService() {
        if (scriptService == null) {
            // Painless, chosen directly rather than discovered from modules/ -- the same route this shell
            // already takes for its transport, and for the same reason. Without an engine registered the
            // service compiles nothing, so every script in a query, an aggregation or an update failed with
            // "cannot execute [painless] scripts" -- which reads as a configuration problem and is really an
            // absent capability.
            final org.opensearch.script.ScriptEngine painless = new org.opensearch.painless.PainlessModulePlugin().getScriptEngine(
                settings,
                org.opensearch.script.ScriptModule.CORE_CONTEXTS.values()
            );
            scriptService = new org.opensearch.script.ScriptService(
                settings,
                Map.of(painless.getType(), painless),
                org.opensearch.script.ScriptModule.CORE_CONTEXTS
            );
        }
        return scriptService;
    }

    private volatile org.opensearch.serverless.ingest.IngestPipelines ingestPipelines;

    /**
     * Returns the ingest pipeline compiler, built over core's own processor factories.
     *
     * <p>{@code Processor.Parameters} is given what this shell has and null for what it does not: there is no
     * {@code IngestService} here, because pipelines come from a register rather than from cluster state, and
     * no {@code Client}, because nothing on this surface forwards a processor's own request. A processor that
     * needs either fails when it is compiled, at {@code PUT} time, naming itself.
     *
     * @return the pipeline compiler
     */
    public synchronized org.opensearch.serverless.ingest.IngestPipelines ingestPipelines() {
        if (ingestPipelines == null) {
            final var parameters = new org.opensearch.ingest.Processor.Parameters(
                new org.opensearch.env.Environment(settings, null),
                scriptService(),
                null,                                   // analysis registry: no processor here needs one
                threadPool().getThreadContext(),
                threadPool()::relativeTimeInMillis,
                (delay, command) -> threadPool().schedule(
                    command,
                    org.opensearch.common.unit.TimeValue.timeValueMillis(delay),
                    ThreadPool.Names.GENERIC
                ),
                null,                                   // ingest service: pipelines live in a register
                null,                                   // client: nothing here re-enters the API
                task -> threadPool().generic().execute(task),
                indicesService
            );
            ingestPipelines = new org.opensearch.serverless.ingest.IngestPipelines(
                new org.opensearch.ingest.common.IngestCommonModulePlugin().getProcessors(parameters),
                scriptService()
            );
        }
        return ingestPipelines;
    }

    /**
     * The named-writeable registry a plugin is given.
     *
     * <p>The node's own, the one its transport uses, rather than an empty one: a plugin deserialising a
     * {@code QueryBuilder} it received has to look it up in the same registry that serialised it.
     *
     * @return the registry
     */
    public NamedWriteableRegistry namedWriteableRegistry() {
        return new NamedWriteableRegistry(searchModule().getNamedWriteables());
    }

    /**
     * The index-name resolver a plugin is given, which refuses rather than answering wrongly.
     *
     * @return the resolver
     * @see RefusingIndexNameExpressionResolver
     */
    public synchronized org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver() {
        if (indexNameExpressionResolver == null) {
            indexNameExpressionResolver = new RefusingIndexNameExpressionResolver(threadPool.getThreadContext());
        }
        return indexNameExpressionResolver;
    }

    private volatile ActionGate actionGate;
    private volatile BigArrays bigArrays;
    private volatile org.opensearch.core.indices.breaker.CircuitBreakerService circuitBreakerService;
    private volatile org.opensearch.index.IndexingPressure indexingPressure;

    /**
     * What bounds the memory a node's in-flight writes are holding.
     *
     * <p><b>Not the circuit breaker, and the difference is not pedantic.</b> The breaker accounts what a
     * request allocates while computing an answer; indexing pressure accounts the bytes of the writes
     * themselves, which are in memory from the moment a batch is parsed until it has been applied. A node
     * with no bound on that is one large enough batch, or enough concurrent ones, away from dying — and
     * dying loses every other request as well.
     *
     * <p>Core's own, with core's own setting: {@code indexing_pressure.memory.limit}, ten per cent of the
     * heap by default, and a rejection an operator has seen before rather than one this shell invented.
     *
     * @return the accounting
     */
    public synchronized org.opensearch.index.IndexingPressure indexingPressure() {
        if (indexingPressure == null) {
            indexingPressure = new org.opensearch.index.IndexingPressure(settings);
        }
        return indexingPressure;
    }

    /**
     * The node's circuit breakers.
     *
     * <p><b>Real ones, and they were not.</b> Every component here was given a
     * {@code NoneCircuitBreakerService}, which accounts nothing and refuses nothing. That was invisible
     * while the shell's surface allocated nothing worth bounding, and stopped being invisible when
     * aggregations arrived: an aggregation over a high-cardinality field is the ordinary way to exhaust a
     * node's heap, and a node that dies is worse for every other request than one that refuses this one.
     *
     * <p>Core's own hierarchy, with core's own defaults, deliberately — the parent limit, the request and
     * field-data children and the thresholds between them are numbers OpenSearch has tuned against real
     * workloads, and inventing different ones here would be inventing a different product.
     *
     * @return the breaker service
     */
    public synchronized org.opensearch.core.indices.breaker.CircuitBreakerService circuitBreakerService() {
        if (circuitBreakerService == null) {
            circuitBreakerService = new org.opensearch.indices.breaker.HierarchyCircuitBreakerService(
                settings,
                java.util.List.of(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
        }
        return circuitBreakerService;
    }

    /**
     * The node's array allocator.
     *
     * <p><b>Not {@code BigArrays.NON_RECYCLING_INSTANCE}, which cannot allocate.</b> That constant is built
     * with a null circuit-breaker service, so the first component to actually ask it for an array gets a
     * {@code NullPointerException} rather than an array. Nothing noticed for as long as nothing allocated:
     * a query needs no scratch space, and an aggregation is the first thing here that does.
     *
     * <p>It accounts against the node's real breakers, so an allocation that would take the node past its
     * request limit is refused rather than granted.
     *
     * @return the allocator
     */
    public synchronized BigArrays bigArrays() {
        if (bigArrays == null) {
            bigArrays = new BigArrays(
                new PageCacheRecycler(settings),
                circuitBreakerService(),
                org.opensearch.core.common.breaker.CircuitBreaker.REQUEST
            );
        }
        return bigArrays;
    }

    /**
     * The gate every operation passes through, where a plugin's {@code ActionFilter}s decide.
     *
     * @return the gate, which is empty and free when no plugin installed a filter
     */
    public synchronized ActionGate actionGate() {
        if (actionGate == null) {
            actionGate = new ActionGate(plugins.actionFilters());
        }
        return actionGate;
    }

    /**
     * Reports whether an index belongs to a plugin, and is therefore not reachable through REST.
     *
     * @param index the index name
     * @return true if it is a system index
     */
    public boolean isSystemIndex(String index) {
        return systemIndices != null && systemIndices.contains(index);
    }

    /**
     * Returns the plugins this node is running.
     *
     * @return the plugin host
     */
    public ServerlessPlugins plugins() {
        return plugins;
    }

    /**
     * Returns the identity service, which is core's no-op unless a plugin supplied one.
     *
     * @return the identity service
     */
    public synchronized org.opensearch.identity.IdentityService identityService() {
        if (identityService == null) {
            identityService = plugins.identityService(settings, threadPool);
        }
        return identityService;
    }

    /**
     * The {@link org.opensearch.transport.client.Client} this node offers to plugins.
     *
     * <p>The shell's own operations behind the interface every plugin expects. The {@code NodeClient} the
     * REST layer hands to handlers delegates here, so there is one allowlist and one set of answers no
     * matter which door a plugin came in by.
     *
     * @return the client
     */
    public synchronized org.opensearch.transport.client.Client client() {
        if (client == null) {
            client = new ServerlessClient(settings, threadPool, () -> this, () -> metadataPlane);
        }
        return client;
    }

    /**
     * Records that a request touched this shard, on the clock the reconciler will compare against.
     *
     * <p><b>The plane's clock, not the wall clock, and that is not a detail.</b> Idle release compares a
     * stamp made here against the time a reconcile pass was given, and a test drives that pass from a
     * clock it controls. Stamping {@code System.currentTimeMillis()} instead makes every shard look as
     * though it were used far in the future, so nothing is ever idle — which is exactly how the first run
     * of these tests failed, with a controller that was working and a stamp that was not comparable.
     *
     * @param shardId the shard a request used
     */
    public void markUsed(org.opensearch.core.index.shard.ShardId shardId) {
        final org.opensearch.serverless.metadata.MetadataPlane plane = metadataPlane;
        reconciler.markUsed(shardId, plane == null ? System.currentTimeMillis() : plane.clock().getAsLong());
    }

    /**
     * Reads one document by id, from this node's own copy of the shard.
     *
     * <p><b>Realtime, and that is the whole reason this exists separately from search.</b> A write is
     * applied to the engine and durable in the log before it is acknowledged, but it is not searchable
     * until a refresh and not in the object store until a publish. A get that went through the search
     * path would therefore fail to find a document the caller had just been told was written — which
     * reads as data loss and is not. This reads the live version map, so it sees the write.
     *
     * <p>Whether the answer is realtime depends on which copy this is, and the caller is the one that
     * knows: a writer's copy includes everything acknowledged, a reader's copy includes only what was
     * published. This method does not choose between them — {@code GetHandler} does, and says which it
     * used in the response.
     *
     * @param shardId the shard, open on this node
     * @param id the document id
     * @return what was found, which may be nothing
     * @throws java.io.IOException if the shard is not open here
     */
    public Document get(org.opensearch.core.index.shard.ShardId shardId, String id) throws java.io.IOException {
        ensureStarted();
        final var shard = reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot read from " + shardId + ": not open on " + nodeName);
        }
        markUsed(shardId);
        final var result = shard.getService()
            .get(
                id,
                null,
                true,
                org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                org.opensearch.index.VersionType.INTERNAL,
                org.opensearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE
            );
        // No searcher to release here: ShardGetService acquires and closes its own, and GetResult is a
        // value rather than a handle.
        if (result.isExists() == false) {
            return Document.absent(id);
        }
        // The sequence identity comes back with the document, and always did -- ShardGetService reads it
        // out of the same doc-values lookup that finds the source. It was simply dropped here, which is
        // why a caller could never obtain the token it needed to send back as if_seq_no.
        return new Document(id, true, result.sourceAsString(), result.getSeqNo(), result.getPrimaryTerm(), result.getVersion());
    }

    /** One document, or the fact that there is none. */
    public static final class Document {

        private final String id;
        private final boolean found;
        private final String source;
        private final long seqNo;
        private final long primaryTerm;
        private final long version;

        /**
         * Creates a found document with no sequence identity.
         *
         * @param id the document id
         * @param found whether it exists
         * @param source its source, or null when it does not
         */
        public Document(String id, boolean found, String source) {
            this(
                id,
                found,
                source,
                org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
                org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                org.opensearch.common.lucene.uid.Versions.NOT_FOUND
            );
        }

        /**
         * Creates a found document, carrying the sequence identity it currently holds.
         *
         * @param id the document id
         * @param found whether it exists
         * @param source its source, or null when it does not
         * @param seqNo the sequence number of the operation that last wrote it
         * @param primaryTerm the primary term that operation ran at
         * @param version its current version
         */
        public Document(String id, boolean found, String source, long seqNo, long primaryTerm, long version) {
            this.id = id;
            this.found = found;
            this.source = source;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
        }

        /**
         * Returns the sequence number of the operation that last wrote this document.
         *
         * @return the sequence number, unassigned when the document was not found
         */
        public long seqNo() {
            return seqNo;
        }

        /**
         * Returns the primary term that operation ran at.
         *
         * @return the primary term, unassigned when the document was not found
         */
        public long primaryTerm() {
            return primaryTerm;
        }

        /**
         * Returns the document's current version.
         *
         * @return the version
         */
        public long version() {
            return version;
        }

        /**
         * Records that no such document exists.
         *
         * @param id the document id
         * @return the absence
         */
        public static Document absent(String id) {
            return new Document(id, false, null);
        }

        /**
         * Returns the document id.
         *
         * @return the id
         */
        public String id() {
            return id;
        }

        /**
         * Reports whether the document exists.
         *
         * @return true when it does
         */
        public boolean found() {
            return found;
        }

        /**
         * Returns the document source, or null when there is none.
         *
         * @return the source
         */
        public String source() {
            return source;
        }
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
        // Stop accepting requests first, then close the plugins that were serving them, then the services
        // those plugins were given.
        //
        // Nothing called this until now: M23 wrote ServerlessPlugins#closeAll and never invoked it, which
        // was invisible while every plugin in existence was a test plugin holding nothing. The first plugin
        // that owned a thread pool leaked it out of every node, and the leak detector said so.
        IOUtils.closeWhileHandlingException(httpServerTransport);
        ServerlessPlugins.closeAll(plugins.plugins());
        // After the plugins, because they were given these and may use them on the way out.
        IOUtils.closeWhileHandlingException(resourceWatcherService, scriptService);
        IOUtils.closeWhileHandlingException(transportService, searchService, indicesService, clusterService, nodeEnvironment);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
