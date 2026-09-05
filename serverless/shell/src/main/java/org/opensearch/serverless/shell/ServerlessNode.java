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

    /**
     * The pool forwarded searches and explains are answered on, and the heartbeat's head scan reads on.
     *
     * <p>Not the coordinator's own shard tasks any more: those run on GENERIC (see {@code SearchFanout}),
     * so a search that waits on GENERIC for tasks that also ran on GENERIC cannot park every thread
     * waiting for work no thread is free to do. See the thread pool's construction for why it is
     * separate at all.
     */
    public static final String FANOUT_POOL = "serverless_fanout";

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
    /** The descriptors of the indices this node holds writer shards of, as last read; the reader half is {@link #served}. */
    private final Map<String, IndexDescriptor> hosted = new java.util.concurrent.ConcurrentHashMap<>();
    /** The term each writer shard was acquired at, keyed {@code index#shard}; pruned to what is open when a view is projected. */
    private final Map<String, ShardAssignment> writerAssignments = new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * One fence per shard. The write path holds the read side from applying an operation to the engine
     * until it is acknowledged; anything that gives the shard away -- an idle release, a lost head, a
     * lapsed lease -- holds the write side from before the publish until after the close. See
     * {@link #underShardFence}.
     */
    private final Map<org.opensearch.core.index.shard.ShardId, java.util.concurrent.locks.ReentrantReadWriteLock> shardFences =
        new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * Writer shards whose log could not be appended to. Still open and still readable; refusing writes;
     * reopened from the log by the next heartbeat that reaches the store. See {@link #appendOrRelease}.
     */
    private final Set<org.opensearch.core.index.shard.ShardId> writeFenced = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /**
     * Heads this node lost to its own lapsed lease and has not yet given back in the register, with the
     * term it lost them at. Drained on every renewal, so a store that was unreachable when the loss was
     * discovered does not leave a head naming a node that is not serving the shard.
     */
    private final Map<org.opensearch.core.index.shard.ShardId, Long> headsToGiveBack = new java.util.concurrent.ConcurrentHashMap<>();
    private final Object leaseRenewalLock = new Object();
    /** Serialises head verification: two passes interleaving their releases was the hazard, and waiting is cheaper than skipping. */
    private final Object headVerificationLock = new Object();
    /**
     * True from the moment this node's lease is found lapsed until a head verification that began after
     * the renewal has read every head. While set, no write is acknowledged and no forwarded write is
     * accepted. See {@link #renewLease}.
     */
    private volatile boolean ownershipUnverified;
    /** Bumped on every lapse, so a verification that began before the renewal cannot lift the suspension. */
    private final java.util.concurrent.atomic.AtomicLong lapseEpoch = new java.util.concurrent.atomic.AtomicLong();
    /** How many shard-head reads a verification pass has in flight at once. */
    public static final int HEAD_READ_LANES = 8;

    /**
     * What a reader or a frozen view asks before taking a slot on this node: the cap, and whether room
     * can be made under it.
     *
     * <p>The cap and the eviction policy live in the reconciler; readers and views used to compare
     * against the constant default and refuse, so a node configured for two hundred shards opened a
     * thousand readers, and one at the constant refused the next search until the idle sweep came
     * round. {@code BackgroundReconciler} installs itself here when it is built.
     */
    public interface ShardAdmission {
        /**
         * Returns how many shards this node will hold.
         *
         * @return the cap
         */
        int maxShardsHeld();

        /**
         * Tries to free a slot by letting go of a shard nobody has used lately.
         *
         * @return true if there is now room under the cap
         */
        boolean makeRoom();
    }

    private volatile ShardAdmission admission = new ShardAdmission() {
        @Override
        public int maxShardsHeld() {
            return org.opensearch.serverless.reconcile.BackgroundReconciler.DEFAULT_MAX_SHARDS_HELD;
        }

        @Override
        public boolean makeRoom() {
            return false;
        }
    };

    /**
     * Installs the cap and eviction policy readers and views are admitted under.
     *
     * @param admission the policy, normally the node's reconciler
     */
    public void setShardAdmission(ShardAdmission admission) {
        this.admission = admission;
    }

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
            // Sized from the heap when not configured: the cache lives outside the breakers (accounting it
            // there would trip every request check by its own size), so its bound has to come from what
            // the process actually has rather than from a constant that was right on one machine.
            settings.getAsInt(
                "serverless.block_cache.max_blocks",
                (int) Math.max(
                    256L,
                    Runtime.getRuntime().maxMemory() / 10 / settings.getAsInt(
                        "serverless.block_cache.block_size",
                        org.opensearch.serverless.store.BlockCache.DEFAULT_BLOCK_SIZE
                    )
                )
            )
        );
        // Rebuilt from the settled settings, so anything a plugin contributed is visible to everything
        // below.
        final Environment environment = new Environment(settings, null);
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        // A pool of its own for fanned-out shard work. The coordinator of a search waits on GENERIC for
        // its shard tasks; when those tasks also ran on GENERIC, enough concurrent searches parked every
        // thread waiting for tasks no thread was free to run. A forwarded search handler ran on SEARCH and
        // blocked on the query phase forked to SEARCH, with the same shape. Both now run here, and this
        // pool waits on nothing that runs on it.
        this.threadPool = new ThreadPool(
            settings,
            new org.opensearch.threadpool.ScalingExecutorBuilder(
                FANOUT_POOL,
                1,
                Math.max(8, 4 * org.opensearch.common.util.concurrent.OpenSearchExecutors.allocatedProcessors(settings)),
                org.opensearch.common.unit.TimeValue.timeValueSeconds(30)
            )
        );
        boolean success = false;
        NodeEnvironment openedEnvironment = null;
        try {
            // Held in a local as well, because the failure path below cannot read a blank final -- and it
            // has to close this one thing above all others: NodeEnvironment holds node.lock.
            openedEnvironment = new NodeEnvironment(settings, environment);
            this.nodeEnvironment = openedEnvironment;
            // Roles from the lease's roles, never BUILT_IN_ROLES. §10.4 makes the role a lease attribute,
            // and every DiscoveryNode this shell built used to say cluster_manager+data+ingest+warm
            // regardless -- a cluster-manager-role node that merely was not elected, which is exactly the
            // latent arming of cluster-manager-only paths §10.5 warns about. Data always: this node opens
            // IndexShards whatever it serves. Ingest only when it accepts writer activation. Never
            // cluster_manager, never core's search role -- that one means "hosts search replicas" and may
            // not be combined with data, which is not what a serverless search node is.
            this.localNode = new DiscoveryNode(
                nodeName,
                nodeEnvironment.nodeId(),
                new TransportAddress(InetAddress.getLoopbackAddress(), 0),
                discoveryAttributesFor(roles),
                discoveryRolesFor(roles),
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
                    discoveryAttributesFor(roles),
                    discoveryRolesFor(roles),
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
            // The search module's writeables, not an empty registry. The shard request cache stores a
            // query result serialised and reads it back through this registry on a hit -- so with an empty
            // one, the second identical size:0 aggregation on a shard failed with "Unknown NamedWriteable
            // category [AggregationBuilder]" while the first succeeded, which reads as a flaky server.
            namedWriteableRegistry(),
            threadPool,
            indexScopedSettings(),
            circuitBreakerService(),
            bigArrays(),
            scriptService(),
            clusterService,
            null,                                   // Client — no node client in phase 1
            // A no-op, as §6.2 instructs. The real one reads and writes on-disk index state under
            // gateway/, which is the persistence half of the control plane this shell does not build;
            // IndicesService wants an instance only to look up a deleted index's metadata on disk, and
            // "there is none" is the truthful answer here, because nothing ever wrote any.
            new NoopMetaStateService(nodeEnvironment, xContentRegistry),
            engineFactoryProviders(),
            directoryFactories(),
            emptyMap(),                             // composite directory factories — none
            emptyMap(),                             // data-format-aware store directory factories — none
            // Aggregations resolve a field's values source through this. It was null, which was invisible
            // for as long as aggregations were refused at the REST layer and became a NullPointerException
            // from inside QueryShardContext the moment one reached a shard.
            searchModule().getValuesSourceRegistry(),
            emptyMap(),                             // recovery state factories
            emptyMap(),                             // store factories
            null,                                   // remote directory factory — serverless supplies its own
            () -> null,                             // RepositoriesService
            null,                                   // SearchRequestStats
            null,                                   // RemoteStoreStatsTrackerFactory
            emptyMap(),                             // ingestion consumer factories
            org.opensearch.indices.pollingingest.IngestionPayloadDecoderRegistry.builder().build(),
            () -> null,                             // IngestService — streaming ingestion is not served here
            // The S0 spike used DefaultRecoverySettings, which lives in test/framework and is not
            // available to production code. A real shell builds these from settings.
            new org.opensearch.indices.recovery.RecoverySettings(settings, clusterSettings),
            new org.opensearch.common.cache.module.CacheModule(new ArrayList<>(), settings).getCacheService(),
            new org.opensearch.indices.RemoteStoreSettings(settings, clusterSettings),
            null,                                   // NodeCacheService
            null,                                   // CompositeIndexSettings
            null,                                   // replicator — segment replication is not served here
            null,                                   // segment replication stats provider
            // A real registry, not null: IndicesService#getEngineConfigFactory dereferences it on
            // every index it creates. With no data-format plugin installed it resolves to none,
            // which is the shell's case and the same answer core gets on an ordinary node.
            new org.opensearch.index.engine.dataformat.DataFormatRegistry(pluginsService)
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
            java.util.function.Function<
                org.opensearch.index.IndexSettings,
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
            return java.util.List.of(indexSettings -> java.util.Optional.of(writer));
        }
        final org.opensearch.index.engine.EngineFactory readOnly = config -> new org.opensearch.index.engine.ReadOnlyEngine(
            config,
            null,
            null,
            true,
            java.util.function.Function.identity(),
            false
        );
        return java.util.List.of(indexSettings -> java.util.Optional.of(readOnly));
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
     * Refuse to start on a plaintext transport.
     *
     * <p>Forwarded requests carry the deployment's transport secret; on a plaintext transport it crosses
     * the wire in the clear, and anyone who can read the wire can forward writes as a member. Off by
     * default because the filesystem-backed, loopback-only configuration every test runs is exactly the
     * case where a plaintext transport is fine; a deployment whose transport port is on a reachable
     * network sets it and gets a refusal instead of a warning.
     */
    public static final org.opensearch.common.settings.Setting<Boolean> REQUIRE_SECURE_TRANSPORT_SETTING =
        org.opensearch.common.settings.Setting.boolSetting(
            "serverless.transport.require_secure",
            false,
            org.opensearch.common.settings.Setting.Property.NodeScope
        );

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
        final String viewId = frozenViewIdOf(indexSettings);
        // The commit the reconciler is opening this shard at, if it is opening one right now. It read the
        // manifest -- or the view's record -- to decide how to open the shard, and arms it for exactly the
        // window in which this factory runs; taking it here is what makes a reader open one register read
        // rather than one per layer that needs the same answer.
        final ShardReconciler opening = reconciler;
        final java.util.Optional<org.opensearch.serverless.store.CommitManifest> armed = opening == null
            ? java.util.Optional.empty()
            : opening.pendingManifest(indexSettings.getIndex(), shardNumber);
        if (armed.isPresent()) {
            final String scope = (viewId != null ? viewId : storageUuidOf(indexSettings)) + "#" + shardNumber;
            return org.opensearch.serverless.store.BlockCacheDirectory.create(
                local,
                plane.blobStore(),
                shardBase,
                blockCache,
                scope,
                armed.get()
            );
        }
        // A frozen view opens the commit it froze, which is not the one the manifest register holds now --
        // that is the entire point of it. The view's identifier is this index's uuid, so the record is one
        // lookup away.
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
            // The uuid, not the name: a recreated index reuses its name and none of its bytes.
            storageUuidOf(indexSettings) + "#" + shardNumber
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
            null                                    // WorkloadGroupService — no workload management here
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
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.CatalogHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.StatsHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.AliasHandler(() -> metadataPlane, () -> this)));
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
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.StoredScriptHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.SearchPipelineHandler(() -> metadataPlane, () -> this))
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.SearchTemplateHandler(() -> this, () -> metadataPlane))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.RankEvalHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.RolloverHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.DataStreamHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.ValidateAndResolveHandler(() -> metadataPlane, () -> this))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.MultiSearchHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.ExplainHandler(() -> metadataPlane, () -> this)));
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
            { "/{index}/_termvectors/{id}", "term vectors are a per-shard Lucene detail this surface does not expose" },
            { "/_reindex", noReindex },
            { "/{index}/_update_by_query", noReindex },
            {
                "/_search/scroll",
                "scroll holds a search context open on a node; use a point in time (POST /{index}/_pit) with "
                    + "search_after, which is held in the object store and is not tied to one node" },
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
            // The nameless form is the one in the spec, and it used to match POST /_index_template/{name}
            // and store a live template called "_simulate".
            { "/_index_template/_simulate", simulate },
            { "/_component_template/_simulate/{name}", simulate },
            { "/_component_template/_simulate", simulate },
            // Found by comparing this surface against AWS OpenSearch Serverless and Elastic Cloud
            // Serverless: every one of these is an endpoint a real client reaches for, and every one of them
            // fell through to core's default 400 rather than the 501 D2 promises. Being absent is a
            // decision; looking like a typo is not.
            {
                "/_field_caps",
                "name an index or a prefix: GET /{index}/_field_caps answers for what it can reach, and "
                    + "asking every index in the deployment what fields it has is the inventory operation "
                    + "this design refuses" },
            {
                "/_cat/templates",
                "listing templates as a table is not routed; GET /_index_template returns them all, and "
                    + "GET /_index_template/{prefix}* narrows by name" },
            // Found by walking every path in rest-api-spec against a running node (ServerlessSpecCoverageTests)
            // rather than by hand: each of these is a sibling of a path already refused above -- the
            // index-less form, the {name} form, the {metric} form -- that the refusal did not cover, so it
            // fell through to core's default 400 "no handler found for uri" while its sibling answered 501
            // with a reason. Same reason, same answer, every spelling.
            { "/_cat/templates/{name}", "GET /_index_template/{name} answers for one template" },
            { "/_cat/aliases/{name}", "aliases are found by name here, not enumerated; GET /_alias/{name} answers for one" },
            { "/_cat/allocation/{nodeId}", noAllocator },
            { "/_cat/count/{index}", "GET /{index}/_count answers for one index, and takes a prefix pattern" },
            { "/_cat/fielddata/{fields}", "fielddata is per-shard memory this surface does not report" },
            { "/_cat/recovery/{index}", noAllocator },
            { "/_cat/segments/{index}", "per-shard Lucene detail is not exposed" },
            { "/_cat/shards/{index}", "GET /_cluster/health/{index}?level=shards reports one index's shards, bounded" },
            { "/_cat/snapshots/{repo}", "GET /_snapshot/{repo}/_all lists a repository's snapshots" },
            { "/_cat/thread_pool/{thread_pool_patterns}", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_cat/segment_replication", "there is no segment replication here: a shard's redundancy is the object store" },
            { "/_cat/segment_replication/{index}", "there is no segment replication here: a shard's redundancy is the object store" },
            { "/_cluster/state/{metric}", noGlobalState },
            { "/_cluster/state/{metric}/{index}", noGlobalState },
            { "/_cluster/stats/nodes/{nodeId}", noGlobalState },
            { "/_cluster/decommission/awareness", noAllocator },
            { "/_cluster/decommission/awareness/{name}/_status", noAllocator },
            { "/_cluster/decommission/awareness/{name}/{value}", noAllocator },
            { "/_cluster/voting_config_exclusions", noClusterManager },
            { "/_cluster/routing/awareness/weights", noAllocator },
            { "/_cluster/routing/awareness/{attribute}/weights", noAllocator },
            { "/_dangling/{index_uuid}", "there is no cluster state here, so an index exists exactly when its descriptor does" },
            { "/_script_context", "the context catalogue is not served; PUT /_scripts/{id}/{context} checks a script against one" },
            { "/_script_language", "the language catalogue is not served; painless and mustache are the languages here" },
            { "/{index}/_block/{block}", noClosedState },
            { "/_cache/clear", "there is no cluster-wide operation to clear a cache" },
            {
                "/_data_stream",
                "listing every data stream is the enumeration this design refuses; GET /_data_stream/{name} answers for "
                    + "one, and takes a prefix pattern" },
            {
                "/_data_stream/_modify",
                "a data stream here is an alias with a generation, so its backing indices are the alias's members "
                    + "and are changed by rolling it over, not by reassigning them underneath it" },
            { "/_data_stream/_stats", "store sizes are not reported here; GET /_data_stream/{name} lists a stream's backing indices" },
            {
                "/_data_stream/{name}/_stats",
                "store sizes are not reported here; GET /_data_stream/{name} lists a stream's backing indices" },
            { "/_template", "legacy templates are not served; composable templates are, at /_index_template" },
            { "/_template/{name}", "legacy templates are not served; composable templates are, at /_index_template/{name}" },
            { "/_flush", "there is no flush to ask for: durability is the write-ahead log" },
            { "/_refresh", "refresh is per-write here: pass refresh=true on the write that must be visible" },
            { "/_forcemerge", "merging is the shard writer's own decision" },
            { "/_recovery", noAllocator },
            { "/_segments", "per-shard Lucene detail is not exposed" },
            { "/_shard_stores", noAllocator },
            { "/_upgrade", "there is no upgrade to run: an index is served by whatever version serves it" },
            { "/{index}/_upgrade", "there is no upgrade to run: an index is served by whatever version serves it" },
            { "/_mapping", enumeration },
            { "/_mapping/field/{fields}", enumeration },
            { "/_settings", enumeration },
            { "/_settings/{name}", enumeration },
            { "/_stats/{metric}", indexEnumeration },
            {
                "/{index}/_stats/{metric}",
                "per-shard statistics are not exposed; GET /_serverless/stats answers for the node it is sent to" },
            { "/_validate/query", "name an index: GET /{index}/_validate/query answers for what it can reach" },
            {
                "/_ingest/processor/grok",
                "the grok pattern catalogue is not served; a pipeline naming a grok processor is compiled at PUT" },
            { "/_mtermvectors", "term vectors are a per-shard Lucene detail this surface does not expose" },
            { "/{index}/_mtermvectors", "term vectors are a per-shard Lucene detail this surface does not expose" },
            { "/{index}/_termvectors", "term vectors are a per-shard Lucene detail this surface does not expose" },
            { "/_nodes/hot_threads", "per-node diagnostics are not served; GET /_serverless/stats answers for the node it is sent to" },
            {
                "/_nodes/{nodeId}/hot_threads",
                "per-node diagnostics are not served; GET /_serverless/stats answers for the node it is sent to" },
            { "/_nodes/reload_secure_settings", "secure settings are read at start; there is no reload" },
            { "/_nodes/{nodeId}/reload_secure_settings", "secure settings are read at start; there is no reload" },
            { "/_nodes/usage", "feature usage is not counted here" },
            { "/_nodes/{nodeId}/usage", "feature usage is not counted here" },
            { "/_nodes/usage/{metric}", "feature usage is not counted here" },
            { "/_nodes/{nodeId}/usage/{metric}", "feature usage is not counted here" },
            { "/_nodes/{nodeId}/stats", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_nodes/stats/{metric}", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_nodes/{nodeId}/stats/{metric}", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_nodes/stats/{metric}/{index_metric}", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_nodes/{nodeId}/stats/{metric}/{index_metric}", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_rank_eval", "name an index: ranking evaluation belongs in the harness doing the evaluating" },
            { "/_reindex/{task_id}/_rethrottle", noReindex },
            { "/_update_by_query/{task_id}/_rethrottle", noReindex },
            { "/_delete_by_query/{task_id}/_rethrottle", "delete_by_query runs inline here and is not throttled" },
            { "/_remotestore/_restore", "the object store is the store; there is no separate remote store to restore from" },
            { "/_remotestore/stats/{index}", "the object store is the store; there is no separate remote store to report on" },
            { "/_remotestore/stats/{index}/{shard_id}", "the object store is the store; there is no separate remote store to report on" },
            { "/_search_shards", "shards here are placed by id and served by whichever node holds them; there is no allocation to report" },
            {
                "/{index}/_search_shards",
                "shards here are placed by id and served by whichever node holds them; there is no allocation to report" },
            {
                "/_snapshot/{repo}/_cleanup",
                "a repository here is a namespace in this deployment's own object store, swept by its own collector" },
            { "/_snapshot/{repo}/{snapshot}/_clone/{target}", "cloning a snapshot is not served; take a new one" },
            { "/_snapshot/_status", "every snapshot here is taken synchronously, so there is no in-progress status to report" },
            { "/_snapshot/{repo}/_status", "every snapshot here is taken synchronously, so there is no in-progress status to report" },
            {
                "/_snapshot/{repo}/{snapshot}/_status",
                "every snapshot here is taken synchronously, so there is no in-progress status to report" },
            {
                "/_snapshot/{repo}/{snapshot}/{index}/_status",
                "every snapshot here is taken synchronously, so there is no in-progress status to report" },
            { "/_tasks/_cancel", "there is no cluster-wide task registry" },
            { "/_tasks/{task_id}", "there is no cluster-wide task registry" },
            { "/_tasks/{task_id}/_cancel", "there is no cluster-wide task registry" },
            { "/_list/wlm_stats", "workload management is not part of this surface" },
            {
                "/{index}/_mapping/field/{fields}",
                "GET /{index}/_mapping returns the whole mapping, which is one register read either way" } }) {
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

    private final java.util.Map<String, long[]> appliedDescriptorVersions = new java.util.concurrent.ConcurrentHashMap<>();

    /** A head as last read, with when: the heartbeat's reads, reused by the same pass rather than repeated. */
    private record SeenHead(org.opensearch.serverless.metadata.ShardHead head, long atMillis) {
    }

    private final java.util.Map<String, SeenHead> recentHeads = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Records a head this node has just read, for the other readers in the same pass and for routing.
     *
     * <p>The heartbeat, the hint rebuild and the publish each read the same register within a second of
     * each other; a write to a shard this node does not hold read it again to learn the owner. One read
     * feeds all of them now. A hint is a hint: every user of it survives its being wrong, because the
     * owner refuses what it does not hold and the caller then reads the register.
     *
     * @param index the index
     * @param shard the shard
     * @param head what was read, or null if there was no head
     */
    public void noteHead(String index, int shard, org.opensearch.serverless.metadata.ShardHead head) {
        final long now = metadataPlane == null ? System.currentTimeMillis() : metadataPlane.clock().getAsLong();
        if (head == null) {
            recentHeads.remove(index + "#" + shard);
        } else {
            recentHeads.put(index + "#" + shard, new SeenHead(head, now));
        }
    }

    /**
     * Returns a head read within the given age, if this node has one.
     *
     * @param index the index
     * @param shard the shard
     * @param maxAgeMillis how old the read may be
     * @return the head, or empty if none that young was read
     */
    public java.util.Optional<org.opensearch.serverless.metadata.ShardHead> recentHead(String index, int shard, long maxAgeMillis) {
        final SeenHead seen = recentHeads.get(index + "#" + shard);
        if (seen == null) {
            return java.util.Optional.empty();
        }
        final long now = metadataPlane == null ? System.currentTimeMillis() : metadataPlane.clock().getAsLong();
        return now - seen.atMillis() <= maxAgeMillis ? java.util.Optional.of(seen.head()) : java.util.Optional.empty();
    }

    /**
     * Returns the node last seen owning a shard, if the sighting is no older than one lease TTL.
     *
     * <p>Bounded, where it used to be unbounded: a hint is invalidated by a refusal from the node it
     * names, and a node that is dead refuses nothing. A coordinator whose hint named a dead owner
     * forwarded every write there until something else happened to re-read the head. A lease is the
     * longest a node can be gone unnoticed, so a sighting older than one is not a hint any more; it is
     * re-read, at the cost of one register read per TTL per shard this node routes to but does not hold.
     *
     * @param index the index
     * @param shard the shard
     * @return the owner's node id, or empty if no head young enough has been read here
     */
    public java.util.Optional<String> ownerHint(String index, int shard) {
        final String key = index + "#" + shard;
        final SeenHead seen = recentHeads.get(key);
        if (seen == null) {
            return java.util.Optional.empty();
        }
        final org.opensearch.serverless.metadata.MetadataPlane plane = metadataPlane;
        if (plane != null && plane.clock().getAsLong() - seen.atMillis() > plane.leaseTtlMillis()) {
            recentHeads.remove(key, seen);
            return java.util.Optional.empty();
        }
        return java.util.Optional.ofNullable(seen.head().ownerNodeId());
    }

    /**
     * Forgets a hint an owner has just refused.
     *
     * @param index the index
     * @param shard the shard
     */
    public void forgetOwner(String index, int shard) {
        recentHeads.remove(index + "#" + shard);
    }

    /**
     * Applies mapping and settings changes made elsewhere to the shards this node holds open.
     *
     * <p>A mapping or settings update reached only the node that served it; every other node's open
     * shard kept the old one until it was reopened. One descriptor read per open index per heartbeat --
     * the heartbeat already reads a head per shard -- and a refresh only when a version moved.
     */
    private java.util.Map<String, java.util.Optional<IndexDescriptor>> refreshDescriptorsOfOpenIndices(
        org.opensearch.serverless.metadata.MetadataPlane plane
    ) {
        final java.util.Set<String> indices = new java.util.LinkedHashSet<>();
        for (org.opensearch.core.index.shard.ShardId shardId : reconciler.openShards()) {
            indices.add(shardId.getIndexName());
        }
        // What was read, for the caller: present, or definitely absent. An index whose read failed is
        // left out, so a transient error is never mistaken for a deletion.
        final java.util.Map<String, java.util.Optional<IndexDescriptor>> read = new java.util.LinkedHashMap<>();
        for (String indexName : indices) {
            try {
                final var descriptor = plane.describe(indexName);
                read.put(indexName, descriptor);
                if (descriptor.isEmpty()) {
                    continue;
                }
                // The copies the next projection is built from follow what was read, or a reader opened
                // after a mapping update would be projected with the mapping from before it.
                hosted.computeIfPresent(indexName, (k, v) -> descriptor.get());
                served.computeIfPresent(indexName, (k, v) -> descriptor.get());
                final long[] applied = appliedDescriptorVersions.computeIfAbsent(indexName, k -> new long[] { -1L, -1L });
                if (applied[0] != descriptor.get().mappingVersion()) {
                    reconciler.refreshMapping(indexName, descriptor.get());
                    applied[0] = descriptor.get().mappingVersion();
                }
                if (applied[1] != descriptor.get().settingsVersion()) {
                    reconciler.refreshSettings(indexName, descriptor.get());
                    applied[1] = descriptor.get().settingsVersion();
                }
            } catch (Exception e) {
                logger.warn("could not refresh the descriptor of " + indexName + " on this node's open shards", e);
            }
        }
        return read;
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
        // Say so, or refuse, when the transport is plaintext. Secure means either the secure netty
        // transport or a plugin that supplies the secure settings a TLS transport is built from.
        final String transportType = NetworkModule.TRANSPORT_TYPE_SETTING.get(settings);
        final boolean secureTransport = Netty4ModulePlugin.NETTY_SECURE_TRANSPORT_NAME.equals(transportType)
            || secureSettingsFactories().isEmpty() == false;
        if (secureTransport == false) {
            if (REQUIRE_SECURE_TRANSPORT_SETTING.get(settings)) {
                throw new IllegalStateException(
                    "transport.type is ["
                        + transportType
                        + "] and "
                        + REQUIRE_SECURE_TRANSPORT_SETTING.getKey()
                        + " is true: forwarded requests would carry the deployment secret in the clear"
                );
            }
            logger.warn(
                "transport.type is [{}]: forwarded requests carry the deployment secret in the clear; install a SecureSettingsFactory"
                    + " plugin or keep the transport port off every reachable network",
                transportType
            );
        }
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
        final java.util.List<Object> services = java.util.List.of(
            new org.opensearch.action.support.ActionFilters(new java.util.HashSet<>(plugins.actionFilters())),
            transportService,
            transportService.getTaskManager(),
            threadPool,
            client(),
            clusterService,
            settings,
            namedWriteableRegistry(),
            indexNameExpressionResolver(),
            circuitBreakerService(),
            indicesService
        );
        // Each action's constructor is resolved against the node's services and the components *its own*
        // plugin built -- never another plugin's. The aggregate list used to be offered to every action,
        // so a plugin could take a constructor argument of a type only some other plugin's component
        // satisfied, and get that plugin's private object handed to it by reflection.
        return PluginActions.build(actionPlugins, plugin -> {
            final java.util.List<Object> available = new java.util.ArrayList<>(
                plugins.componentsOf((org.opensearch.plugins.Plugin) plugin)
            );
            available.addAll(services);
            return available;
        });
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
        // Every shard's term, not just this one's: the view's IndexService is created once and updated as
        // each shard opens, and metadata that knew only one shard's term would push the others back to
        // zero.
        final java.util.Map<Integer, Long> terms = new java.util.HashMap<>();
        pit.shards().forEach((shard, commit) -> terms.put(shard, commit.term()));
        if (reconciler.heldShards().size() >= admission.maxShardsHeld() && admission.makeRoom() == false) {
            throw new IllegalStateException(
                "this node holds " + reconciler.heldShards().size() + " shards, the cap; the view was not opened here"
            );
        }
        // The reconciler checks that the view is of this incarnation of the index and that it froze this
        // shard; an IllegalArgumentException saying "no longer held" is the REST layer's 404.
        return reconciler.openFrozenReader(
            descriptor.toIndexMetadata(terms),
            pit,
            shardNumber,
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
        // What this node hosts as a writer is exactly what it was handed; the view is that plus whatever
        // it serves as a reader, so a dual-role node's readers are not projected away by a writer sync
        // or its writers by a reader open. That used to happen: serveAsReader projected the reader set
        // alone, and the writer indices vanished from the applied state.
        hosted.clear();
        for (IndexDescriptor descriptor : descriptors) {
            hosted.put(descriptor.name(), descriptor);
        }
        for (ShardAssignment assignment : owned) {
            writerAssignments.put(shardKey(assignment.indexName(), assignment.shardId()), assignment);
        }
        final ClusterState view = projectView(java.util.List.of(), owned);
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
        final org.opensearch.serverless.metadata.Truth truth = plane.truthFor(localNode.getId(), localNode.getEphemeralId());

        // Phase 4 has only writer shards, and a node that does not accept writer activation must not
        // open one. Phase 5 adds reader shards, at which point a search-only node opens those instead of
        // opening nothing -- so this gate is on the role, not on "has any role".
        final java.util.Collection<ShardAssignment> assignable = roles.contains(ROLE_INGEST) ? truth.assignments() : java.util.List.of();
        final java.util.Set<org.opensearch.core.index.shard.ShardId> opened = applyTruth(truth.descriptors(), assignable);

        // Closing here is NOT the rule §5.2 forbids, but only because of what the close is keyed on --
        // and it used to be keyed on the wrong thing. Truth's assignment list is derived from a listing
        // of this node's claim blobs, and RegisterMap says of that listing what the RFC says of every
        // listing: a hint, not truth. A claim blob whose write failed after the head was won, or a
        // listing that lagged, made truth omit a shard whose head still named this node, and the old
        // loop released it on that evidence alone. So a shard the listing does not vouch for is judged
        // by its head, read directly: only "the head no longer names this node" closes it. That is the
        // §5.2 rule as written. Readers are skipped outright -- they hold no head and no claim, and on a
        // dual-role node every writer activation used to evict every warm reader through this loop.
        final java.util.Set<String> stillOwned = new java.util.HashSet<>();
        for (ShardAssignment assignment : assignable) {
            stillOwned.add(shardKey(assignment.indexName(), assignment.shardId()));
        }
        for (org.opensearch.core.index.shard.ShardId held : reconciler.openShards()) {
            if (reconciler.readerShards().contains(held) || stillOwned.contains(shardKey(held.getIndexName(), held.id()))) {
                continue;
            }
            final java.util.Optional<org.opensearch.serverless.metadata.ShardHead> head = plane.heads()
                .read(held.getIndexName(), held.id());
            noteHead(held.getIndexName(), held.id(), head.orElse(null));
            if (namesThisNode(head, held)) {
                // The listing lagged, or the claim was never written. The head is what decides, and it
                // says this shard is ours; keep serving it.
                continue;
            }
            releaseLost(plane, held, "the shard-head no longer names " + nodeName, true);
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
        adopt(plane);
        reconciler.setSegmentPublishers(id -> plane.segmentPublisher(id.getIndexName(), id.getIndex().getUUID(), id.id()));
        reconciler.setWalStores(id -> plane.walStore(id.getIndexName(), id.getIndex().getUUID(), id.id()));
        {
            // The lease first, and it has to be first. Under batched liveness a shard-head is not
            // self-describing: it names an owner, and whether that ownership is live is a fact stored in
            // the owner's node lease. A node that acquired a shard before publishing a lease would hold a
            // head that every peer reads as dead, and the shard would be stolen out from under it
            // immediately -- with both nodes believing they had won it fairly.
            //
            // The head's own stamp is a floor that covers this window, but only just: publishing the
            // lease first is what makes the invariant hold rather than merely usually hold.
            //
            // And with the lapsed-lease guard, the same one the heartbeat applies: a node whose lease
            // lapsed lets go of every writer shard before it renews, or this renewal would reopen the
            // write path on shards a successor may already have sealed.
            renewLease(plane);
        }
        // The incarnation being activated, so a head left by a deleted index of the same name is not
        // mistaken for a live owner of this one.
        final java.util.Optional<IndexDescriptor> described = plane.describe(indexName);
        if (described.isEmpty()) {
            return java.util.Optional.empty();
        }
        final org.opensearch.serverless.metadata.Acquisition acquisition = plane.activate(
            indexName,
            shardNumber,
            localNode.getId(),
            localNode.getEphemeralId(),
            described.get().uuid()
        );
        if (acquisition.acquired() == false) {
            // The winner's head is the freshest routing fact this node has, and it was paid for: a
            // write for this shard forwards there without another read.
            noteHead(indexName, shardNumber, acquisition.head());
            return java.util.Optional.empty();
        }
        // Open the one shard just won, from the head and descriptor already in hand. This used to run
        // the full sync -- a listing of this node's claims and one head read per claim -- so taking one
        // shard cost a node holding a thousand a thousand reads, and a failover storm cost the square of
        // that. The full sync still runs from the backstop, rarely, for what only it can find.
        final ShardAssignment assignment = new ShardAssignment(indexName, shardNumber, acquisition.head().term());
        hosted.put(indexName, described.get());
        writerAssignments.put(shardKey(indexName, shardNumber), assignment);
        try {
            final ClusterState view = projectView(java.util.List.of(described.get()), java.util.List.of(assignment));
            applyLocalView(view, () -> 30_000L);
            reconciler.ensureOpen(view, java.util.List.of(assignment));
        } catch (Exception e) {
            // The head was won and the shard could not be opened. Held and unserved, that head would
            // refuse every other node's acquisition for as long as this node's lease lives, and nothing
            // on this node would try again: a shard nobody serves and nobody is looking for. Give the
            // head back, so the next node to ask -- possibly this one -- acquires cleanly.
            writerAssignments.remove(shardKey(indexName, shardNumber));
            try {
                plane.heads().release(indexName, shardNumber, localNode.getId(), assignment.term());
                plane.forgetAssignment(localNode.getId(), indexName, shardNumber);
            } catch (Exception giveBack) {
                e.addSuppressed(giveBack);
            }
            throw e;
        }
        final IndexMetadata metadata = clusterService.state().metadata().index(indexName);
        return metadata == null
            ? java.util.Optional.empty()
            : java.util.Optional.of(new org.opensearch.core.index.shard.ShardId(metadata.getIndex(), shardNumber));
    }

    /**
     * Renews this node's lease and lets go of anything it has lost: {@link #renewLease} then
     * {@link #verifyHeads}, as one pass.
     *
     * <p>This is the whole of failure detection, and it runs on the node that might be failing rather
     * than on one watching it. A node whose process is paused, partitioned or dead simply stops calling
     * this, its leases lapse, and another node acquires by compare-and-swap. Nothing needs to agree that
     * it died.
     *
     * <p>A lease found lapsed suspends acknowledgement until the verification has read every head; see
     * {@link #renewLease}. The two halves are separable, and the scheduler separates them: the lease is renewed on its own
     * timer, and the head scan -- one read per held writer shard -- runs after the next renewal is
     * already on the clock, so a node holding many shards on a slow store cannot lapse behind its own
     * scan. This method is the composition, for callers that drive a node by hand.
     *
     * @param plane the metadata plane
     * @return the shards released because this node no longer owns them
     * @throws Exception if the metadata plane cannot be read or written
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> heartbeat(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        ensureStarted();
        final java.util.Set<org.opensearch.core.index.shard.ShardId> released = new java.util.LinkedHashSet<>(renewLease(plane));
        released.addAll(verifyHeads(plane));
        return released;
    }

    /**
     * Renews this node's lease -- and, if the lease had already lapsed, re-verifies every held head
     * before a single write can be acknowledged again.
     *
     * <p><b>The rule this makes explicit: a node whose own lease lapsed can prove nothing about the
     * shards it holds until it has renewed and re-read their heads.</b> The moment its lease is expired
     * by its own clock (less the skew margin, {@code BlobLeaseMembership#selfLeaseLapsedAt}), any other
     * node is entitled to take any of its shards and seal their logs, and this node cannot tell from
     * here which ones were taken. Renewing first and finding out afterwards -- which is what this did --
     * reopened the write path on every shard for the length of the head scan: a coordinator with a
     * stale hint forwarded a write, the lease was valid again, the record landed behind a successor's
     * seal, and the client was told 201. Nobody replays a record behind a seal.
     *
     * <p>So a lapse <em>suspends acknowledgement</em> ({@link #ownershipUnverified}) before the renewal,
     * and the suspension is lifted only by a head verification that started after the renewal and read
     * every head. Between the two, every write is refused before it is applied. A shard whose head still
     * names this node is kept -- nobody took it, and closing it would move it for no reason; one whose
     * head names another node is released. Once the lease is live again nothing can be taken, so what the
     * verification found is what this node holds.
     *
     * <p>Heads this node lost on the write path ({@link #ensureOwnLeaseIsStillValid}) are given back in
     * the register here, keyed on the term they were lost at, so a closed shard does not sit naming a node
     * that is not serving it once the lease is live again. A give-back the store refused is retried on the
     * next renewal.
     *
     * <p>Always, not only under §7's batching. Under batching this is what keeps the node's shards
     * alive; in either mode it is what keeps the node addressable, since peers resolve a forwarding
     * target from its lease. One write per renewal per node.
     *
     * @param plane the metadata plane
     * @return the writer shards released because the verification after a lapse found them taken
     * @throws Exception if the lease cannot be renewed
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> renewLease(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        ensureStarted();
        adopt(plane);
        final java.util.Set<org.opensearch.core.index.shard.ShardId> released = new java.util.LinkedHashSet<>();
        final long now = plane.clock().getAsLong();
        final boolean lapsed = plane.membership().selfLeaseLapsedAt(now);
        if (lapsed) {
            // Before the renewal, never after: from here until a verification that started after the
            // renewal completes, nothing is acknowledged. The epoch is what tells a verification that
            // began before this moment from one that began after it.
            lapseEpoch.incrementAndGet();
            ownershipUnverified = true;
            logger.info(
                "this node's lease lapsed at {} by its own clock (now {}); acknowledgement is suspended until every held head is re-read",
                plane.membership().ownExpiresAtMillis() - plane.membership().skewMarginMillis(),
                now
            );
        }
        for (java.util.Map.Entry<org.opensearch.core.index.shard.ShardId, Long> lost : new java.util.ArrayList<>(
            headsToGiveBack.entrySet()
        )) {
            try {
                plane.heads().release(lost.getKey().getIndexName(), lost.getKey().id(), localNode.getId(), lost.getValue());
                headsToGiveBack.remove(lost.getKey(), lost.getValue());
            } catch (java.io.IOException e) {
                logger.warn("could not give back the head of " + lost.getKey() + "; will retry on the next renewal", e);
            }
        }
        synchronized (leaseRenewalLock) {
            renewOwnLease(plane);
        }
        if (lapsed) {
            // Now, not on the next timer: the suspension lasts exactly as long as this scan.
            released.addAll(verifyHeads(plane));
        }
        return released;
    }

    /**
     * Reads the head of every writer shard this node holds and lets go of the ones it has lost.
     *
     * <p>The release half of failure detection. A node that has lost a shard must stop serving it, and
     * this is where it finds out — a head that no longer names this node means someone else won it.
     * Also where a shard of a deleted index is let go, where the head was never deleted: the descriptor
     * is read once per open index anyway, and a shard whose index is tombstoned or recreated under a
     * different uuid is an orphan whatever its head says.
     *
     * <p><b>Bounded and parallel.</b> One register read per held writer shard, in {@link #HEAD_READ_LANES}
     * lanes on the fan-out pool once there are more shards than lanes, so a node holding a thousand pays
     * a thousand reads in a hundred-odd round trips rather than a thousand. Single-flight: a pass that
     * finds one already running returns nothing, rather than interleaving two sets of releases.
     *
     * @param plane the metadata plane
     * @return the shards released because this node no longer owns them
     * @throws Exception if a head could not be read; the shards whose heads were read are still judged
     */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> verifyHeads(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        ensureStarted();
        adopt(plane);
        synchronized (headVerificationLock) {
            return verifyHeadsExclusively(plane);
        }
    }

    private java.util.Set<org.opensearch.core.index.shard.ShardId> verifyHeadsExclusively(
        org.opensearch.serverless.metadata.MetadataPlane plane
    ) throws Exception {
        final java.util.Set<org.opensearch.core.index.shard.ShardId> released = new java.util.LinkedHashSet<>();
        // Taken before any read: a lapse that happens during this pass bumps it, and this pass's reads
        // then predate the renewal and may not lift the suspension.
        final long epoch = lapseEpoch.get();

        // First, shards whose log could not be appended to. Reaching this point means the store answered
        // a lease renewal, so it is time to reopen them from their logs: close, and ask for activation,
        // which re-acquires at a bumped term, seals, and replays -- without the operation the failed
        // append left in the engine, which is the state the caller was told about.
        for (org.opensearch.core.index.shard.ShardId shardId : new java.util.ArrayList<>(writeFenced)) {
            if (reconciler.shard(shardId) == null) {
                writeFenced.remove(shardId);
                continue;
            }
            releaseLost(plane, shardId, "reopening from its log after a failed append", false);
            released.add(shardId);
            signals.ownershipDoubted(shardId.getIndexName(), shardId.id());
        }

        final java.util.Map<String, java.util.Optional<IndexDescriptor>> descriptors = refreshDescriptorsOfOpenIndices(plane);

        final java.util.List<org.opensearch.core.index.shard.ShardId> writers = new java.util.ArrayList<>();
        for (org.opensearch.core.index.shard.ShardId shardId : reconciler.openShards()) {
            if (reconciler.readerShards().contains(shardId) == false) {
                writers.add(shardId);
            }
        }
        final java.util.Map<
            org.opensearch.core.index.shard.ShardId,
            java.util.Optional<org.opensearch.serverless.metadata.ShardHead>> heads = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.Map<org.opensearch.core.index.shard.ShardId, java.io.IOException> failures =
            new java.util.concurrent.ConcurrentHashMap<>();
        readHeads(plane, writers, heads, failures);

        for (org.opensearch.core.index.shard.ShardId shardId : writers) {
            final java.util.Optional<org.opensearch.serverless.metadata.ShardHead> head = heads.get(shardId);
            if (head == null) {
                // The read failed. Not evidence of anything; judged on the next pass.
                continue;
            }
            // The head still needs reading, because losing a shard is something only the head can tell
            // us -- but it is a read, not a write, and that is the whole saving. This used to have an
            // else-branch that renewed each head individually; that mode is gone.
            noteHead(shardId.getIndexName(), shardId.id(), head.orElse(null));
            final java.util.Optional<IndexDescriptor> descriptor = descriptors.get(shardId.getIndexName());
            if (descriptor != null && (descriptor.isEmpty() || descriptor.get().uuid().equals(shardId.getIndex().getUUID()) == false)) {
                // The index is gone, or has been recreated under another uuid, and this shard's head
                // was not deleted with it -- a delete that crashed between the tombstone and its head
                // deletes. The head may well still name this node; it is an orphan regardless.
                releaseLost(plane, shardId, "its index was deleted or recreated; this shard belongs to a previous incarnation", true);
                released.add(shardId);
                continue;
            }
            if (namesThisNode(head, shardId)) {
                continue;
            }
            releaseLost(plane, shardId, "lease lost: the shard-head no longer names " + nodeName, true);
            released.add(shardId);
        }
        if (failures.isEmpty() == false) {
            throw failures.values().iterator().next();
        }
        if (ownershipUnverified && lapseEpoch.get() == epoch) {
            // Every head this node holds was read after the renewal that followed the lapse, and each
            // one either still names this node or has been let go. Nothing can be taken from a node with
            // a live lease, so acknowledgement is safe again.
            ownershipUnverified = false;
            logger.info("every held head re-read after the lapse; acknowledgement resumes");
        }
        return released;
    }

    /**
     * Reads the given heads, in lanes on the fan-out pool once there are more than fit in one.
     *
     * <p>Each lane's reads are issued together and waited for together, so the store sees at most
     * {@link #HEAD_READ_LANES} reads from this pass at once. A pool that refuses the work runs the read
     * inline instead, which is slower and correct.
     */
    private void readHeads(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        java.util.List<org.opensearch.core.index.shard.ShardId> shards,
        java.util.Map<org.opensearch.core.index.shard.ShardId, java.util.Optional<org.opensearch.serverless.metadata.ShardHead>> heads,
        java.util.Map<org.opensearch.core.index.shard.ShardId, java.io.IOException> failures
    ) throws InterruptedException {
        if (shards.size() <= HEAD_READ_LANES) {
            for (org.opensearch.core.index.shard.ShardId shardId : shards) {
                try {
                    heads.put(shardId, plane.heads().read(shardId.getIndexName(), shardId.id()));
                } catch (java.io.IOException e) {
                    failures.put(shardId, e);
                }
            }
            return;
        }
        final java.util.concurrent.Executor executor = threadPool.executor(FANOUT_POOL);
        for (int start = 0; start < shards.size(); start += HEAD_READ_LANES) {
            final java.util.List<org.opensearch.core.index.shard.ShardId> lane = shards.subList(
                start,
                Math.min(shards.size(), start + HEAD_READ_LANES)
            );
            final CountDownLatch done = new CountDownLatch(lane.size());
            for (org.opensearch.core.index.shard.ShardId shardId : lane) {
                final Runnable read = () -> {
                    try {
                        heads.put(shardId, plane.heads().read(shardId.getIndexName(), shardId.id()));
                    } catch (java.io.IOException e) {
                        failures.put(shardId, e);
                    } catch (RuntimeException e) {
                        failures.put(shardId, new java.io.IOException("could not read the head of " + shardId, e));
                    } finally {
                        done.countDown();
                    }
                };
                try {
                    executor.execute(read);
                } catch (RuntimeException rejected) {
                    read.run();
                }
            }
            done.await();
        }
    }

    /** Whether a head names this node, this incarnation of it, and this incarnation of the index. */
    private boolean namesThisNode(
        java.util.Optional<org.opensearch.serverless.metadata.ShardHead> head,
        org.opensearch.core.index.shard.ShardId shardId
    ) {
        return head.isPresent() && localNode.getId().equals(head.get().ownerNodeId())
        // This incarnation's, not a previous one's under the same node id.
            && (head.get().ownerEphemeralId() == null || localNode.getEphemeralId().equals(head.get().ownerEphemeralId()))
            // And for this index, not a deleted one of the same name.
            && (head.get().indexUuid() == null || head.get().indexUuid().equals(shardId.getIndex().getUUID()));
    }

    /**
     * Closes a writer shard this node no longer holds, under its fence, and tidies what referred to it.
     *
     * @param plane the metadata plane
     * @param shardId the shard
     * @param reason why, for the log
     * @param forgetClaim whether to drop this node's claim blob too; not for a shard that is about to be
     *        re-acquired by this node
     */
    private void releaseLost(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        org.opensearch.core.index.shard.ShardId shardId,
        String reason,
        boolean forgetClaim
    ) throws Exception {
        underShardFence(shardId, () -> {
            reconciler.releaseShard(shardId, reason);
            return null;
        });
        writeFenced.remove(shardId);
        forgetShardRecords(shardId);
        if (forgetClaim) {
            // Drop the claim too. Leaving it is safe -- it is verified on read -- but a node that has
            // churned through shards for a year should not list a year of them to find today's.
            try {
                plane.forgetAssignment(localNode.getId(), shardId.getIndexName(), shardId.id());
            } catch (java.io.IOException e) {
                logger.warn("could not forget the claim on " + shardId + "; it costs one head read per resync until it is", e);
            }
        }
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

        // Readers this node has since let go of are dropped from the record before this one is added,
        // or a search node that cycles through many distinct readers carries every one it ever opened
        // into every reader open for the rest of its life: the N-th open cost O(N) and descriptors of
        // deleted indices stayed projected forever.
        pruneReaderBookkeeping();
        served.put(indexName, descriptor);
        readerShards.add(new java.util.AbstractMap.SimpleEntry<>(indexName, shardNumber));

        if (localShardOf(indexName, shardNumber) == null
            && reconciler.heldShards().size() >= admission.maxShardsHeld()
            && admission.makeRoom() == false) {
            // Readers used to be uncapped: a wide search burst could open thousands of shards on one
            // node. The same ceiling demand-driven writers have applies -- the configured one, counting
            // views -- and the same eviction makes room before this refuses.
            throw new IllegalStateException(
                "this node holds "
                    + reconciler.openShards().size()
                    + " shards, the cap; "
                    + indexName
                    + "["
                    + shardNumber
                    + "] was not opened"
            );
        }
        // One manifest read: the shard being opened. Every other reader's term is what was read when it
        // was opened. This used to re-read every held reader's manifest on every open -- the 500th reader
        // cost 500 reads before its own.
        final String openingKey = indexName + "#" + shardNumber;
        // Read once, and handed on: the reconciler and the directory factory each used to read the same
        // register again, so one reader open was three to four reads of one manifest.
        final java.util.Optional<org.opensearch.serverless.store.CommitManifest> manifest = plane.segmentPublisher(indexName, shardNumber)
            .readManifest();
        readerTerms.put(openingKey, manifest.map(org.opensearch.serverless.store.CommitManifest::term).orElse(1L));
        // The union of what this node serves and what it hosts, plus the reader being opened: a view
        // that described only the readers used to drop every writer index from the applied state on a
        // dual-role node.
        final ShardAssignment opening = new ShardAssignment(indexName, shardNumber, readerTerms.getOrDefault(openingKey, 1L));
        final ClusterState view = projectView(java.util.List.of(descriptor), java.util.List.of(opening));
        applyLocalView(view, () -> 30_000L);
        return reconciler.openReader(view, indexName, shardNumber, manifest.orElse(null));
    }

    /**
     * Forgets that this node served a shard as a reader, once it no longer does.
     *
     * @param shardId the reader shard that was closed
     */
    public void forgetReader(org.opensearch.core.index.shard.ShardId shardId) {
        readerShards.remove(new java.util.AbstractMap.SimpleEntry<>(shardId.getIndexName(), shardId.id()));
        readerTerms.remove(shardKey(shardId.getIndexName(), shardId.id()));
        pruneServedIndices();
    }

    /** Drops reader records for shards no longer open here as readers, and the descriptors nothing refers to. */
    private void pruneReaderBookkeeping() {
        final java.util.Set<org.opensearch.core.index.shard.ShardId> readers = reconciler.readerShards();
        final java.util.Set<String> openReaderKeys = new java.util.HashSet<>();
        for (org.opensearch.core.index.shard.ShardId reader : readers) {
            openReaderKeys.add(shardKey(reader.getIndexName(), reader.id()));
        }
        for (Map.Entry<String, Integer> entry : new java.util.ArrayList<>(readerShards)) {
            final String key = shardKey(entry.getKey(), entry.getValue());
            if (openReaderKeys.contains(key) == false) {
                readerShards.remove(entry);
                readerTerms.remove(key);
            }
        }
        pruneServedIndices();
    }

    private void pruneServedIndices() {
        final java.util.Set<String> referenced = new java.util.HashSet<>();
        for (Map.Entry<String, Integer> entry : readerShards) {
            referenced.add(entry.getKey());
        }
        served.keySet().removeIf(index -> referenced.contains(index) == false);
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
        // Under the shard's fence from before the engine applies the operation until after it is
        // acknowledged. See appendOrRelease for the interleaving this closes.
        final java.util.concurrent.locks.Lock fence = enterWritePath(shardId);
        try (org.opensearch.common.lease.Releasable guard = reconciler.writeGuard(shardId)) {
            return indexUnderFence(shardId, id, source, ifSeqNo, ifPrimaryTerm, requireAbsent, mayGrowMapping);
        } finally {
            fence.unlock();
        }
    }

    private WriteOutcome indexUnderFence(
        org.opensearch.core.index.shard.ShardId shardId,
        String id,
        String source,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent,
        boolean mayGrowMapping
    ) throws java.io.IOException {
        final var shard = reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot index into " + shardId + ": not open on " + nodeName);
        }
        if (reconciler.readerShards().contains(shardId)) {
            throw new java.io.IOException("cannot index into " + shardId + ": it is open as a reader");
        }
        refuseIfWriteFenced(shardId);
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
        // The local translog is never replayed by any open path -- a shard opens from a published commit
        // and its object-store log -- so this sync is not for durability; the log append above is. It is
        // for memory: the translog holds its write buffer, a 16 KB page charged to the request breaker,
        // until it is synced, and a node with a one-kilobyte request limit found every later search
        // refused by a page a write had left behind. One local fsync per acknowledged write is the price,
        // and it is not an object-store request.
        shard.sync();
        // The edge. Fired after the write is durable and applied, so a listener that publishes on it
        // can never publish a commit describing an operation the caller was not told about.
        signals.wrote(shardId);
        return new WriteOutcome(result.getSeqNo(), result.getTerm(), result.getVersion(), result.isCreated(), true);
    }

    /**
     * Appends a record to the log, and fences the shard against further writes if that fails.
     *
     * <p><b>Why a failed append cannot simply be reported to the caller.</b> By the time this runs, the
     * operation is already in the engine. Reporting failure while leaving it there would make the refusal
     * a lie: this node would go on serving the document and would eventually publish it, so a caller told
     * the write failed would find it present, permanently. The refusal is made true by never publishing
     * that engine state and rebuilding the shard from the log, which does not contain the operation.
     *
     * <p><b>Fenced rather than released, which is a brownout decision.</b> This used to close the shard
     * on the spot, and a store that was merely slow for a moment took every writer shard on the node
     * offline for reads too -- a fully cached copy answering "not open" to a get. §13's rule is that
     * reads degrade to staleness and writes to rejection. So the shard stays open and readable, refuses
     * every further write ({@link #refuseIfWriteFenced}), is skipped by publication, and is closed and
     * re-acquired from its log by the next heartbeat that reaches the store. Until then a read from this
     * node's own copy can see the operation the caller was told failed; that window is one renewal
     * interval, and the alternative was no reads at all.
     *
     * <p><b>The fence around this method is what makes acknowledgement safe against a voluntary
     * release.</b> Idle release, eviction and a clean shutdown all run publish, release the head, close.
     * With no exclusion against the write path, a write applied after the publish and appended after the
     * head release -- by which time a successor may have acquired and sealed the log -- was acknowledged
     * and never replayed. The caller holds the shard's read fence from before applying to after the
     * check below, and the release holds the write fence around the whole sequence, so the two cannot
     * interleave: a write either completes before the release begins, or finds no shard after it ends.
     *
     * @param shardId the shard being written to
     * @param shard the open shard
     * @param records the records to append, in order; empty is a no-op
     * @throws java.io.IOException if the append failed, after the shard has been fenced
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
            writeFenced.add(shardId);
            throw new java.io.IOException(
                "could not log "
                    + records.size()
                    + " operation(s) to "
                    + shardId
                    + "; the write was not acknowledged, the shard now refuses writes and will be reopened from its log",
                e
            );
        }
        // And once more after the PUT landed. A successor seals the log only after this node's lease has
        // expired, so a record written while the lease was valid on both sides of the PUT is ahead of
        // any seal. A writer that paused across the expiry may have landed its record behind one, and
        // the successor never replays it: that record must not be acknowledged, and it is not -- the
        // shard is released and the caller sees a failure it can retry against the new owner.
        ensureOwnLeaseIsStillValid(shardId);
        if (reconciler.shard(shardId) != shard) {
            // Closed underneath this write by a sibling write's lease failure. The record may or may not
            // be ahead of a seal; nothing here can tell, so it is not acknowledged.
            throw new java.io.IOException(
                "refusing to acknowledge a write to " + shardId + ": the shard was closed while it was in flight"
            );
        }
    }

    /**
     * Refuses a write to a shard whose log could not be appended to, before the engine applies it.
     *
     * @param shardId the shard
     * @throws java.io.IOException if the shard is write-fenced
     */
    private void refuseIfWriteFenced(org.opensearch.core.index.shard.ShardId shardId) throws java.io.IOException {
        if (ownershipUnverified) {
            throw new java.io.IOException(
                "refusing to write to "
                    + shardId
                    + ": this node's lease lapsed and it has not yet re-read the shard-heads it holds; retry shortly"
            );
        }
        if (writeFenced.contains(shardId)) {
            throw new java.io.IOException(
                "refusing to write to "
                    + shardId
                    + ": its write-ahead log could not be appended to and the shard is being reopened from the log; reads are still served"
            );
        }
    }

    /**
     * Reports whether a writer shard is refusing writes after a failed log append.
     *
     * @param shardId the shard
     * @return true while the shard is fenced and awaiting a reopen
     */
    public boolean isWriteFenced(org.opensearch.core.index.shard.ShardId shardId) {
        return writeFenced.contains(shardId);
    }

    private final Map<String, Object> mappingGrowthLocks = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, java.util.Queue<org.opensearch.index.mapper.Mapping>> pendingMappingAdditions =
        new java.util.concurrent.ConcurrentHashMap<>();

    /** How many compare-and-swaps one mapping growth attempts before failing the write. */
    private static final int MAPPING_GROWTH_ATTEMPTS = 5;

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
        // Coalesced, per index, on this node. This is the one register write a data path performs, and
        // it used to be one compare-and-swap per document that named a new field: a bulk of a thousand
        // such documents across the write pool was a thousand swaps on one register, of which one won
        // per round and the rest re-read and retried, four times each, and then failed the write. Now
        // every thread queues what it needs and the first to take the index's monitor swaps once for
        // all of them; a thread that finds its addition already drained by that leader has nothing left
        // to do. The rate is bounded by the field limit, which is the argument for allowing the write
        // here at all.
        pendingMappingAdditions.computeIfAbsent(indexName, k -> new java.util.concurrent.ConcurrentLinkedQueue<>()).add(addition);
        synchronized (mappingGrowthLocks.computeIfAbsent(indexName, k -> new Object())) {
            final java.util.Queue<org.opensearch.index.mapper.Mapping> queue = pendingMappingAdditions.get(indexName);
            final java.util.List<org.opensearch.index.mapper.Mapping> batch = new java.util.ArrayList<>();
            org.opensearch.index.mapper.Mapping next;
            while ((next = queue.poll()) != null) {
                batch.add(next);
            }
            if (batch.isEmpty()) {
                // A leader that ran while this thread waited took this thread's addition into its own
                // swap and applied the result here; the shard already has the field.
                return;
            }
            try {
                growMappingBy(plane, indexName, batch);
            } catch (Exception e) {
                // Whatever this leader was carrying for others goes back on the queue, so the next
                // thread in tries again rather than every waiter returning as though it had succeeded.
                queue.addAll(batch);
                throw e;
            }
        }
    }

    /** One index's mapping grown by a batch of additions: read, merge all of them, swap once, retry with backoff. */
    private void growMappingBy(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        java.util.List<org.opensearch.index.mapper.Mapping> additions
    ) throws java.io.IOException {
        for (int attempt = 0; attempt < MAPPING_GROWTH_ATTEMPTS; attempt++) {
            if (attempt > 0) {
                // Exponential, jittered, capped: the register's other writers are other nodes doing the
                // same thing, and four immediate retries by every loser was a stampede that mostly lost.
                final long upper = Math.min(500L, 20L << Math.min(attempt, 6));
                try {
                    Thread.sleep(java.util.concurrent.ThreadLocalRandom.current().nextLong(1L, upper + 1L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.io.IOException("interrupted while growing the mapping of " + indexName, e);
                }
            }
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
                org.opensearch.index.mapper.DocumentMapper grown = null;
                for (org.opensearch.index.mapper.Mapping addition : additions) {
                    grown = mapperService.merge(
                        org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME,
                        new org.opensearch.common.compress.CompressedXContent(addition.toString()),
                        org.opensearch.index.mapper.MapperService.MergeReason.MAPPING_UPDATE
                    );
                }
                merged = unwrapMappingType(grown.mappingSource().string());
            } catch (Exception e) {
                throw new java.io.IOException("could not grow the mapping of " + indexName + " to fit the document", e);
            } finally {
                mapperService.close();
            }
            if (merged.equals(current.get().mapping())) {
                // Another node already grew it. The descriptor is right; this node's open shard is not,
                // and without this refresh every write carrying the field failed here until the shard
                // reopened.
                reconciler.refreshMapping(indexName, current.get());
                return;
            }
            final var updated = current.get().withMapping(merged);
            if (plane.updateDescriptor(updated, generation).isPresent()) {
                hosted.computeIfPresent(indexName, (k, v) -> updated);
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
        if (plane.membership().selfLeaseValidAt(now) && ownershipUnverified == false) {
            return;
        }
        // Remembered so the next renewal gives the head back in the register: a closed shard whose head
        // still names this node, once the lease is renewed, is a shard nobody serves and nobody can take.
        final var shard = reconciler.shard(shardId);
        if (shard != null) {
            headsToGiveBack.put(shardId, shard.getOperationPrimaryTerm());
        }
        reconciler.releaseShard(shardId, "this node's own lease expired at or before " + now);
        writeFenced.remove(shardId);
        writerAssignments.remove(shardKey(shardId.getIndexName(), shardId.id()));
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
        final java.util.concurrent.locks.Lock fence = enterWritePath(shardId);
        try (org.opensearch.common.lease.Releasable guard = reconciler.writeGuard(shardId)) {
            return deleteUnderFence(shardId, id, ifSeqNo, ifPrimaryTerm);
        } finally {
            fence.unlock();
        }
    }

    private WriteOutcome deleteUnderFence(org.opensearch.core.index.shard.ShardId shardId, String id, long ifSeqNo, long ifPrimaryTerm)
        throws java.io.IOException {
        final var shard = reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot delete from " + shardId + ": not open on " + nodeName);
        }
        if (reconciler.readerShards().contains(shardId)) {
            throw new java.io.IOException("cannot delete from " + shardId + ": it is open as a reader");
        }
        refuseIfWriteFenced(shardId);
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
        // The same edge a write raises: a deletion changes the shard as surely as an addition, and a
        // shard whose only recent change was a delete still needs publishing or the tombstone lives
        // nowhere but this node's disk and its log.
        // For the breaker, not for durability -- see index.
        shard.sync();
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
        final java.util.concurrent.locks.Lock fence = enterWritePath(shardId);
        try (org.opensearch.common.lease.Releasable guard = reconciler.writeGuard(shardId)) {
            return bulkOperationsUnderFence(shardId, batch);
        } finally {
            fence.unlock();
        }
    }

    private java.util.List<BulkOutcome> bulkOperationsUnderFence(
        org.opensearch.core.index.shard.ShardId shardId,
        java.util.List<BulkOperation> batch
    ) throws java.io.IOException {
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
        refuseIfWriteFenced(shardId);
        markUsed(shardId);

        // Applied first, then logged once for the whole batch -- the same reordering the single-document
        // path took, and the batch economics are unchanged: one PUT for the batch, not one per document.
        // Only operations that actually applied are logged, so replay never reproduces an item the caller
        // was told had failed.
        // Mapping growth is coalesced per batch, and request order is kept while it is. The first item
        // that needs a field the mapping lacks stops the pass: what it needs is grown once, and the pass
        // resumes from that item. It used to grow inline, per item, so a batch of a thousand documents
        // naming one new field was a thousand descriptor swaps on one register. Stopping at the first
        // such item rather than skipping it is what keeps the batch's order: an item deferred past a
        // later delete of the same id would resurrect the document the request said to remove.
        final BulkOutcome[] outcomes = new BulkOutcome[batch.size()];
        final java.util.List<org.opensearch.serverless.store.WalRecord> applied = new java.util.ArrayList<>(batch.size());
        final boolean[] grewFor = new boolean[batch.size()];
        int from = 0;
        while (from < batch.size()) {
            int deferredAt = -1;
            org.opensearch.index.mapper.Mapping required = null;
            for (int i = from; i < batch.size(); i++) {
                final BulkOperation item = batch.get(i);
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
                            outcomes[i] = conflicted(result)
                                ? BulkOutcome.conflicted(operation.id(), describe("deleting", result))
                                : BulkOutcome.failed(operation.id(), describe("deleting", result));
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
                        outcomes[i] = BulkOutcome.deleted(
                            operation.id(),
                            result.isFound(),
                            result.getSeqNo(),
                            result.getTerm(),
                            result.getVersion()
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
                        if (result.getResultType() == org.opensearch.index.engine.Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                            if (grewFor[i]) {
                                // The mapping was grown for this item once already, and it still does
                                // not fit: something other than a missing field, and growing again
                                // forever would hide it.
                                outcomes[i] = BulkOutcome.failed(operation.id(), describe("indexing", result));
                                continue;
                            }
                            deferredAt = i;
                            required = result.getRequiredMappingUpdate();
                            break;
                        }
                        if (result.getResultType() != org.opensearch.index.engine.Engine.Result.Type.SUCCESS) {
                            // A lost condition is a FAILURE carrying a VersionConflictEngineException rather
                            // than a thrown one, so the item has to be inspected rather than only its type
                            // reported -- otherwise a caller that asked a compare-and-swap question is told
                            // only that "indexing returned FAILURE", which is not an answer to it.
                            outcomes[i] = conflicted(result)
                                ? BulkOutcome.conflicted(operation.id(), describe("indexing", result))
                                : BulkOutcome.failed(operation.id(), describe("indexing", result));
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
                        outcomes[i] = BulkOutcome.indexed(
                            operation.id(),
                            result.getSeqNo(),
                            result.getTerm(),
                            result.getVersion(),
                            result.isCreated()
                        );
                    }
                } catch (Exception e) {
                    // One bad document does not fail the batch. A mapping conflict on item 40 is item 40's
                    // problem, and failing the other 99 would make a bulk request less useful than the loop
                    // it replaces.
                    final String message = e.getMessage() == null ? e.toString() : e.getMessage();
                    outcomes[i] = e instanceof org.opensearch.index.engine.VersionConflictEngineException
                        ? BulkOutcome.conflicted(operation.id(), message)
                        : BulkOutcome.failed(operation.id(), message);
                }
            }
            if (deferredAt < 0) {
                break;
            }
            // One growth for everything from here on that needs what this item needs -- which in the
            // common case, a batch of like documents, is all of it. Then the pass resumes at this item.
            grewFor[deferredAt] = true;
            try {
                growMapping(shardId, required);
                from = deferredAt;
            } catch (java.io.IOException e) {
                final String id = batch.get(deferredAt).record().id();
                final String message = e.getMessage() == null ? e.toString() : e.getMessage();
                // Contention on the descriptor is not this document's fault, and a retry writes it;
                // anything else about the growth is.
                outcomes[deferredAt] = message.contains("being changed concurrently")
                    ? BulkOutcome.retryable(id, message)
                    : BulkOutcome.failed(id, message);
                from = deferredAt + 1;
            }
        }

        appendOrRelease(shardId, shard, applied);
        // One local sync for the batch, for the reason index gives: it gives the translog's page back to
        // the request breaker, and costs no object-store request.
        shard.sync();
        // One edge for the batch, not one per document. The
        // edge is raised after everything is applied, so a publish it triggers describes the whole batch
        // or none of it.
        signals.wrote(shardId);
        return java.util.Arrays.asList(outcomes);
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
        private boolean retryable;
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

        /**
         * Records an operation that failed for a reason that will not recur if the caller simply tries
         * again: the index's mapping was being grown by someone else at the same moment.
         *
         * @param id the document id
         * @param reason why it failed
         * @return the outcome
         */
        public static BulkOutcome retryable(String id, String reason) {
            final BulkOutcome outcome = failed(id, reason);
            outcome.retryable = true;
            return outcome;
        }

        /**
         * Whether this item failed on contention rather than on its own content.
         *
         * <p>A 503 rather than a 400: nothing about the document was wrong, and a client that retries the
         * item gets it written. Separated from {@link #conflict()} because a lost condition must not be
         * retried blindly and this must.
         *
         * @return whether the caller should retry the item as it is
         */
        public boolean retryable() {
            return retryable;
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

    /**
     * Returns the search registry with rank-eval's metric parsers added.
     *
     * @return a registry that parses a ranking-evaluation body
     */
    public synchronized org.opensearch.core.xcontent.NamedXContentRegistry rankEvalXContentRegistry() {
        if (rankEvalRegistry == null) {
            final java.util.List<org.opensearch.core.xcontent.NamedXContentRegistry.Entry> entries = new java.util.ArrayList<>(
                searchModule().getNamedXContents()
            );
            entries.addAll(new org.opensearch.index.rankeval.RankEvalNamedXContentProvider().getNamedXContentParsers());
            rankEvalRegistry = new org.opensearch.core.xcontent.NamedXContentRegistry(entries);
        }
        return rankEvalRegistry;
    }

    private volatile org.opensearch.core.xcontent.NamedXContentRegistry rankEvalRegistry;

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
            // Mustache too, chosen the same way: search templates and rank-eval templates render through
            // it, and it is the one engine core registers for the template context.
            final org.opensearch.script.ScriptEngine mustache = new org.opensearch.script.mustache.MustacheModulePlugin().getScriptEngine(
                settings,
                org.opensearch.script.ScriptModule.CORE_CONTEXTS.values()
            );
            // Stored scripts come from a register rather than from cluster state; see StoredScripts.
            scriptService = new org.opensearch.serverless.script.ServerlessScriptService(
                settings,
                Map.of(painless.getType(), painless, mustache.getType(), mustache),
                org.opensearch.script.ScriptModule.CORE_CONTEXTS,
                new org.opensearch.serverless.script.StoredScripts(() -> metadataPlane)
            );
        }
        return scriptService;
    }

    /**
     * Returns this node's copy of the stored scripts.
     *
     * @return the cache
     */
    public org.opensearch.serverless.script.StoredScripts storedScripts() {
        return ((org.opensearch.serverless.script.ServerlessScriptService) scriptService()).stored();
    }

    private volatile org.opensearch.search.pipeline.ServerlessSearchPipelines searchPipelines;

    /**
     * Returns the search-pipeline compiler, built over the common module's processor factories.
     *
     * @return the compiler
     */
    public synchronized org.opensearch.search.pipeline.ServerlessSearchPipelines searchPipelines() {
        if (searchPipelines == null) {
            searchPipelines = new org.opensearch.search.pipeline.ServerlessSearchPipelines(
                new Environment(settings, null),
                scriptService(),
                indicesService().getAnalysis(),
                threadPool,
                searchXContentRegistry(),
                namedWriteableRegistry()
            );
        }
        return searchPipelines;
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
     * Returns the node's settings.
     *
     * @return the settings this node was started with
     */
    public Settings settings() {
        return settings;
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
        forgetShardRecords(shardId);
    }

    /**
     * Drops what this node remembered about a shard once it is no longer open here: the reader records
     * that would otherwise be re-projected on every open, the head it last read, and -- once no shard of
     * the index remains -- the descriptor versions it had applied.
     */
    private void forgetShardRecords(org.opensearch.core.index.shard.ShardId shardId) {
        if (reconciler.shard(shardId) != null) {
            return;
        }
        forgetReader(shardId);
        recentHeads.remove(shardId.getIndexName() + "#" + shardId.id());
        writerAssignments.remove(shardKey(shardId.getIndexName(), shardId.id()));
        if (reconciler.openShards().stream().noneMatch(open -> open.getIndexName().equals(shardId.getIndexName()))) {
            appliedDescriptorVersions.remove(shardId.getIndexName());
            hosted.remove(shardId.getIndexName());
        }
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

    private final java.util.Map<String, Long> readerTerms = new java.util.concurrent.ConcurrentHashMap<>();

    private org.opensearch.core.index.shard.ShardId localShardOf(String indexName, int shardNumber) {
        return reconciler.openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(indexName) && s.id() == shardNumber)
            .findFirst()
            .orElse(null);
    }

    private volatile org.opensearch.serverless.metadata.MetadataPlane.TransportSecrets transportSecrets;
    private volatile long transportSecretsReadAt = Long.MIN_VALUE;

    /**
     * Returns the transport secrets as last read, re-reading the register at most once per lease TTL and
     * on demand.
     *
     * <p>The bearer used to be read once and kept for the life of the process, so rotating it meant
     * restarting every node. The MAC design ({@code TransportAuthenticator}) verifies under the current
     * generation and, for one window, the previous one, and re-reads when it sees a generation it does
     * not know -- that is the {@code refresh} argument. The TTL bound on the cache is what makes a
     * rotation reach every node inside one lease even when nobody forwards it anything under the new
     * generation.
     *
     * @param forceReread true to re-read now -- a verifier that met a generation newer than its copy
     * @return the secrets, or null before a metadata plane is adopted
     * @throws java.io.UncheckedIOException if the register cannot be read
     */
    public org.opensearch.serverless.metadata.MetadataPlane.TransportSecrets transportSecrets(boolean forceReread) {
        final var plane = metadataPlane;
        if (plane == null) {
            return null;
        }
        org.opensearch.serverless.metadata.MetadataPlane.TransportSecrets cached = transportSecrets;
        final long now = plane.clock().getAsLong();
        if (cached == null || forceReread || now - transportSecretsReadAt > plane.leaseTtlMillis()) {
            try {
                cached = plane.transportSecrets();
                transportSecrets = cached;
                transportSecretsReadAt = now;
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
        return cached;
    }

    /**
     * Returns the secret this deployment's nodes present to one another on forwarded requests: the
     * current generation of {@link #transportSecrets(boolean)}.
     *
     * @return the secret, or null before a metadata plane is adopted
     */
    public String transportToken() {
        final var secrets = transportSecrets(false);
        return secrets == null ? null : secrets.current();
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
        final ClusterState toApply = withPeers(state);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        clusterService.getClusterApplierService()
            .onNewClusterState("serverless-projected-view", () -> toApply, new ClusterApplier.ClusterApplyListener() {
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

    /**
     * Adds every live member of the fleet to the view's nodes, as §10.5 describes: {@code DiscoveryNodes}
     * built from the membership view, not the local node alone.
     *
     * <p>Nothing in the data plane looks a peer up by id today, so this is a description rather than a
     * dependency; it is what makes {@code IndexShard}'s node lookups safe should a path ever name a peer,
     * and what lets a plugin reading the cluster state see the fleet. A lease whose address does not parse,
     * or that collides with one already added -- a restarted node whose old lease has not yet expired --
     * is skipped: membership is a hint, and a wrong hint here costs a missing entry, never a failed apply.
     */
    private ClusterState withPeers(ClusterState state) {
        MembershipSource source = membershipSource;
        if (source == null && metadataPlane != null) {
            source = metadataPlane.membership();
        }
        if (source == null || source.current().isEmpty()) {
            return state;
        }
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(state.nodes());
        boolean changed = false;
        for (org.opensearch.serverless.membership.NodeLease lease : source.current()) {
            if (lease.nodeId().equals(localNode.getId()) || state.nodes().nodeExists(lease.nodeId())) {
                continue;
            }
            try {
                nodes.add(peerNode(lease));
                changed = true;
            } catch (Exception e) {
                logger.debug("not describing {} in the local view: {}", lease.nodeId(), e.getMessage());
            }
        }
        return changed ? ClusterState.builder(state).nodes(nodes).build() : state;
    }

    /** A peer as the data plane would see it, from its lease: id, ephemeral id, address and roles all from there. */
    private static DiscoveryNode peerNode(org.opensearch.serverless.membership.NodeLease lease) throws Exception {
        final String address = lease.address();
        final int colon = address.lastIndexOf(':');
        final String host = address.substring(0, colon).replace("[", "").replace("]", "");
        final TransportAddress transportAddress = new TransportAddress(
            InetAddress.getByName(host),
            Integer.parseInt(address.substring(colon + 1))
        );
        return new DiscoveryNode(
            lease.name(),
            lease.nodeId(),
            lease.ephemeralId(),
            transportAddress.address().getHostString(),
            transportAddress.getAddress(),
            transportAddress,
            discoveryAttributesFor(lease.roles()),
            discoveryRolesFor(lease.roles()),
            Version.CURRENT
        );
    }

    /**
     * The data plane's roles for a node advertising the given lease roles: data always, ingest when it
     * accepts writer activation, never cluster_manager. See the constructor for why.
     *
     * @param leaseRoles the roles from the lease or the {@code serverless.roles} setting
     * @return the discovery roles
     */
    public static Set<DiscoveryNodeRole> discoveryRolesFor(Set<String> leaseRoles) {
        final java.util.SortedSet<DiscoveryNodeRole> out = new java.util.TreeSet<>();
        out.add(DiscoveryNodeRole.DATA_ROLE);
        if (leaseRoles.contains(ROLE_INGEST)) {
            out.add(DiscoveryNodeRole.INGEST_ROLE);
        }
        return Collections.unmodifiableSortedSet(out);
    }

    /** The lease roles as a node attribute, so the serverless role is readable off a {@code DiscoveryNode} too. */
    public static Map<String, String> discoveryAttributesFor(Set<String> leaseRoles) {
        return Map.of("serverless.roles", String.join(",", new java.util.TreeSet<>(leaseRoles)));
    }

    /**
     * The on-disk index-state service §6.2 says this shell never builds, as a no-op that satisfies
     * {@link IndicesService}.
     *
     * <p>Every method answers "nothing is stored here", which is true: no path in this process ever
     * writes index state to the gateway directory, so there is never anything to load, and the one
     * caller in the data plane -- verifying a deleted index's on-disk metadata -- is told there is none.
     */
    private static final class NoopMetaStateService extends MetaStateService {

        NoopMetaStateService(NodeEnvironment nodeEnvironment, NamedXContentRegistry registry) {
            super(nodeEnvironment, registry);
        }

        @Override
        public
            org.opensearch.common.collect.Tuple<org.opensearch.cluster.metadata.Manifest, org.opensearch.cluster.metadata.Metadata>
            loadFullState() {
            return new org.opensearch.common.collect.Tuple<>(
                org.opensearch.cluster.metadata.Manifest.empty(),
                org.opensearch.cluster.metadata.Metadata.EMPTY_METADATA
            );
        }

        @Override
        public IndexMetadata loadIndexState(org.opensearch.core.index.Index index) {
            return null;
        }

        @Override
        public org.opensearch.cluster.metadata.Manifest loadManifestOrEmpty() {
            return org.opensearch.cluster.metadata.Manifest.empty();
        }

        @Override
        public void writeManifestAndCleanup(String reason, org.opensearch.cluster.metadata.Manifest manifest) {}

        @Override
        public long writeIndex(String reason, IndexMetadata indexMetadata) {
            return -1L;
        }

        @Override
        public void cleanupIndex(org.opensearch.core.index.Index index, long currentGeneration) {}

        @Override
        public void unreferenceAll() {}

        @Override
        public void deleteAll() {}
    }

    // ---------------------------------------------------------------- shard fences and the local view

    private static String shardKey(String indexName, int shardNumber) {
        return indexName + "#" + shardNumber;
    }

    /**
     * Runs an action that gives a shard away while holding the shard's write fence.
     *
     * <p>Waits for every write in flight on the shard -- applied to the engine, appending to the log,
     * about to be acknowledged -- and keeps new ones out until the action returns. A write that arrives
     * during the action then finds whatever the action left: no shard, and it is refused. This is what
     * makes "publish, release the head, close" safe against the write path; see {@link #appendOrRelease}.
     *
     * <p><b>Two locks, two jobs, one order.</b> This fence guards the shard being <em>given away</em>;
     * the reconciler's write guard ({@code ShardReconciler#writeGuard}) guards a <em>commit</em> landing
     * between an apply and its append. The write path holds this fence's read side and then the guard's
     * read side; a release holds this fence's write side and, through the publish inside it, the guard's
     * write side around the flush; an ordinary publish holds only the guard's. Nothing that holds the
     * guard ever waits on this fence, so the two cannot deadlock, and a publish still overlaps with
     * writes everywhere but the flush.
     *
     * @param shardId the shard
     * @param action what to do while nothing can be acknowledged against the shard
     * @param <T> the action's result
     * @return the action's result
     * @throws Exception whatever the action throws
     */
    public <T> T underShardFence(org.opensearch.core.index.shard.ShardId shardId, java.util.concurrent.Callable<T> action)
        throws Exception {
        final java.util.concurrent.locks.ReentrantReadWriteLock fence = shardFences.computeIfAbsent(
            shardId,
            k -> new java.util.concurrent.locks.ReentrantReadWriteLock()
        );
        fence.writeLock().lock();
        try {
            return action.call();
        } finally {
            fence.writeLock().unlock();
            if (reconciler == null || reconciler.shard(shardId) == null) {
                // Closed: the fence goes with it, or a node that churns through shards keeps a lock per
                // shard it ever held. A writer that fetched this fence and locks it after this point
                // notices the swap in enterWritePath and starts over on the current one.
                shardFences.remove(shardId, fence);
            }
        }
    }

    /** Takes the read side of a shard's fence; the caller unlocks what is returned. */
    private java.util.concurrent.locks.Lock enterWritePath(org.opensearch.core.index.shard.ShardId shardId) {
        while (true) {
            final java.util.concurrent.locks.ReentrantReadWriteLock fence = shardFences.computeIfAbsent(
                shardId,
                k -> new java.util.concurrent.locks.ReentrantReadWriteLock()
            );
            fence.readLock().lock();
            if (shardFences.get(shardId) == fence) {
                return fence.readLock();
            }
            // The shard was closed, and possibly reopened, between fetching the fence and locking it; the
            // fence in the map is the one the next release will take, so that is the one to hold.
            fence.readLock().unlock();
        }
    }

    /**
     * Reports whether this node holds a shard as its writer, by evidence no older than the given age.
     *
     * <p>For the node a write was forwarded to. Being open here was the only check, and "open here" is
     * true of a shard whose head has named another node since the last heartbeat: the lease fences a
     * write only once the lapsed-lease rule in {@link #renewLease} holds, and the head is the truth the
     * rule approximates. The heartbeat notes every held head once per renewal, so with an age of one
     * renewal this costs no register read on a healthy node; a head older than that is read fresh, and
     * one that names another node closes the shard here on the spot.
     *
     * <p>A head that cannot be read is no evidence, and the answer is then no: a write needs the store
     * for its log append anyway, so refusing costs the caller one retry it was going to need.
     *
     * @param shardId the shard
     * @param maxHeadAgeMillis how old a noted head may be before it is re-read
     * @return true if the shard may accept a write on this node right now
     */
    public boolean holdsAsWriter(org.opensearch.core.index.shard.ShardId shardId, long maxHeadAgeMillis) {
        ensureStarted();
        if (reconciler.shard(shardId) == null || reconciler.readerShards().contains(shardId) || writeFenced.contains(shardId)) {
            return false;
        }
        if (ownershipUnverified) {
            // The lease lapsed and the heads have not been re-read since the renewal; see renewLease.
            return false;
        }
        final org.opensearch.serverless.metadata.MetadataPlane plane = metadataPlane;
        if (plane == null) {
            return true;
        }
        if (plane.membership().selfLeaseValidAt(plane.clock().getAsLong()) == false) {
            return false;
        }
        java.util.Optional<org.opensearch.serverless.metadata.ShardHead> head = recentHead(
            shardId.getIndexName(),
            shardId.id(),
            maxHeadAgeMillis
        );
        if (head.isEmpty()) {
            try {
                head = plane.heads().read(shardId.getIndexName(), shardId.id());
            } catch (java.io.IOException e) {
                logger.debug("could not read the head of {} to answer a forwarded write; refusing it", shardId);
                return false;
            }
            noteHead(shardId.getIndexName(), shardId.id(), head.orElse(null));
        }
        if (namesThisNode(head, shardId)) {
            return true;
        }
        try {
            releaseLost(
                plane,
                shardId,
                "a forwarded write found the shard-head naming " + head.map(h -> h.ownerNodeId()).orElse("nobody"),
                true
            );
        } catch (Exception e) {
            logger.warn("could not release " + shardId + " after its head stopped naming this node", e);
        }
        signals.ownershipDoubted(shardId.getIndexName(), shardId.id());
        return false;
    }

    /** Of two copies of one descriptor, the one with the newer mapping and settings. */
    private static IndexDescriptor newerOf(IndexDescriptor a, IndexDescriptor b) {
        if (b.mappingVersion() != a.mappingVersion()) {
            return b.mappingVersion() > a.mappingVersion() ? b : a;
        }
        return b.settingsVersion() >= a.settingsVersion() ? b : a;
    }

    /**
     * Projects the view this node should hold: every index it serves or hosts, with every open reader
     * and writer shard assigned here, plus whatever is being opened right now.
     *
     * <p>One projection for both roles. The reader path and the writer path each used to project their
     * own half and apply it, so on a dual-role node each open replaced the other's indices in the applied
     * state. Assignments for shards that are no longer open are pruned as they are met, so the record of
     * writer terms follows the reconciler rather than growing with every shard ever held.
     */
    private ClusterState projectView(java.util.Collection<IndexDescriptor> extra, java.util.Collection<ShardAssignment> opening)
        throws java.io.IOException {
        final Map<String, IndexDescriptor> descriptors = new java.util.LinkedHashMap<>();
        for (IndexDescriptor descriptor : served.values()) {
            descriptors.merge(descriptor.name(), descriptor, ServerlessNode::newerOf);
        }
        for (IndexDescriptor descriptor : hosted.values()) {
            descriptors.merge(descriptor.name(), descriptor, ServerlessNode::newerOf);
        }
        for (IndexDescriptor descriptor : extra) {
            descriptors.put(descriptor.name(), descriptor);
        }
        final Map<String, org.opensearch.core.index.shard.ShardId> openByKey = new java.util.HashMap<>();
        for (org.opensearch.core.index.shard.ShardId open : reconciler.openShards()) {
            openByKey.put(shardKey(open.getIndexName(), open.id()), open);
        }
        final java.util.Set<org.opensearch.core.index.shard.ShardId> readers = reconciler.readerShards();
        final Map<String, ShardAssignment> assignments = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : readerShards) {
            final String key = shardKey(entry.getKey(), entry.getValue());
            final org.opensearch.core.index.shard.ShardId open = openByKey.get(key);
            if (open != null && readers.contains(open)) {
                assignments.put(key, new ShardAssignment(entry.getKey(), entry.getValue(), readerTerms.getOrDefault(key, 1L)));
            }
        }
        for (Map.Entry<String, ShardAssignment> entry : new java.util.ArrayList<>(writerAssignments.entrySet())) {
            final org.opensearch.core.index.shard.ShardId open = openByKey.get(entry.getKey());
            if (open != null && readers.contains(open) == false) {
                assignments.put(entry.getKey(), entry.getValue());
            } else if (opening.stream().noneMatch(a -> shardKey(a.indexName(), a.shardId()).equals(entry.getKey()))) {
                writerAssignments.remove(entry.getKey(), entry.getValue());
            }
        }
        for (ShardAssignment assignment : opening) {
            assignments.put(shardKey(assignment.indexName(), assignment.shardId()), assignment);
        }
        return projector.project(descriptors.values(), assignments.values());
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
