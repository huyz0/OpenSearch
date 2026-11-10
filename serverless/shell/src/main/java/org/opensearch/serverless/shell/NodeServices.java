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
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.MetaStateService;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rest.RestController;
import org.opensearch.search.SearchService;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Netty4ModulePlugin;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

/**
 * The core services a serverless node is built from, and the one place that calls their constructors.
 *
 * <p>Two kinds live here. The data plane's own services ({@link IndicesService}, {@link SearchService}, the cluster
 * service, the network module and the plugin loader) are built once, in order, by {@link ServerlessNode}'s
 * constructor through the {@code build*} methods. The shared collaborators those services and the plugins are
 * handed (breakers, the array allocator, the script service, the search registries, the pipeline compilers, the
 * client) are built lazily, the first time anything asks, and memoized.
 *
 * <p>Upstream constructor changes to these core classes break this file and no other.
 */
final class NodeServices implements Closeable {

    private final Settings settings;
    private final ServerlessPlugins plugins;
    private final ThreadPool threadPool;
    private final Supplier<MetadataPlane> metadataPlane;
    private final Supplier<ServerlessNode> node;
    private final Supplier<IndicesService> indicesServiceSupplier;

    /**
     * Creates the service set of one node. Nothing is built until asked for.
     *
     * @param settings the node's settled settings
     * @param plugins the plugins the node runs
     * @param threadPool the node's thread pool
     * @param metadataPlane the node's metadata plane, which is installed after construction
     * @param node the node, handed to the client
     * @param indicesService the node's indices service, built after this
     */
    NodeServices(
        Settings settings,
        ServerlessPlugins plugins,
        ThreadPool threadPool,
        Supplier<MetadataPlane> metadataPlane,
        Supplier<ServerlessNode> node,
        Supplier<IndicesService> indicesService
    ) {
        this.settings = settings;
        this.plugins = plugins;
        this.threadPool = threadPool;
        this.metadataPlane = metadataPlane;
        this.node = node;
        this.indicesServiceSupplier = indicesService;
    }

    private volatile org.opensearch.identity.IdentityService identityService;

    /**
     * Layers the shell's own transport choice under whatever the caller supplied.
     *
     * <p>A classic node discovers its transport through the plugin system and then fails at runtime if
     * nothing set {@code transport.type}. The shell supports exactly one transport, so leaving that as a
     * lookup with a single possible answer only creates a way to misconfigure it.
     */
    static Settings withShellDefaults(Settings supplied) {
        return Settings.builder()
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4ModulePlugin.NETTY_TRANSPORT_NAME)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            .put(supplied)
            .build();
    }

    /**
     * A real {@link ClusterService} with no coordinator behind it.
     */
    ClusterService buildClusterService(ClusterSettings clusterSettings, DiscoveryNode localNode) {
        final ClusterService service = new ClusterService(settings, clusterSettings, threadPool);

        // The applier refuses to start without this. Peer connections are not managed from the applied
        // view, so this one connects to nothing.
        service.setNodeConnectionsService(new NodeConnectionsService(settings, threadPool, null) {
            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
                onCompletion.run();
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
                // peer connections are not driven by the applied view
            }
        });

        // clusterManagerNodeId is deliberately unset. There is no election, and nothing in the data
        // plane needs one.
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
    static PluginsService buildPluginsService(Settings settings, Environment environment) {
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

    IndicesService buildIndicesService(
        Environment environment,
        ClusterSettings clusterSettings,
        PluginsService pluginsService,
        NodeEnvironment nodeEnvironment,
        ClusterService clusterService,
        java.util.Collection<
            java.util.function.Function<
                org.opensearch.index.IndexSettings,
                java.util.Optional<org.opensearch.index.engine.EngineFactory>>> engineFactoryProviders,
        Map<String, org.opensearch.plugins.IndexStorePlugin.DirectoryFactory> directoryFactories
    ) throws Exception {
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
            plugins.filter(org.opensearch.plugins.AnalysisPlugin.class)
        ).getAnalysisRegistry();
        // Mandatory — DataFormatRegistry calls filterPlugins() during construction. It is now the
        // real one rather than an empty stub, so an installed plugin gets onIndexModule like it would on a
        // classic node.
        return new IndicesService(
            settings,
            pluginsService,
            nodeEnvironment,
            xContentRegistry,
            analysisRegistry,
            new IndexNameExpressionResolver(new ThreadContext(settings)),
            new IndicesModule(plugins.filter(org.opensearch.plugins.MapperPlugin.class)).getMapperRegistry(),
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
            null,                                   // Client — none at construction
            // A no-op. The real one reads and writes on-disk index state under
            // gateway/, which is the persistence half of the control plane this shell does not build;
            // IndicesService wants an instance only to look up a deleted index's metadata on disk, and
            // "there is none" is the truthful answer here, because nothing ever wrote any.
            new NoopMetaStateService(nodeEnvironment, xContentRegistry),
            engineFactoryProviders,
            directoryFactories,
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
            // Built from settings: DefaultRecoverySettings lives in test/framework and is not
            // available to production code.
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
                ServerlessNode.STORAGE_UUID_SETTING,
                org.opensearch.common.settings.Setting.Property.IndexScope,
                org.opensearch.common.settings.Setting.Property.PrivateIndex
            )
        );
        return new IndexScopedSettings(settings, known);
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
    java.util.List<org.opensearch.plugins.SecureSettingsFactory> secureSettingsFactories() {
        final java.util.List<org.opensearch.plugins.SecureSettingsFactory> factories = new java.util.ArrayList<>();
        for (org.opensearch.plugins.Plugin plugin : plugins.plugins()) {
            plugin.getSecureSettingFactory(settings).ifPresent(factories::add);
        }
        return factories;
    }

    SearchService buildSearchService(ClusterSettings clusterSettings, ClusterService clusterService, IndicesService indicesService) {
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
     * The shell picks netty4 directly rather than discovering a transport through the plugin system.
     * There is exactly one supported transport, and pretending otherwise would add a lookup with one
     * possible answer.
     */
    NetworkModule buildNetworkModule(ClusterSettings clusterSettings, RestController restController) {
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
                new org.opensearch.serverless.script.StoredScripts(metadataPlane)
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
                indicesServiceSupplier.get().getAnalysis(),
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
                threadPool.getThreadContext(),
                threadPool::relativeTimeInMillis,
                (delay, command) -> threadPool.schedule(
                    command,
                    org.opensearch.common.unit.TimeValue.timeValueMillis(delay),
                    ThreadPool.Names.GENERIC
                ),
                null,                                   // ingest service: pipelines live in a register
                null,                                   // client: nothing here re-enters the API
                task -> threadPool.generic().execute(task),
                indicesServiceSupplier.get()
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
            client = new ServerlessClient(settings, threadPool, node, metadataPlane);
        }
        return client;
    }

    /**
     * The on-disk index-state service this shell never builds, as a no-op that satisfies
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

    /** Closes the services built here that hold resources: the file watcher and the script service. */
    @Override
    public void close() {
        org.opensearch.common.util.io.IOUtils.closeWhileHandlingException(resourceWatcherService, scriptService);
    }
}
