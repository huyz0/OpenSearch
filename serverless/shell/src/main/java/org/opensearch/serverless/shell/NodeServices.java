/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.MetaStateService;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rest.RestController;
import org.opensearch.search.SearchService;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Netty4ModulePlugin;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Supplier;

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
public final class NodeServices implements Closeable {

    final Settings settings;
    final ServerlessPlugins plugins;
    final ThreadPool threadPool;
    final Supplier<MetadataPlane> metadataPlane;
    final Supplier<ServerlessNode> node;
    final Supplier<IndicesService> indicesServiceSupplier;
    /** Builds the core services. */
    final NodeServiceBuilders builders = new NodeServiceBuilders(this);

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

    volatile org.opensearch.identity.IdentityService identityService;

    /**
     * Layers the shell's own transport choice under whatever the caller supplied.
     *
     * <p>A classic node discovers its transport through the plugin system and then fails at runtime if
     * nothing set {@code transport.type}. The shell supports exactly one transport, so leaving that as a
     * lookup with a single possible answer only creates a way to misconfigure it.
     */
    static Settings withShellDefaults(Settings supplied) {
        final Settings.Builder builder = Settings.builder()
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4ModulePlugin.NETTY_TRANSPORT_NAME)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME);
        if (org.opensearch.serverless.rest.BulkHandler.streams(supplied)) {
            // Streamed batches need an HTTP transport that hands a body over as it arrives, and reads it only as fast as the
            // handler asks: core's Reactor Netty transport, with its demand-driven reads on.
            builder.put(NetworkModule.HTTP_TYPE_KEY, org.opensearch.transport.reactor.ReactorNetty4Plugin.REACTOR_NETTY_HTTP_TRANSPORT_NAME)
                .put(org.opensearch.http.reactor.netty4.ReactorNetty4HttpServerTransport.SETTING_STREAMING_DEMAND_DRIVEN.getKey(), true);
        }
        return builder.put(supplied).build();
    }

    ClusterService buildClusterService(ClusterSettings clusterSettings, DiscoveryNode localNode) {
        return builders.buildClusterService(clusterSettings, localNode);
    }

    static PluginsService buildPluginsService(Settings settings, Environment environment) {
        return NodeServiceBuilders.buildPluginsService(settings, environment);
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
        return builders.buildIndicesService(
            environment,
            clusterSettings,
            pluginsService,
            nodeEnvironment,
            clusterService,
            engineFactoryProviders,
            directoryFactories
        );
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
        return builders.buildSearchService(clusterSettings, clusterService, indicesService);
    }

    NetworkModule buildNetworkModule(ClusterSettings clusterSettings, RestController restController) {
        return builders.buildNetworkModule(clusterSettings, restController);
    }

    /**
     * The index settings this node validates against, including those its plugins declare.
     *
     * @return the settings
     */
    synchronized org.opensearch.common.settings.IndexScopedSettings indexScopedSettings() {
        if (indexScopedSettings == null) {
            indexScopedSettings = builders.indexScopedSettings();
        }
        return indexScopedSettings;
    }

    private org.opensearch.common.settings.IndexScopedSettings indexScopedSettings;

    volatile org.opensearch.search.SearchModule searchModule;

    /**
     * The one {@link org.opensearch.search.SearchModule} this node builds, memoized.
     *
     * <p>It was constructed inline for its fetch phase and thrown away. It also carries the parsers for
     * every query, aggregation and suggester in the codebase, which is what a real query body needs to be
     * read at all — so it is kept rather than rebuilt per request.
     */
    synchronized org.opensearch.search.SearchModule searchModule() {
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

    volatile org.opensearch.core.xcontent.NamedXContentRegistry searchRegistry;

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

    volatile org.opensearch.core.xcontent.NamedXContentRegistry rankEvalRegistry;

    volatile ServerlessClient client;

    volatile org.opensearch.watcher.ResourceWatcherService resourceWatcherService;

    volatile org.opensearch.script.ScriptService scriptService;

    volatile org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver;

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
            //
            // A plugin's engine comes first, and a plugin's contexts are registered, as core's ScriptModule does:
            // the painless module loaded as a bundle carries the allowlists of the plugins that extend it (k-NN's
            // scoring functions), and hands its engine to its own transport actions. The shell's built-in
            // engines fill in only the languages no plugin provides.
            final Map<String, org.opensearch.script.ScriptContext<?>> contexts = new java.util.HashMap<>(
                org.opensearch.script.ScriptModule.CORE_CONTEXTS
            );
            final java.util.List<org.opensearch.plugins.ScriptPlugin> scriptPlugins = plugins.filter(
                org.opensearch.plugins.ScriptPlugin.class
            );
            for (org.opensearch.plugins.ScriptPlugin plugin : scriptPlugins) {
                for (org.opensearch.script.ScriptContext<?> context : plugin.getContexts()) {
                    if (contexts.put(context.name, context) != null) {
                        throw new IllegalArgumentException("a script context named [" + context.name + "] is registered twice");
                    }
                }
            }
            final Map<String, org.opensearch.script.ScriptEngine> engines = new java.util.HashMap<>();
            for (org.opensearch.plugins.ScriptPlugin plugin : scriptPlugins) {
                final org.opensearch.script.ScriptEngine engine = plugin.getScriptEngine(settings, contexts.values());
                if (engine != null && engines.put(engine.getType(), engine) != null) {
                    throw new IllegalArgumentException("two plugins provide the script language [" + engine.getType() + "]");
                }
            }
            if (engines.containsKey("painless") == false) {
                final org.opensearch.script.ScriptEngine painless = new org.opensearch.painless.PainlessModulePlugin().getScriptEngine(
                    settings,
                    contexts.values()
                );
                engines.put(painless.getType(), painless);
            }
            // Mustache too, chosen the same way: search templates and rank-eval templates render through
            // it, and it is the one engine core registers for the template context.
            if (engines.containsKey("mustache") == false) {
                final org.opensearch.script.ScriptEngine mustache = new org.opensearch.script.mustache.MustacheModulePlugin()
                    .getScriptEngine(settings, contexts.values());
                engines.put(mustache.getType(), mustache);
            }
            // Stored scripts come from a register rather than from cluster state; see StoredScripts.
            scriptService = new org.opensearch.serverless.script.ServerlessScriptService(
                settings,
                Map.copyOf(engines),
                Map.copyOf(contexts),
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

    volatile org.opensearch.search.pipeline.ServerlessSearchPipelines searchPipelines;

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

    volatile org.opensearch.serverless.ingest.IngestPipelines ingestPipelines;

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

    volatile ActionGate actionGate;

    volatile BigArrays bigArrays;

    volatile org.opensearch.core.indices.breaker.CircuitBreakerService circuitBreakerService;

    volatile org.opensearch.index.IndexingPressure indexingPressure;

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
    static final class NoopMetaStateService extends MetaStateService {

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
