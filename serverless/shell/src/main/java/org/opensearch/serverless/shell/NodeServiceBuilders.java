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
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rest.RestController;
import org.opensearch.search.SearchService;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.transport.Netty4ModulePlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Builds the core services a {@link ServerlessNode} runs on for {@link NodeServices}: the plugins service, the
 * cluster, indices and search services, and the network module.
 */
final class NodeServiceBuilders {

    private final NodeServices services;

    NodeServiceBuilders(NodeServices services) {
        this.services = services;
    }

    /**
     * A real {@link ClusterService} with no coordinator behind it.
     */
    ClusterService buildClusterService(ClusterSettings clusterSettings, DiscoveryNode localNode) {
        final ClusterService service = new ClusterService(services.settings, clusterSettings, services.threadPool);

        // The applier refuses to start without this. Peer connections are not managed from the applied
        // view, so this one connects to nothing.
        service.setNodeConnectionsService(new NodeConnectionsService(services.settings, services.threadPool, null) {
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
                ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(services.settings))
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
    static java.util.Collection<org.opensearch.plugins.PluginInfo> classpathPlugins(Settings settings) {
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
            services.plugins.filter(org.opensearch.plugins.AnalysisPlugin.class)
        ).getAnalysisRegistry();
        // Mandatory — DataFormatRegistry calls filterPlugins() during construction. It is now the
        // real one rather than an empty stub, so an installed plugin gets onIndexModule like it would on a
        // classic node.
        return new IndicesService(
            services.settings,
            pluginsService,
            nodeEnvironment,
            xContentRegistry,
            analysisRegistry,
            new IndexNameExpressionResolver(new ThreadContext(services.settings)),
            new IndicesModule(services.plugins.filter(org.opensearch.plugins.MapperPlugin.class)).getMapperRegistry(),
            // The search module's writeables, not an empty registry. The shard request cache stores a
            // query result serialised and reads it back through this registry on a hit -- so with an empty
            // one, the second identical size:0 aggregation on a shard failed with "Unknown NamedWriteable
            // category [AggregationBuilder]" while the first succeeded, which reads as a flaky server.
            services.namedWriteableRegistry(),
            services.threadPool,
            indexScopedSettings(),
            services.circuitBreakerService(),
            services.bigArrays(),
            services.scriptService(),
            clusterService,
            null,                                   // Client — none at construction
            // A no-op. The real one reads and writes on-disk index state under
            // gateway/, which is the persistence half of the control plane this shell does not build;
            // IndicesService wants an instance only to look up a deleted index's metadata on disk, and
            // "there is none" is the truthful answer here, because nothing ever wrote any.
            new NodeServices.NoopMetaStateService(nodeEnvironment, xContentRegistry),
            engineFactoryProviders,
            directoryFactories,
            emptyMap(),                             // composite directory factories — none
            emptyMap(),                             // data-format-aware store directory factories — none
            // Aggregations resolve a field's values source through this. It was null, which was invisible
            // for as long as aggregations were refused at the REST layer and became a NullPointerException
            // from inside QueryShardContext the moment one reached a shard.
            services.searchModule().getValuesSourceRegistry(),
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
            new org.opensearch.indices.recovery.RecoverySettings(services.settings, clusterSettings),
            new org.opensearch.common.cache.module.CacheModule(new ArrayList<>(), services.settings).getCacheService(),
            new org.opensearch.indices.RemoteStoreSettings(services.settings, clusterSettings),
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
    IndexScopedSettings indexScopedSettings() {
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
        known.add(SegmentPlacement.INDEX);
        known.add(org.opensearch.serverless.transport.TailOverlays.INDEX);
        return new IndexScopedSettings(services.settings, known);
    }

    SearchService buildSearchService(ClusterSettings clusterSettings, ClusterService clusterService, IndicesService indicesService) {
        return new SearchService(
            clusterService,
            indicesService,
            services.threadPool,
            services.scriptService(),
            // The node's allocator, not one built here with a null breaker service. An aggregator asks its
            // search context's BigArrays for the breaker and calls getBreaker on it before it has counted a
            // single document, so a null one is a NullPointerException at the top of every aggregation.
            services.bigArrays(),
            new QueryPhase(),
            // Built by SearchModule, not by hand. A FetchPhase with no sub-phases returns hits with an
            // id and no _source, and reports success -- the fetch sub-phases are what load the source,
            // highlight, and so on. Constructing it empty looked harmless and silently dropped every
            // document body.
            services.searchModule().getFetchPhase(),
            new org.opensearch.node.ResponseCollectorService(clusterService),
            services.circuitBreakerService(),
            null,                                   // indexSearcherExecutor — no concurrent search yet
            new org.opensearch.tasks.TaskResourceTrackingService(services.settings, clusterSettings, services.threadPool),
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
        networkPlugins.addAll(services.plugins.filter(org.opensearch.plugins.NetworkPlugin.class));
        return new NetworkModule(
            services.settings,
            networkPlugins,
            services.threadPool,
            services.bigArrays(),
            new PageCacheRecycler(services.settings),
            services.circuitBreakerService(),
            // Not empty, and it took a forwarded query to find out why. A search body crossing the
            // network is a SearchSourceBuilder, and its query serializes as a named writeable -- so an
            // empty registry reads it back as "Unknown NamedWriteable category [QueryBuilder]" on the
            // receiving node. Local shards answered fine and remote ones silently dropped out of the
            // coverage count, which is the shape of failure this design reports rather than hides.
            new NamedWriteableRegistry(services.searchModule().getNamedWriteables()),
            new NamedXContentRegistry(Collections.emptyList()),
            new NetworkService(Collections.emptyList()),
            restController,
            clusterSettings,
            NoopTracer.INSTANCE,
            Collections.emptyList(),
            services.secureSettingsFactories()
        );
    }
}
