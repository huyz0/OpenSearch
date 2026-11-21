/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.node.NodeComponents;
import org.opensearch.serverless.node.NodeContext;
import org.opensearch.serverless.node.NodePackWiring;
import org.opensearch.serverless.node.NodeStartup;
import org.opensearch.serverless.plugins.PluginActions;
import org.opensearch.serverless.plugins.PluginInjector;
import org.opensearch.serverless.plugins.ServerlessPlugins;
import org.opensearch.serverless.rest.SystemIndices;
import org.opensearch.serverless.transport.ShardRouter;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A serverless node: the OpenSearch data plane with no control plane.
 *
 * <p>This is the serverless shell. It constructs {@link IndicesService}
 * and {@link SearchService} directly and drives them from a node-local {@link ClusterState} handed to
 * {@link ClusterApplier}, rather than from a consensus-published one. It contains no
 * {@code Coordinator}, no {@code AllocationService}, no {@code GatewayMetaState} and no {@code Node} —
 * a property asserted by test, not by inspection.
 *
 * <p>Two properties of this construction are load-bearing and are commented at their call sites: the
 * applier requires a {@link NodeConnectionsService}, and the data plane refuses to activate a primary
 * at term 0.
 */
public final class ServerlessNode extends NodeContext implements Closeable {

    /** The plugins' own indices, which the request path refuses. */
    SystemIndices systemIndices;

    /** Wires this node's REST and transport layers. */
    final NodeWiring wiring = new NodeWiring(this);
    NodeTelemetry telemetry;

    @Override
    protected void beforeAcceptingRequests() {
        pluginActionNames = wiring.declaredPluginActionNames();
    }

    private PluginActions pluginActions = PluginActions.none();
    /** Core's injector for plugin objects, once the node has started with plugins installed; null otherwise. */
    PluginInjector pluginInjector;
    /**
     * The names of every action the installed plugins declare, known before any of them is built.
     *
     * <p><b>Why not ask {@link #pluginActions}.</b> A {@code HandledTransportAction} registers its own
     * transport handler from its constructor, and those constructors run inside {@code
     * NodeWiring#buildPluginActions()} -- which cannot run until {@code createComponents} has, which cannot run
     * until the node has started, which is after {@code acceptIncomingRequests()}. So each plugin action
     * becomes reachable over the transport strictly before the registry naming it exists, and a check
     * that consulted the registry would wave those requests through for the whole of that window: the
     * duration of every plugin's {@code createComponents} plus the construction loop.
     *
     * <p>The names do not need any of that. They come off {@code ActionPlugin#getActions()}, which is a
     * list of descriptors, so this is filled in before the transport accepts anything and the window
     * closes. Volatile because it is written on the starting thread and read on transport threads.
     */
    volatile java.util.Set<String> pluginActionNames = Set.of();

    /** {@return the actions the plugins provide} */
    public PluginActions pluginActions() {
        return pluginActions;
    }

    @Override
    protected void transportBound() {
        final ShardRouter router = new ShardRouter(this, transportService, this::metadataPlane);
        attach(ShardRouter.class, router);
        router.statsDocument((builder, plane) -> org.opensearch.serverless.rest.StatsHandler.describe(builder, this, plane));
        router.registerHandlers();
        attach(org.opensearch.serverless.transport.CacheWarmCopy.Answers.class, new org.opensearch.serverless.warm.WarmAnswers(this));
        org.opensearch.serverless.transport.CacheWarmCopy.of(this).registerHandlers();
        org.opensearch.serverless.transport.NewestCommits.of(this).registerHandlers();
        router.commitPush().registerHandlers();
        router.tailPush().registerHandlers();
        router.tailOverlays().registerHandlers();
        push = router.readerPush();
    }

    /**
     * Returns this node's shard router, which forwards work to the node that owns a shard.
     *
     * @return the router, or null before the transport has bound
     */
    public ShardRouter router() {
        return ShardRouter.of(this);
    }

    @Override
    protected void afterStarted() {
        // After createComponents, because a plugin's action almost always takes one of its own components,
        // and before onNodeStarted, because a plugin told the node is up may immediately call its own
        // action.
        pluginActions = wiring.buildPluginActions();
        final org.opensearch.serverless.usage.Guardrails guardrails = part(org.opensearch.serverless.usage.Guardrails.class);
        if (guardrails != null && guardrails.mode() != org.opensearch.serverless.usage.UsageSettings.Mode.OFF) {
            threadPool().scheduleWithFixedDelay(
                guardrails::tick,
                org.opensearch.common.unit.TimeValue.timeValueMillis(org.opensearch.serverless.usage.Guardrails.TICK_MILLIS),
                org.opensearch.threadpool.ThreadPool.Names.GENERIC
            );
        }
        // The plugins' lifecycle components, then the services their injector built, as core starts them.
        for (Object component : plugins.components()) {
            if (component instanceof org.opensearch.common.lifecycle.LifecycleComponent lifecycle) {
                pluginLifecycle.add(lifecycle);
            }
        }
        if (pluginInjector != null) {
            pluginLifecycle.addAll(pluginInjector.services());
        }
        pluginLifecycle.forEach(org.opensearch.common.lifecycle.LifecycleComponent::start);
        // And then tell them the node is up. A plugin that has to read its own index cannot do it while
        // components are being built; this is the event it waits for. Deferred until the node also has its
        // metadata plane, if it has none yet.
        notifyPluginsStarted();
    }

    @Override
    protected ClusterState withPluginOwners(ClusterState view) {
        return pluginIndices().withOwners(view);
    }

    @Override
    protected java.util.Collection<IndexDescriptor> pluginDescriptors() {
        return pluginIndices().descriptors();
    }

    @Override
    protected ClusterState withPluginAliases(ClusterState projected, java.util.Collection<IndexDescriptor> descriptors) {
        return pluginIndices().withAliases(ViewAliases.with(projected, descriptors, metadataPlane(), this));
    }

    /**
     * Constructs a node. Nothing is started until {@link NodeStartup#start()} is called.
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
        super(settings, plugins);
        services.clientFactory(() -> new ServerlessClient(this.settings, threadPool, () -> this, this::metadataPlane));
        boolean success = false;
        try {
            telemetry = NodeTelemetry.create(
                this.settings,
                clusterSettings,
                threadPool,
                this.plugins.filter(org.opensearch.plugins.TelemetryPlugin.class)
            );
            // The plugins build their components here, before the REST controller, the network module and the
            // transport exist -- the order a classic node uses, and the one plugins are written for. Each of
            // those asks the plugins for something (a request wrapper, REST handlers, transport interceptors),
            // and a plugin commonly answers from a field its createComponents sets: the security plugin's
            // request wrapper, index management's rollup interceptor and cross-cluster replication's REST
            // handlers all dereference one, and failed with the old order of components last. What a plugin
            // reaches through its client still waits for the node to start: the client refuses until then.
            try {
                this.plugins.createComponents(
                    new org.opensearch.serverless.plugins.PluginHost(
                        nodeClient(),
                        clusterService(),
                        threadPool(),
                        services.resourceWatcherService(),
                        services.scriptService(),
                        services.searchXContentRegistry(),
                        environment,
                        nodeEnvironment(),
                        services.namedWriteableRegistry(),
                        services.indexNameExpressionResolver(),
                        telemetry.tracer(),
                        telemetry.metrics()
                    )
                );
            } catch (Exception e) {
                throw new IllegalStateException("a plugin failed to start; the node will not serve", e);
            }
            // Each identity-aware plugin gets the subject it runs its own work as, after its components, as core does: Job
            // Scheduler's client refuses every call without one, so a scheduled job could never read its own configuration.
            final java.util.List<org.opensearch.plugins.IdentityAwarePlugin> identityAware = new java.util.ArrayList<>();
            for (org.opensearch.plugins.Plugin plugin : this.plugins.plugins()) {
                if (plugin instanceof org.opensearch.plugins.IdentityAwarePlugin aware) {
                    identityAware.add(aware);
                }
            }
            services.identityService().initializeIdentityAwarePlugins(identityAware);
            // Before the controller, because every handler it registers is wrapped in this.
            this.systemIndices = new SystemIndices(this.plugins.systemIndexPatterns(this.settings));
            this.systemIndex = systemIndices::contains;
            this.restController = wiring.buildRestController(clusterSettings);
            final Tracer protocolTracer = telemetry.tracer() == NoopTracer.INSTANCE
                ? NoopTracer.INSTANCE
                : new ProtocolTracer(telemetry.tracer(), action -> transportService.getRequestHandler(action) != null);
            final NetworkModule networkModule = services.buildNetworkModule(clusterSettings, restController, protocolTracer);
            final Transport transport = networkModule.getTransportSupplier().get();
            this.transportService = new TransportService(
                this.settings,
                transport,
                null,
                threadPool,
                wiring.authenticatedPeerHops(networkModule.getTransportInterceptor()),
                boundAddress -> NodeComponents.discoveryNode(nodeName, nodeEnvironment.nodeId(), boundAddress.publishAddress(), roles),
                clusterSettings,
                Set.of(),
                protocolTracer
            );
            this.httpServerTransport = networkModule.getHttpServerTransportSupplier().get();
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(telemetry);
                closeAfterFailedConstruction();
            }
        }
    }

    private final java.util.concurrent.atomic.AtomicBoolean pluginsNotified = new java.util.concurrent.atomic.AtomicBoolean();

    /**
     * Tells the plugins the node is up, once it is started <em>and</em> has its metadata plane, whichever comes
     * last.
     *
     * <p>A plugin answers {@code onNodeStarted} by reading or creating its own index -- the security plugin
     * creates its configuration index there -- and without the plane every such call is refused. A deployed
     * node starts first and is given its plane straight after, so telling the plugins at start told them too
     * early, and the security plugin gave up on its configuration.
     */
    @Override
    protected void afterPlaneSet() {
        notifyPluginsStarted();
    }

    void notifyPluginsStarted() {
        if (started && metadataPlane != null && pluginsNotified.compareAndSet(false, true)) {
            pluginIndices().start(metadataPlane);
            pluginClusterSettings().start(metadataPlane);
            plugins.onNodeStarted(localNode);
        }
    }

    private PluginNodeAnswers.Monitors monitors;

    private PluginIndexAdmin indexAdmin;

    /** Index administration for plugins, built the first time one asks. */
    synchronized PluginIndexAdmin indexAdmin() {
        if (indexAdmin == null) {
            indexAdmin = new PluginIndexAdmin(this);
        }
        return indexAdmin;
    }

    private PluginIndicesView pluginIndices;

    /** The plugins' own indices kept in this node's view, built the first time they are asked for. */
    synchronized PluginIndicesView pluginIndices() {
        if (pluginIndices == null) {
            pluginIndices = new PluginIndicesView(this);
        }
        return pluginIndices;
    }

    /** The plugins' lifecycle components and injector-built services this node started, in start order. */
    final java.util.List<org.opensearch.common.lifecycle.LifecycleComponent> pluginLifecycle =
        new java.util.concurrent.CopyOnWriteArrayList<>();

    /** The monitoring services plugins read node statistics from, built the first time one asks. */
    synchronized PluginNodeAnswers.Monitors monitors() throws java.io.IOException {
        if (monitors == null) {
            monitors = new PluginNodeAnswers.Monitors(this);
        }
        return monitors;
    }

    @Override
    public void close() {
        started = false;
        // Stop accepting requests first, then close the plugins that were serving them, then the services
        // those plugins were given.
        //
        // ServerlessPlugins#closeAll must be invoked here. Skipping it is invisible while every plugin is a
        // test plugin holding nothing; a plugin that owns a thread pool leaks it out of every node.
        IOUtils.closeWhileHandlingException(httpServerTransport);
        pluginIndices().stop();
        pluginClusterSettings().stop();
        // The plugins' lifecycle services before the plugins, the order a classic node stops them in.
        for (org.opensearch.common.lifecycle.LifecycleComponent service : pluginLifecycle) {
            try {
                service.stop();
            } catch (RuntimeException e) {
                logger.warn("Could not stop a plugin service (error_type={})", e.getClass().getSimpleName());
            }
            IOUtils.closeWhileHandlingException(service);
        }
        ServerlessPlugins.closeAll(plugins.plugins());
        // After the plugins, because they were given these and may use them on the way out.
        IOUtils.closeWhileHandlingException(services);
        IOUtils.closeWhileHandlingException(transportService, searchService, indicesService, clusterService, nodeEnvironment);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        IOUtils.closeWhileHandlingException(telemetry);
        NodePackWiring.forget(this);
    }
}
