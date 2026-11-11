/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.cluster.ClusterName;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.rest.RestController;
import org.opensearch.serverless.rest.ServerlessHealthHandler;
import org.opensearch.serverless.rest.ServerlessRootHandler;
import org.opensearch.usage.UsageService;

import java.util.Set;

/**
 * Wires {@link ServerlessNode}'s REST and transport layers: the REST controller, authenticated plugin hops, and
 * the plugin actions it serves.
 */
final class NodeWiring {

    private final ServerlessNode node;

    NodeWiring(ServerlessNode node) {
        this.node = node;
    }

    /**
     * The REST surface is an explicit allowlist. Anything not registered here does not
     * exist, and returns 404/501 rather than an empty success.
     */
    RestController buildRestController(ClusterSettings clusterSettings) {
        final RestController controller = new RestController(
            Set.of(),
            // The seam a plugin authenticates on: it sees every request before the handler does, can
            // refuse it, and can leave an identity in the ThreadContext. Passed here because the
            // controller takes it at construction, which is why plugins are held before this runs.
            node.plugins.restHandlerWrapper(node.threadPool.getThreadContext(), Set.of()),
            node.nodeClient,
            node.circuitBreakerService(),
            new UsageService()
        );
        // Every shell handler is registered behind the system-index guard, rather than each one checking:
        // a guard a handler has to remember to call is a guard that one handler will not call.
        final java.util.function.UnaryOperator<org.opensearch.rest.RestHandler> guarded =
            handler -> org.opensearch.serverless.rest.SystemIndices.guard(handler, node.systemIndices);
        controller.registerHandler(
            guarded.apply(
                new ServerlessRootHandler(
                    node.nodeName,
                    ClusterName.CLUSTER_NAME_SETTING.get(node.settings).value(),
                    node.nodeEnvironment::nodeId
                )
            )
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.IndexAdminHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.DocumentHandler(() -> node, () -> node.metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.BulkHandler(() -> node, () -> node.metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.GetHandler(() -> node, () -> node.metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.MultiGetHandler(() -> node, () -> node.metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.UpdateHandler(() -> node, () -> node.metadataPlane)));
        // Plugin routes last, so a plugin cannot take over an endpoint the shell has already claimed:
        // RestController refuses a duplicate path rather than replacing it, and refusing loudly at boot
        // is the right answer -- a plugin that silently replaced the write path would be a system whose
        // behaviour depends on registration order.
        for (org.opensearch.rest.RestHandler handler : node.plugins.restHandlers(node.settings, controller, clusterSettings)) {
            controller.registerHandler(handler);
        }
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.SearchHandler(() -> node, () -> node.metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.CatalogHandler(() -> node.metadataPlane, () -> node)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.StatsHandler(() -> node, () -> node.metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.AliasHandler(() -> node.metadataPlane, () -> node)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.PointInTimeHandler(() -> node, () -> node.metadataPlane))
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.DeleteByQueryHandler(() -> node, () -> node.metadataPlane))
        );
        // Snapshot/restore, backed by this deployment's own object store rather than a distinct
        // repository backend.
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.RepositoryHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.SnapshotHandler(() -> node.metadataPlane, () -> node)));
        // The one piece of /_cluster/* not refused below: cluster/config is one register, exactly
        // like an index descriptor, and this node can answer for it the same honest way it answers for an
        // index -- unlike health, state or stats, which are genuinely cluster-wide and would mislead
        // coming from one node.
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.ClusterSettingsHandler(() -> node.metadataPlane, () -> node))
        );
        // Cluster-shaped endpoints this deployment can answer honestly. See ClusterHealthHandler for what
        // green is redefined to mean and why, and NodesHandler for why "no node can answer this" stopped
        // being true the moment leases became an address book.
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.ClusterHealthHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.NodesHandler(() -> node.metadataPlane, () -> node)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.MappingUpdateHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.FieldCapabilitiesHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.ListIndicesHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.SettingsUpdateHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.AnalyzeHandler(() -> node.metadataPlane, () -> node)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.TemplateHandler(() -> node.metadataPlane, () -> node)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.PipelineHandler(() -> node.metadataPlane, () -> node)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.StoredScriptHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.SearchPipelineHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.SearchTemplateHandler(() -> node, () -> node.metadataPlane))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.RankEvalHandler(() -> node, () -> node.metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.RolloverHandler(() -> node, () -> node.metadataPlane)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.DataStreamHandler(() -> node, () -> node.metadataPlane))
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.ValidateAndResolveHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.MultiSearchHandler(() -> node.metadataPlane, () -> node))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.ExplainHandler(() -> node.metadataPlane, () -> node)));
        controller.registerHandler(
            new ServerlessHealthHandler(
                () -> node.started,
                () -> node.membershipSource == null ? 1 : node.membershipSource.current().size(),
                node.nodeEnvironment::nodeId
            )
        );

        // Then every endpoint a classic node serves and this one refuses, each with its reason.
        RefusedEndpoints.register(controller);
        return controller;
    }

    /**
     * Wraps the plugins' own interceptors in the one that authenticates a plugin's action across a hop.
     *
     * <p>The shell's is outermost on both paths, and that is the point of the ordering rather than an
     * accident of composition: inbound, an unauthenticated plugin action is refused before any plugin's
     * interceptor is given it; outbound, the signature is on the context before a plugin's interceptor
     * runs, so a plugin that inspects or forwards the request sees it as it will go on the wire.
     *
     * <p>Both dependencies are looked up lazily. This runs while the transport service is being built,
     * which is before the router exists and long before the plugins' actions have been constructed.
     *
     * @param fromPlugins what the network module composed from the installed plugins
     * @return the composed interceptor
     */
    org.opensearch.transport.TransportInterceptor authenticatedPluginHops(org.opensearch.transport.TransportInterceptor fromPlugins) {
        final org.opensearch.transport.TransportInterceptor shell = new org.opensearch.serverless.transport.PluginHopAuthentication(
            () -> node.router,
            name -> node.pluginActionNames.contains(name)
        );
        return new org.opensearch.transport.TransportInterceptor() {
            @Override
            public <
                T extends org.opensearch.transport.TransportRequest> org.opensearch.transport.TransportRequestHandler<T> interceptHandler(
                    String action,
                    String executor,
                    boolean forceExecution,
                    org.opensearch.transport.TransportRequestHandler<T> actualHandler
                ) {
                return shell.interceptHandler(
                    action,
                    executor,
                    forceExecution,
                    fromPlugins.interceptHandler(action, executor, forceExecution, actualHandler)
                );
            }

            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return shell.interceptSender(fromPlugins.interceptSender(sender));
            }
        };
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
    /**
     * Every action name the installed plugins declare, read from their handler descriptors.
     *
     * @return the names, empty when no plugin contributes an action
     */
    Set<String> declaredPluginActionNames() {
        final Set<String> names = new java.util.HashSet<>();
        for (org.opensearch.plugins.ActionPlugin plugin : node.plugins.filter(org.opensearch.plugins.ActionPlugin.class)) {
            for (org.opensearch.plugins.ActionPlugin.ActionHandler<?, ?> handler : plugin.getActions()) {
                names.add(handler.getAction().name());
            }
        }
        return Set.copyOf(names);
    }

    PluginActions buildPluginActions() {
        final java.util.List<org.opensearch.plugins.ActionPlugin> actionPlugins = node.plugins.filter(
            org.opensearch.plugins.ActionPlugin.class
        );
        if (actionPlugins.isEmpty()) {
            return PluginActions.none();
        }
        final java.util.List<Object> services = java.util.List.of(
            new org.opensearch.action.support.ActionFilters(new java.util.HashSet<>(node.plugins.actionFilters())),
            node.transportService,
            node.transportService.getTaskManager(),
            node.threadPool,
            node.client(),
            node.clusterService,
            node.settings,
            node.namedWriteableRegistry(),
            node.indexNameExpressionResolver(),
            node.circuitBreakerService(),
            node.indicesService
        );
        // Each action's constructor is resolved against the node's services and the components *its own*
        // plugin built -- never another plugin's. The aggregate list used to be offered to every action,
        // so a plugin could take a constructor argument of a type only some other plugin's component
        // satisfied, and get that plugin's private object handed to it by reflection.
        return PluginActions.build(actionPlugins, plugin -> {
            final java.util.List<Object> available = new java.util.ArrayList<>(
                node.plugins.componentsOf((org.opensearch.plugins.Plugin) plugin)
            );
            available.addAll(services);
            return available;
        });
    }
}
