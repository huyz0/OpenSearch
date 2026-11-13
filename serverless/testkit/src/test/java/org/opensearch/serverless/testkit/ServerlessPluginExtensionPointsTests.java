/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginComponentRegistry;
import org.opensearch.plugins.TelemetryAwarePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestHeaderDefinition;
import org.opensearch.rest.RestRequest;
import org.opensearch.script.ScriptService;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The extension points real plugins use and the shell used to skip, each pinned by a plugin written the way the
 * stock ones are.
 *
 * <p>Found by installing the released OpenSearch plugins on a shell node and reading where each stopped. Job
 * Scheduler failed in {@code createComponents} because its own setting was not registered. Index management
 * failed because the shell asked for its REST handlers and transport interceptors before its components existed,
 * and then because it overrides only the {@code createComponents} overload that takes a component registry.
 * Its actions wanted a {@code NodeClient}, and cross-cluster replication's wanted classes only Guice would build.
 * Each plugin here does one of those things, and each test fails on the shell as it was.
 */
public class ServerlessPluginExtensionPointsTests extends OpenSearchTestCase {

    static final Setting<Integer> PERIOD = Setting.intSetting(
        "test_extensions.period",
        5,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    static final Setting<Boolean> FLAG = Setting.boolSetting("index.test_extensions.flag", false, Setting.Property.IndexScope);
    static final String POOL = "test_extensions_pool";

    private static final AtomicReference<String> REGISTRY_OVERLOAD = new AtomicReference<>();
    private static final AtomicBoolean TELEMETRY_OVERLOAD = new AtomicBoolean();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        REGISTRY_OVERLOAD.set(null);
        TELEMETRY_OVERLOAD.set(false);
    }

    /**
     * Declares a setting and a pool, and answers the REST and transport hooks from state its components set --
     * as the security plugin's wrapper, index management's interceptor and replication's handlers do.
     */
    public static final class ClassicOrderPlugin extends Plugin implements ActionPlugin, NetworkPlugin {

        private volatile ClusterService clusterService;
        private volatile ThreadPool threadPool;

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(PERIOD, FLAG);
        }

        @Override
        public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
            return List.of(new FixedExecutorBuilder(settings, POOL, 1, 10, "thread_pool." + POOL));
        }

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            // Job Scheduler's shape: listen to one's own dynamic setting. Throws unless the setting is registered.
            clusterService.getClusterSettings().addSettingsUpdateConsumer(PERIOD, period -> {});
            // And look up one's own pool, which throws unless it was built.
            threadPool.executor(POOL);
            this.clusterService = clusterService;
            this.threadPool = threadPool;
            return List.of();
        }

        @Override
        public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster
        ) {
            final String name = clusterService.getClusterName().value();
            return List.of(new RestHandler() {
                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.GET, "/_test_extensions/cluster"));
                }

                @Override
                public void handleRequest(RestRequest request, org.opensearch.rest.RestChannel channel, NodeClient client) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, name));
                }
            });
        }

        @Override
        public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext, Set<RestHeaderDefinition> headersToCopy) {
            final ThreadPool pool = threadPool;
            return handler -> {
                pool.getThreadContext();
                return handler;
            };
        }

        @Override
        public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry registry, ThreadContext threadContext) {
            threadPool.getThreadContext();
            return List.of();
        }
    }

    /** Overrides only the overload with a component registry, as index management does. */
    public static final class RegistryOverloadPlugin extends Plugin {
        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier,
            PluginComponentRegistry pluginComponentRegistry
        ) {
            REGISTRY_OVERLOAD.set(pluginComponentRegistry == null ? "null registry" : "called");
            return List.of();
        }
    }

    /** Builds its components through the telemetry-aware overload. */
    public static final class TelemetryOverloadPlugin extends Plugin implements TelemetryAwarePlugin {
        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier,
            Tracer tracer,
            MetricsRegistry metricsRegistry
        ) {
            TELEMETRY_OVERLOAD.set(tracer != null && metricsRegistry != null);
            return List.of();
        }
    }

    public static final class EchoAction extends ActionType<EchoResponse> {
        static final EchoAction INSTANCE = new EchoAction();
        static final String NAME = "cluster:admin/serverless_testkit/echo";

        private EchoAction() {
            super(NAME, EchoResponse::new);
        }
    }

    public static final class EchoRequest extends ActionRequest {
        EchoRequest() {}

        EchoRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static final class EchoResponse extends ActionResponse {
        final String clusterName;

        EchoResponse(String clusterName) {
            this.clusterName = clusterName;
        }

        EchoResponse(StreamInput in) throws IOException {
            this.clusterName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterName);
        }
    }

    /** A helper the plugin never returns from createComponents: on a classic node only Guice builds it. */
    public static final class ClusterNames {
        private final ClusterService clusterService;

        @Inject
        public ClusterNames(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        String name() {
            return clusterService.getClusterName().value();
        }
    }

    /** An action taking the NodeClient and an injectable helper, as index management's and replication's do. */
    public static final class TransportEchoAction extends HandledTransportAction<EchoRequest, EchoResponse> {
        private final ClusterNames names;

        @Inject
        public TransportEchoAction(TransportService transportService, ActionFilters actionFilters, NodeClient client, ClusterNames names) {
            super(EchoAction.NAME, transportService, actionFilters, EchoRequest::new);
            this.names = names;
        }

        @Override
        protected void doExecute(Task task, EchoRequest request, ActionListener<EchoResponse> listener) {
            listener.onResponse(new EchoResponse(names.name()));
        }
    }

    public static final class InjectingPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(EchoAction.INSTANCE, TransportEchoAction.class));
        }
    }

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-extension-points")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(extra)
            .build();
    }

    /** Settings, pools, and REST and transport hooks answered from components: the classic order works. */
    public void testAPluginBuiltForTheClassicOrderStartsAndServes() throws Exception {
        try (
            ServerlessNode node = new ServerlessNode(
                nodeSettings("extensions", Settings.builder().put(PERIOD.getKey(), 7).build()),
                List.of(new ClassicOrderPlugin())
            )
        ) {
            node.startup().start();
            assertNotNull("the plugin's pool must exist", node.threadPool().executor(POOL));
            assertTrue(
                "its node setting must be known to the node's cluster settings",
                node.clusterService().getClusterSettings().isDynamicSetting(PERIOD.getKey())
            );
            final Response served = send(node, "/_test_extensions/cluster");
            assertEquals("its route must be served, built from its components: " + served.body(), 200, served.status());
            assertEquals("serverless-extension-points", served.body());
        }
    }

    /** With the switch off, the node is as it was: the same plugin cannot start. */
    public void testTheSwitchRestoresTheOldBehaviour() {
        final Exception failure = expectThrows(
            Exception.class,
            () -> new ServerlessNode(
                nodeSettings("extensions-off", Settings.builder().put("serverless.plugin_extensions.enabled", false).build()),
                List.of(new ClassicOrderPlugin())
            ).close()
        );
        final String reported = failure + " / " + failure.getCause();
        assertTrue("an unregistered setting is what stops it: " + reported, reported.contains("not registered"));
    }

    /** Both of core's other createComponents overloads are called. */
    public void testTheRegistryAndTelemetryOverloadsAreCalled() throws Exception {
        try (
            ServerlessNode node = new ServerlessNode(
                nodeSettings("overloads", Settings.EMPTY),
                List.of(new RegistryOverloadPlugin(), new TelemetryOverloadPlugin())
            )
        ) {
            node.startup().start();
            assertEquals("the registry overload must be called with a registry", "called", REGISTRY_OVERLOAD.get());
            assertTrue("the telemetry overload must be called with telemetry", TELEMETRY_OVERLOAD.get());
        }
    }

    /** An action taking the NodeClient and a helper only Guice would build is built, and runs. */
    public void testAnActionNeedingAnInjectableHelperIsBuilt() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("inject", Settings.EMPTY), List.of(new InjectingPlugin()))) {
            node.startup().start();
            node.setMetadataPlane(
                new org.opensearch.serverless.metadata.MetadataPlane(
                    new org.opensearch.common.blobstore.fs.FsBlobStore(1024, createTempDir(), false),
                    org.opensearch.common.blobstore.BlobPath.cleanPath(),
                    System::currentTimeMillis,
                    30_000L
                )
            );
            final EchoResponse response = node.services().client().execute(EchoAction.INSTANCE, new EchoRequest()).actionGet();
            assertEquals("serverless-extension-points", response.clusterName);
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String path) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .GET()
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
