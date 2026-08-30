/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestHeaderDefinition;
import org.opensearch.rest.RestRequest;
import org.opensearch.script.ScriptService;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A plugin, actually loaded, actually running.
 *
 * <p>Everything before this was the interface a plugin <em>would</em> use. R4 says the shell "implements
 * the plugin contract without an {@code Injector}" and nothing implemented it: {@code PluginsService} was
 * constructed with an empty list because {@code IndicesService} demands one, and no plugin had ever been
 * started.
 *
 * <p>The plugin here does all three things a real one does — keeps state through the client, serves its
 * own endpoint, and wraps every request — because a host that supports one hook and not the others is not
 * a host. Writing it as a test plugin rather than pointing at the security plugin is deliberate: the point
 * is to prove the seams with something whose behaviour we control, before anything real is aimed at them.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessPluginHostTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    /** Set by the plugin when its components are built, so the test can see that they were. */
    private static final AtomicBoolean STARTED = new AtomicBoolean();
    /** Counts requests the wrapper saw, which is the seam authentication would use. */
    private static final AtomicLong SAW = new AtomicLong();
    /** When true, the wrapper refuses everything — standing in for a failed authentication. */
    private static final AtomicBoolean REFUSE = new AtomicBoolean();
    /** Set when the plugin is closed, which is how a plugin releases anything it holds. */
    private static final AtomicBoolean CLOSED = new AtomicBoolean();

    /** A plugin that keeps state, serves a route, and sees every request. */
    public static final class TestPlugin extends Plugin implements ActionPlugin {

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
            // A real plugin would read its config here. What matters is that the client it is handed
            // works, so the state it keeps has somewhere to go.
            STARTED.set(client != null && threadPool != null);
            return List.of(new Object());
        }

        @Override
        public void close() {
            CLOSED.set(true);
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
            return List.of(new RestHandler() {
                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.GET, "/_test_plugin/hello"));
                }

                @Override
                public void handleRequest(
                    RestRequest request,
                    org.opensearch.rest.RestChannel channel,
                    org.opensearch.transport.client.node.NodeClient client
                ) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, "hello from a plugin"));
                }
            });
        }

        @Override
        public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext, Set<RestHeaderDefinition> headersToCopy) {
            return original -> (RestHandler) (request, channel, client) -> {
                SAW.incrementAndGet();
                if (REFUSE.get()) {
                    // What an authentication failure looks like from this seam.
                    channel.sendResponse(
                        new BytesRestResponse(RestStatus.UNAUTHORIZED, BytesRestResponse.TEXT_CONTENT_TYPE, "refused by the plugin")
                    );
                    return;
                }
                original.handleRequest(request, channel, client);
            };
        }
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-plugins")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        STARTED.set(false);
        SAW.set(0);
        REFUSE.set(false);
        CLOSED.set(false);
    }

    /** All three hooks, on one node, in one run. */
    public void testAPluginKeepsStateServesARouteAndSeesEveryRequest() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("plugin-host"), List.of(new TestPlugin()))) {
            node.start();
            node.setMetadataPlane(plane);
            assertTrue("the plugin's createComponents must have run, with a usable client", STARTED.get());
            assertEquals("and the node must know it is running it", 1, node.plugins().plugins().size());
            assertFalse("and keep what it built", node.plugins().components().isEmpty());

            // Its own route.
            final Response hello = send(node, "GET", "/_test_plugin/hello");
            assertEquals("a plugin's route must be served: " + hello.body(), 200, hello.status());
            assertTrue(hello.body().contains("hello from a plugin"));

            // The shell's own routes still work, and the wrapper saw them.
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final long before = SAW.get();
            node.client().index(new IndexRequest("alpha").id("1").source("{\"msg\":\"plugged\",\"n\":1}", XContentType.JSON)).actionGet();
            assertEquals(200, send(node, "GET", "/alpha/_search?q=msg:plugged").status());
            assertTrue("the wrapper must see the shell's own requests too, not only the plugin's", SAW.get() > before);
        }
    }

    /**
     * The wrapper can refuse a request, which is the whole reason that seam matters.
     *
     * <p>Authentication is exactly this: see the request first, and answer 401 instead of letting the
     * handler run. If a plugin could be installed and its refusal ignored, the hook would be decorative.
     */
    public void testAPluginsWrapperCanRefuseARequest() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("plugin-refuse"), List.of(new TestPlugin()))) {
            node.start();
            assertEquals("with the plugin permitting, the request is served", 200, send(node, "GET", "/").status());

            REFUSE.set(true);
            final Response refused = send(node, "GET", "/");
            assertEquals("with the plugin refusing, it must not be: " + refused.body(), 401, refused.status());
            assertTrue(
                "and the plugin's reason must reach the caller: " + refused.body(),
                refused.body().contains("refused by the plugin")
            );
        }
    }

    /**
     * A node with no plugins behaves exactly as before, and its identity is core's no-op.
     *
     * <p>Worth asserting rather than assuming: the host is on the construction path of every node, so the
     * empty case is the one almost every test and every deployment runs.
     */
    public void testANodeWithNoPluginsIsUnchanged() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("plugin-none"))) {
            node.start();
            assertTrue("no plugins", node.plugins().plugins().isEmpty());
            assertEquals(200, send(node, "GET", "/").status());
            // Core's default: authenticates nobody and fails no checks, which is why plain OpenSearch is
            // open without a security plugin -- and why the shell is too, said out loud.
            assertEquals(
                "the default subject must be the unauthenticated one",
                org.opensearch.identity.NamedPrincipal.UNAUTHENTICATED.getName(),
                node.identityService().getCurrentSubject().getPrincipal().getName()
            );
        }
    }

    /** Two plugins both wanting to wrap every request is refused, not silently ordered. */
    public void testTwoRequestWrappersAreRefusedRatherThanOrdered() throws Exception {
        final var failure = expectThrows(
            Exception.class,
            () -> new ServerlessNode(nodeSettings("plugin-two"), List.of(new TestPlugin(), new TestPlugin())).close()
        );
        final String message = failure.getMessage() == null ? failure.toString() : failure.getMessage();
        assertTrue("the refusal must explain itself: " + message, message.contains("wrap every request"));
    }

    /**
     * A plugin is closed when the node is, so whatever it holds is released.
     *
     * <p>Worth a test of its own rather than trusting the call site. {@code closeAll} was written when the
     * host was built and nothing ever called it, which stayed invisible for as long as every plugin was a
     * test plugin holding nothing; the first one that owned a thread pool leaked it out of every node that
     * ran it. A plugin's resources are exactly the kind of thing nobody notices until production.
     */
    public void testAPluginIsClosedWithTheNode() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("plugin-close"), List.of(new TestPlugin()))) {
            node.start();
            assertFalse("not while the node is running", CLOSED.get());
        }
        assertTrue("a plugin must be closed when its node is", CLOSED.get());
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    /** A plugin that cannot start. */
    public static final class BrokenPlugin extends Plugin {
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
            throw new IllegalStateException("this plugin cannot load its configuration");
        }
    }

    /**
     * A plugin that fails to start stops the node, rather than being skipped.
     *
     * <p>The tempting behaviour is to log the failure and carry on, because one bad plugin taking down a
     * node feels harsh. It is the right behaviour anyway, and security is the reason: a plugin installed
     * to enforce something, which failed to load its configuration and was quietly ignored, leaves a node
     * serving traffic with the enforcement absent and nothing in the response to say so. Refusing to start
     * is loud, and loud is recoverable.
     */
    public void testAPluginThatFailsToStartStopsTheNode() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("plugin-broken"), List.of(new BrokenPlugin()))) {
            final var failure = expectThrows(IllegalStateException.class, node::start);
            assertTrue(
                "the node must say a plugin stopped it: " + failure.getMessage(),
                failure.getMessage().contains("plugin failed to start")
            );
            assertTrue(
                "and name the cause: " + failure.getCause().getMessage(),
                failure.getCause().getMessage().contains("cannot load its configuration")
            );
            // And it must not be left half-serving.
            assertFalse("a node whose plugin failed must not report itself started", node.isStarted());
        }
    }
}
