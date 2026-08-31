/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A plugin that gets to see the wire.
 *
 * <p>M28's audit found the shell building its {@code NetworkModule} with netty4 and nothing else, so an
 * installed {@code NetworkPlugin} was loaded and then never asked. Two things follow from that, and they
 * are the two the audit named as the reason this host cannot yet carry the whole of OpenSearch Security:
 * the plugin cannot substitute its TLS transport, and it cannot install a transport interceptor — which is
 * how it propagates an authenticated user between nodes.
 *
 * <p>The module is core's own and already resolves both from whatever {@code NetworkPlugin}s it is given.
 * It is now given them.
 *
 * <p><b>What this proves and what it does not.</b> Interceptors are proved end to end: a plugin's
 * interceptor sees the shell's own node-to-node forwarding, by action name, on the node that sent it.
 * Transport <em>substitution</em> is not proved here — implementing a {@code Transport} is a project rather
 * than a test fixture — and what is proved instead is the part that made substitution impossible: a
 * plugin's {@code additionalSettings} now reaches the node, and loses to an operator's explicit setting
 * rather than to the shell's default.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessNetworkPluginTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    /** Actions the interceptor watched leave a node. */
    private static final CopyOnWriteArrayList<String> SENT = new CopyOnWriteArrayList<>();
    /** The settings the plugin was handed when its components were built. */
    private static final AtomicReference<Settings> SEEN_SETTINGS = new AtomicReference<>();

    /** A plugin that contributes a node setting and watches every outgoing transport request. */
    public static final class WireWatchingPlugin extends Plugin implements NetworkPlugin {

        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("serverless.block_cache.max_blocks", 512).put("serverless.wire_watcher.installed", true).build();
        }

        @Override
        public java.util.Collection<Object> createComponents(
            org.opensearch.transport.client.Client client,
            org.opensearch.cluster.service.ClusterService clusterService,
            org.opensearch.threadpool.ThreadPool threadPool,
            org.opensearch.watcher.ResourceWatcherService resourceWatcherService,
            org.opensearch.script.ScriptService scriptService,
            org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry,
            org.opensearch.env.Environment environment,
            org.opensearch.env.NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver,
            java.util.function.Supplier<org.opensearch.repositories.RepositoriesService> repositoriesServiceSupplier
        ) {
            SEEN_SETTINGS.set(environment.settings());
            return List.of(new Object());
        }

        @Override
        public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry registry, ThreadContext threadContext) {
            return List.of(new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return new AsyncSender() {
                        @Override
                        public <T extends TransportResponse> void sendRequest(
                            Transport.Connection connection,
                            String action,
                            TransportRequest request,
                            TransportRequestOptions options,
                            TransportResponseHandler<T> handler
                        ) {
                            SENT.add(action);
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    };
                }
            });
        }
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-network-plugin")
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
        SENT.clear();
        SEEN_SETTINGS.set(null);
    }

    /**
     * A plugin's interceptor sees the shell's own forwarding.
     *
     * <p>The write goes to the node that does not own the shard, so the shell forwards it. That forward is
     * an ordinary transport request, and a plugin that asked to see them sees it — which is the mechanism
     * an authenticated user would travel on.
     */
    public void testAPluginsInterceptorSeesForwardedWork() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("wire-owner"));
            ServerlessNode other = new ServerlessNode(nodeSettings("wire-other"), List.of(new WireWatchingPlugin()))
        ) {
            owner.start();
            other.start();
            owner.setMetadataPlane(plane);
            other.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(owner, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            assertTrue("the watching node must hold nothing, so its work has to be forwarded", other.reconciler().openShards().isEmpty());

            SENT.clear();
            other.client()
                .index(new IndexRequest("alpha").id("k").source("{\"msg\":\"forwarded\",\"n\":1}", XContentType.JSON))
                .actionGet();
            assertTrue(
                "a plugin's interceptor must see the forwarded write leave the node: " + SENT,
                SENT.contains("internal:serverless/document/write")
            );

            SENT.clear();
            assertTrue(other.client().get(new GetRequest("alpha", "k")).actionGet().isExists());
            assertTrue("and the forwarded read: " + SENT, SENT.contains("internal:serverless/document/get"));
        }
    }

    /**
     * A plugin's {@code additionalSettings} reaches the node, and knows its place.
     *
     * <p>Both halves matter. The shell used to layer its own transport choice <em>over</em> everything,
     * which would have made this setting decorative for the one thing plugins most use it for — swapping in
     * their own transport. And a plugin must not be able to override what an operator explicitly wrote,
     * which is core's rule and now the shell's.
     */
    public void testAPluginsAdditionalSettingsReachTheNodeButLoseToAnOperator() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("wire-settings"), List.of(new WireWatchingPlugin()))) {
            node.start();
            final Settings seen = SEEN_SETTINGS.get();
            assertNotNull("the plugin's components must have been built", seen);
            assertEquals("a plugin's contributed setting must reach the node", "true", seen.get("serverless.wire_watcher.installed"));
            assertEquals("including one the shell itself reads", "512", seen.get("serverless.block_cache.max_blocks"));
            // The shell's own default is underneath, so it still applies where nobody had an opinion.
            assertEquals("netty4", seen.get("transport.type"));
        }

        SEEN_SETTINGS.set(null);
        final Settings explicit = Settings.builder()
            .put(nodeSettings("wire-explicit"))
            .put("serverless.block_cache.max_blocks", 64)
            .build();
        try (ServerlessNode node = new ServerlessNode(explicit, List.of(new WireWatchingPlugin()))) {
            node.start();
            assertEquals(
                "an operator's explicit setting must beat a plugin's contribution",
                "64",
                SEEN_SETTINGS.get().get("serverless.block_cache.max_blocks")
            );
        }
    }

    /** Records that the substituted transport was the one actually built. */
    private static final java.util.concurrent.atomic.AtomicBoolean SUBSTITUTE_USED = new java.util.concurrent.atomic.AtomicBoolean();

    /**
     * A plugin that supplies the node's transport under a name of its own.
     *
     * <p>It delegates to netty4 rather than implementing a {@code Transport}, because implementing one is a
     * project and what is in question here is not whether a transport can be written but whether the shell
     * will use one a plugin supplies. That is the whole mechanism the OpenSearch security plugin needs for
     * TLS: {@code additionalSettings} names its implementation and {@code getTransports} provides it.
     */
    public static final class SubstituteTransportPlugin extends Plugin implements NetworkPlugin {

        private static final String NAME = "substituted";
        private final org.opensearch.transport.Netty4ModulePlugin netty = new org.opensearch.transport.Netty4ModulePlugin();

        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("transport.type", NAME).build();
        }

        @Override
        public java.util.Map<String, java.util.function.Supplier<org.opensearch.transport.Transport>> getTransports(
            Settings settings,
            org.opensearch.threadpool.ThreadPool threadPool,
            org.opensearch.common.util.PageCacheRecycler pageCacheRecycler,
            org.opensearch.core.indices.breaker.CircuitBreakerService circuitBreakerService,
            NamedWriteableRegistry namedWriteableRegistry,
            org.opensearch.common.network.NetworkService networkService,
            org.opensearch.telemetry.tracing.Tracer tracer
        ) {
            final var inner = netty.getTransports(
                settings,
                threadPool,
                pageCacheRecycler,
                circuitBreakerService,
                namedWriteableRegistry,
                networkService,
                tracer
            ).get(org.opensearch.transport.Netty4ModulePlugin.NETTY_TRANSPORT_NAME);
            return java.util.Map.of(NAME, () -> {
                SUBSTITUTE_USED.set(true);
                return inner.get();
            });
        }
    }

    /**
     * A plugin can supply the node's transport, and the node uses it.
     *
     * <p>M29 wired {@code NetworkPlugin}s into the network module and could not show this: proving it needs
     * a transport to substitute, and writing one is a project rather than a fixture. Delegating to netty4
     * under a different name proves the part that was in question — that {@code additionalSettings} selects
     * an implementation and {@code getTransports} provides it — without pretending to have written a TLS
     * stack.
     *
     * <p>The node then has to actually work on it, so the test forwards a write between two nodes: a
     * transport that was selected and then did not carry traffic would be a worse outcome than one that was
     * never selected at all.
     */
    public void testAPluginCanSupplyTheNodesTransport() throws Exception {
        SUBSTITUTE_USED.set(false);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("wire-sub-owner"), List.of(new SubstituteTransportPlugin()));
            ServerlessNode other = new ServerlessNode(nodeSettings("wire-sub-other"), List.of(new SubstituteTransportPlugin()))
        ) {
            owner.start();
            other.start();
            assertTrue("the plugin's transport must be the one built", SUBSTITUTE_USED.get());

            owner.setMetadataPlane(plane);
            other.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(owner, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            // Through the node that does not own the shard, so the write has to cross the transport the
            // plugin supplied.
            other.client()
                .index(new IndexRequest("alpha").id("k").source("{\"msg\":\"substituted\",\"n\":1}", XContentType.JSON))
                .actionGet();
            assertTrue("and it must actually carry traffic", other.client().get(new GetRequest("alpha", "k")).actionGet().isExists());
        }
    }

    /** A node with no network plugin is unchanged, which is every node the suite otherwise starts. */
    public void testANodeWithNoNetworkPluginIsUnchanged() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("wire-none"))) {
            node.start();
            assertNotNull(node.boundHttpAddress().publishAddress());
            assertTrue("nothing can have been intercepted", SENT.isEmpty());
        }
    }

    /** The caller a receiving node saw in the thread context of a forwarded request. */
    private static final AtomicReference<String> RECEIVED_CALLER = new AtomicReference<>();

    /**
     * A plugin that puts a caller into the thread context and looks for it on the other side.
     *
     * <p>Both halves are core's own mechanism: a thread-context <em>header</em> travels with a transport
     * request, unlike a transient, which is exactly how the OpenSearch security plugin moves an
     * authenticated user between nodes.
     */
    public static final class CallerCarryingPlugin extends Plugin implements NetworkPlugin, org.opensearch.plugins.ActionPlugin {

        static final String HEADER = "x-testkit-caller";
        static final AtomicReference<ThreadContext> CONTEXT = new AtomicReference<>();

        @Override
        public java.util.Collection<Object> createComponents(
            org.opensearch.transport.client.Client client,
            org.opensearch.cluster.service.ClusterService clusterService,
            org.opensearch.threadpool.ThreadPool threadPool,
            org.opensearch.watcher.ResourceWatcherService resourceWatcherService,
            org.opensearch.script.ScriptService scriptService,
            org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry,
            org.opensearch.env.Environment environment,
            org.opensearch.env.NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver,
            java.util.function.Supplier<org.opensearch.repositories.RepositoriesService> repositoriesServiceSupplier
        ) {
            CONTEXT.set(threadPool.getThreadContext());
            return List.of(new Object());
        }

        @Override
        public java.util.function.UnaryOperator<org.opensearch.rest.RestHandler> getRestHandlerWrapper(
            ThreadContext threadContext,
            java.util.Set<org.opensearch.rest.RestHeaderDefinition> headersToCopy
        ) {
            return handler -> (request, channel, client) -> {
                final String caller = request.header(HEADER);
                if (caller != null && threadContext.getHeader(HEADER) == null) {
                    threadContext.putHeader(HEADER, caller);
                }
                handler.handleRequest(request, channel, client);
            };
        }

        @Override
        public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry registry, ThreadContext threadContext) {
            return List.of(new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    String executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> actualHandler
                ) {
                    return (request, channel, task) -> {
                        if (action.startsWith("internal:serverless")) {
                            RECEIVED_CALLER.set(threadContext.getHeader(HEADER));
                        }
                        actualHandler.messageReceived(request, channel, task);
                    };
                }
            });
        }
    }

    /**
     * A caller put into the thread context reaches the node a write is forwarded to.
     *
     * <p><b>What was actually missing, stated precisely.</b> The status document said node-to-node
     * forwarding "carries no identity". What is true is narrower and more useful: a thread-context header
     * travels with a forwarded request through core's own mechanism, and this test shows it arriving. What
     * does not happen is the receiving node running the action filters a second time — the decision is made
     * at the node the request reached, and re-running every filter per hop would make a filter that counts
     * or rate-limits wrong rather than merely slow.
     *
     * <p>So a plugin that wants its user on the far side has the mechanism; what it does not have is a
     * second enforcement point, which is a design choice rather than a missing wire.
     */
    public void testACallerInTheThreadContextReachesTheForwardedNode() throws Exception {
        RECEIVED_CALLER.set(null);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("carry-owner"), List.of(new CallerCarryingPlugin()));
            ServerlessNode other = new ServerlessNode(nodeSettings("carry-other"), List.of(new CallerCarryingPlugin()))
        ) {
            owner.start();
            other.start();
            owner.setMetadataPlane(plane);
            other.setMetadataPlane(plane);
            owner.activateWriter(plane, "alpha", 0);
            other.syncFrom(plane);
            assertTrue("the write must have to cross the wire", other.reconciler().openShards().isEmpty());

            final var address = other.boundHttpAddress().publishAddress();
            final java.net.http.HttpResponse<String> response;
            try (java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient()) {
                response = client.send(
                    java.net.http.HttpRequest.newBuilder()
                        .uri(java.net.URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/alpha/_doc/1?refresh=true"))
                        .header("Content-Type", "application/json")
                        .header(CallerCarryingPlugin.HEADER, "priya")
                        .PUT(java.net.http.HttpRequest.BodyPublishers.ofString("{\"msg\":\"forwarded\"}"))
                        .build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString()
                );
            }
            assertEquals(response.body(), 201, response.statusCode());
            assertEquals("the caller must have travelled with the forwarded write", "priya", RECEIVED_CALLER.get());
        }
    }
}
