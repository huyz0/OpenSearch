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

    /** A node with no network plugin is unchanged, which is every node the suite otherwise starts. */
    public void testANodeWithNoNetworkPluginIsUnchanged() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("wire-none"))) {
            node.start();
            assertNotNull(node.boundHttpAddress().publishAddress());
            assertTrue("nothing can have been intercepted", SENT.isEmpty());
        }
    }
}
