/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.AuxTransport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * The client protocols the shell serves: HTTP/2 in clear text on each of its two HTTP transports, and the
 * auxiliary transports a plugin registers (the gRPC transport is one), which the shell now starts and stops
 * with the node.
 *
 * <p>The shell builds its own {@code NetworkModule}, and nothing started the list of auxiliary transports
 * that module resolves, so {@code aux.transport.types: [transport-grpc]} registered a transport and left it
 * unbound. These tests fail on that.
 */
public class ServerlessProtocolsTests extends OpenSearchTestCase {

    private static final AtomicInteger STARTS = new AtomicInteger();
    private static final AtomicInteger STOPS = new AtomicInteger();

    /** An auxiliary transport that only counts its lifecycle. */
    static final class CountingAuxTransport extends AuxTransport {
        @Override
        public String settingKey() {
            return "counting-aux";
        }

        @Override
        public BoundTransportAddress getBoundAddress() {
            return null;
        }

        @Override
        protected void doStart() {
            STARTS.incrementAndGet();
        }

        @Override
        protected void doStop() {
            STOPS.incrementAndGet();
        }

        @Override
        protected void doClose() {}
    }

    /** A plugin registering {@link CountingAuxTransport}. */
    public static final class AuxPlugin extends Plugin implements NetworkPlugin {
        @Override
        public Map<String, Supplier<AuxTransport>> getAuxTransports(
            Settings settings,
            ThreadPool threadPool,
            CircuitBreakerService circuitBreakerService,
            NetworkService networkService,
            ClusterSettings clusterSettings,
            Tracer tracer
        ) {
            return Map.of("counting-aux", CountingAuxTransport::new);
        }
    }

    private Settings.Builder nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-protocols")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        STARTS.set(0);
        STOPS.set(0);
    }

    /** The shell's default transport (streaming bodies, Reactor Netty) answers HTTP/2 over clear text. */
    public void testHttp2ClearTextOnTheStreamingTransport() throws Exception {
        assertHttp2ClearText(nodeSettings("h2c-streaming").put("serverless.http.streaming", true).build());
    }

    /** The plain Netty 4 transport answers HTTP/2 over clear text. */
    public void testHttp2ClearTextOnTheNettyTransport() throws Exception {
        assertHttp2ClearText(nodeSettings("h2c-netty").put("serverless.http.streaming", false).build());
    }

    private void assertHttp2ClearText(Settings settings) throws Exception {
        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.startup().start();
            final int port = node.boundHttpAddress().publishAddress().getPort();
            final URI uri = URI.create("http://127.0.0.1:" + port + "/_serverless/health");
            final HttpClient h2 = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).connectTimeout(Duration.ofSeconds(10)).build();
            final HttpResponse<String> over2 = h2.send(
                HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString()
            );
            assertEquals("the request must be served over HTTP/2", HttpClient.Version.HTTP_2, over2.version());
            assertEquals(200, over2.statusCode());

            final HttpClient h1 = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
            final HttpResponse<String> over1 = h1.send(
                HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString()
            );
            assertEquals(HttpClient.Version.HTTP_1_1, over1.version());
            assertEquals("the answer is the same over both versions", over1.body(), over2.body());
        }
    }

    /** An auxiliary transport an operator enables is started with the node and stopped with it. */
    public void testAnEnabledAuxiliaryTransportIsStartedAndStoppedWithTheNode() throws Exception {
        final Settings settings = nodeSettings("aux-on").putList("aux.transport.types", "counting-aux").build();
        try (ServerlessNode node = new ServerlessNode(settings, List.of(new AuxPlugin()))) {
            assertEquals("built but not started before the node starts", 0, STARTS.get());
            node.startup().start();
            assertEquals("started once with the node", 1, STARTS.get());
            assertEquals(0, STOPS.get());
        }
        assertEquals("stopped when the node closes", 1, STOPS.get());
    }

    /** With nothing enabled, a registered auxiliary transport is neither built nor started: no port opens. */
    public void testARegisteredAuxiliaryTransportIsOffUntilEnabled() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("aux-off").build(), List.of(new AuxPlugin()))) {
            node.startup().start();
            assertEquals(0, STARTS.get());
        }
        assertEquals(0, STOPS.get());
    }

    /** Naming a transport nothing registered fails the node's construction, as on a classic node. */
    public void testAnUnknownAuxiliaryTransportRefusesToStart() {
        final Settings settings = nodeSettings("aux-unknown").putList("aux.transport.types", "no-such-transport").build();
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> new ServerlessNode(settings).close());
        assertTrue(e.getMessage(), e.getMessage().contains("no-such-transport"));
    }
}
