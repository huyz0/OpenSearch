/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * How long a write waits on an owner that has stopped answering, measured rather than inferred.
 *
 * <p><b>Why this exists beside the process test.</b> {@code ServerlessContentionTests} freezes a real
 * process with SIGSTOP, which is the honest reproduction and is also two forked JVMs, an HTTP client
 * timeout and a wall-clock assertion stacked on each other. When it failed once under load, the useful
 * question — is the bound wrong, or is the harness thin? — could not be answered from what it printed,
 * because the client gave up before the node did and the number that mattered was never measured.
 *
 * <p>This measures the same bound with nothing in the way: one JVM, a real transport connection that is
 * already established, and an owner whose handler simply never answers. No process control, no HTTP
 * timeout racing the thing under test, and a failure reports the elapsed milliseconds rather than an
 * exception saying somebody else gave up first.
 *
 * <p><b>The bound is the lease TTL</b>, and that is a design choice rather than a tuning: waiting longer
 * than the lease is waiting for a node whose ownership has already lapsed, and waiting less would fail
 * writes that a briefly slow owner would have completed.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessForwardBoundTests extends OpenSearchTestCase {

    private static final long TTL = 8_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    /** Set while the owner is refusing to answer forwarded writes. */
    private static final AtomicBoolean SWALLOWING = new AtomicBoolean();

    /** A plugin that lets the owner stop answering without closing the connection. */
    public static final class SwallowingPlugin extends Plugin implements NetworkPlugin {

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
                        if (SWALLOWING.get() && action.equals(org.opensearch.serverless.transport.ForwardedIndexRequest.ACTION)) {
                            // Received and never answered: a process frozen mid-request, without the
                            // connection going away. Dropping the request is the whole fixture.
                            return;
                        }
                        actualHandler.messageReceived(request, channel, task);
                    };
                }
            });
        }
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-forward-bound")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /**
     * A write to an owner that has stopped answering gives up at the lease, on a warm connection.
     *
     * <p>The connection is established by a successful forward first, so nothing here is waiting on a
     * handshake — the wait is for a response that never comes, which is the case the process test calls
     * "warm" and the one that failed under load.
     */
    public void testAWarmForwardToASilentOwnerGivesUpAtTheLease() throws Exception {
        SWALLOWING.set(false);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("bound-owner"), List.of(new SwallowingPlugin()));
            ServerlessNode other = new ServerlessNode(nodeSettings("bound-other"), List.of(new SwallowingPlugin()))
        ) {
            owner.start();
            other.start();
            owner.setMetadataPlane(plane);
            other.setMetadataPlane(plane);
            owner.activateWriter(plane, "alpha", 0);
            other.syncFrom(plane);
            assertTrue("the write must have to cross the wire", other.reconciler().openShards().isEmpty());

            // A successful forward first, so the connection is open when the owner goes quiet.
            assertEquals(201, send(other, "PUT", "/alpha/_doc/warm", "{\"msg\":\"before\"}").status());

            SWALLOWING.set(true);
            final long startedAt = System.nanoTime();
            final Response refused = send(other, "PUT", "/alpha/_doc/during", "{\"msg\":\"during\"}");
            final long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
            SWALLOWING.set(false);

            logger.info("a warm forward to a silent owner returned {} after {}ms (lease ttl {}ms)", refused.status(), elapsed, TTL);

            assertNotEquals("a write nobody answered must not report success: " + refused.body(), 201, refused.status());
            assertTrue(
                "it must give up rather than hang, and the bound is the lease: took " + elapsed + "ms against a " + TTL + "ms lease",
                elapsed < TTL * 2
            );
            assertTrue(
                "and it must actually have waited for the owner rather than failing immediately: took " + elapsed + "ms",
                elapsed > TTL / 2
            );
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build()) {
            // Deliberately far above the bound under test: a client that gave up first would replace the
            // measurement with an exception, which is exactly what made the process test hard to diagnose.
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(120))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
