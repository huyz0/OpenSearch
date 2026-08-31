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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A node that refuses rather than dies.
 *
 * <p>Every component in this shell was given a {@code NoneCircuitBreakerService}, which accounts nothing
 * and refuses nothing. That was invisible while the surface allocated nothing worth bounding, and stopped
 * being invisible when aggregations arrived: an aggregation over a high-cardinality field is the ordinary
 * way to exhaust a node's heap, and a node that dies is worse for every other request than a node that
 * refuses this one.
 *
 * <p>The breakers are core's own hierarchy with core's own defaults, so what this suite has to show is not
 * that the numbers are right — they are OpenSearch's — but that the wiring is real: an allocation that
 * exceeds the limit is refused, the caller is told, and the node is still there afterwards.
 *
 * <p>The limit is set absurdly low by configuration rather than by allocating absurdly much, because a test
 * that has to exhaust a real heap to prove a point is a test that intermittently exhausts the build's.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessCircuitBreakerTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"colour\":{\"type\":\"keyword\"}}}";

    private Settings nodeSettings(String name, String requestLimit) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-breaker")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest");
        if (requestLimit != null) {
            // Accounted rather than measured, so the limit means what it says instead of depending on what
            // the JVM running the build happens to have done with its heap.
            builder.put("indices.breaker.total.use_real_memory", false);
            builder.put("indices.breaker.request.limit", requestLimit);
        }
        return builder.build();
    }

    /** An aggregation past the request limit is refused, and the node keeps serving. */
    public void testAnAggregationPastTheLimitIsRefusedRatherThanFatal() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("bounded", "uuid-bounded-000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("breaker-agg", "1kb"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("bounded", 0);
            loop.tick(clock.get());

            for (int i = 0; i < 5; i++) {
                assertEquals(201, send(node, "PUT", "/bounded/_doc/b" + i + "?refresh=true", "{\"msg\":\"m\",\"colour\":\"c\"}").status());
            }

            final Response refused = send(
                node,
                "POST",
                "/bounded/_search",
                "{\"size\":0,\"aggs\":{\"by_colour\":{\"terms\":{\"field\":\"colour\"}}}}"
            );
            assertNotEquals("an aggregation past the limit must not succeed: " + refused.body(), 200, refused.status());
            assertTrue(
                "the caller must be told it was the breaker, not a mystery: " + refused.body(),
                refused.body().contains("circuit_breaking_exception") || refused.body().contains("Data too large")
            );

            // And the node is still there. A breaker that took the node down with the request would be a
            // more expensive way of doing the thing it exists to prevent.
            final Response after = send(node, "POST", "/bounded/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals("the node must still serve ordinary searches: " + after.body(), 200, after.status());
            assertTrue(after.body().contains("\"value\":5"));
        }
    }

    /**
     * A search no shard could answer is an error, not an empty result.
     *
     * <p>The fan-out reports coverage rather than failing, which is right while some other shard answered.
     * When none did there is nothing to report coverage over, and {@code "total":0} beside
     * {@code "complete":false} is a confident empty answer wearing a disclaimer. The failure that stopped
     * every shard becomes the response instead — with its own status, so a breaker is a 429 rather than
     * everything collapsing into one code.
     */
    public void testASearchNoShardCouldAnswerIsAnError() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("bounded", "uuid-bounded-000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("breaker-none", "1kb"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("bounded", 0);
            loop.want("bounded", 1);
            loop.tick(clock.get());
            assertEquals(201, send(node, "PUT", "/bounded/_doc/1?refresh=true", "{\"msg\":\"m\",\"colour\":\"c\"}").status());

            final Response refused = send(
                node,
                "POST",
                "/bounded/_search",
                "{\"size\":0,\"aggs\":{\"by_colour\":{\"terms\":{\"field\":\"colour\"}}}}"
            );
            assertNotEquals("every shard failed, so this cannot be a 200: " + refused.body(), 200, refused.status());
            assertFalse("and must not look like an empty result: " + refused.body(), refused.body().contains("\"total\":{\"value\":0}"));
        }
    }

    /** With no limit configured, core's defaults apply and ordinary work is unaffected. */
    public void testTheDefaultLimitsDoNotRefuseOrdinaryWork() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("ordinary", "uuid-ordinary-00000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("breaker-default", null))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("ordinary", 0);
            loop.want("ordinary", 1);
            loop.tick(clock.get());

            for (int i = 0; i < 20; i++) {
                assertEquals(
                    201,
                    send(node, "PUT", "/ordinary/_doc/o" + i + "?refresh=true", "{\"msg\":\"m\",\"colour\":\"c" + (i % 3) + "\"}").status()
                );
            }

            final Response aggregated = send(
                node,
                "POST",
                "/ordinary/_search",
                "{\"size\":0,\"aggs\":{\"by_colour\":{\"terms\":{\"field\":\"colour\"}}}}"
            );
            assertEquals("real breakers must not refuse ordinary work: " + aggregated.body(), 200, aggregated.status());
            assertTrue("and it must be complete: " + aggregated.body(), aggregated.body().contains("\"complete\":true"));
        }
    }

    /**
     * A write larger than the node's indexing budget is rejected, and the node keeps serving.
     *
     * <p>Indexing pressure is a different bound from the circuit breaker and both are needed. The breaker
     * accounts what a request allocates while computing an answer; this accounts the bytes of the writes
     * themselves, which are in memory from the moment a batch is parsed until it has been applied. A node
     * with no bound on that is one large enough batch — or enough concurrent ones — away from dying, and
     * dying loses every other request as well.
     *
     * <p>The limit is set to a few bytes by configuration, for the same reason the breaker's is: a test
     * that has to allocate a real tenth of a heap to prove a point is a test that intermittently exhausts
     * the build's.
     */
    public void testAWriteBeyondTheIndexingBudgetIsRejected() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("pressed", "uuid-pressed-000000", 1, MAPPING, null));

        final Settings settings = Settings.builder()
            .put(nodeSettings("pressure", null))
            .put("indexing_pressure.memory.limit", "8b")
            .build();

        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("pressed", 0);
            loop.tick(clock.get());

            final Response tooBig = send(
                node,
                "PUT",
                "/pressed/_doc/1?refresh=true",
                "{\"msg\":\"far more than eight bytes of document\"}"
            );
            assertEquals("a write past the budget must be rejected: " + tooBig.body(), 429, tooBig.status());
            assertTrue(
                "and say it was the indexing budget, not a mystery: " + tooBig.body(),
                tooBig.body().contains("coordinating") || tooBig.body().contains("rejected execution")
            );

            final Response batchTooBig = send(
                node,
                "POST",
                "/_bulk",
                "{\"index\":{\"_index\":\"pressed\",\"_id\":\"b1\"}}\n{\"msg\":\"also far more than eight bytes\"}\n"
            );
            assertEquals("a batch past the budget too: " + batchTooBig.body(), 429, batchTooBig.status());

            final Response ordinary = send(node, "POST", "/pressed/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals("and the node still serves reads: " + ordinary.body(), 200, ordinary.status());
        }
    }

    /**
     * The budget is given back when a write finishes.
     *
     * <p>Accounting that only ever adds is a node that stops accepting writes after a while and never says
     * why — the worst shape a bound can have, because it looks like a limit that is simply too low.
     *
     * <p>The budget here fits a handful of these documents at a time, and forty are written one after
     * another. Every one has to succeed: if the release leaked, the sixth or so would be rejected and every
     * write after it. The first version of this suite could not have caught that, because it only ever made
     * writes that were *refused* — and a refused write never took the budget in the first place. The canary
     * for a leaked release passed, which is how the gap was found.
     */
    public void testTheIndexingBudgetIsReleasedWhenAWriteFinishes() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("recycled", "uuid-recycled-00000", 1, MAPPING, null));

        final Settings settings = Settings.builder()
            .put(nodeSettings("pressure-release", null))
            // Room for a few of these at once and nothing like the total, so a leak runs out well before
            // the end and a working release never does: forty writes of about 35 bytes and ten batches of
            // about 200 are several times this, and every one of them has to succeed.
            .put("indexing_pressure.memory.limit", "400b")
            .build();

        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("recycled", 0);
            loop.tick(clock.get());

            for (int i = 0; i < 40; i++) {
                final Response written = send(node, "PUT", "/recycled/_doc/r" + i, "{\"msg\":\"m\",\"colour\":\"c\"}");
                assertEquals(
                    "write " + i + " must succeed; a budget that is never given back runs out: " + written.body(),
                    201,
                    written.status()
                );
            }

            final StringBuilder batch = new StringBuilder();
            for (int i = 0; i < 3; i++) {
                batch.append("{\"index\":{\"_index\":\"recycled\",\"_id\":\"s")
                    .append(i)
                    .append("\"}}\n{\"msg\":\"m\",\"colour\":\"c\"}\n");
            }
            for (int round = 0; round < 10; round++) {
                final Response bulk = send(node, "POST", "/_bulk", batch.toString());
                assertEquals("batch " + round + " must succeed for the same reason: " + bulk.body(), 200, bulk.status());
                assertFalse("and none of its items rejected: " + bulk.body(), bulk.body().contains("\"error\""));
            }
        }
    }

    /** With no limit configured, core's default applies and ordinary writes are unaffected. */
    public void testTheDefaultIndexingBudgetDoesNotRejectOrdinaryWrites() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("plenty", "uuid-plenty-0000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pressure-default", null))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("plenty", 0);
            loop.tick(clock.get());

            for (int i = 0; i < 25; i++) {
                assertEquals(201, send(node, "PUT", "/plenty/_doc/p" + i + "?refresh=true", "{\"msg\":\"m\",\"colour\":\"c\"}").status());
            }
            final StringBuilder batch = new StringBuilder();
            for (int i = 0; i < 25; i++) {
                batch.append("{\"index\":{\"_index\":\"plenty\",\"_id\":\"q").append(i).append("\"}}\n{\"msg\":\"m\",\"colour\":\"c\"}\n");
            }
            final Response bulk = send(node, "POST", "/_bulk?refresh=true", batch.toString());
            assertEquals("real accounting must not refuse ordinary writes: " + bulk.body(), 200, bulk.status());
            assertFalse("and none of them individually: " + bulk.body(), bulk.body().contains("\"error\""));
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)
                )
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
