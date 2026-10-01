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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A mapping or settings change made through one node is in force on another node's open shard for the first
 * operation after it, with no reconcile pass in between.
 *
 * <p><b>Why this exists.</b> Every node re-read the descriptor of every index it held open every 30 s, so a change
 * made elsewhere arrived within 30 s -- and that re-read was most of an idle node's store traffic. The re-read is
 * now a five-minute backstop, and what carries a change is the incarnation fence, which every write, get and
 * search passes before it runs. These tests run with the production interval and never tick the reconciler,
 * so only the fence can make them pass.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessDescriptorPropagationTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-descriptor-propagation")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private record Answer(int status, String body) {
        boolean has(String fragment) {
            return body.contains(fragment);
        }
    }

    private static Answer call(ServerlessNode node, String method, String path, String body) throws Exception {
        final int port = node.boundHttpAddress().publishAddress().getPort();
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .header("Content-Type", "application/json")
                    .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(30))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return new Answer(response.statusCode(), response.body());
        }
    }

    private ServerlessNode started(MetadataPlane plane, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        node.setDescriptorRefreshIntervalMillis(ServerlessBootstrap.DEFAULT_DESCRIPTOR_REFRESH_INTERVAL_MILLIS);
        return node;
    }

    /** The owner of alpha, open and written to once, and a second node that holds nothing. */
    private record Pair(ServerlessNode owner, ServerlessNode other) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            org.opensearch.common.util.io.IOUtils.close(other, owner);
        }
    }

    private Pair pair() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final ServerlessNode owner = started(plane, "owner");
        final ServerlessNode other = started(plane, "other");
        final Answer created = call(
            owner,
            "PUT",
            "/alpha",
            "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}"
        );
        assertEquals(created.body(), 200, created.status());
        final BackgroundReconciler loop = new BackgroundReconciler(owner, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        assertEquals(201, call(owner, "PUT", "/alpha/_doc/warm", "{\"msg\":\"warm\"}").status());
        return new Pair(owner, other);
    }

    /** Past the fence window, so the owner's next operation reads the descriptor rather than trusting its last read. */
    private static void pastTheFenceWindow() {
        final long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ServerlessNode.INCARNATION_FENCE_MILLIS + 100);
        while (System.nanoTime() < until) {
            java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
    }

    /**
     * A field declared as {@code keyword} through another node is indexed as one by the owner's very next write.
     *
     * <p>An owner still on the old mapping would map the field dynamically, as text, and the growth would then
     * conflict with the descriptor and fail the write; or, had the field been unconstrained, index it as text,
     * so the exact-value query below would find nothing.
     */
    public void testAMappingChangeMadeElsewhereIsInForceForTheNextWrite() throws Exception {
        try (Pair p = pair()) {
            final Answer mapped = call(p.other(), "PUT", "/alpha/_mapping", "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}");
            assertEquals(mapped.body(), 200, mapped.status());
            pastTheFenceWindow();

            final Answer written = call(p.owner(), "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"tag\":\"Hello World\"}");
            assertEquals("the owner's next write uses the declared mapping: " + written.body(), 201, written.status());
            final Answer exact = call(p.owner(), "POST", "/alpha/_search", "{\"query\":{\"term\":{\"tag\":\"Hello World\"}}}");
            assertTrue("and indexes the field as a keyword: " + exact.body(), exact.has("\"value\":1"));
        }
    }

    /**
     * A dynamic setting changed through another node reaches the owner's open shard by its next read.
     *
     * <p>The assertion is the shard's own settings object, not the descriptor, for the reason
     * {@code ServerlessSettingsUpdateTests} gives.
     */
    public void testASettingsChangeMadeElsewhereIsInForceForTheNextRead() throws Exception {
        try (Pair p = pair()) {
            final ShardId shardId = p.owner().reconciler().openShards().iterator().next();
            final var indexService = p.owner().indicesService().indexService(shardId.getIndex());
            assertNotNull(indexService);

            final Answer updated = call(p.other(), "PUT", "/alpha/_settings", "{\"index\":{\"refresh_interval\":\"37s\"}}");
            assertEquals(updated.body(), 200, updated.status());
            pastTheFenceWindow();

            final Answer got = call(p.owner(), "GET", "/alpha/_doc/warm", null);
            assertEquals(got.body(), 200, got.status());
            assertEquals(
                "the owner's open shard carries the new value after one get",
                org.opensearch.common.unit.TimeValue.timeValueSeconds(37),
                indexService.getIndexSettings().getRefreshInterval()
            );
        }
    }

    /**
     * A write the owner refuses because its shard was behind the mapping neither repeats on retry nor fails every
     * later write that grows the mapping.
     *
     * <p>The owner's fence is widened so it trusts its last read and its shard stays behind: the window a real
     * node has for up to a second after a change. A document naming the newly declared field is mapped
     * dynamically, as text, and the growth conflicts with the descriptor's keyword. That addition went back on the
     * node's queue of additions to make, and every later growth of the index on that node merged it again and
     * failed with it.
     */
    public void testAWriteRefusedForAStaleMappingNeitherRepeatsNorPoisonsLaterGrowth() throws Exception {
        try (Pair p = pair()) {
            p.owner().setIncarnationFenceMillis(TimeUnit.MINUTES.toMillis(10));
            assertEquals(200, call(p.other(), "PUT", "/alpha/_mapping", "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}").status());

            final Answer refused = call(p.owner(), "PUT", "/alpha/_doc/1", "{\"msg\":\"x\",\"tag\":\"Hello World\"}");
            assertTrue("a shard behind the mapping cannot grow it into a conflict: " + refused.body(), refused.status() >= 400);

            final Answer grown = call(p.owner(), "PUT", "/alpha/_doc/2", "{\"msg\":\"y\",\"fresh\":\"field\"}");
            assertEquals("a later document naming a new field still grows the mapping: " + grown.body(), 201, grown.status());
            final Answer retried = call(p.owner(), "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"tag\":\"Hello World\"}");
            assertEquals("and the refused write succeeds when retried: " + retried.body(), 201, retried.status());
            final Answer exact = call(p.owner(), "POST", "/alpha/_search", "{\"query\":{\"term\":{\"tag\":\"Hello World\"}}}");
            assertTrue("as a keyword: " + exact.body(), exact.has("\"value\":1"));
        }
    }
}
