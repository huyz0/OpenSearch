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
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M11: the data path. A document can be written and found over HTTP, by a user, with nothing else.
 *
 * <p>Everything M10 built sat behind a Java method until now — the durable write path existed and
 * nothing outside a test could reach it. These tests use only HTTP.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessDataPathTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-m11")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private record Response(int status, String body) {
    }

    private static Response send(TransportAddress address, String method, String path, String body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    private MetadataPlane planeOver(java.nio.file.Path dir, AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    /** The milestone, in one test: write a document over HTTP and find it over HTTP. */
    public void testWriteAndSearchOverHttp() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m11-a"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            assertEquals(200, send(http, "PUT", "/library?shards=1", MAPPING).status());
            node.activateWriter(plane, "library", 0);

            final Response written = send(
                http,
                "PUT",
                "/library/_doc/book-1?refresh=true",
                "{\"msg\":\"the object store is the truth\",\"n\":1}"
            );
            assertEquals(201, written.status());
            assertTrue("the write must say what it guaranteed: " + written.body(), written.body().contains("write-ahead log"));

            final Response found = send(http, "GET", "/library/_search?q=msg:truth", null);
            assertEquals(200, found.status());
            assertTrue("the document was not found: " + found.body(), found.body().contains("\"value\":1"));
            assertTrue("the hit must carry its id: " + found.body(), found.body().contains("book-1"));
            assertTrue("the hit must carry its source: " + found.body(), found.body().contains("object store is the truth"));
            assertTrue("a fully-covered search must say so: " + found.body(), found.body().contains("\"complete\":true"));
        }
    }

    /**
     * M10 and M11 together: a write that arrives over HTTP and is never published still survives its
     * writer dying.
     */
    public void testAWriteOverHttpSurvivesTheWriterDying() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        final ServerlessNode a = new ServerlessNode(nodeSettings("m11-durable"));
        a.start();
        a.setMetadataPlane(plane);
        final TransportAddress http = a.boundHttpAddress().publishAddress();
        send(http, "PUT", "/library?shards=1", MAPPING);
        a.activateWriter(plane, "library", 0);

        assertEquals(201, send(http, "PUT", "/library/_doc/only?refresh=true", "{\"msg\":\"never published\",\"n\":1}").status());

        // No tick, so no publication has ever run. The process dies.
        a.close();

        clock.set(1_000L + TTL);
        try (ServerlessNode b = new ServerlessNode(nodeSettings("m11-successor"))) {
            b.start();
            b.setMetadataPlane(plane);
            final var shardId = b.activateWriter(plane, "library", 0).orElseThrow();
            assertEquals(
                "a write acknowledged over HTTP was lost when its writer died",
                1L,
                ShardOps.hits(b.searchService(), shardId, "msg", "published")
            );
        }
    }

    /** A partial search says so. This is the property the class exists to hold. */
    public void testASearchThatReachedSomeShardsSaysSo() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m11-partial"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            assertEquals(200, send(http, "PUT", "/wide?shards=3", MAPPING).status());
            // Only one of the three shards is held here.
            node.activateWriter(plane, "wide", 0);

            final Response partial = send(http, "GET", "/wide/_search?q=msg:anything", null);
            assertEquals(200, partial.status());
            assertTrue("coverage must be reported: " + partial.body(), partial.body().contains("\"total\":3"));
            assertTrue("coverage must be reported: " + partial.body(), partial.body().contains("\"searched\":1"));
            assertTrue("a partial search must not look complete: " + partial.body(), partial.body().contains("\"complete\":false"));
        }
    }

    /** A write to any node reaches the shard's owner, and the response says which node held it. */
    public void testAWriteToTheWrongNodeIsForwardedToTheOwner() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("m11-owner"));
            ServerlessNode other = new ServerlessNode(nodeSettings("m11-other"))
        ) {
            owner.start();
            other.start();
            owner.setMetadataPlane(plane);
            other.setMetadataPlane(plane);
            final TransportAddress ownerHttp = owner.boundHttpAddress().publishAddress();
            final TransportAddress otherHttp = other.boundHttpAddress().publishAddress();

            send(ownerHttp, "PUT", "/library?shards=1", MAPPING);
            owner.activateWriter(plane, "library", 0);

            // The client should not need to know which node owns which shard; that is what the
            // shard-head is for. Sending to the wrong node must work, not merely fail helpfully.
            final Response forwarded = send(otherHttp, "PUT", "/library/_doc/x?refresh=true", "{\"msg\":\"forwarded\",\"n\":1}");
            assertEquals(201, forwarded.status());
            assertTrue(
                "the response must say which node actually held the shard: " + forwarded.body(),
                forwarded.body().contains(owner.localNode().getId())
            );

            // And the document is really on the owner, not merely acknowledged by the other node.
            assertEquals(1L, ShardOps.hits(owner.searchService(), owner.reconciler().openShards().iterator().next(), "msg", "forwarded"));
        }
    }

    /** Routing is the data plane's own function, so a document lands where a search will look. */
    public void testDocumentsRouteToTheShardTheDataPlaneWouldChoose() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m11-routing"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/spread?shards=3", MAPPING);

            final var loop = new BackgroundReconciler(node, plane);
            for (int shard = 0; shard < 3; shard++) {
                loop.want("spread", shard);
            }
            loop.tick(clock.get());
            assertEquals("the node should hold all three shards", 3, node.reconciler().openShards().size());

            for (int i = 0; i < 12; i++) {
                assertEquals(
                    201,
                    send(http, "PUT", "/spread/_doc/doc-" + i + "?refresh=true", "{\"msg\":\"spread\",\"n\":" + i + "}").status()
                );
            }

            final Response all = send(http, "GET", "/spread/_search?q=msg:spread&size=20", null);
            assertTrue("every document must be findable: " + all.body(), all.body().contains("\"value\":12"));
            assertTrue(all.body().contains("\"complete\":true"));
        }
    }

    /** The milestone's other half: a search on one node covers shards held by another. */
    public void testSearchFansOutAcrossNodes() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("m11-fan-a"));
            ServerlessNode b = new ServerlessNode(nodeSettings("m11-fan-b"))
        ) {
            a.start();
            b.start();
            a.setMetadataPlane(plane);
            b.setMetadataPlane(plane);
            final TransportAddress aHttp = a.boundHttpAddress().publishAddress();
            final TransportAddress bHttp = b.boundHttpAddress().publishAddress();

            send(aHttp, "PUT", "/split?shards=2", MAPPING);
            // One shard each. Neither node can answer a whole search alone.
            assertTrue(a.activateWriter(plane, "split", 0).isPresent());
            assertTrue(b.activateWriter(plane, "split", 1).isPresent());

            // Written through one node; forwarding puts each document on whichever node owns its shard.
            for (int i = 0; i < 10; i++) {
                assertEquals(
                    201,
                    send(aHttp, "PUT", "/split/_doc/d" + i + "?refresh=true", "{\"msg\":\"fanout\",\"n\":" + i + "}").status()
                );
            }

            // Asked of either node, the answer covers both shards and says so.
            for (TransportAddress http : new TransportAddress[] { aHttp, bHttp }) {
                final Response found = send(http, "GET", "/split/_search?q=msg:fanout&size=20", null);
                assertEquals(200, found.status());
                assertTrue("fan-out did not cover both shards: " + found.body(), found.body().contains("\"searched\":2"));
                assertTrue("a fully-covered search must say so: " + found.body(), found.body().contains("\"complete\":true"));
                assertTrue("documents were lost across the fan-out: " + found.body(), found.body().contains("\"value\":10"));
            }
        }
    }

    /**
     * Placement is a hint: a search node holding nothing serves a shard by opening it from the manifest.
     *
     * <p>This is what stops placement from becoming a requirement — and therefore from reinventing the
     * stateful cluster this design exists to escape.
     */
    public void testASearchNodeHoldingNothingStillServes() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        final ServerlessNode writer = new ServerlessNode(nodeSettings("m11-w"));
        writer.start();
        writer.setMetadataPlane(plane);
        final TransportAddress writerHttp = writer.boundHttpAddress().publishAddress();
        send(writerHttp, "PUT", "/cold?shards=1", MAPPING);
        final var shardId = writer.activateWriter(plane, "cold", 0).orElseThrow();
        assertEquals(201, send(writerHttp, "PUT", "/cold/_doc/1?refresh=true", "{\"msg\":\"cached\",\"n\":1}").status());
        writer.publishShard(shardId, plane.heads().read("cold", 0).orElseThrow().term());
        writer.close();

        // A search-only node that has never seen this index, and no writer is left alive.
        final Settings searchOnly = Settings.builder()
            .put("node.name", "m11-s")
            .put("cluster.name", "serverless-m11")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "search")
            .build();
        try (ServerlessNode searcher = new ServerlessNode(searchOnly)) {
            searcher.start();
            searcher.setMetadataPlane(plane);
            final Response found = send(searcher.boundHttpAddress().publishAddress(), "GET", "/cold/_search?q=msg:cached", null);
            assertEquals(200, found.status());
            assertTrue("a cold search node served nothing: " + found.body(), found.body().contains("\"value\":1"));
            assertTrue(found.body().contains("\"complete\":true"));
        }
    }

    public void testBadRequestsAreRefusedPlainly() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m11-bad"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            assertEquals(404, send(http, "PUT", "/ghost/_doc/1", "{\"msg\":\"x\"}").status());
            assertEquals(404, send(http, "GET", "/ghost/_search?q=msg:x", null).status());

            send(http, "PUT", "/library?shards=1", MAPPING);
            assertEquals("a write with no body must be refused", 400, send(http, "PUT", "/library/_doc/1", null).status());
            assertEquals("a search with no query must be refused", 400, send(http, "GET", "/library/_search", null).status());
        }
    }
}
