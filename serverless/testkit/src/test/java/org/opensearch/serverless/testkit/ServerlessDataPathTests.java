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
            assertTrue("coverage must be reported: " + partial.body(), partial.body().contains("\"successful\":1"));
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
                assertTrue("fan-out did not cover both shards: " + found.body(), found.body().contains("\"successful\":2"));
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
            // A search with no body is match_all, as in core; a body that does not parse is the bad request.
            assertEquals(
                "a search that does not parse must be refused",
                400,
                send(http, "POST", "/library/_search", "{\"query\":{\"no_such_query\":{}}}").status()
            );
        }
    }

    /**
     * A get on a fresh index, before its first write has ever landed, is a plain miss.
     *
     * <p><b>Found while testing something else.</b> An index that exists and has never had a writer
     * activate has no owner and no published commit. The get path's "no owner" branch used to assume that
     * meant it could safely open a reader on the published commit — which is right once something has been
     * published, and wrong here, where nothing ever has: {@code openReader} refused with an internal
     * "cannot serve as a reader" message, and nothing caught it, so it surfaced as a 500. A document that
     * has never existed answering 500 instead of a plain miss is exactly the kind of internal-invariant
     * leak this project keeps finding at its edges.
     */
    public void testAGetOnAFreshIndexIsAMissNotAServerError() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m15-fresh-get"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();

            send(http, "PUT", "/fresh?shards=1", MAPPING);
            final Response got = send(http, "GET", "/fresh/_doc/never-written", null);
            assertEquals("a document in an index nobody has ever written to must be a miss: " + got.body(), 404, got.status());
            assertTrue("and say so plainly: " + got.body(), got.body().contains("\"found\":false"));

            // And a write afterward still works -- this is a get on empty state, not a broken index.
            // Activated explicitly: the first write to an index with no owner is itself what triggers
            // activation, asynchronously, so it is refused rather than served -- a routing signal this
            // test is not about, and testAWriteToTheWrongNodeIsForwardedToTheOwner is.
            node.activateWriter(plane, "fresh", 0);
            assertEquals(201, send(http, "PUT", "/fresh/_doc/1", "{\"msg\":\"x\"}").status());
        }
    }

    /**
     * A conditional write is refused, not silently turned unconditional.
     *
     * <p><b>This is the difference between an unimplemented endpoint and a wrong answer.</b> A caller
     * sending {@code if_seq_no} expects a 409 if the document has changed underneath it, and the failure
     * mode of ignoring the parameter is not "feature missing" — it is a client believing it holds a
     * compare-and-swap and silently clobbering a concurrent write. That is worse than every other refusal
     * on this surface, because it looks like success.
     *
     * <p>Both action shapes are asserted: the single-document path, where the parameters ride the query
     * string, and bulk, where they ride the action line. A refusal that covered one and not the other
     * would leave exactly the gap a client migrating between the two would fall into.
     */
    public void testConditionalWritesAreRefusedNotIgnored() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m15-conditional"))) {
            node.start();
            node.setMetadataPlane(plane);
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/conditional?shards=1", MAPPING);
            node.activateWriter(plane, "conditional", 0);

            // External versioning is still refused, and still says why -- it asks this system to order
            // writes by a number it does not maintain, which optimistic concurrency does not.
            final Response externalVersion = send(http, "PUT", "/conditional/_doc/1?version=3", "{\"msg\":\"x\"}");
            assertEquals("version= must still be refused: " + externalVersion.body(), 501, externalVersion.status());
            assertTrue(
                "and say why, pointing at what does work: " + externalVersion.body(),
                externalVersion.body().contains("external versioning") && externalVersion.body().contains("if_seq_no")
            );

            // Half a condition is refused as a bad request rather than silently treated as none.
            final Response halfCondition = send(http, "PUT", "/conditional/_doc/1?if_primary_term=1", "{\"msg\":\"x\"}");
            assertEquals("half a condition must be refused: " + halfCondition.body(), 400, halfCondition.status());

            // A condition on a document that does not exist yet is a conflict, not a silent write.
            final Response staleCondition = send(http, "PUT", "/conditional/_doc/1?if_seq_no=0&if_primary_term=1", "{\"msg\":\"x\"}");
            assertEquals("a condition against an absent document must conflict: " + staleCondition.body(), 409, staleCondition.status());

            // None of the refused attempts wrote anything.
            assertEquals(404, send(http, "GET", "/conditional/_doc/1", null).status());

            // And a plain write to the same document afterward must still work -- the refusal is per
            // request, not a state the index gets stuck in.
            assertEquals(201, send(http, "PUT", "/conditional/_doc/1", "{\"msg\":\"x\"}").status());

            // Bulk carries conditions on the action line, and since M50 it honours them. What it refuses
            // is what it cannot honour: external versioning, and a condition spelled with the names a
            // response uses rather than the ones a request does. Both are refused per item rather than
            // dropped, which is the property this test is about and which did not change.
            final String bulk = "{\"index\":{\"_index\":\"conditional\",\"_id\":\"2\",\"_seq_no\":0,\"_primary_term\":1}}\n"
                + "{\"msg\":\"y\"}\n"
                + "{\"delete\":{\"_index\":\"conditional\",\"_id\":\"1\",\"_version\":3}}\n";
            final Response bulkResponse = send(http, "POST", "/_bulk", bulk);
            assertEquals(bulkResponse.body(), 200, bulkResponse.status());
            assertTrue(
                "a condition spelled the response's way must be refused, not dropped: " + bulkResponse.body(),
                bulkResponse.body().contains("is what a bulk response reports")
            );
            assertTrue(
                "external versioning must still be refused per item: " + bulkResponse.body(),
                bulkResponse.body().contains("\"status\":501") && bulkResponse.body().contains("external versioning")
            );
            // Neither item was applied: document 2 was never written, and document 1 -- written plainly
            // just above -- was not deleted by the refused conditional delete.
            assertEquals(404, send(http, "GET", "/conditional/_doc/2", null).status());
            assertEquals(200, send(http, "GET", "/conditional/_doc/1", null).status());
        }
    }
}
