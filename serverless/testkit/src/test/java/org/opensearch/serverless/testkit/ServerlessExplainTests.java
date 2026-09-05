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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M60: {@code _explain}, and the two answers it has to keep apart.
 *
 * <p><b>The refusal it replaces was another stale reason.</b> It said explaining a score "needs the scorer for
 * one document on one shard; the fan-out here merges hits rather than exposing per-shard scoring internals" —
 * true of nothing. An explain names a document, a named document lives on one shard, and there is no fan-out
 * to merge. It is strictly cheaper than the search whose score it accounts for.
 *
 * <p><b>The hard half is the document that exists and does not match.</b> That is a 200 saying so, with
 * Lucene's account of which clause failed — not a 404. Collapsing it into "not found" would answer the
 * question a caller can already answer with a get.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessExplainTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private int port;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-explain")
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

    private Answer call(String method, String path, String body) throws Exception {
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

    private static MetadataPlane plane(AtomicLong clock, Path store) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, store, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private ServerlessNode running(MetadataPlane plane, AtomicLong clock, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        port = node.boundHttpAddress().publishAddress().getPort();
        call("PUT", "/books", "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}");
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want("books", 0);
        loop.tick(clock.get());
        call("PUT", "/books/_doc/1?refresh=true", "{\"title\":\"the sea wall\"}");
        call("PUT", "/books/_doc/2?refresh=true", "{\"title\":\"a stone bridge\"}");
        return node;
    }

    /** A document that matches: the score is accounted for, term by term. */
    public void testAMatchingDocumentIsExplained() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "explain-match")) {
            assertNotNull(node);

            final Answer explained = call("POST", "/books/_explain/1", "{\"query\":{\"match\":{\"title\":\"sea\"}}}");
            assertEquals(explained.body(), 200, explained.status());
            assertTrue(explained.body(), explained.has("\"matched\":true"));
            assertTrue("an explanation, not just a verdict: " + explained.body(), explained.has("\"explanation\""));
            // Lucene's own account, which names the term it scored. If this were computed here rather than
            // taken from the searcher, that word would not appear.
            assertTrue("Lucene's description, naming the term: " + explained.body(), explained.has("title:sea"));
            assertTrue("with a real score: " + explained.body(), explained.has("\"value\""));
            assertFalse("a matching document scores above zero: " + explained.body(), explained.has("\"value\":0.0"));
        }
    }

    /**
     * A document that exists and does not match is a 200 saying so.
     *
     * <p>This is the half of the endpoint that a get cannot answer, and the half a "not found" would destroy.
     */
    public void testADocumentThatDoesNotMatchIsExplainedToo() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "explain-nomatch")) {
            assertNotNull(node);

            final Answer explained = call("POST", "/books/_explain/2", "{\"query\":{\"match\":{\"title\":\"sea\"}}}");
            assertEquals("it exists, so it is a 200: " + explained.body(), 200, explained.status());
            assertTrue(explained.body(), explained.has("\"matched\":false"));
            // And it still says why, rather than returning an empty body with a false in it. Lucene's
            // account of a non-match is terse -- "no matching term", scored zero -- because there is no
            // scoring to break down. Terse is not empty: it distinguishes a document that failed the query
            // from one that is not there, which is the distinction this endpoint exists to draw.
            assertTrue("the non-match is explained: " + explained.body(), explained.has("\"explanation\""));
            assertTrue("saying why it did not match: " + explained.body(), explained.has("no matching term"));
            assertTrue("and scored as one: " + explained.body(), explained.has("\"value\":0.0"));
        }
    }

    /** A document that is not there is a 404, which is a different answer from not matching. */
    public void testAMissingDocumentIsNotFound() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "explain-missing")) {
            assertNotNull(node);

            final Answer missing = call("POST", "/books/_explain/99", "{\"query\":{\"match\":{\"title\":\"sea\"}}}");
            assertEquals(missing.body(), 404, missing.status());
            assertTrue(missing.body(), missing.has("\"matched\":false"));
            assertFalse("nothing to explain about a document that is not there: " + missing.body(), missing.has("\"explanation\""));

            assertEquals(
                "an index that is not there is also a 404",
                404,
                call("POST", "/ghost/_explain/1", "{\"query\":{\"match_all\":{}}}").status()
            );
        }
    }

    /**
     * Which copy scored it, said plainly.
     *
     * <p>A score is a function of the whole shard's term and document frequencies, not of the document alone.
     * A caller comparing this number against a search result has to know the two were computed over the same
     * segments, and this node's copy may be an owner's live segments or a published commit.
     */
    public void testTheAnsweringCopyIsDisclosed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "explain-copy")) {
            assertNotNull(node);

            final Answer explained = call("POST", "/books/_explain/1", "{\"query\":{\"match\":{\"title\":\"sea\"}}}");
            assertTrue("the owner answered, so it is realtime: " + explained.body(), explained.has("\"realtime\":true"));
            assertTrue("and which node: " + explained.body(), explained.has("\"_node\":\"" + node.localNode().getId() + "\""));
        }
    }

    /**
     * The other copy: a shard nobody owns is explained from its published commit, and says so.
     *
     * <p>Without this case the disclosure above proves nothing — every path it exercises is realtime, so a
     * hardcoded {@code true} would pass it. This is the path where the flag carries information, and it is
     * the path where a caller most needs it: these scores were computed over the segments in one commit, and
     * a writer taking the shard back would score the same query over more of them.
     */
    public void testAnUnownedShardIsExplainedFromItsPublishedCommit() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());

        final ServerlessNode writer = new ServerlessNode(nodeSettings("explain-dormant-writer"));
        writer.start();
        writer.setMetadataPlane(plane);
        port = writer.boundHttpAddress().publishAddress().getPort();
        call("PUT", "/books", "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}");
        final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
        loop.want("books", 0);
        loop.tick(clock.get());
        assertEquals(201, call("PUT", "/books/_doc/1", "{\"title\":\"the sea wall\"}").status());
        assertEquals(
            "the document must actually be published, or there is nothing to explain against",
            1,
            loop.tick(clock.get() + 1_000).published().size()
        );

        final var held = writer.reconciler().openShards().iterator().next();
        assertTrue("the writer should release the head it holds", plane.heads().release("books", 0, writer.localNode().getId()));
        writer.releaseShard(held, "test: scaling to zero");
        writer.close();
        assertTrue("nobody should own the shard now", plane.heads().read("books", 0).map(h -> h.ownerNodeId()).isEmpty());

        try (ServerlessNode reader = new ServerlessNode(nodeSettings("explain-dormant-reader"))) {
            reader.start();
            reader.setMetadataPlane(plane);
            port = reader.boundHttpAddress().publishAddress().getPort();

            final Answer explained = call("POST", "/books/_explain/1", "{\"query\":{\"match\":{\"title\":\"sea\"}}}");
            assertEquals("an unowned shard must be explained, not refused: " + explained.body(), 200, explained.status());
            assertTrue(explained.body(), explained.has("\"matched\":true"));
            assertTrue("scored from the commit's own statistics: " + explained.body(), explained.has("title:sea"));
            assertTrue("and it says which copy scored it: " + explained.body(), explained.has("\"realtime\":false"));
        }
    }

    /**
     * An explain for a shard this node does not own is answered by the node that does.
     *
     * <p><b>Forwarded rather than served from the commit, following a get and not a search.</b> A search
     * forwarded to a node that has since lost the shard opens it as a reader, because a search's answer is a
     * set of hits and a slightly older set is a partial answer with a number attached. A score is not like
     * that: it is computed from the whole shard's term and document frequencies, so scoring against a
     * published commit while a writer holds newer segments returns a plausible number that is not the number
     * a search would give. Both requests here go through node a, which owns neither.
     *
     * <p>The explanation itself crosses the wire, so this also covers the one genuinely new piece of
     * serialization: a tree of values and descriptions, written whole rather than flattened to text.
     */
    public void testAnExplainIsAnsweredByTheShardOwner() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("explain-fwd-a"));
            ServerlessNode b = new ServerlessNode(nodeSettings("explain-fwd-b"))
        ) {
            a.start();
            b.start();
            a.setMetadataPlane(plane);
            b.setMetadataPlane(plane);
            port = a.boundHttpAddress().publishAddress().getPort();
            call("PUT", "/books", "{\"settings\":{\"number_of_shards\":2},\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}");
            final BackgroundReconciler loopA = new BackgroundReconciler(a, plane);
            loopA.want("books", 0);
            loopA.tick(clock.get());
            final BackgroundReconciler loopB = new BackgroundReconciler(b, plane);
            loopB.want("books", 1);
            loopB.tick(clock.get());

            final var descriptor = plane.describe("books").orElseThrow();
            final String remote = idRoutingTo(descriptor, 1);
            assertTrue(
                "the fixture must target a shard node a does not hold",
                a.reconciler().openShards().stream().noneMatch(shard -> shard.id() == 1)
            );

            assertEquals(201, call("PUT", "/books/_doc/" + remote + "?refresh=true", "{\"title\":\"the sea wall\"}").status());

            final Answer explained = call("POST", "/books/_explain/" + remote, "{\"query\":{\"match\":{\"title\":\"sea\"}}}");
            assertEquals("an explain for a remote shard must be forwarded, not refused: " + explained.body(), 200, explained.status());
            assertTrue(explained.body(), explained.has("\"matched\":true"));
            // The tree survived the wire: a bare "value" and "description" would pass a laxer check, and the
            // nesting is the part that is new here.
            assertTrue("the nested explanation came back whole: " + explained.body(), explained.has("\"details\""));
            assertTrue("naming the term it scored: " + explained.body(), explained.has("title:sea"));
            assertTrue("and the owner that answered: " + explained.body(), explained.has(b.localNode().getId()));
            assertTrue("realtime on the far side too: " + explained.body(), explained.has("\"realtime\":true"));
        }
    }

    private static String idRoutingTo(org.opensearch.serverless.cluster.IndexDescriptor descriptor, int shard) {
        for (int i = 0;; i++) {
            final String candidate = "aimed-" + i;
            if (org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor, candidate) == shard) {
                return candidate;
            }
        }
    }

    /** The shorthands, and the one thing that is refused rather than guessed. */
    public void testTheQueryHasToComeFromSomewhere() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "explain-query")) {
            assertNotNull(node);

            // q= means what it means on a search, through the same parser.
            final Answer shorthand = call("GET", "/books/_explain/1?q=title:sea", null);
            assertEquals(shorthand.body(), 200, shorthand.status());
            assertTrue(shorthand.body(), shorthand.has("\"matched\":true"));

            // No match_all default: explaining against nothing in particular would hand back a constant
            // score that reads like a real result.
            final Answer nothing = call("GET", "/books/_explain/1", null);
            assertEquals(nothing.body(), 400, nothing.status());
            assertTrue("saying what to send: " + nothing.body(), nothing.has("q= query string"));

            final Answer bodyless = call("POST", "/books/_explain/1", "{\"size\":1}");
            assertEquals(bodyless.body(), 400, bodyless.status());
            assertTrue("a body without a query is refused by name: " + bodyless.body(), bodyless.has("must contain a query"));
        }
    }

    /** It is served, so it is no longer refused. */
    public void testExplainIsNoLongerRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "explain-served")) {
            assertNotNull(node);
            assertNotEquals(
                "the refusal must be gone, not shadowed",
                501,
                call("POST", "/books/_explain/1", "{\"query\":{\"match_all\":{}}}").status()
            );
        }
    }
}
