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
 * M55: {@code _analyze} and {@code _msearch} — the last two endpoints both compared products ship.
 *
 * <p><b>{@code _analyze} replaces a refusal that was wrong about itself.</b> It used to say "analysis runs
 * inside a shard's mapping, and a node that does not hold a shard of this index cannot answer for it". Analysis
 * needs the mapping, and any node can read one — the same observation that made {@code _field_caps} shard-free
 * in M53. A fourth stale reason, found the same way as the others.
 *
 * <p><b>{@code _msearch} saves round trips, not shard work.</b> Each search still fans out to the shards it
 * names, so ten searches in one request cost one HTTP round trip and exactly the same object-store traffic as
 * ten requests. The endpoint's name suggests a bulk discount it does not give, and the tests say so rather
 * than implying otherwise.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessAnalyzeAndMsearchTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private int port;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-analyze-msearch")
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
        call(
            "PUT",
            "/alpha",
            "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"},\"tag\":{\"type\":\"keyword\"}}}}"
        );
        call("PUT", "/beta", "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}");
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want("alpha", 0);
        loop.want("beta", 0);
        loop.tick(clock.get());
        call("PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"Quick Brown Foxes\",\"tag\":\"Quick Brown\"}");
        call("PUT", "/beta/_doc/1?refresh=true", "{\"msg\":\"a different document\"}");
        return node;
    }

    /**
     * A field's own analyzer, which is the question this endpoint exists to answer.
     *
     * <p>A text field lowercases and splits; a keyword field does neither. That difference is the usual cause
     * of a query matching nothing, and seeing it is the whole reason a client calls {@code _analyze}.
     */
    public void testAFieldsOwnAnalyzerIsWhatAnswers() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "analyze-field")) {
            assertNotNull(node);

            final Answer text = call("POST", "/alpha/_analyze", "{\"text\":\"Quick Brown Foxes\",\"field\":\"msg\"}");
            assertEquals(text.body(), 200, text.status());
            assertTrue("a text field lowercases and splits: " + text.body(), text.has("\"token\":\"quick\""));
            assertTrue(text.body(), text.has("\"token\":\"foxes\""));
            assertTrue("with offsets into the original string: " + text.body(), text.has("\"start_offset\":12,\"end_offset\":17"));

            final Answer keyword = call("POST", "/alpha/_analyze", "{\"text\":\"Quick Brown\",\"field\":\"tag\"}");
            assertTrue("a keyword field does neither: " + keyword.body(), keyword.has("\"token\":\"Quick Brown\""));
            assertFalse("and does not split: " + keyword.body(), keyword.has("\"token\":\"quick\""));

            // Which analyzer answered, said rather than left to be inferred: a caller looking at unexpected
            // tokens wants to know what produced them.
            assertTrue(keyword.body(), keyword.has("\"analyzer_used\":\"tag\""));
        }
    }

    /** A named analyzer, the index default, and the two ways of being wrong about either. */
    public void testNamedAnalyzersAndTheirRefusals() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "analyze-named")) {
            assertNotNull(node);

            final Answer whitespace = call("POST", "/alpha/_analyze", "{\"text\":\"Quick Brown\",\"analyzer\":\"whitespace\"}");
            assertTrue("whitespace splits but does not lowercase: " + whitespace.body(), whitespace.has("\"token\":\"Quick\""));

            // Text can come from the query string too, which is how a person types it.
            final Answer viaQuery = call("GET", "/alpha/_analyze?text=Hello+World", null);
            assertEquals(viaQuery.body(), 200, viaQuery.status());
            assertTrue(viaQuery.body(), viaQuery.has("\"token\":\"hello\""));

            assertEquals(
                "an analyzer that does not exist is the caller's mistake",
                400,
                call("POST", "/alpha/_analyze", "{\"text\":\"x\",\"analyzer\":\"nope\"}").status()
            );
            assertEquals(
                "as is a field that does not exist",
                400,
                call("POST", "/alpha/_analyze", "{\"text\":\"x\",\"field\":\"nope\"}").status()
            );
            assertEquals("and an index that does not exist is a 404", 404, call("POST", "/ghost/_analyze", "{\"text\":\"x\"}").status());
        }
    }

    /**
     * A custom tokenizer is refused rather than answered with something else.
     *
     * <p>Accepting the parameter and analysing with the index's analyzer would show a caller the analysis of
     * something other than what they asked for — and this endpoint is used precisely when somebody does not
     * believe what they are being told about analysis.
     */
    public void testACustomAnalyzerIsRefusedRatherThanSubstituted() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "analyze-custom")) {
            assertNotNull(node);
            final Answer refused = call("POST", "/alpha/_analyze", "{\"text\":\"x\",\"tokenizer\":\"standard\"}");
            assertEquals(refused.body(), 501, refused.status());
            assertTrue("saying what would otherwise be shown: " + refused.body(), refused.has("something other than what was asked for"));
        }
    }

    /** Several searches in one request, each answered in the shape {@code _search} would have used. */
    public void testSeveralSearchesInOneRequest() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "msearch-two")) {
            assertNotNull(node);

            final Answer both = call(
                "POST",
                "/_msearch",
                "{\"index\":\"alpha\"}\n{\"query\":{\"match_all\":{}}}\n{\"index\":\"beta\"}\n{\"query\":{\"match_all\":{}}}\n"
            );
            assertEquals(both.body(), 200, both.status());
            assertTrue("the first search's document: " + both.body(), both.has("Quick Brown Foxes"));
            assertTrue("and the second's: " + both.body(), both.has("a different document"));
            // The same body _search produces, which is the point of sharing the renderer.
            assertTrue("each response carries the search shape: " + both.body(), both.has("\"_shards\""));
            assertTrue(both.body(), both.has("\"timed_out\":false"));
            assertTrue("hits are returned, not just counted: " + both.body(), both.has("\"_source\""));

            // The index may come from the path, with an empty header, as classic allows.
            final Answer fromPath = call("POST", "/alpha/_msearch", "{}\n{\"query\":{\"match_all\":{}}}\n");
            assertTrue(fromPath.body(), fromPath.has("Quick Brown Foxes"));
        }
    }

    /**
     * One search's failure is that search's failure, not the batch's.
     *
     * <p>The same accounting {@code _bulk} uses: failing all ten because the third was malformed would make
     * the endpoint less useful than the loop it replaces, and a caller pairing responses to requests by
     * position still can.
     */
    public void testOneBadSearchDoesNotFailTheBatch() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "msearch-partial")) {
            assertNotNull(node);

            final Answer mixed = call(
                "POST",
                "/_msearch",
                "{\"index\":\"alpha\"}\n{\"query\":{\"match_all\":{}}}\n{\"index\":\"alpha\"}\n{\"query\":{\"not_a_query\":{}}}\n"
            );
            assertEquals("the request itself is fine: " + mixed.body(), 200, mixed.status());
            assertTrue("the good search still answers: " + mixed.body(), mixed.has("Quick Brown Foxes"));
            assertTrue("and the bad one carries its own error: " + mixed.body(), mixed.has("unknown query [not_a_query]"));
            assertTrue("in its own slot: " + mixed.body(), mixed.has("\"status\":400"));

            // An index that is not there is that search's 404, in its slot, not the request's.
            final Answer missing = call("POST", "/_msearch", "{\"index\":\"ghost\"}\n{\"query\":{\"match_all\":{}}}\n");
            assertEquals(missing.body(), 200, missing.status());
            assertTrue(missing.body(), missing.has("\"status\":404"));
            assertTrue(missing.body(), missing.has("no such index: ghost"));
        }
    }

    /** A body that is not newline-delimited pairs at all fails the request rather than a search. */
    public void testAMalformedBatchFailsTheRequest() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "msearch-malformed")) {
            assertNotNull(node);
            assertEquals("an empty body is a bad request", 400, call("POST", "/_msearch", "\n").status());

            final Answer dangling = call("POST", "/_msearch", "{\"index\":\"alpha\"}\n");
            assertEquals("a header with no query is a bad request: " + dangling.body(), 400, dangling.status());
            assertTrue(dangling.body(), dangling.has("must be followed by a query body"));

            final Answer notAnObject = call("POST", "/_msearch", "not json\n{\"query\":{\"match_all\":{}}}\n");
            assertEquals(notAnObject.body(), 400, notAnObject.status());
            assertTrue(notAnObject.body(), notAnObject.has("header must be an object"));
        }
    }
}
