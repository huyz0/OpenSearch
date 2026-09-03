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
 * M52: changing an index after it exists, and the endpoints a comparison said were missing.
 *
 * <p><b>Where this milestone came from.</b> Comparing this shell's surface against AWS OpenSearch Serverless
 * and Elastic Cloud Serverless turned up one capability both of them ship and this did not: a mutable
 * mapping. An index whose mapping is fixed at creation cannot have a field added to it, and with no
 * {@code _reindex} to escape through, the only remedy was to delete the index and its data. Two independent
 * vendors treating something as non-optional is the strongest signal a comparison can produce.
 *
 * <p><b>The design is that nothing here decides what a legal mapping change is.</b> The merge is core's own,
 * under {@code MergeReason.MAPPING_UPDATE} — the same call classic OpenSearch makes for the same request —
 * so adding a field succeeds and changing a field's type is refused in core's own words. That is the same
 * shape as {@code if_seq_no}, where the compare-and-swap is the engine's and this shell only carries the
 * question to it.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessMappingUpdateTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private int port;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-mapping-update")
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
        call("PUT", "/alpha", "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}");
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        return node;
    }

    /**
     * A field added to a live index is usable immediately: writable, and queryable as its declared type.
     *
     * <p><b>Why the query is the assertion and not the mapping read.</b> Storing a new mapping and reporting
     * it back proves only that a string round-tripped. What has to be true is that the shard already open and
     * already serving traffic <em>picked the change up</em> — so the test writes a document using the new
     * field and runs a range query, which only matches if the field is genuinely indexed as a long. A shard
     * that kept its old mapping would index it as text and return nothing, with no error anywhere.
     */
    public void testAFieldAddedToALiveIndexIsImmediatelyUsable() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "map-add")) {
            assertNotNull(node);
            assertEquals(201, call("PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"before\"}").status());

            final Answer updated = call("PUT", "/alpha/_mapping", "{\"properties\":{\"n\":{\"type\":\"long\"}}}");
            assertEquals(updated.body(), 200, updated.status());
            assertTrue("an update that changes something says so: " + updated.body(), updated.has("\"changed\":true"));

            assertEquals(201, call("PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"after\",\"n\":42}").status());

            // The assertion that matters: a range query is only answerable if the shard indexed n as a long.
            final Answer ranged = call("POST", "/alpha/_search", "{\"query\":{\"range\":{\"n\":{\"gte\":40}}}}");
            assertEquals(ranged.body(), 200, ranged.status());
            assertTrue("the shard already open must have picked the new field up: " + ranged.body(), ranged.has("\"value\":1"));

            // And the old field still works, which is what "merge" rather than "replace" means.
            final Answer old = call("POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"before\"}}}");
            assertTrue("the pre-existing field must survive the update: " + old.body(), old.has("\"value\":1"));
        }
    }

    /**
     * A change that would reinterpret data already indexed is refused, in core's words.
     *
     * <p>Nothing in this shell knows that text cannot become long. Core does, and the refusal carries its
     * explanation verbatim rather than a paraphrase — re-wording it would mean maintaining a second account
     * of the type system that could drift from the real one.
     */
    public void testAConflictingChangeIsRefusedInCoresOwnWords() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "map-conflict")) {
            assertNotNull(node);
            final Answer refused = call("PUT", "/alpha/_mapping", "{\"properties\":{\"msg\":{\"type\":\"long\"}}}");
            assertEquals("a caller's mistake is a 400, not a 500: " + refused.body(), 400, refused.status());
            assertTrue(
                "and names the field and both types: " + refused.body(),
                refused.has("cannot be changed from type [text] to [long]")
            );

            // Refused means refused: the mapping is untouched and the field still works as text.
            final Answer mapping = call("GET", "/alpha/_mapping", null);
            assertTrue("the stored mapping must be unchanged: " + mapping.body(), mapping.has("\"msg\":{\"type\":\"text\"}"));
        }
    }

    /**
     * Replaying an update is a no-op that says it was one, and the mapping keeps the shape a client reads.
     *
     * <p>Core's merged mapping source nests everything under the mapping type ({@code _doc}); OpenSearch's own
     * {@code GET /{index}/_mapping} does not show that level and this shell's stored mappings never had it. So
     * the first update of any index would silently have changed the shape of every mapping read afterwards.
     */
    public void testAReplayedUpdateChangesNothingAndKeepsTheMappingShape() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "map-replay")) {
            assertNotNull(node);
            assertTrue(call("PUT", "/alpha/_mapping", "{\"properties\":{\"n\":{\"type\":\"long\"}}}").has("\"changed\":true"));

            final Answer again = call("PUT", "/alpha/_mapping", "{\"properties\":{\"n\":{\"type\":\"long\"}}}");
            assertEquals(again.body(), 200, again.status());
            assertTrue("a replay must be acknowledged as a no-op, not as a change: " + again.body(), again.has("\"changed\":false"));

            final Answer mapping = call("GET", "/alpha/_mapping", null);
            assertTrue("no mapping-type level in what a client reads: " + mapping.body(), mapping.has("\"mappings\":{\"properties\""));
            assertFalse("core's internal wrapper must not leak: " + mapping.body(), mapping.has("_doc"));
        }
    }

    /** An update to an index that does not exist is a 404, not a create. */
    public void testUpdatingAMissingIndexIsNotACreate() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "map-missing")) {
            assertNotNull(node);
            final Answer missing = call("PUT", "/ghost/_mapping", "{\"properties\":{\"a\":{\"type\":\"long\"}}}");
            assertEquals(missing.body(), 404, missing.status());
            assertNull("and must not have created anything", call("GET", "/ghost", null).status() == 200 ? "created" : null);
        }
    }

    /**
     * A mapping change survives the node that made it, because it lives in the descriptor.
     *
     * <p>There is no push here and nothing to push from: a successor reads the same descriptor and gets the
     * same mapping. This is the property that makes the update a control-plane write rather than a message.
     */
    public void testAMappingChangeSurvivesTheNodeThatMadeIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        final ServerlessNode first = running(plane, clock, "map-a");
        call("PUT", "/alpha/_mapping", "{\"properties\":{\"n\":{\"type\":\"long\"}}}");
        call("PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"n\":7}");
        first.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode second = new ServerlessNode(nodeSettings("map-b"))) {
            second.start();
            second.setMetadataPlane(plane);
            port = second.boundHttpAddress().publishAddress().getPort();
            final BackgroundReconciler loop = new BackgroundReconciler(second, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final Answer mapping = call("GET", "/alpha/_mapping", null);
            assertTrue("the successor reads the updated mapping: " + mapping.body(), mapping.has("\"n\":{\"type\":\"long\"}"));

            final Answer ranged = call("POST", "/alpha/_search", "{\"query\":{\"range\":{\"n\":{\"gte\":5}}}}");
            assertTrue("and indexes with it, not with the mapping the index was created with: " + ranged.body(), ranged.has("\"value\":1"));
        }
    }

    /** {@code _count} answers with a count, with and without a query, and reports its shard coverage. */
    public void testCountAnswersWithAndWithoutAQuery() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "count")) {
            assertNotNull(node);
            call("PUT", "/alpha/_mapping", "{\"properties\":{\"n\":{\"type\":\"long\"}}}");
            call("PUT", "/alpha/_doc/1", "{\"msg\":\"a\",\"n\":1}");
            call("PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"b\",\"n\":50}");

            final Answer all = call("GET", "/alpha/_count", null);
            assertEquals("a count with no body means all of them: " + all.body(), 200, all.status());
            assertTrue(all.body(), all.has("\"count\":2"));
            assertTrue(
                "coverage is reported, because a count over some shards is a different number: " + all.body(),
                all.has("\"_shards\"")
            );

            final Answer some = call("POST", "/alpha/_count", "{\"query\":{\"range\":{\"n\":{\"gte\":40}}}}");
            assertTrue(some.body(), some.has("\"count\":1"));
            // The count shape, not the search shape. This asserts what the renderer emits and nothing about
            // whether hits were fetched on the way -- the handler sets size=0 for that, and the saving is
            // invisible from here, so claiming it in an assertion would be claiming something untested.
            assertFalse("a count is not a search response: " + some.body(), some.has("\"hits\""));
            assertFalse(some.body(), some.has("\"took\""));
        }
    }

    /** {@code _source} returns the document alone, and a missing one is still a 404. */
    public void testSourceReturnsTheDocumentWithoutTheEnvelope() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "source")) {
            assertNotNull(node);
            call("PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"only me\"}");

            final Answer source = call("GET", "/alpha/_source/1", null);
            assertEquals(source.body(), 200, source.status());
            assertEquals("the document and nothing else", "{\"msg\":\"only me\"}", source.body());

            assertEquals("a missing document is a 404, not an empty 200", 404, call("GET", "/alpha/_source/ghost", null).status());
            assertEquals("HEAD answers with a status alone", 200, call("HEAD", "/alpha/_source/1", null).status());
        }
    }

    /**
     * The point-in-time API answers at OpenSearch's spelling, not only at Elasticsearch's.
     *
     * <p>This shell shipped {@code POST /{index}/_pit} — Elasticsearch's endpoint — on a fork of OpenSearch,
     * whose own spelling is {@code POST /{index}/_search/point_in_time}. A correctly written OpenSearch client
     * got "no handler found for uri". Both are routed now; removing the old one to fix a compatibility bug
     * would have broken this shell's own callers to do it.
     */
    public void testPointInTimeAnswersAtOpenSearchsOwnSpelling() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "pit")) {
            call("PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"frozen\"}");
            new BackgroundReconciler(node, plane).tick(clock.get() + 1_000);

            final Answer canonical = call("POST", "/alpha/_search/point_in_time?keep_alive=5m", null);
            assertEquals("OpenSearch's spelling must reach the handler: " + canonical.body(), 200, canonical.status());
            assertTrue("and mint a real view: " + canonical.body(), canonical.has("pit_id"));

            final Answer legacy = call("POST", "/alpha/_pit?keep_alive=5m", null);
            assertEquals("the spelling this shell's own callers use must keep working: " + legacy.body(), 200, legacy.status());
        }
    }

    /**
     * The endpoints a comparison found falling through to core's 400 now refuse explicitly.
     *
     * <p>Being absent is a decision; looking like a typo is not. Each of these is an endpoint a real client
     * reaches for, and each was answering {@code 400 no handler found for uri} rather than the 501-with-a-
     * reason this surface promises.
     */
    public void testTheEndpointsThatFellThroughNowRefuseProperly() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "fallthrough")) {
            assertNotNull(node);
            for (String[] expected : new String[][] {
                { "/_msearch", "one search per request" },
                { "/alpha/_validate/query", "send the query to _search" },
                { "/_resolve/index/alpha", "look an index up by name" },
                { "/_aliases", "atomic swaps" },
                { "/alpha/_alias/x", "PUT /_alias/{name}" },
                { "/_cat/templates", "no cluster state" } }) {
                final Answer refused = call("GET", expected[0], null);
                assertEquals(expected[0] + " must refuse explicitly: " + refused.body(), 501, refused.status());
                assertTrue(expected[0] + " must say why: " + refused.body(), refused.has(expected[1]));
            }
        }
    }
}
