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
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.Mapping;
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
 * Indices with the same mapping and analysis settings share one parsed mapping and one set of analyzers, and
 * nothing one of them does changes another.
 *
 * <p>What sharing must never do: let a mapping update on one index show up on another, or let one index closing
 * close analyzers another is still using. Every test here checks behaviour through the REST API, not only which
 * objects are shared, so a wrong share fails as a wrong answer.
 */
public class SharedSchemaTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"tag\":{\"type\":\"keyword\"}}}";

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

    private final AtomicLong clock = new AtomicLong(1_000L);
    private MetadataPlane plane;

    private ServerlessNode node(boolean sharing) throws Exception {
        plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final ServerlessNode node = new ServerlessNode(
            Settings.builder()
                .put("node.name", "shared-schema")
                .put("cluster.name", "shared-schema")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .put("serverless.schema.sharing.enabled", sharing)
                .build()
        );
        node.startup().start();
        node.setMetadataPlane(plane);
        return node;
    }

    /** Creates the index and opens its one shard on the node. */
    private void create(ServerlessNode node, String index, String settings, String mapping) throws Exception {
        final Answer created = call(
            node,
            "PUT",
            "/" + index,
            "{\"settings\":{\"number_of_shards\":1" + settings + "},\"mappings\":" + mapping + "}"
        );
        assertEquals(created.body(), 200, created.status());
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want(index, 0);
        loop.tick(clock.get());
        assertNotNull("the shard must be open", service(node, index));
    }

    private static IndexService service(ServerlessNode node, String index) {
        for (IndexService service : node.indicesService()) {
            if (service.index().getName().equals(index)) {
                return service;
            }
        }
        return null;
    }

    private static Mapping mapping(ServerlessNode node, String index) {
        return service(node, index).mapperService().documentMapper().mapping();
    }

    private static void assertWritesAndFinds(ServerlessNode node, String index) throws Exception {
        final Answer written = call(node, "PUT", "/" + index + "/_doc/1?refresh=true", "{\"msg\":\"Quick Brown Fox\",\"tag\":\"Red\"}");
        assertEquals(written.body(), 201, written.status());
        final Answer analyzed = call(node, "POST", "/" + index + "/_search", "{\"query\":{\"match\":{\"msg\":\"brown\"}}}");
        assertTrue("text is analyzed on " + index + ": " + analyzed.body(), analyzed.has("\"value\":1"));
        final Answer exact = call(node, "POST", "/" + index + "/_search", "{\"query\":{\"term\":{\"tag\":\"Red\"}}}");
        assertTrue("keywords are exact on " + index + ": " + exact.body(), exact.has("\"value\":1"));
    }

    public void testIndicesWithOneMappingShareOneParsedCopy() throws Exception {
        try (ServerlessNode node = node(true)) {
            create(node, "a", "", MAPPING);
            create(node, "b", "", MAPPING);
            assertSame("same mapping and settings: one parsed copy", mapping(node, "a"), mapping(node, "b"));
            assertSame(
                "and one set of analyzers",
                service(node, "a").mapperService().getIndexAnalyzers().getDefaultIndexAnalyzer(),
                service(node, "b").mapperService().getIndexAnalyzers().getDefaultIndexAnalyzer()
            );
            assertWritesAndFinds(node, "a");
            assertWritesAndFinds(node, "b");
        }
    }

    public void testWithSharingOffEveryIndexParsesItsOwn() throws Exception {
        try (ServerlessNode node = node(false)) {
            create(node, "a", "", MAPPING);
            create(node, "b", "", MAPPING);
            assertNotSame(mapping(node, "a"), mapping(node, "b"));
            assertWritesAndFinds(node, "b");
        }
    }

    public void testADynamicFieldGivesOnlyThatIndexAMappingOfItsOwn() throws Exception {
        try (ServerlessNode node = node(true)) {
            create(node, "a", "", MAPPING);
            create(node, "b", "", MAPPING);
            create(node, "c", "", MAPPING);
            final Mapping shared = mapping(node, "b");

            final Answer grown = call(node, "PUT", "/a/_doc/1?refresh=true", "{\"msg\":\"x\",\"extra\":42}");
            assertEquals(grown.body(), 201, grown.status());

            assertNotNull("the writer's index has the new field", service(node, "a").mapperService().fieldType("extra"));
            assertNull("its neighbour does not", service(node, "b").mapperService().fieldType("extra"));
            assertSame("the neighbours still share the copy they had", shared, mapping(node, "b"));
            assertSame(shared, mapping(node, "c"));
            final Answer mapped = call(node, "GET", "/b/_mapping", null);
            assertFalse("and b's mapping as rendered has no new field: " + mapped.body(), mapped.has("extra"));
            assertWritesAndFinds(node, "b");
        }
    }

    public void testIndicesThatAddTheSameFieldsShareAgain() throws Exception {
        try (ServerlessNode node = node(true)) {
            create(node, "a", "", MAPPING);
            create(node, "b", "", MAPPING);
            create(node, "c", "", MAPPING);
            for (String index : new String[] { "a", "b" }) {
                final Answer grown = call(node, "PUT", "/" + index + "/_doc/1?refresh=true", "{\"msg\":\"x\",\"extra\":42}");
                assertEquals(grown.body(), 201, grown.status());
            }
            assertNotNull(service(node, "a").mapperService().fieldType("extra"));
            assertSame("two indices that grew the same way hold one mapping again", mapping(node, "a"), mapping(node, "b"));
            assertNotSame("and not the one the unchanged index holds", mapping(node, "a"), mapping(node, "c"));
            assertNull(service(node, "c").mapperService().fieldType("extra"));
            final Answer found = call(node, "POST", "/b/_search", "{\"query\":{\"term\":{\"extra\":42}}}");
            assertTrue(found.body(), found.has("\"value\":1"));
            assertWritesAndFinds(node, "c");
        }
    }

    public void testAPutMappingChangesOnlyItsIndex() throws Exception {
        try (ServerlessNode node = node(true)) {
            create(node, "a", "", MAPPING);
            create(node, "b", "", MAPPING);
            final Answer put = call(node, "PUT", "/a/_mapping", "{\"properties\":{\"added\":{\"type\":\"keyword\"}}}");
            assertEquals(put.body(), 200, put.status());
            // A write reads the descriptor, so the owner's open shard has applied the update by the time it answers.
            assertEquals(201, call(node, "PUT", "/a/_doc/9?refresh=true", "{\"added\":\"v\"}").status());
            assertNotNull(service(node, "a").mapperService().fieldType("added"));
            assertNull(service(node, "b").mapperService().fieldType("added"));
            assertFalse(call(node, "GET", "/b/_mapping", null).has("added"));
            assertWritesAndFinds(node, "b");
        }
    }

    public void testDeletingOneIndexLeavesTheOthersAnalyzersWorking() throws Exception {
        try (ServerlessNode node = node(true)) {
            create(node, "a", "", MAPPING);
            create(node, "b", "", MAPPING);
            assertWritesAndFinds(node, "a");
            final Answer deleted = call(node, "DELETE", "/a", null);
            assertEquals(deleted.body(), 200, deleted.status());
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            assertBusy(() -> {
                loop.tick(clock.get());
                assertNull("a's shard is closed", service(node, "a"));
            });
            // The analyzers b uses were built when a opened. Had a's close closed them, analysing text on b would now
            // fail with "this Analyzer is closed".
            assertWritesAndFinds(node, "b");
            create(node, "c", "", MAPPING);
            assertSame("a later index joins the copy still in use", mapping(node, "b"), mapping(node, "c"));
        }
    }

    public void testDifferentAnalysisSettingsDoNotShare() throws Exception {
        try (ServerlessNode node = node(true)) {
            create(node, "plain", "", MAPPING);
            create(node, "custom", ",\"analysis\":{\"analyzer\":{\"default\":{\"type\":\"standard\",\"stopwords\":[\"fox\"]}}}", MAPPING);
            assertNotSame(mapping(node, "plain"), mapping(node, "custom"));
            // With "fox" as a stopword, a match on it finds nothing; the plain index's analyzer finds the document.
            assertEquals(201, call(node, "PUT", "/custom/_doc/1?refresh=true", "{\"msg\":\"Quick Brown Fox\"}").status());
            final Answer stopped = call(node, "POST", "/custom/_search", "{\"query\":{\"match\":{\"msg\":\"fox\"}}}");
            assertTrue("custom keeps its own analyzer: " + stopped.body(), stopped.has("\"value\":0"));
            assertWritesAndFinds(node, "plain");
            final Answer plain = call(node, "POST", "/plain/_search", "{\"query\":{\"match\":{\"msg\":\"fox\"}}}");
            assertTrue("plain does not take custom's analyzer: " + plain.body(), plain.has("\"value\":1"));
        }
    }
}
