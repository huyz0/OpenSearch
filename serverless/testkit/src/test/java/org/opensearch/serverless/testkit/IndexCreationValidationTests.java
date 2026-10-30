/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public class IndexCreationValidationTests extends OpenSearchTestCase {
    private static final String INVALID_MAPPING = "{\"properties\":{\"bad\":{\"type\":\"not_a_mapper\"}}}";

    public void testRestCreateRefusesAnUnusableMappingBeforePersistence() throws Exception {
        withNode((node, metadata) -> {
            final var response = send(node, "PUT", "/invalid", "{\"mappings\":" + INVALID_MAPPING + "}");
            assertEquals(response.body(), 400, response.statusCode());
            assertTrue(metadata.describe("invalid").isEmpty());
            assertFalse(node.indicesService().iterator().hasNext());
        });
    }

    public void testNativeCreateRefusesAnUnusableMappingBeforePersistence() throws Exception {
        withNode((node, metadata) -> {
            final Exception failure = expectThrows(
                Exception.class,
                () -> node.client().admin().indices().create(new CreateIndexRequest("invalid").mapping(INVALID_MAPPING)).actionGet()
            );
            assertEquals(failure.toString(), RestStatus.BAD_REQUEST, org.opensearch.ExceptionsHelper.status(failure));
            assertTrue(metadata.describe("invalid").isEmpty());
            assertFalse(node.indicesService().iterator().hasNext());
        });
    }

    public void testInheritedMappingsAreValidatedBeforePersistence() throws Exception {
        withNode((node, metadata) -> {
            metadata.indexTemplates()
                .put("events", "{\"index_patterns\":[\"events*\"],\"template\":{\"mappings\":" + INVALID_MAPPING + "}}");
            final var response = send(node, "PUT", "/events-1", "{}");
            assertEquals(response.body(), 400, response.statusCode());
            assertTrue(metadata.describe("events-1").isEmpty());
        });
    }

    public void testRolloverCannotMoveAnAliasToAnUnusableMapping() throws Exception {
        withNode((node, metadata) -> {
            metadata.createIndex(new IndexDescriptor("logs-000001", "uuid-logs", 1, "{}", null));
            metadata.createAlias(new AliasRecord("logs", List.of("logs-000001")));
            final var response = send(node, "POST", "/logs/_rollover", "{\"mappings\":" + INVALID_MAPPING + "}");
            assertEquals(response.body(), 400, response.statusCode());
            assertTrue(metadata.describe("logs-000002").isEmpty());
            assertEquals(List.of("logs-000001"), metadata.resolve("logs").alias().indices());
        });
    }

    public void testDataStreamCannotCreateAnUnusableBackingIndex() throws Exception {
        withNode((node, metadata) -> {
            metadata.indexTemplates()
                .put("events", "{\"index_patterns\":[\"events*\"],\"data_stream\":{},\"template\":{\"mappings\":" + INVALID_MAPPING + "}}");
            final var response = send(node, "PUT", "/_data_stream/events", null);
            assertEquals(response.body(), 400, response.statusCode());
            assertTrue(metadata.describe(".ds-events-000001").isEmpty());
            assertNull(metadata.resolve("events").alias());
        });
    }

    public void testFieldLimitsAreCheckedBeforePersistence() throws Exception {
        withNode((node, metadata) -> {
            final var response = send(
                node,
                "PUT",
                "/invalid",
                "{\"settings\":{\"index.mapping.total_fields.limit\":1},\"mappings\":{\"properties\":{\"a\":{\"type\":\"long\"},\"b\":{\"type\":\"long\"}}}}"
            );
            assertEquals(response.body(), 400, response.statusCode());
            assertTrue(metadata.describe("invalid").isEmpty());
        });
    }

    public void testInvalidAnalyzersAreCheckedBeforePersistence() throws Exception {
        withNode((node, metadata) -> {
            final var response = send(
                node,
                "PUT",
                "/invalid",
                "{\"settings\":{\"analysis\":{\"analyzer\":{\"custom\":{\"type\":\"custom\",\"tokenizer\":\"not_a_tokenizer\"}}}},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\",\"analyzer\":\"custom\"}}}}"
            );
            assertEquals(response.body(), 400, response.statusCode());
            assertTrue(metadata.describe("invalid").isEmpty());
        });
    }

    public void testValidCustomAnalysisDoesNotOpenAnIndexDuringValidation() throws Exception {
        withNode((node, metadata) -> {
            final var response = send(
                node,
                "PUT",
                "/valid",
                "{\"settings\":{\"analysis\":{\"analyzer\":{\"custom\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[]}}}},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\",\"analyzer\":\"custom\"}}}}"
            );
            assertEquals(response.body(), 200, response.statusCode());
            assertFalse("validation must not register an IndexService", node.indicesService().iterator().hasNext());
            final var shard = node.activateWriter(metadata, "valid", 0).orElseThrow();
            node.index(shard, "doc", "{\"msg\":\"HELLO\"}");
            assertTrue(node.get(shard, "doc").found());
        });
    }

    private void withNode(org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work) throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "index-create-validation")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(metadata);
                work.accept(node, metadata);
            }
        }
    }

    private static HttpResponse<String> send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final var request = HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }

    public void testWrappedRestMappingsStillComposeWithTemplates() throws Exception {
        withNode((node, metadata) -> {
            metadata.indexTemplates()
                .put(
                    "events",
                    "{\"index_patterns\":[\"events*\"],\"template\":{\"mappings\":{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}}}"
                );
            final var response = send(node, "PUT", "/events-1", "{\"mappings\":{\"_doc\":{\"properties\":{\"n\":{\"type\":\"long\"}}}}}");
            assertEquals(response.body(), 200, response.statusCode());
            final var shard = node.activateWriter(metadata, "events-1", 0).orElseThrow();
            node.index(shard, "doc", "{\"n\":9,\"tag\":\"valid\"}");
            assertTrue(node.get(shard, "doc").found());
        });
    }

    public void testUnknownIndexSortFieldsAreRefusedBeforePersistence() throws Exception {
        assertIndexSortCreation("missing", "{\"properties\":{\"n\":{\"type\":\"long\"}}}", 400);
    }

    public void testTextIndexSortFieldsAreRefusedBeforePersistence() throws Exception {
        assertIndexSortCreation("n", "{\"properties\":{\"n\":{\"type\":\"text\"}}}", 400);
    }

    public void testIndexSortFieldsWithoutDocValuesAreRefusedBeforePersistence() throws Exception {
        assertIndexSortCreation("n", "{\"properties\":{\"n\":{\"type\":\"long\",\"doc_values\":false}}}", 400);
    }

    public void testValidIndexSortingStillOpensAndWrites() throws Exception {
        assertIndexSortCreation("n", "{\"properties\":{\"n\":{\"type\":\"long\"}}}", 200);
    }

    private void assertIndexSortCreation(String field, String mapping, int status) throws Exception {
        withNode((node, metadata) -> {
            final var response = send(
                node,
                "PUT",
                "/sorted",
                "{\"settings\":{\"index.sort.field\":\"" + field + "\",\"index.sort.order\":\"desc\"},\"mappings\":" + mapping + "}"
            );
            assertEquals(response.body(), status, response.statusCode());
            if (status == 400) {
                assertTrue(metadata.describe("sorted").isEmpty());
            } else {
                assertFalse(node.indicesService().iterator().hasNext());
                final var shard = node.activateWriter(metadata, "sorted", 0).orElseThrow();
                node.index(shard, "doc", "{\"n\":9}");
                assertTrue(node.get(shard, "doc").found());
            }
        });
    }
}
