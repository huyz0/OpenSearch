/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.indices.mapping.put.PutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
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
import java.util.Map;

public class MappingRequestExecutionTests extends OpenSearchTestCase {
    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "mapping-filter-contract")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.createIndex(new IndexDescriptor("original", "uuid-original", 1, "{}", null));
                metadata.createIndex(new IndexDescriptor("selected", "uuid-selected", 1, "{}", null));
                work.accept(node, metadata);
            }
        }
    }

    private static String mapping(String field) {
        return "{\"properties\":{\"" + field + "\":{\"type\":\"keyword\"}}}";
    }

    public void testNativeMappingRequestExposesTheActualBody() throws Exception {
        withNode(PolicyFilterPlugin.request(PutMappingAction.NAME, request -> {
            final var mapping = (PutMappingRequest) request;
            assertArrayEquals(new String[] { "original" }, mapping.indices());
            assertNotNull(mapping.source());
            assertTrue(mapping.source(), mapping.source().contains("original_field"));
            return request;
        }), (node, metadata) -> {
            final var response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
        });
    }

    public void testMappingUpdateUsesTheAdmittedIndexAndBody() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                PutMappingAction.NAME,
                request -> new PutMappingRequest("selected").source(Map.of("properties", Map.of("chosen", Map.of("type", "keyword"))))
            ),
            (node, metadata) -> {
                final var response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
                assertEquals(response.body(), 200, response.statusCode());
                assertFalse(metadata.describe("original").orElseThrow().mapping().contains("original_field"));
                assertTrue(metadata.describe("selected").orElseThrow().mapping().contains("chosen"));
            }
        );
    }

    public void testResponseAuthorizationRunsBeforeHttpSuccess() throws Exception {
        withNode(PolicyFilterPlugin.denyAfter(PutMappingAction.NAME), (node, metadata) -> {
            final var response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
            assertEquals(response.body(), 403, response.statusCode());
        });
    }

    public void testNativeMappingAcknowledgementRewritesReachTheCaller() throws Exception {
        withNode(PolicyFilterPlugin.response(PutMappingAction.NAME, response -> {
            assertTrue(response.toString(), response instanceof AcknowledgedResponse);
            return new AcknowledgedResponse(false);
        }), (node, metadata) -> {
            final var response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"acknowledged\":false"));
        });
    }

    public void testCachedAcknowledgementsDoNotWriteTheMapping() throws Exception {
        withNode(PolicyFilterPlugin.cached(PutMappingAction.NAME, new AcknowledgedResponse(false)), (node, metadata) -> {
            final var response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"acknowledged\":false"));
            assertFalse(metadata.describe("original").orElseThrow().mapping().contains("original_field"));
        });
    }

    public void testUnsupportedAdmittedMultipleIndicesAreRefused() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                PutMappingAction.NAME,
                request -> new PutMappingRequest("original", "selected").source(
                    Map.of("properties", Map.of("chosen", Map.of("type", "keyword")))
                )
            ),
            (node, metadata) -> {
                final var response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
                assertEquals(response.body(), 501, response.statusCode());
                assertFalse(metadata.describe("original").orElseThrow().mapping().contains("original_field"));
                assertFalse(metadata.describe("selected").orElseThrow().mapping().contains("chosen"));
            }
        );
    }

    public void testAdmittedConcreteIndexIdentityIsHonored() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                PutMappingAction.NAME,
                request -> new PutMappingRequest().setConcreteIndex(new Index("selected", "uuid-selected"))
                    .source(Map.of("properties", Map.of("chosen", Map.of("type", "keyword"))))
            ),
            (node, metadata) -> {
                final var response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(metadata.describe("selected").orElseThrow().mapping().contains("chosen"));
                assertFalse(metadata.describe("original").orElseThrow().mapping().contains("original_field"));
            }
        );
    }

    public void testStaleAdmittedConcreteIndexCannotChangeAReplacement() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                PutMappingAction.NAME,
                request -> new PutMappingRequest().setConcreteIndex(new Index("original", "stale-uuid"))
                    .source(Map.of("properties", Map.of("chosen", Map.of("type", "keyword"))))
            ),
            (node, metadata) -> {
                final var response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
                assertEquals(response.body(), 404, response.statusCode());
                assertFalse(metadata.describe("original").orElseThrow().mapping().contains("chosen"));
                assertFalse(metadata.describe("original").orElseThrow().mapping().contains("original_field"));
            }
        );
    }

    public void testInPlaceIndexOptionsRewritesAreNotIgnored() throws Exception {
        withNode(PolicyFilterPlugin.request(PutMappingAction.NAME, request -> {
            ((PutMappingRequest) request).indicesOptions(org.opensearch.action.support.IndicesOptions.lenientExpandOpen());
            return request;
        }), (node, metadata) -> {
            final var response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
            assertEquals(response.body(), 501, response.statusCode());
            assertFalse(metadata.describe("original").orElseThrow().mapping().contains("original_field"));
        });
    }

    public void testUnchangedNativeResponsesPreserveTheNoOpReport() throws Exception {
        withNode(PolicyFilterPlugin.request(PutMappingAction.NAME, request -> request), (node, metadata) -> {
            var response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
            response = send(node, "PUT", "/original/_mapping", mapping("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"index\":\"original\""));
            assertTrue(response.body(), response.body().contains("\"changed\":false"));
        });
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
}
