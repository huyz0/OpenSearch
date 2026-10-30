/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.get.MultiGetAction;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class BatchRetainedPayloadBudgetTests extends OpenSearchTestCase {
    private static final String IDS = "{\"ids\":[\"0\",\"1\",\"2\",\"3\",\"4\"]}";

    private void withDocuments(PolicyFilterPlugin plugin, org.opensearch.common.CheckedConsumer<ServerlessNode, Exception> work)
        throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            metadata.createIndex(
                new IndexDescriptor(
                    "docs",
                    "uuid-docs",
                    1,
                    "{\"properties\":{\"payload\":{\"type\":\"text\",\"index\":false,\"store\":true},\"order\":{\"type\":\"keyword\"}}}",
                    null
                )
            );
            final var settings = Settings.builder()
                .put("node.name", "batch-retained-payload")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest,search")
                .put("indices.breaker.total.use_real_memory", false)
                .put("indices.breaker.request.limit", "128kb")
                .build();
            try (var node = new ServerlessNode(settings, plugin == null ? List.of() : List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                final var loop = new BackgroundReconciler(node, metadata);
                loop.want("docs", 0);
                loop.tick(1000L);
                final String payload = "x".repeat(32 * 1024);
                for (int i = 0; i < 5; i++) {
                    final var written = send(
                        node,
                        "PUT",
                        "/docs/_doc/" + i + "?refresh=true",
                        "{\"payload\":\"" + payload + "\",\"order\":\"" + i + "\"}"
                    );
                    assertEquals(written.body(), 201, written.statusCode());
                }
                work.accept(node);
                assertBusy(() -> assertEquals(0L, node.circuitBreakerService().getBreaker(CircuitBreaker.REQUEST).getUsed()));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> items(HttpResponse<String> response, String key) {
        assertEquals(response.body(), 200, response.statusCode());
        return (List<Map<String, Object>>) XContentHelper.convertToMap(new BytesArray(response.body()), false, XContentType.JSON)
            .v2()
            .get(key);
    }

    public void testMultiSearchChargesItsCumulativeRetainedResponses() throws Exception {
        withDocuments(null, node -> {
            final String query =
                "{\"allow_partial_search_results\":false}\n{\"size\":1,\"_source\":false,\"stored_fields\":[\"payload\"]}\n";
            final var response = send(node, "POST", "/docs/_msearch", query.repeat(3));
            final var items = items(response, "responses");
            assertEquals(3, items.size());
            assertEquals(response.body(), 200, ((Number) items.getFirst().get("status")).intValue());
            assertTrue(response.body(), items.stream().anyMatch(item -> ((Number) item.get("status")).intValue() == 429));
        });
    }

    public void testMultiGetChargesItsCumulativeRetainedSources() throws Exception {
        withDocuments(null, node -> {
            final var response = send(node, "POST", "/docs/_mget", IDS);
            if (response.statusCode() == 429) {
                assertTrue(response.body(), response.body().contains("circuit_breaking_exception"));
                return;
            }
            final var items = items(response, "docs");
            assertEquals(5, items.size());
            assertTrue(response.body(), items.stream().anyMatch(item -> Boolean.TRUE.equals(item.get("found"))));
            assertTrue(response.body(), response.body().contains("circuit_breaking_exception"));
        });
    }

    public void testMultiGetChargesOnlyItsSelectedSource() throws Exception {
        withDocuments(null, node -> {
            final var response = send(node, "POST", "/docs/_mget?_source=order", IDS);
            final var items = items(response, "docs");
            assertEquals(5, items.size());
            assertTrue(response.body(), items.stream().allMatch(item -> Boolean.TRUE.equals(item.get("found"))));
            assertFalse(response.body(), response.body().contains("\"payload\""));
        });
    }

    public void testMultiSearchSmallFilteredResponsesStillFitTogether() throws Exception {
        withDocuments(null, node -> {
            final String query = "{\"allow_partial_search_results\":false}\n{\"size\":1,\"_source\":[\"order\"]}\n";
            final var response = send(node, "POST", "/docs/_msearch", query.repeat(3));
            final var items = items(response, "responses");
            assertEquals(3, items.size());
            assertTrue(response.body(), items.stream().allMatch(item -> ((Number) item.get("status")).intValue() == 200));
        });
    }

    public void testMultiGetReleasesReservationsWhenItsResponseFilterDeniesAccess() throws Exception {
        withDocuments(PolicyFilterPlugin.denyAfter(MultiGetAction.NAME), node -> {
            final var response = send(node, "POST", "/docs/_mget?_source=order", IDS);
            assertEquals(response.body(), 403, response.statusCode());
            assertFalse(response.body(), response.body().contains("\"found\":true"));
        });
    }

    private static org.opensearch.action.get.MultiGetItemResponse oversizedItem() {
        return new org.opensearch.action.get.MultiGetItemResponse(
            new org.opensearch.action.get.GetResponse(
                new org.opensearch.index.get.GetResult(
                    "docs",
                    "0",
                    0L,
                    1L,
                    1L,
                    true,
                    new BytesArray("{\"payload\":\"" + "x".repeat(256 * 1024) + "\"}"),
                    Map.of(),
                    Map.of()
                )
            ),
            null
        );
    }

    public void testCachedMultiGetResponseCannotBypassTheBudget() throws Exception {
        final var cached = new org.opensearch.action.get.MultiGetResponse(
            new org.opensearch.action.get.MultiGetItemResponse[] { oversizedItem() }
        );
        withDocuments(PolicyFilterPlugin.cached(MultiGetAction.NAME, cached), node -> {
            final var response = send(node, "POST", "/docs/_mget", "{\"ids\":[\"0\"]}");
            assertEquals(response.body(), 429, response.statusCode());
            assertTrue(response.body(), response.body().contains("circuit_breaking_exception"));
            assertFalse(response.body(), response.body().contains("\"payload\""));
        });
    }

    public void testInPlaceResponseRewriteCannotBypassTheBudget() throws Exception {
        withDocuments(PolicyFilterPlugin.response(MultiGetAction.NAME, response -> {
            ((org.opensearch.action.get.MultiGetResponse) response).getResponses()[0] = oversizedItem();
            return response;
        }), node -> {
            final var response = send(node, "POST", "/docs/_mget", "{\"ids\":[\"0\"]}");
            assertEquals(response.body(), 429, response.statusCode());
            assertTrue(response.body(), response.body().contains("circuit_breaking_exception"));
            assertFalse(response.body(), response.body().contains("\"payload\""));
        });
    }

    private static HttpResponse<String> send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final var request = HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
