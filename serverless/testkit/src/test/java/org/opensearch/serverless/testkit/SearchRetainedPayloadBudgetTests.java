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
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.index.shard.ShardId;
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

public class SearchRetainedPayloadBudgetTests extends OpenSearchTestCase {
    private void withDocument(boolean nested, org.opensearch.common.CheckedConsumer<ServerlessNode, Exception> work) throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final String field = "{\"type\":\"text\",\"index\":false,\"store\":true}";
            final String mapping = nested
                ? "{\"properties\":{\"items\":{\"type\":\"nested\",\"properties\":{\"payload\":" + field + "}}}}"
                : "{\"properties\":{\"payload\":" + field + "}}";
            metadata.createIndex(new IndexDescriptor("docs", "uuid-docs", 1, mapping, null));
            final var settings = Settings.builder()
                .put("node.name", "retained-search-payload")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest,search")
                .put("indices.breaker.total.use_real_memory", false)
                .put("indices.breaker.request.limit", "128kb")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(metadata);
                final var loop = new BackgroundReconciler(node, metadata);
                loop.want("docs", 0);
                loop.tick(1000L);
                final String payload = "x".repeat(256 * 1024);
                final String source = nested ? "{\"items\":[{\"payload\":\"" + payload + "\"}]}" : "{\"payload\":\"" + payload + "\"}";
                final var written = send(node, "PUT", "/docs/_doc/id?refresh=true", source);
                assertEquals(written.body(), 201, written.statusCode());
                // Drain the translog's charged write buffer and the completed HTTP response before
                // measuring search allocations; background cleanup can otherwise change the baseline.
                node.reconciler().shard(new ShardId("docs", "uuid-docs", 0)).sync();
                assertBusy(() -> assertEquals(0L, node.circuitBreakerService().getBreaker(CircuitBreaker.REQUEST).getUsed()));
                work.accept(node);
            }
        }
    }

    private void assertPayloadRefused(ServerlessNode node, String query) throws Exception {
        final var breaker = node.circuitBreakerService().getBreaker(CircuitBreaker.REQUEST);
        final long before = breaker.getUsed();
        final var response = send(node, "POST", "/docs/_search?allow_partial_search_results=false", query);
        assertEquals(response.body(), 429, response.statusCode());
        assertTrue(response.body(), response.body().contains("circuit_breaking_exception"));
        assertBusy(() -> assertEquals(before, breaker.getUsed()));
        final var after = send(node, "POST", "/docs/_search", "{\"size\":0}");
        assertEquals(after.body(), 200, after.statusCode());
        assertBusy(() -> assertEquals(before, breaker.getUsed()));
    }

    public void testStoredFieldsAreChargedWhenSourceIsDisabled() throws Exception {
        withDocument(false, node -> assertPayloadRefused(node, "{\"_source\":false,\"stored_fields\":[\"payload\"],\"size\":1}"));
    }

    public void testTopHitsAggregationPayloadIsCharged() throws Exception {
        withDocument(
            false,
            node -> assertPayloadRefused(
                node,
                "{\"size\":0,\"aggs\":{\"top\":{\"top_hits\":{\"size\":1,\"_source\":false,\"stored_fields\":[\"payload\"]}}}}"
            )
        );
    }

    public void testInnerHitsPayloadIsChargedWhenParentSourceIsDisabled() throws Exception {
        withDocument(
            true,
            node -> assertPayloadRefused(
                node,
                "{\"_source\":false,\"query\":{\"nested\":{\"path\":\"items\",\"query\":{\"match_all\":{}},"
                    + "\"inner_hits\":{\"size\":1,\"_source\":false,\"stored_fields\":[\"items.payload\"]}}}}"
            )
        );
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
