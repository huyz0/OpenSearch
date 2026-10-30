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

public class RolloverRequestValidationTests extends OpenSearchTestCase {
    private void assertRejected(String body) throws Exception {
        withNode((node, metadata) -> {
            final var answer = send(node, "/logs/_rollover", body);
            assertEquals(answer.body(), 400, answer.statusCode());
            assertEquals(List.of("logs-000001"), metadata.resolve("logs").alias().indices());
            assertTrue(metadata.describe("logs-000002").isEmpty());
        });
    }

    public void testScalarConditionsCannotBecomeUnconditionalRollover() throws Exception {
        assertRejected("{\"conditions\":\"max_docs=1000\"}");
    }

    public void testNullConditionsCannotBecomeUnconditionalRollover() throws Exception {
        assertRejected("{\"conditions\":null}");
    }

    public void testArrayConditionsCannotBecomeUnconditionalRollover() throws Exception {
        assertRejected("{\"conditions\":[]}");
    }

    public void testMisspelledConditionsCannotBecomeUnconditionalRollover() throws Exception {
        assertRejected("{\"condition\":{\"max_docs\":1000}}");
    }

    public void testUnknownConditionIsStillRejected() throws Exception {
        assertRejected("{\"conditions\":{\"max_doc\":1000}}");
    }

    public void testEmptyConditionsStillPermitIntentionalRollover() throws Exception {
        withNode((node, metadata) -> {
            final var answer = send(node, "/logs/_rollover", "{\"conditions\":{}}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"rolled_over\":true"));
            assertEquals(List.of("logs-000002"), metadata.resolve("logs").alias().indices());
            assertTrue(metadata.describe("logs-000002").isPresent());
        });
    }

    public void testUnmetConditionStillPreservesTarget() throws Exception {
        withNode((node, metadata) -> {
            node.activateWriter(metadata, "logs-000001", 0).orElseThrow();
            final var answer = send(node, "/logs/_rollover", "{\"conditions\":{\"max_docs\":1000}}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"rolled_over\":false"));
            assertEquals(List.of("logs-000001"), metadata.resolve("logs").alias().indices());
            assertTrue(metadata.describe("logs-000002").isEmpty());
        });
    }

    private void withNode(org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work) throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "rollover-request-validation")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest,search")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.createIndex(new IndexDescriptor("logs-000001", "uuid-logs", 1, null, null));
                metadata.createAlias(new org.opensearch.serverless.cluster.AliasRecord("logs", List.of("logs-000001")));
                work.accept(node, metadata);
            }
        }
    }

    private static HttpResponse<String> send(ServerlessNode node, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final var request = HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
