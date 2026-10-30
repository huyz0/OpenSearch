/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.indices.rollover.RolloverAction;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
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
import java.util.Map;

public class RolloverRequestExecutionTests extends OpenSearchTestCase {
    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "rollover-filter-contract")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.createIndex(new IndexDescriptor("logs-000001", "uuid-logs", 1, null, null));
                metadata.createAlias(new AliasRecord("logs", List.of("logs-000001")));
                work.accept(node, metadata);
            }
        }
    }

    public void testNativeRequestExposesDryRunConditionsAndCreationBody() throws Exception {
        withNode(PolicyFilterPlugin.request(RolloverAction.NAME, request -> {
            final var rollover = (RolloverRequest) request;
            assertEquals("logs", rollover.getRolloverTarget());
            assertEquals("logs-000002", rollover.getNewIndexName());
            assertTrue(rollover.isDryRun());
            assertEquals(1000L, rollover.getConditions().get("max_docs").value());
            assertEquals("2", rollover.getCreateIndexRequest().settings().get("number_of_shards"));
            assertTrue(rollover.getCreateIndexRequest().mappings().contains("chosen"));
            return request;
        }), (node, metadata) -> {
            node.activateWriter(metadata, "logs-000001", 0).orElseThrow();
            final var response = send(
                node,
                "POST",
                "/logs/_rollover/logs-000002?dry_run=true",
                "{\"conditions\":{\"max_docs\":1000},\"settings\":{\"number_of_shards\":2},"
                    + "\"mappings\":{\"properties\":{\"chosen\":{\"type\":\"keyword\"}}}}"
            );
            assertEquals(response.body(), 200, response.statusCode());
        });
    }

    public void testRolloverUsesAdmittedTargetAndNewName() throws Exception {
        withNode(
            PolicyFilterPlugin.request(RolloverAction.NAME, request -> new RolloverRequest("logs", "logs-000009")),
            (node, metadata) -> {
                final var response = send(node, "POST", "/missing/_rollover", "{}");
                assertEquals(response.body(), 200, response.statusCode());
                assertEquals(List.of("logs-000009"), metadata.resolve("logs").alias().indices());
                assertTrue(metadata.describe("logs-000009").isPresent());
            }
        );
    }

    public void testAdmittedDryRunPreventsRollover() throws Exception {
        withNode(PolicyFilterPlugin.request(RolloverAction.NAME, request -> {
            final var selected = new RolloverRequest("logs", "logs-000002");
            selected.dryRun(true);
            return selected;
        }), (node, metadata) -> {
            final var response = send(node, "POST", "/logs/_rollover", "{}");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"dry_run\":true"));
            assertTrue(metadata.describe("logs-000002").isEmpty());
        });
    }

    public void testAdmittedCreationBodyShapesTheNewIndex() throws Exception {
        withNode(PolicyFilterPlugin.request(RolloverAction.NAME, request -> {
            final var selected = new RolloverRequest("logs", "logs-000002");
            selected.getCreateIndexRequest().settings(Settings.builder().put("index.number_of_shards", 2));
            selected.getCreateIndexRequest().mapping(Map.of("properties", Map.of("chosen", Map.of("type", "keyword"))));
            return selected;
        }), (node, metadata) -> {
            final var response = send(node, "POST", "/logs/_rollover", "{}");
            assertEquals(response.body(), 200, response.statusCode());
            final var created = metadata.describe("logs-000002").orElseThrow();
            assertEquals(2, created.numberOfShards());
            assertTrue(created.mapping(), created.mapping().contains("chosen"));
        });
    }

    public void testResponseAuthorizationRunsBeforeHttpSuccess() throws Exception {
        withNode(PolicyFilterPlugin.denyAfter(RolloverAction.NAME), (node, metadata) -> {
            final var response = send(node, "POST", "/logs/_rollover?dry_run=true", "{}");
            assertEquals(response.body(), 403, response.statusCode());
        });
    }

    public void testNativeResponseRewritesReachTheCaller() throws Exception {
        withNode(PolicyFilterPlugin.response(RolloverAction.NAME, response -> {
            assertTrue(response.toString(), response instanceof RolloverResponse);
            return new RolloverResponse("visible-old", "visible-new", Map.of(), true, false, false, false);
        }), (node, metadata) -> {
            final var response = send(node, "POST", "/logs/_rollover?dry_run=true", "{}");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("visible-old"));
            assertFalse(response.body(), response.body().contains("logs-000001"));
        });
    }

    public void testCachedNativeResponsesDoNotPerformRollover() throws Exception {
        final var cached = new RolloverResponse("cached-old", "cached-new", Map.of(), true, false, false, false);
        withNode(PolicyFilterPlugin.cached(RolloverAction.NAME, cached), (node, metadata) -> {
            final var response = send(node, "POST", "/logs/_rollover", "{}");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("cached-old"));
            assertTrue(metadata.describe("logs-000002").isEmpty());
        });
    }

    public void testUnsupportedAdmittedConditionsAreRefusedBeforeMutation() throws Exception {
        withNode(PolicyFilterPlugin.request(RolloverAction.NAME, request -> {
            final var selected = new RolloverRequest("logs", "logs-000002");
            selected.addMaxIndexSizeCondition(new ByteSizeValue(1L));
            return selected;
        }), (node, metadata) -> {
            final var response = send(node, "POST", "/logs/_rollover", "{}");
            assertEquals(response.body(), 501, response.statusCode());
            assertTrue(metadata.describe("logs-000002").isEmpty());
        });
    }

    public void testDataStreamRolloverRejectsExplicitShardSettings() throws Exception {
        withNode(PolicyFilterPlugin.request(RolloverAction.NAME, request -> request), (node, metadata) -> {
            createStream(node);
            final var response = send(node, "POST", "/events/_rollover", "{\"settings\":{\"number_of_shards\":2}}");
            assertEquals(response.body(), 400, response.statusCode());
            assertTrue(metadata.describe(".ds-events-000002").isEmpty());
            assertEquals(".ds-events-000001", metadata.resolve("events").alias().writeIndex());
        });
    }

    public void testDataStreamRolloverWithoutOverridesStillSucceeds() throws Exception {
        withNode(PolicyFilterPlugin.request(RolloverAction.NAME, request -> request), (node, metadata) -> {
            createStream(node);
            final var response = send(node, "POST", "/events/_rollover", "{}");
            assertEquals(response.body(), 200, response.statusCode());
            assertEquals(".ds-events-000002", metadata.resolve("events").alias().writeIndex());
        });
    }

    public void testAdmittedConditionsCanPreventAnUnconditionalRollover() throws Exception {
        withNode(PolicyFilterPlugin.request(RolloverAction.NAME, request -> {
            final var selected = new RolloverRequest("logs", "logs-000002");
            selected.addMaxIndexDocsCondition(1000L);
            return selected;
        }), (node, metadata) -> {
            node.activateWriter(metadata, "logs-000001", 0).orElseThrow();
            final var response = send(node, "POST", "/logs/_rollover", "{}");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"rolled_over\":false"));
            assertTrue(metadata.describe("logs-000002").isEmpty());
        });
    }

    private static void createStream(ServerlessNode node) throws Exception {
        var response = send(node, "PUT", "/_index_template/events", "{\"index_patterns\":[\"events*\"],\"data_stream\":{}}");
        assertEquals(response.body(), 200, response.statusCode());
        response = send(node, "PUT", "/_data_stream/events", null);
        assertEquals(response.body(), 200, response.statusCode());
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
