/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
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

public class ClusterSettingsRequestExecutionTests extends OpenSearchTestCase {
    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "cluster-settings-filter-contract")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                work.accept(node, metadata);
            }
        }
    }

    private static ClusterUpdateSettingsResponse response(String body) throws Exception {
        try (
            var parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, body)
        ) {
            return ClusterUpdateSettingsResponse.fromXContent(parser);
        }
    }

    public void testNativeSettingsRequestExposesThePersistentBody() throws Exception {
        withNode(PolicyFilterPlugin.request(ClusterUpdateSettingsAction.NAME, request -> {
            final var update = (ClusterUpdateSettingsRequest) request;
            assertEquals("value", update.persistentSettings().get("original.key"));
            assertTrue(update.transientSettings().isEmpty());
            return request;
        }), (node, metadata) -> {
            final var answer = send(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"original.key\":\"value\"}}");
            assertEquals(answer.body(), 200, answer.statusCode());
        });
    }

    public void testSettingsUpdateUsesTheAdmittedPersistentValues() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                ClusterUpdateSettingsAction.NAME,
                request -> new ClusterUpdateSettingsRequest().persistentSettings(Map.of("chosen.key", "safe"))
            ),
            (node, metadata) -> {
                final var answer = send(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"original.key\":\"value\"}}");
                assertEquals(answer.body(), 200, answer.statusCode());
                final var stored = metadata.clusterConfig().read().settings();
                assertNull(stored.get("original.key"));
                assertEquals("safe", stored.get("chosen.key"));
            }
        );
    }

    public void testSettingsUpdateHonorsCachedNativeResponses() throws Exception {
        final var cached = response("{\"acknowledged\":false,\"persistent\":{\"cached\":\"safe\"},\"transient\":{}}");
        withNode(PolicyFilterPlugin.cached(ClusterUpdateSettingsAction.NAME, cached), (node, metadata) -> {
            final var answer = send(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"original.key\":\"value\"}}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"acknowledged\":false"));
            assertTrue(answer.body(), answer.body().contains("\"cached\":\"safe\""));
            assertTrue(metadata.clusterConfig().read().settings().isEmpty());
        });
    }

    public void testNativeSettingsResponseRedactionReachesTheCaller() throws Exception {
        final var redacted = response("{\"acknowledged\":true,\"persistent\":{},\"transient\":{}}");
        withNode(PolicyFilterPlugin.response(ClusterUpdateSettingsAction.NAME, result -> {
            final var settings = (ClusterUpdateSettingsResponse) result;
            assertEquals("value", settings.getPersistentSettings().get("original.key"));
            return redacted;
        }), (node, metadata) -> {
            final var answer = send(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"original.key\":\"value\"}}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"persistent\":{}"));
            assertFalse(answer.body(), answer.body().contains("value"));
        });
    }

    public void testUnsupportedAdmittedTransientValuesAreRefused() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                ClusterUpdateSettingsAction.NAME,
                request -> new ClusterUpdateSettingsRequest().transientSettings(Map.of("chosen.key", "unsafe"))
            ),
            (node, metadata) -> {
                final var answer = send(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"original.key\":\"value\"}}");
                assertEquals(answer.body(), 501, answer.statusCode());
                assertTrue(metadata.clusterConfig().read().settings().isEmpty());
            }
        );
    }

    public void testUnsupportedAdmittedListsAreRefused() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                ClusterUpdateSettingsAction.NAME,
                request -> new ClusterUpdateSettingsRequest().persistentSettings(Map.of("chosen.key", List.of("one", "two")))
            ),
            (node, metadata) -> {
                final var answer = send(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"original.key\":\"value\"}}");
                assertEquals(answer.body(), 501, answer.statusCode());
                assertTrue(metadata.clusterConfig().read().settings().isEmpty());
            }
        );
    }

    public void testAdmittedNullValuesStillRemoveSettings() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                ClusterUpdateSettingsAction.NAME,
                request -> new ClusterUpdateSettingsRequest().persistentSettings(Settings.builder().putNull("chosen.key"))
            ),
            (node, metadata) -> {
                metadata.clusterConfig().update(Map.of("chosen.key", "old"));
                final var answer = send(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"original.key\":\"value\"}}");
                assertEquals(answer.body(), 200, answer.statusCode());
                assertNull(metadata.clusterConfig().read().settings().get("chosen.key"));
                assertNull(metadata.clusterConfig().read().settings().get("original.key"));
            }
        );
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
