/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
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

public class ClusterSettingsReadPolicyTests extends OpenSearchTestCase {
    private record Fixture(ServerlessNode node, MetadataPlane metadata, CountingBlobStore store) {
    }

    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedConsumer<Fixture, Exception> work) throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "cluster-settings-read-policy")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.clusterConfig().update(Map.of("original.key", "secret"));
                store.reset();
                work.accept(new Fixture(node, metadata, store));
            }
        }
    }

    private static ClusterStateResponse state(Settings persistent, Settings transientSettings) {
        final var name = new ClusterName("policy-state");
        return new ClusterStateResponse(
            name,
            ClusterState.builder(name)
                .metadata(Metadata.builder().persistentSettings(persistent).transientSettings(transientSettings))
                .build(),
            false
        );
    }

    private static HttpResponse<String> send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            return client.send(
                HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
        }
    }

    public void testReadRequestDescribesOnlyTheMetadataItNeeds() throws Exception {
        withNode(PolicyFilterPlugin.request(ClusterStateAction.NAME, request -> {
            final var selected = (ClusterStateRequest) request;
            assertTrue(selected.metadata());
            assertFalse(selected.routingTable());
            assertFalse(selected.nodes());
            assertFalse(selected.blocks());
            assertFalse(selected.customs());
            return request;
        }), fixture -> {
            final var result = send(fixture.node(), "GET", "/_cluster/settings", null);
            assertEquals(result.body(), 200, result.statusCode());
        });
    }

    public void testTypedStateResponseAllowsSettingsRedaction() throws Exception {
        withNode(PolicyFilterPlugin.response(ClusterStateAction.NAME, response -> {
            final var actual = (ClusterStateResponse) response;
            assertEquals("secret", actual.getState().metadata().persistentSettings().get("original.key"));
            return state(Settings.EMPTY, Settings.EMPTY);
        }), fixture -> {
            final var result = send(fixture.node(), "GET", "/_cluster/settings", null);
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"persistent\":{}"));
            assertFalse(result.body(), result.body().contains("secret"));
        });
    }

    public void testCachedStateIsRenderedWithoutReadingTheConfiguration() throws Exception {
        withNode(
            PolicyFilterPlugin.cached(
                ClusterStateAction.NAME,
                state(Settings.builder().put("cached", "safe").build(), Settings.builder().put("filtered", "value").build())
            ),
            fixture -> {
                final var result = send(fixture.node(), "GET", "/_cluster/settings", null);
                assertEquals(result.body(), 200, result.statusCode());
                assertTrue(result.body(), result.body().contains("\"cached\":\"safe\""));
                assertTrue(result.body(), result.body().contains("\"filtered\":\"value\""));
                assertEquals(0L, fixture.store().reads());
            }
        );
    }

    public void testAdmittedRequestCanExcludeMetadataWithoutReadingIt() throws Exception {
        withNode(PolicyFilterPlugin.request(ClusterStateAction.NAME, request -> new ClusterStateRequest().clear()), fixture -> {
            final var result = send(fixture.node(), "GET", "/_cluster/settings", null);
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"persistent\":{}"));
            assertEquals(0L, fixture.store().reads());
        });
    }

    public void testAdmittedMetadataVersionWaitIsRefusedBeforeReading() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                ClusterStateAction.NAME,
                request -> new ClusterStateRequest().clear().metadata(true).waitForMetadataVersion(7L)
            ),
            fixture -> {
                final var result = send(fixture.node(), "GET", "/_cluster/settings", null);
                assertEquals(result.body(), 501, result.statusCode());
                assertEquals(0L, fixture.store().reads());
            }
        );
    }

    public void testAdmittedFleetStateRequestIsRefusedBeforeReading() throws Exception {
        withNode(PolicyFilterPlugin.request(ClusterStateAction.NAME, request -> ((ClusterStateRequest) request).nodes(true)), fixture -> {
            final var result = send(fixture.node(), "GET", "/_cluster/settings", null);
            assertEquals(result.body(), 501, result.statusCode());
            assertEquals(0L, fixture.store().reads());
        });
    }

    public void testFlatSettingsIsHonoredWhenReading() throws Exception {
        withNode(PolicyFilterPlugin.request(ClusterStateAction.NAME, request -> request), fixture -> {
            final var result = send(fixture.node(), "GET", "/_cluster/settings?flat_settings=true", null);
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"original.key\":\"secret\""));
        });
    }

    public void testFlatSettingsIsHonoredWhenUpdating() throws Exception {
        withNode(PolicyFilterPlugin.request(ClusterStateAction.NAME, request -> request), fixture -> {
            final var result = send(
                fixture.node(),
                "PUT",
                "/_cluster/settings?flat_settings=true",
                "{\"persistent\":{\"changed.key\":\"value\"}}"
            );
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"changed.key\":\"value\""));
        });
    }

    public void testCachedStateTimeoutIsReportedWithoutReading() throws Exception {
        final var cached = new ClusterStateResponse(new ClusterName("policy-timeout"), null, true);
        withNode(PolicyFilterPlugin.cached(ClusterStateAction.NAME, cached), fixture -> {
            final var result = send(fixture.node(), "GET", "/_cluster/settings", null);
            assertEquals(result.body(), 408, result.statusCode());
            assertEquals(0L, fixture.store().reads());
        });
    }
}
