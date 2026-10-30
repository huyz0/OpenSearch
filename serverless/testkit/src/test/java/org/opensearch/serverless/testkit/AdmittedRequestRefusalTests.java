/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
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

public class AdmittedRequestRefusalTests extends OpenSearchTestCase {
    public void testGetRoutingIntroducedByPolicyIsExplicitlyUnsupported() throws Exception {
        assertUnsupported(PolicyFilterPlugin.request(GetAction.NAME, request -> ((GetRequest) request).routing("elsewhere")), false);
    }

    public void testGetStoredFieldsIntroducedByPolicyAreExplicitlyUnsupported() throws Exception {
        assertUnsupported(PolicyFilterPlugin.request(GetAction.NAME, request -> ((GetRequest) request).storedFields("field")), false);
    }

    public void testDeleteByQueryRoutingIntroducedByPolicyIsExplicitlyUnsupported() throws Exception {
        assertUnsupported(
            PolicyFilterPlugin.request(DeleteByQueryAction.NAME, request -> ((DeleteByQueryRequest) request).setRouting("elsewhere")),
            true
        );
    }

    public void testDeleteByQuerySlicingIntroducedByPolicyIsExplicitlyUnsupported() throws Exception {
        assertUnsupported(
            PolicyFilterPlugin.request(DeleteByQueryAction.NAME, request -> ((DeleteByQueryRequest) request).setSlices(2)),
            true
        );
    }

    public void testUnrelatedUnsupportedFailuresRemainInternalErrors() throws Exception {
        assertResponse(PolicyFilterPlugin.request(GetAction.NAME, request -> {
            throw new UnsupportedOperationException("unexpected plugin failure");
        }), false, 500);
    }

    private void assertUnsupported(PolicyFilterPlugin plugin, boolean deletion) throws Exception {
        assertResponse(plugin, deletion, 501);
    }

    private void assertResponse(PolicyFilterPlugin plugin, boolean deletion, int expectedStatus) throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            metadata.createIndex(new IndexDescriptor("docs", "uuid-docs", 1, "{}", null));
            final var settings = Settings.builder()
                .put("node.name", "admitted-request-refusal")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                final var shard = node.activateWriter(metadata, "docs", 0).orElseThrow();
                node.index(shard, "kept", "{}");
                final var address = node.boundHttpAddress().publishAddress();
                final String suffix = deletion ? "/docs/_delete_by_query" : "/docs/_doc/kept";
                try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
                    final var request = HttpRequest.newBuilder(
                        URI.create("http://" + address.getAddress() + ":" + address.getPort() + suffix)
                    ).timeout(Duration.ofSeconds(10));
                    if (deletion) {
                        request.header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString("{\"query\":{\"match_all\":{}}}"));
                    } else {
                        request.GET();
                    }
                    final var response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
                    assertEquals(response.body(), expectedStatus, response.statusCode());
                    if (expectedStatus == 501) {
                        assertTrue(response.body(), response.body().contains("not supported") || response.body().contains("no routing"));
                    } else {
                        assertTrue(response.body(), response.body().contains("unexpected plugin failure"));
                    }
                }
                assertTrue("refusal must not delete the document", node.get(shard, "kept").found());
            }
        }
    }
}
