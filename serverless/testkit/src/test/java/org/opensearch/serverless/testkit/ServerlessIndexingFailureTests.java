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

public class ServerlessIndexingFailureTests extends OpenSearchTestCase {
    public void testRejectedFieldKeepsItsNativeParsingErrorAndDoesNotEnterTheWal() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var descriptor = new IndexDescriptor("typed", "uuid-typed", 1, "{\"properties\":{\"n\":{\"type\":\"long\"}}}", null);
            metadata.createIndex(descriptor);
            final var settings = Settings.builder()
                .put("node.name", "indexing-native-failure")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(metadata);
                final var shard = node.activateWriter(metadata, descriptor.name(), 0).orElseThrow();
                final var address = node.boundHttpAddress().publishAddress();
                try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
                    final var request = HttpRequest.newBuilder(
                        URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/typed/_doc/bad")
                    )
                        .timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString("{\"n\":\"not-a-number\"}"))
                        .build();
                    final var response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    assertEquals(response.body(), 400, response.statusCode());
                    assertTrue(response.body(), response.body().contains("mapper_parsing_exception"));
                    assertFalse(node.get(shard, "bad").found());
                    assertTrue(metadata.walStore(descriptor.name(), descriptor.uuid(), 0).replayable().isEmpty());
                }
            }
        }
    }
}
