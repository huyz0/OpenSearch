/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.ObjectStores;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;

public class FilesystemShardLayoutTests extends OpenSearchTestCase {
    private void assertRefusedBeforePublication(String index) throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "fs-shard-layout")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(metadata);
                final var answer = put(node, index);
                assertEquals("an acknowledged index must have a usable physical shard key: " + answer.body(), 400, answer.statusCode());
                assertTrue("refusal must precede publishing the descriptor", metadata.describe(index).isEmpty());
            }
        }
    }

    public void testLongLogicalNameIsRefusedBeforeAnUnusableIndexIsPublished() throws Exception {
        assertRefusedBeforePublication("a".repeat(240));
    }

    public void testFilesystemComponentLimitCountsUtf8Bytes() throws Exception {
        assumeFalse("the underlying Windows component limit is expressed in UTF-16 units", org.apache.lucene.util.Constants.WINDOWS);
        assertRefusedBeforePublication("é".repeat(120));
    }

    public void testValidationThroughDecoratorsDoesNotTouchTheStore() throws Exception {
        final var root = createTempDir();
        final var counting = new CountingBlobStore(new FsBlobStore(1024, root, false));
        try (BlobStore store = new ObjectStores.Metered(new HookedBlobStore(counting))) {
            expectThrows(
                IllegalArgumentException.class,
                () -> store.validateKey(BlobPath.cleanPath().add("not-created").add("x".repeat(256)), "manifest")
            );
            assertEquals(0L, counting.reads());
            assertEquals(0L, counting.writes());
            assertFalse(Files.exists(root.resolve("not-created")));
        }
    }

    public void testBoundarySizedShardDirectoryCanActivate() throws Exception {
        final String index = "a".repeat(216);
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "fs-boundary")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(metadata);
                final var answer = put(node, index);
                assertEquals(answer.body(), 200, answer.statusCode());
                final var descriptor = metadata.describe(index).orElseThrow();
                final var path = RegisterMap.shardData(BlobPath.cleanPath(), index, descriptor.uuid(), 0).toArray();
                assertEquals(255, path[path.length - 1].getBytes(StandardCharsets.UTF_8).length);
                node.activateWriter(metadata, index, 0);
            }
        }
    }

    private static HttpResponse<String> put(ServerlessNode node, String index) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            return client.send(
                HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/" + index))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString("{}"))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
        }
    }
}
