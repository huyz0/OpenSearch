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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public class TemplateResolutionCostTests extends OpenSearchTestCase {
    private Settings settings() {
        return Settings.builder()
            .put("node.name", "template-resolution-cost")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    @FunctionalInterface
    private interface Work {
        void accept(ServerlessNode node, MetadataPlane metadata, HookedBlobStore store) throws Exception;
    }

    private void withNode(Work work) throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            try (var node = new ServerlessNode(settings())) {
                node.start();
                node.setMetadataPlane(metadata);
                work.accept(node, metadata, store);
            }
        }
    }

    private static void failUnusedComponentRead(HookedBlobStore store, AtomicBoolean read) {
        store.onNextRegisterRead("templates/component", "_version", () -> {
            read.set(true);
            throw new IllegalStateException("the unrelated component store is unavailable");
        });
    }

    public void testUnmatchedIndexCreationDoesNotReadComponentTemplates() throws Exception {
        withNode((node, metadata, store) -> {
            final var read = new AtomicBoolean();
            failUnusedComponentRead(store, read);
            final var answer = send(node, "PUT", "/unmatched", "{}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertFalse("no component is relevant when no index template matches", read.get());
        });
    }

    public void testTemplatesWithoutCompositionDoNotReadComponentTemplates() throws Exception {
        withNode((node, metadata, store) -> {
            metadata.indexTemplates().put("logs", "{\"index_patterns\":[\"logs-*\"],\"template\":{\"settings\":{\"number_of_shards\":2}}}");
            final var read = new AtomicBoolean();
            failUnusedComponentRead(store, read);
            final var answer = send(node, "PUT", "/logs-1", "{}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertEquals(2, metadata.describe("logs-1").orElseThrow().numberOfShards());
            assertFalse(read.get());
        });
    }

    public void testColdCreationFetchesOnlyReferencedComponentBodies() throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var writer = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            writer.componentTemplates().put("chosen", "{\"template\":{\"mappings\":{\"properties\":{\"value\":{\"type\":\"keyword\"}}}}}");
            for (int i = 0; i < 20; i++) {
                writer.componentTemplates().put("unrelated-" + i, "{\"version\":" + i + ",\"template\":{}}");
            }
            writer.indexTemplates().put("logs", "{\"index_patterns\":[\"logs-*\"],\"composed_of\":[\"chosen\"]}");
            // A fresh plane has none of the writer's configuration caches.
            final var reader = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            try (var node = new ServerlessNode(settings())) {
                node.start();
                node.setMetadataPlane(reader);
                store.reset();
                final var answer = send(node, "PUT", "/logs-1", "{}");
                assertEquals(answer.body(), 200, answer.statusCode());
                assertEquals("one winning index template and its one referenced component", 2L, store.blobReads());
                assertTrue(reader.describe("logs-1").orElseThrow().mapping().contains("keyword"));
            }
        }
    }

    public void testDescribingAStreamNeedsTheTemplateNameWithoutItsComponents() throws Exception {
        withNode((node, metadata, store) -> {
            metadata.componentTemplates().put("base", "{\"template\":{}}");
            metadata.indexTemplates().put("events", "{\"index_patterns\":[\"events*\"],\"data_stream\":{},\"composed_of\":[\"base\"]}");
            var answer = send(node, "PUT", "/_data_stream/events", null);
            assertEquals(answer.body(), 200, answer.statusCode());
            final var read = new AtomicBoolean();
            failUnusedComponentRead(store, read);
            answer = send(node, "GET", "/_data_stream/events", null);
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"template\":\"events\""));
            assertFalse(read.get());
        });
    }

    public void testListingStreamsReadsTheIndexTemplateMarkerOnce() throws Exception {
        withNode((node, metadata, store) -> {
            metadata.indexTemplates().put("events", "{\"index_patterns\":[\"events*\"],\"data_stream\":{}}");
            for (int i = 0; i < 3; i++) {
                final var created = send(node, "PUT", "/_data_stream/events-" + i, null);
                assertEquals(created.body(), 200, created.statusCode());
            }
            final var reads = new java.util.concurrent.atomic.AtomicInteger();
            final Runnable[] record = new Runnable[1];
            record[0] = () -> {
                reads.incrementAndGet();
                store.onNextRegisterRead("templates/index", "_version", record[0]);
            };
            store.onNextRegisterRead("templates/index", "_version", record[0]);
            final var answer = send(node, "GET", "/_data_stream/*", null);
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("events-0"));
            assertTrue(answer.body(), answer.body().contains("events-1"));
            assertTrue(answer.body(), answer.body().contains("events-2"));
            assertEquals("one captured template index serves the whole response", 1, reads.get());
        });
    }

    public void testWarmEmptyTemplateStoresDoNotListOnEveryCreate() throws Exception {
        withNode((node, metadata, store) -> {
            var answer = send(node, "PUT", "/warm-1", "{}");
            assertEquals(answer.body(), 200, answer.statusCode());
            store.failListingsUnder("templates/index");
            answer = send(node, "PUT", "/warm-2", "{}");
            assertEquals(answer.body(), 200, answer.statusCode());
        });
    }

    public void testCreationRetriesAnIndexTemplateRevisionReplacedAfterTheMarkerRead() throws Exception {
        withNode((node, metadata, store) -> {
            metadata.indexTemplates().put("logs", "{\"index_patterns\":[\"logs-*\"],\"template\":{\"settings\":{\"number_of_shards\":2}}}");
            final var reader = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            node.setMetadataPlane(reader);
            store.onNextRegisterRead("templates/index", "_version", () -> {
                try {
                    metadata.indexTemplates()
                        .put("logs", "{\"index_patterns\":[\"logs-*\"],\"template\":{\"settings\":{\"number_of_shards\":3}}}");
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
            final var answer = send(node, "PUT", "/logs-1", "{}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(store.fired());
            assertEquals(3, reader.describe("logs-1").orElseThrow().numberOfShards());
        });
    }

    public void testCreationRetriesAComponentRevisionReplacedAfterTheMarkerRead() throws Exception {
        withNode((node, metadata, store) -> {
            metadata.componentTemplates().put("base", "{\"template\":{\"settings\":{\"number_of_shards\":2}}}");
            metadata.indexTemplates().put("logs", "{\"index_patterns\":[\"logs-*\"],\"composed_of\":[\"base\"]}");
            final var reader = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            node.setMetadataPlane(reader);
            store.onNextRegisterRead("templates/component", "_version", () -> {
                try {
                    metadata.componentTemplates().put("base", "{\"template\":{\"settings\":{\"number_of_shards\":3}}}");
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
            final var answer = send(node, "PUT", "/logs-1", "{}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(store.fired());
            assertEquals(3, reader.describe("logs-1").orElseThrow().numberOfShards());
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
