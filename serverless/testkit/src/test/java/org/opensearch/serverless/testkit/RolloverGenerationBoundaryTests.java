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
import java.util.Set;

public class RolloverGenerationBoundaryTests extends OpenSearchTestCase {
    private record Fixture(ServerlessNode node, MetadataPlane metadata, CountingBlobStore store) {
        void index(String name, String uuid) throws Exception {
            metadata.createIndex(new IndexDescriptor(name, uuid, 1, "{}", null));
        }

        String stream(String name, long generation) throws Exception {
            final String backing = backing(name, generation);
            index(backing, "uuid-current");
            metadata.createAlias(new AliasRecord(name, List.of(backing), true, generation, "@timestamp"));
            store.reset();
            return backing;
        }
    }

    private static String backing(String name, long generation) {
        return org.opensearch.cluster.metadata.DataStream.getDefaultBackingIndexName(name, generation);
    }

    private void withNode(org.opensearch.common.CheckedConsumer<Fixture, Exception> work) throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "rollover-counter-boundaries")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.indexTemplates().put("streams", "{\"index_patterns\":[\"stream-*\"],\"data_stream\":{}}");
                work.accept(new Fixture(node, metadata, store));
            }
        }
    }

    private static HttpResponse<String> rollover(ServerlessNode node, String path) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            return client.send(
                HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{}"))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
        }
    }

    private void assertExhaustedStreamIsUnchanged(boolean dryRun) throws Exception {
        withNode(fixture -> {
            final String current = fixture.stream("stream-exhausted", Long.MAX_VALUE);
            final var result = rollover(fixture.node(), "/stream-exhausted/_rollover" + (dryRun ? "?dry_run=true" : ""));
            assertEquals(result.body(), 400, result.statusCode());
            assertEquals("exhaustion must be detected before attempting metadata writes", 0L, fixture.store().writes());
            final var record = fixture.metadata().aliasWithGeneration("stream-exhausted").orElseThrow().alias();
            assertEquals(Long.MAX_VALUE, record.generation());
            assertEquals(List.of(current), record.indices());
            assertTrue(fixture.metadata().describe(backing("stream-exhausted", Long.MIN_VALUE)).isEmpty());
        });
    }

    public void testExhaustedStreamGenerationIsRefusedBeforeWriting() throws Exception {
        assertExhaustedStreamIsUnchanged(false);
    }

    public void testExhaustedStreamDryRunDoesNotPreviewANegativeGeneration() throws Exception {
        assertExhaustedStreamIsUnchanged(true);
    }

    public void testSkippingAnOccupiedLastGenerationDoesNotWrap() throws Exception {
        withNode(fixture -> {
            final String name = "stream-collision";
            final String current = fixture.stream(name, Long.MAX_VALUE - 1);
            final String occupied = backing(name, Long.MAX_VALUE);
            fixture.index(occupied, "uuid-foreign");
            final var before = Set.copyOf(fixture.metadata().namesWithPrefix(".ds-" + name + "-", 20));
            final var result = rollover(fixture.node(), "/" + name + "/_rollover");
            assertEquals(result.body(), 400, result.statusCode());
            assertEquals(before, Set.copyOf(fixture.metadata().namesWithPrefix(".ds-" + name + "-", 20)));
            final var record = fixture.metadata().aliasWithGeneration(name).orElseThrow().alias();
            assertEquals(Long.MAX_VALUE - 1, record.generation());
            assertEquals(List.of(current), record.indices());
            assertEquals("uuid-foreign", fixture.metadata().describe(occupied).orElseThrow().uuid());
        });
    }

    public void testAValidCollisionSkipCanReachTheLastGeneration() throws Exception {
        withNode(fixture -> {
            final String name = "stream-boundary";
            fixture.stream(name, Long.MAX_VALUE - 2);
            fixture.index(backing(name, Long.MAX_VALUE - 1), "uuid-foreign");
            final var result = rollover(fixture.node(), "/" + name + "/_rollover");
            assertEquals(result.body(), 200, result.statusCode());
            final var record = fixture.metadata().aliasWithGeneration(name).orElseThrow().alias();
            assertEquals(Long.MAX_VALUE, record.generation());
            assertEquals(backing(name, Long.MAX_VALUE), record.writeIndex());
            assertEquals("uuid-foreign", fixture.metadata().describe(backing(name, Long.MAX_VALUE - 1)).orElseThrow().uuid());
        });
    }

    public void testExhaustedAliasSuffixIsRefusedBeforeWriting() throws Exception {
        withNode(fixture -> {
            final String current = "logs-" + Long.MAX_VALUE;
            fixture.index(current, "uuid-current");
            fixture.metadata().createAlias(new AliasRecord("logs", List.of(current)));
            fixture.store().reset();
            final var result = rollover(fixture.node(), "/logs/_rollover");
            assertEquals(result.body(), 400, result.statusCode());
            assertEquals(0L, fixture.store().writes());
            assertEquals(List.of(current), fixture.metadata().aliasWithGeneration("logs").orElseThrow().alias().indices());
        });
    }

    public void testAliasSuffixCanReachTheLastValue() throws Exception {
        withNode(fixture -> {
            fixture.index("logs-" + (Long.MAX_VALUE - 1), "uuid-current");
            fixture.metadata().createAlias(new AliasRecord("logs", List.of("logs-" + (Long.MAX_VALUE - 1))));
            final var result = rollover(fixture.node(), "/logs/_rollover");
            assertEquals(result.body(), 200, result.statusCode());
            assertEquals(List.of("logs-" + Long.MAX_VALUE), fixture.metadata().aliasWithGeneration("logs").orElseThrow().alias().indices());
        });
    }

    public void testExplicitAliasTargetDoesNotNeedToIncrementTheSuffix() throws Exception {
        withNode(fixture -> {
            fixture.index("logs-" + Long.MAX_VALUE, "uuid-current");
            fixture.metadata().createAlias(new AliasRecord("logs", List.of("logs-" + Long.MAX_VALUE)));
            final var result = rollover(fixture.node(), "/logs/_rollover/chosen-target");
            assertEquals(result.body(), 200, result.statusCode());
            assertEquals(List.of("chosen-target"), fixture.metadata().aliasWithGeneration("logs").orElseThrow().alias().indices());
        });
    }
}
