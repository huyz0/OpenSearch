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

public class DataStreamDeletionIncarnationTests extends OpenSearchTestCase {
    private static final String FIRST = ".ds-events-000001";
    private static final String SECOND = ".ds-events-000002";

    private record Fixture(ServerlessNode node, MetadataPlane metadata, HookedBlobStore store) {
    }

    private void withNode(boolean conditional, org.opensearch.common.CheckedConsumer<Fixture, Exception> work) throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath().add("test-base"), () -> 1000L, 30_000L);
            metadata.descriptors().setConditionalDelete(conditional);
            metadata.createIndex(new IndexDescriptor(FIRST, "uuid-first", 1, "{}", null));
            metadata.createIndex(new IndexDescriptor(SECOND, "uuid-second", 1, "{}", null));
            metadata.createAlias(new AliasRecord("events", List.of(FIRST, SECOND), true, 2L, "@timestamp"));
            final var settings = Settings.builder()
                .put("node.name", "stream-delete-incarnation")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(metadata);
                work.accept(new Fixture(node, metadata, store));
            }
        }
    }

    private static HttpResponse<String> delete(ServerlessNode node) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            return client.send(
                HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/_data_stream/events"))
                    .timeout(Duration.ofSeconds(30))
                    .DELETE()
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
        }
    }

    private void assertReplacementSurvives(boolean conditional) throws Exception {
        withNode(conditional, fixture -> {
            final Runnable recreate = () -> {
                try {
                    if (fixture.metadata().aliasWithGeneration("events").isPresent()) {
                        throw new IllegalStateException("the replacement must happen after the old stream is unpublished");
                    }
                    if (fixture.metadata().deleteIndex(FIRST, "uuid-first") == false) {
                        throw new IllegalStateException("the first backing must still have its original incarnation");
                    }
                    fixture.metadata().createIndex(new IndexDescriptor(FIRST, "uuid-replacement", 1, "{}", null));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            };
            if (conditional) {
                fixture.store().onNextRegisterDeleteAfter("test-base/indices/", "events", recreate);
            } else {
                fixture.store().onNextRegisterSwapAfter("test-base/indices/", "events", recreate);
            }
            final var result = delete(fixture.node());
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue("the replacement must occur inside the deletion window", fixture.store().fired());
            assertTrue(fixture.metadata().aliasWithGeneration("events").isEmpty());
            assertEquals(
                "the old stream must not delete a new index sharing a backing name",
                "uuid-replacement",
                fixture.metadata().describe(FIRST).orElseThrow().uuid()
            );
            assertTrue("unchanged old backings must still be deleted", fixture.metadata().describe(SECOND).isEmpty());
        });
    }

    public void testConditionalStreamDeletionDoesNotDeleteAReplacementBacking() throws Exception {
        assertReplacementSurvives(true);
    }

    public void testTombstonedStreamDeletionDoesNotDeleteAReplacementBacking() throws Exception {
        assertReplacementSurvives(false);
    }

    public void testUnchangedBackingIndicesAreDeletedWithTheStream() throws Exception {
        for (boolean conditional : new boolean[] { true, false }) {
            withNode(conditional, fixture -> {
                final var result = delete(fixture.node());
                assertEquals(result.body(), 200, result.statusCode());
                assertTrue(fixture.metadata().aliasWithGeneration("events").isEmpty());
                assertTrue(fixture.metadata().describe(FIRST).isEmpty());
                assertTrue(fixture.metadata().describe(SECOND).isEmpty());
            });
        }
    }
}
