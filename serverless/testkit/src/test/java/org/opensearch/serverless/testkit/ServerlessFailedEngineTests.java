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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A writer whose engine fails gives the shard up to be taken again, with nothing acknowledged lost.
 *
 * <p>A failed engine used to leave the shard counted as open: the node kept the head with a live lease, every peer
 * forwarded to it, every activation found the shard already held, and every request was answered "engine is closed".
 * A fleet run stranded acknowledged writes that way -- durable in the log, and readable by nobody.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessFailedEngineTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-failed-engine")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** The failed writer is closed, its head kept, and taking it again replays what only the log held. */
    public void testAFailedWriterIsTakenAgainFromItsLog() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("failed-engine"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            for (int i = 0; i < 5; i++) {
                node.index(shardId, "published-" + i, "{\"msg\":\"m\",\"n\":" + i + "}");
            }
            loop.publishAll();
            // Acknowledged, in the log only.
            node.index(shardId, "logged", "{\"msg\":\"m\",\"n\":9}");
            final long term = plane.heads().read("alpha", 0).orElseThrow().term();

            node.reconciler().shard(shardId).failShard("injected", new java.io.IOException("injected engine failure"));

            assertBusy(() -> assertTrue("the failed writer is closed", node.reconciler().openShards().isEmpty()));
            final var head = plane.heads().read("alpha", 0).orElseThrow();
            assertEquals("its head is kept, not given back unpublished", node.localNode().getId(), head.ownerNodeId());
            assertEquals(term, head.term());

            // What the doubt the failure raised does: takes the shard again.
            final var taken = loop.activateForRequest("alpha", 0).get();
            assertTrue("the shard is taken again", taken.isPresent());
            assertTrue("at a higher term, fencing the failed engine", plane.heads().read("alpha", 0).orElseThrow().term() > term);
            assertTrue("and what only the log held is there", node.get(taken.get(), "logged").found());
            assertTrue(node.get(taken.get(), "published-3").found());
        }
    }

    public void testAFailedReaderIsClosedEvenWhileAQueryIsAdmitted() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, TTL);
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-failed-reader", 1, MAPPING, null));
            try (
                var writer = new ServerlessNode(nodeSettings("failed-reader-writer"));
                var reader = new ServerlessNode(
                    Settings.builder().put(nodeSettings("failed-reader")).put("serverless.roles", "search").build()
                )
            ) {
                writer.start();
                writer.setMetadataPlane(plane);
                final var writerId = writer.activateWriter(plane, "alpha", 0).orElseThrow();
                writer.index(writerId, "doc", "{\"msg\":\"survives\",\"n\":1}");
                writer.publishShard(writerId, plane.heads().read("alpha", 0).orElseThrow().term());
                assertTrue(plane.heads().release("alpha", 0, writer.localNode().getId()));
                reader.start();
                reader.setMetadataPlane(plane);
                final var id = reader.serveAsReader(plane, "alpha", 0);
                assertTrue(reader.reconciler().isReader(id));
                final var failed = reader.reconciler().shard(id);
                reader.reconciler().enter(id);
                try {
                    failed.failShard("injected", new java.io.IOException("injected reader failure"));
                    assertBusy(
                        () -> assertNull("failed readers must not remain cached behind their active query", reader.reconciler().shard(id))
                    );
                } finally {
                    reader.reconciler().exit(id);
                }
                final var reopened = reader.serveAsReader(plane, "alpha", 0);
                final var replacement = reader.reconciler().shard(reopened);
                assertNotSame(failed, replacement);
                assertEquals(
                    org.opensearch.serverless.shard.ShardReconciler.FailedShardRelease.NOT_CURRENT,
                    reader.reconciler().releaseFailedShard(failed, "delayed failure")
                );
                assertSame(replacement, reader.reconciler().shard(reopened));
                assertTrue(reader.get(reopened, "doc").found());
            }
        }
    }

}
