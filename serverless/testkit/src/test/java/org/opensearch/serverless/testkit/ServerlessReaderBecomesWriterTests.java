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
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A node that serves a shard as a reader and then becomes its writer must never be left with the head and without
 * the shard.
 *
 * <p><b>What the fleet run found.</b> A reader and the writer that replaces it on the same node share a
 * {@code ShardId}. Every pass that lets go of a reader decided it was one, then went to the store, then closed the
 * shard by id -- and a write arriving in between turned the reader into the writer. The pass closed the writer. The
 * head still named the node and its lease was live, so every other node forwarded to it; the node answered
 * "acquiring, retry" for ever, and an acknowledged write sat in the log with nobody to replay it.
 *
 * <p>The window is one register read. {@link HookedBlobStore} puts the takeover inside it on every run.
 */
public class ServerlessReaderBecomesWriterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-reader-writer")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .build();
    }

    /** Publishes a commit of {@code docs} documents from a writer node of its own, then closes it. */
    private void publishFromElsewhere(MetadataPlane plane, String name, int docs) throws Exception {
        try (ServerlessNode writer = new ServerlessNode(nodeSettings(name, "ingest"))) {
            writer.start();
            final long term = plane.activate("alpha", 0, writer.localNode().getId(), writer.localNode().getEphemeralId()).head().term();
            final ShardId shardId = writer.syncFrom(plane).iterator().next();
            for (int i = 1; i <= docs; i++) {
                ShardOps.indexDoc(writer.reconciler().shard(shardId), name + "-" + i, "{\"msg\":\"searchable\",\"n\":" + i + "}");
            }
            writer.reconciler().shard(shardId).refresh("reader-writer");
            writer.publishShard(shardId, term);
        }
    }

    /** The stale-reader pass, with the node taking the shard as its writer while the pass reads the manifest. */
    public void testRefreshingAStaleReaderDoesNotCloseTheWriterThatReplacedIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final HookedBlobStore store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        publishFromElsewhere(plane, "first-writer", 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("both", "ingest,search"))) {
            node.start();
            final ShardId shardId = node.serveAsReader(plane, "alpha", 0);
            assertTrue(node.reconciler().readerShards().contains(shardId));

            // The commit moves on, so the reader is stale and the next pass will let it go.
            clock.addAndGet(TTL + 1_000);
            publishFromElsewhere(plane, "second-writer", 2);
            clock.addAndGet(TTL + 1_000);

            try (BackgroundReconciler loop = new BackgroundReconciler(node, plane)) {
                final AtomicReference<Exception> takeover = new AtomicReference<>();
                store.onNextRegisterRead("alpha#", SegmentPublisher.MANIFEST, () -> {
                    try {
                        // A write arrives: the reader is closed and the node opens the shard as its writer.
                        assertTrue("the takeover must succeed", node.activateWriter(plane, "alpha", 0).isPresent());
                    } catch (Exception e) {
                        takeover.set(e);
                    }
                });
                loop.refreshReaders();
                assertTrue("the takeover must have run inside the pass's manifest read", store.fired());
                if (takeover.get() != null) {
                    throw takeover.get();
                }
            }

            final var head = plane.heads().read("alpha", 0).orElseThrow();
            assertEquals("the node holds the head", node.localNode().getId(), head.ownerNodeId());
            assertNotNull(
                "and so it must still hold the shard: a head with nothing open behind it answers 'retry' until the node dies",
                node.reconciler().shard(shardId)
            );
            assertFalse("as its writer, not as a reader", node.reconciler().readerShards().contains(shardId));
        }
    }
}
