/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fencing a writer that has lost its shard but has not yet noticed.
 *
 * <p>A node that checks its own lease before appending stops writing no later than the deadline it last
 * published, which turns an unbounded zombie into one bounded by the TTL. That is a bound, not a fence: the
 * check and the append are not atomic. The fence is the successor's: it fences each predecessor's node log
 * before reading it, so nothing appended after the takeover is replayed. That half is held by
 * {@code NodeLogNodeFaultTestCase}; the tests here cover the bound, and a write that landed before the takeover.
 *
 * <p>Runs against {@code FsBlobContainer} only.
 */
public class ServerlessWriteFencingTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-fencing")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane freshIndex(AtomicLong clock, Path objectStore) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, objectStore, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        return plane;
    }

    /**
     * A node past the expiry it last published for itself refuses to write, and lets the shard go.
     *
     * <p><b>Why this is not already covered by the heartbeat.</b> A node learns it lost a shard by reading
     * the shard-head, which it does on its heartbeat. A node that cannot reach the object store cannot read
     * the head <em>or</em> renew its lease — so the case where the existing check is guaranteed not to fire
     * is precisely the case where a zombie is possible. This check needs no I/O, which is what makes it
     * affordable on the write path, which is where it has to be.
     *
     * <p>The clock is advanced rather than the store broken, because what is being asserted is the node's
     * own reasoning about its own deadline: past it, this node may no longer claim to hold anything.
     */
    public void testANodePastItsOwnLeaseRefusesToWriteAndReleasesTheShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir());

        try (ServerlessNode node = new ServerlessNode(nodeSettings("fence-self"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            assertTrue("a node inside its lease writes normally", node.writes().index(shardId, "a", "{\"msg\":\"a\"}").seqNo() >= 0);

            // Past the deadline this node last published, with no renewal in between: from here on it has
            // no standing to claim it still holds the shard.
            clock.set(clock.get() + TTL + 1);

            final IOException refused = expectThrows(IOException.class, () -> node.writes().index(shardId, "b", "{\"msg\":\"b\"}"));
            assertTrue(
                "the refusal must say the lease is why, so an operator is not left looking at the store: " + refused.getMessage(),
                refused.getMessage().contains("lease has expired")
            );
            assertNull("a node that may no longer write must not go on holding the shard", node.reconciler().shard(shardId));
        }
    }

    /**
     * A writer whose record has landed and which then stalls past its lease, while another node takes over,
     * acknowledges the write when it resumes -- and the successor has it. The record is ahead of the fence, so
     * it is durable; refusing it, as this used to, is what made a write "refused" and later replayed.
     *
     * <p>Runs against {@code FsBlobContainer} only.
     */
    public void testAWriteThatLandedBeforeATakeoverIsAcknowledgedAndKept() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final Path objectStore = createTempDir();
        final java.util.concurrent.CountDownLatch landed = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch resume = new java.util.concurrent.CountDownLatch(1);
        final AtomicBoolean armed = new AtomicBoolean();
        final BlobStore stalling = new StallAfterLogWriteBlobStore(new FsBlobStore(1024, objectStore, false), armed, landed, resume);
        final MetadataPlane planeA = new MetadataPlane(stalling, BlobPath.cleanPath(), clock::get, TTL);
        planeA.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        final ServerlessNode a = new ServerlessNode(nodeSettings("stall-a"));
        a.startup().start();
        final BackgroundReconciler loopA = new BackgroundReconciler(a, planeA);
        loopA.want("alpha", 0);
        loopA.tick(clock.get());
        final ShardId onA = a.reconciler().openShards().iterator().next();

        armed.set(true);
        final java.util.concurrent.atomic.AtomicReference<Throwable> outcome = new java.util.concurrent.atomic.AtomicReference<>();
        final Thread writer = new Thread(() -> {
            try {
                a.writes().index(onA, "stalled", "{\"msg\":\"landed, then stalled\"}");
            } catch (Throwable t) {
                outcome.set(t);
            }
        });
        writer.start();
        try {
            assertTrue("the record must have landed", landed.await(30, java.util.concurrent.TimeUnit.SECONDS));
            // A stalls past its lease; B takes over and fences.
            clock.set(clock.get() + TTL + 1);
            final MetadataPlane planeB = new MetadataPlane(
                new FsBlobStore(1024, objectStore, false),
                BlobPath.cleanPath(),
                clock::get,
                TTL
            );
            try (ServerlessNode b = new ServerlessNode(nodeSettings("stall-b"))) {
                b.startup().start();
                final BackgroundReconciler loopB = new BackgroundReconciler(b, planeB);
                loopB.want("alpha", 0);
                loopB.tick(clock.get());
                resume.countDown();
                writer.join(30_000);
                assertNull("a write whose record landed before the fence must be acknowledged: " + outcome.get(), outcome.get());
                final ShardId onB = b.reconciler().openShards().iterator().next();
                assertTrue("and the successor must have it", b.reads().get(onB, "stalled").found());
            }
        } finally {
            resume.countDown();
            writer.join(30_000);
            a.close();
        }
    }

    /** Stalls the first log record write, once armed, right after it has landed. */
    private static final class StallAfterLogWriteBlobStore implements BlobStore {

        private final BlobStore delegate;
        private final AtomicBoolean armed;
        private final java.util.concurrent.CountDownLatch landed;
        private final java.util.concurrent.CountDownLatch resume;

        StallAfterLogWriteBlobStore(
            BlobStore delegate,
            AtomicBoolean armed,
            java.util.concurrent.CountDownLatch landed,
            java.util.concurrent.CountDownLatch resume
        ) {
            this.delegate = delegate;
            this.armed = armed;
            this.landed = landed;
            this.resume = resume;
        }

        @Override
        public void validateKey(BlobPath path, String blobName) {
            delegate.validateKey(path, blobName);
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            final BlobContainer honest = delegate.blobContainer(path);
            if (path.buildAsString().startsWith("nodelog/") == false) {
                return honest;
            }
            return new DelegatingBlobContainer(honest) {
                @Override
                public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                    throws IOException {
                    super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
                    if (blobSize > 0 && armed.compareAndSet(true, false)) {
                        landed.countDown();
                        try {
                            resume.await(60, java.util.concurrent.TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            };
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
