/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * What a node serves after it has lost, failed, or re-taken a shard: the published commit, and nothing of
 * its own that the commit does not name.
 *
 * <p>Local disk outlives a node's tenure of a shard, and the segment counter travels in the commit, so a
 * writer fenced after a local flush leaves a segment on disk with exactly the name its successor gives the
 * next segment it publishes. The tests here are the ones a directory that "prefers local" fails: not with
 * an error, but with a confident answer from the wrong incarnation.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessReaderCorrectnessTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        final Settings.Builder b = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-reader-correctness")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0");
        if (roles != null) {
            b.put("serverless.roles", roles);
        }
        return b.build();
    }

    private static MetadataPlane planeOver(BlobStore store, AtomicLong clock) {
        return new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
    }

    private static long hits(ServerlessNode node, ShardId shardId, String text) throws Exception {
        return ShardOps.hits(node.searchService(), shardId, "msg", text);
    }

    /**
     * A writer fenced after a local flush, later serving the same shard as a reader, serves the successor's
     * segment and not its own of the same name.
     *
     * <p>Both nodes' first unpublished segment after A's commit is {@code _1}: A flushed one and never
     * published it, B restored A's commit and published its own. A's local {@code _1.*} is internally
     * consistent and named by B's manifest, so a directory that preferred local files opened A's fenced
     * documents under B's commit -- no checksum is checked on open, and nothing looked wrong.
     */
    public void testAFencedWriterServingAsAReaderShowsTheSuccessorsBytesNotItsOwn() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(new FsBlobStore(1024, createTempDir(), false), clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("rc-fenced", null));
            ServerlessNode b = new ServerlessNode(nodeSettings("rc-successor", "ingest"))
        ) {
            a.start();
            b.start();

            final ShardId onA = a.activateWriter(plane, "alpha", 0).orElseThrow();
            final long termA = plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(a.reconciler().shard(onA), "1", "{\"msg\":\"shared\",\"n\":1}");
            a.reconciler().shard(onA).refresh("rc");
            a.publishShard(onA, termA);

            // A flushes a segment it will never publish: the fenced writer's leftover on local disk.
            ShardOps.indexDoc(a.reconciler().shard(onA), "2", "{\"msg\":\"fenced\",\"n\":2}");
            a.reconciler().shard(onA).flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true));

            // A's lease lapses; B takes the shard from A's published commit and publishes a segment of its
            // own -- which gets the name A's unpublished one had, because the counter came from the commit.
            clock.set(1_000L + TTL);
            final ShardId onB = b.activateWriter(plane, "alpha", 0).orElseThrow();
            final long termB = plane.heads().read("alpha", 0).orElseThrow().term();
            assertTrue("the successor must hold a higher term", termB > termA);
            ShardOps.indexDoc(b.reconciler().shard(onB), "3", "{\"msg\":\"successor\",\"n\":3}");
            b.reconciler().shard(onB).refresh("rc");
            final CommitManifest published = b.publishShard(onB, termB);
            assertTrue(
                "the fixture needs the successor to publish a segment under its own term: " + published.files(),
                published.files().values().stream().anyMatch(t -> t.equals("t=" + termB))
            );

            // A discovers it lost the shard and lets go, then serves the very same shard as a reader from
            // the very same data path.
            assertEquals("A must release the shard it lost", Set.of(onA), a.heartbeat(plane));
            final ShardId asReader = a.serveAsReader(plane, "alpha", 0);

            assertEquals("the successor's document is what a reader on the ex-writer serves", 1L, hits(a, asReader, "successor"));
            assertEquals("and the fenced writer's unpublished document is not", 0L, hits(a, asReader, "fenced"));
            assertEquals("while what both published is still there", 1L, hits(a, asReader, "shared"));
        }
    }

    /**
     * An activation that fails after core has created the shard can be retried, and the retry opens it.
     *
     * <p>Between {@code IndexService#createShard} and the shard being recorded as open there are reads of
     * the object store -- the published commit's {@code segments_N}, the log's seal -- and any of them can
     * fail. The shard was left in the service, so the next attempt was refused with "already exists" for
     * the life of the process, while the head still named this node and nobody else could take it.
     */
    public void testAnActivationThatFailsAfterTheShardIsCreatedCanBeRetried() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FlakySegmentReads store = new FlakySegmentReads(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = planeOver(store, clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode a = new ServerlessNode(nodeSettings("rc-first", "ingest"))) {
            a.start();
            final ShardId onA = a.activateWriter(plane, "alpha", 0).orElseThrow();
            for (int i = 1; i <= 3; i++) {
                ShardOps.indexDoc(a.reconciler().shard(onA), String.valueOf(i), "{\"msg\":\"durable\",\"n\":" + i + "}");
            }
            a.reconciler().shard(onA).refresh("rc");
            a.publishShard(onA, plane.heads().read("alpha", 0).orElseThrow().term());
        }

        clock.set(1_000L + TTL);
        try (ServerlessNode b = new ServerlessNode(nodeSettings("rc-retry", "ingest"))) {
            b.start();

            // The first read of the published commit fails, after the shard exists in core.
            store.failNextSegmentsRead();
            final Exception failed = expectThrows(Exception.class, () -> b.activateWriter(plane, "alpha", 0));
            assertTrue("the fixture must have failed the read it was armed for: " + failed, store.tripped());
            assertTrue("nothing is recorded open after a failed activation", b.reconciler().openShards().isEmpty());

            // The store is back. The same node, the same shard: this used to be "already exists", forever.
            final ShardId onB = b.activateWriter(plane, "alpha", 0).orElseThrow();
            assertEquals("and the retry serves the published data", 3L, hits(b, onB, "durable"));
        }
    }

    /**
     * A view taken over an index does not open over a later index created under the same name.
     *
     * <p>The new index's first segments have exactly the names the old one's had, so a view that knew its
     * index only by name would serve the new index's bytes under the old view's id -- a "frozen" view of
     * whatever is there now. The view records the uuid it froze, and an index with a different one refuses
     * it.
     */
    public void testAViewOfADeletedIndexDoesNotOpenOverTheIndexRecreatedUnderItsName() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(new FsBlobStore(1024, createTempDir(), false), clock);
        plane.createIndex(new IndexDescriptor("logs", "uuid-logs-first-0000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("rc-pit", "ingest"))) {
            node.start();
            final ShardId first = node.activateWriter(plane, "logs", 0).orElseThrow();
            ShardOps.indexDoc(node.reconciler().shard(first), "1", "{\"msg\":\"first\",\"n\":1}");
            node.reconciler().shard(first).refresh("rc");
            final CommitManifest frozen = node.publishShard(first, plane.heads().read("logs", 0).orElseThrow().term());
            final PointInTime pit = new PointInTime(
                UUIDs.randomBase64UUID(),
                "logs",
                "uuid-logs-first-0000",
                clock.get() + 600_000L,
                Map.of(0, frozen)
            );
            plane.createPointInTime(pit);

            // The name is deleted and created again. Its first published segment has the same name.
            assertTrue(plane.deleteIndex("logs"));
            node.heartbeat(plane);
            plane.createIndex(new IndexDescriptor("logs", "uuid-logs-second-000", 1, MAPPING, null));
            final ShardId second = node.activateWriter(plane, "logs", 0).orElseThrow();
            ShardOps.indexDoc(node.reconciler().shard(second), "1", "{\"msg\":\"second\",\"n\":1}");
            node.reconciler().shard(second).refresh("rc");
            node.publishShard(second, plane.heads().read("logs", 0).orElseThrow().term());

            final IndexDescriptor recreated = plane.describe("logs").orElseThrow();
            assertEquals("uuid-logs-second-000", recreated.uuid());
            final IndexMetadata metadata = recreated.toIndexMetadata(Map.of(0, frozen.term()));
            final DiscoveryNodes nodes = DiscoveryNodes.builder().add(node.localNode()).localNodeId(node.localNode().getId()).build();

            final IllegalArgumentException refused = expectThrows(
                IllegalArgumentException.class,
                () -> node.reconciler().openFrozenReader(metadata, pit, 0, nodes)
            );
            assertTrue("the refusal must say why: " + refused.getMessage(), refused.getMessage().contains("no longer held"));
            assertTrue("and nothing was opened under the view's identity", node.reconciler().frozenShards().isEmpty());
        }
    }

    /**
     * Closing a frozen view deletes what it left on disk.
     *
     * <p>A view is opened under a synthetic index uuid, so its shard's index and translog live in a
     * directory nothing else shares and nothing will ever open again. Closing used to keep it, so every
     * point in time ever taken cost a directory for the life of the node's disk.
     */
    public void testClosingAViewDeletesWhatItLeftOnDisk() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(new FsBlobStore(1024, createTempDir(), false), clock);
        plane.createIndex(new IndexDescriptor("paged", "uuid-paged-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("rc-view-disk", "ingest"))) {
            node.start();
            final ShardId live = node.activateWriter(plane, "paged", 0).orElseThrow();
            ShardOps.indexDoc(node.reconciler().shard(live), "1", "{\"msg\":\"paged\",\"n\":1}");
            node.reconciler().shard(live).refresh("rc");
            final CommitManifest frozen = node.publishShard(live, plane.heads().read("paged", 0).orElseThrow().term());
            final PointInTime pit = new PointInTime(
                UUIDs.randomBase64UUID(),
                "paged",
                "uuid-paged-00000000",
                clock.get() + 600_000L,
                Map.of(0, frozen)
            );
            plane.createPointInTime(pit);

            final IndexDescriptor descriptor = plane.describe("paged").orElseThrow();
            final IndexMetadata metadata = descriptor.toIndexMetadata(Map.of(0, frozen.term()));
            final DiscoveryNodes nodes = DiscoveryNodes.builder().add(node.localNode()).localNodeId(node.localNode().getId()).build();
            final ShardId view = node.reconciler().openFrozenReader(metadata, pit, 0, nodes);
            assertEquals("the view serves what it froze", 1L, hits(node, view, "paged"));

            final Path onDisk = node.reconciler().shard(view).shardPath().getDataPath();
            final Path liveOnDisk = node.reconciler().shard(live).shardPath().getDataPath();
            assertTrue("the fixture needs the view on disk: " + onDisk, Files.isDirectory(onDisk));

            assertEquals(1, node.reconciler().closeFrozenReader(pit.id()));
            assertFalse("a closed view's directory must not be left behind: " + onDisk, Files.exists(onDisk));
            assertTrue("while the live shard's is untouched", Files.isDirectory(liveOnDisk));
            assertEquals("and the live shard still serves", 1L, hits(node, live, "paged"));
        }
    }

    /**
     * A store whose next ranged read of a {@code segments_N} blob fails, once.
     *
     * <p>That read is the first thing a node does with the published commit after core has created the
     * shard, so it is the failure that used to leave the shard stranded in the {@code IndexService}.
     */
    private static final class FlakySegmentReads implements BlobStore {

        private final BlobStore delegate;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicBoolean tripped = new AtomicBoolean();

        FlakySegmentReads(BlobStore delegate) {
            this.delegate = delegate;
        }

        void failNextSegmentsRead() {
            armed.set(true);
        }

        boolean tripped() {
            return tripped.get();
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new Flaky(delegate.blobContainer(path));
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private final class Flaky extends DelegatingBlobContainer {

            Flaky(BlobContainer inner) {
                super(inner);
            }

            @Override
            public InputStream readBlob(String blobName, long position, long length) throws IOException {
                if (blobName.startsWith("segments_") && armed.compareAndSet(true, false)) {
                    tripped.set(true);
                    throw new IOException("the object store answered 503 for " + blobName);
                }
                return super.readBlob(blobName, position, length);
            }

            @Override
            public Map<String, BlobContainer> children() throws IOException {
                return super.children().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new Flaky(e.getValue())));
            }
        }
    }
}
