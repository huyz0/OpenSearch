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
import org.opensearch.index.shard.IndexShard;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 5: the search path. Reader shards over object-store segments, owning nothing.
 *
 * <p>Reader activation is the half of the design that needs no coordination at all — no
 * compare-and-swap, no shard-head entry, no agreement. Two search nodes serving the same shard cannot
 * conflict, because neither can write: a node that does not accept writer activation runs
 * {@code ReadOnlyEngine}, so the guarantee is structural rather than conventional.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only. No durability claim for S3 or GCS until R11 runs.
 */
public class ServerlessSearchPathTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-p5")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0");
        if (roles != null) {
            builder.put("serverless.roles", roles);
        }
        return builder.build();
    }

    private MetadataPlane planeOver(Path dir, AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    /** Indexes and publishes some documents from a writer node, then closes it. */
    private void writeAndPublish(MetadataPlane plane, Path home, int docs) throws Exception {
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("p5-writer", "ingest"))) {
            writer.start();
            final long term = plane.activate("alpha", 0, writer.localNode().getId(), writer.localNode().getEphemeralId()).head().term();
            final ShardId shardId = writer.syncFrom(plane).iterator().next();
            for (int i = 1; i <= docs; i++) {
                ShardOps.indexDoc(writer.reconciler().shard(shardId), String.valueOf(i), "{\"msg\":\"searchable\",\"n\":" + i + "}");
            }
            writer.reconciler().shard(shardId).refresh("p5");
            writer.publishShard(shardId, term);
        }
    }

    public void testASearchNodeServesPublishedSegmentsWithoutOwningTheShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        writeAndPublish(plane, null, 4);

        final String ownerBefore = plane.heads().read("alpha", 0).orElseThrow().ownerNodeId();
        final long termBefore = plane.heads().read("alpha", 0).orElseThrow().term();

        try (ServerlessNode reader = new ServerlessNode(nodeSettings("p5-reader", "search"))) {
            reader.start();
            final ShardId shardId = reader.serveAsReader(plane, "alpha", 0);

            assertEquals("the reader served nothing", 4L, ShardOps.hits(reader.searchService(), shardId, "msg", "searchable"));
            assertTrue("the shard must be recorded as a reader", reader.reconciler().readerShards().contains(shardId));

            // Reader activation touches no register: the head is exactly as the writer left it.
            assertEquals("a reader took ownership of the shard", ownerBefore, plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
            assertEquals("a reader bumped the term", termBefore, plane.heads().read("alpha", 0).orElseThrow().term());
        }
    }

    public void testAReaderCannotWrite() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        writeAndPublish(plane, null, 2);

        try (ServerlessNode reader = new ServerlessNode(nodeSettings("p5-ro", "search"))) {
            reader.start();
            final ShardId shardId = reader.serveAsReader(plane, "alpha", 0);
            final IndexShard shard = reader.reconciler().shard(shardId);

            // Structural, not conventional: the engine is ReadOnlyEngine because of the node's role.
            // It refuses with `assert false` and then UnsupportedOperationException, so which one
            // surfaces depends on whether assertions are enabled. Both are a refusal; catching only
            // Exception would pass under -ea for the wrong reason and fail without it.
            final Throwable refusal = expectThrows(Throwable.class, () -> ShardOps.indexDoc(shard, "99", "{\"msg\":\"nope\",\"n\":99}"));
            assertTrue(
                "a reader accepted a write, or failed for an unrelated reason: " + refusal,
                refusal instanceof AssertionError || refusal instanceof UnsupportedOperationException
            );

            // And publishing is refused early, with a reason rather than a Lucene-level complaint.
            final Exception refused = expectThrows(Exception.class, () -> reader.publishShard(shardId, 1L));
            assertTrue("the refusal must explain itself: " + refused.getMessage(), refused.getMessage().contains("owns no head"));
        }
    }

    /** Scale to zero: every search node dies, and a brand-new one serves the same data. */
    public void testScaleToZeroAndBack() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        writeAndPublish(plane, null, 5);

        // A search node serves, then goes away entirely.
        try (ServerlessNode first = new ServerlessNode(nodeSettings("p5-z1", "search"))) {
            first.start();
            final ShardId shardId = first.serveAsReader(plane, "alpha", 0);
            assertEquals(5L, ShardOps.hits(first.searchService(), shardId, "msg", "searchable"));
        }

        // Zero search nodes now exist. Nothing was handed over and nothing was drained.

        // A different node, with its own empty disk, serves the same data.
        try (ServerlessNode second = new ServerlessNode(nodeSettings("p5-z2", "search"))) {
            second.start();
            final ShardId shardId = second.serveAsReader(plane, "alpha", 0);
            assertEquals(
                "scale-to-zero lost the data: a fresh search node served nothing",
                5L,
                ShardOps.hits(second.searchService(), shardId, "msg", "searchable")
            );
        }
    }

    /** Readers are interchangeable, so two may serve one shard at once with no coordination. */
    public void testTwoReadersServeTheSameShardConcurrently() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        writeAndPublish(plane, null, 3);

        try (
            ServerlessNode r1 = new ServerlessNode(nodeSettings("p5-m1", "search"));
            ServerlessNode r2 = new ServerlessNode(nodeSettings("p5-m2", "search"))
        ) {
            r1.start();
            r2.start();
            final ShardId s1 = r1.serveAsReader(plane, "alpha", 0);
            final ShardId s2 = r2.serveAsReader(plane, "alpha", 0);

            assertEquals(3L, ShardOps.hits(r1.searchService(), s1, "msg", "searchable"));
            assertEquals(3L, ShardOps.hits(r2.searchService(), s2, "msg", "searchable"));

            // Neither took the head, so neither could have evicted the other.
            assertEquals("a reader took ownership", "p5-writer", nameOfOwner(plane));
        }
    }

    private String nameOfOwner(MetadataPlane plane) throws Exception {
        // The writer node is gone, but the head still records the id it held.
        return plane.heads().read("alpha", 0).orElseThrow().ownerNodeId() == null ? null : "p5-writer";
    }

    /** Nothing published means the reader refuses, rather than serving a confident empty result. */
    public void testAReaderRefusesAShardWithNoPublishedCommit() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode reader = new ServerlessNode(nodeSettings("p5-empty", "search"))) {
            reader.start();
            final Exception e = expectThrows(Exception.class, () -> reader.serveAsReader(plane, "alpha", 0));
            assertTrue(
                "the refusal must say why an empty answer would be wrong: " + e.getMessage(),
                e.getMessage().contains("indistinguishable from an empty index")
            );
            assertTrue(reader.reconciler().openShards().isEmpty());
        }
    }

    /** A reader opened before newer data was published sees it after re-opening. */
    public void testAReaderPicksUpALaterCommitWhenReopened() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        writeAndPublish(plane, null, 2);

        try (ServerlessNode reader = new ServerlessNode(nodeSettings("p5-refresh", "search"))) {
            reader.start();
            final ShardId shardId = reader.serveAsReader(plane, "alpha", 0);
            assertEquals(2L, ShardOps.hits(reader.searchService(), shardId, "msg", "searchable"));

            // A new writer takes the shard over -- the old one's lease lapsed -- and publishes more.
            clock.set(1_000L + TTL);
            try (ServerlessNode writer2 = new ServerlessNode(nodeSettings("p5-writer2", "ingest"))) {
                writer2.start();
                final long term = plane.activate("alpha", 0, writer2.localNode().getId(), writer2.localNode().getEphemeralId())
                    .head()
                    .term();
                final ShardId onWriter = writer2.syncFrom(plane).iterator().next();
                ShardOps.indexDoc(writer2.reconciler().shard(onWriter), "3", "{\"msg\":\"searchable\",\"n\":3}");
                writer2.reconciler().shard(onWriter).refresh("p5");
                writer2.publishShard(onWriter, term);
            }

            // The already-open reader still serves the commit it opened -- it is a cache, not a follower.
            assertEquals(
                "an open reader must not silently change underneath a query",
                2L,
                ShardOps.hits(reader.searchService(), shardId, "msg", "searchable")
            );

            // Re-opening picks up the newer commit. Phase 8's notification is what makes this automatic.
            reader.releaseShard(shardId, "reopening onto a newer commit");
            final ShardId reopened = reader.serveAsReader(plane, "alpha", 0);
            assertEquals(
                "a reopened reader did not see the newer commit",
                3L,
                ShardOps.hits(reader.searchService(), reopened, "msg", "searchable")
            );
        }
    }
}
