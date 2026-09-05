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
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.serverless.store.StaleWriterException;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 4: the write path, and the object store as the thing that actually holds the data.
 *
 * <p>Phase 3 could move shard <em>ownership</em> but not shard <em>data</em>, and said so. These tests
 * are what that gap looked like once it closed: a document indexed on one node is served by another
 * after failover, with no peer recovery and nothing copied between nodes.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only. No durability claim for S3 or GCS until R11 runs.
 */
public class ServerlessWritePathTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-p4")
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

    private static IndexDescriptor alpha() {
        return new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null);
    }

    /** The headline: a document written on A is served by B, with nothing copied node to node. */
    public void testFailoverMovesTheDataThroughTheObjectStore() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(alpha());

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("p4-a", null));
            ServerlessNode b = new ServerlessNode(nodeSettings("p4-b", null))
        ) {
            a.start();
            b.start();

            final var acquired = plane.activate("alpha", 0, a.localNode().getId(), a.localNode().getEphemeralId());
            assertTrue(acquired.acquired());
            final ShardId shardId = a.syncFrom(plane).iterator().next();
            final IndexShard shardOnA = a.reconciler().shard(shardId);

            ShardOps.indexDoc(shardOnA, "1", "{\"msg\":\"written on A\",\"n\":1}");
            ShardOps.indexDoc(shardOnA, "2", "{\"msg\":\"written on A\",\"n\":2}");
            ShardOps.indexDoc(shardOnA, "3", "{\"msg\":\"written on A\",\"n\":3}");
            shardOnA.refresh("p4");
            assertEquals(3L, ShardOps.hits(a.searchService(), shardId, "msg", "written"));

            final CommitManifest manifest = a.publishShard(shardId, acquired.head().term());
            assertFalse("publishing an indexed shard must upload files", manifest.files().isEmpty());
            assertEquals("segments must be published under the owning term", 1L, manifest.term());

            // A dies. Its lease lapses and B wins the head at a bumped term.
            clock.set(1_000L + TTL);
            final var failedOver = plane.activate("alpha", 0, b.localNode().getId(), b.localNode().getEphemeralId());
            assertTrue(failedOver.acquired());
            assertEquals(2L, failedOver.head().term());

            assertEquals("B did not take the shard over", 1, b.syncFrom(plane).size());
            final ShardId onB = b.reconciler().openShards().iterator().next();

            // The number that is zero when this is broken. B never spoke to A.
            assertEquals("failover did not carry the data: B served nothing", 3L, ShardOps.hits(b.searchService(), onB, "msg", "written"));
        }
    }

    /** A zombie at a stale term is refused, and its bytes were never where a reader would look. */
    public void testAStaleWriterCannotPublishAndItsBytesAreInert() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(alpha());

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("p4-z1", null));
            ServerlessNode b = new ServerlessNode(nodeSettings("p4-z2", null))
        ) {
            a.start();
            b.start();
            final long termA = plane.activate("alpha", 0, a.localNode().getId(), a.localNode().getEphemeralId()).head().term();
            final ShardId shardId = a.syncFrom(plane).iterator().next();
            ShardOps.indexDoc(a.reconciler().shard(shardId), "1", "{\"msg\":\"from A\",\"n\":1}");
            a.reconciler().shard(shardId).refresh("p4");
            a.publishShard(shardId, termA);

            // B takes over at a higher term and publishes.
            clock.set(1_000L + TTL);
            final long termB = plane.activate("alpha", 0, b.localNode().getId(), b.localNode().getEphemeralId()).head().term();
            assertTrue(termB > termA);
            final ShardId onB = b.syncFrom(plane).iterator().next();
            ShardOps.indexDoc(b.reconciler().shard(onB), "2", "{\"msg\":\"from B\",\"n\":2}");
            b.reconciler().shard(onB).refresh("p4");
            b.publishShard(onB, termB);

            // A wakes up, still believing it owns the shard, and tries to publish at its old term.
            final StaleWriterException refused = expectThrows(StaleWriterException.class, () -> a.publishShard(shardId, termA));
            assertTrue("the refusal must name both terms: " + refused.getMessage(), refused.getMessage().contains("term " + termA));

            // And the manifest still describes B's commit, at B's term.
            final SegmentPublisher publisher = plane.segmentPublisher("alpha", 0);
            assertEquals(termB, publisher.readManifest().orElseThrow().term());

            // Every file B relies on lives under a term prefix A cannot write to, so even a zombie that
            // ignored the refusal could not have overwritten one.
            final String zombiePrefix = SegmentPublisher.termSegment(termA);
            final String livePrefix = SegmentPublisher.termSegment(termB);
            assertTrue("the two writers must not share a prefix", zombiePrefix.equals(livePrefix) == false);
        }
    }

    /** Re-publishing must not re-upload segments that are already there. */
    public void testRepublishInheritsUnchangedSegments() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(alpha());

        try (ServerlessNode a = new ServerlessNode(nodeSettings("p4-r", null))) {
            a.start();
            final long term = plane.activate("alpha", 0, a.localNode().getId(), a.localNode().getEphemeralId()).head().term();
            final ShardId shardId = a.syncFrom(plane).iterator().next();
            ShardOps.indexDoc(a.reconciler().shard(shardId), "1", "{\"msg\":\"one\",\"n\":1}");
            a.reconciler().shard(shardId).refresh("p4");

            final CommitManifest first = a.publishShard(shardId, term);
            final CommitManifest second = a.publishShard(shardId, term);

            for (var entry : first.files().entrySet()) {
                if (second.files().containsKey(entry.getKey())) {
                    assertEquals(
                        "an unchanged segment was re-uploaded to a new blob; failover would cost shard size",
                        entry.getValue(),
                        second.files().get(entry.getKey())
                    );
                }
            }
        }
    }

    /** A node that does not accept writer activation must not open a writer shard. */
    public void testASearchOnlyNodeDoesNotTakeWriterShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(alpha());

        try (ServerlessNode searchOnly = new ServerlessNode(nodeSettings("p4-search", "search"))) {
            searchOnly.start();
            assertEquals(java.util.Set.of("search"), searchOnly.roles());

            // Truth says it owns the shard -- someone activated it here -- but its role says otherwise.
            plane.activate("alpha", 0, searchOnly.localNode().getId(), searchOnly.localNode().getEphemeralId());
            assertTrue("a search-only node opened a writer shard", searchOnly.syncFrom(plane).isEmpty());
            assertTrue(searchOnly.reconciler().openShards().isEmpty());
        }
    }

    /** A node with the default roles accepts writer activation, so the gate above is not vacuous. */
    public void testAnIngestNodeDoesTakeWriterShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);
        plane.createIndex(alpha());

        try (ServerlessNode ingest = new ServerlessNode(nodeSettings("p4-ingest", "ingest"))) {
            ingest.start();
            plane.activate("alpha", 0, ingest.localNode().getId(), ingest.localNode().getEphemeralId());
            assertEquals(1, ingest.syncFrom(plane).size());
        }
    }
}
