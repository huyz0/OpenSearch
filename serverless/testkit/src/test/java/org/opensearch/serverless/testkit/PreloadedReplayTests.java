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
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.Document;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.shell.WriteOutcome;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Records handed to a shard's next writer open are replayed with their own sequence numbers, the numbers between them
 * become no-ops, and the open publishes them before it serves.
 */
public class PreloadedReplayTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-preloaded-replay")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .build();
    }

    private static ShardId take(ServerlessNode node, MetadataPlane plane, AtomicLong clock) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        return node.reconciler().openShards().iterator().next();
    }

    public void testPreloadedRecordsKeepTheirSequenceNumbersAndArePublishedAtOpen() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        final long checkpoint;
        final ShardId shardId;
        try (ServerlessNode first = new ServerlessNode(nodeSettings("preload-first"))) {
            first.startup().start();
            first.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            shardId = first.reconciler().openShards().iterator().next();
            WriteOutcome last = null;
            for (int i = 0; i < 3; i++) {
                last = first.writes().index(shardId, "base-" + i, "{\"msg\":\"base\",\"n\":" + i + "}");
            }
            loop.publishAll();
            checkpoint = last.seqNo();
            loop.setIdleAfterMillis(1);
            loop.stopWanting("alpha", 0);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
        }

        // Every other sequence number past the commit, as one half of a split would see its parent's tail.
        final List<WalRecord> half = List.of(
            new WalRecord("tail-a", "{\"msg\":\"tail\",\"n\":10}", checkpoint + 2, 1L, 1L),
            new WalRecord("tail-b", "{\"msg\":\"tail\",\"n\":11}", checkpoint + 4, 1L, 1L),
            WalRecord.deletion("base-0", checkpoint + 6, 1L, 2L)
        );
        try (ServerlessNode second = new ServerlessNode(nodeSettings("preload-second"))) {
            second.startup().start();
            second.setMetadataPlane(plane);
            second.reconciler().preloadReplay(shardId, half);
            expectThrows(IllegalStateException.class, () -> second.reconciler().preloadReplay(shardId, half));
            assertEquals(shardId, take(second, plane, clock));

            final Document a = second.reads().get(shardId, "tail-a");
            assertTrue(a.found());
            assertEquals("a replayed record keeps its sequence number", checkpoint + 2, a.seqNo());
            assertEquals(checkpoint + 4, second.reads().get(shardId, "tail-b").seqNo());
            assertFalse("a replayed deletion applies", second.reads().get(shardId, "base-0").found());
            assertTrue(second.reads().get(shardId, "base-1").found());

            final SeqNoStats stats = second.reconciler().shard(shardId).seqNoStats();
            assertEquals(checkpoint + 6, stats.getMaxSeqNo());
            assertEquals(
                "the numbers between them are filled, so the checkpoint reaches the top",
                stats.getMaxSeqNo(),
                stats.getLocalCheckpoint()
            );

            final long term = plane.heads().read("alpha", 0).orElseThrow().term();
            assertEquals(
                "published at the open's term before it served",
                term,
                plane.segmentPublisher("alpha", 0).readManifest().orElseThrow().term()
            );
            final WriteOutcome next = second.writes().index(shardId, "after", "{\"msg\":\"after\",\"n\":12}");
            assertEquals("a write after it takes the next number", checkpoint + 7, next.seqNo());
        }

        // Handed in for one open only: the next one replays nothing extra.
        clock.addAndGet(2 * TTL);
        try (ServerlessNode third = new ServerlessNode(nodeSettings("preload-third"))) {
            third.startup().start();
            third.setMetadataPlane(plane);
            third.leases().renewLease(plane);
            plane.membership().refresh();
            take(third, plane, clock);
            assertTrue("what the open published survives the owner", third.reads().get(shardId, "tail-b").found());
            assertTrue(third.reads().get(shardId, "after").found());
            assertFalse(third.reads().get(shardId, "base-0").found());
        }
    }
}
