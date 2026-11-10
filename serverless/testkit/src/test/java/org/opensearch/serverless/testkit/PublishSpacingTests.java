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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What spacing out a shard's publishes saves, counted in store requests per acknowledged write.
 *
 * <p>A write costs one log append. A publish costs a flush, one upload per new segment file and a manifest swap,
 * which for a commit adding one small segment is several PUTs. A shard written about once a second and published
 * on every debounce window therefore pays for a publish per write, and that, not the log, is where most of the PUTs
 * per acknowledged write come from. These tests pin the saving and the two rules that keep it safe: a shard past
 * its unpublished-operation limit is published early, and a deferred shard is published once its interval passes
 * even if nothing else is written to it.
 *
 * <p>Counted on a filesystem store through {@link CountingBlobStore}, whose request classes are the ones S3 bills.
 * The clock is the plane's and is advanced by hand, so the numbers do not depend on how fast the machine is.
 */
public class PublishSpacingTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-publish-spacing")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** What one run of the write pattern cost. */
    private record Cost(long writes, long puts, long requests, String breakdown) {
        double putsPerWrite() {
            return puts / (double) writes;
        }
    }

    /**
     * Writes one document a second to each of {@code shards} shards for {@code seconds} seconds, running a
     * publish pass after every second the way the scheduler's debounce does, then lets every shard publish.
     */
    private Cost writeOncePerSecond(String label, int shards, int seconds, long minIntervalMillis) throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final AtomicLong clock = new AtomicLong(1_000L);
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));
            try (ServerlessNode node = new ServerlessNode(nodeSettings("publish-spacing-" + label))) {
                node.start();
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
                for (int shard = 0; shard < shards; shard++) {
                    loop.want("alpha", shard);
                }
                loop.tick(clock.get());
                final List<ShardId> held = new ArrayList<>(node.reconciler().openShards());
                assertEquals("every shard must be held before measuring", shards, held.size());

                store.reset();
                long writes = 0;
                for (int second = 0; second < seconds; second++) {
                    for (ShardId shardId : held) {
                        node.index(shardId, second + "-" + shardId.id(), "{\"msg\":\"spacing\",\"n\":" + second + "}");
                        loop.markDirty(shardId);
                        writes++;
                    }
                    clock.addAndGet(1_000L);
                    loop.publishDirty(clock.get(), minIntervalMillis, 0L);
                }
                // The tail: whatever the last interval deferred is published once it is due, so both runs end with
                // every acknowledged write in a published commit and the comparison is like for like.
                clock.addAndGet(Math.max(1L, minIntervalMillis));
                loop.publishDirty(clock.get(), minIntervalMillis, 0L);
                assertFalse("nothing may be left unpublished", loop.hasDirty());
                for (ShardId shardId : held) {
                    assertEquals(
                        "every write to " + shardId + " must be in its published commit",
                        seconds - 1L,
                        loop.lastPublishedMaxSeqNo(shardId).orElse(-1L)
                    );
                }

                final long puts = store.blobWrites() + store.registerWrites();
                final Cost cost = new Cost(writes, puts, store.impliedS3Requests(), store.breakdown());
                logger.info(
                    "publish spacing [{} min interval {}ms]: {} writes cost {} PUTs ({} per write), {} requests  [{}]",
                    label,
                    minIntervalMillis,
                    writes,
                    puts,
                    String.format(Locale.ROOT, "%.2f", cost.putsPerWrite()),
                    cost.requests(),
                    cost.breakdown()
                );
                logger.info("publish spacing [{}]: where the requests went\n{}", label, store.byPlace());
                return cost;
            }
        }
    }

    /**
     * The saving itself. Publishing every pass is the behaviour a 500 ms debounce gives a shard written about once
     * a second; a five-second interval is the default a bootstrapped node runs with.
     */
    public void testSpacedPublishesCutPutsPerAcknowledgedWrite() throws Exception {
        final int shards = 4;
        final int seconds = 20;
        final Cost everyPass = writeOncePerSecond("every-pass", shards, seconds, 0L);
        final Cost spaced = writeOncePerSecond("spaced", shards, seconds, ReconcileScheduler.DEFAULT_PUBLISH_MIN_INTERVAL.millis());
        logger.info(
            "publish spacing: {} PUTs per write publishing every pass, {} with a {} minimum interval",
            String.format(Locale.ROOT, "%.2f", everyPass.putsPerWrite()),
            String.format(Locale.ROOT, "%.2f", spaced.putsPerWrite()),
            ReconcileScheduler.DEFAULT_PUBLISH_MIN_INTERVAL
        );
        // Every write still pays its own log append, so neither run can go below one PUT per write.
        assertTrue("a write costs at least its log append: " + spaced.putsPerWrite(), spaced.putsPerWrite() >= 1.0);
        // Publishing every pass costs a publish per write on top of the append. Spaced, the publish is shared by the
        // writes of one interval, so the total must fall by at least half; on this pattern it falls by more.
        assertTrue(
            "spacing publishes must at least halve PUTs per write: every pass "
                + everyPass.putsPerWrite()
                + ", spaced "
                + spaced.putsPerWrite(),
            spaced.putsPerWrite() * 2 <= everyPass.putsPerWrite()
        );
    }

    /** A shard written faster than the interval is published as soon as it holds the limit of unpublished operations. */
    public void testAShardPastItsUnpublishedOperationLimitIsPublishedEarly() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("publish-spacing-limit"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            final long interval = TimeValue.timeValueHours(1).millis();
            final long limit = 5;

            node.index(shardId, "first", "{\"msg\":\"limit\",\"n\":0}");
            loop.markDirty(shardId);
            assertEquals("a shard this node never published is due at once", 1, loop.publishDirty(clock.get(), interval, limit).size());

            for (int i = 1; i < limit; i++) {
                node.index(shardId, "under-" + i, "{\"msg\":\"limit\",\"n\":" + i + "}");
                loop.markDirty(shardId);
                assertTrue(
                    "under the limit and inside the interval, nothing is due",
                    loop.publishDirty(clock.get(), interval, limit).isEmpty()
                );
                assertTrue("and the shard keeps its mark", loop.hasDirty());
            }
            node.index(shardId, "at-limit", "{\"msg\":\"limit\",\"n\":" + limit + "}");
            loop.markDirty(shardId);
            assertTrue(loop.hasDirtyDue(clock.get(), interval, limit));
            assertEquals(
                "at the limit the shard is published inside its interval",
                1,
                loop.publishDirty(clock.get(), interval, limit).size()
            );
            assertEquals(limit, loop.lastPublishedMaxSeqNo(shardId).orElse(-1L));
            assertFalse(loop.hasDirty());
        }
    }

    /**
     * A shard deferred by its interval is published once the interval passes, through the scheduler, with nothing
     * else written to it. Without this a last write before a quiet period would wait for the backstop.
     */
    public void testADeferredShardIsPublishedOnceItsIntervalPasses() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("publish-spacing-deferred"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            final TimeValue interval = TimeValue.timeValueSeconds(5);
            try (
                ReconcileScheduler scheduler = new ReconcileScheduler(
                    loop,
                    node.threadPool(),
                    clock::get,
                    TimeValue.timeValueHours(1),   // no renewal during the test
                    TimeValue.timeValueHours(1),   // passes run only when the test calls them
                    null
                ).setPublishSpacing(interval, 0L)
            ) {
                node.setSignals(scheduler);
                node.index(shardId, "1", "{\"msg\":\"deferred\",\"n\":1}");
                assertEquals("the first write after a quiet period publishes at once", Set.of(shardId), scheduler.publishNow());

                node.index(shardId, "2", "{\"msg\":\"deferred\",\"n\":2}");
                clock.addAndGet(interval.millis() - 1);
                assertTrue("inside the interval the second write waits", scheduler.publishNow().isEmpty());
                assertEquals(0L, loop.lastPublishedMaxSeqNo(shardId).orElse(-1L));

                clock.addAndGet(1);
                assertEquals("once the interval has passed it is published", Set.of(shardId), scheduler.publishNow());
                assertEquals(1L, loop.lastPublishedMaxSeqNo(shardId).orElse(-1L));
                assertFalse(loop.hasDirty());
            }
        }
    }
}
