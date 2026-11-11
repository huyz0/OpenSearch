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

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/** With a write log per node, a shard is published by the age of its oldest unpublished write, or by how much it holds. */
public class NodeLogLazyPublishTests extends OpenSearchTestCase {

    private static final long TTL = 600_000L;
    private static final long MAX_AGE = 60_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-node-log-lazy")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 20)
            .build();
    }

    private static void write(ServerlessNode node, BackgroundReconciler loop, ShardId shardId, String id) throws Exception {
        node.writes().index(shardId, id, "{\"msg\":\"m\",\"n\":1}");
        loop.markDirty(shardId);
    }

    public void testAWriteIsPublishedOnceItIsOldEnoughOrTheShardHoldsEnough() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("node-log-lazy"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setLazyPublish(MAX_AGE, 5, 1L);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            write(node, loop, shardId, "first");
            assertEquals(
                "a shard this node has never published is published at once",
                Set.of(shardId),
                loop.publishDirty(clock.get(), 0L, 0L)
            );

            write(node, loop, shardId, "second");
            assertEquals("a young write waits", Set.of(), loop.publishDirty(clock.get(), 0L, 0L));
            clock.addAndGet(MAX_AGE / 2);
            assertEquals("and the backstop leaves it too", Set.of(), loop.tick(clock.get()).published());
            write(node, loop, shardId, "third");
            clock.addAndGet(MAX_AGE / 2);
            assertEquals("aged by its oldest unpublished write, not its newest", Set.of(shardId), loop.publishDirty(clock.get(), 0L, 0L));

            for (int i = 0; i < 4; i++) {
                write(node, loop, shardId, "burst-" + i);
            }
            assertEquals("below the size bound, a burst waits", Set.of(), loop.publishDirty(clock.get(), 0L, 0L));
            write(node, loop, shardId, "burst-4");
            assertEquals("at it, it is published at once", Set.of(shardId), loop.publishDirty(clock.get(), 0L, 0L));
            assertEquals("and nothing is left to publish", Set.of(), loop.tick(clock.get() + 2 * MAX_AGE).published());
        }
    }

    private static ShardId openOne(ServerlessNode node, MetadataPlane plane, BackgroundReconciler loop, AtomicLong clock, String index)
        throws Exception {
        loop.want(index, 0);
        loop.tick(clock.get());
        for (ShardId shardId : node.reconciler().openShards()) {
            if (shardId.getIndexName().equals(index)) {
                return shardId;
            }
        }
        throw new AssertionError(index + " did not open");
    }

    public void testAnIndexsRefreshIntervalIsItsPublishAgeLinedUpWithTheNodesTick() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(
            new IndexDescriptor("fresh", "uuid-fresh-00000000", 1, MAPPING, Settings.builder().put("index.refresh_interval", "5s").build())
        );
        plane.createIndex(
            new IndexDescriptor("off", "uuid-off-0000000000", 1, MAPPING, Settings.builder().put("index.refresh_interval", "-1").build())
        );
        try (ServerlessNode node = new ServerlessNode(nodeSettings("node-log-refresh"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final long tick = 250L;
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setLazyPublish(MAX_AGE, 0L, tick);
            final ShardId fresh = openOne(node, plane, loop, clock, "fresh");
            final ShardId off = openOne(node, plane, loop, clock, "off");
            write(node, loop, fresh, "first");
            write(node, loop, off, "first");
            assertEquals("each is published once at once", Set.of(fresh, off), loop.publishDirty(clock.get(), 0L, 0L));

            clock.addAndGet(100L);
            write(node, loop, fresh, "second");
            write(node, loop, off, "second");
            clock.addAndGet(5_000L);
            assertEquals("five seconds on, but the tick after them is not yet here", Set.of(), loop.publishDirty(clock.get(), 0L, 0L));
            clock.addAndGet(tick);
            assertEquals(
                "published at the node's tick after its refresh interval; refresh off waits for size or release",
                Set.of(fresh),
                loop.publishDirty(clock.get(), 0L, 0L)
            );
            clock.addAndGet(2 * MAX_AGE);
            assertEquals("not even the node's default age applies to it", Set.of(), loop.publishDirty(clock.get(), 0L, 0L));
        }
    }
}
