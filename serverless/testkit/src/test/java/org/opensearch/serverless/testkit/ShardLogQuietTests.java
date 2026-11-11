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
import org.opensearch.serverless.metadata.ShardHead;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Once a shard's head says its own write log is quiet, a node writing its node's log opens the shard without listing that
 * log; an owner that writes the shard's own log takes the head without the word, and the next owner closes that log again.
 */
public class ShardLogQuietTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 3;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, boolean nodeLog) {
        return Settings.builder()
            .put("serverless.wal.node_log.enabled", nodeLog)
            .put("node.name", name)
            .put("cluster.name", "serverless-shard-log-quiet")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static List<ShardId> takeAll(ServerlessNode node, BackgroundReconciler loop, AtomicLong clock) throws Exception {
        for (int shard = 0; shard < SHARDS; shard++) {
            loop.want("alpha", shard);
        }
        loop.tick(clock.get());
        final List<ShardId> held = new ArrayList<>(node.reconciler().openShards());
        assertEquals(SHARDS, held.size());
        return held;
    }

    private static void write(ServerlessNode node, List<ShardId> held, String prefix) throws Exception {
        for (ShardId shardId : held) {
            node.writes().index(shardId, prefix + shardId.id(), "{\"msg\":\"" + prefix + "\",\"n\":" + shardId.id() + "}");
        }
    }

    private static void assertFound(ServerlessNode node, List<ShardId> held, String prefix) throws Exception {
        for (ShardId shardId : held) {
            assertTrue(prefix + shardId.id() + " on " + shardId, node.reads().get(shardId, prefix + shardId.id()).found());
        }
    }

    private static boolean allQuiet(MetadataPlane plane) throws Exception {
        for (int shard = 0; shard < SHARDS; shard++) {
            final ShardHead head = plane.heads().read("alpha", shard).orElseThrow();
            if (head.shardLogQuiet() == false) {
                return false;
            }
        }
        return true;
    }

    /** The listings of the shards' own write logs, across a breakdown by place. */
    private static long shardLogListings(String byPlace) {
        long listings = 0;
        for (String line : byPlace.split("\n")) {
            final String trimmed = line.trim();
            if (trimmed.contains(" LIST ") && trimmed.contains("/wal")) {
                listings += Long.parseLong(trimmed.substring(0, trimmed.indexOf(' ')));
            }
        }
        return listings;
    }

    private static void releaseAll(ServerlessNode node, BackgroundReconciler loop) throws Exception {
        loop.setIdleAfterMillis(1);
        for (int shard = 0; shard < SHARDS; shard++) {
            loop.stopWanting("alpha", shard);
        }
        loop.releaseIdle(System.currentTimeMillis() + 60_000L);
        assertTrue(node.reconciler().openShards().isEmpty());
    }

    public void testATakeoverOfAQuietShardListsNothingOfItsLogAndMissesNothing() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));

        // The first owner closes the shard's log the long way, finds nothing in it, and says so in the head.
        try (ServerlessNode first = new ServerlessNode(nodeSettings("quiet-first", true))) {
            first.startup().start();
            first.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane);
            final List<ShardId> held = takeAll(first, loop, clock);
            assertTrue("a first open that replayed nothing says the shard's log is quiet", allQuiet(plane));
            write(first, held, "published-");
            loop.publishAll();
            releaseAll(first, loop);
        }
        assertTrue("a clean release keeps the word", allQuiet(plane));

        // A clean takeover: nothing of the shards' own logs is listed, fenced or read.
        final ServerlessNode second = new ServerlessNode(nodeSettings("quiet-second", true));
        try {
            second.startup().start();
            second.setMetadataPlane(plane);
            store.reset();
            final List<ShardId> held = takeAll(second, new BackgroundReconciler(second, plane), clock);
            final String where = store.byPlace();
            assertEquals("no listing of a quiet shard's own log\n" + where, 0L, shardLogListings(where));
            assertFalse("and no fence in it\n" + where, where.matches("(?s).*PUT \\S*/wal/t=.*"));
            assertFound(second, held, "published-");
            // Writes go on, through the node's log, and are acknowledged.
            write(second, held, "unpublished-");
            assertFound(second, held, "unpublished-");
            clock.addAndGet(2 * TTL);
            // The second owner dies with those writes unpublished: its node's log is what holds them.
            try (ServerlessNode third = new ServerlessNode(nodeSettings("quiet-third", true))) {
                third.startup().start();
                third.setMetadataPlane(plane);
                third.leases().renewLease(plane);
                plane.membership().refresh();
                store.reset();
                final List<ShardId> taken = takeAll(third, new BackgroundReconciler(third, plane), clock);
                final String crashed = store.byPlace();
                assertEquals("a crashed owner's shards open without listing their own logs\n" + crashed, 0L, shardLogListings(crashed));
                assertFound(third, taken, "published-");
                assertFound(third, taken, "unpublished-");
                assertTrue("and the word survives the takeover", allQuiet(plane));
            }
        } finally {
            try {
                second.close();
            } catch (Exception e) {
                // It lost every shard it held.
            }
        }
    }

    public void testAnOwnerWritingTheShardsOwnLogTakesTheHeadWithoutTheWord() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));

        try (ServerlessNode first = new ServerlessNode(nodeSettings("mixed-first", true))) {
            first.startup().start();
            first.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane);
            final List<ShardId> held = takeAll(first, loop, clock);
            write(first, held, "published-");
            loop.publishAll();
            releaseAll(first, loop);
        }
        assertTrue(allQuiet(plane));

        // An owner that writes the shard's own log, and dies with writes there it never published.
        final ServerlessNode perShard = new ServerlessNode(nodeSettings("mixed-per-shard", false));
        try {
            perShard.startup().start();
            perShard.setMetadataPlane(plane);
            final List<ShardId> held = takeAll(perShard, new BackgroundReconciler(perShard, plane), clock);
            assertFalse("taking the head drops the word", allQuiet(plane));
            write(perShard, held, "in-shard-log-");
            clock.addAndGet(2 * TTL);

            // The next owner writes its node's log, so closes the shard's own log the long way and replays it.
            try (ServerlessNode next = new ServerlessNode(nodeSettings("mixed-next", true))) {
                next.startup().start();
                next.setMetadataPlane(plane);
                next.leases().renewLease(plane);
                plane.membership().refresh();
                store.reset();
                final BackgroundReconciler loop = new BackgroundReconciler(next, plane);
                final List<ShardId> taken = takeAll(next, loop, clock);
                final String where = store.byPlace();
                assertTrue("a shard whose log is not quiet is closed by listing it\n" + where, shardLogListings(where) > 0);
                assertFound(next, taken, "published-");
                assertFound(next, taken, "in-shard-log-");
                // Quiet only once a commit at the new owner's term holds what it replayed from the shard's own log.
                for (int shard = 0; shard < SHARDS; shard++) {
                    final ShardHead head = plane.heads().read("alpha", shard).orElseThrow();
                    final long publishedTerm = plane.segmentPublisher("alpha", shard).readManifest().orElseThrow().term();
                    logger.info("after takeover: {} published at term {}", head, publishedTerm);
                    if (head.shardLogQuiet()) {
                        assertEquals("quiet only after a publish at the owner's term: " + head, head.term(), publishedTerm);
                    }
                }
                loop.publishAll();
                assertTrue("and quiet once it is published", allQuiet(plane));
            }
        } finally {
            try {
                perShard.close();
            } catch (Exception e) {
                // It lost every shard it held.
            }
        }
    }
}
