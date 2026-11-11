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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What taking a shard costs in object-store requests, counted on a filesystem store by the request classes S3 bills.
 *
 * <p>Two cases: a shard its writer let go of cleanly, and a shard whose writer crashed with writes it never published.
 * Both are what a node pays per shard when a fleet wakes up or a node dies, so a request read twice in one activation is
 * paid millions of times a day.
 */
public class TakeoverRequestCountTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 3;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-takeover-requests")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static List<ShardId> holdAndWrite(ServerlessNode node, BackgroundReconciler loop, AtomicLong clock) throws Exception {
        for (int shard = 0; shard < SHARDS; shard++) {
            loop.want("alpha", shard);
        }
        loop.tick(clock.get());
        final List<ShardId> held = new ArrayList<>(node.reconciler().openShards());
        assertEquals(SHARDS, held.size());
        for (int i = 0; i < 30; i++) {
            node.index(held.get(i % SHARDS), "first-" + i, "{\"msg\":\"first\",\"n\":" + i + "}");
        }
        loop.publishAll();
        return held;
    }

    private long takeAll(String label, CountingBlobStore store, ServerlessNode node, MetadataPlane plane, AtomicLong clock)
        throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < SHARDS; shard++) {
            loop.want("alpha", shard);
        }
        store.reset();
        loop.tick(clock.get());
        final long requests = store.impliedS3Requests();
        assertEquals(SHARDS, node.reconciler().openShards().size());
        logger.info(
            "takeover requests [{}]: {} for {} shards ({} a shard)\n{}\nwhere\n{}\nby caller\n{}",
            label,
            requests,
            SHARDS,
            requests / (double) SHARDS,
            store.breakdown(),
            store.byPlace(),
            store.byCaller()
        );
        return requests;
    }

    private static long rollupSwaps(String byPlace) {
        return count(byPlace, "REGISTER_CAS rollups/");
    }

    /** The requests of the places a breakdown line names with this prefix. */
    private static long count(String byPlace, String kindAndPlace) {
        long requests = 0;
        for (String line : byPlace.split("\n")) {
            if (line.contains(kindAndPlace)) {
                requests += Long.parseLong(line.substring(0, line.indexOf(' ')));
            }
        }
        return requests;
    }

    private CountingBlobStore store() throws java.io.IOException {
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false));
        if (Boolean.parseBoolean(System.getProperty("tests.takeover.trace", "false"))) {
            store.traceCallers(Integer.parseInt(System.getProperty("tests.takeover.trace.depth", "4")));
        }
        return store;
    }

    public void testTakingShardsLetGoOfCleanly() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final CountingBlobStore store = store();
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        try (ServerlessNode first = new ServerlessNode(nodeSettings("released-first"))) {
            first.start();
            first.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane);
            holdAndWrite(first, loop, clock);
            loop.setIdleAfterMillis(1);
            for (int shard = 0; shard < SHARDS; shard++) {
                loop.stopWanting("alpha", shard);
            }
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
        }
        try (ServerlessNode second = new ServerlessNode(nodeSettings("released-second"))) {
            second.start();
            second.setMetadataPlane(plane);
            takeAll("released", store, second, plane, clock);
            final String where = store.byPlace();
            // The commit it was opened from already holds everything: nothing is published again, so no new commit,
            // no manifest swap, no log trim and no sweep.
            assertFalse("an untouched shard is not published again\n" + where, where.matches("(?s).*PUT \\S*segments_#.*"));
            assertFalse("nor is its manifest swapped\n" + where, where.contains("REGISTER_CAS segments/"));
            // Marks of shards of one index share a register, and are written together.
            assertTrue("a swap of the rollup per shard at most\n" + where, rollupSwaps(where) <= SHARDS);
        }
    }

    public void testTakingACrashedWritersShards() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final CountingBlobStore store = store();
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        final ServerlessNode first = new ServerlessNode(nodeSettings("crashed-first"));
        try {
            first.start();
            first.setMetadataPlane(plane);
            final List<ShardId> held = holdAndWrite(first, new BackgroundReconciler(first, plane), clock);
            for (ShardId shardId : held) {
                first.index(shardId, "unpublished-" + shardId.id(), "{\"msg\":\"first\",\"n\":" + shardId.id() + "}");
            }
            clock.addAndGet(2 * TTL);
            try (ServerlessNode survivor = new ServerlessNode(nodeSettings("crashed-survivor"))) {
                survivor.start();
                survivor.setMetadataPlane(plane);
                survivor.renewLease(plane);
                plane.membership().refresh();
                takeAll("crashed", store, survivor, plane, clock);
                // The dead writer's lease is judged and revoked once for all its shards taken together, not read
                // twice per shard and swapped by each.
                final String where = store.byPlace();
                assertTrue(
                    "the dead lease is read less than once a shard\n" + where,
                    count(where, "REGISTER_GET cluster/members/") < SHARDS
                );
                assertTrue("and revoked once\n" + where, count(where, "REGISTER_CAS cluster/members/") <= 2);
                for (ShardId shardId : survivor.reconciler().openShards()) {
                    assertTrue(survivor.get(shardId, "unpublished-" + shardId.id()).found());
                }
            }
        } finally {
            try {
                first.close();
            } catch (Exception e) {
                // It lost every shard it held.
            }
        }
    }
}
