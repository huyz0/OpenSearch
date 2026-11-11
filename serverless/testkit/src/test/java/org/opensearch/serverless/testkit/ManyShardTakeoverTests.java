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
 * A node that takes several shards of one index in one pass opens every one of them at the term it won.
 *
 * <p>Each shard's open refreshes the index's shared metadata with the term of the shard it opens and a default for
 * the others. Two shards opening at once each pushed the other's term back down, so all but one started at the old
 * term and were refused when they were promoted to the new one. A first take never showed it, because every term is
 * then the default; it took a second writer.
 */
public class ManyShardTakeoverTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 4;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-many-shard-takeover")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** Opens every shard of the index on a first writer, writes to each, and publishes. */
    private static List<ShardId> holdAndWrite(ServerlessNode node, BackgroundReconciler loop, AtomicLong clock) throws Exception {
        for (int shard = 0; shard < SHARDS; shard++) {
            loop.want("alpha", shard);
        }
        loop.tick(clock.get());
        final List<ShardId> held = new ArrayList<>(node.reconciler().openShards());
        assertEquals("the first writer opens every shard", SHARDS, held.size());
        for (int i = 0; i < 20; i++) {
            node.writes().index(held.get(i % SHARDS), "first-" + i, "{\"msg\":\"first\",\"n\":" + i + "}");
        }
        loop.publishAll();
        return held;
    }

    /** Takes every shard in one pass and checks each is open at a term above the first writer's and takes writes. */
    private static void takeAllInOnePass(ServerlessNode node, MetadataPlane plane, AtomicLong clock) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < SHARDS; shard++) {
            loop.want("alpha", shard);
        }
        loop.tick(clock.get());
        assertEquals("one pass opens every shard it wants", SHARDS, node.reconciler().openShards().size());
        for (ShardId shardId : node.reconciler().openShards()) {
            assertTrue(shardId + " opens at the term it won", node.reconciler().shard(shardId).getOperationPrimaryTerm() > 1L);
            node.writes().index(shardId, "second-" + shardId.id(), "{\"msg\":\"second\",\"n\":" + shardId.id() + "}");
        }
        loop.publishAll();
    }

    public void testShardsLetGoOfAreAllTakenInOnePass() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        try (ServerlessNode first = new ServerlessNode(nodeSettings("many-take-first"))) {
            first.startup().start();
            first.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane);
            holdAndWrite(first, loop, clock);
            loop.setIdleAfterMillis(1);
            for (int shard = 0; shard < SHARDS; shard++) {
                loop.stopWanting("alpha", shard);
            }
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
            assertTrue("given back", first.reconciler().openShards().isEmpty());
        }
        try (ServerlessNode second = new ServerlessNode(nodeSettings("many-take-second"))) {
            second.startup().start();
            second.setMetadataPlane(plane);
            takeAllInOnePass(second, plane, clock);
        }
    }

    public void testACrashedWritersShardsAreAllTakenInOnePass() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        final ServerlessNode first = new ServerlessNode(nodeSettings("many-crash-first"));
        try {
            first.startup().start();
            first.setMetadataPlane(plane);
            final List<ShardId> held = holdAndWrite(first, new BackgroundReconciler(first, plane), clock);
            // Unpublished writes, so the survivor has a log to replay on every shard.
            final java.util.Map<Integer, String> unpublished = new java.util.HashMap<>();
            for (ShardId shardId : held) {
                final String id = "unpublished-" + shardId.id();
                first.writes().index(shardId, id, "{\"msg\":\"first\",\"n\":" + shardId.id() + "}");
                unpublished.put(shardId.id(), id);
            }
            // The writer never lets go; its lease runs out on the shared clock.
            clock.addAndGet(2 * TTL);
            try (ServerlessNode survivor = new ServerlessNode(nodeSettings("many-crash-survivor"))) {
                survivor.startup().start();
                survivor.setMetadataPlane(plane);
                survivor.leases().renewLease(plane);
                plane.membership().refresh();
                takeAllInOnePass(survivor, plane, clock);
                for (ShardId shardId : survivor.reconciler().openShards()) {
                    assertTrue(
                        "the unpublished write to " + shardId + " was replayed",
                        survivor.reads().get(shardId, unpublished.get(shardId.id())).found()
                    );
                }
            }
        } finally {
            try {
                first.close();
            } catch (Exception e) {
                // It lost every shard it held; closing it may report that.
            }
        }
    }

    /** A settings refresh keeps the terms the index's service holds for the shards already open. */
    public void testASettingsRefreshKeepsTheTermsOfOpenShards() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        try (ServerlessNode first = new ServerlessNode(nodeSettings("many-refresh-first"))) {
            first.startup().start();
            first.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane);
            holdAndWrite(first, loop, clock);
            loop.setIdleAfterMillis(1);
            for (int shard = 0; shard < SHARDS; shard++) {
                loop.stopWanting("alpha", shard);
            }
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
        }
        try (ServerlessNode second = new ServerlessNode(nodeSettings("many-refresh-second"))) {
            second.startup().start();
            second.setMetadataPlane(plane);
            final ShardId shardId = second.activation().activateWriter(plane, "alpha", 0).orElseThrow();
            final long term = second.reconciler().shard(shardId).getOperationPrimaryTerm();
            assertTrue("the fixture needs a term above the first writer's", term > 1L);

            final IndexDescriptor described = plane.describe("alpha").orElseThrow();
            second.reconciler()
                .refreshSettings("alpha", described.withSettings(Settings.builder().put("index.refresh_interval", "5s").build()));
            final var service = second.indicesService().indexService(shardId.getIndex());
            assertEquals("5s", service.getMetadata().getSettings().get("index.refresh_interval"));
            // Pushed back to the default, the next shard of this index to open here would be created from it.
            assertEquals("the refresh keeps the open shard's term", term, service.getMetadata().primaryTerm(0));
        }
    }
}
