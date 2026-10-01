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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A dead node's shards are taken when its lease runs out, not when a request for each happens to arrive.
 *
 * <p>A fleet run measured kill-to-writable at about two leases at the median and four at the tail: every shard of a
 * dead node waited for a write or a get to find it, fail to reach the owner, and raise a doubt. Now each survivor
 * hears the departure and takes the shards that hash to it.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessProactiveTakeoverTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-proactive")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** A survivor takes a dead node's shards with no request for them, and replays what the dead node acknowledged. */
    public void testASurvivorTakesADeadNodesShardsWithoutBeingAsked() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final List<String> indices = List.of("alpha", "bravo", "charlie", "delta");
        for (String index : indices) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index + "-000000000000".substring(index.length()), 1, MAPPING, null));
        }

        try (
            ServerlessNode dying = new ServerlessNode(nodeSettings("takeover-dying"));
            ServerlessNode survivor = new ServerlessNode(nodeSettings("takeover-survivor"))
        ) {
            dying.start();
            survivor.start();
            dying.setMetadataPlane(plane);
            survivor.setMetadataPlane(plane);
            final BackgroundReconciler dyingLoop = new BackgroundReconciler(dying, plane).setDemandDrivenActivation(true);
            dyingLoop.activateOnDemand(indices.stream().map(i -> Map.entry(i, 0)).toList());
            for (ShardId shardId : dying.reconciler().openShards()) {
                dying.index(shardId, "acked", "{\"msg\":\"in the log only\",\"n\":1}");
            }

            // The dying node stops renewing; its lease runs out on the survivor's clock.
            clock.addAndGet(2 * TTL);
            survivor.renewLease(plane);
            plane.membership().refresh();

            final BackgroundReconciler loop = new BackgroundReconciler(survivor, plane).setDemandDrivenActivation(true);
            final var later = loop.takeOverFrom(dying.localNode().getId());
            assertTrue("the only survivor takes everything at once: " + later, later.isEmpty());
            assertBusy(() -> {
                final StringBuilder state = new StringBuilder();
                for (String index : indices) {
                    state.append(index).append('=').append(plane.heads().read(index, 0).orElseThrow()).append("; ");
                }
                assertEquals(
                    "open " + survivor.reconciler().openShards() + "; heads " + state,
                    indices.size(),
                    survivor.reconciler().openShards().size()
                );
            }, 30, java.util.concurrent.TimeUnit.SECONDS);
            for (ShardId shardId : survivor.reconciler().openShards()) {
                assertEquals(survivor.localNode().getId(), plane.heads().read(shardId.getIndexName(), 0).orElseThrow().ownerNodeId());
                assertTrue("what the dead node acknowledged is there: " + shardId, survivor.get(shardId, "acked").found());
            }
        }
    }

    /** A node that left cleanly released its heads: its shards stay dormant rather than being taken by everyone. */
    public void testShardsReleasedCleanlyAreLeftAlone() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-0000000000", 1, MAPPING, null));
        try (
            ServerlessNode leaving = new ServerlessNode(nodeSettings("takeover-leaving"));
            ServerlessNode staying = new ServerlessNode(nodeSettings("takeover-staying"))
        ) {
            leaving.start();
            staying.start();
            leaving.setMetadataPlane(plane);
            staying.setMetadataPlane(plane);
            final BackgroundReconciler leavingLoop = new BackgroundReconciler(leaving, plane).setDemandDrivenActivation(true);
            leavingLoop.activateOnDemand(List.of(Map.entry("alpha", 0)));
            leavingLoop.publishAll();
            leavingLoop.setIdleAfterMillis(1);
            leavingLoop.releaseIdle(System.currentTimeMillis() + 60_000L);
            assertNull(plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());

            clock.addAndGet(2 * TTL);
            final BackgroundReconciler loop = new BackgroundReconciler(staying, plane).setDemandDrivenActivation(true);
            loop.takeOverFrom(leaving.localNode().getId());
            assertTrue("nothing is taken that nobody held", staying.reconciler().openShards().isEmpty());
        }
    }
}
