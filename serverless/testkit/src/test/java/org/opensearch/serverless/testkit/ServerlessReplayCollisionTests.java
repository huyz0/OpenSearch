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
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A shard whose log holds an acknowledged operation under a sequence number its restored commit gives to a different
 * document refuses to open, rather than opening without it.
 *
 * <p>Core's recovery skips a replayed operation whose sequence number the commit has already processed. A fleet run lost
 * three acknowledged writes whose records were in the log at a takeover and never reached the successor's commit; one
 * way that happens is two operations sharing a number. Opened, the shard would drop one silently and its next publish
 * would trim the record away for good. Refused, it is an outage, named in the log, with both operations still there.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessReplayCollisionTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-replay-collision")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    public void testAShardWhoseLogCollidesWithItsCommitRefusesToOpen() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final IndexDescriptor alpha = new IndexDescriptor(
            "alpha",
            "uuid-alpha-0000000000",
            1,
            "{\"properties\":{\"msg\":{\"type\":\"text\"}}}",
            null
        );
        plane.createIndex(alpha);
        try (
            ServerlessNode first = new ServerlessNode(nodeSettings("collision-first"));
            ServerlessNode second = new ServerlessNode(nodeSettings("collision-second"))
        ) {
            first.start();
            first.setMetadataPlane(plane);
            first.renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane).setDemandDrivenActivation(true);
            loop.activateOnDemand(List.of(Map.entry("alpha", 0)));
            final ShardId shardId = first.reconciler().openShards().iterator().next();
            first.index(shardId, "a", "{\"msg\":\"a\"}");
            loop.publishAll();
            loop.setIdleAfterMillis(1);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
            assertTrue(first.reconciler().openShards().isEmpty());

            // A different acknowledged operation under the same sequence number, at a later term: what a writer that
            // opened from an outdated commit would have written.
            final long term = plane.heads().read("alpha", 0).orElseThrow().term() + 1;
            plane.walStore("alpha", alpha.uuid(), 0).append(term, new WalRecord("b", "{\"msg\":\"b\"}", 0L, term, 1L));

            second.start();
            second.setMetadataPlane(plane);
            second.renewLease(plane);
            final BackgroundReconciler other = new BackgroundReconciler(second, plane).setDemandDrivenActivation(true);
            try {
                other.activateOnDemand(List.of(Map.entry("alpha", 0)));
            } catch (Exception expected) {
                // Refused: the activation reports it, or settles without the shard.
            }
            assertTrue("the shard does not open without the colliding write", second.reconciler().openShards().isEmpty());
            assertNull("and its head is given back for the next attempt", plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
        }
    }

    /**
     * Two records in the log itself under one sequence number, both past the commit: what a writer's record landing past
     * its successor's fence leaves. Core would apply the first and skip the second, and the open's first publish would
     * trim it -- which is how a fleet run lost three acknowledged writes. The shard refuses to open instead.
     */
    public void testAShardWhoseLogHoldsOneSequenceNumberTwiceRefusesToOpen() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final IndexDescriptor alpha = new IndexDescriptor(
            "alpha",
            "uuid-alpha-0000000000",
            1,
            "{\"properties\":{\"msg\":{\"type\":\"text\"}}}",
            null
        );
        plane.createIndex(alpha);
        try (
            ServerlessNode first = new ServerlessNode(nodeSettings("twice-first"));
            ServerlessNode second = new ServerlessNode(nodeSettings("twice-second"))
        ) {
            first.start();
            first.setMetadataPlane(plane);
            first.renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane).setDemandDrivenActivation(true);
            loop.activateOnDemand(List.of(Map.entry("alpha", 0)));
            final ShardId shardId = first.reconciler().openShards().iterator().next();
            first.index(shardId, "a", "{\"msg\":\"a\"}");
            loop.publishAll();
            loop.setIdleAfterMillis(1);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
            assertTrue(first.reconciler().openShards().isEmpty());

            // Sequence number 1, twice: an older writer's record past the fence, and its successor's own write.
            final long term = plane.heads().read("alpha", 0).orElseThrow().term();
            plane.walStore("alpha", alpha.uuid(), 0).append(term, new WalRecord("stale", "{\"msg\":\"stale\"}", 1L, term, 1L));
            plane.walStore("alpha", alpha.uuid(), 0)
                .append(term + 1, new WalRecord("acknowledged", "{\"msg\":\"acknowledged\"}", 1L, term + 1, 1L));

            second.start();
            second.setMetadataPlane(plane);
            second.renewLease(plane);
            final BackgroundReconciler other = new BackgroundReconciler(second, plane).setDemandDrivenActivation(true);
            try {
                other.activateOnDemand(List.of(Map.entry("alpha", 0)));
            } catch (Exception expected) {
                // Refused: the activation reports it, or settles without the shard.
            }
            assertTrue("the shard does not open dropping one of them", second.reconciler().openShards().isEmpty());
            assertNull("and its head is given back for the next attempt", plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
        }
    }
}
