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
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.WriteBackpressure;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * A node sheds writes while the store is throttling, and takes them again as soon as it stops.
 *
 * <p>After a minute of SlowDown on half its requests, a fleet ran at a quarter of its writes refused for minutes after
 * the store had recovered, and during the burst admitted writes that then timed out rather than refusing them early.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessSlowdownRecoveryTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    /**
     * A lapse whose head re-read fails -- as it does while the store is throttling -- is lifted by the next pass once
     * the reads work, not by the next five-minute head verification. Waiting for that left a node refusing every write
     * long after the throttling had ended.
     */
    public void testASuspensionIsLiftedByTheNextPassOnceTheStoreAnswers() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final HookedBlobStore store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-0000000000", 1, "{\"properties\":{\"msg\":{\"type\":\"text\"}}}", null));
        final Settings settings = Settings.builder()
            .put("node.name", "recovering")
            .put("cluster.name", "serverless-slowdown-recovery")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.start();
            node.setMetadataPlane(plane);
            node.setHeadVerifyIntervalMillis(TimeUnit.MINUTES.toMillis(5));
            node.renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);
            loop.activateOnDemand(List.of(Map.entry("alpha", 0)));
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            loop.verifyHeads();
            assertTrue("held and verified", node.holdsAsWriter(shardId, TTL));

            // The lease runs out while the store throttles: the re-read of every head after the renewal fails.
            clock.addAndGet(2 * TTL);
            store.failRegisterReadsUnder("shards");
            expectThrows(Exception.class, () -> node.renewLease(plane));
            assertFalse("suspended until the heads are read again", node.holdsAsWriter(shardId, TTL));

            // The store answers again. The next periodic pass, well inside the five minutes, must lift it.
            store.failRegisterReadsUnder(null);
            loop.verifyHeads();
            assertTrue("acknowledging again after one pass", node.holdsAsWriter(shardId, TTL));
        }
    }

    /**
     * A write that has waited past the budget refuses the next one, though no append has finished to say the store is
     * slow: under SlowDown appends sit in retries for tens of seconds, and a limiter that only learned from finished
     * appends admitted writes for the whole burst.
     */
    public void testAWriteWaitingPastTheBudgetRefusesTheNext() {
        final WriteBackpressure limiter = new WriteBackpressure(100, 300, 1, 64);
        final Releasable stuck = limiter.admit();
        try {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(400));
            final WriteBackpressure.ThrottledException refused = expectThrows(WriteBackpressure.ThrottledException.class, limiter::admit);
            assertTrue("with a retry hint: " + refused.getMessage(), refused.retryAfterSeconds() >= 1);
        } finally {
            stuck.close();
        }
        try (Releasable next = limiter.admit()) {
            assertNotNull("taken again once nothing is waiting", next);
        }
    }
}
