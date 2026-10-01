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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A shard is activated however busy the GENERIC pool is with things waiting for activations.
 *
 * <p>Activations ran on GENERIC, which is also where a write waits for the activation of its shard. A fleet run had
 * all 128 GENERIC threads waiting, and the activations they waited for queued behind them: each started only when a
 * waiting write timed out. Shards went unserved for ten minutes and more.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessActivationStarvationTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    public void testAnActivationRunsWhileEveryGenericThreadWaits() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(1024, createTempDir(), false),
            BlobPath.cleanPath(),
            clock::get,
            30_000L
        );
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-0000000000", 1, MAPPING, null));
        final Settings settings = Settings.builder()
            .put("node.name", "starved")
            .put("cluster.name", "serverless-starvation")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);

            final int generic = node.threadPool().info(ThreadPool.Names.GENERIC).getMax();
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch occupied = new CountDownLatch(generic);
            for (int i = 0; i < generic; i++) {
                node.threadPool().generic().execute(() -> {
                    occupied.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            try {
                assertTrue("every GENERIC thread is waiting", occupied.await(30, TimeUnit.SECONDS));
                final var taken = loop.activateForRequest("alpha", 0).get(30, TimeUnit.SECONDS);
                assertTrue("the shard is taken with no GENERIC thread free", taken.isPresent());
                assertEquals(node.localNode().getId(), plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
            } finally {
                release.countDown();
            }
        }
    }
}
