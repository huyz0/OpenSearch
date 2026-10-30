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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.script.StoredScripts;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StoredScriptRefreshCostTests extends OpenSearchTestCase {
    public void testConcurrentMissesShareTheRefreshTheyWaitedFor() throws Exception {
        final List<Thread> threads = new CopyOnWriteArrayList<>();
        final var refreshing = new AtomicReference<Thread>();
        try (
            var counted = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
            var store = new HookedBlobStore(counted);
            var workers = Executors.newFixedThreadPool(8, task -> {
                final var thread = new Thread(task, "stored-script-refresh");
                threads.add(thread);
                return thread;
            })
        ) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var cache = new StoredScripts(() -> metadata);
            cache.refresh(true);
            counted.reset();
            final var entered = new CountDownLatch(1);
            final var resume = new CountDownLatch(1);
            final var start = new CyclicBarrier(8);
            store.onNextRegisterRead("scripts", "_version", () -> {
                refreshing.set(Thread.currentThread());
                entered.countDown();
                try {
                    assertTrue(resume.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            final var results = new java.util.ArrayList<java.util.concurrent.Future<Boolean>>();
            for (int i = 0; i < 8; i++) {
                results.add(workers.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return cache.lookup("missing").isEmpty();
                }));
            }
            try {
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                assertBusy(
                    () -> assertEquals(
                        7L,
                        threads.stream().filter(thread -> thread != refreshing.get() && thread.getState() == Thread.State.BLOCKED).count()
                    )
                );
            } finally {
                resume.countDown();
            }
            for (var result : results) {
                assertTrue(result.get(10, TimeUnit.SECONDS));
            }
            assertEquals("waiters should use the refresh that completed while they waited", 1L, counted.registerReads());
        }
    }

    public void testFreshMissStillDiscoversANewRemoteScriptImmediately() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var cache = new StoredScripts(() -> metadata);
            assertTrue(cache.lookup("new-script").isEmpty());
            metadata.scripts().put("new-script", "{\"script\":{\"lang\":\"mustache\",\"source\":\"new-value\"}}");
            assertEquals("new-value", cache.lookup("new-script").orElseThrow().getSource());
        }
    }
}
