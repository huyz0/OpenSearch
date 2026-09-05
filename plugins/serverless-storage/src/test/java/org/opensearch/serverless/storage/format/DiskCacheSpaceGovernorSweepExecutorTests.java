/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The node-wide sweep walks the whole cache tree and stats every file in it, and
 * {@code recordBytesAdded} is called from a search thread servicing a cache miss. Running that walk
 * on the caller's thread is what the optional executor exists to avoid; these tests pin that the
 * hand-off actually happens and that the default stays exactly the inline behaviour it always was.
 */
public class DiskCacheSpaceGovernorSweepExecutorTests extends OpenSearchTestCase {

    private static SegmentBundle threeTenByteFiles() {
        return BundleWriter.write(
            List.of(
                new BundleFileContent("a.bin", new byte[10]),
                new BundleFileContent("b.bin", new byte[10]),
                new BundleFileContent("c.bin", new byte[10])
            )
        );
    }

    private static BundleFileReader inMemoryReader(SegmentBundle bundle) {
        return (bundleName, entry) -> BundleReader.extractFile(bundle.bytes(), entry);
    }

    /**
     * The property the executor exists for: the tree walk must not happen on the thread whose
     * cache write crossed the budget, because that thread is a search thread servicing a miss.
     */
    public void testTheSweepRunsOnTheSuppliedExecutorAndNotOnTheTriggeringThread() throws Exception {
        SegmentBundle bundle = threeTenByteFiles();
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        AtomicReference<Thread> sweptOn = new AtomicReference<>();
        ExecutorService sweepPool = Executors.newSingleThreadExecutor(r -> new Thread(r, "test-sweep"));
        try {
            DiskCacheSpaceGovernor governor = new DiskCacheSpaceGovernor(cacheRoot, 25L, task -> sweepPool.execute(() -> {
                sweptOn.set(Thread.currentThread());
                task.run();
            }));
            LocalDiskCachingBundleStore store = new LocalDiskCachingBundleStore(
                inMemoryReader(bundle),
                cacheRoot.resolve("index-uuid").resolve("0"),
                null,
                0L,
                governor
            );

            for (String name : List.of("a.bin", "b.bin", "c.bin")) {
                store.readFile("bundle-1", bundle.entries().get(name));
            }

            assertTrue("the handed-off sweep must complete", governor.awaitSweepQuiescenceForTesting(30_000L));
            assertNotNull("a sweep must actually have been handed off", sweptOn.get());
            assertNotSame(
                "the tree walk must never run on the thread whose cache write triggered it",
                Thread.currentThread(),
                sweptOn.get()
            );
            assertTrue("and it must still have done its job", governor.evictedCount() > 0);
        } finally {
            sweepPool.shutdownNow();
            assertTrue(sweepPool.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    /**
     * With no executor the sweep is inline, which is what every existing caller and the default
     * plugin wiring get -- so a triggering write must find the eviction already done when it
     * returns, with no thread created anywhere.
     */
    public void testWithNoExecutorTheSweepIsInlineAndOwnsNoThread() throws Exception {
        SegmentBundle bundle = threeTenByteFiles();
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        DiskCacheSpaceGovernor governor = new DiskCacheSpaceGovernor(cacheRoot, 25L);
        Path shardDir = cacheRoot.resolve("index-uuid").resolve("0");
        LocalDiskCachingBundleStore store = new LocalDiskCachingBundleStore(inMemoryReader(bundle), shardDir, null, 0L, governor);

        for (String name : List.of("a.bin", "b.bin", "c.bin")) {
            store.readFile("bundle-1", bundle.entries().get(name));
        }

        // No awaiting: inline means done by the time the third read returned.
        assertTrue("the inline sweep must have evicted down to the target", governor.evictedCount() > 0);
        try (var remaining = Files.list(shardDir)) {
            assertTrue("the tree must be under budget once the triggering write returns", remaining.count() < 3);
        }
        assertTrue("nothing is outstanding when sweeps are inline", governor.awaitSweepQuiescenceForTesting(0L));
    }
}
