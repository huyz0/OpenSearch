/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Two latent defects in the in-memory tier, both of which only bite once someone wires it the way
 * its own javadoc permits.
 */
public class InMemoryTierOwnershipAndSingleFlightTests extends OpenSearchTestCase {

    private static SegmentBundle oneFileBundle(String name, String content) {
        return BundleWriter.write(List.of(new BundleFileContent(name, content.getBytes(StandardCharsets.UTF_8))));
    }

    /**
     * The cache used to hand out the cached array itself. No caller mutates it today, which is
     * exactly what made this latent: one {@code Arrays.fill} or one in-place decrypt away from
     * poisoning every subsequent hit for that key across every shard on the node, with nothing able
     * to detect it -- this tier, unlike the disk one, does not re-verify a checksum on a hit.
     */
    public void testTheCacheNeverHandsOutTheArrayItIsHoldingOnTo() throws Exception {
        SegmentBundle bundle = oneFileBundle("a.bin", "original");
        BundleFileEntry entry = bundle.entries().get("a.bin");
        InMemoryPlaintextBundleCache cache = new InMemoryPlaintextBundleCache(1024 * 1024);
        BundleFileReader source = (name, e) -> BundleReader.extractFile(bundle.bytes(), bundle.entries().get(e.name()));

        byte[] first = cache.readFile("bundle-1", entry, source);
        assertEquals("original", new String(first, StandardCharsets.UTF_8));
        Arrays.fill(first, (byte) 'X');

        byte[] second = cache.readFile("bundle-1", entry, source);
        assertEquals("a mutation by one caller must never reach the next", "original", new String(second, StandardCharsets.UTF_8));
        assertEquals("and the second read must still have been a hit", 1, cache.hitCount());
    }

    /**
     * {@code CachingBundleFileReader}'s constructor accepts any delegate, and its own javadoc
     * permits wiring it straight over a {@code BlobContainerBundleStore}. In that configuration the
     * absence of a single-flight turned a cold burst on one file into N object-store GETs -- masked
     * today only because the delegate the plugin happens to wire dedupes internally.
     */
    public void testConcurrentMissesForOneKeyShareASingleFetch() throws Exception {
        SegmentBundle bundle = oneFileBundle("a.bin", "shared-content");
        BundleFileEntry entry = bundle.entries().get("a.bin");
        AtomicInteger fetches = new AtomicInteger();
        CountDownLatch inFetch = new CountDownLatch(1);
        CountDownLatch releaseFetch = new CountDownLatch(1);

        BundleFileReader slowSource = (name, e) -> {
            fetches.incrementAndGet();
            inFetch.countDown();
            try {
                // Hold the leader inside the fetch so every follower genuinely arrives while it is
                // in flight; without a single-flight they would each start their own.
                assertTrue(releaseFetch.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            return BundleReader.extractFile(bundle.bytes(), bundle.entries().get(e.name()));
        };

        CachingBundleFileReader reader = new CachingBundleFileReader(new InMemoryPlaintextBundleCache(1024 * 1024), slowSource);
        int readerCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(readerCount);
        try {
            Future<byte[]> leader = executor.submit(() -> reader.readFile("bundle-1", entry));
            assertTrue("the leader must actually be inside the fetch", inFetch.await(30, TimeUnit.SECONDS));

            java.util.List<Future<byte[]>> followers = new java.util.ArrayList<>();
            for (int i = 0; i < readerCount - 1; i++) {
                followers.add(executor.submit(() -> reader.readFile("bundle-1", entry)));
            }
            // Give the followers a moment to reach the in-flight map before releasing the leader.
            Thread.sleep(200);
            releaseFetch.countDown();

            assertEquals("shared-content", new String(leader.get(30, TimeUnit.SECONDS), StandardCharsets.UTF_8));
            for (Future<byte[]> follower : followers) {
                assertEquals("shared-content", new String(follower.get(30, TimeUnit.SECONDS), StandardCharsets.UTF_8));
            }
        } finally {
            releaseFetch.countDown();
            executor.shutdown();
        }
        assertEquals("every concurrent miss for one key must share one fetch", 1, fetches.get());
    }

    /** A failed fetch must propagate to every waiter rather than leaving them hung on a future nobody completes. */
    public void testAFailedFetchIsReportedToEveryWaiterAndLeavesNoStuckEntry() throws Exception {
        SegmentBundle bundle = oneFileBundle("a.bin", "content");
        BundleFileEntry entry = bundle.entries().get("a.bin");
        AtomicInteger attempts = new AtomicInteger();
        BundleFileReader failingThenWorking = (name, e) -> {
            if (attempts.getAndIncrement() == 0) {
                throw new IOException("transient");
            }
            return BundleReader.extractFile(bundle.bytes(), bundle.entries().get(e.name()));
        };

        CachingBundleFileReader reader = new CachingBundleFileReader(new InMemoryPlaintextBundleCache(1024 * 1024), failingThenWorking);
        expectThrows(IOException.class, () -> reader.readFile("bundle-1", entry));
        // The in-flight entry must have been removed, so a retry actually retries.
        assertEquals("content", new String(reader.readFile("bundle-1", entry), StandardCharsets.UTF_8));
        assertEquals(2, attempts.get());
    }
}
