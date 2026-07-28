/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * T1. What concurrent resolution of one name costs, when the callers all miss together.
 *
 * <p>P8 gave {@link DescriptorStore#get} a one second window, and {@link DescriptorReadCountIT} pins that
 * fifty <em>sequential</em> resolutions cost one read. Nothing pins what concurrent ones cost, and the two
 * are different questions: the window only helps a caller that arrives after some other caller has finished
 * reading and populated it. Callers that arrive together all see the same empty slot and all go to the index.
 *
 * <p>That case is not hypothetical here. P7 made routing resolution read the descriptor on every resolution
 * for a gated index, so a search across a gated index's shards produces exactly this shape: many threads,
 * one name, at the same instant.
 *
 * <p>TiDB has the same lazy-load design and closes this with a singleflight group keyed
 * {@code dbID-tblID-schemaVersion} around the storage read ({@code infoschema/infoschema_v2.go:1401}), so
 * concurrent misses for the same object issue one read. We have no equivalent.
 *
 * <p><b>Measured before the fix, one read per caller and perfectly linear:</b>
 *
 * <pre>
 *   callers   reads issued   wall ms
 *         1              1       2.7
 *         4              4       3.7
 *        16             16       8.9
 *        64             64      19.4
 * </pre>
 *
 * <p>Note what the wall clock says and does not say. Sixty-four reads took 19.4 ms rather than 64 x 2.7 ms
 * because they run in parallel, so collapsing them is not primarily a latency fix for one caller. It is a
 * load fix: at 100M indices the descriptor index is a shared resource, and multiplying every resolution by
 * the shard count is how a shared resource becomes the ceiling.
 *
 * <p>This counts reads rather than asserting a collapser exists, following the P8 lesson that the failure
 * shape in this area is a mechanism that is present and useless.
 */
public class DescriptorFanOutIT extends OpenSearchIntegTestCase {

    /** One is the uncontended floor. The rest are shard counts a gated index plausibly has. */
    private static final int[] CONCURRENCY = { 1, 4, 16, 64 };

    private static final String NAME = "fanned-out-idx";

    public void testConcurrentMissesCollapseToOneRead() throws Exception {
        DescriptorStore store = new DescriptorStore(client(), 1);
        store.create(descriptor(NAME));

        // Warm once, so the measurement is not paying for the descriptor index's first ever read.
        assertNotNull(store.get(NAME));

        StringBuilder table = new StringBuilder("\nT1 concurrent resolution of one name\n");
        table.append(String.format(Locale.ROOT, "  %8s %14s %14s %16s%n", "callers", "reads issued", "collapsed", "wall ms"));
        java.util.Map<Integer, Long> readsByCallers = new java.util.LinkedHashMap<>();

        for (int callers : CONCURRENCY) {
            // Cold, so every caller misses. Without this the window from a previous round would serve them
            // and the measurement would be of the cache rather than of the miss.
            store.invalidate(NAME);

            long readsBefore = store.readCount();
            long startedAt = System.nanoTime();
            resolveConcurrently(store, callers);
            double millis = (System.nanoTime() - startedAt) / 1_000_000.0;
            long reads = store.readCount() - readsBefore;
            readsByCallers.put(callers, reads);

            table.append(String.format(Locale.ROOT, "  %8d %14d %14s %16.1f%n", callers, reads, reads <= 1 ? "yes" : "no", millis));
        }

        table.append("\n  Before T2 this was one read per caller: 1, 4, 16, 64.\n");
        logger.warn(table.toString());

        for (var measured : readsByCallers.entrySet()) {
            assertEquals(
                "concurrent misses on one name must collapse to a single read, however many callers arrive "
                    + "together. Before T2 this was one read each, so a search across a gated index multiplied "
                    + "every resolution by its shard count",
                1L,
                (long) measured.getValue()
            );
        }
    }

    /**
     * Every caller released at once, so they all observe the empty slot before any of them fills it.
     * Staggering them would measure the P8 window instead, which {@link DescriptorReadCountIT} already pins.
     */
    private void resolveConcurrently(DescriptorStore store, int callers) throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(callers);
        // Captured rather than counted, since a run where every caller silently resolved to null would
        // otherwise report a beautifully low read count for the wrong reason.
        java.util.concurrent.atomic.AtomicInteger nulls = new java.util.concurrent.atomic.AtomicInteger();

        for (int i = 0; i < callers; i++) {
            Thread caller = new Thread(() -> {
                try {
                    release.await();
                    if (store.get(NAME) == null) {
                        nulls.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });
            caller.setDaemon(true);
            caller.start();
        }

        release.countDown();
        assertTrue("every caller must finish", finished.await(2, TimeUnit.MINUTES));
        assertEquals("a caller resolving to null measured a failure, not a read", 0, nulls.get());
    }

    private static IndexDescriptor descriptor(String name) {
        return new IndexDescriptor(
            name,
            name + "-uuid",
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            1_700_000_000_000L
        );
    }
}
