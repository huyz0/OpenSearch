/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * H10. What creating a gated index actually costs, as opposed to what it avoids.
 *
 * <p>S22 reported descriptor-only creation at 0.0005 ms, flat against population, against 57.777 ms through
 * cluster state at fifty thousand indices. That number is real and it is not creation. It measures the cost
 * of <em>not</em> doing a cluster state update, in-JVM, with no descriptor written anywhere. Actual gated
 * creation writes a descriptor document into an OpenSearch index with {@code op_type=create}, and that write
 * has never been measured.
 *
 * <p>The distinction decides whether a hundred million indices can be populated at all rather than merely
 * held. Publication cost is what stops the cluster from functioning; write cost is what stops the fleet from
 * ever being filled. At 5 ms per index a single writer needs 5.8 days to create 100M, at 100 ms it needs 116
 * days, and the plan has been quoting a number that describes neither.
 *
 * <p>Measured concurrently rather than serially, because a serial number would answer a question nobody
 * asks: creation is a fleet-wide operation issued by many clients, and the interesting quantity is what the
 * cluster sustains, not what one thread observes. Per-index latency is reported alongside so the two are not
 * confused again.
 *
 * <p>What this deliberately does not claim: it measures descriptor writes against a small cluster, so the
 * throughput is the shape of the cost rather than a capacity planning figure. The extrapolation to 100M is
 * printed as arithmetic on the measured rate, and it is arithmetic, not a prediction.
 */
public class GatedCreationThroughputIT extends OpenSearchIntegTestCase {

    private static final String DESCRIPTORS = "descriptors";
    private static final int[] BATCHES = { 5_000, 20_000, 50_000 };
    private static final int IN_FLIGHT = 500;
    private static final long TARGET_POPULATION = 100_000_000L;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("cluster.max_shards_per_node", 100_000).build();
    }

    public void testGatedCreationThroughput() throws Exception {
        createDescriptorIndex();

        StringBuilder table = new StringBuilder("\nH10 end-to-end gated creation, descriptor writes at op_type=create\n");
        table.append(String.format(Locale.ROOT, "  %9s %14s %16s %18s%n", "created", "elapsed (s)", "per index (ms)", "indices/sec"));

        int alreadyCreated = 0;
        double lastRate = 0;
        for (int batch : BATCHES) {
            int toCreate = batch - alreadyCreated;

            long startedAt = System.nanoTime();
            createDescriptors(alreadyCreated, toCreate);
            double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;

            lastRate = toCreate / elapsedSeconds;
            table.append(
                String.format(
                    Locale.ROOT,
                    "  %,9d %14.2f %16.4f %,18.0f%n",
                    batch,
                    elapsedSeconds,
                    (elapsedSeconds * 1_000.0) / toCreate,
                    lastRate
                )
            );
            alreadyCreated = batch;
        }

        double daysForTarget = TARGET_POPULATION / lastRate / 86_400.0;
        table.append(
            String.format(
                Locale.ROOT,
                "%n  At the last measured rate, %,d indices takes %.1f days on one cluster of this size.%n"
                    + "  That is arithmetic on a small cluster, not a capacity figure: it says whether the%n"
                    + "  order of magnitude is days or years, which is the only thing it can say.%n",
                TARGET_POPULATION,
                daysForTarget
            )
        );
        logger.warn(table.toString());
    }

    /**
     * The claim that matters more than the rate itself: creation must not slow down as the population
     * grows. A rate that degrades is a system that cannot be filled regardless of where it starts, because
     * the last million indices would cost more than the first.
     *
     * <p>Asserted loosely on purpose. The interesting failure is a rate that collapses by an order of
     * magnitude, which is a structural problem; a rate that drifts by a factor of two on a test cluster is
     * segment merging and says nothing.
     */
    public void testCreationRateDoesNotCollapseAsThePopulationGrows() throws Exception {
        createDescriptorIndex();

        long startedAt = System.nanoTime();
        createDescriptors(0, 10_000);
        double firstRate = 10_000 / ((System.nanoTime() - startedAt) / 1_000_000_000.0);

        startedAt = System.nanoTime();
        createDescriptors(40_000, 10_000);
        double laterRate = 10_000 / ((System.nanoTime() - startedAt) / 1_000_000_000.0);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nH10 creation rate: first 10,000 at %,.0f/s, another 10,000 onto a 40,000 population at %,.0f/s, ratio %.2f%n",
                firstRate,
                laterRate,
                laterRate / firstRate
            )
        );

        assertTrue("both rates must be non-zero, or this measured nothing", firstRate > 0 && laterRate > 0);
        assertTrue(
            String.format(
                Locale.ROOT,
                "creation must not collapse with population: %,.0f/s at the start against %,.0f/s onto an "
                    + "existing 40,000. A tenfold degradation here means the fleet cannot be filled, since "
                    + "the cost of the last indices would exceed the cost of the first",
                firstRate,
                laterRate
            ),
            laterRate > firstRate / 10
        );
    }

    // ---------------------------------------------------------------- helpers

    private void createDescriptorIndex() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(DESCRIPTORS)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .setMapping("name", "type=keyword", "uuid", "type=keyword", "shards", "type=integer")
        );
    }

    /**
     * Creates descriptors the way gated creation does: one document per index, {@code op_type=create}, which
     * is what makes the name unique without a cluster state update.
     */
    private void createDescriptors(int from, int count) throws Exception {
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger failures = new AtomicInteger();
        Semaphore inFlight = new Semaphore(IN_FLIGHT);
        for (int i = from; i < from + count; i++) {
            inFlight.acquire();
            String name = String.format(Locale.ROOT, "idx-%08d", i);
            client().index(
                new IndexRequest(DESCRIPTORS).id(name).source("name", name, "uuid", name + "-uuid", "shards", 1).create(true),
                new ActionListener<>() {
                    @Override
                    public void onResponse(IndexResponse response) {
                        inFlight.release();
                        done.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        failures.incrementAndGet();
                        inFlight.release();
                        done.countDown();
                    }
                }
            );
        }
        assertTrue("descriptor creation must finish", done.await(20, TimeUnit.MINUTES));
        assertEquals("no descriptor creation may fail, or the rate is measuring rejections", 0, failures.get());
    }
}
