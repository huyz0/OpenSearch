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
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * T22. Whether the descriptor cache works for a multi-tenant access pattern.
 *
 * <p>Earlier reasoning about the read path at many coordinators assumed uniform access and concluded the
 * cache would be useless: fifty thousand entries against a hundred million indices is a 0.05 percent hit
 * rate, so effectively every read pays the 0.35 to 0.48 ms miss. That assumption was never stated as an
 * assumption, and for an index per tenant it is the wrong one. Tenant traffic is heavily skewed, a few
 * tenants generate most requests, and skew is exactly what a cache is for.
 *
 * <p>So this measures hit rate against a Zipf distribution over tenants rather than against a uniform one,
 * at cache capacities expressed as a fraction of the tenant population. The question it answers is what
 * fraction of tenants a node has to hold to make the cache carry its traffic, which is a different and much
 * more favourable question than what fraction of tenants it can hold.
 *
 * <p>Counted rather than asserted, using the store's own read counter, following the P8 lesson that a cache
 * in this area is capable of being present and useless.
 *
 * <p>The clock is fixed so the one second window never expires. That isolates capacity and access pattern
 * from staleness, which is the right decomposition: the window is a freshness decision made by H18 and the
 * capacity is a memory decision made by T4b, and mixing them would measure neither.
 */
public class DescriptorHitRateUnderSkewIT extends OpenSearchIntegTestCase {

    /** Enough tenants that a capacity of a few percent is meaningful, few enough to populate quickly. */
    private static final int TENANTS = 4_000;

    /**
     * Accesses per arm, reduced from forty thousand.
     *
     * <p>Twenty arms at forty thousand is eight hundred thousand reads through the transport client, which
     * took this benchmark seven to nine minutes on its own and eventually exceeded the twenty minute suite
     * budget it shares with every other descriptor test. The timeout was first blamed on an unrelated change
     * to the descriptor index settings, which an A/B appeared to confirm and a per-operation measurement
     * then contradicted. It was neither: the benchmark is simply expensive enough that a loaded suite tips
     * it over.
     *
     * <p>A hit rate is a ratio, and ten thousand samples over four thousand tenants estimates one to well
     * within the precision anything here is quoted at. Spending four times as long to add a decimal place
     * nobody uses is what made this fragile.
     */
    private static final int ACCESSES = 10_000;

    /** Capacity as a count, against 4,000 tenants: 0.5%, 2.5%, 5%, 12.5%, 25%. */
    private static final int[] CAPACITIES = { 20, 100, 200, 500, 1_000 };

    /**
     * Zipf exponents worth reporting. 0.0 is uniform, the assumption the earlier reasoning made. 1.0 is the
     * classic Zipf that multi-tenant and web traffic usually resemble. 1.2 is a heavier head.
     */
    private static final double[] SKEWS = { 0.0, 0.8, 1.0, 1.2 };

    private final AtomicLong now = new AtomicLong(1_000_000_000L);

    public void testHitRateAgainstTenantSkew() {
        DescriptorStore populate = new DescriptorStore(client(), 1, now::get);
        for (int i = 0; i < TENANTS; i++) {
            populate.create(descriptor(tenant(i)));
        }

        StringBuilder table = new StringBuilder(
            String.format(Locale.ROOT, "%nT22 descriptor cache hit rate, %,d tenants, %,d accesses%n", TENANTS, ACCESSES)
        );
        table.append(String.format(Locale.ROOT, "  %10s", "capacity"));
        for (double skew : SKEWS) {
            table.append(String.format(Locale.ROOT, " %14s", skew == 0.0 ? "uniform" : ("zipf " + skew)));
        }
        table.append(String.format(Locale.ROOT, "%n"));

        for (int capacity : CAPACITIES) {
            table.append(String.format(Locale.ROOT, "  %6d %3.1f%%", capacity, 100.0 * capacity / TENANTS));
            for (double skew : SKEWS) {
                table.append(String.format(Locale.ROOT, " %13.1f%%", 100.0 * hitRate(capacity, skew)));
            }
            table.append(String.format(Locale.ROOT, "%n"));
        }

        table.append("\n  Uniform is the assumption earlier reasoning made. Real tenant traffic is skewed,\n");
        table.append("  and a cache sized at a few percent of tenants is sized for the head, not the tail.\n");
        logger.warn(table.toString());

        assertTrue("the measurement must be non-zero, or this measured nothing", hitRate(CAPACITIES[0], 1.0) > 0);
    }

    private double hitRate(int capacity, double skew) {
        // A fresh store per arm, so one arm's warm cache cannot flatter the next.
        DescriptorStore store = new DescriptorStore(client(), 1, now::get, DescriptorStore.COLLAPSE_WAIT_MILLIS, capacity);
        // Seeded per arm so every capacity sees the identical access sequence, which is what makes the
        // columns comparable rather than four separate experiments.
        Random random = new Random(Double.hashCode(skew));
        double[] cumulative = zipfCumulative(TENANTS, skew);

        long before = store.readCount();
        for (int i = 0; i < ACCESSES; i++) {
            assertNotNull(store.get(tenant(sample(random, cumulative))));
        }
        long reads = store.readCount() - before;
        return 1.0 - (reads / (double) ACCESSES);
    }

    /** Cumulative Zipf weights over ranks 1..n. An exponent of zero is the uniform distribution. */
    private static double[] zipfCumulative(int n, double exponent) {
        double[] cumulative = new double[n];
        double total = 0;
        for (int rank = 1; rank <= n; rank++) {
            total += 1.0 / Math.pow(rank, exponent);
            cumulative[rank - 1] = total;
        }
        for (int i = 0; i < n; i++) {
            cumulative[i] /= total;
        }
        return cumulative;
    }

    private static int sample(Random random, double[] cumulative) {
        double u = random.nextDouble();
        int lo = 0;
        int hi = cumulative.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cumulative[mid] < u) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private static String tenant(int i) {
        return String.format(Locale.ROOT, "tenant-%06d", i);
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
