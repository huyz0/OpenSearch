/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What fraction of descriptor lookups the cache actually absorbs, under an access distribution that looks
 * like a real tenant population rather than a uniform one.
 *
 * <h2>Why this can be measured here and the latency cannot</h2>
 *
 * A hit rate is a property of the access distribution and the cache policy. Neither depends on what the
 * backend is, so it is the same number against a system index and against an object store. The latency it
 * implies is not, and this deliberately measures no latency at all: doing that against a filesystem
 * container would report local disk speed dressed up as an object store result, which is the shape this
 * area calls failing by succeeding.
 *
 * <h2>Why uniform access would have been the wrong fixture</h2>
 *
 * T22 already found the read path had assumed uniform tenant access once. Real tenant traffic is heavily
 * skewed: a small set of tenants takes most of the requests. Uniform access is the worst case for a cache
 * and would understate the hit rate; more importantly it would hide the thing worth knowing, which is where
 * the rate collapses as the working set outgrows the bound.
 *
 * <p>The decision rule is stated before the numbers, as this area requires. The design claims the cache
 * hides the backend round trip. That is credible only if a cache holding a small fraction of the population
 * still serves the large majority of lookups under skew. If a bound of one tenth of the population cannot
 * reach 90%, the prefetch in C1 is doing the real work and the cache is not, which changes what has to be
 * built first.
 */
public class DescriptorCacheHitRateTests extends OpenSearchTestCase {

    private static IndexDescriptor descriptor(String name) {
        return new IndexDescriptor(
            name,
            "uuid-" + name,
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            org.opensearch.Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            0L
        );
    }

    /**
     * Zipf-distributed tenant picks, which is the shape multi-tenant traffic actually has.
     *
     * <p>Built by inverting a cumulative table rather than by sampling and rejecting, so the distribution
     * is exact and the test is deterministic for a given seed. Determinism matters here: a hit rate that
     * moves run to run cannot be compared against a threshold.
     */
    private static final class Zipf {
        private final double[] cumulative;
        private final Random random;

        Zipf(int population, double exponent, long seed) {
            this.random = new Random(seed);
            this.cumulative = new double[population];
            double sum = 0;
            for (int i = 0; i < population; i++) {
                sum += 1.0 / Math.pow(i + 1, exponent);
                cumulative[i] = sum;
            }
            for (int i = 0; i < population; i++) {
                cumulative[i] /= sum;
            }
        }

        int next() {
            double target = random.nextDouble();
            int low = 0;
            int high = cumulative.length - 1;
            while (low < high) {
                int mid = (low + high) >>> 1;
                if (cumulative[mid] < target) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }
    }

    private record Result(double hitRate, long reads, long evictions) {
    }

    /**
     * The share of lookups that land in the {@code capacity} most popular tenants.
     *
     * <p>The ceiling any capacity-bounded cache could reach on this distribution, given a perfect eviction
     * policy that always kept exactly the hottest set. Reported beside the measured rate so the gap between
     * them is attributable: a low ceiling is the workload, and a rate well under the ceiling is the policy.
     */
    private static double reachableCeiling(int population, int capacity, double exponent) {
        double head = 0;
        double all = 0;
        for (int i = 0; i < population; i++) {
            double weight = 1.0 / Math.pow(i + 1, exponent);
            all += weight;
            if (i < capacity) {
                head += weight;
            }
        }
        return head / all;
    }

    private Result run(int population, int capacity, double exponent, int lookups) {
        // A TTL long enough not to expire during the run, so this measures the capacity bound rather than
        // the freshness window. The window is a separate question and T3 already established its default
        // is wrong for an object store.
        DescriptorCache cache = new DescriptorCache(new AtomicLong()::get, Long.MAX_VALUE, 3_000, capacity, Long.MAX_VALUE);
        Zipf zipf = new Zipf(population, exponent, 42L);

        for (int i = 0; i < lookups; i++) {
            String name = "tenant-" + zipf.next();
            cache.get(name, DescriptorCacheHitRateTests::descriptor);
        }
        return new Result(cache.hitRate(), cache.readCount(), cache.evictionCount());
    }

    public void testHitRateUnderSkewAcrossCacheBounds() {
        int population = 100_000;
        int lookups = 500_000;
        double exponent = 1.0;

        StringBuilder table = new StringBuilder("\ncache bound | share of population | hit rate | ceiling | shortfall | backend reads\n");
        double atOneTenth = Double.NaN;
        double ceilingAtOneTenth = Double.NaN;
        for (int capacity : new int[] { 100, 1_000, 10_000, 50_000 }) {
            Result result = run(population, capacity, exponent, lookups);
            double ceiling = reachableCeiling(population, capacity, exponent);
            table.append(
                String.format(
                    Locale.ROOT,
                    "%11d | %18.1f%% | %7.1f%% | %6.1f%% | %8.1f%% | %13d%n",
                    capacity,
                    100.0 * capacity / population,
                    100.0 * result.hitRate(),
                    100.0 * ceiling,
                    100.0 * (ceiling - result.hitRate()),
                    result.reads()
                )
            );
            if (capacity == 10_000) {
                atOneTenth = result.hitRate();
                ceilingAtOneTenth = ceiling;
            }
        }
        logger.info("descriptor cache hit rate, {} tenants, Zipf s={}, {} lookups{}", population, exponent, lookups, table);

        assertFalse("the one-tenth arm must have run", Double.isNaN(atOneTenth));

        // The rule stated before the numbers was 90% at a tenth of the population, and it was not met.
        // The finding is recorded in the design document rather than dissolved by relaxing the rule; what
        // is asserted here is a regression floor, set below the measured value, plus the two structural
        // facts that make the number interpretable.
        assertTrue("hit rate regressed below the T20 measurement of 69.3%; got " + atOneTenth, atOneTenth >= 0.65);
        assertTrue(
            "the ceiling must exceed the measured rate, or the policy is not the thing leaving headroom",
            ceilingAtOneTenth > atOneTenth
        );
        assertTrue(
            "and the shortfall against a perfect policy must stay bounded; ceiling=" + ceilingAtOneTenth + " measured=" + atOneTenth,
            ceilingAtOneTenth - atOneTenth < 0.20
        );
    }

    /**
     * The control arm, and the reason the skewed number means anything. Uniform access is what the design
     * would look like if the assumption T22 corrected were true, and it is the worst case for any cache.
     */
    public void testUniformAccessIsMarkedlyWorseWhichIsWhySkewMatters() {
        int population = 100_000;
        int capacity = 10_000;
        int lookups = 200_000;

        Result skewed = run(population, capacity, 1.0, lookups);
        // Exponent zero makes the Zipf table flat, which is exactly a uniform draw over the population.
        Result uniform = run(population, capacity, 0.0, lookups);

        logger.info(
            "hit rate at {}% capacity: skewed {}%, uniform {}%",
            100 * capacity / population,
            String.format(Locale.ROOT, "%.1f", 100 * skewed.hitRate()),
            String.format(Locale.ROOT, "%.1f", 100 * uniform.hitRate())
        );

        assertTrue(
            "skew must beat uniform, or the fixture is not modelling what it claims to; skewed="
                + skewed.hitRate()
                + " uniform="
                + uniform.hitRate(),
            skewed.hitRate() > uniform.hitRate() + 0.3
        );
    }

    /**
     * The tail is the part that does not benefit, and it is worth stating rather than averaging away. A
     * tenant outside the working set pays a backend round trip on essentially every lookup, so a per-tenant
     * latency claim cannot be made from a fleet-wide hit rate.
     */
    public void testTheColdTailStaysCold() {
        DescriptorCache cache = new DescriptorCache(new AtomicLong()::get, Long.MAX_VALUE, 3_000, 100, Long.MAX_VALUE);

        // Fill the cache with a hot set, then interleave one cold tenant that can never stay resident.
        for (int round = 0; round < 50; round++) {
            for (int hot = 0; hot < 100; hot++) {
                cache.get("hot-" + hot, DescriptorCacheHitRateTests::descriptor);
            }
            cache.get("cold-" + round, DescriptorCacheHitRateTests::descriptor);
        }

        long readsBefore = cache.readCount();
        for (int round = 0; round < 50; round++) {
            cache.get("cold-" + round, DescriptorCacheHitRateTests::descriptor);
        }
        long coldReads = cache.readCount() - readsBefore;

        assertTrue("a cold tenant must not be reported as cached; " + coldReads + " of 50 reached the backend", coldReads >= 40);
    }
}
