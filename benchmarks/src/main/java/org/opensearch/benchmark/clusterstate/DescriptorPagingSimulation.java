/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.clusterstate;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Spike S5: a pure simulation of the tiered descriptor design, with no OpenSearch dependency.
 *
 * <p>The design under evaluation keeps per-index routing descriptors out of every node's heap by
 * grouping them into pages held in an object store, with only a small page index resident and a
 * bounded LRU of hot pages cached locally. Two costs decide whether that is affordable, and
 * neither was modelled before this: how many object-store GETs the cache miss path generates
 * under a realistic tenant access distribution, and how much write amplification page-granular
 * updates cause when a single descriptor changes.
 *
 * <p>Access is modelled as Zipf (a few tenants very hot, a long tail nearly idle) versus uniform.
 * Uniform is the adversarial case: it defeats caching entirely and is the one that would make the
 * design too expensive. Real multi-tenant fleets are typically Zipf-ish, but the design should not
 * depend on that without knowing the margin.
 *
 * <p>Run standalone:
 *
 * <pre>{@code
 * java -cp <benchmarks runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.DescriptorPagingSimulation
 * }</pre>
 */
public final class DescriptorPagingSimulation {

    private DescriptorPagingSimulation() {}

    /** Measured by spike S2: a no-alias routing descriptor's retained heap. */
    private static final int DESCRIPTOR_HEAP_BYTES = 289;
    /** Serialized form is materially smaller than the deserialized graph; used for page sizing. */
    private static final int DESCRIPTOR_WIRE_BYTES = 120;

    private static final long TENANTS = 100_000_000L;
    private static final int REQUESTS = 2_000_000;

    // S3 GET/PUT list pricing, US-East-1 S3 Standard, per 1,000 requests.
    private static final double GET_COST_PER_1000 = 0.0004;
    private static final double PUT_COST_PER_1000 = 0.005;

    public static void main(String[] args) {
        System.out.println("=== S5: descriptor paging simulation ===");
        System.out.println("tenants=" + TENANTS + ", sampledRequests=" + REQUESTS);
        System.out.println("descriptor: " + DESCRIPTOR_HEAP_BYTES + " B heap (S2 measured), " + DESCRIPTOR_WIRE_BYTES + " B wire");
        System.out.println();

        for (int descriptorsPerPage : new int[] { 100, 1_000, 10_000 }) {
            for (int cachedPages : new int[] { 1_000, 10_000 }) {
                simulateReads("zipf", descriptorsPerPage, cachedPages);
                simulateReads("uniform", descriptorsPerPage, cachedPages);
            }
        }

        System.out.println();
        System.out.println("=== write amplification ===");
        System.out.println(
            "descriptorsPerPage | updateRate/s | naiveRewriteMB/s | naivePUT/s | naive$/mo | withDeltaLogPUT/s | withDelta$/mo"
        );
        for (int descriptorsPerPage : new int[] { 100, 1_000, 10_000 }) {
            for (int updatesPerSec : new int[] { 100, 1_000, 10_000 }) {
                simulateWrites(descriptorsPerPage, updatesPerSec);
            }
        }
    }

    private static void simulateReads(String distribution, int descriptorsPerPage, int cachedPages) {
        long totalPages = (TENANTS + descriptorsPerPage - 1) / descriptorsPerPage;
        // Cache can't hold more pages than exist.
        long effectiveCachedPages = Math.min(cachedPages, totalPages);

        Random random = new Random(42);
        LruSet cache = new LruSet((int) effectiveCachedPages);
        long hits = 0;
        long misses = 0;

        for (int i = 0; i < REQUESTS; i++) {
            long tenant = distribution.equals("zipf") ? zipfTenant(random) : (long) (random.nextDouble() * TENANTS);
            long page = tenant / descriptorsPerPage;
            if (cache.access(page)) {
                hits++;
            } else {
                misses++;
            }
        }

        double hitRate = (double) hits / (hits + misses);
        int pageWireBytes = descriptorsPerPage * DESCRIPTOR_WIRE_BYTES;
        // Heap held by the cache assumes a fetched page is deserialized and retained.
        double cacheHeapGb = effectiveCachedPages * (double) descriptorsPerPage * DESCRIPTOR_HEAP_BYTES / (1024 * 1024 * 1024);

        // Scale the sampled miss ratio to a sustained request rate to get a monthly cost.
        double missRatio = (double) misses / REQUESTS;
        double getsPerSecAt10k = missRatio * 10_000;
        double costPerMonth = getsPerSecAt10k * 2_592_000 / 1000 * GET_COST_PER_1000;

        System.out.printf(
            java.util.Locale.ROOT,
            "%-8s pagesOf=%-6d cached=%-6d hitRate=%6.2f%%  pageWire=%6dKB  cacheHeap=%7.2fGB  GET/s@10kRPS=%8.1f  $/mo=%9.2f%n",
            distribution,
            descriptorsPerPage,
            (int) effectiveCachedPages,
            hitRate * 100,
            pageWireBytes / 1024,
            cacheHeapGb,
            getsPerSecAt10k,
            costPerMonth
        );
    }

    /**
     * Zipf-ish tenant selection: most requests concentrate on a small hot set. Uses an inverse
     * power-law transform rather than a full Zipf table so it stays cheap at 10^8 tenants.
     */
    private static long zipfTenant(Random random) {
        double u = random.nextDouble();
        // exponent ~1.1: roughly 90% of traffic to the hottest ~1% of tenants
        double x = Math.pow(u, 10.0);
        return (long) (x * TENANTS);
    }

    private static void simulateWrites(int descriptorsPerPage, int updatesPerSec) {
        // Naive: any descriptor change rewrites its whole page.
        // Distinct pages touched per second, assuming updates spread over the whole keyspace.
        long totalPages = (TENANTS + descriptorsPerPage - 1) / descriptorsPerPage;
        double distinctPagesPerSec = expectedDistinct(totalPages, updatesPerSec);
        double naiveBytesPerSec = distinctPagesPerSec * descriptorsPerPage * DESCRIPTOR_WIRE_BYTES;
        double naiveMbPerSec = naiveBytesPerSec / (1024 * 1024);
        double naiveCost = distinctPagesPerSec * 2_592_000 / 1000 * PUT_COST_PER_1000;

        // With a per-page delta log: one small append per update, plus a periodic compaction of
        // each dirty page. Compaction assumed once per 100 updates to that page.
        double compactionsPerSec = distinctPagesPerSec / 100.0;
        double deltaPutsPerSec = updatesPerSec + compactionsPerSec;
        double deltaCost = deltaPutsPerSec * 2_592_000 / 1000 * PUT_COST_PER_1000;

        System.out.printf(
            java.util.Locale.ROOT,
            "%-6d | %-10d | %14.2f | %10.1f | %9.0f | %17.1f | %12.0f%n",
            descriptorsPerPage,
            updatesPerSec,
            naiveMbPerSec,
            distinctPagesPerSec,
            naiveCost,
            deltaPutsPerSec,
            deltaCost
        );
    }

    /** Expected number of distinct pages hit by k uniform draws over n pages. */
    private static double expectedDistinct(long n, int k) {
        return n * (1 - Math.pow(1 - 1.0 / n, k));
    }

    /** Minimal LRU membership set; returns true on hit. */
    private static final class LruSet {
        private final LinkedHashMap<Long, Boolean> map;

        LruSet(int capacity) {
            this.map = new LinkedHashMap<>(Math.min(capacity, 1 << 16), 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
                    return size() > capacity;
                }
            };
        }

        boolean access(long key) {
            Boolean present = map.get(key);
            if (present != null) {
                return true;
            }
            map.put(key, Boolean.TRUE);
            return false;
        }
    }

    @SuppressWarnings("unused")
    private static final Map<String, String> UNUSED = new HashMap<>();
}
