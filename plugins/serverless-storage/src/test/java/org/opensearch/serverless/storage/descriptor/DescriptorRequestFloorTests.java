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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The floor under object-store requests: what a node costs when nothing is happening.
 *
 * <h2>Why a floor rather than a rate</h2>
 *
 * Caching is usually discussed as a hit rate, and hit rate is the wrong frame for this cost. A descriptor
 * cache with a freshness window re-reads every entry once per window whether or not anyone asked, so a node
 * holding T active tenants issues T reads per window <em>regardless of the request rate above it</em>. At a
 * one second window that is T reads per second, forever, on an idle cluster.
 *
 * <p>That is a floor, not a rate, and it is the number that decides whether 100M tenants is affordable. S3
 * allows roughly 5,500 GET per second per prefix; at a one second window a single node holding ten thousand
 * warm tenants exceeds that on its own, before any request is served.
 *
 * <h2>What is asserted</h2>
 *
 * Reads per tenant per window, counted rather than timed, by advancing a fake clock instead of sleeping. The
 * window is a constructor parameter, so the test can hold the population fixed and vary only the window, and
 * show that the request count falls in proportion. That is the whole claim: the window is the lever.
 *
 * <p>This exists because the lever was already built and then not pulled. {@code DescriptorCache} made the
 * window a parameter, and said in its own javadoc that one second is "right for a local index, wrong by two
 * orders of magnitude for an object store". {@code BlobDescriptorBackend} then called the no-argument
 * constructor and took the one second anyway.
 */
public class DescriptorRequestFloorTests extends OpenSearchTestCase {

    private static final int TENANTS = 40;

    private static IndexDescriptor descriptor(String name) {
        return IndexDescriptor.from(
            IndexMetadata.builder(name)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                        .build()
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build()
        );
    }

    private static String tenant(int i) {
        return String.format(Locale.ROOT, "serverless_tenant-%04d", i);
    }

    /**
     * Loader invocations while resolving every tenant repeatedly over a fixed span of simulated time.
     *
     * <p>Counted at the loader rather than at a blob container on purpose. The loader is exactly what
     * {@code BlobDescriptorBackend} passes to the cache, and one invocation is one store read, so counting
     * here measures the property without a filesystem in the way. The clock is driven rather than waited on,
     * so this measures the window and not the machine.
     *
     * <p>Every tenant is resolved on every tick, which is the case the floor describes: continuous traffic
     * across the whole working set.
     */
    private long loadsOverSpan(long windowNanos, long spanNanos, long tickNanos) {
        final long[] now = { 0L };
        AtomicLong loads = new AtomicLong();
        DescriptorCache cache = new DescriptorCache(
            () -> now[0],
            windowNanos,
            DescriptorCache.DEFAULT_COLLAPSE_WAIT_MILLIS,
            DescriptorCache.DEFAULT_CAPACITY,
            DescriptorCache.DEFAULT_BYTES
        );

        for (long elapsed = 0; elapsed < spanNanos; elapsed += tickNanos) {
            now[0] = elapsed;
            for (int i = 0; i < TENANTS; i++) {
                final String name = tenant(i);
                assertNotNull(cache.get(name, n -> {
                    loads.incrementAndGet();
                    return descriptor(n);
                }));
            }
        }
        return loads.get();
    }

    /**
     * A longer window costs proportionally fewer reads for the same traffic over the same time.
     *
     * <p>Ten seconds of traffic against a one second window is ten refreshes per tenant; against a five
     * second window it is two. The ratio is what matters and it is exact, because the clock is driven.
     */
    public void testTheWindowIsTheLever() throws Exception {
        long tenSeconds = TimeUnit.SECONDS.toNanos(10);
        long tick = TimeUnit.MILLISECONDS.toNanos(200);

        long atOneSecond = loadsOverSpan(TimeUnit.SECONDS.toNanos(1), tenSeconds, tick);
        long atFiveSeconds = loadsOverSpan(TimeUnit.SECONDS.toNanos(5), tenSeconds, tick);

        logger.info("reads over 10s of traffic across {} tenants: {} at a 1s window, {} at 5s", TENANTS, atOneSecond, atFiveSeconds);

        assertEquals("ten one-second windows, one read per tenant per window", 10L * TENANTS, atOneSecond);
        assertEquals("two five-second windows", 2L * TENANTS, atFiveSeconds);
    }

    /**
     * The floor is set by the window and the population, and not by how hard the node is being asked.
     *
     * <p>This is the property that makes it a floor: ten times the traffic over the same span costs the same
     * number of reads. Anyone reasoning about this cost from a hit rate would expect otherwise.
     */
    public void testTheFloorDoesNotDependOnRequestRate() throws Exception {
        long tenSeconds = TimeUnit.SECONDS.toNanos(10);
        long window = TimeUnit.SECONDS.toNanos(1);

        long atLowRate = loadsOverSpan(window, tenSeconds, TimeUnit.SECONDS.toNanos(1));
        long atHighRate = loadsOverSpan(window, tenSeconds, TimeUnit.MILLISECONDS.toNanos(100));

        assertEquals("ten times the requests, identical store reads", atLowRate, atHighRate);
    }

    /** The shipped default is a minute, not the cache's own one second. */
    public void testTheDefaultWindowIsAMinute() {
        assertEquals(TimeUnit.SECONDS.toNanos(60), BlobDescriptorBackend.DEFAULT_CACHE_TTL_NANOS);
        assertTrue(
            "the shipped default must be longer than the cache's local-index default, which is what this fixed",
            BlobDescriptorBackend.DEFAULT_CACHE_TTL_NANOS > DescriptorCache.DEFAULT_TTL_NANOS
        );
    }
}
