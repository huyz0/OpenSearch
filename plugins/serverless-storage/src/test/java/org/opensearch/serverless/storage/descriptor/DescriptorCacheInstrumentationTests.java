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
import java.util.concurrent.atomic.AtomicLong;

/**
 * The counters the blob-backed design rests on.
 *
 * <p>Worth their own tests because a hit rate is the sort of number that is believed without being
 * checked, and the two ways a lookup avoids a backend trip are not equivalent: a fresh hit costs nothing,
 * a collapsed wait costs a full round trip somebody else is paying for. A cache reporting 99% while every
 * request blocks behind one cold read is not a cache that is working.
 */
public class DescriptorCacheInstrumentationTests extends OpenSearchTestCase {

    private static IndexDescriptor descriptor(String name) {
        return new IndexDescriptor(
            name,
            java.util.UUID.randomUUID().toString(),
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

    /** A clock the test advances by hand, so the freshness window is exercised without sleeping. */
    private static final class ManualClock {
        private final AtomicLong nanos = new AtomicLong();

        long get() {
            return nanos.get();
        }

        void advance(long by) {
            nanos.addAndGet(by);
        }
    }

    public void testHitRateIsNotANumberBeforeAnyLookup() {
        assertTrue(Double.isNaN(new DescriptorCache().hitRate()));
    }

    public void testFirstLookupIsAReadAndTheSecondIsAFreshHit() {
        ManualClock clock = new ManualClock();
        DescriptorCache cache = new DescriptorCache(clock::get, 1_000_000L, 3_000, 100, 1 << 20);
        AtomicLong loaderCalls = new AtomicLong();

        cache.get("tenant-a", name -> {
            loaderCalls.incrementAndGet();
            return descriptor(name);
        });
        cache.get("tenant-a", name -> {
            loaderCalls.incrementAndGet();
            return descriptor(name);
        });

        assertEquals("the loader must run exactly once", 1, loaderCalls.get());
        assertEquals(1, cache.readCount());
        assertEquals(1, cache.freshHitCount());
        assertEquals(0, cache.collapsedWaitCount());
        assertEquals(0.5, cache.hitRate(), 0.0);
    }

    public void testAnExpiredEntryIsReadAgainRatherThanCountedAsAHit() {
        ManualClock clock = new ManualClock();
        long ttl = 1_000L;
        DescriptorCache cache = new DescriptorCache(clock::get, ttl, 3_000, 100, 1 << 20);

        cache.get("tenant-a", DescriptorCacheInstrumentationTests::descriptor);
        clock.advance(ttl + 1);
        cache.get("tenant-a", DescriptorCacheInstrumentationTests::descriptor);

        assertEquals(2, cache.readCount());
        assertEquals(0, cache.freshHitCount());
        assertEquals(0.0, cache.hitRate(), 0.0);
    }

    /**
     * A miss is never cached, so repeated lookups of an absent name keep reading. Asserted because it is
     * the one case where a low hit rate is correct behaviour rather than a problem, and a reader of the
     * metric needs to know that.
     */
    public void testAbsentNamesNeverBecomeHits() {
        DescriptorCache cache = new DescriptorCache(System::nanoTime, DescriptorCache.DEFAULT_TTL_NANOS, 3_000, 100, 1 << 20);

        cache.get("never-created", name -> null);
        cache.get("never-created", name -> null);
        cache.get("never-created", name -> null);

        assertEquals(3, cache.readCount());
        assertEquals(0, cache.freshHitCount());
        assertEquals(0.0, cache.hitRate(), 0.0);
    }

    /**
     * A waiter that gives up on somebody else's read is counted apart from one that is served by it,
     * because the two mean opposite things about whether collapsing is working.
     */
    public void testGivingUpOnAnInFlightReadIsCountedAsAFallbackNotAHit() {
        DescriptorCache cache = new DescriptorCache(System::nanoTime, DescriptorCache.DEFAULT_TTL_NANOS, 1, 100, 1 << 20);
        cache.pretendReadIsInFlight("tenant-a");

        IndexDescriptor read = cache.get("tenant-a", DescriptorCacheInstrumentationTests::descriptor);

        assertNotNull("the fallback must still answer", read);
        assertEquals(1, cache.collapseFallbackCount());
        assertEquals("a fallback is a backend trip, not a saved one", 1, cache.readCount());
        assertEquals(0, cache.collapsedWaitCount());
        assertEquals(0, cache.freshHitCount());
    }

    /** Invalidation drops the entry, so the next lookup reads rather than serving a stale value. */
    public void testInvalidationTurnsTheNextLookupBackIntoARead() {
        ManualClock clock = new ManualClock();
        DescriptorCache cache = new DescriptorCache(clock::get, 1_000_000L, 3_000, 100, 1 << 20);

        cache.get("tenant-a", DescriptorCacheInstrumentationTests::descriptor);
        cache.invalidate("tenant-a");
        cache.get("tenant-a", DescriptorCacheInstrumentationTests::descriptor);

        assertEquals(2, cache.readCount());
        assertEquals(0, cache.freshHitCount());
    }
}
