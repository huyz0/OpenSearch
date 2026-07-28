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
import java.util.concurrent.atomic.AtomicLong;

/**
 * T3. What the descriptor cache does once it is full.
 *
 * <p>P8 bounded the cache the way H11 and H12 bounded theirs, with a capacity check on admission:
 *
 * <pre>{@code
 *   if (cache.size() < cacheCapacity) {
 *       cache.put(name, new CachedDescriptor(descriptor, now));
 *   }
 * }</pre>
 *
 * <p>Nothing else ever removes an entry, apart from an explicit invalidate on write. So the check is not an
 * eviction policy, it is a freeze: the first {@code cacheCapacity} names to be resolved hold every slot
 * permanently.
 *
 * <p><b>And the consequence is worse than new names going uncached.</b> An entry past its window is not
 * removed either, it is re-read and then <em>not</em> re-admitted, because the map is still full. So every
 * entry goes stale one second after it was written and can never be refreshed. Past the bound the cache
 * does not degrade to serving the first names only; it degrades to serving nothing at all, while still
 * holding the memory. That is this area's signature failure again, a mechanism that is present, bounded,
 * and silently does nothing.
 *
 * <p>This is measured rather than argued, and measured by counting reads rather than by asserting that a
 * cache exists, which is the P8 lesson.
 *
 * <p>The capacity is a constructor seam so the bound can be reached with a handful of descriptors instead
 * of fifty thousand. The behaviour under test is the admission rule, which does not care what the number is.
 */
public class DescriptorCacheCapacityIT extends OpenSearchIntegTestCase {

    /** Small enough to fill quickly, large enough that a per-name effect is not a rounding error. */
    private static final int CAPACITY = 20;

    private final AtomicLong now = new AtomicLong(1_000_000_000L);

    public void testTheCacheKeepsWorkingOnceItIsFull() {
        DescriptorStore store = new DescriptorStore(client(), 1, now::get, DescriptorStore.COLLAPSE_WAIT_MILLIS, CAPACITY);
        for (int i = 0; i < CAPACITY * 2; i++) {
            store.create(descriptor(name(i)));
        }

        // Fill the cache exactly to its bound.
        for (int i = 0; i < CAPACITY; i++) {
            assertNotNull(store.get(name(i)));
        }

        long readsForRepeatWhileFresh = readsFor(store, () -> resolveAll(store, 0, CAPACITY));

        // Past the window every entry must be re-read. That read is expected, and is not the finding.
        now.addAndGet(DescriptorStore.CACHE_TTL_NANOS + 1);
        long readsForFirstRefresh = readsFor(store, () -> resolveAll(store, 0, CAPACITY));

        // The finding: having just re-read all of them, are they cached again? With admission gated on a
        // full map, they are not, so this repeat costs another full set of reads rather than none.
        long readsForRepeatAfterRefresh = readsFor(store, () -> resolveAll(store, 0, CAPACITY));

        // A name the cache has never seen, while the map is full.
        long readsForNewNameTwice = readsFor(store, () -> {
            store.get(name(CAPACITY));
            store.get(name(CAPACITY));
        });

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT3 descriptor cache behaviour at capacity %d%n"
                    + "  %-46s %4d reads (want 0)%n"
                    + "  %-46s %4d reads (want %d, the window expiring)%n"
                    + "  %-46s %4d reads (want 0)%n"
                    + "  %-46s %4d reads (want 1)%n",
                CAPACITY,
                "repeat resolution inside the window",
                readsForRepeatWhileFresh,
                "first resolution after the window expires",
                readsForFirstRefresh,
                CAPACITY,
                "repeat resolution after that refresh",
                readsForRepeatAfterRefresh,
                "a new name resolved twice",
                readsForNewNameTwice
            )
        );

        assertEquals("inside the window a cached descriptor must not be re-read", 0, readsForRepeatWhileFresh);
        assertEquals("past the window every entry is legitimately re-read", CAPACITY, readsForFirstRefresh);
        assertEquals(
            "a descriptor just re-read must be cached again. Before T4 it was not, because admission was "
                + "gated on a map that never evicts, so past the bound the cache held memory and served "
                + "nothing",
            0,
            readsForRepeatAfterRefresh
        );
        assertEquals(
            "a name arriving after the bound is reached must still be cacheable, or every index created "
                + "after the first "
                + CAPACITY
                + " pays a read on every resolution forever",
            1,
            readsForNewNameTwice
        );
    }

    /**
     * The bound must hold, and it must hold under a frozen clock.
     *
     * <p>This is the regression test for a flaw in T4's own first version, found in review rather than by
     * running it. Recency was stamped with {@code System.nanoTime}, and eviction takes a threshold from the
     * sorted stamps and drops everything strictly below it. Entries admitted within one clock tick share a
     * stamp, so the threshold could equal every candidate and evict nothing, leaving the cache over its
     * bound and re-sorting every entry on every subsequent admission for no progress. Recency is now a
     * monotonic sequence, which is unique by construction.
     *
     * <p>A frozen clock is the worst case rather than an artificial one: it is what a coarse timer looks
     * like during a bulk warm-up, which is exactly when a cache fills.
     */
    public void testTheBoundHoldsEvenWhenEveryEntrySharesATimestamp() {
        DescriptorStore store = new DescriptorStore(client(), 1, now::get, DescriptorStore.COLLAPSE_WAIT_MILLIS, CAPACITY);
        int names = CAPACITY * 10;
        for (int i = 0; i < names; i++) {
            store.create(descriptor(name(i)));
        }

        // The clock never advances across this loop, so every entry is admitted at the same instant.
        for (int i = 0; i < names; i++) {
            assertNotNull(store.get(name(i)));
        }

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT4 bound under a frozen clock: %d names resolved, %d cached, %d evicted (capacity %d)%n",
                names,
                store.cachedCount(),
                store.evictionCount(),
                CAPACITY
            )
        );

        assertTrue(
            "the cache must stay near its bound. Resolving "
                + names
                + " names left "
                + store.cachedCount()
                + " cached against a capacity of "
                + CAPACITY
                + ", which means eviction made no progress",
            store.cachedCount() <= CAPACITY + 1
        );
        assertTrue("and it must have actually evicted something", store.evictionCount() > 0);
    }

    /**
     * T4b. The byte budget has to bind before the entry count does, for descriptors big enough to matter.
     *
     * <p>T4b measured a twenty alias descriptor at 5.8 times a typical one and a two hundred alias
     * descriptor at 51.6 times, which is why the bound is bytes. An entry count would let this population
     * occupy fifty times what the same count of ordinary descriptors would, and the count would report the
     * cache as being comfortably within its limit the whole time.
     *
     * <p>The capacity here is deliberately far above the number of names resolved, so if the byte budget
     * did nothing the cache would simply grow and this would fail.
     */
    public void testTheByteBudgetBindsBeforeTheEntryCount() {
        long budget = 64 * 1024;
        DescriptorStore store = new DescriptorStore(client(), 1, now::get, DescriptorStore.COLLAPSE_WAIT_MILLIS, 100_000, budget);

        int names = 200;
        for (int i = 0; i < names; i++) {
            store.create(heavilyAliasedDescriptor(name(i)));
        }
        for (int i = 0; i < names; i++) {
            assertNotNull(store.get(name(i)));
        }

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT4b byte budget: %d heavily aliased names resolved, %d cached, %,d bytes held against a "
                    + "%,d byte budget and a 100,000 entry capacity%n",
                names,
                store.cachedCount(),
                store.cachedBytes(),
                budget
            )
        );

        assertTrue(
            "the byte budget must bind. Held " + store.cachedBytes() + " bytes against a budget of " + budget,
            store.cachedBytes() <= budget
        );
        assertTrue(
            "and it must be the thing that bound, not the entry count, which was never approached: "
                + store.cachedCount()
                + " entries against a capacity of 100,000",
            store.cachedCount() < names
        );
    }

    /**
     * A descriptor larger than the whole budget is not cached, rather than being cached and never evicted.
     *
     * <p>Found reviewing T4b's own eviction rather than by running it, which is where T4's flaw came from
     * too. Eviction drops the stalest entries until the cache is under budget, bounded to ten passes. An
     * entry that alone exceeds the budget can never be evicted down to fit, so admitting it would leave the
     * cache permanently over budget and run the scan on every subsequent admission forever. Same CPU sink
     * as T4's equal-stamps case, reached a different way.
     *
     * <p>Refusing to cache it costs one read per resolution for one index. Admitting it costs the read path
     * a full scan per admission for every index.
     */
    public void testADescriptorLargerThanTheWholeBudgetIsNotCached() {
        // Far below one descriptor, which is a misconfiguration rather than a realistic setting. The point
        // is that the failure is bounded rather than that the setting is sensible.
        DescriptorStore store = new DescriptorStore(client(), 1, now::get, DescriptorStore.COLLAPSE_WAIT_MILLIS, 100, 32);
        store.create(heavilyAliasedDescriptor(name(0)));

        assertNotNull("the descriptor must still resolve, since not caching is not the same as not finding", store.get(name(0)));
        assertEquals("but nothing may be cached, or the cache is stuck over budget forever", 0, store.cachedCount());
        assertEquals("and the accounting must not drift", 0L, store.cachedBytes());

        // Every resolution is a real read, which is the cost of the refusal and is bounded and stated.
        long before = store.readCount();
        store.get(name(0));
        assertEquals("an uncacheable descriptor is re-read rather than served stale", before + 1, store.readCount());
    }

    /** Twenty plain aliases, the case T4b measured at 5.8 times a typical descriptor. */
    private static IndexDescriptor heavilyAliasedDescriptor(String name) {
        List<String> aliases = new java.util.ArrayList<>();
        for (int i = 0; i < 20; i++) {
            aliases.add(String.format(Locale.ROOT, "%s-alias-%04d-padding-to-a-realistic-length", name, i));
        }
        return new IndexDescriptor(
            name,
            name + "-uuid",
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            aliases,
            Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            1_700_000_000_000L
        );
    }

    private static void resolveAll(DescriptorStore store, int from, int to) {
        for (int i = from; i < to; i++) {
            assertNotNull(store.get(name(i)));
        }
    }

    private static long readsFor(DescriptorStore store, Runnable work) {
        long before = store.readCount();
        work.run();
        return store.readCount() - before;
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "capacity-idx-%04d", i);
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
