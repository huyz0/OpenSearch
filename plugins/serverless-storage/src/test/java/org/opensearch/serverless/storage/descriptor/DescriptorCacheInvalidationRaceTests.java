/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.DescriptorUnavailableException;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * What happens to an invalidation that lands while a read of the same name is in flight.
 *
 * <h2>The gap this closes</h2>
 *
 * Every cross-node freshness mechanism in the descriptor plane terminates in {@link
 * DescriptorCache#invalidate}: the change tailer calls it for every name that changed anywhere in the
 * cluster, and the backend calls it after every write of its own. {@code invalidate} removed the cached
 * entry and nothing more, while {@link DescriptorCache#get}'s load admitted its result unconditionally --
 * so an invalidation arriving between the start of a read and its admission removed nothing and was then
 * overwritten by the very value it was meant to expel.
 *
 * <p>The sequential version of that -- delete, then read -- was already covered and passed, which is what
 * made the concurrent version look handled. It is not the same question. The dangerous ordering is the one
 * where the read is <em>older</em> than the delete and lands after it:
 *
 * <pre>
 * t0  a coordinator resolves "logs-a": cache miss, in-flight slot taken, readRegister issued
 * t1  another node deletes "logs-a": tombstone durable, live key deleted, change-log entry appended
 * t2  the tailer here reads the DELETED entry and invalidates -- removing nothing, since t0 has not
 *     admitted yet
 * t3  t0's read, issued before the delete, returns the live descriptor and caches it
 * t4  for the whole freshness window this node resolves a deleted index as OPEN and accepts
 *     acknowledged writes against a shard the cluster no longer believes in
 * </pre>
 */
public class DescriptorCacheInvalidationRaceTests extends OpenSearchTestCase {

    private static IndexDescriptor descriptor(String name) {
        return new IndexDescriptor(
            name,
            name + "-uuid",
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
     * The race, run in exactly the order above: the invalidation happens while the read is suspended
     * inside the loader, so there is no timing to be lucky about.
     */
    public void testAnInvalidationDuringAReadIsNotUndoneByThatReadsAdmission() throws Exception {
        DescriptorCache cache = new DescriptorCache(new AtomicLong()::get, Long.MAX_VALUE, 3_000, 100, Long.MAX_VALUE);
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch invalidated = new CountDownLatch(1);
        AtomicReference<IndexDescriptor> answered = new AtomicReference<>();
        AtomicReference<Throwable> failed = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try {
                answered.set(cache.get("logs-a", name -> {
                    readStarted.countDown();
                    try {
                        assertTrue(invalidated.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                    // What the store answers: the descriptor as it was before the delete, because this
                    // read was issued before it.
                    return descriptor(name);
                }));
            } catch (Throwable t) {
                failed.set(t);
            }
        });
        reader.start();
        assertTrue("the read must actually be in flight before the invalidation", readStarted.await(30, TimeUnit.SECONDS));

        cache.invalidate("logs-a");
        invalidated.countDown();
        reader.join(TimeUnit.SECONDS.toMillis(30));

        assertNull("the reader must not have failed: " + failed.get(), failed.get());
        assertNotNull("the reader still gets the answer its own read produced", answered.get());
        assertEquals("the invalidated name must not be left cached by the read it overtook", 0, cache.cachedCount());
        assertEquals("and the drop must be visible, not silent", 1, cache.staleAdmissionsDroppedCount());

        // The consequence the whole thing is about: the next resolution goes to the store, which now says
        // the index is gone, rather than being served the pre-delete value for a whole freshness window.
        assertNull(cache.get("logs-a", name -> null));
    }

    /** An invalidation with no read outstanding still evicts, and leaves nothing behind to leak. */
    public void testAnOrdinaryInvalidationStillEvictsAndKeepsNoState() {
        DescriptorCache cache = new DescriptorCache(new AtomicLong()::get, Long.MAX_VALUE, 3_000, 100, Long.MAX_VALUE);
        cache.get("logs-a", DescriptorCacheInvalidationRaceTests::descriptor);
        assertEquals(1, cache.cachedCount());

        // Invalidating names this node has never read is the tailer's normal traffic -- it sees every
        // change in the cluster -- so it must not accumulate per-name bookkeeping.
        for (int i = 0; i < 10_000; i++) {
            cache.invalidate("never-read-" + i);
        }
        cache.invalidate("logs-a");

        assertEquals(0, cache.cachedCount());
        assertEquals("no admission was dropped, because no read was in flight", 0, cache.staleAdmissionsDroppedCount());
        assertNotNull(cache.get("logs-a", DescriptorCacheInvalidationRaceTests::descriptor));
        assertEquals("the invalidated name must have been re-read rather than served from memory", 2, cache.readCount());
    }

    /**
     * A waiter behind a failed read is told the descriptor is unavailable, not that the index is absent.
     *
     * <p>Null means "there is no such index" to every caller of this class, and a client acting on that may
     * go on to create one that already exists. The reader already rethrew; the waiter laundered any
     * exception that was not a {@link DescriptorUnavailableException} into null, so a malformed descriptor
     * or a rejected execution became "no such index" for every thread collapsed behind it.
     */
    public void testAWaiterBehindAFailedReadIsToldUnavailableRatherThanAbsent() throws Exception {
        DescriptorCache cache = new DescriptorCache(new AtomicLong()::get, Long.MAX_VALUE, 60_000, 100, Long.MAX_VALUE);
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch waiterArrived = new CountDownLatch(1);
        AtomicReference<Throwable> readerRaised = new AtomicReference<>();
        AtomicReference<Throwable> waiterRaised = new AtomicReference<>();
        AtomicReference<Object> waiterAnswer = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try {
                cache.get("logs-a", name -> {
                    readStarted.countDown();
                    try {
                        assertTrue(waiterArrived.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    // Not a DescriptorUnavailableException: an ordinary bug, which is the case that was
                    // being reported as absence.
                    throw new IllegalStateException("malformed descriptor");
                });
            } catch (Throwable t) {
                readerRaised.set(t);
            }
        });
        reader.start();
        assertTrue(readStarted.await(30, TimeUnit.SECONDS));

        Thread waiter = new Thread(() -> {
            try {
                waiterAnswer.set(cache.get("logs-a", DescriptorCacheInvalidationRaceTests::descriptor));
            } catch (Throwable t) {
                waiterRaised.set(t);
            }
        });
        waiter.start();
        // Waited for rather than slept past: the waiter has to be parked on the in-flight slot before the
        // reader is allowed to fail, or it would find the slot already gone and issue its own successful
        // read, and the test would pass without ever exercising the path it is about. The collapse wait is
        // a minute, so once it is parked it stays parked until the reader completes.
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, waiter.getState()));
        waiterArrived.countDown();

        reader.join(TimeUnit.SECONDS.toMillis(30));
        waiter.join(TimeUnit.SECONDS.toMillis(30));

        assertNotNull("the reader must have failed", readerRaised.get());
        assertNull("a failure must never be answered as absence", waiterAnswer.get());
        assertNotNull("the waiter must have been told something", waiterRaised.get());
        assertTrue(
            "and what it is told must be unavailability: " + waiterRaised.get(),
            waiterRaised.get() instanceof DescriptorUnavailableException
        );
    }

    /**
     * Names admitted in one burst do not all expire in the same instant.
     *
     * <p>The jitter existed and was applied only to {@code getIfFresh}, the non-blocking peek, which issues
     * no read -- so the mitigation was absent from the one path it could mitigate. A node that warms T
     * tenants together (a mass reactivation, a prefetch over a wide bulk) re-read all T of them in the same
     * tick one window later, once per node per window.
     *
     * <p>Asserted as a spread rather than as a specific schedule: what matters is that the expiries do not
     * coincide, not where each one lands.
     */
    public void testEntriesAdmittedTogetherDoNotAllExpireTogether() {
        long window = 1_000_000L;
        AtomicLong clock = new AtomicLong();
        DescriptorCache cache = new DescriptorCache(clock::get, window, 3_000, 10_000, Long.MAX_VALUE);

        int names = 200;
        for (int i = 0; i < names; i++) {
            cache.get("serverless_tenant-" + i, DescriptorCacheInvalidationRaceTests::descriptor);
        }
        assertEquals(names, cache.readCount());

        // Just inside the nominal window: with a single shared window every one of these is a fresh hit.
        clock.set((long) (window * 0.95));
        for (int i = 0; i < names; i++) {
            cache.get("serverless_tenant-" + i, DescriptorCacheInvalidationRaceTests::descriptor);
        }

        long rereads = cache.readCount() - names;
        assertTrue("some of the burst must have expired before the nominal window: " + rereads, rereads > 0);
        assertTrue("but not all of it, or the window is not being honoured at all: " + rereads, rereads < names);
    }

    /** The jittered window never exceeds the configured one, so the TTL stays a real ceiling on staleness. */
    public void testTheJitterOnlyEverShortensTheWindow() {
        long window = 1_000L;
        AtomicLong clock = new AtomicLong();
        DescriptorCache cache = new DescriptorCache(clock::get, window, 3_000, 10_000, Long.MAX_VALUE);

        for (int i = 0; i < 200; i++) {
            cache.get("serverless_tenant-" + i, DescriptorCacheInvalidationRaceTests::descriptor);
        }
        clock.set(window + 1);
        for (int i = 0; i < 200; i++) {
            cache.get("serverless_tenant-" + i, DescriptorCacheInvalidationRaceTests::descriptor);
        }
        assertEquals("past the configured window, every entry must be re-read", 400, cache.readCount());
    }
}
