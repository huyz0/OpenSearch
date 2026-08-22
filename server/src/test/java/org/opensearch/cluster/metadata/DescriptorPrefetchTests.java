/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * C1, both halves: the prefetch seam, and the refusal to do store I/O on a thread that cannot afford it.
 */
public class DescriptorPrefetchTests extends OpenSearchTestCase {

    @After
    public void clearSeams() {
        DescriptorPrefetch.register(null);
        AbsentIndexDescriptorSuppliers.register(null);
    }

    private static IndexDescriptor descriptor(String name) {
        return new IndexDescriptor(
            name,
            "uuid-" + name,
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
            0L
        );
    }

    // ------------------------------------------------------------------ the seam

    /** Unregistered it must be a no-op that still calls back, or every caller grows a null check. */
    public void testUnregisteredPrefetchCompletesImmediately() {
        AtomicBoolean called = new AtomicBoolean();
        DescriptorPrefetch.prefetch(Set.of("tenant-a"), ActionListener.wrap(ignored -> called.set(true), e -> fail("must not fail")));
        assertTrue(called.get());
        assertFalse(DescriptorPrefetch.isRegistered());
    }

    public void testRegisteredPrefetchReceivesTheWholeBatchAtOnce() {
        AtomicReference<Set<String>> seen = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        DescriptorPrefetch.register((names, listener) -> {
            calls.incrementAndGet();
            seen.set(Set.copyOf(names));
            listener.onResponse(null);
        });

        AtomicBoolean done = new AtomicBoolean();
        DescriptorPrefetch.prefetch(Set.of("a", "b", "c"), ActionListener.wrap(ignored -> done.set(true), e -> fail("must not fail")));

        assertTrue(done.get());
        assertEquals("the batch is the point; one call, not one per name", 1, calls.get());
        assertEquals(Set.of("a", "b", "c"), seen.get());
    }

    /**
     * The contract that keeps this an optimisation. A prefetch that failed must leave the request to
     * resolve inline, not fail it: otherwise a speculative read can break the thing it speeds up.
     */
    public void testAFailedPrefetchStillProceeds() {
        DescriptorPrefetch.register((names, listener) -> listener.onFailure(new RuntimeException("store is down")));

        AtomicBoolean proceeded = new AtomicBoolean();
        DescriptorPrefetch.prefetch(
            Set.of("tenant-a"),
            ActionListener.wrap(ignored -> proceeded.set(true), e -> fail("a failed prefetch must not fail the request"))
        );

        assertTrue(proceeded.get());
    }

    /** And one that throws rather than calling back is the same outcome, not a dropped request. */
    public void testAThrowingPrefetcherStillProceeds() {
        DescriptorPrefetch.register((names, listener) -> { throw new IllegalStateException("prefetcher bug"); });

        AtomicBoolean proceeded = new AtomicBoolean();
        DescriptorPrefetch.prefetch(
            Set.of("tenant-a"),
            ActionListener.wrap(ignored -> proceeded.set(true), e -> fail("a broken prefetcher must not fail the request"))
        );

        assertTrue(proceeded.get());
    }

    public void testAnEmptyBatchDoesNotReachThePrefetcher() {
        AtomicInteger calls = new AtomicInteger();
        DescriptorPrefetch.register((names, listener) -> {
            calls.incrementAndGet();
            listener.onResponse(null);
        });

        DescriptorPrefetch.prefetch(Set.of(), ActionListener.wrap(ignored -> {}, e -> fail("must not fail")));

        assertEquals(0, calls.get());
    }

    // ------------------------------------------------------------------ the guard

    public void testAnOrdinaryThreadResolvesNormally() {
        AbsentIndexDescriptorSuppliers.register(DescriptorPrefetchTests::descriptor);
        assertNotNull(AbsentIndexDescriptorSuppliers.supply("tenant-a"));
    }

    /**
     * W4's deadlock, made impossible rather than forbidden.
     *
     * <p>A supplier backed by a remote store, called from the cluster state applier thread, waits on the
     * thread it is blocking. T1 found six call sites that reach a supplier from there, which is few enough
     * to fix individually and too many to keep correct by discipline: the failure only appears on a cache
     * miss, which is the case testing does not produce.
     */
    public void testTheApplierThreadGetsAbsenceRatherThanAStall() throws Exception {
        AtomicInteger supplierCalls = new AtomicInteger();
        AbsentIndexDescriptorSuppliers.register(name -> {
            supplierCalls.incrementAndGet();
            return descriptor(name);
        });

        AtomicReference<IndexDescriptor> answer = new AtomicReference<>();
        runNamed("clusterApplierService#updateTask", () -> answer.set(AbsentIndexDescriptorSuppliers.supply("tenant-a")));

        assertNull("the applier thread must be told absent, not made to wait", answer.get());
        assertEquals("and the supplier must never be reached at all", 0, supplierCalls.get());
    }

    /** The cluster manager's update thread is the same shape, and is the serialised one besides. */
    public void testTheClusterManagerThreadGetsAbsenceToo() throws Exception {
        AbsentIndexDescriptorSuppliers.register(DescriptorPrefetchTests::descriptor);

        AtomicReference<IndexDescriptor> answer = new AtomicReference<>();
        runNamed("clusterManagerService#updateTask", () -> answer.set(AbsentIndexDescriptorSuppliers.supply("tenant-a")));

        assertNull(answer.get());
    }

    /**
     * The guard must not swallow a name that is genuinely resolvable off those threads, or a cluster would
     * lose descriptor resolution everywhere as soon as one unlucky thread name matched.
     */
    public void testAThreadWhoseNameMerelyMentionsClusterIsFine() throws Exception {
        AbsentIndexDescriptorSuppliers.register(DescriptorPrefetchTests::descriptor);

        AtomicReference<IndexDescriptor> answer = new AtomicReference<>();
        runNamed("opensearch[node][clusterStateWatcher][T#1]", () -> answer.set(AbsentIndexDescriptorSuppliers.supply("tenant-a")));

        assertNotNull("only the update threads are unsafe, not anything cluster-ish", answer.get());
    }

    private void runNamed(String threadName, Runnable body) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                body.run();
            } finally {
                done.countDown();
            }
        }, threadName);
        thread.start();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        thread.join();
    }
}
