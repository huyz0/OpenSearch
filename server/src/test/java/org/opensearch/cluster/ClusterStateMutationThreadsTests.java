/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Phase C of core-pluggability-refactor-plan.md. Pins down the exact thread-name matching this class uses
 * to gate resolver consultation -- see the class's own javadoc for why this exists at all (the same
 * deadlock {@code AbsentIndexDescriptorSuppliers}/{@code AbsentIndexRoutingSuppliers} already solved).
 */
public class ClusterStateMutationThreadsTests extends OpenSearchTestCase {

    public void testOrdinaryThreadIsSafe() {
        assertFalse(ClusterStateMutationThreads.blockingIsUnsafeOnCurrentThread());
    }

    public void testClusterApplierUpdateThreadIsUnsafe() throws InterruptedException {
        assertUnsafeOnThreadNamed("opensearch[nodeA][clusterApplierService#updateTask][T#1]");
    }

    public void testClusterManagerUpdateThreadIsUnsafe() throws InterruptedException {
        assertUnsafeOnThreadNamed("opensearch[nodeA][clusterManagerService#updateTask][T#1]");
    }

    public void testMasterUpdateThreadIsUnsafe() throws InterruptedException {
        assertUnsafeOnThreadNamed("opensearch[nodeA][masterService#updateTask][T#1]");
    }

    public void testUnrelatedThreadNameContainingSimilarTextIsSafe() throws InterruptedException {
        // Must match by the exact documented substring, not by loose association -- a transport or generic
        // thread pool thread must never be refused, or every ordinary read path pays this for nothing.
        assertSafeOnThreadNamed("opensearch[nodeA][generic][T#1]");
    }

    private static void assertUnsafeOnThreadNamed(String threadName) throws InterruptedException {
        AtomicBoolean result = new AtomicBoolean();
        runOnThreadNamed(threadName, () -> result.set(ClusterStateMutationThreads.blockingIsUnsafeOnCurrentThread()));
        assertTrue("expected [" + threadName + "] to be treated as unsafe", result.get());
    }

    private static void assertSafeOnThreadNamed(String threadName) throws InterruptedException {
        AtomicBoolean result = new AtomicBoolean();
        runOnThreadNamed(threadName, () -> result.set(ClusterStateMutationThreads.blockingIsUnsafeOnCurrentThread()));
        assertFalse("expected [" + threadName + "] to be treated as safe", result.get());
    }

    private static void runOnThreadNamed(String threadName, Runnable runnable) throws InterruptedException {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                failure.set(t);
            }
        }, threadName);
        thread.start();
        thread.join();
        assertNull(failure.get());
    }
}
