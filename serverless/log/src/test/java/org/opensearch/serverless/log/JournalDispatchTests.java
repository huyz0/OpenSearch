/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JournalDispatchTests extends OpenSearchTestCase {
    public void testNativeCompletionDoesNotRunContinuationsUnderItsMonitor() throws Exception {
        final var limits = limits(1);
        final var admission = new JournalAdmission(limits);
        final var dispatch = new JournalDispatch("monitor", limits);
        final var source = new CompletableFuture<Integer>();
        final var submitted = new CountDownLatch(1);
        final Object monitor = new Object();
        try {
            final var result = dispatch.submit(admission.acquire(0), () -> {
                submitted.countDown();
                return source;
            }).thenApply(value -> {
                assertFalse(Thread.holdsLock(monitor));
                synchronized (monitor) {
                    return value;
                }
            });
            assertTrue(submitted.await(10, TimeUnit.SECONDS));
            synchronized (monitor) {
                source.complete(7);
            }
            assertEquals(7, (int) result.toCompletableFuture().get(10, TimeUnit.SECONDS));
        } finally {
            dispatch.close();
            assertTrue(dispatch.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testCallerCancellationDoesNotReleasePersistenceBudgetAndCloseDrainsLateReply() throws Exception {
        final var limits = limits(1);
        final var admission = new JournalAdmission(limits);
        final var dispatch = new JournalDispatch("cancel", limits);
        final var source = new CompletableFuture<Integer>();
        final var submitted = new CountDownLatch(1);
        try {
            final var result = dispatch.submit(admission.acquire(0), () -> {
                submitted.countDown();
                return source;
            });
            assertTrue(submitted.await(10, TimeUnit.SECONDS));
            assertTrue(result.toCompletableFuture().cancel(false));
            assertEquals(1, admission.pendingRequests());
            expectThrows(IOException.class, () -> admission.acquire(0));
            dispatch.close();
            source.complete(9);
            assertEquals(9, (int) result.toCompletableFuture().get(10, TimeUnit.SECONDS));
        } finally {
            source.complete(9);
            dispatch.close();
            assertTrue(dispatch.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertEquals(0, admission.pendingRequests());
    }

    public void testDependentSubmissionUsesTheReleasedPermit() throws Exception {
        final var limits = limits(1);
        final var admission = new JournalAdmission(limits);
        final var dispatch = new JournalDispatch("dependent", limits);
        try {
            final var result = dispatch.submit(admission.acquire(0), () -> CompletableFuture.completedFuture(1)).thenCompose(value -> {
                try {
                    return dispatch.submit(admission.acquire(0), () -> CompletableFuture.completedFuture(value + 1));
                } catch (IOException failure) {
                    return CompletableFuture.<Integer>failedFuture(failure);
                }
            });
            assertEquals(2, (int) result.toCompletableFuture().get(10, TimeUnit.SECONDS));
        } finally {
            dispatch.close();
            assertTrue(dispatch.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertEquals(0, admission.pendingRequests());
    }

    public void testFullWindowWithSlowContinuationsCannotBlockTheNativeCompletingThread() throws Exception {
        final int window = 16;
        final var limits = limits(window);
        final var admission = new JournalAdmission(limits);
        final var dispatch = new JournalDispatch("window", limits);
        final var submitted = new CountDownLatch(window);
        final var entered = new CountDownLatch(4);
        final var release = new CountDownLatch(1);
        final var sources = new ArrayList<CompletableFuture<Integer>>();
        final var results = new ArrayList<CompletableFuture<Integer>>();
        try {
            for (int i = 0; i < window; i++) {
                final var source = new CompletableFuture<Integer>();
                sources.add(source);
                results.add(dispatch.submit(admission.acquire(0), () -> {
                    submitted.countDown();
                    return source;
                }).thenApply(value -> {
                    entered.countDown();
                    try {
                        if (release.await(10, TimeUnit.SECONDS) == false) {
                            throw new AssertionError("test did not release the continuation");
                        }
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(failure);
                    }
                    return value;
                }).toCompletableFuture());
            }
            assertTrue(submitted.await(10, TimeUnit.SECONDS));
            for (var source : sources) {
                source.complete(1);
            }
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            for (int i = 0; i < 4; i++) {
                results.add(dispatch.submit(admission.acquire(0), () -> CompletableFuture.completedFuture(1)).toCompletableFuture());
            }
            assertEquals(window, admission.pendingRequests());
            expectThrows(IOException.class, () -> admission.acquire(0));
            release.countDown();
            CompletableFuture.allOf(results.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
            for (var result : results) {
                assertEquals(1, (int) result.get());
            }
        } finally {
            release.countDown();
            sources.forEach(source -> source.complete(1));
            dispatch.close();
            assertTrue(dispatch.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertEquals(0, admission.pendingRequests());
    }

    public void testClosedDispatcherRefusesWithoutCallingTheSource() throws Exception {
        final var limits = limits(1);
        final var admission = new JournalAdmission(limits);
        final var dispatch = new JournalDispatch("closed", limits);
        final var called = new AtomicBoolean();
        dispatch.close();
        expectThrows(IOException.class, () -> dispatch.submit(admission.acquire(0), () -> {
            called.set(true);
            return CompletableFuture.completedFuture(1);
        }));
        assertFalse(called.get());
        assertEquals(0, admission.pendingRequests());
        assertTrue(dispatch.awaitTermination(10, TimeUnit.SECONDS));
    }

    public void testSynchronousSourceFailureReleasesTheBudget() throws Exception {
        final var limits = limits(1);
        final var admission = new JournalAdmission(limits);
        final var dispatch = new JournalDispatch("failure", limits);
        try {
            final var result = dispatch.submit(
                admission.acquire(0),
                () -> { throw new IOException("injected failure before native submission"); }
            );
            final var failure = expectThrows(ExecutionException.class, () -> result.toCompletableFuture().get(10, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof IOException);
        } finally {
            dispatch.close();
            assertTrue(dispatch.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertEquals(0, admission.pendingRequests());
    }

    private static JournalConfiguration.Limits limits(int requests) {
        final var initial = JournalConfiguration.Limits.defaults();
        return new JournalConfiguration.Limits(
            initial.maximumGroups(),
            initial.maximumPayloadBytes(),
            requests,
            initial.maximumPendingBytes(),
            initial.queueBytes(),
            initial.segmentBytes(),
            initial.cachedSegmentBytes(),
            initial.maximumRetainedEntries(),
            initial.minimumFreeBytes()
        );
    }
}
