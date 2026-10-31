/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.get.GetRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.shell.ActionGate;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ActionGateAsyncTests extends OpenSearchTestCase {
    private static final String ACTION = "test:async-gate";

    private static ActionGate gate(PolicyFilterPlugin plugin) {
        return new ActionGate(plugin.getActionFilters());
    }

    public void testNetworkWorkMovesToAWorkerWithItsRequestContext() throws Exception {
        final var pool = new org.opensearch.threadpool.TestThreadPool("async-gate-dispatch");
        try {
            for (boolean filtered : new boolean[] { false, true }) {
                final var gate = filtered ? gate(PolicyFilterPlugin.request(ACTION, request -> request)) : new ActionGate(List.of());
                final var completion = PlainActionFuture.<AcknowledgedResponse>newFuture();
                onNetworkThread(() -> {
                    try (var context = pool.getThreadContext().stashContext()) {
                        pool.getThreadContext().putHeader("request-marker", "preserved");
                        gate.runAsync(pool, ACTION, new GetRequest("index", "item"), (request, listener) -> {
                            assertFalse(org.opensearch.transport.Transports.isTransportThread(Thread.currentThread()));
                            assertEquals("preserved", pool.getThreadContext().getHeader("request-marker"));
                            listener.onResponse(new AcknowledgedResponse(true));
                        }, ActionGate.identity(AcknowledgedResponse.class), completion);
                    }
                });
                assertTrue(completion.get(10, TimeUnit.SECONDS).isAcknowledged());
            }
        } finally {
            assertTrue(org.opensearch.threadpool.ThreadPool.terminate(pool, 10, TimeUnit.SECONDS));
        }
    }

    public void testWorkerFailurePreservesTheCheckedException() throws Exception {
        final var pool = new org.opensearch.threadpool.TestThreadPool("async-gate-failure");
        try {
            for (boolean filtered : new boolean[] { false, true }) {
                final var gate = filtered ? gate(PolicyFilterPlugin.request(ACTION, request -> request)) : new ActionGate(List.of());
                final var completion = PlainActionFuture.<AcknowledgedResponse>newFuture();
                final var failure = new IOException("store unavailable");
                onNetworkThread(
                    () -> gate.runAsync(
                        pool,
                        ACTION,
                        new GetRequest("index", "item"),
                        (request, listener) -> { throw failure; },
                        ActionGate.identity(AcknowledgedResponse.class),
                        completion
                    )
                );
                assertSame(failure, expectThrows(ExecutionException.class, () -> completion.get(10, TimeUnit.SECONDS)).getCause());
            }
        } finally {
            assertTrue(org.opensearch.threadpool.ThreadPool.terminate(pool, 10, TimeUnit.SECONDS));
        }
    }

    public void testRejectedWorkerFailsCompletionWithoutStartingWork() throws Exception {
        final var rejected = Executors.newSingleThreadExecutor();
        rejected.shutdown();
        final var pool = org.mockito.Mockito.mock(org.opensearch.threadpool.ThreadPool.class);
        org.mockito.Mockito.when(pool.executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC)).thenReturn(rejected);
        try {
            for (boolean filtered : new boolean[] { false, true }) {
                final var gate = filtered ? gate(PolicyFilterPlugin.request(ACTION, request -> request)) : new ActionGate(List.of());
                final var completion = PlainActionFuture.<AcknowledgedResponse>newFuture();
                final var started = new AtomicBoolean();
                onNetworkThread(() -> gate.runAsync(pool, ACTION, new GetRequest("index", "item"), (request, listener) -> {
                    started.set(true);
                    listener.onResponse(new AcknowledgedResponse(true));
                }, ActionGate.identity(AcknowledgedResponse.class), completion));
                final var failure = expectThrows(ExecutionException.class, () -> completion.get(10, TimeUnit.SECONDS)).getCause();
                assertTrue(failure.toString(), failure instanceof java.util.concurrent.RejectedExecutionException);
                assertFalse(started.get());
            }
        } finally {
            rejected.shutdownNow();
            assertTrue(rejected.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static void onNetworkThread(org.opensearch.common.CheckedRunnable<Exception> work) throws Exception {
        final var executor = Executors.newSingleThreadExecutor(
            task -> new Thread(task, org.opensearch.transport.Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX + "-gate-dispatch")
        );
        try {
            executor.submit(() -> {
                work.run();
                return null;
            }).get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testWaitingForAsyncWorkDoesNotHoldTheCallingWorker() throws Exception {
        for (boolean filtered : new boolean[] { false, true }) {
            final var gate = filtered ? gate(PolicyFilterPlugin.request(ACTION, request -> request)) : new ActionGate(List.of());
            final var completion = PlainActionFuture.<AcknowledgedResponse>newFuture();
            final var work = new AtomicReference<ActionListener<AcknowledgedResponse>>();
            final ActionGate.ResponseView<AcknowledgedResponse> view = filtered
                ? ActionGate.identity(AcknowledgedResponse.class)
                : new ActionGate.ResponseView<>() {
                    @Override
                    public org.opensearch.core.action.ActionResponse show(AcknowledgedResponse result) {
                        throw new AssertionError("an empty chain must not build native response views");
                    }

                    @Override
                    public AcknowledgedResponse read(org.opensearch.core.action.ActionResponse response, AcknowledgedResponse original) {
                        throw new AssertionError("an empty chain must not read native response views");
                    }
                };
            final var executor = Executors.newSingleThreadExecutor();
            try {
                final var returned = executor.submit(
                    () -> gate.runAsync(
                        ACTION,
                        new GetRequest("original", "item"),
                        (request, listener) -> work.set(listener),
                        view,
                        completion
                    )
                );
                returned.get(10, TimeUnit.SECONDS);
                assertNotNull(work.get());
                assertFalse(completion.isDone());
                final var actual = new AcknowledgedResponse(true);
                work.get().onResponse(actual);
                assertSame(actual, completion.get(10, TimeUnit.SECONDS));
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }

    public void testAdmittedRequestAndFilteredResponseAreApplied() throws Exception {
        final var plugin = new PolicyFilterPlugin(
            ACTION,
            request -> new GetRequest("selected", "chosen"),
            response -> new AcknowledgedResponse(false),
            null
        );
        final var completion = PlainActionFuture.<AcknowledgedResponse>newFuture();
        gate(plugin).runAsync(ACTION, new GetRequest("original", "item"), (request, listener) -> {
            final var selected = (GetRequest) request;
            assertEquals("selected", selected.index());
            assertEquals("chosen", selected.id());
            listener.onResponse(new AcknowledgedResponse(true));
        }, ActionGate.identity(AcknowledgedResponse.class), completion);
        assertFalse(completion.get(10, TimeUnit.SECONDS).isAcknowledged());
    }

    public void testCachedReplyDoesNotStartWork() throws Exception {
        final var cached = new AcknowledgedResponse(false);
        final var started = new AtomicBoolean();
        final var completion = PlainActionFuture.<AcknowledgedResponse>newFuture();
        gate(PolicyFilterPlugin.cached(ACTION, cached)).runAsync(
            ACTION,
            new GetRequest("original", "item"),
            (request, listener) -> started.set(true),
            ActionGate.identity(AcknowledgedResponse.class),
            completion
        );
        assertSame(cached, completion.get(10, TimeUnit.SECONDS));
        assertFalse(started.get());
    }

    public void testDelayedWorkFailurePreservesTheCheckedException() throws Exception {
        final var completion = PlainActionFuture.<AcknowledgedResponse>newFuture();
        final var work = new AtomicReference<ActionListener<AcknowledgedResponse>>();
        gate(PolicyFilterPlugin.request(ACTION, request -> request)).runAsync(
            ACTION,
            new GetRequest("original", "item"),
            (request, listener) -> work.set(listener),
            ActionGate.identity(AcknowledgedResponse.class),
            completion
        );
        assertFalse(completion.isDone());
        final var failure = new IOException("backend unavailable");
        work.get().onFailure(failure);
        assertSame(failure, expectThrows(ExecutionException.class, () -> completion.get(10, TimeUnit.SECONDS)).getCause());
    }

    public void testSynchronousWorkFailurePreservesTheCheckedException() throws Exception {
        final var failure = new IOException("could not start");
        final var completion = PlainActionFuture.<AcknowledgedResponse>newFuture();
        gate(PolicyFilterPlugin.request(ACTION, request -> request)).runAsync(
            ACTION,
            new GetRequest("original", "item"),
            (request, listener) -> {
                throw failure;
            },
            ActionGate.identity(AcknowledgedResponse.class),
            completion
        );
        assertSame(failure, expectThrows(ExecutionException.class, () -> completion.get(10, TimeUnit.SECONDS)).getCause());
    }

    public void testFilterDenialDoesNotStartWork() throws Exception {
        final var failure = new org.opensearch.OpenSearchSecurityException("denied", org.opensearch.core.rest.RestStatus.FORBIDDEN);
        final var started = new AtomicBoolean();
        final var completion = PlainActionFuture.<AcknowledgedResponse>newFuture();
        gate(PolicyFilterPlugin.request(ACTION, request -> { throw failure; })).runAsync(
            ACTION,
            new GetRequest("original", "item"),
            (request, listener) -> started.set(true),
            ActionGate.identity(AcknowledgedResponse.class),
            completion
        );
        assertSame(failure, expectThrows(ExecutionException.class, () -> completion.get(10, TimeUnit.SECONDS)).getCause());
        assertFalse(started.get());
    }

    public void testResponseDenialPrecedesCompletion() throws Exception {
        final var completion = PlainActionFuture.<AcknowledgedResponse>newFuture();
        final var work = new AtomicReference<ActionListener<AcknowledgedResponse>>();
        gate(PolicyFilterPlugin.denyAfter(ACTION)).runAsync(
            ACTION,
            new GetRequest("original", "item"),
            (request, listener) -> work.set(listener),
            ActionGate.identity(AcknowledgedResponse.class),
            completion
        );
        assertFalse(completion.isDone());
        work.get().onResponse(new AcknowledgedResponse(true));
        final var denied = expectThrows(ExecutionException.class, () -> completion.get(10, TimeUnit.SECONDS)).getCause();
        assertEquals(org.opensearch.core.rest.RestStatus.FORBIDDEN, ((org.opensearch.OpenSearchSecurityException) denied).status());
    }
}
