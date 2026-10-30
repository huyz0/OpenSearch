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
