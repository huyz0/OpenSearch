/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.serverless.shell.ActionGate;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ActionGateFailureTests extends OpenSearchTestCase {
    private static ActionFilter filter(Exception failure, CountDownLatch waiting) {
        return new ActionFilter() {
            @Override
            public int order() {
                return 0;
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionRequestMetadata<Request, Response> metadata,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                if (waiting != null) {
                    waiting.countDown();
                } else if (failure != null) {
                    listener.onFailure(failure);
                } else {
                    chain.proceed(task, action, request, listener);
                }
            }
        };
    }

    public void testCheckedOperationFailureDoesNotChangeWhenAFilterIsInstalled() throws Exception {
        final var failure = new IOException("backend unavailable");
        for (var filters : List.of(List.<ActionFilter>of(), List.of(filter(null, null)))) {
            final var gate = new ActionGate(filters);
            assertSame(
                failure,
                expectThrows(IOException.class, () -> gate.run("action", new GetRequest("docs", "id"), () -> { throw failure; }))
            );
        }
    }

    public void testCheckedFilterFailureIsPreserved() throws Exception {
        final var failure = new IOException("policy backend unavailable");
        final var gate = new ActionGate(List.of(filter(failure, null)));
        assertSame(failure, expectThrows(IOException.class, () -> gate.run("action", new GetRequest("docs", "id"), () -> "unused")));
    }

    public void testRuntimeFilterFailureKeepsItsStatusAndIdentity() throws Exception {
        final var failure = new org.opensearch.OpenSearchStatusException("policy denied", org.opensearch.core.rest.RestStatus.FORBIDDEN);
        final var gate = new ActionGate(List.of(filter(failure, null)));
        assertSame(
            failure,
            expectThrows(
                org.opensearch.OpenSearchStatusException.class,
                () -> gate.run("action", new GetRequest("docs", "id"), () -> "unused")
            )
        );
    }

    public void testInterruptedWaitPreservesTheCancellationSignal() throws Exception {
        final var waiting = new CountDownLatch(1);
        final var caller = new AtomicReference<Thread>();
        final var gate = new ActionGate(List.of(filter(null, waiting)));
        try (var worker = Executors.newSingleThreadExecutor()) {
            final var interrupted = worker.submit(() -> {
                caller.set(Thread.currentThread());
                try {
                    gate.run("action", new GetRequest("docs", "id"), () -> "unused");
                    return false;
                } catch (InterruptedException e) {
                    return Thread.currentThread().isInterrupted();
                }
            });
            assertTrue(waiting.await(10, TimeUnit.SECONDS));
            caller.get().interrupt();
            assertTrue(interrupted.get(10, TimeUnit.SECONDS));
        }
    }
}
