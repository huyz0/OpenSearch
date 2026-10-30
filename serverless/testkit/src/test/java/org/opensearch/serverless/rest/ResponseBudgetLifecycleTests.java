/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.common.breaker.ChildMemoryCircuitBreaker;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ResponseBudgetLifecycleTests extends OpenSearchTestCase {
    private void withNode(org.opensearch.common.CheckedConsumer<ServerlessNode, Exception> work) throws Exception {
        final var settings = Settings.builder()
            .put("node.name", "response-budget-lifecycle")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("indices.breaker.total.use_real_memory", false)
            .put("indices.breaker.request.limit", "64kb")
            .build();
        try (var node = new ServerlessNode(settings)) {
            node.start();
            work.accept(node);
        }
    }

    public void testClosingReturnsReservationsAndRejectsLateResults() throws Exception {
        withNode(node -> {
            final var breaker = node.circuitBreakerService().getBreaker(CircuitBreaker.REQUEST);
            final long before = breaker.getUsed();
            final var retained = new AtomicReference<String>();
            try (var budget = new ResponseBudget(node)) {
                budget.keep(1024L, () -> retained.set("first"));
                assertEquals(before + 1024L, breaker.getUsed());
                budget.close();
                expectThrows(IllegalStateException.class, () -> budget.keep(1L, () -> retained.set("late")));
                assertEquals("first", retained.get());
            }
            assertEquals(before, breaker.getUsed());
        });
    }

    public void testAbandonedBatchCannotRetainAResultThatWasStillBeingEstimated() throws Exception {
        withNode(node -> {
            final var breaker = node.circuitBreakerService().getBreaker(CircuitBreaker.REQUEST);
            final long before = breaker.getUsed();
            final var retained = new AtomicReference<String>();
            final var entered = new CountDownLatch(1);
            final var resume = new CountDownLatch(1);
            try (var budget = new ResponseBudget(node); var worker = Executors.newSingleThreadExecutor()) {
                budget.keep(1024L, () -> retained.set("first"));
                final var completing = worker.submit(() -> expectThrows(IllegalStateException.class, () -> budget.keep(output -> {
                    entered.countDown();
                    try {
                        assertTrue(resume.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    output.writeString("late");
                }, () -> retained.set("late"))));
                try {
                    assertTrue(entered.await(10, TimeUnit.SECONDS));
                    budget.abandon(() -> retained.set(null));
                } finally {
                    resume.countDown();
                }
                completing.get(10, TimeUnit.SECONDS);
                assertNull(retained.get());
                assertEquals(before, breaker.getUsed());
            }
            assertEquals(before, breaker.getUsed());
        });
    }

    public void testAnActiveBatchObservesAnUpdatedBreakerLimit() throws Exception {
        withNode(node -> {
            final var breaker = (ChildMemoryCircuitBreaker) node.circuitBreakerService().getBreaker(CircuitBreaker.REQUEST);
            final long before = breaker.getUsed();
            try (var budget = new ResponseBudget(node)) {
                budget.keep(1024L, () -> {});
                breaker.setLimitAndOverhead(before + 2048L, 1.0);
                expectThrows(CircuitBreakingException.class, () -> budget.keep(2048L, () -> fail("an over-budget result was retained")));
                assertEquals(before + 1024L, breaker.getUsed());
            }
            assertEquals(before, breaker.getUsed());
        });
    }
}
