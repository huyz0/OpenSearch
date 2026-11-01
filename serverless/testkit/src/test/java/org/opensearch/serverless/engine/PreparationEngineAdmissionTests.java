/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PreparationEngineAdmissionTests extends OpenSearchTestCase {
    public void testBorrowedContextSlotsPreserveAnotherGroupAndCloseIsIdempotent() {
        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final var admission = new PreparationEngineAdmission(Set.of(first, second), 3, 300, 100);
        final var one = admission.acquire(first, shard(0), 10);
        final var two = admission.acquire(first, shard(1), 10);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> admission.acquire(first, shard(2), 10));
        final var other = admission.acquire(second, shard(2), 100);
        assertEquals(new PreparationEngineAdmission.Stats(3, 120, 0), admission.stats());
        two.close();
        two.close();
        final var borrowed = admission.acquire(first, shard(1), 50);
        two.close();
        assertEquals(new PreparationEngineAdmission.Stats(3, 160, 0), admission.stats());
        borrowed.close();
        one.close();
        other.close();
        assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
    }

    public void testByteBorrowingPreservesUnusedGroupAllowanceAndRestoresCredits() {
        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final var admission = new PreparationEngineAdmission(Set.of(first, second), 4, 300, 100);
        final var one = admission.acquire(first, shard(0), 80);
        final var two = admission.acquire(first, shard(1), 100);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> admission.acquire(first, shard(2), 21));
        final var other = admission.acquire(second, shard(2), 100);
        final var last = admission.acquire(first, shard(3), 20);
        assertEquals(new PreparationEngineAdmission.Stats(4, 300, 0), admission.stats());
        one.close();
        last.close();
        other.close();
        final var borrowed = admission.acquire(first, shard(4), 100);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> admission.acquire(first, shard(5), 1));
        // The remaining first-group context already covers its allowance; the second can reopen.
        final var reopened = admission.acquire(second, shard(2), 100);
        reopened.close();
        borrowed.close();
        two.close();
        assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
    }

    public void testUncertainCleanupRetainsResourcesAndOtherGroupStillOpens() {
        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final var admission = new PreparationEngineAdmission(Set.of(first, second), 2, 200, 100);
        final var uncertain = admission.acquire(first, shard(0), 100);
        expectThrows(NullPointerException.class, () -> uncertain.quarantine(null));
        assertEquals(0, admission.stats().quarantinedContexts());
        uncertain.quarantine(new Object());
        uncertain.quarantine(new Object());
        uncertain.close();
        uncertain.close();
        expectThrows(OpenSearchRejectedExecutionException.class, () -> admission.acquire(second, shard(0), 100));
        try (var healthy = admission.acquire(second, shard(1), 100)) {
            assertEquals(new PreparationEngineAdmission.Stats(2, 200, 1), admission.stats());
        }
        assertEquals(new PreparationEngineAdmission.Stats(1, 100, 1), admission.stats());
    }

    public void testMaximumLongBudgetCannotOverflowDuringAdmissionOrRelease() {
        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final long half = Long.MAX_VALUE / 2;
        final var admission = new PreparationEngineAdmission(Set.of(first, second), 3, Long.MAX_VALUE, half);
        final var one = admission.acquire(first, shard(0), half);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> admission.acquire(first, shard(1), half));
        final var two = admission.acquire(second, shard(1), half);
        final var last = admission.acquire(first, shard(2), 1);
        assertEquals(Long.MAX_VALUE, admission.stats().reservedBytes());
        one.close();
        try (var reuse = admission.acquire(first, shard(0), half)) {
            assertEquals(Long.MAX_VALUE, admission.stats().reservedBytes());
        }
        two.close();
        last.close();
        assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
    }

    public void testParallelOpenAttemptsCannotExceedCountOrMemoryCeilings() throws Exception {
        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final var admission = new PreparationEngineAdmission(Set.of(first, second), 4, 400, 100);
        final var attempted = new CountDownLatch(8);
        final var start = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var firstAccepted = new AtomicInteger();
        final var secondAccepted = new AtomicInteger();
        final var executor = Executors.newFixedThreadPool(8);
        final var futures = new ArrayList<Future<?>>();
        try {
            for (int i = 0; i < 8; i++) {
                final int id = i;
                futures.add(executor.submit(() -> {
                    assertTrue(start.await(10, TimeUnit.SECONDS));
                    PreparationEngineAdmission.Ticket ticket = null;
                    try {
                        ticket = admission.acquire(id % 2 == 0 ? first : second, shard(id), 100);
                        (id % 2 == 0 ? firstAccepted : secondAccepted).incrementAndGet();
                    } catch (OpenSearchRejectedExecutionException expected) {
                        // Half of the simultaneous opens must be refused before native resource work.
                    } finally {
                        attempted.countDown();
                    }
                    try {
                        assertTrue(release.await(10, TimeUnit.SECONDS));
                    } finally {
                        if (ticket != null) {
                            ticket.close();
                            ticket.close();
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            assertTrue(attempted.await(10, TimeUnit.SECONDS));
            assertEquals(new PreparationEngineAdmission.Stats(4, 400, 0), admission.stats());
            assertTrue(firstAccepted.get() > 0);
            assertTrue(secondAccepted.get() > 0);
        } finally {
            start.countDown();
            release.countDown();
            try {
                for (var future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
        assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
    }

    public void testInvalidBudgetsAndUnknownGroupsCannotChangeAccounting() {
        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        expectThrows(IllegalArgumentException.class, () -> new PreparationEngineAdmission(Set.of(first, second), 1, 200, 100));
        expectThrows(IllegalArgumentException.class, () -> new PreparationEngineAdmission(Set.of(first, second), 2, 199, 100));
        expectThrows(IllegalArgumentException.class, () -> new PreparationEngineAdmission(Set.of(first), 257, 200, 100));
        expectThrows(IllegalArgumentException.class, () -> new PreparationEngineAdmission(Set.of(first), 1, 200, 0));
        final var admission = new PreparationEngineAdmission(Set.of(first), 1, 100, 100);
        expectThrows(IllegalArgumentException.class, () -> admission.acquire(second, shard(0), 100));
        expectThrows(IllegalArgumentException.class, () -> admission.acquire(first, shard(0), 0));
        expectThrows(IllegalArgumentException.class, () -> admission.acquire(first, shard(0), 101));
        final var ticket = admission.acquire(first, shard(0), 100);
        ticket.close();
        expectThrows(IllegalStateException.class, () -> ticket.quarantine(new Object()));
        assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
    }

    private static ShardId shard(int number) {
        return new ShardId("index", "index-uuid", number);
    }
}
