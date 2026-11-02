/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.ShardLogId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PreparationWorkDispatcherTests extends OpenSearchTestCase {
    public void testStalledGroupCannotConsumeHealthyGroupWorkerOrLastRequestCredit() throws Exception {
        final var first = UUID.randomUUID();
        final var second = UUID.randomUUID();
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var dispatcher = dispatcher(Set.of(first, second), 4, 2, 200, 100);
        try {
            final var stalled = binding(dispatcher, first, 0);
            final var spare = binding(dispatcher, first, 1);
            final var healthy = binding(dispatcher, second, 2);
            final var pending = stalled.submit(100, () -> {
                entered.countDown();
                await(release);
                return mutation();
            });
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            assertFailure(OpenSearchRejectedExecutionException.class, spare.submit(100, () -> {
                throw new AssertionError("the other group's last credit must be reserved");
            }));
            healthy.submit(100, PreparationWorkDispatcherTests::mutation).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(new PreparationWorkDispatcher.Stats(3, 1, 100), dispatcher.stats());
            release.countDown();
            pending.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(new PreparationWorkDispatcher.Stats(3, 0, 0), dispatcher.stats());
        } finally {
            release.countDown();
            dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        assertEquals(new PreparationWorkDispatcher.Stats(0, 0, 0), dispatcher.stats());
    }

    public void testQueuedClosedRequestKeepsBytesUntilItsWrapperDrains() throws Exception {
        final var group = UUID.randomUUID();
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var prepared = new AtomicInteger();
        final var dispatcher = dispatcher(Set.of(group), 4, 2, 200, 100);
        try {
            final var running = binding(dispatcher, group, 0);
            final var queued = binding(dispatcher, group, 1);
            final var extra = binding(dispatcher, group, 2);
            final var first = running.submit(100, () -> {
                entered.countDown();
                await(release);
                return mutation();
            });
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            final var second = queued.submit(100, () -> {
                prepared.incrementAndGet();
                return mutation();
            });
            final var closed = queued.closeAsync();
            assertFalse(closed.toCompletableFuture().isDone());
            assertFalse(second.toCompletableFuture().isDone());
            assertEquals(new PreparationWorkDispatcher.Stats(3, 2, 200), dispatcher.stats());
            assertFailure(OpenSearchRejectedExecutionException.class, extra.submit(1, PreparationWorkDispatcherTests::mutation));
            release.countDown();
            first.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertFailure(IOException.class, second);
            closed.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(0, prepared.get());
            assertEquals(new PreparationWorkDispatcher.Stats(2, 0, 0), dispatcher.stats());
            extra.submit(100, PreparationWorkDispatcherTests::mutation).toCompletableFuture().get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    public void testCloseDoesNotWaitForOrCancelAcceptedJournalReply() throws Exception {
        final var group = UUID.randomUUID();
        final var submitted = new CountDownLatch(1);
        final var append = new CompletableFuture<LogCheckpoint>();
        final var applied = new AtomicInteger();
        final var dispatcher = dispatcher(Set.of(group), 1, 1, 100, 100);
        try {
            final var binding = dispatcher.bind(group, shard(0), mutation -> {
                submitted.countDown();
                return append;
            }, (mutation, checkpoint) -> {
                applied.incrementAndGet();
                return new CommittedMutationReplayer.AppliedMutation(mutation, checkpoint);
            });
            final var result = binding.submit(100, PreparationWorkDispatcherTests::mutation);
            assertTrue(submitted.await(10, TimeUnit.SECONDS));
            assertEquals(new PreparationWorkDispatcher.Stats(1, 1, 100), dispatcher.stats());
            dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertFailure(IOException.class, result);
            assertFalse(append.isCancelled());
            append.complete(checkpoint());
            assertEquals(0, applied.get());
            assertEquals(new PreparationWorkDispatcher.Stats(0, 0, 0), dispatcher.stats());
        } finally {
            dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    public void testClosingHostRetainsRunningNativeWorkAndRefusesNewBindings() throws Exception {
        final var group = UUID.randomUUID();
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var dispatcher = dispatcher(Set.of(group), 2, 2, 200, 100);
        try {
            final var binding = binding(dispatcher, group, 0);
            final var result = binding.submit(100, () -> {
                entered.countDown();
                await(release);
                return mutation();
            });
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            final var closed = dispatcher.closeAsync();
            assertFalse(closed.toCompletableFuture().isDone());
            assertFalse(result.toCompletableFuture().isDone());
            assertEquals(new PreparationWorkDispatcher.Stats(1, 1, 100), dispatcher.stats());
            expectThrows(OpenSearchRejectedExecutionException.class, () -> binding(dispatcher, group, 1));
            assertFailure(OpenSearchRejectedExecutionException.class, binding.submit(1, PreparationWorkDispatcherTests::mutation));
            release.countDown();
            closed.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertFailure(IOException.class, result);
            assertEquals(new PreparationWorkDispatcher.Stats(0, 0, 0), dispatcher.stats());
        } finally {
            release.countDown();
            dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    public void testBindingReservationsAndBudgetValidationPrecedeWork() throws Exception {
        final var first = UUID.randomUUID();
        final var second = UUID.randomUUID();
        expectThrows(IllegalArgumentException.class, () -> dispatcher(Set.of(first, second), 2, 2, 199, 100));
        final var dispatcher = dispatcher(Set.of(first, second), 3, 2, 200, 100);
        try {
            final var one = binding(dispatcher, first, 0);
            binding(dispatcher, first, 1);
            expectThrows(OpenSearchRejectedExecutionException.class, () -> binding(dispatcher, first, 2));
            final var healthy = binding(dispatcher, second, 2);
            expectThrows(OpenSearchRejectedExecutionException.class, () -> binding(dispatcher, second, 0));
            assertFailure(IllegalArgumentException.class, one.submit(101, PreparationWorkDispatcherTests::mutation));
            healthy.submit(100, PreparationWorkDispatcherTests::mutation).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(new PreparationWorkDispatcher.Stats(3, 0, 0), dispatcher.stats());
            one.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            one.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            binding(dispatcher, first, 0).submit(100, PreparationWorkDispatcherTests::mutation)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        } finally {
            dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    public void testUnknownFailureAfterQueueAcceptanceRetainsItsReservation() throws Exception {
        final var group = UUID.randomUUID();
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var failAfterQueue = new java.util.concurrent.atomic.AtomicBoolean();
        final var original = new IllegalStateException("scheduler failed after queue acceptance");
        class FaultingWorker extends java.util.concurrent.ThreadPoolExecutor {
            @org.opensearch.common.SuppressForbidden(reason = "Inject a failure after real queue acceptance using a bounded test executor")
            FaultingWorker() {
                super(
                    1,
                    1,
                    0,
                    TimeUnit.MILLISECONDS,
                    new java.util.concurrent.ArrayBlockingQueue<>(2),
                    org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory("uncertain-private-dispatch")
                );
            }

            @Override
            public void execute(Runnable command) {
                super.execute(command);
                if (failAfterQueue.getAndSet(false)) {
                    throw original;
                }
            }

            @Override
            protected void afterExecute(Runnable task, Throwable failure) {
                super.afterExecute(task, failure);
                org.opensearch.common.util.concurrent.OpenSearchExecutors.rethrowErrors(task);
            }
        }
        final var dispatcher = new PreparationWorkDispatcher(Set.of(group), 3, 2, 200, 100, ignored -> new FaultingWorker());

        try {
            final var running = binding(dispatcher, group, 0);
            final var queued = binding(dispatcher, group, 1);
            final var extra = binding(dispatcher, group, 2);
            final var first = running.submit(100, () -> {
                entered.countDown();
                await(release);
                return mutation();
            });
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            failAfterQueue.set(true);
            final var second = queued.submit(
                100,
                () -> { throw new AssertionError("uncertain canceled job must not enter native preparation"); }
            );
            assertFalse(second.toCompletableFuture().isDone());
            assertEquals(new PreparationWorkDispatcher.Stats(3, 2, 200), dispatcher.stats());
            assertFailure(OpenSearchRejectedExecutionException.class, extra.submit(1, PreparationWorkDispatcherTests::mutation));
            final var closed = queued.closeAsync();
            assertFalse(closed.toCompletableFuture().isDone());
            release.countDown();
            first.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertFailure(IllegalStateException.class, second);
            closed.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(new PreparationWorkDispatcher.Stats(2, 0, 0), dispatcher.stats());
        } finally {
            release.countDown();
            dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static PreparationWorkDispatcher dispatcher(Set<UUID> groups, int bindings, int requests, long bytes, long maximumRequest) {
        return new PreparationWorkDispatcher(
            "private-work-test",
            groups,
            bindings,
            requests,
            bytes,
            maximumRequest,
            new ThreadContext(Settings.EMPTY)
        );
    }

    private static PreparationWorkDispatcher.Binding binding(PreparationWorkDispatcher dispatcher, UUID group, int shard) {
        return dispatcher.bind(
            group,
            shard(shard),
            mutation -> CompletableFuture.completedFuture(checkpoint()),
            CommittedMutationReplayer.AppliedMutation::new
        );
    }

    private static ShardId shard(int id) {
        return new ShardId("index", "index-uuid", id);
    }

    private static ResolvedMutation mutation() {
        return new ResolvedMutation(UUID.randomUUID(), new Translog.NoOp(0, 1, "resolved"), ResolvedMutation.Outcome.NO_OP);
    }

    private static LogCheckpoint checkpoint() {
        return new LogCheckpoint(new ShardLogId(new UUID(1, 2), "index-uuid", 0, new UUID(3, 4)), 0, 1);
    }

    private static <T extends Throwable> void assertFailure(Class<T> type, CompletionStage<?> result) throws Exception {
        final var failure = expectThrows(ExecutionException.class, () -> result.toCompletableFuture().get(10, TimeUnit.SECONDS)).getCause();
        assertTrue("unexpected failure: " + failure, type.isInstance(failure));
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (latch.await(10, TimeUnit.SECONDS) == false) {
                throw new IOException("test did not release private native work");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException(failure);
        }
    }
}
