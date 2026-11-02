/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.ShardLogId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PreparationCommitSequencerTests extends OpenSearchTestCase {
    public void testQueuedPreparationClosesWithoutTouchingNativeResources() throws Exception {
        final var tasks = new ArrayDeque<Runnable>();
        final var gate = new PreparationCommitSequencer(tasks::add, mutation -> {
            throw new AssertionError("closed queued preparation must not append");
        }, (mutation, checkpoint) -> { throw new AssertionError("closed queued preparation must not apply"); });
        final var result = gate.submit(() -> { throw new AssertionError("queued preparation must not run after its owner closes"); });
        assertEquals(PreparationCommitSequencer.State.PREPARING, gate.state());
        assertFalse(result.toCompletableFuture().isDone());
        gate.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertFailure(IOException.class, result);
        assertEquals(1, tasks.size());
        tasks.remove().run();
        assertEquals(PreparationCommitSequencer.State.CLOSED, gate.state());
    }

    public void testQueuedApplicationClosesWithoutTouchingServingResources() throws Exception {
        final var tasks = new ArrayDeque<Runnable>();
        final var append = new CompletableFuture<LogCheckpoint>();
        final var gate = new PreparationCommitSequencer(tasks::add, mutation -> append, (mutation, checkpoint) -> {
            throw new AssertionError("queued application must not run after serving resources close");
        });
        final var result = gate.submit(() -> mutation(0));
        tasks.remove().run();
        assertEquals(PreparationCommitSequencer.State.APPENDING, gate.state());
        append.complete(checkpoint(0));
        assertEquals(PreparationCommitSequencer.State.APPLYING, gate.state());
        gate.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertFailure(IOException.class, result);
        tasks.remove().run();
        assertFalse(append.isCancelled());
        assertEquals(PreparationCommitSequencer.State.CLOSED, gate.state());
    }

    public void testDispatchRejectionNeverFallsBackToCallerOrJournalThread() throws Exception {
        final var original = new RejectedExecutionException("bounded private worker is full");
        final Executor refused = task -> { throw original; };
        final var first = new PreparationCommitSequencer(
            refused,
            mutation -> { throw new AssertionError("refused preparation must not append"); },
            (mutation, checkpoint) -> {
                throw new AssertionError("refused preparation must not apply");
            }
        );
        assertSame(original, assertFailure(RejectedExecutionException.class, first.submit(() -> {
            throw new AssertionError("must not index on the submitting thread");
        })));
        assertEquals(PreparationCommitSequencer.State.FENCED, first.state());
        final var tasks = new ArrayDeque<Runnable>();
        final var dispatch = new AtomicReference<Executor>(tasks::add);
        final var append = new CompletableFuture<LogCheckpoint>();
        final var second = new PreparationCommitSequencer(
            task -> dispatch.get().execute(task),
            mutation -> append,
            (mutation, checkpoint) -> {
                throw new AssertionError("must not apply on the journal completion thread");
            }
        );
        final var result = second.submit(() -> mutation(0));
        tasks.remove().run();
        dispatch.set(refused);
        append.complete(checkpoint(0));
        assertSame(original, assertFailure(RejectedExecutionException.class, result));
        assertEquals(PreparationCommitSequencer.State.FENCED, second.state());
        assertFalse(append.isCancelled());
        second.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    public void testSynchronousJournalReplyStillDispatchesApplicationToOwnedWorker() throws Exception {
        final var owner = Thread.currentThread();
        final var preparationThread = new AtomicReference<Thread>();
        final var gateRef = new AtomicReference<PreparationCommitSequencer>();
        final var context = new org.opensearch.common.util.concurrent.ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        final var worker = org.opensearch.common.util.concurrent.OpenSearchExecutors.newFixed(
            "private-stage-test",
            1,
            1,
            org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory("private-stage-test"),
            context
        );
        try {
            final var gate = new PreparationCommitSequencer(worker, mutation -> {
                assertSame(preparationThread.get(), Thread.currentThread());
                return CompletableFuture.completedFuture(checkpoint(0));
            }, (mutation, checkpoint) -> {
                assertFalse(Thread.holdsLock(gateRef.get()));
                assertNotSame(owner, Thread.currentThread());
                assertSame(preparationThread.get(), Thread.currentThread());
                return new CommittedMutationReplayer.AppliedMutation(mutation, checkpoint);
            });
            gateRef.set(gate);
            final var result = gate.submit(() -> {
                assertFalse(Thread.holdsLock(gate));
                assertNotSame(owner, Thread.currentThread());
                preparationThread.set(Thread.currentThread());
                return mutation(0);
            });
            result.toCompletableFuture().get(10, TimeUnit.SECONDS);
            gate.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        } finally {
            worker.shutdown();
            assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testExecutorFailureAfterNativeStartCannotReleaseRunningPreparation() throws Exception {
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var original = new RejectedExecutionException("executor decorator failed after dispatch");
        final var worker = Executors.newSingleThreadExecutor();
        try {
            final Executor dispatch = task -> {
                worker.execute(task);
                try {
                    await(entered);
                } catch (IOException failure) {
                    throw new AssertionError(failure);
                }
                throw original;
            };
            final var gate = new PreparationCommitSequencer(
                dispatch,
                mutation -> { throw new AssertionError("fenced preparation must not append"); },
                (mutation, checkpoint) -> {
                    throw new AssertionError("fenced preparation must not apply");
                }
            );
            final var result = gate.submit(() -> {
                entered.countDown();
                await(release);
                return mutation(0);
            });
            final var closed = gate.closeAsync();
            assertFalse(closed.toCompletableFuture().isDone());
            assertFalse(result.toCompletableFuture().isDone());
            release.countDown();
            closed.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertSame(original, assertFailure(RejectedExecutionException.class, result));
        } finally {
            release.countDown();
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testDependentPreparationWaitsForCommitAndApplication() throws Exception {
        final var append = new CompletableFuture<LogCheckpoint>();
        final var cp = checkpoint(0);
        final var prepared = new AtomicInteger();
        final var applied = new AtomicInteger();
        final var gate = new PreparationCommitSequencer(Runnable::run, mutation -> append, (mutation, checkpoint) -> {
            applied.incrementAndGet();
            return new CommittedMutationReplayer.AppliedMutation(mutation, checkpoint);
        });
        final var first = gate.submit(() -> {
            prepared.incrementAndGet();
            return mutation(0);
        });
        assertEquals(PreparationCommitSequencer.State.APPENDING, gate.state());
        assertFailure(OpenSearchRejectedExecutionException.class, gate.submit(() -> {
            prepared.incrementAndGet();
            return mutation(1);
        }));
        assertEquals(1, prepared.get());
        assertEquals(0, applied.get());
        assertFalse(first.toCompletableFuture().isDone());
        append.complete(cp);
        assertEquals(cp, first.toCompletableFuture().get(10, TimeUnit.SECONDS).checkpoint());
        assertEquals(1, applied.get());
        assertEquals(PreparationCommitSequencer.State.READY, gate.state());
        gate.submit(() -> mutation(1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertEquals(2, applied.get());
    }

    public void testCancelledClientDoesNotCancelAppendOrFreeThePendingGate() throws Exception {
        final var append = new CompletableFuture<LogCheckpoint>();
        final var applied = new AtomicInteger();
        final var gate = new PreparationCommitSequencer(Runnable::run, mutation -> append, (mutation, checkpoint) -> {
            applied.incrementAndGet();
            return new CommittedMutationReplayer.AppliedMutation(mutation, checkpoint);
        });
        final var result = gate.submit(() -> mutation(0));
        final var cancelledView = result.toCompletableFuture();
        assertTrue(cancelledView.cancel(true));
        assertFalse(append.isCancelled());
        assertFailure(OpenSearchRejectedExecutionException.class, gate.submit(() -> mutation(1)));
        append.complete(checkpoint(0));
        result.toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertTrue(cancelledView.isCancelled());
        assertEquals(1, applied.get());
        assertEquals(PreparationCommitSequencer.State.READY, gate.state());
    }

    public void testAppendFailureFencesPrivateStateAndPreservesTheFailure() throws Exception {
        final var original = new IOException("ambiguous durable append");
        final var gate = new PreparationCommitSequencer(Runnable::run, mutation -> { throw original; }, (mutation, checkpoint) -> {
            throw new AssertionError("failed append cannot apply");
        });
        assertSame(original, assertFailure(IOException.class, gate.submit(() -> mutation(0))));
        assertEquals(PreparationCommitSequencer.State.FENCED, gate.state());
        assertFailure(OpenSearchRejectedExecutionException.class, gate.submit(() -> {
            throw new AssertionError("fenced private state must never prepare another mutation");
        }));
    }

    public void testPreparationAndApplicationFailuresFenceWithoutRepeatingWork() throws Exception {
        final var preparationFailure = new IllegalArgumentException("preparation may have changed private state");
        final var first = new PreparationCommitSequencer(Runnable::run, mutation -> {
            throw new AssertionError("failed preparation cannot append");
        }, (mutation, checkpoint) -> { throw new AssertionError("failed preparation cannot apply"); });
        assertSame(preparationFailure, assertFailure(IllegalArgumentException.class, first.submit(() -> { throw preparationFailure; })));
        assertEquals(PreparationCommitSequencer.State.FENCED, first.state());
        final var applicationFailure = new IOException("committed serving application failed");
        final var second = new PreparationCommitSequencer(
            Runnable::run,
            mutation -> CompletableFuture.completedStage(checkpoint(0)),
            (mutation, checkpoint) -> {
                throw applicationFailure;
            }
        );
        assertSame(applicationFailure, assertFailure(IOException.class, second.submit(() -> mutation(0))));
        assertEquals(PreparationCommitSequencer.State.FENCED, second.state());
    }

    public void testCloseWaitsForPreparationAndPreventsItsAppend() throws Exception {
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var appended = new AtomicInteger();
        final var gate = new PreparationCommitSequencer(Runnable::run, mutation -> {
            appended.incrementAndGet();
            return CompletableFuture.completedStage(checkpoint(0));
        }, CommittedMutationReplayer.AppliedMutation::new);
        final var executor = Executors.newSingleThreadExecutor();
        try {
            final var submitted = executor.submit(() -> gate.submit(() -> {
                entered.countDown();
                await(release);
                return mutation(0);
            }));
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            final var closed = gate.closeAsync();
            assertFalse(closed.toCompletableFuture().isDone());
            assertEquals(PreparationCommitSequencer.State.CLOSED, gate.state());
            release.countDown();
            assertFailure(IOException.class, submitted.get(10, TimeUnit.SECONDS));
            closed.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(0, appended.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testCloseDoesNotWaitForAnAcceptedQuorumReplyOrApplyAfterClosing() throws Exception {
        final var append = new CompletableFuture<LogCheckpoint>();
        final var applied = new AtomicInteger();
        final var gate = new PreparationCommitSequencer(Runnable::run, mutation -> append, (mutation, checkpoint) -> {
            applied.incrementAndGet();
            return new CommittedMutationReplayer.AppliedMutation(mutation, checkpoint);
        });
        final var result = gate.submit(() -> mutation(0));
        gate.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertFailure(IOException.class, result);
        assertFalse(append.isCancelled());
        append.complete(checkpoint(0));
        assertEquals(0, applied.get());
        assertEquals(PreparationCommitSequencer.State.CLOSED, gate.state());
    }

    public void testCloseWaitsForServingApplicationBeforeResourceRelease() throws Exception {
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var append = new CompletableFuture<LogCheckpoint>();
        final var gate = new PreparationCommitSequencer(Runnable::run, mutation -> append, (mutation, checkpoint) -> {
            entered.countDown();
            await(release);
            return new CommittedMutationReplayer.AppliedMutation(mutation, checkpoint);
        });
        final var result = gate.submit(() -> mutation(0));
        final var executor = Executors.newSingleThreadExecutor();
        try {
            final var completing = executor.submit(() -> append.complete(checkpoint(0)));
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            final var closed = gate.closeAsync();
            assertFalse(closed.toCompletableFuture().isDone());
            release.countDown();
            completing.get(10, TimeUnit.SECONDS);
            closed.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertFailure(IOException.class, result);
            assertEquals(PreparationCommitSequencer.State.CLOSED, gate.state());
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testNativeAndCompletionCallbacksNeverHoldTheStateMonitor() throws Exception {
        final var reference = new AtomicReference<PreparationCommitSequencer>();
        final var append = new CompletableFuture<LogCheckpoint>();
        final var gate = new PreparationCommitSequencer(Runnable::run, mutation -> {
            assertFalse(Thread.holdsLock(reference.get()));
            return append;
        }, (mutation, checkpoint) -> {
            assertFalse(Thread.holdsLock(reference.get()));
            return new CommittedMutationReplayer.AppliedMutation(mutation, checkpoint);
        });
        reference.set(gate);
        final var result = gate.submit(() -> {
            assertFalse(Thread.holdsLock(gate));
            return mutation(0);
        });
        final var notified = result.thenRun(() -> assertFalse(Thread.holdsLock(gate)));
        append.complete(checkpoint(0));
        notified.toCompletableFuture().get(10, TimeUnit.SECONDS);
        gate.closeAsync().thenRun(() -> assertFalse(Thread.holdsLock(gate))).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    public void testWrongAppliedIdentityOrCheckpointCannotAcknowledgeSuccess() throws Exception {
        for (boolean wrongIdentity : new boolean[] { true, false }) {
            final var checkpoint = checkpoint(0);
            final var gate = new PreparationCommitSequencer(
                Runnable::run,
                mutation -> CompletableFuture.completedStage(checkpoint),
                (mutation, committed) -> new CommittedMutationReplayer.AppliedMutation(
                    wrongIdentity ? mutation(1) : mutation,
                    wrongIdentity ? committed : checkpoint(1)
                )
            );
            assertFailure(IOException.class, gate.submit(() -> mutation(0)));
            assertEquals(PreparationCommitSequencer.State.FENCED, gate.state());
        }
    }

    public void testExternalFenceDoesNotAbandonAcceptedPersistence() throws Exception {
        final var append = new CompletableFuture<LogCheckpoint>();
        final var original = new IOException("ownership was fenced");
        final var gate = new PreparationCommitSequencer(Runnable::run, mutation -> append, (mutation, checkpoint) -> {
            throw new AssertionError("fenced context cannot apply");
        });
        final var result = gate.submit(() -> mutation(0));
        gate.fence(original);
        assertSame(original, assertFailure(IOException.class, result));
        assertFalse(append.isCancelled());
        append.complete(checkpoint(0));
        assertEquals(PreparationCommitSequencer.State.FENCED, gate.state());
        gate.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static ResolvedMutation mutation(long seqNo) {
        return new ResolvedMutation(UUID.randomUUID(), new Translog.NoOp(seqNo, 1, "resolved"), ResolvedMutation.Outcome.NO_OP);
    }

    private static LogCheckpoint checkpoint(long index) {
        return new LogCheckpoint(new ShardLogId(new UUID(1, 2), "index-uuid", 0, new UUID(3, 4)), index, 1);
    }

    private static <T extends Throwable> T assertFailure(Class<T> type, CompletionStage<?> stage) throws Exception {
        final var failure = expectThrows(ExecutionException.class, () -> stage.toCompletableFuture().get(10, TimeUnit.SECONDS)).getCause();
        assertTrue("unexpected failure: " + failure, type.isInstance(failure));
        return type.cast(failure);
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (latch.await(10, TimeUnit.SECONDS) == false) {
                throw new IOException("timed out waiting for test release");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException(interrupted);
        }
    }
}
