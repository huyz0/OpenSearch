/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.serverless.log.LogCheckpoint;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * One pending preparation per logical stream until its append and verified serving application finish.
 * Adapters must validate ownership/schema, supply real journal append and committed replay, and close
 * private resources only after closeAsync settles. No native/SDK work or completion runs under the
 * state monitor. This initial gate deliberately does not pipeline independent document keys.
 * Preparation runs on the submitting thread and application on the append-completion thread; the
 * runtime adapter must provide bounded dispatch. Any preparation exception conservatively fences
 * this initial gate; a native adapter must distinguish proven mutation-free rejections before rollout.
 */
final class PreparationCommitSequencer {
    enum State {
        READY,
        PREPARING,
        APPENDING,
        APPLYING,
        FENCED,
        CLOSED
    }

    @FunctionalInterface
    interface Preparer {
        ResolvedMutation prepare() throws IOException;
    }

    @FunctionalInterface
    interface Appender {
        CompletionStage<LogCheckpoint> append(ResolvedMutation mutation) throws IOException;
    }

    @FunctionalInterface
    interface Applier {
        CommittedMutationReplayer.AppliedMutation apply(ResolvedMutation mutation, LogCheckpoint checkpoint) throws IOException;
    }

    private static final class Attempt {
        private final CompletableFuture<CommittedMutationReplayer.AppliedMutation> result = new CompletableFuture<>();
        private boolean working = true;
        private Throwable failure;
    }

    private final Appender appender;
    private final Applier applier;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private State state = State.READY;
    private Attempt active;
    private int work;

    PreparationCommitSequencer(Appender appender, Applier applier) {
        this.appender = Objects.requireNonNull(appender);
        this.applier = Objects.requireNonNull(applier);
    }

    CompletionStage<CommittedMutationReplayer.AppliedMutation> submit(Preparer preparer) {
        Objects.requireNonNull(preparer);
        final Attempt attempt;
        synchronized (this) {
            if (state != State.READY) {
                return CompletableFuture.failedStage(
                    new OpenSearchRejectedExecutionException("private preparation is not ready: " + state)
                );
            }
            active = attempt = new Attempt();
            state = State.PREPARING;
            work++;
        }
        try {
            final var mutation = Objects.requireNonNull(preparer.prepare());
            synchronized (this) {
                if (state != State.PREPARING || active != attempt) {
                    throw new IOException("private preparation was fenced before append", attempt.failure);
                }
                state = State.APPENDING;
            }
            final var append = Objects.requireNonNull(appender.append(mutation));
            finishWork(attempt);
            append.whenComplete((checkpoint, failure) -> {
                if (failure != null) {
                    fail(attempt, failure);
                    return;
                }
                synchronized (this) {
                    if (state != State.APPENDING || active != attempt) {
                        return;
                    }
                    state = State.APPLYING;
                    attempt.working = true;
                    work++;
                }
                try {
                    final var applied = Objects.requireNonNull(applier.apply(mutation, Objects.requireNonNull(checkpoint)));
                    if (applied.mutation().operationId().equals(mutation.operationId()) == false
                        || applied.checkpoint().equals(checkpoint) == false) {
                        throw new IOException("serving application returned a different operation or committed checkpoint");
                    }
                    final boolean succeeded;
                    synchronized (this) {
                        succeeded = state == State.APPLYING && active == attempt;
                        if (succeeded) {
                            state = State.READY;
                            active = null;
                        }
                    }
                    finishWork(attempt);
                    if (succeeded) {
                        attempt.result.complete(applied);
                    }
                } catch (IOException | RuntimeException | Error failed) {
                    fail(attempt, failed);
                }
            });
        } catch (IOException | RuntimeException | Error failed) {
            fail(attempt, failed);
        }
        // Caller cancellation cannot cancel accepted persistence, free the gate or discard application.
        return attempt.result.minimalCompletionStage();
    }

    synchronized State state() {
        return state;
    }

    void fence(Throwable cause) {
        Objects.requireNonNull(cause);
        final Attempt attempt;
        synchronized (this) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.FENCED;
            attempt = active;
            if (attempt != null && attempt.failure == null) {
                attempt.failure = cause;
            }
        }
        settle(attempt);
    }

    /** Stops new work and waits for preparation/application, not an indefinitely delayed quorum reply. */
    CompletionStage<Void> closeAsync() {
        final Attempt attempt;
        synchronized (this) {
            state = State.CLOSED;
            attempt = active;
            if (attempt != null && attempt.failure == null) {
                attempt.failure = new IOException("private preparation closed");
            }
        }
        settle(attempt);
        return closed.minimalCompletionStage();
    }

    private void fail(Attempt attempt, Throwable cause) {
        synchronized (this) {
            if (attempt.failure == null) {
                attempt.failure = cause;
            }
            if (active == attempt && state != State.CLOSED) {
                state = State.FENCED;
            }
        }
        finishWork(attempt);
    }

    private void finishWork(Attempt attempt) {
        synchronized (this) {
            if (attempt.working) {
                attempt.working = false;
                work--;
            }
        }
        settle(attempt);
    }

    private void settle(Attempt attempt) {
        final boolean closeReady;
        final Throwable failure;
        synchronized (this) {
            closeReady = state == State.CLOSED && work == 0;
            failure = attempt != null && attempt.working == false ? attempt.failure : null;
            if (failure != null && active == attempt) {
                active = null;
            }
        }
        // Resource ownership can finish before notifying closed callers. Neither callback holds the monitor.
        if (closeReady) {
            closed.complete(null);
        }
        if (failure != null) {
            attempt.result.completeExceptionally(failure);
        }
    }
}
