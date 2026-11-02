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
import java.util.concurrent.Executor;

/**
 * One pending preparation per logical stream until its append and verified serving application finish.
 * Adapters must validate ownership/schema, supply real journal append and committed replay, and close
 * private resources only after closeAsync settles. No native/SDK work or completion runs under the
 * state monitor. This initial gate deliberately does not pipeline independent document keys.
 * The host must supply a bounded, group-isolated executor and account for queued wrappers and
 * pending results, including capacity for an admitted stream's application handoff. Do not use inline
 * or caller-runs execution on a journal completion thread. Rejection conservatively fences the context; proven
 * mutation-free rejection handling and runtime admission still belong to the native adapter.
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
        private int work;
        private Preparer preparer;
        private ResolvedMutation mutation;
        private Throwable failure;
    }

    private final Executor executor;
    private Appender appender;
    private Applier applier;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private State state = State.READY;
    private Attempt active;
    private int work;

    PreparationCommitSequencer(Executor executor, Appender appender, Applier applier) {
        this.executor = Objects.requireNonNull(executor);
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
            attempt.preparer = preparer;
            state = State.PREPARING;
        }
        try {
            executor.execute(() -> prepare(attempt));
        } catch (RuntimeException | Error rejected) {
            fail(attempt, rejected);
        }
        // Caller cancellation cannot cancel accepted persistence, free the gate or discard application.
        return attempt.result.minimalCompletionStage();
    }

    private void prepare(Attempt attempt) {
        final Preparer preparer;
        synchronized (this) {
            if (state != State.PREPARING || active != attempt) {
                return;
            }
            preparer = attempt.preparer;
            attempt.preparer = null;
            attempt.work++;
            work++;
        }
        final ResolvedMutation mutation;
        final CompletionStage<LogCheckpoint> append;
        try {
            mutation = Objects.requireNonNull(preparer.prepare());
            synchronized (this) {
                if (state != State.PREPARING || active != attempt) {
                    throw new IOException("private preparation was fenced before append", attempt.failure);
                }
                state = State.APPENDING;
                attempt.mutation = mutation;
            }
            append = Objects.requireNonNull(appender.append(mutation));
        } catch (IOException | RuntimeException | Error failed) {
            fail(attempt, failed);
            return;
        } finally {
            finishWork(attempt);
        }
        try {
            append.whenComplete((checkpoint, failure) -> {
                if (failure != null) {
                    fail(attempt, failure);
                    return;
                }
                synchronized (this) {
                    if (state != State.APPENDING || active != attempt) {
                        return;
                    }
                    // Claim the application once, before dispatch. Queued work has not touched
                    // native resources and can be skipped if close/fencing wins before it starts.
                    state = State.APPLYING;
                }
                try {
                    executor.execute(() -> apply(attempt, checkpoint));
                } catch (RuntimeException | Error rejected) {
                    fail(attempt, rejected);
                }
            });
        } catch (RuntimeException | Error registrationFailure) {
            fail(attempt, registrationFailure);
        }
    }

    private void apply(Attempt attempt, LogCheckpoint checkpoint) {
        final ResolvedMutation mutation;
        final Applier application;
        synchronized (this) {
            if (state != State.APPLYING || active != attempt) {
                return;
            }
            mutation = attempt.mutation;
            attempt.mutation = null;
            application = applier;
            attempt.work++;
            work++;
        }
        CommittedMutationReplayer.AppliedMutation applied = null;
        boolean succeeded = false;
        try {
            applied = Objects.requireNonNull(application.apply(Objects.requireNonNull(mutation), Objects.requireNonNull(checkpoint)));
            if (applied.mutation().operationId().equals(mutation.operationId()) == false
                || applied.checkpoint().equals(checkpoint) == false) {
                throw new IOException("serving application returned a different operation or committed checkpoint");
            }
            synchronized (this) {
                succeeded = state == State.APPLYING && active == attempt;
                if (succeeded) {
                    state = State.READY;
                    active = null;
                }
            }
        } catch (IOException | RuntimeException | Error failed) {
            fail(attempt, failed);
        } finally {
            finishWork(attempt);
        }
        if (succeeded) {
            attempt.result.complete(applied);
        }
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
        settle(attempt);
    }

    private void finishWork(Attempt attempt) {
        synchronized (this) {
            attempt.work--;
            work--;
        }
        settle(attempt);
    }

    private void settle(Attempt attempt) {
        final boolean closeReady;
        final Throwable failure;
        synchronized (this) {
            closeReady = state == State.CLOSED && work == 0;
            failure = attempt != null && attempt.work == 0 ? attempt.failure : null;
            if (failure != null && active == attempt) {
                active = null;
            }
            if (attempt != null && attempt.work == 0 && (failure != null || state == State.CLOSED)) {
                // Queued jobs retain only this gate/attempt, not the canceled request or payload.
                attempt.preparer = null;
                attempt.mutation = null;
            }
            if (closeReady) {
                appender = null;
                applier = null;
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
