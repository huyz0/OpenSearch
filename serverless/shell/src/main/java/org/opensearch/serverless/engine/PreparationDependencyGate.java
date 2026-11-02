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
 * One pending prepared request per logical stream until append, application and mandatory result verification finish.
 * Supports mutations and result-only proposals without manufacturing a document operation for a no-op.
 * Adapters must validate ownership/schema, supply real journal append and committed replay, and close
 * private resources only after closeAsync settles. No native/SDK work or completion runs under the
 * state monitor. This initial gate deliberately does not pipeline independent document keys.
 * The host must supply a bounded, group-isolated executor and account for queued wrappers and
 * pending results, including capacity for an admitted stream's application handoff. Do not use inline
 * or caller-runs execution on a journal completion thread. Rejection conservatively fences the context; proven
 * proposal-free rejection handling and runtime admission still belong to the native adapter.
 */
final class PreparationDependencyGate<P, R> {
    enum State {
        READY,
        PREPARING,
        APPENDING,
        APPLYING,
        FENCED,
        CLOSED
    }

    @FunctionalInterface
    interface Preparer<P> {
        P prepare() throws IOException;
    }

    @FunctionalInterface
    interface Appender<P> {
        CompletionStage<LogCheckpoint> append(P proposal) throws IOException;
    }

    @FunctionalInterface
    interface Applier<P, R> {
        R apply(P proposal, LogCheckpoint checkpoint) throws IOException;
    }

    /** Mandatory host validation of original request/result identity and the applicable committed checkpoint. */
    @FunctionalInterface
    interface ResultVerifier<P, R> {
        void verify(P proposal, LogCheckpoint physicalCheckpoint, R applied) throws IOException;
    }

    private static final class Attempt<P, R> {
        private final CompletableFuture<R> result = new CompletableFuture<>();
        private int work;
        private Preparer<P> preparer;
        private P proposal;
        private Throwable failure;
    }

    private final Executor executor;
    private Appender<P> appender;
    private Applier<P, R> applier;
    private ResultVerifier<P, R> verifier;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private State state = State.READY;
    private Attempt<P, R> active;
    private int work;

    PreparationDependencyGate(Executor executor, Appender<P> appender, Applier<P, R> applier, ResultVerifier<P, R> verifier) {
        this.executor = Objects.requireNonNull(executor);
        this.appender = Objects.requireNonNull(appender);
        this.applier = Objects.requireNonNull(applier);
        this.verifier = Objects.requireNonNull(verifier);
    }

    CompletionStage<R> submit(Preparer<P> preparer) {
        Objects.requireNonNull(preparer);
        final Attempt<P, R> attempt;
        synchronized (this) {
            if (state != State.READY) {
                return CompletableFuture.failedStage(
                    new OpenSearchRejectedExecutionException("private preparation is not ready: " + state)
                );
            }
            active = attempt = new Attempt<>();
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

    private void prepare(Attempt<P, R> attempt) {
        final Preparer<P> preparer;
        synchronized (this) {
            if (state != State.PREPARING || active != attempt) {
                return;
            }
            preparer = attempt.preparer;
            attempt.preparer = null;
            attempt.work++;
            work++;
        }
        final P proposal;
        final CompletionStage<LogCheckpoint> append;
        try {
            proposal = Objects.requireNonNull(preparer.prepare());
            synchronized (this) {
                if (state != State.PREPARING || active != attempt) {
                    throw new IOException("private preparation was fenced before append", attempt.failure);
                }
                state = State.APPENDING;
                attempt.proposal = proposal;
            }
            append = Objects.requireNonNull(appender.append(proposal));
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

    private void apply(Attempt<P, R> attempt, LogCheckpoint checkpoint) {
        final P proposal;
        final Applier<P, R> application;
        final ResultVerifier<P, R> verification;
        synchronized (this) {
            if (state != State.APPLYING || active != attempt) {
                return;
            }
            proposal = attempt.proposal;
            attempt.proposal = null;
            application = applier;
            verification = verifier;
            attempt.work++;
            work++;
        }
        R applied = null;
        boolean succeeded = false;
        try {
            applied = Objects.requireNonNull(application.apply(Objects.requireNonNull(proposal), Objects.requireNonNull(checkpoint)));
            verification.verify(proposal, checkpoint, applied);
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
        final Attempt<P, R> attempt;
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
        final Attempt<P, R> attempt;
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

    private void fail(Attempt<P, R> attempt, Throwable cause) {
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

    private void finishWork(Attempt<P, R> attempt) {
        synchronized (this) {
            attempt.work--;
            work--;
        }
        settle(attempt);
    }

    private void settle(Attempt<P, R> attempt) {
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
                attempt.proposal = null;
            }
            if (closeReady) {
                appender = null;
                applier = null;
                verifier = null;
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
