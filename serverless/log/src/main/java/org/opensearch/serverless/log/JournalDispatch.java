/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.util.function.CheckedSupplier;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Detaches SDK submission and completion from consensus locks. Native "async" submission can join
 * its own client executor (managed state machines bypass that extra handoff); native futures can
 * complete under leader/watch locks. Neither calling
 * back into the SDK nor running application continuations is safe on those completion threads.
 *
 * At most one queued task belongs to each charged permit: submission is dequeued before completion
 * can be queued. A permit stays charged until notification is dequeued. Executing notification tasks
 * release their permits before publishing, so dependent proposals work even with a one-request limit.
 */
final class JournalDispatch implements AutoCloseable {
    private final ThreadPoolExecutor executor;
    private int outstanding;
    private boolean closed;

    JournalDispatch(String name, JournalConfiguration.Limits limits) {
        executor = JournalExecutors.bounded(name + "-journal-dispatch", 4, limits.maximumPendingRequests());
    }

    <T> CompletionStage<T> submit(JournalAdmission.Permit permit, CheckedSupplier<CompletableFuture<T>, Exception> source)
        throws IOException {
        Objects.requireNonNull(permit);
        Objects.requireNonNull(source);
        synchronized (this) {
            if (closed) {
                permit.close();
                throw new IOException("journal dispatch is closed");
            }
            outstanding++;
        }
        final var result = new CompletableFuture<T>();
        try {
            executor.execute(() -> {
                try {
                    if (isClosed()) {
                        throw new IOException("journal closed before accepted submission started");
                    }
                    Objects.requireNonNull(source.get()).whenComplete((value, failure) -> publish(permit, result, value, failure));
                } catch (Exception failure) {
                    if (failure instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    publish(permit, result, null, failure);
                } catch (Error failure) {
                    publish(permit, result, null, failure);
                    throw failure;
                }
            });
        } catch (RejectedExecutionException failure) {
            // No source has been submitted and no application callback can exist yet.
            permit.close();
            finished();
            throw new IOException("journal dispatch cannot accept the proposal", failure);
        }
        return result.minimalCompletionStage();
    }

    private <T> void publish(JournalAdmission.Permit permit, CompletableFuture<T> result, T value, Throwable failure) {
        executor.execute(() -> {
            permit.close();
            try {
                if (failure == null) {
                    result.complete(value);
                } else {
                    result.completeExceptionally(failure);
                }
            } finally {
                finished();
            }
        });
    }

    private synchronized boolean isClosed() {
        return closed;
    }

    private synchronized void finished() {
        outstanding--;
        if (closed && outstanding == 0) {
            executor.shutdown();
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (outstanding == 0) {
            executor.shutdown();
        }
        // Native close can leave an uninterruptible I/O future. Keep the bounded dispatcher alive
        // for its eventual notification; do not reject a result or run it under an SDK lock.
    }

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }
}
