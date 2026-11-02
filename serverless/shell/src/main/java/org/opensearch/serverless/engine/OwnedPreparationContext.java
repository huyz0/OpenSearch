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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns admitted private resources after a successful binding. Close drains request work before
 * dispatching native cleanup, retaining resident admission through queued cleanup. The host supplies
 * a bounded lifecycle executor with reserved cleanup slots, keeps it alive until contexts close and
 * closes dependent mapper/index services only after successful closure. Quarantined failures require
 * process-level recovery before dependent teardown. Inline cleanup is refused. This owns resources,
 * not consensus authority; the host still supplies verified replay and ownership/schema exclusion.
 */
final class OwnedPreparationContext<P, R> {
    private PreparationWorkDispatcher.Binding<P, R> binding;
    private PrivatePreparationResources resources;
    private Executor lifecycleExecutor;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private boolean closing;

    /** Ownership transfers only if construction succeeds; otherwise resources remain caller-owned. */
    OwnedPreparationContext(
        PreparationWorkDispatcher dispatcher,
        Executor lifecycleExecutor,
        UUID group,
        ShardId shard,
        PrivatePreparationResources resources,
        PreparationDependencyGate.Appender<P> appender,
        PreparationDependencyGate.Applier<P, R> applier,
        PreparationDependencyGate.ResultVerifier<P, R> verifier
    ) {
        this.lifecycleExecutor = Objects.requireNonNull(lifecycleExecutor);
        this.resources = Objects.requireNonNull(resources);
        if (resources.group().equals(group) == false || resources.engine().config().getShardId().equals(shard) == false) {
            throw new IllegalArgumentException("owned preparation resources differ from the admitted group or shard");
        }
        binding = Objects.requireNonNull(dispatcher).bind(group, shard, appender, applier, verifier);
    }

    CompletionStage<R> submit(long retainedBytes, PreparationDependencyGate.Preparer<P> preparer) {
        final PreparationWorkDispatcher.Binding<P, R> current;
        synchronized (this) {
            if (closing) {
                return CompletableFuture.failedStage(new OpenSearchRejectedExecutionException("owned preparation context is closing"));
            }
            current = binding;
        }
        return current.submit(retainedBytes, preparer);
    }

    CompletionStage<Void> closeAsync() {
        final PreparationWorkDispatcher.Binding<P, R> current;
        synchronized (this) {
            if (closing) {
                return closed.minimalCompletionStage();
            }
            closing = true;
            current = binding;
        }
        current.closeAsync().whenComplete((ignored, failure) -> {
            if (failure == null) {
                dispatchCleanup();
            } else {
                quarantine(resources, failure);
            }
        });
        return closed.minimalCompletionStage();
    }

    private void dispatchCleanup() {
        final PrivatePreparationResources owned;
        final Executor executor;
        synchronized (this) {
            owned = resources;
            executor = lifecycleExecutor;
        }
        final var claimed = new AtomicBoolean();
        final var dispatching = new AtomicBoolean(true);
        final Thread submittingThread = Thread.currentThread();
        try {
            executor.execute(() -> {
                if (claimed.compareAndSet(false, true) == false) {
                    return;
                }
                if (dispatching.get() && Thread.currentThread() == submittingThread) {
                    quarantine(owned, new OpenSearchRejectedExecutionException("native cleanup cannot run inline"));
                    return;
                }
                Throwable failure = null;
                try {
                    owned.close();
                } catch (Exception | Error failed) {
                    failure = failed;
                }
                finishClose(failure);
            });
        } catch (RuntimeException | Error uncertain) {
            if (claimed.compareAndSet(false, true)) {
                // Even a post-acceptance throw cannot later close a quarantined lifecycle.
                // The queued wrapper becomes a no-op; its native resources stay charged.
                quarantine(owned, uncertain);
            }
            // If cleanup already started, its actual completion determines resource lifetime.
        } finally {
            dispatching.set(false);
        }
    }

    private void quarantine(PrivatePreparationResources owned, Throwable failure) {
        try {
            owned.quarantineLifecycleFailure(failure);
        } catch (RuntimeException | Error secondary) {
            if (secondary != failure) {
                failure.addSuppressed(secondary);
            }
        }
        finishClose(failure);
    }

    private void finishClose(Throwable failure) {
        synchronized (this) {
            binding = null;
            resources = null;
            lifecycleExecutor = null;
        }
        if (failure == null) {
            closed.complete(null);
        } else {
            closed.completeExceptionally(failure);
        }
    }
}
