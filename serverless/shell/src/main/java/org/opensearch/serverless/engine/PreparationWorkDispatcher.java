/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Host-owned preparation workers and declared request-byte/count admission. One worker per assigned
 * group isolates native stalls. A request keeps its charge until its result and every queued/running
 * wrapper settle. This does not own engines or establish ownership/schema or actual heap residency.
 */
final class PreparationWorkDispatcher {
    record Stats(int bindings, int pendingRequests, long pendingBytes) {
    }

    private final PreparationEngineAdmission bindingAdmission;
    private final PreparationEngineAdmission requestAdmission;
    private final Map<UUID, ThreadPoolExecutor> workers;
    private final Map<ShardId, Binding> bindings = new HashMap<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private boolean closing;
    private boolean workersStopped;

    @FunctionalInterface
    interface WorkerFactory {
        ThreadPoolExecutor create(UUID group);
    }

    PreparationWorkDispatcher(
        String name,
        Set<UUID> groups,
        int maximumBindings,
        int maximumRequests,
        long maximumPendingBytes,
        long maximumRequestBytes,
        ThreadContext context
    ) {
        this(groups, maximumBindings, maximumRequests, maximumPendingBytes, maximumRequestBytes, workers(name, maximumRequests, context));
    }

    private static WorkerFactory workers(String name, int maximumRequests, ThreadContext context) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(context);
        return group -> OpenSearchExecutors.newFixed(
            name + "-prepare-" + group,
            1,
            maximumRequests,
            OpenSearchExecutors.daemonThreadFactory(name + "-prepare-" + group),
            context
        );
    }

    // Inject faults after actual queue acceptance without modifying native code.
    PreparationWorkDispatcher(
        Set<UUID> groups,
        int maximumBindings,
        int maximumRequests,
        long maximumPendingBytes,
        long maximumRequestBytes,
        WorkerFactory factory
    ) {
        Objects.requireNonNull(factory);
        final var assigned = Set.copyOf(groups);
        bindingAdmission = new PreparationEngineAdmission(assigned, maximumBindings, maximumBindings, 1);
        requestAdmission = new PreparationEngineAdmission(assigned, maximumRequests, maximumPendingBytes, maximumRequestBytes);
        final var created = new HashMap<UUID, ThreadPoolExecutor>();
        try {
            for (var group : assigned) {
                created.put(group, Objects.requireNonNull(factory.create(group)));
            }
            workers = Map.copyOf(created);
        } catch (RuntimeException | Error failure) {
            created.values().forEach(ThreadPoolExecutor::shutdown);
            throw failure;
        }
    }

    Binding bind(UUID group, ShardId shard, PreparationCommitSequencer.Appender appender, PreparationCommitSequencer.Applier applier) {
        Objects.requireNonNull(appender);
        Objects.requireNonNull(applier);
        synchronized (this) {
            if (closing) {
                throw new OpenSearchRejectedExecutionException("private work dispatcher is closing");
            }
            if (bindings.containsKey(shard)) {
                throw new OpenSearchRejectedExecutionException("private stream binding already exists");
            }
            final var ticket = bindingAdmission.acquire(group, shard, 1);
            try {
                final var binding = new Binding(group, shard, ticket, appender, applier);
                bindings.put(shard, binding);
                return binding;
            } catch (RuntimeException | Error failure) {
                ticket.close();
                throw failure;
            }
        }
    }

    synchronized Stats stats() {
        final var pending = requestAdmission.stats();
        return new Stats(bindings.size(), pending.contexts(), pending.reservedBytes());
    }

    final class Binding {
        private final UUID group;
        private final ShardId shard;
        private final PreparationEngineAdmission.Ticket ticket;
        private final PreparationCommitSequencer gate;
        private final CompletableFuture<Void> closed = new CompletableFuture<>();
        private Request pending;
        private boolean closing;
        private boolean gateClosed;
        private boolean finalizing;

        private Binding(
            UUID group,
            ShardId shard,
            PreparationEngineAdmission.Ticket ticket,
            PreparationCommitSequencer.Appender appender,
            PreparationCommitSequencer.Applier applier
        ) {
            this.group = group;
            this.shard = shard;
            this.ticket = ticket;
            gate = new PreparationCommitSequencer(task -> enqueue(this, task), appender, applier);
        }

        CompletionStage<CommittedMutationReplayer.AppliedMutation> submit(
            long retainedBytes,
            PreparationCommitSequencer.Preparer preparer
        ) {
            Objects.requireNonNull(preparer);
            final Request request;
            synchronized (PreparationWorkDispatcher.this) {
                if (closing || PreparationWorkDispatcher.this.closing || pending != null) {
                    return CompletableFuture.failedStage(
                        new OpenSearchRejectedExecutionException("private stream has no request capacity")
                    );
                }
                try {
                    request = new Request(this, requestAdmission.acquire(group, shard, retainedBytes));
                } catch (RuntimeException rejected) {
                    return CompletableFuture.failedStage(rejected);
                }
                pending = request;
            }
            try {
                gate.submit(preparer).whenComplete((value, failure) -> outcome(request, value, failure));
            } catch (RuntimeException | Error failure) {
                gate.fence(failure);
                outcome(request, null, failure);
            }
            return request.result.minimalCompletionStage();
        }

        /** Waits for native work and all retained queue wrappers; never cancels an accepted append. */
        CompletionStage<Void> closeAsync() {
            final boolean start;
            synchronized (PreparationWorkDispatcher.this) {
                start = closing == false;
                closing = true;
            }
            if (start) {
                gate.closeAsync().whenComplete((ignored, failure) -> {
                    synchronized (PreparationWorkDispatcher.this) {
                        gateClosed = true;
                    }
                    finishBinding(this);
                });
            }
            return closed.minimalCompletionStage();
        }
    }

    private final class Request {
        private final Binding binding;
        private final PreparationEngineAdmission.Ticket ticket;
        private final CompletableFuture<CommittedMutationReplayer.AppliedMutation> result = new CompletableFuture<>();
        private int jobs;
        private boolean outcomeKnown;
        private boolean settled;
        private CommittedMutationReplayer.AppliedMutation value;
        private Throwable failure;

        private Request(Binding binding, PreparationEngineAdmission.Ticket ticket) {
            this.binding = binding;
            this.ticket = ticket;
        }
    }

    private final class Job implements Runnable {
        private final Request request;
        private Runnable action;
        private boolean started;
        private boolean finished;

        private Job(Request request, Runnable action) {
            this.request = request;
            this.action = action;
        }

        @Override
        public void run() {
            final Runnable current;
            synchronized (PreparationWorkDispatcher.this) {
                if (finished) {
                    return;
                }
                started = true;
                current = action;
                action = null;
            }
            try {
                if (current != null) {
                    current.run();
                }
            } finally {
                finishJob(this, false);
            }
        }
    }

    private void enqueue(Binding binding, Runnable action) {
        final Job job;
        synchronized (this) {
            if (closing || binding.closing || binding.pending == null) {
                throw new OpenSearchRejectedExecutionException("private stream worker is closing");
            }
            job = new Job(binding.pending, action);
            job.request.jobs++;
        }
        try {
            workers.get(binding.group).execute(job);
        } catch (RejectedExecutionException rejected) {
            finishJob(job, true);
            throw rejected;
        } catch (RuntimeException | Error uncertain) {
            synchronized (this) {
                if (job.started == false) {
                    job.action = null;
                }
            }
            // Acceptance is unknown. Keep the charge until the wrapper drains. If it never
            // runs, this capacity cannot be reused before process recovery.
            throw uncertain;
        }
    }

    private void finishJob(Job job, boolean dispatchFailed) {
        synchronized (this) {
            // A decorator may throw after dispatch. Running work keeps its charge until its
            // own finally; rejected queued work is canceled before any native action can run.
            if (job.finished || (dispatchFailed && job.started)) {
                return;
            }
            job.finished = true;
            job.action = null;
            job.request.jobs--;
        }
        settle(job.request);
    }

    private void outcome(Request request, CommittedMutationReplayer.AppliedMutation value, Throwable failure) {
        synchronized (this) {
            if (request.outcomeKnown) {
                return;
            }
            request.outcomeKnown = true;
            request.value = value;
            request.failure = failure;
        }
        settle(request);
    }

    private void settle(Request request) {
        synchronized (this) {
            if (request.settled || request.outcomeKnown == false || request.jobs != 0) {
                return;
            }
            request.settled = true;
            request.binding.pending = null;
        }
        request.ticket.close();
        finishBinding(request.binding);
        if (request.failure == null) {
            request.result.complete(request.value);
        } else {
            request.result.completeExceptionally(request.failure);
        }
    }

    private void finishBinding(Binding binding) {
        synchronized (this) {
            if (binding.finalizing || binding.closing == false || binding.gateClosed == false || binding.pending != null) {
                return;
            }
            binding.finalizing = true;
        }
        binding.ticket.close();
        synchronized (this) {
            bindings.remove(binding.shard, binding);
        }
        binding.closed.complete(null);
        finishClose();
    }

    CompletionStage<Void> closeAsync() {
        final Binding[] snapshot;
        synchronized (this) {
            closing = true;
            snapshot = bindings.values().toArray(Binding[]::new);
        }
        for (var binding : snapshot) {
            binding.closeAsync();
        }
        finishClose();
        return closed.minimalCompletionStage();
    }

    private void finishClose() {
        synchronized (this) {
            if (closing == false || workersStopped || bindings.isEmpty() == false) {
                return;
            }
            workersStopped = true;
        }
        workers.values().forEach(ThreadPoolExecutor::shutdown);
        closed.complete(null);
    }
}
