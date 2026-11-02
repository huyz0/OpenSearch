/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.log.JournalConfiguration;
import org.opensearch.serverless.log.JournalGroupRecord;
import org.opensearch.serverless.log.JournalMembershipRecord;
import org.opensearch.serverless.log.RatisJournal;
import org.opensearch.serverless.metadata.JournalGroupCatalog;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Bounded host lifecycle for journal bootstrap and membership reconciliation. Catalog I/O runs only
 * on the node's dedicated one-worker, one-queued-task executor. Native progress uses cached durable
 * intent, one pending operation per group and coalesced wakeups. Readiness is historical controller
 * state, not quorum read authority or a serving-reader guarantee.
 */
@InternalApi
public final class JournalController implements Closeable {
    /** Fast local control progress, without a catalog read on every pass. */
    public static final TimeValue TICK_INTERVAL = TimeValue.timeValueMillis(200);
    private static final long METADATA_INTERVAL_NANOS = TimeValue.timeValueSeconds(30).nanos();
    private static final long RETRY_INTERVAL_NANOS = TimeValue.timeValueSeconds(5).nanos();

    /** Local reconciliation state; none of these values certifies a current search read. */
    public enum State {
        /** Native startup has not completed. */
        OPENING,
        /** Initial all-voter activation has not completed. */
        ACTIVATING,
        /** An established local activation marker is still replaying. */
        CATCHING_UP,
        /** Local identity is a candidate, not yet a completed voter. */
        LEARNER,
        /** The last observed durable head names this voter and its activation is applied. */
        ACTIVE,
        /** The completed durable head excludes this local identity. */
        RETIRED,
        /** Authority or native state cannot currently be established. */
        UNAVAILABLE,
        /** The host has stopped admission and reconciliation. */
        CLOSED
    }

    private static final class Progress {
        private JournalGroupRecord genesis;
        private JournalMembershipRecord membership;
        private CompletableFuture<?> pending;
        private long nextMetadata;
        private boolean metadataValid;
        private boolean metadataAttempted;
        private volatile State state = State.OPENING;
        private volatile String failure;
    }

    private final JournalConfiguration configuration;
    private final JournalGroupCatalog catalog;
    private final X509KeyManager keyManager;
    private final X509TrustManager trustManager;
    private final RatisJournal.DiskSpaceProbe diskSpace;
    private final ThreadPool threadPool;
    private final LongSupplier nanoTime;
    private final Map<UUID, Progress> progress;
    private final CompletableFuture<RatisJournal> opened = new CompletableFuture<>();
    private final AtomicBoolean queued = new AtomicBoolean();
    private final AtomicBoolean refreshRequested = new AtomicBoolean();
    private final AtomicLong passes = new AtomicLong();
    private volatile boolean closed;
    private volatile RatisJournal journal;
    private Scheduler.Cancellable backstop;
    private boolean started;

    /**
     * Constructs an unstarted controller; attach it to the host before starting it.
     * @param configuration explicit provisioned assignment and bootstrap authorization
     * @param catalog deployment-scoped durable catalog
     * @param keyManager member credentials
     * @param trustManager deployment-scoped journal trust
     * @param diskSpace actual-volume reserve probe
     * @param threadPool host pool containing {@link ServerlessNode#JOURNAL_CONTROLLER_POOL}
     * @param nanoTime monotonic clock, independent of object-store timestamps
     */
    public JournalController(
        JournalConfiguration configuration,
        JournalGroupCatalog catalog,
        X509KeyManager keyManager,
        X509TrustManager trustManager,
        RatisJournal.DiskSpaceProbe diskSpace,
        ThreadPool threadPool,
        LongSupplier nanoTime
    ) {
        this.configuration = Objects.requireNonNull(configuration);
        this.catalog = Objects.requireNonNull(catalog);
        this.keyManager = Objects.requireNonNull(keyManager);
        this.trustManager = Objects.requireNonNull(trustManager);
        this.diskSpace = Objects.requireNonNull(diskSpace);
        this.threadPool = Objects.requireNonNull(threadPool);
        final var executor = threadPool.info(ServerlessNode.JOURNAL_CONTROLLER_POOL);
        if (executor == null
            || executor.getMin() != 1
            || executor.getMax() != 1
            || executor.getQueueSize() == null
            || executor.getQueueSize().singles() != 1) {
            throw new IllegalArgumentException("journal control requires exactly one worker and one queued task");
        }
        this.nanoTime = Objects.requireNonNull(nanoTime);
        final var assigned = new HashMap<UUID, Progress>();
        configuration.groups().forEach(group -> assigned.put(group.id(), new Progress()));
        progress = Map.copyOf(assigned);
    }

    /**
     * Starts native opening on the bounded controller worker. Initial activation proceeds after
     * opening, so three hosts can start sequentially without waiting for one another's activation.
     * @throws IOException if closed, already started or the host executor rejects startup
     */
    public synchronized void start() throws IOException {
        if (closed || started) {
            throw new IOException("journal controller is closed or already started");
        }
        started = true;
        try {
            threadPool.executor(ServerlessNode.JOURNAL_CONTROLLER_POOL).execute(this::open);
        } catch (RuntimeException failure) {
            opened.completeExceptionally(failure);
            throw new IOException("journal controller startup was rejected", failure);
        }
    }

    private void open() {
        try {
            if (configuration.bootstrap()) {
                for (var group : configuration.groups()) {
                    if (group.peers().stream().anyMatch(peer -> peer.id().equals(configuration.localVoterId()))) {
                        requireOpen();
                        catalog.reserveProtocol(group, configuration.limits());
                    }
                }
            }
            requireOpen();
            final var local = catalog.openAssignedJournal(configuration, keyManager, trustManager, diskSpace);
            synchronized (this) {
                journal = local;
                if (closed == false) {
                    backstop = threadPool.scheduleWithFixedDelay(this::requestReconcile, TICK_INTERVAL, ThreadPool.Names.SAME);
                }
            }
            if (closed) {
                local.close();
                throw new IOException("journal controller closed during native startup");
            }
            opened.complete(local);
            requestReconcile();
        } catch (Exception failure) {
            progress.values().forEach(state -> rememberFailure(state, failure));
            opened.completeExceptionally(failure);
        } catch (Error failure) {
            progress.values().forEach(state -> rememberFailure(state, failure));
            opened.completeExceptionally(failure);
            throw failure;
        }
    }

    /**
     * Waits only for native opening, not all-voter activation. Startup failure is propagated without
     * cancelling accepted persistence; close still owns a native open that finishes later.
     * @return opened local journal
     * @throws IOException on startup failure, interruption or close
     */
    public RatisJournal awaitOpened() throws IOException {
        try {
            final var local = opened.get();
            requireOpen();
            return local;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for journal startup", failure);
        } catch (ExecutionException failure) {
            throw new IOException("journal startup failed", failure.getCause());
        }
    }

    /** Coalesces a local progress edge without forcing an object-store read. */
    public void requestReconcile() {
        if (closed || journal == null || queued.compareAndSet(false, true) == false) {
            return;
        }
        try {
            threadPool.executor(ServerlessNode.JOURNAL_CONTROLLER_POOL).execute(() -> {
                try {
                    if (closed == false) {
                        reconcile();
                    }
                } finally {
                    queued.set(false);
                }
            });
        } catch (RuntimeException rejected) {
            queued.set(false);
            // Closing executors reject; the periodic backstop retries a live coalesced edge.
        }
    }

    /** Requests a catalog reload after an explicit controller-side placement change. */
    public void requestCatalogRefresh() {
        refreshRequested.set(true);
        requestReconcile();
    }

    private void reconcile() {
        final boolean refresh = refreshRequested.getAndSet(false);
        final long now = nanoTime.getAsLong();
        for (var entry : progress.entrySet()) {
            if (closed) {
                return;
            }
            final var state = entry.getValue();
            try {
                if (refresh || state.metadataAttempted == false || now - state.nextMetadata >= 0) {
                    state.metadataAttempted = true;
                    state.genesis = catalog.read(entry.getKey()).orElseThrow(() -> new IOException("assigned genesis is missing"));
                    state.membership = state.genesis.activation() == null ? null : catalog.membership(entry.getKey());
                    state.metadataValid = true;
                    state.nextMetadata = now + METADATA_INTERVAL_NANOS;
                }
                if (state.metadataValid) {
                    reconcile(entry.getKey(), state);
                    state.failure = null;
                }
            } catch (Exception unavailable) {
                rememberFailure(state, unavailable);
                state.metadataValid = false;
                state.nextMetadata = now + RETRY_INTERVAL_NANOS;
            }
        }
        passes.incrementAndGet();
    }

    private void reconcile(UUID group, Progress state) throws IOException {
        if (state.pending != null && state.pending.isDone()) {
            // A failed native request remains ambiguous. Inspect committed state before retrying.
            state.pending = null;
        }
        if (state.membership != null) {
            final UUID local = configuration.localVoterId();
            final boolean voter = state.membership.voters().peers().stream().anyMatch(peer -> peer.id().equals(local));
            final boolean candidate = state.membership.pending() != null
                && state.membership.pending().target().peers().stream().anyMatch(peer -> peer.id().equals(local));
            if (voter == false && candidate == false) {
                state.state = State.RETIRED;
                return;
            }
        }
        final boolean leaderReady = journal.isLeaderReady(group);
        final RatisJournal.GroupActivation activation;
        try {
            activation = journal.groupActivation(group);
        } catch (IOException awaitingActivation) {
            state.state = state.genesis.activation() == null ? State.ACTIVATING : State.CATCHING_UP;
            if (state.genesis.activation() == null && state.pending == null && leaderReady) {
                track(state, journal.activateGroup(group).toCompletableFuture());
            }
            return;
        }
        if (state.genesis.activation() == null) {
            state.genesis = catalog.publishActivation(journal, group);
            state.membership = catalog.membership(group);
        }
        if (activation.checkpoint().equals(state.genesis.activation()) == false) {
            throw new IOException("local activation differs from the durable controller head");
        }
        if (state.membership.pending() != null) {
            final var receipt = journal.replacementReceipt(state.membership);
            if (receipt.isPresent()) {
                state.membership = catalog.publishReplacement(journal, state.membership).completedMembership();
            } else if (state.pending == null && leaderReady) {
                track(state, journal.advanceReplacement(state.membership).toCompletableFuture());
            }
        }
        final UUID local = configuration.localVoterId();
        if (state.membership.voters().peers().stream().anyMatch(peer -> peer.id().equals(local))) {
            state.state = State.ACTIVE;
        } else {
            state.state = state.membership.pending() == null ? State.RETIRED : State.LEARNER;
        }
    }

    private static void rememberFailure(Progress state, Throwable failure) {
        final String message = failure.toString();
        state.failure = message.substring(0, Math.min(1024, message.length()));
        state.state = State.UNAVAILABLE;
    }

    private void track(Progress state, CompletableFuture<?> pending) {
        state.pending = pending;
        pending.whenComplete((ignored, failure) -> requestReconcile());
    }

    private void requireOpen() throws IOException {
        if (closed) {
            throw new IOException("journal controller is closed");
        }
    }

    /**
     * Returns bounded local reconciliation state, not current read authority.
     * @param group assigned physical group
     * @return current local controller state
     */
    public State state(UUID group) {
        final var local = progress.get(group);
        if (local == null) {
            throw new IllegalArgumentException("unassigned journal group");
        }
        return closed ? State.CLOSED : local.state;
    }

    /**
     * Returns a bounded diagnostic from the most recent failed control pass until reconciliation succeeds.
     * @param group assigned physical group
     * @return failure description, if any
     */
    public Optional<String> failure(UUID group) {
        final var local = progress.get(group);
        if (local == null) {
            throw new IllegalArgumentException("unassigned journal group");
        }
        return Optional.ofNullable(local.failure);
    }

    /**
     * Reports completed control passes, including unavailable groups, for host observability.
     * @return monotonically increasing pass count
     */
    public long reconciliationPasses() {
        return passes.get();
    }

    /**
     * Returns an opened journal until close; callers still need group readiness and read authority.
     * @return local journal if native opening completed and the host is not closed
     */
    public Optional<RatisJournal> journal() {
        return closed ? Optional.empty() : Optional.ofNullable(journal);
    }

    @Override
    public void close() throws IOException {
        final RatisJournal local;
        synchronized (this) {
            closed = true;
            if (backstop != null) {
                backstop.cancel();
            }
            local = journal;
        }
        opened.completeExceptionally(new IOException("journal controller closed"));
        if (local != null) {
            local.close();
        }
    }
}
