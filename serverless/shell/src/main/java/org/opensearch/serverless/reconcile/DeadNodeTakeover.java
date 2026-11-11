/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Takes over a departed node's shards for {@link ReconcileScheduler}: rechecks that the node is really gone, then
 * claims its shards a slice at a time, spread over a lease so the survivors do not all move at once.
 */
final class DeadNodeTakeover {

    private final ReconcileScheduler scheduler;

    DeadNodeTakeover(ReconcileScheduler scheduler) {
        this.scheduler = scheduler;
    }

    /** One observed absence, discarded on rejoin so a later absence of the same process is independent. */
    record Departure(org.opensearch.serverless.membership.NodeLease lease, long noticedAt, AtomicBoolean takenOver) {
        Departure(org.opensearch.serverless.membership.NodeLease lease, long noticedAt) {
            this(lease, noticedAt, new AtomicBoolean());
        }
    }

    void pruneDepartureHistory() {
        final long now = scheduler.clock.getAsLong();
        scheduler.departures.entrySet()
            .removeIf(entry -> now - entry.getValue().noticedAt() > ReconcileScheduler.RECENT_DEPARTURE_TTLS * scheduler.leaseTtlMillis());
    }

    /**
     * Takes a departed node's shards once its lease, read from the store, has run out.
     *
     * <p>A node leaves the membership view when its lease read fails or comes back late, as well as when it dies, and a
     * live node's shards taken on that evidence is a storm of contested activations. So the lease is read first; a
     * live one is looked at again just after it would expire, which is when a dead node's departure is real. The
     * absence is marked taken over only when the takeover happens, so a false departure does not use up the
     * proactive takeover a later real departure gets.
     */
    void takeOverIfGone(Departure departure, int rechecks) {
        try (org.opensearch.common.lease.Releasable work = scheduler.activity.enter()) {
            if (work == null) {
                return;
            }
            final var left = departure.lease();
            final String incarnation = left.nodeId() + "/" + left.ephemeralId();
            if (scheduler.closed || departure.takenOver().get() || scheduler.departures.get(incarnation) != departure) {
                return;
            }
            long liveUntil;
            try {
                liveUntil = scheduler.loop.leaseLiveUntil(left.nodeId()).orElse(Long.MIN_VALUE);
            } catch (Exception e) {
                // Unknown, not gone: look again shortly.
                liveUntil = scheduler.clock.getAsLong();
            }
            if (liveUntil == Long.MIN_VALUE) {
                if (departure.takenOver().compareAndSet(false, true)) {
                    takeOver(left.nodeId());
                }
                return;
            }
            if (rechecks >= ReconcileScheduler.DEPARTURE_RECHECKS) {
                return;
            }
            final long delay = Math.max(0L, liveUntil - scheduler.clock.getAsLong()) + scheduler.renewalInterval.millis();
            scheduler.threadPool.schedule(
                () -> takeOverIfGone(departure, rechecks + 1),
                TimeValue.timeValueMillis(delay),
                ThreadPool.Names.GENERIC
            );
        }
    }

    /** The two looks at a departed node's shards; see BackgroundReconciler#takeOverFrom. */
    void takeOver(String deadNodeId) {
        try (org.opensearch.common.lease.Releasable work = scheduler.activity.enter()) {
            if (work == null) {
                return;
            }
            try {
                final java.util.List<Map.Entry<String, Integer>> later = scheduler.loop.takeOverFrom(deadNodeId);
                if (later.isEmpty() == false && scheduler.closed == false) {
                    scheduler.threadPool.schedule(
                        () -> takeOverRemaining(deadNodeId, later, false),
                        scheduler.renewalInterval,
                        ThreadPool.Names.GENERIC
                    );
                }
            } catch (Exception e) {
                ReconcileScheduler.logger.warn("could not take over the shards of departed node " + deadNodeId, e);
            }
        }
    }

    void takeOverRemaining(String deadNodeId, java.util.List<Map.Entry<String, Integer>> claims, boolean last) {
        try (org.opensearch.common.lease.Releasable work = scheduler.activity.enter()) {
            if (work == null) {
                return;
            }
            final java.util.List<Map.Entry<String, Integer>> deferred = scheduler.loop.takeOverRemaining(deadNodeId, claims, last);
            if (last == false && deferred.isEmpty() == false && scheduler.closed == false) {
                scheduler.threadPool.schedule(
                    () -> takeOverRemaining(deadNodeId, deferred, true),
                    TimeValue.timeValueMillis(scheduler.renewalInterval.millis() * ReconcileScheduler.RENEWALS_PER_TTL),
                    ThreadPool.Names.GENERIC
                );
            }
        }
    }
}
