/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Admission before opening native preparation resources. Only resident contexts occupy the map;
 * cold logical descriptors do not. Each assigned physical group retains one context and its maximum
 * byte allowance while idle. Byte reservations are declared budgets, not measurements of actual RSS.
 * The owning context must release its ticket only after native IO/resources have stopped.
 */
@InternalApi
public final class PreparationEngineAdmission {
    /** Initial prototype ceiling from the capacity design; this is not a qualified engine count. */
    public static final int MAXIMUM_CONTEXTS = 256;

    /**
     * Resident accounting, including reservations retained after uncertain cleanup.
     * @param contexts admitted contexts whose resources have not been released
     * @param reservedBytes sum of declared resident-memory reservations
     * @param quarantinedContexts contexts retained after uncertain native cleanup
     */
    @InternalApi
    public record Stats(int contexts, long reservedBytes, int quarantinedContexts) {
    }

    private static final class GroupUsage {
        private int contexts;
        private long bytes;
    }

    private final Map<UUID, GroupUsage> groups;
    private final Map<ShardId, Ticket> resident = new HashMap<>();
    private final int maximumContexts;
    private final long maximumBytes;
    private final long maximumContextBytes;
    private final long groupReservedBytes;
    private int occupiedGroups;
    private long usedBytes;
    private long coveredReservedBytes;
    private int quarantinedContexts;

    /**
     * Configures bounded active state without opening any engine, thread or file.
     * @param assignedGroups immutable local physical assignments, at most 16
     * @param maximumContexts maximum resident contexts, between group count and the prototype ceiling
     * @param maximumBytes total declared resident-memory allowance
     * @param maximumContextBytes largest context reservation; one is protected per assigned group
     */
    public PreparationEngineAdmission(Set<UUID> assignedGroups, int maximumContexts, long maximumBytes, long maximumContextBytes) {
        final Set<UUID> assignments = Set.copyOf(assignedGroups);
        if (assignments.isEmpty()
            || assignments.size() > 16
            || maximumContexts < assignments.size()
            || maximumContexts > MAXIMUM_CONTEXTS
            || maximumContextBytes <= 0
            || maximumBytes <= 0
            || maximumContextBytes > maximumBytes / assignments.size()) {
            throw new IllegalArgumentException("resident engine budget cannot serve every assigned group");
        }
        this.maximumContexts = maximumContexts;
        this.maximumBytes = maximumBytes;
        this.maximumContextBytes = maximumContextBytes;
        this.groupReservedBytes = maximumContextBytes * assignments.size();
        final var usages = new HashMap<UUID, GroupUsage>();
        assignments.forEach(group -> usages.put(group, new GroupUsage()));
        this.groups = Map.copyOf(usages);
    }

    // Only the resource-owning context in this package receives tickets; clients must never own one.
    synchronized Ticket acquire(UUID group, ShardId shard, long reservationBytes) {
        final var usage = groups.get(Objects.requireNonNull(group));
        Objects.requireNonNull(shard);
        if (usage == null || reservationBytes <= 0 || reservationBytes > maximumContextBytes) {
            throw new IllegalArgumentException("invalid preparation group or context reservation");
        }
        if (resident.containsKey(shard)) {
            throw new OpenSearchRejectedExecutionException("preparation context is already resident for this shard");
        }
        final int occupiedAfter = occupiedGroups + (usage.contexts == 0 ? 1 : 0);
        final int protectedContexts = groups.size() - occupiedAfter;
        final long addedCoverage = Math.min(maximumContextBytes - Math.min(usage.bytes, maximumContextBytes), reservationBytes);
        final long protectedBytes = groupReservedBytes - coveredReservedBytes - addedCoverage;
        if (resident.size() >= maximumContexts - protectedContexts || reservationBytes > maximumBytes - usedBytes - protectedBytes) {
            throw new OpenSearchRejectedExecutionException("resident preparation engine count or byte budget is exhausted");
        }
        final var ticket = new Ticket(this, group, shard, reservationBytes);
        resident.put(shard, ticket);
        occupiedGroups = occupiedAfter;
        usage.contexts++;
        usage.bytes += reservationBytes;
        usedBytes += reservationBytes;
        coveredReservedBytes += addedCoverage;
        return ticket;
    }

    /** Returns fixed-cost accounting without walking cold descriptors or invoking engine code. */
    public synchronized Stats stats() {
        return new Stats(resident.size(), usedBytes, quarantinedContexts);
    }

    private synchronized void release(Ticket ticket) {
        if (ticket.released || ticket.quarantined) {
            return;
        }
        ticket.released = true;
        final var usage = groups.get(ticket.group);
        final long coverageBefore = Math.min(usage.bytes, maximumContextBytes);
        resident.remove(ticket.shard);
        usage.contexts--;
        usage.bytes -= ticket.bytes;
        usedBytes -= ticket.bytes;
        coveredReservedBytes -= coverageBefore - Math.min(usage.bytes, maximumContextBytes);
        if (usage.contexts == 0) {
            occupiedGroups--;
        }
    }

    private synchronized void quarantine(Ticket ticket, Object uncertainResources) {
        Objects.requireNonNull(uncertainResources);
        if (ticket.released) {
            throw new IllegalStateException("cannot quarantine a released preparation reservation");
        }
        if (ticket.quarantined == false) {
            ticket.quarantined = true;
            ticket.uncertainResources = uncertainResources;
            quarantinedContexts++;
        }
    }

    static final class Ticket implements Releasable {
        private final PreparationEngineAdmission owner;
        private final UUID group;
        private final ShardId shard;
        private final long bytes;
        private boolean released;
        private boolean quarantined;
        // Retain native handles/leases after uncertain cleanup; bounded by the resident context cap.
        private Object uncertainResources;

        private Ticket(PreparationEngineAdmission owner, UUID group, ShardId shard, long bytes) {
            this.owner = owner;
            this.group = group;
            this.shard = shard;
            this.bytes = bytes;
        }

        void quarantine(Object uncertainResources) {
            owner.quarantine(this, uncertainResources);
        }

        @Override
        public void close() {
            owner.release(this);
        }
    }
}
