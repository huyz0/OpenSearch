/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** Node-wide proposals retain their charge while preserving progress capacity for each physical group. */
final class JournalAdmission {
    private static final UUID SINGLE_GROUP = new UUID(0, 0);
    private final int maximumRequests;
    private final int maximumPayloadBytes;
    private final long maximumBytes;
    private final long reservedBytesPerGroup;
    private final Map<UUID, Usage> groups;
    private int occupiedGroups;
    private long coveredReservedBytes;
    private int requests;
    private long bytes;
    private boolean closed;

    JournalAdmission(JournalConfiguration.Limits limits) {
        this(limits, Set.of(SINGLE_GROUP));
    }

    JournalAdmission(JournalConfiguration.Limits limits, Set<UUID> groupIds) {
        validate(limits, groupIds.size());
        maximumRequests = limits.maximumPendingRequests();
        maximumPayloadBytes = limits.maximumPayloadBytes();
        maximumBytes = limits.maximumPendingBytes();
        reservedBytesPerGroup = charge(maximumPayloadBytes);
        groups = groupIds.stream().collect(Collectors.toUnmodifiableMap(id -> id, id -> new Usage()));
    }

    static void validate(JournalConfiguration.Limits limits, int groups) {
        if (groups < 0
            || groups > limits.maximumGroups()
            || limits.maximumPendingRequests() < groups
            || limits.maximumPendingBytes() < groups * charge(limits.maximumPayloadBytes())) {
            throw new IllegalArgumentException("journal budgets must reserve one maximum-size proposal for every active physical group");
        }
    }

    Permit acquire(int payloadBytes) throws IOException {
        return acquire(SINGLE_GROUP, payloadBytes);
    }

    synchronized Permit acquire(UUID group, int payloadBytes) throws IOException {
        final Usage usage = groups.get(group);
        if (usage == null || payloadBytes < 0 || payloadBytes > maximumPayloadBytes) {
            throw new IOException("journal admission unavailable: unassigned group or invalid payload size");
        }
        final long charge = charge(payloadBytes);
        final int otherOccupied = occupiedGroups - (usage.requests == 0 ? 0 : 1);
        final int reservedOtherRequests = groups.size() - 1 - otherOccupied;
        final long ownCovered = Math.min(usage.bytes, reservedBytesPerGroup);
        final long reservedOtherBytes = (groups.size() - 1L) * reservedBytesPerGroup - (coveredReservedBytes - ownCovered);
        if (closed || requests >= maximumRequests - reservedOtherRequests || charge > maximumBytes - bytes - reservedOtherBytes) {
            throw new IOException("journal admission unavailable: closed or pending proposal limit reached");
        }
        if (usage.requests++ == 0) {
            occupiedGroups++;
        }
        usage.bytes += charge;
        coveredReservedBytes += Math.min(usage.bytes, reservedBytesPerGroup) - ownCovered;
        requests++;
        bytes += charge;
        return new Permit(usage, charge);
    }

    private static long charge(int payloadBytes) {
        return 6L * (payloadBytes + 512L);
    }

    synchronized void close() {
        closed = true;
    }

    synchronized int pendingRequests() {
        return requests;
    }

    synchronized long pendingBytes() {
        return bytes;
    }

    private static final class Usage {
        private int requests;
        private long bytes;
    }

    final class Permit implements AutoCloseable {
        private final Usage usage;
        private final long charge;
        private final AtomicBoolean released = new AtomicBoolean();

        private Permit(Usage usage, long charge) {
            this.usage = usage;
            this.charge = charge;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                synchronized (JournalAdmission.this) {
                    final long before = Math.min(usage.bytes, reservedBytesPerGroup);
                    if (--usage.requests == 0) {
                        occupiedGroups--;
                    }
                    usage.bytes -= charge;
                    coveredReservedBytes += Math.min(usage.bytes, reservedBytesPerGroup) - before;
                    requests--;
                    bytes -= charge;
                }
            }
        }
    }
}
