/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.annotation.InternalApi;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Controller-authorized static journal placement. Bootstrap is a one-time operation on blank storage;
 * the caller must first durably reserve the deployment/group identities in the control plane. This
 * configuration does not authorize independent bootstrap after loss of an established quorum.
 *
 * @param deploymentId cluster namespace
 * @param localVoterId expected storage-lineage identity, not a replaceable hostname
 * @param directory preprovisioned durable volume directory
 * @param groups bounded physical groups assigned to this voter
 * @param bootstrap explicit initial-group creation authorization
 * @param limits admission and storage limits
 */
@InternalApi
public record JournalConfiguration(UUID deploymentId, UUID localVoterId, Path directory, List<Group> groups, boolean bootstrap,
    Limits limits) {
    /**
     * Validates and snapshots placement before any network or storage service starts.
     * @param deploymentId cluster namespace
     * @param localVoterId storage identity
     * @param directory provisioned durable directory
     * @param groups assigned physical groups
     * @param bootstrap initial creation authorization
     * @param limits resource limits
     */
    public JournalConfiguration {
        Objects.requireNonNull(deploymentId);
        Objects.requireNonNull(localVoterId);
        Objects.requireNonNull(directory);
        Objects.requireNonNull(limits);
        groups = List.copyOf(groups);
        if (groups.isEmpty() || groups.size() > limits.maximumGroups()) {
            throw new IllegalArgumentException("physical group count exceeds configured bounds");
        }
        final var ids = new HashSet<UUID>();
        Peer local = null;
        final var voters = new java.util.HashMap<UUID, Peer>();
        for (Group group : groups) {
            if (ids.add(group.id()) == false) {
                throw new IllegalArgumentException("duplicate physical group identity");
            }
            final Peer candidate = group.peers()
                .stream()
                .filter(peer -> peer.id().equals(localVoterId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("local voter is absent from an assigned group"));
            if (local != null && local.equals(candidate) == false) {
                throw new IllegalArgumentException("local voter addresses differ between groups");
            }
            local = candidate;
            for (Peer peer : group.peers()) {
                final Peer previous = voters.putIfAbsent(peer.id(), peer);
                if (previous != null && previous.equals(peer) == false) {
                    throw new IllegalArgumentException("voter placement differs between groups");
                }
            }
        }
    }

    /**
     * A storage-lineage identity and its authenticated transport address.
     * @param id unique durable storage identity
     * @param host certificate-verifiable DNS name or IP address
     * @param port dedicated replication port
     * @param availabilityZone distinct failure-domain label, verified by the controller
     */
    @InternalApi
    public record Peer(UUID id, String host, int port, String availabilityZone) {
        /**
         * Validates transport placement.
         * @param id storage identity
         * @param host transport host
         * @param port transport port
         * @param availabilityZone failure-domain label
         */
        public Peer {
            Objects.requireNonNull(id);
            Objects.requireNonNull(host);
            Objects.requireNonNull(availabilityZone);
            if (host.isBlank()
                || host.length() > 253
                || port < 1
                || port > 65535
                || availabilityZone.isBlank()
                || availabilityZone.length() > 256) {
                throw new IllegalArgumentException("invalid voter address or availability zone");
            }
        }
    }

    /**
     * One physical Raft group multiplexing many logical shard streams.
     * @param id unique control-plane-reserved group identity
     * @param peers three voters in three failure domains
     */
    @InternalApi
    public record Group(UUID id, List<Peer> peers) {
        /**
         * Validates a three-voter placement.
         * @param id group identity
         * @param peers voter placement
         */
        public Group {
            Objects.requireNonNull(id);
            peers = List.copyOf(peers);
            if (peers.size() != 3
                || peers.stream().map(Peer::id).distinct().count() != 3
                || peers.stream().map(Peer::availabilityZone).distinct().count() != 3
                || peers.stream().map(peer -> peer.host() + ":" + peer.port()).distinct().count() != 3) {
                throw new IllegalArgumentException("a physical group requires three distinct voters, addresses, and availability zones");
            }
        }
    }

    /**
     * Finite node admission, per-group disk-log buffers, and disk reserve. The configured buffers and
     * native transport overhead require additional heap beyond the pending-request byte budget.
     * @param maximumGroups physical groups per process, at most sixteen
     * @param maximumPayloadBytes per-operation payload bound
     * @param maximumPendingRequests accepted local proposals per process
     * @param maximumPendingBytes conservative retained proposal byte budget
     * @param queueBytes per-group log/write/appender queue budget
     * @param segmentBytes maximum disk segment and active-segment cache size
     * @param cachedSegmentBytes per-group closed-segment cache budget
     * @param maximumRetainedEntries per-group history bound, including log-entry metadata retained in heap
     * @param minimumFreeBytes disk space reserved for recovery and metadata
     */
    @InternalApi
    public record Limits(int maximumGroups, int maximumPayloadBytes, int maximumPendingRequests, long maximumPendingBytes, int queueBytes,
        int segmentBytes, long cachedSegmentBytes, long maximumRetainedEntries, long minimumFreeBytes) {
        /**
         * Validates that every bound is finite and can admit a maximum-size operation.
         * @param maximumGroups physical group bound
         * @param maximumPayloadBytes payload bound
         * @param maximumPendingRequests proposal count bound
         * @param maximumPendingBytes retained proposal bytes
         * @param queueBytes per-group queue bytes
         * @param segmentBytes disk segment bytes
         * @param cachedSegmentBytes closed-segment cache bytes
         * @param maximumRetainedEntries retained history positions
         * @param minimumFreeBytes disk reserve
         */
        public Limits {
            if (maximumGroups < 1
                || maximumGroups > 16
                || maximumPayloadBytes < 1
                || maximumPayloadBytes > OperationEnvelope.MAX_PAYLOAD_BYTES
                || maximumPendingRequests < 1
                || maximumPendingRequests > 65536
                || maximumPendingBytes < 6L * (maximumPayloadBytes + 512L)
                || queueBytes < 1024 * 1024
                || queueBytes > 128 * 1024 * 1024
                || queueBytes < maximumPayloadBytes + 1024
                || segmentBytes < queueBytes
                || cachedSegmentBytes < 0
                || maximumRetainedEntries < 1024
                || maximumRetainedEntries > 10_000_000
                || minimumFreeBytes < 1) {
                throw new IllegalArgumentException("invalid journal resource limits");
            }
        }

        /**
         * Returns conservative starting limits; these are not a capacity guarantee.
         * @return bounded default limits
         */
        public static Limits defaults() {
            return new Limits(
                4,
                4 * 1024 * 1024,
                256,
                128L * 1024 * 1024,
                8 * 1024 * 1024,
                16 * 1024 * 1024,
                16L * 1024 * 1024,
                100_000,
                1024L * 1024 * 1024
            );
        }
    }
}
