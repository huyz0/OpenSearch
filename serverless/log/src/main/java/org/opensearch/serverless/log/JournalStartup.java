/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Validates catalog authority before opening a volume or starting native services. */
final class JournalStartup {
    private JournalStartup() {}

    record Authorized(Map<UUID, LogCheckpoint> activations, Set<UUID> retired, Map<UUID, RaftGroup> initialGroups) {
        Authorized {
            activations = Map.copyOf(activations);
            retired = Set.copyOf(retired);
            initialGroups = Map.copyOf(initialGroups);
        }
    }

    static Authorized authorize(
        JournalConfiguration configuration,
        Map<UUID, JournalMembershipRecord> memberships,
        Map<UUID, JournalMembershipRecord> originalJoins
    ) throws IOException {
        if (memberships.size() > configuration.groups().size() || originalJoins.size() > configuration.groups().size()) {
            throw new IOException("journal startup authority exceeds assigned group bounds");
        }
        final var heads = Map.copyOf(memberships);
        final var joins = Map.copyOf(originalJoins);
        final var assigned = configuration.groups()
            .stream()
            .map(JournalConfiguration.Group::id)
            .collect(java.util.stream.Collectors.toSet());
        if (assigned.containsAll(heads.keySet()) == false || assigned.containsAll(joins.keySet()) == false) {
            throw new IOException("journal startup authority names an unassigned group");
        }
        final var activations = new HashMap<UUID, LogCheckpoint>();
        final var retired = new HashSet<UUID>();
        final var initial = new HashMap<UUID, RaftGroup>();
        final UUID local = configuration.localVoterId();
        for (var group : configuration.groups()) {
            final boolean original = group.peers().stream().anyMatch(peer -> peer.id().equals(local));
            final var head = heads.get(group.id());
            final var join = joins.get(group.id());
            if (head == null) {
                if (original == false || join != null) {
                    throw new IOException("a learner requires activated genesis and its original durable intent");
                }
                initial.put(group.id(), raftGroup(group));
                continue;
            }
            if (head.genesis().deploymentId().equals(configuration.deploymentId()) == false
                || head.genesis().initialPlacement().id().equals(group.id()) == false
                || head.genesis()
                    .equals(new JournalGroupRecord(configuration.deploymentId(), group, head.genesis().activation())) == false) {
                throw new IOException("journal startup membership differs from its immutable genesis");
            }
            if (head.voters().peers().stream().anyMatch(peer -> peer.id().equals(local) && peer.equals(configuration.localPeer()) == false)
                || (head.pending() != null
                    && head.pending()
                        .target()
                        .peers()
                        .stream()
                        .anyMatch(peer -> peer.id().equals(local) && peer.equals(configuration.localPeer()) == false))) {
                throw new IOException("current membership rebinds the original local transport identity");
            }
            activations.put(group.id(), head.genesis().activation());
            if (original) {
                if (join != null || configuration.bootstrap()) {
                    throw new IOException("an activated original voter cannot bootstrap or acquire a learner identity");
                }
                initial.put(group.id(), raftGroup(group));
            } else {
                if (configuration.joiningPeer() == null
                    || join == null
                    || join.pending() == null
                    || join.genesis().equals(head.genesis()) == false
                    || join.voters().peers().stream().anyMatch(peer -> peer.id().equals(local))) {
                    throw new IOException("learner startup lacks its original permanent reservation");
                }
                final var newPeer = join.pending().target().peers().stream().filter(peer -> peer.id().equals(local)).findFirst();
                if (newPeer.isEmpty()
                    || newPeer.get().equals(configuration.joiningPeer()) == false
                    || head.revision() < join.revision()
                    || (head.revision() == join.revision() && head.equals(join) == false)
                    || (configuration.bootstrap() && head.equals(join) == false)) {
                    throw new IOException("learner reservation was never active or differs from its original placement");
                }
                final var peers = new ArrayList<RaftPeer>();
                for (var voter : join.voters().peers()) {
                    peers.add(peer(voter, RaftPeerRole.FOLLOWER));
                }
                peers.add(peer(newPeer.get(), RaftPeerRole.LISTENER));
                initial.put(group.id(), RaftGroup.valueOf(RaftGroupId.valueOf(group.id()), peers));
            }
            final boolean voter = head.voters().peers().stream().anyMatch(peer -> peer.id().equals(local));
            final boolean pending = head.pending() != null
                && head.pending().target().peers().stream().anyMatch(peer -> peer.id().equals(local));
            if (voter == false && pending) {
                if (head.equals(join) == false) {
                    throw new IOException("a retired learner identity cannot join another transition");
                }
            } else if (voter == false) {
                if (configuration.bootstrap()) {
                    throw new IOException("retired membership cannot authorize bootstrap");
                }
                retired.add(group.id());
            }
        }
        return new Authorized(activations, retired, initial);
    }

    static RaftGroup raftGroup(JournalConfiguration.Group group) {
        return RaftGroup.valueOf(
            RaftGroupId.valueOf(group.id()),
            group.peers().stream().map(value -> peer(value, RaftPeerRole.FOLLOWER)).toList()
        );
    }

    static RaftPeer peer(JournalConfiguration.Peer peer, RaftPeerRole role) {
        final String host = peer.host().indexOf(':') >= 0 && peer.host().startsWith("[") == false ? "[" + peer.host() + "]" : peer.host();
        return RaftPeer.newBuilder().setId(peer.id().toString()).setAddress(host + ":" + peer.port()).setStartupRole(role).build();
    }
}
