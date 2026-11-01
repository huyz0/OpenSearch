/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class JournalStartupTests extends OpenSearchTestCase {
    public void testLearnerRequiresTheWinningPermanentIntent() throws Exception {
        final var pending = pending();
        final var configuration = learner(pending, true);
        final UUID group = pending.voters().id();
        expectThrows(IOException.class, () -> JournalStartup.authorize(configuration, Map.of(), Map.of()));
        expectThrows(IOException.class, () -> JournalStartup.authorize(configuration, Map.of(group, pending), Map.of()));
        final var initial = JournalMembershipRecord.initial(pending.genesis());
        expectThrows(IOException.class, () -> JournalStartup.authorize(configuration, Map.of(group, initial), Map.of(group, pending)));
        final var authorized = JournalStartup.authorize(configuration, Map.of(group, pending), Map.of(group, pending));
        assertEquals(pending.genesis().activation(), authorized.activations().get(group));
        assertTrue(authorized.retired().isEmpty());
        final var peers = authorized.initialGroups().get(group).getPeers();
        assertEquals(4, peers.size());
        assertEquals(1, peers.stream().filter(peer -> peer.getStartupRole() == RaftPeerRole.LISTENER).count());
        assertEquals(
            configuration.localVoterId().toString(),
            peers.stream().filter(peer -> peer.getStartupRole() == RaftPeerRole.LISTENER).findFirst().orElseThrow().getId().toString()
        );
    }

    public void testPromotedLearnerCanRecoverButCannotBootstrapAgain() throws Exception {
        final var pending = pending();
        final UUID group = pending.voters().id();
        final var completed = new JournalMembershipRecord(pending.genesis(), 1, pending.pending().target(), null);
        expectThrows(
            IOException.class,
            () -> JournalStartup.authorize(learner(pending, true), Map.of(group, completed), Map.of(group, pending))
        );
        final var recovered = JournalStartup.authorize(learner(pending, false), Map.of(group, completed), Map.of(group, pending));
        assertTrue(recovered.retired().isEmpty());
        assertEquals(pending.genesis().activation(), recovered.activations().get(group));
    }

    public void testOriginalRetirementAndPendingRecoveryKeepGenesis() throws Exception {
        final var pending = pending();
        final UUID group = pending.voters().id();
        final var removed = pending.voters()
            .peers()
            .stream()
            .filter(peer -> pending.pending().target().peers().contains(peer) == false)
            .findFirst()
            .orElseThrow();
        final var configuration = new JournalConfiguration(
            pending.genesis().deploymentId(),
            removed.id(),
            Path.of("unused"),
            List.of(pending.genesis().initialPlacement()),
            false,
            JournalConfiguration.Limits.defaults()
        );
        assertTrue(JournalStartup.authorize(configuration, Map.of(group, pending), Map.of()).retired().isEmpty());
        final var completed = new JournalMembershipRecord(pending.genesis(), 1, pending.pending().target(), null);
        assertEquals(java.util.Set.of(group), JournalStartup.authorize(configuration, Map.of(group, completed), Map.of()).retired());
        expectThrows(IOException.class, () -> JournalStartup.authorize(configuration, Map.of(group, pending), Map.of(group, pending)));
    }

    public void testWrongScopeOrReboundLearnerIsRefused() throws Exception {
        final var pending = pending();
        final UUID group = pending.voters().id();
        final var configuration = learner(pending, true);
        expectThrows(
            IOException.class,
            () -> JournalStartup.authorize(configuration, Map.of(UUID.randomUUID(), pending), Map.of(group, pending))
        );
        final var local = configuration.joiningPeer();
        final var rebound = new JournalConfiguration(
            configuration.deploymentId(),
            local.id(),
            configuration.directory(),
            configuration.groups(),
            true,
            configuration.limits(),
            new JournalConfiguration.Peer(local.id(), "another-host", local.port(), local.availabilityZone())
        );
        expectThrows(IOException.class, () -> JournalStartup.authorize(rebound, Map.of(group, pending), Map.of(group, pending)));
        expectThrows(
            IllegalArgumentException.class,
            () -> new JournalConfiguration(
                configuration.deploymentId(),
                UUID.randomUUID(),
                configuration.directory(),
                configuration.groups(),
                true,
                configuration.limits(),
                local
            )
        );
    }

    public void testOnlyUnactivatedOriginalAssignmentsAuthorizeEmptyRecovery() throws Exception {
        final var pending = pending();
        final var group = pending.genesis().initialPlacement();
        final var configuration = new JournalConfiguration(
            pending.genesis().deploymentId(),
            group.peers().getFirst().id(),
            Path.of("unused"),
            List.of(group),
            false,
            JournalConfiguration.Limits.defaults()
        );
        assertEquals(java.util.Set.of(group.id()), JournalStartup.authorize(configuration, Map.of(), Map.of()).emptyOriginals());
        assertTrue(
            JournalStartup.authorize(configuration, Map.of(group.id(), JournalMembershipRecord.initial(pending.genesis())), Map.of())
                .emptyOriginals()
                .isEmpty()
        );
        assertTrue(JournalStartup.authorize(configuration, Map.of(group.id(), pending), Map.of()).emptyOriginals().isEmpty());
        assertTrue(
            JournalStartup.authorize(learner(pending, false), Map.of(group.id(), pending), Map.of(group.id(), pending))
                .emptyOriginals()
                .isEmpty()
        );
    }

    private static JournalConfiguration learner(JournalMembershipRecord pending, boolean bootstrap) {
        final var joining = pending.pending()
            .target()
            .peers()
            .stream()
            .filter(peer -> pending.voters().peers().contains(peer) == false)
            .findFirst()
            .orElseThrow();
        return new JournalConfiguration(
            pending.genesis().deploymentId(),
            joining.id(),
            Path.of("unused"),
            List.of(pending.genesis().initialPlacement()),
            bootstrap,
            JournalConfiguration.Limits.defaults(),
            joining
        );
    }

    static JournalMembershipRecord pending() {
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        for (int i = 0; i < 3; i++) {
            peers.add(new JournalConfiguration.Peer(UUID.randomUUID(), "localhost", 9000 + i, "az-" + i));
        }
        final UUID deployment = UUID.randomUUID();
        final var group = new JournalConfiguration.Group(UUID.randomUUID(), peers);
        final var activation = new LogCheckpoint(JournalGroupRecord.controlIdentity(deployment, group.id()), 2, 1);
        final var initial = JournalMembershipRecord.initial(new JournalGroupRecord(deployment, group, activation));
        final var target = new ArrayList<>(initial.voters().peers());
        final var removed = target.removeLast();
        target.add(new JournalConfiguration.Peer(UUID.randomUUID(), "localhost", 9010, removed.availabilityZone()));
        return initial.begin(0, UUID.randomUUID(), new JournalConfiguration.Group(group.id(), target));
    }
}
