/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Real JVM restart/crash tests, distinct from power-loss and three-AZ qualification. */
@SuppressFileSystems("*")
public class RatisJournalProcessTests extends OpenSearchTestCase {
    public void testLearnerCatchupPromotionAndReplacementQuorumSurviveRestart() throws Exception {
        final var fixture = fixture(1);
        final var nodes = new ArrayList<JournalProcess>();
        final var stream = new ShardLogId(fixture.deployment(), "replacement-index", 0, UUID.randomUUID());
        try {
            startAll(fixture, nodes, true);
            final var leader = leader(nodes, 0);
            final var group = fixture.configurations().getFirst().groups().getFirst();
            final var genesis = new JournalGroupRecord(fixture.deployment(), group, leader.activation(0));
            final var removed = fixture.configurations()
                .get(nodes.indexOf(nodes.stream().filter(node -> node != leader).findFirst().orElseThrow()))
                .localPeer();
            final Path directory = createTempDir();
            final JournalConfiguration.Peer joining;
            try (var port = reservePort()) {
                joining = new JournalConfiguration.Peer(
                    JournalDiskIdentity.discover(directory),
                    "127.0.0.1",
                    port.getLocalPort(),
                    removed.availabilityZone()
                );
            }
            final var targetPeers = new ArrayList<>(group.peers());
            targetPeers.remove(removed);
            targetPeers.add(joining);
            final var target = new JournalConfiguration.Group(group.id(), targetPeers);
            final var intent = JournalMembershipRecord.initial(genesis).begin(0, UUID.randomUUID(), target);
            final var learnerConfiguration = new JournalConfiguration(
                fixture.deployment(),
                joining.id(),
                directory,
                List.of(group),
                true,
                JournalConfiguration.Limits.defaults(),
                joining
            );
            // A controller can crash after empty learner startup but before adding its listener.
            final var empty = JournalProcess.start(
                learnerConfiguration,
                createTempDir().resolve("empty-learner.log"),
                Map.of(),
                Map.of(group.id(), intent),
                Map.of(group.id(), intent)
            );
            nodes.add(empty);
            empty.stop();
            final var recovery = new JournalConfiguration(
                fixture.deployment(),
                joining.id(),
                directory,
                List.of(group),
                false,
                learnerConfiguration.limits(),
                joining
            );
            final var learner = JournalProcess.start(
                recovery,
                createTempDir().resolve("learner.log"),
                Map.of(),
                Map.of(group.id(), intent),
                Map.of(group.id(), intent)
            );
            nodes.add(learner);
            leader.advance(intent); // Add a nonvoting listener only.
            leader.append(0, operation(stream, 0));
            assertBusy(() -> {
                try {
                    assertEquals(genesis.activation(), learner.activation(0));
                } catch (IOException replaying) {
                    throw new AssertionError("waiting for learner activation replay", replaying);
                }
            }, 20, TimeUnit.SECONDS);
            learner.stall();
            final var last = leader.append(0, operation(stream, 1));
            assertBusy(() -> assertTrue(learner.stalled()), 10, TimeUnit.SECONDS);
            leader.advance(intent); // Commit a fresh nonce-bound barrier through the original quorum.
            leader.advance(intent); // A stalled learner must still not be promoted.
            assertEquals(
                group.peers().stream().map(JournalConfiguration.Peer::id).collect(java.util.stream.Collectors.toSet()),
                leader.voters(0)
            );
            assertTrue(leader.receipt(intent).isEmpty());
            assertTrue(learner.progress(0).flushed() < last.index());
            learner.release();
            final var receipt = new JournalReplacementReceipt[1];
            assertBusy(() -> {
                try {
                    leader.advance(intent);
                    receipt[0] = leader.receipt(intent).orElseThrow(() -> new IOException("waiting for target commit"));
                } catch (IOException waiting) {
                    throw new AssertionError("waiting for learner promotion", waiting);
                }
            }, 30, TimeUnit.SECONDS);
            assertEquals(intent, receipt[0].intent());
            assertTrue(receipt[0].learnerBarrierIndex() < receipt[0].targetConfigurationIndex());
            assertEquals(
                target.peers().stream().map(JournalConfiguration.Peer::id).collect(java.util.stream.Collectors.toSet()),
                leader.voters(0)
            );
            leader.append(0, operation(stream, 2));
            final var retired = nodes.get(
                fixture.configurations().stream().map(JournalConfiguration::localVoterId).toList().indexOf(removed.id())
            );
            retired.close();
            leader.close();
            // Only the caught-up replacement and one retained voter remain alive.
            leader(nodes, 0).append(0, operation(stream, 3));
            for (var node : nodes) {
                if (node.alive()) {
                    node.stop();
                }
            }
            nodes.clear();
            // Model controller loss before receipt/head publication: all processes recover with
            // the old pending head, including the source voter already excluded by native consensus.
            for (var original : fixture.configurations()) {
                nodes.add(restart(original, Map.of(), Map.of(group.id(), intent)));
            }
            nodes.add(
                JournalProcess.start(
                    recovery,
                    createTempDir().resolve("promoted-restart.log"),
                    Map.of(),
                    Map.of(group.id(), intent),
                    Map.of(group.id(), intent)
                )
            );
            final var recovered = leader(nodes, 0);
            final var recoveredReceipt = recovered.receipt(intent).orElseThrow();
            assertEquals(receipt[0].intent(), recoveredReceipt.intent());
            assertEquals(receipt[0].learnerBarrierIndex(), recoveredReceipt.learnerBarrierIndex());
            assertEquals(receipt[0].learnerBarrierTerm(), recoveredReceipt.learnerBarrierTerm());
            assertTrue(recoveredReceipt.targetConfigurationIndex() >= receipt[0].targetConfigurationIndex());
            recovered.append(0, operation(stream, 4));
            final var history = recovered.scan(0, -1, Set.of(stream)).operations();
            assertEquals(5, history.size());
            for (int i = 0; i < history.size(); i++) {
                assertEquals(i, history.get(i).operation().payloadReader().read());
            }
        } finally {
            closeAll(nodes);
        }
    }

    public void testReplacingTheLeaderReconcilesOnTheNewQuorum() throws Exception {
        final var fixture = fixture(1);
        final var nodes = new ArrayList<JournalProcess>();
        try {
            startAll(fixture, nodes, true);
            final var oldLeader = leader(nodes, 0);
            final var removed = fixture.configurations().get(nodes.indexOf(oldLeader)).localPeer();
            final var group = fixture.configurations().getFirst().groups().getFirst();
            final var genesis = new JournalGroupRecord(fixture.deployment(), group, oldLeader.activation(0));
            final Path directory = createTempDir();
            final JournalConfiguration.Peer joining;
            try (var port = reservePort()) {
                joining = new JournalConfiguration.Peer(
                    JournalDiskIdentity.discover(directory),
                    "127.0.0.1",
                    port.getLocalPort(),
                    removed.availabilityZone()
                );
            }
            final var peers = new ArrayList<>(group.peers());
            peers.remove(removed);
            peers.add(joining);
            final var target = new JournalConfiguration.Group(group.id(), peers);
            final var intent = JournalMembershipRecord.initial(genesis).begin(0, UUID.randomUUID(), target);
            nodes.add(
                JournalProcess.start(
                    new JournalConfiguration(
                        fixture.deployment(),
                        joining.id(),
                        directory,
                        List.of(group),
                        true,
                        JournalConfiguration.Limits.defaults(),
                        joining
                    ),
                    createTempDir().resolve("leader-replacement.log"),
                    Map.of(),
                    Map.of(group.id(), intent),
                    Map.of(group.id(), intent)
                )
            );
            assertBusy(() -> {
                try {
                    final var currentLeader = leader(nodes, 0);
                    currentLeader.advance(intent);
                    assertTrue(currentLeader.receipt(intent).isPresent());
                } catch (Exception waiting) {
                    throw new AssertionError("waiting for replacement of the previous leader", waiting);
                }
            }, 45, TimeUnit.SECONDS);
            oldLeader.close();
            final var replacement = leader(nodes, 0);
            assertEquals(
                peers.stream().map(JournalConfiguration.Peer::id).collect(java.util.stream.Collectors.toSet()),
                replacement.voters(0)
            );
            final var stream = new ShardLogId(fixture.deployment(), "leader-replacement", 0, UUID.randomUUID());
            replacement.append(0, operation(stream, 0));
            replacement.close();
            // Exactly two members of the new three remain after both leader processes are killed.
            final var remaining = leader(nodes, 0);
            remaining.append(0, operation(stream, 1));
            assertEquals(2, remaining.scan(0, -1, Set.of(stream)).operations().size());
        } finally {
            closeAll(nodes);
        }
    }

    public void testAcknowledgedStreamsSurviveLeaderKillAndFullJvmRestart() throws Exception {
        final var fixture = fixture(2);
        final List<JournalProcess> nodes = new ArrayList<>();
        final var first = new ShardLogId(fixture.deployment(), "first-index", 0, UUID.randomUUID());
        final var second = new ShardLogId(fixture.deployment(), "second-index", 7, UUID.randomUUID());
        final var third = new ShardLogId(fixture.deployment(), "other-group", 1, UUID.randomUUID());
        try {
            startAll(fixture, nodes, true);
            final var leader = leader(nodes, 0);
            final var activation = leader.activation(0);
            for (int i = 0; i < 20; i++) {
                leader.append(0, operation(i % 2 == 0 ? first : second, i));
            }
            leader(nodes, 1).append(1, operation(third, 99));
            final var otherActivation = leader(nodes, 1).activation(1);
            assertEquals(10, leader.scan(0, -1, Set.of(first)).operations().size());
            leader.close(); // SIGKILL, without native shutdown or a final flush.
            final var replacement = leader(nodes, 0);
            replacement.append(0, operation(first, 20));
            for (var node : nodes) {
                if (node.alive()) {
                    node.stop();
                }
            }
            nodes.clear();
            final var groups = fixture.configurations().getFirst().groups();
            startAll(fixture, nodes, false, Map.of(groups.getFirst().id(), activation, groups.get(1).id(), otherActivation));
            final var recovered = leader(nodes, 0);
            assertEquals(activation, recovered.activation(0));
            recovered.append(0, operation(first, 21));
            final var history = recovered.scan(0, -1, Set.of(first, second)).operations();
            assertEquals(22, history.size());
            for (int i = 0; i < history.size(); i++) {
                assertEquals(i, history.get(i).operation().payloadReader().read());
                assertEquals(i < 20 && i % 2 != 0 ? second : first, history.get(i).checkpoint().logId());
            }
            assertEquals(1, leader(nodes, 1).scan(1, -1, Set.of(third)).operations().size());
        } finally {
            closeAll(nodes);
        }
    }

    public void testStalledShutdownKeepsVolumeLockedUntilTheOldJvmExits() throws Exception {
        final var fixture = fixture(1);
        final List<JournalProcess> nodes = new ArrayList<>();
        final var stream = new ShardLogId(fixture.deployment(), "index", 0, UUID.randomUUID());
        try {
            startAll(fixture, nodes, true);
            final var leader = leader(nodes, 0);
            leader.append(0, operation(stream, 0));
            final var stalled = nodes.stream().filter(node -> node != leader).findFirst().orElseThrow();
            final var configuration = fixture.configurations().get(nodes.indexOf(stalled));
            stalled.stall();
            leader.append(0, operation(stream, 1)); // The other two voters still form a persistent majority.
            assertBusy(() -> assertTrue(stalled.stalled()), 10, TimeUnit.SECONDS);
            assertTrue(stalled.ioResourcesOpen());
            stalled.stopButKeepProcessAlive();
            assertTrue("native log channel must not be closed while its worker is active", stalled.ioResourcesOpen());
            assertTrue(stalled.alive());
            expectThrows(IOException.class, () -> JournalDiskIdentity.discover(configuration.directory()));
            stalled.close();
            assertEquals(configuration.localVoterId(), JournalDiskIdentity.discover(configuration.directory()));
        } finally {
            closeAll(nodes);
        }
    }

    public void testMissingAcknowledgedTailCannotRestartAsAnEstablishedVoter() throws Exception {
        final var fixture = fixture(1);
        final List<JournalProcess> nodes = new ArrayList<>();
        final var stream = new ShardLogId(fixture.deployment(), "tail-index", 0, UUID.randomUUID());
        try {
            startAll(fixture, nodes, true);
            final var leader = leader(nodes, 0);
            final var activation = leader.activation(0);
            final var follower = nodes.stream().filter(node -> node != leader).findFirst().orElseThrow();
            final var original = fixture.configurations().get(nodes.indexOf(follower));
            LogCheckpoint last = null;
            for (int i = 0; i < 200; i++) {
                final byte[] payload = new byte[16384];
                payload[0] = (byte) i;
                last = leader.append(0, new OperationEnvelope(stream, OperationEnvelope.Kind.DOCUMENTS, 1, 1, 1, payload));
            }
            final var acknowledged = last;
            assertBusy(() -> {
                try {
                    final var tail = follower.scan(0, acknowledged.index() - 1, Set.of(stream));
                    assertTrue(tail.operations().stream().anyMatch(item -> item.checkpoint().equals(acknowledged)));
                } catch (IOException awaitingCommit) {
                    throw new AssertionError("waiting for the follower's acknowledged prefix", awaitingCommit);
                }
            }, 20, TimeUnit.SECONDS);
            follower.close(); // Stop this voter before other shutdowns can elect another term.
            closeAll(nodes);
            final var group = original.groups().getFirst().id();
            final Path current = original.directory().resolve(group.toString()).resolve("current");
            final List<Path> segments;
            try (var entries = Files.list(current)) {
                segments = entries.filter(path -> path.getFileName().toString().startsWith("log_")).toList();
            }
            assertTrue("fixture must rotate its two-MiB segments", segments.size() >= 2);
            int removed = 0;
            for (var segment : segments) {
                final String name = segment.getFileName().toString();
                final long start = name.startsWith("log_inprogress_")
                    ? Long.parseLong(name.substring("log_inprogress_".length()))
                    : Long.parseLong(name.substring("log_".length(), name.indexOf('-')));
                if (start > 0) {
                    Files.delete(segment);
                    removed++;
                }
            }
            assertTrue(removed > 0);
            final var recovering = new JournalConfiguration(
                original.deploymentId(),
                original.localVoterId(),
                original.directory(),
                original.groups(),
                false,
                original.limits()
            );
            expectThrows(IOException.class, () -> {
                try (
                    var restarted = JournalProcess.start(recovering, createTempDir().resolve("missing-tail.log"), Map.of(group, activation))
                ) {
                    fail("a voter missing its previously acknowledged tail must fail before serving consensus RPCs");
                }
            });
        } finally {
            closeAll(nodes);
        }
    }

    public void testLegitimateUncommittedSuffixTruncationKeepsRecoveryUsable() throws Exception {
        final var fixture = fixture(1);
        final List<JournalProcess> nodes = new ArrayList<>();
        final var stream = new ShardLogId(fixture.deployment(), "truncate-index", 0, UUID.randomUUID());
        try {
            startAll(fixture, nodes, true);
            final var oldLeader = leader(nodes, 0);
            final int oldLeaderIndex = nodes.indexOf(oldLeader);
            final var activation = oldLeader.activation(0);
            final UUID group = fixture.configurations().getFirst().groups().getFirst().id();
            final var requirements = Map.of(group, activation);
            oldLeader.append(0, operation(stream, 0));
            for (var node : nodes) {
                if (node != oldLeader) {
                    node.stall();
                }
            }
            expectThrows(IOException.class, () -> oldLeader.appendBatch(0, operation(stream, 99), 20));
            for (var node : nodes) {
                if (node != oldLeader) {
                    assertBusy(() -> assertTrue(node.stalled()), 10, TimeUnit.SECONDS);
                }
            }
            final var oldProgress = oldLeader.progress(0);
            assertTrue("fixture must persist a long uncommitted suffix", oldProgress.flushed() - oldProgress.committed() >= 10);
            closeAll(nodes);
            // Elect a new leader from the two voters that never persisted the uncommitted suffix.
            for (int i = 0; i < fixture.configurations().size(); i++) {
                if (i != oldLeaderIndex) {
                    nodes.add(restart(fixture.configurations().get(i), requirements));
                }
            }
            final var newLeader = leader(nodes, 0);
            newLeader.append(0, operation(stream, 1));
            final var old = restart(fixture.configurations().get(oldLeaderIndex), requirements);
            nodes.add(old);
            assertBusy(() -> {
                try {
                    assertEquals(2, old.scan(0, -1, Set.of(stream)).operations().size());
                } catch (IOException awaitingCatchup) {
                    throw new AssertionError("waiting for the old leader to discard its uncommitted suffix", awaitingCatchup);
                }
            }, 20, TimeUnit.SECONDS);
            assertTrue("the old durable floor must actually move backwards", old.progress(0).flushed() < oldProgress.flushed());
            for (var node : nodes) {
                node.stop();
            }
            nodes.clear();
            startAll(fixture, nodes, false, requirements);
            final var recovered = leader(nodes, 0);
            recovered.append(0, operation(stream, 2));
            final var history = recovered.scan(0, -1, Set.of(stream)).operations();
            assertEquals(3, history.size());
            for (int i = 0; i < history.size(); i++) {
                assertEquals(i, history.get(i).operation().payloadReader().read());
            }
        } finally {
            closeAll(nodes);
        }
    }

    public void testLeaderStorageFailureAllowsTheHealthyQuorumToTakeOver() throws Exception {
        final var fixture = fixture(1);
        final List<JournalProcess> nodes = new ArrayList<>();
        final var stream = new ShardLogId(fixture.deployment(), "failed-leader", 0, UUID.randomUUID());
        try {
            startAll(fixture, nodes, true);
            final var failed = leader(nodes, 0);
            failed.append(0, operation(stream, 0));
            failed.failStorage();
            expectThrows(IOException.class, () -> failed.append(0, operation(stream, 1)));
            assertBusy(() -> assertTrue(failed.storageFailed()), 10, TimeUnit.SECONDS);
            final var healthy = nodes.stream().filter(node -> node != failed).toList();
            final var replacement = leader(healthy, 0);
            replacement.append(0, operation(stream, 2));
            final var history = replacement.scan(0, -1, Set.of(stream)).operations();
            assertEquals(0, history.getFirst().operation().payloadReader().read());
            assertEquals(2, history.getLast().operation().payloadReader().read());
            // The failed append may have committed elsewhere; its IO result is not rollback.
            assertTrue(failed.alive());
            final var placement = fixture.configurations().get(nodes.indexOf(failed));
            expectThrows(IOException.class, () -> JournalDiskIdentity.discover(placement.directory()));
        } finally {
            closeAll(nodes);
        }
    }

    public void testFailedFollowerCannotSupplyTheSecondPersistentAcknowledgement() throws Exception {
        final var fixture = fixture(1);
        final List<JournalProcess> nodes = new ArrayList<>();
        final var stream = new ShardLogId(fixture.deployment(), "failed-follower", 0, UUID.randomUUID());
        try {
            startAll(fixture, nodes, true);
            final var leader = leader(nodes, 0);
            leader.append(0, operation(stream, 0));
            final var failed = nodes.stream().filter(node -> node != leader).findFirst().orElseThrow();
            failed.failStorage();
            leader.append(0, operation(stream, 1));
            assertBusy(() -> assertTrue(failed.storageFailed()), 10, TimeUnit.SECONDS);
            final var other = nodes.stream().filter(node -> node != leader && node != failed).findFirst().orElseThrow();
            other.close();
            expectThrows(IOException.class, () -> leader.append(0, operation(stream, 2)));
            for (var item : leader.scan(0, -1, Set.of(stream)).operations()) {
                assertNotEquals(2, item.operation().payloadReader().read());
            }
        } finally {
            closeAll(nodes);
        }
    }

    public void testRetiredGroupStaysOutOfNativeRecoveryWhileColocatedGroupContinues() throws Exception {
        final var fixture = fixture(2);
        final List<JournalProcess> nodes = new ArrayList<>();
        final var retiredStream = new ShardLogId(fixture.deployment(), "retired-assignment", 0, UUID.randomUUID());
        final var activeStream = new ShardLogId(fixture.deployment(), "active-assignment", 0, UUID.randomUUID());
        try {
            startAll(fixture, nodes, true);
            leader(nodes, 0).append(0, operation(retiredStream, 0));
            leader(nodes, 1).append(1, operation(activeStream, 0));
            final var original = fixture.configurations().getFirst();
            final var retiredGroup = original.groups().getFirst();
            final var activeGroup = original.groups().get(1);
            final var requirements = Map.of(
                retiredGroup.id(),
                leader(nodes, 0).activation(0),
                activeGroup.id(),
                leader(nodes, 1).activation(1)
            );
            final var target = new ArrayList<>(retiredGroup.peers());
            final var removed = target.removeFirst();
            assertEquals(original.localVoterId(), removed.id());
            target.add(new JournalConfiguration.Peer(UUID.randomUUID(), "replacement.invalid", 9001, removed.availabilityZone()));
            final var retiredGenesis = new JournalGroupRecord(fixture.deployment(), retiredGroup, requirements.get(retiredGroup.id()));
            // Model a controller-authorized completed head to isolate restart/retirement behavior.
            // This test does not establish native learner promotion or configuration completion.
            final var memberships = Map.of(
                retiredGroup.id(),
                new JournalMembershipRecord(retiredGenesis, 1, new JournalConfiguration.Group(retiredGroup.id(), target), null),
                activeGroup.id(),
                JournalMembershipRecord.initial(
                    new JournalGroupRecord(fixture.deployment(), activeGroup, requirements.get(activeGroup.id()))
                )
            );
            nodes.getFirst().stop();
            final Path source = original.directory().resolve(retiredGroup.id().toString());
            final byte[] boundary = Files.readAllBytes(source.resolve("journal-floor"));
            final var recovered = restart(original, requirements, memberships);
            nodes.set(0, recovered);
            expectThrows(IOException.class, () -> recovered.leader(0));
            assertFalse(Files.exists(source));
            final Path archivedFloor = original.directory()
                .resolve("retired")
                .resolve(retiredGroup.id().toString())
                .resolve("journal-floor");
            assertArrayEquals(boundary, Files.readAllBytes(archivedFloor));
            leader(nodes, 1).append(1, operation(activeStream, 1));
            assertBusy(() -> assertEquals(2, recovered.scan(1, -1, Set.of(activeStream)).operations().size()), 30, TimeUnit.SECONDS);
            recovered.close();
            expectThrows(IOException.class, () -> restart(original, requirements));
            final var again = restart(original, requirements, memberships);
            nodes.set(0, again);
            expectThrows(IOException.class, () -> again.leader(0));
            assertBusy(() -> assertEquals(2, again.scan(1, -1, Set.of(activeStream)).operations().size()), 30, TimeUnit.SECONDS);
            assertArrayEquals(boundary, Files.readAllBytes(archivedFloor));
        } finally {
            closeAll(nodes);
        }
    }

    public void testRecoveryRefusesHistoryBeyondTheConfiguredMetadataBudgetBeforeSdkStartup() throws Exception {
        final var fixture = fixture(1);
        final List<JournalProcess> nodes = new ArrayList<>();
        final var stream = new ShardLogId(fixture.deployment(), "metadata-budget", 0, UUID.randomUUID());
        try {
            startAll(fixture, nodes, true);
            final var leader = leader(nodes, 0);
            final var activation = leader.activation(0);
            final var follower = nodes.stream().filter(node -> node != leader).findFirst().orElseThrow();
            final int followerIndex = nodes.indexOf(follower);
            final var original = fixture.configurations().get(followerIndex);
            LogCheckpoint last = null;
            for (int i = 0; i < 1500 && (last == null || last.index() < 1350); i++) {
                last = leader.append(0, operation(stream, i));
            }
            assertNotNull(last);
            assertTrue(last.index() >= 1350);
            final var required = last;
            assertBusy(
                () -> assertTrue(
                    follower.scan(0, -1, Set.of(stream)).operations().stream().anyMatch(item -> item.checkpoint().equals(required))
                ),
                30,
                TimeUnit.SECONDS
            );
            follower.stop();
            final var limits = original.limits();
            final var smaller = new JournalConfiguration(
                original.deploymentId(),
                original.localVoterId(),
                original.directory(),
                original.groups(),
                false,
                new JournalConfiguration.Limits(
                    limits.maximumGroups(),
                    limits.maximumPayloadBytes(),
                    32,
                    limits.maximumPendingBytes(),
                    limits.queueBytes(),
                    limits.segmentBytes(),
                    limits.cachedSegmentBytes(),
                    1024,
                    limits.minimumFreeBytes()
                )
            );
            final var requirements = Map.of(original.groups().getFirst().id(), activation);
            expectThrows(IOException.class, () -> nodes.set(followerIndex, restart(smaller, requirements)));
            // A synthetic low local boundary leaves a valid larger suffix. Recovery must bound
            // the observed tail too, not merely check the saved boundary before streaming it.
            try (
                var floor = JournalDurableFloor.open(
                    original.directory().resolve(original.groups().getFirst().id().toString()).resolve("journal-floor"),
                    original.deploymentId(),
                    original.localVoterId(),
                    original.groups().getFirst().id()
                )
            ) {
                floor.persist(0);
            }
            expectThrows(IOException.class, () -> nodes.set(followerIndex, restart(smaller, requirements)));
            final var recovered = restart(original, requirements);
            nodes.set(followerIndex, recovered);
            assertBusy(
                () -> assertTrue(
                    recovered.scan(0, -1, Set.of(stream)).operations().stream().anyMatch(item -> item.checkpoint().equals(required))
                ),
                30,
                TimeUnit.SECONDS
            );
        } finally {
            closeAll(nodes);
        }
    }

    private JournalProcess restart(JournalConfiguration original, Map<UUID, LogCheckpoint> requirements) throws IOException {
        return restart(original, requirements, Map.of());
    }

    private JournalProcess restart(
        JournalConfiguration original,
        Map<UUID, LogCheckpoint> requirements,
        Map<UUID, JournalMembershipRecord> memberships
    ) throws IOException {
        final var configuration = new JournalConfiguration(
            original.deploymentId(),
            original.localVoterId(),
            original.directory(),
            original.groups(),
            false,
            original.limits()
        );
        return JournalProcess.start(configuration, createTempDir().resolve("recovered-voter.log"), requirements, memberships);
    }

    private record Fixture(UUID deployment, List<JournalConfiguration> configurations) {
    }

    private Fixture fixture(int groupCount) throws Exception {
        final UUID deployment = UUID.randomUUID();
        final var directories = new ArrayList<Path>();
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        final var reservations = new ArrayList<ServerSocket>();
        try {
            for (int i = 0; i < 3; i++) {
                final Path directory = createTempDir();
                directories.add(directory);
                final var socket = reservePort();
                reservations.add(socket);
                peers.add(
                    new JournalConfiguration.Peer(JournalDiskIdentity.discover(directory), "127.0.0.1", socket.getLocalPort(), "az-" + i)
                );
            }
        } finally {
            for (var socket : reservations) {
                socket.close();
            }
        }
        final var groups = new ArrayList<JournalConfiguration.Group>();
        for (int i = 0; i < groupCount; i++) {
            groups.add(new JournalConfiguration.Group(UUID.randomUUID(), peers));
        }
        final var configurations = new ArrayList<JournalConfiguration>();
        for (int i = 0; i < 3; i++) {
            configurations.add(
                new JournalConfiguration(
                    deployment,
                    peers.get(i).id(),
                    directories.get(i),
                    groups,
                    true,
                    JournalConfiguration.Limits.defaults()
                )
            );
        }
        return new Fixture(deployment, List.copyOf(configurations));
    }

    private ServerSocket reservePort() throws IOException {
        // Stay below the Linux ephemeral range used by the in-process fixtures.
        for (int attempt = 0; attempt < 100; attempt++) {
            try {
                return new ServerSocket(randomIntBetween(20000, 30000), 0, InetAddress.getByName("127.0.0.1"));
            } catch (BindException busy) {
                // Try another fixture port while retaining all previous reservations.
            }
        }
        throw new IOException("could not reserve a fixture voter port");
    }

    private void startAll(Fixture fixture, List<JournalProcess> nodes, boolean bootstrap) throws Exception {
        startAll(fixture, nodes, bootstrap, Map.of());
    }

    private void startAll(Fixture fixture, List<JournalProcess> nodes, boolean bootstrap, Map<UUID, LogCheckpoint> requiredActivations)
        throws Exception {
        for (var original : fixture.configurations()) {
            final var configuration = new JournalConfiguration(
                original.deploymentId(),
                original.localVoterId(),
                original.directory(),
                original.groups(),
                bootstrap,
                original.limits()
            );
            nodes.add(JournalProcess.start(configuration, createTempDir().resolve("voter.log"), requiredActivations));
        }
        final var groups = fixture.configurations().getFirst().groups();
        for (int group = 0; group < groups.size(); group++) {
            final var node = leader(nodes, group);
            final var required = requiredActivations.get(groups.get(group).id());
            if (required == null) {
                node.activate(group);
            } else {
                final int groupIndex = group;
                assertBusy(() -> {
                    try {
                        assertEquals(required, node.activation(groupIndex));
                    } catch (IOException awaitingReplay) {
                        throw new AssertionError("waiting for established activation replay", awaitingReplay);
                    }
                }, 20, TimeUnit.SECONDS);
            }
        }
    }

    private static JournalProcess leader(List<JournalProcess> nodes, int group) throws Exception {
        final var result = new JournalProcess[1];
        assertBusy(() -> {
            for (var node : nodes) {
                if (node.alive() && node.leader(group)) {
                    result[0] = node;
                    return;
                }
            }
            fail("no ready voter leader");
        }, 30, TimeUnit.SECONDS);
        return result[0];
    }

    private static OperationEnvelope operation(ShardLogId stream, int value) {
        return new OperationEnvelope(stream, OperationEnvelope.Kind.DOCUMENTS, 1, 1, 1, new byte[] { (byte) value });
    }

    private static void closeAll(List<JournalProcess> nodes) {
        RuntimeException first = null;
        for (var node : nodes) {
            try {
                node.close();
            } catch (RuntimeException failure) {
                if (first == null) {
                    first = failure;
                } else {
                    first.addSuppressed(failure);
                }
            }
        }
        nodes.clear();
        if (first != null) {
            throw first;
        }
    }
}
