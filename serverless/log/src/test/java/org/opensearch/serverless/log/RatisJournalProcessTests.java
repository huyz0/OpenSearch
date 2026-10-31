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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Real JVM restart/crash tests, distinct from power-loss and three-AZ qualification. */
@SuppressFileSystems("*")
public class RatisJournalProcessTests extends OpenSearchTestCase {
    public void testAcknowledgedStreamsSurviveLeaderKillAndFullJvmRestart() throws Exception {
        final var fixture = fixture(2);
        final List<JournalProcess> nodes = new ArrayList<>();
        final var first = new ShardLogId(fixture.deployment(), "first-index", 0, UUID.randomUUID());
        final var second = new ShardLogId(fixture.deployment(), "second-index", 7, UUID.randomUUID());
        final var third = new ShardLogId(fixture.deployment(), "other-group", 1, UUID.randomUUID());
        try {
            startAll(fixture, nodes, true);
            final var leader = leader(nodes, 0);
            for (int i = 0; i < 20; i++) {
                leader.append(0, operation(i % 2 == 0 ? first : second, i));
            }
            leader(nodes, 1).append(1, operation(third, 99));
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
            startAll(fixture, nodes, false);
            final var recovered = leader(nodes, 0);
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
            stalled.stopButKeepProcessAlive();
            assertTrue(stalled.alive());
            expectThrows(IOException.class, () -> JournalDiskIdentity.discover(configuration.directory()));
            stalled.close();
            assertEquals(configuration.localVoterId(), JournalDiskIdentity.discover(configuration.directory()));
        } finally {
            closeAll(nodes);
        }
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
        for (var original : fixture.configurations()) {
            final var configuration = new JournalConfiguration(
                original.deploymentId(),
                original.localVoterId(),
                original.directory(),
                original.groups(),
                bootstrap,
                original.limits()
            );
            nodes.add(JournalProcess.start(configuration, createTempDir().resolve("voter.log")));
        }
        for (int group = 0; group < fixture.configurations().getFirst().groups().size(); group++) {
            leader(nodes, group).activate(group);
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
