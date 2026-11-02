/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Shared native fixture; each concrete suite runs in its own process with its own volume budget. */
abstract class JournalTestCase extends OpenSearchTestCase {
    @SuppressForbidden(reason = "Model an unavailable pinned-SDK consensus monitor without a production fault-injection API")
    protected static Object nativeDivisionForStall(RatisJournal journal, UUID group) throws Exception {
        final var field = RatisJournal.class.getDeclaredField("server");
        field.setAccessible(true);
        final var server = (org.apache.ratis.server.RaftServer) field.get(journal);
        return server.getDivision(org.apache.ratis.protocol.RaftGroupId.valueOf(group));
    }

    protected record Fixture(UUID deployment, List<JournalConfiguration> configurations, X509KeyManager key, X509TrustManager trust,
        AtomicLong space) {
    }

    protected Fixture fixture(int groupCount) throws Exception {
        final UUID deployment = UUID.randomUUID();
        final var directories = new ArrayList<Path>();
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        final var reservations = new ArrayList<ServerSocket>();
        try {
            for (int i = 0; i < 3; i++) {
                final Path directory = createTempDir();
                directories.add(directory);
                final UUID id = JournalDiskIdentity.discover(directory);
                final var socket = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"));
                reservations.add(socket);
                peers.add(new JournalConfiguration.Peer(id, "127.0.0.1", socket.getLocalPort(), "az-" + i));
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
        final var limits = new JournalConfiguration.Limits(
            Math.max(2, groupCount),
            16384,
            32,
            1024 * 1024,
            1024 * 1024,
            2 * 1024 * 1024,
            0,
            10_000,
            1
        );
        final var configurations = new ArrayList<JournalConfiguration>();
        for (int i = 0; i < 3; i++) {
            configurations.add(new JournalConfiguration(deployment, peers.get(i).id(), directories.get(i), groups, true, limits));
        }
        final var keys = KeyStore.getInstance("PKCS12");
        try (var input = getClass().getResourceAsStream("tls-test-keystore.p12")) {
            keys.load(input, "testonly".toCharArray());
        }
        final var keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyFactory.init(keys, "testonly".toCharArray());
        final var trust = KeyStore.getInstance("PKCS12");
        try (var input = getClass().getResourceAsStream("tls-test-truststore.p12")) {
            trust.load(input, "testonly".toCharArray());
        }
        final var trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trust);
        return new Fixture(
            deployment,
            List.copyOf(configurations),
            (X509KeyManager) keyFactory.getKeyManagers()[0],
            (X509TrustManager) trustFactory.getTrustManagers()[0],
            new AtomicLong(Long.MAX_VALUE)
        );
    }

    protected static void startAll(Fixture fixture, List<RatisJournal> journals, boolean bootstrap) throws Exception {
        openAll(fixture, journals, bootstrap);
        for (var group : fixture.configurations().getFirst().groups()) {
            leader(journals, group.id()).activateGroup(group.id()).toCompletableFuture().get(20, TimeUnit.SECONDS);
        }
    }

    protected static void openAll(Fixture fixture, List<RatisJournal> journals, boolean bootstrap) throws Exception {
        openAll(fixture, journals, bootstrap, Map.of());
    }

    protected static void openAll(
        Fixture fixture,
        List<RatisJournal> journals,
        boolean bootstrap,
        Map<UUID, LogCheckpoint> requiredActivations
    ) throws Exception {
        for (var initial : fixture.configurations()) {
            final var configuration = new JournalConfiguration(
                initial.deploymentId(),
                initial.localVoterId(),
                initial.directory(),
                initial.groups(),
                bootstrap,
                initial.limits()
            );
            journals.add(RatisJournal.open(configuration, fixture.key(), fixture.trust(), fixture.space()::get, requiredActivations));
        }

    }

    protected static OperationEnvelope operation(ShardLogId stream, int number) {
        return new OperationEnvelope(stream, OperationEnvelope.Kind.DOCUMENTS, 1, 1, 1, new byte[] { (byte) number });
    }

    protected static RatisJournal leader(List<RatisJournal> journals, UUID group) throws Exception {
        final var ready = new RatisJournal[1];
        assertBusy(() -> {
            for (var journal : journals) {
                if (journal.isLeaderReady(group)) {
                    ready[0] = journal;
                    return;
                }
            }
            fail("no ready leader");
        }, 30, TimeUnit.SECONDS);
        return ready[0];
    }

    protected static void closeAll(List<RatisJournal> journals) throws IOException {
        IOException first = null;
        for (var journal : journals) {
            try {
                journal.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        journals.clear();
        if (first != null) {
            throw first;
        }
    }
}
