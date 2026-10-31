/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.apache.ratis.proto.RaftProtos.GroupListRequestProto;
import org.apache.ratis.proto.grpc.AdminProtocolServiceGrpc;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/** Local persistence and security tests; not a power-loss or three-AZ qualification. */
@SuppressFileSystems("*")
// gRPC shared client executors retire after their 60-second idle timeout. Do not filter their leaks.
@ThreadLeakLingering(linger = 70_000)
@ThreadLeakFilters(filters = RatisJournalTests.GlobalTimerFilter.class)
public class RatisJournalTests extends OpenSearchTestCase {
    /** Only known idle JVM-global library threads without a public shutdown API are excluded. */
    public static class GlobalTimerFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            final var group = thread.getThreadGroup();
            if (thread.isDaemon() && thread.getName().equals("grpc-shared-destroyer-0") && thread.getState() == Thread.State.WAITING) {
                // SharedResourceHolder keeps its cleanup scheduler after all client executors retire.
                for (var frame : thread.getStackTrace()) {
                    if (frame.getClassName().equals("java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue")
                        && frame.getMethodName().equals("take")) {
                        return true;
                    }
                }
            }
            if (thread.isDaemon() == false
                || thread.getName().matches("timer[0-7]") == false
                || group == null
                || group.getName().matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}@group-[0-9A-F]{12}") == false) {
                return false;
            }
            for (var frame : thread.getStackTrace()) {
                if (frame.getClassName().equals("java.util.TimerThread")) {
                    return true;
                }
            }
            return false;
        }
    }

    public void testCommittedSharedStreamsSurviveLeaderLossAndFullRestart() throws Exception {
        final var fixture = fixture(2);
        final var journals = new ArrayList<RatisJournal>();
        try {
            startAll(fixture, journals, true);
            final UUID group = fixture.configurations().getFirst().groups().getFirst().id();
            final UUID otherGroup = fixture.configurations().getFirst().groups().get(1).id();
            final var first = new ShardLogId(fixture.deployment(), "first-index", 0, UUID.randomUUID());
            final var second = new ShardLogId(fixture.deployment(), "second-index", 7, UUID.randomUUID());
            final RatisJournal leader = leader(journals, group);
            for (int i = 0; i < 30; i++) {
                final var operation = operation(i % 2 == 0 ? first : second, i);
                final var checkpoint = leader.append(group, operation).toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertEquals(operation.logId(), checkpoint.logId());
            }
            leader(journals, otherGroup).append(otherGroup, operation(first, 50)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            final var before = leader.scan(group, -1, Set.of(first), 4096, 65536);
            assertEquals(15, before.operations().size());
            assertTrue(before.throughIndex() >= before.operations().getLast().checkpoint().index());
            assertEquals(30, leader.scan(group, -1, Set.of(first, second), 4096, 65536).operations().size());
            final var skipped = leader.scan(group, -1, Set.of(), 4096, operation(first, 0).encode(16384).length + 1);
            assertTrue("unselected payloads must consume the scan byte budget", skipped.throughIndex() < skipped.committedIndex());
            assertTrue(skipped.operations().isEmpty());
            expectThrows(IOException.class, () -> leader.scan(group, -1, Set.of(first), 4096, 1));
            expectThrows(IOException.class, () -> leader.scan(group, Long.MAX_VALUE, Set.of(first), 1, 65536));
            final RatisJournal oldLeader = leader;
            oldLeader.close();
            journals.remove(oldLeader);
            leader(journals, group).append(group, operation(first, 30)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            closeAll(journals);
            startAll(fixture, journals, false);
            final RatisJournal recoveredLeader = leader(journals, group);
            recoveredLeader.append(group, operation(first, 31)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(32, recoveredLeader.scan(group, -1, Set.of(first, second), 4096, 65536).operations().size());
            final var replay = recoveredLeader.scan(group, -1, Set.of(first, second), 4096, 65536);
            for (int i = 0; i < replay.operations().size(); i++) {
                final var item = replay.operations().get(i);
                assertEquals(i, item.operation().payloadReader().read());
                assertEquals(i < 30 && i % 2 != 0 ? second : first, item.checkpoint().logId());
            }
            assertEquals(1, leader(journals, otherGroup).scan(otherGroup, -1, Set.of(first), 4096, 65536).operations().size());
        } finally {
            closeAll(journals);
        }
    }

    public void testDiskReserveAndNamespaceRefuseNewProposals() throws Exception {
        final var fixture = fixture(1);
        final var journals = new ArrayList<RatisJournal>();
        try {
            startAll(fixture, journals, true);
            final UUID group = fixture.configurations().getFirst().groups().getFirst().id();
            final RatisJournal leader = leader(journals, group);
            final var context = SSLContext.getInstance("TLS");
            context.init(new KeyManager[0], new TrustManager[] { fixture.trust() }, null);
            final var peer = fixture.configurations().getFirst().groups().getFirst().peers().getFirst();
            try (var socket = (SSLSocket) context.getSocketFactory().createSocket(peer.host(), peer.port())) {
                socket.setSoTimeout(5000);
                socket.setEnabledProtocols(new String[] { "TLSv1.2" });
                expectThrows(IOException.class, socket::startHandshake);
            }
            final var tls = GrpcSslContexts.forClient().keyManager(fixture.key()).trustManager(fixture.trust()).build();
            final var channel = NettyChannelBuilder.forAddress(peer.host(), peer.port())
                .proxyDetector(ignored -> null)
                .useTransportSecurity()
                .sslContext(tls)
                .build();
            try {
                final var denied = expectThrows(
                    StatusRuntimeException.class,
                    () -> AdminProtocolServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(5, TimeUnit.SECONDS)
                        .groupList(GroupListRequestProto.getDefaultInstance())
                );
                assertEquals(Status.Code.PERMISSION_DENIED, denied.getStatus().getCode());
            } finally {
                assertTrue(channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
            }
            final var foreign = new ShardLogId(UUID.randomUUID(), "index", 0, UUID.randomUUID());
            expectThrows(IOException.class, () -> leader.append(group, operation(foreign, 0)));
            expectThrows(IOException.class, () -> leader.append(UUID.randomUUID(), operation(foreign, 0)));
            fixture.space().set(0);
            final var stream = new ShardLogId(fixture.deployment(), "index", 0, UUID.randomUUID());
            expectThrows(IOException.class, () -> leader.append(group, operation(stream, 0)));
            fixture.space().set(Long.MAX_VALUE);
            leader.append(group, operation(stream, 0)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        } finally {
            closeAll(journals);
        }
    }

    public void testLossOfMajorityCannotAcknowledgeAnotherWrite() throws Exception {
        final var fixture = fixture(1);
        final var journals = new ArrayList<RatisJournal>();
        try {
            startAll(fixture, journals, true);
            final UUID group = fixture.configurations().getFirst().groups().getFirst().id();
            final var stream = new ShardLogId(fixture.deployment(), "index", 0, UUID.randomUUID());
            final RatisJournal leader = leader(journals, group);
            leader.append(group, operation(stream, 0)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            for (var journal : List.copyOf(journals)) {
                if (journal != leader) {
                    journal.close();
                    journals.remove(journal);
                }
            }
            try {
                final var pending = leader.append(group, operation(stream, 1)).toCompletableFuture();
                try {
                    pending.get(500, TimeUnit.MILLISECONDS);
                    fail("one of three voters cannot acknowledge a new write");
                } catch (TimeoutException | ExecutionException expected) {
                    // A timeout remains ambiguous; cancellation must not be interpreted as rollback.
                    pending.cancel(false);
                }
            } catch (IOException expected) {
                // The former leader may already have stepped down before admission.
            }
        } finally {
            closeAll(journals);
        }
    }

    public void testOccupiedPortFailsWithoutExitingOrReplacingTheHostHandler() throws Exception {
        final var fixture = fixture(1);
        final var configuration = fixture.configurations().getFirst();
        final var peer = configuration.groups().getFirst().peers().getFirst();
        final var handler = Thread.getDefaultUncaughtExceptionHandler();
        try (var reserved = new ServerSocket(peer.port(), 0, InetAddress.getByName(peer.host()))) {
            expectThrows(IOException.class, () -> RatisJournal.open(configuration, fixture.key(), fixture.trust(), fixture.space()::get));
        }
        assertSame(handler, Thread.getDefaultUncaughtExceptionHandler());
        assertEquals(configuration.localVoterId(), JournalDiskIdentity.discover(configuration.directory()));
    }

    private record Fixture(UUID deployment, List<JournalConfiguration> configurations, X509KeyManager key, X509TrustManager trust,
        AtomicLong space) {
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
        final var limits = new JournalConfiguration.Limits(2, 16384, 32, 1024 * 1024, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
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

    private static void startAll(Fixture fixture, List<RatisJournal> journals, boolean bootstrap) throws IOException {
        for (var initial : fixture.configurations()) {
            final var configuration = new JournalConfiguration(
                initial.deploymentId(),
                initial.localVoterId(),
                initial.directory(),
                initial.groups(),
                bootstrap,
                initial.limits()
            );
            journals.add(RatisJournal.open(configuration, fixture.key(), fixture.trust(), fixture.space()::get));
        }
    }

    private static OperationEnvelope operation(ShardLogId stream, int number) {
        return new OperationEnvelope(stream, OperationEnvelope.Kind.DOCUMENTS, 1, 1, 1, new byte[] { (byte) number });
    }

    private static RatisJournal leader(List<RatisJournal> journals, UUID group) throws Exception {
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

    private static void closeAll(List<RatisJournal> journals) throws IOException {
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
