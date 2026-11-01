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
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeoutTimer;
import org.opensearch.common.SuppressForbidden;
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
import java.util.Map;
import java.util.Set;
import java.util.Timer;
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
    /** Only SDK-owned global timers and the known idle gRPC destroyer are excluded. */
    public static class GlobalTimerFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            if (thread.isDaemon() && thread.getName().equals("grpc-shared-destroyer-0") && thread.getState() == Thread.State.WAITING) {
                // SharedResourceHolder keeps its cleanup scheduler after all client executors retire.
                for (var frame : thread.getStackTrace()) {
                    if (frame.getClassName().equals("java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue")
                        && frame.getMethodName().equals("take")) {
                        return true;
                    }
                }
            }
            if (thread.isDaemon() == false || thread.getName().matches("timer[0-7]") == false) {
                return false;
            }
            for (var frame : thread.getStackTrace()) {
                if (frame.getClassName().equals("java.util.TimerThread")) {
                    return nativeTimerThreads().contains(thread);
                }
            }
            return false;
        }

        @SuppressForbidden(reason = "Identify exact pinned-SDK global timer threads; fixture opens java.util for identity checks")
        private static Set<Thread> nativeTimerThreads() {
            try {
                final var field = TimeoutTimer.class.getDeclaredField("timers");
                field.setAccessible(true);
                final var result = new java.util.HashSet<Thread>();
                for (var value : (List<?>) field.get(TimeoutTimer.getInstance())) {
                    if (value instanceof MemoizedSupplier<?> supplier && supplier.isInitialized()) {
                        result.add(timerThread((Timer) supplier.getInitializedValue()));
                    }
                }
                return Set.copyOf(result);
            } catch (ReflectiveOperationException | RuntimeException failure) {
                throw new AssertionError("cannot identify pinned Ratis global timers", failure);
            }
        }

        @SuppressForbidden(reason = "Read the identity of a test/global Timer thread without suppressing unrelated timer leaks")
        private static Thread timerThread(Timer timer) throws ReflectiveOperationException {
            final var field = Timer.class.getDeclaredField("thread");
            field.setAccessible(true);
            return (Thread) field.get(timer);
        }
    }

    public void testTimerFilterDoesNotHideAnUnrelatedTimerWithTheSameName() throws Exception {
        final var filter = new GlobalTimerFilter();
        final var ran = new java.util.concurrent.CountDownLatch(1);
        TimeoutTimer.getInstance().onTimeout(org.apache.ratis.util.TimeDuration.ZERO, ran::countDown, failure -> {
            throw new AssertionError(failure);
        });
        assertTrue(ran.await(5, TimeUnit.SECONDS));
        assertFalse(GlobalTimerFilter.nativeTimerThreads().isEmpty());
        for (var thread : GlobalTimerFilter.nativeTimerThreads()) {
            assertTrue(filter.reject(thread));
        }
        final var unrelated = new Timer("timer5", true);
        final var thread = GlobalTimerFilter.timerThread(unrelated);
        try {
            assertFalse(filter.reject(thread));
        } finally {
            unrelated.cancel();
            thread.join(5000);
        }
        assertFalse(thread.isAlive());
    }

    public void testUnserviceableGroupBudgetsRefuseBeforeBindingTheVolume() throws Exception {
        final var fixture = fixture(2);
        final var original = fixture.configurations().getFirst();
        final var limited = new JournalConfiguration.Limits(2, 16384, 1, 1024 * 1024, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        final var invalid = new JournalConfiguration(
            original.deploymentId(),
            original.localVoterId(),
            original.directory(),
            original.groups(),
            true,
            limited
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> RatisJournal.open(invalid, fixture.key(), fixture.trust(), fixture.space()::get)
        );
        assertFalse(java.nio.file.Files.exists(original.directory().resolve("journal-binding")));
        assertEquals(original.localVoterId(), JournalDiskIdentity.discover(original.directory()));
    }

    public void testBlockedGroupCannotOccupyEverySubmissionWorker() throws Exception {
        assertBlockedGroupIsolation(4, 32);
    }

    public void testBlockedGroupCannotTakeEveryPendingRequest() throws Exception {
        assertBlockedGroupIsolation(32, 32);
    }

    public void testBlockedGroupCannotTakeEveryPendingByte() throws Exception {
        assertBlockedGroupIsolation(12, 16384);
    }

    private void assertBlockedGroupIsolation(int proposals, int payloadBytes) throws Exception {
        final var fixture = fixture(4);
        final var journals = new ArrayList<RatisJournal>();
        final var entered = new java.util.concurrent.CountDownLatch(1);
        final var release = new java.util.concurrent.CountDownLatch(1);
        Thread holder = null;
        try {
            startAll(fixture, journals, true);
            final var byLeader = new java.util.HashMap<RatisJournal, List<UUID>>();
            for (var group : fixture.configurations().getFirst().groups()) {
                byLeader.computeIfAbsent(leader(journals, group.id()), ignored -> new ArrayList<>()).add(group.id());
            }
            final var colocated = byLeader.entrySet().stream().filter(entry -> entry.getValue().size() >= 2).findFirst().orElseThrow();
            final var local = colocated.getKey();
            final UUID blocked = colocated.getValue().getFirst();
            final UUID healthy = colocated.getValue().get(1);
            final Object monitor = nativeDivisionForStall(local, blocked);
            holder = org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory("stalled-group-submit").newThread(() -> {
                synchronized (monitor) {
                    entered.countDown();
                    try {
                        if (release.await(15, TimeUnit.SECONDS) == false) {
                            throw new AssertionError("test did not release the blocked group");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            holder.start();
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            final var stream = new ShardLogId(fixture.deployment(), "blocked-submission-isolation", 0, UUID.randomUUID());
            final var operation = new OperationEnvelope(stream, OperationEnvelope.Kind.DOCUMENTS, 1, 1, 1, new byte[payloadBytes]);
            int accepted = 0;
            for (int i = 0; i < proposals; i++) {
                try {
                    local.append(blocked, operation);
                    accepted++;
                } catch (IOException exhausted) {
                    assertTrue(exhausted.getMessage(), exhausted.getMessage().contains("admission"));
                }
            }
            assertTrue("the blocked group must have queued real submissions", accepted > 0);
            local.append(healthy, operation).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(1, local.scan(healthy, -1, Set.of(stream), 100, 65536).operations().size());
        } finally {
            release.countDown();
            if (holder != null) {
                holder.join(TimeUnit.SECONDS.toMillis(10));
                assertFalse(holder.isAlive());
            }
            closeAll(journals);
        }
    }

    public void testCommittedSharedStreamsSurviveLeaderLoss() throws Exception {
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
            final var skipped = leader.scan(group, -1, Set.of(), 4096, 512);
            assertTrue("unselected payloads must consume the scan byte budget", skipped.throughIndex() < skipped.committedIndex());
            assertTrue(skipped.operations().isEmpty());
            expectThrows(IOException.class, () -> leader.scan(group, -1, Set.of(first), 4096, 1));
            expectThrows(IOException.class, () -> leader.scan(group, Long.MAX_VALUE, Set.of(first), 1, 65536));
            final RatisJournal oldLeader = leader;
            oldLeader.close();
            journals.remove(oldLeader);
            leader(journals, group).append(group, operation(first, 30)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            final var replay = leader(journals, group).scan(group, -1, Set.of(first, second), 4096, 65536);
            assertEquals(31, replay.operations().size());
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

    public void testInitialActivationAndReservedControlIdentity() throws Exception {
        final var fixture = fixture(1);
        final var journals = new ArrayList<RatisJournal>();
        try {
            openAll(fixture, journals, true);
            final UUID group = fixture.configurations().getFirst().groups().getFirst().id();
            final var leader = leader(journals, group);
            final var stream = new ShardLogId(fixture.deployment(), "index", 0, UUID.randomUUID());
            expectThrows(IOException.class, () -> leader.append(group, operation(stream, 0)));
            expectThrows(IOException.class, () -> leader.groupActivation(group));
            leader.activateGroup(group).toCompletableFuture().get(20, TimeUnit.SECONDS);
            final var activation = leader.groupActivation(group);
            assertEquals(fixture.deployment(), activation.deploymentId());
            assertEquals(group, activation.initialPlacement().id());
            final var control = new ShardLogId(fixture.deployment(), "__journal_group/" + group, 0, group);
            assertEquals(control, activation.checkpoint().logId());
            expectThrows(IOException.class, () -> leader.append(group, operation(control, 0)));
            leader.append(group, operation(stream, 0)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            final var before = leader.scan(group, -1, Set.of(stream, control), 4096, 65536);
            assertEquals(1, before.operations().size());
            assertEquals(stream, before.operations().getFirst().operation().logId());
            leader.activateGroup(group).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(before.committedIndex(), leader.scan(group, -1, Set.of(), 4096, 65536).committedIndex());
            assertEquals(activation, leader.groupActivation(group));
            Thread.currentThread().interrupt();
            try {
                expectThrows(IOException.class, leader::close);
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
            expectThrows(IOException.class, () -> leader.append(group, operation(stream, 1)));
            // Finally retries native cleanup without the interrupt; the process lease remains held.

        } finally {
            closeAll(journals);
        }
    }

    public void testEstablishedActivationCannotBeReplacedWhenItsHistoryIsMissing() throws Exception {
        final var fixture = fixture(1);
        final var journals = new ArrayList<RatisJournal>();
        final UUID group = fixture.configurations().getFirst().groups().getFirst().id();
        final var required = new LogCheckpoint(JournalGroupRecord.controlIdentity(fixture.deployment(), group), 100, 50);
        try {
            // Simulate controller evidence of an established history absent from these empty logs.
            openAll(fixture, journals, true, Map.of(group, required));
            final var leader = leader(journals, group);
            expectThrows(IOException.class, () -> leader.activateGroup(group));
            expectThrows(IOException.class, () -> leader.groupActivation(group));
            final var stream = new ShardLogId(fixture.deployment(), "index", 0, UUID.randomUUID());
            expectThrows(IOException.class, () -> leader.append(group, operation(stream, 0)));
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
        expectThrows(IOException.class, () -> JournalDiskIdentity.discover(configuration.directory()));
    }

    public void testActivationAndDependentAppendsWorkWithOnePendingPermit() throws Exception {
        final var initial = fixture(1);
        final var configurations = new ArrayList<JournalConfiguration>();
        for (var configuration : initial.configurations()) {
            final var limits = configuration.limits();
            configurations.add(
                new JournalConfiguration(
                    configuration.deploymentId(),
                    configuration.localVoterId(),
                    configuration.directory(),
                    configuration.groups(),
                    true,
                    new JournalConfiguration.Limits(
                        limits.maximumGroups(),
                        limits.maximumPayloadBytes(),
                        1,
                        limits.maximumPendingBytes(),
                        limits.queueBytes(),
                        limits.segmentBytes(),
                        limits.cachedSegmentBytes(),
                        limits.maximumRetainedEntries(),
                        limits.minimumFreeBytes()
                    )
                )
            );
        }
        final var fixture = new Fixture(initial.deployment(), configurations, initial.key(), initial.trust(), initial.space());
        final var journals = new ArrayList<RatisJournal>();
        try {
            startAll(fixture, journals, true);
            final UUID group = configurations.getFirst().groups().getFirst().id();
            final var stream = new ShardLogId(fixture.deployment(), "dependent-append", 0, UUID.randomUUID());
            final var leader = leader(journals, group);
            leader.append(group, operation(stream, 0)).thenCompose(checkpoint -> {
                try {
                    return leader.append(group, operation(stream, 1));
                } catch (IOException failure) {
                    return java.util.concurrent.CompletableFuture.<LogCheckpoint>failedFuture(failure);
                }
            }).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(2, leader.scan(group, -1, Set.of(stream), 100, 65536).operations().size());
        } finally {
            closeAll(journals);
        }
    }

    public void testMembershipRecoveryRejectsPendingOrForeignAuthorizationBeforeStorage() throws Exception {
        final var fixture = fixture(2);
        final var original = fixture.configurations().getFirst();
        final var configuration = new JournalConfiguration(
            original.deploymentId(),
            original.localVoterId(),
            original.directory(),
            original.groups(),
            false,
            original.limits()
        );
        final var group = configuration.groups().getFirst();
        final var genesis = new JournalGroupRecord(
            fixture.deployment(),
            group,
            new LogCheckpoint(JournalGroupRecord.controlIdentity(fixture.deployment(), group.id()), 2, 1)
        );
        final var initial = JournalMembershipRecord.initial(genesis);
        final var target = new ArrayList<>(group.peers());
        final var removed = target.removeLast();
        target.add(new JournalConfiguration.Peer(UUID.randomUUID(), "replacement.invalid", 9001, removed.availabilityZone()));
        final var pending = initial.begin(0, UUID.randomUUID(), new JournalConfiguration.Group(group.id(), target));
        final List<Path> before;
        try (var files = java.nio.file.Files.list(configuration.directory())) {
            before = files.sorted().toList();
        }
        expectThrows(
            IOException.class,
            () -> RatisJournal.recoverMembership(
                original,
                fixture.key(),
                fixture.trust(),
                fixture.space()::get,
                Map.of(group.id(), initial)
            )
        );
        expectThrows(
            IOException.class,
            () -> RatisJournal.recoverMembership(
                configuration,
                fixture.key(),
                fixture.trust(),
                fixture.space()::get,
                Map.of(group.id(), pending)
            )
        );
        expectThrows(
            IOException.class,
            () -> RatisJournal.recoverMembership(
                configuration,
                fixture.key(),
                fixture.trust(),
                fixture.space()::get,
                Map.of(configuration.groups().get(1).id(), initial)
            )
        );
        final UUID foreign = UUID.randomUUID();
        final var foreignGenesis = new JournalGroupRecord(
            foreign,
            group,
            new LogCheckpoint(JournalGroupRecord.controlIdentity(foreign, group.id()), 2, 1)
        );
        expectThrows(
            IOException.class,
            () -> RatisJournal.recoverMembership(
                configuration,
                fixture.key(),
                fixture.trust(),
                fixture.space()::get,
                Map.of(group.id(), JournalMembershipRecord.initial(foreignGenesis))
            )
        );
        try (var files = java.nio.file.Files.list(configuration.directory())) {
            assertEquals(before, files.sorted().toList());
        }
    }

    public void testQuorumAppendDoesNotDependOnTheSdkClientPool() throws Exception {
        final var fixture = fixture(1);
        final var journals = new ArrayList<RatisJournal>();
        final var entered = new java.util.concurrent.CountDownLatch(4);
        final var release = new java.util.concurrent.CountDownLatch(1);
        try {
            startAll(fixture, journals, true);
            final var group = fixture.configurations().getFirst().groups().getFirst().id();
            final var local = leader(journals, group);
            final var clients = nativeClientExecutor(local, group);
            for (int task = 0; task < 4; task++) {
                clients.execute(() -> {
                    entered.countDown();
                    try {
                        if (release.await(20, TimeUnit.SECONDS) == false) {
                            throw new AssertionError("test did not release native client workers");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            final var stream = new ShardLogId(fixture.deployment(), "host-managed-dispatch", 0, UUID.randomUUID());
            local.append(group, operation(stream, 1)).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(1, local.scan(group, -1, Set.of(stream), 100, 65536).operations().size());
        } finally {
            release.countDown();
            closeAll(journals);
        }
    }

    @SuppressForbidden(reason = "Stall the pinned SDK client pool to verify the host dispatch boundary")
    private static java.util.concurrent.ExecutorService nativeClientExecutor(RatisJournal journal, UUID group) throws Exception {
        final var division = nativeDivisionForStall(journal, group);
        final var field = division.getClass().getDeclaredField("clientExecutor");
        field.setAccessible(true);
        return (java.util.concurrent.ExecutorService) field.get(division);
    }

    public void testRetiredTransportRemainsUntilEveryColocatedGroupStopsUsingIt() throws Exception {
        final var fixture = fixture(2);
        final var journals = new ArrayList<RatisJournal>();
        try {
            startAll(fixture, journals, true);
            final var groups = fixture.configurations().getFirst().groups();
            final var retained = leader(journals, groups.getFirst().id());
            final var removedJournal = journals.stream().filter(journal -> journal != retained).findFirst().orElseThrow();
            final var removed = fixture.configurations().get(journals.indexOf(removedJournal)).localPeer();
            final Path directory = createTempDir();
            final JournalConfiguration.Peer joining;
            try (var port = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
                joining = new JournalConfiguration.Peer(
                    JournalDiskIdentity.discover(directory),
                    "127.0.0.1",
                    port.getLocalPort(),
                    removed.availabilityZone()
                );
            }
            final var intents = new java.util.HashMap<UUID, JournalMembershipRecord>();
            for (var group : groups) {
                final var target = new ArrayList<>(group.peers());
                target.remove(removed);
                target.add(joining);
                final var genesis = new JournalGroupRecord(
                    fixture.deployment(),
                    group,
                    leader(journals, group.id()).groupActivation(group.id()).checkpoint()
                );
                intents.put(
                    group.id(),
                    JournalMembershipRecord.initial(genesis).begin(0, UUID.randomUUID(), new JournalConfiguration.Group(group.id(), target))
                );
            }
            journals.add(
                RatisJournal.openAssigned(
                    new JournalConfiguration(
                        fixture.deployment(),
                        joining.id(),
                        directory,
                        groups,
                        true,
                        fixture.configurations().getFirst().limits(),
                        joining
                    ),
                    fixture.key(),
                    fixture.trust(),
                    fixture.space()::get,
                    intents,
                    intents
                )
            );
            final var channel = peerChannel(retained, groups.getFirst().id(), removed.id());
            assertFalse(channel.isShutdown());
            for (int position = 0; position < groups.size(); position++) {
                final var group = groups.get(position);
                final var intent = intents.get(group.id());
                assertBusy(() -> {
                    try {
                        final var current = leader(journals, group.id());
                        current.advanceReplacement(intent).toCompletableFuture().get(10, TimeUnit.SECONDS);
                        assertTrue(current.replacementReceipt(intent).isPresent());
                        assertTrue(retained.replacementReceipt(intent).isPresent());
                    } catch (Exception waiting) {
                        throw new AssertionError("waiting for the colocated replacement", waiting);
                    }
                }, 40, TimeUnit.SECONDS);
                if (position == 0) {
                    assertFalse("the other group still requires this peer", retained.releaseUnusedPeer(removed.id()));
                    assertFalse(channel.isShutdown());
                } else {
                    assertTrue(retained.releaseUnusedPeer(removed.id()));
                    assertTrue(channel.isShutdown());
                    assertTrue("releasing an already unused peer is idempotent", retained.releaseUnusedPeer(removed.id()));
                }
            }
            for (var group : groups) {
                final var stream = new ShardLogId(fixture.deployment(), "after-transport-retirement", 0, UUID.randomUUID());
                leader(journals, group.id()).append(group.id(), operation(stream, 1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertBusy(
                    () -> assertEquals(1, retained.scan(group.id(), -1, Set.of(stream), 100, 65536).operations().size()),
                    20,
                    TimeUnit.SECONDS
                );
            }
        } finally {
            closeAll(journals);
        }
    }

    @SuppressForbidden(reason = "Inspect an existing pinned SDK channel used by real replication")
    private static org.apache.ratis.thirdparty.io.grpc.ManagedChannel peerChannel(RatisJournal journal, UUID group, UUID peer)
        throws Exception {
        final var division = (org.apache.ratis.server.RaftServer.Division) nativeDivisionForStall(journal, group);
        final Object client = ((org.apache.ratis.server.RaftServerRpcWithProxy<?, ?>) division.getRaftServer().getServerRpc()).getProxies()
            .getProxy(org.apache.ratis.protocol.RaftPeerId.valueOf(peer.toString()));
        final var channel = client.getClass().getDeclaredField("channel");
        channel.setAccessible(true);
        return (org.apache.ratis.thirdparty.io.grpc.ManagedChannel) channel.get(client);
    }

    public void testPeerRpcExecutionRemainsBoundedWhenTheConsensusMonitorStalls() throws Exception {
        final var fixture = fixture(1);
        final var journals = new ArrayList<RatisJournal>();
        final var entered = new java.util.concurrent.CountDownLatch(1);
        final var release = new java.util.concurrent.CountDownLatch(1);
        Thread holder = null;
        try {
            startAll(fixture, journals, true);
            final var placement = fixture.configurations().getFirst().groups().getFirst();
            final var target = leader(journals, placement.id());
            final int targetIndex = journals.indexOf(target);
            final var local = fixture.configurations().get(targetIndex).localVoterId();
            final var address = placement.peers().stream().filter(peer -> peer.id().equals(local)).findFirst().orElseThrow();
            final var candidate = placement.peers().stream().filter(peer -> peer.id().equals(local) == false).findFirst().orElseThrow();
            final Object monitor = nativeDivisionForStall(target, placement.id());
            holder = org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory("stalled-raft-monitor").newThread(() -> {
                synchronized (monitor) {
                    entered.countDown();
                    try {
                        if (release.await(20, TimeUnit.SECONDS) == false) {
                            throw new AssertionError("test did not release the consensus monitor");
                        }
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(failure);
                    }
                }
            });
            final var tls = GrpcSslContexts.forClient().keyManager(fixture.key()).trustManager(fixture.trust()).build();
            final var channel = NettyChannelBuilder.forAddress(address.host(), address.port()).sslContext(tls).build();
            try {
                final var rpc = org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto.newBuilder()
                    .setRequestorId(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFromUtf8(candidate.id().toString()))
                    .setReplyId(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFromUtf8(local.toString()))
                    .setRaftGroupId(
                        org.apache.ratis.util.ProtoUtils.toRaftGroupIdProtoBuilder(
                            org.apache.ratis.protocol.RaftGroupId.valueOf(placement.id())
                        )
                    )
                    .setCallId(0)
                    .build();
                final var vote = org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto.newBuilder()
                    .setServerRequest(rpc)
                    .setCandidateTerm(0)
                    .setCandidateLastEntry(org.apache.ratis.proto.RaftProtos.TermIndexProto.newBuilder().setTerm(0).setIndex(0))
                    .setPreVote(true)
                    .build();
                final var readyStub = org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.newFutureStub(channel);
                // Establish TLS/HTTP2 before the stall, so cold connection setup cannot consume
                // the overload window and turn the burst into client-side deadline failures.
                readyStub.withDeadlineAfter(10, TimeUnit.SECONDS).requestVote(vote).get(10, TimeUnit.SECONDS);
                holder.start();
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                final var stub = readyStub.withDeadlineAfter(6, TimeUnit.SECONDS);
                final var requests = new ArrayList<org.apache.ratis.thirdparty.com.google.common.util.concurrent.ListenableFuture<?>>();
                for (int i = 0; i < 256; i++) {
                    requests.add(stub.requestVote(vote.toBuilder().setServerRequest(rpc.toBuilder().setCallId(i + 1)).build()));
                }
                int rejected = 0;
                for (var request : requests) {
                    final var failure = expectThrows(ExecutionException.class, () -> request.get(10, TimeUnit.SECONDS));
                    final var status = Status.fromThrowable(failure.getCause());
                    if (status.getCode() == Status.Code.INTERNAL) {
                        // The pinned gRPC transport reports executor rejection as an RPC failure.
                        // It must back off rather than create another blocked handler thread.
                        rejected++;
                    } else {
                        assertEquals(status.toString(), Status.Code.DEADLINE_EXCEEDED, status.getCode());
                    }
                }
                assertTrue("the burst must exercise the bounded queue's overload path", rejected > 0);
                final int identity = System.identityHashCode(monitor);
                final long blocked = java.util.Arrays.stream(
                    java.lang.management.ManagementFactory.getThreadMXBean().dumpAllThreads(false, false)
                )
                    .filter(
                        info -> info.getThreadState() == Thread.State.BLOCKED
                            && info.getLockInfo() != null
                            && info.getLockInfo().getIdentityHashCode() == identity
                    )
                    .filter(
                        info -> java.util.Arrays.stream(info.getStackTrace())
                            .anyMatch(
                                frame -> frame.getClassName().equals("org.apache.ratis.grpc.server.GrpcServerProtocolService")
                                    && frame.getMethodName().equals("requestVote")
                            )
                    )
                    .count();
                assertTrue("the fixture must reach the blocked peer RPC handler", blocked > 0);
                assertTrue("peer RPC threads exceeded the node execution bound: " + blocked, blocked <= 4);
            } finally {
                release.countDown();
                assertTrue(channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
            }
            final var stream = new ShardLogId(fixture.deployment(), "after-peer-overload", 0, UUID.randomUUID());
            leader(journals, placement.id()).append(placement.id(), operation(stream, 1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertBusy(
                () -> assertEquals(1, target.scan(placement.id(), -1, Set.of(stream), 100, 65536).operations().size()),
                20,
                TimeUnit.SECONDS
            );
        } finally {
            release.countDown();
            if (holder != null) {
                holder.join(TimeUnit.SECONDS.toMillis(10));
                assertFalse(holder.isAlive());
            }
            closeAll(journals);
        }
    }

    @SuppressForbidden(reason = "Model an unavailable pinned-SDK consensus monitor without a production fault-injection API")
    private static Object nativeDivisionForStall(RatisJournal journal, UUID group) throws Exception {
        final var field = RatisJournal.class.getDeclaredField("server");
        field.setAccessible(true);
        final var server = (org.apache.ratis.server.RaftServer) field.get(journal);
        return server.getDivision(org.apache.ratis.protocol.RaftGroupId.valueOf(group));
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

    private static void startAll(Fixture fixture, List<RatisJournal> journals, boolean bootstrap) throws Exception {
        openAll(fixture, journals, bootstrap);
        for (var group : fixture.configurations().getFirst().groups()) {
            leader(journals, group.id()).activateGroup(group.id()).toCompletableFuture().get(20, TimeUnit.SECONDS);
        }
    }

    private static void openAll(Fixture fixture, List<RatisJournal> journals, boolean bootstrap) throws Exception {
        openAll(fixture, journals, bootstrap, Map.of());
    }

    private static void openAll(
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
