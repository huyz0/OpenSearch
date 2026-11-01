/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeoutTimer;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.log.JournalConfiguration;
import org.opensearch.serverless.log.JournalDiskIdentity;
import org.opensearch.serverless.log.OperationEnvelope;
import org.opensearch.serverless.log.RatisJournal;
import org.opensearch.serverless.log.ShardLogId;
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
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Real local journal activation plus conditional object-store metadata, not a three-AZ test. */
@SuppressFileSystems("*")
@ThreadLeakLingering(linger = 70_000)
@ThreadLeakFilters(filters = JournalGroupCatalogRuntimeTests.GlobalTimerFilter.class)
public class JournalGroupCatalogRuntimeTests extends OpenSearchTestCase {
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

    public void testOnlyCommittedActivationCanBePublishedAndLostReplyIsIdempotent() throws Exception {
        final var directories = new ArrayList<Path>();
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        final var ports = new ArrayList<ServerSocket>();
        try {
            for (int i = 0; i < 3; i++) {
                final var directory = createTempDir();
                directories.add(directory);
                final var port = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"));
                ports.add(port);
                peers.add(
                    new JournalConfiguration.Peer(JournalDiskIdentity.discover(directory), "127.0.0.1", port.getLocalPort(), "az-" + i)
                );
            }
        } finally {
            IOUtils.close(ports);
        }
        final var group = new JournalConfiguration.Group(UUID.randomUUID(), peers);
        final UUID deployment = UUID.randomUUID();
        final var keys = KeyStore.getInstance("PKCS12");
        try (var input = getClass().getResourceAsStream("/org/opensearch/serverless/testkit/tls-test-keystore.p12")) {
            keys.load(input, "testonly".toCharArray());
        }
        final var keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyFactory.init(keys, "testonly".toCharArray());
        final var trust = KeyStore.getInstance("PKCS12");
        try (var input = getClass().getResourceAsStream("/org/opensearch/serverless/testkit/tls-test-truststore.p12")) {
            trust.load(input, "testonly".toCharArray());
        }
        final var trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trust);
        final var key = (X509KeyManager) keyFactory.getKeyManagers()[0];
        final var trusted = (X509TrustManager) trustFactory.getTrustManagers()[0];
        final var limits = new JournalConfiguration.Limits(2, 16384, 32, 1024 * 1024, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        final var journals = new ArrayList<RatisJournal>();
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var real = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var loseReply = new AtomicBoolean();
            final var loseMembershipReply = new AtomicBoolean();
            final var loseReceiptReply = new AtomicBoolean();
            final var hideReceiptRead = new AtomicBoolean();
            final var wrapper = new FilterBlobContainer(real) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("catalog must not list children");
                }

                @Override
                public Optional<BlobRegister> readRegister(String name) throws IOException {
                    if (name.startsWith("completed-") && hideReceiptRead.compareAndSet(true, false)) {
                        return Optional.empty(); // Model a competing publisher winning after our empty read.
                    }
                    return real.readRegister(name);
                }

                @Override
                public BlobRegisterCasResult createRegisterIfAbsent(String name, BytesReference value) throws IOException {
                    final var result = real.createRegisterIfAbsent(name, value);
                    if (name.startsWith("membership-") && result.applied() && loseMembershipReply.compareAndSet(true, false)) {
                        throw new IOException("lost membership-head creation reply after persistence");
                    }
                    if (name.startsWith("completed-") && result.applied() && loseReceiptReply.compareAndSet(true, false)) {
                        throw new IOException("lost replacement receipt reply after persistence");
                    }
                    return result;
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String name, long expected, BytesReference value) throws IOException {
                    final var result = real.compareAndSwapRegister(name, expected, value);
                    if (name.startsWith("membership-") && result.applied() && loseMembershipReply.compareAndSet(true, false)) {
                        throw new IOException("lost membership-head reply before activation publication");
                    }
                    if (name.startsWith("group-") && result.applied() && loseReply.compareAndSet(true, false)) {
                        throw new IOException("lost activation reply after persistence");
                    }
                    return result;
                }
            };
            final var catalog = new JournalGroupCatalog(wrapper, deployment);
            final var reservation = catalog.reserve(group);
            try {
                for (int i = 0; i < 3; i++) {
                    final var configuration = new JournalConfiguration(
                        deployment,
                        peers.get(i).id(),
                        directories.get(i),
                        List.of(group),
                        true,
                        limits
                    );
                    journals.add(catalog.openJournal(configuration, key, trusted, () -> Long.MAX_VALUE));
                }
                expectThrows(IOException.class, () -> catalog.publishActivation(journals.getFirst(), group.id()));
                assertEquals(reservation, catalog.read(group.id()).orElseThrow());
                final var leader = new RatisJournal[1];
                assertBusy(() -> {
                    for (var journal : journals) {
                        if (journal.isLeaderReady(group.id())) {
                            leader[0] = journal;
                            return;
                        }
                    }
                    fail("no ready journal leader");
                }, 30, TimeUnit.SECONDS);
                leader[0].activateGroup(group.id()).toCompletableFuture().get(20, TimeUnit.SECONDS);
                loseMembershipReply.set(true);
                expectThrows(IOException.class, () -> catalog.publishActivation(leader[0], group.id()));
                assertEquals(reservation, catalog.read(group.id()).orElseThrow());
                assertTrue(real.readRegister("membership-" + group.id()).isPresent());
                loseReply.set(true);
                expectThrows(IOException.class, () -> catalog.publishActivation(leader[0], group.id()));
                final var active = catalog.read(group.id()).orElseThrow();
                assertNotNull(active.activation());
                assertEquals(leader[0].groupActivation(group.id()).checkpoint(), active.activation());
                final var restartedController = new JournalGroupCatalog(real, deployment);
                assertEquals(active, restartedController.publishActivation(leader[0], group.id()));
                assertEquals(active, restartedController.reserve(group));
                assertEquals(active, restartedController.membership(group.id()).genesis());
                assertNull(restartedController.membership(group.id()).pending());
                final var configuration = new JournalConfiguration(
                    deployment,
                    peers.getFirst().id(),
                    directories.getFirst(),
                    List.of(group),
                    true,
                    limits
                );
                final var refused = expectThrows(
                    IOException.class,
                    () -> restartedController.openJournal(configuration, key, trusted, () -> Long.MAX_VALUE)
                );
                assertTrue(refused.getMessage(), refused.getMessage().contains("activated physical group"));
                final var removed = peers.get(
                    journals.indexOf(journals.stream().filter(journal -> journal != leader[0]).findFirst().orElseThrow())
                );
                final Path learnerDirectory = createTempDir();
                final JournalConfiguration.Peer joining;
                try (var port = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
                    joining = new JournalConfiguration.Peer(
                        JournalDiskIdentity.discover(learnerDirectory),
                        "127.0.0.1",
                        port.getLocalPort(),
                        removed.availabilityZone()
                    );
                }
                final var targetPeers = new ArrayList<>(peers);
                targetPeers.remove(removed);
                targetPeers.add(joining);
                final var intent = catalog.prepareReplacement(
                    group.id(),
                    0,
                    UUID.randomUUID(),
                    new JournalConfiguration.Group(group.id(), targetPeers)
                );
                final var learnerConfiguration = new JournalConfiguration(
                    deployment,
                    joining.id(),
                    learnerDirectory,
                    List.of(group),
                    true,
                    limits,
                    joining
                );
                journals.add(catalog.openAssignedJournal(learnerConfiguration, key, trusted, () -> Long.MAX_VALUE));
                expectThrows(IOException.class, () -> catalog.publishReplacement(leader[0], intent));
                assertBusy(() -> {
                    try {
                        leader[0].advanceReplacement(intent).toCompletableFuture().get(10, TimeUnit.SECONDS);
                        assertTrue(leader[0].replacementReceipt(intent).isPresent());
                    } catch (Exception waiting) {
                        throw new AssertionError("waiting for native replacement completion", waiting);
                    }
                }, 30, TimeUnit.SECONDS);
                final var nativeProof = leader[0].replacementReceipt(intent).orElseThrow();
                loseReceiptReply.set(true);
                expectThrows(IOException.class, () -> catalog.publishReplacement(leader[0], intent));
                assertEquals(intent, restartedController.membership(group.id()));
                final var previousLeader = leader[0];
                leader[0].close();
                assertBusy(() -> {
                    leader[0] = readyLeader(journals, group.id());
                    final var newerProof = leader[0].replacementReceipt(intent);
                    assertTrue(newerProof.isPresent());
                    assertTrue(newerProof.get().targetConfigurationIndex() > nativeProof.targetConfigurationIndex());
                }, 30, TimeUnit.SECONDS);
                hideReceiptRead.set(true);
                loseMembershipReply.set(true);
                expectThrows(IOException.class, () -> catalog.publishReplacement(leader[0], intent));
                assertEquals(nativeProof.completedMembership(), restartedController.membership(group.id()));
                final var completedController = new JournalGroupCatalog(real, deployment);
                final var published = completedController.publishReplacement(leader[0], intent);
                assertEquals(nativeProof, published);
                assertEquals(published, catalog.publishReplacement(leader[0], intent));
                final var stream = new ShardLogId(deployment, "replacement-catalog", 0, UUID.randomUUID());
                leader[0].append(group.id(), new OperationEnvelope(stream, OperationEnvelope.Kind.DOCUMENTS, 1, 1, 1, new byte[] { 1 }))
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
                // Replace the first learner again: its original reservation remains immutable,
                // while retries for the first operation must return the earlier stored receipt.
                final Path secondDirectory = createTempDir();
                final JournalConfiguration.Peer secondJoining;
                try (var port = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
                    secondJoining = new JournalConfiguration.Peer(
                        JournalDiskIdentity.discover(secondDirectory),
                        "127.0.0.1",
                        port.getLocalPort(),
                        joining.availabilityZone()
                    );
                }
                final var secondPeers = new ArrayList<>(targetPeers);
                secondPeers.remove(joining);
                secondPeers.add(secondJoining);
                final var retiredOriginal = journals.get(peers.indexOf(removed));
                final var retainedJournal = journals.subList(0, 3)
                    .stream()
                    .filter(journal -> journal != previousLeader && journal != retiredOriginal)
                    .findFirst()
                    .orElseThrow();
                final var oldChannel = establishedPeerChannel(retainedJournal, group.id(), joining.id());
                assertFalse(oldChannel.isShutdown());
                final var secondIntent = completedController.prepareReplacement(
                    group.id(),
                    1,
                    UUID.randomUUID(),
                    new JournalConfiguration.Group(group.id(), secondPeers)
                );
                journals.add(
                    completedController.openAssignedJournal(
                        new JournalConfiguration(
                            deployment,
                            secondJoining.id(),
                            secondDirectory,
                            List.of(group),
                            true,
                            limits,
                            secondJoining
                        ),
                        key,
                        trusted,
                        () -> Long.MAX_VALUE
                    )
                );
                assertBusy(() -> {
                    try {
                        leader[0] = readyLeader(journals, group.id());
                        leader[0].advanceReplacement(secondIntent).toCompletableFuture().get(10, TimeUnit.SECONDS);
                        assertTrue(leader[0].replacementReceipt(secondIntent).isPresent());
                    } catch (Exception waiting) {
                        throw new AssertionError("waiting for the next native replacement", waiting);
                    }
                }, 30, TimeUnit.SECONDS);
                final var secondReceipt = completedController.publishReplacement(leader[0], secondIntent);
                assertEquals(2, secondReceipt.completedMembership().revision());
                assertBusy(() -> {
                    try {
                        // The new learner can win this election. Reconcile on the retained host
                        // whose local channel this test captured, independently of the publisher.
                        assertEquals(secondReceipt, completedController.publishReplacement(retainedJournal, secondIntent));
                        assertTrue("the retained host must release its retired peer channel", oldChannel.isShutdown());
                    } catch (IOException waiting) {
                        throw new AssertionError("waiting for local peer cleanup", waiting);
                    }
                }, 10, TimeUnit.SECONDS);
                assertEquals(published, new JournalGroupCatalog(real, deployment).publishReplacement(leader[0], intent));
                expectThrows(IOException.class, () -> leader[0].advanceReplacement(intent));
                final String receiptKey = "completed-" + group.id() + "-" + intent.pending().id();
                final var receiptRegister = real.readRegister(receiptKey).orElseThrow();
                assertTrue(real.deleteRegisterIfUnchanged(receiptKey, receiptRegister.generation()));
                expectThrows(IOException.class, () -> completedController.publishReplacement(leader[0], intent));
                assertTrue(real.readRegister(receiptKey).isEmpty());
                final String membershipKey = "membership-" + group.id();
                final var head = real.readRegister(membershipKey).orElseThrow();
                assertTrue(real.deleteRegisterIfUnchanged(membershipKey, head.generation()));
                expectThrows(IOException.class, () -> restartedController.publishActivation(leader[0], group.id()));
                assertTrue(real.readRegister(membershipKey).isEmpty());
            } finally {
                IOUtils.close(journals);
            }
        }
    }

    @SuppressForbidden(reason = "Inspect the exact pinned SDK transport channel after a real peer RPC")
    private static org.apache.ratis.thirdparty.io.grpc.ManagedChannel establishedPeerChannel(RatisJournal journal, UUID group, UUID peer)
        throws Exception {
        final var serverField = RatisJournal.class.getDeclaredField("server");
        serverField.setAccessible(true);
        final var server = (org.apache.ratis.server.RaftServer) serverField.get(journal);
        final var request = org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto.newBuilder()
            .setServerRequest(
                org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto.newBuilder()
                    .setRequestorId(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFromUtf8(server.getId().toString()))
                    .setReplyId(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFromUtf8(peer.toString()))
                    .setRaftGroupId(
                        org.apache.ratis.util.ProtoUtils.toRaftGroupIdProtoBuilder(org.apache.ratis.protocol.RaftGroupId.valueOf(group))
                    )
                    .setCallId(0)
            )
            .setCandidateTerm(0)
            .setCandidateLastEntry(org.apache.ratis.proto.RaftProtos.TermIndexProto.newBuilder().setTerm(0).setIndex(0))
            .setPreVote(true)
            .build();
        server.getServerRpc().requestVote(request);
        final Object client = ((org.apache.ratis.server.RaftServerRpcWithProxy<?, ?>) server.getServerRpc()).getProxies()
            .getProxy(org.apache.ratis.protocol.RaftPeerId.valueOf(peer.toString()));
        final var channel = client.getClass().getDeclaredField("channel");
        channel.setAccessible(true);
        return (org.apache.ratis.thirdparty.io.grpc.ManagedChannel) channel.get(client);
    }

    private static RatisJournal readyLeader(List<RatisJournal> journals, UUID group) {
        for (var journal : journals) {
            try {
                if (journal.isLeaderReady(group)) {
                    return journal;
                }
            } catch (IOException unavailable) {
                // Closed or retired local group; other members can still form a quorum.
            }
        }
        throw new AssertionError("waiting for a ready journal leader");
    }

}
