/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.SizeInBytes;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Exercises native library failure cleanup before the journal is allowed to serve traffic. */
@SuppressFileSystems("*")
public class RatisFailureCleanupTests extends OpenSearchTestCase {
    public void testUsesOneMergedPatchedServerArtifact() throws Exception {
        final var loader = RaftServer.class.getClassLoader();
        final var replacement = Class.forName("org.apache.ratis.server.impl.RaftServerProxy", false, loader);
        final var helper = Class.forName("org.apache.ratis.server.impl.ServerImplUtils", false, loader);
        final var origin = replacement.getProtectionDomain().getCodeSource().getLocation();
        assertEquals(origin, helper.getProtectionDomain().getCodeSource().getLocation());
        assertTrue(origin.toString(), origin.toString().contains("opensearch-ratis-server"));
        final var copies = loader.getResources("org/apache/ratis/server/impl/RaftServerProxy.class");
        assertTrue(copies.hasMoreElements());
        copies.nextElement();
        assertFalse("the unpatched server must not leak onto the runtime classpath", copies.hasMoreElements());
    }

    public void testFailedGroupInitializationReleasesItsStorageLock() throws Exception {
        // Reuse the host embedding policy before any native startup error can call System.exit.
        Class.forName(RatisJournal.class.getName(), true, RatisJournal.class.getClassLoader());
        final Path storage = createTempDir();
        final var groupId = RaftGroupId.randomId();
        final var peerId = RaftPeerId.valueOf("failing-peer");
        final int port;
        try (var reservation = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
            port = reservation.getLocalPort();
        }
        final var peer = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        final var group = RaftGroup.valueOf(groupId, peer);
        final var properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        GrpcConfigKeys.Server.setHost(properties, "127.0.0.1");
        GrpcConfigKeys.Server.setPort(properties, port);
        configureStorage(properties, storage);
        final var acquiredStorage = new AtomicReference<RaftStorage>();
        final var acquiredDivision = new AtomicReference<RaftServer.Division>();
        try {
            try (
                var server = RaftServer.newBuilder()
                    .setServerId(peerId)
                    .setProperties(properties)
                    .setStateMachineRegistry(ignored -> new BaseStateMachine() {
                        @Override
                        public void initialize(RaftServer raftServer, RaftGroupId id, RaftStorage raftStorage) throws IOException {
                            acquiredStorage.set(raftStorage);
                            acquiredDivision.set(raftServer.getDivision(id));
                            throw new IOException("injected state-machine initialization failure");
                        }
                    })
                    .build()
            ) {
                server.start();
                final var failure = expectThrows(
                    ExecutionException.class,
                    () -> server.groupManagementAsync(GroupManagementRequest.newAdd(ClientId.randomId(), peerId, 0, group, false))
                        .get(10, TimeUnit.SECONDS)
                );
                assertTrue(failure.toString(), failure.toString().contains("injected state-machine initialization failure"));
            }
            final Path lockPath = storage.resolve(groupId.getUuid().toString()).resolve("in_use.lock");
            try (var channel = FileChannel.open(lockPath, StandardOpenOption.WRITE)) {
                try (var lock = channel.tryLock()) {
                    assertNotNull("failed group startup must release its native storage lock", lock);
                } catch (OverlappingFileLockException e) {
                    throw new AssertionError("failed group startup leaked its native storage lock", e);
                }
            }
        } finally {
            // Also clean the failing upstream implementation when this is run as a reproducer.
            if (acquiredDivision.get() != null) {
                acquiredDivision.get().close();
            }
            if (acquiredStorage.get() != null) {
                acquiredStorage.get().close();
            }
        }
    }

    public void testCorruptLogOpenClosesTheUnpublishedLog() throws Exception {
        Class.forName(RatisJournal.class.getName(), true, RatisJournal.class.getClassLoader());
        final Path storage = createTempDir();
        final var groupId = RaftGroupId.randomId();
        final var peerId = RaftPeerId.valueOf("corrupt-log-peer");
        final Path groupDirectory = storage.resolve(groupId.getUuid().toString());
        formatGroupStorage(groupDirectory);
        final byte[] corruptHeader = new byte[64];
        Arrays.fill(corruptHeader, (byte) 0x7f);
        Files.write(groupDirectory.resolve("current/log_inprogress_0"), corruptHeader);
        final int port;
        try (var reservation = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
            port = reservation.getLocalPort();
        }
        final var peer = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        final var group = RaftGroup.valueOf(groupId, peer);
        final var properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        GrpcConfigKeys.Server.setHost(properties, "127.0.0.1");
        GrpcConfigKeys.Server.setPort(properties, port);
        RaftServerConfigKeys.Log.setCorruptionPolicy(properties, RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION);
        configureStorage(properties, storage);
        final String cacheThread = RaftGroupMemberId.valueOf(peerId, groupId) + "-cacheEviction-AwaitToRun";
        try {
            try (
                var server = RaftServer.newBuilder()
                    .setServerId(peerId)
                    .setGroup(group)
                    .setOption(RaftStorage.StartupOption.RECOVER)
                    .setProperties(properties)
                    .setStateMachineRegistry(ignored -> new BaseStateMachine())
                    .build()
            ) {
                final var failure = expectThrows(Exception.class, server::start);
                Throwable root = failure;
                while (root.getCause() != null) {
                    root = root.getCause();
                }
                assertTrue("must actually exercise corrupted log recovery: " + root, root.getMessage().contains("Log header mismatched"));
            }
            assertBusy(
                () -> assertFalse("failed log open leaked its cache-eviction thread", fixtureThreads(cacheThread).isEmpty() == false),
                5,
                TimeUnit.SECONDS
            );
            assertArrayEquals(
                "failed recovery must preserve the corrupted segment for diagnosis",
                corruptHeader,
                Files.readAllBytes(groupDirectory.resolve("current/log_inprogress_0"))
            );
        } finally {
            // Reproducer cleanup targets only this fixture's unique group thread; retain other leak checks.
            for (Thread thread : fixtureThreads(cacheThread)) {
                thread.interrupt();
                thread.join(5000);
            }
        }
    }

    @SuppressForbidden(reason = "Dedicated fault test inspects and cleans only its uniquely named native worker; security manager is off")
    private static List<Thread> fixtureThreads(String exactName) {
        return Thread.getAllStackTraces()
            .keySet()
            .stream()
            .filter(thread -> thread.getName().equals(exactName) && thread.isAlive())
            .toList();
    }

    @SuppressForbidden(reason = "Native Ratis storage formatting requires java.io.File")
    private static void formatGroupStorage(Path directory) throws IOException {
        try (
            var storage = RaftStorage.newBuilder()
                .setDirectory(directory.toFile())
                .setOption(RaftStorage.StartupOption.FORMAT)
                .setStorageFreeSpaceMin(SizeInBytes.valueOf(1))
                .setLogCorruptionPolicy(RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION)
                .build()
        ) {
            storage.initialize();
        }
    }

    @SuppressForbidden(reason = "Ratis storage configuration requires java.io.File")
    private static void configureStorage(RaftProperties properties, Path storage) {
        // Match the runtime's owned event loops, including cleanup before an RPC server starts.
        GrpcConfigKeys.Server.setBossGroupSize(properties, 1);
        GrpcConfigKeys.Server.setWorkerGroupSize(properties, 2);
        RaftServerConfigKeys.setStorageDir(properties, List.of(storage.toFile()));
    }
}
