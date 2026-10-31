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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

    @SuppressForbidden(reason = "Ratis storage configuration requires java.io.File")
    private static void configureStorage(RaftProperties properties, Path storage) {
        RaftServerConfigKeys.setStorageDir(properties, List.of(storage.toFile()));
    }
}
