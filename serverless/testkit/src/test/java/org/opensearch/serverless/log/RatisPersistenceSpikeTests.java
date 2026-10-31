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

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutTimer;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A local feasibility probe, not a production consensus implementation or an AZ-failure test.
 * Ratis is test-scoped; no service endpoint or storage mode is enabled by this probe.
 */
@ThreadLeakLingering(linger = 10_000)
@ThreadLeakFilters(filters = RatisPersistenceSpikeTests.GlobalTimerFilter.class)
public class RatisPersistenceSpikeTests extends OpenSearchTestCase {

    /** Ratis has eight JVM-global daemon Timers with no shutdown API; keep all other leak checks. */
    public static class GlobalTimerFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            if (thread.isDaemon() == false || thread.getName().matches("timer[0-7]") == false) {
                return false;
            }
            for (StackTraceElement frame : thread.getStackTrace()) {
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

    public void testPersistedPrefixSurvivesLeaderLossAndCompleteRestart() throws Exception {
        final List<RaftPeer> peers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            try (var socket = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
                peers.add(RaftPeer.newBuilder().setId("peer-" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
            }
        }
        final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
        final Path storage = createTempDir();
        final List<RaftServer> servers = new ArrayList<>();
        try {
            for (RaftPeer peer : peers) {
                servers.add(start(peer, group, storage));
            }
            append(group, 1, 100);
            RaftServer leader = null;
            for (RaftServer server : servers) {
                if (server.getDivision(group.getGroupId()).getInfo().isLeader()) {
                    leader = server;
                    break;
                }
            }
            assertNotNull("a successful commit must have a leader", leader);
            leader.close();
            servers.remove(leader);
            append(group, 101, 200);
            closeServers(servers);
            for (RaftPeer peer : peers) {
                servers.add(start(peer, group, storage));
            }
            // Every replica has a new in-memory state machine. Reaching 201 requires disk replay.
            append(group, 201, 201);
        } finally {
            closeServers(servers);
        }
    }

    static RaftServer start(RaftPeer peer, RaftGroup group, Path storage) throws Exception {
        return start(peer, group, storage, new CountingStateMachine());
    }

    static RaftServer start(RaftPeer peer, RaftGroup group, Path storage, StateMachine stateMachine) throws Exception {
        // Initialize the host embedding policy before native SDK startup can install its handler.
        Class.forName(RatisJournal.class.getName(), true, RatisJournal.class.getClassLoader());
        final RaftProperties properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        GrpcConfigKeys.Server.setHost(properties, "127.0.0.1");
        GrpcConfigKeys.Server.setPort(properties, Integer.parseInt(peer.getAddress().split(":")[1]));
        configureStorage(properties, storage.resolve(peer.getId().toString()));
        // Native staging uses matchIndex + gap > committedIndex, so one means no committed gap.
        RaftServerConfigKeys.setStagingCatchupGap(properties, 1);
        RaftServerConfigKeys.Log.StateMachineData.setSyncTimeout(properties, TimeDuration.valueOf(60, TimeUnit.SECONDS));
        RaftServerConfigKeys.Log.setUnsafeFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, false);
        final RaftServer server = RaftServer.newBuilder()
            .setServerId(peer.getId())
            .setGroup(group)
            .setProperties(properties)
            .setStateMachineRegistry(ignored -> stateMachine)
            .setOption(RaftStorage.StartupOption.RECOVER)
            .build();
        try {
            server.start();
            return server;
        } catch (Exception | Error e) {
            server.close();
            throw e;
        }
    }

    @SuppressForbidden(reason = "Ratis storage configuration requires java.io.File")
    private static void configureStorage(RaftProperties properties, Path directory) {
        RaftServerConfigKeys.setStorageDir(properties, List.of(directory.toFile()));
    }

    static void append(RaftGroup group, int first, int last) throws Exception {
        final RaftProperties properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        try (
            RaftClient client = RaftClient.newBuilder()
                .setRaftGroup(group)
                .setProperties(properties)
                .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(30, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS)))
                .build()
        ) {
            for (int i = first; i <= last; i++) {
                final var reply = client.io().send(Message.valueOf("operation-" + i));
                assertTrue(reply.toString(), reply.isSuccess());
                assertEquals(Integer.toString(i), reply.getMessage().getContent().toStringUtf8());
            }
        }
    }

    static void closeServers(List<RaftServer> servers) throws Exception {
        Exception first = null;
        for (RaftServer server : servers) {
            try {
                server.close();
            } catch (Exception e) {
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        servers.clear();
        if (first != null) {
            throw first;
        }
    }

    private static final class CountingStateMachine extends BaseStateMachine {
        private final AtomicLong applied = new AtomicLong();

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext transaction) {
            final var entry = transaction.getLogEntry();
            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
            return CompletableFuture.completedFuture(Message.valueOf(Long.toString(applied.incrementAndGet())));
        }
    }
}
