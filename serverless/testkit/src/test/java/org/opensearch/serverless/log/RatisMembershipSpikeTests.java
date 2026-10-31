/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.SetConfigurationException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/** Native-library feasibility only: no production reconfiguration API, TLS or AZ qualification. */
@ThreadLeakLingering(linger = 10_000)
@ThreadLeakFilters(filters = RatisPersistenceSpikeTests.GlobalTimerFilter.class)
public class RatisMembershipSpikeTests extends OpenSearchTestCase {
    public void testLearnerRequiresItsOwnCatchupProofBeforeAtomicVoterReplacement() throws Exception {
        // Preserve the embedding host's uncaught exception policy even for bare SDK fixtures.
        Class.forName(RatisJournal.class.getName(), true, RatisJournal.class.getClassLoader());
        final var peers = new ArrayList<RaftPeer>();
        final var ports = new ArrayList<ServerSocket>();
        try {
            for (int i = 0; i < 4; i++) {
                final var port = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"));
                ports.add(port);
                peers.add(RaftPeer.newBuilder().setId("peer-" + i).setAddress("127.0.0.1:" + port.getLocalPort()).build());
            }
        } finally {
            IOUtils.close(ports);
        }
        final var initial = List.copyOf(peers.subList(0, 3));
        final var group = RaftGroup.valueOf(RaftGroupId.randomId(), initial);
        final var listener = RaftPeer.newBuilder()
            .setId(peers.get(3).getId())
            .setAddress(peers.get(3).getAddress())
            .setStartupRole(RaftPeerRole.LISTENER)
            .build();
        final var servers = new ArrayList<RaftServer>();
        final var storage = createTempDir();
        final var gate = new AtomicReference<CompletableFuture<Void>>();
        final var entered = new CountDownLatch(1);
        try {
            for (var peer : initial) {
                servers.add(RatisPersistenceSpikeTests.start(peer, group, storage));
            }
            RatisPersistenceSpikeTests.append(group, 1, 20);
            final var leader = leader(servers, group);
            final var withListener = new ArrayList<>(initial);
            withListener.add(listener);
            final var learner = RatisPersistenceSpikeTests.start(
                listener,
                RaftGroup.valueOf(group.getGroupId(), withListener),
                storage,
                new GatedStateMachine(gate, entered)
            );
            servers.add(learner);
            assertTrue(
                setConfiguration(leader, group, initial, List.of(), initial, List.of(listener)).get(20, TimeUnit.SECONDS).isSuccess()
            );
            assertBusy(() -> assertTrue(learner.getDivision(group.getGroupId()).getInfo().isListener()), 10, TimeUnit.SECONDS);

            final var pending = new CompletableFuture<Void>();
            gate.set(pending);
            RatisPersistenceSpikeTests.append(group, 21, 21);
            final long cut = leader.getDivision(group.getGroupId()).getRaftLog().getLastCommittedIndex();
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            final var watch = RaftClientRequest.newBuilder()
                .setClientId(ClientId.randomId())
                .setServerId(leader.getId())
                .setGroupId(group.getGroupId())
                .setCallId(1)
                .setMessage(Message.valueOf(ByteString.EMPTY))
                .setType(RaftClientRequest.watchRequestType(cut, ReplicationLevel.ALL_COMMITTED))
                .build();
            assertTrue(leader.submitClientRequestAsync(watch).get(10, TimeUnit.SECONDS).isSuccess());
            assertTrue("voter ALL_COMMITTED must not masquerade as learner catch-up", learnerCommit(leader, group, listener) < cut);
            assertTrue(learner.getDivision(group.getGroupId()).getRaftLog().getFlushIndex() < cut);
            pending.complete(null);
            assertBusy(() -> assertTrue(learnerCommit(leader, group, listener) >= cut), 10, TimeUnit.SECONDS);
            assertBusy(
                () -> assertTrue(learner.getDivision(group.getGroupId()).getInfo().getLastAppliedIndex() >= cut),
                10,
                TimeUnit.SECONDS
            );

            final var removed = initial.stream().filter(peer -> peer.getId().equals(leader.getId()) == false).findFirst().orElseThrow();
            final var replacement = new ArrayList<>(initial);
            replacement.remove(removed);
            replacement.add(peers.get(3));
            assertTrue(
                setConfiguration(leader, group, initial, List.of(listener), replacement, List.of()).get(20, TimeUnit.SECONDS).isSuccess()
            );
            final var configuration = leader.getDivision(group.getGroupId()).getRaftConf();
            assertEquals(
                replacement.stream().map(RaftPeer::getId).collect(Collectors.toSet()),
                configuration.getCurrentPeers().stream().map(RaftPeer::getId).collect(Collectors.toSet())
            );
            assertTrue(configuration.getPreviousPeers().isEmpty());
            assertTrue(configuration.getCurrentPeers(RaftPeerRole.LISTENER).isEmpty());
            try {
                final var stale = setConfiguration(leader, group, initial, List.of(listener), initial, List.of(listener)).get(
                    10,
                    TimeUnit.SECONDS
                );
                assertFalse("stale expected membership must not overwrite the committed replacement", stale.isSuccess());
            } catch (ExecutionException expected) {
                Throwable cause = expected.getCause();
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                assertTrue(cause.toString(), cause instanceof SetConfigurationException);
            }
            final var retired = servers.stream().filter(server -> server.getId().equals(removed.getId())).findFirst().orElseThrow();
            retired.close();
            servers.remove(retired);
            final var newGroup = RaftGroup.valueOf(group.getGroupId(), replacement);
            RatisPersistenceSpikeTests.append(newGroup, 22, 25);
            leader.close();
            servers.remove(leader);
            // Only the caught-up replacement and the remaining original voter can now form a quorum.
            RatisPersistenceSpikeTests.append(newGroup, 26, 26);
        } finally {
            final var pending = gate.get();
            if (pending != null) {
                pending.complete(null);
            }
            RatisPersistenceSpikeTests.closeServers(servers);
        }
    }

    private static CompletableFuture<RaftClientReply> setConfiguration(
        RaftServer leader,
        RaftGroup group,
        List<RaftPeer> oldVoters,
        List<RaftPeer> oldListeners,
        List<RaftPeer> newVoters,
        List<RaftPeer> newListeners
    ) throws IOException {
        final var arguments = SetConfigurationRequest.Arguments.newBuilder()
            .setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
            .setServersInCurrentConf(oldVoters)
            .setListenersInCurrentConf(oldListeners)
            .setServersInNewConf(newVoters)
            .setListenersInNewConf(newListeners)
            .build();
        return leader.setConfigurationAsync(
            new SetConfigurationRequest(ClientId.randomId(), leader.getId(), group.getGroupId(), 0, arguments)
        );
    }

    private static long learnerCommit(RaftServer leader, RaftGroup group, RaftPeer learner) throws Exception {
        return leader.getDivision(group.getGroupId())
            .getCommitInfos()
            .stream()
            .filter(info -> info.getServer().getId().toStringUtf8().equals(learner.getId().toString()))
            .mapToLong(info -> info.getCommitIndex())
            .findFirst()
            .orElse(-1);
    }

    private static RaftServer leader(List<RaftServer> servers, RaftGroup group) throws Exception {
        final var result = new RaftServer[1];
        assertBusy(() -> {
            for (var server : servers) {
                if (server.getDivision(group.getGroupId()).getInfo().isLeaderReady()) {
                    result[0] = server;
                    return;
                }
            }
            fail("no ready native leader");
        }, 30, TimeUnit.SECONDS);
        return result[0];
    }

    private static final class GatedStateMachine extends BaseStateMachine {
        private final AtomicLong applied = new AtomicLong();
        private final AtomicReference<CompletableFuture<Void>> gate;
        private final CountDownLatch entered;

        private GatedStateMachine(AtomicReference<CompletableFuture<Void>> gate, CountDownLatch entered) {
            this.gate = gate;
            this.entered = entered;
        }

        @Override
        public CompletableFuture<Void> flush(long index) {
            final var pending = gate.get();
            if (pending != null) {
                entered.countDown();
                return pending;
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext transaction) {
            final var entry = transaction.getLogEntry();
            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
            return CompletableFuture.completedFuture(Message.valueOf(Long.toString(applied.incrementAndGet())));
        }
    }
}
