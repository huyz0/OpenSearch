/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.io.grpc.Context;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.util.ProtoUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class JournalPeerRpcDispatcherTests extends OpenSearchTestCase {
    private static final JournalConfiguration.Limits LIMITS = new JournalConfiguration.Limits(
        2,
        16384,
        32,
        1024 * 1024,
        1024 * 1024,
        2 * 1024 * 1024,
        0,
        10_000,
        1
    );

    public void testStalledGroupDoesNotBlockAnotherAndCallbackContextOrderArePreserved() throws Exception {
        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var finished = new CountDownLatch(1);
        final var events = new ArrayList<String>();
        final Context.Key<String> key = Context.key("journal-peer-test");
        final var decoding = JournalExecutors.bounded("test-peer-decoding", 1, 128);
        try (var dispatcher = new JournalPeerRpcDispatcher("test", LIMITS, Set.of(first, second), decoding)) {
            final var blocked = dispatcher.interceptCall(new Call(), new Metadata(), (call, headers) -> new ServerCall.Listener<>() {
                @Override
                public void onMessage(RequestVoteRequestProto message) {
                    entered.countDown();
                    await(release);
                }
            });
            blocked.onMessage(vote(first));
            blocked.onHalfClose();
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            final var healthy = Context.current()
                .withValue(key, "preserved")
                .call(() -> dispatcher.interceptCall(new Call(), new Metadata(), (call, headers) -> new ServerCall.Listener<>() {
                    private void record(String event) {
                        assertFalse(Thread.holdsLock(dispatcher));
                        assertEquals("preserved", key.get());
                        assertTrue(Thread.currentThread().getName().contains("peer-group-" + second));
                        events.add(event);
                    }

                    @Override
                    public void onMessage(RequestVoteRequestProto message) {
                        record("message");
                    }

                    @Override
                    public void onHalfClose() {
                        record("half-close");
                    }

                    @Override
                    public void onComplete() {
                        record("complete");
                        finished.countDown();
                    }
                }));
            healthy.onMessage(vote(second));
            healthy.onHalfClose();
            healthy.onComplete();
            assertTrue(finished.await(10, TimeUnit.SECONDS));
            assertEquals(List.of("message", "half-close", "complete"), events);
            release.countDown();
            blocked.onComplete();
            assertBusy(() -> assertEquals(0, dispatcher.activeCalls()));
            assertEquals(0, dispatcher.pendingFirstMessages());
        } finally {
            release.countDown();
            decoding.shutdown();
            assertTrue(decoding.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testOneGroupCannotUseTheOtherGroupsReservedCalls() throws Exception {
        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final var decoding = JournalExecutors.bounded("test-peer-decoding", 1, 128);
        try (var dispatcher = new JournalPeerRpcDispatcher("test", LIMITS, Set.of(first, second), decoding)) {
            final var listeners = new ArrayList<ServerCall.Listener<RequestVoteRequestProto>>();
            final var messages = new AtomicInteger();
            for (int i = 0; i < 4; i++) {
                final var listener = dispatcher.interceptCall(new Call(), new Metadata(), (call, headers) -> counting(messages));
                listener.onMessage(vote(first));
                listeners.add(listener);
            }
            assertBusy(() -> assertEquals(4, messages.get()));
            final var exhausted = new Call();
            dispatcher.interceptCall(exhausted, new Metadata(), (call, headers) -> counting(messages)).onMessage(vote(first));
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, exhausted.closed.get(10, TimeUnit.SECONDS).getCode());
            assertBusy(() -> assertEquals(4, dispatcher.activeCalls()));
            for (int i = 0; i < 4; i++) {
                final var listener = dispatcher.interceptCall(new Call(), new Metadata(), (call, headers) -> counting(messages));
                listener.onMessage(vote(second));
                listeners.add(listener);
            }
            assertBusy(() -> assertEquals(8, messages.get()));
            assertEquals(8, dispatcher.activeCalls());
            final var nodeFull = new Call();
            dispatcher.interceptCall(nodeFull, new Metadata(), (call, headers) -> {
                throw new AssertionError("refused calls must not initialize a native handler");
            });
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, nodeFull.closed.get(10, TimeUnit.SECONDS).getCode());
            listeners.forEach(ServerCall.Listener::onComplete);
            assertBusy(() -> assertEquals(0, dispatcher.activeCalls()));
        } finally {
            decoding.shutdown();
            assertTrue(decoding.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testCallbackOverflowCannotReleaseAStillRunningNativeHandler() throws Exception {
        final UUID group = UUID.randomUUID();
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var messages = new AtomicInteger();
        final var cancelled = new CountDownLatch(1);
        final var decoding = JournalExecutors.bounded("test-peer-decoding", 1, 128);
        try (var dispatcher = new JournalPeerRpcDispatcher("test", LIMITS, Set.of(group), decoding)) {
            final var call = new Call();
            final var listener = dispatcher.interceptCall(call, new Metadata(), (ignored, headers) -> new ServerCall.Listener<>() {
                @Override
                public void onMessage(RequestVoteRequestProto message) {
                    messages.incrementAndGet();
                    entered.countDown();
                    await(release);
                }

                @Override
                public void onCancel() {
                    cancelled.countDown();
                }
            });
            listener.onMessage(vote(group));
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            listener.onMessage(vote(group));
            listener.onMessage(vote(group));
            listener.onMessage(vote(group));
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, call.closed.get(10, TimeUnit.SECONDS).getCode());
            assertEquals(1, dispatcher.activeCalls());
            release.countDown();
            assertTrue(cancelled.await(10, TimeUnit.SECONDS));
            assertBusy(() -> assertEquals(0, dispatcher.activeCalls()));
            assertEquals(1, messages.get());
        } finally {
            release.countDown();
            decoding.shutdown();
            assertTrue(decoding.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testIdleUnclassifiedStreamExpiresAndTimerQueueDoesNotAccumulate() throws Exception {
        final UUID group = UUID.randomUUID();
        final var decoding = JournalExecutors.bounded("test-peer-decoding", 1, 128);
        try (var dispatcher = new JournalPeerRpcDispatcher("test", LIMITS, Set.of(group), decoding)) {
            final var call = new Call();
            dispatcher.interceptCall(call, new Metadata(), (ignored, headers) -> new ServerCall.Listener<>() {
            });
            assertEquals(Status.Code.DEADLINE_EXCEEDED, call.closed.get(10, TimeUnit.SECONDS).getCode());
            assertBusy(() -> assertEquals(0, dispatcher.activeCalls()));
            assertEquals(0, dispatcher.pendingFirstMessages());
            for (int i = 0; i < 20; i++) {
                final var listener = dispatcher.interceptCall(
                    new Call(),
                    new Metadata(),
                    (ignored, headers) -> new ServerCall.Listener<>() {
                    }
                );
                listener.onMessage(vote(group));
                listener.onComplete();
                assertBusy(() -> assertEquals(0, dispatcher.activeCalls()));
            }
            assertEquals(0, dispatcher.pendingFirstMessages());
        } finally {
            decoding.shutdown();
            assertTrue(decoding.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testUnassignedGroupAndGroupSwitchNeverReachTheNativeHandler() throws Exception {
        final UUID group = UUID.randomUUID();
        final var messages = new AtomicInteger();
        final var decoding = JournalExecutors.bounded("test-peer-decoding", 1, 128);
        try (var dispatcher = new JournalPeerRpcDispatcher("test", LIMITS, Set.of(group), decoding)) {
            final var unknown = new Call();
            dispatcher.interceptCall(unknown, new Metadata(), (call, headers) -> counting(messages)).onMessage(vote(UUID.randomUUID()));
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, unknown.closed.get(10, TimeUnit.SECONDS).getCode());
            assertBusy(() -> assertEquals(0, dispatcher.activeCalls()));
            assertEquals(0, messages.get());
            final var switched = new Call();
            final var listener = dispatcher.interceptCall(switched, new Metadata(), (call, headers) -> counting(messages));
            listener.onMessage(vote(group));
            assertBusy(() -> assertEquals(1, messages.get()));
            listener.onMessage(vote(UUID.randomUUID()));
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, switched.closed.get(10, TimeUnit.SECONDS).getCode());
            assertBusy(() -> assertEquals(0, dispatcher.activeCalls()));
            assertEquals(1, messages.get());
        } finally {
            decoding.shutdown();
            assertTrue(decoding.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static ServerCall.Listener<RequestVoteRequestProto> counting(AtomicInteger messages) {
        return new ServerCall.Listener<>() {
            @Override
            public void onMessage(RequestVoteRequestProto message) {
                messages.incrementAndGet();
            }
        };
    }

    public void testSnapshotDataIsDeniedButCatchupNotificationReachesNativeHandler() throws Exception {
        final var decoding = JournalExecutors.bounded("test-peer-decoding", 1, 128);
        try {
            final var closed = new CompletableFuture<Status>();
            final var call = new ServerCall<InstallSnapshotRequestProto, InstallSnapshotReplyProto>() {
                @Override
                public void request(int count) {}

                @Override
                public void sendHeaders(Metadata headers) {}

                @Override
                public void sendMessage(InstallSnapshotReplyProto reply) {}

                @Override
                public void close(Status status, Metadata trailers) {
                    closed.complete(status);
                }

                @Override
                public boolean isCancelled() {
                    return closed.isDone();
                }

                @Override
                public MethodDescriptor<InstallSnapshotRequestProto, InstallSnapshotReplyProto> getMethodDescriptor() {
                    return RaftServerProtocolServiceGrpc.getInstallSnapshotMethod();
                }
            };
            final UUID group = UUID.randomUUID();
            try (var assigned = new JournalPeerRpcDispatcher("snapshot-test", LIMITS, Set.of(group), decoding)) {
                final var messages = new AtomicInteger();
                final var listener = assigned.interceptCall(call, new Metadata(), (ignored, headers) -> new ServerCall.Listener<>() {
                    @Override
                    public void onMessage(InstallSnapshotRequestProto request) {
                        assertTrue(request.hasNotification());
                        messages.incrementAndGet();
                    }
                });
                listener.onMessage(
                    InstallSnapshotRequestProto.newBuilder()
                        .setServerRequest(
                            RaftRpcRequestProto.newBuilder()
                                .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(RaftGroupId.valueOf(group)))
                        )
                        .setNotification(InstallSnapshotRequestProto.NotificationProto.newBuilder())
                        .build()
                );
                assertBusy(() -> assertEquals(1, messages.get()));
                listener.onMessage(
                    InstallSnapshotRequestProto.newBuilder()
                        .setServerRequest(
                            RaftRpcRequestProto.newBuilder()
                                .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(RaftGroupId.valueOf(group)))
                        )
                        .setSnapshotChunk(InstallSnapshotRequestProto.SnapshotChunkProto.newBuilder())
                        .build()
                );
                assertEquals(Status.Code.PERMISSION_DENIED, closed.get(10, TimeUnit.SECONDS).getCode());
                assertBusy(() -> assertEquals(0, assigned.activeCalls()));
                assertEquals(1, messages.get());
            }
        } finally {
            decoding.shutdown();
            assertTrue(decoding.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static RequestVoteRequestProto vote(UUID group) {
        return RequestVoteRequestProto.newBuilder()
            .setServerRequest(
                RaftRpcRequestProto.newBuilder().setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(RaftGroupId.valueOf(group)))
            )
            .build();
    }

    private static void await(CountDownLatch release) {
        try {
            if (release.await(10, TimeUnit.SECONDS) == false) {
                throw new AssertionError("test did not release native callback");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static final class Call extends ServerCall<RequestVoteRequestProto, RequestVoteReplyProto> {
        private final CompletableFuture<Status> closed = new CompletableFuture<>();

        @Override
        public void request(int count) {}

        @Override
        public void sendHeaders(Metadata headers) {}

        @Override
        public void sendMessage(RequestVoteReplyProto message) {}

        @Override
        public void close(Status status, Metadata trailers) {
            closed.complete(status);
        }

        @Override
        public boolean isCancelled() {
            return closed.isDone();
        }

        @Override
        public MethodDescriptor<RequestVoteRequestProto, RequestVoteReplyProto> getMethodDescriptor() {
            return RaftServerProtocolServiceGrpc.getRequestVoteMethod();
        }
    }
}
