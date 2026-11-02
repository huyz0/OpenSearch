/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageLite;
import org.apache.ratis.thirdparty.io.grpc.Context;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.ProtoUtils;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Keep native peer callbacks off the shared gRPC decoding pool. One worker per physical group
 * preserves listener ordering and prevents a stalled consensus monitor from occupying other groups'
 * handlers. Active calls and each call's callback/message queue are finite, including idle streams.
 */
final class JournalPeerRpcDispatcher implements ServerInterceptor, AutoCloseable {
    private static final int MINIMUM_CALLS_PER_GROUP = 4;
    private static final int MAXIMUM_CALLS = 128;
    private static final int MAXIMUM_EVENTS_PER_CALL = 8;
    private static final int MAXIMUM_QUEUED_MESSAGES = 2;
    private final Map<UUID, ThreadPoolExecutor> workers;
    private final Map<UUID, Integer> classified = new HashMap<>();
    private final Set<PeerCall<?>> active = new HashSet<>();
    private final int maximumCalls;
    private final int maximumMessageBytes;
    private final ScheduledThreadPoolExecutor firstMessageTimer;
    private final Executor decoding;
    private int classifiedCalls;
    private int coveredReservations;
    private boolean closed;

    JournalPeerRpcDispatcher(String name, JournalConfiguration.Limits limits, Set<UUID> groups, Executor decoding) {
        this.decoding = Objects.requireNonNull(decoding);
        if (groups.size() > 16) {
            throw new IllegalArgumentException("too many physical peer groups");
        }
        maximumCalls = maximumCalls(limits, groups.size());
        maximumMessageBytes = limits.queueBytes() + 1024 * 1024;
        final var executors = new HashMap<UUID, ThreadPoolExecutor>();
        groups.forEach(group -> {
            classified.put(group, 0);
            // At most one queued runner per admitted call, plus a runner finishing its current event.
            executors.put(group, JournalExecutors.bounded(name + "-peer-group-" + group, 1, maximumCalls + 1));
        });
        workers = Map.copyOf(executors);
        final var threadGroup = Thread.currentThread().getThreadGroup();
        firstMessageTimer = new ScheduledThreadPoolExecutor(
            1,
            runnable -> Daemon.newBuilder().setName(name + "-peer-first-message").setThreadGroup(threadGroup).setRunnable(runnable).build()
        );
        firstMessageTimer.setRemoveOnCancelPolicy(true);
        firstMessageTimer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    static int maximumCalls(JournalConfiguration.Limits limits, int groups) {
        if (groups == 0) {
            return 0;
        }
        final long allowancePerCall = 4L * (limits.queueBytes() + 1024L * 1024);
        return Math.max(MINIMUM_CALLS_PER_GROUP * groups, (int) Math.min(MAXIMUM_CALLS, limits.maximumPendingBytes() / allowancePerCall));
    }

    static long retainedMessageAllowance(JournalConfiguration.Limits limits, int groups) {
        return maximumCalls(limits, groups) * 4L * (limits.queueBytes() + 1024L * 1024);
    }

    synchronized int activeCalls() {
        return active.size();
    }

    int pendingFirstMessages() {
        return firstMessageTimer.getQueue().size();
    }

    @Override
    public <RequestT, ReplyT> ServerCall.Listener<RequestT> interceptCall(
        ServerCall<RequestT, ReplyT> call,
        Metadata headers,
        ServerCallHandler<RequestT, ReplyT> next
    ) {
        final var peer = new PeerCall<RequestT>(call, Context.current());
        final boolean accepted;
        synchronized (this) {
            accepted = closed == false && active.size() < maximumCalls;
            if (accepted) {
                active.add(peer);
            }
        }
        if (accepted == false) {
            call.close(Status.RESOURCE_EXHAUSTED.withDescription("journal peer call limit"), new Metadata());
            return new ServerCall.Listener<>() {
            };
        }
        try {
            peer.delegate = next.startCall(call, headers);
            peer.firstMessage = firstMessageTimer.schedule(
                () -> peer.fail(Status.DEADLINE_EXCEEDED.withDescription("journal peer did not provide a group")),
                5,
                TimeUnit.SECONDS
            );
            return peer;
        } catch (RuntimeException | Error failure) {
            peer.fail(Status.UNAVAILABLE.withCause(failure).withDescription("journal peer call initialization failed"));
            throw failure;
        }
    }

    private synchronized boolean bind(PeerCall<?> call, UUID group) {
        if (closed || active.contains(call) == false || classified.containsKey(group) == false) {
            return false;
        }
        if (call.group != null) {
            return call.group.equals(group);
        }
        final int current = classified.get(group);
        final int addedCoverage = current < MINIMUM_CALLS_PER_GROUP ? 1 : 0;
        final int protectedCalls = classified.size() * MINIMUM_CALLS_PER_GROUP - coveredReservations - addedCoverage;
        if (classifiedCalls >= maximumCalls - protectedCalls) {
            return false;
        }
        classified.put(group, current + 1);
        classifiedCalls++;
        coveredReservations += addedCoverage;
        call.group = group;
        return true;
    }

    private synchronized void release(PeerCall<?> call) {
        if (active.remove(call) && call.group != null) {
            final int current = classified.get(call.group);
            classified.put(call.group, current - 1);
            classifiedCalls--;
            if (current <= MINIMUM_CALLS_PER_GROUP) {
                coveredReservations--;
            }
        }
    }

    @Override
    public void close() {
        final Set<PeerCall<?>> calls;
        synchronized (this) {
            closed = true;
            calls = Set.copyOf(active);
        }
        firstMessageTimer.shutdown();
        calls.forEach(call -> call.fail(Status.UNAVAILABLE.withDescription("journal peer dispatcher closed")));
        workers.values().forEach(ThreadPoolExecutor::shutdown);
    }

    private static RaftRpcRequestProto request(Object message) {
        if (message instanceof RequestVoteRequestProto request) {
            return request.getServerRequest();
        }
        if (message instanceof StartLeaderElectionRequestProto request) {
            return request.getServerRequest();
        }
        if (message instanceof ReadIndexRequestProto request) {
            return request.getServerRequest();
        }
        if (message instanceof AppendEntriesRequestProto request) {
            return request.getServerRequest();
        }
        if (message instanceof InstallSnapshotRequestProto request) {
            return request.getServerRequest();
        }
        throw new IllegalArgumentException("unsupported journal peer request");
    }

    private record Event(Runnable callback, boolean message, boolean ready, boolean terminal) {
    }

    private final class PeerCall<RequestT> extends ServerCall.Listener<RequestT> {
        private final ServerCall<?, ?> call;
        private final Context context;
        private final ArrayDeque<Event> events = new ArrayDeque<>();
        private ServerCall.Listener<RequestT> delegate;
        private volatile UUID group;
        private volatile ScheduledFuture<?> firstMessage;
        private boolean scheduled;
        private boolean running;
        private boolean ending;
        private boolean readyQueued;
        private int messages;

        private PeerCall(ServerCall<?, ?> call, Context context) {
            this.call = call;
            this.context = context;
        }

        @Override
        public void onMessage(RequestT message) {
            try {
                if (message instanceof MessageLite proto == false || ((MessageLite) message).getSerializedSize() > maximumMessageBytes) {
                    fail(Status.RESOURCE_EXHAUSTED.withDescription("journal peer message limit"));
                    return;
                }
                // With snapshot transfer disabled, Ratis still uses a notification to start
                // an empty learner's full-log catch-up. It carries no snapshot bytes. Never
                // allow chunk installation to bypass the journal's verified history.
                if (message instanceof InstallSnapshotRequestProto snapshot && snapshot.hasNotification() == false) {
                    fail(Status.PERMISSION_DENIED.withDescription("journal snapshot installation requires archive recovery proof"));
                    return;
                }
                final var id = request(message).getRaftGroupId();
                if (id.getId().size() != 16 || bind(this, ProtoUtils.toRaftGroupId(id).getUuid()) == false) {
                    fail(Status.RESOURCE_EXHAUSTED.withDescription("journal peer group admission"));
                    return;
                }
                firstMessage.cancel(false);
                enqueue(new Event(() -> delegate.onMessage(message), true, false, false));
            } catch (RuntimeException failed) {
                fail(Status.INVALID_ARGUMENT.withCause(failed).withDescription("invalid journal peer request"));
            }
        }

        @Override
        public void onHalfClose() {
            enqueue(new Event(() -> delegate.onHalfClose(), false, false, false));
        }

        @Override
        public void onReady() {
            enqueue(new Event(() -> delegate.onReady(), false, true, false));
        }

        @Override
        public void onCancel() {
            enqueue(new Event(() -> delegate.onCancel(), false, false, true));
        }

        @Override
        public void onComplete() {
            enqueue(new Event(() -> delegate.onComplete(), false, false, true));
        }

        private void enqueue(Event event) {
            final boolean start;
            final boolean overflow;
            synchronized (this) {
                if (ending || (event.ready() && readyQueued)) {
                    return;
                }
                overflow = events.size() >= MAXIMUM_EVENTS_PER_CALL || (event.message() && messages >= MAXIMUM_QUEUED_MESSAGES);
                if (overflow) {
                    start = false;
                } else {
                    events.add(event);
                    if (event.message()) {
                        messages++;
                    }
                    readyQueued |= event.ready();
                    ending = event.terminal();
                    start = scheduled == false;
                    scheduled = true;
                }
            }
            if (overflow) {
                fail(Status.RESOURCE_EXHAUSTED.withDescription("journal peer callback limit"));
            } else if (start) {
                schedule();
            }
        }

        private void schedule() {
            try {
                // Before a body, callbacks can only perform non-division initialization/error cleanup.
                final var executor = group == null ? decoding : workers.get(group);
                executor.execute(this::runOne);
            } catch (RejectedExecutionException closed) {
                call.close(Status.UNAVAILABLE.withDescription("journal peer worker unavailable"), new Metadata());
                retire();
            }
        }

        private void runOne() {
            final Event event;
            synchronized (this) {
                event = events.poll();
                running = event != null;
                if (event != null) {
                    if (event.message()) {
                        messages--;
                    }
                    if (event.ready()) {
                        readyQueued = false;
                    }
                }
            }
            try {
                if (event != null) {
                    context.run(event.callback());
                }
            } catch (RuntimeException | Error failure) {
                fail(Status.INTERNAL.withCause(failure).withDescription("journal peer callback failed"));
            } finally {
                final boolean more;
                synchronized (this) {
                    running = false;
                    more = events.isEmpty() == false;
                    if (more == false) {
                        scheduled = false;
                    }
                }
                if (event != null && event.terminal()) {
                    retire();
                } else if (more) {
                    schedule();
                }
            }
        }

        private void fail(Status status) {
            final boolean start;
            synchronized (this) {
                if (ending) {
                    return;
                }
                ending = true;
                events.clear();
                messages = 0;
                readyQueued = false;
                events.add(new Event(() -> {
                    final var current = delegate;
                    if (current != null) {
                        current.onCancel();
                    }
                }, false, false, true));
                start = scheduled == false;
                scheduled |= start;
            }
            call.close(status, new Metadata());
            if (start) {
                schedule();
            }
        }

        private void retire() {
            synchronized (this) {
                if (running) {
                    return;
                }
                ending = true;
                events.clear();
                messages = 0;
                readyQueued = false;
                delegate = null;
            }
            final var timer = firstMessage;
            if (timer != null) {
                timer.cancel(false);
            }
            release(this);
        }
    }
}
