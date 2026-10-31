/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.SizeInBytes;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;

import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Persistent, mutually authenticated shared-group journal. Only committed frames produce append
 * acknowledgements or replay records. This component does not provide current-read authority,
 * document preparation, logical-shard ownership, S3 archival, or automatic disaster recovery.
 * Every physical group must be durably reserved by the controller before initial bootstrap.
 */
@InternalApi
public final class RatisJournal implements Closeable {
    static {
        // This embedded library must not exit the host JVM or replace OpenSearch's fatal handler.
        // Ratis initializes its own default handler when ExitUtils.States is first loaded.
        final var previousHandler = Thread.getDefaultUncaughtExceptionHandler();
        try {
            ExitUtils.disableSystemExit();
            ExitUtils.setTerminateOnUncaughtException(false);
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(previousHandler);
        }
    }

    /** Host-supplied disk probe; implementations must measure the actual provisioned volume. */
    @FunctionalInterface
    public interface DiskSpaceProbe {
        /**
         * Measures available disk space without following a missing-volume fallback.
         * @return usable bytes on the journal volume
         * @throws IOException when space or volume identity cannot be established
         */
        long usableBytes() throws IOException;
    }

    /**
     * A committed logical operation and its physical-log coordinate.
     * @param operation immutable operation
     * @param checkpoint committed coordinate, not a searchable-reader guarantee
     */
    @InternalApi
    public record CommittedOperation(OperationEnvelope operation, LogCheckpoint checkpoint) {
    }

    /**
     * A bounded replay result. Through-index includes skipped configuration and unselected logical
     * streams; it is valid only for this physical group and the exact subscription selection used.
     * @param operations selected committed operations
     * @param throughIndex last physical position inspected, including skipped entries
     * @param committedIndex local applied-and-committed prefix observed at scan start
     */
    @InternalApi
    public record ScanBatch(List<CommittedOperation> operations, long throughIndex, long committedIndex) {
        /**
         * Copies the selected list to prevent mutable replay results.
         * @param operations selected operations
         * @param throughIndex scan progress
         * @param committedIndex local committed prefix
         */
        public ScanBatch {
            operations = List.copyOf(operations);
        }
    }

    private final JournalConfiguration configuration;
    private final JournalStorage storage;
    private final DiskSpaceProbe diskSpace;
    private final JournalAdmission admission;
    private final Map<UUID, JournalConfiguration.Group> groups;
    private final ClientId clientId = ClientId.randomId();
    private final AtomicLong callIds = new AtomicLong();
    private final RaftServer server;
    private volatile boolean closed;
    private volatile IOException failed;

    private RatisJournal(
        JournalConfiguration configuration,
        JournalStorage storage,
        DiskSpaceProbe diskSpace,
        X509KeyManager keyManager,
        X509TrustManager trustManager
    ) throws IOException {
        this.configuration = configuration;
        this.storage = storage;
        this.diskSpace = diskSpace;
        this.admission = new JournalAdmission(configuration.limits());
        groups = configuration.groups().stream().collect(Collectors.toUnmodifiableMap(JournalConfiguration.Group::id, group -> group));
        requireDiskSpace();
        final var parameters = new Parameters();
        GrpcConfigKeys.TLS.setConf(parameters, new GrpcTlsConfig(keyManager, trustManager, true));
        GrpcConfigKeys.Server.setServicesCustomizer(parameters, (builder, types) -> builder.intercept(new ServerInterceptor() {
            @Override
            public <RequestT, ReplyT> ServerCall.Listener<RequestT> interceptCall(
                ServerCall<RequestT, ReplyT> call,
                Metadata headers,
                ServerCallHandler<RequestT, ReplyT> next
            ) {
                if (RaftServerProtocolServiceGrpc.SERVICE_NAME.equals(call.getMethodDescriptor().getServiceName())) {
                    return next.startCall(call, headers);
                }
                call.close(
                    Status.PERMISSION_DENIED.withDescription("journal network endpoint accepts peer consensus RPCs only"),
                    new Metadata()
                );
                return new ServerCall.Listener<>() {
                };
            }
        }));
        server = RaftServer.newBuilder()
            .setServerId(peerId(configuration.localVoterId()))
            .setProperties(properties())
            .setParameters(parameters)
            .setStateMachineRegistry(group -> {
                if (groups.containsKey(group.getUuid()) == false) {
                    throw new IllegalArgumentException("unassigned physical group");
                }
                return new JournalStateMachine(group.getUuid());
            })
            .setOption(RaftStorage.StartupOption.RECOVER)
            .build();
        try {
            server.start();
            if (configuration.bootstrap()) {
                for (var group : configuration.groups()) {
                    final var reply = await(
                        server.groupManagementAsync(
                            GroupManagementRequest.newAdd(clientId, server.getId(), callIds.getAndIncrement(), raftGroup(group), false)
                        )
                    );
                    if (reply.isSuccess() == false) {
                        throw new IOException("physical group initialization failed", reply.getException());
                    }
                }
            }
            for (UUID group : groups.keySet()) {
                server.getDivision(RaftGroupId.valueOf(group));
            }
        } catch (IOException | RuntimeException | Error failure) {
            // A failed close must retain the volume lease: another process must not start a voter
            // while replication services might still be running.
            try {
                server.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
                throw new UnclosedJournalException(failure);
            }
            if (failure instanceof RuntimeException) {
                throw new IOException("journal startup failed", failure);
            }
            throw failure;
        }
    }

    /**
     * Starts assigned groups under an exclusive storage lease. Mutual TLS is mandatory for all
     * replication and admin RPCs; the trust manager must be scoped to this deployment's journal
     * members. The controller remains responsible for certificate and placement authorization.
     *
     * @param configuration control-plane-authorized placement and limits
     * @param keyManager journal member credentials
     * @param trustManager deployment-scoped journal trust
     * @param diskSpace actual-volume disk probe
     * @return running journal
     * @throws IOException on identity mismatch, incomplete recovery, failed persistence or startup
     */
    public static RatisJournal open(
        JournalConfiguration configuration,
        X509KeyManager keyManager,
        X509TrustManager trustManager,
        DiskSpaceProbe diskSpace
    ) throws IOException {
        Objects.requireNonNull(configuration);
        Objects.requireNonNull(keyManager);
        Objects.requireNonNull(trustManager);
        Objects.requireNonNull(diskSpace);
        final var storage = JournalStorage.open(configuration);
        try {
            return new RatisJournal(configuration, storage, diskSpace, keyManager, trustManager);
        } catch (UnclosedJournalException failure) {
            throw new IOException("journal startup failed and replication services did not close; volume lease retained", failure);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                storage.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /**
     * Proposes to the local leader only. Prepared operations are never redirected to another leader.
     * Cancellation of a derived future does not cancel accepted persistence or release its budget.
     * Any failure after submission is ambiguous; callers must reconcile their operation identity.
     *
     * @param groupId assigned physical group
     * @param operation resolved immutable operation
     * @return committed coordinate after the persistent majority acknowledgement
     * @throws IOException on synchronous validation, leader or admission failure
     */
    public CompletionStage<LogCheckpoint> append(UUID groupId, OperationEnvelope operation) throws IOException {
        final var division = division(groupId);
        Objects.requireNonNull(operation);
        if (operation.logId().deploymentId().equals(configuration.deploymentId()) == false
            || operation.payloadLength() > configuration.limits().maximumPayloadBytes()) {
            throw new IOException("operation namespace or size is outside this journal's admission");
        }
        if (division.getInfo().isLeaderReady() == false) {
            throw new IOException("local journal member is not the ready leader");
        }
        requireDiskSpace();
        requireRetainedCapacity(groupId);
        final var permit = admission.acquire(operation.payloadLength());
        final var completion = new CompletableFuture<LogCheckpoint>();
        try {
            final byte[] frame = operation.encode(configuration.limits().maximumPayloadBytes());
            final var request = RaftClientRequest.newBuilder()
                .setClientId(clientId)
                .setServerId(server.getId())
                .setGroupId(RaftGroupId.valueOf(groupId))
                .setCallId(callIds.getAndIncrement())
                .setMessage(Message.valueOf(ByteString.copyFrom(frame)))
                .setType(RaftClientRequest.writeRequestType())
                .build();
            server.submitClientRequestAsync(request).whenComplete((reply, failure) -> {
                try {
                    if (failure != null) {
                        completion.completeExceptionally(failure);
                    } else if (reply.isSuccess() == false) {
                        completion.completeExceptionally(reply.getException());
                    } else {
                        final var checkpoint = decodeCheckpoint(reply.getMessage());
                        if (checkpoint.logId().equals(operation.logId()) == false || checkpoint.index() != reply.getLogIndex()) {
                            throw new IOException("committed reply does not match proposed operation");
                        }
                        completion.complete(checkpoint);
                    }
                } catch (Exception e) {
                    completion.completeExceptionally(e);
                } finally {
                    permit.close();
                }
            });
        } catch (IOException | RuntimeException | Error failure) {
            permit.close();
            throw failure;
        }
        return completion.minimalCompletionStage();
    }

    /**
     * Scans only a bounded locally applied committed prefix. This is replay, not a linearizable
     * latest read. The caller must obtain quorum read authority separately for current reads.
     * Missing history fails instead of reporting an empty successful catch-up.
     *
     * @param groupId assigned physical group
     * @param afterIndex exclusive physical cursor, initially -1
     * @param selectedStreams authorized logical streams, bounded to 4096 identities
     * @param maximumEntries maximum physical positions inspected, at most 4096
     * @param maximumBytes maximum inspected encoded operation bytes, at most the configured queue budget
     * @return selected operations and progress through skipped physical entries
     * @throws IOException on missing history, invalid namespace, or invalid cursor
     */
    public ScanBatch scan(UUID groupId, long afterIndex, Set<ShardLogId> selectedStreams, int maximumEntries, int maximumBytes)
        throws IOException {
        if (afterIndex < -1
            || maximumEntries < 1
            || maximumEntries > 4096
            || maximumBytes < 1
            || maximumBytes > configuration.limits().queueBytes()
            || selectedStreams.size() > 4096) {
            throw new IllegalArgumentException("invalid journal scan bounds");
        }
        final Set<ShardLogId> selection = Set.copyOf(selectedStreams);
        if (selection.stream().anyMatch(id -> id.deploymentId().equals(configuration.deploymentId()) == false)) {
            throw new IOException("subscription namespace differs from journal deployment");
        }
        final var division = division(groupId);
        final var log = division.getRaftLog();
        final long committed = Math.min(log.getLastCommittedIndex(), division.getInfo().getLastAppliedIndex());
        if (afterIndex > committed || afterIndex < log.getStartIndex() - 1) {
            throw new IOException("journal cursor is ahead of the committed prefix or requires snapshot recovery");
        }
        final List<CommittedOperation> operations = new ArrayList<>();
        long through = afterIndex;
        int bytes = 0;
        for (int inspected = 0; inspected < maximumEntries && through < committed; inspected++) {
            final var entry = log.get(through + 1);
            if (entry == null) {
                throw new IOException("committed journal history is missing; snapshot recovery required");
            }
            if (entry.hasStateMachineLogEntry()) {
                final var data = entry.getStateMachineLogEntry().getLogData();
                if (data.size() > maximumBytes - bytes) {
                    if (bytes == 0) {
                        throw new IOException("scan byte budget cannot hold the next operation");
                    }
                    break;
                }
                bytes += data.size();
                final var operation = decodeOperation(data);
                if (selection.contains(operation.logId())) {
                    operations.add(
                        new CommittedOperation(operation, new LogCheckpoint(operation.logId(), entry.getIndex(), entry.getTerm()))
                    );
                }
            }
            through = entry.getIndex();
        }
        return new ScanBatch(operations, through, committed);
    }

    /**
     * Reports local leader readiness as a routing hint. This is not quorum read authority.
     * @param groupId assigned physical group
     * @return whether the local member currently reports a ready leader role
     * @throws IOException when closed or the group is unavailable
     */
    public boolean isLeaderReady(UUID groupId) throws IOException {
        return division(groupId).getInfo().isLeaderReady();
    }

    private RaftServer.Division division(UUID groupId) throws IOException {
        if (closed || failed != null || groups.containsKey(groupId) == false) {
            throw new IOException("journal is closed or physical group is unassigned");
        }
        return server.getDivision(RaftGroupId.valueOf(groupId));
    }

    private void requireDiskSpace() throws IOException {
        if (failed != null) {
            throw new IOException("journal is fenced after a committed apply failure", failed);
        }
        if (diskSpace.usableBytes() < configuration.limits().minimumFreeBytes()) {
            throw new IOException("journal disk reserve reached; archive or add capacity before accepting writes");
        }
    }

    private void requireRetainedCapacity(UUID group) throws IOException {
        final var log = server.getDivision(RaftGroupId.valueOf(group)).getRaftLog();
        final long retained = log.getNextIndex() - Math.max(0, log.getStartIndex());
        if (retained >= configuration.limits().maximumRetainedEntries()) {
            throw new IOException("journal retained-entry watermark reached; archive and reclaim history before accepting writes");
        }
    }

    private OperationEnvelope decodeOperation(ByteString bytes) throws IOException {
        if (bytes.size() > configuration.limits().maximumPayloadBytes() + 512L) {
            throw new IOException("operation frame exceeds configured admission limit");
        }
        final var operation = OperationEnvelope.decode(bytes.toByteArray(), configuration.limits().maximumPayloadBytes());
        if (operation.logId().deploymentId().equals(configuration.deploymentId()) == false) {
            throw new IOException("operation namespace differs from journal deployment");
        }
        return operation;
    }

    private final class JournalStateMachine extends BaseStateMachine {
        private final UUID group;

        private JournalStateMachine(UUID group) {
            this.group = group;
        }

        @Override
        public void initialize(RaftServer raftServer, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
            super.initialize(raftServer, groupId, raftStorage);
            storage.initialize(group);
        }

        @Override
        public CompletableFuture<Void> flush(long index) {
            try {
                requireDiskSpace();
                storage.flush(group);
                return CompletableFuture.completedFuture(null);
            } catch (IOException failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }

        @Override
        public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
            requireDiskSpace();
            requireRetainedCapacity(group);
            decodeOperation(request.getMessage().getContent());
            return super.startTransaction(request);
        }

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext transaction) {
            try {
                if (failed != null) {
                    throw new IOException("journal is fenced after a committed apply failure", failed);
                }
                final var entry = transaction.getLogEntry();
                final var operation = decodeOperation(entry.getStateMachineLogEntry().getLogData());
                final var checkpoint = new LogCheckpoint(operation.logId(), entry.getIndex(), entry.getTerm());
                final var bytes = new ByteArrayOutputStream();
                try (var out = new OutputStreamStreamOutput(bytes)) {
                    checkpoint.writeTo(out);
                }
                updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(bytes.toByteArray())));
            } catch (IOException failure) {
                failed = failure;
                return CompletableFuture.failedFuture(failure);
            }
        }
    }

    private RaftProperties properties() {
        final var properties = new RaftProperties();
        final var limits = configuration.limits();
        final var local = configuration.groups()
            .getFirst()
            .peers()
            .stream()
            .filter(peer -> peer.id().equals(configuration.localVoterId()))
            .findFirst()
            .orElseThrow();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        GrpcConfigKeys.Server.setHost(properties, local.host());
        GrpcConfigKeys.Server.setPort(properties, local.port());
        GrpcConfigKeys.setMessageSizeMax(properties, SizeInBytes.valueOf(limits.queueBytes() + 1024L * 1024));
        GrpcConfigKeys.Server.setAsyncRequestThreadPoolCached(properties, false);
        GrpcConfigKeys.Server.setAsyncRequestThreadPoolSize(properties, 4);
        GrpcConfigKeys.Server.setLeaderOutstandingAppendsMax(properties, 8);
        GrpcConfigKeys.Server.setBossGroupSize(properties, 1);
        GrpcConfigKeys.Server.setWorkerGroupSize(properties, 2);
        configureStorage(properties, storage.directory());
        RaftServerConfigKeys.setStorageFreeSpaceMin(properties, SizeInBytes.valueOf(limits.minimumFreeBytes()));
        RaftServerConfigKeys.ThreadPool.setProxyCached(properties, false);
        RaftServerConfigKeys.ThreadPool.setProxySize(properties, 4);
        RaftServerConfigKeys.ThreadPool.setServerCached(properties, false);
        RaftServerConfigKeys.ThreadPool.setServerSize(properties, 4);
        RaftServerConfigKeys.ThreadPool.setClientCached(properties, false);
        RaftServerConfigKeys.ThreadPool.setClientSize(properties, 4);
        RaftServerConfigKeys.Log.setUseMemory(properties, false);
        RaftServerConfigKeys.Log.setUnsafeFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.setCorruptionPolicy(properties, RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION);
        RaftServerConfigKeys.Log.setQueueElementLimit(properties, limits.maximumPendingRequests());
        RaftServerConfigKeys.Log.setQueueByteLimit(properties, limits.queueBytes());
        RaftServerConfigKeys.Write.setElementLimit(properties, limits.maximumPendingRequests());
        RaftServerConfigKeys.Write.setByteLimit(properties, SizeInBytes.valueOf(limits.queueBytes()));
        RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(limits.segmentBytes()));
        RaftServerConfigKeys.Log.setSegmentCacheNumMax(properties, 1);
        RaftServerConfigKeys.Log.setSegmentCacheSizeMax(properties, SizeInBytes.valueOf(limits.cachedSegmentBytes()));
        RaftServerConfigKeys.Log.setPreallocatedSize(properties, SizeInBytes.valueOf(1024 * 1024));
        RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf(limits.queueBytes() + 8L));
        RaftServerConfigKeys.Log.StateMachineData.setSync(properties, true);
        RaftServerConfigKeys.Log.StateMachineData.setCachingEnabled(properties, false);
        RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, limits.maximumPendingRequests());
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(limits.queueBytes()));
        RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false);
        RaftServerConfigKeys.Snapshot.setTriggerWhenStopEnabled(properties, false);
        RaftServerConfigKeys.Snapshot.setTriggerWhenRemoveEnabled(properties, false);
        return properties;
    }

    @SuppressForbidden(reason = "Ratis storage configuration requires java.io.File")
    private static void configureStorage(RaftProperties properties, Path directory) {
        RaftServerConfigKeys.setStorageDir(properties, List.of(directory.toFile()));
    }

    private static RaftPeerId peerId(UUID id) {
        return RaftPeerId.valueOf(id.toString());
    }

    private static RaftGroup raftGroup(JournalConfiguration.Group group) {
        final List<RaftPeer> peers = group.peers()
            .stream()
            .map(
                peer -> RaftPeer.newBuilder()
                    .setId(peerId(peer.id()))
                    .setAddress((peer.host().contains(":") ? "[" + peer.host() + "]" : peer.host()) + ":" + peer.port())
                    .build()
            )
            .toList();
        return RaftGroup.valueOf(RaftGroupId.valueOf(group.id()), peers);
    }

    private static LogCheckpoint decodeCheckpoint(Message message) throws IOException {
        if (message == null || message.getContent().size() > 512) {
            throw new IOException("invalid committed checkpoint reply");
        }
        try (var input = new InputStreamStreamInput(new ByteArrayInputStream(message.getContent().toByteArray()))) {
            final var checkpoint = LogCheckpoint.readFrom(input);
            if (input.read() != -1) {
                throw new IOException("trailing committed checkpoint bytes");
            }
            return checkpoint;
        }
    }

    private static <T> T await(CompletableFuture<T> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while starting journal groups", e);
        } catch (ExecutionException e) {
            throw new IOException("failed to start journal groups", e.getCause());
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        admission.close();
        server.close();
        storage.close();
    }

    private static final class UnclosedJournalException extends IOException {
        private UnclosedJournalException(Throwable cause) {
            super(cause);
        }
    }
}
