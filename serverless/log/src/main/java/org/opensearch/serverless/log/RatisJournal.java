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
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.OpenSearchClientRequestDispatch;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.segmented.OpenSearchLogPersistence;
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
import org.apache.ratis.util.LifeCycle;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Persistent, mutually authenticated shared-group journal. Only committed frames produce append
 * acknowledgements or replay records. This component does not provide current-read authority,
 * document preparation, logical-shard ownership, S3 archival, or automatic disaster recovery.
 * Every physical group must be durably reserved by the controller before initial bootstrap.
 * Once native startup begins, the volume remains exclusively leased until this JVM exits,
 * including after close or failed startup. A replacement voter requires process termination.
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

    /**
     * Historical proof exposed only to the trusted bootstrap controller. It does not establish
     * present read authority or authorize a replacement group.
     * @param deploymentId deployment namespace
     * @param initialPlacement original reserved voters
     * @param checkpoint first committed activation marker
     */
    @InternalApi
    public record GroupActivation(UUID deploymentId, JournalConfiguration.Group initialPlacement, LogCheckpoint checkpoint) {
    }

    private static final byte[] SEED_PAYLOAD = new byte[] { 0 };
    private static final byte[] ACTIVATION_PAYLOAD = new byte[] { 1 };
    private final Map<UUID, JournalStateMachine> stateMachines = new ConcurrentHashMap<>();
    private final JournalConfiguration configuration;
    private final Map<UUID, LogCheckpoint> expectedActivations;
    private final JournalStorage storage;
    private final DiskSpaceProbe diskSpace;
    private final JournalAdmission admission;
    private final Map<UUID, JournalDispatch> dispatch;
    private final ThreadPoolExecutor peerRpcExecutor;
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
        X509TrustManager trustManager,
        Map<UUID, LogCheckpoint> expectedActivations,
        Map<UUID, RaftGroup> initialGroups
    ) throws IOException {
        this.configuration = configuration;
        this.expectedActivations = expectedActivations;
        this.storage = storage;
        this.diskSpace = diskSpace;
        groups = configuration.groups()
            .stream()
            .filter(group -> storage.isActiveGroup(group.id()))
            .collect(Collectors.toUnmodifiableMap(JournalConfiguration.Group::id, group -> group));
        requireDiskSpace();
        this.admission = new JournalAdmission(configuration.limits(), groups.keySet());
        dispatch = groups.keySet()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    group -> group,
                    group -> new JournalDispatch(configuration.localVoterId() + "-" + group, configuration.limits(), 1)
                )
            );
        peerRpcExecutor = JournalExecutors.bounded(
            configuration.localVoterId() + "-journal-peer-rpc",
            4,
            Math.max(128, configuration.limits().maximumPendingRequests())
        );
        final var parameters = new Parameters();
        GrpcConfigKeys.TLS.setConf(parameters, new GrpcTlsConfig(keyManager, trustManager, true));
        GrpcConfigKeys.Server.setServicesCustomizer(
            parameters,
            (builder, types) -> builder.executor(peerRpcExecutor).intercept(new ServerInterceptor() {
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
            })
        );
        storage.retainUntilProcessExit();
        try {
            server = RaftServer.newBuilder()
                .setServerId(peerId(configuration.localVoterId()))
                .setProperties(properties())
                .setParameters(parameters)
                .setStateMachineRegistry(group -> {
                    if (groups.containsKey(group.getUuid()) == false) {
                        throw new IllegalArgumentException("unassigned physical group");
                    }
                    final var machine = new JournalStateMachine(group.getUuid());
                    if (stateMachines.putIfAbsent(group.getUuid(), machine) != null) {
                        throw new IllegalStateException("physical group state machine already exists");
                    }
                    return machine;
                })
                .setOption(RaftStorage.StartupOption.RECOVER)
                .build();
        } catch (IOException | RuntimeException | Error failure) {
            dispatch.values().forEach(JournalDispatch::close);
            peerRpcExecutor.shutdown();
            throw failure;
        }
        try {
            server.start();
            if (configuration.bootstrap()) {
                for (var group : configuration.groups()) {
                    final var reply = await(
                        server.groupManagementAsync(
                            GroupManagementRequest.newAdd(
                                clientId,
                                server.getId(),
                                callIds.getAndIncrement(),
                                initialGroups.getOrDefault(group.id(), raftGroup(group)),
                                false
                            )
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
            // Native close is best effort; the process-lifetime lease prevents overlapping voters.
            try {
                server.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            } finally {
                dispatch.values().forEach(JournalDispatch::close);
                peerRpcExecutor.shutdown();
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
        return open(configuration, keyManager, trustManager, diskSpace, Map.of());
    }

    /**
     * Starts a controller-authorized journal with historical activation requirements. An established
     * group can replay its original marker but cannot initialize a fresh history if replay is missing.
     * @param configuration authorized placement and limits
     * @param keyManager journal member credentials
     * @param trustManager deployment-scoped journal trust
     * @param diskSpace actual-volume disk probe
     * @param expectedActivations original catalog markers for already-activated assigned groups
     * @return running journal, initially unable to append until required activation is replayed
     * @throws IOException on invalid authorization, storage or startup failure
     */
    public static RatisJournal open(
        JournalConfiguration configuration,
        X509KeyManager keyManager,
        X509TrustManager trustManager,
        DiskSpaceProbe diskSpace,
        Map<UUID, LogCheckpoint> expectedActivations
    ) throws IOException {
        if (configuration.joiningPeer() != null) {
            throw new IOException("learner startup requires its catalog-authorized original intent");
        }
        return open(configuration, keyManager, trustManager, diskSpace, expectedActivations, Set.of(), Map.of(), Set.of());
    }

    /**
     * Recovers immutable local assignments with controller-authorized completed membership heads.
     * A retired assignment is preserved outside native group discovery before any SDK thread starts;
     * other groups on the same volume remain active. This does not execute or certify a native
     * membership change: the controller must publish completion only after its consensus proof.
     * @param configuration original volume binding and recovery placement
     * @param keyManager journal member credentials
     * @param trustManager deployment-scoped trust
     * @param diskSpace actual-volume disk probe
     * @param memberships completed heads read from the durable catalog, for activated groups
     * @return recovered journal excluding retired groups
     * @throws IOException on bootstrap, pending/conflicting authorization or storage failure
     */
    public static RatisJournal recoverMembership(
        JournalConfiguration configuration,
        X509KeyManager keyManager,
        X509TrustManager trustManager,
        DiskSpaceProbe diskSpace,
        Map<UUID, JournalMembershipRecord> memberships
    ) throws IOException {
        Objects.requireNonNull(configuration);
        if (configuration.bootstrap() || configuration.joiningPeer() != null) {
            throw new IOException("static membership recovery requires an established genesis voter; learners need their original intent");
        }
        final var retired = new java.util.HashSet<UUID>();
        final var activations = new java.util.HashMap<UUID, LogCheckpoint>();
        for (var entry : Map.copyOf(memberships).entrySet()) {
            final var record = entry.getValue();
            final var group = configuration.groups().stream().filter(candidate -> candidate.id().equals(entry.getKey())).findFirst();
            if (group.isEmpty()
                || record.pending() != null
                || record.genesis().deploymentId().equals(configuration.deploymentId()) == false
                || record.genesis().initialPlacement().id().equals(entry.getKey()) == false) {
                throw new IOException("membership recovery requires assigned groups and completed controller intent");
            }
            final var expected = new JournalGroupRecord(configuration.deploymentId(), group.get(), record.genesis().activation());
            if (record.genesis().equals(expected) == false) {
                throw new IOException("membership recovery differs from the original volume lineage");
            }
            activations.put(entry.getKey(), record.genesis().activation());
            if (record.voters().peers().stream().noneMatch(peer -> peer.id().equals(configuration.localVoterId()))) {
                retired.add(entry.getKey());
            }
        }
        return open(configuration, keyManager, trustManager, diskSpace, activations, retired, Map.of(), Set.of());
    }

    /**
     * Opens controller-authorized assignments, including a new nonvoting learner and pending-aware
     * recovery. The catalog must validate permanent reservations and the current pending winner
     * before calling. Original genesis bindings and activation coordinates remain immutable.
     * @param configuration original local assignment and optional learner transport identity
     * @param keyManager member credentials
     * @param trustManager deployment-scoped trust
     * @param diskSpace actual-volume reserve probe
     * @param memberships current catalog heads for activated assignments
     * @param originalJoins permanent original reservations for non-genesis local assignments
     * @return running assigned groups with retired histories excluded
     * @throws IOException on conflicting authority, storage or startup failure
     */
    public static RatisJournal openAssigned(
        JournalConfiguration configuration,
        X509KeyManager keyManager,
        X509TrustManager trustManager,
        DiskSpaceProbe diskSpace,
        Map<UUID, JournalMembershipRecord> memberships,
        Map<UUID, JournalMembershipRecord> originalJoins
    ) throws IOException {
        final var authorized = JournalStartup.authorize(configuration, memberships, originalJoins);
        return open(
            configuration,
            keyManager,
            trustManager,
            diskSpace,
            authorized.activations(),
            authorized.retired(),
            authorized.initialGroups(),
            authorized.emptyOriginals()
        );
    }

    private static RatisJournal open(
        JournalConfiguration configuration,
        X509KeyManager keyManager,
        X509TrustManager trustManager,
        DiskSpaceProbe diskSpace,
        Map<UUID, LogCheckpoint> expectedActivations,
        Set<UUID> retiredGroups,
        Map<UUID, RaftGroup> initialGroups,
        Set<UUID> emptyOriginals
    ) throws IOException {
        Objects.requireNonNull(configuration);
        Objects.requireNonNull(keyManager);
        Objects.requireNonNull(trustManager);
        Objects.requireNonNull(diskSpace);
        final var activations = Map.copyOf(expectedActivations);
        for (var required : activations.entrySet()) {
            final var assigned = configuration.groups().stream().filter(group -> group.id().equals(required.getKey())).findFirst();
            if (assigned.isEmpty()) {
                throw new IOException("activation requirement names an unassigned physical group");
            }
            try {
                new JournalGroupRecord(configuration.deploymentId(), assigned.get(), required.getValue());
            } catch (IllegalArgumentException failure) {
                throw new IOException("activation requirement differs from authorized group lineage", failure);
            }
        }
        JournalAdmission.validate(configuration.limits(), configuration.groups().size() - retiredGroups.size());
        final var storage = JournalStorage.open(configuration, retiredGroups, emptyOriginals);
        try {
            return new RatisJournal(configuration, storage, diskSpace, keyManager, trustManager, activations, initialGroups);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                if (storage.isProcessOwned() == false) {
                    storage.close();
                }
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
        division(groupId);
        if (operation.logId().equals(activationIdentity(groupId)) || operation.logId().indexUuid().startsWith("__journal_membership/")) {
            throw new IOException("physical group control identity is reserved");
        }
        if (stateMachines.get(groupId).activation == null) {
            throw new IOException("physical group has not completed initial voter activation");
        }
        return appendFrame(groupId, operation);
    }

    /**
     * Activates an initially reserved group after every initial voter has committed a seed
     * entry. Ratis deliberately rejects votes from empty logs after a nonempty commit; admitting
     * traffic before this barrier can strand a two-voter remainder after one failure. The activation
     * record is then committed through the normal majority path and replayed on every member.
     * This barrier runs once; ordinary writes never wait for every voter.
     *
     * @param groupId assigned, control-plane-reserved physical group
     * @return completion after the durable activation record is committed and applied locally
     * @throws IOException on local leadership or admission failure
     */
    public CompletionStage<Void> activateGroup(UUID groupId) throws IOException {
        final var division = division(groupId);
        if (leaderEligible(division) == false) {
            throw new IOException("local journal member is not the ready leader");
        }
        if (stateMachines.get(groupId).activation != null) {
            return CompletableFuture.<Void>completedFuture(null).minimalCompletionStage();
        }
        if (expectedActivations.containsKey(groupId)) {
            throw new IOException("established group must replay its original activation; fresh activation is forbidden");
        }
        return appendFrame(
            groupId,
            new OperationEnvelope(activationIdentity(groupId), OperationEnvelope.Kind.RECOVERY_BARRIER, 0, 0, 0, SEED_PAYLOAD)
        ).thenCompose(checkpoint -> {
            try {
                return watchAllCommitted(groupId, checkpoint.index());
            } catch (IOException failure) {
                return CompletableFuture.<Void>failedFuture(failure);
            }
        }).thenCompose(ignored -> {
            try {
                return appendFrame(
                    groupId,
                    new OperationEnvelope(activationIdentity(groupId), OperationEnvelope.Kind.RECOVERY_BARRIER, 0, 0, 0, ACTIVATION_PAYLOAD)
                ).thenApply(checkpoint -> (Void) null);
            } catch (IOException failure) {
                return CompletableFuture.<Void>failedFuture(failure);
            }
        }).toCompletableFuture().minimalCompletionStage();
    }

    /**
     * Returns the original activation marker after committed local application. The controller
     * must persist it against the same reserved placement before authorizing logical shards.
     * This historical evidence remains valid through leadership changes and quorum loss.
     * @param groupId assigned physical group
     * @return immutable original placement and its committed activation coordinate
     * @throws IOException when closed, fenced, unassigned, or not activated
     */
    public GroupActivation groupActivation(UUID groupId) throws IOException {
        division(groupId);
        final var checkpoint = stateMachines.get(groupId).activation;
        if (checkpoint == null) {
            throw new IOException("physical group has no committed activation marker");
        }
        return new GroupActivation(configuration.deploymentId(), groups.get(groupId), checkpoint);
    }

    /**
     * Advances one controller-authorized replacement without blocking a consensus callback.
     * Repeated calls reconcile the same durable intent. A learner is first added without a vote,
     * then must report commitment through a nonce-bound barrier before the atomic voter change.
     * Call on a bounded controller executor; obtain the intent from the durable catalog each time.
     * Completion means this attempt finished, not necessarily that the transition is complete.
     * @param intent exact durable pending head
     * @return completion of one bounded proposal or observation
     * @throws IOException on conflicting lineage, leadership, admission or storage failure
     */
    public CompletionStage<Void> advanceReplacement(JournalMembershipRecord intent) throws IOException {
        final UUID group = requireReplacementIntent(intent);
        final var division = division(group);
        if (leaderEligible(division) == false) {
            throw new IOException("replacement must run on the ready local leader");
        }
        final var nativeConf = division.getRaftConf();
        if (stable(nativeConf) == false || committedConfiguration(division, nativeConf) == false) {
            return CompletableFuture.<Void>completedFuture(null).minimalCompletionStage();
        }
        if (matches(nativeConf.getCurrentPeers(), intent.pending().target().peers())
            && nativeConf.getCurrentPeers(RaftPeerRole.LISTENER).isEmpty()) {
            replacementReceipt(intent); // Refuse a target with no preceding committed nonce barrier.
            return CompletableFuture.<Void>completedFuture(null).minimalCompletionStage();
        }
        if (matches(nativeConf.getCurrentPeers(), intent.voters().peers()) == false) {
            throw new IOException("native voters differ from both sides of the durable replacement intent");
        }
        final var learner = replacementPeer(intent);
        final var listeners = nativeConf.getCurrentPeers(RaftPeerRole.LISTENER);
        if (listeners.isEmpty()) {
            return setConfiguration(group, intent.voters(), List.of(), intent.voters(), List.of(learner));
        }
        if (matches(listeners, List.of(learner)) == false) {
            throw new IOException("native listeners differ from the durable replacement identity");
        }
        final var barrier = replacementBarrier(group, intent, division.getInfo().getCurrentTerm());
        if (barrier.isDone() == false) {
            return barrier.thenApply(ignored -> (Void) null).minimalCompletionStage();
        }
        final LogCheckpoint cut;
        try {
            cut = barrier.join();
        } catch (java.util.concurrent.CompletionException failure) {
            throw new IOException("replacement barrier failed; reconcile the committed history", failure);
        }
        final long learnerCommit = division.getCommitInfos()
            .stream()
            .filter(info -> info.getServer().getId().toStringUtf8().equals(learner.id().toString()))
            .mapToLong(info -> info.getCommitIndex())
            .findFirst()
            .orElse(-1);
        if (learnerCommit < cut.index()) {
            return CompletableFuture.<Void>completedFuture(null).minimalCompletionStage();
        }
        // Native CAS atomically transitions three voters plus one listener to three voters.
        // There is never a stable four-voter configuration requiring three live failure domains.
        return setConfiguration(group, intent.voters(), List.of(learner), intent.pending().target(), List.of());
    }

    /**
     * Returns historical proof only after the exact target configuration is locally committed and
     * applied. This is neither current read authority nor a guarantee of current learner health.
     * @param intent original durable pending replacement
     * @return receipt, or empty while the target is not committed
     * @throws IOException on lineage mismatch, missing barrier or unavailable history
     */
    public java.util.Optional<JournalReplacementReceipt> replacementReceipt(JournalMembershipRecord intent) throws IOException {
        final UUID group = requireReplacementIntent(intent);
        final var division = division(group);
        final var nativeConf = division.getRaftConf();
        if (stable(nativeConf) == false
            || matches(nativeConf.getCurrentPeers(), intent.pending().target().peers()) == false
            || nativeConf.getCurrentPeers(RaftPeerRole.LISTENER).isEmpty() == false
            || committedConfiguration(division, nativeConf) == false) {
            return java.util.Optional.empty();
        }
        final var barrier = stateMachines.get(group).membershipBarrier;
        if (barrier == null || barrier.revision() != intent.revision() + 1 || barrier.operation().equals(intent.pending().id()) == false) {
            throw new IOException("committed target lacks its original nonce-bound learner barrier");
        }
        final var entry = division.getRaftLog().get(nativeConf.getLogEntryIndex());
        try {
            return java.util.Optional.of(
                new JournalReplacementReceipt(
                    intent,
                    barrier.checkpoint().index(),
                    barrier.checkpoint().entryTerm(),
                    entry.getIndex(),
                    entry.getTerm()
                )
            );
        } catch (IllegalArgumentException failure) {
            throw new IOException("committed target configuration precedes its learner barrier", failure);
        }
    }

    /**
     * Releases a cached transport client only when no local assignment still uses the peer. Current
     * and transitional voters/listeners in every group protect colocated traffic. Call only on a
     * bounded controller executor: SDK channel close must not run under consensus callbacks. This
     * changes transport resources, not membership, durable history, or read authority.
     * @param peer controller-observed retired peer identity
     * @return whether reset was requested; false if still used or this journal is closed
     * @throws IOException if a live assignment cannot be inspected safely
     */
    public boolean releaseUnusedPeer(UUID peer) throws IOException {
        final var id = peerId(Objects.requireNonNull(peer));
        if (closed) {
            return false;
        }
        for (UUID group : groups.keySet()) {
            final var nativeConf = division(group).getRaftConf();
            if (nativeConf.getAllPeers().stream().anyMatch(value -> value.getId().equals(id))
                || nativeConf.getAllPeers(RaftPeerRole.LISTENER).stream().anyMatch(value -> value.getId().equals(id))) {
                return false;
            }
        }
        // Supported SDK reset closes its heavy channels/stub pool, retaining only a lightweight
        // descriptor. A concurrent membership change may reconnect; no log or vote is altered.
        server.getServerRpc().handleException(id, new IOException("peer is unused by every local journal assignment"), true);
        return true;
    }

    private UUID requireReplacementIntent(JournalMembershipRecord intent) throws IOException {
        Objects.requireNonNull(intent);
        final UUID group = intent.genesis().initialPlacement().id();
        final var proof = groupActivation(group);
        if (intent.pending() == null
            || intent.genesis()
                .equals(new JournalGroupRecord(proof.deploymentId(), proof.initialPlacement(), proof.checkpoint())) == false) {
            throw new IOException("replacement intent differs from the activated physical group");
        }
        final var barrier = stateMachines.get(group).membershipBarrier;
        if (barrier != null
            && (barrier.revision() > intent.revision() + 1
                || (barrier.revision() == intent.revision() + 1 && barrier.operation().equals(intent.pending().id()) == false))) {
            throw new IOException("replacement intent is stale or conflicts with an applied controller nonce");
        }
        return group;
    }

    private static JournalConfiguration.Peer replacementPeer(JournalMembershipRecord intent) {
        return intent.pending()
            .target()
            .peers()
            .stream()
            .filter(peer -> intent.voters().peers().contains(peer) == false)
            .findFirst()
            .orElseThrow();
    }

    private static boolean stable(RaftConfiguration configuration) {
        return configuration.getPreviousPeers().isEmpty() && configuration.getPreviousPeers(RaftPeerRole.LISTENER).isEmpty();
    }

    private static boolean committedConfiguration(RaftServer.Division division, RaftConfiguration configuration) throws IOException {
        final long index = configuration.getLogEntryIndex();
        if (index < 0 || index > Math.min(division.getRaftLog().getLastCommittedIndex(), division.getInfo().getLastAppliedIndex())) {
            return false;
        }
        final var entry = division.getRaftLog().get(index);
        if (entry == null || entry.hasConfigurationEntry() == false) {
            throw new IOException("native committed configuration history is unavailable");
        }
        return true;
    }

    private static boolean matches(java.util.Collection<RaftPeer> nativePeers, List<JournalConfiguration.Peer> expected) {
        if (nativePeers.size() != expected.size()) {
            return false;
        }
        return expected.stream().allMatch(peer -> {
            final var nativePeer = JournalStartup.peer(peer, RaftPeerRole.FOLLOWER);
            return nativePeers.stream()
                .anyMatch(
                    actual -> actual.getId().equals(nativePeer.getId()) && Objects.equals(actual.getAddress(), nativePeer.getAddress())
                );
        });
    }

    private CompletionStage<Void> setConfiguration(
        UUID group,
        JournalConfiguration.Group source,
        List<JournalConfiguration.Peer> oldListeners,
        JournalConfiguration.Group target,
        List<JournalConfiguration.Peer> newListeners
    ) throws IOException {
        requireDiskSpace();
        final var arguments = SetConfigurationRequest.Arguments.newBuilder()
            .setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
            .setServersInCurrentConf(List.copyOf(JournalStartup.raftGroup(source).getPeers()))
            .setListenersInCurrentConf(oldListeners.stream().map(peer -> JournalStartup.peer(peer, RaftPeerRole.LISTENER)).toList())
            .setServersInNewConf(List.copyOf(JournalStartup.raftGroup(target).getPeers()))
            .setListenersInNewConf(newListeners.stream().map(peer -> JournalStartup.peer(peer, RaftPeerRole.LISTENER)).toList())
            .build();
        final var request = new SetConfigurationRequest(
            clientId,
            server.getId(),
            RaftGroupId.valueOf(group),
            callIds.getAndIncrement(),
            arguments
        );
        final var permit = admission.acquire(group, 0);
        return dispatch.get(group).submit(permit, () -> server.setConfigurationAsync(request)).thenApply(reply -> {
            if (reply.isSuccess() == false) {
                throw new java.util.concurrent.CompletionException(reply.getException());
            }
            return (Void) null;
        });
    }

    private record MembershipBarrier(long revision, UUID operation, LogCheckpoint checkpoint) {
    }

    private record BarrierAttempt(long revision, UUID operation, long leaderTerm, CompletableFuture<LogCheckpoint> future) {
    }

    private CompletableFuture<LogCheckpoint> replacementBarrier(UUID group, JournalMembershipRecord intent, long term) throws IOException {
        final var machine = stateMachines.get(group);
        final CompletableFuture<LogCheckpoint> future;
        synchronized (machine) {
            final var previous = machine.barrierAttempt;
            if (previous != null
                && previous.revision() == intent.revision() + 1
                && previous.operation().equals(intent.pending().id())
                && previous.leaderTerm() == term
                && previous.future().isCompletedExceptionally() == false) {
                return previous.future();
            }
            future = new CompletableFuture<>();
            machine.barrierAttempt = new BarrierAttempt(intent.revision() + 1, intent.pending().id(), term, future);
        }
        try {
            appendFrame(
                group,
                new OperationEnvelope(
                    membershipIdentity(group, intent.pending().id()),
                    OperationEnvelope.Kind.RECOVERY_BARRIER,
                    intent.revision() + 1,
                    0,
                    0,
                    SEED_PAYLOAD
                )
            ).whenComplete((checkpoint, failure) -> {
                if (failure == null) {
                    future.complete(checkpoint);
                } else {
                    future.completeExceptionally(failure);
                }
            });
        } catch (IOException | RuntimeException failure) {
            future.completeExceptionally(failure);
            throw failure;
        }
        return future;
    }

    private ShardLogId membershipIdentity(UUID group, UUID operation) {
        return new ShardLogId(configuration.deploymentId(), "__journal_membership/" + group, 0, operation);
    }

    private boolean isMembershipBarrier(OperationEnvelope operation, UUID group) throws IOException {
        if (operation.logId().indexUuid().equals("__journal_membership/" + group) == false) {
            return false;
        }
        if (operation.logId().shardNumber() != 0
            || operation.kind() != OperationEnvelope.Kind.RECOVERY_BARRIER
            || operation.producerEpoch() < 1
            || operation.primaryTerm() != 0
            || operation.schemaVersion() != 0
            || operation.payloadLength() != 1
            || Arrays.equals(operation.payloadReader().readAllBytes(), SEED_PAYLOAD) == false) {
            throw new IOException("invalid physical group membership barrier");
        }
        return true;
    }

    private CompletableFuture<Void> watchAllCommitted(UUID groupId, long index) throws IOException {
        final var permit = admission.acquire(groupId, 0);
        try {
            final var request = RaftClientRequest.newBuilder()
                .setClientId(clientId)
                .setServerId(server.getId())
                .setGroupId(RaftGroupId.valueOf(groupId))
                .setCallId(callIds.getAndIncrement())
                .setMessage(Message.valueOf(ByteString.EMPTY))
                .setType(RaftClientRequest.watchRequestType(index, ReplicationLevel.ALL_COMMITTED))
                .build();
            return dispatch.get(groupId).submit(permit, () -> server.submitClientRequestAsync(request)).thenApply(reply -> {
                if (reply.isSuccess() == false) {
                    throw new java.util.concurrent.CompletionException(reply.getException());
                }
                return (Void) null;
            }).toCompletableFuture();
        } catch (RuntimeException | Error | IOException failure) {
            permit.close();
            throw failure;
        }
    }

    private CompletionStage<LogCheckpoint> appendFrame(UUID groupId, OperationEnvelope operation) throws IOException {
        final var division = division(groupId);
        Objects.requireNonNull(operation);
        if (operation.logId().deploymentId().equals(configuration.deploymentId()) == false
            || operation.payloadLength() > configuration.limits().maximumPayloadBytes()) {
            throw new IOException("operation namespace or size is outside this journal's admission");
        }
        if (leaderEligible(division) == false) {
            throw new IOException("local journal member is not the ready leader");
        }
        requireDiskSpace();
        if (isMembershipBarrier(operation, groupId) == false) {
            requireRetainedCapacity(groupId);
        }
        final var permit = admission.acquire(groupId, operation.payloadLength());
        try {
            final byte[] frame = operation.encode(configuration.limits().maximumPayloadBytes());
            final var expectedIdentity = operation.logId();
            final var request = RaftClientRequest.newBuilder()
                .setClientId(clientId)
                .setServerId(server.getId())
                .setGroupId(RaftGroupId.valueOf(groupId))
                .setCallId(callIds.getAndIncrement())
                .setMessage(Message.valueOf(ByteString.copyFrom(frame)))
                .setType(RaftClientRequest.writeRequestType())
                .build();
            return dispatch.get(groupId).submit(permit, () -> server.submitClientRequestAsync(request)).thenApply(reply -> {
                try {
                    if (reply.isSuccess() == false) {
                        throw reply.getException();
                    }
                    final var checkpoint = decodeCheckpoint(reply.getMessage());
                    if (checkpoint.logId().equals(expectedIdentity) == false || checkpoint.index() != reply.getLogIndex()) {
                        throw new IOException("committed reply does not match proposed operation");
                    }
                    return checkpoint;
                } catch (IOException failure) {
                    throw new java.util.concurrent.CompletionException(failure);
                }
            });
        } catch (IOException | RuntimeException | Error failure) {
            permit.close();
            throw failure;
        }
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
                if (isActivation(operation, groupId) == false
                    && isMembershipBarrier(operation, groupId) == false
                    && selection.contains(operation.logId())) {
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
        return leaderEligible(division(groupId));
    }

    private boolean leaderEligible(RaftServer.Division division) {
        return division.getInfo().getLifeCycleState() == LifeCycle.State.RUNNING
            && division.getInfo().isLeaderReady()
            && division.getRaftConf().getPeer(server.getId(), RaftPeerRole.FOLLOWER) != null;
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

    private ShardLogId activationIdentity(UUID group) {
        return JournalGroupRecord.controlIdentity(configuration.deploymentId(), group);
    }

    private boolean isActivation(OperationEnvelope operation, UUID group) throws IOException {
        if (operation.logId().equals(activationIdentity(group)) == false) {
            return false;
        }
        if (operation.kind() != OperationEnvelope.Kind.RECOVERY_BARRIER
            || operation.producerEpoch() != 0
            || operation.primaryTerm() != 0
            || operation.schemaVersion() != 0
            || operation.payloadLength() > ACTIVATION_PAYLOAD.length) {
            throw new IOException("invalid physical group activation record");
        }
        final byte[] payload = operation.payloadReader().readAllBytes();
        if (Arrays.equals(payload, ACTIVATION_PAYLOAD) == false && Arrays.equals(payload, SEED_PAYLOAD) == false) {
            throw new IOException("invalid physical group activation payload");
        }
        return true;
    }

    private final class JournalStateMachine extends BaseStateMachine implements OpenSearchLogPersistence, OpenSearchClientRequestDispatch {
        private final UUID group;
        private volatile LogCheckpoint activation;
        private volatile MembershipBarrier membershipBarrier;
        private BarrierAttempt barrierAttempt;

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
                failed = failure;
                return CompletableFuture.failedFuture(failure);
            }
        }

        @Override
        public void onLogFlushed(long index) throws IOException {
            try {
                storage.recordFlushed(group, index);
            } catch (IOException failure) {
                failed = failure;
                throw failure;
            }
        }

        @Override
        public void beforeLogTruncate(long firstIndex) throws IOException {
            try {
                storage.beforeTruncate(group, firstIndex);
            } catch (IOException failure) {
                failed = failure;
                throw failure;
            }
        }

        @Override
        public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
            requireDiskSpace();
            final var operation = decodeOperation(request.getMessage().getContent());
            if (isMembershipBarrier(operation, group) == false) {
                requireRetainedCapacity(group);
            }
            if (isActivation(operation, group) == false && activation == null) {
                throw new IOException("physical group has not completed initial voter activation");
            }
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
                final boolean control = isActivation(operation, group);
                if (control == false && activation == null) {
                    throw new IOException("logical operation precedes physical group activation");
                }
                final var checkpoint = new LogCheckpoint(operation.logId(), entry.getIndex(), entry.getTerm());
                final var bytes = new ByteArrayOutputStream();
                try (var out = new OutputStreamStreamOutput(bytes)) {
                    checkpoint.writeTo(out);
                }
                final boolean initialActivation = control
                    && activation == null
                    && Arrays.equals(operation.payloadReader().readAllBytes(), ACTIVATION_PAYLOAD);
                if (initialActivation) {
                    final var required = expectedActivations.get(group);
                    if (required != null && required.equals(checkpoint) == false) {
                        throw new IOException("committed activation differs from the durable controller catalog");
                    }
                }
                if (isMembershipBarrier(operation, group)) {
                    final var previous = membershipBarrier;
                    if (previous == null || operation.producerEpoch() > previous.revision()) {
                        membershipBarrier = new MembershipBarrier(operation.producerEpoch(), operation.logId().activationId(), checkpoint);
                    } else if (operation.producerEpoch() == previous.revision()
                        && operation.logId().activationId().equals(previous.operation()) == false) {
                            throw new IOException("conflicting committed replacement nonce for the same controller revision");
                        }
                }
                updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                if (initialActivation) {
                    activation = checkpoint;
                }
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
        final var local = configuration.localPeer();
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
        RaftServerConfigKeys.setStagingCatchupGap(properties, 1);
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
        return JournalStartup.raftGroup(group);
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
        dispatch.values().forEach(JournalDispatch::close);
        if (Thread.currentThread().isInterrupted()) {
            throw new IOException("interrupted journal shutdown; retry cleanup from a non-interrupted thread");
        }
        try {
            server.close();
        } finally {
            peerRpcExecutor.shutdown();
        }
        // SDK shutdown may return while an uninterruptible I/O task is still running. Only OS
        // process teardown proves those tasks cannot touch this volume again.

    }

}
