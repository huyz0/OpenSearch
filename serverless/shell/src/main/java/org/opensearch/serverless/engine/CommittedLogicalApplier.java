/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.LogicalCommand;
import org.opensearch.serverless.log.OperationEnvelope;
import org.opensearch.serverless.log.RatisJournal;
import org.opensearch.serverless.log.ShardLogId;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Native application of newly accepted logical effects into an empty committed-serving primary.
 * Input must come from verified logical replay, not raw physical scans or client checkpoints. The
 * trusted host must serialize application/lifecycle and pin an already installed exact native
 * schema/analysis context. This adapter supports that single schema and engine terms no greater
 * than the serving shard's installed term; schema changes and native term/rebuild transitions need
 * their lifecycle controller. It provides no latest-reader proof or original outcome-only codec.
 */
@InternalApi
public final class CommittedLogicalApplier {
    private final IndexShard serving;
    private final CommittedServingEngine engine;
    private final ShardLogId stream;
    private final long installedSchemaVersion;
    private final long nativeMappingVersion;
    private final long nativePrimaryTerm;
    private final DocumentMapper nativeMapper;
    private final byte[] definitionDigest;
    private final int maximumPayloadBytes;
    private UUID owner;
    private long epoch;
    private long term;
    private long schema;
    private long lastSequence = -1;
    private long lastIndex = -1;
    private LogCheckpoint lastApplied;
    private boolean retired;
    private Throwable failure;

    /**
     * Binds an admitted empty serving shard to its trusted installed schema definition.
     * @param serving public primary using the committed-serving engine
     * @param engine exact engine owning this shard's store
     * @param stream deployment/index/shard/activation identity
     * @param installedSchemaVersion logical version of the pinned native schema
     * @param exactDefinition exact already installed schema/analysis definition, not an unverified client assertion
     * @param maximumPayloadBytes complete logical payload admission bound
     */
    public CommittedLogicalApplier(
        IndexShard serving,
        CommittedServingEngine engine,
        ShardLogId stream,
        long installedSchemaVersion,
        byte[] exactDefinition,
        int maximumPayloadBytes
    ) {
        this.serving = Objects.requireNonNull(serving);
        this.engine = Objects.requireNonNull(engine);
        this.stream = Objects.requireNonNull(stream);
        if (stream.indexUuid().equals(serving.shardId().getIndex().getUUID()) == false
            || stream.shardNumber() != serving.shardId().id()
            || installedSchemaVersion < 1
            || Objects.requireNonNull(exactDefinition).length == 0
            || maximumPayloadBytes < 91
            || maximumPayloadBytes > OperationEnvelope.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("invalid logical serving binding or payload limit");
        }
        this.installedSchemaVersion = installedSchemaVersion;
        nativeMappingVersion = serving.indexSettings().getIndexMetadata().getMappingVersion();
        nativePrimaryTerm = serving.getOperationPrimaryTerm();
        nativeMapper = Objects.requireNonNull(serving.mapperService().documentMapper());
        this.maximumPayloadBytes = maximumPayloadBytes;
        definitionDigest = digest(exactDefinition);
        requireNativeBinding();
        if (engine.getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo() != SequenceNumbers.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException("nonempty serving state requires a verified recovery base");
        }
    }

    /**
     * Applies one newly accepted ordered effect. A failure permanently stops this instance; native
     * recovery/application must rebuild from verified state. Outcome-only/control records consume
     * no document sequence position. The caller owns any returned result payload's byte budget.
     * @param accepted newly accepted logical replay effect
     * @return original resolved mutation and physical coordinate, or empty for controls/result-only records
     * @throws IOException on changed binding, malformed identity or native application failure
     */
    public Optional<CommittedMutationReplayer.AppliedMutation> apply(RatisJournal.CommittedOperation accepted) throws IOException {
        if (failure != null) {
            throw new IOException("logical native application is stopped after an earlier failure", failure);
        }
        try {
            requireNativeBinding();
            final var envelope = Objects.requireNonNull(accepted).operation();
            final var checkpoint = accepted.checkpoint();
            if (retired
                || envelope.kind() != OperationEnvelope.Kind.LOGICAL
                || envelope.logId().equals(stream) == false
                || checkpoint.logId().equals(stream) == false
                || checkpoint.index() <= lastIndex
                || envelope.payloadLength() > maximumPayloadBytes
                || envelope.primaryTerm() > nativePrimaryTerm) {
                throw new IOException("logical effect differs from ordered native serving binding");
            }
            final var command = LogicalCommand.decode(envelope.payloadReader().readAllBytes(), maximumPayloadBytes);
            final Optional<CommittedMutationReplayer.AppliedMutation> result;
            if (command.action() == LogicalCommand.Action.OWNERSHIP) {
                if (epoch == Long.MAX_VALUE
                    || command.argument() != epoch
                    || envelope.producerEpoch() != epoch + 1
                    || envelope.primaryTerm() <= term
                    || envelope.schemaVersion() != schema) {
                    throw new IOException("logical ownership transition is inconsistent with native replay");
                }
                owner = command.actor();
                epoch = envelope.producerEpoch();
                term = envelope.primaryTerm();
                result = Optional.empty();
            } else {
                if (owner == null
                    || owner.equals(command.actor()) == false
                    || envelope.producerEpoch() != epoch
                    || envelope.primaryTerm() != term) {
                    throw new IOException("logical effect differs from installed producer");
                }
                if (command.action() == LogicalCommand.Action.SCHEMA) {
                    if (schema == Long.MAX_VALUE
                        || command.argument() != schema
                        || envelope.schemaVersion() != schema + 1
                        || envelope.schemaVersion() != installedSchemaVersion
                        || Arrays.equals(definitionDigest, digest(command)) == false) {
                        throw new IOException("logical schema requires another verified native schema controller");
                    }
                    schema = envelope.schemaVersion();
                    result = Optional.empty();
                } else {
                    if (envelope.schemaVersion() != schema) {
                        throw new IOException("logical effect has a different schema dependency");
                    }
                    if (command.action() == LogicalCommand.Action.RETIRE) {
                        retired = true;
                        result = Optional.empty();
                    } else {
                        if (schema != installedSchemaVersion) {
                            throw new IOException("logical document/result requires the installed schema barrier");
                        }
                        result = command.action() == LogicalCommand.Action.MUTATION
                            ? Optional.of(applyMutation(command, checkpoint))
                            : Optional.empty();
                    }
                }
            }
            lastApplied = checkpoint;
            lastIndex = checkpoint.index();
            return result;
        } catch (IOException | RuntimeException | Error stopped) {
            failure = stopped;
            if (stopped instanceof Error fatal) {
                throw fatal;
            }
            throw stopped instanceof IOException io ? io : new IOException("logical native application stopped", stopped);
        }
    }

    private CommittedMutationReplayer.AppliedMutation applyMutation(LogicalCommand command, LogCheckpoint checkpoint) throws IOException {
        final var mutation = ResolvedMutation.decode(command.payloadReader().readAllBytes(), maximumPayloadBytes);
        final var operation = mutation.operation();
        if (mutation.operationId().equals(command.requestId()) == false
            || operation.primaryTerm() != term
            || command.argument() != operation.seqNo()
            || lastSequence == Long.MAX_VALUE
            || operation.seqNo() != lastSequence + 1) {
            throw new IOException("resolved operation differs from logical request, sequence or term");
        }
        final var result = serving.applyTranslogOperation(operation, Engine.Operation.Origin.PRIMARY);
        if (result.getResultType() != Engine.Result.Type.SUCCESS
            || result.getSeqNo() != operation.seqNo()
            || result.getTerm() != operation.primaryTerm()
            || (operation instanceof Translog.Index index && result.getVersion() != index.version())
            || (operation instanceof Translog.Delete delete && result.getVersion() != delete.version())) {
            throw new IOException("logical native application failed or changed resolved identity", result.getFailure());
        }
        lastSequence = operation.seqNo();
        return new CommittedMutationReplayer.AppliedMutation(mutation, checkpoint);
    }

    /** Returns the last successfully applied selected control/data position, not a refreshed-reader proof. */
    public Optional<LogCheckpoint> lastAppliedCheckpoint() {
        return Optional.ofNullable(lastApplied);
    }

    /** Returns the last resolved document sequence position applied successfully. */
    public long lastSequenceNumber() {
        return lastSequence;
    }

    private void requireNativeBinding() {
        engine.ensureReplayOpen();
        if (serving.routingEntry().primary() == false
            || serving.store() != engine.config().getStore()
            || serving.getOperationPrimaryTerm() != nativePrimaryTerm
            || serving.indexSettings().getIndexMetadata().getMappingVersion() != nativeMappingVersion
            || serving.mapperService().documentMapper() != nativeMapper) {
            throw new IllegalStateException("native serving term, mapping or store binding changed");
        }
    }

    private static byte[] digest(LogicalCommand definition) throws IOException {
        final var digest = sha256();
        try (var in = definition.payloadReader()) {
            final byte[] buffer = new byte[4096];
            int count;
            while ((count = in.read(buffer)) != -1) {
                digest.update(buffer, 0, count);
            }
        }
        return digest.digest();
    }

    private static byte[] digest(byte[] definition) {
        return sha256().digest(definition);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
