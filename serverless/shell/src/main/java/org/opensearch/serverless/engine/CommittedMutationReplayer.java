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
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.OperationEnvelope;
import org.opensearch.serverless.log.RatisJournal;
import org.opensearch.serverless.log.ShardLogId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Bounded replay of one fixed ownership/schema binding directly from the local committed journal.
 * Starts at the beginning of the retained history and never accepts a caller-provided commit claim.
 * This is not latest-read authority, routing, a schema-transition controller or retry deduplication.
 * The trusted host must install the binding and prevent concurrent shard mutations/lifecycle changes.
 */
@InternalApi
public final class CommittedMutationReplayer {
    /**
     * An applied resolved record. Its original result may differ from the native replay result.
     * @param mutation original operation identity and preparation outcome
     * @param checkpoint journal position applied to the serving engine, not yet a refresh proof
     */
    @InternalApi
    public record AppliedMutation(ResolvedMutation mutation, LogCheckpoint checkpoint) {
    }

    private final RatisJournal journal;
    private final UUID group;
    private final IndexShard serving;
    private final CommittedServingEngine servingEngine;
    private final ShardLogId stream;
    private final long producerEpoch;
    private final long primaryTerm;
    private final long mappingVersion;
    private final int maximumEntries;
    private final int maximumScanBytes;
    private final int maximumPayloadBytes;
    private long throughIndex = -1;
    private long lastSequenceNumber = -1;
    private LogCheckpoint lastApplied;
    private Throwable failure;

    /**
     * Binds replay to an already installed schema and a trusted fixed ownership/engine term.
     * @param journal authorized local journal; scans establish the applied committed prefix
     * @param group physical group containing the stream
     * @param serving public primary shard using {@link CommittedServingEngine}
     * @param servingEngine engine created by the shard factory; must own this shard's exact store
     * @param stream exact deployment, index incarnation, shard and activation identity
     * @param producerEpoch installed ownership epoch
     * @param primaryTerm installed engine term; original operations retain this term
     * @param mappingVersion installed native mapping version
     * @param maximumEntries physical positions per poll, including unselected streams, at most 4096
     * @param maximumScanBytes encoded bytes inspected per poll, within journal queue limits
     * @param maximumPayloadBytes resolved payload admission limit
     */
    public CommittedMutationReplayer(
        RatisJournal journal,
        UUID group,
        IndexShard serving,
        CommittedServingEngine servingEngine,
        ShardLogId stream,
        long producerEpoch,
        long primaryTerm,
        long mappingVersion,
        int maximumEntries,
        int maximumScanBytes,
        int maximumPayloadBytes
    ) {
        this.journal = Objects.requireNonNull(journal);
        this.group = Objects.requireNonNull(group);
        this.serving = Objects.requireNonNull(serving);
        this.servingEngine = Objects.requireNonNull(servingEngine);
        this.stream = Objects.requireNonNull(stream);
        if (stream.indexUuid().equals(serving.shardId().getIndex().getUUID()) == false || stream.shardNumber() != serving.shardId().id()) {
            throw new IllegalArgumentException("committed replay stream differs from serving shard incarnation");
        }
        if (serving.routingEntry().primary() == false || serving.store() != servingEngine.config().getStore()) {
            throw new IllegalArgumentException("committed replay requires a committed-serving primary shard");
        }
        if (producerEpoch < 0
            || primaryTerm <= 0
            || mappingVersion < 0
            || maximumEntries < 1
            || maximumEntries > 4096
            || maximumScanBytes < 1
            || maximumPayloadBytes < 1
            || maximumPayloadBytes > OperationEnvelope.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("invalid committed replay binding or bounds");
        }
        this.producerEpoch = producerEpoch;
        this.primaryTerm = primaryTerm;
        this.mappingVersion = mappingVersion;
        this.maximumEntries = maximumEntries;
        this.maximumScanBytes = maximumScanBytes;
        this.maximumPayloadBytes = maximumPayloadBytes;
        requireInstalledBinding();
    }

    /**
     * Applies at most one bounded scan, advancing progress only after successful native application.
     * A failure stops this instance permanently; recovery must replay into a verified serving state.
     * This initial adapter requires consecutive resolved sequence positions, including explicit no-ops.
     * Internal retry records and ownership/schema transitions require their later controllers.
     * @return immutable results for selected operations applied during this poll
     * @throws IOException on missing history, a changed binding, malformed records or application failure
     */
    public synchronized List<AppliedMutation> poll() throws IOException {
        if (failure != null) {
            throw new IOException("committed replay is stopped after an earlier failure", failure);
        }
        try {
            requireInstalledBinding();
            final var batch = journal.scan(group, throughIndex, Set.of(stream), maximumEntries, maximumScanBytes);
            final List<AppliedMutation> applied = new ArrayList<>();
            for (var committed : batch.operations()) {
                final var envelope = committed.operation();
                if (envelope.producerEpoch() != producerEpoch
                    || envelope.primaryTerm() != primaryTerm
                    || envelope.schemaVersion() != mappingVersion) {
                    throw new IOException("committed operation differs from installed ownership, engine term or schema");
                }
                if (envelope.kind() != OperationEnvelope.Kind.DOCUMENTS && envelope.kind() != OperationEnvelope.Kind.NO_OP) {
                    throw new IOException("committed replay requires a controller for this record kind");
                }
                if (envelope.payloadLength() > maximumPayloadBytes) {
                    throw new IOException("committed mutation exceeds configured payload limit");
                }
                final var mutation = ResolvedMutation.decode(envelope.payloadReader().readAllBytes(), maximumPayloadBytes);
                final var operation = mutation.operation();
                if (operation.primaryTerm() != primaryTerm
                    || operation.seqNo() != lastSequenceNumber + 1
                    || (operation instanceof Translog.NoOp) != (envelope.kind() == OperationEnvelope.Kind.NO_OP)) {
                    throw new IOException("committed mutation has a mismatched term, kind or nonconsecutive sequence number");
                }
                final var result = serving.applyTranslogOperation(operation, Engine.Operation.Origin.PRIMARY);
                if (result.getResultType() != Engine.Result.Type.SUCCESS
                    || result.getSeqNo() != operation.seqNo()
                    || result.getTerm() != operation.primaryTerm()
                    || (operation instanceof Translog.Index index && result.getVersion() != index.version())
                    || (operation instanceof Translog.Delete delete && result.getVersion() != delete.version())) {
                    throw new IOException("committed native application failed or changed operation identity", result.getFailure());
                }
                lastSequenceNumber = operation.seqNo();
                lastApplied = committed.checkpoint();
                throughIndex = committed.checkpoint().index();
                applied.add(new AppliedMutation(mutation, lastApplied));
            }
            throughIndex = batch.throughIndex();
            return List.copyOf(applied);
        } catch (IOException | RuntimeException | Error stopped) {
            failure = stopped;
            if (stopped instanceof Error fatal) {
                throw fatal;
            }
            throw stopped instanceof IOException io ? io : new IOException("committed native application stopped", stopped);
        }
    }

    /** Returns bounded physical scan progress, including skipped streams; not a document sequence number. */
    public synchronized long throughIndex() {
        return throughIndex;
    }

    /** Returns the last selected record applied successfully; it does not attest reader refresh. */
    public synchronized Optional<LogCheckpoint> lastAppliedCheckpoint() {
        return Optional.ofNullable(lastApplied);
    }

    private void requireInstalledBinding() {
        servingEngine.ensureReplayOpen();
        if (serving.routingEntry().primary() == false
            || serving.store() != servingEngine.config().getStore()
            || serving.getOperationPrimaryTerm() != primaryTerm
            || serving.indexSettings().getIndexMetadata().getMappingVersion() != mappingVersion) {
            throw new IllegalStateException("serving engine term or installed mapping version changed");
        }
    }
}
