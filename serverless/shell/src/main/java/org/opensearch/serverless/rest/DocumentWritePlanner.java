/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.WriteTarget;
import org.opensearch.serverless.rest.DocumentWriteOutcomes.ForwardFailed;
import org.opensearch.serverless.rest.DocumentWriteOutcomes.WriteRefusal;
import org.opensearch.serverless.rest.DocumentWriteOutcomes.WriteResult;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.shell.WriteOutcome;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Plans a single-document write for {@link DocumentHandler}: runs its pipeline, picks the owner, and applies or forwards
 * it, activating the shard first when nobody holds it.
 */
final class DocumentWritePlanner {

    /** The handler's own logger, so moving the planning here changes no logger name. */
    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(DocumentHandler.class);

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    DocumentWritePlanner(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    void planAcceptedWrite(
        MetadataPlane metadata,
        ServerlessNode serving,
        boolean deletion,
        org.opensearch.action.ActionRequest admitted,
        org.opensearch.core.action.ActionListener<WriteResult> listener
    ) throws Exception {

        if (deletion) {
            final var selected = (org.opensearch.action.delete.DeleteRequest) admitted;
            final var invalid = selected.validate();
            if (invalid != null) {
                throw invalid;
            }
            DocumentWriteOutcomes.validateWriteOptions(
                selected.routing(),
                selected.version(),
                selected.versionType(),
                selected.waitForActiveShards(),
                false
            );
            planWrite(
                metadata,
                serving,
                selected.index(),
                selected.id(),
                null,
                null,
                true,
                false,
                selected.getRefreshPolicy() != org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE,
                selected.ifSeqNo(),
                selected.ifPrimaryTerm(),
                listener
            );
        } else {
            final var selected = (org.opensearch.action.index.IndexRequest) admitted;
            final var invalid = selected.validate();
            if (invalid != null) {
                throw invalid;
            }
            final boolean create = selected.opType() == org.opensearch.action.DocWriteRequest.OpType.CREATE;
            DocumentWriteOutcomes.validateWriteOptions(
                selected.routing(),
                selected.version(),
                selected.versionType(),
                selected.waitForActiveShards(),
                create
            );
            if (selected.isRequireAlias() || selected.getFinalPipeline() != null && "_none".equals(selected.getFinalPipeline()) == false) {
                throw new WriteRefusal(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "require_alias and an explicit final_pipeline are not supported"
                );
            }
            final String chosenId = selected.id() == null ? org.opensearch.common.UUIDs.base64UUID() : selected.id();
            selected.id(chosenId);
            final String chosenSource = org.opensearch.common.xcontent.XContentHelper.convertToJson(
                selected.source(),
                false,
                selected.getContentType()
            );
            planWrite(
                metadata,
                serving,
                selected.index(),
                chosenId,
                chosenSource,
                selected.getPipeline(),
                false,
                create,
                selected.getRefreshPolicy() != org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE,
                selected.ifSeqNo(),
                selected.ifPrimaryTerm(),
                listener
            );
        }
    }

    void planWrite(
        MetadataPlane metadata,
        ServerlessNode serving,
        String index,
        String id,
        String source,
        String pipeline,
        boolean deletion,
        boolean requireAbsent,
        boolean refresh,
        long ifSeqNo,
        long ifPrimaryTerm,
        org.opensearch.core.action.ActionListener<WriteResult> answer
    ) throws IOException {
        final Optional<WriteTarget> target = metadata.writeTarget(index);
        if (target.isEmpty()) {
            throw new WriteRefusal(RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index);
        }
        if (target.get().dataStream() != null && (deletion || requireAbsent == false)) {
            throw new WriteRefusal(
                RestStatus.BAD_REQUEST,
                "illegal_argument_exception",
                "only write ops with an op_type of create are allowed in data streams; address the backing index ["
                    + target.get().index().name()
                    + "] for anything else"
            );
        }
        final Optional<IndexDescriptor> descriptor = Optional.of(target.get().index());
        final String writtenIndex = descriptor.get().name();
        if (serving.isSystemIndex(writtenIndex)) {
            throw new WriteRefusal(
                RestStatus.FORBIDDEN,
                "system_index",
                "[" + writtenIndex + "] belongs to a plugin and is not reachable through the request path"
            );
        }
        final java.util.List<String> pipelines = deletion
            ? java.util.List.of()
            : org.opensearch.serverless.ingest.IngestPipelines.pipelinesFor(pipeline, descriptor.get().extraSettings());
        String shaped = source;
        for (String ran : pipelines) {
            try {
                final var stored = metadata.pipelines().get(ran);
                if (stored.isEmpty()) {
                    throw new WriteRefusal(RestStatus.BAD_REQUEST, "pipeline_missing", "no such pipeline: " + ran);
                }
                final var outcome = serving.services()
                    .ingestPipelines()
                    .run(serving.services().ingestPipelines().compile(ran, stored.get()), writtenIndex, id, shaped);
                if (outcome.dropped()) {
                    answer.onResponse(
                        WriteResult.dropped(
                            new ShardId(writtenIndex, descriptor.get().uuid(), DocumentRouting.shardFor(descriptor.get(), id)),
                            id,
                            ran,
                            requireAbsent
                        )
                    );
                    return;
                }
                shaped = outcome.source();
            } catch (WriteRefusal e) {
                throw e;
            } catch (Exception e) {
                throw new WriteRefusal(RestStatus.BAD_REQUEST, "pipeline_failed", e.getMessage() == null ? e.toString() : e.getMessage());
            }
        }
        final String shapedSource = shaped;
        final int shard = DocumentRouting.shardFor(descriptor.get(), id);
        final long inFlightBytes = org.opensearch.serverless.store.WalRecord.contentBytes(id, deletion ? null : shapedSource);
        final java.util.function.BiConsumer<org.opensearch.core.action.ActionListener<WriteResult>, ShardId> writeHere = (
            listener,
            into) -> {
            try (
                org.opensearch.common.lease.Releasable inFlight = serving.services()
                    .indexingPressure()
                    .markCoordinatingOperationStarted(inFlightBytes, false)
            ) {
                final WriteOutcome outcome = deletion
                    ? serving.writes().delete(into, id, ifSeqNo, ifPrimaryTerm)
                    : serving.writes().index(into, id, shapedSource, ifSeqNo, ifPrimaryTerm, requireAbsent);
                boolean refreshed = false;
                if (refresh) {
                    final var open = serving.reconciler().shard(into);
                    if (open != null) {
                        open.refresh("serverless-rest-refresh");
                        refreshed = true;
                    }
                }
                listener.onResponse(WriteResult.written(into, id, serving.localNode().getId(), deletion, outcome, refreshed));
            } catch (Exception e) {
                if (org.opensearch.ExceptionsHelper.unwrap(e, org.opensearch.serverless.shell.StaleIncarnationException.class) != null) {
                    metadata.forgetRouting(index);
                }
                listener.onFailure(e);
            }
        };
        final var writer = serving.reconciler().writerShard(new ShardId(writtenIndex, descriptor.get().uuid(), shard));
        final ShardId shardId = writer == null ? null : writer.shardId();

        if (shardId == null) {
            // The owner as last seen, if this node has seen it: a head read per write was the largest
            // fixed cost of a write this node does not serve itself. A hint that is wrong is refused by
            // the node it names, and the register is read then -- once -- and the write forwarded again.
            final Optional<String> hinted = serving.sightings().ownerHint(writtenIndex, shard);
            final String owner;
            if (hinted.isPresent()) {
                owner = hinted.get();
            } else {
                final var head = metadata.heads().read(writtenIndex, shard);
                serving.sightings().noteHead(writtenIndex, shard, head.orElse(null));
                owner = head.map(h -> h.ownerNodeId()).orElse(null);
            }
            if (owner != null && owner.equals(serving.localNode().getId())) {
                // The head names this node and this node has no open shard. That is not a routing
                // problem, it is the gap inside activation: activateWriter wins the compare-and-swap
                // before it opens the shard, so for a moment the head is true and the node is not ready.
                //
                // Forwarding here would send the write to ourselves, and the receiving side would
                // correctly refuse it -- a 500 for what is a normal, brief, self-resolving state. Say
                // "not yet" instead, which is a status a client retries. The doubt makes the activation
                // pass run now rather than on the next write, which is what turns "retry" into a short wait.
                //
                // Better than either: wait for the activation in flight, which is the one this node's own
                // single flight is running, and write once it is open.
                serving.sightings().forgetOwner(writtenIndex, shard);
                final String routedUuid = descriptor.get().uuid();
                serving.threadPool()
                    .executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC)
                    .execute(() -> writeOnceActivated(answer, serving, writtenIndex, routedUuid, shard, true, writeHere));
                return;
            }
            if (owner != null) {
                // Forward rather than refuse. The client should not have to know which node owns which
                // shard; that is exactly the knowledge the shard-head exists to hold.
                final String indexUuid = descriptor.get().uuid();
                serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
                    // Accounted on this node for as long as the forward is in flight, exactly as a local
                    // write is: the bytes are held here until the owner answers, whichever node applies them.
                    try (
                        org.opensearch.common.lease.Releasable forwarded = serving.services()
                            .indexingPressure()
                            .markCoordinatingOperationStarted(inFlightBytes, false)
                    ) {
                        String forwardTarget = owner;
                        org.opensearch.serverless.transport.ForwardedIndexResponse ack;
                        try {
                            ack = DocumentForwarding.forwardTo(
                                serving,
                                forwardTarget,
                                writtenIndex,
                                indexUuid,
                                shard,
                                id,
                                shapedSource,
                                refresh,
                                deletion,
                                ifSeqNo,
                                ifPrimaryTerm,
                                requireAbsent
                            );
                        } catch (ForwardFailed first) {
                            // Whatever went wrong, the hint that sent the write there is not consulted
                            // again. It used to survive everything but a refusal worded "does not own", so
                            // a hint naming a node that had died, or that held the shard only as a reader,
                            // was tried on every request and every request through this node failed the
                            // same way until it restarted -- while the same write through _bulk, which
                            // reads the register each time, succeeded.
                            serving.sightings().forgetOwner(writtenIndex, shard);
                            if (org.opensearch.serverless.transport.ForwardFailure.classify(first.getCause())
                                .mayRetryElsewhere() == false) {
                                // Applied, possibly applied, or answered: not sent anywhere else.
                                throw first;
                            }
                            // The owner never applied it. Read the register once and forward to whoever it names.
                            final var head = metadata.heads().read(writtenIndex, shard);
                            serving.sightings().noteHead(writtenIndex, shard, head.orElse(null));
                            final String fresh = head.map(h -> h.ownerNodeId()).orElse(null);
                            if (fresh == null) {
                                // Whoever the hint named is gone and nobody has taken over: the same state,
                                // and the same answer, as a register that never named anyone.
                                serving.signals().ownershipDoubted(writtenIndex, shard);
                                answer.onFailure(DocumentWriteOutcomes.notWriterRefusal(writtenIndex, shard, null));
                                return;
                            }
                            if (fresh.equals(serving.localNode().getId())) {
                                // Named here with nothing open: see ShardOperations#notHere for why this doubts.
                                serving.signals().ownershipDoubted(writtenIndex, shard);
                                answer.onFailure(DocumentWriteOutcomes.activationRefusal(writtenIndex, shard));
                                return;
                            }
                            if (fresh.equals(forwardTarget)) {
                                // Still the owner that did not answer. If its lease has run out, the write goes to the
                                // member that takes its shards over, once, marked to be taken there -- not to a doubt
                                // here that queued the shard on every survivor a write reached. Anything short of an
                                // answer from there falls back to taking it here.
                                if (serving.nodeCapacity().routesTakeovers()
                                    && head.isPresent()
                                    && metadata.heads().isHeld(head.get(), metadata.clock().getAsLong()) == false) {
                                    final Optional<String> winner = serving.nodeCapacity()
                                        .takeoverWinner(metadata, writtenIndex, shard, fresh);
                                    if (winner.isPresent() && winner.get().equals(serving.localNode().getId()) == false) {
                                        try {
                                            ack = DocumentForwarding.forwardTo(
                                                serving,
                                                winner.get(),
                                                writtenIndex,
                                                indexUuid,
                                                shard,
                                                id,
                                                shapedSource,
                                                refresh,
                                                deletion,
                                                ifSeqNo,
                                                ifPrimaryTerm,
                                                requireAbsent,
                                                true
                                            );
                                            serving.nodeCapacity().noteTakeoverWriteRouted();
                                            answer.onResponse(
                                                WriteResult.written(
                                                    new ShardId(writtenIndex, descriptor.get().uuid(), shard),
                                                    id,
                                                    ack.ownerNodeId(),
                                                    deletion,
                                                    new WriteOutcome(
                                                        ack.seqNo(),
                                                        ack.primaryTerm(),
                                                        ack.version(),
                                                        ack.created(),
                                                        ack.found()
                                                    ),
                                                    refresh
                                                )
                                            );
                                            return;
                                        } catch (ForwardFailed routed) {
                                            if (org.opensearch.serverless.transport.ForwardFailure.classify(routed.getCause())
                                                .mayRetryElsewhere() == false) {
                                                throw routed;
                                            }
                                        }
                                    }
                                    // This node is the one to take it, or the one that is did not: take it here.
                                    serving.threadPool()
                                        .executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC)
                                        .execute(
                                            () -> writeOnceActivated(answer, serving, writtenIndex, indexUuid, shard, false, writeHere)
                                        );
                                    return;
                                }
                                throw first;
                            }
                            forwardTarget = fresh;
                            ack = DocumentForwarding.forwardTo(
                                serving,
                                forwardTarget,
                                writtenIndex,
                                indexUuid,
                                shard,
                                id,
                                shapedSource,
                                refresh,
                                deletion,
                                ifSeqNo,
                                ifPrimaryTerm,
                                requireAbsent
                            );
                        }
                        answer.onResponse(
                            WriteResult.written(
                                new ShardId(writtenIndex, descriptor.get().uuid(), shard),
                                id,
                                ack.ownerNodeId(),
                                deletion,
                                new WriteOutcome(ack.seqNo(), ack.primaryTerm(), ack.version(), ack.created(), ack.found()),
                                refresh
                            )
                        );
                    } catch (ForwardFailed failed) {
                        answer.onFailure(
                            DocumentWriteOutcomes.classifyForwardFailure(serving, writtenIndex, shard, failed.target(), failed.getCause())
                        );
                    } catch (Exception e) {
                        // Not the forward's failure: a filter refused the write before it left this node,
                        // or the answer could not be written. Rendered with the plugin's own status, as the
                        // local path renders it. This used to be a 503 forward_failed that also cast doubt
                        // on ownership, so whether a caller was told 403 or 503 depended on which node it
                        // happened to reach.
                        answer.onFailure(e);
                    }
                });
                return;
            }
            // No owner at all. Nothing will fix this except some node activating the shard, and the
            // only reason anyone would is that a write arrived -- which just happened. So this node takes
            // it, and the write waits for that rather than being sent away to retry.
            final String routedUuid = descriptor.get().uuid();
            // Unless this node is nearly full and a member has room, or holds far more than its share and a member is
            // below the mean: the write goes there, marked to be taken, so a node that has just joined takes new shards
            // rather than the full ones evicting to make room for them, and load evens out below the cap as well.
            // Anything that went wrong before the member could have applied it falls back to taking it here.
            final Optional<String> roomy = serving.nodeCapacity().handOffTarget(metadata, descriptor.get());
            if (roomy.isPresent()) {
                serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
                    try (
                        org.opensearch.common.lease.Releasable forwarded = serving.services()
                            .indexingPressure()
                            .markCoordinatingOperationStarted(inFlightBytes, false)
                    ) {
                        final var ack = DocumentForwarding.forwardTo(
                            serving,
                            roomy.get(),
                            writtenIndex,
                            routedUuid,
                            shard,
                            id,
                            shapedSource,
                            refresh,
                            deletion,
                            ifSeqNo,
                            ifPrimaryTerm,
                            requireAbsent,
                            true
                        );
                        serving.nodeCapacity().noteWriteSteered();
                        answer.onResponse(
                            WriteResult.written(
                                new ShardId(writtenIndex, descriptor.get().uuid(), shard),
                                id,
                                ack.ownerNodeId(),
                                deletion,
                                new WriteOutcome(ack.seqNo(), ack.primaryTerm(), ack.version(), ack.created(), ack.found()),
                                refresh
                            )
                        );
                    } catch (ForwardFailed failed) {
                        if (org.opensearch.serverless.transport.ForwardFailure.classify(failed.getCause()).mayRetryElsewhere() == false) {
                            // Applied, possibly applied, or answered: not written anywhere else.
                            answer.onFailure(
                                DocumentWriteOutcomes.classifyForwardFailure(
                                    serving,
                                    writtenIndex,
                                    shard,
                                    failed.target(),
                                    failed.getCause()
                                )
                            );
                            return;
                        }
                        // Waiting for an activation belongs on GENERIC, as below, not on a write thread.
                        serving.threadPool()
                            .executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC)
                            .execute(() -> writeOnceActivated(answer, serving, writtenIndex, routedUuid, shard, false, writeHere));
                    } catch (Exception e) {
                        answer.onFailure(e);
                    }
                });
                return;
            }
            serving.threadPool()
                .executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC)
                .execute(() -> writeOnceActivated(answer, serving, writtenIndex, routedUuid, shard, false, writeHere));
            return;
        }

        // Off the HTTP thread: a WAL append is an object-store write, and blocking the thread that
        // should be reading the next request on remote IO is how a node stops answering under load.
        serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> writeHere.accept(answer, shardId));
        return;
    }

    /** How long a write to a shard nobody holds waits for this node to take it, before it is told to retry. */
    static final long ACTIVATION_WAIT_MILLIS = 30_000L;

    /**
     * Waits for this node to take a shard a write is waiting for, then writes.
     *
     * <p>A write to a shard nobody holds used to be answered 421 at once, with the doubt raised so a pass
     * would take the shard; the client retried, every retry read the head again, and those retries were most
     * of what a first write cost. The write now waits on the same single-flight activation every pass uses --
     * so a burst of writes to one cold shard is one activation -- and is written here once the shard is open.
     * Only a write to the incarnation it was routed for: a shard opened under another uuid is an index
     * recreated since, and the answer is the refusal a retry resolves afresh.
     */
    void writeOnceActivated(
        org.opensearch.core.action.ActionListener<WriteResult> answer,
        ServerlessNode serving,
        String writtenIndex,
        String indexUuid,
        int shard,
        boolean ownedHere,
        java.util.function.BiConsumer<org.opensearch.core.action.ActionListener<WriteResult>, ShardId> writeHere
    ) {
        Optional<ShardId> taken = Optional.empty();
        try {
            taken = serving.signals().activate(writtenIndex, shard).get(ACTIVATION_WAIT_MILLIS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            // Still opening: the same "not yet" a client already retries.
            answer.onFailure(DocumentWriteOutcomes.activationRefusal(writtenIndex, shard));
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException e) {
            logger.warn("activating shard " + shard + " of " + writtenIndex + " for a waiting write failed", e.getCause());
        }
        if (taken.isPresent() && taken.get().getIndex().getUUID().equals(indexUuid)) {
            writeHere.accept(answer, taken.get());
            return;
        }
        // Not taken here: another node won it (its head is noted, so the retry forwards), this node is full,
        // or demand-driven activation is off. The answer a client already knows how to act on.
        serving.signals().ownershipDoubted(writtenIndex, shard);
        answer.onFailure(
            ownedHere
                ? DocumentWriteOutcomes.activationRefusal(writtenIndex, shard)
                : DocumentWriteOutcomes.notWriterRefusal(writtenIndex, shard, null)
        );
    }
}
