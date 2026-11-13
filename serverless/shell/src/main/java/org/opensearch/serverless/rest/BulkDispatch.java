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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.WriteTarget;
import org.opensearch.serverless.shell.BulkOperation;
import org.opensearch.serverless.shell.BulkOutcome;
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Routes a parsed bulk request for {@link BulkHandler}: runs each item's pipeline, groups the items by shard, applies
 * the groups this node owns and forwards the rest, and records every item's outcome.
 */
final class BulkDispatch {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    BulkDispatch(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    /**
     * Decides, per item, which shard it belongs to and who owns it — or records why it cannot be placed.
     */
    void route(ServerlessNode serving, MetadataPlane metadata, List<BulkHandler.Item> items) throws IOException {
        final Map<String, Optional<WriteTarget>> targets = new HashMap<>();
        final java.util.Set<String> failures = new java.util.HashSet<>();
        for (BulkHandler.Item item : items) {
            if (item.failed()) {
                continue;
            }
            if (item.index == null || item.index.isBlank()) {
                item.fail(RestStatus.BAD_REQUEST, "missing_index", "no index given for this item and none in the path");
                continue;
            }
            if (serving.isSystemIndex(item.index)) {
                // The one place the registration-time guard cannot reach: a bulk request names its indices
                // in the body, so the path a request arrived on says nothing about what it touches. Without
                // this, a plugin's system index would be readable and writable by anyone who could spell
                // _bulk -- which for the authentication plugin's accounts means writing a password record
                // of your choosing under somebody else's name.
                item.fail(
                    RestStatus.FORBIDDEN,
                    "system_index",
                    "[" + item.index + "] belongs to a plugin and is not reachable through the request path"
                );
                continue;
            }
            final Optional<WriteTarget> target = targets.computeIfAbsent(item.index, name -> {
                try {
                    return metadata.writeTarget(name);
                } catch (IOException e) {
                    failures.add(name);
                    return Optional.empty();
                }
            });
            if (failures.contains(item.index)) {
                item.fail(RestStatus.SERVICE_UNAVAILABLE, "metadata_unavailable", "could not resolve index [" + item.index + "]; retry");
                continue;
            }
            if (target.isEmpty()) {
                item.fail(RestStatus.NOT_FOUND, "index_not_found", "no such index: " + item.index);
                continue;
            }
            if (target.get().dataStream() != null && (item.requireAbsent == false || item.operation.isDeletion())) {
                item.fail(
                    RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    "only write ops with an op_type of create are allowed in data streams; address the backing index ["
                        + target.get().index().name()
                        + "] for anything else"
                );
                continue;
            }
            // The backing index is where the write lands, and it is what the item reports, as core reports
            // it. Its uuid rides along so the local lookup and the forward both name the incarnation.
            item.writtenIndex = target.get().index().name();
            item.writtenUuid = target.get().index().uuid();
            try {
                item.shard = DocumentRouting.shardFor(target.get().index(), item.operation.id(), item.operation.routing());
            } catch (org.opensearch.action.RoutingMissingException e) {
                // The mapping requires routing and this item has none: its own failure, as core reports it.
                item.fail(RestStatus.BAD_REQUEST, "routing_missing_exception", e.getMessage());
                continue;
            }
            // Resolved here rather than in shape(), because this is where the index actually written to is
            // known: index.default_pipeline belongs to the backing index a data stream write lands on, not
            // to the name the caller used.
            item.pipelines = org.opensearch.serverless.ingest.IngestPipelines.pipelinesFor(
                item.pipeline,
                target.get().index().extraSettings()
            );
        }
    }

    /**
     * Runs each item's ingest pipeline, before anything is written.
     *
     * <p>The same rule as the single-document path: a document is shaped and then stored, and a caller
     * must never be told a write succeeded against a document the pipeline was supposed to change and did
     * not. An action line's own {@code pipeline} wins over the request's and over the index's
     * {@code index.default_pipeline}, as in core; {@code index.final_pipeline} runs after whichever of
     * those ran, and is the one a caller cannot opt out of. The order is settled in {@code route}, where
     * the index actually written to is known -- see {@code IngestPipelines#pipelinesFor}.
     *
     * <p>A pipeline that is missing or fails is that item's failure, not the batch's; a document a pipeline
     * drops is a {@code noop}, which is not an error and not a write, and ends that item so a final
     * pipeline does not run over a document nobody will store. Compiled once per distinct pipeline per
     * batch, which now matters more: a default pipeline is by definition the same one for every item.
     */
    void shape(ServerlessNode serving, MetadataPlane metadata, List<BulkHandler.Item> items) {
        final Map<String, org.opensearch.ingest.Pipeline> compiled = new HashMap<>();
        for (BulkHandler.Item item : items) {
            if (item.failed() || item.pipelines.isEmpty() || item.operation.isDeletion()) {
                continue;
            }
            try {
                for (String id : item.pipelines) {
                    org.opensearch.ingest.Pipeline pipeline = compiled.get(id);
                    if (pipeline == null) {
                        final var stored = metadata.pipelines().get(id);
                        if (stored.isEmpty()) {
                            item.fail(RestStatus.BAD_REQUEST, "pipeline_missing", "no such pipeline: " + id);
                            break;
                        }
                        pipeline = serving.services().ingestPipelines().compile(id, stored.get());
                        compiled.put(id, pipeline);
                    }
                    final var outcome = serving.services()
                        .ingestPipelines()
                        .run(pipeline, item.writtenIndex, item.operation.id(), item.operation.source());
                    if (outcome.dropped()) {
                        // A drop ends the document, so a final pipeline does not run over one that is not
                        // going to be written. Core does the same.
                        item.noop(serving.localNode().getId());
                        break;
                    }
                    item.operation = item.operation.withSource(outcome.source());
                }
            } catch (Exception e) {
                item.fail(RestStatus.BAD_REQUEST, "pipeline_failed", e.getMessage() == null ? e.toString() : e.getMessage());
            }
        }
    }

    /**
     * Applies each shard's group once, locally or over transport, and records the outcomes in place.
     *
     * <p><b>Groups run concurrently.</b> They used to run one after another, so a batch spanning three
     * remote shards was three round trips end to end — the whole point of batching is to stop paying per
     * document, and paying per shard instead was the same mistake one level up. Each group touches only
     * its own items, so they do not interact; the outcomes are written into the items themselves and read
     * back after every group has finished.
     *
     * @return true if a refresh was asked for and every group that applied is visible as far as it promises
     */
    boolean dispatch(ServerlessNode serving, List<BulkHandler.Item> items, boolean refresh) throws IOException {
        final Map<String, List<BulkHandler.Item>> groups = new LinkedHashMap<>();
        for (BulkHandler.Item item : items) {
            if (item.failed() == false && item.status == null) {
                groups.computeIfAbsent(item.writtenIndex + "[" + item.shard + "]", key -> new ArrayList<>()).add(item);
            }
        }
        final List<java.util.concurrent.Callable<Void>> tasks = new ArrayList<>(groups.size());
        final java.util.concurrent.atomic.AtomicBoolean refreshed = new java.util.concurrent.atomic.AtomicBoolean(refresh);
        for (List<BulkHandler.Item> group : groups.values()) {
            tasks.add(() -> {
                if (applyGroup(serving, group, refresh) == false) {
                    refreshed.set(false);
                }
                return null;
            });
        }
        try {
            Fanout.run(
                serving.threadPool().executor(org.opensearch.serverless.shell.WritePools.writePool(serving.settings())),
                Fanout.DEFAULT_CONCURRENCY,
                tasks
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while writing a batch", e);
        }
        // A group whose task died before recording anything would leave its items with no outcome at all,
        // which would serialize as a success with no result. Say so instead.
        for (BulkHandler.Item item : items) {
            if (item.failed() == false && item.status == null) {
                item.fail(RestStatus.INTERNAL_SERVER_ERROR, "no_outcome", "the writer returned no outcome for this operation");
            }
        }
        return refreshed.get();
    }

    /**
     * Applies one shard's group.
     *
     * @return false if a refresh was asked for and what the group applied is not visible as far as it promises; true
     *     otherwise, a failed group included, since it applied nothing to make visible
     */
    boolean applyGroup(ServerlessNode serving, List<BulkHandler.Item> group, boolean refresh) throws IOException {
        final String index = group.get(0).writtenIndex;
        final String uuid = group.get(0).writtenUuid;
        final int shard = group.get(0).shard;
        final List<BulkOperation> operations = new ArrayList<>(group.size());
        for (BulkHandler.Item item : group) {
            operations.add(new BulkOperation(item.operation, item.ifSeqNo, item.ifPrimaryTerm, item.requireAbsent));
        }

        // By uuid as well as name, as the single-document path has matched since the recreate bug: a
        // deleted and recreated index keeps its name, and a writer that has not yet heartbeat still holds
        // the old incarnation open under it. Matched by name alone, a batch through that node was applied
        // to the old shard, logged into a log nothing replays, and answered 201.
        final var writer = serving.reconciler().writerShard(new ShardId(index, uuid, shard));
        final ShardId local = writer == null ? null : writer.shardId();

        if (local != null) {
            try {
                final List<org.opensearch.serverless.shell.BulkOutcome> outcomes = serving.writes().bulkOperations(local, operations);
                record(group, outcomes, serving.localNode().getId());
                if (refresh) {
                    return serving.writes().refreshWritten(local, outcomes, "serverless-bulk-refresh");
                }
            } catch (Exception e) {
                // A batch that could not be applied here -- the log append failed and the shard was
                // released, the shard turned out to be a reader, this node's own lease had lapsed -- used
                // to escape into the fan-out, which swallowed it, and every item answered 500 "the writer
                // returned no outcome". The reason and the right status: the shard is moving, retry.
                serving.signals().ownershipDoubted(index, shard);
                if (org.opensearch.ExceptionsHelper.unwrap(e, org.opensearch.serverless.shell.StaleIncarnationException.class) != null) {
                    // Both the name the client wrote to and the index it resolved to, which differ for a data stream.
                    plane.get().forgetRouting(group.get(0).index);
                    plane.get().forgetRouting(index);
                }
                failGroup(
                    group,
                    RestStatus.SERVICE_UNAVAILABLE,
                    "write_failed",
                    "the batch could not be applied on this node, which held shard "
                        + shard
                        + " of "
                        + index
                        + ": "
                        + (e.getMessage() == null ? e.toString() : e.getMessage())
                        + "; the shard may have moved, retry"
                );
            }
            return true;
        }

        final var head = plane.get().heads().read(index, shard);
        final String owner = head.map(h -> h.ownerNodeId()).orElse(null);
        if (owner == null && serving.nodeCapacity().takesWriters() == false) {
            // Nobody owns it and this node cannot take it: a search node, or one that only coordinates. The batch goes
            // to the writer member every such node ranks first for the shard, marked to be taken there.
            return steerGroup(serving, group, index, uuid, shard, operations, refresh);
        }
        if (owner == null) {
            // Nobody owns it, and the only reason anyone would activate it is that a write arrived --
            // which just did. Same conclusion the single-document path reaches.
            serving.signals().ownershipDoubted(index, shard);
            failGroup(group, RestStatus.MISDIRECTED_REQUEST, "not_the_writer", "no node currently owns shard " + shard + " of " + index);
            return true;
        }
        if (owner.equals(serving.localNode().getId())) {
            // The head names this node and the shard is not open here: the activation window, brief and
            // self-resolving -- provided an activation is running, which the doubt makes sure of. Forwarding
            // would send the batch to ourselves and be refused.
            serving.signals().ownershipDoubted(index, shard);
            failGroup(
                group,
                RestStatus.SERVICE_UNAVAILABLE,
                "activation_in_progress",
                "this node is acquiring shard " + shard + " of " + index + "; retry"
            );
            return true;
        }
        try {
            final var peer = serving.router().peer(owner);
            if (peer.isEmpty()) {
                // The head names an owner holding no live lease -- a dead writer nobody has noticed, and
                // this batch is the first thing in the system to prove it.
                serving.signals().ownershipDoubted(index, shard);
                failGroup(
                    group,
                    RestStatus.SERVICE_UNAVAILABLE,
                    "owner_unreachable",
                    "shard " + shard + " is owned by " + owner + ", which has no reachable lease"
                );
                return true;
            }
            final var response = serving.router()
                .forwardBulk(
                    peer.get(),
                    org.opensearch.serverless.transport.ForwardedBulkRequest.of(index, uuid, shard, operations, refresh)
                );
            record(group, response.outcomes(), response.ownerNodeId());
            return refresh == false || response.refreshed();
        } catch (Exception e) {
            failForwarded(serving, group, index, shard, owner, e);
        }
        return true;
    }

    /**
     * Sends a group for a shard nobody holds to a writer member to take, from a node that takes no writers.
     *
     * @return as {@link #applyGroup}
     */
    boolean steerGroup(
        ServerlessNode serving,
        List<BulkHandler.Item> group,
        String index,
        String uuid,
        int shard,
        List<BulkOperation> operations,
        boolean refresh
    ) {
        final MetadataPlane metadata = plane.get();
        final Optional<String> target;
        try {
            final Optional<org.opensearch.serverless.cluster.IndexDescriptor> descriptor = metadata.describeForRouting(index);
            target = descriptor.isPresent() ? serving.nodeCapacity().writerForUnheld(metadata, descriptor.get(), shard) : Optional.empty();
        } catch (IOException e) {
            failGroup(
                group,
                RestStatus.SERVICE_UNAVAILABLE,
                "no_writer",
                "could not read " + index + " to route shard " + shard + "; retry"
            );
            return true;
        }
        if (target.isEmpty()) {
            failGroup(
                group,
                RestStatus.SERVICE_UNAVAILABLE,
                "no_writer",
                "no node that takes writers is live to take shard " + shard + " of " + index + "; retry"
            );
            return true;
        }
        try {
            final var peer = serving.router().peer(target.get());
            if (peer.isEmpty()) {
                failGroup(
                    group,
                    RestStatus.SERVICE_UNAVAILABLE,
                    "owner_unreachable",
                    "shard " + shard + " of " + index + " was to be taken by " + target.get() + ", which has no reachable lease"
                );
                return true;
            }
            final var response = serving.router()
                .forwardBulk(
                    peer.get(),
                    org.opensearch.serverless.transport.ForwardedBulkRequest.of(index, uuid, shard, operations, refresh).steered()
                );
            serving.nodeCapacity().noteWriteSteered();
            record(group, response.outcomes(), response.ownerNodeId());
            return refresh == false || response.refreshed();
        } catch (Exception e) {
            failForwarded(serving, group, index, shard, target.get(), e);
        }
        return true;
    }

    /**
     * Fails a forwarded group with the status its failure deserves, the same sorting the single-document
     * path applies.
     *
     * <p>Every failure used to be a 503 {@code forward_failed} that cast doubt on ownership. An owner at its
     * indexing-pressure limit told a local batch 429 and a forwarded one 503; a deadline was worded as
     * "stale routing, retry", which for a batch of auto-id documents is an invitation to write them twice.
     */
    void failForwarded(ServerlessNode serving, List<BulkHandler.Item> group, String index, int shard, String owner, Exception e) {
        final org.opensearch.serverless.transport.ForwardFailure kind = org.opensearch.serverless.transport.ForwardFailure.classify(e);
        if (kind.castsDoubtOnOwnership()) {
            serving.signals().ownershipDoubted(index, shard);
        }
        switch (kind) {
            case REJECTED: {
                final Throwable rejected = org.opensearch.ExceptionsHelper.unwrap(
                    e,
                    org.opensearch.core.concurrency.OpenSearchRejectedExecutionException.class
                );
                failGroup(group, RestStatus.TOO_MANY_REQUESTS, "rejected_execution_exception", rejected.getMessage());
                return;
            }
            case CONFLICT: {
                final Throwable conflict = org.opensearch.ExceptionsHelper.unwrap(
                    e,
                    org.opensearch.index.engine.VersionConflictEngineException.class
                );
                failGroup(group, RestStatus.CONFLICT, "version_conflict_engine_exception", conflict.getMessage());
                return;
            }
            case REMOTE: {
                if (org.opensearch.serverless.transport.ForwardFailure.isClientStatus(e)) {
                    // The owner's own answer, with the owner's own status and core's own type name.
                    final Throwable answer = org.opensearch.serverless.transport.ForwardFailure.answer(e);
                    failGroup(
                        group,
                        org.opensearch.ExceptionsHelper.status(answer),
                        org.opensearch.OpenSearchException.getExceptionName(answer),
                        answer.getMessage() == null ? answer.toString() : answer.getMessage()
                    );
                    return;
                }
                failGroup(
                    group,
                    RestStatus.SERVICE_UNAVAILABLE,
                    "forward_failed",
                    org.opensearch.serverless.transport.ForwardFailure.describe(owner, e)
                );
                return;
            }
            default: {
                // 503 rather than the exception's own status, for the reason the single-write path gives:
                // a forward that fails means the routing was stale, and stale routing is a retry. A deadline
                // is worded as one, so the client reads before it retries.
                failGroup(
                    group,
                    RestStatus.SERVICE_UNAVAILABLE,
                    "forward_failed",
                    org.opensearch.serverless.transport.ForwardFailure.describe(owner, e)
                );
            }
        }
    }

    void record(List<BulkHandler.Item> group, List<BulkOutcome> outcomes, String nodeId) {
        if (outcomes.size() != group.size()) {
            // Cannot happen through either path, and if it ever does, silently pairing the wrong outcome
            // with the wrong document is far worse than saying so.
            failGroup(
                group,
                RestStatus.INTERNAL_SERVER_ERROR,
                "outcome_mismatch",
                "the writer returned " + outcomes.size() + " outcomes for " + group.size() + " operations"
            );
            return;
        }
        for (int i = 0; i < group.size(); i++) {
            group.get(i).succeed(outcomes.get(i), nodeId);
        }
    }

    void failGroup(List<BulkHandler.Item> group, RestStatus status, String type, String reason) {
        for (BulkHandler.Item item : group) {
            item.fail(status, type, reason);
        }
    }
}
