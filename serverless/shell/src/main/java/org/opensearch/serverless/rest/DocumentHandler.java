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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code PUT|POST /{index}/_doc/{id}} — the durable write path, reachable by a user.
 *
 * <p>Everything M10 built sat behind a Java method until this existed. A write here reaches the
 * write-ahead log before it is acknowledged, so it survives the writer dying before the next publication.
 *
 * <p><b>A node that does not own the shard forwards to the one that does</b>, over transport, and answers
 * as if it had done the write itself. Losing an activation race is still a routing instruction rather
 * than an error, and the owner's id still rides in the response — a client that wants to skip the extra
 * hop next time can, but nothing requires it to.
 *
 * <p><b>Conditional writes are supported, and every response carries the token they compare against.</b>
 * {@code if_seq_no} and {@code if_primary_term} are passed to the engine, which performs the
 * compare-and-swap itself against the live version map; a lost race is a 409. That became possible when
 * the log started recording each operation's sequence number, so the numbers survive a failover and a
 * token stays meaningful across one — see {@code m48-sequence-numbers-notes.md}.
 *
 * <p><b>{@code version} is still refused</b>, and the difference matters: external versioning asks this
 * system to order writes by a number the caller maintains and this system does not keep, which is a
 * different feature from comparing against a number the engine itself assigned.
 */
public final class DocumentHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public DocumentHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_document_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/{index}/_doc/{id}"),
            new Route(RestRequest.Method.POST, "/{index}/_doc/{id}"),
            // An id the caller does not supply. The routing key is the id, so it has to exist before the
            // request can be placed -- which is why it is generated here rather than by the shard. _bulk
            // has generated ids all along; this path simply had no route.
            new Route(RestRequest.Method.POST, "/{index}/_doc"),
            // Create-if-absent. The engine does the comparison, at MATCH_DELETED, under the per-document
            // lock it already holds -- so this is a different constant on the same call, not a read
            // followed by a write, which would be a race rather than a check.
            new Route(RestRequest.Method.PUT, "/{index}/_create/{id}"),
            new Route(RestRequest.Method.POST, "/{index}/_create/{id}"),
            // A deletion routes, forwards and logs exactly like a write, so it belongs on the same
            // handler rather than in a parallel one that would have to be kept in step with it.
            new Route(RestRequest.Method.DELETE, "/{index}/_doc/{id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Read every parameter before any early return: BaseRestHandler rejects a request whose
        // parameters were not all consumed, which would turn a deliberate refusal into a 400.
        final String index = request.param("index");
        // op_type=create is the same request as the /_create/ path, and core accepts either spelling.
        final String opType = request.param("op_type");
        final boolean requireAbsent = request.path().contains("/_create/") || "create".equals(opType);
        // An absent id is generated, exactly as _bulk has always done, because routing is a function of
        // the id and so it must exist before the request can be placed at all.
        final String id = request.param("id") == null ? org.opensearch.common.UUIDs.base64UUID() : request.param("id");
        final String source = request.hasContent() ? request.content().utf8ToString() : null;
        final boolean refresh = IndexAdminHandler.refresh(request);
        final boolean deletion = request.method() == RestRequest.Method.DELETE;
        // Read, not merely present-checked, so every parameter is consumed regardless of which branch
        // below returns -- the class-level idiom this handler already follows.
        final String ifSeqNoParam = request.param("if_seq_no");
        final String ifPrimaryTermParam = request.param("if_primary_term");
        final String version = request.param("version");
        final String versionType = request.param("version_type");
        final String pipeline = request.param("pipeline");
        final String routing = request.param("routing");
        final boolean requireAlias = request.paramAsBoolean("require_alias", false);
        // Hints with one possible answer here: there is one copy of each shard, so wait_for_active_shards
        // can only be 1, and a write is acknowledged when its log write returns, which is what timeout
        // would bound. Consumed so a client that always sends them is not turned away.
        final String timeoutValue = request.param("timeout");
        final String activeShardsValue = request.param("wait_for_active_shards");
        final String refreshValue = request.param("refresh");

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if (opType != null && "create".equals(opType) == false && "index".equals(opType) == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "bad_request",
                    "op_type must be create or index, not [" + opType + "]"
                )
            );
        }
        if (routing != null) {
            // Refused rather than ignored: a document here is placed by its id alone, and a caller
            // supplying a routing value expects it to decide the shard.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "routing is not supported: a document is placed by its id alone, so a routing value would be "
                        + "accepted and change nothing"
                )
            );
        }
        if (requireAlias) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "require_alias is not supported: writes here name an index, and an alias resolves on read"
                )
            );
        }
        if (version != null || versionType != null) {
            // Still refused, and for a reason that did not go away when if_seq_no arrived. External
            // versioning asks this system to order writes by a number the caller maintains and this
            // system does not; optimistic concurrency asks it to compare against a number the engine
            // itself assigned, which it now returns. Those are different features, and only the second
            // one is here. Classic OpenSearch steers callers the same way.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "'version' asks for external versioning, which this system does not have: it keeps no "
                        + "caller-supplied version model. Use if_seq_no and if_primary_term, which compare "
                        + "against the sequence number this system does assign and does return."
                )
            );
        }
        if ((ifSeqNoParam == null) != (ifPrimaryTermParam == null)) {
            // Core requires both together, and requiring it here too makes the refusal legible: half a
            // condition is a condition that would silently not be one.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "invalid_condition",
                    "if_seq_no and if_primary_term must be supplied together"
                )
            );
        }
        final long ifSeqNo;
        final long ifPrimaryTerm;
        try {
            ifSeqNo = ifSeqNoParam == null ? org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO : Long.parseLong(ifSeqNoParam);
            ifPrimaryTerm = ifPrimaryTermParam == null ? 0L : Long.parseLong(ifPrimaryTermParam);
        } catch (NumberFormatException e) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "invalid_condition",
                    "if_seq_no and if_primary_term must be numbers"
                )
            );
        }
        if (deletion == false && (source == null || source.isBlank())) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a document body is required")
            );
        }

        final ServerlessNode serving = node.get();
        if (serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "node_not_ready", "no node is serving requests yet")
            );
        }
        // Resolution, pipeline reads/execution and owner discovery can all block on the store.
        // Preserve the existing execution pools for applying and forwarding the planned write.
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                startWrite(
                    channel,
                    metadata,
                    serving,
                    index,
                    id,
                    source,
                    pipeline,
                    deletion,
                    requireAbsent,
                    refresh,
                    ifSeqNo,
                    ifPrimaryTerm,
                    refreshValue,
                    timeoutValue,
                    activeShardsValue
                );
            } catch (Exception e) {
                respondFailure(channel, e);
            }
        });
    }

    private void startWrite(
        org.opensearch.rest.RestChannel channel,
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
        String refreshValue,
        String timeoutValue,
        String activeShardsValue
    ) throws Exception {
        final org.opensearch.core.action.ActionListener<WriteResult> completed = org.opensearch.core.action.ActionListener.wrap(result -> {
            try {
                respondResult(channel, result);
            } catch (Exception e) {
                respondFailure(channel, e);
            }
        }, failure -> respondFailure(channel, failure));
        if (serving.actionGate().isEmpty()) {
            planWrite(metadata, serving, index, id, source, pipeline, deletion, requireAbsent, refresh, ifSeqNo, ifPrimaryTerm, completed);
            return;
        }
        final var policy = "wait_for".equals(refreshValue) ? org.opensearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL
            : refresh ? org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE
            : org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE;
        final org.opensearch.action.ActionRequest request;
        if (deletion) {
            final var delete = new org.opensearch.action.delete.DeleteRequest(index, id).setIfSeqNo(ifSeqNo)
                .setIfPrimaryTerm(ifPrimaryTerm)
                .setRefreshPolicy(policy);
            if (timeoutValue != null) {
                delete.timeout(org.opensearch.common.unit.TimeValue.parseTimeValue(timeoutValue, "timeout"));
            }
            if (activeShardsValue != null) {
                delete.waitForActiveShards(org.opensearch.action.support.ActiveShardCount.parseString(activeShardsValue));
            }
            request = delete;
        } else {
            final var write = new org.opensearch.action.index.IndexRequest(index).id(id)
                .source(source, org.opensearch.common.xcontent.XContentType.JSON)
                .create(requireAbsent)
                .setIfSeqNo(ifSeqNo)
                .setIfPrimaryTerm(ifPrimaryTerm)
                .setRefreshPolicy(policy)
                .setPipeline(pipeline);
            if (timeoutValue != null) {
                write.timeout(org.opensearch.common.unit.TimeValue.parseTimeValue(timeoutValue, "timeout"));
            }
            if (activeShardsValue != null) {
                write.waitForActiveShards(org.opensearch.action.support.ActiveShardCount.parseString(activeShardsValue));
            }
            request = write;
        }
        serving.actionGate()
            .runAsync(
                serving.threadPool(),
                deletion ? org.opensearch.action.delete.DeleteAction.NAME : org.opensearch.action.index.IndexAction.NAME,
                request,
                (admitted, listener) -> planAcceptedWrite(metadata, serving, deletion, admitted, listener),
                writeView(deletion),
                completed
            );
    }

    private void planAcceptedWrite(
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
            validateWriteOptions(selected.routing(), selected.version(), selected.versionType(), selected.waitForActiveShards(), false);
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
            validateWriteOptions(selected.routing(), selected.version(), selected.versionType(), selected.waitForActiveShards(), create);
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

    private static void validateWriteOptions(
        String routing,
        long version,
        org.opensearch.index.VersionType versionType,
        org.opensearch.action.support.ActiveShardCount activeShards,
        boolean create
    ) {
        if (routing != null) {
            throw new WriteRefusal(
                RestStatus.NOT_IMPLEMENTED,
                "unsupported_write",
                "routing is not supported: documents are placed by their id"
            );
        }
        if (versionType != org.opensearch.index.VersionType.INTERNAL
            || version != org.opensearch.common.lucene.uid.Versions.MATCH_ANY
                && (create == false || version != org.opensearch.common.lucene.uid.Versions.MATCH_DELETED)) {
            throw new WriteRefusal(
                RestStatus.NOT_IMPLEMENTED,
                "unsupported_write",
                "version/version_type is not supported; use sequence conditions"
            );
        }
        if (activeShards.validate(0) == false) {
            throw new WriteRefusal(
                RestStatus.NOT_IMPLEMENTED,
                "unsupported_write",
                "wait_for_active_shards cannot exceed one: shards have no replicas"
            );
        }
    }

    /** The result and the routing facts that belong only to an actual execution. */
    private static final class WriteResult {
        private final ShardId shardId;
        private final String id;
        private final String owner;
        private final boolean deletion;
        private final ServerlessNode.WriteOutcome outcome;
        private final boolean refreshed;
        private final String droppedBy;
        private final boolean requireAbsent;
        private org.opensearch.action.DocWriteResponse response;

        private WriteResult(
            ShardId shardId,
            String id,
            String owner,
            boolean deletion,
            ServerlessNode.WriteOutcome outcome,
            boolean refreshed,
            String droppedBy,
            boolean requireAbsent
        ) {
            this.shardId = shardId;
            this.id = id;
            this.owner = owner;
            this.deletion = deletion;
            this.outcome = outcome;
            this.refreshed = refreshed;
            this.droppedBy = droppedBy;
            this.requireAbsent = requireAbsent;
        }

        static WriteResult written(
            ShardId shardId,
            String id,
            String owner,
            boolean deletion,
            ServerlessNode.WriteOutcome outcome,
            boolean refreshed
        ) {
            return new WriteResult(shardId, id, owner, deletion, outcome, refreshed, null, false);
        }

        static WriteResult dropped(ShardId shardId, String id, String pipeline, boolean requireAbsent) {
            return new WriteResult(shardId, id, null, false, null, false, pipeline, requireAbsent);
        }

        static WriteResult supplied(org.opensearch.action.DocWriteResponse response) {
            final var result = new WriteResult(null, null, null, false, null, false, null, false);
            result.response = response;
            return result;
        }

        org.opensearch.action.DocWriteResponse nativeResponse() {
            if (response == null) {
                if (droppedBy != null) {
                    // Core's single-item bulk adapter likewise uses UpdateResponse for an ingest NOOP.
                    response = new org.opensearch.action.update.UpdateResponse(
                        shardId,
                        id,
                        org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
                        0L,
                        requireAbsent
                            ? org.opensearch.common.lucene.uid.Versions.MATCH_DELETED
                            : org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                        org.opensearch.action.DocWriteResponse.Result.NOOP
                    );
                    response.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(0, 0));
                } else {
                    response = deletion
                        ? new org.opensearch.action.delete.DeleteResponse(
                            shardId,
                            id,
                            outcome.seqNo(),
                            outcome.primaryTerm(),
                            outcome.version(),
                            outcome.found()
                        )
                        : new org.opensearch.action.index.IndexResponse(
                            shardId,
                            id,
                            outcome.seqNo(),
                            outcome.primaryTerm(),
                            outcome.version(),
                            outcome.created()
                        );
                    response.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
                    response.setForcedRefresh(refreshed);
                }
            }
            return response;
        }
    }

    private static org.opensearch.serverless.shell.ActionGate.ResponseView<WriteResult> writeView(boolean deletion) {
        return new org.opensearch.serverless.shell.ActionGate.ResponseView<>() {
            @Override
            public org.opensearch.core.action.ActionResponse show(WriteResult result) {
                return result.nativeResponse();
            }

            @Override
            public WriteResult read(org.opensearch.core.action.ActionResponse response, WriteResult original) {
                final boolean supported = deletion
                    ? response instanceof org.opensearch.action.delete.DeleteResponse
                    : response instanceof org.opensearch.action.index.IndexResponse
                        || response instanceof org.opensearch.action.update.UpdateResponse update
                            && update.getResult() == org.opensearch.action.DocWriteResponse.Result.NOOP;
                if (supported == false) {
                    throw new IllegalStateException("a write filter supplied an incompatible response: " + response.getClass().getName());
                }
                return original != null && original.response == response
                    ? original
                    : WriteResult.supplied((org.opensearch.action.DocWriteResponse) response);
            }
        };
    }

    /** Renders only after every response filter has completed. */
    private void respondResult(org.opensearch.rest.RestChannel channel, WriteResult result) throws IOException {
        if (result.response == null && result.outcome != null) {
            // Keep native request/response allocation out of the no-filter write path.
            respond(
                channel,
                result.shardId.getIndexName(),
                result.id,
                result.shardId.id(),
                result.owner,
                result.deletion,
                result.outcome,
                result.refreshed
            );
            return;
        }
        try (var builder = channel.newBuilder()) {
            if (result.response == null) {
                builder.startObject()
                    .field("_index", result.shardId.getIndexName())
                    .field("_id", result.id)
                    .field("result", "noop")
                    .field("dropped_by_pipeline", result.droppedBy)
                    .endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                return;
            }
            if (result.outcome != null || result.droppedBy != null) {
                builder.startObject();
                result.response.innerToXContent(builder, channel.request());
                if (result.outcome != null) {
                    builder.field("_shard", result.shardId.id()).field("durable", "write-ahead log").field("_node", result.owner);
                } else {
                    builder.field("dropped_by_pipeline", result.droppedBy);
                }
                builder.endObject();
            } else {
                result.response.toXContent(builder, channel.request());
            }
            channel.sendResponse(new BytesRestResponse(result.response.status(), builder));
        }
    }

    private void respondFailure(org.opensearch.rest.RestChannel channel, Exception failure) {
        try {
            channel.sendResponse(
                failure instanceof WriteRefusal refusal
                    ? IndexAdminHandler.error(channel, refusal.status(), refusal.type, refusal.getMessage())
                    : IndexAdminHandler.failure(channel, failure)
            );
        } catch (IOException nested) {
            logger.error("failed to report a write failure", nested);
        }
    }

    private static final class WriteRefusal extends org.opensearch.OpenSearchStatusException {
        private final String type;

        WriteRefusal(RestStatus status, String type, String reason) {
            super(reason, status);
            this.type = type;
        }
    }

    private static WriteRefusal notWriterRefusal(String index, int shard, String owner) {
        final String reason = owner == null
            ? "no node currently owns shard " + shard + " of " + index + "; activate it before writing"
            : "this node does not own shard " + shard + " of " + index + "; it is owned by " + owner;
        return new WriteRefusal(RestStatus.MISDIRECTED_REQUEST, "not_the_writer", reason);
    }

    private static WriteRefusal activationRefusal(String index, int shard) {
        return new WriteRefusal(
            RestStatus.SERVICE_UNAVAILABLE,
            "activation_in_progress",
            "this node is acquiring shard " + shard + " of " + index + "; retry"
        );
    }

    private Exception classifyForwardFailure(ServerlessNode serving, String index, int shard, String owner, Throwable cause) {
        final var kind = org.opensearch.serverless.transport.ForwardFailure.classify(cause);
        if (kind.castsDoubtOnOwnership()) {
            serving.signals().ownershipDoubted(index, shard);
        }
        return switch (kind) {
            case CONFLICT -> asException(
                org.opensearch.ExceptionsHelper.unwrap(cause, org.opensearch.index.engine.VersionConflictEngineException.class)
            );
            case REJECTED -> asException(
                org.opensearch.ExceptionsHelper.unwrap(cause, org.opensearch.core.concurrency.OpenSearchRejectedExecutionException.class)
            );
            case REMOTE -> org.opensearch.serverless.transport.ForwardFailure.isClientStatus(cause)
                ? asException(org.opensearch.serverless.transport.ForwardFailure.answer(cause))
                : new WriteRefusal(
                    RestStatus.SERVICE_UNAVAILABLE,
                    "forward_failed",
                    org.opensearch.serverless.transport.ForwardFailure.describe(owner, cause)
                );
            default -> cause instanceof org.opensearch.serverless.transport.OwnerUnreachableException
                ? new WriteRefusal(RestStatus.SERVICE_UNAVAILABLE, "owner_unreachable", cause.getMessage())
                : new WriteRefusal(
                    RestStatus.SERVICE_UNAVAILABLE,
                    "forward_failed",
                    org.opensearch.serverless.transport.ForwardFailure.describe(owner, cause)
                );
        };
    }

    private void planWrite(
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
        final Optional<MetadataPlane.WriteTarget> target = metadata.writeTarget(index);
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
                final var outcome = serving.ingestPipelines()
                    .run(serving.ingestPipelines().compile(ran, stored.get()), writtenIndex, id, shaped);
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
                org.opensearch.common.lease.Releasable inFlight = serving.indexingPressure()
                    .markCoordinatingOperationStarted(inFlightBytes, false)
            ) {
                final ServerlessNode.WriteOutcome outcome = deletion
                    ? serving.delete(into, id, ifSeqNo, ifPrimaryTerm)
                    : serving.index(into, id, shapedSource, ifSeqNo, ifPrimaryTerm, requireAbsent);
                boolean refreshed = false;
                if (refresh) {
                    final var open = serving.reconciler().shard(into);
                    if (open != null) {
                        serving.refreshAfterMutation(open, "serverless-rest-refresh");
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
            final Optional<String> hinted = serving.ownerHint(writtenIndex, shard);
            final String owner;
            if (hinted.isPresent()) {
                owner = hinted.get();
            } else {
                final var head = metadata.heads().read(writtenIndex, shard);
                serving.noteHead(writtenIndex, shard, head.orElse(null));
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
                serving.forgetOwner(writtenIndex, shard);
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
                        org.opensearch.common.lease.Releasable forwarded = serving.indexingPressure()
                            .markCoordinatingOperationStarted(inFlightBytes, false)
                    ) {
                        String forwardTarget = owner;
                        org.opensearch.serverless.transport.ForwardedIndexResponse ack;
                        try {
                            ack = forwardTo(
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
                            serving.forgetOwner(writtenIndex, shard);
                            if (org.opensearch.serverless.transport.ForwardFailure.classify(first.getCause())
                                .mayRetryElsewhere() == false) {
                                // Applied, possibly applied, or answered: not sent anywhere else.
                                throw first;
                            }
                            // The owner never applied it. Read the register once and forward to whoever it names.
                            final var head = metadata.heads().read(writtenIndex, shard);
                            serving.noteHead(writtenIndex, shard, head.orElse(null));
                            final String fresh = head.map(h -> h.ownerNodeId()).orElse(null);
                            if (fresh == null) {
                                // Whoever the hint named is gone and nobody has taken over: the same state,
                                // and the same answer, as a register that never named anyone.
                                serving.signals().ownershipDoubted(writtenIndex, shard);
                                answer.onFailure(notWriterRefusal(writtenIndex, shard, null));
                                return;
                            }
                            if (fresh.equals(serving.localNode().getId())) {
                                // Named here with nothing open: see ShardOperations#notHere for why this doubts.
                                serving.signals().ownershipDoubted(writtenIndex, shard);
                                answer.onFailure(activationRefusal(writtenIndex, shard));
                                return;
                            }
                            if (fresh.equals(forwardTarget)) {
                                // Still the owner that did not answer. If its lease has run out, the write goes to the
                                // member that takes its shards over, once, marked to be taken there -- not to a doubt
                                // here that queued the shard on every survivor a write reached. Anything short of an
                                // answer from there falls back to taking it here.
                                if (serving.routesTakeovers()
                                    && head.isPresent()
                                    && metadata.heads().isHeld(head.get(), metadata.clock().getAsLong()) == false) {
                                    final Optional<String> winner = serving.takeoverWinner(metadata, writtenIndex, shard, fresh);
                                    if (winner.isPresent() && winner.get().equals(serving.localNode().getId()) == false) {
                                        try {
                                            ack = forwardTo(
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
                                            serving.noteTakeoverWriteRouted();
                                            answer.onResponse(
                                                WriteResult.written(
                                                    new ShardId(writtenIndex, descriptor.get().uuid(), shard),
                                                    id,
                                                    ack.ownerNodeId(),
                                                    deletion,
                                                    new ServerlessNode.WriteOutcome(
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
                            ack = forwardTo(
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
                                new ServerlessNode.WriteOutcome(ack.seqNo(), ack.primaryTerm(), ack.version(), ack.created(), ack.found()),
                                refresh
                            )
                        );
                    } catch (ForwardFailed failed) {
                        answer.onFailure(classifyForwardFailure(serving, writtenIndex, shard, failed.target(), failed.getCause()));
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
            final Optional<String> roomy = serving.handOffTarget(metadata);
            if (roomy.isPresent()) {
                serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
                    try (
                        org.opensearch.common.lease.Releasable forwarded = serving.indexingPressure()
                            .markCoordinatingOperationStarted(inFlightBytes, false)
                    ) {
                        final var ack = forwardTo(
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
                        serving.noteWriteSteered();
                        answer.onResponse(
                            WriteResult.written(
                                new ShardId(writtenIndex, descriptor.get().uuid(), shard),
                                id,
                                ack.ownerNodeId(),
                                deletion,
                                new ServerlessNode.WriteOutcome(ack.seqNo(), ack.primaryTerm(), ack.version(), ack.created(), ack.found()),
                                refresh
                            )
                        );
                    } catch (ForwardFailed failed) {
                        if (org.opensearch.serverless.transport.ForwardFailure.classify(failed.getCause()).mayRetryElsewhere() == false) {
                            // Applied, possibly applied, or answered: not written anywhere else.
                            answer.onFailure(classifyForwardFailure(serving, writtenIndex, shard, failed.target(), failed.getCause()));
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
    private void writeOnceActivated(
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
            answer.onFailure(activationRefusal(writtenIndex, shard));
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
        answer.onFailure(ownedHere ? activationRefusal(writtenIndex, shard) : notWriterRefusal(writtenIndex, shard, null));
    }

    private void respond(
        org.opensearch.rest.RestChannel channel,
        String index,
        String id,
        int shard,
        String writtenBy,
        boolean deletion,
        ServerlessNode.WriteOutcome outcome,
        boolean refreshed
    ) throws IOException {
        {
            final boolean found = outcome.found();
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("_index", index);
                builder.field("_id", id);
                builder.field("_shard", shard);
                builder.field("_version", outcome.version());
                // "created" versus "updated", told apart honestly rather than always claiming the first.
                // The engine has always known which it was -- IndexResult#isCreated -- and this path
                // simply never asked, so an overwrite reported itself as a creation and answered 201.
                builder.field("result", deletion ? (found ? "deleted" : "not_found") : (outcome.created() ? "created" : "updated"));
                // One shard, always: this write touched exactly the one it was routed to. Real OpenSearch's
                // field, added for a client that reads it rather than only checking the HTTP status.
                builder.startObject("_shards");
                builder.field("total", 1);
                builder.field("successful", 1);
                builder.field("failed", 0);
                builder.endObject();
                // The two numbers a caller sends back as if_seq_no and if_primary_term. They were always
                // being assigned; this path used to discard them.
                builder.field("_seq_no", outcome.seqNo());
                builder.field("_primary_term", outcome.primaryTerm());
                // Says what was actually guaranteed. "created" alone would leave a reader to assume the
                // usual meaning; here the write is in the log before this response exists, and the
                // segment it will live in may not be published yet.
                // Says what was actually guaranteed, and it matters most for a deletion: the tombstone is
                // in the log before this response exists, so a successor replaying that log removes the
                // document again rather than resurrecting it.
                builder.field("durable", "write-ahead log");
                if (refreshed) {
                    // Core's own flag for "this write was made visible before answering".
                    builder.field("forced_refresh", true);
                }
                // Which node actually holds the shard. Useful when the write was forwarded, and never
                // misleading when it was not.
                builder.field("_node", writtenBy);
                builder.endObject();
                channel.sendResponse(
                    new BytesRestResponse(
                        deletion
                            ? (found ? RestStatus.OK : RestStatus.NOT_FOUND)
                            : (outcome.created() ? RestStatus.CREATED : RestStatus.OK),
                        builder
                    )
                );
            }
        }
    }

    /**
     * A forward that failed, and which node it was sent to.
     *
     * <p>Only transport failures receive this wrapper. A filter refusal occurs outside forwarding and
     * cannot be mistaken for a stale owner hint or an instruction to retry elsewhere.
     */
    private static final class ForwardFailed extends RuntimeException {

        private final String target;

        ForwardFailed(String target, Throwable cause) {
            super(cause.getMessage(), cause);
            this.target = target;
        }

        String target() {
            return target;
        }
    }

    /**
     * Whether a forward failed because the node it reached does not hold the shard as its writer.
     *
     * <p>Typed, not phrased: the check is {@link org.opensearch.serverless.transport.NotShardOwnerException},
     * which the owner throws and which carries a token no other message does. This used to search every
     * cause's message for "does not own", so a security plugin refusing with "user does not own index
     * alpha" re-routed the write instead of reaching the caller as a 403.
     *
     * @param e what the forward threw
     * @return true for the owner's own refusal
     */
    public static boolean refusedAsNotOwner(Exception e) {
        return org.opensearch.serverless.transport.NotShardOwnerException.describes(e);
    }

    private static Exception asException(Throwable failure) {
        return failure instanceof Exception e ? e : new RuntimeException(failure);
    }

    private org.opensearch.serverless.transport.ForwardedIndexResponse forwardTo(
        ServerlessNode serving,
        String owner,
        String writtenIndex,
        String indexUuid,
        int shard,
        String id,
        String shapedSource,
        boolean refresh,
        boolean deletion,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent
    ) throws Exception {
        return forwardTo(
            serving,
            owner,
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
            false
        );
    }

    private org.opensearch.serverless.transport.ForwardedIndexResponse forwardTo(
        ServerlessNode serving,
        String owner,
        String writtenIndex,
        String indexUuid,
        int shard,
        String id,
        String shapedSource,
        boolean refresh,
        boolean deletion,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent,
        boolean steered
    ) throws Exception {
        final Optional<org.opensearch.cluster.node.DiscoveryNode> peer;
        try {
            peer = serving.router().peer(owner);
        } catch (Exception e) {
            // Resolving the peer connects to it, so this fails as readily as the forward itself does, and
            // means the same thing.
            throw new ForwardFailed(owner, e);
        }
        if (peer.isEmpty()) {
            // The head names an owner that holds no live lease. That is precisely a dead writer nobody
            // has noticed yet, and this request is the first thing in the system to prove it.
            throw new ForwardFailed(
                owner,
                new org.opensearch.serverless.transport.OwnerUnreachableException(
                    "shard " + shard + " is owned by " + owner + ", which has no reachable lease"
                )
            );
        }
        try {
            final var request = new org.opensearch.serverless.transport.ForwardedIndexRequest(
                writtenIndex,
                indexUuid,
                shard,
                id,
                shapedSource == null ? "" : shapedSource,
                refresh,
                deletion,
                ifSeqNo,
                ifPrimaryTerm,
                requireAbsent
            );
            return serving.router().forwardIndex(peer.get(), steered ? request.steered() : request);
        } catch (Exception e) {
            throw new ForwardFailed(owner, e);
        }
    }
}
