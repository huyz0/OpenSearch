/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.rest.DocumentWriteOutcomes.WriteRefusal;
import org.opensearch.serverless.rest.DocumentWriteOutcomes.WriteResult;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.shell.WriteOutcome;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code PUT|POST /{index}/_doc/{id}} — the durable write path, reachable by a user.
 *
 * <p>A write here reaches the write-ahead log before it is acknowledged, so it survives the writer dying before the next publication.
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
 * token stays meaningful across one.
 *
 * <p><b>{@code version} is still refused</b>, and the difference matters: external versioning asks this
 * system to order writes by a number the caller maintains and this system does not keep, which is a
 * different feature from comparing against a number the engine itself assigned.
 */
public final class DocumentHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;
    private final DocumentWritePlanner writes;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public DocumentHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
        this.writes = new DocumentWritePlanner(node, plane);
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
        if (serving.services().actionGate().isEmpty()) {
            writes.planWrite(
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
                completed
            );
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
        serving.services()
            .actionGate()
            .runAsync(
                serving.threadPool(),
                deletion ? org.opensearch.action.delete.DeleteAction.NAME : org.opensearch.action.index.IndexAction.NAME,
                request,
                (admitted, listener) -> writes.planAcceptedWrite(metadata, serving, deletion, admitted, listener),
                DocumentWriteOutcomes.writeView(deletion),
                completed
            );
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

    private void respond(
        org.opensearch.rest.RestChannel channel,
        String index,
        String id,
        int shard,
        String writtenBy,
        boolean deletion,
        WriteOutcome outcome,
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
        return DocumentWriteOutcomes.refusedAsNotOwner(e);
    }

}
