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
        request.param("timeout");
        request.param("wait_for_active_shards");

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
        // The pipeline runs here, before anything is routed or written: a document is shaped and then
        // stored, and a caller must never be told a write succeeded against a document the pipeline was
        // supposed to change and did not. A dropped document is not an error and not a write.
        String shaped = source;
        boolean dropped = false;
        if (deletion == false && pipeline != null && source != null) {
            final var serving0 = node.get();
            final var plane0 = plane.get();
            if (serving0 != null && plane0 != null) {
                try {
                    final var stored = plane0.pipelines().get(pipeline);
                    if (stored.isEmpty()) {
                        return channel -> channel.sendResponse(
                            IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "pipeline_missing", "no such pipeline: " + pipeline)
                        );
                    }
                    final var compiled = serving0.ingestPipelines().compile(pipeline, stored.get());
                    final var outcome = serving0.ingestPipelines().run(compiled, index, id, source);
                    dropped = outcome.dropped();
                    shaped = outcome.source();
                } catch (Exception e) {
                    final String reason = e.getMessage() == null ? e.toString() : e.getMessage();
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "pipeline_failed", reason)
                    );
                }
            }
        }
        if (dropped) {
            // Reported as what it is. Classic OpenSearch answers "noop" for a dropped document, and a
            // caller that cannot tell "dropped" from "written" cannot tell whether its pipeline works.
            return channel -> {
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    builder.field("_index", index);
                    builder.field("_id", id);
                    builder.field("result", "noop");
                    builder.field("dropped_by_pipeline", pipeline);
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
            };
        }
        final String shapedSource = shaped;

        if (deletion == false && (shapedSource == null || shapedSource.isBlank())) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a document body is required")
            );
        }

        // An index by name, or a data stream's newest backing index. The name the caller wrote is the one
        // the response reports, as core reports it; the routing is the backing index's.
        final Optional<MetadataPlane.WriteTarget> target = metadata.writeTarget(index);
        if (target.isEmpty()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index)
            );
        }
        if (target.get().dataStream() != null && (deletion || requireAbsent == false)) {
            // Core's own rule: a data stream is append-only through its name, and anything else addresses
            // the backing index directly.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    "only write ops with an op_type of create are allowed in data streams; address the backing index ["
                        + target.get().index().name()
                        + "] for anything else"
                )
            );
        }
        final Optional<IndexDescriptor> descriptor = Optional.of(target.get().index());
        final String writtenIndex = descriptor.get().name();
        if (node.get() != null && node.get().isSystemIndex(writtenIndex)) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.FORBIDDEN,
                    "system_index",
                    "[" + writtenIndex + "] belongs to a plugin and is not reachable through the request path"
                )
            );
        }

        final int shard = DocumentRouting.shardFor(descriptor.get(), id);
        final ServerlessNode serving = node.get();
        final ShardId shardId = serving.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(writtenIndex) && s.id() == shard && s.getIndex().getUUID().equals(descriptor.get().uuid()))
            .findFirst()
            .orElse(null);

        if (shardId == null || serving.reconciler().readerShards().contains(shardId)) {
            final var head = metadata.heads().read(writtenIndex, shard);
            final String owner = head.map(h -> h.ownerNodeId()).orElse(null);
            if (owner != null && owner.equals(serving.localNode().getId())) {
                // The head names this node and this node has no open shard. That is not a routing
                // problem, it is the gap inside activation: activateWriter wins the compare-and-swap
                // before it opens the shard, so for a moment the head is true and the node is not ready.
                //
                // Forwarding here would send the write to ourselves, and the receiving side would
                // correctly refuse it -- a 500 for what is a normal, brief, self-resolving state. Say
                // "not yet" instead, which is a status a client retries.
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.SERVICE_UNAVAILABLE,
                        "activation_in_progress",
                        "this node is acquiring shard " + shard + " of " + writtenIndex + "; retry"
                    )
                );
            }
            if (owner != null) {
                // Forward rather than refuse. The client should not have to know which node owns which
                // shard; that is exactly the knowledge the shard-head exists to hold.
                return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
                    // Accounted on this node for as long as the forward is in flight, exactly as a local
                    // write is: the bytes are held here until the owner answers, whichever node applies them.
                    try (
                        org.opensearch.common.lease.Releasable forwarded = serving.indexingPressure()
                            .markCoordinatingOperationStarted(shapedSource == null ? 0L : shapedSource.length(), false)
                    ) {
                        final var peer = serving.router().peer(owner);
                        if (peer.isEmpty()) {
                            // The head names an owner that holds no live lease. That is precisely a
                            // dead writer nobody has noticed yet, and this request is the first thing
                            // in the system to prove it -- so say so rather than waiting for a timer.
                            serving.signals().ownershipDoubted(writtenIndex, shard);
                            channel.sendResponse(
                                IndexAdminHandler.error(
                                    channel,
                                    RestStatus.SERVICE_UNAVAILABLE,
                                    "owner_unreachable",
                                    "shard " + shard + " is owned by " + owner + ", which has no reachable lease"
                                )
                            );
                            return;
                        }
                        // Filtered here, on the coordinating node, before the write leaves it. The owner
                        // has no idea who the caller is -- an internal transport request carries no
                        // identity -- so this is the only node where the question can be asked.
                        final var ack = gated(
                            serving,
                            deletion,
                            writtenIndex,
                            id,
                            shapedSource,
                            () -> serving.router()
                                .forwardIndex(
                                    peer.get(),
                                    new org.opensearch.serverless.transport.ForwardedIndexRequest(
                                        writtenIndex,
                                        shard,
                                        id,
                                        shapedSource == null ? "" : shapedSource,
                                        refresh,
                                        deletion,
                                        ifSeqNo,
                                        ifPrimaryTerm,
                                        requireAbsent
                                    )
                                )
                        );
                        respond(
                            channel,
                            writtenIndex,
                            id,
                            shard,
                            ack.ownerNodeId(),
                            deletion,
                            new ServerlessNode.WriteOutcome(ack.seqNo(), ack.primaryTerm(), ack.version(), ack.created(), ack.found()),
                            refresh
                        );
                    } catch (Exception e) {
                        // A lost compare-and-swap is not a routing problem, and must not be dressed as
                        // one. The owner answered, correctly, that the document had moved on; reporting
                        // that as a 503 would tell the caller to retry an operation whose whole point is
                        // that it must not be retried blindly -- and would cast doubt on an ownership
                        // that was never in question. It arrives wrapped in a transport exception, so it
                        // has to be unwrapped before it can be recognised.
                        final Throwable conflict = org.opensearch.ExceptionsHelper.unwrap(
                            e,
                            org.opensearch.index.engine.VersionConflictEngineException.class
                        );
                        if (conflict != null) {
                            try {
                                channel.sendResponse(
                                    IndexAdminHandler.error(
                                        channel,
                                        RestStatus.CONFLICT,
                                        "version_conflict_engine_exception",
                                        conflict.getMessage()
                                    )
                                );
                            } catch (IOException nested) {
                                logger.error("failed to report a forwarded version conflict", nested);
                            }
                            return;
                        }
                        // The owner was reachable and still refused or failed. Either it lost the shard
                        // between our read and its receipt, or it is going away. Same conclusion: the
                        // head we routed on is not to be trusted.
                        serving.signals().ownershipDoubted(writtenIndex, shard);
                        try {
                            // 503, not the exception's own status. A forward that fails means routing was
                            // stale, and stale routing is a retry -- rendering it as a 500 tells a client
                            // the write is hopeless when the correct answer is "ask again in a moment".
                            // The cause is carried in the message so nothing is hidden by saying so.
                            channel.sendResponse(
                                IndexAdminHandler.error(
                                    channel,
                                    RestStatus.SERVICE_UNAVAILABLE,
                                    "forward_failed",
                                    IndexAdminHandler.forwardFailureMessage(owner, e)
                                )
                            );
                        } catch (IOException nested) {
                            logger.error("failed to report a forwarding failure", nested);
                        }
                    }
                });
            }
            // No owner at all. Nothing will fix this except some node activating the shard, and the
            // only reason anyone would is that a write arrived -- which just happened.
            serving.signals().ownershipDoubted(writtenIndex, shard);
            // Through the shared error helper, so this renders the same nested "error" object every other
            // deliberate refusal on this surface does. It used to build its own body with "error" as a bare
            // string -- the exact shape M47 removed everywhere else, missed here because this one path
            // builds its response by hand. The owner is still named: a bare "wrong node" makes the client
            // guess, and guessing at ownership is how two writers end up believing the same thing.
            final String reason = owner == null
                ? "no node currently owns shard " + shard + " of " + writtenIndex + "; activate it before writing"
                : "this node does not own shard " + shard + " of " + writtenIndex + "; it is owned by " + owner;
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.MISDIRECTED_REQUEST, "not_the_writer", reason)
            );
        }

        // Off the HTTP thread: a WAL append is an object-store write, and blocking the thread that
        // should be reading the next request on remote IO is how a node stops answering under load.
        // A single write is accounted the same way a batch is, for the same reason: a flood of large
        // documents is a flood of large documents whether or not they arrived together.
        final long inFlightBytes = shapedSource == null ? 0L : shapedSource.length();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
            try (
                org.opensearch.common.lease.Releasable inFlight = serving.indexingPressure()
                    .markCoordinatingOperationStarted(inFlightBytes, false)
            ) {
                // The document as it will be written, pipeline applied -- the same one the forwarded path
                // shows its filters. A redaction filter that saw the pre-pipeline source here inspected a
                // different document from the one that landed, depending on which node the request reached.
                final ServerlessNode.WriteOutcome outcome = gated(
                    serving,
                    deletion,
                    writtenIndex,
                    id,
                    shapedSource,
                    () -> deletion
                        ? serving.delete(shardId, id, ifSeqNo, ifPrimaryTerm)
                        : serving.index(shardId, id, shapedSource, ifSeqNo, ifPrimaryTerm, requireAbsent)
                );
                if (refresh) {
                    // Same meaning as classic OpenSearch: make this write visible to search before
                    // answering. Without it a caller that writes and immediately searches gets zero
                    // hits and no error, which reads as data loss and is not.
                    serving.reconciler().shard(shardId).refresh("serverless-rest-refresh");
                }
                respond(channel, index, id, shard, serving.localNode().getId(), deletion, outcome, refresh);
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a write failure", nested);
                }
            }
        });
    }

    /**
     * Runs a write through the plugins' action filters.
     *
     * <p><b>Here rather than in {@code ShardOperations}, and that is worth explaining.</b> This handler
     * predates that class and still carries its own routing, forwarding and response vocabulary — a 421
     * naming the owner, a {@code durable} field, the node that actually wrote. Rewriting it onto the shared
     * operations would change what callers see, so the gate comes to it instead. What keeps that from
     * becoming a place somebody forgets is a test that walks every endpoint which reads or changes data and
     * asserts an action name reached a filter for each one.
     */
    private static <T> T gated(
        ServerlessNode serving,
        boolean deletion,
        String index,
        String id,
        String source,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws Exception {
        final org.opensearch.action.ActionRequest request;
        final String action;
        if (deletion) {
            action = org.opensearch.action.delete.DeleteAction.NAME;
            request = new org.opensearch.action.delete.DeleteRequest(index, id);
        } else {
            action = org.opensearch.action.index.IndexAction.NAME;
            request = new org.opensearch.action.index.IndexRequest(index).id(id)
                .source(source == null ? "{}" : source, org.opensearch.common.xcontent.XContentType.JSON);
        }
        return serving.actionGate().run(action, request, work);
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
}
