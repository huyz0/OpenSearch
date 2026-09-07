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
        if (deletion == false && (source == null || source.isBlank())) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a document body is required")
            );
        }

        // An index by name, or a data stream's newest backing index. The response reports the backing
        // index, as core does -- {@code .ds-logs-000001} for a write through {@code logs} -- on the local
        // path and the forwarded path alike. The comment here used to claim the caller's name, the local
        // path agreed with it and the forwarded path did not.
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
        final ServerlessNode serving = node.get();
        if (serving == null) {
            // Checked once, here, rather than checked at one use and dereferenced at the next.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "node_not_ready", "no node is serving requests yet")
            );
        }
        if (serving.isSystemIndex(writtenIndex)) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.FORBIDDEN,
                    "system_index",
                    "[" + writtenIndex + "] belongs to a plugin and is not reachable through the request path"
                )
            );
        }

        // The pipelines run here: after the index is resolved, because index.default_pipeline and
        // index.final_pipeline belong to the index actually written to -- a data stream's backing index,
        // not the name the caller used -- and before anything is routed or written, because a caller must
        // never be told a write succeeded against a document the pipeline was supposed to change and did
        // not. A dropped document is not an error and not a write.
        final java.util.List<String> pipelines = deletion
            ? java.util.List.of()
            : org.opensearch.serverless.ingest.IngestPipelines.pipelinesFor(pipeline, descriptor.get().extraSettings());
        String shaped = source;
        boolean dropped = false;
        String droppedBy = null;
        for (String ran : pipelines) {
            try {
                final var stored = metadata.pipelines().get(ran);
                if (stored.isEmpty()) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "pipeline_missing", "no such pipeline: " + ran)
                    );
                }
                final var outcome = serving.ingestPipelines()
                    .run(serving.ingestPipelines().compile(ran, stored.get()), writtenIndex, id, shaped);
                if (outcome.dropped()) {
                    // A drop ends the document, so a final pipeline does not run over one that is not going
                    // to be written. Core does the same.
                    dropped = true;
                    droppedBy = ran;
                    break;
                }
                shaped = outcome.source();
            } catch (Exception e) {
                final String reason = e.getMessage() == null ? e.toString() : e.getMessage();
                return channel -> channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "pipeline_failed", reason));
            }
        }
        if (dropped) {
            // Reported as what it is. Classic OpenSearch answers "noop" for a dropped document, and a
            // caller that cannot tell "dropped" from "written" cannot tell whether its pipeline works.
            final String by = droppedBy;
            return channel -> {
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    builder.field("_index", writtenIndex);
                    builder.field("_id", id);
                    builder.field("result", "noop");
                    builder.field("dropped_by_pipeline", by);
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
            };
        }
        final String shapedSource = shaped;

        final int shard = DocumentRouting.shardFor(descriptor.get(), id);
        final ShardId shardId = serving.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(writtenIndex) && s.id() == shard && s.getIndex().getUUID().equals(descriptor.get().uuid()))
            .findFirst()
            .orElse(null);

        if (shardId == null || serving.reconciler().readerShards().contains(shardId)) {
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
                serving.forgetOwner(writtenIndex, shard);
                serving.signals().ownershipDoubted(writtenIndex, shard);
                return channel -> channel.sendResponse(activationInProgress(channel, writtenIndex, shard));
            }
            if (owner != null) {
                // Forward rather than refuse. The client should not have to know which node owns which
                // shard; that is exactly the knowledge the shard-head exists to hold.
                final String indexUuid = descriptor.get().uuid();
                return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
                    // Accounted on this node for as long as the forward is in flight, exactly as a local
                    // write is: the bytes are held here until the owner answers, whichever node applies them.
                    try (
                        org.opensearch.common.lease.Releasable forwarded = serving.indexingPressure()
                            .markCoordinatingOperationStarted(shapedSource == null ? 0L : shapedSource.length(), false)
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
                                channel.sendResponse(notTheWriter(channel, writtenIndex, shard, null));
                                return;
                            }
                            if (fresh.equals(serving.localNode().getId())) {
                                channel.sendResponse(activationInProgress(channel, writtenIndex, shard));
                                return;
                            }
                            if (fresh.equals(forwardTarget)) {
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
                    } catch (ForwardFailed failed) {
                        reportForwardFailure(channel, serving, writtenIndex, shard, failed.target(), failed.getCause());
                    } catch (Exception e) {
                        // Not the forward's failure: a filter refused the write before it left this node,
                        // or the answer could not be written. Rendered with the plugin's own status, as the
                        // local path renders it. This used to be a 503 forward_failed that also cast doubt
                        // on ownership, so whether a caller was told 403 or 503 depended on which node it
                        // happened to reach.
                        try {
                            channel.sendResponse(IndexAdminHandler.failure(channel, e));
                        } catch (IOException nested) {
                            logger.error("failed to report a refused forward", nested);
                        }
                    }
                });
            }
            // No owner at all. Nothing will fix this except some node activating the shard, and the
            // only reason anyone would is that a write arrived -- which just happened.
            serving.signals().ownershipDoubted(writtenIndex, shard);
            return channel -> channel.sendResponse(notTheWriter(channel, writtenIndex, shard, owner));
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
                boolean refreshed = false;
                if (refresh) {
                    // Same meaning as classic OpenSearch: make this write visible to search before
                    // answering. Without it a caller that writes and immediately searches gets zero
                    // hits and no error, which reads as data loss and is not. Null-checked: a heartbeat
                    // can release the shard between the durable append and this refresh, and a 500 for a
                    // write that is in the log told the client the opposite of the truth.
                    final var open = serving.reconciler().shard(shardId);
                    if (open != null) {
                        open.refresh("serverless-rest-refresh");
                        refreshed = true;
                    }
                }
                // The backing index, as core reports it and as the forwarded path has always reported it.
                respond(channel, writtenIndex, id, shard, serving.localNode().getId(), deletion, outcome, refreshed);
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

    /**
     * A forward that failed, and which node it was sent to.
     *
     * <p>A {@code RuntimeException} so the action gate hands it back unwrapped -- a checked exception
     * thrown inside the gate comes out as an {@code UncategorizedExecutionException} when a filter is
     * installed and as itself when none is, and a classification that depended on that would depend on
     * which plugins happen to be loaded.
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

    /** The 421 for a shard this node does not own, naming the owner when there is one. */
    private static BytesRestResponse notTheWriter(org.opensearch.rest.RestChannel channel, String index, int shard, String owner)
        throws IOException {
        // Through the shared error helper, so this renders the same nested "error" object every other
        // deliberate refusal on this surface does. The owner is still named: a bare "wrong node" makes the
        // client guess, and guessing at ownership is how two writers end up believing the same thing.
        final String reason = owner == null
            ? "no node currently owns shard " + shard + " of " + index + "; activate it before writing"
            : "this node does not own shard " + shard + " of " + index + "; it is owned by " + owner;
        return IndexAdminHandler.error(channel, RestStatus.MISDIRECTED_REQUEST, "not_the_writer", reason);
    }

    /** The 503 for the gap inside activation, which is brief and self-resolving. */
    private static BytesRestResponse activationInProgress(org.opensearch.rest.RestChannel channel, String index, int shard)
        throws IOException {
        return IndexAdminHandler.error(
            channel,
            RestStatus.SERVICE_UNAVAILABLE,
            "activation_in_progress",
            "this node is acquiring shard " + shard + " of " + index + "; retry"
        );
    }

    /**
     * Answers for a forward that failed, with the status the failure actually deserves.
     *
     * <p>Every failure used to be a 503 {@code forward_failed} that also cast doubt on ownership. An
     * owner at its indexing-pressure limit answered 429 to a local write and 503 to a forwarded one, and
     * cost the coordinator an activation pass for saying so; a mapper's 400 became a "retry". Now the
     * owner's own answer keeps its status, and only a refusal, a silence or an absence is doubt.
     */
    private void reportForwardFailure(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        String writtenIndex,
        int shard,
        String owner,
        Throwable cause
    ) {
        final org.opensearch.serverless.transport.ForwardFailure kind = org.opensearch.serverless.transport.ForwardFailure.classify(cause);
        if (kind.castsDoubtOnOwnership()) {
            serving.signals().ownershipDoubted(writtenIndex, shard);
        }
        try {
            switch (kind) {
                case CONFLICT: {
                    final Throwable conflict = org.opensearch.ExceptionsHelper.unwrap(
                        cause,
                        org.opensearch.index.engine.VersionConflictEngineException.class
                    );
                    channel.sendResponse(
                        IndexAdminHandler.error(channel, RestStatus.CONFLICT, "version_conflict_engine_exception", conflict.getMessage())
                    );
                    return;
                }
                case REJECTED: {
                    // Owner-side back-pressure, in core's own shape and with core's own 429.
                    final Throwable rejected = org.opensearch.ExceptionsHelper.unwrap(
                        cause,
                        org.opensearch.core.concurrency.OpenSearchRejectedExecutionException.class
                    );
                    channel.sendResponse(IndexAdminHandler.failure(channel, asException(rejected)));
                    return;
                }
                case REMOTE: {
                    if (org.opensearch.serverless.transport.ForwardFailure.isClientStatus(cause)) {
                        // The owner's own answer, with the owner's own status.
                        channel.sendResponse(
                            IndexAdminHandler.failure(
                                channel,
                                asException(org.opensearch.serverless.transport.ForwardFailure.answer(cause))
                            )
                        );
                        return;
                    }
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.SERVICE_UNAVAILABLE,
                            "forward_failed",
                            org.opensearch.serverless.transport.ForwardFailure.describe(owner, cause)
                        )
                    );
                    return;
                }
                default: {
                    // 503, not the exception's own status. A forward that fails means routing was stale,
                    // and stale routing is a retry -- rendering it as a 500 tells a client the write is
                    // hopeless when the correct answer is "ask again in a moment". A deadline is worded
                    // as one, so the client reads before it retries.
                    final boolean noLease = cause instanceof org.opensearch.serverless.transport.OwnerUnreachableException;
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.SERVICE_UNAVAILABLE,
                            noLease ? "owner_unreachable" : "forward_failed",
                            noLease ? cause.getMessage() : org.opensearch.serverless.transport.ForwardFailure.describe(owner, cause)
                        )
                    );
                }
            }
        } catch (IOException nested) {
            logger.error("failed to report a forwarding failure", nested);
        }
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
        // Filtered here, on the coordinating node, before the write leaves it. The owner has no idea who
        // the caller is -- an internal transport request carries no identity -- so this is the only node
        // where the question can be asked. The forward's own failure is wrapped inside the gate, so that
        // what comes out of the gate unwrapped is the gate's own refusal and nothing else.
        return gated(serving, deletion, writtenIndex, id, shapedSource, () -> {
            try {
                return serving.router()
                    .forwardIndex(
                        peer.get(),
                        new org.opensearch.serverless.transport.ForwardedIndexRequest(
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
                        )
                    );
            } catch (Exception e) {
                throw new ForwardFailed(owner, e);
            }
        });
    }
}
