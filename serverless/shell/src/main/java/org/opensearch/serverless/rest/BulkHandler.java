/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.action.ActionGate;
import org.opensearch.serverless.documents.BulkBodyParser;
import org.opensearch.serverless.documents.BulkDispatch;
import org.opensearch.serverless.documents.BulkItem;
import org.opensearch.serverless.documents.BulkMemoryBudget;
import org.opensearch.serverless.documents.BulkNative;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.node.BulkSettings;
import org.opensearch.serverless.node.NodeContext;
import org.opensearch.serverless.ops.DocumentRouting;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@code POST /_bulk} and {@code POST /{index}/_bulk} — the write path priced per request.
 *
 * <p>Until this existed a document cost one HTTP request and one object-store PUT, and that second
 * number was the one that mattered: the cost tests measured exactly one write per document, and the log
 * itself carried a note saying group commit belonged there and was not built. This is that, end to end.
 * A batch landing on one shard becomes <b>one</b> log append, and a batch crossing the network stays a
 * batch on the far side rather than being unrolled into single writes that would put the cost back.
 *
 * <p><b>Grouped by shard, because that is what a batch can be.</b> Items are sorted into (index, shard)
 * groups, each group is applied or forwarded once, and the outcomes are reassembled into request order.
 * A single request may therefore touch several shards on several nodes and still cost one append per
 * shard.
 *
 * <p><b>Unsupported actions are refused per item.</b> {@code create} uses the engine's fail-if-present
 * operation. A partial {@code update} requires reading the current document and is refused in a batch;
 * callers can use the single-document update endpoint. Versioning and condition fields are validated
 * before dispatch so they cannot silently become unconditional writes.
 *
 * <p><b>Groups are dispatched together.</b> A batch spanning three shards is one round trip's worth of
 * latency rather than three, because paying per shard instead of per document is the same mistake batching
 * exists to remove, one level up.
 *
 * <p>This paragraph said the opposite for some time after it stopped being true — the dispatch had been
 * made concurrent and the class documentation had not — and nothing asserted either version, so the code
 * and its description could have disagreed indefinitely.
 * {@code ServerlessBulkTests#testABatchSpanningShardsCostsOneWrite} settles it by counting rather than timing:
 * every shard group joins one node-log window only when they run at once, since a group run after another
 * starts only once the other's blob has landed.
 */
public final class BulkHandler extends BaseRestHandler {

    private final Supplier<NodeContext> node;
    private final Supplier<MetadataPlane> plane;
    private final BulkDispatch dispatcher;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public BulkHandler(Supplier<NodeContext> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
        this.dispatcher = new BulkDispatch(node, plane);
    }

    @Override
    public String getName() {
        return "serverless_bulk_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, "/_bulk"),
            new Route(RestRequest.Method.PUT, "/_bulk"),
            new Route(RestRequest.Method.POST, "/{index}/_bulk"),
            new Route(RestRequest.Method.PUT, "/{index}/_bulk")
        );
    }

    /**
     * The bytes a batch of this length is charged up front.
     *
     * @param contentLength the body's length
     * @param settings the node settings
     * @return the charge
     */
    static long upFrontCharge(long contentLength, org.opensearch.common.settings.Settings settings) {
        final double factor = Math.max(1.0, BulkSettings.MEMORY_FACTOR.get(settings));
        return (long) Math.min(Long.MAX_VALUE / 2.0, contentLength * factor);
    }

    /** A charge released once, by whichever of the request's paths ends it. */
    static final class Charge implements org.opensearch.common.lease.Releasable {
        private final java.util.concurrent.atomic.AtomicReference<org.opensearch.common.lease.Releasable> held;
        /** Set once the batch's own work owns the charge; until then the refusals that return early release it. */
        volatile boolean handedOff;

        Charge(org.opensearch.common.lease.Releasable held) {
            this.held = new java.util.concurrent.atomic.AtomicReference<>(held);
        }

        @Override
        public void close() {
            final org.opensearch.common.lease.Releasable releasing = held.getAndSet(null);
            if (releasing != null) {
                releasing.close();
            }
        }
    }

    private volatile BulkMemoryBudget budget;

    /**
     * Whether a node with these settings streams batches.
     *
     * @param settings the node settings
     * @return true to stream
     */
    public static boolean streams(org.opensearch.common.settings.Settings settings) {
        return BulkSettings.STREAMING.get(settings);
    }

    @Override
    public boolean supportsStreaming() {
        final NodeContext serving = node.get();
        // A node with action filters runs every batch through them whole, so it takes the body whole.
        return serving != null && streams(serving.settings()) && serving.services().actionGate().isEmpty();
    }

    BulkDispatch dispatcher() {
        return dispatcher;
    }

    private BulkMemoryBudget budget(NodeContext serving) {
        BulkMemoryBudget current = budget;
        if (current == null) {
            synchronized (this) {
                if (budget == null) {
                    budget = new BulkMemoryBudget(
                        org.opensearch.common.unit.MemorySizeValue.parseBytesSizeValueOrHeapRatio(
                            BulkSettings.MEMORY_BUDGET.get(serving.settings()),
                            BulkSettings.MEMORY_BUDGET.getKey()
                        ).getBytes(),
                        BulkSettings.MAX_APPLYING_SLICES.get(serving.settings()),
                        serving.threadPool()
                    );
                }
                current = budget;
            }
        }
        return current;
    }

    private RestChannelConsumer prepareStreaming(RestRequest request, NodeContext serving) {
        final String defaultIndex = request.param("index");
        final var refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param("refresh", "false"));
        final boolean tailPush = tailPush(request);
        final String defaultPipeline = request.param("pipeline");
        final String routing = DocumentRouting.normalize(request.param("routing"));
        final boolean requireAlias = request.paramAsBoolean("require_alias", false);
        for (String hint : new String[] {
            "timeout",
            "wait_for_active_shards",
            "_source",
            "_source_includes",
            "_source_excludes",
            "type" }) {
            request.param(hint);
        }
        final MetadataPlane metadata = plane.get();
        return channel -> {
            if (channel instanceof org.opensearch.rest.StreamingRestChannel == false) {
                // A wrapper in front of this handler (an auth plugin's, say) that does not pass on supportsStreaming has
                // the transport take the body whole: apply it whole.
                prepareWhole(request, serving).accept(channel);
                return;
            }
            if (metadata == null) {
                channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
                );
                return;
            }
            if (requireAlias) {
                channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.NOT_IMPLEMENTED,
                        "unsupported_write",
                        "require_alias is not supported: writes here name an index, and an alias resolves on read"
                    )
                );
                return;
            }
            final var settings = serving.settings();
            new BulkStream(
                this,
                (org.opensearch.rest.StreamingRestChannel) channel,
                serving,
                metadata,
                new BulkStream.Request(defaultIndex, routing, defaultPipeline, refreshPolicy != WriteRequest.RefreshPolicy.NONE, tailPush),
                budget(serving),
                Math.max(64 * 1024, BulkSettings.SLICE_BYTES.get(settings).getBytes()),
                org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).getBytes(),
                Math.max(1.0, BulkSettings.MEMORY_FACTOR.get(settings)),
                BulkSettings.MAX_WAIT.get(settings)
            ).start();
        };
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final NodeContext serving = node.get();
        if (supportsStreaming()) {
            return prepareStreaming(request, serving);
        }
        return prepareWhole(request, serving);
    }

    /** Takes the body whole: charged up front, then parsed and applied. */
    private RestChannelConsumer prepareWhole(RestRequest request, NodeContext serving) throws IOException {
        if (serving == null || request.hasContent() == false) {
            return prepareCharged(request, null);
        }
        // Charged before the body is copied or parsed, so a node at its limit refuses a large batch before holding any
        // more of it, and at what the batch will hold rather than its length alone: a 100 MB body is a few hundred MB
        // of heap at its peak, and charging 100 MB let the limit admit several times what the heap could take.
        final Charge charge;
        try {
            charge = new Charge(
                serving.services()
                    .indexingPressure()
                    .markCoordinatingOperationStarted(upFrontCharge(request.content().length(), serving.settings()), false)
            );
        } catch (org.opensearch.core.concurrency.OpenSearchRejectedExecutionException e) {
            // Every parameter still has to be read, or the refusal becomes a 400 about unread parameters.
            for (String param : request.params().keySet().toArray(new String[0])) {
                request.param(param);
            }
            return channel -> channel.sendResponse(IndexAdminHandler.failure(channel, e));
        }
        final RestChannelConsumer consumer;
        try {
            consumer = prepareCharged(request, charge);
        } catch (IOException | RuntimeException | Error e) {
            charge.close();
            throw e;
        }
        return channel -> {
            try {
                consumer.accept(channel);
            } finally {
                if (charge.handedOff == false) {
                    charge.close();
                }
            }
        };
    }

    private RestChannelConsumer prepareCharged(RestRequest request, Charge charge) throws IOException {
        // Every parameter read before any early return: BaseRestHandler rejects a request whose
        // parameters were not all consumed, which would turn a deliberate refusal into a 400 about
        // something else.
        final String defaultIndex = request.param("index");
        final var refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param("refresh", "false"));
        final boolean refresh = refreshPolicy != WriteRequest.RefreshPolicy.NONE;
        final boolean tailPush = tailPush(request);
        final String timeout = request.param("timeout");
        final var activeShards = org.opensearch.action.support.ActiveShardCount.parseString(request.param("wait_for_active_shards"));
        final String defaultPipeline = request.param("pipeline");
        final String routing = DocumentRouting.normalize(request.param("routing"));
        final boolean requireAlias = request.paramAsBoolean("require_alias", false);
        // Hints with one possible answer here, and the _source trio, which shapes only the response of an
        // update action -- the one action this batch refuses. Consumed so a client that sends them is not
        // turned away; see DocumentHandler for why timeout and wait_for_active_shards are hints.
        for (String hint : new String[] {
            "timeout",
            "wait_for_active_shards",
            "_source",
            "_source_includes",
            "_source_excludes",
            "type" }) {
            request.param(hint);
        }
        final byte[] body = request.hasContent() ? org.opensearch.core.common.bytes.BytesReference.toBytes(request.content()) : null;

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if (body == null || body.length == 0) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a bulk request requires a newline-delimited body")
            );
        }

        final List<BulkItem> items;
        try {
            items = BulkBodyParser.parse(body, defaultIndex, routing);
        } catch (BulkBodyParser.BadRequest e) {
            // A body that does not parse is not a batch with some bad items in it -- there is no way to
            // know where the good ones end. The whole request is refused, and the line is named.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_bulk_body", e.getMessage())
            );
        }
        if (items.isEmpty()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "empty_bulk", "the body contained no operations")
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
        for (BulkItem item : items) {
            if (item.pipeline == null) {
                item.pipeline = defaultPipeline;
            }
        }
        BulkNative.pushTo(items, tailPush);
        final NodeContext serving = node.get();
        // The coordinator runs on GENERIC and the per-shard groups run on WRITE. It used to be the other
        // way round for a moment, with both on WRITE, which is a deadlock waiting for enough concurrent
        // bulk requests: every request thread would be holding a WRITE slot while waiting for group tasks
        // that need WRITE slots to start. Nothing on WRITE now waits on WRITE.
        // What this batch is holding in memory, for as long as it holds it.
        final long inFlightBytes = request.content().length();
        return channel -> {
            if (charge != null) {
                charge.handedOff = true;
            }
            try {
                serving.threadPool()
                    .executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC)
                    .execute(
                        () -> runBatch(
                            channel,
                            serving,
                            metadata,
                            items,
                            refresh,
                            refreshPolicy,
                            defaultPipeline,
                            activeShards,
                            timeout,
                            charge,
                            inFlightBytes
                        )
                    );
            } catch (RuntimeException e) {
                if (charge != null) {
                    charge.close();
                }
                throw e;
            }
        };
    }

    private void runBatch(
        org.opensearch.rest.RestChannel channel,
        NodeContext serving,
        MetadataPlane metadata,
        List<BulkItem> items,
        boolean refresh,
        WriteRequest.RefreshPolicy refreshPolicy,
        String defaultPipeline,
        org.opensearch.action.support.ActiveShardCount activeShards,
        String timeout,
        Charge charge,
        long inFlightBytes
    ) {
        {
            final long startedAt = System.nanoTime();
            // Accounted before any work, released when the batch is done however it ends. A node with no
            // bound here is one large enough batch away from dying, and dying loses every other request
            // too; rejecting this one is the cheaper failure. Core's own accounting, so the rejection is
            // the 429 an operator has seen before. Charged already, before parsing, when that is on.
            try (
                org.opensearch.common.lease.Releasable inFlight = charge != null
                    ? charge
                    : serving.services().indexingPressure().markCoordinatingOperationStarted(inFlightBytes, false)
            ) {
                if (serving.services().actionGate().isEmpty()) {
                    // The usual path needs no second UTF-8 source encoding or native response objects.
                    dispatcher.route(serving, metadata, items);
                    dispatcher.shape(serving, metadata, items);
                    final boolean refreshed = dispatcher.dispatch(serving, items, refresh);
                    respond(channel, items, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt), refreshed);
                } else {
                    final BulkNative.NativeBatch batch = BulkNative.bulkRequestFor(items, refreshPolicy, defaultPipeline);
                    batch.request().waitForActiveShards(activeShards);
                    if (timeout != null) {
                        batch.request().timeout(timeout);
                    }
                    final var result = serving.services().actionGate().run(BulkAction.NAME, batch.request(), admitted -> {
                        final var selected = (BulkRequest) admitted;
                        if (selected.waitForActiveShards().equals(org.opensearch.action.support.ActiveShardCount.DEFAULT) == false
                            && selected.waitForActiveShards().equals(org.opensearch.action.support.ActiveShardCount.NONE) == false
                            && selected.waitForActiveShards().equals(org.opensearch.action.support.ActiveShardCount.ONE) == false
                            && selected.waitForActiveShards().equals(org.opensearch.action.support.ActiveShardCount.ALL) == false) {
                            throw new org.opensearch.OpenSearchStatusException(
                                "bulk active-shard requirements are not supported",
                                RestStatus.NOT_IMPLEMENTED
                            );
                        }
                        final List<BulkItem> admittedItems = BulkNative.admittedItems(selected, batch.rejected());
                        BulkNative.pushTo(admittedItems, items.isEmpty() || items.get(0).tailPush);
                        dispatcher.route(serving, metadata, admittedItems);
                        dispatcher.shape(serving, metadata, admittedItems);
                        final boolean selectedRefresh = selected.getRefreshPolicy() != WriteRequest.RefreshPolicy.NONE;
                        final boolean refreshed = dispatcher.dispatch(serving, admittedItems, selectedRefresh);
                        return BulkNative.nativeResult(
                            admittedItems,
                            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
                            refreshed
                        );
                    }, bulkResponseView());
                    respondNative(channel, result);
                }
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a bulk failure", nested);
                }
            }
        }
    }

    /** Gives filters the actual inputs before any ingest configuration is read or executed. */
    /** The request parameter that leaves a batch's writes to the next commit rather than push them to search nodes. */
    static final String TAIL_OVERLAY_PARAM = "tail_overlay";

    /**
     * Reads {@code tail_overlay}: {@code false} has the batch's writes searched on the search nodes from the shard's next
     * commit, rather than pushed to them as they are acknowledged -- for a large load, whose pushes would otherwise be
     * every byte of it sent again to each search node serving its shards. A batch asked to be refreshed is pushed
     * anyway, since that is what makes it visible on return. True, the default, changes nothing.
     *
     * @param request the request
     * @return false to leave the batch to the next commit
     */
    static boolean tailPush(RestRequest request) {
        return request.paramAsBoolean(TAIL_OVERLAY_PARAM, true);
    }

    private static ActionGate.ResponseView<BulkNative.NativeResult> bulkResponseView() {
        return new ActionGate.ResponseView<>() {
            @Override
            public org.opensearch.core.action.ActionResponse show(BulkNative.NativeResult result) {
                return result.response();
            }

            @Override
            public BulkNative.NativeResult read(org.opensearch.core.action.ActionResponse response, BulkNative.NativeResult original) {
                return new BulkNative.NativeResult(
                    (BulkResponse) response,
                    original != null && response == original.response() ? original.extensions() : Map.of()
                );
            }
        };
    }

    /** Renders the filtered native fields, retaining extension fields only for unchanged item identities. */
    private static void respondNative(org.opensearch.rest.RestChannel channel, BulkNative.NativeResult result) throws IOException {
        if (result.extensions().isEmpty()) {
            try (var builder = channel.newBuilder()) {
                result.response().toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
            return;
        }
        try (var builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("took", result.response().getTook().millis());
            if (result.response().getIngestTookInMillis() != BulkResponse.NO_INGEST_TOOK) {
                builder.field("ingest_took", result.response().getIngestTookInMillis());
            }
            builder.field("errors", result.response().hasFailures());
            builder.startArray("items");
            for (BulkItemResponse response : result.response().getItems()) {
                final BulkItem extension = result.extensions().get(response);
                if (extension == null) {
                    response.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
                    continue;
                }
                builder.startObject();
                builder.startObject(response.getOpType().getLowercase());
                if (response.isFailed()) {
                    builder.field("_index", response.getIndex());
                    builder.field("_id", response.getId());
                    builder.field("status", response.status().getStatus());
                    builder.startObject("error");
                    builder.field("type", extension.errorType);
                    builder.field("reason", extension.errorReason);
                    builder.endObject();
                } else {
                    response.getResponse().innerToXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
                    builder.field("status", response.status().getStatus());
                    if ("noop".equals(extension.result) == false) {
                        builder.field("_node", extension.nodeId);
                        builder.field("durable", "write-ahead log");
                    }
                }
                if (extension.shard >= 0) {
                    builder.field("_shard", extension.shard);
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    void respond(org.opensearch.rest.RestChannel channel, List<BulkItem> items, long tookMillis, boolean refreshed) throws IOException {
        boolean errors = false;
        for (BulkItem item : items) {
            if (item.failed()) {
                errors = true;
                break;
            }
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("took", tookMillis);
            // Classic semantics, and worth keeping exactly: the request succeeded, some items may not
            // have. A client that checks only the HTTP status of a bulk is wrong in classic OpenSearch
            // too, and making this a 4xx would break the clients that check the field instead.
            builder.field("errors", errors);
            builder.startArray("items");
            for (BulkItem item : items) {
                builder.startObject();
                // The action the caller wrote, not a normalised one: a client matching items to the
                // actions it sent looks up this key.
                builder.startObject("index".equals(item.action) && item.requireAbsent ? "create" : item.action);
                // The backing index once the item was routed, as core reports it and as the single-document
                // path reports it; the name the caller wrote until then, since nothing else is known.
                builder.field("_index", item.writtenIndex != null ? item.writtenIndex : item.index);
                builder.field("_id", item.operation.id());
                if (item.shard >= 0) {
                    builder.field("_shard", item.shard);
                }
                if (item.failed()) {
                    builder.field("status", item.status.getStatus());
                    builder.startObject("error");
                    builder.field("type", item.errorType);
                    builder.field("reason", item.errorReason);
                    builder.endObject();
                } else if ("noop".equals(item.result)) {
                    // Dropped by its pipeline: nothing was written, so there is no sequence identity to
                    // report, the same as core's answer for a dropped document.
                    builder.field("status", item.status.getStatus());
                    builder.field("result", item.result);
                } else {
                    builder.field("status", item.status.getStatus());
                    builder.field("result", item.result);
                    builder.field("_version", item.version);
                    builder.field("_seq_no", item.seqNo);
                    builder.field("_primary_term", item.primaryTerm);
                    builder.startObject("_shards");
                    builder.field("total", 1);
                    builder.field("successful", 1);
                    builder.field("failed", 0);
                    builder.endObject();
                    builder.field("_node", item.nodeId);
                    // The same promise the single-document path makes, and the reason bulk did not have
                    // to weaken it: the whole batch is in the log before any of this response exists.
                    builder.field("durable", "write-ahead log");
                    if (refreshed) {
                        builder.field("forced_refresh", true);
                    }
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

}
