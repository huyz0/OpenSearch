/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.VersionType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ActionGate;
import org.opensearch.serverless.shell.BulkOutcome;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
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
 * {@code ServerlessBulkTests#testTheShardGroupsOfOneBatchRunAtTheSameTime} settles it as a rendezvous
 * rather than a stopwatch: each shard's first log append waits for the others and fails if they never come,
 * so a sequential dispatch cannot pass and a concurrent one cannot fail.
 */
public final class BulkHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;
    private final BulkDispatch dispatcher;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public BulkHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
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
     * Whether a batch's memory is charged to indexing pressure before its body is copied and parsed, at
     * {@link #MEMORY_FACTOR_SETTING} times its length; off, it is charged its length once, after parsing, as before.
     */
    public static final String CHARGE_BEFORE_PARSE_SETTING = "serverless.write.bulk.charge_before_parse";

    /** The default {@link #CHARGE_BEFORE_PARSE_SETTING}. */
    public static final boolean DEFAULT_CHARGE_BEFORE_PARSE = true;

    /**
     * How many times its length a batch is charged when {@link #CHARGE_BEFORE_PARSE_SETTING} is on: what a node holds of
     * it at once, the body, each source as a string, the bytes the engine parses and the node-log blob carrying it.
     */
    public static final String MEMORY_FACTOR_SETTING = "serverless.write.bulk.memory_factor";

    /** The default {@link #MEMORY_FACTOR_SETTING}. */
    public static final double DEFAULT_MEMORY_FACTOR = 4.0;

    /**
     * The bytes a batch of this length is charged up front.
     *
     * @param contentLength the body's length
     * @param settings the node settings
     * @return the charge
     */
    static long upFrontCharge(long contentLength, org.opensearch.common.settings.Settings settings) {
        final double factor = Math.max(1.0, settings.getAsDouble(MEMORY_FACTOR_SETTING, DEFAULT_MEMORY_FACTOR));
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

    /**
     * Whether {@code _bulk} bodies are streamed: read as they arrive, a slice at a time, with memory per request bounded
     * whatever the body's size ({@link BulkStream}). Needs the streaming HTTP transport, which the same setting selects.
     */
    public static final String STREAMING_SETTING = "serverless.http.streaming";

    /** The default {@link #STREAMING_SETTING}. */
    public static final boolean DEFAULT_STREAMING = false;

    /** How much of a streamed body is parsed and applied at once. */
    public static final String SLICE_BYTES_SETTING = "serverless.write.bulk.slice_bytes";

    /** The default {@link #SLICE_BYTES_SETTING}: one node-log blob's worth. */
    public static final long DEFAULT_SLICE_BYTES = 8L * 1024 * 1024;

    /** The heap all streamed batches on a node may hold at once while being applied. */
    public static final String BUDGET_SETTING = "serverless.write.bulk.memory_budget";

    /** How long a streamed slice waits for its share of the budget before its request is refused with a 429. */
    public static final String WAIT_SETTING = "serverless.write.bulk.max_wait";

    /** How many streamed slices a node applies at once, across all batches; the node's cores by default. */
    public static final String MAX_APPLYING_SETTING = "serverless.write.bulk.max_applying_slices";

    /** The default {@link #WAIT_SETTING}. */
    public static final org.opensearch.common.unit.TimeValue DEFAULT_WAIT = org.opensearch.common.unit.TimeValue.timeValueMinutes(2);

    private volatile BulkMemoryBudget budget;

    /**
     * Whether a node with these settings streams batches.
     *
     * @param settings the node settings
     * @return true to stream
     */
    public static boolean streams(org.opensearch.common.settings.Settings settings) {
        return settings.getAsBoolean(STREAMING_SETTING, DEFAULT_STREAMING);
    }

    @Override
    public boolean supportsStreaming() {
        final ServerlessNode serving = node.get();
        // A node with action filters runs every batch through them whole, so it takes the body whole.
        return serving != null && streams(serving.settings()) && serving.services().actionGate().isEmpty();
    }

    BulkDispatch dispatcher() {
        return dispatcher;
    }

    private BulkMemoryBudget budget(ServerlessNode serving) {
        BulkMemoryBudget current = budget;
        if (current == null) {
            synchronized (this) {
                if (budget == null) {
                    budget = new BulkMemoryBudget(
                        org.opensearch.common.unit.MemorySizeValue.parseBytesSizeValueOrHeapRatio(
                            serving.settings().get(BUDGET_SETTING, "10%"),
                            BUDGET_SETTING
                        ).getBytes(),
                        serving.settings()
                            .getAsInt(
                                MAX_APPLYING_SETTING,
                                org.opensearch.common.util.concurrent.OpenSearchExecutors.allocatedProcessors(serving.settings())
                            ),
                        serving.threadPool()
                    );
                }
                current = budget;
            }
        }
        return current;
    }

    private RestChannelConsumer prepareStreaming(RestRequest request, ServerlessNode serving) {
        final String defaultIndex = request.param("index");
        final var refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param("refresh", "false"));
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
                channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.INTERNAL_SERVER_ERROR,
                        "not_streaming",
                        "the transport did not stream this body"
                    )
                );
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
                new BulkStream.Request(defaultIndex, routing, defaultPipeline, refreshPolicy != WriteRequest.RefreshPolicy.NONE),
                budget(serving),
                Math.max(
                    64 * 1024,
                    settings.getAsBytesSize(SLICE_BYTES_SETTING, new org.opensearch.core.common.unit.ByteSizeValue(DEFAULT_SLICE_BYTES))
                        .getBytes()
                ),
                org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).getBytes(),
                Math.max(1.0, settings.getAsDouble(MEMORY_FACTOR_SETTING, DEFAULT_MEMORY_FACTOR)),
                settings.getAsTime(WAIT_SETTING, DEFAULT_WAIT)
            ).start();
        };
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final ServerlessNode serving = node.get();
        if (supportsStreaming()) {
            return prepareStreaming(request, serving);
        }
        if (serving == null
            || request.hasContent() == false
            || serving.settings().getAsBoolean(CHARGE_BEFORE_PARSE_SETTING, DEFAULT_CHARGE_BEFORE_PARSE) == false) {
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

        final List<Item> items;
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
        for (Item item : items) {
            if (item.pipeline == null) {
                item.pipeline = defaultPipeline;
            }
        }
        final ServerlessNode serving = node.get();
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
        ServerlessNode serving,
        MetadataPlane metadata,
        List<Item> items,
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
                    final NativeBatch batch = bulkRequestFor(items, refreshPolicy, defaultPipeline);
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
                        final List<Item> admittedItems = admittedItems(selected, batch.rejected());
                        dispatcher.route(serving, metadata, admittedItems);
                        dispatcher.shape(serving, metadata, admittedItems);
                        final boolean selectedRefresh = selected.getRefreshPolicy() != WriteRequest.RefreshPolicy.NONE;
                        final boolean refreshed = dispatcher.dispatch(serving, admittedItems, selectedRefresh);
                        return nativeResult(
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

    record NativeBatch(BulkRequest request, Map<DocWriteRequest<?>, Item> rejected) {
    }

    record NativeResult(BulkResponse response, Map<BulkItemResponse, Item> extensions) {
    }

    /** Gives filters the actual inputs before any ingest configuration is read or executed. */
    static NativeBatch bulkRequestFor(List<Item> items, WriteRequest.RefreshPolicy refreshPolicy, String pipeline) {
        final var request = new BulkRequest().pipeline(pipeline).setRefreshPolicy(refreshPolicy);
        final Map<DocWriteRequest<?>, Item> rejected = new java.util.IdentityHashMap<>();
        for (Item item : items) {
            final DocWriteRequest<?> write;
            if ("update".equals(item.action)) {
                // Update is explicitly refused by this surface; it must still have the correct native type.
                write = new UpdateRequest(item.index, item.operation.id());
            } else if (item.operation.isDeletion()) {
                final var delete = new DeleteRequest(item.index, item.operation.id()).routing(item.operation.routing());
                if (item.ifSeqNo >= 0L && item.ifPrimaryTerm > 0L) {
                    delete.setIfSeqNo(item.ifSeqNo).setIfPrimaryTerm(item.ifPrimaryTerm);
                }
                write = delete;
            } else {
                final var index = new IndexRequest(item.index).id(item.operation.id())
                    .routing(item.operation.routing())
                    .source(item.operation.source(), XContentType.JSON)
                    .opType(item.requireAbsent ? DocWriteRequest.OpType.CREATE : DocWriteRequest.OpType.INDEX)
                    .setPipeline(item.pipeline);
                if (item.ifSeqNo >= 0L && item.ifPrimaryTerm > 0L) {
                    index.setIfSeqNo(item.ifSeqNo).setIfPrimaryTerm(item.ifPrimaryTerm);
                }
                write = index;
            }
            request.add(write);
            if (item.failed()) {
                rejected.put(write, item);
            }
        }
        return new NativeBatch(request, rejected);
    }

    static List<Item> admittedItems(BulkRequest request, Map<DocWriteRequest<?>, Item> rejected) throws IOException {
        final List<Item> items = new ArrayList<>(request.numberOfActions());
        for (DocWriteRequest<?> write : request.requests()) {
            final Item refused = rejected.get(write);
            if (refused != null) {
                // Parsing failures cannot turn into writes merely because a filter changes another item.
                items.add(refused);
                continue;
            }
            final BulkBodyParser.Action action = new BulkBodyParser.Action(write.opType().getLowercase(), write.index());
            action.id = write.id();
            action.ifSeqNo = write.ifSeqNo();
            action.ifPrimaryTerm = write.ifPrimaryTerm();
            action.routing = write.routing();
            action.requireAlias = write.isRequireAlias();
            if (write instanceof IndexRequest index) {
                action.source = index.source() == null
                    ? ""
                    : org.opensearch.common.xcontent.XContentHelper.convertToJson(index.source(), false, false, index.getContentType());
                action.pipeline = index.getPipeline() == null ? request.pipeline() : index.getPipeline();
            }
            final long version = write.version();
            final boolean defaultVersion = version == org.opensearch.common.lucene.uid.Versions.MATCH_ANY
                || (write.opType() == DocWriteRequest.OpType.CREATE && version == org.opensearch.common.lucene.uid.Versions.MATCH_DELETED);
            if (write.versionType() != VersionType.INTERNAL || defaultVersion == false) {
                action.conditional = "version";
            }
            final Item item = action.toItem();
            if (write instanceof IndexRequest index) {
                if (index.getFinalPipeline() != null) {
                    item.fail(
                        RestStatus.NOT_IMPLEMENTED,
                        "unsupported_action",
                        "an explicit native final pipeline is not supported; configure index.final_pipeline"
                    );
                }
                if (index.id() == null) {
                    index.id(item.operation.id());
                }
            }
            items.add(item);
        }
        return items;
    }

    static NativeResult nativeResult(List<Item> items, long took, boolean refreshed) {
        final var responses = new BulkItemResponse[items.size()];
        final Map<BulkItemResponse, Item> extensions = new java.util.IdentityHashMap<>();
        for (int i = 0; i < items.size(); i++) {
            final Item item = items.get(i);
            final var op = DocWriteRequest.OpType.fromString("index".equals(item.action) && item.requireAbsent ? "create" : item.action);
            final String index = item.writtenIndex == null ? item.index : item.writtenIndex;
            if (item.failed()) {
                responses[i] = new BulkItemResponse(
                    i,
                    op,
                    new BulkItemResponse.Failure(
                        index,
                        item.operation.id(),
                        new org.opensearch.OpenSearchStatusException(item.errorReason, item.status),
                        item.status
                    )
                );
            } else {
                final var shardId = new ShardId(index, item.writtenUuid, item.shard);
                final DocWriteResponse response;
                if ("noop".equals(item.result)) {
                    // Core uses UpdateResponse for an ingest drop even when the input operation is index/create.
                    response = new UpdateResponse(
                        shardId,
                        item.operation.id(),
                        item.seqNo,
                        item.primaryTerm,
                        item.version,
                        DocWriteResponse.Result.NOOP
                    );
                } else if (item.operation.isDeletion()) {
                    response = new DeleteResponse(
                        shardId,
                        item.operation.id(),
                        item.seqNo,
                        item.primaryTerm,
                        item.version,
                        "deleted".equals(item.result)
                    );
                } else {
                    response = new IndexResponse(
                        shardId,
                        item.operation.id(),
                        item.seqNo,
                        item.primaryTerm,
                        item.version,
                        "created".equals(item.result)
                    );
                }
                if ("noop".equals(item.result) == false) {
                    response.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
                    response.setForcedRefresh(refreshed);
                }
                responses[i] = new BulkItemResponse(i, op, response);
            }
            extensions.put(responses[i], item);
        }
        return new NativeResult(new BulkResponse(responses, took), extensions);
    }

    private static ActionGate.ResponseView<NativeResult> bulkResponseView() {
        return new ActionGate.ResponseView<>() {
            @Override
            public org.opensearch.core.action.ActionResponse show(NativeResult result) {
                return result.response();
            }

            @Override
            public NativeResult read(org.opensearch.core.action.ActionResponse response, NativeResult original) {
                return new NativeResult(
                    (BulkResponse) response,
                    original != null && response == original.response() ? original.extensions() : Map.of()
                );
            }
        };
    }

    /** Renders the filtered native fields, retaining extension fields only for unchanged item identities. */
    private static void respondNative(org.opensearch.rest.RestChannel channel, NativeResult result) throws IOException {
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
                final Item extension = result.extensions().get(response);
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

    void respond(org.opensearch.rest.RestChannel channel, List<Item> items, long tookMillis, boolean refreshed) throws IOException {
        boolean errors = false;
        for (Item item : items) {
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
            for (Item item : items) {
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

    /** One operation and, eventually, what became of it. */
    static final class Item {
        final String action;
        final String index;
        String writtenIndex;
        String writtenUuid;
        WalRecord operation;
        String pipeline;
        /** The pipelines to run, in order: the request's or the index's default, then the index's final. */
        List<String> pipelines = List.of();
        int shard = -1;
        RestStatus status;
        String result;
        String nodeId;
        String errorType;
        String errorReason;
        long seqNo = org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
        long primaryTerm = org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
        long version = org.opensearch.common.lucene.uid.Versions.NOT_FOUND;
        boolean requireAbsent;
        long ifSeqNo = org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
        long ifPrimaryTerm = 0L;

        Item(String action, String index, WalRecord operation) {
            this.action = action;
            this.index = index;
            this.operation = operation;
        }

        boolean failed() {
            return errorType != null;
        }

        void fail(RestStatus status, String type, String reason) {
            if (failed()) {
                return;
            }
            this.status = status;
            this.errorType = type;
            this.errorReason = reason;
        }

        /** Forgets a refusal that applied nothing, so the operation can be sent again. */
        void clearRefusal() {
            this.status = null;
            this.errorType = null;
            this.errorReason = null;
        }

        /** Dropped by its pipeline: answered, not written. */
        void noop(String nodeId) {
            this.nodeId = nodeId;
            this.status = RestStatus.OK;
            this.result = "noop";
        }

        void succeed(BulkOutcome outcome, String nodeId) {
            if (outcome.failure() != null) {
                // A lost condition is a 409, not a 400, and carries the type classic OpenSearch uses --
                // a client retrying a compare-and-swap needs to tell "your condition did not hold" from
                // "your document was malformed", and only one of those is worth retrying.
                if (outcome.conflict()) {
                    fail(RestStatus.CONFLICT, "version_conflict_engine_exception", outcome.failure());
                } else if (outcome.retryable()) {
                    // The mapping was being grown by another node at the same moment. The document is
                    // fine and a retry lands it; a 400 told the client the opposite.
                    fail(RestStatus.SERVICE_UNAVAILABLE, "mapping_contention", outcome.failure());
                } else {
                    fail(RestStatus.BAD_REQUEST, "operation_failed", outcome.failure());
                }
                return;
            }
            this.nodeId = nodeId;
            this.seqNo = outcome.seqNo();
            this.primaryTerm = outcome.primaryTerm();
            this.version = outcome.version();
            if (outcome.isDeletion()) {
                this.result = outcome.found() ? "deleted" : "not_found";
                this.status = outcome.found() ? RestStatus.OK : RestStatus.NOT_FOUND;
            } else {
                // Told apart honestly now, the same as the single-document path: the engine reports
                // whether the document existed, and this used to say "created" regardless.
                this.result = outcome.created() ? "created" : "updated";
                this.status = outcome.created() ? RestStatus.CREATED : RestStatus.OK;
            }
        }
    }

}
