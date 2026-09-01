/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.UUIDs;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.transport.client.node.NodeClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 * <p><b>Unsupported actions are refused per item, not quietly reinterpreted.</b> {@code create} means
 * "fail if this document exists" and {@code update} means a partial merge; neither can be honoured
 * without version-conditional writes, which this system does not have. Treating them as {@code index}
 * would be the failure mode D2 exists to prevent — a request that is accepted and means something other
 * than what it says. They are rejected individually, so the rest of the batch still lands.
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

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public BulkHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
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

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Every parameter read before any early return: BaseRestHandler rejects a request whose
        // parameters were not all consumed, which would turn a deliberate refusal into a 400 about
        // something else.
        final String defaultIndex = request.param("index");
        final boolean refresh = request.paramAsBoolean("refresh", false);
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
            items = parse(body, defaultIndex);
        } catch (BadRequest e) {
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

        final ServerlessNode serving = node.get();
        // The coordinator runs on GENERIC and the per-shard groups run on WRITE. It used to be the other
        // way round for a moment, with both on WRITE, which is a deadlock waiting for enough concurrent
        // bulk requests: every request thread would be holding a WRITE slot while waiting for group tasks
        // that need WRITE slots to start. Nothing on WRITE now waits on WRITE.
        // What this batch is holding in memory, for as long as it holds it.
        final long inFlightBytes = request.content().length();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            final long startedAt = System.nanoTime();
            // Accounted before any work, released when the batch is done however it ends. A node with no
            // bound here is one large enough batch away from dying, and dying loses every other request
            // too; rejecting this one is the cheaper failure. Core's own accounting, so the rejection is
            // the 429 an operator has seen before.
            try (
                org.opensearch.common.lease.Releasable inFlight = serving.indexingPressure()
                    .markCoordinatingOperationStarted(inFlightBytes, false)
            ) {
                route(serving, metadata, items);
                // One gate call for the whole batch, under the bulk action, rather than one per item.
                //
                // A privilege evaluator written for OpenSearch expects to see indices:data/write/bulk with
                // a BulkRequest whose indices() covers the batch; filtering each document under
                // indices:data/write/index instead would be stricter but would show a filter an action name
                // it never registered for, which for a filter that only guards bulk is a hole rather than a
                // difference. The request is built from the items that survived routing, so a filter sees
                // the indices actually about to be written.
                serving.actionGate().run(org.opensearch.action.bulk.BulkAction.NAME, bulkRequestFor(items), () -> {
                    dispatch(serving, items, refresh);
                    return null;
                });
                respond(channel, items, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a bulk failure", nested);
                }
            }
        });
    }

    /**
     * Decides, per item, which shard it belongs to and who owns it — or records why it cannot be placed.
     */
    /**
     * Describes the batch to an action filter, in the request type a filter expects.
     *
     * <p>Only what a filter needs: which indices, and which operations. Sources are not copied — a bulk of
     * ten thousand documents would be duplicated in memory to tell a privilege evaluator something it does
     * not read.
     */
    private static org.opensearch.action.bulk.BulkRequest bulkRequestFor(List<Item> items) {
        final org.opensearch.action.bulk.BulkRequest request = new org.opensearch.action.bulk.BulkRequest();
        for (Item item : items) {
            if (item.failed() || item.index == null) {
                continue;
            }
            if (item.operation.isDeletion()) {
                request.add(new org.opensearch.action.delete.DeleteRequest(item.index, item.operation.id()));
            } else {
                request.add(new org.opensearch.action.index.IndexRequest(item.index).id(item.operation.id()));
            }
        }
        return request;
    }

    private void route(ServerlessNode serving, MetadataPlane metadata, List<Item> items) throws IOException {
        final Map<String, Optional<IndexDescriptor>> described = new HashMap<>();
        for (Item item : items) {
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
            final Optional<IndexDescriptor> descriptor = described.computeIfAbsent(item.index, name -> {
                try {
                    return metadata.describe(name);
                } catch (IOException e) {
                    return Optional.empty();
                }
            });
            if (descriptor.isEmpty()) {
                item.fail(RestStatus.NOT_FOUND, "index_not_found", "no such index: " + item.index);
                continue;
            }
            item.shard = DocumentRouting.shardFor(descriptor.get(), item.operation.id());
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
     */
    private void dispatch(ServerlessNode serving, List<Item> items, boolean refresh) throws IOException {
        final Map<String, List<Item>> groups = new LinkedHashMap<>();
        for (Item item : items) {
            if (item.failed() == false) {
                groups.computeIfAbsent(item.index + "[" + item.shard + "]", key -> new ArrayList<>()).add(item);
            }
        }
        final List<java.util.concurrent.Callable<Void>> tasks = new ArrayList<>(groups.size());
        for (List<Item> group : groups.values()) {
            tasks.add(() -> {
                applyGroup(serving, group, refresh);
                return null;
            });
        }
        try {
            Fanout.run(serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE), Fanout.DEFAULT_CONCURRENCY, tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while writing a batch", e);
        }
        // A group whose task died before recording anything would leave its items with no outcome at all,
        // which would serialize as a success with no result. Say so instead.
        for (Item item : items) {
            if (item.failed() == false && item.status == null) {
                item.fail(RestStatus.INTERNAL_SERVER_ERROR, "no_outcome", "the writer returned no outcome for this operation");
            }
        }
    }

    private void applyGroup(ServerlessNode serving, List<Item> group, boolean refresh) throws IOException {
        final String index = group.get(0).index;
        final int shard = group.get(0).shard;
        final List<WalRecord> operations = new ArrayList<>(group.size());
        for (Item item : group) {
            operations.add(item.operation);
        }

        final ShardId local = serving.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(index) && s.id() == shard)
            .findFirst()
            .orElse(null);

        if (local != null && serving.reconciler().readerShards().contains(local) == false) {
            record(group, serving.bulk(local, operations), serving.localNode().getId());
            if (refresh) {
                serving.reconciler().shard(local).refresh("serverless-bulk-refresh");
            }
            return;
        }

        final var head = plane.get().heads().read(index, shard);
        final String owner = head.map(h -> h.ownerNodeId()).orElse(null);
        if (owner == null) {
            // Nobody owns it, and the only reason anyone would activate it is that a write arrived --
            // which just did. Same conclusion the single-document path reaches.
            serving.signals().ownershipDoubted(index, shard);
            failGroup(group, RestStatus.MISDIRECTED_REQUEST, "not_the_writer", "no node currently owns shard " + shard + " of " + index);
            return;
        }
        if (owner.equals(serving.localNode().getId())) {
            // The head names this node and the shard is not open here: the activation window, which is
            // brief and self-resolving. Forwarding would send the batch to ourselves and be refused.
            failGroup(
                group,
                RestStatus.SERVICE_UNAVAILABLE,
                "activation_in_progress",
                "this node is acquiring shard " + shard + " of " + index + "; retry"
            );
            return;
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
                return;
            }
            final var response = serving.router()
                .forwardBulk(peer.get(), new org.opensearch.serverless.transport.ForwardedBulkRequest(index, shard, operations, refresh));
            record(group, response.outcomes(), response.ownerNodeId());
        } catch (Exception e) {
            // 503 rather than the exception's own status, for the reason the single-write path gives:
            // a forward that fails means the routing was stale, and stale routing is a retry.
            serving.signals().ownershipDoubted(index, shard);
            failGroup(
                group,
                RestStatus.SERVICE_UNAVAILABLE,
                "forward_failed",
                "could not forward to " + owner + ", which the shard-head named as owner: " + e.getMessage()
            );
        }
    }

    private void record(List<Item> group, List<ServerlessNode.BulkOutcome> outcomes, String nodeId) {
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

    private void failGroup(List<Item> group, RestStatus status, String type, String reason) {
        for (Item item : group) {
            item.fail(status, type, reason);
        }
    }

    private void respond(org.opensearch.rest.RestChannel channel, List<Item> items, long tookMillis) throws IOException {
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
                builder.startObject(item.operation.isDeletion() ? "delete" : "index");
                builder.field("_index", item.index);
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
                } else {
                    builder.field("status", item.status.getStatus());
                    builder.field("result", item.result);
                    builder.field("_node", item.nodeId);
                    // The same promise the single-document path makes, and the reason bulk did not have
                    // to weaken it: the whole batch is in the log before any of this response exists.
                    builder.field("durable", "write-ahead log");
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Splits the newline-delimited body into operations.
     *
     * <p>Structural problems throw: a body whose action and source lines do not pair up has no
     * well-defined prefix of good items, so guessing where the batch resumes would be inventing a
     * request the client did not send. Problems with an individual, well-formed action — an unsupported
     * one — become a failed item instead, and the rest of the batch proceeds.
     */
    private static List<Item> parse(byte[] body, String defaultIndex) throws BadRequest, IOException {
        final List<Item> items = new ArrayList<>();
        final List<byte[]> lines = split(body);
        int line = 0;
        while (line < lines.size()) {
            final Action action = parseAction(lines.get(line), line + 1, defaultIndex);
            line++;
            if (action.needsSource) {
                if (line >= lines.size()) {
                    throw new BadRequest("line " + line + ": action '" + action.name + "' has no source line after it");
                }
                action.source = new String(lines.get(line), StandardCharsets.UTF_8);
                line++;
            }
            items.add(action.toItem());
        }
        return items;
    }

    /** Splits on newlines, dropping blank lines: a serialized action or source is never blank. */
    private static List<byte[]> split(byte[] body) {
        final List<byte[]> lines = new ArrayList<>();
        int start = 0;
        for (int i = 0; i <= body.length; i++) {
            if (i != body.length && body[i] != '\n') {
                continue;
            }
            int end = i;
            // Tolerate CRLF, which a client sending a file rather than a buffer will produce.
            if (end > start && body[end - 1] == '\r') {
                end--;
            }
            if (end > start && new String(body, start, end - start, StandardCharsets.UTF_8).isBlank() == false) {
                final byte[] copy = new byte[end - start];
                System.arraycopy(body, start, copy, 0, end - start);
                lines.add(copy);
            }
            start = i + 1;
        }
        return lines;
    }

    /** Action-line fields that ask for a conditional write, none of which this system honours. */
    private static final java.util.Set<String> CONDITIONAL_FIELDS = java.util.Set.of(
        "_seq_no",
        "_primary_term",
        "_version",
        "version_type"
    );

    private static Action parseAction(byte[] bytes, int lineNumber, String defaultIndex) throws BadRequest, IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new ByteArrayInputStream(bytes))
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new BadRequest("line " + lineNumber + ": expected an action object");
            }
            if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new BadRequest("line " + lineNumber + ": expected an action name");
            }
            final Action action = new Action(parser.currentName(), defaultIndex);
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new BadRequest("line " + lineNumber + ": action '" + action.name + "' must be followed by an object");
            }
            String field = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null && token != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
                } else if (token.isValue()) {
                    if ("_index".equals(field)) {
                        action.index = parser.text();
                    } else if ("_id".equals(field)) {
                        action.id = parser.text();
                    } else if (CONDITIONAL_FIELDS.contains(field)) {
                        // Read and kept, not merely noticed: a client that asked for a compare-and-swap
                        // and got an unconditional write believing it got one is the confident wrong
                        // answer this design refuses everywhere else. Every operation on the line stays
                        // unconditional, so the whole line is unsupported rather than just this field.
                        action.conditional = field;
                    }
                }
            }
            return action;
        } catch (org.opensearch.core.xcontent.XContentParseException e) {
            throw new BadRequest("line " + lineNumber + ": " + e.getMessage());
        }
    }

    /** One parsed action line, before it becomes an item. */
    private static final class Action {
        private final String name;
        private final boolean needsSource;
        private final boolean supported;
        private String index;
        private String id;
        private String source;
        private String conditional;

        Action(String name, String defaultIndex) {
            this.name = name;
            this.index = defaultIndex;
            // "create" is fail-if-exists and "update" is a partial merge. Both need machinery this
            // system does not have, so both are unsupported rather than approximated.
            this.supported = "index".equals(name) || "delete".equals(name);
            this.needsSource = "index".equals(name) || "create".equals(name) || "update".equals(name);
        }

        Item toItem() {
            // An id is generated when none is given, the same as classic. It has to exist before the
            // item does, because routing is a function of it.
            final String documentId = id != null ? id : UUIDs.base64UUID();
            final Item item = new Item(
                index,
                "delete".equals(name) ? WalRecord.deletion(documentId) : new WalRecord(documentId, source == null ? "" : source)
            );
            if (supported == false) {
                item.fail(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_action",
                    "'" + name + "' is not supported: it requires version-conditional writes, which this system does not have"
                );
            } else if (conditional != null) {
                item.fail(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_action",
                    "'"
                        + conditional
                        + "' on '"
                        + documentId
                        + "' asks for a conditional write, which this system does not have: "
                        + "every write is a plain overwrite and every delete a plain removal"
                );
            } else if ("index".equals(name) && (source == null || source.isBlank())) {
                item.fail(RestStatus.BAD_REQUEST, "missing_source", "the document body for '" + documentId + "' was empty");
            }
            return item;
        }
    }

    /** One operation and, eventually, what became of it. */
    private static final class Item {
        private final String index;
        private final WalRecord operation;
        private int shard = -1;
        private RestStatus status;
        private String result;
        private String nodeId;
        private String errorType;
        private String errorReason;

        Item(String index, WalRecord operation) {
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

        void succeed(ServerlessNode.BulkOutcome outcome, String nodeId) {
            if (outcome.failure() != null) {
                fail(RestStatus.BAD_REQUEST, "operation_failed", outcome.failure());
                return;
            }
            this.nodeId = nodeId;
            if (outcome.isDeletion()) {
                this.result = outcome.found() ? "deleted" : "not_found";
                this.status = outcome.found() ? RestStatus.OK : RestStatus.NOT_FOUND;
            } else {
                this.result = "created";
                this.status = RestStatus.CREATED;
            }
        }
    }

    /** A body that cannot be split into operations at all. */
    private static final class BadRequest extends Exception {
        BadRequest(String message) {
            super(message);
        }
    }
}
