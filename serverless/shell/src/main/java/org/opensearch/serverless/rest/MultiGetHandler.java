/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * {@code POST /_mget} and {@code POST /{index}/_mget} — reading many documents in one request.
 *
 * <p><b>Why this is not a loop over the get endpoint.</b> Documents in one request belong to different
 * shards on different nodes, so fetching them one after another costs the sum of their round trips. They
 * are fetched concurrently through the same bounded fan-out a search uses, which makes the cost the slowest
 * of them. That is the same argument {@code _bulk} makes on the write side, and for the same reason: a
 * batch API whose latency is the sum of its items is a batch API in name only.
 *
 * <p><b>Each item answers for itself.</b> One document being on an unreachable node does not fail the
 * others, and one that does not exist is {@code "found": false} rather than an error — which is what makes
 * a multi-get worth having over a loop the caller writes. What must not happen is an item silently
 * disappearing from the response, so every item asked for appears in the answer, in the order it was asked
 * for, with either a document or a reason.
 *
 * <p><b>The reasons are the get endpoint's reasons.</b> Both route through {@link ShardOperations}, so a
 * document that a single get would refuse to answer from a stale copy is refused here in the same words,
 * rather than a batch API quietly having weaker guarantees than the single one.
 */
public final class MultiGetHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public MultiGetHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_mget_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, "/_mget"),
            new Route(RestRequest.Method.GET, "/_mget"),
            new Route(RestRequest.Method.POST, "/{index}/_mget"),
            new Route(RestRequest.Method.GET, "/{index}/_mget")
        );
    }

    /** One document asked for, and whatever came back. */
    private static final class Item {
        private final String index;
        private final String id;
        private ServerlessNode.Document document;
        private String failureType;
        private String failureReason;

        Item(String index, String id) {
            this.index = index;
            this.id = id;
        }

        void fail(String type, String reason) {
            this.failureType = type;
            this.failureReason = reason;
        }
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Every parameter before any early return, or BaseRestHandler turns a deliberate refusal into a
        // 400 about an unconsumed parameter.
        final String defaultIndex = request.param("index");
        if (request.hasContentOrSourceParam() == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a multi-get needs a body naming the documents")
            );
        }

        final List<Item> items = new ArrayList<>();
        final String malformed = parse(request, defaultIndex, items);
        if (malformed != null) {
            return channel -> channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_request", malformed));
        }
        if (items.isEmpty()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "no_documents", "a multi-get must name at least one document")
            );
        }

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final ServerlessNode serving = node.get();

        // The coordinator runs on GENERIC and the items on GET, so nothing on the pool that reads a
        // document is waiting for the pool that reads a document.
        return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                fetch(serving, metadata, items);
                respond(channel, items);
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a multi-get failure", nested);
                }
            }
        });
    }

    /**
     * Reads the body, which may name documents individually or list ids against the path's index.
     *
     * @return a reason the body could not be read, or null
     */
    private String parse(RestRequest request, String defaultIndex, List<Item> items) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            final var body = parser.map();
            if (body.get("docs") instanceof List<?> docs) {
                for (Object entry : docs) {
                    if ((entry instanceof java.util.Map<?, ?>) == false) {
                        return "every entry in docs must be an object naming _index and _id";
                    }
                    final java.util.Map<?, ?> doc = (java.util.Map<?, ?>) entry;
                    final Object index = doc.get("_index") == null ? defaultIndex : doc.get("_index");
                    final Object id = doc.get("_id");
                    if (index == null || id == null) {
                        return "every document needs an _id, and an _index unless the request path names one";
                    }
                    items.add(new Item(index.toString(), id.toString()));
                }
                return null;
            }
            if (body.get("ids") instanceof List<?> ids) {
                if (defaultIndex == null) {
                    return "a bare list of ids needs an index in the request path";
                }
                for (Object id : ids) {
                    items.add(new Item(defaultIndex, String.valueOf(id)));
                }
                return null;
            }
            return "a multi-get body needs either docs or ids";
        } catch (Exception e) {
            return "the body could not be read: " + e.getMessage();
        }
    }

    /** Fetches every item at once, up to the fan-out's bound. */
    private void fetch(ServerlessNode serving, MetadataPlane metadata, List<Item> items) throws IOException {
        final ShardOperations operations = new ShardOperations(serving, metadata);
        final List<Callable<Boolean>> tasks = new ArrayList<>(items.size());
        for (Item item : items) {
            tasks.add(() -> {
                try {
                    item.document = operations.get(item.index, item.id).document();
                    return true;
                } catch (ShardOperations.NoSuchIndexException e) {
                    item.fail("index_not_found", e.getMessage());
                } catch (ShardOperations.NotHereException e) {
                    // The get endpoint's vocabulary, not a batch-shaped approximation of it.
                    item.fail(
                        e.owner() != null && e.owner().equals(serving.localNode().getId())
                            ? "activation_in_progress"
                            : (e.getMessage().contains("could not forward") ? "forward_failed" : "owner_unreachable"),
                        e.getMessage()
                    );
                } catch (Exception e) {
                    item.fail("read_failed", String.valueOf(e.getMessage()));
                }
                return false;
            });
        }
        try {
            Fanout.run(serving.threadPool().executor(ThreadPool.Names.GET), Fanout.DEFAULT_CONCURRENCY, tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while reading a multi-get", e);
        }
    }

    private void respond(org.opensearch.rest.RestChannel channel, List<Item> items) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("docs");
            for (Item item : items) {
                builder.startObject();
                builder.field("_index", item.index);
                builder.field("_id", item.id);
                if (item.failureType != null) {
                    // An item that could not be read says so rather than reporting "not found", because a
                    // document that exists on a node we could not reach is not a document that is absent.
                    builder.startObject("error");
                    builder.field("type", item.failureType);
                    builder.field("reason", item.failureReason);
                    builder.endObject();
                    builder.endObject();
                    continue;
                }
                final boolean found = item.document != null && item.document.found();
                builder.field("found", found);
                if (found) {
                    builder.rawField(
                        "_source",
                        new ByteArrayInputStream(item.document.source().getBytes(StandardCharsets.UTF_8)),
                        XContentType.JSON
                    );
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
