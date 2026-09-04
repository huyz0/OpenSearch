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
        private final org.opensearch.search.fetch.subphase.FetchSourceContext fetchSource;
        private ServerlessNode.Document document;
        private String failureType;
        private String failureReason;

        Item(String index, String id, org.opensearch.search.fetch.subphase.FetchSourceContext fetchSource) {
            this.index = index;
            this.id = id;
            this.fetchSource = fetchSource;
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
        // What the request as a whole asked for, which an individual document may override.
        final org.opensearch.search.fetch.subphase.FetchSourceContext requestSource =
            org.opensearch.search.fetch.subphase.FetchSourceContext.parseFromRestRequest(request);
        final String storedFields = request.param("stored_fields");
        final String routing = request.param("routing");
        // Hints; see GetHandler.
        request.param("preference");
        request.param("realtime");
        request.param("refresh");
        if (storedFields != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_read",
                    "stored_fields is not supported: a get here returns the document's source, filtered by _source"
                )
            );
        }
        if (routing != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_read",
                    "routing is not supported: a document is placed by its id alone"
                )
            );
        }
        if (request.hasContentOrSourceParam() == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a multi-get needs a body naming the documents")
            );
        }

        final List<Item> items = new ArrayList<>();
        final String malformed = parse(request, defaultIndex, requestSource, items);
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
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
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
    private String parse(
        RestRequest request,
        String defaultIndex,
        org.opensearch.search.fetch.subphase.FetchSourceContext requestSource,
        List<Item> items
    ) throws IOException {
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
                    // A document may name its own _source, which is the whole reason a multi-get takes
                    // objects rather than ids: one request can want the body of one document and only the
                    // field names of another.
                    final Item item = new Item(index.toString(), id.toString(), perDocumentSource(doc.get("_source"), requestSource));
                    // Refused per item rather than dropped: a document's own routing or stored_fields
                    // used to be read into the map and never looked at again.
                    if (doc.get("routing") != null || doc.get("_routing") != null) {
                        item.fail("unsupported_read", "routing is not supported: a document is placed by its id alone");
                    } else if (doc.get("stored_fields") != null || doc.get("_stored_fields") != null) {
                        item.fail("unsupported_read", "stored_fields is not supported: a get here returns the document's source");
                    }
                    items.add(item);
                }
                return null;
            }
            if (body.get("ids") instanceof List<?> ids) {
                if (defaultIndex == null) {
                    return "a bare list of ids needs an index in the request path";
                }
                for (Object id : ids) {
                    items.add(new Item(defaultIndex, String.valueOf(id), requestSource));
                }
                return null;
            }
            return "a multi-get body needs either docs or ids";
        } catch (Exception e) {
            return "the body could not be read: " + e.getMessage();
        }
    }

    /**
     * Reads one document's {@code _source} instruction, falling back to the request's.
     *
     * <p>The three shapes the real API takes: a boolean, a list of includes, or an object with includes and
     * excludes. Anything else is left to the request's own setting rather than guessed at.
     */
    private static org.opensearch.search.fetch.subphase.FetchSourceContext perDocumentSource(
        Object declared,
        org.opensearch.search.fetch.subphase.FetchSourceContext fallback
    ) {
        if (declared == null) {
            return fallback;
        }
        if (declared instanceof Boolean wanted) {
            return new org.opensearch.search.fetch.subphase.FetchSourceContext(wanted);
        }
        if (declared instanceof List<?> includes) {
            return new org.opensearch.search.fetch.subphase.FetchSourceContext(
                true,
                strings(includes),
                org.opensearch.core.common.Strings.EMPTY_ARRAY
            );
        }
        if (declared instanceof java.util.Map<?, ?> object) {
            final Object includes = object.get("includes") == null ? object.get("include") : object.get("includes");
            final Object excludes = object.get("excludes") == null ? object.get("exclude") : object.get("excludes");
            return new org.opensearch.search.fetch.subphase.FetchSourceContext(
                true,
                includes instanceof List<?> list ? strings(list) : org.opensearch.core.common.Strings.EMPTY_ARRAY,
                excludes instanceof List<?> list ? strings(list) : org.opensearch.core.common.Strings.EMPTY_ARRAY
            );
        }
        return fallback;
    }

    private static String[] strings(List<?> values) {
        final String[] out = new String[values.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = String.valueOf(values.get(i));
        }
        return out;
    }

    /** Fetches every item at once, up to the fan-out's bound. */
    private void fetch(ServerlessNode serving, MetadataPlane metadata, List<Item> items) throws IOException {
        final ShardOperations operations = new ShardOperations(serving, metadata, true);
        final List<Callable<Boolean>> tasks = new ArrayList<>(items.size());
        for (Item item : items) {
            if (item.failureType != null) {
                // Refused while parsing; there is nothing to read for it.
                continue;
            }
            if (serving.isSystemIndex(item.index)) {
                // The registration-time guard reads only the index in the request path, and a multi-get
                // names its indices in the body: POST /_mget {"docs":[{"_index":".serverless_auth",...}]}
                // walked straight past it to the credential records. Refused here, per item, and again in
                // ShardOperations.place for whatever body-addressed handler comes next.
                item.fail("system_index", "[" + item.index + "] belongs to a plugin and is not reachable through the request path");
                continue;
            }
            tasks.add(() -> {
                try {
                    item.document = operations.get(item.index, item.id).document();
                    return true;
                } catch (ShardOperations.NoSuchIndexException e) {
                    item.fail("index_not_found", e.getMessage());
                } catch (ShardOperations.SystemIndexException e) {
                    item.fail("system_index", e.getMessage());
                } catch (ShardOperations.NotHereException e) {
                    // The get endpoint's vocabulary, not a batch-shaped approximation of it.
                    item.fail(e.restType(serving.localNode().getId()), e.getMessage());
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
                if (found) {
                    // The same token a single get reports, so a caller building a conditional write from a
                    // multi-get has one. These were never rendered here, only on GET /{index}/_doc/{id}.
                    builder.field("_version", item.document.version());
                    builder.field("_seq_no", item.document.seqNo());
                    builder.field("_primary_term", item.document.primaryTerm());
                }
                builder.field("found", found);
                final String source = found ? SourceFiltering.apply(item.document.source(), item.fetchSource) : null;
                if (source != null) {
                    builder.rawField("_source", new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
