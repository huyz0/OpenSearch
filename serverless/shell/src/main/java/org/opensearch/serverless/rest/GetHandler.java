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
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code GET /{index}/_doc/{id}} — reading back what was written.
 *
 * <p>Until this existed a document could be written, deleted and searched for, but not read by id. It is
 * the smallest gap in the surface and the most conspicuous one to anyone trying the system.
 *
 * <p><b>A get is routed to the writer, which is the opposite of how a search is routed, and the
 * difference is not an inconsistency.</b> A search fans out to readers because it answers from published
 * commits and placement is a hint a node can override by opening the shard itself. A get cannot work that
 * way: a write is acknowledged once it is in the log and applied to the engine, but it is not searchable
 * until a refresh and not in the object store until a publish. A get served from a published commit would
 * therefore fail to find a document the caller had just been told was written — data loss, as far as
 * anyone can tell from outside. So the owner answers, and it answers realtime.
 *
 * <p><b>When nobody owns the shard, a published commit is the right answer rather than a compromise.</b>
 * An index nobody is writing has no owner and no unpublished writes, so the commit <em>is</em> the current
 * state, and serving it is what lets a get work against an index that has scaled to zero — the same
 * property the search path already has.
 *
 * <p><b>When somebody owns the shard and cannot be reached, this refuses.</b> That is the case worth being
 * careful about: falling back to a published commit there would answer from a copy that is knowably behind
 * a live writer, returning a stale document, or a 404 for a document that exists, while reporting success.
 * A 503 says "ask again", which is true, instead of a wrong answer that looks right.
 */
public final class GetHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public GetHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_get_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/{index}/_doc/{id}"), new Route(RestRequest.Method.HEAD, "/{index}/_doc/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Every parameter read before any early return, or BaseRestHandler turns a deliberate refusal
        // into a 400 about an unconsumed parameter.
        final String index = request.param("index");
        final String id = request.param("id");
        final boolean bodyless = request.method() == RestRequest.Method.HEAD;
        // What the caller wants back. Shapes the response only -- the document is fetched whole either
        // way; see SourceFiltering.
        final org.opensearch.search.fetch.subphase.FetchSourceContext fetchSource = org.opensearch.search.fetch.subphase.FetchSourceContext
            .parseFromRestRequest(request);

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final Optional<IndexDescriptor> descriptor = metadata.describe(index);
        if (descriptor.isEmpty()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index)
            );
        }

        final int shard = DocumentRouting.shardFor(descriptor.get(), id);
        final ServerlessNode serving = node.get();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GET).execute(() -> {
            try {
                answer(channel, serving, metadata, index, id, shard, bodyless, fetchSource);
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a read failure", nested);
                }
            }
        });
    }

    private void answer(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        String index,
        String id,
        int shard,
        boolean bodyless,
        org.opensearch.search.fetch.subphase.FetchSourceContext fetchSource
    ) throws Exception {
        final var operations = new org.opensearch.serverless.shard.ShardOperations(serving, metadata);
        try {
            final var read = operations.get(index, id);
            respond(channel, index, shard, read.document(), read.servedBy(), read.realtime(), bodyless, fetchSource);
        } catch (org.opensearch.serverless.shard.ShardOperations.NotHereException e) {
            // Every "not here" is a 503 for a get, and deliberately so: the alternative is answering from
            // a published commit that a live writer is already ahead of, which is a stale document -- or a
            // 404 for a document that exists -- reported as success.
            channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.SERVICE_UNAVAILABLE,
                    e.owner() != null && e.owner().equals(serving.localNode().getId())
                        ? "activation_in_progress"
                        : (e.getMessage().contains("could not forward") ? "forward_failed" : "owner_unreachable"),
                    e.getMessage()
                )
            );
        }
    }

    private void respond(
        org.opensearch.rest.RestChannel channel,
        String index,
        int shard,
        ServerlessNode.Document document,
        String servedBy,
        boolean realtime,
        boolean bodyless,
        org.opensearch.search.fetch.subphase.FetchSourceContext fetchSource
    ) throws IOException {
        final RestStatus status = document.found() ? RestStatus.OK : RestStatus.NOT_FOUND;
        if (bodyless) {
            // HEAD is an existence check, and a body on it would be discarded by any correct client.
            channel.sendResponse(new BytesRestResponse(status, BytesRestResponse.TEXT_CONTENT_TYPE, ""));
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("_index", index);
            builder.field("_id", document.id());
            builder.field("_shard", shard);
            builder.field("found", document.found());
            if (document.found()) {
                // The token a caller sends back as if_seq_no. Only on a found document: there is no
                // sequence identity for a document that does not exist, and emitting the unassigned
                // sentinel would look like one.
                builder.field("_version", document.version());
                builder.field("_seq_no", document.seqNo());
                builder.field("_primary_term", document.primaryTerm());
            }
            // Which copy answered, said plainly rather than left to be inferred. A realtime answer comes
            // from the shard's owner and includes writes that are acknowledged but not yet published; a
            // non-realtime one comes from a published commit, which is the whole truth only while nobody
            // is writing. A client that cares about the difference should not have to guess.
            builder.field("realtime", realtime);
            builder.field("_node", servedBy);
            final String source = document.found() ? SourceFiltering.apply(document.source(), fetchSource) : null;
            if (source != null) {
                builder.rawField("_source", new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        }
    }
}
