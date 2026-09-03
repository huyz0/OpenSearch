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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code _validate/query} and {@code _resolve/index} — two questions that need no shard and no cluster.
 *
 * <p><b>Validating a query is parsing it.</b> A caller sends a query to find out whether it is well formed
 * before committing to a search that might be expensive, and the answer is whatever the parser says. This
 * shell already parses queries on every search through the node's own registry; the endpoint reports what
 * that parse did rather than running anything.
 *
 * <p><b>Resolving a name is one bounded listing.</b> {@code _resolve/index} says which of the names a caller
 * typed are indices, which are aliases, and which are neither. Every part of that is a descriptor read or the
 * same prefix listing search wildcards use, so it is bounded the same way and refuses past the same cap
 * rather than truncating.
 *
 * <p>Both were refused before, with reasons that were true of neither: validation was called unimplemented
 * and resolution was called an enumeration. Resolution over a <em>named</em> set is not an enumeration, and
 * that distinction is the same one {@code _list/indices/{prefix}*} rests on.
 */
public final class ValidateAndResolveHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public ValidateAndResolveHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_validate_resolve_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/{index}/_validate/query"),
            new Route(RestRequest.Method.POST, "/{index}/_validate/query"),
            new Route(RestRequest.Method.GET, "/_resolve/index/{name}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final boolean resolving = request.path().startsWith("/_resolve/index/");
        final String index = request.param("index");
        final String names = request.param("name");
        final boolean explain = request.paramAsBoolean("explain", false);
        final String body = request.hasContent() ? request.content().utf8ToString() : null;

        final MetadataPlane metadata = plane.get();
        final var serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                if (resolving) {
                    resolve(channel, metadata, serving, names);
                } else {
                    validate(channel, metadata, serving, index, body, explain);
                }
            } catch (org.opensearch.serverless.metadata.DescriptorStore.TooManyMatchesException e) {
                sendQuietly(channel, RestStatus.BAD_REQUEST, "too_many_indices", e.getMessage());
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a validate or resolve failure", nested);
                }
            }
        });
    }

    private void validate(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        String index,
        String body,
        boolean explain
    ) throws IOException {
        if (metadata.describe(index).isEmpty() && IndexPatterns.isPrefixPattern(index) == false) {
            sendQuietly(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index);
            return;
        }
        if (body == null || body.isBlank()) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "missing_body", "a query body is required");
            return;
        }

        String failure = null;
        try (
            XContentParser parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                .createParser(
                    serving.searchXContentRegistry(),
                    org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    new java.io.ByteArrayInputStream(body.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                )
        ) {
            SearchSourceBuilder.fromXContent(parser, true);
        } catch (Exception e) {
            failure = e.getMessage() == null ? e.toString() : e.getMessage();
        }

        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("valid", failure == null);
            // Reported the way search reports it, so a caller cannot tell from the shape which endpoint
            // answered. Nothing was searched, so every shard "succeeded" at the thing that was asked.
            builder.startObject("_shards");
            builder.field("total", 0);
            builder.field("successful", 0);
            builder.field("failed", 0);
            builder.endObject();
            if (failure != null && explain) {
                // Only when asked, as OpenSearch does: a caller checking a boolean should not have to read
                // past an explanation it did not want.
                builder.startArray("explanations");
                builder.startObject();
                builder.field("index", index);
                builder.field("valid", false);
                builder.field("error", failure);
                builder.endObject();
                builder.endArray();
            }
            // Said plainly, because "valid" here is narrower than a caller may assume: the query parses.
            // Whether every field it names exists is a question about a mapping this endpoint does not read.
            builder.field("validated", "parsing only; field existence is not checked");
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void resolve(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        String names
    ) throws IOException {
        final List<String> wanted = IndexPatterns.expand(metadata, names, serving.patternCap());
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("indices");
            for (String name : wanted) {
                if (metadata.describe(name).isPresent()) {
                    builder.startObject();
                    builder.field("name", name);
                    builder.startArray("attributes").value("open").endArray();
                    builder.endObject();
                }
            }
            builder.endArray();
            builder.startArray("aliases");
            for (String name : wanted) {
                final var resolved = metadata.resolve(name);
                if (resolved.alias() != null) {
                    builder.startObject();
                    builder.field("name", name);
                    builder.startArray("indices");
                    for (String index : resolved.alias().indices()) {
                        builder.value(index);
                    }
                    builder.endArray();
                    builder.endObject();
                }
            }
            builder.endArray();
            // Data streams do not exist here. The key is present and empty rather than absent, because a
            // client walking all three arrays should not have to branch on which of them this deployment has.
            builder.startArray("data_streams").endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void sendQuietly(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report a refusal", e);
        }
    }
}
