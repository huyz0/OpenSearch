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
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code /_alias/{name}} — naming a set of indices.
 *
 * <p>An alias is one register holding a list of index names, in the same namespace as the descriptors, so
 * resolving one costs a single read and a name cannot be both an index and an alias. See
 * {@link AliasRecord} for why that namespace is shared rather than separate.
 *
 * <p><b>There is no {@code GET /_alias} that lists them all</b>, and there will not be one for the same
 * reason there is no way to list indices: enumerating a deployment is not an operation this system offers
 * on a request path (&sect;6.3). An alias is looked up by name, like everything else here.
 *
 * <p><b>Replacing an alias is a delete and a create.</b> Offering an in-place update would mean a
 * read-modify-write on a register that something may be resolving at the same moment, and the compare-and
 * -swap to make that safe is real machinery for an operation nobody performs in a loop. Saying so is better
 * than offering an update that quietly loses a concurrent one.
 */
public final class AliasHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     */
    public AliasHandler(Supplier<MetadataPlane> plane) {
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_alias_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_alias/{name}"),
            new Route(RestRequest.Method.GET, "/_alias/{name}"),
            new Route(RestRequest.Method.DELETE, "/_alias/{name}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        final RestRequest.Method method = request.method();
        final List<String> indices = new ArrayList<>();
        if (method == RestRequest.Method.PUT) {
            if (request.hasContentOrSourceParam() == false) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "an alias needs a body naming its indices")
                );
            }
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                final var body = parser.map();
                if (body.get("indices") instanceof List<?> listed) {
                    for (Object index : listed) {
                        indices.add(String.valueOf(index));
                    }
                }
            }
            if (indices.isEmpty()) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.BAD_REQUEST,
                        "no_indices",
                        "an alias must name at least one index; an alias that stands for nothing would resolve to nothing"
                    )
                );
            }
        }

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        return channel -> {
            try {
                switch (method) {
                    case PUT -> create(channel, metadata, name, indices);
                    case GET -> read(channel, metadata, name);
                    case DELETE -> delete(channel, metadata, name);
                    default -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.METHOD_NOT_ALLOWED,
                            "method_not_allowed",
                            method + " is not supported here"
                        )
                    );
                }
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report an alias failure", nested);
                }
            }
        };
    }

    private void create(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String name, List<String> indices)
        throws IOException {
        // Every index named has to exist. An alias created over a typo would resolve to nothing and look
        // like an empty index, which is the failure this whole surface is arranged to avoid -- and unlike a
        // search, the mistake here is permanent until somebody notices.
        for (String index : indices) {
            if (metadata.describe(index).isEmpty()) {
                channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "cannot alias [" + index + "]: no such index")
                );
                return;
            }
        }
        try {
            metadata.createAlias(new AliasRecord(name, indices));
        } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
            channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "name_taken",
                    "[" + name + "] is already an index or an alias; a name cannot be both"
                )
            );
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.field("alias", name);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void read(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String name) throws IOException {
        final var resolved = metadata.resolve(name);
        if (resolved.alias() == null) {
            channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_FOUND,
                    "alias_not_found",
                    resolved.index() != null ? "[" + name + "] is an index, not an alias" : "no such alias: " + name
                )
            );
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("alias", name);
            builder.startArray("indices");
            for (String index : resolved.alias().indices()) {
                builder.value(index);
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void delete(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String name) throws IOException {
        if (metadata.deleteAlias(name) == false) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "alias_not_found", "no such alias: " + name));
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.field("alias", name);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
