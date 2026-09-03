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
            new Route(RestRequest.Method.DELETE, "/_alias/{name}"),
            // OpenSearch's own spellings. This shell shipped a name-scoped API of its own invention and
            // refused these, which was the last shape on this surface that differed from classic for no
            // architectural reason -- the category M47 cleared out everywhere else.
            new Route(RestRequest.Method.PUT, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.POST, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.GET, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.HEAD, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.DELETE, "/{index}/_alias/{name}"),
            // The bulk action API, for the one case it exists to serve. See actions().
            new Route(RestRequest.Method.POST, "/_aliases")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        final String indexParam = request.param("index");
        final RestRequest.Method method = request.method();
        final boolean bulk = request.path().equals("/_aliases");
        final List<String> indices = new ArrayList<>();
        if (indexParam != null) {
            // The index-scoped spellings name their index in the path rather than in a body.
            for (String each : indexParam.split(",")) {
                if (each.isBlank() == false) {
                    indices.add(each.trim());
                }
            }
        }
        if (method == RestRequest.Method.PUT && indexParam == null) {
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

        if (bulk) {
            final String body;
            try {
                body = request.hasContent() ? request.content().utf8ToString() : null;
            } catch (Exception e) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "malformed_body", "could not read the actions")
                );
            }
            return channel -> {
                try {
                    actions(channel, metadata, body);
                } catch (Exception e) {
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    } catch (IOException nested) {
                        logger.error("failed to report an alias action failure", nested);
                    }
                }
            };
        }

        return channel -> {
            try {
                if (indexParam != null) {
                    switch (method) {
                        case PUT, POST -> attach(channel, metadata, name, indices, true);
                        case DELETE -> attach(channel, metadata, name, indices, false);
                        case GET, HEAD -> membership(channel, metadata, name, indices, method == RestRequest.Method.HEAD);
                        default -> channel.sendResponse(
                            IndexAdminHandler.error(
                                channel,
                                RestStatus.METHOD_NOT_ALLOWED,
                                "method_not_allowed",
                                method + " is not supported here"
                            )
                        );
                    }
                    return;
                }
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

    /**
     * Adds indices to an alias, or removes them, creating and deleting the alias as needed.
     *
     * <p>This is what {@code PUT /{index}/_alias/{name}} means: an alias is a set of indices, and naming one
     * index adds it to that set rather than replacing it. Removing the last index removes the alias, because
     * an alias standing for nothing resolves to nothing and reads exactly like an empty index — the failure
     * this whole surface is arranged to avoid.
     *
     * @param channel the channel to answer on
     * @param metadata the metadata plane
     * @param name the alias
     * @param indices the indices to add or remove
     * @param adding whether to add or remove them
     * @throws IOException if the read or the swap fails
     */
    private void attach(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String name, List<String> indices, boolean adding)
        throws IOException {
        if (adding) {
            for (String index : indices) {
                if (metadata.describe(index).isEmpty()) {
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_FOUND,
                            "index_not_found",
                            "cannot alias [" + index + "]: no such index"
                        )
                    );
                    return;
                }
            }
        }
        final String failure = swap(metadata, name, indices, adding);
        if (failure != null) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "alias_not_updated", failure));
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

    /**
     * Applies one add-or-remove to an alias under a compare-and-swap, retrying a lost race.
     *
     * @param metadata the metadata plane
     * @param name the alias
     * @param indices the indices to add or remove
     * @param adding whether to add or remove
     * @return a failure to report, or null on success
     * @throws IOException if the store cannot be read or written
     */
    private String swap(MetadataPlane metadata, String name, List<String> indices, boolean adding) throws IOException {
        for (int attempt = 0; attempt < 4; attempt++) {
            final var resolved = metadata.resolve(name);
            if (resolved.index() != null) {
                return "[" + name + "] is an index, not an alias; a name cannot be both";
            }
            final List<String> now = new ArrayList<>(resolved.alias() == null ? List.of() : resolved.alias().indices());
            for (String index : indices) {
                if (adding) {
                    if (now.contains(index) == false) {
                        now.add(index);
                    }
                } else {
                    now.remove(index);
                }
            }
            if (resolved.alias() == null) {
                if (adding == false) {
                    return "no such alias: " + name;
                }
                try {
                    metadata.createAlias(new org.opensearch.serverless.cluster.AliasRecord(name, now));
                    return null;
                } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
                    continue; // somebody created it between the read and the write; re-read and merge
                }
            }
            if (now.isEmpty()) {
                // The last index left. An alias over nothing is worse than no alias, so it goes.
                metadata.deleteAlias(name);
                return null;
            }
            final long generation = metadata.aliasGeneration(name);
            if (metadata.updateAlias(new org.opensearch.serverless.cluster.AliasRecord(name, now), generation).isPresent()) {
                return null;
            }
        }
        return "[" + name + "] is being changed concurrently; retry";
    }

    /**
     * Whether an index is under an alias.
     *
     * @param channel the channel to answer on
     * @param metadata the metadata plane
     * @param name the alias
     * @param indices the indices asked about
     * @param bodyless whether this is a HEAD
     * @throws IOException if the read fails
     */
    private void membership(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        String name,
        List<String> indices,
        boolean bodyless
    ) throws IOException {
        final var resolved = metadata.resolve(name);
        final boolean all = resolved.alias() != null && resolved.alias().indices().containsAll(indices);
        if (bodyless) {
            channel.sendResponse(
                new BytesRestResponse(all ? RestStatus.OK : RestStatus.NOT_FOUND, BytesRestResponse.TEXT_CONTENT_TYPE, "")
            );
            return;
        }
        if (all == false) {
            channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "alias_not_found", "alias [" + name + "] does not cover " + indices)
            );
            return;
        }
        // OpenSearch's shape: keyed by index, each carrying the aliases it is under that were asked about.
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            for (String index : indices) {
                builder.startObject(index);
                builder.startObject("aliases");
                builder.startObject(name).endObject();
                builder.endObject();
                builder.endObject();
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * {@code POST /_aliases}, for the one thing it exists to do.
     *
     * <p><b>Why this is served for a single alias and refused across several.</b> The API's purpose is the
     * atomic move: remove an alias from yesterday's index and add it to today's, so a caller searching it sees
     * one or the other and never neither. Every action on <em>one</em> alias touches one register, so the
     * whole move is one compare-and-swap and is genuinely atomic. Actions spanning several aliases are several
     * registers with no transaction over them, and serving that would be a different operation wearing the
     * same name — the objection that keeps {@code PUT /_settings} off patterns.
     *
     * @param channel the channel to answer on
     * @param metadata the metadata plane
     * @param body the actions
     * @throws IOException if the read or the swap fails
     */
    private void actions(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String body) throws IOException {
        if (body == null || body.isBlank()) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "an actions body is required"));
            return;
        }
        final java.util.Map<String, Object> parsed;
        try {
            parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                new org.opensearch.core.common.bytes.BytesArray(body.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
        } catch (Exception e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "malformed_body", "could not parse the actions"));
            return;
        }

        final List<java.util.Map<String, Object>> steps = new ArrayList<>();
        final java.util.Set<String> aliases = new java.util.LinkedHashSet<>();
        if (parsed.get("actions") instanceof List<?> given) {
            for (Object each : given) {
                if (each instanceof java.util.Map<?, ?> action && action.size() == 1) {
                    final var entry = action.entrySet().iterator().next();
                    if (entry.getValue() instanceof java.util.Map<?, ?> detail) {
                        final java.util.Map<String, Object> step = new java.util.LinkedHashMap<>();
                        step.put("op", String.valueOf(entry.getKey()));
                        step.put("alias", String.valueOf(detail.get("alias")));
                        step.put("index", String.valueOf(detail.get("index")));
                        steps.add(step);
                        aliases.add(String.valueOf(detail.get("alias")));
                    }
                }
            }
        }
        if (steps.isEmpty()) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "no_actions", "the body named no actions"));
            return;
        }
        if (aliases.size() > 1) {
            channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_action",
                    "actions spanning more than one alias are not supported: each alias is its own "
                        + "compare-and-swap and there is no transaction across them, so a request touching "
                        + aliases
                        + " could apply partly. Send one request per alias, or keep the actions to one alias, "
                        + "which is the atomic move this API exists for"
                )
            );
            return;
        }

        final String alias = aliases.iterator().next();
        for (int attempt = 0; attempt < 4; attempt++) {
            final var resolved = metadata.resolve(alias);
            if (resolved.index() != null) {
                channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "name_taken", "[" + alias + "] is an index, not an alias")
                );
                return;
            }
            final List<String> now = new ArrayList<>(resolved.alias() == null ? List.of() : resolved.alias().indices());
            for (java.util.Map<String, Object> step : steps) {
                final String index = String.valueOf(step.get("index"));
                if ("add".equals(step.get("op"))) {
                    if (metadata.describe(index).isEmpty()) {
                        channel.sendResponse(
                            IndexAdminHandler.error(
                                channel,
                                RestStatus.NOT_FOUND,
                                "index_not_found",
                                "cannot alias [" + index + "]: no such index"
                            )
                        );
                        return;
                    }
                    if (now.contains(index) == false) {
                        now.add(index);
                    }
                } else if ("remove".equals(step.get("op"))) {
                    now.remove(index);
                } else {
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "unsupported_action",
                            "'" + step.get("op") + "' is not an alias action here; add and remove are"
                        )
                    );
                    return;
                }
            }

            final boolean applied;
            if (resolved.alias() == null) {
                if (now.isEmpty()) {
                    channel.sendResponse(
                        IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "alias_not_found", "no such alias: " + alias)
                    );
                    return;
                }
                try {
                    metadata.createAlias(new org.opensearch.serverless.cluster.AliasRecord(alias, now));
                    applied = true;
                } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
                    continue;
                }
            } else if (now.isEmpty()) {
                metadata.deleteAlias(alias);
                applied = true;
            } else {
                applied = metadata.updateAlias(
                    new org.opensearch.serverless.cluster.AliasRecord(alias, now),
                    metadata.aliasGeneration(alias)
                ).isPresent();
            }
            if (applied) {
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    builder.field("acknowledged", true);
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
                return;
            }
        }
        channel.sendResponse(
            IndexAdminHandler.error(
                channel,
                RestStatus.CONFLICT,
                "version_conflict_engine_exception",
                "[" + alias + "] is being changed concurrently; retry"
            )
        );
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
