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
        this(plane, () -> null);
    }

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the node, for its action gate and system-index list
     */
    public AliasHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    @Override
    public String getName() {
        return "serverless_alias_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_alias/{name}"),
            new Route(RestRequest.Method.POST, "/_alias/{name}"),
            new Route(RestRequest.Method.PUT, "/_aliases/{name}"),
            new Route(RestRequest.Method.POST, "/_aliases/{name}"),
            new Route(RestRequest.Method.GET, "/_alias/{name}"),
            new Route(RestRequest.Method.HEAD, "/_alias/{name}"),
            new Route(RestRequest.Method.DELETE, "/_alias/{name}"),
            // OpenSearch's own spellings. This shell shipped a name-scoped API of its own invention and
            // refused these, which was the last shape on this surface that differed from classic for no
            // architectural reason -- the category M47 cleared out everywhere else.
            new Route(RestRequest.Method.PUT, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.POST, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.GET, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.HEAD, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.DELETE, "/{index}/_alias/{name}"),
            // The _aliases spelling of the same thing, and the form that names the alias in its body.
            new Route(RestRequest.Method.PUT, "/{index}/_aliases/{name}"),
            new Route(RestRequest.Method.POST, "/{index}/_aliases/{name}"),
            new Route(RestRequest.Method.DELETE, "/{index}/_aliases/{name}"),
            new Route(RestRequest.Method.PUT, "/{index}/_alias"),
            new Route(RestRequest.Method.PUT, "/{index}/_aliases"),
            // GET /{index}/_alias: which aliases name an index, from the descriptor's verified hint.
            new Route(RestRequest.Method.GET, "/{index}/_alias"),
            // The bulk action API, for the one case it exists to serve. See actions().
            new Route(RestRequest.Method.POST, "/_aliases")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String name = request.param("name");
        final String indexParam = request.param("index");
        final RestRequest.Method method = request.method();
        final boolean bulk = request.path().equals("/_aliases");
        // Hints: there is no cluster manager to time out against or be local to.
        for (String hint : new String[] {
            "timeout",
            "master_timeout",
            "cluster_manager_timeout",
            "local",
            "ignore_unavailable",
            "allow_no_indices",
            "expand_wildcards" }) {
            request.param(hint);
        }
        if (name == null && indexParam != null && method == RestRequest.Method.GET) {
            // The reverse question, answered from the hint each descriptor carries and verified against the
            // alias records it names -- bounded by the aliases on this index, not by the aliases that exist.
            final MetadataPlane reverse = plane.get();
            if (reverse == null) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
                );
            }
            return channel -> {
                try {
                    aliasesOf(channel, reverse, indexParam);
                } catch (Exception e) {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                }
            };
        }
        if (name == null && bulk == false && request.hasContentOrSourceParam()) {
            // PUT /{index}/_alias with the alias named in the body, which core accepts as "alias" or "name".
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                final var body = parser.map();
                final Object named = body.get("alias") == null ? body.get("name") : body.get("alias");
                if (named instanceof String one && one.isBlank() == false) {
                    name = one;
                }
            }
        }
        if (name == null && bulk == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "missing_alias",
                    "name the alias, in the path or as 'alias' in the body"
                )
            );
        }
        final String aliasName = name;
        final List<String> indices = new ArrayList<>();
        if (indexParam != null) {
            // The index-scoped spellings name their index in the path rather than in a body.
            for (String each : indexParam.split(",")) {
                if (each.isBlank() == false) {
                    indices.add(each.trim());
                }
            }
        }
        // Alias options, refused on every spelling rather than dropped. A filter, a routing value or a
        // write-index flag used to be read into the body map and never looked at again, so a filtered
        // alias was created unfiltered with {"acknowledged": true} -- and the index-scoped spelling never
        // parsed its body at all.
        if ((method == RestRequest.Method.PUT || method == RestRequest.Method.POST) && bulk == false && request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                final String option = unsupportedAliasOption(parser.map());
                if (option != null) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_alias_option", option)
                    );
                }
            }
        }
        if ((method == RestRequest.Method.PUT || method == RestRequest.Method.POST) && indexParam == null && bulk == false) {
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
                    IndexAdminHandler.gate(
                        node.get(),
                        org.opensearch.action.admin.indices.alias.IndicesAliasesAction.NAME,
                        new org.opensearch.action.admin.indices.alias.IndicesAliasesRequest(),
                        () -> {
                            actions(channel, metadata, body);
                            return null;
                        }
                    );
                } catch (Exception e) {
                    try {
                        channel.sendResponse(IndexAdminHandler.failure(channel, e));
                    } catch (IOException nested) {
                        logger.error("failed to report an alias action failure", nested);
                    }
                }
            };
        }

        final String gateAction = method == RestRequest.Method.GET || method == RestRequest.Method.HEAD
            ? org.opensearch.action.admin.indices.alias.get.GetAliasesAction.NAME
            : org.opensearch.action.admin.indices.alias.IndicesAliasesAction.NAME;
        final org.opensearch.action.ActionRequest gateRequest = method == RestRequest.Method.GET || method == RestRequest.Method.HEAD
            ? new org.opensearch.action.admin.indices.alias.get.GetAliasesRequest(aliasName).indices(indices.toArray(new String[0]))
            : new org.opensearch.action.admin.indices.alias.IndicesAliasesRequest();
        return channel -> {
            try {
                IndexAdminHandler.gate(node.get(), gateAction, gateRequest, () -> {
                    dispatch(channel, metadata, method, indexParam, aliasName, indices);
                    return null;
                });
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report an alias failure", nested);
                }
            }
        };
    }

    private void dispatch(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        RestRequest.Method method,
        String indexParam,
        String aliasName,
        List<String> indices
    ) throws Exception {
        {
            try {
                if (indexParam != null) {
                    switch (method) {
                        case PUT, POST -> attach(channel, metadata, aliasName, indices, true);
                        case DELETE -> attach(channel, metadata, aliasName, indices, false);
                        case GET, HEAD -> membership(channel, metadata, aliasName, indices, method == RestRequest.Method.HEAD);
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
                    case PUT, POST -> create(channel, metadata, aliasName, indices);
                    case GET -> read(channel, metadata, aliasName);
                    case HEAD -> {
                        // An existence check on a name: the same one register read a GET does, and no body.
                        final boolean exists = metadata.resolve(aliasName).alias() != null;
                        channel.sendResponse(new BytesRestResponse(exists ? RestStatus.OK : RestStatus.NOT_FOUND, "application/json", ""));
                    }
                    case DELETE -> delete(channel, metadata, aliasName);
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
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report an alias failure", nested);
                }
            }
        }
        ;
    }

    /** Whether an index is a plugin's, which no alias may name: an alias over one is a read of it by another name. */
    private boolean isSystemIndex(String index) {
        final var serving = node.get();
        return serving != null && serving.isSystemIndex(index);
    }

    /** One name or a list of them, from whichever of the two spellings the body used. */
    private static List<String> namesOf(Object single, Object many) {
        final List<String> names = new ArrayList<>();
        if (single instanceof String one && one.isBlank() == false) {
            names.add(one);
        } else if (single instanceof List<?> list) {
            for (Object each : list) {
                names.add(String.valueOf(each));
            }
        }
        if (many instanceof List<?> list) {
            for (Object each : list) {
                names.add(String.valueOf(each));
            }
        } else if (many instanceof String one && one.isBlank() == false) {
            names.add(one);
        }
        return names;
    }

    /**
     * Names the first alias option a body carries that this design cannot honour, or null.
     *
     * <p>An alias here is a name for a set of indices and nothing else. A filter would make a search
     * through the alias narrower than a search of its indices, routing would place documents by a value
     * this system does not route on, and is_write_index would make one of several indices the target of a
     * write through the alias -- each a promise that would be accepted and not kept.
     */
    static String unsupportedAliasOption(java.util.Map<?, ?> body) {
        if (body.get("filter") != null) {
            return "filter is not supported: an alias here names indices and does not narrow them";
        }
        if (body.get("routing") != null || body.get("index_routing") != null || body.get("search_routing") != null) {
            return "routing on an alias is not supported: a document is placed by its id alone";
        }
        if (Boolean.TRUE.equals(body.get("is_write_index"))) {
            return "is_write_index is not supported: writes here name an index, and an alias resolves on read";
        }
        if (Boolean.TRUE.equals(body.get("is_hidden"))) {
            return "is_hidden is not supported: aliases are found by name and never listed, so there is nothing to hide from";
        }
        return null;
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
                if (isSystemIndex(index)) {
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.FORBIDDEN,
                            "system_index",
                            "[" + index + "] belongs to a plugin and cannot be aliased"
                        )
                    );
                    return;
                }
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
                if (!(each instanceof java.util.Map<?, ?> action) || action.size() != 1) {
                    // Refused rather than skipped: an action that is not one verb with one body is a
                    // malformed request, and skipping it answered the rest with "acknowledged".
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "malformed_body",
                            "each action must be one object with one verb"
                        )
                    );
                    return;
                }
                final var entry = action.entrySet().iterator().next();
                if (!(entry.getValue() instanceof java.util.Map<?, ?> detail)) {
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "malformed_body",
                            "the body of '" + entry.getKey() + "' must be an object"
                        )
                    );
                    return;
                }
                final String option = unsupportedAliasOption(detail);
                if (option != null) {
                    channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_alias_option", option));
                    return;
                }
                // Both spellings core takes: one index or alias, or a list. A list used to be unread, so
                // the action resolved to the literal string "null" and failed as a missing index.
                final List<String> names = namesOf(detail.get("index"), detail.get("indices"));
                final List<String> aliasNames = namesOf(detail.get("alias"), detail.get("aliases"));
                if (names.isEmpty() || aliasNames.isEmpty()) {
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "malformed_body",
                            "'" + entry.getKey() + "' must name an index (or indices) and an alias (or aliases)"
                        )
                    );
                    return;
                }
                for (String aliasName : aliasNames) {
                    for (String name : names) {
                        final java.util.Map<String, Object> step = new java.util.LinkedHashMap<>();
                        step.put("op", String.valueOf(entry.getKey()));
                        step.put("alias", aliasName);
                        step.put("index", name);
                        step.put("must_exist", Boolean.TRUE.equals(detail.get("must_exist")));
                        steps.add(step);
                    }
                    aliases.add(aliasName);
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
                    if (isSystemIndex(index)) {
                        channel.sendResponse(
                            IndexAdminHandler.error(
                                channel,
                                RestStatus.FORBIDDEN,
                                "system_index",
                                "[" + index + "] belongs to a plugin and cannot be aliased"
                            )
                        );
                        return;
                    }
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
                    if (now.contains(index) == false && Boolean.TRUE.equals(step.get("must_exist"))) {
                        // must_exist is the caller saying a silent no-op would be a mistake, which is the
                        // default position of everything else on this surface.
                        channel.sendResponse(
                            IndexAdminHandler.error(
                                channel,
                                RestStatus.NOT_FOUND,
                                "aliases_not_found_exception",
                                "aliases [" + alias + "] missing on [" + index + "]"
                            )
                        );
                        return;
                    }
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
            if (isSystemIndex(index)) {
                channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.FORBIDDEN,
                        "system_index",
                        "[" + index + "] belongs to a plugin and cannot be aliased"
                    )
                );
                return;
            }
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
        // Core's shape: keyed by index, each carrying the alias. This was {alias, indices[]} -- the last
        // shell-specific shape on the alias surface, kept for the shell's own callers and now retired,
        // since a client library's alias parser is written for this one and nothing else reads the other.
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            for (String index : resolved.alias().indices()) {
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

    /** {@code GET /{index}/_alias}: every alias that names the index, verified against each alias record. */
    private void aliasesOf(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String index) throws IOException {
        final var descriptor = metadata.describe(index);
        if (descriptor.isEmpty()) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index));
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startObject(index);
            builder.startObject("aliases");
            for (String alias : metadata.aliasesOf(descriptor.get())) {
                builder.startObject(alias).endObject();
            }
            builder.endObject();
            builder.endObject();
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
