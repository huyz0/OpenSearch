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
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Applies the alias changes {@link AliasHandler} parsed: the {@code _aliases} actions, and adding, creating and
 * deleting a single alias, each refused when it would touch a system index.
 */
final class AliasUpdates {

    /** The handler's own logger, so moving this code here changes no logger name. */
    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(AliasHandler.class);

    private final AliasHandler handler;

    AliasUpdates(AliasHandler handler) {
        this.handler = handler;
    }

    void dispatch(
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
                        case GET, HEAD -> AliasReads.membership(channel, metadata, aliasName, indices, method == RestRequest.Method.HEAD);
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
                    case GET -> AliasReads.read(channel, metadata, aliasName);
                    case HEAD -> {
                        // An existence check on a name: the same one register read a GET does, and no body.
                        // A data stream is not an alias, so HEAD /_alias/{stream} is a 404 as it is in core.
                        final var record = metadata.resolve(aliasName).alias();
                        final boolean exists = record != null && record.dataStream() == false;
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
    void attach(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String name, List<String> indices, boolean adding)
        throws IOException {
        if (adding) {
            for (String index : indices) {
                if (handler.isSystemIndex(index)) {
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
        final AliasSwap.Refusal failure = AliasSwap.swap(metadata, name, indices, adding);
        if (failure != null) {
            channel.sendResponse(IndexAdminHandler.error(channel, failure.status(), failure.type(), failure.reason()));
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
    void actions(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String body) throws IOException {
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
                final String option = AliasHandler.unsupportedAliasOption(detail);
                if (option != null) {
                    channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_alias_option", option));
                    return;
                }
                if ("remove_index".equals(entry.getKey())) {
                    // Names no alias, so it is answered here rather than by the shape check below.
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_IMPLEMENTED,
                            "unsupported_action",
                            "remove_index is not supported here: it deletes an index inside an alias request, and index "
                                + "deletion is its own compare-and-swap on the descriptor rather than part of the alias "
                                + "swap, so the two could not be one atomic step. DELETE /{index} does the same thing"
                        )
                    );
                    return;
                }
                // Both spellings core takes: one index or alias, or a list. A list used to be unread, so
                // the action resolved to the literal string "null" and failed as a missing index.
                final List<String> names = AliasHandler.namesOf(detail.get("index"), detail.get("indices"));
                final List<String> aliasNames = AliasHandler.namesOf(detail.get("alias"), detail.get("aliases"));
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
                        // Tri-state, as core reads it: unspecified means "404 if the whole request removed
                        // nothing", true means "404 if this step removes nothing", false means silence.
                        step.put(
                            "must_exist",
                            detail.get("must_exist") == null ? null : Boolean.valueOf(String.valueOf(detail.get("must_exist")))
                        );
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
            final var resolved = metadata.resolveForAliasMutation(alias);
            if (resolved.index() != null) {
                channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "name_taken", "[" + alias + "] is an index, not an alias")
                );
                return;
            }
            final AliasRecord before = resolved.alias();
            if (before != null && before.dataStream()) {
                // Core's refusal, in core's words. Rebuilding the record here would have turned the stream
                // into a plain alias: no generation, no timestamp field, no write index, and a 200.
                channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.BAD_REQUEST,
                        "illegal_argument_exception",
                        AliasSwap.dataStreamNotAlias(alias)
                    )
                );
                return;
            }
            final List<String> now = new ArrayList<>(before == null ? List.of() : before.indices());
            // Whether any remove found what it named, for core's rule that a request removing nothing is a 404.
            boolean removedSomething = false;
            boolean removing = false;
            for (java.util.Map<String, Object> step : steps) {
                final String index = String.valueOf(step.get("index"));
                if ("add".equals(step.get("op"))) {
                    if (handler.isSystemIndex(index)) {
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
                    removing = true;
                    if (now.remove(index)) {
                        removedSomething = true;
                    } else if (Boolean.TRUE.equals(step.get("must_exist"))) {
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
                } else if ("remove_index".equals(step.get("op"))) {
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_IMPLEMENTED,
                            "unsupported_action",
                            "remove_index is not supported here: it deletes an index inside an alias request, and index "
                                + "deletion is its own compare-and-swap on the descriptor rather than part of the alias "
                                + "swap, so the two could not be one atomic step. DELETE /{index} does the same thing"
                        )
                    );
                    return;
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
            if (removing && removedSomething == false && now.equals(before == null ? List.of() : before.indices())) {
                // Core's default when must_exist is unspecified and the request removed nothing at all:
                // aliases_not_found_exception, because a 200 would report a removal that did not happen. A
                // step that said must_exist=false asked for silence and gets it -- only the wholly silent
                // request is refused.
                boolean silenced = false;
                for (java.util.Map<String, Object> step : steps) {
                    silenced |= Boolean.FALSE.equals(step.get("must_exist"));
                }
                if (silenced == false) {
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_FOUND,
                            "aliases_not_found_exception",
                            "aliases [" + alias + "] missing"
                        )
                    );
                    return;
                }
            }

            final boolean applied;
            if (before == null) {
                if (now.isEmpty()) {
                    acknowledge(channel);
                    return;
                }
                try {
                    metadata.createAlias(new AliasRecord(alias, now));
                    applied = true;
                } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
                    continue;
                }
            } else if (now.equals(before.indices())) {
                applied = true; // nothing to swap: the record already says this
            } else if (now.isEmpty()) {
                applied = metadata.deleteAlias(before, resolved.generation());
            } else {
                applied = metadata.updateAlias(before, MetadataPlane.withIndices(before, now), resolved.generation()).isPresent();
            }
            if (applied) {
                acknowledge(channel);
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

    void create(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String name, List<String> indices) throws IOException {
        // Every index named has to exist. An alias created over a typo would resolve to nothing and look
        // like an empty index, which is the failure this whole surface is arranged to avoid -- and unlike a
        // search, the mistake here is permanent until somebody notices.
        for (String index : indices) {
            if (handler.isSystemIndex(index)) {
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

    static void acknowledge(org.opensearch.rest.RestChannel channel) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    void delete(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String name) throws IOException {
        final var found = metadata.aliasWithGeneration(name);
        if (found.isEmpty()) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "alias_not_found", "no such alias: " + name));
            return;
        }
        if (found.get().alias().dataStream()) {
            // Deleting the record and leaving the backing indices would strand them under names nothing
            // resolves; the data-stream API deletes both, and is the one that knows to.
            channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    "[" + name + "] is a data stream, not an alias; DELETE /_data_stream/" + name + " removes it and its backing indices"
                )
            );
            return;
        }
        if (metadata.deleteAlias(found.get().alias(), found.get().generation()) == false) {
            channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.CONFLICT,
                    "version_conflict_engine_exception",
                    "[" + name + "] is being changed concurrently; retry"
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
}
