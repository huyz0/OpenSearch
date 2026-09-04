/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.IndexAlreadyExistsException;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Index lifecycle over the metadata plane: {@code PUT}, {@code GET} and {@code DELETE} on {@code /{index}}.
 *
 * <p>Each of these is one object-store operation, not a cluster-state update. Creation is a
 * put-if-absent whose cost does not depend on how many indices already exist — the ceiling
 * {@code plan-area-h-metadata-off-cluster-state.md} measured, where creating the 6,000th index took
 * 98.8 ms because {@code Metadata.Builder.build()} rebuilds name lookups across every index each time.
 *
 * <p>Name uniqueness is arbitrated by the object store rather than by an elected node, so a duplicate
 * create returns 400 without anything having been serialised anywhere.
 */
public final class IndexAdminHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane, which may not exist yet when routes are registered
     */
    public IndexAdminHandler(Supplier<MetadataPlane> plane) {
        this(plane, () -> null);
    }

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the node, whose action gate the plugins' filters live behind
     */
    public IndexAdminHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    /**
     * Runs an administrative operation through the plugins' action filters.
     *
     * <p>Creating and deleting an index are the two operations here that change anything, and a plugin
     * evaluating privileges needs to see them under the names it already knows. A node with no filters
     * pays nothing.
     */
    /**
     * Runs work off the transport thread, or inline when there is no node to borrow a pool from.
     *
     * <p>Everything here touches the object store, and the thread that reads HTTP is not the thread to wait
     * on remote IO from.
     */
    private void dispatch(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        final var serving = node.get();
        if (serving == null) {
            runQuietly(channel, work);
            return;
        }
        serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> runQuietly(channel, work));
    }

    private void runQuietly(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        try {
            work.run();
        } catch (Exception e) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (IOException nested) {
                logger.error("failed to report an index administration failure", nested);
            }
        }
    }

    private <T> T gated(
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws Exception {
        final var serving = node.get();
        return serving == null ? work.get() : serving.actionGate().run(action, request, work);
    }

    @Override
    public String getName() {
        return "serverless_index_admin_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/{index}"),
            new Route(RestRequest.Method.GET, "/{index}"),
            // An existence check, which a client makes before almost anything else. It answered 405 until
            // now, because GET was routed and HEAD was not -- so "does this index exist" was unanswerable
            // on the one path that exists to answer it.
            new Route(RestRequest.Method.HEAD, "/{index}"),
            new Route(RestRequest.Method.DELETE, "/{index}"),
            // Reading back what was configured. Both are projections of the same descriptor GET already
            // reads, so neither costs an object-store request that GET did not already make.
            new Route(RestRequest.Method.GET, "/{index}/_mapping"),
            new Route(RestRequest.Method.GET, "/{index}/_mappings"),
            new Route(RestRequest.Method.GET, "/{index}/_settings"),
            // One setting, or a prefix of them, out of the same descriptor read.
            new Route(RestRequest.Method.GET, "/{index}/_settings/{name}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MetadataPlane metadata = plane.get();
        final String index = request.param("index");
        // Hints, each with one possible answer here: no cluster manager to time out against, one copy of
        // each shard for wait_for_active_shards, no closed or hidden indices for expand_wildcards to
        // reach, no defaults rendered. Consumed so a client library that always sends them is not turned
        // away with "unrecognized parameter". flat_settings is a rendering choice and is honoured below.
        for (String hint : new String[] {
            "local",
            "include_defaults",
            "ignore_unavailable",
            "allow_no_indices",
            "expand_wildcards",
            "master_timeout",
            "cluster_manager_timeout",
            "timeout",
            "wait_for_active_shards",
            "include_type_name" }) {
            request.param(hint);
        }
        final boolean flatSettings = request.paramAsBoolean("flat_settings", false);
        final String settingName = request.param("name");
        if (metadata == null) {
            return channel -> channel.sendResponse(
                error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "this node has no metadata plane configured")
            );
        }

        // A name beginning with an underscore is an API this shell does not implement, not an index.
        //
        // <b>Why a backstop rather than one refusal per endpoint.</b> The GET /{index} route matches any
        // single-segment path, so every top-level API that is not separately registered lands here and is
        // answered "no such index: _stats" -- a confident wrong answer about something that was never an
        // index, which sends a caller looking for a missing index instead of a missing endpoint. M50 fixed
        // exactly this for /_search by routing /_search; that fixes one path and leaves the next one. This
        // catches the whole class, including endpoints nobody has thought of yet.
        if (index != null && index.startsWith("_")) {
            return channel -> channel.sendResponse(
                error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "not_implemented",
                    "'"
                        + index
                        + "' is not an index: names beginning with an underscore are reserved for APIs, and "
                        + "this shell does not implement this one. An index name that is not there answers 404; this "
                        + "is a 501 because there is nothing here to find under any name"
                )
            );
        }

        // Which projection of the descriptor this path asks for. _mapping and _settings are the same read
        // as GET /{index}, rendered narrower, so neither adds an object-store request.
        final View view;
        if (request.path().endsWith("/_settings") || settingName != null) {
            view = View.SETTINGS;
        } else if (request.path().endsWith("/_mapping") || request.path().endsWith("/_mappings")) {
            view = View.MAPPING;
        } else {
            view = View.FULL;
        }

        switch (request.method()) {
            case HEAD: {
                // No body, by definition: the status is the answer.
                return channel -> dispatch(channel, () -> {
                    final Optional<IndexDescriptor> descriptor = gated(
                        org.opensearch.action.admin.indices.get.GetIndexAction.NAME,
                        new org.opensearch.action.admin.indices.get.GetIndexRequest().indices(index),
                        () -> metadata.describe(index)
                    );
                    channel.sendResponse(
                        new BytesRestResponse(descriptor.isPresent() ? RestStatus.OK : RestStatus.NOT_FOUND, "application/json", "")
                    );
                });
            }
            case PUT: {
                final CreateRequest create;
                try {
                    create = CreateRequest.parse(request);
                } catch (RefusedException e) {
                    return channel -> channel.sendResponse(error(channel, e.status, e.type, e.getMessage()));
                }
                final int shards = create.shards;
                final String mapping = create.mapping;
                // Off the transport thread. Creating an index writes to the object store, so this was
                // already blocking the thread that should be reading the next request -- invisibly, because
                // blob IO does not assert about it the way a future does. Running the action filters here
                // made it visible, and the fix is the one every other handler already had.
                // The count the index was actually made with, which a template may have decided. Reporting
                // the request's would tell a caller they got one shard when they got three.
                final java.util.concurrent.atomic.AtomicInteger resolvedShards = new java.util.concurrent.atomic.AtomicInteger(shards);
                return channel -> dispatch(channel, () -> {
                    try {
                        gated(
                            org.opensearch.action.admin.indices.create.CreateIndexAction.NAME,
                            new org.opensearch.action.admin.indices.create.CreateIndexRequest(index),
                            () -> {
                                resolvedShards.set(createIndex(metadata, index, create).numberOfShards());
                                return null;
                            }
                        );
                        try (XContentBuilder builder = channel.newBuilder()) {
                            builder.startObject();
                            builder.field("acknowledged", true);
                            // Real OpenSearch's own second flag, distinguishing "the descriptor exists" from
                            // "every shard is allocated and ready." Nothing here is ever the second without
                            // the first: creation is one put-if-absent, not a routing decision that can lag
                            // it, so the two are always true together.
                            builder.field("shards_acknowledged", true);
                            builder.field("index", index);
                            builder.field("shards", resolvedShards.get());
                            builder.endObject();
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                        }
                    } catch (org.opensearch.serverless.metadata.TemplateResolver.MissingComponentException e) {
                        // Configuration the caller can fix, so 400 rather than 500.
                        channel.sendResponse(error(channel, RestStatus.BAD_REQUEST, "missing_component_template", e.getMessage()));
                    } catch (IndexAlreadyExistsException e) {
                        channel.sendResponse(error(channel, RestStatus.BAD_REQUEST, "index_already_exists", e.getMessage()));
                    } catch (Exception e) {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    }
                });
            }
            case GET: {
                return channel -> dispatch(channel, () -> {
                    final Optional<IndexDescriptor> descriptor = gated(
                        org.opensearch.action.admin.indices.get.GetIndexAction.NAME,
                        new org.opensearch.action.admin.indices.get.GetIndexRequest().indices(index),
                        () -> metadata.describe(index)
                    );
                    if (descriptor.isEmpty()) {
                        // Absent, said plainly. An empty body with 200 would be the confident empty
                        // answer this whole surface exists to avoid.
                        channel.sendResponse(error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index));
                        return;
                    }
                    try (XContentBuilder builder = channel.newBuilder()) {
                        builder.startObject();
                        builder.startObject(index);
                        if (view == View.FULL) {
                            // Core's shape, keyed by alias name. Verified against each alias record, so an
                            // alias that lost its creation race or has since moved on is not listed; the
                            // cost is one register read per alias that names this index, and nothing for
                            // an index nothing names.
                            builder.startObject("aliases");
                            for (String alias : metadata.aliasesOf(descriptor.get())) {
                                builder.startObject(alias).endObject();
                            }
                            builder.endObject();
                        }
                        if (view != View.SETTINGS) {
                            renderMapping(builder, descriptor.get());
                        }
                        if (view != View.MAPPING) {
                            renderSettings(builder, descriptor.get(), settingName);
                        }
                        builder.endObject();
                        builder.endObject();
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    }
                });
            }
            case DELETE: {
                // Inside the consumer, not before it: the delete has to happen after the filters have had
                // their say, and doing it while preparing the request would delete the index and then ask.
                return channel -> dispatch(channel, () -> {
                    final boolean existed;
                    try {
                        existed = gated(
                            org.opensearch.action.admin.indices.delete.DeleteIndexAction.NAME,
                            new org.opensearch.action.admin.indices.delete.DeleteIndexRequest(index),
                            () -> metadata.deleteIndex(index)
                        );
                    } catch (Exception e) {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                        return;
                    }
                    if (existed == false) {
                        channel.sendResponse(error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index));
                        return;
                    }
                    try (XContentBuilder builder = channel.newBuilder()) {
                        builder.startObject();
                        builder.field("acknowledged", true);
                        builder.field("index", index);
                        builder.endObject();
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    }
                });
            }
            default:
                return channel -> channel.sendResponse(
                    error(channel, RestStatus.METHOD_NOT_ALLOWED, "method_not_allowed", request.method() + " is not supported here")
                );
        }
    }

    /** Which part of a descriptor a GET is asking for. */
    private enum View {
        FULL,
        MAPPING,
        SETTINGS
    }

    /**
     * Renders the mapping the way OpenSearch does, as the mapping object itself.
     *
     * <p>This replaced a {@code "has_mapping": true} flag, which told a caller a mapping existed without
     * letting them see it — and with {@code _mapping} and {@code _settings} both unrouted, there was no
     * path by which a client could read back anything it had configured.
     *
     * @param builder the response being built
     * @param descriptor the index
     * @throws IOException if writing fails
     */
    private static void renderMapping(XContentBuilder builder, IndexDescriptor descriptor) throws IOException {
        if (descriptor.mapping() == null) {
            builder.startObject("mappings").endObject();
            return;
        }
        try {
            final Map<String, Object> mapping = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                new org.opensearch.core.common.bytes.BytesArray(descriptor.mapping()),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
            builder.field("mappings", mapping);
        } catch (Exception e) {
            // A mapping that does not parse is stored data this handler did not write, and swallowing it
            // would report an index as having no mapping when it has an unreadable one.
            throw new IOException("the stored mapping for " + descriptor.name() + " could not be read", e);
        }
    }

    /**
     * Renders settings under {@code index}, the way OpenSearch nests them.
     *
     * <p><b>{@code aliases} is absent rather than empty.</b> Resolving an index's aliases needs a reverse
     * lookup this design does not offer — aliases are found by name, and enumerating them is the inventory
     * operation {@code /_serverless/indices} refuses for the same reason. Rendering {@code "aliases": {}}
     * would be a confident empty answer, which is the one thing this surface will not do; a caller that
     * needs aliases asks {@code GET /_alias/{name}}.
     *
     * @param builder the response being built
     * @param descriptor the index
     * @throws IOException if writing fails
     */
    private static void renderSettings(XContentBuilder builder, IndexDescriptor descriptor, String only) throws IOException {
        final java.util.Map<String, String> settings = new java.util.LinkedHashMap<>();
        // Strings, not numbers, because that is what OpenSearch returns: settings are a string map there.
        settings.put("number_of_shards", Integer.toString(descriptor.numberOfShards()));
        // Zero, and true: a shard here has exactly one writer, and its redundancy is the object store
        // rather than a second copy. Reporting it is more honest than omitting it, because a client
        // reading a replica count gets the real one.
        settings.put("number_of_replicas", "0");
        settings.put("uuid", descriptor.uuid());
        settings.put("provided_name", descriptor.name());
        // Recorded at creation since M62; a descriptor from before then has neither, and omitting them is
        // truer than inventing a moment or a version.
        if (descriptor.createdAtMillis() != 0L) {
            settings.put("creation_date", Long.toString(descriptor.createdAtMillis()));
        }
        if (descriptor.createdVersionId() != 0) {
            settings.put("version.created", Integer.toString(descriptor.createdVersionId()));
        }
        for (String key : descriptor.extraSettings().keySet()) {
            final String bare = key.startsWith(IndexMetadata.INDEX_SETTING_PREFIX)
                ? key.substring(IndexMetadata.INDEX_SETTING_PREFIX.length())
                : key;
            settings.put(bare, descriptor.extraSettings().get(key));
        }
        builder.startObject("settings");
        builder.startObject("index");
        for (java.util.Map.Entry<String, String> setting : settings.entrySet()) {
            if (only == null || matchesSettingName(setting.getKey(), only)) {
                if ("version.created".equals(setting.getKey())) {
                    // Nested, as core renders it: settings.index.version.created.
                    builder.startObject("version").field("created", setting.getValue()).endObject();
                } else {
                    builder.field(setting.getKey(), setting.getValue());
                }
            }
        }
        builder.endObject();
        builder.endObject();
    }

    /** {@code GET /{index}/_settings/{name}}: an exact name, with or without the {@code index.} prefix, or a trailing-star prefix. */
    private static boolean matchesSettingName(String bare, String wanted) {
        for (String each : wanted.split(",")) {
            final String name = each.startsWith(IndexMetadata.INDEX_SETTING_PREFIX)
                ? each.substring(IndexMetadata.INDEX_SETTING_PREFIX.length())
                : each;
            if (name.endsWith("*") ? bare.startsWith(name.substring(0, name.length() - 1)) : bare.equals(name)) {
                return true;
            }
        }
        return false;
    }

    /** A refusal raised while parsing, carrying the status and type the caller should see. */
    static final class RefusedException extends Exception {
        final RestStatus status;
        final String type;

        RefusedException(RestStatus status, String type, String reason) {
            super(reason);
            this.status = status;
            this.type = type;
        }
    }

    /**
     * What a create request asked for, after reading it the way an OpenSearch client writes it.
     *
     * <p><b>Why this exists.</b> This handler used to take the shard count from a {@code ?shards=}
     * query parameter and treat the entire request body as the mapping. A client sending OpenSearch's own
     * create-index envelope — {@code {"settings": {...}, "mappings": {...}}} — therefore got an index with
     * one shard rather than the number it asked for, and a mapping that was actually the envelope, and was
     * told {@code "acknowledged": true} and {@code "shards_acknowledged": true} for both. Shard count is
     * fixed at creation, so that was not recoverable without deleting the index and its data.
     *
     * <p>That was the one place on this surface where a caller was misled about their own data rather than
     * refused, which is the failure {@link NotImplementedHandler} exists to avoid and this handler was
     * committing.
     *
     * <p><b>Both shapes are read, and the older one still works.</b> A body carrying {@code settings},
     * {@code mappings} or {@code aliases} at the top level is the classic envelope. Anything else is taken
     * as a bare mapping, which is what this handler has always accepted and what the shell's own callers
     * send. The two cannot be confused: a mapping's top level is {@code properties}, {@code _source},
     * {@code dynamic} and the like.
     */
    /**
     * Creates an index from a request, with what matching templates contribute layered under it.
     *
     * <p>A caller who spelled out a mapping is not overruled by configuration they may not know exists.
     * Shared with rollover and data streams, which create indices the same way from a body of their own.
     *
     * @param metadata the metadata plane
     * @param index the name
     * @param create the request
     * @return the descriptor as created
     * @throws IOException if a register cannot be read or written
     */
    static IndexDescriptor createIndex(MetadataPlane metadata, String index, CreateRequest create) throws IOException {
        final var inherited = org.opensearch.serverless.metadata.TemplateResolver.resolve(
            metadata.indexTemplates().all(),
            metadata.componentTemplates().all(),
            index
        );
        final IndexDescriptor descriptor = new IndexDescriptor(
            index,
            UUID.randomUUID().toString(),
            CreateRequest.shardsWith(create.shards, create.explicitShards, inherited),
            CreateRequest.mappingWith(create.mapping, inherited),
            CreateRequest.settingsWith(create.settings, inherited)
        ).createdAt(metadata.clock().getAsLong(), org.opensearch.Version.CURRENT.id);
        metadata.createIndex(descriptor);
        return descriptor;
    }

    static final class CreateRequest {
        final int shards;
        final String mapping;
        final Settings settings;
        /** Whether the caller named a shard count, as opposed to falling through to the default of one. */
        final boolean explicitShards;

        /**
         * The shard count to create with: the caller's if they gave one, else a template's, else the default.
         *
         * <p>The distinction matters because "one shard" is both a legitimate request and what an absent
         * request looks like. Without tracking which it was, a template's {@code number_of_shards} could
         * never take effect — every create would look like an explicit request for one.
         *
         * @param requested what the request resolved to
         * @param explicit whether the request actually said
         * @param inherited what templates contributed
         * @return the shard count
         */
        static int shardsWith(int requested, boolean explicit, org.opensearch.serverless.metadata.TemplateResolver.Inherited inherited) {
            if (explicit) {
                return requested;
            }
            final Object fromTemplate = inherited.settings().get("number_of_shards");
            if (fromTemplate == null) {
                return requested;
            }
            try {
                return Integer.parseInt(String.valueOf(fromTemplate));
            } catch (NumberFormatException e) {
                return requested;
            }
        }

        /**
         * The mapping to create with: the template's fields, with the request's layered over them.
         *
         * @param requested the request's mapping, or null
         * @param inherited what templates contributed
         * @return the merged mapping source, or null when there is none
         * @throws IOException if either mapping cannot be read
         */
        static String mappingWith(String requested, org.opensearch.serverless.metadata.TemplateResolver.Inherited inherited)
            throws IOException {
            if (inherited.mappings().isEmpty()) {
                return requested;
            }
            final Map<String, Object> merged = new java.util.LinkedHashMap<>(inherited.mappings());
            if (requested != null) {
                final Map<String, Object> own = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                    new org.opensearch.core.common.bytes.BytesArray(requested.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    false,
                    org.opensearch.common.xcontent.XContentType.JSON
                ).v2();
                org.opensearch.serverless.metadata.TemplateResolver.deepMerge(merged, own);
            }
            try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                builder.map(merged);
                return builder.toString();
            }
        }

        /**
         * The settings to create with: the template's, with the request's layered over them.
         *
         * @param requested the request's settings, or null
         * @param inherited what templates contributed
         * @return the merged settings, or null when there are none
         */
        static Settings settingsWith(Settings requested, org.opensearch.serverless.metadata.TemplateResolver.Inherited inherited) {
            if (inherited.settings().isEmpty()) {
                return requested;
            }
            final Settings.Builder merged = Settings.builder();
            for (Map.Entry<String, Object> each : inherited.settings().entrySet()) {
                // Structural settings are the create path's business, not a layered value: shards are
                // resolved separately above and replicas are refused outright.
                if ("number_of_shards".equals(each.getKey()) || "number_of_replicas".equals(each.getKey())) {
                    continue;
                }
                merged.put(IndexMetadata.INDEX_SETTING_PREFIX + each.getKey(), String.valueOf(each.getValue()));
            }
            if (requested != null) {
                merged.put(requested);
            }
            final Settings result = merged.build();
            return result.isEmpty() ? null : result;
        }

        CreateRequest(int shards, String mapping, Settings settings) {
            this(shards, mapping, settings, false);
        }

        private CreateRequest(int shards, String mapping, Settings settings, boolean explicitShards) {
            this.shards = shards;
            this.mapping = mapping;
            this.settings = settings;
            this.explicitShards = explicitShards;
        }

        static CreateRequest parse(RestRequest request) throws RefusedException {
            final int fromQuery = request.paramAsInt("shards", 1);
            if (request.hasContent() == false) {
                return new CreateRequest(fromQuery, null, null, fromQuery != 1);
            }
            final String body = request.content().utf8ToString();
            final Map<String, Object> parsed;
            try {
                parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                    request.content(),
                    false,
                    org.opensearch.common.xcontent.XContentType.JSON
                ).v2();
            } catch (Exception e) {
                throw new RefusedException(RestStatus.BAD_REQUEST, "malformed_body", "the body is not a JSON object");
            }
            return parse(parsed, body, fromQuery);
        }

        /**
         * Reads a create-index body already parsed, for a caller that builds one -- a rollover carrying the
         * body's settings and mappings to the index it creates.
         *
         * @param parsed the body
         * @param body the body's text, for the bare-mapping form
         * @param fromQuery the shard count the query gave, 1 when it gave none
         * @return the request
         * @throws RefusedException if the body asks for something refused
         */
        static CreateRequest parse(Map<String, Object> parsed, String body, int fromQuery) throws RefusedException {
            try {
                if (parsed == null) {
                    throw new IllegalArgumentException("no body");
                }
            } catch (Exception e) {
                throw new RefusedException(RestStatus.BAD_REQUEST, "malformed_body", "could not parse the request body: " + e.getMessage());
            }

            final boolean envelope = parsed.containsKey("settings") || parsed.containsKey("mappings") || parsed.containsKey("aliases");
            if (envelope == false) {
                // The shape this handler has always taken: the body is the mapping.
                return new CreateRequest(fromQuery, body, null, fromQuery != 1);
            }

            if (parsed.containsKey("aliases")) {
                // Refused rather than dropped. An alias silently not created is a search that silently
                // matches nothing later, which is the same class of failure this whole parse exists to end.
                throw new RefusedException(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_setting",
                    "'aliases' in a create-index body is not supported here; create the index, then PUT /_alias/{name}"
                );
            }

            String mapping = null;
            if (parsed.get("mappings") instanceof Map) {
                try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> mappings = (Map<String, Object>) parsed.get("mappings");
                    builder.map(mappings);
                    mapping = builder.toString();
                } catch (IOException e) {
                    throw new RefusedException(RestStatus.BAD_REQUEST, "malformed_body", "could not read 'mappings': " + e.getMessage());
                }
            } else if (parsed.containsKey("mappings")) {
                throw new RefusedException(RestStatus.BAD_REQUEST, "malformed_body", "'mappings' must be an object");
            }

            Settings settings = Settings.EMPTY;
            if (parsed.get("settings") instanceof Map) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> raw = (Map<String, Object>) parsed.get("settings");
                // Prefixing is what turns a client's "number_of_shards" into the "index.number_of_shards"
                // core names it by, and it is core's own normalisation rather than a guess at one.
                settings = Settings.builder().loadFromMap(raw).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
            } else if (parsed.containsKey("settings")) {
                throw new RefusedException(RestStatus.BAD_REQUEST, "malformed_body", "'settings' must be an object");
            }

            final String replicas = settings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
            if (replicas != null && "0".equals(replicas) == false) {
                // Refused, and with the real reason. Accepting it would mean recording a replica count that
                // nothing acts on, and reporting a redundancy this deployment does not provide that way.
                throw new RefusedException(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_setting",
                    "number_of_replicas is not supported: a shard has one writer and its durability comes from "
                        + "the object store, not from replica copies. Readers are added by scaling search nodes, "
                        + "not by setting a replica count."
                );
            }

            final int shards;
            final String requested = settings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS);
            if (requested == null) {
                shards = fromQuery;
            } else {
                try {
                    shards = Integer.parseInt(requested);
                } catch (NumberFormatException e) {
                    throw new RefusedException(RestStatus.BAD_REQUEST, "malformed_body", "number_of_shards must be a number");
                }
            }
            if (shards < 1 || shards > IndexDescriptor.MAX_SHARDS) {
                throw new RefusedException(
                    RestStatus.BAD_REQUEST,
                    "invalid_shard_count",
                    "number_of_shards must be between 1 and " + IndexDescriptor.MAX_SHARDS + ", got " + shards
                );
            }

            // Shard count and replica count are structural and are consumed here; everything else a client
            // set -- refresh_interval, analysis, whatever core understands -- is carried into the
            // descriptor, because the data plane under this shell is core's and does understand them.
            final Settings.Builder rest = Settings.builder().put(settings);
            rest.remove(IndexMetadata.SETTING_NUMBER_OF_SHARDS);
            rest.remove(IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
            final Settings carried = rest.build();
            return new CreateRequest(shards, mapping, carried.isEmpty() ? null : carried, requested != null || fromQuery != 1);
        }
    }

    /**
     * Reads every parameter a request carries, so a refusal made before a handler got to them all renders
     * as itself rather than as BaseRestHandler's "unrecognized parameter" about whichever one it had not
     * reached yet.
     *
     * @param request the request
     */
    /**
     * Runs work under the plugins' action filters, named as core names the action.
     *
     * <p>The same call every handler makes, so the table of what is gated is the table of handlers that
     * call this -- not a list kept elsewhere that drifts. Two-thirds of this surface used to reach the
     * metadata plane directly, and a security plugin's privilege evaluation never ran for any of it.
     *
     * @param <T> what the work returns
     * @param serving the node, or null before one is wired
     * @param action core's action name
     * @param request the request a filter evaluates, carrying the indices it touches
     * @param work what to do if admitted
     * @return what the work returned
     * @throws Exception a filter's refusal, or the work's failure
     */
    /**
     * Words a failed forward for the caller, telling a deadline apart from a refusal.
     *
     * <p>A forward that timed out is not "stale routing, retry": the owner may have applied the write
     * and been slow to answer, and a client that retries an auto-id write on that message writes the
     * document twice. The deadline case says so, so the client can read before it writes again.
     *
     * @param owner the node the request was forwarded to
     * @param e what went wrong
     * @return the message
     */
    public static String forwardFailureMessage(String owner, Exception e) {
        if (org.opensearch.ExceptionsHelper.unwrap(e, org.opensearch.transport.ReceiveTimeoutTransportException.class) != null
            || org.opensearch.ExceptionsHelper.unwrap(
                e,
                org.opensearch.common.util.concurrent.UncategorizedExecutionException.class
            ) != null && e.getMessage() != null && e.getMessage().contains("imeout")) {
            return "the owner "
                + owner
                + " did not answer within the deadline; the operation may or may not have been applied there, so read before retrying a write";
        }
        return "could not forward to " + owner + ", which the shard-head named as owner: " + e.getMessage();
    }

    public static <T> T gate(
        org.opensearch.serverless.shell.ServerlessNode serving,
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws Exception {
        return serving == null ? work.get() : serving.actionGate().run(action, request, work);
    }

    /**
     * Renders an uncaught failure without the node's internals.
     *
     * <p>Core's own renderer, except that an object-store failure carrying a filesystem path is answered
     * as what it is -- an object-store failure naming the blob -- rather than with the absolute path of
     * a file on this node's disk.
     *
     * @param channel the channel to answer on
     * @param e the failure
     * @return the response
     * @throws IOException if rendering fails
     */
    static BytesRestResponse failure(org.opensearch.rest.RestChannel channel, Exception e) throws IOException {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof java.nio.file.FileSystemException fs) {
                final String blob = fs.getFile() == null ? "?" : java.nio.file.Path.of(fs.getFile()).getFileName().toString();
                return error(
                    channel,
                    RestStatus.INTERNAL_SERVER_ERROR,
                    "object_store_failure",
                    cause.getClass().getSimpleName() + " on blob [" + blob + "]"
                );
            }
            if (cause == cause.getCause()) {
                break;
            }
        }
        return new BytesRestResponse(channel, e);
    }

    static void consumeAllParams(RestRequest request) {
        for (String name : java.util.List.copyOf(request.params().keySet())) {
            request.param(name);
        }
    }

    /**
     * Reads {@code refresh} the way core reads it: {@code true}, {@code false}, bare (meaning true) or
     * {@code wait_for}.
     *
     * <p>{@code wait_for} means "answer once this write is visible to search". A write here is made
     * visible by refreshing the shard before answering, which is what {@code true} does -- so the two
     * spellings ask for the same thing at the same cost, and {@code wait_for} is honoured by doing it.
     * This used to be parsed as a boolean, which made a spelling every client library offers a 400.
     *
     * @param request the request
     * @return whether to refresh before answering
     */
    static boolean refresh(RestRequest request) {
        final String value = request.param("refresh");
        if (value == null) {
            return false;
        }
        if (value.isEmpty() || "wait_for".equals(value)) {
            return true;
        }
        return org.opensearch.common.Booleans.parseBoolean(value);
    }

    /**
     * Renders a deliberate refusal in real OpenSearch's own error shape.
     *
     * <p><b>{@code error} is an object here, not a bare string, and that is not cosmetic.</b> Every
     * uncaught exception on this surface already renders correctly — {@code new BytesRestResponse(channel,
     * e)} is core's own {@code OpenSearchException} renderer, and it has always produced
     * {@code {"error": {"root_cause": [...], "type": ..., "reason": ...}, "status": N}}. This helper is
     * what every <em>deliberate</em> refusal on this surface goes through instead — the 501s, the 400s,
     * the conditional-write and wildcard-pattern refusals — and until now it rendered {@code "error"} as a
     * plain string. Any client that parses an error structurally rather than only checking the HTTP status
     * broke on precisely the responses that were trying hardest to explain themselves. This makes the two
     * paths render the same shape, because a caller should not be able to tell "the shell refused this on
     * purpose" from "something threw" by the error envelope's own shape.
     *
     * @param channel the channel to build a response for
     * @param status the HTTP status
     * @param type the error type, e.g. {@code "index_not_found"}
     * @param reason a human-readable explanation
     * @return the rendered response
     * @throws IOException if building the response fails
     */
    static BytesRestResponse error(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason)
        throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startObject("error");
            builder.startArray("root_cause");
            builder.startObject();
            builder.field("type", type);
            builder.field("reason", reason);
            builder.endObject();
            builder.endArray();
            builder.field("type", type);
            builder.field("reason", reason);
            builder.endObject();
            builder.field("status", status.getStatus());
            builder.endObject();
            return new BytesRestResponse(status, builder);
        }
    }
}
