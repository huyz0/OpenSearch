/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.TemplateResolver;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code /_data_stream/{name}}: data streams, as aliases with a generation.
 *
 * <p>A data stream here is exactly what core's is at the surface -- a name that is written through to its
 * newest backing index, searched across all of them, and rolled over to a new one -- built from what this
 * design already has: an alias record carrying a generation, backing indices named {@code .ds-<name>-<n>},
 * and the same template resolution an index creation uses. The template that matches the name must
 * declare {@code data_stream}, as core requires, and the timestamp field it names is mapped as a date on
 * every backing index. Writes through the name must be creates, as core requires.
 */
public final class DataStreamHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node
     * @param plane supplies the metadata plane
     */
    public DataStreamHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_data_stream/{name}"),
            new Route(RestRequest.Method.GET, "/_data_stream/{name}"),
            new Route(RestRequest.Method.DELETE, "/_data_stream/{name}")
        );
    }

    /** The prefix every backing index name carries, which {@code PUT /{index}} refuses so a rollover never finds its name taken. */
    static final String BACKING_INDEX_PREFIX = ".ds-";

    /** Core's backing-index naming: {@code .ds-<stream>-<generation, six digits>}. */
    static String backingIndexName(String stream, long generation) {
        return String.format(Locale.ROOT, BACKING_INDEX_PREFIX + "%s-%06d", stream, generation);
    }

    /** A create request with the timestamp field mapped as a date, over whatever else it carried. */
    static IndexAdminHandler.CreateRequest withTimestamp(IndexAdminHandler.CreateRequest create, String timestampField) {
        final String stamp = "{\"properties\":{\"" + timestampField + "\":{\"type\":\"date\"}}}";
        if (create.mapping == null) {
            return new IndexAdminHandler.CreateRequest(create.shards, stamp, create.settings == null ? Settings.EMPTY : create.settings);
        }
        try {
            final Map<String, Object> merged = new LinkedHashMap<>(
                org.opensearch.common.xcontent.XContentHelper.convertToMap(
                    new org.opensearch.core.common.bytes.BytesArray(stamp.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    false,
                    org.opensearch.common.xcontent.XContentType.JSON
                ).v2()
            );
            TemplateResolver.mergeMappings(
                merged,
                org.opensearch.common.xcontent.XContentHelper.convertToMap(
                    new org.opensearch.core.common.bytes.BytesArray(create.mapping.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    false,
                    org.opensearch.common.xcontent.XContentType.JSON
                ).v2()
            );
            try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                builder.map(merged);
                return new IndexAdminHandler.CreateRequest(
                    create.shards,
                    org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString(),
                    create.settings == null ? Settings.EMPTY : create.settings
                );
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("could not merge the timestamp mapping: " + e.getMessage(), e);
        }
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        for (String hint : new String[] { "timeout", "master_timeout", "cluster_manager_timeout", "expand_wildcards" }) {
            request.param(hint);
        }
        final MetadataPlane metadata = plane.get();
        final ServerlessNode serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if (name.startsWith("_") || name.startsWith(".")) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    name.startsWith("_") ? RestStatus.NOT_IMPLEMENTED : RestStatus.BAD_REQUEST,
                    name.startsWith("_") ? "not_implemented" : "illegal_argument_exception",
                    "'" + name + "' is not a data stream name"
                )
            );
        }
        final RestRequest.Method method = request.method();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                switch (method) {
                    case PUT -> IndexAdminHandler.gate(
                        serving,
                        org.opensearch.action.admin.indices.datastream.CreateDataStreamAction.NAME,
                        new org.opensearch.action.admin.indices.datastream.CreateDataStreamAction.Request(name),
                        () -> {
                            create(channel, serving, metadata, name);
                            return null;
                        }
                    );
                    case GET -> IndexAdminHandler.gate(
                        serving,
                        org.opensearch.action.admin.indices.datastream.GetDataStreamAction.NAME,
                        new org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Request(new String[] { name }),
                        () -> {
                            describe(channel, serving, metadata, name);
                            return null;
                        }
                    );
                    case DELETE -> IndexAdminHandler.gate(
                        serving,
                        org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction.NAME,
                        new org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction.Request(new String[] { name }),
                        () -> {
                            delete(channel, metadata, name);
                            return null;
                        }
                    );
                    default -> sendQuietly(channel, RestStatus.METHOD_NOT_ALLOWED, "method_not_allowed", method + " is not supported here");
                }
            } catch (org.opensearch.serverless.metadata.DescriptorStore.TooManyMatchesException e) {
                sendQuietly(channel, RestStatus.BAD_REQUEST, "too_many_indices", e.getMessage());
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a data-stream failure", nested);
                }
            }
        });
    }

    private void create(org.opensearch.rest.RestChannel channel, ServerlessNode serving, MetadataPlane metadata, String name)
        throws IOException {
        final String backing = backingIndexName(name, 1);
        for (String minted : new String[] { name, backing }) {
            if (serving.isSystemIndex(minted)) {
                // A name a plugin declared as its own is reserved on every path that creates a name, not
                // only on PUT /{index}: a stream created under it would resolve the plugin's own writes to a
                // backing index nothing guards.
                sendQuietly(
                    channel,
                    RestStatus.FORBIDDEN,
                    "system_index",
                    "[" + minted + "] belongs to a plugin and cannot be created here"
                );
                return;
            }
        }
        if (metadata.resolve(name).absent() == false) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "resource_already_exists_exception", "[" + name + "] already exists");
            return;
        }
        final TemplateResolver.Inherited inherited = TemplateResolver.resolve(
            metadata.indexTemplates().all(),
            metadata.componentTemplates().all(),
            name
        );
        if (inherited.isDataStream() == false) {
            // Core's own requirement, in core's words.
            sendQuietly(
                channel,
                RestStatus.BAD_REQUEST,
                "illegal_argument_exception",
                "no matching index template found for data stream [" + name + "]"
            );
            return;
        }
        // The hint before the truth, as every alias creation does: the backing index lists the stream
        // before the stream's record exists, and a reader verifies the hint against the record.
        //
        // What the backing index inherits is what the *stream's* name matches -- the template that
        // declared the stream -- handed in rather than resolved against ".ds-events-000001", which no
        // pattern written for "events*" matches. Resolved against its own name, the backing index got
        // one shard and only the timestamp mapped, whatever the template said.
        try {
            IndexAdminHandler.createIndex(
                metadata,
                backing,
                withTimestamp(new IndexAdminHandler.CreateRequest(1, null, Settings.EMPTY), inherited.timestampField()),
                inherited
            );
        } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "resource_already_exists_exception", "index [" + backing + "] already exists");
            return;
        } catch (IndexAdminHandler.RefusedException e) {
            sendQuietly(channel, e.status, e.type, e.getMessage());
            return;
        }
        try {
            metadata.createAlias(new AliasRecord(name, List.of(backing), true, 1L, inherited.timestampField()));
        } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
            // The stream lost its name; the backing index made for it must not stay, or every later
            // attempt to create the stream would find ".ds-<name>-000001" taken and the name unusable.
            try {
                metadata.deleteIndex(backing);
            } catch (Exception cleanup) {
                logger.warn("could not remove the backing index [" + backing + "] of a data stream that was not created", cleanup);
            }
            sendQuietly(channel, RestStatus.BAD_REQUEST, "resource_already_exists_exception", "[" + name + "] already exists");
            return;
        }
        acknowledge(channel);
    }

    private void describe(org.opensearch.rest.RestChannel channel, ServerlessNode serving, MetadataPlane metadata, String names)
        throws IOException {
        final List<String> wanted = IndexPatterns.expand(metadata, names, serving.patternCap());
        final List<AliasRecord> streams = new java.util.ArrayList<>();
        for (String each : wanted) {
            final DescriptorStore.Resolution resolved = metadata.resolve(each);
            if (resolved.alias() != null && resolved.alias().dataStream()) {
                streams.add(resolved.alias());
            } else if (IndexPatterns.isPrefixPattern(names) == false) {
                sendQuietly(channel, RestStatus.NOT_FOUND, "resource_not_found_exception", "data stream [" + each + "] does not exist");
                return;
            }
        }
        // Core's GetDataStreamAction shape.
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("data_streams");
            for (AliasRecord stream : streams) {
                builder.startObject();
                builder.field("name", stream.name());
                builder.startObject("timestamp_field").field("name", stream.timestampField()).endObject();
                builder.startArray("indices");
                for (String backing : stream.indices()) {
                    final Optional<IndexDescriptor> descriptor = metadata.describe(backing);
                    builder.startObject();
                    builder.field("index_name", backing);
                    builder.field("index_uuid", descriptor.map(IndexDescriptor::uuid).orElse("_na_"));
                    builder.endObject();
                }
                builder.endArray();
                builder.field("generation", stream.generation());
                builder.field("status", "GREEN");
                final String template = TemplateResolver.resolve(
                    metadata.indexTemplates().all(),
                    metadata.componentTemplates().all(),
                    stream.name()
                ).from();
                if (template != null) {
                    builder.field("template", template);
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void delete(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String name) throws IOException {
        final Optional<MetadataPlane.AliasAtGeneration> found = metadata.aliasWithGeneration(name);
        if (found.isEmpty() || found.get().alias().dataStream() == false) {
            sendQuietly(channel, RestStatus.NOT_FOUND, "resource_not_found_exception", "data stream [" + name + "] does not exist");
            return;
        }
        // The name first, so a write arriving during the deletion has nowhere to land, then the indices --
        // and only the indices the record this deleted actually named. A rollover landing between the read
        // and the delete used to leave a live record over deleted backing indices, with a 200 to the caller.
        final AliasRecord record = found.get().alias();
        if (metadata.deleteAlias(record, found.get().generation()) == false) {
            sendQuietly(
                channel,
                RestStatus.CONFLICT,
                "version_conflict_engine_exception",
                "data stream [" + name + "] was changed concurrently; nothing was deleted, retry"
            );
            return;
        }
        for (String backing : record.indices()) {
            metadata.deleteIndex(backing);
        }
        acknowledge(channel);
    }

    private static void acknowledge(org.opensearch.rest.RestChannel channel) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void sendQuietly(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report a data-stream refusal", e);
        }
    }
}
