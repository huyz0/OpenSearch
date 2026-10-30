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

    /** Applies the default timestamp mapping after templates and the request have been composed. */
    static IndexAdminHandler.CreateRequest withTimestamp(
        IndexAdminHandler.CreateRequest create,
        String timestampField,
        TemplateResolver.Inherited inherited
    ) throws IOException {
        return withTimestamp(create.withMapping(IndexAdminHandler.CreateRequest.mappingWith(create.mapping, inherited)), timestampField);
    }

    /** A create request with the timestamp field mapped as a date, over whatever else it carried. */
    static IndexAdminHandler.CreateRequest withTimestamp(IndexAdminHandler.CreateRequest create, String timestampField) {
        final Map<String, Object> merged = new LinkedHashMap<>(Map.of("properties", Map.of(timestampField, Map.of("type", "date"))));
        try {
            if (create.mapping != null) {
                TemplateResolver.mergeMappings(
                    merged,
                    org.opensearch.common.xcontent.XContentHelper.convertToMap(
                        new org.opensearch.core.common.bytes.BytesArray(create.mapping),
                        false,
                        org.opensearch.common.xcontent.XContentType.JSON
                    ).v2()
                );
            }
            // Enforce the same document invariant on every write path through core's own mapper.
            // Install this after the supplied mapping so a template cannot turn validation off.
            merged.put(
                org.opensearch.index.mapper.DataStreamFieldMapper.NAME,
                Map.of("enabled", true, "timestamp_field", Map.of("name", timestampField))
            );
            try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                builder.map(merged);
                return create.withMapping(org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString());
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
            switch (method) {
                case PUT -> serving.actionGate()
                    .runAsync(
                        org.opensearch.action.admin.indices.datastream.CreateDataStreamAction.NAME,
                        new org.opensearch.action.admin.indices.datastream.CreateDataStreamAction.Request(name),
                        (admitted, listener) -> onWorker(serving, listener, () -> {
                            final var selected = (org.opensearch.action.admin.indices.datastream.CreateDataStreamAction.Request) admitted;
                            final var invalid = selected.validate();
                            if (invalid != null) {
                                throw invalid;
                            }
                            listener.onResponse(create(serving, metadata, selected.indices()[0]));
                        }),
                        org.opensearch.serverless.shell.ActionGate.identity(
                            org.opensearch.action.support.clustermanager.AcknowledgedResponse.class
                        ),
                        answer(channel)
                    );
                case GET -> serving.actionGate()
                    .runAsync(
                        org.opensearch.action.admin.indices.datastream.GetDataStreamAction.NAME,
                        new org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Request(
                            org.opensearch.core.common.Strings.splitStringByCommaToArray(name)
                        ),
                        (admitted, listener) -> onWorker(serving, listener, () -> {
                            final var selected = (org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Request) admitted;
                            final var invalid = selected.validate();
                            if (invalid != null) {
                                throw invalid;
                            }
                            listener.onResponse(describe(serving, metadata, selected.indices()));
                        }),
                        org.opensearch.serverless.shell.ActionGate.identity(
                            org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Response.class
                        ),
                        answer(channel)
                    );
                case DELETE -> serving.actionGate()
                    .runAsync(
                        org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction.NAME,
                        new org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction.Request(
                            org.opensearch.core.common.Strings.splitStringByCommaToArray(name)
                        ),
                        (admitted, listener) -> onWorker(serving, listener, () -> {
                            final var selected = (org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction.Request) admitted;
                            final var invalid = selected.validate();
                            if (invalid != null) {
                                throw invalid;
                            }
                            if (selected.indices().length != 1
                                || selected.indices()[0] == null
                                || selected.indices()[0].contains(",")
                                || selected.indices()[0].contains("*")
                                || selected.indices()[0].contains("?")) {
                                throw new IndexAdminHandler.RefusedException(
                                    RestStatus.NOT_IMPLEMENTED,
                                    "unsupported_data_stream_delete",
                                    "deleting multiple data streams or patterns is not supported; name one concrete data stream"
                                );
                            }
                            delete(metadata, selected.indices()[0]);
                            // Complete the native operation after settling, so response filters still precede HTTP.
                            IndexAdminHandler.afterDeleteSettles(
                                serving,
                                () -> listener.onResponse(new org.opensearch.action.support.clustermanager.AcknowledgedResponse(true))
                            );
                        }),
                        org.opensearch.serverless.shell.ActionGate.identity(
                            org.opensearch.action.support.clustermanager.AcknowledgedResponse.class
                        ),
                        answer(channel)
                    );
                default -> sendQuietly(channel, RestStatus.METHOD_NOT_ALLOWED, "method_not_allowed", method + " is not supported here");
            }
        });
    }

    private static void onWorker(
        ServerlessNode serving,
        org.opensearch.core.action.ActionListener<?> listener,
        org.opensearch.common.CheckedRunnable<Exception> work
    ) {
        final Runnable run = () -> {
            try {
                work.run();
            } catch (Exception e) {
                listener.onFailure(e);
            }
        };
        if (org.opensearch.transport.Transports.isTransportThread(Thread.currentThread())) {
            serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(run);
        } else {
            run.run();
        }
    }

    private <
        Response extends org.opensearch.core.action.ActionResponse & org.opensearch.core.xcontent.ToXContent>
        org.opensearch.core.action.ActionListener<Response>
        answer(org.opensearch.rest.RestChannel channel) {
        return org.opensearch.core.action.ActionListener.wrap(response -> {
            try (var builder = channel.newBuilder()) {
                response.toXContent(builder, channel.request());
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        }, failure -> {
            try {
                if (failure instanceof IndexAdminHandler.RefusedException refused) {
                    sendQuietly(channel, refused.status, refused.type, refused.getMessage());
                } else if (failure instanceof DescriptorStore.TooManyMatchesException matches) {
                    sendQuietly(channel, RestStatus.BAD_REQUEST, "too_many_indices", matches.getMessage());
                } else {
                    channel.sendResponse(IndexAdminHandler.failure(channel, failure));
                }
            } catch (IOException nested) {
                logger.error("failed to report a data-stream failure", nested);
            }
        });
    }

    private static void validateName(String name) throws IndexAdminHandler.RefusedException {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("a data stream name is required");
        }
        if (name.startsWith("_") || name.startsWith(".")) {
            throw new IndexAdminHandler.RefusedException(
                name.startsWith("_") ? RestStatus.NOT_IMPLEMENTED : RestStatus.BAD_REQUEST,
                name.startsWith("_") ? "not_implemented" : "illegal_argument_exception",
                "'" + name + "' is not a data stream name"
            );
        }
        org.opensearch.cluster.metadata.MetadataCreateIndexService.validateIndexOrAliasName(
            name,
            (invalid, reason) -> new IllegalArgumentException("data_stream [" + invalid + "] " + reason)
        );
        if (name.toLowerCase(Locale.ROOT).equals(name) == false) {
            throw new IllegalArgumentException("data_stream [" + name + "] must be lowercase");
        }
    }

    private org.opensearch.action.support.clustermanager.AcknowledgedResponse create(
        ServerlessNode serving,
        MetadataPlane metadata,
        String name
    ) throws Exception {
        validateName(name);
        final String backing = backingIndexName(name, 1);
        for (String minted : new String[] { name, backing }) {
            if (serving.isSystemIndex(minted)) {
                // A name a plugin declared as its own is reserved on every path that creates a name, not
                // only on PUT /{index}: a stream created under it would resolve the plugin's own writes to a
                // backing index nothing guards.
                throw new IndexAdminHandler.RefusedException(
                    RestStatus.FORBIDDEN,
                    "system_index",
                    "[" + minted + "] belongs to a plugin and cannot be created here"
                );
            }
        }
        if (metadata.resolve(name).absent() == false) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.BAD_REQUEST,
                "resource_already_exists_exception",
                "[" + name + "] already exists"
            );
        }
        final TemplateResolver.Inherited inherited = TemplateResolver.resolveFromStores(
            metadata.indexTemplates(),
            metadata::componentTemplates,
            name
        );
        if (inherited.isDataStream() == false) {
            // Core's own requirement, in core's words.
            throw new IndexAdminHandler.RefusedException(
                RestStatus.BAD_REQUEST,
                "illegal_argument_exception",
                "no matching index template found for data stream [" + name + "]"
            );
        }
        // The hint before the truth, as every alias creation does: the backing index lists the stream
        // before the stream's record exists, and a reader verifies the hint against the record.
        //
        // What the backing index inherits is what the *stream's* name matches -- the template that
        // declared the stream -- handed in rather than resolved against ".ds-events-000001", which no
        // pattern written for "events*" matches. Resolved against its own name, the backing index got
        // one shard and only the timestamp mapped, whatever the template said.
        final org.opensearch.serverless.cluster.IndexDescriptor created;
        try {
            created = IndexAdminHandler.createIndex(
                serving,
                metadata,
                backing,
                withTimestamp(new IndexAdminHandler.CreateRequest(1, null, Settings.EMPTY), inherited.timestampField(), inherited),
                inherited
            );
        } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.BAD_REQUEST,
                "resource_already_exists_exception",
                "index [" + backing + "] already exists"
            );
        } catch (IndexAdminHandler.RefusedException e) {
            throw new IndexAdminHandler.RefusedException(e.status, e.type, e.getMessage());
        }
        try {
            metadata.createAlias(new AliasRecord(name, List.of(backing), true, 1L, inherited.timestampField()));
        } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
            // The stream lost its name; the backing index made for it must not stay, or every later
            // attempt to create the stream would find ".ds-<name>-000001" taken and the name unusable.
            try {
                metadata.deleteIndex(backing, created.uuid());
            } catch (Exception cleanup) {
                logger.warn("could not remove the backing index [" + backing + "] of a data stream that was not created", cleanup);
            }
            throw new IndexAdminHandler.RefusedException(
                RestStatus.BAD_REQUEST,
                "resource_already_exists_exception",
                "[" + name + "] already exists"
            );
        }
        return new org.opensearch.action.support.clustermanager.AcknowledgedResponse(true);
    }

    private org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Response describe(
        ServerlessNode serving,
        MetadataPlane metadata,
        String[] names
    ) throws Exception {
        final String[] selectors = names == null || names.length == 0 ? new String[] { "*" } : names;
        final Map<String, AliasRecord> streams = new LinkedHashMap<>();
        for (String expression : selectors) {
            if (expression == null || expression.isBlank()) {
                throw new IllegalArgumentException("a data stream selector must not be empty");
            }
            for (String token : expression.split(",")) {
                final String selector = token.trim();
                if (selector.isEmpty()) {
                    continue;
                }
                final String wanted = "_all".equals(selector) ? "*" : selector;
                final boolean pattern = IndexPatterns.isPrefixPattern(wanted);
                if (pattern == false) {
                    validateName(wanted);
                }
                for (String each : IndexPatterns.expand(metadata, wanted, serving.patternCap())) {
                    final var resolved = metadata.resolve(each);
                    if (resolved.alias() != null && resolved.alias().dataStream()) {
                        streams.putIfAbsent(each, resolved.alias());
                    } else if (pattern == false) {
                        throw new IndexAdminHandler.RefusedException(
                            RestStatus.NOT_FOUND,
                            "resource_not_found_exception",
                            "data stream [" + each + "] does not exist"
                        );
                    }
                }
            }
        }
        final Map<String, String> templateNames = TemplateResolver.matchingTemplateNames(
            metadata.indexTemplates(),
            streams.keySet().stream().toList()
        );
        final var result = new java.util.ArrayList<
            org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Response.DataStreamInfo>();
        for (AliasRecord stream : streams.values()) {
            final var indices = new java.util.ArrayList<org.opensearch.core.index.Index>();
            var health = org.opensearch.cluster.health.ClusterHealthStatus.GREEN;
            for (String backing : stream.indices()) {
                final var descriptor = metadata.describe(backing);
                if (descriptor.isEmpty()) {
                    health = org.opensearch.cluster.health.ClusterHealthStatus.RED;
                }
                indices.add(new org.opensearch.core.index.Index(backing, descriptor.map(IndexDescriptor::uuid).orElse("_na_")));
            }
            if (indices.isEmpty()) {
                throw new IOException("data stream [" + stream.name() + "] has no backing indices");
            }
            final var nativeStream = new org.opensearch.cluster.metadata.DataStream(
                stream.name(),
                new org.opensearch.cluster.metadata.DataStream.TimestampField(stream.timestampField()),
                indices,
                stream.generation()
            );
            result.add(
                new org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Response.DataStreamInfo(
                    nativeStream,
                    health,
                    templateNames.get(stream.name())
                )
            );
        }
        return new org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Response(result);
    }

    private void delete(MetadataPlane metadata, String name) throws Exception {
        validateName(name);
        final Optional<MetadataPlane.AliasAtGeneration> found = metadata.aliasWithGeneration(name);
        if (found.isEmpty() || found.get().alias().dataStream() == false) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.NOT_FOUND,
                "resource_not_found_exception",
                "data stream [" + name + "] does not exist"
            );
        }
        // The name first, so a write arriving during the deletion has nowhere to land, then the indices --
        // and only the indices the record this deleted actually named. A rollover landing between the read
        // and the delete used to leave a live record over deleted backing indices, with a 200 to the caller.
        final AliasRecord record = found.get().alias();
        final List<org.opensearch.core.index.Index> backings = new java.util.ArrayList<>(record.indices().size());
        for (String backing : record.indices()) {
            metadata.describe(backing)
                .ifPresent(descriptor -> backings.add(new org.opensearch.core.index.Index(backing, descriptor.uuid())));
        }
        if (metadata.deleteAlias(record, found.get().generation()) == false) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.CONFLICT,
                "version_conflict_engine_exception",
                "data stream [" + name + "] was changed concurrently; nothing was deleted, retry"
            );
        }
        for (org.opensearch.core.index.Index backing : backings) {
            // A name can be reused after the stream is unpublished; only its captured incarnation belongs to this delete.
            metadata.deleteIndex(backing.getName(), backing.getUUID());
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
