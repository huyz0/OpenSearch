/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.action.admin.indices.mapping.put.PutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ActionGate;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code PUT|POST /{index}/_mapping} — adding fields to an index that already exists.
 *
 * <p><b>Why this was the gap worth closing first.</b> Comparing this shell against AWS OpenSearch Serverless
 * and Elastic Cloud Serverless turned up one capability both of them ship and this did not: changing an index
 * after creating it. An index whose mapping is fixed at creation cannot have a field added to it, and with no
 * {@code _reindex} to escape through, the only remedy was to delete the index and its data. That is not an
 * endpoint that was missed — it is the thing that separates a shell from a product.
 *
 * <p><b>Nothing here decides what a legal mapping change is.</b> The merge is core's own
 * {@link MapperService#merge}, under {@code MergeReason.MAPPING_UPDATE}, which is the same call classic
 * OpenSearch makes for the same request. Adding a field succeeds; changing an existing field's type throws,
 * and the exception carries core's own explanation of which field and why. Re-implementing that reasoning
 * here would be re-implementing the type system.
 *
 * <p><b>Validating without a shard.</b> {@code IndicesService#createIndexMapperService} builds a mapper
 * service from index metadata alone, which is how core validates a mapping before any shard exists. So this
 * works on whichever node the request lands on, whether or not that node holds a shard of the index — which
 * matters, because a mapping is a property of the index and there is no reason a client should have to reach
 * a particular node to change one.
 *
 * <p><b>The write is a compare-and-swap.</b> Two clients adding different fields at the same time is exactly
 * the race this design keeps an object store to arbitrate. The loser is told to retry rather than having its
 * change silently dropped or the winner's silently overwritten; retrying re-reads and re-merges, so both
 * fields end up present.
 */
public final class MappingUpdateHandler extends BaseRestHandler {

    /** How many times a lost compare-and-swap is retried before the caller is asked to. */
    private static final int ATTEMPTS = 4;

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public MappingUpdateHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_mapping_update_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/{index}/_mapping"),
            new Route(RestRequest.Method.POST, "/{index}/_mapping"),
            new Route(RestRequest.Method.PUT, "/{index}/_mappings"),
            new Route(RestRequest.Method.POST, "/{index}/_mappings")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");
        final var content = request.hasContent() ? request.content() : null;
        final var mediaType = request.getMediaType();
        for (String hint : new String[] {
            "write_index_only",
            "timeout",
            "master_timeout",
            "cluster_manager_timeout",
            "ignore_unavailable",
            "allow_no_indices",
            "expand_wildcards",
            "include_type_name" }) {
            request.param(hint);
        }
        if (content == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a mapping body is required")
            );
        }
        final MetadataPlane metadata = plane.get();
        final var serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final var nativeRequest = new PutMappingRequest(index).source(content, mediaType);
                final var supportedOptions = nativeRequest.indicesOptions();
                final var result = serving.actionGate().run(PutMappingAction.NAME, nativeRequest, admitted -> {
                    final var selected = (PutMappingRequest) admitted;
                    final var invalid = selected.validate();
                    if (invalid != null) {
                        throw invalid;
                    }
                    if (selected.indicesOptions().equals(supportedOptions) == false) {
                        throw new org.opensearch.OpenSearchStatusException(
                            "mapping index options are not supported",
                            RestStatus.NOT_IMPLEMENTED
                        );
                    }
                    final Index concrete = selected.getConcreteIndex();
                    final String selectedIndex;
                    if (concrete != null) {
                        selectedIndex = concrete.getName();
                    } else if (selected.indices() != null && selected.indices().length == 1) {
                        selectedIndex = selected.indices()[0];
                    } else {
                        throw new org.opensearch.OpenSearchStatusException(
                            "a mapping update requires one concrete index",
                            RestStatus.NOT_IMPLEMENTED
                        );
                    }
                    org.opensearch.serverless.metadata.Names.validateIndexOrAlias(selectedIndex);
                    return apply(metadata, serving, selectedIndex, concrete, selected.source());
                }, responseView());
                acknowledge(channel, result);
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a mapping update failure", nested);
                }
            }
        });
    }

    private record MappingResult(AcknowledgedResponse response, String index, Boolean changed) {
    }

    private static ActionGate.ResponseView<MappingResult> responseView() {
        return new ActionGate.ResponseView<>() {
            @Override
            public org.opensearch.core.action.ActionResponse show(MappingResult result) {
                return result.response();
            }

            @Override
            public MappingResult read(org.opensearch.core.action.ActionResponse response, MappingResult original) {
                if (original != null && response == original.response()) {
                    return original;
                }
                // A native cached or replaced acknowledgement has no index-specific extension fields.
                return new MappingResult((AcknowledgedResponse) response, null, null);
            }
        };
    }

    private MappingResult apply(
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        String index,
        Index concrete,
        String body
    ) throws Exception {
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            final long generation = metadata.descriptorGeneration(index);
            final Optional<IndexDescriptor> current = metadata.describe(index);
            if (current.isEmpty() || (concrete != null && concrete.getUUID().equals(current.get().uuid()) == false)) {
                throw new org.opensearch.index.IndexNotFoundException(index);
            }
            final String merged;
            try {
                merged = mergedMapping(serving, current.get(), body);
            } catch (IllegalArgumentException | org.opensearch.index.mapper.MapperParsingException e) {
                throw new org.opensearch.OpenSearchStatusException(
                    e.getMessage() == null ? e.toString() : e.getMessage(),
                    RestStatus.BAD_REQUEST,
                    e
                );
            }
            if (merged.equals(current.get().mapping())) {
                return new MappingResult(new AcknowledgedResponse(true), index, false);
            }
            final IndexDescriptor updated = current.get().withMapping(merged);
            if (metadata.updateDescriptor(updated, generation).isPresent()) {
                serving.reconciler().refreshMapping(index, updated);
                return new MappingResult(new AcknowledgedResponse(true), index, true);
            }
        }
        throw new org.opensearch.OpenSearchStatusException(
            "the mapping for " + index + " is being changed concurrently; retry",
            RestStatus.CONFLICT
        );
    }

    /**
     * Merges the requested mapping into the existing one, using core's own merge.
     *
     * @param serving the node, for its indices service
     * @param descriptor the index as it stands
     * @param body the requested mapping
     * @return the merged mapping source
     * @throws IOException if the mapper service cannot be built
     */
    private static String mergedMapping(org.opensearch.serverless.shell.ServerlessNode serving, IndexDescriptor descriptor, String body)
        throws IOException {
        final IndexMetadata metadata = descriptor.toIndexMetadata(Map.of());
        final MapperService mapperService = serving.indicesService().createIndexMapperService(metadata);
        try {
            // The existing mapping first, then the change on top of it. MAPPING_UPDATE is the reason core
            // attaches to a client-initiated change, which is what this is; the refusal of a conflicting
            // field type does not come from the reason but from the field mapper's own merge, which rejects
            // it under either reason. Tested: swapping the reason does not make the conflict legal.
            if (descriptor.mapping() != null) {
                mapperService.merge(
                    MapperService.SINGLE_MAPPING_NAME,
                    new CompressedXContent(descriptor.mapping()),
                    MapperService.MergeReason.MAPPING_RECOVERY
                );
            }
            final var mapper = mapperService.merge(
                MapperService.SINGLE_MAPPING_NAME,
                new CompressedXContent(body),
                MapperService.MergeReason.MAPPING_UPDATE
            );
            // Core's merged source nests everything under the mapping type ("_doc"). OpenSearch's own
            // GET /{index}/_mapping does not show that level, and this shell's stored mappings never had
            // it -- so storing core's form verbatim would silently change the shape of every mapping a
            // client reads back, the first time it updated one. Unwrapped here, once, rather than special-
            // cased in every reader.
            return unwrapType(mapper.mappingSource().string());
        } finally {
            mapperService.close();
        }
    }

    /**
     * Strips the single mapping type core wraps a merged mapping in.
     *
     * @param source core's merged mapping source
     * @return the mapping as a client writes and reads it
     * @throws IOException if the source does not parse
     */
    private static String unwrapType(String source) throws IOException {
        final Map<String, Object> parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
            new org.opensearch.core.common.bytes.BytesArray(source),
            false,
            org.opensearch.common.xcontent.XContentType.JSON
        ).v2();
        final Object inner = parsed.get(MapperService.SINGLE_MAPPING_NAME);
        if (parsed.size() != 1 || inner instanceof Map == false) {
            return source;
        }
        try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> body = (Map<String, Object>) inner;
            builder.map(body);
            return builder.toString();
        }
    }

    private static void acknowledge(org.opensearch.rest.RestChannel channel, MappingResult result) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", result.response().isAcknowledged());
            if (result.index() != null) {
                builder.field("index", result.index());
                // Preserve the existing additive no-op report when the native response is unchanged.
                builder.field("changed", result.changed());
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
