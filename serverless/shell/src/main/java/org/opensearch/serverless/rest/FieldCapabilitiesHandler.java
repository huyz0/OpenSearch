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
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * {@code GET|POST /{index}/_field_caps} — what a client can query, before it queries it.
 *
 * <p><b>Why this was the largest thing missing.</b> Both AWS OpenSearch Serverless and Elastic Cloud
 * Serverless ship it, and clients reach for it first: it is how a UI learns which fields exist, which of them
 * can be searched, and which can be aggregated. Until now it was not merely unimplemented — it fell through to
 * core's default handler and answered {@code 400 no handler found for uri}, which reads as a typo rather than
 * as an absence.
 *
 * <p><b>It needs no shard.</b> Field capabilities are a property of the mapping, and the mapping is in the
 * descriptor. {@code IndicesService#createIndexMapperService} builds a mapper service from index metadata
 * alone, and {@code MapperService#fieldTypes} then reports each field's type and whether it is searchable and
 * aggregatable — answers core computes from the field's own mapper rather than from anything on disk. So this
 * works against an index whose shards are all dormant, which is the normal resting state here and would
 * otherwise have made the endpoint useless exactly when a client needs it most.
 *
 * <p><b>Conflicts are reported, not resolved.</b> Two indices can map the same field to different types.
 * OpenSearch's answer lists each type separately with the indices that use it, so a caller can see the
 * disagreement; collapsing it to one answer would be choosing for them. When every index agrees, the
 * {@code indices} key is omitted, which is also what OpenSearch does and is how a client tells the two cases
 * apart.
 *
 * <p><b>Scope is bounded the same way search is.</b> A named index or a prefix pattern; more matches than the
 * cap is a refusal rather than a truncation. There is no unscoped form: asking every index in the deployment
 * what fields it has is the inventory operation this design refuses, and it would be answered by reading every
 * descriptor there is.
 */
public final class FieldCapabilitiesHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public FieldCapabilitiesHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_field_caps_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/{index}/_field_caps"),
            new Route(RestRequest.Method.POST, "/{index}/_field_caps")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");
        String fields = request.param("fields", "*");
        // Hints; indices here resolve by the same rule everywhere and the flags cannot change it.
        request.param("ignore_unavailable");
        request.param("allow_no_indices");
        request.param("expand_wildcards");
        // The body, which core accepts as the other place to name fields, and which this used to ignore
        // entirely: a POST naming three fields in its body was answered with every field.
        if (request.hasContent()) {
            try (var parser = request.contentParser()) {
                final Map<String, Object> body = parser.map();
                if (body.get("index_filter") != null) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_IMPLEMENTED,
                            "unsupported_parameter",
                            "index_filter is not supported: capabilities here come from the mapping, and a filter over "
                                + "the data would need every shard opened to evaluate it"
                        )
                    );
                }
                if (body.get("runtime_mappings") != null) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_IMPLEMENTED,
                            "unsupported_parameter",
                            "runtime_mappings are not supported"
                        )
                    );
                }
                if (body.get("fields") instanceof List<?> named) {
                    if (named.isEmpty() || named.stream().anyMatch(field -> field instanceof String == false)) {
                        throw new IllegalArgumentException("fields must be a nonempty array of strings");
                    }
                    fields = String.join(",", named.stream().map(String::valueOf).toList());
                } else if (body.get("fields") instanceof String one) {
                    fields = one;
                }
            } catch (Exception e) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.BAD_REQUEST,
                        "malformed_body",
                        "could not parse the body: " + e.getMessage()
                    )
                );
            }
        }
        final String wantedFields = fields;
        // Read so the request is fully consumed, and refused rather than ignored: an unmapped field is a
        // property of the data this endpoint does not read, so answering as though the parameter had been
        // honoured would be inventing an answer.
        final String includeUnmapped = request.param("include_unmapped");

        if (includeUnmapped != null && Boolean.parseBoolean(includeUnmapped)) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_parameter",
                    "include_unmapped is not supported: it reports fields an index does not map, which this "
                        + "implementation cannot distinguish from fields that do not exist anywhere"
                )
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
                final var ask = new org.opensearch.action.fieldcaps.FieldCapabilitiesRequest().indices(
                    org.opensearch.core.common.Strings.splitStringByCommaToArray(index)
                ).fields(org.opensearch.core.common.Strings.splitStringByCommaToArray(wantedFields));
                final var result = serving.actionGate().run(org.opensearch.action.fieldcaps.FieldCapabilitiesAction.NAME, ask, admitted -> {
                    if (admitted instanceof org.opensearch.action.fieldcaps.FieldCapabilitiesRequest == false) {
                        throw unsupported("field capabilities require a native field capabilities request");
                    }
                    final var selected = (org.opensearch.action.fieldcaps.FieldCapabilitiesRequest) admitted;
                    final var invalid = selected.validate();
                    if (invalid != null) {
                        throw new IllegalArgumentException(invalid.getMessage(), invalid);
                    }
                    if (selected.includeUnmapped() || selected.indexFilter() != null) {
                        throw unsupported("unmapped capabilities and index filters are not supported");
                    }
                    if (selected.indicesOptions().equals(org.opensearch.action.support.IndicesOptions.strictExpandOpen()) == false) {
                        throw unsupported("rewritten index expansion options are not supported for field capabilities");
                    }
                    if (selected.indices().length == 0) {
                        throw unsupported("field capabilities require index names or a bounded prefix pattern");
                    }
                    return answer(metadata, serving, String.join(",", selected.indices()), selected.fields());
                }, new org.opensearch.serverless.shell.ActionGate.ResponseView<CapabilitiesResult>() {
                    @Override
                    public org.opensearch.core.action.ActionResponse show(CapabilitiesResult value) {
                        return value.response();
                    }

                    @Override
                    public CapabilitiesResult read(org.opensearch.core.action.ActionResponse response, CapabilitiesResult original) {
                        if (response instanceof org.opensearch.action.fieldcaps.FieldCapabilitiesResponse == false) {
                            throw unsupported("field capabilities require a native field capabilities response");
                        }
                        return new CapabilitiesResult(
                            (org.opensearch.action.fieldcaps.FieldCapabilitiesResponse) response,
                            original == null ? java.util.Set.of() : original.metadataFields()
                        );
                    }
                });
                render(channel, result);
            } catch (IndexAdminHandler.RefusedException e) {
                sendQuietly(channel, e.status, e.type, e.getMessage());
            } catch (org.opensearch.serverless.metadata.DescriptorStore.TooManyMatchesException e) {
                sendQuietly(channel, RestStatus.BAD_REQUEST, "too_many_indices", e.getMessage());
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a field capabilities failure", nested);
                }
            }
        });
    }

    private CapabilitiesResult answer(
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        String index,
        String[] wanted
    ) throws Exception {
        final List<String> names = new java.util.ArrayList<>(IndexPatterns.expand(metadata, index, serving.patternCap()));
        // A plugin's index is not described through the request path, whatever spelling reached it.
        names.removeIf(serving::isSystemIndex);
        if (names.isEmpty()) {
            throw new IndexAdminHandler.RefusedException(RestStatus.BAD_REQUEST, "no_index", "name an index or a prefix pattern");
        }

        // field -> type -> the indices mapping it that way. Sorted so the answer is stable across calls,
        // which matters for a response a client may diff.
        final Map<String, Map<String, TreeSet<String>>> capabilities = new TreeMap<>();
        final Map<String, Boolean> searchable = new LinkedHashMap<>();
        final Map<String, Boolean> aggregatable = new LinkedHashMap<>();
        final Map<String, TreeSet<String>> nonSearchable = new TreeMap<>();
        final Map<String, TreeSet<String>> nonAggregatable = new TreeMap<>();
        final Map<String, Map<String, TreeSet<String>>> fieldMetadata = new TreeMap<>();
        final List<String> answered = new ArrayList<>();
        final java.util.Set<String> metadataFields = new java.util.HashSet<>();

        for (String name : names) {
            final Optional<IndexDescriptor> descriptor = metadata.describe(name);
            if (descriptor.isEmpty()) {
                if (IndexPatterns.isPrefixPattern(index)) {
                    // A pattern is a filter over what exists, not an assertion that anything does.
                    continue;
                }
                throw new IndexAdminHandler.RefusedException(RestStatus.NOT_FOUND, "index_not_found", "no such index: " + name);
            }
            answered.add(name);
            final MapperService mapperService = serving.indicesService()
                .createIndexMapperService(descriptor.get().toIndexMetadata(Map.of()));
            try {
                // createIndexMapperService builds the service; it does not load the mapping into it. Without
                // this the field list is empty and the endpoint answers "this index has no fields" for every
                // index -- a confident wrong answer rather than a failure, which is why it is worth saying.
                if (descriptor.get().mapping() != null) {
                    mapperService.merge(
                        MapperService.SINGLE_MAPPING_NAME,
                        new org.opensearch.common.compress.CompressedXContent(descriptor.get().mapping()),
                        MapperService.MergeReason.MAPPING_RECOVERY
                    );
                }
                final TreeSet<String> fieldNames = new TreeSet<>();
                for (String pattern : wanted) {
                    // Core's matcher includes field aliases and avoids regex backtracking.
                    fieldNames.addAll(mapperService.simpleMatchToFullName(pattern));
                }
                final java.util.function.Predicate<String> visible = serving.indicesService().getFieldFilter().apply(name);
                final java.util.Set<String> objectPaths = new java.util.HashSet<>();
                for (String fieldName : fieldNames) {
                    final MappedFieldType type = mapperService.fieldType(fieldName);
                    if (type == null || (mapperService.isMetadataField(fieldName) == false && visible.test(type.name()) == false)) {
                        continue;
                    }
                    // familyTypeName, not typeName: OpenSearch reports the family a field belongs to, so a
                    // specialised keyword answers as the thing a client can actually act on.
                    capabilities.computeIfAbsent(fieldName, key -> new TreeMap<>())
                        .computeIfAbsent(type.familyTypeName(), key -> new TreeSet<>())
                        .add(name);
                    final String key = fieldName + " " + type.familyTypeName();
                    searchable.merge(key, type.isSearchable(), Boolean::logicalAnd);
                    aggregatable.merge(key, type.isAggregatable(), Boolean::logicalAnd);
                    if (type.isSearchable() == false) {
                        nonSearchable.computeIfAbsent(key, ignored -> new TreeSet<>()).add(name);
                    }
                    if (type.isAggregatable() == false) {
                        nonAggregatable.computeIfAbsent(key, ignored -> new TreeSet<>()).add(name);
                    }
                    for (Map.Entry<String, String> entry : type.meta().entrySet()) {
                        fieldMetadata.computeIfAbsent(key, ignored -> new TreeMap<>())
                            .computeIfAbsent(entry.getKey(), ignored -> new TreeSet<>())
                            .add(entry.getValue());
                    }
                    // Flagged, as OpenSearch flags them. A caller asking fields=* gets _id, _index, _seq_no
                    // and the rest, and needs to be able to tell them from the fields it put there itself.
                    if (mapperService.isMetadataField(fieldName)) {
                        metadataFields.add(fieldName);
                    }
                    // Core also reports the object paths of each visible selected field, including
                    // an alias's underlying path. Multi-field parents remain ordinary mapped fields.
                    int dot = type.name().lastIndexOf('.');
                    while (dot >= 0) {
                        final String parent = type.name().substring(0, dot);
                        if (objectPaths.add(parent) == false) {
                            break;
                        }
                        if (mapperService.fieldType(parent) == null) {
                            final var object = mapperService.getObjectMapper(parent);
                            // Literal dotted names under disable_objects need not have object mappers.
                            if (object != null) {
                                final String parentType = object.nested().isNested() ? "nested" : "object";
                                capabilities.computeIfAbsent(parent, ignored -> new TreeMap<>())
                                    .computeIfAbsent(parentType, ignored -> new TreeSet<>())
                                    .add(name);
                                final String parentKey = parent + " " + parentType;
                                searchable.put(parentKey, false);
                                aggregatable.put(parentKey, false);
                            }
                        }
                        dot = parent.lastIndexOf('.');
                    }

                }
            } finally {
                mapperService.close();
            }
        }

        final Map<String, Map<String, org.opensearch.action.fieldcaps.FieldCapabilities>> fields = new TreeMap<>();
        for (var field : capabilities.entrySet()) {
            final Map<String, org.opensearch.action.fieldcaps.FieldCapabilities> types = new TreeMap<>();
            for (var type : field.getValue().entrySet()) {
                final String key = field.getKey() + " " + type.getKey();
                final var cannotSearch = nonSearchable.get(key);
                final var cannotAggregate = nonAggregatable.get(key);
                final Map<String, java.util.Set<String>> meta = new TreeMap<>();
                meta.putAll(fieldMetadata.getOrDefault(key, Map.of()));
                types.put(
                    type.getKey(),
                    new org.opensearch.action.fieldcaps.FieldCapabilities(
                        field.getKey(),
                        type.getKey(),
                        searchable.getOrDefault(key, Boolean.FALSE),
                        aggregatable.getOrDefault(key, Boolean.FALSE),
                        field.getValue().size() > 1 ? type.getValue().toArray(String[]::new) : null,
                        cannotSearch != null && cannotSearch.size() < type.getValue().size() ? cannotSearch.toArray(String[]::new) : null,
                        cannotAggregate != null && cannotAggregate.size() < type.getValue().size()
                            ? cannotAggregate.toArray(String[]::new)
                            : null,
                        meta
                    )
                );
            }
            fields.put(field.getKey(), types);
        }
        return new CapabilitiesResult(
            new org.opensearch.action.fieldcaps.FieldCapabilitiesResponse(answered.toArray(String[]::new), fields),
            java.util.Set.copyOf(metadataFields)
        );
    }

    /** The HTTP metadata-field extension is retained alongside the native filter-visible result. */
    private record CapabilitiesResult(org.opensearch.action.fieldcaps.FieldCapabilitiesResponse response, java.util.Set<
        String> metadataFields) {
    }

    private static void render(org.opensearch.rest.RestChannel channel, CapabilitiesResult result) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("indices", result.response().getIndices());
            builder.startObject("fields");
            for (var field : result.response().get().entrySet()) {
                builder.startObject(field.getKey());
                for (var type : field.getValue().entrySet()) {
                    final var capability = type.getValue();
                    builder.startObject(type.getKey());
                    builder.field("type", capability.getType());
                    builder.field("searchable", capability.isSearchable());
                    builder.field("aggregatable", capability.isAggregatable());
                    if (capability.nonSearchableIndices() != null) {
                        builder.field("non_searchable_indices", capability.nonSearchableIndices());
                    }
                    if (capability.nonAggregatableIndices() != null) {
                        builder.field("non_aggregatable_indices", capability.nonAggregatableIndices());
                    }
                    if (capability.meta().isEmpty() == false) {
                        builder.field("meta", capability.meta());
                    }
                    if (result.metadataFields().contains(field.getKey())) {
                        builder.field("metadata_field", true);
                    }
                    if (capability.indices() != null) {
                        builder.field("indices", capability.indices());
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private static org.opensearch.OpenSearchStatusException unsupported(String reason) {
        return new org.opensearch.OpenSearchStatusException(reason, RestStatus.NOT_IMPLEMENTED);
    }

    private void sendQuietly(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report a field capabilities refusal", e);
        }
    }
}
