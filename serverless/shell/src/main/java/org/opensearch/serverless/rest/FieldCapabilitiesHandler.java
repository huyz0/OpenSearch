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
import java.util.regex.Pattern;

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
                if (body.get("fields") instanceof List<?> named && named.isEmpty() == false) {
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
                IndexAdminHandler.gate(
                    serving,
                    org.opensearch.action.fieldcaps.FieldCapabilitiesAction.NAME,
                    new org.opensearch.action.fieldcaps.FieldCapabilitiesRequest().indices(index),
                    () -> {
                        answer(channel, metadata, serving, index, wantedFields);
                        return null;
                    }
                );
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

    private void answer(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        String index,
        String fields
    ) throws Exception {
        final List<String> names = new java.util.ArrayList<>(IndexPatterns.expand(metadata, index, serving.patternCap()));
        // A plugin's index is not described through the request path, whatever spelling reached it.
        names.removeIf(serving::isSystemIndex);
        if (names.isEmpty()) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "no_index", "name an index or a prefix pattern");
            return;
        }

        final List<Pattern> wanted = compile(fields);
        // field -> type -> the indices mapping it that way. Sorted so the answer is stable across calls,
        // which matters for a response a client may diff.
        final Map<String, Map<String, TreeSet<String>>> capabilities = new TreeMap<>();
        final Map<String, Boolean> searchable = new LinkedHashMap<>();
        final Map<String, Boolean> aggregatable = new LinkedHashMap<>();
        final List<String> answered = new ArrayList<>();
        final java.util.Set<String> metadataFields = new java.util.HashSet<>();

        for (String name : names) {
            final Optional<IndexDescriptor> descriptor = metadata.describe(name);
            if (descriptor.isEmpty()) {
                if (IndexPatterns.isPrefixPattern(index)) {
                    // A pattern is a filter over what exists, not an assertion that anything does.
                    continue;
                }
                sendQuietly(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + name);
                return;
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
                for (MappedFieldType type : mapperService.fieldTypes()) {
                    if (matches(wanted, type.name()) == false) {
                        continue;
                    }
                    // familyTypeName, not typeName: OpenSearch reports the family a field belongs to, so a
                    // specialised keyword answers as the thing a client can actually act on.
                    capabilities.computeIfAbsent(type.name(), key -> new TreeMap<>())
                        .computeIfAbsent(type.familyTypeName(), key -> new TreeSet<>())
                        .add(name);
                    final String key = type.name() + " " + type.familyTypeName();
                    searchable.merge(key, type.isSearchable(), Boolean::logicalAnd);
                    aggregatable.merge(key, type.isAggregatable(), Boolean::logicalAnd);
                    // Flagged, as OpenSearch flags them. A caller asking fields=* gets _id, _index, _seq_no
                    // and the rest, and needs to be able to tell them from the fields it put there itself.
                    if (mapperService.isMetadataField(type.name())) {
                        metadataFields.add(type.name());
                    }
                }
            } finally {
                mapperService.close();
            }
        }

        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("indices");
            for (String name : answered) {
                builder.value(name);
            }
            builder.endArray();
            builder.startObject("fields");
            for (Map.Entry<String, Map<String, TreeSet<String>>> field : capabilities.entrySet()) {
                builder.startObject(field.getKey());
                for (Map.Entry<String, TreeSet<String>> type : field.getValue().entrySet()) {
                    final String key = field.getKey() + " " + type.getKey();
                    builder.startObject(type.getKey());
                    builder.field("type", type.getKey());
                    // Conservative where indices disagree: a field is reported searchable only if it is
                    // searchable everywhere it appears, because a query that works against one index and
                    // silently matches nothing in another is worse than being told it cannot be run.
                    builder.field("searchable", searchable.getOrDefault(key, Boolean.FALSE));
                    builder.field("aggregatable", aggregatable.getOrDefault(key, Boolean.FALSE));
                    if (metadataFields.contains(field.getKey())) {
                        builder.field("metadata_field", true);
                    }
                    if (field.getValue().size() > 1) {
                        // Named only when there is a disagreement to attribute. OpenSearch omits this key
                        // when every index agrees, and a client uses its presence to detect a conflict.
                        builder.startArray("indices");
                        for (String name : type.getValue()) {
                            builder.value(name);
                        }
                        builder.endArray();
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

    private static List<Pattern> compile(String fields) {
        final List<Pattern> patterns = new ArrayList<>();
        for (String each : fields.split(",")) {
            final String field = each.trim();
            if (field.isEmpty()) {
                continue;
            }
            final StringBuilder regex = new StringBuilder();
            for (char character : field.toCharArray()) {
                if (character == '*') {
                    regex.append(".*");
                } else {
                    regex.append(Pattern.quote(String.valueOf(character)));
                }
            }
            patterns.add(Pattern.compile(regex.toString()));
        }
        return patterns;
    }

    private static boolean matches(List<Pattern> wanted, String field) {
        for (Pattern pattern : wanted) {
            if (pattern.matcher(field).matches()) {
                return true;
            }
        }
        return false;
    }

    private void sendQuietly(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report a field capabilities refusal", e);
        }
    }
}
