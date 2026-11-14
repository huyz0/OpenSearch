/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.rollover.Condition;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.AliasAtGeneration;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Index administration for plugins: whether an index exists, its mappings, settings and aliases, and changes to them.
 *
 * <p><b>Why plugins ask.</b> A plugin that keeps its state in its own indices checks they exist, reads and updates
 * their mappings when it upgrades, changes their settings, and rolls its history indices over by alias. The alerting
 * plugin does all of these before it runs a monitor, and Index Management, anomaly detection and ML Commons do most.
 *
 * <p><b>Reads</b> are answered from the metadata plane's descriptors, one cached descriptor read per index, in core's
 * response types. A name may be an index, an alias, or end in a wildcard; asking for every index answers for the
 * indices this node serves, never by listing every index there is.
 *
 * <p><b>Changes</b> go through the shell's own REST handlers, in process; see {@link InProcessRest}. A failure comes
 * back as the core exception a plugin catches, so a missing index is an {@code IndexNotFoundException} and a lost
 * creation race a {@code ResourceAlreadyExistsException}.
 */
final class PluginIndexAdmin {

    /** The most names a wildcard in a plugin's request may expand to. */
    private static final int WILDCARD_CAP = 10_000;

    private final ServerlessNode node;
    private final RestHandler mappings;
    private final RestHandler settings;
    private final RestHandler indices;
    private final RestHandler aliases;
    private final RestHandler rollover;

    PluginIndexAdmin(ServerlessNode node) {
        this.node = node;
        this.mappings = new org.opensearch.serverless.rest.MappingUpdateHandler(() -> node.metadataPlane, () -> node);
        this.settings = new org.opensearch.serverless.rest.SettingsUpdateHandler(() -> node.metadataPlane, () -> node);
        this.indices = new org.opensearch.serverless.rest.IndexAdminHandler(() -> node.metadataPlane, () -> node);
        this.aliases = org.opensearch.serverless.rest.AliasHandler.forPlugins(() -> node.metadataPlane, () -> node);
        this.rollover = new org.opensearch.serverless.rest.RolloverHandler(() -> node, () -> node.metadataPlane);
    }

    // ---- reads

    IndicesExistsResponse exists(MetadataPlane plane, IndicesExistsRequest request) throws IOException {
        for (String name : request.indices()) {
            if (resolve(plane, name, request.indicesOptions()).isEmpty() && isWildcard(name) == false) {
                return new IndicesExistsResponse(false);
            }
        }
        return new IndicesExistsResponse(true);
    }

    GetIndexResponse getIndex(MetadataPlane plane, GetIndexRequest request) throws IOException {
        final Map<String, IndexMetadata> found = collect(plane, request.indices(), request.indicesOptions());
        final Map<String, MappingMetadata> mappings = new LinkedHashMap<>();
        final Map<String, List<AliasMetadata>> aliases = new LinkedHashMap<>();
        final Map<String, Settings> settings = new LinkedHashMap<>();
        for (Map.Entry<String, IndexMetadata> entry : found.entrySet()) {
            if (entry.getValue().mapping() != null) {
                mappings.put(entry.getKey(), entry.getValue().mapping());
            }
            aliases.put(entry.getKey(), aliasesOf(plane, entry.getKey()));
            settings.put(entry.getKey(), entry.getValue().getSettings());
        }
        return new GetIndexResponse(found.keySet().toArray(new String[0]), mappings, aliases, settings, Map.of(), Map.of(), Map.of());
    }

    GetMappingsResponse getMappings(MetadataPlane plane, GetMappingsRequest request) throws IOException {
        final Map<String, MappingMetadata> mappings = new LinkedHashMap<>();
        for (Map.Entry<String, IndexMetadata> entry : collect(plane, request.indices(), request.indicesOptions()).entrySet()) {
            mappings.put(entry.getKey(), entry.getValue().mapping() == null ? MappingMetadata.EMPTY_MAPPINGS : entry.getValue().mapping());
        }
        return new GetMappingsResponse(mappings);
    }

    GetSettingsResponse getSettings(MetadataPlane plane, GetSettingsRequest request) throws IOException {
        final Map<String, Settings> settings = new LinkedHashMap<>();
        for (Map.Entry<String, IndexMetadata> entry : collect(plane, request.indices(), request.indicesOptions()).entrySet()) {
            Settings indexSettings = entry.getValue().getSettings();
            if (request.names() != null && request.names().length > 0) {
                indexSettings = indexSettings.filter(key -> org.opensearch.common.regex.Regex.simpleMatch(request.names(), key));
            }
            settings.put(entry.getKey(), indexSettings);
        }
        return new GetSettingsResponse(settings, Map.of());
    }

    GetAliasesResponse getAliases(MetadataPlane plane, GetAliasesRequest request) throws IOException {
        final Map<String, List<AliasMetadata>> result = new LinkedHashMap<>();
        final String[] named = request.aliases();
        final boolean anyAlias = named == null
            || named.length == 0
            || (named.length == 1 && ("_all".equals(named[0]) || "*".equals(named[0])));
        if (anyAlias == false) {
            final String[] onlyIndices = request.indices();
            final boolean anyIndex = onlyIndices == null || onlyIndices.length == 0;
            for (String alias : named) {
                final Optional<AliasAtGeneration> record = plane.aliasWithGeneration(alias);
                if (record.isEmpty()) {
                    continue;
                }
                final AliasRecord aliasRecord = record.get().alias();
                for (String index : aliasRecord.indices()) {
                    if (anyIndex || org.opensearch.common.regex.Regex.simpleMatch(onlyIndices, index)) {
                        result.computeIfAbsent(index, k -> new ArrayList<>()).add(aliasMetadata(aliasRecord, index));
                    }
                }
            }
            return new GetAliasesResponse(result);
        }
        for (String index : collect(plane, request.indices(), IndicesOptions.lenientExpandOpen()).keySet()) {
            final List<AliasMetadata> of = aliasesOf(plane, index);
            if (of.isEmpty() == false) {
                result.put(index, of);
            }
        }
        return new GetAliasesResponse(result);
    }

    // ---- changes

    AcknowledgedResponse putMapping(PutMappingRequest request) throws Exception {
        final String index = request.getConcreteIndex() != null ? request.getConcreteIndex().getName() : dateMath(request.indices());
        return acknowledged(
            InProcessRest.call(
                mappings,
                RestRequest.Method.PUT,
                "/" + index + "/_mapping",
                Map.of("index", index),
                request.source(),
                node.nodeClient
            ),
            index
        );
    }

    AcknowledgedResponse updateSettings(UpdateSettingsRequest request) throws Exception {
        final String index = dateMath(request.indices());
        final XContentBuilder body = JsonXContent.contentBuilder().startObject();
        request.settings().toXContent(body, new ToXContent.MapParams(Map.of("flat_settings", "true")));
        body.endObject();
        return acknowledged(
            InProcessRest.call(
                settings,
                RestRequest.Method.PUT,
                "/" + index + "/_settings",
                Map.of("index", index),
                body.toString(),
                node.nodeClient
            ),
            index
        );
    }

    AcknowledgedResponse deleteIndex(DeleteIndexRequest request) throws Exception {
        boolean acknowledged = true;
        for (String named : request.indices()) {
            final String index = dateMath(named);
            final InProcessRest.Answer answer = InProcessRest.call(
                indices,
                RestRequest.Method.DELETE,
                "/" + index,
                Map.of("index", index),
                null,
                node.nodeClient
            );
            if (answer.ok() == false) {
                throw answer.failure(index);
            }
            acknowledged &= readAcknowledged(answer);
        }
        return new AcknowledgedResponse(acknowledged);
    }

    AcknowledgedResponse updateAliases(IndicesAliasesRequest request) throws Exception {
        final XContentBuilder body = JsonXContent.contentBuilder();
        request.toXContent(body, ToXContent.EMPTY_PARAMS);
        return acknowledged(
            InProcessRest.call(aliases, RestRequest.Method.POST, "/_aliases", Map.of(), body.toString(), node.nodeClient),
            null
        );
    }

    RolloverResponse rollover(RolloverRequest request) throws Exception {
        final String target = request.getRolloverTarget();
        final Map<String, String> params = new LinkedHashMap<>();
        params.put("index", target);
        String path = "/" + target + "/_rollover";
        if (request.getNewIndexName() != null) {
            params.put("new_index", request.getNewIndexName());
            path += "/" + request.getNewIndexName();
        }
        if (request.isDryRun()) {
            params.put("dry_run", "true");
        }
        final InProcessRest.Answer answer = InProcessRest.call(
            rollover,
            RestRequest.Method.POST,
            path,
            params,
            rolloverBody(request),
            node.nodeClient
        );
        if (answer.ok() == false) {
            throw answer.failure(target);
        }
        try (XContentParser parser = answer.parser()) {
            return RolloverResponse.fromXContent(parser);
        }
    }

    /** The REST body core's rollover endpoint reads: its conditions and the new index's shape. */
    private static String rolloverBody(RolloverRequest request) throws IOException {
        final XContentBuilder body = JsonXContent.contentBuilder().startObject();
        if (request.getConditions().isEmpty() == false) {
            body.startObject("conditions");
            for (Condition<?> condition : request.getConditions().values()) {
                condition.toXContent(body, ToXContent.EMPTY_PARAMS);
            }
            body.endObject();
        }
        final CreateIndexRequest create = request.getCreateIndexRequest();
        if (create.settings().isEmpty() == false) {
            body.startObject("settings");
            create.settings().toXContent(body, ToXContent.EMPTY_PARAMS);
            body.endObject();
        }
        if (create.mappings() != null && create.mappings().isEmpty() == false && "{}".equals(create.mappings()) == false) {
            body.rawField(
                "mappings",
                new java.io.ByteArrayInputStream(create.mappings().getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                org.opensearch.common.xcontent.XContentType.JSON
            );
        }
        if (create.aliases().isEmpty() == false) {
            body.startObject("aliases");
            for (Alias alias : create.aliases()) {
                alias.toXContent(body, ToXContent.EMPTY_PARAMS);
            }
            body.endObject();
        }
        return body.endObject().toString();
    }

    private static AcknowledgedResponse acknowledged(InProcessRest.Answer answer, String index) throws Exception {
        if (answer.ok() == false) {
            throw answer.failure(index);
        }
        return new AcknowledgedResponse(readAcknowledged(answer));
    }

    private static boolean readAcknowledged(InProcessRest.Answer answer) throws IOException {
        try (XContentParser parser = answer.parser()) {
            return Boolean.TRUE.equals(parser.map().get("acknowledged"));
        }
    }

    // ---- names

    /** Resolves only date-math names; it is never asked to resolve names against cluster state. */
    private static final org.opensearch.cluster.metadata.IndexNameExpressionResolver DATE_MATH =
        new org.opensearch.cluster.metadata.IndexNameExpressionResolver(
            new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY)
        );

    /**
     * A date-math name as core resolves it in every index expression: the alerting plugin names its history indices
     * {@code <.opendistro-alerting-alert-history-{now/d}-1>}.
     */
    static String dateMath(String name) {
        return name.startsWith("<") && name.endsWith(">") ? DATE_MATH.resolveDateMathExpression(name) : name;
    }

    private static String dateMath(String[] names) {
        final List<String> resolved = new ArrayList<>();
        for (String name : names) {
            resolved.add(dateMath(name));
        }
        return String.join(",", resolved);
    }

    private static boolean isWildcard(String name) {
        return name.contains("*");
    }

    /**
     * The indices a set of names stands for, described: the indices this node serves for none or {@code _all}, the
     * names a trailing wildcard matches, an alias's indices, and each concrete index.
     */
    private Map<String, IndexMetadata> collect(MetadataPlane plane, String[] names, IndicesOptions options) throws IOException {
        final Map<String, IndexMetadata> found = new LinkedHashMap<>();
        if (names == null || names.length == 0 || (names.length == 1 && ("_all".equals(names[0]) || "*".equals(names[0])))) {
            for (IndexMetadata index : node.clusterService().state().metadata()) {
                found.put(index.getIndex().getName(), index);
            }
            return found;
        }
        for (String name : names) {
            final List<IndexDescriptor> resolved = resolve(plane, name, options);
            if (resolved.isEmpty() && isWildcard(name) == false && options.ignoreUnavailable() == false) {
                throw new org.opensearch.index.IndexNotFoundException(name);
            }
            for (IndexDescriptor descriptor : resolved) {
                found.put(descriptor.name(), descriptor.toIndexMetadata(Map.of()));
            }
        }
        return found;
    }

    private static List<IndexDescriptor> resolve(MetadataPlane plane, String name, IndicesOptions options) throws IOException {
        name = dateMath(name);
        final List<IndexDescriptor> resolved = new ArrayList<>();
        if (isWildcard(name)) {
            if (name.indexOf('*') != name.length() - 1) {
                throw new IllegalArgumentException("only a trailing wildcard is supported here, got [" + name + "]");
            }
            for (String match : plane.namesWithPrefix(name.substring(0, name.length() - 1), WILDCARD_CAP)) {
                plane.describe(match).ifPresent(resolved::add);
            }
            return resolved;
        }
        final Optional<IndexDescriptor> index = plane.describe(name);
        if (index.isPresent()) {
            resolved.add(index.get());
            return resolved;
        }
        final Optional<AliasAtGeneration> alias = plane.aliasWithGeneration(name);
        if (alias.isPresent()) {
            for (String member : alias.get().alias().indices()) {
                plane.describe(member).ifPresent(resolved::add);
            }
        }
        return resolved;
    }

    private static List<AliasMetadata> aliasesOf(MetadataPlane plane, String index) throws IOException {
        final Optional<IndexDescriptor> descriptor = plane.describe(index);
        if (descriptor.isEmpty()) {
            return List.of();
        }
        final List<AliasMetadata> result = new ArrayList<>();
        for (String alias : plane.aliasesOf(descriptor.get())) {
            final Optional<AliasAtGeneration> record = plane.aliasWithGeneration(alias);
            result.add(record.isPresent() ? aliasMetadata(record.get().alias(), index) : AliasMetadata.builder(alias).build());
        }
        return result;
    }

    static AliasMetadata aliasMetadata(AliasRecord alias, String index) {
        final String writeIndex = alias.writeIndex();
        return AliasMetadata.builder(alias.name()).writeIndex(writeIndex == null ? null : writeIndex.equals(index)).build();
    }
}
