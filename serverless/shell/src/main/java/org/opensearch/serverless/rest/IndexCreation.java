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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Creating an index: the request parsed from REST or a plugin's native request, its settings and mapping validated,
 * and the descriptor written. Split out of {@link IndexAdminHandler}, which keeps the entry points.
 */
final class IndexCreation {

    private IndexCreation() {}

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
     * Creates an index from a native request using the REST path's settings and template validation.
     *
     * @param serving the node supplying plugin-aware mapper validation
     * @param metadata the metadata plane
     * @param request the admitted native request
     * @return the created descriptor
     * @throws Exception if validation or metadata persistence fails
     */
    public static IndexDescriptor createIndex(
        org.opensearch.serverless.shell.ServerlessNode serving,
        MetadataPlane metadata,
        org.opensearch.action.admin.indices.create.CreateIndexRequest request
    ) throws Exception {
        try {
            return createIndex(serving, metadata, request.index(), fromNative(request));
        } catch (IndexAdminHandler.RefusedException e) {
            throw new org.opensearch.OpenSearchStatusException(e.getMessage(), e.status);
        } catch (org.opensearch.serverless.metadata.TemplateResolver.MissingComponentException e) {
            throw new org.opensearch.OpenSearchStatusException(e.getMessage(), RestStatus.BAD_REQUEST);
        }
    }

    /** Shared native creation payload validation for create-index and rollover. */
    static IndexCreateRequest fromNative(org.opensearch.action.admin.indices.create.CreateIndexRequest request)
        throws IndexAdminHandler.RefusedException {
        final var invalid = request.validate();
        if (invalid != null) {
            throw invalid;
        }
        if (request.context() != null) {
            throw new org.opensearch.OpenSearchStatusException("create-index contexts are not supported", RestStatus.NOT_IMPLEMENTED);
        }
        if (request.aliases().isEmpty() == false) {
            throw new org.opensearch.OpenSearchStatusException(
                "aliases in a create-index request are not supported; create the index, then create the alias",
                RestStatus.NOT_IMPLEMENTED
            );
        }
        final Settings settings = Settings.builder().put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        final String mapping = normalizeMapping(request.mappings());
        return IndexCreateRequest.fromSettings(settings, mapping, 1);
    }

    static String normalizeMapping(String source) {
        if (source == null || source.isEmpty()) {
            return null;
        }
        try (
            var parser = org.opensearch.common.xcontent.json.JsonXContent.jsonXContent.createParser(
                org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                source
            )
        ) {
            if (parser.nextToken() != org.opensearch.core.xcontent.XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("mappings must be an object");
            }
            Map<String, Object> mapping = parser.mapOrdered();
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException("trailing content after mappings");
            }
            // Native map-based builders wrap mappings in _doc; templates use the unwrapped shape.
            final String type = org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
            if (mapping.containsKey(type)) {
                if (mapping.size() != 1 || mapping.get(type) instanceof Map == false) {
                    throw new IllegalArgumentException("a typed mapping must contain one mapping object");
                }
                @SuppressWarnings("unchecked")
                final Map<String, Object> unwrapped = (Map<String, Object>) mapping.get(type);
                mapping = unwrapped;
            }
            try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                builder.map(mapping);
                return builder.toString();
            }
        } catch (IOException | RuntimeException e) {
            throw new org.opensearch.OpenSearchStatusException("could not parse mappings: " + e.getMessage(), RestStatus.BAD_REQUEST, e);
        }
    }

    /**
     * Creates an index from a request, with what matching templates contribute layered under it.
     *
     * <p>A caller who spelled out a mapping is not overruled by configuration they may not know exists.
     * Shared with rollover and data streams, which create indices the same way from a body of their own.
     *
     * @param serving the node supplying plugin-aware mapper validation
     * @param metadata the metadata plane
     * @param index the name
     * @param create the request
     * @return the descriptor as created
     * @throws IOException if a register cannot be read or written
     */
    static IndexDescriptor createIndex(
        org.opensearch.serverless.shell.ServerlessNode serving,
        MetadataPlane metadata,
        String index,
        IndexCreateRequest create
    ) throws IOException, IndexAdminHandler.RefusedException {
        return createIndex(
            serving,
            metadata,
            index,
            create,
            org.opensearch.serverless.metadata.TemplateResolver.resolveFromStores(
                metadata.indexTemplates(),
                metadata::componentTemplates,
                index
            )
        );
    }

    /**
     * Creates an index from a request and what it inherits, for a caller that resolved the templates
     * against a different name than the index's own.
     *
     * <p>A data stream's backing index is named {@code .ds-<stream>-<n>}, which no template pattern written
     * for the stream matches; what it inherits is what the <em>stream's</em> name matches, so the caller
     * resolves against that and hands the result here.
     *
     * @param serving the node supplying plugin-aware mapper validation
     * @param metadata the metadata plane
     * @param index the name
     * @param create the request
     * @param inherited what the templates contributed
     * @return the descriptor as created
     * @throws IOException if a register cannot be read or written
     * @throws IndexAdminHandler.RefusedException if the merged settings are ones core would refuse, or this design cannot honour
     */
    static IndexDescriptor createIndex(
        org.opensearch.serverless.shell.ServerlessNode serving,
        MetadataPlane metadata,
        String index,
        IndexCreateRequest create,
        org.opensearch.serverless.metadata.TemplateResolver.Inherited inherited
    ) throws IOException, IndexAdminHandler.RefusedException {
        final Settings requested = IndexCreateRequest.settingsWith(create.settings, inherited);
        // The shard mode is the descriptor's own field, not a setting core knows, so it is taken out
        // before core validates the rest. Absent, the index has no unit layout and routes as it always has.
        final org.opensearch.serverless.routing.ShardMode mode;
        try {
            mode = org.opensearch.serverless.routing.ShardMode.requested(requested);
        } catch (IllegalArgumentException e) {
            throw new IndexAdminHandler.RefusedException(RestStatus.BAD_REQUEST, "illegal_argument_exception", e.getMessage());
        }
        final Settings settings = org.opensearch.serverless.routing.ShardMode.without(requested);
        // Parsed and validated before the descriptor exists, not by the first shard to open it. A value
        // core cannot parse used to land in the register and fail every node that later built
        // IndexSettings from it, with a 400 to the caller that read like a refusal.
        validateIndexSettings(settings == null ? Settings.EMPTY : settings, true);
        final int declaredShards = IndexCreateRequest.shardsWith(create.shards, create.explicitShards, inherited);
        IndexDescriptor descriptor = new IndexDescriptor(
            index,
            UUID.randomUUID().toString(),
            declaredShards,
            IndexCreateRequest.mappingWith(create.mapping, inherited),
            settings
        ).createdAt(metadata.clock().getAsLong(), org.opensearch.Version.CURRENT.id);
        if (mode != null) {
            descriptor = descriptor.withLayout(org.opensearch.serverless.routing.UnitLayout.initial(mode, declaredShards));
        }
        if (serving == null) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.SERVICE_UNAVAILABLE,
                "node_unavailable",
                "the serving node is unavailable"
            );
        }
        // Validate the final mapping and analysis configuration with this node's actual plugin registry.
        // The administrative factory creates no registered IndexService, shard, or on-disk index.
        final var candidate = descriptor.toIndexMetadata(Map.of());
        try (var mapper = serving.indicesService().createIndexMapperService(candidate)) {
            mapper.merge(candidate, org.opensearch.index.mapper.MapperService.MergeReason.INDEX_TEMPLATE);
            validateTimestampMapping(mapper, candidate, descriptor.mapping());
            final var validatedSettings = mapper.getIndexSettings();
            if (validatedSettings.getIndexSortConfig().hasIndexSort()) {
                final var noCache = new org.opensearch.index.fielddata.IndexFieldDataCache.None();
                validatedSettings.getIndexSortConfig()
                    .buildIndexSort(
                        validatedSettings.shouldWidenIndexSortType(),
                        mapper::fieldType,
                        (field, lookup) -> field.fielddataBuilder(index, lookup).build(noCache, serving.services().circuitBreakerService())
                    );
            }
        }
        metadata.createIndex(descriptor);
        return descriptor;
    }

    static void validateTimestampMapping(org.opensearch.index.mapper.MapperService mapper, IndexMetadata candidate, String source) {
        final String metadataField = org.opensearch.index.mapper.DataStreamFieldMapper.NAME;
        if (source == null || source.contains(metadataField) == false) {
            return;
        }
        final Object declared = candidate.mapping().sourceAsMap().get(metadataField);
        if (declared instanceof Map<?, ?> == false) {
            return;
        }
        final Map<?, ?> guard = (Map<?, ?>) declared;
        if (Boolean.parseBoolean(String.valueOf(guard.get("enabled"))) == false) {
            return;
        }
        String name = org.opensearch.index.mapper.DataStreamFieldMapper.Defaults.TIMESTAMP_FIELD.getName();
        final Object selected = guard.get("timestamp_field");
        if (selected instanceof Map<?, ?> named) {
            if (named.get("name") instanceof String == false || ((String) named.get("name")).isEmpty()) {
                throw new org.opensearch.index.mapper.MapperParsingException("data stream timestamp field requires a name");
            }
            name = (String) named.get("name");
        }
        final var field = mapper.fieldType(name);
        if (field instanceof org.opensearch.index.mapper.DateFieldMapper.DateFieldType == false || field.name().equals(name) == false) {
            throw new org.opensearch.index.mapper.MapperParsingException(
                "data stream timestamp field [" + name + "] must be a date or date_nanos field"
            );
        }
        if (mapper.getIndexSettings().isPluggableDataFormatEnabled() == false && field.hasDocValues() == false) {
            throw new org.opensearch.index.mapper.MapperParsingException("data stream timestamp field [" + name + "] requires doc values");
        }
    }

    /**
     * Names an index setting this design accepts nowhere, with the reason, or null.
     *
     * <p>{@code index.default_pipeline} and {@code index.final_pipeline} used to be here, refused because
     * nothing read them: stored, they would have reported a pipeline that never ran. The write path reads
     * both now -- see {@code IngestPipelines#pipelinesFor} -- so they are ordinary settings. The search
     * one is still refused, for the reason all three were.
     *
     * @param settings normalised settings
     * @return the reason to refuse, or null when there is none
     */
    static String unsupportedIndexSetting(Settings settings) {
        final String key = org.opensearch.index.IndexSettings.DEFAULT_SEARCH_PIPELINE.getKey();
        if (settings.hasValue(key)) {
            return key
                + " is not supported: an index-level search pipeline is applied by the search path, and this "
                + "shell's does not read it, so the value would be stored and never run. Name the pipeline on "
                + "the request instead (?search_pipeline=)";
        }
        return null;
    }

    /**
     * Refuses settings core would not accept, or this design cannot honour, before anything is written.
     *
     * @param given normalised ({@code index.}-prefixed) settings
     * @param atCreation whether this is a create, where dependencies between settings are checked as
     *     core checks them; an update validates each value on its own, as core's update does
     * @throws IndexAdminHandler.RefusedException with the status and words the caller should see
     */
    static void validateIndexSettings(Settings given, boolean atCreation) throws IndexAdminHandler.RefusedException {
        // The shard mode is the shell's, not core's: checked here, and left out of what core validates.
        try {
            org.opensearch.serverless.routing.ShardMode.requested(given);
        } catch (IllegalArgumentException e) {
            throw new IndexAdminHandler.RefusedException(RestStatus.BAD_REQUEST, "illegal_argument_exception", e.getMessage());
        }
        // So is the segment placement.
        final String placement = given.get(org.opensearch.serverless.shell.SegmentPlacement.INDEX_SETTING);
        if (placement != null) {
            try {
                org.opensearch.serverless.shell.SegmentPlacement.check(placement);
            } catch (IllegalArgumentException e) {
                throw new IndexAdminHandler.RefusedException(RestStatus.BAD_REQUEST, "illegal_argument_exception", e.getMessage());
            }
        }
        final Settings settings = org.opensearch.serverless.routing.ShardMode.without(given)
            .filter(key -> org.opensearch.serverless.shell.SegmentPlacement.INDEX_SETTING.equals(key) == false);
        final String unsupported = unsupportedIndexSetting(settings);
        if (unsupported != null) {
            throw new IndexAdminHandler.RefusedException(RestStatus.NOT_IMPLEMENTED, "unsupported_setting", unsupported);
        }
        final var scoped = org.opensearch.common.settings.IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        for (String key : settings.keySet()) {
            final var setting = scoped.get(key);
            if (scoped.isPrivateSetting(key) || (setting != null && setting.isPrivateIndex())) {
                throw new IndexAdminHandler.RefusedException(
                    RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    "private index setting [" + key + "] can not be set explicitly"
                );
            }
        }
        try {
            // Only user settings reach the descriptor: private values belong to its authoritative fields.
            scoped.validate(settings, atCreation, false, false);
        } catch (IllegalArgumentException e) {
            throw new IndexAdminHandler.RefusedException(RestStatus.BAD_REQUEST, "illegal_argument_exception", e.getMessage());
        }
    }
}
