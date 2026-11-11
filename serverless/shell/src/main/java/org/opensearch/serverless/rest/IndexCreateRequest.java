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
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;

import java.io.IOException;
import java.util.Map;

/**
 * What a create-index request asks for: its shard count, mapping and settings.
 */
final class IndexCreateRequest {
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
        // Through the same normalisation the request path uses, so "number_of_shards",
        // {"index": {"number_of_shards": n}} and "index.number_of_shards" are one key.
        final String fromTemplate = normalised(inherited).get(IndexMetadata.SETTING_NUMBER_OF_SHARDS);
        if (fromTemplate == null) {
            return requested;
        }
        try {
            return Integer.parseInt(fromTemplate);
        } catch (NumberFormatException e) {
            return requested;
        }
    }

    /**
     * A template's settings in core's canonical spelling.
     *
     * <p>A template may write {@code "number_of_shards"}, {@code {"index": {"number_of_shards": ...}}} or
     * {@code "index.number_of_shards"}, and core treats them as one. The create path used to prefix every
     * key with {@code index.} unconditionally, which turned the nested form into a key called
     * {@code index.index} holding a map's {@code toString()} and the dotted form into
     * {@code index.index.refresh_interval} -- a template written the way core's documentation shows it
     * was silently mangled and its shard count ignored.
     */
    static Settings normalised(org.opensearch.serverless.metadata.TemplateResolver.Inherited inherited) {
        if (inherited.settings().isEmpty()) {
            return Settings.EMPTY;
        }
        return Settings.builder().loadFromMap(inherited.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
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
        requested = IndexCreation.normalizeMapping(requested);
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
            // Core layers the request over a template with MergeReason.INDEX_TEMPLATE, where a field definition
            // replaces rather than merges parameter by parameter; mergeMappings is that rule.
            org.opensearch.serverless.metadata.TemplateResolver.mergeMappings(merged, own);
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
        final Settings.Builder merged = Settings.builder().put(normalised(inherited));
        // Structural settings are the create path's business, not a layered value: shards are resolved
        // separately above and replicas are refused outright.
        merged.remove(IndexMetadata.SETTING_NUMBER_OF_SHARDS);
        merged.remove(IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
        if (requested != null) {
            merged.put(requested);
        }
        final Settings result = merged.build();
        return result.isEmpty() ? null : result;
    }

    IndexCreateRequest(int shards, String mapping, Settings settings) {
        this(shards, mapping, settings, false);
    }

    private IndexCreateRequest(int shards, String mapping, Settings settings, boolean explicitShards) {
        this.shards = shards;
        this.mapping = mapping;
        this.settings = settings;
        this.explicitShards = explicitShards;
    }

    IndexCreateRequest withMapping(String updatedMapping) {
        return new IndexCreateRequest(shards, updatedMapping, settings, explicitShards);
    }

    static IndexCreateRequest parse(RestRequest request) throws IndexAdminHandler.RefusedException {
        final int fromQuery = request.paramAsInt("shards", 1);
        if (request.hasContent() == false) {
            return new IndexCreateRequest(fromQuery, null, null, fromQuery != 1);
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
            throw new IndexAdminHandler.RefusedException(RestStatus.BAD_REQUEST, "malformed_body", "the body is not a JSON object");
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
     * @throws IndexAdminHandler.RefusedException if the body asks for something refused
     */
    static IndexCreateRequest parse(Map<String, Object> parsed, String body, int fromQuery) throws IndexAdminHandler.RefusedException {
        try {
            if (parsed == null) {
                throw new IllegalArgumentException("no body");
            }
        } catch (Exception e) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.BAD_REQUEST,
                "malformed_body",
                "could not parse the request body: " + e.getMessage()
            );
        }

        final boolean envelope = parsed.containsKey("settings") || parsed.containsKey("mappings") || parsed.containsKey("aliases");
        if (envelope == false) {
            // The shape this handler has always taken: the body is the mapping.
            return new IndexCreateRequest(fromQuery, body, null, fromQuery != 1);
        }

        if (parsed.containsKey("aliases")) {
            // Refused rather than dropped. An alias silently not created is a search that silently
            // matches nothing later, which is the same class of failure this whole parse exists to end.
            throw new IndexAdminHandler.RefusedException(
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
                throw new IndexAdminHandler.RefusedException(
                    RestStatus.BAD_REQUEST,
                    "malformed_body",
                    "could not read 'mappings': " + e.getMessage()
                );
            }
        } else if (parsed.containsKey("mappings")) {
            throw new IndexAdminHandler.RefusedException(RestStatus.BAD_REQUEST, "malformed_body", "'mappings' must be an object");
        }

        Settings settings = Settings.EMPTY;
        if (parsed.get("settings") instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> raw = (Map<String, Object>) parsed.get("settings");
            // Prefixing is what turns a client's "number_of_shards" into the "index.number_of_shards"
            // core names it by, and it is core's own normalisation rather than a guess at one.
            settings = Settings.builder().loadFromMap(raw).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        } else if (parsed.containsKey("settings")) {
            throw new IndexAdminHandler.RefusedException(RestStatus.BAD_REQUEST, "malformed_body", "'settings' must be an object");
        }

        return fromSettings(settings, mapping, fromQuery);
    }

    static IndexCreateRequest fromSettings(Settings settings, String mapping, int fromQuery) throws IndexAdminHandler.RefusedException {
        final String replicas = settings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
        if (replicas != null && "0".equals(replicas) == false) {
            // Refused, and with the real reason. Accepting it would mean recording a replica count that
            // nothing acts on, and reporting a redundancy this deployment does not provide that way.
            throw new IndexAdminHandler.RefusedException(
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
                throw new IndexAdminHandler.RefusedException(RestStatus.BAD_REQUEST, "malformed_body", "number_of_shards must be a number");
            }
        }
        if (shards < 1 || shards > IndexDescriptor.MAX_SHARDS) {
            throw new IndexAdminHandler.RefusedException(
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
        return new IndexCreateRequest(shards, mapping, carried.isEmpty() ? null : carried, requested != null || fromQuery != 1);
    }
}
