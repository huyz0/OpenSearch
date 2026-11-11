/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.serverless.cluster.IndexDescriptor;

import java.io.IOException;
import java.util.Map;

/**
 * Renders an index descriptor's mapping and settings the way a GET on the index returns them. Split out of
 * {@link IndexAdminHandler}.
 */
final class IndexDescriptorRendering {

    private IndexDescriptorRendering() {}

    /** Which part of a descriptor a GET is asking for. */
    enum View {
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
    static void renderMapping(XContentBuilder builder, IndexDescriptor descriptor) throws IOException {
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
    static void renderSettings(XContentBuilder builder, IndexDescriptor descriptor, String only) throws IOException {
        final java.util.Map<String, String> settings = new java.util.LinkedHashMap<>();
        // Strings, not numbers, because that is what OpenSearch returns: settings are a string map there.
        settings.put("number_of_shards", Integer.toString(descriptor.numberOfShards()));
        // Zero, and true: a shard here has exactly one writer, and its redundancy is the object store
        // rather than a second copy. Reporting it is more honest than omitting it, because a client
        // reading a replica count gets the real one.
        settings.put("number_of_replicas", "0");
        settings.put("uuid", descriptor.uuid());
        settings.put("provided_name", descriptor.name());
        // Recorded at creation; a descriptor written by an older version has neither, and omitting them is
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
    static boolean matchesSettingName(String bare, String wanted) {
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
}
