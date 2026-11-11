/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.io.IOException;
import java.util.List;

/**
 * Answers the alias reads: one alias, the aliases of an index, and whether an alias names an index. Split out of
 * {@link AliasHandler}.
 */
final class AliasReads {

    private AliasReads() {}

    /**
     * Whether an index is under an alias.
     *
     * @param channel the channel to answer on
     * @param metadata the metadata plane
     * @param name the alias
     * @param indices the indices asked about
     * @param bodyless whether this is a HEAD
     * @throws IOException if the read fails
     */
    static void membership(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        String name,
        List<String> indices,
        boolean bodyless
    ) throws IOException {
        final var resolved = metadata.resolve(name);
        // A data stream's backing indices are not "under an alias", so the answer for one is core's 404.
        final boolean all = resolved.alias() != null
            && resolved.alias().dataStream() == false
            && resolved.alias().indices().containsAll(indices);
        if (bodyless) {
            channel.sendResponse(
                new BytesRestResponse(all ? RestStatus.OK : RestStatus.NOT_FOUND, BytesRestResponse.TEXT_CONTENT_TYPE, "")
            );
            return;
        }
        if (all == false) {
            channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "alias_not_found", "alias [" + name + "] does not cover " + indices)
            );
            return;
        }
        // OpenSearch's shape: keyed by index, each carrying the aliases it is under that were asked about.
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            for (String index : indices) {
                builder.startObject(index);
                builder.startObject("aliases");
                writeAliasBody(builder, name, resolved.alias().writeIndex(), index);
                builder.endObject();
                builder.endObject();
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    static void read(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String name) throws IOException {
        final var resolved = metadata.resolve(name);
        if (resolved.alias() == null || resolved.alias().dataStream()) {
            // A data stream is not an alias, and rendering it as one -- backing indices keyed as if a
            // caller had aliased them -- would tell a client it can be managed through this API.
            channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_FOUND,
                    "alias_not_found",
                    resolved.index() != null ? "[" + name + "] is an index, not an alias"
                        : resolved.alias() != null ? "[" + name + "] is a data stream, not an alias; GET /_data_stream/" + name
                        : "no such alias: " + name
                )
            );
            return;
        }
        // Core's shape: keyed by index, each carrying the alias. This was {alias, indices[]} -- the last
        // shell-specific shape on the alias surface, kept for the shell's own callers and now retired,
        // since a client library's alias parser is written for this one and nothing else reads the other.
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            for (String index : resolved.alias().indices()) {
                builder.startObject(index);
                builder.startObject("aliases");
                writeAliasBody(builder, name, resolved.alias().writeIndex(), index);
                builder.endObject();
                builder.endObject();
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /** {@code GET /{index}/_alias}: every alias that names the index, verified against each alias record. */
    static void aliasesOf(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String index, String[] wantedAliases)
        throws IOException {
        final var descriptor = metadata.describe(index);
        if (descriptor.isEmpty()) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index));
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startObject(index);
            builder.startObject("aliases");
            for (String alias : metadata.aliasesOf(descriptor.get())) {
                if (wantedAliases != null
                    && wantedAliases.length != 0
                    && org.opensearch.common.regex.Regex.simpleMatch(wantedAliases, alias) == false) {
                    continue;
                }
                final var record = metadata.aliasWithGeneration(alias);
                if (record.isEmpty() || record.get().alias().dataStream() || record.get().alias().indices().contains(index) == false) {
                    continue;
                }
                writeAliasBody(builder, alias, record.get().alias().writeIndex(), index);
            }
            builder.endObject();
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Writes one alias body the way core writes it.
     *
     * <p>This surface used to emit {@code {}} for every alias, which is right for an alias carrying
     * nothing and silently wrong for one with an explicit write index: real OpenSearch reports
     * {@code is_write_index} there, and a client that reads it to decide where to send a write saw
     * nothing to read. Handing the job to {@link AliasMetadata.Builder#toXContent} fixes that and, more
     * to the point, stops this shape being a thing someone has to keep in step by hand -- filter,
     * routing and hidden come along the moment this shell has anything to put in them.
     *
     * @param builder the builder, positioned inside the {@code aliases} object
     * @param alias the alias name
     * @param writeIndex the index this alias writes to, or null when it names no particular one
     * @param index the index this body is being written under
     */
    static void writeAliasBody(XContentBuilder builder, String alias, String writeIndex, String index) throws IOException {
        final AliasMetadata.Builder metadata = AliasMetadata.builder(alias);
        if (writeIndex != null) {
            metadata.writeIndex(writeIndex.equals(index));
        }
        AliasMetadata.Builder.toXContent(metadata.build(), builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
    }
}
