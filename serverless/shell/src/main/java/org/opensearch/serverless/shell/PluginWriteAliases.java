/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.metadata.AliasAtGeneration;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.io.IOException;
import java.util.Optional;

/**
 * Writes a plugin addresses to an alias of its own indices, sent to the index the alias writes to, as core sends them.
 *
 * <p>Plugins keep their history in a dated index behind an alias and write through the alias: the anomaly-detection plugin
 * writes every result to {@code .opendistro-anomaly-results}, which names the current history index. Core writes through an
 * alias that names one index, or through the index an alias marks as its write index; the shell's write path writes to an
 * index or a data stream only. For a plugin's own aliases -- names matching a plugin's declared system-index patterns, never a
 * tenant's -- the plugin client resolves the alias the way core does before the write is routed. An alias naming several
 * indices and no write index is left as it is, and the write is refused as before.
 */
final class PluginWriteAliases {

    private PluginWriteAliases() {}

    /** Rewrites the index of every write in the request that names a plugin alias. */
    static void resolve(ServerlessNode serving, MetadataPlane plane, ActionRequest request) throws IOException {
        if (serving.settings.getAsBoolean(PluginIndicesView.ENABLED, true) == false) {
            return;
        }
        if (request instanceof BulkRequest bulk) {
            for (DocWriteRequest<?> item : bulk.requests()) {
                resolveOne(serving, plane, item);
            }
        } else if (request instanceof DocWriteRequest<?> write) {
            resolveOne(serving, plane, write);
        }
    }

    private static void resolveOne(ServerlessNode serving, MetadataPlane plane, DocWriteRequest<?> write) throws IOException {
        final String name = write.index();
        if (name == null || serving.isSystemIndex(name) == false || plane.describeForRouting(name).isPresent()) {
            return;
        }
        final Optional<AliasAtGeneration> alias = plane.aliasWithGeneration(name);
        if (alias.isEmpty() || alias.get().alias().dataStream()) {
            return;
        }
        final AliasRecord record = alias.get().alias();
        final String target = record.writeIndex() != null ? record.writeIndex()
            : record.indices().size() == 1 ? record.indices().get(0)
            : null;
        if (target != null) {
            write.index(target);
        }
    }
}
