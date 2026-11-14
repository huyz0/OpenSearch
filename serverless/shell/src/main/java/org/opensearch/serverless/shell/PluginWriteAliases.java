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
import org.opensearch.serverless.metadata.AliasAtGeneration;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.io.IOException;
import java.util.Optional;

/**
 * Writes the plugin client is given addressed to an alias, sent to the index the alias writes to, as core sends them.
 *
 * <p>Plugins keep their history in a dated index behind an alias and write through the alias: the anomaly-detection plugin
 * writes every result to {@code .opendistro-anomaly-results}, which names the current history index. The REST write path
 * resolves an alias through the metadata plane's write target; the plugin client routes by index name, so it resolves the
 * alias the same way first: the alias's {@code is_write_index} member, or its only index. An alias naming several indices and
 * marking none is left as it is, and the write is refused as in core. A name that is an index costs nothing more here.
 */
final class PluginWriteAliases {

    private PluginWriteAliases() {}

    /** Rewrites the index of every write in the request that names an alias. */
    static void resolve(ServerlessNode serving, MetadataPlane plane, ActionRequest request) throws IOException {
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
        if (name == null || plane.describeForRouting(name).isPresent()) {
            return;
        }
        final Optional<AliasAtGeneration> alias = plane.aliasWithGeneration(name);
        if (alias.isEmpty() || alias.get().alias().dataStream()) {
            return;
        }
        final String target = alias.get().alias().writesTo();
        if (target != null) {
            write.index(target);
        }
    }
}
