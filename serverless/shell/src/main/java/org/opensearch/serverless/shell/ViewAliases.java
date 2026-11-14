/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.util.Collection;

/**
 * The aliases of the indices in this node's local view, added to each index's metadata as core's cluster state carries them.
 *
 * <p>A descriptor does not hold its aliases, so the projected view had none for the indices a node serves. Plugins read them
 * there: index management checks, before rolling an index over, that the rollover alias names the index and marks it as the
 * write index. Each alias an index's descriptor lists is read through the routing cache and added when it still names the
 * index, with its {@code is_write_index} flag. An index with no alias costs nothing, and a plugin index's aliases are added by
 * the plugin indices view instead.
 */
final class ViewAliases {

    private static final Logger logger = LogManager.getLogger(ViewAliases.class);

    private ViewAliases() {}

    /**
     * The view with the aliases of the given indices added.
     *
     * @param state the projected view
     * @param descriptors the indices projected into it
     * @param plane the metadata plane, or null when the node has none yet
     * @param node the node, to leave its plugin indices to their own view
     * @return the view with the aliases
     */
    static ClusterState with(ClusterState state, Collection<IndexDescriptor> descriptors, MetadataPlane plane, ServerlessNode node) {
        if (plane == null) {
            return state;
        }
        Metadata.Builder metadata = null;
        for (IndexDescriptor descriptor : descriptors) {
            if (descriptor.aliasedBy().isEmpty() || node.isSystemIndex(descriptor.name())) {
                continue;
            }
            final IndexMetadata index = state.metadata().index(descriptor.name());
            if (index == null) {
                continue;
            }
            final IndexMetadata withAliases = of(index, descriptor, plane);
            if (withAliases == index) {
                continue;
            }
            if (metadata == null) {
                metadata = Metadata.builder(state.metadata());
            }
            metadata.put(withAliases, false);
        }
        if (metadata == null) {
            return state;
        }
        try {
            return ClusterState.builder(state).metadata(metadata).build();
        } catch (RuntimeException e) {
            // An alias that clashes with an index name in the view: the indices without their aliases rather than no view.
            logger.debug("not adding the indices' aliases to the local view", e);
            return state;
        }
    }

    /**
     * One index's metadata with the aliases its descriptor lists that still name it.
     *
     * @param index the index's metadata
     * @param descriptor its descriptor
     * @param plane the metadata plane
     * @return the metadata with its aliases, or the same instance when there are none to add
     */
    static IndexMetadata of(IndexMetadata index, IndexDescriptor descriptor, MetadataPlane plane) {
        IndexMetadata.Builder withAliases = null;
        for (String name : descriptor.aliasedBy()) {
            if (index.getAliases().containsKey(name)) {
                continue;
            }
            final AliasRecord alias;
            try {
                alias = plane.resolveForRouting(name).alias();
            } catch (Exception e) {
                logger.debug("could not read alias [" + name + "] for index [" + descriptor.name() + "]", e);
                continue;
            }
            if (alias == null || alias.dataStream() || alias.indices().contains(descriptor.name()) == false) {
                continue;
            }
            if (withAliases == null) {
                withAliases = IndexMetadata.builder(index);
            }
            withAliases.putAlias(PluginIndexAdmin.aliasMetadata(alias, descriptor.name()));
        }
        return withAliases == null ? index : withAliases.build();
    }
}
