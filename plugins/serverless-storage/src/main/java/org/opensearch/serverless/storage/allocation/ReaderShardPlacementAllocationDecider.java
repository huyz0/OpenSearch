/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;

/**
 * Enforces the asymmetric writer/reader node-role split for serverless-storage indices
 * (rfc-serverless-opensearch.md &sect;10: "a new decider forbids writer shards on search-compute
 * nodes and vice versa"), in both directions:
 *
 * <ul>
 *   <li>a reader (search-only) shard copy may only land on a node explicitly marked as willing to
 *       serve that role, {@code node.attr.serverless_storage_reader: "true"} -- ordinary node
 *       attribute matching, the same mechanism {@code cluster.routing.allocation.require.*}
 *       filters already use, just scoped automatically to this one shard role instead of
 *       requiring an operator to hand-write a filter for every serverless index;
 *   <li>a writer shard (or any other, non-reader copy) may <em>not</em> land on a node marked
 *       that way -- reader-designated capacity is reserved for readers, so an ordinary
 *       writer/primary shard never silently consumes it. This is the "vice versa" half: unlike
 *       readers, writer shards have no opt-in attribute of their own and remain allowed on any
 *       node that isn't reader-designated, matching how writer placement works today outside
 *       serverless storage.
 * </ul>
 *
 * <p>Every shard of every index that hasn't opted into serverless storage
 * (&sect;index.serverless_storage.enabled) is entirely unaffected: this decider only ever returns
 * {@code YES} (no opinion) for those, consistent with how every other {@link AllocationDecider} in
 * the allocator is expected to behave for shards it has nothing to say about.
 */
public class ReaderShardPlacementAllocationDecider extends AllocationDecider {

    /** Creates a decider with no state; every decision is derived from cluster/routing state passed to it per call. */
    public ReaderShardPlacementAllocationDecider() {}

    /** The decider name this class is registered under. */
    public static final String NAME = "serverless_storage_reader_placement";

    /** A node carrying this attribute set to {@code "true"} may host reader shard copies -- and only reader shard copies. */
    public static final String READER_NODE_ATTRIBUTE = "serverless_storage_reader";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decide(shardRouting, node, allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decide(shardRouting, node, allocation);
    }

    private Decision decide(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        boolean isServerlessStorageIndex = ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexMetadata.getSettings());
        if (isServerlessStorageIndex == false) {
            return allocation.decision(Decision.YES, NAME, "index has not opted into serverless storage, no opinion");
        }

        boolean nodeIsReaderDesignated = "true".equals(node.node().getAttributes().get(READER_NODE_ATTRIBUTE));

        if (shardRouting.isSearchOnly()) {
            if (nodeIsReaderDesignated) {
                return allocation.decision(
                    Decision.YES,
                    NAME,
                    "node is marked with [node.attr.%s: true], may host reader shards",
                    READER_NODE_ATTRIBUTE
                );
            }
            return allocation.decision(
                Decision.NO,
                NAME,
                "reader shards of a serverless-storage index may only be allocated to nodes marked with [node.attr.%s: true]",
                READER_NODE_ATTRIBUTE
            );
        }

        // A writer shard (or any other non-reader copy) of a serverless-storage index: forbidden
        // only from landing on a reader-designated node, so that capacity stays reserved for
        // readers -- unrestricted everywhere else, since writer shards have no opt-in attribute of
        // their own to check.
        if (nodeIsReaderDesignated) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "writer shards of a serverless-storage index may not be allocated to nodes marked with [node.attr.%s: true], "
                    + "which are reserved for reader shards",
                READER_NODE_ATTRIBUTE
            );
        }
        return allocation.decision(Decision.YES, NAME, "node is not reader-designated, no capacity conflict");
    }
}
