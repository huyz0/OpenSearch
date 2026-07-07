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
 * Restricts placement of reader (search-only) shard copies belonging to a serverless-storage
 * index to nodes explicitly marked as willing to serve that role (rfc-serverless-opensearch.md
 * &sect;10: asymmetric writer/reader tiers that scale independently). A node opts in with
 * {@code node.attr.serverless_storage_reader: "true"} -- ordinary node attribute matching, the
 * same mechanism {@code cluster.routing.allocation.require.*} filters already use, just scoped
 * automatically to this one shard role instead of requiring an operator to hand-write a filter
 * for every serverless index.
 *
 * <p>Writer shards, and every shard of every index that hasn't opted into serverless storage
 * (&sect;index.serverless_storage.enabled}), are entirely unaffected: this decider only ever
 * returns a {@code NO} for the one specific (serverless index, reader shard, non-opted-in node)
 * combination, {@code YES} (no opinion) for everything else -- consistent with how every other
 * {@link AllocationDecider} in the allocator is expected to behave.
 */
public class ReaderShardPlacementAllocationDecider extends AllocationDecider {

    public static final String NAME = "serverless_storage_reader_placement";

    /** A node carrying this attribute set to {@code "true"} may host reader shard copies. */
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
        if (shardRouting.isSearchOnly() == false) {
            return allocation.decision(Decision.YES, NAME, "not a reader shard, no opinion");
        }

        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        boolean isServerlessStorageIndex = ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexMetadata.getSettings());
        if (isServerlessStorageIndex == false) {
            return allocation.decision(Decision.YES, NAME, "index has not opted into serverless storage, no opinion");
        }

        String attributeValue = node.node().getAttributes().get(READER_NODE_ATTRIBUTE);
        if ("true".equals(attributeValue)) {
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
}
