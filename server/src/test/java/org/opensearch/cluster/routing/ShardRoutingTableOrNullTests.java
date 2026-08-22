/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.test.OpenSearchTestCase;

/**
 * A3. {@link RoutingTable#shardRoutingTable(ShardId)} throws for a missing index or shard, which is
 * right for most of its twenty-odd callers and wrong for the two on the write path:
 * {@code TransportReplicationAction.ReroutePhase} and {@code TransportBulkAction} both already have a
 * "primary is not allocated yet, wait and retry" branch one statement below the lookup, and the
 * throwing lookup meant they failed the request before ever reaching it.
 *
 * <p>Rather than change a contract twenty callers depend on, {@link
 * RoutingTable#shardRoutingTableOrNull(ShardId)} is the null-returning sibling and those two callers
 * use it. These tests pin the sibling's behaviour and, importantly, that the original is unchanged.
 */
public class ShardRoutingTableOrNullTests extends OpenSearchTestCase {

    private static final String INDEX = "idx";

    public void testReturnsNullForAnIndexWithNoRoutingEntry() {
        RoutingTable empty = RoutingTable.builder().build();
        ShardId shardId = new ShardId(new Index(INDEX, "idx-uuid"), 0);

        assertNull(empty.shardRoutingTableOrNull(shardId));
    }

    /**
     * An unknown shard of an index that is in the routing table is a permanent error and must stay
     * one. Returning null here would push it into the caller's retry branch, which would wait out the
     * whole timeout and then report the wrong error -- a regression an existing
     * {@code TransportReplicationActionTests} case caught when this method was first written the
     * broad way.
     */
    public void testAnUnknownShardOfAKnownIndexStillThrows() {
        RoutingTable routingTable = routingTableWith(2);
        Index index = indexMetadata(2).getIndex();

        assertNotNull("shard 1 exists", routingTable.shardRoutingTableOrNull(new ShardId(index, 1)));
        expectThrows(ShardNotFoundException.class, () -> routingTable.shardRoutingTableOrNull(new ShardId(index, 5)));
    }

    /**
     * A stale {@link ShardId} carrying a different index UUID names a different index, one that was
     * deleted and recreated. That is not a cold index and must keep its existing error.
     */
    public void testAMatchingNameWithADifferentUuidStillThrows() {
        RoutingTable routingTable = routingTableWith(1);

        expectThrows(
            IndexNotFoundException.class,
            () -> routingTable.shardRoutingTableOrNull(new ShardId(new Index(INDEX, "a-different-uuid"), 0))
        );
    }

    /** The original contract is untouched, which is the whole reason for adding a sibling. */
    public void testTheThrowingVariantStillThrows() {
        RoutingTable empty = RoutingTable.builder().build();
        RoutingTable oneShard = routingTableWith(1);
        Index index = indexMetadata(1).getIndex();

        expectThrows(IndexNotFoundException.class, () -> empty.shardRoutingTable(new ShardId(index, 0)));
        expectThrows(ShardNotFoundException.class, () -> oneShard.shardRoutingTable(new ShardId(index, 7)));
    }

    private static RoutingTable routingTableWith(int shards) {
        return RoutingTable.builder().addAsNew(indexMetadata(shards)).build();
    }

    private static IndexMetadata indexMetadata(int shards) {
        return IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "idx-uuid")
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }
}
