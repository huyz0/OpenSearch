/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.recovery.RecoveryState;

/**
 * Verifies core's {@code IN_PLACE_SPLIT_SHARD} recovery-source dispatch seam: a child shard routed
 * with {@link RecoverySource.InPlaceSplitShardRecoverySource} shares {@link IndexShard#startRecovery}'s
 * {@code EMPTY_STORE}/{@code EXISTING_STORE} case (both dispatch to {@link IndexShard#recoverFromStore}),
 * closing the gap where it previously had no handling at all and would have failed recovery with
 * "Unknown recovery source" via that method's {@code default:} case. See {@code
 * ShardRecoveryStrategy#recoverLocalStore}'s {@code IN_PLACE_SPLIT_SHARD} case for where a plugin
 * strategy actually attaches this shard to a split parent's data -- covered by that plugin's own
 * tests, not here, since this test's own strategy (core's {@code local-lucene}) has nothing to attach
 * to and is only verifying dispatch doesn't throw.
 */
public class InPlaceSplitShardRecoveryTests extends IndexShardTestCase {

    public void testChildShardRecoversFromInPlaceSplitRecoverySource() throws Exception {
        ShardId shardId = new ShardId("in-place-split-child-index", "_na_", 0);
        ShardRouting initializingRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        ).initialize(randomAlphaOfLength(5), null, -1);

        IndexShard shard = newShard(initializingRouting);
        try {
            shard.markAsRecovering(
                "in-place split",
                new RecoveryState(shard.routingEntry(), IndexShardTestUtils.getFakeDiscoNode(shard.routingEntry().currentNodeId()), null)
            );

            PlainActionFuture<Boolean> future = new PlainActionFuture<>();
            shard.recoverFromStore(future);
            assertTrue("recovery from an in-place split recovery source must succeed with core's local-lucene strategy", future.get());

            updateRoutingEntry(shard, ShardRoutingHelper.moveToStarted(shard.routingEntry()));
            assertEquals(IndexShardState.STARTED, shard.state());
        } finally {
            closeShards(shard);
        }
    }
}
