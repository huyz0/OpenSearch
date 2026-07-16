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
 * Verifies core's {@code IN_PLACE_MERGE_SHARD} recovery-source dispatch seam -- the reverse of
 * {@link InPlaceSplitShardRecoveryTests}. A parent shard revived by an in-place merge and routed
 * with {@link RecoverySource.InPlaceMergeShardRecoverySource} shares {@link IndexShard#startRecovery}'s
 * {@code EMPTY_STORE}/{@code EXISTING_STORE}/{@code IN_PLACE_SPLIT_SHARD} case (all dispatch to
 * {@link IndexShard#recoverFromStore}), so the merge recovery source does not fall into that
 * method's {@code default:} "Unknown recovery source" case.
 *
 * <p>See {@code org.opensearch.index.engine.EngineFactory#recoverInPlaceMergeLocalStore} for where a
 * plugin engine would revive the parent from its children's data -- deliberately a no-op in every
 * engine today (the data-reconciliation design problem is unresolved, see the progress doc), so this
 * test only verifies that dispatch does not throw and the shard reaches STARTED.
 */
public class InPlaceMergeShardRecoveryTests extends IndexShardTestCase {

    public void testParentShardRecoversFromInPlaceMergeRecoverySource() throws Exception {
        ShardId shardId = new ShardId("in-place-merge-parent-index", "_na_", 0);
        ShardRouting initializingRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.InPlaceMergeShardRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        ).initialize(randomAlphaOfLength(5), null, -1);

        IndexShard shard = newShard(initializingRouting);
        try {
            shard.markAsRecovering(
                "in-place merge",
                new RecoveryState(shard.routingEntry(), IndexShardTestUtils.getFakeDiscoNode(shard.routingEntry().currentNodeId()), null)
            );

            PlainActionFuture<Boolean> future = new PlainActionFuture<>();
            shard.recoverFromStore(future);
            assertTrue("recovery from an in-place merge recovery source must succeed with core's no-op default engine hook", future.get());

            updateRoutingEntry(shard, ShardRoutingHelper.moveToStarted(shard.routingEntry()));
            assertEquals(IndexShardState.STARTED, shard.state());
        } finally {
            closeShards(shard);
        }
    }
}
