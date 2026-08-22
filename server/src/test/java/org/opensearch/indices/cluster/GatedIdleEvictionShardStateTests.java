/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.cluster;

import org.opensearch.test.OpenSearchTestCase;

/**
 * What a shard that cannot answer "how long have you been idle" must say.
 *
 * <h2>The bug this pins</h2>
 *
 * {@code IndexShard.idleMillis} combines two figures: time since the last search and time since the last
 * write. The first version, when the shard had no indexer -- recovering, or already closing -- fell back to
 * the search figure alone, reasoning that half an answer beat none.
 *
 * <p>That is not half an answer, it is the opposite one. {@code lastSearcherAccess} is zero until something
 * searches the shard, so a shard nobody had searched reported the node's entire uptime as its idle time.
 * A recovering shard therefore looked colder than anything else on the node and would be evicted before it
 * had finished opening -- and the eviction would then race the on-demand opener rebuilding it.
 *
 * <p>The integration test did not catch it. Its shards are written to before the sweep first runs, so they
 * always have an indexer by the time anything asks. The case only appears when a sweep lands inside the
 * window where a shard exists and its engine does not, which is timing this suite cannot arrange and
 * production reaches routinely.
 *
 * <h2>Why the default lives here too</h2>
 *
 * {@link IndicesClusterStateService.Shard#idleMillis} defaults to zero for the same reason, and the two have
 * to agree: an implementation that cannot answer must not be read as idle, because the caller's response to
 * "idle" is to close the shard. Zero is the answer that costs nothing when wrong.
 */
public class GatedIdleEvictionShardStateTests extends OpenSearchTestCase {

    /** A shard that answers the interface default, standing in for one that cannot measure itself. */
    private static final class ShardThatCannotSayHowIdleItIs implements IndicesClusterStateService.Shard {
        @Override
        public org.opensearch.core.index.shard.ShardId shardId() {
            throw new UnsupportedOperationException("not needed by this test");
        }

        @Override
        public org.opensearch.cluster.routing.ShardRouting routingEntry() {
            throw new UnsupportedOperationException("not needed by this test");
        }

        @Override
        public org.opensearch.index.shard.IndexShardState state() {
            throw new UnsupportedOperationException("not needed by this test");
        }

        @Override
        public org.opensearch.indices.recovery.RecoveryState recoveryState() {
            throw new UnsupportedOperationException("not needed by this test");
        }

        @Override
        public void updateShardState(
            org.opensearch.cluster.routing.ShardRouting shardRouting,
            long primaryTerm,
            java.util.function.BiConsumer<
                org.opensearch.index.shard.IndexShard,
                org.opensearch.core.action.ActionListener<org.opensearch.index.shard.PrimaryReplicaSyncer.ResyncTask>> primaryReplicaSyncer,
            long applyingClusterStateVersion,
            java.util.Set<String> inSyncAllocationIds,
            org.opensearch.cluster.routing.IndexShardRoutingTable routingTable,
            org.opensearch.cluster.node.DiscoveryNodes discoveryNodes
        ) {
            throw new UnsupportedOperationException("not needed by this test");
        }
    }

    public void testAShardThatCannotAnswerIsNeverIdle() {
        assertEquals(
            "a shard that cannot measure its own idleness must report zero, because the caller's response "
                + "to a large number here is to close it",
            0L,
            new ShardThatCannotSayHowIdleItIs().idleMillis()
        );
    }
}
