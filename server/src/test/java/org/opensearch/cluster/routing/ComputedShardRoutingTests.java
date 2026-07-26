/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

/**
 * C18a. Whether a computed shard keeps its identity across cluster states.
 *
 * <p>Placement being computed is only half of it. {@code IndexShard.updateShardState} throws when the
 * routing entry it is handed is not the same allocation as the one it holds, and {@code removeShards}
 * tears down a shard whose allocation id has changed. An entry rebuilt per cluster state with a fresh
 * allocation id would destroy and recreate the shard on every update, which is a failure mode that looks
 * like flapping rather than like a bug in identity.
 */
public class ComputedShardRoutingTests extends OpenSearchTestCase {

    private static final Index INDEX = new Index("computed-idx", "uuid-0000000000000000");

    /** The load-bearing property: two independent computations agree. */
    public void testAllocationIdIsAFunctionOfItsInputs() {
        assertEquals(
            ComputedShardRouting.allocationId(INDEX.getUUID(), 3, "node-1"),
            ComputedShardRouting.allocationId(INDEX.getUUID(), 3, "node-1")
        );
    }

    /** And distinguishes what has to be distinguished, or two shards share one identity. */
    public void testAllocationIdSeparatesShardsNodesAndIndices() {
        String base = ComputedShardRouting.allocationId(INDEX.getUUID(), 3, "node-1");

        assertNotEquals(base, ComputedShardRouting.allocationId(INDEX.getUUID(), 4, "node-1"));
        assertNotEquals(base, ComputedShardRouting.allocationId(INDEX.getUUID(), 3, "node-2"));
        assertNotEquals(base, ComputedShardRouting.allocationId("other-uuid", 3, "node-1"));
    }

    /**
     * Pinned, for the same reason C1 pins the placement hash. Two nodes disagreeing about a shard's
     * identity is a split view of the cluster rather than a performance problem, so a change to this
     * value has to be a deliberate one.
     */
    public void testAllocationIdIsPinned() {
        assertEquals("dXVpZC0wMDAwMDAwMDAwMDAwMDAwLzMvbm9kZS0x", ComputedShardRouting.allocationId(INDEX.getUUID(), 3, "node-1"));
    }

    /** The two views are one shard. Same allocation, differing only in state. */
    public void testTheLocalAndCoordinatorViewsAreTheSameAllocation() {
        ShardId shardId = new ShardId(INDEX, 2);
        RecoverySource source = RecoverySource.ExistingStoreRecoverySource.INSTANCE;

        ShardRouting local = ComputedShardRouting.initializing(shardId, "node-1", source);
        ShardRouting coordinator = ComputedShardRouting.started(shardId, "node-1", source);

        assertTrue(
            "the node's view and the coordinator's view must be the same allocation, or updateShardState throws",
            local.isSameAllocation(coordinator)
        );
        assertTrue(local.initializing());
        assertTrue(coordinator.started());
        assertEquals(local.currentNodeId(), coordinator.currentNodeId());
    }

    /** Rebuilding is what actually happens on every applied cluster state. */
    public void testRebuildingProducesTheSameAllocation() {
        ShardId shardId = new ShardId(INDEX, 0);
        RecoverySource source = RecoverySource.ExistingStoreRecoverySource.INSTANCE;

        ShardRouting first = ComputedShardRouting.initializing(shardId, "node-1", source);
        ShardRouting second = ComputedShardRouting.initializing(shardId, "node-1", source);

        assertTrue("a rebuilt entry must not read as a different allocation", first.isSameAllocation(second));
    }

    /** The recovery source is stated by the caller and carried through, never derived. */
    public void testRecoverySourceIsCarriedThroughUnchanged() {
        ShardId shardId = new ShardId(INDEX, 0);

        ShardRouting empty = ComputedShardRouting.initializing(shardId, "node-1", RecoverySource.EmptyStoreRecoverySource.INSTANCE);
        ShardRouting existing = ComputedShardRouting.initializing(shardId, "node-1", RecoverySource.ExistingStoreRecoverySource.INSTANCE);

        assertEquals(RecoverySource.Type.EMPTY_STORE, empty.recoverySource().getType());
        assertEquals(RecoverySource.Type.EXISTING_STORE, existing.recoverySource().getType());
    }
}
