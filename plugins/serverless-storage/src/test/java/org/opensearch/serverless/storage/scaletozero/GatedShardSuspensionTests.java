/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.ComputedShardRouting;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;
import org.junit.After;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * H9d. Scale-to-zero for an index that cluster state does not hold.
 *
 * <p>H9a measured the gap and H9c built the mechanism. This connects them, and the connection is where the
 * value is: H9c proved that placement omits a suspended shard, but nothing recorded a suspension, so the
 * mechanism was correct and dead. That is the same shape as H7a, where the mapping store existed with no
 * caller and a test calling it directly passed with the wiring disabled.
 *
 * <p>The property that makes this affordable rather than merely working: a gated suspension submits no
 * cluster state task at all. Scale-to-zero suspends shards continuously, so at the index counts this area
 * exists for, one publication per suspension would leave the cluster manager doing nothing else. The
 * assertion is on the absence of the submission, because a design that happens to work while paying a
 * publication per suspension does not survive contact with a hundred million indices.
 */
public class GatedShardSuspensionTests extends OpenSearchTestCase {

    private static final String GATED_UUID = "gated-uuid";
    private static final int SHARDS = 3;

    private final GatedShardSuspensionRegistry registry = new GatedShardSuspensionRegistry();

    @After
    public void clearRegistrations() {
        registry.uninstall();
        AbsentIndexRoutingSuppliers.register(null);
    }

    /**
     * The gap H9a pinned, now closed. A gated shard suspends, and it does so without a publication.
     */
    public void testAGatedShardSuspendsWithoutACLusterStateUpdate() {
        ClusterService clusterService = clusterServiceFor(ClusterState.builder(ClusterName.DEFAULT).build());
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterService, mock(Client.class), 0L, false, registry);

        coordinator.suspendWriterShard(GATED_UUID, 1);

        assertTrue("the shard must be recorded as asleep", registry.isSuspended(GATED_UUID, 1));
        verify(
            clusterService,
            never().description(
                "a gated suspension must not submit a cluster state task: at a hundred million indices, one "
                    + "publication per suspension is the whole cluster manager"
            )
        ).submitStateUpdateTask(anyString(), any(), any(), any(), any());
    }

    /**
     * And the recorded suspension actually reaches placement, which is the half that makes it a suspension
     * rather than a note in a map nobody reads.
     */
    public void testASuspendedGatedShardIsNoLongerPlaced() {
        registry.install();
        AbsentIndexRoutingSuppliers.register((state, metadata) -> placement());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        ClusterService clusterService = clusterServiceFor(state);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterService, mock(Client.class), 0L, false, registry);

        assertNotNull("the premise: the shard is placed before it sleeps", AbsentIndexRoutingSuppliers.resolve(state, "gated").shard(1));

        coordinator.suspendWriterShard(GATED_UUID, 1);

        IndexRoutingTable afterSuspension = AbsentIndexRoutingSuppliers.resolve(state, "gated");
        assertNull("the suspended shard must no longer be placed", afterSuspension.shard(1));
        assertNotNull("and its siblings must be untouched", afterSuspension.shard(0));
    }

    /**
     * A repeat tick must be a no-op. Suspension is attempted once per candidate shard on every tick, so
     * in steady state nearly every call is a repeat, and one that reported a change would re-evict an
     * already-evicted shard forever.
     */
    public void testASecondSuspensionOfTheSameShardReportsNoChange() {
        assertTrue("the first suspension changes something", registry.suspend(GATED_UUID, 0));
        assertFalse("the second must not", registry.suspend(GATED_UUID, 0));
    }

    /**
     * Waking removes the index's entry once nothing is asleep. A map holding an entry per index that has
     * ever slept is the residency ceiling this area exists to remove, rebuilt in another data structure.
     */
    public void testAnIndexWithNothingAsleepIsNotTracked() {
        registry.suspend(GATED_UUID, 0);
        registry.suspend(GATED_UUID, 1);
        assertEquals(1, registry.trackedIndexCount());

        assertTrue(registry.reactivate(GATED_UUID, 0));
        assertEquals("one shard still asleep, so the index is still tracked", 1, registry.trackedIndexCount());

        assertTrue(registry.reactivate(GATED_UUID, 1));
        assertEquals("with nothing asleep the entry must be gone, not left empty", 0, registry.trackedIndexCount());
        assertFalse("and waking an awake shard must report no change", registry.reactivate(GATED_UUID, 1));
    }

    /**
     * The control. An ordinary index still goes through cluster state, so the silence above is about the
     * gate rather than about suspension having been disabled for everything.
     */
    public void testAnOrdinaryIndexStillPublishes() {
        ClusterState state = ShardSuspensionCoordinatorTests.stateForBatching(1);
        ClusterService clusterService = clusterServiceFor(state);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterService, mock(Client.class), 0L, false, registry);

        coordinator.suspendWriterShard(ShardSuspensionCoordinatorTests.indexUuid(state), 0);

        verify(clusterService).submitStateUpdateTask(anyString(), any(), any(), any(), any());
        assertEquals("an ordinary index must not be recorded here at all", 0, registry.trackedIndexCount());
    }

    /**
     * With no registry supplied the old behaviour is unchanged, so H9d cannot break a deployment that has
     * not opted into it.
     */
    public void testWithoutARegistryTheGatedPathIsUnchanged() {
        ClusterService clusterService = clusterServiceFor(ClusterState.builder(ClusterName.DEFAULT).build());
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterService, mock(Client.class), 0L);

        coordinator.suspendWriterShard(GATED_UUID, 0);

        verify(clusterService).submitStateUpdateTask(anyString(), any(), any(), any(), any());
    }

    // ---------------------------------------------------------------- helpers

    private static ClusterService clusterServiceFor(ClusterState state) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    private static IndexRoutingTable placement() {
        Index index = new Index("gated", GATED_UUID);
        IndexRoutingTable.Builder placement = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < SHARDS; shard++) {
            ShardId shardId = new ShardId(index, shard);
            placement.addIndexShard(
                new IndexShardRoutingTable.Builder(shardId).addShard(
                    ComputedShardRouting.started(shardId, "node-1", RecoverySource.EmptyStoreRecoverySource.INSTANCE)
                ).build()
            );
        }
        return placement.build();
    }
}
