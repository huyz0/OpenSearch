/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The reactivation half of cold-absence: {@code ShardSuspensionCoordinator} removes a fully cold
 * index's routing entry, so reactivating one has to put it back, and in the same cluster-state update
 * that clears the suspended marker.
 *
 * <p>The atomicity is the point rather than an implementation detail.
 * {@code ShardReactivationActionFilter} reads absent-routing-with-present-metadata as "not started
 * yet" and holds the search on it; if the marker cleared in one update and the entry appeared in a
 * later one, an index with no search-only replicas would short-circuit that check in the gap and
 * proceed against an index with no shards.
 */
public class TransportReactivateShardsActionTests extends OpenSearchTestCase {

    private static final String INDEX = "cold-idx";

    public void testReactivationRecreatesTheRoutingEntryAndClearsTheMarkerInOneState() {
        ClusterState cold = coldState();
        assertFalse("precondition: the cold index has no routing entry", cold.routingTable().hasIndex(INDEX));
        assertFalse(
            "precondition: and its shard is marked suspended",
            SuspendedShardsMetadata.suspendedShardIds(cold.metadata().index(INDEX)).isEmpty()
        );

        ClusterState reactivated = TransportReactivateShardsAction.reactivate(cold, INDEX, false, 1_000L);

        assertTrue("the routing entry must come back", reactivated.routingTable().hasIndex(INDEX));
        assertTrue(
            "...in the same state that clears the marker, not a later one",
            SuspendedShardsMetadata.suspendedShardIds(reactivated.metadata().index(INDEX)).isEmpty()
        );
    }

    /**
     * The recreated shards must recover from their existing store rather than be treated as new --
     * the data is in the object store, and {@code ServerlessStorageExistingShardsAllocator} is
     * written against {@code ExistingStoreRecoverySource}. {@code addAsNew} would silently discard
     * the index's contents.
     */
    public void testRecreatedShardsRecoverFromTheExistingStore() {
        ClusterState reactivated = TransportReactivateShardsAction.reactivate(coldState(), INDEX, false, 1_000L);

        ShardRouting primary = reactivated.routingTable().index(INDEX).shard(0).primaryShard();
        assertEquals(RecoverySource.Type.EXISTING_STORE, primary.recoverySource().getType());
    }

    /**
     * An entry pruned while pruning was enabled must still come back if the setting is turned off
     * afterwards, so recreation cannot be gated on it. The marker-already-clear case is what a
     * disabled cluster looks like on the second reactivation attempt, and it must still repair the
     * routing table rather than short-circuit as a no-op.
     */
    public void testRecreationIsNotGatedOnTheMarkerStillBeingSet() {
        ClusterState cold = coldState();
        IndexMetadata markerCleared = SuspendedShardsMetadata.withAllShardsReactivated(cold.metadata().index(INDEX), 500L);
        ClusterState strandedState = ClusterState.builder(cold)
            .metadata(Metadata.builder(cold.metadata()).put(markerCleared, true))
            .build();

        ClusterState repaired = TransportReactivateShardsAction.reactivate(strandedState, INDEX, false, 1_000L);

        assertTrue("a cleared marker with a missing entry must still be repaired", repaired.routingTable().hasIndex(INDEX));
    }

    /** A fully normal index must be left byte-identical -- reactivation of nothing changes nothing. */
    public void testAnActiveIndexIsUntouched() {
        ClusterState cold = coldState();
        ClusterState active = TransportReactivateShardsAction.reactivate(cold, INDEX, false, 1_000L);

        assertSame(
            "a second reactivation has nothing left to do",
            active,
            TransportReactivateShardsAction.reactivate(active, INDEX, false, 2_000L)
        );
    }

    /**
     * Finding L-3. Reactivation used to be unavoidably index-wide, so on a 100-shard index with one
     * continuously hot shard and 99 cold ones, the very next request to the hot shard woke all 99 --
     * which then went idle, were suspended and evicted, and were woken again on the next request.
     * Forever, roughly every {@code idle_threshold + cooldown}. The index could never scale to zero
     * and paid continuous recover/evict churn for nothing.
     */
    public void testAShardScopedReactivationLeavesTheOtherShardsSuspended() {
        ClusterState cold = multiShardColdState(4);
        ClusterState reactivated = TransportReactivateShardsAction.reactivate(cold, MULTI_INDEX, false, java.util.Set.of(2), 1_000L);

        java.util.Set<Integer> stillSuspended = SuspendedShardsMetadata.suspendedShardIds(reactivated.metadata().index(MULTI_INDEX));
        assertEquals(
            "only the shard the caller actually needs may be woken; the rest must stay asleep",
            java.util.Set.of(0, 1, 3),
            stillSuspended
        );
        assertFalse("...and the requested shard must genuinely be awake", stillSuspended.contains(2));
    }

    public void testAnEmptyShardSetStillMeansTheWholeIndex() {
        ClusterState cold = multiShardColdState(4);
        ClusterState reactivated = TransportReactivateShardsAction.reactivate(cold, MULTI_INDEX, false, java.util.Set.of(), 1_000L);
        assertTrue(
            "a search genuinely needs every shard, so the whole-index scope must still exist and still work",
            SuspendedShardsMetadata.suspendedShardIds(reactivated.metadata().index(MULTI_INDEX)).isEmpty()
        );
    }

    public void testAShardScopedReactivationOfAnAlreadyAwakeShardPublishesNothing() {
        // The routing entry exists and shard 2 is not suspended, so there is nothing to do. Returning
        // the same reference is what stops MasterService publishing a no-op cluster state -- and this
        // path runs on every write to a hot shard of a partly-cold index, so it is not a rare one.
        ClusterState cold = multiShardColdState(4);
        ClusterState withRouting = TransportReactivateShardsAction.reactivate(cold, MULTI_INDEX, false, java.util.Set.of(), 1_000L);
        assertSame(withRouting, TransportReactivateShardsAction.reactivate(withRouting, MULTI_INDEX, false, java.util.Set.of(2), 2_000L));
    }

    private static final String MULTI_INDEX = "partly-cold-idx";

    /** An index of {@code shardCount} shards, every one suspended, with a routing table present. */
    private static ClusterState multiShardColdState(int shardCount) {
        IndexMetadata indexMetadata = IndexMetadata.builder(MULTI_INDEX)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(shardCount)
            .numberOfReplicas(0)
            .build();
        for (int shardId = 0; shardId < shardCount; shardId++) {
            indexMetadata = SuspendedShardsMetadata.withShardSuspended(indexMetadata, shardId);
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(RoutingTable.builder().build())
            .build();
    }

    /** Present in metadata with a suspended shard, absent from routing. */
    private static ClusterState coldState() {
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        indexMetadata = SuspendedShardsMetadata.withShardSuspended(indexMetadata, 0);

        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(RoutingTable.builder().build())
            .build();
    }
}
