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
