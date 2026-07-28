/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.Set;

/**
 * H9c. How a computed shard goes to sleep, given that nothing can tell the allocator to refuse it.
 *
 * <p>Scale-to-zero works today by rewriting {@code IndexMetadata} and letting
 * {@code SuspendedShardAllocationDecider} refuse the shard on the next reroute. Neither half applies to a
 * gated index: H9a measured that the rewrite finds no metadata to rewrite and returns the state unchanged,
 * and the decider never runs at all, because a computed index does not go through the allocator. Area C
 * derives its placement instead.
 *
 * <p>So suspension has to move from being a verdict handed to an allocator to being an input to placement.
 * A shard that placement does not place is a shard assigned nowhere, which is what asleep means, and it is
 * the only definition available to an index whose routing is computed rather than allocated.
 *
 * <p>The filter sits in {@code supply} rather than in each supplier deliberately. Every computed routing
 * read funnels through that one method, so a supplier cannot forget the check and wake a sleeping shard.
 * C3 is the precedent: it hooked resolution, left the write path reading the table directly, and the two
 * halves disagreed for as long as the pairing was open-coded at each site.
 */
public class SuspendedComputedShardTests extends OpenSearchTestCase {

    private static final String INDEX = "gated";
    private static final int SHARDS = 3;

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerSuspendedShards(null);
    }

    /**
     * The property. A suspended shard is not placed, so it is assigned nowhere, so it is asleep.
     */
    public void testASuspendedShardIsNotPlaced() {
        registerPlacementFor(SHARDS);
        AbsentIndexRoutingSuppliers.registerSuspendedShards(name -> Set.of(1));

        IndexRoutingTable resolved = AbsentIndexRoutingSuppliers.resolve(stateWithout(INDEX), INDEX);

        assertNotNull("the index must still resolve, since only one of its shards is asleep", resolved);
        assertNull("a suspended shard must not be placed", resolved.shard(1));
        assertNotNull("its awake siblings must still be placed", resolved.shard(0));
        assertNotNull(resolved.shard(2));
    }

    /**
     * The same absence through the shard-level read, which is the one a write or a get takes. Resolution
     * and routing disagreeing about whether a shard exists is the C3 failure exactly, so both are pinned.
     */
    public void testASuspendedShardIsAbsentFromTheShardLevelRead() {
        registerPlacementFor(SHARDS);
        AbsentIndexRoutingSuppliers.registerSuspendedShards(name -> Set.of(1));
        ClusterState state = stateWithout(INDEX);

        assertNull(
            "the shard-level read must agree with the index-level one, or a write reaches a sleeping shard " + "that a search cannot see",
            AbsentIndexRoutingSuppliers.resolveShard(state, new ShardId(new Index(INDEX, INDEX + "-uuid"), 1))
        );
        assertNotNull(AbsentIndexRoutingSuppliers.resolveShard(state, new ShardId(new Index(INDEX, INDEX + "-uuid"), 0)));
    }

    /**
     * Waking is the inverse and must be immediate: the next read places the shard again, with no
     * republication in between, because there is no cluster state entry to republish.
     */
    public void testWakingRestoresPlacement() {
        registerPlacementFor(SHARDS);
        ClusterState state = stateWithout(INDEX);

        AbsentIndexRoutingSuppliers.registerSuspendedShards(name -> Set.of(1));
        assertNull("the premise: the shard is asleep", AbsentIndexRoutingSuppliers.resolve(state, INDEX).shard(1));

        AbsentIndexRoutingSuppliers.registerSuspendedShards(name -> Set.of());
        assertNotNull("waking must take effect on the next read", AbsentIndexRoutingSuppliers.resolve(state, INDEX).shard(1));
    }

    /**
     * Every shard asleep is a scaled-to-zero index, which must still resolve as an index with no shards
     * rather than as a missing index. The distinction is the one this area has got wrong eight times: an
     * index that answers "no shards" is asleep, an index that answers "not found" is gone.
     */
    public void testAFullySuspendedIndexStillResolves() {
        registerPlacementFor(SHARDS);
        AbsentIndexRoutingSuppliers.registerSuspendedShards(name -> Set.of(0, 1, 2));

        IndexRoutingTable resolved = AbsentIndexRoutingSuppliers.resolve(stateWithout(INDEX), INDEX);

        assertNotNull("a scaled-to-zero index must resolve rather than vanish", resolved);
        assertEquals("and it must have no shards placed", 0, resolved.shards().size());
    }

    /**
     * The control that keeps this from taxing every cluster. With no suspension source registered, the
     * supplier's own entry is returned unchanged and not even copied.
     */
    public void testNothingRegisteredReturnsTheSuppliedEntryUntouched() {
        IndexRoutingTable supplied = placementFor(SHARDS);
        AbsentIndexRoutingSuppliers.register((state, metadata) -> supplied);

        assertSame(
            "with nothing suspended the supplied entry must be returned as-is, so the common case allocates " + "nothing",
            supplied,
            AbsentIndexRoutingSuppliers.resolve(stateWithout(INDEX), INDEX)
        );
    }

    /**
     * A suspension source that throws must leave every shard placed. A shard wrongly awake serves a
     * request; a shard wrongly asleep is an outage, so the failure has to fall on the safe side.
     */
    public void testAFailingSuspensionSourceLeavesEveryShardPlaced() {
        registerPlacementFor(SHARDS);
        AbsentIndexRoutingSuppliers.registerSuspendedShards(name -> { throw new IllegalStateException("suspension source down"); });

        IndexRoutingTable resolved = AbsentIndexRoutingSuppliers.resolve(stateWithout(INDEX), INDEX);

        assertNotNull("a failing suspension source must not take the index away", resolved);
        assertEquals("every shard must stay placed", SHARDS, resolved.shards().size());
    }

    /**
     * A published index must not be filtered. Its shards are the allocator's business, and the decider
     * already handles them, so applying the filter here as well would suspend the same shard twice by two
     * different mechanisms.
     */
    public void testAPublishedIndexIsUnaffected() {
        AbsentIndexRoutingSuppliers.registerSuspendedShards(name -> Set.of(0, 1, 2));
        IndexMetadata metadata = indexMetadata();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(metadata, false).build())
            .routingTable(RoutingTable.builder().addAsNew(metadata).build())
            .build();

        IndexRoutingTable resolved = AbsentIndexRoutingSuppliers.resolve(state, INDEX);

        assertEquals("a published entry must be returned untouched", SHARDS, resolved.shards().size());
    }

    // ---------------------------------------------------------------- helpers

    private static void registerPlacementFor(int shardCount) {
        AbsentIndexRoutingSuppliers.register((state, metadata) -> placementFor(shardCount));
    }

    /** Placement as Area C computes it: every shard started on a node, with no allocator involved. */
    private static IndexRoutingTable placementFor(int shardCount) {
        Index index = new Index(INDEX, INDEX + "-uuid");
        IndexRoutingTable.Builder placement = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < shardCount; shard++) {
            ShardId shardId = new ShardId(index, shard);
            placement.addIndexShard(
                new IndexShardRoutingTable.Builder(shardId).addShard(
                    ComputedShardRouting.started(shardId, "node-1", RecoverySource.EmptyStoreRecoverySource.INSTANCE)
                ).build()
            );
        }
        return placement.build();
    }

    /** A cluster state that does not hold this index, which is what a gated index looks like after H5. */
    private static ClusterState stateWithout(String indexName) {
        return ClusterState.builder(ClusterName.DEFAULT).build();
    }

    private static IndexMetadata indexMetadata() {
        return IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX + "-uuid")
                    .build()
            )
            .numberOfShards(SHARDS)
            .numberOfReplicas(0)
            .build();
    }
}
