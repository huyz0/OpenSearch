/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * C12. Whether computed placement is reachable at all in a running cluster.
 *
 * <p>This test exists because of what writing it revealed. The unit tests for C3, C4 and C5 all passed
 * against a hand-built state in which the index had no routing entry. Every real index has one:
 * {@code MetadataCreateIndexService} calls {@code addAsNew} for everything it creates, and a supplier
 * only ever runs when there is no entry. So the supplier was installed and never invoked -- the whole
 * chain looked wired and was dead, and no unit test could have noticed, because they all constructed the
 * absence they were testing.
 *
 * <p>The fix was a second registration that opts an index out of publication. What this asserts is the
 * property that fix exists for: that an opted-out index really has no published routing entry after
 * creation, and that an ordinary index still does.
 *
 * <h2>What running it found, and what C17 did about it</h2>
 *
 * The first run timed out. Index creation <b>hung</b> for an index whose routing is not published,
 * because the create API waits for active shards and counts them by reading the routing table. With no
 * entry there are no active shards, the wait never satisfies, and the call never returns.
 *
 * <p>So skipping publication was necessary and not sufficient: everything that counts active shards has
 * to consult the supplier the same way resolution does. C17 is that second seam, and this test is what
 * it is for. The registration below now installs both halves, a supplier and the opt-out, which is the
 * configuration the design intends: an opt-out with no supplier leaves an index with no routing at all,
 * and that is a configuration error rather than a mode of operation.
 *
 * <p>What this test does <b>not</b> claim is that the index is usable. Data nodes create shards from
 * {@code RoutingNodes}, which are built from the published table, so a computed index has no shard
 * anywhere. Creation completing and health reflecting the computed entry is the whole of the claim here;
 * whether a shard exists to receive a document is C18.
 */
public class ComputedPlacementReachabilityIT extends OpenSearchIntegTestCase {

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.register(null);
    }

    public void testAnOptedOutIndexPublishesNoRoutingEntry() throws Exception {
        registerComputedPlacementFor("computed-");

        assertAcked(prepareCreate("computed-one").setSettings(oneShard()));
        assertAcked(prepareCreate("classic-one").setSettings(oneShard()));

        ClusterState state = client().admin().cluster().prepareState().get().getState();

        assertTrue("metadata must still hold the index", state.metadata().hasIndex("computed-one"));
        assertFalse(
            "an opted-out index must publish no routing entry, or the supplier is never consulted",
            state.routingTable().hasIndex("computed-one")
        );

        assertTrue("an ordinary index must still publish routing", state.routingTable().hasIndex("classic-one"));
    }

    /**
     * The C17 assertion proper: creation must observe the computed shards as active.
     *
     * <p>{@code shardsAcknowledged} is what carries the claim, and {@code assertAcked} is not enough on
     * its own. Without the fix the create call still returns and is still acknowledged; what it does is
     * wait out the full active-shards timeout and come back with {@code shardsAcknowledged=false}. The
     * difference is visible in the clock as well as in the flag: 6.3 seconds against 33.9, the second
     * being the 30-second timeout plus cluster start. A test that asserted only acknowledgement passed
     * against both, which is how this one was written the first time.
     */
    public void testCreatingAComputedIndexSeesItsShardsAsActive() throws Exception {
        registerComputedPlacementFor("computed-");

        CreateIndexResponse response = prepareCreate("computed-two").setSettings(oneShard())
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .get();

        assertTrue("creation must be acknowledged", response.isAcknowledged());
        assertTrue(
            "the computed shards must count as active, or creation waits out its timeout for shards that "
                + "were never going to be published",
            response.isShardsAcknowledged()
        );

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertFalse(state.routingTable().hasIndex("computed-two"));
    }

    /**
     * Health must count the computed entry rather than skipping the index. Skipping is not neutral: it
     * drops the index from every counter and reports green, which would make later tests pass vacuously.
     */
    public void testHealthCountsTheComputedEntry() throws Exception {
        registerComputedPlacementFor("computed-");

        assertAcked(prepareCreate("computed-three").setSettings(threeShards()));

        ClusterHealthResponse health = client().admin().cluster().prepareHealth("computed-three").get();

        assertEquals("the computed shards must be counted, not skipped", 3, health.getActiveShards());
        assertEquals(3, health.getActivePrimaryShards());
        assertEquals(0, health.getUnassignedShards());
    }

    /** Without the opt-out, every index publishes routing, which is what made the mechanism dead. */
    public void testWithoutTheOptOutEveryIndexPublishesRouting() throws Exception {
        assertAcked(prepareCreate("plain-one").setSettings(oneShard()));

        ClusterState state = client().admin().cluster().prepareState().get().getState();

        assertTrue(state.routingTable().hasIndex("plain-one"));
    }

    /**
     * Installs both halves for indices with the given name prefix: the opt-out that keeps routing
     * unpublished, and a supplier that computes it. Placement here is deliberately the simplest thing
     * that is deterministic across nodes, round-robin over data node ids in sorted order, because what
     * is under test is the seam rather than the placement function.
     */
    private static void registerComputedPlacementFor(String prefix) {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> metadata.getIndex().getName().startsWith(prefix));
        AbsentIndexRoutingSuppliers.register(ComputedPlacementReachabilityIT::compute);
    }

    private static IndexRoutingTable compute(ClusterState state, IndexMetadata indexMetadata) {
        List<String> dataNodes = new ArrayList<>(state.nodes().getDataNodes().keySet());
        if (dataNodes.isEmpty()) {
            return null;
        }
        Collections.sort(dataNodes);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            ShardId shard = new ShardId(indexMetadata.getIndex(), shardId);
            // ExistingStoreRecoverySource explicitly. The A5 trap is that a recovery source derived from
            // absent inSyncAllocationIds silently becomes EmptyStoreRecoverySource, which recovers a live
            // index blank.
            ShardRouting routing = ShardRouting.newUnassigned(
                shard,
                true,
                RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "computed placement")
            ).initialize(dataNodes.get(shardId % dataNodes.size()), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted();
            builder.addIndexShard(new IndexShardRoutingTable.Builder(shard).addShard(routing).build());
        }
        return builder.build();
    }

    private static Settings oneShard() {
        return shards(1);
    }

    private static Settings threeShards() {
        return shards(3);
    }

    private static Settings shards(int count) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, count)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }
}
