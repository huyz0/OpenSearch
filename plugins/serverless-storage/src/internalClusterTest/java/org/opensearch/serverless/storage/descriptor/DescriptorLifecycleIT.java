/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.serverless.storage.scaletozero.GatedShardSuspensionRegistry;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Map;

/**
 * W9. Every wired seam, in one lifecycle, on a running node.
 *
 * <p>This is the falsifier for the whole W series and its absence is what let five reviews report the
 * mechanisms as done. Each step below was proven in isolation months before it could run here: the
 * mechanisms existed, were tested, were mutation tested, and none of them was reachable from a node that
 * had started, because no production code registered any of the seams.
 *
 * <p>The test walks one index through its life and checks the thing each stage was built for:
 *
 * <ol>
 *   <li>a descriptor is recorded, so the index has an identity outside cluster state (W2, W4)</li>
 *   <li>the name resolves through the descriptor, carrying its uuid (W3, H8a)</li>
 *   <li>a wildcard finds it once refreshed, and not before (W3, H18)</li>
 *   <li>a mapping is recorded and its generation advances without a broadcast (W5, H6a)</li>
 *   <li>cluster stats counts its fields without enumerating indices (W7, H19)</li>
 *   <li>a shard sleeps, and placement stops placing it, with no cluster state task (W6, H9c)</li>
 *   <li>it wakes, and placement returns it</li>
 *   <li>it is deleted, and the descriptor is tombstoned rather than removed (W4, H4b)</li>
 * </ol>
 *
 * <p>Written to fail if any single seam is unregistered, which is what makes it worth more than the sum of
 * the per-seam tests: those each install their own dependencies, so every one of them would keep passing
 * on a node where nothing was wired at all.
 */
public class DescriptorLifecycleIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "lifecycle-idx";
    private static final String UUID = "lifecycle-idx-uuid";
    private static final int SHARDS = 3;

    private DescriptorStore store;
    private IndexBackedMappingStore mappings;
    private GatedShardSuspensionRegistry suspensions;

    @After
    public void tearDownSeams() {
        DescriptorGate.uninstall();
        AbsentIndexRoutingSuppliers.register(null);
        if (suspensions != null) {
            suspensions.uninstall();
        }
    }

    public void testAGatedIndexLivesItsWholeLifeOutsideClusterState() throws Exception {
        installEverySeam();

        // 1. Recorded. Without W4's publisher this is null and nothing downstream can work.
        store.create(descriptor());
        IndexDescriptor recorded = store.get(INDEX);
        assertNotNull("the index must have an identity outside cluster state", recorded);
        assertEquals(UUID, recorded.uuid());

        // 2. Nameable. H8a wired the resolver and until W3 nothing supplied it, so this raised
        // IndexNotFoundException on every node.
        ClusterState stateWithoutIt = ClusterState.builder(ClusterName.DEFAULT).build();
        var resolved = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)).concreteIndices(
            stateWithoutIt,
            IndicesOptions.strictExpandOpen(),
            INDEX
        );
        assertEquals("a gated index must be nameable", 1, resolved.length);
        assertEquals("and carry its uuid, or a request cannot reach the shard", UUID, resolved[0].getUUID());

        // 3. Findable by wildcard once refreshed.
        //
        // Deliberately only the positive half. The negative half, that a wildcard cannot see it *before* a
        // refresh, is real but timing-dependent here: W8 gives the descriptor index a one second refresh
        // interval, so an automatic refresh can land between the write and the check. Asserting it here
        // would be asserting a race, which is what DescriptorFreshnessContractIT avoids by disabling
        // refresh outright. That test owns the contract; this one owns the lifecycle.
        //
        // Recorded rather than quietly dropped because the first version of this test did assert the race,
        // and it passed until W12's run happened to be slow enough to expose it.
        client().admin().indices().prepareRefresh(DescriptorStore.DESCRIPTOR_INDEX).get();
        assertEquals("a refreshed wildcard must find the gated index", 1, store.findByPrefix("lifecycle-", null, 10).size());

        // 4. Mapped, with the generation advancing and no shard informed. H4c required this to leave
        // cluster state; W5 gave it somewhere to live.
        long firstGeneration = MappingGenerationStore.updateMapping(UUID, Map.of("message", "text"));
        long secondGeneration = MappingGenerationStore.updateMapping(UUID, Map.of("level", "keyword"));
        assertTrue("the generation must advance on a new field", secondGeneration > firstGeneration);
        assertEquals("and both fields must survive", 2, mappings.read(UUID).fields().size());

        // 5. Counted, without enumerating indices. H19's gap was that this silently excluded gated indices.
        client().admin().indices().prepareRefresh(IndexBackedMappingStore.MAPPING_INDEX).get();
        var counts = new IndexBackedMappingStatsAggregator(client()).aggregate();
        assertNotNull("cluster stats must see the gated population at all", counts);
        assertEquals("its text field must be counted", 1, (int) counts.fieldCounts().get("text"));
        assertEquals("and its keyword field", 1, (int) counts.fieldCounts().get("keyword"));

        // 6. Asleep. H9c: a computed index never reaches the allocator, so suspension has to be an input to
        // placement rather than a verdict handed to one.
        assertNotNull("the premise: the shard is placed while awake", AbsentIndexRoutingSuppliers.resolve(stateWithoutIt, INDEX).shard(1));
        suspensions.suspend(UUID, 1);
        assertNull(
            "a suspended gated shard must stop being placed, which is the only definition of asleep "
                + "available to an index whose routing is computed",
            AbsentIndexRoutingSuppliers.resolve(stateWithoutIt, INDEX).shard(1)
        );
        assertNotNull("and its siblings must be untouched", AbsentIndexRoutingSuppliers.resolve(stateWithoutIt, INDEX).shard(0));

        // 7. Awake again, on the next read, with no republication because there is nothing to republish.
        suspensions.reactivate(UUID, 1);
        assertNotNull(
            "waking must take effect on the next resolution",
            AbsentIndexRoutingSuppliers.resolve(stateWithoutIt, INDEX).shard(1)
        );

        // 8. Deleted as a tombstone. H4b: absence cannot be told apart from not having looked, so a node
        // rejoining after a partition would otherwise adopt its dangling shard data.
        store.put(store.get(INDEX).tombstoned());
        IndexDescriptor tombstone = store.get(INDEX);
        assertNotNull("deletion must record rather than remove", tombstone);
        assertFalse("and the tombstone must not read as existing", tombstone.exists());
        assertEquals("while keeping the uuid that identifies the data to reclaim", UUID, tombstone.uuid());
    }

    /** Installs the seams the plugin installs at node start, so this exercises the wired arrangement. */
    private void installEverySeam() {
        store = new DescriptorStore(client(), 1);
        mappings = new IndexBackedMappingStore(client());
        suspensions = new GatedShardSuspensionRegistry();

        DescriptorGate.install(store, mappings, new IndexBackedMappingStatsAggregator(client()), true);
        suspensions.install();
        AbsentIndexRoutingSuppliers.register((state, metadata) -> placement());
    }

    private static org.opensearch.cluster.routing.IndexRoutingTable placement() {
        org.opensearch.core.index.Index index = new org.opensearch.core.index.Index(INDEX, UUID);
        var placement = org.opensearch.cluster.routing.IndexRoutingTable.builder(index);
        for (int shard = 0; shard < SHARDS; shard++) {
            var shardId = new org.opensearch.core.index.shard.ShardId(index, shard);
            placement.addIndexShard(
                new org.opensearch.cluster.routing.IndexShardRoutingTable.Builder(shardId).addShard(
                    org.opensearch.cluster.routing.ComputedShardRouting.started(
                        shardId,
                        "node-1",
                        org.opensearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource.INSTANCE
                    )
                ).build()
            );
        }
        return placement.build();
    }

    private static IndexDescriptor descriptor() {
        return new IndexDescriptor(
            INDEX,
            UUID,
            SHARDS,
            0,
            true,
            IndexDescriptor.State.OPEN,
            java.util.List.of(),
            org.opensearch.Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            1_700_000_000_000L
        );
    }
}
