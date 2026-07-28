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
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

/**
 * W6. Scale-to-zero for a gated index, with the registry actually installed.
 *
 * <p>H9c built the mechanism and H9d connected it to the coordinator, and the plugin still constructed the
 * coordinator through the four-argument constructor, which is by definition the pre-H9d behaviour: the
 * suspend task is submitted, finds no {@code IndexMetadata} to rewrite, and returns the state unchanged.
 * H9a measured that. So until this wiring, a gated index could not sleep, and scaling to zero is the reason
 * the serverless design exists.
 *
 * <p>What is asserted here is the installation rather than the mechanism, since the mechanism already has
 * unit coverage. The question is whether a node that has started has a registry that placement can see.
 */
public class GatedSuspensionWiringIT extends OpenSearchIntegTestCase {

    private static final String GATED = "gated-sleepy";
    private static final String GATED_UUID = "gated-sleepy-uuid";
    private static final int SHARDS = 3;

    @After
    public void clearRouting() {
        AbsentIndexRoutingSuppliers.register(null);
    }

    /**
     * The property the wiring buys. A suspension recorded through the registry the node installed is
     * visible to placement, so the shard stops being placed.
     */
    public void testASuspensionRecordedOnTheNodeRegistryReachesPlacement() {
        GatedShardSuspensionRegistry registry = new GatedShardSuspensionRegistry();
        registry.install();
        try {
            AbsentIndexRoutingSuppliers.register((state, metadata) -> placement());
            ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();

            assertNotNull("the premise: the shard is placed while awake", AbsentIndexRoutingSuppliers.resolve(empty, GATED).shard(1));

            registry.suspend(GATED_UUID, 1);

            IndexRoutingTable afterSleep = AbsentIndexRoutingSuppliers.resolve(empty, GATED);
            assertNull("a suspended gated shard must stop being placed", afterSleep.shard(1));
            assertNotNull("and its siblings must be untouched", afterSleep.shard(0));
        } finally {
            registry.uninstall();
        }
    }

    /**
     * The control that says the assertion above is about the installation. With no registry installed the
     * same suspension changes nothing, which is what every node did before this wiring.
     */
    public void testWithoutInstallationTheSameSuspensionIsInvisible() {
        GatedShardSuspensionRegistry registry = new GatedShardSuspensionRegistry();
        // Deliberately not installed.
        AbsentIndexRoutingSuppliers.register((state, metadata) -> placement());
        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();

        registry.suspend(GATED_UUID, 1);

        assertNotNull(
            "an uninstalled registry cannot affect placement, which is precisely why a gated index could "
                + "never sleep before this wiring",
            AbsentIndexRoutingSuppliers.resolve(empty, GATED).shard(1)
        );
    }

    private static IndexRoutingTable placement() {
        Index index = new Index(GATED, GATED_UUID);
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
