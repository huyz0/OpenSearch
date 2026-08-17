/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.tiering;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.tiering.TieringValidationResult;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.ComputedShardRouting;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

/**
 * C28. What tiering does to an index whose placement is computed.
 *
 * <p>The counterpart of the scaling probe, and the answer is different enough to be worth separating.
 * Tiering already refuses a computed index, because {@code validateIndexHealth} carries a Phase A guard
 * that treats an absent routing entry as not healthy. So the outcome is already safe: nothing downstream
 * runs, and {@code getIndexPrimaryStoreSize}, which would throw {@code IndexNotFoundException} on the
 * single-index accessor, is only reached for accepted indices.
 *
 * <p>What is wrong is the sentence. The index is rejected as "index is red", which is not true and sends
 * an operator to look at shard health for an index that is perfectly healthy. C14 established the
 * standard here: a refusal is only worth shipping if it names the actual reason.
 */
public class ComputedTieringProbeTests extends OpenSearchTestCase {

    private static final String INDEX = "computed-tiering";

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
    }

    /**
     * The refusal, asserted through the validator an operator actually reaches.
     *
     * <p>The outcome was already safe before this change, because an absent routing entry makes
     * {@code validateIndexHealth} return false. What was wrong was the sentence: a fully available index
     * was rejected as "index is red", sending whoever read it to investigate shard health that is not the
     * problem. C14 set the standard that a refusal is only worth shipping if it names the real reason.
     */
    public void testTieringRefusesAComputedIndexWithTheRealReason() {
        registerComputedPlacement();
        Index index = new Index(INDEX, INDEX + "-uuid-0000000000");
        ClusterState state = ClusterState.builder(stateWithoutRouting()).nodes(warmNodes()).build();

        TieringValidationResult result = TieringRequestValidator.validateHotToWarm(
            state,
            Set.of(index),
            clusterInfo(),
            diskThresholdSettings()
        );

        assertTrue("a computed index must be rejected for tiering", result.getRejectedIndices().containsKey(index));
        assertEquals(
            "the rejection must name computed placement rather than claiming the index is red",
            "index shard placement is computed rather than published, and tiering relocates shards through the allocator",
            result.getRejectedIndices().get(index)
        );
    }

    /**
     * The reason the old refusal was not good enough, kept as an assertion so the claim is measured
     * rather than asserted in prose: the index calling itself red is fully available.
     */
    public void testTheComputedIndexIsNotActuallyRed() {
        registerComputedPlacement();
        ClusterState state = stateWithoutRouting();

        IndexRoutingTable resolved = AbsentIndexRoutingSuppliers.resolve(state, INDEX);

        assertNotNull("placement resolves an entry, so the index is reachable rather than red", resolved);
        assertTrue(
            "every shard of the computed index is active, so calling it red misdirects whoever reads it",
            resolved.allPrimaryShardsActive()
        );
    }

    // ---------------------------------------------------------------- helpers

    private static DiscoveryNodes warmNodes() {
        return DiscoveryNodes.builder()
            .add(
                new DiscoveryNode(
                    "node-w0",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Map.of(),
                    Set.of(DiscoveryNodeRole.WARM_ROLE),
                    Version.CURRENT
                )
            )
            .build();
    }

    private static ClusterInfo clusterInfo() {
        Map<String, DiskUsage> usages = Map.of("node-w0", new DiskUsage("node-w0", "node-w0", "/foo/bar", 100, 50));
        return new ClusterInfo(usages, null, Map.of(), null, null, Map.of(), Map.of());
    }

    private static DiskThresholdSettings diskThresholdSettings() {
        return new DiskThresholdSettings(
            Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "10b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "10b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "5b")
                .build(),
            new org.opensearch.common.settings.ClusterSettings(
                Settings.EMPTY,
                org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
            )
        );
    }

    private static void registerComputedPlacement() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> true);
        AbsentIndexRoutingSuppliers.register((clusterState, metadata) -> computedEntry(metadata));
    }

    private static IndexRoutingTable computedEntry(IndexMetadata indexMetadata) {
        ShardId shard = new ShardId(indexMetadata.getIndex(), 0);
        return IndexRoutingTable.builder(indexMetadata.getIndex())
            .addIndexShard(
                new IndexShardRoutingTable.Builder(shard).addShard(
                    ComputedShardRouting.started(shard, "node-1", RecoverySource.EmptyStoreRecoverySource.INSTANCE)
                ).build()
            )
            .build();
    }

    private static ClusterState stateWithoutRouting() {
        IndexMetadata metadata = IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX + "-uuid-0000000000")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // A real (non-EMPTY_ROUTING_TABLE) instance, not the builder's default -- attachIndexRoutingResolver
        // is deliberately a no-op on the shared EMPTY_ROUTING_TABLE singleton (see its own javadoc), so
        // resolving via the new SPI (Phase C4b of core-pluggability-refactor-plan.md, which
        // TieringRequestValidator#validateHotToWarm now goes through for shouldPublishRouting) needs an
        // explicit one here, same as production code gets from a real cluster state. Harmless for this
        // class's other test, which resolves through AbsentIndexRoutingSuppliers directly and never reads
        // this routing table's own attached resolver.
        org.opensearch.cluster.routing.RoutingTable routingTable = org.opensearch.cluster.routing.RoutingTable.builder().build();
        routingTable.attachIndexRoutingResolver(new org.opensearch.cluster.routing.SupplierBackedIndexRoutingResolver());
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(metadata, false).build())
            .routingTable(routingTable)
            .build();
    }
}
