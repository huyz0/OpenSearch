/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.net.InetAddress;
import java.util.Set;

/**
 * C5. The gate: which indices get computed placement, and whether this node does it at all.
 *
 * <p>The two conditions are deliberately separate, and the test that matters most is
 * {@link #testANonServerlessIndexIsDeclinedEvenWhenEnabled}. Collapsing them would mean enabling the
 * feature reroutes every index in the cluster, including ones with local disk and peer recovery, and
 * that is not a mistake that announces itself: those indices would simply start being told their shards
 * live somewhere they do not.
 */
public class ComputedPlacementGateTests extends OpenSearchTestCase {

    @After
    public void uninstall() {
        // The registry is static, so a leaked supplier makes an unrelated suite fail for reasons
        // nothing in it explains.
        ComputedPlacementGate.uninstall();
    }

    public void testDisabledInstallsNothing() {
        ComputedPlacementGate.install(false);

        assertFalse(AbsentIndexRoutingSuppliers.isRegistered());
    }

    public void testEnabledInstallsTheSupplier() {
        ComputedPlacementGate.install(true);

        assertTrue(AbsentIndexRoutingSuppliers.isRegistered());
    }

    public void testServerlessIndexGetsAComputedEntry() {
        ComputedPlacementGate.install(true);

        IndexRoutingTable routing = AbsentIndexRoutingSuppliers.supply(stateWithNodes(), serverlessIndex("serverless_idx", 4));

        assertNotNull(routing);
        assertEquals(4, routing.shards().size());
        for (int shardId = 0; shardId < 4; shardId++) {
            assertEquals(ShardRoutingState.STARTED, routing.shard(shardId).primaryShard().state());
        }
    }

    /**
     * The one that matters. A non-serverless index with no routing entry is a genuine problem and must
     * keep reporting no shard available, exactly as it did before this hook existed.
     */
    public void testANonServerlessIndexIsDeclinedEvenWhenEnabled() {
        ComputedPlacementGate.install(true);

        assertNull(AbsentIndexRoutingSuppliers.supply(stateWithNodes(), plainIndex("classic", 4)));
    }

    public void testOwnershipIsReadFromIndexSettings() {
        assertTrue(ComputedPlacementGate.ownsIndex(serverlessIndex("serverless_idx", 1)));
        assertFalse(ComputedPlacementGate.ownsIndex(plainIndex("idx", 1)));
        assertFalse(ComputedPlacementGate.ownsIndex(null));
    }

    public void testUninstallRestoresTheDefault() {
        ComputedPlacementGate.install(true);
        assertTrue(AbsentIndexRoutingSuppliers.isRegistered());

        ComputedPlacementGate.uninstall();

        assertFalse(AbsentIndexRoutingSuppliers.isRegistered());
        assertNull(AbsentIndexRoutingSuppliers.supply(stateWithNodes(), serverlessIndex("serverless_idx", 1)));
    }

    public void testInstallingTwiceReplacesRatherThanAccumulates() {
        ComputedPlacementGate.install(true);
        ComputedPlacementGate.install(true);

        assertTrue(AbsentIndexRoutingSuppliers.isRegistered());
        assertNotNull(AbsentIndexRoutingSuppliers.supply(stateWithNodes(), serverlessIndex("serverless_idx", 1)));
    }

    // ---------------------------------------------------------------- helpers

    private static IndexMetadata serverlessIndex(String name, int shards) {
        return index(name, shards, true);
    }

    private static IndexMetadata plainIndex(String name, int shards) {
        return index(name, shards, false);
    }

    private static IndexMetadata index(String name, int shards, boolean serverless) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-0000000000")
                    .put("index.serverless_storage.enabled", serverless)
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }

    private static ClusterState stateWithNodes() {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node("data-1")).add(node("data-2")).add(node("data-3")).build())
            .build();
    }

    private static DiscoveryNode node(String id) {
        return new DiscoveryNode(
            id,
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + Math.abs(id.hashCode() % 1000)),
            java.util.Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }
}
