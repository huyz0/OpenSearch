/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.split.InPlaceMergeShardClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.split.InPlaceSplitShardClusterStateUpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

/**
 * C14. Resharding an index whose placement is computed is rejected, with a reason.
 *
 * <p>In-place split and merge are built on manipulating the published routing table: they add child
 * primaries, retire parent entries, and drive their commit off what that table reports. A computed index
 * publishes nothing, so without a rejection split throws {@code IndexNotFoundException} from a routing
 * lookup, merge dereferences a null index routing table, and both commit services read the absence as
 * "still in progress" and stay pending forever. Three failure modes, none of which tells an operator
 * what is actually wrong.
 *
 * <p>Making resharding work under computed placement is a redesign rather than a fix. The children would
 * have to be placed by the same function, and the operation would have to reach agreement without
 * publishing anything, which is most of what this area exists to avoid. Rejecting is the honest interim
 * answer, and the message is the deliverable.
 *
 * <p>A unit test rather than an integration test on purpose. The guard is pure logic over a cluster
 * state, so a constructed state exercises it exactly and in milliseconds, while the equivalent
 * integration test spent its time on cluster teardown rather than on the assertion.
 */
public class ComputedReshardingRejectionTests extends OpenSearchTestCase {

    private static final String COMPUTED = "computed-idx";

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
    }

    public void testSplitIsRejectedForAComputedIndex() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> metadata.getIndex().getName().startsWith("computed-"));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, this::split);

        assertTrue(
            "the message must say why, not merely that it failed: " + e.getMessage(),
            e.getMessage().contains("placement is computed rather than published")
        );
    }

    public void testMergeIsRejectedForAComputedIndex() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> metadata.getIndex().getName().startsWith("computed-"));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, this::merge);

        assertTrue(
            "the message must say why, not merely that it failed: " + e.getMessage(),
            e.getMessage().contains("placement is computed rather than published")
        );
    }

    /**
     * The control, and it is the half that matters. A guard that rejected everything would pass both
     * tests above while breaking every ordinary resharding request in the product. With nothing
     * registered no index is computed, so these must fail on their own merits instead.
     */
    public void testAnOrdinaryIndexIsNotRejectedForBeingComputed() {
        // These still fail, because this state publishes no routing for any index, and that is the point:
        // they fail the way they always did, with IndexNotFoundException from a routing lookup, rather
        // than through the new guard. That old failure is also the argument for the guard, since
        // "no such index" is a poor description of an index that plainly exists.
        Exception split = expectThrows(Exception.class, this::split);
        assertFalse(
            "with nothing registered, no index is computed and the rejection must stay silent",
            String.valueOf(split.getMessage()).contains("placement is computed rather than published")
        );

        Exception merge = expectThrows(Exception.class, this::merge);
        assertFalse(String.valueOf(merge.getMessage()).contains("placement is computed rather than published"));
    }

    private void split() {
        MetadataInPlaceSplitShardService.applySplitShardRequest(
            state(),
            new InPlaceSplitShardClusterStateUpdateRequest("test", COMPUTED, 0, 2),
            (s, reason) -> s
        );
    }

    private void merge() {
        MetadataInPlaceMergeShardService.applyMergeShardRequest(
            state(),
            new InPlaceMergeShardClusterStateUpdateRequest("test", COMPUTED, 0),
            (s, reason) -> s
        );
    }

    private static ClusterState state() {
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index(COMPUTED), false).build())
            .nodes(DiscoveryNodes.builder().add(node("node-1")).localNodeId("node-1").clusterManagerNodeId("node-1").build())
            .build();
    }

    private static IndexMetadata index(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-000000000")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private static DiscoveryNode node(String id) {
        return new DiscoveryNode(
            id,
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }
}
