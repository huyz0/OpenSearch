/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Bug fix: {@code MetadataUpdateSettingsService#validateIndexTotalPrimaryShardsPerNodeSetting} decided
 * "is remote store enabled" via {@code nodes.stream().allMatch(DiscoveryNode::isRemoteStoreNode)}, which
 * is vacuously {@code true} for an empty node set ({@link java.util.stream.Stream#allMatch} on an empty
 * stream always returns {@code true}) -- so a cluster with zero nodes was wrongly treated as remote-store
 * enabled, the opposite of intended, whenever this setting validation ran (e.g. during early cluster
 * bootstrap, before any node has joined).
 */
public class MetadataUpdateSettingsServiceEmptyNodeSetTests extends OpenSearchTestCase {

    private ClusterService clusterServiceWithNoNodes() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(DiscoveryNodes.EMPTY_NODES).build();
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    public void testEmptyNodeSetIsNotTreatedAsRemoteStoreEnabled() {
        Settings settings = Settings.builder()
            .put(ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2)
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataUpdateSettingsService.validateIndexTotalPrimaryShardsPerNodeSetting(settings, clusterServiceWithNoNodes())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("can only be used with remote store enabled clusters"));
    }

    public void testDefaultSettingSkipsValidationEvenWithNoNodes() {
        // indexPrimaryShardsPerNode/indexRemoteCapablePrimaryShardsPerNode both default to -1: the method
        // returns before ever consulting the node set, so an empty cluster must not throw here either.
        MetadataUpdateSettingsService.validateIndexTotalPrimaryShardsPerNodeSetting(Settings.EMPTY, clusterServiceWithNoNodes());
    }
}
