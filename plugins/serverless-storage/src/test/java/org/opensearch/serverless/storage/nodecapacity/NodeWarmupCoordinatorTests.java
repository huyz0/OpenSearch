/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class NodeWarmupCoordinatorTests extends OpenSearchTestCase {

    public void testCurrentlyWarmingNamesEmptyByDefault() {
        ClusterState state = clusterStateWithWarming(null);
        assertEquals(Set.of(), NodeWarmupCoordinator.currentlyWarmingNames(state));
        assertEquals(Set.of(), NodeWarmupCoordinator.currentlyWarmingNames(state.metadata()));
    }

    public void testCurrentlyWarmingNamesParsesCommaSeparatedList() {
        ClusterState state = clusterStateWithWarming("node-1,node-2, node-3");
        assertEquals(Set.of("node-1", "node-2", "node-3"), NodeWarmupCoordinator.currentlyWarmingNames(state));
        assertEquals(Set.of("node-1", "node-2", "node-3"), NodeWarmupCoordinator.currentlyWarmingNames(state.metadata()));
    }

    public void testCurrentlyWarmingNamesIgnoresBlankEntries() {
        ClusterState state = clusterStateWithWarming("node-1,,node-2,");
        assertEquals(Set.of("node-1", "node-2"), NodeWarmupCoordinator.currentlyWarmingNames(state));
    }

    private static ClusterState clusterStateWithWarming(String value) {
        Settings.Builder settings = Settings.builder();
        if (value != null) {
            settings.put(NodeWarmupCoordinator.WARMING_NAMES_SETTING_KEY, value);
        }
        Metadata metadata = Metadata.builder().transientSettings(settings.build()).build();
        return ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
    }
}
