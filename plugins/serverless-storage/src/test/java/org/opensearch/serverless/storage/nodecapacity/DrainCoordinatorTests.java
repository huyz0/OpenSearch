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

public class DrainCoordinatorTests extends OpenSearchTestCase {

    public void testCurrentlyExcludedNamesEmptyByDefault() {
        ClusterState state = clusterStateWithExclude(null);
        assertEquals(Set.of(), DrainCoordinator.currentlyExcludedNames(state));
    }

    public void testCurrentlyExcludedNamesParsesCommaSeparatedList() {
        ClusterState state = clusterStateWithExclude("node-1,node-2, node-3");
        assertEquals(Set.of("node-1", "node-2", "node-3"), DrainCoordinator.currentlyExcludedNames(state));
    }

    public void testCurrentlyExcludedNamesIgnoresBlankEntries() {
        ClusterState state = clusterStateWithExclude("node-1,,node-2,");
        assertEquals(Set.of("node-1", "node-2"), DrainCoordinator.currentlyExcludedNames(state));
    }

    private static ClusterState clusterStateWithExclude(String value) {
        Settings.Builder settings = Settings.builder();
        if (value != null) {
            settings.put(DrainCoordinator.EXCLUDE_NAME_SETTING_KEY, value);
        }
        Metadata metadata = Metadata.builder().transientSettings(settings.build()).build();
        return ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
    }
}
