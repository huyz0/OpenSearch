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

    /**
     * Finding N-7. {@code currentlyExcludedNames} falls back to the <em>persistent</em> setting, but
     * {@code mutateExcludeNames} only ever writes the <em>transient</em> one. Comparing the mutation
     * result against the fallback-resolved value meant that, with an operator's persistent exclude of
     * a departed node, the hygiene sweep computed {} != {old-node} on every tick, "removed" a
     * transient key that was never there, and still returned a freshly constructed ClusterState --
     * which MasterService publishes on reference inequality. One full publication and one full
     * reroute, every tick, forever, on a cluster that looked healthy.
     *
     * <p>The read-modify-write basis is now the transient value alone, so a persistent-only exclude
     * resolves to an empty starting set and a removal of it is genuinely a no-op.
     */
    public void testTransientExcludedNamesIgnoresThePersistentSetting() {
        Settings persistent = Settings.builder().put(DrainCoordinator.EXCLUDE_NAME_SETTING_KEY, "old-node").build();
        Metadata metadata = Metadata.builder().persistentSettings(persistent).build();
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        assertEquals(
            "the reporting path still answers what is actually excluded right now, persistent entries included",
            Set.of("old-node"),
            DrainCoordinator.currentlyExcludedNames(state)
        );
        assertEquals(
            "but the write path must see only what it itself wrote, or removing a persistent entry it cannot write "
                + "looks like a change on every single tick",
            Set.of(),
            DrainCoordinator.transientExcludedNames(state)
        );
    }

    public void testTransientTakesPrecedenceOverPersistentForReporting() {
        Settings persistent = Settings.builder().put(DrainCoordinator.EXCLUDE_NAME_SETTING_KEY, "old-node").build();
        Settings transientSettings = Settings.builder().put(DrainCoordinator.EXCLUDE_NAME_SETTING_KEY, "draining-node").build();
        Metadata metadata = Metadata.builder().persistentSettings(persistent).transientSettings(transientSettings).build();
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        assertEquals(Set.of("draining-node"), DrainCoordinator.currentlyExcludedNames(state));
        assertEquals(Set.of("draining-node"), DrainCoordinator.transientExcludedNames(state));
    }

    /**
     * Finding N-10. The exclude list is a comma-joined string with no escaping, so a node name
     * containing a comma would be written and then read back as two bogus names -- neither matching a
     * live node, so the hygiene sweep would try to remove them on every tick, forever, while the node
     * the operator actually asked to drain was never excluded at all.
     */
    public void testDrainRefusesANodeNameContainingAComma() {
        DrainCoordinator coordinator = new DrainCoordinator(null);
        java.util.concurrent.atomic.AtomicReference<Exception> failure = new java.util.concurrent.atomic.AtomicReference<>();
        coordinator.drain("bad,name", org.opensearch.core.action.ActionListener.wrap(ignored -> {
            throw new AssertionError("draining a comma-containing node name must not be accepted");
        }, failure::set));
        assertNotNull("the request must be refused rather than silently mangled", failure.get());
        assertThat(failure.get(), org.hamcrest.Matchers.instanceOf(IllegalArgumentException.class));
        assertTrue(failure.get().getMessage().contains("comma"));
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
