/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.broadcast.node;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.List;
import java.util.Set;

/**
 * C24. Whether a broadcast that reached no shards can be noticed at all.
 *
 * <p>Seven times in this area an operation returned a confident empty answer instead of an error:
 * refresh, field mappings, stats, segments, recovery, force merge and cat. Not one threw. Every one was
 * found by a person eventually noticing a zero, and C21 survived six passes over the same seam for
 * exactly that reason.
 *
 * <p>The tests below stand in for a subclass that resolves the old way, by simply passing an empty set of
 * contributing indices, which is what a direct routing table read produces for a computed index.
 */
public class BroadcastEmptinessTests extends OpenSearchTestCase {

    private static final String OPEN_INDEX = "computed-open";
    private static final String CLOSED_INDEX = "computed-closed";
    private static final String ACTION = "indices:monitor/stats";

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
    }

    /** The load-bearing one: an open index that contributed nothing has to be reported. */
    public void testAnOpenIndexThatContributedNothingIsReported() {
        registerPlacement();

        List<String> missing = BroadcastEmptiness.check(ACTION, state(), new String[] { OPEN_INDEX }, Set.of());

        assertEquals("an open index contributing no shards must be named", List.of(OPEN_INDEX), missing);
    }

    /**
     * The control that stops this being a nuisance. Without a supplier an open index always has a
     * published entry with at least unassigned shards, so any zero here means something else, and a guard
     * written for this feature must not fail requests on clusters that do not use it.
     */
    public void testWithoutASupplierNothingIsReported() {
        List<String> missing = BroadcastEmptiness.check(ACTION, state(), new String[] { OPEN_INDEX }, Set.of());

        assertTrue("the guard must stay silent when no placement supplier is installed", missing.isEmpty());
    }

    /** A caller that resolved correctly must not be reported, or the guard cries wolf on every request. */
    public void testAnIndexThatContributedShardsIsNotReported() {
        registerPlacement();

        List<String> missing = BroadcastEmptiness.check(ACTION, state(), new String[] { OPEN_INDEX }, Set.of(OPEN_INDEX));

        assertTrue("an index that contributed shards must not be reported: " + missing, missing.isEmpty());
    }

    /** A closed index legitimately contributes nothing, and is the main false positive to avoid. */
    public void testAClosedIndexIsNotReported() {
        registerPlacement();

        List<String> missing = BroadcastEmptiness.check(ACTION, state(), new String[] { CLOSED_INDEX }, Set.of());

        assertTrue("a closed index contributes no shards legitimately: " + missing, missing.isEmpty());
    }

    /** An index deleted between resolution and this check is a race, not a bug worth failing over. */
    public void testAnIndexMissingFromMetadataIsNotReported() {
        registerPlacement();

        List<String> missing = BroadcastEmptiness.check(ACTION, state(), new String[] { "vanished" }, Set.of());

        assertTrue("an index deleted mid-request must not be reported: " + missing, missing.isEmpty());
    }

    /** The assertion form is what turns this from a log line into a failing test. */
    public void testTheAssertionNamesTheActionAndTheIndex() {
        registerPlacement();

        AssertionError caught = expectThrows(
            AssertionError.class,
            () -> BroadcastEmptiness.assertEveryOpenIndexContributedShards(ACTION, state(), new String[] { OPEN_INDEX }, Set.of())
        );

        assertTrue("the message must name the action: " + caught.getMessage(), caught.getMessage().contains(ACTION));
        assertTrue("the message must name the index: " + caught.getMessage(), caught.getMessage().contains(OPEN_INDEX));
    }

    // ---------------------------------------------------------------- helpers

    private static void registerPlacement() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> true);
        AbsentIndexRoutingSuppliers.register((clusterState, metadata) -> null);
    }

    private static ClusterState state() {
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(index(OPEN_INDEX, IndexMetadata.State.OPEN), false)
                    .put(index(CLOSED_INDEX, IndexMetadata.State.CLOSE), false)
                    .build()
            )
            .build();
    }

    private static IndexMetadata index(String name, IndexMetadata.State indexState) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-0000000000")
                    .build()
            )
            .state(indexState)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
