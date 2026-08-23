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
import org.opensearch.cluster.metadata.IndexCatalogRegistry;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.SupplierBackedIndexCatalog;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.RoutingTable;
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
        IndexCatalogRegistry.register(null);
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

    /**
     * The {@code isActive()}-not-{@code isRegistered()} distinction, at one of the four call sites that
     * depend on it. The catalog is registered in both halves of this test -- a real node registers it
     * unconditionally at startup, whether or not the feature is on -- and only the placement supplier
     * underneath differs. A guard that asked "is a catalog registered" would fire in both and report a
     * false positive on every ordinary cluster running the plugin with the feature off.
     */
    public void testARegisteredButInactiveCatalogLeavesTheGuardOff() {
        ClusterState state = state();
        assertTrue("premise: a catalog is registered", IndexCatalogRegistry.isRegistered());
        assertFalse("premise: the feature underneath it is off", IndexCatalogRegistry.isActive());

        assertTrue(
            "a registered-but-inactive catalog must leave the guard off",
            BroadcastEmptiness.check(ACTION, state, new String[] { OPEN_INDEX }, Set.of()).isEmpty()
        );

        registerPlacement();

        assertTrue("the same registered catalog must now read as active", IndexCatalogRegistry.isActive());
        assertEquals(
            "and the guard must now fire",
            List.of(OPEN_INDEX),
            BroadcastEmptiness.check(ACTION, state, new String[] { OPEN_INDEX }, Set.of())
        );
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
        // The node-scoped catalog a real node gets from ClusterPlugin#getIndexCatalog(), registered for
        // every test here including the ones that register no placement supplier -- which is the point:
        // BroadcastEmptiness#check guards on IndexCatalog#isActive(), not on a catalog being registered,
        // so a registered catalog with nothing underneath it must still leave the guard off. Cleared in
        // this class's @After.
        IndexCatalogRegistry.register(new SupplierBackedIndexCatalog());
        RoutingTable routingTable = RoutingTable.builder().build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(index(OPEN_INDEX, IndexMetadata.State.OPEN), false)
                    .put(index(CLOSED_INDEX, IndexMetadata.State.CLOSE), false)
                    .build()
            )
            .routingTable(routingTable)
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
