/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.ComputedShardRouting;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.SupplierBackedIndexRoutingResolver;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * C28. What search-only scaling actually does to an index whose placement is computed.
 *
 * <p>A probe rather than a fix. The plan expected one failure here, a NullPointerException from an
 * unguarded dereference, and reading the code already contradicts that: two of the three call sites are
 * null-guarded and quietly do nothing instead. Both are wrong, differently, and the decision about what
 * scaling should do needs the real behaviour rather than the expected one.
 *
 * <p>Exercised directly rather than through the transport action, because scale-down demands remote
 * store and segment replication, and a probe that got rejected by those prerequisites would look like
 * correct refusal while proving nothing about the routing reads underneath.
 */
public class ComputedScaleIndexProbeTests extends OpenSearchTestCase {

    private static final String INDEX = "computed-scale";

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
    }

    /**
     * The refusal, and the two failures it replaces.
     *
     * <p>Measured before the fix: {@code ScaleIndexShardSyncManager} threw NullPointerException from
     * {@code clusterStateProcessed}, which runs on the cluster state applier thread, so the failure
     * landed in an applier rather than in the request. Scale-up failed more quietly still, since a null
     * guard let it build a routing table with no trace of the index and report success.
     */
    public void testScalingRefusesAComputedIndexWithAReason() {
        registerComputedPlacement();
        ClusterState state = stateWithoutRouting();
        AtomicReference<Exception> failure = new AtomicReference<>();

        boolean valid = new ScaleIndexOperationValidator().validateScalePrerequisites(
            state.metadata().index(INDEX),
            state.routingTable(),
            INDEX,
            listenerCapturing(failure),
            randomBoolean()
        );

        assertFalse("scaling a computed index must not validate", valid);
        assertNotNull("the caller must be told why rather than left to an NPE on an applier thread", failure.get());
        assertTrue(
            "the refusal must name computed placement as the reason: " + failure.get().getMessage(),
            failure.get().getMessage().contains("computed rather than published")
        );
    }

    /**
     * The control that matters most here. An ordinary index must still be refused or accepted on its own
     * merits, since this check runs before every other prerequisite and a mistake would block all
     * scaling rather than only the computed case.
     */
    public void testAnOrdinaryIndexStillReachesItsOwnPrerequisites() {
        ClusterState state = stateWithPublishedRouting();
        AtomicReference<Exception> failure = new AtomicReference<>();

        new ScaleIndexOperationValidator().validateScalePrerequisites(
            state.metadata().index(INDEX),
            state.routingTable(),
            INDEX,
            listenerCapturing(failure),
            true
        );

        assertNotNull("an ordinary index must be judged on its own prerequisites", failure.get());
        assertFalse(
            "an ordinary index must never be refused for computed placement: " + failure.get().getMessage(),
            failure.get().getMessage().contains("computed rather than published")
        );
    }

    /** The control: an ordinary published index reaches the same code and is handled. */
    public void testAnOrdinaryIndexIsUnaffected() {
        ClusterState state = stateWithPublishedRouting();
        IndexMetadata indexMetadata = state.metadata().index(INDEX);

        Map<ShardId, String> assignments = new ScaleIndexShardSyncManager(null, null, null).getPrimaryShardAssignments(
            indexMetadata,
            state
        );

        assertTrue("an ordinary index must still be read without throwing", assignments.isEmpty());
    }

    // ---------------------------------------------------------------- helpers

    private static
        org.opensearch.core.action.ActionListener<org.opensearch.action.support.clustermanager.AcknowledgedResponse>
        listenerCapturing(AtomicReference<Exception> failure) {
        return org.opensearch.core.action.ActionListener.wrap(response -> {}, failure::set);
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
        // A real (non-EMPTY_ROUTING_TABLE) instance, not the builder's default -- attachIndexRoutingResolver
        // is deliberately a no-op on the shared EMPTY_ROUTING_TABLE singleton (see its own javadoc), so
        // resolving via the new SPI (Phase C4b of core-pluggability-refactor-plan.md) needs an explicit one
        // here, same as production code gets from a real cluster state.
        RoutingTable routingTable = RoutingTable.builder().build();
        routingTable.attachIndexRoutingResolver(new SupplierBackedIndexRoutingResolver());
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata(), false).build())
            .routingTable(routingTable)
            .build();
    }

    private static ClusterState stateWithPublishedRouting() {
        IndexMetadata metadata = indexMetadata();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(metadata, false).build())
            .routingTable(RoutingTable.builder().addAsNew(metadata).build())
            .build();
    }

    private static IndexMetadata indexMetadata() {
        return IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX + "-uuid-0000000000")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
