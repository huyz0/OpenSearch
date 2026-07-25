/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * A7.6. A1 found sixteen call sites that dereferenced a routing lookup without checking, and A7.2
 * guarded four of them. Three of those four are {@code shards()} overrides that a unit test cannot
 * reach without disproportionate scaffolding, so they shipped covered by inspection alone. This is
 * the coverage that closes that gap.
 *
 * <p>The state under test -- an index present in metadata and absent from the routing table -- has no
 * API that produces it, which is why it needed an integration test rather than a unit one. The
 * cluster-manager's own {@code ClusterService} can be driven directly from the test to remove the
 * entry, which is the same technique {@code RareClusterStateIT} uses to inject one.
 *
 * <p>Each assertion here is "reports no shard available", never "throws NullPointerException". Before
 * A7.2 all three threw.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class RoutingAbsentIndexRequestPathsIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "cold-index";

    public void testAnalyzeReportsNoShardAvailableRatherThanThrowing() throws Exception {
        createColdIndex();

        expectThrows(NoShardAvailableActionException.class, () -> client().admin().indices().prepareAnalyze(INDEX, "some text").get());
    }

    public void testGetFieldMappingsReportsNoShardAvailableRatherThanThrowing() throws Exception {
        createColdIndex();

        // The broadcast wrapper reports per-index failures rather than throwing, so the assertion is
        // that the call completes at all and reports nothing for the cold index -- the pre-A7.2
        // behaviour was a NullPointerException out of shards().
        var response = client().admin().indices().prepareGetFieldMappings(INDEX).setFields("*").get();
        assertTrue(
            "a cold index has no shard to answer from, so no mappings come back",
            response.mappings().isEmpty() || response.mappings().get(INDEX) == null || response.mappings().get(INDEX).isEmpty()
        );
    }

    public void testUpdateRetriesRatherThanThrowing() throws Exception {
        createColdIndex();

        // TransportUpdateAction's shards() returns an empty iterator, which
        // TransportInstanceSingleOperationAction turns into a retry that ends in its own timeout
        // rather than a NullPointerException. A short timeout keeps the test quick; the point is
        // which exception comes back, not how long it waits.
        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareUpdate(INDEX, "1").setDoc("field", "value").setTimeout("1s").get()
        );
        assertFalse(
            "the failure must not be a NullPointerException: " + e,
            e instanceof NullPointerException || e.getCause() instanceof NullPointerException
        );
    }

    /**
     * Creates an ordinary index, waits for it to be green, then removes its {@link IndexRoutingTable}
     * entry while leaving its metadata in place.
     *
     * <p>Nothing in core produces this state on its own -- that is the whole reason A5 exists -- so the
     * test manufactures it against the cluster-manager's own state.
     */
    private void createColdIndex() throws Exception {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                org.opensearch.common.settings.Settings.builder()
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
        );
        ensureGreen(INDEX);

        String clusterManagerName = internalCluster().getClusterManagerName();
        ClusterService clusterService = internalCluster().clusterService(clusterManagerName);

        CountDownLatch applied = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test-remove-routing-entry", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return ClusterState.builder(currentState)
                    .routingTable(RoutingTable.builder(currentState.routingTable()).remove(INDEX).build())
                    .build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                applied.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                applied.countDown();
                throw new AssertionError("failed to remove the routing entry", e);
            }
        });
        assertTrue("cluster state update did not apply", applied.await(30, TimeUnit.SECONDS));

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertTrue("precondition: the index is still in metadata", state.metadata().hasIndex(INDEX));
            assertFalse("precondition: and gone from routing", state.routingTable().hasIndex(INDEX));
        });
    }
}
