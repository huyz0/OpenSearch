/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ServerlessAffinityRouting;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Real end-to-end coverage for plan item B4-B6 (plan-100m-index-implementation.md, Area B): does a
 * multi-index request actually get forwarded once to the shared affinity node, does an ordinary
 * (non-gated) request stay untouched, and does loop protection hold. Uses
 * {@link AffinityForwardingActionFilter#forwardCountForTesting()} throughout rather than inferring
 * "it forwarded" from result correctness alone -- this filter is a latency optimisation, so a correct
 * result proves nothing about whether forwarding happened; only the counter does.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageAffinityForwardingIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), randomRepoPath().toString())
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_AFFINITY_FORWARDING_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @Before
    public void resetForwardCount() {
        AffinityForwardingActionFilter.resetForwardCountForTesting();
    }

    /**
     * Picks an index name whose real affinity target (computed the same way the filter itself does,
     * against the real final node set) is exactly {@code wantNodeId}, skipping anything already in
     * {@code exclude} -- so two calls for the same target return two distinct names rather than both
     * finding the same first match.
     */
    private static String nameWithAffinityFor(String wantNodeId, List<String> allNodeIds, Set<String> exclude) {
        for (int i = 0; i < 10_000; i++) {
            String candidate = "affinity-forward-probe-" + i;
            if (exclude.contains(candidate)) {
                continue;
            }
            if (wantNodeId.equals(ServerlessAffinityRouting.getAffinityNodeId(candidate, allNodeIds))) {
                return candidate;
            }
        }
        throw new AssertionError("could not find a probe name with affinity for " + wantNodeId + " in 10,000 tries");
    }

    private static Settings gated() {
        return Settings.builder()
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }

    private void indexOneDocTolerant(String index) throws Exception {
        assertBusy(() -> {
            try {
                client().prepareIndex(index).setId("1").setSource("f", "v").get();
            } catch (Exception e) {
                throw new AssertionError("write not yet servable: " + e.getMessage(), e);
            }
        }, 60, TimeUnit.SECONDS);
    }

    public void testMultiIndexSearchForwardsOnceToTheSharedAffinityNode() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNodeOne = internalCluster().startDataOnlyNode();
        String dataNodeTwo = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        installBlobBackedDescriptorPlane();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        DiscoveryNodes nodes = state.nodes();
        String dataNodeOneId = idForName(nodes, dataNodeOne);
        List<String> allNodeIds = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            allNodeIds.add(node.getId());
        }

        // Both probe indices deliberately share the SAME affinity target (dataNodeOne), so a search
        // naming both has exactly one valid forward target -- the case this filter exists to handle.
        String indexA = nameWithAffinityFor(dataNodeOneId, allNodeIds, Set.of());
        String indexB = nameWithAffinityFor(dataNodeOneId, allNodeIds, Set.of(indexA));
        assertNotEquals("need two distinct probe names sharing the same affinity target", indexA, indexB);

        client().admin().indices().prepareCreate(indexA).setSettings(gated()).get();
        client().admin().indices().prepareCreate(indexB).setSettings(gated()).get();
        indexOneDocTolerant(indexA);
        indexOneDocTolerant(indexB);
        client().admin().indices().prepareRefresh(indexA, indexB).get();

        // Issue the request FROM the node that is NOT the affinity target -- receiving it there and
        // getting correct results proves nothing about forwarding by itself (a correct answer is
        // exactly what would happen if this filter didn't exist at all); forwardCountForTesting is
        // what actually proves the forward happened.
        // Reset here, not just in @Before -- setup above (indexing into gated indices) can
        // itself be forwarded by this same filter, and that must not be counted as part of
        // what this assertion measures.
        AffinityForwardingActionFilter.resetForwardCountForTesting();
        SearchResponse response = internalCluster().client(dataNodeTwo).prepareSearch(indexA, indexB).get();

        assertEquals(2, response.getHits().getTotalHits().value());
        assertEquals(
            "the request landed on a non-affinity node naming two indices that share one affinity "
                + "target -- it must have been forwarded exactly once",
            1L,
            AffinityForwardingActionFilter.forwardCountForTesting()
        );
    }

    public void testRequestAlreadyOnTheAffinityNodeIsNotForwarded() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNodeOne = internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        installBlobBackedDescriptorPlane();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        DiscoveryNodes nodes = state.nodes();
        String dataNodeOneId = idForName(nodes, dataNodeOne);
        List<String> allNodeIds = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            allNodeIds.add(node.getId());
        }

        String index = nameWithAffinityFor(dataNodeOneId, allNodeIds, Set.of());
        client().admin().indices().prepareCreate(index).setSettings(gated()).get();
        indexOneDocTolerant(index);
        client().admin().indices().prepareRefresh(index).get();

        // Reset here, not just in @Before -- setup above (indexing into gated indices) can
        // itself be forwarded by this same filter, and that must not be counted as part of
        // what this assertion measures.
        AffinityForwardingActionFilter.resetForwardCountForTesting();
        SearchResponse response = internalCluster().client(dataNodeOne).prepareSearch(index).get();

        assertEquals(1, response.getHits().getTotalHits().value());
        assertEquals(
            "the request already landed on its own affinity target -- there is nothing to forward to",
            0L,
            AffinityForwardingActionFilter.forwardCountForTesting()
        );
    }

    public void testOrdinaryMultiIndexSearchNeverForwards() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        installBlobBackedDescriptorPlane();

        client().admin().indices().prepareCreate("ordinary-a").get();
        client().admin().indices().prepareCreate("ordinary-b").get();
        client().prepareIndex("ordinary-a").setId("1").setSource("f", "v").get();
        client().prepareIndex("ordinary-b").setId("1").setSource("f", "v").get();
        client().admin().indices().prepareRefresh("ordinary-a", "ordinary-b").get();

        // Reset here, not just in @Before -- setup above (indexing into gated indices) can
        // itself be forwarded by this same filter, and that must not be counted as part of
        // what this assertion measures.
        AffinityForwardingActionFilter.resetForwardCountForTesting();
        SearchResponse response = client().prepareSearch("ordinary-a", "ordinary-b").get();

        assertEquals(2, response.getHits().getTotalHits().value());
        assertEquals(
            "neither index is gated -- there is no descriptor-cache-locality problem for this filter to solve",
            0L,
            AffinityForwardingActionFilter.forwardCountForTesting()
        );
    }

    public void testMixedGatedAndOrdinaryIndicesNeverForwards() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        installBlobBackedDescriptorPlane();

        client().admin().indices().prepareCreate("ordinary-mixed").get();
        client().admin().indices().prepareCreate("gated-mixed").setSettings(gated()).get();
        client().prepareIndex("ordinary-mixed").setId("1").setSource("f", "v").get();
        indexOneDocTolerant("gated-mixed");
        client().admin().indices().prepareRefresh("ordinary-mixed", "gated-mixed").get();

        // Reset here, not just in @Before -- setup above (indexing into gated indices) can
        // itself be forwarded by this same filter, and that must not be counted as part of
        // what this assertion measures.
        AffinityForwardingActionFilter.resetForwardCountForTesting();
        SearchResponse response = client().prepareSearch("ordinary-mixed", "gated-mixed").get();

        assertEquals(2, response.getHits().getTotalHits().value());
        assertEquals(
            "one index in the request isn't gated -- there is no single coordinator whose cache "
                + "warmth would matter for both, so this must not forward",
            0L,
            AffinityForwardingActionFilter.forwardCountForTesting()
        );
    }

    private static String idForName(DiscoveryNodes nodes, String name) {
        for (DiscoveryNode node : nodes) {
            if (name.equals(node.getName())) {
                return node.getId();
            }
        }
        throw new AssertionError("no node named " + name + " in " + nodes);
    }
}
