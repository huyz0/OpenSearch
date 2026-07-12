/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.serverless.storage.scaleup.ReaderReplicaExpansionCoordinator;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesAction;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesRequest;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * End-to-end proof of scale-up (see the RFC's scale-up autoscaling subsection): real search
 * traffic against a real reader shard drives {@code ObjectStoreReaderEngine#queriesPerMinute()}
 * up, a cluster-wide {@link ScaleUpCandidatesAction} evaluation (exactly what {@code
 * ScaleUpCandidatesSchedulerTask}'s own tick would run) picks that shard up as a candidate against
 * a deliberately low threshold, and {@link ReaderReplicaExpansionCoordinator} actually bumps {@code
 * index.number_of_search_replicas} -- the same "drive the transport action's own merged output
 * into the coordinator directly, rather than going through the scheduler's private {@code
 * evaluateForTesting()}" shape {@code ServerlessStorageReaderShardSuspensionIT} already uses for
 * scale-to-zero.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageReaderScaleUpIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "serverless-reader-scaleup-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(ServerlessStoragePlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private volatile Path sharedBasePath;

    private Path serverlessStorageBasePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Path sharedPath = serverlessStorageBasePath();
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), sharedPath.toString())
            .build();
    }

    public void testBusyReaderShardTriggersASearchReplicaExpansion() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureYellow(INDEX_NAME);

        internalCluster().startNode(
            Settings.builder()
                .put(nodeSettings(0))
                .put("node.roles", "search")
                .put("node.attr." + ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE, "true")
                .build()
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        refresh(INDEX_NAME);

        // The reader shard's own materialized copy may not have caught up with the manifest the
        // instant the primary flushed -- same "wait for it to actually be visible" reasoning
        // ServerlessStorageReaderShardSuspensionIT already needs before its own first search.
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setPreference(Preference.SEARCH_REPLICA.type()).setSize(0).get();
            assertHitCount(response, 1);
        }, 30, TimeUnit.SECONDS);

        // ObjectStoreReaderEngine#queriesPerMinute() deliberately only ever reports the previous
        // *completed* 60-second window (see that method's own javadoc for why), and a window only
        // rolls over when a query lands after it has run long enough -- so proving a real,
        // non-just-reset queries-per-minute reading genuinely requires spanning a real 60-second
        // window with query traffic on both sides of the rollover, not just firing a quick burst.
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(75);
        while (System.currentTimeMillis() < deadline) {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setPreference(Preference.SEARCH_REPLICA.type()).setSize(0).get();
            assertHitCount(response, 1);
            Thread.sleep(5000);
        }

        ClusterService clusterManagerClusterService = internalCluster().getInstance(
            ClusterService.class,
            internalCluster().getClusterManagerName()
        );

        // qpmThreshold=0 -- any nonzero completed-window count is enough to be a candidate for
        // this test's purposes, since the point is proving the mechanism wires together end to
        // end, not exercising the real default threshold's exact value.
        assertBusy(() -> {
            ScaleUpCandidatesResponse response = client().execute(ScaleUpCandidatesAction.INSTANCE, new ScaleUpCandidatesRequest(0L, 5))
                .actionGet();
            boolean anyCandidate = response.candidates().stream().anyMatch(c -> c.indexName().equals(INDEX_NAME) && c.candidate());
            if (anyCandidate == false) {
                // Force another completed query-rate window by sending one more burst, then retry.
                client().prepareSearch(INDEX_NAME).setPreference(Preference.SEARCH_REPLICA.type()).setSize(0).get();
            }
            assertTrue("the busy reader shard must eventually be flagged a scale-up candidate", anyCandidate);

            ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client(), 5);
            coordinator.expandCandidates(response.candidates());
            // ObjectStoreReaderEngine#queriesPerMinute() only ever reports the previous *completed*
            // 60-second window (see its own javadoc for why), so this assertBusy genuinely needs
            // slack past one real window to pass reliably.
        }, 90, TimeUnit.SECONDS);

        assertBusy(() -> {
            IndexMetadata indexMetadata = clusterManagerClusterService.state().metadata().index(INDEX_NAME);
            assertTrue(
                "a busy reader shard must have its search-replica count expanded past its original 1",
                indexMetadata.getNumberOfSearchOnlyReplicas() > 1
            );
        }, 30, TimeUnit.SECONDS);
    }
}
