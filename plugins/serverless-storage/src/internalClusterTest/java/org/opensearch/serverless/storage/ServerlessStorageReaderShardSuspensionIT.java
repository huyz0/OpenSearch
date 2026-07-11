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
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.serverless.storage.scaletozero.ShardSuspensionCoordinator;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidateEntry;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * End-to-end proof of reader-shard suspend/reactivate (rfc-serverless-opensearch.md &sect;7.3),
 * exercised over the same real search-only-replica cluster topology {@code
 * ServerlessStorageSearchOnlyReplicaIT} already proved a reader engine serves real reads under --
 * specifically the two things a unit test alone cannot prove: that suspending a reader copy
 * genuinely evicts an already-started search-only shard, and that a real search against a fully-
 * suspended reader shard is never a client-visible failure (the explicit product decision this
 * feature is built around) despite core itself providing no retry-on-unassigned for search-only
 * shard routing.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageReaderShardSuspensionIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "serverless-reader-suspension-idx";

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
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), sharedPath.toString())
            .build();
    }

    public void testSuspendingAStartedReaderShardEvictsItAndASearchReactivatesItWithoutFailing() throws Exception {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
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

        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setPreference(Preference.SEARCH_REPLICA.type()).setSize(0).get();
            assertHitCount(response, 1);
        }, 30, TimeUnit.SECONDS);

        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();
        ClusterService clusterManagerClusterService = internalCluster().getInstance(ClusterService.class, clusterManagerNode);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterManagerClusterService, client());
        coordinator.suspendReaderCandidates(List.of(new ScaleToZeroCandidateEntry(indexUuid, 0, 0L, 0L, false, 0L, true)));

        assertBusy(() -> {
            IndexMetadata indexMetadata = clusterManagerClusterService.state().metadata().index(INDEX_NAME);
            assertTrue(
                "the index's cluster state must record shard 0's reader copy as suspended",
                SuspendedShardsMetadata.isReaderSuspended(indexMetadata, 0)
            );
            boolean anyReaderAssigned = clusterManagerClusterService.state()
                .routingTable()
                .index(INDEX_NAME)
                .shard(0)
                .searchOnlyReplicas()
                .stream()
                .anyMatch(r -> r.unassigned() == false);
            assertFalse("a suspended reader shard must actually be evicted (UNASSIGNED), not merely marked", anyReaderAssigned);
        }, 30, TimeUnit.SECONDS);

        // The core assertion this whole feature exists for: a search against the now fully-
        // suspended reader shard must succeed and return the correct result, not throw
        // NoShardAvailableActionException -- ShardReactivationActionFilter's own bounded
        // ClusterStateObserver wait (there is no core-provided retry for search-only shard
        // routing, confirmed by direct research before this was built) is what makes that true.
        SearchResponse response = client().prepareSearch(INDEX_NAME).setPreference(Preference.SEARCH_REPLICA.type()).setSize(0).get();
        assertHitCount(response, 1);

        assertBusy(() -> {
            IndexMetadata indexMetadata = clusterManagerClusterService.state().metadata().index(INDEX_NAME);
            assertFalse("reactivation must clear the reader suspended marker", SuspendedShardsMetadata.isReaderSuspended(indexMetadata, 0));
            boolean anyReaderStarted = clusterManagerClusterService.state()
                .routingTable()
                .index(INDEX_NAME)
                .shard(0)
                .searchOnlyReplicas()
                .stream()
                .anyMatch(r -> r.state() == ShardRoutingState.STARTED);
            assertTrue("the reader shard must be reactivated (STARTED) again", anyReaderStarted);
        }, 30, TimeUnit.SECONDS);
    }
}
