/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.serverless.storage.scaletozero.ShardSuspensionCoordinator;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidateEntry;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end proof of the suspend/reactivate mechanism (rfc-serverless-opensearch.md &sect;7.3),
 * exercised over a real cluster rather than {@code SuspendedShardAllocationDeciderTests}'s own
 * directly-constructed {@code RoutingAllocation} -- specifically the two things a decider-only unit
 * test cannot prove: that marking a shard suspended genuinely evicts an already-<em>started</em>
 * writer shard from its node (not just blocks a not-yet-assigned one), and that a real write against
 * a suspended shard's index triggers real reactivation end to end.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageShardSuspensionIT extends OpenSearchIntegTestCase {

    private static final String IDX = "shard-suspension-it-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testSuspendingAStartedWriterShardEvictsItAndAWriteReactivatesIt() throws Exception {
        Path basePath = createTempDir("serverless-storage-shard-suspension-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            IDX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(IDX);

        IndexResponse firstIndex = client().prepareIndex(IDX).setSource("field", "value").get();
        assertEquals(org.opensearch.core.rest.RestStatus.CREATED, firstIndex.status());

        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(IDX).getIndexUUID();

        ClusterService clusterManagerClusterService = internalCluster().getInstance(ClusterService.class, clusterManagerNode);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterManagerClusterService, client());
        coordinator.suspendCandidates(List.of(new ScaleToZeroCandidateEntry(indexUuid, 0, 0L, 0L, true)));

        assertBusy(() -> {
            IndexMetadata indexMetadata = clusterManagerClusterService.state().metadata().index(IDX);
            assertTrue("the index's cluster state must record shard 0 as suspended", SuspendedShardsMetadata.isSuspended(indexMetadata, 0));
            assertEquals(
                "a suspended writer shard must actually be evicted (UNASSIGNED), not merely marked",
                ShardRoutingState.UNASSIGNED,
                clusterManagerClusterService.state().routingTable().index(IDX).shard(0).primaryShard().state()
            );
        });

        // A real write against the now-suspended index must reactivate it: ShardReactivationActionFilter
        // clears the suspended marker and triggers a reroute, and core's own existing
        // ClusterStateObserver-driven retry loop inside TransportReplicationAction waits for the shard
        // to become active again rather than failing fast (see this feature's commit message for the
        // research this design is based on).
        IndexResponse secondIndex = client().prepareIndex(IDX).setSource("field", "value-after-reactivation").get();
        assertEquals(org.opensearch.core.rest.RestStatus.CREATED, secondIndex.status());

        assertBusy(() -> {
            IndexMetadata indexMetadata = clusterManagerClusterService.state().metadata().index(IDX);
            assertFalse("reactivation must clear the suspended marker", SuspendedShardsMetadata.isSuspended(indexMetadata, 0));
            assertEquals(
                ShardRoutingState.STARTED,
                clusterManagerClusterService.state().routingTable().index(IDX).shard(0).primaryShard().state()
            );
        });

        refresh(IDX);
        assertEquals(2L, client().prepareSearch(IDX).setSize(0).get().getHits().getTotalHits().value());
    }
}
