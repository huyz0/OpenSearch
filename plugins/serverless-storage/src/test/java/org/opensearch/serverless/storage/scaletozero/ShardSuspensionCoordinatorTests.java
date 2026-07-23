/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardSuspensionCoordinatorTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "suspend-coordinator-idx";

    private Client client;
    private ClusterAdminClient clusterAdminClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
    }

    /**
     * Regression test: writer suspension must evict every writer-role copy, not just the primary --
     * a serverless-storage index may have {@code index.number_of_replicas > 0}, and {@code evict}
     * used to build its cancel list from only {@code shardRoutingTable.primaryShard()}, leaving any
     * ordinary replica permanently STARTED (nothing else would ever evict it, per this class's own
     * "no canRemain=NO-only eviction" javadoc).
     */
    public void testEvictWriterCancelsThePrimaryAndEveryOrdinaryReplica() {
        ClusterState state = buildClusterState(1);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(mock(org.opensearch.cluster.service.ClusterService.class), client);

        coordinator.evictForTesting(state, indexUuid(state), 0, false);

        org.mockito.ArgumentCaptor<ClusterRerouteRequest> captor = org.mockito.ArgumentCaptor.forClass(ClusterRerouteRequest.class);
        verify(clusterAdminClient, times(1)).reroute(captor.capture(), any());
        assertEquals(
            "both the primary and the one ordinary replica must be cancelled",
            2,
            captor.getValue().getCommands().commands().size()
        );
        for (Object command : captor.getValue().getCommands().commands()) {
            assertTrue(command instanceof CancelAllocationCommand);
        }
    }

    /** With zero replicas configured, only the primary is there to evict -- the pre-existing behavior must still hold. */
    public void testEvictWriterCancelsOnlyThePrimaryWhenThereAreNoReplicas() {
        ClusterState state = buildClusterState(0);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(mock(org.opensearch.cluster.service.ClusterService.class), client);

        coordinator.evictForTesting(state, indexUuid(state), 0, false);

        org.mockito.ArgumentCaptor<ClusterRerouteRequest> captor = org.mockito.ArgumentCaptor.forClass(ClusterRerouteRequest.class);
        verify(clusterAdminClient, times(1)).reroute(captor.capture(), any());
        assertEquals(1, captor.getValue().getCommands().commands().size());
    }

    private static String indexUuid(ClusterState state) {
        return state.metadata().index(INDEX_NAME).getIndexUUID();
    }

    /** Builds a cluster state with a started primary (and, if {@code numberOfReplicas > 0}, one started ordinary replica) for shard 0. */
    private static ClusterState buildClusterState(int numberOfReplicas) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX_NAME).settings(settings).build();
        Index index = indexMetadata.getIndex();
        ShardId shardId = new ShardId(index, 0);

        IndexShardRoutingTable.Builder shardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
        shardRoutingBuilder.addShard(
            TestShardRouting.newShardRouting(shardId, "node-primary", true, ShardRoutingState.STARTED)
        );
        List<String> replicaNodes = numberOfReplicas > 0 ? List.of("node-replica") : List.of();
        for (String node : replicaNodes) {
            shardRoutingBuilder.addShard(TestShardRouting.newShardRouting(shardId, node, false, ShardRoutingState.STARTED));
        }

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addIndexShard(shardRoutingBuilder.build()).build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();
        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();

        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();
    }
}
