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
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(
            mock(org.opensearch.cluster.service.ClusterService.class),
            client
        );

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
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(
            mock(org.opensearch.cluster.service.ClusterService.class),
            client
        );

        coordinator.evictForTesting(state, indexUuid(state), 0, false);

        org.mockito.ArgumentCaptor<ClusterRerouteRequest> captor = org.mockito.ArgumentCaptor.forClass(ClusterRerouteRequest.class);
        verify(clusterAdminClient, times(1)).reroute(captor.capture(), any());
        assertEquals(1, captor.getValue().getCommands().commands().size());
    }

    /**
     * Regression test: suspendCandidates/suspendReaderCandidates call suspend() once per candidate
     * on every scheduler tick regardless of whether it's already suspended -- suspend() used to
     * unconditionally submit a ClusterStateUpdateTask every time, even though most ticks in steady
     * state find nothing new to do (the task's own execute() would just no-op). The cheap pre-check
     * against already-known state must skip the submission entirely in that case.
     */
    public void testSuspendSkipsSubmittingAClusterStateTaskWhenAlreadySuspended() {
        ClusterState state = buildClusterState(0);
        IndexMetadata alreadySuspended = SuspendedShardsMetadata.withShardSuspended(state.metadata().index(INDEX_NAME), 0);
        ClusterState suspendedState = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).put(alreadySuspended, true))
            .build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(suspendedState);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterService, client);

        coordinator.suspendWriterShard(indexUuid(suspendedState), 0);

        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    /**
     * Regression test: clusterStateProcessed's own eviction call only ever fires once, on the tick
     * that actually flips the suspended flag -- if that one attempt is lost (a cluster-manager
     * failover between the cluster-state commit and the reroute call, or the reroute call itself
     * failing), the shard stays marked suspended forever with an assigned copy still STARTED, and
     * nothing used to ever retry it (suspend()'s own pre-check just returned). The already-suspended
     * pre-check path must reconcile by re-attempting eviction, not merely skip the redundant
     * cluster-state submission.
     */
    public void testSuspendRetriesEvictionWhenAlreadySuspendedButAnAssignedCopySurvived() {
        ClusterState state = buildClusterState(0);
        IndexMetadata alreadySuspended = SuspendedShardsMetadata.withShardSuspended(state.metadata().index(INDEX_NAME), 0);
        ClusterState suspendedButStillAssignedState = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).put(alreadySuspended, true))
            .build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(suspendedButStillAssignedState);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterService, client);

        coordinator.suspendWriterShard(indexUuid(suspendedButStillAssignedState), 0);

        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        // A suspended shard with a surviving assigned copy must have its lost eviction retried.
        org.mockito.ArgumentCaptor<ClusterRerouteRequest> captor = org.mockito.ArgumentCaptor.forClass(ClusterRerouteRequest.class);
        verify(clusterAdminClient, times(1)).reroute(captor.capture(), any());
        assertEquals(1, captor.getValue().getCommands().commands().size());
        assertTrue(captor.getValue().getCommands().commands().get(0) instanceof CancelAllocationCommand);
    }

    /** Control: when the shard is genuinely not yet suspended, the pre-check must not skip the real submission. */
    public void testSuspendStillSubmitsAClusterStateTaskWhenNotYetSuspended() {
        ClusterState state = buildClusterState(0);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterService, client);

        coordinator.suspendWriterShard(indexUuid(state), 0);

        verify(clusterService, times(1)).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    /**
     * The routing entry is per-index but suspension is per-shard-per-role, so pruning may only fire
     * when the whole index is cold. A partially suspended index that lost its entry would take its
     * still-serving shards down with it, which is the failure this predicate exists to prevent.
     */
    public void testPruningRequiresEveryShardOfTheIndexToBeSuspended() {
        ClusterState halfSuspended = coldState(2, 0, ShardRoutingState.UNASSIGNED, true, false);
        assertFalse(
            "one suspended shard out of two is not a cold index",
            ShardSuspensionCoordinator.isFullyColdAndEvictedForTesting(halfSuspended, halfSuspended.metadata().index(INDEX_NAME))
        );

        ClusterState fullySuspended = coldState(2, 0, ShardRoutingState.UNASSIGNED, true, true);
        assertTrue(ShardSuspensionCoordinator.isFullyColdAndEvictedForTesting(fullySuspended, fullySuspended.metadata().index(INDEX_NAME)));
    }

    /**
     * Eviction is an asynchronous reroute, so the suspended marker commits well before the copies
     * are actually unassigned. Pruning on the marker alone would drop the entry out from under a
     * still-assigned shard.
     */
    public void testPruningWaitsForEvictionToSettle() {
        ClusterState markedButStillAssigned = coldState(1, 0, ShardRoutingState.STARTED, true, true);
        assertFalse(
            "a suspended-but-still-STARTED copy means eviction has not happened yet",
            ShardSuspensionCoordinator.isFullyColdAndEvictedForTesting(
                markedButStillAssigned,
                markedButStillAssigned.metadata().index(INDEX_NAME)
            )
        );
    }

    /**
     * An index with search-only replicas is not cold until the reader role is suspended too --
     * otherwise a reader copy is still expected to serve queries. An index with none configured must
     * not be held back waiting for a role it does not have.
     */
    public void testPruningAccountsForTheReaderRoleOnlyWhenSearchReplicasAreConfigured() {
        ClusterState writerOnlySuspended = coldState(1, 1, ShardRoutingState.UNASSIGNED, true, true);
        assertFalse(
            "search replicas are configured, so writer-only suspension is not a cold index",
            ShardSuspensionCoordinator.isFullyColdAndEvictedForTesting(
                writerOnlySuspended,
                writerOnlySuspended.metadata().index(INDEX_NAME)
            )
        );

        IndexMetadata bothRoles = SuspendedShardsMetadata.withReaderShardSuspended(writerOnlySuspended.metadata().index(INDEX_NAME), 0);
        ClusterState state = ClusterState.builder(writerOnlySuspended)
            .metadata(Metadata.builder(writerOnlySuspended.metadata()).put(bothRoles, true))
            .build();
        assertTrue(ShardSuspensionCoordinator.isFullyColdAndEvictedForTesting(state, state.metadata().index(INDEX_NAME)));
    }

    /** Off by default: the same fully cold state must produce no cluster-state task at all. */
    public void testPruningIsOffByDefault() {
        ClusterState state = coldState(1, 0, ShardRoutingState.UNASSIGNED, true, true);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        new ShardSuspensionCoordinator(clusterService, client, 0L).maybePruneRoutingEntryForTesting(state, indexUuid(state));

        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    /** Enabled, the task is submitted and its execute() actually removes the entry. */
    public void testPruningRemovesTheRoutingEntryWhenEnabled() throws Exception {
        ClusterState state = coldState(1, 0, ShardRoutingState.UNASSIGNED, true, true);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        new ShardSuspensionCoordinator(clusterService, client, 0L, true).maybePruneRoutingEntryForTesting(state, indexUuid(state));

        org.mockito.ArgumentCaptor<ClusterStateUpdateTask> captor = org.mockito.ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService, times(1)).submitStateUpdateTask(anyString(), captor.capture());

        ClusterState pruned = captor.getValue().execute(state);
        assertFalse("the cold index must no longer have a routing entry", pruned.routingTable().hasIndex(INDEX_NAME));
        assertTrue(
            "...and must still be in metadata, which is what makes it cold rather than gone",
            pruned.metadata().hasIndex(INDEX_NAME)
        );
    }

    /**
     * Builds a state for one index with {@code shards} shards, optionally search-only replicas, each
     * shard's writer copy in {@code writerState}, and writer suspension applied to shard 0 (and, when
     * {@code suspendAll}, to every shard).
     */
    private static ClusterState coldState(
        int shards,
        int searchReplicas,
        ShardRoutingState writerState,
        boolean suspendFirst,
        boolean suspendAll
    ) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true);
        if (searchReplicas > 0) {
            settings.put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, searchReplicas);
        }
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX_NAME).settings(settings.build()).build();
        for (int i = 0; i < shards; i++) {
            if (i == 0 ? suspendFirst : suspendAll) {
                indexMetadata = SuspendedShardsMetadata.withShardSuspended(indexMetadata, i);
            }
        }

        Index index = indexMetadata.getIndex();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
        for (int i = 0; i < shards; i++) {
            ShardId shardId = new ShardId(index, i);
            IndexShardRoutingTable.Builder shardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
            shardRoutingBuilder.addShard(
                writerState == ShardRoutingState.UNASSIGNED
                    ? TestShardRouting.newShardRouting(shardId, null, true, ShardRoutingState.UNASSIGNED)
                    : TestShardRouting.newShardRouting(shardId, "node-primary", true, writerState)
            );
            indexRoutingTable.addIndexShard(shardRoutingBuilder.build());
        }
        routingTable.add(indexRoutingTable.build());

        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(routingTable.build())
            .build();
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
        shardRoutingBuilder.addShard(TestShardRouting.newShardRouting(shardId, "node-primary", true, ShardRoutingState.STARTED));
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
