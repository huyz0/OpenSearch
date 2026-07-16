/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.split.InPlaceSplitShardAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidateEntry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class InPlaceSplitTriggerCoordinatorTests extends OpenSearchTestCase {

    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onResponse(new AcknowledgedResponse(true));
            return null;
        }).when(client).execute(eq(InPlaceSplitShardAction.INSTANCE), any(InPlaceSplitShardAction.Request.class), any());
    }

    private static ClusterState stateWithUnsplitIndex(String indexUuid, String indexName) {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(indexMetadata, false).build()).build();
    }

    private static ClusterState stateWithInProgressSplitIndex(String indexUuid, String indexName) {
        SplitShardsMetadata.Builder splitBuilder = new SplitShardsMetadata.Builder(1);
        splitBuilder.splitShard(0, 2);
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .splitShardsMetadata(splitBuilder.build())
            .build();
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(indexMetadata, false).build()).build();
    }

    public void testSplitsOnlyCandidates() {
        // requiredConsecutiveTicks=1 -- this test is about which entries get acted on, not hysteresis.
        InPlaceSplitTriggerCoordinator coordinator = new InPlaceSplitTriggerCoordinator(client, 1, 0);
        ShardSplitCandidateEntry candidate = new ShardSplitCandidateEntry("uuid-1", 0, "my-index", 900_000L, 0L, true, false);
        ShardSplitCandidateEntry notCandidate = new ShardSplitCandidateEntry("uuid-2", 0, "other-index", 10L, 0L, false, false);

        coordinator.triggerCandidates(List.of(candidate, notCandidate), stateWithUnsplitIndex("uuid-1", "my-index"));

        verify(client, times(1)).execute(eq(InPlaceSplitShardAction.INSTANCE), any(InPlaceSplitShardAction.Request.class), any());
    }

    public void testSplitRequestTargetsCorrectIndexAndShard() {
        InPlaceSplitTriggerCoordinator coordinator = new InPlaceSplitTriggerCoordinator(client, 1, 0);
        ShardSplitCandidateEntry candidate = new ShardSplitCandidateEntry("uuid-1", 2, "my-index", 900_000L, 0L, true, false);

        coordinator.triggerCandidates(List.of(candidate), stateWithUnsplitIndex("uuid-1", "my-index"));

        org.mockito.ArgumentCaptor<InPlaceSplitShardAction.Request> captor = org.mockito.ArgumentCaptor.forClass(
            InPlaceSplitShardAction.Request.class
        );
        verify(client).execute(eq(InPlaceSplitShardAction.INSTANCE), captor.capture(), any());
        assertEquals("my-index", captor.getValue().index());
        assertEquals(2, captor.getValue().shardId());
        assertEquals(InPlaceSplitTriggerCoordinator.SPLIT_INTO, captor.getValue().splitInto());
    }

    public void testDoesNotReSplitAShardAlreadyMidSplit() {
        InPlaceSplitTriggerCoordinator coordinator = new InPlaceSplitTriggerCoordinator(client, 1, 0);
        ShardSplitCandidateEntry candidate = new ShardSplitCandidateEntry("uuid-1", 0, "my-index", 900_000L, 0L, true, false);

        coordinator.triggerCandidates(List.of(candidate), stateWithInProgressSplitIndex("uuid-1", "my-index"));

        verify(client, never()).execute(eq(InPlaceSplitShardAction.INSTANCE), any(InPlaceSplitShardAction.Request.class), any());
    }

    public void testDoesNotSplitAShardWhoseIndexHasSinceBeenDeleted() {
        InPlaceSplitTriggerCoordinator coordinator = new InPlaceSplitTriggerCoordinator(client, 1, 0);
        ShardSplitCandidateEntry candidate = new ShardSplitCandidateEntry("uuid-1", 0, "my-index", 900_000L, 0L, true, false);
        ClusterState emptyState = ClusterState.builder(new ClusterName("test")).build();

        coordinator.triggerCandidates(List.of(candidate), emptyState);

        verify(client, never()).execute(eq(InPlaceSplitShardAction.INSTANCE), any(InPlaceSplitShardAction.Request.class), any());
    }

    public void testASingleOverThresholdTickDoesNotTriggerASplitWhenHysteresisIsConfigured() {
        InPlaceSplitTriggerCoordinator coordinator = new InPlaceSplitTriggerCoordinator(client, 3, 0);
        ShardSplitCandidateEntry candidate = new ShardSplitCandidateEntry("uuid-1", 0, "my-index", 900_000L, 0L, true, false);
        ClusterState state = stateWithUnsplitIndex("uuid-1", "my-index");

        coordinator.triggerCandidates(List.of(candidate), state);
        coordinator.triggerCandidates(List.of(candidate), state);

        verify(client, never()).execute(eq(InPlaceSplitShardAction.INSTANCE), any(InPlaceSplitShardAction.Request.class), any());
    }

    public void testSplitsOnlyOnceTheStreakReachesTheRequiredConsecutiveTickCount() {
        InPlaceSplitTriggerCoordinator coordinator = new InPlaceSplitTriggerCoordinator(client, 3, 0);
        ShardSplitCandidateEntry candidate = new ShardSplitCandidateEntry("uuid-1", 0, "my-index", 900_000L, 0L, true, false);
        ClusterState state = stateWithUnsplitIndex("uuid-1", "my-index");

        coordinator.triggerCandidates(List.of(candidate), state);
        coordinator.triggerCandidates(List.of(candidate), state);
        verify(client, never()).execute(eq(InPlaceSplitShardAction.INSTANCE), any(InPlaceSplitShardAction.Request.class), any());

        coordinator.triggerCandidates(List.of(candidate), state);
        verify(client, times(1)).execute(eq(InPlaceSplitShardAction.INSTANCE), any(InPlaceSplitShardAction.Request.class), any());
    }

    public void testAGapInCandidacyResetsTheStreak() {
        InPlaceSplitTriggerCoordinator coordinator = new InPlaceSplitTriggerCoordinator(client, 3, 0);
        ShardSplitCandidateEntry candidate = new ShardSplitCandidateEntry("uuid-1", 0, "my-index", 900_000L, 0L, true, false);
        ShardSplitCandidateEntry notCandidate = new ShardSplitCandidateEntry("uuid-1", 0, "my-index", 10L, 0L, false, false);
        ClusterState state = stateWithUnsplitIndex("uuid-1", "my-index");

        coordinator.triggerCandidates(List.of(candidate), state);
        coordinator.triggerCandidates(List.of(candidate), state);
        coordinator.triggerCandidates(List.of(notCandidate), state); // resets the streak
        coordinator.triggerCandidates(List.of(candidate), state);
        coordinator.triggerCandidates(List.of(candidate), state);

        // Two more candidate ticks after the reset is only a streak of 2, still short of 3.
        verify(client, never()).execute(eq(InPlaceSplitShardAction.INSTANCE), any(InPlaceSplitShardAction.Request.class), any());
    }

    public void testPerTickBudgetLimitsHowManyShardsSplitInOneCall() {
        InPlaceSplitTriggerCoordinator coordinator = new InPlaceSplitTriggerCoordinator(client, 1, 2);
        ShardSplitCandidateEntry a = new ShardSplitCandidateEntry("uuid-a", 0, "index-a", 100L, 0L, true, false);
        ShardSplitCandidateEntry b = new ShardSplitCandidateEntry("uuid-b", 0, "index-b", 200L, 0L, true, false);
        ShardSplitCandidateEntry c = new ShardSplitCandidateEntry("uuid-c", 0, "index-c", 300L, 0L, true, false);
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(unsplitIndexMetadata("uuid-a", "index-a"), false)
                    .put(unsplitIndexMetadata("uuid-b", "index-b"), false)
                    .put(unsplitIndexMetadata("uuid-c", "index-c"), false)
                    .build()
            )
            .build();

        coordinator.triggerCandidates(List.of(a, b, c), state);

        // Budget=2: only 2 of the 3 sustained candidates actually get split, not all 3.
        verify(client, times(2)).execute(eq(InPlaceSplitShardAction.INSTANCE), any(InPlaceSplitShardAction.Request.class), any());
    }

    public void testPerTickBudgetPrioritizesTheBusiestShardFirst() {
        InPlaceSplitTriggerCoordinator coordinator = new InPlaceSplitTriggerCoordinator(client, 1, 1);
        ShardSplitCandidateEntry quiet = new ShardSplitCandidateEntry("uuid-quiet", 0, "quiet-index", 50L, 0L, true, false);
        ShardSplitCandidateEntry busy = new ShardSplitCandidateEntry("uuid-busy", 0, "busy-index", 5_000_000L, 0L, true, false);
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(unsplitIndexMetadata("uuid-quiet", "quiet-index"), false)
                    .put(unsplitIndexMetadata("uuid-busy", "busy-index"), false)
                    .build()
            )
            .build();

        coordinator.triggerCandidates(List.of(quiet, busy), state);

        org.mockito.ArgumentCaptor<InPlaceSplitShardAction.Request> captor = org.mockito.ArgumentCaptor.forClass(
            InPlaceSplitShardAction.Request.class
        );
        verify(client, times(1)).execute(eq(InPlaceSplitShardAction.INSTANCE), captor.capture(), any());
        assertEquals(
            "with only budget for one, the busiest shard's index must be the one that actually splits",
            "busy-index",
            captor.getValue().index()
        );
    }

    public void testNonPositiveBudgetMeansUnlimited() {
        InPlaceSplitTriggerCoordinator coordinator = new InPlaceSplitTriggerCoordinator(client, 1, 0);
        ShardSplitCandidateEntry a = new ShardSplitCandidateEntry("uuid-a", 0, "index-a", 100L, 0L, true, false);
        ShardSplitCandidateEntry b = new ShardSplitCandidateEntry("uuid-b", 0, "index-b", 200L, 0L, true, false);
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(unsplitIndexMetadata("uuid-a", "index-a"), false)
                    .put(unsplitIndexMetadata("uuid-b", "index-b"), false)
                    .build()
            )
            .build();

        coordinator.triggerCandidates(List.of(a, b), state);

        verify(client, times(2)).execute(eq(InPlaceSplitShardAction.INSTANCE), any(InPlaceSplitShardAction.Request.class), any());
    }

    private static IndexMetadata unsplitIndexMetadata(String indexUuid, String indexName) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
