/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup;

import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidateEntry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReaderReplicaExpansionCoordinatorTests extends OpenSearchTestCase {

    private Client client;
    private IndicesAdminClient indicesAdminClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        indicesAdminClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
    }

    public void testExpandsOnlyCandidatesByOneStep() {
        // requiredConsecutiveTicks=1 -- this test is about which entries get acted on, not hysteresis.
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);
        ScaleUpCandidateEntry notCandidate = new ScaleUpCandidateEntry("uuid-2", 0, "other-index", 10L, 1, false);

        coordinator.expandCandidates(List.of(candidate, notCandidate));

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testDedupesMultipleShardsOfSameIndexIntoOneUpdate() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0);
        ScaleUpCandidateEntry shard0 = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);
        ScaleUpCandidateEntry shard1 = new ScaleUpCandidateEntry("uuid-1", 1, "my-index", 700L, 1, true);

        coordinator.expandCandidates(List.of(shard0, shard1));

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testNeverExceedsConfiguredCap() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 3, 1, 0);
        // Already at the cap: candidate() should never have been true for this in real use, but the
        // coordinator's own second guard must still refuse to act on it.
        ScaleUpCandidateEntry atCap = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 3, true);

        coordinator.expandCandidates(List.of(atCap));

        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testUpdateRequestTargetsCorrectIndexAndReplicaCount() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));

        org.mockito.ArgumentCaptor<UpdateSettingsRequest> captor = org.mockito.ArgumentCaptor.forClass(UpdateSettingsRequest.class);
        verify(indicesAdminClient).updateSettings(captor.capture(), any());
        UpdateSettingsRequest request = captor.getValue();
        assertArrayEquals(new String[] { "my-index" }, request.indices());
        assertEquals("2", request.settings().get(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS));
    }

    public void testASingleOverThresholdTickDoesNotTriggerExpansionWhenHysteresisIsConfigured() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 3, 0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));
        coordinator.expandCandidates(List.of(candidate));

        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testExpandsOnlyOnceTheStreakReachesTheRequiredConsecutiveTickCount() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 3, 0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));
        coordinator.expandCandidates(List.of(candidate));
        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());

        coordinator.expandCandidates(List.of(candidate));
        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testAGapInCandidacyResetsTheStreak() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 3, 0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);
        ScaleUpCandidateEntry notCandidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 10L, 1, false);

        coordinator.expandCandidates(List.of(candidate));
        coordinator.expandCandidates(List.of(candidate));
        coordinator.expandCandidates(List.of(notCandidate)); // resets the streak
        coordinator.expandCandidates(List.of(candidate));
        coordinator.expandCandidates(List.of(candidate));

        // Two more candidate ticks after the reset is only a streak of 2, still short of 3.
        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testDefaultConstructorArgumentOfOneRestoresOriginalSingleTickBehavior() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testPerTickExpansionBudgetLimitsHowManyIndicesExpandInOneCall() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 2);
        ScaleUpCandidateEntry a = new ScaleUpCandidateEntry("uuid-a", 0, "index-a", 100L, 1, true);
        ScaleUpCandidateEntry b = new ScaleUpCandidateEntry("uuid-b", 0, "index-b", 200L, 1, true);
        ScaleUpCandidateEntry c = new ScaleUpCandidateEntry("uuid-c", 0, "index-c", 300L, 1, true);

        coordinator.expandCandidates(List.of(a, b, c));

        // Budget=2: only 2 of the 3 sustained candidates actually get an UpdateSettingsRequest,
        // not all 3 -- proves the cap is real, not merely accepted and ignored.
        verify(indicesAdminClient, times(2)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testPerTickExpansionBudgetPrioritizesTheBusiestShardFirst() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 1);
        ScaleUpCandidateEntry quiet = new ScaleUpCandidateEntry("uuid-quiet", 0, "quiet-index", 50L, 1, true);
        ScaleUpCandidateEntry busy = new ScaleUpCandidateEntry("uuid-busy", 0, "busy-index", 5000L, 1, true);

        coordinator.expandCandidates(List.of(quiet, busy));

        org.mockito.ArgumentCaptor<UpdateSettingsRequest> captor = org.mockito.ArgumentCaptor.forClass(UpdateSettingsRequest.class);
        verify(indicesAdminClient, times(1)).updateSettings(captor.capture(), any());
        assertArrayEquals(
            "with only budget for one, the busiest shard's index must be the one that actually expands",
            new String[] { "busy-index" },
            captor.getValue().indices()
        );
    }

    public void testCandidatesThatLoseTheBudgetKeepTheirStreakForTheNextTick() {
        // requiredConsecutiveTicks=1 and budget=1: "quiet" qualifies every tick but never wins the
        // budget against "busy" -- its streak must stay intact (not reset) so it doesn't have to
        // re-qualify from scratch once it finally does win.
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 1);
        ScaleUpCandidateEntry quiet = new ScaleUpCandidateEntry("uuid-quiet", 0, "quiet-index", 50L, 1, true);
        ScaleUpCandidateEntry busy = new ScaleUpCandidateEntry("uuid-busy", 0, "busy-index", 5000L, 1, true);

        coordinator.expandCandidates(List.of(quiet, busy));
        verify(indicesAdminClient, never()).updateSettings(
            org.mockito.ArgumentMatchers.argThat(r -> r != null && java.util.Arrays.asList(r.indices()).contains("quiet-index")),
            any()
        );

        // Now alone (busy already expanded and is no longer reported as a candidate this tick):
        // quiet must expand immediately, proving its earlier loss didn't reset anything.
        coordinator.expandCandidates(List.of(quiet));
        verify(indicesAdminClient, times(1)).updateSettings(
            org.mockito.ArgumentMatchers.argThat(r -> r != null && java.util.Arrays.asList(r.indices()).contains("quiet-index")),
            any()
        );
    }

    public void testNonPositiveBudgetMeansUnlimited() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0);
        List<ScaleUpCandidateEntry> candidates = List.of(
            new ScaleUpCandidateEntry("uuid-a", 0, "index-a", 100L, 1, true),
            new ScaleUpCandidateEntry("uuid-b", 0, "index-b", 200L, 1, true),
            new ScaleUpCandidateEntry("uuid-c", 0, "index-c", 300L, 1, true)
        );

        coordinator.expandCandidates(candidates);

        verify(indicesAdminClient, times(3)).updateSettings(any(UpdateSettingsRequest.class), any());
    }
}
