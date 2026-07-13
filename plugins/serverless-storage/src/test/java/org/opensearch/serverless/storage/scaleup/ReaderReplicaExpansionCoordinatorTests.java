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
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);
        ScaleUpCandidateEntry notCandidate = new ScaleUpCandidateEntry("uuid-2", 0, "other-index", 10L, 1, false);

        coordinator.expandCandidates(List.of(candidate, notCandidate));

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testDedupesMultipleShardsOfSameIndexIntoOneUpdate() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1);
        ScaleUpCandidateEntry shard0 = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);
        ScaleUpCandidateEntry shard1 = new ScaleUpCandidateEntry("uuid-1", 1, "my-index", 700L, 1, true);

        coordinator.expandCandidates(List.of(shard0, shard1));

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testNeverExceedsConfiguredCap() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 3, 1);
        // Already at the cap: candidate() should never have been true for this in real use, but the
        // coordinator's own second guard must still refuse to act on it.
        ScaleUpCandidateEntry atCap = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 3, true);

        coordinator.expandCandidates(List.of(atCap));

        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testUpdateRequestTargetsCorrectIndexAndReplicaCount() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));

        org.mockito.ArgumentCaptor<UpdateSettingsRequest> captor = org.mockito.ArgumentCaptor.forClass(UpdateSettingsRequest.class);
        verify(indicesAdminClient).updateSettings(captor.capture(), any());
        UpdateSettingsRequest request = captor.getValue();
        assertArrayEquals(new String[] { "my-index" }, request.indices());
        assertEquals("2", request.settings().get(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS));
    }

    public void testASingleOverThresholdTickDoesNotTriggerExpansionWhenHysteresisIsConfigured() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 3);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));
        coordinator.expandCandidates(List.of(candidate));

        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testExpandsOnlyOnceTheStreakReachesTheRequiredConsecutiveTickCount() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 3);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));
        coordinator.expandCandidates(List.of(candidate));
        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());

        coordinator.expandCandidates(List.of(candidate));
        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testAGapInCandidacyResetsTheStreak() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 3);
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
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }
}
