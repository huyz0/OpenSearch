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

    /**
     * Finding S-2. {@code required_consecutive_ticks} counted ticks, but the query-rate signal
     * behind them is a fixed 60-second window. With {@code scale_up.eval_interval} at 10s -- a
     * perfectly reasonable operator choice that nothing validated against the window -- six
     * consecutive ticks read the identical measurement, so the default of two ticks was satisfied by
     * a single observation; and because the streak is cleared after each expansion, an index
     * ratcheted 1 -&gt; 2 -&gt; 3 -&gt; 4 -&gt; 5 in about 40 seconds on the strength of it. The
     * setting's javadoc claims it prevents "reacting to one noisy evaluation"; it did not.
     */
    public void testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0);
        // Deliberately NOT opting out of the spacing: that is what this test is about.
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        // Ten evaluations in immediate succession, which is what a 10s eval interval produces over
        // less than two query-rate windows. Only the first can be a distinct measurement.
        for (int i = 0; i < 10; i++) {
            coordinator.expandCandidates(List.of(candidate));
        }

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testExpandsOnlyCandidatesByOneStep() {
        // requiredConsecutiveTicks=1 -- this test is about which entries get acted on, not hysteresis.
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);
        ScaleUpCandidateEntry notCandidate = new ScaleUpCandidateEntry("uuid-2", 0, "other-index", 10L, 1, false);

        coordinator.expandCandidates(List.of(candidate, notCandidate));

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testDedupesMultipleShardsOfSameIndexIntoOneUpdate() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        ScaleUpCandidateEntry shard0 = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);
        ScaleUpCandidateEntry shard1 = new ScaleUpCandidateEntry("uuid-1", 1, "my-index", 700L, 1, true);

        coordinator.expandCandidates(List.of(shard0, shard1));

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testNeverExceedsConfiguredCap() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 3, 1, 0);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        // Already at the cap: candidate() should never have been true for this in real use, but the
        // coordinator's own second guard must still refuse to act on it.
        ScaleUpCandidateEntry atCap = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 3, true);

        coordinator.expandCandidates(List.of(atCap));

        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testUpdateRequestTargetsCorrectIndexAndReplicaCount() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
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
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));
        coordinator.expandCandidates(List.of(candidate));

        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testExpandsOnlyOnceTheStreakReachesTheRequiredConsecutiveTickCount() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 3, 0);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));
        coordinator.expandCandidates(List.of(candidate));
        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());

        coordinator.expandCandidates(List.of(candidate));
        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testAGapInCandidacyResetsTheStreak() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 3, 0);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
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
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testPerTickExpansionBudgetLimitsHowManyIndicesExpandInOneCall() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 2);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
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
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
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
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
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
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        List<ScaleUpCandidateEntry> candidates = List.of(
            new ScaleUpCandidateEntry("uuid-a", 0, "index-a", 100L, 1, true),
            new ScaleUpCandidateEntry("uuid-b", 0, "index-b", 200L, 1, true),
            new ScaleUpCandidateEntry("uuid-c", 0, "index-c", 300L, 1, true)
        );

        coordinator.expandCandidates(candidates);

        verify(indicesAdminClient, times(3)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testSkipsExpansionWhenReaderCapacitySaturated() {
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0, () -> true);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));

        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testResumesExpansionOnceCapacityFreesUp() {
        java.util.concurrent.atomic.AtomicBoolean saturated = new java.util.concurrent.atomic.AtomicBoolean(true);
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0, saturated::get);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));
        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());

        saturated.set(false);
        coordinator.expandCandidates(List.of(candidate));
        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testZeroHeadroomBlocksExpansionEvenWithinTheConfiguredCap() {
        // maxExpansionsPerTick=0 (unlimited by its own convention), but headroom=0 must still block --
        // headroom is a real signal at zero, not a disabled setting, unlike maxExpansionsPerTick.
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0, () -> false, () -> 0);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        coordinator.expandCandidates(List.of(candidate));

        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testHeadroomBudgetLimitsExpansionsIndependentlyOfTheConfiguredCap() {
        // maxExpansionsPerTick=5 (would allow all 3), but headroom=1 must still ration to 1.
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 5, () -> false, () -> 1);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        List<ScaleUpCandidateEntry> candidates = List.of(
            new ScaleUpCandidateEntry("uuid-a", 0, "index-a", 100L, 1, true),
            new ScaleUpCandidateEntry("uuid-b", 0, "index-b", 200L, 1, true),
            new ScaleUpCandidateEntry("uuid-c", 0, "index-c", 300L, 1, true)
        );

        coordinator.expandCandidates(candidates);

        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testMaxValueHeadroomImposesNoAdditionalConstraint() {
        // The default (disabled) headroom supplier -- verifies it doesn't accidentally clamp the
        // configured per-tick cap down to something smaller than intended.
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(
            client,
            5,
            1,
            0,
            () -> false,
            () -> Integer.MAX_VALUE
        );
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        List<ScaleUpCandidateEntry> candidates = List.of(
            new ScaleUpCandidateEntry("uuid-a", 0, "index-a", 100L, 1, true),
            new ScaleUpCandidateEntry("uuid-b", 0, "index-b", 200L, 1, true)
        );

        coordinator.expandCandidates(candidates);

        verify(indicesAdminClient, times(2)).updateSettings(any(UpdateSettingsRequest.class), any());
    }

    public void testHeadroomExhaustionLeavesTheStreakIntactForTheNextTick() {
        java.util.concurrent.atomic.AtomicInteger headroom = new java.util.concurrent.atomic.AtomicInteger(0);
        ReaderReplicaExpansionCoordinator coordinator = new ReaderReplicaExpansionCoordinator(client, 5, 1, 0, () -> false, headroom::get);
        // Finding S-2 added a minimum spacing between counted evaluations so hysteresis measures
        // distinct 60-second query-rate windows rather than ticks. These tests feed synthetic
        // observations back to back on purpose -- they exercise the streak arithmetic itself -- so
        // they opt out of the spacing. testHysteresisIgnoresEvaluationsInsideOneQueryRateWindow is
        // the test that pins the spacing itself.
        coordinator.setMinimumEvaluationSpacingMillisForTesting(0);
        ScaleUpCandidateEntry candidate = new ScaleUpCandidateEntry("uuid-1", 0, "my-index", 900L, 1, true);

        // requiredConsecutiveTicks=1 -- the candidate would qualify immediately if headroom allowed
        // it. Under zero headroom it must not expand, and once headroom returns it must expand
        // right away with no re-qualification needed, proving exhaustion never reset the streak.
        coordinator.expandCandidates(List.of(candidate));
        verify(indicesAdminClient, never()).updateSettings(any(UpdateSettingsRequest.class), any());

        headroom.set(1);
        coordinator.expandCandidates(List.of(candidate));
        verify(indicesAdminClient, times(1)).updateSettings(any(UpdateSettingsRequest.class), any());
    }
}
