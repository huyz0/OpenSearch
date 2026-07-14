/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidateEntry;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The "do the work" half of scale-up (see the RFC's scale-up autoscaling subsection), consuming
 * {@link ScaleUpCandidateEntry} the same way {@code
 * org.opensearch.serverless.storage.scaletozero.ShardSuspensionCoordinator} consumes {@code
 * ScaleToZeroCandidateEntry} -- this class makes no eligibility decision of its own, it only acts
 * on a candidate already flagged eligible.
 *
 * <p>Unlike {@code ShardSuspensionCoordinator}, which mutates {@link IndexMetadata} custom data
 * directly via a {@link org.opensearch.cluster.ClusterStateUpdateTask}, replica count is ordinary
 * index configuration ({@link IndexMetadata#SETTING_NUMBER_OF_SEARCH_REPLICAS}, exposed as {@link
 * IndexMetadata#getNumberOfSearchOnlyReplicas()}), so there is no dedicated builder setter for it
 * -- core only ever derives it from settings during {@code IndexMetadata.Builder#build()}. Bumping
 * it is therefore a plain {@link UpdateSettingsRequest}, the same sanctioned path an operator's own
 * {@code PUT /index/_settings} would use, rather than a bespoke cluster-state mutation.
 *
 * <p>Deliberately per-index, not per-shard: {@code index.number_of_search_replicas} is an
 * index-wide setting, so one busy shard's candidacy expands every shard's search-replica count for
 * that index together -- {@link #expandCandidates} dedupes multiple candidate shards of the same
 * index down to a single settings update.
 *
 * <p><b>Sustained-duration hysteresis</b> (rfc-serverless-opensearch.md &sect;10's own "would need
 * multiple consecutive over-threshold ticks to avoid reacting to one noisy evaluation" gap,
 * previously left open): {@link #expandCandidates} tracks, per {@code (indexUuid, shardId)}, how
 * many *consecutive* evaluations in a row have flagged that shard a candidate. A shard only
 * actually triggers expansion once that streak reaches {@link #requiredConsecutiveTicks}; any tick
 * where it is not flagged a candidate resets its streak to zero, so the requirement really means
 * "sustained," not "N times ever." A shard no longer reported at all (relocated, deleted, or its
 * index no longer exists) has its tracked streak dropped rather than left to leak forever.
 *
 * <p><b>Illustrative per-tick expansion budget</b> (rfc-serverless-opensearch.md &sect;10's own "a
 * real cost model remains future work" note): without one, a single evaluation tick that finds many
 * sustained candidates at once (e.g. a cluster-wide traffic spike touching dozens of indices)
 * fans out an unbounded burst of {@link UpdateSettingsRequest} calls in one go -- exactly the kind
 * of stampede this plugin already guards against elsewhere via its cost-accounting PUT-budget gates
 * (&sect;17). This is deliberately <em>not</em> a dollar-cost model -- this repo has no real cloud
 * pricing data to calibrate one, the same reasoning that kept a $-based model out of scope when this
 * gap was first scoped -- it is a rate limit on cluster-state-mutation calls per tick, ranked by
 * {@link ScaleUpCandidateEntry#queriesPerMinute()} so the busiest shards win the budget first.
 * Candidates that lose out to the budget keep their sustained-duration streak intact (not reset),
 * so they carry the highest priority into the very next tick rather than having to re-qualify from
 * scratch.
 */
public final class ReaderReplicaExpansionCoordinator {

    private static final Logger logger = LogManager.getLogger(ReaderReplicaExpansionCoordinator.class);

    private final Client client;
    private final int maxSearchReplicas;
    private final int requiredConsecutiveTicks;
    private final int maxExpansionsPerTick;
    private final ConcurrentMap<String, Integer> consecutiveCandidateTicks = new ConcurrentHashMap<>();

    /**
     * Creates a coordinator.
     *
     * @param client dispatches the {@link UpdateSettingsRequest} that actually expands the index.
     * @param maxSearchReplicas the cap {@link #expandCandidates} never bumps a candidate index past,
     *                          even if called repeatedly -- normally already enforced by {@link
     *                          ScaleUpCandidateEntry#candidate()} itself, this is a second,
     *                          coordinator-local guard against acting on a stale entry.
     * @param requiredConsecutiveTicks how many consecutive evaluations in a row a shard must be
     *                                 flagged a candidate on before its index is actually expanded --
     *                                 see this class's own "Sustained-duration hysteresis" javadoc.
     *                                 Values {@code <= 1} restore the original single-tick behavior.
     * @param maxExpansionsPerTick the maximum number of distinct indices {@link #expandCandidates}
     *                             will actually expand in one call -- see this class's own
     *                             "Illustrative per-tick expansion budget" javadoc. Values
     *                             {@code <= 0} mean unlimited (the original, unbounded behavior).
     */
    public ReaderReplicaExpansionCoordinator(Client client, int maxSearchReplicas, int requiredConsecutiveTicks, int maxExpansionsPerTick) {
        this.client = client;
        this.maxSearchReplicas = maxSearchReplicas;
        this.requiredConsecutiveTicks = Math.max(1, requiredConsecutiveTicks);
        this.maxExpansionsPerTick = maxExpansionsPerTick;
    }

    /**
     * Bumps every distinct sustained candidate index's {@code index.number_of_search_replicas} by
     * one, capped at {@link #maxSearchReplicas} -- idempotent in the sense that calling this on
     * every scheduled evaluation tick is safe, since each call only ever bumps by one step past
     * whatever {@link ScaleUpCandidateEntry#currentSearchReplicaCount()} the evaluation itself
     * observed. Bounded by {@link #maxExpansionsPerTick}: if more distinct indices are sustained
     * candidates than the budget allows, the busiest ones (by {@link
     * ScaleUpCandidateEntry#queriesPerMinute()}) are expanded first.
     *
     * @param candidates one evaluation's full merged shard list -- every shard this evaluation
     *                   observed, not just the ones currently flagged a candidate, so this method
     *                   can correctly reset the streak of a shard that stopped qualifying and drop
     *                   the streak of a shard no longer reported at all.
     */
    public void expandCandidates(List<ScaleUpCandidateEntry> candidates) {
        Set<String> observedKeys = new HashSet<>();
        List<ScaleUpCandidateEntry> sustainedCandidates = new ArrayList<>();
        for (ScaleUpCandidateEntry entry : candidates) {
            String key = entry.indexUuid() + "/" + entry.shardId();
            observedKeys.add(key);
            if (entry.candidate() == false) {
                consecutiveCandidateTicks.remove(key);
                continue;
            }
            int streak = consecutiveCandidateTicks.merge(key, 1, Integer::sum);
            if (streak < requiredConsecutiveTicks) {
                continue; // flagged, but not sustained long enough yet -- wait for the next tick.
            }
            sustainedCandidates.add(entry);
        }
        consecutiveCandidateTicks.keySet().retainAll(observedKeys);

        // Busiest shard first, so a tight budget is spent on whichever candidates need it most.
        sustainedCandidates.sort(Comparator.comparingLong(ScaleUpCandidateEntry::queriesPerMinute).reversed());

        Set<String> alreadyExpanded = new HashSet<>();
        int expansionsIssuedThisTick = 0;
        for (ScaleUpCandidateEntry entry : sustainedCandidates) {
            if (alreadyExpanded.contains(entry.indexName()) == false
                && maxExpansionsPerTick > 0
                && expansionsIssuedThisTick >= maxExpansionsPerTick) {
                // Budget exhausted for any *new* index this tick -- deliberately leave this (and
                // every remaining) candidate's streak intact rather than clearing it, so it keeps
                // its "already sustained" status and highest priority into the next tick instead
                // of having to re-qualify from scratch.
                break;
            }
            consecutiveCandidateTicks.remove(entry.indexUuid() + "/" + entry.shardId()); // acted on -- start counting fresh.
            if (alreadyExpanded.add(entry.indexName()) == false) {
                continue; // another shard of the same index already triggered this index's expansion this tick.
            }
            expandIndex(entry.indexName(), entry.currentSearchReplicaCount());
            expansionsIssuedThisTick++;
        }
    }

    private void expandIndex(String indexName, int currentSearchReplicaCount) {
        int target = Math.min(currentSearchReplicaCount + 1, maxSearchReplicas);
        if (target <= currentSearchReplicaCount) {
            return; // already at (or somehow past) the cap -- nothing to do.
        }
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
        request.settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, target).build());
        client.admin()
            .indices()
            .updateSettings(
                request,
                ActionListener.wrap(
                    response -> logger.info("expanded serverless-storage reader index [{}] to {} search replica(s)", indexName, target),
                    e -> logger.warn("failed to expand serverless-storage reader index [" + indexName + "]", e)
                )
            );
    }
}
