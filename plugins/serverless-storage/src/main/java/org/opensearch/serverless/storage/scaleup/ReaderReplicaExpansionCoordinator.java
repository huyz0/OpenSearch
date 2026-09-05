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
import org.opensearch.serverless.storage.util.SustainedCandidateTracker;
import org.opensearch.transport.client.Client;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

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
 * actually triggers expansion once that streak reaches the configured {@code requiredConsecutiveTicks}; any tick
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
 *
 * <p><b>Ceiling-aware, node autoscaling design doc part 4</b> ("Ceiling-aware replica expansion"):
 * before acting on any candidate, {@link #expandCandidates} consults {@link #readerCapacitySaturated}.
 * If the reader node fleet is already saturated (no node has room for another shard copy), expansion
 * is skipped entirely for this tick -- bumping a replica count that can never actually be assigned
 * would just leave a permanently-unassigned shard and stick {@code unassigned_shard_count} nonzero
 * for no benefit. Every candidate's sustained-duration streak is left intact either way, so expansion
 * resumes immediately once capacity frees up rather than needing candidates to re-qualify. This is
 * a one-way dependency: this coordinator only ever <em>reads</em> the node-autoscaling signal, it
 * never commands the fleet -- see the design doc's "one-way dependency" framing.
 *
 * <p><b>Headroom-aware expansion budget, node autoscaling design doc part 4</b> ("per-index
 * scale-up fairness"): {@link #headroomBudget} refines the binary ceiling check above into a
 * rationed one. Where {@link #readerCapacitySaturated} only distinguishes "some room" from "none,"
 * {@link #headroomBudget} estimates <em>how much</em> room is left -- {@code (reader node count
 * &times; a configured max shards per node) - currently assigned reader shards} -- and shrinks the
 * per-tick budget to that estimate as the fleet approaches its ceiling, so the last slice of
 * capacity is spent on the busiest sustained candidates rather than whichever tick happened to ask
 * first. Returns {@link Integer#MAX_VALUE} (no additional constraint) when the per-node capacity
 * setting is disabled or the signal isn't available yet, matching {@link #readerCapacitySaturated}'s
 * own safe-degrade convention.
 */
public final class ReaderReplicaExpansionCoordinator {

    private static final Logger logger = LogManager.getLogger(ReaderReplicaExpansionCoordinator.class);

    private final Client client;
    private final int maxSearchReplicas;
    private final int maxExpansionsPerTick;
    private final SustainedCandidateTracker<ScaleUpCandidateEntry> tracker;
    private final BooleanSupplier readerCapacitySaturated;
    private final IntSupplier headroomBudget;

    /**
     * When {@link #expandCandidates} last counted an evaluation toward the hysteresis streak. See
     * that method for why ticks are throttled to the query-rate window (finding S-2).
     */
    private volatile long lastCountedEvaluationMillis = 0L;

    /**
     * The width of the query-rate window the candidate signal is derived from -- the minimum spacing
     * between two evaluations that can be treated as independent observations. Kept in step with
     * {@code ScaleUpCandidatesResponse.QUERY_RATE_WINDOW_MILLIS} and {@code
     * ObjectStoreReaderEngine.QUERY_RATE_WINDOW_MILLIS}.
     */
    static final long QUERY_RATE_WINDOW_MILLIS = 60_000L;

    /**
     * The spacing actually enforced. Defaults to {@link #QUERY_RATE_WINDOW_MILLIS}; a unit test that
     * is exercising the streak arithmetic itself sets it to zero, because there the point is to feed
     * N distinct observations, not to prove they were N distinct <em>measurements</em>.
     */
    private volatile long minimumEvaluationSpacingMillis = QUERY_RATE_WINDOW_MILLIS;

    /**
     * Disables or changes the minimum spacing between counted evaluations -- test-only visibility.
     *
     * @param millis the spacing to enforce; {@code 0} counts every call.
     */
    void setMinimumEvaluationSpacingMillisForTesting(long millis) {
        this.minimumEvaluationSpacingMillis = millis;
    }

    /**
     * Creates a coordinator with no capacity-ceiling awareness -- {@link #expandCandidates} always
     * proceeds regardless of node-level fleet capacity, the original behavior before node
     * autoscaling existed. Equivalent to the other constructor with a supplier that always returns
     * {@code false}.
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
        this(client, maxSearchReplicas, requiredConsecutiveTicks, maxExpansionsPerTick, () -> false);
    }

    /**
     * Creates a coordinator with no headroom-aware budget refinement -- {@link #expandCandidates}
     * is bounded only by {@code maxExpansionsPerTick}, never by an estimate of remaining fleet
     * capacity. Equivalent to the other constructor with a supplier that always returns {@link
     * Integer#MAX_VALUE}.
     *
     * @param client dispatches the {@link UpdateSettingsRequest} that actually expands the index.
     * @param maxSearchReplicas the cap {@link #expandCandidates} never bumps a candidate index past.
     * @param requiredConsecutiveTicks how many consecutive evaluations in a row a shard must be
     *                                 flagged a candidate on before its index is actually expanded.
     * @param maxExpansionsPerTick the maximum number of distinct indices {@link #expandCandidates}
     *                             will actually expand in one call.
     * @param readerCapacitySaturated returns {@code true} when the reader node fleet has no room for
     *                                another shard copy right now -- see this class's own
     *                                "Ceiling-aware" javadoc. Typically backed by {@code
     *                                NodeCapacitySignalService#latestSignal().reader().unassignedShardCount() > 0},
     *                                and must degrade safely (return {@code false}) before that
     *                                signal has ever been computed, so a fresh node capacity service
     *                                never blocks expansion by default.
     */
    public ReaderReplicaExpansionCoordinator(
        Client client,
        int maxSearchReplicas,
        int requiredConsecutiveTicks,
        int maxExpansionsPerTick,
        BooleanSupplier readerCapacitySaturated
    ) {
        this(client, maxSearchReplicas, requiredConsecutiveTicks, maxExpansionsPerTick, readerCapacitySaturated, () -> Integer.MAX_VALUE);
    }

    /**
     * Creates a coordinator.
     *
     * @param client dispatches the {@link UpdateSettingsRequest} that actually expands the index.
     * @param maxSearchReplicas the cap {@link #expandCandidates} never bumps a candidate index past.
     * @param requiredConsecutiveTicks how many consecutive evaluations in a row a shard must be
     *                                 flagged a candidate on before its index is actually expanded.
     * @param maxExpansionsPerTick the maximum number of distinct indices {@link #expandCandidates}
     *                             will actually expand in one call.
     * @param readerCapacitySaturated returns {@code true} when the reader node fleet has no room for
     *                                another shard copy right now -- see this class's own
     *                                "Ceiling-aware" javadoc.
     * @param headroomBudget returns an estimate of how many more shard copies the current reader
     *                       fleet can actually absorb -- see this class's own "Headroom-aware
     *                       expansion budget" javadoc. Must return {@link Integer#MAX_VALUE} (not
     *                       zero, and not a negative number) when the estimate is unavailable or
     *                       the underlying setting is disabled, so a fresh or disabled headroom
     *                       model never blocks expansion by default.
     */
    public ReaderReplicaExpansionCoordinator(
        Client client,
        int maxSearchReplicas,
        int requiredConsecutiveTicks,
        int maxExpansionsPerTick,
        BooleanSupplier readerCapacitySaturated,
        IntSupplier headroomBudget
    ) {
        this.client = client;
        this.maxSearchReplicas = maxSearchReplicas;
        this.maxExpansionsPerTick = maxExpansionsPerTick;
        this.tracker = new SustainedCandidateTracker<>(requiredConsecutiveTicks);
        this.readerCapacitySaturated = readerCapacitySaturated;
        this.headroomBudget = headroomBudget;
    }

    private static String key(ScaleUpCandidateEntry entry) {
        return entry.indexUuid() + "/" + entry.shardId();
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
        // Finding S-2. requiredConsecutiveTicks counts *ticks*, but the underlying query-rate signal
        // is a fixed 60-second window, and nothing validates the eval interval against it. With
        // scale_up.eval_interval at 10s -- a perfectly reasonable operator choice -- six consecutive
        // ticks read the identical completedWindowQueryCount, so the default of 2 ticks was satisfied
        // by a *single* 60-second observation; and because the streak is cleared after each expansion,
        // an index ratcheted 1 -> 2 -> 3 -> 4 -> 5 in about 40 seconds on the strength of that one
        // measurement. The setting's own javadoc claims it prevents "reacting to one noisy
        // evaluation"; it did not.
        //
        // Enforcing a minimum spacing between counted evaluations is what makes the hysteresis
        // count distinct measurements again, whatever the configured interval. Ticks that arrive too
        // soon are skipped entirely rather than folded in: touching the tracker would either advance
        // a streak on a repeated observation (the bug) or, via filterSustained's retainAll, disturb
        // streaks it has no new information about.
        long now = System.currentTimeMillis();
        long previous = lastCountedEvaluationMillis;
        if (previous != 0L && now - previous < minimumEvaluationSpacingMillis) {
            logger.debug(
                "skipping reader replica expansion this tick: only {} ms since the last counted evaluation, and the query-rate "
                    + "signal has a {} ms window -- this tick would re-count the same measurement",
                now - previous,
                minimumEvaluationSpacingMillis
            );
            return;
        }
        lastCountedEvaluationMillis = now;

        if (readerCapacitySaturated.getAsBoolean()) {
            // Fleet is already at capacity for the reader role -- skip acting this tick, but still
            // run filterSustained below so every candidate's streak stays current (neither reset nor
            // left stale), and skip straight past selectWithBudget so nothing is expanded.
            tracker.filterSustained(candidates, ReaderReplicaExpansionCoordinator::key, ScaleUpCandidateEntry::candidate);
            logger.debug("skipping reader replica expansion this tick -- node capacity signal reports the reader fleet is saturated");
            return;
        }

        List<ScaleUpCandidateEntry> sustainedCandidates = tracker.filterSustained(
            candidates,
            ReaderReplicaExpansionCoordinator::key,
            ScaleUpCandidateEntry::candidate
        );

        // Busiest shard first, so a tight budget is spent on whichever candidates need it most.
        sustainedCandidates.sort(Comparator.comparingLong(ScaleUpCandidateEntry::queriesPerMinute).reversed());

        // maxExpansionsPerTick <= 0 means "unlimited" by SustainedCandidateTracker's own convention,
        // so only intersect it with the headroom estimate when it's a real, positive cap. Headroom
        // itself is never treated as "unlimited" at 0 -- a headroom of exactly 0 is a real signal
        // (the fleet has no room left), not a disabled setting, so it must actually block expansion
        // rather than being passed through to selectWithBudget's own <= 0-means-unlimited handling.
        int headroom = headroomBudget.getAsInt();
        int effectiveBudget = maxExpansionsPerTick <= 0 ? headroom : Math.min(maxExpansionsPerTick, headroom);
        if (effectiveBudget <= 0) {
            // Headroom is exhausted -- same shape as the saturated-fleet fast path above: keep every
            // candidate's streak current, act on nothing.
            logger.debug("skipping reader replica expansion this tick -- node capacity headroom estimate is exhausted");
            return;
        }

        // Budget exhausted for any *new* index this tick -- deliberately leave this (and every
        // remaining) candidate's streak intact rather than clearing it, so it keeps its "already
        // sustained" status and highest priority into the next tick instead of having to re-qualify
        // from scratch. A candidate whose index another shard already triggered this tick never
        // needs the budget at all -- it's always acted on (streak cleared) but never counted.
        Set<String> alreadyExpanded = new HashSet<>();
        tracker.selectWithBudget(
            sustainedCandidates,
            effectiveBudget,
            entry -> alreadyExpanded.contains(entry.indexName()) == false,
            entry -> {
                tracker.clearStreak(key(entry)); // acted on -- start counting fresh.
                if (alreadyExpanded.add(entry.indexName()) == false) {
                    return false; // another shard of the same index already triggered this index's expansion this tick.
                }
                expandIndex(entry.indexName(), entry.currentSearchReplicaCount());
                return true;
            }
        );
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
