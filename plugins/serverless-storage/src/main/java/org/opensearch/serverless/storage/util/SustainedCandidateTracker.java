/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The mechanical part shared by {@code
 * org.opensearch.serverless.storage.resharding.InPlaceSplitTriggerCoordinator} and {@code
 * org.opensearch.serverless.storage.scaleup.ReaderReplicaExpansionCoordinator}: requiring a
 * candidate to be flagged on {@code requiredConsecutiveTicks} consecutive evaluations before it
 * counts as "sustained" (hysteresis against reacting to one noisy tick), and then handing out a
 * limited per-tick action budget to the sustained candidates, busiest first.
 *
 * <p>Deliberately holds only this mechanism -- per-key streak bookkeeping and prioritized budget
 * iteration -- not any domain logic. What "act" means, how to fetch a candidate's priority metric,
 * and whether any given candidate actually needs the shared budget (e.g. a coordinator that dedupes
 * several candidates down to one action may decide some don't) all stay with each coordinator, via
 * the callbacks passed to {@link #selectWithBudget}.
 *
 * @param <T> the candidate type (e.g. one shard's or one index's evaluation result for a tick).
 */
public final class SustainedCandidateTracker<T> {

    private final int requiredConsecutiveTicks;
    private final ConcurrentMap<String, Integer> consecutiveCandidateTicks = new ConcurrentHashMap<>();

    /**
     * @param requiredConsecutiveTicks how many consecutive evaluations in a row a candidate must be
     *                                 flagged on before it counts as sustained. Values {@code <= 1}
     *                                 mean every single flagged tick counts (no hysteresis).
     */
    public SustainedCandidateTracker(int requiredConsecutiveTicks) {
        this.requiredConsecutiveTicks = Math.max(1, requiredConsecutiveTicks);
    }

    /**
     * Updates each observed candidate's streak and returns just the ones now sustained.
     *
     * <p>A candidate not flagged this tick has its streak reset to zero (removed). A key not present
     * in {@code candidates} at all -- i.e. no longer reported by this tick's evaluation, such as a
     * relocated, deleted, or split-away shard -- has its tracked streak dropped too, rather than
     * leaking forever.
     *
     * @param candidates this tick's full merged candidate list -- every candidate this evaluation
     *                   observed, not just the ones currently flagged, so a candidate that stopped
     *                   qualifying or vanished entirely is handled correctly.
     * @param keyFn extracts the stable per-candidate tracking key (e.g. {@code indexUuid + "/" +
     *              shardId}).
     * @param isCandidate whether this tick flags the candidate at all, before hysteresis is applied.
     * @return the candidates whose streak has now reached {@link #requiredConsecutiveTicks}, in the
     *     same relative order as {@code candidates}.
     */
    public List<T> filterSustained(List<T> candidates, Function<T, String> keyFn, Predicate<T> isCandidate) {
        Set<String> observedKeys = ConcurrentHashMap.newKeySet();
        List<T> sustained = new ArrayList<>();
        for (T entry : candidates) {
            String key = keyFn.apply(entry);
            observedKeys.add(key);
            if (isCandidate.test(entry) == false) {
                consecutiveCandidateTicks.remove(key);
                continue;
            }
            int streak = consecutiveCandidateTicks.merge(key, 1, Integer::sum);
            if (streak < requiredConsecutiveTicks) {
                continue; // flagged, but not sustained long enough yet -- wait for the next tick.
            }
            sustained.add(entry);
        }
        consecutiveCandidateTicks.keySet().retainAll(observedKeys);
        return sustained;
    }

    /** Drops a candidate's tracked streak outright -- e.g. once acted on, or found no longer valid. */
    public void clearStreak(String key) {
        consecutiveCandidateTicks.remove(key);
    }

    /**
     * Hands out {@code budget} actions to {@code sustainedCandidates}, in the order given (callers
     * sort busiest-first beforehand), stopping once the budget is exhausted.
     *
     * <p>A candidate for which {@code needsBudget} is {@code false} is always acted on regardless of
     * remaining budget and never consumes a slot -- e.g. a coordinator that dedupes several
     * candidates down to one action per group can let every candidate but the group's first pass
     * through free. Once a candidate that does need budget is reached with none left, iteration
     * stops entirely: every remaining candidate (all lower priority) keeps whatever streak it had --
     * callers are expected to have already cleared the streak of any candidate not passed in here at
     * all (e.g. one already handled outside this tick's budget concern).
     *
     * @param sustainedCandidates the sustained candidates, highest priority first.
     * @param budget the maximum number of budget-consuming actions this call may take. Values {@code
     *               <= 0} mean unlimited.
     * @param needsBudget whether acting on this candidate should be counted against {@code budget}.
     * @param act performs the action for one candidate, returning whether it actually consumed a
     *            budget slot (a coordinator that discovers mid-action the candidate is redundant --
     *            e.g. already handled by an earlier candidate in the same tick -- may return {@code
     *            false} even for a candidate {@code needsBudget} flagged {@code true}).
     */
    public void selectWithBudget(List<T> sustainedCandidates, int budget, Predicate<T> needsBudget, Function<T, Boolean> act) {
        int used = 0;
        for (T entry : sustainedCandidates) {
            if (needsBudget.test(entry) && budget > 0 && used >= budget) {
                break;
            }
            if (Boolean.TRUE.equals(act.apply(entry))) {
                used++;
            }
        }
    }
}
