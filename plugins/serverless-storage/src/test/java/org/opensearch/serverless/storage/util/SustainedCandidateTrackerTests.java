/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.util;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SustainedCandidateTrackerTests extends OpenSearchTestCase {

    private static final Function<String, String> IDENTITY = Function.identity();

    public void testFilterSustainedRequiresConsecutiveTicks() {
        SustainedCandidateTracker<String> tracker = new SustainedCandidateTracker<>(3);
        List<String> candidates = List.of("a");

        assertEquals(List.of(), tracker.filterSustained(candidates, IDENTITY, c -> true));
        assertEquals(List.of(), tracker.filterSustained(candidates, IDENTITY, c -> true));
        assertEquals(List.of("a"), tracker.filterSustained(candidates, IDENTITY, c -> true));
    }

    public void testFilterSustainedResetsStreakWhenNotFlagged() {
        SustainedCandidateTracker<String> tracker = new SustainedCandidateTracker<>(2);
        tracker.filterSustained(List.of("a"), IDENTITY, c -> true);
        tracker.filterSustained(List.of("a"), IDENTITY, c -> false); // resets the streak
        assertEquals(List.of(), tracker.filterSustained(List.of("a"), IDENTITY, c -> true));
    }

    public void testFilterSustainedDropsAKeyNoLongerReportedAtAll() {
        SustainedCandidateTracker<String> tracker = new SustainedCandidateTracker<>(2);
        tracker.filterSustained(List.of("a"), IDENTITY, c -> true);
        tracker.filterSustained(List.of(), IDENTITY, c -> true); // "a" vanished entirely
        assertEquals(List.of(), tracker.filterSustained(List.of("a"), IDENTITY, c -> true));
    }

    public void testSelectWithBudgetActsOnEveryEntryWhenUnlimited() {
        SustainedCandidateTracker<String> tracker = new SustainedCandidateTracker<>(1);
        List<String> acted = new ArrayList<>();
        tracker.selectWithBudget(List.of("a", "b", "c"), 0, e -> true, e -> {
            acted.add(e);
            return true;
        });
        assertEquals(List.of("a", "b", "c"), acted);
    }

    public void testSelectWithBudgetStopsActingOnBudgetNeedingEntriesOnceExhausted() {
        SustainedCandidateTracker<String> tracker = new SustainedCandidateTracker<>(1);
        List<String> acted = new ArrayList<>();
        tracker.selectWithBudget(List.of("a", "b", "c"), 1, e -> true, e -> {
            acted.add(e);
            return true;
        });
        assertEquals(List.of("a"), acted);
    }

    /**
     * Regression test: once budget-needing candidates exhaust the budget, a *later* candidate that
     * does not need budget at all (e.g. a sibling of an already-acted-on candidate from the same
     * dedup group) must still be visited and acted on -- the original implementation `break`s the
     * whole loop the moment budget runs out, silently skipping every remaining candidate regardless
     * of whether it would have needed budget, which left such a sibling's tracked streak un-cleared.
     */
    public void testSelectWithBudgetStillActsOnALaterEntryThatDoesNotNeedBudgetOnceExhausted() {
        SustainedCandidateTracker<String> tracker = new SustainedCandidateTracker<>(1);
        List<String> acted = new ArrayList<>();
        // "a" consumes the only budget slot; "b" needs budget but there's none left, so it's skipped;
        // "c" does NOT need budget (needsBudget returns false only for "c") and must still be acted on
        // despite coming after the exhaustion point.
        tracker.selectWithBudget(List.of("a", "b", "c"), 1, e -> e.equals("c") == false, e -> {
            acted.add(e);
            return true;
        });
        assertEquals(List.of("a", "c"), acted);
    }

    public void testSelectWithBudgetLeavesTheStreakOfASkippedBudgetNeedingCandidateIntact() {
        SustainedCandidateTracker<String> tracker = new SustainedCandidateTracker<>(1);
        // "a" gets the budget and its streak is explicitly cleared by the caller (as
        // ReaderReplicaExpansionCoordinator does); "b" needs budget, none left, skipped -- its streak
        // must remain (so it re-qualifies as sustained immediately next tick, not from scratch).
        tracker.selectWithBudget(List.of("a", "b"), 1, e -> true, e -> {
            if (e.equals("a")) {
                tracker.clearStreak("a");
                return true;
            }
            return false; // "b" was never actually acted on.
        });

        // "b" alone, still flagged a candidate: since its streak survived the skip above, one more
        // tick (this call, with requiredConsecutiveTicks=1) is immediately sustained.
        assertEquals(List.of("b"), tracker.filterSustained(List.of("b"), IDENTITY, c -> true));
    }
}
