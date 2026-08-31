/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * The checker, checked.
 *
 * <p><b>An instrument nobody has ever seen reject anything is not evidence.</b> When R11 finally runs
 * against a real provider, the answer will be a green test — and a green test from a checker that cannot
 * fail is exactly as informative as no test. So each way a register can deviate is written out here as a
 * history the checker must reject, and the reason it must is the reason the design cares.
 *
 * <p>These histories are built by hand rather than recorded, deliberately: a hand-built history says
 * precisely which deviation is being modelled, where a recorded one from a broken store says "something
 * was wrong somewhere". {@link ConformanceSuiteSelfTests} does the recorded version, over a store that is
 * really misbehaving; this does the version that names each failure.
 */
public class LinearizabilityCheckerTests extends OpenSearchTestCase {

    private final List<RegisterHistory.Entry> history = new ArrayList<>();
    private long tick;

    /** Adds a call that was in flight alone, so its position in the order is fixed. */
    private void sequential(RegisterHistory.Op op) {
        final long invoked = ++tick;
        history.add(new RegisterHistory.Entry(op, invoked, ++tick));
    }

    /** Adds two calls whose windows overlap, so either could have taken effect first. */
    private void concurrent(RegisterHistory.Op first, RegisterHistory.Op second) {
        final long firstInvoked = ++tick;
        final long secondInvoked = ++tick;
        history.add(new RegisterHistory.Entry(first, firstInvoked, ++tick));
        history.add(new RegisterHistory.Entry(second, secondInvoked, ++tick));
    }

    private void assertLinearizable() {
        LinearizabilityChecker.check(history, 1L, "v0").assertLinearizable("this history");
    }

    private void assertNotLinearizable(String why) {
        final var verdict = LinearizabilityChecker.check(history, 1L, "v0");
        assertFalse(why + " -- but the checker accepted it, so it would accept a broken provider too", verdict.linearizable());
    }

    /** The ordinary case: one writer, one reader, nothing surprising. */
    public void testAnHonestSequentialHistoryIsAccepted() {
        sequential(new RegisterHistory.Read(1L, "v0"));
        sequential(new RegisterHistory.Cas(1L, "v1", true, 2L));
        sequential(new RegisterHistory.Read(2L, "v1"));
        sequential(new RegisterHistory.Cas(1L, "v2", false, 2L));
        assertLinearizable();
    }

    /**
     * Two nodes race and one loses, which is the whole of failover.
     *
     * <p>The loser's refusal reports generation 2 — the winner's — which it then re-reads on. Both calls
     * overlapped, so the checker has to find the order in which the winner went first.
     */
    public void testAContendedSwapWithOneWinnerIsAccepted() {
        concurrent(new RegisterHistory.Cas(1L, "a", true, 2L), new RegisterHistory.Cas(1L, "b", false, 2L));
        sequential(new RegisterHistory.Read(2L, "a"));
        assertLinearizable();
    }

    /**
     * A read that returns a value already overwritten.
     *
     * <p>The two writes are strictly ordered — the second was invoked after the first returned — so by the
     * time the read is made, "v1" is gone. A replica that had not caught up would answer exactly this, and
     * a reader in this system would open a commit that is no longer current.
     */
    public void testAStaleReadIsRejected() {
        sequential(new RegisterHistory.Cas(1L, "v1", true, 2L));
        sequential(new RegisterHistory.Cas(2L, "v2", true, 3L));
        sequential(new RegisterHistory.Read(2L, "v1"));
        assertNotLinearizable("a read returned a value two writes out of date");
    }

    /**
     * Two swaps against the same generation both win.
     *
     * <p>This is the failure the whole design turns on: two nodes both believe they own the shard, both
     * publish at the same term, and the fence in §9.6 never fires because both sides think they are the
     * one holding it.
     */
    public void testTwoWinnersOnOneGenerationAreRejected() {
        concurrent(new RegisterHistory.Cas(1L, "a", true, 2L), new RegisterHistory.Cas(1L, "b", true, 2L));
        assertNotLinearizable("two swaps against one generation both reported success");
    }

    /**
     * A swap against the current generation that is refused anyway.
     *
     * <p>Not merely unhelpful: a node that is genuinely the owner is told it is not, and this design reads
     * that as "somebody else took the shard" and gives it up.
     */
    public void testARefusalAgainstTheCurrentGenerationIsRejected() {
        sequential(new RegisterHistory.Cas(1L, "v1", false, 1L));
        assertNotLinearizable("a swap against the generation actually stored was refused");
    }

    /**
     * A refusal that reports a generation nobody ever stored.
     *
     * <p>The contract says a failed swap reports the generation now actually stored, and a caller re-reads
     * on that number. A fictitious one sends the next swap against something that never existed, so it
     * fails for ever and the shard is never taken.
     */
    public void testARefusalReportingAGenerationNobodyStoredIsRejected() {
        sequential(new RegisterHistory.Cas(1L, "v1", true, 2L));
        sequential(new RegisterHistory.Cas(1L, "v2", false, 99L));
        assertNotLinearizable("a refusal reported a generation that was never stored");
    }

    /**
     * An applied write that leaves the generation where it was.
     *
     * <p>Part of the specification rather than a nicety: a generation that can repeat is a fence a fenced
     * writer can walk back through, because its stale number matches again.
     */
    public void testAnAppliedWriteThatDoesNotMoveTheGenerationIsRejected() {
        sequential(new RegisterHistory.Cas(1L, "v1", true, 1L));
        assertNotLinearizable("an applied write left the generation unchanged");
    }

    /**
     * Concurrency is a real defence, not a loophole.
     *
     * <p>The checker must not accept a violation merely because the calls overlapped. Here both reads
     * overlap the write, so each could legally see either value — but one sees the old value and the other
     * the new, and then a third read sees the old one again after the write returned. No order explains
     * that.
     */
    public void testOverlappingCallsDoNotExcuseAnImpossibleResult() {
        concurrent(new RegisterHistory.Cas(1L, "v1", true, 2L), new RegisterHistory.Read(2L, "v1"));
        sequential(new RegisterHistory.Read(1L, "v0"));
        assertNotLinearizable("a read after the write returned still saw the old value");
    }

    /** And the same shape, legal this time: the read overlapped, so seeing the old value is fine. */
    public void testAReadOverlappingAWriteMaySeeEitherValue() {
        concurrent(new RegisterHistory.Read(1L, "v0"), new RegisterHistory.Cas(1L, "v1", true, 2L));
        sequential(new RegisterHistory.Read(2L, "v1"));
        assertLinearizable();
    }

    /** A history longer than the search will take is refused loudly rather than silently truncated. */
    public void testAnOversizedHistoryIsRefusedRatherThanTruncated() {
        for (int i = 0; i < LinearizabilityChecker.MAX_ENTRIES + 1; i++) {
            sequential(new RegisterHistory.Read(1L, "v0"));
        }
        final var thrown = expectThrows(IllegalArgumentException.class, () -> LinearizabilityChecker.check(history, 1L, "v0"));
        assertTrue(thrown.getMessage(), thrown.getMessage().contains("beyond what this checker will search"));
    }
}
