/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Decides whether a recorded register history could have happened on a linearizable register.
 *
 * <p><b>What is being checked.</b> A register is linearizable if every call appears to take effect at one
 * instant between when it was made and when it returned, and one single sequential order explains every
 * result. So the question is a search: is there an order of these calls, consistent with the real-time
 * windows they were in flight for, that a correct register would have produced?
 *
 * <p><b>The sequential specification.</b> The register holds a generation and a value.
 *
 * <ul>
 *   <li>A read returns exactly the current generation and value.</li>
 *   <li>A compare-and-swap against the current generation applies, stores the new value, and reports a
 *       generation that is <em>different</em> from the one it replaced.</li>
 *   <li>A compare-and-swap against any other generation does not apply, changes nothing, and reports the
 *       generation actually stored — which is what {@code BlobContainer#compareAndSwapRegister} promises
 *       and what a caller re-reading after a lost race depends on.</li>
 * </ul>
 *
 * <p>That an applied write advances the generation is part of the specification rather than an extra: a
 * register whose generation could repeat would let a fenced writer's stale generation match again later,
 * which is the whole mechanism this design uses to stop a zombie.
 *
 * <p><b>The algorithm</b> is Wing and Gong's: repeatedly pick a call that could go next — one invoked
 * before the earliest return still outstanding, since anything invoked later cannot precede it — apply it
 * to the model, and recurse; on failure, put it back and try another. Exponential in the worst case, which
 * is why histories are checked in small rounds rather than one long one, and why failed states are
 * remembered so the same dead end is not explored twice.
 *
 * <p><b>What it does not check.</b> Calls that threw are not in the history. An operation with no result
 * may or may not have taken effect, and admitting that needs a model where the generation after an applied
 * write is unknown rather than observed — which weakens every subsequent read into a guess and makes the
 * search much larger. That case is real (a retried conditional write after a dropped connection is exactly
 * it) and it is covered a different way: {@link MisbehavingBlobContainer} can produce it, and the tests
 * ask what the <em>shell</em> does about it rather than asking this checker to model it.
 */
public final class LinearizabilityChecker {

    /** The most calls one history may contain, bounded by the bitmask used to remember dead ends. */
    public static final int MAX_ENTRIES = 62;

    private LinearizabilityChecker() {}

    /** What the checker concluded, and enough to say why if the answer is no. */
    public record Verdict(boolean linearizable, String reason) {

        /**
         * Throws if the history is not linearizable.
         *
         * @param context what was being checked, for the message
         */
        public void assertLinearizable(String context) {
            if (linearizable == false) {
                throw new AssertionError(context + ": " + reason);
            }
        }
    }

    private record State(long generation, String value) {
    }

    private record DeadEnd(State state, long remaining) {
    }

    /**
     * Checks a history.
     *
     * @param history what happened
     * @param initialGeneration the register's generation before the history began
     * @param initialValue the register's value before the history began
     * @return the verdict
     */
    public static Verdict check(List<RegisterHistory.Entry> history, long initialGeneration, String initialValue) {
        if (history.size() > MAX_ENTRIES) {
            throw new IllegalArgumentException(
                "a history of " + history.size() + " calls is beyond what this checker will search; keep rounds under " + MAX_ENTRIES
            );
        }
        final List<RegisterHistory.Entry> entries = new ArrayList<>(history);
        entries.sort(java.util.Comparator.comparingLong(RegisterHistory.Entry::invoked));
        final long all = entries.size() == 64 ? -1L : (1L << entries.size()) - 1;
        final boolean found = search(entries, all, new State(initialGeneration, initialValue), new HashSet<>());
        if (found) {
            return new Verdict(true, "");
        }
        return new Verdict(
            false,
            "no sequential order of these "
                + entries.size()
                + " calls explains what they returned, so the register did not behave like one register. History: "
                + describe(entries)
        );
    }

    private static boolean search(List<RegisterHistory.Entry> entries, long remaining, State state, Set<DeadEnd> seen) {
        if (remaining == 0) {
            return true;
        }
        if (seen.add(new DeadEnd(state, remaining)) == false) {
            // This exact position has already been searched and lost. Nothing about how we arrived here
            // changes what can follow.
            return false;
        }
        // Anything invoked after the earliest outstanding return cannot be the next to take effect: that
        // call had already finished before this one was made.
        long earliestReturn = Long.MAX_VALUE;
        for (int i = 0; i < entries.size(); i++) {
            if ((remaining & (1L << i)) != 0) {
                earliestReturn = Math.min(earliestReturn, entries.get(i).returned());
            }
        }
        for (int i = 0; i < entries.size(); i++) {
            if ((remaining & (1L << i)) == 0) {
                continue;
            }
            final RegisterHistory.Entry entry = entries.get(i);
            if (entry.invoked() > earliestReturn) {
                continue;
            }
            final State next = apply(state, entry.op());
            if (next == null) {
                continue;
            }
            if (search(entries, remaining & ~(1L << i), next, seen)) {
                return true;
            }
        }
        return false;
    }

    /** The sequential specification: the state after this call, or null if a correct register could not. */
    private static State apply(State state, RegisterHistory.Op op) {
        if (op instanceof RegisterHistory.Read read) {
            if (read.generation() != state.generation() || Objects.equals(read.value(), state.value()) == false) {
                return null;
            }
            return state;
        }
        final RegisterHistory.Cas cas = (RegisterHistory.Cas) op;
        if (cas.expected() == state.generation()) {
            if (cas.applied() == false) {
                // It swapped against what was there and was refused: no correct register does that.
                return null;
            }
            if (cas.currentGeneration() == state.generation()) {
                // Applied, and the generation did not move. A generation that can repeat is a fence a
                // zombie can walk back through.
                return null;
            }
            return new State(cas.currentGeneration(), cas.value());
        }
        if (cas.applied()) {
            // It swapped against a generation that was not current and won anyway. This is the two-winners
            // failure: it is how two nodes come to believe they own one shard.
            return null;
        }
        if (cas.currentGeneration() != state.generation()) {
            // It lost, and reported a generation that was not the one stored. A caller re-reads on this
            // number; a wrong one sends it to swap against something that was never there.
            return null;
        }
        return state;
    }

    private static String describe(List<RegisterHistory.Entry> entries) {
        final StringBuilder text = new StringBuilder();
        for (RegisterHistory.Entry entry : entries) {
            text.append("\n  [").append(entry.invoked()).append("..").append(entry.returned()).append("] ");
            if (entry.op() instanceof RegisterHistory.Read read) {
                text.append("read -> gen ").append(read.generation()).append(" value ").append(read.value());
            } else {
                final RegisterHistory.Cas cas = (RegisterHistory.Cas) entry.op();
                text.append("cas expecting ")
                    .append(cas.expected())
                    .append(" writing ")
                    .append(cas.value())
                    .append(" -> ")
                    .append(cas.applied() ? "applied" : "refused")
                    .append(", now gen ")
                    .append(cas.currentGeneration());
            }
        }
        return text.toString();
    }
}
