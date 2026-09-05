/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What actually happened to a register, recorded so it can be judged afterwards.
 *
 * <p><b>Why a history rather than more assertions.</b> The conformance suite asks specific questions —
 * do two contenders produce one winner, does a stale generation lose. Each is a real property and each
 * looks at one scenario somebody thought of. Linearizability is not a scenario; it is a property of every
 * possible interleaving, and the way to test it is to run a concurrent workload, write down what every
 * call returned and when, and then ask whether <em>any</em> sequential order explains all of it.
 *
 * <p>That catches the deviations nobody wrote a scenario for: a read that returns a value already
 * overwritten, a failed compare-and-swap that reports a generation nobody ever stored, an ordering that is
 * fine pairwise and impossible taken together.
 *
 * <p><b>Time is a counter, not a clock.</b> Every event takes a number from one shared counter, at the
 * moment it happens. Two events cannot get the same number, and the numbers respect real time — which is
 * all linearizability needs, and is more than a wall clock would give across threads.
 *
 * <p><b>Only completed operations are recorded.</b> An operation that threw may or may not have taken
 * effect, and a checker that admits "maybe it happened" needs a model where the resulting generation is
 * unknown — see {@link LinearizabilityChecker} for why that is left out and what is done instead.
 */
public final class RegisterHistory {

    /** A generation meaning the register was not there. */
    public static final long ABSENT = -1L;

    /** What a call returned, and when it was in flight. */
    public sealed interface Op permits Read, Cas {}

    /**
     * A read and what it saw.
     *
     * @param generation the generation returned, or {@link #ABSENT}
     * @param value the value returned, or null when absent
     */
    public record Read(long generation, String value) implements Op {
    }

    /**
     * A compare-and-swap and what it reported.
     *
     * @param expected the generation it swapped against
     * @param value the value it tried to write
     * @param applied whether it reported success
     * @param currentGeneration the generation it reported as now stored
     */
    public record Cas(long expected, String value, boolean applied, long currentGeneration) implements Op {
    }

    /**
     * One call, with the window it was in flight for.
     *
     * @param op what it was and what it returned
     * @param invoked the tick at which the call was made
     * @param returned the tick at which it came back
     */
    public record Entry(Op op, long invoked, long returned) {
    }

    private final AtomicLong tick = new AtomicLong();
    private final List<Entry> entries = Collections.synchronizedList(new ArrayList<>());

    /**
     * Takes the next tick, to be used as an invocation time.
     *
     * @return the tick
     */
    public long invoke() {
        return tick.incrementAndGet();
    }

    /**
     * Records a completed call.
     *
     * @param op what it returned
     * @param invoked the tick taken before the call
     */
    public void completed(Op op, long invoked) {
        entries.add(new Entry(op, invoked, tick.incrementAndGet()));
    }

    /**
     * Returns everything recorded, in invocation order.
     *
     * @return the entries
     */
    public List<Entry> entries() {
        synchronized (entries) {
            final List<Entry> copy = new ArrayList<>(entries);
            copy.sort(java.util.Comparator.comparingLong(Entry::invoked));
            return copy;
        }
    }

    /**
     * Returns how many calls were recorded.
     *
     * @return the count
     */
    public int size() {
        return entries.size();
    }
}
