/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.serverless.testkit.CrashPointBlobStore.Fault;
import org.opensearch.serverless.testkit.CrashPointBlobStore.Step;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Runs a scenario once to learn which store calls it makes, then again with a fault at each of those calls.
 *
 * <p>The question it answers is the one a crash-safety argument has to survive: whatever single request a writer
 * dies at, or loses the answer to, does recovery still find everything that was acknowledged and nothing that was
 * not? Picking crash points by hand answers that only for the points somebody thought of. Recording the calls
 * first and faulting each in turn covers the ones nobody did, and a new store call added to the write path is
 * covered the moment it exists.
 *
 * <p>Each run gets its own root path in the same store, so one bucket or directory serves the whole exploration.
 * The scenario must be single-threaded and deterministic: the explorer checks that every faulted run makes the
 * same calls as the recorded run up to the fault, and fails loudly if it does not, because a fault injected at a
 * different call from the one recorded tests nothing that was asked.
 */
public final class CrashPointExplorer {

    /** What one writer does. It records what it acknowledged in the ledger and may throw at any point. */
    @FunctionalInterface
    public interface Scenario {
        /**
         * Runs the writer.
         *
         * @param store the writer's view of the store, which may fault
         * @param root the run's own root path
         * @param ledger where the writer records what it attempted and acknowledged
         * @throws Exception whatever the writer throws when a fault reaches it
         */
        void run(CrashPointBlobStore store, BlobPath root, Ledger ledger) throws Exception;
    }

    /** What must hold after the writer stopped, checked through the honest store. */
    @FunctionalInterface
    public interface Check {
        /**
         * Recovers and checks.
         *
         * @param store the store without faults, as a successor sees it
         * @param root the run's root path
         * @param ledger what the writer attempted and acknowledged
         * @param where the fault, or null for the recorded run
         * @throws Exception if recovery fails or a property does not hold
         */
        void verify(BlobStore store, BlobPath root, Ledger ledger, Point where) throws Exception;
    }

    /**
     * A fault at one step.
     *
     * @param step the store call
     * @param fault what happened at it
     */
    public record Point(Step step, Fault fault) {
        @Override
        public String toString() {
            return fault + " at " + step;
        }
    }

    /** What a writer attempted and what it told its callers succeeded. */
    public static final class Ledger {
        private final Set<String> attempted = new LinkedHashSet<>();
        private final Set<String> acknowledged = new LinkedHashSet<>();

        /**
         * Records that a write was sent towards the store. A write never attempted must never be recovered.
         *
         * @param id the write's id
         */
        public void attempted(String id) {
            attempted.add(id);
        }

        /**
         * Records that a write was acknowledged to its caller. An acknowledged write must always be recovered.
         *
         * @param id the write's id
         */
        public void acknowledged(String id) {
            if (attempted.contains(id) == false) {
                throw new IllegalStateException("acknowledged " + id + " without attempting it");
            }
            acknowledged.add(id);
        }

        /** @return the attempted ids, in order */
        public Set<String> attempted() {
            return Collections.unmodifiableSet(attempted);
        }

        /** @return the acknowledged ids, in order */
        public Set<String> acknowledged() {
            return Collections.unmodifiableSet(acknowledged);
        }

        /**
         * Asserts the recovered ids hold every acknowledged write and only attempted ones.
         *
         * @param recovered the ids recovery produced
         * @param where the fault, for the message
         */
        public void assertRecovered(Set<String> recovered, Object where) {
            final Set<String> lost = new LinkedHashSet<>(acknowledged);
            lost.removeAll(recovered);
            if (lost.isEmpty() == false) {
                throw new AssertionError("with " + where + ": acknowledged writes were lost: " + lost + "; recovered " + recovered);
            }
            final Set<String> invented = new LinkedHashSet<>(recovered);
            invented.removeAll(attempted);
            if (invented.isEmpty() == false) {
                throw new AssertionError("with " + where + ": recovered writes nobody attempted: " + invented);
            }
        }
    }

    /**
     * What an exploration covered.
     *
     * @param recorded the calls the fault-free run made
     * @param explored every fault point that was run and checked
     */
    public record Result(List<Step> recorded, List<Point> explored) {
    }

    private CrashPointExplorer() {}

    /**
     * Explores every fault in {@code faults} at every recorded step {@code which} accepts.
     *
     * @param store the shared store
     * @param base a path under which each run gets its own root
     * @param scenario the writer
     * @param check the recovery and its properties
     * @param faults which faults to inject
     * @param which which recorded steps to fault
     * @return what was covered
     * @throws Exception if the scenario misbehaves without a fault or a check fails
     */
    public static Result explore(BlobStore store, BlobPath base, Scenario scenario, Check check, Set<Fault> faults, Predicate<Step> which)
        throws Exception {
        final BlobPath recordedRoot = base.add("recorded");
        final CrashPointBlobStore recorder = new CrashPointBlobStore(store);
        final Ledger recordedLedger = new Ledger();
        scenario.run(recorder, recordedRoot, recordedLedger);
        check.verify(store, recordedRoot, recordedLedger, null);
        final List<Step> recorded = recorder.steps();

        final List<Point> explored = new ArrayList<>();
        int run = 0;
        for (Step step : recorded) {
            if (which.test(step) == false) {
                continue;
            }
            for (Fault fault : faults) {
                final BlobPath root = base.add("run-" + run++);
                final CrashPointBlobStore faulty = new CrashPointBlobStore(store).at(step.index(), fault);
                final Ledger ledger = new Ledger();
                try {
                    scenario.run(faulty, root, ledger);
                } catch (Exception e) {
                    // The writer stopping because of the fault is the normal outcome. Anything else is a bug the
                    // fault exposed, in the writer or the scenario, and must not pass as an explored point.
                    if (CrashPointBlobStore.isInjected(e) == false) {
                        throw new AssertionError("with " + new Point(step, fault) + ": the writer failed for another reason", e);
                    }
                }
                final Point point = new Point(step, fault);
                assertSameCallsUpTo(recorded, faulty.steps(), step.index(), recordedRoot, root, point);
                check.verify(store, root, ledger, point);
                explored.add(point);
            }
        }
        return new Result(recorded, explored);
    }

    /**
     * Explores every fault at every recorded step.
     *
     * @param store the shared store
     * @param base a path under which each run gets its own root
     * @param scenario the writer
     * @param check the recovery and its properties
     * @return what was covered
     * @throws Exception if a check fails
     */
    public static Result exploreAll(BlobStore store, BlobPath base, Scenario scenario, Check check) throws Exception {
        return explore(store, base, scenario, check, EnumSet.allOf(Fault.class), step -> true);
    }

    private static void assertSameCallsUpTo(
        List<Step> recorded,
        List<Step> actual,
        int index,
        BlobPath recordedRoot,
        BlobPath root,
        Point point
    ) {
        if (actual.size() <= index) {
            throw new AssertionError("with " + point + ": the run stopped after " + actual.size() + " calls, before reaching the fault");
        }
        final String from = recordedRoot.buildAsString();
        final String to = root.buildAsString();
        for (int i = 0; i <= index; i++) {
            final String expected = recorded.get(i).what().replace(from, to);
            final String got = actual.get(i).what();
            if (expected.equals(got) == false) {
                throw new AssertionError(
                    "with " + point + ": the scenario is not deterministic; call #" + i + " was " + got + " where it was " + expected
                );
            }
        }
    }
}
