/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.settings.Settings;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * The gate that decides whether an index gets a cluster state entry at all.
 *
 * <p>This is Area H's third phase in one predicate. When it answers true for an index, creation records a
 * descriptor and writes nothing to cluster state: no metadata entry, no routing entry, no publication, and
 * none of the O(total indices) work that S19 and S20 measured. Creation becomes one indexing operation
 * whose cost is independent of how many indices exist.
 *
 * <p><b>The measurement that decides whether this was worth building</b> is G2a re-run with the gate open:
 * per-index creation cost against population, today 7.4, 10.3, 34.6 and 98.8 ms at 200, 1k, 3k and 6k. If
 * that curve is not flat, something else is superlinear and Area H should stop rather than continue.
 *
 * <p><b>Why a predicate rather than a setting.</b> The decision is per index, not per cluster: the same
 * cluster carries ordinary indices whose metadata must stay in cluster state and serverless ones whose
 * must not. A setting would force the choice for everything at once, and the migration this area needs is
 * exactly the ability to run both at the same time.
 *
 * <p><b>Failure semantics invert here</b>, and that is the part most easily got wrong. During dual write
 * (H2b) the descriptor was redundant, so a publisher that threw could be swallowed. Once this gate is
 * open the descriptor is the only record of the index, so a failed descriptor write must fail the
 * creation. An index that exists in neither place is a lost request; an index whose descriptor write
 * silently failed is a lost index.
 *
 * <p>Closed by default. Nothing changes until something opens it.
 */
public final class DescriptorOnlyCreation {

    private static final AtomicReference<Predicate<IndexMetadata>> GATE = new AtomicReference<>();

    private static final AtomicReference<Predicate<Settings>> ADMISSION = new AtomicReference<>();

    private DescriptorOnlyCreation() {}

    /** Opens the gate for indices the predicate accepts. Registering null closes it entirely. */
    public static void register(Predicate<IndexMetadata> gate) {
        GATE.set(gate);
    }

    public static boolean isRegistered() {
        return GATE.get() != null;
    }

    /**
     * Whether this index should skip its cluster state entry.
     *
     * <p>A predicate that throws answers false, so a broken gate leaves the index in cluster state rather
     * than nowhere. That direction is deliberate: the failure mode of answering true wrongly is an index
     * with no record anywhere, and the failure mode of answering false wrongly is an index that costs
     * what it always cost.
     */
    public static boolean skipsClusterState(IndexMetadata indexMetadata) {
        Predicate<IndexMetadata> gate = GATE.get();
        if (gate == null || indexMetadata == null) {
            return false;
        }
        try {
            return gate.test(indexMetadata);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Registers the cheap check that decides whether a creation request is worth admitting off the
     * cluster-manager's state update thread. Registering null restores the ordinary path for everything.
     */
    public static void registerAdmissionCheck(Predicate<Settings> admission) {
        ADMISSION.set(admission);
    }

    /** Whether anything is asking, so a cluster with no gate does not pay to prepare an answer. */
    public static boolean hasAdmissionCheck() {
        return ADMISSION.get() != null;
    }

    /**
     * Whether a creation request looks gated enough to be admitted without the cluster state update thread.
     *
     * <h4>Why this exists when {@link #skipsClusterState} already answers the question</h4>
     *
     * {@link #skipsClusterState} takes the finished {@link IndexMetadata}, and building that is the expensive
     * part: templates resolved, settings aggregated, and a whole throwaway {@code IndexService} constructed to
     * validate the mapping. By the time it can be asked, the work it was supposed to let us avoid has already
     * been done, and it has been done on the one thread in the cluster that serialises state updates. That is
     * why gating removed publication and left creation costing what it always cost.
     *
     * <p>So the decision has to be made earlier, from the only thing available earlier: the request's own
     * settings. This is that check, and it is deliberately a different question. It does not decide whether
     * the index is gated -- {@link #skipsClusterState} still decides that, in the same place as before, from
     * the same finished metadata. It decides only <em>which road the request takes to get there</em>.
     *
     * <h4>The two ways it can be wrong are not symmetric, and that sets its direction</h4>
     *
     * Answering false for a request that turns out gated was believed to cost nothing but speed: the request
     * takes the ordinary road, reaches the same gate at the bottom, and is gated there. That was wrong, and
     * round 004's T49 is why this method is now given template-merged settings rather than the request's own.
     * The gate at the bottom runs on the state update thread, and gating there means writing the index's
     * declared mapping to a store whose writes block -- on the one thread that must never block, by a caller
     * that had already decided this request was not gated. An index gated only by a template took exactly
     * that road, because a template's settings are not in the request.
     *
     * <p>Answering true for a request that turns out <em>not</em> gated costs a repeat: the off-thread attempt
     * finds the index needs its cluster state entry after all, discards what it built, and falls back to the
     * ordinary path, which redoes it. Correct, and paid for twice.
     *
     * <p>So one direction is safe and the other is not, which is why the caller now resolves templates before
     * asking: over-admitting is absorbed by a fallback that exists, and under-admitting is the defect. This
     * check should stay liberal for the same reason. What it must never be is the third
     * thing -- a request admitted off-thread that then writes to cluster state from a thread that may not.
     * That cannot happen here, because this predicate does not authorise the write; it only chooses a road.
     *
     * <p>Unregistered answers false, so a cluster with no serverless plugin takes exactly the path it took
     * before this existed.
     */
    public static boolean mayBypassClusterState(Settings requestSettings) {
        Predicate<Settings> admission = ADMISSION.get();
        if (admission == null || requestSettings == null) {
            return false;
        }
        try {
            return admission.test(requestSettings);
        } catch (Exception e) {
            // Same direction as the gate above: a broken check sends the request down the road that has
            // always worked rather than the new one.
            return false;
        }
    }
}
