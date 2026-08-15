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

    /**
     * The namespace a serverless index's name lives in, and the whole of what decides gating.
     *
     * <h4>Why a name and not a setting</h4>
     *
     * Gating used to be inferred: from the request's settings, and since T49 from the settings a template
     * resolves too. Inference has three costs this removes. It cannot be exact -- a creation was admitted on
     * a guess and had to carry a fallback for the times the real gate disagreed, which is why a gated
     * creation could only be distributed for the cases that were *certainly* gated. It made the same index
     * name mean either plane, so a gated index and an ordinary index could both claim it: neither creation
     * path consults the other's authority, and {@code GatedAndOrdinaryNameCollisionIT} measured both being
     * granted, sequentially, with no concurrency involved. And it put the answer somewhere a caller holding
     * only a name could not see it.
     *
     * <p>A name settles all three at once, and settles the collision by construction rather than by
     * agreement: the two namespaces are disjoint, so there is no window in which both could be claimed and
     * nothing for two authorities to race over. The check is a string comparison, which is why it can live
     * wherever it is needed -- including inside the cluster state update task, where a descriptor read is
     * the deadlock W4 paid for.
     *
     * <p><b>The setting keeps its old meaning</b> and stops being the decider. It still turns on serverless
     * storage and computed placement, and the plugin's setting provider derives it for a name in this
     * namespace, so everything downstream of gating is untouched by this.
     */
    public static final String SERVERLESS_NAME_PREFIX = "serverless_";

    /**
     * Whether this name is in the serverless namespace.
     *
     * <p>Total on the name alone: no cluster state, no templates, no store. That is the property every
     * caller here relies on, and the reason this is a constant and a string comparison rather than a
     * registry.
     */
    public static boolean namesAServerlessIndex(String indexName) {
        return indexName != null && indexName.startsWith(SERVERLESS_NAME_PREFIX);
    }

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
     * <h4>What used to be here: the settings-based admission road, and why it is gone</h4>
     *
     * A second registry answered "is this request worth taking off the cluster state update thread" from the
     * request's settings, because the real gate above needs finished {@link IndexMetadata} and building that
     * is the expensive part the road exists to avoid. It was never able to be exact. T49 found that an index
     * gated only by a *template* said nothing in its request, took the ordinary road, and met the gate on the
     * state update thread -- where being gated means a blocking mapping write on the one thread that must not
     * block -- so admission had to be handed template-merged settings, at the cost of resolving templates on
     * the request path. T52 then found three cases where that resolution disagreed with what creation itself
     * resolves (a resize target, a data stream backing index, a system index), each over-admitted and paying
     * for a whole creation twice. And even correct, it could only reach *probably* gated, so a creation could
     * be admitted and then declined, which needed a fallback onto a cluster state update task, which only the
     * cluster manager can publish -- which is why distributing creation had to be limited to the cases that
     * were certainly gated.
     *
     * <p>Every one of those problems was the same problem: gating was inferred from settings, and settings
     * are not knowable early. {@link #namesAServerlessIndex} is knowable at every point on the path, agrees
     * with the gate by construction because the gate reads the same name, and needs no registry to answer.
     * The two predicates, the template cache, the merged-settings computation and the fallback road were all
     * removed together; what replaced them is a string comparison.
     */
}
