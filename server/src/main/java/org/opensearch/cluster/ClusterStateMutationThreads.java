/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

/**
 * Phase C of {@code core-pluggability-refactor-plan.md}: identifies the threads on which it is unsafe to
 * consult a plugin-supplied {@code org.opensearch.cluster.metadata.IndexMetadataResolver} or {@code
 * org.opensearch.cluster.routing.IndexRoutingResolver}.
 *
 * <p><b>The deadlock this exists to prevent.</b> A resolver may need to do real work to answer -- in the
 * general case this SPI is designed for, a remote lookup -- and the cluster-state-mutation threads named
 * here (the cluster applier's and the cluster manager's own single update threads) need to make progress
 * before any such work could complete. A resolver invoked from one of them while it does that work would
 * therefore wait on itself: this node's own cluster-state processing is what would eventually let the
 * lookup finish, and the lookup is what the resolver is blocking that processing on.
 *
 * <p>This is not a new problem particular to this SPI -- the static registries this SPI's call-site
 * migration (Phase C4) is meant to replace, {@code AbsentIndexDescriptorSuppliers} and {@code
 * AbsentIndexRoutingSuppliers}, discovered and solved exactly this deadlock (their own javadoc records it,
 * with real investigation history) by keeping a thread-name check identical to this one and refusing to
 * call their primary, possibly-blocking supplier from an unsafe thread -- falling back instead to a second,
 * explicitly cache-only supplier tier. The single-method {@code IndexMetadataResolver}/{@code
 * IndexRoutingResolver} contract this SPI settled on deliberately has no such second tier -- see those
 * interfaces' own javadoc -- which makes this check load-bearing rather than optional: without it, a
 * resolver that behaves exactly like the existing primary supplier (a real remote lookup) would reintroduce
 * the identical deadlock the instant a plugin registers one, since {@code Metadata#index(String)}/{@code
 * ClusterState#getIndexRoutingTable(String)} consult a resolver unconditionally otherwise.
 *
 * <p>Consulted from the two resolver-consultation points core owns -- {@code Metadata#index(String)} and
 * {@code ClusterState#getIndexRoutingTable(String)} -- rather than trusted to every resolver implementation
 * individually: a plugin author forgetting this check is a production deadlock discovered on a cache miss
 * under load, not a compile error or an obvious test failure, exactly the failure mode the existing
 * registries' own history warns about. Enforcing it once, structurally, in core removes that failure mode
 * instead of documenting around it.
 */
public final class ClusterStateMutationThreads {

    /**
     * Thread-name substrings identical to the ones the pre-existing static registries already treat as
     * unsafe -- kept exactly in sync rather than re-derived, since that list is itself the product of real
     * production investigation, not a guess.
     */
    private static final String[] THREADS_WHERE_BLOCKING_IS_UNSAFE = {
        "clusterApplierService#updateTask",
        "clusterManagerService#updateTask",
        "masterService#updateTask" };

    private ClusterStateMutationThreads() {}

    /**
     * Whether the calling thread is one on which a resolver must not be consulted.
     *
     * <p>Matched on thread name, mirroring how {@link org.opensearch.cluster.service.ClusterService} and
     * {@link org.opensearch.cluster.service.ClusterApplierService} already assert the same property about
     * themselves -- see {@code ClusterApplierService#assertNotCalledFromClusterStateApplier}. It is a
     * weaker check than holding a reference to the executor, and it is the one available to a resolver
     * consultation point that has no services injected into it.
     */
    public static boolean blockingIsUnsafeOnCurrentThread() {
        String threadName = Thread.currentThread().getName();
        for (String unsafe : THREADS_WHERE_BLOCKING_IS_UNSAFE) {
            if (threadName.contains(unsafe)) {
                return true;
            }
        }
        return false;
    }
}
