/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

/**
 * Identifies the threads on which it is unsafe to consult a plugin-supplied {@code
 * org.opensearch.cluster.metadata.IndexCatalog}.
 *
 * <p><b>The deadlock this exists to prevent.</b> A catalog may need to do real work to answer -- in the
 * general case this SPI is designed for, a remote lookup -- and the cluster-state-mutation threads named
 * here (the cluster applier's and the cluster manager's own single update threads) need to make progress
 * before any such work could complete. A catalog invoked from one of them while it does that work would
 * therefore wait on itself: this node's own cluster-state processing is what would eventually let the
 * lookup finish, and the lookup is what the catalog is blocking that processing on.
 *
 * <p>This is not a new problem particular to this SPI -- the static registries this SPI's call-site
 * migration is meant to replace, {@code AbsentIndexDescriptorSuppliers} and {@code
 * AbsentIndexRoutingSuppliers}, discovered and solved exactly this deadlock (their own javadoc records it,
 * with real investigation history) by keeping a thread-name check identical to this one and refusing to
 * call their primary, possibly-blocking supplier from an unsafe thread -- falling back instead to a second,
 * explicitly cache-only supplier tier. The {@code IndexCatalog} contract this SPI settled on deliberately
 * has no such second tier -- see that interface's own javadoc -- which makes this check load-bearing rather
 * than optional: without it, a catalog that behaves exactly like the existing primary supplier (a real
 * remote lookup) would reintroduce the identical deadlock the instant a plugin registers one, since {@code
 * Metadata#indexOrResolved(String)}/{@code ClusterState#getIndexRoutingTable(String)} consult a catalog
 * unconditionally otherwise.
 *
 * <p>Consulted from the two catalog-consultation points core owns -- {@code Metadata#indexOrResolved(String)}
 * and {@code ClusterState#getIndexRoutingTable(String)} -- rather than trusted to every catalog
 * implementation individually: a plugin author forgetting this check is a production deadlock discovered on
 * a cache miss under load, not a compile error or an obvious test failure, exactly the failure mode the existing
 * registries' own history warns about. Enforcing it once, structurally, in core removes that failure mode
 * instead of documenting around it.
 */
public final class ClusterStateMutationThreads {

    /**
     * Thread-name substrings identical to the ones the pre-existing static registries already treat as
     * unsafe -- kept exactly in sync rather than re-derived, since that list is itself the product of real
     * production investigation, not a guess.
     *
     * <p><b>Deliberately shorter than {@code AbsentIndexDescriptorSuppliers}' list, which also names the
     * event-loop threads ({@code http_server_worker}, {@code transport_worker}).</b> The two lists answer
     * different questions and have different fallbacks, so they are no longer identical and must not be
     * "resynchronised" without reading this. That registry has a second, explicitly cache-only supplier
     * tier, so refusing there degrades a cold lookup to a fresh-cache-only one and a warm descriptor still
     * answers. This check has no second tier: its two consultation points ({@code
     * Metadata#indexOrResolved}, {@code ClusterState#getIndexRoutingTable}) return {@code null} outright
     * when it trips, which for a request coordinated on an event loop -- which is nearly all of them --
     * would mean "no such index" for every gated index, warm or cold. Since every catalog that actually
     * blocks reaches the object store through that registry's {@code supply}, guarding it there is what
     * removes the event-loop stall; adding the same names here would remove the feature instead.
     */
    private static final String[] THREADS_WHERE_BLOCKING_IS_UNSAFE = {
        "clusterApplierService#updateTask",
        "clusterManagerService#updateTask",
        "masterService#updateTask" };

    private ClusterStateMutationThreads() {}

    /**
     * Whether the calling thread is one on which a catalog must not be consulted.
     *
     * <p>Matched on thread name, mirroring how {@link org.opensearch.cluster.service.ClusterService} and
     * {@link org.opensearch.cluster.service.ClusterApplierService} already assert the same property about
     * themselves -- see {@code ClusterApplierService#assertNotCalledFromClusterStateApplier}. It is a
     * weaker check than holding a reference to the executor, and it is the one available to a catalog
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
