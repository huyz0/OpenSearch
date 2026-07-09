/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.serverless.storage.manifest.CommitManifest;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Computes bundle liveness as a reference count over whatever collection of live manifests it is
 * given. In principle this should be evaluated over <strong>all</strong> live manifests
 * cluster-wide, not just the ones belonging to the bundle's own index: a zero-copy clone
 * (rfc-serverless-opensearch.md &sect;14) creates manifests in a new index that reference an
 * existing index's bundles, so "does any manifest reference this bundle" is, in the fully general
 * case, a global question.
 *
 * <p>{@code GcSchedulerTask} does not do that -- it only ever evaluates this method over its own
 * shard's own manifests, never scanning other indices. That is safe today anyway, because
 * {@link org.opensearch.serverless.storage.clone.ShardCloner} closes the gap a different way: at
 * clone time it durably pins the exact source generation being cloned from in the source shard's
 * own {@link org.opensearch.serverless.storage.retention.DurablePinRegistry}, which the source
 * shard's own GC sweep already consults before ever calling this method -- so a pinned generation,
 * and everything it references, never becomes a candidate for {@link #computeLiveBundles} to
 * exclude in the first place. This is more precise than a global scan (it protects exactly the
 * generation actually cloned from, not every manifest across every index) and needs no change to
 * this class or {@code GcSchedulerTask}. See &sect;6.5, &sect;14, and the GC model-check
 * requirement in &sect;18.5 for the fuller picture this pin-based approach is a scoped slice of.
 */
public final class BundleReferenceCounter {

    private BundleReferenceCounter() {}

    /**
     * The union of every bundle name referenced by any of the given (already-filtered-to-live) manifests.
     *
     * @param liveManifests the manifests considered live, already filtered by the caller.
     * @return the union of bundle names referenced by any of them.
     */
    public static Set<String> computeLiveBundles(Collection<CommitManifest> liveManifests) {
        Set<String> live = new HashSet<>();
        for (CommitManifest manifest : liveManifests) {
            live.addAll(manifest.referencedBundles());
        }
        return live;
    }

    /**
     * Of the bundles known to exist in the object store, the subset referenced by none of
     * {@code liveBundles} &mdash; i.e. safe to delete. Callers are expected to additionally apply
     * a safety delay before actually issuing deletes (rfc-serverless-opensearch.md &sect;6.5:
     * "asynchronous, batched, and delayed by a safety window").
     *
     * @param allKnownBundles every bundle name known to exist in the object store.
     * @param liveBundles the bundle names still referenced by a live manifest.
     * @return the subset of {@code allKnownBundles} referenced by none of {@code liveBundles}.
     */
    public static Set<String> computeDeletableBundles(Collection<String> allKnownBundles, Set<String> liveBundles) {
        Set<String> deletable = new HashSet<>();
        for (String bundle : allKnownBundles) {
            if (!liveBundles.contains(bundle)) {
                deletable.add(bundle);
            }
        }
        return deletable;
    }
}
