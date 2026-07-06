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
 * Computes bundle liveness as a reference count over <strong>all</strong> live manifests, not
 * just the ones belonging to the bundle's own index. This is deliberate: a zero-copy clone
 * (rfc-serverless-opensearch.md &sect;14) creates manifests in a new index that reference an
 * existing index's bundles, so "does any manifest reference this bundle" must be evaluated
 * globally &mdash; scoping it to one index would let a clone's source deletion silently corrupt
 * the clone. See &sect;6.5 and the GC model-check requirement in &sect;18.5.
 */
public final class BundleReferenceCounter {

    private BundleReferenceCounter() {}

    /** The union of every bundle name referenced by any of the given (already-filtered-to-live) manifests. */
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
