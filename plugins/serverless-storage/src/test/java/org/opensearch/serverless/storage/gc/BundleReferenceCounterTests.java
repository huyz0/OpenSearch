/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BundleReferenceCounterTests extends OpenSearchTestCase {

    private static CommitManifest manifestReferencing(String index, int shard, long term, long generation, String... bundleNames) {
        Map<String, FileReference> files = new java.util.LinkedHashMap<>();
        String segmentsFile = "segments_" + generation;
        for (int i = 0; i < bundleNames.length; i++) {
            String fileName = i == 0 ? segmentsFile : "_" + i + ".si";
            files.put(fileName, new FileReference(bundleNames[i], 0, 10, 1L));
        }
        return new CommitManifest(index, shard, term, generation, segmentsFile, files, 0, 0, null, 0, PruningStats.empty(), 0L);
    }

    public void testUnreferencedBundleIsDeletable() {
        CommitManifest manifest = manifestReferencing("idx", 0, 1, 0, "bundle-A");
        Set<String> live = BundleReferenceCounter.computeLiveBundles(List.of(manifest));

        Set<String> deletable = BundleReferenceCounter.computeDeletableBundles(List.of("bundle-A", "bundle-B"), live);

        assertEquals(Set.of("bundle-B"), deletable);
    }

    public void testBundleReferencedByAnyLiveManifestIsNotDeletable() {
        CommitManifest manifestA = manifestReferencing("idx", 0, 1, 0, "bundle-shared");
        CommitManifest manifestB = manifestReferencing("idx", 1, 1, 0, "bundle-shared");

        Set<String> live = BundleReferenceCounter.computeLiveBundles(List.of(manifestA, manifestB));
        Set<String> deletable = BundleReferenceCounter.computeDeletableBundles(List.of("bundle-shared"), live);

        assertEquals(Set.of(), deletable);
    }

    /**
     * The scenario rfc-serverless-opensearch.md section 6.5/14 calls out explicitly: a clone
     * creates manifests under a brand new index UUID that reference the source index's bundles.
     * If bundle liveness were scoped per-index, deleting the source index (whose own manifests no
     * longer reference the bundle) would let GC delete a bundle the clone still needs -- silent,
     * unrecoverable data corruption in the clone. Liveness must be a *global* reference count.
     */
    public void testCloneKeepsSourceBundleAliveAfterSourceIndexIsLogicallyDeleted() {
        String sourceIndex = "source-index-uuid";
        String cloneIndex = "clone-index-uuid";
        String sharedBundle = "bundle-from-source";

        CommitManifest cloneManifest = manifestReferencing(cloneIndex, 0, 1, 0, sharedBundle);
        // The source index has been deleted: none of ITS manifests are live any more. Only the
        // clone's manifest remains in the "live manifests" view passed to the reference counter.
        List<CommitManifest> liveManifestsAfterSourceDeletion = List.of(cloneManifest);

        Set<String> live = BundleReferenceCounter.computeLiveBundles(liveManifestsAfterSourceDeletion);
        Set<String> deletable = BundleReferenceCounter.computeDeletableBundles(List.of(sharedBundle), live);

        assertEquals(
            "bundle referenced by a clone's manifest must never be deletable, even after the source index is gone",
            Set.of(),
            deletable
        );
    }

    public void testBundleBecomesDeletableOnlyAfterBothSourceAndCloneStopReferencingIt() {
        String sharedBundle = "bundle-shared";
        CommitManifest sourceManifest = manifestReferencing("source", 0, 1, 0, sharedBundle);
        CommitManifest cloneManifest = manifestReferencing("clone", 0, 1, 0, sharedBundle);

        // Both still reference it: not deletable.
        Set<String> liveBoth = BundleReferenceCounter.computeLiveBundles(List.of(sourceManifest, cloneManifest));
        assertEquals(Set.of(), BundleReferenceCounter.computeDeletableBundles(List.of(sharedBundle), liveBoth));

        // Source compacted away its reference (new manifest points at a merged bundle instead),
        // but the clone still references the original bundle: still not deletable.
        CommitManifest sourceAfterCompaction = manifestReferencing("source", 0, 1, 1, "bundle-merged");
        Set<String> liveAfterCompaction = BundleReferenceCounter.computeLiveBundles(List.of(sourceAfterCompaction, cloneManifest));
        assertEquals(Set.of(), BundleReferenceCounter.computeDeletableBundles(List.of(sharedBundle, "bundle-merged"), liveAfterCompaction));

        // Now the clone is also gone: no live manifest anywhere references the shared bundle.
        Set<String> liveAfterCloneGone = BundleReferenceCounter.computeLiveBundles(List.of(sourceAfterCompaction));
        assertEquals(
            Set.of(sharedBundle),
            BundleReferenceCounter.computeDeletableBundles(List.of(sharedBundle, "bundle-merged"), liveAfterCloneGone)
        );
    }

    public void testMultipleFilesInOneManifestAllKeepTheirBundlesAlive() {
        CommitManifest manifest = manifestReferencing("idx", 0, 1, 0, "bundle-A", "bundle-B", "bundle-C");
        Set<String> live = BundleReferenceCounter.computeLiveBundles(List.of(manifest));
        assertEquals(Set.of("bundle-A", "bundle-B", "bundle-C"), live);
        assertEquals(Set.of(), BundleReferenceCounter.computeDeletableBundles(List.of("bundle-A", "bundle-B", "bundle-C"), live));
    }

    public void testNoLiveManifestsMeansAllKnownBundlesAreDeletable() {
        Set<String> deletable = BundleReferenceCounter.computeDeletableBundles(List.of("bundle-A", "bundle-B"), Set.of());
        assertEquals(Set.of("bundle-A", "bundle-B"), deletable);
    }
}
