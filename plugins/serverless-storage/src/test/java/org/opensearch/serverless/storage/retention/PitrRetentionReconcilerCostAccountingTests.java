/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.security.ObjectStoreRequestCounter;
import org.opensearch.serverless.storage.security.RequestCountingBlobContainer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * rfc-serverless-opensearch.md &sect;17's own still-open "GC sweep and PITR reconciliation's own
 * request-cost profiles remain ungated" gap, the PITR half: pins down {@link PitrRetentionReconciler#reconcile}'s
 * real request cost as a function of how many pins actually change, not of how many manifests it
 * was handed -- the same "regardless of N" shape {@code CostAccountingRegressionTests} already
 * established for compaction's PUT cost, adapted to this class's own GET+PUT-per-pin-change shape
 * ({@link BlobContainerDurablePinRegistry#addPin}/{@link BlobContainerDurablePinRegistry#removePin}
 * are each an independent read-modify-CAS cycle, not batched into one register write).
 */
public class PitrRetentionReconcilerCostAccountingTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private static CommitManifest manifest(long term, long generation, long createdAtMillis) {
        return new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            term,
            generation,
            "segments_" + generation,
            Map.of("segments_" + generation, new FileReference("bundle-" + term + "-" + generation, 0, 10, 1L)),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            createdAtMillis
        );
    }

    public void testFirstReconcileOverManyManifestsCostsExactlyOneGetPlusOneGetPutPairPerPinAdded() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer uncountedContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        RequestCountingBlobContainer countingContainer = new RequestCountingBlobContainer(uncountedContainer, counter);
        DurablePinRegistry registry = new BlobContainerDurablePinRegistry(countingContainer);
        PitrRetentionReconciler reconciler = new PitrRetentionReconciler(registry);

        // 5 manifests all inside a wide window -- every one of them needs its own pin on this,
        // the very first, reconciliation. The request cost must scale with this count (5 pins
        // added), never with some larger, unrelated multiple of it.
        int manifestCount = 5;
        List<CommitManifest> manifests = new java.util.ArrayList<>();
        for (int generation = 0; generation < manifestCount; generation++) {
            manifests.add(manifest(1, generation, 9000L + generation));
        }

        PitrRetentionReconciler.ReconcileResult result = reconciler.reconcile(INDEX_UUID, SHARD_ID, manifests, 10_000L, 5_000L);

        assertEquals(manifestCount, result.added());
        assertEquals(0, result.removed());
        // 1 initial getPins() read, plus one more GET per pin added (BlobContainerDurablePinRegistry#addPin's
        // own read-before-CAS) -- see this class's own javadoc for why each addition isn't batched.
        assertEquals(
            "reconciling "
                + manifestCount
                + " new pins must cost exactly 1 (initial read) + "
                + manifestCount
                + " (one read per addPin) GETs -- got "
                + counter.getCount()
                + ", a change likely altered how many reads each pin change costs",
            1L + manifestCount,
            counter.getCount()
        );
        assertEquals(
            "reconciling " + manifestCount + " new pins must cost exactly " + manifestCount + " PUTs (one CAS per addPin)",
            (long) manifestCount,
            counter.putCount()
        );
    }

    public void testReconcilingWithNoChangeCostsOnlyTheInitialRead() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer uncountedContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        DurablePinRegistry uncountedRegistry = new BlobContainerDurablePinRegistry(uncountedContainer);
        PitrRetentionReconciler setupReconciler = new PitrRetentionReconciler(uncountedRegistry);
        CommitManifest inWindow = manifest(1, 0, 9500L);
        setupReconciler.reconcile(INDEX_UUID, SHARD_ID, List.of(inWindow), 10_000L, 2_000L);

        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        RequestCountingBlobContainer countingContainer = new RequestCountingBlobContainer(uncountedContainer, counter);
        PitrRetentionReconciler reconciler = new PitrRetentionReconciler(new BlobContainerDurablePinRegistry(countingContainer));

        PitrRetentionReconciler.ReconcileResult result = reconciler.reconcile(INDEX_UUID, SHARD_ID, List.of(inWindow), 10_000L, 2_000L);

        assertEquals(0, result.added());
        assertEquals(0, result.removed());
        assertEquals(
            "a steady-state reconcile with nothing to change must cost only its own initial getPins() read",
            1L,
            counter.getCount()
        );
        assertEquals("a steady-state reconcile with nothing to change must never write anything", 0L, counter.putCount());
    }
}
