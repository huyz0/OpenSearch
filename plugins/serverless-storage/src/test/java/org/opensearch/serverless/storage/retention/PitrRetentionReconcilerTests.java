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
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PitrRetentionReconcilerTests extends OpenSearchTestCase {

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

    private DurablePinRegistry newRegistry() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        return new BlobContainerDurablePinRegistry(blobContainer);
    }

    public void testFirstReconcileAddsPinsForEveryRequiredGeneration() throws Exception {
        DurablePinRegistry registry = newRegistry();
        PitrRetentionReconciler reconciler = new PitrRetentionReconciler(registry);
        CommitManifest inWindow = manifest(1, 0, 9500L);

        PitrRetentionReconciler.ReconcileResult result = reconciler.reconcile(INDEX_UUID, SHARD_ID, List.of(inWindow), 10_000L, 2_000L);

        assertEquals(1, result.added());
        assertEquals(0, result.removed());
        assertEquals(Set.of(new PinRecord("pitr", 1, 0)), registry.getPins(INDEX_UUID, SHARD_ID));
    }

    public void testReconcilingTwiceWithNoChangeIsANoOp() throws Exception {
        DurablePinRegistry registry = newRegistry();
        PitrRetentionReconciler reconciler = new PitrRetentionReconciler(registry);
        CommitManifest inWindow = manifest(1, 0, 9500L);

        reconciler.reconcile(INDEX_UUID, SHARD_ID, List.of(inWindow), 10_000L, 2_000L);
        PitrRetentionReconciler.ReconcileResult second = reconciler.reconcile(INDEX_UUID, SHARD_ID, List.of(inWindow), 10_000L, 2_000L);

        assertEquals(0, second.added());
        assertEquals(0, second.removed());
    }

    public void testAsTheWindowRollsForwardOldPinsAreRemovedAndNewOnesAdded() throws Exception {
        DurablePinRegistry registry = newRegistry();
        PitrRetentionReconciler reconciler = new PitrRetentionReconciler(registry);
        CommitManifest gen0 = manifest(1, 0, 8000L);
        CommitManifest gen1 = manifest(1, 1, 9500L);

        // At t=10000, window=2000 (cutoff=8000): both are required (gen0 is the "before cutoff"
        // fallback since nothing is older than it).
        reconciler.reconcile(INDEX_UUID, SHARD_ID, List.of(gen0, gen1), 10_000L, 2_000L);
        assertEquals(Set.of(new PinRecord("pitr", 1, 0), new PinRecord("pitr", 1, 1)), registry.getPins(INDEX_UUID, SHARD_ID));

        // Time moves on: at t=15000, window=2000 (cutoff=13000), a new manifest arrives at
        // t=14000. Now gen1 becomes the "before cutoff" fallback and gen0 is no longer required.
        CommitManifest gen2 = manifest(1, 2, 14_000L);
        PitrRetentionReconciler.ReconcileResult result = reconciler.reconcile(
            INDEX_UUID,
            SHARD_ID,
            List.of(gen0, gen1, gen2),
            15_000L,
            2_000L
        );

        assertEquals(1, result.added());
        assertEquals(1, result.removed());
        assertEquals(Set.of(new PinRecord("pitr", 1, 1), new PinRecord("pitr", 1, 2)), registry.getPins(INDEX_UUID, SHARD_ID));
    }

    public void testReconcileNeverTouchesPinsFromOtherReasons() throws Exception {
        DurablePinRegistry registry = newRegistry();
        PinRecord snapshotPin = new PinRecord("snapshot-1", 1, 0);
        registry.addPin(INDEX_UUID, SHARD_ID, snapshotPin);

        PitrRetentionReconciler reconciler = new PitrRetentionReconciler(registry);
        CommitManifest inWindow = manifest(1, 5, 9500L);
        reconciler.reconcile(INDEX_UUID, SHARD_ID, List.of(inWindow), 10_000L, 2_000L);

        Set<PinRecord> pins = registry.getPins(INDEX_UUID, SHARD_ID);
        assertTrue("the unrelated snapshot pin must survive untouched", pins.contains(snapshotPin));
        assertTrue(pins.contains(new PinRecord("pitr", 1, 5)));
    }
}
