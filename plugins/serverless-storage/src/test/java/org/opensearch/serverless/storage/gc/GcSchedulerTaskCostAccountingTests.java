/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.security.ObjectStoreRequestCounter;
import org.opensearch.serverless.storage.security.RequestCountingBlobContainer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * rfc-serverless-opensearch.md &sect;17's own still-open "GC sweep and PITR reconciliation's own
 * request-cost profiles remain ungated" gap, the GC half: proves a real sweep's DELETE-shaped
 * request cost stays flat (exactly 2: one batched bundle delete, one batched manifest delete)
 * regardless of how many manifests/bundles are actually deletable -- the same "regardless of N"
 * shape {@code CostAccountingRegressionTests#testCompactionPublishingStaysWithinItsExpectedPutBudget}
 * already established for compaction's PUT cost.
 */
public class GcSchedulerTaskCostAccountingTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;

    /** Writes a real one-file bundle and its manifest directly, independent of ObjectStoreCommitPublisher so the test controls createdAtMillis. */
    private static CommitManifest writeGeneration(
        BlobContainerBundleStore bundleStore,
        BlobContainerManifestStore manifestStore,
        long generation,
        long createdAtMillis
    ) throws Exception {
        String bundleName = BlobContainerBundleStore.NAME_PREFIX + INDEX_UUID + "-" + SHARD_ID + "-" + PRIMARY_TERM + "-" + generation;
        byte[] content = ("content-" + generation).getBytes("UTF-8");
        var bundle = bundleStore.writeBundle(bundleName, List.of(new BundleFileContent("segments_" + generation, content)));
        var entry = bundle.entries().get("segments_" + generation);
        CommitManifest manifest = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            PRIMARY_TERM,
            generation,
            "segments_" + generation,
            java.util.Map.of(
                "segments_" + generation,
                new org.opensearch.serverless.storage.manifest.FileReference(bundleName, entry.offset(), entry.length(), entry.checksum())
            ),
            0,
            0,
            null,
            0,
            org.opensearch.serverless.storage.manifest.PruningStats.empty(),
            createdAtMillis
        );
        manifestStore.writeManifest(manifest);
        return manifest;
    }

    public void testSweepDeletingManyGenerationsStaysWithinItsExpectedDeleteBudget() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer uncountedContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore setupBundleStore = new BlobContainerBundleStore(uncountedContainer);
        BlobContainerManifestStore setupManifestStore = new BlobContainerManifestStore(uncountedContainer);

        long now = System.currentTimeMillis();
        long farInThePast = now - TimeValue.timeValueDays(1).millis();
        // 10 superseded, unpinned, far-past-retention generations plus one current latest -- proves
        // the delete cost doesn't scale with how many are actually deletable.
        int deletableGenerationCount = 10;
        for (long generation = 1; generation <= deletableGenerationCount; generation++) {
            writeGeneration(setupBundleStore, setupManifestStore, generation, farInThePast);
        }
        writeGeneration(setupBundleStore, setupManifestStore, deletableGenerationCount + 1L, farInThePast);
        // The sweep anchors "latest" to the head register, so the setup publishes one -- and it does so on
        // the uncounted container, because what this test measures is the sweep's own request cost, not the
        // cost of standing the fixture up.
        TestShardHeads.publish(uncountedContainer, INDEX_UUID, SHARD_ID, PRIMARY_TERM, deletableGenerationCount + 1L);

        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        RequestCountingBlobContainer countingContainer = new RequestCountingBlobContainer(uncountedContainer, counter);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(countingContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(countingContainer);
        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(countingContainer);

        long retentionWindowMillis = TimeValue.timeValueMinutes(1).millis();
        GcSchedulerConfig config = new GcSchedulerConfig(
            TimeValue.timeValueMinutes(5),
            retentionWindowMillis,
            manifestStore,
            bundleStore,
            pinRegistry,
            new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(countingContainer),
            new BlobContainerGcSweepStateStore(countingContainer, INDEX_UUID, SHARD_ID)
        );
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            // Bundles now get their own sustained-observation safety window on top of the manifest
            // retention window (GcSchedulerTask's own javadoc explains why), so this test drives a
            // controllable clock across two ticks: the first deletes the manifests (one batched
            // request) and merely observes their now-orphaned bundles; the second, after the window
            // elapses, deletes the bundles (one more batched request). The combined two-tick cost is
            // still flat regardless of deletableGenerationCount -- that's what this test proves.
            long[] clockMillis = { now };
            GcSchedulerTask task = new GcSchedulerTask(threadPool, config.interval(), INDEX_UUID, SHARD_ID, config, () -> clockMillis[0]);
            try {
                task.sweepForTesting();
                clockMillis[0] = now + retentionWindowMillis + 1;
                task.sweepForTesting();
            } finally {
                task.close();
            }

            assertEquals(
                "sweeping " + deletableGenerationCount + " deletable generations must delete all of them, leaving only the current latest",
                1,
                setupManifestStore.listManifests().size()
            );
            assertEquals(
                "deleting "
                    + deletableGenerationCount
                    + " manifests and their bundles across the two ticks must cost exactly 2 DELETE-shaped "
                    + "requests total (one batched bundle delete, one batched manifest delete) -- got "
                    + counter.deleteCount()
                    + ", the sweep likely started issuing one delete per item instead of a batch",
                2L,
                counter.deleteCount()
            );
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }
}
