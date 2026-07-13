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
import org.opensearch.serverless.storage.e2e.ProbabilisticFailingBlobContainer;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * rfc-serverless-opensearch.md &sect;17's "broader probabilistic multi-operation throttling/5xx-storm
 * chaos injection" gap, the GC-sweep half: closes {@code
 * org.opensearch.serverless.storage.e2e.ChaosMultiOperationRegressionTests}' own explicitly-deferred
 * "GC-sweep-under-chaos" scope note. Unlike compaction (whose retries are only safe up to a real,
 * documented content-collision limit -- see {@code BlobContainerBundleStore#writeBundle}'s own
 * javadoc), a GC sweep's own retry story is simpler and unconditionally safe: {@code
 * GcSchedulerTask#sweep} issues nothing but idempotent {@code deleteBlobsIgnoringIfNotExists} calls
 * (bundles, then manifests, in that deliberate order -- see that class's own javadoc for why a
 * mid-sweep crash is merely retry-safe, never orphaning), so a sustained, randomized-fault sweep is
 * expected to always eventually converge, not merely "converge or safely abort" the way compaction
 * does.
 */
public class GcSchedulerTaskChaosTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "chaos-gc-idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;
    private static final int MAX_ATTEMPTS = 50;

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
            Map.of("segments_" + generation, new FileReference(bundleName, entry.offset(), entry.length(), entry.checksum())),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            createdAtMillis
        );
        manifestStore.writeManifest(manifest);
        return manifest;
    }

    public void testSweepConvergesDespiteSustainedRandomizedFaults() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer uncountedContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore setupBundleStore = new BlobContainerBundleStore(uncountedContainer);
        BlobContainerManifestStore setupManifestStore = new BlobContainerManifestStore(uncountedContainer);

        long now = System.currentTimeMillis();
        long farInThePast = now - TimeValue.timeValueDays(1).millis();
        int deletableGenerationCount = 10;
        for (long generation = 1; generation <= deletableGenerationCount; generation++) {
            writeGeneration(setupBundleStore, setupManifestStore, generation, farInThePast);
        }
        CommitManifest latest = writeGeneration(setupBundleStore, setupManifestStore, deletableGenerationCount + 1L, farInThePast);

        BlobContainer faultyContainer = new ProbabilisticFailingBlobContainer(uncountedContainer, random(), 0.3);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(faultyContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(faultyContainer);
        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(faultyContainer);

        GcSchedulerConfig config = new GcSchedulerConfig(
            TimeValue.timeValueMinutes(5),
            TimeValue.timeValueMinutes(1).millis(),
            manifestStore,
            bundleStore,
            pinRegistry
        );
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            GcSchedulerTask task = new GcSchedulerTask(threadPool, config.interval(), INDEX_UUID, SHARD_ID, config);
            try {
                // A single sweepForTesting() call may itself only partially complete under chaos
                // (e.g. the bundle delete succeeds but the manifest delete faults) -- retrying the
                // whole sweep is always safe (see this class's own javadoc), so keep sweeping until
                // one full pass completes with no fault at all, then convergence is guaranteed.
                IOException lastFailure = null;
                boolean converged = false;
                for (int attempt = 0; attempt < MAX_ATTEMPTS && converged == false; attempt++) {
                    try {
                        task.sweepForTesting();
                        converged = true;
                    } catch (IOException e) {
                        lastFailure = e;
                    }
                }
                assertTrue("sweep must eventually complete a full pass once retried past any injected faults: " + lastFailure, converged);
            } finally {
                task.close();
            }

            // Convergence check 1: every deletable generation is really gone, only the latest survives.
            List<CommitManifest> remaining = setupManifestStore.listManifests();
            assertEquals(
                "sweeping " + deletableGenerationCount + " deletable generations must delete all of them, leaving only the current latest",
                1,
                remaining.size()
            );
            assertEquals(latest.generation(), remaining.get(0).generation());

            // Convergence check 2: the surviving manifest's bundle is still fully present and
            // checksum-clean -- no injected fault corrupted or partially deleted it.
            for (var entry : remaining.get(0).files().entrySet()) {
                FileReference ref = entry.getValue();
                byte[] bytes = setupBundleStore.readFile(
                    ref.bundleName(),
                    new BundleFileEntry(entry.getKey(), ref.offset(), ref.length(), ref.checksum())
                );
                assertNotNull("the surviving manifest's own bundle file must read back cleanly", bytes);
            }

            // Convergence check 3: no orphaned bundle from a deleted generation was left behind either.
            Set<String> remainingBundles = setupBundleStore.listBundleNames();
            assertEquals(1, remainingBundles.size());
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }
}
