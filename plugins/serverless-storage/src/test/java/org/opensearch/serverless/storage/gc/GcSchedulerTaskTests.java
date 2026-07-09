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
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class GcSchedulerTaskTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;

    private BlobContainer blobContainer;
    private BlobContainerBundleStore bundleStore;
    private BlobContainerManifestStore manifestStore;
    private DurablePinRegistry pinRegistry;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        bundleStore = new BlobContainerBundleStore(blobContainer);
        manifestStore = new BlobContainerManifestStore(blobContainer);
        pinRegistry = new BlobContainerDurablePinRegistry(blobContainer);
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    /** Writes a real one-file bundle and its manifest, independent of ObjectStoreCommitPublisher so the test controls createdAtMillis directly. */
    private CommitManifest writeGeneration(long generation, long createdAtMillis) throws Exception {
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

    public void testSweepDeletesOnlySupersededUnpinnedManifestsPastTheRetentionWindowAndTheirOrphanedBundles() throws Exception {
        long now = System.currentTimeMillis();
        long farInThePast = now - TimeValue.timeValueDays(1).millis();

        // gen 1: superseded, unpinned, far past retention -- must be deleted, bundle and all.
        CommitManifest gen1 = writeGeneration(1, farInThePast);
        // gen 2: superseded, unpinned, far past retention, but durably pinned -- must survive.
        CommitManifest gen2 = writeGeneration(2, farInThePast);
        pinRegistry.addPin(INDEX_UUID, SHARD_ID, new PinRecord("snapshot-1", PRIMARY_TERM, gen2.generation()));
        // gen 3: the current latest -- must always survive regardless of age (nothing newer exists).
        CommitManifest gen3 = writeGeneration(3, farInThePast);

        GcSchedulerConfig config = new GcSchedulerConfig(
            TimeValue.timeValueMinutes(5),
            TimeValue.timeValueMinutes(1).millis(),
            manifestStore,
            bundleStore,
            pinRegistry
        );
        GcSchedulerTask task = new GcSchedulerTask(threadPool, config.interval(), INDEX_UUID, SHARD_ID, config);
        try {
            task.sweepForTesting();

            List<CommitManifest> remaining = manifestStore.listManifests();
            assertEquals(2, remaining.size());
            assertTrue(remaining.stream().anyMatch(m -> m.generation() == gen2.generation()));
            assertTrue(remaining.stream().anyMatch(m -> m.generation() == gen3.generation()));
            assertFalse(remaining.stream().anyMatch(m -> m.generation() == gen1.generation()));

            Set<String> remainingBundles = bundleStore.listBundleNames();
            assertFalse(
                "gen 1's now-unreferenced bundle must have been deleted",
                remainingBundles.contains(gen1.referencedBundles().iterator().next())
            );
            assertTrue(
                "gen 2's bundle must survive -- it's still referenced by a durably pinned manifest",
                remainingBundles.contains(gen2.referencedBundles().iterator().next())
            );
            assertTrue(
                "gen 3's bundle must survive -- it's referenced by the current latest manifest",
                remainingBundles.contains(gen3.referencedBundles().iterator().next())
            );
        } finally {
            task.close();
        }
    }

    public void testSweepRetainsEverythingWithinTheRetentionWindowEvenIfSupersededAndUnpinned() throws Exception {
        long now = System.currentTimeMillis();

        // Both created "just now" -- well inside a generous retention window, even though gen 1 is
        // already superseded by gen 2. This is the actual safety net this scheduler relies on (see
        // its own javadoc): a reader that only just barely lags behind must never have its
        // currently-open generation deleted out from under it.
        CommitManifest gen1 = writeGeneration(1, now);
        writeGeneration(2, now);

        GcSchedulerConfig config = new GcSchedulerConfig(
            TimeValue.timeValueMinutes(5),
            TimeValue.timeValueMinutes(30).millis(),
            manifestStore,
            bundleStore,
            pinRegistry
        );
        GcSchedulerTask task = new GcSchedulerTask(threadPool, config.interval(), INDEX_UUID, SHARD_ID, config);
        try {
            task.sweepForTesting();

            List<CommitManifest> remaining = manifestStore.listManifests();
            assertEquals("nothing should have been deleted -- everything is within the retention window", 2, remaining.size());
            assertTrue(remaining.stream().anyMatch(m -> m.generation() == gen1.generation()));
        } finally {
            task.close();
        }
    }
}
