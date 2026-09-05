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
import org.opensearch.serverless.storage.security.ObjectStoreRequestCounter;
import org.opensearch.serverless.storage.security.RequestCountingBlobContainer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The sweep's cross-tick memory, and what it costs when nothing has happened.
 *
 * <h2>Why the orphan clock has to be durable</h2>
 *
 * A bundle is deleted only once it has been observed unreferenced continuously for a full retention window.
 * Held in a field, that clock restarted every time the node hosting the sweep restarted, the shard
 * relocated, or the index was closed and reopened -- all routine under autoscaling and spot capacity, and
 * all far more frequent than a thirty-minute window. The threshold was therefore never reached and no bundle
 * was ever deleted, silently, leaking the shard's entire merge write amplification.
 */
public class GcSweepStatePersistenceTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private static CommitManifest writeGeneration(
        BlobContainerBundleStore bundleStore,
        BlobContainerManifestStore manifestStore,
        long generation,
        long createdAtMillis
    ) throws Exception {
        String bundleName = BlobContainerBundleStore.NAME_PREFIX + INDEX_UUID + "-" + SHARD_ID + "-" + PRIMARY_TERM + "-" + generation;
        String fileName = "segments_" + generation;
        byte[] content = ("content-" + generation).getBytes("UTF-8");
        var bundle = bundleStore.writeBundle(bundleName, List.of(new BundleFileContent(fileName, content)));
        var entry = bundle.entries().get(fileName);
        CommitManifest manifest = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            PRIMARY_TERM,
            generation,
            fileName,
            Map.of(fileName, new FileReference(bundleName, entry.offset(), entry.length(), entry.checksum())),
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

    /**
     * The restart case. One task observes the orphaned bundle and is then thrown away, exactly as a node
     * restart or a shard relocation throws one away; a brand-new task, with no in-memory history at all,
     * must still delete the bundle once the window has elapsed. Before the state was persisted this second
     * task would have started the clock over and the assertion below would fail.
     */
    public void testTheOrphanClockSurvivesLosingTheTaskThatStartedIt() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(container);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);

        long now = System.currentTimeMillis();
        long farInThePast = now - TimeValue.timeValueDays(1).millis();
        CommitManifest gen1 = writeGeneration(bundleStore, manifestStore, 1, farInThePast);
        writeGeneration(bundleStore, manifestStore, 2, farInThePast);
        TestShardHeads.publish(container, INDEX_UUID, SHARD_ID, PRIMARY_TERM, 2);
        String orphanedBundle = gen1.referencedBundles().iterator().next();

        long retentionWindowMillis = TimeValue.timeValueMinutes(30).millis();
        GcSchedulerConfig config = new GcSchedulerConfig(
            TimeValue.timeValueMinutes(5),
            retentionWindowMillis,
            manifestStore,
            bundleStore,
            pinRegistry,
            shardStateStore,
            new BlobContainerGcSweepStateStore(container, INDEX_UUID, SHARD_ID)
        );

        long[] clockMillis = { now };
        GcSchedulerTask firstIncarnation = new GcSchedulerTask(
            threadPool,
            config.interval(),
            INDEX_UUID,
            SHARD_ID,
            config,
            () -> clockMillis[0],
            0L
        );
        try {
            firstIncarnation.sweepForTesting();
            assertTrue(
                "the first sweep only observes the newly-orphaned bundle, it does not delete it",
                bundleStore.listBundleNames().contains(orphanedBundle)
            );
        } finally {
            firstIncarnation.close();
        }

        clockMillis[0] = now + retentionWindowMillis + 1;
        GcSchedulerTask afterRestart = new GcSchedulerTask(
            threadPool,
            config.interval(),
            INDEX_UUID,
            SHARD_ID,
            config,
            () -> clockMillis[0],
            0L
        );
        try {
            afterRestart.sweepForTesting();
            assertFalse(
                "a restart must not reset the orphan clock -- the bundle has been continuously unreferenced for a "
                    + "full window and the shard should not have to wait another one for a process that happens to "
                    + "have been running the whole time",
                bundleStore.listBundleNames().contains(orphanedBundle)
            );
        } finally {
            afterRestart.close();
        }
    }

    /**
     * The cost half. A sweep's two blob listings are priced in the object store's write tier and were paid on
     * every tick of every shard on every reader replica, whether or not anything had changed. When the head
     * has not moved and the pin set is identical, the answer cannot have changed either, so the tick stops at
     * the two register reads it had to make anyway.
     */
    public void testATickWithNothingChangedSkipsBothListings() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore setupBundleStore = new BlobContainerBundleStore(rawContainer);
        BlobContainerManifestStore setupManifestStore = new BlobContainerManifestStore(rawContainer);

        long now = System.currentTimeMillis();
        long farInThePast = now - TimeValue.timeValueDays(1).millis();
        writeGeneration(setupBundleStore, setupManifestStore, 1, farInThePast);
        writeGeneration(setupBundleStore, setupManifestStore, 2, farInThePast);
        TestShardHeads.publish(rawContainer, INDEX_UUID, SHARD_ID, PRIMARY_TERM, 2);

        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        BlobContainer countingContainer = new RequestCountingBlobContainer(rawContainer, counter);
        long retentionWindowMillis = TimeValue.timeValueMinutes(30).millis();
        GcSchedulerConfig config = new GcSchedulerConfig(
            TimeValue.timeValueMinutes(5),
            retentionWindowMillis,
            new BlobContainerManifestStore(countingContainer),
            new BlobContainerBundleStore(countingContainer),
            new BlobContainerDurablePinRegistry(countingContainer),
            new BlobContainerShardStateStore(countingContainer),
            new BlobContainerGcSweepStateStore(countingContainer, INDEX_UUID, SHARD_ID)
        );

        long[] clockMillis = { now };
        GcSchedulerTask task = new GcSchedulerTask(threadPool, config.interval(), INDEX_UUID, SHARD_ID, config, () -> clockMillis[0], 0L);
        try {
            task.sweepForTesting();
            long listsAfterFirstSweep = counter.listCount();
            assertTrue("the first sweep must really have listed -- otherwise this proves nothing", listsAfterFirstSweep > 0);

            // A moment later, with the head and the pins untouched.
            clockMillis[0] = now + 1000;
            task.sweepForTesting();
            assertEquals(
                "a tick that can prove nothing changed must not pay for a single listing",
                listsAfterFirstSweep,
                counter.listCount()
            );

            // Past the floor: even a completely idle shard is swept in full once per retention window, so a
            // bundle left behind by a publish that crashed before writing any manifest is still found.
            clockMillis[0] = now + retentionWindowMillis + 1;
            task.sweepForTesting();
            assertTrue("the once-per-window full sweep must still happen on an idle shard", counter.listCount() > listsAfterFirstSweep);
        } finally {
            task.close();
        }
    }
}
