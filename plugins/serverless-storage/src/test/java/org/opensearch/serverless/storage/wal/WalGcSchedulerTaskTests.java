/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

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
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class WalGcSchedulerTaskTests extends OpenSearchTestCase {

    private static final long PRIMARY_TERM = 1;

    private BlobContainer walBlobContainer;
    private WalShardRegistry registry;
    private WalChunkService walChunkService;
    private ThreadPool threadPool;
    private java.nio.file.Path basePath;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        basePath = createTempDir();
        walBlobContainer = shardScopedBlobContainer("wal-root");
        registry = new WalShardRegistry(walBlobContainer);
        walChunkService = new WalChunkService(walBlobContainer, "epoch-0");
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private BlobContainer shardScopedBlobContainer(String name) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, basePath.resolve(name), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    /** Publishes a real manifest, with a real WalPosition, as the given shard's head -- the exact shape a live writer engine produces. */
    private void publishHead(BlobContainer shardContainer, String indexUuid, int shardId, long generation, long walOffset)
        throws Exception {
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(shardContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(shardContainer);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(shardContainer);

        String bundleName = "bundle-" + indexUuid + "-" + shardId + "-" + PRIMARY_TERM + "-" + generation;
        byte[] content = ("content-" + generation).getBytes("UTF-8");
        var bundle = bundleStore.writeBundle(bundleName, List.of(new BundleFileContent("segments_" + generation, content)));
        var entry = bundle.entries().get("segments_" + generation);
        CommitManifest manifest = new CommitManifest(
            indexUuid,
            shardId,
            PRIMARY_TERM,
            generation,
            "segments_" + generation,
            Map.of("segments_" + generation, new FileReference(bundleName, entry.offset(), entry.length(), entry.checksum())),
            0,
            0,
            new WalPosition("epoch-0", walOffset),
            0,
            PruningStats.empty(),
            System.currentTimeMillis()
        );
        manifestStore.writeManifest(manifest);
        Optional<org.opensearch.serverless.storage.shardstate.VersionedShardHead> existing = shardStateStore.get(indexUuid, shardId);
        shardStateStore.compareAndSet(
            indexUuid,
            shardId,
            existing.map(org.opensearch.serverless.storage.shardstate.VersionedShardHead::version),
            new ShardHead(PRIMARY_TERM, null, 0L, generation)
        );
    }

    private WalGcSchedulerTask newTask(java.util.function.BiFunction<String, Integer, BlobContainer> shardContainerResolver) {
        return new WalGcSchedulerTask(threadPool, TimeValue.timeValueDays(1), walBlobContainer, registry, shardContainerResolver);
    }

    public void testSweepDoesNothingWhenNoShardIsRegistered() throws Exception {
        walChunkService.append(new WalRecord("idx", 0, PRIMARY_TERM, 0, "a".getBytes("UTF-8")));
        walChunkService.flush();

        WalGcSchedulerTask task = newTask((indexUuid, shardId) -> {
            throw new AssertionError("resolver must not be invoked when no shard is registered");
        });
        try {
            task.sweepForTesting();
            assertEquals(1, walBlobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).size());
        } finally {
            task.close();
        }
    }

    public void testSweepDeletesChunksCoveredByEveryRegisteredShardsLatestManifest() throws Exception {
        // Three chunks written; shard's manifest covers up through chunk 1 (sequence 1).
        walChunkService.append(new WalRecord("idx", 0, PRIMARY_TERM, 0, "a".getBytes("UTF-8")));
        long seq0 = walChunkService.flush();
        walChunkService.append(new WalRecord("idx", 0, PRIMARY_TERM, 1, "b".getBytes("UTF-8")));
        long seq1 = walChunkService.flush();
        walChunkService.append(new WalRecord("idx", 0, PRIMARY_TERM, 2, "c".getBytes("UTF-8")));
        long seq2 = walChunkService.flush();

        BlobContainer shardContainer = shardScopedBlobContainer("idx-0");
        publishHead(shardContainer, "idx", 0, 1, seq1);
        registry.register("idx", 0);

        WalGcSchedulerTask task = newTask((indexUuid, shardId) -> shardContainer);
        try {
            task.sweepForTesting();
            var remaining = walBlobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).keySet();
            assertFalse(remaining.contains(WalChunkNaming.blobName("epoch-0", seq0)));
            assertFalse(remaining.contains(WalChunkNaming.blobName("epoch-0", seq1)));
            assertTrue(remaining.contains(WalChunkNaming.blobName("epoch-0", seq2)));
        } finally {
            task.close();
        }
    }

    public void testSweepUsesTheMinimumCoveredPositionAcrossAllRegisteredShards() throws Exception {
        walChunkService.append(new WalRecord("idx-a", 0, PRIMARY_TERM, 0, "a".getBytes("UTF-8")));
        long seq0 = walChunkService.flush();
        walChunkService.append(new WalRecord("idx-b", 0, PRIMARY_TERM, 0, "b".getBytes("UTF-8")));
        long seq1 = walChunkService.flush();

        BlobContainer containerA = shardScopedBlobContainer("idx-a-0");
        BlobContainer containerB = shardScopedBlobContainer("idx-b-0");
        // Shard A is fully caught up (covers seq1); shard B has only published up through seq0 --
        // the sweep must respect B's slower coverage, not A's.
        publishHead(containerA, "idx-a", 0, 1, seq1);
        publishHead(containerB, "idx-b", 0, 1, seq0);
        registry.register("idx-a", 0);
        registry.register("idx-b", 0);

        WalGcSchedulerTask task = newTask((indexUuid, shardId) -> "idx-a".equals(indexUuid) ? containerA : containerB);
        try {
            task.sweepForTesting();
            var remaining = walBlobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).keySet();
            assertFalse(remaining.contains(WalChunkNaming.blobName("epoch-0", seq0)));
            assertTrue(
                "seq1 is only covered by shard A, not shard B -- must survive since B hasn't caught up",
                remaining.contains(WalChunkNaming.blobName("epoch-0", seq1))
            );
        } finally {
            task.close();
        }
    }

    public void testSweepIsConservativeWhenARegisteredShardHasNeverPublished() throws Exception {
        walChunkService.append(new WalRecord("idx", 0, PRIMARY_TERM, 0, "a".getBytes("UTF-8")));
        walChunkService.flush();

        registry.register("idx", 0); // registered, but never actually published a manifest
        BlobContainer shardContainer = shardScopedBlobContainer("idx-0");

        WalGcSchedulerTask task = newTask((indexUuid, shardId) -> shardContainer);
        try {
            task.sweepForTesting();
            assertEquals(
                "a registered-but-never-published shard must block the whole sweep, not just be skipped",
                1,
                walBlobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).size()
            );
        } finally {
            task.close();
        }
    }
}
