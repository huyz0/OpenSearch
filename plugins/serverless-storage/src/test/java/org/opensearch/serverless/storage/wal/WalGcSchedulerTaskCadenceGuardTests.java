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
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
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
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Tests for the guards added when {@code serverless_storage.wal_gc.interval} stopped defaulting to
 * "disabled". WAL mirroring and group-commit batching are now both on by default, so chunks are
 * produced continuously (up to one per {@code wal_flush.interval}, 200&nbsp;ms, per node) and a
 * disabled sweep means they accumulate for the life of the cluster. Turning the sweep on makes three
 * things matter that did not before: its per-tick cost, whether a repeated tick can do redundant
 * work, and whether a permanently blocked sweep is visible.
 *
 * <p>The safety bound itself is unchanged and is covered by {@code WalGcSchedulerTaskTests}; nothing
 * here relaxes it. See {@code WalGcSchedulerTask}'s class javadoc for why the cadence cannot affect
 * which chunks are deletable.
 */
public class WalGcSchedulerTaskCadenceGuardTests extends OpenSearchTestCase {

    private static final long PRIMARY_TERM = 1;

    private Path basePath;
    private BlobContainer walBlobContainer;
    private WalShardRegistry registry;
    private WalChunkService walChunkService;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        basePath = createTempDir();
        walBlobContainer = container("wal-root");
        registry = new WalShardRegistry(walBlobContainer);
        walChunkService = new WalChunkService(walBlobContainer, "epoch-0");
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private BlobContainer container(String name) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, basePath.resolve(name), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    private long writeChunk(String indexUuid, long seqNo) throws Exception {
        walChunkService.append(new WalRecord(indexUuid, 0, PRIMARY_TERM, seqNo, ("op-" + seqNo).getBytes("UTF-8")));
        return walChunkService.flush();
    }

    /** Publishes a real manifest carrying {@code walOffset} as this shard's head, exactly as a live writer engine does. */
    private void publishHead(BlobContainer shardContainer, String indexUuid, int shardId, long generation, Long walOffset)
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
            walOffset == null ? null : new WalPosition("epoch-0", walOffset),
            0,
            PruningStats.empty(),
            System.currentTimeMillis()
        );
        manifestStore.writeManifest(manifest);
        Optional<VersionedShardHead> existing = shardStateStore.get(indexUuid, shardId);
        shardStateStore.compareAndSet(
            indexUuid,
            shardId,
            existing.map(VersionedShardHead::version),
            new ShardHead(PRIMARY_TERM, null, 0L, generation)
        );
    }

    private WalGcSchedulerTask newTask(BlobContainer walContainerForTask, BiFunction<String, Integer, BlobContainer> resolver) {
        return new WalGcSchedulerTask(threadPool, TimeValue.timeValueDays(1), walContainerForTask, registry, resolver);
    }

    /**
     * A repeated tick at an unchanged bound must not list the WAL container or issue a delete. The
     * listing is the one part of a sweep whose cost grows with how many chunks are retained, so on a
     * quiet cluster -- where the bound only moves when someone publishes -- a frequent cadence has to
     * cost essentially nothing beyond the per-shard head reads, or it cannot be a default.
     */
    public void testARepeatedSweepAtAnUnchangedBoundDoesNoListingOrDelete() throws Exception {
        long seq0 = writeChunk("idx", 0);
        long seq1 = writeChunk("idx", 1);

        BlobContainer shardContainer = container("idx-0");
        publishHead(shardContainer, "idx", 0, 1, seq0);
        registry.register("idx", 0);

        CountingWalContainer counting = new CountingWalContainer(walBlobContainer);
        WalGcSchedulerTask task = newTask(counting, (indexUuid, shardId) -> shardContainer);
        try {
            task.sweepForTesting();
            assertEquals("the first sweep must actually do the work", 1, counting.chunkListings.get());
            assertEquals(1, counting.deleteCalls.get());
            assertFalse(walBlobContainer.blobExists(WalChunkNaming.blobName("epoch-0", seq0)));
            assertTrue(walBlobContainer.blobExists(WalChunkNaming.blobName("epoch-0", seq1)));

            task.sweepForTesting();
            task.sweepForTesting();
            assertEquals(
                "a bound that has not advanced cannot have made anything new deletable, so the listing must be skipped",
                1,
                counting.chunkListings.get()
            );
            assertEquals(1, counting.deleteCalls.get());
        } finally {
            task.close();
        }
    }

    /** The memo is "nothing new", not "already ran once": once the bound advances, the next tick sweeps again. */
    public void testASweepResumesOnceTheCoveredBoundActuallyAdvances() throws Exception {
        long seq0 = writeChunk("idx", 0);
        long seq1 = writeChunk("idx", 1);
        long seq2 = writeChunk("idx", 2);

        BlobContainer shardContainer = container("idx-0");
        publishHead(shardContainer, "idx", 0, 1, seq0);
        registry.register("idx", 0);

        WalGcSchedulerTask task = newTask(walBlobContainer, (indexUuid, shardId) -> shardContainer);
        try {
            task.sweepForTesting();
            assertTrue(walBlobContainer.blobExists(WalChunkNaming.blobName("epoch-0", seq1)));

            publishHead(shardContainer, "idx", 0, 2, seq1);
            task.sweepForTesting();

            assertFalse(
                "the newly covered chunk must now be deleted",
                walBlobContainer.blobExists(WalChunkNaming.blobName("epoch-0", seq1))
            );
            assertTrue("and the still-uncovered one must survive", walBlobContainer.blobExists(WalChunkNaming.blobName("epoch-0", seq2)));
        } finally {
            task.close();
        }
    }

    /**
     * A shard that has not published since the last sweep must not have its manifest re-read. The
     * per-tick read cost is {@code O(registered shards)} and independent of cadence, so halving it is
     * what makes a frequent default affordable on a cluster-manager that is already busy.
     */
    public void testAnUnchangedManifestIsNotReReadOnEverySweep() throws Exception {
        long seq0 = writeChunk("idx", 0);
        writeChunk("idx", 1);

        CountingShardContainer shardContainer = new CountingShardContainer(container("idx-0"));
        publishHead(shardContainer, "idx", 0, 1, seq0);
        registry.register("idx", 0);
        shardContainer.manifestReads.set(0);

        WalGcSchedulerTask task = newTask(walBlobContainer, (indexUuid, shardId) -> shardContainer);
        try {
            task.sweepForTesting();
            assertEquals("the first sweep has nothing cached and must read the manifest", 1, shardContainer.manifestReads.get());

            task.sweepForTesting();
            task.sweepForTesting();
            assertEquals(
                "manifests are immutable and write-once, so an unchanged (term, generation) never needs re-reading",
                1,
                shardContainer.manifestReads.get()
            );

            // Publishing a new generation changes the cache key, so it is read exactly once more.
            publishHead(shardContainer, "idx", 0, 2, seq0);
            task.sweepForTesting();
            assertEquals(2, shardContainer.manifestReads.get());
        } finally {
            task.close();
        }
    }

    /**
     * A bail-out is the safe answer, but a bail-out that recurs forever means WAL GC has silently
     * stopped while chunks keep being produced -- a disk-exhaustion path once this sweep runs by
     * default. The counter that drives the escalation from INFO to WARN must actually count, and must
     * reset when the sweep recovers.
     */
    public void testAPersistentlyBlockedSweepIsCountedAndTheCountResetsOnRecovery() throws Exception {
        long seq0 = writeChunk("idx", 0);

        BlobContainer shardContainer = container("idx-0");
        // Registered, but has never published a manifest -- the shape a marker left behind by a
        // failed deregister presents as, and the one that blocks the whole container.
        registry.register("idx", 0);

        WalGcSchedulerTask task = newTask(walBlobContainer, (indexUuid, shardId) -> shardContainer);
        try {
            for (int i = 1; i <= WalGcSchedulerTask.STALL_WARN_AFTER_CONSECUTIVE_SKIPS; i++) {
                task.sweepForTesting();
                assertEquals(i, task.consecutiveIncompleteInformationSkipsForTesting());
            }
            assertTrue(
                "nothing may have been deleted while the sweep was blocked",
                walBlobContainer.blobExists(WalChunkNaming.blobName("epoch-0", seq0))
            );

            publishHead(shardContainer, "idx", 0, 1, seq0);
            task.sweepForTesting();
            assertEquals("a completed sweep must clear the stall counter", 0, task.consecutiveIncompleteInformationSkipsForTesting());
            assertFalse(walBlobContainer.blobExists(WalChunkNaming.blobName("epoch-0", seq0)));
        } finally {
            task.close();
        }
    }

    /**
     * A manifest with a {@code null} WAL position blocks the sweep too, and must be counted the same
     * way -- it is the other half of "incomplete information", and re-reading that manifest on every
     * tick to rediscover the same {@code null} would be the most expensive possible way to stay stuck.
     */
    public void testABlockingNullWalPositionIsCountedAndCachedRatherThanReReadEveryTick() throws Exception {
        writeChunk("idx", 0);

        CountingShardContainer shardContainer = new CountingShardContainer(container("idx-0"));
        publishHead(shardContainer, "idx", 0, 1, null);
        registry.register("idx", 0);
        shardContainer.manifestReads.set(0);

        WalGcSchedulerTask task = newTask(walBlobContainer, (indexUuid, shardId) -> shardContainer);
        try {
            task.sweepForTesting();
            task.sweepForTesting();
            task.sweepForTesting();
            assertEquals(3, task.consecutiveIncompleteInformationSkipsForTesting());
            assertEquals("a null WalPosition is a real, cacheable answer", 1, shardContainer.manifestReads.get());
        } finally {
            task.close();
        }
    }

    /**
     * The bound is advanced only after the delete call actually returned. Advancing optimistically
     * would leak every chunk in the failed range permanently, because the next sweep would skip it as
     * "already swept"; repeating the range instead costs one idempotent call.
     */
    public void testAFailedDeleteDoesNotAdvanceTheBoundSoTheNextSweepRetries() throws Exception {
        long seq0 = writeChunk("idx", 0);
        writeChunk("idx", 1);

        BlobContainer shardContainer = container("idx-0");
        publishHead(shardContainer, "idx", 0, 1, seq0);
        registry.register("idx", 0);

        CountingWalContainer flaky = new CountingWalContainer(walBlobContainer);
        flaky.failNextDelete.set(true);
        WalGcSchedulerTask task = newTask(flaky, (indexUuid, shardId) -> shardContainer);
        try {
            expectThrows(IOException.class, task::sweepForTesting);
            assertTrue(
                "the chunk must still be there after a failed delete",
                walBlobContainer.blobExists(WalChunkNaming.blobName("epoch-0", seq0))
            );

            task.sweepForTesting();
            assertFalse(
                "the next sweep must retry the same range rather than skip it as already swept",
                walBlobContainer.blobExists(WalChunkNaming.blobName("epoch-0", seq0))
            );
        } finally {
            task.close();
        }
    }

    /** Counts chunk listings and deletes on the WAL container, and can fail one delete on demand. Registers pass through. */
    private static final class CountingWalContainer extends FilterBlobContainer {
        private final BlobContainer raw;
        final AtomicInteger chunkListings = new AtomicInteger();
        final AtomicInteger deleteCalls = new AtomicInteger();
        final AtomicBoolean failNextDelete = new AtomicBoolean(false);

        CountingWalContainer(BlobContainer raw) {
            super(raw);
            this.raw = raw;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new CountingWalContainer(child);
        }

        @Override
        public Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobsByPrefix(String prefix) throws IOException {
            if (WalChunkNaming.LOG_BLOB_PREFIX.equals(prefix)) {
                chunkListings.incrementAndGet();
            }
            return raw.listBlobsByPrefix(prefix);
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            deleteCalls.incrementAndGet();
            if (failNextDelete.compareAndSet(true, false)) {
                throw new IOException("injected delete failure");
            }
            raw.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            return raw.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return raw.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }

    /** Counts manifest body reads on a shard's own container. */
    private static final class CountingShardContainer extends FilterBlobContainer {
        private final BlobContainer raw;
        final AtomicInteger manifestReads = new AtomicInteger();

        CountingShardContainer(BlobContainer raw) {
            super(raw);
            this.raw = raw;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new CountingShardContainer(child);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            if (blobName.startsWith(CommitManifest.NAME_PREFIX)) {
                manifestReads.incrementAndGet();
            }
            return raw.readBlob(blobName);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            return raw.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return raw.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }
}
