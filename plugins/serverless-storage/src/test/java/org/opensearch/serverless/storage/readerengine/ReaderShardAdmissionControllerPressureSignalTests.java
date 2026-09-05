/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.apache.lucene.store.IndexInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

/**
 * The budget was measured against {@code FileCache#usage()}, which counts every entry including
 * every freely evictable one. An LRU cache that is doing its job sits at essentially 100% of
 * capacity in the steady state, so once a node's cache first filled, two things became permanently
 * true: no further reader shard could ever be opened on that node, and no reader already on it
 * could ever advance a generation again. Both tests here fail against {@code usage()}.
 */
public class ReaderShardAdmissionControllerPressureSignalTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("idx", "idx-uuid"), 0);

    /** Reports a fixed length so a {@link FileCache}'s accounting can be driven without real file I/O. */
    private static final class FixedLengthCachedInput implements CachedIndexInput {
        private final long length;

        FixedLengthCachedInput(long length) {
            this.length = length;
        }

        @Override
        public IndexInput getIndexInput() {
            throw new UnsupportedOperationException("not needed for admission-control tests");
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {}
    }

    /**
     * A cache that is full but whose entries are all unreferenced: every byte in it is available to
     * whoever asks next. That is the steady state of a warm LRU, not a pressure condition, and it
     * must not stop a reader advancing.
     */
    /**
     * A real file under this test's own temp directory.
     *
     * <p>{@code FileCache} keys are real paths and {@code prune()} really deletes them, so a
     * relative {@code Path.of("...")} key resolves against the test JVM's working directory --
     * somewhere under {@code build/} -- and the delete is refused by the security manager. Keys have
     * to live where this test is allowed to write.
     */
    private Path cacheKeyPath(String name) throws Exception {
        Path path = createTempDir().resolve(name);
        java.nio.file.Files.write(path, new byte[8]);
        return path;
    }

    private FileCache fullButFullyEvictableCache() throws Exception {
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1024L, 1);
        Path key = cacheKeyPath("warm-entry");
        fileCache.put(key, new FixedLengthCachedInput(1000L));
        // put() takes a reference; releasing it is what makes the entry evictable, which is exactly
        // what happens to a real block once the IndexInput reading it is closed.
        fileCache.decRef(key);
        assertTrue("test setup: the cache must be full by total usage", fileCache.usage() >= 900L);
        assertEquals("test setup: and hold nothing that cannot be evicted", 0L, fileCache.activeUsage());
        return fileCache;
    }

    public void testAWarmButFullyEvictableCacheDoesNotDeferRefreshes() throws Exception {
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(10, fullButFullyEvictableCache(), 0.5);
        assertFalse(
            "a full-but-evictable cache is a warm cache, not an over-budget one -- treating it as pressure froze every"
                + " reader on the node at whatever generation it happened to be on, forever",
            controller.isOverBudgetForRefresh()
        );
    }

    public void testAWarmButFullyEvictableCacheDoesNotRefuseAShardOpen() throws Exception {
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(10, fullButFullyEvictableCache(), 0.5);
        // Must not throw: a node whose cache is merely warm has to keep accepting reader shards, or
        // every subsequent allocation to it fails, is retried, and fails again indefinitely.
        controller.acquire(SHARD_ID);
        assertEquals(9, controller.availablePermits());
    }

    /** Genuinely unevictable bytes are still pressure, and must still defer. */
    public void testGenuinelyReferencedBytesAreStillOverBudget() throws Exception {
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1024L, 1);
        fileCache.put(cacheKeyPath("pinned-entry"), new FixedLengthCachedInput(900L));
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(10, fileCache, 0.5);
        assertTrue(controller.isOverBudgetForRefresh());
        expectThrows(IllegalStateException.class, () -> controller.acquire(SHARD_ID));
    }

    /**
     * The prune-and-recheck the refresh path uses before concluding "no headroom": reading the
     * budget and reclaiming space are otherwise decoupled, so a node can be over its active-usage
     * budget purely because nothing recently asked the cache to let go of anything.
     */
    public void testPruneAndRecheckClearsAnEvictableOverage() throws Exception {
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1024L, 1);
        Path key = cacheKeyPath("evictable");
        fileCache.put(key, new FixedLengthCachedInput(900L));
        fileCache.decRef(key);
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(10, fileCache, 0.5);
        assertFalse(controller.pruneAndRecheckOverBudgetForRefresh());
        assertEquals("pruning must actually have reclaimed the evictable entry", 0L, fileCache.usage());
    }

    /**
     * The quantity the design actually names as expensive per open reader is heap held by open
     * segment readers, and nothing in the plugin bounded it: this controller counted shards and
     * counted the block cache's disk bytes, neither of which is heap.
     */
    public void testAnOpenIsRefusedOnceEstimatedReaderHeapCrossesItsCeiling() {
        long[] heapBytes = { 0L };
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(10, null, 1.0, () -> heapBytes[0], 1_000L);
        controller.acquire(SHARD_ID);

        heapBytes[0] = 1_500L;
        IllegalStateException overHeap = expectThrows(IllegalStateException.class, () -> controller.acquire(SHARD_ID));
        assertTrue(overHeap.getMessage(), overHeap.getMessage().contains("estimated reader-shard heap"));
        assertEquals("a rejected acquire must not consume a count permit", 9, controller.availablePermits());
    }

    /** With no supplier or no ceiling, behaviour must be exactly what it was before the heap term existed. */
    public void testTheHeapTermIsInertWhenUnconfigured() {
        ReaderShardAdmissionController noSupplier = new ReaderShardAdmissionController(1, null, 1.0, null, 1L);
        noSupplier.acquire(SHARD_ID);
        ReaderShardAdmissionController noCeiling = new ReaderShardAdmissionController(1, null, 1.0, () -> Long.MAX_VALUE, 0L);
        noCeiling.acquire(SHARD_ID);
    }
}
