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

public class ReaderShardAdmissionControllerTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("idx", "idx-uuid"), 0);

    /** A {@link CachedIndexInput} whose only job is to report a fixed {@link #length()} so tests can push a {@link FileCache}'s usage() to a known value without any real file I/O. */
    private static final class FakeCachedIndexInput implements CachedIndexInput {
        private final long length;

        FakeCachedIndexInput(long length) {
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

    public void testConstructorRejectsNonPositiveLimits() {
        expectThrows(IllegalArgumentException.class, () -> new ReaderShardAdmissionController(0));
        expectThrows(IllegalArgumentException.class, () -> new ReaderShardAdmissionController(-1));
    }

    public void testConstructorRejectsInvalidFileCacheUsageRatios() {
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1024L, 1);
        expectThrows(IllegalArgumentException.class, () -> new ReaderShardAdmissionController(1, fileCache, 0.0));
        expectThrows(IllegalArgumentException.class, () -> new ReaderShardAdmissionController(1, fileCache, 1.1));
    }

    public void testAcquireSucceedsWhenFileCacheUsageIsUnderBudget() {
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1024L, 1);
        fileCache.put(Path.of("under-budget"), new FakeCachedIndexInput(100L));
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(10, fileCache, 0.5);
        // Must not throw -- 100 bytes used is well under 50% of 1024 bytes capacity.
        controller.acquire(SHARD_ID);
        assertEquals(9, controller.availablePermits());
    }

    public void testAcquireThrowsWhenFileCacheUsageIsOverBudgetEvenWithCountHeadroom() {
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1024L, 1);
        fileCache.put(Path.of("over-budget"), new FakeCachedIndexInput(600L));
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(10, fileCache, 0.5);
        // The count cap (10) has plenty of headroom, but 600/1024 bytes already exceeds the 50% budget.
        expectThrows(IllegalStateException.class, () -> controller.acquire(SHARD_ID));
        // The rejected acquire must not have consumed a count permit.
        assertEquals(10, controller.availablePermits());
    }

    public void testAcquireSucceedsUpToTheLimit() {
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(2);
        controller.acquire(SHARD_ID);
        controller.acquire(SHARD_ID);
        assertEquals(0, controller.availablePermits());
    }

    public void testAcquireBeyondTheLimitThrows() {
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(1);
        controller.acquire(SHARD_ID);
        expectThrows(IllegalStateException.class, () -> controller.acquire(SHARD_ID));
    }

    public void testReleaseFreesAPermitForTheNextAcquire() {
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(1);
        controller.acquire(SHARD_ID);
        controller.release();
        // Must not throw -- the released permit is available again.
        controller.acquire(SHARD_ID);
        assertEquals(0, controller.availablePermits());
    }
}
