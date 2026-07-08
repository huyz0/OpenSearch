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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PitrRetentionSchedulerTaskTests extends OpenSearchTestCase {

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

    public void testConstructorRejectsNonPositiveWindow() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(newFsBlobContainer());
            DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(newFsBlobContainer());
            expectThrows(
                IllegalArgumentException.class,
                () -> new PitrRetentionSchedulerTask(
                    threadPool,
                    TimeValue.timeValueMillis(10),
                    INDEX_UUID,
                    SHARD_ID,
                    manifestStore,
                    pinRegistry,
                    0
                )
            );
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testScheduledTickActuallyReconcilesAndAddsAPin() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            BlobContainer manifestContainer = newFsBlobContainer();
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(manifestContainer);
            manifestStore.writeManifest(manifest(1, 0, System.currentTimeMillis()));

            DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(newFsBlobContainer());

            // A latch-observing wrapper so the test can wait for the real background tick to run,
            // rather than sleeping and hoping.
            CountDownLatch tickHappened = new CountDownLatch(1);
            DurablePinRegistry observingRegistry = new ForwardingDurablePinRegistry(pinRegistry) {
                @Override
                public void addPin(String indexUuid, int shardId, PinRecord pin) throws java.io.IOException {
                    super.addPin(indexUuid, shardId, pin);
                    tickHappened.countDown();
                }
            };

            PitrRetentionSchedulerTask task = new PitrRetentionSchedulerTask(
                threadPool,
                TimeValue.timeValueMillis(50),
                INDEX_UUID,
                SHARD_ID,
                manifestStore,
                observingRegistry,
                60_000L
            );
            try {
                assertTrue("a scheduled tick should have reconciled and added a pin", tickHappened.await(30, TimeUnit.SECONDS));
                assertEquals(Set.of(new PinRecord("pitr", 1, 0)), pinRegistry.getPins(INDEX_UUID, SHARD_ID));
            } finally {
                task.close();
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testCloseCancelsFurtherTicks() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(newFsBlobContainer());
            DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(newFsBlobContainer());

            java.util.concurrent.atomic.AtomicInteger tickCount = new java.util.concurrent.atomic.AtomicInteger();
            DurablePinRegistry countingRegistry = new ForwardingDurablePinRegistry(pinRegistry) {
                @Override
                public java.util.Set<PinRecord> getPins(String indexUuid, int shardId) throws java.io.IOException {
                    tickCount.incrementAndGet();
                    return super.getPins(indexUuid, shardId);
                }
            };

            PitrRetentionSchedulerTask task = new PitrRetentionSchedulerTask(
                threadPool,
                TimeValue.timeValueMillis(20),
                INDEX_UUID,
                SHARD_ID,
                manifestStore,
                countingRegistry,
                60_000L
            );
            Thread.sleep(100); // let a couple of ticks happen
            task.close();
            int countAtClose = tickCount.get();
            Thread.sleep(150); // long enough that more ticks would have fired if not canceled

            assertEquals("no further ticks should run after close()", countAtClose, tickCount.get());
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private BlobContainer newFsBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    /** Minimal forwarding base so tests can observe one method's calls without reimplementing the whole interface. */
    private static class ForwardingDurablePinRegistry implements DurablePinRegistry {
        private final DurablePinRegistry delegate;

        ForwardingDurablePinRegistry(DurablePinRegistry delegate) {
            this.delegate = delegate;
        }

        @Override
        public java.util.Set<PinRecord> getPins(String indexUuid, int shardId) throws java.io.IOException {
            return delegate.getPins(indexUuid, shardId);
        }

        @Override
        public void addPin(String indexUuid, int shardId, PinRecord pin) throws java.io.IOException {
            delegate.addPin(indexUuid, shardId, pin);
        }

        @Override
        public void removePin(String indexUuid, int shardId, String pinId) throws java.io.IOException {
            delegate.removePin(indexUuid, shardId, pinId);
        }

        @Override
        public void removePin(String indexUuid, int shardId, PinRecord pin) throws java.io.IOException {
            delegate.removePin(indexUuid, shardId, pin);
        }
    }
}
