/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
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

    /** Short enough that several ticks land inside the test, named so the waits can be derived from it. */
    private static final TimeValue TICK_INTERVAL = TimeValue.timeValueMillis(20);

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

    /**
     * {@code close()} must stop the schedule, and the straggler must not be mistaken for a failure to do so.
     *
     * <p>This deliberately forces the race that used to make it flaky, rather than hoping to hit it.
     * The stall below sits in {@code listBlobsByPrefix}, the first thing a tick does, so a tick
     * dispatched just before {@code cancel()} took effect is guaranteed to still be well short of the
     * {@code getPins} call that counts it. Reading the baseline the instant {@code close()} returned
     * then missed that tick and saw it arrive afterwards, which looked exactly like a tick
     * {@code close()} had failed to prevent.
     *
     * <p>Worth stating because the flake was originally seen once under a loaded parallel run and
     * could not be reproduced by repetition on an idle machine, not even at 60 iterations. Forcing it
     * this way makes the old shape fail every time and the current one pass, so the fix is verified
     * rather than merely argued.
     */
    public void testCloseCancelsFurtherTicks() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            // Long enough that the straggler lands after the baseline is taken, short enough that it
            // lands before the final assertion: a stall that outlasts the whole test would let the
            // test pass for the wrong reason, having never observed the tick at all.
            TimeValue stall = TimeValue.timeValueMillis(50);
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(new StallingBlobContainer(newFsBlobContainer(), stall));
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
                TICK_INTERVAL,
                INDEX_UUID,
                SHARD_ID,
                manifestStore,
                countingRegistry,
                60_000L
            );
            Thread.sleep(TICK_INTERVAL.millis() * 5); // let a couple of ticks happen
            task.close();

            // Baseline taken once the straggler has landed, not the instant close() returns.
            // Reading immediately after close() is what made this test flaky, seen as
            // "expected:<3> but was:<4>", and the production behaviour it accused was never wrong.
            int countOnceQuiet = awaitStragglingTick(tickCount, TICK_INTERVAL);

            Thread.sleep(TICK_INTERVAL.millis() * 5); // more ticks would have fired by now if not canceled

            assertEquals("no further ticks should run after close()", countOnceQuiet, tickCount.get());
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Waits for any straggling tick to land, then returns the settled count.
     *
     * <p>The tick being waited on is one the scheduler dispatched just before {@code cancel()} took
     * effect. It has been handed to the GENERIC pool but may still be inside {@code listManifests},
     * some way short of the {@code getPins} call that does the counting. So "the count did not move
     * between two quick reads" is evidence of nothing: the straggler may simply not have arrived
     * yet, which is the same race one level down. An earlier attempt at this fix polled with a short
     * backoff and had exactly that hole.
     *
     * <p>What makes this terminate rather than merely narrow the window is that {@code cancel()} has
     * already run, so no further ticks can be dispatched and there is <em>at most one</em>
     * straggler. Requiring the count to hold still across a dwell longer than a full interval means
     * either it has already landed or it never will, so this can loop at most twice in practice.
     */
    private static int awaitStragglingTick(java.util.concurrent.atomic.AtomicInteger tickCount, TimeValue interval)
        throws InterruptedException {
        long dwellMillis = Math.max(200, interval.millis() * 5);
        for (int attempt = 0; attempt < 5; attempt++) {
            int before = tickCount.get();
            Thread.sleep(dwellMillis);
            if (tickCount.get() == before) {
                return before;
            }
        }
        throw new AssertionError("ticks never stopped after close(), so cancel() did not take effect");
    }

    /**
     * Delays the first call a reconciliation tick makes, so a tick dispatched just before
     * {@code cancel()} is reliably still in flight rather than occasionally so.
     */
    private static final class StallingBlobContainer extends FilterBlobContainer {

        private final TimeValue stall;

        StallingBlobContainer(BlobContainer delegate, TimeValue stall) {
            super(delegate);
            this.stall = stall;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return child;
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String prefix) throws java.io.IOException {
            try {
                Thread.sleep(stall.millis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return super.listBlobsByPrefix(prefix);
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

        @Override
        public void replacePin(String indexUuid, int shardId, PinRecord newPin) throws java.io.IOException {
            delegate.replacePin(indexUuid, shardId, newPin);
        }
    }
}
