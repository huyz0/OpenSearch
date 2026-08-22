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
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.gc.BlobGcCandidateLog.LoggedCandidate;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * The append/list/resolve mechanics {@link GcCandidateTailer} relies on, exercised directly against the
 * log rather than through the tailer -- so a failure here points at the log, not at the reconciliation
 * logic layered on top of it.
 */
public class BlobGcCandidateLogTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";

    private Path root;
    private AtomicLong clockMillis;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        root = createTempDir();
        clockMillis = new AtomicLong(0);
    }

    private BlobGcCandidateLog log() {
        Function<BlobPath, BlobContainer> containers = path -> {
            try {
                FsBlobStore blobStore = new FsBlobStore(1024, root, false);
                return blobStore.blobContainer(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return new BlobGcCandidateLog(containers, BlobPath.cleanPath(), clockMillis::get);
    }

    public void testAnAppendedCandidateIsFoundByEntriesSince() {
        BlobGcCandidateLog log = log();
        GcCandidate candidate = new GcCandidate(INDEX_UUID, 0, 1, 5);
        log.append(candidate);

        List<LoggedCandidate> pending = log.entriesSince(null);
        assertEquals("the appended candidate must be found by a listing with no floor", 1, pending.size());
        assertEquals(candidate, pending.get(0).candidate());
    }

    public void testResolvingRemovesItFromFutureListings() {
        BlobGcCandidateLog log = log();
        GcCandidate candidate = new GcCandidate(INDEX_UUID, 0, 1, 5);
        log.append(candidate);

        LoggedCandidate logged = log.entriesSince(null).get(0);
        log.resolve(logged);

        assertTrue(
            "a resolved candidate's mere absence is what retirement means here -- there is no cursor to advance",
            log.entriesSince(null).isEmpty()
        );
    }

    public void testResolvingAnAlreadyResolvedEntryIsANoOp() {
        BlobGcCandidateLog log = log();
        GcCandidate candidate = new GcCandidate(INDEX_UUID, 0, 1, 5);
        log.append(candidate);
        LoggedCandidate logged = log.entriesSince(null).get(0);

        log.resolve(logged);
        log.resolve(logged);

        assertTrue(log.entriesSince(null).isEmpty());
    }

    /**
     * The property that makes the queue safe under a caller-level retry: two appends of the identical fact
     * must not become two entries a tailer has to resolve separately.
     */
    public void testAppendingTheSameCandidateTwiceOverwritesRatherThanDuplicates() {
        BlobGcCandidateLog log = log();
        GcCandidate candidate = new GcCandidate(INDEX_UUID, 0, 1, 5);

        log.append(candidate);
        log.append(candidate);

        assertEquals("a retried append of the same supersession fact must land on one entry, not two", 1, log.entriesSince(null).size());
    }

    /**
     * {@code entriesSince} is what bounds a tailer pass' cost to the lookback rather than to fleet size --
     * this is the one thing that has to be true for that bound to hold.
     */
    public void testEntriesSinceSkipsBucketsBeforeTheFloor() {
        BlobGcCandidateLog log = log();

        clockMillis.set(0);
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 1)); // an old bucket

        clockMillis.set(BlobGcCandidateLog.BUCKET_MILLIS * 100);
        String floor = log.bucketAtOrBefore(0);
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 2)); // the current bucket

        List<LoggedCandidate> pending = log.entriesSince(floor);
        assertEquals("only the entry at or after the floor bucket may be returned", 1, pending.size());
        assertEquals(2, pending.get(0).candidate().generation());
    }

    public void testBucketAtOrBeforeLooksBackByTheRequestedAmount() {
        BlobGcCandidateLog log = log();
        clockMillis.set(BlobGcCandidateLog.BUCKET_MILLIS * 100);

        assertEquals(
            "a zero lookback must resolve to the current bucket",
            BlobGcCandidateLog.bucketOf(clockMillis.get()),
            log.bucketAtOrBefore(0)
        );
        assertEquals(
            BlobGcCandidateLog.bucketOf(clockMillis.get() - BlobGcCandidateLog.BUCKET_MILLIS * 10),
            log.bucketAtOrBefore(BlobGcCandidateLog.BUCKET_MILLIS * 10)
        );
    }

    /**
     * Many candidates across many shards of the same index and across different indices, all discoverable
     * from one unfiltered listing -- the property that makes one shared log cheaper than one container per
     * shard: discovery does not multiply with shard count the way {@code GcSchedulerTask}'s own per-shard
     * listing does.
     */
    public void testManyCandidatesAcrossManyShardsAreAllDiscoverableFromOneListing() {
        BlobGcCandidateLog log = log();
        for (int shard = 0; shard < 50; shard++) {
            log.append(new GcCandidate(INDEX_UUID, shard, 1, shard));
            log.append(new GcCandidate("other-index", shard, 1, shard));
        }

        assertEquals(100, log.entriesSince(null).size());
    }

    public void testPruneOlderThanRemovesOnlyBucketsPastTheCutoff() {
        BlobGcCandidateLog log = log();
        clockMillis.set(0);
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 1));

        clockMillis.set(BlobGcCandidateLog.BUCKET_MILLIS * 1000);
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 2));

        int pruned = log.pruneOlderThan(BlobGcCandidateLog.BUCKET_MILLIS * 10);
        assertEquals("only the far-old bucket must be pruned", 1, pruned);

        List<LoggedCandidate> remaining = log.entriesSince(null);
        assertEquals(1, remaining.size());
        assertEquals(2, remaining.get(0).candidate().generation());
    }

    public void testFailedAppendCountReflectsRealFailures() {
        // A container that always fails, standing in for "no object store reachable" -- the same shape
        // BlobDescriptorChangeLogTests uses for its own equivalent assertion.
        Function<BlobPath, BlobContainer> alwaysFails = path -> { throw new RuntimeException("no store here"); };
        BlobGcCandidateLog log = new BlobGcCandidateLog(alwaysFails, BlobPath.cleanPath(), clockMillis::get);

        assertEquals(0, log.failedAppendCount());
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 1));
        assertEquals("a failed append must be counted, not silently dropped", 1, log.failedAppendCount());
    }
}
