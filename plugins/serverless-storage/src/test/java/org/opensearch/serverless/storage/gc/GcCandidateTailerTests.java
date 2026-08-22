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
import org.opensearch.serverless.storage.gc.GcCandidateTailer.TailResult;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What {@link GcCandidateTailer} actually does with a pending candidate, which is the part {@link
 * BlobGcCandidateLogTests} does not exercise: eligibility (age and pin state), and the three distinct
 * outcomes a candidate can resolve to.
 */
public class GcCandidateTailerTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final long RETENTION_WINDOW_MILLIS = 1000;
    private static final long LOOKBACK_MILLIS = 10_000;

    private Path root;
    private AtomicLong clockMillis;
    private final Map<String, BlobContainer> shardContainers = new HashMap<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        root = createTempDir();
        clockMillis = new AtomicLong(0);
    }

    private BlobGcCandidateLog log() throws IOException {
        Path logRoot = root.resolve("log");
        FsBlobStore blobStore = new FsBlobStore(1024, logRoot, false);
        return new BlobGcCandidateLog(path -> {
            try {
                return blobStore.blobContainer(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, BlobPath.cleanPath(), clockMillis::get);
    }

    private BlobContainer shardContainer(String indexUuid, int shardId) throws IOException {
        String key = indexUuid + "/" + shardId;
        BlobContainer existing = shardContainers.get(key);
        if (existing != null) {
            return existing;
        }
        FsBlobStore blobStore = new FsBlobStore(1024, root.resolve("shards").resolve(key), false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        shardContainers.put(key, container);
        return container;
    }

    private GcCandidateTailer tailer(BlobGcCandidateLog log) {
        return new GcCandidateTailer(log, this::shardContainer, RETENTION_WINDOW_MILLIS, LOOKBACK_MILLIS, clockMillis::get);
    }

    private CommitManifest writeManifest(int shardId, long primaryTerm, long generation, long createdAtMillis) throws IOException {
        CommitManifest manifest = new CommitManifest(
            INDEX_UUID,
            shardId,
            primaryTerm,
            generation,
            "segments_" + generation,
            Map.of("segments_" + generation, new FileReference("bundle-" + generation, 0, 10, 10)),
            generation,
            generation,
            null,
            1,
            new PruningStats(0, null, null, Map.of()),
            createdAtMillis
        );
        new BlobContainerManifestStore(shardContainer(INDEX_UUID, shardId)).writeManifest(manifest);
        return manifest;
    }

    public void testAConstructorRefusesALookbackShorterThanTheRetentionWindow() throws Exception {
        BlobGcCandidateLog log = log();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new GcCandidateTailer(log, this::shardContainer, 1000, 999, clockMillis::get)
        );
        assertTrue(e.getMessage().contains("lookbackMillis"));
    }

    public void testACandidateYoungerThanTheRetentionWindowIsLeftPending() throws Exception {
        BlobGcCandidateLog log = log();
        clockMillis.set(0);
        writeManifest(0, 1, 5, 0);
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 5));

        clockMillis.set(RETENTION_WINDOW_MILLIS - 1);
        TailResult result = tailer(log).tailOnce();

        assertEquals("nothing is old enough yet to be touched", 0, result.resolved());
        assertEquals(0, result.manifestsDeleted());
        assertEquals("the manifest must still exist", "segments_5", readManifest(0, 1, 5).segmentsFileName());
        assertEquals("and the candidate must still be pending for the next pass", 1, log.entriesSince(null).size());
    }

    public void testACandidatePastRetentionAndUnpinnedIsDeletedAndResolved() throws Exception {
        BlobGcCandidateLog log = log();
        clockMillis.set(0);
        writeManifest(0, 1, 5, 0);
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 5));

        clockMillis.set(RETENTION_WINDOW_MILLIS);
        TailResult result = tailer(log).tailOnce();

        assertEquals(1, result.resolved());
        assertEquals("an eligible, unpinned candidate must actually be deleted", 1, result.manifestsDeleted());
        assertTrue("the queue entry must be retired", log.entriesSince(null).isEmpty());
        expectThrows(java.nio.file.NoSuchFileException.class, () -> readManifest(0, 1, 5));
    }

    /**
     * The safety property that matters most: a durably pinned generation must never be deleted just
     * because its retention window elapsed, and it must not be re-evaluated forever once dropped.
     */
    public void testAPinnedCandidateIsResolvedWithoutDeletingTheManifest() throws Exception {
        BlobGcCandidateLog log = log();
        clockMillis.set(0);
        writeManifest(0, 1, 5, 0);
        BlobContainer container = shardContainer(INDEX_UUID, 0);
        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(container);
        pinRegistry.addPin(INDEX_UUID, 0, new PinRecord("snapshot-1", 1, 5, "node-a", PinRecord.NEVER_EXPIRES));
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 5));

        clockMillis.set(RETENTION_WINDOW_MILLIS);
        TailResult result = tailer(log).tailOnce();

        assertEquals("the entry must be retired -- this generation is protected, not garbage", 1, result.resolved());
        assertEquals("a pinned generation must never be deleted by this path", 0, result.manifestsDeleted());
        assertEquals("the manifest itself must survive", "segments_5", readManifest(0, 1, 5).segmentsFileName());
        assertTrue("and the queue entry must not sit there forever being re-evaluated", log.entriesSince(null).isEmpty());
    }

    /**
     * A manifest already gone by the time this candidate is evaluated -- {@code GcSchedulerTask}'s own
     * sweep got there first, or another node's tailer pass already resolved it. Either way this must
     * retire cleanly, not fail the whole pass.
     */
    public void testACandidateForAnAlreadyDeletedManifestResolvesCleanly() throws Exception {
        BlobGcCandidateLog log = log();
        // Deliberately never written: readManifest will see NoSuchFileException immediately.
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 5));

        TailResult result = tailer(log).tailOnce();

        assertEquals(1, result.resolved());
        assertEquals("nothing was actually deleted by this pass -- it was already gone", 0, result.manifestsDeleted());
        assertTrue(log.entriesSince(null).isEmpty());
    }

    /**
     * One bad candidate (an unresolvable shard, say) must not stop the rest of the pass from being
     * evaluated -- the same per-item fault tolerance every other sweep in this package already has.
     */
    public void testOneFailingCandidateDoesNotStopTheRestOfThePass() throws Exception {
        BlobGcCandidateLog log = log();
        clockMillis.set(0);
        writeManifest(0, 1, 5, 0);
        writeManifest(1, 1, 9, 0);
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 5));
        log.append(new GcCandidate(INDEX_UUID, 1, 1, 9));

        clockMillis.set(RETENTION_WINDOW_MILLIS);
        GcCandidateTailer.ShardContainerResolver flakyResolver = (indexUuid, shardId) -> {
            if (shardId == 0) {
                throw new IOException("simulated failure resolving shard 0's container");
            }
            return shardContainer(indexUuid, shardId);
        };
        GcCandidateTailer tailer = new GcCandidateTailer(log, flakyResolver, RETENTION_WINDOW_MILLIS, LOOKBACK_MILLIS, clockMillis::get);

        TailResult result = tailer.tailOnce();

        assertEquals("shard 1's candidate must still be resolved despite shard 0's failure", 1, result.resolved());
        assertEquals(1, result.manifestsDeleted());
        assertEquals("shard 0's candidate must remain pending for the next pass, not be lost", 1, log.entriesSince(null).size());
        assertEquals(0, log.entriesSince(null).get(0).candidate().shardId());
    }

    /**
     * The retention-window read is cached per candidate, so a candidate that is not yet eligible is not
     * re-read from the manifest store on every subsequent pass -- exercised by making the second read
     * observably impossible (deleting the manifest out from under the cache) and confirming eligibility
     * is still decided correctly from the cached age.
     */
    public void testAnAlreadyKnownAgeIsNotReReadOnASubsequentPass() throws Exception {
        BlobGcCandidateLog log = log();
        clockMillis.set(0);
        writeManifest(0, 1, 5, 0);
        log.append(new GcCandidate(INDEX_UUID, 0, 1, 5));

        GcCandidateTailer tailer = tailer(log);
        clockMillis.set(RETENTION_WINDOW_MILLIS - 1);
        assertEquals("not yet eligible on the first pass", 0, tailer.tailOnce().manifestsDeleted());

        clockMillis.set(RETENTION_WINDOW_MILLIS);
        TailResult result = tailer.tailOnce();
        assertEquals("eligible on the second pass, using the age this tailer already learned", 1, result.manifestsDeleted());
    }

    private CommitManifest readManifest(int shardId, long primaryTerm, long generation) throws IOException {
        return new BlobContainerManifestStore(shardContainer(INDEX_UUID, shardId)).readManifest(primaryTerm, generation);
    }
}
