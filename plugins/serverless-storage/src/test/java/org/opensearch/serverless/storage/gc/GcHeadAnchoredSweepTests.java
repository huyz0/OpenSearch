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
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The crash-during-publish interleaving, which used to end in unrecoverable data loss.
 *
 * <h2>What went wrong</h2>
 *
 * A commit is two durable steps: write the bundle and the manifest blob, then compare-and-swap the shard
 * head onto it. A writer killed between them -- OOM killer, spot reclaim, {@code kill -9}, or just an
 * {@code IOException} out of the CAS -- leaves a manifest blob that was never published. The sweep used to
 * infer "the latest manifest" by taking the maximum over its own blob listing, so that orphan <em>became</em>
 * the latest, the shard's real head looked superseded, and one retention window later GC deleted the live
 * head manifest and every bundle only it referenced. The shard could then never open again and its segments
 * were gone.
 *
 * <p>Each test here builds exactly that state and asserts the head survives. Before the fix the first two
 * would have deleted it.
 */
public class GcHeadAnchoredSweepTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private BlobContainer blobContainer;
    private BlobContainerBundleStore bundleStore;
    private BlobContainerManifestStore manifestStore;
    private DurablePinRegistry pinRegistry;
    private ShardStateStore shardStateStore;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        bundleStore = new BlobContainerBundleStore(blobContainer);
        manifestStore = new BlobContainerManifestStore(blobContainer);
        pinRegistry = new BlobContainerDurablePinRegistry(blobContainer);
        shardStateStore = new BlobContainerShardStateStore(blobContainer);
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    /** Writes a real bundle and manifest at (term, generation) -- the durable half of a publish, with no head CAS. */
    private CommitManifest writeManifestOnly(long primaryTerm, long generation, long createdAtMillis) throws Exception {
        String bundleName = BlobContainerBundleStore.NAME_PREFIX + INDEX_UUID + "-" + SHARD_ID + "-" + primaryTerm + "-" + generation;
        String fileName = "segments_" + primaryTerm + "_" + generation;
        byte[] content = ("content-" + primaryTerm + "-" + generation).getBytes("UTF-8");
        var bundle = bundleStore.writeBundle(bundleName, List.of(new BundleFileContent(fileName, content)));
        var entry = bundle.entries().get(fileName);
        CommitManifest manifest = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            primaryTerm,
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

    private GcSchedulerTask taskWith(long retentionWindowMillis, long nowMillis, ShardStateStore headStore) {
        GcSchedulerConfig config = new GcSchedulerConfig(
            TimeValue.timeValueMinutes(5),
            retentionWindowMillis,
            manifestStore,
            bundleStore,
            pinRegistry,
            headStore,
            new BlobContainerGcSweepStateStore(blobContainer, INDEX_UUID, SHARD_ID)
        );
        // Zero skew allowance: these tests are about which manifest is "latest", not about the margin.
        return new GcSchedulerTask(threadPool, config.interval(), INDEX_UUID, SHARD_ID, config, () -> nowMillis, 0L);
    }

    /**
     * The plain case: same term, one generation ahead. The head is at (7, 41), a crashed writer left
     * (7, 42) behind, and nothing has published for longer than the retention window.
     */
    public void testAnUnpublishedManifestOneGenerationAheadOfTheHeadDoesNotMakeTheHeadDeletable() throws Exception {
        long now = System.currentTimeMillis();
        long farInThePast = now - TimeValue.timeValueDays(1).millis();

        CommitManifest liveHead = writeManifestOnly(7, 41, farInThePast);
        assertEquals(
            CasResult.SUCCESS,
            shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), new ShardHead(7, null, 0L, 41))
        );
        // Past the ordinary 30-minute window, well inside the longer window an unpublished manifest gets.
        CommitManifest crashedOrphan = writeManifestOnly(7, 42, now - TimeValue.timeValueMinutes(45).millis());

        GcSchedulerTask task = taskWith(TimeValue.timeValueMinutes(30).millis(), now, shardStateStore);
        try {
            task.sweepForTesting();

            List<CommitManifest> remaining = manifestStore.listManifests();
            assertTrue(
                "the live head must survive: the manifest one generation ahead of it was never published, so it " + "supersedes nothing",
                remaining.stream().anyMatch(m -> m.primaryTerm() == 7 && m.generation() == 41)
            );
            assertTrue(
                "the head's bundles must survive too -- deleting them is what made this unrecoverable",
                bundleStore.listBundleNames().containsAll(liveHead.referencedBundles())
            );
            assertTrue(
                "the unpublished orphan gets its own, longer window rather than being deleted immediately: its "
                    + "writer may still be about to CAS the head onto it",
                remaining.stream().anyMatch(m -> m.generation() == crashedOrphan.generation())
            );
        } finally {
            task.close();
        }
    }

    /**
     * The failover variant, which is worse because term dominates the ordering: a new writer at term 8 wrote
     * (8, 42) and died before its CAS. {@code acquireOrRenewLease} deliberately does not advance the head's
     * {@code primaryTerm}, so the orphan's term is strictly greater than the head's and a listing-derived
     * "latest" picks it every time.
     */
    public void testAnUnpublishedManifestUnderANewerTermDoesNotMakeTheHeadDeletable() throws Exception {
        long now = System.currentTimeMillis();
        long farInThePast = now - TimeValue.timeValueDays(1).millis();

        writeManifestOnly(7, 41, farInThePast);
        assertEquals(
            CasResult.SUCCESS,
            shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), new ShardHead(7, null, 0L, 41))
        );
        writeManifestOnly(8, 42, farInThePast);

        GcSchedulerTask task = taskWith(TimeValue.timeValueMinutes(30).millis(), now, shardStateStore);
        try {
            task.sweepForTesting();
            assertTrue(
                "a manifest under a term the head has never published under is an orphan, not the latest",
                manifestStore.listManifests().stream().anyMatch(m -> m.primaryTerm() == 7 && m.generation() == 41)
            );
        } finally {
            task.close();
        }
    }

    /** An orphan does eventually go, once it has outlived the far longer window unpublished manifests get. */
    public void testAnUnpublishedOrphanIsReclaimedOnceItIsFarOlderThanTheOrdinaryWindow() throws Exception {
        long now = System.currentTimeMillis();
        long retentionWindowMillis = TimeValue.timeValueMinutes(30).millis();
        long longAgo = now - (GcSchedulerTask.ORPHAN_RETENTION_WINDOW_MULTIPLIER + 1) * retentionWindowMillis;

        writeManifestOnly(7, 41, longAgo);
        assertEquals(
            CasResult.SUCCESS,
            shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), new ShardHead(7, null, 0L, 41))
        );
        writeManifestOnly(7, 42, longAgo);

        GcSchedulerTask task = taskWith(retentionWindowMillis, now, shardStateStore);
        try {
            task.sweepForTesting();
            List<CommitManifest> remaining = manifestStore.listManifests();
            assertFalse(
                "an orphan this old cannot still be one CAS away from publication",
                remaining.stream().anyMatch(m -> m.generation() == 42)
            );
            assertTrue("the head is never deletable at any age", remaining.stream().anyMatch(m -> m.generation() == 41));
        } finally {
            task.close();
        }
    }

    /** No head at all: every manifest present is unpublished, so the sweep has no basis for calling any of it garbage. */
    public void testASweepWithNoHeadDeletesNothing() throws Exception {
        long now = System.currentTimeMillis();
        long farInThePast = now - TimeValue.timeValueDays(1).millis();
        writeManifestOnly(1, 1, farInThePast);
        writeManifestOnly(1, 2, farInThePast);

        GcSchedulerTask task = taskWith(TimeValue.timeValueMinutes(30).millis(), now, shardStateStore);
        try {
            task.sweepForTesting();
            assertEquals("a shard with no published head must be left entirely alone", 2, manifestStore.listManifests().size());
        } finally {
            task.close();
        }
    }

    /** A head that cannot be read is not "no head": the sweep must fail closed and delete nothing at all. */
    public void testASweepWhoseHeadReadFailsDeletesNothingAndSurfacesTheFailure() throws Exception {
        long now = System.currentTimeMillis();
        long farInThePast = now - TimeValue.timeValueDays(1).millis();
        writeManifestOnly(1, 1, farInThePast);
        writeManifestOnly(1, 2, farInThePast);
        assertEquals(
            CasResult.SUCCESS,
            shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), new ShardHead(1, null, 0L, 2))
        );

        ShardStateStore unreadableHead = new ShardStateStore() {
            @Override
            public Optional<VersionedShardHead> get(String indexUuid, int shardId) throws IOException {
                throw new IOException("simulated head register read failure");
            }

            @Override
            public CasResult compareAndSet(String indexUuid, int shardId, Optional<Long> expectedVersion, ShardHead newHead) {
                throw new UnsupportedOperationException("not used by a sweep");
            }
        };

        GcSchedulerTask task = taskWith(TimeValue.timeValueMinutes(30).millis(), now, unreadableHead);
        try {
            expectThrows(IOException.class, task::sweepForTesting);
            assertEquals("a sweep that cannot see the head must not have deleted anything", 2, manifestStore.listManifests().size());
            Set<String> bundles = bundleStore.listBundleNames();
            assertEquals("and must not have deleted any bundle either", 2, bundles.size());
        } finally {
            task.close();
        }
    }
}
