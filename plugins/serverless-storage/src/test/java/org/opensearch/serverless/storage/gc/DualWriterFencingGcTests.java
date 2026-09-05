/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitHeadPublisher;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Proves the dual-writer fencing property rfc-serverless-opensearch.md &sect;17's testing strategy
 * calls for ("old-term writer keeps publishing during/after failover; assert its manifests are
 * never visible and are GC'd") end to end -- signal to sweep, using the real production classes
 * ({@link ObjectStoreCommitPublisher}, {@link ObjectStoreCommitHeadPublisher}, {@link
 * BlobContainerShardStateStore}, {@link GcSchedulerTask}), not mocks.
 *
 * <p>{@link ObjectStoreCommitHeadPublisher#publishCommitAsHead}'s own javadoc already documents the
 * mechanism this proves: the term check happens before packaging, so the common case (a writer
 * already superseded by the time it next tries to publish) never writes anything at all -- nothing
 * for GC to reclaim. The genuinely interesting, previously-untested case is narrower: an old-term
 * writer reads the head, passes the term check (nothing newer has published yet), durably packages
 * its bundle and manifest, and only then loses the race -- because a new-term writer's own publish
 * against that same prior head lands first. That leaves a real, durably-written manifest/bundle
 * pair that was never referenced by any head: exactly the "orphan" {@link
 * ObjectStoreCommitHeadPublisher}'s own class javadoc says is "simply unreferenced garbage, eligible
 * for the same GC path as any other orphaned bundle."
 *
 * <p>This test reproduces that interleaving deterministically, without real threads, by manually
 * driving the same two low-level steps {@code publishCommitAsHead} itself uses internally
 * ({@link ObjectStoreCommitPublisher#publishCommit} to package, then {@link
 * ShardStateStore#compareAndSet} to attempt to become head) for the old-term writer, with the
 * new-term writer's own real {@code publishCommitAsHead} call landing in between -- the same
 * interleaving a genuine concurrent race would produce, forced instead of hoped-for.
 */
public class DualWriterFencingGcTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private BlobContainer blobContainer;
    private BlobContainerManifestStore manifestStore;
    private BlobContainerBundleStore bundleStore;
    private ShardStateStore shardStateStore;
    private DurablePinRegistry pinRegistry;
    private ObjectStoreCommitPublisher commitPublisher;
    private ObjectStoreCommitHeadPublisher headPublisher;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore fsBlobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(fsBlobStore, BlobPath.cleanPath(), fsBlobStore.path());
        manifestStore = new BlobContainerManifestStore(blobContainer);
        bundleStore = new BlobContainerBundleStore(blobContainer);
        shardStateStore = new BlobContainerShardStateStore(blobContainer);
        pinRegistry = new BlobContainerDurablePinRegistry(blobContainer);
        commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        headPublisher = new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore);
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private static SegmentInfos commitOneDocument(Directory directory, String docId) throws Exception {
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("id", docId, Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();
        }
        return SegmentInfos.readLatestCommit(directory);
    }

    public void testOldTermWritersOrphanedManifestIsNeverVisibleAndIsReclaimedByGc() throws Exception {
        // Establish a real initial head under term 1, exactly like any writer's first commit.
        try (Directory initialDirectory = new ByteBuffersDirectory()) {
            SegmentInfos initialSegments = commitOneDocument(initialDirectory, "0");
            assertTrue(
                headPublisher.publishCommitAsHead(
                    initialDirectory,
                    initialSegments,
                    INDEX_UUID,
                    SHARD_ID,
                    1L,
                    0,
                    0,
                    null,
                    0,
                    PruningStats.empty()
                )
            );
        }

        // Both the old (term 1) and new (term 2) writer see this same head -- the state right
        // before the race. Snapshot it once, exactly like each writer's own read would.
        VersionedShardHead headBeforeRace = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
        long targetGeneration = headBeforeRace.head().latestManifestGeneration() + 1;
        assertEquals(2L, targetGeneration);

        // The old (term 1) writer: passes the term check against headBeforeRace (1 <= 1) and
        // durably packages its commit -- a real bundle+manifest write via the same
        // ObjectStoreCommitPublisher#publishCommit the real publishCommitAsHead uses internally.
        // It has not yet attempted the head CAS.
        CommitManifest oldWritersOrphanedManifest;
        try (Directory oldWriterDirectory = new ByteBuffersDirectory()) {
            SegmentInfos oldWriterSegments = commitOneDocument(oldWriterDirectory, "1-from-old-writer");
            oldWritersOrphanedManifest = commitPublisher.publishCommit(
                oldWriterDirectory,
                oldWriterSegments,
                INDEX_UUID,
                SHARD_ID,
                1L,
                targetGeneration,
                0,
                0,
                null,
                0,
                PruningStats.empty()
            );
        }
        assertTrue(
            "the old writer's manifest must be real and durably written, not merely attempted",
            manifestStore.manifestExists(1L, targetGeneration)
        );

        // The new (term 2) writer now publishes for real, against the exact same headBeforeRace --
        // this is the legitimate takeover, landing first.
        try (Directory newWriterDirectory = new ByteBuffersDirectory()) {
            SegmentInfos newWriterSegments = commitOneDocument(newWriterDirectory, "1-from-new-writer");
            assertTrue(
                "the new-term writer's publish must succeed -- nothing has fenced it out",
                headPublisher.publishCommitAsHead(
                    newWriterDirectory,
                    newWriterSegments,
                    INDEX_UUID,
                    SHARD_ID,
                    2L,
                    0,
                    0,
                    null,
                    0,
                    PruningStats.empty()
                )
            );
        }

        // The old writer now attempts to complete its own publish -- the head CAS
        // publishCommitAsHead would have attempted internally, replicated here manually to force
        // the exact interleaving. It must lose: the version it read no longer matches.
        ShardHead oldWritersIntendedHead = headBeforeRace.head().withPublishedGeneration(1L, targetGeneration);
        CasResult oldWritersCasResult = shardStateStore.compareAndSet(
            INDEX_UUID,
            SHARD_ID,
            Optional.of(headBeforeRace.version()),
            oldWritersIntendedHead
        );
        assertNotEquals(
            "the old writer must lose the CAS race -- the new writer's publish already moved the version",
            CasResult.SUCCESS,
            oldWritersCasResult
        );

        // The published head/latest-manifest view must reflect only the new writer's content --
        // the old writer's manifest, despite being real and durably written, must never be visible.
        CommitManifest latest = headPublisher.readLatestManifest(INDEX_UUID, SHARD_ID).orElseThrow();
        assertEquals(2L, latest.primaryTerm());
        assertEquals(targetGeneration, latest.generation());
        assertNotEquals(
            "the visible manifest's own files must differ from the old writer's orphaned commit's files",
            oldWritersOrphanedManifest.files(),
            latest.files()
        );

        VersionedShardHead headAfterRace = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
        assertEquals(2L, headAfterRace.head().primaryTerm());

        // GC must reclaim the orphan. A minimal positive retention window plus a short real sleep
        // (rather than a hand-constructed past timestamp) keeps this exercising the actual
        // publish-time System.currentTimeMillis() path, not a synthetic one.
        Thread.sleep(50);
        GcSchedulerConfig gcConfig = new GcSchedulerConfig(
            TimeValue.timeValueMinutes(5),
            1L,
            manifestStore,
            bundleStore,
            pinRegistry,
            shardStateStore,
            new BlobContainerGcSweepStateStore(blobContainer, INDEX_UUID, SHARD_ID)
        );
        // Zero clock-skew allowance: this test is about term ordering, and it deliberately uses a
        // one-millisecond retention window against manifests stamped with the real publish-time clock. The
        // production allowance would add five minutes to that window and turn the assertion below into a
        // statement about the margin rather than about fencing.
        GcSchedulerTask gcTask = new GcSchedulerTask(
            threadPool,
            gcConfig.interval(),
            INDEX_UUID,
            SHARD_ID,
            gcConfig,
            System::currentTimeMillis,
            0L
        );
        try {
            gcTask.sweepForTesting();
        } finally {
            gcTask.close();
        }

        List<CommitManifest> remaining = manifestStore.listManifests();
        assertFalse(
            "the old writer's orphaned manifest must be gone after GC",
            remaining.stream().anyMatch(m -> m.primaryTerm() == 1L && m.generation() == targetGeneration)
        );
        assertTrue(
            "the winning manifest must survive GC",
            remaining.stream().anyMatch(m -> m.primaryTerm() == 2L && m.generation() == targetGeneration)
        );
    }
}
