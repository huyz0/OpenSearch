/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Optional;

/**
 * Tests for report findings P1 (a writer/compactor generation collision escaped the publish retry
 * loop and failed the primary), P3 (manifests that lost the head CAS stayed in the container,
 * indistinguishable by listing from the one that actually won), and C4 (the retry loop was unbounded,
 * so sustained contention meant unbounded orphaned bundle uploads).
 */
public class ObjectStoreCommitHeadPublisherRaceTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "race-idx";
    private static final int SHARD_ID = 0;

    private BlobContainer blobContainer;
    private BlobContainerManifestStore manifestStore;
    private ObjectStoreCommitPublisher commitPublisher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        manifestStore = new BlobContainerManifestStore(blobContainer);
        commitPublisher = new ObjectStoreCommitPublisher(new BlobContainerBundleStore(blobContainer), manifestStore);
    }

    private static SegmentInfos commit(Directory directory, String... ids) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig().setUseCompoundFile(false).setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (String id : ids) {
                Document doc = new Document();
                doc.add(new StringField("id", id, Field.Store.YES));
                writer.addDocument(doc);
            }
            writer.commit();
        }
        return SegmentInfos.readLatestCommit(directory);
    }

    private Optional<CommitManifest> publish(ObjectStoreCommitHeadPublisher headPublisher, Directory directory, SegmentInfos segmentInfos)
        throws IOException {
        return headPublisher.publishCommitAsHeadReturningManifest(
            directory,
            segmentInfos,
            INDEX_UUID,
            SHARD_ID,
            1L,
            1L,
            1L,
            new WalPosition("epoch-0", 0),
            0,
            PruningStats.empty(),
            null,
            null,
            -1L
        );
    }

    /**
     * <b>P1.</b> A compaction publish that already occupies the writer's target generation is a
     * benign, expected race: both parties compute {@code currentHead.latestManifestGeneration() + 1}
     * under the same term, i.e. the identical manifest name. The publish must skip past the occupied
     * slot and succeed, rather than letting the collision escape as an {@code IOException} that
     * {@code ObjectStoreWriterEngine#commitIndexWriter}'s catch-all turned into {@code failEngine}.
     */
    public void testAForeignManifestOccupyingTheTargetGenerationIsRetriedPastRatherThanFatal() throws Exception {
        // The "compactor": a manifest with entirely different content already sitting at (term 1,
        // generation 1), which is exactly what a fresh writer's first publish targets.
        try (Directory foreign = new ByteBuffersDirectory()) {
            SegmentInfos foreignInfos = commit(foreign, "compacted-a", "compacted-b");
            commitPublisher.publishCommit(
                foreign,
                foreignInfos,
                INDEX_UUID,
                SHARD_ID,
                1L,
                1L,
                1L,
                1L,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }
        assertTrue("precondition: the colliding manifest exists", manifestStore.manifestExists(1L, 1L));

        ShardStateStore store = new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitHeadPublisher headPublisher = new ObjectStoreCommitHeadPublisher(commitPublisher, store);

        try (Directory mine = new ByteBuffersDirectory()) {
            SegmentInfos mineInfos = commit(mine, "mine-1");
            Optional<CommitManifest> published = publish(headPublisher, mine, mineInfos);
            assertTrue("the publish must succeed by moving past the occupied generation, not fail", published.isPresent());
            assertEquals("it must have skipped generation 1 entirely", 2L, published.get().generation());

            ShardHead head = store.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(2L, head.latestManifestGeneration());
        }
    }

    /**
     * <b>P3.</b> A manifest written and then beaten to the head CAS must not be left behind as a
     * name that a listing-based "latest" resolution (rfc-serverless-opensearch.md &sect;6.3's
     * documented bootstrap/fallback discovery path) could select. It is removed as soon as a fresh
     * head read proves the head has moved strictly past its generation, so it can never become head.
     */
    public void testAManifestThatLostTheHeadCasIsRemovedOnceItProvablyCannotBecomeTheHead() throws Exception {
        ShardStateStore realStore = new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(blobContainer);
        // Steals the CAS once: right before our first attempt lands, a foreign actor installs a head
        // at a much higher generation, so our just-written manifest at generation 1 loses and can
        // never win.
        ShardStateStore stealing = new ShardStateStore() {
            private boolean stolen = false;

            @Override
            public Optional<VersionedShardHead> get(String indexUuid, int shardId) throws IOException {
                return realStore.get(indexUuid, shardId);
            }

            @Override
            public CasResult compareAndSet(String indexUuid, int shardId, Optional<Long> expectedVersion, ShardHead newHead)
                throws IOException {
                if (stolen == false) {
                    stolen = true;
                    realStore.compareAndSet(indexUuid, shardId, Optional.empty(), new ShardHead(1L, null, 0L, 5L));
                }
                return realStore.compareAndSet(indexUuid, shardId, expectedVersion, newHead);
            }
        };
        ObjectStoreCommitHeadPublisher headPublisher = new ObjectStoreCommitHeadPublisher(commitPublisher, stealing);

        try (Directory mine = new ByteBuffersDirectory()) {
            SegmentInfos mineInfos = commit(mine, "mine-1");
            Optional<CommitManifest> published = publish(headPublisher, mine, mineInfos);
            assertTrue(published.isPresent());
            assertEquals("the winning publish must land above the stolen head's generation", 6L, published.get().generation());

            assertFalse(
                "the manifest that lost the head CAS must not still be discoverable by name -- a listing-based "
                    + "'latest' resolution cannot tell it apart from the winner",
                manifestStore.manifestExists(1L, 1L)
            );
            assertTrue("the winner must of course still be there", manifestStore.manifestExists(1L, 6L));
        }
    }

    /**
     * <b>C4.</b> The loop is bounded. It used to be {@code for(;;)}, so a store under sustained
     * contention meant spinning forever and, because a bundle name embeds the target generation,
     * uploading an unbounded number of immediately-orphaned bundles. It must give up with an
     * {@code IOException} the caller's own retry-with-backoff can handle.
     */
    public void testTheCasRetryLoopIsBoundedInsteadOfSpinningForever() throws Exception {
        ShardStateStore alwaysConflicting = new ShardStateStore() {
            @Override
            public Optional<VersionedShardHead> get(String indexUuid, int shardId) {
                return Optional.empty();
            }

            @Override
            public CasResult compareAndSet(String indexUuid, int shardId, Optional<Long> expectedVersion, ShardHead newHead) {
                return CasResult.VERSION_CONFLICT;
            }
        };
        ObjectStoreCommitHeadPublisher headPublisher = new ObjectStoreCommitHeadPublisher(commitPublisher, alwaysConflicting);

        try (Directory mine = new ByteBuffersDirectory()) {
            SegmentInfos mineInfos = commit(mine, "mine-1");
            IOException failure = expectThrows(IOException.class, () -> publish(headPublisher, mine, mineInfos));
            assertTrue(
                "the failure must name the bounded attempt budget, got: " + failure.getMessage(),
                failure.getMessage().contains(String.valueOf(ObjectStoreCommitHeadPublisher.MAX_PUBLISH_ATTEMPTS))
            );
            assertTrue(
                "no more than the attempt budget's worth of bundles can have been uploaded",
                new BlobContainerBundleStore(blobContainer).listBundleNames().size() <= ObjectStoreCommitHeadPublisher.MAX_PUBLISH_ATTEMPTS
            );
        }
    }
}
