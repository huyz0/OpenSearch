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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Optional;

public class ObjectStoreCommitHeadPublisherTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private ShardStateStore shardStateStore;
    private ObjectStoreCommitHeadPublisher headPublisher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        headPublisher = new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore);
    }

    private static SegmentInfos commitOneDocument(Directory directory, String id) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            Document doc = new Document();
            doc.add(new StringField("id", id, Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();
        }
        return SegmentInfos.readLatestCommit(directory);
    }

    public void testFirstEverPublicationActivatesTheShardHeadAtThatTermAtGenerationOne() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitOneDocument(directory, "1");

            boolean published = headPublisher.publishCommitAsHead(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                3,
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            assertTrue(published);
            ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(3, head.primaryTerm());
            // The generation is always the live head's + 1, never derived from local Lucene state --
            // the very first publish under a fresh head (generation 0, i.e. ShardHead#initial()'s
            // sentinel) always lands at generation 1, regardless of what segmentInfos.getGeneration()
            // happens to be.
            assertEquals(1, head.latestManifestGeneration());
        }
    }

    public void testSecondPublicationUnderTheSameTermAdvancesTheGenerationByExactlyOne() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos first = commitOneDocument(directory, "1");
            assertTrue(
                headPublisher.publishCommitAsHead(
                    directory,
                    first,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );
            assertEquals(1, shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head().latestManifestGeneration());

            SegmentInfos second = commitOneDocument(directory, "2");
            assertTrue(
                headPublisher.publishCommitAsHead(
                    directory,
                    second,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    1,
                    1,
                    new WalPosition("epoch-0", 1),
                    0,
                    PruningStats.empty()
                )
            );

            ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(2, head.latestManifestGeneration());
        }
    }

    public void testPublicationUnderADifferentTermThanTheCurrentHeadIsFencedOut() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitOneDocument(directory, "1");

            // Someone else already activated the shard at term 5 (e.g. this node's lease expired
            // and another node took over) before this writer's commit publication runs.
            assertEquals(
                CasResult.SUCCESS,
                shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), new ShardHead(5, "other-node", 0L, 0L))
            );

            boolean published = headPublisher.publishCommitAsHead(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            assertFalse("a stale-term writer must be fenced out, not allowed to publish", published);
            ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(5, head.primaryTerm());
            assertEquals(0, head.latestManifestGeneration());
        }
    }

    public void testPublicationAfterAConcurrentCompactionUnderTheSameTermSucceedsAtTheNextLiveGeneration() throws Exception {
        // With the writer's generation numbering decoupled from local Lucene state (formally
        // verified in plugins/serverless-storage/formal/ShardHead.tla's PublishDecoupled/
        // SpecDecoupled, ShardHeadDecoupled.cfg), a compactor advancing the head no longer fences the
        // writer out: the writer simply computes its target as the live head's generation + 1, same
        // as the compactor itself does, and lands its own content one slot after whatever the
        // compactor last published.
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos first = commitOneDocument(directory, "1");
            assertTrue(
                headPublisher.publishCommitAsHead(
                    directory,
                    first,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );

            // Simulate a compactor advancing the head well past this writer's first publish, under
            // the same term (exactly what LuceneMergeCompactionPublisher's rebase-on-CAS publish
            // does, independently of this writer's local commit history).
            VersionedShardHead afterFirstPublish = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
            long compactedGeneration = afterFirstPublish.head().latestManifestGeneration() + 100;
            ShardHead compactedHead = afterFirstPublish.head().withPublishedGeneration(compactedGeneration);
            assertEquals(
                CasResult.SUCCESS,
                shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.of(afterFirstPublish.version()), compactedHead)
            );

            SegmentInfos second = commitOneDocument(directory, "2");
            boolean published = headPublisher.publishCommitAsHead(
                directory,
                second,
                INDEX_UUID,
                SHARD_ID,
                1,
                1,
                1,
                new WalPosition("epoch-0", 1),
                0,
                PruningStats.empty()
            );

            assertTrue("a writer resuming after a concurrent compaction must succeed at the next live generation", published);
            ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(compactedGeneration + 1, head.latestManifestGeneration());
        }
    }
}
