/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class PartitionRewritePublisherTests extends OpenSearchTestCase {

    private static final String TARGET_INDEX_UUID = "target-idx";
    private static final int SHARD_ID = 0;
    private static final int DOC_COUNT = 100;

    private BlobContainer targetContainer;
    private BlobContainerBundleStore bundleStore;
    private BlobContainerManifestStore manifestStore;
    private ShardStateStore shardStateStore;
    private BlobContainerShardPartitionStore partitionStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        targetContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        bundleStore = new BlobContainerBundleStore(targetContainer);
        manifestStore = new BlobContainerManifestStore(targetContainer);
        shardStateStore = new BlobContainerShardStateStore(targetContainer);
        partitionStore = new BlobContainerShardPartitionStore(targetContainer);
    }

    private CommitManifest publishTargetCommit() throws Exception {
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                for (int i = 0; i < DOC_COUNT; i++) {
                    Document doc = new Document();
                    doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId("doc-" + i), IdFieldMapper.Defaults.FIELD_TYPE));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                TARGET_INDEX_UUID,
                SHARD_ID,
                1,
                1,
                DOC_COUNT,
                DOC_COUNT,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }
        assertEquals(
            CasResult.SUCCESS,
            shardStateStore.compareAndSet(TARGET_INDEX_UUID, SHARD_ID, Optional.empty(), new ShardHead(1, null, 0L, manifest.generation()))
        );
        return manifest;
    }

    private PartitionRewritePublisher publisher() {
        return new PartitionRewritePublisher(
            TARGET_INDEX_UUID,
            SHARD_ID,
            shardStateStore,
            manifestStore,
            bundleStore,
            new ObjectStoreCommitMaterializer(bundleStore),
            new ObjectStoreCommitPublisher(bundleStore, manifestStore),
            partitionStore
        );
    }

    public void testRewriteIsANoOpWithNoPartitionDescriptor() throws Exception {
        publishTargetCommit();
        assertFalse("a shard that was never split has nothing to rewrite", publisher().rewrite());
    }

    public void testRewriteIsANoOpWithNoPublishedHead() throws Exception {
        partitionStore.writeDescriptor(new ShardPartitionDescriptor(0, 2));
        assertFalse("a shard with no published head has nothing to rewrite", publisher().rewrite());
    }

    public void testRewritePublishesAManifestContainingOnlyThisPartitionsDocumentsAndClearsTheDescriptor() throws Exception {
        publishTargetCommit();
        ShardPartitionDescriptor descriptor = new ShardPartitionDescriptor(0, 3);
        partitionStore.writeDescriptor(descriptor);

        // Compute the expected doc set independently, the same way the reader-side filter would.
        Set<String> expectedIds = new HashSet<>();
        for (int i = 0; i < DOC_COUNT; i++) {
            String id = "doc-" + i;
            if (RoutingPartitionFilter.matches(id, descriptor)) {
                expectedIds.add(id);
            }
        }
        assertFalse("this test needs a real, non-trivial partition to be meaningful", expectedIds.isEmpty());

        assertTrue("a real rewrite must have been performed", publisher().rewrite());

        assertTrue(
            "the descriptor must be cleared once the rewrite is durably published -- future engine "
                + "opens must stop applying the filter",
            partitionStore.readDescriptor().isEmpty()
        );

        VersionedShardHead newHead = shardStateStore.get(TARGET_INDEX_UUID, SHARD_ID).orElseThrow();
        CommitManifest rewrittenManifest = manifestStore.readManifest(
            newHead.head().primaryTerm(),
            newHead.head().latestManifestGeneration()
        );

        Directory rewrittenDirectory = new ByteBuffersDirectory();
        new ObjectStoreCommitMaterializer(bundleStore).materialize(rewrittenManifest, rewrittenDirectory);
        try (DirectoryReader reader = DirectoryReader.open(rewrittenDirectory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            ScoreDoc[] hits = searcher.search(new MatchAllDocsQuery(), DOC_COUNT * 2).scoreDocs;
            assertEquals(
                "the physically rewritten bundle must contain exactly this partition's documents, no more, no fewer",
                expectedIds.size(),
                hits.length
            );
            Set<String> actualIds = new HashSet<>();
            for (ScoreDoc hit : hits) {
                byte[] idBytes = reader.storedFields().document(hit.doc).getField(IdFieldMapper.NAME).binaryValue().bytes;
                actualIds.add(Uid.decodeId(idBytes));
            }
            assertEquals(expectedIds, actualIds);
        }
    }

    public void testRewriteIsANoOpTheSecondTimeItsCalled() throws Exception {
        publishTargetCommit();
        partitionStore.writeDescriptor(new ShardPartitionDescriptor(1, 2));

        assertTrue(publisher().rewrite());
        assertFalse("calling rewrite again after the descriptor is already cleared must be a safe no-op", publisher().rewrite());
    }

    public void testRewriteTreatsAnAmbiguousCasFaultAsSuccessWhenTheWriteActuallyLanded() throws Exception {
        publishTargetCommit();
        partitionStore.writeDescriptor(new ShardPartitionDescriptor(0, 2));

        ShardStateStore faultingAfterRealCas = new CasFaultInjectingShardStateStore(shardStateStore, true);
        PartitionRewritePublisher publisher = new PartitionRewritePublisher(
            TARGET_INDEX_UUID,
            SHARD_ID,
            faultingAfterRealCas,
            manifestStore,
            bundleStore,
            new ObjectStoreCommitMaterializer(bundleStore),
            new ObjectStoreCommitPublisher(bundleStore, manifestStore),
            partitionStore
        );

        assertTrue(
            "an ambiguous CAS fault whose write actually landed must be treated as success, not a failure to surface",
            publisher.rewrite()
        );
        assertTrue(
            "the descriptor must still be cleared when the ambiguous fault's write actually landed",
            partitionStore.readDescriptor().isEmpty()
        );
    }

    public void testRewriteCleansUpItsOwnOrphanedManifestWhenAnAmbiguousCasFaultDidNotActuallyLand() throws Exception {
        publishTargetCommit();
        partitionStore.writeDescriptor(new ShardPartitionDescriptor(0, 2));

        ShardStateStore faultingWithoutRealCas = new CasFaultInjectingShardStateStore(shardStateStore, false);
        PartitionRewritePublisher publisher = new PartitionRewritePublisher(
            TARGET_INDEX_UUID,
            SHARD_ID,
            faultingWithoutRealCas,
            manifestStore,
            bundleStore,
            new ObjectStoreCommitMaterializer(bundleStore),
            new ObjectStoreCommitPublisher(bundleStore, manifestStore),
            partitionStore
        );

        java.io.IOException thrown = expectThrows(java.io.IOException.class, publisher::rewrite);
        assertEquals("injected ambiguous CAS fault", thrown.getMessage());
        assertTrue(
            "the descriptor must survive an ambiguous CAS fault whose write never landed -- nothing succeeded",
            partitionStore.readDescriptor().isPresent()
        );

        // The manifest this failed attempt published at (primaryTerm, currentGeneration + 1) must
        // not be left occupying that generation -- otherwise the next retry's own fresh rewrite
        // (non-deterministic content, a new IndexWriter/addIndexes run) would collide with it as if
        // it were a foreign write, per ObjectStoreCommitPublisher#publishCommit's content-verification
        // guard.
        ShardHead currentHead = shardStateStore.get(TARGET_INDEX_UUID, SHARD_ID).orElseThrow().head();
        assertFalse(
            "this attempt's own orphaned manifest must be cleaned up so a genuine retry doesn't collide with it",
            manifestStore.manifestExists(currentHead.primaryTerm(), currentHead.latestManifestGeneration() + 1)
        );

        // And a genuine retry, now that the collision is cleared, must converge normally.
        assertTrue("a real retry after the ambiguous fault is cleaned up must succeed normally", publisher().rewrite());
    }

    /** Delegates every call to a real {@link ShardStateStore}, but always throws after compareAndSet -- optionally after actually performing it. */
    private static final class CasFaultInjectingShardStateStore implements ShardStateStore {

        private final ShardStateStore delegate;
        private final boolean performRealCasBeforeFaulting;

        CasFaultInjectingShardStateStore(ShardStateStore delegate, boolean performRealCasBeforeFaulting) {
            this.delegate = delegate;
            this.performRealCasBeforeFaulting = performRealCasBeforeFaulting;
        }

        @Override
        public Optional<VersionedShardHead> get(String indexUuid, int shardId) throws java.io.IOException {
            return delegate.get(indexUuid, shardId);
        }

        @Override
        public CasResult compareAndSet(String indexUuid, int shardId, Optional<Long> expectedVersion, ShardHead newHead)
            throws java.io.IOException {
            if (performRealCasBeforeFaulting) {
                delegate.compareAndSet(indexUuid, shardId, expectedVersion, newHead);
            }
            throw new java.io.IOException("injected ambiguous CAS fault");
        }
    }
}
