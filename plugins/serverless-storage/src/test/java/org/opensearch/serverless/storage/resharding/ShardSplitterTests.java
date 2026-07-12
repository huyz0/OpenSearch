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
import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

public class ShardSplitterTests extends OpenSearchTestCase {

    private static final String SOURCE_INDEX_UUID = "source-idx";
    private static final String TARGET_INDEX_UUID = "target-idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;

    private BlobContainer sourceContainer;
    private BlobContainer targetContainer;
    private BlobContainerManifestStore sourceManifestStore;
    private BlobContainerManifestStore targetManifestStore;
    private BlobContainerBundleStore sourceBundleStore;
    private ShardStateStore sourceShardStateStore;
    private ShardStateStore targetShardStateStore;
    private DurablePinRegistry sourcePinRegistry;
    private BlobContainerCloneLineageStore targetLineageStore;
    private BlobContainerShardPartitionStore targetPartitionStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore sourceBlobStore = new FsBlobStore(1024, createTempDir(), false);
        sourceContainer = new FsBlobContainer(sourceBlobStore, BlobPath.cleanPath(), sourceBlobStore.path());
        FsBlobStore targetBlobStore = new FsBlobStore(1024, createTempDir(), false);
        targetContainer = new FsBlobContainer(targetBlobStore, BlobPath.cleanPath(), targetBlobStore.path());

        sourceManifestStore = new BlobContainerManifestStore(sourceContainer);
        targetManifestStore = new BlobContainerManifestStore(targetContainer);
        sourceBundleStore = new BlobContainerBundleStore(sourceContainer);
        sourceShardStateStore = new BlobContainerShardStateStore(sourceContainer);
        targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        sourcePinRegistry = new BlobContainerDurablePinRegistry(sourceContainer);
        targetLineageStore = new BlobContainerCloneLineageStore(targetContainer);
        targetPartitionStore = new BlobContainerShardPartitionStore(targetContainer);
    }

    private CommitManifest publishSourceCommit() throws Exception {
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(sourceBundleStore, sourceManifestStore);
        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc1 = new Document();
                doc1.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc1);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                SOURCE_INDEX_UUID,
                SHARD_ID,
                PRIMARY_TERM,
                1,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }
        assertEquals(
            CasResult.SUCCESS,
            sourceShardStateStore.compareAndSet(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                Optional.empty(),
                new ShardHead(PRIMARY_TERM, null, 0L, manifest.generation())
            )
        );
        return manifest;
    }

    public void testSplitRefusesAnInvalidPartitionAssignmentBeforeTouchingAnything() throws Exception {
        publishSourceCommit();
        expectThrows(
            IllegalArgumentException.class,
            () -> ShardSplitter.split(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                sourceManifestStore,
                sourceShardStateStore,
                sourcePinRegistry,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetManifestStore,
                targetShardStateStore,
                targetLineageStore,
                targetPartitionStore,
                5,
                2, // partitionIndex 5 >= numPartitions 2 -- invalid
                System.currentTimeMillis()
            )
        );
        assertTrue(
            "an invalid partition assignment must be rejected before any durable write happens -- "
                + "no pin, no target manifest, no target head",
            sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID).isEmpty()
        );
        assertTrue(targetShardStateStore.get(TARGET_INDEX_UUID, SHARD_ID).isEmpty());
    }

    public void testSplitProducesATargetReferencingTheSameBundlesAsTheSourceAndRecordsThePartitionDescriptor() throws Exception {
        CommitManifest sourceManifest = publishSourceCommit();

        ShardSplitter.split(
            SOURCE_INDEX_UUID,
            SHARD_ID,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            TARGET_INDEX_UUID,
            SHARD_ID,
            targetManifestStore,
            targetShardStateStore,
            targetLineageStore,
            targetPartitionStore,
            1,
            3,
            System.currentTimeMillis()
        );

        Optional<org.opensearch.serverless.storage.shardstate.VersionedShardHead> targetHead = targetShardStateStore.get(
            TARGET_INDEX_UUID,
            SHARD_ID
        );
        assertTrue("the split must have published a real target head", targetHead.isPresent());
        CommitManifest targetManifest = targetManifestStore.readManifest(
            targetHead.get().head().primaryTerm(),
            targetHead.get().head().latestManifestGeneration()
        );
        assertEquals(
            "a split target's manifest must reference the exact same bundle files as the source -- zero-copy, no re-materialization",
            sourceManifest.files(),
            targetManifest.files()
        );

        Optional<ShardPartitionDescriptor> descriptor = targetPartitionStore.readDescriptor();
        assertTrue("the target must have a real partition descriptor recorded", descriptor.isPresent());
        assertEquals(1, descriptor.get().partitionIndex());
        assertEquals(3, descriptor.get().numPartitions());
    }

    public void testSplitPinsTheSourceGenerationExactlyLikeAPlainCloneWould() throws Exception {
        CommitManifest sourceManifest = publishSourceCommit();

        ShardSplitter.split(
            SOURCE_INDEX_UUID,
            SHARD_ID,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            TARGET_INDEX_UUID,
            SHARD_ID,
            targetManifestStore,
            targetShardStateStore,
            targetLineageStore,
            targetPartitionStore,
            0,
            2,
            System.currentTimeMillis()
        );

        Set<PinRecord> pins = sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID);
        assertEquals(1, pins.size());
        PinRecord pin = pins.iterator().next();
        assertEquals(sourceManifest.primaryTerm(), pin.primaryTerm());
        assertEquals(sourceManifest.generation(), pin.generation());
    }

    public void testANonSplitTargetHasNoPartitionDescriptor() throws Exception {
        // A plain clone (never routed through ShardSplitter) must not accidentally read a
        // descriptor -- confirms readDescriptor()'s absent-blob path is the default for every
        // ordinary shard, not just an assumption.
        assertTrue(targetPartitionStore.readDescriptor().isEmpty());
    }

    // Regression test for a real bug: the partition descriptor used to be written only *after*
    // ShardCloner.clone returned successfully, i.e. after the head CAS that makes the target
    // shard visible/openable. An engine-open retry landing in the window between that CAS and the
    // descriptor write would cache "no partition filter" for the engine's entire lifetime,
    // silently serving the full pre-split document set. Proven here by making the head CAS fail:
    // if the descriptor is written before activation (the fix), it survives a failed activation;
    // if it were still written after (the bug), a failed activation would leave no descriptor at
    // all -- a directly observable difference between the two orderings.
    public void testPartitionDescriptorIsDurableBeforeActivationEvenIfActivationFails() throws Exception {
        publishSourceCommit();

        ShardStateStore activationFailingStore = new ShardStateStore() {
            @Override
            public Optional<VersionedShardHead> get(String indexUuid, int shardId) throws IOException {
                return targetShardStateStore.get(indexUuid, shardId);
            }

            @Override
            public CasResult compareAndSet(String indexUuid, int shardId, Optional<Long> expectedVersion, ShardHead newHead)
                throws IOException {
                throw new IOException("simulated activation failure");
            }
        };

        expectThrows(
            IOException.class,
            () -> ShardSplitter.split(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                sourceManifestStore,
                sourceShardStateStore,
                sourcePinRegistry,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetManifestStore,
                activationFailingStore,
                targetLineageStore,
                targetPartitionStore,
                1,
                3,
                System.currentTimeMillis()
            )
        );

        assertTrue(
            "the partition descriptor must be durable even when activation (the head CAS) itself "
                + "fails -- it must never depend on activation having already succeeded",
            targetPartitionStore.readDescriptor().isPresent()
        );
        assertTrue(
            "a failed activation must leave no published target head",
            targetShardStateStore.get(TARGET_INDEX_UUID, SHARD_ID).isEmpty()
        );
    }
}
