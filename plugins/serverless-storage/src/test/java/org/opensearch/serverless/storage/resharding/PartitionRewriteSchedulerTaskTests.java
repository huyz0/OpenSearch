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
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.scheduling.RewriteAdmissionController;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Closes rfc-serverless-opensearch.md &sect;16 Phase 5's own "this increment does not add its own
 * background scheduler for rewrite -- on-demand only" gap: proves {@link
 * PartitionRewriteSchedulerTask} actually drives a real {@link PartitionRewritePublisher#rewrite}
 * on its own schedule, with no on-demand trigger involved, mirroring {@code
 * CompactionSchedulerTask}'s own already-proven "eventually converges" test shape.
 */
public class PartitionRewriteSchedulerTaskTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;
    private static final int DOC_COUNT = 20;

    private BlobContainer blobContainer;
    private BlobContainerBundleStore bundleStore;
    private BlobContainerManifestStore manifestStore;
    private ShardStateStore shardStateStore;
    private BlobContainerShardPartitionStore partitionStore;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        bundleStore = new BlobContainerBundleStore(blobContainer);
        manifestStore = new BlobContainerManifestStore(blobContainer);
        shardStateStore = new BlobContainerShardStateStore(blobContainer);
        partitionStore = new BlobContainerShardPartitionStore(blobContainer);
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private CommitManifest publishCommit() throws Exception {
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
                INDEX_UUID,
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
            shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), new ShardHead(1, null, 0L, manifest.generation()))
        );
        return manifest;
    }

    public void testSchedulerEventuallyRewritesASplitTargetWithNoOnDemandTriggerInvolved() throws Exception {
        publishCommit();
        partitionStore.writeDescriptor(new ShardPartitionDescriptor(0, 3));
        assertTrue("test setup must leave a real descriptor for the scheduler to find", partitionStore.readDescriptor().isPresent());

        PartitionRewritePublisher publisher = new PartitionRewritePublisher(
            INDEX_UUID,
            SHARD_ID,
            shardStateStore,
            manifestStore,
            new ObjectStoreCommitMaterializer(bundleStore),
            new ObjectStoreCommitPublisher(bundleStore, manifestStore),
            partitionStore
        );
        PartitionRewriteSchedulerTask task = new PartitionRewriteSchedulerTask(threadPool, TimeValue.timeValueMillis(10), publisher);
        try {
            assertBusy(() -> {
                assertTrue(
                    "the background scheduler, not any on-demand call, must have cleared the descriptor",
                    partitionStore.readDescriptor().isEmpty()
                );
            });
        } finally {
            task.close();
        }
    }

    public void testRewriteSafelySwallowsAnExceptionRatherThanEscapingTheMethodItself() throws Exception {
        // Matching CompactionSchedulerTaskTests#testScheduledTickItselfSwallowsAnUncheckedExceptionNotJustIOException's
        // own documented finding: core's own scheduler wrapper (Scheduler.ReschedulingRunnable)
        // already tolerates an escaping exception and keeps rescheduling regardless, so asserting
        // end-to-end recovery through a real scheduled run would not actually distinguish a real
        // catch from no catch at all -- confirmed empirically while writing this test (an escaping
        // RuntimeException from rewriteSafely still let the scheduler keep ticking). Calling
        // rewriteSafely() directly (package-private for exactly this reason) isolates this class's
        // own catch behavior from that outer resilience instead.
        //
        // A no-descriptor rewrite() is itself a safe no-op that never even reaches
        // ShardStateStore#get (see PartitionRewritePublisherTests), so a real descriptor is needed
        // to actually exercise the fault-injecting store below.
        partitionStore.writeDescriptor(new ShardPartitionDescriptor(0, 3));
        ShardStateStore faultInjectingShardStateStore = new ShardStateStore() {
            @Override
            public Optional<org.opensearch.serverless.storage.shardstate.VersionedShardHead> get(String indexUuid, int shardId) {
                throw new SecurityException("simulated fault");
            }

            @Override
            public CasResult compareAndSet(
                String indexUuid,
                int shardId,
                Optional<Long> expectedVersion,
                org.opensearch.serverless.storage.shardstate.ShardHead newHead
            ) {
                throw new UnsupportedOperationException();
            }
        };
        PartitionRewritePublisher faultyPublisher = new PartitionRewritePublisher(
            INDEX_UUID,
            SHARD_ID,
            faultInjectingShardStateStore,
            manifestStore,
            new ObjectStoreCommitMaterializer(bundleStore),
            new ObjectStoreCommitPublisher(bundleStore, manifestStore),
            partitionStore
        );
        PartitionRewriteSchedulerTask task = new PartitionRewriteSchedulerTask(
            threadPool,
            TimeValue.timeValueHours(1), // never actually ticks on its own -- invoked directly below
            faultyPublisher
        );
        try {
            // Must not throw -- a SecurityException here means it escaped this task's own catch.
            task.rewriteSafely();
        } finally {
            task.close();
        }
    }

    public void testTickIsSkippedWhenTheNodeWideAdmissionCapIsAlreadyExhausted() throws Exception {
        publishCommit();
        partitionStore.writeDescriptor(new ShardPartitionDescriptor(0, 3));

        PartitionRewritePublisher publisher = new PartitionRewritePublisher(
            INDEX_UUID,
            SHARD_ID,
            shardStateStore,
            manifestStore,
            new ObjectStoreCommitMaterializer(bundleStore),
            new ObjectStoreCommitPublisher(bundleStore, manifestStore),
            partitionStore
        );

        RewriteAdmissionController admissionController = new RewriteAdmissionController(1);
        // Simulates another shard's tick already holding the node's one permit.
        assertTrue(admissionController.tryAcquire());

        PartitionRewriteSchedulerTask task = new PartitionRewriteSchedulerTask(
            threadPool,
            TimeValue.timeValueHours(1), // never actually ticks on its own -- invoked directly below
            publisher,
            admissionController
        );
        try {
            task.rewriteSafely();

            assertTrue(
                "the descriptor must survive untouched -- the tick must have been skipped entirely, not just failed",
                partitionStore.readDescriptor().isPresent()
            );
        } finally {
            task.close();
        }
    }

    public void testTickReleasesItsAdmissionPermitOnceFinishedSoTheNextTickCanProceed() throws Exception {
        publishCommit();
        partitionStore.writeDescriptor(new ShardPartitionDescriptor(0, 3));

        PartitionRewritePublisher publisher = new PartitionRewritePublisher(
            INDEX_UUID,
            SHARD_ID,
            shardStateStore,
            manifestStore,
            new ObjectStoreCommitMaterializer(bundleStore),
            new ObjectStoreCommitPublisher(bundleStore, manifestStore),
            partitionStore
        );

        RewriteAdmissionController admissionController = new RewriteAdmissionController(1);

        PartitionRewriteSchedulerTask task = new PartitionRewriteSchedulerTask(
            threadPool,
            TimeValue.timeValueHours(1), // never actually ticks on its own -- invoked directly below
            publisher,
            admissionController
        );
        try {
            task.rewriteSafely();

            assertTrue(
                "the tick must have run and cleared the descriptor, proving admission was granted",
                partitionStore.readDescriptor().isEmpty()
            );
            assertTrue(
                "the permit acquired for this tick must be released once the tick finishes, freeing it for the next one",
                admissionController.tryAcquire()
            );
        } finally {
            task.close();
        }
    }
}
