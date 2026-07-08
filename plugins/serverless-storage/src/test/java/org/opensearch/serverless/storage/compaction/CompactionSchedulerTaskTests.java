/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

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
import org.opensearch.common.unit.TimeValue;
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
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class CompactionSchedulerTaskTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private BlobContainer blobContainer;
    private ObjectStoreCommitPublisher commitPublisher;
    private BlobContainerManifestStore manifestStore;
    private ShardStateStore shardStateStore;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        manifestStore = new BlobContainerManifestStore(blobContainer);
        commitPublisher = new ObjectStoreCommitPublisher(new BlobContainerBundleStore(blobContainer), manifestStore);
        shardStateStore = new BlobContainerShardStateStore(blobContainer);
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private SegmentInfos commitSeparateSegments(Directory directory, int count) throws Exception {
        SegmentInfos infos = null;
        for (int i = 0; i < count; i++) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                Document doc = new Document();
                doc.add(new StringField("id", "doc-" + i, Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            infos = SegmentInfos.readLatestCommit(directory);
        }
        return infos;
    }

    public void testCompactsAQuiescentShardOverTheDefaultSegmentCountThreshold() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            // CompactionPolicy.withDefaults() triggers at 10+ segments.
            SegmentInfos infos = commitSeparateSegments(directory, 12);
            CommitManifest manifest = commitPublisher.publishCommit(
                directory,
                infos,
                INDEX_UUID,
                SHARD_ID,
                1,
                infos.getGeneration(),
                11,
                11,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            // Lease already expired (in the past) -- no active writer, exactly the case this task exists for.
            ShardHead quiescentHead = new ShardHead(1, "node-1", 1L, manifest.generation());
            assertEquals(CasResult.SUCCESS, shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), quiescentHead));

            CompactionSchedulerTask task = new CompactionSchedulerTask(
                threadPool,
                TimeValue.timeValueMillis(10),
                INDEX_UUID,
                SHARD_ID,
                shardStateStore,
                manifestStore,
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
                commitPublisher,
                CompactionPolicy.withDefaults(),
                new CompactionRebaseExecutor(shardStateStore, 10)
            );
            try {
                assertBusy(() -> {
                    ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
                    assertTrue(
                        "a quiescent, over-threshold shard must eventually get compacted",
                        head.latestManifestGeneration() > manifest.generation()
                    );
                });
            } finally {
                task.close();
            }
        }
    }

    public void testDoesNotCompactWhileAnActiveWriterHoldsTheLease() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos infos = commitSeparateSegments(directory, 12);
            CommitManifest manifest = commitPublisher.publishCommit(
                directory,
                infos,
                INDEX_UUID,
                SHARD_ID,
                1,
                infos.getGeneration(),
                11,
                11,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            // Lease held far into the future -- an active writer, whose own local merges are
            // responsible for this shard, not the compaction service.
            ShardHead activeHead = new ShardHead(1, "node-1", Long.MAX_VALUE, manifest.generation());
            assertEquals(CasResult.SUCCESS, shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), activeHead));

            CompactionSchedulerTask task = new CompactionSchedulerTask(
                threadPool,
                TimeValue.timeValueMillis(10),
                INDEX_UUID,
                SHARD_ID,
                shardStateStore,
                manifestStore,
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
                commitPublisher,
                CompactionPolicy.withDefaults(),
                new CompactionRebaseExecutor(shardStateStore, 10)
            );
            try {
                // Give it several ticks' worth of time to (wrongly) fire before asserting it never did.
                Thread.sleep(200);
                ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
                assertEquals(
                    "a shard with an actively-held lease must never be compacted by this task",
                    manifest.generation(),
                    head.latestManifestGeneration()
                );
            } finally {
                task.close();
            }
        }
    }

    public void testDoesNothingForAShardThatHasNeverBeenActivated() throws Exception {
        CompactionSchedulerTask task = new CompactionSchedulerTask(
            threadPool,
            TimeValue.timeValueMillis(10),
            INDEX_UUID,
            SHARD_ID,
            shardStateStore,
            manifestStore,
            new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
            commitPublisher,
            CompactionPolicy.withDefaults(),
            new CompactionRebaseExecutor(shardStateStore, 10)
        );
        try {
            Thread.sleep(100);
            assertTrue(
                "no head should ever be created for a shard that was never activated",
                shardStateStore.get(INDEX_UUID, SHARD_ID).isEmpty()
            );
        } finally {
            task.close();
        }
    }
}
