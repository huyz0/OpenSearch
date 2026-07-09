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

    public void testCompactsAFragmentedShardEvenWhileAnActiveWriterHoldsTheLease() throws Exception {
        // The "busy writer accepts an offload handoff" case (rfc-serverless-opensearch.md &sect;16
        // Phase 4.5): the publish protocol both a writer and this task use always recomputes the
        // target generation live from the current head inside a CAS-retry loop, so there is nothing
        // unsafe about this task and an active writer both touching the same shard -- lease presence
        // is no longer a reason to skip, only CompactionPolicy#shouldCompact's own thresholds are.
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
            // Lease held far into the future -- an active writer -- unlike the old version of this
            // test, this must no longer prevent compaction from running.
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
                assertBusy(() -> {
                    ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
                    assertTrue(
                        "an over-threshold shard must eventually get compacted even with an active writer's lease held",
                        head.latestManifestGeneration() > manifest.generation()
                    );
                    // The lease itself (this test's simulated writer holding it) must be left
                    // completely untouched -- compaction publishing a new generation never disturbs
                    // who holds the lease, only ShardHead#latestManifestGeneration.
                    assertEquals("node-1", head.leaseHolderNodeId());
                });
            } finally {
                task.close();
            }
        }
    }

    public void testMaybeCompactReturnsTrueWhenTheShardIsACompactionCandidate() throws Exception {
        // Direct coverage of the static maybeCompact(...) method itself -- factored out of the
        // instance so both this task's own recurring schedule and the on-demand compaction trigger
        // action share exactly one implementation. The scheduled-task tests above only observe the
        // eventual side effect (a newer generation gets published); this test observes the method's
        // own return-value contract directly, with no scheduler or timing involved.
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
            ShardHead head = new ShardHead(1, "node-1", 1L, manifest.generation());
            assertEquals(CasResult.SUCCESS, shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), head));

            boolean attempted = CompactionSchedulerTask.maybeCompact(
                INDEX_UUID,
                SHARD_ID,
                shardStateStore,
                manifestStore,
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
                commitPublisher,
                CompactionPolicy.withDefaults(),
                new CompactionRebaseExecutor(shardStateStore, 10)
            );

            assertTrue("a 12-segment shard is over the default 10-segment threshold and must be a candidate", attempted);
            ShardHead afterCompaction = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertTrue(
                "the attempt reported as made must have actually published a newer generation",
                afterCompaction.latestManifestGeneration() > manifest.generation()
            );
        }
    }

    public void testMaybeCompactReturnsFalseWhenTheShardIsUnderThreshold() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            // Well under CompactionPolicy.withDefaults()'s 10-segment threshold.
            SegmentInfos infos = commitSeparateSegments(directory, 2);
            CommitManifest manifest = commitPublisher.publishCommit(
                directory,
                infos,
                INDEX_UUID,
                SHARD_ID,
                1,
                infos.getGeneration(),
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            ShardHead head = new ShardHead(1, "node-1", 1L, manifest.generation());
            assertEquals(CasResult.SUCCESS, shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), head));

            boolean attempted = CompactionSchedulerTask.maybeCompact(
                INDEX_UUID,
                SHARD_ID,
                shardStateStore,
                manifestStore,
                new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
                commitPublisher,
                CompactionPolicy.withDefaults(),
                new CompactionRebaseExecutor(shardStateStore, 10)
            );

            assertFalse("a 2-segment shard is well under threshold and must not be a candidate", attempted);
            ShardHead afterCall = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(
                "no publish attempt must have been made, so the generation must be unchanged",
                manifest.generation(),
                afterCall.latestManifestGeneration()
            );
        }
    }

    public void testMaybeCompactReturnsFalseForAShardThatHasNeverBeenActivated() throws Exception {
        boolean attempted = CompactionSchedulerTask.maybeCompact(
            INDEX_UUID,
            SHARD_ID,
            shardStateStore,
            manifestStore,
            new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
            commitPublisher,
            CompactionPolicy.withDefaults(),
            new CompactionRebaseExecutor(shardStateStore, 10)
        );
        assertFalse("a shard with no published head at all must be a safe no-op", attempted);
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
