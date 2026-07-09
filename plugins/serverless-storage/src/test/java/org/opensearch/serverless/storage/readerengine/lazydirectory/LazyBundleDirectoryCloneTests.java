/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.lazydirectory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.clone.CloneLineage;
import org.opensearch.serverless.storage.clone.FallbackStreamReader;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Proves {@link ServerlessStorageLazyDirectoryFactory}'s clone-aware {@code resolveStreamReader}
 * actually works end to end: a real {@link LazyBundleDirectory} opened over a cloned shard's
 * manifest, backed by a {@link FallbackStreamReader}-wrapped {@link TransferManager}, genuinely
 * fetches and searches bundle bytes it never copied -- not just the directory-level unit proof
 * {@code LazyBundleDirectoryTests} already gives for the non-cloned case.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class LazyBundleDirectoryCloneTests extends OpenSearchTestCase {

    private static final String SOURCE_INDEX_UUID = "source-idx";
    private static final String TARGET_INDEX_UUID = "target-idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testClonedShardsLazyDirectoryFetchesAndSearchesSourceBundlesItNeverCopied() throws Exception {
        FsBlobStore sourceBlobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer sourceContainer = new FsBlobContainer(sourceBlobStore, BlobPath.cleanPath(), sourceBlobStore.path());
        FsBlobStore targetBlobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer targetContainer = new FsBlobContainer(targetBlobStore, BlobPath.cleanPath(), targetBlobStore.path());

        BlobContainerBundleStore sourceBundleStore = new BlobContainerBundleStore(sourceContainer);
        BlobContainerManifestStore sourceManifestStore = new BlobContainerManifestStore(sourceContainer);
        ShardStateStore sourceShardStateStore = new BlobContainerShardStateStore(sourceContainer);
        DurablePinRegistry sourcePinRegistry = new BlobContainerDurablePinRegistry(sourceContainer);

        BlobContainerBundleStore targetBundleStore = new BlobContainerBundleStore(targetContainer);
        BlobContainerManifestStore targetManifestStore = new BlobContainerManifestStore(targetContainer);
        ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        BlobContainerCloneLineageStore targetLineageStore = new BlobContainerCloneLineageStore(targetContainer);

        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(sourceBundleStore, sourceManifestStore);
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc1 = new Document();
                doc1.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc1);
                Document doc2 = new Document();
                doc2.add(new StringField("id", "2", Field.Store.YES));
                writer.addDocument(doc2);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            CommitManifest sourceManifest = publisher.publishCommit(
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
            assertEquals(
                CasResult.SUCCESS,
                sourceShardStateStore.compareAndSet(
                    SOURCE_INDEX_UUID,
                    SHARD_ID,
                    Optional.empty(),
                    new ShardHead(PRIMARY_TERM, null, 0L, sourceManifest.generation())
                )
            );
        }

        ShardCloner.clone(
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
            System.currentTimeMillis()
        );
        assertTrue(
            "clone must not have copied any bundle bytes into the target's own container",
            targetBundleStore.listBundleNames().isEmpty()
        );

        Optional<VersionedShardHead> targetHead = targetShardStateStore.get(TARGET_INDEX_UUID, SHARD_ID);
        assertTrue(targetHead.isPresent());
        CommitManifest targetManifest = targetManifestStore.readManifest(
            targetHead.get().head().primaryTerm(),
            targetHead.get().head().latestManifestGeneration()
        );

        // Exactly the resolution ServerlessStorageLazyDirectoryFactory#resolveStreamReader performs:
        // read the target's own lineage, and if present, wrap its StreamReader with a fallback to
        // the source's own bundle store.
        Optional<CloneLineage> lineage = targetLineageStore.readLineage();
        assertTrue(lineage.isPresent());
        assertEquals(SOURCE_INDEX_UUID, lineage.get().sourceIndexUuid());
        TransferManager.StreamReader streamReader = new FallbackStreamReader(targetBundleStore::openRange, sourceBundleStore::openRange);

        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE);
        try {
            TransferManager transferManager = new TransferManager(streamReader, fileCache, threadPool);
            try (LazyBundleDirectory lazyDirectory = new LazyBundleDirectory(targetManifest, cacheDirectory, transferManager)) {
                try (DirectoryReader reader = DirectoryReader.open(lazyDirectory)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs hits = searcher.search(new TermQuery(new Term("id", "1")), 10);
                    assertEquals(1, hits.totalHits.value());
                    hits = searcher.search(new TermQuery(new Term("id", "2")), 10);
                    assertEquals(1, hits.totalHits.value());
                }
            }
        } finally {
            fileCache.clear();
        }
    }
}
