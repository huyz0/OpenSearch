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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
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
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.readerengine.ObjectStoreReaderEngine;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

import java.util.Optional;

/**
 * Proves {@code ObjectStoreReaderEngine} genuinely works end to end against a real {@link
 * LazyBundleDirectory}-backed {@link Store} -- i.e. the same shape of {@code EngineConfig}/{@code
 * Store} that {@code ServerlessStorageLazyDirectoryFactory} would hand core in a real cluster, not
 * just the standalone directory-level proof {@code LazyBundleDirectoryTests} already gives.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class LazyDirectoryReaderEngineTests extends EngineTestCase {

    private static final long PRIMARY_TERM = 1;
    private static final String LOCAL_NODE_ID = "test-node";

    /** A {@link BundleFileReader} that fails loudly if ever invoked -- proof the lazy path never falls back to eager fetch. */
    private static final class ExplodingBundleFileReader implements BundleFileReader {
        @Override
        public byte[] readFile(String bundleName, BundleFileEntry entry) {
            throw new AssertionError("materializer must never be invoked when the engine's directory is already a LazyBundleDirectory");
        }
    }

    public void testReaderEngineSearchesCorrectlyOverARealLazyBundleDirectoryBackedStoreWithoutEverMaterializing() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ShardDirectory shardDirectory = new InMemoryShardDirectory();

        CommitManifest manifest;
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
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                "idx",
                0,
                PRIMARY_TERM,
                1,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }

        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        try (MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE)) {
            TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
            // Exactly what ServerlessStorageLazyDirectoryFactory#newDirectory would have built for a
            // real search-only shard copy -- the same LazyBundleDirectory, wrapped in a real Store the
            // same way core's own IndexService#createShard wraps whatever a DirectoryFactory returns.
            try (LazyBundleDirectory lazyDirectory = new LazyBundleDirectory(manifest, cacheDirectory, transferManager)) {
                try (Store store = createStore(lazyDirectory)) {
                    EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
                    String indexUuid = engineConfig.getShardId().getIndex().getUUID();
                    int shardId = engineConfig.getShardId().getId();

                    assertEquals(
                        CasResult.SUCCESS,
                        shardStateStore.compareAndSet(
                            indexUuid,
                            shardId,
                            Optional.empty(),
                            new ShardHead(PRIMARY_TERM, null, 0L, manifest.generation())
                        )
                    );

                    // The exploding materializer proves applyManifestToDirectory's LazyBundleDirectory
                    // branch is what actually ran -- if the engine ever fell back to the eager path,
                    // this test fails loudly instead of silently passing for the wrong reason.
                    try (
                        ObjectStoreReaderEngine readerEngine = ObjectStoreReaderEngine.open(
                            engineConfig,
                            manifest,
                            new ObjectStoreCommitMaterializer(new ExplodingBundleFileReader()),
                            PRIMARY_TERM,
                            shardStateStore,
                            manifestStore,
                            shardDirectory,
                            LOCAL_NODE_ID
                        )
                    ) {
                        try (Engine.Searcher searcher = readerEngine.acquireSearcher("test")) {
                            TopDocs hits = searcher.search(new TermQuery(new Term("id", "1")), 10);
                            assertEquals(1, hits.totalHits.value());
                            hits = searcher.search(new TermQuery(new Term("id", "2")), 10);
                            assertEquals(1, hits.totalHits.value());
                        }
                    }
                }
            }
        } finally {
            fileCache.clear();
        }
    }
}
