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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class LazyBundleDirectoryTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

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

    public void testSearchesCorrectlyWithoutEverMaterializingTheFullManifestToLocalDisk() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, new BlobContainerManifestStore(blobContainer));

        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(writerDirectory, config)) {
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
                INDEX_UUID,
                SHARD_ID,
                1,
                segmentInfos.getGeneration(),
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }

        // A completely separate, otherwise-empty local directory as the block cache's on-disk
        // location -- proof that no full-manifest materialization ever touches it: every byte a
        // real Lucene search needs still comes from here, but only the specific blocks Lucene
        // actually reads, never a whole-file upfront copy the way ObjectStoreCommitMaterializer
        // would have made.
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        try (MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE)) {
            TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
            try (LazyBundleDirectory lazyDirectory = new LazyBundleDirectory(manifest, cacheDirectory, transferManager)) {
                try (DirectoryReader reader = DirectoryReader.open(lazyDirectory)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs hits = searcher.search(new TermQuery(new Term("id", "1")), 10);
                    assertEquals(1, hits.totalHits.value());
                    hits = searcher.search(new TermQuery(new Term("id", "2")), 10);
                    assertEquals(1, hits.totalHits.value());
                    assertEquals(2, reader.numDocs());
                }
            }
        } finally {
            fileCache.clear();
        }
    }

    public void testListAllAndFileLengthMatchTheManifestWithNoFetchAtAll() throws Exception {
        // listAll()/fileLength() must be answerable purely from the manifest's own file map --
        // opening a LazyBundleDirectory and asking what files it has must never itself trigger a
        // fetch, unlike ObjectStoreCommitMaterializer#materialize which fetches every file just to
        // populate a directory listing.
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, new BlobContainerManifestStore(blobContainer));

        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                segmentInfos.getGeneration(),
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }

        // Point the cache directory at a location that would fail loudly if anything ever tried to
        // fetch through it -- listAll/fileLength must not need the TransferManager at all, so a
        // deliberately unusable one (null cache dir would NPE on first real fetch) proves that.
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        try (MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE)) {
            TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
            try (LazyBundleDirectory lazyDirectory = new LazyBundleDirectory(manifest, cacheDirectory, transferManager)) {
                String[] names = lazyDirectory.listAll();
                assertEquals(manifest.files().keySet().size(), names.length);
                for (String name : names) {
                    assertEquals(manifest.files().get(name).length(), lazyDirectory.fileLength(name));
                }
            }
        } finally {
            fileCache.clear();
        }
    }

    public void testWriteOperationsAreRejected() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, new BlobContainerManifestStore(blobContainer));

        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                segmentInfos.getGeneration(),
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }

        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        try (MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE)) {
            TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
            try (LazyBundleDirectory lazyDirectory = new LazyBundleDirectory(manifest, cacheDirectory, transferManager)) {
                expectThrows(java.io.IOException.class, () -> lazyDirectory.createOutput("new-file", null));
                expectThrows(java.io.IOException.class, () -> lazyDirectory.deleteFile(lazyDirectory.listAll()[0]));
                expectThrows(java.io.IOException.class, () -> lazyDirectory.rename(lazyDirectory.listAll()[0], "renamed"));
            }
        } finally {
            fileCache.clear();
        }
    }

    public void testLargeFileIsFetchedInReal1MegabyteBlocksNotOneWholeFileFetch() throws Exception {
        // rfc-serverless-opensearch.md &sect;9's target block granularity is 1 MiB regions, not
        // AbstractBlockIndexInput's own 8 MiB default (tuned for whole snapshot files) --
        // LazyBundleIndexInput.BLOCK_SIZE_SHIFT overrides it. A file spanning several blocks must
        // genuinely produce several distinct block-cache entries, not one fetch for the whole file.
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);

        int oneMebibyte = 1 << 20;
        int fileLength = (int) (2.5 * oneMebibyte); // spans 3 blocks: [0,1MiB), [1MiB,2MiB), [2MiB,2.5MiB)
        byte[] content = new byte[fileLength];
        random().nextBytes(content);
        String fileName = "segments_1";
        String bundleName = "bundle-large-file-test";
        var bundle = bundleStore.writeBundle(bundleName, List.of(new BundleFileContent(fileName, content)));
        var entry = bundle.entries().get(fileName);

        CommitManifest manifest = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            1,
            1,
            fileName,
            Map.of(fileName, new FileReference(bundleName, entry.offset(), entry.length(), entry.checksum())),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            System.currentTimeMillis()
        );

        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        // Not a try-with-resources: LazyBundleDirectory now owns and closes cacheDirectory itself
        // (see its own close() javadoc), so this test must inspect cacheDirectory's contents before
        // lazyDirectory closes, not after -- closing it a second time afterward is still safe
        // (Lucene's own FSDirectory#close is idempotent) but reading from it afterward is not.
        MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE);
        try {
            TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
            long blockCacheEntries;
            try (LazyBundleDirectory lazyDirectory = new LazyBundleDirectory(manifest, cacheDirectory, transferManager)) {
                try (IndexInput input = lazyDirectory.openInput(fileName, null)) {
                    byte[] readBack = new byte[fileLength];
                    input.readBytes(readBack, 0, fileLength);
                    assertArrayEquals("bytes read back through the lazy path must match exactly what was written", content, readBack);
                }
                blockCacheEntries = java.util.Arrays.stream(cacheDirectory.listAll())
                    .filter(AbstractBlockIndexInput::isBlockFilename)
                    .count();
            }
            assertEquals(
                "a 2.5 MiB file at 1 MiB block granularity must produce exactly 3 distinct block-cache entries, not one whole-file fetch",
                3,
                blockCacheEntries
            );
        } finally {
            fileCache.clear();
        }
    }
}
