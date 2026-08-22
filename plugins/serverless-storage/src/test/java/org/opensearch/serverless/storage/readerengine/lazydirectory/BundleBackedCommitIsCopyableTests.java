/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.lazydirectory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The spike that decides whether a real byte-copying snapshot is days of work or a different design.
 *
 * <h2>What is being decided</h2>
 *
 * A serverless index's {@code _snapshot} is a pointer today: a few hundred bytes naming a manifest
 * generation, with the bundles left in the source object store. That is not an independent copy, so it does
 * not survive losing the source bucket, and the design writeup declined a byte-copying variant on the
 * grounds that {@code BlobContainer} has no {@code copyBlob} and no repository plugin uses server-side copy.
 *
 * <p>That objection is about an optimization, not about the operation. If the node moves the bytes, there is
 * a much cheaper design available than the one that was costed: build a {@link LazyBundleDirectory} over the
 * manifest, take its {@link IndexCommit}, and hand the pair to core's existing {@code
 * Repository#snapshotShard}, which takes a {@code Store} and an {@code IndexCommit} <em>as parameters</em>
 * rather than reading them off a live shard. Core then reads each Lucene file -- each read pulling a byte
 * range from the object store -- and writes it into the target repository in the standard format. No new
 * {@code BlobContainer} SPI, no repack format, no new restore path, no temporary disk, and the copy is
 * correctly sized because Lucene-file granularity is what the repository format already wants.
 *
 * <h2>The one assumption that has to hold</h2>
 *
 * <b>That a commit opened over bundles can be read end to end, file by file, the way a snapshot reads it.</b>
 * Everything above rests on it, and nothing else in this plugin exercises it: the lazy directory exists to
 * serve searches, which read the blocks a query touches and never the whole of every file. A snapshot is the
 * opposite access pattern -- every byte of every file in the commit, sequentially, with checksums verified.
 *
 * <p>So this reads exactly that way, and verifies each file's checksum with {@link
 * CodecUtil#checksumEntireFile}, which is the same call {@code BlobStoreRepository} makes while deciding
 * whether a file can be reused or must be uploaded. Passing means the copy is a wiring exercise. Failing
 * would have meant the design above does not exist and the costed one was right after all.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class BundleBackedCommitIsCopyableTests extends OpenSearchTestCase {

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

    public void testEveryFileOfABundleBackedCommitCanBeReadEndToEndAndChecksummed() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, new BlobContainerManifestStore(blobContainer));

        // Several segments rather than one, and a deletion, so the commit is the shape a real snapshot meets:
        // more than one file per kind, and a live docs file that only exists because something was deleted.
        CommitManifest manifest;
        long localBytes = 0;
        List<String> localFiles;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
                    writer.addDocument(doc);
                    if (i % 10 == 0) {
                        writer.commit();
                    }
                }
                writer.deleteDocuments(new org.apache.lucene.index.Term("id", "7"));
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            localFiles = new ArrayList<>(segmentInfos.files(true));
            for (String file : localFiles) {
                localBytes += writerDirectory.fileLength(file);
            }
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

        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        try (MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE)) {
            TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
            try (LazyBundleDirectory lazyDirectory = new LazyBundleDirectory(manifest, cacheDirectory, transferManager)) {

                // 1. The commit, taken the way a snapshot takes it. Nothing here has a live shard, an
                // engine or a local Lucene directory -- only the manifest and the object store, which is
                // what would let this run on a node holding no copy of the index.
                List<IndexCommit> commits = DirectoryReader.listCommits(lazyDirectory);
                assertEquals("the bundle-backed directory must present exactly the published commit", 1, commits.size());
                IndexCommit commit = commits.get(0);

                Collection<String> commitFiles = commit.getFileNames();
                assertFalse("a commit with no files would make every assertion below vacuous", commitFiles.isEmpty());

                // 2. Every file, end to end, with its checksum verified -- the snapshot access pattern, and
                // the opposite of the search pattern this directory was built for.
                long copiedBytes = 0;
                for (String file : commitFiles) {
                    long length = lazyDirectory.fileLength(file);
                    assertTrue("[" + file + "] must have a length", length > 0);
                    try (IndexInput input = lazyDirectory.openInput(file, IOContext.READONCE)) {
                        assertEquals("[" + file + "] must report the length the manifest recorded", length, input.length());
                        if (file.startsWith("segments")) {
                            // Lucene's segments_N carries no per-file codec footer, so it is read whole
                            // rather than checksummed -- which is exactly how the repository treats it.
                            byte[] whole = new byte[Math.toIntExact(length)];
                            input.readBytes(whole, 0, whole.length);
                        } else {
                            // The same call BlobStoreRepository makes when deciding whether a file can be
                            // reused or must be uploaded. It reads every byte and validates the footer, so
                            // a truncated or misaddressed range fails here rather than silently producing a
                            // corrupt snapshot.
                            CodecUtil.checksumEntireFile(input);
                        }
                    }
                    copiedBytes += length;
                }

                assertEquals(
                    "the bytes a copy would move must match what the local commit held -- a smaller total "
                        + "means a file was skipped and a larger one means whole bundles were being read "
                        + "rather than the ranges the commit refers to",
                    localBytes,
                    copiedBytes
                );
                assertEquals(
                    "and it must be the same set of files, so nothing is copied that the commit does not "
                        + "name and nothing the commit names is missed",
                    new java.util.TreeSet<>(localFiles),
                    new java.util.TreeSet<>(commitFiles)
                );
                logger.info("a bundle-backed commit of {} files and {} bytes is readable end to end", commitFiles.size(), copiedBytes);
            }
        } finally {
            fileCache.clear();
        }
    }
}
