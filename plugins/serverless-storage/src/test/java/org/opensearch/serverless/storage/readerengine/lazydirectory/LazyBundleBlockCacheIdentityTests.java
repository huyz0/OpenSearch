/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.lazydirectory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

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
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.format.SegmentBundle;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The lazy block cache used to be keyed by Lucene file name alone, resolved inside a persistent,
 * shard-path-derived directory that core's {@code TransferManager} trusts without validating. Both
 * tests here reproduce a way that served bytes belonging to a different file, and both fail against
 * that key.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class LazyBundleBlockCacheIdentityTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "AbCdEfGhIjKlMnOpQrStUv";

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

    private static CommitManifest manifestOf(long generation, String fileName, FileReference ref) {
        return new CommitManifest(
            INDEX_UUID,
            0,
            1,
            generation,
            fileName,
            Map.of(fileName, ref),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            System.currentTimeMillis()
        );
    }

    private static String read(LazyBundleDirectory directory, String name) throws Exception {
        try (IndexInput in = directory.openInput(name, IOContext.DEFAULT)) {
            byte[] bytes = new byte[(int) in.length()];
            in.readBytes(bytes, 0, bytes.length);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    /**
     * The compaction vector. A newer manifest binds {@code segments_1} to different bytes in a
     * different bundle -- exactly what a compaction publishes, since its merged commit restarts at
     * Lucene generation 1 and recycles segment names. With a file-name-only cache key, the block
     * for the new file was already on disk holding the OLD file's bytes, so the reader was handed
     * the pre-compaction commit back for a file it had just re-resolved.
     */
    public void testARebindingFileNameDoesNotServeThePreviousFilesCachedBlocks() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);

        byte[] oldBytes = "AAAAAAAAAAAAAAAAAAAA".getBytes(StandardCharsets.UTF_8);
        byte[] newBytes = "BBBBBBBBBBBBBBBBBBBB".getBytes(StandardCharsets.UTF_8);
        assertEquals("same length, different content -- the case a length check cannot see", oldBytes.length, newBytes.length);

        SegmentBundle oldBundle = bundleStore.writeBundle("bundle-old", List.of(new BundleFileContent("segments_1", oldBytes)));
        SegmentBundle newBundle = bundleStore.writeBundle("bundle-new", List.of(new BundleFileContent("segments_1", newBytes)));

        FileReference oldRef = new FileReference(
            "bundle-old",
            oldBundle.entries().get("segments_1").offset(),
            oldBytes.length,
            oldBundle.entries().get("segments_1").checksum()
        );
        FileReference newRef = new FileReference(
            "bundle-new",
            newBundle.entries().get("segments_1").offset(),
            newBytes.length,
            newBundle.entries().get("segments_1").checksum()
        );

        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        try (MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE)) {
            TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
            try (
                LazyBundleDirectory directory = new LazyBundleDirectory(
                    manifestOf(1, "segments_1", oldRef),
                    cacheDirectory,
                    transferManager
                )
            ) {
                assertEquals("AAAAAAAAAAAAAAAAAAAA", read(directory, "segments_1"));

                directory.advanceToManifest(manifestOf(2, "segments_1", newRef));
                assertEquals(
                    "after a rebind, the new bundle's bytes must be read -- not the previous file's cached block",
                    "BBBBBBBBBBBBBBBBBBBB",
                    read(directory, "segments_1")
                );
            }
        } finally {
            fileCache.clear();
        }
    }

    /**
     * The restart vector, with no compaction involved at all. {@code close()} used to close the
     * FSDirectory and delete nothing, so a reopen of the same shard on the same node found an empty
     * {@code FileCache} but a full on-disk block directory -- and every block read short-circuited
     * on {@code Files.exists}, serving whatever the previous incarnation left there.
     */
    public void testClosingRemovesThisShardsBlockFilesSoARestartCannotServeThem() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);

        byte[] bytes = "cached-block-content".getBytes(StandardCharsets.UTF_8);
        SegmentBundle bundle = bundleStore.writeBundle("bundle-0", List.of(new BundleFileContent("segments_1", bytes)));
        FileReference ref = new FileReference(
            "bundle-0",
            bundle.entries().get("segments_1").offset(),
            bytes.length,
            bundle.entries().get("segments_1").checksum()
        );

        Path blockCachePath = createTempDir().resolve("lazy_directory_cache");
        Files.createDirectories(blockCachePath);
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        try {
            MMapDirectory cacheDirectory = new MMapDirectory(blockCachePath, SimpleFSLockFactory.INSTANCE);
            TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
            LazyBundleDirectory directory = new LazyBundleDirectory(
                manifestOf(1, "segments_1", ref),
                cacheDirectory,
                transferManager,
                fileCache
            );
            assertEquals("cached-block-content", read(directory, "segments_1"));
            try (var blockFiles = Files.list(blockCachePath)) {
                assertTrue("the read must actually have left a block file behind", blockFiles.findAny().isPresent());
            }

            directory.close();

            assertFalse(
                "closing must remove this shard's block cache directory -- leaving it lets the next"
                    + " incarnation of this shard serve the previous one's bytes without validating them",
                Files.exists(blockCachePath)
            );
        } finally {
            fileCache.clear();
        }
    }

    /**
     * The file map used to grow forever, so {@code listAll()} returned every file name the shard had
     * ever published -- an unbounded leak, and (on the eager path's equivalent) part of what made a
     * superseded commit selectable at all. One generation of slack is kept on purpose.
     */
    public void testTheFileMapIsPrunedToTheCurrentAndPreviousGenerations() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);
        SegmentBundle bundle = bundleStore.writeBundle("bundle-0", List.of(new BundleFileContent("f", bytes)));
        FileReference ref = new FileReference(
            "bundle-0",
            bundle.entries().get("f").offset(),
            bytes.length,
            bundle.entries().get("f").checksum()
        );

        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        try (MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE)) {
            TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
            try (
                LazyBundleDirectory directory = new LazyBundleDirectory(manifestOf(1, "segments_1", ref), cacheDirectory, transferManager)
            ) {
                directory.advanceToManifest(manifestOf(2, "segments_2", ref));
                directory.advanceToManifest(manifestOf(3, "segments_3", ref));
                assertEquals(
                    "generation 1's file must be gone once it is two generations superseded",
                    List.of("segments_2", "segments_3"),
                    List.of(directory.listAll())
                );

                directory.advanceToManifest(manifestOf(4, "segments_4", ref));
                assertEquals(List.of("segments_3", "segments_4"), List.of(directory.listAll()));
            }
        } finally {
            fileCache.clear();
        }
    }
}
