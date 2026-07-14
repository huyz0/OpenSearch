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
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
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
import org.opensearch.serverless.storage.benchmark.LatencyInjectingBlobContainer;
import org.opensearch.serverless.storage.benchmark.LatencyProfile;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Proves rfc-serverless-opensearch.md &sect;18 risk #2's own "boot-set prefetch" mitigation
 * actually reduces cold-open latency, not merely that the mechanism exists -- {@link
 * LazyBundleDirectory#prefetchBootSet} exists specifically to answer this, and this test is the
 * "no speculative machinery without verification" check every other declined-until-measured item
 * in this section's own status notes insists on.
 *
 * <p>Under simulated {@link LatencyProfile#HIGH} latency, opening a real multi-file {@link
 * DirectoryReader} against a cold {@link LazyBundleDirectory} means Lucene's own {@code
 * SegmentInfos}/segment-open sequence fetches each file's first block <em>serially</em>, one
 * latency-afflicted call at a time. {@link LazyBundleDirectory#prefetchBootSet} fires all of those
 * same first-block fetches <em>concurrently</em> instead, before Lucene ever asks -- this measures
 * whether that reordering (not a reduction in request count; the same fetches happen either way)
 * genuinely shortens wall-clock open time.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class LazyDirectoryBootSetPrefetchBenchmarkTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "boot-set-prefetch-bench-idx";
    private static final int SHARD_ID = 0;
    private static final int TRIALS = 8;
    /** Enough real segment files (each an independent, unmerged commit) that a serial cold open has real serial latency to hide. */
    private static final int SEGMENT_COUNT = 12;

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

    public void testBootSetPrefetchReducesColdOpenLatencyUnderHighSimulatedLatency() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer publishContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        CommitManifest manifest = publishMultiSegmentManifest(publishContainer);
        assertTrue("test setup should produce multiple real segment files", manifest.files().size() > SEGMENT_COUNT);

        long[] withoutPrefetch = new long[TRIALS];
        long[] withPrefetch = new long[TRIALS];
        for (int trial = 0; trial < TRIALS; trial++) {
            withoutPrefetch[trial] = timeColdOpen(blobStore, manifest, false);
            withPrefetch[trial] = timeColdOpen(blobStore, manifest, true);
        }

        Arrays.sort(withoutPrefetch);
        Arrays.sort(withPrefetch);
        long medianWithout = withoutPrefetch[TRIALS / 2];
        long medianWith = withPrefetch[TRIALS / 2];
        logger.info(
            "[boot-set prefetch benchmark] withoutPrefetchMedianMs=[{}] withPrefetchMedianMs=[{}] withoutSamples={} withSamples={}",
            medianWithout,
            medianWith,
            Arrays.toString(withoutPrefetch),
            Arrays.toString(withPrefetch)
        );

        // A strict "<" alone is too weak to reliably catch a broken/no-op prefetch: confirmed the
        // hard way -- a version of this test with only "<" still passed by pure noise once when
        // prefetch was deliberately disabled (4488ms vs 4512ms, an unrelated ~0.5% jitter, not a
        // real effect). Requiring at least half the median instead is easily met by the real
        // effect size this mechanism actually has (~30x observed in practice, serial vs. concurrent
        // first-block fetches across a dozen real segment files) but not by noise alone.
        assertTrue(
            "boot-set prefetch must genuinely, substantially reduce median cold-open latency under HIGH simulated "
                + "latency, not just exist or produce a noise-level difference -- without="
                + medianWithout
                + "ms, with="
                + medianWith
                + "ms",
            medianWith < medianWithout / 2
        );
    }

    /** @return elapsed milliseconds to open a real {@link DirectoryReader} against a fresh, cold {@link LazyBundleDirectory}. */
    private long timeColdOpen(FsBlobStore blobStore, CommitManifest manifest, boolean prefetch) throws Exception {
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainer latencyContainer = new LatencyInjectingBlobContainer(rawContainer, LatencyProfile.HIGH);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(latencyContainer);
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        try (MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE)) {
            TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
            LazyBundleDirectory directory = new LazyBundleDirectory(manifest, cacheDirectory, transferManager);

            long start = System.nanoTime();
            if (prefetch) {
                directory.prefetchBootSet(threadPool.executor(ThreadPool.Names.GENERIC));
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertTrue("sanity check: the materialized commit must actually be searchable", reader.numDocs() > 0);
            }
            return (System.nanoTime() - start) / 1_000_000;
        }
    }

    /** Commits {@link #SEGMENT_COUNT} separate single-document segments (no merge), so a real multi-file cold open results. */
    private static CommitManifest publishMultiSegmentManifest(BlobContainer container) throws Exception {
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(container),
            new BlobContainerManifestStore(container)
        );
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);

        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = null;
            for (int i = 0; i < SEGMENT_COUNT; i++) {
                IndexWriterConfig config = new IndexWriterConfig();
                config.setMergePolicy(NoMergePolicy.INSTANCE);
                try (IndexWriter writer = new IndexWriter(writerDirectory, config)) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "doc-" + i, Field.Store.YES));
                    writer.addDocument(doc);
                    writer.commit();
                }
                segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            }

            CommitManifest manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                1,
                SEGMENT_COUNT,
                SEGMENT_COUNT,
                new WalPosition("bench-epoch", 0),
                0,
                PruningStats.empty()
            );
            CasResult result = shardStateStore.compareAndSet(
                INDEX_UUID,
                SHARD_ID,
                Optional.empty(),
                new ShardHead(1, null, 0L, manifest.generation())
            );
            if (result != CasResult.SUCCESS) {
                throw new IllegalStateException("failed to activate the benchmark shard's head: " + result);
            }
            return manifest;
        }
    }
}
