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
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * rfc-serverless-opensearch.md &sect;18 risk #8's own "vector/kNN workloads... nearly the worst
 * case for a 1&nbsp;MiB-block LRU cache" concern, actually measured for the first time -- every
 * other item in that risk section that got a "declined until measured" resolution earned it by
 * real benchmark evidence (see {@code LazyDirectoryBootSetPrefetchBenchmarkTests}); this concern
 * had none until now.
 *
 * <p>Compares real cold-query latency between a k-NN vector search and an ordinary term search
 * against the exact same {@link LazyBundleDirectory} setup (same block size, same simulated
 * latency, same file cache), with and without boot-set prefetch -- the two variables risk #8's own
 * text names as the open questions: does cold k-NN cost more than cold term search under this
 * cache design, and does boot-set prefetch (first-block-of-every-file only) help it the way it
 * helps a sequential segment open, or does HNSW graph traversal's random-access pattern defeat it.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class VectorWorkloadColdQueryBenchmarkTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "vector-bench-idx";
    private static final int SHARD_ID = 0;
    private static final int TRIALS = 5;
    private static final int VECTOR_DIMENSION = 128;
    /** Large enough that the raw vector data alone (~{@code VECTOR_COUNT * VECTOR_DIMENSION * 4} bytes) spans several real 1 MiB {@link LazyBundleIndexInput} blocks, not just one. */
    private static final int VECTOR_COUNT = 6000;

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

    public void testColdKnnQueryVsColdTermQueryUnderHighSimulatedLatency() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer publishContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        CommitManifest manifest = publishVectorAndTextManifest(publishContainer);
        logger.info("[vector workload benchmark] manifest files: {}", manifest.files().keySet());
        long vectorFileBytes = manifest.files()
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().endsWith(".vec"))
            .mapToLong(entry -> entry.getValue().length())
            .sum();
        logger.info("[vector workload benchmark] total .vec file bytes={} (block size=1 MiB)", vectorFileBytes);
        assertTrue(
            "test setup must produce a real multi-block vector file, or this benchmark isn't exercising the "
                + "thing risk #8 is actually about -- got "
                + vectorFileBytes
                + " bytes",
            vectorFileBytes > 2L * 1024 * 1024
        );

        long[] termNoPrefetch = new long[TRIALS];
        long[] termWithPrefetch = new long[TRIALS];
        long[] knnNoPrefetch = new long[TRIALS];
        long[] knnWithPrefetch = new long[TRIALS];
        for (int trial = 0; trial < TRIALS; trial++) {
            termNoPrefetch[trial] = timeColdQuery(blobStore, manifest, false, false);
            termWithPrefetch[trial] = timeColdQuery(blobStore, manifest, false, true);
            knnNoPrefetch[trial] = timeColdQuery(blobStore, manifest, true, false);
            knnWithPrefetch[trial] = timeColdQuery(blobStore, manifest, true, true);
        }

        long termNoPrefetchMedian = median(termNoPrefetch);
        long termWithPrefetchMedian = median(termWithPrefetch);
        long knnNoPrefetchMedian = median(knnNoPrefetch);
        long knnWithPrefetchMedian = median(knnWithPrefetch);

        logger.info(
            "[vector workload benchmark] term: noPrefetchMedianMs={} withPrefetchMedianMs={} | "
                + "knn: noPrefetchMedianMs={} withPrefetchMedianMs={} | termSamples={} knnSamples={}",
            termNoPrefetchMedian,
            termWithPrefetchMedian,
            knnNoPrefetchMedian,
            knnWithPrefetchMedian,
            Arrays.toString(termNoPrefetch),
            Arrays.toString(knnNoPrefetch)
        );

        // The real, load-bearing question this benchmark exists to answer: is a cold k-NN query
        // measurably more expensive than a cold term query against the same cache/latency setup.
        // Not asserting a specific multiplier -- this is a real measurement, not a target to hit --
        // just that the comparison itself is real and in the direction risk #8 predicted.
        assertTrue(
            "a cold k-NN query is expected to cost at least as much as a cold term query against the same "
                + "1 MiB-block cache under simulated latency (risk #8's own prediction) -- term="
                + termNoPrefetchMedian
                + "ms, knn="
                + knnNoPrefetchMedian
                + "ms",
            knnNoPrefetchMedian >= termNoPrefetchMedian
        );
    }

    private static long median(long[] samples) {
        long[] sorted = samples.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    /** @return elapsed milliseconds for a single cold query (k-NN or term) against a fresh {@link LazyBundleDirectory}. */
    private long timeColdQuery(FsBlobStore blobStore, CommitManifest manifest, boolean knn, boolean prefetch) throws Exception {
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
                IndexSearcher searcher = new IndexSearcher(reader);
                if (knn) {
                    float[] queryVector = randomVector(new Random(42));
                    TopDocs topDocs = searcher.search(new KnnFloatVectorQuery("vector", queryVector, 10), 10);
                    assertTrue("sanity check: the k-NN query must return real hits", topDocs.scoreDocs.length > 0);
                } else {
                    TopDocs topDocs = searcher.search(new TermQuery(new org.apache.lucene.index.Term("body", "shared")), 10);
                    assertTrue("sanity check: the term query must return real hits", topDocs.scoreDocs.length > 0);
                }
            }
            return (System.nanoTime() - start) / 1_000_000;
        }
    }

    /** Commits one segment of {@link #VECTOR_COUNT} documents, each carrying both a real k-NN vector field and a term-searchable text field, over the same file layout. */
    private static CommitManifest publishVectorAndTextManifest(BlobContainer container) throws Exception {
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(container),
            new BlobContainerManifestStore(container)
        );
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);

        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setMergePolicy(NoMergePolicy.INSTANCE);
            // Without this, a segment this small gets wrapped into one compound .cfs/.cfe blob,
            // hiding the individual .vec/.vex files this benchmark needs to see and size
            // individually -- matching how a real, larger production segment would actually be
            // stored (compound format is a small-segment optimization core itself skips past a
            // size threshold).
            config.setUseCompoundFile(false);
            Random random = new Random(1234);
            try (IndexWriter writer = new IndexWriter(writerDirectory, config)) {
                for (int i = 0; i < VECTOR_COUNT; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "doc-" + i, Field.Store.YES));
                    doc.add(new TextField("body", "shared common terms for every document plus doc " + i, Field.Store.NO));
                    doc.add(new KnnFloatVectorField("vector", randomVector(random), VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);

            CommitManifest manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                1,
                VECTOR_COUNT,
                VECTOR_COUNT,
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

    private static float[] randomVector(Random random) {
        float[] vector = new float[VECTOR_DIMENSION];
        for (int i = 0; i < VECTOR_DIMENSION; i++) {
            vector[i] = random.nextFloat();
        }
        return vector;
    }
}
