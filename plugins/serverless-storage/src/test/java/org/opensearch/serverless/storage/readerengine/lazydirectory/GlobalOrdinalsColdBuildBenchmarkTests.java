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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.util.BytesRef;
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
 * rfc-serverless-opensearch.md &sect;18 risk #9's own "aggregation-heavy heap costs on readers...
 * building global ordinals on a cache-miss storm is a latency cliff" concern, actually measured for
 * the first time -- same "no speculative machinery without verification" discipline
 * {@code LazyDirectoryBootSetPrefetchBenchmarkTests} and {@code VectorWorkloadColdQueryBenchmarkTests}
 * already applied to their own risk items.
 *
 * <p>"Global ordinals" (the term risk #9 uses, matching OpenSearch/Elasticsearch's own terms-/
 * cardinality-aggregation terminology) is, at the Lucene level this plugin's reader engine actually
 * sits on, {@link org.apache.lucene.index.OrdinalMap} construction: merging every segment's own
 * per-segment {@link SortedSetDocValues} term ordinals into one cross-segment numbering, done once
 * per field per reader open by {@link MultiDocValues#getSortedSetValues}. This is a real, direct
 * proxy for the cost risk #9 is actually about -- the full OpenSearch aggregation framework
 * ({@code GlobalOrdinalsStringTermsAggregator} et al.) is a thin layer building on top of exactly
 * this Lucene primitive, not a different cost source.
 *
 * <p>Multiple real, unmerged segments (not one) is deliberate: {@code OrdinalMap} construction is a
 * cross-segment merge, so a single-segment reader would trivially skip the actual merge cost this
 * benchmark exists to measure.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class GlobalOrdinalsColdBuildBenchmarkTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "ordinals-bench-idx";
    private static final int SHARD_ID = 0;
    private static final int TRIALS = 5;
    private static final int SEGMENT_COUNT = 8;
    private static final int DOCS_PER_SEGMENT = 4000;
    /** High cardinality: every document gets its own distinct term, the real worst case for ordinal-map merge cost (no term-dedup shortcut across segments). */
    private static final String FIELD = "category";

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

    public void testColdGlobalOrdinalBuildUnderHighSimulatedLatency() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer publishContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        CommitManifest manifest = publishMultiSegmentManifest(publishContainer);
        assertTrue(
            "test setup should produce multiple real segments for the ordinal merge to actually do work across",
            SegmentInfosCountHelper.segmentCount(manifest) >= SEGMENT_COUNT
        );

        long[] noPrefetch = new long[TRIALS];
        long[] withPrefetch = new long[TRIALS];
        for (int trial = 0; trial < TRIALS; trial++) {
            noPrefetch[trial] = timeColdOrdinalBuild(blobStore, manifest, false);
            withPrefetch[trial] = timeColdOrdinalBuild(blobStore, manifest, true);
        }

        long medianNoPrefetch = median(noPrefetch);
        long medianWithPrefetch = median(withPrefetch);
        logger.info(
            "[global ordinals cold-build benchmark] noPrefetchMedianMs={} withPrefetchMedianMs={} "
                + "noPrefetchSamples={} withPrefetchSamples={} segments={} docsPerSegment={}",
            medianNoPrefetch,
            medianWithPrefetch,
            Arrays.toString(noPrefetch),
            Arrays.toString(withPrefetch),
            SEGMENT_COUNT,
            DOCS_PER_SEGMENT
        );

        // The real, load-bearing question: does a cold, cache-miss-storm ordinal-map build under
        // HIGH simulated latency actually cost real, measurable time (risk #9's "latency cliff"
        // claim), not whether it's fast in absolute terms -- this is a simulated-latency benchmark,
        // not a production SLO. A build that costs near-zero regardless of latency would mean this
        // risk's premise doesn't hold for this implementation; one that scales with latency confirms it.
        assertTrue(
            "cold global-ordinal construction under HIGH simulated latency must show real, non-trivial "
                + "cost -- got "
                + medianNoPrefetch
                + "ms, expected meaningfully more than a warm/no-latency build would cost",
            medianNoPrefetch > 200
        );
    }

    private static long median(long[] samples) {
        long[] sorted = samples.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    /** @return elapsed milliseconds to open a fresh {@link LazyBundleDirectory} and build the cross-segment ordinal map for {@link #FIELD}. */
    private long timeColdOrdinalBuild(FsBlobStore blobStore, CommitManifest manifest, boolean prefetch) throws Exception {
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
                SortedSetDocValues ordinals = MultiDocValues.getSortedSetValues(reader, FIELD);
                assertNotNull("sanity check: the field must actually have doc values to build ordinals from", ordinals);
                // Force real materialization, not just construction of a lazy wrapper: walk every
                // live doc's ordinals, the same access pattern a real terms/cardinality aggregation
                // performs once the ordinal map itself is built.
                long touched = 0;
                for (int doc = ordinals.nextDoc(); doc != SortedSetDocValues.NO_MORE_DOCS; doc = ordinals.nextDoc()) {
                    for (int i = 0; i < ordinals.docValueCount(); i++) {
                        BytesRef term = ordinals.lookupOrd(ordinals.nextOrd());
                        touched += term.length;
                    }
                }
                assertTrue("sanity check: real terms must have been touched", touched > 0);
            }
            return (System.nanoTime() - start) / 1_000_000;
        }
    }

    /** Commits {@link #SEGMENT_COUNT} separate high-cardinality segments (no merge), so a real cross-segment ordinal-map merge is exercised. */
    private static CommitManifest publishMultiSegmentManifest(BlobContainer container) throws Exception {
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(container),
            new BlobContainerManifestStore(container)
        );
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);

        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = null;
            int globalDocIndex = 0;
            for (int segment = 0; segment < SEGMENT_COUNT; segment++) {
                IndexWriterConfig config = new IndexWriterConfig();
                config.setMergePolicy(NoMergePolicy.INSTANCE);
                config.setUseCompoundFile(false);
                try (IndexWriter writer = new IndexWriter(writerDirectory, config)) {
                    for (int i = 0; i < DOCS_PER_SEGMENT; i++, globalDocIndex++) {
                        Document doc = new Document();
                        doc.add(new StringField("id", "doc-" + globalDocIndex, Field.Store.YES));
                        doc.add(new SortedSetDocValuesField(FIELD, new BytesRef("category-" + globalDocIndex)));
                        writer.addDocument(doc);
                    }
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
                SEGMENT_COUNT * DOCS_PER_SEGMENT,
                SEGMENT_COUNT * DOCS_PER_SEGMENT,
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

    /** Tiny local helper so the setup assertion doesn't need to open a reader just to count segments. */
    private static final class SegmentInfosCountHelper {
        static int segmentCount(CommitManifest manifest) {
            return (int) manifest.files().keySet().stream().filter(name -> name.endsWith(".si")).count();
        }
    }
}
