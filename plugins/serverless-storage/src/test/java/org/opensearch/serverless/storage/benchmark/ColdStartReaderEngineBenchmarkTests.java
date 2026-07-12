/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.benchmark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
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

import java.util.Arrays;
import java.util.Optional;

/**
 * Times a real reader-shard "cold start" -- resolve the shard's published head, read its manifest,
 * open an {@link ObjectStoreReaderEngine} against it, and acquire a searcher, the same sequence
 * {@code ReaderEngineFactory#newReadWriteEngine} actually runs -- under each {@link LatencyProfile}
 * plus a no-injected-latency baseline, computing p50/p95/p99 across many trials.
 *
 * <p>Answers rfc-serverless-opensearch.md &sect;16 Phase 4's own "not yet benchmarked" status note
 * for the "first query after idle returns &lt; 5s p95 for a cached-manifest index" milestone: this
 * gives that milestone a real, repeatable percentile measurement instead of none at all. Same
 * "simulated latency, not a real cloud connection" scope decision as {@link
 * BlobLatencyBenchmarkTests}, and the same honesty about what the resulting numbers mean: this
 * sandbox is not representative production hardware/network, so the logged percentiles are for
 * regression-tracking (did this change get slower relative to itself?) across runs in this repo,
 * not a claim about real-world cold-start latency -- no pass/fail threshold is asserted here for
 * that reason.
 */
public class ColdStartReaderEngineBenchmarkTests extends EngineTestCase {

    private static final Logger logger = LogManager.getLogger(ColdStartReaderEngineBenchmarkTests.class);
    private static final String INDEX_UUID = "cold-start-bench-idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;
    private static final String LOCAL_NODE_ID = "bench-node";
    private static final int TRIALS = 15;
    /** A realistic-ish document count for a shard that's been running a while, not the 5-file toy bundles {@link BlobLatencyBenchmarkTests} uses. */
    private static final int DOCUMENT_COUNT = 30;

    public void testColdStartLatencyPercentilesAcrossLatencyProfiles() throws Exception {
        long[] none = timeColdStarts(null, "NONE (plain FsBlobContainer)");
        long[] low = timeColdStarts(LatencyProfile.LOW, "LOW");
        long[] typical = timeColdStarts(LatencyProfile.TYPICAL, "TYPICAL");
        long[] high = timeColdStarts(LatencyProfile.HIGH, "HIGH");

        // Only ordinal relationships are asserted, matching BlobLatencyBenchmarkTests' own
        // reasoning: wall-clock numbers on shared CI hardware are inherently noisy, so this is a
        // sanity check that the harness itself behaves as designed (a slower profile must not
        // measure faster), not a claim about absolute latency.
        assertTrue("LOW p50 should be at least as slow as the no-latency baseline's p50", percentile(low, 50) >= percentile(none, 50));
        assertTrue("TYPICAL p50 should be at least as slow as LOW's p50", percentile(typical, 50) >= percentile(low, 50));
        assertTrue("HIGH p50 should be at least as slow as TYPICAL's p50", percentile(high, 50) >= percentile(typical, 50));
    }

    /** @return sorted elapsed-millis samples across {@link #TRIALS} independent cold starts, published once and re-opened fresh each trial. */
    private long[] timeColdStarts(LatencyProfile profile, String label) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024 * 1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainer publishContainer = profile == null ? rawContainer : new LatencyInjectingBlobContainer(rawContainer, profile);

        CommitManifest manifest = publishRealisticManifest(publishContainer);

        long[] samples = new long[TRIALS];
        for (int trial = 0; trial < TRIALS; trial++) {
            // A fresh BlobContainer wrapper per trial (same underlying FsBlobStore, so the already-
            // published bundle/manifest/head are still there) -- each trial re-injects its own
            // independent random per-call latency, matching a genuinely new reader activation
            // rather than reusing warmed-up state.
            BlobContainer readContainer = profile == null ? rawContainer : new LatencyInjectingBlobContainer(rawContainer, profile);
            ShardStateStore shardStateStore = new BlobContainerShardStateStore(readContainer);
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(readContainer);
            ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(readContainer));

            try (Store store = createStore()) {
                EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
                ShardDirectory shardDirectory = new InMemoryShardDirectory();

                long start = System.nanoTime();
                // The real ReaderEngineFactory#newReadWriteEngine sequence: resolve the head, read
                // its manifest, then open the engine and get a searcher -- not just the open() call
                // in isolation, so this reflects the same work a real cold reader activation does.
                var head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
                CommitManifest resolvedManifest = manifestStore.readManifest(head.primaryTerm(), head.latestManifestGeneration());
                try (
                    ObjectStoreReaderEngine readerEngine = ObjectStoreReaderEngine.open(
                        engineConfig,
                        resolvedManifest,
                        materializer,
                        PRIMARY_TERM,
                        shardStateStore,
                        manifestStore,
                        shardDirectory,
                        LOCAL_NODE_ID
                    )
                ) {
                    try (Engine.Searcher searcher = readerEngine.acquireSearcher("cold-start-benchmark")) {
                        TopDocs hits = searcher.search(new TermQuery(new Term("id", "0")), 1);
                        assert hits.totalHits.value() == 1 : "sanity check: the materialized commit must actually be searchable";
                    }
                }
                samples[trial] = (System.nanoTime() - start) / 1_000_000;
            }
        }
        Arrays.sort(samples);
        logger.info(
            "[cold-start benchmark] profile=[{}] trials=[{}] p50Ms=[{}] p95Ms=[{}] p99Ms=[{}] minMs=[{}] maxMs=[{}]",
            label,
            TRIALS,
            percentile(samples, 50),
            percentile(samples, 95),
            percentile(samples, 99),
            samples[0],
            samples[samples.length - 1]
        );
        return samples;
    }

    /** @param sortedSamples ascending-sorted samples. @param p the percentile to compute, in [0, 100]. */
    private static long percentile(long[] sortedSamples, int p) {
        int index = (int) Math.ceil(p / 100.0 * sortedSamples.length) - 1;
        return sortedSamples[Math.max(0, Math.min(index, sortedSamples.length - 1))];
    }

    private static CommitManifest publishRealisticManifest(BlobContainer container) throws Exception {
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(container),
            new BlobContainerManifestStore(container)
        );
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);

        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(writerDirectory, config)) {
                for (int i = 0; i < DOCUMENT_COUNT; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
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
                PRIMARY_TERM,
                1,
                DOCUMENT_COUNT - 1,
                DOCUMENT_COUNT - 1,
                new WalPosition("bench-epoch", 0),
                0,
                PruningStats.empty()
            );
            CasResult result = shardStateStore.compareAndSet(
                INDEX_UUID,
                SHARD_ID,
                Optional.empty(),
                new ShardHead(PRIMARY_TERM, null, 0L, manifest.generation())
            );
            if (result != CasResult.SUCCESS) {
                throw new IllegalStateException("failed to activate the benchmark shard's head: " + result);
            }
            return manifest;
        }
    }
}
