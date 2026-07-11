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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleHeader;
import org.opensearch.serverless.storage.format.SegmentBundle;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Times representative {@code serverless-storage} blob operations under each {@link
 * LatencyProfile} preset, using {@link LatencyInjectingBlobContainer} to simulate object-store
 * latency without a real S3/GCS/Azure connection -- more reproducible and controllable in CI than
 * hitting a real cloud backend. The resulting numbers are illustrative only, meant to give the
 * RFC's cold-start/reactivation milestones (rfc-serverless-opensearch.md cold-start/p95/p99
 * section) a repeatable order-of-magnitude signal, not a measurement of real cloud latency.
 *
 * <p><b>Scope decision:</b> this benchmark deliberately does not wire into a full cluster
 * integration test via {@code resolveBlobContainer}/repository configuration. That seam is
 * per-shard, imperative, and tied to real repository backends (see {@code
 * ServerlessStoragePlugin}); standing up a fake repository plugin just to exercise it here would
 * be disproportionate to what this benchmark needs. Instead this class stays at the
 * store/engine-construction level, matching where this plugin's existing unit tests (e.g. {@code
 * BlobContainerManifestStoreTests}, {@code BlobContainerBundleStoreTests}) already operate,
 * against a real {@code FsBlobContainer} wrapped in the latency-injecting decorator.
 *
 * <p>No assertion here depends on absolute timing numbers, since wall-clock timings on shared CI
 * hardware are inherently noisy -- each measurement instead asserts an ordinal relationship
 * (slower preset >= faster preset) and logs the raw min/avg/max for a human to read.
 */
public class BlobLatencyBenchmarkTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(BlobLatencyBenchmarkTests.class);
    private static final int ITERATIONS = 8;

    private BlobContainer newLatencyInjectingContainer(LatencyProfile profile) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer fsContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        return new LatencyInjectingBlobContainer(fsContainer, profile);
    }

    public void testManifestWriteReadRoundTripSlowerUnderHigherLatencyProfile() throws Exception {
        long[] low = timeManifestRoundTrip(LatencyProfile.LOW, "LOW");
        long[] typical = timeManifestRoundTrip(LatencyProfile.TYPICAL, "TYPICAL");
        long[] high = timeManifestRoundTrip(LatencyProfile.HIGH, "HIGH");

        // avg elapsed time (min/avg/max index 1) should scale up with the profile's own latency.
        assertTrue("TYPICAL should be measurably slower than LOW for manifest round-trip", typical[1] >= low[1]);
        assertTrue("HIGH should be measurably slower than TYPICAL for manifest round-trip", high[1] >= typical[1]);
    }

    public void testBundleReadSlowerUnderHigherLatencyProfile() throws Exception {
        long[] low = timeBundleRead(LatencyProfile.LOW, "LOW");
        long[] typical = timeBundleRead(LatencyProfile.TYPICAL, "TYPICAL");
        long[] high = timeBundleRead(LatencyProfile.HIGH, "HIGH");

        assertTrue("TYPICAL should be measurably slower than LOW for bundle read", typical[1] >= low[1]);
        assertTrue("HIGH should be measurably slower than TYPICAL for bundle read", high[1] >= typical[1]);
    }

    /** @return {min, avg, max} elapsed millis across {@link #ITERATIONS} iterations. */
    private long[] timeManifestRoundTrip(LatencyProfile profile, String label) throws Exception {
        long[] samples = new long[ITERATIONS];
        for (int i = 0; i < ITERATIONS; i++) {
            BlobContainerManifestStore store = new BlobContainerManifestStore(newLatencyInjectingContainer(profile));
            CommitManifest manifest = new CommitManifest(
                "bench-index-uuid",
                0,
                1,
                i + 1,
                "segments_" + (i + 1),
                Map.of("segments_" + (i + 1), new FileReference("bundle-1-" + (i + 1), 0, 100, 1L)),
                10,
                10,
                new WalPosition("bench-epoch", 42),
                0,
                PruningStats.empty(),
                123456789L
            );

            long start = System.nanoTime();
            store.writeManifest(manifest);
            store.readManifest(1, i + 1);
            samples[i] = (System.nanoTime() - start) / 1_000_000;
        }
        return logStats("manifest write+read round-trip", label, samples);
    }

    /** @return {min, avg, max} elapsed millis across {@link #ITERATIONS} iterations. */
    private long[] timeBundleRead(LatencyProfile profile, String label) throws Exception {
        long[] samples = new long[ITERATIONS];
        for (int i = 0; i < ITERATIONS; i++) {
            BlobContainerBundleStore store = new BlobContainerBundleStore(newLatencyInjectingContainer(profile));

            List<BundleFileContent> files = new ArrayList<>();
            for (int f = 0; f < 5; f++) {
                files.add(new BundleFileContent("_" + f + ".si", randomByteArrayOfLength(1024)));
            }
            SegmentBundle written = store.writeBundle("bench-bundle-" + i, files);

            long start = System.nanoTime();
            BundleHeader header = store.readHeader("bench-bundle-" + i, written.length());
            BundleFileEntry entry = header.entries().get("_0.si");
            store.readFile("bench-bundle-" + i, entry);
            samples[i] = (System.nanoTime() - start) / 1_000_000;
        }
        return logStats("bundle read", label, samples);
    }

    private long[] logStats(String operation, String label, long[] samplesMillis) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long sum = 0;
        for (long sample : samplesMillis) {
            min = Math.min(min, sample);
            max = Math.max(max, sample);
            sum += sample;
        }
        long avg = sum / samplesMillis.length;
        logger.info(
            "[simulated blob latency benchmark] operation=[{}] profile=[{}] iterations=[{}] minMs=[{}] avgMs=[{}] maxMs=[{}]",
            operation,
            label,
            samplesMillis.length,
            min,
            avg,
            max
        );
        return new long[] { min, avg, max };
    }
}
