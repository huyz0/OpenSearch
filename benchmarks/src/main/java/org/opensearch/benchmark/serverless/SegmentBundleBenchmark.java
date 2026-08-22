/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.serverless;

import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleReader;
import org.opensearch.serverless.storage.format.BundleWriter;
import org.opensearch.serverless.storage.format.SegmentBundle;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures the throughput of the object-store-native segment bundle format
 * (rfc-serverless-opensearch.md &sect;6.2) at the write path (packing files into a bundle, on
 * every flush) and the read path (extracting one file from an already-fetched bundle, on every
 * cache miss). Bundle count/size directly informs the batching thresholds discussed in the RFC
 * (&sect;6.2's {@code serverless.bundle.max_size}) and the request-cost economics flagged as a
 * risk in &sect;18.1.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SegmentBundleBenchmark {

    @Param({ "10", "100", "1000" })
    private int fileCount;

    @Param({ "4096", "65536" })
    private int fileSizeBytes;

    private List<BundleFileContent> files;
    private SegmentBundle prebuiltBundle;
    private BundleFileEntry entryToExtract;

    @Setup(Level.Trial)
    public void setUp() {
        Random random = new Random(42);
        files = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            byte[] content = new byte[fileSizeBytes];
            random.nextBytes(content);
            files.add(new BundleFileContent("_" + i + ".si", content));
        }
        prebuiltBundle = BundleWriter.write(files);
        entryToExtract = prebuiltBundle.entries().get(files.get(files.size() / 2).name());
    }

    @Benchmark
    public void writeBundle(Blackhole blackhole) {
        blackhole.consume(BundleWriter.write(files));
    }

    @Benchmark
    public void parseHeader(Blackhole blackhole) throws Exception {
        blackhole.consume(BundleReader.parseHeader(prebuiltBundle.bytes()));
    }

    @Benchmark
    public void extractOneFile(Blackhole blackhole) throws Exception {
        blackhole.consume(BundleReader.extractFile(prebuiltBundle.bytes(), entryToExtract));
    }
}
