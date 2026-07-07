/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.serverless;

import org.opensearch.serverless.storage.wal.WalChunkReader;
import org.opensearch.serverless.storage.wal.WalChunkWriter;
import org.opensearch.serverless.storage.wal.WalRecord;
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
 * Measures group-commit throughput of the WAL chunk format (rfc-serverless-opensearch.md
 * &sect;6.4): building one chunk from many shards' buffered operations (the per-flush-interval
 * hot path of the node-level WAL service) and replaying it back. {@code recordsPerChunk} spans
 * the range implied by the RFC's default 8-16MB / 100-250ms group-commit thresholds at typical
 * per-document payload sizes.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class WalChunkBenchmark {

    @Param({ "100", "1000", "10000" })
    private int recordsPerChunk;

    @Param({ "256", "2048" })
    private int payloadSizeBytes;

    private List<WalRecord> records;
    private byte[] prebuiltChunk;

    @Setup(Level.Trial)
    public void setUp() {
        Random random = new Random(42);
        records = new ArrayList<>(recordsPerChunk);
        for (int i = 0; i < recordsPerChunk; i++) {
            byte[] payload = new byte[payloadSizeBytes];
            random.nextBytes(payload);
            // Simulate ~20 distinct shards on one node sharing the chunk, as the RFC describes.
            records.add(new WalRecord("index-" + (i % 4), i % 20, i, payload));
        }
        prebuiltChunk = WalChunkWriter.write(records);
    }

    @Benchmark
    public void writeChunk(Blackhole blackhole) {
        blackhole.consume(WalChunkWriter.write(records));
    }

    @Benchmark
    public void readChunk(Blackhole blackhole) throws Exception {
        blackhole.consume(WalChunkReader.readRecords(prebuiltChunk));
    }
}
