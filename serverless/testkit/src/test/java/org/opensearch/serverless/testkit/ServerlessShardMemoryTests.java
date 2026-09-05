/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M14's last unmeasured claim: what an open shard costs this node, and whether that cost is flat in how
 * many are already open.
 *
 * <p>Core's own {@code rfc-100m-index-architecture.md} measured 150,888 B and 3.0 file descriptors per
 * <em>gated</em> index — a descriptor with no shard actually open, the lightest state that design has.
 * There is no equivalent here to measure: this shell has no state lighter than an open shard. An index
 * with no shard open costs one register read to describe and nothing else; the number that bounds how
 * many indices a node can actually serve at once is what one <em>open</em> shard costs, which is what
 * {@code maxShardsHeld} (M13) is a bound on. That number had never been measured, only reasoned about.
 *
 * <p><b>What "flat" means here, and why nothing is asserted on an absolute byte count.</b> A wall-clock or
 * byte-count threshold would make this suite a JVM-version detector rather than an architecture check —
 * compressed oops, heap layout and JIT warmup all move the absolute number across environments that have
 * changed nothing this design controls. What is asserted is the trend {@code ServerlessScaleTests} already
 * asserts trends on: the marginal cost of the next N shards must not be dramatically larger than the
 * marginal cost of the first N — which is what a hidden O(open shards) structure (a linear scan over every
 * open shard triggered by opening one more) would produce, and a flat per-shard cost would not.
 *
 * <p><b>The honest limits of a heap measurement inside a shared test JVM.</b> {@code Runtime}'s heap
 * accounting is an estimate, not an audit — object headers, compressed oops and JIT-compiled code are not
 * uniform across JVMs, and other test classes loaded earlier in this JVM leave garbage that {@code
 * System.gc()} is asked, not guaranteed, to collect. The absolute numbers are logged for the record and
 * are not the claim; the trend across two batches of the same size, forced through the same GC in the same
 * JVM, is.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only. A local filesystem is not an object store, and shard opening
 * does not touch the network here regardless — this measures JVM heap and file descriptors, not
 * object-store cost, which {@code ServerlessCostTests} already covers.
 */
public class ServerlessShardMemoryTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    /** No mapping beyond what every index needs, so what is measured is the shard's fixed overhead. */
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    /** The size of each batch opened before a measurement. Big enough to clear noise, small enough to run. */
    private static final int BATCH = 30;

    /**
     * The batch size for the reader-versus-writer comparison specifically. Larger than {@link #BATCH}:
     * the real gap between the two is a percentage of the batch, so a bigger batch grows the signal while
     * per-run JVM noise -- which is roughly a fixed amount, not proportional to shard count -- does not
     * grow with it. Chosen empirically after the default batch size proved too close to the noise floor to
     * assert on reliably even with three independent trials.
     */
    private static final int COMPARISON_BATCH = 100;

    private Settings nodeSettings(String name, String role) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-shard-memory")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", role)
            .build();
    }

    /**
     * Forces the heap to a state worth reading. Two collections and a short pause, because one collection
     * on some collectors reports objects it is about to reclaim as still live.
     */
    private static long usedHeapBytes() {
        System.gc();
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        final Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /** {@code /proc/self/fd} is Linux-only; every other platform's equivalent is not a directory listing. */
    private static boolean fileDescriptorCountingIsAvailable() {
        return Files.isDirectory(Paths.get("/proc/self/fd"));
    }

    private static long openFileDescriptorCount() throws Exception {
        try (var listing = Files.list(Paths.get("/proc/self/fd"))) {
            return listing.count();
        }
    }

    /**
     * Opens {@code count} more writer shards on one index and returns their ids, so a caller can measure
     * before and after.
     */
    private List<ShardId> openWriterShards(ServerlessNode node, MetadataPlane plane, String index, int startAt, int count)
        throws Exception {
        final List<ShardId> opened = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            opened.add(node.activateWriter(plane, index, startAt + i).orElseThrow());
        }
        return opened;
    }

    /**
     * The marginal heap cost of the next batch of open writer shards is not dramatically larger than the
     * marginal cost of the first batch.
     *
     * <p>Absolute numbers are logged, not asserted on — see the class javadoc. What is asserted is the
     * ratio between two equal-sized batches, which is what would move if opening a shard triggered work
     * proportional to how many are already open rather than work proportional to one shard.
     */
    public void testHeapPerOpenWriterShardDoesNotGrowWithHowManyAreAlreadyOpen() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final int totalShards = BATCH * 2;
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", totalShards, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("mem-writer", "ingest"))) {
            node.start();
            node.setMetadataPlane(plane);

            final long baseline = usedHeapBytes();
            openWriterShards(node, plane, "alpha", 0, BATCH);
            final long afterFirstBatch = usedHeapBytes();
            openWriterShards(node, plane, "alpha", BATCH, BATCH);
            final long afterSecondBatch = usedHeapBytes();

            assertEquals("both batches must actually have opened", totalShards, node.reconciler().openShards().size());

            final long firstBatchCost = afterFirstBatch - baseline;
            final long secondBatchCost = afterSecondBatch - afterFirstBatch;
            final double perShardFirst = firstBatchCost / (double) BATCH;
            final double perShardSecond = secondBatchCost / (double) BATCH;

            logger.info(
                "shard memory: baseline {} MB, +{} shards -> {} MB ({} B/shard), +{} more -> {} MB ({} B/shard)",
                baseline / 1_048_576.0,
                BATCH,
                afterFirstBatch / 1_048_576.0,
                Math.round(perShardFirst),
                BATCH,
                afterSecondBatch / 1_048_576.0,
                Math.round(perShardSecond)
            );

            // Real consumption, asserted before the ratio is trusted: a measurement stuck at zero or
            // negative (GC noise swamping the signal, or a broken accounting) would make the ratio below
            // pass vacuously whichever way its escape hatch pointed. 30 real Lucene shards costs at least a
            // few hundred KB by construction -- one segment file each is already more than that.
            assertTrue("opening " + BATCH + " shards must have cost real heap: " + firstBatchCost + " B", firstBatchCost > 100_000);

            // A generous ratio, deliberately: this is a noisy, single-run, single-JVM measurement, and the
            // property worth failing a build over is a structural blow-up (an O(open shards) cost hiding
            // in a per-open path), not a 2x wobble from GC timing. 5x would not happen from noise alone.
            assertTrue(
                "the second batch must not cost dramatically more per shard than the first "
                    + "(first "
                    + Math.round(perShardFirst)
                    + " B/shard, second "
                    + Math.round(perShardSecond)
                    + " B/shard) -- that shape is what a per-open-shard scan over every already-open shard "
                    + "would produce",
                perShardSecond < perShardFirst * 5
            );
        }
    }

    /**
     * The same trend, in file descriptors rather than heap -- a resource {@code Runtime} says nothing
     * about, and the one an operating system will refuse a node for exhausting before heap becomes the
     * problem.
     */
    public void testFileDescriptorsPerOpenWriterShardDoesNotGrowWithHowManyAreAlreadyOpen() throws Exception {
        assumeTrue("file descriptor counting needs /proc/self/fd (Linux only)", fileDescriptorCountingIsAvailable());

        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final int totalShards = BATCH * 2;
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", totalShards, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("fd-writer", "ingest"))) {
            node.start();
            node.setMetadataPlane(plane);

            final long baseline = openFileDescriptorCount();
            openWriterShards(node, plane, "alpha", 0, BATCH);
            final long afterFirstBatch = openFileDescriptorCount();
            openWriterShards(node, plane, "alpha", BATCH, BATCH);
            final long afterSecondBatch = openFileDescriptorCount();

            final double perShardFirst = (afterFirstBatch - baseline) / (double) BATCH;
            final double perShardSecond = (afterSecondBatch - afterFirstBatch) / (double) BATCH;

            logger.info(
                "shard file descriptors: baseline {}, +{} -> {} ({} fd/shard), +{} more -> {} ({} fd/shard)",
                baseline,
                BATCH,
                afterFirstBatch,
                perShardFirst,
                BATCH,
                afterSecondBatch,
                perShardSecond
            );

            assertTrue("some file descriptors must actually have been opened", afterFirstBatch > baseline);
            assertTrue(
                "the second batch must not cost dramatically more descriptors per shard than the first "
                    + "(first "
                    + perShardFirst
                    + ", second "
                    + perShardSecond
                    + ")",
                perShardSecond < perShardFirst * 5
            );
        }
    }

    /** What one trial of the reader-versus-writer comparison measured. */
    private record MemoryTrial(long writerCost, long readerCost) {
    }

    /**
     * Runs one trial of the writer-versus-reader comparison: fresh temp directory, fresh plane, fresh
     * nodes, so nothing from a previous trial leaks into this one's measurement.
     */
    private MemoryTrial memoryTrial(int trialNumber) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", COMPARISON_BATCH, MAPPING, null));
        final String suffix = "-t" + trialNumber;

        // Published, so a reader has something to open -- an unpublished shard cannot be served as a
        // reader at all (ServerlessSearchPathTests), so measuring readers needs real published commits.
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("mem-publisher" + suffix, "ingest"))) {
            writer.start();
            writer.setMetadataPlane(plane);
            for (int shard = 0; shard < COMPARISON_BATCH; shard++) {
                final var shardId = writer.activateWriter(plane, "alpha", shard).orElseThrow();
                final long term = plane.heads().read("alpha", shard).orElseThrow().term();
                writer.publishShard(shardId, term);
            }
        }
        // close() tears down local infrastructure and does not release shard-head ownership -- only the
        // lease expiring does that. The publisher's leases are still live at the clock's current instant,
        // so the comparison writer below needs the clock moved past the TTL first, the same way any test
        // of a successor taking over after a predecessor is gone does.
        clock.set(clock.get() + TTL);

        // The publisher block above is an unintentional warm-up for the writer path: every class it needs
        // was already loaded before the timed measurement runs. Reader-only code -- ReadOnlyEngine and
        // everything under it -- has no equivalent unless one is added, so the first thing to touch it
        // would otherwise be the timed measurement itself: one-time class-loading cost counted as a
        // per-shard cost. A throwaway reader shard pays that cost first, matching the writer path's
        // free warm-up rather than leaving the comparison asymmetric.
        try (ServerlessNode warmup = new ServerlessNode(nodeSettings("mem-reader-warmup" + suffix, "search"))) {
            warmup.start();
            warmup.setMetadataPlane(plane);
            warmup.serveAsReader(plane, "alpha", 0);
        }

        final long writerCost;
        try (ServerlessNode node = new ServerlessNode(nodeSettings("mem-writer-cmp" + suffix, "ingest"))) {
            node.start();
            node.setMetadataPlane(plane);
            final long baseline = usedHeapBytes();
            openWriterShards(node, plane, "alpha", 0, COMPARISON_BATCH);
            writerCost = usedHeapBytes() - baseline;
        }

        final long readerCost;
        try (ServerlessNode node = new ServerlessNode(nodeSettings("mem-reader-cmp" + suffix, "search"))) {
            node.start();
            node.setMetadataPlane(plane);
            final long baseline = usedHeapBytes();
            for (int shard = 0; shard < COMPARISON_BATCH; shard++) {
                node.serveAsReader(plane, "alpha", shard);
            }
            readerCost = usedHeapBytes() - baseline;
            assertEquals("every shard must actually have opened as a reader", COMPARISON_BATCH, node.reconciler().readerShards().size());
        }

        assertTrue("opening " + COMPARISON_BATCH + " writer shards must have cost real heap: " + writerCost + " B", writerCost > 100_000);
        assertTrue("opening " + COMPARISON_BATCH + " reader shards must have cost real heap: " + readerCost + " B", readerCost > 100_000);
        return new MemoryTrial(writerCost, readerCost);
    }

    /**
     * A reader shard -- no translog, no indexing buffers, a read-only engine -- costs less heap than a
     * writer shard, which is the premise index/search separation is built on.
     *
     * <p>Both are measured in the same JVM, one after the other, rather than compared across two separate
     * test runs, so JVM-to-JVM noise cannot be mistaken for the difference this is actually about.
     *
     * <p><b>Three independent trials, majority rules, not one.</b> A single heap snapshot inside a shared
     * test JVM is close enough to the real gap between these two (roughly 50 KB against roughly 67 KB at
     * this batch size) that background allocation from unrelated threads can occasionally swing the sign
     * of one measurement — this test was flaky at exactly that margin before trials were added. Three
     * independent runs, agreeing on direction at least twice, is what tells a real 25%-ish structural
     * difference apart from noise of comparable size to it without either hiding a genuine regression
     * behind noise tolerance or chasing single-run precision heap accounting cannot actually offer.
     */
    public void testAReaderShardCostsLessHeapThanAWriterShard() throws Exception {
        final int trials = 3;
        int readerWasLighter = 0;
        final List<MemoryTrial> results = new ArrayList<>();
        for (int trial = 1; trial <= trials; trial++) {
            final MemoryTrial result = memoryTrial(trial);
            results.add(result);
            if (result.readerCost() < result.writerCost()) {
                readerWasLighter++;
            }
            logger.info(
                "shard memory trial {}/{}: {} writer shards cost {} MB ({} B/shard), {} reader shards cost {} MB ({} B/shard)",
                trial,
                trials,
                COMPARISON_BATCH,
                result.writerCost() / 1_048_576.0,
                result.writerCost() / COMPARISON_BATCH,
                COMPARISON_BATCH,
                result.readerCost() / 1_048_576.0,
                result.readerCost() / COMPARISON_BATCH
            );
        }

        assertTrue(
            "a reader shard must cost less heap than a writer shard in at least "
                + (trials / 2 + 1)
                + " of "
                + trials
                + " independent trials, got "
                + readerWasLighter
                + ": "
                + results,
            readerWasLighter > trials / 2
        );
    }
}
