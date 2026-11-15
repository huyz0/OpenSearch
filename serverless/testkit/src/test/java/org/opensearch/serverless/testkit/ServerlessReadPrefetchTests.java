/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.serverless.shard.StoredFieldsPrefetch;
import org.opensearch.serverless.store.BlockCache;
import org.opensearch.serverless.store.BlockCacheDirectory;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lucene's read hints, {@code IndexInput.prefetch}, turned into concurrent block fetches.
 *
 * <p>Lucene announces some reads before it makes them: a term lookup schedules the terms-dictionary block it will
 * read in every segment before reading any, and a postings enum announces where its postings start before the
 * first document is asked for. On a local disk the hint saves a page fault; here every read that misses the block
 * cache is an object-store round trip, so a hint left unanswered keeps those reads one after another. These tests
 * pin that an announced range is fetched in the background and then served without a request, that slices of a
 * compound file keep the hint and share the file's blocks, that a failed hint never fails the read it was for, and
 * that a cold query over many segments waits for fewer round trips with the same answer.
 */
public class ServerlessReadPrefetchTests extends OpenSearchTestCase {

    private static final int BLOCK = 4096;
    private static final String TERM_DIR = "t=1";
    private static final String FILE = "_0.cfs";
    private static final BlobPath SHARD = BlobPath.cleanPath().add("segments").add("alpha#uuid#0");

    private static byte[] pattern(int size) {
        final byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) (i % 251);
        }
        return bytes;
    }

    private static BlockCacheDirectory publish(BlobStore store, BlockCache cache, byte[] bytes) throws IOException {
        store.blobContainer(SHARD.add(TERM_DIR)).writeBlob(FILE, new ByteArrayInputStream(bytes), bytes.length, false);
        final CommitManifest manifest = new CommitManifest(1L, Map.of(FILE, TERM_DIR), "writer", Map.of(FILE, (long) bytes.length));
        return BlockCacheDirectory.create(new ByteBuffersDirectory(), store, SHARD, cache, "alpha#uuid#0", manifest);
    }

    /** Waits until every block of a byte range of {@link #FILE} is held, or fails the test after ten seconds. */
    private static void awaitHeld(BlockCache cache, long offset, long length) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (long block = offset / BLOCK; block <= (offset + length - 1) / BLOCK; block++) {
            while (heldUnderAnyScope(cache, block) == false) {
                assertTrue("block " + block + " was never prefetched", System.nanoTime() < deadline);
                Thread.sleep(5);
            }
        }
    }

    private static boolean heldUnderAnyScope(BlockCache cache, long block) {
        for (java.util.List<String> keys : cache.residentByScope(java.util.Set.of("alpha#uuid#0")).values()) {
            for (String key : keys) {
                if (key.endsWith(FILE + "#" + block)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void testAnAnnouncedRangeIsFetchedAheadAndThenReadWithoutARequest() throws Exception {
        final byte[] bytes = pattern(64 * BLOCK);
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final ExecutorService helpers = Executors.newFixedThreadPool(4);
        try {
            final BlockCache cache = new BlockCache(BLOCK, 4096).prefetchingWith(helpers).prefetchingReads(true);
            try (
                BlockCacheDirectory directory = publish(store, cache, bytes);
                IndexInput in = directory.openInput(FILE, IOContext.DEFAULT)
            ) {
                final long offset = 37L * BLOCK + 100;
                final int length = 2 * BLOCK;
                store.reset();
                in.prefetch(offset, length);
                awaitHeld(cache, offset, length);
                assertEquals("the announced blocks, consecutive ones in one ranged read", 1, store.blobReads());

                store.reset();
                in.seek(offset);
                final byte[] read = new byte[length];
                in.readBytes(read, 0, length);
                assertArrayEquals(java.util.Arrays.copyOfRange(bytes, (int) offset, (int) offset + length), read);
                assertEquals("served from what the hint fetched", 0, store.blobReads());
            }
        } finally {
            helpers.shutdownNow();
        }
    }

    public void testWithReadPrefetchOffAHintFetchesNothing() throws Exception {
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final ExecutorService helpers = Executors.newFixedThreadPool(4);
        try {
            final BlockCache cache = new BlockCache(BLOCK, 4096).prefetchingWith(helpers).prefetchingReads(false);
            try (BlockCacheDirectory directory = publish(store, cache, pattern(64 * BLOCK))) {
                store.reset();
                try (IndexInput in = directory.openInput(FILE, IOContext.DEFAULT)) {
                    in.prefetch(20L * BLOCK, BLOCK);
                    helpers.shutdown();
                    assertTrue(helpers.awaitTermination(10, TimeUnit.SECONDS));
                    assertEquals("a hint is only a hint when read prefetch is off", 0, store.blobReads());
                }
            }
        } finally {
            helpers.shutdownNow();
        }
    }

    public void testASliceKeepsTheHintAndSharesTheFilesBlocks() throws Exception {
        final byte[] bytes = pattern(64 * BLOCK);
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final ExecutorService helpers = Executors.newFixedThreadPool(4);
        try {
            final BlockCache cache = new BlockCache(BLOCK, 4096).prefetchingWith(helpers).prefetchingReads(true);
            try (
                BlockCacheDirectory directory = publish(store, cache, bytes);
                IndexInput file = directory.openInput(FILE, IOContext.DEFAULT)
            ) {
                // A compound sub-file starts wherever the compound writer put it, not on a block boundary.
                final long sliceStart = 10L * BLOCK + 123;
                final IndexInput slice = file.slice("sub-file", sliceStart, 30L * BLOCK);
                final long within = 5L * BLOCK;
                store.reset();
                slice.prefetch(within, 100);
                awaitHeld(cache, sliceStart + within, 100);
                assertEquals(1, store.blobReads());

                store.reset();
                slice.seek(within);
                for (int i = 0; i < 100; i++) {
                    assertEquals((byte) ((sliceStart + within + i) % 251), slice.readByte());
                }
                file.seek(sliceStart + within);
                assertEquals("the file and its slice read the same block", (byte) ((sliceStart + within) % 251), file.readByte());
                assertEquals("both served from the block the slice's hint fetched", 0, store.blobReads());
                assertEquals(30L * BLOCK, slice.length());
                expectThrows(IOException.class, () -> slice.seek(31L * BLOCK));
            }
        } finally {
            helpers.shutdownNow();
        }
    }

    public void testAHintThatFailsLeavesTheReadToFetchForItself() throws Exception {
        final byte[] bytes = pattern(64 * BLOCK);
        final AtomicInteger failuresLeft = new AtomicInteger(0);
        final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        final BlobStore inner = new FsBlobStore(1024, createTempDir(), false);
        final BlobStore failingOnce = new BlobStore() {
            @Override
            public BlobContainer blobContainer(BlobPath path) {
                return new FilterBlobContainer(inner.blobContainer(path)) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public InputStream readBlob(String blobName, long position, long length) throws IOException {
                        if (failuresLeft.getAndDecrement() > 0) {
                            // The hint's request: held until a reader is waiting on it, then failed.
                            entered.countDown();
                            try {
                                release.await(10, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            throw new IOException("injected: the hint's request fails");
                        }
                        return super.readBlob(blobName, position, length);
                    }
                };
            }

            @Override
            public void close() throws IOException {
                inner.close();
            }
        };
        final ExecutorService helpers = Executors.newFixedThreadPool(4);
        try {
            final BlockCache cache = new BlockCache(BLOCK, 4096).prefetchingWith(helpers).prefetchingReads(true);
            try (
                BlockCacheDirectory directory = publish(failingOnce, cache, bytes);
                IndexInput in = directory.openInput(FILE, IOContext.DEFAULT)
            ) {
                failuresLeft.set(1);
                in.prefetch(40L * BLOCK, 10);
                assertTrue("the hint's request started", entered.await(10, TimeUnit.SECONDS));
                final long waitingBefore = cache.coalesced();
                final java.util.concurrent.Future<Byte> read = helpers.submit(() -> {
                    final IndexInput reader = in.clone();
                    reader.seek(40L * BLOCK);
                    return reader.readByte();
                });
                // The reader misses, finds the hint's fetch in flight, and waits on it rather than fetching again.
                assertBusy(() -> assertTrue(cache.coalesced() > waitingBefore));
                release.countDown();
                assertEquals(
                    "the reader fetched for itself after the hint failed",
                    (byte) ((40L * BLOCK) % 251),
                    (byte) read.get(10, TimeUnit.SECONDS)
                );
            }
        } finally {
            helpers.shutdownNow();
        }
    }

    private record Timed(long millis, int requests, long totalHits, int topDoc) {
    }

    /**
     * A cold scored search over many segments, with and without read prefetch, against a store that delays every
     * request: the same hits, about the same requests, and less waiting, because each segment's terms-dictionary
     * reads are announced together and run concurrently rather than one segment after another.
     */
    public void testAColdQueryOverManySegmentsWaitsForFewerRoundTrips() throws Exception {
        final LatencyInjectingBlobStore store = new LatencyInjectingBlobStore(new FsBlobStore(1 << 20, createTempDir(), false), 0);
        final CommitManifest manifest = publishSegments(store);
        store.setDelayMillis(10);
        final ExecutorService helpers = Executors.newFixedThreadPool(16);
        try {
            // Small blocks, so the open's own head-and-tail prefetch leaves the query's blocks cold, as it does for
            // segments of a realistic size at the default block size.
            final int block = 1024;
            // One uncounted run first, so neither measured run pays for class loading and compilation.
            search(store, manifest, new BlockCache(block, 65_536).prefetchingWith(helpers).prefetchingReads(true));
            Timed without = null;
            Timed with = null;
            for (int round = 0; round < 3; round++) {
                // Alternated, and the best of three kept for each, so the order the two run in favours neither.
                final Timed off = search(store, manifest, new BlockCache(block, 65_536).prefetchingWith(helpers));
                final Timed on = search(store, manifest, new BlockCache(block, 65_536).prefetchingWith(helpers).prefetchingReads(true));
                without = without == null || off.millis() < without.millis() ? off : without;
                with = with == null || on.millis() < with.millis() ? on : with;
            }
            assertTrue("the query must reach the store, or there is nothing to measure", without.requests() >= SEGMENTS);
            logger.info(
                "cold query over {} segments at 10 ms a request: without read prefetch {} ms, {} requests; with {} ms, {} requests",
                SEGMENTS,
                without.millis(),
                without.requests(),
                with.millis(),
                with.requests()
            );
            assertEquals("the same hits", without.totalHits(), with.totalHits());
            assertEquals("the same top document", without.topDoc(), with.topDoc());
            assertTrue(
                "read prefetch must not add many requests: " + with.requests() + " against " + without.requests(),
                with.requests() <= without.requests() * 1.25 + 2
            );
            assertTrue(
                "the cold query must wait less: " + with.millis() + " ms against " + without.millis() + " ms",
                with.millis() * 4 < without.millis() * 3
            );
        } finally {
            helpers.shutdownNow();
            assertTrue(helpers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /**
     * A cold fetch of one hit from each of many segments, as the fetch phase runs it: with every hit's stored fields
     * announced before the first is read, the blocks are fetched together rather than one segment after another, for
     * the same requests.
     */
    public void testAColdFetchOfHitsInManySegmentsWaitsForFewerRoundTrips() throws Exception {
        final LatencyInjectingBlobStore store = new LatencyInjectingBlobStore(new FsBlobStore(1 << 20, createTempDir(), false), 0);
        final CommitManifest manifest = publishSegments(store, 1_024);
        store.setDelayMillis(10);
        final ExecutorService helpers = Executors.newFixedThreadPool(16);
        try {
            final int block = BlockCache.DEFAULT_BLOCK_SIZE;
            fetch(store, manifest, new BlockCache(block, 65_536).prefetchingWith(helpers).prefetchingReads(true), true);
            Timed without = null;
            Timed with = null;
            for (int round = 0; round < 3; round++) {
                final Timed off = fetch(
                    store,
                    manifest,
                    new BlockCache(block, 65_536).prefetchingWith(helpers).prefetchingReads(true),
                    false
                );
                final Timed on = fetch(
                    store,
                    manifest,
                    new BlockCache(block, 65_536).prefetchingWith(helpers).prefetchingReads(true),
                    true
                );
                without = without == null || off.millis() < without.millis() ? off : without;
                with = with == null || on.millis() < with.millis() ? on : with;
            }
            assertTrue("the fetch must reach the store, or there is nothing to measure", without.requests() >= SEGMENTS);
            logger.info(
                "cold fetch of one hit in each of {} segments at 10 ms a request: read one after another {} ms, {} requests; "
                    + "announced first {} ms, {} requests",
                SEGMENTS,
                without.millis(),
                without.requests(),
                with.millis(),
                with.requests()
            );
            assertEquals("the same documents", without.topDoc(), with.topDoc());
            assertTrue(
                "announcing must not add requests: " + with.requests() + " against " + without.requests(),
                with.requests() <= without.requests()
            );
            assertTrue(
                "the cold fetch must wait less: " + with.millis() + " ms against " + without.millis() + " ms",
                with.millis() * 2 < without.millis()
            );
        } finally {
            helpers.shutdownNow();
            assertTrue(helpers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /** Reads the stored fields of one document in every segment, announcing them all first if asked, as the fetch phase does. */
    private Timed fetch(LatencyInjectingBlobStore store, CommitManifest manifest, BlockCache cache, boolean announce) throws Exception {
        try (
            BlockCacheDirectory directory = BlockCacheDirectory.create(new ByteBuffersDirectory(), store, SHARD, cache, "logs#0", manifest);
            DirectoryReader reader = DirectoryReader.open(directory)
        ) {
            assertEquals(SEGMENTS, reader.leaves().size());
            store.drain();
            final long startedAt = System.nanoTime();
            final java.util.List<org.apache.lucene.index.StoredFields> fields = new java.util.ArrayList<>();
            for (org.apache.lucene.index.LeafReaderContext leaf : reader.leaves()) {
                fields.add(leaf.reader().storedFields());
            }
            final int doc = DOCS_PER_SEGMENT / 2 + 7;
            if (announce) {
                // Through the fetch phase's listener: top-level ids, each announced to its own segment.
                final int[] hits = new int[reader.leaves().size()];
                for (int i = 0; i < hits.length; i++) {
                    hits[i] = reader.leaves().get(i).docBase + doc;
                }
                StoredFieldsPrefetch.announce(reader.leaves(), hits);
            }
            int checksum = 0;
            for (org.apache.lucene.index.StoredFields each : fields) {
                checksum = 31 * checksum + each.document(doc).get("_id").hashCode();
            }
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
            final java.util.List<LatencyInjectingBlobStore.Request> requests = store.drain();
            return new Timed(millis, requests.size(), fields.size(), checksum);
        }
    }

    private static final int SEGMENTS = 12;
    private static final int DOCS_PER_SEGMENT = 2_000;

    private Timed search(LatencyInjectingBlobStore store, CommitManifest manifest, BlockCache cache) throws Exception {
        try (
            BlockCacheDirectory directory = BlockCacheDirectory.create(new ByteBuffersDirectory(), store, SHARD, cache, "logs#0", manifest);
            DirectoryReader reader = DirectoryReader.open(directory)
        ) {
            assertEquals(SEGMENTS, reader.leaves().size());
            store.drain();
            final IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            final BooleanQuery query = new BooleanQuery.Builder().add(new TermQuery(new Term("message", "host")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("message", "slow")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("message", "status")), BooleanClause.Occur.SHOULD)
                .build();
            final long startedAt = System.nanoTime();
            final TopDocs top = searcher.search(query, 10);
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
            return new Timed(millis, store.drain().size(), top.totalHits.value(), top.scoreDocs[0].doc);
        }
    }

    /** A body that varies from document to document, as sources do, so stored-field chunks do not compress to nothing. */
    private static String body(int id, int bytes) {
        final java.util.Random random = new java.util.Random(id);
        final StringBuilder text = new StringBuilder(bytes);
        while (text.length() < bytes) {
            text.append(Integer.toString(random.nextInt(), 36)).append(' ');
        }
        return text.toString();
    }

    /** Writes {@link #SEGMENTS} segments, half of them compound, and publishes every file, as a writer would. */
    private CommitManifest publishSegments(LatencyInjectingBlobStore store) throws Exception {
        return publishSegments(store, 0);
    }

    /**
     * As {@link #publishSegments(LatencyInjectingBlobStore)}, each document also storing a body of about this many bytes
     * that compresses as little as a typical source does.
     */
    private CommitManifest publishSegments(LatencyInjectingBlobStore store, int bodyBytes) throws Exception {
        final java.nio.file.Path path = createTempDir();
        try (Directory local = new NIOFSDirectory(path)) {
            // The production codec, not the test framework's random one: what a hint reaches depends on the stored-fields
            // format, and a randomly chosen one may not pass hints on at all.
            final IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            if (bodyBytes > 0) {
                config.setCodec(org.apache.lucene.tests.util.TestUtil.getDefaultCodec());
            }
            try (IndexWriter writer = new IndexWriter(local, config)) {
                int id = 0;
                for (int segment = 0; segment < SEGMENTS; segment++) {
                    writer.getConfig().setUseCompoundFile(segment % 2 == 0);
                    for (int d = 0; d < DOCS_PER_SEGMENT; d++, id++) {
                        final Document doc = new Document();
                        doc.add(new StringField("_id", Integer.toString(id), Field.Store.YES));
                        doc.add(
                            new TextField(
                                "message",
                                String.format(
                                    Locale.ROOT,
                                    "request %d from host %d status %d was %s",
                                    id,
                                    id % 97,
                                    200 + id % 5,
                                    id % 13 == 0 ? "slow" : "fast"
                                ),
                                Field.Store.YES
                            )
                        );
                        if (bodyBytes > 0) {
                            doc.add(new org.apache.lucene.document.StoredField("body", body(id, bodyBytes)));
                        }
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            final BlobContainer container = store.blobContainer(SHARD.add(TERM_DIR));
            final Map<String, String> files = new LinkedHashMap<>();
            final Map<String, Long> lengths = new LinkedHashMap<>();
            for (String name : org.apache.lucene.index.SegmentInfos.readLatestCommit(local).files(true)) {
                final byte[] bytes = java.nio.file.Files.readAllBytes(path.resolve(name));
                container.writeBlob(name, new ByteArrayInputStream(bytes), bytes.length, false);
                files.put(name, TERM_DIR);
                lengths.put(name, (long) bytes.length);
            }
            return new CommitManifest(1L, files, "writer", lengths);
        }
    }
}
