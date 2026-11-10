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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.store.BlockCache;
import org.opensearch.serverless.store.BlockCacheDirectory;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * What opening a published commit costs, with and without fetching its blocks ahead.
 *
 * <p>A commit of many segments, half compound and half not, each large enough that its files span several
 * blocks, is published to a filesystem store that delays every request. A reader is opened over it on a cold
 * cache, once with the prefetch and once without, and the two are compared: the same documents found, about
 * the same requests made, and the open waiting a fraction as long, because the requests run concurrently
 * rather than one after another.
 */
public class ServerlessOpenPrefetchTests extends OpenSearchTestCase {

    private static final String TERM_DIR = "t=1";
    private static final int SEGMENTS = 12;
    private static final int DOCS_PER_SEGMENT = 3_000;
    private static final long DELAY_MILLIS = 10;

    private record Opened(long millis, int requests, int hits, BlockCache.Stats stats) {
    }

    public void testFetchingAheadMakesTheOpenWaitForFewerRoundTripsAndFindsTheSameDocuments() throws Exception {
        final LatencyInjectingBlobStore store = new LatencyInjectingBlobStore(new FsBlobStore(1 << 20, createTempDir(), false), 0);
        final BlobPath shardBase = BlobPath.cleanPath().add("segments").add("logs#uuid#0");
        final CommitManifest manifest = publish(store, shardBase);
        store.setDelayMillis(DELAY_MILLIS);

        final ExecutorService helpers = Executors.newFixedThreadPool(16);
        try {
            final Opened without = open(store, shardBase, manifest, new BlockCache(BlockCache.DEFAULT_BLOCK_SIZE, 65_536));
            final Opened with = open(
                store,
                shardBase,
                manifest,
                new BlockCache(BlockCache.DEFAULT_BLOCK_SIZE, 65_536).prefetchingWith(helpers)
            );
            logger.info(
                "open of {} segments at {} ms a request: without prefetch {} ms, {} requests; with {} ms, {} requests",
                SEGMENTS,
                DELAY_MILLIS,
                without.millis(),
                without.requests(),
                with.millis(),
                with.requests()
            );
            assertEquals("the same documents are found", without.hits(), with.hits());
            assertEquals(SEGMENTS * DOCS_PER_SEGMENT / 2, with.hits());
            // Only blocks the open reads anyway are fetched, so the count stays put; a little slack for a
            // range that merged blocks Lucene reads apart, or a block it does not read on this codec.
            assertTrue(
                "prefetch must not add requests: " + with.requests() + " against " + without.requests(),
                with.requests() <= without.requests() * 1.25
            );
            assertTrue(
                "the open must wait far less: " + with.millis() + " ms against " + without.millis() + " ms",
                with.millis() * 2 < without.millis()
            );
        } finally {
            helpers.shutdownNow();
            assertTrue(helpers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testACacheTooSmallToHoldTheOpenIsNotPrefetchedInto() throws Exception {
        final LatencyInjectingBlobStore store = new LatencyInjectingBlobStore(new FsBlobStore(1 << 20, createTempDir(), false), 0);
        final BlobPath shardBase = BlobPath.cleanPath().add("segments").add("logs#uuid#0");
        final CommitManifest manifest = publish(store, shardBase);
        final ExecutorService helpers = Executors.newFixedThreadPool(4);
        try {
            final BlockCache tiny = new BlockCache(BlockCache.DEFAULT_BLOCK_SIZE, 16).prefetchingWith(helpers);
            store.drain();
            try (
                BlockCacheDirectory directory = BlockCacheDirectory.create(
                    new ByteBuffersDirectory(),
                    store,
                    shardBase,
                    tiny,
                    "s",
                    manifest
                )
            ) {
                assertEquals("nothing fetched before a reader asks", 0, store.drain().size());
                assertEquals(0, tiny.stats().blocksResident());
            }
        } finally {
            helpers.shutdownNow();
        }
    }

    public void testAFileHeldLocallyIsNotFetched() throws Exception {
        final LatencyInjectingBlobStore store = new LatencyInjectingBlobStore(new FsBlobStore(1 << 20, createTempDir(), false), 0);
        final BlobPath shardBase = BlobPath.cleanPath().add("segments").add("logs#uuid#0");
        final BlobContainer container = store.blobContainer(shardBase.add(TERM_DIR));
        final byte[] bytes = randomByteArrayOfLength(3 * BlockCache.DEFAULT_BLOCK_SIZE);
        container.writeBlob("_0.fdt", new ByteArrayInputStream(bytes), bytes.length, false);
        container.writeBlob("_0.fdx", new ByteArrayInputStream(bytes), bytes.length, false);
        final CommitManifest manifest = new CommitManifest(
            1L,
            Map.of("_0.fdt", TERM_DIR, "_0.fdx", TERM_DIR),
            "writer",
            Map.of("_0.fdt", (long) bytes.length, "_0.fdx", (long) bytes.length)
        );
        final Directory local = new ByteBuffersDirectory();
        try (var out = local.createOutput("_0.fdx", IOContext.DEFAULT)) {
            out.writeBytes(bytes, bytes.length);
        }
        final ExecutorService helpers = Executors.newFixedThreadPool(4);
        try {
            final BlockCache cache = new BlockCache(BlockCache.DEFAULT_BLOCK_SIZE, 4_096).prefetchingWith(helpers);
            store.drain();
            try (BlockCacheDirectory directory = BlockCacheDirectory.create(local, store, shardBase, cache, "s", manifest)) {
                final List<LatencyInjectingBlobStore.Request> requests = store.drain();
                assertEquals("head and tail of the one remote file: " + requests, 2, requests.size());
                for (LatencyInjectingBlobStore.Request request : requests) {
                    assertTrue(request.toString(), request.key().endsWith("_0.fdt"));
                }
                // And what was fetched is what a reader is served.
                store.drain();
                try (IndexInput in = directory.openInput("_0.fdt", IOContext.DEFAULT)) {
                    final byte[] head = new byte[16];
                    in.readBytes(head, 0, head.length);
                    in.seek(bytes.length - 16);
                    final byte[] tail = new byte[16];
                    in.readBytes(tail, 0, tail.length);
                    assertArrayEquals(java.util.Arrays.copyOfRange(bytes, 0, 16), head);
                    assertArrayEquals(java.util.Arrays.copyOfRange(bytes, bytes.length - 16, bytes.length), tail);
                }
                assertEquals("served from the prefetched blocks", 0, store.drain().size());
            }
        } finally {
            helpers.shutdownNow();
        }
    }

    private Opened open(LatencyInjectingBlobStore store, BlobPath shardBase, CommitManifest manifest, BlockCache cache) throws Exception {
        store.drain();
        final long startedAt = System.nanoTime();
        try (
            BlockCacheDirectory directory = BlockCacheDirectory.create(
                new ByteBuffersDirectory(),
                store,
                shardBase,
                cache,
                "logs#0",
                manifest
            );
            DirectoryReader reader = DirectoryReader.open(directory)
        ) {
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
            final int requests = store.drain().size();
            assertEquals(SEGMENTS, reader.leaves().size());
            final int hits = new IndexSearcher(reader).count(new TermQuery(new Term("parity", "even")));
            return new Opened(millis, requests, hits, cache.stats());
        }
    }

    /** Writes a commit of {@link #SEGMENTS} segments locally and publishes every file of it, as a writer would. */
    private CommitManifest publish(LatencyInjectingBlobStore store, BlobPath shardBase) throws Exception {
        final java.nio.file.Path path = createTempDir();
        try (Directory local = new NIOFSDirectory(path)) {
            final IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(local, config)) {
                int id = 0;
                for (int segment = 0; segment < SEGMENTS; segment++) {
                    // Both layouts a published commit holds: flushed segments compound, large merged ones not.
                    writer.getConfig().setUseCompoundFile(segment % 2 == 0);
                    for (int d = 0; d < DOCS_PER_SEGMENT; d++, id++) {
                        final Document doc = new Document();
                        doc.add(new StringField("_id", Integer.toString(id), Field.Store.YES));
                        doc.add(new StringField("parity", id % 2 == 0 ? "even" : "odd", Field.Store.NO));
                        doc.add(
                            new TextField(
                                "message",
                                String.format(Locale.ROOT, "request %d from host-%d took %d ms", id, id % 97, id % 1_000),
                                Field.Store.YES
                            )
                        );
                        doc.add(new SortedDocValuesField("host", new BytesRef("host-" + (id % 97))));
                        doc.add(new LongPoint("timestamp", 1_700_000_000_000L + id));
                        doc.add(new NumericDocValuesField("timestamp", 1_700_000_000_000L + id));
                        doc.add(new StoredField("bytes", id * 7L));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            final BlobContainer container = store.blobContainer(shardBase.add(TERM_DIR));
            final Map<String, String> files = new LinkedHashMap<>();
            final Map<String, Long> lengths = new LinkedHashMap<>();
            final org.apache.lucene.index.SegmentInfos commit = org.apache.lucene.index.SegmentInfos.readLatestCommit(local);
            for (String name : commit.files(true)) {
                final byte[] bytes = java.nio.file.Files.readAllBytes(path.resolve(name));
                container.writeBlob(name, new ByteArrayInputStream(bytes), bytes.length, false);
                files.put(name, TERM_DIR);
                lengths.put(name, (long) bytes.length);
            }
            return new CommitManifest(1L, files, "writer", lengths);
        }
    }
}
