/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.store.BlockCache;
import org.opensearch.serverless.store.BlockCacheDirectory;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.util.Map;

/**
 * What a cold read costs in requests, as opposed to in bytes.
 *
 * <p>{@link ServerlessBlockReadTests} pins the bytes: a reader fetches a fraction of the segments it
 * serves, which is the property block-range reads exist for. It says nothing about <em>requests</em>, and
 * on an object store those are the bill. A block was one ranged GET, so a cold scan of a segment cost one
 * billed round trip per block however obviously sequential it was — about 1,600 of them for 100 MB at the
 * production block size.
 *
 * <p>Read-ahead is the answer, and the reason it can be applied safely is that the two access patterns are
 * distinguishable: a miss for the block immediately after the last fetch ended is a scan, anything else is
 * a seek. So the tests below are a pair. Sequential reading must collapse into far fewer requests than
 * blocks; random reading must cost exactly what it did before, because over-fetching for a term dictionary
 * lookup would trade a cheap problem for an expensive one.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only. The count is what transfers.
 */
public class ServerlessReadAheadTests extends OpenSearchTestCase {

    private static final int BLOCK = 4096;
    private static final String TERM_DIR = "t=1";
    private static final String FILE = "_0.cfs";

    /** Publishes one blob of {@code size} bytes and returns a directory reading it through the cache. */
    private BlockCacheDirectory publish(CountingBlobStore store, BlockCache cache, int size) throws Exception {
        final BlobPath shardBase = BlobPath.cleanPath().add("segments").add("alpha#uuid#0");
        final byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) (i % 251);
        }
        store.blobContainer(shardBase.add(TERM_DIR)).writeBlob(FILE, new ByteArrayInputStream(bytes), size, false);

        final CommitManifest manifest = new CommitManifest(1L, Map.of(FILE, TERM_DIR), "writer", Map.of(FILE, (long) size));
        final Directory local = new ByteBuffersDirectory();
        return BlockCacheDirectory.create(local, store, shardBase, cache, "alpha#uuid#0", manifest);
    }

    /**
     * A cold sequential read costs far fewer requests than it touches blocks.
     *
     * <p>Read one byte at a time, deliberately: that is what a buffered Lucene read looks like from
     * underneath, and it is the pattern that used to turn one block into one request no matter how plainly
     * the reader was walking forward.
     */
    public void testAColdSequentialReadCostsFarFewerRequestsThanBlocks() throws Exception {
        final int size = 512 * BLOCK;
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final BlockCache cache = new BlockCache(BLOCK, 4096);
        try (BlockCacheDirectory directory = publish(store, cache, size)) {
            store.reset();
            try (IndexInput in = directory.openInput(FILE, IOContext.DEFAULT)) {
                for (long i = 0; i < size; i++) {
                    assertEquals("byte " + i, (byte) (i % 251), in.readByte());
                }
            }
            final long reads = store.blobReads();
            final int blocks = size / BLOCK;
            logger.info("read-ahead: a cold sequential read of {} blocks cost {} object-store reads", blocks, reads);

            // The claim, as a ratio rather than a count: the ramp's exact shape is an implementation
            // detail, and one request per block is what it must not be.
            assertTrue("a sequential scan must not cost a request per block: " + reads + " for " + blocks, reads < blocks / 4);
            assertTrue("and must still have read something", reads > 0);
        }
    }

    /**
     * Random reads cost what they always did, because over-fetching for a seek is the trade this must not
     * make.
     *
     * <p>Every read here is a block apart and out of order, so no miss ever follows the last fetch. The
     * detector stays at one block and the request count matches the blocks actually touched.
     */
    public void testRandomReadsDoNotOverFetch() throws Exception {
        final int size = 512 * BLOCK;
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final BlockCache cache = new BlockCache(BLOCK, 4096);
        try (BlockCacheDirectory directory = publish(store, cache, size)) {
            store.reset();
            // Strided so that consecutive touches are never consecutive blocks.
            final int[] blocks = { 300, 7, 180, 42, 411, 96, 255, 130 };
            try (IndexInput in = directory.openInput(FILE, IOContext.DEFAULT)) {
                for (int block : blocks) {
                    in.seek((long) block * BLOCK + 11);
                    assertEquals((byte) ((((long) block * BLOCK + 11)) % 251), in.readByte());
                }
            }
            final long reads = store.blobReads();
            logger.info("read-ahead: {} scattered reads cost {} object-store reads", blocks.length, reads);
            assertEquals("a seek must fetch its own block and nothing more", blocks.length, reads);
        }
    }

    /**
     * A scan that follows a seek still ramps, so one jump does not cost the reader its read-ahead for good.
     */
    public void testASeekResetsTheRampAndSequentialReadingEarnsItBack() throws Exception {
        final int size = 512 * BLOCK;
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final BlockCache cache = new BlockCache(BLOCK, 4096);
        try (BlockCacheDirectory directory = publish(store, cache, size)) {
            store.reset();
            try (IndexInput in = directory.openInput(FILE, IOContext.DEFAULT)) {
                in.seek(256L * BLOCK);
                for (long i = 256L * BLOCK; i < size; i++) {
                    assertEquals((byte) (i % 251), in.readByte());
                }
            }
            final long reads = store.blobReads();
            final int blocks = 256;
            logger.info("read-ahead: a scan of {} blocks after a seek cost {} object-store reads", blocks, reads);
            assertTrue("a scan after a seek must still ramp: " + reads + " for " + blocks, reads < blocks / 4);
        }
    }
}
