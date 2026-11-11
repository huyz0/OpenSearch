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
import org.opensearch.serverless.store.NodeLogBlob;
import org.opensearch.serverless.store.NodeLogBlob.Range;
import org.opensearch.serverless.store.NodeLogBlob.ShardBatch;
import org.opensearch.serverless.store.NodeLogBlob.ShardKey;
import org.opensearch.serverless.store.NodeLogStore;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** A node's write log: one blob per window holding every shard, an index in each header, fences and deletes by name. */
public class NodeLogStoreTests extends OpenSearchTestCase {

    private static final ShardKey A0 = new ShardKey("uuid-a", 0);
    private static final ShardKey A1 = new ShardKey("uuid-a", 1);
    private static final ShardKey B0 = new ShardKey("uuid-b", 0);

    private static WalRecord record(String id, long seqNo) {
        return new WalRecord(id, "{\"n\":" + seqNo + "}", seqNo, 1L, 1L);
    }

    private static List<String> ids(List<WalRecord> records) {
        final List<String> out = new ArrayList<>();
        for (WalRecord record : records) {
            out.add(record.id());
        }
        return out;
    }

    public void testOneBlobHoldsEveryShardAndEachSliceReadsAlone() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final NodeLogBlob.Encoded encoded = NodeLogBlob.encode(
                "node-1",
                3L,
                0L,
                0L,
                List.of(
                    new ShardBatch(A0, 2L, List.of(record("a", 7), record("b", 8))),
                    new ShardBatch(B0, 5L, List.of(record("c", 1))),
                    new ShardBatch(A1, 2L, List.of(WalRecord.deletion("d", 4, 2, 2)))
                ),
                Map.of()
            );
            store.reset();
            log.append(3L, 0L, encoded.bytes());
            assertEquals("a window of three shards is one PUT", 1L, store.blobWrites());

            store.reset();
            final NodeLogBlob.Header header = (NodeLogBlob.Header) log.readEntry(3L, 0L).orElseThrow();
            assertEquals("a header is one ranged read", 1L, store.blobReads());
            assertEquals(encoded.header(), header);
            assertEquals(3, header.slices().size());
            assertEquals(List.of(new Range(0, 0)), header.ordinalsOf(B0));

            store.reset();
            final NodeLogBlob.Slice a0 = header.slicesOf(A0).get(0);
            assertEquals(List.of("a", "b"), ids(log.readSlice(3L, header, a0)));
            assertEquals(8L, a0.maxSeqNo());
            assertEquals(2L, a0.term());
            assertEquals("one shard's slice is one ranged read", 1L, store.blobReads());
            final List<WalRecord> deletions = log.readSlice(3L, header, header.slicesOf(A1).get(0));
            assertTrue(deletions.get(0).isDeletion());
            assertEquals(List.of("c"), ids(log.readSlice(3L, header, header.slicesOf(B0).get(0))));
        }
    }

    public void testTheRunningIndexDescribesTheLogFromTheTrimPoint() throws Exception {
        Map<ShardKey, List<Range>> index = Map.of();
        NodeLogBlob.Header last = null;
        // A0 in every blob, B0 in blobs 1 and 3, A1 only in blob 0.
        for (long ordinal = 0; ordinal < 5; ordinal++) {
            final List<ShardBatch> batches = new ArrayList<>();
            batches.add(new ShardBatch(A0, 1L, List.of(record("a" + ordinal, ordinal))));
            if (ordinal == 1 || ordinal == 3) {
                batches.add(new ShardBatch(B0, 1L, List.of(record("b" + ordinal, ordinal))));
            }
            if (ordinal == 0) {
                batches.add(new ShardBatch(A1, 1L, List.of(record("x", 0))));
            }
            final long trimPoint = ordinal >= 3 ? 2L : 0L;
            last = NodeLogBlob.encode("node-1", 0L, ordinal, trimPoint, batches, index).header();
            index = last.index();
        }
        assertEquals(List.of(new Range(2, 4)), last.ordinalsOf(A0));
        assertEquals("a run below the trim point is gone, the rest stays", List.of(new Range(3, 3)), last.ordinalsOf(B0));
        assertEquals("a shard with nothing at or above the trim point leaves the index", List.of(), last.ordinalsOf(A1));
        assertEquals(2L, last.trimPoint());
    }

    public void testAFenceStopsTheWriterAndEveryBlobItLandedIsBelowIt() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            for (long ordinal = 0; ordinal < 37; ordinal++) {
                log.append(1L, ordinal, blob(ordinal));
            }
            store.reset();
            final long fence = log.fence(1L, 5L, "node-2", 9L);
            assertEquals("the fence takes the first free slot", 37L, fence);
            assertEquals("found by name, not by listing", 0L, store.listings());
            assertEquals("and one PUT", 1L, store.blobWrites());
            assertTrue(log.readEntry(1L, fence).orElseThrow() instanceof NodeLogBlob.Fence);

            expectThrows(FileAlreadyExistsException.class, () -> log.append(1L, 37L, blob(37L)));
            assertEquals(36L, log.lastHeaderBelow(1L, fence, 0L).orElseThrow().ordinal());

            // A second successor shares the fence rather than adding one.
            assertEquals(37L, log.fence(1L, 0L, "node-3", 9L));
        }
    }

    public void testAFenceRacingTheWriterLandsAfterEverythingTheWriterLanded() throws Exception {
        for (int round = 0; round < 20; round++) {
            try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
                final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
                final AtomicLong landed = new AtomicLong(-1L);
                final AtomicReference<Exception> stopped = new AtomicReference<>();
                final CountDownLatch started = new CountDownLatch(1);
                final java.util.concurrent.atomic.AtomicBoolean done = new java.util.concurrent.atomic.AtomicBoolean();
                final Thread writer = new Thread(() -> {
                    try {
                        for (long ordinal = 0; ordinal < 100_000 && done.get() == false; ordinal++) {
                            log.append(2L, ordinal, blob(ordinal));
                            landed.set(ordinal);
                            if (ordinal == 3) {
                                started.countDown();
                            }
                            // A writer appends once per window; this one is far faster, but not unboundedly so.
                            Thread.sleep(1);
                        }
                    } catch (Exception e) {
                        stopped.set(e);
                    } finally {
                        started.countDown();
                    }
                });
                writer.start();
                final long fence;
                try {
                    started.await();
                    fence = log.fence(2L, 0L, "node-2", 3L);
                } catch (Exception e) {
                    // The fence failed: stop the writer rather than wait out its loop.
                    done.set(true);
                    writer.join();
                    throw e;
                }
                // The writer stops at the fence on its next append.
                writer.join();
                assertTrue("the writer is stopped by the fence: " + stopped.get(), stopped.get() instanceof FileAlreadyExistsException);
                assertEquals("every blob the writer landed is below the fence", fence - 1, landed.get());
            }
        }
    }

    public void testDeletesGoByNameInBatches() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            for (long ordinal = 0; ordinal < 1205; ordinal++) {
                log.append(4L, ordinal, blob(ordinal));
            }
            store.reset();
            log.deleteRange(4L, 0L, 1200L);
            assertEquals("1,200 names in two requests", 2L, store.deletes());
            assertEquals("and no listing", 0L, store.listings());
            assertTrue(log.readEntry(4L, 1199L).isEmpty());
            assertTrue(log.readEntry(4L, 1200L).isPresent());
        }
    }

    public void testAHeaderLongerThanTheFirstReadTakesASecond() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            // Enough shards in one window that the header outgrows the first read.
            final List<ShardBatch> batches = new ArrayList<>();
            final Map<ShardKey, List<Range>> index = new HashMap<>();
            for (int shard = 0; shard < 3000; shard++) {
                batches.add(new ShardBatch(new ShardKey("uuid-" + shard, shard % 100), 1L, List.of(record("id" + shard, shard))));
            }
            final NodeLogBlob.Encoded encoded = NodeLogBlob.encode("node-1", 0L, 0L, 0L, batches, index);
            assertTrue("the fixture needs a long header", encoded.header().bodyOffset() > 64 * 1024);
            log.append(0L, 0L, encoded.bytes());
            store.reset();
            final NodeLogBlob.Header header = (NodeLogBlob.Header) log.readEntry(0L, 0L).orElseThrow();
            assertEquals(2L, store.blobReads());
            assertEquals(3000, header.slices().size());
            final NodeLogBlob.Slice last = header.slices().get(2999);
            assertEquals(List.of("id2999"), ids(log.readSlice(0L, header, last)));
        }
    }

    private static byte[] blob(long ordinal) throws Exception {
        return NodeLogBlob.encode(
            "node-1",
            0L,
            ordinal,
            0L,
            List.of(new ShardBatch(A0, 1L, List.of(record("r" + ordinal, ordinal)))),
            Map.of()
        ).bytes();
    }
}
