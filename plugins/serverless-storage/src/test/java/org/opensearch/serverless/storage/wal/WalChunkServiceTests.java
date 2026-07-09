/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WalChunkServiceTests extends OpenSearchTestCase {

    private BlobContainer newBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testFlushWithNothingBufferedIsANoOp() throws Exception {
        WalChunkService service = new WalChunkService(newBlobContainer(), "epoch-0");
        assertEquals(-1, service.flush());
    }

    public void testFlushWritesAllBufferedRecordsIntoOneChunk() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");

        service.append(new WalRecord("idx", 0, 1, 0, "a".getBytes("UTF-8")));
        service.append(new WalRecord("idx", 1, 1, 0, "b".getBytes("UTF-8")));
        assertEquals(2, service.bufferedRecordCount());

        long chunkSeq = service.flush();
        assertEquals(0, chunkSeq);
        assertEquals(0, service.bufferedRecordCount());

        byte[] chunkBytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 0))) {
            chunkBytes = in.readAllBytes();
        }
        List<WalRecord> records = WalChunkReader.readRecords(chunkBytes);
        assertEquals(2, records.size());
    }

    public void testSuccessiveFlushesGetIncreasingChunkSequences() throws Exception {
        WalChunkService service = new WalChunkService(newBlobContainer(), "epoch-0");
        service.append(new WalRecord("idx", 0, 1, 0, "a".getBytes("UTF-8")));
        assertEquals(0, service.flush());
        service.append(new WalRecord("idx", 0, 1, 1, "b".getBytes("UTF-8")));
        assertEquals(1, service.flush());
    }

    public void testResumesChunkSequenceFromExistingBlobsUnderTheSameEpoch() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService firstLifetime = new WalChunkService(blobContainer, "epoch-0");
        firstLifetime.append(new WalRecord("idx", 0, 1, 0, "a".getBytes("UTF-8")));
        assertEquals(0, firstLifetime.flush());
        firstLifetime.append(new WalRecord("idx", 0, 1, 1, "b".getBytes("UTF-8")));
        assertEquals(1, firstLifetime.flush());

        // A new instance against the same container/epoch (e.g. after a process restart) must
        // not restart the sequence at 0 and overwrite the chunks the prior instance wrote.
        WalChunkService secondLifetime = new WalChunkService(blobContainer, "epoch-0");
        secondLifetime.append(new WalRecord("idx", 0, 1, 2, "c".getBytes("UTF-8")));
        assertEquals(2, secondLifetime.flush());

        byte[] chunk0Bytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 0))) {
            chunk0Bytes = in.readAllBytes();
        }
        assertEquals(1, WalChunkReader.readRecords(chunk0Bytes).size());
        assertEquals(0L, WalChunkReader.readRecords(chunk0Bytes).get(0).seqNo());
    }

    public void testConcurrentAppendsAreAllCapturedByFlush() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");
        int recordCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(recordCount);
        CountDownLatch startLine = new CountDownLatch(1);
        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                final int seqNo = i;
                futures.add(executor.submit(() -> {
                    try {
                        startLine.await();
                        service.append(new WalRecord("idx", 0, 1, seqNo, ("v" + seqNo).getBytes("UTF-8")));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            startLine.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals(recordCount, service.bufferedRecordCount());
        service.flush();

        byte[] chunkBytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 0))) {
            chunkBytes = in.readAllBytes();
        }
        assertEquals(recordCount, WalChunkReader.readRecords(chunkBytes).size());
    }

    public void testWithNoBudgetConfiguredANoisyShardDoesNotOverflowEarly() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");
        for (int i = 0; i < 100; i++) {
            service.append(new WalRecord("noisy-idx", 0, 1, i, new byte[1000]));
        }
        assertEquals("with no budget, everything just accumulates in the shared buffer", 100, service.bufferedRecordCount());
    }

    public void testANoisyShardOverBudgetIsSiphonedIntoItsOwnDedicatedChunkImmediately() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0", 100);

        service.append(new WalRecord("quiet-idx", 0, 1, 0, "small".getBytes("UTF-8")));
        // Crosses the 100-byte budget for this shard on this append -- must overflow immediately,
        // independent of any flush() call.
        service.append(new WalRecord("noisy-idx", 0, 1, 0, new byte[150]));

        // The noisy shard's record was written out as its own dedicated chunk (sequence 0) without
        // any flush() call; only the quiet shard's record remains buffered.
        assertEquals(1, service.bufferedRecordCount());

        byte[] overflowChunkBytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 0))) {
            overflowChunkBytes = in.readAllBytes();
        }
        List<WalRecord> overflowRecords = WalChunkReader.readRecords(overflowChunkBytes);
        assertEquals(1, overflowRecords.size());
        assertEquals("noisy-idx", overflowRecords.get(0).indexUuid());

        // The quiet shard's record is still in the normal shared buffer, unaffected, and the next
        // flush() gets the next chunk sequence after the overflow chunk's.
        long chunkSeq = service.flush();
        assertEquals(1, chunkSeq);
        byte[] normalChunkBytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 1))) {
            normalChunkBytes = in.readAllBytes();
        }
        List<WalRecord> normalRecords = WalChunkReader.readRecords(normalChunkBytes);
        assertEquals(1, normalRecords.size());
        assertEquals("quiet-idx", normalRecords.get(0).indexUuid());
    }

    public void testOverflowingOneShardDoesNotDisturbAnotherShardsAlreadyBufferedRecords() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0", 100);

        service.append(new WalRecord("shard-a", 0, 1, 0, new byte[40]));
        service.append(new WalRecord("shard-b", 0, 1, 0, new byte[40]));
        // Pushes shard-a's cumulative total to 40+70=110, over budget -- only shard-a overflows.
        service.append(new WalRecord("shard-a", 0, 1, 1, new byte[70]));

        assertEquals("only shard-b's record should remain buffered", 1, service.bufferedRecordCount());
        service.flush();
        byte[] chunkBytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 1))) {
            chunkBytes = in.readAllBytes();
        }
        List<WalRecord> records = WalChunkReader.readRecords(chunkBytes);
        assertEquals(1, records.size());
        assertEquals("shard-b", records.get(0).indexUuid());
    }

    public void testAfterOverflowTheShardsByteCounterResetsSoItCanAccumulateAgain() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0", 100);

        service.append(new WalRecord("idx", 0, 1, 0, new byte[150])); // overflow #1 -> chunk 0
        service.append(new WalRecord("idx", 0, 1, 1, new byte[150])); // overflow #2 -> chunk 1, not accumulated onto the first

        assertEquals(0, service.bufferedRecordCount());
        byte[] secondOverflowBytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 1))) {
            secondOverflowBytes = in.readAllBytes();
        }
        assertEquals(1, WalChunkReader.readRecords(secondOverflowBytes).size());
    }

    public void testFlushClearsPerShardByteCountersToo() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0", 100);

        service.append(new WalRecord("idx", 0, 1, 0, new byte[60]));
        service.flush();
        // If the per-shard counter weren't cleared on flush, this append (60 more bytes, 120
        // cumulative) would incorrectly trigger an overflow instead of just buffering normally.
        service.append(new WalRecord("idx", 0, 1, 1, new byte[60]));

        assertEquals(1, service.bufferedRecordCount());
    }

    /**
     * The fix for a real bug this class used to have (see rfc-serverless-opensearch.md &sect;6.4's
     * own status note): two {@link WalChunkService} instances sharing one container -- exactly
     * what happens when two nodes in the same cluster both have WAL mirroring enabled against the
     * same shared {@code serverless_storage.base_path}, since {@code ServerlessStoragePlugin}
     * constructs the WAL container from that one shared root with no per-node scoping -- used to
     * each compute their own "first unused chunk sequence" independently and increment it locally
     * with no cross-instance coordination, silently overwriting each other's chunks under real
     * concurrent use. Chunk sequence allocation now goes through {@code blobContainer}'s own {@link
     * org.opensearch.common.blobstore.BlobContainer#compareAndSwapRegister}, so two instances
     * writing concurrently against the same container get genuinely distinct sequences instead.
     */
    public void testTwoInstancesSharingOneContainerGetDistinctChunkSequencesNotOverwritingEachOther() throws Exception {
        BlobContainer sharedContainer = newBlobContainer();
        // Two independent "node incarnations" -- distinct writerEpoch strings, exactly as
        // ServerlessStoragePlugin#createComponents constructs a fresh UUIDs.base64UUID() epoch per
        // node -- both against the SAME shared container, both starting fresh.
        WalChunkService serviceA = new WalChunkService(sharedContainer, "node-a-epoch");
        WalChunkService serviceB = new WalChunkService(sharedContainer, "node-b-epoch");

        serviceA.append(new WalRecord("idx", 0, 1, 0, "from-node-a".getBytes("UTF-8")));
        long chunkSequenceA = serviceA.flush();

        serviceB.append(new WalRecord("idx", 0, 1, 0, "from-node-b".getBytes("UTF-8")));
        long chunkSequenceB = serviceB.flush();

        assertNotEquals(
            "two concurrently-writing instances must never be assigned the same chunk sequence",
            chunkSequenceA,
            chunkSequenceB
        );

        byte[] chunkABytes;
        try (InputStream in = sharedContainer.readBlob(WalChunkNaming.blobName("node-a-epoch", chunkSequenceA))) {
            chunkABytes = in.readAllBytes();
        }
        List<WalRecord> chunkARecords = WalChunkReader.readRecords(chunkABytes);
        assertEquals(1, chunkARecords.size());
        assertEquals("from-node-a", new String(chunkARecords.get(0).payload(), "UTF-8"));

        byte[] chunkBBytes;
        try (InputStream in = sharedContainer.readBlob(WalChunkNaming.blobName("node-b-epoch", chunkSequenceB))) {
            chunkBBytes = in.readAllBytes();
        }
        List<WalRecord> chunkBRecords = WalChunkReader.readRecords(chunkBBytes);
        assertEquals(1, chunkBRecords.size());
        assertEquals(
            "node B's write must not have overwritten node A's chunk -- both must independently survive",
            "from-node-b",
            new String(chunkBRecords.get(0).payload(), "UTF-8")
        );
    }

    public void testCurrentChunkSequenceUpperBoundReflectsConcurrentWritersLiveNotCached() throws Exception {
        BlobContainer sharedContainer = newBlobContainer();
        WalChunkService serviceA = new WalChunkService(sharedContainer, "node-a-epoch");
        WalChunkService serviceB = new WalChunkService(sharedContainer, "node-b-epoch");

        assertEquals(0L, serviceA.currentChunkSequenceUpperBound());

        serviceB.append(new WalRecord("idx", 0, 1, 0, "from-node-b".getBytes("UTF-8")));
        serviceB.flush();

        // serviceA never wrote anything itself, but must see serviceB's write reflected live --
        // a locally cached value would wrongly still report 0 here.
        assertEquals(1L, serviceA.currentChunkSequenceUpperBound());
    }
}
