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

        service.append(new WalRecord("idx", 0, 0, "a".getBytes("UTF-8")));
        service.append(new WalRecord("idx", 1, 0, "b".getBytes("UTF-8")));
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
        service.append(new WalRecord("idx", 0, 0, "a".getBytes("UTF-8")));
        assertEquals(0, service.flush());
        service.append(new WalRecord("idx", 0, 1, "b".getBytes("UTF-8")));
        assertEquals(1, service.flush());
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
                        service.append(new WalRecord("idx", 0, seqNo, ("v" + seqNo).getBytes("UTF-8")));
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
}
