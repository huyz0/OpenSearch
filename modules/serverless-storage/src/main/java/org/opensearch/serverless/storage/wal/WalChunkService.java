/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.common.bytes.BytesArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node-level WAL chunk writer (rfc-serverless-opensearch.md &sect;6.4): buffers {@link WalRecord}s
 * appended by any number of shards' translogs and, on {@link #flush()}, group-commits everything
 * buffered so far into a single chunk blob under one writer epoch. This is the piece a
 * WAL-backed {@code TranslogFactory} durably appends to instead of (or in addition to) local
 * translog files.
 *
 * <p>Callers control batching: {@link #append} only buffers in memory, {@link #flush} is what
 * actually durably writes a chunk. A caller wanting synchronous durability per operation (as a
 * local translog does) should flush after every append; a caller wanting real group-commit
 * batching across shards should flush on a timer or size threshold instead.
 */
public final class WalChunkService {

    private final BlobContainer blobContainer;
    private final String writerEpoch;
    private final AtomicLong nextChunkSequence = new AtomicLong(0);
    private final List<WalRecord> buffered = new ArrayList<>();

    public WalChunkService(BlobContainer blobContainer, String writerEpoch) {
        this.blobContainer = blobContainer;
        this.writerEpoch = writerEpoch;
    }

    public synchronized void append(WalRecord record) {
        buffered.add(record);
    }

    /**
     * Writes every currently-buffered record into one new chunk blob and clears the buffer.
     * A no-op (no blob written) if nothing is buffered.
     *
     * @return the chunk sequence number written, or -1 if there was nothing to flush
     */
    public synchronized long flush() throws IOException {
        if (buffered.isEmpty()) {
            return -1;
        }
        long chunkSequence = nextChunkSequence.getAndIncrement();
        byte[] chunkBytes = WalChunkWriter.write(buffered);
        String blobName = WalChunkNaming.blobName(writerEpoch, chunkSequence);
        blobContainer.writeBlob(blobName, new BytesArray(chunkBytes).streamInput(), chunkBytes.length, false);
        buffered.clear();
        return chunkSequence;
    }

    public synchronized int bufferedRecordCount() {
        return buffered.size();
    }
}
