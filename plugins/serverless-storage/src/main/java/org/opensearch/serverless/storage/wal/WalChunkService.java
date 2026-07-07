/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
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
 *
 * <p>The chunk sequence for a given {@code writerEpoch} resumes from whatever is already present
 * in the blob container rather than always starting at 0: a caller must pick a {@code
 * writerEpoch} that is unique to one actual writer lifetime (e.g. derived from the shard's
 * primary term, not reused verbatim across a plain process restart under the same term), but a
 * fresh {@link WalChunkService} instance constructed against a container that already holds
 * chunks for that epoch (e.g. after a process restart before the term changed) will not overwrite
 * them.
 */
public final class WalChunkService {

    private final BlobContainer blobContainer;
    private final String writerEpoch;
    private final AtomicLong nextChunkSequence;
    private final List<WalRecord> buffered = new ArrayList<>();

    public WalChunkService(BlobContainer blobContainer, String writerEpoch) throws IOException {
        this.blobContainer = blobContainer;
        this.writerEpoch = writerEpoch;
        this.nextChunkSequence = new AtomicLong(firstUnusedChunkSequence(blobContainer, writerEpoch));
    }

    private static long firstUnusedChunkSequence(BlobContainer blobContainer, String writerEpoch) throws IOException {
        long maxExisting = -1;
        for (BlobMetadata blob : blobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).values()) {
            long sequence = WalChunkNaming.parseChunkSequence(blob.name());
            if (sequence > maxExisting) {
                maxExisting = sequence;
            }
        }
        return maxExisting + 1;
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
