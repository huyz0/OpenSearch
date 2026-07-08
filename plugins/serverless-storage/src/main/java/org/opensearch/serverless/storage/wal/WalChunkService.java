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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 * <p><b>Per-shard fairness</b> (rfc-serverless-opensearch.md &sect;18 risk #4, "WAL multiplexing
 * fairness"): one node-level chunk mixing many shards' records means one noisy shard's writes
 * could otherwise bloat the shared buffer and delay every other shard's flush/ack indefinitely,
 * since {@link #flush} only fires on the caller's own timer/size trigger, not per shard. If a
 * positive {@code perShardBudgetBytes} is configured, a shard whose own buffered payload bytes
 * (since its last flush) crosses that budget is immediately siphoned into its own dedicated chunk
 * -- written right away, independent of whatever the caller's own flush trigger is doing -- so a
 * noisy shard's records stop accumulating in (and delaying) the shared buffer other shards depend
 * on, without needing every shard to flush early just because one of them is busy.
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
    private final long perShardBudgetBytes;
    private final AtomicLong nextChunkSequence;
    private final List<WalRecord> buffered = new ArrayList<>();
    private final Map<ShardKey, Long> bufferedBytesByShard = new HashMap<>();

    public WalChunkService(BlobContainer blobContainer, String writerEpoch) throws IOException {
        this(blobContainer, writerEpoch, -1);
    }

    /** @param perShardBudgetBytes see the class javadoc's "Per-shard fairness" section; {@code <= 0} disables the budget entirely. */
    public WalChunkService(BlobContainer blobContainer, String writerEpoch, long perShardBudgetBytes) throws IOException {
        this.blobContainer = blobContainer;
        this.writerEpoch = writerEpoch;
        this.perShardBudgetBytes = perShardBudgetBytes;
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

    public synchronized void append(WalRecord record) throws IOException {
        buffered.add(record);
        if (perShardBudgetBytes > 0) {
            ShardKey key = new ShardKey(record.indexUuid(), record.shardId());
            long newTotal = bufferedBytesByShard.merge(key, (long) record.payload().length, Long::sum);
            if (newTotal >= perShardBudgetBytes) {
                overflowShardToDedicatedChunk(key);
            }
        }
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
        long chunkSequence = writeChunk(buffered);
        buffered.clear();
        bufferedBytesByShard.clear();
        return chunkSequence;
    }

    public synchronized int bufferedRecordCount() {
        return buffered.size();
    }

    /**
     * Pulls every currently-buffered record belonging to {@code key} out of the shared buffer and
     * writes them into their own dedicated chunk immediately, leaving every other shard's buffered
     * records (and their own per-shard byte counters) untouched.
     */
    private void overflowShardToDedicatedChunk(ShardKey key) throws IOException {
        List<WalRecord> shardRecords = new ArrayList<>();
        Iterator<WalRecord> iterator = buffered.iterator();
        while (iterator.hasNext()) {
            WalRecord record = iterator.next();
            if (key.matches(record)) {
                shardRecords.add(record);
                iterator.remove();
            }
        }
        bufferedBytesByShard.remove(key);
        if (shardRecords.isEmpty() == false) {
            writeChunk(shardRecords);
        }
    }

    private long writeChunk(List<WalRecord> records) throws IOException {
        long chunkSequence = nextChunkSequence.getAndIncrement();
        byte[] chunkBytes = WalChunkWriter.write(records);
        String blobName = WalChunkNaming.blobName(writerEpoch, chunkSequence);
        blobContainer.writeBlob(blobName, new BytesArray(chunkBytes).streamInput(), chunkBytes.length, false);
        return chunkSequence;
    }

    private static final class ShardKey {
        private final String indexUuid;
        private final int shardId;

        ShardKey(String indexUuid, int shardId) {
            this.indexUuid = indexUuid;
            this.shardId = shardId;
        }

        boolean matches(WalRecord record) {
            return record.belongsTo(indexUuid, shardId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ShardKey)) return false;
            ShardKey shardKey = (ShardKey) o;
            return shardId == shardKey.shardId && indexUuid.equals(shardKey.indexUuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexUuid, shardId);
        }
    }
}
