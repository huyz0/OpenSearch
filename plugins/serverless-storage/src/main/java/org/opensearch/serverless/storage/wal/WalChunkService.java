/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.core.common.bytes.BytesArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
 * <p>{@code writerEpoch} identifies this WAL service's own lifetime (this node's incarnation of
 * it), not any one shard's -- corrected here after an earlier draft of this javadoc incorrectly
 * described it as "unique to one actual writer lifetime, e.g. derived from the shard's primary
 * term." That would be self-contradictory: this service buffers records from any number of
 * writer shards on the node into shared chunks (see "Per-shard fairness" above), so its epoch
 * cannot simultaneously be scoped to a single shard's term without abandoning the cross-shard
 * batching that's the entire reason this is a node-level service rather than a per-shard one
 * (rfc-serverless-opensearch.md &sect;6.4's cost-sanity argument). Fencing a superseded writer
 * therefore cannot mean "discard its epoch directory" (that would fence every other shard sharing
 * it); it is a per-record filter instead -- see {@link WalRecord}'s own javadoc.
 *
 * <p><b>Chunk sequence numbers are claimed via {@code blobContainer}'s own {@link
 * BlobContainer#compareAndSwapRegister} primitive</b>, not counted locally: an earlier version of
 * this class seeded a plain {@code AtomicLong} once at construction from whatever the container
 * already held and incremented it purely locally thereafter, which is only safe for a single
 * {@link WalChunkService} instance -- since every node in a cluster with WAL mirroring enabled
 * writes into the exact same shared container (one {@code <base_path>/wal/} root, not scoped per
 * node), two independently-numbered instances writing concurrently would silently overwrite each
 * other's chunks (confirmed with a real regression test before this fix,
 * {@code WalChunkServiceTests#testTwoInstancesSharingOneContainerCanSilentlyOverwriteEachOthersChunks}
 * -- see rfc-serverless-opensearch.md &sect;6.4's own status note for the full writeup). Using the
 * register's own {@link BlobRegister#generation()} as the counter -- "monotonically increasing
 * with each successful write," exactly a distributed atomic counter -- gets real cross-node safety
 * for free from the same primitive {@code ShardStateStore}/{@code DurablePinRegistry} already rely
 * on, with no new storage-backend-specific code.
 */
public final class WalChunkService implements WalAppendTarget {

    /**
     * The register blob {@link #claimNextChunkSequence} and {@link #currentChunkSequenceUpperBound}
     * both use -- shared across every {@link WalChunkService} instance pointed at this container
     * (i.e. cluster-wide, not per-node), which is the entire point: its generation is what makes
     * chunk sequence allocation actually safe under concurrent writers.
     */
    static final String SEQUENCE_REGISTER_NAME = "chunk-sequence";

    private final BlobContainer blobContainer;
    private final String writerEpoch;
    private final long perShardBudgetBytes;
    private final List<WalRecord> buffered = new ArrayList<>();
    private final Map<ShardKey, Long> bufferedBytesByShard = new HashMap<>();

    /**
     * Creates a WAL chunk service with per-shard fairness budgeting disabled.
     *
     * @param blobContainer the shared blob container chunks are written to
     * @param writerEpoch the writer epoch every chunk this service writes is keyed under
     */
    public WalChunkService(BlobContainer blobContainer, String writerEpoch) throws IOException {
        this(blobContainer, writerEpoch, -1);
    }

    /**
     * Creates a WAL chunk service.
     *
     * @param blobContainer the shared blob container chunks are written to
     * @param writerEpoch the writer epoch every chunk this service writes is keyed under
     * @param perShardBudgetBytes see the class javadoc's "Per-shard fairness" section; {@code <= 0} disables the budget entirely.
     */
    public WalChunkService(BlobContainer blobContainer, String writerEpoch, long perShardBudgetBytes) throws IOException {
        this.blobContainer = blobContainer;
        this.writerEpoch = writerEpoch;
        this.perShardBudgetBytes = perShardBudgetBytes;
    }

    /**
     * A bounded retry budget for {@link #claimNextChunkSequence}, matching the same
     * bounded-not-unbounded shape {@link org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry#mutate}
     * already established for CAS retries in this plugin: an unbounded {@code while (true)} loop
     * would let sustained contention (many nodes flushing at once) spin indefinitely rather than
     * failing loudly, which {@link org.opensearch.serverless.storage.translog.WalMirroringTranslog
     * #flushWithRetry}'s own outer retry-with-backoff is already set up to handle gracefully as an
     * ordinary transient failure.
     */
    private static final int MAX_CAS_ATTEMPTS = 50;

    /**
     * Atomically claims the next chunk sequence number, safe under any number of concurrent {@link
     * WalChunkService} instances (any number of nodes) sharing {@link #blobContainer}: retries the
     * compare-and-swap against whatever the register's current generation actually is until one
     * attempt wins, rather than assuming a single locally-cached expectation is still correct
     * (which, under real concurrent writers, it frequently won't be -- that assumption was
     * precisely this class's earlier bug, see the class javadoc). The register's own value is
     * never read for content, only its generation; {@link org.opensearch.core.common.bytes.BytesArray#EMPTY}
     * is written every time. Only the very first attempt needs a separate {@link
     * BlobContainer#readRegister} call -- a failed {@link BlobContainer#compareAndSwapRegister}
     * already reports the generation that beat it, in {@link BlobRegisterCasResult#currentGeneration()},
     * so every retry after the first can use that directly instead of paying for another read.
     */
    private long claimNextChunkSequence() throws IOException {
        long expectedGeneration = blobContainer.readRegister(SEQUENCE_REGISTER_NAME)
            .map(BlobRegister::generation)
            .orElse(BlobRegister.ABSENT_GENERATION);
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            BlobRegisterCasResult result = blobContainer.compareAndSwapRegister(
                SEQUENCE_REGISTER_NAME,
                expectedGeneration,
                BytesArray.EMPTY
            );
            if (result.applied()) {
                // Generation 1 is the first successful write (ABSENT_GENERATION is 0), so the
                // 0-based chunk sequence it corresponds to is one less.
                return result.currentGeneration() - 1;
            }
            // Lost the race -- another instance (this node or another) claimed a sequence between
            // our read and our CAS attempt. The conflict result already reports what the register
            // actually is now; retry against that directly rather than re-reading.
            expectedGeneration = result.currentGeneration();
        }
        throw new IOException(
            "failed to claim a WAL chunk sequence under writer epoch " + writerEpoch + " after " + MAX_CAS_ATTEMPTS + " CAS attempts"
        );
    }

    @Override
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
    @Override
    public synchronized long flush() throws IOException {
        if (buffered.isEmpty()) {
            return -1;
        }
        long chunkSequence = writeChunk(buffered);
        buffered.clear();
        bufferedBytesByShard.clear();
        return chunkSequence;
    }

    @Override
    public synchronized int bufferedRecordCount() {
        return buffered.size();
    }

    /** The epoch every chunk this service writes is keyed under -- see this class's own javadoc for what it identifies. */
    public String writerEpoch() {
        return writerEpoch;
    }

    /**
     * The underlying blob container every chunk under this epoch is written to -- what {@link
     * org.opensearch.serverless.storage.wal.WalReplayRecovery} reads chunks back from during
     * activation replay. Note this container is not itself scoped per-epoch on disk (see {@link
     * WalChunkNaming}'s javadoc: {@code blobName} does not actually incorporate the epoch, only
     * {@code blobPath} does, and only {@code blobName} is what {@link #writeChunk} uses) -- chunk
     * sequence numbers are the real, globally-continuous ordering key this service and {@code
     * WalReplayRecovery} both rely on, not the epoch string.
     */
    public BlobContainer blobContainer() {
        return blobContainer;
    }

    /**
     * The chunk sequence that will be used <em>next</em>, under this epoch -- i.e. an exclusive
     * upper bound on every chunk sequence written so far. This is the real-world analogue of
     * {@code WalReplayFencing.tla}'s {@code Len(wal)}: a writer activating under a new term can
     * snapshot this value as early as possible (see {@code ObjectStoreWriterEngine}'s own use of
     * it) as a fencing bound for a future replay -- anything appended at or after this sequence is
     * excluded regardless of what term it claims, closing the gap a term-only filter (see {@link
     * WalChunkReader#filterByShardAndMinimumTerm}) leaves open on its own. See that model's own
     * STATUS note for the full verification result and what's still not wired to consume this.
     *
     * <p>A live read of {@link #SEQUENCE_REGISTER_NAME}'s current generation, deliberately not a
     * locally cached value: since the register is the real, cross-node-shared source of truth (see
     * {@link #claimNextChunkSequence}), a stale local cache could under-report this bound and let a
     * fencing snapshot silently exclude chunks that genuinely existed before activation -- exactly
     * the failure mode {@code WalReplayFencing.tla} exists to rule out.
     */
    public long currentChunkSequenceUpperBound() throws IOException {
        return blobContainer.readRegister(SEQUENCE_REGISTER_NAME).map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
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
        long chunkSequence = claimNextChunkSequence();
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
