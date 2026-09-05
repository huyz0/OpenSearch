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
     * The node-shared group-commit processor attached to this service when batching is enabled, or
     * {@code null} on the legacy synchronous path -- see {@link #batchingProcessor()}. Volatile
     * because it is attached ({@link #attachBatchingProcessor}) on one thread during plugin wiring
     * and read from any writer shard's translog thread thereafter.
     */
    private volatile WalBatchingProcessor batchingProcessor;

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

    /** The configured per-shard fairness budget, exposed for tests confirming node-setting wiring; not part of the read/write API. */
    public long perShardBudgetBytesForTesting() {
        return perShardBudgetBytes;
    }

    /**
     * Associates the node-shared group-commit processor that drives this service's batching path,
     * called once during plugin wiring right after both objects are constructed (they reference each
     * other -- the processor group-commits into this service, this service hands the processor to
     * every writer shard's translog via {@link #batchingProcessor()}). Only ever set on the
     * node-shared service when {@code serverless_storage.wal_flush.batching.enabled} is on; a
     * dedicated per-shard service is never given one, keeping dedicated WAL streams on the legacy
     * synchronous path.
     *
     * @param processor the group-commit processor to attach.
     */
    public void attachBatchingProcessor(WalBatchingProcessor processor) {
        this.batchingProcessor = processor;
    }

    @Override
    public WalBatchingProcessor batchingProcessor() {
        return batchingProcessor;
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
     * A transient object-store error writing one chunk should not fail the whole engine on the first
     * blip: {@link #writeChunkWithRetry} retries this many times with a short linear backoff before
     * giving up and propagating the failure. This is the retry policy that used to live in {@code
     * WalMirroringTranslog#flushWithRetry} on the caller side, hoisted down to this layer so both the
     * legacy per-operation {@link #flush}/{@link #append} overflow paths and the new
     * group-commit batching path ({@code WalBatchingProcessor}) share exactly one retry
     * implementation rather than each reimplementing it (or, as the overflow path did before this,
     * silently having none at all).
     */
    public static final int MAX_WRITE_ATTEMPTS = 3;

    /** The linear backoff base between {@link #writeChunkWithRetry} attempts -- attempt <em>n</em> waits {@code RETRY_BASE_DELAY_MILLIS * n}. */
    static final long RETRY_BASE_DELAY_MILLIS = 10;

    /** Sentinel for {@link #cachedRegisterGeneration} meaning "we have no idea, read the register". */
    private static final long UNKNOWN_REGISTER_GENERATION = Long.MIN_VALUE;

    /**
     * The last generation {@link #SEQUENCE_REGISTER_NAME} was observed at by this instance -- a pure
     * cost optimisation, never a correctness input; see {@link #claimNextChunkSequence}'s own inline
     * comment for why a stale value here can only ever cost one extra CAS round trip. Volatile
     * rather than atomic because a lost update between two concurrent claimers is harmless (both
     * values are equally valid hints, and whichever loses simply re-learns the truth from its own
     * failed CAS).
     */
    private volatile long cachedRegisterGeneration = UNKNOWN_REGISTER_GENERATION;

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
        // rfc-serverless-opensearch.md &sect;6.4's cost budget (see this field's own javadoc):
        // skipping the readRegister when a previous successful CAS on this instance already told us
        // exactly what the register's generation is takes the steady-state claim from two
        // object-store requests (READ + CAS) down to one (CAS alone) -- a third off the whole legacy
        // per-operation write path, and a third off every group-commit chunk on the batching path.
        // This is safe under any number of concurrent writers for the same reason the retry loop
        // below already is: a wrong expectation does not corrupt anything, it merely loses the CAS,
        // and a lost CAS reports the register's real current generation, which the loop then uses.
        // The cache is therefore a hint that costs one extra CAS round trip when wrong, never a
        // correctness assumption -- unlike the locally-incremented AtomicLong this class used to
        // have (see the class javadoc), which never consulted the register at all.
        long expectedGeneration = cachedRegisterGeneration;
        if (expectedGeneration == UNKNOWN_REGISTER_GENERATION) {
            expectedGeneration = blobContainer.readRegister(SEQUENCE_REGISTER_NAME)
                .map(BlobRegister::generation)
                .orElse(BlobRegister.ABSENT_GENERATION);
        }
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            BlobRegisterCasResult result = blobContainer.compareAndSwapRegister(
                SEQUENCE_REGISTER_NAME,
                expectedGeneration,
                BytesArray.EMPTY
            );
            if (result.applied()) {
                // Generation 1 is the first successful write (ABSENT_GENERATION is 0), so the
                // 0-based chunk sequence it corresponds to is one less.
                cachedRegisterGeneration = result.currentGeneration();
                return result.currentGeneration() - 1;
            }
            // Lost the race -- another instance (this node or another) claimed a sequence between
            // our read and our CAS attempt. The conflict result already reports what the register
            // actually is now; retry against that directly rather than re-reading.
            expectedGeneration = result.currentGeneration();
            cachedRegisterGeneration = expectedGeneration;
        }
        // Give up on the cache too: MAX_CAS_ATTEMPTS consecutive losses means our idea of the
        // register is being invalidated faster than we can act on it, so the next call should pay
        // for a fresh read rather than starting from a value already proven unhelpful.
        cachedRegisterGeneration = UNKNOWN_REGISTER_GENERATION;
        throw new IOException(
            "failed to claim a WAL chunk sequence under writer epoch " + writerEpoch + " after " + MAX_CAS_ATTEMPTS + " CAS attempts"
        );
    }

    /**
     * Buffers {@code record} and, if that pushes its shard over {@link #perShardBudgetBytes},
     * siphons that shard's buffered records into their own dedicated chunk.
     *
     * <p><b>Deliberately not {@code synchronized} as a whole any more.</b> It used to be, and the
     * overflow write ran inside that monitor -- meaning one shard crossing its fairness budget
     * blocked <em>every</em> shard's {@code append} on this node for the duration of an object-store
     * PUT plus its retry backoffs, which is precisely the node-wide bottleneck {@link #flush}'s own
     * javadoc explains it goes out of its way to avoid. The buffer mutation (cheap, in-memory) stays
     * under the monitor; the write happens outside it, exactly as {@link #flush} already does.
     *
     * <p><b>And a failed overflow write no longer destroys records.</b> The old overflow step
     * removed the shard's records from the buffer and then wrote them; if the write ultimately
     * threw, those records -- including any this shard had buffered from <em>earlier</em> appends,
     * which no caller was even waiting on -- were simply
     * gone, silently violating the "a flush failure never loses or duplicates records, a later flush
     * retries them" contract {@link #flush} maintains and {@code
     * WalMirroringTranslog#flushWithRetry} depends on. They are now restored to the front of the
     * buffer (with their byte counters) before the failure is rethrown, so the very next flush
     * retries them.
     *
     * @param record the operation to buffer.
     */
    @Override
    public void append(WalRecord record) throws IOException {
        List<WalRecord> overflow = null;
        synchronized (this) {
            buffered.add(record);
            if (perShardBudgetBytes > 0) {
                ShardKey key = new ShardKey(record.indexUuid(), record.shardId());
                long newTotal = bufferedBytesByShard.merge(key, (long) record.payload().length, Long::sum);
                if (newTotal >= perShardBudgetBytes) {
                    overflow = takeShardRecords(key);
                }
            }
        }
        if (overflow == null || overflow.isEmpty()) {
            return;
        }
        try {
            writeChunkWithRetry(overflow);
        } catch (IOException e) {
            restoreToBufferFront(overflow);
            throw e;
        }
    }

    /**
     * Removes every currently-buffered record belonging to {@code key} from the shared buffer and
     * returns them, clearing that shard's byte counter and leaving every other shard's records and
     * counters untouched. Must be called with this service's monitor held.
     */
    private List<WalRecord> takeShardRecords(ShardKey key) {
        assert Thread.holdsLock(this);
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
        return shardRecords;
    }

    /**
     * Puts {@code records} back at the front of the buffer and re-adds their per-shard byte counts,
     * after a write attempt for them failed -- the same restore {@link #flush} performs, so a
     * failed write never loses records regardless of which path attempted it. Front, not back:
     * these records were appended before anything still in the buffer, and one shard's own records
     * must never replay out of their original append order.
     */
    private synchronized void restoreToBufferFront(List<WalRecord> records) {
        buffered.addAll(0, records);
        if (perShardBudgetBytes > 0) {
            for (WalRecord record : records) {
                bufferedBytesByShard.merge(new ShardKey(record.indexUuid(), record.shardId()), (long) record.payload().length, Long::sum);
            }
        }
    }

    /**
     * Writes every currently-buffered record into one new chunk blob and clears the buffer.
     * A no-op (no blob written) if nothing is buffered.
     *
     * <p>Deliberately does not hold this service's monitor across the actual write: only the
     * cheap in-memory swap (snapshot the buffer, clear it) is synchronized, and {@link
     * #writeChunkWithRetry} -- the network I/O, including its own bounded retry-with-backoff --
     * runs with no lock held, exactly as its own javadoc says is safe (writing a chunk needs no
     * lock of its own since each call claims its own globally-unique chunk sequence). This
     * service is shared by every writer shard on the node (see the class javadoc's "Per-shard
     * fairness" section), and on the legacy synchronous path {@link
     * org.opensearch.serverless.storage.translog.WalMirroringTranslog#add} calls {@link #append}
     * then this method on every single operation -- holding the lock across the write would
     * serialize every shard's indexing threads behind one shard's object-store PUT (plus retries),
     * turning this node-shared buffer into a single bottleneck for the whole node's legacy-path
     * write throughput. Releasing it lets concurrent flushes from different shards actually
     * overlap their I/O, each against its own independently-claimed chunk sequence.
     *
     * <p>On failure (every {@link #writeChunkWithRetry} attempt exhausted), the snapshotted
     * records are put back at the front of the buffer rather than left in {@code toWrite} and
     * discarded -- preserving the existing "a flush failure never loses or duplicates records, a
     * later flush retries them" contract {@code WalMirroringTranslog#flushWithRetry} depends on,
     * now regardless of whatever else was appended to the buffer while this attempt's I/O was in
     * flight.
     *
     * <p><b>Releasing the lock lets two different shards' {@link #flush()} calls race on {@link
     * #claimNextChunkSequence}, so their claimed chunk sequences can land out of "which call
     * started first" order -- this is safe.</b> Nothing about correctness ever depended on chunk
     * sequence order reflecting real time across <em>different</em> shards: replay is filtered
     * per-shard ({@link WalChunkReader#filterByShardAndMinimumTerm}), and {@link
     * #currentChunkSequenceUpperBound} fences on the register's live generation, not on any
     * assumption about which in-flight claim finishes writing first -- a claim not yet made simply
     * cannot be assigned a sequence below the current generation, by construction of the CAS
     * counter itself. What genuinely must stay ordered -- one shard's own records never replaying
     * out of the order they were originally appended in -- still does: {@link #flush} is
     * synchronous (it blocks the caller until the write completes or exhausts retries), and {@code
     * WalMirroringTranslog#add} calls {@link #append} then this method sequentially for every
     * operation on that shard's own indexing thread, so operation N+1's append can never happen
     * until operation N's flush -- claim included -- has fully completed. This is also not a new
     * risk class: the group-commit batching path ({@code WalBatchingProcessor}) has always claimed
     * its shared sequence outside of any single shard's own lock, for the identical reason.
     *
     * @return the chunk sequence number written, or -1 if there was nothing to flush
     */
    @Override
    public long flush() throws IOException {
        List<WalRecord> toWrite;
        synchronized (this) {
            if (buffered.isEmpty()) {
                return -1;
            }
            toWrite = new ArrayList<>(buffered);
            buffered.clear();
            bufferedBytesByShard.clear();
        }
        try {
            return writeChunkWithRetry(toWrite);
        } catch (IOException e) {
            restoreToBufferFront(toWrite);
            throw e;
        }
    }

    @Override
    public synchronized int bufferedRecordCount() {
        return buffered.size();
    }

    /**
     * Total payload bytes currently buffered (since the last {@link #flush}), summed across every
     * shard -- rfc-serverless-opensearch.md &sect;10's still-open "ingest tier: WAL upload backlog"
     * autoscaling signal, the byte-weighted counterpart to {@link #bufferedRecordCount()}. Computed
     * directly from {@link #buffered}, not {@link #bufferedBytesByShard} -- that map is only ever
     * populated when {@link #perShardBudgetBytes} is configured (the fairness-budget feature, off
     * by default), so summing it would silently read as zero for every node running with the
     * default no-budget configuration despite records genuinely being buffered.
     */
    public synchronized long totalBufferedBytes() {
        long total = 0;
        for (WalRecord record : buffered) {
            total += record.payload().length;
        }
        return total;
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
     * {@code blobPath} does, and only {@code blobName} is what {@link #writeChunkWithRetry} uses) -- chunk
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
     * Writes {@code records} into one new chunk blob, retrying a bounded number of times with a
     * short linear backoff (see {@link #MAX_WRITE_ATTEMPTS}) on a transient {@link IOException}
     * before giving up and rethrowing -- never silently swallowed. Shared by the legacy
     * per-operation {@link #flush}/{@link #append} overflow paths (which pass the
     * shared buffer, snapshotted out from under this service's own monitor) and the group-commit
     * batching path ({@code WalBatchingProcessor#write}, which passes its own drained batch):
     * writing a chunk needs no lock of its own since each call claims one globally-unique chunk
     * sequence (see {@link #claimNextChunkSequence}) and writes a uniquely-named blob, so two
     * concurrent writers never collide regardless of which path they came in on.
     *
     * @param records the records to group-commit into one chunk; a no-op returning {@code -1} if empty.
     * @return the chunk sequence number written, or {@code -1} if {@code records} was empty.
     */
    long writeChunkWithRetry(List<WalRecord> records) throws IOException {
        if (records.isEmpty()) {
            return -1;
        }
        // The sequence is claimed ONCE, ahead of the retry loop, and every attempt reuses it.
        //
        // It used to be claimed inside writeChunk, i.e. freshly on every attempt, which had two
        // consequences, one merely wasteful and one a real (if narrow) acked-and-lost bug:
        // * Each failed attempt advanced the shared register without writing anything, burning a
        // sequence and leaving a permanent hole in the chunk-sequence space (tolerated by
        // WalReplayRecovery#listChunkSequencesInRange's per-candidate blobExists probe, but at
        // the cost of a wasted probe per hole forever after, and -- see WalReplayRecovery's own
        // corrupt-tail branch -- a hole can make a mid-stream chunk look like the last one).
        // * More seriously, the retry's record landed at a strictly *higher* sequence than the
        // first attempt claimed. If a new writer snapshotted its activationWalPosition in that
        // window, the successfully-written (and then acked) records fell *above* that fencing
        // cutoff and were silently excluded from replay -- the same acked-and-lost failure mode
        // as D1/D3, reached by a different route.
        // Rewriting the same blob name on a retry is safe: writeBlob is called with
        // failIfAlreadyExists=false, and every attempt writes byte-identical content (the records
        // are serialized once, below, outside the loop), so an attempt that actually succeeded
        // server-side but reported failure to us is simply overwritten with itself.
        long chunkSequence = claimNextChunkSequence();
        byte[] chunkBytes = WalChunkWriter.write(records);
        String blobName = WalChunkNaming.blobName(writerEpoch, chunkSequence);
        for (int attempt = 1; attempt <= MAX_WRITE_ATTEMPTS; attempt++) {
            try {
                blobContainer.writeBlob(blobName, new BytesArray(chunkBytes).streamInput(), chunkBytes.length, false);
                return chunkSequence;
            } catch (IOException e) {
                if (attempt == MAX_WRITE_ATTEMPTS) {
                    throw e;
                }
                try {
                    Thread.sleep(RETRY_BASE_DELAY_MILLIS * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        throw new AssertionError("writeChunkWithRetry loop exited without returning or throwing");
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
