/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.serverless.store.NodeLogBlob.Header;
import org.opensearch.serverless.store.NodeLogBlob.Range;
import org.opensearch.serverless.store.NodeLogBlob.ShardKey;
import org.opensearch.serverless.store.NodeLogBlob.Slice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What a successor reads of a predecessor's write log: every epoch its lease names fenced, then any shard's records.
 *
 * <p><b>Fence first, once.</b> The first read fences every epoch the lease names, so the predecessor can land nothing
 * after what is read. Every shard taken from that predecessor shares those fences, and a second successor taking another
 * of its shards shares them too, by {@link NodeLogStore#fence}. Each epoch's last header below its fence is then read
 * once: its running index says which blobs hold each shard's unpublished records.
 *
 * <p><b>Then by shard.</b> A shard's records are read blob by blob from that index: one ranged read for each blob's
 * header, kept for the other shards in the same blob, and one for each of the shard's slices. Nothing lists the store.
 *
 * <p>Only what is unpublished is read. A blob below an epoch's trim point holds nothing a shard still needs, so the
 * index no longer names it. A record the restored commit already holds is skipped by recovery, which compares sequence
 * numbers, so reading a little more than needed costs a read and never a duplicate.
 *
 * <p>Built after the successor has moved the shard's head and read the predecessor's lease, in that order: an epoch the
 * predecessor opens after the lease was read is one it opened after the head moved, and its ownership check before
 * writing refuses the shard.
 */
public final class NodeLogTakeover {

    /** How many times a fence is searched for again after the predecessor's lease moved past it. */
    static final int MAX_REFENCES = 8;

    private final NodeLogStore log;
    private final List<NodeLogEpoch> epochs;
    private final String fencedBy;
    private final long term;

    /** Per epoch, the last header below its fence, or empty if it held no records above its trim point. */
    private volatile Map<Long, Optional<Header>> lastHeaders;
    /** Per epoch, the slot its fence took, or its first free slot when nothing was fenced. */
    private volatile Map<Long, Long> ends;
    /** Headers read for one shard, kept for the others in the same blob. */
    private final Map<Long, Map<Long, Header>> headers = new ConcurrentHashMap<>();

    /** The largest body read whole, rather than slice by slice. */
    static final long MAX_BODY_BYTES = 8L << 20;
    /** How many bytes of bodies one reader keeps at most. */
    static final long MAX_KEPT_BYTES = 64L << 20;
    /** How long bodies are kept from the first one read: a takeover's opens read them within seconds of each other. */
    static final long KEEP_BODIES_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(60);

    /**
     * Bodies read whole, per epoch and slot. A takeover of many shards from one node finds most of their records, and
     * the commits logged for them, in the same few blobs: read slice by slice, that is one read per shard per blob;
     * read whole, one per blob.
     */
    private final Map<BodyKey, java.util.concurrent.CompletableFuture<byte[]>> bodies = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong keptBytes = new java.util.concurrent.atomic.AtomicLong();
    private volatile long keptSince = Long.MIN_VALUE;

    private record BodyKey(long epoch, long ordinal) {
    }

    /**
     * Creates a reader of one predecessor's log.
     *
     * @param log the predecessor's log
     * @param epochs the epochs its lease names
     * @param fencedBy this node
     * @param term the term this node has just won, recorded in each fence for diagnosis
     */
    public NodeLogTakeover(NodeLogStore log, List<NodeLogEpoch> epochs, String fencedBy, long term) {
        this(log, epochs, fencedBy, term, null);
    }

    /** Reads the predecessor's lease again, for its epochs as they stand now. */
    @FunctionalInterface
    public interface LeaseReader {
        /**
         * Reads the lease.
         *
         * @return the epochs it names now; empty if it is gone
         * @throws IOException if it cannot be read
         */
        List<NodeLogEpoch> epochs() throws IOException;
    }

    private final LeaseReader reread;

    /**
     * Creates a reader of one predecessor's log that checks each fence against the predecessor's lease as it stands after
     * the fence.
     *
     * <p>The predecessor deletes behind a trim point only once a lease naming it has landed, but the lease this was built
     * from may be older than that. Its landed hint can then point into what was deleted, and a fence searching from there
     * lands in a hole below the writer's tail, which the writer never reaches. Any deletion that made such a hole followed
     * a lease naming a trim point above it, so reading the lease after the fence finds a trim point above the fence
     * exactly when the fence may be in a hole, and the fence is searched for again from that lease's hint.
     *
     * @param log the predecessor's log
     * @param epochs the epochs its lease names
     * @param fencedBy this node
     * @param term the term this node has just won, recorded in each fence for diagnosis
     * @param reread reads the predecessor's lease again; null to trust the epochs given, as for this node's own log
     */
    public NodeLogTakeover(NodeLogStore log, List<NodeLogEpoch> epochs, String fencedBy, long term, LeaseReader reread) {
        this(log, epochs, fencedBy, term, reread, true);
    }

    /**
     * Creates a reader of a log whose writer has let go of the shards it will be asked about: it reads up to each epoch's
     * first free slot and fences nothing, so the writer's other shards go on writing. A writer that has let go of a shard
     * writes nothing more of it, so every header written since says whether the shard still has records there.
     *
     * <p>The writer may be deleting behind its trim point meanwhile, so the end of each epoch is checked against its lease
     * as it stands afterwards, as a fence is.
     *
     * @param log the writer's log
     * @param epochs the epochs its lease names
     * @param reread reads the writer's lease again
     * @return the reader
     */
    public static NodeLogTakeover unfenced(NodeLogStore log, List<NodeLogEpoch> epochs, LeaseReader reread) {
        return new NodeLogTakeover(log, epochs, log.nodeId(), 0L, reread, false);
    }

    private final boolean fence;

    private NodeLogTakeover(NodeLogStore log, List<NodeLogEpoch> epochs, String fencedBy, long term, LeaseReader reread, boolean fence) {
        this.fence = fence;
        this.reread = reread;
        this.log = Objects.requireNonNull(log, "log");
        this.epochs = List.copyOf(epochs);
        this.fencedBy = Objects.requireNonNull(fencedBy, "fencedBy");
        this.term = term;
    }

    /**
     * Returns the epochs this reads, as the predecessor's lease named them.
     *
     * @return the epochs
     */
    public List<NodeLogEpoch> epochs() {
        return epochs;
    }

    /**
     * Returns the node whose log this reads.
     *
     * @return the predecessor's id
     */
    public String predecessor() {
        return log.nodeId();
    }

    /**
     * Fences every epoch the lease names and reads each one's last header, if that has not been done.
     *
     * @throws IOException if a fence or a read fails; the next call tries again
     */
    public synchronized void fenceAll() throws IOException {
        if (lastHeaders != null) {
            return;
        }
        final Map<Long, Long> fences = new HashMap<>();
        final Map<Long, Long> floors = new HashMap<>();
        for (NodeLogEpoch epoch : epochs) {
            floors.put(epoch.epoch(), epoch.trimPoint());
            fences.put(
                epoch.epoch(),
                this.fence ? log.fence(epoch.epoch(), epoch.landed(), fencedBy, term) : log.firstFree(epoch.epoch(), epoch.landed())
            );
        }
        if (reread != null) {
            for (int attempt = 0; attempt < MAX_REFENCES; attempt++) {
                boolean moved = false;
                for (NodeLogEpoch now : reread.epochs()) {
                    final Long fence = fences.get(now.epoch());
                    if (fence != null) {
                        // Nothing below a trim point the lease has recorded is needed, so the header search stops there.
                        floors.merge(now.epoch(), now.trimPoint(), Math::max);
                    }
                    if (fence != null && now.trimPoint() > fence) {
                        // Deleted past the fence after the lease this started from: the fence may be in a hole.
                        fences.put(
                            now.epoch(),
                            this.fence ? log.fence(now.epoch(), now.landed(), fencedBy, term) : log.firstFree(now.epoch(), now.landed())
                        );
                        moved = true;
                    }
                }
                if (moved == false) {
                    break;
                }
            }
        }
        final Map<Long, Optional<Header>> found = new ConcurrentHashMap<>();
        for (NodeLogEpoch epoch : epochs) {
            found.put(epoch.epoch(), log.lastHeaderBelow(epoch.epoch(), fences.get(epoch.epoch()), floors.get(epoch.epoch())));
        }
        ends = Map.copyOf(fences);
        lastHeaders = found;
    }

    /**
     * Returns, per epoch, the slot the fence took, or the first free slot for a reader that fences nothing: every slot
     * of the epoch the writer used is below it.
     *
     * @return the slot per epoch
     * @throws IOException if a fence or a read fails
     */
    public Map<Long, Long> ends() throws IOException {
        fenceAll();
        return ends;
    }

    /**
     * Returns every shard any epoch's last header names as holding records not yet published.
     *
     * @return the shards
     * @throws IOException if a fence or a read fails
     */
    public java.util.Set<ShardKey> named() throws IOException {
        fenceAll();
        final java.util.Set<ShardKey> out = new java.util.TreeSet<>();
        for (Optional<Header> last : lastHeaders.values()) {
            last.ifPresent(header -> out.addAll(header.index().keySet()));
        }
        return out;
    }

    /**
     * Returns the highest sequence number of a shard's records here, reading the header of each blob that holds them
     * and no slice.
     *
     * @param shard the shard
     * @return the highest sequence number, or {@link org.opensearch.index.seqno.SequenceNumbers#UNASSIGNED_SEQ_NO} if
     *     none is held
     * @throws IOException if a fence or a read fails
     */
    public long maxSeqNoOf(ShardKey shard) throws IOException {
        fenceAll();
        long max = org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
        for (NodeLogEpoch epoch : epochs) {
            final Optional<Header> last = lastHeaders.get(epoch.epoch());
            if (last.isEmpty()) {
                continue;
            }
            for (Range run : last.get().ordinalsOf(shard)) {
                for (long ordinal = run.from(); ordinal <= run.to(); ordinal++) {
                    final Header header = header(epoch.epoch(), ordinal, last.get());
                    if (header == null) {
                        continue;
                    }
                    for (Slice slice : header.slicesOf(shard)) {
                        max = Math.max(max, slice.maxSeqNo());
                    }
                }
            }
        }
        return max;
    }

    /**
     * Streams a shard's unpublished records from every fenced epoch, oldest first, in the order they must replay.
     *
     * @param shard the shard
     * @param consumer what takes each record
     * @throws IOException if a fence or a read fails
     */
    public void forEachRecord(ShardKey shard, CheckedConsumer<WalRecord, IOException> consumer) throws IOException {
        fenceAll();
        for (NodeLogEpoch epoch : epochs) {
            final Optional<Header> last = lastHeaders.get(epoch.epoch());
            if (last.isEmpty()) {
                continue;
            }
            readRuns(epoch.epoch(), last.get().ordinalsOf(shard), last.get(), shard, consumer);
        }
    }

    /**
     * Streams a shard's unpublished records that landed after a position, as {@link #forEachRecord} does: for a reader
     * that holds a commit covering every record of the shard at or before it.
     *
     * @param shard the shard
     * @param after the position; records in its blob and before are not read
     * @param consumer what takes each record
     * @throws IOException if a fence or a read fails
     */
    public void forEachRecordAfter(ShardKey shard, NodeLogCommitter.Position after, CheckedConsumer<WalRecord, IOException> consumer)
        throws IOException {
        fenceAll();
        for (NodeLogEpoch epoch : epochs) {
            if (epoch.epoch() < after.epoch()) {
                continue;
            }
            final Optional<Header> last = lastHeaders.get(epoch.epoch());
            if (last.isEmpty()) {
                continue;
            }
            final List<Range> runs = last.get().ordinalsOf(shard);
            readRuns(
                epoch.epoch(),
                epoch.epoch() == after.epoch() ? NodeLogBlob.trimBelow(runs, after.ordinal() + 1) : runs,
                last.get(),
                shard,
                consumer
            );
        }
    }

    /**
     * Streams a shard's records from known runs of one epoch, with no fence: for a node reading its own log, which knows
     * what it landed and where.
     *
     * @param epoch the epoch
     * @param runs the runs of slots holding the shard's records
     * @param shard the shard
     * @param consumer what takes each record
     * @throws IOException if a read fails
     */
    public void forEachRecordIn(long epoch, List<Range> runs, ShardKey shard, CheckedConsumer<WalRecord, IOException> consumer)
        throws IOException {
        readRuns(epoch, runs, null, shard, consumer);
    }

    private void readRuns(long epoch, List<Range> runs, Header last, ShardKey shard, CheckedConsumer<WalRecord, IOException> consumer)
        throws IOException {
        for (Range run : runs) {
            for (long ordinal = run.from(); ordinal <= run.to(); ordinal++) {
                final Header header = header(epoch, ordinal, last);
                if (header == null) {
                    continue;
                }
                final List<Slice> slices = header.slicesOf(shard);
                if (slices.isEmpty()) {
                    continue;
                }
                final byte[] body = header.slices().size() > 1 ? body(epoch, header) : null;
                for (Slice slice : slices) {
                    for (WalRecord record : body == null ? log.readSlice(epoch, header, slice) : NodeLogStore.parseSlice(body, slice)) {
                        consumer.accept(record);
                    }
                }
            }
        }
    }

    /**
     * Returns whether any epoch's last header names unpublished records of a shard, reading no slice.
     *
     * <p>A header's index is as of when it was written, so a shard that published after it is still named: an answer
     * that errs towards a replay, never away from one.
     *
     * @param shard the shard
     * @return true if the shard may have records here that it has not published
     * @throws IOException if a fence or a read fails
     */
    public boolean holds(ShardKey shard) throws IOException {
        fenceAll();
        for (Optional<Header> last : lastHeaders.values()) {
            if (last.isPresent() && last.get().ordinalsOf(shard).isEmpty() == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a shard's unpublished records, oldest first.
     *
     * @param shard the shard
     * @return the records
     * @throws IOException if a fence or a read fails
     */
    public List<WalRecord> records(ShardKey shard) throws IOException {
        final List<WalRecord> out = new ArrayList<>();
        forEachRecord(shard, out::add);
        return out;
    }

    /** A blob's body read whole and kept for the other shards in it, or null to read it slice by slice. */
    private byte[] body(long epoch, Header header) throws IOException {
        final long now = System.nanoTime();
        final long since = keptSince;
        if (since != Long.MIN_VALUE && now - since > KEEP_BODIES_NANOS) {
            // Long past the takeover that read them: let them go, and start again for whatever reads next.
            synchronized (bodies) {
                if (keptSince == since) {
                    bodies.clear();
                    keptBytes.set(0);
                    keptSince = Long.MIN_VALUE;
                }
            }
        }
        final long length = NodeLogStore.bodyLength(header);
        final BodyKey key = new BodyKey(epoch, header.ordinal());
        java.util.concurrent.CompletableFuture<byte[]> read = bodies.get(key);
        if (read == null) {
            if (length > MAX_BODY_BYTES || keptBytes.get() + length > MAX_KEPT_BYTES) {
                return null;
            }
            final java.util.concurrent.CompletableFuture<byte[]> mine = new java.util.concurrent.CompletableFuture<>();
            read = bodies.putIfAbsent(key, mine);
            if (read == null) {
                keptBytes.addAndGet(length);
                if (keptSince == Long.MIN_VALUE) {
                    keptSince = now;
                }
                try {
                    mine.complete(log.readBody(epoch, header));
                } catch (IOException | RuntimeException e) {
                    bodies.remove(key, mine);
                    keptBytes.addAndGet(-length);
                    mine.completeExceptionally(e);
                    throw e;
                }
                return mine.join();
            }
        }
        try {
            return read.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for a node-log body", e);
        } catch (java.util.concurrent.ExecutionException e) {
            // Its reader failed: this one reads its own slices instead.
            return null;
        }
    }

    private Header header(long epoch, long ordinal, Header last) throws IOException {
        if (last != null && ordinal == last.ordinal()) {
            return last;
        }
        final Map<Long, Header> ofEpoch = headers.computeIfAbsent(epoch, ignored -> new ConcurrentHashMap<>());
        final Header cached = ofEpoch.get(ordinal);
        if (cached != null) {
            return cached;
        }
        final Optional<NodeLogBlob.Entry> entry = log.readEntry(epoch, ordinal);
        if (entry.isEmpty()) {
            // The index named a blob whose append's outcome was unknown, and it did not land.
            return null;
        }
        if (entry.get() instanceof Header header) {
            ofEpoch.put(ordinal, header);
            return header;
        }
        throw new IOException(
            "node-log blob " + ordinal + " of epoch " + epoch + " of " + log.nodeId() + " is a fence below the last header"
        );
    }
}
