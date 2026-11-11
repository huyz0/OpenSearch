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

    private final NodeLogStore log;
    private final List<NodeLogEpoch> epochs;
    private final String fencedBy;
    private final long term;

    /** Per epoch, the last header below its fence, or empty if it held no records above its trim point. */
    private volatile Map<Long, Optional<Header>> lastHeaders;
    /** Headers read for one shard, kept for the others in the same blob. */
    private final Map<Long, Map<Long, Header>> headers = new ConcurrentHashMap<>();

    /**
     * Creates a reader of one predecessor's log.
     *
     * @param log the predecessor's log
     * @param epochs the epochs its lease names
     * @param fencedBy this node
     * @param term the term this node has just won, recorded in each fence for diagnosis
     */
    public NodeLogTakeover(NodeLogStore log, List<NodeLogEpoch> epochs, String fencedBy, long term) {
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
        final Map<Long, Optional<Header>> found = new ConcurrentHashMap<>();
        for (NodeLogEpoch epoch : epochs) {
            final long fence = log.fence(epoch.epoch(), epoch.landed(), fencedBy, term);
            found.put(epoch.epoch(), log.lastHeaderBelow(epoch.epoch(), fence, epoch.trimPoint()));
        }
        lastHeaders = found;
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
                for (Slice slice : header.slicesOf(shard)) {
                    for (WalRecord record : log.readSlice(epoch, header, slice)) {
                        consumer.accept(record);
                    }
                }
            }
        }
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
