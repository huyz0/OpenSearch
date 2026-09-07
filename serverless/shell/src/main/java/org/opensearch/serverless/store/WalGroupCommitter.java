/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Group commit for the write-ahead log: many concurrent single-document writes to one shard, one PUT.
 *
 * <p><b>The cost this removes.</b> {@link WalStore#append(long, List)} has always written a whole list as
 * one blob, and {@code _bulk} on one shard has always cost exactly one PUT because of it. A single-document
 * write never joined a batch, so a workload of individual writes paid one object-store PUT per document —
 * and a PUT is billed at roughly twelve times a GET, which made the log, not the metadata plane, the
 * overwhelming majority of what a write-heavy deployment spends. Nothing else on the write path is within
 * an order of magnitude of it.
 *
 * <p><b>There is no timer, and that is the design rather than a simplification.</b> A fixed buffering
 * window taxes every write with latency whether or not anything else is there to share the PUT with. This
 * batches only what arrives <em>while a PUT for that shard is already in flight</em>: the first caller to
 * find no flush running issues its own immediately, and everyone who arrives during that round trip rides
 * the next one. So a lightly loaded shard sees latency identical to before, a busy one batches hard, and
 * the group size tunes itself to offered load with nothing to configure.
 *
 * <p>The consequence worth stating plainly: <b>PUTs per second per shard are bounded by the store's write
 * latency rather than by the write rate.</b> At a 25ms PUT a shard costs at most forty per second whether
 * it is taking a hundred writes a second or a hundred thousand.
 *
 * <p><b>What this does not change.</b> Records are grouped <em>after</em> the engine has applied them --
 * see {@code ServerlessNode#indexUnderFence}, which applies first precisely because the engine assigns the
 * sequence number the record carries. So the order of operations is already fixed before anything here
 * sees them, and this batches the durability step alone. A caller returns only once the PUT carrying its
 * record has landed, so the contract that a write is durable before it is acknowledged is exactly the one
 * that held before.
 *
 * <p><b>Ordering.</b> Groups are a FIFO deque and each carries a single term, so the total order over a
 * term stays (ordinal, position within the blob) — the order {@link WalStore#replayable} already relies
 * on. A term change starts a new group rather than being merged into the open one; in practice it cannot
 * arrive mid-group anyway, because a release takes the write side of the shard's fence and so waits for
 * every in-flight write.
 *
 * <p><b>What gets worse, said out loud.</b> A document is in the engine's version map, and so visible to a
 * realtime get from the shard's owner, from the moment it is applied until its record is durable. That
 * window existed before at one PUT's width; queueing widens it by however long the caller waits for its
 * group. Nothing is acknowledged inside it, so no client is told something untrue — but a concurrent get
 * against the owner can see a document that a failover then takes away. The window is also the window in
 * which this node's lease can lapse between applying and logging, which fences the shard and rebuilds it
 * from the log; that is milliseconds against a lease measured in tens of seconds.
 *
 * <p>A failed PUT fails every member of its group, all of which are already in the engine. That is the
 * same shape a {@code _bulk} has had all along, and {@code ServerlessNode#appendOrRelease} handles it the
 * same way: the shard is fenced and reopened from the log, which does not contain the operations, so the
 * refusal every caller receives is made true rather than merely reported.
 */
public final class WalGroupCommitter {

    /**
     * How many records a single group may carry before the next arrival starts a new one.
     *
     * <p>A thousand: the blob is read whole by {@link WalStore#replayable}, so a group is also a unit of
     * recovery work, and there is no reason to make one arbitrarily large when the saving is already
     * asymptotic well below this.
     */
    public static final int DEFAULT_MAX_RECORDS = 1_000;

    /**
     * Roughly how many bytes of document source a group may carry before the next arrival starts a new one.
     *
     * <p>Four megabytes, and <b>approximate on purpose</b>: it is measured on the raw id and source rather
     * than on the serialized record, because measuring the real thing means serializing every record twice.
     * JSON escaping can push the written blob above this, so it is a bound on the order of magnitude and
     * not a guarantee — which is all it needs to be, since its job is to stop one group growing without
     * limit rather than to hit a precise size.
     */
    public static final long DEFAULT_MAX_BYTES = 4L * 1024 * 1024;

    /** What actually writes a group. {@link WalStore#append(long, List)}, in production. */
    @FunctionalInterface
    public interface Flush {
        /**
         * Writes a whole group as one blob.
         *
         * @param term the term every record in the group was written at
         * @param records the records, in the order they must replay
         * @throws IOException if the append fails
         */
        void append(long term, List<WalRecord> records) throws IOException;
    }

    private final int maxRecords;
    private final long maxBytes;
    private final Map<ShardId, State> states = new ConcurrentHashMap<>();
    private final AtomicLong groups = new AtomicLong();
    private final AtomicLong recordsCommitted = new AtomicLong();

    /** Creates a committer with the default bounds. */
    public WalGroupCommitter() {
        this(DEFAULT_MAX_RECORDS, DEFAULT_MAX_BYTES);
    }

    /**
     * Creates a committer with explicit bounds.
     *
     * <p>{@code maxRecords} of one disables grouping: every caller then flushes its own records and the
     * behaviour is what it was before this class existed. That is the off switch, and it is a value rather
     * than a branch so there is only one code path to reason about.
     *
     * @param maxRecords the most records one group may carry; at least one
     * @param maxBytes roughly the most bytes of id and source one group may carry; at least one
     */
    public WalGroupCommitter(int maxRecords, long maxBytes) {
        if (maxRecords < 1) {
            throw new IllegalArgumentException("maxRecords must be positive, got " + maxRecords);
        }
        if (maxBytes < 1) {
            throw new IllegalArgumentException("maxBytes must be positive, got " + maxBytes);
        }
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
    }

    /**
     * Appends records durably, sharing a PUT with whatever else is being written to the same shard.
     *
     * <p>Returns once the blob carrying these records has landed. Throws if it did not, and every other
     * member of the same group sees the same failure — see this class's documentation for why that is the
     * honest outcome rather than a partial success.
     *
     * @param shardId the shard whose log this is
     * @param term the writing node's term
     * @param records the records, in the order they must replay; empty is a no-op
     * @param flush what writes a group
     * @throws IOException if the group carrying these records could not be written
     */
    public void commit(ShardId shardId, long term, List<WalRecord> records, Flush flush) throws IOException {
        if (records.isEmpty()) {
            // Not an error and not a blob: what a bulk whose items all belong to other shards leaves here.
            return;
        }
        final State state = states.computeIfAbsent(shardId, ignored -> new State());
        final Group mine;
        IOException failure = null;

        state.lock.lock();
        try {
            mine = state.join(term, records, maxRecords, maxBytes);
            while (mine.done == false) {
                if (state.flushing) {
                    // Someone else's PUT is in flight. Ours is either in it, or in the group being built
                    // behind it; either way the thing to do is wait, which is also what makes this a group
                    // commit rather than a queue of individual writes.
                    //
                    // Uninterruptibly, and that is load-bearing rather than lazy. These records are in
                    // some other thread's group and will be written whatever this thread does; abandoning
                    // the wait would report a failure for a write that then lands in the log. The caller
                    // turns a failure here into a fenced shard reopened from the log precisely because the
                    // log does not contain the operation -- so a caller that gives up early would make
                    // that refusal a lie, which is the one thing the write path may not do. The interrupt
                    // is preserved for whatever this thread does next.
                    state.changed.awaitUninterruptibly();
                    continue;
                }
                // Nothing in flight, so this caller does the flushing -- of the oldest group, which may be
                // an earlier one than its own. It keeps going until its own group has landed.
                final Group batch = state.pending.pollFirst();
                state.flushing = true;
                groups.incrementAndGet();
                recordsCommitted.addAndGet(batch.records.size());

                IOException thrown = null;
                state.lock.unlock();
                try {
                    flush.append(batch.term, batch.records);
                } catch (IOException e) {
                    thrown = e;
                } catch (RuntimeException e) {
                    thrown = new IOException("write-ahead log append to " + shardId + " failed", e);
                } finally {
                    state.lock.lock();
                }
                batch.failure = thrown;
                batch.done = true;
                state.flushing = false;
                // Everyone: the members of the group that just landed, and the members of the group behind
                // it, one of whom now has to take over as flusher.
                state.changed.signalAll();
            }
            failure = mine.failure;
        } finally {
            state.lock.unlock();
        }

        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Drops the state for a shard this node no longer holds.
     *
     * <p>A node that churns through shards would otherwise keep a lock and a deque per shard it has ever
     * held. A flush already in flight holds its own reference and finishes normally; its members are then
     * judged by the checks their caller makes after the append, which is where a shard closed underneath a
     * write is caught.
     *
     * @param shardId the shard
     */
    public void forget(ShardId shardId) {
        states.remove(shardId);
    }

    /**
     * Returns how many groups have been written: the number of object-store PUTs this has spent.
     *
     * @return the count
     */
    public long groups() {
        return groups.get();
    }

    /**
     * Returns how many records those groups carried, so the ratio against {@link #groups()} is the
     * batching actually achieved rather than the batching hoped for.
     *
     * @return the count
     */
    public long recordsCommitted() {
        return recordsCommitted.get();
    }

    /** One shard's queue of groups, and whether a PUT for it is in flight. */
    private static final class State {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();
        private final Deque<Group> pending = new ArrayDeque<>();
        private boolean flushing;

        /** Adds to the youngest group that will still take them, or starts a new one. Caller holds the lock. */
        private Group join(long term, List<WalRecord> records, int maxRecords, long maxBytes) {
            Group last = pending.peekLast();
            if (last == null || last.term != term || last.records.size() >= maxRecords || last.bytes >= maxBytes) {
                // A term change starts a new group rather than joining one: a group is written at one term
                // and there is no such thing as a blob spanning two.
                last = new Group(term);
                pending.addLast(last);
            }
            last.records.addAll(records);
            for (WalRecord record : records) {
                last.bytes += record.id().length() + (record.source() == null ? 0 : record.source().length());
            }
            return last;
        }
    }

    /** One blob's worth of records, and the outcome of writing it. */
    private static final class Group {
        private final long term;
        private final List<WalRecord> records = new ArrayList<>();
        private long bytes;
        private boolean done;
        private IOException failure;

        private Group(long term) {
            this.term = term;
        }
    }
}
