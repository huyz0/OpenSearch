/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.serverless.store.NodeLogBlob.Range;
import org.opensearch.serverless.store.NodeLogBlob.ShardBatch;
import org.opensearch.serverless.store.NodeLogBlob.ShardKey;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Group commit across every shard of a node: whatever the node logs in one window is one PUT to its write log.
 *
 * <p><b>The window.</b> The first record to arrive opens a window, and every record that arrives before it closes rides
 * the same blob, whichever shard it is for. A window closes when its time is up or when it is full, and the caller whose
 * records are in it returns once the blob has landed, so a write is durable before it is acknowledged, as before. A
 * window of zero writes as soon as nothing else is in flight, which is the group commit {@link WalGroupCommitter} does
 * per shard.
 *
 * <p><b>One blob in flight.</b> Ordinals have no gaps, which is what lets a successor fence by taking the first free
 * slot. So the next window is written only once the last has landed, and records keep joining it meanwhile.
 *
 * <p><b>Epochs.</b> The log is written in epochs, and an epoch is written only by this node. Before the first blob of
 * an epoch, the epoch is recorded in this node's lease ({@link EpochLease}), so a successor that reads the lease
 * knows to fence it. A failed append ends the epoch, whatever the failure: a fence in the slot means a successor has
 * closed it, and an unknown outcome leaves a slot this writer cannot reason about. The next window opens a new epoch.
 *
 * <p><b>Ownership.</b> Before a shard's first records go into an epoch, {@link ShardAdmission} confirms this node still
 * owns the shard at that term, after the epoch is in the lease. A successor takes a shard by moving its head and then
 * fencing every epoch in this node's lease. So either the check sees the move and refuses, or the epoch was in the
 * lease when the successor read it and its fence closes the epoch to this writer.
 *
 * <p><b>Trimming.</b> For every epoch this tracks where each shard's records are that the shard has not published. An
 * epoch's trim point is the lowest of them, and an epoch that is no longer written and holds nothing unpublished is
 * complete: its blobs can be deleted and it leaves the lease.
 */
public final class NodeLogCommitter {

    /** How long a window stays open after its first record arrives. */
    public static final long DEFAULT_WINDOW_MILLIS = 250L;

    /**
     * The windows a deployment may choose: long enough that a node writing many shards pays one PUT for all of them,
     * short enough that a write is acknowledged within a quarter of a second.
     */
    public static final java.util.Set<Long> ALLOWED_WINDOW_MILLIS = java.util.Set.of(100L, 200L, 250L);
    /** Roughly the most bytes of id and source one window takes before the next arrival opens another. */
    public static final long DEFAULT_MAX_BYTES = 8L * 1024 * 1024;
    /** The most records one window takes before the next arrival opens another. */
    public static final int DEFAULT_MAX_RECORDS = 50_000;

    /** Records an epoch, and the state of every other open one, in this node's lease before the epoch is written. */
    @FunctionalInterface
    public interface EpochLease {
        /**
         * Writes the lease with these epochs; returns only once the write has landed.
         *
         * @param epochs every epoch a successor would have to fence and replay, oldest first
         * @throws IOException if the lease could not be written; the epoch is then not used
         */
        void record(List<NodeLogEpoch> epochs) throws IOException;
    }

    /** Confirms this node still owns a shard before the shard's first records go into an epoch. */
    @FunctionalInterface
    public interface ShardAdmission {
        /**
         * Checks the shard's head names this node at this term.
         *
         * @param shard the shard
         * @param term the term the records were written at
         * @param epoch the epoch they are about to go into, already in the lease
         * @throws IOException if this node does not own the shard at that term, or the check failed
         */
        void admit(ShardKey shard, long term, long epoch) throws IOException;
    }

    /**
     * Where a shard's records landed: they are in this blob, and the shard's earlier records in it or before.
     *
     * @param epoch the epoch
     * @param ordinal the blob
     */
    public record Position(long epoch, long ordinal) implements Comparable<Position> {
        @Override
        public int compareTo(Position other) {
            final int byEpoch = Long.compare(epoch, other.epoch);
            return byEpoch != 0 ? byEpoch : Long.compare(ordinal, other.ordinal);
        }
    }

    private final NodeLogStore store;
    private final EpochLease lease;
    private final ShardAdmission admission;
    private final long windowNanos;
    private final long maxBytes;
    private final int maxRecords;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Deque<Window> windows = new ArrayDeque<>();
    private final TreeMap<Long, EpochState> epochs = new TreeMap<>();
    private boolean flushing;
    /** The epoch being written, or -1 when the next window must open one. */
    private long current = -1L;
    /** An epoch being recorded in the lease, which {@link #positions} names from before the write. */
    private long opening = -1L;
    private long lastEpoch;

    private final AtomicLong blobs = new AtomicLong();
    private final AtomicLong recordsCommitted = new AtomicLong();

    /**
     * Creates a committer.
     *
     * @param store the node's write log
     * @param firstEpoch the first epoch to write, above every epoch this node has written before
     * @param lease records epochs in this node's lease
     * @param admission confirms ownership of a shard new to an epoch
     * @param windowMillis how long a window stays open; zero writes as soon as nothing is in flight
     * @param maxBytes roughly the most bytes of id and source in one window
     * @param maxRecords the most records in one window
     */
    public NodeLogCommitter(
        NodeLogStore store,
        long firstEpoch,
        EpochLease lease,
        ShardAdmission admission,
        long windowMillis,
        long maxBytes,
        int maxRecords
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.admission = Objects.requireNonNull(admission, "admission");
        if (firstEpoch < 0) {
            throw new IllegalArgumentException("firstEpoch must not be negative, got " + firstEpoch);
        }
        if (windowMillis < 0 || maxBytes < 1 || maxRecords < 1) {
            throw new IllegalArgumentException(
                "window must not be negative and bounds must be positive, got " + windowMillis + ", " + maxBytes + ", " + maxRecords
            );
        }
        this.lastEpoch = firstEpoch - 1;
        this.windowNanos = TimeUnit.MILLISECONDS.toNanos(windowMillis);
        this.maxBytes = maxBytes;
        this.maxRecords = maxRecords;
    }

    /**
     * Logs a shard's records durably, in the same blob as whatever else the node logs in this window.
     *
     * @param shard the shard
     * @param term the term the records were written at
     * @param records the records, in the order they must replay; empty writes nothing
     * @return where they landed; null if there were none
     * @throws IOException if the blob carrying them did not land, or the shard was refused admission to the epoch
     */
    public Position commit(ShardKey shard, long term, List<WalRecord> records) throws IOException {
        Objects.requireNonNull(shard, "shard");
        if (records.isEmpty()) {
            return null;
        }
        lock.lock();
        try {
            final Window mine = join(shard, term, records);
            while (mine.done == false) {
                if (flushing) {
                    // Uninterruptibly: these records are in a window that will be written whatever this thread does, so
                    // giving up would report a failure for a write that then lands.
                    changed.awaitUninterruptibly();
                    continue;
                }
                final Window head = windows.peekFirst();
                final long left = head.closesAtNanos - System.nanoTime();
                if (head.full(maxBytes, maxRecords) == false && left > 0) {
                    awaitNanosUninterruptibly(left);
                    continue;
                }
                windows.pollFirst();
                flushing = true;
                try {
                    lock.unlock();
                    try {
                        write(head);
                    } catch (RuntimeException | Error e) {
                        // Nobody in the window may read this as landed.
                        head.failAll(new IOException("node-log write failed", e));
                        throw e;
                    } finally {
                        lock.lock();
                    }
                } finally {
                    head.done = true;
                    flushing = false;
                    changed.signalAll();
                }
            }
            final IOException failure = mine.failures.get(shard);
            if (failure != null) {
                throw failure;
            }
            return mine.landed;
        } finally {
            lock.unlock();
        }
    }

    private void awaitNanosUninterruptibly(long nanos) {
        try {
            changed.awaitNanos(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Adds to the youngest window that will take them, or opens one. Caller holds the lock. */
    private Window join(ShardKey shard, long term, List<WalRecord> records) {
        Window last = windows.peekLast();
        if (last == null || last.full(maxBytes, maxRecords)) {
            last = new Window(System.nanoTime() + windowNanos);
            windows.addLast(last);
        }
        last.add(shard, term, records);
        if (last.full(maxBytes, maxRecords)) {
            // Whoever is lingering on it can write it now.
            changed.signalAll();
        }
        return last;
    }

    /** Writes one window. Called with the lock released, by the one flusher. */
    private void write(Window window) {
        final long epoch;
        try {
            epoch = openEpoch();
        } catch (IOException | RuntimeException e) {
            window.failAll(new IOException("could not record a new epoch of the node log in the lease; nothing was logged", e));
            return;
        }

        // A shard's first records in this epoch wait for a check that this node still owns it.
        final List<ShardBatch> batches = new ArrayList<>();
        for (Map.Entry<Window.Key, List<WalRecord>> entry : window.batches.entrySet()) {
            final Window.Key key = entry.getKey();
            if (window.failures.containsKey(key.shard)) {
                continue;
            }
            if (admitted(epoch, key.shard, key.term) == false) {
                try {
                    admission.admit(key.shard, key.term, epoch);
                    markAdmitted(epoch, key.shard, key.term);
                } catch (IOException | RuntimeException e) {
                    window.failures.put(key.shard, new IOException("refused to log to " + key.shard + " at term " + key.term, e));
                    continue;
                }
            }
            batches.add(new ShardBatch(key.shard, key.term, entry.getValue()));
        }
        batches.removeIf(batch -> window.failures.containsKey(batch.shard()));
        if (batches.isEmpty()) {
            return;
        }

        final long ordinal;
        final long trimPoint;
        final Map<ShardKey, List<Range>> prior;
        lock.lock();
        try {
            final EpochState state = epochs.get(epoch);
            ordinal = state.next;
            trimPoint = state.trimPoint();
            prior = new HashMap<>(state.unpublished);
        } finally {
            lock.unlock();
        }

        final NodeLogBlob.Encoded encoded;
        try {
            encoded = NodeLogBlob.encode(store.nodeId(), epoch, ordinal, trimPoint, batches, prior);
        } catch (IOException | RuntimeException e) {
            // Nothing was written, so the epoch is still good.
            window.failAll(new IOException("could not encode a node-log blob", e));
            return;
        }

        IOException failure = null;
        try {
            store.append(epoch, ordinal, encoded.bytes());
        } catch (IOException e) {
            failure = e;
        } catch (RuntimeException e) {
            failure = new IOException("node-log append failed", e);
        }

        lock.lock();
        try {
            final EpochState state = epochs.get(epoch);
            // A fence in the slot means the blob did not land. Any other failure may have landed it, so its records count
            // as unpublished until the shards publish past it, and the epoch stays in the lease until then.
            final boolean mayHaveLanded = (failure instanceof java.nio.file.FileAlreadyExistsException) == false;
            for (ShardBatch batch : mayHaveLanded ? batches : List.<ShardBatch>of()) {
                state.unpublished.merge(batch.shard(), NodeLogBlob.withOrdinal(List.of(), ordinal), (runs, ignored) -> {
                    final Range last = runs.get(runs.size() - 1);
                    return last.to() >= ordinal ? runs : NodeLogBlob.withOrdinal(runs, ordinal);
                });
            }
            if (failure == null) {
                state.next = ordinal + 1;
                window.landed = new Position(epoch, ordinal);
                blobs.incrementAndGet();
                for (ShardBatch batch : batches) {
                    recordsCommitted.addAndGet(batch.records().size());
                }
            } else {
                // Closed to this writer, by a fence or by an outcome it cannot know. The next window opens a new epoch.
                state.writing = false;
                current = -1L;
            }
        } finally {
            lock.unlock();
        }
        if (failure != null) {
            window.failAll(
                failure instanceof java.nio.file.FileAlreadyExistsException
                    ? new IOException("epoch " + epoch + " of the node log was fenced at slot " + ordinal, failure)
                    : failure
            );
        }
    }

    /** Returns the epoch being written, recording a new one in the lease first if there is none. */
    private long openEpoch() throws IOException {
        final long epoch;
        final List<NodeLogEpoch> recorded;
        lock.lock();
        try {
            if (current >= 0) {
                return current;
            }
            epoch = lastEpoch + 1;
            // Named by positions() from here, so a renewal that writes the lease meanwhile cannot drop it.
            opening = epoch;
            recorded = positionsLocked();
        } finally {
            lock.unlock();
        }
        try {
            lease.record(recorded);
        } catch (IOException | RuntimeException e) {
            lock.lock();
            try {
                opening = -1L;
            } finally {
                lock.unlock();
            }
            throw e;
        }
        lock.lock();
        try {
            opening = -1L;
            lastEpoch = epoch;
            epochs.put(epoch, new EpochState());
            current = epoch;
            return epoch;
        } finally {
            lock.unlock();
        }
    }

    private boolean admitted(long epoch, ShardKey shard, long term) {
        lock.lock();
        try {
            final Long at = epochs.get(epoch).admitted.get(shard);
            return at != null && at == term;
        } finally {
            lock.unlock();
        }
    }

    private void markAdmitted(long epoch, ShardKey shard, long term) {
        lock.lock();
        try {
            epochs.get(epoch).admitted.put(shard, term);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Records that a shard has published everything up to a position, so the log no longer needs it.
     *
     * <p>The position is one {@link #commit} returned for the shard, or {@link #lastPosition}: a publish covers every
     * record the engine had applied when it began, and every record that landed at or before that position had been.
     *
     * @param shard the shard
     * @param upTo the last position its publish covers
     */
    public void onPublished(ShardKey shard, Position upTo) {
        lock.lock();
        try {
            for (Map.Entry<Long, EpochState> entry : epochs.headMap(upTo.epoch(), true).entrySet()) {
                final EpochState state = entry.getValue();
                final List<Range> runs = state.unpublished.get(shard);
                if (runs == null) {
                    continue;
                }
                final List<Range> kept = entry.getKey() < upTo.epoch() ? List.of() : NodeLogBlob.trimBelow(runs, upTo.ordinal() + 1);
                if (kept.isEmpty()) {
                    state.unpublished.remove(shard);
                } else {
                    state.unpublished.put(shard, kept);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns where a shard's last records landed, across every epoch this node still tracks.
     *
     * @param shard the shard
     * @return the position; null if the log holds nothing of the shard's that it has not published
     */
    public Position lastPosition(ShardKey shard) {
        lock.lock();
        try {
            for (Map.Entry<Long, EpochState> entry : epochs.descendingMap().entrySet()) {
                final List<Range> runs = entry.getValue().unpublished.get(shard);
                if (runs != null && runs.isEmpty() == false) {
                    return new Position(entry.getKey(), runs.get(runs.size() - 1).to());
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns, for every epoch this node still tracks, the runs of slots holding a shard's records that it has not
     * published: what this node reads to reopen one of its own shards from the log, with no fence.
     *
     * @param shard the shard
     * @return the runs by epoch, oldest first; empty if there are none
     */
    public Map<Long, List<Range>> unpublishedRuns(ShardKey shard) {
        lock.lock();
        try {
            final Map<Long, List<Range>> out = new TreeMap<>();
            for (Map.Entry<Long, EpochState> entry : epochs.entrySet()) {
                final List<Range> runs = entry.getValue().unpublished.get(shard);
                if (runs != null && runs.isEmpty() == false) {
                    out.put(entry.getKey(), runs);
                }
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Forgets that a shard was admitted, so its next records in any epoch are checked again. For a shard this node let
     * go of.
     *
     * @param shard the shard
     */
    public void forget(ShardKey shard) {
        lock.lock();
        try {
            for (EpochState state : epochs.values()) {
                state.admitted.remove(shard);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns every epoch a successor would have to fence and replay, for the lease: the one being written, and every
     * earlier one that still holds records not yet published.
     *
     * @return the epochs, oldest first
     */
    public List<NodeLogEpoch> positions() {
        lock.lock();
        try {
            return positionsLocked();
        } finally {
            lock.unlock();
        }
    }

    private List<NodeLogEpoch> positionsLocked() {
        final List<NodeLogEpoch> out = new ArrayList<>();
        for (Map.Entry<Long, EpochState> entry : epochs.entrySet()) {
            final EpochState state = entry.getValue();
            if (state.complete() == false) {
                out.add(new NodeLogEpoch(entry.getKey(), state.trimPoint(), state.next));
            }
        }
        if (opening >= 0) {
            out.add(new NodeLogEpoch(opening, 0L, 0L));
        }
        return out;
    }

    /**
     * Removes the epochs that are no longer written and hold nothing unpublished, and returns them, for deletion: every
     * slot up to and including {@link NodeLogEpoch#landed} of each, since the slot at it is the one an append whose
     * outcome is unknown may have taken.
     *
     * @return the complete epochs, oldest first
     */
    public List<NodeLogEpoch> takeComplete() {
        lock.lock();
        try {
            final List<NodeLogEpoch> out = new ArrayList<>();
            final var it = epochs.entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<Long, EpochState> entry = it.next();
                if (entry.getValue().complete()) {
                    out.add(new NodeLogEpoch(entry.getKey(), entry.getValue().next, entry.getValue().next));
                    it.remove();
                }
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the trim point of the epoch being written: the lowest slot it may still need.
     *
     * @return the trim point; -1 if no epoch is being written
     */
    public long trimPoint() {
        lock.lock();
        try {
            return current < 0 ? -1L : epochs.get(current).trimPoint();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns how many blobs have landed: the PUTs this has spent on records.
     *
     * @return the count
     */
    public long blobs() {
        return blobs.get();
    }

    /**
     * Returns how many records those blobs carried.
     *
     * @return the count
     */
    public long recordsCommitted() {
        return recordsCommitted.get();
    }

    /** What this node knows of one epoch of its log. */
    private static final class EpochState {
        /** For each shard, the runs of slots holding its records that it has not published. */
        private final Map<ShardKey, List<Range>> unpublished = new TreeMap<>();
        /** The term each shard was confirmed at for this epoch. */
        private final Map<ShardKey, Long> admitted = new HashMap<>();
        /** The slot after the last that landed. */
        private long next;
        private boolean writing = true;

        private long trimPoint() {
            long trim = next;
            for (List<Range> runs : unpublished.values()) {
                trim = Math.min(trim, runs.get(0).from());
            }
            return trim;
        }

        private boolean complete() {
            return writing == false && unpublished.isEmpty();
        }
    }

    /** One window's records, by shard and term, and how writing them went. */
    private static final class Window {
        private record Key(ShardKey shard, long term) {
        }

        private final long closesAtNanos;
        private final Map<Key, List<WalRecord>> batches = new LinkedHashMap<>();
        private final Map<ShardKey, IOException> failures = new HashMap<>();
        private long bytes;
        private int records;
        private boolean done;
        private Position landed;

        private Window(long closesAtNanos) {
            this.closesAtNanos = closesAtNanos;
        }

        private void add(ShardKey shard, long term, List<WalRecord> more) {
            batches.computeIfAbsent(new Key(shard, term), ignored -> new ArrayList<>()).addAll(more);
            records += more.size();
            for (WalRecord record : more) {
                bytes += WalRecord.contentBytes(record.id(), record.source());
            }
        }

        private boolean full(long maxBytes, int maxRecords) {
            return bytes >= maxBytes || records >= maxRecords;
        }

        private void failAll(IOException failure) {
            for (Key key : batches.keySet()) {
                failures.putIfAbsent(key.shard, failure);
            }
        }
    }
}
