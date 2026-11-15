/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.nodelog;

import org.opensearch.serverless.nodelog.NodeLogBlob.Range;
import org.opensearch.serverless.nodelog.NodeLogBlob.ShardBatch;
import org.opensearch.serverless.nodelog.NodeLogBlob.ShardKey;
import org.opensearch.serverless.store.StoreUsage;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
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
 * window of zero writes as soon as nothing else is in flight.
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
    /** The default {@link org.opensearch.serverless.nodelog.NodeLogSettings#ADMISSION_CONCURRENCY}. */
    public static final int DEFAULT_ADMISSION_CONCURRENCY = 32;

    /** The default of {@link org.opensearch.serverless.nodelog.NodeLogSettings#HEDGE_AFTER_MILLIS}. */
    public static final long DEFAULT_HEDGE_AFTER_MILLIS = 60L;

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
    private final NodeLogAdmissions admissions;
    private final long windowNanos;
    private final long maxBytes;
    private final int maxRecords;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Deque<NodeLogWindow> windows = new ArrayDeque<>();
    private final TreeMap<Long, NodeLogEpochState> epochs = new TreeMap<>();
    private final NodeLogPins pins = new NodeLogPins(lock, epochs);
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
        this.admissions = new NodeLogAdmissions(Objects.requireNonNull(admission, "admission"), timings, this::markAdmitted);
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
        final long joined = System.nanoTime();
        lock.lock();
        boolean locked = true;
        try {
            final List<NodeLogWindow> joinedWindows = joinAll(shard, term, records);
            final NodeLogWindow mine = joinedWindows.get(joinedWindows.size() - 1);
            // Windows are written in order, one at a time, so the last of them landing means the others were written.
            if (mine.driven) {
                // Another writer already waits on this window and will write it, or the ones before it, when due: this
                // one waits for it to land without the lock, so a blob that lands does not wake its hundreds of writers
                // to take the lock one after another before any of them can return.
                lock.unlock();
                locked = false;
                if (awaitLandedUnlocked(mine) == false) {
                    lock.lock();
                    locked = true;
                    awaitLanded(mine);
                }
            } else {
                mine.driven = true;
                awaitLanded(mine);
            }
            timings.caller(joined, mine);
            for (NodeLogWindow each : joinedWindows) {
                final IOException failure = each.failures.get(shard);
                if (failure != null) {
                    throw failure;
                }
            }
            return mine.landed;
        } finally {
            if (locked) {
                lock.unlock();
            }
        }
    }

    /**
     * Waits for a window another writer drives to land, without the lock.
     *
     * @return true once it has landed; false if it took so long that this writer should drive it itself, as when its
     *     driver failed with an unexpected error and left it
     */
    private boolean awaitLandedUnlocked(NodeLogWindow mine) {
        final long patience = Math.max(4 * windowNanos, TimeUnit.SECONDS.toNanos(1));
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (mine.landedLatch.await(patience, TimeUnit.NANOSECONDS)) {
                        return true;
                    }
                    return mine.done;
                } catch (InterruptedException e) {
                    // Uninterruptibly, as the locked wait: its records will be written whatever this thread does.
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Adds a shard's records to the window now open without waiting for it to land: they ride the blob of whatever is
     * logged next, and add no write of their own. For records nothing waits on, such as the commits a node's publishes
     * stage; a node that logs nothing more never writes them.
     *
     * @param shard the shard
     * @param term the term they were written at
     * @param records the records; empty does nothing
     * @param onLanded told where they landed, on the thread that wrote them and holding this committer's lock, so it
     *     must be quick; not told if they did not land
     */
    public void enqueue(ShardKey shard, long term, List<WalRecord> records, java.util.function.Consumer<Position> onLanded) {
        Objects.requireNonNull(shard, "shard");
        if (records.isEmpty()) {
            return;
        }
        lock.lock();
        try {
            final List<NodeLogWindow> joinedWindows = joinAll(shard, term, records);
            final NodeLogWindow window = joinedWindows.get(joinedWindows.size() - 1);
            window.onLanded.add(() -> {
                if (window.landed != null && joinedWindows.stream().noneMatch(each -> each.failures.containsKey(shard))) {
                    onLanded.accept(window.landed);
                }
            });
        } finally {
            lock.unlock();
        }
    }

    /**
     * Writes whatever {@link #enqueue} left in the open windows, so that records nothing else would carry land within a
     * renewal: one write on a node that logged nothing since, none on a node that is logging.
     */
    public void flushQueued() {
        lock.lock();
        try {
            final NodeLogWindow last = windows.peekLast();
            if (last != null && last.onLanded.isEmpty() == false) {
                awaitLanded(last);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Lingers on, or writes, windows until this one is done. Caller holds the lock. */
    private void awaitLanded(NodeLogWindow mine) {
        while (mine.done == false) {
            if (flushing) {
                // Uninterruptibly: these records are in a window that will be written whatever this thread does, so
                // giving up would report a failure for a write that then lands.
                changed.awaitUninterruptibly();
                continue;
            }
            final NodeLogWindow head = windows.peekFirst();
            final long left = head.closesAtNanos - System.nanoTime();
            if (head.full(maxBytes, maxRecords) == false && left > 0) {
                awaitNanosUninterruptibly(left);
                continue;
            }
            windows.pollFirst();
            flushing = true;
            head.flushStartedNanos = System.nanoTime();
            lastFlushStartedNanos = head.flushStartedNanos;
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
                head.doneNanos = System.nanoTime();
                head.done = true;
                head.landedLatch.countDown();
                flushing = false;
                changed.signalAll();
                for (Runnable each : head.onLanded) {
                    try {
                        each.run();
                    } catch (RuntimeException e) {
                        // Told where they landed for bookkeeping only; nothing that wrote them waits on it.
                    }
                }
            }
        }
    }

    private void awaitNanosUninterruptibly(long nanos) {
        try {
            changed.awaitNanos(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Adds a shard's records to the youngest window, splitting them across new windows in order where they would carry
     * one past its bounds. Caller holds the lock.
     *
     * @return the windows they joined, oldest first
     */
    private List<NodeLogWindow> joinAll(ShardKey shard, long term, List<WalRecord> records) {
        final List<NodeLogWindow> joined = new ArrayList<>(1);
        int from = 0;
        while (from < records.size()) {
            NodeLogWindow last = windows.peekLast();
            final long roomBytes = last == null || last.full(maxBytes, maxRecords) ? maxBytes : maxBytes - last.bytes;
            final int roomRecords = last == null || last.full(maxBytes, maxRecords) ? maxRecords : maxRecords - last.records;
            int to = from;
            long bytes = 0;
            while (to < records.size() && to - from < roomRecords) {
                final long next = WalRecord.contentBytes(records.get(to).id(), records.get(to).source());
                if (to > from && bytes + next > roomBytes) {
                    break;
                }
                if (to == from && next > roomBytes && last != null && last.records > 0 && last.full(maxBytes, maxRecords) == false) {
                    // Does not fit what is left of a window others have joined: it starts the next one.
                    last.closeEarly = true;
                    break;
                }
                bytes += next;
                to++;
            }
            if (to == from) {
                // The open window was closed for it; the next pass opens a new one.
                changed.signalAll();
                continue;
            }
            final NodeLogWindow window = join(shard, term, from == 0 && to == records.size() ? records : records.subList(from, to));
            if (to < records.size()) {
                // More of this shard's records follow in the next window: this one is as full as it will get.
                window.closeEarly = true;
                changed.signalAll();
            }
            if (joined.isEmpty() || joined.get(joined.size() - 1) != window) {
                joined.add(window);
            }
            from = to;
        }
        return joined;
    }

    /** Adds to the youngest window that will take them, or opens one. Caller holds the lock. */
    private NodeLogWindow join(ShardKey shard, long term, List<WalRecord> records) {
        NodeLogWindow last = windows.peekLast();
        if (last == null || last.full(maxBytes, maxRecords)) {
            final long now = System.nanoTime();
            long closesAt = now + windowNanos;
            if (idleFlush) {
                // Paced by the last blob rather than by this record: at most one blob starts per window, as before, and a
                // record that finds the log quiet for a whole window goes at once instead of waiting one out alone.
                closesAt = Math.min(closesAt, Math.max(now, lastFlushStartedNanos + windowNanos));
            }
            last = new NodeLogWindow(closesAt);
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
    private void write(NodeLogWindow window) {
        final long epoch;
        final long opening = System.nanoTime();
        try {
            epoch = openEpoch();
        } catch (IOException | RuntimeException e) {
            window.failAll(new IOException("could not record a new epoch of the node log in the lease; nothing was logged", e));
            return;
        } finally {
            timings.epochNanos.add(System.nanoTime() - opening);
        }
        final long admitting = System.nanoTime();

        // A shard's first records in this epoch wait for a check that this node still owns it.
        final List<ShardBatch> batches = new ArrayList<>();
        final List<NodeLogWindow.Key> toAdmit = new ArrayList<>();
        // Under one hold of the lock: a window carries thousands of shards on a busy node, and taking it once a shard made
        // the flusher queue behind every writer joining the next window, thousands of times per blob.
        lock.lock();
        try {
            final Map<ShardKey, Long> admittedHere = epochs.get(epoch).admitted;
            for (Map.Entry<NodeLogWindow.Key, List<WalRecord>> entry : window.batches.entrySet()) {
                final NodeLogWindow.Key key = entry.getKey();
                if (window.failures.containsKey(key.shard())) {
                    continue;
                }
                final Long at = admittedHere.get(key.shard());
                if (at == null || at != key.term()) {
                    toAdmit.add(key);
                }
                batches.add(new ShardBatch(key.shard(), key.term(), entry.getValue()));
            }
        } finally {
            lock.unlock();
        }
        admissions.admitAll(epoch, toAdmit, window);
        batches.removeIf(batch -> window.failures.containsKey(batch.shard()));
        timings.admissionNanos.add(System.nanoTime() - admitting);
        if (batches.isEmpty()) {
            return;
        }

        final long ordinal;
        final long trimPoint;
        final Map<ShardKey, List<Range>> prior;
        lock.lock();
        try {
            final NodeLogEpochState state = epochs.get(epoch);
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
        final long putting = System.nanoTime();
        try {
            // One PUT for every shard in the window: each index pays its share of the blob's bytes.
            final NodeLogStore.HedgeOutcome hedged = StoreUsage.split(
                encoded.header().bytesByIndex(),
                () -> store.appendHedged(epoch, ordinal, encoded.bytes(), hedgeDelayNanos(encoded.bytes().length), hedgeExecutor)
            );
            if (hedged != NodeLogStore.HedgeOutcome.NONE) {
                timings.hedges.increment();
                if (hedged == NodeLogStore.HedgeOutcome.WON) {
                    timings.hedgesWon.increment();
                }
            }
        } catch (IOException e) {
            failure = e;
        } catch (RuntimeException e) {
            failure = new IOException("node-log append failed", e);
        } finally {
            final long took = System.nanoTime() - putting;
            timings.putNanos.add(took);
            timings.puts.increment();
            if (failure == null) {
                hedgeDelay.notePutTook(took, encoded.bytes().length);
            }
        }

        lock.lock();
        try {
            final NodeLogEpochState state = epochs.get(epoch);
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
            epochs.put(epoch, new NodeLogEpochState());
            current = epoch;
            timings.epochsOpened.increment();
            return epoch;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks the shards new to an epoch up to this many at a time, on the given executor and the flusher itself.
     *
     * @param concurrency how many checks may run at once; one or less checks them one after another
     * @param executor where the extra checks run; null checks them one after another
     */
    public void admitConcurrently(int concurrency, java.util.concurrent.Executor executor) {
        admissions.configure(concurrency, executor);
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
            for (Map.Entry<Long, NodeLogEpochState> entry : epochs.headMap(upTo.epoch(), true).entrySet()) {
                final NodeLogEpochState state = entry.getValue();
                final List<Range> runs = state.unpublished.get(shard);
                if (runs == null) {
                    continue;
                }
                final List<Range> kept = entry.getKey() < upTo.epoch() ? List.of() : NodeLogBlob.trimBelow(runs, upTo.ordinal() + 1);
                if (pins.pinnedLocked(shard)) {
                    // Published, but pinned: kept readable, and the trim point stays below it, until the pin goes.
                    final long below = entry.getKey() < upTo.epoch() ? Long.MAX_VALUE : upTo.ordinal() + 1;
                    final List<Range> dropped = NodeLogPins.keptBelow(runs, below);
                    if (dropped.isEmpty() == false) {
                        state.retained.computeIfAbsent(shard, k -> new ArrayList<>()).addAll(dropped);
                    }
                }
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
     * Pins a shard's records in the log: from now until the pin is closed or lapses, a publish of the shard keeps the runs
     * it covers readable through {@link #heldRuns}, and the epochs holding them are neither trimmed nor deleted. For a
     * caller that reads a shard's records since a commit while the shard goes on publishing, such as a shard split.
     *
     * <p>Only the runs the log holds as unpublished when the pin is taken, and every later one: a caller pins first,
     * then reads the commit it starts from, whose checkpoint covers what was dropped before.
     *
     * @param shard the shard
     * @param maxMillis how long the pin may last before it lapses, so a holder that never closes it cannot grow the log
     *     without bound; at least 1
     * @return the pin
     * @throws IllegalStateException if the shard is already pinned
     */
    public NodeLogPin pin(ShardKey shard, long maxMillis) {
        return pins.pin(shard, maxMillis);
    }

    /**
     * Returns every run of a shard the log keeps readable, by epoch, oldest first: what a pin kept after a publish, then
     * what is unpublished.
     *
     * @param shard the shard
     * @return epoch to its runs
     * @throws IOException if a pin on the shard lapsed, so runs it kept may have been let go of
     */
    public Map<Long, List<Range>> heldRuns(ShardKey shard) throws IOException {
        return pins.heldRuns(shard);
    }

    /** Lets go of the pins past their time, so the log can trim what they kept. Called with every lease renewal. */
    public void expirePins() {
        pins.expire();
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
            for (Map.Entry<Long, NodeLogEpochState> entry : epochs.descendingMap().entrySet()) {
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
            for (Map.Entry<Long, NodeLogEpochState> entry : epochs.entrySet()) {
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
            for (NodeLogEpochState state : epochs.values()) {
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
        for (Map.Entry<Long, NodeLogEpochState> entry : epochs.entrySet()) {
            final NodeLogEpochState state = entry.getValue();
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
                final Map.Entry<Long, NodeLogEpochState> entry = it.next();
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

    private final NodeLogCommitTimings timings = new NodeLogCommitTimings();

    private volatile boolean idleFlush;
    /** When the last blob started to be written; guarded by the lock. */
    private long lastFlushStartedNanos = System.nanoTime() - TimeUnit.DAYS.toNanos(1);

    /**
     * Paces windows by the last blob instead of by their first record: a window closes a window's length after the last
     * blob started, or at once when that is past, rather than a window's length after its first record arrived.
     *
     * <p><b>What it changes.</b> A record that arrives after the log has been quiet for a whole window no longer waits one
     * out on its own: it is written at once, which is the common case on a node that is not busy. A busy node is paced
     * exactly as before, one blob per window. Between the two, it costs PUTs: records that would have shared one window
     * opened by the first of them may now take two, the first written at once and the rest a window after it. With
     * arrivals at random, that is at most about half as many blobs again, when about one record arrives per window, and
     * a tenth or less when far fewer or far more do.
     *
     * @param on whether windows are paced by the last blob
     */
    public void flushWhenIdle(boolean on) {
        this.idleFlush = on;
    }

    private final NodeLogHedgeDelay hedgeDelay = new NodeLogHedgeDelay();

    /**
     * How long a PUT of this many bytes waits before it is sent again; see {@link #hedgeAppends}.
     *
     * @param blobBytes the blob's size; zero or less scales nothing
     * @return the delay; zero when hedging is off
     */
    public long hedgeDelayNanos(long blobBytes) {
        return hedgeDelay.delayNanos(blobBytes);
    }

    private volatile java.util.concurrent.Executor hedgeExecutor;

    /**
     * Sends a window's PUT a second time when the first has not answered within the given time, and lets the window
     * land on whichever answers first; see {@link NodeLogStore#appendHedged}. A store's slowest PUTs are slow for reasons
     * of their own, so a second one sent late usually answers well before the first: the window's writers wait for the
     * faster of two rather than for the slow one, and so does the window behind it, which waits for this one to land.
     * Each hedge is one more PUT, for the slow PUTs only: the delay is at least the 95th percentile of the last 128 PUTs, so
     * at most about one PUT in twenty is sent twice whatever the store's latency, and fewer when the setting is longer.
     *
     * @param afterMillis how long a PUT may go unanswered before it is sent again; zero or less never sends a second
     * @param executor where the PUTs of a hedged window run; null never sends a second
     */
    public void hedgeAppends(long afterMillis, java.util.concurrent.Executor executor) {
        this.hedgeExecutor = executor;
        hedgeDelay.configure(afterMillis);
    }

    /**
     * Returns where the callers of {@link #commit} spend their time, summed since the last reset.
     *
     * @return the timings
     */
    public NodeLogCommitTimings timings() {
        return timings;
    }
}
