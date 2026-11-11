/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardReconciler;
import org.opensearch.serverless.store.NodeLogBlob.Range;
import org.opensearch.serverless.store.NodeLogBlob.ShardKey;
import org.opensearch.serverless.store.NodeLogCommitter;
import org.opensearch.serverless.store.NodeLogEpoch;
import org.opensearch.serverless.store.NodeLogStore;
import org.opensearch.serverless.store.NodeLogTakeover;
import org.opensearch.serverless.store.WalRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A node's per-node write log, wired to its lease and its shards: what the write path logs through, what renewals
 * record, and what a shard being opened replays.
 *
 * <p><b>Epochs from an earlier process.</b> A node that restarts under the same id finds the epochs its earlier process
 * left in the lease. It fences them before writing anything, so that process can land nothing more, and keeps naming
 * them in its lease: the shards they hold records of are still to be taken, by this node or another, and the lease is
 * where whoever takes them finds them. It writes its own epochs above them.
 *
 * <p><b>The lease is written under one lock.</b> A renewal and the recording of a new epoch each compute the epochs and
 * write the lease, and a renewal that computed its list before an epoch was recorded and wrote it after would drop the
 * epoch from the lease while blobs were written to it. Both hold {@link #leaseLock} across the two steps, and the
 * committer names an epoch being recorded from before the write.
 *
 * <p><b>Replay.</b> A shard taken from another node replays that node's records: every epoch its lease names is fenced
 * once, and shared by every shard taken from it ({@link NodeLogTakeover}). A shard this node reopens itself, after a
 * failed append or a restart, replays its own records: those of the earlier process's epochs, fenced at start, and
 * those of this process's epochs, read from where the committer knows it landed them, with no fence, so the writes of
 * every other shard carry on.
 */
public final class NodeLogWriter implements ShardReconciler.NodeLogHooks {

    /** Writes this node's lease with the given epochs; returns once it has landed. */
    @FunctionalInterface
    public interface LeaseWriter {
        /**
         * Renews the lease naming these epochs.
         *
         * @param epochs the epochs, oldest first
         * @param opensEpoch whether the write records a new epoch before its first blob, rather than renewing: a node
         *     whose lease has lapsed may renew, and must not open an epoch until it has let go of what it held
         * @throws IOException if the lease could not be written
         */
        void write(List<NodeLogEpoch> epochs, boolean opensEpoch) throws IOException;
    }

    /** Whether this node owns a shard at a term, by its head. */
    @FunctionalInterface
    public interface Ownership {
        /**
         * Checks the head.
         *
         * @param shardId the shard
         * @param term the term
         * @return true if the head names this node at that term
         */
        boolean ownsAtTerm(ShardId shardId, long term);
    }

    private final MetadataPlane plane;
    private final String nodeId;
    private final NodeLogStore own;
    private final List<NodeLogEpoch> inherited;
    private final NodeLogCommitter committer;
    private final LeaseWriter leaseWriter;
    private final Object leaseLock = new Object();
    private final Map<ShardKey, ShardId> shards = new ConcurrentHashMap<>();
    /** Per predecessor, the epochs its lease named when first read and the reader that fenced them. */
    private final Map<String, NodeLogTakeover> takeovers = new ConcurrentHashMap<>();

    /**
     * Starts this node's log: reads the epochs its lease names from an earlier process, fences them, and opens a
     * committer that writes above them.
     *
     * @param plane the metadata plane
     * @param nodeId this node
     * @param leaseWriter writes this node's lease
     * @param ownership checks this node's heads
     * @param windowMillis how long a window stays open
     * @param maxBytes roughly the most bytes of id and source in one window
     * @param maxRecords the most records in one window
     * @return the started log
     * @throws IOException if the lease cannot be read or an earlier epoch cannot be fenced
     */
    public static NodeLogWriter start(
        MetadataPlane plane,
        String nodeId,
        LeaseWriter leaseWriter,
        Ownership ownership,
        long windowMillis,
        long maxBytes,
        int maxRecords
    ) throws IOException {
        final List<NodeLogEpoch> inherited = plane.membership().read(nodeId).map(NodeLease::nodeLog).orElse(List.of());
        final NodeLogStore own = new NodeLogStore(plane.blobStore(), plane.basePath(), nodeId);
        if (inherited.isEmpty() == false) {
            // The earlier process may be gone, but a zombie of it must land nothing after what a reopen reads.
            new NodeLogTakeover(own, inherited, nodeId, 0L).fenceAll();
        }
        long firstEpoch = 0L;
        for (NodeLogEpoch epoch : inherited) {
            firstEpoch = Math.max(firstEpoch, epoch.epoch() + 1);
        }
        return new NodeLogWriter(plane, nodeId, own, inherited, firstEpoch, leaseWriter, ownership, windowMillis, maxBytes, maxRecords);
    }

    private NodeLogWriter(
        MetadataPlane plane,
        String nodeId,
        NodeLogStore own,
        List<NodeLogEpoch> inherited,
        long firstEpoch,
        LeaseWriter leaseWriter,
        Ownership ownership,
        long windowMillis,
        long maxBytes,
        int maxRecords
    ) {
        this.plane = Objects.requireNonNull(plane, "plane");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.own = own;
        this.inherited = List.copyOf(inherited);
        this.leaseWriter = Objects.requireNonNull(leaseWriter, "leaseWriter");
        this.committer = new NodeLogCommitter(own, firstEpoch, epochs -> {
            synchronized (leaseLock) {
                leaseWriter.write(withInherited(epochs), true);
            }
        }, (shard, term, epoch) -> {
            final ShardId shardId = shards.get(shard);
            if (shardId == null || ownership.ownsAtTerm(shardId, term) == false) {
                throw new IOException("this node does not own " + shard + " at term " + term + " by its head");
            }
        }, windowMillis, maxBytes, maxRecords);
    }

    private List<NodeLogEpoch> withInherited(List<NodeLogEpoch> epochs) {
        final List<NodeLogEpoch> out = new ArrayList<>(inherited);
        out.addAll(epochs);
        return out;
    }

    /**
     * Returns every epoch a successor would have to fence and replay: the earlier process's, then this one's.
     *
     * @return the epochs, oldest first
     */
    public List<NodeLogEpoch> epochs() {
        return withInherited(committer.positions());
    }

    /**
     * Renews the lease with the epochs as they stand, under the lock a new epoch's recording takes, then deletes what the
     * lease now says nobody needs.
     *
     * <p><b>Deleted only behind the lease.</b> A successor's fence searches for the first free slot from the landed hint
     * the lease records, trusting that every slot below it is taken. So a slot is deleted only once a lease naming a trim
     * point above it has landed: the landed hint of that lease, and of every later one, is at or above the trim point, so
     * no search starts inside what was deleted. An epoch that is complete -- no longer written, nothing unpublished -- is
     * left out of the lease first and deleted whole once that lease has landed. All of it is by name, with no listing, and
     * a delete is free where a PUT is not.
     *
     * <p>Only this process's own epochs. Those an earlier process left are deleted once every shard they hold records of
     * has been taken and published again, which is the janitor's to decide.
     *
     * @throws IOException if the lease could not be written
     */
    public void renew() throws IOException {
        synchronized (leaseLock) {
            committer.expirePins();
            completed.addAll(committer.takeComplete());
            final List<NodeLogEpoch> mine = committer.positions();
            leaseWriter.write(withInherited(mine), false);
            deleteBehind(mine);
        }
    }

    /** Epochs no longer named by the lease and not yet deleted. */
    private final List<NodeLogEpoch> completed = new ArrayList<>();
    /** Per epoch of this process, the slot below which everything has been deleted. */
    private final Map<Long, Long> deletedBelow = new ConcurrentHashMap<>();

    private void deleteBehind(List<NodeLogEpoch> recorded) {
        try {
            for (NodeLogEpoch epoch : recorded) {
                final long from = deletedBelow.getOrDefault(epoch.epoch(), 0L);
                if (epoch.trimPoint() > from) {
                    own.deleteRange(epoch.epoch(), from, epoch.trimPoint());
                    deletedBelow.put(epoch.epoch(), epoch.trimPoint());
                }
            }
            while (completed.isEmpty() == false) {
                final NodeLogEpoch epoch = completed.get(0);
                // Past the landed hint too: the slot an append whose outcome was unknown may have taken, and a fence.
                own.deleteRange(epoch.epoch(), deletedBelow.getOrDefault(epoch.epoch(), 0L), epoch.landed() + 2);
                deletedBelow.remove(epoch.epoch());
                completed.remove(0);
            }
        } catch (IOException e) {
            // Tried again after the next renewal; what is left costs storage, never correctness.
            logger.debug("could not delete node-log blobs behind the lease; will retry", e);
        }
    }

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(NodeLogWriter.class);

    /**
     * Returns the committer, for its counters.
     *
     * @return the committer
     */
    public NodeLogCommitter committer() {
        return committer;
    }

    static ShardKey keyOf(ShardId shardId) {
        return new ShardKey(shardId.getIndex().getUUID(), shardId.id(), shardId.getIndexName());
    }

    /**
     * Logs a shard's records durably, sharing the window's blob with every other shard of this node.
     *
     * @param shardId the shard
     * @param term the term they were written at
     * @param records the records
     * @throws IOException if they did not land
     */
    public void commit(ShardId shardId, long term, List<WalRecord> records) throws IOException {
        final ShardKey key = keyOf(shardId);
        shards.put(key, shardId);
        committer.commit(key, term, records);
    }

    /**
     * Returns how far a shard's records have landed, for a publish to record before it flushes.
     *
     * @param shardId the shard
     * @return the position; null if nothing of the shard's is unpublished
     */
    @Override
    public NodeLogCommitter.Position position(ShardId shardId) {
        return committer.lastPosition(keyOf(shardId));
    }

    /**
     * Records that a shard has published up to a position, so the log no longer needs its records there.
     *
     * @param shardId the shard
     * @param upTo what {@link #position} returned before the publish flushed; null does nothing
     */
    @Override
    public void published(ShardId shardId, NodeLogCommitter.Position upTo) {
        if (upTo != null) {
            committer.onPublished(keyOf(shardId), upTo);
        }
    }

    /**
     * Forgets a shard this node let go of, so its next records here are checked against its head again.
     *
     * @param shardId the shard
     */
    public void forget(ShardId shardId) {
        committer.forget(keyOf(shardId));
        shards.remove(keyOf(shardId));
    }

    /**
     * Streams the records a shard being opened must replay from the node logs, oldest first.
     *
     * @param shardId the shard
     * @param predecessor a node the head named as holding the shard's unpublished records in its log; null or this
     *     node's id for this node's own
     * @param term the term this node opens it at
     * @param consumer what takes each record
     * @return how many records were streamed
     * @throws IOException if a fence or a read fails
     */
    @Override
    public long replay(ShardId shardId, String predecessor, long term, CheckedConsumer<WalRecord, IOException> consumer)
        throws IOException {
        final ShardKey key = keyOf(shardId);
        final long[] count = { 0 };
        final CheckedConsumer<WalRecord, IOException> counting = record -> {
            count[0]++;
            consumer.accept(record);
        };
        if (predecessor == null || predecessor.equals(nodeId)) {
            if (inherited.isEmpty() == false) {
                // Fenced at start; the fence is shared, so this costs only the reads.
                takeoverOf(nodeId, own, inherited, term, null).forEachRecord(key, counting);
            }
            final NodeLogTakeover reader = new NodeLogTakeover(own, List.of(), nodeId, term);
            for (Map.Entry<Long, List<Range>> runs : committer.unpublishedRuns(key).entrySet()) {
                reader.forEachRecordIn(runs.getKey(), runs.getValue(), key, counting);
            }
            return count[0];
        }
        // Read after the head moved, so an epoch the predecessor opens later is one whose ownership check refuses this shard.
        final Optional<NodeLease> lease = plane.membership().read(predecessor);
        final List<NodeLogEpoch> epochs = lease.map(NodeLease::nodeLog).orElse(List.of());
        if (epochs.isEmpty()) {
            return 0L;
        }
        final NodeLogStore theirs = new NodeLogStore(plane.blobStore(), plane.basePath(), predecessor);
        final NodeLogTakeover.LeaseReader reread = () -> plane.membership().read(predecessor).map(NodeLease::nodeLog).orElse(List.of());
        if (plane.isLive(lease.get())) {
            // Live, so it gave the shard back itself and writes no more of it: a node taken from has its lease revoked
            // first. Fencing would fail the writes of every shard it still holds. One that is not live may still be
            // writing an epoch it opened while it held the shard, so it is fenced below.
            NodeLogTakeover.unfenced(theirs, epochs, reread).forEachRecord(key, counting);
            return count[0];
        }
        takeoverOf(predecessor, theirs, epochs, term, reread).forEachRecord(key, counting);
        return count[0];
    }

    /**
     * Streams the records of a shard this node holds that its node log still keeps and a commit does not, oldest first:
     * those past {@code afterSeqNo}, the local checkpoint of the commit the caller started from. For a caller that
     * moves part of a shard elsewhere and filters what it is handed, such as a shard split. It reads only; it fences
     * nothing a live writer uses, and changes nothing.
     *
     * <p>The node log keeps a shard's records only until a commit holding them is published, unless the shard is pinned
     * with {@link #retain}, so {@code afterSeqNo} must be at or past the local checkpoint of the latest commit published
     * before the pin, or before the read when there is none; records the log has let go of are not streamed. Records of a write not yet acknowledged are not streamed either: a caller that needs every write holds
     * the shard's writes first.
     *
     * @param shardId the shard
     * @param term the term this node holds it at
     * @param afterSeqNo the highest sequence number the caller already has; negative for every record the log keeps
     * @param consumer what takes each record
     * @return how many records were streamed
     * @throws IOException if a read fails, or the shard's pin lapsed
     */
    public long forEachRecordSince(ShardId shardId, long term, long afterSeqNo, CheckedConsumer<WalRecord, IOException> consumer)
        throws IOException {
        final ShardKey key = keyOf(shardId);
        final long[] count = { 0 };
        final CheckedConsumer<WalRecord, IOException> filtering = record -> {
            if (record.seqNo() > afterSeqNo) {
                count[0]++;
                consumer.accept(record);
            }
        };
        if (inherited.isEmpty() == false) {
            takeoverOf(nodeId, own, inherited, term, null).forEachRecord(key, filtering);
        }
        final NodeLogTakeover reader = new NodeLogTakeover(own, List.of(), nodeId, term);
        for (Map.Entry<Long, List<Range>> runs : committer.heldRuns(key).entrySet()) {
            reader.forEachRecordIn(runs.getKey(), runs.getValue(), key, filtering);
        }
        return count[0];
    }

    /**
     * Pins what the node log holds of a shard, so {@link #forEachRecordSince} goes on streaming it after the shard
     * publishes: for a caller that builds from a commit while the shard keeps publishing, such as a shard split. Take the
     * pin first, then read the commit to start from, whose checkpoint covers what the log let go of before the pin.
     *
     * <p>While the pin is open the log keeps every blob holding the shard's records since the pin, so it grows with the
     * shard's writes. A pin past {@code maxMillis} lapses: the log lets go of what it kept, and {@link #forEachRecordSince}
     * throws for the shard until the pin is closed or another is taken, so the caller starts over rather than miss
     * records.
     *
     * @param shardId the shard
     * @param maxMillis how long the pin may last; at least 1
     * @return the pin, which the caller closes
     * @throws IllegalStateException if the shard is already pinned
     */
    public NodeLogCommitter.Pin retain(ShardId shardId, long maxMillis) {
        return committer.pin(keyOf(shardId), maxMillis);
    }

    /**
     * Stops the shard's head naming the logs the open replayed, now that what they held of it is published.
     *
     * @param shardId the shard
     * @param term the term it opened at
     * @throws IOException if the head cannot be read or written
     */
    @Override
    public void replayedAndPublished(ShardId shardId, long term) throws IOException {
        plane.heads().clearWriters(shardId.getIndexName(), shardId.id(), nodeId, term);
    }

    /**
     * Records in the shard's head that its own write log is quiet.
     *
     * @param shardId the shard
     * @param term the term this node holds it at
     * @throws IOException if the head cannot be read or written
     */
    @Override
    public void shardLogQuiet(ShardId shardId, long term) throws IOException {
        plane.heads().markShardLogQuiet(shardId.getIndexName(), shardId.id(), nodeId, term);
    }

    private NodeLogTakeover takeoverOf(
        String predecessor,
        NodeLogStore log,
        List<NodeLogEpoch> epochs,
        long term,
        NodeLogTakeover.LeaseReader reread
    ) {
        // One reader per predecessor and set of epochs, so every shard taken from it shares its fences and headers. A
        // lease naming other epochs gets a new one: fencing an epoch again is shared, not repeated.
        return takeovers.compute(predecessor, (id, known) -> {
            if (known != null && known.epochs().equals(epochs)) {
                return known;
            }
            return new NodeLogTakeover(log, epochs, nodeId, term, reread);
        });
    }
}
