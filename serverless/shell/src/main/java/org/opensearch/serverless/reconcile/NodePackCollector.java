/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.metadata.SnapshotRecord;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.FileLocation;
import org.opensearch.serverless.store.NodePackBlob;
import org.opensearch.serverless.store.NodePackLedger;
import org.opensearch.serverless.store.NodePackStore;
import org.opensearch.serverless.store.NodePackWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.LongSupplier;

/**
 * Deletes a node's packs once no shard's manifest, frozen view or snapshot names them, without listing anything.
 *
 * <p><b>Finding packs.</b> The node's {@link NodePackLedger} names every epoch it has written in and how far collection
 * has got through each, so every pack that may exist has a computable name. The live writer tells this collector of
 * each pack it writes, with its header, so a new pack costs no read. An older epoch, from before a restart, is read
 * from where collection left off, one header per pack, and a few slots past its recorded end in case the old process
 * wrote packs after it last recorded one.
 *
 * <p><b>Judging a pack.</b> A pack's header names every shard with a block in it, by index name and uuid, which
 * locates that shard's manifest. The pack is referenced while any of those manifests, any frozen view of those shards,
 * or any shallow snapshot of them names it. A pack is only judged once it is {@link
 * NodePackWriter#COLLECTABLE_AFTER_MILLIS} old, which is well past {@link NodePackWriter#SWAP_DEADLINE_MILLIS}: a
 * publish whose swap would come later than that refuses it, so an unreferenced pack that old will never become
 * referenced. It is deleted only when two passes at least {@code minimumUnreferencedMillis} apart both found it
 * unreferenced, so a reader still on the commit before the swap has time to move on, the same grace the per-shard sweep
 * gives. While any view or snapshot is still being taken, or cannot be read, nothing is judged.
 *
 * <p><b>Cost.</b> A pass with nothing due makes no requests. A due pack costs one manifest read per shard it holds,
 * shared by every pack due in the same pass, and a pack found referenced is looked at again later and later, up to
 * {@link #MAX_RECHECK_MILLIS}, so a long-lived segment does not cost a read every pass. Deletes go in batches.
 *
 * <p>A pack whose header names an index without its name (written by the first format version) is never judged
 * unreferenced, since its shards' manifests cannot be found.
 */
public final class NodePackCollector {

    private static final Logger logger = LogManager.getLogger(NodePackCollector.class);

    /** How many slots past an old epoch's recorded end are read before concluding it ends there. */
    static final int PROBE_PAST_END = 16;
    /** The least time between two looks at a pack found referenced. */
    public static final long MIN_RECHECK_MILLIS = 60_000L;
    /** The most time between two looks at a pack found referenced. */
    public static final long MAX_RECHECK_MILLIS = 6L * 60 * 60 * 1000;

    /** What one pass did. */
    public record Pass(int judged, int deleted, int kept, int discovered, int manifestReads) {
    }

    private static final class Tracked {
        final NodePackBlob.Header header;
        final long firstSeenMillis;
        long unreferencedSinceMillis = -1L;
        long nextCheckMillis;
        long recheckMillis = MIN_RECHECK_MILLIS;

        Tracked(NodePackBlob.Header header, long firstSeenMillis) {
            this.header = header;
            this.firstSeenMillis = firstSeenMillis;
            this.nextCheckMillis = firstSeenMillis + NodePackWriter.COLLECTABLE_AFTER_MILLIS;
        }
    }

    private record Reported(long epoch, long ordinal, NodePackBlob.Header header) {
    }

    private final NodePackStore store;
    private final NodePackLedger ledger;
    private final MetadataPlane plane;
    private final LongSupplier clock;
    private final long minimumUnreferencedMillis;
    private volatile NodePackWriter writer;

    private final ConcurrentLinkedQueue<Reported> reported = new ConcurrentLinkedQueue<>();
    /** Packs that exist and are not yet deleted, by epoch and ordinal. */
    private final Map<Long, TreeMap<Long, Tracked>> tracked = new HashMap<>();
    /** Slots at or above an epoch's collected prefix known to hold nothing: deleted, or never written. */
    private final Map<Long, TreeSet<Long>> empty = new HashMap<>();
    /** Old epochs whose end has been found, so they are not probed again. */
    private final Set<Long> ended = new HashSet<>();
    private NodePackLedger.State lastLedger;

    /**
     * Creates a collector of one node's packs.
     *
     * @param store the node's packs
     * @param ledger the node's ledger
     * @param plane where manifests, views and snapshots are read
     * @param clock the plane's clock, in milliseconds
     * @param minimumUnreferencedMillis how long apart the two sights of a pack unreferenced must be
     */
    public NodePackCollector(
        NodePackStore store,
        NodePackLedger ledger,
        MetadataPlane plane,
        LongSupplier clock,
        long minimumUnreferencedMillis
    ) {
        this.store = store;
        this.ledger = ledger;
        this.plane = plane;
        this.clock = clock;
        this.minimumUnreferencedMillis = minimumUnreferencedMillis;
    }

    /**
     * Follows the node's live writer: it is told of every pack the writer writes, and the writer's epoch is not probed
     * for an end.
     *
     * @param writer the writer, whose epoch the ledger already records
     */
    public void follow(NodePackWriter writer) {
        writer.setListener(new NodePackWriter.Listener() {
            @Override
            public void written(NodePackBlob.Header header) {
                reported.add(new Reported(header.epoch(), header.ordinal(), header));
            }

            @Override
            public void failed(long ordinal) {
                reported.add(new Reported(writer.epoch(), ordinal, null));
            }
        });
        this.writer = writer;
    }

    /**
     * Runs one pass, logging rather than throwing a failure; the next pass starts from what this one learned.
     */
    public void runQuietly() {
        try {
            run();
        } catch (Exception e) {
            logger.warn("node pack collection pass for [" + ledger.nodeId() + "] failed; the next one will try again", e);
        }
    }

    /**
     * Runs one pass.
     *
     * @return what it did
     * @throws IOException if the store cannot be read or written
     */
    public synchronized Pass run() throws IOException {
        final long now = clock.getAsLong();
        if (lastLedger == null) {
            lastLedger = ledger.read();
        }
        final NodePackWriter live = writer;
        int discovered = 0;
        for (Reported pack; (pack = reported.poll()) != null;) {
            if (pack.header() != null) {
                track(pack.epoch(), pack.ordinal(), pack.header(), now);
            } else if (known(pack.epoch(), pack.ordinal()) == false && discover(pack.epoch(), pack.ordinal(), now)) {
                // A failed write that landed after all.
                discovered++;
            }
        }
        if (live != null && lastLedger.epoch(live.epoch()).isEmpty() && live.nextOrdinal() > 0L) {
            // The writer recorded its epoch after this collector last read the ledger.
            lastLedger = ledger.read();
        }
        final Map<Long, Long> highs = new HashMap<>();
        for (NodePackLedger.Epoch epoch : lastLedger.epochs()) {
            final boolean current = live != null && live.epoch() == epoch.epoch();
            long high = Math.max(epoch.high(), highestTracked(epoch.epoch()) + 1);
            if (current) {
                high = Math.max(high, live.nextOrdinal());
            }
            // The live writer's own slots are learned from the writer alone: one not reported yet is a write still in
            // flight, which a read now would wrongly find empty and let the collected prefix pass.
            for (long ordinal = epoch.collectedBelow(); current == false && ordinal < high; ordinal++) {
                if (known(epoch.epoch(), ordinal) == false && discover(epoch.epoch(), ordinal, now)) {
                    discovered++;
                }
            }
            if (current == false && ended.contains(epoch.epoch()) == false) {
                int misses = 0;
                for (long ordinal = high; misses < PROBE_PAST_END; ordinal++) {
                    if (discover(epoch.epoch(), ordinal, now)) {
                        discovered++;
                        misses = 0;
                        high = ordinal + 1;
                    } else {
                        misses++;
                    }
                }
                ended.add(epoch.epoch());
            }
            highs.put(epoch.epoch(), high);
        }

        final Judgement judgement = judge(now);
        deleteAll(judgement.doomed());
        lastLedger = ledger.update(lastLedger, epochs -> advance(epochs, highs, live));
        return new Pass(judgement.judged(), judgement.doomed().size(), judgement.kept(), discovered, judgement.manifestReads());
    }

    private record Judgement(int judged, int kept, int manifestReads, List<Tracked> doomed) {
    }

    private Judgement judge(long now) throws IOException {
        final List<Tracked> due = new ArrayList<>();
        for (TreeMap<Long, Tracked> packs : tracked.values()) {
            for (Tracked pack : packs.values()) {
                if (now - pack.firstSeenMillis >= NodePackWriter.COLLECTABLE_AFTER_MILLIS && now >= pack.nextCheckMillis) {
                    due.add(pack);
                }
            }
        }
        if (due.isEmpty()) {
            return new Judgement(0, 0, 0, List.of());
        }
        final List<PointInTime> views = plane.livePointsInTime(now);
        final List<SnapshotRecord> snapshots = plane.liveSnapshots();
        for (PointInTime view : views) {
            if (view.isPlaceholder() || view.isCapturing()) {
                // What it holds, or is about to name, is not known yet; judging now could delete it.
                return new Judgement(0, due.size(), 0, List.of());
            }
        }
        for (SnapshotRecord snapshot : snapshots) {
            if (MetadataPlane.inProgress(snapshot)) {
                return new Judgement(0, due.size(), 0, List.of());
            }
        }
        final Map<NodePackBlob.ShardKey, Set<String>> referencedByShard = new HashMap<>();
        int manifestReads = 0;
        int kept = 0;
        final List<Tracked> doomed = new ArrayList<>();
        for (Tracked pack : due) {
            final String name = FileLocation.nodePacked(pack.header.nodeId(), pack.header.epoch(), pack.header.ordinal(), 0L, 0L)
                .referencedBlob();
            boolean referenced = false;
            for (NodePackBlob.ShardKey shard : pack.header.shards()) {
                if (shard.indexName().isEmpty()) {
                    referenced = true;
                    break;
                }
                Set<String> names = referencedByShard.get(shard);
                if (names == null) {
                    names = referencedBy(shard, views, snapshots);
                    referencedByShard.put(shard, names);
                    manifestReads++;
                }
                if (names.contains(name)) {
                    referenced = true;
                    break;
                }
            }
            if (referenced) {
                pack.unreferencedSinceMillis = -1L;
                pack.nextCheckMillis = now + pack.recheckMillis;
                pack.recheckMillis = Math.min(MAX_RECHECK_MILLIS, pack.recheckMillis * 2);
                kept++;
            } else if (pack.unreferencedSinceMillis < 0L) {
                pack.unreferencedSinceMillis = now;
                pack.nextCheckMillis = now + minimumUnreferencedMillis;
                kept++;
            } else if (now - pack.unreferencedSinceMillis >= minimumUnreferencedMillis) {
                doomed.add(pack);
            } else {
                kept++;
            }
        }
        return new Judgement(due.size(), kept, manifestReads, doomed);
    }

    /** Every blob one shard's current manifest, frozen views and shallow snapshots name. */
    private Set<String> referencedBy(NodePackBlob.ShardKey shard, List<PointInTime> views, List<SnapshotRecord> snapshots)
        throws IOException {
        final Set<String> names = new HashSet<>();
        final Optional<CommitManifest> manifest = plane.segmentPublisher(shard.indexName(), shard.indexUuid(), shard.shard())
            .readManifest();
        if (manifest.isPresent()) {
            for (String file : manifest.get().files().keySet()) {
                names.add(manifest.get().location(file).referencedBlob());
            }
        }
        for (PointInTime view : views) {
            if (view.pins(shard.indexName(), shard.indexUuid())) {
                names.addAll(view.referencedBlobs(shard.shard()));
            }
        }
        for (SnapshotRecord snapshot : snapshots) {
            names.addAll(snapshot.referencedBlobs(shard.indexUuid(), shard.shard()));
        }
        return names;
    }

    private void deleteAll(List<Tracked> doomed) throws IOException {
        final Map<Long, List<Long>> byEpoch = new TreeMap<>();
        for (Tracked pack : doomed) {
            byEpoch.computeIfAbsent(pack.header.epoch(), e -> new ArrayList<>()).add(pack.header.ordinal());
        }
        for (Map.Entry<Long, List<Long>> epoch : byEpoch.entrySet()) {
            store.delete(epoch.getKey(), epoch.getValue());
            for (long ordinal : epoch.getValue()) {
                tracked.get(epoch.getKey()).remove(ordinal);
                empty.computeIfAbsent(epoch.getKey(), e -> new TreeSet<>()).add(ordinal);
            }
        }
    }

    /** Moves each epoch's collected prefix past every slot known empty, records its end, and drops finished old epochs. */
    private List<NodePackLedger.Epoch> advance(List<NodePackLedger.Epoch> epochs, Map<Long, Long> highs, NodePackWriter live) {
        final List<NodePackLedger.Epoch> next = new ArrayList<>(epochs.size());
        final long newest = epochs.stream().mapToLong(NodePackLedger.Epoch::epoch).max().orElse(-1L);
        for (NodePackLedger.Epoch epoch : epochs) {
            final Long seenHigh = highs.get(epoch.epoch());
            if (seenHigh == null) {
                // Added since this pass read the ledger; the next pass gets to it.
                next.add(epoch);
                continue;
            }
            final TreeSet<Long> gone = empty.getOrDefault(epoch.epoch(), new TreeSet<>());
            long collectedBelow = epoch.collectedBelow();
            while (collectedBelow < seenHigh && gone.contains(collectedBelow)) {
                collectedBelow++;
            }
            gone.headSet(collectedBelow).clear();
            final long high = Math.max(epoch.high(), seenHigh);
            final boolean current = live != null && live.epoch() == epoch.epoch();
            if (current == false && epoch.epoch() != newest && ended.contains(epoch.epoch()) && collectedBelow >= high) {
                // An old epoch with nothing left in it. The newest epoch always stays: it may be a writer's that has
                // not written yet, whatever this collector follows.
                tracked.remove(epoch.epoch());
                empty.remove(epoch.epoch());
                continue;
            }
            next.add(new NodePackLedger.Epoch(epoch.epoch(), collectedBelow, high));
        }
        return next;
    }

    private void track(long epoch, long ordinal, NodePackBlob.Header header, long now) {
        tracked.computeIfAbsent(epoch, e -> new TreeMap<>()).putIfAbsent(ordinal, new Tracked(header, now));
    }

    private boolean known(long epoch, long ordinal) {
        final TreeMap<Long, Tracked> packs = tracked.get(epoch);
        final TreeSet<Long> gone = empty.get(epoch);
        return (packs != null && packs.containsKey(ordinal)) || (gone != null && gone.contains(ordinal));
    }

    private long highestTracked(long epoch) {
        final TreeMap<Long, Tracked> packs = tracked.get(epoch);
        return packs == null || packs.isEmpty() ? -1L : packs.lastKey();
    }

    /** Reads one slot's header: tracks the pack if there is one, else remembers the slot as empty. */
    private boolean discover(long epoch, long ordinal, long now) throws IOException {
        final Optional<NodePackBlob.Header> header = store.readHeader(epoch, ordinal);
        if (header.isPresent()) {
            track(epoch, ordinal, header.get(), now);
            return true;
        }
        empty.computeIfAbsent(epoch, e -> new TreeSet<>()).add(ordinal);
        return false;
    }

    /**
     * How many packs this collector knows exist and has not deleted.
     *
     * @return the count
     */
    public synchronized int trackedPacks() {
        return tracked.values().stream().mapToInt(Map::size).sum();
    }
}
