/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.split;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.routing.UnitLayout;
import org.opensearch.serverless.shell.NodeLogWriter;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.shell.WriteHold;
import org.opensearch.serverless.store.NodeLogCommitter;
import org.opensearch.serverless.store.WalRecord;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Splits a unit this node holds as its writer into two halves, while it keeps taking writes.
 *
 * <p>In order:
 * <ol>
 *   <li>The split is recorded in the descriptor as pending, which reserves the halves' ids. Only the old unit is live.
 *   <li>The old unit's records in this node's log are pinned, so a publish during the build does not let go of any.
 *   <li>The halves are built from the old unit's published commit and published ({@link UnitSplitBuilder}). Writes go
 *       on meanwhile; this is the long step.
 *   <li>The old unit's writes are held. Every write already under way is in the log by then.
 *   <li>The tail, every record past the commit the halves were built from, is read from the log and handed to each
 *       half's writer open, filtered by the half each record's id routes to. Both halves open on this node; each open
 *       replays its records with their own sequence numbers and publishes before it serves.
 *   <li>The held writes, and every later one sent to the old unit, are forwarded to the halves.
 *   <li>The descriptor is switched to the split layout. Coordinators route to the halves from their next read of it.
 *   <li>Once every coordinator's routing cache has had time to expire, the old unit is published one last time, which
 *       lets the log go of its records, and given up. Only then does the hold end; a write that still reaches the old
 *       unit finds it closed and retired, and is refused.
 * </ol>
 *
 * <p>If the hold runs out before the halves are open, the old unit took writes the halves do not have. The halves are
 * given up, the pending split is dropped (its ids are not reused), and the caller may plan a new split. The old unit
 * keeps serving throughout; nothing a client was told is lost.
 *
 * <p>Needs this node's write log: the tail is read from it.
 */
public final class UnitSplitMover {

    private static final Logger logger = LogManager.getLogger(UnitSplitMover.class);

    /** How long the old unit's writes may be held while the halves open, by default. */
    public static final long DEFAULT_HOLD_MILLIS = 5_000L;

    /** How long the old unit's records stay pinned in the log while the halves are built, by default. */
    public static final long DEFAULT_PIN_MILLIS = 30L * 60L * 1_000L;

    private static final int DESCRIPTOR_ATTEMPTS = 20;

    /**
     * What a split did.
     *
     * @param after the descriptor at the split layout
     * @param built the halves as built from the old unit's commit
     * @param leftTail records of the tail the lower half replayed
     * @param rightTail records of the tail the upper half replayed
     * @param heldMillis how long the old unit's writes waited before they were forwarded
     */
    public record Moved(IndexDescriptor after, UnitSplitBuilder.Built built, int leftTail, int rightTail, long heldMillis) {
    }

    private final ServerlessNode node;
    private final MetadataPlane plane;
    private long holdMillis = DEFAULT_HOLD_MILLIS;
    private long pinMillis = DEFAULT_PIN_MILLIS;
    private long routingCacheMillis = MetadataPlane.DEFAULT_ROUTING_CACHE_MILLIS;

    /**
     * Creates a mover for one node.
     *
     * @param node the node that holds the units it splits
     * @param plane the metadata plane
     */
    public UnitSplitMover(ServerlessNode node, MetadataPlane plane) {
        this.node = node;
        this.plane = plane;
    }

    /**
     * Sets how long the old unit's writes may be held while the halves open.
     *
     * @param millis at least 1
     * @return this
     */
    public UnitSplitMover setHoldMillis(long millis) {
        this.holdMillis = millis;
        return this;
    }

    /**
     * Sets how long the old unit's records stay pinned in the log while the halves are built.
     *
     * @param millis at least 1
     * @return this
     */
    public UnitSplitMover setPinMillis(long millis) {
        this.pinMillis = millis;
        return this;
    }

    /**
     * Sets how long a coordinator may route with a descriptor it read, which is how long the old unit's writes are
     * forwarded after the switch.
     *
     * @param millis zero or more
     * @return this
     */
    public UnitSplitMover setRoutingCacheMillis(long millis) {
        this.routingCacheMillis = millis;
        return this;
    }

    /**
     * Splits one unit.
     *
     * @param indexName the index
     * @param unitId the unit, which this node must hold as its writer
     * @param scratch an empty local directory the build may use and the caller deletes afterwards
     * @return what was done, or empty if the hold ran out before the halves opened, in which case nothing changed but
     *     the reserved ids
     * @throws IllegalStateException if the index has no shard mode, the unit is not held here as a writer, another split
     *     of the index is pending, or this node writes no node log
     * @throws Exception if a read, the build, an open or a swap fails
     */
    public Optional<Moved> split(String indexName, int unitId, Path scratch) throws Exception {
        final NodeLogWriter log = node.nodeLogWriter();
        if (log == null) {
            throw new IllegalStateException("splitting a unit needs the node log, and node " + node.localNode().getName() + " writes none");
        }
        final ShardId parent = heldWriter(indexName, unitId);
        final long term = node.reconciler().shard(parent).getOperationPrimaryTerm();
        final UnitLayout.Split split = reserve(indexName, unitId);
        final IndexDescriptor reserved = plane.describe(indexName).orElseThrow();
        final UnitLayout after = reserved.layout().apply(split);

        try (NodeLogCommitter.Pin pin = log.retain(parent, pinMillis)) {
            // Pinned before the build reads the published commit, so every record past that commit's checkpoint stays.
            final UnitSplitBuilder.Built built = UnitSplitBuilder.build(plane, reserved, split, scratch);
            final ShardId left = new ShardId(parent.getIndex(), split.left().id());
            final ShardId right = new ShardId(parent.getIndex(), split.right().id());

            final long heldAt = System.nanoTime();
            final WriteHold hold = node.fences().hold(parent, holdMillis);
            try {
                final List<WalRecord> toLeft = new ArrayList<>();
                final List<WalRecord> toRight = new ArrayList<>();
                final HalvesTarget target = new HalvesTarget(node.writes(), after, split, left, right);
                log.forEachRecordSince(parent, term, built.checkpoint(), record -> {
                    (target.halfOf(record.id()).equals(left) ? toLeft : toRight).add(record);
                });
                openHalf(left, toLeft);
                openHalf(right, toRight);
                if (hold.forwardTo(target) == false) {
                    logger.warn("the hold on {} ran out before its halves opened; giving the split up", parent);
                    abandon(indexName, split, left, right);
                    return Optional.empty();
                }
                final long heldMillis = (System.nanoTime() - heldAt) / 1_000_000L;
                final IndexDescriptor switched = apply(indexName, split);
                logger.info(
                    "split {} into units {} and {}: {} and {} tail records, writes held {} ms",
                    parent,
                    split.left().id(),
                    split.right().id(),
                    toLeft.size(),
                    toRight.size(),
                    heldMillis
                );
                // A coordinator that read the old layout just before the switch routes with it until its cache expires.
                Thread.sleep(routingCacheMillis);
                retire(parent, term);
                return Optional.of(new Moved(switched, built, toLeft.size(), toRight.size(), heldMillis));
            } finally {
                hold.close();
            }
        }
    }

    private ShardId heldWriter(String indexName, int unitId) {
        for (ShardId open : node.reconciler().openShards()) {
            if (open.getIndexName().equals(indexName) && open.id() == unitId && node.reconciler().isReader(open) == false) {
                return open;
            }
        }
        throw new IllegalStateException("unit " + unitId + " of [" + indexName + "] is not held as a writer on this node");
    }

    /** Records the split as pending, or finds it already pending from an attempt that stopped before the switch. */
    private UnitLayout.Split reserve(String indexName, int unitId) throws IOException {
        for (int attempt = 0; attempt < DESCRIPTOR_ATTEMPTS; attempt++) {
            final long generation = plane.descriptorGeneration(indexName);
            final IndexDescriptor current = plane.describe(indexName).orElseThrow();
            final UnitLayout layout = current.layout();
            if (layout == null) {
                throw new IllegalStateException("[" + indexName + "] has no shard mode, so its shards do not split");
            }
            if (layout.pending() != null) {
                if (layout.pending().retired().id() == unitId) {
                    return layout.pending();
                }
                throw new IllegalStateException("[" + indexName + "] is already splitting " + layout.pending().retired());
            }
            final UnitLayout.Split split = layout.planSplit(unitId);
            if (plane.updateDescriptor(current.withLayout(layout.withPending(split)), generation).isPresent()) {
                return split;
            }
        }
        throw new IOException("could not record the split of unit " + unitId + " of [" + indexName + "]: the descriptor kept changing");
    }

    private void openHalf(ShardId half, List<WalRecord> tail) throws Exception {
        if (tail.isEmpty() == false) {
            node.reconciler().preloadReplay(half, tail);
        }
        final Optional<ShardId> opened = node.activation().activateWriter(plane, half.getIndexName(), half.id());
        if (opened.isEmpty()) {
            throw new IOException("could not take " + half + " as its writer on this node");
        }
    }

    /** Switches the descriptor to the split layout; the mapping or settings may have changed since the reservation. */
    private IndexDescriptor apply(String indexName, UnitLayout.Split split) throws IOException {
        for (int attempt = 0; attempt < DESCRIPTOR_ATTEMPTS; attempt++) {
            final long generation = plane.descriptorGeneration(indexName);
            final IndexDescriptor current = plane.describe(indexName).orElseThrow();
            if (split.equals(current.layout().pending()) == false) {
                throw new IllegalStateException("the pending split of [" + indexName + "] is no longer " + split);
            }
            final IndexDescriptor switched = current.withLayout(current.layout().apply(split));
            if (plane.updateDescriptor(switched, generation).isPresent()) {
                return switched;
            }
        }
        // The writes are forwarded to the halves by now, and the old unit's layout still routes to it: the hold forwards
        // them for as long as it lasts, so the caller's retry finds the split pending and the halves open.
        throw new IOException("could not switch [" + indexName + "] to the split layout: the descriptor kept changing");
    }

    /** Gives the halves up and drops the pending split, after the hold ran out and the old unit took writes they lack. */
    private void abandon(String indexName, UnitLayout.Split split, ShardId left, ShardId right) throws Exception {
        for (ShardId half : List.of(left, right)) {
            final IndexShard shard = node.reconciler().shard(half);
            if (shard != null) {
                final long halfTerm = shard.getOperationPrimaryTerm();
                node.fences().underShardFence(half, () -> {
                    plane.heads()
                        .release(
                            half.getIndexName(),
                            half.id(),
                            node.localNode().getId(),
                            halfTerm,
                            half.getIndex().getUUID(),
                            node.localNode().getEphemeralId()
                        );
                    node.reconciler().releaseShard(half, "its split was given up");
                    return null;
                });
            }
        }
        for (int attempt = 0; attempt < DESCRIPTOR_ATTEMPTS; attempt++) {
            final long generation = plane.descriptorGeneration(indexName);
            final IndexDescriptor current = plane.describe(indexName).orElseThrow();
            if (split.equals(current.layout().pending()) == false) {
                return;
            }
            if (plane.updateDescriptor(current.withLayout(current.layout().withoutPending()), generation).isPresent()) {
                return;
            }
        }
        throw new IOException("could not drop the pending split of [" + indexName + "]: the descriptor kept changing");
    }

    /**
     * Publishes the old unit one last time, so the log lets go of its records, then gives up its head and closes it.
     * Under the shard's fence, so no write is between the publish and the close; none should be, as the hold forwards
     * them all.
     */
    private void retire(ShardId parent, long term) throws Exception {
        node.fences().underShardFence(parent, () -> {
            node.shards().publishShard(parent, term);
            if (plane.heads()
                .release(
                    parent.getIndexName(),
                    parent.id(),
                    node.localNode().getId(),
                    term,
                    parent.getIndex().getUUID(),
                    node.localNode().getEphemeralId()
                ) == false) {
                logger.warn("could not give up the head of retired unit {}; it stays open here until it is let go", parent);
                return null;
            }
            node.reconciler().releaseShard(parent, "retired by a split");
            return null;
        });
    }
}
