/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

/**
 * Tracks {@link ServerlessNode}'s standing in the fleet: its capacity and refusals at the cap, the fleet's size and
 * mean, and where a takeover or a hand-off should go.
 */
public final class NodeCapacity {

    private final ServerlessNode node;

    NodeCapacity(ServerlessNode node) {
        this.node = node;
        this.placement = new FleetPlacement(
            () -> node.localNode.getId(),
            () -> admission.maxShardsHeld(),
            () -> node.reconciler == null ? 0 : node.reconciler.heldShards().size()
        );
    }

    volatile ShardAdmission admission = new ShardAdmission() {
        @Override
        public int maxShardsHeld() {
            return org.opensearch.serverless.reconcile.BackgroundReconciler.DEFAULT_MAX_SHARDS_HELD;
        }

        @Override
        public boolean makeRoom() {
            return false;
        }
    };

    /**
     * Installs the cap and eviction policy readers and views are admitted under.
     *
     * @param admission the policy, normally the node's reconciler
     */
    public void setShardAdmission(ShardAdmission admission) {
        this.admission = admission;
    }

    /** Reader opens this node refused at its shard cap. */
    final java.util.concurrent.atomic.AtomicLong readerOpensRefusedAtCap = new java.util.concurrent.atomic.AtomicLong();
    /** Writer activations this node refused at its shard cap. */
    final java.util.concurrent.atomic.AtomicLong activationsRefusedAtCap = new java.util.concurrent.atomic.AtomicLong();
    /** Shards given up to make room for another. */
    final java.util.concurrent.atomic.AtomicLong evictedForRoom = new java.util.concurrent.atomic.AtomicLong();
    /** Shards given up because a member with room could take them. */
    final java.util.concurrent.atomic.AtomicLong handedOff = new java.util.concurrent.atomic.AtomicLong();
    /** Writes for a shard nobody held that this node sent to a member with room rather than take. */
    final java.util.concurrent.atomic.AtomicLong writesSteered = new java.util.concurrent.atomic.AtomicLong();

    /**
     * When each shard turned away at the cap was last turned away: distinct shards, not refusals. A burst of searches asked
     * for the same shards over and over, and counted as events one minute's refusals said the fleet wanted forty-three nodes.
     */
    final java.util.Map<String, Long> refusedShards = new java.util.concurrent.ConcurrentHashMap<>();

    /** How long a shard counts as in use after it was last used. */
    public static final long IN_USE_WINDOW_MILLIS = 60_000L;

    /** The share of a node's cap a fleet sized by {@link NodeCapacity#fleetSize} aims to use. */
    public static final double TARGET_UTILISATION = FleetPlacement.TARGET_UTILISATION;

    final java.util.concurrent.atomic.AtomicLong takeoversOvercommitted = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Dead members' shards taken past the cap, since the node started.
     *
     * @return the count
     */
    public long takeoversOvercommitted() {
        return takeoversOvercommitted.get();
    }

    /** Above this share of its cap a node looks for a member to hand work to. */
    public static final double HAND_OFF_ABOVE = FleetPlacement.HAND_OFF_ABOVE;
    /** At or below this share of its cap a member can take work handed to it. */
    public static final double ROOM_AT_OR_BELOW = FleetPlacement.ROOM_AT_OR_BELOW;
    /** Above this multiple of the fleet's mean held shards a node hands work off, however far it is from its cap. */
    public static final double HAND_OFF_ABOVE_MEAN = FleetPlacement.HAND_OFF_ABOVE_MEAN;
    /** A node handing off for being above the mean hands off down to this multiple of it. */
    public static final double HAND_OFF_DOWN_TO_MEAN = FleetPlacement.HAND_OFF_DOWN_TO_MEAN;
    /** Below this share of its cap a node is never too busy for the mean to matter: a small fleet's few shards stay. */
    public static final double MEAN_MATTERS_FROM = FleetPlacement.MEAN_MATTERS_FROM;

    /** Decides where this node's work belongs across the fleet; the methods below forward to it. */
    final FleetPlacement placement;

    /**
     * Where writer activations spend their time, summed: reading the descriptor, acquiring the head, marking the rollup
     * entry owned, and opening the shard -- restoring its commit and replaying its log -- of which projecting and applying
     * the local view that names it is a part of its own. With {@link #activationsOpened}
     * an operator reads the average of each, and which one a burst of cold shards is waiting on.
     */
    final java.util.concurrent.atomic.AtomicLong[] activationPhases = {
        new java.util.concurrent.atomic.AtomicLong(),
        new java.util.concurrent.atomic.AtomicLong(),
        new java.util.concurrent.atomic.AtomicLong(),
        new java.util.concurrent.atomic.AtomicLong(),
        new java.util.concurrent.atomic.AtomicLong() };
    final java.util.concurrent.atomic.AtomicLong activationsOpened = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Returns how full this node is and what fullness has cost.
     *
     * @return the figures
     */
    public Capacity capacity() {
        return new Capacity(
            admission.maxShardsHeld(),
            node.reconciler == null ? 0 : node.reconciler.heldShards().size(),
            readerOpensRefusedAtCap.get(),
            activationsRefusedAtCap.get(),
            evictedForRoom.get(),
            handedOff.get(),
            writesSteered.get()
        );
    }

    /**
     * Counts a writer activation refused at the cap.
     *
     * @param indexName the index
     * @param shard the shard
     */
    public void noteActivationRefusedAtCap(String indexName, int shard) {
        activationsRefusedAtCap.incrementAndGet();
        noteRefusedRecently(indexName, shard);
    }

    void noteRefusedRecently(String indexName, int shard) {
        refusedShards.put(ServerlessNode.shardKey(indexName, shard), System.nanoTime());
    }

    /**
     * How many distinct shards this node turned away at its cap in the last minute: writer activations and reader opens.
     *
     * @return the count
     */
    public int refusedLastMinute() {
        final long cutoff = System.nanoTime() - java.util.concurrent.TimeUnit.MINUTES.toNanos(1);
        refusedShards.values().removeIf(at -> at < cutoff);
        return refusedShards.size();
    }

    /**
     * How many shards this node used in the last minute: what it holds for demand, rather than as a cache that fills
     * whatever the load.
     *
     * @return the count
     */
    public int inUseLastMinute() {
        if (node.reconciler == null) {
            return 0;
        }
        final org.opensearch.serverless.metadata.MetadataPlane plane = node.metadataPlane;
        final long now = plane == null ? System.currentTimeMillis() : plane.clock().getAsLong();
        int used = 0;
        for (org.opensearch.core.index.shard.ShardId shardId : node.reconciler.openShards()) {
            final java.util.OptionalLong last = node.reconciler.lastUsed(shardId);
            if (last.isPresent() && now - last.getAsLong() < NodeCapacity.IN_USE_WINDOW_MILLIS) {
                used++;
            }
        }
        return used;
    }

    /**
     * Sizes the writer fleet from the load every member renews into its lease. See {@link FleetPlacement#fleetSize}.
     *
     * @param plane the metadata plane
     * @return the members counted and the nodes wanted
     */
    public FleetSize fleetSize(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.fleetSize(plane);
    }

    /**
     * Counts a shard given up, to make room or for a member with room.
     *
     * @param forHandOff true if given up for a member with room
     */
    public void noteEvicted(boolean forHandOff) {
        (forHandOff ? handedOff : evictedForRoom).incrementAndGet();
    }

    /** Counts a dead member's shard taken past the cap, without evicting for it. */
    public void noteTakeoverOvercommitted() {
        takeoversOvercommitted.incrementAndGet();
    }

    /**
     * Whether this node holds more than {@link #HAND_OFF_ABOVE} of its cap.
     *
     * @return true when nearly full
     */
    public boolean nearlyFull() {
        return placement.nearlyFull();
    }

    /**
     * The live writer member with the most room, if any. See {@link FleetPlacement#memberWithRoom}.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public java.util.Optional<String> memberWithRoom(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.memberWithRoom(plane);
    }

    /**
     * Sets whether requests for a dead member's shard go to the one survivor named for it.
     *
     * @param route whether to
     */
    public void setRouteTakeovers(boolean route) {
        placement.setRouteTakeovers(route);
    }

    /**
     * Whether requests for a dead member's shard go to the one survivor named for it.
     *
     * @return true if they do
     */
    public boolean routesTakeovers() {
        return placement.routesTakeovers();
    }

    /**
     * Sets how far past its cap a member may take a dead member's shards. See {@link FleetPlacement#setTakeoverOvercommit}.
     *
     * @param fraction of the cap
     */
    public void setTakeoverOvercommit(double fraction) {
        placement.setTakeoverOvercommit(fraction);
    }

    /** Counts a write for a dead member's shard sent to the member that takes it over. */
    public void noteTakeoverWriteRouted() {
        placement.noteTakeoverWriteRouted();
    }

    /** Counts a doubt about a dead member's shard this node left to the member that takes it over. */
    public void noteTakeoverDoubtLeft() {
        placement.noteTakeoverDoubtLeft();
    }

    /**
     * Writes routed to, and doubts left to, the member that takes a dead member's shard over.
     *
     * @return routed, then left
     */
    public long[] takeoverRouting() {
        return placement.takeoverRouting();
    }

    /**
     * The member that takes over a shard whose owner's lease has run out. See {@link FleetPlacement#takeoverWinner}.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shard the shard
     * @param deadOwner the owner whose lease ran out, left out of the choice
     * @return the member's id, or empty if no live writer member is known
     */
    public java.util.Optional<String> takeoverWinner(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shard,
        String deadOwner
    ) {
        return placement.takeoverWinner(plane, indexName, shard, deadOwner);
    }

    /**
     * Whether a shard is a dead member's that another live member is to take over. See
     * {@link FleetPlacement#takeoverBelongsElsewhere}.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shard the shard
     * @return true to leave it to the member named
     */
    public boolean takeoverBelongsElsewhere(org.opensearch.serverless.metadata.MetadataPlane plane, String indexName, int shard) {
        return placement.takeoverBelongsElsewhere(plane, indexName, shard);
    }

    /**
     * The mean of held shards over the live writer members. See {@link FleetPlacement#fleetMeanHeld}.
     *
     * @param plane the metadata plane
     * @return the mean, or -1 if no member says
     */
    public double fleetMeanHeld(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.fleetMeanHeld(plane);
    }

    /**
     * Whether this node holds far more than its share of the fleet. See {@link FleetPlacement#aboveFleetShare}.
     *
     * @param plane the metadata plane
     * @return true when above its share
     */
    public boolean aboveFleetShare(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.aboveFleetShare(plane);
    }

    /**
     * The live writer member holding the fewest shards, if below the fleet's mean. See {@link FleetPlacement#memberBelowMean}.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public java.util.Optional<String> memberBelowMean(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.memberBelowMean(plane);
    }

    /**
     * Where this node should send work it would otherwise take, if anywhere. See {@link FleetPlacement#handOffTarget}.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public java.util.Optional<String> handOffTarget(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.handOffTarget(plane);
    }

    /** Where unheld buckets go: the same rendezvous ranking on every node. */
    final org.opensearch.serverless.bucket.BucketPlacement bucketPlacement = new org.opensearch.serverless.bucket.BucketPlacement();

    /**
     * Where this node should send a write for an index nobody holds, if anywhere: for a member of a bucket, to the
     * writer the bucket ranks first, so every node places a bucket alike whichever one a client reached; for any other
     * index, as {@link #handOffTarget(org.opensearch.serverless.metadata.MetadataPlane)}.
     *
     * @param plane the metadata plane
     * @param descriptor the index written to
     * @return the member's id, or empty to take it here
     */
    public java.util.Optional<String> handOffTarget(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        org.opensearch.serverless.cluster.IndexDescriptor descriptor
    ) {
        if (descriptor.bucket() == null || bucketsOwnedTogether() == false) {
            return handOffTarget(plane);
        }
        final java.util.Optional<String> owner = bucketPlacement.ownerFor(
            descriptor.bucket(),
            plane.membership().current(),
            plane.clock().getAsLong(),
            lease -> lease.roles().contains(ServerlessNode.ROLE_INGEST)
        );
        return owner.filter(id -> id.equals(node.localNode.getId()) == false);
    }

    /** Whether this node owns bucket members through their bucket's head. */
    boolean bucketsOwnedTogether() {
        return node.nodeLogEnabled && org.opensearch.serverless.bucket.BucketSettings.enabled(node.settings);
    }

    /**
     * Writer activations' time by phase, in milliseconds summed since the node started, and how many opened.
     *
     * @return describe, acquire, mark owned and open, the count of activations that opened, then the part of open spent on
     *         the local view
     */
    public long[] activationPhaseMillis() {
        return new long[] {
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationPhases[0].get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationPhases[1].get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationPhases[2].get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationPhases[3].get()),
            activationsOpened.get(),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationPhases[4].get()) };
    }

    /** Counts a write for an unheld shard sent to a member with room. */
    public void noteWriteSteered() {
        writesSteered.incrementAndGet();
    }
}
