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
    }

    /**
     * Returns how full this node is and what fullness has cost.
     *
     * @return the figures
     */
    public Capacity capacity() {
        return new Capacity(
            node.admission.maxShardsHeld(),
            node.reconciler == null ? 0 : node.reconciler.heldShards().size(),
            node.readerOpensRefusedAtCap.get(),
            node.activationsRefusedAtCap.get(),
            node.evictedForRoom.get(),
            node.handedOff.get(),
            node.writesSteered.get()
        );
    }

    /**
     * Counts a writer activation refused at the cap.
     *
     * @param indexName the index
     * @param shard the shard
     */
    public void noteActivationRefusedAtCap(String indexName, int shard) {
        node.activationsRefusedAtCap.incrementAndGet();
        noteRefusedRecently(indexName, shard);
    }

    void noteRefusedRecently(String indexName, int shard) {
        node.refusedShards.put(ServerlessNode.shardKey(indexName, shard), System.nanoTime());
    }

    /**
     * How many distinct shards this node turned away at its cap in the last minute: writer activations and reader opens.
     *
     * @return the count
     */
    public int refusedLastMinute() {
        final long cutoff = System.nanoTime() - java.util.concurrent.TimeUnit.MINUTES.toNanos(1);
        node.refusedShards.values().removeIf(at -> at < cutoff);
        return node.refusedShards.size();
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
            if (last.isPresent() && now - last.getAsLong() < ServerlessNode.IN_USE_WINDOW_MILLIS) {
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
        return node.placement.fleetSize(plane);
    }

    /**
     * Counts a shard given up, to make room or for a member with room.
     *
     * @param forHandOff true if given up for a member with room
     */
    public void noteEvicted(boolean forHandOff) {
        (forHandOff ? node.handedOff : node.evictedForRoom).incrementAndGet();
    }

    /** Counts a dead member's shard taken past the cap, without evicting for it. */
    public void noteTakeoverOvercommitted() {
        node.takeoversOvercommitted.incrementAndGet();
    }

    /**
     * Whether this node holds more than {@link ServerlessNode#HAND_OFF_ABOVE} of its cap.
     *
     * @return true when nearly full
     */
    public boolean nearlyFull() {
        return node.placement.nearlyFull();
    }

    /**
     * The live writer member with the most room, if any. See {@link FleetPlacement#memberWithRoom}.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public java.util.Optional<String> memberWithRoom(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return node.placement.memberWithRoom(plane);
    }

    /**
     * Sets whether requests for a dead member's shard go to the one survivor named for it.
     *
     * @param route whether to
     */
    public void setRouteTakeovers(boolean route) {
        node.placement.setRouteTakeovers(route);
    }

    /**
     * Whether requests for a dead member's shard go to the one survivor named for it.
     *
     * @return true if they do
     */
    public boolean routesTakeovers() {
        return node.placement.routesTakeovers();
    }

    /**
     * Sets how far past its cap a member may take a dead member's shards. See {@link FleetPlacement#setTakeoverOvercommit}.
     *
     * @param fraction of the cap
     */
    public void setTakeoverOvercommit(double fraction) {
        node.placement.setTakeoverOvercommit(fraction);
    }

    /** Counts a write for a dead member's shard sent to the member that takes it over. */
    public void noteTakeoverWriteRouted() {
        node.placement.noteTakeoverWriteRouted();
    }

    /** Counts a doubt about a dead member's shard this node left to the member that takes it over. */
    public void noteTakeoverDoubtLeft() {
        node.placement.noteTakeoverDoubtLeft();
    }

    /**
     * Writes routed to, and doubts left to, the member that takes a dead member's shard over.
     *
     * @return routed, then left
     */
    public long[] takeoverRouting() {
        return node.placement.takeoverRouting();
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
        return node.placement.takeoverWinner(plane, indexName, shard, deadOwner);
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
        return node.placement.takeoverBelongsElsewhere(plane, indexName, shard);
    }

    /**
     * The mean of held shards over the live writer members. See {@link FleetPlacement#fleetMeanHeld}.
     *
     * @param plane the metadata plane
     * @return the mean, or -1 if no member says
     */
    public double fleetMeanHeld(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return node.placement.fleetMeanHeld(plane);
    }

    /**
     * Whether this node holds far more than its share of the fleet. See {@link FleetPlacement#aboveFleetShare}.
     *
     * @param plane the metadata plane
     * @return true when above its share
     */
    public boolean aboveFleetShare(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return node.placement.aboveFleetShare(plane);
    }

    /**
     * The live writer member holding the fewest shards, if below the fleet's mean. See {@link FleetPlacement#memberBelowMean}.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public java.util.Optional<String> memberBelowMean(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return node.placement.memberBelowMean(plane);
    }

    /**
     * Where this node should send work it would otherwise take, if anywhere. See {@link FleetPlacement#handOffTarget}.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public java.util.Optional<String> handOffTarget(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return node.placement.handOffTarget(plane);
    }

    /**
     * Writer activations' time by phase, in milliseconds summed since the node started, and how many opened.
     *
     * @return describe, acquire, mark owned and open, the count of activations that opened, then the part of open spent on
     *         the local view
     */
    public long[] activationPhaseMillis() {
        return new long[] {
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(node.activationPhases[0].get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(node.activationPhases[1].get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(node.activationPhases[2].get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(node.activationPhases[3].get()),
            node.activationsOpened.get(),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(node.activationPhases[4].get()) };
    }

    /** Counts a write for an unheld shard sent to a member with room. */
    public void noteWriteSteered() {
        node.writesSteered.incrementAndGet();
    }
}
