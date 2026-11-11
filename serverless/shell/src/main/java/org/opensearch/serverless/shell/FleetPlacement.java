/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Where a node's shards belong across the writer fleet: how many nodes the fleet's demand wants, which member has room
 * for work this node would hand off, and which survivor takes over a dead member's shard.
 *
 * <p>Every answer is computed from the leases each member renews with its load, plus this node's own cap and held
 * count, so none of them costs a request to the object store. Routing only: the shard head's compare-and-swap still
 * decides who owns a shard.
 */
final class FleetPlacement {

    private final Supplier<String> localNodeId;
    private final IntSupplier maxShardsHeld;
    private final IntSupplier heldShards;

    /**
     * Creates the placement view of one node.
     *
     * @param localNodeId this node's id, read on every call because the node rebinds it at start
     * @param maxShardsHeld this node's shard cap
     * @param heldShards how many shards this node holds now
     */
    FleetPlacement(Supplier<String> localNodeId, IntSupplier maxShardsHeld, IntSupplier heldShards) {
        this.localNodeId = localNodeId;
        this.maxShardsHeld = maxShardsHeld;
        this.heldShards = heldShards;
    }

    /** The share of a node's cap a fleet sized by {@link #fleetSize} aims to use. */
    public static final double TARGET_UTILISATION = 0.8;

    /**
     * Sizes the writer fleet from the load every member renews into its lease: shards used in the last minute, and shards
     * turned away at a cap, over the average cap at {@link #TARGET_UTILISATION}. Shards merely held do not count -- a
     * node's cap is a cache that fills whatever the load -- so a fleet full but turning nothing away does not ask for more,
     * and one turning shards away asks for as many more nodes as would have taken them.
     *
     * @param plane the metadata plane
     * @return the members counted and the nodes wanted
     */
    public FleetSize fleetSize(MetadataPlane plane) {
        final long now = plane.clock().getAsLong();
        int members = 0;
        long demand = 0;
        long caps = 0;
        for (NodeLease lease : plane.membership().current()) {
            // A member's lease as this node last read it, which is re-read only once it has run out: up to a TTL past its
            // expiry it is a live member whose renewal this node has not fetched yet, not a departed one. Counting only
            // unexpired copies dropped a varying share of the fleet, and nodes asked at the same moment answered 1 and 7.
            if (lease.revoked()
                || now >= lease.expiresAtMillis() + plane.leaseTtlMillis()
                || lease.roles().contains(ServerlessNode.ROLE_INGEST) == false
                || lease.inUse() < 0
                || lease.cap() <= 0) {
                continue;
            }
            members++;
            demand += lease.inUse() + Math.max(0, lease.refused());
            caps += lease.cap();
        }
        if (members == 0) {
            // Nobody reports a load yet -- a fleet starting, or a view from before leases carried it. Not "none wanted".
            return new FleetSize(0, -1, 0);
        }
        final double perNode = (double) caps / members * TARGET_UTILISATION;
        return new FleetSize(members, (int) Math.max(1, Math.ceil(demand / perNode)), demand);
    }

    /** Above this share of its cap a node looks for a member to hand work to. */
    public static final double HAND_OFF_ABOVE = 0.9;
    /** At or below this share of its cap a member can take work handed to it. */
    public static final double ROOM_AT_OR_BELOW = 0.75;

    /**
     * Whether this node holds more than {@link #HAND_OFF_ABOVE} of its cap.
     *
     * @return true when nearly full
     */
    public boolean nearlyFull() {
        final int cap = maxShardsHeld.getAsInt();
        return cap > 0 && heldShards.getAsInt() >= cap * HAND_OFF_ABOVE;
    }

    /**
     * The live writer member with the most room, if any has {@link #ROOM_AT_OR_BELOW} of its cap or less: where a full
     * node sends work. From the leases every member renews with its load, so it costs no request.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public Optional<String> memberWithRoom(MetadataPlane plane) {
        final long now = plane.clock().getAsLong();
        String best = null;
        double bestShare = ROOM_AT_OR_BELOW;
        for (NodeLease lease : plane.membership().current()) {
            if (lease.nodeId().equals(localNodeId.get())
                || lease.isExpiredAt(now)
                || lease.roles().contains(ServerlessNode.ROLE_INGEST) == false
                || lease.held() < 0
                || lease.cap() <= 0) {
                continue;
            }
            final double share = (double) lease.held() / lease.cap();
            if (share <= bestShare) {
                best = lease.nodeId();
                bestShare = share;
            }
        }
        return Optional.ofNullable(best);
    }

    private volatile double takeoverOvercommit = 0.0;

    private volatile boolean routeTakeovers = true;

    /**
     * Sets whether requests for a dead member's shard go to the one survivor named for it: on by default; off only to
     * measure what it saves.
     *
     * @param route whether to
     */
    public void setRouteTakeovers(boolean route) {
        this.routeTakeovers = route;
    }

    /**
     * Whether requests for a dead member's shard go to the one survivor named for it.
     *
     * @return true if they do
     */
    public boolean routesTakeovers() {
        return routeTakeovers;
    }

    /**
     * Sets how far past its cap a member may take a dead member's shards, as BackgroundReconciler does: the ceiling a
     * member's room is judged against when choosing who takes over a shard.
     *
     * @param fraction of the cap
     */
    public void setTakeoverOvercommit(double fraction) {
        this.takeoverOvercommit = Math.max(0.0, fraction);
    }

    private final AtomicLong takeoverWritesRouted = new AtomicLong();
    private final AtomicLong takeoverDoubtsLeft = new AtomicLong();

    /** Counts a write for a dead member's shard sent to the member that takes it over. */
    public void noteTakeoverWriteRouted() {
        takeoverWritesRouted.incrementAndGet();
    }

    /** Counts a doubt about a dead member's shard this node left to the member that takes it over. */
    public void noteTakeoverDoubtLeft() {
        takeoverDoubtsLeft.incrementAndGet();
    }

    /**
     * Writes routed to, and doubts left to, the member that takes a dead member's shard over.
     *
     * @return routed, then left
     */
    public long[] takeoverRouting() {
        return new long[] { takeoverWritesRouted.get(), takeoverDoubtsLeft.get() };
    }

    /**
     * The member that takes over a shard whose owner's lease has run out: of the live writer members other than that
     * owner, those in the owner's zone first, in name order, the first from the shard's hash with room under its
     * overcommit ceiling; then of all of them the same way -- or, if none has
     * room, the one the hash names. Every member computes the same answer from the same leases, so a dead node's shards
     * are each taken by one survivor rather than queued by every survivor a request for them happens to reach: a fleet
     * run had every survivor queueing ~300 of a dead node's shards, and requests waited 30-60 s in those queues.
     *
     * <p>Routing only. The head's compare-and-swap still decides who owns the shard; two members with different views
     * cost a wasted activation, never a second owner.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shard the shard
     * @param deadOwner the owner whose lease ran out, left out of the choice
     * @return the member's id, or empty if no live writer member is known
     */
    public Optional<String> takeoverWinner(MetadataPlane plane, String indexName, int shard, String deadOwner) {
        final long now = plane.clock().getAsLong();
        final TreeMap<String, NodeLease> live = new TreeMap<>();
        String deadZone = null;
        for (NodeLease lease : plane.membership().current()) {
            if (lease.nodeId().equals(deadOwner)) {
                deadZone = lease.zone();
            }
            if (lease.nodeId().equals(deadOwner) == false
                && lease.isExpiredAt(now) == false
                && lease.roles().contains(ServerlessNode.ROLE_INGEST)) {
                live.put(lease.nodeId(), lease);
            }
        }
        if (live.isEmpty()) {
            return Optional.empty();
        }
        if (deadZone == null) {
            deadZone = zoneOfDeparted(plane, deadOwner);
        }
        // A member in the dead owner's zone first, with room: the shard's writes were arriving in that zone, and a
        // survivor there keeps them from crossing to another. Any member otherwise, exactly as before.
        final List<String> inZone = new ArrayList<>();
        for (NodeLease lease : live.values()) {
            if (org.opensearch.serverless.cluster.Zones.same(deadZone, lease.zone())) {
                inZone.add(lease.nodeId());
            }
        }
        if (inZone.isEmpty() == false) {
            final int firstInZone = Math.floorMod((indexName + "#" + shard).hashCode(), inZone.size());
            for (int i = 0; i < inZone.size(); i++) {
                final String candidate = inZone.get((firstInZone + i) % inZone.size());
                if (hasTakeoverRoom(candidate, live.get(candidate))) {
                    return Optional.of(candidate);
                }
            }
        }
        final List<String> order = new ArrayList<>(live.keySet());
        final int first = Math.floorMod((indexName + "#" + shard).hashCode(), order.size());
        for (int i = 0; i < order.size(); i++) {
            final String candidate = order.get((first + i) % order.size());
            if (hasTakeoverRoom(candidate, live.get(candidate))) {
                return Optional.of(candidate);
            }
        }
        return Optional.of(order.get(first));
    }

    /**
     * Whether a shard is a dead member's that another live member is to take over, so this node should leave it: the head
     * names an owner with no live lease, and the member named for it is not this one. False when anything is unknown, and
     * when routing is off: routing must never become nobody taking it.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shard the shard
     * @return true to leave it to the member named
     */
    public boolean takeoverBelongsElsewhere(MetadataPlane plane, String indexName, int shard) {
        if (routeTakeovers == false) {
            return false;
        }
        try {
            final var head = plane.heads().read(indexName, shard);
            if (head.isEmpty()
                || head.get().ownerNodeId() == null
                || head.get().ownerNodeId().equals(localNodeId.get())
                || plane.heads().isHeld(head.get(), plane.clock().getAsLong())) {
                return false;
            }
            final Optional<String> winner = takeoverWinner(plane, indexName, shard, head.get().ownerNodeId());
            return winner.isPresent() && winner.get().equals(localNodeId.get()) == false;
        } catch (Exception e) {
            return false;
        }
    }

    /** Zones of owners whose leases have run out, "" when the lease says none; read once each, then remembered. */
    private final java.util.Map<String, String> departedZones = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The zone of a member no longer in the live view, from its lease, which outlives it until the sweep: one read per
     * departed member, not one per shard it owned, and nothing at all once its zone is known.
     */
    private String zoneOfDeparted(MetadataPlane plane, String nodeId) {
        final String known = departedZones.get(nodeId);
        if (known != null) {
            return known.isEmpty() ? null : known;
        }
        try {
            final String zone = plane.membership().read(nodeId).map(NodeLease::zone).orElse(null);
            if (departedZones.size() >= 4096) {
                departedZones.clear();
            }
            departedZones.put(nodeId, zone == null ? "" : zone);
            return zone;
        } catch (Exception e) {
            // Unknown this time; asked again on the next takeover. Any member takes it meanwhile.
            return null;
        }
    }

    private boolean hasTakeoverRoom(String nodeId, NodeLease lease) {
        if (nodeId.equals(localNodeId.get())) {
            final int cap = maxShardsHeld.getAsInt();
            final int held = heldShards.getAsInt();
            return cap <= 0 || held < cap + Math.floor(cap * takeoverOvercommit);
        }
        if (lease.held() < 0 || lease.cap() <= 0) {
            return true;
        }
        return lease.held() < lease.cap() + Math.floor(lease.cap() * takeoverOvercommit);
    }

    /** Above this multiple of the fleet's mean held shards a node hands work off, however far it is from its cap. */
    public static final double HAND_OFF_ABOVE_MEAN = 1.5;
    /** A node handing off for being above the mean hands off down to this multiple of it. */
    public static final double HAND_OFF_DOWN_TO_MEAN = 1.25;
    /** Below this share of its cap a node is never too busy for the mean to matter: a small fleet's few shards stay. */
    public static final double MEAN_MATTERS_FROM = 0.25;

    /**
     * The mean of held shards over the live writer members, this node counted from its own count rather than its lease.
     *
     * @param plane the metadata plane
     * @return the mean, or -1 if no member says
     */
    public double fleetMeanHeld(MetadataPlane plane) {
        final long now = plane.clock().getAsLong();
        long sum = heldShards.getAsInt();
        int members = 1;
        for (NodeLease lease : plane.membership().current()) {
            if (lease.nodeId().equals(localNodeId.get())
                || lease.isExpiredAt(now)
                || lease.roles().contains(ServerlessNode.ROLE_INGEST) == false
                || lease.held() < 0) {
                continue;
            }
            sum += lease.held();
            members++;
        }
        return members < 2 ? -1 : (double) sum / members;
    }

    /**
     * Whether this node holds far more than its share: more than {@link #HAND_OFF_ABOVE_MEAN} of the fleet's mean, and at
     * least {@link #MEAN_MATTERS_FROM} of its cap. Hand-off near the cap left load uneven everywhere below it: the node a
     * fleet run killed held anywhere from 103 to 847 shards, and how long recovery took followed that.
     *
     * @param plane the metadata plane
     * @return true when above its share
     */
    public boolean aboveFleetShare(MetadataPlane plane) {
        final int cap = maxShardsHeld.getAsInt();
        final int held = heldShards.getAsInt();
        final double mean = fleetMeanHeld(plane);
        return cap > 0 && mean > 0 && held >= cap * MEAN_MATTERS_FROM && held > mean * HAND_OFF_ABOVE_MEAN;
    }

    /**
     * The live writer member holding the fewest shards, if it holds fewer than the fleet's mean: where a node above its
     * share sends work.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public Optional<String> memberBelowMean(MetadataPlane plane) {
        final double mean = fleetMeanHeld(plane);
        if (mean <= 0) {
            return Optional.empty();
        }
        final long now = plane.clock().getAsLong();
        String best = null;
        long fewest = Long.MAX_VALUE;
        for (NodeLease lease : plane.membership().current()) {
            if (lease.nodeId().equals(localNodeId.get())
                || lease.isExpiredAt(now)
                || lease.roles().contains(ServerlessNode.ROLE_INGEST) == false
                || lease.held() < 0
                || lease.held() >= mean) {
                continue;
            }
            if (lease.held() < fewest) {
                best = lease.nodeId();
                fewest = lease.held();
            }
        }
        return Optional.ofNullable(best);
    }

    /**
     * Where this node should send work it would otherwise take, if anywhere: to a member with room when this one is nearly
     * full, or to a member below the mean when this one holds far more than its share.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public Optional<String> handOffTarget(MetadataPlane plane) {
        if (nearlyFull()) {
            final Optional<String> roomy = memberWithRoom(plane);
            if (roomy.isPresent()) {
                return roomy;
            }
        }
        return aboveFleetShare(plane) ? memberBelowMean(plane) : Optional.empty();
    }
}
