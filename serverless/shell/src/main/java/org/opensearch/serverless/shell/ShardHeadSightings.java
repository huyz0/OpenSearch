/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ShardHead;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The shard heads this node has read lately, kept so the readers in one pass share one register read and so
 * a coordinator can route a write to the owner it last saw.
 *
 * <p>Every answer here is a hint: whoever uses one survives its being wrong, because the owner refuses what it
 * does not hold and the caller then reads the register.
 */
public final class ShardHeadSightings {

    private final Supplier<MetadataPlane> metadataPlane;

    /**
     * Creates an empty set of sightings.
     *
     * @param metadataPlane the node's metadata plane, whose clock and lease TTL age a sighting; null before it is set
     */
    ShardHeadSightings(Supplier<MetadataPlane> metadataPlane) {
        this.metadataPlane = metadataPlane;
    }

    /** A head as last read, with when: the heartbeat's reads, reused by the same pass rather than repeated. */
    private record SeenHead(ShardHead head, long atMillis) {
    }

    private final Map<String, SeenHead> recentHeads = new ConcurrentHashMap<>();

    /**
     * Records a head this node has just read, for the other readers in the same pass and for routing.
     *
     * <p>The heartbeat, the hint rebuild and the publish each read the same register within a second of
     * each other; a write to a shard this node does not hold read it again to learn the owner. One read
     * feeds all of them now. A hint is a hint: every user of it survives its being wrong, because the
     * owner refuses what it does not hold and the caller then reads the register.
     *
     * @param index the index
     * @param shard the shard
     * @param head what was read, or null if there was no head
     */
    public void noteHead(String index, int shard, ShardHead head) {
        final long now = metadataPlane.get() == null ? System.currentTimeMillis() : metadataPlane.get().clock().getAsLong();
        if (head == null) {
            recentHeads.remove(index + "#" + shard);
        } else {
            recentHeads.put(index + "#" + shard, new SeenHead(head, now));
            pruneRecentHeads(now);
        }
    }

    /**
     * Past this many shards, sightings older than a lease are dropped.
     *
     * <p>No reader believes one that old -- {@link #ownerHint} discards it and every {@link #recentHead}
     * caller asks for less -- so dropping it changes no answer. Only the lookups that happened to find one
     * used to remove it, so a shard routed to once and never asked about again stayed for the life of the
     * process: a coordinator that routed to a million shards held a million sightings.
     */
    static final int DEFAULT_RECENT_HEADS_PRUNE_ABOVE = 10_000;

    private volatile int recentHeadsPruneAbove = DEFAULT_RECENT_HEADS_PRUNE_ABOVE;

    private volatile long recentHeadsPrunedAt = Long.MIN_VALUE;

    /**
     * Sets how many head sightings this node may hold before those older than a lease are dropped.
     *
     * @param entries the threshold
     */
    public void setRecentHeadsPruneAbove(int entries) {
        this.recentHeadsPruneAbove = Math.max(0, entries);
    }

    private void pruneRecentHeads(long now) {
        final MetadataPlane plane = metadataPlane.get();
        // The oldest any reader accepts: a lease, or a second where a test runs a shorter one.
        final long ttl = Math.max(1_000L, plane == null ? 30_000L : plane.leaseTtlMillis());
        // At most once per lease: a sighting cannot age out faster than that.
        if (recentHeads.size() <= recentHeadsPruneAbove || (recentHeadsPrunedAt != Long.MIN_VALUE && now - recentHeadsPrunedAt < ttl)) {
            return;
        }
        recentHeadsPrunedAt = now;
        recentHeads.values().removeIf(seen -> now - seen.atMillis() > ttl);
    }

    /**
     * Returns how many shards this node holds a head sighting for, for tests that pin its bound.
     *
     * @return the entry count
     */
    public int recentHeadCount() {
        return recentHeads.size();
    }

    /**
     * Returns a head read within the given age, if this node has one.
     *
     * @param index the index
     * @param shard the shard
     * @param maxAgeMillis how old the read may be
     * @return the head, or empty if none that young was read
     */
    public Optional<ShardHead> recentHead(String index, int shard, long maxAgeMillis) {
        final SeenHead seen = recentHeads.get(index + "#" + shard);
        if (seen == null) {
            return Optional.empty();
        }
        final long now = metadataPlane.get() == null ? System.currentTimeMillis() : metadataPlane.get().clock().getAsLong();
        return now - seen.atMillis() <= maxAgeMillis ? Optional.of(seen.head()) : Optional.empty();
    }

    /**
     * Returns the head last read for a shard, if it was read at or after a moment, whatever its age.
     *
     * @param index the index
     * @param shard the shard
     * @param sinceMillis the earliest the read may have been, by the plane's clock
     * @return the head, or empty if none was read since then
     */
    public Optional<ShardHead> headSeenSince(String index, int shard, long sinceMillis) {
        final SeenHead seen = recentHeads.get(index + "#" + shard);
        return seen != null && seen.atMillis() >= sinceMillis ? Optional.of(seen.head()) : Optional.empty();
    }

    /**
     * Returns the node last seen owning a shard, if the sighting is no older than one lease TTL.
     *
     * <p>Bounded, where it used to be unbounded: a hint is invalidated by a refusal from the node it
     * names, and a node that is dead refuses nothing. A coordinator whose hint named a dead owner
     * forwarded every write there until something else happened to re-read the head. A lease is the
     * longest a node can be gone unnoticed, so a sighting older than one is not a hint any more; it is
     * re-read, at the cost of one register read per TTL per shard this node routes to but does not hold.
     *
     * @param index the index
     * @param shard the shard
     * @return the owner's node id, or empty if no head young enough has been read here
     */
    public Optional<String> ownerHint(String index, int shard) {
        final String key = index + "#" + shard;
        final SeenHead seen = recentHeads.get(key);
        if (seen == null) {
            return Optional.empty();
        }
        final MetadataPlane plane = metadataPlane.get();
        if (plane != null && plane.clock().getAsLong() - seen.atMillis() > plane.leaseTtlMillis()) {
            recentHeads.remove(key, seen);
            return Optional.empty();
        }
        return Optional.ofNullable(seen.head().ownerNodeId());
    }

    /**
     * Forgets a hint an owner has just refused.
     *
     * @param index the index
     * @param shard the shard
     */
    public void forgetOwner(String index, int shard) {
        recentHeads.remove(index + "#" + shard);
    }
}
