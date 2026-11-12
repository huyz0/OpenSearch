/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.serverless.membership.NodeLease;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Which writer a bucket nobody holds should go to: the highest-ranked live writer for the bucket, by rendezvous hashing
 * over the writer leases every node already reads.
 *
 * <p><b>Why.</b> Without it, whichever node a bucket's first write lands on takes the bucket, and buckets pile up on
 * the nodes clients happen to reach. With it, every node computes the same answer from the same leases, with no
 * request and no coordinator, and a node joining or leaving moves only about one bucket in N.
 *
 * <p><b>Churn.</b> A node is ranked only once it has been seen for a hold-down (two minutes), so a node that restarts
 * or flaps does not pull buckets back and forth; one that comes back under a new incarnation soon after the last
 * doubles its hold-down, up to thirty minutes. The hold-down is measured from when this node first saw the lease: views
 * differ by at most a lease refresh, and since the head's compare-and-swap decides ownership, a disagreement costs at
 * most a forwarded write. A node holding more than 1.25 times the mean is passed over while another has room. A bucket
 * already held stays where it is while its owner ranks in the top two ({@link #keeps}).
 */
public final class BucketPlacement {

    /** How long a newly seen writer waits before buckets are placed on it. */
    public static final long DEFAULT_HOLD_DOWN_MILLIS = 120_000L;

    /** The longest hold-down a flapping writer is given. */
    public static final long MAX_HOLD_DOWN_MILLIS = 1_800_000L;

    /** How much above the fleet's mean a writer may hold and still be placed on. */
    public static final double LOAD_BOUND = 1.25;

    /** How many top-ranked writers may keep a bucket they hold. */
    public static final int KEEP_RANKS = 2;

    private record Seen(String ephemeralId, long since, long holdDownMillis) {
    }

    private final long baseHoldDownMillis;
    private final Map<String, Seen> seen = new HashMap<>();

    /**
     * Creates a placement with the default hold-down.
     */
    public BucketPlacement() {
        this(DEFAULT_HOLD_DOWN_MILLIS);
    }

    /**
     * Creates a placement.
     *
     * @param holdDownMillis how long a newly seen writer waits before it is ranked
     */
    public BucketPlacement(long holdDownMillis) {
        this.baseHoldDownMillis = Math.max(0L, holdDownMillis);
    }

    /** A bucket's score on a node: the highest score wins. */
    static long score(String bucketId, String nodeId) {
        final byte[] bytes = (bucketId + '\u0000' + nodeId).getBytes(StandardCharsets.UTF_8);
        return MurmurHash3.hash128(bytes, 0, bytes.length, 0L, new MurmurHash3.Hash128()).h1;
    }

    /**
     * Notes the writers in a membership view, starting the hold-down of each one not seen before.
     *
     * @param leases the current leases
     * @param nowMillis the clock
     */
    public synchronized void observe(Collection<NodeLease> leases, long nowMillis) {
        final boolean firstView = seen.isEmpty();
        for (NodeLease lease : leases) {
            final Seen before = seen.get(lease.nodeId());
            if (before == null) {
                // The first view this node has: everyone already there counts as settled, or a fresh node would wait
                // two minutes before placing anything at all.
                seen.put(lease.nodeId(), new Seen(lease.ephemeralId(), firstView ? Long.MIN_VALUE / 2 : nowMillis, baseHoldDownMillis));
            } else if (lease.ephemeralId() != null && lease.ephemeralId().equals(before.ephemeralId()) == false) {
                // A new incarnation. Soon after the last one came up, it is flapping: wait twice as long.
                final boolean flapping = nowMillis - before.since() < 2 * before.holdDownMillis();
                final long hold = flapping ? Math.min(MAX_HOLD_DOWN_MILLIS, Math.max(1L, before.holdDownMillis()) * 2) : baseHoldDownMillis;
                seen.put(lease.nodeId(), new Seen(lease.ephemeralId(), nowMillis, hold));
            }
        }
    }

    private synchronized boolean settled(NodeLease lease, long nowMillis) {
        final Seen s = seen.get(lease.nodeId());
        return s == null || nowMillis - s.since() >= s.holdDownMillis();
    }

    /**
     * Ranks the live writers for a bucket, best first. Writers still in their hold-down are left out while any settled
     * writer is live; writers past the load bound are put last.
     *
     * @param bucketId the bucket
     * @param leases the current leases
     * @param nowMillis the clock
     * @param writer which leases are live writers
     * @return node ids, best first
     */
    public List<String> rank(String bucketId, Collection<NodeLease> leases, long nowMillis, Predicate<NodeLease> writer) {
        observe(leases, nowMillis);
        final List<NodeLease> live = new ArrayList<>();
        for (NodeLease lease : leases) {
            if (lease.isExpiredAt(nowMillis) == false && lease.revoked() == false && writer.test(lease)) {
                live.add(lease);
            }
        }
        final List<NodeLease> settled = live.stream().filter(lease -> settled(lease, nowMillis)).toList();
        final List<NodeLease> candidates = settled.isEmpty() ? live : settled;
        final double mean = candidates.stream().filter(l -> l.held() >= 0).mapToInt(NodeLease::held).average().orElse(0);
        final Predicate<NodeLease> overloaded = lease -> mean > 0 && lease.held() > LOAD_BOUND * mean;
        return candidates.stream()
            .sorted(
                Comparator.comparing((NodeLease lease) -> overloaded.test(lease))
                    .thenComparing(Comparator.comparingLong((NodeLease lease) -> score(bucketId, lease.nodeId())).reversed())
                    .thenComparing(NodeLease::nodeId)
            )
            .map(NodeLease::nodeId)
            .toList();
    }

    /**
     * Returns the writer an unheld bucket should go to.
     *
     * @param bucketId the bucket
     * @param leases the current leases
     * @param nowMillis the clock
     * @param writer which leases are live writers
     * @return the writer's node id, or empty if no writer is live
     */
    public Optional<String> ownerFor(String bucketId, Collection<NodeLease> leases, long nowMillis, Predicate<NodeLease> writer) {
        return rank(bucketId, leases, nowMillis, writer).stream().findFirst();
    }

    /**
     * Returns whether a writer holding a bucket should keep it: while it ranks in the top {@value #KEEP_RANKS}, a bucket
     * stays put, so a joining node takes over only the buckets it is clearly better for.
     *
     * @param bucketId the bucket
     * @param ownerNodeId its holder
     * @param leases the current leases
     * @param nowMillis the clock
     * @param writer which leases are live writers
     * @return true if the holder should keep it
     */
    public boolean keeps(String bucketId, String ownerNodeId, Collection<NodeLease> leases, long nowMillis, Predicate<NodeLease> writer) {
        final List<String> ranked = rank(bucketId, leases, nowMillis, writer);
        final int at = ranked.indexOf(ownerNodeId);
        return at >= 0 && at < KEEP_RANKS;
    }
}
